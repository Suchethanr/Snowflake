#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import snowflake.connector as sf
import time
import os
import csv
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import getpass

class MyHandler(FileSystemEventHandler):
    def __init__(self, account, user, password, database, warehouse, schema, directory_path):
        self.account = account
        self.user = user
        self.password = password
        self.database = database
        self.warehouse = warehouse
        self.schema = schema
        self.directory_path = directory_path
        self.existing_tables = {}  

    def on_created(self, event):
        if event.is_directory:
            return
        elif event.src_path.endswith('.csv'):
            print("New CSV file detected:", event.src_path)
            self.load_to_snowflake(event.src_path)

    def load_to_snowflake(self, file_path):
        sf_conn_obj = sf.connect(
            account=self.account,
            user=self.user,
            password=self.password,
            database=self.database,
            warehouse=self.warehouse,
            schema=self.schema
        )

        sf_cur_obj = sf_conn_obj.cursor()
        sf_cur_obj.execute("USE DATABASE {}".format(self.database))
        sf_cur_obj.execute("USE SCHEMA {}".format(self.schema))
        print("Loading file into Snowflake:", file_path)
        try:
            table_name = os.path.splitext(os.path.basename(file_path))[0]
            print("File name:", table_name)

            matched_table = None
            for existing_table, existing_file in self.existing_tables.items():
                if table_name.startswith(existing_table):
                    matched_table = existing_table
                    break

            if matched_table: 
                print("Matching table found:", matched_table)
                stage_name = "{}_stage".format(matched_table)
                sf_cur_obj.execute("PUT 'file://{}' @{} AUTO_COMPRESS = FALSE".format(file_path.replace('\\', '/'), stage_name))
                print("Put command executed")

                with open(file_path, 'r') as f:
                    header = f.readline().strip()
                    existing_header = None
                    with open(existing_file, 'r') as ef:
                        existing_header = ef.readline().strip()
                    if header == existing_header:
                        sf_cur_obj.execute("""
                            COPY INTO {} 
                            FROM @{} 
                            FILE_FORMAT = (format_name = {}_ff)
                        """.format(matched_table, stage_name, matched_table))
                        print("Copy into query executed")
                        print("File {} appended to Snowflake table {}".format(file_path, matched_table))
                    else:
                        print("Header names do not match. Creating a new stage, table, and following the usual process.")
                        
                        self.create_new_table(sf_cur_obj, file_path, table_name)

            else:
                self.create_new_table(sf_cur_obj, file_path, table_name)

            print("------------------------------------------------------------------")
        except Exception as e:
            print("Error:", e)
            print("------------------------------------------------------------------")
        finally:
            sf_cur_obj.close()
            sf_conn_obj.close()

    def create_new_table(self, sf_cur_obj, file_path, table_name):
        sf_cur_obj.execute("CREATE OR REPLACE STAGE {}_stage".format(table_name))
        print("Internal stage created")

        def detect_delimiter(file_path):
            with open(file_path, 'r') as f:
                first_line = f.readline().strip()
                dialect = csv.Sniffer().sniff(first_line)
                return dialect.delimiter

        delimiter = detect_delimiter(file_path)
        with open(file_path, 'r') as f:
            header = f.readline().strip()
            print("Header:", header)
            columns = ['"{}" STRING'.format(col.strip('"')) for col in header.split(delimiter)]
                    
        create_table_query = "CREATE OR REPLACE TABLE {} ({})".format(table_name, ','.join(columns))
        sf_cur_obj.execute(create_table_query)
        print("Table creation query executed")

        sf_cur_obj.execute("""
            CREATE OR REPLACE FILE FORMAT {}_ff
            TYPE = 'csv'
            FIELD_DELIMITER = '{}'
            SKIP_HEADER = 1
        """.format(table_name, delimiter))
        print("File format query executed")

        sf_cur_obj.execute("""
            PUT 'file://{}' @{}_stage AUTO_COMPRESS = FALSE
        """.format(file_path.replace('\\', '/'), table_name))
        print("Put command executed")

        sf_cur_obj.execute("""
            COPY INTO {} 
            FROM @{}_stage 
            FILE_FORMAT = (format_name = {}_ff)
        """.format(table_name, table_name, table_name))
        print("Copy into query executed")

        print("File {} moved to Snowflake table {}".format(file_path, table_name))

        self.existing_tables[table_name] = file_path

        print("------------------------------------------------------------------")

snowflake_account = input("Snowflake Account: ")
snowflake_user = getpass.getpass(prompt="Snowflake User: ")
snowflake_password = getpass.getpass(prompt="Snowflake Password: ")
snowflake_database = input("Snowflake Database: ")
snowflake_warehouse = input("Snowflake Warehouse: ")
snowflake_schema = input("Snowflake Schema: ")
directory_path = input("Directory path containing CSV files: ")

observer = Observer()
handler = MyHandler(
    snowflake_account,
    snowflake_user,
    snowflake_password,
    snowflake_database,
    snowflake_warehouse,
    snowflake_schema,
    directory_path
)
observer.schedule(handler, path=directory_path, recursive=True)
observer.start()
print("Observer started")

try:
    while True:
        time.sleep(60)
except KeyboardInterrupt:
    observer.stop()
observer.join()

