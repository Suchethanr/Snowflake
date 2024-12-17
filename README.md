# Snowflake CSV Uploader

This Python script automates the process of uploading CSV files to Snowflake. It monitors a specified directory for new CSV files, detects changes in the file structure, and uploads the data to the appropriate Snowflake table. If a table does not exist, the script will automatically create one based on the CSV file's structure.

## Features

- **Automatic CSV Detection**: Continuously monitors a specified directory for new CSV files.
- **Table Management**: If the corresponding Snowflake table already exists, new data is appended; if not, a new table is created based on the CSV header.
- **Data Consistency**: Ensures that files with matching headers are processed together, maintaining data integrity.
- **Snowflake Integration**: Leverages Snowflake’s `COPY INTO` functionality to load data efficiently.

## Prerequisites

Ensure the following Python packages are installed:

- `snowflake-connector-python` – Snowflake connector for Python.
- `watchdog` – A library to monitor file system changes.
- `getpass` – Securely collects sensitive information.
- `csv` – Standard library for CSV file handling.

You can install the required dependencies using `pip`:

```bash
pip install snowflake-connector-python watchdog
