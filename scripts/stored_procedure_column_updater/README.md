# SQL stored procedure column updater

This script is used to update the column names that are present in 
the result of the DWH SQL stored procedure. The column names from 
the Excel template file are used to update the .sql file that contains 
the stored procedure.

## Requirements
- Docker / docker-compose.
- The DWH Excel template. Paste it to the same directory as the script, and name it `template.xlsx`.
- A config file, named `config.env`. Replace the placeholders with the actual values: 
the file which contains the stored procedure contents, and the output file name.

## Usage

Run the script from CLI with the following command (when in the script directory):
```bash
docker-compose up --build
```

You can also run the container using Docker Desktop UI 
(but sometimes it may freeze in the middle of a process - then a second run is necessary).
