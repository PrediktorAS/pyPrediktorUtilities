# Excel DWH filler

This script is used to fill the Excel file with data from DWH.

## Requirements
- Docker / docker-compose.
- The DWH Excel template. Paste it to the same directory as the script, and name it `template.xlsx`.
- A config file, named `config.env`. Replace the placeholders with the actual values: 
the output file name, the database URL, database name, 
database username, database password, and site name.

## Usage

Run the script from CLI with the following command (when in the script directory):
```bash
docker-compose up --build
```

You can also run the container using Docker Desktop UI 
(but sometimes it may freeze in the middle of a process - then a second run is necessary).
