# Excel DWH filler

This script is used to fill the Excel file with data from DWH.

## Requirements
- Python 3.10+
- The packages listed in the requirements file. You can install them with:
```bash
pip install -r requirements.txt
```
- The DWH Excel template. Paste it to the same directory as the script, and name it `template.xlsx`.

## Usage

Run the script from CLI with the following command:
```bash
python excel_sheets_filler.py --output_file=JO-GL.xlsx --db_url=XX.XXX.XX.XXX --db_name=PUTDBNAMEHERE --db_username=PUTDBUSERNAMEHERE --db_password=PUTPASSWORDHERE --site=JO-GL
```

Remember to replace the placeholders with the actual values: the database URL, database name, database username, database password, and site name.