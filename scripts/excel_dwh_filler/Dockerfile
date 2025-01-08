FROM python:3.13-bullseye

WORKDIR /app
COPY . .

RUN curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc
RUN curl https://packages.microsoft.com/config/debian/11/prod.list | tee /etc/apt/sources.list.d/mssql-release.list

RUN apt-get update
RUN ACCEPT_EULA=Y apt-get install -y msodbcsql18

RUN pip install --no-cache-dir -r requirements.txt

RUN chmod 644 config.env

CMD ["python", "excel_sheets_filler.py"]
