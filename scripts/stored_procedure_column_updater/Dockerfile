FROM python:3.13-bullseye

WORKDIR /app
COPY . .

RUN pip install --no-cache-dir -r requirements.txt

RUN chmod 644 config.env

CMD ["python", "main.py"]
