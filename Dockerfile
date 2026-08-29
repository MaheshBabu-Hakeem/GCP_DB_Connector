FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY unified_server.py create_datastore.py sync_databricks_to_datastore.py .

ENV PORT=8080
EXPOSE 8080

CMD ["sh", "-c", "uvicorn unified_server:app --host 0.0.0.0 --port ${PORT}"]
