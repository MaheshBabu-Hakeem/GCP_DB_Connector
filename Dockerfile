FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY sync_databricks_to_datastore.py .

CMD ["python", "sync_databricks_to_datastore.py"]
