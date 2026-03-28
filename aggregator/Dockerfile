FROM python:3.12-slim

WORKDIR /app

COPY aggregator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ /app/common/
COPY schemas/ /app/schemas/
COPY aggregator/ /app/

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
