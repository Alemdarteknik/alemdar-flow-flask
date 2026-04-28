FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000

CMD ["gunicorn", "--bind", "0.0.0.0:5000",
     "--workers", "2", "--threads", "4",
     "--worker-class", "gthread",
     "--timeout", "60",
     "--graceful-timeout", "30",
     "--max-requests", "1000", "--max-requests-jitter", "100",
     "app:app"]