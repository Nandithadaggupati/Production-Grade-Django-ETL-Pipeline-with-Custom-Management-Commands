FROM python:3.9-slim

WORKDIR /app

# Install system dependencies needed for compiling psycopg2 if binary has any issues
RUN apt-get update && apt-get install -y \
    libpq-dev \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Run python with unbuffered output to ensure logs show immediately
CMD ["python", "-u", "main.py"]
