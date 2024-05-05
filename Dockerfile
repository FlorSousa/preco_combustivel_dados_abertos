FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN apt-get update \
    && apt-get install -y libpq-dev build-essential
RUN apt-get update && apt-get install -y libpq-dev
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN chmod +x /app/wait-for-it.sh