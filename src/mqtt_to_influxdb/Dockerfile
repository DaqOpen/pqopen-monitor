# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.11
FROM python:${PYTHON_VERSION}-alpine as base

RUN apk update && apk add tzdata gcc musl-dev python3-dev libffi-dev openssl-dev
ENV TZ=Europe/Vienna

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Copy the source code into the container.
COPY . .

RUN pip install -r requirements.txt

# Run the application.
CMD ["python3", "mqtt-to-influxdbv2.py"]
