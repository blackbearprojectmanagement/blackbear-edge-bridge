FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN adduser --disabled-password --gecos "" beb

COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

COPY app ./app

RUN mkdir -p /app/data /app/logs \
    && chown -R beb:beb /app

USER beb

EXPOSE 8000

CMD ["python", "-m", "app.main"]
