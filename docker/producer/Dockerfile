FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    confluent-kafka \
    faker

COPY scripts/producer_tickets.py /app/producer_tickets.py

CMD ["python", "/app/producer_tickets.py"]