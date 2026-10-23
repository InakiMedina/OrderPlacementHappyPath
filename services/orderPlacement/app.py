import json
import sys
import os
import time
import pika

fname = "order.json"
order = {}

try:
  with open(fname, "r", encoding="utf-8") as file:
    order = json.load(file)
except OSError:
  sys.exit(f"Could not open/read file: {fname}")
except json.JSONDecodeError:
  sys.exit(f"File contains invalid JSON: {fname}")

RABBITMQ_HOST = os.getenv("place-order-broker", "rabbitmq")

# Retry loop in case RabbitMQ takes a few seconds to start up in Docker
while True:
  try:
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    break
  except pika.exceptions.AMQPConnectionError:
    print("Waiting for RabbitMQ to become ready...")
    time.sleep(2)

channel = connection.channel()
channel.exchange_declare(exchange="orders", exchange_type="topic", durable=True)

channel.basic_publish(
  exchange="orders",
  routing_key="order.placed",
  body=json.dumps(order),
  properties=pika.BasicProperties(delivery_mode=2)
)

print(f"Order placed : {json.dumps(order)}")
connection.close()