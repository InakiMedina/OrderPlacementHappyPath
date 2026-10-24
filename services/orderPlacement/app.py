import json
import sys
import os
import time
import pika
from datetime import datetime, timezone
import random
from loguru import logger
import uuid


def get_order_from_file():

  fname = "order.json"

  try:
    with open(fname, "r", encoding="utf-8") as file:
      order = json.load(file)
  except OSError:
    logger.error(f"Could not open/read file: {fname}")
    sys.exit(f"Could not open/read file: {fname}")
  except json.JSONDecodeError:
    logger.error(f"File contains invalid JSON: {fname}")
    sys.exit(f"File contains invalid JSON: {fname}")

  return order


def place_order(channel):

  order = get_order_from_file()

  event_id = str(uuid.uuid4())
  timestamp = datetime.now(timezone.utc).isoformat()

  bodyPayload = {
      "metadata": {
          "event_id": event_id,
          "event_type": "OrderPlaced",
          "producer": "order-placement-service",
          "timestamp": timestamp,
      },
      "payload": order,  # Your order.json content
  }

  channel.basic_publish(
    exchange="orders",
    routing_key="order.placed",
    body=json.dumps(bodyPayload),
    properties=pika.BasicProperties(delivery_mode=2)
  )

  order_id = order.get("order_id", "N/A")
  logger.success(
    f"Order Dispatched -> <bold><green>ID: {order_id}</green></bold> "
      f"[EventID: <dim>{event_id}</dim>]"
  )


if __name__ == "__main__":

  RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")


  # Retry loop in case RabbitMQ takes a few seconds to start up in Docker
  while True:
    try:
      connection = pika.BlockingConnection(
          pika.ConnectionParameters(host=RABBITMQ_HOST)
      )
      logger.success("Established connection with RabbitMQ")
      break
    except pika.exceptions.AMQPConnectionError:
      logger.info("Waiting for RabbitMQ")
      time.sleep(5)

  
  logger.remove()
  logger.add(
      "/var/log/app/system.log",
      format="{time:YYYY-MM-DD HH:mm:ss.SSS} | <cyan>[ORDER-SERVICE]</cyan> | <level>{level: <8}</level> | {message}",
      colorize=True,
      enqueue=False
  )
  logger.success("Worker started")

  channel = connection.channel()
  channel.exchange_declare(exchange="orders", exchange_type="topic", durable=True)

  place_order(channel)

  connection.close()
  logger.complete()
