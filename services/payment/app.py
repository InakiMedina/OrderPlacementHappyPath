import json
import os
import sys
import time
import pika
from loguru import logger

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")

logger.remove()
logger.add(
    "/var/log/app/system.log",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | <magenta>[ORDER-SERVICE]</magenta> | <level>{level: <8}</level> | {message}",
    colorize=True,
    enqueue=False
)

def payOrder(ch, method, properties, body):
  try:
    event_data = json.loads(body.decode("utf-8"))
  except (json.JSONDecodeError, UnicodeDecodeError) as e:
    logger.error(f"Failed to decode message: {e}", file=sys.stderr)
    # Reject message without requeueing corrupted payload
    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    return

  order_payload = event_data.get("payload", event_data)

  logger.success("payment Done")

  # Acknowledge completion
  ch.basic_ack(delivery_tag=method.delivery_tag)


# Connect with retry loop
while True:
  try:
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST)
    )
    break
  except pika.exceptions.AMQPConnectionError:
    logger.info("Waiting for RabitMQ")
    time.sleep(2)

channel = connection.channel()

channel.exchange_declare(exchange="orders", exchange_type="topic", durable=True)
channel.queue_declare(queue="order_processing_queue", durable=True)
channel.queue_bind(
    exchange="orders", queue="order_processing_queue", routing_key="order.*"
)

# Optional: ensure fair dispatch (processes 1 message at a time per worker)
channel.basic_qos(prefetch_count=1)

channel.basic_consume(
    queue="order_processing_queue",
    on_message_callback=payOrder,
    auto_ack=False,
)

logger.success("Worker started")
channel.start_consuming()

logger.complete()