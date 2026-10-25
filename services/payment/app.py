import json
import os
import sys
import time
import pika
from loguru import logger

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")

def get_payment_details():

  fname = "payment.json"

  try:
    with open(fname, "r", encoding="utf-8") as file:
      payment = json.load(file)
  except OSError:
    logger.error(f"Could not open/read file: {fname}")
    sys.exit(f"Could not open/read file: {fname}")
  except json.JSONDecodeError:
    logger.error(f"File contains invalid JSON: {fname}")
    sys.exit(f"File contains invalid JSON: {fname}")

  logger.info("Payment details read")
  return payment



def payOrder(ch, method, properties, body):
  try:
    event_data = json.loads(body.decode("utf-8"))
  except (json.JSONDecodeError, UnicodeDecodeError) as e:
    logger.error("Failed to decode message")
    # Reject message without requeueing corrupted payload
    ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    return

  order_payload = event_data.get("payload", event_data)
  order_metadata = event_data.get("metadata", event_data)
  order_id = order_payload["order_id"]
  event_id = order_metadata["event_id"]
  logger.opt(colors=True).success(
    f"Order Received <- <bold><green>ID: {order_id}</green></bold> "
      f"[EventID: <dim>{event_id}</dim>]"
  )

  payment = get_payment_details()

  order_payload["payment"] = payment

  order_payload["status"] = "PAYED"

  logger.success("payment Done")

  # Acknowledge completion
  ch.basic_ack(delivery_tag=method.delivery_tag)


if __name__ == "__main__":

  logger.remove()
  logger.add(sys.stdout, colorize=True)
  logger.add(
      "/var/log/app/system.log",
      format="{time:YYYY-MM-DD HH:mm:ss.SSS} | <blue>[PAYMENT-SERVICE]</blue> | <level>{level: <8}</level> | {message}",
      colorize=True,
      enqueue=False
  )
  
  # Connect with retry loop
  while True:
    try:
      connection = pika.BlockingConnection(
          pika.ConnectionParameters(host=RABBITMQ_HOST)
      )
      logger.success("Established connection with RabbitMQ")
      break
    except pika.exceptions.AMQPConnectionError:
      logger.info("Waiting for RabitMQ")
      time.sleep(5)

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
