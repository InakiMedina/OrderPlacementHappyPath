import json
import os
import sys
import time
import pika
import socket
import uuid
from loguru import logger
from datetime import datetime, timezone

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

def sendPaymentEvent(ch, order):

  event_id = str(uuid.uuid4())
  timestamp = datetime.now(timezone.utc).isoformat()

  payment_payload = {
      "metadata": {
          "event_id": event_id,
          "event_type": "PaymentApplied",
          "producer": "payment-service",
          "timestamp": timestamp,
      },
      "payload": order,  
  }

  ch.basic_publish(
      exchange="payment",
      routing_key="payment.applied",
      body=json.dumps(payment_payload),
      properties=pika.BasicProperties(delivery_mode=2)
  )

  order_id = order.get("order_id", "N/A")
  logger.opt(colors=True).success(
    f"Payment Dispatched -> <bold><green>ID: {order_id}</green></bold> "
    f"[EventID: <dim>{event_id}</dim>]"
  )

def payOrder(ch, method, body):
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

  # Acknowledge completion
  ch.basic_ack(delivery_tag=method.delivery_tag)
  logger.success("payment Done")

  return order_payload


if __name__ == "__main__":

  RABBITMQ_IN_HOST = os.getenv("RABBITMQ_IN_HOST", "order-placed")
  RABBITMQ_OUT_HOST = os.getenv("RABBITMQ_OUT_HOST", "payment-applied")

  logger.remove()
  logger.add(sys.stdout, colorize=True)
  logger.add(
      "/var/log/app/system.log",
      format="{time:YYYY-MM-DD HH:mm:ss.SSS} | <blue>[PAYMENT-SERVICE]</blue> | <level>{level: <8}</level> | {message}",
      colorize=True,
      enqueue=False
  )

  params_in = pika.ConnectionParameters(host=RABBITMQ_IN_HOST)
  params_out = pika.ConnectionParameters(host=RABBITMQ_OUT_HOST)
  
  # Connect with retry loop
  def connectBroker(params, name):
    while True:
      try:
        conn = pika.BlockingConnection(params)
        logger.success(f"Established connection with {name} at {params.host}")
        return conn
      except (pika.exceptions.AMQPConnectionError, socket.gaierror, OSError):
        logger.info(f"Waiting for {name} ({params.host})...")
        time.sleep(5)

  connIn = connectBroker(params_in, "Order-Placed-Broker")
  connOut = connectBroker(params_out, "Payment-Applied-Broker")

  channelOP = connIn.channel()
  channelPA = connOut.channel()

  # Setup up order processed channel
  channelOP.exchange_declare(exchange="orders", exchange_type="topic", durable=True)
  channelOP.queue_declare(queue="order_placed_queue", durable=True)
  channelOP.queue_bind(
      exchange="orders", queue="order_placed_queue", routing_key="order.*"
  )

  # Optional: ensure fair dispatch (processes 1 message at a time per worker)
  channelOP.basic_qos(prefetch_count=1)

    
  # Setup up payment applied channel
  channelPA.exchange_declare(exchange="payment", exchange_type="topic", durable=True)

  channelPA.queue_declare(queue="payment_applied_queue", durable=True)
  channelPA.queue_bind(
      exchange="payment", queue="payment_applied_queue", routing_key="payment.*"
  )


  logger.success("Worker started")

  while True: 

    for method, properties, body in channelOP.consume(
      queue="order_placed_queue"
    ):

      order = payOrder(channelOP, method, body)

      if order is not None:
        sendPaymentEvent(channelPA, order)
    
