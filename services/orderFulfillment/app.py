import json
import os
import sys
import time
import pika
import socket
import uuid
from loguru import logger
from datetime import datetime, timezone

if __name__ == "__main__":

	RABBITMQ_IN_HOST = os.getenv("RABBITMQ_IN_HOST", "payment-applied")
	  
	logger.remove()
	logger.add(sys.stdout, colorize=True)
	logger.add(
		"/var/log/app/system.log",
		format="{time:YYYY-MM-DD HH:mm:ss.SSS} | <red>[ORDER-FULFILLMENT-SERVICE]</red> | <level>{level: <8}</level> | {message}",
		colorize=True,
		enqueue=False
	)

	params_in = pika.ConnectionParameters(host=RABBITMQ_IN_HOST)

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
	
	connIn = connectBroker(params_in, "Payment-Applied-Broker")
	
	channelPA = connIn.channel()

	  # Setup up payment applied channel
	channelPA.exchange_declare(exchange="payment", exchange_type="topic", durable=True)

	channelPA.queue_declare(queue="payment_applied_queue", durable=True)
	channelPA.queue_bind(
      	exchange="payment", queue="payment_applied_queue", routing_key="payment.*"
  	)

	channelPA.basic_qos(prefetch_count=1)

	logger.success("Worker started")

	while True: 
	
		for method, properties, body in channelPA.consume(
		  queue="payment_applied_queue"
		):
			if method is None or body is None:
				continue

			try:
				event_data = json.loads(body.decode("utf-8"))
			except (json.JSONDecodeError, UnicodeDecodeError) as e:
				logger.error("Failed to decode message")
				# Reject message without requeueing corrupted payload
				channelPA.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
				break
			
			order_payload = event_data.get("payload", event_data)
			order_metadata = event_data.get("metadata", event_data)
			order_id = order_payload["order_id"]
			event_id = order_metadata["event_id"]

			# full fill order
			logger.opt(colors=True).success(
				f"Order Received <- <bold><green>ID: {order_id}</green></bold> "
				f"[EventID: <dim>{event_id}</dim>]"
			)

			# Acknowledge completion
			channelPA.basic_ack(delivery_tag=method.delivery_tag)
			logger.success("Order Fulfilled")
