import fcntl
import json
import os
import sys
import time

import pika

QUEUE_NAME = "orders"
HOST = "localhost"
PORT = 5672
WORKER_NAME = os.environ.get("WORKER_NAME", "worker-default")
CRASH_BEFORE_ACK = os.environ.get("CRASH_BEFORE_ACK") == "1"
PROCESSING_DELAY_SECONDS = 1
PROCESSED_EVENT_IDS_PATH = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "processed_event_ids.txt",
)


def read_processed_event_ids(handle):
    handle.seek(0)
    return {line.strip() for line in handle if line.strip()}


def on_message(channel, method, properties, body):
    try:
        message = json.loads(body)
    except json.JSONDecodeError:
        print(f"[{WORKER_NAME}] Invalid JSON. Message was not processed: {body!r}")
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
        return

    event_id = message.get("event_id")
    order_id = message.get("order_id")
    item_id = message.get("item_id")
    quantity = message.get("quantity")
    event_key = "" if event_id is None else str(event_id)

    with open(PROCESSED_EVENT_IDS_PATH, "a+", encoding="utf-8") as handle:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        try:
            if event_key in read_processed_event_ids(handle):
                is_duplicate = True
            else:
                is_duplicate = False
                print(
                    f"[{WORKER_NAME}] Processing order "
                    f"event_id={event_id} order_id={order_id} "
                    f"item_id={item_id} quantity={quantity}"
                )
                if CRASH_BEFORE_ACK:
                    print(
                        f"[{WORKER_NAME}] event_id={event_id} "
                        "Simulating crash before ACK."
                    )
                    sys.exit(1)

                print(f"[{WORKER_NAME}] redelivered={method.redelivered}")
                time.sleep(PROCESSING_DELAY_SECONDS)
                handle.seek(0, os.SEEK_END)
                handle.write(f"{event_key}\n")
                handle.flush()
                os.fsync(handle.fileno())
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)

    if is_duplicate:
        print(f"[{WORKER_NAME}] Duplicate detected event_id={event_id}")
        channel.basic_ack(delivery_tag=method.delivery_tag)
        print(f"[{WORKER_NAME}] Acknowledged duplicate event_id={event_id}")
        return

    channel.basic_ack(delivery_tag=method.delivery_tag)
    print(f"[{WORKER_NAME}] Acknowledged event_id={event_id}")


def consume_orders():
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=HOST, port=PORT)
    )
    try:
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=QUEUE_NAME,
            on_message_callback=on_message,
            auto_ack=False,
        )
        print(f"[{WORKER_NAME}] Waiting for messages. Press Ctrl+C to stop.")
        try:
            channel.start_consuming()
        except KeyboardInterrupt:
            print(f"[{WORKER_NAME}] Stopping.")
            channel.stop_consuming()
    finally:
        try:
            if connection.is_open:
                connection.close()
        except Exception:
            pass


if __name__ == "__main__":
    consume_orders()
