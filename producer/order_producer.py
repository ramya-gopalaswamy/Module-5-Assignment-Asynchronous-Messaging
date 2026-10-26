import argparse
import json
import uuid

import pika

QUEUE_NAME = "orders"
HOST = "localhost"
PORT = 5672

SAMPLE_ORDERS = [
    {"order_id": "order-1001", "item_id": "item-501", "quantity": 2},
    {"order_id": "order-1002", "item_id": "item-742", "quantity": 1},
    {"order_id": "order-1003", "item_id": "item-318", "quantity": 4},
]


def parse_args():
    parser = argparse.ArgumentParser(
        description="Publish order messages to the orders queue."
    )
    parser.add_argument("--event-id")
    parser.add_argument("--order-id")
    parser.add_argument("--item-id")
    parser.add_argument("--quantity", type=int)
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Times to publish one fixed message. Default is 1.",
    )
    return parser.parse_args()


def build_messages(args):
    fields = {
        "event_id": args.event_id,
        "order_id": args.order_id,
        "item_id": args.item_id,
        "quantity": args.quantity,
    }
    supplied = [name for name, value in fields.items() if value is not None]
    if not supplied:
        if args.count != 1:
            raise SystemExit(
                "--count applies only when a fixed event_id is supplied. "
                "Provide --event-id, --order-id, --item-id, and --quantity."
            )
        return [
            {
                "event_id": str(uuid.uuid4()),
                "order_id": order["order_id"],
                "item_id": order["item_id"],
                "quantity": order["quantity"],
            }
            for order in SAMPLE_ORDERS
        ]

    missing = [name for name, value in fields.items() if value is None]
    if missing:
        formatted = ", ".join(f"--{name.replace('_', '-')}" for name in missing)
        raise SystemExit(
            "A fixed message requires --event-id, --order-id, --item-id, "
            f"and --quantity. Missing: {formatted}"
        )
    if args.count < 1:
        raise SystemExit("--count must be at least 1.")

    message = {
        "event_id": args.event_id,
        "order_id": args.order_id,
        "item_id": args.item_id,
        "quantity": args.quantity,
    }
    return [dict(message) for _ in range(args.count)]


def publish_orders(messages):
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=HOST, port=PORT)
    )
    try:
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        for message in messages:
            body = json.dumps(message)
            channel.basic_publish(
                exchange="",
                routing_key=QUEUE_NAME,
                body=body,
                properties=pika.BasicProperties(
                    content_type="application/json",
                    delivery_mode=pika.DeliveryMode.Persistent,
                ),
            )
            print(f"Published: {body}")
    finally:
        connection.close()


if __name__ == "__main__":
    publish_orders(build_messages(parse_args()))
