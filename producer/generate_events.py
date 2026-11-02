"""Produce synthetic sales, with optional bad-record scenarios for CI tests."""
import argparse
from datetime import datetime, timedelta, timezone
import json
from kafka import KafkaProducer


def event_for(i: int) -> dict:
    if i < 1:
        raise ValueError("Event index must be positive")
    return {
        "event_id": f"synthetic-{i:08d}",
        "event_time": (datetime(2026, 1, 1, tzinfo=timezone.utc)
                       + timedelta(seconds=i)).isoformat(),
        "store_id": f"store-{i % 5 + 1}",
        "product_id": f"sku-{i % 25 + 1}",
        "quantity": i % 4 + 1,
        "unit_price": float((i % 10 + 1) * 2),
    }


def adversarial_events() -> list:
    """Duplicate a legitimate event and add invalid records to test filtering.

    None of these should survive the Spark deduplication / validation sink.
    These are intentionally synthetic negative test cases.
    """
    duplicate = event_for(1)
    bad_quantity = {**event_for(2), "event_id": "bad-negative-quantity", "quantity": -4}
    missing_id = {**event_for(3), "event_id": None}
    bad_payload = "this is not a JSON object"
    blank_id = {**event_for(4), "event_id": "   "}
    bad_price = {**event_for(5), "event_id": "bad-infinite-price", "unit_price": float("inf")}
    return [duplicate, bad_quantity, missing_id, bad_payload, blank_id, bad_price]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--bootstrap", default="localhost:9092")
    parser.add_argument("--adversarial", action="store_true",
                        help="Also send duplicate and invalid synthetic events")
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count must be positive")
    if args.adversarial and args.count < 3:
        parser.error("--adversarial requires at least three valid events")

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda row: json.dumps(row).encode("utf-8"),
        acks="all",
        retries=5,
    )
    try:
        for i in range(1, args.count + 1):
            producer.send("sales-events", value=event_for(i)).get(timeout=30)
        if args.adversarial:
            for invalid in adversarial_events():
                producer.send("sales-events", value=invalid).get(timeout=30)
        producer.flush()
    finally:
        producer.close()
    injected = len(adversarial_events()) if args.adversarial else 0
    print(f"Delivered {args.count} valid and {injected} adversarial synthetic events")


if __name__ == "__main__":
    main()
