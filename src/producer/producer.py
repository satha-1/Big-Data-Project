import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timedelta, timezone

from kafka import KafkaProducer


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [producer] %(message)s",
)
logger = logging.getLogger(__name__)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


MERCHANT_CATEGORIES = [
    "grocery",
    "fuel",
    "electronics",
    "travel",
    "restaurants",
    "fashion",
    "gaming",
    "utilities",
]

LOCATIONS = [
    ("US", "New York"),
    ("US", "San Francisco"),
    ("GB", "London"),
    ("DE", "Berlin"),
    ("FR", "Paris"),
    ("IN", "Bengaluru"),
    ("SG", "Singapore"),
    ("AU", "Sydney"),
    ("AE", "Dubai"),
    ("BR", "Sao Paulo"),
]


def build_txn(
    user_id: str,
    event_time: datetime,
    *,
    amount: float,
    merchant_category: str,
    country: str,
    city: str,
) -> dict:
    return {
        "txn_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_time": iso_utc(event_time),
        "merchant_category": merchant_category,
        "amount": float(amount),
        "country": country,
        "city": city,
    }


def wait_for_kafka(bootstrap_servers: str, timeout_s: int = 180) -> KafkaProducer:
    start = time.time()
    last_err = None
    while time.time() - start < timeout_s:
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                acks="all",
                linger_ms=10,
                retries=5,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            # Force a metadata fetch
            _ = producer.bootstrap_connected()
            logger.info("Connected to Kafka at %s", bootstrap_servers)
            return producer
        except Exception as e:  # noqa: BLE001
            last_err = e
            logger.warning("Kafka not ready yet (%s). Retrying...", repr(e))
            time.sleep(3)
    raise RuntimeError(f"Kafka not reachable after {timeout_s}s: {last_err!r}")


def main() -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    topic = os.getenv("RAW_TOPIC", "transactions_raw")
    txns_per_sec = int(os.getenv("TXNS_PER_SEC", "5"))
    fraud_every_n = int(os.getenv("FRAUD_EVERY_N", "20"))
    seed = int(os.getenv("PRODUCER_SEED", "42"))

    rng = random.Random(seed)

    users = [f"user_{i:03d}" for i in range(1, 201)]

    producer = wait_for_kafka(bootstrap)

    produced = 0
    injected_high_spend = 0
    injected_impossible_travel = 0

    logger.info(
        "Starting producer: topic=%s txns_per_sec=%s fraud_every_n=%s seed=%s",
        topic,
        txns_per_sec,
        fraud_every_n,
        seed,
    )

    while True:
        second_start = time.time()

        for i in range(txns_per_sec):
            produced += 1
            now = utc_now()

            user_id = rng.choice(users)
            merchant_category = rng.choice(MERCHANT_CATEGORIES)
            (country, city) = rng.choice(LOCATIONS)

            # Baseline amount distribution: mostly small, occasional medium
            base = rng.random()
            if base < 0.85:
                amount = rng.uniform(5, 200)
            elif base < 0.98:
                amount = rng.uniform(200, 1200)
            else:
                amount = rng.uniform(1200, 4000)

            txn = build_txn(
                user_id=user_id,
                event_time=now,
                amount=amount,
                merchant_category=merchant_category,
                country=country,
                city=city,
            )

            # Inject HIGH_SPEND exactly: amount > 5000
            if fraud_every_n > 0 and produced % fraud_every_n == 0:
                txn["amount"] = float(rng.uniform(6000, 15000))
                injected_high_spend += 1

            # Inject IMPOSSIBLE_TRAVEL: same user, different countries within 10 minutes (event time).
            # We emit a paired transaction right after this one.
            if fraud_every_n > 0 and produced % (fraud_every_n * 2) == 0:
                user_it = rng.choice(users)
                (c1, city1) = rng.choice(LOCATIONS)
                (c2, city2) = rng.choice([x for x in LOCATIONS if x[0] != c1])
                t1 = now - timedelta(seconds=rng.randint(0, 60))
                t2 = t1 + timedelta(seconds=rng.randint(10, 120))  # within 2 minutes
                txn1 = build_txn(
                    user_id=user_it,
                    event_time=t1,
                    amount=float(rng.uniform(20, 250)),
                    merchant_category=rng.choice(MERCHANT_CATEGORIES),
                    country=c1,
                    city=city1,
                )
                txn2 = build_txn(
                    user_id=user_it,
                    event_time=t2,
                    amount=float(rng.uniform(20, 250)),
                    merchant_category=rng.choice(MERCHANT_CATEGORIES),
                    country=c2,
                    city=city2,
                )

                producer.send(topic, txn1)
                producer.send(topic, txn2)
                produced += 2
                injected_impossible_travel += 1

            producer.send(topic, txn)

        producer.flush(timeout=10)

        if produced % max(1, txns_per_sec * 10) == 0:
            logger.info(
                "Produced=%s high_spend_injected=%s impossible_travel_pairs=%s",
                produced,
                injected_high_spend,
                injected_impossible_travel,
            )

        elapsed = time.time() - second_start
        sleep_s = max(0.0, 1.0 - elapsed)
        time.sleep(sleep_s)


if __name__ == "__main__":
    main()

