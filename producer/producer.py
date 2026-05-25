import json
import os
import time
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime

BOOTSTRAP_SERVERS        = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
TOPIC                    = os.getenv("KAFKA_TOPIC", "zomato-orders")
MESSAGE_INTERVAL_SECONDS = float(os.getenv("MESSAGE_INTERVAL_SECONDS", "0.5"))
CSV_PATH                 = os.getenv("CSV_PATH", "../data/zomato_dataset_cleaned.csv")


def clean_row(row: dict) -> dict:
    record = {}
    for col, val in row.items():
        col_clean = col.strip()
        if pd.isna(val):
            record[col_clean] = None
        elif isinstance(val, float):
            record[col_clean] = round(val, 2)
        else:
            record[col_clean] = str(val).strip()
    record["event_timestamp"] = datetime.utcnow().isoformat(timespec="seconds")
    return record


def main() -> None:
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks="all",
    )
    df = pd.read_csv(CSV_PATH)
    df.columns = df.columns.str.strip()
    print(f"[Producer] Loaded {len(df):,} records → topic '{TOPIC}' on {BOOTSTRAP_SERVERS}")
    try:
        for idx, row in df.iterrows():
            record = clean_row(row.to_dict())
            key = record.get("Delivery_person_ID", str(idx))
            producer.send(TOPIC, key=key, value=record)
            producer.flush()
            print(json.dumps(record))
            if idx % 100 == 0:
                print(f"[Producer] Sent {idx} records...")
            time.sleep(MESSAGE_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        print("\n[Producer] Stopped by user.")
    finally:
        producer.close()


if __name__ == "__main__":
    main()