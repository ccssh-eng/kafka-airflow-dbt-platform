import json
import os
import logging
import time
from pathlib import Path

import pandas as pd
from kafka import KafkaProducer
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("producer")

DATA_DIR = Path("/app/data")
ORDERS_FILE = DATA_DIR / "commandes.xlsx"
CLIENTS_FILE = DATA_DIR / "clients.xlsx"
DEBOUNCE_SECONDS = 3

producer = KafkaProducer(
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    api_version=(3, 9),
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=os.environ["KAFKA_SASL_USERNAME"],
    sasl_plain_password=os.environ["KAFKA_SASL_PASSWORD"],
    ssl_cafile="/app/certs/ca-cert.pem",
    value_serializer=lambda v: json.dumps(v, default=str).encode(),
)


def load_and_merge():
    orders_df = pd.read_excel(ORDERS_FILE)
    clients_df = pd.read_excel(CLIENTS_FILE)

    missing = set(orders_df["customer_id"]) - set(clients_df["customer_id"])
    if missing:
        log.warning(f"customer_id sans fiche client dans clients.xlsx : {sorted(missing)}")

    return orders_df.merge(clients_df, on="customer_id", how="left")


def build_message(row):
    return {
        "order_id": int(row["order_id"]),
        "customer_id": int(row["customer_id"]),
        "amount": float(row["amount"]),
        "status": row["status"],
        "order_date": row["order_date"].isoformat() if pd.notna(row["order_date"]) else None,
        "customer_name": row["nom"] if pd.notna(row["nom"]) else None,
        "region": row["region"] if pd.notna(row["region"]) else None,
        "email": row["email"] if pd.notna(row["email"]) else None,
    }


def process_files():
    if not ORDERS_FILE.exists() or not CLIENTS_FILE.exists():
        log.info("En attente des deux fichiers (commandes.xlsx et clients.xlsx)...")
        return
    log.info("Nouveau depot detecte, traitement en cours...")
    try:
        merged_df = load_and_merge()
    except Exception as exc:
        log.error(f"Echec de lecture/fusion des fichiers Excel : {exc}")
        return

    count = 0
    for _, row in merged_df.iterrows():
        producer.send("orders", value=build_message(row))
        count += 1
    producer.flush()
    log.info(f"{count} commandes envoyees vers Kafka.")


class DropzoneHandler(FileSystemEventHandler):
    def __init__(self):
        self._last_triggered = 0.0

    def _maybe_trigger(self, path):
        if Path(path).name not in {ORDERS_FILE.name, CLIENTS_FILE.name}:
            return
        now = time.time()
        if now - self._last_triggered < DEBOUNCE_SECONDS:
            return
        self._last_triggered = now
        time.sleep(DEBOUNCE_SECONDS)  # laisse le temps au fichier de finir de s'écrire/copier
        process_files()

    def on_created(self, event):
        if not event.is_directory:
            self._maybe_trigger(event.src_path)

    def on_modified(self, event):
        if not event.is_directory:
            self._maybe_trigger(event.src_path)


def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    process_files()  # traitement initial si les fichiers sont deja presents

    observer = Observer()
    observer.schedule(DropzoneHandler(), str(DATA_DIR), recursive=False)
    observer.start()
    log.info(f"En surveillance de {DATA_DIR} pour de nouveaux depots...")
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == "__main__":
    main()

