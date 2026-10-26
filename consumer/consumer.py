import json
import os

import psycopg2
from kafka import KafkaConsumer

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "airflow")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "airflow")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "airflow")

# Connexion à Kafka
consumer = KafkaConsumer(
    "orders",
    bootstrap_servers=os.environ["KAFKA_BOOTSTRAP_SERVERS"],
    group_id="orders-consumer-v2",
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    api_version=(3, 9),  # évite l'auto-détection qui échoue parfois avec Kafka 4.x
    security_protocol="SASL_SSL",
    sasl_mechanism="SCRAM-SHA-256",
    sasl_plain_username=os.environ["KAFKA_SASL_USERNAME"],
    sasl_plain_password=os.environ["KAFKA_SASL_PASSWORD"],
    ssl_cafile="/app/certs/ca-cert.pem",
    value_deserializer=lambda value: json.loads(value.decode("utf-8")),
)


# Connexion à PostgreSQL
connection = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
)

cursor = connection.cursor()

print("Consumer started. Waiting for orders...")


for message in consumer:
    order = message.value

    print(f"Received order: {order}")

    cursor.execute(
        """
        INSERT INTO orders (
            order_id,
            customer_id,
            amount,
            status,
            order_date,
            customer_name,
            region,
            email
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (order_id) DO UPDATE SET
            amount = EXCLUDED.amount,
            status = EXCLUDED.status,
            order_date = EXCLUDED.order_date,
            customer_name = EXCLUDED.customer_name,
            region = EXCLUDED.region,
            email = EXCLUDED.email;
        """,
        (
            order["order_id"],
            order["customer_id"],
            order["amount"],
            order["status"],
            order.get("order_date"),
            order.get("customer_name"),
            order.get("region"),
            order.get("email"),
        ),
    )

    connection.commit()

    if cursor.rowcount == 1:
        print(
            f"Order {order['order_id']} inserted into PostgreSQL"
        )
    else:
        print(
            f"Order {order['order_id']} already exists - skipped"
        )

    consumer.commit()
