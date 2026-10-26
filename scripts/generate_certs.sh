#!/bin/bash
set -e
rm -rf certs
mkdir -p certs/kafka certs/nginx

# --- CA commune ---
openssl req -new -x509 -keyout certs/ca-key.pem -out certs/ca-cert.pem -days 3650 \
  -subj "/CN=kafka-airflow-dbt-CA" -passout pass:${CA_PASSWORD}

# --- Certificat Kafka (keystore + truststore) ---
keytool -keystore certs/kafka/kafka.keystore.jks -alias kafka -validity 3650 -genkey \
  -keyalg RSA -storepass ${KAFKA_SSL_KEYSTORE_PASSWORD} -keypass ${KAFKA_SSL_KEY_PASSWORD} \
  -dname "CN=kafka, OU=dev, O=kafka-airflow-dbt" -ext SAN=DNS:kafka,DNS:localhost

keytool -keystore certs/kafka/kafka.keystore.jks -alias kafka -certreq -file certs/kafka/cert-file \
  -storepass ${KAFKA_SSL_KEYSTORE_PASSWORD}

openssl x509 -req -CA certs/ca-cert.pem -CAkey certs/ca-key.pem -in certs/kafka/cert-file \
  -out certs/kafka/cert-signed -days 3650 -CAcreateserial -passin pass:${CA_PASSWORD}

keytool -keystore certs/kafka/kafka.keystore.jks -alias CARoot -import -file certs/ca-cert.pem \
  -storepass ${KAFKA_SSL_KEYSTORE_PASSWORD} -noprompt
keytool -keystore certs/kafka/kafka.keystore.jks -alias kafka -import -file certs/kafka/cert-signed \
  -storepass ${KAFKA_SSL_KEYSTORE_PASSWORD} -noprompt

keytool -keystore certs/kafka/kafka.truststore.jks -alias CARoot -import -file certs/ca-cert.pem \
  -storepass ${KAFKA_SSL_TRUSTSTORE_PASSWORD} -noprompt

# --- Fichier JAAS pour l'authentification SASL/SCRAM ---
cat > certs/kafka/kafka_server_jaas.conf <<'JAAS'
KafkaServer {
   org.apache.kafka.common.security.scram.ScramLoginModule required;
};
JAAS

# --- Certificat nginx (PEM, pour Airflow) ---
openssl req -new -x509 -keyout certs/nginx/airflow.key -out certs/nginx/airflow.crt -days 3650 \
  -nodes -subj "/CN=airflow.local"

echo "Certificats generes dans ./certs/"

