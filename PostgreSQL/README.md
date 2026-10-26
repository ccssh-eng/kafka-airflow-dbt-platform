Plateforme Streaming Kafka — Airflow — dbt
Pipeline de données streaming temps réel, entièrement conteneurisé, sécurisé et auto-résilient.
Architecture
```
[Excel/CSV — producer/data/]
        │
        ▼
producer/producer.py
  Python · pandas · watchdog
        │  SASL/SCRAM-SHA-256 · TLS
        ▼
Apache Kafka (KRaft — sans ZooKeeper)
  Topic : orders
        │
        ▼
consumer/consumer.py
  Upsert idempotent → PostgreSQL 16
        │
        ▼
PostgreSQL/models/staging/    ← dbt staging (nettoyage + typage)
PostgreSQL/models/marts/      ← dbt marts (KPI agrégés)
PostgreSQL/tests/             ← tests qualité dbt
        │
        ▼
airflow/dags/orders_dbt_pipeline.py
  Orchestration toutes les 15 minutes
```
Stack technique
Composant	Rôle
Apache Kafka (KRaft)	Broker de messages sans ZooKeeper
Python	Producer (pandas + watchdog)
PostgreSQL 16	Stockage des données consommées
dbt-core	Transformation et tests qualité
Apache Airflow	Orchestration (schedule 15 min)
Docker Compose	Conteneurisation complète
nginx	Reverse proxy HTTPS
autoheal	Redémarrage automatique des containers
Structure du projet
```
├── producer/               # Producer Kafka (Python)
│   ├── producer.py
│   ├── Dockerfile
│   └── data/               # Fichiers source (Excel)
├── consumer/               # Consumer Kafka → PostgreSQL
│   ├── consumer.py
│   └── Dockerfile
├── PostgreSQL/             # Projet dbt
│   ├── models/
│   │   ├── staging/        # Couche nettoyage + typage
│   │   └── marts/          # Couche KPI agrégés
│   └── tests/              # Tests qualité personnalisés
├── airflow/                # Orchestration
│   ├── dags/
│   │   └── orders_dbt_pipeline.py
│   ├── dbt_profiles/
│   └── Dockerfile
├── kafka/                  # Configuration Kafka JAAS
├── nginx/                  # Reverse proxy HTTPS
├── scripts/                # Génération des certificats TLS
├── docker-compose.yml      # Stack complète
└── .gitignore              # .env et certs/ exclus
```
Sécurité
Authentification Kafka : SASL/SCRAM-SHA-256
Chiffrement : TLS (certificats générés via `scripts/generate_certs.sh`)
Secrets : fichier `.env` exclu du repo (jamais de credentials en clair)
Résilience : autoheal + retries Airflow configurés
Lancement
```bash
# 1. Copier et configurer les variables d'environnement
cp .env.example .env   # éditer avec vos valeurs

# 2. Générer les certificats TLS
bash scripts/generate_certs.sh

# 3. Démarrer la stack complète
docker compose up -d

# 4. Vérifier les containers
docker compose ps
```
DAG Airflow
Le DAG `orders_dbt_pipeline` s'exécute toutes les 15 minutes :
```
check_consumer → dbt_run → dbt_test → log_result
```
Accessible sur : `https://localhost/airflow` (via nginx reverse proxy)

Auteur
Cédric SH — Data Engineer  
Plateforme déployée en production sur VPS avec Docker Compose

