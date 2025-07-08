# parque/db_utils/db_config.py
import os

DB_HOST = os.getenv("PG_HOST", "db") # Use PG_HOST para ser consistente com .env
DB_NAME = os.getenv("PG_DATABASE", "base_leilao")
DB_USER = os.getenv("PG_USER", "root")
DB_PASSWORD = os.getenv("PG_PASSWORD", "root")
DB_PORT = os.getenv("PG_PORT", "5432")