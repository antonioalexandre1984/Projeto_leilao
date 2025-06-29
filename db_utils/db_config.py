import os

# --- Configurações do PostgreSQL ---
# Lendo as variáveis de ambiente que serão passadas pelo Docker Compose
# Isso garante que as credenciais não estejam "hardcoded" diretamente no código.
DB_HOST = os.getenv("PG_HOST", "db")  # Host do PostgreSQL (no Docker Compose, será 'db')
DB_NAME = os.getenv("PG_DATABASE", "base_leilao") # Nome do banco de dados
DB_USER = os.getenv("PG_USER", "root")     # Usuário do banco de dados
DB_PASSWORD = os.getenv("PG_PASSWORD", "root") # Senha do usuário