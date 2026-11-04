"""
Конфигурация ETL-пайплайна.

Часть пайплайна mysql_replica → local-postgres.
Читает .env, валидирует обязательные поля, отдаёт конфиги PG/MySQL.

Роль в пайплайне:
    - единая точка конфигурации для всех модулей;
    - разделение PG (приёмник) и MySQL (источник);
    - SOURCE_SCHEMA вынесена в .env, чтобы имя схемы источника
      не хардкодилось в SQL.
"""
import os
from dotenv import load_dotenv
from pathlib import Path

# Загружаем .env рядом с этим файлом (или на уровень выше)
_env_candidates = [
    Path(__file__).parent.parent / '.env',  # retail-data-pipelines/.env
    Path(__file__).parent / '.env',
]
for _p in _env_candidates:
    if _p.exists():
        load_dotenv(_p)
        break
else:
    load_dotenv()  # fallback: ищем в CWD


class Config:
    """Конфигурация приложения."""

    # ---------- PostgreSQL (приёмник / мини-DWH) ----------
    POSTGRES_CONFIG = {
        'host': os.getenv('PG_HOST', 'localhost'),
        'port': int(os.getenv('PG_PORT', 5432)),
        'database': os.getenv('PG_DATABASE', 'retail_dwh'),
        'user': os.getenv('PG_USER', 'postgres'),
        'password': os.getenv('PG_PASSWORD', 'postgres'),
    }

    USE_POSTGRES = os.getenv('USE_POSTGRES', 'true').lower() == 'true'

    # ---------- MySQL (источник / реплика) ----------
    MYSQL_CONFIG = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', 3306)),
        'user': os.getenv('DB_USER', ''),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_NAME', 'cods'),
        'charset': 'utf8mb4',
    }

    # Имя схемы источника в MySQL-реплике.
    # Вынесено в .env, чтобы не хардкодить в SQL.
    SOURCE_SCHEMA = os.getenv('SOURCE_SCHEMA', 'mysql_replica')

    # ---------- SQLite (legacy, для локальных экспериментов) ----------
    SQLITE_DB = os.getenv('SQLITE_DB', 'retail_local.db')

    @classmethod
    def validate(cls):
        """Проверяет, что обязательные поля MySQL заполнены."""
        required = ['host', 'user', 'password']
        for key in required:
            if not cls.MYSQL_CONFIG[key]:
                raise ValueError(f"В .env отсутствует: {key}")
        return True


# Точка импорта: from pipeline.config import config
config = Config()


if __name__ == "__main__":
    try:
        config.validate()
        print("✅ Конфигурация загружена успешно")
        print(f"   MySQL host: {config.MYSQL_CONFIG['host']}")
        print(f"   MySQL user: {config.MYSQL_CONFIG['user']}")
        print(f"   MySQL password: {'*' * len(config.MYSQL_CONFIG['password'])}")
        print(f"   MySQL schema: {config.SOURCE_SCHEMA}")
        print(f"   PG host: {config.POSTGRES_CONFIG['host']}")
        print(f"   PG database: {config.POSTGRES_CONFIG['database']}")
    except Exception as e:
        print(f"❌ Ошибка конфигурации: {e}")