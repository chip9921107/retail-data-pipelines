"""
Точка входа ETL-пайплайна mysql_replica → local-postgres.

Запуск:
    python run_etl.py --batch-size 5000
    python run_etl.py --batch-size 5000 --no-resume
"""
import argparse
import sys

from pipeline.config import config
from pipeline.parsers import SimpleParser
from pipeline.database_postgres import PostgresManager
from pipeline.etl_pipeline import ResumeETL


def main():
    parser = argparse.ArgumentParser(
        description="ETL: MySQL-реплика → локальный PostgreSQL (staging)"
    )
    parser.add_argument(
        "--batch-size", type=int, default=5000,
        help="Размер батча (по умолчанию 5000)",
    )
    parser.add_argument(
        "--no-resume", action="store_true",
        help="Начать с нуля (игнорировать etl_state)",
    )
    args = parser.parse_args()

    print("🔧 Загрузка конфигурации...")
    config.validate()
    print("✅ Конфигурация загружена")
    print(f"   MySQL host: {config.MYSQL_CONFIG['host']}")
    print(f"   MySQL schema: {config.SOURCE_SCHEMA}")
    print(f"   PG database: {config.POSTGRES_CONFIG['database']}")

    db = PostgresManager(config)
    etl = ResumeETL(config=config, db_manager=db, parser=SimpleParser)

    etl.run(batch_size=args.batch_size, resume=not args.no_resume)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n❌ ETL завершился с ошибкой: {e}", file=sys.stderr)
        sys.exit(1)