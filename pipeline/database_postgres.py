"""
Слой доступа к БД: PostgreSQL (приёмник) и MySQL (источник).

Часть пайплайна mysql_replica → local-postgres.

Роль в пайплайне:
    - контекстные менеджеры подключений к PostgreSQL и MySQL;
    - DDL staging-слоя: parsed_checks, etl_state, индексы;
    - батч-вставка через execute_values + ON CONFLICT DO NOTHING
      (идемпотентность);
    - чекпоинты (etl_state) для resume-логики;
    - статистика по загруженным данным.

Источник — схема mysql_replica в старой MySQL-реплике.
Полная трансформация и хранение — в локальном PostgreSQL (ELT).
"""
import psycopg2
import mysql.connector
from psycopg2.extras import execute_values
from contextlib import contextmanager


class PostgresManager:
    """Менеджер для PostgreSQL (приёмник) и MySQL (источник)."""

    def __init__(self, config):
        self.config = config

    # ---------- Подключения ----------

    @contextmanager
    def pg_connection(self):
        """Контекстный менеджер для PostgreSQL."""
        conn = None
        try:
            conn = psycopg2.connect(**self.config.POSTGRES_CONFIG)
            yield conn
        finally:
            if conn:
                conn.close()

    @contextmanager
    def mysql_connection(self):
        """Контекстный менеджер для MySQL-реплики (источник)."""
        conn = None
        try:
            conn = mysql.connector.connect(**self.config.MYSQL_CONFIG)
            yield conn
        finally:
            if conn:
                conn.close()

    # ---------- DDL ----------

    def init_tables(self):
        """Инициализирует таблицы staging-слоя в PostgreSQL."""
        print("🏗️  Создание таблиц в PostgreSQL...")

        with self.pg_connection() as conn:
            cursor = conn.cursor()

            # Проверим, какие таблицы уже есть
            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
            """)
            existing_tables = [row[0] for row in cursor.fetchall()]
            print(f"   Существующие таблицы: {existing_tables}")

            # parsed_checks — нормализованные чеки
            print("   Создаем таблицу parsed_checks...")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS parsed_checks (
                    id BIGSERIAL PRIMARY KEY,
                    mysql_check_id VARCHAR(100) UNIQUE,
                    client_id BIGINT,
                    check_date TIMESTAMP,
                    all_skus TEXT,
                    sku_count INTEGER,
                    location_id INTEGER,
                    coupon_rule_id INTEGER,
                    raw_json_preview TEXT,
                    total NUMERIC(20,2),
                    check_number VARCHAR(50),
                    check_type INTEGER,
                    discount_used NUMERIC(20,2),
                    bonus_used NUMERIC(20,2),
                    bonus_issued NUMERIC(20,2),
                    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # etl_state — чекпоинт (одна строка)
            print("   Создаем таблицу etl_state...")
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS etl_state (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    last_processed_date TIMESTAMP,
                    last_processed_id BIGINT,
                    total_processed BIGINT DEFAULT 0,
                    total_inserted BIGINT DEFAULT 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT single_row CHECK (id = 1)
                )
            ''')

            cursor.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'public'
                  AND table_name IN ('parsed_checks', 'etl_state')
            """)
            created_tables = [row[0] for row in cursor.fetchall()]
            print(f"   Созданные таблицы: {created_tables}")

            # Индексы под аналитику
            print("   Создаем индексы...")
            indexes = [
                ('idx_check_date', 'parsed_checks(check_date)'),
                ('idx_client_date', 'parsed_checks(client_id, check_date)'),
                ('idx_client_id', 'parsed_checks(client_id)'),
                ('idx_location', 'parsed_checks(location_id)'),
            ]
            for index_name, index_def in indexes:
                try:
                    cursor.execute(
                        f'CREATE INDEX IF NOT EXISTS {index_name} ON {index_def}'
                    )
                    print(f"     ✅ {index_name}")
                except Exception as e:
                    print(f"     ❌ {index_name}: {e}")

            conn.commit()
            print("✅ Таблицы созданы")

    # ---------- ETL state ----------

    def get_etl_state(self):
        """Получает текущее состояние ETL (или None)."""
        with self.pg_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM etl_state WHERE id = 1")
            row = cursor.fetchone()
            if row:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))
            return None

    def save_etl_state(self, last_date=None, last_id=None,
                       processed=0, inserted=0):
        """Сохраняет состояние ETL (upsert одной строки)."""
        with self.pg_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO etl_state
                (id, last_processed_date, last_processed_id,
                 total_processed, total_inserted)
                VALUES (1, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE SET
                    last_processed_date = EXCLUDED.last_processed_date,
                    last_processed_id = EXCLUDED.last_processed_id,
                    total_processed = EXCLUDED.total_processed,
                    total_inserted = EXCLUDED.total_inserted,
                    updated_at = CURRENT_TIMESTAMP
            ''', (last_date, last_id, processed, inserted))
            conn.commit()

    def reset_etl_state(self):
        """Сбрасывает состояние ETL (начать заново)."""
        with self.pg_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("TRUNCATE TABLE etl_state, parsed_checks RESTART IDENTITY")
            conn.commit()

    # ---------- Extract из MySQL ----------

    def get_checks_batch(self, last_date=None, last_id=None, limit=1000):
        """
        Получает следующую пачку чеков из MySQL-реплики.

        Инкрементальная выборка по (date, id):
            (date > :last_date) OR (date = :last_date AND id > :last_id)
        ORDER BY date, id — чтобы чекпоинт был монотонным.

        check_type = 1 — только покупки (0 = возврат).
        """
        with self.mysql_connection() as conn:
            cursor = conn.cursor(dictionary=True)

            query = f"""
                SELECT
                    id,
                    client_id,
                    date,
                    context_json,
                    location_id,
                    position_count,
                    total,
                    check_number,
                    check_type,
                    discount_used,
                    bonus_used,
                    bonus_issued
                FROM {self.config.SOURCE_SCHEMA}.check_context_storage
                WHERE check_type = 1
            """

            params = []
            if last_date and last_id:
                query += " AND (date > %s OR (date = %s AND id > %s))"
                params.extend([last_date, last_date, last_id])
            elif last_date:
                query += " AND date >= %s"
                params.append(last_date)

            query += " ORDER BY date ASC, id ASC LIMIT %s"
            params.append(limit)

            cursor.execute(query, params)
            return cursor.fetchall()

    # ---------- Load в PostgreSQL ----------

    def save_checks_batch(self, checks_data):
        """
        Сохраняет пачку чеков в PostgreSQL staging.

        Идемпотентность: UNIQUE(mysql_check_id) + ON CONFLICT DO NOTHING.
        Повторная обработка батча безопасна.
        """
        if not checks_data:
            return 0

        with self.pg_connection() as conn:
            cursor = conn.cursor()

            query = '''
                INSERT INTO parsed_checks
                (mysql_check_id, client_id, check_date, all_skus, sku_count,
                 location_id, coupon_rule_id, raw_json_preview,
                 total, check_number, check_type,
                 discount_used, bonus_used, bonus_issued)
                VALUES %s
                ON CONFLICT (mysql_check_id) DO NOTHING
            '''

            execute_values(cursor, query, checks_data, page_size=10000)
            inserted = cursor.rowcount
            conn.commit()

            return inserted

    # ---------- Stats ----------

    def get_stats(self):
        """Статистика по загруженным данным."""
        with self.pg_connection() as conn:
            cursor = conn.cursor()

            cursor.execute("SELECT COUNT(*) FROM parsed_checks")
            total = cursor.fetchone()[0]

            cursor.execute("SELECT MIN(check_date), MAX(check_date) FROM parsed_checks")
            result = cursor.fetchone()
            min_date, max_date = result if result else (None, None)

            cursor.execute("SELECT COUNT(DISTINCT DATE(check_date)) FROM parsed_checks")
            days_count = cursor.fetchone()[0] if cursor.rowcount > 0 else 0

            return {
                'total_checks': total,
                'min_date': min_date,
                'max_date': max_date,
                'days_count': days_count,
            }