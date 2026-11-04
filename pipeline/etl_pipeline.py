"""
Оркестратор ETL: resume-able цикл по батчам.

Часть пайплайна mysql_replica → local-postgres.

Роль в пайплайне:
    - читает чекпоинт из etl_state;
    - в цикле: SELECT батча из MySQL → parse (parsers) → INSERT в Postgres
      (database_postgres) → UPDATE чекпоинта;
    - прогресс через tqdm, промежуточная и финальная статистика;
    - KeyboardInterrupt-safe: при Ctrl+C печатает состояние и выходит,
      не теряя прогресс.
"""
from tqdm import tqdm
from datetime import datetime
import time


class ResumeETL:
    """ETL с возможностью продолжения после остановки."""

    def __init__(self, config, db_manager, parser):
        self.config = config
        self.db = db_manager
        self.parser = parser
        self.stats = {
            'start_time': None,
            'total_processed': 0,
            'total_inserted': 0,
            'batches_processed': 0,
        }

    def run(self, batch_size=1000, resume=True):
        """Запускает или продолжает ETL процесс."""
        print(f"🚀 {'ПРОДОЛЖАЕМ' if resume else 'НАЧИНАЕМ'} ETL: {datetime.now()}")
        print("=" * 60)

        self.db.init_tables()
        state = self.db.get_etl_state() if resume else None

        if state:
            print("📊 Текущее состояние ETL:")
            print(f"   Дата: {state['last_processed_date']}")
            print(f"   ID: {state['last_processed_id']}")
            print(f"   Обработано: {state['total_processed']:,}")
            print(f"   Добавлено: {state['total_inserted']:,}")
            print(f"   Обновлено: {state['updated_at']}")

            confirm = input("\n▶️  Продолжить с этой точки? (y/n): ")
            if confirm.lower() != 'y':
                reset = input("🔄 Сбросить и начать заново? (y/n): ")
                if reset.lower() == 'y':
                    self.db.reset_etl_state()
                    state = None
                    print("♻️  Состояние сброшено, начинаем сначала")
                else:
                    print("⏹️  Операция отменена")
                    return
        else:
            print("🆕 Начинаем с самого начала")

        last_date = state['last_processed_date'] if state else None
        last_id = state['last_processed_id'] if state else None
        self.stats['total_processed'] = state['total_processed'] if state else 0
        self.stats['total_inserted'] = state['total_inserted'] if state else 0
        self.stats['start_time'] = datetime.now()

        batch_num = 0

        try:
            while True:
                batch_num += 1
                self.stats['batches_processed'] += 1

                print(f"\n📦 Батч {batch_num}:")
                checks = self.db.get_checks_batch(last_date, last_id, batch_size)

                if not checks:
                    print("✅ Все чеки обработаны!")
                    break

                print(f"   Загружено: {len(checks)} чеков")
                print(f"   Дата начала батча: {checks[0]['date']}")

                batch_data = []
                batch_start = time.time()

                for check in tqdm(checks, desc="Обработка", unit="чек"):
                    parsed = self.parser.parse_minimal_fields(check['context_json'])

                    batch_data.append((
                        check['id'],
                        check['client_id'],
                        check['date'],
                        parsed['skus'],
                        check.get('position_count', 0),
                        check.get('location_id'),
                        parsed['coupon_rule_id'],
                        check['context_json'][:200] if check['context_json'] else '',
                        float(check.get('total')) if check.get('total') is not None else None,
                        check.get('check_number'),
                        int(check.get('check_type')) if check.get('check_type') is not None else None,
                        float(check.get('discount_used')) if check.get('discount_used') is not None else None,
                        float(check.get('bonus_used')) if check.get('bonus_used') is not None else None,
                        float(check.get('bonus_issued')) if check.get('bonus_issued') is not None else None,
                    ))

                    last_date = check['date']
                    last_id = check['id']

                inserted = self.db.save_checks_batch(batch_data)
                self.stats['total_processed'] += len(checks)
                self.stats['total_inserted'] += inserted

                self.db.save_etl_state(
                    last_date=last_date,
                    last_id=last_id,
                    processed=self.stats['total_processed'],
                    inserted=self.stats['total_inserted'],
                )

                batch_time = time.time() - batch_start
                checks_per_sec = len(checks) / batch_time if batch_time > 0 else 0

                print(f"   ✅ Сохранено: {inserted} новых записей")
                print(f"   📅 Последняя дата: {last_date}")
                print(f"   🆔 Последний ID: {last_id}")
                print(f"   ⚡ Скорость: {checks_per_sec:.1f} чеков/сек")

                if batch_num % 10 == 0:
                    self._print_intermediate_report()

                if batch_num % 50 == 0:
                    time.sleep(2)

        except KeyboardInterrupt:
            print("\n⏸️  ETL приостановлен")
            self._print_final_report(interrupted=True)
        except Exception as e:
            print(f"\n❌ Ошибка: {e}")
            self._print_final_report(interrupted=True)
            raise
        else:
            self._print_final_report(interrupted=False)

    def _print_intermediate_report(self):
        """Печатает промежуточный отчёт."""
        elapsed = datetime.now() - self.stats['start_time']
        hours = elapsed.total_seconds() / 3600

        print(f"\n{'📈' * 30}")
        print("📊 ПРОМЕЖУТОЧНАЯ СТАТИСТИКА:")
        print(f"   Обработано: {self.stats['total_processed']:,}")
        print(f"   Добавлено: {self.stats['total_inserted']:,}")
        print(f"   Пропущено: {self.stats['total_processed'] - self.stats['total_inserted']:,}")
        print(f"   Батчей: {self.stats['batches_processed']}")
        print(f"   Время: {str(elapsed).split('.')[0]}")

        if hours > 0:
            print(f"   Скорость: {self.stats['total_processed'] / hours:.1f} чеков/час")
        print(f"{'📈' * 30}\n")

    def _print_final_report(self, interrupted=False):
        """Печатает финальный отчёт."""
        elapsed = datetime.now() - self.stats['start_time']
        stats = self.db.get_stats()

        print("\n" + "=" * 60)
        print("🏁 ФИНАЛЬНЫЙ ОТЧЁТ" if not interrupted else "⏸️  ТЕКУЩЕЕ СОСТОЯНИЕ")
        print("=" * 60)
        print(f"✅ Обработано чеков: {self.stats['total_processed']:,}")
        print(f"✅ Добавлено записей: {self.stats['total_inserted']:,}")
        print(f"⚠️  Пропущено (дубликаты): "
              f"{self.stats['total_processed'] - self.stats['total_inserted']:,}")

        print("\n📊 База данных:")
        print(f"   Всего записей: {stats['total_checks']:,}")
        print(f"   Диапазон дат: {stats['min_date'] or 'Нет данных'} - "
              f"{stats['max_date'] or 'Нет данных'}")
        print(f"   Дней с данными: {stats['days_count'] or 0}")

        print(f"\n⏱️  Общее время: {str(elapsed).split('.')[0]}")
        print(f"💾 Файл базы: {self.config.SQLITE_DB}")

        if interrupted:
            state = self.db.get_etl_state()
            if state:
                print(f"\n🔸 Продолжить можно с даты: {state['last_processed_date']}")
                print(f"🔸 Последний ID: {state['last_processed_id']}")

        print("=" * 60)