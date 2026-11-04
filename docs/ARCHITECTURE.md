<!DOCTYPE html>
<html lang="ru">
<head>
<meta charset="utf-8">
<title>retail-data-pipelines — Architecture</title>
<style>
  body { font-family: -apple-system, Segoe UI, Roboto, sans-serif; max-width: 860px; margin: 2rem auto; padding: 0 1rem; line-height: 1.55; color: #24292f; }
  h1, h2, h3 { border-bottom: 1px solid #d0d7de; padding-bottom: .3em; }
  code { background: #f6f8fa; padding: .1em .35em; border-radius: 4px; }
  pre { background: #f6f8fa; padding: 1rem; border-radius: 6px; overflow-x: auto; }
  blockquote { border-left: 4px solid #d0d7de; margin: 0; padding: 0 1rem; color: #57606a; }
  table { border-collapse: collapse; width: 100%; }
  th, td { border: 1px solid #d0d7de; padding: .5rem .75rem; text-align: left; vertical-align: top; }
  th { background: #f6f8fa; }
</style>
</head>
<body>

<h1>Архитектура retail-data-pipelines</h1>

<blockquote>
Production-код ETL-пайплайна: MySQL-реплика → локальный PostgreSQL (мини-DWH).<br>
Реальные данные розничной сети, обезличенные для публикации (NDA).
</blockquote>

<h2>1. Контекст и ограничения</h2>

<p>Исходные условия:</p>
<ul>
<li>Источник — <strong>реплика MySQL</strong> старой версии. Нет оконных функций, UPSERT, полноценных CTE, пакетной вставки.</li>
<li>Целевой слой должен поддерживать трансформации, дедупликацию, историзацию (SCD2) — то, чего старая MySQL не даёт.</li>
<li>Объём — миллионы чеков, инкрементальная загрузка по <code>(date, id)</code>.</li>
<li>Пайплайн должен переживать падения и продолжать с чекпоинта.</li>
<li>В источнике есть разные типы точек (магазины, развлекательный центр, пиццерии). В оригинале был фильтр по <code>partner_id</code>; в обезличенной версии он убран, чтобы пайплайн не зависел от бизнес-типологии.</li>
</ul>

<h2>2. Принятое решение: локальный Postgres как мини-DWH</h2>

<p>
MySQL-реплика используется <strong>только как источник (extract)</strong>.
Вся трансформация и хранение — в локальном PostgreSQL.
Это <strong>ELT с локальным хранилищем</strong>: сначала грузим «как есть» в staging,
затем трансформируем на стороне Postgres.
</p>

<p><strong>Почему не ETL «в источнике»?</strong></p>
<ul>
<li>Реплика старая → нет нужных SQL-конструкций.</li>
<li>Нагрузка на реплику нежелательна (её читают другие потребители).</li>
<li>Локальный Postgres даёт полный контроль над схемой, индексами и движком.</li>
</ul>

<h2>3. Слои пайплайна</h2>

<table>
<thead><tr><th>Слой</th><th>Модуль</th><th>Что делает</th></tr></thead>
<tbody>
<tr><td>Config</td><td><code>config.py</code></td><td>Читает <code>.env</code>, валидирует обязательные поля, отдаёт конфиги PG/MySQL, <code>SOURCE_SCHEMA</code></td></tr>
<tr><td>Extract / Parse</td><td><code>parsers.py</code></td><td>Regex-парсинг <code>context_json</code> → <code>skus</code>, <code>coupon_rule_id</code></td></tr>
<tr><td>Load / State</td><td><code>database_postgres.py</code></td><td>DDL, батч-вставка, <code>ON CONFLICT</code>, чекпоинты, статистика</td></tr>
<tr><td>Orchestration</td><td><code>etl_pipeline.py</code></td><td>Resume-able цикл, прогресс, отчёты, обработка <code>KeyboardInterrupt</code></td></tr>
</tbody>
</table>

<h2>4. Поток данных</h2>

<pre>
   ┌──────────────────────────────┐
   │  mysql_replica               │  SELECT ... WHERE check_type = 1
   │  .check_context_storage      │  ORDER BY date, id LIMIT :batch_size
   │  (старая версия MySQL)       │
   └──────────────┬───────────────┘
                  │
                  ▼
   ┌──────────────────────────────┐
   │  etl_pipeline.run()          │
   │  → parsers.parse_minimal_fields()
   │  regex → skus, coupon_rule_id│
   └──────────────┬───────────────┘
                  │
                  ▼
   ┌──────────────────────────────┐     ┌──────────────────────┐
   │  database_postgres           │────▶│  parsed_checks       │
   │  execute_values(...)         │     │  (staging, Postgres) │
   │  ON CONFLICT DO NOTHING      │     └──────────────────────┘
   └──────────────┬───────────────┘
                  │
                  ▼
   ┌──────────────────────────────┐
   │  etl_state                   │
   │  (date, id, total_processed) │
   └──────────────────────────────┘
</pre>

<h2>5. Идемпотентность</h2>

<p>
Таблица <code>parsed_checks</code> имеет <code>mysql_check_id VARCHAR(100) UNIQUE</code>.
Вставка идёт с <code>ON CONFLICT (mysql_check_id) DO NOTHING</code>.
Поэтому повторная обработка одного и того же батча (например, после сбоя между
коммитом данных и коммитом чекпоинта) безопасна — дубликатов не будет.
</p>

<h2>6. Resume-логика</h2>

<p>
Чекпоинт хранится в <code>etl_state</code> — одна строка, <code>CHECK (id = 1)</code>.
Поля: <code>last_processed_date</code>, <code>last_processed_id</code>,
<code>total_processed</code>, <code>total_inserted</code>, <code>updated_at</code>.
</p>

<p>Алгоритм:</p>
<ol>
<li>При старте читаем <code>etl_state</code>.</li>
<li>Спрашиваем пользователя: продолжить с этой точки / сбросить / отменить.</li>
<li>В цикле: <code>SELECT</code> следующего батча по <code>(date &gt; last_date OR (date = last_date AND id &gt; last_id))</code>.</li>
<li>Парсим, вставляем батч, <strong>коммитим</strong>.</li>
<li>Обновляем <code>etl_state</code> новыми <code>(date, id)</code> и счётчиками.</li>
<li>При <code>KeyboardInterrupt</code> — печатаем текущее состояние и выходим, не теряя прогресс.</li>
</ol>

<h2>7. Производительность</h2>

<table>
<thead><tr><th>Решение</th><th>Эффект</th></tr></thead>
<tbody>
<tr><td>Предкомпилированные regex вместо <code>json.loads</code></td><td>~7–9 тыс. чеков/сек на одном потоке</td></tr>
<tr><td><code>execute_values(page_size=10000)</code></td><td>На порядок быстрее построчных <code>INSERT</code></td></tr>
<tr><td>Индексы: <code>check_date</code>, <code>(client_id, check_date)</code>, <code>client_id</code>, <code>location_id</code></td><td>Быстрая аналитика по клиенту/дате/точке</td></tr>
<tr><td><code>ORDER BY date, id</code> + чекпоинт <code>(date, id)</code></td><td>Инкрементальная загрузка без полного перечитывания</td></tr>
</tbody>
</table>

<h2>8. Обработка ошибок</h2>
<ul>
<li><code>KeyboardInterrupt</code> — перехватывается, состояние сохраняется, отчёт печатается.</li>
<li>Прочие исключения — печатается отчёт, исключение пробрасывается (чтобы не скрывать ошибку).</li>
<li>Парсинг JSON — regex-подход не падает на невалидном JSON (в отличие от <code>json.loads</code>).</li>
</ul>

<h2>9. Что этот код демонстрирует</h2>
<ul>
<li>Понимание ограничений источника и <strong>осознанный выбор архитектуры</strong> (локальный Postgres как мини-DWH).</li>
<li>Идемпотентный дизайн загрузки (UNIQUE + ON CONFLICT + чекпоинты).</li>
<li>Работу с производительностью (regex vs JSON, batch insert, индексы).</li>
<li>Устойчивость к сбоям (resume-able, <code>KeyboardInterrupt</code>-safe).</li>
<li>Разделение ответственности между модулями (config / parser / db / orchestrator).</li>
</ul>

<h2>10. Обезличивание</h2>
<p>
Реальные имена схем, хосты, креды и название компании заменены.
В коде используется нейтральная схема <code>mysql_replica.*</code>.
Пайплайн воспроизводим на любом источнике с аналогичной структурой
<code>check_context_storage</code>.
</p>

</body>
</html>