<h1 align="center">retail-data-pipelines</h1>

<p align="center">
  <strong>ETL-пайплайн: MySQL-реплика → локальный PostgreSQL (мини-DWH)</strong><br>
  <em>Production-код, написанный и поддерживаемый на реальных данных розничной сети (NDA)</em>
</p>

<hr>

<h2>Что это</h2>

<p>
Это <strong>один</strong> ETL-пайплайн, разбитый на 4 модуля по зонам ответственности.
Он извлекает чеки из реплики MySQL, парсит JSON-поле <code>context_json</code>
и загружает нормализованные записи в локальный PostgreSQL (staging-слой мини-DWH).
</p>

<blockquote>
<p><strong>Почему не работаем напрямую с репликой?</strong><br>
Реплика источника работала на старой версии MySQL, где не поддерживались нужные функции
(оконные функции, <code>ON CONFLICT</code>/UPSERT, полноценные CTE, пакетная вставка).
Поэтому было принято решение построить <strong>мини-DWH на локальном PostgreSQL</strong>:
MySQL используется только как источник (extract), а вся трансформация, дедупликация
и историзация выполняются на стороне Postgres. Это классический <strong>ELT с локальным хранилищем</strong>.</p>
</blockquote>

<hr>

<h2>Это один пайплайн из 4 модулей</h2>

<table>
<thead>
<tr><th>Модуль</th><th>Роль в пайплайне</th><th>Ключевые фичи</th></tr>
</thead>
<tbody>
<tr>
<td><code>pipeline/config.py</code></td>
<td>Конфигурация подключений (MySQL-реплика, Postgres) через <code>.env</code>, валидация</td>
<td><code>python-dotenv</code>, разделение PG/MySQL, <code>SOURCE_SCHEMA</code> в конфиге</td>
</tr>
<tr>
<td><code>pipeline/parsers.py</code></td>
<td>Extract-парсинг JSON-поля <code>context_json</code> → <code>skus</code>, <code>coupon_rule_id</code></td>
<td>Предкомпилированные regex, <code>re.DOTALL</code>, без полной десериализации JSON</td>
</tr>
<tr>
<td><code>pipeline/database_postgres.py</code></td>
<td>Слой доступа к БД: контекстные менеджеры, DDL staging, батч-вставка, чекпоинты, статистика</td>
<td><code>execute_values</code>, <code>ON CONFLICT DO NOTHING</code>, индексы, таблица <code>etl_state</code></td>
</tr>
<tr>
<td><code>pipeline/etl_pipeline.py</code></td>
<td>Оркестратор: resume-able цикл по батчам <code>(date, id)</code>, прогресс, отчёты</td>
<td><code>tqdm</code>, <code>KeyboardInterrupt</code>-safe, промежуточная и финальная статистика</td>
</tr>
</tbody>
</table>

<p>Точка входа — <code>run_etl.py</code>, который создаёт <code>ResumeETL</code> и запускает его.</p>

<hr>

<h2>Поток данных</h2>

<pre>
┌────────────────────────┐
│  mysql_replica         │   SELECT ... WHERE check_type = 1
│  .check_context_storage│   ORDER BY date, id LIMIT :batch_size
└───────────┬────────────┘
            │
            ▼
┌────────────────────────┐
│  parsers.py            │   regex → skus, coupon_rule_id
│  (в etl_pipeline.run)  │
└───────────┬────────────┘
            │
            ▼
┌────────────────────────┐     ┌──────────────────────┐
│  database_postgres.py  │────▶│  parsed_checks       │
│  execute_values +      │     │  (staging, Postgres) │
│  ON CONFLICT DO NOTHING│     └──────────────────────┘
└───────────┬────────────┘
            │
            ▼
┌────────────────────────┐
│  etl_state             │   чекпоинт (date, id)
│  (resume-логика)       │
└────────────────────────┘
</pre>

<hr>

<h2>Ключевые инженерные решения</h2>

<h3>1. Идемпотентность</h3>
<p>
Повторная обработка батча безопасна благодаря <code>UNIQUE(mysql_check_id)</code> +
<code>ON CONFLICT DO NOTHING</code>. Чекпоинт <code>(date, id)</code> сохраняется
<strong>после</strong> коммита батча, поэтому худший сценарий при сбое — повторная
обработка одного батча, а не потеря данных.
</p>

<h3>2. Resume-able ETL</h3>
<p>
Прогресс хранится в таблице <code>etl_state</code> (одна строка, <code>CHECK (id = 1)</code>).
При перезапуске пайплайн читает последнюю обработанную пару <code>(date, id)</code>
и продолжает с неё. <code>KeyboardInterrupt</code> перехватывается, состояние сохраняется,
финальный отчёт печатается.
</p>

<h3>3. Быстрый парсинг JSON</h3>
<p>
<code>context_json</code> — большое JSON-поле, полная десериализация которого дорога.
Вместо <code>json.loads</code> используются <strong>предкомпилированные regex</strong>,
которые вытаскивают только нужные поля (<code>skus</code>, <code>coupon_rule_id</code>).
Это даёт ~7–9 тыс. чеков/сек на одном потоке.
</p>

<h3>4. Пакетная вставка</h3>
<p>
<code>psycopg2.extras.execute_values</code> с <code>page_size=10000</code> вместо
построчных <code>INSERT</code>. Это на порядок быстрее и не упирается в лимит параметров.
</p>

<hr>

<h2>Быстрый старт (PowerShell)</h2>

<pre><code>cd "$env:USERPROFILE\OneDrive\Рабочий стол\retail-data-pipelines"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
# отредактировать .env: доступы к MySQL-реплике и локальному Postgres
python run_etl.py --batch-size 5000</code></pre>

<p>Опции:</p>
<ul>
<li><code>--batch-size N</code> — размер батча (по умолчанию 5000).</li>
<li><code>--no-resume</code> — начать с нуля, игнорируя <code>etl_state</code>.</li>
</ul>

<hr>

<h2>Связанные проекты</h2>
<ul>
<li>
<strong><a href="https://github.com/chip9921107/dwh-pet-project">dwh-pet-project</a></strong> —
учебный pet-проект: проектирование DWH с нуля (staging → core-звезда + SCD Type 2 → mart в ClickHouse).
Демонстрирует <em>архитектурное</em> мышление.
</li>
<li>
<strong>retail-data-pipelines</strong> (этот репозиторий) — production-код ETL,
работавший на реальных данных. Демонстрирует <em>инженерную</em> зрелость.
</li>
</ul>