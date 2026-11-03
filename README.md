# Параллельность и асинхронность — учебный краулер

Проект завершён за семь дней. Весь код приложения находится в одном
`crawler_day1.py`, тесты — в `tests/`. Python 3.9+.

| День | Возможности |
| --- | --- |
| 1 | aiohttp, сессия и пул соединений, конкурентная загрузка |
| 2 | BeautifulSoup/lxml, текст, ссылки, метаданные, изображения, таблицы |
| 3 | Приоритетная очередь, глубина, дедупликация, фильтры, семафоры |
| 4 | Rate limiting, robots.txt, Crawl-delay, User-Agent, jitter |
| 5 | Классификация ошибок, RetryStrategy, backoff, рост таймаутов |
| 6 | Асинхронные JSON/JSONL, CSV, SQLite, транзакции и повторы записи |
| 7 | Sitemap index, AdvancedCrawler, JSON-конфигурация, CLI, отчёты, замеры |

## Установка и первый запуск

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python crawler_day1.py --local --output artifacts/demo.json
```

`--local` запускает учебный сайт, robots.txt и sitemap index без внешней сети.
Показывает обход, запрет страницы, 404/403, временные 429/500/503 и таймаут.
Sitemap делает страницы 0 и 3 стартовыми, поэтому страница 4 также укладывается
в глубину 2: финальный пример обрабатывает 12 URL, из них 9 успешно и 3 с ошибками.

Получаются файлы:

- `demo.json` — страницы, статистика и ошибки;
- `demo.pages.json`, `demo.pages.csv`, `demo.pages.db` — данные трёх хранилищ;
- `demo.json.errors.json` — подробная история ошибок;
- `demo.stats.json` — статистика отдельно;
- `demo.report.html` — автономный HTML-отчёт с графиками и таблицами;
- `demo.log`, `demo.log.1` и далее — структурированные логи с ротацией.

Демонстрация фактически проверена: [HTML-отчёт](artifacts/final_demo.report.html),
[JSON-статистика](artifacts/final_demo.stats.json), [результаты замеров](artifacts/benchmark_results.json).
HTML не требует JavaScript, внешних шрифтов или сервисов. Откройте его локально
в браузере для просмотра графиков. Автоматические проверки проверяют структуру
HTML, счётчики и экранирование значений.

## CLI

```bash
python crawler_day1.py --urls https://example.com --max-pages 100 --max-depth 2 --rate-limit 2 --respect-robots --output results.json
python crawler_day1.py --config config.json
python crawler_day1.py --config config.json --max-pages 20 --rate-limit 1
python crawler_day1.py --urls https://example.com --sitemaps https://example.com/sitemap.xml
python crawler_day1.py --help
```

| Параметр | Значение |
| --- | --- |
| `--urls URL [URL ...]` | Стартовые страницы |
| `--sitemaps URL [URL ...]` | Источники sitemap |
| `--max-pages N` | Бюджет попыток обработки страниц, включая ошибки и robots-запреты |
| `--max-depth N` | Глубина, начиная с 0 для всех стартовых и sitemap URL |
| `--rate-limit R` | Частота HTTP-попыток на домен |
| `--respect-robots` / `--no-respect-robots` | Соблюдать или отключить robots.txt |
| `--same-domain-only` / `--no-same-domain-only` | Ограничить обход доменами стартовых страниц |
| `--output FILE` | Основной JSON и префикс автоматически создаваемых файлов |
| `--config FILE` | JSON-конфигурация |
| `--log-level LEVEL`, `--log-file FILE` | Логи в консоль и файл |
| `--local` | Локальная финальная демонстрация |
| `--compare` | Демонстрация первого дня: сравнение загрузки |
| `--parse` | Демонстрация второго дня: парсинг отдельных страниц |
| `--benchmark` | Синхронный и асинхронный обход 100/500/1000 страниц |

CLI-параметры переопределяют соответствующие значения JSON, остальные настройки
сохраняются. Явно заданные пути storage и logging не меняются при `--output`.
`--local` заменяет стартовые страницы учебным сайтом и добавляет локальный sitemap,
если sitemaps не заданы в конфигурации. Режимы compare/parse оставлены как отдельные
учебные демонстрации и используют свои настройки; основной CLI — AdvancedCrawler.

## Использование из Python

```python
import asyncio
from crawler_day1 import AdvancedCrawler

async def main():
    async with AdvancedCrawler.from_config("config.json") as crawler:
        results = await crawler.crawl()
        stats = crawler.get_stats()
        print(f"Обработано: {stats['total_pages']}")
        print(f"Успешно: {stats['successful']}; ошибок: {stats['failed']}")
        await crawler.save_results()
        crawler.export_to_json("stats.json")
        crawler.export_to_html_report("report.html")

asyncio.run(main())
```

`crawl()` возвращает словарь URL → данные страниц. В нём остаются частичные
результаты парсинга, даже если страница учтена как неудачная. `save_results()`
сохраняет основной JSON, ошибки, JSON-статистику и HTML-отчёт. Экспорт статистики
также доступен отдельными синхронными методами; для больших отчётов в работающем
event loop их можно вызывать через `asyncio.to_thread`.
Без контекстного менеджера вызывайте `await crawler.close()` в `finally`.
Клиент закрывает свою сессию, storage и принадлежащие ему обработчики логов.

## Конфигурация

Полный пример — [config.json](config.json). Поддерживается JSON, YAML не нужен.
Относительные пути в конфигурации разрешаются относительно её каталога.
Пути, переданные через CLI, относятся к текущему рабочему каталогу.
Неизвестные ключи основных разделов, неверные лимиты и регулярные выражения
приводят к понятной ошибке до начала обхода.

| Раздел | Параметры |
| --- | --- |
| `start_urls` | Список абсолютных HTTP(S)-адресов; по умолчанию example.com |
| `sitemaps` | Список карт; для обхода только по карте задайте start_urls=[] |
| `crawler` | Параметры AsyncCrawler: max_concurrent, per_domain, max_depth, requests_per_second, respect_robots, min_delay, jitter, user_agent, rate_per_domain, max_retries, backoff_factor, connect_timeout, read_timeout, total_timeout, timeout_multiplier, max_timeout |
| `crawl` | max_pages, same_domain_only, include_patterns, exclude_patterns |
| `storage` | Список хранилищ; [] отключает сохранение отдельных страниц |
| `sitemap` | max_sitemaps=100, max_urls=10000, max_depth=5, max_bytes=5242880 |
| `logging` | enabled, level, file, max_bytes, backup_count |
| `output` | Путь основного JSON |

Типы storage: json, jsonl, csv, sqlite. Общие настройки: path, max_retries=2,
backoff_factor=0.1. Для JSON доступны indent и json_lines, для CSV — encoding
(utf-8, utf-8-sig, cp1251). Если storage отсутствует, используется JSON с суффиксом
`.pages.json`. Несколько хранилищ объединяются в MultiStorage.

Регулярные выражения include/exclude проверяются по полному URL, включая
стартовые страницы. Достаточно одного include-совпадения, любое exclude-совпадение
запрещает адрес. same_domain_only принимает hostname всех стартовых страниц;
при отсутствии start_urls используются hostname карт. Поддомены считаются отдельно.

## API

| Класс | Основные методы |
| --- | --- |
| `AsyncCrawler` | fetch_url(url) → HTML; fetch_urls(urls) → dict; fetch_and_parse(url) → dict; crawl(start_urls, max_pages=100, same_domain_only=True, include_patterns=None, exclude_patterns=None); close() |
| `HTMLParser` | parse_html(html, url); extract_links; extract_text(soup, selector=None); extract_metadata; extract_images; extract_headings; extract_tables; extract_lists |
| `CrawlerQueue` | add_url(url, priority=0); get_next(); mark_processed; mark_failed; get_stats |
| `SemaphoreManager` | acquire(url) — async-контекст; get_stats |
| `RateLimiter` | RateLimiter(requests_per_second=1, per_domain=True); await acquire(domain) |
| `RobotsParser` | fetch_robots(base_url); can_fetch(url, user_agent); get_crawl_delay(user_agent, base_url=...) |
| `RetryStrategy` | execute_with_retry(async_function, *args, **kwargs); get_stats |
| `DataStorage` | Абстрактные async save(data), close(); save_batch(records); get_stats |
| `JSONStorage`, `CSVStorage`, `SQLiteStorage` | save, close, read_all; SQLite дополнительно init_db и транзакционный save_batch |
| `MultiStorage` | Сохранение в несколько хранилищ, независимые повторы ошибок |
| `SitemapParser` | fetch_sitemap(sitemap_url) → list[str], errors, visited_sitemaps |
| `CrawlerStats` | record_request; record_page; get_stats; export_to_json; export_to_html_report |
| `AdvancedCrawler` | from_config; crawl; get_stats; save_results; export_to_json; export_to_html_report; close |

AsyncCrawler можно использовать отдельно, например:

```python
from crawler_day1 import AsyncCrawler, JSONStorage, SQLiteStorage, RetryStrategy

async with AsyncCrawler(
    max_concurrent=5, per_domain=2, max_depth=2,
    requests_per_second=2, min_delay=0.5, respect_robots=True,
    storage=JSONStorage("pages.jsonl", json_lines=True),
) as crawler:
    pages = await crawler.crawl(["https://example.com"], max_pages=20)
    print(crawler.get_error_report())

async with SQLiteStorage("pages.db") as storage:
    await storage.save_batch(pages.values())
```

Страница содержит url, title, text, links, metadata, crawled_at (datetime UTC),
status_code, content_type, images, headings, tables, lists, errors и глубину при
crawl. При хранении datetime превращается в ISO-строку. Относительные ссылки
разрешаются относительно конечного URL ответа; исходный URL сохраняется в url.
Основной текст берётся из первого main, article или body. JS не выполняется.

## Очереди, ошибки и вежливость

- Обход идёт по глубинам конкурентными пакетами. URL не обрабатывается повторно
  в одном обходе. Повторы HTTP-операции относятся к одной попытке обхода страницы.
- max_pages включает ошибки. После достижения лимита необработанные URL могут
  остаться в очереди. Повторный crawl сбрасывает очередь и статистику страниц;
  статистика RetryStrategy, RateLimiter и storage накопительная за жизнь объекта.
- Общий семафор ограничивает конкурентность; отдельный — hostname, включая все
  его порты. RateLimiter задаёт интервалы между стартами запросов, а не число задач.
- Интервал — max(1/rate, min_delay, Crawl-delay) + случайное число [0, jitter].
  rate_per_domain=False задаёт общий лимит на все домены.
- robots.txt кэшируется по origin на час и проверяется перед каждой HTTP-попыткой,
  включая редиректы и повторы. Загрузка самого robots не рекурсивна, но ограничена
  скоростью. Поддержаны группы User-Agent, Allow/Disallow, длиннейшее совпадение,
  `*`, `$`, дробный Crawl-delay. Сетевая ошибка/5xx запрещает доступ на срок до 60 с;
  обычный 404 разрешает. 401/403/429 также дают временный запрет.
- TransientError: таймауты и 408/429/500/502/503/504; NetworkError: DNS, соединение,
  разрыв ответа. PermanentError: прочие статусы, неверный URL, robots-запрет.
  ParseError сохраняет частичные данные без повторного скачивания.
- RetryStrategy по умолчанию: max_retries=3, backoff_factor=2. Для NetworkError
  лимит min(2, max_retries) и вдвое меньший factor. Для 500 — максимум один повтор,
  для 429 factor удваивается. Можно задать retry_on, retry_limits,
  backoff_by_type и status_limits. Retry-After учитывается. PermanentError и
  ParseError не повторяются. Передавайте функцию, а не готовую корутину.
- Таймауты connect/read/total растут с timeout_multiplier=1.5 до max_timeout=120,
  независимо для каждой задачи. total ограничивает одну HTTP-попытку,
  а не весь обход с backoff. Отмена не превращается в сетевую ошибку.
- Ошибки записи учитываются отдельно от сетевых. Backend повторяет OSError или
  sqlite OperationalError, откатывая частичную запись/транзакцию. При окончательном
  отказе данные остаются в памяти, обход продолжается. Между разными backend
  общей транзакции нет. Откат не является гарантией от выключения компьютера.

## Форматы хранения

JSON дополняет массив без переписывания предыдущих записей; после каждого save
файл корректен. При повторном открытии массив проверяется целиком. Для больших
выгрузок используйте JSONL: запись и проверка старых строк идут построчно.
JSON и CSV дополняются, SQLite обновляет совпавший URL через UPSERT.

CSV автоматически берёт заголовок из первой записи. Все ячейки содержат
JSON-значения, что сохраняет типы, вложенные данные и спецсимволы; read_all их
декодирует. При использовании обычного csv.DictReader применяйте json.loads
к ячейкам. Новые поля после создания заголовка отклоняются явной ошибкой.

SQLite хранит стандартные колонки и полную запись data_json. Есть индексы по
URL (первичный ключ), времени и HTTP-статусу. save_batch делает executemany
в одной транзакции. Чтение read_all загружает результат в память. Работать с
одним файловым storage следует через один экземпляр, без сторонних писателей.

## Sitemap, статистика и мониторинг

SitemapParser поддерживает urlset, sitemapindex и XML namespace, рекурсивно
обходит индексы, удаляет повторы и защищается от циклов. XML разбирается с
отключёнными внешними сущностями/DTD, проверяются ограничения размера и глубины.
При ошибке одной карты доступны URL из остальных. XML разбирается в to_thread;
интегрированный HTTP-клиент проверяет размер после загрузки текста. Сжатые .gz
карты и автоматический поиск Sitemap: в robots.txt не реализованы: задавайте
адреса через sitemaps или --sitemaps.

Все sitemap URL становятся стартовыми с глубиной 0. Сетевые обращения sitemap
используют общие rate/robots/retry и попадают в статистику запросов, но не в
max_pages и счётчик обработанных HTML-страниц.

CrawlerStats разделяет завершённые страницы (total_pages/successful/failed)
и HTTP-попытки (total_requests/successful_requests/failed_requests), включая robots,
sitemap, редиректы и повторы. status_codes считает полученные ответы; при ошибке
соединения статус отсутствует. Ошибка чтения тела учитывается как неудачный
запрос даже при полученном HTTP 200. top_domains считает завершённые страницы.

Прогресс выводится после страницы и раз в секунду. Процент считается от бюджета
max_pages, а не от неизвестного размера сайта: при раннем завершении он может
быть меньше 100%. ETA приблизительно оценивает известную очередь и может меняться
при обнаружении ссылок. На завершении время и средняя скорость фиксируются.

JSON-логи имеют timestamp UTC, level, logger, message и exception. Console использует
временные метки окружения. Уровни DEBUG/INFO/WARNING/ERROR задаются в logging.level.
Файл ротируется по max_bytes с сохранением backup_count архивов. Настройка
логирования относится к процессу; не создавайте одновременно AdvancedCrawler
с разными настройками одного и того же logger.

## Тестирование и производительность

```bash
python -m unittest discover -s tests -v
python crawler_day1.py --benchmark --output artifacts/benchmark_results.json
```

Проверены локальные HTTP-ответы и ошибки, конкурентность, глубина, фильтры,
rate/robots/retries, сохранение и восстановление данных, sitemap index/циклы/DTD,
JSON-конфигурация и CLI overrides, статистика, ротация логов и HTML-экранирование.

Замер 01.11.2026: локальный сервер задерживает страницу на 5 мс. Оба клиента
используют keep-alive и одинаковый HTML-парсер; синхронный клиент http.client
в отдельном потоке, асинхронный aiohttp — до 20 запросов. Robots, rate limiting
и storage отключены для сравнения сетевого выполнения. Результаты всех страниц
сохраняются в памяти и проверяются по количеству.

| Страниц | Синхронно, с | Асинхронно, с | Ускорение | Пик Python async, МиБ |
| ---: | ---: | ---: | ---: | ---: |
| 100 | 0.801 | 0.154 | 5.21x | 1.45 |
| 500 | 4.134 | 0.654 | 6.32x | 2.56 |
| 1000 | 8.106 | 1.288 | 6.30x | 3.64 |

tracemalloc измеряет Python-аллокации клиента и локального сервера, не RSS,
не native-память lxml и не полное потребление системы. Это один локальный прогон,
а не обещание скорости внешних сайтов. Хранение всех результатов и множества
URL требует памяти пропорционально объёму обхода. Для более крупных выгрузок
понадобятся внешняя очередь и потоковая выдача результатов.

При интеграции устранены линейный поиск дубликатов ссылок (заменён set) и
построение подробной статистики прогресса при отключённом INFO-логировании.

## Исправления после ревью

SitemapParser читает ответ порциями до конца с контролем размера.
Внешняя RetryStrategy сохраняет единый бюджет при перенаправлениях;
robots.txt остаётся отдельной операцией.

Чтение закрытой пустой SQLite возвращает [] без создания файла.
Повторный close() повторяет незавершённое закрытие хранилища.

Отмена файлового save ожидает завершения записи и обновления счётчиков,
затем передаёт CancelledError. Запись может уже быть сохранена: автоматически
повторять отменённый save не следует. Блокировка удерживается до завершения.
При неудачном откате дальнейшие записи этим экземпляром запрещаются;
перед повторным открытием файл необходимо проверить.
