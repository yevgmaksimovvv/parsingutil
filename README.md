# Скрипт для обхода сайта с crawl4ai

Обходит сайт и сохраняет контент страниц в .md файлы для дальнейшего использования в AI.

**Особенности:**
- Обход только в пределах указанного домена (BFS)
- Древовидная структура файлов, отражающая URL
- Логирование и статистика
- Автоматическое фильтрование нерелевантного контента

## Установка

```bash
pip install -r requirements.txt
crawl4ai-setup
playwright install chromium
```

## Использование

```bash
python crawler.py -u <URL> -s <SITE_CODE> [-m <MAX_PAGES>] [-o <OUTPUT_DIR>]
```

### Аргументы

- `-u, --base-url` — стартовый URL сайта (обязательно)
- `-s, --site-code` — произвольный префикс для имен файлов, например: `example`, `mysite`, `shop` (обязательно)
- `-m, --max-pages` — максимум страниц (по умолчанию: 50)
- `-o, --output-dir` — директория для сохранения (по умолчанию: `./output`)

**Примечание:** `site-code` — это просто название, которое вы придумываете сами. Оно используется как префикс в имени файлов для удобной организации.

### Примеры

```bash
# Тестовый запуск (site-code = "test")
python crawler.py -u https://example.com -s test -m 10

# Полный обход (site-code = "example")
python crawler.py -u https://example.com -s example -m 100 -o ./results

# Реальный пример (site-code можно выбрать любой, например название сайта)
python crawler.py -u https://mysite.com -s mysite -m 50
```

## Формат выходных файлов

Файлы сохраняются в древовидной структуре, отражающей структуру URL:

```
output/
  <site-code>/
    index.md                    # Главная страница
    about/
      index.md                  # /about/
    articles/
      item.php_id_123.md        # /articles/item.php?id=123
```

Примеры:
- `https://example.com/` → `output/example/index.md`
- `https://example.com/about/` → `output/example/about/index.md`
- `https://example.com/articles/123` → `output/example/articles/123.md`

## Логирование

Логи сохраняются в `output/logs/<site-code>.log` (уровень DEBUG). В консоль выводится информация уровня INFO.

## Статистика

По завершении обхода выводится статистика:
- Количество обработанных страниц (успешно/ошибки)
- Количество сохраненных файлов
- Найдено/добавлено ссылок
- Время работы
- Типы ошибок
