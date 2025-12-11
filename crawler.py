#!/usr/bin/env python3
"""
Скрипт для обхода сайта и сохранения контента страниц в .md файлы.
"""

import asyncio
import argparse
import os
import re
import time
import random
import json
import logging
import hashlib
import datetime
import urllib.robotparser
from collections import deque, defaultdict
from typing import Optional, Set, List
from urllib.parse import urlparse, urljoin

from crawl4ai import (
    AsyncWebCrawler,
    BrowserConfig,
    CrawlerRunConfig,
    CacheMode,
    DefaultMarkdownGenerator,
    PruningContentFilter,
)
from crawl4ai.utils import normalize_url_for_deep_crawl


EXCLUDED_FILE_EXTENSIONS: Set[str] = {
    'pdf', 'doc', 'docx', 'xls', 'xlsx', 'zip', 'rar',
    'jpg', 'jpeg', 'png', 'gif', 'svg', 'css', 'js', 'xml'
}

DEFAULT_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Cache-Control": "max-age=0"
}

CRAWLER_USER_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def setup_logging(output_dir: str, site_code: str) -> logging.Logger:
    """Настраивает логирование: файл (DEBUG) и консоль (INFO)."""
    log_dir = os.path.join(output_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{site_code}.log")
    
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    
    logger = logging.getLogger('crawler')
    logger.setLevel(logging.DEBUG)
    logger.handlers.clear()
    
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger


def has_file_extension(url: str, extensions: Set[str]) -> bool:
    """Проверяет наличие расширения файла в URL."""
    try:
        parsed = urlparse(url)
        path = parsed.path.lower()
        if '.' in path:
            ext = path.split('.')[-1].split('/')[0].split('?')[0]
            return ext in extensions
        return False
    except Exception:
        return False


def get_site_folder_name(base_url: str, fallback: str) -> str:
    """Возвращает безопасное имя папки на основе домена сайта."""
    netloc = urlparse(base_url).netloc or fallback
    if netloc.startswith("www."):
        netloc = netloc[4:]
    safe_name = re.sub(r'[^a-zA-Z0-9._-]', '_', netloc.lower())
    return safe_name or fallback


def resolve_site_code(base_url: str, site_code: Optional[str]) -> str:
    """Определяет имя папки/лога: приоритет у переданного -s, иначе домен сайта."""
    default_name = get_site_folder_name(base_url, "site")
    if not site_code:
        return default_name
    safe_code = re.sub(r'[^a-zA-Z0-9._-]', '_', site_code.strip().lower())
    return safe_code or default_name


def url_to_file_path(url: str, site_code: str, site_output_dir: str, ext: str = "md") -> str:
    """Преобразует URL в путь к файлу в одной папке."""
    
    # Создаем уникальное имя файла на основе URL
    url_hash = hashlib.md5(url.encode('utf-8')).hexdigest()[:12]
    parsed = urlparse(url)
    path = parsed.path.strip('/')
    
    # Берем последнюю часть пути для имени файла
    if path and path != '/':
        path_parts = [p for p in path.split('/') if p]
        if path_parts:
            base_name = path_parts[-1]
            # Убираем расширения и небезопасные символы
            base_name = re.sub(r'[<>:"|?*\\]', '_', base_name)
            if '.' in base_name:
                base_name = base_name.rsplit('.', 1)[0]
            if base_name:
                file_name = f"{site_code}_{base_name}_{url_hash}.{ext}"
            else:
                file_name = f"{site_code}_{url_hash}.{ext}"
        else:
            file_name = f"{site_code}_index_{url_hash}.{ext}"
    else:
        file_name = f"{site_code}_index_{url_hash}.{ext}"
    
    # Все файлы в одну папку
    os.makedirs(site_output_dir, exist_ok=True)
    return os.path.join(site_output_dir, file_name)


def print_stats(stats: dict, base_url: str, max_pages: int) -> None:
    """Выводит статистику обхода."""
    total = stats['success'] + stats['failed']
    success_rate = (stats['success'] / total * 100) if total > 0 else 0
    duration = stats['end_time'] - stats['start_time']
    
    print("\n" + "=" * 60)
    print("СТАТИСТИКА ОБХОДА")
    print("=" * 60)
    print(f"Стартовый URL:     {base_url}")
    print(f"Максимум страниц:  {max_pages}")
    print(f"Обработано:        {total}")
    print(f"Успешно:           {stats['success']} ({success_rate:.1f}%)")
    print(f"Ошибок:            {stats['failed']} ({100-success_rate:.1f}%)")
    print(f"Сохранено файлов:  {len(stats['saved_files'])}")
    if stats.get('skipped_unchanged'):
        print(f"Пропущено (без изменений): {stats['skipped_unchanged']}")
    if stats.get('skipped_by_robots'):
        print(f"Пропущено robots.txt: {stats['skipped_by_robots']}")
    print(f"Ссылок найдено:    {stats.get('links_found', 0)}")
    print(f"Ссылок добавлено:  {stats.get('links_added', 0)}")
    print(f"В очереди осталось:{stats.get('queue_remaining', 0)}")
    print(f"Время работы:      {duration:.1f} сек ({duration/60:.1f} мин)")
    if total > 0:
        print(f"Средняя скорость:   {duration/total:.1f} сек/страница")
    
    if stats.get('errors'):
        print("\nТипы ошибок:")
        for error, count in sorted(stats['errors'].items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  - {error}: {count}")
    
    print("=" * 60)


class SiteCrawler:
    """Класс для обхода сайта и сохранения контента."""
    
    def __init__(
        self,
        base_url: str,
        site_code: Optional[str],
        max_pages: int,
        output_dir: str,
        content_format: str,
        page_timeout_ms: int,
        ignore_robots: bool,
    ):
        self.base_url = base_url
        self.site_code = resolve_site_code(base_url, site_code)
        self.max_pages = max_pages
        self.output_dir = output_dir
        self.content_format = content_format  # html | raw-md | filtered-md
        self.page_timeout_ms = page_timeout_ms
        self.ignore_robots = ignore_robots
        self.base_domain_netloc = urlparse(base_url).netloc
        self.site_folder = self.site_code
        self.site_output_dir = os.path.join(self.output_dir, self.site_folder)
        self.logger = setup_logging(output_dir, self.site_code)
        self.registry_path = os.path.join(self.site_output_dir, "registry.json")
        self.registry = self._load_registry()
        self.robot_parser = self._init_robot_parser()
        
        self.stats = {
            'start_time': time.time(),
            'end_time': None,
            'success': 0,
            'failed': 0,
            'errors': defaultdict(int),
            'saved_files': [],
            'links_found': 0,
            'links_added': 0,
            'queue_remaining': 0,
            'skipped_unchanged': 0,
            'skipped_by_robots': 0
        }
        
        self.visited: Set[str] = set()
        self.queue: deque = deque()
    
    def _create_configs(self):
        """Создает конфигурации браузера и краулера."""
        browser_config = BrowserConfig(
            headless=True,
            browser_type="chromium",
            user_agent=CRAWLER_USER_AGENT,
            enable_stealth=True,
            viewport_width=1920,
            viewport_height=1080,
            headers=DEFAULT_HEADERS
        )
        
        content_filter = PruningContentFilter(threshold=0.5, threshold_type="fixed", min_word_threshold=10)
        markdown_generator = DefaultMarkdownGenerator(content_filter=content_filter)
        
        crawler_config = CrawlerRunConfig(
            cache_mode=CacheMode.ENABLED,
            markdown_generator=markdown_generator,
            exclude_external_links=True,
            delay_before_return_html=2.5,
            mean_delay=4.0,
            max_range=2.0,
            wait_until="domcontentloaded",
            page_timeout=self.page_timeout_ms,
            simulate_user=True,
            scroll_delay=1.0,
            max_scroll_steps=2
        )
        
        # Запасной конфиг на случай таймаутов при навигации
        fallback_crawler_config = CrawlerRunConfig(
            cache_mode=CacheMode.ENABLED,
            markdown_generator=markdown_generator,
            exclude_external_links=True,
            delay_before_return_html=1.5,
            mean_delay=3.0,
            max_range=2.0,
            wait_until="domcontentloaded",
            page_timeout=int(self.page_timeout_ms * 1.5),
            simulate_user=False,
            scroll_delay=0.5,
            max_scroll_steps=1
        )
        
        return browser_config, crawler_config, fallback_crawler_config

    def _load_registry(self) -> dict:
        """Загружает реестр страниц с хешами и датами."""
        if not os.path.exists(self.registry_path):
            return {}
        try:
            with open(self.registry_path, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
        except Exception as e:
            self.logger.warning(f"Не удалось прочитать registry.json: {e}")
            return {}

    def _save_registry(self) -> None:
        """Сохраняет реестр страниц."""
        try:
            with open(self.registry_path, "w", encoding="utf-8") as f:
                json.dump(self.registry, f, ensure_ascii=False, indent=2)
        except Exception as e:
            self.logger.warning(f"Не удалось сохранить registry.json: {e}")

    def _init_robot_parser(self):
        """Инициализирует парсер robots.txt."""
        if self.ignore_robots:
            return None
        try:
            parsed_base = urlparse(self.base_url)
            robots_url = f"{parsed_base.scheme}://{parsed_base.netloc}/robots.txt"
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(robots_url)
            rp.read()
            self.logger.info(f"robots.txt: {robots_url} загружен")
            return rp
        except Exception as e:
            self.logger.warning(f"Не удалось загрузить robots.txt: {e}")
            return None
    
    def _extract_title(self, result) -> str:
        """Извлекает заголовок страницы из результата."""
        # Пробуем разные способы получить заголовок
        if hasattr(result, 'metadata') and result.metadata:
            if isinstance(result.metadata, dict):
                title = result.metadata.get('title') or result.metadata.get('og:title')
                if title:
                    return title.strip()
        
        # Пробуем извлечь из HTML
        if hasattr(result, 'html') and result.html:
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(result.html, 'html.parser')
                title_tag = soup.find('title')
                if title_tag:
                    return title_tag.get_text().strip()
            except Exception:
                pass
        
        # Пробуем из markdown (если там есть заголовок)
        if hasattr(result, 'markdown'):
            markdown = None
            if hasattr(result.markdown, 'fit_markdown') and result.markdown.fit_markdown:
                markdown = result.markdown.fit_markdown
            elif hasattr(result.markdown, 'raw_markdown') and result.markdown.raw_markdown:
                markdown = result.markdown.raw_markdown
            elif isinstance(result.markdown, str):
                markdown = result.markdown
            
            if markdown:
                # Ищем первый заголовок в markdown
                lines = markdown.split('\n')
                for line in lines[:10]:  # Проверяем первые 10 строк
                    line = line.strip()
                    if line.startswith('# '):
                        return line[2:].strip()
                    elif line.startswith('## '):
                        return line[3:].strip()
        
        return "Без заголовка"
    
    def _extract_content(self, result):
        """Возвращает контент и расширение файла согласно выбранному формату."""
        if self.content_format == "html":
            html = getattr(result, 'html', None)
            return html, "html"

        if not hasattr(result, 'markdown'):
            return None, "md"

        markdown_obj = result.markdown
        raw_md = None
        fit_md = None

        if hasattr(markdown_obj, 'raw_markdown'):
            raw_md = markdown_obj.raw_markdown
        elif isinstance(markdown_obj, str):
            raw_md = markdown_obj

        if hasattr(markdown_obj, 'fit_markdown'):
            fit_md = markdown_obj.fit_markdown

        if self.content_format == "raw-md":
            return raw_md or fit_md, "md"

        # filtered-md
        return fit_md or raw_md, "md"
    
    def _record_error(self, url: str, error_type: str):
        """Записывает ошибку в статистику."""
        self.stats['failed'] += 1
        self.stats['errors'][error_type] += 1
        self.logger.warning(f"FAIL {url}: {error_type}")
        print(f"FAIL {url}: {error_type}")

    def _is_allowed_by_robots(self, url: str) -> bool:
        if not self.robot_parser:
            return True
        try:
            return self.robot_parser.can_fetch(CRAWLER_USER_AGENT, url)
        except Exception:
            return True
    
    def _extract_links(self, result, current_url: str) -> List[str]:
        """Извлекает и фильтрует ссылки из результата."""
        links = []
        
        if not hasattr(result, 'links') or not isinstance(result.links, dict) or 'internal' not in result.links:
            return links
        
        for link_item in result.links['internal']:
            href = link_item.get('href') if isinstance(link_item, dict) else link_item
            if not href or not isinstance(href, str):
                continue
            
            try:
                parsed_netloc = urlparse(urljoin(current_url, href)).netloc
                
                if parsed_netloc and parsed_netloc != self.base_domain_netloc:
                    continue
                
                if has_file_extension(href, EXCLUDED_FILE_EXTENSIONS):
                    continue
                
                if href not in links:
                    links.append(href)
            except Exception:
                pass
        
        return links
    
    async def _fetch_with_timeout_handling(
        self,
        crawler: AsyncWebCrawler,
        url: str,
        primary_config: CrawlerRunConfig,
        fallback_config: CrawlerRunConfig,
    ):
        """Выполняет загрузку страницы с запасным конфигом при таймауте."""
        try:
            return await crawler.arun(url, config=primary_config)
        except Exception as e:
            if "Page.goto: Timeout" in str(e):
                self.logger.warning(f"Таймаут при переходе на {url}, повторяю с увеличенным таймаутом")
                return await crawler.arun(url, config=fallback_config)
            raise
    
    async def _crawl_page(
        self,
        crawler: AsyncWebCrawler,
        crawler_config: CrawlerRunConfig,
        fallback_config: CrawlerRunConfig,
        url: str,
    ):
        """Обходит одну страницу."""
        normalized_url = normalize_url_for_deep_crawl(url, self.base_url)

        if not self._is_allowed_by_robots(normalized_url):
            self.stats['skipped_by_robots'] += 1
            self.logger.info(f"SKIP robots.txt {normalized_url}")
            return

        if normalized_url in self.visited:
            return
        
        self.visited.add(normalized_url)
        self.logger.info(f"[{len(self.visited)}/{self.max_pages}] {normalized_url}")
        
        try:
            delay = 4.0 if len(self.visited) == 1 else random.uniform(4.0, 7.0)
            self.logger.debug(f"Задержка: {delay:.2f}с")
            await asyncio.sleep(delay)
            
            self.logger.debug(f"Запрос: {normalized_url}")
            result = await self._fetch_with_timeout_handling(
                crawler, normalized_url, crawler_config, fallback_config
            )
            await asyncio.sleep(0.5)
            
            if not result.success:
                error_msg = getattr(result, 'error_message', 'Неизвестная ошибка')
                self._record_error(url, error_msg)
                return
            
            status_code = getattr(result, 'status_code', None)
            
            if status_code == 403:
                self._record_error(url, "403 Forbidden")
                return
            elif status_code == 404:
                self._record_error(url, "404 Not Found")
                return

            links = self._extract_links(result, url)
            
            if links:
                self.stats['links_found'] += len(links)
                for link in links:
                    try:
                        normalized_link = normalize_url_for_deep_crawl(link, url)
                        if normalized_link not in self.visited and normalized_link not in self.queue:
                            self.queue.append(normalized_link)
                            self.stats['links_added'] += 1
                    except Exception:
                        pass

            content, ext = self._extract_content(result)

            if not content:
                self._record_error(url, "Нет контента")
                return
            
            # Извлекаем заголовок страницы
            page_title = self._extract_title(result)
            self.logger.debug(f"Заголовок страницы: {page_title}")
            
            # Формируем содержимое файла с URL и заголовком в начале
            if ext == "md":
                file_content = f"""---
URL: {normalized_url}
Заголовок: {page_title}
---

{content}
"""
            else:
                file_content = f"""<!--
---
URL: {normalized_url}
Заголовок: {page_title}
---
-->
{content}
"""

            file_path = url_to_file_path(url, self.site_code, self.site_output_dir, ext)
            content_hash = hashlib.sha256(file_content.encode('utf-8')).hexdigest()
            stored = self.registry.get(normalized_url, {})
            timestamp = datetime.datetime.utcnow().isoformat()

            if stored.get("hash") == content_hash:
                stored_path = stored.get("path", file_path)
                self.registry[normalized_url] = {
                    "hash": content_hash,
                    "last_crawled": timestamp,
                    "path": stored_path,
                }
                self._save_registry()
                self.stats['success'] += 1
                self.stats['skipped_unchanged'] += 1
                self.logger.info(f"UNCHANGED {url} -> {stored_path}")
                return

            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(file_content)

            self.registry[normalized_url] = {
                "hash": content_hash,
                "last_crawled": timestamp,
                "path": file_path,
            }
            self._save_registry()
                        
            self.stats['success'] += 1
            self.stats['saved_files'].append(file_path)
            self.logger.info(f"OK {url} -> {file_path}")
            print(f"OK {url} -> {file_path}")
                        
        except Exception as e:
            self._record_error(url, str(e))
    
    async def crawl(self) -> None:
        """Основной метод обхода сайта."""
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.site_output_dir, exist_ok=True)
        
        self.logger.info("=" * 60)
        self.logger.info("НАЧАЛО ОБХОДА")
        self.logger.info("=" * 60)
        self.logger.info(f"URL: {self.base_url}")
        self.logger.info(f"Site code: {self.site_code}")
        self.logger.info(f"Максимум страниц: {self.max_pages}")
        self.logger.info(f"Формат контента: {self.content_format}")
        self.logger.info(f"Таймаут страницы: {self.page_timeout_ms} мс")
        self.logger.info(f"robots.txt: {'игнорируем' if self.ignore_robots else 'соблюдаем'}")
        self.logger.info(f"Директория: {self.site_output_dir}")
        
        normalized_base = normalize_url_for_deep_crawl(self.base_url, self.base_url)
        self.logger.debug(f"Домен: {self.base_domain_netloc}, нормализованный URL: {normalized_base}")
        
        self.queue.append(normalized_base)
        browser_config, crawler_config, fallback_config = self._create_configs()
        
        async with AsyncWebCrawler(config=browser_config) as crawler:
            self.logger.info("Инициализирован AsyncWebCrawler")
            
            while self.queue and len(self.visited) < self.max_pages:
                url = self.queue.popleft()
                await self._crawl_page(crawler, crawler_config, fallback_config, url)
        
        self.stats['end_time'] = time.time()
        self.stats['queue_remaining'] = len(self.queue)
        
        self.logger.info("=" * 60)
        self.logger.info("ЗАВЕРШЕНИЕ ОБХОДА")
        self.logger.info("=" * 60)
        self.logger.info(f"Обработано: {self.stats['success'] + self.stats['failed']} | Успешно: {self.stats['success']} | Ошибок: {self.stats['failed']}")
        
        duration = self.stats['end_time'] - self.stats['start_time']
        self.logger.info(f"Время: {duration:.1f}с ({duration/60:.1f} мин) | В очереди: {self.stats['queue_remaining']}")
        
        print_stats(self.stats, self.base_url, self.max_pages)


async def crawl_site(
    base_url: str,
    site_code: Optional[str],
    max_pages: int,
    output_dir: str,
    content_format: str,
    page_timeout_ms: int,
    ignore_robots: bool,
) -> None:
    """Обходит сайт и сохраняет контент страниц в файлы указанного формата."""
    crawler = SiteCrawler(
        base_url,
        site_code,
        max_pages,
        output_dir,
        content_format,
        page_timeout_ms,
        ignore_robots,
    )
    await crawler.crawl()


def main() -> None:
    """Точка входа скрипта."""
    parser = argparse.ArgumentParser(description="Обход сайта и сохранение контента страниц в .md файлы")
    parser.add_argument("--base-url", "-u", required=True, help="Стартовый URL сайта")
    parser.add_argument(
        "--site-code",
        "-s",
        required=False,
        default=None,
        help="Название для папки/лога и префикса файлов (если не указано, берется из домена сайта)",
    )
    parser.add_argument("--max-pages", "-m", type=int, default=50, help="Максимальное количество страниц (по умолчанию: 50)")
    parser.add_argument(
        "--content-format",
        "-f",
        choices=["filtered-md", "raw-md", "html"],
        default="filtered-md",
        help="Формат сохранения контента: filtered-md (по умолчанию), raw-md или html",
    )
    parser.add_argument(
        "--page-timeout",
        type=int,
        default=60,
        help="Таймаут загрузки страницы в секундах (по умолчанию: 60)",
    )
    parser.add_argument(
        "--ignore-robots",
        action="store_true",
        help="Игнорировать robots.txt (по умолчанию соблюдаем)",
    )
    parser.add_argument("--output-dir", "-o", default="./output", help="Директория для сохранения (по умолчанию: ./output)")
    
    args = parser.parse_args()
    asyncio.run(
        crawl_site(
            args.base_url,
            args.site_code,
            args.max_pages,
            args.output_dir,
            args.content_format,
            int(args.page_timeout * 1000),
            args.ignore_robots,
        )
    )


if __name__ == "__main__":
    main()
