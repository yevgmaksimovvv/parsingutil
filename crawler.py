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
import logging
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


def url_to_file_path(url: str, site_code: str, output_dir: str) -> str:
    """Преобразует URL в путь к файлу с древовидной структурой."""
    parsed = urlparse(url)
    path = parsed.path.strip('/')
    query = parsed.query
    base_dir = os.path.join(output_dir, site_code)
    
    if not path or path == '/':
        dir_path = base_dir
        file_name = 'index.md'
    else:
        path_parts = [p for p in path.split('/') if p]
        safe_parts = [re.sub(r'[<>:"|?*\\]', '_', p) for p in path_parts if p]
        
        if not safe_parts:
            dir_path = base_dir
            file_name = 'index.md'
        else:
            if query:
                query_normalized = re.sub(r'[<>:"|?*\\]', '_', query.replace('=', '_').replace('&', '_')[:50])
                file_name = f"{safe_parts[-1]}_{query_normalized}.md"
                dir_parts = safe_parts[:-1]
            else:
                if '.' in safe_parts[-1] and len(safe_parts[-1].split('.')[-1]) <= 4:
                    file_name = safe_parts[-1].replace('.', '_') + '.md'
                    dir_parts = safe_parts[:-1]
                else:
                    file_name = 'index.md'
                    dir_parts = safe_parts
            
            dir_path = os.path.join(base_dir, *dir_parts) if dir_parts else base_dir
    
    os.makedirs(dir_path, exist_ok=True)
    return os.path.join(dir_path, file_name)


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
    
    def __init__(self, base_url: str, site_code: str, max_pages: int, output_dir: str):
        self.base_url = base_url
        self.site_code = site_code
        self.max_pages = max_pages
        self.output_dir = output_dir
        self.base_domain_netloc = urlparse(base_url).netloc
        self.logger = setup_logging(output_dir, site_code)
        
        self.stats = {
            'start_time': time.time(),
            'end_time': None,
            'success': 0,
            'failed': 0,
            'errors': defaultdict(int),
            'saved_files': [],
            'links_found': 0,
            'links_added': 0,
            'queue_remaining': 0
        }
        
        self.visited: Set[str] = set()
        self.queue: deque = deque()
    
    def _create_configs(self):
        """Создает конфигурации браузера и краулера."""
        browser_config = BrowserConfig(
            headless=True,
            browser_type="chromium",
            user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
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
            wait_until="networkidle",
            page_timeout=30000,
            simulate_user=True,
            scroll_delay=1.0,
            max_scroll_steps=2
        )
        
        return browser_config, crawler_config
    
    def _extract_markdown(self, result) -> Optional[str]:
        """Извлекает markdown контент из результата."""
        if not hasattr(result, 'markdown'):
            return None
        
        if hasattr(result.markdown, 'fit_markdown') and result.markdown.fit_markdown:
            return result.markdown.fit_markdown
        elif hasattr(result.markdown, 'raw_markdown') and result.markdown.raw_markdown:
            return result.markdown.raw_markdown
        elif isinstance(result.markdown, str):
            return result.markdown
        return None
    
    def _record_error(self, url: str, error_type: str):
        """Записывает ошибку в статистику."""
        self.stats['failed'] += 1
        self.stats['errors'][error_type] += 1
        self.logger.warning(f"FAIL {url}: {error_type}")
        print(f"FAIL {url}: {error_type}")
    
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
    
    async def _crawl_page(self, crawler: AsyncWebCrawler, crawler_config: CrawlerRunConfig, url: str):
        """Обходит одну страницу."""
        normalized_url = normalize_url_for_deep_crawl(url, self.base_url)
        
        if normalized_url in self.visited:
            return
        
        self.visited.add(normalized_url)
        self.logger.info(f"[{len(self.visited)}/{self.max_pages}] {normalized_url}")
        
        try:
            delay = 4.0 if len(self.visited) == 1 else random.uniform(4.0, 7.0)
            self.logger.debug(f"Задержка: {delay:.2f}с")
            await asyncio.sleep(delay)
            
            self.logger.debug(f"Запрос: {normalized_url}")
            result = await crawler.arun(normalized_url, config=crawler_config)
            await asyncio.sleep(0.5)
            
            if not result.success:
                error_msg = getattr(result, 'error_message', 'Неизвестная ошибка')
                self._record_error(url, error_msg)
                return
            
            markdown_content = self._extract_markdown(result)
            
            if not markdown_content:
                self._record_error(url, "Нет markdown контента")
                return
            
            status_code = getattr(result, 'status_code', None)
            
            if status_code == 403:
                self._record_error(url, "403 Forbidden")
                return
            elif status_code == 404:
                self._record_error(url, "404 Not Found")
                return
            
            file_path = url_to_file_path(url, self.site_code, self.output_dir)
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(markdown_content)
            
            self.stats['success'] += 1
            self.stats['saved_files'].append(file_path)
            self.logger.info(f"OK {url} -> {file_path}")
            print(f"OK {url} -> {file_path}")
            
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
                        
        except Exception as e:
            self._record_error(url, str(e))
    
    async def crawl(self) -> None:
        """Основной метод обхода сайта."""
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.logger.info("=" * 60)
        self.logger.info("НАЧАЛО ОБХОДА")
        self.logger.info("=" * 60)
        self.logger.info(f"URL: {self.base_url}")
        self.logger.info(f"Site code: {self.site_code}")
        self.logger.info(f"Максимум страниц: {self.max_pages}")
        self.logger.info(f"Директория: {self.output_dir}")
        
        normalized_base = normalize_url_for_deep_crawl(self.base_url, self.base_url)
        self.logger.debug(f"Домен: {self.base_domain_netloc}, нормализованный URL: {normalized_base}")
        
        self.queue.append(normalized_base)
        browser_config, crawler_config = self._create_configs()
        
        async with AsyncWebCrawler(config=browser_config) as crawler:
            self.logger.info("Инициализирован AsyncWebCrawler")
            
            while self.queue and len(self.visited) < self.max_pages:
                url = self.queue.popleft()
                await self._crawl_page(crawler, crawler_config, url)
        
        self.stats['end_time'] = time.time()
        self.stats['queue_remaining'] = len(self.queue)
        
        self.logger.info("=" * 60)
        self.logger.info("ЗАВЕРШЕНИЕ ОБХОДА")
        self.logger.info("=" * 60)
        self.logger.info(f"Обработано: {self.stats['success'] + self.stats['failed']} | Успешно: {self.stats['success']} | Ошибок: {self.stats['failed']}")
        
        duration = self.stats['end_time'] - self.stats['start_time']
        self.logger.info(f"Время: {duration:.1f}с ({duration/60:.1f} мин) | В очереди: {self.stats['queue_remaining']}")
        
        print_stats(self.stats, self.base_url, self.max_pages)


async def crawl_site(base_url: str, site_code: str, max_pages: int, output_dir: str) -> None:
    """Обходит сайт и сохраняет контент страниц в .md файлы."""
    crawler = SiteCrawler(base_url, site_code, max_pages, output_dir)
    await crawler.crawl()


def main() -> None:
    """Точка входа скрипта."""
    parser = argparse.ArgumentParser(description="Обход сайта и сохранение контента страниц в .md файлы")
    parser.add_argument("--base-url", "-u", required=True, help="Стартовый URL сайта")
    parser.add_argument("--site-code", "-s", required=True, help="Произвольный префикс для имен файлов (например: example, mysite)")
    parser.add_argument("--max-pages", "-m", type=int, default=50, help="Максимальное количество страниц (по умолчанию: 50)")
    parser.add_argument("--output-dir", "-o", default="./output", help="Директория для сохранения (по умолчанию: ./output)")
    
    args = parser.parse_args()
    asyncio.run(crawl_site(args.base_url, args.site_code, args.max_pages, args.output_dir))


if __name__ == "__main__":
    main()
