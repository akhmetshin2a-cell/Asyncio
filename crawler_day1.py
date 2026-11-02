"""Дни 1–7: асинхронная загрузка, HTML-парсинг и обход сайта с очередью."""

import argparse
import copy
import inspect
import tracemalloc
from html import escape
from logging.handlers import RotatingFileHandler
from lxml import etree
import asyncio
import logging
import json
import heapq
import csv
import io
import codecs
import sqlite3
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path
import re
import math
import random
import time
from collections import Counter, deque
from contextvars import ContextVar
from socket import gaierror
from email.utils import parsedate_to_datetime
from contextlib import asynccontextmanager
from itertools import count

import aiofiles
import aiosqlite
from time import perf_counter
from typing import Optional
from urllib.parse import quote, urldefrag, urljoin, urlsplit

import aiohttp
from aiohttp import web
from bs4 import BeautifulSoup, Comment

logger = logging.getLogger(__name__)


_retry_context = ContextVar("crawler_retry_context", default=None)


class CrawlerError(aiohttp.ClientError):
    def __init__(self, message, *, url=None, status=None, headers=None):
        super().__init__(message)
        self.url = url
        self.status = status
        self.headers = headers or {}
        self.recorded = False


class TransientError(CrawlerError):
    """Временный HTTP-сбой или таймаут."""


class PermanentError(CrawlerError):
    """Запрос не следует повторять: HTTP 4xx, запрет или неверный URL."""


class NetworkError(CrawlerError):
    """Ошибка соединения, DNS или чтения сетевого ответа."""


class ParseError(CrawlerError):
    """Не удалось извлечь часть данных из HTML."""


def classify_error(exc, url=None):
    if isinstance(exc, CrawlerError):
        if exc.url is None:
            exc.url = url
        return exc
    message = f"{type(exc).__name__}: {exc}"
    if isinstance(exc, asyncio.TimeoutError):
        return TransientError(message, url=url)
    if isinstance(exc, aiohttp.ClientResponseError):
        kind = TransientError if exc.status in {408, 429, 500, 502, 503, 504} else PermanentError
        return kind(message, url=url, status=exc.status, headers=exc.headers)
    if isinstance(exc, (aiohttp.ClientConnectorCertificateError, aiohttp.ClientSSLError, aiohttp.InvalidURL)):
        return PermanentError(message, url=url)
    if isinstance(exc, (aiohttp.ClientConnectionError, aiohttp.ClientPayloadError, ConnectionError, gaierror)):
        return NetworkError(message, url=url)
    if isinstance(exc, aiohttp.ClientError):
        return PermanentError(message, url=url)
    return exc  # Ошибки программирования не маскируем сетевыми исключениями.


class RetryStrategy:
    """coro — вызываемая async-функция: для каждой попытки нужна новая корутина."""

    def __init__(
        self, max_retries: int = 3, backoff_factor: float = 2.0, retry_on: list = None,
        *, retry_limits=None, backoff_by_type=None, status_limits=None,
    ):
        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 0:
            raise ValueError("max_retries должен быть неотрицательным целым числом")
        self.max_retries = max_retries
        self.backoff_factor = nonnegative_number(backoff_factor, "backoff_factor")
        self.retry_on = tuple([TransientError, NetworkError] if retry_on is None else retry_on)
        self.retry_limits = {TransientError: max_retries, NetworkError: min(2, max_retries)}
        self.retry_limits.update(retry_limits or {})
        self.backoff_by_type = {TransientError: self.backoff_factor, NetworkError: self.backoff_factor / 2}
        self.backoff_by_type.update(backoff_by_type or {})
        self.status_limits = {500: 1}
        self.status_limits.update(status_limits or {})
        for kind in [*self.retry_on, *self.retry_limits, *self.backoff_by_type]:
            if not isinstance(kind, type) or not issubclass(kind, Exception):
                raise ValueError("Типы ошибок должны быть классами Exception")
        for limit in [*self.retry_limits.values(), *self.status_limits.values()]:
            if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
                raise ValueError("Лимиты повторов должны быть неотрицательными целыми числами")
        for delay in self.backoff_by_type.values():
            nonnegative_number(delay, "backoff")
        self.errors = []
        self.errors_by_type = Counter()
        self.permanent_urls = set()
        self.retries = 0
        self.retry_attempts = 0
        self.successful_retries = 0
        self.retry_wait_seconds = 0.0
        self.retry_execution_seconds = 0.0

    def record_error(self, error, url, attempt=1, delay=None, final=True):
        kind = type(error).__name__
        category = next((cls.__name__ for cls in (TransientError, PermanentError, NetworkError, ParseError)
                         if isinstance(error, cls)), kind)
        self.errors_by_type[category] += 1
        if isinstance(error, PermanentError) and url:
            self.permanent_urls.add(url)
        self.errors.append({
            "url": url, "type": kind, "category": category, "message": str(error),
            "status": getattr(error, "status", None), "attempt": attempt,
            "next_delay": delay, "final": final, "timestamp": time.time(),
        })
        if isinstance(error, CrawlerError):
            error.recorded = True

    def _setting(self, mapping, error, default):
        for cls in type(error).__mro__:
            if cls in mapping:
                return mapping[cls]
        return default

    def _retry_after(self, error):
        value = getattr(error, "headers", {}).get("Retry-After", "")
        try:
            seconds = float(value)
            return max(0, seconds) if math.isfinite(seconds) else 0
        except ValueError:
            try:
                return max(0, parsedate_to_datetime(value).timestamp() - time.time())
            except (ValueError, TypeError, OverflowError):
                return 0

    async def execute_with_retry(self, coro, *args, **kwargs):
        if not callable(coro):
            raise TypeError("Передайте async-функцию, например crawler.fetch_url, без её вызова")
        url = kwargs.get("url") or (args[0] if args and isinstance(args[0], str) else None)
        parent = _retry_context.get()
        if url is not None and parent is not None and url in parent.get("urls", {parent["url"]}):
            # Внешняя стратегия уже управляет этим запросом: не умножаем повторы.
            return await coro(*args, **kwargs)
        type_failures = Counter()
        status_failures = Counter()
        for attempt in range(self.max_retries + 1):
            token = _retry_context.set({"url": url, "urls": {url}, "attempt": attempt})
            started = perf_counter()
            if attempt:
                self.retry_attempts += 1
            logger.info("%s | Попытка %s/%s", url, attempt + 1, self.max_retries + 1)
            try:
                result = await coro(*args, **kwargs)
            except asyncio.CancelledError:
                logger.info("%s | Попытка %s отменена", url, attempt + 1)
                raise
            except Exception as original:
                error = classify_error(original, url)
                type_failures[type(error)] += 1
                status = getattr(error, "status", None)
                status_failures[status] += 1
                limit = self._setting(self.retry_limits, error, self.max_retries)
                retry = (
                    not isinstance(error, (PermanentError, ParseError))
                    and isinstance(error, self.retry_on)
                    and attempt < self.max_retries
                    and type_failures[type(error)] <= limit
                    and status_failures[status] <= self.status_limits.get(status, self.max_retries)
                )
                factor = self._setting(self.backoff_by_type, error, self.backoff_factor)
                if status == 429:
                    factor *= 2
                delay = max(min(60.0, factor * 2 ** min(attempt, 16)), self._retry_after(error)) if retry else None
                self.record_error(error, url, attempt + 1, delay, final=not retry)
                if not retry:
                    logger.error("%s | %s | попытка %s | итог: неудача | %s", url, type(error).__name__, attempt + 1, error)
                    if error is original:
                        raise
                    raise error from original
                logger.warning("%s | %s | попытка %s | повтор через %.3f с | %s", url, type(error).__name__, attempt + 1, delay, error)
            else:
                if attempt:
                    self.successful_retries += 1
                logger.info("%s | Попытка %s | итог: успех", url, attempt + 1)
                return result
            finally:
                if attempt:
                    self.retry_execution_seconds += perf_counter() - started
                _retry_context.reset(token)
            self.retries += 1
            wait_started = perf_counter()
            try:
                await asyncio.sleep(delay)
            finally:
                self.retry_wait_seconds += perf_counter() - wait_started

    def get_stats(self):
        return {
            "errors_by_type": dict(self.errors_by_type), "total_errors": len(self.errors),
            "retries": self.retries, "retry_attempts": self.retry_attempts,
            "successful_retries": self.successful_retries,
            "retry_wait_seconds": self.retry_wait_seconds,
            "average_retry_time": (self.retry_wait_seconds + self.retry_execution_seconds) / self.retry_attempts if self.retry_attempts else 0.0,
            "permanent_error_urls": sorted(self.permanent_urls),
        }


class HTMLParser:
    """Извлечение данных из HTML; по умолчанию сохраняются и внешние ссылки."""

    def __init__(self, internal_only: bool = False):
        self.internal_only = internal_only

    @staticmethod
    def empty_result(url: str) -> dict:
        return {
            "url": url, "title": "", "text": "", "links": [],
            "metadata": {}, "images": [], "headings": [],
            "tables": [], "lists": [], "errors": [],
        }

    @staticmethod
    def _absolute_url(value: str, base_url: str) -> Optional[str]:
        """Принимаем только абсолютные HTTP(S) URL без пробелов и credentials."""
        value = value.strip()
        if not value or value.startswith("#"):
            return None
        if any(character.isspace() or ord(character) < 32 for character in value):
            return None
        try:
            absolute = urldefrag(urljoin(base_url, value))[0]
            parts = urlsplit(absolute)
            if parts.scheme not in {"http", "https"} or not parts.hostname:
                return None
            if parts.username is not None or parts.password is not None:
                return None
            parts.port  # Проверяем, что порт корректен.
            return absolute
        except ValueError:
            return None

    def extract_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        links = []
        seen = set()
        for anchor in soup.find_all("a", href=True):
            url = self._absolute_url(anchor["href"], base_url)
            if url is None:
                continue
            if self.internal_only and urlsplit(url).netloc.lower() != urlsplit(base_url).netloc.lower():
                continue
            if url not in seen:
                seen.add(url)
                links.append(url)
        return links

    def extract_text(self, soup: BeautifulSoup, selector: str = None) -> str:
        if selector is not None:
            roots = soup.select(selector)
        else:
            roots = [soup.find("main") or soup.find("article") or soup.body or soup]
        # Не удаляем теги из soup: другие методы ещё будут использовать дерево.
        parts = []
        for root in roots:
            for text in root.find_all(string=True):
                if isinstance(text, Comment):
                    continue
                if text.find_parent(["script", "style", "noscript", "template", "title"]):
                    continue
                parts.extend(text.split())
        return " ".join(parts)

    def extract_metadata(self, soup: BeautifulSoup) -> dict:
        metadata = {
            "title": soup.title.get_text(" ", strip=True) if soup.title else "",
            "description": "", "keywords": "",
        }
        for tag in soup.find_all("meta"):
            name = str(tag.get("name", "")).lower()
            if name in {"description", "keywords"}:
                metadata[name] = tag.get("content", "")
        return metadata

    def extract_images(self, soup: BeautifulSoup, base_url: str) -> list[dict]:
        images = []
        for tag in soup.find_all("img", src=True):
            src = self._absolute_url(tag["src"], base_url)
            if src:
                images.append({"src": src, "alt": tag.get("alt", "")})
        return images

    def extract_headings(self, soup: BeautifulSoup) -> list[dict]:
        return [
            {"level": int(tag.name[1]), "text": tag.get_text(" ", strip=True)}
            for tag in soup.find_all(["h1", "h2", "h3"])
        ]

    def extract_tables(self, soup: BeautifulSoup) -> list[list[list[str]]]:
        tables = []
        for table in soup.find_all("table"):
            rows = []
            for row in table.find_all("tr"):
                if row.find_parent("table") is table:
                    rows.append([
                        cell.get_text(" ", strip=True)
                        for cell in row.find_all(["th", "td"], recursive=False)
                    ])
            tables.append(rows)
        return tables

    def extract_lists(self, soup: BeautifulSoup) -> list[dict]:
        return [
            {
                "type": tag.name,
                "items": [item.get_text(" ", strip=True) for item in tag.find_all("li", recursive=False)],
            }
            for tag in soup.find_all(["ul", "ol"])
        ]

    async def parse_html(self, html: str, url: str) -> dict:
        # BeautifulSoup выполняет синхронную работу: переносим её из event loop.
        return await asyncio.to_thread(self._parse_html, html, url)

    def _parse_html(self, html: str, url: str) -> dict:
        result = self.empty_result(url)
        try:
            soup = BeautifulSoup(html, "lxml")
        except Exception as exc:
            message = f"Не удалось построить HTML-дерево: {type(exc).__name__}: {exc}"
            logger.warning("%s | %s", url, message)
            result["errors"].append(message)
            return result

        if not html.strip() or soup.title is None:
            logger.warning("%s | Пустой HTML или отсутствует title", url)

        # Каждый раздел извлекаем независимо: ошибка одного не стирает остальные.
        extractors = {
            "metadata": lambda: self.extract_metadata(soup),
            "text": lambda: self.extract_text(soup),
            "links": lambda: self.extract_links(soup, url),
            "images": lambda: self.extract_images(soup, url),
            "headings": lambda: self.extract_headings(soup),
            "tables": lambda: self.extract_tables(soup),
            "lists": lambda: self.extract_lists(soup),
        }
        for field, extract in extractors.items():
            try:
                result[field] = extract()
            except Exception as exc:
                message = f"{field}: {type(exc).__name__}: {exc}"
                logger.warning("%s | Ошибка парсинга %s", url, message)
                result["errors"].append(message)
        result["title"] = result["metadata"].get("title", "")
        return result


class CrawlerQueue:
    """Очередь без повторов. Меньшее число = более высокий приоритет.

    get_next не ждёт появления новых элементов: пустая очередь возвращает None.
    Очередь предназначена для задач одного event loop.
    """

    def __init__(self):
        self._heap = []
        self._counter = count()
        self._known = set()
        self._active = set()
        self.processed = set()
        self.failed = {}

    def add_url(self, url: str, priority: int = 0):
        if url in self._known:
            return
        self._known.add(url)
        heapq.heappush(self._heap, (priority, next(self._counter), url))

    @property
    def next_priority(self):
        return self._heap[0][0] if self._heap else None

    async def get_next(self) -> Optional[str]:
        if not self._heap:
            return None
        _, _, url = heapq.heappop(self._heap)
        self._active.add(url)
        return url

    def mark_processed(self, url: str):
        if url not in self._active:
            raise ValueError("URL не находится в обработке")
        self._active.remove(url)
        self.processed.add(url)

    def mark_failed(self, url: str, error: str):
        if url not in self._active:
            raise ValueError("URL не находится в обработке")
        self._active.remove(url)
        self.failed[url] = error

    def get_stats(self) -> dict:
        return {
            "total": len(self._known), "queued": len(self._heap),
            "active": len(self._active), "processed": len(self.processed),
            "failed": len(self.failed),
        }


class SemaphoreManager:
    """Общий лимит и отдельный лимит на hostname (для всех портов домена)."""

    def __init__(self, max_concurrent: int = 10, per_domain: int = 2):
        for value in (max_concurrent, per_domain):
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise ValueError("Лимиты должны быть положительными целыми числами")
        self._global = asyncio.Semaphore(max_concurrent)
        self._domains = {}
        self.per_domain = per_domain
        self.active = 0
        self.active_by_domain = {}
        self.peak_active = 0
        self.peak_by_domain = {}

    @asynccontextmanager
    async def acquire(self, url: str):
        domain = (urlsplit(url).hostname or "").lower()
        semaphore = self._domains.setdefault(domain, asyncio.Semaphore(self.per_domain))
        # Сначала ждём домен, чтобы ожидающие его задачи не занимали общий лимит.
        async with semaphore:
            async with self._global:
                self.active += 1
                self.active_by_domain[domain] = self.active_by_domain.get(domain, 0) + 1
                self.peak_active = max(self.peak_active, self.active)
                self.peak_by_domain[domain] = max(
                    self.peak_by_domain.get(domain, 0), self.active_by_domain[domain]
                )
                try:
                    yield
                finally:
                    self.active -= 1
                    self.active_by_domain[domain] -= 1

    def get_stats(self) -> dict:
        return {
            "active": self.active, "active_by_domain": dict(self.active_by_domain),
            "peak_active": self.peak_active, "peak_by_domain": dict(self.peak_by_domain),
        }


def nonnegative_number(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
        raise ValueError(f"{name} должен быть конечным неотрицательным числом")
    return float(value)


class RateLimiter:
    """Равномерные разрешения без накопления burst; монотонные часы."""

    def __init__(self, requests_per_second: float = 1.0, per_domain: bool = True):
        self.rate = nonnegative_number(requests_per_second, "requests_per_second")
        if self.rate == 0:
            raise ValueError("requests_per_second должен быть больше нуля")
        self.per_domain = per_domain
        self._locks = {}
        self._last = {}
        self._next = {}

    async def acquire(self, domain: str = None, *, min_delay=0.0, jitter=0.0):
        delay = nonnegative_number(min_delay, "min_delay")
        spread = nonnegative_number(jitter, "jitter")
        key = (domain or "").lower() if self.per_domain else "global"
        lock = self._locks.setdefault(key, asyncio.Lock())
        async with lock:
            interval = max(1 / self.rate, delay) + random.uniform(0, spread)
            now = perf_counter()
            if key in self._last:
                wait = max(self._next[key], self._last[key] + interval) - now
                if wait > 0:
                    await asyncio.sleep(wait)
            granted = perf_counter()
            self._last[key] = granted
            self._next[key] = granted + interval


class RobotsDeniedError(PermanentError):
    """Запрос не отправлен: robots.txt не разрешает загрузку."""


class RobotsParser:
    """Кэш по origin (схема, hostname, порт); загрузчик можно передать снаружи."""

    def __init__(self, downloader=None, user_agent="LearningCrawler/1.0", cache_ttl=3600):
        self.downloader = downloader
        self.user_agent = user_agent
        self.cache_ttl = nonnegative_number(cache_ttl, "cache_ttl")
        self.cache = {}
        self._locks = {}
        self._last_origin = None

    @staticmethod
    def origin(url):
        parts = urlsplit(url)
        if parts.scheme not in {"http", "https"} or not parts.hostname:
            raise ValueError("Нужен абсолютный HTTP(S) URL")
        host = parts.hostname.lower()
        host = f"[{host}]" if ":" in host else host
        port = parts.port
        if port and port != (443 if parts.scheme == "https" else 80):
            host += f":{port}"
        return f"{parts.scheme}://{host}"

    @staticmethod
    def parse_rules(text):
        groups = []
        group = None
        has_directives = False
        for raw in text.lstrip("\ufeff").splitlines():
            line = raw.split("#", 1)[0].strip()
            if ":" not in line:
                continue
            name, value = (part.strip() for part in line.split(":", 1))
            name = name.lower()
            if name == "user-agent":
                if group is None or has_directives:
                    group = {"agents": [], "rules": [], "delay": 0.0}
                    groups.append(group)
                    has_directives = False
                group["agents"].append(value.lower())
            elif group is not None and name in {"allow", "disallow", "crawl-delay"}:
                has_directives = True
                if name == "crawl-delay":
                    try:
                        group["delay"] = max(group["delay"], nonnegative_number(float(value), "Crawl-delay"))
                    except ValueError:
                        logger.warning("Некорректный Crawl-delay: %s", value)
                elif value:
                    group["rules"].append((value, name == "allow"))
        return groups

    async def _download(self, url):
        if self.downloader:
            return await self.downloader(url)
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=30), headers={"User-Agent": self.user_agent}
        ) as session:
            async with session.get(url, allow_redirects=False) as response:
                response.raise_for_status()
                return {"status": response.status, "headers": dict(response.headers), "text": await response.text(errors="replace")}

    async def fetch_robots(self, base_url: str) -> dict:
        origin = self.origin(base_url)
        lock = self._locks.setdefault(origin, asyncio.Lock())
        async with lock:
            cached = self.cache.get(origin)
            if cached and cached["expires"] > perf_counter():
                self._last_origin = origin
                return cached
            entry = {"groups": [], "deny_all": False, "error": "", "status": None}
            try:
                url = origin + "/robots.txt"
                for _ in range(6):
                    response = await self._download(url)
                    if response["status"] in {301, 302, 303, 307, 308}:
                        target = HTMLParser._absolute_url(response["headers"].get("Location", ""), url)
                        if target is None:
                            raise aiohttp.ClientError("Некорректный редирект robots.txt")
                        url = target
                        continue
                    entry["status"] = response["status"]
                    status = response["status"]
                    entry["deny_all"] = status >= 500 or status in {401, 403, 429}
                    if status < 400:
                        entry["groups"] = self.parse_rules(response["text"])
                    break
                else:
                    raise aiohttp.ClientError("Слишком много редиректов robots.txt")
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                status = getattr(exc, "status", None)
                entry["status"] = status
                entry["deny_all"] = status is None or status >= 500 or status in {401, 403, 429}
                entry["error"] = f"{type(exc).__name__}: {exc}"
                logger.warning("%s | robots.txt: %s", origin, entry["error"])
            ttl = min(self.cache_ttl, 60) if entry["deny_all"] else self.cache_ttl
            entry["expires"] = perf_counter() + ttl
            self.cache[origin] = entry
            self._last_origin = origin
            return entry

    def _groups(self, entry, user_agent):
        token = user_agent.split("/", 1)[0].lower()
        scored = []
        for group in entry["groups"]:
            score = max((len(agent) for agent in group["agents"] if agent != "*" and agent and agent in token), default=0)
            if score or "*" in group["agents"]:
                scored.append((score, group))
        best = max((score for score, _ in scored), default=-1)
        return [group for score, group in scored if score == best]

    @staticmethod
    def _path(value):
        # Приводим Unicode и %-escape к сопоставимому виду, сохраняя reserved-символы.
        value = quote(value, safe="/%?=&:;,+!$'()*@[]~-._")
        def normalize(match):
            char = chr(int(match.group()[1:], 16))
            return char if char.isascii() and (char.isalnum() or char in "-._~") else match.group().upper()
        return re.sub(r"%[0-9a-fA-F]{2}", normalize, value)

    def can_fetch(self, url: str, user_agent: str = "*") -> bool:
        entry = self.cache.get(self.origin(url))
        if entry is None or entry["deny_all"]:
            return False
        parts = urlsplit(url)
        path = self._path((parts.path or "/") + ("?" + parts.query if parts.query else ""))
        matches = []
        for group in self._groups(entry, user_agent):
            for rule, allow in group["rules"]:
                normalized = self._path(rule)
                end = normalized.endswith("$")
                pattern = normalized[:-1] if end else normalized
                expression = "^" + ".*".join(re.escape(piece) for piece in pattern.split("*")) + ("$" if end else "")
                if re.search(expression, path):
                    matches.append((len(pattern.replace("*", "")), allow))
        return max(matches)[1] if matches else True

    def get_crawl_delay(self, user_agent: str = "*", *, base_url=None) -> float:
        origin = self.origin(base_url) if base_url else self._last_origin
        entry = self.cache.get(origin)
        if entry is None:
            return 0.0
        return max((group["delay"] for group in self._groups(entry, user_agent)), default=0.0)


def json_default(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    raise TypeError(f"Не поддерживается JSON: {type(value).__name__}")


def normalize_record(data):
    """Единая схема, JSON-совместимые значения и сохранение дополнительных полей."""
    record = {
        "url": "", "title": "", "text": "", "links": [], "metadata": {},
        "crawled_at": datetime.now(timezone.utc), "status_code": 0, "content_type": "",
        **data,
    }
    if not isinstance(record["url"], str) or not record["url"]:
        raise ValueError("У записи должен быть непустой url")
    for key in ("title", "text", "content_type"):
        if not isinstance(record[key], str):
            raise ValueError(f"{key} должен быть строкой")
    if not isinstance(record["links"], list) or not all(isinstance(link, str) for link in record["links"]):
        raise ValueError("links должен быть списком строк")
    if not isinstance(record["metadata"], dict):
        raise ValueError("metadata должен быть словарём")
    if isinstance(record["status_code"], bool) or not isinstance(record["status_code"], int):
        raise ValueError("status_code должен быть целым числом")
    stamp = record["crawled_at"]
    if not isinstance(stamp, (str, datetime)):
        raise ValueError("crawled_at должен быть datetime или ISO-строкой")
    if isinstance(stamp, str):
        datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    return json.loads(json.dumps(record, ensure_ascii=False, default=json_default, allow_nan=False))


class StorageError(Exception):
    pass


class DataStorage(ABC):
    """Один экземпляр хранилища безопасен для задач одного event loop."""

    def __init__(self, max_retries=2, backoff_factor=0.1):
        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 0:
            raise ValueError("max_retries должен быть неотрицательным целым числом")
        self.max_retries = max_retries
        self.backoff_factor = nonnegative_number(backoff_factor, "backoff_factor")
        self._lock = asyncio.Lock()
        self._closed = False
        self.saved = 0
        self.retries = 0
        self.errors = []

    @abstractmethod
    async def save(self, data: dict):
        pass

    async def save_batch(self, records):
        for record in records:
            await self.save(record)

    @abstractmethod
    async def close(self):
        pass

    async def _execute(self, operation, count=1):
        if self._closed:
            raise StorageError("Хранилище уже закрыто")
        for attempt in range(self.max_retries + 1):
            try:
                await operation()
            except Exception as exc:
                retry = isinstance(exc, (OSError, sqlite3.OperationalError)) and attempt < self.max_retries
                self.errors.append({"type": type(exc).__name__, "message": str(exc), "attempt": attempt + 1, "final": not retry})
                logger.warning("%s | ошибка записи | попытка %s | %s", type(self).__name__, attempt + 1, exc)
                if not retry:
                    raise StorageError(f"{type(self).__name__}: {exc}") from exc
                self.retries += 1
                await asyncio.sleep(min(60, self.backoff_factor * 2 ** min(attempt, 16)))
            else:
                self.saved += count
                return

    def get_stats(self):
        return {"saved": self.saved, "retries": self.retries, "failed_operations": sum(error["final"] for error in self.errors)}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        await self.close()


class FileStorage(DataStorage):
    """Откат неудачной записи перед повтором; записи сериализованы lock."""

    def __init__(self, path, **kwargs):
        super().__init__(**kwargs)
        self.path = Path(path)
        self._ready = False
        self._size = 0
        self._count = 0
        self._broken = False

    async def _execute(self, operation, count=1):
        # aiofiles выполняет I/O в потоке. Отмена корутины не останавливает поток.
        # Держим lock до завершения всей записи и обновления счётчиков.
        task = asyncio.create_task(super()._execute(operation, count))
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    continue
                except Exception:
                    break
            if not task.cancelled():
                task.exception()  # Ошибка уже зарегистрирована в DataStorage.
            raise

    async def _append(self, payload, offset, tail=b""):
        if self._broken:
            raise StorageError("Хранилище повреждено после неудачного отката; запись остановлена")
        async with aiofiles.open(self.path, "r+b") as file:
            await file.seek(offset)
            old_tail = await file.read()
            old_size = self._size
            try:
                await file.seek(offset)
                await file.write(payload + tail)
                await file.truncate(offset + len(payload) + len(tail))
                await file.flush()
            except BaseException:
                try:
                    await file.seek(offset)
                    await file.write(old_tail)
                    await file.truncate(old_size)
                    await file.flush()
                except Exception as rollback_error:
                    self._broken = True
                    # После неудачного отката повторять append небезопасно.
                    raise StorageError(f"Не удалось откатить запись: {rollback_error}") from rollback_error
                raise
            self._size = offset + len(payload) + len(tail)

    async def close(self):
        async with self._lock:
            self._closed = True  # Каждый save уже сбрасывает и закрывает файловый буфер.


class JSONStorage(FileStorage):
    def __init__(self, path, *, json_lines=False, indent=2, **kwargs):
        super().__init__(path, **kwargs)
        self.json_lines = json_lines
        self.indent = indent
        self._end = 0

    def _inspect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists() or self.path.stat().st_size == 0:
            self.path.write_bytes(b"" if self.json_lines else b"[\n]\n")
        if self.json_lines:
            count = 0
            with self.path.open(encoding="utf-8") as file:
                for line in file:
                    if line.strip():
                        if not isinstance(json.loads(line), dict):
                            raise ValueError("Каждая строка JSONL должна содержать объект")
                        count += 1
            size = self.path.stat().st_size
            return count, size, size
        raw = self.path.read_bytes()
        records = json.loads(raw)
        if not isinstance(records, list) or not all(isinstance(row, dict) for row in records):
            raise ValueError("JSON-файл должен содержать массив объектов")
        return len(records), len(raw), raw.rfind(b"]")

    async def save(self, data: dict):
        async with self._lock:
            async def write():
                record = await asyncio.to_thread(normalize_record, data)
                payload = await asyncio.to_thread(json.dumps, record, ensure_ascii=False, indent=None if self.json_lines else self.indent)
                if not self._ready:
                    self._count, self._size, self._end = await asyncio.to_thread(self._inspect)
                    self._ready = True
                if self.json_lines:
                    separator = b""
                    if self._size:
                        async with aiofiles.open(self.path, "rb") as file:
                            await file.seek(-1, 2)
                            if await file.read(1) != b"\n":
                                separator = b"\n"
                    await self._append(separator + payload.encode("utf-8") + b"\n", self._size)
                else:
                    prefix = b",\n" if self._count else b""
                    await self._append(prefix + payload.encode("utf-8"), self._end, b"\n]\n")
                    self._end = self._size - 2
                self._count += 1
            await self._execute(write)

    async def read_all(self):
        def read():
            if not self.path.exists() or self.path.stat().st_size == 0:
                return []
            with self.path.open(encoding="utf-8") as file:
                return [json.loads(line) for line in file if line.strip()] if self.json_lines else json.load(file)
        async with self._lock:
            return await asyncio.to_thread(read)


class CSVStorage(FileStorage):
    def __init__(self, path, *, encoding="utf-8", **kwargs):
        super().__init__(path, **kwargs)
        self.encoding = codecs.lookup(encoding).name
        if self.encoding not in {"utf-8", "utf-8-sig", "cp1251"}:
            raise ValueError("Поддержаны utf-8, utf-8-sig и cp1251")
        self.fieldnames = None

    def _inspect(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.touch(exist_ok=True)
        with self.path.open(encoding=self.encoding, newline="") as file:
            reader = csv.DictReader(file)
            headers = reader.fieldnames
            count = sum(1 for _ in reader)
        return headers, count, self.path.stat().st_size

    async def save(self, data: dict):
        async with self._lock:
            async def write():
                record = await asyncio.to_thread(normalize_record, data)
                if not self._ready:
                    self.fieldnames, self._count, self._size = await asyncio.to_thread(self._inspect)
                    self._ready = True
                headers = self.fieldnames or list(record)
                if set(record) - set(headers):
                    raise ValueError("Новые поля CSV отсутствуют в заголовке; используйте единый формат записей")
                def serialize():
                    buffer = io.StringIO(newline="")
                    writer = csv.DictWriter(buffer, fieldnames=headers)
                    if not self._size:
                        writer.writeheader()
                    writer.writerow({key: json.dumps(value, ensure_ascii=False) for key, value in record.items()})
                    encoded = buffer.getvalue().encode(self.encoding)
                    if self._size and self.encoding == "utf-8-sig":
                        encoded = encoded.removeprefix(codecs.BOM_UTF8)
                    return encoded
                payload = await asyncio.to_thread(serialize)
                await self._append(payload, self._size)
                self.fieldnames = headers
                self._count += 1
            await self._execute(write)

    async def read_all(self):
        def read():
            if not self.path.exists() or self.path.stat().st_size == 0:
                return []
            with self.path.open(encoding=self.encoding, newline="") as file:
                return [{key: json.loads(value) if value else None for key, value in row.items()} for row in csv.DictReader(file)]
        async with self._lock:
            return await asyncio.to_thread(read)


class SQLiteStorage(DataStorage):
    def __init__(self, path, **kwargs):
        super().__init__(**kwargs)
        self.path = Path(path)
        self._db = None

    async def _init_db(self):
        if self._db is not None:
            return
        await asyncio.to_thread(self.path.parent.mkdir, parents=True, exist_ok=True)
        db = await aiosqlite.connect(self.path)
        try:
            await db.executescript("""
                CREATE TABLE IF NOT EXISTS pages (
                    url TEXT PRIMARY KEY, title TEXT NOT NULL, text TEXT NOT NULL,
                    links TEXT NOT NULL, metadata TEXT NOT NULL,
                    crawled_at TEXT NOT NULL, status_code INTEGER NOT NULL,
                    content_type TEXT NOT NULL, data_json TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_pages_crawled_at ON pages(crawled_at);
                CREATE INDEX IF NOT EXISTS idx_pages_status_code ON pages(status_code);
            """)
            await db.commit()
        except BaseException:
            await db.close()
            raise
        self._db = db

    async def init_db(self):
        async with self._lock:
            await self._execute(self._init_db, count=0)

    async def save(self, data: dict):
        await self.save_batch([data])

    async def save_batch(self, records):
        records = list(records)
        if not records:
            return
        async with self._lock:
            async def write():
                def prepare():
                    rows = []
                    for item in records:
                        record = normalize_record(item)
                        rows.append((
                            record["url"], record["title"], record["text"],
                            json.dumps(record["links"], ensure_ascii=False),
                            json.dumps(record["metadata"], ensure_ascii=False),
                            record["crawled_at"], record["status_code"], record["content_type"],
                            json.dumps(record, ensure_ascii=False),
                        ))
                    return rows
                rows = await asyncio.to_thread(prepare)
                await self._init_db()
                try:
                    await self._db.execute("BEGIN")
                    await self._db.executemany("""
                        INSERT INTO pages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ON CONFLICT(url) DO UPDATE SET
                        title=excluded.title, text=excluded.text, links=excluded.links,
                        metadata=excluded.metadata, crawled_at=excluded.crawled_at,
                        status_code=excluded.status_code, content_type=excluded.content_type,
                        data_json=excluded.data_json
                    """, rows)
                    await self._db.commit()
                except BaseException:
                    await self._db.rollback()
                    raise
            await self._execute(write, count=len(records))

    async def read_all(self):
        async with self._lock:
            if self._db is None and not self._closed:
                await self._init_db()
            if self._db is not None:
                async with self._db.execute("SELECT data_json FROM pages ORDER BY url") as cursor:
                    return [json.loads(row[0]) for row in await cursor.fetchall()]
            if not await asyncio.to_thread(self.path.exists):
                return []
            async with aiosqlite.connect(self.path.resolve().as_uri() + "?mode=ro", uri=True) as db:
                async with db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='pages'") as cursor:
                    if await cursor.fetchone() is None:
                        return []
                async with db.execute("SELECT data_json FROM pages ORDER BY url") as cursor:
                    return [json.loads(row[0]) for row in await cursor.fetchall()]

    async def close(self):
        async with self._lock:
            if self._db is not None:
                await self._db.close()
                self._db = None
            self._closed = True


class MultiStorage(DataStorage):
    """Сохранение в несколько форматов; каждый backend повторяет только свою запись."""

    def __init__(self, storages):
        super().__init__()
        self.storages = list(storages)
        if not self.storages or not all(isinstance(storage, DataStorage) for storage in self.storages):
            raise ValueError("Нужен непустой список DataStorage")

    async def save(self, data: dict):
        async with self._lock:
            if self._closed:
                raise StorageError("Хранилище уже закрыто")
            results = await asyncio.gather(*(storage.save(data) for storage in self.storages), return_exceptions=True)
            for result in results:
                if isinstance(result, asyncio.CancelledError):
                    raise result
            errors = [str(result) for result in results if isinstance(result, Exception)]
            if errors:
                self.errors.append({"message": "; ".join(errors), "final": True})
                raise StorageError("; ".join(errors))
            self.saved += 1

    async def close(self):
        async with self._lock:
            results = await asyncio.gather(*(storage.close() for storage in self.storages), return_exceptions=True)
            self._closed = True
            for result in results:
                if isinstance(result, asyncio.CancelledError):
                    raise result
            errors = [str(result) for result in results if isinstance(result, Exception)]
            if errors:
                raise StorageError("; ".join(errors))

    def get_stats(self):
        return {**super().get_stats(), "backends": [
            {"format": type(storage).__name__, **storage.get_stats()} for storage in self.storages
        ]}


class AsyncCrawler:
    """Один экземпляр используется в пределах одного event loop.

    fetch_url возвращает текст или поднимает исключение. fetch_urls возвращает
    только успешно загруженные страницы (включая пустые ответы).
    """

    def __init__(
        self, max_concurrent: int = 10, max_depth: int = 2, per_domain: int = 2,
        requests_per_second: float = 1.0, respect_robots: bool = True,
        min_delay: float = 0.0, jitter: float = 0.0,
        user_agent: str = "LearningCrawler/1.0", rate_per_domain: bool = True,
        max_retries: int = 3, backoff_factor: float = 2.0,
        retry_strategy: RetryStrategy = None,
        connect_timeout: float = 10, read_timeout: float = 15, total_timeout: float = 30,
        timeout_multiplier: float = 1.5, max_timeout: float = 120,
        storage: DataStorage = None,
    ):
        if isinstance(max_concurrent, bool) or not isinstance(max_concurrent, int):
            raise TypeError("max_concurrent должен быть целым числом")
        if max_concurrent < 1:
            raise ValueError("max_concurrent должен быть положительным")
        self.max_concurrent = max_concurrent
        for name, value in (("connect_timeout", connect_timeout), ("read_timeout", read_timeout),
                            ("total_timeout", total_timeout), ("max_timeout", max_timeout)):
            if nonnegative_number(value, name) == 0:
                raise ValueError(f"{name} должен быть больше нуля")
        if nonnegative_number(timeout_multiplier, "timeout_multiplier") < 1:
            raise ValueError("timeout_multiplier должен быть не меньше 1")
        self.timeout = aiohttp.ClientTimeout(total=total_timeout, connect=connect_timeout, sock_read=read_timeout)
        self.timeout_multiplier = timeout_multiplier
        self.max_timeout = max_timeout
        if isinstance(max_depth, bool) or not isinstance(max_depth, int) or max_depth < 0:
            raise ValueError("max_depth должен быть неотрицательным целым числом")
        self.max_depth = max_depth
        self.semaphore_manager = SemaphoreManager(max_concurrent, per_domain)
        self.queue = CrawlerQueue()
        self.visited_urls = set()
        self.failed_urls = {}
        self.processed_urls = {}
        self.depths = {}
        self._crawling = False
        self._crawl_started = None
        self._crawl_elapsed = None
        self._session: Optional[aiohttp.ClientSession] = None
        self._closed = False
        self.parser = HTMLParser()
        self.stats = CrawlerStats()
        self._max_pages = 0
        if storage is not None and not isinstance(storage, DataStorage):
            raise TypeError("storage должен реализовывать DataStorage")
        self.storage = storage
        self.storage_errors = []
        self._storage_closed = storage is None
        self._close_lock = asyncio.Lock()
        self.rate_limiter = RateLimiter(requests_per_second, rate_per_domain)
        self.min_delay = nonnegative_number(min_delay, "min_delay")
        self.jitter = nonnegative_number(jitter, "jitter")
        self.backoff_factor = nonnegative_number(backoff_factor, "backoff_factor")
        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 0:
            raise ValueError("max_retries должен быть неотрицательным целым числом")
        if not isinstance(user_agent, str) or not user_agent.strip() or any(ord(char) < 32 for char in user_agent):
            raise ValueError("Нужен непустой User-Agent без управляющих символов")
        self.user_agent = user_agent
        self.respect_robots = respect_robots
        self.max_retries = max_retries
        self.robots = RobotsParser(self._request, user_agent)
        self.blocked_urls = set()
        self._request_times = deque()
        self._request_count = 0
        self._last_request = None
        self._gap_total = 0.0
        self.retry_strategy = retry_strategy if retry_strategy is not None else RetryStrategy(max_retries, backoff_factor)
        if not isinstance(self.retry_strategy, RetryStrategy):
            raise TypeError("retry_strategy должен быть RetryStrategy")

    def _get_session(self) -> aiohttp.ClientSession:
        if self._closed:
            raise RuntimeError("AsyncCrawler уже закрыт")
        if self._session is None:
            connector = aiohttp.TCPConnector(limit=self.max_concurrent)
            self._session = aiohttp.ClientSession(
                connector=connector, timeout=self.timeout, headers={"User-Agent": self.user_agent}
            )
        return self._session

    def _attempt_timeout(self):
        context = _retry_context.get()
        attempt = context["attempt"] if context else 0
        def scale(value):
            if value is None or value <= 0:
                return value
            limit = max(value, self.max_timeout)
            scaled = value
            for _ in range(attempt):
                if scaled >= limit / self.timeout_multiplier:
                    return limit
                scaled *= self.timeout_multiplier
            return scaled
        return aiohttp.ClientTimeout(
            total=scale(self.timeout.total), connect=scale(self.timeout.connect),
            sock_read=scale(self.timeout.sock_read), sock_connect=scale(self.timeout.sock_connect),
            ceil_threshold=self.timeout.ceil_threshold,
        )

    async def _request_once(self, url, crawl_delay=0.0):
        async with self.semaphore_manager.acquire(url):
            await self.rate_limiter.acquire(
                urlsplit(url).hostname, min_delay=max(self.min_delay, crawl_delay), jitter=self.jitter,
            )
            session = self._get_session()
            now = perf_counter()
            while self._request_times and self._request_times[0] <= now - 1:
                self._request_times.popleft()
            self._request_times.append(now)
            if self._last_request is not None:
                self._gap_total += now - self._last_request
            self._last_request = now
            self._request_count += 1
            status = None
            successful = False
            try:
                async with session.get(url, allow_redirects=False, timeout=self._attempt_timeout()) as response:
                    status = response.status
                    response.raise_for_status()
                    text = await response.text(errors="replace")
                    successful = True
                    return {
                        "status": response.status, "headers": dict(response.headers),
                        "text": text,
                        "content_type": response.headers.get("Content-Type", ""),
                    }
            finally:
                self.stats.record_request(url, status, successful)

    async def _request(self, url, crawl_delay=0.0, *, check_robots=False):
        async def attempt(request_url):
            delay = crawl_delay
            if check_robots:
                await self.robots.fetch_robots(request_url)
                if not self.robots.can_fetch(request_url, self.user_agent):
                    self.blocked_urls.add(request_url)
                    logger.warning("%s | Заблокирован robots.txt", request_url)
                    raise RobotsDeniedError(f"robots.txt запрещает {request_url}", url=request_url)
                delay = self.robots.get_crawl_delay(self.user_agent, base_url=request_url)
            return await self._request_once(request_url, delay)
        return await self.retry_strategy.execute_with_retry(attempt, url)

    async def fetch_url(self, url: str, *, url_filter=None) -> str:
        response = await self._fetch_response(url, url_filter=url_filter)
        return response["text"]

    async def _fetch_response(self, url: str, *, url_filter=None) -> dict:
        """Каждая HTTP-попытка соблюдает robots, задержки и лимиты."""
        current = url
        logger.info("Начало загрузки: %s", url)
        try:
            for _ in range(11):
                response = await self._request(current, check_robots=self.respect_robots)
                if response["status"] in {301, 302, 303, 307, 308} and "Location" in response["headers"]:
                    target = HTMLParser._absolute_url(response["headers"]["Location"], current)
                    if target is None or (url_filter is not None and not url_filter(target)):
                        raise PermanentError("Редирект отклонён фильтром URL", url=current)
                    context = _retry_context.get()
                    if context is not None and url in context["urls"]:
                        context["urls"].add(target)
                    current = target
                    continue
                logger.info("Успешно: %s | HTTP %s", url, response["status"])
                response["final_url"] = current
                response["crawled_at"] = datetime.now(timezone.utc)
                return response
            raise PermanentError("Превышен лимит перенаправлений", url=current)
        except (asyncio.TimeoutError, aiohttp.ClientError) as exc:
            if isinstance(exc, CrawlerError) and not exc.recorded:
                self.retry_strategy.record_error(exc, exc.url or url)
            logger.error("%s | %s: %s", url, type(exc).__name__, exc)
            raise

    def get_error_report(self):
        return {
            "stats": self.retry_strategy.get_stats(),
            "errors": list(self.retry_strategy.errors),
            "storage_errors": list(self.storage_errors),
        }

    async def save_error_report(self, path):
        async with aiofiles.open(path, "w", encoding="utf-8") as file:
            await file.write(json.dumps(self.get_error_report(), ensure_ascii=False, indent=2))

    def get_rate_stats(self):
        now = perf_counter()
        while self._request_times and self._request_times[0] <= now - 1:
            self._request_times.popleft()
        return {
            "requests_total": self._request_count,
            "requests_per_second": len(self._request_times),
            "average_delay": self._gap_total / (self._request_count - 1) if self._request_count > 1 else 0.0,
            "robots_blocked": len(self.blocked_urls), "retries": self.retry_strategy.retries,
        }

    async def fetch_urls(self, urls: list[str]) -> dict[str, str]:
        """Загрузить уникальные URL; ошибки отдельных запросов не прерывают пакет."""
        async def fetch_safely(url: str) -> Optional[str]:
            try:
                return await self.fetch_url(url)
            except (aiohttp.ClientError, asyncio.TimeoutError):
                return None

        unique_urls = list(dict.fromkeys(urls))
        pages = await asyncio.gather(*(fetch_safely(url) for url in unique_urls))
        return {url: page for url, page in zip(unique_urls, pages) if page is not None}

    async def fetch_and_parse(self, url: str, *, url_filter=None) -> dict:
        """Загрузить страницу и извлечь данные; при сетевой ошибке вернуть errors."""
        try:
            response = await self._fetch_response(url, url_filter=url_filter)
            html = response["text"]
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            result = self.parser.empty_result(url)
            result["errors"].append(f"{type(exc).__name__}: {exc}")
            return result
        try:
            result = await self.parser.parse_html(html, response["final_url"])
            result["url"] = url
        except Exception as exc:
            result = self.parser.empty_result(url)
            result["errors"].append(f"{type(exc).__name__}: {exc}")
        if result["errors"]:
            error = ParseError("; ".join(result["errors"]), url=url)
            self.retry_strategy.record_error(error, url)
            logger.warning("%s | ParseError | итог: частичные данные | %s", url, error)
        result.update({
            "crawled_at": response["crawled_at"], "status_code": response["status"],
            "content_type": response["content_type"],
        })
        if url in self.depths:
            result["depth"] = self.depths[url]
        if self.storage is not None:
            try:
                await self.storage.save(result)
            except Exception as exc:
                self.storage_errors.append({"url": url, "operation": "save", "error": str(exc)})
                logger.error("%s | Ошибка сохранения: %s", url, exc)
        return result

    async def crawl(
        self, start_urls: list[str], max_pages: int = 100,
        same_domain_only: bool = True, exclude_patterns=None, include_patterns=None,
        _reset_stats=True,
    ) -> dict:
        """Обход в ширину; max_pages ограничивает число попыток, включая ошибки.

        Стартовая глубина 0. Паттерны — регулярные выражения по полному URL.
        Каждый новый запуск сбрасывает результаты предыдущего обхода.
        """
        if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages < 0:
            raise ValueError("max_pages должен быть неотрицательным целым числом")
        if self._crawling:
            raise RuntimeError("На одном клиенте уже выполняется crawl")
        if self._closed:
            raise RuntimeError("AsyncCrawler уже закрыт")
        self._max_pages = max_pages
        if _reset_stats:
            self.stats.reset()
        includes = [re.compile(pattern) for pattern in (include_patterns or [])]
        excludes = [re.compile(pattern) for pattern in (exclude_patterns or [])]
        seeds = []
        for url in start_urls:
            normalized = HTMLParser._absolute_url(url, "")
            if normalized:
                seeds.append(normalized)
            else:
                logger.warning("Пропущен некорректный стартовый URL: %s", url)
        domains = {urlsplit(url).hostname.lower() for url in seeds}

        def allowed(url):
            return (
                (not same_domain_only or urlsplit(url).hostname.lower() in domains)
                and not any(pattern.search(url) for pattern in excludes)
                and (not includes or any(pattern.search(url) for pattern in includes))
            )

        self.queue = CrawlerQueue()
        self.visited_urls = set()
        self.failed_urls = {}
        self.processed_urls = {}
        self.depths = {}
        self._crawl_started = perf_counter()
        self._crawl_elapsed = None
        self._crawling = True

        def enqueue(url, depth):
            if depth <= self.max_depth and allowed(url) and url not in self.depths:
                self.depths[url] = depth
                self.queue.add_url(url, priority=depth)

        for url in seeds:
            enqueue(url, 0)

        async def process(url):
            try:
                page = await self.fetch_and_parse(url, url_filter=allowed)
            except asyncio.CancelledError:
                self.queue.mark_failed(url, "Задача отменена")
                self.failed_urls[url] = "Задача отменена"
                raise
            except Exception as exc:
                page = self.parser.empty_result(url)
                page["errors"].append(f"{type(exc).__name__}: {exc}")
                logger.warning("%s | %s", url, page["errors"][-1])
            page["depth"] = self.depths[url]
            if page["errors"]:
                error = "; ".join(page["errors"])
                self.failed_urls[url] = error
                self.queue.mark_failed(url, error)
                # Не теряем данные, если парсер вернул частичный результат.
                if "status_code" in page:
                    self.processed_urls[url] = page
            else:
                self.processed_urls[url] = page
                self.queue.mark_processed(url)
            self.stats.record_page(url, not page["errors"])
            for link in page["links"]:
                enqueue(link, self.depths[url] + 1)
            self._log_progress()

        async def report_progress():
            while True:
                await asyncio.sleep(1)
                self._log_progress()

        reporter = asyncio.create_task(report_progress())
        tasks = []
        try:
            while self.queue.get_stats()["queued"] and len(self.visited_urls) < max_pages:
                priority = self.queue.next_priority
                tasks = []
                remaining = max_pages - len(self.visited_urls)
                # Завершаем текущую глубину до следующей: первый путь к URL — кратчайший.
                for _ in range(min(self.max_concurrent, remaining)):
                    if self.queue.next_priority != priority:
                        break
                    url = await self.queue.get_next()
                    self.visited_urls.add(url)
                    tasks.append(asyncio.create_task(process(url)))
                await asyncio.gather(*tasks)
        finally:
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            reporter.cancel()
            await asyncio.gather(reporter, return_exceptions=True)
            self._crawl_elapsed = perf_counter() - self._crawl_started
            self._crawling = False
            self.stats.finish()
            self._log_progress()
        return dict(self.processed_urls)

    def get_stats(self) -> dict:
        stats = self.queue.get_stats()
        elapsed = self._crawl_elapsed
        if elapsed is None:
            elapsed = perf_counter() - self._crawl_started if self._crawl_started else 0
        completed = stats["processed"] + stats["failed"]
        return {
            **stats, "visited": len(self.visited_urls), "elapsed": elapsed,
            "pages_per_second": completed / elapsed if elapsed else 0,
            "requests": self.semaphore_manager.get_stats(),
            "rate": self.get_rate_stats(),
            "errors": self.retry_strategy.get_stats(),
            "storage": self.storage.get_stats() if self.storage is not None else None,
            "summary": self.stats.get_stats(),
            "progress_percent": min(100.0, completed / self._max_pages * 100) if self._max_pages else 0.0,
            "estimated_remaining_seconds": (stats["queued"] + stats["active"]) / (completed / elapsed) if self._crawling and completed and elapsed else None,
        }

    def _log_progress(self):
        if not logger.isEnabledFor(logging.INFO):
            return
        stats = self.get_stats()
        logger.info(
            "Прогресс | обработано: %s | очередь: %s | активных: %s | ошибок: %s | %.2f стр/с | %.1f req/s | интервал %.3f с | robots: %s | %.1f%% лимита | ETA ~%s с",
            stats["processed"], stats["queued"], stats["active"],
            stats["failed"], stats["pages_per_second"], stats["rate"]["requests_per_second"],
            stats["rate"]["average_delay"], stats["rate"]["robots_blocked"],
            stats["progress_percent"], round(stats["estimated_remaining_seconds"], 1) if stats["estimated_remaining_seconds"] is not None else "?",
        )

    async def close(self):
        """Повторный вызов завершает очистку ресурсов после временного отказа."""
        async with self._close_lock:
            self._closed = True
            try:
                if self._session is not None and not self._session.closed:
                    await self._session.close()
            finally:
                if not self._storage_closed:
                    try:
                        await self.storage.close()
                    except Exception as exc:
                        self.storage_errors.append({"url": None, "operation": "close", "error": str(exc)})
                        logger.error("Ошибка закрытия хранилища: %s", exc)
                    else:
                        self._storage_closed = True

    async def __aenter__(self):
        self._get_session()
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        await self.close()


class SitemapParser:
    """Обычные карты и sitemap index; XML без внешних сущностей и DTD."""

    def __init__(self, downloader=None, *, max_sitemaps=100, max_urls=10000, max_depth=5, max_bytes=5 * 1024 * 1024, url_filter=None):
        for value in (max_sitemaps, max_urls, max_depth, max_bytes):
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError("Лимиты sitemap должны быть неотрицательными целыми числами")
        self.downloader = downloader
        self.max_sitemaps, self.max_urls = max_sitemaps, max_urls
        self.max_depth, self.max_bytes = max_depth, max_bytes
        self.url_filter = url_filter
        self.errors = []
        self.visited_sitemaps = set()

    async def _download(self, url):
        if self.downloader:
            return await self.downloader(url)
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
            async with session.get(url) as response:
                response.raise_for_status()
                raw = bytearray()
                async for chunk in response.content.iter_chunked(65536):
                    raw.extend(chunk)
                    if len(raw) > self.max_bytes:
                        raise ValueError("Превышен размер sitemap")
                return bytes(raw)

    def _parse(self, data):
        raw = data.encode("utf-8") if isinstance(data, str) else data
        if len(raw) > self.max_bytes:
            raise ValueError("Превышен размер sitemap")
        root = etree.fromstring(raw, parser=etree.XMLParser(resolve_entities=False, no_network=True, load_dtd=False))
        if root.getroottree().docinfo.doctype:
            raise ValueError("DTD в sitemap запрещён")
        kind = etree.QName(root).localname
        if kind not in {"urlset", "sitemapindex"}:
            raise ValueError("Ожидается urlset или sitemapindex")
        child_name = "sitemap" if kind == "sitemapindex" else "url"
        urls = []
        for child in root:
            if not isinstance(child.tag, str) or etree.QName(child).localname != child_name:
                continue
            for loc in child:
                if isinstance(loc.tag, str) and etree.QName(loc).localname == "loc" and loc.text:
                    urls.append(loc.text.strip())
        return kind, urls

    async def fetch_sitemap(self, sitemap_url: str) -> list[str]:
        self.errors = []
        self.visited_sitemaps = set()
        found = dict()

        async def visit(url, depth):
            url = HTMLParser._absolute_url(url, "")
            if url is None or (self.url_filter and not self.url_filter(url)):
                return
            if url in self.visited_sitemaps or depth > self.max_depth or len(found) >= self.max_urls:
                return
            if len(self.visited_sitemaps) >= self.max_sitemaps:
                return
            self.visited_sitemaps.add(url)
            try:
                data = await self._download(url)
                kind, locations = await asyncio.to_thread(self._parse, data)
            except Exception as exc:
                error = {"url": url, "error": f"{type(exc).__name__}: {exc}"}
                self.errors.append(error)
                logger.warning("%s | Ошибка sitemap: %s", url, error["error"])
                return
            for location in locations:
                target = HTMLParser._absolute_url(location, url)
                if target is None or (self.url_filter and not self.url_filter(target)):
                    continue
                if kind == "sitemapindex":
                    await visit(target, depth + 1)
                else:
                    found.setdefault(target, None)
                if len(found) >= self.max_urls:
                    break

        await visit(sitemap_url, 0)
        return list(found)


class CrawlerStats:
    def __init__(self):
        self.reset()

    def reset(self):
        self.started = perf_counter()
        self.finished = None
        self.successful = self.failed = 0
        self.successful_requests = self.failed_requests = 0
        self.status_codes = Counter()
        self.domains = Counter()

    def record_request(self, url, status, successful=None):
        if status is not None:
            self.status_codes[str(status)] += 1
        is_success = successful if successful is not None else status is not None and status < 400
        if is_success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1

    def record_page(self, url, successful):
        self.successful += bool(successful)
        self.failed += not successful
        self.domains[urlsplit(url).hostname or "unknown"] += 1

    def finish(self):
        self.finished = perf_counter()

    def get_stats(self):
        elapsed = (self.finished if self.finished is not None else perf_counter()) - self.started
        total = self.successful + self.failed
        return {
            "total_pages": total, "successful": self.successful, "failed": self.failed,
            "total_requests": self.successful_requests + self.failed_requests,
            "successful_requests": self.successful_requests, "failed_requests": self.failed_requests,
            "average_speed": total / elapsed if elapsed else 0,
            "elapsed_seconds": elapsed, "status_codes": dict(self.status_codes),
            "top_domains": dict(self.domains.most_common(10)),
        }

    def export_to_json(self, filename, *, extra=None):
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(extra if extra is not None else self.get_stats(), ensure_ascii=False, indent=2, default=json_default), encoding="utf-8")

    def export_to_html_report(self, filename, *, extra=None):
        stats = self.get_stats() if extra is None else extra
        def bars(values):
            peak = max(values.values(), default=1) or 1
            return "".join(
                f'<div class="bar-row"><span>{escape(str(key))}</span><div class="track"><div class="bar" style="width:{value / peak * 100:.2f}%"></div></div><b>{value}</b></div>'
                for key, value in values.items()
            ) or "<p>Нет данных</p>"
        cards = "".join(f'<div class="card"><small>{label}</small><strong>{value}</strong></div>' for label, value in [
            ("Страниц", stats["total_pages"]), ("Успешно", stats["successful"]), ("Ошибок", stats["failed"]),
            ("HTTP-попыток", stats["total_requests"]), ("Страниц/сек", f'{stats["average_speed"]:.2f}'),
            ("Время, сек", f'{stats["elapsed_seconds"]:.2f}'),
        ])
        rows = "".join(f"<tr><td>{escape(str(status))}</td><td>{count}</td></tr>" for status, count in stats["status_codes"].items())
        details = escape(json.dumps(stats, ensure_ascii=False, indent=2, default=json_default))
        html = f'''<!doctype html><html lang="ru"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Отчёт краулера</title><style>
body{{font:16px system-ui;background:#f3f5fa;color:#18233a;margin:0}}main{{max-width:1050px;margin:auto;padding:40px 24px}}
h1{{font-size:36px;margin-bottom:8px}}.muted,small{{color:#56637b}}.cards{{display:grid;grid-template-columns:repeat(auto-fit,minmax(145px,1fr));gap:14px;margin:28px 0}}
.card,section{{background:white;border-radius:14px;padding:22px}}strong{{display:block;font-size:30px;margin-top:8px}}section{{margin:20px 0}}h2{{margin-top:0}}
.bar-row{{display:grid;grid-template-columns:minmax(100px,220px) 1fr 60px;gap:14px;align-items:center;margin:14px 0;overflow-wrap:anywhere}}
.track{{background:#e9edf7;border-radius:6px;height:22px}}.bar{{height:100%;background:#5368db;border-radius:6px}}table{{border-collapse:collapse;width:100%}}td,th{{padding:10px;text-align:left;border-bottom:1px solid #e9edf7}}pre{{white-space:pre-wrap;overflow-wrap:anywhere;font-size:13px}}
</style><main><h1>Отчёт краулера</h1><p class="muted">Статистика завершённых страниц и всех HTTP-попыток, включая robots, sitemap и повторы.</p>
<div class="cards">{cards}</div><section><h2>HTTP-статусы</h2>{bars(stats["status_codes"])}<table><tr><th>Статус</th><th>Ответов</th></tr>{rows}</table></section>
<section><h2>Топ доменов по страницам</h2>{bars(stats["top_domains"])}</section><section><details><summary>Полная статистика</summary><pre>{details}</pre></details></section></main></html>'''
        path = Path(filename)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(html, encoding="utf-8")


class JSONLogFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": datetime.fromtimestamp(record.created, timezone.utc).isoformat(),
            "level": record.levelname, "logger": record.name, "message": record.getMessage(),
            "exception": self.formatException(record.exc_info) if record.exc_info else None,
        }, ensure_ascii=False)


def configure_logging(config):
    if set(config) - {"file", "level", "max_bytes", "backup_count", "enabled"}:
        raise ValueError("Неизвестная настройка logging")
    if not isinstance(config.get("enabled", True), bool):
        raise ValueError("logging.enabled должен быть bool")
    for name, default in (("max_bytes", 2 * 1024 * 1024), ("backup_count", 3)):
        value = config.get(name, default)
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError(f"logging.{name} должен быть положительным целым числом")
    level = config.get("level", "INFO")
    if not isinstance(level, str):
        raise ValueError("logging.level должен быть строкой")
    level = level.upper()
    if level not in {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}:
        raise ValueError("Неизвестный уровень логирования")
    handlers = []
    console = logging.StreamHandler()
    console.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    handlers.append(console)
    if config.get("file"):
        path = Path(config["file"])
        path.parent.mkdir(parents=True, exist_ok=True)
        rotating = RotatingFileHandler(path, maxBytes=config.get("max_bytes", 2 * 1024 * 1024), backupCount=config.get("backup_count", 3), encoding="utf-8")
        rotating.setFormatter(JSONLogFormatter())
        handlers.append(rotating)
    for old in list(logger.handlers):
        if getattr(old, "_crawler_owned", False):
            logger.removeHandler(old)
            old.close()
    for handler in handlers:
        handler._crawler_owned = True
        logger.addHandler(handler)
    logger.setLevel(level)
    logger.propagate = False
    return handlers


def read_config(filename):
    path = Path(filename).resolve()
    config = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(config, dict):
        raise ValueError("Конфигурация должна быть JSON-объектом")
    # Пути в конфигурации относятся к каталогу конфигурационного файла.
    for section, key in [(config, "output"), (config.get("logging", {}), "file")]:
        if isinstance(section, dict) and section.get(key):
            section[key] = str(path.parent / section[key])
    for storage in config.get("storage", []):
        if isinstance(storage, dict) and storage.get("path"):
            storage["path"] = str(path.parent / storage["path"])
    return config


class AdvancedCrawler:
    """Фасад: конфигурация, sitemap, AsyncCrawler, storage, статистика и отчёты."""

    def __init__(self, config=None):
        if config is not None and not isinstance(config, dict):
            raise ValueError("Конфигурация должна быть объектом")
        config = copy.deepcopy(config or {})
        allowed = {"crawler", "crawl", "start_urls", "sitemaps", "sitemap", "storage", "logging", "output"}
        if set(config) - allowed:
            raise ValueError(f"Неизвестные параметры конфигурации: {set(config) - allowed}")
        self.output = config.get("output", "crawl_results.json")
        if not isinstance(self.output, str) or not self.output:
            raise ValueError("output должен быть непустой строкой")
        self.start_urls = config.get("start_urls", ["https://example.com"])
        self.sitemap_urls = config.get("sitemaps", [])
        for name, values in [("start_urls", self.start_urls), ("sitemaps", self.sitemap_urls)]:
            if not isinstance(values, list) or not all(isinstance(url, str) and HTMLParser._absolute_url(url, "") for url in values):
                raise ValueError(f"{name} должен содержать абсолютные HTTP(S) URL")
        self.crawl_options = {"max_pages": 100, "same_domain_only": True, **config.get("crawl", {})}
        self._validate_crawl_options(self.crawl_options)
        options = config.get("crawler", {})
        if not isinstance(options, dict) or set(options) - (set(inspect.signature(AsyncCrawler).parameters) - {"storage", "retry_strategy"}):
            raise ValueError("Неизвестная настройка crawler")
        for field in ("respect_robots", "rate_per_domain"):
            if field in options and not isinstance(options[field], bool):
                raise ValueError(f"{field} должен быть bool")
        stem = str(Path(self.output).with_suffix(""))
        storage_config = config.get("storage", [{"type": "json", "path": stem + ".pages.json"}])
        if not isinstance(storage_config, list):
            raise ValueError("storage должен быть списком")
        storages = []
        factories = {"json": JSONStorage, "jsonl": JSONStorage, "csv": CSVStorage, "sqlite": SQLiteStorage}
        for entry in storage_config:
            if not isinstance(entry, dict) or entry.get("type") not in factories or not isinstance(entry.get("path"), str) or not entry["path"]:
                raise ValueError("storage требует type (json/jsonl/csv/sqlite) и path")
            entry = dict(entry)
            kind = entry.pop("type")
            if kind == "jsonl":
                entry["json_lines"] = True
            storages.append(factories[kind](**entry))
        logging_config = config.get("logging", {})
        if not isinstance(logging_config, dict):
            raise ValueError("logging должен быть объектом")
        # Все писатели должны владеть разными файлами, включая отчёты и архивы логов.
        paths = [self.output, self.output + ".errors.json", stem + ".stats.json", stem + ".report.html"]
        paths.extend(str(item.path) for item in storages)
        if logging_config.get("enabled", True):
            log_path = logging_config.get("file", stem + ".log")
            if log_path:
                paths.append(log_path)
                backups = logging_config.get("backup_count", 3)
                if isinstance(backups, int) and not isinstance(backups, bool) and backups > 0:
                    paths.extend(str(log_path) + f".{number}" for number in range(1, backups + 1))
        resolved = []
        for value in paths:
            path = Path(value).resolve()
            if any(path == previous or (path.exists() and previous.exists() and path.samefile(previous))
                   for previous in resolved):
                raise ValueError(f"Совпадают пути выходных файлов: {path}")
            resolved.append(path)
        storage = MultiStorage(storages) if len(storages) > 1 else storages[0] if storages else None
        self.crawler = AsyncCrawler(storage=storage, **options)
        self.stats = self.crawler.stats
        sitemap_options = config.get("sitemap", {})
        self.sitemap_parser = SitemapParser(self._fetch_sitemap, **sitemap_options)
        self.sitemap_errors = []
        self._running = False
        self._handlers = []
        if logging_config.get("enabled", True):
            self._handlers = configure_logging({"file": stem + ".log", **logging_config})

    @staticmethod
    def _validate_crawl_options(options):
        if set(options) - {"max_pages", "same_domain_only", "exclude_patterns", "include_patterns"}:
            raise ValueError("Неизвестная настройка crawl")
        maximum = options["max_pages"]
        if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum < 0:
            raise ValueError("max_pages должен быть неотрицательным целым числом")
        if not isinstance(options["same_domain_only"], bool):
            raise ValueError("same_domain_only должен быть bool")
        for field in ("include_patterns", "exclude_patterns"):
            values = options.get(field, [])
            if not isinstance(values, list) or not all(isinstance(value, str) for value in values):
                raise ValueError(f"{field} должен быть списком регулярных выражений")
            for value in values:
                re.compile(value)

    async def _fetch_sitemap(self, url):
        # Тот же фильтр действует и на каждый шаг HTTP-перенаправления.
        return await self.crawler.fetch_url(url, url_filter=self.sitemap_parser.url_filter)

    @classmethod
    def from_config(cls, filename):
        return cls(read_config(filename))

    async def crawl(self, start_urls=None, **overrides):
        if self._running:
            raise RuntimeError("AdvancedCrawler уже выполняет обход")
        if self.crawler._closed:
            raise RuntimeError("Краулер уже закрыт")
        options = {**self.crawl_options, **overrides}
        self._validate_crawl_options(options)
        seeds = self.start_urls if start_urls is None else start_urls
        if not isinstance(seeds, list) or not all(
            isinstance(url, str) and HTMLParser._absolute_url(url, "") for url in seeds
        ):
            raise ValueError("start_urls должен содержать абсолютные HTTP(S) URL")
        seeds = list(seeds)
        self._running = True
        self.stats.reset()
        self.sitemap_errors = []
        try:
            if options["max_pages"] > 0:
                domains = {urlsplit(url).hostname for url in seeds or self.sitemap_urls}
                self.sitemap_parser.url_filter = (lambda url: urlsplit(url).hostname in domains) if options.get("same_domain_only", True) else None
                for sitemap in self.sitemap_urls:
                    seeds.extend(await self.sitemap_parser.fetch_sitemap(sitemap))
                    self.sitemap_errors.extend(self.sitemap_parser.errors)
            return await self.crawler.crawl(list(dict.fromkeys(seeds)), _reset_stats=False, **options)
        finally:
            self.stats.finish()
            self._running = False

    def get_stats(self):
        return {**self.crawler.get_stats(), **self.stats.get_stats(), "sitemap_errors": list(self.sitemap_errors)}

    def export_to_json(self, filename):
        self.stats.export_to_json(filename, extra=self.get_stats())

    def export_to_html_report(self, filename):
        self.stats.export_to_html_report(filename, extra=self.get_stats())

    async def save_results(self):
        path = Path(self.output)
        await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
        report = {
            "pages": self.crawler.processed_urls, "stats": self.get_stats(),
            "failed_urls": self.crawler.failed_urls, "blocked_urls": sorted(self.crawler.blocked_urls),
            "error_report": self.crawler.get_error_report(), "depths": self.crawler.depths,
        }
        async with aiofiles.open(path, "w", encoding="utf-8") as file:
            payload = await asyncio.to_thread(json.dumps, report, ensure_ascii=False, indent=2, default=json_default)
            await file.write(payload)
        await self.crawler.save_error_report(str(path) + ".errors.json")
        stem = str(path.with_suffix(""))
        await asyncio.to_thread(self.export_to_json, stem + ".stats.json")
        await asyncio.to_thread(self.export_to_html_report, stem + ".report.html")

    async def close(self):
        await self.crawler.close()
        for handler in self._handlers:
            logger.removeHandler(handler)
            handler.close()
        if self._handlers:
            logger.propagate = True
        self._handlers = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        await self.close()


URLS = [
    "https://example.com",
    "https://example.org",
    "https://httpbin.org/delay/1",
    "https://httpbin.org/delay/2",
    "https://httpbin.org/delay/3",
    "https://httpbin.org/status/404",
]


async def compare(urls):
    timings = []
    for limit, label in [(1, "Последовательно"), (5, "Конкурентно")]:
        start = perf_counter()
        async with AsyncCrawler(max_concurrent=limit, per_domain=limit, requests_per_second=1000000, respect_robots=False, max_retries=0) as crawler:
            results = await crawler.fetch_urls(urls)
        elapsed = perf_counter() - start
        timings.append(elapsed)
        print(f"\n{label}: {elapsed:.2f} с, загружено {len(results)}/{len(urls)}")
        for url in urls:
            status = f"OK ({len(results[url])} символов)" if url in results else "ОШИБКА (см. лог)"
            print(f"  {status}: {url}")
    print(f"\nУскорение: {timings[0] / timings[1]:.2f}x")


async def demonstrate_parsing(urls):
    async with AsyncCrawler(max_concurrent=5) as crawler:
        pages = await asyncio.gather(*(crawler.fetch_and_parse(url) for url in urls))
    print("\nДень 2 — результаты парсинга:")
    for page in pages:
        summary = {
            "url": page["url"], "title": page["title"],
            "text_length": len(page["text"]), "links_count": len(page["links"]),
            "links": page["links"], "metadata": page["metadata"],
            "images_count": len(page["images"]), "images": page["images"],
            "headings": page["headings"], "tables": page["tables"],
            "lists": page["lists"], "errors": page["errors"],
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))


async def demonstrate_crawl(urls, output):
    stem = str(Path(output).with_suffix(""))
    storages = [JSONStorage(stem + ".pages.json"), CSVStorage(stem + ".pages.csv"), SQLiteStorage(stem + ".pages.db")]
    async with AsyncCrawler(
        max_concurrent=5, per_domain=2, max_depth=2, requests_per_second=5,
        min_delay=0.1, jitter=0.02, backoff_factor=0.2, read_timeout=0.4,
        storage=MultiStorage(storages),
    ) as crawler:
        results = await crawler.crawl(urls[:1], max_pages=20, same_domain_only=True)
        report = {
            "pages": results, "failed_urls": crawler.failed_urls,
            "blocked_urls": sorted(crawler.blocked_urls),
            "error_report": crawler.get_error_report(),
            "visited_urls": sorted(crawler.visited_urls),
            "depths": crawler.depths, "stats": crawler.get_stats(),
        }
        async with aiofiles.open(output, "w", encoding="utf-8") as file:
            await file.write(json.dumps(report, ensure_ascii=False, indent=2, default=json_default))
        print(f"\nОбработано: {len(results)} | ошибок: {len(crawler.failed_urls)}")
        error_output = str(output) + ".errors.json"
        await crawler.save_error_report(error_output)
        print(f"Ошибки: {crawler.retry_strategy.get_stats()}")
        print(f"Отчёт об ошибках: {error_output}")
        print(f"Robots: {crawler.get_rate_stats()}")
        for storage in storages:
            restored = await storage.read_all()
            print(f"{type(storage).__name__}: записей {len(restored)}, статистика {storage.get_stats()}")
            if restored:
                print(f"  Прочитано: {restored[0]['url']} | HTTP {restored[0]['status_code']}")
        print(f"Данные сохранены: {output}; страницы: {stem}.pages.json / .csv / .db")


async def main(local=False, compare_only=False, parse_only=False, output="crawl_results.json", config=None):
    async def run_demo(urls):
        if compare_only:
            await compare(urls)
        elif parse_only:
            await demonstrate_parsing(urls)
        else:
            effective = copy.deepcopy(config or {})
            effective.setdefault("output", output)
            if local:
                effective["start_urls"] = urls[:1]
                effective.setdefault("sitemaps", [urljoin(urls[0], "/sitemap.xml")])
                defaults = {"max_concurrent": 5, "per_domain": 2, "max_depth": 2, "requests_per_second": 5,
                            "min_delay": 0.1, "jitter": 0.02, "backoff_factor": 0.2, "read_timeout": 0.4}
                effective["crawler"] = {**defaults, **effective.get("crawler", {})}
                effective["crawl"] = {"max_pages": 20, **effective.get("crawl", {})}
                stem = str(Path(effective["output"]).with_suffix(""))
                effective.setdefault("storage", [{"type": kind, "path": stem + ".pages." + suffix}
                                                 for kind, suffix in [("json", "json"), ("csv", "csv"), ("sqlite", "db")]])
            else:
                effective.setdefault("start_urls", urls[:1])
            async with AdvancedCrawler(effective) as crawler:
                await crawler.crawl()
                await crawler.save_results()
                stats = crawler.get_stats()
                print(f"Страниц: {stats['total_pages']} | успешно: {stats['successful']} | ошибок: {stats['failed']}")
                print(f"Данные: {crawler.output}; отчёт: {Path(crawler.output).with_suffix('')}.report.html")

    if not local:
        await run_demo(URLS if compare_only else URLS[:2])
        return

    page_attempts = Counter()

    async def robots(request):
        return web.Response(text="User-agent: *\nDisallow: /page/private\nCrawl-delay: 0.3\n")

    async def delayed(request):
        page = request.match_info["page"]
        page_attempts[page] += 1
        attempt = page_attempts[page]
        await asyncio.sleep(0.5 if page == "timeout" and attempt == 1 else 0.2)
        if page == "flaky" and attempt == 1:
            raise web.HTTPServiceUnavailable(headers={"Retry-After": "1"})
        if page == "server-error" and attempt == 1:
            raise web.HTTPInternalServerError()
        if page == "busy" and attempt == 1:
            raise web.HTTPTooManyRequests(headers={"Retry-After": "1"})
        if page == "forbidden":
            raise web.HTTPForbidden()
        if page == "missing":
            raise web.HTTPNotFound()
        graph = {"0": ["1", "2", "missing", "private", "flaky", "server-error", "forbidden", "busy", "timeout"], "1": ["2", "3"], "2": ["0", "3"], "3": ["4"]}
        navigation = "".join(f'<a href="/page/{target}">Страница {target}</a>' for target in graph.get(page, []))
        return web.Response(text=f"""<!doctype html>
<html><head><title>Страница {page}</title>
<meta name="description" content="Учебная страница">
<meta name="keywords" content="asyncio, HTML"></head>
<body><main><h1>Страница {page}</h1><h2>Данные</h2><h3>Пример</h3>
<p>Изучаем асинхронность и парсинг HTML.</p>
{navigation}<a href="../page/0">Начало</a>
<a href="https://example.org">Внешний сайт</a>
<a href="mailto:test@example.org">Почта</a>
<img src="/images/demo.png" alt="Учебная картинка">
<table><tr><th>Язык</th><th>Тема</th></tr><tr><td>Python</td><td>asyncio</td></tr></table>
<ul><li>Загрузить</li><li>Разобрать</li></ul>
<ol><li>Первый шаг</li><li>Второй шаг</li></ol>
<script>console.log('не включать в текст');</script>
</main></body></html>""", content_type="text/html")

    async def sitemap(request):
        base = f"{request.scheme}://{request.host}"
        if request.path == "/sitemap.xml":
            xml = f'<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><sitemap><loc>{base}/sitemap-pages.xml</loc></sitemap></sitemapindex>'
        else:
            xml = f'<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>{base}/page/0</loc></url><url><loc>{base}/page/3</loc></url></urlset>'
        return web.Response(text=xml, content_type="application/xml")

    app = web.Application()
    app.router.add_get("/sitemap.xml", sitemap)
    app.router.add_get("/sitemap-pages.xml", sitemap)
    app.router.add_get("/robots.txt", robots)
    app.router.add_get("/page/{page}", delayed)
    runner = web.AppRunner(app)
    await runner.setup()
    try:
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        urls = [f"http://127.0.0.1:{port}/page/{i}" for i in range(5)]
        await run_demo(urls)
    finally:
        await runner.cleanup()


async def run_benchmarks(filename="benchmark_results.json"):
    """Локальное сравнение одинакового HTTP+HTML workload, без robots/rate/storage."""
    async def page(request):
        await asyncio.sleep(0.005)
        number = request.match_info["number"]
        return web.Response(text=f"<html><head><title>Page {number}</title></head><body><main><h1>Page {number}</h1><p>{'text ' * 100}</p></main></body></html>", content_type="text/html")
    app = web.Application()
    app.router.add_get("/p/{number}", page)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    rows = []
    try:
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        base = f"http://127.0.0.1:{port}"

        def synchronous(urls):
            import http.client
            connection = http.client.HTTPConnection("127.0.0.1", port, timeout=30)
            parser = HTMLParser()
            results = {}
            try:
                for url in urls:
                    connection.request("GET", urlsplit(url).path, headers={"User-Agent": "Benchmark/1.0"})
                    response = connection.getresponse()
                    html = response.read().decode("utf-8")
                    if response.status != 200:
                        raise RuntimeError(f"Benchmark HTTP {response.status}")
                    record = parser._parse_html(html, url)
                    record.update({"crawled_at": datetime.now(timezone.utc), "status_code": response.status,
                                   "content_type": response.getheader("Content-Type"), "depth": 0})
                    results[url] = record
            finally:
                connection.close()
            return results

        async def asynchronous(urls):
            async with AsyncCrawler(max_concurrent=20, per_domain=20, requests_per_second=1000000,
                                    respect_robots=False, max_retries=0, max_depth=0) as crawler:
                return await crawler.crawl(urls, max_pages=len(urls))

        # Прогрев библиотек и пула потоков до измерения памяти и времени.
        await asynchronous([base + "/p/warmup"])
        await asyncio.to_thread(synchronous, [base + "/p/warmup"])
        for count in (100, 500, 1000):
            urls = [f"{base}/p/{number}" for number in range(count)]
            measurement = {"pages": count}
            for mode in ("sync", "async"):
                tracemalloc.start()
                started = perf_counter()
                try:
                    results = await asyncio.to_thread(synchronous, urls) if mode == "sync" else await asynchronous(urls)
                    seconds = perf_counter() - started
                    _, peak = tracemalloc.get_traced_memory()
                    if len(results) != count or any(page["errors"] for page in results.values()):
                        raise RuntimeError("Потеряны страницы benchmark")
                    measurement[mode] = {"seconds": seconds, "pages_per_second": count / seconds,
                                         "peak_python_mib": peak / 1024 ** 2, "saved_pages": len(results)}
                    del results
                finally:
                    tracemalloc.stop()
            measurement["speedup"] = measurement["sync"]["seconds"] / measurement["async"]["seconds"]
            rows.append(measurement)
            print(json.dumps(measurement, ensure_ascii=False), flush=True)
    finally:
        await runner.cleanup()
    report = {
        "measured_at": datetime.now(timezone.utc).isoformat(),
        "method": "HTTP keep-alive + одинаковый HTML-парсер; sync http.client в отдельном потоке, async aiohttp с 20 задачами; задержка сервера 5 мс; robots/rate/storage отключены. tracemalloc измеряет Python-аллокации клиента и локального сервера, не RSS и не native-память lxml.",
        "measurements": rows,
    }
    path = Path(filename)
    await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
    async with aiofiles.open(path, "w", encoding="utf-8") as file:
        await file.write(json.dumps(report, ensure_ascii=False, indent=2))
    return report


def parse_cli(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local", action="store_true", help="Учебный сайт, sitemap и ошибки HTTP на localhost")
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--compare", action="store_true", help="День 1: сравнение загрузки")
    modes.add_argument("--parse", action="store_true", help="День 2: парсинг отдельных страниц")
    modes.add_argument("--benchmark", action="store_true", help="Замеры 100/500/1000 страниц; --output задаёт JSON замеров")
    parser.add_argument("--urls", nargs="+", help="Стартовые URL")
    parser.add_argument("--sitemaps", nargs="+", help="Адреса sitemap.xml")
    parser.add_argument("--max-pages", type=int, default=None)
    parser.add_argument("--max-depth", type=int, default=None)
    parser.add_argument("--output", default=None)
    parser.add_argument("--config", help="Путь к JSON-конфигурации")
    parser.add_argument("--respect-robots", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--same-domain-only", action=argparse.BooleanOptionalAction, default=None)
    parser.add_argument("--rate-limit", type=float, default=None, help="Запросов в секунду на домен")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default=None)
    parser.add_argument("--log-file", default=None)
    args = parser.parse_args(argv)
    try:
        config = read_config(args.config) if args.config else {}
        config = copy.deepcopy(config)
        for section in ("crawler", "crawl", "logging"):
            if section in config and not isinstance(config[section], dict):
                raise ValueError(f"{section} должен быть объектом")
            config.setdefault(section, {})
        for value, section, field in [
            (args.max_pages, "crawl", "max_pages"), (args.max_depth, "crawler", "max_depth"),
            (args.respect_robots, "crawler", "respect_robots"),
            (args.same_domain_only, "crawl", "same_domain_only"),
            (args.rate_limit, "crawler", "requests_per_second"),
            (args.log_level, "logging", "level"), (args.log_file, "logging", "file"),
        ]:
            if value is not None:
                config[section][field] = value
        if args.urls is not None:
            config["start_urls"] = args.urls
        if args.sitemaps is not None:
            config["sitemaps"] = args.sitemaps
        if args.output is not None:
            config["output"] = args.output
        if args.max_pages is not None and args.max_pages < 0:
            raise ValueError("--max-pages должен быть неотрицательным")
        if args.max_depth is not None and args.max_depth < 0:
            raise ValueError("--max-depth должен быть неотрицательным")
        if args.rate_limit is not None and (not math.isfinite(args.rate_limit) or args.rate_limit <= 0):
            raise ValueError("--rate-limit должен быть положительным")
    except (OSError, ValueError, TypeError) as exc:
        parser.error(str(exc))
    return args, config


async def run_cli(args, config):
    if args.benchmark:
        await run_benchmarks(config.get("output", "benchmark_results.json"))
    else:
        await main(args.local, args.compare, args.parse, config.get("output", "crawl_results.json"), config)


if __name__ == "__main__":
    cli_args, cli_config = parse_cli()
    try:
        asyncio.run(run_cli(cli_args, cli_config))
    except (ValueError, TypeError, OSError) as exc:
        raise SystemExit(f"Ошибка: {exc}") from exc
