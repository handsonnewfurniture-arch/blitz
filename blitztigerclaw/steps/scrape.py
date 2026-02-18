from __future__ import annotations

import asyncio
import re
import aiohttp
from typing import Any

from blitztigerclaw.steps import BaseStep, StepMeta, StepRegistry
from blitztigerclaw.utils.url_expander import expand_url_pattern


@StepRegistry.register("scrape")
class ScrapeStep(BaseStep):
    """Async HTML scraping with CSS-like selector extraction.

    Uses a lightweight built-in parser (no external deps required).
    For advanced selectors, install beautifulsoup4.
    """

    meta = StepMeta(
        default_strategy="async",
        is_source=True,
        required_config=("url", "urls"),
        description="HTML scraping with CSS selectors",
        config_docs={
            "url": "string — URL to scrape",
            "urls": "list[string] — multiple URLs",
            "select": "dict — {field_name: css_selector}",
            "parallel": "int — concurrent requests",
        },
    )

    async def execute(self) -> list[dict[str, Any]]:
        return await self.execute_async()

    async def execute_async(self) -> list[dict[str, Any]]:
        urls = self._expand_urls()
        parallel = self.config.get("parallel", 5)
        selectors = self.config.get("select", {})
        timeout = self.config.get("timeout", 30)

        semaphore = asyncio.Semaphore(parallel)
        connector = aiohttp.TCPConnector(limit=parallel)
        client_timeout = aiohttp.ClientTimeout(total=timeout)

        results = []

        async with aiohttp.ClientSession(
            connector=connector, timeout=client_timeout
        ) as session:
            tasks = [
                self._scrape_one(session, url, selectors, semaphore)
                for url in urls
            ]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

        for resp in responses:
            if isinstance(resp, Exception):
                continue
            if isinstance(resp, list):
                results.extend(resp)
            elif isinstance(resp, dict):
                results.append(resp)

        return results

    async def _scrape_one(self, session, url, selectors, semaphore):
        async with semaphore:
            async with session.get(url) as resp:
                resp.raise_for_status()
                html = await resp.text()

        try:
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(html, "html.parser")
            return self._extract_bs4(soup, selectors, url)
        except ImportError:
            return self._extract_regex(html, selectors, url)

    def _extract_bs4(self, soup, selectors: dict, url: str) -> list[dict]:
        """Extract data using BeautifulSoup CSS selectors."""
        # Find the first selector's matches to determine row count
        first_key = list(selectors.keys())[0]
        first_sel = selectors[first_key]
        css, attr_type = self._parse_selector(first_sel)
        first_matches = soup.select(css)

        rows = []
        for i, _ in enumerate(first_matches):
            row = {"_url": url}
            for field, selector in selectors.items():
                css, attr_type = self._parse_selector(selector)
                matches = soup.select(css)
                if i < len(matches):
                    el = matches[i]
                    if attr_type == "text":
                        row[field] = el.get_text(strip=True)
                    elif attr_type.startswith("attr("):
                        attr_name = attr_type[5:-1]
                        row[field] = el.get(attr_name, "")
                    else:
                        row[field] = el.get_text(strip=True)
                else:
                    row[field] = None
            rows.append(row)

        return rows

    def _extract_regex(self, html: str, selectors: dict, url: str) -> list[dict]:
        """Fallback extraction using regex when BeautifulSoup is not available."""
        row = {"_url": url}
        for field, selector in selectors.items():
            css, attr_type = self._parse_selector(selector)
            tag = css.split(".")[-1] if "." in css else css.split("#")[-1] if "#" in css else css
            if attr_type == "text":
                pattern = f"<{tag}[^>]*>(.*?)</{tag}>"
                match = re.search(pattern, html, re.DOTALL)
                row[field] = match.group(1).strip() if match else None
            else:
                row[field] = None
        return [row]

    def _parse_selector(self, selector: str) -> tuple[str, str]:
        """Parse 'h1::text' or 'a.link::attr(href)' into (css, type)."""
        if "::text" in selector:
            return selector.replace("::text", ""), "text"
        if "::attr(" in selector:
            parts = selector.split("::attr(")
            attr_name = parts[1].rstrip(")")
            return parts[0], f"attr({attr_name})"
        return selector, "text"

    def _expand_urls(self) -> list[str]:
        raw = self.config.get("urls") or self.config.get("url", "")
        if isinstance(raw, list):
            expanded = []
            for u in raw:
                expanded.extend(expand_url_pattern(u))
            return expanded
        return expand_url_pattern(raw)
