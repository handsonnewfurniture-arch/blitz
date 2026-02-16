from __future__ import annotations

import asyncio
import aiohttp
from typing import Any

from blitz.steps import BaseStep, StepRegistry
from blitz.utils.url_expander import expand_url_pattern
from blitz.utils.jsonpath import jsonpath_extract


@StepRegistry.register("fetch")
class FetchStep(BaseStep):
    """Async parallel HTTP fetching with retry, concurrency control, and connection pooling."""

    async def execute(self) -> list[dict[str, Any]]:
        return await self.execute_async()

    async def execute_async(self) -> list[dict[str, Any]]:
        urls = self._expand_urls()
        parallel = self.config.get("parallel", 10)
        retry_count = self.config.get("retry", 0)
        timeout = self.config.get("timeout", 30)
        extract_path = self.config.get("extract", None)
        method = self.config.get("method", "GET").upper()
        headers = self.config.get("headers", {})
        body = self.config.get("body", None)

        semaphore = asyncio.Semaphore(parallel)
        connector = aiohttp.TCPConnector(
            limit=parallel,
            enable_cleanup_closed=True,
        )
        client_timeout = aiohttp.ClientTimeout(total=timeout)

        results: list[dict[str, Any]] = []
        errors: list[str] = []

        async with aiohttp.ClientSession(
            connector=connector, timeout=client_timeout
        ) as session:
            tasks = [
                self._fetch_one(
                    session, url, method, headers, body,
                    semaphore, retry_count
                )
                for url in urls
            ]
            responses = await asyncio.gather(*tasks, return_exceptions=True)

        for resp in responses:
            if isinstance(resp, Exception):
                errors.append(str(resp))
                continue

            if extract_path:
                extracted = jsonpath_extract(resp, extract_path)
                if isinstance(extracted, list):
                    results.extend(
                        item if isinstance(item, dict) else {"value": item}
                        for item in extracted
                    )
                elif isinstance(extracted, dict):
                    results.append(extracted)
                elif extracted is not None:
                    results.append({"value": extracted})
            elif isinstance(resp, list):
                results.extend(
                    item if isinstance(item, dict) else {"value": item}
                    for item in resp
                )
            elif isinstance(resp, dict):
                results.append(resp)
            else:
                results.append({"value": resp})

        if errors:
            self.context.vars["_fetch_errors"] = errors

        return results

    async def _fetch_one(
        self, session: aiohttp.ClientSession,
        url: str, method: str, headers: dict,
        body: Any, semaphore: asyncio.Semaphore,
        retries: int,
    ):
        async with semaphore:
            last_error = None
            for attempt in range(retries + 1):
                try:
                    async with session.request(
                        method, url, headers=headers, json=body
                    ) as resp:
                        resp.raise_for_status()
                        content_type = resp.content_type or ""
                        if "json" in content_type:
                            return await resp.json()
                        text = await resp.text()
                        return {"_url": url, "_body": text}
                except Exception as e:
                    last_error = e
                    if attempt < retries:
                        await asyncio.sleep(2**attempt * 0.5)
            raise last_error

    def _expand_urls(self) -> list[str]:
        raw = self.config.get("urls") or self.config.get("url", "")
        if isinstance(raw, list):
            expanded = []
            for u in raw:
                expanded.extend(expand_url_pattern(u))
            return expanded
        return expand_url_pattern(raw)
