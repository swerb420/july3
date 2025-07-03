import asyncio
import logging
from typing import Any
import aiohttp

logger = logging.getLogger(__name__)


def aretry(max_attempts: int = 3, delay: float = 1.0):
    """Asynchronous retry decorator with exponential backoff."""

    def decorator(func):
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempts = 0
            while True:
                try:
                    return await func(*args, **kwargs)
                except Exception as e:  # broad catch for reliability layer
                    attempts += 1
                    logger.warning(
                        "Error in %s attempt %s/%s: %s",
                        func.__name__, attempts, max_attempts, e,
                    )
                    if attempts >= max_attempts:
                        logger.error("Failed after %s attempts", max_attempts)
                        raise
                    await asyncio.sleep(delay * attempts)
        return wrapper

    return decorator


@aretry(max_attempts=3, delay=2.0)
async def safe_request_async(method: str, url: str, **kwargs: Any) -> str:
    """Perform an async HTTP request with retries and timeout."""
    timeout = aiohttp.ClientTimeout(total=kwargs.pop("timeout", 10))
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.request(method, url, **kwargs) as resp:
            resp.raise_for_status()
            return await resp.text()
