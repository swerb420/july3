import logging
import time
from functools import wraps
from typing import Callable, Any
import requests

logger = logging.getLogger(__name__)


def retry(max_attempts: int = 3, delay: float = 1.0) -> Callable:
    """Retry decorator with exponential backoff."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            attempts = 0
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as e:  # broad catch for reliability layer
                    attempts += 1
                    logger.warning(
                        "Error in %s attempt %s/%s: %s",
                        func.__name__, attempts, max_attempts, e,
                    )
                    if attempts >= max_attempts:
                        logger.error("Failed after %s attempts", max_attempts)
                        raise
                    time.sleep(delay * attempts)
        return wrapper

    return decorator


@retry(max_attempts=3, delay=2.0)
def safe_request(method: str, url: str, **kwargs: Any) -> requests.Response:
    """Perform a HTTP request with retries and timeout."""
    if "timeout" not in kwargs:
        kwargs["timeout"] = 10
    response = requests.request(method, url, **kwargs)
    response.raise_for_status()
    return response
