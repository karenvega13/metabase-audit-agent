"""
src/utils/retry.py — Decorador de retry con backoff exponencial.

Uso:
    from src.utils.retry import retry_with_backoff

    @retry_with_backoff(max_retries=3, base_wait=5, retryable_codes=["429","500","503"])
    def llamar_api():
        ...
"""

import time
import functools
from typing import Callable


def retry_with_backoff(
    max_retries: int = 3,
    base_wait: int = 5,
    retryable_codes: list[str] | None = None,
) -> Callable:
    """
    Decorador que reintenta la función decorada ante errores transitorios.

    Args:
        max_retries: número máximo de reintentos.
        base_wait: segundos de espera inicial (se dobla en cada intento: 5s, 10s, 20s...).
        retryable_codes: fragmentos de texto que, si aparecen en el error, activan el retry.
    """
    if retryable_codes is None:
        retryable_codes = ["429", "500", "503", "RateLimitError", "APIStatusError"]

    def decorator(fn: Callable) -> Callable:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            last_error = None
            for attempt in range(max_retries):
                try:
                    return fn(*args, **kwargs)
                except Exception as exc:
                    last_error = exc
                    err_str = str(exc)
                    if any(code in err_str for code in retryable_codes):
                        wait = (2 ** attempt) * base_wait
                        print(f"  [retry] Error ({err_str[:60]}) — reintentando en {wait}s ({attempt + 1}/{max_retries})")
                        time.sleep(wait)
                    else:
                        raise
            raise RuntimeError(f"Max reintentos alcanzados: {last_error}") from last_error
        return wrapper
    return decorator
