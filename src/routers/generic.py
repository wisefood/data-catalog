# rendered_with_logging.py
from __future__ import annotations

import asyncio
import functools
import inspect
import time
from logging import Logger, getLogger
from typing import Any, Awaitable, Callable, Optional, Union, TypeVar, Dict

from fastapi import Request
from starlette.responses import Response
from pydantic import BaseModel

from exceptions import APIException

log = getLogger(__name__)

# -----------------------------------
# Generic API rendering
#
# These functions implement generically the
# ReST standards of the catalog API.
#
# N.B.: these standards should be
# observed all over the WiseFood APIs.
# ------------------------------------


# ---------- Success envelope ----------
class APIEnvelope(BaseModel):
    help: str
    success: bool = True
    result: Any


def _ok(result: Any, request: Request) -> APIEnvelope:
    return APIEnvelope(help=str(request.url), result=result)


# ---------- Helpers ----------
T = TypeVar("T")
EndpointFn = Union[Callable[..., T], Callable[..., Awaitable[T]]]
ResultMapper = Callable[[Any], Any]

REDACT_KEYS = {
    "password",
    "pwd",
    "token",
    "access_token",
    "authorization",
    "secret",
    "apikey",
    "api_key",
}


def _redact(d: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(d, dict):
        return d  # best effort
    out = {}
    for k, v in d.items():
        if k.lower() in REDACT_KEYS:
            out[k] = "***"
        else:
            out[k] = v
    return out


def _pick_request(args, kwargs, fn) -> Optional[Request]:
    req = kwargs.get("request")
    if isinstance(req, Request):
        return req
    sig = inspect.signature(fn)
    bound = sig.bind_partial(*args, **kwargs)
    for name, param in sig.parameters.items():
        val = bound.arguments.get(name)
        if isinstance(val, Request):
            return val
    return None


# ---------- Decorator ----------
def render(
    map_result: Optional[ResultMapper] = None,
    *,
    logger: Optional[Logger] = None,
    event: Optional[str] = None,
) -> Callable[[EndpointFn], EndpointFn]:
    """
    Wrap an endpoint to:
      - log start/end, duration, and request id (if present on request.state)
      - pass through Response objects
      - re-raise APIException for global handlers
      - wrap unknown errors via APIException.from_unexpected
      - envelope successful results uniformly

    Args:
      map_result: optional transformer for the endpoint's return before wrapping
      logger:     optional logger (defaults to module logger)
      event:      optional event name used in logs (defaults to func.__name__)
    """
    logger = logger or log

    def decorator(func: EndpointFn) -> EndpointFn:
        is_coro = inspect.iscoroutinefunction(func)

        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            req = _pick_request(args, kwargs, func)
            if req is None:
                raise RuntimeError(
                    "rendered(): endpoint must accept a 'request: Request' parameter "
                    "to build the success envelope and include context in logs."
                )

            started = time.perf_counter()
            ev = event or func.__name__
            rid = getattr(getattr(req, "state", None), "request_id", None)

            # Light, structured "start" log
            try:
                # Avoid dumping large bodies; include query/path only.
                logger.info(
                    "api.start",
                    extra={
                        "event": ev,
                        "method": req.method,
                        "path": req.url.path,
                        "query": dict(req.query_params),
                        "request_id": rid,
                    },
                )
            except Exception:
                # logging should never break the request
                pass

            try:
                result = (
                    await func(*args, **kwargs) if is_coro else func(*args, **kwargs)
                )

                # Pass-through starlette Response (files/streams/etc.)
                if isinstance(result, Response):
                    # end log still useful
                    dur = (time.perf_counter() - started) * 1000
                    logger.info(
                        "api.end",
                        extra={
                            "event": ev,
                            "method": req.method,
                            "path": req.url.path,
                            "status": getattr(result, "status_code", 200),
                            "duration_ms": round(dur, 2),
                            "request_id": rid,
                        },
                    )
                    return result

                if map_result:
                    result = map_result(result)

                envelope = _ok(result, req)

                dur = (time.perf_counter() - started) * 1000
                logger.info(
                    "api.end",
                    extra={
                        "event": ev,
                        "method": req.method,
                        "path": req.url.path,
                        "status": 200,  # FastAPI route decorator controls actual status code
                        "duration_ms": round(dur, 2),
                        "request_id": rid,
                    },
                )
                return envelope

            except APIException as exc:
                # Mirror your Flask approach: log but let handlers render
                level = (
                    30 if exc.status_code < 500 else 40
                )  # WARNING for 4xx, ERROR for 5xx
                dur = (time.perf_counter() - started) * 1000
                logger.log(
                    level,
                    "api.error.api_exception",
                    extra={
                        "event": ev,
                        "method": req.method,
                        "path": req.url.path,
                        "status": exc.status_code,
                        "code": getattr(exc, "code", None),
                        "detail": exc.detail,
                        "duration_ms": round(dur, 2),
                        "request_id": rid,
                    },
                    exc_info=exc.status_code >= 500,  # stack only on 5xx
                )
                raise  # handled by global APIException handler

            except Exception as exc:
                # Unexpected error: log stack and wrap
                dur = (time.perf_counter() - started) * 1000
                logger.exception(
                    "api.error.unexpected",
                    extra={
                        "event": ev,
                        "method": req.method,
                        "path": req.url.path,
                        "duration_ms": round(dur, 2),
                        "request_id": rid,
                    },
                )
                raise APIException.from_unexpected(exc) from exc

        if not is_coro:

            @functools.wraps(func)
            def sync_wrapper(*args, **kwargs):
                # run the async wrapper inside FastAPI's threadpool? We can just call it directly;
                # Starlette can handle sync endpoints directly, but we need our async logging logic.
                return asyncio.run(async_wrapper(*args, **kwargs))

            return sync_wrapper  # type: ignore[return-value]

        return async_wrapper  # type: ignore[return-value]

    return decorator
