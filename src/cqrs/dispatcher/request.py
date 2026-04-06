import asyncio
import logging
import typing
from collections import abc

from cqrs.circuit_breaker import should_use_fallback
from cqrs.container.protocol import Container
from cqrs.dispatcher.exceptions import (
    RequestHandlerDoesNotExist,
    RequestHandlerTypeError,
)
from cqrs.dispatcher.models import RequestDispatchResult
from cqrs.middlewares.base import HandleType, MiddlewareChain
from cqrs.requests.cor_request_handler import (
    CORRequestHandler,
    build_chain,
    CORRequestHandlerT as CORRequestHandlerType,
)
from cqrs.requests.fallback import RequestHandlerFallback
from cqrs.requests.map import RequestMap, HandlerType
from cqrs.requests.request import IRequest
from cqrs.requests.request_handler import RequestHandler

logger = logging.getLogger("cqrs")

_RequestHandler = typing.Union[RequestHandler, CORRequestHandler]


class RequestDispatcher:
    def __init__(
        self,
        request_map: RequestMap,
        container: Container,
        middleware_chain: typing.Optional[MiddlewareChain] = None,
    ) -> None:
        self._request_map = request_map
        self._container = container
        self._middleware_chain = middleware_chain or MiddlewareChain()

    async def _resolve_handler(
        self,
        handler_type: HandlerType,
    ) -> _RequestHandler:
        """
        Resolve a single handler or build a chain from multiple handlers.

        For single handlers, resolves them using the DI container.
        For lists of handlers, validates they are COR handlers and builds a chain.
        RequestHandlerFallback is not resolved here; use dispatch fallback path.
        """
        if isinstance(handler_type, RequestHandlerFallback):
            raise RequestHandlerTypeError(
                "RequestHandlerFallback must be handled in dispatch, not _resolve_handler",
            )
        if isinstance(handler_type, abc.Iterable):
            if not all(
                issubclass(
                    handler_cls,
                    CORRequestHandler,
                )
                for handler_cls in handler_type
            ):
                raise RequestHandlerTypeError(
                    "COR handler must be type CORRequestHandler",
                )

            tasks = [self._container.resolve(h) for h in handler_type]
            handlers = await asyncio.gather(*tasks)
            return build_chain(
                typing.cast(typing.List[CORRequestHandlerType], handlers),
            )

        return typing.cast(_RequestHandler, await self._container.resolve(handler_type))

    async def _dispatch_fallback(
        self,
        request: IRequest,
        fallback_config: RequestHandlerFallback,
    ) -> RequestDispatchResult:
        """Dispatch using primary handler with fallback on failure."""
        primary = await self._container.resolve(fallback_config.primary)
        try:
            wrapped_primary = self._middleware_chain.wrap(
                typing.cast(HandleType, primary.handle),
            )
            if fallback_config.circuit_breaker is not None:
                response = await fallback_config.circuit_breaker.call(
                    fallback_config.primary,
                    wrapped_primary,
                    request,
                )
            else:
                response = await wrapped_primary(request)
            return RequestDispatchResult(response=response, events=primary.events)
        except Exception as primary_error:
            should_fallback = should_use_fallback(
                primary_error,
                fallback_config.circuit_breaker,
                fallback_config.failure_exceptions,
            )
            if should_fallback:
                if (
                    fallback_config.circuit_breaker is not None
                    and fallback_config.circuit_breaker.is_circuit_breaker_error(
                        primary_error,
                    )
                ):
                    logger.warning(
                        "Circuit breaker open for request handler %s, switching to fallback %s",
                        fallback_config.primary.__name__,
                        fallback_config.fallback.__name__,
                    )
                else:
                    logger.warning(
                        "Primary handler %s failed: %s. Switching to fallback %s.",
                        fallback_config.primary.__name__,
                        primary_error,
                        fallback_config.fallback.__name__,
                    )
                fallback_handler = await self._container.resolve(fallback_config.fallback)
                wrapped_fallback = self._middleware_chain.wrap(
                    typing.cast(HandleType, fallback_handler.handle),
                )
                response = await wrapped_fallback(request)
                return RequestDispatchResult(
                    response=response,
                    events=fallback_handler.events,
                )
            raise primary_error

    async def dispatch(self, request: IRequest) -> RequestDispatchResult:
        handler_type = self._request_map.get(type(request), None)
        if not handler_type:
            raise RequestHandlerDoesNotExist(
                f"RequestHandler not found matching Request type {type(request)}",
            )
        if isinstance(handler_type, RequestHandlerFallback):
            return await self._dispatch_fallback(request, handler_type)
        handler: _RequestHandler = await self._resolve_handler(handler_type)
        wrapped_handle = self._middleware_chain.wrap(handler.handle)
        response = await wrapped_handle(request)

        return RequestDispatchResult(response=response, events=handler.events)
