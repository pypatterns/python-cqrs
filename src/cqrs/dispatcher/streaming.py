import inspect
import logging
import typing

from cqrs.circuit_breaker import should_use_fallback
from cqrs.container.protocol import Container
from cqrs.dispatcher.exceptions import RequestHandlerDoesNotExist
from cqrs.dispatcher.models import RequestDispatchResult
from cqrs.middlewares.base import MiddlewareChain
from cqrs.requests.fallback import RequestHandlerFallback
from cqrs.requests.map import RequestMap
from cqrs.requests.request import IRequest
from cqrs.requests.request_handler import StreamingRequestHandler

logger = logging.getLogger("cqrs")


class StreamingRequestDispatcher:
    """
    Dispatcher for streaming request handlers that work as generators.

    This dispatcher handles requests using handlers that yield responses
    as generators. After each yield, events are collected and can be emitted.

    When a primary streaming handler (used via RequestHandlerFallback) fails
    mid-stream, already-yielded RequestDispatchResult items are not rolled
    back and the fallback handler streams from its start. Results may
    therefore be duplicated; callers can de-duplicate if needed. The
    fallback path is driven by _stream_from_handler and handler_type
    (primary vs fallback).
    """

    def __init__(
        self,
        request_map: RequestMap,
        container: Container,
        middleware_chain: typing.Optional[MiddlewareChain] = None,
    ) -> None:
        self._request_map = request_map
        self._container = container
        self._middleware_chain = middleware_chain or MiddlewareChain()

    def dispatch(
        self,
        request: IRequest,
    ) -> typing.AsyncIterator[RequestDispatchResult]:
        """
        Dispatch a request to a streaming handler and yield results.

        Called without await; returns an AsyncIterator consumed with async for.
        After each yield from the handler, events are collected and included
        in the dispatch result. The generator continues until StopIteration.
        """
        return self._dispatch_impl(request)

    @staticmethod
    async def _stream_from_handler(
        request: IRequest,
        handler: StreamingRequestHandler,
    ) -> typing.AsyncIterator[RequestDispatchResult]:
        async for response in handler.handle(request):
            events = list(handler.events)
            handler.clear_events()
            yield RequestDispatchResult(response=response, events=events)

    async def _dispatch_impl(
        self,
        request: IRequest,
    ) -> typing.AsyncIterator[RequestDispatchResult]:
        """
        Dispatch to the mapped handler. For RequestHandlerFallback, on primary
        failure the fallback streams from scratch (see class docstring).
        """
        handler_type = self._request_map.get(type(request), None)
        if handler_type is None:
            raise RequestHandlerDoesNotExist(f"StreamingRequestHandler not found matching Request type {type(request)}")

        if isinstance(handler_type, list):
            raise TypeError(
                "StreamingRequestDispatcher does not support COR handler chains. "
                "Use RequestDispatcher for chain of responsibility pattern.",
            )

        if isinstance(handler_type, RequestHandlerFallback):
            primary = await self._container.resolve(handler_type.primary)
            fallback_handler = await self._container.resolve(handler_type.fallback)
            if not inspect.isasyncgenfunction(primary.handle) or not inspect.isasyncgenfunction(
                fallback_handler.handle,
            ):
                raise TypeError(
                    "RequestHandlerFallback with StreamingRequestDispatcher requires "
                    "both primary and fallback to be async generator handlers",
                )
            try:
                async for result in self._stream_from_handler(
                    request,
                    typing.cast(StreamingRequestHandler, primary),
                ):
                    yield result
            except Exception as primary_error:
                should_fallback = should_use_fallback(
                    primary_error,
                    handler_type.circuit_breaker,
                    handler_type.failure_exceptions,
                )
                if should_fallback:
                    if (
                        handler_type.circuit_breaker is not None
                        and handler_type.circuit_breaker.is_circuit_breaker_error(
                            primary_error,
                        )
                    ):
                        logger.warning(
                            "Circuit breaker open for streaming handler %s, switching to fallback %s",
                            handler_type.primary.__name__,
                            handler_type.fallback.__name__,
                        )
                    else:
                        logger.warning(
                            "Primary streaming handler %s failed: %s. Switching to fallback %s.",
                            handler_type.primary.__name__,
                            primary_error,
                            handler_type.fallback.__name__,
                        )
                    async for result in self._stream_from_handler(
                        request,
                        typing.cast(StreamingRequestHandler, fallback_handler),
                    ):
                        yield result
                else:
                    raise primary_error
            return

        handler_type_typed = typing.cast(typing.Type[StreamingRequestHandler], handler_type)
        handler: StreamingRequestHandler = await self._container.resolve(handler_type_typed)

        if not inspect.isasyncgenfunction(handler.handle):
            handler_name = (
                handler_type_typed.__name__ if hasattr(handler_type_typed, "__name__") else str(handler_type_typed)
            )
            raise TypeError(
                f"Handler {handler_name}.handle must be an async generator function",
            )

        async for result in self._stream_from_handler(request, handler):
            yield result
