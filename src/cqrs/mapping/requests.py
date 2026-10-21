import typing

from cqrs.handlers.cor import CORRequestHandler
from cqrs.requests.fallback import RequestHandlerFallback
from cqrs.models.request import IRequest
from cqrs.handlers.request import (
    RequestHandler,
    StreamingRequestHandler,
)
from cqrs.saga.models import SagaContext
from cqrs.saga.saga import Saga

_KT = typing.TypeVar("_KT", bound=typing.Type[IRequest])

# Type alias for handler types that can be bound to requests
HandlerType = (
    typing.Type[RequestHandler | StreamingRequestHandler]
    | typing.List[typing.Type[CORRequestHandler]]
    | RequestHandlerFallback
)


class RequestMap(typing.Dict[_KT, HandlerType]):
    """
    Registry of one handler per request type.

    [AI Instruction]
    Commands and queries are both registered here; both are ``cqrs.Request``.
    Fill this map in ``commands_mapper`` and ``queries_mapper`` passed to
    ``bootstrap``, or hand it to ``RequestMediator``. One request type has one
    entry. Events go on ``EventMap``, sagas on ``SagaMap``.
    """

    _registry: typing.Dict[_KT, HandlerType]

    def bind(self, request_type: _KT, handler_type: HandlerType) -> None:
        """
        Register the single handler for a request type.

        [AI Instruction]
        Wire one request type to the class that handles it. Pass a
        ``RequestHandler``, a ``StreamingRequestHandler``, a list of
        ``CORRequestHandler`` classes, or a ``RequestHandlerFallback``. A
        second ``bind`` for the same request raises ``KeyError``. There is no
        registration decorator and no command bus. The handler's result type
        comes from the request; this call does not declare it.
        """
        self[request_type] = handler_type

    def __setitem__(self, __key: _KT, __value: HandlerType) -> None:
        if __key in self:
            raise KeyError(f"{__key} already exists in registry")
        super().__setitem__(__key, __value)

    def __delitem__(self, __key: _KT) -> typing.NoReturn:
        raise TypeError(f"{self.__class__.__name__} has no delete method")


_SagaKT = typing.TypeVar("_SagaKT", bound=typing.Type[SagaContext])
_SagaHandlerType = typing.Type[Saga]


class SagaMap(typing.Dict[_SagaKT, _SagaHandlerType]):
    """
    Registry of one saga class per saga context type.

    [AI Instruction]
    Use when a process spans services and a later failure must undo earlier
    steps. Register from ``sagas_mapper`` on ``cqrs.bootstrap.saga.bootstrap``,
    or pass the map to ``SagaMediator``. A single-service write is a request
    on ``RequestMap``, not a saga.
    """

    _registry: typing.Dict[_SagaKT, _SagaHandlerType]

    def bind(self, context_type: _SagaKT, handler_type: _SagaHandlerType) -> None:
        """
        [AI Instruction]
        Bind one context class to one ``Saga`` subclass. Every step of that
        saga must use the same context type. A second ``bind`` for the same
        context raises ``KeyError``.
        """
        self[context_type] = handler_type

    def __setitem__(self, __key: _SagaKT, __value: _SagaHandlerType) -> None:
        if __key in self:
            raise KeyError(f"{__key} already exists in registry")
        super().__setitem__(__key, __value)

    def __delitem__(self, __key: _SagaKT) -> typing.NoReturn:
        raise TypeError(f"{self.__class__.__name__} has no delete method")
