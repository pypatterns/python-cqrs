import typing

from cqrs.requests.cor_request_handler import CORRequestHandler
from cqrs.requests.fallback import RequestHandlerFallback
from cqrs.requests.request import IRequest
from cqrs.requests.request_handler import (
    RequestHandler,
    StreamingRequestHandler,
)
from cqrs.saga.models import SagaContext
from cqrs.saga.saga import Saga

_KT = typing.TypeVar("_KT", bound=typing.Type[IRequest])

# Type alias for handler types that can be bound to requests
HandlerType = typing.Union[
    typing.Type[RequestHandler],
    typing.Type[StreamingRequestHandler],
    typing.List[typing.Type[CORRequestHandler]],
    RequestHandlerFallback,
]


class RequestMap(typing.Dict[_KT, HandlerType]):
    _registry: typing.Dict[_KT, HandlerType]

    def bind(self, request_type: _KT, handler_type: HandlerType) -> None:
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
    _registry: typing.Dict[_SagaKT, _SagaHandlerType]

    def bind(self, context_type: _SagaKT, handler_type: _SagaHandlerType) -> None:
        self[context_type] = handler_type

    def __setitem__(self, __key: _SagaKT, __value: _SagaHandlerType) -> None:
        if __key in self:
            raise KeyError(f"{__key} already exists in registry")
        super().__setitem__(__key, __value)

    def __delitem__(self, __key: _SagaKT) -> typing.NoReturn:
        raise TypeError(f"{self.__class__.__name__} has no delete method")
