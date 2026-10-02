import typing

if typing.TYPE_CHECKING:
    from cqrs.events.pydantic import (
        PydanticDomainEvent,
        PydanticEvent,
        PydanticNotificationEvent,
    )
    from cqrs.requests.pydantic import PydanticRequest
    from cqrs.pydantic_response import PydanticResponse
    from cqrs.outbox.sqlalchemy import (
        SqlAlchemyOutboxedEventRepository,
        rebind_outbox_model,
    )
    from cqrs.sqlalchemy_types import (
        Binary16,
        DialectAwareType,
        DialectTypeHandler,
        JSONType,
        PayloadBinary,
        UUIDBinary,
    )


from cqrs.compressors import Compressor, ZlibCompressor
from cqrs.container.di import DIContainer
from cqrs.container.protocol import Container, SupportsScope
from cqrs.container.scope import (
    ScopeAwareContainer,
    ScopeStrategy,
    bind_scope,
    current_container,
    enter_scope,
)
from cqrs.circuit_breaker import ICircuitBreaker
from cqrs.events import EventMap
from cqrs.events.fallback import EventHandlerFallback
from cqrs.events.event import (
    DCEvent,
    DCDomainEvent,
    DCNotificationEvent,
    DomainEvent,
    Event,
    IDomainEvent,
    IEvent,
    INotificationEvent,
    NotificationEvent,
)
from cqrs.events.event_emitter import EventEmitter
from cqrs.events.event_handler import EventHandler
from cqrs.mediator import (
    EventMediator,
    RequestMediator,
    SagaMediator,
    StreamingRequestMediator,
)
from cqrs.outbox.map import OutboxedEventMap
from cqrs.outbox.repository import (
    EventStatus,
    OutboxedEvent,
    OutboxedEventRepository,
)
from cqrs.producer import EventProducer
from cqrs.deserializers import DeserializeProtobufError, ProtobufDeserializer
from cqrs.serializers import (
    EventCodec,
    EventSerializer,
    JsonEventSerializer,
    ProtobufEventSerializer,
)
from cqrs.requests.fallback import RequestHandlerFallback
from cqrs.requests.map import RequestMap, SagaMap
from cqrs.requests.mermaid import CoRMermaid
from cqrs.requests.request import DCRequest, IRequest, Request
from cqrs.requests.request_handler import (
    RequestHandler,
    StreamingRequestHandler,
)
from cqrs.response import DCResponse, IResponse, Response
from cqrs.saga.mermaid import SagaMermaid
from cqrs.saga.models import ContextT
from cqrs.saga.saga import Saga
from cqrs.saga.step import (
    Resp,
    SagaStepHandler,
    SagaStepResult,
)


from cqrs._dataclass_utils import pydantic_extra_error

__all__ = (
    "ICircuitBreaker",
    "EventHandlerFallback",
    "RequestHandlerFallback",
    "RequestMediator",
    "SagaMediator",
    "StreamingRequestMediator",
    "EventMediator",
    "DomainEvent",
    "IDomainEvent",
    "DCDomainEvent",
    "PydanticDomainEvent",
    "NotificationEvent",
    "INotificationEvent",
    "DCNotificationEvent",
    "PydanticNotificationEvent",
    "Event",
    "IEvent",
    "DCEvent",
    "PydanticEvent",
    "EventEmitter",
    "EventHandler",
    "EventMap",
    "OutboxedEventMap",
    "EventStatus",
    "OutboxedEvent",
    "Request",
    "IRequest",
    "DCRequest",
    "PydanticRequest",
    "RequestHandler",
    "StreamingRequestHandler",
    "RequestMap",
    "SagaMap",
    "Response",
    "IResponse",
    "DCResponse",
    "PydanticResponse",
    "OutboxedEventRepository",
    "SqlAlchemyOutboxedEventRepository",
    "EventProducer",
    "EventSerializer",
    "EventCodec",
    "JsonEventSerializer",
    "ProtobufEventSerializer",
    "ProtobufDeserializer",
    "DeserializeProtobufError",
    "Container",
    "SupportsScope",
    "DIContainer",
    "ScopeAwareContainer",
    "ScopeStrategy",
    "bind_scope",
    "current_container",
    "enter_scope",
    "Compressor",
    "ZlibCompressor",
    "rebind_outbox_model",
    "Saga",
    "SagaStepHandler",
    "SagaStepResult",
    "Resp",
    "ContextT",
    "SagaMermaid",
    "CoRMermaid",
    "DialectAwareType",
    "DialectTypeHandler",
    "UUIDBinary",
    "Binary16",
    "JSONType",
    "PayloadBinary",
)

_LAZY_PYDANTIC = {
    "PydanticEvent": ("cqrs.events.pydantic", "PydanticEvent"),
    "PydanticDomainEvent": ("cqrs.events.pydantic", "PydanticDomainEvent"),
    "PydanticNotificationEvent": (
        "cqrs.events.pydantic",
        "PydanticNotificationEvent",
    ),
    "PydanticRequest": ("cqrs.requests.pydantic", "PydanticRequest"),
    "PydanticResponse": ("cqrs.pydantic_response", "PydanticResponse"),
}

_LAZY_SQLALCHEMY = {
    "SqlAlchemyOutboxedEventRepository": (
        "cqrs.outbox.sqlalchemy",
        "SqlAlchemyOutboxedEventRepository",
    ),
    "rebind_outbox_model": ("cqrs.outbox.sqlalchemy", "rebind_outbox_model"),
    "DialectAwareType": ("cqrs.sqlalchemy_types", "DialectAwareType"),
    "DialectTypeHandler": ("cqrs.sqlalchemy_types", "DialectTypeHandler"),
    "UUIDBinary": ("cqrs.sqlalchemy_types", "UUIDBinary"),
    "Binary16": ("cqrs.sqlalchemy_types", "Binary16"),
    "JSONType": ("cqrs.sqlalchemy_types", "JSONType"),
    "PayloadBinary": ("cqrs.sqlalchemy_types", "PayloadBinary"),
}


def __getattr__(name: str) -> typing.Any:
    if name in _LAZY_PYDANTIC:
        module_name, attr = _LAZY_PYDANTIC[name]
        try:
            module = __import__(module_name, fromlist=[attr])
        except ImportError as exc:
            raise pydantic_extra_error(name) from exc
        return getattr(module, attr)
    if name in _LAZY_SQLALCHEMY:
        module_name, attr = _LAZY_SQLALCHEMY[name]
        # ImportError message is raised by the sqlalchemy modules themselves.
        module = __import__(module_name, fromlist=[attr])
        return getattr(module, attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
