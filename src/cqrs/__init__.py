"""
python-cqrs.

[AI Instruction]
Import the library as ``cqrs``. Names listed in ``__all__`` below are the
stable surface. Commands and queries are both ``cqrs.Request``. Wire a
request with ``RequestMap.bind``, an event with ``EventMap.bind``, and a
saga with ``SagaMap.bind``. There is no ``Command``, ``Query``, or
``CommandBus`` type, and no ``register_handler`` decorator.

Startup is not on this package. Import the submodule:

- ``cqrs.bootstrap.requests.bootstrap`` builds ``RequestMediator``.
- ``cqrs.bootstrap.requests.bootstrap_streaming`` builds
  ``StreamingRequestMediator``.
- ``cqrs.bootstrap.events.bootstrap`` builds ``EventMediator``.
- ``cqrs.bootstrap.saga.bootstrap`` builds ``SagaMediator``.

Tests import ``cqrs.testing``. ``from cqrs.testing import test_harness`` runs the
mediators above and records what they did. ``test_harness`` is not in
``__all__``: tests are not part of the runtime surface, the same way
``cqrs.bootstrap`` is not.

Requests and handlers on ``cqrs``: ``Request``, ``DCRequest``, ``Response``,
``RequestHandler``, ``StreamingRequestHandler``, ``RequestMediator``,
``StreamingRequestMediator``, ``RequestMap``, ``RequestHandlerFallback``.
``PydanticRequest`` and ``PydanticResponse`` are lazy and need the
``pydantic`` extra. A chain of handlers is ``cqrs.handlers.CORRequestHandler``.
``build_chain`` is ``cqrs.handlers.cor.build_chain``.

Events on ``cqrs``: ``DomainEvent`` for this process, ``NotificationEvent``
for another service, plus ``EventHandler``, ``EventMap``, ``EventMediator``,
``EventEmitter``, ``EventHandlerFallback``. Pydantic event classes are lazy
and need the ``pydantic`` extra.

Sagas on ``cqrs``: ``Saga``, ``SagaStepHandler``, ``SagaStepResult``,
``SagaMediator``, ``SagaMap``. Import the rest from their modules.
``SagaContext`` is ``cqrs.saga.models.SagaContext``. A fallback step is
``cqrs.saga.fallback.Fallback``. Resume a run with
``cqrs.saga.recovery.recover_saga``. The storage protocol is
``cqrs.saga.storage.ISagaStorage``. Implementations are
``cqrs.saga.storage.memory.MemorySagaStorage``,
``cqrs.saga.storage.sqlalchemy.SqlAlchemySagaStorage`` (extra
``sqlalchemy``), and ``cqrs.saga.storage.sqlmodel.SqlModelSagaStorage``
(extra ``sqlmodel``). Those storage classes are not attributes of ``cqrs``.

Outbox on ``cqrs``: ``OutboxedEventRepository``, ``OutboxedEventMap``,
``EventProducer``, ``EventStatus``. ``SqlAlchemyOutboxedEventRepository``,
``rebind_outbox_model``, and the dialect types are lazy and need the
``sqlalchemy`` extra. ``SqlModelOutboxedEventRepository`` is lazy and needs
the ``sqlmodel`` extra. Codecs on ``cqrs``: ``JsonEventSerializer``,
``ProtobufEventSerializer`` (extra ``protobuf``).

Inbox on ``cqrs``: ``InboxRepository``, ``InboxMessage``, ``InboxStatus``,
``InboxMessageMap``, ``InboxProcessor``. ``SqlAlchemyInboxRepository`` and
``rebind_inbox_model`` are lazy and need the ``sqlalchemy`` extra.
``SqlModelInboxRepository`` is lazy and needs the ``sqlmodel`` extra.
Presentation calls ``add`` with a batch. ``InboxProcessor`` reads rows and
calls the mediator that is already registered. FastAPI and FastStream are
not imported here.

Brokers are not on ``cqrs``. Kafka is
``cqrs.adapters.kafka.kafka_producer_factory`` plus
``cqrs.message_brokers.kafka.KafkaMessageBroker`` (extra ``kafka``). AMQP is
``cqrs.message_brokers.amqp.AMQPMessageBroker`` (extra ``rabbit``).

Containers: pass one into bootstrap. ``cqrs.DIContainer`` wraps the ``di``
package, and bootstrap wraps a raw ``di.Container`` itself.
``cqrs.container.dependency_injector.DependencyInjectorCQRSContainer`` wraps
dependency-injector. ``cqrs.container.dishka.DishkaCQRSContainer`` needs the
``dishka`` extra. Scope helpers on ``cqrs`` are ``ScopeStrategy``,
``bind_scope``, ``enter_scope``, and ``current_container``.

Middleware is ``cqrs.middlewares.Middleware``. Logging is
``cqrs.middlewares.logging.LoggingMiddleware``; bootstrap adds it when the
list does not already contain it. A circuit breaker adapter is
``cqrs.adapters.circuit_breaker.AioBreakerAdapter`` (extra ``aiobreaker``).
Diagrams on ``cqrs`` are ``CoRMermaid`` and ``SagaMermaid``.

Do not import ``cqrs.dispatcher`` from application code. Mediators call it.
Optional integrations are extras in ``pyproject.toml``. Reading a lazy name
without its extra raises ``ImportError``.
"""

import typing

if typing.TYPE_CHECKING:
    from cqrs.models.pydantic import (
        PydanticDomainEvent,
        PydanticEvent,
        PydanticNotificationEvent,
    )
    from cqrs.models.pydantic import PydanticRequest
    from cqrs.models.pydantic import PydanticResponse
    from cqrs.inbox.sqlalchemy import (
        SqlAlchemyInboxRepository,
        rebind_inbox_model,
    )
    from cqrs.inbox.sqlmodel import SqlModelInboxRepository
    from cqrs.outbox.sqlalchemy import (
        SqlAlchemyOutboxedEventRepository,
        rebind_outbox_model,
    )
    from cqrs.outbox.sqlmodel import SqlModelOutboxedEventRepository
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
from cqrs.models.event import (
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
from cqrs.handlers.event import EventHandler
from cqrs.mediators import (
    EventMediator,
    RequestMediator,
    SagaMediator,
    StreamingRequestMediator,
)
from cqrs.inbox.map import InboxMessageMap
from cqrs.inbox.processor import InboxProcessor
from cqrs.inbox.repository import InboxMessage, InboxRepository, InboxStatus
from cqrs.mapping.outbox import OutboxedEventMap
from cqrs.outbox.repository import (
    EventStatus,
    OutboxedEvent,
    OutboxedEventRepository,
)
from cqrs.message_brokers.producer import EventProducer
from cqrs.deserializers import DeserializeProtobufError, ProtobufDeserializer
from cqrs.serializers import (
    EventCodec,
    EventSerializer,
    JsonEventSerializer,
    ProtobufEventSerializer,
)
from cqrs.requests.fallback import RequestHandlerFallback
from cqrs.mapping.requests import RequestMap, SagaMap
from cqrs.mermaid.cor import CoRMermaid
from cqrs.models.request import DCRequest, IRequest, Request
from cqrs.handlers.request import (
    RequestHandler,
    StreamingRequestHandler,
)
from cqrs.models.response import DCResponse, IResponse, Response
from cqrs.mermaid.saga import SagaMermaid
from cqrs.saga.models import ContextT
from cqrs.saga.saga import Saga
from cqrs.handlers.saga import (
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
    "SqlModelOutboxedEventRepository",
    "InboxRepository",
    "InboxMessage",
    "InboxStatus",
    "InboxMessageMap",
    "InboxProcessor",
    "SqlAlchemyInboxRepository",
    "SqlModelInboxRepository",
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
    "rebind_inbox_model",
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
    "PydanticEvent": ("cqrs.models.pydantic", "PydanticEvent"),
    "PydanticDomainEvent": ("cqrs.models.pydantic", "PydanticDomainEvent"),
    "PydanticNotificationEvent": (
        "cqrs.models.pydantic",
        "PydanticNotificationEvent",
    ),
    "PydanticRequest": ("cqrs.models.pydantic", "PydanticRequest"),
    "PydanticResponse": ("cqrs.models.pydantic", "PydanticResponse"),
}

_LAZY_SQLALCHEMY = {
    "SqlAlchemyOutboxedEventRepository": (
        "cqrs.outbox.sqlalchemy",
        "SqlAlchemyOutboxedEventRepository",
    ),
    "rebind_outbox_model": ("cqrs.outbox.sqlalchemy", "rebind_outbox_model"),
    "SqlAlchemyInboxRepository": (
        "cqrs.inbox.sqlalchemy",
        "SqlAlchemyInboxRepository",
    ),
    "rebind_inbox_model": ("cqrs.inbox.sqlalchemy", "rebind_inbox_model"),
    "DialectAwareType": ("cqrs.sqlalchemy_types", "DialectAwareType"),
    "DialectTypeHandler": ("cqrs.sqlalchemy_types", "DialectTypeHandler"),
    "UUIDBinary": ("cqrs.sqlalchemy_types", "UUIDBinary"),
    "Binary16": ("cqrs.sqlalchemy_types", "Binary16"),
    "JSONType": ("cqrs.sqlalchemy_types", "JSONType"),
    "PayloadBinary": ("cqrs.sqlalchemy_types", "PayloadBinary"),
}

_LAZY_SQLMODEL = {
    "SqlModelOutboxedEventRepository": (
        "cqrs.outbox.sqlmodel",
        "SqlModelOutboxedEventRepository",
    ),
    "SqlModelInboxRepository": (
        "cqrs.inbox.sqlmodel",
        "SqlModelInboxRepository",
    ),
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
    if name in _LAZY_SQLMODEL:
        module_name, attr = _LAZY_SQLMODEL[name]
        # ImportError message is raised by the sqlmodel modules themselves.
        module = __import__(module_name, fromlist=[attr])
        return getattr(module, attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
