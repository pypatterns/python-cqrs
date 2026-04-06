import datetime
import typing
import uuid

from cqrs.saga.storage.enums import SagaStepStatus


class SagaLogEntry:
    """
    Entry in the saga execution log.
    Ordinary Python class as requested (no Pydantic).
    """

    def __init__(
        self,
        saga_id: uuid.UUID,
        step_name: str,
        action: typing.Literal["act", "compensate"],
        status: SagaStepStatus,
        timestamp: datetime.datetime,
        details: typing.Optional[str] = None,
    ) -> None:
        self.saga_id = saga_id
        self.step_name = step_name
        self.action = action
        self.status = status
        self.timestamp = timestamp
        self.details = details

    def __repr__(self) -> str:
        return (
            f"SagaLogEntry(saga_id={self.saga_id}, "
            f"step_name='{self.step_name}', action='{self.action}', "
            f"status={self.status}, timestamp={self.timestamp})"
        )
