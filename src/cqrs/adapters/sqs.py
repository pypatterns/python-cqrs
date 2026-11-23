"""
SQS client used by ``SQSTransport``.

[AI Instruction]
This module is the ``sqs`` extra. It imports ``aioboto3`` and is loaded only
when something imports it (the factory does that for an ``sqs://`` URL).
Queue names are passed through. Do not replace dots here. The transport
decides the name, including the ``{topic}-dlq`` emulation and the
``{queue}-{group}`` fan-out suffix.
"""

from __future__ import annotations

import base64
import json
import logging
import urllib.parse
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from cqrs.transport.errors import TransportError

import aioboto3
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger("cqrs")

_ATTR_CONTENT_TYPE = "content_type"
_ATTR_KEY = "key"
_ATTR_HEADERS = "headers"
_ATTR_BODY_ENCODING = "body_encoding"
_ATTR_ATTEMPT = "delivery_attempt"
_ENCODING_BASE64 = "base64"
_UNKEYED_GROUP = "_cqrs"
_ALREADY_EXISTS = {"QueueAlreadyExists", "QueueNameExists"}


@dataclass
class SQSDelivery:
    """One received SQS message, already decoded into transport fields."""

    message_id: str
    receipt_handle: str
    body: bytes
    headers: dict[str, str]
    content_type: str | None
    key: str | None
    receive_count: int
    preserved_attempt: int | None
    sent_timestamp: int


class SQSClient:
    """
    Long-lived aioboto3 SQS client.

    ``start`` opens it and ``close`` closes it. Queue URLs returned by
    LocalStack are rewritten onto ``endpoint_url`` so later calls hit the
    same host the caller configured.
    """

    def __init__(
        self,
        *,
        endpoint_url: str | None,
        region_name: str,
        aws_access_key_id: str | None,
        aws_secret_access_key: str | None,
        aws_session_token: str | None,
        read_timeout: float,
    ) -> None:
        self._endpoint_url = endpoint_url
        self._region_name = region_name
        self._access_key = aws_access_key_id
        self._secret_key = aws_secret_access_key
        self._session_token = aws_session_token
        self._read_timeout = read_timeout
        self._session = aioboto3.Session()
        self._context: Any = None
        self._client: Any = None
        self._urls: dict[str, str] = {}

    async def start(self) -> None:
        if self._client is not None:
            return
        kwargs: dict[str, Any] = {
            "region_name": self._region_name,
            "config": Config(
                retries={"max_attempts": 2, "mode": "standard"},
                connect_timeout=2,
                read_timeout=self._read_timeout,
            ),
        }
        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url
        if self._access_key:
            kwargs["aws_access_key_id"] = self._access_key
        if self._secret_key:
            kwargs["aws_secret_access_key"] = self._secret_key
        if self._session_token:
            kwargs["aws_session_token"] = self._session_token
        self._context = self._session.client("sqs", **kwargs)
        self._client = await self._context.__aenter__()

    async def close(self) -> None:
        context = self._context
        self._client = None
        self._context = None
        self._urls.clear()
        if context is not None:
            await context.__aexit__(None, None, None)

    async def ensure_queue(self, name: str) -> str:
        """
        Create ``name`` if it does not exist and return its queue URL.

        A name that ends with ``.fifo`` is created as a FIFO queue.
        """

        cached = self._urls.get(name)
        if cached is not None:
            return cached
        client = self._require()
        attributes: dict[str, str] = {}
        if name.endswith(".fifo"):
            attributes = {
                "FifoQueue": "true",
                "ContentBasedDeduplication": "false",
            }
        try:
            if attributes:
                response = await client.create_queue(QueueName=name, Attributes=attributes)
            else:
                response = await client.create_queue(QueueName=name)
        except ClientError as exc:
            if _error_code(exc) not in _ALREADY_EXISTS:
                raise TransportError(f"SQS queue {name!r} was rejected: {_error_message(exc)}") from exc
            try:
                response = await client.get_queue_url(QueueName=name)
            except ClientError as lookup_error:
                raise TransportError(
                    f"SQS queue {name!r} was rejected: {_error_message(lookup_error)}",
                ) from lookup_error
        queue_url = self._align(str(response["QueueUrl"]))
        self._urls[name] = queue_url
        return queue_url

    async def has_redrive_policy(self, queue_url: str) -> bool:
        client = self._require()
        try:
            response = await client.get_queue_attributes(
                QueueUrl=queue_url,
                AttributeNames=["RedrivePolicy"],
            )
        except ClientError as exc:
            if _error_code(exc) in {"InvalidAttributeName"}:
                return False
            raise TransportError(
                f"SQS queue attributes could not be read: {_error_message(exc)}",
            ) from exc
        attributes = response.get("Attributes") or {}
        return "RedrivePolicy" in attributes

    async def send(
        self,
        queue_url: str,
        *,
        body: bytes,
        headers: Mapping[str, str],
        content_type: str | None,
        key: str | None,
        preserved_attempt: int | None,
        fifo: bool,
    ) -> None:
        client = self._require()
        attributes = _message_attributes(
            headers=headers,
            content_type=content_type,
            key=key,
            preserved_attempt=preserved_attempt,
        )
        encoded = base64.b64encode(body).decode("ascii")
        # SQS rejects an empty body. A single space stands in for empty bytes.
        if encoded == "":
            encoded = " "
            attributes[_ATTR_BODY_ENCODING] = _string_attribute("empty")
        params: dict[str, Any] = {
            "QueueUrl": queue_url,
            "MessageBody": encoded,
        }
        if attributes:
            params["MessageAttributes"] = attributes
        if fifo:
            params["MessageGroupId"] = key if key else _UNKEYED_GROUP
            params["MessageDeduplicationId"] = _deduplication_id()
        try:
            await client.send_message(**params)
        except ClientError as exc:
            raise TransportError(f"SQS send failed: {_error_message(exc)}") from exc

    async def receive(
        self,
        queue_url: str,
        *,
        max_messages: int,
        wait_time_seconds: int,
        visibility_timeout: int,
    ) -> list[SQSDelivery]:
        client = self._require()
        try:
            response = await client.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=max(1, min(10, max_messages)),
                WaitTimeSeconds=max(0, min(20, wait_time_seconds)),
                VisibilityTimeout=visibility_timeout,
                MessageAttributeNames=["All"],
                MessageSystemAttributeNames=[
                    "ApproximateReceiveCount",
                    "SentTimestamp",
                ],
            )
        except ClientError as exc:
            raise TransportError(f"SQS receive failed: {_error_message(exc)}") from exc
        messages = response.get("Messages") or []
        return [_delivery(message) for message in messages]

    async def delete(self, queue_url: str, receipt_handle: str) -> None:
        client = self._require()
        try:
            await client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        except ClientError as exc:
            code = _error_code(exc)
            if code in {"ReceiptHandleIsInvalid", "InvalidParameterValue"}:
                return
            raise TransportError(f"SQS delete failed: {_error_message(exc)}") from exc

    async def change_visibility(self, queue_url: str, receipt_handle: str, timeout: int) -> None:
        client = self._require()
        try:
            await client.change_message_visibility(
                QueueUrl=queue_url,
                ReceiptHandle=receipt_handle,
                VisibilityTimeout=max(0, min(43200, timeout)),
            )
        except ClientError as exc:
            code = _error_code(exc)
            if code in {"ReceiptHandleIsInvalid", "InvalidParameterValue", "MessageNotInflight"}:
                return
            raise TransportError(f"SQS visibility change failed: {_error_message(exc)}") from exc

    def _require(self) -> Any:
        if self._client is None:
            raise TransportError("SQS client is not open")
        return self._client

    def _align(self, queue_url: str) -> str:
        if not self._endpoint_url:
            return queue_url
        endpoint = urllib.parse.urlparse(self._endpoint_url)
        parsed = urllib.parse.urlparse(queue_url)
        if not endpoint.netloc:
            return queue_url
        return urllib.parse.urlunparse(
            (endpoint.scheme, endpoint.netloc, parsed.path, "", parsed.query, ""),
        )


def _message_attributes(
    *,
    headers: Mapping[str, str],
    content_type: str | None,
    key: str | None,
    preserved_attempt: int | None,
) -> dict[str, dict[str, str]]:
    attributes: dict[str, dict[str, str]] = {
        _ATTR_BODY_ENCODING: _string_attribute(_ENCODING_BASE64),
    }
    if content_type:
        attributes[_ATTR_CONTENT_TYPE] = _string_attribute(content_type)
    if key:
        attributes[_ATTR_KEY] = _string_attribute(key)
    if headers:
        attributes[_ATTR_HEADERS] = _string_attribute(
            json.dumps({str(name): str(value) for name, value in headers.items()}, sort_keys=True),
        )
    if preserved_attempt is not None:
        attributes[_ATTR_ATTEMPT] = _string_attribute(str(preserved_attempt))
    return attributes


def _string_attribute(value: str) -> dict[str, str]:
    return {"DataType": "String", "StringValue": value}


def _delivery(message: Mapping[str, Any]) -> SQSDelivery:
    attributes = message.get("MessageAttributes") or {}
    system = message.get("Attributes") or message.get("MessageSystemAttributes") or {}
    encoding = _attribute(attributes, _ATTR_BODY_ENCODING)
    raw_body = message.get("Body") or ""
    if not isinstance(raw_body, str):
        raw_body = str(raw_body)
    body = _decode_body(raw_body, encoding)
    headers = _headers(_attribute(attributes, _ATTR_HEADERS))
    content_type = _attribute(attributes, _ATTR_CONTENT_TYPE)
    key = _attribute(attributes, _ATTR_KEY)
    preserved = _attribute(attributes, _ATTR_ATTEMPT)
    attempt: int | None = None
    if preserved:
        try:
            attempt = int(preserved)
        except ValueError:
            attempt = None
    try:
        receive_count = int(system.get("ApproximateReceiveCount", "1"))
    except (TypeError, ValueError):
        receive_count = 1
    try:
        sent_timestamp = int(system.get("SentTimestamp", "0"))
    except (TypeError, ValueError):
        sent_timestamp = 0
    message_id = message.get("MessageId") or ""
    receipt = message.get("ReceiptHandle") or ""
    return SQSDelivery(
        message_id=str(message_id),
        receipt_handle=str(receipt),
        body=body,
        headers=headers,
        content_type=content_type,
        key=key,
        receive_count=max(1, receive_count),
        preserved_attempt=attempt,
        sent_timestamp=sent_timestamp,
    )


def _attribute(attributes: Mapping[str, Any], name: str) -> str | None:
    entry = attributes.get(name)
    if not isinstance(entry, Mapping):
        return None
    value = entry.get("StringValue")
    if value is None:
        return None
    text = str(value)
    if text == "":
        return None
    return text


def _headers(payload: str | None) -> dict[str, str]:
    if not payload:
        return {}
    try:
        loaded = json.loads(payload)
    except json.JSONDecodeError:
        logger.exception("sqs header attribute is not json")
        return {}
    if not isinstance(loaded, dict):
        return {}
    return {str(name): str(value) for name, value in loaded.items()}


def _decode_body(raw_body: str, encoding: str | None) -> bytes:
    if encoding == "empty":
        return b""
    if encoding == _ENCODING_BASE64:
        try:
            return base64.b64decode(raw_body, validate=True)
        except (ValueError, TypeError):
            logger.exception("sqs body was marked base64 but did not decode")
    return raw_body.encode("utf-8")


def _deduplication_id() -> str:
    return uuid.uuid4().hex


def _error_code(exc: ClientError) -> str:
    error = exc.response.get("Error", {})
    if not isinstance(error, dict):
        return ""
    return str(error.get("Code", ""))


def _error_message(exc: ClientError) -> str:
    error = exc.response.get("Error", {})
    if not isinstance(error, dict):
        return str(exc)
    message = error.get("Message") or error.get("Code") or exc
    return str(message)


__all__ = (
    "SQSClient",
    "SQSDelivery",
)
