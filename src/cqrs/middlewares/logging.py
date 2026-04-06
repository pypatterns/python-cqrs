import logging
import typing

from cqrs.middlewares import base
from cqrs.middlewares.base import HandleType
from cqrs.requests.request import IRequest
from cqrs.response import IResponse

logger = logging.getLogger("cqrs")


class LoggingMiddleware(base.Middleware):
    async def __call__(self, request: IRequest, handle: HandleType) -> typing.Optional[IResponse]:
        logger.debug(
            "Handle %s request",
            type(request).__name__,
            extra={
                "request_json_fields": {"request": request.to_dict()},
                "to_mask": True,
            },
        )
        resp = await handle(request)
        resp_dict = {}
        if resp:
            resp_dict = resp.to_dict()
        logger.debug(
            "Request %s handled",
            type(request).__name__,
            extra={
                "request_json_fields": {
                    "response": resp_dict,
                },
                "to_mask": True,
            },
        )

        return resp
