import logging

from cqrs.middlewares import base
from cqrs.middlewares.base import HandleType
from cqrs.models.request import IRequest
from cqrs.models.response import IResponse

logger = logging.getLogger("cqrs")


class LoggingMiddleware(base.Middleware):
    async def __call__(self, request: IRequest, handle: HandleType) -> IResponse | None:
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
