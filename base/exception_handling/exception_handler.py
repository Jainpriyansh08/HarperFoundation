import logging

from rest_framework.response import Response
from rest_framework.views import exception_handler as drf_exception_handler

import base

logger = logging.getLogger(__name__)


def _convert_to_response(exception: base.exceptions.APIException) -> Response:
    if exception.status_code >= 500:
        logger.exception(exception)

    data = {
        'error': True,
        'message': exception.message,
    }

    if exception.error_code:
        data['code'] = exception.error_code

    if exception.extra:
        data.update(exception.extra)

    return Response(
        data=data,
        status=exception.status_code
    )


def exception_handler(exc, context):
    if isinstance(exc, base.exceptions.APIException):
        return _convert_to_response(exc)

    return drf_exception_handler(exc, context)
