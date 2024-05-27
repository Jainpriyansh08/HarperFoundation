from json.decoder import JSONDecodeError

from requests import Response
from rest_framework import status


class APIException(Exception):
    extra = None
    error_code = None
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    message = 'The server encounters an unexpected exception while trying to process the request.'
    errors = []
    rollback_transaction = True

    def __init__(self, message: str = None, status_code: int = None) -> None:
        if message:
            self.message = message
        if status_code:
            self.status_code = status_code
        super().__init__()


class HttpException(APIException):

    def __init__(self, response: Response) -> None:
        try:
            error_message = response.json()
        except JSONDecodeError:
            error_message = response.content

        self.str = 'status: {} - message: {}'.format(response.status_code, error_message)

        super().__init__()

    def __str__(self):
        return self.str  # pragma: no cover
