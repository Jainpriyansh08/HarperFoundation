import logging
from typing import Dict

from django.db import transaction, connection
from django.http import Http404
from django.utils.decorators import classonlymethod
from rest_framework import generics
from rest_framework import status as http_status
from rest_framework.response import Response
from rest_framework.viewsets import ViewSetMixin

DJANGO_DB_BACKENDS_LOGGER = logging.getLogger('django.db.backends')


class ApplicationBaseAPIView(generics.GenericAPIView):
    lookup_field = 'id'
    ATOMIC_METHODS = ('post', 'patch', 'put', 'delete')
    request_serializer_class = None
    response_serializer_class = None

    def get_validated_data(self, instance=None):
        serializer = self.get_request_serializer(data=self.request.data, instance=instance)
        serializer.is_valid(raise_exception=True)
        return serializer.validated_data

    def get_serializer_context(self) -> dict:
        context = super().get_serializer_context()
        context['user'] = self.request.user
        return context

    def get_object_or_none(self):
        try:
            return self.get_object()
        except Http404:
            pass

    def get_handler(self, request):
        # Get the appropriate handler method
        if request.method.lower() in self.http_method_names:
            handler = getattr(self, request.method.lower(),
                              self.http_method_not_allowed)
        else:
            handler = self.http_method_not_allowed

        return handler

    # pylint: disable=W0201, W0703
    def _dispatch(self, request, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        request = self.initialize_request(request, *args, **kwargs)
        self.request = request
        self.headers = self.default_response_headers  # deprecate?

        try:
            self.initial(request, *args, **kwargs)
            handler = self.get_handler(request)
            response = handler(request, *args, **kwargs)

        except Exception as exc:
            response = self.handle_exception(exc)

        self.response = self.finalize_response(request, response, *args, **kwargs)
        return self.response

    def dispatch(self, request, *args, **kwargs):
        if request.method.lower() not in self.ATOMIC_METHODS:
            return self._dispatch(request, *args, **kwargs)

        with transaction.atomic():
            DJANGO_DB_BACKENDS_LOGGER.debug('BEGIN')

            response = self._dispatch(request, *args, **kwargs)

            if transaction.get_rollback():
                DJANGO_DB_BACKENDS_LOGGER.debug('ROLLBACK')
            else:
                DJANGO_DB_BACKENDS_LOGGER.debug('COMMIT')

            return response

    def handle_exception(self, exc):
        response = super().handle_exception(exc)

        if getattr(response, 'exception') and connection.in_atomic_block:
            rollback_transaction = getattr(exc, 'rollback_transaction', True)

            if rollback_transaction:
                transaction.set_rollback(True)

        return response

    def get_request_serializer_class(self):
        if self.request_serializer_class:
            return self.request_serializer_class
        return self.get_serializer_class()

    def get_response_serializer_class(self):
        if self.response_serializer_class:
            return self.response_serializer_class
        return self.get_serializer_class()

    def get_request_serializer(self, *args, **kwargs):
        serializer_class = self.get_request_serializer_class()
        kwargs['context'] = self.get_serializer_context()
        return serializer_class(*args, **kwargs)

    def get_response_serializer(self, *args, **kwargs):
        serializer_class = self.get_response_serializer_class()
        kwargs['context'] = self.get_serializer_context()
        return serializer_class(*args, **kwargs)

    # pylint: disable=R0913
    def get_response(self, obj=None, data=None, status: http_status = None, headers: Dict = None, many=False):
        if obj is not None and data is not None:  # pragma: no cover
            raise AssertionError('obj and data can not be passed together.')

        if data is None:
            if obj is None:
                status = status or http_status.HTTP_204_NO_CONTENT
                data = None
            else:
                serializer = self.get_response_serializer(instance=obj, many=many)
                data = serializer.data
        else:
            status = status or http_status.HTTP_200_OK

        return Response(data=data, status=status, headers=headers)


class ApplicationBaseViewSet(ViewSetMixin, ApplicationBaseAPIView):
    action_to_request_serializer_class = {}
    action_to_response_serializer_class = {}

    def get_request_serializer_class(self):
        if self.action in self.action_to_request_serializer_class:
            return self.action_to_request_serializer_class[self.action]
        return super().get_request_serializer_class()

    def get_response_serializer_class(self):
        if self.action in self.action_to_response_serializer_class:
            return self.action_to_response_serializer_class[self.action]
        return super().get_response_serializer_class()

    def get_queryset(self):
        queryset_method = 'get_{action}_queryset'.format(action=self.action)

        if hasattr(self, queryset_method):
            return getattr(self, queryset_method)(self.request.user)

        return super().get_queryset()


class SingleResourceViewSet(ApplicationBaseViewSet):
    METHOD_TO_ACTION = {
        'retrieve': 'get',
        'partial_update': 'patch',
        'destroy': 'delete',
        'update': 'put',
        'post': 'create'
    }

    @classonlymethod
    def as_view(cls, actions=None, **initkwargs):
        class_methods = set(dir(cls))
        if not actions:
            actions = {}
            for action, method in cls.METHOD_TO_ACTION.items():
                if action in class_methods:
                    actions[method] = action

        return super().as_view(actions, **initkwargs)  # noqa
