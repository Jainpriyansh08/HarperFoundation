from django.http import HttpResponse

from base.views import ApplicationBaseAPIView


class HealthCheckView(ApplicationBaseAPIView):
    permission_classes = []

    def get(self, request, *args, **kwargs):
        return HttpResponse('ok')
