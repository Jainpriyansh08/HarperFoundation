import os

from rest_framework.utils.urls import replace_query_param


class PaginationUtils:
    def __init__(self, request):
        self.request = request

    def get_next_page_url(self, current_page: int = 0, limit: int = 10):
        next_page = current_page + 1
        url = self.request.build_absolute_uri()

        url = replace_query_param(url, 'limit', limit)
        url = replace_query_param(url, 'page', next_page)

        url_suffix = url.split(self.request.path)

        host = os.environ.get('HOST_URL', self.request.get_host())
        return host + self.request.path + url_suffix[-1]
