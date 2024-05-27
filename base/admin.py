from django.contrib import admin
from django.core.paginator import Paginator
from django.db import connection


class LargeTablePaginator(Paginator):
    """ Warning: Postgresql only hack
    Overrides the count method of QuerySet objects to get an estimate instead of actual count when not filtered.
    However, this estimate can be stale and hence not fit for situations where the count of objects actually matter.
    """

    def _get_count(self):
        if getattr(self, '_count', None) is not None:
            return self._count  # pylint: disable=E0203

        query = self.object_list.query
        if not query.where:
            try:
                cursor = connection.cursor()
                cursor.execute('SELECT reltuples FROM pg_class WHERE relname = %s',
                               [query.model._meta.db_table])  # noqa
                self._count = int(cursor.fetchone()[0])  # pylint: disable=W0201
            except:  # noqa
                self._count = super()._get_count()  # noqa
        else:
            self._count = super().count  # pylint: disable=W0201

        return self._count

    count = property(_get_count)


class BaseAdmin(admin.ModelAdmin):
    show_full_result_count = False
    paginator = LargeTablePaginator
    list_per_page = 20
