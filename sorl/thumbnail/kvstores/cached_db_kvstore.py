from django.core.cache import cache
from sorl.thumbnail.kvstores.base import KVStoreBase
from sorl.thumbnail.conf import settings
from sorl.thumbnail.models import KVStore as KVStoreModel


class EMPTY_VALUE(object):
    pass


class KVStore(KVStoreBase):
    def clear(self):
        """
        We can clear the database more efficiently using the prefix here rather
        than calling :meth:`_delete_raw`.
        """
        prefix = settings.THUMBNAIL_KEY_PREFIX
        for key in self._find_keys_raw(prefix):
            cache.delete(key)
        KVStoreModel.objects.filter(key__startswith=prefix).delete()

    def _get_raw(self, key):
        value = cache.get(key)
        if value is None:
            values = self._get_raw_multiple([key])
            assert len(values) <= 1

            if len(values) == 0 or values[0].value is None:
                value = EMPTY_VALUE
            else:
                value = values[0].value

            cache.set(key, value, settings.THUMBNAIL_CACHE_TIMEOUT)
        if value == EMPTY_VALUE:
            return None
        return value

    def _get_raw_multiple(self, keys):
        try:
            value = KVStoreModel.objects.filter(key__in=keys)
        except KVStoreModel.DoesNotExist:
            # we set the cache to prevent further db lookups
            value = EMPTY_VALUE
        if value == EMPTY_VALUE:
            return None
        return value

    def _set_raw(self, key, value):
        kv = KVStoreModel.objects.get_or_create(key=key)[0]
        kv.value = value
        kv.save()
        cache.set(key, value, settings.THUMBNAIL_CACHE_TIMEOUT)

    def _delete_raw(self, *keys):
        KVStoreModel.objects.filter(key__in=keys).delete()
        for key in keys:
            cache.delete(key)

    def _find_keys_raw(self, prefix):
        qs = KVStoreModel.objects.filter(key__startswith=prefix)
        return qs.values_list('key', flat=True)

