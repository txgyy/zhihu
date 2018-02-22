#!/usr/bin/env python
# -*- coding: utf-8 -*-
from scrapy.dupefilters import RFPDupeFilter
import weakref
import hashlib
from scrapy.utils.python import to_bytes

_fingerprint_cache = weakref.WeakKeyDictionary()


class ZhihuRFPDupeFilter(RFPDupeFilter):

    def request_fingerprint(self, request):
        cache = _fingerprint_cache.setdefault(request, {})
        fp = hashlib.sha1()
        fp.update(to_bytes(request.meta.get('s_url_token') + str(request.meta.get('offset'))))
        cache[None] = fp.hexdigest()
        return cache[None]
