# -*- coding: utf-8 -*-

# Define here the models for your spider middleware
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/spider-middleware.html
from scrapy import signals
from fake_useragent import UserAgent
import random
import pymysql
from scrapy.spidermiddlewares.httperror import HttpError, IgnoreRequest
import re
import json
import brotli


class ZhihuSpiderMiddleware(object):
    # Not all methods need to be defined. If a method is not defined,
    # scrapy acts as if the spider middleware does not modify the
    # passed objects.
    def __init__(self, mysql_config):
        self.mysql_config = mysql_config

    @classmethod
    def from_crawler(cls, crawler):
        # This method is used by Scrapy to create your spiders.
        s = cls(
            mysql_config=crawler.settings.get('MYSQL_CONFIG')
        )
        crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(s.spider_closed, signal=signals.spider_closed)
        return s

    def spider_opened(self, spider):
        spider.logger.info('Spider opened: %s' % spider.name)
        self.zhihu_con = pymysql.connect(db='zhihu', **self.mysql_config)
        self.zhihu_cur = self.zhihu_con.cursor()
        self.pattern = re.compile(r'https://.*?members/(.*?)/followees.*?offset=(\d+)&limit=20')

    def spider_closed(self, spider):
        self.zhihu_cur.close()
        self.zhihu_con.close()

    def process_spider_input(self, response, spider):
        # Called for each response that goes through the spider
        # middleware and into the spider.
        # Should return None or raise an exception.
        if 200 <= response.status <= 300:
            self.update_url(spider, response)
            if response.headers.get('Content-Encoding') == 'br':
                response.meta['content'] = json.loads(brotli.decompress(response.body))
            else:
                response.meta['content'] = json.loads(response.body_as_unicode())
            if not response.meta.get('content')['data'][0].get('follower_count'):
                raise HttpError
            elif response.meta.get('content')['paging']['totals']:
                return
            else:
                raise Exception('Ignore Response')
        else:
            raise HttpError

    def process_spider_output(self, response, result, spider):
        # Called with the results returned from the Spider, after
        # it has processed the response.
        # Must return an iterable of Request, dict or Item objects.
        for i in result:
            yield i

    def process_spider_exception(self, response, exception, spider):
        # Called when a spider or process_spider_input() method
        # (from other spider middleware) raises an exception.

        # Should return either None or an iterable of Response, dict
        # or Item objects.
        if isinstance(exception, HttpError):
            if response.status == 401:
                self.update_url(spider, response, 'deny')
            else:
                self.update_url(spider, response, 'fail')
        elif isinstance(exception, Exception):
            spider.logger.info(
                "Ignoring response %(response)r: content is empty",
                {'response': response},
            )
            return []

    def process_start_requests(self, start_requests, spider):
        # Called with the start requests of the spider, and works
        # similarly to the process_spider_output() method, except
        # that it doesn’t have a response associated.

        # Must return only requests (not items).
        for r in start_requests:
            yield r

    def update_url(self, spider, http, status='success'):
        return update_url(self, spider, http, status)


class MyCustomDownloaderMiddleware():
    def __init__(self, mysql_config):
        self.mysql_config = mysql_config

    @classmethod
    def from_crawler(cls, crawler):
        o = cls(
            mysql_config=crawler.settings.get('MYSQL_CONFIG')
        )
        crawler.signals.connect(o.spider_opened, signal=signals.spider_opened)
        crawler.signals.connect(o.spider_closed, signal=signals.spider_closed)
        return o

    def spider_opened(self, spider):
        self.sql = "SELECT ip,port FROM httpbin"
        self.pattern = re.compile(r'https://.*?members/(.*?)/followees.*?offset=(\d+)&limit=20')
        self.err_proxy = set()
        self.proxy = set()
        self.useragent = UserAgent()

        self.proxy_con = pymysql.connect(db='ipproxy', **self.mysql_config)
        self.proxy_cur = self.proxy_con.cursor()
        self.zhihu_con = pymysql.connect(db='zhihu', **self.mysql_config)
        self.zhihu_cur = self.zhihu_con.cursor()
        self.ok_url_token_offset = set(self.get_status_urls(spider, 'success'))

    def spider_closed(self, spider):
        self.proxy_cur.close()
        self.proxy_con.close()
        self.zhihu_cur.close()
        self.zhihu_con.close()

    def process_request(self, request, spider):
        if self.ok_url_token_offset.issuperset((request.meta.get('s_url_token'), str(request.meta.get('offset')))):
            raise IgnoreRequest

        request.dont_filter = True
        headers = {
            'User-Agent': self.useragent.random
        }
        for k, v in headers.items():
            request.headers[k] = v
        if request.meta.get('s_url_token') == 'tian-xin-68-67':
            return
        self.change_proxy(request, spider)
        request.meta['times'] = request.meta.get('times', 0) + 1

    def process_response(self, request, response, spider):
        if 200 <= response.status <= 300:
            self.ok_url_token_offset.add((request.meta.get('s_url_token'), str(request.meta.get('offset'))))
        return response

    def process_exception(self, request, exception, spider):
        if isinstance(exception, IgnoreRequest):
            return
        self.err_proxy.add(request.meta['proxy'])
        self.change_proxy(request, spider)
        if request.meta['times'] == 3:
            self.update_url(spider, request, 'fail')
            return
        else:
            return request

    def change_proxy(self, request, spider):
        if self.proxy == self.err_proxy:
            self.err_proxy = set()
            try:
                self.proxy_cur.execute(self.sql)
                self.proxy = set(self.proxy_cur.fetchall())
            except Exception as e:
                spider.logger.error(e)
        request.meta['proxy'] = 'https://{}:{}'.format(*random.choice(list(self.proxy - self.err_proxy)))

    def update_url(self, spider, http, status='success'):
        return update_url(self, spider, http, status)

    def get_status_urls(self, spider, *status):
        sql = "SELECT url_token,offset FROM urls WHERE status=%s;"
        try:
            self.zhihu_cur.execute(sql, status)
            return set(self.zhihu_cur.fetchall())
        except Exception as e:
            spider.logger.error(e)


def update_url(self, spider, http, status='success'):
    url_token, offset = self.pattern.search(http.url).groups()
    insert_sql = "INSERT INTO urls (status, url_token,offset) VALUE (%s,%s,%s);"
    delete_sql = "DELETE FROM urls WHERE status=%s AND url_token=%s AND offset=%s;"
    try:
        if status == 'success':
            self.zhihu_cur.execute(delete_sql, ('fail', url_token, offset))
        if not http.meta.get('url_retry') or status == 'success':
            self.zhihu_cur.execute(insert_sql, (status, url_token, offset))
        self.zhihu_con.commit()
    except Exception as e:
        self.zhihu_con.rollback()
        spider.logger.error(e)
