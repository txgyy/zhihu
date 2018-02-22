# -*- coding: utf-8 -*-
import scrapy
from zhihu.items import userItem, guanxiItem
from scrapy.loader import ItemLoader
import pymysql
from scrapy.utils.project import get_project_settings
from scrapy.selector import Selector


class FollowingSpider(scrapy.Spider):
    name = 'followees'
    allowed_domains = ['www.zhihu.com']
    custom_settings = {
        'LOG_FILE': 'followees.log',
        # 'JOBDIR': 'crawls/followees'
    }
    start_url = 'https://www.zhihu.com/api/v4/members/{url_token:s}/followees?include=data%5B*%5D.answer_count%2Carticles_count%2Cgender%2Cfollower_count&offset={offset:d}&limit=20'

    def __init__(self, *args, **kwargs):
        super(FollowingSpider, self).__init__(*args, **kwargs)
        self.zhihu_con = pymysql.connect(db='zhihu', **get_project_settings().get('MYSQL_CONFIG'))
        self.zhihu_cur = self.zhihu_con.cursor()
        fetch_sql = 'SELECT url_token FROM user;'
        self.url_tokens = {url_token for url_token, in self.sql_fetch(fetch_sql)}

    def closed(self, reason):
        self.zhihu_cur.close()
        self.zhihu_con.close()

    def start_requests(self):
        fetch_sql = "SELECT url_token,offset FROM urls WHERE status=%s;"
        for url_token, offset in self.sql_fetch(fetch_sql, 'fail') or (('tian-xin-68-67', 0),):
            yield scrapy.Request(
                url=self.start_url.format(url_token=url_token, offset=offset),
                meta={
                    's_url_token': url_token,
                    'url_retry': False if url_token == 'tian-xin-68-67' else True,
                    'offset': offset,
                },
                dont_filter=True,
                callback=self.parse
            )

    def parse(self, response):
        guanxi = ItemLoader(item=guanxiItem())
        user = ItemLoader(item=userItem())
        contents = response.meta.get('content')
        followee_count = contents['paging']['totals']
        s_url_token = response.meta['s_url_token']

        for followee in contents['data']:
            url_token = followee.get('url_token')
            guanxi.add_value('s_url_token', s_url_token)
            guanxi.add_value('followee_count', followee_count)
            guanxi.add_value('url_token', url_token)

            if not self.url_tokens.issuperset((url_token,)):
                self.url_tokens.add(url_token)
                for key in userItem.__dict__['fields'].keys():
                    if key == 'headline':
                        followee['headline'] = Selector(text=followee.get(key)).xpath('string(.)').extract_first() or ''
                    user.add_value(key, followee.get(key))

            if followee.get('follower_count') > 1000:
                url = self.start_url.format(url_token=url_token, offset=0)
                yield scrapy.Request(
                    url=url,
                    meta={
                        's_url_token': url_token,
                        'offset': 0,
                    },
                    dont_filter=True,
                    callback=self.parse)
        yield guanxi.load_item()
        yield user.load_item()

        if followee_count > 20 and (response.meta.get('offset') == 0 or response.meta.get('url_retry')):
            for offset in range(20, followee_count, 20):
                url = self.start_url.format(url_token=s_url_token, offset=offset)
                yield scrapy.Request(
                    url=url,
                    meta={
                        's_url_token': s_url_token,
                        'offset': offset,
                    },
                    dont_filter=True,
                    callback=self.parse
                )

    def sql_fetch(self, fetch_sql, *args):
        try:
            self.zhihu_cur.execute(fetch_sql, args)
            return set(self.zhihu_cur.fetchall())
        except Exception as e:
            self.logger.error(e)
            return set()
