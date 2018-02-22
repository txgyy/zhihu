# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy

class userItem(scrapy.Item):
    user_type = scrapy.Field()
    answer_count = scrapy.Field()
    url_token = scrapy.Field()
    id = scrapy.Field()
    articles_count = scrapy.Field()
    name = scrapy.Field()
    headline = scrapy.Field()
    gender = scrapy.Field()
    is_advertiser = scrapy.Field()
    is_org = scrapy.Field()
    follower_count = scrapy.Field()

class guanxiItem(scrapy.Item):
    s_url_token = scrapy.Field()
    followee_count = scrapy.Field()
    url_token = scrapy.Field()

class problem_answersItem(scrapy.Item):
    content = scrapy.Field()

class topicsItem(scrapy.Item):
    offset = scrapy.Field()
    problem = scrapy.Field()