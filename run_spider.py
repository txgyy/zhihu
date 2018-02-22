#!/usr/bin/env python
# -*- coding: utf-8 -*-

from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from zhihu.spiders.followees import FollowingSpider

if __name__ == '__main__':
    process = CrawlerProcess(get_project_settings())
    process.crawl(FollowingSpider)
    process.start()