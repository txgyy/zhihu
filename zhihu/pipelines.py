# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import pymysql
from .items import userItem, guanxiItem


class ZhihuPipeline(object):
    def __init__(self, mysql_config):
        self.mysql_config = mysql_config

    def process_item(self, item, spider):
        if isinstance(item, userItem):
            sql = """REPLACE INTO {}({}) VALUES ({});""".format('user', ','.join(item._values.keys()), ','.join(['%s'] * len(item)))
        elif isinstance(item, guanxiItem):
            sql = """REPLACE INTO {}({}) VALUES ({});""".format('guanxi', ','.join(item._values.keys()), ','.join(['%s'] * len(item)))
        try:
            self.zhihu_cur.executemany(sql.format(*item._values.keys()), zip(*item._values.values(), ))
            self.zhihu_con.commit()
        except Exception as e:
            spider.logger.error(e)
            self.zhihu_con.rollback()
            spider.logger.error(item)
        return item

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mysql_config=crawler.settings.get('MYSQL_CONFIG')
        )

    def open_spider(self, spider):
        self.zhihu_con = pymysql.connect(db='zhihu', **self.mysql_config)
        self.zhihu_cur = self.zhihu_con.cursor()
        self.create_tables(spider)

    def close_spider(self, spider):
        self.zhihu_cur.close()
        self.zhihu_con.close()

    def create_tables(self, spider):
        guanxi_sql = '''
            -- auto-generated definition
            CREATE TABLE IF NOT EXISTS guanxi
            (
              auto_id             INT AUTO_INCREMENT
                PRIMARY KEY,
              s_url_token    CHAR(70) NOT NULL,
              followee_count MEDIUMINT NOT NULL,
              url_token      CHAR(70) NOT NULL,
              CONSTRAINT guanxi_s_url_token_url_token_uindex
              UNIQUE (s_url_token, url_token)
            )
              ENGINE = InnoDB;
        '''
        user_sql = '''
            -- auto-generated definition
            CREATE TABLE IF NOT EXISTS user
            (
              auto_id             INT AUTO_INCREMENT
                PRIMARY KEY,
              url_token      CHAR(70)    NOT NULL,
              name           CHAR(40) NOT NULL,
              gender         TINYINT NOT NULL,
              headline       CHAR(160) NOT NULL,
              follower_count MEDIUMINT NOT NULL,
              answer_count   MEDIUMINT NOT NULL,
              articles_count MEDIUMINT NOT NULL,
              user_type ENUM('people','organization','guest') NOT NULL,
              id        CHAR(32) NOT NULL,
              is_advertiser  TINYINT(1) NOT NULL,
              is_org         TINYINT(1) NOT NULL,
              CONSTRAINT user_url_token_uindex
              UNIQUE (url_token)
            )
              ENGINE = InnoDB;
        '''
        urls_sql = """
            -- auto-generated definition
            CREATE TABLE IF NOT EXISTS urls
            (
              auto_id        INT AUTO_INCREMENT
                PRIMARY KEY,
              status    ENUM('fail','success','deny') NOT NULL,
              url_token CHAR(70)   NOT NULL,
              offset    SMALLINT(6) NOT NULL,
              CONSTRAINT urls_url_token_offset_uindex
              UNIQUE (url_token, offset)
            )
              ENGINE = InnoDB;
        """
        try:
            self.zhihu_cur.execute(guanxi_sql)
            self.zhihu_cur.execute(user_sql)
            self.zhihu_cur.execute(urls_sql)
            self.zhihu_con.commit()
        except Exception as e:
            spider.logger.error(e)
            self.zhihu_con.rollback()
