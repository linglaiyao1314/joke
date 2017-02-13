# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class JokeItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    joke_id = scrapy.Field()
    content = scrapy.Field()
    popular_value = scrapy.Field()


class JianDanItem(JokeItem):
    """http://jandan.net/"""
    author = scrapy.Field()

