# -*- coding: utf-8 -*-
import pymongo
import elasticsearch
from elasticsearch import helpers

class JokePipeline(object):
    def process_item(self, item, spider):
        return item


class MongoPipeline(object):

    def __init__(self, mongo_uri, mongo_db):
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            mongo_uri=crawler.settings.get('MONGO_URI'),
            mongo_db=crawler.settings.get('MONGO_DATABASE', 'joke')
        )

    def open_spider(self, spider):
        self.client = pymongo.MongoClient(self.mongo_uri)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        item["joke_id"] = spider.name + "-" + item["joke_id"]
        item["popular_value"] = int(item["popular_value"])
        if self.db["duanzi"].find_one({"joke_id": item["joke_id"]}):
            return item
        else:
            insert_item = {key: value for key, value in item.items() if value}
            self.db["duanzi"].insert(insert_item)
        return item


class ElasticsearchPipeline(object):
    def __init__(self, hosts):
        self.es = elasticsearch.Elasticsearch(hosts=hosts)
        self.es.cluster.health(wait_for_status='yellow')

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            hosts=crawler.settings.get('HOSTS', "localhost:9200"),
        )

    def open_spider(self, spider):
        self.documents = []

    def close_spider(self, spider):
        if self.documents:
            helpers.bulk(self.es, self.documents)
        del self.documents

    def process_item(self, item, spider):
        es_item = dict(item)
        document = {"_index": "joke", "_type": "duanzi", "_id": es_item.pop("joke_id"),
                    "_source": es_item}
        self.documents.append(document)
        if len(self.documents) >= 1000:
            helpers.bulk(self.es, self.documents)
            self.documents = list()
        return item
