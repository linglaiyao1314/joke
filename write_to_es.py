# coding=utf-8
from elasticsearch import Elasticsearch
from elasticsearch import helpers


def read_from_mongo(db, collection, query, host='localhost', port=27017, username=None, password=None):
    from pymongo import MongoClient
    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)

    db = conn[db]
    cursor = db[collection].find(query)
    return cursor


def bulk_to_elasticsearch(datas, hosts=None):
    if hosts is None:
        es = Elasticsearch(["localhost:9200"])
    else:
        es = Elasticsearch(hosts)
    return helpers.bulk(es, datas)


if __name__ == '__main__':
    documents = []
    for index, i in enumerate(read_from_mongo("joke", "duanzi", {})):
        _id = str(i.pop("_id"))
        document = {"_index": "joke", "_type": "duanzi", "_id": _id, "_source": i}
        documents.append(document)
        if len(documents) >= 1000:
            bulk_to_elasticsearch(documents)
            print("finish bulk", index + 1, " items")
            documents = []
    if documents:
        bulk_to_elasticsearch(documents)
        print("finish bulk all items")
