
from elasticsearch import Elasticsearch


class ElasticsearchWrapper():
    def __init__(self, es: Elasticsearch, index_name):
        self.es = es
        self.index_name = index_name

    def search(self, query, *args, **kwargs):
        return self.es.search(self.index_name, query, *args, **kwargs)
