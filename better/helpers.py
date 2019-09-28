
from elasticsearch import Elasticsearch

from better.features import bet_odds, previous_matches


class ElasticsearchWrapper():
    def __init__(self, es: Elasticsearch, index_name):
        self.es = es
        self.index_name = index_name

    def search(self, query, *args, **kwargs):
        return self.es.search(self.index_name, query, *args, **kwargs)


class FeaturesBuilder():
    FEATURE_LIST = [
        bet_odds.BetOdds,
        previous_matches.PreviousMatchesTogether,
    ]

    def __init__(self, es: Elasticsearch):
        self.es = es

    def get_feature_names(self):
        _feature_names = []
        for feature_class in self.FEATURE_LIST:
            _feature_names.extend(feature_class.NAMES)

        return _feature_names

    def load_features(self, doc_id, doc):
        _es = ElasticsearchWrapper(self.es, 'matches')

        for feature_class in self.FEATURE_LIST:
            feature_class_instance = feature_class(_es, doc_id, doc)
            yield feature_class_instance.get_features()
