
import csv
import logging

from elasticsearch import Elasticsearch

from better.features import bet_odds, score, previous_matches
from better.helpers import ElasticsearchWrapper

logger = logging.getLogger(__name__)

INDEX_NAME = 'matches'

ROW_COUNT = 1000


def get_argparse_options():
    """Returns kwargs for argument parser constructor.

    Returns:
        dict: Arguments for arg. parser.
    """
    return {
        'description': "Job to calculate dataset of features.",
    }


def set_arguments(parser):
    """Adds command line arguments to the task.

    Args:
        parser (argparse.ArgumentParser): Arg. parser to setup.
    """
    pass


def execute(config, options):
    """Main task function to be executed via launcher."""

    es_host = config['ELASTICSEARCH_HOST']

    es = Elasticsearch(es_host)

    processor = Processor(config, es)
    processor.run()

    logger.info("All done. Bye!")


class Processor():

    FEATURE_LIST = [
        bet_odds.BetOdds,
        score.Score,
        previous_matches.PreviousMatchesTogether,
    ]

    def __init__(self, config, es: Elasticsearch):
        self.config = config
        self.es = es

        self.csvfile = open('models/features.csv', 'w')
        self.csvwriter = csv.writer(self.csvfile)

        _feature_names = ['doc_id', 'date', 'sport', 'team_1', 'team_2']
        for feature_class in self.FEATURE_LIST:
            _feature_names.extend(feature_class.NAMES)

        _feature_names.append('target')

        self.csvwriter.writerow(_feature_names)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.csvfile.close()

    def run(self):
        for doc_id, doc, target in self._get_candidates():

            logger.info('Calculating of {}, {}, {}, {}'.format(doc_id, doc['date'], doc['team1'], doc['team2']))

            features_list = [doc_id, doc['date'], doc['sport_name'], doc['team1'], doc['team2']]
            for features in self._load_features(doc_id, doc):
                features_list.extend(features)

            features_list.append("class_" + target)

            self.csvwriter.writerow(features_list)

    def _load_features(self, doc_id, doc):
        _es = ElasticsearchWrapper(self.es, INDEX_NAME)

        for feature_class in self.FEATURE_LIST:
            feature_class_instance = feature_class(_es, doc_id, doc)
            yield feature_class_instance.get_features()

    def _get_candidates(self):
        es_candidates = self._load_candidates_from_es()

        for doc in es_candidates:
            allowed_targets = ["0", "1", "2"]
            target = "0"
            for t in allowed_targets:
                if t in doc['_source']['correct_bets']:
                    target = t
                    break

            yield doc['_id'], doc['_source'], target

    def _load_candidates_from_es(self):
        query = {
            "query": {
                "exists" : { "field" : "correct_bets" }
            },
            "size": ROW_COUNT
        }
        es_result = self.es.search(INDEX_NAME, body=query)

        return es_result['hits']['hits']



