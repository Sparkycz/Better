
import csv
import logging

from elasticsearch import Elasticsearch

from better.helpers import FeaturesBuilder

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

    for sport in config['SPORTS']:
        processor = Processor(config, es, sport)
        processor.run()

    logger.info("All done. Bye!")


class Processor():

    def __init__(self, config, es: Elasticsearch, sport):
        self.config = config
        self.es = es
        self.sport = sport

        self.csvfile = open(f'models/features_{self.sport}.csv', 'w')
        self.csvwriter = csv.writer(self.csvfile)

        self.features_builder = FeaturesBuilder(self.es)

        _feature_names = ['doc_id', 'date', 'sport', 'team_1', 'team_2']
        _feature_names.extend(self.features_builder.get_feature_names())
        _feature_names.extend(['target', 'class'])

        self.csvwriter.writerow(_feature_names)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.csvfile.close()

    def run(self):
        for doc_id, doc, target in self._get_candidates():

            logger.info('Calculating of {}, {}, {}, {}'.format(doc_id, doc['date'], doc['team1'], doc['team2']))

            features_list = [doc_id, doc['date'], doc['sport_name'], doc['team1'], doc['team2']]
            for features in self.features_builder.load_features(doc_id, doc):
                features_list.extend(features)

            features_list.append(target)
            if target == "1": class_ = "vyhra_domaci"
            elif target == "2": class_ = "vyhra_hoste"
            else: class_ = "remiza"
            features_list.append(class_)

            self.csvwriter.writerow(features_list)

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
                "bool": {
                    "must": [
                        {
                            "term": {
                                "sport_name": {
                                    "value": self.sport
                                }
                            }
                        },
                        {
                            "range": {
                                "date": {
                                    "lte": "now-1d/d"
                                }
                            }
                        }
                    ],
                    "filter": {
                        "exists": {"field": "correct_bets"}
                    }
                }
            },
            "sort": [
                {"date": {"order": "desc"}}
            ],
            "size": ROW_COUNT
        }
        es_result = self.es.search(INDEX_NAME, body=query)

        return es_result['hits']['hits']
