
import logging

from elasticsearch import Elasticsearch
import joblib
import pandas

from better.helpers import FeaturesBuilder

logger = logging.getLogger(__name__)

INDEX_NAME = 'matches'

ROW_COUNT_BY_SPORT = 50


def get_argparse_options():
    """Returns kwargs for argument parser constructor.

    Returns:
        dict: Arguments for arg. parser.
    """
    return {
        'description': "Job to predict 5 the best bets for today.",
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

        self.features_builder = FeaturesBuilder(self.es)

        _feature_names = ['doc_id', 'date', 'sport', 'team_1', 'team_2']
        _feature_names.extend(self.features_builder.get_feature_names())
        _feature_names.append('target')

        self.model = joblib.load(f'models/random_forest_classifier_{self.sport}.pkl')

    def run(self):
        features_list = []
        docs = []
        for doc_id, doc in self._get_candidates():
            docs.append(doc)
            features_list.append(self._load_features(doc_id, doc))

        predictions, probabilities = self._predict(features_list)

        df_data = []
        for i, doc in enumerate(docs):
            df_data.append([
                doc['datetime'], doc['team1'], doc['team2'],
                self.get_bet_odds_by_bet_type(doc, predictions[i]),
                predictions[i],
                probabilities[i],
            ])

        df = pandas.DataFrame(df_data, columns=['date', 'team1', 'team2', 'bet_odds', 'prediction', 'probability'])
        df = df.sort_values(by=['probability'], ascending=False)

        best_tips = df.head()  # the 5 most probability bets
        logger.info(f"\nBet tips of {self.sport} for you: \n" + str(best_tips))

    def _predict(self, features_list):
        predictions = self.model.predict(features_list)
        probabilities = []
        for __probabilities, __prediction in zip(self.model.predict_proba(features_list), predictions):
            probabilities.append(__probabilities[__prediction])

        return predictions, probabilities

    def _load_features(self, doc_id, doc):
        features = []
        for _f in self.features_builder.load_features(doc_id, doc):
            features.extend(_f)

        return features

    def _get_candidates(self):
        es_candidates = self._load_candidates_from_es()

        for doc in es_candidates:
            yield doc['_id'], doc['_source']

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
                                "datetime": {
                                    "gte": "now/d",
                                    "lte": "now+2d/d"
                                }
                            }
                        }
                    ],
                    "filter": {
                        "nested": {
                            "path": "bets",
                            "query": {
                                "bool": {
                                    "must": [
                                        { "exists": { "field": "bets.bet_info" }}
                                    ]
                                }
                            }
                        }
                    }
                }
            },
            "size": ROW_COUNT_BY_SPORT
        }
        es_result = self.es.search(INDEX_NAME, body=query)

        return es_result['hits']['hits']

    @staticmethod
    def get_bet_odds_by_bet_type(match, predicition_bet_type):
        predicition_bet_type = str(predicition_bet_type)
        for bet in match['bets']:
            if bet['bet_type'] == predicition_bet_type:
                return bet['bet']
