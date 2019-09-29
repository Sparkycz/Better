
import logging

from elasticsearch import Elasticsearch

logger = logging.getLogger(__name__)


def get_argparse_options():
    """Returns kwargs for argument parser constructor.

    Returns:
        dict: Arguments for arg. parser.
    """
    return {
        'description': "Job to fix matches in ES.",
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
    def __init__(self, config, es: Elasticsearch):
        self.config = config
        self.es = es

    def run(self):
        logger.info('You must know what you do!')
        exit()

        candidates = self._load_candidates()

        for original in candidates:
            print('--------')
            doc = original['_source']
            doc['datetime'] = doc.get('datetime', doc.get('date'))
            c2 = self._load_pair_candidate(doc['sport_name'], doc['datetime'], doc['team1'], doc['team2'])
            c2_doc = c2.get('_source', {})

            if c2 and c2_doc['team1'] == doc['team1'].strip() and c2_doc['team2'] == doc['team2'].strip():

                body = c2_doc
                body['bets'] = doc['bets']

                self.es.index('matches', id=c2['_id'], body=body)
                self.es.delete('matches', original['_id'])

                print(c2['_id'], original['_id'])

    def _load_pair_candidate(self, sport, date, team1, team2):
        query = {
            "query": {
                "bool": {
                    "must_not": [
                        {
                            "exists": {"field": "bets.bet_info"}
                        }
                    ],
                    "must": [
                        {
                            "match": {
                                "team1": team1
                            }
                        },
                        {
                            "match": {
                                "team2": team2
                            }
                        },
                        {
                            "match": {
                                "date": date
                            }
                        },
                        {
                            "match": {
                                "sport_name": sport
                            }
                        }
                    ]
                }
            }
        }
        try:
            return self.es.search('matches', query)['hits']['hits'][0]
        except IndexError:
            return {}

    def _load_candidates(self):
        query = {
            "query": {
                "bool": {
                    "must_not": [
                        {"exists" : { "field" : "team1_points" }}],
                    "filter":
                        {
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
            "size": 1800
        }

        return self.es.search('matches', query)['hits']['hits']
