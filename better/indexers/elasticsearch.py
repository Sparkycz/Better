
import abc
import logging
import hashlib
from pathlib import Path

from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk

from better.indexers import Indexer


logger = logging.getLogger(__name__)

INDEX_NAME = 'matches'


class ESIndexer(Indexer):
    __metaclass__ = abc.ABCMeta

    def __init__(self, es: Elasticsearch):
        super().__init__()

        self.es = es

        self.check_index()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.es.indices.refresh(INDEX_NAME)
        self.es.indices.forcemerge(INDEX_NAME)

    @abc.abstractmethod
    def flush(self):
        pass

    @abc.abstractmethod
    def insert_command_generator(self):
        pass

    def flush_values(self, values):
        commands_bulk = map(self.insert_command_generator(), values)

        for success, detail_info in streaming_bulk(self.es, commands_bulk):
            if not success:
                logger.warning(detail_info)

    def check_index(self):
        if not self.es.indices.exists(INDEX_NAME):
            self.create_index(INDEX_NAME)

    def create_index(self, index_name):
        logger.info("Index {} does not exist yet. It will be created".format(index_name))
        mapping_path = Path(__file__).parent / 'es_mappings/{}.json'.format(index_name)

        with open(mapping_path, 'r') as f:
            mapping = f.read()
        self.es.indices.create(index_name, mapping)

    def _get_document_id(self, sport_name, date, team1, team2):
        doc_id = '{}-{}-{}-{}'.format(sport_name, date, team1, team2)

        doc_id = hashlib.md5(doc_id.encode())

        return doc_id.hexdigest()


class BetsIndexer(ESIndexer):

    def flush(self):
        self.flush_values(self.bets)
        self.bets = []

    def insert_command_generator(self):
        def _reformat_bets(match_bets):
            bets = []
            for i, bet in enumerate(match_bets):
                bets.append({
                    'bet_type': self.bet_types[i],
                    'bet_info': self.bet_titles[i],
                    'bet': float(bet) if bet else None
                })

            return bets

        def _generate_document(_values):
            return {
                '_id': self._get_document_id(self.sport_name, _values[0], _values[1], _values[2]),
                '_index': INDEX_NAME,
                '_source': {
                    'sport_name': self.sport_name,
                    'competition_name': self.competition_name,
                    'datetime': _values[0],
                    'team1': _values[1],
                    'team2': _values[2],
                    'bets': _reformat_bets(_values[3])
                }
            }

        return _generate_document


class ResultsIndexer(ESIndexer):

    def flush(self):
        self.flush_values(self.results)
        self.results = []

    def insert_command_generator(self):
        def _generate_document(_values):
            return {
                '_id': self._get_document_id(self.sport_name, _values[0], _values[1], _values[2]),
                '_index': INDEX_NAME,
                '_op_type': 'update',
                'doc_as_upsert': True,
                'doc': {
                    'sport_name': self.sport_name,
                    'competition_name': self.competition_name,
                    'date': _values[0],
                    'team1': _values[1],
                    'team2': _values[2],
                    'team1_points': _values[3],
                    'team2_points': _values[4],
                    'correct_bets': _values[5]
                }
            }

        return _generate_document
