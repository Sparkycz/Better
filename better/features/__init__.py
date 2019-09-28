
import abc

from better.helpers import ElasticsearchWrapper


class Features():
    __metaclass__ = abc.ABCMeta

    NAMES = []
    """List of features' names."""

    def __init__(self, es: ElasticsearchWrapper, match_doc_id, match_doc):
        self.es = es
        self.match_doc_id = match_doc_id
        self.match_doc = match_doc

    @abc.abstractmethod
    def get_features(self):
        pass
