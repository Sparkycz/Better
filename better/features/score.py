
from . import Features


class Score(Features):

    NAMES = ['team1_points', 'team2_points']

    def get_features(self):
        return [self.match_doc['team1_points'], self.match_doc['team2_points']]
