
from . import Features


class BetOdds(Features):

    NAMES = ['team1_bet_odds', 'team2_bet_odds']

    def get_features(self):
        team1, team2 = 0, 0
        for bets in self.match_doc.get('bets', []):
            if bets['bet_type'] == str(1):
                team1 = bets['bet'] if bets['bet'] is not None else 0
            if bets['bet_type'] == str(2):
                team2 = bets['bet'] if bets['bet'] is not None else 0

        return [team1, team2]
