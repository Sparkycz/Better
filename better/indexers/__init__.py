

class Indexer():

    def __init__(self):
        self.sport_name = ''
        self.competition_name = ''
        self.bet_types = []
        self.bet_titles = []
        self.bets = []  # list of tuples ('datetime', 'team1', 'team2', 'bets' in list)
        self.results = []  # list of tuples ('datetime', 'team1', 'team2', 'team1_points', 'team2_points', 'correct_bets' in list)

    def set_competition(self, sport: str, compotetion: str):
        self.sport_name = sport
        self.competition_name = compotetion

    def set_bet_types(self, bet_types: list, bet_titles: list):
        self.bet_types = bet_types
        self.bet_titles = bet_titles

    def add_match_bets(self, date, team1: str, team2: str, bets: list):
        self.bets.append((date, team1, team2, bets))

    def add_match_results(self, date, team1: str, team2: str, team1_points: int, team2_points: int, correct_bets: list):
        self.results.append((date, team1, team2, team1_points, team2_points, correct_bets))

    def flush(self):
        print(self.sport_name)
        print(self.competition_name)
        print(self.bets)
        print(self.results)

