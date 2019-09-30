
from . import Features


class PreviousMatchesTogether(Features):

    NAMES = [
        'match_together_count',
        'team1_wins', 'team2_wins', 'draw_counts',
        'team1_points_sum', 'team2_points_sum',
        'last_match_team1_points', 'last_match_team2_points', 'last_match_points_ratio'
    ]

    def get_features(self):
        matches = self._load_previous_matches(self.match_doc_id, self.match_doc)

        if not matches:
            return [0 for _ in self.NAMES]

        _features = [len(matches)]
        _features.extend(self._get_wins_losts(matches))
        _features.extend(self._get_points_summarize(matches))
        _features.extend(self._get_last_match_points(matches))

        return _features

    def _get_last_match_points(self, matches):
        last_match_team1_points, last_match_team2_points = self._get_points_summarize([matches[0]])
        ratio = last_match_team1_points - last_match_team2_points

        return [last_match_team1_points, last_match_team2_points, ratio]


    def _get_wins_losts(self, matches):
        team1_wins, team2_wins, draws = 0, 0, 0
        for match in matches:
            if "0" in match['correct_bets']:
                draws += 1

            if match['team1'] == self.match_doc['team1']:
                if "1" in match['correct_bets']:
                    team1_wins += 1
                if "2" in match['correct_bets']:
                    team2_wins += 1

            if match['team2'] == self.match_doc['team1']:
                if "1" in match['correct_bets']:
                    team2_wins += 1
                if "2" in match['correct_bets']:
                    team1_wins += 1

        return [team1_wins, team2_wins, draws]

    def _get_points_summarize(self, matches):
        team1_points, team2_points = 0, 0
        for match in matches:
            if match['team1'] == self.match_doc['team1']:
                team1_points += int(match['team1_points'])
                team2_points += int(match['team2_points'])
            if match['team2'] == self.match_doc['team1']:
                team1_points += int(match['team2_points'])
                team2_points += int(match['team1_points'])

        return [team1_points, team2_points]

    def _load_previous_matches(self, doc_id, doc):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "exists": {"field": "correct_bets"}
                        },
                        {
                            "exists": {"field": "team1_points"}
                        },
                        {
                            "exists": {"field": "team2_points"}
                        },
                    ],
                    "must_not": [
                        {
                            "term": {
                                "_id": {
                                    "value": doc_id
                                }
                            }
                        }
                    ],
                    "should": [
                        {
                            "bool": {
                                "must": [
                                    {
                                        "match": {
                                            "team1": doc['team1']
                                        }
                                    },
                                    {
                                        "match": {
                                            "team2": doc['team2']
                                        }
                                    }
                                ]
                            }
                        },
                        {
                            "bool": {
                                "must": [
                                    {
                                        "match": {
                                            "team1": doc['team2']
                                        }
                                    },
                                    {
                                        "match": {
                                            "team2": doc['team1']
                                        }
                                    }
                                ]
                            }
                        }
                    ],
                    "minimum_should_match": 1
                }
            },
            "sort": [
                {
                    "date": {
                        "order": "desc"
                    }
                }
            ]
        }

        es_result = self.es.search(query)

        try:
            return [doc['_source'] for doc in es_result['hits']['hits']]
        except KeyError:
            return []
