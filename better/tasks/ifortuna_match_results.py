
import datetime
import logging

import requests
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch

from better.indexers import Indexer
from better.indexers.elasticsearch import ResultsIndexer
from . import HtmlObject

logger = logging.getLogger(__name__)

SPORTS = ['Hokej', 'Fotbal', 'Basketbal']

URL_BASE = 'https://www.ifortuna.cz/cz/sazeni/vysledky/{date}'


def get_argparse_options():
    """Returns kwargs for argument parser constructor.

    Returns:
        dict: Arguments for arg. parser.
    """
    return {
        'description': "Job to crawl ifortuna.cz and load history competition results.",
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

    indexer = ResultsIndexer(es)

    processor = Crawler(config, es, indexer)
    processor.run()

    logger.info("All done. Bye!")


class Crawler():
    def __init__(self, config, es, indexer: Indexer):
        self.config = config
        self.es = es
        self.indexer = indexer

    def run(self):
        proxy_session = None
        if self.config.get('PROXY'):
            logger.info('Starting up proxy session.')
            proxy_session = requests.Session()
            proxy_session.proxies = {
                'http': self.config['PROXY'],
                'https': self.config['PROXY'],
            }

        date = datetime.date.today() - datetime.timedelta(days=1)

        page = self._request(date.strftime('%Y-%m-%d'), proxy_session)
        bs = BeautifulSoup(page, 'html.parser')

        _segments = bs.find_all('div', attrs={'class': 'box def nopadding'})

        for _segment in _segments:
            s = Segment(self.config['SPORTS'], self.indexer)

            s.parse(_segment)

            if len(self.indexer.results):
                self.indexer.flush()

    def _request(self, date, proxy_session):
        url = URL_BASE.format(date=date)

        logger.info('request to: ' + url)
        if proxy_session:
            page = proxy_session.get(url, timeout=30)
        else:
            page = requests.get(url, timeout=30)

        return page.content


class Segment(HtmlObject):
    def __init__(self, sports, indexer: Indexer):
        self.sports = sports
        self.indexer = indexer

    def parse(self, _segment):
        bet_table = _segment.findChild('table', attrs={'class': 'bet_table'})
        table_head = bet_table.find('thead')

        if not self._is_that_match(table_head):
            return

        sport, competition = self._parse_competition_name(_segment)

        if sport.lower() not in self.sports:
            return

        self.indexer.set_competition(sport, competition)

        self._parse_body(bet_table.find('tbody'))

    def _parse_competition_name(self, _segment):
        title = self.get_text(_segment.findChild('h3', attrs={'class': 'bet_table_title'}))
        logger.info('Processing of the segment {}'.format(title))

        sport, competition = title.split(' | ')

        return sport, competition

    def _parse_body(self, tbody_block):
        matches = tbody_block.findChildren(['tr'])
        for match_row in matches:
            match = Match(self.indexer)
            try:
                match.parse(match_row)
            except AttributeError:
                # Ignore the match cause the team names does contain link which I need to get rid of unwanted number.
                pass

    def _is_that_match(self, thead_block):
        return self.get_text(thead_block.find('th', attrs={'class': 'col_title'})) == 'Zápas'


class Match(HtmlObject):
    def __init__(self, indexer: Indexer):
        self.indexer = indexer

    def parse(self, match_row):
        team1, team2 = self._parse_teams(match_row)

        correct_bets = self._parse_correct_bets(match_row)

        if correct_bets[0] == 'není k dispozici':
            return

        team1_points, team2_points = self._parse_match_points(match_row)

        date = self._parse_date(match_row)

        self.indexer.add_match_results(date, team1, team2, team1_points, team2_points, correct_bets)

    def _parse_date(self, match_row):
        date = self.get_text(match_row.find(attrs={'class': 'col_date'}))

        _datetime = datetime.datetime.strptime(date, '%d.%m.%Y')

        return _datetime.strftime('%Y-%m-%d')

    def _parse_correct_bets(self, match_row):
        return self.get_text(match_row.findChild('td', attrs={'class': 'col_correctBets'})).split(',')

    def _parse_match_points(self, match_row):
        p1, p2 = self.get_text(match_row.findChild('td', attrs={'class': 'col_betResult'})).split(':')

        return int(p1), int(p2)

    def _parse_teams(self, match_row):
        teams = self.get_text(match_row.findChild(attrs={'class': 'bet_item_detail_href'}))
        team1, team2 = teams.split(' - ')

        return team1, team2