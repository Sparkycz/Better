
import datetime
import logging

import requests
from bs4 import BeautifulSoup
from elasticsearch import Elasticsearch

from better.indexers import Indexer
from better.indexers.elasticsearch import BetsIndexer
from . import HtmlObject

logger = logging.getLogger(__name__)


MAX_URL_OFFSET = 500

URL_BASE = 'https://www.ifortuna.cz/cz/sazeni/{sport}?start={offset}&action=bet_table_load&table_type=tables_by_sport&_ajax=1'


def get_argparse_options():
    """Returns kwargs for argument parser constructor.

    Returns:
        dict: Arguments for arg. parser.
    """
    return {
        'description': "Job to crawl ifortuna.cz and load daily bet offers.",
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

    indexer = BetsIndexer(es)

    processor = Crawler(config, es, indexer)
    processor.run()

    logger.info("All done. Bye!")


class Crawler():
    def __init__(self, config, es, indexer: Indexer):
        self.config = config
        self.es = es
        self.indexer = indexer

    def run(self):
        for sport in self.config['SPORTS']:
            try:
                logger.info('Start processing page of {}'.format(sport))
                self._process_page(sport)
            except Exception as e:
                logger.exception(e)

    def _process_page(self, sport):
        proxy_session = None
        if self.config.get('PROXY'):
            logger.info('Starting up proxy session.')
            proxy_session = requests.Session()
            proxy_session.proxies = {
                'http': self.config['PROXY'],
                'https': self.config['PROXY'],
            }

        ifortuna_offset_pages = range(0, MAX_URL_OFFSET, 100)
        for offset in ifortuna_offset_pages:
            self._process_part_of_page(sport, offset, proxy_session)

    def _process_part_of_page(self, sport, offset, proxy_session):
        page = self._request(sport, offset, proxy_session)
        bs = BeautifulSoup(page, 'html.parser')

        bet_segments = bs.find_all('div', attrs={'class': 'box', 'class': 'def'})

        for _segment in bet_segments:
            s = Segment(self.indexer)

            s.parse(_segment)

            if len(self.indexer.bets):
                self.indexer.flush()

    def _request(self, sport, offset, proxy_session):
        url = URL_BASE.format(sport=sport, offset=offset)

        logger.info('request to: ' + url)
        if proxy_session:
            page = proxy_session.get(url, timeout=30)
        else:
            page = requests.get(url, timeout=30)

        return page.content


class Segment(HtmlObject):
    def __init__(self, indexer: Indexer):
        self.indexer = indexer

    def parse(self, _segment):
        bet_table = _segment.findChild('table', attrs={'class': 'bet_table'})
        table_head = bet_table.find('thead')

        if not self._is_that_match(table_head):
            return

        self._parse_competition_name(_segment)

        self._parse_header(table_head)

        self._parse_body(bet_table.find('tbody'))

    def _parse_competition_name(self, _segment):
        title = self.get_text(_segment.findChild('h3', attrs={'class': 'bet_table_title'}))
        logger.info('Processing of the segment {}'.format(title))

        sport, competition = title.split(' | ')


        self.indexer.set_competition(sport, competition)

    def _parse_header(self, thead_block):
        cells = thead_block.findChildren(attrs={'class': 'col_bet'})

        bet_types, bet_titles = [], []
        for cell in cells:
            bet_type = self.get_text(cell.contents[1])
            bet_types.append(bet_type)

            try:
                bet_title = cell.contents[1].attrs['title']
                bet_titles.append(bet_title)
            except KeyError:
                pass

        self.indexer.set_bet_types(bet_types, bet_titles)

    def _parse_body(self, tbody_block):
        matches = tbody_block.findChildren(['tr'])
        for match_row in matches:
            match = Match(self.indexer)
            match.parse(match_row)

    def _is_that_match(self, thead_block):
        return self.get_text(thead_block.find('th', attrs={'class': 'col_title_info'})) == 'Zápas'


class Match(HtmlObject):
    def __init__(self, indexer: Indexer):
        self.indexer = indexer

    def parse(self, match_row):
        team1, team2 = self._parse_teams(match_row)

        bets = self._parse_bets(match_row)

        date = self._parse_date(match_row)

        self.indexer.add_match_bets(date, team1, team2, bets)

    def _parse_date(self, match_row):
        date = self.get_text(match_row.find(attrs={'class': 'col_date'}))

        current_year = datetime.datetime.now().year

        _datetime = datetime.datetime.strptime('{} {}'.format(current_year, date), '%Y %d.%m. %H:%M')

        return _datetime.strftime('%Y-%m-%d')

    def _parse_bets(self, match_row):
        bets = []
        for bet in match_row.findChildren(attrs={'class': 'col_bet'}):
            try:
                bets.append(self.get_text(bet.findChild('a')))
            except AttributeError:
                bets.append(None)

        return bets

    def _parse_teams(self, match_row):
        teams = self.get_text(match_row.findChild(attrs={'class': 'bet_item_detail_href'}))
        team1, team2 = teams.split(' - ')

        return team1, team2