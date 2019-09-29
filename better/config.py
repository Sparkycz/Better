
import logging
from llconfig import Config

logger = logging.getLogger(__name__)


def init_config():

    c = Config(env_prefix='')

    c.init('SPORTS', list, ['fotbal', 'hokej', 'basketbal', 'hazena', 'baseball', 'florbal', 'futsal', 'ragby'])

    c.init('PROXY', str)

    c.init('ELASTICSEARCH_HOST', str)

    c.load()

    return c
