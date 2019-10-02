import argparse
import logging
from importlib import import_module

from . import config


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logging.getLogger("elasticsearch").setLevel(logging.WARNING)


def run(argv):
    """Main application runner

    This module is responsible for loading the configuration and parsing the arguments.
    """
    if len(argv) < 2 or argv[1].startswith('-'):
        raise Exception("The first argument has to be the task name")

    # init module, call the options setter if any and execute
    module = import_module('.tasks.' + argv[1], 'better')

    options = {}
    if hasattr(module, 'get_argparse_options'):
        options = module.get_argparse_options()

    parser = argparse.ArgumentParser("{} {}".format(*argv[0:2]), **options)

    if hasattr(module, 'set_arguments'):
        module.set_arguments(parser)

    cfg = config.init_config()

    module.execute(cfg, parser.parse_args(argv[2:]))
