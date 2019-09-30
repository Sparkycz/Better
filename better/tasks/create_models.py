
import logging

import pandas
from sklearn.ensemble import RandomForestClassifier
import joblib

logger = logging.getLogger(__name__)


def get_argparse_options():
    """Returns kwargs for argument parser constructor.

    Returns:
        dict: Arguments for arg. parser.
    """
    return {
        'description': "Job to create Random Forest Classification model.",
    }


def set_arguments(parser):
    """Adds command line arguments to the task.

    Args:
        parser (argparse.ArgumentParser): Arg. parser to setup.
    """
    pass


def execute(config, options):
    """Main task function to be executed via launcher."""

    for sport in config['SPORTS']:
        logger.info(f'Creating model of {sport}')
        processor = Processor(config, sport)
        processor.run()

    logger.info("All done. Bye!")


class Processor():

    def __init__(self, config, sport):
        self.config = config
        self.sport = sport

    def run(self):
        csv_data = pandas.read_csv(f'models/features_{self.sport}.csv', sep=',')

        features = csv_data.drop(['doc_id', 'date', 'sport', 'team_1', 'team_2', 'target', 'class'], axis=1)
        targets = csv_data['target']

        if not targets.empty:
            rfc = RandomForestClassifier(n_estimators=100, max_depth=2, random_state=0)
            rfc.fit(features, targets)

            joblib.dump(rfc, f'models/random_forest_classifier_{self.sport}.pkl', compress=True)
        else:
            logger.info('No data.')
