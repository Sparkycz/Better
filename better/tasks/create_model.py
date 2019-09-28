
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

    processor = Processor(config)
    processor.run()

    logger.info("All done. Bye!")


class Processor():

    def __init__(self, config):
        self.config = config

    def run(self):
        csv_data = pandas.read_csv('models/features.csv', sep=',')

        features = csv_data.drop(['doc_id', 'date', 'sport', 'team_1', 'team_2', 'target', 'class'], axis=1)
        targets = csv_data['target']

        rfc = RandomForestClassifier(n_estimators=100, max_depth=2, random_state=0)
        rfc.fit(features, targets)

        joblib.dump(rfc, 'models/random_forest_classifier.pkl', compress=True)
