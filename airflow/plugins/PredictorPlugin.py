import logging

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)

class Predictor(BaseOperator):

    @apply_defaults
    def __init__(self,
                 *args,
                 **kwargs):

        super(Predictor, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('Predictor Initiated')


class PredictorPlugin(AirflowPlugin):
    name = "PredictorPlugin"
    operators = [Predictor]