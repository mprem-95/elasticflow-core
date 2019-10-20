import logging

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

log = logging.getLogger(__name__)

class APMCAnalyser(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(APMCAnalyser, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('APMCAnalyser Initiated')

class NewsAnalyser(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(NewsAnalyser, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('NewsAnalyser Initiated')


class GlobalCuesAnalyser(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(GlobalCuesAnalyser, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('GlobalCuesAnalyser Initiated')


class SocialMediaSentimentAnalyser(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(SocialMediaSentimentAnalyser, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('SocialMediaSentimentAnalyser Initiated')


class PoliticalEventsAnalyser(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(PoliticalEventsAnalyser, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('PoliticalEventsAnalyser Initiated')


class NaturalEventsAnalyser(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(NaturalEventsAnalyser, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('NaturalEventsAnalyser Initiated')

        value = context["task_instance"].xcom_pull(
            task_ids=self.parent_task_id)

        if value is not None:
            log.info("Persisted News : " + value)
            return value

class SectorDataAnalyser(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(SectorDataAnalyser, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('SectorDataAnalyser Initiated')

class DataParserPlugin(AirflowPlugin):
    name = "DataParserPlugin"
    operators = [APMCAnalyser, NewsAnalyser, GlobalCuesAnalyser, SocialMediaSentimentAnalyser, PoliticalEventsAnalyser,
                    NaturalEventsAnalyser, SectorDataAnalyser]

