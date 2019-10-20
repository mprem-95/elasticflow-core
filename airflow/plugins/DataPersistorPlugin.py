import logging

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

import cache_util as cache

log = logging.getLogger(__name__)

class APMCDataPersister(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(APMCDataPersister, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('APMCDataPersister Initiated')

        value = context["task_instance"].xcom_pull(
            task_ids=self.parent_task_id)

        if value is not None:
            log.info("Dataframe from raw json : " + str(value))
            cache.set("apmc",value.to_string())

        return value

class NewsPersister(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(NewsPersister, self).__init__(*args, **kwargs)

    def execute(self, context):

        log.info('NewsPersister Initiated')

        value = context["task_instance"].xcom_pull(
            task_ids=self.parent_task_id)

        if value is not None:
            log.info("Processed News : " + value)
            cache.set("news", value)

        return value

class GlobalCuesPersister(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(GlobalCuesPersister, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('GlobalCuesPersister Initiated')

        value = context["task_instance"].xcom_pull(
            task_ids=self.parent_task_id)

        if value is not None:
            log.info("Processed News : " + value)
            cache.set("global_cues", value)

class SocialMediaSentimentPersister(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(SocialMediaSentimentPersister, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('SocialMediaSentimentPersister Initiated')


class PoliticalEventsPersister(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(PoliticalEventsPersister, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('PoliticalEventsPersister Initiated')

        value = context["task_instance"].xcom_pull(
            task_ids=self.parent_task_id)

        if value is not None:
            log.info("Processed News : " + value)
            cache.set("politics", value)

        return value

class NaturalEventsPersister(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(NaturalEventsPersister, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('NaturalEventsPersister Initiated')


class SectorDataPersister(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(SectorDataPersister, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('SectorDataPersister Initiated')


class DataPersisterPlugin(AirflowPlugin):
    name = "DataPersisterPlugin"
    operators = [APMCDataPersister, NewsPersister, GlobalCuesPersister, SocialMediaSentimentPersister,
                 PoliticalEventsPersister, NaturalEventsPersister, SectorDataPersister]

