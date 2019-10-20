import logging

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

import json
import pandas as pd

log = logging.getLogger(__name__)

class APMCDataParser(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):

        self.parent_task_id = parent_task_id
        super(APMCDataParser, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('APMCDataParser Initiated')
        value = context["task_instance"].xcom_pull(
            task_ids=self.parent_task_id)

        if value is not None:
            raw_data = json.loads(value)
            rows = raw_data["data"]
            df = pd.DataFrame(rows)
            return df

class NewsParser(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(NewsParser, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('NewsParser Initiated')

        news = {}
        value = context["task_instance"].xcom_pull(
                task_ids=self.parent_task_id)

        if value is not None:
            news_raw = json.loads(value)
            news["data"] = []

            for item in news_raw["items"]:
                news_item = {
                    "link" : item["link"],
                    "snippet" : item["snippet"],
                }

                news["data"].append(news_item)

            log.info(str(value))

            return json.dumps(news)

        else:
            log.info("No news available")
            return None

class GlobalCuesParser(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(GlobalCuesParser, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('GlobalCuesParser Initiated')
        news = {}
        value = context["task_instance"].xcom_pull(
            task_ids=self.parent_task_id)

        if value is not None:
            news_raw = json.loads(value)
            news["data"] = []

            for item in news_raw["items"]:
                news_item = {
                    "link": item["link"],
                    "snippet": item["snippet"],
                }

                news["data"].append(news_item)

            log.info(str(value))

            return json.dumps(news)

        else:
            log.info("No news available")
            return None

class SectorDataParser(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(SectorDataParser, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('SectorDataParser Initiated')


class SocialMediaSentimentParser(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(SocialMediaSentimentParser, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('SocialMediaSentimentParser Initiated')


class NaturalEventsParser(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(NaturalEventsParser, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('NaturalEventsParser Initiated')


class PoliticalEventsParser(BaseOperator):

    @apply_defaults
    def __init__(self,
                 parent_task_id,
                 *args,
                 **kwargs):
        self.parent_task_id = parent_task_id
        super(PoliticalEventsParser, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('PoliticalEventsParser Initiated')
        news = {}
        value = context["task_instance"].xcom_pull(
            task_ids=self.parent_task_id)

        if value is not None:
            news_raw = json.loads(value)
            news["data"] = []

            for item in news_raw["items"]:
                news_item = {
                    "link": item["link"],
                    "snippet": item["snippet"],
                }

                news["data"].append(news_item)

            log.info(str(value))

            return json.dumps(news)

        else:
            log.info("No news available")
            return None

class DataParserPlugin(AirflowPlugin):
    name = "DataParserPlugin"
    operators = [APMCDataParser, NewsParser, SectorDataParser, SocialMediaSentimentParser, NaturalEventsParser,
                 PoliticalEventsParser, GlobalCuesParser]

