import logging

from airflow.models import BaseOperator
from airflow.models import Variable
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults

import requests
import constants

log = logging.getLogger(__name__)

class APMCCollector(BaseOperator):

    @apply_defaults
    def __init__(self,
                 state,
                 apmc,
                 commodity,
                 start_date,
                 end_date,
                 *args,
                 **kwargs):

        self.state = state
        self.apmc = apmc
        self.commodity = commodity
        self.start_date = str(start_date)
        self.end_date = str(end_date)

        super(APMCCollector, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info('APMCCollector Initiated')

        log.info(str(self.start_date) + " " + str(self.end_date))

        payload = str(constants.APMC_PAYLOAD)
        payload = payload.format(self.state, self.apmc, self.commodity, "2018-01-01", "2019-10-18")

        headers = {
            'Content-Type': "application/x-www-form-urlencoded",
            'Accept': "*/*",
            'Cache-Control': "no-cache",
            'Host': "enam.gov.in",
            'Accept-Encoding': "gzip, deflate",
            'Content-Length': "107",
            'Connection': "keep-alive",
            'cache-control': "no-cache"
        }

        log.info("URL : " + constants.APMC_ENDPOINT)
        log.info("payload : " + str(payload))
        log.info("headers" + str(headers))

        response = requests.request("POST", constants.APMC_ENDPOINT, data=payload, headers=headers)

        log.info("API endpoint returned status code " + str(response.status_code))

        return response.text

class NewsCollector(BaseOperator):

    @apply_defaults
    def __init__(self,
                query,
                *args,
                **kwargs):

        self.query = str(query)
        super(NewsCollector, self).__init__(*args, **kwargs)

    def execute(self, context):

        log.info('NewsCollector Initiated')

        querystring = {"key": constants.GOOGLE_API_KEY, "q": self.query,
                   "cx": constants.GOOGLE_CSE}

        headers = {
            'Accept': "*/*",
            'Cache-Control': "no-cache",
            'Host': "www.googleapis.com",
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
            'cache-control': "no-cache"
        }

        response = requests.request("GET", constants.GOOGLE_SEARCH_URL, headers=headers, params=querystring)
        log.info("API endpoint returned status code " + str(response.status_code))

        # context["task_instance"].xcom_push("news", response.text)
        return response.text


class GlobalCuesCollector(BaseOperator):

    @apply_defaults
    def __init__(self,
                query,
                *args,
                **kwargs):

        self.query = str(query)
        super(GlobalCuesCollector, self).__init__(*args, **kwargs)

    def execute(self, context):

        log.info('GlobalCuesCollector Initiated')

        querystring = {"key": constants.GOOGLE_API_KEY, "q": "policy",
                   "cx": "012467406080593405763:h25gwylbijp"}

        headers = {
            'Accept': "*/*",
            'Cache-Control': "no-cache",
            'Host': "www.googleapis.com",
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
            'cache-control': "no-cache"
        }

        response = requests.request("GET", constants.GOOGLE_SEARCH_URL, headers=headers, params=querystring)
        log.info("API endpoint returned status code " + str(response.status_code))

        return response.text

class SectorDataCollector(BaseOperator):

    @apply_defaults
    def __init__(self,
                query,
                *args,
                **kwargs):

        self.query = str(query)
        super(SectorDataCollector, self).__init__(*args, **kwargs)

    def execute(self, context):

        log.info('SectorDataCollector Initiated')

        querystring = {"key": constants.GOOGLE_API_KEY, "q": self.query,
                   "cx": constants.GOOGLE_CSE}

        headers = {
            'Accept': "*/*",
            'Cache-Control': "no-cache",
            'Host': "www.googleapis.com",
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
            'cache-control': "no-cache"
        }

        response = requests.request("GET", constants.GOOGLE_SEARCH_URL, headers=headers, params=querystring)
        log.info("API endpoint returned status code " + str(response.status_code))

        context["task_instance"].xcom_push("sector_data", response.text)


class SocialMediaSentimentCollector(BaseOperator):

    @apply_defaults
    def __init__(self,
                query,
                *args,
                **kwargs):

        self.query = str(query)
        super(SocialMediaSentimentCollector, self).__init__(*args, **kwargs)

    def execute(self, context):

        log.info('SocialMediaSentimentCollector Initiated')

        querystring = {"key": constants.GOOGLE_API_KEY, "q": self.query,
                   "cx": constants.GOOGLE_CSE}

        headers = {
            'Accept': "*/*",
            'Cache-Control': "no-cache",
            'Host': "www.googleapis.com",
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
            'cache-control': "no-cache"
        }

        response = requests.request("GET", constants.GOOGLE_SEARCH_URL, headers=headers, params=querystring)
        log.info("API endpoint returned status code " + str(response.status_code))

        context["task_instance"].xcom_push("social_media_sentiments", response.text)


class NaturalEventsCollector(BaseOperator):

    @apply_defaults
    def __init__(self,
                query,
                *args,
                **kwargs):

        self.query = str(query)
        super(NaturalEventsCollector, self).__init__(*args, **kwargs)

    def execute(self, context):

        log.info('NaturalEventsCollector Initiated')

        querystring = {"key": constants.GOOGLE_API_KEY, "q": self.query,
                   "cx": constants.GOOGLE_CSE}

        headers = {
            'Accept': "*/*",
            'Cache-Control': "no-cache",
            'Host': "www.googleapis.com",
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
            'cache-control': "no-cache"
        }

        response = requests.request("GET", constants.GOOGLE_SEARCH_URL, headers=headers, params=querystring)
        log.info("API endpoint returned status code " + str(response.status_code))

        context["task_instance"].xcom_push("natural_events", response.text)


class PoliticalEventsCollector(BaseOperator):

    @apply_defaults
    def __init__(self,
                query,
                *args,
                **kwargs):

        self.query = str(query)
        super(PoliticalEventsCollector, self).__init__(*args, **kwargs)

    def execute(self, context):

        log.info('PoliticalEventsCollector Initiated')

        querystring = {"key": constants.GOOGLE_API_KEY, "q": "policy",
                   "cx": "012467406080593405763:g2mgttuy1bj"}

        headers = {
            'Accept': "*/*",
            'Cache-Control': "no-cache",
            'Host': "www.googleapis.com",
            'Accept-Encoding': "gzip, deflate",
            'Connection': "keep-alive",
            'cache-control': "no-cache"
        }

        response = requests.request("GET", constants.GOOGLE_SEARCH_URL, headers=headers, params=querystring)
        log.info("API endpoint returned status code " + str(response.status_code))

        return response.text


class DataCollectorPlugin(AirflowPlugin):
    name = "DataCollectorPlugin"
    operators = [APMCCollector, NewsCollector, GlobalCuesCollector, SectorDataCollector, SocialMediaSentimentCollector,
                    NaturalEventsCollector, PoliticalEventsCollector]