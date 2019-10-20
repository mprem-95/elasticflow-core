from airflow.operators import APMCCollector
from airflow.operators import APMCDataParser
from airflow.operators import APMCAnalyser
from airflow.operators import APMCDataPersister

from airflow.operators import NewsCollector
from airflow.operators import NewsParser
from airflow.operators import NewsAnalyser
from airflow.operators import NewsPersister

from airflow.operators import GlobalCuesCollector
from airflow.operators import GlobalCuesParser
from airflow.operators import GlobalCuesAnalyser
from airflow.operators import GlobalCuesPersister

from airflow.operators import SectorDataCollector
from airflow.operators import SectorDataParser
from airflow.operators import SectorDataAnalyser
from airflow.operators import SectorDataPersister

from airflow.operators import SocialMediaSentimentCollector
from airflow.operators import SocialMediaSentimentParser
from airflow.operators import SocialMediaSentimentAnalyser
from airflow.operators import SocialMediaSentimentPersister

from airflow.operators import NaturalEventsCollector
from airflow.operators import NaturalEventsParser
from airflow.operators import NaturalEventsAnalyser
from airflow.operators import NaturalEventsPersister

from airflow.operators import PoliticalEventsCollector
from airflow.operators import PoliticalEventsParser
from airflow.operators import PoliticalEventsAnalyser
from airflow.operators import PoliticalEventsPersister


def get_operator(dag, type, meta):
    if type == "apmc":
        collection_operator = APMCCollector(
                        task_id=type + "_collector",
                        state=meta["state"],
                        apmc=meta["region"],
                        commodity=meta["commodity"],
                        start_date=meta["start_date"],
                        end_date=meta["end_date"],
                        retries=3,
                        dag=dag)

        parser_operator = APMCDataParser(
                        task_id=type + "_parser",
                        parent_task_id=type + "_collector",
                        retries=3,
                        dag=dag)

        persister_operator = APMCDataPersister(
                        task_id=type + "_persister",
                        parent_task_id=type + "_parser",
                        retries=3,
                        dag=dag)

        analyser_operator = APMCAnalyser(
                        task_id=type + "_analyser",
                        parent_task_id=type + "_persister",
                        retries=3,
                        dag=dag)

        return collection_operator, parser_operator, analyser_operator, persister_operator


    if type == "news":
        collection_operator = NewsCollector(
                        task_id=type + "_collector",
                        query=meta["commodity"],
                        retries=3,
                        dag=dag)

        parser_operator = NewsParser(
                        task_id=type + "_parser",
                        parent_task_id=type + "_collector",
                        retries=3,
                        dag=dag)

        persister_operator = NewsPersister(
                        task_id=type + "_persister",
                        parent_task_id=type + "_parser",
                        retries=3,
                        dag=dag)

        analyser_operator = NewsAnalyser(
                        task_id=type + "_analyser",
                        parent_task_id=type + "_persister",
                        retries=3,
                        dag=dag)

        return collection_operator, parser_operator, analyser_operator, persister_operator


    if type == "sector":
        collection_operator = SectorDataCollector(
                        task_id=type + "_collector",
                        query=meta["commodity"],
                        retries=3,
                        dag=dag)

        parser_operator = SectorDataParser(
                        task_id=type + "_parser",
                        parent_task_id=type + "_collector",
                        retries=3,
                        dag=dag)

        persister_operator = SectorDataPersister(
                        task_id=type + "_persister",
                        parent_task_id=type + "_parser",
                        retries=3,
                        dag=dag)

        analyser_operator = SectorDataAnalyser(
                        task_id=type + "_analyser",
                        parent_task_id=type + "_persister",
                        retries=3,
                        dag=dag)

        return collection_operator, parser_operator, analyser_operator, persister_operator


    if type == "global_cues":
        collection_operator = GlobalCuesCollector(
                        task_id=type + "_collector",
                        query=meta["commodity"],
                        retries=3,
                        dag=dag)

        parser_operator = GlobalCuesParser(
                        task_id=type + "_parser",
                        parent_task_id=type + "_collector",
                        retries=3,
                        dag=dag)

        persister_operator = GlobalCuesPersister(
                        task_id=type + "_persister",
                        parent_task_id=type + "_parser",
                        retries=3,
                        dag=dag)

        analyser_operator = GlobalCuesAnalyser(
                        task_id=type + "_analyser",
                        parent_task_id=type + "_persister",
                        retries=3,
                        dag=dag)

        return collection_operator, parser_operator, analyser_operator, persister_operator


    if type == "social_media":
        collection_operator = SocialMediaSentimentCollector(
                        task_id=type + "_collector",
                        query=meta["commodity"],
                        retries=3,
                        dag=dag)

        parser_operator = SocialMediaSentimentParser(
                        task_id=type + "_parser",
                        parent_task_id=type + "_collector",
                        retries=3,
                        dag=dag)

        persister_operator = SocialMediaSentimentPersister(
                        task_id=type + "_persister",
                        parent_task_id=type + "_parser",
                        retries=3,
                        dag=dag)

        analyser_operator = SocialMediaSentimentAnalyser(
                        task_id=type + "_analyser",
                        parent_task_id=type + "_persister",
                        retries=3,
                        dag=dag)

        return collection_operator, parser_operator, analyser_operator, persister_operator


    if type == "politics":
        collection_operator = PoliticalEventsCollector(
                        task_id=type + "_collector",
                        query=meta["commodity"],
                        retries=3,
                        dag=dag)

        parser_operator = PoliticalEventsParser(
                        task_id=type + "_parser",
                        parent_task_id=type + "_collector",
                        retries=3,
                        dag=dag)

        persister_operator = PoliticalEventsPersister(
                        task_id=type + "_persister",
                        parent_task_id=type + "_parser",
                        retries=3,
                        dag=dag)

        analyser_operator = PoliticalEventsAnalyser(
                        task_id=type + "_analyser",
                        parent_task_id=type + "_persister",
                        retries=3,
                        dag=dag)

        return collection_operator, parser_operator, analyser_operator, persister_operator


    if type == "nature":
        collection_operator = NaturalEventsCollector(
                        task_id=type + "_collector",
                        query=meta["commodity"],
                        retries=3,
                        dag=dag)

        parser_operator = NaturalEventsParser(
                        task_id=type + "_parser",
                        parent_task_id=type + "_collector",
                        retries=3,
                        dag=dag)

        persister_operator = NaturalEventsPersister(
                        task_id=type + "_persister",
                        parent_task_id=type + "_parser",
                        retries=3,
                        dag=dag)

        analyser_operator = NaturalEventsAnalyser(
                        task_id=type + "_analyser",
                        parent_task_id=type + "_persister",
                        retries=3,
                        dag=dag)

        return collection_operator, parser_operator, analyser_operator, persister_operator

# apmc, news, sector, social_media, politics, nature, global_cues