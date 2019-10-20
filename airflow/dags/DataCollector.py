from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator

from airflow.operators import Predictor
import json

import operator_util, cache_util

# data = {
#         "sources" : [
#             {
#                 "name" : "apmc",
#                 "meta" : {
#                     "state" : "MAHARASHTRA",
#                     "apmc" : "NAGPUR",
#                     "commodity" : "WHEAT",
#                     "start_date" : "2017-01-01",
#                     "end_date" : "2019-10-18"
#                 }
#             },
#             {
#                 "name" : "news",
#                 "meta" : {
#                     "query" : "wheat"
#                 }
#             }
#     ]
# }
#
# def get_data_sources():
#
#     return data["sources"]


dag = DAG('ElasticFlow',
                        description='Dynamically load, transform, analyse, persist and predict trends',
                        schedule_interval='0 12 * * *',
                        start_date=datetime(2019, 10, 19),
                        catchup=False)


initiate_operator = DummyOperator(
                        task_id="ElasticFlow_Orchestrator",
                        retries=3,
                        dag=dag)

predictor_operator = Predictor(
                        task_id="Prediction",
                        retries=3,
                        dag=dag)

configuration = json.loads(cache_util.get("configuration"))

for source in configuration["sources"]:

    if source is not None:
        source_initiate_operator = DummyOperator(
            task_id=source + "_initiate",
            retries=3,
            dag=dag)

        initiate_operator >> source_initiate_operator

        collection_operator, parser_operator, analyser_operator, persister_operator = operator_util.get_operator(dag, source, configuration["meta"])

        source_initiate_operator >> collection_operator \
            >> parser_operator >> persister_operator \
                >> analyser_operator >> predictor_operator

