from stackapi import StackAPI
from google.cloud import bigquery
import datetime
import json
import utils

ANSWER_JOB_CONFIG = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("user_id", "INTEGER"),
            bigquery.SchemaField("display_name", "STRING"),
            bigquery.SchemaField("answer_id", "INTEGER"),
            bigquery.SchemaField("question_id", "INTEGER"),
            bigquery.SchemaField("is_accepted", "BOOLEAN"),
            bigquery.SchemaField("score", "INTEGER"),
            bigquery.SchemaField("answer_date", "TIMESTAMP"),
            bigquery.SchemaField("tags", "STRING", mode="REPEATED"),
            ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition='WRITE_TRUNCATE',
        )

def get_answer_data(start_date,end_date,so_ids):

    stack_client = StackAPI(name="stackoverflow",version="2.3")
    answers = stack_client.fetch(
            endpoint="users/{ids}/answers",
            filter="!nKzQUR4x1Q",
            ids=so_ids,
            fromdate=int(start_date),
            todate=int(end_date)
            )

    answer_json = json.dumps(answers, indent=2)

    return answer_json

def parse_answer(bucket_name,blob_name,prefix) -> str:

    answers = []
    json_content = utils.read_from_gcs(bucket_name=bucket_name,blob_name=blob_name,prefix="raw/")

    for item in json_content["items"]:
        answer_dict = {
                "user_id": item["owner"]["user_id"],
                "display_name": item["owner"]["display_name"],
                "answer_id": item["answer_id"],
                "question_id": item["question_id"],
                "is_accepted": item["is_accepted"],
                "score": item["score"],
                "answer_date": item["creation_date"],
                "tags": item["tags"]
                }
        answers.append(answer_dict)

    nd_json = utils.json_list_to_ndjson(json_list=answers)

    return nd_json

