from stackapi import StackAPI
from google.cloud import bigquery
import datetime
import json
import utils

#add filter support

COMMENT_JOB_CONFIG = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("user_id", "INTEGER"),
            bigquery.SchemaField("display_name", "STRING"),
            bigquery.SchemaField("comment_id", "INTEGER"),
            bigquery.SchemaField("question_id", "INTEGER"),
            bigquery.SchemaField("post_type", "STRING"),
            bigquery.SchemaField("comment_date", "TIMESTAMP"),
            bigquery.SchemaField("link", "STRING"),
            ],
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition='WRITE_TRUNCATE',
        )

def get_comment_data(start_date,end_date,so_ids):

    stack_client = StackAPI(name="stackoverflow",version="2.3")
    comments = stack_client.fetch(
            endpoint="users/{ids}/comments",
            filter="!nKzQUR8g67", # with post_type, link
            ids=so_ids,
            fromdate=int(start_date),
            todate=int(end_date)
            )

    comment_json = json.dumps(comments, indent=2)

    return comment_json 

def parse_comment(bucket_name,blob_name,prefix) -> str:

    comments = []
    json_content = utils.read_from_gcs(bucket_name=bucket_name,blob_name=blob_name,prefix="raw/")

    for item in json_content["items"]:
        comment_dict = {
                "user_id": item["owner"]["user_id"],
                "display_name": item["owner"]["display_name"],
                "comment_id": item["comment_id"],
                "question_id": item["post_id"],
                "post_type": item["post_type"],
                "comment_date": item["creation_date"],
                "link": item["link"],
                }
        comments.append(comment_dict)

    nd_json = utils.json_list_to_ndjson(json_list=comments)

    return nd_json
