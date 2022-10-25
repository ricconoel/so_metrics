from google.cloud import storage,bigquery
import datetime
import json

def write_to_gcs(json_data,bucket_name,destination_name,prefix) -> str:

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(prefix + destination_name)

    blob.upload_from_string(json_data)
    obj_uri = f"gs://{bucket_name}/{prefix}{destination_name}"

    print(f"Uploaded to {obj_uri}")

    return obj_uri

def read_from_gcs(bucket_name,blob_name,prefix) -> dict:

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(prefix + blob_name)
    content = blob.download_as_string().decode()
    json_content = json.loads(content)

    return json_content

def json_list_to_ndjson(json_list) -> str:

    result = [json.dumps(entry) for entry in json_list]
    nd_json = ('\n'.join(result))

    return nd_json

def load_data_to_bq(table_id,uri,job_config):

    bq_client = bigquery.Client()
    load_job = bq_client.load_table_from_uri(
        uri,
        table_id,
        location="US",
        job_config=job_config,
        )

    load_job.result()
    destination_table = bq_client.get_table(table_id)

    print(destination_table)
