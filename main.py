import datetime
import utils
import answer
import comment
import constants
import query
from dateutil.relativedelta import relativedelta

#start_str = "2022-10-01"
#end_str = "2022-11-01"

def generate_dates():

    end_datetime = datetime.datetime.today() + datetime.timedelta(days=1) 
    # handle start of month dates
    # Expected output: end_datetime = 2022-11-01 | start_datetime = 2022-10-01
    if end_datetime == end_datetime.replace(day=1):
        start_datetime = end_datetime - relativedelta(months=1)
    else:
        start_datetime = end_datetime.replace(day=1)

    end_date = end_datetime.timestamp()
    start_date = start_datetime.timestamp()

    start_str = str(start_datetime).split(" ")[0]
    end_str = str(end_datetime).split(" ")[0]
    year_month = str(start_datetime).split(" ")[0].replace("-","_")[:-3]

    date_info = {
            "unix_start_date":start_date,
            "unix_end_date":end_date,
            "start_date_str":start_str,
            "end_date_str":end_str,
            "table_suffix":year_month
            }

    return date_info

def answer_runner(start_str,end_str,start_date,end_date,table_name_suffix):

    table_name = f"answer_{table_name_suffix}"
    table_id = f"{constants.PROJECT_ID}.{constants.DATASET_ID}.{table_name}"

    raw_ans_blob = f"raw_ans_{start_str}_{end_str}.json"
    proc_ans_blob = f"proc_ans_{start_str}_{end_str}.ndjson"

    raw_answer_json = answer.get_answer_data(
            start_date=start_date,
            end_date=end_date,
            so_ids=constants.so_ids
            )
    raw_answer_uri = utils.write_to_gcs(
            json_data=raw_answer_json,
            bucket_name=constants.BUCKET_NAME,
            destination_name=raw_ans_blob,
            prefix="raw/"
            )
    proc_answer_nd_json = answer.parse_answer(
            bucket_name=constants.BUCKET_NAME,
            blob_name=raw_ans_blob,
            prefix="raw/"
            )
    answer_nd_json_uri = utils.write_to_gcs(
            json_data=proc_answer_nd_json,
            bucket_name=constants.BUCKET_NAME,
            destination_name=proc_ans_blob,
            prefix="proc/"
            )
    utils.load_data_to_bq(
            table_id=table_id,
            uri=answer_nd_json_uri,
            job_config=answer.ANSWER_JOB_CONFIG)

def comment_runner(start_str,end_str,start_date,end_date,table_name_suffix):

    table_name = f"comment_{table_name_suffix}"
    table_id = f"{constants.PROJECT_ID}.{constants.DATASET_ID}.{table_name}"

    raw_com_blob = f"raw_com_{start_str}_{end_str}.json"
    proc_com_blob = f"proc_com_{start_str}_{end_str}.ndjson"

    raw_com_json = comment.get_comment_data(
            start_date=start_date,
            end_date=end_date,
            so_ids=constants.so_ids
            )
    raw_com_uri = utils.write_to_gcs(
            json_data=raw_com_json,
            bucket_name=constants.BUCKET_NAME,
            destination_name=raw_com_blob,
            prefix="raw/"
            )
    proc_com_nd_json = comment.parse_comment(
            bucket_name=constants.BUCKET_NAME,
            blob_name=raw_com_blob,
            prefix="raw/"
            )
    com_nd_json_uri = utils.write_to_gcs(
            json_data=proc_com_nd_json,
            bucket_name=constants.BUCKET_NAME,
            destination_name=proc_com_blob,
            prefix="proc/"
            )
    utils.load_data_to_bq(
            table_id=table_id,
            uri=com_nd_json_uri,
            job_config=comment.COMMENT_JOB_CONFIG)

def query_runner(table_name_suffix):
    answer_view_id = query.create_answer_view(project_id=constants.PROJECT_ID,dataset_id=constants.DATASET_ID,year_month=table_name_suffix)
    comment_view_id = query.create_comment_view(project_id=constants.PROJECT_ID,dataset_id=constants.DATASET_ID,year_month=table_name_suffix)
    total_view_id = query.create_total_view(project_id=constants.PROJECT_ID,dataset_id=constants.DATASET_ID,year_month=table_name_suffix)
    summary_view_id = query.create_summary_view(project_id=constants.PROJECT_ID,dataset_id=constants.DATASET_ID,year_month=table_name_suffix)


def main():

    date_dict = generate_dates()
    table_name_suffix = date_dict["table_suffix"]
    start_str = date_dict["start_date_str"]
    end_str = date_dict["end_date_str"]
    start_date = date_dict["unix_start_date"]
    end_date = date_dict["unix_end_date"]

    answer_runner(start_str=start_str,end_str=end_str,start_date=start_date,end_date=end_date,table_name_suffix=table_name_suffix)
    comment_runner(start_str=start_str,end_str=end_str,start_date=start_date,end_date=end_date,table_name_suffix=table_name_suffix)
    query_runner(table_name_suffix=table_name_suffix)

if __name__ == "__main__":
    main()
