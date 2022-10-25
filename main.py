import datetime
import utils
import answer
import comment
import constants
import query

start_str = "2022-10-01"
end_str = "2022-11-01"

start_date = datetime.datetime.strptime(start_str, "%Y-%m-%d").timestamp()
end_date = datetime.datetime.strptime(end_str, "%Y-%m-%d").timestamp()

def answer_runner():
    table_name_suffix = start_str.replace("-","_")[:-3]
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

def comment_runner():
    table_name_suffix = start_str.replace("-","_")[:-3]
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

def query_runner():
    table_name_suffix = start_str.replace("-","_")[:-3]
    answer_view_id = query.answer_view(project_id=constants.PROJECT_ID,dataset_id=constants.DATASET_ID,year_month=table_name_suffix)
    comment_view_id = query.comment_view(project_id=constants.PROJECT_ID,dataset_id=constants.DATASET_ID,year_month=table_name_suffix)
    total_view_id = query.total_view(project_id=constants.PROJECT_ID,dataset_id=constants.DATASET_ID,year_month=table_name_suffix)
    summary_view_id = query.summary_view(project_id=constants.PROJECT_ID,dataset_id=constants.DATASET_ID,year_month=table_name_suffix)


def main():
    answer_runner()
    comment_runner()
    query_runner()

if __name__ == "__main__":
    main()
