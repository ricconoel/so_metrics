from google.cloud import bigquery
from google.api_core.exceptions import NotFound
import constants

client = bigquery.Client()

def convert_file_to_string(sql_filename):
    with open(sql_filename) as sql_file:
        return sql_file.read()

def try_create_view(view_id,query_str):
    try:
        view = client.get_table(view_id)
        print(f"View {view_id} already exist")

        return view_id

    except NotFound:
        print(f"View {view_id} not found.")
        print("Creating view")
        view = bigquery.Table(view_id)
        view.view_query = query_str
        view = client.create_table(view)
        print(f"Created {view.table_type}: {str(view.reference)}")
        view_ref = str(view.reference)

        return view_ref

def create_answer_view(project_id,dataset_id,year_month):

    view_id = f"{project_id}.{dataset_id}.calc_answer_{year_month}"

    q = convert_file_to_string(constants.ANSWER_VIEW).format(project_id=project_id,dataset_id=dataset_id,year_month=year_month)
    view_ref = try_create_view(view_id=view_id,query_str=q)

    return view_ref

def create_comment_view(project_id,dataset_id,year_month):

    view_id = f"{project_id}.{dataset_id}.calc_comment_{year_month}"
    q = convert_file_to_string(constants.COMMENT_VIEW).format(project_id=project_id,dataset_id=dataset_id,year_month=year_month)
    view_ref = try_create_view(view_id=view_id,query_str=q)

    return view_ref

def create_total_view(project_id,dataset_id,year_month):

    view_id = f"{project_id}.{dataset_id}.calc_total_{year_month}"
    q = convert_file_to_string(constants.TOTAL_VIEW).format(project_id=project_id,dataset_id=dataset_id,year_month=year_month)
    view_ref = try_create_view(view_id=view_id,query_str=q)

    return view_ref

def create_summary_view(project_id,dataset_id,year_month):

    view_id = f"{project_id}.{dataset_id}.calc_summary_{year_month}"
    q = convert_file_to_string(constants.SUMMARY_VIEW).format(project_id=project_id,dataset_id=dataset_id,year_month=year_month)
    view_ref = try_create_view(view_id=view_id,query_str=q)

    return view_ref

