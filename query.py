from google.cloud import bigquery
from google.api_core.exceptions import NotFound

client = bigquery.Client()

def bq_create_view(view_id,query_str):
    print("Creating view")
    view = bigquery.Table(view_id)
    view.view_query = query_str
    view = client.create_table(view)
    print(f"Created {view.table_type}: {str(view.reference)}")

    return str(view.reference)


def answer_view(project_id,dataset_id,year_month):

    view_id = f"{project_id}.{dataset_id}.calc_answer_{year_month}"

    q = f"""
    select 
        concat(extract(year from answer_date),'-',extract(month from answer_date)) as ans_date,
        user_id,
        display_name,
        countif(is_accepted = true or score > 0) as PAR_count,
        count(answer_id) as answer_count,
    from `{project_id}.{dataset_id}.answer_{year_month}`
    group by ans_date,user_id,display_name
    """

    try:
        view = client.get_table(view_id)
        print(f"View {view_id} already exist")

        return view_id

    except NotFound as error:
        print(error.message)
        view_ref = bq_create_view(view_id=view_id,query_str=q)

        return view_ref

def comment_view(project_id,dataset_id,year_month):

    view_id = f"{project_id}.{dataset_id}.calc_comment_{year_month}"
    q = f"""
    select 
        concat(extract(year from comment_date),'-',extract(month from comment_date)) as com_date,
        user_id,
        display_name,
        count(distinct question_id) as comment_count,
    from `{project_id}.{dataset_id}.comment_{year_month}`
        where post_type="question"
    group by com_date,user_id,display_name
    """

    try:
        view = client.get_table(view_id)
        print(f"View {view_id} already exist")

        return view_id

    except NotFound as error:
        print(error.message)
        view_ref = bq_create_view(view_id=view_id,query_str=q)

        return view_ref

def total_view(project_id,dataset_id,year_month):

    view_id = f"{project_id}.{dataset_id}.calc_total_{year_month}"
    q = f"""
        with union_ans_com as (
            select 
                distinct
                concat(extract(year from answer_date),'-',extract(month from answer_date)) as post_date,
                user_id,
                display_name,
                question_id
            from `{project_id}.{dataset_id}.answer_{year_month}`
            union all
            select
                distinct
                concat(extract(year from comment_date),'-',extract(month from comment_date)) as post_date,
                user_id,
                display_name,
                question_id
            from `{project_id}.{dataset_id}.comment_{year_month}`
            where post_type="question"
        )

        select 
            post_date,
            user_id,
            display_name,
            count(distinct question_id) as total_cases
        from union_ans_com
        group by post_date,user_id,display_name
    """
    try:
        view = client.get_table(view_id)
        print(f"View {view_id} already exist")

        return view_id

    except NotFound as error:
        print(error.message)
        view_ref = bq_create_view(view_id=view_id,query_str=q)

        return view_ref

def summary_view(project_id,dataset_id,year_month):

    view_id = f"{project_id}.{dataset_id}.calc_summary_{year_month}"
    q = f"""
        select 
            coalesce(t_com.user_id,t_ans.user_id) as user_id,
            coalesce(t_com.display_name,t_ans.display_name) as display_name,
            (t_ans.answer_count/total_cases) * 100 as CWA,
            (t_ans.PAR_count/t_ans.answer_count) * 100 as PAR,
            t_ans.answer_count,
            t_ans.PAR_count,
            t_com.comment_count,
            t_total.total_cases,
  
        from `{project_id}.{dataset_id}.calc_answer_{year_month}` t_ans
        full outer join `{project_id}.{dataset_id}.calc_comment_{year_month}` t_com
            on t_ans.user_id = t_com.user_id
        full outer join `{project_id}.{dataset_id}.calc_total_{year_month}` t_total
            on t_total.user_id = coalesce(t_com.user_id,t_ans.user_id)
    """
    try:
        view = client.get_table(view_id)
        print(f"View {view_id} already exist")

        return view_id

    except NotFound as error:
        print(error.message)
        view_ref = bq_create_view(view_id=view_id,query_str=q)

        return view_ref

