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
