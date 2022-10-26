select
  concat(extract(year from comment_date),'-',extract(month from comment_date)) as com_date,
  user_id,
  display_name,
  count(distinct question_id) as comment_count,
from `{project_id}.{dataset_id}.comment_{year_month}`
  where post_type="question"
group by com_date,user_id,display_name
