select
  date_trunc(cast(comment_date as date),week(sunday)) as week_date,
  user_id,
  display_name,
  count(distinct question_id) as comment_count,
from `{project_id}.{dataset_id}.comment_{year_month}`
  where post_type="question"
group by week_date,user_id,display_name
order by week_date