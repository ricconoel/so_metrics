select
  date_trunc(cast(answer_date as date),week(sunday)) as week_date,
  user_id,
  display_name,
  countif(is_accepted = true or score > 0) as PAR_count,
  count(answer_id) as answer_count,
from `{project_id}.{dataset_id}.filtered_answer_{year_month}`
  group by week_date,user_id,display_name
order by week_date
