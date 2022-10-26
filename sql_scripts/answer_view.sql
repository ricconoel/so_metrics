select
  concat(extract(year from answer_date),'-',extract(month from answer_date)) as ans_date,
  user_id,
  display_name,
  countif(is_accepted = true or score > 0) as PAR_count,
  count(answer_id) as answer_count,
from `{project_id}.{dataset_id}.answer_{year_month}`
group by ans_date,user_id,display_name
