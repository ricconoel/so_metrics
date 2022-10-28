select distinct 
  a_t.user_id,
  a_t.display_name,
  a_t.answer_id,
  a_t.question_id,
  a_t.is_accepted,
  a_t.score,
  a_t.answer_date,
  g_t.is_valid
from (
  select
    user_id,
    display_name,
    answer_id,
    question_id,
    is_accepted,
    score,
    answer_date,
    t 
  from `{project_id}.{dataset_id}.answer_{year_month}`,
  unnest(tags) t
) a_t
inner join `tiph-ricconoel-batch8.so_dataset.gcp_tags` g_t
  on g_t.tags = a_t.t
order by answer_date
