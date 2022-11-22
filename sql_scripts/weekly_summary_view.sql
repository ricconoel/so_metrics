select
  t_total.post_week,
  coalesce(t_com.user_id,t_ans.user_id) as user_id,
  coalesce(t_com.display_name,t_ans.display_name) as display_name,
  (t_ans.answer_count/total_cases) * 100 as CWA,
  (t_ans.PAR_count/t_ans.answer_count) * 100 as PAR,
  t_ans.answer_count,
  t_ans.PAR_count,
  t_com.comment_count,
  t_total.total_cases,
from `{project_id}.{dataset_id}.calc_weekly_answer_{year_month}` t_ans
full outer join `{project_id}.{dataset_id}.calc_weekly_comment_{year_month}` t_com
  on t_ans.user_id = t_com.user_id and t_ans.week_date = t_com.week_date
full outer join `{project_id}.{dataset_id}.calc_weekly_total_{year_month}` t_total
  on t_total.user_id = coalesce(t_com.user_id,t_ans.user_id) and t_total.post_week = coalesce(t_com.week_date,t_ans.week_date)
where total_cases is not null
order by post_week
