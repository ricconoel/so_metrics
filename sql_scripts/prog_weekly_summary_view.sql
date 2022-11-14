select 
  post_week,
  sum(answer_count) / sum(total_cases) * 100 as CWA,
  sum(PAR_count) / sum(answer_count) * 100 as PAR,
  sum(answer_count) as answer_count,
  sum(PAR_count) as PAR_count,
  sum(comment_count) as comment_count,
  sum(total_cases) as total_cases
from `{project_id}.{dataset_id}.calc_weekly_summary_{year_month}`
group by post_week