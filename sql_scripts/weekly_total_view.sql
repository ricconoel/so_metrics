with union_ans_com as (
  select
    distinct
    date_trunc(cast(answer_date as date),week(sunday)) as post_week,
    user_id,
    display_name,
    question_id
  from `{project_id}.{dataset_id}.filtered_answer_{year_month}`
  union all
  select
    distinct
    date_trunc(cast(comment_date as date),week(sunday)) as post_week,
    user_id,
    display_name,
    question_id
  from `{project_id}.{dataset_id}.comment_{year_month}`
  where post_type="question"
),
remove_duplicates as (
  select 
    * 
  from 
    (
      select *, 
      row_number() over (partition by user_id,question_id order by user_id) rn
      from union_ans_com
    )
  where rn = 1
)
/*
- remove duplicates is needed because of the following scenario:
  - post_week = 2022-10-30, comment_date = 2022-11-04, user_id = 111
    - comment on question A
    - This will count as total_case + 1
  - post_week = 2022-11-06, comment_date = 2022-11-07, user_id = 111
    - comment on question A
    - This will count as total_case + 1
- scenario above gives total_case = 2 for user_id = 111 since user commented on the same case on different weeks
- hence removing duplicates is needed
*/

select
  post_week,
  user_id,
  display_name,
  count(distinct question_id) as total_cases
from remove_duplicates
group by post_week,user_id,display_name
order by post_week