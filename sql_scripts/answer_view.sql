with t1 as (select user_id,
    display_name,
    answer_id,
    question_id,
    is_accepted,
    score,
    answer_date,
    max(if(is_downvoted = 'true','true', 'false' )) over (partition by display_name) as with_downvote
from `{project_id}.{dataset_id}.filtered_answer_{year_month}`)

 
select
  concat(extract(year from answer_date),'-',extract(month from answer_date)) as ans_date,
  user_id,
  display_name,
  countif(is_accepted = true or score > 0) as PAR_count,
  count(answer_id) as answer_count, 
  with_downvote
  from t1
  group by ans_date,user_id,display_name,with_downvote;
