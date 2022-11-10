Tables created are the following:

1. answer_YYYY_MM
2. comment_YYYY_MM
3. filtered_answer_YYYY_MM
4. calc_answer_YYYY_MM
5. calc_comment_YYYY_MM
6. calc_total_YYYY_MM
7. calc_summary_YYYY_MM

## Details per table:

### answer_YYYY_MM
- Contains raw data fetched from Stackoverflow API.
- Used endpoint [/users/{ids}/answers](https://api.stackexchange.com/docs/answers-on-users) which returns the answers the users in {ids} have posted.
- Contains all answers of TSRs.
### comment_YYYY_MM
- Contains raw data fetched from Stackoverflow API.
- Used endpoint [/users/{ids}/comments](https://api.stackexchange.com/docs/comments-on-users) which returns the comments posted by users in {ids}.
- Contains all comments (comments on answers and comment on questions) made by TSRs.
### filtered_answer_YYYY_MM
- Removed answers that are not related to GCP.
- Only answers that have valid GCP tags are shown.
### calc_answer_YYYY_MM
- Calculation done in this table references `filtered_answer_YYYY_MM`.
- Gets the `PAR_count` and `answer_count`.
- `PAR_Count` increases if an upvote or accept is made on the answer.
- `answer_count` increases if TSR posted an answer.
### calc_comment_YYYY_MM
- Calculation done in this table references `comment_YYYY_MM`.
- Gets `comment_count`.
- `comment_count` increases if TSR posted a comment on a **question**. 
   - If a TSR posted multiple comments in a question, `comment_count` counts it as 1.
### calc_total_YYYY_MM
- Calculation done in this table references `filtered_answer_YYYY_MM` and `comment_YYYY_MM`.
- Gets `total_cases`.
- `total_cases` is the sum of `answer_count` and `comment_count`.
### calc_summary_YYYY_MM
- Calculation done in this table references `calc_answer_YYYY_MM`, `calc_comment_YYYY_MM` and `calc_total_YYYY_MM`.
- Calculated `CWA` and `PAR`.
- Combination of fields from the said tables from above.
