# so_metrics

Steps to set up:

1. `git clone https://github.com/ricconoel/so_metrics.git`
2. Edit the following in `constants.py` to point to your project:
   - BUCKET_NAME
   - PROJECT_ID
   - DATASET_ID
3. `pip install -r requirements.txt`
4. Load https://github.com/ricconoel/so_metrics/blob/main/gcp_tags/valid_gcp_tags.csv to your BQ project
5. Edit [this](https://github.com/ricconoel/so_metrics/blob/main/sql_scripts/filter_answer_view.sql#L26) locally to point to your created table from step 4.
6. python `main.py`

**NOTE:**
Make sure that `BUCKET_NAME` is existing prior to running `main.py`.
