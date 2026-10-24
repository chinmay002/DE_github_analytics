"""(airflow_env) chinmaymaganur@Mac github_proj % airflow config get-value core dags_folder
/Users/chinmaymaganur/airflow/dags
(airflow_env) chinmaymaganur@Mac github_proj % export AIRFLOW__CORE__DAGS_FOLDER=/Users/chinmaymaganur/Desktop/github_proj/dags
(airflow_env) chinmaymaganur@Mac github_proj % airflow config get-value core dags_folder                                       
/Users/chinmaymaganur/Desktop/github_proj/dags

"""



from airflow.sdk import dag,task,get_current_context
from airflow.providers.standard.operators.bash import BashOperator

from pathlib import Path
from datetime import datetime,timedelta,timezone
import sys
import os

PROJECT_ROOT = Path(__file__).resolve().parent.parent #locate projet root. github_Proj
print(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT)) #add the about root_location to first place in sys.path to seach here firs.


from bq_connect import bq_connect,create_dataset,load_data,table_exists,merge_pull_data,merge_review_data,backfill_is_draft
from test import validate_pull,validate_temp
from helpers import get_ingestion_success_date,insert_ingetsion_row,upload_dataframe_parquet,ensure_bucket_exists,load_gcs_to_bigquery,update_ingestion_success
from main import *
from api import *
from dotenv import load_dotenv
load_dotenv()

project_id = os.getenv("project_id")
pull_temp = os.getenv("pull_temp")
review_temp = os.getenv("review_temp")
dataset_name = os.getenv("dataset_name")

default_args ={
    "retries":1,
    "retry_delay" :timedelta(seconds = 30)
}

@dag(
    dag_id = "github_pipeline_dag_id",
    schedule= None,
    start_date = datetime(2026, 9, 22),
    catchup = False,
    default_args = default_args,
    params={
        "run_type": "NORMAL",
        "backfill_start": None,
        "backfill_end": None,
        "backfill_reason": None
    }
)
def github_analytics_pipeline():

    @task
    def test_github():
        import requests

        r = requests.get(
            "https://api.github.com",
            timeout=(5, 15)
        )

        print(r.status_code)
        print(r.text[:200])

    @task()
    def init_pipeline():

        client = bq_connect(project_id)

        context = get_current_context()
        run_id = context["run_id"]
        params = context["params"]

        run_type = params["run_type"]
        backfill_start = params["backfill_start"]
        backfill_end = params["backfill_end"]
        backfill_reason = params["backfill_reason"]

        INITIAL_START_DATE = datetime(2026, 6, 1, tzinfo=timezone.utc)
        LOOKBACK = timedelta(days=2)

        if run_type == "NORMAL":

            last_watermark = get_ingestion_success_date(
                client,
                project_id,
                "ingestion_runs"
            )

            if last_watermark is None:
                extraction_from = INITIAL_START_DATE
            else:
                extraction_from = last_watermark - LOOKBACK

            extraction_to = datetime.now(timezone.utc)
            watermarked_at = extraction_to

        elif run_type == "BACKFILL":

            extraction_from = backfill_start
            extraction_to = backfill_end
            watermarked_at = None

        else:
            raise ValueError(f"Unknown run_type: {run_type}")

        print(f"Run type: {run_type}")
        print(f"Extraction from: {extraction_from}")
        print(f"Extraction to: {extraction_to}")
        print(f"Watermark: {watermarked_at}")

        metadata = {
            "run_id": run_id,
            "run_type": run_type,
            "extraction_from": extraction_from,
            "extraction_to": extraction_to,
            "watermarked_at": watermarked_at,
            "backfill_reason": backfill_reason,
            "completed_at": None,
            "pr_count": None,
            "review_count": None,
            "status": "Running"
        }

        stored_metadata = insert_ingetsion_row(
            client,
            project_id,
            "ingestion_runs",
            metadata
        )

        return stored_metadata
    # @task()
    # def init_pipeline():

    #     client = bq_connect(project_id)



    #     # last_watermark = get_ingestion_success_date(
    #     #     client,
    #     #     project_id,
    #     #     "ingestion_runs"
    #     # )

    #     # INITIAL_START_DATE = datetime(2026, 9, 19, tzinfo=timezone.utc)
    #     # LOOKBACK = timedelta(hours=1)
    #     INITIAL_START_DATE = datetime.now(timezone.utc) - timedelta(hours=6)
    #     LOOKBACK = timedelta(hours=6)
    #     last_watermark = None
    #     if last_watermark is None:
    #         extraction_from = INITIAL_START_DATE
    #     else:
    #         extraction_from = last_watermark - LOOKBACK

    #     watermarked_at = datetime.now(timezone.utc)

    #     context = get_current_context()
    #     run_id = context["run_id"]

    #     print(f"Last watermark: {last_watermark}")
    #     print(f"Extraction from: {extraction_from}")
    #     print(f"Current watermark: {watermarked_at}")

    #     metadata= {"run_id":run_id,
    #                 "watermarked_at":watermarked_at,
    #                 "extraction_from": extraction_from,
    #                 "completed_at": None,
    #                 "pr_count":None,
    #                 "review_count":None,
    #                 "status":"Running" }

        
    #     stored_metadata = insert_ingetsion_row(client,project_id,"ingestion_runs",metadata)
    #     return stored_metadata

            


    # @task
    # def extract_and_store_gcs(init_metadata):

    #     watermaked_at = init_metadata['watermarked_at']

    #     extraction_from = init_metadata["extraction_from"]
    #     extraction_to = init_metadata["extraction_to"]

    #     pulls_df, reviews_df, rejected_pulls = extract_data(
    #                                                         extraction_from=extraction_from,
    #                                                         extraction_to=extraction_to,
    #                                                         run_type=init_metadata["run_type"],
    #                                                         backfill_reason=init_metadata["backfill_reason"]
    #                                                     )

    #     context = get_current_context()
    #     run_id = context["run_id"]

    #     bucket_name = "github-analytics-staging"
    #     bucket = ensure_bucket_exists(project_id,bucket_name)

    #     pull_local = "/tmp/pull_requests.parquet"
    #     review_local = "/tmp/reviews.parquet"

    #     pull_gcs_path = f"runs/{run_id}/pull_requests.parquet"
    #     review_gcs_path = f"runs/{run_id}/reviews.parquet"

    #     upload_dataframe_parquet(
    #         bucket,
    #         pulls_df,
    #         pull_local,
    #         pull_gcs_path
    #     )

    #     upload_dataframe_parquet(
    #         bucket,
    #         reviews_df,
    #         review_local,
    #         review_gcs_path
    #     )

    #     return {
    #         **init_metadata,
    #         "pull_path": pull_gcs_path,
    #         "review_path": review_gcs_path,
    #         "pull_count": len(pulls_df),
    #         "review_count": len(reviews_df),
    #         "ingested_at": pulls_df["ingested_at"].iloc[0]
            
    #     }

    @task
    def extract_and_store_gcs(init_metadata):

        extraction_from = init_metadata["extraction_from"]
        extraction_to = init_metadata["extraction_to"]
        run_type = init_metadata["run_type"]
        backfill_reason = init_metadata["backfill_reason"]

        pulls_df, reviews_df, rejected_pulls = extract_data(
            extraction_from=extraction_from,
            extraction_to=extraction_to,
            run_type=run_type,
            backfill_reason=backfill_reason
        )

        if pulls_df.empty:
            raise RuntimeError("No pull requests extracted")

        context = get_current_context()
        run_id = context["run_id"]

        bucket_name = "github-analytics-staging"
        bucket = ensure_bucket_exists(project_id, bucket_name)

        pull_local = "/tmp/pull_requests.parquet"
        pull_gcs_path = f"runs/{run_id}/pull_requests.parquet"

        upload_dataframe_parquet(
            bucket,
            pulls_df,
            pull_local,
            pull_gcs_path
        )

        review_gcs_path = None
        review_count = 0

        if reviews_df is not None:
            review_local = "/tmp/reviews.parquet"
            review_gcs_path = f"runs/{run_id}/reviews.parquet"

            upload_dataframe_parquet(
                bucket,
                reviews_df,
                review_local,
                review_gcs_path
            )

            review_count = len(reviews_df)

        return {
            **init_metadata,
            "pull_path": pull_gcs_path,
            "review_path": review_gcs_path,
            "pull_count": len(pulls_df),
            "review_count": review_count,
            "rejected_count": len(rejected_pulls),
            "ingested_at": pulls_df["ingested_at"].iloc[0]
        }

   
    @task()
    def load_temporary_tables(metadata):

        client = bq_connect(project_id)

        bucket_name = "github-analytics-staging"

        pull_uri = f"gs://{bucket_name}/{metadata['pull_path']}"

        pull_rows = load_gcs_to_bigquery(
            client,
            pull_uri,
            project_id,
            "bronze",
            pull_temp
        )

        print("Expected pull rows:", metadata["pull_count"])
        print("Actual pull rows:", pull_rows)

        assert pull_rows == metadata["pull_count"]

        review_rows = 0

        if metadata["review_path"] is not None:

            review_uri = f"gs://{bucket_name}/{metadata['review_path']}"

            review_rows = load_gcs_to_bigquery(
                client,
                review_uri,
                project_id,
                "bronze",
                review_temp
            )

            print("Expected review rows:", metadata["review_count"])
            print("Actual review rows:", review_rows)

            assert review_rows == metadata["review_count"]

        return {
            **metadata,
            "pull_rows": pull_rows,
            "review_rows": review_rows
        }
    # @task()
    # def load_temporary_tables(metadata):

    #     client = bq_connect(project_id)

    #     bucket_name = "github-analytics-staging"

    #     pull_uri = ( f"gs://{bucket_name}/{metadata['pull_path']}")

    #     review_uri = (f"gs://{bucket_name}/{metadata['review_path']}")

    #     pull_rows = load_gcs_to_bigquery(
    #         client,
    #         pull_uri,
    #         project_id,
    #         "bronze",
    #         pull_temp
    #     )

    #     review_rows = load_gcs_to_bigquery(
    #         client,
    #         review_uri,
    #         project_id,
    #         "bronze",
    #         review_temp
    #     )

    #     print("Expected pull rows:", metadata["pull_count"])
    #     print("Actual pull rows:", pull_rows)

    #     print("Expected review rows:", metadata["review_count"])
    #     print("Actual review rows:", review_rows)

    #     assert pull_rows == metadata["pull_count"]

    #     assert review_rows == metadata["review_count"]

    #     return {
    #         **metadata,
    #         "pull_rows": pull_rows,
    #         "review_rows": review_rows,
    #         "run_id": metadata["run_id"],
    #     }

    @task
    def validate_temp_task():
        client = bq_connect(project_id)
        validate_temp(client)


    @task()
    def merge_pull_task(metadata):

        client = bq_connect(project_id)

        run_type = metadata["run_type"]
        backfill_reason = metadata["backfill_reason"]

        if run_type == "NORMAL":
            affected_rows = merge_pull_data(
                client,
                project_id,
                pull_table,
                pull_temp
            )

        elif run_type == "BACKFILL" and backfill_reason == "populate_is_draft":
            affected_rows = backfill_is_draft(
                client,
                project_id,
                pull_table,
                pull_temp
            )

        else:
            raise ValueError(
                f"Unsupported run: run_type={run_type}, backfill_reason={backfill_reason}"
            )

        print(f"PR affected rows: {affected_rows}")

        return affected_rows
    # @task()
    # def merge_pull_task():

    #     client = bq_connect(project_id)

    #     affected_rows = merge_pull_data(
    #         client,
    #         project_id,
    #         pull_table,
    #         pull_temp
    #     )

    #     print(f"PR MERGE affected rows: {affected_rows}")

    #     return affected_rows   
    @task()
    def merge_review_task(metadata):

        if metadata["review_path"] is None:
            print("No review dataset for this run. Skipping review MERGE.")
            return 0

        client = bq_connect(project_id)

        affected_rows = merge_review_data(
            client,
            project_id,
            review_table,
            review_temp
        )

        print(f"Review MERGE affected rows: {affected_rows}")

        return affected_rows

    # @task()
    # def merge_review_task():

    #     client = bq_connect(project_id)

    #     affected_rows = merge_review_data(
    #         client,
    #         project_id,
    #         review_table,
    #         review_temp
    #     )

    #     print(f"Review MERGE affected rows: {affected_rows}")

    #     return affected_rows

    @task()
    def post_merge_validation(metadata):

        client = bq_connect(project_id)

        run_type = metadata["run_type"]
        backfill_reason = metadata["backfill_reason"]

        # -------------------------
        # NORMAL INGESTION
        # -------------------------

        if run_type == "NORMAL":

            pr_query = f"""
            SELECT
                COUNT(*) AS mismatches
            FROM `{project_id}.{dataset_name}.{pull_temp}` t
            LEFT JOIN `{project_id}.{dataset_name}.{pull_table}` m
                ON t.id = m.id
            WHERE
                m.id IS NULL
                OR t.state IS DISTINCT FROM m.state
                OR t.updated_at IS DISTINCT FROM m.updated_at
                OR CAST(t.merged_at AS STRING) IS DISTINCT FROM m.merged_at
                OR CAST(t.closed_at AS STRING) IS DISTINCT FROM m.closed_at
                OR t.additions IS DISTINCT FROM m.additions
                OR t.deletions IS DISTINCT FROM m.deletions
                OR t.changed_files IS DISTINCT FROM m.changed_files
                OR t.is_draft IS DISTINCT FROM m.is_draft
            """

            review_query = f"""
            SELECT
                COUNT(*) AS mismatches
            FROM `{project_id}.{dataset_name}.{review_temp}` t
            LEFT JOIN `{project_id}.{dataset_name}.{review_table}` m
                ON t.review_id = m.review_id
            WHERE
                m.review_id IS NULL
                OR t.state IS DISTINCT FROM m.state
                OR t.pr_id IS DISTINCT FROM m.pr_id
                OR t.repo_id IS DISTINCT FROM m.repo_id
                OR t.pr_number IS DISTINCT FROM m.pr_number
                OR t.reviewer_login IS DISTINCT FROM m.reviewer_login
                OR t.submitted_at IS DISTINCT FROM m.submitted_at
            """

            pr_mismatches = list(client.query(pr_query).result())[0]["mismatches"]
            review_mismatches = list(client.query(review_query).result())[0]["mismatches"]

            print("PR mismatches:", pr_mismatches)
            print("Review mismatches:", review_mismatches)

            assert pr_mismatches == 0, f"{pr_mismatches} PR rows do not match temp"
            assert review_mismatches == 0, f"{review_mismatches} review rows do not match temp"

        # -------------------------
        # IS_DRAFT BACKFILL
        # -------------------------

        elif run_type == "BACKFILL" and backfill_reason == "populate_is_draft":

            validation_query = f"""
            SELECT
                COUNTIF(m.id IS NULL) AS missing_bronze_rows,
                COUNTIF(
                    m.id IS NOT NULL
                    AND t.is_draft IS DISTINCT FROM m.is_draft
                ) AS draft_mismatches
            FROM `{project_id}.{dataset_name}.{pull_temp}` t
            LEFT JOIN `{project_id}.{dataset_name}.{pull_table}` m
                ON t.id = m.id
            """

            result = list(client.query(validation_query).result())[0]

            missing_bronze_rows = result["missing_bronze_rows"]
            draft_mismatches = result["draft_mismatches"]

            print("Missing Bronze PRs:", missing_bronze_rows)
            print("is_draft mismatches:", draft_mismatches)

            assert draft_mismatches == 0, f"{draft_mismatches} is_draft values were not repaired"

            if missing_bronze_rows > 0:
                print(f"Warning: {missing_bronze_rows} PRs were not found in Bronze and were not inserted")

        else:
            raise ValueError(
                f"Unsupported validation: run_type={run_type}, backfill_reason={backfill_reason}"
            )

        completed_at = datetime.now(timezone.utc)

        return {
            **metadata,
            "completed_at": completed_at
        }

    
    # @task()
    # def post_merge_validation(metadata):

    #     client = bq_connect(project_id)

    #     pr_query = f"""
    #     SELECT
    #         COUNT(*) AS mismatches
    #     FROM `{project_id}.{dataset_name}.{pull_temp}` t
    #     LEFT JOIN `{project_id}.{dataset_name}.{pull_table}` m
    #         ON t.id = m.id
    #     WHERE
    #         m.id IS NULL
    #         OR t.state IS DISTINCT FROM m.state
    #         OR t.updated_at IS DISTINCT FROM m.updated_at
    #         OR cast(t.merged_at as string) IS DISTINCT FROM m.merged_at
    #         OR cast(t.closed_at as string) IS DISTINCT FROM m.closed_at
    #         OR t.additions IS DISTINCT FROM m.additions
    #         OR t.deletions IS DISTINCT FROM m.deletions
    #         OR t.changed_files IS DISTINCT FROM m.changed_files
    #         OR t.is_draft IS DISTINCT FROM m.is_draft
    #     """

    #     review_query = f"""
    #     SELECT
    #         COUNT(*) AS mismatches
    #     FROM `{project_id}.{dataset_name}.{review_temp}` t
    #     LEFT JOIN `{project_id}.{dataset_name}.{review_table}` m
    #         ON t.review_id = m.review_id
    #     WHERE
    #         m.review_id IS NULL                         
    #         OR t.state IS DISTINCT FROM m.state
    #         OR t.pr_id IS DISTINCT FROM m.pr_id
    #         OR t.repo_id IS DISTINCT FROM m.repo_id
    #         OR t.pr_number IS DISTINCT FROM m.pr_number
    #         OR t.reviewer_login IS DISTINCT FROM m.reviewer_login
    #         OR t.submitted_at IS DISTINCT FROM m.submitted_at
    #     """

    #     pr_mismatches = list(client.query(pr_query).result())[0]["mismatches"]
    #     review_mismatches = list(client.query(review_query).result())[0]["mismatches"]

    #     print("PR mismatches:", pr_mismatches)
    #     print("Review mismatches:", review_mismatches)

    #     assert pr_mismatches == 0, f"{pr_mismatches} PR rows do not match temp"
    #     assert review_mismatches == 0, f"{review_mismatches} review rows do not match temp"

    #     completed_at = datetime.now(timezone.utc)

    #     return {**metadata,"completed_at":completed_at}

    @task()
    def mark_ingestion_success(metadata):

        client = bq_connect(project_id)

        return update_ingestion_success(
            client,
            project_id,
            "ingestion_runs",
            metadata
    )

 
         

    init_metadata = init_pipeline()

    metadata = extract_and_store_gcs(init_metadata)

    loaded = load_temporary_tables(metadata)

    validated = validate_temp_task()

    pr_merge = merge_pull_task(metadata)
    review_merge = merge_review_task(metadata)

    post_validation = post_merge_validation(metadata)
    success = mark_ingestion_success(post_validation)

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"{PROJECT_ROOT}/dbt_env/bin/dbt build",
        cwd=str(PROJECT_ROOT / "github_analytics"),
        retries=0
    )

    dbt_src_freshness = BashOperator(
        task_id="dbt_src_freshness",
        bash_command=f"{PROJECT_ROOT}/dbt_env/bin/dbt source freshness",
        cwd=str(PROJECT_ROOT / "github_analytics"),
        retries=0
    )

    loaded >> validated
    validated >> [pr_merge, review_merge]
    [pr_merge, review_merge] >> post_validation
    post_validation >> success >> dbt_src_freshness >> dbt_build
    # init_metadata = init_pipeline()    

    # metadata = extract_and_store_gcs(init_metadata)
    
    # loaded = load_temporary_tables(metadata)

    # validated = validate_temp_task()

    # pr_merge = merge_pull_task()
    # review_merge = merge_review_task()
    # post_validation = post_merge_validation(metadata)
    # success = mark_ingestion_success(metadata)

    # dbt_build = BashOperator(task_id="dbt_build",bash_command=f"{PROJECT_ROOT}/dbt_env/bin/dbt build",cwd=str(PROJECT_ROOT / "github_analytics"),retries=0)
    # dbt_src_freshness = BashOperator(task_id="dbt_src_freshness",bash_command=f"{PROJECT_ROOT}/dbt_env/bin/dbt source freshness",cwd=str(PROJECT_ROOT / "github_analytics"),retries=0)
        
    # loaded >> validated
    # validated >> [pr_merge, review_merge]
    # [pr_merge, review_merge] >> post_validation
    # post_validation >> success >>dbt_src_freshness>>dbt_build

    



    # init_metadata>>metadata
   

github_analytics_pipeline()


   # @task
    # def update_ingestion_table(init_metadata,metadata,post_valid_meta):
    #     watermaked_at = init_metadata['watermarked_at']
    #     extraction_from = init_metadata['extraction_from']
    #     init_run_id = init_metadata['init_run_id']
    #     completed_at = post_valid_meta['completed_at']
    #     status = "SUCCESS"

    #     #update_table(client,project_id,table_name)


    # metadata = extract_and_store_gcs(init_metadata)
    
        # loaded = load_temporary_tables(metadata)
    
        # validated = validate_temp_task()
    
        # pr_merge = merge_pull_task()
        # review_merge = merge_review_task()
        # post_validation = post_merge_validation()
        # final = update_ingestion_table(init_metadata,metadata,post_valid_meta)

         # loaded >> validated
            # validated >> [pr_merge, review_merge]
            # [pr_merge, review_merge] >> post_validation
        

    #final = update_ingestion_table(init_metadata,metadata,post_valid_meta)
    