"""(airflow_env) chinmaymaganur@Mac github_proj % airflow config get-value core dags_folder
/Users/chinmaymaganur/airflow/dags
(airflow_env) chinmaymaganur@Mac github_proj % export AIRFLOW__CORE__DAGS_FOLDER=/Users/chinmaymaganur/Desktop/github_proj/dags
(airflow_env) chinmaymaganur@Mac github_proj % airflow config get-value core dags_folder                                       
/Users/chinmaymaganur/Desktop/github_proj/dags

"""



from airflow.sdk import dag,task,get_current_context
from airflow.providers.standard.operators.bash import BashOperator

from pathlib import Path
from datetime import datetime,timedelta
import sys
import os

PROJECT_ROOT = Path(__file__).resolve().parent.parent #locate projet root. github_Proj
print(PROJECT_ROOT)
sys.path.insert(0, str(PROJECT_ROOT)) #add the about root_location to first place in sys.path to seach here firs.

from api import *
from bq_connect import bq_connect,create_dataset,load_data,table_exists,merge_pull_data,merge_review_data
from test import validate_pull,validate_temp
from helpers import *
from main import *
from test import validate_pull,validate_temp
from dotenv import load_dotenv
load_dotenv()

project_id = os.getenv("project_id")
pull_temp = os.getenv("pull_temp")
review_temp = os.getenv("review_temp")
dataset_name = os.getenv("dataset_name")

default_args ={
    "retries":1,
    "retry_delay" :timedelta(seconds = 60)
}

@dag(
    dag_id = "github_pipeline_dag_id",
    schedule= None,
    start_date = datetime(2026, 9, 13),
    catchup = False,
    default_args = default_args,
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

        last_watermark = get_ingestion_success_date(
            client,
            project_id,
            "ingestion_runs"
        )

        INITIAL_START_DATE = datetime(2026, 6, 1,tzinfo=timezone.utc)

        if last_watermark is None:
            extraction_from = INITIAL_START_DATE
        else:
            extraction_from = last_watermark - timedelta(days=2)

        watermarked_at = datetime.now(timezone.utc)

        context = get_current_context()
        run_id = context["run_id"]

        print(f"Last watermark: {last_watermark}")
        print(f"Extraction from: {extraction_from}")
        print(f"Current watermark: {watermarked_at}")

        metadata= {"init_run_id":run_id,
                    "watermarked_at":watermarked_at,
                    "extraction_from": extraction_from,
                    "completed_at": None,
                    "pr_count":None,
                    "review_count":None,
                    "status":"Running" }

        
        stored_metadata = insert_ingetsion_row(client,project_id,"ingestion_runs",metadata)
        return stored_metadata

            


    @task
    def extract_and_store_gcs(init_metadata):

        watermaked_at = init_metadata['watermarked_at']
        extraction_from = init_metadata['extraction_from']


        pulls_df, reviews_df = extract_data()

        context = get_current_context()
        run_id = context["run_id"]

        bucket_name = "github-analytics-staging"
        bucket = ensure_bucket_exists(project_id,bucket_name)

        pull_local = "/tmp/pull_requests.parquet"
        review_local = "/tmp/reviews.parquet"

        pull_gcs_path = f"runs/{run_id}/pull_requests.parquet"
        review_gcs_path = f"runs/{run_id}/reviews.parquet"

        upload_dataframe_parquet(
            bucket,
            pulls_df,
            pull_local,
            pull_gcs_path
        )

        upload_dataframe_parquet(
            bucket,
            reviews_df,
            review_local,
            review_gcs_path
        )

        return {
            "run_id": run_id,
            "pull_path": pull_gcs_path,
            "review_path": review_gcs_path,
            "pull_count": len(pulls_df),
            "review_count": len(reviews_df),
            "ingested_at": pulls_df["ingested_at"].iloc[0]
        }

    """
    {
    "run_id": "manual__2026-09-14T13:21:23.981214+00:00",
    "pull_path": "runs/manual__2026-09-14T13:21:23.981214+00:00/pull_requests.parquet",
    "review_path": "runs/manual__2026-09-14T13:21:23.981214+00:00/reviews.parquet",
    "pull_count": 4,
    "review_count": 11,
    "ingested_at": "2026-09-14T13:21:25.926510+00:00"
}
    """
    
    @task()
    def load_temporary_tables(metadata):

        client = bq_connect(project_id)

        bucket_name = "github-analytics-staging"

        pull_uri = ( f"gs://{bucket_name}/{metadata['pull_path']}")

        review_uri = (f"gs://{bucket_name}/{metadata['review_path']}")

        pull_rows = load_gcs_to_bigquery(
            client,
            pull_uri,
            project_id,
            "bronze",
            pull_temp
        )

        review_rows = load_gcs_to_bigquery(
            client,
            review_uri,
            project_id,
            "bronze",
            review_temp
        )

        print("Expected pull rows:", metadata["pull_count"])
        print("Actual pull rows:", pull_rows)

        print("Expected review rows:", metadata["review_count"])
        print("Actual review rows:", review_rows)

        assert pull_rows == metadata["pull_count"]

        assert review_rows == metadata["review_count"]

        return {
            "pull_rows": pull_rows,
            "review_rows": review_rows,
            "run_id": metadata["run_id"]
        }

    @task
    def validate_temp_task():
        client = bq_connect(project_id)
        validate_temp(client)

    @task()
    def merge_pull_task():

        client = bq_connect(project_id)

        affected_rows = merge_pull_data(
            client,
            project_id,
            pull_table,
            pull_temp
        )

        print(f"PR MERGE affected rows: {affected_rows}")

        return affected_rows   

    @task()
    def merge_review_task():

        client = bq_connect(project_id)

        affected_rows = merge_review_data(
            client,
            project_id,
            review_table,
            review_temp
        )

        print(f"Review MERGE affected rows: {affected_rows}")

        return affected_rows


    @task()
    def post_merge_validation():

        client = bq_connect(project_id)

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
            OR cast(t.merged_at as string) IS DISTINCT FROM m.merged_at
            OR cast(t.closed_at as string) IS DISTINCT FROM m.closed_at
            OR t.additions IS DISTINCT FROM m.additions
            OR t.deletions IS DISTINCT FROM m.deletions
            OR t.changed_files IS DISTINCT FROM m.changed_files
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

        completed_at = datetime.now(tzinfo=timezone.utc)

        return {"completed_at":completed_at}

 
         


    init_metadata = init_pipeline()    

    



    init_metadata
   

github_analytics_pipeline()

        