##main.py
from api import *
from bq_connect import bq_connect,create_dataset,load_data,table_exists,merge_pull_data,merge_review_data
import os
from test import validate_pull,validate_temp
from datetime import datetime, timezone

from dotenv import load_dotenv



load_dotenv()


project_id = os.getenv("project_id")
dataset_name = os.getenv('dataset_name')
pull_table = os.getenv("pull_table")
pull_temp = os.getenv("pull_temp")

review_table=os.getenv("review_table")
review_temp=os.getenv('review_temp')


def extract_data(extraction_from, extraction_to, run_type, backfill_reason):
    ingested_at = datetime.now(timezone.utc).isoformat()

    pulls, reviews, rejected_pulls = get_pulls(
        "apache",
        "airflow",
        extraction_from,
        extraction_to,
        run_type,
        backfill_reason
    )

    pulls_df = pd.DataFrame(pulls)
    pulls_df["ingested_at"] = ingested_at

    reviews_df = None

    if reviews:
        reviews_df = pd.DataFrame(reviews)
        reviews_df["ingested_at"] = ingested_at

        
    print("\ndata extracted successfully\n")
    return pulls_df, reviews_df

def load_temp_tables(client, pulls_df, reviews_df):
    load_data(
        client,
        project_id,
        "bronze",
        pull_temp,
        pulls_df,
        "WRITE_TRUNCATE"
    )

    load_data(
        client,
        project_id,
        "bronze",
        review_temp,
        reviews_df,
        "WRITE_TRUNCATE"
    )


def upsert_pull_data(client, project_id, df, main_table, temp_table):
    print("\nProcessing pull request data")

    if table_exists(client, project_id, main_table):

        print("Pull request table exists")

        upds, inserts = validate_pull(client)

        print(
            f"Expected PR updates = {upds}, "
            f"expected inserts = {inserts}"
        )

        affected_rows = merge_pull_data(
            client,
            project_id,
            main_table,
            temp_table
        )

        print(f"Actual affected PR rows = {affected_rows}")

        assert affected_rows == upds + inserts, (
            f"Expected {upds + inserts} affected PR rows "
            f"({upds} updates + {inserts} inserts), "
            f"but MERGE affected {affected_rows}"
        )

    else:
        print("Pull request table does not exist. Creating initial table for the FIRST RUN.")

        load_data(
            client,
            project_id,
            "bronze",
            main_table,
            df,
            "WRITE_TRUNCATE"
        )


def upsert_review_data(client, project_id, df, main_table, temp_table):
    print("\nProcessing review data")

    if table_exists(client, project_id, main_table):

        print("Review table exists")

        # upds, inserts = validate_reviews(client)

        # print(
        #     f"Expected review updates = {upds}, "
        #     f"expected inserts = {inserts}"
        # )

        affected_rows = merge_review_data(
            client,
            project_id,
            main_table,
            temp_table
        )

        print(f"Actual affected review rows = {affected_rows}")

        # assert affected_rows == upds + inserts, (
        #     f"Expected {upds + inserts} affected review rows "
        #     f"({upds} updates + {inserts} inserts), "
        #     f"but MERGE affected {affected_rows}"
        # )

    else:
        print("Review table does not exist. Creating initial table.")

        load_data(
            client,
            project_id,
            "bronze",
            main_table,
            df,
            "WRITE_TRUNCATE"
        )


def merge_tables(client):
    """
    Use this when you want to test MERGE behavior
    without calling GitHub or reloading temp tables.
    """

    print("\nRunning PR MERGE only")

    pr_rows = merge_pull_data(
        client,
        project_id,
        pull_table,
        pull_temp
    )

    print("\nRunning review MERGE only")

    review_rows = merge_review_data(
        client,
        project_id,
        review_table,
        review_temp
    )

    return pr_rows, review_rows




def merge_tables(client):
    pr_rows = merge_pull_data(
        client,
        project_id,
        pull_table,
        pull_temp
    )

    review_rows = merge_review_data(
        client,
        project_id,
        review_table,
        review_temp
    )

    return pr_rows, review_rows


def main():

    client = bq_connect(project_id)

    '''
    #idempotency test
    pr_rows, review_rows = merge_tables(client)

    print("PR affected:", pr_rows)
    print("Review affected:", review_rows)
    '''

    create_dataset(client,project_id,dataset_name)

    # 1. Extract
    pulls_df, reviews_df = extract_data()
    print(f"length of pull_df {pulls_df.shape}")
    print(f"length of pull_df {reviews_df.shape}")

    # 2. Load temp
    load_temp_tables(client,pulls_df,reviews_df)
    # validate_temp(client)

    # 3. Merge temp -> main
    upsert_pull_data(client,project_id,pulls_df,pull_table,pull_temp)

    upsert_review_data(client, project_id,reviews_df,review_table,review_temp)
    



# if __name__=="__main__":
#     ingested_at = datetime.now(timezone.utc)
    
#     main()