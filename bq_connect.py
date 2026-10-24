##bqconnect.py

import os
from google.cloud import bigquery
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
load_dotenv()


def backfill_is_draft(client, project_id, pull_table, pull_temp):

    query = f"""
    MERGE `{project_id}.bronze.{pull_table}` target
    USING `{project_id}.bronze.{pull_temp}` source
    ON target.id = source.id

    WHEN MATCHED THEN
        UPDATE SET
            target.is_draft = source.is_draft
    """

    result = client.query(query).result()

    return result.num_dml_affected_rows


def bq_connect(project_id):
    print(f'project_id passed {project_id}')
    try:
        client = bigquery.Client(project=project_id)
        client.query("SELECT 1").result()

        print("BigQuery connection successful")
        return client

    except Exception as e:
        print(f"BigQuery connection failed: {e}")
        return None

def create_dataset(client, project_id, dataset_name):

    dataset_id = f"{project_id}.{dataset_name}"

    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"

    client.create_dataset(dataset, exists_ok=True)

    print(f"Dataset {dataset_id} is ready")

def load_data(
    client,
    project_id,
    dataset_name,
    table_name,
    df,
    ops
):
    table_id = f"{project_id}.{dataset_name}.{table_name}"

    if not table_exists(client, project_id, table_name):
        print(f"{table_name} does not exist. Creating it.")

    job_config = bigquery.LoadJobConfig(
        write_disposition=ops,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED
    )

    job = client.load_table_from_dataframe(
        df,
        table_id,
        job_config=job_config
    )

    job.result()

    print(f"Loaded {len(df)} rows into {table_id}")
# def load_data(client, project_id, dataset_name, table_name, df,ops):

#     table_id = f"{project_id}.{dataset_name}.{table_name}"

#     job_config = bigquery.LoadJobConfig(
#         write_disposition=f"{ops}"
#     )

#     job = client.load_table_from_dataframe(
#         df,
#         table_id,
#         job_config=job_config
#     )

#     job.result()

#     print(f"Loaded {len(df)} rows into {table_id}")


def table_exists(client, project_id,table_name):
    table_id = f"{project_id}.bronze.{table_name}"

    try:
        client.get_table(table_id)
        print(f"table {table_id} exiists")

        return True
    except NotFound:
        print(f"table {table_id} does not. exiists")
        return False


def merge_pull_data(client, project_id, pull_table, pull_temp):

    print("\nMerging pull data\n")
    print(f"Target: {project_id}.bronze.{pull_table}")
    print(f"Source: {project_id}.bronze.{pull_temp}")

    query = f"""
        merge `{project_id}.bronze.{pull_table}` as target

        using `{project_id}.bronze.{pull_temp}` as source

        on target.id = source.id

        when matched
            and source.updated_at > target.updated_at
        then update set
            state = source.state,
            is_draft = source.is_draft,
            updated_at = cast(source.updated_at as string),
            merged_at = cast(source.merged_at as string),
            closed_at = cast(source.closed_at as string),
            additions = source.additions,
            deletions = source.deletions,
            changed_files = source.changed_files,
            ingested_at = cast(source.ingested_at as string)

        when not matched then
            insert (
                id,
                number,
                state,
                title,
                is_draft,
                created_at,
                updated_at,
                merged_at,
                closed_at,
                user_id,
                login,
                repo_id,
                repo_name,
                author_association,
                additions,
                deletions,
                changed_files,
                ingested_at
            )
            values (
                source.id,
                source.number,
                source.state,
                source.title,
                source.is_draft,
                cast(source.created_at as string),
                cast(source.updated_at as string),
                cast(source.merged_at as string),
                cast(source.closed_at as string),
                source.user_id,
                source.login,
                source.repo_id,
                source.repo_name,
                source.author_association,
                source.additions,
                source.deletions,
                source.changed_files,
                cast(source.ingested_at as string)
            )
    """

    query_job = client.query(query)
    query_job.result()
    result = query_job.num_dml_affected_rows

    print(f"PR merge complete. Affected rows: {query_job.num_dml_affected_rows}")
    return result

def merge_review_data(client, project_id,main_table,temp_table):

    print("\nMerging review data\n")

    print(f"Target: {project_id}.bronze.{main_table}")
    print(f"Source: {project_id}.bronze.{temp_table}")

    test_query = f"""
    SELECT *
    FROM `{project_id}.bronze.{temp_table}`
    LIMIT 5
    """

    rows = client.query(test_query).result()
    for row in rows:
        print(row)



    query = f"""
        merge `{project_id}.bronze.{main_table}` as target

        using (
            select *
            from `{project_id}.bronze.{temp_table}`

            qualify row_number() over (
                partition by review_id
                order by submitted_at desc
            ) = 1
        ) as source

        on target.review_id = source.review_id

        when matched
            and source.state != target.state
        then update set
            pr_id = source.pr_id,
            repo_id = source.repo_id,
            pr_number = source.pr_number,
            reviewer_login = source.reviewer_login,
            state = source.state,
            submitted_at = cast(source.submitted_at as string),
            ingested_at = cast(source.ingested_at as string)

        when not matched then
            insert (
                review_id,
                pr_id,
                repo_id,
                pr_number,
                reviewer_login,
                state,
                submitted_at,
                ingested_at
            )
            values (
                source.review_id,
                source.pr_id,
                source.repo_id,
                source.pr_number,
                source.reviewer_login,
                source.state,
                cast(source.submitted_at as string),
                cast(source.ingested_at as string)
            )
    """

    query_job = client.query(query)
    query_job.result()

    print(
        f"Review merge complete. "
        f"Affected rows: {query_job.num_dml_affected_rows}"
    )