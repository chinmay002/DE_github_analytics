import requests, os,time
from google.cloud import storage
from pathlib import Path
from datetime import datetime,timedelta
from bq_connect import table_exists
from datetime import datetime, timezone

def make_api_requests(url,headers,params=None,max_tries = 3):
    retryable_status_codes = [429, 500, 502, 503]

    for trys in range(0,max_tries):
            
        try:
            response = requests.get(url, headers=headers, params=params)
            
                 

            if response.status_code == 200:
                return response.json()

            if response.status_code in retryable_status_codes:
                wait_time = 2 ** trys

                if response.status_code ==429:
                    retry_after = response.headers.get('retry_after')

                    if retry_after:
                        wait_time = int(retry_after)

                print(f"Temporary API error {response.status_code}. "
                      f"Retrying in {wait_time}s...")
                
                time.sleep(wait_time)
                continue

            response.raise_for_status()  


        except requests.exceptions.RequestException as e:
                    print(f"API requesr failed {e}")
                    print(f"APU try used {trys}/{max_tries}")
                    if trys < max_tries-1:
                        time.sleep(2**trys)

    raise RuntimeError(f"API request failed after {max_tries} attempts: {url}")

def filter_pr_page(data, extraction_from, extraction_to):  # #data by max_per_page page 1: [pr1,pr2,pr3,pr4,pr5]--->[pr4 ,PR5 OLD]
    valid_prs = []
    boundary_crossed = False

    for pr in data:

        updated_at = datetime.fromisoformat(
            pr.get("updated_at").replace("Z", "+00:00")
        )
        if updated_at > extraction_to:
            continue

        if updated_at >= extraction_from:
            valid_prs.append(pr)
        else:
            boundary_crossed = True
            break

    return valid_prs, boundary_crossed

    #     if updated_at >= boundary:
    #         valid_prs.append(pr)
    #     else:
    #         boundary_crossed = True
    #         break

    # return valid_prs, boundary_crossed


def get_paginated_data(
    url,
    headers,
    params=None,
    per_page=5,
    max_pages=None,
    page_filter = None,
    extraction_from=None,
    extraction_to=None):


    all_data = []
    page = 1

    # Don't modify the original dictionary passed by the caller
    params = params.copy() if params else {}

    while True:
        params["per_page"] = per_page
        params["page"] = page

        print(url,params)

        data = make_api_requests(
            url,
            headers=headers,
            params=params
        )  

        if page_filter:

            valid_prs, boundary_crossed = page_filter(data,extraction_from, extraction_to)

        else:
            valid_prs = data
            boundary_crossed = False

        all_data.extend(valid_prs)    #[PR1,Pr2,Pr3,pr3.....]
        print(
                f"Page {page}: "
                f"API rows={len(data)}, "
                f"valid rows={len(valid_prs)}, "
                f"boundary_crossed={boundary_crossed}"
            )
                


        # GitHub returned less than a full page -> last page
        if len(data) < per_page:
            break

        # Optional development limit
        if max_pages is not None and page >= max_pages:
            break
        
        if  boundary_crossed:
            break


        
        

        page += 1


    return all_data

        


def ensure_bucket_exists(project_id,name):

    client = storage.Client(project=project_id)
    bucket = client.bucket(name)

    if bucket.exists():
        print(f"Bucket {name} exists")
    else:
        print(f"Bucket {name} does not exist. Creating it.")
        bucket = client.create_bucket(name)
        print(f"Bucket {name} created")

    return bucket



def upload_dataframe_parquet(bucket, df, local_path, gcs_path):

    local_path = Path(local_path)

    # 1. Save DataFrame locally as parquet
    df.to_parquet(local_path, index=False)

    # 2. Create blob reference in GCS
    blob = bucket.blob(gcs_path)

    # 3. Upload local file to GCS
    blob.upload_from_filename(str(local_path))

    print(f"Uploaded to gs://{bucket.name}/{gcs_path}")


from google.cloud import bigquery

def load_gcs_to_bigquery(
    client,
    gcs_uri,
    project_id,
    dataset_name,
    table_name
):
    destination_table = f"{project_id}.{dataset_name}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )

    load_job = client.load_table_from_uri(
        gcs_uri,
        destination_table,
        job_config=job_config
    )

    load_job.result()

    table = client.get_table(destination_table)

    print(
        f"Loaded {table.num_rows} rows "
        f"into {destination_table}"
    )

    return table.num_rows




def get_ingestion_success_date(client, project_id, table_name):
    print('\n entered ingestion_sucess_date')

    # Create table if it doesn't exist
    query = f"""
        CREATE TABLE IF NOT EXISTS `{project_id}.bronze.{table_name}` (
            run_id STRING NOT NULL,
            watermarked_at TIMESTAMP,
            extraction_from TIMESTAMP,
            completed_at TIMESTAMP,
            pr_count INT64,
            review_count INT64,
            status STRING
        )
    """

    query_job = client.query(query)
    query_job.result()

    print(f"Table {table_name} verified or created successfully.")


    # Get latest successful watermark
    query = f"""
        SELECT watermarked_at
        FROM `{project_id}.bronze.{table_name}`
        WHERE status = 'SUCCESS'
        ORDER BY watermarked_at DESC
        LIMIT 1
    """

    query_job = client.query(query)
    results = query_job.result()


    if results.total_rows > 0:

        for row in results:
            watermark = row["watermarked_at"]

        print(f"Latest successful watermark: {watermark}")

        return watermark

    else:

        # First pipeline run
       

        print(f"No successful runs. Using initial date: ")

        return None


def insert_ingetsion_row(client,project_id,table_name,metadata):

    job_config = bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("run_id", "STRING", metadata["run_id"]),
                                                           bigquery.ScalarQueryParameter("watermarked_at", "TIMESTAMP", metadata["watermarked_at"]),
                                                           bigquery.ScalarQueryParameter("extraction_from", "TIMESTAMP", metadata["extraction_from"]),
                                                           bigquery.ScalarQueryParameter("completed_at", "TIMESTAMP", metadata["completed_at"]),
                                                           bigquery.ScalarQueryParameter("pr_count", "INT64", metadata["pr_count"]),
                                                           bigquery.ScalarQueryParameter("review_count", "INT64", metadata["review_count"]),
                                                           bigquery.ScalarQueryParameter("status", "STRING", metadata["status"]),
                                                           ])

    merge_sql = f"""
                MERGE `{project_id}.bronze.{table_name}` T
                USING (SELECT @run_id as run_id) S
                on T.run_id = S.run_id
                WHEN NOT MATCHED THEN
                    INSERT(run_id ,
                        watermarked_at,
                        extraction_from,
                        completed_at,
                        pr_count,
                        review_count,
                        status)
                    
                    VALUES(@run_id ,
                        @watermarked_at,
                        @extraction_from,
                        @completed_at,
                        @pr_count,
                        @review_count,
                        @status)

        """
    
    query_job = client.query(merge_sql,job_config = job_config)
    results = query_job.result()

    print(
    f"Ingestion initialization complete. "
    f"Affected rows: {query_job.num_dml_affected_rows}"
)

    query = f"""Select * from `{project_id}.bronze.{table_name}`
                WHERE run_id = @run_id
            """

    query_job_ = client.query(query,job_config = job_config)
    results_ = query_job_.result()

    row = next(iter(results_), None)

    if row is None:
        raise ValueError(f"No ingestion row found for run_id: {metadata['run_id']}")

    return dict(row)


from google.cloud import bigquery
from datetime import datetime, timezone


def update_ingestion_success(client, project_id, table_name, metadata):

    completed_at = datetime.now(timezone.utc)

    query = f"""
        UPDATE `{project_id}.bronze.{table_name}`
        SET
            completed_at = @completed_at,
            pr_count = @pr_count,
            review_count = @review_count,
            status = 'SUCCESS'
        WHERE run_id = @run_id
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "completed_at",
                "TIMESTAMP",
                metadata["completed_at"]
            ),
            bigquery.ScalarQueryParameter(
                "pr_count",
                "INT64",
                metadata["pr_count"]
            ),
            bigquery.ScalarQueryParameter(
                "review_count",
                "INT64",
                metadata["review_count"]
            ),
            bigquery.ScalarQueryParameter(
                "run_id",
                "STRING",
                metadata["run_id"]
            ),
        ]
    )

    query_job = client.query(query, job_config=job_config)
    query_job.result()

    affected_rows = query_job.num_dml_affected_rows

    if affected_rows != 1:
        raise ValueError(
            f"Expected to update 1 ingestion row for "
            f"run_id={metadata['run_id']}, but updated {affected_rows}"
        )

    print(
        f"Ingestion run {metadata['run_id']} marked SUCCESS. "
        f"PRs={metadata['pr_count']}, "
        f"Reviews={metadata['review_count']}"
    )

    return {
        **metadata,
        "status": "SUCCESS"
    }

	
	

     

