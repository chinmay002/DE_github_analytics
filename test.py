# import pandas as pd
# #import numpy as np

# pull = pd.read_csv("/Users/chinmaymaganur/Desktop/github_proj/pulls.csv")
# rev = pd.read_csv("reviews.csv")

# print(pull['id'].is_unique)
# print(rev['review_id'].is_unique)

# all_present = set(rev['pr_id']).issubset(set(pull['id']))
# print(all_present)

# all_presentsss = rev['pr_id'].isin(pull['id']).all()

# print(all_presentsss) 

import pandas as pd
import numpy as np
from google.cloud import bigquery
from bq_connect import bq_connect


def validate_pull(client):

    query_updates = f"""
        SELECT
        COUNT(*) AS matching_ids,
        COUNTIF(source.updated_at > target.updated_at) AS expected_updates
        FROM `bronze.pull_request_temp` source
        JOIN `bronze.pull_requests` target
        ON source.id = target.id;
""" 
    query_inserts = f"""
            SELECT COUNT(*) AS expected_inserts
            FROM `bronze.pull_request_temp` source
            LEFT JOIN `bronze.pull_requests` target
                ON source.id = target.id
            WHERE target.id IS NULL;
    """ 
    
    
    result_upds = client.query(query_updates).to_dataframe()
    result_insrt = client.query(query_inserts).to_dataframe()

    expected_updates = int(result_upds.loc[0, "expected_updates"])
    expected_inserts = int(result_insrt.loc[0, "expected_inserts"])

    return expected_updates, expected_inserts

def validate_temp(client):

    query_pull_temp = """
        SELECT DISTINCT ingested_at
        FROM `bronze.pull_request_temp`
    """

    query_rev_temp = """
        SELECT DISTINCT ingested_at
        FROM `bronze.reviews_temp`
    """

    pull_rows = list(client.query(query_pull_temp).result())
    rev_rows = list(client.query(query_rev_temp).result())

    if len(pull_rows) != 1:
        raise RuntimeError(
            f"Expected one PR temp batch, found {len(pull_rows)}"
        )

    if len(rev_rows) != 1:
        raise RuntimeError(
            f"Expected one review temp batch, found {len(rev_rows)}"
        )

    pull_ingested_at = pull_rows[0]["ingested_at"]
    rev_ingested_at = rev_rows[0]["ingested_at"]

    if pull_ingested_at != rev_ingested_at:
        raise RuntimeError(
            f"Temp batch mismatch: "
            f"PR={pull_ingested_at}, Reviews={rev_ingested_at}"
        )

    print("Temp batch validation passed")

   # print(query_job)

    # result = query_job.result() #iterator

    # for r in result:
    #     print(r['matching_ids'])

# client = bq_connect("github-proj-508123")
# validate_pull(client)