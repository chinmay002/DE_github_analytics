import requests
import pandas as pd
import json
from contract import *
import os
from datetime import datetime,timezone,timedelta


from helpers import make_api_requests,get_paginated_data, filter_pr_page

from dotenv import load_dotenv

load_dotenv()

token = os.getenv("API_CODE")
headers = {
    "Accept": "application/vnd.github+json",
    "Authorization": f"Bearer {token}",
    "X-GitHub-Api-Version": "2022-11-28"
}


def get_pulls(owner, repo, extraction_from, extraction_to, run_type, backfill_reason):

    pull_lst = []
    all_revs=[]
    rejected_pulls = []

    url = f"https://api.github.com/repos/{owner}/{repo}/pulls"

    
    data_pulls = get_paginated_data(url,
                                    headers=headers,
                                    params={'state':'all',"direction":"desc",'sort':"updated"},
                                    per_page = 5, 
                                    max_pages = None,
                                    page_filter=filter_pr_page,
                                    extraction_from=extraction_from,
                                    extraction_to=extraction_to)

    for data in data_pulls:  #[pr1,pr2,pr3,pr4]

        errors, warnings = validate_pr_contract(data)
        is_draft_backfill = run_type == "BACKFILL" and backfill_reason == "populate_is_draft"

        if warnings:
            print(f"Contract warning id={data.get('id')}, number={data.get('number')}: {warnings}")

        if errors:
            rejected_pulls.append({"raw_payload": data, "errors": errors})
            continue

        pulls = {
            "id":data.get('id'),
            "number":data.get("number"),
            "state":data.get("state"),
            "title":data.get("title"),
            "is_draft": data.get("draft"),
            "created_at":data.get("created_at"),
            "updated_at":data.get("updated_at"),
            "merged_at":data.get("merged_at"),
            "closed_at":data.get("closed_at"),
            "user_id":data['user'].get("id"),
            "login":data['user'].get("login"),
            "repo_id":data['base']['repo'].get("id"),
            "repo_name":data['base']['repo'].get("name"),
            "author_association":data['author_association']

        }
        pr_number = data.get('number')
        pull_id = data.get('id')
        repo_id = data['base']['repo'].get("id")


        pull_number_url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}"  #for eeach pr1 get reviews

        try:
            lines_data = make_api_requests(pull_number_url,headers=headers)
        except Exception as e:
            raise RuntimeError( f"PR detail extraction failed for PR {pr_number}" ) from e
        
        pulls['additions'] = lines_data['additions']
        pulls['deletions'] = lines_data['deletions']
        pulls['changed_files'] = lines_data['changed_files']
        



        pull_lst.append(pulls)

        try:
            results = get_reviews(owner,repo,pr_number,pull_id,repo_id)
        except Exception as e:
            raise RuntimeError(f'Reveiw etraction for PR {pr_number} failed') from e
        all_revs.extend(results)

    print("Newest:", data_pulls[0]["updated_at"])
    print("Oldest:", data_pulls[-1]["updated_at"])

    return pull_lst,all_revs,rejected_pulls




def get_reviews(owner,repo,pr_number,pull_id,repo_id):

    #print(f'\tin reveiw section for {pr_number},{pull_id},{repo_id}\n')

    rev_url = f"https://api.github.com/repos/{owner}/{repo}/pulls/{pr_number}/reviews"
    rev_lst = []
    
    data_revs = get_paginated_data(
        url=rev_url,
        headers=headers,
        per_page=3
    )


    for data in data_revs:
        revs = {
            "review_id":data.get("id"),
            "pr_number":pr_number,
            "pr_id" :pull_id,
            'repo_id':repo_id,
            "reviewer_login" :data['user'].get("login"),
            "state":data.get("state"),
            'submitted_at':data.get('submitted_at'),
        }

        rev_lst.append(revs)

    return rev_lst




# boundary = datetime.now(timezone.utc) - timedelta(days=2)

# pulls, reviews = get_pulls(
#     "apache",
#     "airflow",
#     boundary
# )

# print("Boundary:", boundary)
# print("PR count:", len(pulls))
# print("Review count:", len(reviews))





    