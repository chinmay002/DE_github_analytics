select *
from {{ ref('mart_monthly_repo_activities') }}

where
    created_prs < 0
    or first_reviewed_prs < 0
    or merged_prs < 0
    or closed_unmerged_prs < 0
    or avg_time_to_first_review_hours < 0
    or avg_time_to_merge_hours < 0