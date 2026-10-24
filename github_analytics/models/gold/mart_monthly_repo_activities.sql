with pull_requests as (

    select *
    from {{ ref('fct_pr_metrics') }}

),

repositories as (

    select
        repo_id,
        any_value(repo_name) as repo_name

    from pull_requests

    group by repo_id

),

created_metrics as (

    select
        repo_id,
        date_trunc(date(created_at), month) as reporting_month,
        count(pr_id) as created_prs

    from pull_requests

    group by
        repo_id,
        reporting_month

),

review_metrics as (

    select
        repo_id,
        date_trunc(date(first_review_at), month) as reporting_month,
        count(pr_id) as first_reviewed_prs,
        avg(time_to_merge_minutes)/60 as avg_time_to_first_review_hours

    from pull_requests

    where first_review_at is not null

    group by
        repo_id,
        reporting_month

),

merged_metrics as (

    select
        repo_id,
        date_trunc(date(merged_at), month) as reporting_month,
        count(pr_id) as merged_prs,
        avg(time_to_merge_minutes)/60 as avg_time_to_merge_hours

    from pull_requests

    where merged_at is not null

    group by
        repo_id,
        reporting_month

),

closed_unmerged_metrics as (

    select
        repo_id,
        date_trunc(date(closed_at), month) as reporting_month,
        count(pr_id) as closed_unmerged_prs

    from pull_requests

    where closed_at is not null
      and is_merged = false

    group by
        repo_id,
        reporting_month

),

month_spine as (

    select repo_id, reporting_month
    from created_metrics

    union distinct

    select repo_id, reporting_month
    from review_metrics

    union distinct

    select repo_id, reporting_month
    from merged_metrics

    union distinct

    select repo_id, reporting_month
    from closed_unmerged_metrics

),

final as (

    select
        spine.repo_id,
        repositories.repo_name,
        spine.reporting_month,

        coalesce(created.created_prs, 0) as created_prs,

        coalesce(reviewed.first_reviewed_prs, 0)
            as first_reviewed_prs,

        reviewed.avg_time_to_first_review_hours,

        coalesce(merged.merged_prs, 0) as merged_prs,

        merged.avg_time_to_merge_hours,

        coalesce(closed.closed_unmerged_prs, 0)
            as closed_unmerged_prs

    from month_spine as spine

    left join repositories
        on spine.repo_id = repositories.repo_id

    left join created_metrics as created
        on spine.repo_id = created.repo_id
       and spine.reporting_month = created.reporting_month

    left join review_metrics as reviewed
        on spine.repo_id = reviewed.repo_id
       and spine.reporting_month = reviewed.reporting_month

    left join merged_metrics as merged
        on spine.repo_id = merged.repo_id
       and spine.reporting_month = merged.reporting_month

    left join closed_unmerged_metrics as closed
        on spine.repo_id = closed.repo_id
       and spine.reporting_month = closed.reporting_month

)

select *
from final