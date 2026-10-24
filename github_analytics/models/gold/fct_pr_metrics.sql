with review_metrics as (

    select
        pr_id,

        min(submitted_at) as first_review_at,

        count(review_id) as total_reviews,

        count(distinct reviewer_login) as unique_reviewers,

        countif(review_state = 'approved') > 0 as ever_approved

    from {{ ref('stg_reviews') }}

    group by pr_id

),

final as (

    select
        pr.pr_id,
        pr.repo_id,
        pr.repo_name,
        pr.pull_number,
        pr.title,
        pr.author_id,
        pr.author_login,
        pr.author_association,

        pr.pr_state,
        pr.is_merged,

        case
            when pr.is_merged then 'merged'
            when pr.pr_state = 'closed' then 'closed_not_merged'
            else 'open'
        end as pr_outcome,

        pr.created_at,
        pr.updated_at,
        pr.closed_at,
        pr.merged_at,

        pr.additions,
        pr.deletions,
        pr.total_changes,
        pr.changed_files,

        reviews.first_review_at,

        timestamp_diff(
            reviews.first_review_at,
            pr.created_at,
            minute
        ) as time_to_first_review_minutes,

        coalesce(reviews.total_reviews, 0) as total_reviews,

        coalesce(reviews.unique_reviewers, 0) as unique_reviewers,

        coalesce(reviews.ever_approved, false) as ever_approved,

        case
            when pr.merged_at is not null
            then timestamp_diff(
                pr.merged_at,
                pr.created_at,
                minute
            )
        end as time_to_merge_minutes,

        pr.ingested_at

    from {{ ref('stg_pull_requests') }} as pr

    left join review_metrics as reviews
        on pr.pr_id = reviews.pr_id

)

select *
from final