--assert_pr_metric_oonsistency

select *
from {{ ref('fct_pr_metrics') }}

where
    time_to_first_review_minutes < 0

    or time_to_merge_minutes < 0

    or (
        is_merged = true
        and merged_at is null
    )

    or (
        is_merged = false
        and merged_at is not null
    )

    or (
        pr_outcome = 'open'
        and pr_state != 'open'
    )

    or (
        pr_outcome = 'merged'
        and merged_at is null
    )

    or (
        pr_outcome = 'closed_not_merged'
        and (
            pr_state != 'closed'
            or merged_at is not null
        )
    )