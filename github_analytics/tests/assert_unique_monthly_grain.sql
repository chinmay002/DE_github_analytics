select
    repo_id,
    reporting_month,
    count(*) as row_count

from {{ ref('mart_monthly_repo_activities') }}

group by
    repo_id,
    reporting_month

having count(*) > 1