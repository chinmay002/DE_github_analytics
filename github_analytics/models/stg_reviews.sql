with source as (

    select *
    from {{ source('dbt_github_source', 'reviews') }}

),

renamed as (

    select
        review_id,
        pr_id,
        pr_number as pull_number,
        repo_id,
        reviewer_login,

        lower(state) as review_state,

        timestamp(submitted_at) as submitted_at,
        timestamp(ingested_at) as ingested_at

    from source

)

select *
from renamed