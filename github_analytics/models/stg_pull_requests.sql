with source as (

    select *
    from {{ source('dbt_github_source', 'pull_requests') }}

),

renamed as (

    select
        id as pr_id,
        number as pull_number,
        state as pr_state,
        title,

        timestamp(created_at) as created_at,
        timestamp(updated_at) as updated_at,
        timestamp(merged_at) as merged_at,
        timestamp(closed_at) as closed_at,

        user_id as author_id,
        login as author_login,
        is_draft,

        repo_id,
        repo_name,
        author_association,

        additions,
        deletions,
        changed_files,
        additions + deletions as total_changes,

        merged_at is not null as is_merged,

        timestamp(ingested_at) as ingested_at

    from source

)

select *
from renamed