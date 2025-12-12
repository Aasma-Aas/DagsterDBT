{{
    config(
        materialized='table',
        schema='staging',
        engine='MergeTree()',
        order_by='user_id',
        partition_by='toYYYYMM(updated_at)',
        settings={'index_granularity': 8192}
    )
}}

WITH source AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC) as rn
    FROM {{ source('raw', 'users') }}
    WHERE created_at >= '2020-01-01' 
),

cleaned AS (
    SELECT 
        -- Primary key
        user_id,
        
        -- User attributes
        lower(trim(email)) as email,
        initcap(trim(first_name)) as first_name,
        initcap(trim(last_name)) as last_name,
        upper(trim(country_code)) as country_code,
        
        -- Timestamps
        created_at,
        updated_at,
        
        -- Status
        is_active,
        
        -- Derived fields
        concat(initcap(trim(first_name)), ' ', initcap(trim(last_name))) as full_name,
        splitByChar('@', email)[2] as email_domain,
        toDate(created_at) as signup_date,
        toDate(updated_at) as last_updated_date,
        date_diff('day', created_at, updated_at) as account_age_days,
        
        -- Data quality flags
        CASE 
            WHEN email NOT LIKE '%@%' THEN 1 
            ELSE 0 
        END as is_email_invalid,
        CASE 
            WHEN country_code = '' OR length(country_code) != 2 THEN 1 
            ELSE 0 
        END as is_country_invalid,
        
        -- Metadata
        _loaded_at as dbt_loaded_at,
        now() as dbt_updated_at
        
    FROM source
    WHERE rn = 1  -- Deduplication
)

SELECT * FROM cleaned