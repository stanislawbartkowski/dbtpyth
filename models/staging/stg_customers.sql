with source as (

    select * from {{ source('raw', 'raw_customers') }}

),

renamed as (

    select
        customer_id,
        first_name,
        last_name

    from source

)

select * from renamed
