with source as (

    select * from {{ source('raw', 'raw_orders') }}

),

renamed as (

    select
        order_id,
        customer_id,
        cast(order_date as date) as order_date,
        amount,
        status

    from source
    where status != 'cancelled'

)

select * from renamed
