with

source as (

    select * from {{ source('ecomm', 'orders') }}

),

renamed as (

    select
        id as order_id,
        total_amount,
        status as order_status,
        created_at as ordered_at,
        customer_id,
        store_id,
        _synced_at

    from source

)

select * from renamed
