with

source as (

    select * from {{ source('ecomm', 'order_lines') }}

),

renamed as (

    select
        id as order_line_id,
        order_id,
        line_number,
        product_id,
        quantity,
        unit_price,
        _synced_at

    from source

)

select * from renamed
