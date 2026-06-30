with

source as (

    select * from {{ source('ecomm', 'customers') }}

),

renamed as (

    select
        id as customer_id,
        first_name,
        last_name,
        email,
        address,
        phone_number,
        created_at

    from source

)

select * from renamed
