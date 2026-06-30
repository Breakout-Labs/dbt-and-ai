with 

source as (

    select * from {{ source('sheets', 'customer_survey_responses') }}

),

renamed as (

    select
        customer_email,
        {{ parse_date('survey_date') }} as survey_date,
        satisfaction_score,
        feedback

    from source

)

select * from renamed
