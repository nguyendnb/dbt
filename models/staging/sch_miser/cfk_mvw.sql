with source as (

    select * from {{ source('sch_miser', 'cfk_mvw') }}

),

final as (

    select

        az_batch_date::date,
        cfk_cif_nbr::int,
        cfk_acct_nbr::varchar(100)

    from source

)

select * from source