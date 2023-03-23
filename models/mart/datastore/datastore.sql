{{
    config(
        materialized='incremental',
        unique_key='cfk_az_batch_update'
    )
}}

with

final as (

    select
        cfm.cfmr_customer_id as cfk_cif_nbr,
        cfm.az_batch_date as cfk_az_batch_update,
        {{ dbt_utils.star(from=ref('cfm_mvw'), except=['cfmr_customer_id', 'az_batch_date']) }},
        {{ dbt_utils.star(from=ref('test_nda_core'), except=['cfk_cif_nbr', 'cfk_az_batch_update']) }},
        {{ dbt_utils.star(from=ref('test_loan_core'), except=['cfk_cif_nbr', 'cfk_az_batch_update']) }},
        {{ dbt_utils.star(from=ref('test_savings_core'), except=['cfk_cif_nbr', 'cfk_az_batch_update']) }}
    from {{ ref('cfm_mvw') }}
    left join {{ ref('test_nda_core') }}
        on cfm.cfmr_customer_id = nda.cfk_cif_nbr
    left join {{ ref('test_loan_core') }}
        on cfm.cfmr_customer_id = loan.cfk_cif_nbr
    left join {{ ref('test_savings_core') }}
        on cfm.cfmr_customer_id = loan.cfk_cif_nbr
    where cfm.az_batch_date >= date({{ var('az_batch_date', 'current_date()') }})

)

select * from final