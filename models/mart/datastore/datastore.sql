{{
    config(
        materialized='incremental',
        unique_key=['cfk_az_batch_update', 'cfk_cif_nbr']
    )
}}

with

final as (

    select distinct
        cfm.cfmr_customer_id as cfk_cif_nbr,
        cfm.az_batch_date as cfk_az_batch_update,
        {{ dbt_utils.star(from=ref('cfm_mvw'), except=['cfmr_customer_id', 'az_batch_date']) }},
        {{ dbt_utils.star(from=ref('nda_core'), except=['cfk_cif_nbr', 'cfk_az_batch_update']) }},
        {{ dbt_utils.star(from=ref('loan_core'), except=['cfk_cif_nbr', 'cfk_az_batch_update']) }},
        {{ dbt_utils.star(from=ref('savings_core'), except=['cfk_cif_nbr', 'cfk_az_batch_update']) }}
    from {{ ref('cfm_mvw') }} cfm
    left join {{ ref('nda_core') }} nda
        on cfm.cfmr_customer_id = nda.cfk_cif_nbr
            and cfm.az_batch_date = nda.cfk_az_batch_update
    left join {{ ref('loan_core') }} loan
        on cfm.cfmr_customer_id = loan.cfk_cif_nbr
            and cfm.az_batch_date = loan.cfk_az_batch_update
    left join {{ ref('savings_core') }} savings
        on cfm.cfmr_customer_id = savings.cfk_cif_nbr
            and cfm.az_batch_date = savings.cfk_az_batch_update
    {% if
        is_incremental() == true
        and var('date', none) != None
    %}
        where az_batch_date = to_date('{{ var("date") }}')
    {% endif %}

)

select * from final