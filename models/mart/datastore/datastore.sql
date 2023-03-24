{{
    config(
        materialized='incremental',
        unique_key=['cfk_az_batch_update', 'cfk_cif_nbr']
    )
}}

{% set sql_statement %}
    select max(cfk_az_batch_update) from {{ this }}
{% endset %}

{%- set temp = dbt_utils.get_single_value(sql_statement) -%}

{% if temp is not none %}
    {% set lastest_update_at = temp %}
{% else %}
    {% set lastest_update_at = '0000-00-00' %}
{% endif %}

with

final as (

    select
        cfm.cfmr_customer_id as cfk_cif_nbr,
        cfm.az_batch_date as cfk_az_batch_update,
        {{ dbt_utils.star(from=ref('cfm_mvw'), except=['cfmr_customer_id', 'az_batch_date']) }},
        {{ dbt_utils.star(from=ref('test_nda_core'), except=['cfk_cif_nbr', 'cfk_az_batch_update']) }},
        {{ dbt_utils.star(from=ref('test_loan_core'), except=['cfk_cif_nbr', 'cfk_az_batch_update']) }},
        {{ dbt_utils.star(from=ref('test_savings_core'), except=['cfk_cif_nbr', 'cfk_az_batch_update']) }}
    from {{ ref('cfm_mvw') }} cfm
    left join {{ ref('test_nda_core') }} nda
        on cfm.cfmr_customer_id = nda.cfk_cif_nbr
    left join {{ ref('test_loan_core') }} loan
        on cfm.cfmr_customer_id = loan.cfk_cif_nbr
    left join {{ ref('test_savings_core') }} savings
        on cfm.cfmr_customer_id = savings.cfk_cif_nbr
    where cfm.az_batch_date >= date({{ var('az_batch_date', lastest_update_at) }})

)

select * from final