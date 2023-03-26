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

{% if temp is not none and is_incremental() == true %}
    {% set lastest_update_at = temp %}
{% else %}
    {% set lastest_update_at = '0000-00-00' %}
{% endif %}

with

nda as (

    select * from {{ ref('nda_mvw') }}
    where az_batch_date >= date({{ var('az_batch_date', lastest_update_at) }})

),

cfk as (

    select distinct
        cfk_cif_nbr,
        cfk_acct_nbr
    from {{ ref('cfk_mvw') }}
    where az_batch_date >= date({{ var('az_batch_date', lastest_update_at) }})

),

final as (

    select
        az_batch_date as cfk_az_batch_update,
        cfk.cfk_cif_nbr,
        count(distinct(nd_acct_nbr)) as amt_acct_ca,
        count_if(nd_status = 1) as amt_acct_open_ca,
        count_if(nd_status in (3,4)) as amt_acct_close_ca,
        sum(nd_days_od_prv_yr) as sum_days_overdrawn_prv_yr_ca,
        sum(nd_days_od_ytd) as sum_days_overdrawn_ytd_ca,
        sum(nd_kite_susp_cnt) as fraud_ca,
        sum(nd_eptime_nsf_ltd) as nbr_withdrawals_overdarwn_ca,
        case when sum(nd_card_flag) > 0 then 1 else 0 end as atm_card_ca,
        case when sum(nd_tran_acct) > 0 then 1 else 0 end as tran_acct_ca,
        case when sum(nd_employee_code) > 0 then 1 else 0 end as if_employee_ca,
        case when sum(nd_cmt_flag) > 0 then 1 else 0 end as cash_mngmnt_ca,
        case when sum(nd_eod_code) > 0 then 1 else 0 end as extnd_overdraft_ca,
        case when sum(nd_lc_code) > 0 then 1 else 0 end as if_lc_code_ca,
        {{ apply_calculations(
            ['min', 'max', 'avg', 'sum'],
            [
                'nd_acct_bal',
                'nd_avgbal_13',
                'nd_avgbal_14',
                'nd_nbr_credits',
                'nd_nbr_debits',
                'nd_nbr_dep_items',
                'nd_nbr_deposits',
                'nd_otc_tran_ctr',
                'nd_times_nsf_ltd',
                'nd_patm_wd',
                'nd_pos_wd',
                'nd_hard_holds'

            ],
            [
                'acct_bal_ca',
                'bal_ytd_ca',
                'bal_prv_ca',
                'nbr_credits_ca',
                'nbr_debits_ca',
                'nbr_dep_items_ca',
                'nbr_deposits_ca',
                'otc_ca',
                'times_nsf_ltd_ca',
                'patm_wd_ca',
                'pos_wd_ca',
                'hard_holds_ca'
            ]
        )}},
        {{ apply_calculations(
            ['min', 'max', 'avg'],
            [
                'nd_opening_amt',
                'nd_int_rate',
                'datediff(day, nd_opening_dt, current_date())',
                'nd_amt_last_dep',
                'datediff(day, nd_dt_last_dep, current_date())',
                'datediff(day, nd_dt_last_act, current_date())',
                'nd_prev_ytd_int',
                'nd_ytd_int',
                'nd_sor_int_py',
                'nd_sor_int_ytd'

            ],
            [
                'open_amt_ca',
                'ir_ca',
                'days_open_ca',
                'dep_ca',
                'days_dep_ca',
                'days_monent_trans_ca',
                'prev_ytd_ir_ca',
                'ytd_ir_ca',
                'int_pr_state_ca',
                'int_ytd_state_ca'
            ]
        )}}

    from cfk
    left join nda
        on cfk.cfk_acct_nbr = nda.nd_acct_nbr
    group by 1, 2

)

select * from final