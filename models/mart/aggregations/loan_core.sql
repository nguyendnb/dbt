{{
    config(
        materialized='incremental',
        unique_key=['cfk_az_batch_update', 'cfk_cif_nbr']
    )
}}

with

loan as (

    select * from {{ ref('loan_mvw') }}
    {% if
        is_incremental() == true
        and var('start_date', none) == None
        and var('end_date', none) == None
    %}
        where az_batch_date not in (select distinct cfk_az_batch_update from {{ this }})
    {% elif
        is_incremental() == true
        and var('start_date', none) != None
        and var('end_date', none) != None
    %}
        where az_batch_date between to_date('{{ var("start_date") }}') and to_date('{{ var("end_date") }}')
    {% endif %}

),

cfk as (

    select distinct
        az_batch_date,
        cfk_cif_nbr,
        cfk_acct_nbr
    from {{ ref('cfk_mvw') }}
    {% if
        is_incremental() == true
        and var('start_date', none) == None
        and var('end_date', none) == None
    %}
        where az_batch_date not in (select distinct cfk_az_batch_update from {{ this }})
    {% elif
        is_incremental() == true
        and var('start_date', none) != None
        and var('end_date', none) != None
    %}
        where az_batch_date between to_date('{{ var("start_date") }}') and to_date('{{ var("end_date") }}')
    {% endif %}

),

final as (

    select

        cfk.az_batch_date as cfk_az_batch_update,
        cfk.cfk_cif_nbr,
        count(distinct(l_acct_nbr)) as amt_nbr_l,
        count_if(l_status = 1) as amt_active_l,
        count_if(l_status in (3,4)) as amt_close_l,
        count_if(l_appl_code = '51') as amt_installment_l,
	    count_if(l_appl_code = '50') as amt_mortgage_l,
	    count_if(l_appl_code = '53') as amt_commercial_l,
        sum(l_late_30_days) as amt_late_30_l,
        sum(l_late_60_days) as amt_late_60_l,
        sum(l_late_90_days) as amt_late_90_l,
        sum(l_late_120_days) as amt_late_120_l,
        max(cast(l_max_rate as float)) as max_rate_l,
        min(cast(l_min_rate as float)) as min_rate_l,
        {{ apply_calculations(
            ['min', 'max',],
            [
                'l_cred_score',
                'l_comm_loan_amt'
            ],
            [
                'cred_score_l',
                'comm_loan_amt_l'
            ]
        ) }},
        {{ apply_calculations(
            ['min', 'max',],
            [
                'l_pr_high_bal',
                'l_late_days'
            ],
            [
                'outstand_l',
                'late_days_l'
            ]
        ) }},
        {{ apply_calculations(
            ['min', 'max', 'avg'],
            [
                'l_orig_amt',
                'l_orig_rate',
                'l_intr_rate',
                'l_ytd_intr',
                'l_billed_pr_due',
                'l_chrg_off_int',
                'l_lc_paid',
                'datediff(day, l_dt_last_act, current_date())'
            ], 
            [
                'orig_bal_l',
                'orig_rate_l',
                'int_rate_l',
                'ytd_intr_l'
                'billed_pr_due_l',
                'chrg_off_int_l',
                'lc_paid_l',
                'dt_last_act_l'
            ]
        ) }},
        {{ apply_calculations(
            ['min', 'max', 'avg', 'sum'],
            [
                'l_avgbal_13',
                'l_avgbal_14',
                'l_late_py',
                'l_late_ty',
                'l_pr_bal',
                'l_bal_last_stmt',
                'l_beg_year_bal',
                'l_chrg_off_amt',
                'l_esc_bal',
                'l_orig_comm_amt',
                'l_ave_bal_pyr',
                'l_delq_py'
            ], 
            [
                'bal_ytd_l',
                'bal_prev_l',
                'late_py_l',
                'late_ty_l',
                'pr_bal_l',
                'bal_last_stmt_l',
                'beg_year_bal_l',
                'chrg_off_amt_l',
                'esc_bal_l',
                'l_orig_comm_amt',
                'ave_bal_pyr_l',
                'delq_py_l'
            ]
        ) }}
    
    from cfk
    left join loan
        on cfk.cfk_acct_nbr = loan.l_acct_nbr
            and cfk.az_batch_date = loan.az_batch_date
    group by 1, 2
)

select * from final
