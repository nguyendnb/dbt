{{
    config(
        materialized='incremental',
        unique_key=['cfk_az_batch_update', 'cfk_cif_nbr']
    )
}}

{%- set types = ['savings', 'cd'] -%}

{% set sql_statement %}
    select max(cfk_az_batch_update) from {{ this }}
{% endset %}

{%- set temp = dbt_utils.get_single_value(sql_statement) -%}

{% if temp is not none and is_incremental() == true %}
    {% set lastest_update_at = temp %}
{% else %}
    {% set lastest_update_at = '0000-00-00' %}
{% endif %}

with savings as (

    select * from {{ ref('savings_mvw') }}
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

        savings.az_batch_date as cfk_az_batch_update,
        cfk.cfk_cif_nbr,
        {% for type in types -%}
        count(distinct case when type = '{{ type }}' then s_acct_nbr end) as cmt_nbr_{{ type }},
        count_if(type = '{{ type }}' and s_status = 1) as amount_open_{{ type }},
        count_if(type = '{{ type }}' and s_status in (3, 4)) as amount_close_{{ type }},
        count_if(type = '{{ type }}' and s_pb_stmt_ind = 0) as amt_pass_{{ type }},
        count_if(type = '{{ type }}' and s_pb_stmt_ind = 1) as amt_pass_statmnt_{{ type }},
        count_if(type = '{{ type }}' and s_pb_stmt_ind = 2) as amt_statmnt_{{ type }},
        -- min(case when type = '{{ type }}' then s_bal_last_stmt end) as 
        {{ apply_calculations(
            ['min', 'max', 'avg', 'sum'],
            [
                "case when type = '"~type~"' then s_nbr_wd_cyc end",
                "case when type = '"~type~"' then s_acct_bal end",
                "case when type = '"~type~"' then s_avgbal_13 end"
            ],
            [
                "nbr_wd_cyc_"~type,
                "acct_bal_"~type,
                "bal_ytd_"~type
            ]
        )}},
        {{ apply_calculations(
            ['min', 'max', 'avg'],
            [
                "case when type = '"~type~"' then s_opening_amt end",
                "case when type = '"~type~"' then s_int_rate end",
                "case when type = '"~type~"' then s_ytd_int end",
                "case when type = '"~type~"' then s_sor_int_ytd end",
                "case when type = '"~type~"' then s_bal_last_stmt end",
                "case when type = '"~type~"' then s_amt_last_dep end",
                "DATEDIFF(day, case when type = '"~type~"' then s_dorm_dt end, savings.az_batch_date)"
            ],
            [
                "open_"~type,
                "ir_"~type,
                "ir_ytd_"~type,
                "sor_int_ytd_"~type,
                "bal_last_stmt_"~type,
                "amt_last_dep_"~type,
                "dorm_dt_"~type
            ]
        )}}
        {%- if not loop.last %},{% endif -%}
        {%- endfor -%}

    from cfk
    left join savings
        on cfk.cfk_acct_nbr = savings.s_acct_nbr
    group by 1, 2

)

select * from final