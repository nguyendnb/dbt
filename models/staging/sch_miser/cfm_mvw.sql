with source as (

    select

        {{ dbt_utils.star(from=source('sch_miser', 'cfm_mvw'), except=['deleted_record', 'update_time', 'update_type', 'az_batch_update']) }}

    from {{ source('sch_miser', 'cfm_mvw') }}

),

final as (

    select

        {{ cast_data_types([
            'az_batch_date',
            'cfmr_alrt_cancl_dt',
            'cfmr_ar_last_fm',
            'cfmr_aud_lst_act',
            'cfmr_birthdate',
            'cfmr_cert_dt',
            'cfmr_cert_thru_dt',
            'cfmr_cib_lst_act',
            'cfmr_cmb_lst_act',
            'cfmr_date_opened',
            'cfmr_death_dt',
            'cfmr_del_cancel_dt',
            'cfmr_dt_closed',
            'cfmr_dt_last_addr',
            'cfmr_dt_last_cl',
            'cfmr_dt_last_fm',
            'cfmr_exp_dt_1',
            'cfmr_exp_dt_2',
            'cfmr_exp_dt_3',
            'cfmr_exp_dt_4',
            'cfmr_exp_dt_5',
            'cfmr_exp_dt_6',
            'cfmr_ffi_verify_dt',
            'cfmr_itin_expir_dt',
            'cfmr_privacy_dt',
            'cfmr_rib_lst_act',
            'cfmr_rmb_lst_act',
            'cfmr_sea_start_dt',
            'cfmr_sea_stop_dt',
            'cfmr_w8bene_req_dt'
        ], 'date') }},
        {{ cast_data_types([
            'cfmr_alerts',
            'cfmr_last_name',
            'cfmr_appl_code',
            'cfmr_appl_type',
            'cfmr_ar_status',
            'cfmr_ar_vpin_ll',
            'cfmr_ar_vpin_off',
            'cfmr_assn_nbr',
            'cfmr_cc_option',
            'cfmr_cen_officer',
            'cfmr_combine_ind',
            'cfmr_ctr_exmt_amt',
            'cfmr_ctr_exmt_end',
            'cfmr_delivery',
            'cfmr_delivery_pt',
            'cfmr_delq_ctr',
            'cfmr_edelivery',
            'cfmr_email_cnt',
            'cfmr_emp_cd',
            'cfmr_entity_flag',
            'cfmr_entity_id',
            'cfmr_exp_alpha_1',
            'cfmr_exp_alpha_2',
            'cfmr_exp_alpha_3',
            'cfmr_exp_alpha_4',
            'cfmr_exp_alpha_5',
            'cfmr_exp_alpha_6',
            'cfmr_exp_alpha20',
            'cfmr_exp_char_1',
            'cfmr_exp_char_2',
            'cfmr_exp_char_3',
            'cfmr_exp_char_4',
            'cfmr_exp_char_5',
            'cfmr_exp_char_6',
            'cfmr_exp_nbr_1',
            'cfmr_exp_nbr_2',
            'cfmr_exp_nbr_3',
            'cfmr_exp_numric_1',
            'cfmr_exp_numric_2',
            'cfmr_exp_numric_3',
            'cfmr_exp_numric_4',
            'cfmr_exp_numric_5',
            'cfmr_exp_numric_6',
            'cfmr_exp1_option',
            'cfmr_exp2_option',
            'cfmr_exp3_option',
            'cfmr_fatca_class',
            'cfmr_foreign_ind',
            'cfmr_grp3_nbr',
            'cfmr_ib_vpin_off',
            'cfmr_lock_code_1',
            'cfmr_lock_code_2',
            'cfmr_lock_flag_01',
            'cfmr_lock_flag_02',
            'cfmr_lock_flag_03',
            'cfmr_lock_flag_04',
            'cfmr_lock_flag_05',
            'cfmr_lock_flag_06',
            'cfmr_lock_flag_07',
            'cfmr_lock_flag_08',
            'cfmr_lock_flag_09',
            'cfmr_lock_flag_10',
            'cfmr_lock_flag_11',
            'cfmr_lock_flag_12',
            'cfmr_lock_flag_13',
            'cfmr_lock_flag_14',
            'cfmr_lock_flag_15',
            'cfmr_lock_flag_16',
            'cfmr_lock_flag_17',
            'cfmr_lock_flag_18',
            'cfmr_lock_flag_19',
            'cfmr_lock_flag_20',
            'cfmr_lock_flag_21',
            'cfmr_lock_flag_22',
            'cfmr_lock_flag_23',
            'cfmr_lock_flag_24',
            'cfmr_lock_flag_25',
            'cfmr_lock_flag_26',
            'cfmr_lock_flag_27',
            'cfmr_lock_flag_28',
            'cfmr_lock_flag_29',
            'cfmr_lock_flag_30',
            'cfmr_lock_flag_31',
            'cfmr_lock_flag_32',
            'cfmr_lock_flag_33',
            'cfmr_lock_flag_34',
            'cfmr_lock_flag_35',
            'cfmr_lock_flag_36',
            'cfmr_lock_flag_37',
            'cfmr_lock_flag_38',
            'cfmr_lock_flag_39',
            'cfmr_lock_flag_40',
            'cfmr_lock_flag_41',
            'cfmr_lock_flag_42',
            'cfmr_lock_flag_43',
            'cfmr_lock_flag_44',
            'cfmr_lock_flag_45',
            'cfmr_lock_flag_53',
            'cfmr_lock_flag_54',
            'cfmr_lock_flag_58',
            'cfmr_lock_flag_59',
            'cfmr_mail_code',
            'cfmr_nosol_email',
            'cfmr_nosol_exp',
            'cfmr_nosol_mail',
            'cfmr_nosol_phone',
            'cfmr_note_ind',
            'cfmr_nv_option',
            'cfmr_own_rent',
            'cfmr_review_code',
            'cfmr_sea_usage',
            'cfmr_state_code2',
            'cfmr_temp_pin_ind',
            'cfmr_warn_cd_1',
            'cfmr_warn_cd_2',
            'cfmr_warn_flag_04',
            'cfmr_warn_flag_05',
            'cfmr_warn_flag_06',
            'cfmr_warn_flag_07',
            'cfmr_warn_flag_08',
            'cfmr_warn_flag_09',
            'cfmr_warn_flag_10',
            'cfmr_warn_flag_11',
            'cfmr_warn_flag_12',
            'cfmr_warn_flag_13',
            'cfmr_warn_flag_14',
            'cfmr_warn_flag_15',
            'cfmr_warn_flag_16',
            'cfmr_warn_flag_17',
            'cfmr_warn_flag_18',
            'cfmr_warn_flag_19',
            'cfmr_warn_flag_20',
            'cfmr_warn_flag_21',
            'cfmr_warn_flag_22',
            'cfmr_warn_flag_23',
            'cfmr_warn_flag_24',
            'cfmr_warn_flag_25',
            'cfmr_warn_flag_26',
            'cfmr_warn_flag_27',
            'cfmr_warn_flag_28',
            'cfmr_warn_flag_29',
            'cfmr_warn_flag_30',
            'cfmr_warn_flag_31',
            'cfmr_warn_flag_32',
            'cfmr_warn_flag_33',
            'cfmr_warn_flag_34',
            'cfmr_warn_flag_35',
            'cfmr_warn_flag_36',
            'cfmr_warn_flag_37',
            'cfmr_warn_flag_38',
            'cfmr_warn_flag_39',
            'cfmr_warn_flag_40',
            'cfmr_warn_flag_41',
            'cfmr_warn_flag_42',
            'cfmr_warn_flag_43',
            'cfmr_warn_flag_44',
            'cfmr_warn_flag_45',
            'cfmr_warn_flag_46',
            'cfmr_warn_flag_47',
            'cfmr_warn_flag_48',
            'cfmr_warn_flag_49',
            'cfmr_warn_flag_50',
            'cfmr_warn_flag_51',
            'cfmr_warn_flag_52',
            'cfmr_warn_flag_53',
            'cfmr_warn_flag_54',
            'cfmr_warn_flag_55',
            'cfmr_warn_flag_56',
            'cfmr_warn_flag_57',
            'cfmr_warn_flag_58',
            'cfmr_warn_flag_59',
            'cfmr_warn_flag_60'
        ], 'float') }},
        {{ cast_data_types([
            'cfmr_age',
            'cfmr_agent_officer',
            'cfmr_ar_fm_hhmm',
            'cfmr_ar_pin_off',
            'cfmr_bus_ph_intl',
            'cfmr_bus_phone_ext',
            'cfmr_cell_ph_intl',
            'cfmr_census_tract',
            'cfmr_cra_nbr',
            'cfmr_educ_level',
            'cfmr_fax_ph_intl',
            'cfmr_home_ph_intl',
            'cfmr_household_no',
            'cfmr_income_level',
            'cfmr_lifestyle_cd',
            'cfmr_loc_code',
            'cfmr_naics_code',
            'cfmr_nbr_depend',
            'cfmr_occup_code',
            'cfmr_open_branch',
            'cfmr_open_reason',
            'cfmr_sic_code',
            'cfmr_smsa_nbr',
            'cfmr_state_code',
            'cfmr_tax_id_key',
            'cfmr_tax_id_type'
        ], 'int') }},
        {{ cast_data_types([
            'cfmr_1042_corr',
            'cfmr_1042pct_dp_py',
            'cfmr_1042pct_ln_py',
            'cfmr_acct_name',
            'cfmr_addition_addr',
            'cfmr_addr_key',
            'cfmr_address_ind',
            'cfmr_aff_info_ind',
            'cfmr_affiliate_nbr',
            'cfmr_alpha_key',
            'cfmr_bus_name',
            'cfmr_cash_trk_id',
            'cfmr_cell_phone',
            'cfmr_cert_lob_cat',
            'cfmr_cert_pct',
            'cfmr_cert_type',
            'cfmr_city',
            'cfmr_cnv_exc_flag',
            'cfmr_cred_rating',
            'cfmr_ctr_exempt_st',
            'cfmr_ctr_exmt_strt',
            'cfmr_cust_type',
            'cfmr_customer_id',
            'cfmr_death_added',
            'cfmr_death_src',
            'cfmr_delivery_addr',
            'cfmr_doc_dlvry_ind1',
            'cfmr_doc_dlvry_ind2',
            'cfmr_doc_dlvry_ind3',
            'cfmr_doc_dlvry_ind4',
            'cfmr_doc_dlvry_ind5',
            'cfmr_first_name',
            'cfmr_foreign_flg',
            'cfmr_frgn_tax_type',
            'cfmr_frgn_taxid',
            'cfmr_grp1_nbr',
            'cfmr_grp2_nbr',
            'cfmr_inc_cntry_abr',
            'cfmr_info_attn',
            'cfmr_latitude',
            'cfmr_lock_flag_46',
            'cfmr_lock_flag_47',
            'cfmr_lock_flag_48',
            'cfmr_lock_flag_49',
            'cfmr_lock_flag_50',
            'cfmr_lock_flag_51',
            'cfmr_lock_flag_52',
            'cfmr_lock_flag_55',
            'cfmr_lock_flag_56',
            'cfmr_lock_flag_57',
            'cfmr_lock_flag_60',
            'cfmr_longitude',
            'cfmr_mail_addr_ind',
            'cfmr_marital_stat',
            'cfmr_middle_name',
            'cfmr_misc_id',
            'cfmr_nosol_bus',
            'cfmr_nosol_cell',
            'cfmr_nosol_fax',
            'cfmr_nosol_pager',
            'cfmr_nra_class',
            'cfmr_nra_no_inc_yr',
            'cfmr_occupation',
            'cfmr_org_cntry_abr',
            'cfmr_phone_no_bus',
            'cfmr_phone_no_res',
            'cfmr_prefix',
            'cfmr_privacy_ind',
            'cfmr_race',
            'cfmr_rcif_no',
            'cfmr_res_cntry_abr',
            'cfmr_sex',
            'cfmr_state',
            'cfmr_state_res',
            'cfmr_status',
            'cfmr_suffix',
            'cfmr_tax_id_no',
            'cfmr_us_nffe_pcnt',
            'cfmr_warn_flag_01',
            'cfmr_warn_flag_02',
            'cfmr_warn_flag_03',
            'cfmr_zip'
        ], 'varchar(100)') }}

    from source

)

select * from final
