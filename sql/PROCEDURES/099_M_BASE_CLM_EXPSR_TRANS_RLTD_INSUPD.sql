-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_EXPSR_TRANS_RLTD_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component LKP_TERADATA_ETL_REF_XLAT_CLM_EXPSR_TRANS_RLTD_ROLE_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_clm_expsr_trans_rltd_role_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''CLM_EXPSR_TRANS_RLTD_ROLE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component sq_cc_transaction, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_cc_transaction AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS parent_transaction_id,
                $2 AS child_transaction_id,
                $3 AS retired,
                $4 AS rltd_role_cd,
                $5 AS trans_start_date,
                $6 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT parenttransactionid,
                                                                  childtransactionid,
                                                                  retired,
                                                                  rltd_role_cd,
                                                                  child_updatetime
                                                  FROM            (
                                                                         SELECT child.exposureid,
                                                                                child.claimid,
                                                                                parent.parenttransactionid,
                                                                                child.childtransactionid ,
                                                                                CASE
                                                                                       WHEN child.retired=0
                                                                                       AND    parent.retired=0 THEN 0
                                                                                       ELSE 1
                                                                                END                                                  retired,
                                                                                cast(''CLM_EXPSR_TRANS_RLTD_ROLE1'' AS VARCHAR(30)) AS rltd_role_cd,
                                                                                child.child_updatetime,
                                                                                parent.parent_updatetime,
                                                                                p_eligible,
                                                                                c_eligible
                                                                         FROM   (
                                                                                                SELECT          a.id_stg            AS childtransactionid,
                                                                                                                a.reservelineid_stg AS reservelineid ,
                                                                                                                a.costtype_stg      AS costtype ,
                                                                                                                a.costcategory_stg  AS costcategory,
                                                                                                                a.claimid_stg       AS claimid,
                                                                                                                a.exposureid_stg    AS exposureid ,
                                                                                                                a.retired_stg       AS retired ,
                                                                                                                a.updatetime_stg    AS child_updatetime ,
                                                                                                                CASE
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''voided''
                                                                                                                                AND             cc.payload_new_stg=''voided_11'' THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''voided''
                                                                                                                                AND             cc.payload_new_stg= ''voided_15'' THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''transferred''
                                                                                                                                AND             cc.payload_new_stg= ''transferred_11''THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''transferred''
                                                                                                                                AND             cc.payload_new_stg= ''transferred_13'' THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg ='' transferred''
                                                                                                                                AND             cc.payload_new_stg=''cleared_13'' THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                AND             cc.payload_new_stg=''recoded_11'' THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                AND             cc.payload_new_stg = ''recoded_14'' THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                AND             cc.payload_new_stg=''issued_14'' THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                AND             cc.payload_new_stg= ''cleared_14'' THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                AND             cc.payload_new_stg= ''requested_14'' THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                AND             cc.payload_new_stg= ''voided_14'' THEN ''N''
                                                                                                                                ELSE ''Y''
                                                                                                                END AS p_eligible
                                                                                                FROM            db_t_prod_stag.cc_transaction a
                                                                                                join            db_t_prod_stag.cctl_transactionstatus
                                                                                                ON              a.status_stg= cctl_transactionstatus.id_stg
                                                                                                join
                                                                                                                (
                                                                                                                           SELECT     cc_claim.*
                                                                                                                           FROM       db_t_prod_stag.cc_claim
                                                                                                                           inner join db_t_prod_stag.cctl_claimstate
                                                                                                                           ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                           WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                ON              cc_claim.id_stg=a.claimid_stg
                                                                                                join            db_t_prod_stag.cc_policy
                                                                                                ON              cc_claim.policyid_stg=cc_policy.id_stg
                                                                                                left outer join db_t_prod_stag.cc_exposure
                                                                                                ON              cc_exposure.id_stg=a.exposureid_stg
                                                                                                left outer join db_t_prod_stag.cc_check
                                                                                                ON              cc_check.id_stg= a.checkid_stg
                                                                                                left outer join db_t_prod_stag.cc_user
                                                                                                ON              a.createuserid_stg = cc_user.id_stg
                                                                                                left outer join db_t_prod_stag.cc_contact
                                                                                                ON              cc_user.contactid_stg = cc_contact.id_stg
                                                                                                left outer join db_t_prod_stag.gl_eventstaging_cc cc
                                                                                                ON              cc.publicid_stg=a.publicid_stg
                                                                                                join            db_t_prod_stag.cctl_transaction c
                                                                                                ON              c.id_stg = a.subtype_stg
                                                                                                WHERE           c.typecode_stg = ''Payment''
                                                                                                AND             a.exposureid_stg IS NOT NULL
                                                                                                AND             ((
                                                                                                                                                a.updatetime_stg >($start_dttm)
                                                                                                                                AND             a.updatetime_stg <= ($end_dttm))
                                                                                                                OR              (
                                                                                                                                                cc_check.updatetime_stg >($start_dttm)
                                                                                                                                AND             cc_check.updatetime_stg <= ($end_dttm))) ) child
                                                                         join
                                                                                (
                                                                                                SELECT          a.id_stg            AS parenttransactionid,
                                                                                                                a.reservelineid_stg AS reservelineid,
                                                                                                                a.costtype_stg      AS costtype,
                                                                                                                a.costcategory_stg  AS costcategory,
                                                                                                                a.claimid_stg       AS claimid,
                                                                                                                a.exposureid_stg    AS exposureid,
                                                                                                                a.retired_stg       AS retired,
                                                                                                                a.updatetime_stg    AS parent_updatetime ,
                                                                                                                CASE
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''voided''
                                                                                                                                AND             cc.payload_new_stg=''voided_11'' THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''voided''
                                                                                                                                AND             cc.payload_new_stg= ''voided_15'' THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''transferred''
                                                                                                                                AND             cc.payload_new_stg= ''transferred_11''THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''transferred''
                                                                                                                                AND             cc.payload_new_stg= ''transferred_13'' THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg ='' transferred''
                                                                                                                                AND             cc.payload_new_stg=''cleared_13'' THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                AND             cc.payload_new_stg=''recoded_11'' THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                AND             cc.payload_new_stg = ''recoded_14'' THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                AND             cc.payload_new_stg=''issued_14'' THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                AND             cc.payload_new_stg= ''cleared_14'' THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                AND             cc.payload_new_stg= ''requested_14'' THEN ''N''
                                                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                                                AND             cc.payload_new_stg= ''voided_14'' THEN ''N''
                                                                                                                                ELSE ''Y''
                                                                                                                END AS c_eligible
                                                                                                FROM            db_t_prod_stag.cc_transaction a
                                                                                                join            db_t_prod_stag.cctl_transactionstatus
                                                                                                ON              a.status_stg= cctl_transactionstatus.id_stg
                                                                                                join
                                                                                                                (
                                                                                                                           SELECT     cc_claim.*
                                                                                                                           FROM       db_t_prod_stag.cc_claim
                                                                                                                           inner join db_t_prod_stag.cctl_claimstate
                                                                                                                           ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                           WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                ON              cc_claim.id_stg=a.claimid_stg
                                                                                                join            db_t_prod_stag.cc_policy
                                                                                                ON              cc_claim.policyid_stg=cc_policy.id_stg
                                                                                                left outer join db_t_prod_stag.cc_exposure
                                                                                                ON              cc_exposure.id_stg=a.exposureid_stg
                                                                                                left outer join db_t_prod_stag.cc_check
                                                                                                ON              cc_check.id_stg= a.checkid_stg
                                                                                                left outer join db_t_prod_stag.cc_user
                                                                                                ON              a.createuserid_stg = cc_user.id_stg
                                                                                                left outer join db_t_prod_stag.cc_contact
                                                                                                ON              cc_user.contactid_stg = cc_contact.id_stg
                                                                                                left outer join db_t_prod_stag.gl_eventstaging_cc cc
                                                                                                ON              cc.publicid_stg=a.publicid_stg
                                                                                                join            db_t_prod_stag.cctl_transaction c
                                                                                                ON              c.id_stg = a.subtype_stg
                                                                                                WHERE           c.typecode_stg = ''Reserve''
                                                                                                AND             a.exposureid_stg IS NOT NULL
                                                                                                AND             ((
                                                                                                                                                a.updatetime_stg >($start_dttm)
                                                                                                                                AND             a.updatetime_stg <= ($end_dttm))
                                                                                                                OR              (
                                                                                                                                                cc_check.updatetime_stg >($start_dttm)
                                                                                                                                AND             cc_check.updatetime_stg <= ($end_dttm))) ) parent
                                                                         ON     (
                                                                                       child.claimid = parent.claimid
                                                                                AND    child.exposureid = parent.exposureid
                                                                                AND    child.reservelineid = parent.reservelineid
                                                                                AND    child.costtype = parent.costtype
                                                                                AND    child.costcategory = parent.costcategory )) trns_rltd
                                                  WHERE           p_eligible=''Y''
                                                  AND             c_eligible=''Y'' ) src ) );
  -- Component exp_all_sources, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_all_sources AS
  (
         SELECT sq_cc_transaction.parent_transaction_id AS parent_transaction_id,
                sq_cc_transaction.child_transaction_id  AS child_transaction_id,
                sq_cc_transaction.retired               AS retired,
                sq_cc_transaction.rltd_role_cd          AS rltd_role_cd,
                sq_cc_transaction.trans_start_date      AS trans_start_date,
                sq_cc_transaction.source_record_id
         FROM   sq_cc_transaction );
  -- Component LKP_CHILD_CLM_EXPSR_TRANS, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_child_clm_expsr_trans AS
  (
            SELECT    lkp.clm_expsr_trans_id,
                      exp_all_sources.source_record_id,
                      row_number() over(PARTITION BY exp_all_sources.source_record_id ORDER BY lkp.clm_expsr_trans_id DESC,lkp.clm_expsr_trans_sbtype_cd DESC,lkp.clm_expsr_id DESC,lkp.expsr_cost_type_cd DESC,lkp.expsr_cost_ctgy_type_cd DESC,lkp.pmt_type_cd DESC,lkp.clm_expsr_trans_dttm DESC,lkp.clm_expsr_trans_txt DESC,lkp.rcvry_ctgy_type_cd DESC,lkp.does_not_erode_rserv_ind DESC,lkp.crtd_by_prty_id DESC,lkp.nk_clm_expsr_trans_id DESC,lkp.gl_mth_num DESC,lkp.gl_yr_num DESC,lkp.trty_cd DESC,lkp.prcs_id DESC,lkp.clm_expsr_trans_strt_dttm DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC) rnk
            FROM      exp_all_sources
            left join
                      (
                               SELECT   clm_expsr_trans.clm_expsr_trans_id        AS clm_expsr_trans_id,
                                        clm_expsr_trans.clm_expsr_trans_sbtype_cd AS clm_expsr_trans_sbtype_cd,
                                        clm_expsr_trans.clm_expsr_id              AS clm_expsr_id,
                                        clm_expsr_trans.expsr_cost_type_cd        AS expsr_cost_type_cd,
                                        clm_expsr_trans.expsr_cost_ctgy_type_cd   AS expsr_cost_ctgy_type_cd,
                                        clm_expsr_trans.pmt_type_cd               AS pmt_type_cd,
                                        clm_expsr_trans.clm_expsr_trans_dttm      AS clm_expsr_trans_dttm,
                                        clm_expsr_trans.clm_expsr_trans_txt       AS clm_expsr_trans_txt,
                                        clm_expsr_trans.rcvry_ctgy_type_cd        AS rcvry_ctgy_type_cd,
                                        clm_expsr_trans.does_not_erode_rserv_ind  AS does_not_erode_rserv_ind,
                                        clm_expsr_trans.crtd_by_prty_id           AS crtd_by_prty_id,
                                        clm_expsr_trans.gl_mth_num                AS gl_mth_num,
                                        clm_expsr_trans.gl_yr_num                 AS gl_yr_num,
                                        clm_expsr_trans.trty_cd                   AS trty_cd,
                                        clm_expsr_trans.prcs_id                   AS prcs_id,
                                        clm_expsr_trans.clm_expsr_trans_strt_dttm AS clm_expsr_trans_strt_dttm,
                                        clm_expsr_trans.edw_strt_dttm             AS edw_strt_dttm,
                                        clm_expsr_trans.edw_end_dttm              AS edw_end_dttm,
                                        clm_expsr_trans.nk_clm_expsr_trans_id     AS nk_clm_expsr_trans_id
                               FROM     db_t_prod_core.clm_expsr_trans qualify row_number() over( PARTITION BY clm_expsr_trans.nk_clm_expsr_trans_id ORDER BY clm_expsr_trans.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.nk_clm_expsr_trans_id = exp_all_sources.child_transaction_id qualify rnk = 1 );
  -- Component LKP_PARENT_CLM_EXPSR_TRANS, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_parent_clm_expsr_trans AS
  (
            SELECT    lkp.clm_expsr_trans_id,
                      exp_all_sources.source_record_id,
                      row_number() over(PARTITION BY exp_all_sources.source_record_id ORDER BY lkp.clm_expsr_trans_id DESC,lkp.clm_expsr_trans_sbtype_cd DESC,lkp.clm_expsr_id DESC,lkp.expsr_cost_type_cd DESC,lkp.expsr_cost_ctgy_type_cd DESC,lkp.pmt_type_cd DESC,lkp.clm_expsr_trans_dttm DESC,lkp.clm_expsr_trans_txt DESC,lkp.rcvry_ctgy_type_cd DESC,lkp.does_not_erode_rserv_ind DESC,lkp.crtd_by_prty_id DESC,lkp.nk_clm_expsr_trans_id DESC,lkp.gl_mth_num DESC,lkp.gl_yr_num DESC,lkp.trty_cd DESC,lkp.prcs_id DESC,lkp.clm_expsr_trans_strt_dttm DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC) rnk
            FROM      exp_all_sources
            left join
                      (
                               SELECT   clm_expsr_trans.clm_expsr_trans_id        AS clm_expsr_trans_id,
                                        clm_expsr_trans.clm_expsr_trans_sbtype_cd AS clm_expsr_trans_sbtype_cd,
                                        clm_expsr_trans.clm_expsr_id              AS clm_expsr_id,
                                        clm_expsr_trans.expsr_cost_type_cd        AS expsr_cost_type_cd,
                                        clm_expsr_trans.expsr_cost_ctgy_type_cd   AS expsr_cost_ctgy_type_cd,
                                        clm_expsr_trans.pmt_type_cd               AS pmt_type_cd,
                                        clm_expsr_trans.clm_expsr_trans_dttm      AS clm_expsr_trans_dttm,
                                        clm_expsr_trans.clm_expsr_trans_txt       AS clm_expsr_trans_txt,
                                        clm_expsr_trans.rcvry_ctgy_type_cd        AS rcvry_ctgy_type_cd,
                                        clm_expsr_trans.does_not_erode_rserv_ind  AS does_not_erode_rserv_ind,
                                        clm_expsr_trans.crtd_by_prty_id           AS crtd_by_prty_id,
                                        clm_expsr_trans.gl_mth_num                AS gl_mth_num,
                                        clm_expsr_trans.gl_yr_num                 AS gl_yr_num,
                                        clm_expsr_trans.trty_cd                   AS trty_cd,
                                        clm_expsr_trans.prcs_id                   AS prcs_id,
                                        clm_expsr_trans.clm_expsr_trans_strt_dttm AS clm_expsr_trans_strt_dttm,
                                        clm_expsr_trans.edw_strt_dttm             AS edw_strt_dttm,
                                        clm_expsr_trans.edw_end_dttm              AS edw_end_dttm,
                                        clm_expsr_trans.nk_clm_expsr_trans_id     AS nk_clm_expsr_trans_id
                               FROM     db_t_prod_core.clm_expsr_trans qualify row_number() over( PARTITION BY clm_expsr_trans.nk_clm_expsr_trans_id ORDER BY clm_expsr_trans.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.nk_clm_expsr_trans_id = exp_all_sources.parent_transaction_id 
			qualify row_number() over(PARTITION BY exp_all_sources.source_record_id ORDER BY lkp.clm_expsr_trans_id DESC,lkp.clm_expsr_trans_sbtype_cd DESC,lkp.clm_expsr_id DESC,lkp.expsr_cost_type_cd DESC,lkp.expsr_cost_ctgy_type_cd DESC,lkp.pmt_type_cd DESC,lkp.clm_expsr_trans_dttm DESC,lkp.clm_expsr_trans_txt DESC,lkp.rcvry_ctgy_type_cd DESC,lkp.does_not_erode_rserv_ind DESC,lkp.crtd_by_prty_id DESC,lkp.nk_clm_expsr_trans_id DESC,lkp.gl_mth_num DESC,lkp.gl_yr_num DESC,lkp.trty_cd DESC,lkp.prcs_id DESC,lkp.clm_expsr_trans_strt_dttm DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC) 
            			= 1 );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
             SELECT     lkp_parent_clm_expsr_trans.clm_expsr_trans_id    AS parent_clm_expsr_trans_id,
                        lkp_child_clm_expsr_trans.clm_expsr_trans_id     AS child_clm_expsr_trans_id,
                        $prcs_id                                         AS out_prcs_id,
                        exp_all_sources.retired                          AS retired,
                        ltrim ( rtrim ( exp_all_sources.rltd_role_cd ) ) AS var_rtld_role_cd,
                        lkp_1.tgt_idntftn_val
                        /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_EXPSR_TRANS_RLTD_ROLE_CD */
                        AS var_lkp_rltd_role_cd,
                        CASE
                                   WHEN trim(var_rtld_role_cd) = ''''
                                   OR         var_rtld_role_cd IS NULL
                                   OR         length ( var_rtld_role_cd ) = 0
                                   OR         var_lkp_rltd_role_cd IS NULL THEN ''UNK''
                                   ELSE var_lkp_rltd_role_cd
                        END                              AS out_rltd_role_cd,
                        exp_all_sources.trans_start_date AS trans_start_date,
                        exp_all_sources.source_record_id,
                        row_number() over (PARTITION BY exp_all_sources.source_record_id ORDER BY exp_all_sources.source_record_id) AS rnk
             FROM       exp_all_sources
             inner join lkp_child_clm_expsr_trans
             ON         exp_all_sources.source_record_id = lkp_child_clm_expsr_trans.source_record_id
             inner join lkp_parent_clm_expsr_trans
             ON         lkp_child_clm_expsr_trans.source_record_id = lkp_parent_clm_expsr_trans.source_record_id
             left join  lkp_teradata_etl_ref_xlat_clm_expsr_trans_rltd_role_cd lkp_1
             ON         lkp_1.src_idntftn_val = var_rtld_role_cd 
			 qualify row_number() over (PARTITION BY exp_all_sources.source_record_id ORDER BY exp_all_sources.source_record_id) 
			 = 1 );
  -- Component rtr_invalid_filter_records_VALID, Type ROUTER Output Group VALID
  create or replace TEMPORARY TABLE rtr_invalid_filter_records_valid AS
  SELECT exp_data_transformation.parent_clm_expsr_trans_id AS lkp_parent_clm_expsr_trans_id,
         exp_data_transformation.child_clm_expsr_trans_id  AS lkp_child_clm_expsr_trans_id,
         exp_data_transformation.out_rltd_role_cd          AS out_clm_expsr_trans_rltd_role_cd,
         exp_data_transformation.trans_start_date          AS in_trans_start_date,
         exp_data_transformation.out_prcs_id               AS out_prcs_id,
         exp_data_transformation.retired                   AS retired,
         exp_data_transformation.source_record_id
  FROM   exp_data_transformation
  WHERE
         CASE
                WHEN (
                              exp_data_transformation.parent_clm_expsr_trans_id IS NOT NULL
                       AND    exp_data_transformation.child_clm_expsr_trans_id IS NOT NULL ) THEN TRUE
                ELSE FALSE
         END;
  
  -- Component LKP_CLM_EXPSR_TRANS_RLTD, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_clm_expsr_trans_rltd AS
  (
            SELECT    lkp.parnt_clm_expsr_trans_id,
                      lkp.edw_end_dttm,
                      rtr_invalid_filter_records_valid.source_record_id,
                      row_number() over(PARTITION BY rtr_invalid_filter_records_valid.source_record_id ORDER BY lkp.parnt_clm_expsr_trans_id ASC,lkp.chld_clm_expsr_trans_id ASC,lkp.clm_expsr_trans_rltd_role_cd ASC,lkp.edw_end_dttm ASC) rnk
            FROM      rtr_invalid_filter_records_valid
            left join
                      (
                               SELECT   clm_expsr_trans_rltd.edw_end_dttm                 AS edw_end_dttm,
                                        clm_expsr_trans_rltd.parnt_clm_expsr_trans_id     AS parnt_clm_expsr_trans_id,
                                        clm_expsr_trans_rltd.chld_clm_expsr_trans_id      AS chld_clm_expsr_trans_id,
                                        clm_expsr_trans_rltd.clm_expsr_trans_rltd_role_cd AS clm_expsr_trans_rltd_role_cd
                               FROM     db_t_prod_core.clm_expsr_trans_rltd qualify row_number() over( PARTITION BY parnt_clm_expsr_trans_id,chld_clm_expsr_trans_id, clm_expsr_trans_rltd_role_cd ORDER BY edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.parnt_clm_expsr_trans_id = rtr_invalid_filter_records_valid.lkp_parent_clm_expsr_trans_id
            AND       lkp.chld_clm_expsr_trans_id = rtr_invalid_filter_records_valid.lkp_child_clm_expsr_trans_id
            AND       lkp.clm_expsr_trans_rltd_role_cd = rtr_invalid_filter_records_valid.out_clm_expsr_trans_rltd_role_cd 
			qualify row_number() over(PARTITION BY rtr_invalid_filter_records_valid.source_record_id ORDER BY lkp.parnt_clm_expsr_trans_id ASC,lkp.chld_clm_expsr_trans_id ASC,lkp.clm_expsr_trans_rltd_role_cd ASC,lkp.edw_end_dttm ASC) 
            			= 1 );
  -- Component EXPTRANS, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exptrans AS
  (
             SELECT     lkp_clm_expsr_trans_rltd.parnt_clm_expsr_trans_id AS lkp_parent_clm_expsr_trans_id,
                        CASE
                                   WHEN lkp_clm_expsr_trans_rltd.parnt_clm_expsr_trans_id IS NULL THEN ''I''
                                   ELSE ''R''
                        END                                                               AS flag,
                        lkp_clm_expsr_trans_rltd.edw_end_dttm                             AS lkp_edw_end_dttm,
                        rtr_invalid_filter_records_valid.lkp_parent_clm_expsr_trans_id    AS lkp_parent_clm_expsr_trans_id1,
                        rtr_invalid_filter_records_valid.lkp_child_clm_expsr_trans_id     AS lkp_child_clm_expsr_trans_id1,
                        rtr_invalid_filter_records_valid.out_clm_expsr_trans_rltd_role_cd AS out_clm_expsr_trans_rltd_role_cd1,
                        rtr_invalid_filter_records_valid.out_prcs_id                      AS out_prcs_id1,
                        rtr_invalid_filter_records_valid.retired                          AS retired,
                        rtr_invalid_filter_records_valid.in_trans_start_date              AS in_trans_start_date1,
                        NULL                                                              AS lkp_edw_strt_dttm,
                        rtr_invalid_filter_records_valid.source_record_id
             FROM       rtr_invalid_filter_records_valid
             inner join lkp_clm_expsr_trans_rltd
             ON         rtr_invalid_filter_records_valid.source_record_id = lkp_clm_expsr_trans_rltd.source_record_id );
  -- Component rtr_clm_expsr_trans_rltd_INSERT, Type ROUTER Output Group INSERT
  create or replace TEMPORARY TABLE rtr_clm_expsr_trans_rltd_INSERT as 
  SELECT exptrans.lkp_parent_clm_expsr_trans_id     AS lkp_parent_clm_expsr_trans_id,
         exptrans.lkp_parent_clm_expsr_trans_id1    AS parent_clm_expsr_trans_id,
         exptrans.lkp_child_clm_expsr_trans_id1     AS child_clm_expsr_trans_id,
         exptrans.out_clm_expsr_trans_rltd_role_cd1 AS clm_expsr_trans_rltd_role_cd,
         exptrans.out_prcs_id1                      AS prcs_id,
         exptrans.lkp_edw_end_dttm                  AS lkp_edw_end_dttm,
         exptrans.lkp_edw_strt_dttm                 AS lkp_edw_strt_dttm,
         exptrans.in_trans_start_date1              AS in_trans_strt_dttm,
         exptrans.retired                           AS retired,
         exptrans.flag                              AS flag,
         exptrans.source_record_id
  FROM   exptrans
  WHERE  exptrans.flag = ''I''
  OR     (
                exptrans.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
         AND    exptrans.retired = 0 );
  
  -- Component rtr_clm_expsr_trans_rltd_RETIRED, Type ROUTER Output Group RETIRED
  create or replace temporary table rtr_clm_expsr_trans_rltd_RETIRED as
  SELECT exptrans.lkp_parent_clm_expsr_trans_id     AS lkp_parent_clm_expsr_trans_id,
         exptrans.lkp_parent_clm_expsr_trans_id1    AS parent_clm_expsr_trans_id,
         exptrans.lkp_child_clm_expsr_trans_id1     AS child_clm_expsr_trans_id,
         exptrans.out_clm_expsr_trans_rltd_role_cd1 AS clm_expsr_trans_rltd_role_cd,
         exptrans.out_prcs_id1                      AS prcs_id,
         exptrans.lkp_edw_end_dttm                  AS lkp_edw_end_dttm,
         exptrans.lkp_edw_strt_dttm                 AS lkp_edw_strt_dttm,
         exptrans.in_trans_start_date1              AS in_trans_strt_dttm,
         exptrans.retired                           AS retired,
         exptrans.flag                              AS flag,
         exptrans.source_record_id
  FROM   exptrans
  WHERE  exptrans.flag = ''R''
  AND    exptrans.retired != 0
  AND    exptrans.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component updstr_clm_expsr_trans_rltd_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE updstr_clm_expsr_trans_rltd_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_clm_expsr_trans_rltd_insert.parent_clm_expsr_trans_id    AS parent_clm_expsr_trans_id,
                rtr_clm_expsr_trans_rltd_insert.child_clm_expsr_trans_id     AS child_clm_expsr_trans_id,
                rtr_clm_expsr_trans_rltd_insert.clm_expsr_trans_rltd_role_cd AS clm_expsr_trans_rltd_role_cd,
                rtr_clm_expsr_trans_rltd_insert.prcs_id                      AS prcs_id,
                rtr_clm_expsr_trans_rltd_insert.retired                      AS retired1,
                rtr_clm_expsr_trans_rltd_insert.in_trans_strt_dttm           AS in_trans_strt_dttm1,
                0                                                            AS update_strategy_action,
				rtr_clm_expsr_trans_rltd_insert.source_record_id
         FROM   rtr_clm_expsr_trans_rltd_insert );
  -- Component exp_pass_to_target_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ins AS
  (
         SELECT updstr_clm_expsr_trans_rltd_ins.parent_clm_expsr_trans_id    AS parent_clm_expsr_trans_id,
                updstr_clm_expsr_trans_rltd_ins.child_clm_expsr_trans_id     AS child_clm_expsr_trans_id,
                updstr_clm_expsr_trans_rltd_ins.clm_expsr_trans_rltd_role_cd AS clm_expsr_trans_rltd_role_cd,
                updstr_clm_expsr_trans_rltd_ins.prcs_id                      AS prcs_id,
                current_timestamp                                            AS edw_strt_dttm,
                updstr_clm_expsr_trans_rltd_ins.in_trans_strt_dttm1          AS trans_strt_dttm,
                CASE
                       WHEN updstr_clm_expsr_trans_rltd_ins.retired1 <> 0 THEN updstr_clm_expsr_trans_rltd_ins.in_trans_strt_dttm1
                       ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END AS trans_end_dttm,
                CASE
                       WHEN updstr_clm_expsr_trans_rltd_ins.retired1 <> 0 THEN current_timestamp
                       ELSE to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                END AS edw_end_dttm,
                updstr_clm_expsr_trans_rltd_ins.source_record_id
         FROM   updstr_clm_expsr_trans_rltd_ins );
  -- Component updstr_clm_expsr_trans_rltd_ins1, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE updstr_clm_expsr_trans_rltd_ins1 AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_clm_expsr_trans_rltd_retired.lkp_parent_clm_expsr_trans_id AS parent_clm_expsr_trans_id,
                rtr_clm_expsr_trans_rltd_retired.child_clm_expsr_trans_id      AS child_clm_expsr_trans_id,
                rtr_clm_expsr_trans_rltd_retired.clm_expsr_trans_rltd_role_cd  AS clm_expsr_trans_rltd_role_cd,
                rtr_clm_expsr_trans_rltd_retired.prcs_id                       AS prcs_id,
                rtr_clm_expsr_trans_rltd_retired.in_trans_strt_dttm            AS in_trans_strt_dttm3,
                NULL                                                           AS lkp_edw_strt_dttm,
                1                                                              AS update_strategy_action,
				rtr_clm_expsr_trans_rltd_retired.source_record_id
         FROM   rtr_clm_expsr_trans_rltd_retired );
  -- Component tgt_clm_expsr_trans_rltd, Type TARGET
  INSERT INTO db_t_prod_core.clm_expsr_trans_rltd
              (
                          parnt_clm_expsr_trans_id,
                          chld_clm_expsr_trans_id,
                          clm_expsr_trans_rltd_role_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_target_ins.parent_clm_expsr_trans_id    AS parnt_clm_expsr_trans_id,
         exp_pass_to_target_ins.child_clm_expsr_trans_id     AS chld_clm_expsr_trans_id,
         exp_pass_to_target_ins.clm_expsr_trans_rltd_role_cd AS clm_expsr_trans_rltd_role_cd,
         exp_pass_to_target_ins.prcs_id                      AS prcs_id,
         exp_pass_to_target_ins.edw_strt_dttm                AS edw_strt_dttm,
         exp_pass_to_target_ins.edw_end_dttm                 AS edw_end_dttm,
         exp_pass_to_target_ins.trans_strt_dttm              AS trans_strt_dttm,
         exp_pass_to_target_ins.trans_end_dttm               AS trans_end_dttm
  FROM   exp_pass_to_target_ins;
  
  -- Component exp_pass_to_target_ins1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ins1 AS
  (
         SELECT updstr_clm_expsr_trans_rltd_ins1.parent_clm_expsr_trans_id    AS parent_clm_expsr_trans_id,
                updstr_clm_expsr_trans_rltd_ins1.child_clm_expsr_trans_id     AS child_clm_expsr_trans_id,
                updstr_clm_expsr_trans_rltd_ins1.clm_expsr_trans_rltd_role_cd AS clm_expsr_trans_rltd_role_cd,
                current_timestamp                                             AS edw_end_dttm,
                current_timestamp                                             AS trans_end_dttm,
                updstr_clm_expsr_trans_rltd_ins1.source_record_id
         FROM   updstr_clm_expsr_trans_rltd_ins1 );
  -- Component tgt_clm_expsr_trans_rltd1, Type TARGET
  merge
  INTO         db_t_prod_core.clm_expsr_trans_rltd
  USING        exp_pass_to_target_ins1
  ON (
                            clm_expsr_trans_rltd.parnt_clm_expsr_trans_id = exp_pass_to_target_ins1.parent_clm_expsr_trans_id
               AND          clm_expsr_trans_rltd.chld_clm_expsr_trans_id = exp_pass_to_target_ins1.child_clm_expsr_trans_id
               AND          clm_expsr_trans_rltd.clm_expsr_trans_rltd_role_cd = exp_pass_to_target_ins1.clm_expsr_trans_rltd_role_cd)
  WHEN matched THEN
  UPDATE
  SET    parnt_clm_expsr_trans_id = exp_pass_to_target_ins1.parent_clm_expsr_trans_id,
         chld_clm_expsr_trans_id = exp_pass_to_target_ins1.child_clm_expsr_trans_id,
         clm_expsr_trans_rltd_role_cd = exp_pass_to_target_ins1.clm_expsr_trans_rltd_role_cd,
         edw_end_dttm = exp_pass_to_target_ins1.edw_end_dttm,
         trans_end_dttm = exp_pass_to_target_ins1.trans_end_dttm;

END;
';