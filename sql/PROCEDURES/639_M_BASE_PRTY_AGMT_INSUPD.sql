-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_AGMT_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  PRCS_ID STRING;
  START_DTTM TIMESTAMP;
  END_DTTM TIMESTAMP;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);
  END_DTTM := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
  PRCS_ID := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);
  START_DTTM := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);
  
  -- Component SQ_pc_prty_agmt, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_prty_agmt AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS src_agmt_id,
                $2  AS src_prty_agmt_role_cd,
                $3  AS src_prty_id,
                $4  AS src_eff_dt,
                $5  AS src_end_dt,
                $6  AS src_retired,
                $7  AS src_trans_strt_dttm,
                $8  AS src_uslicensevalid_alfa,
                $9  AS src_training_class_type_cd,
                $10 AS tgt_agmt_id,
                $11 AS tgt_prty_agmt_role_cd,
                $12 AS tgt_prty_id,
                $13 AS tgt_prty_agmt_strt_dttm,
                $14 AS tgt_prty_agmt_end_dttm,
                $15 AS tgt_trng_type_cd,
                $16 AS tgt_vld_drvrs_lic_ind,
                $17 AS tgt_edw_strt_dttm,
                $18 AS tgt_edw_end_dttm,
                $19 AS src_md5,
                $20 AS tgt_md5,
                $21 AS calc_ins_upd,
                $22 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     ( WITH prty_agmt_temp AS
                                  (
                                         SELECT publicid,
                                                policynumber,
                                                naiccode,
                                                party_type,
                                                eff_dt,
                                                end_dt,
                                                src_sys_cd,
                                                retired,
                                                updatetime,
                                                uslicensevalid_alfa ,
                                                lkp_prty_type_id,
                                                prty_agmt_type_cd,
                                                src_prty_agmt_role_cd,
                                                busn_ctgy_cd,
                                                training_class_type_cd,
                                                agmt_id AS src_agmt_id,
                                                v_prty_id,
                                                busn_prty_id,
                                                CASE
                                                       WHEN (
                                                                     lkp_prty_type_id =''insured''
                                                              AND    v_prty_id IS NULL) THEN busn_prty_id
                                                       ELSE v_prty_id
                                                END AS src_prty_id
                                         FROM   (
                                                       SELECT publicid,
                                                              policynumber,
                                                              naiccode,
                                                              party_type,
                                                              eff_dt,
                                                              end_dt,
                                                              src_sys_cd,
                                                              retired,
                                                              updatetime,
                                                              uslicensevalid_alfa ,
                                                              lkp_prty_type_id,
                                                              prty_agmt_type_cd,
                                                              src_prty_agmt_role_cd,
                                                              busn_ctgy_cd,
                                                              training_class_type_cd ,
                                                              intrnl_org_prty_id ,
                                                              agmt_id ,
                                                              CASE
                                                                     WHEN upper(intrnl_org_sbtype) IN (''CO'',
                                                                                                       ''AGT'',
                                                                                                       ''SRVCCTR'',
                                                                                                       ''PRDA'',
                                                                                                       ''UWRTDSTRCT'') THEN intrnl_org_prty_id
                                                                     WHEN lkp_prty_type_id =''Company'' THEN busn_prty_id
                                                                     WHEN busn_ctgy_cd =''INSCAR'' THEN busn_prty_id
                                                                     WHEN (
                                                                                   lkp_prty_type_id =''Person''
                                                                            AND    (
                                                                                          substr(party_type,position(''-'',party_type)+1,length(party_type))=''CM'')) THEN indiv_cnt_mgr_id
                                                                     WHEN upper(lkp_prty_type_id) =''REGION'' THEN intrnl_org_prty_id
                                                                     WHEN (
                                                                                   lkp_prty_type_id IN (''UserContact'',
                                                                                                        ''insured'')
                                                                            OR     (
                                                                                          lkp_prty_type_id =''Person''
                                                                                   AND    (
                                                                                                 substr(party_type,position(''-'',party_type)+1,length(party_type))=''CC''))) THEN indiv_prty_id_usrcnt
                                                              END AS v_prty_id,
                                                              busn_prty_id
                                                       FROM   (
                                                                              SELECT          publicid,
                                                                                              policynumber,
                                                                                              naiccode,
                                                                                              party_type,
                                                                                              prty_agmt_role_cd,
                                                                                              eff_dt,
                                                                                              CASE
                                                                                                              WHEN end_dt IS NULL THEN to_timestamp_ntz(''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DDBHH:MI:SS.FF6'' )
                                                                                                              ELSE end_dt
                                                                                              END AS end_dt,
                                                                                              src_inr.src_sys_cd,
                                                                                              retired,
                                                                                              updatetime,
                                                                                              CASE
                                                                                                              WHEN uslicensevalid_alfa=0 THEN ''N''
                                                                                                              WHEN uslicensevalid_alfa=1 THEN ''Y''
                                                                                                              ELSE NULL
                                                                                              END AS uslicensevalid_alfa,
                                                                                              lkp_prty_type_id,
                                                                                              prty_agmt_type_cd,
                                                                                              src_prty_agmt_role_cd,
                                                                                              busn_ctgy_cd,
                                                                                              CASE
                                                                                                              WHEN training_class_type_cd IS NULL THEN ''UNK''
                                                                                                              ELSE training_class_type_cd
                                                                                              END                                AS training_class_type_cd ,
                                                                                              indiv_usrcnt.indiv_prty_id         AS indiv_prty_id_usrcnt,
                                                                                              intrnl_org_prty.intrnl_org_prty_id AS intrnl_org_prty_id,
                                                                                              indiv_cnt_mgr.indiv_prty_id        AS indiv_cnt_mgr_id,
                                                                                              busn_prty.busn_prty_id             AS busn_prty_id,
                                                                                              intrnl_org_sbtype,
                                                                                              intrnl_org_type,
                                                                                              CASE
                                                                                                              WHEN agmt_act.agmt_act_id IS NULL THEN agmt_inv_ppv.agmt_pol_id
                                                                                                              WHEN agmt_act.agmt_act_id IS NOT NULL THEN agmt_act.agmt_act_id
                                                                                              END AS agmt_id
                                                                              FROM            (
                                                                                                              SELECT          publicid                             AS publicid,
                                                                                                                              policynumber                         AS policynumber ,
                                                                                                                              naiccode                             AS naiccode,
                                                                                                                              party_type                           AS party_type,
                                                                                                                              x.prty_agmt_role_cd                  AS prty_agmt_role_cd,
                                                                                                                              max(x.eff_dt)                        AS eff_dt,
                                                                                                                              max(x.end_dt)                        AS end_dt,
                                                                                                                              x.src_sys_cd                         AS src_sys_cd,
                                                                                                                              retired                              AS retired,
                                                                                                                              max(updatetime)                      AS updatetime ,
                                                                                                                              trainingclasstype_typecode           AS trainingclasstype_typecode,
                                                                                                                              cast(uslicensevalid_alfa AS INTEGER)    uslicensevalid_alfa ,
                                                                                                                              CASE
                                                                                                                                              WHEN publicid IS NULL THEN ''ACT''
                                                                                                                                              WHEN policynumber IS NULL THEN ''INV''
                                                                                                                                              ELSE ''PPV''
                                                                                                                              END AS prty_agmt_type_cd ,
                                                                                                                              CASE
                                                                                                                                              WHEN (
                                                                                                                                                                              (
                                                                                                                                                                                              position(''-'',party_type)=0)
                                                                                                                                                              OR              (
                                                                                                                                                                                              party_type IS NULL)
                                                                                                                                                              OR              (
                                                                                                                                                                                              party_type = '' '')
                                                                                                                                                              OR              (
                                                                                                                                                                                              length(party_type)=0)) THEN party_type
                                                                                                                                              ELSE (substr(party_type,1,position(''-'',party_type)-1))
                                                                                                                              END AS lkp_prty_type_id ,
                                                                                                                              CASE
                                                                                                                                              WHEN xlat_teradata_etl_ref.tgt_idntftn_val IS NULL THEN ''UNK''
                                                                                                                                              ELSE xlat_teradata_etl_ref.tgt_idntftn_val
                                                                                                                              END                                            AS src_prty_agmt_role_cd ,
                                                                                                                              xlat_teradata_busn_ctgy.tgt_idntftn_val        AS busn_ctgy_cd ,
                                                                                                                              xlat_teradata_train_class_type.tgt_idntftn_val AS training_class_type_cd ,
                                                                                                                              xlat_intrnal_org_sbtype.tgt_idntftn_val        AS intrnl_org_sbtype ,
                                                                                                                              xlat_intrnal_org_type.tgt_idntftn_val          AS intrnl_org_type
                                                                                                              FROM            (
                                                                                                                                              SELECT DISTINCT pc_prty_agmt.publicid,
                                                                                                                                                              pc_prty_agmt.policynumber,
                                                                                                                                                              pc_prty_agmt.naiccode,
                                                                                                                                                              pc_prty_agmt.party_type,
                                                                                                                                                              pc_prty_agmt.prty_agmt_role_cd,
                                                                                                                                                              pc_prty_agmt.eff_dt,
                                                                                                                                                              pc_prty_agmt.end_dt,
                                                                                                                                                              pc_prty_agmt.src_sys_cd,
                                                                                                                                                              pc_prty_agmt.retired AS retired,
                                                                                                                                                              pc_prty_agmt.updatetime,
                                                                                                                                                              pc_prty_agmt.trainingclasstype_typecode,
                                                                                                                                                              pc_prty_agmt.uslicensevalid_alfa
                                                                                                                                              FROM            (
                                                                                                                                                                              SELECT DISTINCT cast(b.billingreferencenumber_alfa_stg AS VARCHAR(100)) AS publicid,
                                                                                                                                                                                              cast(NULL AS                              VARCHAR(100)) AS policynumber,
                                                                                                                                                                                              cast(
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN (
                                                                                                                                                                                                        e.externalid_stg IS NULL) THEN e.publicid_stg
                                                                                                                                                                                                        ELSE e.externalid_stg
                                                                                                                                                                                              END AS VARCHAR(50))AS naiccode,
                                                                                                                                                                                              cast(t.typecode_stg
                                                                                                                                                                                                        ||''-CC'' AS VARCHAR(100))AS party_type,
                                                                                                                                                                                              cast(
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN(
                                                                                                                                                                                                        b.overridingpayer_alfa_stg IS NULL) THEN ''PRTY_AGMT_ROLE8''
                                                                                                                                                                                                        ELSE''PRTY_AGMT_ROLE10''
                                                                                                                                                                                              END AS VARCHAR(50))                                AS prty_agmt_role_cd,
                                                                                                                                                                                              b.createtime_stg                                   AS eff_dt,
                                                                                                                                                                                              cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN a.updatetime_stg > e.updatetime_stg THEN a.updatetime_stg
                                                                                                                                                                                                        ELSE e.updatetime_stg
                                                                                                                                                                                              END AS updatetime,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN b.retired_stg=0
                                                                                                                                                                                                        AND             e.retired_stg=0 THEN 0
                                                                                                                                                                                                        ELSE 1
                                                                                                                                                                                              END                              AS retired,
                                                                                                                                                                                              cast(''SRC_SYS5'' AS VARCHAR(50))  AS src_sys_cd,
                                                                                                                                                                                              cast(NULL AS       VARCHAR(100)) AS trainingclasstype_typecode,
                                                                                                                                                                                              cast(NULL AS       VARCHAR(100)) AS uslicensevalid_alfa
                                                                                                                                                                              FROM            db_t_prod_stag.bc_account a
                                                                                                                                                                              inner join      db_t_prod_stag.bc_invoicestream b
                                                                                                                                                                              ON              a.id_stg = b.accountid_stg
                                                                                                                                                                              inner join      db_t_prod_stag.bc_accountcontact c
                                                                                                                                                                              ON              c.id_stg = b.overridingpayer_alfa_stg
                                                                                                                                                                              inner join      db_t_prod_stag.bc_contact e
                                                                                                                                                                              ON              e.id_stg = c.contactid_stg
                                                                                                                                                                              join            db_t_prod_stag.bctl_contact t
                                                                                                                                                                              ON              t.id_stg=e.subtype_stg
                                                                                                                                                                              WHERE           b.overridingpayer_alfa_stg IS NOT NULL
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        b.updatetime_stg>(:start_dttm)
                                                                                                                                                                                                        AND             b.updatetime_stg <=(:end_dttm))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        e.updatetime_stg>(:start_dttm)
                                                                                                                                                                                                        AND             e.updatetime_stg <= (:end_dttm))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        a.updatetime_stg>(:start_dttm)
                                                                                                                                                                                                        AND             a.updatetime_stg <= (:end_dttm)))
                                                                                                                                                                              UNION ALL
                                                                                                                                                                              /* Primary Payer (this is at the Invoicestream level)*/
                                                                                                                                                                              SELECT DISTINCT b.billingreferencenumber_alfa_stg AS publicid,
                                                                                                                                                                                              cast(NULL AS VARCHAR(100))           policynumber,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN (
                                                                                                                                                                                                        e.externalid_stg IS NULL) THEN e.publicid_stg
                                                                                                                                                                                                        ELSE e.externalid_stg
                                                                                                                                                                                              END AS naiccode,
                                                                                                                                                                                              t.typecode_stg
                                                                                                                                                                                                        ||''-CC'' AS party_type,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN(
                                                                                                                                                                                                        b.overridingpayer_alfa_stg IS NULL) THEN ''PRTY_AGMT_ROLE8''
                                                                                                                                                                                                        ELSE''PRTY_AGMT_ROLE10''
                                                                                                                                                                                              END                                                AS prty_agmt_role_cd,
                                                                                                                                                                                              b.createtime_stg                                   AS eff_dt,
                                                                                                                                                                                              cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN a.updatetime_stg > e.updatetime_stg THEN a.updatetime_stg
                                                                                                                                                                                                        ELSE e.updatetime_stg
                                                                                                                                                                                              END AS updatetime,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN b.retired_stg=0
                                                                                                                                                                                                        AND             e.retired_stg=0 THEN 0
                                                                                                                                                                                                        ELSE 1
                                                                                                                                                                                              END                              AS retired,
                                                                                                                                                                                              cast(''SRC_SYS5'' AS VARCHAR(50))  AS src_sys_cd,
                                                                                                                                                                                              cast(NULL AS       VARCHAR(100)) AS trainingclasstype_typecode,
                                                                                                                                                                                              cast(NULL AS       VARCHAR(100)) AS uslicensevalid_alfa
                                                                                                                                                                              FROM            db_t_prod_stag.bc_account a
                                                                                                                                                                              inner join      db_t_prod_stag.bc_invoicestream b
                                                                                                                                                                              ON              a.id_stg = b.accountid_stg
                                                                                                                                                                              inner join      db_t_prod_stag.bc_accountcontact c
                                                                                                                                                                              ON              c.accountid_stg=a.id_stg
                                                                                                                                                                              inner join      db_t_prod_stag.bc_contact e
                                                                                                                                                                              ON              e.id_stg = c.contactid_stg
                                                                                                                                                                              join            db_t_prod_stag.bctl_contact t
                                                                                                                                                                              ON              t.id_stg=e.subtype_stg
                                                                                                                                                                              WHERE           b.overridingpayer_alfa_stg IS NULL
                                                                                                                                                                              AND             c.primarypayer_stg = 1
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        b.updatetime_stg>(:start_dttm)
                                                                                                                                                                                                        AND             b.updatetime_stg <=(:end_dttm))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        e.updatetime_stg>(:start_dttm)
                                                                                                                                                                                                        AND             e.updatetime_stg <= (:end_dttm))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        a.updatetime_stg>(:start_dttm)
                                                                                                                                                                                                        AND             a.updatetime_stg <= (:end_dttm)))
                                                                                                                                                                              UNION ALL
                                                                                                                                                                              /*Primary and Secondary Payer (this is at the Account level)*/
                                                                                                                                                                              SELECT DISTINCT cast(NULL AS VARCHAR(255)) AS publicid,
                                                                                                                                                                                              a.accountnumber_stg        AS policynumber,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN(
                                                                                                                                                                                                        d.externalid_stg IS NOT NULL) THEN d.externalid_stg
                                                                                                                                                                                                        ELSE d.publicid_stg
                                                                                                                                                                                              END AS naiccode,
                                                                                                                                                                                              t.typecode_stg
                                                                                                                                                                                                        ||''-CC'' AS party_type,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN(
                                                                                                                                                                                                        h.primarypayer_stg = 1) THEN ''PRTY_AGMT_ROLE8''
                                                                                                                                                                                                        ELSE ''PRTY_AGMT_ROLE9''
                                                                                                                                                                                              END                                                AS prty_agmt_role_cd,
                                                                                                                                                                                              h.createtime_stg                                   AS eff_dt,
                                                                                                                                                                                              cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN a.updatetime_stg > d.updatetime_stg THEN a.updatetime_stg
                                                                                                                                                                                                        ELSE d.updatetime_stg
                                                                                                                                                                                              END AS updatetime,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN a.retired_stg=0
                                                                                                                                                                                                        AND             d.retired_stg=0 THEN 0
                                                                                                                                                                                                        ELSE 1
                                                                                                                                                                                              END                              AS retired,
                                                                                                                                                                                              cast(''SRC_SYS5'' AS VARCHAR(50))  AS src_sys_cd,
                                                                                                                                                                                              cast(NULL AS       VARCHAR(100)) AS trainingclasstype_typecode,
                                                                                                                                                                                              cast(NULL AS       VARCHAR(100)) AS uslicensevalid_alfa
                                                                                                                                                                              FROM            db_t_prod_stag.bc_account a
                                                                                                                                                                              inner join      db_t_prod_stag.bc_accountcontact h
                                                                                                                                                                              ON              h.accountid_stg = a.id_stg
                                                                                                                                                                              inner join      db_t_prod_stag.bc_contact d
                                                                                                                                                                              ON              d.id_stg = h.contactid_stg
                                                                                                                                                                              join            db_t_prod_stag.bctl_contact t
                                                                                                                                                                              ON              t.id_stg=d.subtype_stg
                                                                                                                                                                              left join       db_t_prod_stag.bc_accountcontactrole i
                                                                                                                                                                              ON              i.accountcontactid_stg = h.id_stg
                                                                                                                                                                              left join       db_t_prod_stag.bctl_accountrole j
                                                                                                                                                                              ON              j.id_stg = i.role_stg
                                                                                                                                                                              WHERE           ((
                                                                                                                                                                                                        h.primarypayer_stg = 1)
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        j.name_stg = ''Payer''))
                                                                                                                                                                              AND            ((
                                                                                                                                                                                                        d.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                        AND             d.updatetime_stg <= ( :end_dttm))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        a.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                        AND             a.updatetime_stg <= ( :end_dttm)))
                                                                                                                                                                              UNION ALL
                                                                                                                                                                              /****************** Change the DB_T_PROD_STAG.cc_claim to DB_T_PROD_STAG.cctl_claimstate as follows *************************/
                                                                                                                                                                              SELECT     cast(cc_policy.id_stg AS VARCHAR(100)) AS publicid ,
                                                                                                                                                                                         policynumber_stg                       AS policynumber,
                                                                                                                                                                                         cc_contact.publicid_stg                AS naiccode,
                                                                                                                                                                                         cctl_contact.typecode_stg
                                                                                                                                                                                                    ||''-CC''                                 AS party_type,
                                                                                                                                                                                         cctl_contactrole.typecode_stg                      AS prty_agmt_role_cd,
                                                                                                                                                                                         cc_claimcontact.createtime_stg                     AS eff_dt,
                                                                                                                                                                                         cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt,
                                                                                                                                                                                         cc_claimcontact.updatetime_stg                     AS updatetime,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN cc_claim.retired_stg=0
                                                                                                                                                                                                    AND        cc_policy.retired_stg=0
                                                                                                                                                                                                    AND        cc_claimcontact.retired_stg=0 THEN 0
                                                                                                                                                                                                    ELSE 1
                                                                                                                                                                                         END                              AS retired,
                                                                                                                                                                                         cast(''SRC_SYS6'' AS VARCHAR(50))  AS src_sys_cd,
                                                                                                                                                                                         cast(NULL AS       VARCHAR(100)) AS trainingclasstype_typecode,
                                                                                                                                                                                         cast(NULL AS       VARCHAR(100)) AS uslicensevalid_alfa
                                                                                                                                                                              FROM       db_t_prod_stag.cc_claim
                                                                                                                                                                              inner join db_t_prod_stag.cctl_claimstate
                                                                                                                                                                              ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                                                                              join       db_t_prod_stag.cc_claimcontact
                                                                                                                                                                              ON         cc_claim.id_stg = cc_claimcontact.claimid_stg
                                                                                                                                                                              join       db_t_prod_stag.cc_claimcontactrole
                                                                                                                                                                              ON         cc_claimcontactrole.claimcontactid_stg = cc_claimcontact.id_stg
                                                                                                                                                                              join       db_t_prod_stag.cc_contact
                                                                                                                                                                              ON         cc_contact.id_stg = cc_claimcontact.contactid_stg
                                                                                                                                                                              join       db_t_prod_stag.cctl_contact
                                                                                                                                                                              ON         cctl_contact.id_stg= cc_contact.subtype_stg
                                                                                                                                                                              join       db_t_prod_stag.cctl_contactrole
                                                                                                                                                                              ON         cc_claimcontactrole.role_stg = cctl_contactrole.id_stg
                                                                                                                                                                              join       db_t_prod_stag.cc_policy
                                                                                                                                                                              ON         cc_policy.id_stg=cc_claim.policyid_stg
                                                                                                                                                                              AND        cctl_contactrole.typecode_stg IN (''insured'',
                                                                                                                                                                                                        ''AdditionalInsured_alfa'',
                                                                                                                                                                                                        ''AdditionalNamedInsured_alfa'')
                                                                                                                                                                              AND       ( (
                                                                                                                                                                                                        cc_policy.verified_stg = 0
                                                                                                                                                                                                    AND        coalesce( legacypolind_alfa_stg,0)<>1)
                                                                                                                                                                                         OR         (
                                                                                                                                                                                                        coalesce( legacypolind_alfa_stg,0)=1))
                                                                                                                                                                              AND        ((
                                                                                                                                                                                                        cc_claimcontact.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                    AND        cc_claimcontact.updatetime_stg <= ( :end_dttm))
                                                                                                                                                                                         OR         (
                                                                                                                                                                                                        cc_policy.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                    AND        cc_policy.updatetime_stg <= ( :end_dttm)))
                                                                                                                                                                              WHERE      cctl_claimstate.name_stg <> ''Draft''
                                                                                                                                                                              UNION
                                                                                                                                                                              /******** Underwriting Company ********** */
                                                                                                                                                                              SELECT cast(cc_policy.id_stg AS VARCHAR(100))             AS publicid ,
                                                                                                                                                                                     cc_policy.policynumber_stg                         AS policynumber,
                                                                                                                                                                                     cctl_underwritingcompanytype.typecode_stg          AS naiccode,
                                                                                                                                                                                     ''INTRNL_ORG_SBTYPE1''                               AS party_type,
                                                                                                                                                                                     ''PRTY_AGMT_ROLE1''                                  AS prty_agmt_role_cd,
                                                                                                                                                                                     cc_policy.createtime_stg                           AS eff_dt,
                                                                                                                                                                                     cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt,
                                                                                                                                                                                     cc_policy.updatetime_stg                           AS updatetime,
                                                                                                                                                                                     CASE
                                                                                                                                                                                            WHEN cc_policy.retired_stg=0 THEN 0
                                                                                                                                                                                            ELSE 1
                                                                                                                                                                                     END                              AS retired,
                                                                                                                                                                                     cast(''SRC_SYS6'' AS VARCHAR(50))  AS src_sys_cd,
                                                                                                                                                                                     cast(NULL AS       VARCHAR(100)) AS trainingclasstype_typecode,
                                                                                                                                                                                     cast(NULL AS       VARCHAR(100)) AS uslicensevalid_alfa
                                                                                                                                                                              FROM   db_t_prod_stag.cc_policy
                                                                                                                                                                              join   db_t_prod_stag.cctl_underwritingcompanytype
                                                                                                                                                                              ON     cc_policy.underwritingco_stg = cctl_underwritingcompanytype.id_stg
                                                                                                                                                                              WHERE  ( (
                                                                                                                                                                                                   cc_policy.verified_stg = 0
                                                                                                                                                                                            AND    coalesce( legacypolind_alfa_stg,0)<>1)
                                                                                                                                                                                     OR     (
                                                                                                                                                                                                   coalesce( legacypolind_alfa_stg,0)=1))
                                                                                                                                                                              AND    cc_policy.updatetime_stg > ( :start_dttm)
                                                                                                                                                                              AND    cc_policy.updatetime_stg <= ( :end_dttm)
                                                                                                                                                                              UNION
                                                                                                                                                                              /******Producer************/
                                                                                                                                                                              SELECT cast(cc_policy.id_stg AS VARCHAR(100))             AS publicid,
                                                                                                                                                                                     cc_policy.policynumber_stg                         AS policynumber,
                                                                                                                                                                                     cc_policy.producercode_stg                         AS naiccode,
                                                                                                                                                                                     ''INTRNL_ORG_SBTYPE2''                               AS party_type,
                                                                                                                                                                                     ''PRTY_AGMT_ROLE5''                                  AS prty_agmt_role_cd,
                                                                                                                                                                                     cc_policy.createtime_stg                           AS eff_dt,
                                                                                                                                                                                     cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt,
                                                                                                                                                                                     cc_policy.updatetime_stg                           AS updatetime,
                                                                                                                                                                                     CASE
                                                                                                                                                                                            WHEN cc_policy.retired_stg=0 THEN 0
                                                                                                                                                                                            ELSE 1
                                                                                                                                                                                     END                              AS retired,
                                                                                                                                                                                     cast(''SRC_SYS6'' AS VARCHAR(50))  AS src_sys_cd,
                                                                                                                                                                                     cast(NULL AS       VARCHAR(100)) AS trainingclasstype_typecode,
                                                                                                                                                                                     cast(NULL AS       VARCHAR(100)) AS uslicensevalid_alfa
                                                                                                                                                                              FROM   db_t_prod_stag.cc_policy
                                                                                                                                                                              WHERE  ( (
                                                                                                                                                                                                   cc_policy.verified_stg = 0
                                                                                                                                                                                            AND    coalesce( legacypolind_alfa_stg,0)<>1)
                                                                                                                                                                                     OR     (
                                                                                                                                                                                                   coalesce( legacypolind_alfa_stg,0)=1))
                                                                                                                                                                              AND    cc_policy.producercode_stg IS NOT NULL
                                                                                                                                                                              AND    cc_policy.updatetime_stg > ( :start_dttm)
                                                                                                                                                                              AND    cc_policy.updatetime_stg <= ( :end_dttm)
                                                                                                                                                                              UNION ALL
                                                                                                                                                                              /****** UW Company************/
                                                                                                                                                                              SELECT     pc_policyperiod.publicid_stg              AS publicid,
                                                                                                                                                                                         policynumber_stg                          AS policynumber,
                                                                                                                                                                                         pctl_uwcompanycode.typecode_stg           AS naiccode,
                                                                                                                                                                                         cast(''INTRNL_ORG_SBTYPE1'' AS VARCHAR(50)) AS party_type,
                                                                                                                                                                                         cast(''PRTY_AGMT_ROLE1'' AS    VARCHAR(100))AS prty_agmt_role_cd,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_policyperiod.editeffectivedate_stg IS NULL THEN pc_policyperiod.periodstart_stg
                                                                                                                                                                                                    ELSE pc_policyperiod.editeffectivedate_stg
                                                                                                                                                                                         END                            AS eff_dt,
                                                                                                                                                                                         pc_policyperiod.periodend_stg  AS end_dt,
                                                                                                                                                                                         pc_policyperiod.updatetime_stg AS updatetime,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_policyperiod.retired_stg=0
                                                                                                                                                                                                    AND        pc_uwcompany.retired_stg=0 THEN 0
                                                                                                                                                                                                    ELSE 1
                                                                                                                                                                                         END                              AS retired,
                                                                                                                                                                                         cast(''SRC_SYS4'' AS VARCHAR(50))  AS src_sys_cd,
                                                                                                                                                                                         cast(NULL AS       VARCHAR(100)) AS trainingclasstypecode_stg,
                                                                                                                                                                                         cast(NULL AS       VARCHAR(100)) AS uslicensevalid_alfa_stg
                                                                                                                                                                              FROM       db_t_prod_stag.pc_policyperiod
                                                                                                                                                                              inner join db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                                                              ON         pctl_policyperiodstatus.id_stg=pc_policyperiod.status_stg
                                                                                                                                                                              inner join db_t_prod_stag.pc_uwcompany
                                                                                                                                                                              ON         pc_uwcompany.id_stg = pc_policyperiod.uwcompany_stg
                                                                                                                                                                              inner join db_t_prod_stag.pctl_uwcompanycode
                                                                                                                                                                              ON         pctl_uwcompanycode.id_stg=pc_uwcompany.code_stg
                                                                                                                                                                              inner join db_t_prod_stag.pc_job
                                                                                                                                                                              ON         pc_job.id_stg=pc_policyperiod.jobid_stg
                                                                                                                                                                              inner join db_t_prod_stag.pctl_job
                                                                                                                                                                              ON         pctl_job.id_stg=pc_job.subtype_stg
                                                                                                                                                                              WHERE      pctl_policyperiodstatus.typecode_stg=''Bound''
                                                                                                                                                                              AND        pc_policyperiod.updatetime_stg > ( :start_dttm)
                                                                                                                                                                              AND        pc_policyperiod.updatetime_stg <= ( :end_dttm)
                                                                                                                                                                              UNION
                                                                                                                                                                              /******SALES REGION********/
                                                                                                                                                                              SELECT     pc_policyperiod.publicid_stg            AS publicid,
                                                                                                                                                                                         pc_policyperiod.policynumber_stg        AS policynumber,
                                                                                                                                                                                         reg.name_stg                            AS naiccode,
                                                                                                                                                                                         pctl_grouptype.typecode_stg             AS party_type,
                                                                                                                                                                                         cast(''PRTY_AGMT_ROLE14''AS VARCHAR(100))    prty_agmt_role_cd,
                                                                                                                                                                                         pc_policyperiod.editeffectivedate_stg   AS eff_dt,
                                                                                                                                                                                         cast(NULL AS timestamp(6))              AS end_dt,
                                                                                                                                                                                         pc_policyperiod.updatetime_stg          AS updatetime,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_policyperiod.retired_stg=0
                                                                                                                                                                                                    AND        reg.retired_stg=0 THEN 0
                                                                                                                                                                                                    ELSE 1
                                                                                                                                                                                         END                              AS retired,
                                                                                                                                                                                         cast(''SRC_SYS4'' AS VARCHAR(50))  AS src_sys_cd,
                                                                                                                                                                                         cast(NULL AS       VARCHAR(100)) AS trainingclasstypecode_stg,
                                                                                                                                                                                         cast(NULL AS       VARCHAR(100)) AS uslicensevalid_alfa_stg
                                                                                                                                                                              FROM       db_t_prod_stag.pc_policyperiod
                                                                                                                                                                              join       db_t_prod_stag.pc_effectivedatedfields
                                                                                                                                                                              ON         pc_policyperiod.id_stg=pc_effectivedatedfields.branchid_stg
                                                                                                                                                                              join       db_t_prod_stag.pc_group AS sc
                                                                                                                                                                              ON         sc.id_stg=pc_effectivedatedfields.servicecenter_alfa_stg
                                                                                                                                                                              join       db_t_prod_stag.pc_parentgroup AS dist
                                                                                                                                                                              ON         dist.ownerid_stg=sc.id_stg
                                                                                                                                                                              join       db_t_prod_stag.pc_group AS sales_dist
                                                                                                                                                                              ON         sales_dist.id_stg=dist.foreignentityid_stg
                                                                                                                                                                              join       db_t_prod_stag.pc_parentgroup dist_reg
                                                                                                                                                                              ON         dist_reg.ownerid_stg=sales_dist.id_stg
                                                                                                                                                                              join       db_t_prod_stag.pc_group reg
                                                                                                                                                                              ON         reg.id_stg =dist_reg.foreignentityid_stg
                                                                                                                                                                              inner join db_t_prod_stag.pctl_grouptype
                                                                                                                                                                              ON         pctl_grouptype.id_stg=reg.grouptype_stg
                                                                                                                                                                              inner join db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                                                              ON         pctl_policyperiodstatus.id_stg= pc_policyperiod.status_stg
                                                                                                                                                                              WHERE      pctl_policyperiodstatus.typecode_stg=''Bound''
                                                                                                                                                                              AND        pc_policyperiod.updatetime_stg > ( :start_dttm)
                                                                                                                                                                              AND        pc_policyperiod.updatetime_stg <= ( :end_dttm)
                                                                                                                                                                              UNION
                                                                                                                                                                              /******Producer************/
                                                                                                                                                                              SELECT     pc_policyperiod.publicid_stg               AS publicid,
                                                                                                                                                                                         policynumber_stg                           AS policynumber,
                                                                                                                                                                                         pc_producercode.code_stg                   AS naiccode,
                                                                                                                                                                                         cast(''INTRNL_ORG_SBTYPE2'' AS VARCHAR(50))  AS party_type,
                                                                                                                                                                                         cast(''PRTY_AGMT_ROLE5'' AS    VARCHAR(100)) AS prty_agmt_role_cd,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_policyperiod.editeffectivedate_stg IS NULL THEN pc_policyperiod.periodstart_stg
                                                                                                                                                                                                    ELSE pc_policyperiod.editeffectivedate_stg
                                                                                                                                                                                         END AS eff_dt,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_effectivedatedfields.expirationdate_stg IS NULL THEN pc_policyperiod.periodend_stg
                                                                                                                                                                                                    ELSE pc_effectivedatedfields.expirationdate_stg
                                                                                                                                                                                         END AS end_dt,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN (
                                                                                                                                                                                                        pc_effectivedatedfields.updatetime_stg > pc_policyperiod.updatetime_stg ) THEN pc_effectivedatedfields.updatetime_stg
                                                                                                                                                                                                    ELSE pc_policyperiod.updatetime_stg
                                                                                                                                                                                         END AS updatetime,
                                                                                                                                                                                         /*Added for EIM-18200 */
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_policyperiod.retired_stg=0
                                                                                                                                                                                                    AND        pc_producercode.retired_stg=0 THEN 0
                                                                                                                                                                                                    ELSE 1
                                                                                                                                                                                         END                              AS retired,
                                                                                                                                                                                         cast(''SRC_SYS4'' AS VARCHAR(50))  AS src_sys_cd,
                                                                                                                                                                                         cast(NULL AS       VARCHAR(100)) AS trainingclasstypecode_stg,
                                                                                                                                                                                         cast(NULL AS       VARCHAR(100)) AS uslicensevalid_alfa_stg
                                                                                                                                                                              FROM       db_t_prod_stag.pc_policyperiod
                                                                                                                                                                              inner join db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                                                              ON         pctl_policyperiodstatus.id_stg=pc_policyperiod.status_stg
                                                                                                                                                                              inner join db_t_prod_stag.pc_effectivedatedfields
                                                                                                                                                                              ON         pc_policyperiod.id_stg=pc_effectivedatedfields.branchid_stg
                                                                                                                                                                              inner join db_t_prod_stag.pc_producercode
                                                                                                                                                                              ON         pc_producercode.id_stg = pc_effectivedatedfields.producercodeid_stg
                                                                                                                                                                              inner join db_t_prod_stag.pc_job
                                                                                                                                                                              ON         pc_job.id_stg=pc_policyperiod.jobid_stg
                                                                                                                                                                              inner join db_t_prod_stag.pctl_job
                                                                                                                                                                              ON         pctl_job.id_stg=pc_job.subtype_stg
                                                                                                                                                                              WHERE      pctl_policyperiodstatus.typecode_stg=''Bound''
                                                                                                                                                                              AND        pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                                                                                                              AND        ((
                                                                                                                                                                                                        pc_effectivedatedfields.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                    AND        pc_effectivedatedfields.updatetime_stg <= ( :end_dttm))
                                                                                                                                                                                         OR         (
                                                                                                                                                                                                        pc_policyperiod.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                    AND        pc_policyperiod.updatetime_stg <= ( :end_dttm)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /******Service Center************/
                                                                                                                                                                              SELECT     pc_policyperiod.publicid_stg            AS publicid,
                                                                                                                                                                                         policynumber_stg                        AS policynumber,
                                                                                                                                                                                         pc_group.name_stg                       AS naiccode,
                                                                                                                                                                                         pctl_grouptype.typecode_stg             AS party_type,
                                                                                                                                                                                         cast(''PRTY_AGMT_ROLE4'' AS VARCHAR(100)) AS prty_agmt_role_cd,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_policyperiod.editeffectivedate_stg IS NULL THEN pc_policyperiod.periodstart_stg
                                                                                                                                                                                                    ELSE pc_policyperiod.editeffectivedate_stg
                                                                                                                                                                                         END AS eff_dt,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_effectivedatedfields.expirationdate_stg IS NULL THEN pc_policyperiod.periodend_stg
                                                                                                                                                                                                    ELSE pc_effectivedatedfields.expirationdate_stg
                                                                                                                                                                                         END AS end_dt,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN (
                                                                                                                                                                                                        pc_effectivedatedfields.updatetime_stg > pc_policyperiod.updatetime_stg ) THEN pc_effectivedatedfields.updatetime_stg
                                                                                                                                                                                                    ELSE pc_policyperiod.updatetime_stg
                                                                                                                                                                                         END AS updatetime,
                                                                                                                                                                                         /*Added for EIM-18200 */
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_policyperiod.retired_stg=0
                                                                                                                                                                                                    AND        pc_group.retired_stg=0 THEN 0
                                                                                                                                                                                                    ELSE 1
                                                                                                                                                                                         END                              AS retired,
                                                                                                                                                                                         cast(''SRC_SYS4'' AS VARCHAR(50))  AS src_sys_cd,
                                                                                                                                                                                         cast(NULL AS       VARCHAR(100)) AS trainingclasstypecode_stg,
                                                                                                                                                                                         cast(NULL AS       VARCHAR(100)) AS uslicensevalid_alfa_stg
                                                                                                                                                                              FROM       db_t_prod_stag.pc_effectivedatedfields
                                                                                                                                                                              inner join db_t_prod_stag.pc_group
                                                                                                                                                                              ON         pc_group.id_stg=pc_effectivedatedfields.servicecenter_alfa_stg
                                                                                                                                                                              inner join db_t_prod_stag.pctl_grouptype
                                                                                                                                                                              ON         pc_group.grouptype_stg= pctl_grouptype.id_stg
                                                                                                                                                                              inner join db_t_prod_stag.pc_policyperiod
                                                                                                                                                                              ON         pc_policyperiod.id_stg=pc_effectivedatedfields.branchid_stg
                                                                                                                                                                              inner join db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                                                              ON         pctl_policyperiodstatus.id_stg=pc_policyperiod.status_stg
                                                                                                                                                                              inner join db_t_prod_stag.pc_job
                                                                                                                                                                              ON         pc_job.id_stg=pc_policyperiod.jobid_stg
                                                                                                                                                                              inner join db_t_prod_stag.pctl_job
                                                                                                                                                                              ON         pctl_job.id_stg=pc_job.subtype_stg
                                                                                                                                                                              WHERE      pctl_policyperiodstatus.typecode_stg=''Bound''
                                                                                                                                                                              AND        pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                                                                                                              AND        pctl_grouptype.typecode_stg = ''servicecenter_alfa''
                                                                                                                                                                              AND        ((
                                                                                                                                                                                                        pc_effectivedatedfields.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                    AND        pc_effectivedatedfields.updatetime_stg <= ( :end_dttm))
                                                                                                                                                                                         OR         (
                                                                                                                                                                                                        pc_policyperiod.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                    AND        pc_policyperiod.updatetime_stg <= ( :end_dttm)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /*************policycontact******************/
                                                                                                                                                                              SELECT     pc_policyperiod.publicid_stg     AS publicid,
                                                                                                                                                                                         pc_policyperiod.policynumber_stg AS policynumber,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN (
                                                                                                                                                                                                        mortgageelienholdernumber_alfa_stg IS NOT NULL
                                                                                                                                                                                                        AND        pc_contact.addressbookuid_stg NOT LIKE ''MORT%''
                                                                                                                                                                                                        AND        pc_contact.addressbookuid_stg NOT LIKE ''%IRS%'') THEN mortgageelienholdernumber_alfa_stg
                                                                                                                                                                                                    ELSE pc_contact.addressbookuid_stg
                                                                                                                                                                                         END AS naiccode,
                                                                                                                                                                                         pctl_contact.typecode_stg
                                                                                                                                                                                                    ||''-CM''                  AS party_type,
                                                                                                                                                                                         pctl_policycontactrole.typecode_stg AS prty_agmt_role_cd,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_policycontactrole.createtime_stg IS NULL THEN pc_policyperiod.periodstart_stg
                                                                                                                                                                                                    ELSE pc_policycontactrole.createtime_stg
                                                                                                                                                                                         END AS eff_dt,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_policycontactrole.expirationdate_stg IS NULL THEN pc_policyperiod.periodend_stg
                                                                                                                                                                                                    ELSE pc_policycontactrole.expirationdate_stg
                                                                                                                                                                                         END AS end_dt,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN (
                                                                                                                                                                                                        pc_policycontactrole.updatetime_stg > pc_policyperiod.updatetime_stg ) THEN pc_policycontactrole.updatetime_stg
                                                                                                                                                                                                    ELSE pc_policyperiod.updatetime_stg
                                                                                                                                                                                         END AS updatetime,
                                                                                                                                                                                         /*Added for EIM-18200 */
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_policyperiod.retired_stg=0
                                                                                                                                                                                                    AND        pc_contact.retired_stg=0 THEN 0
                                                                                                                                                                                                    ELSE 1
                                                                                                                                                                                         END                                                                        AS retired,
                                                                                                                                                                                         cast(''SRC_SYS4'' AS VARCHAR(50))                                            AS src_sys_cd,
                                                                                                                                                                                         pct.typecode_stg                                                           AS trainingclasstypecode_stg,
                                                                                                                                                                                         cast(pc_policycontactrole.uslicensevalid_alfainternal_stg AS VARCHAR(100)) AS uslicensevalid_alfa_stg
                                                                                                                                                                                         /* Added for EIM-21058*/
                                                                                                                                                                              FROM       db_t_prod_stag.pc_policycontactrole
                                                                                                                                                                              inner join db_t_prod_stag.pc_contact
                                                                                                                                                                              ON         pc_contact.id_stg=pc_policycontactrole.contactdenorm_stg
                                                                                                                                                                              inner join db_t_prod_stag.pctl_policycontactrole
                                                                                                                                                                              ON         pctl_policycontactrole.id_stg=pc_policycontactrole.subtype_stg
                                                                                                                                                                              inner join db_t_prod_stag.pctl_contact
                                                                                                                                                                              ON         pctl_contact.id_stg=pc_contact.subtype_stg
                                                                                                                                                                              inner join db_t_prod_stag.pc_policyperiod
                                                                                                                                                                              ON         pc_policyperiod.id_stg=pc_policycontactrole.branchid_stg
                                                                                                                                                                              inner join db_t_prod_stag.pc_job
                                                                                                                                                                              ON         pc_job.id_stg=pc_policyperiod.jobid_stg
                                                                                                                                                                              inner join db_t_prod_stag.pctl_job
                                                                                                                                                                              ON         pctl_job.id_stg=pc_job.subtype_stg
                                                                                                                                                                              inner join db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                                                              ON         pctl_policyperiodstatus.id_stg=pc_policyperiod.status_stg
                                                                                                                                                                              left join  db_t_prod_stag.pctl_trainingclasstype pct
                                                                                                                                                                              ON         pct.id_stg = pc_policycontactrole.trainingclasstypeinternal_stg
                                                                                                                                                                                         /* Including Trianing Classtype for EIM-15512 */
                                                                                                                                                                              WHERE      pctl_policyperiodstatus.typecode_stg=''Bound''
                                                                                                                                                                              AND        pctl_policycontactrole.typecode_stg<>''PolicyPriNamedInsured''
                                                                                                                                                                                         /* Excluding PrimaryNamedInsured(commented)*/
                                                                                                                                                                              AND        (
                                                                                                                                                                                                    pc_policycontactrole.expirationdate_stg IS NULL
                                                                                                                                                                                         OR         pc_policycontactrole.expirationdate_stg > pc_policyperiod.editeffectivedate_stg)
                                                                                                                                                                                         /* Added as part of EIM-16906*/
                                                                                                                                                                              AND        (
                                                                                                                                                                                                    pc_policycontactrole.effectivedate_stg IS NULL
                                                                                                                                                                                         OR         pc_policycontactrole.effectivedate_stg <= pc_policyperiod.editeffectivedate_stg)
                                                                                                                                                                                         /* Added as part of EIM-16906*/
                                                                                                                                                                              AND        ((
                                                                                                                                                                                                        pc_policycontactrole.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                    AND        pc_policycontactrole.updatetime_stg <= ( :end_dttm))
                                                                                                                                                                                         OR        (
                                                                                                                                                                                                        pc_policyperiod.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                    AND        pc_policyperiod.updatetime_stg <= ( :end_dttm)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /* ********************* redundant inner join has been removed ******************************** */
                                                                                                                                                                              SELECT          pc_policyperiod.publicid_stg     AS publicid,
                                                                                                                                                                                              pc_policyperiod.policynumber_stg AS policynumber,
                                                                                                                                                                                              pc_contact.addressbookuid_stg    AS naiccode,
                                                                                                                                                                                              pctl_contact.typecode_stg
                                                                                                                                                                                                        ||''-CM''             AS party_type,
                                                                                                                                                                                              pctl_policycontactrole.typecode_stg AS prty_agmt_role_cd,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN pc_policycontactrole.createtime_stg IS NULL THEN pc_policyperiod.periodstart_stg
                                                                                                                                                                                                        ELSE pc_policycontactrole.createtime_stg
                                                                                                                                                                                              END AS eff_dt,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN pc_policycontactrole.expirationdate_stg IS NULL THEN pc_policyperiod.periodend_stg
                                                                                                                                                                                                        ELSE pc_policycontactrole.expirationdate_stg
                                                                                                                                                                                              END AS end_dt,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN (
                                                                                                                                                                                                        coalesce(pc_policyperiod.updatetime_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp))>coalesce(pc_contact.updatetime_stg, cast(''1900-01-01 00:00:00.000000'' AS timestamp))) THEN pc_policyperiod.updatetime_stg
                                                                                                                                                                                                        ELSE coalesce(pc_contact.updatetime_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp))
                                                                                                                                                                                              END AS updatetime,
                                                                                                                                                                                              /*Added for EIM-36542 */
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN pc_policyperiod.retired_stg=0
                                                                                                                                                                                                        AND             pc_contact.retired_stg=0 THEN 0
                                                                                                                                                                                                        ELSE 1
                                                                                                                                                                                              END                              AS retired,
                                                                                                                                                                                              cast(''SRC_SYS4'' AS                                           VARCHAR(50))  AS src_sys_cd,
                                                                                                                                                                                              cast(NULL AS                                                 VARCHAR(100)) AS trainingclasstypecode_stg,
                                                                                                                                                                                              cast(pc_policycontactrole.uslicensevalid_alfainternal_stg AS VARCHAR(100)) AS uslicensevalid_alfa_stg
                                                                                                                                                                                              /* Added for EIM-21058*/
                                                                                                                                                                              FROM            db_t_prod_stag.pc_policyperiod
                                                                                                                                                                              inner join      db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                                                              ON              pctl_policyperiodstatus.id_stg=pc_policyperiod.status_stg
                                                                                                                                                                              inner join      db_t_prod_stag.pc_contact
                                                                                                                                                                              ON              pc_policyperiod.pnicontactdenorm_stg=pc_contact.id_stg
                                                                                                                                                                              inner join      db_t_prod_stag.pctl_contact
                                                                                                                                                                              ON              pctl_contact.id_stg = pc_contact.subtype_stg
                                                                                                                                                                              join            db_t_prod_stag.pc_policycontactrole
                                                                                                                                                                              ON              pc_policycontactrole.branchid_stg = pc_policyperiod.id_stg
                                                                                                                                                                              join            db_t_prod_stag.pctl_policycontactrole
                                                                                                                                                                              ON              pctl_policycontactrole.id_stg=pc_policycontactrole.subtype_stg
                                                                                                                                                                              inner join      db_t_prod_stag.pc_job
                                                                                                                                                                              ON              pc_job.id_stg=pc_policyperiod.jobid_stg
                                                                                                                                                                              inner join      db_t_prod_stag.pctl_job
                                                                                                                                                                              ON              pctl_job.id_stg=pc_job.subtype_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_user
                                                                                                                                                                              ON              pc_user.contactid_stg = pc_contact.id_stg
                                                                                                                                                                              WHERE           pctl_policyperiodstatus.typecode_stg=''Bound''
                                                                                                                                                                                              /* PrimaryNamedInsured*/
                                                                                                                                                                              AND             pctl_policycontactrole.typecode_stg=''PolicyPriNamedInsured''
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        pc_policyperiod.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                        AND             pc_policyperiod.updatetime_stg <= ( :end_dttm))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        pc_contact.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                        AND             pc_contact.updatetime_stg <= ( :end_dttm))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        pc_user.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                        AND             pc_user.updatetime_stg <= ( :end_dttm)) ) qualify row_number() over ( PARTITION BY publicid ORDER BY updatetime DESC, eff_dt DESC,end_dt DESC)=1
                                                                                                                                                                              UNION
                                                                                                                                                                              /**********Account Contact for Account Agreement****************/
                                                                                                                                                                              /* Sandy Houston - 6/8/2016 - defect 15391 - this section of the query was commented out. I am uncommenting so account contact will be passed*/
                                                                                                                                                                              SELECT     cast(NULL AS VARCHAR(50))     AS publicid,
                                                                                                                                                                                         pc_account.accountnumber_stg  AS policynumber,
                                                                                                                                                                                         pc_contact.addressbookuid_stg AS naiccode,
                                                                                                                                                                                         pctl_contact.typecode_stg
                                                                                                                                                                                                    ||''-CM''                                 AS party_type,
                                                                                                                                                                                         pctl_accountcontactrole.typecode_stg               AS prty_agmt_role_cd,
                                                                                                                                                                                         pc_accountcontactrole.createtime_stg               AS eff_dt,
                                                                                                                                                                                         cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt,
                                                                                                                                                                                         pc_accountcontactrole.updatetime_stg               AS updatetime,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_account.retired_stg=0
                                                                                                                                                                                                    AND        pc_contact.retired_stg=0 THEN 0
                                                                                                                                                                                                    ELSE 1
                                                                                                                                                                                         END                              AS retired,
                                                                                                                                                                                         cast(''SRC_SYS4'' AS                                    VARCHAR(50))  AS src_sys_cd,
                                                                                                                                                                                         cast(NULL AS                                          VARCHAR(100)) AS trainingclasstypecode_stg,
                                                                                                                                                                                         cast(pc_accountcontactrole.uslicensevalid_alfa_stg AS VARCHAR(100)) AS uslicensevalid_alfa_stg
                                                                                                                                                                              FROM       db_t_prod_stag.pc_accountcontactrole
                                                                                                                                                                              inner join db_t_prod_stag.pc_accountcontact
                                                                                                                                                                              ON         pc_accountcontact.id_stg=pc_accountcontactrole.accountcontact_stg
                                                                                                                                                                              inner join db_t_prod_stag.pctl_accountcontactrole
                                                                                                                                                                              ON         pctl_accountcontactrole.id_stg=pc_accountcontactrole.subtype_stg
                                                                                                                                                                              inner join db_t_prod_stag.pc_contact
                                                                                                                                                                              ON         pc_contact.id_stg=pc_accountcontact.contact_stg
                                                                                                                                                                              inner join db_t_prod_stag.pc_account
                                                                                                                                                                              ON         pc_account.id_stg=pc_accountcontact.account_stg
                                                                                                                                                                              inner join db_t_prod_stag.pctl_contact
                                                                                                                                                                              ON         pctl_contact.id_stg=pc_contact.subtype_stg
                                                                                                                                                                              WHERE      pc_accountcontactrole.updatetime_stg > ( :start_dttm)
                                                                                                                                                                              AND        pc_accountcontactrole.updatetime_stg <= ( :end_dttm)
                                                                                                                                                                              UNION
                                                                                                                                                                              /***********************Capture Users such as underwriter,producer*************************/
                                                                                                                                                                              SELECT     pc_policyperiod.publicid_stg     AS publicid,
                                                                                                                                                                                         pc_policyperiod.policynumber_stg AS policynumber,
                                                                                                                                                                                         pc_contact.publicid_stg          AS naiccode,
                                                                                                                                                                                         ''UserContact''
                                                                                                                                                                                                    ||''-CC''                    AS party_type,
                                                                                                                                                                                         pctl_userrole.typecode_stg            AS prty_agmt_role_cd,
                                                                                                                                                                                         pc_policyperiod.editeffectivedate_stg AS eff_dt,
                                                                                                                                                                                         pc_policyuserroleassign.closedate_stg AS end_dt,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN (
                                                                                                                                                                                                        pc_policyuserroleassign.updatetime_stg > pc_policyperiod.updatetime_stg ) THEN pc_policyuserroleassign.updatetime_stg
                                                                                                                                                                                                    ELSE pc_policyperiod.updatetime_stg
                                                                                                                                                                                         END AS updatetime,
                                                                                                                                                                                         /*Added for EIM-18200 */
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_policyperiod.retired_stg=0
                                                                                                                                                                                                    AND        pc_contact.retired_stg=0
                                                                                                                                                                                                    AND        pc_user.retired_stg=0
                                                                                                                                                                                                    AND        pc_policyuserroleassign.retired_stg=0 THEN 0
                                                                                                                                                                                                    ELSE 1
                                                                                                                                                                                         END                              AS retired,
                                                                                                                                                                                         cast(''SRC_SYS4'' AS VARCHAR(50))  AS src_sys_cd,
                                                                                                                                                                                         cast(NULL AS       VARCHAR(100)) AS trainingclasstypecode_stg,
                                                                                                                                                                                         cast(NULL AS       VARCHAR(100)) AS uslicensevalid_alfa_stg
                                                                                                                                                                              FROM       db_t_prod_stag.pc_policyperiod
                                                                                                                                                                              join       db_t_prod_stag.pc_policy
                                                                                                                                                                              ON         pc_policyperiod.policyid_stg = pc_policy.id_stg
                                                                                                                                                                              join
                                                                                                                                                                                         (
                                                                                                                                                                                                SELECT policyid_stg,
                                                                                                                                                                                                       assigneduserid_stg,
                                                                                                                                                                                                       retired_stg ,
                                                                                                                                                                                                       role_stg,
                                                                                                                                                                                                       closedate_stg,
                                                                                                                                                                                                       updatetime_stg
                                                                                                                                                                                                FROM   db_t_prod_stag.pc_policyuserroleassign
                                                                                                                                                                                                WHERE  retired_stg=0) pc_policyuserroleassign
                                                                                                                                                                                         /*Added for EIM-18200 */
                                                                                                                                                                              ON         pc_policy.id_stg = pc_policyuserroleassign.policyid_stg
                                                                                                                                                                              join       db_t_prod_stag.pctl_userrole
                                                                                                                                                                              ON         pc_policyuserroleassign.role_stg = pctl_userrole.id_stg
                                                                                                                                                                              join       db_t_prod_stag.pc_user
                                                                                                                                                                              ON         pc_policyuserroleassign.assigneduserid_stg = pc_user.id_stg
                                                                                                                                                                              join       db_t_prod_stag.pc_contact
                                                                                                                                                                              ON         pc_contact.id_stg = pc_user.contactid_stg
                                                                                                                                                                              inner join db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                                                              ON         pctl_policyperiodstatus.id_stg=pc_policyperiod.status_stg
                                                                                                                                                                              WHERE      pctl_policyperiodstatus.typecode_stg=''Bound''
                                                                                                                                                                              AND        pctl_userrole.typecode_stg<>''Producer''
                                                                                                                                                                              AND        ((
                                                                                                                                                                                                        pc_policyuserroleassign.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                    AND        pc_policyuserroleassign.updatetime_stg <= ( :end_dttm))
                                                                                                                                                                                         OR        (
                                                                                                                                                                                                        pc_policyperiod.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                    AND        pc_policyperiod.updatetime_stg <= ( :end_dttm)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /* Agreement to UW District */
                                                                                                                                                                              /* ********************* redundant inner join has been removed *********************************/
                                                                                                                                                                              SELECT     pc_policyperiod.publicid_stg AS publicid,
                                                                                                                                                                                         policynumber_stg             AS policynumber,
                                                                                                                                                                                         uwdist.name_stg              AS naiccode,
                                                                                                                                                                                         uwdisttype.typecode_stg      AS party_type,
                                                                                                                                                                                         uwdisttype.typecode_stg      AS prty_agmt_role_cd,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_policyperiod.editeffectivedate_stg IS NULL THEN pc_policyperiod.periodstart_stg
                                                                                                                                                                                                    ELSE pc_policyperiod.editeffectivedate_stg
                                                                                                                                                                                         END AS eff_dt,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_effectivedatedfields.expirationdate_stg IS NULL THEN pc_policyperiod.periodend_stg
                                                                                                                                                                                                    ELSE pc_effectivedatedfields.expirationdate_stg
                                                                                                                                                                                         END AS end_dt,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN (
                                                                                                                                                                                                        pc_effectivedatedfields.updatetime_stg > pc_policyperiod.updatetime_stg ) THEN pc_effectivedatedfields.updatetime_stg
                                                                                                                                                                                                    ELSE pc_policyperiod.updatetime_stg
                                                                                                                                                                                         END AS updatetime,
                                                                                                                                                                                         /*Added for EIM-18200 */
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_policyperiod.retired_stg=0
                                                                                                                                                                                                    AND        pc_group.retired_stg=0 THEN 0
                                                                                                                                                                                                    ELSE 1
                                                                                                                                                                                         END                              AS retired,
                                                                                                                                                                                         cast(''SRC_SYS4'' AS VARCHAR(50))  AS src_sys_cd,
                                                                                                                                                                                         cast(NULL AS       VARCHAR(100)) AS trainingclasstypecode_stg,
                                                                                                                                                                                         cast(NULL AS       VARCHAR(100)) AS uslicensevalid_alfa_stg
                                                                                                                                                                              FROM       db_t_prod_stag.pc_effectivedatedfields
                                                                                                                                                                              inner join db_t_prod_stag.pc_group
                                                                                                                                                                              ON         pc_group.id_stg=pc_effectivedatedfields.servicecenter_alfa_stg
                                                                                                                                                                              inner join db_t_prod_stag.pctl_grouptype
                                                                                                                                                                              ON         pc_group.grouptype_stg= pctl_grouptype.id_stg
                                                                                                                                                                              left join  db_t_prod_stag.pcx_uwparentgroup_alfa
                                                                                                                                                                              ON         pcx_uwparentgroup_alfa.ownerid_stg=pc_group.id_stg
                                                                                                                                                                              left join  db_t_prod_stag.pc_group AS uwdist
                                                                                                                                                                              ON         uwdist.id_stg=pcx_uwparentgroup_alfa.foreignentityid_stg
                                                                                                                                                                              left join  db_t_prod_stag.pctl_grouptype AS uwdisttype
                                                                                                                                                                              ON         uwdisttype.id_stg=uwdist.grouptype_stg
                                                                                                                                                                              inner join db_t_prod_stag.pc_policyperiod
                                                                                                                                                                              ON         pc_policyperiod.id_stg=pc_effectivedatedfields.branchid_stg
                                                                                                                                                                              inner join db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                                                              ON         pctl_policyperiodstatus.id_stg=pc_policyperiod.status_stg
                                                                                                                                                                                         /* inner join (select SVC.ID_stg as ServiceCenter_GroupID, SVC.Name_stg as ServiceCenter_Name, UWDist.Name_Stg as UWDistrictName,UWDistType.TYPECODE_stg as Party_type from DB_T_PROD_STAG.pc_group as SVC join DB_T_PROD_STAG.pctl_grouptype on pctl_grouptype.ID_stg=SVC.GroupType_Stg left join DB_T_PROD_STAG.pcx_uwparentgroup_alfa on pcx_uwparentgroup_alfa.OwnerID_stg=SVC.ID_stg left join DB_T_PROD_STAG.pc_group as UWDist on UWDist.ID_stg=pcx_uwparentgroup_alfa.ForeignEntityID_stg left join DB_T_PROD_STAG.pctl_grouptype as UWDistType on UWDistType.ID_stg=UWDist.GroupType_Stg where pctl_grouptype.TYPECODE_stg=''servicecenter_alfa'' ) UWD on UWD.ServiceCenter_Name=pc_group.name_stg */
                                                                                                                                                                              WHERE      pctl_policyperiodstatus.typecode_stg=''Bound''
                                                                                                                                                                              AND        pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                                                                                                              AND        pctl_grouptype.typecode_stg = ''servicecenter_alfa''
                                                                                                                                                                              AND        ( (
                                                                                                                                                                                                        pc_effectivedatedfields.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                    AND        pc_effectivedatedfields.updatetime_stg <= ( :end_dttm))
                                                                                                                                                                                         OR         (
                                                                                                                                                                                                        pc_policyperiod.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                    AND        pc_policyperiod.updatetime_stg <= ( :end_dttm)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /****** Prior Insurance carrier for all bound DB_T_CORE_DM_PROD.policy periods******/
                                                                                                                                                                              SELECT     pc_policyperiod.publicid_stg                              AS publicid,
                                                                                                                                                                                         pc_policyperiod.policynumber_stg                          AS policynumber,
                                                                                                                                                                                         coalesce(other_lob.typecode_stg , farm_lob.typecode_stg ) AS naiccode,
                                                                                                                                                                                         cast(''BUSN_CTGY6'' AS      VARCHAR(100))                        AS party_type,
                                                                                                                                                                                         cast(''PRTY_AGMT_ROLE6'' AS VARCHAR(100))                        AS prty_agmt_role_cd,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_effectivedatedfields.effectivedate_stg IS NULL THEN pc_policyperiod.periodstart_stg
                                                                                                                                                                                                    ELSE pc_effectivedatedfields.effectivedate_stg
                                                                                                                                                                                         END                           AS eff_dt,
                                                                                                                                                                                         pc_policyperiod.periodend_stg AS end_dt,
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN (
                                                                                                                                                                                                        pc_effectivedatedfields.updatetime_stg > pc_policyperiod.updatetime_stg ) THEN pc_effectivedatedfields.updatetime_stg
                                                                                                                                                                                                    ELSE pc_policyperiod.updatetime_stg
                                                                                                                                                                                         END AS updatetime,
                                                                                                                                                                                         /*Added for EIM-18200 */
                                                                                                                                                                                         CASE
                                                                                                                                                                                                    WHEN pc_policyperiod.retired_stg=0
                                                                                                                                                                                                    AND        (
                                                                                                                                                                                                        other_lob.retired_stg=0
                                                                                                                                                                                                        OR         farm_lob.retired_stg=0) THEN 0
                                                                                                                                                                                                    ELSE 1
                                                                                                                                                                                         END                              AS retired,
                                                                                                                                                                                         cast(''SRC_SYS4'' AS VARCHAR(50))  AS src_sys_cd,
                                                                                                                                                                                         cast(NULL AS       VARCHAR(100)) AS trainingclasstypecode_stg,
                                                                                                                                                                                         cast(NULL AS       VARCHAR(100)) AS uslicensevalid_alfa_stg
                                                                                                                                                                              FROM       db_t_prod_stag.pc_policyperiod
                                                                                                                                                                              inner join db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                                                              ON         pctl_policyperiodstatus.id_stg=pc_policyperiod.status_stg
                                                                                                                                                                              inner join db_t_prod_stag.pc_effectivedatedfields
                                                                                                                                                                              ON         pc_policyperiod.id_stg=pc_effectivedatedfields.branchid_stg
                                                                                                                                                                              join       db_t_prod_stag.pc_policyline
                                                                                                                                                                              ON         pc_policyperiod.id_stg=pc_policyline.branchid_stg
                                                                                                                                                                                         /*EIM_48787*/
                                                                                                                                                                              left join  db_t_prod_stag.pctl_priorcarrier_alfa other_lob
                                                                                                                                                                              ON         other_lob.id_stg=pc_effectivedatedfields.priorcarrier_alfa_stg
                                                                                                                                                                              left join  db_t_prod_stag.pctl_priorcarrier_alfa farm_lob
                                                                                                                                                                              ON         farm_lob.id_stg=pc_policyline.foppriorcarrier_stg
                                                                                                                                                                              WHERE      pctl_policyperiodstatus.typecode_stg=''Bound''
                                                                                                                                                                              AND        pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                                                                                                              AND        ( (
                                                                                                                                                                                                        pc_policyperiod.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                    AND        pc_policyperiod.updatetime_stg <= ( :end_dttm))
                                                                                                                                                                                         OR         (
                                                                                                                                                                                                        pc_effectivedatedfields.updatetime_stg > ( :start_dttm)
                                                                                                                                                                                                    AND        pc_effectivedatedfields.updatetime_stg <= ( :end_dttm)))
                                                                                                                                                                              AND        (
                                                                                                                                                                                                    other_lob.typecode_stg IS NOT NULL
                                                                                                                                                                                         OR         farm_lob.typecode_stg IS NOT NULL ))pc_prty_agmt
                                                                                                                                              WHERE           pc_prty_agmt.naiccode IS NOT NULL
                                                                                                                                              UNION
                                                                                                                                                    /* ************** Removed redundant subquery from this union block ********************/
                                                                                                                                                    
                                                                                                                                              SELECT DISTINCT cast(pc_policyperiod.publicid_stg AS VARCHAR(50))  AS publicid,
                                                                                                                                                              cast ('''' AS                          VARCHAR(50))  AS policynumber,
                                                                                                                                                              pc_contact.publicid_stg                            AS naiccode,
                                                                                                                                                              cast (''UserContact'' AS VARCHAR(50))                AS party_type,
                                                                                                                                                              cast(''agent'' AS        VARCHAR(50))                AS prty_agmt_role_cd,
                                                                                                                                                              pc_policyperiod.editeffectivedate_stg              AS eff_dt,
                                                                                                                                                              cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS end_dt,
                                                                                                                                                              cast(''SRC_SYS4'' AS VARCHAR(50))                    AS pc_src_cd,
                                                                                                                                                              CASE
                                                                                                                                                                              WHEN pc_producercode.retired_stg=0
                                                                                                                                                                              AND             pc_contact.retired_stg=0
                                                                                                                                                                              AND             pc_policyperiod.retired_stg=0 THEN 0
                                                                                                                                                                              ELSE 1
                                                                                                                                                              END                            AS retired,
                                                                                                                                                              pc_policyperiod.updatetime_stg AS trans_strt_dt,
                                                                                                                                                              cast('''' AS   VARCHAR(50))        AS trainingclasstype_typecode,
                                                                                                                                                              cast(NULL AS VARCHAR(50))        AS uslicensevalid_alfa
                                                                                                                                              FROM            db_t_prod_stag.pc_producercode
                                                                                                                                              left outer join db_t_prod_stag.pc_effectivedatedfields
                                                                                                                                              ON              pc_producercode.id_stg = pc_effectivedatedfields.producercodeid_stg
                                                                                                                                              left outer join db_t_prod_stag.pc_policyperiod
                                                                                                                                              ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                                                                              left outer join db_t_prod_stag.pc_userproducercode
                                                                                                                                              ON              pc_producercode.id_stg = pc_userproducercode.producercodeid_stg
                                                                                                                                              left outer join db_t_prod_stag.pc_user
                                                                                                                                              ON              pc_userproducercode.userid_stg = pc_user.id_stg
                                                                                                                                              left outer join db_t_prod_stag.pc_userrole
                                                                                                                                              ON              pc_user.id_stg = pc_userrole.userid_stg
                                                                                                                                              left outer join db_t_prod_stag.pc_role userrole
                                                                                                                                              ON              pc_userrole.roleid_stg = userrole.id_stg
                                                                                                                                              left outer join db_t_prod_stag.pc_contact
                                                                                                                                              ON              pc_user.contactid_stg = pc_contact.id_stg
                                                                                                                                              left outer join db_t_prod_stag.pctl_contact
                                                                                                                                              ON              pc_contact.subtype_stg = pctl_contact.id_stg
                                                                                                                                              left outer join db_t_prod_stag.pc_producercoderole
                                                                                                                                              ON              pc_producercode.id_stg = pc_producercoderole.producercodeid_stg
                                                                                                                                              left outer join db_t_prod_stag.pc_role
                                                                                                                                              ON              pc_producercoderole.roleid_stg = pc_role.id_stg
                                                                                                                                              left join       db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                              ON              pctl_policyperiodstatus.id_stg=pc_policyperiod.status_stg
                                                                                                                                                              /* left outer join DB_T_PROD_STAG.pc_role ProducerCodeRole on pc_producercoderole.RoleID_stg = ProducerCodeRole.ID_Stg*/
                                                                                                                                                              
                                                                                                                                              WHERE
                                                                                                                                                              /* pctl_policyperiodstatus.TYPECODE_stg=''Bound'' and*/
                                                                                                                                                              pctl_contact.name_stg=''User Contact'' 
                                                                                                                                              AND             pc_role.name_stg=''Agent''
                                                                                                                                              AND             userrole.name_stg IN (''CSR'',
                                                                                                                                                                                    ''Agent'')
                                                                                                                                              AND             pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                                                                              AND             pc_policyperiod.publicid_stg IS NOT NULL
                                                                                                                                              AND             pc_contact.publicid_stg IS NOT NULL
                                                                                                                                              AND             pctl_policyperiodstatus.typecode_stg=''Bound''
                                                                                                                                              AND             ((
                                                                                                                                                                                              pc_producercode.updatetime_stg > ( :start_dttm)
                                                                                                                                                                              AND             pc_producercode.updatetime_stg <= ( :end_dttm))
                                                                                                                                                              OR              (
                                                                                                                                                                                              pc_policyperiod.updatetime_stg > ( :start_dttm)
                                                                                                                                                                              AND             pc_policyperiod.updatetime_stg <= ( :end_dttm))) )x
                                                                                                                              /* ************************** Removed redundant subquery *************************************/
                                                                                                              left join       db_t_prod_core.teradata_etl_ref_xlat AS xlat_intrnal_org_sbtype
                                                                                                              ON              xlat_intrnal_org_sbtype.tgt_idntftn_nm= ''INTRNL_ORG_SBTYPE''
                                                                                                              AND             xlat_intrnal_org_sbtype.src_idntftn_val = lkp_prty_type_id
                                                                                                                              /* ************************** Removed redundant subquery *************************************/
                                                                                                              left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_intrnal_org_type
                                                                                                              ON              xlat_intrnal_org_type.tgt_idntftn_nm= ''INTRNL_ORG_TYPE''
                                                                                                              AND             xlat_intrnal_org_type.src_idntftn_nm= ''derived''
                                                                                                              AND             xlat_intrnal_org_type.src_idntftn_sys=''DS''
                                                                                                              AND             xlat_intrnal_org_type.expn_dt=''9999-12-31''
                                                                                                              AND             xlat_intrnal_org_type.src_idntftn_val = ''INTRNL_ORG_TYPE15''
                                                                                                              left outer join
                                                                                                                              (
                                                                                                                                              SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                                                              teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                                                              FROM            db_t_prod_core.teradata_etl_ref_xlat
                                                                                                                                              WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRTY_AGMT_ROLE''
                                                                                                                                              AND             teradata_etl_ref_xlat.src_idntftn_nm IN (''pctl_grouptype.TYPECODE'',
                                                                                                                                                                                                       ''pctl_accountcontactrole.typecode'',
                                                                                                                                                                                                       ''pctl_userrole.typecode'',
                                                                                                                                                                                                       ''cctl_contactrole.typecode'',
                                                                                                                                                                                                       ''pctl_policycontactrole.TYPECODE'',
                                                                                                                                                                                                       ''pctl_additionalinteresttype.typecode'',
                                                                                                                                                                                                       ''bctl_accountrole.typecode'',
                                                                                                                                                                                                       ''derived'')
                                                                                                                                              AND             teradata_etl_ref_xlat.src_idntftn_sys IN (''GW'',
                                                                                                                                                                                                        ''DS'')
                                                                                                                                              AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' qualify row_number() over( PARTITION BY src_idntftn_val ORDER BY tgt_idntftn_val DESC)=1 ) xlat_teradata_etl_ref
                                                                                                              ON              xlat_teradata_etl_ref.src_idntftn_val = x.prty_agmt_role_cd COLLATE ''en-ci''
                                                                                                                              /* ************************** Removed redundant subquery *************************************/
                                                                                                              left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_teradata_src_cd
                                                                                                              ON              xlat_teradata_src_cd.src_idntftn_val = x.src_sys_cd
                                                                                                              AND             xlat_teradata_src_cd.tgt_idntftn_nm= ''SRC_SYS''
                                                                                                              AND             xlat_teradata_src_cd.src_idntftn_nm= ''derived''
                                                                                                              AND             xlat_teradata_src_cd.src_idntftn_sys=''DS''
                                                                                                              AND             xlat_teradata_src_cd.expn_dt=''9999-12-31''
                                                                                                                              /* ************************** Removed redundant subquery *************************************/
                                                                                                              left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_teradata_busn_ctgy
                                                                                                              ON              xlat_teradata_busn_ctgy.src_idntftn_val = lkp_prty_type_id
                                                                                                              AND             xlat_teradata_busn_ctgy.tgt_idntftn_nm IN (''BUSN_CTGY'',
                                                                                                                                                                         ''ORG_TYPE'',
                                                                                                                                                                         ''PRTY_TYPE'')
                                                                                                              AND             xlat_teradata_busn_ctgy.src_idntftn_nm IN (''derived'',
                                                                                                                                                                         ''cctl_contact.typecode'',
                                                                                                                                                                         ''cctl_contact.name'')
                                                                                                              AND             xlat_teradata_busn_ctgy.src_idntftn_sys IN (''DS'',
                                                                                                                                                                          ''GW'')
                                                                                                              AND             xlat_teradata_busn_ctgy.expn_dt=''9999-12-31''
                                                                                                                              /* ************************** Removed redundant subquery ************************************ */
                                                                                                              left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_teradata_train_class_type
                                                                                                              ON              xlat_teradata_train_class_type.src_idntftn_val = x.trainingclasstype_typecode
                                                                                                              AND             xlat_teradata_train_class_type.tgt_idntftn_nm= ''TRNG_TYPE''
                                                                                                              AND             xlat_teradata_train_class_type.src_idntftn_nm= ''pctl_trainingclasstype.typecode''
                                                                                                              AND             xlat_teradata_train_class_type.src_idntftn_sys=''GW''
                                                                                                              AND             xlat_teradata_train_class_type.expn_dt=''9999-12-31''
                                                                                                              GROUP BY        publicid,
                                                                                                                              policynumber,
                                                                                                                              naiccode,
                                                                                                                              party_type,
                                                                                                                              x.prty_agmt_role_cd,
                                                                                                                              x.src_sys_cd,
                                                                                                                              retired,
                                                                                                                              trainingclasstype_typecode,
                                                                                                                              uslicensevalid_alfa,
                                                                                                                              lkp_prty_type_id,
                                                                                                                              src_prty_agmt_role_cd,
                                                                                                                              busn_ctgy_cd,
                                                                                                                              training_class_type_cd,
                                                                                                                              prty_agmt_type_cd,
                                                                                                                              intrnl_org_sbtype,
                                                                                                                              intrnl_org_type )src_inr
                                                                                              /* ************************** Removed redundant subquery and added HWM value filter *************************************/
                                                                              left outer join
                                                                                              (
                                                                                                              SELECT DISTINCT indiv.indiv_prty_id AS indiv_prty_id,
                                                                                                                              indiv.nk_publc_id   AS nk_publc_id
                                                                                                              FROM            db_t_prod_core.indiv 
                                                                                                              WHERE           indiv.nk_publc_id IS NOT NULL ) indiv_usrcnt
                                                                              ON              indiv_usrcnt.nk_publc_id = src_inr.naiccode
                                                                              left outer join
                                                                                              (
                                                                                                       SELECT   intrnl_org.intrnl_org_prty_id   AS intrnl_org_prty_id,
                                                                                                                intrnl_org.intrnl_org_type_cd   AS intrnl_org_type_cd,
                                                                                                                intrnl_org.intrnl_org_sbtype_cd AS intrnl_org_sbtype_cd,
                                                                                                                intrnl_org.intrnl_org_num       AS intrnl_org_num
                                                                                                       FROM     db_t_prod_core.intrnl_org  qualify row_number () over ( PARTITION BY intrnl_org_num,intrnl_org_type_cd,intrnl_org_sbtype_cd ORDER BY edw_end_dttm DESC)=1 )intrnl_org_prty
                                                                              ON              intrnl_org_prty.intrnl_org_type_cd=intrnl_org_type
                                                                              AND             intrnl_org_prty.intrnl_org_sbtype_cd=intrnl_org_sbtype
                                                                              AND             intrnl_org_prty.intrnl_org_num = src_inr.naiccode
                                                                                              /* ************************** Removed redundant subquery and added HWM value filter *************************************/
                                                                              left outer join db_t_prod_core.indiv indiv_cnt_mgr
                                                                              ON              indiv_cnt_mgr.nk_link_id = src_inr.naiccode
                                                                              AND             indiv_cnt_mgr.nk_publc_id IS NULL
                                                                              AND             cast(indiv_cnt_mgr.edw_end_dttm AS DATE)=''9999-12-31''
                                                                              left outer join
                                                                                              (
                                                                                                       SELECT   busn.busn_prty_id AS busn_prty_id,
                                                                                                                busn.busn_ctgy_cd AS busn_ctgy,
                                                                                                                busn.nk_busn_cd   AS nk_busn_cd
                                                                                                       FROM     db_t_prod_core.busn  qualify row_number () over ( PARTITION BY nk_busn_cd,busn_ctgy ORDER BY edw_end_dttm DESC,edw_strt_dttm DESC )=1 ) busn_prty
                                                                              ON              busn_ctgy = src_inr.busn_ctgy_cd
                                                                              AND             nk_busn_cd = src_inr.naiccode
                                                                              left outer join
                                                                                              (
                                                                                                              SELECT DISTINCT agmt.agmt_id       AS agmt_act_id,
                                                                                                                              agmt.host_agmt_num AS host_agmt_num,
                                                                                                                              agmt.nk_src_key    AS nk_src_key,
                                                                                                                              agmt.agmt_type_cd  AS agmt_type_cd
                                                                                                              FROM            db_t_prod_core.agmt
                                                                                                              WHERE           agmt.agmt_type_cd=''ACT'' qualify row_number() over( PARTITION BY agmt.nk_src_key,agmt.host_agmt_num ORDER BY agmt.edw_end_dttm DESC) = 1 ) agmt_act
                                                                              ON              agmt_act.host_agmt_num =trim(src_inr.policynumber)
                                                                              AND             agmt_act.agmt_type_cd=''ACT''
                                                                              left outer join
                                                                                              (
                                                                                                              SELECT DISTINCT agmt.agmt_id       AS agmt_pol_id,
                                                                                                                              agmt.host_agmt_num AS host_agmt_num,
                                                                                                                              agmt.nk_src_key    AS nk_src_key,
                                                                                                                              agmt.agmt_type_cd  AS agmt_type_cd
                                                                                                              FROM            db_t_prod_core.agmt
                                                                                                              WHERE           agmt.agmt_type_cd IN (''INV'',
                                                                                                                                                    ''PPV'') qualify row_number() over( PARTITION BY agmt.nk_src_key,agmt.host_agmt_num ORDER BY agmt.edw_end_dttm DESC) = 1 )agmt_inv_ppv
                                                                              ON              agmt_inv_ppv.nk_src_key = src_inr.publicid
                                                                              AND             agmt_inv_ppv.agmt_type_cd = prty_agmt_type_cd )calc )outr_src )
                           SELECT          src.src_agmt_id                       AS src_agmt_id,
                                           src.src_prty_agmt_role_cd             AS src_prty_agmt_role_cd,
                                           src.src_prty_id                       AS src_prty_id,
                                           src.src_eff_dt                        AS src_eff_dt ,
                                           src.src_end_dt                        AS src_end_dt ,
                                           src.src_retired                       AS src_retired,
                                           src.src_trans_strt_dttm               AS src_trans_strt_dttm,
                                           src.src_uslicensevalid_alfa           AS src_uslicensevalid_alfa,
                                           src.src_training_class_type_cd        AS src_training_class_type_cd,
                                           tgt_lkp_prty_agmt.agmt_id             AS tgt_agmt_id,
                                           tgt_lkp_prty_agmt.prty_agmt_role_cd   AS tgt_prty_agmt_role_cd ,
                                           tgt_lkp_prty_agmt.prty_id             AS tgt_prty_id,
                                           tgt_lkp_prty_agmt.prty_agmt_strt_dttm AS tgt_prty_agmt_strt_dttm,
                                           tgt_lkp_prty_agmt.prty_agmt_end_dttm  AS tgt_prty_agmt_end_dttm,
                                           tgt_lkp_prty_agmt.trng_type_cd        AS tgt_trng_type_cd,
                                           tgt_lkp_prty_agmt.vld_drvrs_lic_ind   AS tgt_vld_drvrs_lic_ind,
                                           tgt_lkp_prty_agmt.edw_strt_dttm       AS tgt_edw_strt_dttm,
                                           tgt_lkp_prty_agmt.edw_end_dttm        AS tgt_edw_end_dttm,
                                           /*Source data*/
                                           cast((trim(coalesce(src_training_class_type_cd ,0))
                                                           || trim(coalesce(src_uslicensevalid_alfa,0))
                                                           ||coalesce(cast(cast(src_eff_dt AS timestamp) AS VARCHAR(30)),0)
                                                           ||coalesce(cast(cast(src_end_dt AS timestamp) AS VARCHAR(30)),0)) AS VARCHAR(1100)) AS src_md5,
                                           /*target data*/
                                           cast((trim(coalesce(tgt_lkp_prty_agmt.trng_type_cd,0))
                                                           || trim(coalesce(tgt_lkp_prty_agmt.vld_drvrs_lic_ind,0))
                                                           ||coalesce(cast(to_char(tgt_lkp_prty_agmt.prty_agmt_strt_dttm , ''YYYY-MM-DDBHH:MI:SS'') AS VARCHAR(30)),0)
                                                           ||coalesce(cast(to_char(tgt_lkp_prty_agmt.prty_agmt_end_dttm , ''YYYY-MM-DDBHH:MI:SS'') AS  VARCHAR(30)),0)) AS VARCHAR(1100)) AS tgt_md5,
                                           /*Flag*/
                                           CASE
                                                           WHEN tgt_agmt_id IS NULL THEN ''I''
                                                           WHEN tgt_agmt_id IS NOT NULL
                                                           AND             tgt_prty_id IS NOT NULL
                                                           AND             src_md5<>tgt_md5 THEN ''U''
                                                           WHEN src_agmt_id IS NOT NULL
                                                           AND             src_md5=tgt_md5 THEN ''R''
                                           END AS ins_upd_flag
                           FROM            (
                                                  SELECT src_agmt_id,
                                                         src_prty_id,
                                                         src_prty_agmt_role_cd,
                                                         eff_dt                 AS src_eff_dt,
                                                         end_dt                 AS src_end_dt,
                                                         retired                AS src_retired,
                                                         updatetime             AS src_trans_strt_dttm,
                                                         uslicensevalid_alfa    AS src_uslicensevalid_alfa,
                                                         training_class_type_cd AS src_training_class_type_cd
                                                  FROM   prty_agmt_temp )src
                           left outer join
                                           (
                                                    SELECT   prty_agmt.prty_agmt_strt_dttm AS prty_agmt_strt_dttm,
                                                             prty_agmt.prty_agmt_end_dttm  AS prty_agmt_end_dttm,
                                                             prty_agmt.edw_strt_dttm       AS edw_strt_dttm,
                                                             prty_agmt.trans_strt_dttm     AS trans_strt_dttm,
                                                             prty_agmt.edw_end_dttm        AS edw_end_dttm,
                                                             prty_agmt.agmt_id             AS agmt_id,
                                                             prty_agmt.prty_agmt_role_cd   AS prty_agmt_role_cd,
                                                             prty_agmt.prty_id             AS prty_id,
                                                             prty_agmt.trng_type_cd        AS trng_type_cd,
                                                             prty_agmt.vld_drvrs_lic_ind   AS vld_drvrs_lic_ind
                                                    FROM     db_t_prod_core.prty_agmt
                                                    WHERE    (
                                                                      prty_id,agmt_id) IN
                                                                                           (
                                                                                           SELECT DISTINCT src_prty_id,
                                                                                                           src_agmt_id
                                                                                           FROM            prty_agmt_temp) qualify row_number() over( PARTITION BY agmt_id,prty_agmt_role_cd,prty_id ORDER BY edw_end_dttm DESC) = 1 )tgt_lkp_prty_agmt
                           ON              tgt_lkp_prty_agmt.agmt_id=src_agmt_id
                           AND             tgt_lkp_prty_agmt.prty_agmt_role_cd=src_prty_agmt_role_cd
                           AND             tgt_lkp_prty_agmt.prty_id=src_prty_id
                           WHERE           src_agmt_id IS NOT NULL ) src ) );
  -- Component exp_compare, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_compare AS
  (
         SELECT sq_pc_prty_agmt.tgt_agmt_id                                            AS lkp_agmt_id,
                sq_pc_prty_agmt.tgt_prty_agmt_role_cd                                  AS lkp_prty_agmt_role_cd,
                sq_pc_prty_agmt.tgt_prty_id                                            AS lkp_prty_id,
                sq_pc_prty_agmt.tgt_edw_strt_dttm                                      AS lkp_edw_strt_dttm,
                sq_pc_prty_agmt.tgt_trng_type_cd                                       AS lkp_trainingclasstype_typecode,
                sq_pc_prty_agmt.tgt_vld_drvrs_lic_ind                                  AS lkp_vld_drvrs_lic_ind,
                sq_pc_prty_agmt.tgt_edw_end_dttm                                       AS lkp_edw_end_dttm,
                sq_pc_prty_agmt.src_agmt_id                                            AS in_agmt_id,
                sq_pc_prty_agmt.src_prty_agmt_role_cd                                  AS in_prty_agmt_role_cd,
                sq_pc_prty_agmt.src_eff_dt                                             AS in_eff_dt,
                sq_pc_prty_agmt.src_end_dt                                             AS in_end_dt,
                sq_pc_prty_agmt.src_prty_id                                            AS in_prty_id,
                sq_pc_prty_agmt.src_training_class_type_cd                             AS in_trainingclasstype_typecode,
                sq_pc_prty_agmt.src_uslicensevalid_alfa                                AS in_vld_drvrs_lic_ind,
                sq_pc_prty_agmt.src_retired                                            AS retired,
                current_timestamp                                                      AS edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                :prcs_id                                                               AS prcs_id,
                CASE
                       WHEN sq_pc_prty_agmt.src_trans_strt_dttm IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )
                       ELSE sq_pc_prty_agmt.src_trans_strt_dttm
                END                                                                    AS trans_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS trans_end_dttm,
                sq_pc_prty_agmt.calc_ins_upd                                           AS calc_ins_upd,
                sq_pc_prty_agmt.source_record_id
         FROM   sq_pc_prty_agmt );
  -- Component rtr_ins_upd_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_ins_upd_insert as
  SELECT exp_compare.lkp_agmt_id                    AS lkp_agmt_id,
         exp_compare.lkp_prty_agmt_role_cd          AS lkp_prty_agmt_role_cd,
         exp_compare.lkp_prty_id                    AS lkp_prty_id,
         exp_compare.lkp_edw_strt_dttm              AS lkp_edw_strt_dttm,
         exp_compare.in_agmt_id                     AS in_agmt_id,
         exp_compare.in_prty_agmt_role_cd           AS in_prty_agmt_role_cd,
         exp_compare.in_eff_dt                      AS in_eff_dt,
         exp_compare.in_end_dt                      AS in_end_dt,
         exp_compare.in_prty_id                     AS in_prty_id,
         exp_compare.edw_strt_dttm                  AS in_edw_strt_dttm1,
         exp_compare.edw_end_dttm                   AS in_edw_end_dttm,
         exp_compare.in_trainingclasstype_typecode  AS in_trainingclasstype_typecode,
         exp_compare.in_vld_drvrs_lic_ind           AS in_vld_drvrs_lic_ind,
         exp_compare.prcs_id                        AS prcs_id,
         exp_compare.trans_strt_dttm                AS trans_strt_dttm,
         exp_compare.trans_end_dttm                 AS trans_end_dttm,
         exp_compare.calc_ins_upd                   AS calc_ins_upd,
         exp_compare.retired                        AS retired,
         exp_compare.lkp_edw_end_dttm               AS lkp_edw_end_dttm,
         exp_compare.lkp_trainingclasstype_typecode AS lkp_trainingclasstype_typecode,
         exp_compare.lkp_vld_drvrs_lic_ind          AS lkp_vld_drvrs_lic_ind,
         exp_compare.source_record_id
  FROM   exp_compare
  WHERE  exp_compare.in_agmt_id IS NOT NULL
  AND    exp_compare.in_prty_id IS NOT NULL
  AND    ( (
                       exp_compare.calc_ins_upd = ''I'' )
         OR     (
                       exp_compare.retired = 0
                AND    exp_compare.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) )
  OR     exp_compare.in_agmt_id IS NOT NULL
  AND    exp_compare.in_prty_id IS NOT NULL
  AND    (
                exp_compare.calc_ins_upd = ''U''
         AND    exp_compare.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  
  -- Component rtr_ins_upd_RETIRE, Type ROUTER Output Group RETIRE
  create or replace temporary table rtr_ins_upd_retire as
  SELECT exp_compare.lkp_agmt_id                    AS lkp_agmt_id,
         exp_compare.lkp_prty_agmt_role_cd          AS lkp_prty_agmt_role_cd,
         exp_compare.lkp_prty_id                    AS lkp_prty_id,
         exp_compare.lkp_edw_strt_dttm              AS lkp_edw_strt_dttm,
         exp_compare.in_agmt_id                     AS in_agmt_id,
         exp_compare.in_prty_agmt_role_cd           AS in_prty_agmt_role_cd,
         exp_compare.in_eff_dt                      AS in_eff_dt,
         exp_compare.in_end_dt                      AS in_end_dt,
         exp_compare.in_prty_id                     AS in_prty_id,
         exp_compare.edw_strt_dttm                  AS in_edw_strt_dttm1,
         exp_compare.edw_end_dttm                   AS in_edw_end_dttm,
         exp_compare.in_trainingclasstype_typecode  AS in_trainingclasstype_typecode,
         exp_compare.in_vld_drvrs_lic_ind           AS in_vld_drvrs_lic_ind,
         exp_compare.prcs_id                        AS prcs_id,
         exp_compare.trans_strt_dttm                AS trans_strt_dttm,
         exp_compare.trans_end_dttm                 AS trans_end_dttm,
         exp_compare.calc_ins_upd                   AS calc_ins_upd,
         exp_compare.retired                        AS retired,
         exp_compare.lkp_edw_end_dttm               AS lkp_edw_end_dttm,
         exp_compare.lkp_trainingclasstype_typecode AS lkp_trainingclasstype_typecode,
         exp_compare.lkp_vld_drvrs_lic_ind          AS lkp_vld_drvrs_lic_ind,
         exp_compare.source_record_id
  FROM   exp_compare
  WHERE  exp_compare.in_agmt_id IS NOT NULL
  AND    exp_compare.calc_ins_upd = ''R''
  AND    exp_compare.retired != 0
  AND    exp_compare.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_prty_agmt_update_Retire_Reject, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_prty_agmt_update_retire_reject AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ins_upd_retire.lkp_agmt_id                   AS lkp_agmt_id3,
                rtr_ins_upd_retire.lkp_prty_agmt_role_cd         AS lkp_prty_agmt_role_cd3,
                rtr_ins_upd_retire.lkp_prty_id                   AS lkp_prty_id3,
                rtr_ins_upd_retire.lkp_edw_strt_dttm             AS lkp_edw_strt_dttm3,
                NULL                                             AS in_edw_strt_dttm13,
                rtr_ins_upd_retire.trans_strt_dttm               AS trans_strt_dttm4,
                rtr_ins_upd_retire.in_trainingclasstype_typecode AS trainingclasstype_typecode3,
                rtr_ins_upd_retire.in_vld_drvrs_lic_ind          AS vld_drvrs_lic_ind,
                1                                                AS update_strategy_action,
				rtr_ins_upd_retire.source_record_id
         FROM   rtr_ins_upd_retire );
  -- Component upd_prty_agmt_insert, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_prty_agmt_insert AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ins_upd_insert.in_agmt_id                    AS in_agmt_id1,
                rtr_ins_upd_insert.in_prty_agmt_role_cd          AS in_prty_agmt_role_cd1,
                rtr_ins_upd_insert.in_eff_dt                     AS in_eff_dt1,
                rtr_ins_upd_insert.in_end_dt                     AS in_end_dt1,
                rtr_ins_upd_insert.in_prty_id                    AS in_prty_id1,
                rtr_ins_upd_insert.in_edw_strt_dttm1             AS in_edw_strt_dttm11,
                rtr_ins_upd_insert.in_edw_end_dttm               AS in_edw_end_dttm1,
                rtr_ins_upd_insert.prcs_id                       AS prcs_id1,
                rtr_ins_upd_insert.trans_strt_dttm               AS trans_strt_dttm1,
                rtr_ins_upd_insert.trans_end_dttm                AS trans_end_dttm1,
                rtr_ins_upd_insert.retired                       AS retired1,
                rtr_ins_upd_insert.in_trainingclasstype_typecode AS trainingclasstype_typecode1,
                rtr_ins_upd_insert.in_vld_drvrs_lic_ind          AS vld_drvrs_lic_ind,
                0                                                AS update_strategy_action,
				rtr_ins_upd_insert.source_record_id
         FROM   rtr_ins_upd_insert );
  -- Component exp_prty_agmt_insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_prty_agmt_insert AS
  (
         SELECT upd_prty_agmt_insert.in_edw_strt_dttm11 AS in_edw_strt_dttm11,
                CASE
                       WHEN upd_prty_agmt_insert.retired1 != 0 THEN upd_prty_agmt_insert.in_edw_strt_dttm11
                       ELSE upd_prty_agmt_insert.in_edw_end_dttm1
                END AS o_edw_end_dttm,
                CASE
                       WHEN upd_prty_agmt_insert.retired1 <> 0 THEN upd_prty_agmt_insert.trans_strt_dttm1
                       ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END                                              AS trans_end_dttm1,
                upd_prty_agmt_insert.trainingclasstype_typecode1 AS trainingclasstype_typecode1,
                upd_prty_agmt_insert.vld_drvrs_lic_ind           AS vld_drvrs_lic_ind,
                upd_prty_agmt_insert.source_record_id
         FROM   upd_prty_agmt_insert );
  -- Component exp_enddate_update_Retire_Reject, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_enddate_update_retire_reject AS
  (
         SELECT upd_prty_agmt_update_retire_reject.lkp_agmt_id3           AS lkp_agmt_id3,
                upd_prty_agmt_update_retire_reject.lkp_prty_agmt_role_cd3 AS lkp_prty_agmt_role_cd3,
                upd_prty_agmt_update_retire_reject.lkp_prty_id3           AS lkp_prty_id3,
                upd_prty_agmt_update_retire_reject.lkp_edw_strt_dttm3     AS lkp_edw_strt_dttm3,
                current_timestamp                                         AS expiry_end_date,
                upd_prty_agmt_update_retire_reject.trans_strt_dttm4       AS trans_strt_dttm4,
                upd_prty_agmt_update_retire_reject.source_record_id
         FROM   upd_prty_agmt_update_retire_reject );
  -- Component PRTY_AGMT_update_Retire_Reject, Type TARGET
  merge
  INTO         db_t_prod_core.prty_agmt
  USING        exp_enddate_update_retire_reject
  ON (
                            prty_agmt.agmt_id = exp_enddate_update_retire_reject.lkp_agmt_id3
               AND          prty_agmt.prty_agmt_role_cd = exp_enddate_update_retire_reject.lkp_prty_agmt_role_cd3
               AND          prty_agmt.prty_id = exp_enddate_update_retire_reject.lkp_prty_id3
               AND          prty_agmt.edw_strt_dttm = exp_enddate_update_retire_reject.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    agmt_id = exp_enddate_update_retire_reject.lkp_agmt_id3,
         prty_agmt_role_cd = exp_enddate_update_retire_reject.lkp_prty_agmt_role_cd3,
         prty_id = exp_enddate_update_retire_reject.lkp_prty_id3,
         edw_strt_dttm = exp_enddate_update_retire_reject.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_enddate_update_retire_reject.expiry_end_date,
         trans_end_dttm = exp_enddate_update_retire_reject.trans_strt_dttm4;
  
  -- Component PRTY_AGMT_update_Retire_Reject, Type Post SQL
  UPDATE db_t_prod_core.prty_agmt
    SET    edw_end_dttm = a.lead1,
         trans_end_dttm =a.lead2
  FROM   (
                  SELECT   agmt_id,
                           prty_agmt_role_cd,
                           prty_id,
                           edw_strt_dttm,
                           trans_strt_dttm ,
                           max(edw_strt_dttm) over (PARTITION BY agmt_id,prty_agmt_role_cd , prty_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND      1 following )   - interval ''1  second'' AS lead1 ,
                           max(trans_strt_dttm) over(PARTITION BY agmt_id,prty_agmt_role_cd ,prty_id ORDER BY trans_strt_dttm ASC ROWS BETWEEN 1 following AND      1 following ) - interval ''1  second'' AS lead2
                  FROM     db_t_prod_core.prty_agmt
                  WHERE    prty_agmt_role_cd NOT IN (''PLCYPRININS'',
                                                     ''OVRDPYR'',
                                                     ''CMP'',
                                                     ''UWR'',
                                                     ''PRDA'',
                                                     ''SVC'',
                                                     ''PRIINSCAR'')
                  GROUP BY agmt_id,
                           prty_agmt_role_cd,
                           prty_id,
                           edw_strt_dttm,
                           trans_strt_dttm ) a

  WHERE  prty_agmt.edw_strt_dttm = a.edw_strt_dttm
  AND    prty_agmt.agmt_id = a.agmt_id
  AND    prty_agmt.prty_agmt_role_cd = a.prty_agmt_role_cd
  AND    prty_agmt.prty_id = a.prty_id
  AND    cast(prty_agmt.edw_end_dttm AS DATE) = ''9999-12-31''
  AND    lead1 IS NOT NULL
  AND    lead2 IS NOT NULL;
  
  UPDATE db_t_prod_core.prty_agmt
    SET    edw_end_dttm = a.lead1,
         trans_end_dttm =a.lead2
  FROM   (
                  SELECT   agmt_id,
                           prty_agmt_role_cd,
                           prty_id,
                           edw_strt_dttm ,
                           trans_strt_dttm ,
                           max(edw_strt_dttm) over (PARTITION BY agmt_id,prty_agmt_role_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND      1 following )                      - interval ''1  second'' AS lead1 ,
                           max(trans_strt_dttm) over(PARTITION BY agmt_id,prty_agmt_role_cd ORDER BY trans_strt_dttm ASC,edw_strt_dttm ASC ROWS BETWEEN 1 following AND      1 following ) - interval ''1  second'' AS lead2
                  FROM     db_t_prod_core.prty_agmt
                  WHERE    prty_agmt_role_cd IN(''PLCYPRININS'',
                                                ''OVRDPYR'',
                                                ''CMP'',
                                                ''PRDA'',
                                                ''SVC'',
                                                ''PRIINSCAR'')
                  GROUP BY agmt_id,
                           prty_agmt_role_cd,
                           prty_id,
                           edw_strt_dttm ,
                           trans_strt_dttm ) a

  WHERE  prty_agmt.edw_strt_dttm = a.edw_strt_dttm
  AND    prty_agmt.agmt_id = a.agmt_id
  AND    prty_agmt.prty_agmt_role_cd = a.prty_agmt_role_cd
  AND    prty_agmt.prty_id = a.prty_id
  AND    cast(prty_agmt.edw_end_dttm AS DATE) = ''9999-12-31''
  AND    lead1 IS NOT NULL
  AND    lead2 IS NOT NULL ;
  
  UPDATE db_t_prod_core.prty_agmt
    SET    edw_end_dttm = a.lead1,
         trans_end_dttm =a.lead2
  FROM   (
                         SELECT DISTINCT agmt_id,
                                         prty_agmt_role_cd,
                                         prty_id,
                                         edw_strt_dttm ,
                                         max(edw_strt_dttm) over (PARTITION BY agmt_id,prty_agmt_role_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following )                      - interval ''1  second'' AS lead1 ,
                                         max(trans_strt_dttm) over(PARTITION BY agmt_id,prty_agmt_role_cd ORDER BY trans_strt_dttm ASC,edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following ) - interval ''1  second'' AS lead2
                         FROM            db_t_prod_core.prty_agmt
                         WHERE           prty_agmt_role_cd IN(''UWR'')
                         AND             trans_strt_dttm<>trans_end_dttm ) a

  WHERE  prty_agmt.edw_strt_dttm = a.edw_strt_dttm
  AND    prty_agmt.agmt_id = a.agmt_id
  AND    prty_agmt.prty_agmt_role_cd = a.prty_agmt_role_cd
  AND    prty_agmt.prty_id = a.prty_id
  AND    cast(prty_agmt.edw_end_dttm AS DATE) = ''9999-12-31''
  AND    lead1 IS NOT NULL
  AND    lead2 IS NOT NULL
  AND    prty_agmt.trans_strt_dttm<>prty_agmt.trans_end_dttm;
  
  -- Component PRTY_AGMT_insert, Type TARGET
  INSERT INTO db_t_prod_core.prty_agmt
              (
                          agmt_id,
                          prty_agmt_role_cd,
                          prty_agmt_strt_dttm,
                          prty_id,
                          prty_agmt_end_dttm,
                          prcs_id,
                          trng_type_cd,
                          vld_drvrs_lic_ind,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT     upd_prty_agmt_insert.in_agmt_id1                 AS agmt_id,
             upd_prty_agmt_insert.in_prty_agmt_role_cd1       AS prty_agmt_role_cd,
             upd_prty_agmt_insert.in_eff_dt1                  AS prty_agmt_strt_dttm,
             upd_prty_agmt_insert.in_prty_id1                 AS prty_id,
             upd_prty_agmt_insert.in_end_dt1                  AS prty_agmt_end_dttm,
             upd_prty_agmt_insert.prcs_id1                    AS prcs_id,
             exp_prty_agmt_insert.trainingclasstype_typecode1 AS trng_type_cd,
             exp_prty_agmt_insert.vld_drvrs_lic_ind           AS vld_drvrs_lic_ind,
             upd_prty_agmt_insert.in_edw_strt_dttm11          AS edw_strt_dttm,
             exp_prty_agmt_insert.o_edw_end_dttm              AS edw_end_dttm,
             upd_prty_agmt_insert.trans_strt_dttm1            AS trans_strt_dttm,
             exp_prty_agmt_insert.trans_end_dttm1             AS trans_end_dttm
  FROM       upd_prty_agmt_insert
  inner join exp_prty_agmt_insert
  ON         upd_prty_agmt_insert.source_record_id = exp_prty_agmt_insert.source_record_id;

END;
';