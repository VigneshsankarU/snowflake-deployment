-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AR_INVOICELINE_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component SQ_ar_invoiceline, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_ar_invoiceline AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS ar_invc_id,
                $2  AS host_invc_ln_num,
                $3  AS ar_invc_ln_type_cd,
                $4  AS plcy_agmt_id,
                $5  AS installmentnumber_stg,
                $6  AS flag,
                $7  AS tgt_ar_invc_ln_num,
                $8  AS tgt_ar_invc_id,
                $9  AS tgt_host_invc_ln_num,
                $10 AS out_src_cd,
                $11 AS updatetime,
                $12 AS retired,
                $13 AS tgt_edw_end_dttm,
                $14 AS tgt_edw_strt_dttm,
                $15 AS ar_description,
                $16 AS updatetime_actual,
                $17 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT sq1.ar_invc_id,
                                                sq1.host_invc_ln_num,
                                                sq1.ar_invc_ln_type_cd,
                                                sq1.plcy_agmt_id,
                                                sq1.installmentnumber_stg,
                                                sq1.flag,
                                                sq1.tgt_ar_invc_ln_num,
                                                sq1.tgt_ar_invc_id,
                                                sq1.tgt_host_invc_ln_num,
                                                sq1.out_src_cd,
                                                sq1.updatetime,
                                                sq1.retired,
                                                sq1.tgt_edw_end_dttm,
                                                sq1.tgt_edw_strt_dttm,
                                                sq1.ar_description,
                                                sq1.updatetime_actual
                                         FROM   (
                                                                SELECT          invoicenumber_stg,
                                                                                id_stg AS host_invc_ln_num,
                                                                                typecode_stg,
                                                                                agmt_typecode,
                                                                                policynumber_a,
                                                                                installmentnumber_stg,
                                                                                termnumber_stg,
                                                                                eventdate_stg,
                                                                                coalesce(to_char(cast(eventdate_stg AS timestamp)),''1900-01-01 00:00:00.000000'') AS updatetime,
                                                                                retired,
                                                                                description_stg                                                     AS ar_description,
                                                                                coalesce(to_char(updatetime_actual1) ,''1900/01/01 00:00:00.000000'') AS updatetime_actual,
                                                                                /*lkp_xlat_src_cd*/
                                                                                xlat_src_cd.tgt_idntftn_val AS out_src_cd,
                                                                                /*lkp_agmt_type_cd*/
                                                                                xlat_agmt_type_cd.tgt_idntftn_val AS agmt_type_cd_act_pol,
                                                                                /*lkp_xlat_doc_type*/
                                                                                xlat_doc_type.tgt_idntftn_val AS doc_type_cd,
                                                                                /*lkp_xlat_ctgy_type*/
                                                                                xlat_ctgy_type.tgt_idntftn_val AS doc_ctgy_type_cd,
                                                                                /*lkp_agmt_poltrm*/
                                                                                agmt1.agmt_id AS agmt_id1,
                                                                                /*lkp_doc_id*/
                                                                                doc.doc_id AS ar_invc_id,
                                                                                /*lkp_agmt2*/
                                                                                agmt2.agmt_id               AS agmt_id2,
                                                                                coalesce(agmt_id1,agmt_id2) AS plcy_agmt_id,
                                                                                /*LKP_ETL_REF_XLAT*/
                                                                                etl_ref_xlat.tgt_idntftn_val AS ar_invc_ln_type_cd,
                                                                                /*LKP_TGT_AR_INVC_LN*/
                                                                                tgt_ar_invc_ln.ar_invc_ln_num                                   AS tgt_ar_invc_ln_num,
                                                                                tgt_ar_invc_ln.ar_invc_ln_type_cd                               AS tgt_ar_invc_ln_type_cd,
                                                                                tgt_ar_invc_ln.plcy_term_agmt_id                                AS tgt_plcy_term_agmt_id,
                                                                                tgt_ar_invc_ln.instlmt_num                                      AS tgt_instlmt_num,
                                                                                to_char(cast(tgt_ar_invc_ln.ar_invc_ln_strt_dttm AS timestamp)) AS tgt_ar_invc_ln_strt_dttm,
                                                                                tgt_ar_invc_ln.edw_strt_dttm                                    AS tgt_edw_strt_dttm,
                                                                                tgt_ar_invc_ln.edw_end_dttm                                     AS tgt_edw_end_dttm,
                                                                                tgt_ar_invc_ln.ar_invc_id                                       AS tgt_ar_invc_id,
                                                                                tgt_ar_invc_ln.host_invc_ln_num                                 AS tgt_host_invc_ln_num,
                                                                                /*SOURCEMD5DATA*/
                                                                                cast(concat(coalesce(trim(cast(ar_invc_ln_type_cd AS VARCHAR(100))),''''), coalesce(trim(cast(plcy_agmt_id AS VARCHAR(100))),''''), coalesce(trim(cast(installmentnumber_stg AS VARCHAR(100))),''''), coalesce(trim(cast(updatetime AS VARCHAR(100))),''''), coalesce(trim(cast(to_char(doc_id) AS VARCHAR(100))),'''')) AS VARCHAR(1000)) AS sourcedata,
                                                                                /*TARGETMD5DATA*/
                                                                                cast(concat(coalesce(trim(cast(tgt_ar_invc_ln_type_cd AS VARCHAR(100))),''''), coalesce(trim(cast(tgt_plcy_term_agmt_id AS VARCHAR(100))),''''), coalesce(trim(cast(tgt_instlmt_num AS VARCHAR(100))),''''), coalesce(trim(cast(tgt_ar_invc_ln_strt_dttm AS VARCHAR(100))),''''), coalesce(trim(cast(to_char(tgt_ar_invc_id) AS VARCHAR(100))),'''')) AS VARCHAR(1000)) AS targetdata,
                                                                                /*checkflag*/
                                                                                CASE
                                                                                                WHEN length(targetdata) =0 THEN ''I''
                                                                                                WHEN trim(targetdata) <> trim(sourcedata) THEN ''U''
                                                                                                ELSE ''R''
                                                                                END AS flag
                                                                FROM            (
                                                                                                SELECT DISTINCT bc_invoice.invoicenumber_stg,
                                                                                                                bc_invoiceitem.id_stg,
                                                                                                                bctl_invoiceitemtype.typecode_stg,
                                                                                                                CASE
                                                                                                                                WHEN termnumber_stg IS NULL THEN ''AGMT_TYPE1''
                                                                                                                                ELSE ''AGMT_TYPE6''
                                                                                                                END                                                             agmt_typecode,
                                                                                                                coalesce(bc_policyperiod.policynumber_stg,accountnumber_stg) AS policynumber_a,
                                                                                                                bc_invoiceitem.installmentnumber_stg,
                                                                                                                bc_invoiceitem.eventdate_stg,
                                                                                                                1         AS ctl_id,
                                                                                                                2         AS process_id,
                                                                                                                ''EDW_ETL'' AS load_user,
                                                                                                                y.publicid_stg,
                                                                                                                termnumber_stg,
                                                                                                                CASE
                                                                                                                                WHEN bc_invoiceitem.retired_stg=0
                                                                                                                                                /*  and bc_policyperiod.Retired=0 and bc_invoice.Retired=0  */
                                                                                                                                THEN 0
                                                                                                                                ELSE 1
                                                                                                                END retired,
                                                                                                                bctl_billinginstruction.description_stg ,
                                                                                                                bc_invoiceitem.updatetime_stg AS updatetime_actual1
                                                                                                FROM            db_t_prod_stag.bc_invoice
                                                                                                inner join      db_t_prod_stag.bc_invoiceitem
                                                                                                ON              bc_invoice.id_stg=bc_invoiceitem.invoiceid_stg
                                                                                                inner join      db_t_prod_stag.bctl_invoiceitemtype
                                                                                                ON              bctl_invoiceitemtype.id_stg=bc_invoiceitem.type_stg
                                                                                                left join       db_t_prod_stag.bc_policyperiod
                                                                                                ON              bc_policyperiod.id_stg=bc_invoiceitem.policyperiodid_stg
                                                                                                left join       db_t_prod_stag.bc_account
                                                                                                ON              bc_account.id_stg=bc_invoice.accountid_stg
                                                                                                left join       db_t_prod_stag.bc_charge
                                                                                                ON              bc_charge.id_stg = bc_invoiceitem.chargeid_stg
                                                                                                left join       db_t_prod_stag.bc_billinginstruction
                                                                                                ON              bc_billinginstruction.id_stg = bc_charge.billinginstructionid_stg
                                                                                                left join       db_t_prod_stag.bctl_billinginstruction
                                                                                                ON              bctl_billinginstruction.id_stg = bc_billinginstruction.subtype_stg
                                                                                                left join
                                                                                                                (
                                                                                                                       SELECT policynumber_stg,
                                                                                                                              x.publicid_stg
                                                                                                                       FROM   (
                                                                                                                                         SELECT     pol.policynumber_stg,
                                                                                                                                                    pol.cancellationdate_stg AS cancellationdate,
                                                                                                                                                    pol.periodstart_stg      AS periodstart,
                                                                                                                                                    pol.modelnumber_stg,
                                                                                                                                                    pol.termnumber_stg,
                                                                                                                                                    pol.publicid_stg ,
                                                                                                                                                    row_number() over(PARTITION BY pol.policynumber_stg,pol.termnumber_stg ORDER BY pol.policynumber_stg,pol.modelnumber_stg DESC) AS r
                                                                                                                                         FROM       db_t_prod_stag.pc_policyperiod pol
                                                                                                                                         inner join db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                         ON         pol.status_stg=pctl_policyperiodstatus.id_stg
                                                                                                                                         WHERE      pctl_policyperiodstatus.typecode_stg=''Bound'' qualify row_number() over( PARTITION BY policynumber_stg ORDER BY publicid_stg DESC) = 1)x
                                                                                                                       WHERE  r=1 )y
                                                                                                ON              y.policynumber_stg = policynumber_a
                                                                                                WHERE           bc_invoiceitem.updatetime_stg > ($start_dttm)
                                                                                                AND             bc_invoiceitem.updatetime_stg <= ($end_dttm))sq
                                                                left outer join
                                                                                (
                                                                                       SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                              teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                       FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                                       WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
                                                                                       AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                                       AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                                       AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'') xlat_src_cd
                                                                ON              xlat_src_cd.src_idntftn_val=''SRC_SYS4''
                                                                left outer join
                                                                                (
                                                                                       SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                              teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                       FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                                       WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''AGMT_TYPE''
                                                                                       AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                                       AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                                       AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'') xlat_agmt_type_cd
                                                                ON              xlat_agmt_type_cd.src_idntftn_val =agmt_typecode
                                                                left outer join
                                                                                (
                                                                                       SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                              teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                       FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                                       WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_TYPE''
                                                                                       AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'') xlat_doc_type
                                                                ON              xlat_doc_type.src_idntftn_val=''DOC_TYPE3''
                                                                left outer join
                                                                                (
                                                                                       SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                              teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                       FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                                       WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_CTGY_TYPE''
                                                                                       AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'') xlat_ctgy_type
                                                                ON              xlat_ctgy_type.src_idntftn_val=''DOC_CTGY_TYPE4''
                                                                left outer join
                                                                                (
                                                                                         SELECT   agmt.agmt_id       AS agmt_id,
                                                                                                  agmt.edw_end_dttm  AS edw_end_dttm,
                                                                                                  agmt.host_agmt_num AS host_agmt_num,
                                                                                                  agmt.term_num      AS term_num,
                                                                                                  agmt.agmt_type_cd  AS agmt_type_cd
                                                                                         FROM     db_t_prod_core.agmt qualify row_number() over(PARTITION BY agmt.host_agmt_num, agmt.term_num, agmt.agmt_type_cd ORDER BY agmt.edw_end_dttm DESC) = 1) agmt1
                                                                ON              sq.policynumber_a=agmt1.host_agmt_num
                                                                AND             sq.termnumber_stg=agmt1.term_num
                                                                AND             agmt_type_cd_act_pol=agmt1.agmt_type_cd
                                                                left outer join
                                                                                (
                                                                                         SELECT   doc.doc_id            AS doc_id,
                                                                                                  doc.tm_prd_cd         AS tm_prd_cd,
                                                                                                  doc.doc_crtn_dttm     AS doc_crtn_dttm,
                                                                                                  doc.doc_recpt_dt      AS doc_recpt_dt,
                                                                                                  doc.doc_prd_strt_dttm AS doc_prd_strt_dttm,
                                                                                                  doc.doc_prd_end_dttm  AS doc_prd_end_dttm,
                                                                                                  doc.edw_strt_dttm     AS edw_strt_dttm,
                                                                                                  doc.data_src_type_cd  AS data_src_type_cd,
                                                                                                  doc.doc_desc_txt      AS doc_desc_txt,
                                                                                                  doc.doc_name          AS doc_name,
                                                                                                  doc.doc_host_num      AS doc_host_num,
                                                                                                  doc.doc_host_vers_num AS doc_host_vers_num,
                                                                                                  doc.doc_cycl_cd       AS doc_cycl_cd,
                                                                                                  doc.mm_objt_id        AS mm_objt_id,
                                                                                                  doc.lang_type_cd      AS lang_type_cd,
                                                                                                  doc.prcs_id           AS prcs_id,
                                                                                                  doc.doc_sts_cd        AS doc_sts_cd,
                                                                                                  doc.doc_issur_num     AS doc_issur_num,
                                                                                                  doc.doc_type_cd       AS doc_type_cd,
                                                                                                  doc.doc_ctgy_type_cd  AS doc_ctgy_type_cd
                                                                                         FROM     db_t_prod_core.doc qualify row_number () over (PARTITION BY doc_issur_num,doc_ctgy_type_cd,doc_type_cd ORDER BY edw_end_dttm DESC)=1) doc
                                                                ON              sq.invoicenumber_stg=doc.doc_issur_num
                                                                AND             doc_type_cd=doc.doc_type_cd
                                                                AND             doc_ctgy_type_cd=doc.doc_ctgy_type_cd
                                                                left outer join
                                                                                (
                                                                                         SELECT   agmt.agmt_id       AS agmt_id,
                                                                                                  agmt.host_agmt_num AS host_agmt_num,
                                                                                                  agmt.nk_src_key    AS nk_src_key,
                                                                                                  agmt.edw_end_dttm  AS edw_end_dttm ,
                                                                                                  agmt.agmt_type_cd
                                                                                         FROM     db_t_prod_core.agmt qualify row_number() over(PARTITION BY agmt.nk_src_key,agmt.host_agmt_num ORDER BY agmt.edw_end_dttm DESC) = 1) agmt2
                                                                ON              sq.policynumber_a=agmt2.nk_src_key
                                                                AND             agmt_type_cd_act_pol=agmt2.agmt_type_cd
                                                                left outer join
                                                                                (
                                                                                       SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                              teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                       FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                                       WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''AR_INVC_LN_TYPE''
                                                                                       AND    teradata_etl_ref_xlat.src_idntftn_nm= ''bctl_invoiceitemtype.TYPECODE''
                                                                                       AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                                                                                       AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'') etl_ref_xlat
                                                                ON              sq.typecode_stg=etl_ref_xlat.src_idntftn_val
                                                                left outer join
                                                                                (
                                                                                         SELECT   ar_invc_ln.ar_invc_ln_num       AS ar_invc_ln_num,
                                                                                                  ar_invc_ln.ar_invc_ln_type_cd   AS ar_invc_ln_type_cd,
                                                                                                  ar_invc_ln.plcy_term_agmt_id    AS plcy_term_agmt_id,
                                                                                                  ar_invc_ln.instlmt_num          AS instlmt_num,
                                                                                                  ar_invc_ln.ar_invc_ln_strt_dttm AS ar_invc_ln_strt_dttm,
                                                                                                  ar_invc_ln.edw_strt_dttm        AS edw_strt_dttm,
                                                                                                  ar_invc_ln.edw_end_dttm         AS edw_end_dttm,
                                                                                                  ar_invc_ln.ar_invc_id           AS ar_invc_id,
                                                                                                  ar_invc_ln.host_invc_ln_num     AS host_invc_ln_num
                                                                                         FROM     db_t_prod_core.ar_invc_ln qualify row_number() over(PARTITION BY ar_invc_ln.host_invc_ln_num ORDER BY ar_invc_ln.edw_end_dttm DESC) = 1) tgt_ar_invc_ln
                                                                ON              cast(sq.id_stg AS VARCHAR(50))=tgt_ar_invc_ln.host_invc_ln_num)sq1 ) src ) );
  -- Component exp_check_flag, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_check_flag AS
  (
         SELECT sq_ar_invoiceline.ar_invc_id                                                                                    AS ar_invc_id,
                sq_ar_invoiceline.host_invc_ln_num                                                                              AS host_invc_ln_num,
                sq_ar_invoiceline.ar_invc_ln_type_cd                                                                            AS ar_invc_ln_type_cd,
                sq_ar_invoiceline.plcy_agmt_id                                                                                  AS plcy_agmt_id,
                sq_ar_invoiceline.installmentnumber_stg                                                                         AS installmentnumber_stg,
                sq_ar_invoiceline.flag                                                                                          AS flag,
                sq_ar_invoiceline.tgt_ar_invc_ln_num                                                                            AS tgt_ar_invc_ln_num,
                sq_ar_invoiceline.tgt_ar_invc_id                                                                                AS tgt_ar_invc_id,
                sq_ar_invoiceline.tgt_host_invc_ln_num                                                                          AS tgt_host_invc_ln_num,
                sq_ar_invoiceline.out_src_cd                                                                                    AS out_src_cd,
                sq_ar_invoiceline.updatetime                                                                                    AS updatetime,
                sq_ar_invoiceline.retired                                                                                       AS retired,
                sq_ar_invoiceline.tgt_edw_end_dttm                                                                              AS tgt_edw_end_dttm,
                sq_ar_invoiceline.tgt_edw_strt_dttm                                                                             AS tgt_edw_strt_dttm,
                sq_ar_invoiceline.ar_description                                                                                AS ar_description,
                to_timestamp_ntz ( sq_ar_invoiceline.updatetime_actual ,  ''YYYY/MM/DD HH24:MI:SS'' ) 							AS v_updatetime_actual,
                v_updatetime_actual                                                                                             AS o_updatetime_actual,
                $prcs_id                                                                                                        AS prcsid,
                current_timestamp                                                                                               AS out_edw_strt_dttm,
                to_timestamp_ntz( ''12/31/9999 23:59:59.999999''  , ''MM/DD/YYYY HH24:MI:SS.FF6'' )                                          AS out_edw_end_dttm,
                dateadd(''second'', - 1, v_updatetime_actual)                                                                         AS trans_end_dttm_upd,
                to_timestamp_ntz( ''12/31/9999 23:59:59.999999''  , ''MM/DD/YYYY HH24:MI:SS.FF6'' )                                          AS trans_end_dttm_ins,
                sq_ar_invoiceline.source_record_id
         FROM   sq_ar_invoiceline );
  -- Component rtr_ins_upd_Retired, Type ROUTER Output Group Retired
  create or replace temporary table rtr_ins_upd_Retired as
  SELECT exp_check_flag.ar_invc_id            AS ar_invc_id,
         exp_check_flag.host_invc_ln_num      AS host_invc_ln_num,
         exp_check_flag.ar_invc_ln_type_cd    AS ar_invc_ln_type_cd,
         exp_check_flag.plcy_agmt_id          AS plcy_agmt_id,
         exp_check_flag.installmentnumber_stg AS instlmt_num,
         exp_check_flag.prcsid                AS prcsid,
         exp_check_flag.flag                  AS o_flag,
         exp_check_flag.tgt_ar_invc_ln_num    AS lkp_ar_invc_ln_num,
         exp_check_flag.out_edw_strt_dttm     AS out_edw_strt_dttm,
         exp_check_flag.out_edw_end_dttm      AS out_edw_end_dttm,
         exp_check_flag.tgt_ar_invc_id        AS lkp_ar_invc_id,
         exp_check_flag.tgt_host_invc_ln_num  AS lkp_host_invc_ln_num,
         exp_check_flag.tgt_edw_strt_dttm     AS edw_strt_dttm,
         NULL                                 AS out_edw_end_dttm_1sec,
         exp_check_flag.out_src_cd            AS in_sys_src_cd,
         exp_check_flag.updatetime            AS updatetime_o,
         exp_check_flag.retired               AS retired,
         exp_check_flag.tgt_edw_end_dttm      AS lkp_edw_end_dttm,
         exp_check_flag.ar_description        AS ar_description,
         exp_check_flag.o_updatetime_actual   AS updatetime_actual,
         exp_check_flag.trans_end_dttm_upd    AS trans_end_dttm_upd,
         exp_check_flag.trans_end_dttm_ins    AS trans_end_dttm_ins,
         exp_check_flag.source_record_id
  FROM   exp_check_flag
  WHERE  exp_check_flag.ar_invc_id IS NOT NULL
  AND    (
                exp_check_flag.flag = ''R''
         AND    exp_check_flag.retired != 0
         AND    exp_check_flag.tgt_edw_end_dttm = to_timestamp_ntz( ''12/31/9999 23:59:59.999999''  , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  
  -- Component rtr_ins_upd_insert, Type ROUTER Output Group insert
  create or replace temporary table rtr_ins_upd_insert as
  SELECT exp_check_flag.ar_invc_id            AS ar_invc_id,
         exp_check_flag.host_invc_ln_num      AS host_invc_ln_num,
         exp_check_flag.ar_invc_ln_type_cd    AS ar_invc_ln_type_cd,
         exp_check_flag.plcy_agmt_id          AS plcy_agmt_id,
         exp_check_flag.installmentnumber_stg AS instlmt_num,
         exp_check_flag.prcsid                AS prcsid,
         exp_check_flag.flag                  AS o_flag,
         exp_check_flag.tgt_ar_invc_ln_num    AS lkp_ar_invc_ln_num,
         exp_check_flag.out_edw_strt_dttm     AS out_edw_strt_dttm,
         exp_check_flag.out_edw_end_dttm      AS out_edw_end_dttm,
         exp_check_flag.tgt_ar_invc_id        AS lkp_ar_invc_id,
         exp_check_flag.tgt_host_invc_ln_num  AS lkp_host_invc_ln_num,
         exp_check_flag.tgt_edw_strt_dttm     AS edw_strt_dttm,
         NULL                                 AS out_edw_end_dttm_1sec,
         exp_check_flag.out_src_cd            AS in_sys_src_cd,
         exp_check_flag.updatetime            AS updatetime_o,
         exp_check_flag.retired               AS retired,
         exp_check_flag.tgt_edw_end_dttm      AS lkp_edw_end_dttm,
         exp_check_flag.ar_description        AS ar_description,
         exp_check_flag.o_updatetime_actual   AS updatetime_actual,
         exp_check_flag.trans_end_dttm_upd    AS trans_end_dttm_upd,
         exp_check_flag.trans_end_dttm_ins    AS trans_end_dttm_ins,
         exp_check_flag.source_record_id
  FROM   exp_check_flag
  WHERE  exp_check_flag.ar_invc_id IS NOT NULL
  AND    (
                exp_check_flag.flag = ''I''
         OR     (
                       exp_check_flag.tgt_edw_end_dttm != to_timestamp_ntz( ''12/31/9999 23:59:59.999999''  , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                AND    exp_check_flag.retired = 0 ) );
  
  -- Component rtr_ins_upd_update, Type ROUTER Output Group update
  create or replace temporary table rtr_ins_upd_update as
  SELECT exp_check_flag.ar_invc_id            AS ar_invc_id,
         exp_check_flag.host_invc_ln_num      AS host_invc_ln_num,
         exp_check_flag.ar_invc_ln_type_cd    AS ar_invc_ln_type_cd,
         exp_check_flag.plcy_agmt_id          AS plcy_agmt_id,
         exp_check_flag.installmentnumber_stg AS instlmt_num,
         exp_check_flag.prcsid                AS prcsid,
         exp_check_flag.flag                  AS o_flag,
         exp_check_flag.tgt_ar_invc_ln_num    AS lkp_ar_invc_ln_num,
         exp_check_flag.out_edw_strt_dttm     AS out_edw_strt_dttm,
         exp_check_flag.out_edw_end_dttm      AS out_edw_end_dttm,
         exp_check_flag.tgt_ar_invc_id        AS lkp_ar_invc_id,
         exp_check_flag.tgt_host_invc_ln_num  AS lkp_host_invc_ln_num,
         exp_check_flag.tgt_edw_strt_dttm     AS edw_strt_dttm,
         NULL                                 AS out_edw_end_dttm_1sec,
         exp_check_flag.out_src_cd            AS in_sys_src_cd,
         exp_check_flag.updatetime            AS updatetime_o,
         exp_check_flag.retired               AS retired,
         exp_check_flag.tgt_edw_end_dttm      AS lkp_edw_end_dttm,
         exp_check_flag.ar_description        AS ar_description,
         exp_check_flag.o_updatetime_actual   AS updatetime_actual,
         exp_check_flag.trans_end_dttm_upd    AS trans_end_dttm_upd,
         exp_check_flag.trans_end_dttm_ins    AS trans_end_dttm_ins,
         exp_check_flag.source_record_id
  FROM   exp_check_flag
  WHERE  exp_check_flag.ar_invc_id IS NOT NULL
  AND    (
                exp_check_flag.flag = ''U''
         AND    exp_check_flag.tgt_edw_end_dttm = to_timestamp_ntz( ''12/31/9999 23:59:59.999999''  , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  
  -- Component upd_retired, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_retired AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ins_upd_retired.lkp_ar_invc_id       AS lkp_ar_invc_id3,
                rtr_ins_upd_retired.lkp_host_invc_ln_num AS lkp_host_invc_ln_num3,
                rtr_ins_upd_retired.lkp_ar_invc_ln_num   AS lkp_ar_invc_ln_num3,
                rtr_ins_upd_retired.edw_strt_dttm        AS edw_strt_dttm1,
                rtr_ins_upd_retired.out_edw_strt_dttm    AS edw_end_dttm,
                rtr_ins_upd_retired.trans_end_dttm_upd   AS trans_end_dttm_upd4,
                1                                        AS update_strategy_action,
				source_record_id
         FROM   rtr_ins_upd_retired );
  -- Component AR_INVC_LN_retired, Type TARGET
  /* Perform Updates */
  merge
  INTO         db_t_prod_core.ar_invc_ln
  USING        upd_retired
  ON (
                            update_strategy_action = 1
               AND          ar_invc_ln.ar_invc_ln_num = upd_retired.lkp_ar_invc_ln_num3
               AND          ar_invc_ln.edw_strt_dttm = upd_retired.edw_strt_dttm1)
  WHEN matched THEN
  UPDATE
  SET    host_invc_ln_num = upd_retired.lkp_host_invc_ln_num3,
         edw_end_dttm = upd_retired.edw_end_dttm,
         trans_end_dttm = upd_retired.trans_end_dttm_upd4 ;
  
  -- Component AR_INVC_LN_retired, Type Post SQL
  UPDATE db_t_prod_core.ar_invc_ln
  SET    edw_end_dttm=a.lead1 ,
         trans_end_dttm=a.lead2
  FROM   (
                         SELECT DISTINCT ar_invc_id,
                                         ar_invc_ln_num,
                                         host_invc_ln_num,
                                         edw_strt_dttm,
                                         trans_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY ar_invc_ln_num ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1 ,
                                         max(trans_strt_dttm) over (PARTITION BY ar_invc_ln_num ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead2
                         FROM            db_t_prod_core.ar_invc_ln ) a

  WHERE  ar_invc_ln.edw_strt_dttm = a.edw_strt_dttm
  AND    ar_invc_ln.trans_strt_dttm = a.trans_strt_dttm
  AND    ar_invc_ln.host_invc_ln_num=a.host_invc_ln_num
  AND    ar_invc_ln.ar_invc_id=a.ar_invc_id
  AND    ar_invc_ln.ar_invc_ln_num=a.ar_invc_ln_num
  AND    cast(ar_invc_ln.edw_end_dttm AS   DATE)=''9999-12-31''
  AND    cast(ar_invc_ln.trans_end_dttm AS DATE)=''9999-12-31''
  AND    lead1 IS NOT NULL
  AND    lead2 IS NOT NULL;
  
  -- Component exp_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ins AS
  (
         SELECT rtr_ins_upd_insert.ar_invc_id         AS ar_invc_id1,
                seq_ar_invoiceline.NEXTVAL            AS ar_invc_ln_num,
                rtr_ins_upd_insert.host_invc_ln_num   AS host_invc_ln_num1,
                rtr_ins_upd_insert.ar_invc_ln_type_cd AS ar_invc_ln_type_cd1,
                rtr_ins_upd_insert.plcy_agmt_id       AS plcy_agmt_id1,
                rtr_ins_upd_insert.instlmt_num        AS instlmt_num1,
                rtr_ins_upd_insert.prcsid             AS prcsid1,
                rtr_ins_upd_insert.out_edw_strt_dttm  AS out_edw_strt_dttm1,
                CASE
                       WHEN rtr_ins_upd_insert.retired <> 0 THEN current_timestamp
                       ELSE rtr_ins_upd_insert.out_edw_end_dttm
                END                               AS out_edw_end_dttm11,
                rtr_ins_upd_insert.in_sys_src_cd  AS in_sys_src_cd1,
                rtr_ins_upd_insert.updatetime_o   AS updatetime_o1,
                rtr_ins_upd_insert.ar_description AS ar_description1,
                CASE
                       WHEN rtr_ins_upd_insert.retired <> 0 THEN rtr_ins_upd_insert.updatetime_actual
                       ELSE rtr_ins_upd_insert.trans_end_dttm_ins
                END                                  AS out_trans_end_dttm_ins,
                rtr_ins_upd_insert.updatetime_actual AS updatetime_actual1,
                rtr_ins_upd_insert.source_record_id
         FROM   rtr_ins_upd_insert );
  -- Component exp_Set_end_timestamps, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_set_end_timestamps AS
  (
         SELECT rtr_ins_upd_update.ar_invc_id           AS ar_invc_id3,
                rtr_ins_upd_update.ar_invc_ln_type_cd   AS ar_invc_ln_type_cd3,
                rtr_ins_upd_update.plcy_agmt_id         AS plcy_agmt_id3,
                rtr_ins_upd_update.instlmt_num          AS instlmt_num3,
                rtr_ins_upd_update.prcsid               AS prcsid3,
                rtr_ins_upd_update.lkp_ar_invc_ln_num   AS lkp_ar_invc_ln_num3,
                rtr_ins_upd_update.out_edw_strt_dttm    AS out_edw_strt_dttm3,
                rtr_ins_upd_update.lkp_host_invc_ln_num AS host_invc_ln_num3,
                rtr_ins_upd_update.in_sys_src_cd        AS in_sys_src_cd3,
                rtr_ins_upd_update.updatetime_o         AS updatetime_o3,
                rtr_ins_upd_update.ar_description       AS ar_description3,
                rtr_ins_upd_update.updatetime_actual    AS updatetime_actual3,
                CASE
                       WHEN rtr_ins_upd_update.retired <> 0 THEN rtr_ins_upd_update.out_edw_strt_dttm
                       ELSE rtr_ins_upd_update.out_edw_end_dttm
                END AS out_edw_end_dttm,
                CASE
                       WHEN rtr_ins_upd_update.retired <> 0 THEN rtr_ins_upd_update.trans_end_dttm_upd
                       ELSE rtr_ins_upd_update.trans_end_dttm_ins
                END AS out_trans_end_dttm,
                rtr_ins_upd_update.source_record_id
         FROM   rtr_ins_upd_update );
  -- Component un_merge_ins_upd, Type UNION_TRANSFORMATION
  CREATE
  OR
  replace TEMPORARY TABLE un_merge_ins_upd AS
  (
         /* Union Group INS */
         SELECT exp_ins.ar_invc_id1,
                exp_ins.ar_invc_ln_num,
                exp_ins.host_invc_ln_num1,
                exp_ins.ar_invc_ln_type_cd1,
                exp_ins.plcy_agmt_id1,
                exp_ins.instlmt_num1,
                exp_ins.prcsid1,
                exp_ins.out_edw_strt_dttm1,
                exp_ins.out_edw_end_dttm11 AS out_edw_end_dttm111,
                exp_ins.in_sys_src_cd1,
                exp_ins.updatetime_o1,
                exp_ins.ar_description1,
                exp_ins.out_trans_end_dttm_ins AS trans_end_dttm_ins1,
                exp_ins.updatetime_actual1,
                exp_ins.source_record_id
         FROM   exp_ins
         UNION ALL
         /* Union Group UPD */
         SELECT exp_set_end_timestamps.ar_invc_id3         AS ar_invc_id1,
                exp_set_end_timestamps.lkp_ar_invc_ln_num3 AS ar_invc_ln_num,
                exp_set_end_timestamps.host_invc_ln_num3   AS host_invc_ln_num1,
                exp_set_end_timestamps.ar_invc_ln_type_cd3 AS ar_invc_ln_type_cd1,
                exp_set_end_timestamps.plcy_agmt_id3       AS plcy_agmt_id1,
                exp_set_end_timestamps.instlmt_num3        AS instlmt_num1,
                exp_set_end_timestamps.prcsid3             AS prcsid1,
                exp_set_end_timestamps.out_edw_strt_dttm3  AS out_edw_strt_dttm1,
                exp_set_end_timestamps.out_edw_end_dttm    AS out_edw_end_dttm111,
                exp_set_end_timestamps.in_sys_src_cd3      AS in_sys_src_cd1,
                exp_set_end_timestamps.updatetime_o3       AS updatetime_o1,
                exp_set_end_timestamps.ar_description3     AS ar_description1,
                exp_set_end_timestamps.out_trans_end_dttm  AS trans_end_dttm_ins1,
                exp_set_end_timestamps.updatetime_actual3  AS updatetime_actual1,
                exp_set_end_timestamps.source_record_id
         FROM   exp_set_end_timestamps );
  -- Component AR_INVC_LN_ins_new, Type TARGET
  INSERT INTO db_t_prod_core.ar_invc_ln
              (
                          ar_invc_id,
                          ar_invc_ln_num,
                          ar_invc_ln_desc_txt,
                          ar_invc_ln_type_cd,
                          host_invc_ln_num,
                          plcy_term_agmt_id,
                          instlmt_num,
                          prcs_id,
                          ar_invc_ln_strt_dttm,
                          edw_strt_dttm,
                          edw_end_dttm,
                          src_sys_cd,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT un_merge_ins_upd.ar_invc_id1         AS ar_invc_id,
         un_merge_ins_upd.ar_invc_ln_num      AS ar_invc_ln_num,
         un_merge_ins_upd.ar_description1     AS ar_invc_ln_desc_txt,
         un_merge_ins_upd.ar_invc_ln_type_cd1 AS ar_invc_ln_type_cd,
         un_merge_ins_upd.host_invc_ln_num1   AS host_invc_ln_num,
         un_merge_ins_upd.plcy_agmt_id1       AS plcy_term_agmt_id,
         un_merge_ins_upd.instlmt_num1        AS instlmt_num,
         un_merge_ins_upd.prcsid1             AS prcs_id,
         un_merge_ins_upd.updatetime_o1       AS ar_invc_ln_strt_dttm,
         un_merge_ins_upd.out_edw_strt_dttm1  AS edw_strt_dttm,
         un_merge_ins_upd.out_edw_end_dttm111 AS edw_end_dttm,
         un_merge_ins_upd.in_sys_src_cd1      AS src_sys_cd,
         un_merge_ins_upd.updatetime_actual1  AS trans_strt_dttm,
         un_merge_ins_upd.trans_end_dttm_ins1 AS trans_end_dttm
  FROM   un_merge_ins_upd;

END;
';