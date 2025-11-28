-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FINANCE_CHK_IMAGE_CLAIMCENTER_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_doc_ctgy_type AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_CTGY_TYPE''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_doc_type AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_TYPE''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_PMT_TYPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_pmt_type AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''CHK_IMAGE_PMT_METH_TYPE''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component SQ_cc_check, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_cc_check AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS publicid,
                $2  AS issuedate,
                $3  AS doc_type,
                $4  AS doc_category,
                $5  AS typecode,
                $6  AS createtime,
                $7  AS retired,
                $8  AS checknumber,
                $9  AS updatetime,
                $10 AS insurpay_tracknum,
                $11 AS insurpay_paymt_mtd,
                $12 AS irsupdateind_alfa,
                $13 AS rank,
                $14 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   publicid,
                                                    issuedate,
                                                    doc_type,
                                                    doc_category,
                                                    typecode,
                                                    createtime,
                                                    retired,
                                                    chknum,
                                                    updatetime ,
                                                    insurpay_tracknum ,
                                                    insurpay_paymt_mtd,
                                                    irsupdateind_alfa,
                                                    rank () over(PARTITION BY publicid, doc_type, doc_category ORDER BY updatetime) AS rowrank
                                           FROM     (
                                                                    SELECT DISTINCT chk.publicid,
                                                                                    chk.issuedate,
                                                                                    cast(''DOC_TYPE1'' AS      VARCHAR(50)) AS doc_type,
                                                                                    cast(''DOC_CTGY_TYPE1'' AS VARCHAR(50)) AS doc_category,
                                                                                    decode(cctl_reportabilitytype.typecode_stg,
                                                                                           ''reportable'',''1'',
                                                                                           ''notreportable'',''0'') AS typecode,
                                                                                    chk.createtime,
                                                                                    chk.retired     AS retired,
                                                                                    chk.checknumber AS chknum,
                                                                                    chk.updatetime ,
                                                                                    chk.insurpay_tracknum ,
                                                                                    chk.insurpay_paymt_mtd ,
                                                                                    CASE
                                                                                                    WHEN chk.irsupdateind_alfa = 0 THEN ''F''
                                                                                                    WHEN chk.irsupdateind_alfa = 1 THEN ''T''
                                                                                    END AS irsupdateind_alfa
                                                                    FROM            (
                                                                                                    SELECT DISTINCT cc_check.issuedate_stg               AS issuedate,
                                                                                                                    cc_check.updatetime_stg              AS updatetime,
                                                                                                                    cc_check.retired_stg                 AS retired,
                                                                                                                    cc_check.publicid_stg                AS publicid,
                                                                                                                    cc_check.createtime_stg              AS createtime,
                                                                                                                    cc_check.checknumber_stg             AS checknumber,
                                                                                                                    cc_check.reportability_stg           AS reportability,
                                                                                                                    insurpaytrackingnum_ext_stg          AS insurpay_tracknum,
                                                                                                                    cctl_insurpaymethod_ext.typecode_stg AS insurpay_paymt_mtd,
                                                                                                                    cc_check.irsupdateind_alfa_stg       AS irsupdateind_alfa
                                                                                                    FROM            (
                                                                                                                               SELECT     cc_claim.*
                                                                                                                               FROM       db_t_prod_stag.cc_claim
                                                                                                                               inner join db_t_prod_stag.cctl_claimstate
                                                                                                                               ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                               WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                    join            db_t_prod_stag.cc_check
                                                                                                    ON              cc_claim.id_stg = cc_check.claimid_stg
                                                                                                    join            db_t_prod_stag.cc_transaction
                                                                                                    ON              cc_check.id_stg =cc_transaction.checkid_stg
                                                                                                    join            db_t_prod_stag.cc_transactionlineitem
                                                                                                    ON              cc_transactionlineitem.transactionid_stg = cc_transaction.id_stg
                                                                                                    join            db_t_prod_stag.cctl_transactionstatus
                                                                                                    ON              cc_check.status_stg = cctl_transactionstatus.id_stg
                                                                                                    join            db_t_prod_stag.cctl_paymentmethod
                                                                                                    ON              cc_check.paymentmethod_stg = cctl_paymentmethod.id_stg
                                                                                                    left join       db_t_prod_stag.cctl_insurpaymethod_ext
                                                                                                    ON              cctl_insurpaymethod_ext.id_stg=cc_check.insurpaymethod_ext_stg
                                                                                                    WHERE           cc_check.updatetime_stg>($start_dttm)
                                                                                                    AND             cc_check.updatetime_stg <= ($end_dttm)
                                                                                                    AND             cctl_paymentmethod.typecode_stg <> ''expenseWithheld_alfa'' ) chk
                                                                    left outer join db_t_prod_stag.cctl_reportabilitytype
                                                                    ON              cctl_reportabilitytype.id_stg=chk.reportability
                                                                    UNION
                                                                    SELECT DISTINCT cmbk.combinedchecknumber_alfa AS publicid,
                                                                                    cmbk.issuedate,
                                                                                    cast(''DOC_TYPE6'' AS      VARCHAR(50)) AS doc_type,
                                                                                    cast(''DOC_CTGY_TYPE1'' AS VARCHAR(50)) AS doc_category,
                                                                                    decode(cctl_reportabilitytype.typecode_stg,
                                                                                           ''reportable'',''1'',
                                                                                           ''notreportable'',''0'') AS typecode,
                                                                                    cmbk.createtime,
                                                                                    cmbk.retired                  AS retired,
                                                                                    cmbk.combinedchecknumber_alfa AS chknum,
                                                                                    cmbk.updatetime ,
                                                                                    cmbk.insurpay_tracknum ,
                                                                                    cmbk.insurpay_paymt_mtd ,
                                                                                    CASE
                                                                                                    WHEN cmbk.irsupdateind_alfa = 0 THEN ''F''
                                                                                                    WHEN cmbk.irsupdateind_alfa = 1 THEN ''T''
                                                                                    END AS irsupdateind_alfa
                                                                    FROM            (
                                                                                                    SELECT DISTINCT cc_check.issuedate_stg                AS issuedate,
                                                                                                                    cc_check.updatetime_stg               AS updatetime,
                                                                                                                    cc_check.retired_stg                  AS retired,
                                                                                                                    cc_check.createtime_stg               AS createtime,
                                                                                                                    cc_check.combinedchecknumber_alfa_stg AS combinedchecknumber_alfa,
                                                                                                                    insurpaytrackingnum_ext_stg           AS insurpay_tracknum,
                                                                                                                    cctl_insurpaymethod_ext.typecode_stg  AS insurpay_paymt_mtd,
                                                                                                                    cc_check.irsupdateind_alfa_stg        AS irsupdateind_alfa,
                                                                                                                    cc_check.reportability_stg            AS reportability,
                                                                                                                    cc_check.status_stg                   AS status,
                                                                                                                    cc_claim.claimnumber_stg              AS claimnumber
                                                                                                    FROM            (
                                                                                                                               SELECT     cc_claim.*
                                                                                                                               FROM       db_t_prod_stag.cc_claim
                                                                                                                               inner join db_t_prod_stag.cctl_claimstate
                                                                                                                               ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                               WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                    join            db_t_prod_stag.cc_check
                                                                                                    ON              cc_claim.id_stg = cc_check.claimid_stg
                                                                                                    join            db_t_prod_stag.cc_transaction
                                                                                                    ON              cc_check.id_stg =cc_transaction.checkid_stg
                                                                                                    join            db_t_prod_stag.cc_transactionlineitem
                                                                                                    ON              cc_transactionlineitem.transactionid_stg = cc_transaction.id_stg
                                                                                                    join            db_t_prod_stag.cctl_transactionstatus
                                                                                                    ON              cc_check.status_stg = cctl_transactionstatus.id_stg
                                                                                                    join            db_t_prod_stag.cctl_paymentmethod
                                                                                                    ON              cc_check.paymentmethod_stg = cctl_paymentmethod.id_stg
                                                                                                    left join       db_t_prod_stag.cctl_insurpaymethod_ext
                                                                                                    ON              cctl_insurpaymethod_ext.id_stg=cc_check.insurpaymethod_ext_stg
                                                                                                    WHERE           cc_check.updatetime_stg>($start_dttm)
                                                                                                    AND             cc_check.updatetime_stg <= ($end_dttm)
                                                                                                    AND             cctl_paymentmethod.typecode_stg <> ''expenseWithheld_alfa'' )cmbk
                                                                    join            db_t_prod_stag.cctl_transactionstatus
                                                                    ON              cmbk.status=cctl_transactionstatus.id_stg
                                                                    left outer join db_t_prod_stag.cctl_reportabilitytype
                                                                    ON              cctl_reportabilitytype.id_stg=cmbk.reportability
                                                                    WHERE           cctl_transactionstatus.typecode_stg NOT IN (''issued'',
                                                                                                                                ''voided'')
                                                                    AND             cmbk.combinedchecknumber_alfa IS NOT NULL
                                                                    AND             cmbk.claimnumber IS NOT NULL qualify row_number() over (PARTITION BY cmbk.combinedchecknumber_alfa ORDER BY cmbk.createtime DESC) =1
                                                                    UNION
                                                                    /* EIM-34809 - Seperate UNION query for ISSUED and VOIDED) */
                                                                    SELECT DISTINCT cmbk.combinedchecknumber_alfa AS publicid,
                                                                                    cmbk.issuedate,
                                                                                    cast(''DOC_TYPE6'' AS      VARCHAR(50)) AS doc_type,
                                                                                    cast(''DOC_CTGY_TYPE1'' AS VARCHAR(50)) AS doc_category,
                                                                                    decode(rpt.typecode_stg,
                                                                                           ''reportable'',''1'',
                                                                                           ''notreportable'',''0'') AS typecode,
                                                                                    cmbk.createtime,
                                                                                    cmbk.retired                  AS retired,
                                                                                    cmbk.combinedchecknumber_alfa AS chknum,
                                                                                    cmbk.updatetime ,
                                                                                    cmbk.insurpay_tracknum ,
                                                                                    cmbk.insurpay_paymt_mtd ,
                                                                                    CASE
                                                                                                    WHEN cmbk.irsupdateind_alfa = 0 THEN ''F''
                                                                                                    WHEN cmbk.irsupdateind_alfa = 1 THEN ''T''
                                                                                    END AS irsupdateind_alfa
                                                                    FROM            (
                                                                                                    SELECT DISTINCT chk.issuedate_stg                AS issuedate,
                                                                                                                    chk.issuedate_stg                AS updatetime,
                                                                                                                    chk.retired_stg                  AS retired,
                                                                                                                    chk.createtime_stg               AS createtime,
                                                                                                                    chk.combinedchecknumber_alfa_stg AS combinedchecknumber_alfa,
                                                                                                                    insurpaytrackingnum_ext_stg      AS insurpay_tracknum,
                                                                                                                    ext.typecode_stg                 AS insurpay_paymt_mtd,
                                                                                                                    chk.irsupdateind_alfa_stg        AS irsupdateind_alfa,
                                                                                                                    chk.reportability_stg            AS reportability,
                                                                                                                    chk.status_stg                   AS status,
                                                                                                                    clm.claimnumber_stg              AS claimnumber
                                                                                                    FROM            (
                                                                                                                               SELECT     clm.*
                                                                                                                               FROM       db_t_prod_stag.cc_claim clm
                                                                                                                               inner join db_t_prod_stag.cctl_claimstate cst
                                                                                                                               ON         clm.state_stg= cst.id_stg
                                                                                                                               WHERE      cst.name_stg <> ''Draft'') clm
                                                                                                    join            db_t_prod_stag.cc_check chk
                                                                                                    ON              clm.id_stg = chk.claimid_stg
                                                                                                    join            db_t_prod_stag.cc_transaction ctn
                                                                                                    ON              chk.id_stg =ctn.checkid_stg
                                                                                                    join            db_t_prod_stag.cc_transactionlineitem ctl
                                                                                                    ON              ctl.transactionid_stg = ctn.id_stg
                                                                                                    join            db_t_prod_stag.cctl_transactionstatus ctls
                                                                                                    ON              chk.status_stg = ctls.id_stg
                                                                                                    join            db_t_prod_stag.cctl_paymentmethod pymt
                                                                                                    ON              chk.paymentmethod_stg = pymt.id_stg
                                                                                                    left join       db_t_prod_stag.cctl_insurpaymethod_ext ext
                                                                                                    ON              ext.id_stg=chk.insurpaymethod_ext_stg
                                                                                                    WHERE           chk.updatetime_stg>($start_dttm)
                                                                                                    AND             chk.updatetime_stg <= ($end_dttm)
                                                                                                    AND             pymt.typecode_stg <> ''expenseWithheld_alfa'' ) cmbk
                                                                    join            db_t_prod_stag.cctl_transactionstatus ctls
                                                                    ON              cmbk.status=ctls.id_stg
                                                                    left outer join db_t_prod_stag.cctl_reportabilitytype rpt
                                                                    ON              rpt.id_stg=cmbk.reportability
                                                                    WHERE           cmbk.combinedchecknumber_alfa IS NOT NULL
                                                                    AND             cmbk.claimnumber IS NOT NULL
                                                                    AND             cmbk.issuedate IS NOT NULL qualify row_number() over (PARTITION BY cmbk.combinedchecknumber_alfa ORDER BY cmbk.createtime DESC) =1
                                                                    UNION
                                                                    SELECT DISTINCT cmbk.combinedchecknumber_alfa AS publicid,
                                                                                    cmbk.issuedate,
                                                                                    cast(''DOC_TYPE6'' AS      VARCHAR(50)) AS doc_type,
                                                                                    cast(''DOC_CTGY_TYPE1'' AS VARCHAR(50)) AS doc_category,
                                                                                    decode(ccr.typecode_stg,
                                                                                           ''reportable'',''1'',
                                                                                           ''notreportable'',''0'') AS typecode,
                                                                                    cmbk.createtime,
                                                                                    cmbk.retired                  AS retired,
                                                                                    cmbk.combinedchecknumber_alfa AS chknum,
                                                                                    cmbk.updatetime ,
                                                                                    cmbk.insurpay_tracknum ,
                                                                                    cmbk.insurpay_paymt_mtd ,
                                                                                    CASE
                                                                                                    WHEN cmbk.irsupdateind_alfa = 0 THEN ''F''
                                                                                                    WHEN cmbk.irsupdateind_alfa = 1 THEN ''T''
                                                                                    END AS irsupdateind_alfa
                                                                    FROM            (
                                                                                                    SELECT DISTINCT ck.voiddate_alfa_stg            AS issuedate,
                                                                                                                    ck.voiddate_alfa_stg            AS updatetime,
                                                                                                                    ck.retired_stg                  AS retired,
                                                                                                                    ck.createtime_stg               AS createtime,
                                                                                                                    ck.combinedchecknumber_alfa_stg AS combinedchecknumber_alfa,
                                                                                                                    insurpaytrackingnum_ext_stg     AS insurpay_tracknum,
                                                                                                                    ext.typecode_stg                AS insurpay_paymt_mtd,
                                                                                                                    ck.irsupdateind_alfa_stg        AS irsupdateind_alfa,
                                                                                                                    ck.reportability_stg            AS reportability,
                                                                                                                    ck.status_stg                   AS status,
                                                                                                                    clm.claimnumber_stg             AS claimnumber,
                                                                                                                    ck.issuedate_stg
                                                                                                    FROM            (
                                                                                                                               SELECT     clm.*
                                                                                                                               FROM       db_t_prod_stag.cc_claim clm
                                                                                                                               inner join db_t_prod_stag.cctl_claimstate ccs
                                                                                                                               ON         clm.state_stg= ccs.id_stg
                                                                                                                               WHERE      ccs.name_stg <> ''Draft'') clm
                                                                                                    join            db_t_prod_stag.cc_check ck
                                                                                                    ON              clm.id_stg = ck.claimid_stg
                                                                                                    join            db_t_prod_stag.cc_transaction ctn
                                                                                                    ON              ck.id_stg =ctn.checkid_stg
                                                                                                    join            db_t_prod_stag.cc_transactionlineitem ctl
                                                                                                    ON              ctl.transactionid_stg = ctn.id_stg
                                                                                                    join            db_t_prod_stag.cctl_transactionstatus cts
                                                                                                    ON              ck.status_stg = cts.id_stg
                                                                                                    join            db_t_prod_stag.cctl_paymentmethod ccp
                                                                                                    ON              ck.paymentmethod_stg = ccp.id_stg
                                                                                                    left join       db_t_prod_stag.cctl_insurpaymethod_ext ext
                                                                                                    ON              ext.id_stg=ck.insurpaymethod_ext_stg
                                                                                                    WHERE           ck.updatetime_stg>($start_dttm)
                                                                                                    AND             ck.updatetime_stg <= ($end_dttm)
                                                                                                    AND             ccp.typecode_stg <> ''expenseWithheld_alfa'' ) cmbk
                                                                    join            db_t_prod_stag.cctl_transactionstatus ccts
                                                                    ON              cmbk.status=ccts.id_stg
                                                                    left outer join db_t_prod_stag.cctl_reportabilitytype ccr
                                                                    ON              ccr.id_stg=cmbk.reportability
                                                                    WHERE           cmbk.combinedchecknumber_alfa IS NOT NULL
                                                                    AND             cmbk.claimnumber IS NOT NULL
                                                                    AND             cmbk.issuedate IS NOT NULL
                                                                    AND             cmbk.issuedate_stg IS NOT NULL qualify row_number() over (PARTITION BY cmbk.combinedchecknumber_alfa ORDER BY cmbk.createtime DESC) =1) a ) src ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
         SELECT sq_cc_check.publicid           AS publicid,
                NULL                           AS lastname,
                NULL                           AS middlename,
                NULL                           AS firstname,
                sq_cc_check.issuedate          AS issuedate,
                sq_cc_check.doc_type           AS doc_type,
                sq_cc_check.doc_category       AS doc_category,
                sq_cc_check.typecode           AS typecode,
                sq_cc_check.createtime         AS createtime,
                sq_cc_check.retired            AS retired,
                sq_cc_check.checknumber        AS checknumber,
                sq_cc_check.updatetime         AS updatetime,
                sq_cc_check.insurpay_tracknum  AS insurpay_tracknum,
                sq_cc_check.insurpay_paymt_mtd AS insurpay_paymt_mtd,
                sq_cc_check.irsupdateind_alfa  AS irsupdateind_alfa,
                sq_cc_check.rank               AS rank,
                sq_cc_check.source_record_id
         FROM   sq_cc_check );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
            SELECT    exp_pass_from_source.publicid AS publicid,
                      1                             AS out_cntrl_id,
                      $prcs_id                      AS out_prcs_id,
                      lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE */
                      AS o_doc_type,
                      lkp_2.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE */
                                                                                                  AS o_doc_category,
                      exp_pass_from_source.issuedate                                              AS issuedate,
                      exp_pass_from_source.typecode                                               AS typecode_out,
                      exp_pass_from_source.createtime                                             AS createtime,
                      to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )      AS out_check_image_end_dttm,
                      exp_pass_from_source.retired                                                AS retired,
                      dateadd(''second'', ( 2 * ( exp_pass_from_source.rank - 1 ) ), current_timestamp) AS edw_strt_dttm,
                      to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )      AS edw_end_dttm,
                      exp_pass_from_source.checknumber                                            AS checknumber,
                      exp_pass_from_source.updatetime                                             AS updatetime,
                      exp_pass_from_source.insurpay_tracknum                                      AS insurpay_tracknum,
                      lkp_3.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PMT_TYPE */
                                                             AS out_insurpay_paymt_mtd,
                      exp_pass_from_source.irsupdateind_alfa AS irsupdateind_alfa,
                      exp_pass_from_source.source_record_id,
                      row_number() over (PARTITION BY exp_pass_from_source.source_record_id ORDER BY exp_pass_from_source.source_record_id) AS rnk
            FROM      exp_pass_from_source
            left join lkp_teradata_etl_ref_xlat_doc_type lkp_1
            ON        lkp_1.src_idntftn_val = exp_pass_from_source.doc_type
            left join lkp_teradata_etl_ref_xlat_doc_ctgy_type lkp_2
            ON        lkp_2.src_idntftn_val = exp_pass_from_source.doc_category
            left join lkp_teradata_etl_ref_xlat_pmt_type lkp_3
            ON        lkp_3.src_idntftn_val = exp_pass_from_source.insurpay_paymt_mtd qualify rnk = 1 );
  -- Component LKP_DOC, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_doc AS
  (
            SELECT    lkp.doc_id,
                      exp_data_transformation.source_record_id,
                      row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.doc_id ASC,lkp.tm_prd_cd ASC,lkp.doc_crtn_dttm ASC,lkp.doc_recpt_dt ASC,lkp.doc_prd_strt_dttm ASC,lkp.doc_prd_end_dttm ASC,lkp.doc_issur_num ASC,lkp.data_src_type_cd ASC,lkp.doc_desc_txt ASC,lkp.doc_name ASC,lkp.doc_host_num ASC,lkp.doc_host_vers_num ASC,lkp.doc_cycl_cd ASC,lkp.doc_type_cd ASC,lkp.mm_objt_id ASC,lkp.doc_ctgy_type_cd ASC,lkp.lang_type_cd ASC,lkp.prcs_id ASC,lkp.doc_sts_cd ASC) rnk
            FROM      exp_data_transformation
            left join
                      (
                             SELECT doc_id,
                                    tm_prd_cd,
                                    doc_crtn_dttm,
                                    doc_recpt_dt,
                                    doc_prd_strt_dttm,
                                    doc_prd_end_dttm,
                                    doc_issur_num,
                                    data_src_type_cd,
                                    doc_desc_txt,
                                    doc_name,
                                    doc_host_num,
                                    doc_host_vers_num,
                                    doc_cycl_cd,
                                    doc_type_cd,
                                    mm_objt_id,
                                    doc_ctgy_type_cd,
                                    lang_type_cd,
                                    prcs_id,
                                    doc_sts_cd
                             FROM   db_t_prod_core.doc ) lkp
            ON        lkp.doc_issur_num = exp_data_transformation.publicid
            AND       lkp.doc_type_cd = exp_data_transformation.o_doc_type
            AND       lkp.doc_ctgy_type_cd = exp_data_transformation.o_doc_category 
			qualify row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.doc_id ASC,lkp.tm_prd_cd ASC,lkp.doc_crtn_dttm ASC,lkp.doc_recpt_dt ASC,lkp.doc_prd_strt_dttm ASC,lkp.doc_prd_end_dttm ASC,lkp.doc_issur_num ASC,lkp.data_src_type_cd ASC,lkp.doc_desc_txt ASC,lkp.doc_name ASC,lkp.doc_host_num ASC,lkp.doc_host_vers_num ASC,lkp.doc_cycl_cd ASC,lkp.doc_type_cd ASC,lkp.mm_objt_id ASC,lkp.doc_ctgy_type_cd ASC,lkp.lang_type_cd ASC,lkp.prcs_id ASC,lkp.doc_sts_cd ASC) 
			= 1 );
  -- Component LKP_CHK_IMAGE_CLAIM_CENTER, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_chk_image_claim_center AS
  (
            SELECT    lkp.bnk_drft_doc_id,
                      lkp.chk_ser_num,
                      lkp.chk_entrd_dttm,
                      lkp.irs_rprtbl_ind,
                      lkp.chk_image_strt_dttm,
                      lkp.edw_strt_dttm,
                      lkp.edw_end_dttm,
                      lkp.chk_extr_udk_num,
                      lkp.chk_image_pmt_meth_type_cd,
                      lkp.irs_updt_info_ind,
                      lkp_doc.doc_id AS in_bnk_drft_doc_id,
                      lkp_doc.source_record_id,
                      row_number() over(PARTITION BY lkp_doc.source_record_id ORDER BY lkp.bnk_drft_doc_id DESC,lkp.chk_ser_num DESC,lkp.chk_entrd_dttm DESC,lkp.irs_rprtbl_ind DESC,lkp.chk_image_strt_dttm DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC,lkp.chk_extr_udk_num DESC,lkp.chk_image_pmt_meth_type_cd DESC,lkp.irs_updt_info_ind DESC) rnk
            FROM      lkp_doc
            left join
                      (
                               SELECT   chk_image.chk_ser_num                  AS chk_ser_num,
                                        chk_image.chk_entrd_dttm               AS chk_entrd_dttm,
                                        rtrim(ltrim(chk_image.irs_rprtbl_ind)) AS irs_rprtbl_ind,
                                        chk_image.chk_image_strt_dttm          AS chk_image_strt_dttm,
                                        chk_image.edw_strt_dttm                AS edw_strt_dttm,
                                        chk_image.edw_end_dttm                 AS edw_end_dttm,
                                        chk_image.chk_extr_udk_num             AS chk_extr_udk_num,
                                        chk_image.chk_image_pmt_meth_type_cd   AS chk_image_pmt_meth_type_cd,
                                        chk_image.irs_updt_info_ind            AS irs_updt_info_ind,
                                        chk_image.bnk_drft_doc_id              AS bnk_drft_doc_id
                               FROM     db_t_prod_core.chk_image qualify row_number() over(PARTITION BY chk_image.bnk_drft_doc_id ORDER BY chk_image.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.bnk_drft_doc_id = lkp_doc.doc_id 
			qualify row_number() over(PARTITION BY lkp_doc.source_record_id ORDER BY lkp.bnk_drft_doc_id DESC,lkp.chk_ser_num DESC,lkp.chk_entrd_dttm DESC,lkp.irs_rprtbl_ind DESC,lkp.chk_image_strt_dttm DESC,lkp.edw_strt_dttm DESC,lkp.edw_end_dttm DESC,lkp.chk_extr_udk_num DESC,lkp.chk_image_pmt_meth_type_cd DESC,lkp.irs_updt_info_ind DESC) 
			= 1 );
  -- Component exp_check_flag, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_check_flag AS
  (
             SELECT     lkp_chk_image_claim_center.bnk_drft_doc_id            AS lkp_chk_image_bnk_drft_doc_id,
                        lkp_chk_image_claim_center.chk_ser_num                AS lkp_chk_ser_num,
                        lkp_chk_image_claim_center.chk_entrd_dttm             AS lkp_chk_entrd_dttm,
                        lkp_chk_image_claim_center.irs_rprtbl_ind             AS lkp_irs_rprtbl_ind,
                        lkp_chk_image_claim_center.edw_strt_dttm              AS lkp_edw_strt_dttm,
                        lkp_chk_image_claim_center.edw_end_dttm               AS lkp_edw_end_dttm,
                        lkp_chk_image_claim_center.chk_extr_udk_num           AS lkp_chk_extr_udk_num,
                        lkp_chk_image_claim_center.chk_image_pmt_meth_type_cd AS lkp_chk_image_pmt_meth_type_cd,
                        lkp_chk_image_claim_center.irs_updt_info_ind          AS lkp_irs_updt_info_ind,
                        lkp_chk_image_claim_center.in_bnk_drft_doc_id         AS bnk_drft_doc_id,
                        exp_data_transformation.checknumber                   AS in_chk_ser_num,
                        exp_data_transformation.out_cntrl_id                  AS cntrl_id,
                        exp_data_transformation.out_prcs_id                   AS prcs_id,
                        exp_data_transformation.issuedate                     AS in_chk_entrd_dttm,
                        exp_data_transformation.typecode_out                  AS in_irs_rprtbl_ind,
                        exp_data_transformation.createtime                    AS in_chk_image_strt_dt,
                        exp_data_transformation.updatetime                    AS updatetime,
                        exp_data_transformation.insurpay_tracknum             AS in_insurpay_tracknum,
                        exp_data_transformation.out_insurpay_paymt_mtd        AS in_insurpay_paymt_mtd,
                        exp_data_transformation.irsupdateind_alfa             AS in_irs_updt_info_ind,
                        md5 ( ltrim ( rtrim ( lkp_chk_image_claim_center.chk_ser_num ) )
                                   || ltrim ( rtrim ( lkp_chk_image_claim_center.chk_entrd_dttm ) )
                                   || ltrim ( rtrim ( lkp_chk_image_claim_center.irs_rprtbl_ind ) )
                                   || ltrim ( rtrim ( lkp_chk_image_claim_center.chk_image_strt_dttm ) )
                                   || ltrim ( rtrim ( lkp_chk_image_claim_center.chk_extr_udk_num ) )
                                   || ltrim ( rtrim ( lkp_chk_image_claim_center.chk_image_pmt_meth_type_cd ) )
                                   || ltrim ( rtrim ( lkp_chk_image_claim_center.irs_updt_info_ind ) ) ) AS var_orig_chksm,
                        md5 ( ltrim ( rtrim ( exp_data_transformation.checknumber ) )
                                   || ltrim ( rtrim ( exp_data_transformation.issuedate ) )
                                   || ltrim ( rtrim ( exp_data_transformation.typecode_out ) )
                                   || ltrim ( rtrim ( exp_data_transformation.createtime ) )
                                   || ltrim ( rtrim ( exp_data_transformation.insurpay_tracknum ) )
                                   || ltrim ( rtrim ( exp_data_transformation.out_insurpay_paymt_mtd ) )
                                   || ltrim ( rtrim ( exp_data_transformation.irsupdateind_alfa ) ) ) AS var_calc_chksm,
                        CASE
                                   WHEN var_orig_chksm IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN var_orig_chksm != var_calc_chksm THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                                   AS out_ins_upd,
                        exp_data_transformation.edw_strt_dttm AS out_edw_strt_dttm,
                        exp_data_transformation.edw_end_dttm  AS out_edw_end_dttm,
                        exp_data_transformation.retired       AS retired,
                        exp_data_transformation.source_record_id
             FROM       exp_data_transformation
             inner join lkp_chk_image_claim_center
             ON         exp_data_transformation.source_record_id = lkp_chk_image_claim_center.source_record_id );
  -- Component rtr_CHK_IMAGE_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_chk_image_insert as
  SELECT exp_check_flag.lkp_chk_image_bnk_drft_doc_id  AS lkp_chk_image_bnk_drft_doc_id,
         exp_check_flag.lkp_chk_extr_udk_num           AS lkp_chk_extr_udk_num,
         exp_check_flag.lkp_chk_ser_num                AS lkp_chk_ser_num,
         exp_check_flag.lkp_chk_entrd_dttm             AS lkp_chk_entrd_dttm,
         exp_check_flag.lkp_irs_rprtbl_ind             AS lkp_irs_rprtbl_ind,
         exp_check_flag.lkp_edw_strt_dttm              AS lkp_edw_strt_dttm,
         exp_check_flag.lkp_edw_end_dttm               AS lkp_edw_end_dttm,
         exp_check_flag.lkp_chk_image_pmt_meth_type_cd AS lkp_chk_image_pmt_meth_type_cd,
         exp_check_flag.lkp_irs_updt_info_ind          AS lkp_irs_updt_info_ind,
         exp_check_flag.bnk_drft_doc_id                AS bnk_drft_doc_id,
         exp_check_flag.in_chk_ser_num                 AS checknumber,
         exp_check_flag.cntrl_id                       AS cntrl_id,
         exp_check_flag.prcs_id                        AS prcs_id,
         exp_check_flag.in_chk_entrd_dttm              AS issuedate,
         exp_check_flag.in_irs_rprtbl_ind              AS typecode_out,
         exp_check_flag.out_ins_upd                    AS out_ins_upd,
         exp_check_flag.out_edw_strt_dttm              AS out_edw_strt_dttm,
         exp_check_flag.out_edw_end_dttm               AS out_edw_end_dttm,
         exp_check_flag.in_chk_image_strt_dt           AS in_chk_image_strt_dt,
         exp_check_flag.retired                        AS retired,
         exp_check_flag.updatetime                     AS updatetime,
         exp_check_flag.in_insurpay_tracknum           AS in_insurpay_tracknum,
         exp_check_flag.in_insurpay_paymt_mtd          AS in_insurpay_paymt_mtd,
         exp_check_flag.in_irs_updt_info_ind           AS in_irs_updt_info_ind,
         exp_check_flag.source_record_id
  FROM   exp_check_flag
  WHERE  (
                exp_check_flag.out_ins_upd = ''I''
         OR     (
                       exp_check_flag.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                AND    exp_check_flag.retired = 0 ) )
  OR     (
                exp_check_flag.out_ins_upd = ''U''
         AND    exp_check_flag.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
         AND    exp_check_flag.retired = 0 ) /* - - exp_check_flag.lkp_chk_image_bnk_drft_doc_id IS NULL*/
  AND    exp_check_flag.bnk_drft_doc_id IS NOT NULL;
  
  -- Component rtr_CHK_IMAGE_RETIRED, Type ROUTER Output Group RETIRED
  create or replace temporary table rtr_chk_image_retired as
  SELECT exp_check_flag.lkp_chk_image_bnk_drft_doc_id  AS lkp_chk_image_bnk_drft_doc_id,
         exp_check_flag.lkp_chk_extr_udk_num           AS lkp_chk_extr_udk_num,
         exp_check_flag.lkp_chk_ser_num                AS lkp_chk_ser_num,
         exp_check_flag.lkp_chk_entrd_dttm             AS lkp_chk_entrd_dttm,
         exp_check_flag.lkp_irs_rprtbl_ind             AS lkp_irs_rprtbl_ind,
         exp_check_flag.lkp_edw_strt_dttm              AS lkp_edw_strt_dttm,
         exp_check_flag.lkp_edw_end_dttm               AS lkp_edw_end_dttm,
         exp_check_flag.lkp_chk_image_pmt_meth_type_cd AS lkp_chk_image_pmt_meth_type_cd,
         exp_check_flag.lkp_irs_updt_info_ind          AS lkp_irs_updt_info_ind,
         exp_check_flag.bnk_drft_doc_id                AS bnk_drft_doc_id,
         exp_check_flag.in_chk_ser_num                 AS checknumber,
         exp_check_flag.cntrl_id                       AS cntrl_id,
         exp_check_flag.prcs_id                        AS prcs_id,
         exp_check_flag.in_chk_entrd_dttm              AS issuedate,
         exp_check_flag.in_irs_rprtbl_ind              AS typecode_out,
         exp_check_flag.out_ins_upd                    AS out_ins_upd,
         exp_check_flag.out_edw_strt_dttm              AS out_edw_strt_dttm,
         exp_check_flag.out_edw_end_dttm               AS out_edw_end_dttm,
         exp_check_flag.in_chk_image_strt_dt           AS in_chk_image_strt_dt,
         exp_check_flag.retired                        AS retired,
         exp_check_flag.updatetime                     AS updatetime,
         exp_check_flag.in_insurpay_tracknum           AS in_insurpay_tracknum,
         exp_check_flag.in_insurpay_paymt_mtd          AS in_insurpay_paymt_mtd,
         exp_check_flag.in_irs_updt_info_ind           AS in_irs_updt_info_ind,
         exp_check_flag.source_record_id
  FROM   exp_check_flag
  WHERE  exp_check_flag.out_ins_upd = ''R''
  AND    exp_check_flag.retired != 0
  AND    exp_check_flag.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_CHK_IMAGE_retired, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_chk_image_retired AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_chk_image_retired.bnk_drft_doc_id   AS bnk_drft_doc_id,
                rtr_chk_image_retired.lkp_edw_strt_dttm AS lkp_edw_strt_dttm,
                rtr_chk_image_retired.updatetime        AS updatetime4,
                1                                       AS update_strategy_action,
				rtr_chk_image_retired.source_record_id
         FROM   rtr_chk_image_retired );
  -- Component upd_CHK_IMAGE_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_chk_image_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_chk_image_insert.bnk_drft_doc_id       AS bnk_drft_doc_id,
                rtr_chk_image_insert.checknumber           AS checknumber,
                rtr_chk_image_insert.cntrl_id              AS cntrl_id,
                rtr_chk_image_insert.prcs_id               AS prcs_id,
                rtr_chk_image_insert.issuedate             AS issuedate1,
                rtr_chk_image_insert.typecode_out          AS typecode_out1,
                rtr_chk_image_insert.out_edw_strt_dttm     AS out_edw_strt_dttm1,
                rtr_chk_image_insert.out_edw_end_dttm      AS out_edw_end_dttm1,
                rtr_chk_image_insert.in_chk_image_strt_dt  AS chk_img_strt_dt1,
                rtr_chk_image_insert.retired               AS retired1,
                rtr_chk_image_insert.updatetime            AS updatetime1,
                rtr_chk_image_insert.in_insurpay_tracknum  AS in_insurpay_tracknum2,
                rtr_chk_image_insert.in_insurpay_paymt_mtd AS in_insurpay_paymt_mtd2,
                rtr_chk_image_insert.in_irs_updt_info_ind  AS in_irs_updt_info_ind1,
                0                                          AS update_strategy_action,
				rtr_chk_image_insert.source_record_id
         FROM   rtr_chk_image_insert );
  -- Component exp_CHK_IMAGE_retired, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_chk_image_retired AS
  (
         SELECT upd_chk_image_retired.bnk_drft_doc_id   AS bnk_drft_doc_id,
                upd_chk_image_retired.lkp_edw_strt_dttm AS lkp_edw_strt_dttm,
                current_timestamp                       AS o_edw_end_dttm,
                current_timestamp                       AS trans_end_dttm,
                upd_chk_image_retired.source_record_id
         FROM   upd_chk_image_retired );
  -- Component tgt_CHK_IMAGE_retired, Type TARGET
  merge
  INTO         db_t_prod_core.chk_image
  USING        exp_chk_image_retired
  ON (
                            chk_image.bnk_drft_doc_id = exp_chk_image_retired.bnk_drft_doc_id
               AND          chk_image.edw_strt_dttm = exp_chk_image_retired.lkp_edw_strt_dttm)
  WHEN matched THEN
  UPDATE
  SET    bnk_drft_doc_id = exp_chk_image_retired.bnk_drft_doc_id,
         edw_strt_dttm = exp_chk_image_retired.lkp_edw_strt_dttm,
         edw_end_dttm = exp_chk_image_retired.o_edw_end_dttm,
         trans_end_dttm = exp_chk_image_retired.trans_end_dttm;
  
  -- Component exp_pass_to_target_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ins AS
  (
         SELECT upd_chk_image_ins.bnk_drft_doc_id    AS bnk_drft_doc_id,
                upd_chk_image_ins.checknumber        AS checknumber,
                upd_chk_image_ins.prcs_id            AS prcs_id,
                upd_chk_image_ins.issuedate1         AS issuedate1,
                upd_chk_image_ins.typecode_out1      AS typecode_out1,
                upd_chk_image_ins.out_edw_strt_dttm1 AS out_edw_strt_dttm1,
                upd_chk_image_ins.chk_img_strt_dt1   AS chk_img_strt_dt1,
                CASE
                       WHEN upd_chk_image_ins.retired1 != 0 THEN upd_chk_image_ins.out_edw_strt_dttm1
                       ELSE upd_chk_image_ins.out_edw_end_dttm1
                END                                                                    AS edw_end_dttm,
                to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS chk_img_end_dttm,
                upd_chk_image_ins.updatetime1                                          AS trans_strt_dttm,
                CASE
                       WHEN upd_chk_image_ins.retired1 != 0 THEN upd_chk_image_ins.updatetime1
                       ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END                                      AS trans_end_dttm,
                upd_chk_image_ins.in_insurpay_tracknum2  AS in_insurpay_tracknum2,
                upd_chk_image_ins.in_insurpay_paymt_mtd2 AS in_insurpay_paymt_mtd2,
                upd_chk_image_ins.in_irs_updt_info_ind1  AS in_irs_updt_info_ind1,
                upd_chk_image_ins.source_record_id
         FROM   upd_chk_image_ins );
  -- Component tgt_CHK_IMAGE_ins, Type TARGET
  INSERT INTO db_t_prod_core.chk_image
              (
                          bnk_drft_doc_id,
                          chk_extr_udk_num,
                          chk_ser_num,
                          chk_entrd_dttm,
                          irs_rprtbl_ind,
                          chk_image_pmt_meth_type_cd,
                          irs_updt_info_ind,
                          prcs_id,
                          chk_image_strt_dttm,
                          chk_image_end_dttm,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_target_ins.bnk_drft_doc_id        AS bnk_drft_doc_id,
         exp_pass_to_target_ins.in_insurpay_tracknum2  AS chk_extr_udk_num,
         exp_pass_to_target_ins.checknumber            AS chk_ser_num,
         exp_pass_to_target_ins.issuedate1             AS chk_entrd_dttm,
         exp_pass_to_target_ins.typecode_out1          AS irs_rprtbl_ind,
         exp_pass_to_target_ins.in_insurpay_paymt_mtd2 AS chk_image_pmt_meth_type_cd,
         exp_pass_to_target_ins.in_irs_updt_info_ind1  AS irs_updt_info_ind,
         exp_pass_to_target_ins.prcs_id                AS prcs_id,
         exp_pass_to_target_ins.chk_img_strt_dt1       AS chk_image_strt_dttm,
         exp_pass_to_target_ins.chk_img_end_dttm       AS chk_image_end_dttm,
         exp_pass_to_target_ins.out_edw_strt_dttm1     AS edw_strt_dttm,
         exp_pass_to_target_ins.edw_end_dttm           AS edw_end_dttm,
         exp_pass_to_target_ins.trans_strt_dttm        AS trans_strt_dttm,
         exp_pass_to_target_ins.trans_end_dttm         AS trans_end_dttm
  FROM   exp_pass_to_target_ins;
  
  -- Component tgt_CHK_IMAGE_ins, Type Post SQL
  UPDATE db_t_prod_core.chk_image
    SET    trans_end_dttm = a.lead,
         edw_end_dttm = a.lead1
  FROM   (
                         SELECT DISTINCT bnk_drft_doc_id,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY bnk_drft_doc_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1,
                                         max(trans_strt_dttm) over (PARTITION BY bnk_drft_doc_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead
                         FROM            db_t_prod_core.chk_image ) a

  WHERE  chk_image.bnk_drft_doc_id = a.bnk_drft_doc_id
  AND    chk_image.edw_strt_dttm = a.edw_strt_dttm
  AND    chk_image.trans_strt_dttm <> chk_image.trans_end_dttm
  AND    a.lead IS NOT NULL ;

END;
';