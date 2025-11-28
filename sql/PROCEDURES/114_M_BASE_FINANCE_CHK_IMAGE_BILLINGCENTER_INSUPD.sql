-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FINANCE_CHK_IMAGE_BILLINGCENTER_INSUPD("PARAM_JSON" VARCHAR)
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
  -- Component sq_bc_outgoingpayment, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_bc_outgoingpayment AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS issuedate,
                $2 AS refnumber,
                $3 AS payto,
                $4 AS mailtoaddress,
                $5 AS updatetime,
                $6 AS paytoname2_alfa,
                $7 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                            SELECT    bo.issuedate,
                                                      cast(bo.id AS VARCHAR(50)) AS refnum,
                                                      bo.payto,
                                                      bo.mailtoaddress,
                                                      CASE
                                                                WHEN(
                                                                                    bo.updatetime>bd.updatetime) THEN bo.updatetime
                                                                ELSE bd.updatetime
                                                      END updatetime,
                                                      bd.paytoname2_alfa
                                            FROM      (
                                                             SELECT mailtoaddress_stg  AS mailtoaddress,
                                                                    issuedate_stg      AS issuedate,
                                                                    updatetime_stg     AS updatetime,
                                                                    id_stg             AS id,
                                                                    payto_stg          AS payto,
                                                                    disbursementid_stg AS disbursementid
                                                             FROM   db_t_prod_stag.bc_outgoingpayment
                                                                    /*EIM - 22549*/
                                                                    /*where UpdateTime_stg > ($start_dttm) and UpdateTime_stg <= ( $end_dttm)*/
                                                      ) bo
                                            left join
                                                      (
                                                             SELECT id_stg              AS id,
                                                                    paytoname2_alfa_stg AS paytoname2_alfa,
                                                                    updatetime_stg      AS updatetime
                                                             FROM   db_t_prod_stag.bc_disbursement
                                                                    /*EIM - 22549*/
                                                                    /*where UpdateTime_stg > ($start_dttm) and UpdateTime_stg <= ( $end_dttm)*/
                                                      ) bd
                                            ON        bo.disbursementid=bd.id
                                            WHERE     ((
                                                                          bd.updatetime > ($start_dttm)
                                                                AND       bd.updatetime <= ( $end_dttm))
                                                      OR        (
                                                                          bo.updatetime > ($start_dttm)
                                                                AND       bo.updatetime <= ( $end_dttm)))
                                            ORDER BY  bo.updatetime DESC ) src ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
         SELECT sq_bc_outgoingpayment.issuedate       AS issuedate,
                sq_bc_outgoingpayment.refnumber       AS refnumber,
                upper ( sq_bc_outgoingpayment.payto ) AS out_payto,
                sq_bc_outgoingpayment.mailtoaddress   AS mailtoaddress,
                sq_bc_outgoingpayment.updatetime      AS updatetime,
                sq_bc_outgoingpayment.paytoname2_alfa AS paytoname2_alfa,
                sq_bc_outgoingpayment.source_record_id
         FROM   sq_bc_outgoingpayment );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
            SELECT    exp_pass_from_source.issuedate AS issuedate,
                      exp_pass_from_source.refnumber AS refnumber,
                      exp_pass_from_source.out_payto AS payto,
                      $prcs_id                       AS out_prcs_id,
                      lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DOC_TYPE */
                      AS o_doc_type,
                      lkp_2.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DOC_CTGY_TYPE */
                                                           AS o_doc_cat,
                      exp_pass_from_source.mailtoaddress   AS mailtoaddress,
                      exp_pass_from_source.updatetime      AS updatetime,
                      exp_pass_from_source.paytoname2_alfa AS paytoname2_alfa,
                      exp_pass_from_source.source_record_id,
                      row_number() over (PARTITION BY exp_pass_from_source.source_record_id ORDER BY exp_pass_from_source.source_record_id) AS rnk
            FROM      exp_pass_from_source
            left join lkp_teradata_etl_ref_xlat_doc_type lkp_1
            ON        lkp_1.src_idntftn_val = ''DOC_TYPE1''
            left join lkp_teradata_etl_ref_xlat_doc_ctgy_type lkp_2
            ON        lkp_2.src_idntftn_val = ''DOC_CTGY_TYPE4'' 
			qualify row_number() over (PARTITION BY exp_pass_from_source.source_record_id ORDER BY exp_pass_from_source.source_record_id) 
			= 1 );
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
            ON        lkp.doc_issur_num = exp_data_transformation.refnumber
            AND       lkp.doc_type_cd = exp_data_transformation.o_doc_type
            AND       lkp.doc_ctgy_type_cd = exp_data_transformation.o_doc_cat 
			qualify row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.doc_id ASC,lkp.tm_prd_cd ASC,lkp.doc_crtn_dttm ASC,lkp.doc_recpt_dt ASC,lkp.doc_prd_strt_dttm ASC,lkp.doc_prd_end_dttm ASC,lkp.doc_issur_num ASC,lkp.data_src_type_cd ASC,lkp.doc_desc_txt ASC,lkp.doc_name ASC,lkp.doc_host_num ASC,lkp.doc_host_vers_num ASC,lkp.doc_cycl_cd ASC,lkp.doc_type_cd ASC,lkp.mm_objt_id ASC,lkp.doc_ctgy_type_cd ASC,lkp.lang_type_cd ASC,lkp.prcs_id ASC,lkp.doc_sts_cd ASC) 
			= 1 );
  -- Component LKP_CHK_IMAGE_BILLING_CENTER, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_chk_image_billing_center AS
  (
            SELECT    lkp.bnk_drft_doc_id,
                      lkp.chk_extr_udk_num,
                      lkp.chk_ser_num,
                      lkp.payee_name,
                      lkp.payee_name_2,
                      lkp.payor_addr_txt,
                      lkp.chk_entrd_dttm,
                      lkp.irs_rprtbl_ind,
                      lkp.chk_image_strt_dttm,
                      lkp.edw_strt_dttm,
                      lkp_doc.doc_id AS in_bnk_drft_doc_id,
                      lkp_doc.source_record_id,
                      row_number() over(PARTITION BY lkp_doc.source_record_id ORDER BY lkp.bnk_drft_doc_id ASC,lkp.chk_extr_udk_num ASC,lkp.chk_ser_num ASC,lkp.payee_name ASC,lkp.payee_name_2 ASC,lkp.payor_addr_txt ASC,lkp.chk_entrd_dttm ASC,lkp.irs_rprtbl_ind ASC,lkp.chk_image_strt_dttm ASC,lkp.edw_strt_dttm ASC) rnk
            FROM      lkp_doc
            left join
                      (
                               SELECT   chk_image.chk_extr_udk_num    AS chk_extr_udk_num,
                                        chk_image.chk_ser_num         AS chk_ser_num,
                                        chk_image.payee_name          AS payee_name,
                                        chk_image.payee_name_2        AS payee_name_2,
                                        chk_image.payor_addr_txt      AS payor_addr_txt,
                                        chk_image.chk_entrd_dttm      AS chk_entrd_dttm,
                                        chk_image.irs_rprtbl_ind      AS irs_rprtbl_ind,
                                        chk_image.chk_image_strt_dttm AS chk_image_strt_dttm,
                                        chk_image.edw_strt_dttm       AS edw_strt_dttm,
                                        chk_image.bnk_drft_doc_id     AS bnk_drft_doc_id
                               FROM     db_t_prod_core.chk_image qualify row_number() over( PARTITION BY bnk_drft_doc_id ORDER BY chk_image.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.bnk_drft_doc_id = lkp_doc.doc_id 
			qualify row_number() over(PARTITION BY lkp_doc.source_record_id ORDER BY lkp.bnk_drft_doc_id ASC,lkp.chk_extr_udk_num ASC,lkp.chk_ser_num ASC,lkp.payee_name ASC,lkp.payee_name_2 ASC,lkp.payor_addr_txt ASC,lkp.chk_entrd_dttm ASC,lkp.irs_rprtbl_ind ASC,lkp.chk_image_strt_dttm ASC,lkp.edw_strt_dttm ASC) 
			= 1 );
  -- Component exp_check_flag, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_check_flag AS
  (
             SELECT     lkp_chk_image_billing_center.bnk_drft_doc_id    AS lkp_chk_image_bnk_drft_doc_id,
                        lkp_chk_image_billing_center.chk_ser_num        AS lkp_chk_ser_num,
                        lkp_chk_image_billing_center.payee_name         AS lkp_payee_name,
                        lkp_chk_image_billing_center.payee_name_2       AS lkp_payee_name_2,
                        lkp_chk_image_billing_center.payor_addr_txt     AS lkp_payor_addr_txt,
                        lkp_chk_image_billing_center.chk_entrd_dttm     AS lkp_chk_entrd_dttm,
                        lkp_chk_image_billing_center.irs_rprtbl_ind     AS lkp_irs_rprtbl_ind,
                        lkp_chk_image_billing_center.edw_strt_dttm      AS lkp_edw_strt_dttm,
                        lkp_chk_image_billing_center.in_bnk_drft_doc_id AS bnk_drft_doc_id,
                        exp_data_transformation.issuedate               AS issuedate,
                        exp_data_transformation.payto                   AS payto,
                        exp_data_transformation.paytoname2_alfa         AS paytoname2_alfa,
                        md5 ( lkp_chk_image_billing_center.payee_name
                                   || lkp_chk_image_billing_center.payor_addr_txt
                                   || to_char ( lkp_chk_image_billing_center.chk_entrd_dttm )
                                   || lkp_chk_image_billing_center.payee_name_2 ) AS var_orig_chksm,
                        to_date ( ''01/01/1900'' , ''mm/dd/yyyy'' )                   AS chk_image_strt_dt,
                        md5 ( exp_data_transformation.payto
                                   || exp_data_transformation.mailtoaddress
                                   || to_char ( exp_data_transformation.issuedate )
                                   || exp_data_transformation.paytoname2_alfa ) AS var_calc_chksm,
                        CASE
                                   WHEN var_orig_chksm IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN var_orig_chksm != var_calc_chksm THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                                                                    AS out_ins_upd,
                        current_timestamp                                                      AS out_edw_strt_dttm,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                        exp_data_transformation.updatetime                                     AS updatetime,
                        exp_data_transformation.source_record_id
             FROM       exp_data_transformation
             inner join lkp_chk_image_billing_center
             ON         exp_data_transformation.source_record_id = lkp_chk_image_billing_center.source_record_id );
  -- Component rtr_CHK_IMAGE_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_chk_image_insert as
  SELECT    exp_check_flag.lkp_chk_image_bnk_drft_doc_id AS lkp_chk_image_bnk_drft_doc_id,
            exp_check_flag.lkp_chk_ser_num               AS lkp_chk_ser_num,
            exp_check_flag.lkp_payee_name                AS lkp_payee_name,
            exp_check_flag.lkp_payee_name_2              AS lkp_payee_name_2,
            exp_check_flag.lkp_payor_addr_txt            AS lkp_payor_addr_txt,
            exp_check_flag.lkp_chk_entrd_dttm            AS lkp_chk_entrd_dttm,
            exp_check_flag.lkp_irs_rprtbl_ind            AS lkp_irs_rprtbl_ind,
            exp_check_flag.lkp_edw_strt_dttm             AS lkp_edw_strt_dttm,
            exp_check_flag.bnk_drft_doc_id               AS bnk_drft_doc_id,
            exp_data_transformation.issuedate            AS issuedate,
            exp_data_transformation.payto                AS payto,
            exp_data_transformation.mailtoaddress        AS mailtoaddress,
            exp_check_flag.paytoname2_alfa               AS paytoname2_alfa,
            exp_data_transformation.out_prcs_id          AS prcs_id,
            exp_check_flag.out_ins_upd                   AS out_ins_upd,
            exp_check_flag.out_edw_strt_dttm             AS out_edw_strt_dttm,
            exp_check_flag.out_edw_end_dttm              AS out_edw_end_dttm,
            exp_check_flag.chk_image_strt_dt             AS chk_image_strt_dt,
            exp_check_flag.updatetime                    AS updatetime,
            exp_data_transformation.source_record_id
  FROM      exp_data_transformation
  left join exp_check_flag
  ON        exp_data_transformation.source_record_id = exp_check_flag.source_record_id
  WHERE     exp_check_flag.out_ins_upd = ''I'' /* - - exp_check_flag.lkp_chk_image_bnk_drft_doc_id IS NULL */
  AND       exp_check_flag.bnk_drft_doc_id IS NOT NULL;
  
  -- Component rtr_CHK_IMAGE_UPDATE, Type ROUTER Output Group UPDATE
  create or replace temporary table rtr_chk_image_update as
  SELECT    exp_check_flag.lkp_chk_image_bnk_drft_doc_id AS lkp_chk_image_bnk_drft_doc_id,
            exp_check_flag.lkp_chk_ser_num               AS lkp_chk_ser_num,
            exp_check_flag.lkp_payee_name                AS lkp_payee_name,
            exp_check_flag.lkp_payee_name_2              AS lkp_payee_name_2,
            exp_check_flag.lkp_payor_addr_txt            AS lkp_payor_addr_txt,
            exp_check_flag.lkp_chk_entrd_dttm            AS lkp_chk_entrd_dttm,
            exp_check_flag.lkp_irs_rprtbl_ind            AS lkp_irs_rprtbl_ind,
            exp_check_flag.lkp_edw_strt_dttm             AS lkp_edw_strt_dttm,
            exp_check_flag.bnk_drft_doc_id               AS bnk_drft_doc_id,
            exp_data_transformation.issuedate            AS issuedate,
            exp_data_transformation.payto                AS payto,
            exp_data_transformation.mailtoaddress        AS mailtoaddress,
            exp_check_flag.paytoname2_alfa               AS paytoname2_alfa,
            exp_data_transformation.out_prcs_id          AS prcs_id,
            exp_check_flag.out_ins_upd                   AS out_ins_upd,
            exp_check_flag.out_edw_strt_dttm             AS out_edw_strt_dttm,
            exp_check_flag.out_edw_end_dttm              AS out_edw_end_dttm,
            exp_check_flag.chk_image_strt_dt             AS chk_image_strt_dt,
            exp_check_flag.updatetime                    AS updatetime,
            exp_data_transformation.source_record_id
  FROM      exp_data_transformation
  left join exp_check_flag
  ON        exp_data_transformation.source_record_id = exp_check_flag.source_record_id
  WHERE     exp_check_flag.out_ins_upd = ''U'' /* - - exp_check_flag.lkp_chk_image_bnk_drft_doc_id IS NOT NULL */
  ;
  
  -- Component upd_CHK_IMAGE_ins_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_chk_image_ins_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_chk_image_update.lkp_chk_image_bnk_drft_doc_id AS bnk_drft_doc_id,
                rtr_chk_image_update.issuedate                     AS issuedate,
                rtr_chk_image_update.payto                         AS payto,
                rtr_chk_image_update.mailtoaddress                 AS mailtoaddress3,
                rtr_chk_image_update.paytoname2_alfa               AS paytoname2_alfa3,
                rtr_chk_image_update.prcs_id                       AS prcs_id,
                rtr_chk_image_update.out_edw_strt_dttm             AS out_edw_strt_dttm3,
                rtr_chk_image_update.out_edw_end_dttm              AS out_edw_end_dttm3,
                rtr_chk_image_update.chk_image_strt_dt             AS chk_image_strt_dt3,
                rtr_chk_image_update.updatetime                    AS updatetime,
                0                                                  AS update_strategy_action,
				rtr_chk_image_update.source_record_id
         FROM   rtr_chk_image_update );
  -- Component upd_CHK_IMAGE_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_chk_image_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_chk_image_insert.bnk_drft_doc_id   AS bnk_drft_doc_id,
                rtr_chk_image_insert.issuedate         AS issuedate,
                rtr_chk_image_insert.payto             AS payto,
                rtr_chk_image_insert.paytoname2_alfa   AS paytoname2_alfa1,
                rtr_chk_image_insert.mailtoaddress     AS mailtoaddress1,
                rtr_chk_image_insert.prcs_id           AS prcs_id,
                rtr_chk_image_insert.out_edw_strt_dttm AS out_edw_strt_dttm1,
                rtr_chk_image_insert.out_edw_end_dttm  AS out_edw_end_dttm1,
                rtr_chk_image_insert.chk_image_strt_dt AS chk_image_strt_dt1,
                rtr_chk_image_insert.updatetime        AS updatetime1,
                0                                      AS update_strategy_action,
				rtr_chk_image_insert.source_record_id
         FROM   rtr_chk_image_insert );
  -- Component exp_pass_to_target_ins_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ins_upd AS
  (
         SELECT upd_chk_image_ins_upd.bnk_drft_doc_id    AS bnk_drft_doc_id,
                upd_chk_image_ins_upd.issuedate          AS issuedate,
                upd_chk_image_ins_upd.payto              AS payto,
                upd_chk_image_ins_upd.mailtoaddress3     AS mailtoaddress,
                upd_chk_image_ins_upd.paytoname2_alfa3   AS paytoname2_alfa31,
                upd_chk_image_ins_upd.prcs_id            AS prcs_id,
                upd_chk_image_ins_upd.out_edw_strt_dttm3 AS out_edw_strt_dttm3,
                upd_chk_image_ins_upd.out_edw_end_dttm3  AS out_edw_end_dttm3,
                upd_chk_image_ins_upd.chk_image_strt_dt3 AS chk_image_strt_dt3,
                upd_chk_image_ins_upd.updatetime         AS updatetime,
                upd_chk_image_ins_upd.source_record_id
         FROM   upd_chk_image_ins_upd );
  -- Component upd_CHK_IMAGE_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_chk_image_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_chk_image_update.lkp_chk_image_bnk_drft_doc_id AS bnk_drft_doc_id,
                rtr_chk_image_update.issuedate                     AS issuedate,
                rtr_chk_image_update.payto                         AS payto,
                rtr_chk_image_update.mailtoaddress                 AS mailtoaddress3,
                rtr_chk_image_update.paytoname2_alfa               AS paytoname2_alfa3,
                rtr_chk_image_update.prcs_id                       AS prcs_id,
                rtr_chk_image_update.lkp_edw_strt_dttm             AS lkp_edw_strt_dttm3,
                rtr_chk_image_update.out_edw_strt_dttm             AS out_edw_strt_dttm3,
                NULL                                               AS chk_image_strt_dt1,
                rtr_chk_image_update.updatetime                    AS updatetime3,
                1                                                  AS update_strategy_action,
				rtr_chk_image_update.source_record_id
         FROM   rtr_chk_image_update );
  -- Component exp_pass_to_target_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_upd AS
  (
         SELECT upd_chk_image_upd.bnk_drft_doc_id                        AS bnk_drft_doc_id,
                upd_chk_image_upd.lkp_edw_strt_dttm3                     AS lkp_edw_strt_dttm3,
                dateadd(''second'', - 1, upd_chk_image_upd.out_edw_strt_dttm3) AS out_edw_strt_dttm31,
                dateadd(''second'', - 1, upd_chk_image_upd.updatetime3)        AS trans_dttm,
                upd_chk_image_upd.source_record_id
         FROM   upd_chk_image_upd );
  -- Component exp_pass_to_target_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ins AS
  (
         SELECT upd_chk_image_ins.bnk_drft_doc_id    AS bnk_drft_doc_id,
                upd_chk_image_ins.issuedate          AS issuedate,
                upd_chk_image_ins.payto              AS payto,
                upd_chk_image_ins.paytoname2_alfa1   AS paytoname2_alfa1,
                upd_chk_image_ins.mailtoaddress1     AS mailtoaddress1,
                upd_chk_image_ins.prcs_id            AS prcs_id,
                upd_chk_image_ins.out_edw_strt_dttm1 AS out_edw_strt_dttm1,
                upd_chk_image_ins.out_edw_end_dttm1  AS out_edw_end_dttm1,
                upd_chk_image_ins.chk_image_strt_dt1 AS chk_image_strt_dt1,
                upd_chk_image_ins.updatetime1        AS updatetime1,
                upd_chk_image_ins.source_record_id
         FROM   upd_chk_image_ins );
  -- Component tgt_CHK_IMAGE_ins_upd, Type TARGET
  INSERT INTO db_t_prod_core.chk_image
              (
                          bnk_drft_doc_id,
                          payee_name,
                          payee_name_2,
                          payor_addr_txt,
                          chk_entrd_dttm,
                          prcs_id,
                          chk_image_strt_dttm,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT exp_pass_to_target_ins_upd.bnk_drft_doc_id    AS bnk_drft_doc_id,
         exp_pass_to_target_ins_upd.payto              AS payee_name,
         exp_pass_to_target_ins_upd.paytoname2_alfa31  AS payee_name_2,
         exp_pass_to_target_ins_upd.mailtoaddress      AS payor_addr_txt,
         exp_pass_to_target_ins_upd.issuedate          AS chk_entrd_dttm,
         exp_pass_to_target_ins_upd.prcs_id            AS prcs_id,
         exp_pass_to_target_ins_upd.chk_image_strt_dt3 AS chk_image_strt_dttm,
         exp_pass_to_target_ins_upd.out_edw_strt_dttm3 AS edw_strt_dttm,
         exp_pass_to_target_ins_upd.out_edw_end_dttm3  AS edw_end_dttm,
         exp_pass_to_target_ins_upd.updatetime         AS trans_strt_dttm
  FROM   exp_pass_to_target_ins_upd;
  
  -- Component tgt_CHK_IMAGE_upd, Type TARGET
  merge
  INTO         db_t_prod_core.chk_image
  USING        exp_pass_to_target_upd
  ON (
                            chk_image.bnk_drft_doc_id = exp_pass_to_target_upd.bnk_drft_doc_id
               AND          chk_image.edw_strt_dttm = exp_pass_to_target_upd.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    bnk_drft_doc_id = exp_pass_to_target_upd.bnk_drft_doc_id,
         edw_strt_dttm = exp_pass_to_target_upd.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_target_upd.out_edw_strt_dttm31,
         trans_end_dttm = exp_pass_to_target_upd.trans_dttm;
  
  -- Component tgt_CHK_IMAGE_ins, Type TARGET
  INSERT INTO db_t_prod_core.chk_image
              (
                          bnk_drft_doc_id,
                          payee_name,
                          payee_name_2,
                          payor_addr_txt,
                          chk_entrd_dttm,
                          prcs_id,
                          chk_image_strt_dttm,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT exp_pass_to_target_ins.bnk_drft_doc_id    AS bnk_drft_doc_id,
         exp_pass_to_target_ins.payto              AS payee_name,
         exp_pass_to_target_ins.paytoname2_alfa1   AS payee_name_2,
         exp_pass_to_target_ins.mailtoaddress1     AS payor_addr_txt,
         exp_pass_to_target_ins.issuedate          AS chk_entrd_dttm,
         exp_pass_to_target_ins.prcs_id            AS prcs_id,
         exp_pass_to_target_ins.chk_image_strt_dt1 AS chk_image_strt_dttm,
         exp_pass_to_target_ins.out_edw_strt_dttm1 AS edw_strt_dttm,
         exp_pass_to_target_ins.out_edw_end_dttm1  AS edw_end_dttm,
         exp_pass_to_target_ins.updatetime1        AS trans_strt_dttm
  FROM   exp_pass_to_target_ins;

END;
';