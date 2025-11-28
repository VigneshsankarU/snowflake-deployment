-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FINANCE_INVOICE_AMT_BILLINGCENTER_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- PIPELINE START FOR 1
  -- Component sq_bc_invoice, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_bc_invoice AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS lkp_invc_id,
                $2  AS lkp_invc_amt_type_cd,
                $3  AS lkp_invc_amt_dttm,
                $4  AS lkp_invc_amt_tsactn_amt,
                $5  AS lkp_edw_strt_dttm,
                $6  AS lkp_edw_end_dttm,
                $7  AS in_doc_id,
                $8  AS in_invc_amt_trans_amt,
                $9  AS in_invc_amt_dttm,
                $10 AS in_invc_amt_type_cd,
                $13 AS calc_ins_upd,
                $11 AS source_data,
                $12 AS target_data,
                $14 AS retired,
                $15 AS rnk,
                $16 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT a.invc_id                          AS lkp_invc_id,
                                                a.invc_amt_type_cd                 AS lkp_invc_amt_type_cd,
                                                cast(a.invc_amt_dttm AS timestamp) AS lkp_invc_amt_dttm,
                                                a.invc_amt_trans_amt               AS lkp_invc_amt_tsactn_amt,
                                                cast(a.edw_strt_dttm AS timestamp) AS lkp_edw_strt_dttm,
                                                cast(a.edw_end_dttm AS timestamp)  AS lkp_edw_end_dttm,
                                                a.doc_id                           AS in_doc_id,
                                                cast(a.amt AS DECIMAL (18,4))      AS in_invc_amt_trans_amt,
                                                cast(a.dt AS timestamp)            AS in_invc_amt_dttm,
                                                a.amttype                          AS in_invc_amt_type_cd,
                                                cast(trim(cast(lkp_invc_amt_dttm AS           VARCHAR(100)))
                                                       ||trim(cast(lkp_invc_amt_tsactn_amt AS VARCHAR(100)))AS VARCHAR(1000)) AS source_data,
                                                cast(trim(cast(dt AS                          VARCHAR(100)))
                                                       ||trim(cast(cast(a.amt AS DECIMAL (18,4)) AS VARCHAR(100)))AS VARCHAR(1000)) AS target_data,
                                                CASE
                                                       WHEN source_data IS NULL THEN ''I''
                                                       WHEN source_data <> target_data THEN ''U''
                                                       WHEN source_data = target_data THEN ''R''
                                                END AS calc_ins_upd,													   
                                                a.retired,
                                                a.rnk
                                         FROM  (
                                                          SELECT    *
                                                          FROM      (
                                                                              SELECT    invoicenumber,
                                                                                        amt ,
                                                                                        dt,
																						CASE
                                                                                                  WHEN aa.amttype =''INVC_AMT_TYPE0'' THEN ''INVC_AMT_TYPE10''
                                                                                                  ELSE aa.amttype
                                                                                        END AS var_id,
                                                                                        CASE
                                                                                                  WHEN var_id IS NULL THEN ''UNK''
                                                                                                  ELSE lkp_xlat.tgt_idntftn_val
                                                                                        END AS amttype,
                                                                                        lkp_xlat.src_idntftn_val,
                                                                                        lkp_xlat.tgt_idntftn_val,
                                                                                        retired,
                                                                                        rnk
                                                                              FROM     (
                                                                                                 SELECT   invoicenumber,
                                                                                                          amt,
                                                                                                          dt,
                                                                                                          amttype,
                                                                                                          retired ,
                                                                                                          rank() over(PARTITION BY invoicenumber,amttype ORDER BY dt ) AS rnk
                                                                                                 FROM    (
                                                                                                                          SELECT DISTINCT invoicenumber_stg                 AS invoicenumber,
                                                                                                                                          amountdue_stg                     AS amt,
                                                                                                                                          cast(updatetime_stg AS timestamp) AS dt,
                                                                                                                                          ''INVC_AMT_TYPE10''                 AS amttype,
                                                                                                                                          retired_stg                       AS retired
                                                                                                                          FROM            db_t_prod_stag.bc_invoice
                                                                                                                          WHERE           amountdue_stg IS NOT NULL
                                                                                                                          AND             bc_invoice.updatetime_stg > ($start_dttm)
                                                                                                                          AND             bc_invoice.updatetime_stg <= ($end_dttm)
                                                                                                                          UNION
                                                                                                                          SELECT DISTINCT invoicenumber_stg,
                                                                                                                                          amount_stg                        AS amt,
                                                                                                                                          cast(updatetime_stg AS timestamp) AS dt,
                                                                                                                                          ''INVC_AMT_TYPE1''                  AS amttype,
                                                                                                                                          retired_stg                       AS retired
                                                                                                                          FROM            db_t_prod_stag.bc_invoice
                                                                                                                          WHERE           amount_stg IS NOT NULL
                                                                                                                          AND             bc_invoice.updatetime_stg > ($start_dttm)
                                                                                                                          AND             bc_invoice.updatetime_stg <= ($end_dttm)
                                                                                                                          UNION
                                                                                                                          SELECT DISTINCT invoicenumber_stg,
                                                                                                                                          outstandingamount_stg             AS amt,
                                                                                                                                          cast(updatetime_stg AS timestamp) AS dt,
                                                                                                                                          ''INVC_AMT_TYPE4''                  AS amttype,
                                                                                                                                          retired_stg                       AS retired
                                                                                                                          FROM            db_t_prod_stag.bc_invoice
                                                                                                                          WHERE           outstandingamount_stg IS NOT NULL
                                                                                                                          AND             bc_invoice.updatetime_stg > ($start_dttm)
                                                                                                                          AND             bc_invoice.updatetime_stg <= ($end_dttm)
                                                                                                                          UNION
                                                                                                                          SELECT DISTINCT invoicenumber_stg,
                                                                                                                                          amount_stg                        AS amt,
                                                                                                                                          cast(updatetime_stg AS timestamp) AS dt,
                                                                                                                                          ''INVC_AMT_TYPE9''                  AS amttype,
                                                                                                                                          bc_invoice.retired_stg            AS retired
                                                                                                                          FROM            db_t_prod_stag.bc_invoice
                                                                                                                          inner join      db_t_prod_stag.bctl_invoicestatus
                                                                                                                          ON              bc_invoice.status_stg=bctl_invoicestatus.id_stg
                                                                                                                          WHERE           amount_stg IS NOT NULL
                                                                                                                          AND             bctl_invoicestatus.typecode_stg=''billed''
                                                                                                                          AND             bc_invoice.updatetime_stg > ($start_dttm)
                                                                                                                          AND             bc_invoice.updatetime_stg <= ($end_dttm)
                                                                                                                          UNION
                                                                                                                          SELECT DISTINCT invoicenumber_stg,
                                                                                                                                          amount_stg                        AS amt,
                                                                                                                                          cast(updatetime_stg AS timestamp) AS dt,
                                                                                                                                          ''INVC_AMT_TYPE8''                  AS amttype,
                                                                                                                                          bc_invoice.retired_stg            AS retired
                                                                                                                          FROM            db_t_prod_stag.bc_invoice
                                                                                                                          inner join      db_t_prod_stag.bctl_invoicestatus
                                                                                                                          ON              bc_invoice.status_stg=bctl_invoicestatus.id_stg
                                                                                                                          WHERE           amount_stg IS NOT NULL
                                                                                                                          AND             bctl_invoicestatus.typecode_stg=''planned''
                                                                                                                          AND             bc_invoice.updatetime_stg > ($start_dttm)
                                                                                                                          AND             bc_invoice.updatetime_stg <= ($end_dttm)
                                                                                                                          UNION
                                                                                                                          SELECT DISTINCT invoicenumber_stg,
                                                                                                                                          SUM(amount_stg)                   AS amt,
                                                                                                                                          cast(updatetime_stg AS timestamp) AS dt,
                                                                                                                                          ''INVC_AMT_TYPE5''                  AS amttype,
                                                                                                                                          retired_stg                       AS retired
                                                                                                                          FROM            db_t_prod_stag.bc_invoice
                                                                                                                          WHERE           amount_stg IS NOT NULL
                                                                                                                          AND             bc_invoice.updatetime_stg > ($start_dttm)
                                                                                                                          AND             bc_invoice.updatetime_stg <= ($end_dttm)
                                                                                                                          GROUP BY        invoicenumber_stg,
                                                                                                                                          updatetime_stg,
                                                                                                                                          retired_stg ) x)aa
                                                                              left join
                                                                                        (
                                                                                               SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                      teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                               FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                                               WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''INVC_AMT_TYPE''
                                                                                               AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                                               AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                                               AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'')lkp_xlat
                                                                              ON        aa.amttype=lkp_xlat.src_idntftn_val ) src_qry
                                                          left join
                                                                    (
                                                                             SELECT   doc.doc_id            AS doc_id,
                                                                                      doc.tm_prd_cd         AS tm_prd_cd,
                                                                                      doc.doc_crtn_dttm     AS doc_crtn_dttm,
                                                                                      doc.doc_recpt_dt      AS doc_recpt_dt,
                                                                                      doc.doc_prd_strt_dttm AS doc_prd_strt_dttm,
                                                                                      doc.doc_prd_end_dttm  AS doc_prd_end_dttm,
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
                                                                             FROM     db_t_prod_core.doc qualify row_number() over( PARTITION BY doc.doc_issur_num,doc.doc_type_cd,doc.doc_ctgy_type_cd ORDER BY doc.edw_end_dttm DESC) = 1 )lkp_doc
                                                          ON        lkp_doc.doc_issur_num=src_qry.invoicenumber
                                                          AND       lkp_doc.doc_type_cd=''INVOICE''
                                                          AND       lkp_doc.doc_ctgy_type_cd=''BILL''
                                                          left join
                                                                    (
                                                                             SELECT   invc_amt.invc_amt_dttm      AS invc_amt_dttm,
                                                                                      invc_amt.invc_amt_trans_amt AS invc_amt_trans_amt,
                                                                                      invc_amt.edw_strt_dttm      AS edw_strt_dttm,
                                                                                      invc_amt.edw_end_dttm       AS edw_end_dttm,
                                                                                      invc_amt.invc_id            AS invc_id,
                                                                                      invc_amt.invc_amt_type_cd   AS invc_amt_type_cd
                                                                             FROM     db_t_prod_core.invc_amt qualify row_number() over ( PARTITION BY invc_amt.invc_id,invc_amt.invc_amt_type_cd ORDER BY invc_amt.edw_end_dttm DESC)=1)tgt_lkp_invc_amt
                                                          ON        lkp_doc.doc_id=tgt_lkp_invc_amt.invc_id
                                                          AND       src_qry.amttype=tgt_lkp_invc_amt.invc_amt_type_cd )a ) src ) );
  -- Component exp_src_pass, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_src_pass AS
  (
         SELECT sq_bc_invoice.lkp_invc_id                                              AS lkp_invc_id,
                sq_bc_invoice.lkp_invc_amt_type_cd                                     AS lkp_invc_amt_type_cd,
                sq_bc_invoice.lkp_invc_amt_dttm                                        AS lkp_invc_amt_dttm,
                sq_bc_invoice.lkp_edw_strt_dttm                                        AS lkp_edw_strt_dttm,
                sq_bc_invoice.lkp_edw_end_dttm                                         AS lkp_edw_end_dttm,
                sq_bc_invoice.in_doc_id                                                AS in_doc_id,
                sq_bc_invoice.in_invc_amt_trans_amt                                    AS in_invc_amt_trans_amt,
                sq_bc_invoice.in_invc_amt_dttm                                         AS in_invc_amt_dttm,
                sq_bc_invoice.in_invc_amt_type_cd                                      AS in_invc_amt_type_cd,
                $prcs_id                                                               AS in_prcs_id,
                current_timestamp                                                      AS in_edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS in_edw_end_dttm,
                sq_bc_invoice.calc_ins_upd                                             AS calc_ins_upd,
                sq_bc_invoice.retired                                                  AS retired,
                sq_bc_invoice.rnk                                                      AS rnk,
                sq_bc_invoice.source_record_id
         FROM   sq_bc_invoice );
  -- Component RTR_Insert_Update_Grp_Insert, Type ROUTER Output Group Grp_Insert
  create or replace temporary table rtr_insert_update_grp_insert as
  SELECT exp_src_pass.lkp_invc_id           AS lkp_invc_id,
         exp_src_pass.lkp_invc_amt_type_cd  AS lkp_invc_amt_type_cd,
         exp_src_pass.lkp_edw_strt_dttm     AS lkp_edw_strt_dttm,
         exp_src_pass.in_doc_id             AS in_doc_id,
         exp_src_pass.in_invc_amt_trans_amt AS in_invc_amt_trans_amt,
         exp_src_pass.in_invc_amt_dttm      AS in_invc_amt_dttm,
         exp_src_pass.in_invc_amt_type_cd   AS in_invc_amt_type_cd,
         exp_src_pass.in_prcs_id            AS in_prcs_id,
         exp_src_pass.in_edw_strt_dttm      AS in_edw_strt_dttm,
         exp_src_pass.in_edw_end_dttm       AS in_edw_end_dttm,
         exp_src_pass.calc_ins_upd          AS calc_ins_upd,
         exp_src_pass.lkp_edw_end_dttm      AS lkp_edw_end_dttm,
         exp_src_pass.retired               AS retired,
         exp_src_pass.lkp_invc_amt_dttm     AS lkp_invc_amt_dttm,
         exp_src_pass.rnk                   AS rank,
         exp_src_pass.source_record_id
  FROM   exp_src_pass
  WHERE  exp_src_pass.in_doc_id IS NOT NULL
  AND    (
                exp_src_pass.calc_ins_upd = ''I''
         OR     exp_src_pass.calc_ins_upd = ''U''
         OR     (
                       exp_src_pass.retired = 0
                AND    exp_src_pass.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) );
  
  -- Component RTR_Insert_Update_Grp_Retired, Type ROUTER Output Group Grp_Retired
  create or replace temporary table rtr_insert_update_grp_retired as
  SELECT exp_src_pass.lkp_invc_id           AS lkp_invc_id,
         exp_src_pass.lkp_invc_amt_type_cd  AS lkp_invc_amt_type_cd,
         exp_src_pass.lkp_edw_strt_dttm     AS lkp_edw_strt_dttm,
         exp_src_pass.in_doc_id             AS in_doc_id,
         exp_src_pass.in_invc_amt_trans_amt AS in_invc_amt_trans_amt,
         exp_src_pass.in_invc_amt_dttm      AS in_invc_amt_dttm,
         exp_src_pass.in_invc_amt_type_cd   AS in_invc_amt_type_cd,
         exp_src_pass.in_prcs_id            AS in_prcs_id,
         exp_src_pass.in_edw_strt_dttm      AS in_edw_strt_dttm,
         exp_src_pass.in_edw_end_dttm       AS in_edw_end_dttm,
         exp_src_pass.calc_ins_upd          AS calc_ins_upd,
         exp_src_pass.lkp_edw_end_dttm      AS lkp_edw_end_dttm,
         exp_src_pass.retired               AS retired,
         exp_src_pass.lkp_invc_amt_dttm     AS lkp_invc_amt_dttm,
         exp_src_pass.rnk                   AS rank,
         exp_src_pass.source_record_id
  FROM   exp_src_pass
  WHERE  exp_src_pass.calc_ins_upd = ''R''
  AND    exp_src_pass.retired != 0
  AND    exp_src_pass.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_inv_amt_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_inv_amt_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insert_update_grp_insert.in_doc_id             AS in_doc_id,
                rtr_insert_update_grp_insert.in_invc_amt_trans_amt AS in_invc_amt_trans_amt,
                rtr_insert_update_grp_insert.in_invc_amt_dttm      AS in_invc_amt_dttm,
                rtr_insert_update_grp_insert.in_invc_amt_type_cd   AS in_invc_amt_type_cd,
                rtr_insert_update_grp_insert.in_prcs_id            AS in_prcs_id,
                rtr_insert_update_grp_insert.in_edw_strt_dttm      AS in_edw_strt_dttm1,
                rtr_insert_update_grp_insert.in_edw_end_dttm       AS in_edw_end_dttm1,
                rtr_insert_update_grp_insert.retired               AS retired1,
                rtr_insert_update_grp_insert.rank                  AS rank1,
                0                                                  AS update_strategy_action,
				rtr_insert_update_grp_insert.source_record_id
         FROM   rtr_insert_update_grp_insert );
  -- Component upd_inv_amt_retired, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_inv_amt_retired AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insert_update_grp_retired.lkp_invc_id          AS lkp_invc_id,
                rtr_insert_update_grp_retired.lkp_invc_amt_type_cd AS lkp_invc_amt_type_cd3,
                rtr_insert_update_grp_retired.lkp_edw_strt_dttm    AS lkp_edw_strt_dttm,
                rtr_insert_update_grp_retired.in_edw_strt_dttm     AS in_edw_strt_dttm3,
                rtr_insert_update_grp_retired.in_prcs_id           AS in_prcs_id3,
                1                                                  AS update_strategy_action,
				rtr_insert_update_grp_retired.source_record_id
         FROM   rtr_insert_update_grp_retired );
  -- Component exp_ins_pass_to_target, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ins_pass_to_target AS
  (
         SELECT upd_inv_amt_ins.in_doc_id             AS in_doc_id,
                upd_inv_amt_ins.in_invc_amt_trans_amt AS in_invc_amt_trans_amt,
                upd_inv_amt_ins.in_invc_amt_dttm      AS in_invc_amt_dttm,
                upd_inv_amt_ins.in_invc_amt_type_cd   AS in_invc_amt_type_cd,
                upd_inv_amt_ins.in_prcs_id            AS in_prcs_id,
                CASE
                       WHEN upd_inv_amt_ins.retired1 = 0 THEN upd_inv_amt_ins.in_edw_end_dttm1
                       ELSE current_timestamp
                END AS in_edw_end_dttm11,
                CASE
                       WHEN upd_inv_amt_ins.retired1 = 0 THEN dateadd(''second'', ( 2 * ( upd_inv_amt_ins.rank1 - 1 ) ), current_timestamp)
                       ELSE current_timestamp
                END AS in_edw_strt_dttm1,
                upd_inv_amt_ins.source_record_id
         FROM   upd_inv_amt_ins );
  -- Component exp_retired, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_retired AS
  (
         SELECT upd_inv_amt_retired.lkp_invc_id           AS lkp_invc_id,
                upd_inv_amt_retired.lkp_invc_amt_type_cd3 AS lkp_invc_amt_type_cd3,
                upd_inv_amt_retired.lkp_edw_strt_dttm     AS lkp_edw_strt_dttm,
                current_timestamp                         AS in_edw_end_dttm,
                upd_inv_amt_retired.source_record_id
         FROM   upd_inv_amt_retired );
  -- Component tgt_inv_amt_ins, Type TARGET
  INSERT INTO db_t_prod_core.invc_amt
              (
                          invc_id,
                          invc_amt_type_cd,
                          invc_amt_dttm,
                          invc_amt_trans_amt,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_ins_pass_to_target.in_doc_id             AS invc_id,
         exp_ins_pass_to_target.in_invc_amt_type_cd   AS invc_amt_type_cd,
         exp_ins_pass_to_target.in_invc_amt_dttm      AS invc_amt_dttm,
         exp_ins_pass_to_target.in_invc_amt_trans_amt AS invc_amt_trans_amt,
         exp_ins_pass_to_target.in_prcs_id            AS prcs_id,
         exp_ins_pass_to_target.in_edw_strt_dttm1     AS edw_strt_dttm,
         exp_ins_pass_to_target.in_edw_end_dttm11     AS edw_end_dttm
  FROM   exp_ins_pass_to_target;
  
  -- Component tgt_inv_amt_retired, Type TARGET
  merge
  INTO         db_t_prod_core.invc_amt
  USING        exp_retired
  ON (
                            invc_amt.invc_id = exp_retired.lkp_invc_id
               AND          invc_amt.invc_amt_type_cd = exp_retired.lkp_invc_amt_type_cd3
               AND          invc_amt.edw_strt_dttm = exp_retired.lkp_edw_strt_dttm)
  WHEN matched THEN
  UPDATE
  SET    invc_id = exp_retired.lkp_invc_id,
         invc_amt_type_cd = exp_retired.lkp_invc_amt_type_cd3,
         edw_strt_dttm = exp_retired.lkp_edw_strt_dttm,
         edw_end_dttm = exp_retired.in_edw_end_dttm;
  
  -- PIPELINE END FOR 1
  -- PIPELINE START FOR 2
  -- Component sq_bc_invoice1, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_bc_invoice1 AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS lkp_invc_id,
                $2  AS lkp_invc_amt_type_cd,
                $3  AS lkp_invc_amt_dttm,
                $4  AS lkp_invc_amt_tsactn_amt,
                $5  AS lkp_edw_strt_dttm,
                $6  AS lkp_edw_end_dttm,
                $7  AS in_doc_id,
                $8  AS in_invc_amt_trans_amt,
                $9  AS in_invc_amt_dttm,
                $10 AS in_invc_amt_type_cd,
                $13 AS calc_ins_upd,
                $11 AS source_data,
                $12 AS target_data,
                $14 AS retired,
                $15 AS rnk,
                $16 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT a.invc_id                          AS lkp_invc_id,
                                                a.invc_amt_type_cd                 AS lkp_invc_amt_type_cd,
                                                cast(a.invc_amt_dttm AS timestamp) AS lkp_invc_amt_dttm,
                                                a.invc_amt_trans_amt               AS lkp_invc_amt_tsactn_amt,
                                                cast(a.edw_strt_dttm AS timestamp) AS lkp_edw_strt_dttm,
                                                cast(a.edw_end_dttm AS timestamp)  AS lkp_edw_end_dttm,
                                                a.doc_id                           AS in_doc_id,
                                                cast(a.amt AS DECIMAL (18,4))      AS in_invc_amt_trans_amt,
                                                cast(a.dt AS timestamp)            AS in_invc_amt_dttm,
                                                a.amttype                          AS in_invc_amt_type_cd,
                                                cast(trim(cast(lkp_invc_amt_dttm AS           VARCHAR(100)))
                                                       ||trim(cast(lkp_invc_amt_tsactn_amt AS VARCHAR(100)))AS VARCHAR(1000)) AS source_data,
                                                cast(trim(cast(dt AS                          VARCHAR(100)))
                                                       ||trim(cast(cast(a.amt AS DECIMAL (18,4)) AS VARCHAR(100)))AS VARCHAR(1000)) AS target_data,
                                                CASE
                                                       WHEN source_data IS NULL THEN ''I''
                                                       WHEN source_data <> target_data THEN ''U''
                                                       WHEN source_data = target_data THEN ''R''
                                                END AS calc_ins_upd,													   
                                                a.retired,
                                                a.rnk
                                         FROM  (
                                                          SELECT    *
                                                          FROM      (
                                                                              SELECT    invoicenumber,
                                                                                        amt ,
                                                                                        dt,
                                                                                        CASE
                                                                                                  WHEN aa.amttype =''INVC_AMT_TYPE0'' THEN ''INVC_AMT_TYPE10''
                                                                                                  ELSE aa.amttype
                                                                                        END AS var_id,																						
                                                                                        CASE
                                                                                                  WHEN var_id IS NULL THEN ''UNK''
                                                                                                  ELSE lkp_xlat.tgt_idntftn_val
                                                                                        END AS amttype,
                                                                                        lkp_xlat.src_idntftn_val,
                                                                                        lkp_xlat.tgt_idntftn_val,
                                                                                        retired,
                                                                                        rnk
                                                                              FROM     (
                                                                                                 SELECT   invoicenumber,
                                                                                                          amt,
                                                                                                          dt,
                                                                                                          amttype,
                                                                                                          retired ,
                                                                                                          rank() over(PARTITION BY invoicenumber,amttype ORDER BY dt ) AS rnk
                                                                                                 FROM    (
                                                                                                                          SELECT DISTINCT invoicenumber_stg                 AS invoicenumber,
                                                                                                                                          amountdue_stg                     AS amt,
                                                                                                                                          cast(updatetime_stg AS timestamp) AS dt,
                                                                                                                                          ''INVC_AMT_TYPE10''                 AS amttype,
                                                                                                                                          retired_stg                       AS retired
                                                                                                                          FROM            db_t_prod_stag.bc_invoice
                                                                                                                          WHERE           amountdue_stg IS NOT NULL
                                                                                                                          AND             bc_invoice.updatetime_stg > ($start_dttm)
                                                                                                                          AND             bc_invoice.updatetime_stg <= ($end_dttm)
                                                                                                                          UNION
                                                                                                                          SELECT DISTINCT invoicenumber_stg,
                                                                                                                                          amount_stg                        AS amt,
                                                                                                                                          cast(updatetime_stg AS timestamp) AS dt,
                                                                                                                                          ''INVC_AMT_TYPE1''                  AS amttype,
                                                                                                                                          retired_stg                       AS retired
                                                                                                                          FROM            db_t_prod_stag.bc_invoice
                                                                                                                          WHERE           amount_stg IS NOT NULL
                                                                                                                          AND             bc_invoice.updatetime_stg > ($start_dttm)
                                                                                                                          AND             bc_invoice.updatetime_stg <= ($end_dttm)
                                                                                                                          UNION
                                                                                                                          SELECT DISTINCT invoicenumber_stg,
                                                                                                                                          outstandingamount_stg             AS amt,
                                                                                                                                          cast(updatetime_stg AS timestamp) AS dt,
                                                                                                                                          ''INVC_AMT_TYPE4''                  AS amttype,
                                                                                                                                          retired_stg                       AS retired
                                                                                                                          FROM            db_t_prod_stag.bc_invoice
                                                                                                                          WHERE           outstandingamount_stg IS NOT NULL
                                                                                                                          AND             bc_invoice.updatetime_stg > ($start_dttm)
                                                                                                                          AND             bc_invoice.updatetime_stg <= ($end_dttm)
                                                                                                                          UNION
                                                                                                                          SELECT DISTINCT invoicenumber_stg,
                                                                                                                                          amount_stg                        AS amt,
                                                                                                                                          cast(updatetime_stg AS timestamp) AS dt,
                                                                                                                                          ''INVC_AMT_TYPE9''                  AS amttype,
                                                                                                                                          bc_invoice.retired_stg            AS retired
                                                                                                                          FROM            db_t_prod_stag.bc_invoice
                                                                                                                          inner join      db_t_prod_stag.bctl_invoicestatus
                                                                                                                          ON              bc_invoice.status_stg=bctl_invoicestatus.id_stg
                                                                                                                          WHERE           amount_stg IS NOT NULL
                                                                                                                          AND             bctl_invoicestatus.typecode_stg=''billed''
                                                                                                                          AND             bc_invoice.updatetime_stg > ($start_dttm)
                                                                                                                          AND             bc_invoice.updatetime_stg <= ($end_dttm)
                                                                                                                          UNION
                                                                                                                          SELECT DISTINCT invoicenumber_stg,
                                                                                                                                          amount_stg                        AS amt,
                                                                                                                                          cast(updatetime_stg AS timestamp) AS dt,
                                                                                                                                          ''INVC_AMT_TYPE8''                  AS amttype,
                                                                                                                                          bc_invoice.retired_stg            AS retired
                                                                                                                          FROM            db_t_prod_stag.bc_invoice
                                                                                                                          inner join      db_t_prod_stag.bctl_invoicestatus
                                                                                                                          ON              bc_invoice.status_stg=bctl_invoicestatus.id_stg
                                                                                                                          WHERE           amount_stg IS NOT NULL
                                                                                                                          AND             bctl_invoicestatus.typecode_stg=''planned''
                                                                                                                          AND             bc_invoice.updatetime_stg > ($start_dttm)
                                                                                                                          AND             bc_invoice.updatetime_stg <= ($end_dttm)
                                                                                                                          UNION
                                                                                                                          SELECT DISTINCT invoicenumber_stg,
                                                                                                                                          SUM(amount_stg)                   AS amt,
                                                                                                                                          cast(updatetime_stg AS timestamp) AS dt,
                                                                                                                                          ''INVC_AMT_TYPE5''                  AS amttype,
                                                                                                                                          retired_stg                       AS retired
                                                                                                                          FROM            db_t_prod_stag.bc_invoice
                                                                                                                          WHERE           amount_stg IS NOT NULL
                                                                                                                          AND             bc_invoice.updatetime_stg > ($start_dttm)
                                                                                                                          AND             bc_invoice.updatetime_stg <= ($end_dttm)
                                                                                                                          GROUP BY        invoicenumber_stg,
                                                                                                                                          updatetime_stg,
                                                                                                                                          retired_stg ) x)aa
                                                                              left join
                                                                                        (
                                                                                               SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                      teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                               FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                                               WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''INVC_AMT_TYPE''
                                                                                               AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                                               AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                                               AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'')lkp_xlat
                                                                              ON        aa.amttype=lkp_xlat.src_idntftn_val ) src_qry
                                                          left join
                                                                    (
                                                                             SELECT   doc.doc_id            AS doc_id,
                                                                                      doc.tm_prd_cd         AS tm_prd_cd,
                                                                                      doc.doc_crtn_dttm     AS doc_crtn_dttm,
                                                                                      doc.doc_recpt_dt      AS doc_recpt_dt,
                                                                                      doc.doc_prd_strt_dttm AS doc_prd_strt_dttm,
                                                                                      doc.doc_prd_end_dttm  AS doc_prd_end_dttm,
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
                                                                             FROM     db_t_prod_core.doc qualify row_number() over( PARTITION BY doc.doc_issur_num,doc.doc_type_cd,doc.doc_ctgy_type_cd ORDER BY doc.edw_end_dttm DESC) = 1 )lkp_doc
                                                          ON        lkp_doc.doc_issur_num=src_qry.invoicenumber
                                                          AND       lkp_doc.doc_type_cd=''INVOICE''
                                                          AND       lkp_doc.doc_ctgy_type_cd=''BILL''
                                                          left join
                                                                    (
                                                                             SELECT   invc_amt.invc_amt_dttm      AS invc_amt_dttm,
                                                                                      invc_amt.invc_amt_trans_amt AS invc_amt_trans_amt,
                                                                                      invc_amt.edw_strt_dttm      AS edw_strt_dttm,
                                                                                      invc_amt.edw_end_dttm       AS edw_end_dttm,
                                                                                      invc_amt.invc_id            AS invc_id,
                                                                                      invc_amt.invc_amt_type_cd   AS invc_amt_type_cd
                                                                             FROM     db_t_prod_core.invc_amt qualify row_number() over ( PARTITION BY invc_amt.invc_id,invc_amt.invc_amt_type_cd ORDER BY invc_amt.edw_end_dttm DESC)=1)tgt_lkp_invc_amt
                                                          ON        lkp_doc.doc_id=tgt_lkp_invc_amt.invc_id
                                                          AND       src_qry.amttype=tgt_lkp_invc_amt.invc_amt_type_cd )a
                                         WHERE  1=2 ) src ) );
  -- Component exp_src_pass1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_src_pass1 AS
  (
         SELECT sq_bc_invoice1.lkp_invc_id                                             AS lkp_invc_id,
                sq_bc_invoice1.lkp_invc_amt_type_cd                                    AS lkp_invc_amt_type_cd,
                sq_bc_invoice1.lkp_invc_amt_dttm                                       AS lkp_invc_amt_dttm,
                sq_bc_invoice1.lkp_edw_strt_dttm                                       AS lkp_edw_strt_dttm,
                sq_bc_invoice1.lkp_edw_end_dttm                                        AS lkp_edw_end_dttm,
                sq_bc_invoice1.in_doc_id                                               AS in_doc_id,
                sq_bc_invoice1.in_invc_amt_trans_amt                                   AS in_invc_amt_trans_amt,
                sq_bc_invoice1.in_invc_amt_dttm                                        AS in_invc_amt_dttm,
                sq_bc_invoice1.in_invc_amt_type_cd                                     AS in_invc_amt_type_cd,
                $prcs_id                                                               AS in_prcs_id,
                current_timestamp                                                      AS in_edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS in_edw_end_dttm,
                sq_bc_invoice1.calc_ins_upd                                            AS calc_ins_upd,
                sq_bc_invoice1.retired                                                 AS retired,
                sq_bc_invoice1.rnk                                                     AS rank,
                sq_bc_invoice1.source_record_id
         FROM   sq_bc_invoice1 );
  -- Component RTR_Insert_Update1_Grp_Insert, Type ROUTER Output Group Grp_Insert
  create or replace temporary table rtr_insert_update1_grp_insert as
  SELECT exp_src_pass1.lkp_invc_id           AS lkp_invc_id,
         exp_src_pass1.lkp_invc_amt_type_cd  AS lkp_invc_amt_type_cd,
         exp_src_pass1.lkp_edw_strt_dttm     AS lkp_edw_strt_dttm,
         exp_src_pass1.in_doc_id             AS in_doc_id,
         exp_src_pass1.in_invc_amt_trans_amt AS in_invc_amt_trans_amt,
         exp_src_pass1.in_invc_amt_dttm      AS in_invc_amt_dttm,
         exp_src_pass1.in_invc_amt_type_cd   AS in_invc_amt_type_cd,
         exp_src_pass1.in_prcs_id            AS in_prcs_id,
         exp_src_pass1.in_edw_strt_dttm      AS in_edw_strt_dttm,
         exp_src_pass1.in_edw_end_dttm       AS in_edw_end_dttm,
         exp_src_pass1.calc_ins_upd          AS calc_ins_upd,
         exp_src_pass1.lkp_edw_end_dttm      AS lkp_edw_end_dttm,
         exp_src_pass1.retired               AS retired,
         exp_src_pass1.lkp_invc_amt_dttm     AS lkp_invc_amt_dttm,
         exp_src_pass1.rank                  AS rank,
         exp_src_pass1.source_record_id
  FROM   exp_src_pass1
  WHERE  exp_src_pass1.in_doc_id IS NOT NULL
  AND    (
                exp_src_pass1.calc_ins_upd = ''I''
         OR     exp_src_pass1.calc_ins_upd = ''U''
         OR     (
                       exp_src_pass1.retired = 0
                AND    exp_src_pass1.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) );
  
  -- Component RTR_Insert_Update1_Grp_Retired, Type ROUTER Output Group Grp_Retired
  create or replace temporary table rtr_insert_update1_grp_retired as
  SELECT exp_src_pass1.lkp_invc_id           AS lkp_invc_id,
         exp_src_pass1.lkp_invc_amt_type_cd  AS lkp_invc_amt_type_cd,
         exp_src_pass1.lkp_edw_strt_dttm     AS lkp_edw_strt_dttm,
         exp_src_pass1.in_doc_id             AS in_doc_id,
         exp_src_pass1.in_invc_amt_trans_amt AS in_invc_amt_trans_amt,
         exp_src_pass1.in_invc_amt_dttm      AS in_invc_amt_dttm,
         exp_src_pass1.in_invc_amt_type_cd   AS in_invc_amt_type_cd,
         exp_src_pass1.in_prcs_id            AS in_prcs_id,
         exp_src_pass1.in_edw_strt_dttm      AS in_edw_strt_dttm,
         exp_src_pass1.in_edw_end_dttm       AS in_edw_end_dttm,
         exp_src_pass1.calc_ins_upd          AS calc_ins_upd,
         exp_src_pass1.lkp_edw_end_dttm      AS lkp_edw_end_dttm,
         exp_src_pass1.retired               AS retired,
         exp_src_pass1.lkp_invc_amt_dttm     AS lkp_invc_amt_dttm,
         exp_src_pass1.rank                  AS rank,
         exp_src_pass1.source_record_id
  FROM   exp_src_pass1
  WHERE  exp_src_pass1.calc_ins_upd = ''R''
  AND    exp_src_pass1.retired != 0
  AND    exp_src_pass1.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_inv_amt_ins1, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_inv_amt_ins1 AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insert_update1_grp_insert.in_doc_id             AS in_doc_id,
                rtr_insert_update1_grp_insert.in_invc_amt_trans_amt AS in_invc_amt_trans_amt,
                rtr_insert_update1_grp_insert.in_invc_amt_dttm      AS in_invc_amt_dttm,
                rtr_insert_update1_grp_insert.in_invc_amt_type_cd   AS in_invc_amt_type_cd,
                rtr_insert_update1_grp_insert.in_prcs_id            AS in_prcs_id,
                rtr_insert_update1_grp_insert.in_edw_strt_dttm      AS in_edw_strt_dttm1,
                rtr_insert_update1_grp_insert.in_edw_end_dttm       AS in_edw_end_dttm1,
                rtr_insert_update1_grp_insert.retired               AS retired1,
                rtr_insert_update1_grp_insert.rank                  AS rank1,
                0                                                   AS update_strategy_action,
				rtr_insert_update1_grp_insert.source_record_id
         FROM   rtr_insert_update1_grp_insert );
  -- Component upd_inv_amt_retired1, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_inv_amt_retired1 AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insert_update1_grp_retired.lkp_invc_id          AS lkp_invc_id,
                rtr_insert_update1_grp_retired.lkp_invc_amt_type_cd AS lkp_invc_amt_type_cd3,
                rtr_insert_update1_grp_retired.lkp_edw_strt_dttm    AS lkp_edw_strt_dttm,
                rtr_insert_update1_grp_retired.in_edw_strt_dttm     AS in_edw_strt_dttm3,
                rtr_insert_update1_grp_retired.in_prcs_id           AS in_prcs_id3,
                1                                                   AS update_strategy_action,
				rtr_insert_update1_grp_retired.source_record_id
         FROM   rtr_insert_update1_grp_retired );
  -- Component exp_retired1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_retired1 AS
  (
         SELECT upd_inv_amt_retired1.lkp_invc_id           AS lkp_invc_id,
                upd_inv_amt_retired1.lkp_invc_amt_type_cd3 AS lkp_invc_amt_type_cd3,
                upd_inv_amt_retired1.lkp_edw_strt_dttm     AS lkp_edw_strt_dttm,
                current_timestamp                          AS in_edw_end_dttm,
                upd_inv_amt_retired1.source_record_id
         FROM   upd_inv_amt_retired1 );
  -- Component tgt_inv_amt_retired1, Type TARGET
  merge
  INTO         db_t_prod_core.invc_amt
  USING        exp_retired1
  ON (
                            invc_amt.invc_id = exp_retired1.lkp_invc_id
               AND          invc_amt.invc_amt_type_cd = exp_retired1.lkp_invc_amt_type_cd3
               AND          invc_amt.edw_strt_dttm = exp_retired1.lkp_edw_strt_dttm)
  WHEN matched THEN
  UPDATE
  SET    invc_id = exp_retired1.lkp_invc_id,
         invc_amt_type_cd = exp_retired1.lkp_invc_amt_type_cd3,
         edw_strt_dttm = exp_retired1.lkp_edw_strt_dttm,
         edw_end_dttm = exp_retired1.in_edw_end_dttm;
  
  -- Component exp_ins_pass_to_target1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ins_pass_to_target1 AS
  (
         SELECT upd_inv_amt_ins1.in_doc_id             AS in_doc_id,
                upd_inv_amt_ins1.in_invc_amt_trans_amt AS in_invc_amt_trans_amt,
                upd_inv_amt_ins1.in_invc_amt_dttm      AS in_invc_amt_dttm,
                upd_inv_amt_ins1.in_invc_amt_type_cd   AS in_invc_amt_type_cd,
                upd_inv_amt_ins1.in_prcs_id            AS in_prcs_id,
                CASE
                       WHEN upd_inv_amt_ins1.retired1 = 0 THEN upd_inv_amt_ins1.in_edw_end_dttm1
                       ELSE current_timestamp
                END AS in_edw_end_dttm11,
                CASE
                       WHEN upd_inv_amt_ins1.retired1 = 0 THEN dateadd(''second'', ( 2 * ( upd_inv_amt_ins1.rank1 - 1 ) ), current_timestamp)
                       ELSE current_timestamp
                END AS in_edw_strt_dttm1,
                upd_inv_amt_ins1.source_record_id
         FROM   upd_inv_amt_ins1 );
  -- Component tgt_inv_amt_ins1, Type TARGET
  INSERT INTO db_t_prod_core.invc_amt
              (
                          invc_id,
                          invc_amt_type_cd,
                          invc_amt_dttm,
                          invc_amt_trans_amt,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_ins_pass_to_target1.in_doc_id             AS invc_id,
         exp_ins_pass_to_target1.in_invc_amt_type_cd   AS invc_amt_type_cd,
         exp_ins_pass_to_target1.in_invc_amt_dttm      AS invc_amt_dttm,
         exp_ins_pass_to_target1.in_invc_amt_trans_amt AS invc_amt_trans_amt,
         exp_ins_pass_to_target1.in_prcs_id            AS prcs_id,
         exp_ins_pass_to_target1.in_edw_strt_dttm1     AS edw_strt_dttm,
         exp_ins_pass_to_target1.in_edw_end_dttm11     AS edw_end_dttm
  FROM   exp_ins_pass_to_target1;
  
  -- PIPELINE END FOR 2
  -- Component tgt_inv_amt_ins1, Type Post SQL
  UPDATE db_t_prod_core.invc_amt
    SET    edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT invc_id,
                                         invc_amt_type_cd,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY invc_id, invc_amt_type_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.invc_amt ) a

  WHERE  invc_amt.edw_strt_dttm = a.edw_strt_dttm
  AND    invc_amt.invc_id=a.invc_id
  AND    invc_amt.invc_amt_type_cd=a.invc_amt_type_cd
  AND    lead1 IS NOT NULL;

END;
';