-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_STMT_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component SQ_bc_agmt_stmt, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_bc_agmt_stmt AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS tgt_lkp_agmt_id1,
                $2  AS tgt_lkp_stmt_doc_id,
                $3  AS tgt_lkp_edw_strt_dttm2,
                $4  AS agmt_id,
                $5  AS doc_id,
                $6  AS out_flag,
                $7  AS edw_strt_dttm,
                $8  AS edw_end_dttm1,
                $9  AS retired,
                $10 AS tgt_lkp_edw_end_dttm2,
                $11 AS updatetime_stg,
                $12 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT agmt_id1,
                                                stmt_doc_id,
                                                edw_strt_dttm2,
                                                agmt_id2,
                                                doc_id,
                                                out_flag,
                                                edw_strt_dttm,
                                                edw_end_dttm1,
                                                retired,
                                                edw_end_dttm2,
                                                updatetime_stg
                                         FROM  (
                                                          SELECT    tgt_lkp.agmt_id1,
                                                                    tgt_lkp.stmt_doc_id,
                                                                    tgt_lkp.edw_strt_dttm2,
                                                                    agmt_lkp.agmt_id AS agmt_id2 ,
                                                                    doc_lkp.doc_id,
                                                                    CASE
                                                                              WHEN tgt_lkp.agmt_id1 IS NULL
                                                                              AND       tgt_lkp.stmt_doc_id IS NULL THEN ''I''
                                                                              ELSE ''R''
                                                                    END                                              AS out_flag,
                                                                    current_timestamp                                AS edw_strt_dttm,
                                                                    cast(''9999-12-31 23:59:59.999999'' AS timestamp ) AS edw_end_dttm1,
                                                                    retired,
                                                                    tgt_lkp.edw_end_dttm2,
                                                                    updatetime_stg
                                                          FROM     (
                                                                              SELECT    scr.invoicenumber_stg ,
                                                                                        scr.agmt_type,
                                                                                        scr.billingreferencenumber_alfa_stg,
                                                                                        scr.updatetime_stg,
                                                                                        src_cd_lkp.tgt_idntftn_val1,
                                                                                        doc_type_lkp.tgt_idntftn_val2,
                                                                                        doc_ctgy_type_lkp.tgt_idntftn_val3,
                                                                                        retired
                                                                              FROM     (
                                                                                                   SELECT     bc_invoice.invoicenumber_stg                     AS invoicenumber_stg,
                                                                                                              ''INV''                                            AS agmt_type,
                                                                                                              bc_invoicestream.billingreferencenumber_alfa_stg AS billingreferencenumber_alfa_stg ,
                                                                                                              bc_invoice.updatetime_stg                        AS updatetime_stg ,
                                                                                                              CASE
                                                                                                                         WHEN bc_invoice.retired_stg =0
                                                                                                                         AND        bc_invoicestream.retired_stg =0 THEN 0
                                                                                                                         ELSE 1
                                                                                                              END AS retired
                                                                                                   FROM       db_t_prod_stag.bc_invoice
                                                                                                   inner join db_t_prod_stag.bc_invoicestream
                                                                                                   ON         bc_invoicestream.id_stg=bc_invoice.invoicestreamid_stg
                                                                                                   WHERE      bc_invoice.updatetime_stg > ($start_dttm)
                                                                                                   AND        bc_invoice.updatetime_stg <= ($end_dttm) )scr
                                                                              left join
                                                                                        (
                                                                                               SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val1 ,
                                                                                                      teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val1
                                                                                               FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                                               WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
                                                                                               AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                                               AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                                               AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'') src_cd_lkp
                                                                              ON        src_cd_lkp.src_idntftn_val1=''SRC_SYS5''
                                                                              left join
                                                                                        (
                                                                                               SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val2 ,
                                                                                                      teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val2
                                                                                               FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                                               WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_TYPE''
                                                                                               AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'') doc_type_lkp
                                                                              ON        doc_type_lkp.src_idntftn_val2 =''DOC_TYPE3''
                                                                              left join
                                                                                        (
                                                                                               SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val3 ,
                                                                                                      teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val3
                                                                                               FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                                               WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''DOC_CTGY_TYPE''
                                                                                               AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'') doc_ctgy_type_lkp
                                                                              ON        doc_ctgy_type_lkp.src_idntftn_val3 =''DOC_CTGY_TYPE4'' ) a
                                                          left join
                                                                    (
                                                                             SELECT   agmt.agmt_id       AS agmt_id,
                                                                                      agmt.host_agmt_num AS host_agmt_num,
                                                                                      agmt.nk_src_key    AS nk_src_key,
                                                                                      agmt.agmt_type_cd  AS agmt_type_cd1
                                                                             FROM     db_t_prod_core.agmt 
																			 qualify row_number() over( PARTITION BY agmt.nk_src_key,agmt.host_agmt_num ORDER BY agmt.edw_end_dttm DESC) = 1)agmt_lkp
                                                          ON        a.billingreferencenumber_alfa_stg=agmt_lkp.nk_src_key
                                                          AND       a.agmt_type=agmt_lkp.agmt_type_cd1
                                                          left join
                                                                    (
                                                                             SELECT   doc.doc_id           AS doc_id,
                                                                                      doc.doc_issur_num    AS doc_issur_num,
                                                                                      doc.doc_type_cd      AS doc_type_cd,
                                                                                      doc.doc_ctgy_type_cd AS doc_ctgy_type_cd
                                                                             FROM     db_t_prod_core.doc qualify row_number () over ( PARTITION BY doc_issur_num,doc_ctgy_type_cd,doc_type_cd ORDER BY edw_end_dttm DESC)=1)doc_lkp
                                                          ON        a.invoicenumber_stg=doc_lkp.doc_issur_num
                                                          AND       a.tgt_idntftn_val2=doc_lkp.doc_type_cd
                                                          AND       a.tgt_idntftn_val3=doc_lkp.doc_ctgy_type_cd
                                                          left join
                                                                    (
                                                                             SELECT   agmt_stmt.edw_strt_dttm AS edw_strt_dttm2,
                                                                                      agmt_stmt.edw_end_dttm  AS edw_end_dttm2,
                                                                                      agmt_stmt.agmt_id       AS agmt_id1,
                                                                                      agmt_stmt.stmt_doc_id   AS stmt_doc_id
                                                                             FROM     db_t_prod_core.agmt_stmt qualify row_number() over( PARTITION BY stmt_doc_id,agmt_id ORDER BY edw_end_dttm DESC) = 1)tgt_lkp
                                                          ON        agmt_lkp.agmt_id=tgt_lkp.agmt_id1
                                                          AND       doc_lkp.doc_id=tgt_lkp.stmt_doc_id ) r ) src ) );
  -- Component exp_check_flag, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_check_flag AS
  (
         SELECT sq_bc_agmt_stmt.tgt_lkp_agmt_id1       AS lkp_agmt_id,
                sq_bc_agmt_stmt.tgt_lkp_stmt_doc_id    AS lkp_stmt_doc_id,
                sq_bc_agmt_stmt.tgt_lkp_edw_strt_dttm2 AS lkp_edw_strt_dttm,
                sq_bc_agmt_stmt.agmt_id                AS agmt_id,
                sq_bc_agmt_stmt.doc_id                 AS doc_id,
                $prcs_id                               AS prcs_id,
                sq_bc_agmt_stmt.out_flag               AS out_flag,
                sq_bc_agmt_stmt.edw_strt_dttm          AS edw_strt_dttm,
                sq_bc_agmt_stmt.edw_end_dttm1          AS edw_end_dttm,
                sq_bc_agmt_stmt.retired                AS retired,
                sq_bc_agmt_stmt.tgt_lkp_edw_end_dttm2  AS lkp_edw_end_dttm,
                sq_bc_agmt_stmt.updatetime_stg         AS updatetime,
                sq_bc_agmt_stmt.source_record_id
         FROM   sq_bc_agmt_stmt );
  -- Component rtr_ins_upd_New, Type ROUTER Output Group New
  create or replace TEMPORARY TABLE rtr_ins_upd_new AS
  SELECT exp_check_flag.lkp_agmt_id       AS lkp_agmt_id,
         exp_check_flag.lkp_stmt_doc_id   AS lkp_stmt_doc_id,
         exp_check_flag.lkp_edw_strt_dttm AS lkp_edw_strt_dttm,
         exp_check_flag.agmt_id           AS agmt_id,
         exp_check_flag.doc_id            AS doc_id,
         exp_check_flag.prcs_id           AS prcs_id,
         exp_check_flag.edw_strt_dttm     AS edw_strt_dttm,
         exp_check_flag.edw_end_dttm      AS edw_end_dttm,
         exp_check_flag.out_flag          AS out_flag,
         exp_check_flag.retired           AS retired,
         exp_check_flag.lkp_edw_end_dttm  AS lkp_edw_end_dttm,
         exp_check_flag.updatetime        AS updatetime,
         exp_check_flag.source_record_id
  FROM   exp_check_flag
  WHERE  exp_check_flag.agmt_id IS NOT NULL
  AND    exp_check_flag.doc_id IS NOT NULL
  AND    exp_check_flag.out_flag = ''I''
  OR     (
                exp_check_flag.lkp_edw_end_dttm != to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
         AND    exp_check_flag.retired = 0 );
  
  -- Component rtr_ins_upd_Retired, Type ROUTER Output Group Retired
  create or replace TEMPORARY TABLE rtr_ins_upd_retired AS
  SELECT exp_check_flag.lkp_agmt_id       AS lkp_agmt_id,
         exp_check_flag.lkp_stmt_doc_id   AS lkp_stmt_doc_id,
         exp_check_flag.lkp_edw_strt_dttm AS lkp_edw_strt_dttm,
         exp_check_flag.agmt_id           AS agmt_id,
         exp_check_flag.doc_id            AS doc_id,
         exp_check_flag.prcs_id           AS prcs_id,
         exp_check_flag.edw_strt_dttm     AS edw_strt_dttm,
         exp_check_flag.edw_end_dttm      AS edw_end_dttm,
         exp_check_flag.out_flag          AS out_flag,
         exp_check_flag.retired           AS retired,
         exp_check_flag.lkp_edw_end_dttm  AS lkp_edw_end_dttm,
         exp_check_flag.updatetime        AS updatetime,
         exp_check_flag.source_record_id
  FROM   exp_check_flag
  WHERE  exp_check_flag.out_flag = ''R''
  AND    exp_check_flag.retired != 0
  AND    exp_check_flag.lkp_edw_end_dttm = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_Retired, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_retired AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ins_upd_retired.lkp_agmt_id       AS agmt_id3,
                rtr_ins_upd_retired.lkp_stmt_doc_id   AS doc_id3,
                rtr_ins_upd_retired.prcs_id           AS prcs_id3,
                rtr_ins_upd_retired.lkp_edw_strt_dttm AS edw_strt_dttm3,
                rtr_ins_upd_retired.updatetime        AS updatetime1,
                1                                     AS update_strategy_action,
				rtr_ins_upd_retired.source_record_id
         FROM   rtr_ins_upd_retired );
  -- Component upd_Insert, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_insert AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ins_upd_new.agmt_id       AS agmt_id3,
                rtr_ins_upd_new.doc_id        AS doc_id3,
                rtr_ins_upd_new.prcs_id       AS prcs_id3,
                rtr_ins_upd_new.edw_strt_dttm AS edw_strt_dttm3,
                rtr_ins_upd_new.edw_end_dttm  AS edw_end_dttm3,
                rtr_ins_upd_new.retired       AS retired3,
                rtr_ins_upd_new.updatetime    AS updatetime3,
                0                             AS update_strategy_action,
				rtr_ins_upd_new.source_record_id
         FROM   rtr_ins_upd_new );
  -- Component Exp_PasstoTGT_Retired, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_passtotgt_retired AS
  (
         SELECT upd_retired.agmt_id3       AS agmt_id3,
                upd_retired.doc_id3        AS doc_id3,
                upd_retired.edw_strt_dttm3 AS edw_strt_dttm3,
                current_timestamp          AS edw_end_dttm3,
                upd_retired.updatetime1    AS updatetime1,
                upd_retired.source_record_id
         FROM   upd_retired );
  -- Component Exp_PasstoTGT_Insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_passtotgt_insert AS
  (
         SELECT upd_insert.agmt_id3       AS agmt_id3,
                upd_insert.doc_id3        AS doc_id3,
                upd_insert.prcs_id3       AS prcs_id3,
                upd_insert.edw_strt_dttm3 AS edw_strt_dttm3,
                CASE
                       WHEN upd_insert.retired3 <> 0 THEN upd_insert.edw_strt_dttm3
                       ELSE upd_insert.edw_end_dttm3
                END                    AS edw_end_dttm31,
                upd_insert.updatetime3 AS updatetime3,
                CASE
                       WHEN upd_insert.retired3 <> 0 THEN upd_insert.updatetime3
                       ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END AS trans_end_dttm,
                upd_insert.source_record_id
         FROM   upd_insert );
  -- Component AGMT_STMT_retired, Type TARGET
  merge
  INTO         db_t_prod_core.agmt_stmt
  USING        exp_passtotgt_retired
  ON (
                            agmt_stmt.agmt_id = exp_passtotgt_retired.agmt_id3
               AND          agmt_stmt.stmt_doc_id = exp_passtotgt_retired.doc_id3
               AND          agmt_stmt.edw_strt_dttm = exp_passtotgt_retired.edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    agmt_id = exp_passtotgt_retired.agmt_id3,
         stmt_doc_id = exp_passtotgt_retired.doc_id3,
         edw_strt_dttm = exp_passtotgt_retired.edw_strt_dttm3,
         edw_end_dttm = exp_passtotgt_retired.edw_end_dttm3,
         trans_end_dttm = exp_passtotgt_retired.updatetime1;
  
  -- Component AGMT_STMT_ins, Type TARGET
  INSERT INTO db_t_prod_core.agmt_stmt
              (
                          agmt_id,
                          stmt_doc_id,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_passtotgt_insert.agmt_id3       AS agmt_id,
         exp_passtotgt_insert.doc_id3        AS stmt_doc_id,
         exp_passtotgt_insert.prcs_id3       AS prcs_id,
         exp_passtotgt_insert.edw_strt_dttm3 AS edw_strt_dttm,
         exp_passtotgt_insert.edw_end_dttm31 AS edw_end_dttm,
         exp_passtotgt_insert.updatetime3    AS trans_strt_dttm,
         exp_passtotgt_insert.trans_end_dttm AS trans_end_dttm
  FROM   exp_passtotgt_insert;

END;
';