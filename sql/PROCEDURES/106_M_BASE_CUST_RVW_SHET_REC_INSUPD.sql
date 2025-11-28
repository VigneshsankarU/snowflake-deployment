-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CUST_RVW_SHET_REC_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component SQ_CRS_Records, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_crs_records AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS id_stg,
                $2  AS membernumber_stg,
                $3  AS agentnumber_stg,
                $4  AS lastreviewdate_stg,
                $5  AS docprepareddate_stg,
                $6  AS doccreateuser_stg,
                $7  AS creationuid_stg,
                $8  AS updateuid_stg,
                $9  AS creationts_stg,
                $10 AS updatets_stg,
                $11 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT crs_records.id_stg,
                                                crs_records.membernumber_stg,
                                                crs_records.agentnumber_stg,
                                                crs_records.lastreviewdate_stg,
                                                crs_records.docprepareddate_stg,
                                                crs_records.doccreateuser_stg,
                                                crs_records.creationuid_stg,
                                                crs_records.updateuid_stg,
                                                crs_records.creationts_stg,
                                                crs_records.updatets_stg
                                         FROM   db_t_prod_stag.crs_records ) src ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
         SELECT sq_crs_records.id_stg              AS id_stg,
                sq_crs_records.membernumber_stg    AS membernumber_stg,
                sq_crs_records.agentnumber_stg     AS agentnumber_stg,
                sq_crs_records.lastreviewdate_stg  AS lastreviewdate_stg,
                sq_crs_records.docprepareddate_stg AS docprepareddate_stg,
                sq_crs_records.doccreateuser_stg   AS doccreateuser_stg,
                sq_crs_records.creationuid_stg     AS creationuid_stg,
                sq_crs_records.updateuid_stg       AS updateuid_stg,
                sq_crs_records.creationts_stg      AS creationts_stg,
                sq_crs_records.updatets_stg        AS updatets_stg,
                sq_crs_records.source_record_id
         FROM   sq_crs_records );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
         SELECT exp_pass_from_source.id_stg              AS id_stg,
                exp_pass_from_source.membernumber_stg    AS membernumber_stg,
                exp_pass_from_source.agentnumber_stg     AS agentnumber_stg,
                exp_pass_from_source.lastreviewdate_stg  AS lastreviewdate_stg,
                exp_pass_from_source.docprepareddate_stg AS docprepareddate_stg,
                exp_pass_from_source.doccreateuser_stg   AS doccreateuser_stg,
                exp_pass_from_source.creationuid_stg     AS creationuid_stg,
                exp_pass_from_source.updateuid_stg       AS updateuid_stg,
                exp_pass_from_source.creationts_stg      AS creationts_stg,
                exp_pass_from_source.updatets_stg        AS updatets_stg,
                $prcs_id                                 AS out_prcs_id,
                exp_pass_from_source.source_record_id
         FROM   exp_pass_from_source );
  -- Component LKP_CUST_RVW_SHET_REC, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_cust_rvw_shet_rec AS
  (
            SELECT    lkp.id,
                      lkp.agtnbr,
                      lkp.lstrvwdt,
                      lkp.docprepdt,
                      lkp.doccrtusr,
                      lkp.crtuid,
                      lkp.upduid,
                      lkp.crtnts,
                      lkp.updtts,
                      lkp.edw_strt_dttm,
                      lkp.edw_end_dttm,
                      lkp.mbrnbr,
                      exp_data_transformation.source_record_id,
                      row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.id ASC,lkp.agtnbr ASC,lkp.lstrvwdt ASC,lkp.docprepdt ASC,lkp.doccrtusr ASC,lkp.crtuid ASC,lkp.upduid ASC,lkp.crtnts ASC,lkp.updtts ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.mbrnbr ASC) rnk
            FROM      exp_data_transformation
            left join
                      (
                               SELECT   cust_rvw_shet_rec.id            AS id ,
                                        cust_rvw_shet_rec.agtnbr        AS agtnbr ,
                                        cust_rvw_shet_rec.lstrvwdt      AS lstrvwdt ,
                                        cust_rvw_shet_rec.docprepdt     AS docprepdt ,
                                        cust_rvw_shet_rec.doccrtusr     AS doccrtusr ,
                                        cust_rvw_shet_rec.crtuid        AS crtuid ,
                                        cust_rvw_shet_rec.upduid        AS upduid ,
                                        cust_rvw_shet_rec.crtnts        AS crtnts ,
                                        cust_rvw_shet_rec.updtts        AS updtts ,
                                        cust_rvw_shet_rec.edw_strt_dttm AS edw_strt_dttm ,
                                        cust_rvw_shet_rec.edw_end_dttm  AS edw_end_dttm ,
                                        cust_rvw_shet_rec.mbrnbr        AS mbrnbr
                               FROM     db_t_prod_comn.cust_rvw_shet_rec qualify row_number() over( PARTITION BY mbrnbr ORDER BY edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.mbrnbr = exp_data_transformation.membernumber_stg qualify rnk = 1 );
  -- Component exp_cdc, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_cdc AS
  (
             SELECT     lkp_cust_rvw_shet_rec.id            AS lkp_id,
                        lkp_cust_rvw_shet_rec.mbrnbr        AS lkp_mbrnbr,
                        lkp_cust_rvw_shet_rec.agtnbr        AS lkp_agtnbr,
                        lkp_cust_rvw_shet_rec.lstrvwdt      AS lkp_lstrvwdt,
                        lkp_cust_rvw_shet_rec.docprepdt     AS lkp_docprepdt,
                        lkp_cust_rvw_shet_rec.doccrtusr     AS lkp_doccrtusr,
                        lkp_cust_rvw_shet_rec.crtuid        AS lkp_crtuid,
                        lkp_cust_rvw_shet_rec.upduid        AS lkp_upduid,
                        lkp_cust_rvw_shet_rec.crtnts        AS lkp_crtnts,
                        lkp_cust_rvw_shet_rec.updtts        AS lkp_updtts,
                        lkp_cust_rvw_shet_rec.edw_strt_dttm AS lkp_edw_strt_dttm,
                        lkp_cust_rvw_shet_rec.edw_end_dttm  AS lkp_edw_end_dttm,
                        md5 ( lkp_cust_rvw_shet_rec.id
                                   || lkp_cust_rvw_shet_rec.agtnbr
                                   || lkp_cust_rvw_shet_rec.lstrvwdt
                                   || lkp_cust_rvw_shet_rec.docprepdt
                                   || lkp_cust_rvw_shet_rec.doccrtusr
                                   || lkp_cust_rvw_shet_rec.crtuid
                                   || lkp_cust_rvw_shet_rec.upduid
                                   || lkp_cust_rvw_shet_rec.crtnts
                                   || lkp_cust_rvw_shet_rec.updtts ) AS md5_tgt,
                        md5 ( exp_data_transformation.id_stg
                                   || exp_data_transformation.agentnumber_stg
                                   || exp_data_transformation.lastreviewdate_stg
                                   || exp_data_transformation.docprepareddate_stg
                                   || exp_data_transformation.doccreateuser_stg
                                   || exp_data_transformation.creationuid_stg
                                   || exp_data_transformation.updateuid_stg
                                   || exp_data_transformation.creationts_stg
                                   || exp_data_transformation.updatets_stg ) AS md5_src,
                        CASE
                                   WHEN md5_tgt IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN md5_tgt != md5_src THEN ''U''
                                                         ELSE md5_tgt --$3
                                              END
                        END                                                                    AS ins_upd_flag,
                        current_timestamp                                                      AS edw_strt_dttm,
                        to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                        exp_data_transformation.id_stg                                         AS id_stg,
                        exp_data_transformation.membernumber_stg                               AS membernumber_stg,
                        exp_data_transformation.agentnumber_stg                                AS agentnumber_stg,
                        exp_data_transformation.lastreviewdate_stg                             AS lastreviewdate_stg,
                        exp_data_transformation.docprepareddate_stg                            AS docprepareddate_stg,
                        exp_data_transformation.doccreateuser_stg                              AS doccreateuser_stg,
                        exp_data_transformation.creationuid_stg                                AS creationuid_stg,
                        exp_data_transformation.updateuid_stg                                  AS updateuid_stg,
                        exp_data_transformation.creationts_stg                                 AS creationts_stg,
                        exp_data_transformation.updatets_stg                                   AS updatets_stg,
                        exp_data_transformation.out_prcs_id                                    AS out_prcs_id,
                        exp_data_transformation.source_record_id
             FROM       exp_data_transformation
             inner join lkp_cust_rvw_shet_rec
             ON         exp_data_transformation.source_record_id = lkp_cust_rvw_shet_rec.source_record_id );
  -- Component rtr_ins_sup_Insert, Type ROUTER Output Group Insert
  create or replace TEMPORARY TABLE rtr_ins_sup_insert AS
  SELECT exp_cdc.lkp_id              AS lkp_id,
         exp_cdc.lkp_mbrnbr          AS lkp_mbrnbr,
         exp_cdc.lkp_agtnbr          AS lkp_agtnbr,
         exp_cdc.lkp_lstrvwdt        AS lkp_lstrvwdt,
         exp_cdc.lkp_docprepdt       AS lkp_docprepdt,
         exp_cdc.lkp_doccrtusr       AS lkp_doccrtusr,
         exp_cdc.lkp_crtuid          AS lkp_crtuid,
         exp_cdc.lkp_upduid          AS lkp_upduid,
         exp_cdc.lkp_crtnts          AS lkp_crtnts,
         exp_cdc.lkp_updtts          AS lkp_updtts,
         exp_cdc.ins_upd_flag        AS ins_upd_flag,
         exp_cdc.edw_strt_dttm       AS edw_strt_dttm,
         exp_cdc.edw_end_dttm        AS edw_end_dttm,
         exp_cdc.id_stg              AS id_stg,
         exp_cdc.membernumber_stg    AS membernumber_stg,
         exp_cdc.agentnumber_stg     AS agentnumber_stg,
         exp_cdc.lastreviewdate_stg  AS lastreviewdate_stg,
         exp_cdc.docprepareddate_stg AS docprepareddate_stg,
         exp_cdc.doccreateuser_stg   AS doccreateuser_stg,
         exp_cdc.creationuid_stg     AS creationuid_stg,
         exp_cdc.updateuid_stg       AS updateuid_stg,
         exp_cdc.creationts_stg      AS creationts_stg,
         exp_cdc.updatets_stg        AS updatets_stg,
         exp_cdc.out_prcs_id         AS out_prcs_id,
         exp_cdc.lkp_edw_strt_dttm   AS lkp_edw_strt_dttm,
         exp_cdc.lkp_edw_end_dttm    AS lkp_edw_end_dttm,
         exp_cdc.source_record_id
  FROM   exp_cdc
  WHERE  exp_cdc.ins_upd_flag = ''I''
  AND    exp_cdc.membernumber_stg IS NOT NULL
  OR     (
                exp_cdc.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
         AND    exp_cdc.membernumber_stg IS NOT NULL )
  OR     exp_cdc.ins_upd_flag = ''U''
  AND    exp_cdc.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
  AND    exp_cdc.membernumber_stg IS NOT NULL;
  
  -- Component rtr_ins_sup_Update, Type ROUTER Output Group Update
  create or replace TEMPORARY TABLE rtr_ins_sup_update AS
  SELECT exp_cdc.lkp_id              AS lkp_id,
         exp_cdc.lkp_mbrnbr          AS lkp_mbrnbr,
         exp_cdc.lkp_agtnbr          AS lkp_agtnbr,
         exp_cdc.lkp_lstrvwdt        AS lkp_lstrvwdt,
         exp_cdc.lkp_docprepdt       AS lkp_docprepdt,
         exp_cdc.lkp_doccrtusr       AS lkp_doccrtusr,
         exp_cdc.lkp_crtuid          AS lkp_crtuid,
         exp_cdc.lkp_upduid          AS lkp_upduid,
         exp_cdc.lkp_crtnts          AS lkp_crtnts,
         exp_cdc.lkp_updtts          AS lkp_updtts,
         exp_cdc.ins_upd_flag        AS ins_upd_flag,
         exp_cdc.edw_strt_dttm       AS edw_strt_dttm,
         exp_cdc.edw_end_dttm        AS edw_end_dttm,
         exp_cdc.id_stg              AS id_stg,
         exp_cdc.membernumber_stg    AS membernumber_stg,
         exp_cdc.agentnumber_stg     AS agentnumber_stg,
         exp_cdc.lastreviewdate_stg  AS lastreviewdate_stg,
         exp_cdc.docprepareddate_stg AS docprepareddate_stg,
         exp_cdc.doccreateuser_stg   AS doccreateuser_stg,
         exp_cdc.creationuid_stg     AS creationuid_stg,
         exp_cdc.updateuid_stg       AS updateuid_stg,
         exp_cdc.creationts_stg      AS creationts_stg,
         exp_cdc.updatets_stg        AS updatets_stg,
         exp_cdc.out_prcs_id         AS out_prcs_id,
         exp_cdc.lkp_edw_strt_dttm   AS lkp_edw_strt_dttm,
         exp_cdc.lkp_edw_end_dttm    AS lkp_edw_end_dttm,
         exp_cdc.source_record_id
  FROM   exp_cdc
  WHERE  exp_cdc.ins_upd_flag = ''U''
  AND    exp_cdc.lkp_edw_end_dttm = to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
  AND    exp_cdc.membernumber_stg IS NOT NULL;
  
  -- Component upd_stg_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_stg_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ins_sup_update.edw_end_dttm      AS edw_end_dttm31,
                rtr_ins_sup_update.lkp_mbrnbr        AS lkp_mbrnbr3,
                rtr_ins_sup_update.lkp_edw_strt_dttm AS lkp_edw_strt_dttm3,
                rtr_ins_sup_update.lkp_updtts        AS lkp_updtts,
                1                                    AS update_strategy_action,
                rtr_ins_sup_update.source_record_id
         FROM   rtr_ins_sup_update );
  -- Component upd_stg_ins_original, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_stg_ins_original AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ins_sup_insert.id_stg              AS id_stg1,
                rtr_ins_sup_insert.membernumber_stg    AS membernumber_stg1,
                rtr_ins_sup_insert.agentnumber_stg     AS agentnumber_stg1,
                rtr_ins_sup_insert.lastreviewdate_stg  AS lastreviewdate_stg1,
                rtr_ins_sup_insert.docprepareddate_stg AS docprepareddate_stg1,
                rtr_ins_sup_insert.doccreateuser_stg   AS doccreateuser_stg1,
                rtr_ins_sup_insert.creationuid_stg     AS creationuid_stg1,
                rtr_ins_sup_insert.updateuid_stg       AS updateuid_stg1,
                rtr_ins_sup_insert.creationts_stg      AS creationts_stg1,
                rtr_ins_sup_insert.updatets_stg        AS updatets_stg1,
                rtr_ins_sup_insert.out_prcs_id         AS out_prcs_id,
                rtr_ins_sup_insert.edw_strt_dttm       AS edw_strt_dttm1,
                rtr_ins_sup_insert.edw_end_dttm        AS edw_end_dttm1,
                0                                      AS update_strategy_action,
                rtr_ins_sup_insert.source_record_id
         FROM   rtr_ins_sup_insert );
  -- Component exp_edw_end_dttm, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_edw_end_dttm AS
  (
         SELECT current_timestamp              AS edw_end_dttm3_o,
                upd_stg_upd.lkp_mbrnbr3        AS lkp_mbrnbr3,
                upd_stg_upd.lkp_edw_strt_dttm3 AS lkp_edw_strt_dttm3,
                upd_stg_upd.lkp_updtts         AS lkp_updtts,
                upd_stg_upd.source_record_id
         FROM   upd_stg_upd );
  -- Component exp_insert_pass_to_target, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_insert_pass_to_target AS
  (
         SELECT upd_stg_ins_original.id_stg1              AS id_stg1,
                upd_stg_ins_original.membernumber_stg1    AS membernumber_stg1,
                upd_stg_ins_original.agentnumber_stg1     AS agentnumber_stg1,
                upd_stg_ins_original.lastreviewdate_stg1  AS lastreviewdate_stg1,
                upd_stg_ins_original.docprepareddate_stg1 AS docprepareddate_stg1,
                upd_stg_ins_original.doccreateuser_stg1   AS doccreateuser_stg1,
                upd_stg_ins_original.creationuid_stg1     AS creationuid_stg1,
                upd_stg_ins_original.updateuid_stg1       AS updateuid_stg1,
                upd_stg_ins_original.creationts_stg1      AS creationts_stg1,
                upd_stg_ins_original.updatets_stg1        AS updatets_stg1,
                upd_stg_ins_original.out_prcs_id          AS out_prcs_id,
                upd_stg_ins_original.edw_strt_dttm1       AS edw_strt_dttm1,
                upd_stg_ins_original.edw_end_dttm1        AS edw_end_dttm1,
                upd_stg_ins_original.source_record_id
         FROM   upd_stg_ins_original );
  -- Component CUST_RVW_SHET_REC_upd, Type TARGET
  merge
  INTO         db_t_prod_comn.cust_rvw_shet_rec
  USING        exp_edw_end_dttm
  ON (
                            cust_rvw_shet_rec.mbrnbr = exp_edw_end_dttm.lkp_mbrnbr3
               AND          cust_rvw_shet_rec.edw_strt_dttm = exp_edw_end_dttm.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    mbrnbr = exp_edw_end_dttm.lkp_mbrnbr3,
         updtts = exp_edw_end_dttm.lkp_updtts,
         edw_strt_dttm = exp_edw_end_dttm.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_edw_end_dttm.edw_end_dttm3_o;
  
  -- Component CUST_RVW_SHET_REC_ins, Type TARGET
  INSERT INTO db_t_prod_comn.cust_rvw_shet_rec
              (
                          id,
                          mbrnbr,
                          agtnbr,
                          lstrvwdt,
                          docprepdt,
                          doccrtusr,
                          crtuid,
                          upduid,
                          crtnts,
                          updtts,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_insert_pass_to_target.id_stg1              AS id,
         exp_insert_pass_to_target.membernumber_stg1    AS mbrnbr,
         exp_insert_pass_to_target.agentnumber_stg1     AS agtnbr,
         exp_insert_pass_to_target.lastreviewdate_stg1  AS lstrvwdt,
         exp_insert_pass_to_target.docprepareddate_stg1 AS docprepdt,
         exp_insert_pass_to_target.doccreateuser_stg1   AS doccrtusr,
         exp_insert_pass_to_target.creationuid_stg1     AS crtuid,
         exp_insert_pass_to_target.updateuid_stg1       AS upduid,
         exp_insert_pass_to_target.creationts_stg1      AS crtnts,
         exp_insert_pass_to_target.updatets_stg1        AS updtts,
         exp_insert_pass_to_target.out_prcs_id          AS prcs_id,
         exp_insert_pass_to_target.edw_strt_dttm1       AS edw_strt_dttm,
         exp_insert_pass_to_target.edw_end_dttm1        AS edw_end_dttm
  FROM   exp_insert_pass_to_target;

END;
';