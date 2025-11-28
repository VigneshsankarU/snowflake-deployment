-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_EV_DT_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component SQ_bc_transaction, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_bc_transaction AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS lkp_ev_id,
                $2  AS lkp_ev_dt_type_cd,
                $3  AS lkp_ev_dttm,
                $4  AS lkp_edw_strt_dttm,
                $5  AS in_ev_id,
                $6  AS in_ev_dttm,
                $7  AS in_ev_dt_type_cd,
                $8  AS prcs_id,
                $9  AS edw_strt_dttm,
                $10 AS edw_end_dttm,
                $11 AS v_lkp_chksm,
                $12 AS v_src_chksm,
                $13 AS o_flag,
                $14 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                            SELECT    lkp_target.ev_id         AS lkp_ev_id,
                                                      lkp_target.ev_dt_type_cd AS lkp_ev_dt_type_cd,
                                                      lkp_target.ev_dttm       AS lkp_ev_dttm,
                                                      lkp_target.edw_strt_dttm AS lkp_edw_strt_dttm,
                                                      in_ev_id,
                                                      src_modificationdate AS in_ev_dttm,
                                                      out_ev_dt_type_cd    AS in_ev_dt_type_cd,
                                                      prcs_id,
                                                      sq2.edw_strt_dttm AS edw_strt_dttm,
                                                      sq2.edw_end_dttm  AS edw_end_dttm,
                                                      /*MD5(to_char(ltrim(rtrim(lkp_EV_DTTM))))*/
                                                      cast(trim(cast(lkp_ev_dttm AS VARCHAR(100))) AS VARCHAR(100)) AS v_lkp_chksm,
                                                      /*MD5(to_char(ltrim(rtrim(in_EV_DTTM))))*/
                                                      cast(trim(cast(in_ev_dttm AS VARCHAR(100))) AS VARCHAR(100)) AS v_src_chksm,
                                                      /*CASE WHEN lkp_EV_ID IS NULL THEN ''I'' ELSE CASE WHEN v_lkp_chksm!=v_src_chksm THEN ''U'' ELSE $3 END END*/
                                                      CASE
                                                                WHEN lkp_ev_id IS NULL THEN ''I''
                                                                WHEN v_lkp_chksm <> v_src_chksm THEN ''U''
                                                      END AS o_flag
                                            FROM      (
                                                      (
                                                                SELECT    src_id,
                                                                          lkp_ev_actvy_type.tgt_idntftn_val AS out_typecode ,
                                                                          src_modificationdate,
                                                                          lkp_ev_sbtype.tgt_idntftn_val                   AS out_ev_sbtype_cd,
                                                                          lkp_ev_dt_type.tgt_idntftn_val                  AS out_ev_dt_type_cd,
                                                                          lkp_dir_ev.ev_id                                AS in_ev_id,
                                                                          $prcs_id                                        AS prcs_id,
                                                                          cast(current_timestamp(0) AS timestamp)         AS edw_strt_dttm,
                                                                          cast(''9999-12-31 23:59:59.999999'' AS timestamp) AS edw_end_dttm
                                                                FROM      (
                                                                          (
                                                                                 /*SQ query starts here*/
                                                                                 SELECT src.id_stg               AS src_id,
                                                                                        src.typecode_stg         AS src_typecode,
                                                                                        src.modificationdate_stg AS src_modificationdate,
                                                                                        ''EV_SBTYPE2''             AS src_ev_sbtype_cd,
                                                                                        ''EV_DT_TYPE1''            AS src_ev_dt_type_cd
                                                                                 FROM   (
                                                                                                   SELECT     bc_transaction.id_stg,
                                                                                                              bctl_transaction.typecode_stg,
                                                                                                              bc_billinginstruction.modificationdate_stg
                                                                                                   FROM       db_t_prod_stag.bc_billinginstruction
                                                                                                   inner join db_t_prod_stag.bc_charge
                                                                                                   ON         bc_charge.billinginstructionid_stg = bc_billinginstruction.id_stg
                                                                                                   inner join db_t_prod_stag.bc_invoiceitem
                                                                                                   ON         bc_invoiceitem.chargeid_stg = bc_charge.id_stg
                                                                                                   inner join db_t_prod_stag.bc_itemevent
                                                                                                   ON         bc_itemevent.invoiceitemid_stg = bc_invoiceitem.id_stg
                                                                                                   inner join db_t_prod_stag.bc_transaction
                                                                                                   ON         bc_transaction.id_stg = bc_itemevent.transactionid_stg
                                                                                                   inner join db_t_prod_stag.bctl_transaction
                                                                                                   ON         bctl_transaction.id_stg = bc_transaction.subtype_stg
                                                                                                   WHERE      bc_transaction.updatetime_stg > ($start_dttm)
                                                                                                   AND        bc_transaction.updatetime_stg <= ($end_dttm)
                                                                                                   AND        bc_billinginstruction.modificationdate_stg IS NOT NULL qualify row_number() over( PARTITION BY bc_transaction.id_stg,bctl_transaction.typecode_stg ORDER BY bc_transaction.updatetime_stg DESC) = 1)AS src
                                                                                        /*SQ Query ends here*/
                                                                          )sq1)
                                                                          /*XLAT_EV_ACTVY_TYPE*/
                                                                left join
                                                                          (
                                                                                 SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val,
                                                                                        teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                 FROM   db_t_prod_core.teradata_etl_ref_xlat 
                                                                                 WHERE  tgt_idntftn_nm = ''EV_ACTVY_TYPE''
                                                                                 AND    src_idntftn_nm = ''bctl_transaction.typecode''
                                                                                 AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''GW'')
                                                                                 AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp_ev_actvy_type
                                                                ON        lkp_ev_actvy_type.src_idntftn_val = sq1.src_typecode
                                                                          /*EV_SBTYPE*/
                                                                left join
                                                                          (
                                                                                 SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val,
                                                                                        teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                 FROM   db_t_prod_core.teradata_etl_ref_xlat 
                                                                                 WHERE  tgt_idntftn_nm = ''EV_SBTYPE''
                                                                                 AND    src_idntftn_nm = ''derived''
                                                                                 AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''DS'')
                                                                                 AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp_ev_sbtype
                                                                ON        lkp_ev_sbtype.src_idntftn_val = sq1.src_ev_sbtype_cd
                                                                          /*EV_DT_TYPE*/
                                                                left join
                                                                          (
                                                                                 SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val,
                                                                                        teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                 FROM   db_t_prod_core.teradata_etl_ref_xlat 
                                                                                 WHERE  tgt_idntftn_nm = ''EV_DT_TYPE''
                                                                                 AND    src_idntftn_nm = ''derived''
                                                                                 AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''DS'')
                                                                                 AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp_ev_dt_type
                                                                ON        lkp_ev_dt_type.src_idntftn_val = sq1.src_ev_dt_type_cd
                                                                          /*lkp_dir_ev*/
                                                                left join
                                                                          (
                                                                                 SELECT dir_ev.ev_id            AS ev_id,
                                                                                        dir_ev.src_trans_id     AS src_trans_id,
                                                                                        dir_ev.ev_sbtype_cd     AS ev_sbtype_cd,
                                                                                        dir_ev.ev_actvy_type_cd AS ev_actvy_type_cd
                                                                                 FROM   db_t_prod_core.dir_ev 
                                                                                 WHERE  dir_ev.ev_actvy_type_cd IN (''CHRGBILLED'',
                                                                                                                    ''CHRGDUE'',
                                                                                                                    ''CHRGPAID'',
                                                                                                                    ''CHRGWRTOFF'') ) lkp_dir_ev
                                                                ON        to_number(lkp_dir_ev.src_trans_id) = sq1.src_id
                                                                AND       lkp_dir_ev.ev_sbtype_cd = out_ev_sbtype_cd
                                                                AND       lkp_dir_ev.ev_actvy_type_cd = out_typecode )sq2 )
                                                      /*target*/
                                            left join
                                                      (
                                                             SELECT ev_dt.ev_dt_type_cd AS ev_dt_type_cd,
                                                                    ev_dt.ev_dttm       AS ev_dttm,
                                                                    ev_dt.edw_strt_dttm AS edw_strt_dttm,
                                                                    ev_dt.ev_id         AS ev_id
                                                             FROM   db_t_prod_core.ev_dt 
                                                             WHERE  cast(edw_end_dttm AS DATE)=''9999-12-31'' ) lkp_target
                                            ON        lkp_target.ev_id = sq2.in_ev_id ) src ) );
  -- Component exp_cdc, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_cdc AS
  (
         SELECT sq_bc_transaction.lkp_ev_id         AS lkp_ev_id,
                sq_bc_transaction.lkp_ev_dt_type_cd AS lkp_ev_dt_type_cd,
                sq_bc_transaction.lkp_ev_dttm       AS lkp_ev_dttm,
                sq_bc_transaction.lkp_edw_strt_dttm AS lkp_edw_strt_dttm,
                sq_bc_transaction.in_ev_id          AS in_ev_id,
                sq_bc_transaction.in_ev_dttm        AS in_ev_dttm,
                sq_bc_transaction.in_ev_dt_type_cd  AS in_ev_dt_type_cd,
                sq_bc_transaction.prcs_id           AS prcs_id,
                sq_bc_transaction.edw_strt_dttm     AS edw_strt_dttm,
                sq_bc_transaction.edw_end_dttm      AS edw_end_dttm,
                sq_bc_transaction.o_flag            AS o_flag,
                sq_bc_transaction.source_record_id
         FROM   sq_bc_transaction );
  -- Component RTRTRANS_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtrtrans_insert as
  SELECT exp_cdc.lkp_ev_id         AS lkp_ev_id,
         exp_cdc.lkp_ev_dt_type_cd AS lkp_ev_dt_type_cd,
         exp_cdc.lkp_ev_dttm       AS lkp_ev_dttm,
         exp_cdc.lkp_edw_strt_dttm AS lkp_edw_strt_dttm,
         exp_cdc.in_ev_id          AS in_ev_id,
         exp_cdc.in_ev_dttm        AS in_ev_dttm,
         exp_cdc.in_ev_dt_type_cd  AS in_ev_dt_type_cd,
         exp_cdc.prcs_id           AS prcs_id,
         exp_cdc.edw_strt_dttm     AS edw_strt_dttm,
         exp_cdc.edw_end_dttm      AS edw_end_dttm,
         exp_cdc.o_flag            AS o_flag,
         exp_cdc.source_record_id
  FROM   exp_cdc
  WHERE  exp_cdc.o_flag = ''I''
  OR     exp_cdc.o_flag = ''U'';
  
  -- Component RTRTRANS_UPDATE, Type ROUTER Output Group UPDATE
  create or replace temporary table rtrtrans_update as
  SELECT exp_cdc.lkp_ev_id         AS lkp_ev_id,
         exp_cdc.lkp_ev_dt_type_cd AS lkp_ev_dt_type_cd,
         exp_cdc.lkp_ev_dttm       AS lkp_ev_dttm,
         exp_cdc.lkp_edw_strt_dttm AS lkp_edw_strt_dttm,
         exp_cdc.in_ev_id          AS in_ev_id,
         exp_cdc.in_ev_dttm        AS in_ev_dttm,
         exp_cdc.in_ev_dt_type_cd  AS in_ev_dt_type_cd,
         exp_cdc.prcs_id           AS prcs_id,
         exp_cdc.edw_strt_dttm     AS edw_strt_dttm,
         exp_cdc.edw_end_dttm      AS edw_end_dttm,
         exp_cdc.o_flag            AS o_flag,
         exp_cdc.source_record_id
  FROM   exp_cdc
  WHERE  exp_cdc.o_flag = ''U'';
  
  -- Component upd_ev_dt_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ev_dt_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtrtrans_update.lkp_ev_id         AS lkp_ev_id3,
                rtrtrans_update.lkp_ev_dt_type_cd AS lkp_ev_dt_type_cd3,
                rtrtrans_update.lkp_ev_dttm       AS lkp_ev_dttm3,
                rtrtrans_update.lkp_edw_strt_dttm AS lkp_edw_strt_dttm3,
                rtrtrans_update.edw_strt_dttm     AS edw_strt_dttm3,
                1                                 AS update_strategy_action,
				rtrtrans_update.source_record_id
         FROM   rtrtrans_update );
  -- Component upd_ev_dt_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ev_dt_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtrtrans_insert.in_ev_id         AS in_ev_id1,
                rtrtrans_insert.in_ev_dttm       AS in_ev_dttm1,
                rtrtrans_insert.in_ev_dt_type_cd AS in_ev_dt_type_cd1,
                rtrtrans_insert.prcs_id          AS prcs_id1,
                rtrtrans_insert.edw_strt_dttm    AS edw_strt_dttm1,
                rtrtrans_insert.edw_end_dttm     AS edw_end_dttm1,
                0                                AS update_strategy_action,
				rtrtrans_insert.source_record_id
         FROM   rtrtrans_insert );
  -- Component exp_update, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_update AS
  (
         SELECT upd_ev_dt_upd.lkp_ev_id3                         AS lkp_ev_id3,
                upd_ev_dt_upd.lkp_ev_dt_type_cd3                 AS lkp_ev_dt_type_cd3,
                upd_ev_dt_upd.lkp_ev_dttm3                       AS lkp_ev_dttm3,
                upd_ev_dt_upd.lkp_edw_strt_dttm3                 AS lkp_edw_strt_dttm3,
                dateadd(''second'', - 1, upd_ev_dt_upd.edw_strt_dttm3) AS out_edw_end_dttm,
                upd_ev_dt_upd.source_record_id
         FROM   upd_ev_dt_upd );
  -- Component exp_Insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_insert AS
  (
         SELECT upd_ev_dt_ins.in_ev_id1         AS in_ev_id1,
                upd_ev_dt_ins.in_ev_dttm1       AS in_ev_dttm1,
                upd_ev_dt_ins.in_ev_dt_type_cd1 AS in_ev_dt_type_cd1,
                upd_ev_dt_ins.prcs_id1          AS prcs_id1,
                upd_ev_dt_ins.edw_strt_dttm1    AS edw_strt_dttm1,
                upd_ev_dt_ins.edw_end_dttm1     AS edw_end_dttm1,
                upd_ev_dt_ins.source_record_id
         FROM   upd_ev_dt_ins );
  -- Component EV_DT_update, Type TARGET
  merge
  INTO         db_t_prod_core.ev_dt
  USING        exp_update
  ON (
                            ev_dt.ev_id = exp_update.lkp_ev_id3
               AND          ev_dt.edw_strt_dttm = exp_update.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    ev_id = exp_update.lkp_ev_id3,
         ev_dt_type_cd = exp_update.lkp_ev_dt_type_cd3,
         ev_dttm = exp_update.lkp_ev_dttm3,
         edw_strt_dttm = exp_update.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_update.out_edw_end_dttm;
  
  -- Component EV_DT_ins, Type TARGET
  INSERT INTO db_t_prod_core.ev_dt
              (
                          ev_id,
                          ev_dt_type_cd,
                          ev_dttm,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_insert.in_ev_id1         AS ev_id,
         exp_insert.in_ev_dt_type_cd1 AS ev_dt_type_cd,
         exp_insert.in_ev_dttm1       AS ev_dttm,
         exp_insert.prcs_id1          AS prcs_id,
         exp_insert.edw_strt_dttm1    AS edw_strt_dttm,
         exp_insert.edw_end_dttm1     AS edw_end_dttm
  FROM   exp_insert;

END;
';