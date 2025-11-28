-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_MCHNRY_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM STRING;
  START_DTTM STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
  prcs_id int;


BEGIN

 run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);

  -- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_asset_clasfcn AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRTY_ASSET_CLASFCN''
                --AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'' ,''pcx_holineschcovitemcov_alfa.ChoiceTerm1'', ''cctl_contentlineitemschedule'')
         AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''DS'',
                                                          ''GW'')
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_asset_sbtype AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRTY_ASSET_SBTYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_XREF_PRTY_ASSET, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_xref_prty_asset AS
  (
         SELECT prty_asset_id,
                prty_asset_sbtype_cd,
                asset_host_id_val,
                prty_asset_clasfcn_cd,
                src_sys_cd,
                load_dttm
         FROM   db_t_prod_core.dir_prty_asset );
  -- Component SQ_fopmachinery, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_fopmachinery AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS fixedid,
                $2  AS assettype,
                $3  AS classification_code,
                $4  AS typecode,
                $5  AS sernum,
                $6  AS make,
                $7  AS model,
                $8  AS machyear,
                $9  AS firesuppr,
                $10 AS updatetime,
                $11 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   ltrim(rtrim(fixedid)),
                                                    assettype,
                                                    classification_code,
                                                    typecode,
                                                    sernum,
                                                    make,
                                                    model,
                                                    machyear,
                                                    firesuppr,
                                                    updatetime 
                                           FROM    (
                                                                    SELECT DISTINCT cast(fm.fixedid_stg AS         VARCHAR(100)) AS fixedid,
                                                                                    cast(''PRTY_ASSET_SBTYPE34'' AS  VARCHAR(50))  AS assettype ,
                                                                                    cast(''PRTY_ASSET_CLASFCN12'' AS VARCHAR(50))  AS classification_code,
                                                                                    fmtype.typecode_stg                          AS typecode,
                                                                                    fm.sernum_stg                                AS sernum,
                                                                                    fm.make_stg                                  AS make,
                                                                                    fm.model_stg                                 AS model,
                                                                                    fm.machyear_stg                              AS machyear,
                                                                                    fm.firesuppr_stg                             AS firesuppr,
                                                                                    CASE
                                                                                                    WHEN fm.updatetime_stg>pp.updatetime_stg THEN fm.updatetime_stg
                                                                                                    ELSE pp.updatetime_stg
                                                                                    END               AS updatetime,
                                                                                    fm.createtime_stg AS createtime,
                                                                                    CASE
                                                                                                    WHEN fm.expirationdate_stg IS NULL THEN cast(''9999-12-31 23:59:59.999999'' AS timestamp(6))
                                                                                                    ELSE fm.expirationdate_stg
                                                                                    END AS expirationdate
                                                                    FROM            db_t_prod_stag.pcx_fopmachinery fm
                                                                    join            db_t_prod_stag.pc_policyperiod pp
                                                                    ON              pp.id_stg=fm.branchid_stg
                                                                    left join       db_t_prod_stag.pctl_fopmachinerytype fmtype
                                                                    ON              fmtype.id_stg = fm.machinerytype_stg
                                                                    WHERE           fm.fixedid_stg IS NOT NULL
                                                                    AND             typecode IS NOT NULL
                                                                    AND             sernum IS NOT NULL
                                                                    AND             (
                                                                                                    fm.expirationdate_stg IS NULL
                                                                                    OR              fm.expirationdate_stg>pp.editeffectivedate_stg)
                                                                    AND             (
                                                                                                    fm.updatetime_stg >(:START_DTTM)
                                                                                    AND             fm.updatetime_stg <= (:END_DTTM)
                                                                                    OR              (
                                                                                                                    pp.updatetime_stg>(:START_DTTM)
                                                                                                    AND             pp.updatetime_stg<=(:END_DTTM))) 
											qualify row_number() over(PARTITION BY ltrim(rtrim(fixedid)) ORDER BY expirationdate DESC,updatetime DESC,createtime DESC)=1) AS tmp
							) src ) );
  -- Component EXP_MCHNRY, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_mchnry AS
  (
            SELECT
                      CASE
                                WHEN lkp_1.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_2.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */
                      END AS v_assettype,
                      CASE
                                WHEN lkp_3.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_4.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */
                      END                        AS v_classification_code,
                      sq_fopmachinery.typecode   AS src_typecode,
                      sq_fopmachinery.sernum     AS src_sernum,
                      sq_fopmachinery.make       AS src_make,
                      sq_fopmachinery.model      AS src_model,
                      sq_fopmachinery.machyear   AS src_machyear,
                      sq_fopmachinery.firesuppr  AS src_firesuppr,
                      sq_fopmachinery.updatetime AS src_updatetime,
                      lkp_5.prty_asset_id
                      /* replaced lookup LKP_XREF_PRTY_ASSET */
                                                                                             AS src_prty_asset_id,
                      :prcs_id                                                               AS prcs_id,
                      current_timestamp                                                      AS edw_strt_dttm,
                      to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                      to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS trans_end_dttm,
                      sq_fopmachinery.source_record_id,
                      row_number() over (PARTITION BY sq_fopmachinery.source_record_id ORDER BY sq_fopmachinery.source_record_id) AS rnk
            FROM      sq_fopmachinery
            left join lkp_teradata_etl_ref_xlat_asset_sbtype lkp_1
            ON        lkp_1.src_idntftn_val = sq_fopmachinery.assettype
            left join lkp_teradata_etl_ref_xlat_asset_sbtype lkp_2
            ON        lkp_2.src_idntftn_val = sq_fopmachinery.assettype
            left join lkp_teradata_etl_ref_xlat_asset_clasfcn lkp_3
            ON        lkp_3.src_idntftn_val = sq_fopmachinery.classification_code
            left join lkp_teradata_etl_ref_xlat_asset_clasfcn lkp_4
            ON        lkp_4.src_idntftn_val = sq_fopmachinery.classification_code
            left join lkp_xref_prty_asset lkp_5
            ON        lkp_5.prty_asset_sbtype_cd = v_assettype
            AND       lkp_5.asset_host_id_val = sq_fopmachinery.fixedid
            AND       lkp_5.prty_asset_clasfcn_cd = v_classification_code qualify rnk = 1 );
  -- Component LKP_MCHNRY, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_mchnry AS
  (
            SELECT    lkp.prty_asset_id,
                      lkp.mchnry_type_cd,
                      lkp.serial_num,
                      lkp.mchnry_make,
                      lkp.mchnry_modl,
                      lkp.mchnry_yr,
                      lkp.fire_supr_ind,
                      lkp.edw_end_dttm,
                      exp_mchnry.source_record_id,
                      row_number() over(PARTITION BY exp_mchnry.source_record_id ORDER BY lkp.prty_asset_id ASC,lkp.mchnry_type_cd ASC,lkp.serial_num ASC,lkp.mchnry_make ASC,lkp.mchnry_modl ASC,lkp.mchnry_yr ASC,lkp.fire_supr_ind ASC,lkp.edw_end_dttm ASC) rnk
            FROM      exp_mchnry
            left join
                      (
                               SELECT   mchnry.mchnry_type_cd AS mchnry_type_cd,
                                        mchnry.serial_num     AS serial_num,
                                        mchnry.mchnry_make    AS mchnry_make,
                                        mchnry.mchnry_modl    AS mchnry_modl,
                                        mchnry.mchnry_yr      AS mchnry_yr,
                                        mchnry.fire_supr_ind  AS fire_supr_ind,
                                        mchnry.edw_end_dttm   AS edw_end_dttm,
                                        mchnry.prty_asset_id  AS prty_asset_id
                               FROM     db_t_prod_core.mchnry qualify row_number() over( PARTITION BY mchnry.prty_asset_id ORDER BY edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.prty_asset_id = exp_mchnry.src_prty_asset_id qualify rnk = 1 );
  -- Component EXP_CDC_CHECK, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_cdc_check AS
  (
             SELECT     exp_mchnry.src_typecode      AS src_typecode,
                        exp_mchnry.src_sernum        AS src_sernum,
                        exp_mchnry.src_make          AS src_make,
                        exp_mchnry.src_model         AS src_model,
                        exp_mchnry.src_machyear      AS src_machyear,
                        exp_mchnry.src_firesuppr     AS src_firesuppr,
                        exp_mchnry.src_updatetime    AS src_updatetime,
                        exp_mchnry.src_prty_asset_id AS src_prty_asset_id,
                        exp_mchnry.prcs_id           AS prcs_id,
                        exp_mchnry.edw_strt_dttm     AS edw_strt_dttm,
                        exp_mchnry.edw_end_dttm      AS edw_end_dttm,
                        exp_mchnry.trans_end_dttm    AS trans_end_dttm,
                        lkp_mchnry.prty_asset_id     AS lkp_prty_asset_id,
                        lkp_mchnry.mchnry_type_cd    AS lkp_mchnry_type_cd,
                        lkp_mchnry.serial_num        AS lkp_serial_num,
                        lkp_mchnry.mchnry_make       AS lkp_mchnry_make,
                        lkp_mchnry.mchnry_modl       AS lkp_mchnry_modl,
                        lkp_mchnry.mchnry_yr         AS lkp_mchnry_yr,
                        lkp_mchnry.fire_supr_ind     AS lkp_fire_supr_ind,
                        lkp_mchnry.edw_end_dttm      AS lkp_edw_end_dttm,
                        md5 ( ltrim ( rtrim ( exp_mchnry.src_typecode ) )
                                   || ltrim ( rtrim ( exp_mchnry.src_sernum ) )
                                   || ltrim ( rtrim ( exp_mchnry.src_make ) )
                                   || ltrim ( rtrim ( exp_mchnry.src_model ) )
                                   || ltrim ( rtrim ( exp_mchnry.src_machyear ) )
                                   || ltrim ( rtrim ( exp_mchnry.src_firesuppr ) ) ) AS v_src_md5,
                        md5 ( ltrim ( rtrim ( lkp_mchnry.mchnry_type_cd ) )
                                   || ltrim ( rtrim ( lkp_mchnry.serial_num ) )
                                   || ltrim ( rtrim ( lkp_mchnry.mchnry_make ) )
                                   || ltrim ( rtrim ( lkp_mchnry.mchnry_modl ) )
                                   || ltrim ( rtrim ( lkp_mchnry.mchnry_yr ) )
                                   || ltrim ( rtrim ( lkp_mchnry.fire_supr_ind ) ) ) AS v_tgt_md5,
                        CASE
                                   WHEN v_tgt_md5 IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_src_md5 = v_tgt_md5 THEN ''R''
                                                         ELSE ''U''
                                              END
                        END AS o_flag,
                        exp_mchnry.source_record_id
             FROM       exp_mchnry
             inner join lkp_mchnry
             ON         exp_mchnry.source_record_id = lkp_mchnry.source_record_id );
  -- Component RTRTRANS_INS_UPD, Type ROUTER Output Group INS_UPD
  create or replace temporary table rtrtrans_ins_upd as
  SELECT exp_cdc_check.src_typecode       AS src_typecode,
         exp_cdc_check.src_sernum         AS src_sernum,
         exp_cdc_check.src_make           AS src_make,
         exp_cdc_check.src_model          AS src_model,
         exp_cdc_check.src_machyear       AS src_machyear,
         exp_cdc_check.src_firesuppr      AS src_firesuppr,
         exp_cdc_check.src_updatetime     AS src_updatetime,
         exp_cdc_check.src_prty_asset_id  AS src_prty_asset_id,
         exp_cdc_check.prcs_id            AS prcs_id,
         exp_cdc_check.edw_strt_dttm      AS edw_strt_dttm,
         exp_cdc_check.edw_end_dttm       AS edw_end_dttm,
         exp_cdc_check.trans_end_dttm     AS trans_end_dttm,
         exp_cdc_check.lkp_prty_asset_id  AS lkp_prty_asset_id,
         exp_cdc_check.lkp_mchnry_type_cd AS lkp_mchnry_type_cd,
         exp_cdc_check.lkp_serial_num     AS lkp_serial_num,
         exp_cdc_check.lkp_mchnry_make    AS lkp_mchnry_make,
         exp_cdc_check.lkp_mchnry_modl    AS lkp_mchnry_modl,
         exp_cdc_check.lkp_mchnry_yr      AS lkp_mchnry_yr,
         exp_cdc_check.lkp_fire_supr_ind  AS lkp_fire_supr_ind,
         exp_cdc_check.lkp_edw_end_dttm   AS lkp_edw_end_dttm,
         exp_cdc_check.o_flag             AS o_flag,
         exp_cdc_check.source_record_id
  FROM   exp_cdc_check
  WHERE  exp_cdc_check.src_prty_asset_id IS NOT NULL
  AND    ( (
                       exp_cdc_check.o_flag = ''I''
                OR     exp_cdc_check.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) )
         OR     (
                       exp_cdc_check.o_flag = ''U''
                AND    exp_cdc_check.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) );
  
  -- Component UPDTRANS, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE updtrans AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtrtrans_ins_upd.src_prty_asset_id AS src_prty_asset_id,
                rtrtrans_ins_upd.src_typecode      AS src_typecode,
                rtrtrans_ins_upd.src_sernum        AS src_sernum,
                rtrtrans_ins_upd.src_make          AS src_make,
                rtrtrans_ins_upd.src_model         AS src_model,
                rtrtrans_ins_upd.src_machyear      AS src_machyear,
                rtrtrans_ins_upd.src_firesuppr     AS src_firesuppr,
                rtrtrans_ins_upd.prcs_id           AS prcs_id,
                rtrtrans_ins_upd.src_updatetime    AS trans_strt_dttm,
                rtrtrans_ins_upd.trans_end_dttm    AS trans_end_dttm,
                rtrtrans_ins_upd.edw_strt_dttm     AS edw_strt_dttm,
                rtrtrans_ins_upd.edw_end_dttm      AS edw_end_dttm,
                0                                  AS update_strategy_action,
				rtrtrans_ins_upd.source_record_id
         FROM   rtrtrans_ins_upd );
  -- Component MCHNRY, Type TARGET
  INSERT INTO db_t_prod_core.mchnry
              (
                          prty_asset_id,
                          mchnry_type_cd,
                          serial_num,
                          mchnry_make,
                          mchnry_modl,
                          mchnry_yr,
                          fire_supr_ind,
                          prcs_id,
                          trans_strt_dttm,
                          trans_end_dttm,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT updtrans.src_prty_asset_id AS prty_asset_id,
         updtrans.src_typecode      AS mchnry_type_cd,
         updtrans.src_sernum        AS serial_num,
         updtrans.src_make          AS mchnry_make,
         updtrans.src_model         AS mchnry_modl,
         updtrans.src_machyear      AS mchnry_yr,
         updtrans.src_firesuppr     AS fire_supr_ind,
         updtrans.prcs_id           AS prcs_id,
         updtrans.trans_strt_dttm   AS trans_strt_dttm,
         updtrans.trans_end_dttm    AS trans_end_dttm,
         updtrans.edw_strt_dttm     AS edw_strt_dttm,
         updtrans.edw_end_dttm      AS edw_end_dttm
  FROM   updtrans;
  
  -- Component MCHNRY, Type Post SQL
  UPDATE db_t_prod_core.mchnry
    SET    edw_end_dttm=a.lead1,
         trans_end_dttm=a.lead2
  FROM   (
                         SELECT DISTINCT prty_asset_id,
                                         trans_strt_dttm,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY prty_asset_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)                       - interval ''1 second'' AS lead1,
                                         max(trans_strt_dttm) over (PARTITION BY prty_asset_id ORDER BY trans_strt_dttm ASC,edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead2
                         FROM            db_t_prod_core.mchnry ) a
  WHERE  mchnry.edw_strt_dttm = a.edw_strt_dttm
  AND    mchnry.trans_strt_dttm = a.trans_strt_dttm
  AND    mchnry.prty_asset_id=a.prty_asset_id
  AND    cast(mchnry.edw_end_dttm AS DATE) = ''9999-12-31''
  AND    lead1 IS NOT NULL;

END;
';