-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FD_AND_SD_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
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
         FROM  db_t_prod_core.dir_prty_asset );
  -- Component SQ_pcx_fopfeedandseed, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pcx_fopfeedandseed AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS fixedid,
                $2 AS asset_type,
                $3 AS classification_code,
                $4 AS fstype,
                $5 AS description,
                $6 AS value,
                $7 AS updatetime,
                $8 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   ltrim(rtrim(fixedid)),
                                                    assettype,
                                                    classification_code,
                                                    typecode,
                                                    description,
                                                    fd_sd_value,
                                                    updatetime 
                                           FROM     (
                                                                    SELECT DISTINCT cast(fs.fixedid_stg AS         VARCHAR(100)) AS fixedid,
                                                                                    cast(''PRTY_ASSET_SBTYPE33'' AS  VARCHAR(50))  AS assettype,
                                                                                    cast(''PRTY_ASSET_CLASFCN11'' AS VARCHAR(50))  AS classification_code,
                                                                                    ltrim(rtrim(fstype.typecode_stg))            AS typecode,
                                                                                    ltrim(rtrim(fs.description_stg))             AS description,
                                                                                    cast(fs.value_stg AS DECIMAL(18,4))          AS fd_sd_value,
                                                                                    CASE
                                                                                                    WHEN fs.updatetime_stg>pp.updatetime_stg THEN fs.updatetime_stg
                                                                                                    ELSE pp.updatetime_stg
                                                                                    END               AS updatetime,
                                                                                    fs.createtime_stg AS createtime,
                                                                                    CASE
                                                                                                    WHEN fs.expirationdate_stg IS NULL THEN cast(''9999-12-31 23:59:59.999999'' AS timestamp(6))
                                                                                                    ELSE fs.expirationdate_stg
                                                                                    END AS expirationdate
                                                                    FROM            db_t_prod_stag.pcx_fopfeedandseed fs
                                                                    join            db_t_prod_stag.pc_policyperiod pp
                                                                    ON              fs.branchid_stg=pp.id_stg
                                                                    left join       db_t_prod_stag.pctl_fopfeedandseedtype fstype
                                                                    ON              fstype.id_stg = fs.fstype_stg
                                                                    WHERE           fs.fixedid_stg IS NOT NULL
                                                                    AND             fstype.typecode_stg IS NOT NULL
                                                                    AND             (
                                                                                                    fs.expirationdate_stg IS NULL
                                                                                    OR              fs.expirationdate_stg>pp.editeffectivedate_stg)
                                                                    AND             				((
                                                                                                                    fs.updatetime_stg>($start_dttm)
                                                                                                    AND             fs.updatetime_stg<=($end_dttm))
                                                                                    OR              (
                                                                                                                    pp.updatetime_stg>($start_dttm)
                                                                                                    AND             pp.updatetime_stg<=($end_dttm))) 
												) AS tmp 
												qualify row_number() over(PARTITION BY ltrim(rtrim(fixedid)) ORDER BY expirationdate DESC,updatetime DESC,createtime DESC)=1
									) src 
				) 
	);
  -- Component EXP_FD_AND_SD, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_fd_and_sd AS
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
                      END                               AS v_classification_code,
                      sq_pcx_fopfeedandseed.fstype      AS src_fstype,
                      sq_pcx_fopfeedandseed.description AS src_description,
                      sq_pcx_fopfeedandseed.value       AS src_value,
                      sq_pcx_fopfeedandseed.updatetime  AS src_updatetime,
                      lkp_5.prty_asset_id
                      /* replaced lookup LKP_XREF_PRTY_ASSET */
                                                                                             AS src_prty_asset_id,
                      $prcs_id                                                               AS prcs_id,
                      current_timestamp                                                      AS edw_strt_dttm,
                      to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                      to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS trans_end_dttm,
                      sq_pcx_fopfeedandseed.source_record_id,
                      row_number() over (PARTITION BY sq_pcx_fopfeedandseed.source_record_id ORDER BY sq_pcx_fopfeedandseed.source_record_id) AS rnk
            FROM      sq_pcx_fopfeedandseed
            left join lkp_teradata_etl_ref_xlat_asset_sbtype lkp_1
            ON        lkp_1.src_idntftn_val = sq_pcx_fopfeedandseed.asset_type
            left join lkp_teradata_etl_ref_xlat_asset_sbtype lkp_2
            ON        lkp_2.src_idntftn_val = sq_pcx_fopfeedandseed.asset_type
            left join lkp_teradata_etl_ref_xlat_asset_clasfcn lkp_3
            ON        lkp_3.src_idntftn_val = sq_pcx_fopfeedandseed.classification_code
            left join lkp_teradata_etl_ref_xlat_asset_clasfcn lkp_4
            ON        lkp_4.src_idntftn_val = sq_pcx_fopfeedandseed.classification_code
            left join lkp_xref_prty_asset lkp_5
            ON        lkp_5.prty_asset_sbtype_cd = v_assettype
            AND       lkp_5.asset_host_id_val = sq_pcx_fopfeedandseed.fixedid
            AND       lkp_5.prty_asset_clasfcn_cd = v_classification_code 
			qualify row_number() over (PARTITION BY sq_pcx_fopfeedandseed.source_record_id ORDER BY sq_pcx_fopfeedandseed.source_record_id) 
			= 1 );
  -- Component LKP_FD_AND_SD, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_fd_and_sd AS
  (
            SELECT    lkp.prty_asset_id,
                      lkp.fd_and_sd_type_cd,
                      lkp.fd_and_sd_desc,
                      lkp.fd_and_sd_vlu,
                      lkp.edw_end_dttm,
                      exp_fd_and_sd.source_record_id,
                      row_number() over(PARTITION BY exp_fd_and_sd.source_record_id ORDER BY lkp.prty_asset_id ASC,lkp.fd_and_sd_type_cd ASC,lkp.fd_and_sd_desc ASC,lkp.fd_and_sd_vlu ASC,lkp.edw_end_dttm ASC) rnk
            FROM      exp_fd_and_sd
            left join
                      (
                               SELECT   fd_and_sd.fd_and_sd_type_cd AS fd_and_sd_type_cd,
                                        fd_and_sd.fd_and_sd_desc    AS fd_and_sd_desc,
                                        fd_and_sd.fd_and_sd_vlu     AS fd_and_sd_vlu,
                                        fd_and_sd.edw_end_dttm      AS edw_end_dttm,
                                        fd_and_sd.prty_asset_id     AS prty_asset_id
                               FROM     db_t_prod_core.fd_and_sd qualify row_number() over(PARTITION BY fd_and_sd.prty_asset_id ORDER BY edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.prty_asset_id = exp_fd_and_sd.src_prty_asset_id 
			qualify row_number() over(PARTITION BY exp_fd_and_sd.source_record_id ORDER BY lkp.prty_asset_id ASC,lkp.fd_and_sd_type_cd ASC,lkp.fd_and_sd_desc ASC,lkp.fd_and_sd_vlu ASC,lkp.edw_end_dttm ASC) 
			= 1 );
  -- Component EXP_CDC_CHECK, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_cdc_check AS
  (
             SELECT     lkp_fd_and_sd.prty_asset_id     AS lkp_prty_asset_id,
                        lkp_fd_and_sd.fd_and_sd_type_cd AS lkp_fd_and_sd_type_cd,
                        lkp_fd_and_sd.fd_and_sd_desc    AS lkp_fd_and_sd_desc,
                        lkp_fd_and_sd.fd_and_sd_vlu     AS lkp_fd_and_sd_vlu,
                        lkp_fd_and_sd.edw_end_dttm      AS lkp_edw_end_dttm,
                        exp_fd_and_sd.src_prty_asset_id AS src_prty_asset_id,
                        exp_fd_and_sd.src_fstype        AS src_fstype,
                        exp_fd_and_sd.src_description   AS src_description,
                        exp_fd_and_sd.src_value         AS src_value,
                        exp_fd_and_sd.src_updatetime    AS src_updatetime,
                        exp_fd_and_sd.prcs_id           AS prcs_id,
                        exp_fd_and_sd.edw_strt_dttm     AS edw_strt_dttm,
                        exp_fd_and_sd.edw_end_dttm      AS edw_end_dttm,
                        exp_fd_and_sd.trans_end_dttm    AS trans_end_dttm,
                        md5 ( ltrim ( rtrim ( exp_fd_and_sd.src_fstype ) )
                                   || ltrim ( rtrim ( exp_fd_and_sd.src_description ) )
                                   || ltrim ( rtrim ( exp_fd_and_sd.src_value ) ) ) AS v_src_md5,
                        md5 ( ltrim ( rtrim ( lkp_fd_and_sd.fd_and_sd_type_cd ) )
                                   || ltrim ( rtrim ( lkp_fd_and_sd.fd_and_sd_desc ) )
                                   || ltrim ( rtrim ( lkp_fd_and_sd.fd_and_sd_vlu ) ) ) AS v_tgt_md5,
                        CASE
                                   WHEN v_tgt_md5 IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_src_md5 = v_tgt_md5 THEN ''R''
                                                         ELSE ''U''
                                              END
                        END AS o_flag,
                        exp_fd_and_sd.source_record_id
             FROM       exp_fd_and_sd
             inner join lkp_fd_and_sd
             ON         exp_fd_and_sd.source_record_id = lkp_fd_and_sd.source_record_id );
  -- Component RTRTRANS_INS_UPD, Type ROUTER Output Group INS_UPD
  create or replace temporary table rtrtrans_ins_upd as
  SELECT exp_cdc_check.lkp_prty_asset_id     AS lkp_prty_asset_id,
         exp_cdc_check.lkp_fd_and_sd_type_cd AS lkp_fd_and_sd_type_cd,
         exp_cdc_check.lkp_fd_and_sd_desc    AS lkp_fd_and_sd_desc,
         exp_cdc_check.lkp_fd_and_sd_vlu     AS lkp_fd_and_sd_vlu,
         exp_cdc_check.lkp_edw_end_dttm      AS lkp_edw_end_dttm,
         exp_cdc_check.src_prty_asset_id     AS src_prty_asset_id,
         exp_cdc_check.src_fstype            AS src_fstype,
         exp_cdc_check.src_description       AS src_description,
         exp_cdc_check.src_value             AS src_value,
         exp_cdc_check.src_updatetime        AS src_updatetime,
         exp_cdc_check.prcs_id               AS prcs_id,
         exp_cdc_check.edw_strt_dttm         AS edw_strt_dttm,
         exp_cdc_check.edw_end_dttm          AS edw_end_dttm,
         exp_cdc_check.trans_end_dttm        AS trans_end_dttm,
         exp_cdc_check.o_flag                AS o_flag,
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
                rtrtrans_ins_upd.src_fstype        AS src_fstype,
                rtrtrans_ins_upd.src_description   AS src_description,
                rtrtrans_ins_upd.src_value         AS src_value,
                rtrtrans_ins_upd.prcs_id           AS prcs_id,
                rtrtrans_ins_upd.edw_strt_dttm     AS edw_strt_dttm,
                rtrtrans_ins_upd.edw_end_dttm      AS edw_end_dttm,
                rtrtrans_ins_upd.src_updatetime    AS trans_strt_dttm,
                rtrtrans_ins_upd.trans_end_dttm    AS trans_end_dttm,
                0                                  AS update_strategy_action,
				source_record_id
         FROM   rtrtrans_ins_upd );
  -- Component FD_AND_SD, Type TARGET
  INSERT INTO db_t_prod_core.fd_and_sd
              (
                          prty_asset_id,
                          fd_and_sd_type_cd,
                          fd_and_sd_desc,
                          fd_and_sd_vlu,
                          prcs_id,
                          trans_strt_dttm,
                          trans_end_dttm,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT updtrans.src_prty_asset_id AS prty_asset_id,
         updtrans.src_fstype        AS fd_and_sd_type_cd,
         updtrans.src_description   AS fd_and_sd_desc,
         updtrans.src_value         AS fd_and_sd_vlu,
         updtrans.prcs_id           AS prcs_id,
         updtrans.trans_strt_dttm   AS trans_strt_dttm,
         updtrans.trans_end_dttm    AS trans_end_dttm,
         updtrans.edw_strt_dttm     AS edw_strt_dttm,
         updtrans.edw_end_dttm      AS edw_end_dttm
  FROM   updtrans;
  
  -- Component FD_AND_SD, Type Post SQL
  UPDATE db_t_prod_core.fd_and_sd
  SET    edw_end_dttm=a.lead1,
         trans_end_dttm=a.lead2
  FROM   (
                         SELECT DISTINCT prty_asset_id,
                                         trans_strt_dttm,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY prty_asset_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)                       - interval ''1 second'' AS lead1,
                                         max(trans_strt_dttm) over (PARTITION BY prty_asset_id ORDER BY trans_strt_dttm ASC,edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead2
                         FROM            db_t_prod_core.fd_and_sd ) a

  WHERE  fd_and_sd.edw_strt_dttm = a.edw_strt_dttm
  AND    fd_and_sd.trans_strt_dttm = a.trans_strt_dttm
  AND    fd_and_sd.prty_asset_id=a.prty_asset_id
  AND    cast(fd_and_sd.edw_end_dttm AS DATE) = ''9999-12-31''
  AND    lead1 IS NOT NULL;

END;
';