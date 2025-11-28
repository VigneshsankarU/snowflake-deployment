-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_LVSTCK_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE START_DTTM timestamp;
END_DTTM timestamp;
PRCS_ID varchar;
BEGIN 

START_DTTM:=(select current_timestamp); 
END_DTTM:=(select current_date + 1); 
PRCS_ID:=''1''; 
  -- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_asset_clasfcn AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRTY_ASSET_CLASFCN''
         AND    teradata_etl_ref_xlat.src_idntftn_nm  IN ( ''derived'')-- ,''pcx_holineschcovitemcov_alfa.ChoiceTerm1'', ''cctl_contentlineitemschedule'')
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
  -- Component SQ_pcx_foplivestock, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pcx_foplivestock AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS fixedid_stg,
                $2 AS assettype,
                $3 AS classification_code,
                $4 AS typecode_stg,
                $5 AS descorbreed_stg,
                $6 AS costperani_stg,
                $7 AS numofhead_stg,
                $8 AS updatetime_stg,
                $9 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT cast(pcx_foplivestock.fixedid_stg AS VARCHAR(100))AS id ,
                                                                  cast(''PRTY_ASSET_SBTYPE35'' AS        VARCHAR(50)) AS assettype ,
                                                                  cast(''PRTY_ASSET_CLASFCN14'' AS       VARCHAR(50)) AS classification_code,
                                                                  ltrim(rtrim(typecode_stg)) ,
                                                                  ltrim(rtrim(descorbreed_stg)),
                                                                  ltrim(rtrim(costperani_stg)),
                                                                  ltrim(rtrim(numofhead_stg )),
                                                                  CASE
                                                                                  WHEN (
                                                                                                                  pcx_foplivestock.updatetime_stg>pc_policyperiod.updatetime_stg) THEN pcx_foplivestock.updatetime_stg
                                                                                  ELSE pc_policyperiod.updatetime_stg
                                                                  END updatetime
                                                  FROM            db_t_prod_stag.pcx_foplivestock
                                                  left join       db_t_prod_stag.pctl_foplivestocktype
                                                  ON              pctl_foplivestocktype.id_stg = pcx_foplivestock.lstype_stg
                                                  join            db_t_prod_stag.pc_policyperiod
                                                  ON              pcx_foplivestock.branchid_stg =pc_policyperiod.id_stg
                                                  join            db_t_prod_stag.pc_job
                                                  ON              pc_policyperiod.jobid_stg=pc_job.id_stg
                                                  WHERE           ltrim(rtrim(typecode_stg)) IS NOT NULL
                                                  AND             (
                                                                                  expirationdate_stg IS NULL
                                                                  OR              expirationdate_stg>editeffectivedate_stg)
                                                  AND             ((
                                                                                                  pcx_foplivestock.updatetime_stg>:start_dttm
                                                                                  OR              pcx_foplivestock.updatetime_stg<=:end_dttm)
                                                                  OR              (
                                                                                                  pc_policyperiod.updatetime_stg>:start_dttm
                                                                                  OR              pc_policyperiod.updatetime_stg<=:end_dttm)) qualify row_number() over(PARTITION BY fixedid_stg ORDER BY coalesce(expirationdate_stg , cast(''9999-12-31 23:59:59.999999'' AS timestamp)) DESC,updatetime DESC,pcx_foplivestock.createtime_stg DESC)=1 ) src ) );
  -- Component exp_hold_data, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_hold_data AS
  (
            SELECT    sq_pcx_foplivestock.fixedid_stg     AS fixedid_stg,
                      sq_pcx_foplivestock.typecode_stg    AS typecode_stg,
                      sq_pcx_foplivestock.descorbreed_stg AS descorbreed_stg,
                      sq_pcx_foplivestock.costperani_stg  AS costperani_stg,
                      sq_pcx_foplivestock.numofhead_stg   AS numofhead_stg,
                      sq_pcx_foplivestock.updatetime_stg  AS updatetime_stg,
                      lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */
                      AS lkp_asset_type,
                      lkp_2.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */
                      AS lkp_classification_code,
                      sq_pcx_foplivestock.source_record_id,
                      row_number() over (PARTITION BY sq_pcx_foplivestock.source_record_id ORDER BY sq_pcx_foplivestock.source_record_id) AS rnk
            FROM      sq_pcx_foplivestock
            left join lkp_teradata_etl_ref_xlat_asset_sbtype lkp_1
            ON        lkp_1.src_idntftn_val = sq_pcx_foplivestock.assettype
            left join lkp_teradata_etl_ref_xlat_asset_clasfcn lkp_2
            ON        lkp_2.src_idntftn_val = sq_pcx_foplivestock.classification_code qualify rnk = 1 );
  -- Component LKP_PRTY_ASSET, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_prty_asset AS
  (
            SELECT    lkp.prty_asset_id,
                      exp_hold_data.source_record_id,
                      row_number() over(PARTITION BY exp_hold_data.source_record_id ORDER BY lkp.prty_asset_id ASC,lkp.prty_asset_sbtype_cd ASC,lkp.asset_host_id_val ASC,lkp.prty_asset_clasfcn_cd ASC) rnk
            FROM      exp_hold_data
            left join
                      (
                               SELECT   prty_asset.prty_asset_id         AS prty_asset_id,
                                        prty_asset.asset_host_id_val     AS asset_host_id_val,
                                        prty_asset.prty_asset_sbtype_cd  AS prty_asset_sbtype_cd,
                                        prty_asset.prty_asset_clasfcn_cd AS prty_asset_clasfcn_cd
                               FROM     db_t_prod_core.prty_asset prty_asset 
							   qualify row_number() over(PARTITION BY asset_host_id_val,prty_asset_sbtype_cd,prty_asset_clasfcn_cd ORDER BY edw_end_dttm DESC) = 1
                                        
                      ) lkp
            ON        lkp.asset_host_id_val = exp_hold_data.fixedid_stg
            AND       lkp.prty_asset_sbtype_cd = exp_hold_data.lkp_asset_type
            AND       lkp.prty_asset_clasfcn_cd = exp_hold_data.lkp_classification_code qualify rnk = 1 );
  -- Component exp_to_get_PRTY_ASSET_ID, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_to_get_prty_asset_id AS
  (
             SELECT     lkp_prty_asset.prty_asset_id                                           AS prty_asset_id,
                        exp_hold_data.typecode_stg                                             AS typecode_stg,
                        exp_hold_data.descorbreed_stg                                          AS descorbreed_stg,
                        exp_hold_data.costperani_stg                                           AS costperani_stg,
                        exp_hold_data.numofhead_stg                                            AS numofhead_stg,
                        :prcs_id                                                               AS prcs_id,
                        current_timestamp                                                      AS out_edw_strt_dttm,
                        exp_hold_data.updatetime_stg                                           AS updatetime_stg,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_trans_end_dttm,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm1,
                        exp_hold_data.source_record_id
             FROM       exp_hold_data
             inner join lkp_prty_asset
             ON         exp_hold_data.source_record_id = lkp_prty_asset.source_record_id );
  -- Component LKP_LVSTCK, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_lvstck AS
  (
            SELECT    lkp.prty_asset_id,
                      lkp.lvstck_type_cd,
                      lkp.breed_desc,
                      lkp.cost_per_head,
                      lkp.num_of_head,
                      lkp.prcs_id,
                      exp_to_get_prty_asset_id.source_record_id,
                      row_number() over(PARTITION BY exp_to_get_prty_asset_id.source_record_id ORDER BY lkp.prty_asset_id ASC,lkp.lvstck_type_cd ASC,lkp.breed_desc ASC,lkp.cost_per_head ASC,lkp.num_of_head ASC,lkp.prcs_id ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.trans_strt_dttm ASC,lkp.trans_end_dttm ASC) rnk
            FROM      exp_to_get_prty_asset_id
            left join
                      (
                               SELECT   lvstck.lvstck_type_cd  AS lvstck_type_cd,
                                        lvstck.breed_desc      AS breed_desc,
                                        lvstck.cost_per_head   AS cost_per_head,
                                        lvstck.num_of_head     AS num_of_head,
                                        lvstck.prcs_id         AS prcs_id,
                                        lvstck.edw_strt_dttm   AS edw_strt_dttm,
                                        lvstck.edw_end_dttm    AS edw_end_dttm,
                                        lvstck.trans_strt_dttm AS trans_strt_dttm,
                                        lvstck.trans_end_dttm  AS trans_end_dttm,
                                        lvstck.prty_asset_id   AS prty_asset_id
                               FROM     db_t_prod_core.lvstck qualify row_number() over(PARTITION BY prty_asset_id ORDER BY edw_end_dttm DESC)=1
                                        /*  */
                      ) lkp
            ON        lkp.prty_asset_id = exp_to_get_prty_asset_id.prty_asset_id qualify rnk = 1 );
  -- Component exp_CDC_DATA, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_cdc_data AS
  (
             SELECT     exp_to_get_prty_asset_id.prty_asset_id   AS prty_asset_id,
                        exp_to_get_prty_asset_id.typecode_stg    AS typecode_stg,
                        exp_to_get_prty_asset_id.descorbreed_stg AS descorbreed_stg,
                        exp_to_get_prty_asset_id.costperani_stg  AS costperani_stg,
                        exp_to_get_prty_asset_id.numofhead_stg   AS numofhead_stg,
                        exp_to_get_prty_asset_id.updatetime_stg  AS updatetime_stg,
                        md5 ( exp_to_get_prty_asset_id.typecode_stg
                                   || exp_to_get_prty_asset_id.descorbreed_stg
                                   || exp_to_get_prty_asset_id.costperani_stg
                                   || exp_to_get_prty_asset_id.numofhead_stg ) AS check_in,
                        md5 ( lkp_lvstck.lvstck_type_cd
                                   || lkp_lvstck.breed_desc
                                   || lkp_lvstck.cost_per_head
                                   || lkp_lvstck.num_of_head ) AS check_out,
                        CASE
                                   WHEN lkp_lvstck.prty_asset_id IS NULL THEN ''I''
                                   ELSE (
                                              CASE
                                                         WHEN (
                                                                               check_in <> check_out ) THEN ''U''
                                                         ELSE ''R''
                                              END )
                        END                                         AS out_flag,
                        exp_to_get_prty_asset_id.prcs_id            AS prcs_id1,
                        exp_to_get_prty_asset_id.out_edw_strt_dttm  AS out_edw_strt_dttm,
                        exp_to_get_prty_asset_id.out_trans_end_dttm AS out_trans_end_dttm,
                        exp_to_get_prty_asset_id.out_edw_end_dttm1  AS out_edw_end_dttm1,
                        exp_to_get_prty_asset_id.source_record_id
             FROM       exp_to_get_prty_asset_id
             inner join lkp_lvstck
             ON         exp_to_get_prty_asset_id.source_record_id = lkp_lvstck.source_record_id );
  -- Component fltr_data, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE fltr_data AS
  (
         SELECT exp_cdc_data.prty_asset_id      AS prty_asset_id,
                exp_cdc_data.typecode_stg       AS typecode_stg,
                exp_cdc_data.descorbreed_stg    AS descorbreed_stg,
                exp_cdc_data.costperani_stg     AS costperani_stg,
                exp_cdc_data.numofhead_stg      AS numofhead_stg,
                exp_cdc_data.prcs_id1           AS prcs_id,
                exp_cdc_data.out_edw_strt_dttm  AS edw_strt_dttm,
                exp_cdc_data.out_edw_end_dttm1  AS edw_end_dttm,
                exp_cdc_data.updatetime_stg     AS trans_strt_dttm,
                exp_cdc_data.out_trans_end_dttm AS trans_end_dttm,
                exp_cdc_data.out_flag           AS out_flag,
                exp_cdc_data.source_record_id
         FROM   exp_cdc_data
         WHERE exp_cdc_data.out_flag IN ( 
                    ''I'' ,
                    ''U'' ) );
  -- Component LVSTCK, Type TARGET
  INSERT INTO db_t_prod_core.lvstck
              (
                          prty_asset_id,
                          lvstck_type_cd,
                          breed_desc,
                          cost_per_head,
                          num_of_head,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT nvl(fltr_data.prty_asset_id ,''1'')  AS prty_asset_id,
         nvl(fltr_data.typecode_stg ,''1'')   AS lvstck_type_cd,
         nvl(fltr_data.descorbreed_stg,''1'') AS breed_desc,
         fltr_data.costperani_stg  AS cost_per_head,
         fltr_data.numofhead_stg   AS num_of_head,
         nvl(fltr_data.prcs_id  ,''1'')       AS prcs_id,
         fltr_data.edw_strt_dttm   AS edw_strt_dttm,
         fltr_data.edw_end_dttm    AS edw_end_dttm,
         fltr_data.trans_strt_dttm AS trans_strt_dttm,
         fltr_data.trans_end_dttm  AS trans_end_dttm
  FROM   fltr_data;
  
  -- Component LVSTCK, Type Post SQL
  UPDATE db_t_prod_core.lvstck
    SET    edw_end_dttm=a.edw_end_dttm_new,
         trans_end_dttm=a.trans_end_dttm_new
  FROM   (
                         SELECT DISTINCT prty_asset_id,
                                         trans_strt_dttm,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY prty_asset_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)                         - interval ''1 second'' AS edw_end_dttm_new,
                                         max(trans_strt_dttm) over (PARTITION BY prty_asset_id ORDER BY trans_strt_dttm ASC , edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS trans_end_dttm_new
                         FROM            db_t_prod_core.lvstck ) a

  WHERE  lvstck.edw_strt_dttm = a.edw_strt_dttm
  AND    lvstck.trans_strt_dttm = a.trans_strt_dttm
  AND    lvstck.prty_asset_id=a.prty_asset_id
  AND    cast(lvstck.edw_end_dttm AS DATE)=''9999-12-31''
  AND    edw_end_dttm_new IS NOT NULL;

END;
';