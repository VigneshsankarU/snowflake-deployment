-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_ASSET_VAL_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
declare		
run_id varchar;
start_dttm timestamp;
end_dttm timestamp;
prcs_id int;
DWELLFIXEDFILTER STRING;

BEGIN 
 run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
 PRCS_ID:=     (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
DWELLFIXEDFILTER :=  (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''DWELLFIXEDFILTER'' order by insert_ts desc limit 1);

  -- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_asset_clasfcn AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRTY_ASSET_CLASFCN''
         AND    teradata_etl_ref_xlat.src_idntftn_nm IN ( ''derived'' ,
                                                         ''pcx_holineschcovitemcov_alfa.ChoiceTerm1'',
                                                         ''contentlineitemschedule.typecode'',
                                                         ''pctl_bp7classificationproperty.typecode'')
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
  -- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_VALUT_AMT_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_asset_valut_amt_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''ASSET_VALUT_AMT_TYPE''
                --             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived''
                --  AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_sys_src_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys= ''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component sq_pcx_dwellingcov_hoe, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pcx_dwellingcov_hoe AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS rank,
                $2  AS fixedid,
                $3  AS TYPE,
                $4  AS class_cd,
                $5  AS sys_src_cd,
                $6  AS effectivedate,
                $7  AS expirationdate,
                $8  AS asset_valut_amt_cd,
                $9  AS asset_val_amt,
                $10 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT rank() over (PARTITION BY id,type_cd,classification_code,asset_valut_amt_cd ORDER BY effective_date,expiration_date, asset_val_amt ) rk,
                                                                  a.*
                                                  FROM            (
                                                                                  SELECT DISTINCT dwell.fixedid_stg                                  AS id,
                                                                                                  cast(''PRTY_ASSET_SBTYPE5'' AS  VARCHAR(100))        AS type_cd,
                                                                                                  cast(''PRTY_ASSET_CLASFCN1'' AS VARCHAR(100))        AS classification_code,
                                                                                                  ''SRC_SYS4''                                         AS src_cd,
                                                                                                  to_date(''01/01/1900'', ''dd/mm/yyyy'')     AS effective_date,
                                                                                                  cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS expiration_date,
                                                                                                  cast( ''DirectTerm4'' AS VARCHAR(50))                AS asset_valut_amt_cd ,
                                                                                                  max(dwell.directterm4_stg)                         AS asset_val_amt
                                                                                  FROM            (
                                                                                                                  SELECT          pcx_dwelling_hoe.fixedid_stg,
                                                                                                                                  pcx_dwellingcov_hoe.directterm4_stg
                                                                                                                  FROM            db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                                  left outer join db_t_prod_stag.pcx_dwelling_hoe
                                                                                                                  ON              pcx_dwelling_hoe.fixedid_stg = pcx_dwellingcov_hoe.dwelling_stg
                                                                                                                  WHERE           pcx_dwellingcov_hoe.updatetime_stg > (:start_dttm)
                                                                                                                  AND             pcx_dwellingcov_hoe.updatetime_stg <= (:end_dttm) )dwell
                                                                                  WHERE           dwell.directterm4_stg IS NOT NULL
                                                                                  AND             (
                                                                                                                  dwell.fixedid_stg IS NOT NULL
                                                                                                  AND             dwell.fixedid_stg <> 0)
                                                                                  AND             :DWELLFIXEDFILTER
                                                                                  GROUP BY        dwell.fixedid_stg,
                                                                                                  effective_date,
                                                                                                  expiration_date
                                                                                  UNION
                                                                                  SELECT DISTINCT pcx_dwelling_hoe.fixedid                                         AS id,
                                                                                                  ''PRTY_ASSET_SBTYPE5''                                             AS type_cd,
                                                                                                  ''PRTY_ASSET_CLASFCN1''                                            AS classification_code,
                                                                                                  ''SRC_SYS4''                                                       AS src_cd,
                                                                                                  to_date(pcx_dwelling_hoe.effectivedate) AS effective_date,
                                                                                                  cast(pcx_dwelling_hoe.expirationdate AS timestamp(6))            AS expiration_date,
                                                                                                  cast( ''ReplacementCost'' AS VARCHAR(50))                          AS asset_valut_amt_cd ,
                                                                                                  max(replacementcost)                                             AS asset_val_amt
                                                                                  FROM            (
                                                                                                                  SELECT DISTINCT pcx_dwelling_hoe.fixedid_stg AS fixedid,
                                                                                                                                  CASE
                                                                                                                                                  WHEN pcx_dwelling_hoe.effectivedate_stg IS NULL THEN pc_policyperiod.periodstart_stg
                                                                                                                                                  WHEN pcx_dwelling_hoe.effectivedate_stg IS NOT NULL THEN pcx_dwelling_hoe.effectivedate_stg
                                                                                                                                  END AS effectivedate,
                                                                                                                                  CASE
                                                                                                                                                  WHEN pcx_dwelling_hoe.expirationdate_stg IS NULL THEN pc_policyperiod.periodend_stg
                                                                                                                                                  WHEN pcx_dwelling_hoe.expirationdate_stg IS NOT NULL THEN pcx_dwelling_hoe.expirationdate_stg
                                                                                                                                  END                                  AS expirationdate,
                                                                                                                                  pcx_dwelling_hoe.replacementcost_stg AS replacementcost
                                                                                                                  FROM            db_t_prod_stag.pcx_dwelling_hoe
                                                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                                                  ON              pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg
                                                                                                                  left outer join db_t_prod_stag.pc_policy
                                                                                                                  ON              pc_policy.id_stg=pc_policyperiod.policyid_stg
                                                                                                                  left outer join
                                                                                                                                  (
                                                                                                                                             SELECT     pcx_dwelling_hoe.fixedid_stg  AS homealerfixedid_stg,
                                                                                                                                                        pcx_dwelling_hoe.branchid_stg AS branchid_stg,
                                                                                                                                                        homealertcode_stg             AS homealert_cd,
                                                                                                                                                        hurrmitigationcreditamt_stg
                                                                                                                                             FROM       db_t_prod_stag.pc_policyperiod
                                                                                                                                             inner join db_t_prod_stag.pcx_dwelling_hoe
                                                                                                                                             ON         pcx_dwelling_hoe.branchid_stg=pc_policyperiod.id_stg
                                                                                                                                             inner join db_t_prod_stag.pcx_dwellingratingfactor_alfa
                                                                                                                                             ON         pcx_dwelling_hoe.fixedid_stg = pcx_dwellingratingfactor_alfa.dwelling_hoe_stg
                                                                                                                                             AND        pcx_dwellingratingfactor_alfa.branchid_stg=pc_policyperiod.id_stg
                                                                                                                                             WHERE      pcx_dwelling_hoe.expirationdate_stg IS NULL
                                                                                                                                             AND        pcx_dwellingratingfactor_alfa.expirationdate_stg IS NULL ) homealert
                                                                                                                  ON              pcx_dwelling_hoe.fixedid_stg=homealert.homealerfixedid_stg
                                                                                                                  AND             pcx_dwelling_hoe.branchid_stg=homealert.branchid_stg
                                                                                                                  join            db_t_prod_stag.pcx_holocation_hoe
                                                                                                                  ON              pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.id_stg
                                                                                                                  WHERE           pcx_dwelling_hoe.expirationdate_stg IS NULL
                                                                                                                  AND             pcx_dwelling_hoe.updatetime_stg>(:start_dttm)
                                                                                                                  AND             pcx_dwelling_hoe.updatetime_stg <= (:end_dttm) ) pcx_dwelling_hoe
                                                                                  WHERE           pcx_dwelling_hoe.fixedid IS NOT NULL
                                                                                  AND             replacementcost IS NOT NULL
                                                                                  GROUP BY        fixedid,
                                                                                                  effective_date,
                                                                                                  expiration_date
                                                                                  UNION
                                                                                  SELECT DISTINCT pcx_dwelling_hoe.fixedid                                              AS id,
                                                                                                  ''PRTY_ASSET_SBTYPE5''                                                  AS type_cd,
                                                                                                  ''PRTY_ASSET_CLASFCN1''                                                 AS classification_code,
                                                                                                  ''SRC_SYS4''                                                            AS src_cd,
                                                                                                  to_date(pcx_dwelling_hoe.appraisaldate_alfa ) AS effective_date,
                                                                                                  cast(pcx_dwelling_hoe.expirationdate AS timestamp(6))                 AS expiration_date,
                                                                                                  cast( ''ASSET_VALUT_AMT_TYPE1'' AS VARCHAR(50))                         AS asset_valut_amt_cd ,
                                                                                                  max(replacementcost)                                                  AS asset_val_amt
                                                                                  FROM            (
                                                                                                                  SELECT DISTINCT pcx_dwelling_hoe.fixedid_stg            AS fixedid,
                                                                                                                                  pcx_dwelling_hoe.appraisaldate_alfa_stg AS appraisaldate_alfa,
                                                                                                                                  CASE
                                                                                                                                                  WHEN pcx_dwelling_hoe.expirationdate_stg IS NULL THEN pc_policyperiod.periodend_stg
                                                                                                                                                  WHEN pcx_dwelling_hoe.expirationdate_stg IS NOT NULL THEN pcx_dwelling_hoe.expirationdate_stg
                                                                                                                                  END                                  AS expirationdate,
                                                                                                                                  pcx_dwelling_hoe.replacementcost_stg AS replacementcost
                                                                                                                  FROM            db_t_prod_stag.pcx_dwelling_hoe
                                                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                                                  ON              pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg
                                                                                                                  left outer join db_t_prod_stag.pc_policy
                                                                                                                  ON              pc_policy.id_stg=pc_policyperiod.policyid_stg
                                                                                                                  left outer join
                                                                                                                                  (
                                                                                                                                             SELECT     pcx_dwelling_hoe.fixedid_stg  AS homealerfixedid_stg,
                                                                                                                                                        pcx_dwelling_hoe.branchid_stg AS branchid_stg,
                                                                                                                                                        homealertcode_stg             AS homealert_cd,
                                                                                                                                                        hurrmitigationcreditamt_stg
                                                                                                                                             FROM       db_t_prod_stag.pc_policyperiod
                                                                                                                                             inner join db_t_prod_stag.pcx_dwelling_hoe
                                                                                                                                             ON         pcx_dwelling_hoe.branchid_stg=pc_policyperiod.id_stg
                                                                                                                                             inner join db_t_prod_stag.pcx_dwellingratingfactor_alfa
                                                                                                                                             ON         pcx_dwelling_hoe.fixedid_stg = pcx_dwellingratingfactor_alfa.dwelling_hoe_stg
                                                                                                                                             AND        pcx_dwellingratingfactor_alfa.branchid_stg=pc_policyperiod.id_stg
                                                                                                                                             WHERE      pcx_dwelling_hoe.expirationdate_stg IS NULL
                                                                                                                                             AND        pcx_dwellingratingfactor_alfa.expirationdate_stg IS NULL ) homealert
                                                                                                                  ON              pcx_dwelling_hoe.fixedid_stg=homealert.homealerfixedid_stg
                                                                                                                  AND             pcx_dwelling_hoe.branchid_stg=homealert.branchid_stg
                                                                                                                  join            db_t_prod_stag.pcx_holocation_hoe
                                                                                                                  ON              pcx_dwelling_hoe.holocation_stg = pcx_holocation_hoe.id_stg
                                                                                                                  WHERE           pcx_dwelling_hoe.expirationdate_stg IS NULL
                                                                                                                  AND             pcx_dwelling_hoe.updatetime_stg>(:start_dttm)
                                                                                                                  AND             pcx_dwelling_hoe.updatetime_stg <= (:end_dttm) ) pcx_dwelling_hoe
                                                                                  WHERE           pcx_dwelling_hoe.fixedid IS NOT NULL
                                                                                  AND             replacementcost IS NOT NULL
                                                                                  AND             appraisaldate_alfa IS NOT NULL
                                                                                  GROUP BY        fixedid,
                                                                                                  effective_date,
                                                                                                  expiration_date
                                                                                  UNION
                                                                                  SELECT veh.fixedid_stg,
                                                                                         ''PRTY_ASSET_SBTYPE4''                               AS type_cd,
                                                                                         ''PRTY_ASSET_CLASFCN3''                              AS classification_code,
                                                                                         ''SRC_SYS4''                                         AS src_cd,
                                                                                         to_date(''01/01/1900'', ''dd/mm/yyyy'')     AS effective_date,
                                                                                         cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS expiration_date,
                                                                                         cast( ''ASSET_VALUT_AMT_TYPE2'' AS VARCHAR(50))      AS asset_valut_amt_cd ,
                                                                                         veh.statedvalue_stg
                                                                                  FROM   (
                                                                                                SELECT fixedid_stg,
                                                                                                       statedvalue_stg
                                                                                                FROM   db_t_prod_stag.pc_personalvehicle
                                                                                                WHERE  pc_personalvehicle.updatetime_stg> (:start_dttm)
                                                                                                AND    pc_personalvehicle.updatetime_stg <= (:end_dttm)
                                                                                                AND    (
                                                                                                              pc_personalvehicle.expirationdate_stg IS NULL
                                                                                                       OR     pc_personalvehicle.expirationdate_stg >:start_dttm) )veh
                                                                                  WHERE  veh.statedvalue_stg IS NOT NULL
                                                                                  UNION
                                                                                  SELECT b.fixedid_stg,
                                                                                         ''PRTY_ASSET_SBTYPE4''                               AS type_cd,
                                                                                         ''PRTY_ASSET_CLASFCN3''                              AS classification_code,
                                                                                         ''SRC_SYS4''                                         AS src_cd,
                                                                                         to_date(''01/01/1900'', ''dd/mm/yyyy'')     AS effective_date,
                                                                                         cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS expiration_date,
                                                                                         cast( ''ASSET_VALUT_AMT_TYPE3'' AS VARCHAR(50))      AS asset_valut_amt_cd ,
                                                                                         b.costnew_stg
                                                                                  FROM   (
                                                                                                SELECT fixedid_stg,
                                                                                                       costnew_stg
                                                                                                FROM   db_t_prod_stag.pc_personalvehicle
                                                                                                WHERE  pc_personalvehicle.updatetime_stg> (:start_dttm)
                                                                                                AND    pc_personalvehicle.updatetime_stg <= (:end_dttm)
                                                                                                AND    (
                                                                                                              pc_personalvehicle.expirationdate_stg IS NULL
                                                                                                       OR     pc_personalvehicle.expirationdate_stg >:start_dttm) )b
                                                                                  WHERE  b.costnew_stg IS NOT NULL
                                                                                  UNION
                                                                                  /* ---- EIM-49110 FARM CHANGES */
                                                                                  SELECT   pcx_fopoutbuilding.fixedid_stg                     AS id,
                                                                                           ''PRTY_ASSET_SBTYPE36''                              AS type_cd,
                                                                                           ''PRTY_ASSET_CLASFCN13''                             AS classification_code,
                                                                                           ''SRC_SYS4''                                         AS src_cd,
                                                                                           to_date(''01/01/1900'' , ''dd/mm/yyyy'')     AS effective_date,
                                                                                           cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS expiration_date,
                                                                                           cast( ''ReplacementCost'' AS                    VARCHAR(50))            AS asset_valut_amt_cd ,
                                                                                           cast(pcx_fopoutbuilding.replcostvalamt_stg AS DECIMAL(18,2))          AS asset_val_amt
                                                                                  FROM     db_t_prod_stag.pcx_fopoutbuilding
                                                                                  join     db_t_prod_stag.pc_policyperiod
                                                                                  ON       pcx_fopoutbuilding.branchid_stg=pc_policyperiod.id_stg
                                                                                  WHERE    pcx_fopoutbuilding.replcostvalamt_stg IS NOT NULL
                                                                                  AND      (
                                                                                                    pcx_fopoutbuilding.updatetime_stg> (:start_dttm)
                                                                                           AND      pcx_fopoutbuilding.updatetime_stg <= (:end_dttm)
                                                                                           OR       (
                                                                                                             pc_policyperiod.updatetime_stg>(:start_dttm)
                                                                                                    AND      pc_policyperiod.updatetime_stg<=(:end_dttm)))
                                                                                  AND      (
                                                                                                    pcx_fopoutbuilding.expirationdate_stg IS NULL
                                                                                           OR       pcx_fopoutbuilding.expirationdate_stg >pc_policyperiod.editeffectivedate_stg) qualify rank() over(PARTITION BY id,type_cd ORDER BY coalesce(pcx_fopoutbuilding.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC, pcx_fopoutbuilding.updatetime_stg DESC,pcx_fopoutbuilding.createtime_stg DESC)=1 ) AS a
                                                  ORDER BY        id,
                                                                  type_cd,
                                                                  classification_code,
                                                                  src_cd,
                                                                  asset_valut_amt_cd,
                                                                  effective_date,
                                                                  expiration_date ASC ) src ) );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
            SELECT
                      CASE
                                WHEN sq_pcx_dwellingcov_hoe.asset_val_amt IS NULL THEN 0
                                ELSE sq_pcx_dwellingcov_hoe.asset_val_amt
                      END AS asset_val_amt1,
                      CASE
                                WHEN sq_pcx_dwellingcov_hoe.effectivedate IS NULL THEN to_date ( ''01/01/1900'' , ''mm/dd/yyyy'' )
                                ELSE sq_pcx_dwellingcov_hoe.effectivedate
                      END AS o_effectivedate1,
                      decode ( TRUE ,
                              lkp_1.tgt_idntftn_val
                              /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_VALUT_AMT_CD */
                              IS NULL , ''UNK'' ,
                              lkp_2.tgt_idntftn_val
                              /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_VALUT_AMT_CD */
                              )                                  AS o_asset_valut_amt_cd,
                      :prcs_id                                   AS prcs_id,
                      to_char ( sq_pcx_dwellingcov_hoe.fixedid ) AS var_fixedid,
                      lkp_3.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */
                      AS o_type,
                      lkp_4.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */
                                  AS o_class_cd,
                      var_fixedid AS out_fixedid,
                      lkp_5.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD */
                      AS o_sys_src_cd,
                      CASE
                                WHEN sq_pcx_dwellingcov_hoe.expirationdate IS NULL THEN to_timestamp_ntz( ''12/31/9999 23:59:59.999999''  , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                                ELSE sq_pcx_dwellingcov_hoe.expirationdate
                      END                                                                    AS o_expirationdate,
                      to_timestamp_ntz( ''12/31/9999 23:59:59.999999''  , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                      sq_pcx_dwellingcov_hoe.rank                                            AS rank,
                      sq_pcx_dwellingcov_hoe.source_record_id,
                      row_number() over (PARTITION BY sq_pcx_dwellingcov_hoe.source_record_id ORDER BY sq_pcx_dwellingcov_hoe.source_record_id) AS rnk
            FROM      sq_pcx_dwellingcov_hoe
            left join lkp_teradata_etl_ref_xlat_asset_valut_amt_cd lkp_1
            ON        lkp_1.src_idntftn_val = sq_pcx_dwellingcov_hoe.asset_valut_amt_cd
            left join lkp_teradata_etl_ref_xlat_asset_valut_amt_cd lkp_2
            ON        lkp_2.src_idntftn_val = sq_pcx_dwellingcov_hoe.asset_valut_amt_cd
            left join lkp_teradata_etl_ref_xlat_asset_sbtype lkp_3
            ON        lkp_3.src_idntftn_val = sq_pcx_dwellingcov_hoe.TYPE
            left join lkp_teradata_etl_ref_xlat_asset_clasfcn lkp_4
            ON        lkp_4.src_idntftn_val = sq_pcx_dwellingcov_hoe.class_cd
            left join lkp_teradata_etl_ref_xlat_sys_src_cd lkp_5
            ON        lkp_5.src_idntftn_val = sq_pcx_dwellingcov_hoe.sys_src_cd 
			qualify row_number() over (PARTITION BY sq_pcx_dwellingcov_hoe.source_record_id ORDER BY sq_pcx_dwellingcov_hoe.source_record_id) 
			= 1 );
  -- Component LKP_PRTY_ASSET_ID, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_prty_asset_id AS
  (
            SELECT    lkp.prty_asset_id,
                      lkp.edw_end_dttm,
                      exp_data_transformation.source_record_id,
                      row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.prty_asset_id ASC,lkp.asset_host_id_val ASC,lkp.prty_asset_sbtype_cd ASC,lkp.prty_asset_clasfcn_cd ASC,lkp.asset_insrnc_hist_type_cd ASC,lkp.asset_desc ASC,lkp.prty_asset_name ASC,lkp.prty_asset_strt_dttm ASC,lkp.prty_asset_end_dttm ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.src_sys_cd ASC) rnk
            FROM      exp_data_transformation
            left join
                      (
                               SELECT   prty_asset.prty_asset_id             AS prty_asset_id,
                                        prty_asset.asset_insrnc_hist_type_cd AS asset_insrnc_hist_type_cd,
                                        prty_asset.asset_desc                AS asset_desc,
                                        prty_asset.prty_asset_name           AS prty_asset_name,
                                        prty_asset.edw_strt_dttm             AS edw_strt_dttm,
                                        prty_asset.edw_end_dttm              AS edw_end_dttm,
                                        prty_asset.asset_host_id_val         AS asset_host_id_val,
                                        prty_asset.prty_asset_sbtype_cd      AS prty_asset_sbtype_cd,
                                        prty_asset.prty_asset_clasfcn_cd     AS prty_asset_clasfcn_cd,
                                        prty_asset.src_sys_cd                AS src_sys_cd,
										prty_asset.prty_asset_strt_dttm	 	AS prty_asset_strt_dttm,
										prty_asset.prty_asset_end_dttm       AS prty_asset_end_dttm
                               FROM     db_t_prod_core.prty_asset qualify row_number() over(PARTITION BY asset_host_id_val,prty_asset_sbtype_cd,prty_asset_clasfcn_cd ORDER BY edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.asset_host_id_val = exp_data_transformation.out_fixedid
            AND       lkp.prty_asset_sbtype_cd = exp_data_transformation.o_type
            AND       lkp.prty_asset_clasfcn_cd = exp_data_transformation.o_class_cd 
			qualify row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.prty_asset_id ASC,lkp.asset_host_id_val ASC,lkp.prty_asset_sbtype_cd ASC,lkp.prty_asset_clasfcn_cd ASC,lkp.asset_insrnc_hist_type_cd ASC,lkp.asset_desc ASC,lkp.prty_asset_name ASC,lkp.prty_asset_strt_dttm ASC,lkp.prty_asset_end_dttm ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.src_sys_cd ASC) 
            			= 1 );
  -- Component LKP_ASSET_VAL, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_asset_val AS
  (
             SELECT     lkp.prty_asset_id,
                        lkp.asset_valut_amt_cd,
                        lkp.asset_val_amt,
                        lkp.asset_val_strt_dttm,
                        lkp.asset_val_end_dttm,
                        lkp_prty_asset_id.prty_asset_id              AS in_prty_asset_id,
                        exp_data_transformation.o_asset_valut_amt_cd AS in_asset_valut_amt_cd,
                        exp_data_transformation.o_effectivedate1     AS o_effectivedate1,
                        exp_data_transformation.prcs_id              AS prcs_id,
                        exp_data_transformation.o_expirationdate     AS o_expirationdate,
                        exp_data_transformation.asset_val_amt1       AS asset_val_amt1,
                        exp_data_transformation.edw_end_dttm         AS edw_end_dttm,
                        exp_data_transformation.source_record_id,
                        row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.prty_asset_id DESC,lkp.asset_valut_amt_cd DESC,lkp.asset_val_amt DESC,lkp.asset_val_strt_dttm DESC,lkp.asset_val_end_dttm DESC) rnk
             FROM       exp_data_transformation
             inner join lkp_prty_asset_id
             ON         exp_data_transformation.source_record_id = lkp_prty_asset_id.source_record_id
             left join
                        (
                                 SELECT   asset_val.asset_val_amt       AS asset_val_amt,
                                          asset_val.asset_val_strt_dttm AS asset_val_strt_dttm,
                                          asset_val.asset_val_end_dttm  AS asset_val_end_dttm,
                                          asset_val.prty_asset_id       AS prty_asset_id,
                                          asset_val.asset_valut_amt_cd  AS asset_valut_amt_cd
                                 FROM     db_t_prod_core.asset_val qualify row_number() over( PARTITION BY prty_asset_id,asset_valut_amt_cd ORDER BY edw_end_dttm DESC) = 1 ) lkp
             ON         lkp.prty_asset_id = lkp_prty_asset_id.prty_asset_id
             AND        lkp.asset_valut_amt_cd = exp_data_transformation.o_asset_valut_amt_cd 
			 qualify row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.prty_asset_id DESC,lkp.asset_valut_amt_cd DESC,lkp.asset_val_amt DESC,lkp.asset_val_strt_dttm DESC,lkp.asset_val_end_dttm DESC) 
			 = 1 );
  -- Component exp_asset_val_data_trans, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_asset_val_data_trans AS
  (
             SELECT     lkp_asset_val.prty_asset_id         AS lkp_prty_asset_id,
                        lkp_asset_val.asset_valut_amt_cd    AS lkp_asset_valut_amt_cd,
                        lkp_asset_val.asset_val_amt         AS lkp_asset_val_amt,
                        lkp_asset_val.asset_val_strt_dttm   AS lkp_asset_val_strt_dt,
                        lkp_asset_val.in_prty_asset_id      AS in_prty_asset_id,
                        lkp_asset_val.asset_val_end_dttm    AS lkp_asset_val_end_dttm,
                        lkp_asset_val.in_asset_valut_amt_cd AS in_asset_valut_amt_cd,
                        lkp_asset_val.asset_val_amt1        AS in_asset_val_amt,
                        lkp_asset_val.o_effectivedate1      AS in_asset_val_strt_dt,
                        lkp_asset_val.o_expirationdate      AS in_expirationdate,
                        lkp_asset_val.prcs_id               AS prcs_id,
                        lkp_asset_val.edw_end_dttm          AS edw_end_dttm,
                        md5 ( ltrim ( rtrim ( lkp_asset_val.asset_val_amt ) )
                                   || ltrim ( rtrim ( lkp_asset_val.asset_val_strt_dttm ) )
                                   || ltrim ( rtrim ( lkp_asset_val.asset_val_end_dttm ) ) ) AS v_lkp_checksum,
                        md5 ( ltrim ( rtrim ( lkp_asset_val.asset_val_amt1 ) )
                                   || ltrim ( rtrim ( lkp_asset_val.o_effectivedate1 ) )
                                   || ltrim ( rtrim ( lkp_asset_val.o_expirationdate ) ) ) AS v_in_checksum,
                        CASE
                                   WHEN v_lkp_checksum IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_lkp_checksum <> v_in_checksum THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                                     AS cdc_flag,
                        to_date ( ''9999-12-31'' , ''yyyy-mm-dd'' ) AS asset_val_end_dttm,
                        ''UNK''                                   AS dummy,
                        exp_data_transformation.rank            AS rank,
                        exp_data_transformation.source_record_id
             FROM       exp_data_transformation
             inner join lkp_asset_val
             ON         exp_data_transformation.source_record_id = lkp_asset_val.source_record_id );
  -- Component rtr_asset_val_INSERT, Type ROUTER Output Group INSERT
  create or replace TEMPORARY TABLE rtr_asset_val_insert AS
   SELECT exp_asset_val_data_trans.lkp_prty_asset_id      AS lkp_prty_asset_id,
         exp_asset_val_data_trans.lkp_asset_valut_amt_cd AS lkp_asset_valut_amt_cd,
         exp_asset_val_data_trans.in_prty_asset_id       AS in_prty_asset_id,
         exp_asset_val_data_trans.in_asset_val_amt       AS in_assetvalamt,
         exp_asset_val_data_trans.in_asset_val_strt_dt   AS in_asset_val_strt_dttm,
         exp_asset_val_data_trans.in_asset_valut_amt_cd  AS in_asset_val_valut_amt_cd,
         NULL                                            AS in_asset_val_meth_cd,
         NULL                                            AS in_asset_val_prps_cd,
         NULL                                            AS in_asset_aprsl_rsn_cd,
         exp_asset_val_data_trans.prcs_id                AS prcs_id,
         NULL                                            AS edw_strt_dttm,
         exp_asset_val_data_trans.edw_end_dttm           AS edw_end_dttm,
         NULL                                            AS insertflag,
         NULL                                            AS updateflag,
         exp_asset_val_data_trans.asset_val_end_dttm     AS asset_val_end_dttm,
         exp_asset_val_data_trans.dummy                  AS dummy,
         exp_asset_val_data_trans.cdc_flag               AS cdc_flag,
         exp_asset_val_data_trans.in_expirationdate      AS in_expirationdate,
         exp_asset_val_data_trans.lkp_asset_val_amt      AS lkp_asset_val_amt,
         exp_asset_val_data_trans.lkp_asset_val_strt_dt  AS lkp_asset_val_strt_dt,
         exp_asset_val_data_trans.lkp_asset_val_end_dttm AS lkp_asset_val_end_dttm,
         exp_asset_val_data_trans.rank                   AS rank,
         exp_asset_val_data_trans.rank                   AS rank4,
         exp_asset_val_data_trans.source_record_id
  FROM   exp_asset_val_data_trans
  WHERE  exp_asset_val_data_trans.in_prty_asset_id IS NOT NULL
  AND    (
                exp_asset_val_data_trans.cdc_flag = ''I''
         OR     exp_asset_val_data_trans.cdc_flag = ''U'' );
  
  -- Component upstg_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upstg_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_asset_val_insert.in_prty_asset_id          AS prty_asset_id,
                rtr_asset_val_insert.in_assetvalamt            AS asset_val_amt,
                rtr_asset_val_insert.in_asset_val_strt_dttm    AS asset_val_strt_dt,
                rtr_asset_val_insert.in_asset_val_valut_amt_cd AS asset_val_valut_amt_cd,
                rtr_asset_val_insert.in_asset_val_meth_cd      AS asset_val_meth_cd,
                rtr_asset_val_insert.in_asset_val_prps_cd      AS asset_val_prps_cd,
                rtr_asset_val_insert.prcs_id                   AS prcs_id1,
                rtr_asset_val_insert.in_asset_aprsl_rsn_cd     AS in_asset_aprsl_rsn_cd1,
                rtr_asset_val_insert.edw_strt_dttm             AS edw_strt_dttm1,
                rtr_asset_val_insert.edw_end_dttm              AS edw_end_dttm1,
                rtr_asset_val_insert.asset_val_end_dttm        AS asset_val_end_dttm,
                rtr_asset_val_insert.dummy                     AS dummy1,
                rtr_asset_val_insert.in_expirationdate         AS in_expirationdate1,
                rtr_asset_val_insert.rank                      AS rank1,
                0                                              AS update_strategy_action,
				rtr_asset_val_insert.source_record_id
         FROM   rtr_asset_val_insert );
  -- Component exp_pass_to_tgt_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_ins AS
  (
         SELECT upstg_ins.prty_asset_id                                           AS prty_asset_id,
                upstg_ins.asset_val_amt                                           AS asset_val_amt,
                upstg_ins.asset_val_strt_dt                                       AS asset_val_strt_dt,
                upstg_ins.asset_val_valut_amt_cd                                  AS asset_val_valut_amt_cd,
                upstg_ins.prcs_id1                                                AS out_prcs_id,
                dateadd(''second'', ( 2 * ( upstg_ins.rank1 - 1 ) ), current_timestamp) AS edw_strt_dttm1,
                upstg_ins.edw_end_dttm1                                           AS edw_end_dttm1,
                upstg_ins.dummy1                                                  AS dummy1,
                upstg_ins.in_expirationdate1                                      AS in_expirationdate1,
                upstg_ins.source_record_id
         FROM   upstg_ins );
  -- Component tgt_ASSET_VAL_ins, Type TARGET
  INSERT INTO db_t_prod_core.asset_val
              (
                          prty_asset_id,
                          asset_valut_amt_cd,
                          asset_val_strt_dttm,
                          asset_val_end_dttm,
                          asset_val_amt,
                          asset_valut_meth_cd,
                          asset_valut_prps_cd,
                          prcs_id,
                          asset_aprsl_rsn_cd,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_pass_to_tgt_ins.prty_asset_id          AS prty_asset_id,
         exp_pass_to_tgt_ins.asset_val_valut_amt_cd AS asset_valut_amt_cd,
         exp_pass_to_tgt_ins.asset_val_strt_dt      AS asset_val_strt_dttm,
         exp_pass_to_tgt_ins.in_expirationdate1     AS asset_val_end_dttm,
         exp_pass_to_tgt_ins.asset_val_amt          AS asset_val_amt,
         exp_pass_to_tgt_ins.dummy1                 AS asset_valut_meth_cd,
         exp_pass_to_tgt_ins.dummy1                 AS asset_valut_prps_cd,
         exp_pass_to_tgt_ins.out_prcs_id            AS prcs_id,
         exp_pass_to_tgt_ins.dummy1                 AS asset_aprsl_rsn_cd,
         exp_pass_to_tgt_ins.edw_strt_dttm1         AS edw_strt_dttm,
         exp_pass_to_tgt_ins.edw_end_dttm1          AS edw_end_dttm
  FROM   exp_pass_to_tgt_ins;
  
  -- Component tgt_ASSET_VAL_ins, Type Post SQL
  UPDATE db_t_prod_core.asset_val
 SET    edw_end_dttm=a.lead
  FROM   (
                         SELECT DISTINCT prty_asset_id,
                                         asset_valut_amt_cd,
                                         edw_strt_dttm,
                                         asset_val_amt,
                                         max(edw_strt_dttm) over (PARTITION BY prty_asset_id,asset_valut_amt_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead
                         FROM            db_t_prod_core.asset_val ) a
 
  WHERE  asset_val.edw_strt_dttm = a.edw_strt_dttm
  AND    asset_val.prty_asset_id=a.prty_asset_id
  AND    asset_val.asset_valut_amt_cd=a.asset_valut_amt_cd
  AND    asset_val.asset_val_amt=a.asset_val_amt
  AND    lead IS NOT NULL
         /*
UPDATE  ASSET_VAL  FROM
(
SELECT distinct PRTY_ASSET_ID,ASSET_VALUT_AMT_CD,EDW_STRT_DTTM
FROM ASSET_VAL
WHERE EDW_END_DTTM=TO_DATE(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
QUALIFY ROW_NUMBER() OVER(PARTITION BY PRTY_ASSET_ID,ASSET_VALUT_AMT_CD  ORDER BY ASSET_VAL_STRT_DTTM,ASSET_VAL_END_DTTM DESC) >1
)  A
SET EDW_END_DTTM= A.EDW_STRT_DTTM+ INTERVAL ''1'' SECOND
WHERE  ASSET_VAL.PRTY_ASSET_ID=A.PRTY_ASSET_ID
AND ASSET_VAL.ASSET_VALUT_AMT_CD=A.ASSET_VALUT_AMT_CD
AND  ASSET_VAL.EDW_STRT_DTTM=A.EDW_STRT_DTTM
*/
         ;

END;
';