-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_XREF_APLCTN("WORKLET_NAME" VARCHAR)
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
BEGIN

 run_id :=   (SELECT run_id   FROM control_run_id where worklet_name= :worklet_name order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'');
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'');

  -- Component LKP_TGT_DIR_APLCTN, Type Prerequisite Lookup Object
  CREATE
  OR
  REPLACE TEMPORARY TABLE LKP_TGT_DIR_APLCTN AS
  (
         SELECT APLCTN_ID,
                HOST_APLCTN_ID,
                VERS_NBR,
                APLCTN_TYPE_CD,
                SRC_SYS_CD
         FROM   db_t_prod_core.DIR_APLCTN );
  -- Component SQ_XREF_APLCTN, Type SOURCE
  CREATE
  OR
  REPLACE TEMPORARY TABLE SQ_XREF_APLCTN AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS HOST_APLCTN_ID,
                $2 AS VERS_NBR,
                $3 AS APLCTN_TYPE_CD,
                $4 AS XREF_TYPE_CD,
                $5 AS SRC_SYS_CD,
                $6 AS source_record_id
         FROM   (
                         SELECT   SRC.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                 /* SELECT DISTINCT JobNumber_stg         AS APLCTN_HOST_ID ,
                                                                  CAST(NULL AS INTEGER) AS VERS_NBR ,
                                                                  (
                                                                         SELECT XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL
                                                                         FROM   DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT XLAT
                                                                         WHERE  XLAT.TGT_IDNTFTN_NM= ''APLCTN_TYPE''
                                                                         AND    XLAT.SRC_IDNTFTN_NM= ''pctl_job.typecode''
                                                                         AND    XLAT.SRC_IDNTFTN_SYS= ''GW''
                                                                         AND    XLAT.EXPN_DT=''9999-12-31''
                                                                         AND    XLAT.SRC_IDNTFTN_VAL = pctl_job.TypeCode_stg ) AS APLCTN_TYPE_CD ,
                                                                  CAST(''APLCTN'' AS VARCHAR(50))                                AS XREF_TYPE_CD ,
                                                                  CAST(''GWPC'' AS   VARCHAR(50))                                AS SRC_SYS_CD ,
                                                                  CAST(1 AS        INTEGER)                                    AS SORT
                                                  FROM            (
                                                                                  SELECT DISTINCT pc_job.id_stg ,
                                                                                                  pc_job.ExcludeReason_stg ,
                                                                                                  pc_job.JobNumber_stg ,
                                                                                                  pc_job.Subtype_stg ,
                                                                                                  pc_policyperiod.PolicyNumber_stg
                                                                                  FROM            DB_T_PROD_STAG.PC_JOB
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.Jobid_stg
                                                                                  LEFT JOIN       DB_T_PROD_STAG.pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.pc_effectivedatedfields
                                                                                  ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.pcx_holineratingfactor_alfa
                                                                                  ON              pc_policyperiod.id_stg=pcx_holineratingfactor_alfa.branchid_stg
                                                                                  WHERE           pc_policyperiod.UpdateTime_stg > (:START_DTTM)
                                                                                  AND             pc_policyperiod.UpdateTime_stg <= (:END_DTTM)
                                                                                  AND             pctl_policyperiodstatus.typecode_stg <> ''Temporary''
                                                                                  AND             pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                  AND             pcx_holineratingfactor_alfa.ExpirationDate_stg IS NULL ) PC_JOB
                                                                  /* -----------dropzone mapping query of m_dz_pc_job */
                                                 /* INNER JOIN      DB_T_PROD_STAG.pctl_job
                                                  ON              pc_job.Subtype_stg=pctl_job.id_stg
                                                  WHERE           pctl_job.TYPECODE_stg IN (''Submission'',
                                                                                            ''PolicyChange'',
                                                                                            ''Renewal'')
                                                  AND             PC_JOB.policynumber_stg IS NOT NULL */
                                                  SELECT DISTINCT 
                                                        pc_job.JobNumber_stg AS APLCTN_HOST_ID,
                                                        CAST(NULL AS INTEGER) AS VERS_NBR,
                                                        XLAT.TGT_IDNTFTN_VAL AS APLCTN_TYPE_CD,
                                                        CAST(''APLCTN'' AS VARCHAR(50)) AS XREF_TYPE_CD,
                                                        CAST(''GWPC'' AS VARCHAR(50)) AS SRC_SYS_CD,
                                                        CAST(1 AS INTEGER) AS SORT

                                                        FROM (
                                                        SELECT DISTINCT 
                                                               pc_job.id_stg,
                                                               pc_job.ExcludeReason_stg,
                                                               pc_job.JobNumber_stg,
                                                               pc_job.Subtype_stg,
                                                               pc_policyperiod.PolicyNumber_stg
                                                        FROM 
                                                               DB_T_PROD_STAG.PC_JOB pc_job
                                                        LEFT OUTER JOIN 
                                                               DB_T_PROD_STAG.pc_policyperiod pc_policyperiod
                                                               ON pc_job.id_stg = pc_policyperiod.Jobid_stg
                                                        LEFT JOIN 
                                                               DB_T_PROD_STAG.pctl_policyperiodstatus pctl_policyperiodstatus
                                                               ON pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg
                                                        LEFT OUTER JOIN 
                                                               DB_T_PROD_STAG.pc_effectivedatedfields pc_effectivedatedfields
                                                               ON pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                        LEFT OUTER JOIN 
                                                               DB_T_PROD_STAG.pcx_holineratingfactor_alfa pcx_holineratingfactor_alfa
                                                               ON pc_policyperiod.id_stg = pcx_holineratingfactor_alfa.branchid_stg
                                                        WHERE 
                                                               pc_policyperiod.UpdateTime_stg > :START_DTTM
                                                               AND pc_policyperiod.UpdateTime_stg <= :END_DTTM
                                                               AND pctl_policyperiodstatus.typecode_stg <> ''Temporary''
                                                               AND pc_effectivedatedfields.expirationdate_stg IS NULL
                                                               AND pcx_holineratingfactor_alfa.ExpirationDate_stg IS NULL
                                                        ) pc_job

                                                        INNER JOIN 
                                                        DB_T_PROD_STAG.pctl_job pctl_job
                                                        ON pc_job.Subtype_stg = pctl_job.id_stg

                                                        LEFT JOIN 
                                                        DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT XLAT
                                                        ON XLAT.SRC_IDNTFTN_VAL = pctl_job.TypeCode_stg
                                                        AND XLAT.TGT_IDNTFTN_NM = ''APLCTN_TYPE''
                                                        AND XLAT.SRC_IDNTFTN_NM = ''pctl_job.typecode''
                                                        AND XLAT.SRC_IDNTFTN_SYS = ''GW''
                                                        AND XLAT.EXPN_DT = ''9999-12-31''

                                                        WHERE 
                                                        pctl_job.TYPECODE_stg IN (''Submission'', ''PolicyChange'', ''Renewal'')
                                                        AND pc_job.PolicyNumber_stg IS NOT NULL

                                                  UNION
                                                  /*SELECT DISTINCT jobnumber_stg                     AS HOST_ID ,
                                                                  CAST(branchnumber_stg AS INTEGER) AS VERS_NBR ,
                                                                  (
                                                                         SELECT XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL
                                                                         FROM   DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT XLAT
                                                                         WHERE  XLAT.TGT_IDNTFTN_NM= ''APLCTN_TYPE''
                                                                         AND    XLAT.SRC_IDNTFTN_NM= ''pctl_job.typecode''
                                                                         AND    XLAT.SRC_IDNTFTN_SYS= ''GW''
                                                                         AND    XLAT.EXPN_DT=''9999-12-31''
                                                                         AND    XLAT.SRC_IDNTFTN_VAL = pctl_job.TypeCode_stg ) AS QUOTN_SBTYPE_CD ,
                                                                  CAST(''QUOTN'' AS VARCHAR(50))                                 AS XREF_TYPE_CD ,
                                                                  CAST(''GWPC'' AS  VARCHAR(50))                                 AS SRC_SYS_CD ,
                                                                  CAST(2 AS       INTEGER)                                     AS SORT
                                                  FROM            (
                                                                                  SELECT DISTINCT pc_job.id_stg ,
                                                                                                  pc_job.ExcludeReason_stg ,
                                                                                                  pc_job.JobNumber_stg ,
                                                                                                  pc_job.Subtype_stg ,
                                                                                                  pctl_policyperiodstatus.TYPECODE_stg ,
                                                                                                  pc_policyperiod.branchnumber_stg ,
                                                                                                  pc_policyperiod.PolicyNumber_stg
                                                                                  FROM            DB_T_PROD_STAG.PC_JOB
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.Jobid_stg
                                                                                  LEFT JOIN       DB_T_PROD_STAG.pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.pc_effectivedatedfields
                                                                                  ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.pcx_holineratingfactor_alfa
                                                                                  ON              pc_policyperiod.id_stg=pcx_holineratingfactor_alfa.branchid_stg
                                                                                  WHERE           pc_policyperiod.UpdateTime_stg > (:START_DTTM)
                                                                                  AND             pc_policyperiod.UpdateTime_stg <= (:END_DTTM)
                                                                                  AND             pctl_policyperiodstatus.typecode_stg <> ''Temporary''
                                                                                  AND             pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                  AND             pcx_holineratingfactor_alfa.ExpirationDate_stg IS NULL ) PC_JOB
                                                                  /* -----------------------------dropzone mapping query of m_dz_pc_job */
                                                  /*INNER JOIN      DB_T_PROD_STAG.pctl_job
                                                  ON              pctl_job.id_stg=pc_job.Subtype_stg
                                                  WHERE           pctl_job.TYPECODE_stg IN (''Submission'',
                                                                                            ''PolicyChange'',
                                                                                            ''Renewal'')
                                                  AND             pc_job.Typecode_stg<>''Temporary''
                                                  AND             pc_job.policynumber_stg IS NOT NULL
                                                  ORDER BY        6 ASC */
                                                  
                                                        SELECT DISTINCT 
                                                        pc_job.jobnumber_stg                     AS HOST_ID,
                                                        CAST(pc_job.branchnumber_stg AS INTEGER) AS VERS_NBR,
                                                        XLAT.TGT_IDNTFTN_VAL                     AS QUOTN_SBTYPE_CD,
                                                        CAST(''QUOTN'' AS VARCHAR(50))             AS XREF_TYPE_CD,
                                                        CAST(''GWPC'' AS VARCHAR(50))              AS SRC_SYS_CD,
                                                        CAST(2 AS INTEGER)                       AS SORT

                                                        FROM (
                                                        SELECT DISTINCT 
                                                               pc_job.id_stg,
                                                               pc_job.ExcludeReason_stg,
                                                               pc_job.JobNumber_stg,
                                                               pc_job.Subtype_stg,
                                                               pctl_policyperiodstatus.TYPECODE_stg,
                                                               pc_policyperiod.branchnumber_stg,
                                                               pc_policyperiod.PolicyNumber_stg,
                                                               pc_policyperiod.UpdateTime_stg,
                                                               pc_effectivedatedfields.expirationdate_stg,
                                                               pcx_holineratingfactor_alfa.ExpirationDate_stg
                                                        FROM 
                                                               DB_T_PROD_STAG.PC_JOB pc_job
                                                        LEFT OUTER JOIN 
                                                               DB_T_PROD_STAG.pc_policyperiod pc_policyperiod
                                                               ON pc_job.id_stg = pc_policyperiod.Jobid_stg
                                                        LEFT JOIN 
                                                               DB_T_PROD_STAG.pctl_policyperiodstatus pctl_policyperiodstatus
                                                               ON pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg
                                                        LEFT OUTER JOIN 
                                                               DB_T_PROD_STAG.pc_effectivedatedfields pc_effectivedatedfields
                                                               ON pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                        LEFT OUTER JOIN 
                                                               DB_T_PROD_STAG.pcx_holineratingfactor_alfa pcx_holineratingfactor_alfa
                                                               ON pc_policyperiod.id_stg = pcx_holineratingfactor_alfa.branchid_stg
                                                        WHERE 
                                                               pc_policyperiod.UpdateTime_stg > :START_DTTM
                                                               AND pc_policyperiod.UpdateTime_stg <= :END_DTTM
                                                               AND pctl_policyperiodstatus.typecode_stg <> ''Temporary''
                                                               AND pc_effectivedatedfields.expirationdate_stg IS NULL
                                                               AND pcx_holineratingfactor_alfa.ExpirationDate_stg IS NULL
                                                        ) pc_job

                                                        INNER JOIN 
                                                        DB_T_PROD_STAG.pctl_job pctl_job
                                                        ON pctl_job.id_stg = pc_job.Subtype_stg

                                                        LEFT JOIN 
                                                        DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT XLAT
                                                        ON XLAT.TGT_IDNTFTN_NM = ''APLCTN_TYPE''
                                                        AND XLAT.SRC_IDNTFTN_NM = ''pctl_job.typecode''
                                                        AND XLAT.SRC_IDNTFTN_SYS = ''GW''
                                                        AND XLAT.EXPN_DT = ''9999-12-31''
                                                        AND XLAT.SRC_IDNTFTN_VAL = pctl_job.TypeCode_stg

                                                        WHERE 
                                                        pctl_job.TYPECODE_stg IN (''Submission'', ''PolicyChange'', ''Renewal'')
                                                        AND pctl_job.Typecode_stg <> ''Temporary''
                                                        AND pc_job.PolicyNumber_stg IS NOT NULL

                                                        ORDER BY 
                                                        6 ASC
                                           
                                                  ) SRC ) );
  -- Component exp_pass_to_target, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE exp_pass_to_target AS
  (
            SELECT    SQ_XREF_APLCTN.HOST_APLCTN_ID AS HOST_APLCTN_ID,
                      SQ_XREF_APLCTN.VERS_NBR       AS VERS_NBR,
                      SQ_XREF_APLCTN.APLCTN_TYPE_CD AS APLCTN_TYPE_CD,
                      SQ_XREF_APLCTN.SRC_SYS_CD     AS SRC_SYS_CD,
                      SQ_XREF_APLCTN.XREF_TYPE_CD   AS XREF_TYPE_CD,
                      CURRENT_TIMESTAMP             AS LOAD_DTTM,
                      DECODE ( TRUE ,
                              LKP_1.APLCTN_ID
                              /* replaced lookup LKP_TGT_DIR_APLCTN */
                              IS NOT NULL , ''R'' ,
                              ''I'' ) AS ins_rej_flg,
                      SQ_XREF_APLCTN.source_record_id,
                      --row_number() over (PARTITION BY SQ_XREF_APLCTN.source_record_id ORDER BY SQ_XREF_APLCTN.source_record_id) AS RNK
            FROM      SQ_XREF_APLCTN
            LEFT JOIN LKP_TGT_DIR_APLCTN LKP_1
            ON        COALESCE(LKP_1.HOST_APLCTN_ID, ''~'') = COALESCE(SQ_XREF_APLCTN.HOST_APLCTN_ID, ''~'')
            AND       COALESCE(LKP_1.VERS_NBR, 0) = COALESCE(SQ_XREF_APLCTN.VERS_NBR, 0)
            AND       COALESCE(LKP_1.APLCTN_TYPE_CD, ''~'') = COALESCE(SQ_XREF_APLCTN.APLCTN_TYPE_CD, ''~'')
            AND       COALESCE(LKP_1.SRC_SYS_CD, ''~'') = COALESCE(SQ_XREF_APLCTN.SRC_SYS_CD, ''~'')

            --QUALIFY RNK = 1 
            );
  -- Component flt_ins_rej_agmt_id, Type FILTER
  CREATE
  OR
  REPLACE TEMPORARY TABLE flt_ins_rej_agmt_id AS
  (
         SELECT exp_pass_to_target.HOST_APLCTN_ID AS HOST_APLCTN_ID,
                exp_pass_to_target.VERS_NBR       AS VERS_NBR,
                exp_pass_to_target.APLCTN_TYPE_CD AS APLCTN_TYPE_CD,
                exp_pass_to_target.SRC_SYS_CD     AS SRC_SYS_CD,
                exp_pass_to_target.XREF_TYPE_CD   AS XREF_TYPE_CD,
                exp_pass_to_target.LOAD_DTTM      AS LOAD_DTTM,
                exp_pass_to_target.ins_rej_flg    AS ins_rej_flg,
                exp_pass_to_target.source_record_id
         FROM   exp_pass_to_target
         WHERE  exp_pass_to_target.ins_rej_flg = ''I'' );
  -- Component DIR_APLCTN, Type TARGET
  INSERT INTO DB_T_PROD_CORE.DIR_APLCTN
              (
                          APLCTN_ID,
                          HOST_APLCTN_ID,
                          VERS_NBR,
                          APLCTN_TYPE_CD,
                          DIR_TYPE_VAL,
                          SRC_SYS_CD,
                          LOAD_DTTM
              )
  SELECT   public.seq_aplctn.nextval     AS APLCTN_ID,
           flt_ins_rej_agmt_id.HOST_APLCTN_ID AS HOST_APLCTN_ID,
           flt_ins_rej_agmt_id.VERS_NBR       AS VERS_NBR,
           flt_ins_rej_agmt_id.APLCTN_TYPE_CD AS APLCTN_TYPE_CD,
           flt_ins_rej_agmt_id.XREF_TYPE_CD   AS DIR_TYPE_VAL,
           flt_ins_rej_agmt_id.SRC_SYS_CD     AS SRC_SYS_CD,
           flt_ins_rej_agmt_id.LOAD_DTTM      AS LOAD_DTTM
  FROM     flt_ins_rej_agmt_id;

END;
';