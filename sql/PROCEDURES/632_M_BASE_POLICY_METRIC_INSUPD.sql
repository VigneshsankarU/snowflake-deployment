-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_POLICY_METRIC_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  PRCS_ID STRING;
  premadjfilter STRING;
  START_DTTM TIMESTAMP;
  END_DTTM TIMESTAMP;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);
  END_DTTM := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
  PRCS_ID := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);
  premadjfilter := public.func_get_scoped_param(:run_id, ''premadjfilter'', :workflow_name, :worklet_name, :session_name);
  START_DTTM := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);

  -- Component SQ_pc_policyperiod, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_policyperiod AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS lkp_edw_strt_dttm,
                $2  AS lkp_edw_end_dttm,
                $3  AS agmt_id,
                $4  AS out_insrnc_mtrc_type,
                $5  AS plcy_mtrc_strt_dt,
                $6  AS plcy_mtrc_end_dt,
                $7  AS tm_prd_cd,
                $8  AS plcy_amt,
                $9  AS plcy_cnt,
                $10 AS uom_cd,
                $11 AS uom_type_cd,
                $12 AS out_cntrl_id,
                $13 AS retired,
                $14 AS sourcedata,
                $15 AS targetdata,
                $16 AS ins_upd_flag,
                $17 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT          tg_plcy_mtrc.edw_strt_dttm    AS lkp_edw_strt_dttm,
                                                                  tg_plcy_mtrc.edw_end_dttm     AS lkp_edw_end_dttm,
                                                                  xlat_src.agmt_id              AS agmt_id,
                                                                  xlat_src.out_insrnc_mtrc_type AS out_insrnc_mtrc_type,
                                                                  xlat_src.editeffectivedate    AS plcy_mtrc_strt_dt,
                                                                  xlat_src.periodend            AS plcy_mtrc_end_dt,
                                                                  xlat_src.tm_prd_cd            AS tm_prd_cd,
                                                                  xlat_src.totalpremiumrpt      AS plcy_amt,
                                                                  xlat_src.cnt                  AS plcy_cnt,
                                                                  xlat_src.uom_cd               AS uom_cd,
                                                                  xlat_src.uom_type_cd          AS uom_type_cd,
                                                                  xlat_src.out_cntrl_id         AS out_cntrl_id,
                                                                  xlat_src.retired              AS retired,
                                                                  /* SourceData */
                                                                  cast(to_char(cast(xlat_src.editeffectivedate AS timestamp))
                                                                                  || to_char(cast(xlat_src.periodend AS timestamp))
                                                                                  || trim(coalesce(xlat_src.tm_prd_cd,''0''))
                                                                                  || trim(coalesce(cast(xlat_src.totalpremiumrpt AS DECIMAL(18,4)),''0''))
                                                                                  || trim(cast(coalesce(xlat_src.cnt,''0'') AS VARCHAR(30)))
                                                                                  || trim(coalesce(xlat_src.uom_cd,''0''))
                                                                                  || trim(coalesce(xlat_src.uom_type_cd,''0'')) AS VARCHAR(1100)) AS sourcedata,
                                                                  /* TargetData */
                                                                  cast(to_char(cast(tg_plcy_mtrc.plcy_mtrc_strt_dttm AS timestamp))
                                                                                  || to_char(cast(tg_plcy_mtrc.plcy_mtrc_end_dttm AS timestamp))
                                                                                  || trim(coalesce(tg_plcy_mtrc.tm_prd_cd,''0''))
                                                                                  || trim(coalesce(cast(tg_plcy_mtrc.plcy_amt AS DECIMAL(18,4)),''0''))
                                                                                  || trim(cast(coalesce(tg_plcy_mtrc.plcy_cnt,''0'') AS VARCHAR(30)))
                                                                                  || trim(coalesce(tg_plcy_mtrc.uom_cd,''0''))
                                                                                  || trim(coalesce(tg_plcy_mtrc.uom_type_cd,''0'')) AS VARCHAR(1100)) AS targetdata,
                                                                  /* flag */
                                                                  CASE
                                                                                  WHEN targetdata IS NULL THEN ''I''
                                                                                  WHEN targetdata IS NOT NULL
                                                                                  AND             sourcedata <> targetdata THEN ''U''
                                                                                  WHEN targetdata IS NOT NULL
                                                                                  AND             sourcedata = targetdata THEN ''R''
                                                                  END AS ins_upd_flag
                                                  FROM
                                                                  /*source query with Expression*/
                                                                  (
                                                                                  SELECT          agmt_lkp.agmt_id,
                                                                                                  coalesce(src.editeffectivedate,to_timestamp_ntz(''1900-01-01 00:00:00.000000'' , ''MM/DD/YYYYBHH:MI:SS.FF6'')) AS editeffectivedate,
                                                                                                  src.MAXVALUE                                                                                                      AS totalpremiumrpt,
                                                                                                  coalesce(xlat_insrnc_mtrc_type.tgt_idntftn_val,''UNK'')                                                             AS out_insrnc_mtrc_type,
                                                                                                  coalesce(periodend,cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)))                                            AS periodend,
                                                                                                  src.cnt                                                                                                           AS cnt,
                                                                                                  src.retired                                                                                                       AS retired,
                                                                                                  ''UNK''                                                                                                             AS tm_prd_cd,
                                                                                                  ''UNK''                                                                                                             AS uom_cd,
                                                                                                  ''UNK''                                                                                                             AS uom_type_cd,
                                                                                                  1                                                                                                                 AS out_cntrl_id
                                                                                  FROM
                                                                                                  /* -src query start */
                                                                                                  (
                                                                                                           SELECT   editeffectivedate,
                                                                                                                    periodend,
                                                                                                                    publicid,
                                                                                                                    max(totalpremiumrpt) AS MAXVALUE,
                                                                                                                    insurance_metric_type,
                                                                                                                    cnt,
                                                                                                                    src_cd,
                                                                                                                    retired
                                                                                                           FROM     (
                                                                                                                                    SELECT DISTINCT outer_pc.editeffectivedate,
                                                                                                                                                    outer_pc.periodend,
                                                                                                                                                    outer_pc.publicid,
                                                                                                                                                    outer_pc.totalpremiumrpt ,
                                                                                                                                                    cast(''INSRNC_MTRC_TYPE5'' AS VARCHAR(40)) AS insurance_metric_type,
                                                                                                                                                    NULL                                     AS cnt ,
                                                                                                                                                    ''SRC_SYS4''                               AS src_cd,
                                                                                                                                                    outer_pc.retired
                                                                                                                                    FROM            (
                                                                                                                                                                    SELECT DISTINCT pc_policyperiod.totalpremiumrpt_stg   AS totalpremiumrpt,
                                                                                                                                                                                    pc_policyperiod.editeffectivedate_stg AS editeffectivedate,
                                                                                                                                                                                    pc_policyperiod.periodend_stg         AS periodend,
                                                                                                                                                                                    pc_policyperiod.publicid_stg          AS publicid,
                                                                                                                                                                                    pc_policyperiod.retired_stg           AS retired,
                                                                                                                                                                                    pc_policyperiod.status_stg            AS status,
                                                                                                                                                                                    pc_policyperiod.updatetime_stg        AS updatetime
                                                                                                                                                                    FROM            db_t_prod_stag.pc_policyperiod 
                                                                                                                                                                    WHERE           pc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                                                                                                    AND             pc_policyperiod.updatetime_stg <= (:end_dttm) ) outer_pc
                                                                                                                                    join            db_t_prod_stag.pctl_policyperiodstatus 
                                                                                                                                    ON              outer_pc.status = pctl_policyperiodstatus.id_stg
                                                                                                                                    WHERE           pctl_policyperiodstatus.typecode_stg=''Bound''
                                                                                                                                    UNION ALL
                                                                                                                                    SELECT DISTINCT outer_pc.editeffectivedate,
                                                                                                                                                    outer_pc.periodend,
                                                                                                                                                    outer_pc.publicid,
                                                                                                                                                    outer_pc.totaltermpremrpt_alfa ,
                                                                                                                                                    cast(''INSRNC_MTRC_TYPE22'' AS VARCHAR(40)) AS insurance_metric_type,
                                                                                                                                                    NULL                                      AS cnt ,
                                                                                                                                                    ''SRC_SYS4''                                AS src_cd,
                                                                                                                                                    outer_pc.retired
                                                                                                                                    FROM            (
                                                                                                                                                                    SELECT DISTINCT pc_policyperiod.totaltermpremrpt_alfa_stg AS totaltermpremrpt_alfa,
                                                                                                                                                                                    pc_policyperiod.editeffectivedate_stg     AS editeffectivedate,
                                                                                                                                                                                    pc_policyperiod.periodend_stg             AS periodend,
                                                                                                                                                                                    pc_policyperiod.publicid_stg              AS publicid,
                                                                                                                                                                                    pc_policyperiod.retired_stg               AS retired,
                                                                                                                                                                                    pc_policyperiod.status_stg                AS status,
                                                                                                                                                                                    pc_policyperiod.updatetime_stg            AS updatetime
                                                                                                                                                                    FROM            db_t_prod_stag.pc_policyperiod 
                                                                                                                                                                    WHERE           pc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                                                                                                    AND             pc_policyperiod.updatetime_stg <= (:end_dttm) ) outer_pc
                                                                                                                                    join            db_t_prod_stag.pctl_policyperiodstatus 
                                                                                                                                    ON              outer_pc.status = pctl_policyperiodstatus.id_stg
                                                                                                                                    WHERE           pctl_policyperiodstatus.typecode_stg=''Bound''
                                                                                                                                    UNION ALL
                                                                                                                                    /*POL_DA_1020*/
                                                                                                                                    SELECT DISTINCT outer_pc.editeffectivedate,
                                                                                                                                                    outer_pc.periodend,
                                                                                                                                                    outer_pc.publicid,
                                                                                                                                                    outer_pc.totalpremadjrpt_alfa ,
                                                                                                                                                    cast(''INSRNC_MTRC_TYPE15'' AS VARCHAR(40)) AS insurance_metric_type,
                                                                                                                                                    NULL                                      AS cnt ,
                                                                                                                                                    ''SRC_SYS4''                                AS src_cd,
                                                                                                                                                    outer_pc.retired
                                                                                                                                    FROM            (
                                                                                                                                                                    SELECT DISTINCT pc_policyperiod.totalpremadjrpt_alfa_stg AS totalpremadjrpt_alfa,
                                                                                                                                                                                    pc_policyperiod.editeffectivedate_stg    AS editeffectivedate,
                                                                                                                                                                                    pc_policyperiod.periodend_stg            AS periodend,
                                                                                                                                                                                    pc_policyperiod.publicid_stg             AS publicid,
                                                                                                                                                                                    pc_policyperiod.retired_stg              AS retired,
                                                                                                                                                                                    pc_policyperiod.status_stg               AS status,
                                                                                                                                                                                    pc_policyperiod.updatetime_stg           AS updatetime
                                                                                                                                                                    FROM            db_t_prod_stag.pc_policyperiod 
                                                                                                                                                                    WHERE           pc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                                                                                                    AND             pc_policyperiod.updatetime_stg <= (:end_dttm) ) outer_pc
                                                                                                                                    join            db_t_prod_stag.pctl_policyperiodstatus 
                                                                                                                                    ON              outer_pc.status = pctl_policyperiodstatus.id_stg
                                                                                                                                    WHERE           pctl_policyperiodstatus.typecode_stg=''Bound''
                                                                                                                                    AND             :premadjfilter
                                                                                                                                    UNION ALL
                                                                                                                                    SELECT DISTINCT effectivedate,
                                                                                                                                                    cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS periodend,
                                                                                                                                                    ppv_publicid,
                                                                                                                                                    hurrmitigationcreditamt,
                                                                                                                                                    cast(''INSRNC_MTRC_TYPE21'' AS VARCHAR(40)) AS insurance_metric_type,
                                                                                                                                                    NULL                                      AS cnt,
                                                                                                                                                    ''SRC_SYS4''                                AS src_cd,
                                                                                                                                                    0
                                                                                                                                    FROM            (
                                                                                                                                                                    SELECT          coalesce(pcx_dwelling_hoe.effectivedate_stg,pc_policyperiod.periodstart_stg) AS effectivedate,
                                                                                                                                                                                    pcx_dwelling_hoe.mitigationzone_alfa_stg,
                                                                                                                                                                                    homealert.hurrmitigationcreditamt_stg AS hurrmitigationcreditamt,
                                                                                                                                                                                    pc_policyperiod.publicid_stg          AS ppv_publicid
                                                                                                                                                                    FROM            db_t_prod_stag.pcx_dwelling_hoe 
                                                                                                                                                                    left outer join db_t_prod_stag.pc_policyperiod 
                                                                                                                                                                    ON              pc_policyperiod.id_stg = pcx_dwelling_hoe.branchid_stg
                                                                                                                                                                    left outer join
                                                                                                                                                                                    (
                                                                                                                                                                                               SELECT     pcx_dwelling_hoe.fixedid_stg  AS homealerfixedid,
                                                                                                                                                                                                        pcx_dwelling_hoe.branchid_stg AS branchid,
                                                                                                                                                                                                        homealertcode_stg             AS homealert_cd,
                                                                                                                                                                                                        hurrmitigationcreditamt_stg ,
                                                                                                                                                                                                        pcx_dwelling_hoe.expirationdate_stg AS expirationdate
                                                                                                                                                                                               FROM       db_t_prod_stag.pc_policyperiod 
                                                                                                                                                                                               inner join db_t_prod_stag.pcx_dwelling_hoe 
                                                                                                                                                                                               ON         pcx_dwelling_hoe.branchid_stg=pc_policyperiod.id_stg
                                                                                                                                                                                               inner join db_t_prod_stag.pcx_dwellingratingfactor_alfa 
                                                                                                                                                                                               ON         pcx_dwelling_hoe.fixedid_stg = pcx_dwellingratingfactor_alfa.dwelling_hoe_stg
                                                                                                                                                                                               AND        pcx_dwellingratingfactor_alfa.branchid_stg=pc_policyperiod.id_stg
                                                                                                                                                                                               WHERE      pcx_dwelling_hoe.expirationdate_stg IS NULL
                                                                                                                                                                                               AND        pcx_dwellingratingfactor_alfa.expirationdate_stg IS NULL ) homealert
                                                                                                                                                                    ON              pcx_dwelling_hoe.fixedid_stg=homealert.homealerfixedid
                                                                                                                                                                    AND             pcx_dwelling_hoe.branchid_stg=homealert.branchid
                                                                                                                                                                    WHERE           pcx_dwelling_hoe.expirationdate_stg IS NULL
                                                                                                                                                                    AND             pcx_dwelling_hoe.updatetime_stg>(:start_dttm)
                                                                                                                                                                    AND             pcx_dwelling_hoe.updatetime_stg <= (:end_dttm) ) pcx_dwelling_hoe
                                                                                                                                    WHERE           hurrmitigationcreditamt IS NOT NULL
                                                                                                                                    AND             hurrmitigationcreditamt <> 0
                                                                                                                                    UNION ALL
                                                                                                                                    SELECT editeffectivedate,
                                                                                                                                           periodend,
                                                                                                                                           publicid,
                                                                                                                                           totalpremiumrpt,
                                                                                                                                           insurance_metric_type,
                                                                                                                                           cnt,
                                                                                                                                           ''SRC_SYS4'' AS src_cd,
                                                                                                                                           retired
                                                                                                                                    FROM   (
                                                                                                                                                           SELECT DISTINCT outer_pc.editeffectivedate,
                                                                                                                                                                           outer_pc.periodend,
                                                                                                                                                                           outer_pc.publicid,
                                                                                                                                                                           outer_pc.totalpremiumrpt ,
                                                                                                                                                                           ''INSRNC_MTRC_TYPE6'' AS insurance_metric_type,
                                                                                                                                                                           tendaylatepaycount  AS cnt,
                                                                                                                                                                           outer_pc.retired    AS retired,
                                                                                                                                                                           status,
                                                                                                                                                                           row_number() over (PARTITION BY publicid ORDER BY editeffectivedate DESC) AS r
                                                                                                                                                           FROM            (
                                                                                                                                                                                           SELECT DISTINCT pc_policyperiod.totalpremiumrpt_stg                AS totalpremiumrpt,
                                                                                                                                                                                                        pc_policyperiod.editeffectivedate_stg              AS editeffectivedate,
                                                                                                                                                                                                        pc_policyperiod.periodend_stg                      AS periodend,
                                                                                                                                                                                                        pc_policyperiod.publicid_stg                       AS publicid,
                                                                                                                                                                                                        pc_policyperiod.retired_stg                        AS retired,
                                                                                                                                                                                                        pc_policyperiod.status_stg                         AS status,
                                                                                                                                                                                                        pc_policyperiod.updatetime_stg                     AS updatetime,
                                                                                                                                                                                                        pcx_palineratingfactor_alfa.tendaylatepaycount_stg AS tendaylatepaycount
                                                                                                                                                                                           FROM            db_t_prod_stag.pc_policyperiod 
                                                                                                                                                                                           left outer join db_t_prod_stag.pcx_palineratingfactor_alfa 
                                                                                                                                                                                           ON              pcx_palineratingfactor_alfa.branchid_stg=pc_policyperiod.id_stg
                                                                                                                                                                                           AND             (
                                                                                                                                                                                                        pcx_palineratingfactor_alfa.expirationdate_stg IS NULL
                                                                                                                                                                                                        OR              pcx_palineratingfactor_alfa.expirationdate_stg > pc_policyperiod.editeffectivedate_stg)
                                                                                                                                                                                           WHERE           pc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                                                                                                                           AND             pc_policyperiod.updatetime_stg <= (:end_dttm) ) outer_pc
                                                                                                                                                           join            db_t_prod_stag.pctl_policyperiodstatus 
                                                                                                                                                           ON              outer_pc.status = pctl_policyperiodstatus.id_stg
                                                                                                                                                                           /* JOIN DB_T_PROD_STAG.PCX_PALINERATINGFACTOR_ALFA on PCX_PALINERATINGFACTOR_ALFA.branchid=outer_pc.id */
                                                                                                                                                           WHERE           pctl_policyperiodstatus.typecode_stg=''Bound'')x
                                                                                                                                    WHERE  x.r=1
                                                                                                                                    UNION ALL
                                                                                                                                    SELECT editeffectivedate,
                                                                                                                                           periodend,
                                                                                                                                           publicid,
                                                                                                                                           totalpremiumrpt,
                                                                                                                                           insurance_metric_type,
                                                                                                                                           cnt,
                                                                                                                                           ''SRC_SYS4'' AS src_cd,
                                                                                                                                           retired
                                                                                                                                    FROM   (
                                                                                                                                                           SELECT DISTINCT outer_pc.editeffectivedate,
                                                                                                                                                                           outer_pc.periodend,
                                                                                                                                                                           outer_pc.publicid,
                                                                                                                                                                           outer_pc.totalpremiumrpt ,
                                                                                                                                                                           ''INSRNC_MTRC_TYPE7''  AS insurance_metric_type,
                                                                                                                                                                           threedaylatepaycount AS cnt,
                                                                                                                                                                           outer_pc.retired,
                                                                                                                                                                           status,
                                                                                                                                                                           row_number() over (PARTITION BY publicid ORDER BY editeffectivedate DESC) AS r
                                                                                                                                                           FROM            (
                                                                                                                                                                                           SELECT DISTINCT pc_policyperiod.totalpremiumrpt_stg                  AS totalpremiumrpt,
                                                                                                                                                                                                        pc_policyperiod.editeffectivedate_stg                AS editeffectivedate,
                                                                                                                                                                                                        pc_policyperiod.periodend_stg                        AS periodend,
                                                                                                                                                                                                        pc_policyperiod.publicid_stg                         AS publicid,
                                                                                                                                                                                                        pc_policyperiod.retired_stg                          AS retired,
                                                                                                                                                                                                        pc_policyperiod.status_stg                           AS status,
                                                                                                                                                                                                        pc_policyperiod.updatetime_stg                       AS updatetime,
                                                                                                                                                                                                        pcx_palineratingfactor_alfa.threedaylatepaycount_stg AS threedaylatepaycount
                                                                                                                                                                                           FROM            db_t_prod_stag.pc_policyperiod 
                                                                                                                                                                                           left outer join db_t_prod_stag.pcx_palineratingfactor_alfa 
                                                                                                                                                                                           ON              pcx_palineratingfactor_alfa.branchid_stg=pc_policyperiod.id_stg
                                                                                                                                                                                           AND             (
                                                                                                                                                                                                        pcx_palineratingfactor_alfa.expirationdate_stg IS NULL
                                                                                                                                                                                                        OR              pcx_palineratingfactor_alfa.expirationdate_stg > pc_policyperiod.editeffectivedate_stg)
                                                                                                                                                                                           WHERE           pc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                                                                                                                           AND             pc_policyperiod.updatetime_stg <= (:end_dttm) )outer_pc
                                                                                                                                                           join            db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                                           ON              outer_pc.status = pctl_policyperiodstatus.id_stg
                                                                                                                                                                           /* JOIN DB_T_PROD_STAG.PCX_PALINERATINGFACTOR_ALFA on PCX_PALINERATINGFACTOR_ALFA.branchid=outer_pc.id */
                                                                                                                                                           WHERE           pctl_policyperiodstatus.typecode_stg=''Bound'')x
                                                                                                                                    WHERE  x.r=1
                                                                                                                                    UNION
                                                                                                                                    /*  POL_DP_356 */
                                                                                                                                    SELECT DISTINCT pc_job.editeffectivedate,
                                                                                                                                                    pc_job.periodend,
                                                                                                                                                    pc_job.nk_publicid,
                                                                                                                                                    pc_job.transactionpremiumrpt ,
                                                                                                                                                    ''INSRNC_MTRC_TYPE13'' AS insurance_metric_type,
                                                                                                                                                    /*  Total DB_T_CORE_DM_PROD.Policy Cost */
                                                                                                                                                    NULL       AS cnt ,
                                                                                                                                                    ''SRC_SYS4'' AS src_cd,
                                                                                                                                                    pc_job.retired
                                                                                                                                    FROM            (
                                                                                                                                                                    SELECT DISTINCT pc_job.id_stg                             AS id ,
                                                                                                                                                                                    pc_job.retired_stg                        AS retired ,
                                                                                                                                                                                    pc_job.subtype_stg                        AS SUBTYPE,
                                                                                                                                                                                    pc_policyperiod.transactionpremiumrpt_stg AS transactionpremiumrpt,
                                                                                                                                                                                    pc_policyperiod.editeffectivedate_stg     AS editeffectivedate,
                                                                                                                                                                                    pc_policyperiod.periodend_stg             AS periodend,
                                                                                                                                                                                    pc_policyperiod.publicid_stg              AS nk_publicid,
                                                                                                                                                                                    pc_policyperiod.status_stg                AS status
                                                                                                                                                                    FROM            db_t_prod_stag.pc_job 
                                                                                                                                                                    left outer join db_t_prod_stag.pc_policyperiod 
                                                                                                                                                                    ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                                                                                                    left join       db_t_prod_stag.pctl_policyperiodstatus 
                                                                                                                                                                    ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                                                                                                    left outer join db_t_prod_stag.pc_effectivedatedfields 
                                                                                                                                                                    ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                                                                                                    left outer join db_t_prod_stag.pcx_holineratingfactor_alfa 
                                                                                                                                                                    ON              pc_policyperiod.id_stg=pcx_holineratingfactor_alfa.branchid_stg
                                                                                                                                                                    left join
                                                                                                                                                                                    (
                                                                                                                                                                                                    SELECT DISTINCT jobid_stg
                                                                                                                                                                                                    FROM            db_t_prod_stag.pc_policyperiod 
                                                                                                                                                                                                    WHERE           quotematuritylevel_stg IN (2,3) ) vj
                                                                                                                                                                    ON              pc_job.id_stg=vj.jobid_stg
                                                                                                                                                                    WHERE           pc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                                                                                                    AND             pc_policyperiod.updatetime_stg <= (:end_dttm)
                                                                                                                                                                    AND             pctl_policyperiodstatus.typecode_stg<>''Temporary''
                                                                                                                                                                    AND             pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                                                                                                    AND             pcx_holineratingfactor_alfa.expirationdate_stg IS NULL ) pc_job
                                                                                                                                                    /* join DB_T_PROD_STAG.pc_jobpolicyperiod on pc_job.id = pc_jobpolicyperiod.OwnerID  */
                                                                                                                                                    /* join DB_T_PROD_STAG.pc_policyperiod on pc_jobpolicyperiod.ForeignEntityID = pc_policyperiod.id  */
                                                                                                                                    join            db_t_prod_stag.pctl_job
                                                                                                                                    ON              pc_job.SUBTYPE = pctl_job.id_stg
                                                                                                                                    /* where pctl_job.name in (''Cancellation'', ''DB_T_CORE_DM_PROD.Policy Change'') */
                                                                                                                                    UNION
                                                                                                                                    /*  POL_DA_455 */
                                                                                                                                    SELECT DISTINCT pc_policyperiod.editeffectivedate,
                                                                                                                                                    pc_policyperiod.periodend,
                                                                                                                                                    pc_policyperiod.publicid,
                                                                                                                                                    pc_policyperiod.totalcostrpt ,
                                                                                                                                                    ''INSRNC_MTRC_TYPE11'' AS insurance_metric_type,
                                                                                                                                                    /*  Total DB_T_CORE_DM_PROD.Policy Cost */
                                                                                                                                                    NULL       AS cnt ,
                                                                                                                                                    ''SRC_SYS4'' AS src_cd,
                                                                                                                                                    pc_policyperiod.retired
                                                                                                                                    FROM            (
                                                                                                                                                                    SELECT DISTINCT pc_policyperiod.totalcostrpt_stg      AS totalcostrpt,
                                                                                                                                                                                    pc_policyperiod.editeffectivedate_stg AS editeffectivedate,
                                                                                                                                                                                    pc_policyperiod.periodend_stg         AS periodend,
                                                                                                                                                                                    pc_policyperiod.publicid_stg          AS publicid,
                                                                                                                                                                                    pc_policyperiod.retired_stg           AS retired,
                                                                                                                                                                                    pc_policyperiod.status_stg            AS status,
                                                                                                                                                                                    pc_policyperiod.updatetime_stg        AS updatetime
                                                                                                                                                                    FROM            db_t_prod_stag.pc_policyperiod 
                                                                                                                                                                    WHERE           pc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                                                                                                    AND             pc_policyperiod.updatetime_stg <= (:end_dttm)
                                                                                                                                                                                    /* join DB_T_PROD_STAG.pctl_policyperiodstatus on pc_policyperiod.status = pctl_policyperiodstatus.id */
                                                                                                                                                                                    /* Where pctl_policyperiodstatus.typecode=''BOUND'' */
                                                                                                                                                    ) pc_policyperiod ) x
                                                                                                           GROUP BY editeffectivedate,
                                                                                                                    periodend,
                                                                                                                    publicid,
                                                                                                                    insurance_metric_type,
                                                                                                                    cnt,
                                                                                                                    src_cd,
                                                                                                                    retired )          AS src
                                                                                  left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_src_cd
                                                                                  ON              xlat_src_cd.src_idntftn_val= src.src_cd
                                                                                  AND             xlat_src_cd.tgt_idntftn_nm= ''SRC_SYS''
                                                                                  AND             xlat_src_cd.src_idntftn_nm= ''derived''
                                                                                  AND             xlat_src_cd.src_idntftn_sys=''DS''
                                                                                  AND             xlat_src_cd.expn_dt=''9999-12-31''
                                                                                  left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_insrnc_mtrc_type
                                                                                  ON              xlat_insrnc_mtrc_type.src_idntftn_val= src.insurance_metric_type
                                                                                  AND             xlat_insrnc_mtrc_type.tgt_idntftn_nm= ''INSRNC_MTRC_TYPE''
                                                                                  AND             xlat_insrnc_mtrc_type.src_idntftn_nm= ''derived''
                                                                                  AND             xlat_insrnc_mtrc_type.src_idntftn_sys=''DS''
                                                                                  AND             xlat_insrnc_mtrc_type.expn_dt=''9999-12-31''
                                                                                  left outer join
                                                                                                  (
                                                                                                           SELECT   agmt.agmt_id                     AS agmt_id,
                                                                                                                    agmt.host_agmt_num               AS host_agmt_num,
                                                                                                                    agmt.agmt_name                   AS agmt_name,
                                                                                                                    agmt.agmt_opn_dttm               AS agmt_opn_dttm,
                                                                                                                    agmt.agmt_cls_dttm               AS agmt_cls_dttm,
                                                                                                                    agmt.agmt_plnd_expn_dttm         AS agmt_plnd_expn_dttm,
                                                                                                                    agmt.agmt_signd_dttm             AS agmt_signd_dttm,
                                                                                                                    agmt.agmt_legly_bindg_ind        AS agmt_legly_bindg_ind,
                                                                                                                    agmt.agmt_src_cd                 AS agmt_src_cd,
                                                                                                                    agmt.agmt_cur_sts_cd             AS agmt_cur_sts_cd,
                                                                                                                    agmt.agmt_cur_sts_rsn_cd         AS agmt_cur_sts_rsn_cd,
                                                                                                                    agmt.agmt_obtnd_cd               AS agmt_obtnd_cd,
                                                                                                                    agmt.agmt_sbtype_cd              AS agmt_sbtype_cd,
                                                                                                                    agmt.agmt_prcsg_dttm             AS agmt_prcsg_dttm,
                                                                                                                    agmt.alt_agmt_name               AS alt_agmt_name,
                                                                                                                    agmt.asset_liabty_cd             AS asset_liabty_cd,
                                                                                                                    agmt.bal_shet_cd                 AS bal_shet_cd,
                                                                                                                    agmt.stmt_cycl_cd                AS stmt_cycl_cd,
                                                                                                                    agmt.stmt_ml_type_cd             AS stmt_ml_type_cd,
                                                                                                                    agmt.prposl_id                   AS prposl_id,
                                                                                                                    agmt.agmt_objtv_type_cd          AS agmt_objtv_type_cd,
                                                                                                                    agmt.fincl_agmt_sbtype_cd        AS fincl_agmt_sbtype_cd,
                                                                                                                    agmt.mkt_risk_type_cd            AS mkt_risk_type_cd,
                                                                                                                    agmt.orignl_maturty_dt           AS orignl_maturty_dt,
                                                                                                                    agmt.risk_expsr_mtgnt_sbtype_cd  AS risk_expsr_mtgnt_sbtype_cd,
                                                                                                                    agmt.bnk_trd_bk_cd               AS bnk_trd_bk_cd,
                                                                                                                    agmt.prcg_meth_sbtype_cd         AS prcg_meth_sbtype_cd,
                                                                                                                    agmt.fincl_agmt_type_cd          AS fincl_agmt_type_cd,
                                                                                                                    agmt.dy_cnt_bss_cd               AS dy_cnt_bss_cd,
                                                                                                                    agmt.frst_prem_due_dt            AS frst_prem_due_dt,
                                                                                                                    agmt.insrnc_agmt_sbtype_cd       AS insrnc_agmt_sbtype_cd,
                                                                                                                    agmt.insrnc_agmt_type_cd         AS insrnc_agmt_type_cd,
                                                                                                                    agmt.ntwk_srvc_agmt_type_cd      AS ntwk_srvc_agmt_type_cd,
                                                                                                                    agmt.frmlty_type_cd              AS frmlty_type_cd,
                                                                                                                    agmt.cntrct_term_num             AS cntrct_term_num,
                                                                                                                    agmt.rate_rprcg_cycl_mth_num     AS rate_rprcg_cycl_mth_num,
                                                                                                                    agmt.cmpnd_int_cycl_mth_num      AS cmpnd_int_cycl_mth_num,
                                                                                                                    agmt.mdterm_int_pmt_cycl_mth_num AS mdterm_int_pmt_cycl_mth_num,
                                                                                                                    agmt.prev_mdterm_int_pmt_dt      AS prev_mdterm_int_pmt_dt,
                                                                                                                    agmt.nxt_mdterm_int_pmt_dt       AS nxt_mdterm_int_pmt_dt,
                                                                                                                    agmt.prev_int_rate_rvsd_dt       AS prev_int_rate_rvsd_dt,
                                                                                                                    agmt.nxt_int_rate_rvsd_dt        AS nxt_int_rate_rvsd_dt,
                                                                                                                    agmt.prev_ref_dt_int_rate        AS prev_ref_dt_int_rate,
                                                                                                                    agmt.nxt_ref_dt_for_int_rate     AS nxt_ref_dt_for_int_rate,
                                                                                                                    agmt.mdterm_cncltn_dt            AS mdterm_cncltn_dt,
                                                                                                                    agmt.stk_flow_clas_in_mth_ind    AS stk_flow_clas_in_mth_ind,
                                                                                                                    agmt.stk_flow_clas_in_term_ind   AS stk_flow_clas_in_term_ind,
                                                                                                                    agmt.lgcy_dscnt_ind              AS lgcy_dscnt_ind,
                                                                                                                    agmt.agmt_idntftn_cd             AS agmt_idntftn_cd,
                                                                                                                    agmt.trmtn_type_cd               AS trmtn_type_cd,
                                                                                                                    agmt.int_pmt_meth_cd             AS int_pmt_meth_cd,
                                                                                                                    agmt.lbr_agmt_desc               AS lbr_agmt_desc,
                                                                                                                    agmt.guartd_imprsns_cnt          AS guartd_imprsns_cnt,
                                                                                                                    agmt.cost_per_imprsn_amt         AS cost_per_imprsn_amt,
                                                                                                                    agmt.guartd_clkthru_cnt          AS guartd_clkthru_cnt,
                                                                                                                    agmt.cost_per_clkthru_amt        AS cost_per_clkthru_amt,
                                                                                                                    agmt.busn_prty_id                AS busn_prty_id,
                                                                                                                    agmt.pmt_pln_type_cd             AS pmt_pln_type_cd,
                                                                                                                    agmt.invc_strem_type_cd          AS invc_strem_type_cd,
                                                                                                                    agmt.modl_crtn_dttm              AS modl_crtn_dttm,
                                                                                                                    agmt.cntnus_srvc_dttm            AS cntnus_srvc_dttm,
                                                                                                                    agmt.bilg_meth_type_cd           AS bilg_meth_type_cd,
                                                                                                                    agmt.src_sys_cd                  AS src_sys_cd,
                                                                                                                    agmt.agmt_eff_dttm               AS agmt_eff_dttm,
                                                                                                                    agmt.modl_eff_dttm               AS modl_eff_dttm,
                                                                                                                    agmt.prcs_id                     AS prcs_id,
                                                                                                                    agmt.modl_actl_end_dttm          AS modl_actl_end_dttm,
                                                                                                                    agmt.tier_type_cd                AS tier_type_cd,
                                                                                                                    agmt.edw_strt_dttm               AS edw_strt_dttm,
                                                                                                                    agmt.edw_end_dttm                AS edw_end_dttm,
                                                                                                                    agmt.vfyd_plcy_ind               AS vfyd_plcy_ind,
                                                                                                                    agmt.src_of_busn_cd              AS src_of_busn_cd,
                                                                                                                    agmt.ovrd_coms_type_cd           AS ovrd_coms_type_cd,
                                                                                                                    agmt.lgcy_plcy_ind               AS lgcy_plcy_ind,
                                                                                                                    agmt.trans_strt_dttm             AS trans_strt_dttm,
                                                                                                                    agmt.nk_src_key                  AS nk_src_key,
                                                                                                                    agmt.agmt_type_cd                AS agmt_type_cd
                                                                                                           FROM     db_t_prod_core.agmt qualify row_number() over(PARTITION BY agmt.nk_src_key,agmt.host_agmt_num ORDER BY agmt.edw_end_dttm DESC) = 1 ) AS agmt_lkp
                                                                                  ON              agmt_lkp.nk_src_key= src.publicid
                                                                                  AND             agmt_lkp.agmt_type_cd= ''PPV'' ) AS xlat_src
                                                  left outer join
                                                                  (
                                                                           SELECT   plcy_mtrc.plcy_mtrc_strt_dttm AS plcy_mtrc_strt_dttm,
                                                                                    plcy_mtrc.plcy_mtrc_end_dttm  AS plcy_mtrc_end_dttm,
                                                                                    plcy_mtrc.tm_prd_cd           AS tm_prd_cd,
                                                                                    plcy_mtrc.plcy_amt            AS plcy_amt,
                                                                                    plcy_mtrc.plcy_cnt            AS plcy_cnt,
                                                                                    plcy_mtrc.uom_cd              AS uom_cd,
                                                                                    plcy_mtrc.uom_type_cd         AS uom_type_cd,
                                                                                    plcy_mtrc.edw_strt_dttm       AS edw_strt_dttm,
                                                                                    plcy_mtrc.edw_end_dttm        AS edw_end_dttm,
                                                                                    plcy_mtrc.agmt_id             AS agmt_id,
                                                                                    plcy_mtrc.insrnc_mtrc_type_cd AS insrnc_mtrc_type_cd
                                                                           FROM     db_t_prod_core.plcy_mtrc qualify row_number() over (PARTITION BY agmt_id,insrnc_mtrc_type_cd ORDER BY edw_end_dttm DESC)=1 ) AS tg_plcy_mtrc
                                                  ON              tg_plcy_mtrc.agmt_id=xlat_src.agmt_id
                                                  AND             tg_plcy_mtrc.insrnc_mtrc_type_cd=xlat_src.out_insrnc_mtrc_type ) src ) );
  -- Component exp_ins_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ins_upd AS
  (
         SELECT sq_pc_policyperiod.agmt_id                                             AS agmt_id,
                sq_pc_policyperiod.out_insrnc_mtrc_type                                AS out_insurance_metric_type,
                sq_pc_policyperiod.plcy_mtrc_strt_dt                                   AS plcy_mtrc_strt_dt,
                sq_pc_policyperiod.plcy_mtrc_end_dt                                    AS plcy_mtrc_end_dt,
                sq_pc_policyperiod.tm_prd_cd                                           AS tm_prd_cd,
                sq_pc_policyperiod.plcy_amt                                            AS plcy_amt,
                sq_pc_policyperiod.plcy_cnt                                            AS plcy_cnt,
                sq_pc_policyperiod.uom_cd                                              AS uom_cd,
                sq_pc_policyperiod.uom_type_cd                                         AS uom_type_cd,
                sq_pc_policyperiod.out_cntrl_id                                        AS out_cntrl_id,
                sq_pc_policyperiod.retired                                             AS retired,
                :prcs_id                                                               AS out_prcs_id,
                current_timestamp                                                      AS out_edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                sq_pc_policyperiod.lkp_edw_strt_dttm                                   AS lkp_edw_strt_dttm,
                sq_pc_policyperiod.lkp_edw_end_dttm                                    AS lkp_edw_end_dttm,
                sq_pc_policyperiod.ins_upd_flag                                        AS ins_upd_flag,
                sq_pc_policyperiod.source_record_id
         FROM   sq_pc_policyperiod );
  -- Component rtr_INSRNC_AGMT_LOB_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_INSRNC_AGMT_LOB_INSERT as
  SELECT exp_ins_upd.agmt_id                   AS agmt_id,
         exp_ins_upd.plcy_mtrc_strt_dt         AS editeffectivedate,
         exp_ins_upd.plcy_mtrc_end_dt          AS periodend,
         exp_ins_upd.plcy_amt                  AS totalpremiumrpt,
         exp_ins_upd.out_cntrl_id              AS cntrl_id,
         exp_ins_upd.out_prcs_id               AS prcs_id,
         exp_ins_upd.out_insurance_metric_type AS insurance_metric_type,
         exp_ins_upd.tm_prd_cd                 AS tm_prd_cd,
         exp_ins_upd.uom_cd                    AS uom_cd,
         exp_ins_upd.uom_type_cd               AS uom_type_cd,
         exp_ins_upd.plcy_cnt                  AS plcy_cnt,
         exp_ins_upd.ins_upd_flag              AS out_ins_upd,
         exp_ins_upd.out_edw_strt_dttm         AS out_edw_strt_dttm,
         exp_ins_upd.out_edw_end_dttm          AS out_edw_end_dttm,
         exp_ins_upd.lkp_edw_strt_dttm         AS lkp_edw_strt_dttm,
         exp_ins_upd.retired                   AS retired,
         exp_ins_upd.lkp_edw_end_dttm          AS lkp_edw_end_dttm,
         exp_ins_upd.source_record_id
  FROM   exp_ins_upd
  WHERE  exp_ins_upd.agmt_id IS NOT NULL
  AND    (
                exp_ins_upd.ins_upd_flag = ''I''
         OR     exp_ins_upd.ins_upd_flag = ''U''
         OR     (
                       exp_ins_upd.retired = 0
                AND    exp_ins_upd.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) );
  
  -- Component rtr_INSRNC_AGMT_LOB_RETIRE, Type ROUTER Output Group RETIRE
  create or replace temporary table rtr_INSRNC_AGMT_LOB_RETIRE as
  SELECT exp_ins_upd.agmt_id                   AS agmt_id,
         exp_ins_upd.plcy_mtrc_strt_dt         AS editeffectivedate,
         exp_ins_upd.plcy_mtrc_end_dt          AS periodend,
         exp_ins_upd.plcy_amt                  AS totalpremiumrpt,
         exp_ins_upd.out_cntrl_id              AS cntrl_id,
         exp_ins_upd.out_prcs_id               AS prcs_id,
         exp_ins_upd.out_insurance_metric_type AS insurance_metric_type,
         exp_ins_upd.tm_prd_cd                 AS tm_prd_cd,
         exp_ins_upd.uom_cd                    AS uom_cd,
         exp_ins_upd.uom_type_cd               AS uom_type_cd,
         exp_ins_upd.plcy_cnt                  AS plcy_cnt,
         exp_ins_upd.ins_upd_flag              AS out_ins_upd,
         exp_ins_upd.out_edw_strt_dttm         AS out_edw_strt_dttm,
         exp_ins_upd.out_edw_end_dttm          AS out_edw_end_dttm,
         exp_ins_upd.lkp_edw_strt_dttm         AS lkp_edw_strt_dttm,
         exp_ins_upd.retired                   AS retired,
         exp_ins_upd.lkp_edw_end_dttm          AS lkp_edw_end_dttm,
         exp_ins_upd.source_record_id
  FROM   exp_ins_upd
  WHERE  exp_ins_upd.ins_upd_flag = ''R''
  AND    exp_ins_upd.retired != 0
  AND    exp_ins_upd.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
  AND    exp_ins_upd.agmt_id IS NOT NULL;
  
  -- Component upd_retire, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_retire AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insrnc_agmt_lob_retire.agmt_id               AS lkp_agmt_id3,
                rtr_insrnc_agmt_lob_retire.insurance_metric_type AS lkp_insrnc_mtrc_type_cd3,
                rtr_insrnc_agmt_lob_retire.lkp_edw_strt_dttm     AS edw_strt_dttm_upd3,
                1                                                AS update_strategy_action,
                rtr_insrnc_agmt_lob_retire.source_record_id
         FROM   rtr_insrnc_agmt_lob_retire );
  -- Component upd_ins_new, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ins_new AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insrnc_agmt_lob_insert.agmt_id               AS agmt_id,
                rtr_insrnc_agmt_lob_insert.editeffectivedate     AS editeffectivedate,
                rtr_insrnc_agmt_lob_insert.periodend             AS periodend,
                rtr_insrnc_agmt_lob_insert.totalpremiumrpt       AS totalpremiumrpt,
                rtr_insrnc_agmt_lob_insert.cntrl_id              AS cntrl_id,
                rtr_insrnc_agmt_lob_insert.prcs_id               AS prcs_id,
                rtr_insrnc_agmt_lob_insert.insurance_metric_type AS insurance_metric_type,
                rtr_insrnc_agmt_lob_insert.tm_prd_cd             AS tm_prd_cd,
                rtr_insrnc_agmt_lob_insert.uom_cd                AS uom_cd,
                rtr_insrnc_agmt_lob_insert.uom_type_cd           AS uom_type_cd,
                rtr_insrnc_agmt_lob_insert.plcy_cnt              AS plcy_cnt,
                rtr_insrnc_agmt_lob_insert.out_edw_strt_dttm     AS out_edw_strt_dttm1,
                rtr_insrnc_agmt_lob_insert.out_edw_end_dttm      AS out_edw_end_dttm1,
                rtr_insrnc_agmt_lob_insert.retired               AS retired1,
                0                                                AS update_strategy_action,
                rtr_insrnc_agmt_lob_insert.source_record_id
         FROM   rtr_insrnc_agmt_lob_insert );
  -- Component exp_pass_to_target_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ins AS
  (
         SELECT upd_ins_new.agmt_id               AS agmt_id,
                upd_ins_new.editeffectivedate     AS editeffectivedate,
                upd_ins_new.periodend             AS periodend,
                upd_ins_new.totalpremiumrpt       AS totalpremiumrpt,
                upd_ins_new.prcs_id               AS prcs_id,
                upd_ins_new.insurance_metric_type AS insurance_metric_type,
                upd_ins_new.tm_prd_cd             AS tm_prd_cd,
                upd_ins_new.uom_cd                AS uom_cd,
                upd_ins_new.uom_type_cd           AS uom_type_cd,
                upd_ins_new.plcy_cnt              AS plcy_cnt,
                upd_ins_new.out_edw_strt_dttm1    AS out_edw_strt_dttm1,
                CASE
                       WHEN upd_ins_new.retired1 = 0 THEN upd_ins_new.out_edw_end_dttm1
                       ELSE current_timestamp
                END AS out_edw_end_dttm,
                upd_ins_new.source_record_id
         FROM   upd_ins_new );
  -- Component exp_retire, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_retire AS
  (
         SELECT upd_retire.lkp_agmt_id3                        AS lkp_agmt_id3,
                upd_retire.lkp_insrnc_mtrc_type_cd3            AS lkp_insrnc_mtrc_type_cd3,
                upd_retire.edw_strt_dttm_upd3                  AS lkp_edw_strt_dttm,
                dateadd (second,-1, current_timestamp  ) AS edw_end_dttm,
                upd_retire.source_record_id
         FROM   upd_retire );
  -- Component PLCY_MTRC_INS_NEW, Type TARGET
  INSERT INTO db_t_prod_core.plcy_mtrc
              (
                          agmt_id,
                          insrnc_mtrc_type_cd,
                          plcy_mtrc_strt_dttm,
                          plcy_mtrc_end_dttm,
                          tm_prd_cd,
                          plcy_amt,
                          plcy_cnt,
                          uom_cd,
                          uom_type_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_pass_to_target_ins.agmt_id               AS agmt_id,
         exp_pass_to_target_ins.insurance_metric_type AS insrnc_mtrc_type_cd,
         exp_pass_to_target_ins.editeffectivedate     AS plcy_mtrc_strt_dttm,
         exp_pass_to_target_ins.periodend             AS plcy_mtrc_end_dttm,
         exp_pass_to_target_ins.tm_prd_cd             AS tm_prd_cd,
         exp_pass_to_target_ins.totalpremiumrpt       AS plcy_amt,
         exp_pass_to_target_ins.plcy_cnt              AS plcy_cnt,
         exp_pass_to_target_ins.uom_cd                AS uom_cd,
         exp_pass_to_target_ins.uom_type_cd           AS uom_type_cd,
         exp_pass_to_target_ins.prcs_id               AS prcs_id,
         exp_pass_to_target_ins.out_edw_strt_dttm1    AS edw_strt_dttm,
         exp_pass_to_target_ins.out_edw_end_dttm      AS edw_end_dttm
  FROM   exp_pass_to_target_ins;
  
  -- Component PLCY_MTRC_retire, Type TARGET
  merge
  INTO         db_t_prod_core.plcy_mtrc
  USING        exp_retire
  ON (
                            plcy_mtrc.agmt_id = exp_retire.lkp_agmt_id3
               AND          plcy_mtrc.insrnc_mtrc_type_cd = exp_retire.lkp_insrnc_mtrc_type_cd3
               AND          plcy_mtrc.edw_strt_dttm = exp_retire.lkp_edw_strt_dttm)
  WHEN matched THEN
  UPDATE
  SET    agmt_id = exp_retire.lkp_agmt_id3,
         insrnc_mtrc_type_cd = exp_retire.lkp_insrnc_mtrc_type_cd3,
         edw_strt_dttm = exp_retire.lkp_edw_strt_dttm,
         edw_end_dttm = exp_retire.edw_end_dttm;
  
  -- Component PLCY_MTRC_retire, Type Post SQL
  UPDATE db_t_prod_core.plcy_mtrc
 SET    edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT agmt_id,
                                         insrnc_mtrc_type_cd,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY agmt_id, insrnc_mtrc_type_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.plcy_mtrc
                         WHERE           insrnc_mtrc_type_cd IN (''TTLPLCYCST'',
                                                                 ''ERNDPREM'',
                                                                 ''PREMDISC'',
                                                                 ''PREMDUE'',
                                                                 ''PREM'',
                                                                 ''TOTTRMPREM'',
                                                                 ''UNERNPREM'',
                                                                 ''3DLTPY'',
                                                                 ''10DLTPY'') ) a
 
  WHERE  plcy_mtrc.edw_strt_dttm = a.edw_strt_dttm
  AND    plcy_mtrc.agmt_id=a.agmt_id
  AND    plcy_mtrc.insrnc_mtrc_type_cd=a.insrnc_mtrc_type_cd
  AND    cast(plcy_mtrc.edw_end_dttm AS DATE)=''9999-12-31''
  AND    lead1 IS NOT NULL;

END;
';