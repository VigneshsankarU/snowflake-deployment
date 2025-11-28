-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_MODL_RUN_INS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
declare
	
run_id STRING;
 START_DTTM TIMESTAMP;
 END_DTTM TIMESTAMP;
 PRCS_ID VARCHAR;

BEGIN 
run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);

  -- Component SQ_pc_modl_run_x, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_modl_run_x AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS modelname,
                $2 AS modelrundttm,
                $3 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT modelname_stg ,
                                                                  modelrundttm_stg
                                                  FROM            (
                                                                                  SELECT          cast(''ARS'' AS VARCHAR (50))                                                                                          modelname_stg ,
                                                                                                  coalesce(pcx_palineratingfactor_alfa.arsscoredateoutputpoli_stg, cast(''1900-01-01 00:00:00.000000'' AS timestamp)) AS modelrundttm_stg
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left outer join db_t_prod_stag.pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                  join            db_t_prod_stag.pcx_palineratingfactor_alfa
                                                                                  ON              pc_policyperiod.id_stg=pcx_palineratingfactor_alfa.branchid_stg
                                                                                  left outer join db_t_prod_stag.pctl_job
                                                                                  ON              pctl_job.id_stg=pc_job.subtype_stg
                                                                                  WHERE
                                                                                                  /* PCX_PALINERATINGFACTOR_ALFA.ARSSCOREDATEOUTPUTPOLI_STG IS NOT NULL */
                                                                                                  pcx_palineratingfactor_alfa.arsratedscorepoli_stg IS NOT NULL
                                                                                  AND             pctl_policyperiodstatus.typecode_stg<>''Temporary''
                                                                                  AND             pctl_job.typecode_stg IN (''Submission'',
                                                                                                                            ''PolicyChange'',
                                                                                                                            ''Renewal'')
                                                                                  AND             pcx_palineratingfactor_alfa.updatetime_stg> (:start_dttm)
                                                                                  AND             pcx_palineratingfactor_alfa.updatetime_stg<= (:end_dttm)
                                                                                  UNION
                                                                                  SELECT          ''LVP''                                                                                                               modelname_stg ,
                                                                                                  coalesce(pcx_palineratingfactor_alfa.lvpscoredateoutputpoli_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) AS modelrundttm_stg
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left outer join db_t_prod_stag.pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                  join            db_t_prod_stag.pcx_palineratingfactor_alfa
                                                                                  ON              pc_policyperiod.id_stg =pcx_palineratingfactor_alfa.branchid_stg
                                                                                  left outer join db_t_prod_stag.pctl_job
                                                                                  ON              pctl_job.id_stg =pc_job.subtype_stg
                                                                                  WHERE
                                                                                                  /* PCX_PALINERATINGFACTOR_ALFA.LVPSCOREDATEOUTPUTPOLI_STG  IS NOT NULL */
                                                                                                  pcx_palineratingfactor_alfa.lvpcalcscore_stg IS NOT NULL
                                                                                  AND             pctl_policyperiodstatus.typecode_stg <>''Temporary''
                                                                                  AND             pctl_job.typecode_stg IN (''Submission'',
                                                                                                                            ''PolicyChange'',
                                                                                                                            ''Renewal'')
                                                                                  AND             pcx_palineratingfactor_alfa.updatetime_stg > (:start_dttm)
                                                                                  AND             pcx_palineratingfactor_alfa.updatetime_stg <= (:end_dttm)
                                                                                  UNION
                                                                                  SELECT          ''ISE''                                                                                     modelname_stg ,
                                                                                                  coalesce(pc_pamodifier.createtime_stg,cast(''1900-01-01 00:00:00.000000'' AS timestamp)) AS modelrundttm_stg
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left outer join db_t_prod_stag.pc_pamodifier
                                                                                  ON              branchid_stg = pc_policyperiod.id_stg
                                                                                                  /* AND  PATTERNCODE=''PAINSRATINGSCORE_ALFA'' */
                                                                                  left outer join db_t_prod_stag.pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                  left outer join db_t_prod_stag.pctl_job
                                                                                  ON              pctl_job.id_stg=pc_job.subtype_stg
                                                                                  WHERE           pctl_policyperiodstatus.typecode_stg<>''Temporary''
                                                                                  AND             pctl_job.typecode_stg IN (''Submission'',
                                                                                                                            ''PolicyChange'',
                                                                                                                            ''Renewal'')
                                                                                  AND             pc_pamodifier.updatetime_stg > (:start_dttm)
                                                                                  AND             pc_pamodifier.updatetime_stg <= (:end_dttm)
                                                                                  UNION
                                                                                  SELECT          ''PDT''                     modelname_stg ,
                                                                                                  cast(NULL AS timestamp)AS modelrundttm_stg
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left outer join db_t_prod_stag.pc_effectivedatedfields eff
                                                                                  ON              eff.branchid_stg = pc_policyperiod.id_stg
                                                                                  inner join      db_t_prod_stag.pcx_ratingtierppv2_alfa r
                                                                                  ON              r.id_stg=eff.ratingtierppv2_alfa_stg
                                                                                  left outer join db_t_prod_stag.pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                  left outer join db_t_prod_stag.pctl_job
                                                                                  ON              pctl_job.id_stg=pc_job.subtype_stg
                                                                                  WHERE           pctl_policyperiodstatus.typecode_stg<>''Temporary''
                                                                                  AND             pctl_job.typecode_stg IN (''Submission'',
                                                                                                                            ''PolicyChange'',
                                                                                                                            ''Renewal'')
                                                                                  AND             r.updatetime_stg > (:start_dttm)
                                                                                  AND             r.updatetime_stg <= (:end_dttm)
                                                                                  UNION
                                                                                  SELECT          ''UNMT''                     modelname_stg ,
                                                                                                  cast(NULL AS timestamp) AS modelrundttm_stg
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left outer join db_t_prod_stag.pc_effectivedatedfields eff
                                                                                  ON              eff.branchid_stg = pc_policyperiod.id_stg
                                                                                  inner join      db_t_prod_stag.pcx_ratingtierppv2_alfa r
                                                                                  ON              r.id_stg=eff.ratingtierppv2_alfa_stg
                                                                                  left outer join db_t_prod_stag.pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                  left outer join db_t_prod_stag.pctl_job
                                                                                  ON              pctl_job.id_stg=pc_job.subtype_stg
                                                                                  WHERE           pctl_policyperiodstatus.typecode_stg<>''Temporary''
                                                                                  AND             pctl_job.typecode_stg IN (''Submission'',
                                                                                                                            ''PolicyChange'',
                                                                                                                            ''Renewal'')
                                                                                  AND             r.updatetime_stg > (:start_dttm)
                                                                                  AND             r.updatetime_stg <= (:end_dttm)
                                                                                  UNION
                                                                                  SELECT          ''COLLT''                    modelname_stg ,
                                                                                                  cast(NULL AS timestamp) AS modelrundttm_stg
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left outer join db_t_prod_stag.pc_effectivedatedfields eff
                                                                                  ON              eff.branchid_stg = pc_policyperiod.id_stg
                                                                                  inner join      db_t_prod_stag.pcx_ratingtierppv2_alfa r
                                                                                  ON              r.id_stg=eff.ratingtierppv2_alfa_stg
                                                                                  left outer join db_t_prod_stag.pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                  left outer join db_t_prod_stag.pctl_job
                                                                                  ON              pctl_job.id_stg=pc_job.subtype_stg
                                                                                  WHERE           pctl_policyperiodstatus.typecode_stg<>''Temporary''
                                                                                  AND             pctl_job.typecode_stg IN (''Submission'',
                                                                                                                            ''PolicyChange'',
                                                                                                                            ''Renewal'')
                                                                                  AND             r.updatetime_stg > (:start_dttm)
                                                                                  AND             r.updatetime_stg <= (:end_dttm)
                                                                                  UNION
                                                                                  SELECT          ''SLT''                      modelname_stg ,
                                                                                                  cast(NULL AS timestamp) AS modelrundttm_stg
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left outer join db_t_prod_stag.pc_effectivedatedfields eff
                                                                                  ON              eff.branchid_stg = pc_policyperiod.id_stg
                                                                                  inner join      db_t_prod_stag.pcx_ratingtierppv2_alfa r
                                                                                  ON              r.id_stg=eff.ratingtierppv2_alfa_stg
                                                                                  left outer join db_t_prod_stag.pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg= pc_policyperiod.status_stg
                                                                                  left outer join db_t_prod_stag.pctl_job
                                                                                  ON              pctl_job.id_stg=pc_job.subtype_stg
                                                                                  WHERE           pctl_policyperiodstatus.typecode_stg<>''Temporary''
                                                                                  AND             pctl_job.typecode_stg IN (''Submission'',
                                                                                                                            ''PolicyChange'',
                                                                                                                            ''Renewal'')
                                                                                  AND             r.updatetime_stg > (:start_dttm)
                                                                                  AND             r.updatetime_stg <= (:end_dttm)
                                                                                  UNION
                                                                                  SELECT          ''COMPT''                    modelname_stg ,
                                                                                                  cast(NULL AS timestamp) AS modelrundttm_stg
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left outer join db_t_prod_stag.pc_effectivedatedfields eff
                                                                                  ON              eff.branchid_stg = pc_policyperiod.id_stg
                                                                                  inner join      db_t_prod_stag.pcx_ratingtierppv2_alfa r
                                                                                  ON              r.id_stg=eff.ratingtierppv2_alfa_stg
                                                                                  left outer join db_t_prod_stag.pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                  left outer join db_t_prod_stag.pctl_job
                                                                                  ON              pctl_job.id_stg=pc_job.subtype_stg
                                                                                  WHERE           pctl_policyperiodstatus.typecode_stg<>''Temporary''
                                                                                  AND             pctl_job.typecode_stg IN (''Submission'',
                                                                                                                            ''PolicyChange'',
                                                                                                                            ''Renewal'')
                                                                                  AND             r.updatetime_stg > (:start_dttm)
                                                                                  AND             r.updatetime_stg <= (:end_dttm)
                                                                                  UNION
                                                                                  SELECT          ''BIT''                      modelname_stg ,
                                                                                                  cast(NULL AS timestamp) AS modelrundttm_stg
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left outer join db_t_prod_stag.pc_effectivedatedfields eff
                                                                                  ON              eff.branchid_stg = pc_policyperiod.id_stg
                                                                                  inner join      db_t_prod_stag.pcx_ratingtierppv2_alfa r
                                                                                  ON              r.id_stg =eff.ratingtierppv2_alfa_stg
                                                                                  left outer join db_t_prod_stag.pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                  left outer join db_t_prod_stag.pctl_job
                                                                                  ON              pctl_job.id_stg =pc_job.subtype_stg
                                                                                  WHERE           pctl_policyperiodstatus.typecode_stg <>''Temporary''
                                                                                  AND             pctl_job.typecode_stg IN (''Submission'',
                                                                                                                            ''PolicyChange'',
                                                                                                                            ''Renewal'')
                                                                                  AND             r.updatetime_stg > (:start_dttm)
                                                                                  AND             r.updatetime_stg <= (:end_dttm)
                                                                                  UNION
                                                                                  SELECT          ''MPT''                      modelname_stg ,
                                                                                                  cast(NULL AS timestamp) AS modelrundttm_stg
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left outer join db_t_prod_stag.pc_effectivedatedfields eff
                                                                                  ON              eff.branchid_stg = pc_policyperiod.id_stg
                                                                                  inner join      db_t_prod_stag.pcx_ratingtierppv2_alfa r
                                                                                  ON              r.id_stg=eff.ratingtierppv2_alfa_stg
                                                                                  left outer join db_t_prod_stag.pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                  left outer join db_t_prod_stag.pctl_job
                                                                                  ON              pctl_job.id_stg=pc_job.subtype_stg
                                                                                  WHERE           pctl_policyperiodstatus.typecode_stg<>''Temporary''
                                                                                  AND             pctl_job.typecode_stg IN (''Submission'',
                                                                                                                            ''PolicyChange'',
                                                                                                                            ''Renewal'')
                                                                                  AND             r.updatetime_stg > (:start_dttm)
                                                                                  AND             r.updatetime_stg <= (:end_dttm)
                                                                                  UNION
                                                                                  SELECT DISTINCT ''LEXIS NEXIS''                                   AS modelname_stg,
                                                                                                  pcx_insurancereport_alfa.insurancescoredate_stg AS modelrundttm_stg
                                                                                  FROM            db_t_prod_stag.pc_job
                                                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  left outer join db_t_prod_stag.pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg= pc_policyperiod.status_stg
                                                                                  join            db_t_prod_stag.pcx_insurancereport_alfa
                                                                                  ON              pc_policyperiod.id_stg=pcx_insurancereport_alfa.branchid_stg
                                                                                  join            db_t_prod_stag.pctl_jurisdiction
                                                                                  ON              pc_policyperiod.basestate_stg=pctl_jurisdiction.id_stg
                                                                                  join            db_t_prod_stag.pc_policycontactrole
                                                                                  ON              pc_policycontactrole.id_stg=pcx_insurancereport_alfa.policycontactroleid_stg
                                                                                  join            db_t_prod_stag.pc_contact
                                                                                  ON              pc_contact.id_stg=pc_policycontactrole.contactdenorm_stg
                                                                                  join            db_t_prod_stag.pctl_policycontactrole
                                                                                  ON              pctl_policycontactrole.id_stg=pc_policycontactrole.subtype_stg
                                                                                  join            db_t_prod_stag.pctl_contact
                                                                                  ON              pctl_contact.id_stg=pc_contact.subtype_stg
                                                                                  join            db_t_prod_stag.pctl_job
                                                                                  ON              pctl_job.id_stg=pc_job.subtype_stg
                                                                                  WHERE           pc_contact.addressbookuid_stg IS NOT NULL
                                                                                  AND             addressbookuid_stg IS NOT NULL
                                                                                  AND             insurancescoredate_stg IS NOT NULL
                                                                                  AND             insurancescore_stg IS NOT NULL
                                                                                  AND             pctl_policyperiodstatus.typecode_stg<>''Temporary''
                                                                                  AND             pctl_job.typecode_stg IN (''Submission'',
                                                                                                                            ''PolicyChange'',
                                                                                                                            ''Renewal'')
                                                                                  AND             pcx_insurancereport_alfa.updatetime_stg> (:start_dttm)
                                                                                  AND             pcx_insurancereport_alfa.updatetime_stg <= (:end_dttm)
                                                                                                  /* -EIM-40755 */
                                                                                  OR              (
                                                                                                                  pc_policyperiod.updatetime_stg> (:start_dttm)
                                                                                                  AND             pc_policyperiod.updatetime_stg <= (:end_dttm)))a
                                                  WHERE           a.modelrundttm_stg IS NOT NULL ) src ) );
  -- Component exp_pass_from_src, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_src AS
  (
         SELECT sq_pc_modl_run_x.modelname    AS model_name,
                sq_pc_modl_run_x.modelrundttm AS model_run_dttm,
                sq_pc_modl_run_x.source_record_id
         FROM   sq_pc_modl_run_x );
  -- Component LKP_ANLTCL_MODL, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_anltcl_modl AS
  (
            SELECT    lkp.modl_id,
                      exp_pass_from_src.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_src.source_record_id ORDER BY lkp.modl_id ASC,lkp.modl_name ASC) rnk
            FROM      exp_pass_from_src
            left join
                      (
                               SELECT   anltcl_modl.modl_id   AS modl_id,
                                        anltcl_modl.modl_name AS modl_name
                               FROM     db_t_prod_core.anltcl_modl
                               ORDER BY modl_from_dttm DESC
                                        /*  */
                      ) lkp
            ON        lkp.modl_name = exp_pass_from_src.model_name qualify rnk = 1 );
  -- Component exp_SrcFields, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_srcfields AS
  (
             SELECT     lkp_anltcl_modl.modl_id                                                AS in_modl_id,
                        seq_modl_run.NEXTVAL                                                   AS in_modl_run_id,
                        exp_pass_from_src.model_run_dttm                                       AS in_modl_run_dttm,
                        :prcs_id                                                               AS in_prcs_id,
                        exp_pass_from_src.model_run_dttm                                       AS in_modl_run_strt_dttm,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS in_modl_run_end_dttm,
                        current_timestamp                                                      AS in_edw_strt_dttm,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS in_edw_end_dttm,
                        exp_pass_from_src.source_record_id
             FROM       exp_pass_from_src
             inner join lkp_anltcl_modl
             ON         exp_pass_from_src.source_record_id = lkp_anltcl_modl.source_record_id );
  -- Component LKP_MODL_RUN, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_modl_run AS
  (
            SELECT    lkp.modl_id,
                      lkp.modl_run_id,
                      lkp.modl_run_dttm,
                      lkp.modl_run_strt_dttm,
                      lkp.edw_strt_dttm,
                      exp_srcfields.in_modl_id       AS in_modl_id,
                      exp_srcfields.in_modl_run_dttm AS in_modl_run_dttm,
                      exp_srcfields.source_record_id,
                      row_number() over(PARTITION BY exp_srcfields.source_record_id ORDER BY lkp.modl_id ASC,lkp.modl_run_id ASC,lkp.modl_run_dttm ASC,lkp.modl_run_strt_dttm ASC,lkp.edw_strt_dttm ASC) rnk
            FROM      exp_srcfields
            left join
                      (
                               SELECT   modl_run.modl_run_id        AS modl_run_id,
                                        modl_run.modl_run_strt_dttm AS modl_run_strt_dttm,
                                        modl_run.edw_strt_dttm      AS edw_strt_dttm,
                                        modl_run.edw_end_dttm       AS edw_end_dttm,
                                        modl_run.modl_id            AS modl_id,
                                        modl_run.modl_run_dttm      AS modl_run_dttm
                               FROM     db_t_prod_core.modl_run qualify row_number() over(PARTITION BY modl_run_dttm,modl_id ORDER BY edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.modl_id = exp_srcfields.in_modl_id
            AND       lkp.modl_run_dttm = exp_srcfields.in_modl_run_dttm qualify rnk = 1 );
  -- Component exp_CDC_Check, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_cdc_check AS
  (
             SELECT     exp_srcfields.in_modl_id                                AS in_modl_id,
                        exp_srcfields.in_modl_run_id                            AS in_modl_run_id,
                        exp_srcfields.in_modl_run_dttm                          AS in_modl_run_dttm,
                        exp_srcfields.in_prcs_id                                AS in_prcs_id,
                        exp_srcfields.in_modl_run_strt_dttm                     AS in_modl_run_strt_dttm,
                        exp_srcfields.in_modl_run_end_dttm                      AS in_modl_run_end_dttm,
                        exp_srcfields.in_edw_strt_dttm                          AS in_edw_strt_dttm,
                        exp_srcfields.in_edw_end_dttm                           AS in_edw_end_dttm,
                        lkp_modl_run.modl_id                                    AS lkp_modl_id,
                        lkp_modl_run.modl_run_id                                AS lkp_modl_run_id,
                        lkp_modl_run.modl_run_dttm                              AS lkp_modl_run_dttm,
                        lkp_modl_run.edw_strt_dttm                              AS lkp_edw_strt_dttm,
                        md5 ( to_char ( exp_srcfields.in_modl_run_strt_dttm ) ) AS v_src_md5,
                        md5 ( to_char ( lkp_modl_run.modl_run_strt_dttm ) )     AS v_tgt_md5,
                        CASE
                                   WHEN v_tgt_md5 IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_src_md5 = v_tgt_md5 THEN ''R''
                                                         ELSE ''U''
                                              END
                        END AS o_cdc_check,
                        exp_srcfields.source_record_id
             FROM       exp_srcfields
             inner join lkp_modl_run
             ON         exp_srcfields.source_record_id = lkp_modl_run.source_record_id );
  -- Component rtr_MODL_RUN_Insert, Type ROUTER Output Group Insert
  create or replace temporary table rtr_modl_run_insert as
  SELECT exp_cdc_check.in_modl_id            AS in_modl_id,
         exp_cdc_check.in_modl_run_id        AS in_modl_run_id,
         exp_cdc_check.in_modl_run_dttm      AS in_modl_run_dttm,
         exp_cdc_check.in_prcs_id            AS in_prcs_id,
         exp_cdc_check.in_modl_run_strt_dttm AS in_modl_run_strt_dttm,
         exp_cdc_check.in_modl_run_end_dttm  AS in_modl_run_end_dttm,
         exp_cdc_check.in_edw_strt_dttm      AS in_edw_strt_dttm,
         exp_cdc_check.in_edw_end_dttm       AS in_edw_end_dttm,
         exp_cdc_check.lkp_modl_id           AS lkp_modl_id,
         exp_cdc_check.lkp_modl_run_id       AS lkp_modl_run_id,
         exp_cdc_check.lkp_modl_run_dttm     AS lkp_modl_run_dttm,
         exp_cdc_check.lkp_edw_strt_dttm     AS lkp_edw_strt_dttm,
         exp_cdc_check.o_cdc_check           AS o_cdc_check,
         exp_cdc_check.source_record_id
  FROM   exp_cdc_check
  WHERE  exp_cdc_check.o_cdc_check = ''I'';
  
  -- Component rtr_MODL_RUN_Update, Type ROUTER Output Group Update
  create or replace temporary table rtr_modl_run_update as
  SELECT exp_cdc_check.in_modl_id            AS in_modl_id,
         exp_cdc_check.in_modl_run_id        AS in_modl_run_id,
         exp_cdc_check.in_modl_run_dttm      AS in_modl_run_dttm,
         exp_cdc_check.in_prcs_id            AS in_prcs_id,
         exp_cdc_check.in_modl_run_strt_dttm AS in_modl_run_strt_dttm,
         exp_cdc_check.in_modl_run_end_dttm  AS in_modl_run_end_dttm,
         exp_cdc_check.in_edw_strt_dttm      AS in_edw_strt_dttm,
         exp_cdc_check.in_edw_end_dttm       AS in_edw_end_dttm,
         exp_cdc_check.lkp_modl_id           AS lkp_modl_id,
         exp_cdc_check.lkp_modl_run_id       AS lkp_modl_run_id,
         exp_cdc_check.lkp_modl_run_dttm     AS lkp_modl_run_dttm,
         exp_cdc_check.lkp_edw_strt_dttm     AS lkp_edw_strt_dttm,
         exp_cdc_check.o_cdc_check           AS o_cdc_check,
         exp_cdc_check.source_record_id
  FROM   exp_cdc_check
  WHERE  exp_cdc_check.o_cdc_check = ''U'';
  
  -- Component tgt_MODL_RUN_ins_upd, Type TARGET
  INSERT INTO db_t_prod_core.modl_run
              (
                          modl_id,
                          modl_run_id,
                          modl_run_dttm,
                          prcs_id,
                          modl_run_strt_dttm,
                          modl_run_end_dttm,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT rtr_modl_run_update.in_modl_id            AS modl_id,
         rtr_modl_run_update.lkp_modl_run_id       AS modl_run_id,
         rtr_modl_run_update.in_modl_run_dttm      AS modl_run_dttm,
         rtr_modl_run_update.in_prcs_id            AS prcs_id,
         rtr_modl_run_update.in_modl_run_strt_dttm AS modl_run_strt_dttm,
         rtr_modl_run_update.in_modl_run_end_dttm  AS modl_run_end_dttm,
         rtr_modl_run_update.in_edw_strt_dttm      AS edw_strt_dttm,
         rtr_modl_run_update.in_edw_end_dttm       AS edw_end_dttm
  FROM   rtr_modl_run_update;
  
  -- Component upd_MODL_RUN, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_modl_run AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_modl_run_update.lkp_modl_id       AS lkp_modl_id3,
                rtr_modl_run_update.lkp_modl_run_dttm AS lkp_modl_run_dttm3,
                rtr_modl_run_update.lkp_edw_strt_dttm AS lkp_edw_strt_dttm3,
                1                                     AS update_strategy_action,
                rtr_modl_run_update.source_record_id
         FROM   rtr_modl_run_update );
  -- Component exp_DateExpiry, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_dateexpiry AS
  (
         SELECT upd_modl_run.lkp_modl_id3             AS lkp_modl_id3,
                upd_modl_run.lkp_modl_run_dttm3       AS lkp_modl_run_dttm3,
                upd_modl_run.lkp_edw_strt_dttm3       AS lkp_edw_strt_dttm3,
                dateadd(''second'', - 1, current_timestamp) AS o_dateexpiry,
                upd_modl_run.source_record_id
         FROM   upd_modl_run );
  -- Component tgt_MODL_RUN_ins, Type TARGET
  INSERT INTO db_t_prod_core.modl_run
              (
                          modl_id,
                          modl_run_id,
                          modl_run_dttm,
                          prcs_id,
                          modl_run_strt_dttm,
                          modl_run_end_dttm,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT rtr_modl_run_insert.in_modl_id            AS modl_id,
         rtr_modl_run_insert.in_modl_run_id        AS modl_run_id,
         rtr_modl_run_insert.in_modl_run_dttm      AS modl_run_dttm,
         rtr_modl_run_insert.in_prcs_id            AS prcs_id,
         rtr_modl_run_insert.in_modl_run_strt_dttm AS modl_run_strt_dttm,
         rtr_modl_run_insert.in_modl_run_end_dttm  AS modl_run_end_dttm,
         rtr_modl_run_insert.in_edw_strt_dttm      AS edw_strt_dttm,
         rtr_modl_run_insert.in_edw_end_dttm       AS edw_end_dttm
  FROM   rtr_modl_run_insert;
  
  -- Component tgt_MODL_RUN_upd, Type TARGET
  merge
  INTO         db_t_prod_core.modl_run
  USING        exp_dateexpiry
  ON (
                            modl_run.modl_id = exp_dateexpiry.lkp_modl_id3
               AND          modl_run.modl_run_dttm = exp_dateexpiry.lkp_modl_run_dttm3
               AND          modl_run.edw_strt_dttm = exp_dateexpiry.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    modl_id = exp_dateexpiry.lkp_modl_id3,
         modl_run_dttm = exp_dateexpiry.lkp_modl_run_dttm3,
         edw_strt_dttm = exp_dateexpiry.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_dateexpiry.o_dateexpiry;

END;
';