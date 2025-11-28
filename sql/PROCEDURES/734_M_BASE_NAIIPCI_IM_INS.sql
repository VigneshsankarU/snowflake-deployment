-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_NAIIPCI_IM_INS("WORKLET_NAME" VARCHAR)
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
  GL_END_MTH_ID int;
  P_DEFAULT_STR_CD STRING;
  CC_BOY string;
  CC_EOY string;
  CC_EOFQ string;
  PC_EOY string;
  PC_BOY string;

BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
GL_END_MTH_ID :=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''GL_END_MTH_ID'' order by insert_ts desc limit 1);
P_DEFAULT_STR_CD :=  (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''P_DEFAULT_STR_CD'' order by insert_ts desc limit 1);
CC_BOY := (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''CC_BOY'' order by insert_ts desc limit 1);
CC_EOY := (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''CC_EOY'' order by insert_ts desc limit 1);
CC_EOFQ := (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''CC_EOFQ'' order by insert_ts desc limit 1);
PC_EOY := (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PC_EOY'' order by insert_ts desc limit 1);
PC_BOY := (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PC_BOY'' order by insert_ts desc limit 1);


  -- PIPELINE START FOR 1
  -- Component SQ_pc_policyperiod, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_policyperiod AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS companynumber,
                $2  AS lineofbusinesscode,
                $3  AS statecode,
                $4  AS callyear,
                $5  AS accountingyear,
                $6  AS expperiodyear,
                $7  AS expperiodmonth,
                $8  AS expeperiodday,
                $9  AS classificationcode,
                $10 AS typeoflosscode,
                $11 AS policyeffectiveyear,
                $12 AS aslob,
                $13 AS policyidentificationcode,
                $14 AS policytermcode,
                $15 AS claimidentifier,
                $16 AS claimantidentifier,
                $17 AS writtenpremium,
                $18 AS paidlosses,
                $19 AS paidnumberofclaims,
                $20 AS outstandinglosses,
                $21 AS outstandingnoofclaims,
                $22 AS policynumber,
                $23 AS policyperiodid,
                $24 AS policyidentifier,
                $25 AS vin,
                $26 AS exposurenumber,
                $27 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT*
                                         FROM   (
                                                         SELECT   companynumber,
                                                                  lob,
                                                                  statecode,
                                                                  callyear,
                                                                  accountingyear,
                                                                  expperiodyear,
                                                                  expperiodmonth,
                                                                  expperiodday,
                                                                  classificationcode,
                                                                  typeoflosscode,
                                                                  policy_eff_yr,
                                                                  aslob,
                                                                  policyidcode,
                                                                  CASE
                                                                           WHEN policyterm = 00 THEN ''01''
                                                                           WHEN policyterm <10 THEN cast(''0''
                                                                                             ||cast(policyterm AS VARCHAR(2)) AS VARCHAR(2))
                                                                           ELSE cast(policyterm AS VARCHAR(2))
                                                                  END AS policyterm,
                                                                  claimidentifier,
                                                                  claimantidentifier,
                                                                  SUM(premium) AS wrtprem,
                                                                  paidlosses,
                                                                  paidnoofclaims,
                                                                  outstandinglosses,
                                                                  outstandingclaims,
                                                                  policynumber_stg,
                                                                  policyperiodid,
                                                                  policyidentifier,
                                                                  vin,
                                                                  exposurenumber
                                                         FROM     (
                                                                                  SELECT DISTINCT
                                                                                                  CASE
                                                                                                                  WHEN uw.publicid_stg=''AMI'' THEN ''0005''
                                                                                                                  WHEN uw.publicid_stg=''AMG'' THEN ''0196''
                                                                                                                  WHEN uw.publicid_stg=''AIC'' THEN ''0050''
                                                                                                                  WHEN uw.publicid_stg=''AGI'' THEN ''0318''
                                                                                                  END  AS companynumber,
                                                                                                  ''06'' AS lob,
                                                                                                  CASE
                                                                                                                  WHEN st.typecode_stg=''AL'' THEN ''01''
                                                                                                                  WHEN st.typecode_stg=''GA'' THEN ''10''
                                                                                                                  WHEN st.typecode_stg=''MS'' THEN ''23''
                                                                                                  END                                             AS statecode,
                                                                                                  extract(year FROM cast(:PC_EOY AS timestamp))+1 AS callyear,
                                                                                                  extract(year FROM cast(:PC_EOY AS timestamp))   AS accountingyear,
                                                                                                  ''0000''                                             expperiodyear,
                                                                                                  ''00''                                               expperiodmonth,
                                                                                                  ''00''                                               expperiodday,
                                                                                                  CASE
                                                                                                                  WHEN mt.name_stg = ''Inboard'' THEN ''703100''
                                                                                                                  WHEN mt.name_stg = ''Outboard'' THEN ''702100''
                                                                                                                  WHEN (
                                                                                                                                                  mt.name_stg = ''Inboard/Outboard'')
                                                                                                                  OR              (
                                                                                                                                                  mt.name_stg IS NULL
                                                                                                                                  AND             pc_policyline.papolicytype_alfa_stg IS NOT NULL ) THEN ''799900''
                                                                                                                  ELSE ''725000''
                                                                                                  END  AS classificationcode,
                                                                                                  ''00'' AS typeoflosscode,
                                                                                                  CASE
                                                                                                                  WHEN pctl_job.typecode_stg=''Cancellation'' THEN year(pc_policyperiod.cancellationdate_stg)
                                                                                                                  ELSE year(pc_policyperiod.periodstart_stg)
                                                                                                  END   AS policy_eff_yr,
                                                                                                  ''090'' AS aslob,
                                                                                                  ''010'' AS policyidcode,
                                                                                                  abs(round(
                                                                                                  CASE
                                                                                                                  WHEN pc_policyperiod.cancellationdate_stg IS NOT NULL THEN months_between(cast(pc_policyperiod.cancellationdate_stg AS DATE),cast( pc_policyperiod.editeffectivedate_stg AS DATE))
                                                                                                                  ELSE months_between(cast(pc_policyperiod.periodend_stg AS                                                              DATE),cast( pc_policyperiod.editeffectivedate_stg AS DATE))
                                                                                                  END,0)) AS policyterm,
                                                                                                  ''0''     AS claimidentifier,
                                                                                                  ''000''   AS claimantidentifier,
                                                                                                  ''0''     AS paidlosses,
                                                                                                  ''0''     AS paidnoofclaims,
                                                                                                  ''0''     AS outstandinglosses,
                                                                                                  ''0''     AS outstandingclaims,
                                                                                                  pc_policyperiod.policynumber_stg,
                                                                                                  pc_policyperiod.publicid_stg AS policyperiodid,
                                                                                                  pc_job.jobnumber_stg         AS policyidentifier,
                                                                                                  pv.vin_stg                   AS vin,
                                                                                                  ''00''                         AS exposurenumber,
                                                                                                  phth.amount_stg              AS premium ,
                                                                                                  CASE
                                                                                                                  WHEN pc_policyperiod.editeffectivedate_stg >= pc_policyperiod.modeldate_stg
                                                                                                                  AND             pc_policyperiod.editeffectivedate_stg>= coalesce(pt.confirmationdate_alfa_stg, cast(''1900-01-01 00:00:00.000000'' AS timestamp)) THEN cast(pc_policyperiod.editeffectivedate_stg AS timestamp)
                                                                                                                  WHEN coalesce(pt.confirmationdate_alfa_stg, cast(''1900-01-01 00:00:00.000000'' AS timestamp)) >= pc_policyperiod.modeldate_stg THEN cast(coalesce(pt.confirmationdate_alfa_stg, cast(''1900-01-01 00:00:00.000000'' AS timestamp)) AS timestamp)
                                                                                                                  ELSE cast( pc_policyperiod.modeldate_stg AS timestamp)
                                                                                                  END date_filter,
                                                                                                  phth.id_stg
                                                                                  FROM            db_t_prod_stag.pc_patransaction phth
                                                                                  join
                                                                                                  (
                                                                                                         SELECT pc_policyperiod.policynumber_stg,
                                                                                                                pc_pacost.id_stg,
                                                                                                                pc_pacost.chargepattern_stg,
                                                                                                                pc_pacost.subtype_stg,
                                                                                                                pc_pacost.periltype_alfa_stg,
                                                                                                                personalvehiclecov_stg
                                                                                                         FROM   db_t_prod_stag.pc_pacost
                                                                                                         join   db_t_prod_stag.pc_policyperiod
                                                                                                         ON     pc_pacost.branchid_stg=pc_policyperiod.id_stg ) expandedcosttable
                                                                                  ON              phth.pacost_stg = expandedcosttable.id_stg
                                                                                  left join       db_t_prod_stag.pctl_chargepattern
                                                                                  ON              expandedcosttable.chargepattern_stg = pctl_chargepattern.id_stg
                                                                                                  /* left join DB_T_PROD_STAG.pctl_pacost on ExpandedCostTable.Subtype = pctl_pacost.ID */
                                                                                                  /* left join DB_T_PROD_STAG.pctl_periltype_alfa AutoPerilType on ExpandedCostTable.PerilType_alfa = AutoPerilType.ID  */
                                                                                  join            db_t_prod_stag.pc_policyperiod
                                                                                  ON              phth.branchid_stg = pc_policyperiod.id_stg
                                                                                  AND             expandedcosttable.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                                  AND             phth.branchid_stg=pc_policyperiod.id_stg
                                                                                                  /* join DB_T_PROD_STAG.pctl_policyperiodstatus on pc_policyperiod.status=pctl_policyperiodstatus.ID */
                                                                                  join            db_t_prod_stag.pc_job
                                                                                  ON              pc_policyperiod.jobid_stg = pc_job.id_stg
                                                                                                  /* and pc_policyperiod.policynumber_stg =''SP010347'' */
                                                                                  join            db_t_prod_stag.pctl_job
                                                                                  ON              pc_job.subtype_stg = pctl_job.id_stg
                                                                                  join            db_t_prod_stag.pc_policyline
                                                                                  ON              pc_policyperiod.id_stg = pc_policyline.branchid_stg
                                                                                  AND             pc_policyline.expirationdate_stg IS NULL
                                                                                  join            db_t_prod_stag.pctl_papolicytype_alfa
                                                                                  ON              pc_policyline.papolicytype_alfa_stg = pctl_papolicytype_alfa.id_stg
                                                                                  AND             pctl_papolicytype_alfa.typecode_stg = ''WATERCRAFT''
                                                                                  join            db_t_prod_stag.pc_uwcompany uw
                                                                                  ON              uw.id_stg = pc_policyperiod.uwcompany_stg
                                                                                  join            db_t_prod_stag.pctl_jurisdiction st
                                                                                  ON              st.id_stg = pc_policyperiod.basestate_stg
                                                                                  join            db_t_prod_stag.pc_policyterm pt
                                                                                  ON              pt.id_stg = pc_policyperiod.policytermid_stg
                                                                                                  /* JOIN DB_T_PROD_STAG.pc_policyperiod pp  ON pc_patransaction.BRANCHID =PP.ID  */
                                                                                  left join       db_t_prod_stag.pc_personalvehicle pv
                                                                                  ON              pv.id_stg = personalvehiclecov_stg
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT motortype_alfa_stg,
                                                                                                                branchid_stg
                                                                                                         FROM   db_t_prod_stag.pc_personalvehicle
                                                                                                         WHERE  motortype_alfa_stg IS NOT NULL) pvt
                                                                                  ON              pvt.branchid_stg=pc_policyperiod.id_stg
                                                                                  left join       db_t_prod_stag.pctl_motortype_alfa mt
                                                                                  ON              mt.id_stg = pvt.motortype_alfa_stg
                                                                                  join            db_t_prod_stag.pctl_policyperiodstatus
                                                                                  ON              pc_policyperiod.status_stg=pctl_policyperiodstatus.id_stg
                                                                                  AND             pctl_policyperiodstatus.typecode_stg=''Bound''
                                                                                  AND             date_filter BETWEEN cast(:PC_BOY AS timestamp) AND             cast(:PC_EOY AS timestamp)
                                                                                  WHERE           pctl_chargepattern.name_stg = ''Premium''
                                                                                  AND             (
                                                                                                                  pctl_papolicytype_alfa.typecode_stg = ''WATERCRAFT'' )
                                                                                  AND             NOT EXISTS
                                                                                                  (
                                                                                                         SELECT pc_policyperiod2.policynumber_stg
                                                                                                         FROM   db_t_prod_stag.pc_policyperiod pc_policyperiod2
                                                                                                         join   db_t_prod_stag.pc_policyterm pt2
                                                                                                         ON     pt2.id_stg = pc_policyperiod2.policytermid_stg
                                                                                                         join   db_t_prod_stag.pc_policyline
                                                                                                         ON     pc_policyperiod2.id_stg = pc_policyline.branchid_stg
                                                                                                         AND    pc_policyline.expirationdate_stg IS NULL
                                                                                                         join   db_t_prod_stag.pc_job job2
                                                                                                         ON     job2.id_stg = pc_policyperiod2.jobid_stg
                                                                                                         join   db_t_prod_stag.pctl_job pctl_job2
                                                                                                         ON     pctl_job2.id_stg = job2.subtype_stg
                                                                                                         WHERE  pctl_job2.name_stg = ''Renewal''
                                                                                                         AND    (
                                                                                                                       pt.confirmationdate_alfa_stg > :PC_EOY
                                                                                                                OR     pt.confirmationdate_alfa_stg IS NULL)
                                                                                                         AND    pc_policyperiod2.policynumber_stg = pc_policyperiod.policynumber_stg
                                                                                                         AND    pc_policyperiod2.termnumber_stg = pc_policyperiod.termnumber_stg )
                                                                                           /* AND 1=2 */
                                                                                           UNION
                                                                                           SELECT DISTINCT
                                                                                                           CASE
                                                                                                                           WHEN uw.publicid_stg=''AMI'' THEN ''0005''
                                                                                                                           WHEN uw.publicid_stg=''AMG'' THEN ''0196''
                                                                                                                           WHEN uw.publicid_stg=''AIC'' THEN ''0050''
                                                                                                                           WHEN uw.publicid_stg=''AGI'' THEN ''0318''
                                                                                                           END  AS companynumber,
                                                                                                           ''06'' AS lob,
                                                                                                           CASE
                                                                                                                           WHEN st.typecode_stg=''AL'' THEN ''01''
                                                                                                                           WHEN st.typecode_stg=''GA'' THEN ''10''
                                                                                                                           WHEN st.typecode_stg=''MS'' THEN ''23''
                                                                                                           END                                             AS statecode,
                                                                                                           extract(year FROM cast(:PC_EOY AS timestamp))+1 AS callyear,
                                                                                                           extract(year FROM cast(:PC_EOY AS timestamp))   AS accountingyear,
                                                                                                           ''0000''                                             expperiodyear,
                                                                                                           ''00''                                               expperiodmonth,
                                                                                                           ''00''                                               expperiodday,
                                                                                                           ''725000''                                        AS classificationcode,
                                                                                                           ''00''                                            AS typeoflosscode,
                                                                                                           CASE
                                                                                                                           WHEN pctl_job.typecode_stg=''Cancellation'' THEN year(pc_policyperiod.cancellationdate_stg)
                                                                                                                           ELSE year(pc_policyperiod.periodstart_stg)
                                                                                                           END   AS policy_eff_yr,
                                                                                                           ''090'' AS aslob,
                                                                                                           ''010'' AS policyidcode,
                                                                                                           abs(round(
                                                                                                           CASE
                                                                                                                           WHEN pc_policyperiod.cancellationdate_stg IS NOT NULL THEN months_between(cast(pc_policyperiod.cancellationdate_stg AS DATE),cast( pc_policyperiod.editeffectivedate_stg AS DATE))
                                                                                                                           ELSE months_between(cast(pc_policyperiod.periodend_stg AS                                                              DATE),cast( pc_policyperiod.editeffectivedate_stg AS DATE))
                                                                                                           END,0)) AS policyterm,
                                                                                                           ''0''     AS claimidentifier,
                                                                                                           ''000''   AS claimantidentifier,
                                                                                                           ''0''     AS paidlosses,
                                                                                                           ''0''     AS paidnoofclaims,
                                                                                                           ''0''     AS outstandinglosses,
                                                                                                           ''0''     AS outstandingclaims,
                                                                                                           pc_policyperiod.policynumber_stg,
                                                                                                           pc_policyperiod.publicid_stg AS policyperiodid,
                                                                                                           pc_job.jobnumber_stg         AS policyidentifier,
                                                                                                           ''0''                          AS vin,
                                                                                                           ''00''                         AS exposurenumber,
                                                                                                           phth.amount_stg              AS premium ,
                                                                                                           CASE
                                                                                                                           WHEN pc_policyperiod.editeffectivedate_stg >= pc_policyperiod.modeldate_stg
                                                                                                                           AND             pc_policyperiod.editeffectivedate_stg>= coalesce(pt.confirmationdate_alfa_stg, cast(''1900-01-01 00:00:00.000000'' AS timestamp)) THEN cast(pc_policyperiod.editeffectivedate_stg AS timestamp)
                                                                                                                           WHEN coalesce(pt.confirmationdate_alfa_stg, cast(''1900-01-01 00:00:00.000000'' AS timestamp)) >= pc_policyperiod.modeldate_stg THEN cast(coalesce(pt.confirmationdate_alfa_stg, cast(''1900-01-01 00:00:00.000000'' AS timestamp)) AS timestamp)
                                                                                                                           ELSE cast( pc_policyperiod.modeldate_stg AS timestamp)
                                                                                                           END date_filter,
                                                                                                           phth.id_stg
                                                                                           FROM            db_t_prod_stag.pcx_hotransaction_hoe phth
                                                                                           join
                                                                                                           (
                                                                                                                  SELECT pc_policyperiod.policynumber_stg,
                                                                                                                         pcx_homeownerscost_hoe.id_stg,
                                                                                                                         pcx_homeownerscost_hoe.chargepattern_stg
                                                                                                                  FROM   db_t_prod_stag.pcx_homeownerscost_hoe
                                                                                                                  join   db_t_prod_stag.pc_policyperiod
                                                                                                                  ON     pcx_homeownerscost_hoe.branchid_stg=pc_policyperiod.id_stg ) expandedhocosttable
                                                                                           ON              phth.homeownerscost_stg = expandedhocosttable.id_stg
                                                                                           left join       db_t_prod_stag.pctl_chargepattern
                                                                                           ON              expandedhocosttable.chargepattern_stg = pctl_chargepattern.id_stg
                                                                                                           /* left join DB_T_PROD_STAG.pctl_homeownerscost_hoe on ExpandedHOCostTable.Subtype = pctl_homeownerscost_hoe.ID */
                                                                                                           /* left join DB_T_PROD_STAG.pctl_periltype_alfa HOPerilType on ExpandedHOCostTable.PerilType_alfa = HOPerilType.ID */
                                                                                           join            db_t_prod_stag.pc_policyperiod
                                                                                           ON              phth.branchid_stg = pc_policyperiod.id_stg
                                                                                           AND             expandedhocosttable.policynumber_stg=pc_policyperiod.policynumber_stg
                                                                                           join            db_t_prod_stag.pc_job
                                                                                           ON              pc_policyperiod.jobid_stg = pc_job.id_stg
                                                                                           left join       db_t_prod_stag.pctl_job
                                                                                           ON              pc_job.subtype_stg = pctl_job.id_stg
                                                                                           left join       db_t_prod_stag.pc_policyline
                                                                                           ON              pc_policyperiod.id_stg = pc_policyline.branchid_stg
                                                                                           AND             pc_policyline.expirationdate_stg IS NULL
                                                                                           left join       db_t_prod_stag.pc_policy
                                                                                           ON              pc_policyperiod.policyid_stg = pc_policy.id_stg
                                                                                                           /* left join DB_T_PROD_STAG.pc_account on pc_policy.AccountID = pc_account.ID */
                                                                                                           /* left join DB_T_PROD_STAG.pctl_sectiontype_alfa on ExpandedHOCostTable.SectionType_alfa=pctl_sectiontype_alfa.ID */
                                                                                                           /* join DB_T_PROD_STAG.pctl_policyperiodstatus on pc_policyperiod.Status=pctl_policyperiodstatus.ID */
                                                                                           join            db_t_prod_stag.pc_uwcompany uw
                                                                                           ON              uw.id_stg = pc_policyperiod.uwcompany_stg
                                                                                           join            db_t_prod_stag.pctl_jurisdiction st
                                                                                           ON              st.id_stg = pc_policyperiod.basestate_stg
                                                                                           join            db_t_prod_stag.pc_policyterm pt
                                                                                           ON              pt.id_stg = pc_policyperiod.policytermid_stg
                                                                                           join            db_t_prod_stag.pctl_hopolicytype_hoe ph
                                                                                           ON              ph.id_stg =pc_policyline.hopolicytype_stg
                                                                                                           /* JOIN DB_T_PROD_STAG.pc_policyperiod pp  ON pc_patransaction.BRANCHID =PP.ID  */
                                                                                           WHERE           pctl_chargepattern.name_stg = ''Premium''
                                                                                           AND             ph.typecode_stg = ''PAF''
                                                                                           AND             NOT EXISTS
                                                                                                           (
                                                                                                                  SELECT pc_policyperiod2.policynumber_stg
                                                                                                                  FROM   db_t_prod_stag.pc_policyperiod pc_policyperiod2
                                                                                                                  join   db_t_prod_stag.pc_policyterm pt2
                                                                                                                  ON     pt2.id_stg = pc_policyperiod2.policytermid_stg
                                                                                                                  join   db_t_prod_stag.pc_policyline
                                                                                                                  ON     pc_policyperiod2.id_stg = pc_policyline.branchid_stg
                                                                                                                  AND    pc_policyline.expirationdate_stg IS NULL
                                                                                                                  join   db_t_prod_stag.pc_job job2
                                                                                                                  ON     job2.id_stg = pc_policyperiod2.jobid_stg
                                                                                                                  join   db_t_prod_stag.pctl_job pctl_job2
                                                                                                                  ON     pctl_job2.id_stg = job2.subtype_stg
                                                                                                                  WHERE  pctl_job2.name_stg = ''Renewal''
                                                                                                                  AND    (
                                                                                                                                pt.confirmationdate_alfa_stg > :PC_EOY
                                                                                                                         OR     pt.confirmationdate_alfa_stg IS NULL)
                                                                                                                  AND    pc_policyperiod2.policynumber_stg = pc_policyperiod.policynumber_stg
                                                                                                                  AND    pc_policyperiod2.termnumber_stg = pc_policyperiod.termnumber_stg )
                                                                                           AND             date_filter BETWEEN cast(:PC_BOY AS timestamp) AND             cast(:PC_EOY AS timestamp) ) a
                                                         WHERE    premium <> 0
                                                         AND      premium IS NOT NULL
                                                         GROUP BY companynumber,
                                                                  lob,
                                                                  statecode,
                                                                  callyear,
                                                                  accountingyear,
                                                                  expperiodyear,
                                                                  expperiodmonth,
                                                                  expperiodday,
                                                                  classificationcode,
                                                                  typeoflosscode,
                                                                  policy_eff_yr,
                                                                  aslob,
                                                                  policyidcode,
                                                                  policyterm,
                                                                  claimidentifier,
                                                                  claimantidentifier,
                                                                  policynumber_stg,
                                                                  policyperiodid,
                                                                  paidlosses,
                                                                  paidnoofclaims,
                                                                  outstandinglosses,
                                                                  outstandingclaims,
                                                                  policyperiodid,
                                                                  policyidentifier,
                                                                  vin,
                                                                  exposurenumber)a ) src ) );
  -- Component exp_policy_pass_through, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_policy_pass_through AS
  (
         SELECT sq_pc_policyperiod.companynumber            AS companynumber,
                sq_pc_policyperiod.lineofbusinesscode       AS lineofbusinesscode,
                sq_pc_policyperiod.statecode                AS statecode,
                sq_pc_policyperiod.callyear                 AS callyear,
                sq_pc_policyperiod.accountingyear           AS accountingyear,
                sq_pc_policyperiod.expperiodyear            AS expperiodyear,
                sq_pc_policyperiod.expperiodmonth           AS expperiodmonth,
                sq_pc_policyperiod.expeperiodday            AS expeperiodday,
                sq_pc_policyperiod.classificationcode       AS classsificationcode,
                sq_pc_policyperiod.typeoflosscode           AS typeoflosscode,
                sq_pc_policyperiod.policyeffectiveyear      AS policyeffectiveyear,
                sq_pc_policyperiod.aslob                    AS aslob,
                sq_pc_policyperiod.policyidentificationcode AS policyidentificationcode,
                sq_pc_policyperiod.policytermcode           AS policytermcode,
                sq_pc_policyperiod.claimidentifier          AS claimidentifier,
                sq_pc_policyperiod.claimantidentifier       AS claimantidentifier,
                sq_pc_policyperiod.writtenpremium           AS writtenpremium,
                sq_pc_policyperiod.paidlosses               AS paidlosses,
                sq_pc_policyperiod.paidnumberofclaims       AS paidnumberofclaims,
                sq_pc_policyperiod.outstandinglosses        AS outstandinglosses,
                sq_pc_policyperiod.outstandingnoofclaims    AS outstandingnoofclaims,
                sq_pc_policyperiod.policynumber             AS policynumber,
                sq_pc_policyperiod.policyperiodid           AS policyperiodid,
                sq_pc_policyperiod.policyidentifier         AS policyidentifier,
                sq_pc_policyperiod.vin                      AS vin,
                sq_pc_policyperiod.exposurenumber           AS exposurenumber,
                sq_pc_policyperiod.source_record_id
         FROM   sq_pc_policyperiod );
  -- Component exp_default, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_default AS
  (
         SELECT
                CASE
                       WHEN exp_policy_pass_through.companynumber IS NULL THEN ''0000''
                       ELSE lpad ( exp_policy_pass_through.companynumber , 4 , ''0'' )
                END                                        AS o_companynumber,
                exp_policy_pass_through.lineofbusinesscode AS lineofbusinesscode,
                CASE
                       WHEN exp_policy_pass_through.statecode IS NULL THEN ''00''
                       ELSE exp_policy_pass_through.statecode
                END                                         AS o_statecode,
                exp_policy_pass_through.callyear            AS callyear,
                exp_policy_pass_through.accountingyear      AS accountingyear,
                exp_policy_pass_through.expperiodyear       AS expperiodyear,
                exp_policy_pass_through.expperiodmonth      AS expperiodmonth,
                exp_policy_pass_through.expeperiodday       AS expeperiodday,
                exp_policy_pass_through.classsificationcode AS classificationcode,
                CASE
                       WHEN exp_policy_pass_through.typeoflosscode IS NULL THEN ''00''
                       ELSE lpad ( exp_policy_pass_through.typeoflosscode , 2 , ''0'' )
                END                                              AS o_typeoflosscode,
                exp_policy_pass_through.policyeffectiveyear      AS policyeffectiveyear,
                exp_policy_pass_through.aslob                    AS aslob,
                exp_policy_pass_through.policyidentificationcode AS policyidentificationcode,
                CASE
                       WHEN exp_policy_pass_through.policytermcode IS NULL THEN ''00''
                       ELSE lpad ( exp_policy_pass_through.policytermcode , 2 , ''0'' )
                END AS o_policytermcode,
                CASE
                       WHEN exp_policy_pass_through.claimidentifier IS NULL THEN ''000000000000000''
                       ELSE lpad ( exp_policy_pass_through.claimidentifier , 15 , ''0'' )
                END AS o_claimidentifier,
                CASE
                       WHEN exp_policy_pass_through.claimantidentifier IS NULL THEN ''000''
                       ELSE lpad ( exp_policy_pass_through.claimantidentifier , 3 , ''0'' )
                END AS o_claimantidentifier,
                CASE
                       WHEN (
                                     exp_policy_pass_through.writtenpremium IS NULL
                              OR     (
                                            exp_policy_pass_through.writtenpremium = ''0.00'' ) ) THEN ''000000000000''
                       ELSE exp_policy_pass_through.writtenpremium
                END AS o_writtenpremium,
                CASE
                       WHEN (
                                     exp_policy_pass_through.paidlosses IS NULL
                              OR     exp_policy_pass_through.paidlosses = ''0'' ) THEN ''000000000000''
                       ELSE lpad ( exp_policy_pass_through.paidlosses , 12 , ''0'' )
                END AS o_paidlosses,
                CASE
                       WHEN (
                                     exp_policy_pass_through.paidnumberofclaims IS NULL
                              OR     exp_policy_pass_through.paidnumberofclaims = ''0'' ) THEN ''000000000000''
                       ELSE lpad ( exp_policy_pass_through.paidnumberofclaims , 12 , ''0'' )
                END AS o_paidnumberofclaims,
                CASE
                       WHEN (
                                     exp_policy_pass_through.outstandinglosses IS NULL
                              OR     exp_policy_pass_through.outstandinglosses = ''0'' ) THEN ''000000000000''
                       ELSE lpad ( exp_policy_pass_through.outstandinglosses , 12 , ''0'' )
                END AS o_outstandinglosses,
                CASE
                       WHEN (
                                     exp_policy_pass_through.outstandingnoofclaims IS NULL
                              OR     exp_policy_pass_through.outstandingnoofclaims = ''0'' ) THEN ''000000000000''
                       ELSE lpad ( exp_policy_pass_through.outstandingnoofclaims , 12 , ''0'' )
                END                                  AS o_outstandingnoofclaims,
                exp_policy_pass_through.policynumber AS policynumber,
                CASE
                       WHEN exp_policy_pass_through.policyperiodid IS NULL THEN ''''
                       ELSE rpad ( exp_policy_pass_through.policyperiodid , 20 , ''0'' )
                END AS o_policyperiodid,
                CASE
                       WHEN exp_policy_pass_through.policyidentifier IS NULL THEN ''00000000000000000000''
                       ELSE lpad ( exp_policy_pass_through.policyidentifier , 20 , ''0'' )
                END                                    AS o_policyidentifier,
                current_timestamp                      AS creationts,
                current_timestamp                      AS updatets,
                exp_policy_pass_through.vin            AS vin,
                exp_policy_pass_through.exposurenumber AS exposurenumber,
                :PRCS_ID                               AS prcs_id,
                exp_policy_pass_through.source_record_id
         FROM   exp_policy_pass_through );
  -- Component OUT_NAIIPCI_IM_PC, Type TARGET
  INSERT INTO db_t_prod_comn.out_naiipci_im
              (
                          companynumber,
                          lineofbusinesscode,
                          statecode,
                          callyear,
                          accountingyear,
                          expperiodyear,
                          expperiodmonth,
                          expperiodday,
                          classificationcode,
                          typeoflosscode,
                          policyeffectiveyear,
                          aslob,
                          policyidentificationcode,
                          policyterm,
                          claimidentifier,
                          claimantidentifier,
                          writtenpremium,
                          paidlosses,
                          paidnumberofclaims,
                          outstandinglosses,
                          outstandingnoofclaims,
                          policynumber,
                          policyperiodid,
                          creationts,
                          updatets,
                          policyidentifier,
                          vin,
                          exposurenumber,
                          prcs_id
              )
  SELECT exp_default.o_companynumber          AS companynumber,
         exp_default.lineofbusinesscode       AS lineofbusinesscode,
         exp_default.o_statecode              AS statecode,
         exp_default.callyear                 AS callyear,
         exp_default.accountingyear           AS accountingyear,
         exp_default.expperiodyear            AS expperiodyear,
         exp_default.expperiodmonth           AS expperiodmonth,
         exp_default.expeperiodday            AS expperiodday,
         exp_default.classificationcode       AS classificationcode,
         exp_default.o_typeoflosscode         AS typeoflosscode,
         exp_default.policyeffectiveyear      AS policyeffectiveyear,
         exp_default.aslob                    AS aslob,
         exp_default.policyidentificationcode AS policyidentificationcode,
         exp_default.o_policytermcode         AS policyterm,
         exp_default.o_claimidentifier        AS claimidentifier,
         exp_default.o_claimantidentifier     AS claimantidentifier,
         exp_default.o_writtenpremium         AS writtenpremium,
         exp_default.o_paidlosses             AS paidlosses,
         exp_default.o_paidnumberofclaims     AS paidnumberofclaims,
         exp_default.o_outstandinglosses      AS outstandinglosses,
         exp_default.o_outstandingnoofclaims  AS outstandingnoofclaims,
         exp_default.policynumber             AS policynumber,
         exp_default.o_policyperiodid         AS policyperiodid,
         exp_default.creationts               AS creationts,
         exp_default.updatets                 AS updatets,
         exp_default.o_policyidentifier       AS policyidentifier,
         exp_default.vin                      AS vin,
         exp_default.exposurenumber           AS exposurenumber,
         exp_default.prcs_id                  AS prcs_id
  FROM   exp_default;
  
  -- PIPELINE END FOR 1
  -- PIPELINE START FOR 2
  -- Component SQ_cc_policyperiod, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_cc_policyperiod AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS companynumber,
                $2  AS lineofbusinesscode,
                $3  AS statecode,
                $4  AS callyear,
                $5  AS accountingyear,
                $6  AS expperiodyear,
                $7  AS expperiodmonth,
                $8  AS expeperiodday,
                $9  AS classificationcode,
                $10 AS typeoflosscode,
                $11 AS policyeffectiveyear,
                $12 AS aslob,
                $13 AS policyidentificationcode,
                $14 AS policytermcode,
                $15 AS claimidentifier,
                $16 AS claimantidentifier,
                $17 AS writtenpremium,
                $18 AS paidlosses,
                $19 AS paidnumberofclaims,
                $20 AS outstandinglosses,
                $21 AS outstandingnoofclaims,
                $22 AS policynumber,
                $23 AS policyperiodid,
                $24 AS policyidentifier,
                $25 AS vin,
                $26 AS exposurenumber,
                $27 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT companynumber,
                                                lob,
                                                statecode,
                                                callyear,
                                                accountingyear,
                                                exp_yr,
                                                exp_mth,
                                                exp_day,
                                                policyidcode_stg,
                                                typeoflosscode,
                                                policy_eff_yr,
                                                aslob,
                                                policyidentificationcode,
                                                policyterm,
                                                claim_identifier,
                                                claimant_identifier,
                                                wrtprem,
                                                paidloss,
                                                CASE
                                                       WHEN(
                                                                     closedate > cast(:CC_BOY AS timestamp)
                                                              AND    closedate < cast(:CC_EOY AS timestamp)
                                                              AND    paidloss > 0
                                                              AND    covrank >= 1) THEN 1
                                                       ELSE 0
                                                END AS paidclaims,
                                                outloss,
                                                CASE
                                                       WHEN(
                                                                     closedate IS NULL
                                                              OR     closedate > cast(:CC_EOY AS timestamp) )
                                                       AND    covrank >= 1
                                                       AND    outloss>0 THEN 1
                                                       ELSE 0
                                                END AS outstandingclaims,
                                                ''0'' AS policynumber,
                                                ''0'' AS policyperiodid,
                                                ''0'' AS policyidentifier,
                                                ''0'' AS vin,
                                                exposurenumber
                                         FROM   (
                                                         SELECT   companynumber,
                                                                  lob,
                                                                  statecode,
                                                                  callyear,
                                                                  accountingyear,
                                                                  exp_yr,
                                                                  exp_mth,
                                                                  exp_day,
                                                                  policyidcode_stg,
                                                                  typeoflosscode,
                                                                  policy_eff_yr,
                                                                  aslob,
                                                                  policyidentificationcode,
                                                                  policyterm,
                                                                  claim_identifier,
                                                                  claimant_identifier,
                                                                  wrtprem,
                                                                  lossdate_stg,
                                                                  closedate,
                                                                  covrank,
                                                                  SUM(outres)                                                              AS outloss,
                                                                  SUM(acct500104 + acct500214 + acct500314) - SUM(acct500204 + acct500304) AS paidloss,
                                                                  exposurenumber
                                                         FROM     (
                                                                                  SELECT DISTINCT
                                                                                                  CASE
                                                                                                                  WHEN a.uwco_stg=''AMI'' THEN ''0005''
                                                                                                                  WHEN a.uwco_stg=''AMG'' THEN ''0196''
                                                                                                                  WHEN a.uwco_stg=''AIC'' THEN ''0050''
                                                                                                                  WHEN a.uwco_stg=''AGI'' THEN ''0318''
                                                                                                  END  AS companynumber,
                                                                                                  ''06'' AS lob,
                                                                                                  CASE
                                                                                                                  WHEN a.state_stg=''AL'' THEN ''01''
                                                                                                                  WHEN a.state_stg=''GA'' THEN ''10''
                                                                                                                  WHEN a.state_stg=''MS'' THEN ''23''
                                                                                                  END AS statecode,
                                                                                                  /*extract(year from A.lossdate_stg) + 1  AS CALLYEAR,
extract(year from A.lossdate_stg)  AS ACCOUNTINGYEAR,*/
                                                                                                  extract(year FROM cast(:CC_EOY AS timestamp )) + 1 AS callyear,
                                                                                                  extract(year FROM cast(:CC_EOY AS timestamp))      AS accountingyear,
                                                                                                  extract(year FROM a.lossdate_stg)                  AS exp_yr,
                                                                                                  CASE
                                                                                                                  WHEN extract(month FROM a.lossdate_stg) <10 THEN cast(''0''
                                                                                                                                                  ||cast(extract(month FROM a.lossdate_stg) AS VARCHAR(2))AS VARCHAR(2))
                                                                                                                  ELSE cast(extract(month FROM a.lossdate_stg) AS VARCHAR(2))
                                                                                                  END AS exp_mth,
                                                                                                  CASE
                                                                                                                  WHEN extract(day FROM a.lossdate_stg) <10 THEN cast(''0''
                                                                                                                                                  ||cast(extract(day FROM a.lossdate_stg) AS VARCHAR(2))AS VARCHAR(2))
                                                                                                                  ELSE cast(extract(day FROM a.lossdate_stg) AS VARCHAR(2))
                                                                                                  END AS exp_day,
                                                                                                  a.policyidcode_stg,
                                                                                                  CASE
                                                                                                                  WHEN losscause_stg IN (''Fire - Total/Other'',
                                                                                                                                         ''Fire - Total / Other'',
                                                                                                                                         ''Fire - Partial / Other'',
                                                                                                                                         ''Fire'',
                                                                                                                                         ''Total Fire'',
                                                                                                                                         ''Lightning'' ,
                                                                                                                                         ''Fire - Partial/Lightning'',
                                                                                                                                         ''Fire - Total/Lightning'') THEN ''01''
                                                                                                                  WHEN losscause_stg IN (''Wind'',
                                                                                                                                         ''Hail'',
                                                                                                                                         ''Fire'',
                                                                                                                                         ''Wind, Quake, Hail, Explosion, Tornado, Water Damage'' ) THEN ''02''
                                                                                                                  WHEN losscause_stg IN (''EC'',
                                                                                                                                         ''Vandalism/Malicious Mischief'',
                                                                                                                                         ''Other Perils'' ) THEN ''03''
                                                                                                                  WHEN losscause_stg IN (''Theft (Auto or WTC)'',
                                                                                                                                         ''Theft'') THEN ''05''
                                                                                                                  WHEN losscause_stg IN (''Earthquake'') THEN ''07''
                                                                                                                  WHEN losscause_stg IN (''Water/Other'',
                                                                                                                                         ''Water/Frozen Pipes'') THEN ''08''
                                                                                                                  WHEN losscause_stg IN (''Out of State'',
                                                                                                                                         ''Disputed Liability'',
                                                                                                                                         ''Watercraft Unsafe Condition'',
                                                                                                                                         ''At Fault Accident'',
                                                                                                                                         ''Not at Fault Accident'' ,
                                                                                                                                         ''Flood'',
                                                                                                                                         ''ERA'') THEN ''09''
                                                                                                                  ELSE ''09''
                                                                                                  END                               AS typeoflosscode,
                                                                                                  extract(year FROM a.policy_eff_yr)   policy_eff_yr,
                                                                                                  ''090''                             AS aslob,
                                                                                                  ''010''                             AS policyidentificationcode,
                                                                                                  ''00''                              AS policyterm,
                                                                                                  a.claimnumber_stg                 AS claim_identifier,
                                                                                                  a.claimant_identifier_stg         AS claimant_identifier,
                                                                                                  ''00''                              AS wrtprem,
                                                                                                  a.lossdate_stg,
                                                                                                  a.closedate,
                                                                                                  a.covrank,
                                                                                                  a.exposurenumber,
                                                                                                  SUM(a.outstanding) AS outres,
                                                                                                  SUM(a.acct500104)  AS acct500104,
                                                                                                  SUM(a.acct500204)  AS acct500204,
                                                                                                  SUM(a.acct500214)  AS acct500214,
                                                                                                  SUM(a.acct500304)  AS acct500304,
                                                                                                  SUM(a.acct500314)  AS acct500314
                                                                                  FROM            (
                                                                                                                  SELECT DISTINCT tx.id_stg             AS txid,
                                                                                                                                  claim.claimnumber_stg AS claimnumber_stg,
                                                                                                                                  txli.id_stg,
                                                                                                                                  tl4.name_stg,
                                                                                                                                  tl6.typecode_stg                                                                      AS uwco_stg,
                                                                                                                                  tl5.typecode_stg                                                                      AS state_stg,
                                                                                                                                  pttl.name_stg                                                                         AS policytype_stg,
                                                                                                                                  psttl.typecode_stg                                                                    AS policysubtype_stg,
                                                                                                                                  claim.lossdate_stg                                                                    AS lossdate_stg,
                                                                                                                                  txtl.name_stg                                                                         AS txname_stg,
                                                                                                                                  lctl.name_stg                                                                         AS lcname_stg,
                                                                                                                                  pol.policysystemperiodid_stg                                                          AS policyidcode_stg,
                                                                                                                                  lc.name_stg                                                                           AS losscause_stg,
                                                                                                                                  pol.effectivedate_stg                                                                 AS policy_eff_yr,
                                                                                                                                  exp1.claimantdenormid_stg                                                             AS claimant_identifier_stg,
                                                                                                                                  rank() over(PARTITION BY claim.claimnumber_stg ORDER BY exp1.coveragesubtype_stg ASC) AS covrank,
                                                                                                                                  claim.closedate_stg                                                                   AS closedate,
                                                                                                                                  exp1.claimorder_stg                                                                   AS exposurenumber,
                                                                                                                                  /* Added as part of new task EIM_46414 */
                                                                                                                                  CASE
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Payment''
                                                                                                                                                                  AND             rctl.name_stg IS NULL
                                                                                                                                                                  AND             cctl.name_stg=''Loss''
                                                                                                                                                                  AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                  AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                  AND             lctl.name_stg = ''Diminished Value''
                                                                                                                                                                  AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                  AND             (
                                                                                                                                                                                                  (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp))
                                                                                                                                                                                  OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                  AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Payment''
                                                                                                                                                                  AND             rctl.name_stg IS NULL
                                                                                                                                                                  AND             cctl.name_stg=''Loss''
                                                                                                                                                                  AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                  AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                  AND             lctl.name_stg = ''Deductible''
                                                                                                                                                                  AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                  AND             (
                                                                                                                                                                                                  (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp))
                                                                                                                                                                                  OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                  AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Payment''
                                                                                                                                                                  AND             rctl.name_stg IS NULL
                                                                                                                                                                  AND             cctl.name_stg=''Loss''
                                                                                                                                                                  AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                  AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                  AND             lctl.name_stg = ''Deductible Refund''
                                                                                                                                                                  AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                  AND             (
                                                                                                                                                                                                  (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp))
                                                                                                                                                                                  OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                  AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Payment''
                                                                                                                                                                  AND             rctl.name_stg IS NULL
                                                                                                                                                                  AND             cctl.name_stg=''Loss''
                                                                                                                                                                  AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                  AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                  AND             lctl.name_stg = ''Former Deductible''
                                                                                                                                                                  AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                  AND             (
                                                                                                                                                                                                  (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp))
                                                                                                                                                                                  OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                  AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Payment''
                                                                                                                                                                  AND             rctl.name_stg IS NULL
                                                                                                                                                                  AND             cctl.name_stg=''Loss''
                                                                                                                                                                  AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                  AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                  AND             lctl.name_stg = ''Loss''
                                                                                                                                                                  AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                  AND             (
                                                                                                                                                                                                  (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp))
                                                                                                                                                                                  OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                  AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Recovery''
                                                                                                                                                                  AND             rctl.name_stg = ''Credit to loss''
                                                                                                                                                                  AND             cctl.name_stg=''Loss''
                                                                                                                                                                  AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                  AND             lctl.name_stg = ''Deductible''
                                                                                                                                                                  AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Recovery''
                                                                                                                                                                  AND             rctl.name_stg = ''Credit to loss''
                                                                                                                                                                  AND             cctl.name_stg=''Loss''
                                                                                                                                                                  AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                  AND             lctl.name_stg = ''Loss''
                                                                                                                                                                  AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Recovery''
                                                                                                                                                                  AND             rctl.name_stg = ''Credit to loss''
                                                                                                                                                                  AND             cctl.name_stg=''Loss''
                                                                                                                                                                  AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                  AND             lctl.name_stg = ''Recovery''
                                                                                                                                                                  AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                  ELSE 0
                                                                                                                                  END AS acct500104,
                                                                                                                                  CASE
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Recovery''
                                                                                                                                                                  AND             rctl.name_stg = ''Salvage''
                                                                                                                                                                  AND             cctl.name_stg=''Loss''
                                                                                                                                                                  AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                  AND             lctl.name_stg = ''Recovery''
                                                                                                                                                                  AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg
                                                                                                                                                  ELSE 0
                                                                                                                                  END AS acct500204,
                                                                                                                                  CASE
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Payment''
                                                                                                                                                                  AND             rctl.name_stg IS NULL
                                                                                                                                                                  AND             cctl.name_stg=''Expense''
                                                                                                                                                                  AND             cttl.name_stg = ''Expense''
                                                                                                                                                                  AND             lctl.name_stg = ''Salvage Expense''
                                                                                                                                                                  AND             (
                                                                                                                                                                                                  (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp))
                                                                                                                                                                                  OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                  AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Recovery''
                                                                                                                                                                  AND             rctl.name_stg = ''Credit to expense''
                                                                                                                                                                  AND             cctl.name_stg=''Expense''
                                                                                                                                                                  AND             cttl.name_stg = ''Expense''
                                                                                                                                                                  AND             lctl.name_stg = ''Salvage Expense''
                                                                                                                                                                  AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Recovery''
                                                                                                                                                                  AND             rctl.name_stg = ''Salvage''
                                                                                                                                                                  AND             cctl.name_stg=''Expense''
                                                                                                                                                                  AND             cttl.name_stg = ''Expense''
                                                                                                                                                                  AND             lctl.name_stg = ''Salvage Expense''
                                                                                                                                                                  AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Recovery''
                                                                                                                                                                  AND             rctl.name_stg = ''Salvage''
                                                                                                                                                                  AND             cctl.name_stg=''Expense''
                                                                                                                                                                  AND             cttl.name_stg = ''Expense''
                                                                                                                                                                  AND             lctl.name_stg = ''Recovery''
                                                                                                                                                                  AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                  ELSE 0
                                                                                                                                  END AS acct500214,
                                                                                                                                  tx.comments_stg,
                                                                                                                                  CASE
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Recovery''
                                                                                                                                                                  AND             rctl.name_stg = ''Subrogation''
                                                                                                                                                                  AND             cctl.name_stg=''Loss''
                                                                                                                                                                  AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                  AND             lctl.name_stg = ''Recovery''
                                                                                                                                                                  AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg
                                                                                                                                                  ELSE 0
                                                                                                                                  END AS acct500304,
                                                                                                                                  CASE
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Payment''
                                                                                                                                                                  AND             rctl.name_stg IS NULL
                                                                                                                                                                  AND             cctl.name_stg=''Expense''
                                                                                                                                                                  AND             cttl.name_stg = ''Expense''
                                                                                                                                                                  AND             lctl.name_stg = ''Subrogation Expense''
                                                                                                                                                                  AND             (
                                                                                                                                                                                                  (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp))
                                                                                                                                                                                  OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                  AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Recovery''
                                                                                                                                                                  AND             rctl.name_stg = ''Credit to expense''
                                                                                                                                                                  AND             cctl.name_stg=''Expense''
                                                                                                                                                                  AND             cttl.name_stg = ''Expense''
                                                                                                                                                                  AND             lctl.name_stg = ''Subrogation Expense''
                                                                                                                                                                  AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Recovery''
                                                                                                                                                                  AND             rctl.name_stg = ''Subrogation''
                                                                                                                                                                  AND             cctl.name_stg=''Expense''
                                                                                                                                                                  AND             cttl.name_stg = ''Expense''
                                                                                                                                                                  AND             lctl.name_stg = ''Subrogation Expense''
                                                                                                                                                                  AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Recovery''
                                                                                                                                                                  AND             rctl.name_stg = ''Subrogation''
                                                                                                                                                                  AND             cctl.name_stg=''Expense''
                                                                                                                                                                  AND             cttl.name_stg = ''Expense''
                                                                                                                                                                  AND             lctl.name_stg = ''Recovery''
                                                                                                                                                                  AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                  ELSE 0
                                                                                                                                  END AS acct500314,
                                                                                                                                  CASE
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Reserve''
                                                                                                                                                                  AND             rctl.name_stg IS NULL
                                                                                                                                                                  AND             cctl.name_stg=''Loss''
                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp) ) THEN txli.transactionamount_stg
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Payment''
                                                                                                                                                                  AND             rctl.name_stg IS NULL
                                                                                                                                                                  AND             cctl.name_stg=''Loss''
                                                                                                                                                                  AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                  AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                  AND             lctl.name_stg = ''Deductible''
                                                                                                                                                                  AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp) ) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Payment''
                                                                                                                                                                  AND             rctl.name_stg IS NULL
                                                                                                                                                                  AND             cctl.name_stg=''Loss''
                                                                                                                                                                  AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                  AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                  AND             lctl.name_stg = ''Deductible Refund''
                                                                                                                                                                  AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp) ) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Payment''
                                                                                                                                                                  AND             rctl.name_stg IS NULL
                                                                                                                                                                  AND             cctl.name_stg=''Loss''
                                                                                                                                                                  AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                  AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                  AND             lctl.name_stg = ''Former Deductible''
                                                                                                                                                                  AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp) ) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Payment''
                                                                                                                                                                  AND             rctl.name_stg IS NULL
                                                                                                                                                                  AND             cctl.name_stg=''Loss''
                                                                                                                                                                  AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                  AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                  AND             lctl.name_stg = ''Loss''
                                                                                                                                                                  AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                                  /* Added as part of new task EIM_46414 */
                                                                                                                                                  WHEN (
                                                                                                                                                                                  txtl.name_stg=''Payment''
                                                                                                                                                                  AND             rctl.name_stg IS NULL
                                                                                                                                                                  AND             cctl.name_stg=''Loss''
                                                                                                                                                                  AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                  AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                  AND             lctl.name_stg = ''Diminished Value''
                                                                                                                                                                  AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                  AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                  ELSE 0
                                                                                                                                  END AS outstanding
                                                                                                                  FROM            db_t_prod_stag.cc_claim claim
                                                                                                                  join            db_t_prod_stag.cctl_claimstate tl
                                                                                                                  ON              tl.id_stg = claim.state_stg
                                                                                                                  join            db_t_prod_stag.cc_exposure exp1
                                                                                                                  ON              exp1.claimid_stg = claim.id_stg
                                                                                                                  AND             exp1.retired_stg = 0
                                                                                                                  AND             (
                                                                                                                                                  claim.reporteddate_stg >= cast(:CC_EOY AS timestamp)- interval ''5 year''
                                                                                                                                  AND             claim.reporteddate_stg <= cast(:CC_EOY AS timestamp))
                                                                                                                  AND             (
                                                                                                                                                  claim.lossdate_stg >= cast(:CC_EOY AS timestamp)- interval ''5 year''
                                                                                                                                  AND             claim.lossdate_stg <= cast(:CC_EOY AS timestamp))
                                                                                                                                  /* join DB_T_PROD_STAG.cc_contact expcon on expcon.id = exp1.AssignedUserID */
                                                                                                                                  /* join DB_T_PROD_STAG.cctl_coveragesubtype tl2 on tl2.id = exp1.coveragesubtype */
                                                                                                                                  /* join DB_T_PROD_STAG.cctl_coveragetype tl3 on tl3.id = exp1.PrimaryCoverage */
                                                                                                                  join            db_t_prod_stag.cc_transaction tx
                                                                                                                  ON              tx.exposureid_stg = exp1.id_stg
                                                                                                                  AND             tx.retired_stg = 0
                                                                                                                  join            db_t_prod_stag.cc_transactionlineitem txli
                                                                                                                  ON              txli.transactionid_stg = tx.id_stg
                                                                                                                  join            db_t_prod_stag.cctl_transactionstatus tl4
                                                                                                                  ON              tl4.id_stg = tx.status_stg
                                                                                                                  join            db_t_prod_stag.cc_policy pol
                                                                                                                  ON              pol.id_stg = claim.policyid_stg
                                                                                                                  join            db_t_prod_stag.cctl_jurisdiction tl5
                                                                                                                  ON              tl5.id_stg = pol.basesate_alfa_stg
                                                                                                                  join            db_t_prod_stag.cctl_policytype pttl
                                                                                                                  ON              pttl.id_stg = pol.policytype_stg
                                                                                                                  join            db_t_prod_stag.cctl_policysubtype_alfa psttl
                                                                                                                  ON              psttl.id_stg = pol.policysubtype_alfa_stg
                                                                                                                  join            db_t_prod_stag.cctl_underwritingcompanytype tl6
                                                                                                                  ON              tl6.id_stg = pol.underwritingco_stg
                                                                                                                  left join       db_t_prod_stag.cctl_transaction txtl
                                                                                                                  ON              txtl.id_stg = tx.subtype_stg
                                                                                                                  left join       db_t_prod_stag.cctl_linecategory lctl
                                                                                                                  ON              lctl.id_stg = txli.linecategory_stg
                                                                                                                  left join       db_t_prod_stag.cctl_recoverycategory rctl
                                                                                                                  ON              rctl.id_stg = tx.recoverycategory_stg
                                                                                                                  left join       db_t_prod_stag.cctl_costcategory cctl
                                                                                                                  ON              cctl.id_stg = tx.costcategory_stg
                                                                                                                  left join       db_t_prod_stag.cctl_costtype cttl
                                                                                                                  ON              cttl.id_stg = tx.costtype_stg
                                                                                                                                  /* left join DB_T_PROD_STAG.cc_catastrophe cat on cat.ID = claim.CatastropheID */
                                                                                                                  left join       db_t_prod_stag.cc_check ch
                                                                                                                  ON              ch.id_stg = tx.checkid_stg
                                                                                                                  left join       db_t_prod_stag.cctl_paymentmethod pmtl
                                                                                                                  ON              pmtl.id_stg = ch.paymentmethod_stg
                                                                                                                                  /* left join DB_T_PROD_STAG.cctl_transactionstatus txst on txst.ID = ch.Status */
                                                                                                                                  /* join DB_T_PROD_STAG.cctl_coveragesubtype cov on cov.ID_stg = exp1.CoverageSubType_stg */
                                                                                                                  left join       db_t_prod_stag.cctl_losscause lc
                                                                                                                  ON              lc.id_stg = claim.losscause_stg
                                                                                                                  WHERE           tl4.name_stg NOT IN (''Awaiting submission'',
                                                                                                                                                       ''Rejected'',
                                                                                                                                                       ''Submitting'',
                                                                                                                                                       ''Pending approval'')
                                                                                                                  AND             (
                                                                                                                                                  claimnumber_stg LIKE ''Y%''
                                                                                                                                  OR              claimnumber_stg LIKE ''D%'' ) )a
                                                                                  GROUP BY        a.claimnumber_stg,
                                                                                                  a.closedate,
                                                                                                  a.exposurenumber,
                                                                                                  a.acct500104,
                                                                                                  a.acct500204,
                                                                                                  a.acct500214,
                                                                                                  a.acct500304,
                                                                                                  a.acct500314,
                                                                                                  a.uwco_stg,
                                                                                                  a.lossdate_stg,
                                                                                                  a.policyidcode_stg,
                                                                                                  losscause_stg,
                                                                                                  a.state_stg,
                                                                                                  a.policy_eff_yr,
                                                                                                  a.claimant_identifier_stg,
                                                                                                  a.covrank
                                                                                  HAVING          (
                                                                                                                  acct500104 <> 0
                                                                                                  OR              acct500204 <> 0
                                                                                                  OR              acct500214 <> 0
                                                                                                  OR              acct500304 <> 0
                                                                                                  OR              acct500314 <>0)
                                                                                  OR              (
                                                                                                                  SUM(a.outstanding) <> 0 ) )b
                                                         GROUP BY claim_identifier,
                                                                  b.companynumber,
                                                                  lob,
                                                                  statecode,
                                                                  callyear,
                                                                  accountingyear,
                                                                  exp_yr,
                                                                  exp_mth,
                                                                  exp_day,
                                                                  policyidcode_stg,
                                                                  typeoflosscode,
                                                                  policy_eff_yr,
                                                                  aslob,
                                                                  policyidentificationcode,
                                                                  policyterm,
                                                                  claimant_identifier,
                                                                  wrtprem,
                                                                  lossdate_stg,
                                                                  closedate,
                                                                  covrank,
                                                                  exposurenumber ) c ) src ) );
  -- Component exp_policy_pass_through1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_policy_pass_through1 AS
  (
         SELECT sq_cc_policyperiod.companynumber            AS companynumber,
                sq_cc_policyperiod.lineofbusinesscode       AS lineofbusinesscode,
                sq_cc_policyperiod.statecode                AS statecode,
                sq_cc_policyperiod.callyear                 AS callyear,
                sq_cc_policyperiod.accountingyear           AS accountingyear,
                sq_cc_policyperiod.expperiodyear            AS expperiodyear,
                sq_cc_policyperiod.expperiodmonth           AS expperiodmonth,
                sq_cc_policyperiod.expeperiodday            AS expeperiodday,
                sq_cc_policyperiod.classificationcode       AS classsificationcode,
                sq_cc_policyperiod.typeoflosscode           AS typeoflosscode,
                sq_cc_policyperiod.policyeffectiveyear      AS policyeffectiveyear,
                sq_cc_policyperiod.aslob                    AS aslob,
                sq_cc_policyperiod.policyidentificationcode AS policyidentificationcode,
                sq_cc_policyperiod.policytermcode           AS policytermcode,
                sq_cc_policyperiod.claimidentifier          AS claimidentifier,
                sq_cc_policyperiod.claimantidentifier       AS claimantidentifier,
                sq_cc_policyperiod.writtenpremium           AS writtenpremium,
                sq_cc_policyperiod.paidlosses               AS paidlosses,
                sq_cc_policyperiod.paidnumberofclaims       AS paidnumberofclaims,
                sq_cc_policyperiod.outstandinglosses        AS outstandinglosses,
                sq_cc_policyperiod.outstandingnoofclaims    AS outstandingnoofclaims,
                sq_cc_policyperiod.policynumber             AS policynumber,
                sq_cc_policyperiod.policyperiodid           AS policyperiodid,
                sq_cc_policyperiod.policyidentifier         AS policyidentifier,
                sq_cc_policyperiod.vin                      AS vin,
                sq_cc_policyperiod.exposurenumber           AS exposurenumber,
                sq_cc_policyperiod.source_record_id
         FROM   sq_cc_policyperiod );
  -- Component LKP_CLASS_CODE, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_class_code AS
  (
            SELECT    lkp.classificationcode,
                      exp_policy_pass_through1.source_record_id,
                      row_number() over(PARTITION BY exp_policy_pass_through1.source_record_id ORDER BY lkp.classificationcode ASC) rnk
            FROM      exp_policy_pass_through1
            left join
                      (
                             SELECT pc_policyperiod.classificationcode_stg AS classificationcode,
                                    pc_policyperiod.id_stg                 AS id
                             FROM   (
                                           SELECT
                                                  CASE
                                                         WHEN mt.name_stg = ''Inboard'' THEN ''703100''
                                                         WHEN mt.name_stg = ''Outboard'' THEN ''702100''
                                                         WHEN (
                                                                       mt.name_stg = ''Inboard/Outboard'')
                                                         OR     (
                                                                       mt.name_stg IS NULL
                                                                AND    pc_policyline.papolicytype_alfa_stg IS NOT NULL ) THEN ''799900''
                                                         ELSE ''725000''
                                                  END                             AS classificationcode_stg ,
                                                  cast (pp.id_stg AS VARCHAR(20)) AS id_stg
                                                  /* ,pptl.* */
                                           FROM   db_t_prod_stag.pc_policyperiod pp
                                           join   db_t_prod_stag.pc_policyline
                                           ON     pp.id_stg = pc_policyline.branchid_stg
                                           AND    pc_policyline.expirationdate_stg IS NULL
                                           join   db_t_prod_stag.pc_personalvehicle pv
                                           ON     pv.branchid_stg = pp.id_stg
                                           join   db_t_prod_stag.pctl_motortype_alfa mt
                                           ON     mt.id_stg = pv.motortype_alfa_stg
                                           join   db_t_prod_stag.pctl_papolicytype_alfa pptl
                                           ON     pptl.id_stg =papolicytype_alfa_stg )pc_policyperiod ) lkp
            ON        lkp.id = exp_policy_pass_through1.classsificationcode qualify rnk = 1 );
  -- Component exp_default1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_default1 AS
  (
             SELECT
                        CASE
                                   WHEN exp_policy_pass_through1.companynumber IS NULL THEN ''0000''
                                   ELSE lpad ( exp_policy_pass_through1.companynumber , 4 , ''0'' )
                        END                                         AS o_companynumber,
                        exp_policy_pass_through1.lineofbusinesscode AS lineofbusinesscode,
                        CASE
                                   WHEN exp_policy_pass_through1.statecode IS NULL THEN ''00''
                                   ELSE exp_policy_pass_through1.statecode
                        END                                     AS o_statecode,
                        exp_policy_pass_through1.callyear       AS callyear,
                        exp_policy_pass_through1.accountingyear AS accountingyear,
                        exp_policy_pass_through1.expperiodyear  AS expperiodyear,
                        exp_policy_pass_through1.expperiodmonth AS expperiodmonth,
                        exp_policy_pass_through1.expeperiodday  AS expeperiodday,
                        CASE
                                   WHEN lkp_class_code.classificationcode IS NULL THEN
                                              CASE
                                                         WHEN position(''D'',exp_policy_pass_through1.claimidentifier) > 0 THEN ''799900''
                                                         ELSE ''725000''
                                              END
                                   ELSE lkp_class_code.classificationcode
                        END AS o_classificationcode,
                        CASE
                                   WHEN exp_policy_pass_through1.typeoflosscode IS NULL THEN ''00''
                                   ELSE lpad ( exp_policy_pass_through1.typeoflosscode , 2 , ''0'' )
                        END                                               AS o_typeoflosscode,
                        exp_policy_pass_through1.policyeffectiveyear      AS policyeffectiveyear,
                        exp_policy_pass_through1.aslob                    AS aslob,
                        exp_policy_pass_through1.policyidentificationcode AS policyidentificationcode,
                        CASE
                                   WHEN exp_policy_pass_through1.policytermcode IS NULL THEN ''00''
                                   ELSE lpad ( exp_policy_pass_through1.policytermcode , 2 , ''0'' )
                        END AS o_policytermcode,
                        CASE
                                   WHEN exp_policy_pass_through1.claimidentifier IS NULL THEN ''000000000000000''
                                   ELSE lpad ( exp_policy_pass_through1.claimidentifier , 15 , ''0'' )
                        END AS o_claimidentifier,
                        CASE
                                   WHEN exp_policy_pass_through1.claimantidentifier IS NULL THEN ''000''
                                   ELSE lpad ( exp_policy_pass_through1.claimantidentifier , 3 , ''0'' )
                        END AS o_claimantidentifier,
                        CASE
                                   WHEN exp_policy_pass_through1.writtenpremium IS NULL THEN ''000000000000''
                                   ELSE lpad ( exp_policy_pass_through1.writtenpremium , 12 , ''0'' )
                        END AS o_writtenpremium,
                        CASE
                                   WHEN (
                                                         exp_policy_pass_through1.paidlosses IS NULL
                                              OR         exp_policy_pass_through1.paidlosses = ''0'' ) THEN ''000000000000''
                                   ELSE exp_policy_pass_through1.paidlosses
                        END AS o_paidlosses,
                        CASE
                                   WHEN (
                                                         exp_policy_pass_through1.paidnumberofclaims IS NULL
                                              OR         exp_policy_pass_through1.paidnumberofclaims = ''0'' ) THEN ''000000000000''
                                   ELSE lpad ( exp_policy_pass_through1.paidnumberofclaims , 12 , ''0'' )
                        END AS o_paidnumberofclaims,
                        CASE
                                   WHEN (
                                                         exp_policy_pass_through1.outstandinglosses IS NULL
                                              OR         exp_policy_pass_through1.outstandinglosses = ''0'' ) THEN ''000000000000''
                                   ELSE exp_policy_pass_through1.outstandinglosses
                        END AS o_outstandinglosses,
                        CASE
                                   WHEN (
                                                         exp_policy_pass_through1.outstandingnoofclaims IS NULL
                                              OR         exp_policy_pass_through1.outstandingnoofclaims = ''0'' ) THEN ''000000000000''
                                   ELSE lpad ( exp_policy_pass_through1.outstandingnoofclaims , 12 , ''0'' )
                        END                                   AS o_outstandingnoofclaims,
                        exp_policy_pass_through1.policynumber AS policynumber,
                        CASE
                                   WHEN exp_policy_pass_through1.policyperiodid IS NULL THEN ''''
                                   ELSE rpad ( exp_policy_pass_through1.policyperiodid , 20 , ''0'' )
                        END AS o_policyperiodid,
                        CASE
                                   WHEN exp_policy_pass_through1.policyidentifier IS NULL THEN ''00000000000000000000''
                                   ELSE lpad ( exp_policy_pass_through1.policyidentifier , 20 , ''0'' )
                        END                                     AS o_policyidentifier,
                        current_timestamp                       AS creationts,
                        current_timestamp                       AS updatets,
                        exp_policy_pass_through1.vin            AS vin,
                        exp_policy_pass_through1.exposurenumber AS exposurenumber,
                        :PRCS_ID                                AS prcs_id,
                        exp_policy_pass_through1.source_record_id
             FROM       exp_policy_pass_through1
             inner join lkp_class_code
             ON         exp_policy_pass_through1.source_record_id = lkp_class_code.source_record_id );
  -- Component OUT_NAIIPCI_IM_CC, Type TARGET
  INSERT INTO db_t_prod_comn.out_naiipci_im
              (
                          companynumber,
                          lineofbusinesscode,
                          statecode,
                          callyear,
                          accountingyear,
                          expperiodyear,
                          expperiodmonth,
                          expperiodday,
                          classificationcode,
                          typeoflosscode,
                          policyeffectiveyear,
                          aslob,
                          policyidentificationcode,
                          policyterm,
                          claimidentifier,
                          claimantidentifier,
                          writtenpremium,
                          paidlosses,
                          paidnumberofclaims,
                          outstandinglosses,
                          outstandingnoofclaims,
                          policynumber,
                          policyperiodid,
                          creationts,
                          updatets,
                          policyidentifier,
                          vin,
                          exposurenumber,
                          prcs_id
              )
  SELECT exp_default1.o_companynumber          AS companynumber,
         exp_default1.lineofbusinesscode       AS lineofbusinesscode,
         exp_default1.o_statecode              AS statecode,
         exp_default1.callyear                 AS callyear,
         exp_default1.accountingyear           AS accountingyear,
         exp_default1.expperiodyear            AS expperiodyear,
         exp_default1.expperiodmonth           AS expperiodmonth,
         exp_default1.expeperiodday            AS expperiodday,
         exp_default1.o_classificationcode     AS classificationcode,
         exp_default1.o_typeoflosscode         AS typeoflosscode,
         exp_default1.policyeffectiveyear      AS policyeffectiveyear,
         exp_default1.aslob                    AS aslob,
         exp_default1.policyidentificationcode AS policyidentificationcode,
         exp_default1.o_policytermcode         AS policyterm,
         exp_default1.o_claimidentifier        AS claimidentifier,
         exp_default1.o_claimantidentifier     AS claimantidentifier,
         exp_default1.o_writtenpremium         AS writtenpremium,
         exp_default1.o_paidlosses             AS paidlosses,
         exp_default1.o_paidnumberofclaims     AS paidnumberofclaims,
         exp_default1.o_outstandinglosses      AS outstandinglosses,
         exp_default1.o_outstandingnoofclaims  AS outstandingnoofclaims,
         exp_default1.policynumber             AS policynumber,
         exp_default1.o_policyperiodid         AS policyperiodid,
         exp_default1.creationts               AS creationts,
         exp_default1.updatets                 AS updatets,
         exp_default1.o_policyidentifier       AS policyidentifier,
         exp_default1.vin                      AS vin,
         exp_default1.exposurenumber           AS exposurenumber,
         exp_default1.prcs_id                  AS prcs_id
  FROM   exp_default1;
  
  -- PIPELINE END FOR 2
END;
';