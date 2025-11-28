-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_NAIIPCI_BOP_INS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM timestamp;
  START_DTTM timestamp;
  run_id STRING;
  prcs_id int;
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
CC_BOY := (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''CC_BOY'' order by insert_ts desc limit 1);
CC_EOY := (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''CC_EOY'' order by insert_ts desc limit 1);
CC_EOFQ := (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''CC_EOFQ'' order by insert_ts desc limit 1);
PC_EOY := (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PC_EOY'' order by insert_ts desc limit 1);
PC_BOY := (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PC_BOY'' order by insert_ts desc limit 1);

  -- PIPELINE START FOR 1
  -- Component SQ_cc_Claim, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_cc_claim AS
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
                $9  AS coveragecode,
                $10 AS classificationcode,
                $11 AS typeoflosscode,
                $12 AS territorycode,
                $13 AS policyeffectiveyear,
                $14 AS aslob,
                $15 AS policylimits,
                $16 AS policytermcode,
                $17 AS expsore,
                $18 AS construction,
                $19 AS burglaryoptioncode,
                $20 AS protectionclass,
                $21 AS sprinkler,
                $22 AS amountofinsurance,
                $23 AS typeofpolicycode,
                $24 AS leadpoisoning,
                $25 AS claimidentifier,
                $26 AS claimantidentifier,
                $27 AS wriitenexposure,
                $28 AS writtenpremium,
                $29 AS paidlosses,
                $30 AS paidnumberofclaims,
                $31 AS paidalae,
                $32 AS outstandinglosses,
                $33 AS outstandingnoofclaims,
                $34 AS outstandingalae,
                $35 AS policynumber,
                $36 AS policyperiodid,
                $37 AS policyidentifier,
                $38 AS exposurenumber,
                $39 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   companynumber,
                                                    lob,
                                                    statecode,
                                                    callyear,
                                                    accountingyear,
                                                    exp_yr,
                                                    exp_mth,
                                                    exp_day,
                                                    CASE
                                                             WHEN (
                                                                               coverage LIKE ''%Lia%'') THEN ''05''
                                                             WHEN (
                                                                               coverage LIKE ''%Wind%'') THEN ''13''
                                                             ELSE ''03''
                                                    END coverage_new,
                                                    CASE
                                                             WHEN length(cast(cast(pci_class AS INTEGER) AS VARCHAR(10)))= 5 THEN cast(cast(cast(pci_class AS INTEGER) AS VARCHAR(10))
                                                                               ||''0'' AS VARCHAR(10))
                                                             WHEN length(cast(cast(pci_class AS          INTEGER) AS VARCHAR(10)))= 4 THEN cast(''0''
                                                                               || cast(cast(pci_class AS INTEGER) AS VARCHAR(10))
                                                                               ||''0'' AS VARCHAR(10))
                                                             ELSE rpad(coalesce(cast(pci_class AS INTEGER),''89999''),6,''0'')
                                                    END pci_class,
                                                    typeoflosscode,
                                                    lpad(coalesce(territory_new,''00''),2,''0'')territory_new,
                                                    policy_eff_yr,
                                                    CASE
                                                             WHEN (
                                                                               coverage LIKE ''%Lia%'') THEN ''052''
                                                             ELSE ''051''
                                                    END aslob,
                                                    CASE
                                                             WHEN claim_identifier =''C0000562491'' THEN ''1000''
                                                             ELSE coalesce(lpad(cast(cast(pol_limit/1000 AS INTEGER) AS VARCHAR(6)),4,''0'') ,''0000'')
                                                    END                        pol_limit,
                                                    ''00''                       policyterm,
                                                    coalesce(exposure_new,''0'') exposure_new,
                                                    construction,
                                                    burglary_ind_new,
                                                    protectionclasscode_alfa,
                                                    sprinkler,
                                                    coalesce(lpad(cast(cast(incidentlimit_stg/1000 AS INTEGER) AS VARCHAR(6)),5,''0'') ,''00000'')incidentlimit_stg,
                                                    policytype,
                                                    ''0'' leadpoisioning,
                                                    claim_identifier,
                                                    substring(cast(exposureid_stg AS VARCHAR(10)),4,3)claimant_identifier,
                                                    ''00''                                              writtenexpsore,
                                                    ''00''                                              wrtprem,
                                                    paidloss,
                                                    CASE
                                                             WHEN(
                                                                               closedate > cast(:CC_BOY AS timestamp)
                                                                      AND      closedate < cast(:CC_EOY AS timestamp)
                                                                      AND      paidloss > 0
                                                                      AND      covrank >= 1) THEN 1
                                                             ELSE 0
                                                    END  AS paidclaims,
                                                    ''00''    paidalae,
                                                    outloss,
                                                    CASE
                                                             WHEN(
                                                                               closedate IS NULL
                                                                      OR       closedate > cast(:CC_EOY AS timestamp) )
                                                             AND      covrank >= 1
                                                             AND      outloss>0 THEN 1
                                                             ELSE 0
                                                    END            AS outstandingclaims,
                                                    ''00''              outalae,
                                                    ''0''            AS policynumber,
                                                    ''0''            AS policyperiodid,
                                                    ''0''            AS policyidentifier,
                                                    exposurenumber    RECORD
                                           FROM     (
                                                             SELECT   companynumber,
                                                                      pol_limit,
                                                                      lob,
                                                                      statecode,
                                                                      callyear,
                                                                      losscause_stg,
                                                                      territory_new,
                                                                      exposure_new,
                                                                      accountingyear,
                                                                      exp_yr,
                                                                      exp_mth,
                                                                      exp_day,
                                                                      policyidcode_stg,
                                                                      typeoflosscode,
                                                                      pci_class,
                                                                      policy_eff_yr,
                                                                      aslob,
                                                                      policyidentificationcode,
                                                                      policyterm,
                                                                      claim_identifier,
                                                                      claimant_identifier,
                                                                      wrtprem,
                                                                      sprinkler,
                                                                      incidentlimit_stg,
                                                                      policytype,
                                                                      exposureid_stg,
                                                                      lossdate_stg,
                                                                      protectionclasscode_alfa,
                                                                      construction,
                                                                      closedate,
                                                                      burglary_ind_new,
                                                                      coverage,
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
                                                                                                      ''09'' AS lob,
                                                                                                      CASE
                                                                                                                      WHEN a.state_stg=''AL'' THEN ''01''
                                                                                                                      WHEN a.state_stg=''GA'' THEN ''10''
                                                                                                                      WHEN a.state_stg=''MS'' THEN ''23''
                                                                                                      END AS statecode,
                                                                                                      pci_class,
                                                                                                      territory_new,
                                                                                                      extract(year FROM cast(:CC_EOY AS timestamp))+1 AS callyear,
                                                                                                      extract(year FROM cast(:CC_EOY AS timestamp))   AS accountingyear,
                                                                                                      extract(year FROM a.lossdate_stg)               AS exp_yr,
                                                                                                      losscause_stg,
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
                                                                                                      incidentlimit_stg,
                                                                                                      CASE
                                                                                                                      WHEN losscause_stg IN (''Fire - Total/Other'',
                                                                                                                                             ''fire'',
                                                                                                                                             ''Fire - Total / Other'',
                                                                                                                                             ''Fire - Partial / Other'',
                                                                                                                                             ''Fire'',
                                                                                                                                             ''Total Fire'',
                                                                                                                                             ''Lightning'' ,
                                                                                                                                             ''Fire - Partial/Lightning'',
                                                                                                                                             ''Fire - Total/Lightning'',
                                                                                                                                             ''Lightning'')
                                                                                                                      OR              losscause_stg LIKE ''%Fire%'' THEN ''01''
                                                                                                                      WHEN losscause_stg IN (''Wind'',
                                                                                                                                             ''Hail'',
                                                                                                                                             ''Wind, Quake, Hail, Explosion, Tornado, Water Damage'' )
                                                                                                                      OR              losscause_stg LIKE ''%Wind%'' THEN ''02''
                                                                                                                      WHEN losscause_stg IN ( ''Vandalism/Malicious Mischief'',
                                                                                                                                             ''V & MM'' ) THEN ''04''
                                                                                                                      WHEN losscause_stg LIKE ''%Theft%'' THEN ''03''
                                                                                                                      WHEN losscause_stg LIKE (''%Terrorism%'') THEN ''22''
                                                                                                                      WHEN losscause_stg LIKE''%Liability%'' THEN ''11''
                                                                                                                      ELSE ''09''
                                                                                                      END                               AS typeoflosscode,
                                                                                                      extract(year FROM a.policy_eff_yr)   policy_eff_yr,
                                                                                                      ''090''                             AS aslob,
                                                                                                      ''010''                             AS policyidentificationcode,
                                                                                                      ''00''                              AS policyterm,
                                                                                                      coverage,
                                                                                                      a.claimnumber_stg         AS claim_identifier,
                                                                                                      a.claimant_identifier_stg AS claimant_identifier,
                                                                                                      ''00''                      AS wrtprem,
                                                                                                      a.lossdate_stg,
                                                                                                      a.closedate,
                                                                                                      a.covrank,
                                                                                                      exposure_new,
                                                                                                      construction,
                                                                                                      exposureid_stg,
                                                                                                      a.exposurenumber,
                                                                                                      pol_limit,
                                                                                                      burglary_ind_new,
                                                                                                      protectionclasscode_alfa,
                                                                                                      sprinkler,
                                                                                                      policytype,
                                                                                                      SUM(a.outstanding) AS outres,
                                                                                                      SUM(a.acct500104)  AS acct500104,
                                                                                                      SUM(a.acct500204)  AS acct500204,
                                                                                                      SUM(a.acct500214)  AS acct500214,
                                                                                                      SUM(a.acct500304)  AS acct500304,
                                                                                                      SUM(a.acct500314)  AS acct500314
                                                                                      FROM            (
                                                                                                                      SELECT DISTINCT tx.id_stg                                  AS txid,
                                                                                                                                      claim.claimnumber_stg                      AS claimnumber_stg,
                                                                                                                                      coalesce(construction,construction_pp,''0'')    construction,
                                                                                                                                      CASE
                                                                                                                                                      WHEN cov.typecode_stg LIKE ''%Lia%'' THEN coalesce( expo.exposure_class,expo_pp.exposure_class)
                                                                                                                                                      ELSE ''0''
                                                                                                                                      END                                                                          exposure_new,
                                                                                                                                      coalesce(pollimit.value1,pollimit_pol.value1)                                pol_limit,
                                                                                                                                      coalesce(terr.territory_new,terr_pol.territory_new,terr_claim.territory_new) territory_new,
                                                                                                                                      CASE
                                                                                                                                                      WHEN (
                                                                                                                                                                                      coalesce(bur_bp.burglary_ind,bur_pp.burglary_ind)=''Yes''
                                                                                                                                                                      AND             coalesce(policytype.isnamedperilexistonpolicy_alfa,policytype_pol.isnamedperilexistonpolicy_alfa)=''1'') THEN ''1''
                                                                                                                                                      WHEN (
                                                                                                                                                                                      coalesce(bur_bp.burglary_ind,bur_pp.burglary_ind) IS NULL
                                                                                                                                                                      AND             coalesce(policytype.isnamedperilexistonpolicy_alfa,policytype_pol.isnamedperilexistonpolicy_alfa) IS NOT NULL) THEN ''2''
                                                                                                                                                      ELSE ''0''
                                                                                                                                      END                                                                                               burglary_ind_new,
                                                                                                                                      coalesce(prt_class.protectionclasscode_alfa_stg,prt_class_pp.protectionclasscode_alfa_stg_pp,''00'')protectionclasscode_alfa,
                                                                                                                                      CASE
                                                                                                                                                      WHEN (
                                                                                                                                                                                      spr.bp7sprinklered_stg =1
                                                                                                                                                                      OR              spr_pp.bp7sprinklered_stg_pp =1) THEN ''1''
                                                                                                                                                      ELSE''2''
                                                                                                                                      END sprinkler ,
                                                                                                                                      CASE
                                                                                                                                                      WHEN (
                                                                                                                                                                                      coalesce(policytype.isnamedperilexistonpolicy_alfa,policytype_pol.isnamedperilexistonpolicy_alfa)=''1'') THEN ''1''
                                                                                                                                                      WHEN (
                                                                                                                                                                                      coalesce(policytype.isnamedperilexistonpolicy_alfa,policytype_pol.isnamedperilexistonpolicy_alfa) =0) THEN ''2''
                                                                                                                                                      ELSE ''9''
                                                                                                                                      END policytype,
                                                                                                                                      txli.id_stg,
                                                                                                                                      tl4.name_stg,
                                                                                                                                      tl6.typecode_stg                                                                      AS uwco_stg,
                                                                                                                                      tl5.typecode_stg                                                                      AS state_stg,
                                                                                                                                      pttl.name_stg                                                                         AS policytype_stg,
                                                                                                                                      psttl.typecode_stg                                                                    AS policysubtype_stg,
                                                                                                                                      cov.typecode_stg                                                                         coverage,
                                                                                                                                      coalesce(classi.pci_class,classi_pol.pci_class)                                          pci_class,
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
                                                                                                                                      CASE
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
                                                                                                                                                                      /*  Addded as a part of ticket EIM-46306 */
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
                                                                                                                                                                      AND             txli.createtime_stg <= cast(:CC_EOFQ AS timestamp) ) THEN txli.transactionamount_stg
                                                                                                                                                      WHEN (
                                                                                                                                                                                      txtl.name_stg=''Payment''
                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                      AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                      AND             lctl.name_stg = ''Deductible''
                                                                                                                                                                      AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                      AND             txli.createtime_stg <= cast(:CC_EOFQ AS timestamp) ) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                      WHEN (
                                                                                                                                                                                      txtl.name_stg=''Payment''
                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                      AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                      AND             lctl.name_stg = ''Deductible Refund''
                                                                                                                                                                      AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                      AND             txli.createtime_stg <= cast(:CC_EOFQ AS timestamp) ) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                      WHEN (
                                                                                                                                                                                      txtl.name_stg=''Payment''
                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                      AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                      AND             lctl.name_stg = ''Former Deductible''
                                                                                                                                                                      AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                      AND             txli.createtime_stg <= cast(:CC_EOFQ AS timestamp) ) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                      WHEN (
                                                                                                                                                                                      txtl.name_stg=''Payment''
                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                      AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                      AND             lctl.name_stg = ''Loss''
                                                                                                                                                                      AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                      AND             txli.createtime_stg <= cast(:CC_EOFQ AS timestamp)) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                                      /*  Addded as a part of ticket EIM-46306 */
                                                                                                                                                      WHEN (
                                                                                                                                                                                      txtl.name_stg=''Payment''
                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                      AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                      AND             lctl.name_stg = ''Diminished Value''
                                                                                                                                                                      AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                      AND             txli.createtime_stg <= cast(:CC_EOFQ AS timestamp)) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                      ELSE 0
                                                                                                                                      END AS outstanding ,
                                                                                                                                      exposureid_stg,
                                                                                                                                      coalesce(cc_cov.incidentlimit_stg,cc_cov_pol.incidentlimit_stg)incidentlimit_stg
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
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                               SELECT   *
                                                                                                                                               FROM     (
                                                                                                                                                                        SELECT DISTINCT c.fixedid_stg,
                                                                                                                                                                                        policysystemid_stg,
                                                                                                                                                                                        e.code_stg,
                                                                                                                                                                                        policyid_stg,
                                                                                                                                                                                        incidentlimit_stg
                                                                                                                                                                        FROM            db_t_prod_stag.cc_coverage a
                                                                                                                                                                        join            db_t_prod_stag.cc_policy cc_pol
                                                                                                                                                                        ON              a.policyid_stg =cc_pol.id_stg
                                                                                                                                                                        join            db_t_prod_stag.pcx_bp7buildingcov c
                                                                                                                                                                        ON              c.fixedid_stg =substring(policysystemid_stg,position('':'',cast(policysystemid_stg AS VARCHAR(50)))+1,10)
                                                                                                                                                                        AND             policysystemperiodid_stg=branchid_stg
                                                                                                                                                                        join            db_t_prod_stag.pc_etlclausepattern d
                                                                                                                                                                        ON              d.patternid_stg=c.patterncode_stg
                                                                                                                                                                        join            db_t_prod_stag.pc_etlcovtermpattern e
                                                                                                                                                                        ON              e.clausepatternid_stg=d.id_stg
                                                                                                                                                                        WHERE           e.code_stg IN(''BP7BusnPrsnlPropLimit'',
                                                                                                                                                                                                      ''BP7BuildingLimit'',
                                                                                                                                                                                                      ''BP7EachOccLimit'')
                                                                                                                                                                        AND             c.expirationdate_stg IS NULL
                                                                                                                                                                        UNION
                                                                                                                                                                        SELECT DISTINCT c.fixedid_stg,
                                                                                                                                                                                        policysystemid_stg,
                                                                                                                                                                                        e.code_stg,
                                                                                                                                                                                        policyid_stg,
                                                                                                                                                                                        incidentlimit_stg
                                                                                                                                                                        FROM            db_t_prod_stag.cc_coverage a
                                                                                                                                                                        join            db_t_prod_stag.cc_policy cc_pol
                                                                                                                                                                        ON              a.policyid_stg =cc_pol.id_stg
                                                                                                                                                                        join            db_t_prod_stag.pcx_bp7classificationcov c
                                                                                                                                                                        ON              c.fixedid_stg =substring(policysystemid_stg,position('':'',cast(policysystemid_stg AS VARCHAR(50)))+1,10)
                                                                                                                                                                        AND             policysystemperiodid_stg=branchid_stg
                                                                                                                                                                        join            db_t_prod_stag.pc_etlclausepattern d
                                                                                                                                                                        ON              d.patternid_stg=c.patterncode_stg
                                                                                                                                                                        join            db_t_prod_stag.pc_etlcovtermpattern e
                                                                                                                                                                        ON              e.clausepatternid_stg=d.id_stg
                                                                                                                                                                        WHERE           e.code_stg IN(''BP7BusnPrsnlPropLimit'',
                                                                                                                                                                                                      ''BP7BuildingLimit'',
                                                                                                                                                                                                      ''BP7EachOccLimit'')
                                                                                                                                                                        AND             c.expirationdate_stg IS NULL
                                                                                                                                                                        UNION
                                                                                                                                                                        SELECT DISTINCT c.fixedid_stg,
                                                                                                                                                                                        policysystemid_stg,
                                                                                                                                                                                        e.code_stg,
                                                                                                                                                                                        policyid_stg,
                                                                                                                                                                                        incidentlimit_stg
                                                                                                                                                                        FROM            db_t_prod_stag.cc_coverage a
                                                                                                                                                                        join            db_t_prod_stag.cc_policy cc_pol
                                                                                                                                                                        ON              a.policyid_stg =cc_pol.id_stg
                                                                                                                                                                        join            db_t_prod_stag.pcx_bp7linecov c
                                                                                                                                                                        ON              c.fixedid_stg =substring(policysystemid_stg,position('':'',cast(policysystemid_stg AS VARCHAR(50)))+1,10)
                                                                                                                                                                        AND             policysystemperiodid_stg=branchid_stg
                                                                                                                                                                        join            db_t_prod_stag.pc_etlclausepattern d
                                                                                                                                                                        ON              d.patternid_stg=c.patterncode_stg
                                                                                                                                                                        join            db_t_prod_stag.pc_etlcovtermpattern e
                                                                                                                                                                        ON              e.clausepatternid_stg=d.id_stg
                                                                                                                                                                        WHERE           e.code_stg IN(''BP7BusnPrsnlPropLimit'',
                                                                                                                                                                                                      ''BP7BuildingLimit'',
                                                                                                                                                                                                      ''BP7EachOccLimit'')
                                                                                                                                                                        AND             c.expirationdate_stg IS NULL )a qualify row_number()over(PARTITION BY policyid_stg ORDER BY
                                                                                                                                                        CASE
                                                                                                                                                                 WHEN code_stg=''BP7BuildingLimit'' THEN 1
                                                                                                                                                                 WHEN code_stg=''BP7BusnPrsnlPropLimit'' THEN 1
                                                                                                                                                                 ELSE 3
                                                                                                                                                        END, fixedid_stg)=1 ) cc_cov
                                                                                                                      ON              cc_cov.policyid_stg=claim.policyid_stg
                                                                                                                                      /* and cc_cov.policysystemid_stg like ''%locationcov%'' */
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                               SELECT   *
                                                                                                                                               FROM     (
                                                                                                                                                                        SELECT DISTINCT c.fixedid_stg,
                                                                                                                                                                                        policysystemid_stg,
                                                                                                                                                                                        e.code_stg,
                                                                                                                                                                                        policynumber_stg,
                                                                                                                                                                                        incidentlimit_stg
                                                                                                                                                                        FROM            db_t_prod_stag.cc_coverage a
                                                                                                                                                                        join            db_t_prod_stag.cc_policy cc_pol
                                                                                                                                                                        ON              a.policyid_stg =cc_pol.id_stg
                                                                                                                                                                        join            db_t_prod_stag.pcx_bp7buildingcov c
                                                                                                                                                                        ON              c.fixedid_stg =substring(policysystemid_stg,position('':'',cast(policysystemid_stg AS VARCHAR(50)))+1,10)
                                                                                                                                                                        AND             policysystemperiodid_stg=branchid_stg
                                                                                                                                                                        join            db_t_prod_stag.pc_etlclausepattern d
                                                                                                                                                                        ON              d.patternid_stg=c.patterncode_stg
                                                                                                                                                                        join            db_t_prod_stag.pc_etlcovtermpattern e
                                                                                                                                                                        ON              e.clausepatternid_stg=d.id_stg
                                                                                                                                                                        WHERE           e.code_stg IN(''BP7BusnPrsnlPropLimit'',
                                                                                                                                                                                                      ''BP7BuildingLimit'',
                                                                                                                                                                                                      ''BP7EachOccLimit'')
                                                                                                                                                                        AND             c.expirationdate_stg IS NULL
                                                                                                                                                                        UNION
                                                                                                                                                                        SELECT DISTINCT c.fixedid_stg,
                                                                                                                                                                                        policysystemid_stg,
                                                                                                                                                                                        e.code_stg,
                                                                                                                                                                                        policynumber_stg,
                                                                                                                                                                                        incidentlimit_stg
                                                                                                                                                                        FROM            db_t_prod_stag.cc_coverage a
                                                                                                                                                                        join            db_t_prod_stag.cc_policy cc_pol
                                                                                                                                                                        ON              a.policyid_stg =cc_pol.id_stg
                                                                                                                                                                        join            db_t_prod_stag.pcx_bp7classificationcov c
                                                                                                                                                                        ON              c.fixedid_stg =substring(policysystemid_stg,position('':'',cast(policysystemid_stg AS VARCHAR(50)))+1,10)
                                                                                                                                                                        AND             policysystemperiodid_stg=branchid_stg
                                                                                                                                                                        join            db_t_prod_stag.pc_etlclausepattern d
                                                                                                                                                                        ON              d.patternid_stg=c.patterncode_stg
                                                                                                                                                                        join            db_t_prod_stag.pc_etlcovtermpattern e
                                                                                                                                                                        ON              e.clausepatternid_stg=d.id_stg
                                                                                                                                                                        WHERE           e.code_stg IN(''BP7BusnPrsnlPropLimit'',
                                                                                                                                                                                                      ''BP7BuildingLimit'',
                                                                                                                                                                                                      ''BP7EachOccLimit'')
                                                                                                                                                                        AND             c.expirationdate_stg IS NULL
                                                                                                                                                                        UNION
                                                                                                                                                                        SELECT DISTINCT c.fixedid_stg,
                                                                                                                                                                                        policysystemid_stg,
                                                                                                                                                                                        e.code_stg,
                                                                                                                                                                                        policynumber_stg,
                                                                                                                                                                                        incidentlimit_stg
                                                                                                                                                                        FROM            db_t_prod_stag.cc_coverage a
                                                                                                                                                                        join            db_t_prod_stag.cc_policy cc_pol
                                                                                                                                                                        ON              a.policyid_stg =cc_pol.id_stg
                                                                                                                                                                        join            db_t_prod_stag.pcx_bp7linecov c
                                                                                                                                                                        ON              c.fixedid_stg =substring(policysystemid_stg,position('':'',cast(policysystemid_stg AS VARCHAR(50)))+1,10)
                                                                                                                                                                        AND             policysystemperiodid_stg=branchid_stg
                                                                                                                                                                        join            db_t_prod_stag.pc_etlclausepattern d
                                                                                                                                                                        ON              d.patternid_stg=c.patterncode_stg
                                                                                                                                                                        join            db_t_prod_stag.pc_etlcovtermpattern e
                                                                                                                                                                        ON              e.clausepatternid_stg=d.id_stg
                                                                                                                                                                        WHERE           e.code_stg IN(''BP7BusnPrsnlPropLimit'',
                                                                                                                                                                                                      ''BP7BuildingLimit'',
                                                                                                                                                                                                      ''BP7EachOccLimit'')
                                                                                                                                                                        AND             c.expirationdate_stg IS NULL )a qualify row_number()over(PARTITION BY policynumber_stg ORDER BY
                                                                                                                                                        CASE
                                                                                                                                                                 WHEN code_stg=''BP7BuildingLimit'' THEN 1
                                                                                                                                                                 WHEN code_stg=''BP7BusnPrsnlPropLimit'' THEN 1
                                                                                                                                                                 ELSE 3
                                                                                                                                                        END, fixedid_stg)=1 ) cc_cov_pol
                                                                                                                      ON              cc_cov_pol.policynumber_stg=pol.policynumber_stg
                                                                                                                      join            db_t_prod_stag.cc_incident inc
                                                                                                                      ON              claim.id_stg=inc.claimid_stg
                                                                                                                      join            db_t_prod_stag.cctl_coveragesubtype cov
                                                                                                                      ON              cov.id_stg=exp1.coveragesubtype_stg
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
                                                                                                                                      /* join DB_T_PROD_STAG.CCTL_COVERAGESUBTYPE cov on cov.ID_stg = exp1.CoverageSubType_stg */
                                                                                                                      left join       db_t_prod_stag.cctl_losscause lc
                                                                                                                      ON              lc.id_stg = claim.losscause_stg
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                                      /*Classification*/
                                                                                                                                                      SELECT DISTINCT policynumber_stg,
                                                                                                                                                                      a.id_stg,
                                                                                                                                                                      b.fixedid_stg classid,
                                                                                                                                                                      d.name_stg,
                                                                                                                                                                      coalesce(pci_class,e.code_stg) pci_class
                                                                                                                                                      FROM            db_t_prod_stag.pc_policyperiod a
                                                                                                                                                      join            db_t_prod_stag.pcx_bp7classification b
                                                                                                                                                      ON              b.branchid_stg = a.id_stg
                                                                                                                                                      AND             b.expirationdate_stg IS NULL
                                                                                                                                                      join            db_t_prod_stag.pctl_bp7classificationproperty c
                                                                                                                                                      ON              c.id_stg = b.bp7classpropertytype_stg
                                                                                                                                                      join            db_t_prod_stag.pctl_bp7classdescription d
                                                                                                                                                      ON              d.id_stg = b.bp7classdescription_stg
                                                                                                                                                      left join       db_t_prod_stag.pcx_bp7classcode e
                                                                                                                                                      ON              e.description_stg = d.description_stg
                                                                                                                                                      AND             e.propertytype_stg = c.name_stg
                                                                                                                                                      AND             e.expirationdate_stg IS NULL
                                                                                                                                                      left join       db_t_prod_stag.classification_values
                                                                                                                                                      ON              classification_values=d.name_stg qualify row_number() over(PARTITION BY policynumber_stg,a.id_stg ORDER BY b.fixedid_stg,coalesce(b.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC)=1 ) classi
                                                                                                                      ON              pol.policysystemperiodid_stg =classi.id_stg
                                                                                                                      AND             pol.policynumber_stg =classi.policynumber_stg
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                                      /*Classification*/
                                                                                                                                                      SELECT DISTINCT policynumber_stg,
                                                                                                                                                                      a.id_stg,
                                                                                                                                                                      b.fixedid_stg classid,
                                                                                                                                                                      d.name_stg,
                                                                                                                                                                      coalesce(pci_class,e.code_stg) pci_class
                                                                                                                                                      FROM            db_t_prod_stag.pc_policyperiod a
                                                                                                                                                      join            db_t_prod_stag.pcx_bp7classification b
                                                                                                                                                      ON              b.branchid_stg = a.id_stg
                                                                                                                                                      AND             b.expirationdate_stg IS NULL
                                                                                                                                                      join            db_t_prod_stag.pctl_bp7classificationproperty c
                                                                                                                                                      ON              c.id_stg = b.bp7classpropertytype_stg
                                                                                                                                                      join            db_t_prod_stag.pctl_bp7classdescription d
                                                                                                                                                      ON              d.id_stg = b.bp7classdescription_stg
                                                                                                                                                      left join       db_t_prod_stag.pcx_bp7classcode e
                                                                                                                                                      ON              e.description_stg = d.description_stg
                                                                                                                                                      AND             e.propertytype_stg = c.name_stg
                                                                                                                                                      AND             e.expirationdate_stg IS NULL
                                                                                                                                                      left join       db_t_prod_stag.classification_values
                                                                                                                                                      ON              classification_values=d.name_stg qualify row_number() over(PARTITION BY policynumber_stg ORDER BY b.fixedid_stg,coalesce(b.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC)=1 ) classi_pol
                                                                                                                      ON              pol.policynumber_stg =classi_pol.policynumber_stg
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                                      /*TERRITORYCODE*/
                                                                                                                                                      SELECT DISTINCT
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN g.typecode_stg =''AL''
                                                                                                                                                                                      AND             upper(coalesce(cityinternal_stg,''A''))=''BIRMINGHAM''
                                                                                                                                                                                      AND             upper(coalesce(countyinternal_stg,''A''))=''JEFFERSON'' THEN ''01''
                                                                                                                                                                                      WHEN g.typecode_stg =''AL''
                                                                                                                                                                                      AND             (
                                                                                                                                                                                                        upper(coalesce(cityinternal_stg,''A''))<>''BIRMINGHAM''
                                                                                                                                                                                                      OR              upper(coalesce(countyinternal_stg,''A''))<>''JEFFERSON'' )THEN ''03''
                                                                                                                                                                                      WHEN g.typecode_stg =''GA''
                                                                                                                                                                                      AND             upper(coalesce(cityinternal_stg,''A''))=''ATLANTA''
                                                                                                                                                                                      AND             upper(coalesce(countyinternal_stg,''A''))IN (''DEKALB'',
                                                                                                                                                                                                        ''FULTON'') THEN ''02''
                                                                                                                                                                                      WHEN g.typecode_stg =''GA''
                                                                                                                                                                                      AND             (
                                                                                                                                                                                                        upper(coalesce(cityinternal_stg,''A''))<>''ATLANTA''
                                                                                                                                                                                                      OR              upper(coalesce(countyinternal_stg,''A'')) NOT IN (''DEKALB'',
                                                                                                                                                                                                        ''FULTON'')) THEN ''03''
                                                                                                                                                                                      WHEN g.typecode_stg =''MS'' THEN ''01''
                                                                                                                                                                      END territory_new ,
                                                                                                                                                                      e.branchid_stg,
                                                                                                                                                                      policynumber_stg,
                                                                                                                                                                      c.code_stg,
                                                                                                                                                                      cityinternal_stg,
                                                                                                                                                                      countyinternal_stg ,
                                                                                                                                                                      row_number() over( PARTITION BY e.branchid_stg,policynumber_stg ORDER BY
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        c.policylocation_stg = b.id_stg) THEN 1
                                                                                                                                                                                      ELSE 2
                                                                                                                                                                      END,e.fixedid_stg) ROWNUM
                                                                                                                                                      FROM            db_t_prod_stag.pc_policyperiod a
                                                                                                                                                      join            db_t_prod_stag.pcx_bp7building e
                                                                                                                                                      ON              e.branchid_stg=a.id_stg
                                                                                                                                                      AND             e.expirationdate_stg IS NULL
                                                                                                                                                      left join       db_t_prod_stag.pc_effectivedatedfields eff
                                                                                                                                                      ON              eff.branchid_stg = a.id_stg
                                                                                                                                                      AND             eff.expirationdate_stg IS NULL
                                                                                                                                                      left join       db_t_prod_stag.pc_policylocation b
                                                                                                                                                      ON              b.id_stg= eff.primarylocation_stg
                                                                                                                                                      AND             b.expirationdate_stg IS NULL
                                                                                                                                                      join            db_t_prod_stag.pcx_bp7location bpl
                                                                                                                                                      ON              bpl.id_stg= e.location_stg
                                                                                                                                                      AND             bpl.expirationdate_stg IS NULL
                                                                                                                                                      join            db_t_prod_stag.pc_territorycode c
                                                                                                                                                      ON              c.branchid_stg = a.id_stg
                                                                                                                                                      AND             c.expirationdate_stg IS NULL
                                                                                                                                                      join            db_t_prod_stag.pctl_territorycode d
                                                                                                                                                      ON              c.subtype_stg=d.id_stg
                                                                                                                                                      join            db_t_prod_stag.pctl_jurisdiction g
                                                                                                                                                      ON              basestate_stg=g.id_stg
                                                                                                                                                      WHERE           d.typecode_stg=''BP7TerritoryCode_alfa'' qualify ROWNUM=1 )terr
                                                                                                                      ON              pol.policysystemperiodid_stg=terr.branchid_stg
                                                                                                                      AND             pol.policynumber_stg=terr.policynumber_stg
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                                      /*TERRITORYCODE*/
                                                                                                                                                      SELECT DISTINCT
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN g.typecode_stg =''AL''
                                                                                                                                                                                      AND             upper(coalesce(cityinternal_stg,''A''))=''BIRMINGHAM''
                                                                                                                                                                                      AND             upper(coalesce(countyinternal_stg,''A''))=''JEFFERSON'' THEN ''01''
                                                                                                                                                                                      WHEN g.typecode_stg =''AL''
                                                                                                                                                                                      AND             (
                                                                                                                                                                                                        upper(coalesce(cityinternal_stg,''A''))<>''BIRMINGHAM''
                                                                                                                                                                                                      OR              upper(coalesce(countyinternal_stg,''A''))<>''JEFFERSON'' )THEN ''03''
                                                                                                                                                                                      WHEN g.typecode_stg =''GA''
                                                                                                                                                                                      AND             upper(coalesce(cityinternal_stg,''A''))=''ATLANTA''
                                                                                                                                                                                      AND             upper(coalesce(countyinternal_stg,''A''))IN (''DEKALB'',
                                                                                                                                                                                                        ''FULTON'') THEN ''02''
                                                                                                                                                                                      WHEN g.typecode_stg =''GA''
                                                                                                                                                                                      AND             (
                                                                                                                                                                                                        upper(coalesce(cityinternal_stg,''A''))<>''ATLANTA''
                                                                                                                                                                                                      OR              upper(coalesce(countyinternal_stg,''A'')) NOT IN (''DEKALB'',
                                                                                                                                                                                                        ''FULTON'')) THEN ''03''
                                                                                                                                                                                      WHEN g.typecode_stg =''MS'' THEN ''01''
                                                                                                                                                                      END territory_new ,
                                                                                                                                                                      e.branchid_stg,
                                                                                                                                                                      policynumber_stg,
                                                                                                                                                                      c.code_stg,
                                                                                                                                                                      cityinternal_stg,
                                                                                                                                                                      countyinternal_stg ,
                                                                                                                                                                      row_number() over( PARTITION BY policynumber_stg ORDER BY
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        c.policylocation_stg = b.id_stg) THEN 1
                                                                                                                                                                                      ELSE 2
                                                                                                                                                                      END, e.fixedid_stg) ROWNUM
                                                                                                                                                      FROM            db_t_prod_stag.pc_policyperiod a
                                                                                                                                                      join            db_t_prod_stag.pcx_bp7building e
                                                                                                                                                      ON              e.branchid_stg=a.id_stg
                                                                                                                                                      AND             e.expirationdate_stg IS NULL
                                                                                                                                                      left join       db_t_prod_stag.pc_effectivedatedfields eff
                                                                                                                                                      ON              eff.branchid_stg = a.id_stg
                                                                                                                                                      AND             eff.expirationdate_stg IS NULL
                                                                                                                                                      left join       db_t_prod_stag.pc_policylocation b
                                                                                                                                                      ON              b.id_stg= eff.primarylocation_stg
                                                                                                                                                      AND             b.expirationdate_stg IS NULL
                                                                                                                                                      join            db_t_prod_stag.pcx_bp7location bpl
                                                                                                                                                      ON              bpl.id_stg= e.location_stg
                                                                                                                                                      AND             bpl.expirationdate_stg IS NULL
                                                                                                                                                      join            db_t_prod_stag.pc_territorycode c
                                                                                                                                                      ON              c.branchid_stg = a.id_stg
                                                                                                                                                      AND             c.expirationdate_stg IS NULL
                                                                                                                                                      join            db_t_prod_stag.pctl_territorycode d
                                                                                                                                                      ON              c.subtype_stg=d.id_stg
                                                                                                                                                      join            db_t_prod_stag.pctl_jurisdiction g
                                                                                                                                                      ON              basestate_stg=g.id_stg
                                                                                                                                                      WHERE           d.typecode_stg=''BP7TerritoryCode_alfa'' qualify ROWNUM=1 )terr_pol
                                                                                                                      ON              pol.policynumber_stg=terr_pol.policynumber_stg
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                                      SELECT DISTINCT a.policyid_stg,
                                                                                                                                                                      protectionclasscode_alfa_stg ,
                                                                                                                                                                      claimnumber_stg,
                                                                                                                                                                      coalesce(
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN g.typecode_stg =''AL''
                                                                                                                                                                                      AND             upper(coalesce(e.city_stg,''A''))=''BIRMINGHAM''
                                                                                                                                                                                      AND             upper(coalesce(e.county_stg,''A''))=''JEFFERSON'' THEN ''01''
                                                                                                                                                                                      WHEN g.typecode_stg =''AL''
                                                                                                                                                                                      AND             (
                                                                                                                                                                                                        upper(coalesce(e.city_stg,''A''))<>''BIRMINGHAM''
                                                                                                                                                                                                      OR              upper(coalesce(e.county_stg,''A''))<>''JEFFERSON'' )THEN ''03''
                                                                                                                                                                                      WHEN g.typecode_stg =''GA''
                                                                                                                                                                                      AND             upper(coalesce(e.city_stg,''A''))=''ATLANTA''
                                                                                                                                                                                      AND             upper(coalesce(e.county_stg,''A''))IN (''DEKALB'',
                                                                                                                                                                                                        ''FULTON'') THEN ''02''
                                                                                                                                                                                      WHEN g.typecode_stg =''GA''
                                                                                                                                                                                      AND             (
                                                                                                                                                                                                        upper(coalesce(e.city_stg,''A''))<>''ATLANTA''
                                                                                                                                                                                                      OR              upper(coalesce(e.county_stg,''A'')) NOT IN (''DEKALB'',
                                                                                                                                                                                                        ''FULTON'')) THEN ''03''
                                                                                                                                                                                      WHEN g.typecode_stg =''MS'' THEN ''01''
                                                                                                                                                                      END ,
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN loss_g.typecode_stg =''AL''
                                                                                                                                                                                      AND             upper(coalesce(loss_e.city_stg,''A''))=''BIRMINGHAM''
                                                                                                                                                                                      AND             upper(coalesce(loss_e.county_stg,''A''))=''JEFFERSON'' THEN ''01''
                                                                                                                                                                                      WHEN loss_g.typecode_stg =''AL''
                                                                                                                                                                                      AND             (
                                                                                                                                                                                                        upper(coalesce(loss_e.city_stg,''A''))<>''BIRMINGHAM''
                                                                                                                                                                                                      OR              upper(coalesce(loss_e.county_stg,''A''))<>''JEFFERSON'' )THEN ''03''
                                                                                                                                                                                      WHEN loss_g.typecode_stg =''GA''
                                                                                                                                                                                      AND             upper(coalesce(loss_e.city_stg,''A''))=''ATLANTA''
                                                                                                                                                                                      AND             upper(coalesce(loss_e.county_stg,''A''))IN (''DEKALB'',
                                                                                                                                                                                                        ''FULTON'') THEN ''02''
                                                                                                                                                                                      WHEN loss_g.typecode_stg =''GA''
                                                                                                                                                                                      AND             (
                                                                                                                                                                                                        upper(coalesce(loss_e.city_stg,''A''))<>''ATLANTA''
                                                                                                                                                                                                      OR              upper(coalesce(loss_e.county_stg,''A'')) NOT IN (''DEKALB'',
                                                                                                                                                                                                        ''FULTON'')) THEN ''03''
                                                                                                                                                                                      WHEN loss_g.typecode_stg =''MS'' THEN ''01''
                                                                                                                                                                      END)territory_new
                                                                                                                                                      FROM            db_t_prod_stag.cc_claim c
                                                                                                                                                      left join       db_t_prod_stag.cc_policylocation a
                                                                                                                                                      ON              a.policyid_stg =c.policyid_stg
                                                                                                                                                      left join       db_t_prod_stag.cc_address e
                                                                                                                                                      ON              a.addressid_stg=e.id_stg
                                                                                                                                                      left join       db_t_prod_stag.cctl_state g
                                                                                                                                                      ON              e.state_stg=g.id_stg
                                                                                                                                                      left join       db_t_prod_stag.cc_address loss_e
                                                                                                                                                      ON              c.losslocationid_stg=loss_e.id_stg
                                                                                                                                                      left join       db_t_prod_stag.cctl_state loss_g
                                                                                                                                                      ON              loss_e.state_stg=loss_g.id_stg
                                                                                                                                                      WHERE           claimnumber_stg LIKE ''E%''
                                                                                                                                                      OR              claimnumber_stg LIKE ''C%'' )terr_claim
                                                                                                                      ON              terr_claim.claimnumber_stg =claim.claimnumber_stg
                                                                                                                                      /*policylimit*/
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                                      SELECT DISTINCT polcov.branchid,
                                                                                                                                                                      value1,
                                                                                                                                                                      assetkey
                                                                                                                                                      FROM            (
                                                                                                                                                                                 SELECT     cast(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                                                                                            cast(choiceterm1_stg AS VARCHAR(255)) AS val,
                                                                                                                                                                                            patterncode_stg,
                                                                                                                                                                                            cast(branchid_stg AS    INTEGER)      AS branchid,
                                                                                                                                                                                            cast(bp7line_stg AS     VARCHAR(255)) AS assetkey,
                                                                                                                                                                                            cast(''pc_policyline'' AS VARCHAR(250)) AS assettype,
                                                                                                                                                                                            pcx_bp7linecov.createtime_stg,
                                                                                                                                                                                            effectivedate_stg,
                                                                                                                                                                                            expirationdate_stg,
                                                                                                                                                                                            cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                                                                                            cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                                                                                            pcx_bp7linecov.updatetime_stg
                                                                                                                                                                                 FROM       db_t_prod_stag.pcx_bp7linecov
                                                                                                                                                                                 inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                                                 ON         pp.id_stg = pcx_bp7linecov.branchid_stg
                                                                                                                                                                                 WHERE      choiceterm1avl_stg = 1
                                                                                                                                                                                 AND        expirationdate_stg IS NULL ) polcov
                                                                                                                                                      inner join
                                                                                                                                                                      (
                                                                                                                                                                             SELECT cast(id_stg AS VARCHAR(255)) AS id,
                                                                                                                                                                                    policynumber_stg,
                                                                                                                                                                                    periodstart_stg,
                                                                                                                                                                                    periodend_stg,
                                                                                                                                                                                    mostrecentmodel_stg,
                                                                                                                                                                                    status_stg,
                                                                                                                                                                                    jobid_stg,
                                                                                                                                                                                    publicid_stg,
                                                                                                                                                                                    createtime_stg,
                                                                                                                                                                                    updatetime_stg,
                                                                                                                                                                                    retired_stg
                                                                                                                                                                             FROM   db_t_prod_stag.pc_policyperiod) pp
                                                                                                                                                      ON              pp.id= polcov.branchid
                                                                                                                                                      left join
                                                                                                                                                                      (
                                                                                                                                                                             SELECT pcl.patternid_stg     clausepatternid,
                                                                                                                                                                                    pcv.patternid_stg     covtermpatternid,
                                                                                                                                                                                    pcv.columnname_stg  AS columnname,
                                                                                                                                                                                    pcv.covtermtype_stg AS covtermtype,
                                                                                                                                                                                    pcl.name_stg        AS clausename
                                                                                                                                                                             FROM   db_t_prod_stag.pc_etlclausepattern pcl
                                                                                                                                                                             join   db_t_prod_stag.pc_etlcovtermpattern pcv
                                                                                                                                                                             ON     pcl.id_stg = pcv.clausepatternid_stg
                                                                                                                                                                             UNION
                                                                                                                                                                             SELECT    pcl.patternid_stg                       clausepatternid,
                                                                                                                                                                                       pcv.patternid_stg                       covtermpatternid,
                                                                                                                                                                                       coalesce(pcv.columnname_stg,''Clause'')   columnname,
                                                                                                                                                                                       coalesce(pcv.covtermtype_stg, ''Clause'') covtermtype,
                                                                                                                                                                                       pcl.name_stg                            clausename
                                                                                                                                                                             FROM      db_t_prod_stag.pc_etlclausepattern pcl
                                                                                                                                                                             left join
                                                                                                                                                                                       (
                                                                                                                                                                                              SELECT *
                                                                                                                                                                                              FROM   db_t_prod_stag.pc_etlcovtermpattern
                                                                                                                                                                                              WHERE  name_stg NOT LIKE ''ZZ%'') pcv
                                                                                                                                                                             ON        pcv.clausepatternid_stg = pcl.id_stg
                                                                                                                                                                             WHERE     pcl.name_stg NOT LIKE ''ZZ%''
                                                                                                                                                                             AND       pcv.name_stg IS NULL
                                                                                                                                                                             AND       pcl.owningentitytype_stg IN (''BP7BusinessOwnersLine'') ) covterm
                                                                                                                                                      ON              covterm.clausepatternid = polcov.patterncode_stg
                                                                                                                                                      AND             covterm.columnname = polcov.columnname
                                                                                                                                                      left outer join
                                                                                                                                                                      (
                                                                                                                                                                             SELECT pcp.patternid_stg   packagepatternid,
                                                                                                                                                                                    pcp.packagecode_stg cov_id,
                                                                                                                                                                                    pcp.packagecode_stg name1
                                                                                                                                                                             FROM   db_t_prod_stag.pc_etlcovtermpackage pcp) PACKAGE
                                                                                                                                                      ON              PACKAGE.packagepatternid = polcov.val
                                                                                                                                                      left outer join
                                                                                                                                                                      (
                                                                                                                                                                                 SELECT     pco.patternid_stg                      optionpatternid,
                                                                                                                                                                                            pco.optioncode_stg                     name1,
                                                                                                                                                                                            cast(pco.value_stg AS VARCHAR(255)) AS value1,
                                                                                                                                                                                            pcp.valuetype_stg                   AS valuetype
                                                                                                                                                                                 FROM       db_t_prod_stag.pc_etlcovtermpattern pcp
                                                                                                                                                                                 inner join db_t_prod_stag.pc_etlcovtermoption pco
                                                                                                                                                                                 ON         pcp.id_stg = pco.coveragetermpatternid_stg ) optn
                                                                                                                                                      ON              optn.optionpatternid = polcov.val
                                                                                                                                                      inner join      db_t_prod_stag.pctl_policyperiodstatus pps
                                                                                                                                                      ON              pps.id_stg = pp.status_stg
                                                                                                                                                      inner join      db_t_prod_stag.pc_job pj
                                                                                                                                                      ON              pj.id_stg = pp.jobid_stg
                                                                                                                                                      inner join      db_t_prod_stag.pctl_job pcj
                                                                                                                                                      ON              pcj.id_stg = pj.subtype_stg
                                                                                                                                                      WHERE           covterm.clausename NOT LIKE''%ZZ%''
                                                                                                                                                      AND             pps.typecode_stg = ''Bound''
                                                                                                                                                      AND             covterm.covtermpatternid=''BP7EachOccLimit'' qualify row_number() over(PARTITION BY branchid ORDER BY assetkey )=1 ) pollimit
                                                                                                                      ON              pollimit.branchid=pol.policysystemperiodid_stg
                                                                                                                                      /*policylimit*/
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                                      SELECT DISTINCT pp.policynumber_stg,
                                                                                                                                                                      value1,
                                                                                                                                                                      assetkey
                                                                                                                                                      FROM            (
                                                                                                                                                                                 SELECT     cast(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                                                                                            cast(choiceterm1_stg AS VARCHAR(255)) AS val,
                                                                                                                                                                                            patterncode_stg,
                                                                                                                                                                                            cast(branchid_stg AS    INTEGER)      AS branchid,
                                                                                                                                                                                            cast(bp7line_stg AS     VARCHAR(255)) AS assetkey,
                                                                                                                                                                                            cast(''pc_policyline'' AS VARCHAR(250)) AS assettype,
                                                                                                                                                                                            pcx_bp7linecov.createtime_stg,
                                                                                                                                                                                            effectivedate_stg,
                                                                                                                                                                                            expirationdate_stg,
                                                                                                                                                                                            cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                                                                                            cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                                                                                            pcx_bp7linecov.updatetime_stg
                                                                                                                                                                                 FROM       db_t_prod_stag.pcx_bp7linecov
                                                                                                                                                                                 inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                                                 ON         pp.id_stg = pcx_bp7linecov.branchid_stg
                                                                                                                                                                                 WHERE      choiceterm1avl_stg = 1
                                                                                                                                                                                 AND        expirationdate_stg IS NULL ) polcov
                                                                                                                                                      inner join
                                                                                                                                                                      (
                                                                                                                                                                             SELECT cast(id_stg AS VARCHAR(255)) AS id,
                                                                                                                                                                                    policynumber_stg,
                                                                                                                                                                                    periodstart_stg,
                                                                                                                                                                                    periodend_stg,
                                                                                                                                                                                    mostrecentmodel_stg,
                                                                                                                                                                                    status_stg,
                                                                                                                                                                                    jobid_stg,
                                                                                                                                                                                    publicid_stg,
                                                                                                                                                                                    createtime_stg,
                                                                                                                                                                                    updatetime_stg,
                                                                                                                                                                                    retired_stg
                                                                                                                                                                             FROM   db_t_prod_stag.pc_policyperiod) pp
                                                                                                                                                      ON              pp.id= polcov.branchid
                                                                                                                                                      left join
                                                                                                                                                                      (
                                                                                                                                                                             SELECT pcl.patternid_stg     clausepatternid,
                                                                                                                                                                                    pcv.patternid_stg     covtermpatternid,
                                                                                                                                                                                    pcv.columnname_stg  AS columnname,
                                                                                                                                                                                    pcv.covtermtype_stg AS covtermtype,
                                                                                                                                                                                    pcl.name_stg        AS clausename
                                                                                                                                                                             FROM   db_t_prod_stag.pc_etlclausepattern pcl
                                                                                                                                                                             join   db_t_prod_stag.pc_etlcovtermpattern pcv
                                                                                                                                                                             ON     pcl.id_stg = pcv.clausepatternid_stg
                                                                                                                                                                             UNION
                                                                                                                                                                             SELECT    pcl.patternid_stg                       clausepatternid,
                                                                                                                                                                                       pcv.patternid_stg                       covtermpatternid,
                                                                                                                                                                                       coalesce(pcv.columnname_stg,''Clause'')   columnname,
                                                                                                                                                                                       coalesce(pcv.covtermtype_stg, ''Clause'') covtermtype,
                                                                                                                                                                                       pcl.name_stg                            clausename
                                                                                                                                                                             FROM      db_t_prod_stag.pc_etlclausepattern pcl
                                                                                                                                                                             left join
                                                                                                                                                                                       (
                                                                                                                                                                                              SELECT *
                                                                                                                                                                                              FROM   db_t_prod_stag.pc_etlcovtermpattern
                                                                                                                                                                                              WHERE  name_stg NOT LIKE ''ZZ%'') pcv
                                                                                                                                                                             ON        pcv.clausepatternid_stg = pcl.id_stg
                                                                                                                                                                             WHERE     pcl.name_stg NOT LIKE ''ZZ%''
                                                                                                                                                                             AND       pcv.name_stg IS NULL
                                                                                                                                                                             AND       pcl.owningentitytype_stg IN (''BP7BusinessOwnersLine'') ) covterm
                                                                                                                                                      ON              covterm.clausepatternid = polcov.patterncode_stg
                                                                                                                                                      AND             covterm.columnname = polcov.columnname
                                                                                                                                                      left outer join
                                                                                                                                                                      (
                                                                                                                                                                             SELECT pcp.patternid_stg   packagepatternid,
                                                                                                                                                                                    pcp.packagecode_stg cov_id,
                                                                                                                                                                                    pcp.packagecode_stg name1
                                                                                                                                                                             FROM   db_t_prod_stag.pc_etlcovtermpackage pcp) PACKAGE
                                                                                                                                                      ON              PACKAGE.packagepatternid = polcov.val
                                                                                                                                                      left outer join
                                                                                                                                                                      (
                                                                                                                                                                                 SELECT     pco.patternid_stg                      optionpatternid,
                                                                                                                                                                                            pco.optioncode_stg                     name1,
                                                                                                                                                                                            cast(pco.value_stg AS VARCHAR(255)) AS value1,
                                                                                                                                                                                            pcp.valuetype_stg                   AS valuetype
                                                                                                                                                                                 FROM       db_t_prod_stag.pc_etlcovtermpattern pcp
                                                                                                                                                                                 inner join db_t_prod_stag.pc_etlcovtermoption pco
                                                                                                                                                                                 ON         pcp.id_stg = pco.coveragetermpatternid_stg ) optn
                                                                                                                                                      ON              optn.optionpatternid = polcov.val
                                                                                                                                                      inner join      db_t_prod_stag.pctl_policyperiodstatus pps
                                                                                                                                                      ON              pps.id_stg = pp.status_stg
                                                                                                                                                      inner join      db_t_prod_stag.pc_job pj
                                                                                                                                                      ON              pj.id_stg = pp.jobid_stg
                                                                                                                                                      inner join      db_t_prod_stag.pctl_job pcj
                                                                                                                                                      ON              pcj.id_stg = pj.subtype_stg
                                                                                                                                                      WHERE           covterm.clausename NOT LIKE''%ZZ%''
                                                                                                                                                      AND             pps.typecode_stg = ''Bound''
                                                                                                                                                      AND             covterm.covtermpatternid=''BP7EachOccLimit'' qualify row_number() over(PARTITION BY policynumber_stg ORDER BY assetkey )=1 ) pollimit_pol
                                                                                                                      ON              pollimit_pol.policynumber_stg=pol.policynumber_stg
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                               /*exposure*/
                                                                                                                                               SELECT   pp.id_stg,
                                                                                                                                                        pc.fixedid_stg,
                                                                                                                                                        CASE
                                                                                                                                                                 WHEN pep.typecode_stg =''LimitofInsurance'' THEN ''3''
                                                                                                                                                                 WHEN pep.typecode_stg =''AnnualGrossSales'' THEN ''2''
                                                                                                                                                                 WHEN pep.typecode_stg =''AnnualPayroll'' THEN ''1''
                                                                                                                                                        END exposure_class
                                                                                                                                               FROM     db_t_prod_stag.pc_policyperiod pp
                                                                                                                                               join     db_t_prod_stag.pcx_bp7classification pc
                                                                                                                                               ON       pc.branchid_stg = pp.id_stg
                                                                                                                                               AND      pc.expirationdate_stg IS NULL
                                                                                                                                               join     db_t_prod_stag.pctl_bp7exposurebasis pep
                                                                                                                                               ON       bp7exposurebasis_stg=pep.id_stg qualify row_number() over(PARTITION BY pp.id_stg ORDER BY pc.fixedid_stg,coalesce(pc.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC)=1 ) expo
                                                                                                                      ON              expo.id_stg=pol.policysystemperiodid_stg
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                               /*exposure pp*/
                                                                                                                                               SELECT   pp.policynumber_stg,
                                                                                                                                                        max(
                                                                                                                                                        CASE
                                                                                                                                                                 WHEN pep.typecode_stg =''LimitofInsurance'' THEN ''3''
                                                                                                                                                                 WHEN pep.typecode_stg =''AnnualGrossSales'' THEN ''2''
                                                                                                                                                                 WHEN pep.typecode_stg =''AnnualPayroll'' THEN ''1''
                                                                                                                                                        END) exposure_class
                                                                                                                                               FROM     db_t_prod_stag.pc_policyperiod pp
                                                                                                                                               join     db_t_prod_stag.pcx_bp7classification pc
                                                                                                                                               ON       pc.branchid_stg = pp.id_stg
                                                                                                                                               AND      pc.expirationdate_stg IS NULL
                                                                                                                                               join     db_t_prod_stag.pctl_bp7exposurebasis pep
                                                                                                                                               ON       bp7exposurebasis_stg=pep.id_stg
                                                                                                                                               GROUP BY 1 ) expo_pp
                                                                                                                      ON              expo_pp.policynumber_stg=pol.policynumber_stg
                                                                                                                                      /*Construction*/
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                                      SELECT DISTINCT pp.id_stg,
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        pbc.typecode_stg=''FrameConstruction'') THEN ''1''
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        pbc.typecode_stg=''JoistedMasonry'') THEN ''2''
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        pbc.typecode_stg=''Noncombustible'') THEN ''3''
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        pbc.typecode_stg=''Fireresistive'') THEN ''4''
                                                                                                                                                                                      ELSE ''9''
                                                                                                                                                                      END construction
                                                                                                                                                      FROM            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                      join            db_t_prod_stag.pcx_bp7building pb
                                                                                                                                                      ON              pb.branchid_stg = pp.id_stg
                                                                                                                                                      AND             pb.expirationdate_stg IS NULL
                                                                                                                                                      join            db_t_prod_stag.pctl_bp7constructiontype pbc
                                                                                                                                                      ON              pb.bp7constructiontype_stg = pbc.id_stg qualify row_number() over(PARTITION BY pp.id_stg ORDER BY pb.fixedid_stg ,coalesce(pb.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC)=1 ) cons
                                                                                                                      ON              cons.id_stg=pol.policysystemperiodid_stg
                                                                                                                                      /*Construction*/
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                                      SELECT DISTINCT pp.policynumber_stg,(
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        pbc.typecode_stg=''FrameConstruction'') THEN ''1''
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        pbc.typecode_stg=''JoistedMasonry'') THEN ''2''
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        pbc.typecode_stg=''Noncombustible'') THEN ''3''
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        pbc.typecode_stg=''Fireresistive'') THEN ''4''
                                                                                                                                                                                      ELSE ''9''
                                                                                                                                                                      END) construction_pp
                                                                                                                                                      FROM            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                      join            db_t_prod_stag.pcx_bp7building pb
                                                                                                                                                      ON              pb.branchid_stg = pp.id_stg
                                                                                                                                                      AND             pb.expirationdate_stg IS NULL
                                                                                                                                                      join            db_t_prod_stag.pctl_bp7constructiontype pbc
                                                                                                                                                      ON              pb.bp7constructiontype_stg = pbc.id_stg qualify row_number() over(PARTITION BY pp.policynumber_stg ORDER BY coalesce(pb.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC,pb.fixedid_stg)=1 ) cons_pp
                                                                                                                      ON              cons_pp.policynumber_stg=pol.policynumber_stg
                                                                                                                                      /* Burglary building*/
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                                      SELECT DISTINCT a.id_stg,
                                                                                                                                                                      d.bp7line_stg fixedid_new,
                                                                                                                                                                      ''Yes''         burglary_ind
                                                                                                                                                      FROM            db_t_prod_stag.pc_policyperiod a
                                                                                                                                                      join            db_t_prod_stag.pcx_bp7linecov d
                                                                                                                                                      ON              d.branchid_stg = a.id_stg
                                                                                                                                                      AND             d.patterncode_stg = ''BP7NamedPerils''
                                                                                                                                                      AND             d.expirationdate_stg IS NULL qualify row_number() over(PARTITION BY a.id_stg ORDER BY fixedid_new,coalesce(d.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC)=1 )bur_bp
                                                                                                                      ON              bur_bp.id_stg=pol.policysystemperiodid_stg
                                                                                                                                      /* Burglary Line*/
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                                      SELECT DISTINCT a.policynumber_stg,
                                                                                                                                                                      d.bp7line_stg fixedid_new,
                                                                                                                                                                      ''Yes''         burglary_ind
                                                                                                                                                      FROM            db_t_prod_stag.pc_policyperiod a
                                                                                                                                                      join            db_t_prod_stag.pcx_bp7linecov d
                                                                                                                                                      ON              d.branchid_stg = a.id_stg
                                                                                                                                                      AND             d.patterncode_stg = ''BP7NamedPerils''
                                                                                                                                                      AND             d.expirationdate_stg IS NULL qualify row_number() over(PARTITION BY a.policynumber_stg ORDER BY fixedid_new,coalesce(d.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC)=1 )bur_pp
                                                                                                                      ON              bur_pp.policynumber_stg=pol.policynumber_stg
                                                                                                                                      /* protection Class*/
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                               SELECT   a.id_stg,
                                                                                                                                                        b.fixedid_stg,
                                                                                                                                                        protectionclasscode_alfa_stg
                                                                                                                                               FROM     db_t_prod_stag.pc_policyperiod a
                                                                                                                                               join     db_t_prod_stag.pcx_bp7location b
                                                                                                                                               ON       b.branchid_stg = a.id_stg
                                                                                                                                               AND      b.expirationdate_stg IS NULL qualify row_number() over(PARTITION BY a.id_stg ORDER BY b.fixedid_stg,coalesce(b.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC)=1)prt_class
                                                                                                                      ON              prt_class.id_stg=pol.policysystemperiodid_stg
                                                                                                                                      /* protection Class*/
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                               SELECT   a.policynumber_stg,
                                                                                                                                                        protectionclasscode_alfa_stg protectionclasscode_alfa_stg_pp
                                                                                                                                               FROM     db_t_prod_stag.pc_policyperiod a
                                                                                                                                               join     db_t_prod_stag.pcx_bp7location b
                                                                                                                                               ON       b.branchid_stg = a.id_stg
                                                                                                                                               AND      b.expirationdate_stg IS NULL qualify row_number() over(PARTITION BY a.policynumber_stg ORDER BY coalesce(b.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC,a.id_stg DESC,b.fixedid_stg)=1)prt_class_pp
                                                                                                                      ON              prt_class_pp.policynumber_stg=pol.policynumber_stg
                                                                                                                                      /*Sprinkler*/
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                                      SELECT DISTINCT pp.id_stg,
                                                                                                                                                                      pb.fixedid_stg,
                                                                                                                                                                      bp7sprinklered_stg
                                                                                                                                                      FROM            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                      join            db_t_prod_stag.pcx_bp7building pb
                                                                                                                                                      ON              pb.branchid_stg = pp.id_stg
                                                                                                                                                      AND             pb.expirationdate_stg IS NULL qualify row_number() over(PARTITION BY pp.id_stg ORDER BY pb.fixedid_stg, coalesce(pb.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC)=1 ) spr
                                                                                                                      ON              spr.id_stg=pol.policysystemperiodid_stg
                                                                                                                                      /*Sprinkler*/
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                                      SELECT DISTINCT pp.policynumber_stg,
                                                                                                                                                                      ( bp7sprinklered_stg )bp7sprinklered_stg_pp
                                                                                                                                                      FROM            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                      join            db_t_prod_stag.pcx_bp7building pb
                                                                                                                                                      ON              pb.branchid_stg = pp.id_stg
                                                                                                                                                      AND             pb.expirationdate_stg IS NULL qualify row_number() over(PARTITION BY pp.policynumber_stg ORDER BY coalesce(pb.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC,pb.fixedid_stg)=1 ) spr_pp
                                                                                                                      ON              spr_pp.policynumber_stg=pol.policynumber_stg
                                                                                                                                      /*policy type*/
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                               SELECT   a.policynumber_stg AS policynumber,
                                                                                                                                                        a.id_stg ,
                                                                                                                                                        cast(b.isnamedperilexistonpolicy_alfa_stg AS VARCHAR(100)) AS isnamedperilexistonpolicy_alfa,
                                                                                                                                                        cast(NULL AS                                 DATE )        AS agmt_spec_dt
                                                                                                                                               FROM     db_t_prod_stag.pc_policyperiod a
                                                                                                                                               join     db_t_prod_stag.pc_policyline b
                                                                                                                                               ON       b.branchid_stg = a.id_stg
                                                                                                                                               AND      b.expirationdate_stg IS NULL
                                                                                                                                               join     db_t_prod_stag.pctl_policyperiodstatus ps
                                                                                                                                               ON       ps.id_stg = a.status_stg
                                                                                                                                               join     db_t_prod_stag.pctl_bp7policytype_alfa pt_bp7
                                                                                                                                               ON       b.bp7policytype_alfa_stg = pt_bp7.id_stg
                                                                                                                                               WHERE    isnamedperilexistonpolicy_alfa_stg IS NOT NULL qualify row_number() over(PARTITION BY a.id_stg ORDER BY coalesce(b.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp)))=1 ) policytype
                                                                                                                      ON              policytype.id_stg =pol.policysystemperiodid_stg
                                                                                                                                      /*policy type*/
                                                                                                                      left join
                                                                                                                                      (
                                                                                                                                               SELECT   a.policynumber_stg                                         AS policynumber_stg,
                                                                                                                                                        cast(b.isnamedperilexistonpolicy_alfa_stg AS VARCHAR(100)) AS isnamedperilexistonpolicy_alfa,
                                                                                                                                                        cast(NULL AS                                 DATE )        AS agmt_spec_dt
                                                                                                                                               FROM     db_t_prod_stag.pc_policyperiod a
                                                                                                                                               join     db_t_prod_stag.pc_policyline b
                                                                                                                                               ON       b.branchid_stg = a.id_stg
                                                                                                                                               AND      b.expirationdate_stg IS NULL
                                                                                                                                               join     db_t_prod_stag.pctl_policyperiodstatus ps
                                                                                                                                               ON       ps.id_stg = a.status_stg
                                                                                                                                               join     db_t_prod_stag.pctl_bp7policytype_alfa pt_bp7
                                                                                                                                               ON       b.bp7policytype_alfa_stg = pt_bp7.id_stg
                                                                                                                                               WHERE    isnamedperilexistonpolicy_alfa_stg IS NOT NULL qualify row_number() over(PARTITION BY policynumber_stg ORDER BY a.id_stg)=1 ) policytype_pol
                                                                                                                      ON              policytype_pol.policynumber_stg =pol.policynumber_stg
                                                                                                                      WHERE           tl4.name_stg NOT IN (''Awaiting submission'',
                                                                                                                                                           ''Rejected'',
                                                                                                                                                           ''Submitting'',
                                                                                                                                                           ''Pending approval'')
                                                                                                                      AND             (
                                                                                                                                                      claim.claimnumber_stg LIKE ''E%''
                                                                                                                                      OR              claim.claimnumber_stg LIKE ''C%'') )a
                                                                                      GROUP BY        a.claimnumber_stg,
                                                                                                      construction,
                                                                                                      incidentlimit_stg,
                                                                                                      sprinkler,
                                                                                                      policytype,
                                                                                                      exposureid_stg,
                                                                                                      burglary_ind_new,
                                                                                                      protectionclasscode_alfa,
                                                                                                      a.closedate,
                                                                                                      losscause_stg,
                                                                                                      a.exposurenumber,
                                                                                                      /* A.Acct500104,*/
                                                                                                      territory_new,
                                                                                                      /* A.Acct500204, A.Acct500214,A.Acct500304, A.Acct500314, */
                                                                                                      a.uwco_stg,
                                                                                                      a.lossdate_stg,
                                                                                                      a.policyidcode_stg,
                                                                                                      losscause_stg,
                                                                                                      a.state_stg,
                                                                                                      a.policy_eff_yr,
                                                                                                      a.claimant_identifier_stg,
                                                                                                      a.covrank,
                                                                                                      coverage,
                                                                                                      pci_class,
                                                                                                      pol_limit,
                                                                                                      exposure_new
                                                                                      HAVING          ((
                                                                                                                                      SUM(acct500104) <> 0
                                                                                                                      OR              SUM(acct500204) <> 0
                                                                                                                      OR              SUM(acct500214)<> 0
                                                                                                                      OR              SUM(acct500304) <> 0
                                                                                                                      OR              SUM(acct500314 )<>0))
                                                                                      OR              (
                                                                                                                      SUM(a.outstanding) <> 0 ) )b
                                                             GROUP BY claim_identifier,
                                                                      construction,
                                                                      burglary_ind_new,
                                                                      exposureid_stg,
                                                                      incidentlimit_stg,
                                                                      sprinkler,
                                                                      policytype,
                                                                      b.companynumber,
                                                                      pol_limit,
                                                                      protectionclasscode_alfa,
                                                                      exposure_new,
                                                                      lob,
                                                                      losscause_stg,
                                                                      statecode,
                                                                      territory_new,
                                                                      pci_class,
                                                                      coverage ,
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
                                                                      exposurenumber ) c
                                           ORDER BY companynumber,
                                                    statecode ) src ) );
  -- Component exp_default1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_default1 AS
  (
         SELECT sq_cc_claim.companynumber         AS companynumber,
                sq_cc_claim.lineofbusinesscode    AS lineofbusinesscode,
                sq_cc_claim.statecode             AS statecode,
                sq_cc_claim.callyear              AS callyear,
                sq_cc_claim.accountingyear        AS accountingyear,
                sq_cc_claim.expperiodyear         AS expperiodyear,
                sq_cc_claim.expperiodmonth        AS expperiodmonth,
                sq_cc_claim.expeperiodday         AS expeperiodday,
                sq_cc_claim.coveragecode          AS coveragecode,
                sq_cc_claim.classificationcode    AS classificationcode,
                sq_cc_claim.typeoflosscode        AS typeoflosscode,
                sq_cc_claim.territorycode         AS territorycode,
                sq_cc_claim.policyeffectiveyear   AS policyeffectiveyear,
                sq_cc_claim.aslob                 AS aslob,
                sq_cc_claim.policylimits          AS policylimits,
                sq_cc_claim.policytermcode        AS policytermcode,
                sq_cc_claim.expsore               AS expsore,
                sq_cc_claim.construction          AS construction,
                sq_cc_claim.burglaryoptioncode    AS burglaryoptioncode,
                sq_cc_claim.protectionclass       AS protectionclass,
                sq_cc_claim.sprinkler             AS sprinkler,
                sq_cc_claim.amountofinsurance     AS amountofinsurance,
                sq_cc_claim.typeofpolicycode      AS typeofpolicycode,
                sq_cc_claim.leadpoisoning         AS leadpoisoning,
                sq_cc_claim.claimidentifier       AS claimidentifier,
                sq_cc_claim.claimantidentifier    AS claimantidentifier,
                sq_cc_claim.wriitenexposure       AS wriitenexposure,
                sq_cc_claim.writtenpremium        AS writtenpremium,
                sq_cc_claim.paidlosses            AS paidlosses,
                sq_cc_claim.paidnumberofclaims    AS paidnumberofclaims,
                sq_cc_claim.paidalae              AS paidalae,
                sq_cc_claim.outstandinglosses     AS outstandinglosses,
                sq_cc_claim.outstandingnoofclaims AS outstandingnoofclaims,
                sq_cc_claim.outstandingalae       AS outstandingalae,
                sq_cc_claim.policynumber          AS policynumber,
                sq_cc_claim.policyperiodid        AS policyperiodid,
                sq_cc_claim.policyidentifier      AS policyidentifier,
                current_timestamp                 AS creationts,
                ''0''                               AS creationuid,
                current_timestamp                 AS updatets,
                ''0''                               AS updateuid,
                :prcs_id                          AS prcs_id,
                sq_cc_claim.exposurenumber        AS exposurenumber,
                sq_cc_claim.source_record_id
         FROM   sq_cc_claim );
  -- Component OUT_NAIIPCI_BP7_CC, Type TARGET
  INSERT INTO db_t_prod_comn.out_naiipci_bp7
              (
                          companynumber,
                          lineofbusinesscode,
                          statecode,
                          callyear,
                          accountingyear,
                          experienceperiodyear,
                          experienceperiodmonth,
                          experienceperiodday,
                          coveragecode,
                          classificationcode,
                          typeoflosscode,
                          territorycode,
                          policyeffectiveyear,
                          annualstatementlineofbusinesscode,
                          policylimits,
                          policyterm,
                          exposureidentificationcode,
                          constructioncode,
                          burglaryoptioncode,
                          protectionclasscode,
                          sprinklercode,
                          amountofinsurance,
                          typeofpolicycode,
                          leadpoisoningliabilitycode,
                          claimidentifier,
                          claimantidentifier,
                          writtenexposure,
                          writtenpremium,
                          paidlosses,
                          paidclaims,
                          paidallocatedlossadjustmentexpense,
                          outstandinglosses,
                          outstandingclaims,
                          outstandingallocatedlossadjustmentexpense,
                          policynumber,
                          policyperiodid,
                          jobnumber,
                          creationts,
                          creationuid,
                          updatets,
                          updateuid,
                          recordidentifier,
                          prcs_id
              )
  SELECT exp_default1.companynumber         AS companynumber,
         exp_default1.lineofbusinesscode    AS lineofbusinesscode,
         exp_default1.statecode             AS statecode,
         exp_default1.callyear              AS callyear,
         exp_default1.accountingyear        AS accountingyear,
         exp_default1.expperiodyear         AS experienceperiodyear,
         exp_default1.expperiodmonth        AS experienceperiodmonth,
         exp_default1.expeperiodday         AS experienceperiodday,
         exp_default1.coveragecode          AS coveragecode,
         exp_default1.classificationcode    AS classificationcode,
         exp_default1.typeoflosscode        AS typeoflosscode,
         exp_default1.territorycode         AS territorycode,
         exp_default1.policyeffectiveyear   AS policyeffectiveyear,
         exp_default1.aslob                 AS annualstatementlineofbusinesscode,
         exp_default1.policylimits          AS policylimits,
         exp_default1.policytermcode        AS policyterm,
         exp_default1.expsore               AS exposureidentificationcode,
         exp_default1.construction          AS constructioncode,
         exp_default1.burglaryoptioncode    AS burglaryoptioncode,
         exp_default1.protectionclass       AS protectionclasscode,
         exp_default1.sprinkler             AS sprinklercode,
         exp_default1.amountofinsurance     AS amountofinsurance,
         exp_default1.typeofpolicycode      AS typeofpolicycode,
         exp_default1.leadpoisoning         AS leadpoisoningliabilitycode,
         exp_default1.claimidentifier       AS claimidentifier,
         exp_default1.claimantidentifier    AS claimantidentifier,
         exp_default1.wriitenexposure       AS writtenexposure,
         exp_default1.writtenpremium        AS writtenpremium,
         exp_default1.paidlosses            AS paidlosses,
         exp_default1.paidnumberofclaims    AS paidclaims,
         exp_default1.paidalae              AS paidallocatedlossadjustmentexpense,
         exp_default1.outstandinglosses     AS outstandinglosses,
         exp_default1.outstandingnoofclaims AS outstandingclaims,
         exp_default1.outstandingalae       AS outstandingallocatedlossadjustmentexpense,
         exp_default1.policynumber          AS policynumber,
         exp_default1.policyperiodid        AS policyperiodid,
         exp_default1.policyidentifier      AS jobnumber,
         exp_default1.creationts            AS creationts,
         exp_default1.creationuid           AS creationuid,
         exp_default1.updatets              AS updatets,
         exp_default1.updateuid             AS updateuid,
         exp_default1.exposurenumber        AS recordidentifier,
         exp_default1.prcs_id               AS prcs_id
  FROM   exp_default1;
  
  -- PIPELINE END FOR 1
  -- PIPELINE START FOR 2
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
                $9  AS coveragecode,
                $10 AS classificationcode,
                $11 AS typeoflosscode,
                $12 AS territorycode,
                $13 AS policyeffectiveyear,
                $14 AS aslob,
                $15 AS policylimit,
                $16 AS policytermcode,
                $17 AS expsourenumber,
                $18 AS construction,
                $19 AS burglary,
                $20 AS protectionclass,
                $21 AS sprlinker,
                $22 AS amountofinsurance,
                $23 AS policytype,
                $24 AS leadpoisioning,
                $25 AS claimidentifier,
                $26 AS claimantidentifier,
                $27 AS writtenexposure,
                $28 AS writtenpremium,
                $29 AS paidlosses,
                $30 AS paidnumberofclaims,
                $31 AS paidalae,
                $32 AS outstandinglosses,
                $33 AS outstandingnoofclaims,
                $34 AS outnstandingalae,
                $35 AS policynumber,
                $36 AS policyperiodid,
                $37 AS policyidentifier,
                $38 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT companynumber,
                                                                  lob,
                                                                  statecode,
                                                                  callyear,
                                                                  accountingyear,
                                                                  expperiodyear,
                                                                  expperiodmonth,
                                                                  expperiodday,
                                                                  coverage_code,
                                                                  classificationcode,
                                                                  typeoflosscode,
                                                                  coalesce(territory_new,''00'') territory_new,
                                                                  policy_eff_yr,
                                                                  aslob,
                                                                  pol_limit,
                                                                  lpad(cast(policyterm AS VARCHAR(2)),2,''0'') policyterm,
                                                                  coalesce(exposure_new,''0'')                 exposure_new,
                                                                  construction,
                                                                  burglary_ind_new,
                                                                  lpad(protectionclasscode_alfa,2,''0'')protectionclasscode_alfa,
                                                                  sprinkler ,
                                                                  /* Table_Name_For_FixedID, */
                                                                  lpad(coalesce(amountofinsurnace,pol_limit),5,''0'') amountofinsurance,
                                                                  policytype,
                                                                  leadpoisioning,
                                                                  claimidentifier,
                                                                  claimantidentifier,
                                                                  CASE
                                                                                  WHEN coverage_code =''05'' THEN writtenexposure
                                                                                  ELSE 0
                                                                  END          writtenexposure,
                                                                  SUM(premium) writtenpremium,
                                                                  paidlosses,
                                                                  paidnoofclaims,
                                                                  paidalae,
                                                                  outstandinglosses,
                                                                  outstandingclaims,
                                                                  outalae,
                                                                  policynumber_stg,
                                                                  policyperiodid,
                                                                  policyidentifier
                                                  FROM            (
                                                                                  SELECT DISTINCT
                                                                                                  CASE
                                                                                                                  WHEN uw.publicid_stg=''AMI'' THEN ''0005''
                                                                                                                  WHEN uw.publicid_stg=''AMG'' THEN ''0196''
                                                                                                                  WHEN uw.publicid_stg=''AIC'' THEN ''0050''
                                                                                                                  WHEN uw.publicid_stg=''AGI'' THEN ''0318''
                                                                                                  END  AS companynumber,
                                                                                                  ''09'' AS lob,
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
                                                                                                  coverage_code,
                                                                                                  CASE
                                                                                                                  WHEN length(cast(coalesce(classi.pci_class,classi_bp.pci_class,classi2.pci_class,''89999'') AS VARCHAR(10)))= 5 THEN cast(cast(coalesce(classi.pci_class,classi_bp.pci_class,classi2.pci_class) AS VARCHAR(10))
                                                                                                                                                  ||''0'' AS VARCHAR(10))
                                                                                                                  WHEN length(cast(coalesce(classi.pci_class,classi_bp.pci_class,classi2.pci_class) AS                        VARCHAR(10)))= 4 THEN cast(''0''
                                                                                                                                                  || cast(coalesce(classi.pci_class,classi_bp.pci_class,classi2.pci_class) AS VARCHAR(10))
                                                                                                                                                  ||''0'' AS VARCHAR(10))
                                                                                                                  ELSE rpad(coalesce(coalesce(classi.pci_class,classi_bp.pci_class,classi2.pci_class),''89999''),6,''0'')
                                                                                                  END  AS classificationcode,
                                                                                                  ''00'' AS typeoflosscode,
                                                                                                  territory_new,
                                                                                                  /* Table_Name_For_FixedID,Coverable_or_PolicyLine_PartyAssetID id, */
                                                                                                  CASE
                                                                                                                  WHEN ptj.typecode_stg=''Cancellation'' THEN year(pp.cancellationdate_stg)
                                                                                                                  ELSE year(pp.periodstart_stg)
                                                                                                  END AS policy_eff_yr,
                                                                                                  CASE
                                                                                                                  WHEN psa.typecode_stg=''section1'' THEN ''051''
                                                                                                                  WHEN psa.typecode_stg=''section2'' THEN ''052''
                                                                                                                  ELSE ''051''
                                                                                                  END                                                                                    AS aslob,
                                                                                                  coalesce(lpad(cast(cast(pollimit.value1/1000 AS INTEGER) AS VARCHAR(6)),4,''0'') ,''0000'')   pol_limit,
                                                                                                  /* DB_T_PROD_STAG.pctl_bp7exposurebasis */
                                                                                                 /*  CASE
                                                                                                                  WHEN (
                                                                                                                                                  (
                                                                                                                                                                  (
                                                                                                                                                                                  pp.periodend_stg-pp.editeffectivedate_stg ) month) =0) THEN 1
                                                                                                                  ELSE cast(abs(
                                                                                                                                  CASE
                                                                                                                                                  WHEN amount_stg< 0 THEN (((pp.periodend_stg-pp.editeffectivedate_stg ) month)*-1)
                                                                                                                                                  WHEN amount_stg > 0 THEN( (pp.periodend_stg-pp.editeffectivedate_stg )month)
                                                                                                                                  END) AS INTEGER)
                                                                                                  END policyterm,
																								  */
																								CASE
																								WHEN DATEDIFF(month, pp.editeffectivedate_stg, pp.periodend_stg) = 0 THEN 1
																								ELSE ABS(DATEDIFF(month, pp.editeffectivedate_stg, pp.periodend_stg))
																								END AS policyterm,

                                                                                                  coalesce(
                                                                                                  CASE
                                                                                                                  WHEN coverage_code =''05'' THEN coalesce( exposure_class,exposure_pp)
                                                                                                                  ELSE ''0''
                                                                                                  END,''0'')                                   exposure_new,
                                                                                                  coalesce(construction,construction_pp,''0'') construction,
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(bur_class.burglary_ind,bur_bp.burglary_ind,bur_line.burglary_ind)=''Yes''
                                                                                                                                  AND             isnamedperilexistonpolicy_alfa=''1'') THEN ''1''
                                                                                                                  WHEN (
                                                                                                                                                  coalesce(bur_class.burglary_ind,bur_bp.burglary_ind,bur_line.burglary_ind) IS NULL
                                                                                                                                  AND             isnamedperilexistonpolicy_alfa IS NOT NULL) THEN ''2''
                                                                                                                  ELSE ''0''
                                                                                                  END                                                                                               burglary_ind_new,
                                                                                                  coalesce(prt_class.protectionclasscode_alfa_stg,prt_class_pp.protectionclasscode_alfa_stg_pp,''00'')protectionclasscode_alfa,
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  spr.bp7sprinklered_stg =1
                                                                                                                                  OR              spr_pp.bp7sprinklered_stg_pp =1) THEN ''1''
                                                                                                                  ELSE''2''
                                                                                                  END sprinkler ,
                                                                                                  coalesce((
                                                                                                  CASE
                                                                                                                  WHEN amount.value_stg IS NOT NULL THEN (lpad(cast(round(cast(amount.value_stg/1000.00 AS INTEGER) ,0) AS VARCHAR(6)),5,''0'') )
                                                                                                                  WHEN pet.typecode_stg =''liabilityonly_alfa''
                                                                                                                  AND             amount.value_stg IS NULL THEN lpad(cast(round(cast(coalesce(rating.propertratingexp_alfa_stg,rating_pol.propertratingexp_alfa_stg)/1000.00 AS INTEGER) ,0) AS VARCHAR(6)),5,''0'')
                                                                                                                                  /*when upper(pet.TypeCode_stg) =''PROPERTYANDLIABILITY_ALFA'' and amount.value1_new  is null and PropertRatingExp_alfa_stg is null
then coalesce(lpad(cast(cast(pollimit.value1/1000 as integer) as varchar(6)),4,''0'') ,''0000'') */
                                                                                                  END ) ,lpad(cast(round(cast(amount_pol.value_stg/1000.00 AS INTEGER) ,0) AS VARCHAR(6)),5,''0'')) amountofinsurnace,
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  isnamedperilexistonpolicy_alfa=''1'') THEN ''1''
                                                                                                                  WHEN (
                                                                                                                                                  isnamedperilexistonpolicy_alfa =0) THEN ''2''
                                                                                                                  ELSE ''9''
                                                                                                  END    policytype,
                                                                                                  0      leadpoisioning,
                                                                                                  ''0''   AS claimidentifier,
                                                                                                  ''000'' AS claimantidentifier,
                                                                                                  /*cast(
                                                                                                  CASE
                                                                                                                  WHEN amount_stg< 0 THEN (((pp.periodend_stg-pp.editeffectivedate_stg ) month)*-1)
                                                                                                                  WHEN amount_stg > 0 THEN( (pp.periodend_stg-pp.editeffectivedate_stg )month)
                                                                                                  END AS INTEGER)    writtenexposure, */
																								  CAST(
																									CASE
																										WHEN amount_stg < 0 THEN DATEDIFF(month, pp.editeffectivedate_stg, pp.periodend_stg) * -1
																										WHEN amount_stg > 0 THEN DATEDIFF(month, pp.editeffectivedate_stg, pp.periodend_stg)
																										ELSE NULL
																									END AS INTEGER
																									) AS writtenexposure,

                                                                                                  phth.amount_stg AS premium,
                                                                                                  ''0''             AS paidlosses,
                                                                                                  ''0''             AS paidnoofclaims,
                                                                                                  ''0''             AS paidalae,
                                                                                                  ''0''             AS outstandinglosses,
                                                                                                  ''0''             AS outstandingclaims,
                                                                                                  ''0''             AS outalae,
                                                                                                  /* PV.VIN_stg as VIN, */
                                                                                                  ''00'' AS exposurenumber,
                                                                                                  CASE
                                                                                                                  WHEN pp.editeffectivedate_stg >= pp.modeldate_stg
                                                                                                                  AND             pp.editeffectivedate_stg>= coalesce(pt.confirmationdate_alfa_stg, cast(''1900-01-01 00:00:00.000000'' AS timestamp)) THEN cast(pp.editeffectivedate_stg AS timestamp)
                                                                                                                  WHEN coalesce(pt.confirmationdate_alfa_stg, cast(''1900-01-01 00:00:00.000000'' AS timestamp)) >= pp.modeldate_stg THEN cast(coalesce(pt.confirmationdate_alfa_stg, cast(''1900-01-01 00:00:00.000000'' AS timestamp)) AS timestamp)
                                                                                                                  ELSE cast( pp.modeldate_stg AS timestamp)
                                                                                                  END date_filter,
                                                                                                  phth.id_stg,
                                                                                                  pp.policynumber_stg,
                                                                                                  pp.publicid_stg  AS policyperiodid,
                                                                                                  pj.jobnumber_stg AS policyidentifier
                                                                                  FROM            db_t_prod_stag.pcx_bp7transaction phth
                                                                                  join
                                                                                                  (
                                                                                                            SELECT
                                                                                                                      CASE
                                                                                                                                WHEN cost.buildingcov_stg IS NOT NULL THEN ''pcx_bp7building''
                                                                                                                                WHEN cost.buildingcond_stg IS NOT NULL THEN ''pcx_bp7buildingcond''
                                                                                                                                WHEN cost.buildingexcl_stg IS NOT NULL THEN ''pcx_bp7buildingexcl''
                                                                                                                                WHEN cost.locationcov_stg IS NOT NULL THEN ''pcx_bp7location''
                                                                                                                                WHEN cost.classificationcov_stg IS NOT NULL THEN ''pcx_bp7classification''
                                                                                                                                WHEN cost.classificationexcl_stg IS NOT NULL THEN ''pcx_bp7classificationExcl''
                                                                                                                                WHEN cost.linecoverage_stg IS NOT NULL THEN ''pc_policyline''
                                                                                                                                WHEN cost.linecond_stg IS NOT NULL THEN ''pc_policylinecond''
                                                                                                                                WHEN cost.lineexcl_stg IS NOT NULL THEN ''pc_policylineexcl''
                                                                                                                                WHEN cost.locschedcovitemcov_stg IS NOT NULL THEN ''pcx_bp7locschedcovitemcov''
                                                                                                                                WHEN cost.bldgschedcovitemcov_stg IS NOT NULL THEN ''pcx_bp7bldgschedcovitemcov''
                                                                                                                                WHEN cost.lineschedcovitemcov_stg IS NOT NULL THEN ''pcx_bp7lineschedcovitemcov''
                                                                                                                      END AS table_name_for_fixedid ,
                                                                                                                      CASE
                                                                                                                                WHEN cost.buildingcov_stg IS NOT NULL THEN bcov.building_stg
                                                                                                                                WHEN cost.buildingcond_stg IS NOT NULL THEN bcond.building_stg
                                                                                                                                WHEN cost.buildingexcl_stg IS NOT NULL THEN bexcl.building_stg
                                                                                                                                WHEN cost.locationcov_stg IS NOT NULL THEN lcov.location_stg
                                                                                                                                WHEN cost.classificationcov_stg IS NOT NULL THEN ccov.classification_stg
                                                                                                                                WHEN cost.classificationexcl_stg IS NOT NULL THEN ccexcl.classification_stg
                                                                                                                                WHEN cost.linecoverage_stg IS NOT NULL THEN licov.bp7line_stg
                                                                                                                                WHEN cost.linecond_stg IS NOT NULL THEN licond.bp7line_stg
                                                                                                                                WHEN cost.lineexcl_stg IS NOT NULL THEN liexcl.bp7line_stg
                                                                                                                                WHEN cost.locschedcovitemcov_stg IS NOT NULL THEN lscov.locschedcovitem_stg
                                                                                                                                WHEN cost.bldgschedcovitemcov_stg IS NOT NULL THEN bscov.bldgschedcovitem_stg
                                                                                                                                WHEN cost.lineschedcovitemcov_stg IS NOT NULL THEN liscov.lineschedcovitem_stg
                                                                                                                      END AS coverable_or_policyline_partyassetid,
                                                                                                                      CASE
                                                                                                                                WHEN cost.buildingcov_stg IS NOT NULL THEN bcov.patterncode_stg
                                                                                                                                WHEN cost.buildingcond_stg IS NOT NULL THEN bcond.patterncode_stg
                                                                                                                                WHEN cost.buildingexcl_stg IS NOT NULL THEN bexcl.patterncode_stg
                                                                                                                                WHEN cost.locationcov_stg IS NOT NULL THEN lcov.patterncode_stg
                                                                                                                                WHEN cost.classificationcov_stg IS NOT NULL THEN ccov.patterncode_stg
                                                                                                                                WHEN cost.classificationexcl_stg IS NOT NULL THEN ccexcl.patterncode_stg
                                                                                                                                WHEN cost.linecoverage_stg IS NOT NULL THEN licov.patterncode_stg
                                                                                                                                WHEN cost.linecond_stg IS NOT NULL THEN licond.patterncode_stg
                                                                                                                                WHEN cost.lineexcl_stg IS NOT NULL THEN liexcl.patterncode_stg
                                                                                                                                WHEN cost.locschedcovitemcov_stg IS NOT NULL THEN lscov.patterncode_stg
                                                                                                                                WHEN cost.bldgschedcovitemcov_stg IS NOT NULL THEN bscov.patterncode_stg
                                                                                                                                WHEN cost.lineschedcovitemcov_stg IS NOT NULL THEN liscov.patterncode_stg
                                                                                                                      END AS coverable_or_policyline_covpattern,
                                                                                                                      cost.id_stg,
                                                                                                                      cost.chargepattern_stg,
                                                                                                                      cp.typecode_stg AS class1 ,
                                                                                                                      sectiontype_alfa_stg
                                                                                                            FROM      db_t_prod_stag.pcx_bp7cost cost
                                                                                                                      /* Building DB_T_CORE_DM_PROD.Coverage */
                                                                                                            left join db_t_prod_stag.pcx_bp7buildingcov bcov
                                                                                                            ON        cost.buildingcov_stg = bcov.id_stg
                                                                                                                      /* Building Cond */
                                                                                                            left join db_t_prod_stag.pcx_bp7buildingcond bcond
                                                                                                            ON        cost.buildingcond_stg = bcond.id_stg
                                                                                                                      /* Building Excl */
                                                                                                            left join db_t_prod_stag.pcx_bp7buildingexcl bexcl
                                                                                                            ON        cost.buildingexcl_stg = bexcl.id_stg
                                                                                                                      /* Location DB_T_CORE_DM_PROD.Coverage */
                                                                                                            left join db_t_prod_stag.pcx_bp7locationcov lcov
                                                                                                            ON        cost.locationcov_stg = lcov.id_stg
                                                                                                                      /* Classification DB_T_CORE_DM_PROD.Coverage */
                                                                                                            left join db_t_prod_stag.pcx_bp7classificationcov ccov
                                                                                                            ON        cost.classificationcov_stg = ccov.id_stg
                                                                                                                      /* Classification Exclusion */
                                                                                                            left join db_t_prod_stag.pcx_bp7classificationexcl ccexcl
                                                                                                            ON        cost.classificationexcl_stg = ccexcl.id_stg
                                                                                                            left join db_t_prod_stag.pcx_bp7classification c
                                                                                                            ON        ccov.classification_stg = c.id_stg
                                                                                                            left join db_t_prod_stag.pctl_bp7classificationproperty cp
                                                                                                            ON        c.bp7classpropertytype_stg = cp.id_stg
                                                                                                                      /* Line DB_T_CORE_DM_PROD.Coverage */
                                                                                                            left join db_t_prod_stag.pcx_bp7linecov licov
                                                                                                            ON        cost.linecoverage_stg = licov.id_stg
                                                                                                                      /* Line Condition */
                                                                                                            left join db_t_prod_stag.pcx_bp7linecond licond
                                                                                                            ON        cost.linecond_stg = licond.id_stg
                                                                                                                      /* Line Exclusion */
                                                                                                            left join db_t_prod_stag.pcx_bp7lineexcl liexcl
                                                                                                            ON        cost.lineexcl_stg = liexcl.id_stg
                                                                                                                      /* Location Scheduled Item DB_T_CORE_DM_PROD.Coverage */
                                                                                                            left join db_t_prod_stag.pcx_bp7locschedcovitemcov lscov
                                                                                                            ON        cost.locschedcovitemcov_stg = lscov.id_stg
                                                                                                                      /* Building Scheduled Item DB_T_CORE_DM_PROD.Coverage */
                                                                                                            left join db_t_prod_stag.pcx_bp7bldgschedcovitemcov bscov
                                                                                                            ON        cost.bldgschedcovitemcov_stg = bscov.id_stg
                                                                                                                      /* Line Scheduled Item DB_T_CORE_DM_PROD.Coverage */
                                                                                                            left join db_t_prod_stag.pcx_bp7lineschedcovitemcov liscov
                                                                                                            ON        cost.lineschedcovitemcov_stg = liscov.id_stg ) expandedcosttable
                                                                                  ON              phth.bp7cost_stg = expandedcosttable.id_stg
                                                                                  join            db_t_prod_stag.pctl_chargepattern pt_ch
                                                                                  ON              expandedcosttable.chargepattern_stg = pt_ch.id_stg
                                                                                  AND             pt_ch.name_stg = ''Premium''
                                                                                  join            db_t_prod_stag.pc_policyperiod pp
                                                                                  ON              phth.branchid_stg = pp.id_stg
                                                                                  join            db_t_prod_stag.pc_job pj
                                                                                  ON              pp.jobid_stg = pj.id_stg
                                                                                  join            db_t_prod_stag.pctl_job ptj
                                                                                  ON              pj.subtype_stg = ptj.id_stg
                                                                                  join            db_t_prod_stag.pc_policyline pl
                                                                                  ON              pp.id_stg = pl.branchid_stg
                                                                                  AND             pl.expirationdate_stg IS NULL
                                                                                  left join       db_t_prod_stag.pctl_bp7whatisinsured_alfa pet
                                                                                  ON              pet.id_stg = pl.bp7whatisinsured_alfa_stg
                                                                                  join            db_t_prod_stag.pctl_bp7policytype_alfa pt_bp7
                                                                                  ON              pl.bp7policytype_alfa_stg = pt_bp7.id_stg
                                                                                  left join       db_t_prod_stag.pctl_sectiontype_alfa psa
                                                                                  ON              expandedcosttable.sectiontype_alfa_stg =psa.id_stg
                                                                                                  /* and pt_bp7.TYPECODE_stg = ''BUSINESSOWNERS'' */
                                                                                  join            db_t_prod_stag.pc_uwcompany uw
                                                                                  ON              uw.id_stg = pp.uwcompany_stg
                                                                                  join            db_t_prod_stag.pctl_jurisdiction st
                                                                                  ON              st.id_stg = pp.basestate_stg
                                                                                  join            db_t_prod_stag.pc_policyterm pt
                                                                                  ON              pt.id_stg = pp.policytermid_stg
                                                                                  left join
                                                                                                  (
                                                                                                            /*Coverage*/
                                                                                                            SELECT    pp.id_stg,
                                                                                                                      max(
                                                                                                                      CASE
                                                                                                                                WHEN pt.typecode_stg =''liabilityonly_alfa'' THEN ''05''
                                                                                                                                WHEN p_bp7e.patterncode_stg IS NOT NULL THEN ''13''
                                                                                                                                ELSE ''03''
                                                                                                                      END )coverage_code
                                                                                                            FROM      db_t_prod_stag.pc_policyperiod pp
                                                                                                            join      db_t_prod_stag.pc_policyline pl
                                                                                                            ON        pl.branchid_stg = pp.id_stg
                                                                                                            AND       pl.expirationdate_stg IS NULL
                                                                                                            join      db_t_prod_stag.pctl_bp7whatisinsured_alfa pt
                                                                                                            ON        pt.id_stg = pl.bp7whatisinsured_alfa_stg
                                                                                                            left join db_t_prod_stag.pcx_bp7buildingexcl p_bp7e
                                                                                                            ON        p_bp7e .branchid_stg = pp .id_stg
                                                                                                            AND       p_bp7e.expirationdate_stg IS NULL
                                                                                                            GROUP BY  1 ) cov
                                                                                  ON              cov.id_stg=pp.id_stg
                                                                                  left join
                                                                                                  (
                                                                                                                  /*Classification*/
                                                                                                                  SELECT DISTINCT policynumber_stg,
                                                                                                                                  a.id_stg,
                                                                                                                                  b.fixedid_stg classid,
                                                                                                                                  d.name_stg,
                                                                                                                                  coalesce(pci_class,e.code_stg) pci_class
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod a
                                                                                                                  join            db_t_prod_stag.pcx_bp7classification b
                                                                                                                  ON              b.branchid_stg = a.id_stg
                                                                                                                  AND             b.expirationdate_stg IS NULL
                                                                                                                  join            db_t_prod_stag.pctl_bp7classificationproperty c
                                                                                                                  ON              c.id_stg = b.bp7classpropertytype_stg
                                                                                                                  join            db_t_prod_stag.pctl_bp7classdescription d
                                                                                                                  ON              d.id_stg = b.bp7classdescription_stg
                                                                                                                  left join       db_t_prod_stag.pcx_bp7classcode e
                                                                                                                  ON              e.description_stg = d.description_stg
                                                                                                                  AND             e.propertytype_stg = c.name_stg
                                                                                                                  AND             e.expirationdate_stg IS NULL
                                                                                                                  left join       db_t_prod_stag.classification_values
                                                                                                                  ON              classification_values=d.name_stg qualify row_number() over(PARTITION BY policynumber_stg,a.id_stg,b.fixedid_stg ORDER BY coalesce(b.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC)=1 ) classi
                                                                                  ON              pp.id_stg =classi.id_stg
                                                                                  AND             pp.policynumber_stg =classi.policynumber_stg
                                                                                  AND
                                                                                                  CASE
                                                                                                                  WHEN table_name_for_fixedid =''pcx_bp7classification'' THEN expandedcosttable.coverable_or_policyline_partyassetid
                                                                                                  END= classi.classid
                                                                                  left join
                                                                                                  (
                                                                                                                  /*Classification - building level*/
                                                                                                                  SELECT DISTINCT policynumber_stg,
                                                                                                                                  a.id_stg,
                                                                                                                                  bp.fixedid_stg bp_id ,
                                                                                                                                  d.name_stg,
                                                                                                                                  max(coalesce(pci_class,e.code_stg)) over(PARTITION BY policynumber_stg,a.id_stg,bp.fixedid_stg ORDER BY bp.fixedid_stg DESC) pci_class
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod a
                                                                                                                  join            db_t_prod_stag.pcx_bp7classification b
                                                                                                                  ON              b.branchid_stg = a.id_stg
                                                                                                                  AND             b.expirationdate_stg IS NULL
                                                                                                                  join            db_t_prod_stag.pcx_bp7building bp
                                                                                                                  ON              bp.branchid_stg = a.id_stg
                                                                                                                  AND             bp.fixedid_stg =b.building_stg
                                                                                                                  AND             bp.expirationdate_stg IS NULL
                                                                                                                  join            db_t_prod_stag.pctl_bp7classificationproperty c
                                                                                                                  ON              c.id_stg = b.bp7classpropertytype_stg
                                                                                                                  join            db_t_prod_stag.pctl_bp7classdescription d
                                                                                                                  ON              d.id_stg = b.bp7classdescription_stg
                                                                                                                  left join       db_t_prod_stag.pcx_bp7classcode e
                                                                                                                  ON              e.description_stg = d.description_stg
                                                                                                                  AND             e.propertytype_stg = c.name_stg
                                                                                                                  AND             e.expirationdate_stg IS NULL
                                                                                                                  left join       db_t_prod_stag.classification_values
                                                                                                                  ON              classification_values=d.name_stg qualify row_number() over(PARTITION BY policynumber_stg,a.id_stg,bp.fixedid_stg ORDER BY coalesce(b.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC, coalesce(bp.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC)=1 ) classi_bp
                                                                                  ON              pp.id_stg =classi_bp.id_stg
                                                                                  AND             pp.policynumber_stg =classi_bp.policynumber_stg
                                                                                  AND
                                                                                                  CASE
                                                                                                                  WHEN table_name_for_fixedid =''pcx_bp7building'' THEN expandedcosttable.coverable_or_policyline_partyassetid
                                                                                                  END = classi_bp.bp_id
                                                                                  left join       (
                                                                                                  /*Classification POLICY*/
                                                                                                  
                                                                                  SELECT DISTINCT policynumber_stg,
                                                                                                  a.id_stg,
                                                                                                  max(coalesce(pci_class,e.code_stg)) over(PARTITION BY policynumber_stg,a.id_stg ORDER BY b.fixedid_stg DESC) pci_class
                                                                                  FROM            db_t_prod_stag.pc_policyperiod a
                                                                                  join            db_t_prod_stag.pcx_bp7classification b
                                                                                  ON              b.branchid_stg = a.id_stg
                                                                                  join            db_t_prod_stag.pctl_bp7classificationproperty c
                                                                                  ON              c.id_stg = b.bp7classpropertytype_stg
                                                                                  AND             b.expirationdate_stg IS NULL
                                                                                  join            db_t_prod_stag.pctl_bp7classdescription d
                                                                                  ON              d.id_stg = b.bp7classdescription_stg
                                                                                  left join       db_t_prod_stag.pcx_bp7classcode e
                                                                                  ON              e.description_stg = d.description_stg
                                                                                  AND             e.propertytype_stg = c.name_stg
                                                                                  AND             e.expirationdate_stg IS NULL
                                                                                  left join       db_t_prod_stag.classification_values
                                                                                  ON              classification_values=d.name_stg qualify row_number() over(PARTITION BY policynumber_stg,a.id_stg,b.fixedid_stg ORDER BY coalesce(b.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp))DESC)=1 ) classi2
                                                                                  ON              pp.id_stg =classi2.id_stg
                                                                                  AND             pp.policynumber_stg =classi2.policynumber_stg
                                                                                  left join
                                                                                                  (
                                                                                                                  /*TERRITORYCODE*/
                                                                                                                  SELECT DISTINCT
                                                                                                                                  CASE
                                                                                                                                                  WHEN g.typecode_stg =''AL''
                                                                                                                                                  AND             upper(coalesce(cityinternal_stg,''A''))=''BIRMINGHAM''
                                                                                                                                                  AND             upper(coalesce(countyinternal_stg,''A''))=''JEFFERSON'' THEN ''01''
                                                                                                                                                  WHEN g.typecode_stg =''AL''
                                                                                                                                                  AND             (
                                                                                                                                                                                  upper(coalesce(cityinternal_stg,''A''))<>''BIRMINGHAM''
                                                                                                                                                                  OR              upper(coalesce(countyinternal_stg,''A''))<>''JEFFERSON'' )THEN ''03''
                                                                                                                                                  WHEN g.typecode_stg =''GA''
                                                                                                                                                  AND             upper(coalesce(cityinternal_stg,''A''))=''ATLANTA''
                                                                                                                                                  AND             upper(coalesce(countyinternal_stg,''A''))IN (''DEKALB'',
                                                                                                                                                                                                        ''FULTON'') THEN ''02''
                                                                                                                                                  WHEN g.typecode_stg =''GA''
                                                                                                                                                  AND             (
                                                                                                                                                                                  upper(coalesce(cityinternal_stg,''A''))<>''ATLANTA''
                                                                                                                                                                  OR              upper(coalesce(countyinternal_stg,''A'')) NOT IN (''DEKALB'',
                                                                                                                                                                                                        ''FULTON'')) THEN ''03''
                                                                                                                                                  WHEN g.typecode_stg =''MS'' THEN ''01''
                                                                                                                                  END territory_new ,
                                                                                                                                  e.branchid_stg,
                                                                                                                                  policynumber_stg,
                                                                                                                                  c.code_stg,
                                                                                                                                  cityinternal_stg,
                                                                                                                                  countyinternal_stg ,
                                                                                                                                  row_number() over( PARTITION BY e.branchid_stg,policynumber_stg ORDER BY
                                                                                                                                  CASE
                                                                                                                                                  WHEN (
                                                                                                                                                                                  c.policylocation_stg = b.id_stg) THEN 1
                                                                                                                                                  ELSE 2
                                                                                                                                  END) ROWNUM,
                                                                                                                                  b.fixedid_stg
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod a
                                                                                                                  join            db_t_prod_stag.pcx_bp7building e
                                                                                                                  ON              e.branchid_stg=a.id_stg
                                                                                                                  AND             e.expirationdate_stg IS NULL
                                                                                                                  left join       db_t_prod_stag.pc_effectivedatedfields eff
                                                                                                                  ON              eff.branchid_stg = a.id_stg
                                                                                                                  AND             eff.expirationdate_stg IS NULL
                                                                                                                  left join       db_t_prod_stag.pc_policylocation b
                                                                                                                  ON              b.id_stg= eff.primarylocation_stg
                                                                                                                  AND             b.expirationdate_stg IS NULL
                                                                                                                                  /*  join DB_T_PROD_STAG.pcx_bp7location bpl
on bpl.id_stg= e.location_stg
and bpl.expirationdate_stg is null*/
                                                                                                                  join            db_t_prod_stag.pc_territorycode c
                                                                                                                  ON              c.branchid_stg = a.id_stg
                                                                                                                  AND             c.expirationdate_stg IS NULL
                                                                                                                  join            db_t_prod_stag.pctl_territorycode d
                                                                                                                  ON              c.subtype_stg=d.id_stg
                                                                                                                  join            db_t_prod_stag.pctl_jurisdiction g
                                                                                                                  ON              basestate_stg=g.id_stg
                                                                                                                  WHERE           d.typecode_stg=''BP7TerritoryCode_alfa'' qualify ROWNUM=1 )terr
                                                                                  ON              pp.id_stg=terr.branchid_stg
                                                                                  AND             pp.policynumber_stg=terr.policynumber_stg
                                                                                                  /*policy limit*/
                                                                                  left join
                                                                                                  (
                                                                                                                  SELECT DISTINCT polcov.branchid,
                                                                                                                                  value1,
                                                                                                                                  assettype
                                                                                                                  FROM            (
                                                                                                                                             SELECT     cast(''ChoiceTerm1'' AS   VARCHAR(250)) AS columnname,
                                                                                                                                                        cast(choiceterm1_stg AS VARCHAR(255)) AS val,
                                                                                                                                                        patterncode_stg,
                                                                                                                                                        cast(branchid_stg AS    INTEGER)      AS branchid,
                                                                                                                                                        cast(bp7line_stg AS     VARCHAR(255)) AS assetkey,
                                                                                                                                                        cast(''pc_policyline'' AS VARCHAR(250)) AS assettype,
                                                                                                                                                        pcx_bp7linecov.createtime_stg,
                                                                                                                                                        effectivedate_stg,
                                                                                                                                                        expirationdate_stg,
                                                                                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                                                        pcx_bp7linecov.updatetime_stg
                                                                                                                                             FROM       db_t_prod_stag.pcx_bp7linecov
                                                                                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                                                             ON         pp.id_stg = pcx_bp7linecov.branchid_stg
                                                                                                                                             WHERE      choiceterm1avl_stg = 1
                                                                                                                                             AND        expirationdate_stg IS NULL ) polcov
                                                                                                                  inner join
                                                                                                                                  (
                                                                                                                                         SELECT cast(id_stg AS VARCHAR(255)) AS id,
                                                                                                                                                policynumber_stg,
                                                                                                                                                periodstart_stg,
                                                                                                                                                periodend_stg,
                                                                                                                                                mostrecentmodel_stg,
                                                                                                                                                status_stg,
                                                                                                                                                jobid_stg,
                                                                                                                                                publicid_stg,
                                                                                                                                                createtime_stg,
                                                                                                                                                updatetime_stg,
                                                                                                                                                retired_stg
                                                                                                                                         FROM   db_t_prod_stag.pc_policyperiod) pp
                                                                                                                  ON              pp.id= polcov.branchid
                                                                                                                  left join
                                                                                                                                  (
                                                                                                                                         SELECT pcl.patternid_stg     clausepatternid,
                                                                                                                                                pcv.patternid_stg     covtermpatternid,
                                                                                                                                                pcv.columnname_stg  AS columnname,
                                                                                                                                                pcv.covtermtype_stg AS covtermtype,
                                                                                                                                                pcl.name_stg        AS clausename
                                                                                                                                         FROM   db_t_prod_stag.pc_etlclausepattern pcl
                                                                                                                                         join   db_t_prod_stag.pc_etlcovtermpattern pcv
                                                                                                                                         ON     pcl.id_stg = pcv.clausepatternid_stg
                                                                                                                                         UNION
                                                                                                                                         SELECT    pcl.patternid_stg                       clausepatternid,
                                                                                                                                                   pcv.patternid_stg                       covtermpatternid,
                                                                                                                                                   coalesce(pcv.columnname_stg,''Clause'')   columnname,
                                                                                                                                                   coalesce(pcv.covtermtype_stg, ''Clause'') covtermtype,
                                                                                                                                                   pcl.name_stg                            clausename
                                                                                                                                         FROM      db_t_prod_stag.pc_etlclausepattern pcl
                                                                                                                                         left join
                                                                                                                                                   (
                                                                                                                                                          SELECT *
                                                                                                                                                          FROM   db_t_prod_stag.pc_etlcovtermpattern
                                                                                                                                                          WHERE  name_stg NOT LIKE ''ZZ%'') pcv
                                                                                                                                         ON        pcv.clausepatternid_stg = pcl.id_stg
                                                                                                                                         WHERE     pcl.name_stg NOT LIKE ''ZZ%''
                                                                                                                                         AND       pcv.name_stg IS NULL
                                                                                                                                         AND       pcl.owningentitytype_stg IN (''BP7BusinessOwnersLine'') ) covterm
                                                                                                                  ON              covterm.clausepatternid = polcov.patterncode_stg
                                                                                                                  AND             covterm.columnname = polcov.columnname
                                                                                                                  left outer join
                                                                                                                                  (
                                                                                                                                         SELECT pcp.patternid_stg   packagepatternid,
                                                                                                                                                pcp.packagecode_stg cov_id,
                                                                                                                                                pcp.packagecode_stg name1
                                                                                                                                         FROM   db_t_prod_stag.pc_etlcovtermpackage pcp) PACKAGE
                                                                                                                  ON              PACKAGE.packagepatternid = polcov.val
                                                                                                                  left outer join
                                                                                                                                  (
                                                                                                                                             SELECT     pco.patternid_stg                      optionpatternid,
                                                                                                                                                        pco.optioncode_stg                     name1,
                                                                                                                                                        cast(pco.value_stg AS VARCHAR(255)) AS value1,
                                                                                                                                                        pcp.valuetype_stg                   AS valuetype
                                                                                                                                             FROM       db_t_prod_stag.pc_etlcovtermpattern pcp
                                                                                                                                             inner join db_t_prod_stag.pc_etlcovtermoption pco
                                                                                                                                             ON         pcp.id_stg = pco.coveragetermpatternid_stg ) optn
                                                                                                                  ON              optn.optionpatternid = polcov.val
                                                                                                                  inner join      db_t_prod_stag.pctl_policyperiodstatus pps
                                                                                                                  ON              pps.id_stg = pp.status_stg
                                                                                                                  inner join      db_t_prod_stag.pc_job pj
                                                                                                                  ON              pj.id_stg = pp.jobid_stg
                                                                                                                  inner join      db_t_prod_stag.pctl_job pcj
                                                                                                                  ON              pcj.id_stg = pj.subtype_stg
                                                                                                                  WHERE           covterm.clausename NOT LIKE''%ZZ%''
                                                                                                                  AND             pps.typecode_stg = ''Bound''
                                                                                                                  AND             covterm.covtermpatternid=''BP7EachOccLimit'') pollimit
                                                                                  ON              pollimit.branchid=pp.id_stg
                                                                                  left join
                                                                                                  (
                                                                                                           /*exposure*/
                                                                                                           SELECT   pp.id_stg,
                                                                                                                    pc.fixedid_stg,
                                                                                                                    CASE
                                                                                                                             WHEN pep.typecode_stg =''LimitofInsurance'' THEN ''3''
                                                                                                                             WHEN pep.typecode_stg =''AnnualGrossSales'' THEN ''2''
                                                                                                                             WHEN pep.typecode_stg =''AnnualPayroll'' THEN ''1''
                                                                                                                    END exposure_class
                                                                                                           FROM     db_t_prod_stag.pc_policyperiod pp
                                                                                                           join     db_t_prod_stag.pcx_bp7classification pc
                                                                                                           ON       pc.branchid_stg = pp.id_stg
                                                                                                           join     db_t_prod_stag.pctl_bp7exposurebasis pep
                                                                                                           ON       bp7exposurebasis_stg=pep.id_stg qualify row_number() over(PARTITION BY pp.id_stg,pc.fixedid_stg ORDER BY coalesce(pc.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC)=1 ) expo
                                                                                  ON              expo.id_stg=pp.id_stg
                                                                                  AND
                                                                                                  CASE
                                                                                                                  WHEN table_name_for_fixedid =''pcx_bp7classification'' THEN expandedcosttable.coverable_or_policyline_partyassetid
                                                                                                  END= expo.fixedid_stg
                                                                                  left join
                                                                                                  (
                                                                                                           /*exposure pp*/
                                                                                                           SELECT   pp.id_stg,
                                                                                                                    max(
                                                                                                                    CASE
                                                                                                                             WHEN pep.typecode_stg =''LimitofInsurance'' THEN ''3''
                                                                                                                             WHEN pep.typecode_stg =''AnnualGrossSales'' THEN ''2''
                                                                                                                             WHEN pep.typecode_stg =''AnnualPayroll'' THEN ''1''
                                                                                                                    END) exposure_pp
                                                                                                           FROM     db_t_prod_stag.pc_policyperiod pp
                                                                                                           join     db_t_prod_stag.pcx_bp7classification pc
                                                                                                           ON       pc.branchid_stg = pp.id_stg
                                                                                                           join     db_t_prod_stag.pctl_bp7exposurebasis pep
                                                                                                           ON       bp7exposurebasis_stg=pep.id_stg
                                                                                                           GROUP BY 1 ) expo_pp
                                                                                  ON              expo_pp.id_stg=pp.id_stg
                                                                                                  /*Construction*/
                                                                                  left join
                                                                                                  (
                                                                                                                  SELECT DISTINCT pp.id_stg,
                                                                                                                                  pb.fixedid_stg,
                                                                                                                                  CASE
                                                                                                                                                  WHEN (
                                                                                                                                                                                  pbc.typecode_stg=''FrameConstruction'') THEN ''1''
                                                                                                                                                  WHEN (
                                                                                                                                                                                  pbc.typecode_stg=''JoistedMasonry'') THEN ''2''
                                                                                                                                                  WHEN (
                                                                                                                                                                                  pbc.typecode_stg=''Noncombustible'') THEN ''3''
                                                                                                                                                  WHEN (
                                                                                                                                                                                  pbc.typecode_stg=''Fireresistive'') THEN ''4''
                                                                                                                                                  ELSE ''9''
                                                                                                                                  END construction
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod pp
                                                                                                                  join            db_t_prod_stag.pcx_bp7building pb
                                                                                                                  ON              pb.branchid_stg = pp.id_stg
                                                                                                                  AND             pb.expirationdate_stg IS NULL
                                                                                                                  join            db_t_prod_stag.pctl_bp7constructiontype pbc
                                                                                                                  ON              pb.bp7constructiontype_stg = pbc.id_stg qualify row_number() over(PARTITION BY pp.id_stg,pb.fixedid_stg ORDER BY coalesce(pb.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC)=1 ) cons
                                                                                  ON              cons.id_stg=pp.id_stg
                                                                                  AND
                                                                                                  CASE
                                                                                                                  WHEN table_name_for_fixedid =''pcx_bp7building'' THEN expandedcosttable.coverable_or_policyline_partyassetid
                                                                                                  END=cons.fixedid_stg
                                                                                                  /*Construction*/
                                                                                  left join
                                                                                                  (
                                                                                                                  SELECT DISTINCT pp.id_stg,(
                                                                                                                                  CASE
                                                                                                                                                  WHEN (
                                                                                                                                                                                  pbc.typecode_stg=''FrameConstruction'') THEN ''1''
                                                                                                                                                  WHEN (
                                                                                                                                                                                  pbc.typecode_stg=''JoistedMasonry'') THEN ''2''
                                                                                                                                                  WHEN (
                                                                                                                                                                                  pbc.typecode_stg=''Noncombustible'') THEN ''3''
                                                                                                                                                  WHEN (
                                                                                                                                                                                  pbc.typecode_stg=''Fireresistive'') THEN ''4''
                                                                                                                                                  ELSE ''9''
                                                                                                                                  END) construction_pp
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod pp
                                                                                                                  join            db_t_prod_stag.pcx_bp7building pb
                                                                                                                  ON              pb.branchid_stg = pp.id_stg
                                                                                                                  AND             pb.expirationdate_stg IS NULL
                                                                                                                  join            db_t_prod_stag.pctl_bp7constructiontype pbc
                                                                                                                  ON              pb.bp7constructiontype_stg = pbc.id_stg qualify row_number() over(PARTITION BY pp.id_stg ORDER BY coalesce(pb.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC, pb.fixedid_stg)=1 ) cons_pp
                                                                                  ON              cons_pp.id_stg=pp.id_stg
                                                                                                  /* Burglary building*/
                                                                                  left join
                                                                                                  (
                                                                                                                  SELECT DISTINCT a.id_stg,
                                                                                                                                  c.building_stg fixedid_new,
                                                                                                                                  ''Yes''          burglary_ind
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod a
                                                                                                                  join            db_t_prod_stag.pcx_bp7buildingcov c
                                                                                                                  ON              c.branchid_stg = a.id_stg
                                                                                                                  AND             c.patterncode_stg = ''BP7NamedPerilsBldg''
                                                                                                                  AND             c.expirationdate_stg IS NULL )bur_bp
                                                                                  ON              bur_bp.id_stg=pp.id_stg
                                                                                  AND
                                                                                                  CASE
                                                                                                                  WHEN table_name_for_fixedid =''pcx_bp7building'' THEN expandedcosttable.coverable_or_policyline_partyassetid
                                                                                                  END=bur_bp.fixedid_new
                                                                                                  /* Burglary Class*/
                                                                                  left join
                                                                                                  (
                                                                                                                  SELECT DISTINCT a.id_stg,
                                                                                                                                  b.classification_stg fixedid_new,
                                                                                                                                  ''Yes''                burglary_ind
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod a
                                                                                                                  join            db_t_prod_stag.pcx_bp7classificationcov b
                                                                                                                  ON              b.branchid_stg = a.id_stg
                                                                                                                  AND             b.patterncode_stg = ''BP7NamedPerilsBusnPrsnlProp''
                                                                                                                  AND             b.expirationdate_stg IS NULL )bur_class
                                                                                  ON              bur_class.id_stg=pp.id_stg
                                                                                  AND
                                                                                                  CASE
                                                                                                                  WHEN table_name_for_fixedid =''pcx_bp7classification'' THEN expandedcosttable.coverable_or_policyline_partyassetid
                                                                                                  END=bur_class.fixedid_new
                                                                                                  /* Burglary Line*/
                                                                                  left join
                                                                                                  (
                                                                                                                  SELECT DISTINCT a.id_stg,
                                                                                                                                  d.bp7line_stg fixedid_new,
                                                                                                                                  ''Yes''         burglary_ind
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod a
                                                                                                                  join            db_t_prod_stag.pcx_bp7linecov d
                                                                                                                  ON              d.branchid_stg = a.id_stg
                                                                                                                  AND             d.patterncode_stg = ''BP7NamedPerils''
                                                                                                                  AND             d.expirationdate_stg IS NULL )bur_line
                                                                                  ON              bur_line.id_stg=pp.id_stg
                                                                                  AND
                                                                                                  CASE
                                                                                                                  WHEN table_name_for_fixedid =''pc_policyline'' THEN expandedcosttable.coverable_or_policyline_partyassetid
                                                                                                  END=bur_line.fixedid_new
                                                                                                  /* protection Class*/
                                                                                  left join
                                                                                                  (
                                                                                                           SELECT   a.id_stg,
                                                                                                                    b.fixedid_stg,
                                                                                                                    protectionclasscode_alfa_stg
                                                                                                           FROM     db_t_prod_stag.pc_policyperiod a
                                                                                                           join     db_t_prod_stag.pcx_bp7location b
                                                                                                           ON       b.branchid_stg = a.id_stg
                                                                                                           AND      b.expirationdate_stg IS NULL qualify row_number() over(PARTITION BY a.id_stg,b.fixedid_stg ORDER BY coalesce(b.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC)=1)prt_class
                                                                                  ON              prt_class.id_stg=pp.id_stg
                                                                                  AND
                                                                                                  CASE
                                                                                                                  WHEN table_name_for_fixedid =''pcx_bp7location'' THEN expandedcosttable.coverable_or_policyline_partyassetid
                                                                                                  END=prt_class.fixedid_stg
                                                                                                  /* protection Class*/
                                                                                  left join
                                                                                                  (
                                                                                                           SELECT   a.id_stg,
                                                                                                                    protectionclasscode_alfa_stg protectionclasscode_alfa_stg_pp
                                                                                                           FROM     db_t_prod_stag.pc_policyperiod a
                                                                                                           join     db_t_prod_stag.pcx_bp7location b
                                                                                                           ON       b.branchid_stg = a.id_stg
                                                                                                           AND      b.expirationdate_stg IS NULL qualify row_number() over(PARTITION BY a.id_stg ORDER BY coalesce(b.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC,b.fixedid_stg)=1)prt_class_pp
                                                                                  ON              prt_class_pp.id_stg=pp.id_stg
                                                                                                  /*Sprinkler*/
                                                                                  left join
                                                                                                  (
                                                                                                                  SELECT DISTINCT pp.id_stg,
                                                                                                                                  pb.fixedid_stg,
                                                                                                                                  bp7sprinklered_stg
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod pp
                                                                                                                  join            db_t_prod_stag.pcx_bp7building pb
                                                                                                                  ON              pb.branchid_stg = pp.id_stg
                                                                                                                  AND             pb.expirationdate_stg IS NULL qualify row_number() over(PARTITION BY pp.id_stg,pb.fixedid_stg ORDER BY coalesce(pb.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC)=1 ) spr
                                                                                  ON              spr.id_stg=pp.id_stg
                                                                                  AND
                                                                                                  CASE
                                                                                                                  WHEN table_name_for_fixedid =''pcx_bp7building'' THEN expandedcosttable.coverable_or_policyline_partyassetid
                                                                                                  END=spr.fixedid_stg
                                                                                                  /*Sprinkler*/
                                                                                  left join
                                                                                                  (
                                                                                                                  SELECT DISTINCT pp.id_stg,
                                                                                                                                  ( bp7sprinklered_stg )bp7sprinklered_stg_pp
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod pp
                                                                                                                  join            db_t_prod_stag.pcx_bp7building pb
                                                                                                                  ON              pb.branchid_stg = pp.id_stg
                                                                                                                  AND             pb.expirationdate_stg IS NULL qualify row_number() over(PARTITION BY pp.id_stg ORDER BY coalesce(pb.expirationdate_stg,cast(''9999-12-31 23:59:59.999999'' AS timestamp) ) DESC, pb.fixedid_stg)=1 ) spr_pp
                                                                                  ON              spr_pp.id_stg=pp.id_stg
                                                                                                  /*policy type*/
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT a.policynumber_stg AS policynumber,
                                                                                                                a.id_stg ,
                                                                                                                cast(b.isnamedperilexistonpolicy_alfa_stg AS VARCHAR(100)) AS isnamedperilexistonpolicy_alfa,
                                                                                                                cast(NULL AS                                 DATE )        AS agmt_spec_dt
                                                                                                         FROM   db_t_prod_stag.pc_policyperiod a
                                                                                                         join   db_t_prod_stag.pc_policyline b
                                                                                                         ON     b.branchid_stg = a.id_stg
                                                                                                         AND    b.expirationdate_stg IS NULL
                                                                                                         join   db_t_prod_stag.pctl_policyperiodstatus ps
                                                                                                         ON     ps.id_stg = a.status_stg
                                                                                                         join   db_t_prod_stag.pctl_bp7policytype_alfa pt_bp7
                                                                                                         ON     b.bp7policytype_alfa_stg = pt_bp7.id_stg
                                                                                                         WHERE  isnamedperilexistonpolicy_alfa_stg IS NOT NULL ) policytype
                                                                                  ON              policytype.id_stg =pp.id_stg
                                                                                                  /*Amount of insurance*/
                                                                                  left join
                                                                                                  (
                                                                                                           SELECT   policynumber_stg,
                                                                                                                    id_stg,
                                                                                                                    coverabletable,
                                                                                                                    coverableid,
                                                                                                                    SUM(value_stg) value_stg
                                                                                                           FROM     (
                                                                                                                                    SELECT DISTINCT b.policynumber_stg,
                                                                                                                                                    b.id_stg,
                                                                                                                                                    coverabletable,
                                                                                                                                                    coverableid, (
                                                                                                                                                    CASE
                                                                                                                                                                    WHEN e.covtermtype_stg=''Option'' THEN f.value_stg
                                                                                                                                                                    WHEN e.columnname_stg=''DirectTerm1'' THEN c.directterm1_stg
                                                                                                                                                    END) value_stg
                                                                                                                                    FROM            (
                                                                                                                                                           SELECT id_stg,
                                                                                                                                                                  building_stg                            coverableid,
                                                                                                                                                                  cast(''pcx_bp7building'' AS VARCHAR(100)) coverabletable,
                                                                                                                                                                  patterncode_stg,
                                                                                                                                                                  choiceterm1_stg,
                                                                                                                                                                  choiceterm2_stg,
                                                                                                                                                                  choiceterm3_stg,
                                                                                                                                                                  choiceterm4_stg,
                                                                                                                                                                  choiceterm5_stg,
                                                                                                                                                                  cast(choiceterm6_stg AS VARCHAR(100))choiceterm6_stg ,
                                                                                                                                                                  cast(NULL AS            VARCHAR(100))choiceterm7_stg,
                                                                                                                                                                  directterm1_stg,
                                                                                                                                                                  branchid_stg
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7buildingcov
                                                                                                                                                           WHERE  pcx_bp7buildingcov.expirationdate_stg IS NULL
                                                                                                                                                           UNION ALL
                                                                                                                                                           SELECT pcx_bp7classificationcov.id_stg,
                                                                                                                                                                  building_stg,
                                                                                                                                                                  ''pcx_bp7building'',
                                                                                                                                                                  patterncode_stg,
                                                                                                                                                                  choiceterm1_stg,
                                                                                                                                                                  choiceterm2_stg,
                                                                                                                                                                  choiceterm3_stg,
                                                                                                                                                                  choiceterm4_stg,
                                                                                                                                                                  choiceterm5_stg,
                                                                                                                                                                  cast(NULL AS VARCHAR(100)) choiceterm6_stg,
                                                                                                                                                                  cast(NULL AS VARCHAR(100)) choiceterm7_stg,
                                                                                                                                                                  directterm1_stg,
                                                                                                                                                                  pcx_bp7classificationcov.branchid_stg
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7classificationcov
                                                                                                                                                           join   db_t_prod_stag.pcx_bp7classification
                                                                                                                                                           ON     pcx_bp7classificationcov.branchid_stg=pcx_bp7classification.branchid_stg
                                                                                                                                                           AND    pcx_bp7classificationcov.classification_stg =pcx_bp7classification.fixedid_stg
                                                                                                                                                           WHERE  pcx_bp7classificationcov.expirationdate_stg IS NULL
                                                                                                                                                           UNION ALL
                                                                                                                                                           SELECT id_stg,
                                                                                                                                                                  bp7line_stg,
                                                                                                                                                                  ''pc_Policyline'',
                                                                                                                                                                  patterncode_stg,
                                                                                                                                                                  choiceterm1_stg,
                                                                                                                                                                  choiceterm2_stg,
                                                                                                                                                                  choiceterm3_stg,
                                                                                                                                                                  choiceterm4_stg,
                                                                                                                                                                  choiceterm5_stg,
                                                                                                                                                                  choiceterm6_stg,
                                                                                                                                                                  choiceterm7_stg,
                                                                                                                                                                  directterm1_stg,
                                                                                                                                                                  branchid_stg
                                                                                                                                                           FROM   db_t_prod_stag.pcx_bp7linecov
                                                                                                                                                           WHERE  pcx_bp7linecov.expirationdate_stg IS NULL ) c
                                                                                                                                    join            db_t_prod_stag.pc_policyperiod b
                                                                                                                                    ON              c.branchid_stg=b.id_stg
                                                                                                                                    join            db_t_prod_stag.pc_etlclausepattern d
                                                                                                                                    ON              d.patternid_stg=c.patterncode_stg
                                                                                                                                    join            db_t_prod_stag.pc_etlcovtermpattern e
                                                                                                                                    ON              e.clausepatternid_stg=d.id_stg
                                                                                                                                    left join       db_t_prod_stag.pc_etlcovtermoption f
                                                                                                                                    ON              e.covtermtype_stg=''Option''
                                                                                                                                    AND             f.patternid_stg=
                                                                                                                                                    CASE
                                                                                                                                                                    WHEN e.columnname_stg = ''ChoiceTerm1'' THEN c.choiceterm1_stg
                                                                                                                                                                    WHEN e.columnname_stg = ''ChoiceTerm2'' THEN c.choiceterm2_stg
                                                                                                                                                                    WHEN e.columnname_stg = ''ChoiceTerm3'' THEN c.choiceterm3_stg
                                                                                                                                                                    WHEN e.columnname_stg = ''ChoiceTerm4'' THEN c.choiceterm4_stg
                                                                                                                                                                    WHEN e.columnname_stg = ''ChoiceTerm5'' THEN c.choiceterm5_stg
                                                                                                                                                                    WHEN e.columnname_stg = ''ChoiceTerm6'' THEN c.choiceterm6_stg
                                                                                                                                                                    WHEN e.columnname_stg = ''ChoiceTerm7'' THEN c.choiceterm7_stg
                                                                                                                                                    END
                                                                                                                                    left join       db_t_prod_stag.pc_etlcovtermpackage g
                                                                                                                                    ON              e.covtermtype_stg=''Package''
                                                                                                                                    AND             g.patternid_stg=
                                                                                                                                                    CASE
                                                                                                                                                                    WHEN e.columnname_stg = ''ChoiceTerm1'' THEN c.choiceterm1_stg
                                                                                                                                                                    WHEN e.columnname_stg = ''ChoiceTerm2'' THEN c.choiceterm2_stg
                                                                                                                                                                    WHEN e.columnname_stg = ''ChoiceTerm3'' THEN c.choiceterm3_stg
                                                                                                                                                                    WHEN e.columnname_stg = ''ChoiceTerm4'' THEN c.choiceterm4_stg
                                                                                                                                                                    WHEN e.columnname_stg = ''ChoiceTerm5'' THEN c.choiceterm5_stg
                                                                                                                                                                    WHEN e.columnname_stg = ''ChoiceTerm6'' THEN c.choiceterm6_stg
                                                                                                                                                                    WHEN e.columnname_stg = ''ChoiceTerm7'' THEN c.choiceterm7_stg
                                                                                                                                                    END
                                                                                                                                                    /* where b.PolicyNumber = ''19000336451'' */
                                                                                                                                                    /* where d.name = ''Building'' */
                                                                                                                                                    /* and e.name = ''Limit'' */
                                                                                                                                    WHERE           e.code_stg IN (''BP7BusnPrsnlPropLimit'',
                                                                                                                                                                   ''BP7BuildingLimit'')
                                                                                                                                    AND             status_stg = 9 ) a
                                                                                                           GROUP BY 1,
                                                                                                                    2,
                                                                                                                    3,
                                                                                                                    4) amount
                                                                                  ON              amount.id_stg=pp.id_stg
                                                                                  AND             cast(amount.coverableid AS INTEGER)= cast(expandedcosttable.coverable_or_policyline_partyassetid AS INTEGER)
                                                                                                  /* and Table_Name_For_FixedID=amount.coverabletable */
                                                                                                  /*Amount of insurance*/
                                                                                  left join
                                                                                                  (
                                                                                                           SELECT   policynumber_stg,
                                                                                                                    id_stg,
                                                                                                                    max(value_stg)value_stg
                                                                                                           FROM     (
                                                                                                                             SELECT   policynumber_stg,
                                                                                                                                      id_stg,
                                                                                                                                      coverabletable,
                                                                                                                                      coverableid,
                                                                                                                                      SUM(value_stg) value_stg
                                                                                                                             FROM     (
                                                                                                                                                      SELECT DISTINCT b.policynumber_stg,
                                                                                                                                                                      b.id_stg,
                                                                                                                                                                      coverabletable,
                                                                                                                                                                      coverableid, (
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN e.covtermtype_stg=''Option'' THEN f.value_stg
                                                                                                                                                                                      WHEN e.columnname_stg=''DirectTerm1'' THEN c.directterm1_stg
                                                                                                                                                                      END) value_stg
                                                                                                                                                      FROM            (
                                                                                                                                                                             SELECT id_stg,
                                                                                                                                                                                    building_stg                            coverableid,
                                                                                                                                                                                    cast(''pcx_bp7building'' AS VARCHAR(100)) coverabletable,
                                                                                                                                                                                    patterncode_stg,
                                                                                                                                                                                    choiceterm1_stg,
                                                                                                                                                                                    choiceterm2_stg,
                                                                                                                                                                                    choiceterm3_stg,
                                                                                                                                                                                    choiceterm4_stg,
                                                                                                                                                                                    choiceterm5_stg,
                                                                                                                                                                                    cast(choiceterm6_stg AS VARCHAR(100))choiceterm6_stg ,
                                                                                                                                                                                    cast(NULL AS            VARCHAR(100))choiceterm7_stg,
                                                                                                                                                                                    directterm1_stg,
                                                                                                                                                                                    branchid_stg
                                                                                                                                                                             FROM   db_t_prod_stag.pcx_bp7buildingcov
                                                                                                                                                                             WHERE  pcx_bp7buildingcov.expirationdate_stg IS NULL
                                                                                                                                                                             UNION ALL
                                                                                                                                                                             SELECT pcx_bp7classificationcov.id_stg,
                                                                                                                                                                                    building_stg,
                                                                                                                                                                                    ''pcx_bp7building'',
                                                                                                                                                                                    patterncode_stg,
                                                                                                                                                                                    choiceterm1_stg,
                                                                                                                                                                                    choiceterm2_stg,
                                                                                                                                                                                    choiceterm3_stg,
                                                                                                                                                                                    choiceterm4_stg,
                                                                                                                                                                                    choiceterm5_stg,
                                                                                                                                                                                    cast(NULL AS VARCHAR(100)) choiceterm6_stg,
                                                                                                                                                                                    cast(NULL AS VARCHAR(100)) choiceterm7_stg,
                                                                                                                                                                                    directterm1_stg,
                                                                                                                                                                                    pcx_bp7classificationcov.branchid_stg
                                                                                                                                                                             FROM   db_t_prod_stag.pcx_bp7classificationcov
                                                                                                                                                                             join   db_t_prod_stag.pcx_bp7classification
                                                                                                                                                                             ON     pcx_bp7classificationcov.branchid_stg=pcx_bp7classification.branchid_stg
                                                                                                                                                                             AND    pcx_bp7classificationcov.classification_stg =pcx_bp7classification.fixedid_stg
                                                                                                                                                                             WHERE  pcx_bp7classificationcov.expirationdate_stg IS NULL
                                                                                                                                                                             UNION ALL
                                                                                                                                                                             SELECT id_stg,
                                                                                                                                                                                    bp7line_stg,
                                                                                                                                                                                    ''pc_Policyline'',
                                                                                                                                                                                    patterncode_stg,
                                                                                                                                                                                    choiceterm1_stg,
                                                                                                                                                                                    choiceterm2_stg,
                                                                                                                                                                                    choiceterm3_stg,
                                                                                                                                                                                    choiceterm4_stg,
                                                                                                                                                                                    choiceterm5_stg,
                                                                                                                                                                                    choiceterm6_stg,
                                                                                                                                                                                    choiceterm7_stg,
                                                                                                                                                                                    directterm1_stg,
                                                                                                                                                                                    branchid_stg
                                                                                                                                                                             FROM   db_t_prod_stag.pcx_bp7linecov
                                                                                                                                                                             WHERE  pcx_bp7linecov.expirationdate_stg IS NULL ) c
                                                                                                                                                      join            db_t_prod_stag.pc_policyperiod b
                                                                                                                                                      ON              c.branchid_stg=b.id_stg
                                                                                                                                                      join            db_t_prod_stag.pc_etlclausepattern d
                                                                                                                                                      ON              d.patternid_stg=c.patterncode_stg
                                                                                                                                                      join            db_t_prod_stag.pc_etlcovtermpattern e
                                                                                                                                                      ON              e.clausepatternid_stg=d.id_stg
                                                                                                                                                      left join       db_t_prod_stag.pc_etlcovtermoption f
                                                                                                                                                      ON              e.covtermtype_stg=''Option''
                                                                                                                                                      AND             f.patternid_stg=
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN e.columnname_stg = ''ChoiceTerm1'' THEN c.choiceterm1_stg
                                                                                                                                                                                      WHEN e.columnname_stg = ''ChoiceTerm2'' THEN c.choiceterm2_stg
                                                                                                                                                                                      WHEN e.columnname_stg = ''ChoiceTerm3'' THEN c.choiceterm3_stg
                                                                                                                                                                                      WHEN e.columnname_stg = ''ChoiceTerm4'' THEN c.choiceterm4_stg
                                                                                                                                                                                      WHEN e.columnname_stg = ''ChoiceTerm5'' THEN c.choiceterm5_stg
                                                                                                                                                                                      WHEN e.columnname_stg = ''ChoiceTerm6'' THEN c.choiceterm6_stg
                                                                                                                                                                                      WHEN e.columnname_stg = ''ChoiceTerm7'' THEN c.choiceterm7_stg
                                                                                                                                                                      END
                                                                                                                                                      left join       db_t_prod_stag.pc_etlcovtermpackage g
                                                                                                                                                      ON              e.covtermtype_stg=''Package''
                                                                                                                                                      AND             g.patternid_stg=
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN e.columnname_stg = ''ChoiceTerm1'' THEN c.choiceterm1_stg
                                                                                                                                                                                      WHEN e.columnname_stg = ''ChoiceTerm2'' THEN c.choiceterm2_stg
                                                                                                                                                                                      WHEN e.columnname_stg = ''ChoiceTerm3'' THEN c.choiceterm3_stg
                                                                                                                                                                                      WHEN e.columnname_stg = ''ChoiceTerm4'' THEN c.choiceterm4_stg
                                                                                                                                                                                      WHEN e.columnname_stg = ''ChoiceTerm5'' THEN c.choiceterm5_stg
                                                                                                                                                                                      WHEN e.columnname_stg = ''ChoiceTerm6'' THEN c.choiceterm6_stg
                                                                                                                                                                                      WHEN e.columnname_stg = ''ChoiceTerm7'' THEN c.choiceterm7_stg
                                                                                                                                                                      END
                                                                                                                                                                      /* where b.PolicyNumber = ''19000336451'' */
                                                                                                                                                                      /* where d.name = ''Building'' */
                                                                                                                                                                      /* and e.name = ''Limit'' */
                                                                                                                                                      WHERE           e.code_stg IN (''BP7BusnPrsnlPropLimit'',
                                                                                                                                                                                     ''BP7BuildingLimit'')
                                                                                                                                                      AND             status_stg = 9 ) a
                                                                                                                             GROUP BY 1,
                                                                                                                                      2,
                                                                                                                                      3,
                                                                                                                                      4) b
                                                                                                           GROUP BY 1,
                                                                                                                    2 ) amount_pol
                                                                                  ON              amount_pol.id_stg=pp.id_stg
                                                                                  left join
                                                                                                  (
                                                                                                                  SELECT DISTINCT a.id_stg,
                                                                                                                                  fixedid_stg,
                                                                                                                                  propertratingexp_alfa_stg
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod a
                                                                                                                  join            db_t_prod_stag.pcx_bp7classification b
                                                                                                                  ON              a.id_stg =b.branchid_stg
                                                                                                                  WHERE           propertratingexp_alfa_stg IS NOT NULL
                                                                                                                  AND             expirationdate_stg IS NULL) rating
                                                                                  ON              pp.id_stg=rating.id_stg
                                                                                  AND             rating.fixedid_stg=
                                                                                                  CASE
                                                                                                                  WHEN table_name_for_fixedid =''pcx_bp7classification'' THEN expandedcosttable.coverable_or_policyline_partyassetid
                                                                                                  END
                                                                                  left join
                                                                                                  (
                                                                                                                  SELECT DISTINCT a.id_stg,
                                                                                                                                  max(propertratingexp_alfa_stg) propertratingexp_alfa_stg
                                                                                                                  FROM            db_t_prod_stag.pc_policyperiod a
                                                                                                                  join            db_t_prod_stag.pcx_bp7classification b
                                                                                                                  ON              a.id_stg =b.branchid_stg
                                                                                                                  WHERE           propertratingexp_alfa_stg IS NOT NULL
                                                                                                                  GROUP BY        1) rating_pol
                                                                                  ON              pp.id_stg=rating.id_stg
                                                                                  join            db_t_prod_stag.pctl_policyperiodstatus ps
                                                                                  ON              pp.status_stg=ps.id_stg
                                                                                  AND             ps.typecode_stg=''Bound''
                                                                                  AND             date_filter BETWEEN cast(:PC_BOY AS timestamp) AND             cast(:PC_EOY AS timestamp)
                                                                                  WHERE           pt_ch.name_stg = ''Premium''
                                                                                  AND             NOT EXISTS
                                                                                                  (
                                                                                                         SELECT pc_policyperiod2.policynumber_stg
                                                                                                         FROM   db_t_prod_stag.pc_policyperiod pc_policyperiod2
                                                                                                         join   db_t_prod_stag.pc_policyterm pt2
                                                                                                         ON     pt2.id_stg = pc_policyperiod2.policytermid_stg
                                                                                                         join   db_t_prod_stag.pc_policyline pl
                                                                                                         ON     pc_policyperiod2.id_stg = pl.branchid_stg
                                                                                                         AND    pl.expirationdate_stg IS NULL
                                                                                                         join   db_t_prod_stag.pc_job job2
                                                                                                         ON     job2.id_stg = pc_policyperiod2.jobid_stg
                                                                                                         join   db_t_prod_stag.pctl_job pctl_job2
                                                                                                         ON     pctl_job2.id_stg = job2.subtype_stg
                                                                                                         WHERE  pctl_job2.name_stg = ''Renewal''
                                                                                                         AND    (
                                                                                                                       pt.confirmationdate_alfa_stg > :PC_EOY
                                                                                                                OR     pt.confirmationdate_alfa_stg IS NULL)
                                                                                                         AND    pc_policyperiod2.policynumber_stg = pp.policynumber_stg
                                                                                                         AND    pc_policyperiod2.termnumber_stg = pp.termnumber_stg ) )a
                                                         GROUP BY companynumber,
                                                                  lob,
                                                                  statecode,
                                                                  callyear,
                                                                  accountingyear,
                                                                  expperiodyear,
                                                                  expperiodmonth,
                                                                  expperiodday,
                                                                  coverage_code,
                                                                  classificationcode,
                                                                  typeoflosscode,
                                                                  territory_new,
                                                                  policy_eff_yr,
                                                                  aslob,
                                                                  pol_limit,
                                                                  policyterm,
                                                                  exposure_new,
                                                                  construction,
                                                                  burglary_ind_new,
                                                                  protectionclasscode_alfa,
                                                                  sprinkler ,
                                                                  amountofinsurnace,
                                                                  policytype,
                                                                  leadpoisioning,
                                                                  claimidentifier,
                                                                  claimantidentifier,
                                                                  writtenexposure,
                                                                  paidlosses,
                                                                  paidnoofclaims,
                                                                  paidalae,
                                                                  outstandinglosses,
                                                                  outstandingclaims,
                                                                  outalae,
                                                                  policynumber_stg,
                                                                  policyperiodid,
                                                                  policyidentifier
                                                                  /* ,Table_Name_For_FixedID */
                                                         HAVING   SUM(premium)<>0 ) src ) );
  -- Component exp_hold_data, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_hold_data AS
  (
         SELECT sq_pc_policyperiod.companynumber         AS companynumber,
                sq_pc_policyperiod.lineofbusinesscode    AS lineofbusinesscode,
                sq_pc_policyperiod.statecode             AS statecode,
                sq_pc_policyperiod.callyear              AS callyear,
                sq_pc_policyperiod.accountingyear        AS accountingyear,
                sq_pc_policyperiod.expperiodyear         AS expperiodyear,
                sq_pc_policyperiod.expperiodmonth        AS expperiodmonth,
                sq_pc_policyperiod.expeperiodday         AS expeperiodday,
                sq_pc_policyperiod.coveragecode          AS coveragecode,
                sq_pc_policyperiod.classificationcode    AS classificationcode,
                sq_pc_policyperiod.typeoflosscode        AS typeoflosscode,
                sq_pc_policyperiod.territorycode         AS territorycode,
                sq_pc_policyperiod.policyeffectiveyear   AS policyeffectiveyear,
                sq_pc_policyperiod.aslob                 AS aslob,
                sq_pc_policyperiod.policylimit           AS policylimit,
                sq_pc_policyperiod.policytermcode        AS policytermcode,
                sq_pc_policyperiod.expsourenumber        AS expsourenumber,
                sq_pc_policyperiod.construction          AS construction,
                sq_pc_policyperiod.burglary              AS burglary,
                sq_pc_policyperiod.protectionclass       AS protectionclass,
                sq_pc_policyperiod.sprlinker             AS sprlinker,
                sq_pc_policyperiod.amountofinsurance     AS amountofinsurance,
                sq_pc_policyperiod.policytype            AS policytype,
                sq_pc_policyperiod.leadpoisioning        AS leadpoisioning,
                sq_pc_policyperiod.claimidentifier       AS claimidentifier,
                sq_pc_policyperiod.claimantidentifier    AS claimantidentifier,
                sq_pc_policyperiod.writtenexposure       AS writtenexposure,
                sq_pc_policyperiod.writtenpremium        AS writtenpremium,
                sq_pc_policyperiod.paidlosses            AS paidlosses,
                sq_pc_policyperiod.paidnumberofclaims    AS paidnumberofclaims,
                sq_pc_policyperiod.paidalae              AS paidalae,
                sq_pc_policyperiod.outstandinglosses     AS outstandinglosses,
                sq_pc_policyperiod.outstandingnoofclaims AS outstandingnoofclaims,
                sq_pc_policyperiod.outnstandingalae      AS outnstandingalae,
                sq_pc_policyperiod.policynumber          AS policynumber,
                sq_pc_policyperiod.policyperiodid        AS policyperiodid,
                sq_pc_policyperiod.policyidentifier      AS policyidentifier,
                current_timestamp                        AS creationts,
                ''0''                                      AS creationuid,
                current_timestamp                        AS updatets,
                ''0''                                      AS updateuid,
                :prcs_id                                 AS prcs_id,
                sq_pc_policyperiod.source_record_id
         FROM   sq_pc_policyperiod );
  -- Component OUT_NAIIPCI_BP7_PC, Type TARGET
  INSERT INTO db_t_prod_comn.out_naiipci_bp7
              (
                          companynumber,
                          lineofbusinesscode,
                          statecode,
                          callyear,
                          accountingyear,
                          experienceperiodyear,
                          experienceperiodmonth,
                          experienceperiodday,
                          coveragecode,
                          classificationcode,
                          typeoflosscode,
                          territorycode,
                          policyeffectiveyear,
                          annualstatementlineofbusinesscode,
                          policylimits,
                          policyterm,
                          exposureidentificationcode,
                          constructioncode,
                          burglaryoptioncode,
                          protectionclasscode,
                          sprinklercode,
                          amountofinsurance,
                          typeofpolicycode,
                          leadpoisoningliabilitycode,
                          claimidentifier,
                          claimantidentifier,
                          writtenexposure,
                          writtenpremium,
                          paidlosses,
                          paidclaims,
                          paidallocatedlossadjustmentexpense,
                          outstandinglosses,
                          outstandingclaims,
                          outstandingallocatedlossadjustmentexpense,
                          policynumber,
                          policyperiodid,
                          jobnumber,
                          creationts,
                          creationuid,
                          updatets,
                          updateuid,
                          prcs_id
              )
  SELECT exp_hold_data.companynumber         AS companynumber,
         exp_hold_data.lineofbusinesscode    AS lineofbusinesscode,
         exp_hold_data.statecode             AS statecode,
         exp_hold_data.callyear              AS callyear,
         exp_hold_data.accountingyear        AS accountingyear,
         exp_hold_data.expperiodyear         AS experienceperiodyear,
         exp_hold_data.expperiodmonth        AS experienceperiodmonth,
         exp_hold_data.expeperiodday         AS experienceperiodday,
         exp_hold_data.coveragecode          AS coveragecode,
         exp_hold_data.classificationcode    AS classificationcode,
         exp_hold_data.typeoflosscode        AS typeoflosscode,
         exp_hold_data.territorycode         AS territorycode,
         exp_hold_data.policyeffectiveyear   AS policyeffectiveyear,
         exp_hold_data.aslob                 AS annualstatementlineofbusinesscode,
         exp_hold_data.policylimit           AS policylimits,
         exp_hold_data.policytermcode        AS policyterm,
         exp_hold_data.expsourenumber        AS exposureidentificationcode,
         exp_hold_data.construction          AS constructioncode,
         exp_hold_data.burglary              AS burglaryoptioncode,
         exp_hold_data.protectionclass       AS protectionclasscode,
         exp_hold_data.sprlinker             AS sprinklercode,
         exp_hold_data.amountofinsurance     AS amountofinsurance,
         exp_hold_data.policytype            AS typeofpolicycode,
         exp_hold_data.leadpoisioning        AS leadpoisoningliabilitycode,
         exp_hold_data.claimidentifier       AS claimidentifier,
         exp_hold_data.claimantidentifier    AS claimantidentifier,
         exp_hold_data.writtenexposure       AS writtenexposure,
         exp_hold_data.writtenpremium        AS writtenpremium,
         exp_hold_data.paidlosses            AS paidlosses,
         exp_hold_data.paidnumberofclaims    AS paidclaims,
         exp_hold_data.paidalae              AS paidallocatedlossadjustmentexpense,
         exp_hold_data.outstandinglosses     AS outstandinglosses,
         exp_hold_data.outstandingnoofclaims AS outstandingclaims,
         exp_hold_data.outnstandingalae      AS outstandingallocatedlossadjustmentexpense,
         exp_hold_data.policynumber          AS policynumber,
         exp_hold_data.policyperiodid        AS policyperiodid,
         exp_hold_data.policyidentifier      AS jobnumber,
         exp_hold_data.creationts            AS creationts,
         exp_hold_data.creationuid           AS creationuid,
         exp_hold_data.updatets              AS updatets,
         exp_hold_data.updateuid             AS updateuid,
         exp_hold_data.prcs_id               AS prcs_id
  FROM   exp_hold_data;
  
  -- PIPELINE END FOR 2
END;
';