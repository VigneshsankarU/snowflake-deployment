-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_NAIIPCI_PA_INS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
  DECLARE
    run_id STRING;
    prcs_id INT;
    CC_BOY string;
    CC_EOY string;
    PC_EOY string;
    PC_BOY string;
    CC_EOFQ string;
  BEGIN
    run_id :=
    (
             SELECT   run_id
             FROM     control_run_id
             WHERE    UPPER(worklet_name) = UPPER(:worklet_name)
             ORDER BY insert_ts DESC limit 1);
    PRCS_ID:=
    (
             SELECT   param_value
             FROM     control_params
             WHERE    run_id = :run_id
             AND      UPPER(param_name)=''PRCS_ID''
             ORDER BY insert_ts DESC limit 1);
    CC_BOY :=
    (
             SELECT   param_value
             FROM     control_params
             WHERE    run_id = :run_id
             AND      UPPER(param_name)=''CC_BOY''
             ORDER BY insert_ts DESC limit 1);
    CC_EOY :=
    (
             SELECT   param_value
             FROM     control_params
             WHERE    run_id = :run_id
             AND      UPPER(param_name)=''CC_EOY''
             ORDER BY insert_ts DESC limit 1);
    PC_EOY :=
    (
             SELECT   param_value
             FROM     control_params
             WHERE    run_id = :run_id
             AND      UPPER(param_name)=''PC_EOY''
             ORDER BY insert_ts DESC limit 1);
    PC_BOY :=
    (
             SELECT   param_value
             FROM     control_params
             WHERE    run_id = :run_id
             AND      UPPER(param_name)=''PC_BOY''
             ORDER BY insert_ts DESC limit 1);
    CC_EOFQ := 
    (
             SELECT   param_value
             FROM     control_params
             WHERE    run_id = :run_id
             AND      UPPER(param_name)=''CC_EOFQ''
             ORDER BY insert_ts DESC limit 1);
    -- Component LKP_CLASSIFICATION, Type Prerequisite Lookup Object
    CREATE
    OR
    REPLACE TEMPORARY TABLE LKP_CLASSIFICATION AS
    (
           SELECT PolicyNumber,
                  Classification
           FROM   x /*missing table name during conversion*/ );
    -- PIPELINE START FOR 1
    -- Component SQ_cc_claim, Type SOURCE
    CREATE
    OR
    REPLACE TEMPORARY TABLE SQ_cc_claim AS
    (
           SELECT
                  /* adding column aliases to ensure proper downstream column references */
                  $1  AS Policynumber,
                  $2  AS PolicySystemPeriodID,
                  $3  AS VIN,
                  $4  AS CompanyNumber,
                  $5  AS LOB,
                  $6  AS StateCode,
                  $7  AS CallYear,
                  $8  AS AccountingYear,
                  $9  AS Exp_Yr,
                  $10 AS Exp_Mth,
                  $11 AS Exp_Day,
                  $12 AS Cvge_Code,
                  $13 AS LossCause,
                  $14 AS TerritoryCode,
                  $15 AS ZipCode,
                  $16 AS PolicyEffectiveYear,
                  $17 AS Ded_Ind_Code,
                  $18 AS Ded_Amt,
                  $19 AS Mf_Mdl_Yr,
                  $20 AS ClaimIdentifier,
                  $21 AS Paid_Loss,
                  $22 AS Paid_Claim,
                  $23 AS Paid_alae,
                  $24 AS Outstanding_Loss,
                  $25 AS Outstanding_Claim,
                  $26 AS ExposureNumber,
                  $27 AS PolicyIdentifier,
                  $28 AS City,
                  $29 AS Ann_Stmt_LOB,
                  $30 AS PolicySubType,
                  $31 AS Clm_Veh_Cnt,
                  $32 AS VIN_MAX,
                  $33 AS ClaimnatDenormiD,
                  $34 AS POlicyeffectivedate,
                  $35 AS VEH_CNT,
                  $36 AS UnverfiiedClaim,
                  $37 AS RateDriverClass_alfa,
                  $38 AS ServiceCenter,
                  $39 AS st_ind,
                  $40 AS lkp_terr_mis_NAIPCI_TERR,
                  $41 AS lkp_terr_mis_CITY,
                  $42 AS lkp_terr_mis_PostalCode,
                  $43 AS lkp_age_flag,
                  $44 AS lkp_srv_Cen_Territory,
                  $45 AS lkp_srv_Cen_County,
                  $46 AS lkp_srv_Cen_PostalCode,
                  $47 AS lkp_pol_VEH_TYPE,
                  $48 AS lkp_pol_RATEDRIVECLASS_ALFA,
                  $49 AS lkp_pol_RADIUSOFUSE_ALFA,
                  $50 AS lkp_pol_TONNAGE_ALFA,
                  $51 AS lkp_pol_PPV2_CLASSCODe,
                  $52 AS lkp_pol_Editeffectivedate,
                  $53 AS lkp_pol_Veh_Cnt,
                  $54 AS lkp_pol_rnk,
                  $55 AS lkp_VEH_CNT,
                  $56 AS lkp_def_drv_policynumber,
                  $57 AS source_record_id
           FROM   (
                           SELECT   SRC.*,
                                    row_number() over (ORDER BY 1) AS source_record_id
                           FROM     (
                                              SELECT    c_claim_src.Policynumber,
                                                        c_claim_src.PolicySystemPeriodID,
                                                        c_claim_src.VIN,
                                                        c_claim_src.COMPANYNUMBER,
                                                        c_claim_src.LOB,
                                                        c_claim_src.StateCode,
                                                        c_claim_src.CALLYEAR,
                                                        c_claim_src.ACCOUNTINGYEAR,
                                                        c_claim_src.EXP_YR,
                                                        c_claim_src.EXP_MTH,
                                                        c_claim_src.EXP_DAY,
                                                        c_claim_src.CVGE_CODE,
                                                        c_claim_src.Losscause,
                                                        c_claim_src.TERRITORYCODE,
                                                        c_claim_src.ZIPCODE,
                                                        c_claim_src.POLICYEFFECTIVEYEAR,
                                                        c_claim_src.DED_IND_CODE,
                                                        CAST(c_claim_src.DED_AMT AS VARCHAR(10))DED_AMT,
                                                        c_claim_src.Mf_Mdl_Yr,
                                                        c_claim_src.CLAIMIDENTIFIER,
                                                        c_claim_src.PaidLoss,
                                                        c_claim_src. PaidClaims,
                                                        c_claim_src.PaidALAE,
                                                        c_claim_src.OutstandingLosses,
                                                        c_claim_src.OutstandingClaims,
                                                        c_claim_src.EXPOSURENUMBER,
                                                        c_claim_src.PolicySystemPeriodID policyidentifier,
                                                        DB_T_PROD_CORE.CITY,
                                                        c_claim_src.Ann_Stmt_LOB,
                                                        c_claim_src.PolicySubtype,
                                                        c_claim_src.clm_veh_cnt,
                                                        c_claim_src.vin_max,
                                                        c_claim_src.ClaimantDenormID,
                                                        c_claim_src.EffectiveDate ,
                                                        c_claim_src.veh_cnt,
                                                        c_claim_src.UnverifiedClaimType_alfa,
                                                        c_claim_src.RateDriverClass_alfa,
                                                        c_claim_src.ServiceCenterID_alfa,
                                                        c_claim_src.st_ind,
                                                        COALESCE(lkp_terr_mismatch.NAIPCI_TERR,c_claim_src.TERRITORYCODE)            AS lkp_terr_mis_NAIPCI_TERR_PPV,
                                                        DB_T_PROD_CORE.CITY                                                          AS lkp_terr_mis_CITY,
                                                        lkp_terr_mismatch.PostalCode                                                 AS lkp_terr_mis_PostalCode,
                                                        lkp_age_flag.Age_flag                                                        AS lkp_age_flag,
                                                        lkp_service_centre.Territory                                                 AS lkp_srv_Cen_Territory,
                                                        lkp_service_centre.County                                                    AS lkp_srv_Cen_County,
                                                        lkp_service_centre.PostalCode                                                AS lkp_srv_Cen_PostalCode,
                                                        lkp_policy.VEH_TYPE                                                          AS lkp_pol_VEH_TYPE,
                                                        lkp_policy.RATEDRIVECLASS_ALFA                                               AS lkp_pol_RATEDRIVECLASS_ALFA,
                                                        lkp_policy.RADIUSOFUSE_ALFA                                                  AS lkp_pol_RADIUSOFUSE_ALFA,
                                                        lkp_policy.TONNAGE_ALFA                                                      AS lkp_pol_TONNAGE_ALFA,
                                                        lkp_policy.PPV2_CLASSCODe                                                    AS lkp_pol_PPV2_CLASSCODe,
                                                        lkp_policy.Editeffectivedate                                                 AS lkp_pol_Editeffectivedate,
                                                        lkp_policy.VEH_CNT                                                           AS lkp_pol_Veh_Cnt,
                                                        lkp_policy.rnk                                                               AS lkp_pol_rnk,
                                                        lkp_veh_count.VEH_CNT                                                        AS lkp_VEH_CNT,
                                                        CAST(COALESCE(lkp_defensive_driver. Ind, defensivedriver,''1'') AS VARCHAR(1)) AS lkp_policynumber
                                              FROM      (
                                                                 SELECT   Policynumber,
                                                                          PolicySystemPeriodID,
                                                                          VIN,
                                                                          COMPANYNUMBER,
                                                                          LOB,
                                                                          StateCode,
                                                                          CALLYEAR,
                                                                          ACCOUNTINGYEAR,
                                                                          EXP_YR,
                                                                          EXP_MTH,
                                                                          EXP_DAY,
                                                                          CVGE_CODE,
                                                                          Losscause,
                                                                          TERRITORYCODE,
                                                                          ZIPCODE,
                                                                          POLICYEFFECTIVEYEAR,
                                                                          DED_IND_CODE,
                                                                          DED_AMT,
                                                                          Mf_Mdl_Yr,
                                                                          CLAIMIDENTIFIER,
                                                                          PaidLoss,
                                                                          ROUND(
                                                                          CASE
                                                                                   WHEN(
                                                                                                     CloseDate > CAST(:CC_BOY AS TIMESTAMP)
                                                                                            AND      CloseDate < CAST(:CC_EOY AS TIMESTAMP)
                                                                                            AND      PaidLoss > 0
                                                                                            AND      CovRank = 1) THEN 1
                                                                                   ELSE 0
                                                                          END,0) AS PaidClaims,
                                                                          PaidALAE,
                                                                          OutstandingLosses OutstandingLosses,
                                                                          CASE
                                                                                   WHEN(
                                                                                                     CloseDate IS NULL
                                                                                            OR       CloseDate > CAST(:CC_EOY AS TIMESTAMP) )
                                                                                   AND      OutstandingLosses>0
                                                                                   AND      CovRank = 1 THEN 1
                                                                                   ELSE 0
                                                                          END AS OutstandingClaims,
                                                                          EXPOSURENUMBER,
                                                                          CITY,
                                                                          Ann_Stmt_LOB,
                                                                          PolicySubtype,
                                                                          clm_veh_cnt,
                                                                          VEHID vin_max,
                                                                          ClaimantDenormID,
                                                                          EffectiveDate ,
                                                                          veh_cnt,
                                                                          UnverifiedClaimType_alfa ,
                                                                          MAX(RateDriverClass_alfa) over( PARTITION BY CLAIMIDENTIFIER ORDER BY vehid DESC) RateDriverClass_alfa,
                                                                          ServiceCenterID_alfa,
                                                                          st_ind
                                                                 FROM     (
                                                                                   SELECT   PolicySystemPeriodID,
                                                                                            VIN,
                                                                                            COMPANYNUMBER,
                                                                                            LOB,
                                                                                            StateCode,
                                                                                            CALLYEAR,
                                                                                            ACCOUNTINGYEAR,
                                                                                            EXP_YR,
                                                                                            EXP_MTH,
                                                                                            EXP_DAY,
                                                                                            CVGE_CODE,
                                                                                            Losscause_new AS Losscause,
                                                                                            TERRITORYCODE,
                                                                                            ZIPCODE,
                                                                                            POLICYEFFECTIVEYEAR,
                                                                                            DED_IND_CODE,
                                                                                            DED_AMT,
                                                                                            Mf_Mdl_Yr,
                                                                                            CLAIMIDENTIFIER,
                                                                                            CovRank,
                                                                                            CloseDate,
                                                                                            SUM(Acct500104 - Acct500204 + Acct500214 - Acct500304 + Acct500314) AS PaidLoss,
                                                                                            SUM(Acct521004)                                                     AS PaidALAE,
                                                                                            CASE
                                                                                                     WHEN Cvge_Code IN (''001'',
                                                                                                                        ''003'',
                                                                                                                        ''004'',
                                                                                                                        ''006'',
                                                                                                                        ''009'',
                                                                                                                        ''201'',
                                                                                                                        ''211'',
                                                                                                                        ''259'',
                                                                                                                        ''249'') THEN SUM(OUTRESEOFQ)
                                                                                                     WHEN Cvge_Code IN (''810'',
                                                                                                                        ''812'',
                                                                                                                        ''821'',
                                                                                                                        ''845'',
                                                                                                                        ''846'',
                                                                                                                        ''848'',
                                                                                                                        ''860'') THEN SUM(OUTRESEOY)
                                                                                                     ELSE 0
                                                                                            END AS OutstandingLosses,
                                                                                            EXPOSURENUMBER,
                                                                                            Policynumber,
                                                                                            CITY,
                                                                                            CASE
                                                                                                     WHEN Cvge_Code IN (''001'',
                                                                                                                        ''003'',
                                                                                                                        ''004'',
                                                                                                                        ''006'',
                                                                                                                        ''009'',
                                                                                                                        ''201'',
                                                                                                                        ''211'',
                                                                                                                        ''259'',
                                                                                                                        ''249'')
                                                                                                     AND      PolicySubtype IN (''PPV'',
                                                                                                                                ''PPV2'') THEN ''192''
                                                                                                     WHEN Cvge_Code IN (''001'',
                                                                                                                        ''003'',
                                                                                                                        ''004'',
                                                                                                                        ''006'',
                                                                                                                        ''009'',
                                                                                                                        ''201'',
                                                                                                                        ''211'',
                                                                                                                        ''259'',
                                                                                                                        ''249'')
                                                                                                     AND      PolicySubtype =''COMMERCIAL'' THEN ''194''
                                                                                                     WHEN Cvge_Code IN (''810'',
                                                                                                                        ''812'',
                                                                                                                        ''821'',
                                                                                                                        ''845'',
                                                                                                                        ''846'',
                                                                                                                        ''848'',
                                                                                                                        ''860'')
                                                                                                     AND      PolicySubtype IN (''PPV'',
                                                                                                                                ''PPV2'') THEN ''211''
                                                                                                     WHEN Cvge_Code IN (''810'',
                                                                                                                        ''812'',
                                                                                                                        ''821'',
                                                                                                                        ''845'',
                                                                                                                        ''846'',
                                                                                                                        ''848'',
                                                                                                                        ''860'')
                                                                                                     AND      PolicySubtype =''COMMERCIAL'' THEN ''212''
                                                                                            END AS Ann_Stmt_LOB,
                                                                                            PolicySubtype,
                                                                                            clm_veh_cnt,
                                                                                            VEHID,
                                                                                            ClaimantDenormID,
                                                                                            EffectiveDate,
                                                                                            veh_cnt,
                                                                                            UnverifiedClaimType_alfa ,
                                                                                            RateDriverClass_alfa,
                                                                                            ServiceCenterID_alfa,
                                                                                            st_ind
                                                                                   FROM     (
                                                                                                            SELECT DISTINCT POL.PolicySystemPeriodID_stg AS PolicySystemPeriodID,
                                                                                                                            CC_VEHICLE.VIN_stg           AS VIN,
                                                                                                                            CASE
                                                                                                                                            WHEN UWC.TYPECODE_stg=''AMI'' THEN ''0005''
                                                                                                                                            WHEN UWC.TYPECODE_stg=''AMG'' THEN ''0196''
                                                                                                                                            WHEN UWC.TYPECODE_stg=''AIC'' THEN ''0050''
                                                                                                                                            WHEN UWC.TYPECODE_stg=''AGI'' THEN ''0318''
                                                                                                                            END  AS COMPANYNUMBER,
                                                                                                                            ''01'' AS LOB,
                                                                                                                            CASE
                                                                                                                                            WHEN (
                                                                                                                                                                            jd.TYPECODE_stg<>c_st.typecode_stg)
                                                                                                                                            OR              (
                                                                                                                                                                            c_st.typecode_stg=''AL''
                                                                                                                                                            AND             CC_PL.NAIIPCITC_alfa_stg=''18'') THEN ''policy_state''
                                                                                                                                            WHEN PolicySubtype IN (''PPV'',
                                                                                                                                                                   ''PPV2'')
                                                                                                                                            AND             UWC.TYPECODE_stg=''AIC''
                                                                                                                                                            /*eim-48316*/
                                                                                                                                            AND             jd.TYPECODE_stg=''MS''
                                                                                                                                            AND             CC_PL.NAIIPCITC_alfa_stg IN (''10'',
                                                                                                                                                                                         ''11'',
                                                                                                                                                                                         ''12'',
                                                                                                                                                                                         ''13'',
                                                                                                                                                                                         ''14'',
                                                                                                                                                                                         ''15'',
                                                                                                                                                                                         ''16'',
                                                                                                                                                                                         ''17'',
                                                                                                                                                                                         ''18'',
                                                                                                                                                                                         ''19'',
                                                                                                                                                                                         ''20'',
                                                                                                                                                                                         ''21'',
                                                                                                                                                                                         ''22'') THEN ''policy_state''
                                                                                                                                                            /*eim-48316*/
                                                                                                                                            ELSE ''claim_state''
                                                                                                                            END st_ind,
                                                                                                                            CASE
                                                                                                                                            WHEN jd.TYPECODE_stg=''AL'' THEN ''01''
                                                                                                                                            WHEN jd.TYPECODE_stg=''GA'' THEN ''10''
                                                                                                                                            WHEN jd.TYPECODE_stg=''MS'' THEN ''23''
                                                                                                                            END                                                AS StateCode,
                                                                                                                            extract(YEAR FROM CAST(:CC_EOY AS TIMESTAMP )) + 1    CALLYEAR,
                                                                                                                            extract(YEAR FROM CAST (:CC_EOY AS TIMESTAMP ))       ACCOUNTINGYEAR,
                                                                                                                            extract(YEAR FROM CLM.LOSSDATE_stg)                AS EXP_YR,
                                                                                                                            RIGHT(''00''
                                                                                                                                            || CAST(extract(MONTH FROM CLM.LOSSDATE_stg) AS VARCHAR(2)), 2) AS EXP_MTH,
                                                                                                                            RIGHT(''00''
                                                                                                                                            || CAST(extract(DAY FROM CLM.LOSSDATE_stg)AS VARCHAR(2)), 2) AS EXP_DAY,
                                                                                                                            CASE
                                                                                                                                            WHEN COV.TYPECODE_stg IN( ''PABI_alfa'' ,
                                                                                                                                                                     ''PAADD_alfa'') THEN ''001''
                                                                                                                                            WHEN COV.TYPECODE_stg= ''PAMedicalPayments_alfa'' THEN ''003''
                                                                                                                                            WHEN COV.TYPECODE_stg IN ( ''PAPropertyDamage_alfa'' ,
                                                                                                                                                                      ''PAPropertyDamageVeh_alfa'') THEN ''004''
                                                                                                                                            WHEN COV.TYPECODE_stg IN (''PASingleLimits_alfa'',
                                                                                                                                                                      ''PASingleLimitsInjury_alfa'',
                                                                                                                                                                      ''PASingleLimitsProperty_alfa'',
                                                                                                                                                                      ''PASingleLimitsVehicle_alfa'',
                                                                                                                                                                      ''PAUMSLInjury_alfa'') THEN ''006''
                                                                                                                                            WHEN COV.TYPECODE_stg IN (''PAExtendedNonOwned_alfa'',
                                                                                                                                                                      ''PALossOfIncome_alfa'',
                                                                                                                                                                      ''PAGovernment_alfa'',
                                                                                                                                                                      ''PAUninsuredMotoristSL_alfa'',
                                                                                                                                                                      ''PASplitBIPDLimits_alfa'')THEN ''009''
                                                                                                                                            WHEN jd.TYPECODE_stg IN ( ''AL'',
                                                                                                                                                                     ''MS'')
                                                                                                                                            AND             COV.TYPECODE_stg =''PAUninsuredMotoristBI_alfa'' THEN ''201''
                                                                                                                                            WHEN jd.TYPECODE_stg IN ( ''AL'',
                                                                                                                                                                     ''MS'')
                                                                                                                                            AND             COV.TYPECODE_stg IN ( ''PAUninsuredMotoristPD_alfa'',
                                                                                                                                                                                 ''PAUMSLVehicle'') THEN ''211''
                                                                                                                                            WHEN jd.TYPECODE_stg IN ( ''GA'')
                                                                                                                                            AND             (
                                                                                                                                                                            COV.TYPECODE_stg IN ( ''PAUninsuredMotoristBI_alfa'',
                                                                                                                                                                                                 ''PAUninsuredMotoristPD_alfa'',
                                                                                                                                                                                                 ''PAUMSLVehicle'')
                                                                                                                                                            OR              (
                                                                                                                                                                                            COV.TYPECODE_stg =''PAUninsuredMotoristCommon_alfa''
                                                                                                                                                                            AND             (
                                                                                                                                                                                                        COALESCE(CommonTermCovCovTerm_alfa_stg,''~'') IN (''Addedon'')
                                                                                                                                                                                            OR              COALESCE(CommonTermCovCovTerm_alfa_stg,''~'') IN (''~''))) ) THEN ''259''
                                                                                                                                            WHEN jd.TYPECODE_stg IN ( ''GA'')
                                                                                                                                            AND             COV.TYPECODE_stg =''PAUninsuredMotoristCommon_alfa''
                                                                                                                                            AND             COALESCE(CommonTermCovCovTerm_alfa_stg,''~'') IN (''Reduced'') THEN ''249''
                                                                                                                                            WHEN COV.TYPECODE_stg = ''PAComprehensiveCov'' THEN ''810''
                                                                                                                                            WHEN COV.TYPECODE_stg = ''PALeaseLoan_alfa'' THEN ''812''
                                                                                                                                            WHEN COV.TYPECODE_stg =''PAFireAndTheft_alfa'' THEN ''821''
                                                                                                                                            WHEN COV.TYPECODE_stg IN ( ''PACamperShell_alfa'',
                                                                                                                                                                      ''PACustomized_alfa'') THEN ''845''
                                                                                                                                            WHEN COV.TYPECODE_stg = ''PATowingLaborCov'' THEN ''846''
                                                                                                                                            WHEN COV.TYPECODE_stg = ''PALossOfUseCov_alfa'' THEN ''848''
                                                                                                                                            WHEN COV.TYPECODE_stg =''PACollisionCov'' THEN ''860''
                                                                                                                            END CVGE_CODE,
                                                                                                                            CASE
                                                                                                                                            WHEN UPPER(COV.TYPECODE_STG) = ''PACOMPREHENSIVECOV''
                                                                                                                                            AND             UPPER(LC.NAME_STG)LIKE ''%FIRE%'' THEN ''01''
                                                                                                                                            WHEN UPPER(COV.TYPECODE_STG) = ''PACOMPREHENSIVECOV''
                                                                                                                                            AND             UPPER(LC.NAME_STG)LIKE ''%THEFT%'' THEN ''02''
                                                                                                                                            WHEN UPPER(COV.TYPECODE_STG) = ''PACOMPREHENSIVECOV''
                                                                                                                                            AND             UPPER(LC.NAME_STG)=''GLASS BREAKAGE'' THEN ''03''
                                                                                                                                            WHEN UPPER(COV.TYPECODE_STG) = ''PACOMPREHENSIVECOV''
                                                                                                                                            AND             UPPER(LC.NAME_STG)IN (''V & MM'',
                                                                                                                                                                                  ''VANDALISM/MALICIOUS MISCHIEF'') THEN ''05''
                                                                                                                                            WHEN UPPER(COV.TYPECODE_STG) = ''PACOMPREHENSIVECOV''
                                                                                                                                            AND             UPPER(LC.NAME_STG) = ''WIND, QUAKE, HAIL, EXPLOSION, TORNADO, WATER DAMAGE'' THEN''06''
                                                                                                                                            WHEN UPPER(COV.TYPECODE_STG) = ''PACOMPREHENSIVECOV''
                                                                                                                                            AND             UPPER(LC.NAME_STG)=''FLOOD'' THEN ''07''
                                                                                                                                            WHEN UPPER(COV.TYPECODE_STG) = ''PACOMPREHENSIVECOV''
                                                                                                                                            AND             UPPER(LC.NAME_STG)=''ERS'' THEN ''08''
                                                                                                                                            WHEN UPPER(COV.TYPECODE_STG) = ''PACOMPREHENSIVECOV''
                                                                                                                                            AND             UPPER(LC.NAME_STG) NOT IN (''FIRE'',
                                                                                                                                                                                       ''THEFT (AUTO OR WTC)'',
                                                                                                                                                                                       ''GLASS BREAKAGE'',
                                                                                                                                                                                       ''V & MM'',
                                                                                                                                                                                       ''VANDALISM/MALICIOUS MISCHIEF'',
                                                                                                                                                                                       ''WIND, QUAKE, HAIL, EXPLOSION, TORNADO, WATER DAMAGE'',
                                                                                                                                                                                       ''FLOOD'',
                                                                                                                                                                                       ''ERS'') THEN ''09''
                                                                                                                                            WHEN UPPER(COV.TYPECODE_STG) IN (''PAUNINSUREDMOTORISTBI_ALFA'',
                                                                                                                                                                             ''PAUMSLINJURY_ALFA'',
                                                                                                                                                                             ''PASINGLELIMITSINJURY_ALFA'') THEN ''01''
                                                                                                                                            WHEN UPPER(COV.TYPECODE_STG) IN ( ''PAUNINSUREDMOTORISTPD_ALFA'',
                                                                                                                                                                             ''PASINGLELIMITS_ALFA'',
                                                                                                                                                                             ''PASINGLELIMITSPROPERTY_ALFA'',
                                                                                                                                                                             ''PASINGLELIMITSVEHICLE_ALFA'') THEN ''04''
                                                                                                                                            ELSE ''00''
                                                                                                                            END                                                       losscause_new,
                                                                                                                            lc.name_stg                                               Losscause,
                                                                                                                            CC_PL.NAIIPCITC_alfa_stg                                  AS TERRITORYCODE,
                                                                                                                            LEFT ((CAST(ADDR.PostalCodeDenorm_stg AS VARCHAR(40))),5) AS ZIPCODE,
                                                                                                                            EXTRACT (YEAR FROM POL.EFFECTIVEDATE_stg)                 AS POLICYEFFECTIVEYEAR,
                                                                                                                            CASE
                                                                                                                                            WHEN COV.TYPECODE_stg IN (''PAComprehensiveCov'' ,
                                                                                                                                                                      ''PAFireAndTheft_alfa'' ,
                                                                                                                                                                      ''PACollisionCov'') THEN ''D''
                                                                                                                                            ELSE ''0''
                                                                                                                            END DED_IND_CODE,
                                                                                                                            CASE
                                                                                                                                            WHEN DED_IND_CD.AMOUNT_stg IS NOT NULL
                                                                                                                                            AND             COV.TYPECODE_stg IN (''PAComprehensiveCov'',
                                                                                                                                                                                 ''PAFireAndTheft_alfa'' ,
                                                                                                                                                                                 ''PACollisionCov'') THEN CAST(CAST(DED_IND_CD.AMOUNT_stg AS INTEGER) AS VARCHAR(10))
                                                                                                                                            ELSE ''0000''
                                                                                                                            END DED_AMT,
                                                                                                                            CASE
                                                                                                                                            WHEN COV.TYPECODE_stg IN (''PAComprehensiveCov'',
                                                                                                                                                                      ''PALeaseLoan_alfa'' ,
                                                                                                                                                                      ''PAFireAndTheft_alfa'' ,
                                                                                                                                                                      ''PACamperShell_alfa'',
                                                                                                                                                                      ''PACustomized_alfa'',
                                                                                                                                                                      ''PATowingLaborCov'',
                                                                                                                                                                      ''PALossOfUseCov_alfa'',
                                                                                                                                                                      ''PACollisionCov'')
                                                                                                                                            AND             CC_VEHICLE.year_stg IS NOT NULL THEN CC_VEHICLE.year_stg
                                                                                                                            END                                                                                      AS Mf_Mdl_Yr,
                                                                                                                            CLM.CLAIMNUMBER_stg                                                                      AS CLAIMIDENTIFIER,
                                                                                                                            rank() over( PARTITION BY CLM.ClaimNumber_stg ORDER BY cov.TypeCode_stg,exps.id_stg ASC) AS CovRank,
                                                                                                                            CLM.CloseDate_stg                                                                        AS CloseDate, (
                                                                                                                            CASE
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Reserve''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP) ) THEN txli.TransactionAmount_stg
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Payment''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                            AND             lctl.name_stg = ''Deductible''
                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP) ) THEN (tx.doesNotErodeReserves_stg-1)*txli.TransactionAmount_stg
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Payment''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                            AND             lctl.name_stg = ''Deductible Refund''
                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP) ) THEN (tx.doesNotErodeReserves_stg-1)*txli.TransactionAmount_stg
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Payment''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                            AND             lctl.name_stg = ''Former Deductible''
                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP) ) THEN (tx.doesNotErodeReserves_stg-1)*txli.TransactionAmount_stg
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Payment''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                            AND             lctl.name_stg = ''Loss''
                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)) THEN (tx.doesNotErodeReserves_stg-1)*txli.TransactionAmount_stg
                                                                                                                                                            /* EIM-46305 - DV Changes */
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Payment''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                            AND             lctl.name_stg = ''Diminished Value''
                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)) THEN (tx.doesNotErodeReserves_stg-1)*txli.TransactionAmount_stg
                                                                                                                                            ELSE 0
                                                                                                                            END) AS OUTRESEOY, (
                                                                                                                            CASE
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Reserve''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOFQ AS TIMESTAMP) ) THEN txli.TransactionAmount_stg
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Payment''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                            AND             lctl.name_stg = ''Deductible''
                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOFQ AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOFQ AS TIMESTAMP) ) THEN (tx.doesNotErodeReserves_stg-1)*txli.TransactionAmount_stg
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Payment''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                            AND             lctl.name_stg = ''Deductible Refund''
                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOFQ AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOFQ AS TIMESTAMP) ) THEN (tx.doesNotErodeReserves_stg-1)*txli.TransactionAmount_stg
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Payment''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                            AND             lctl.name_stg = ''Former Deductible''
                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOFQ AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOFQ AS TIMESTAMP) ) THEN (tx.doesNotErodeReserves_stg-1)*txli.TransactionAmount_stg
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Payment''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                            AND             lctl.name_stg = ''Loss''
                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOFQ AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOFQ AS TIMESTAMP)) THEN (tx.doesNotErodeReserves_stg-1)*txli.TransactionAmount_stg
                                                                                                                                                            /* EIM-46305 - DV Changes */
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Payment''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                            AND             lctl.name_stg = ''Diminished Value''
                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOFQ AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOFQ AS TIMESTAMP)) THEN (tx.doesNotErodeReserves_stg-1)*txli.TransactionAmount_stg
                                                                                                                                            ELSE 0
                                                                                                                            END) AS OUTRESEOFQ, (
                                                                                                                            CASE
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Payment''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                            AND             lctl.name_stg = ''Deductible''
                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                            AND             (
                                                                                                                                                                                            (
                                                                                                                                                                                                        txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP))
                                                                                                                                                                            OR              (
                                                                                                                                                                                                        ch.IssueDate_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)))) THEN txli.TransactionAmount_stg
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Payment''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                            AND             lctl.name_stg = ''Deductible Refund''
                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                            AND             (
                                                                                                                                                                                            (
                                                                                                                                                                                                        txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP))
                                                                                                                                                                            OR              (
                                                                                                                                                                                                        ch.IssueDate_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)))) THEN txli.TransactionAmount_stg
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Payment''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                            AND             lctl.name_stg = ''Former Deductible''
                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                            AND             (
                                                                                                                                                                                            (
                                                                                                                                                                                                        txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP))
                                                                                                                                                                            OR              (
                                                                                                                                                                                                        ch.IssueDate_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)))) THEN txli.TransactionAmount_stg
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Payment''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                            AND             lctl.name_stg = ''Loss''
                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                            AND             (
                                                                                                                                                                                            (
                                                                                                                                                                                                        txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP))
                                                                                                                                                                            OR              (
                                                                                                                                                                                                        ch.IssueDate_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)))) THEN txli.TransactionAmount_stg
                                                                                                                                                            /* EIM-46305 - DV Changes */
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Payment''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                            AND             lctl.name_stg = ''Diminished Value''
                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                            AND             (
                                                                                                                                                                                            (
                                                                                                                                                                                                        txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP))
                                                                                                                                                                            OR              (
                                                                                                                                                                                                        ch.IssueDate_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)))) THEN txli.TransactionAmount_stg
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Recovery''
                                                                                                                                                            AND             rctl.name_stg = ''Credit to loss''
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             lctl.name_stg = ''Deductible''
                                                                                                                                                            AND             txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)) THEN txli.TransactionAmount_stg*-1
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Recovery''
                                                                                                                                                            AND             rctl.name_stg = ''Credit to loss''
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             lctl.name_stg = ''Loss''
                                                                                                                                                            AND             txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)) THEN txli.TransactionAmount_stg*-1
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Recovery''
                                                                                                                                                            AND             rctl.name_stg = ''Credit to loss''
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             lctl.name_stg = ''Recovery''
                                                                                                                                                            AND             txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)) THEN txli.TransactionAmount_stg*-1
                                                                                                                                            ELSE 0
                                                                                                                            END) AS Acct500104, (
                                                                                                                            CASE
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Recovery''
                                                                                                                                                            AND             rctl.name_stg = ''Salvage''
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             lctl.name_stg = ''Recovery''
                                                                                                                                                            AND             txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)) THEN txli.TransactionAmount_stg
                                                                                                                                            ELSE 0
                                                                                                                            END) AS Acct500204, (
                                                                                                                            CASE
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Payment''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Expense''
                                                                                                                                                            AND             cttl.name_stg = ''Expense''
                                                                                                                                                            AND             lctl.name_stg = ''Salvage Expense''
                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                            AND             (
                                                                                                                                                                                            (
                                                                                                                                                                                                        txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP))
                                                                                                                                                                            OR              (
                                                                                                                                                                                                        ch.IssueDate_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)))) THEN txli.TransactionAmount_stg
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Recovery''
                                                                                                                                                            AND             rctl.name_stg IN (''Credit to expense'' ,
                                                                                                                                                                                              ''Subrogation'')
                                                                                                                                                            AND             cctl.name_stg=''Expense''
                                                                                                                                                            AND             cttl.name_stg = ''Expense''
                                                                                                                                                            AND             lctl.name_stg = ''Salvage Expense''
                                                                                                                                                            AND             txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)) THEN txli.TransactionAmount_stg*-1
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Recovery''
                                                                                                                                                            AND             rctl.name_stg = ''Salvage''
                                                                                                                                                            AND             cctl.name_stg=''Expense''
                                                                                                                                                            AND             cttl.name_stg = ''Expense''
                                                                                                                                                            AND             lctl.name_stg = ''Salvage Expense''
                                                                                                                                                            AND             txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)) THEN txli.TransactionAmount_stg*-1
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Recovery''
                                                                                                                                                            AND             rctl.name_stg = ''Salvage''
                                                                                                                                                            AND             cctl.name_stg=''Expense''
                                                                                                                                                            AND             cttl.name_stg = ''Expense''
                                                                                                                                                            AND             lctl.name_stg = ''Recovery''
                                                                                                                                                            AND             txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)) THEN txli.TransactionAmount_stg*-1
                                                                                                                                            ELSE 0
                                                                                                                            END) AS Acct500214, (
                                                                                                                            CASE
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Recovery''
                                                                                                                                                            AND             rctl.name_stg = ''Subrogation''
                                                                                                                                                            AND             cctl.name_stg=''Loss''
                                                                                                                                                            AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                            AND             lctl.name_stg = ''Recovery''
                                                                                                                                                            AND             txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)) THEN txli.TransactionAmount_stg
                                                                                                                                            ELSE 0
                                                                                                                            END) AS Acct500304, (
                                                                                                                            CASE
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Payment''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Expense''
                                                                                                                                                            AND             cttl.name_stg = ''Expense''
                                                                                                                                                            AND             lctl.name_stg = ''Subrogation Expense''
                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                            AND             (
                                                                                                                                                                                            (
                                                                                                                                                                                                        txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP))
                                                                                                                                                                            OR              (
                                                                                                                                                                                                        ch.IssueDate_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)))) THEN txli.TransactionAmount_stg
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Recovery''
                                                                                                                                                            AND             rctl.name_stg IN (''Credit to expense'' ,
                                                                                                                                                                                              ''Subrogation'')
                                                                                                                                                            AND             cctl.name_stg=''Expense''
                                                                                                                                                            AND             cttl.name_stg = ''Expense''
                                                                                                                                                            AND             lctl.name_stg = ''Subrogation Expense''
                                                                                                                                                            AND             txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)) THEN txli.TransactionAmount_stg*-1
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Recovery''
                                                                                                                                                            AND             rctl.name_stg = ''Subrogation''
                                                                                                                                                            AND             cctl.name_stg=''Expense''
                                                                                                                                                            AND             cttl.name_stg = ''Expense''
                                                                                                                                                            AND             lctl.name_stg = ''Subrogation Expense''
                                                                                                                                                            AND             txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)) THEN txli.TransactionAmount_stg*-1
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Recovery''
                                                                                                                                                            AND             rctl.name_stg = ''Subrogation''
                                                                                                                                                            AND             cctl.name_stg=''Expense''
                                                                                                                                                            AND             cttl.name_stg = ''Expense''
                                                                                                                                                            AND             lctl.name_stg = ''Recovery''
                                                                                                                                                            AND             txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)) THEN txli.TransactionAmount_stg*-1
                                                                                                                                            ELSE 0
                                                                                                                            END) AS Acct500314, (
                                                                                                                            CASE
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Payment''
                                                                                                                                                            AND             rctl.name_stg IS NULL
                                                                                                                                                            AND             cctl.name_stg=''Expense''
                                                                                                                                                            AND             cttl.name_stg = ''Expense''
                                                                                                                                                            AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                            AND             lctl.name_stg = ''Legal - Defense''
                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                            AND             (
                                                                                                                                                                                            (
                                                                                                                                                                                                        txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP))
                                                                                                                                                                            OR              (
                                                                                                                                                                                                        ch.IssueDate_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                                                            AND             ch.IssueDate_stg <= CAST(:CC_EOY AS TIMESTAMP)
                                                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)))) THEN txli.TransactionAmount_stg
                                                                                                                                            WHEN (
                                                                                                                                                                            txtl.name_stg=''Recovery''
                                                                                                                                                            AND             rctl.name_stg =''Credit to expense''
                                                                                                                                                            AND             cctl.name_stg=''Expense''
                                                                                                                                                            AND             cttl.name_stg = ''Expense''
                                                                                                                                                            AND             lctl.name_stg = ''Legal - Defense''
                                                                                                                                                            AND             txli.CreateTime_stg >= CAST(:CC_BOY AS TIMESTAMP)
                                                                                                                                                            AND             txli.CreateTime_stg <= CAST(:CC_EOY AS TIMESTAMP)) THEN txli.TransactionAmount_stg*-1
                                                                                                                                            ELSE 0
                                                                                                                            END)                 AS Acct521004,
                                                                                                                            EXPS.CLAIMORDER_stg  AS EXPOSURENUMBER,
                                                                                                                            POL.Policynumber_stg AS Policynumber,
                                                                                                                            ADDR.CITY_stg        AS CITY,
                                                                                                                            psttl.TYPECODE_stg   AS PolicySubtype,
                                                                                                                            0                    AS clm_veh_cnt,
                                                                                                                            VEH_VIN.VEHID,
                                                                                                                            LEFT ((CAST(exps.ClaimantDenormID_stg AS VARCHAR(40))),3)    ClaimantDenormID,
                                                                                                                            LOSSDATE_stg                                                 EffectiveDate,
                                                                                                                            pol.TotalVehicles_stg                                        veh_cnt,
                                                                                                                            UnverifiedClaimType_alfa_stg                              AS UnverifiedClaimType_alfa ,
                                                                                                                            txli.id_stg,
                                                                                                                            CASE
                                                                                                                                            WHEN(
                                                                                                                                                                            tl2.name_stg LIKE ''%Insured%'') THEN RateDriverClass_alfa_stg
                                                                                                                                            ELSE NULL
                                                                                                                            END RateDriverClass_alfa,
                                                                                                                            CASE
                                                                                                                                            WHEN (
                                                                                                                                                                            UnverifiedClaimType_alfa IN (10003,10004)) THEN RIGHT(''0000000''
                                                                                                                                                                            || CAST(pol.ServiceCenterID_alfa_stg AS VARCHAR(7)),7)
                                                                                                                            END ServiceCenterID_alfa
                                                                                                            FROM            DB_T_PROD_STAG.CC_CLAIM CLM
                                                                                                            JOIN            DB_T_PROD_STAG.CC_POLICY POL
                                                                                                            ON              CLM.POLICYID_stg=POL.ID_stg
                                                                                                                            /* and pol.Verified_stg = 1        */
                                                                                                            AND             ((
                                                                                                                                                            CAST(CLM.ReportedDate_stg AS TIMESTAMP) >= CAST(CAST(:CC_EOY AS TIMESTAMP) AS TIMESTAMP) - interval ''5 year''
                                                                                                                                            AND             CAST(CLM.ReportedDate_stg AS TIMESTAMP) <= CAST(:CC_EOY AS TIMESTAMP))
                                                                                                                            AND             (
                                                                                                                                                            CAST(CLM.LossDate_stg AS TIMESTAMP) >= CAST(CAST(:CC_EOY AS TIMESTAMP) AS TIMESTAMP) - interval ''5 year''
                                                                                                                                            AND             CAST(CLM.LossDate_stg AS TIMESTAMP) <= CAST(:CC_EOY AS TIMESTAMP)))
                                                                                                            AND             clm.ClaimNumber_stg LIKE ''A%''
                                                                                                            JOIN            DB_T_PROD_STAG.cctl_claimstate sts
                                                                                                            ON              CLM.State_stg= sts.id_stg
                                                                                                            AND             sts.name_stg <> ''Draft''
                                                                                                            JOIN            DB_T_PROD_STAG.CCTL_UNDERWRITINGCOMPANYTYPE UWC
                                                                                                            ON              POL.UNDERWRITINGCO_stg=UWC.ID_stg
                                                                                                            JOIN            DB_T_PROD_STAG.CCTL_JURISDICTION JD
                                                                                                            ON              POL.BASESATE_ALFA_stg=JD.ID_stg
                                                                                                            JOIN            DB_T_PROD_STAG.cctl_policytype pttl
                                                                                                            ON              pttl.id_stg = pol.PolicyType_stg
                                                                                                            JOIN            DB_T_PROD_STAG.cctl_policysubtype_alfa psttl
                                                                                                            ON              psttl.ID_stg = pol.PolicySubType_alfa_stg
                                                                                                            JOIN            DB_T_PROD_STAG.CC_INCIDENT INC
                                                                                                            ON              CLM.ID_stg=INC.CLAIMID_stg
                                                                                                            JOIN            DB_T_PROD_STAG.CC_EXPOSURE EXPS
                                                                                                            ON              INC.ID_stg=EXPS.INCIDENTId_stg
                                                                                                            AND             exps.Retired_stg = 0
                                                                                                            JOIN            DB_T_PROD_STAG.CCTL_COVERAGESUBTYPE COV
                                                                                                            ON              COV.ID_stg=EXPS.COVERAGESUBTYPE_stg
                                                                                                            JOIN            DB_T_PROD_STAG.cctl_coveragetype tl3
                                                                                                            ON              tl3.id_stg = exps.PrimaryCoverage_stg
                                                                                                            JOIN            DB_T_PROD_STAG.cc_transaction tx
                                                                                                            ON              tx.ExposureID_stg = exps.ID_stg
                                                                                                            AND             tx.Retired_stg = 0
                                                                                                            JOIN            DB_T_PROD_STAG.cc_transactionlineitem txli
                                                                                                            ON              txli.TransactionID_stg = tx.ID_stg
                                                                                                            JOIN            DB_T_PROD_STAG.cctl_transactionstatus tl4
                                                                                                            ON              tl4.ID_stg = tx.Status_stg
                                                                                                            LEFT JOIN       DB_T_PROD_STAG.cctl_transaction txtl
                                                                                                            ON              txtl.ID_stg = tx.Subtype_stg
                                                                                                            LEFT JOIN       DB_T_PROD_STAG.cctl_linecategory lctl
                                                                                                            ON              lctl.ID_stg = txli.LineCategory_stg
                                                                                                            LEFT JOIN       DB_T_PROD_STAG.cctl_recoverycategory rctl
                                                                                                            ON              rctl.id_stg = tx.RecoveryCategory_stg
                                                                                                            LEFT JOIN       DB_T_PROD_STAG.cctl_costcategory cctl
                                                                                                            ON              cctl.id_stg = tx.costcategory_stg
                                                                                                            LEFT JOIN       DB_T_PROD_STAG.cctl_costtype cttl
                                                                                                            ON              cttl.id_stg = tx.CostType_stg
                                                                                                            LEFT JOIN       DB_T_PROD_STAG.cc_catastrophe cat
                                                                                                            ON              cat.ID_stg = CLM.CatastropheID_stg
                                                                                                            LEFT JOIN       DB_T_PROD_STAG.cc_check ch
                                                                                                            ON              ch.ID_stg = tx.CheckID_stg
                                                                                                            LEFT JOIN       DB_T_PROD_STAG.cctl_paymentmethod pmtl
                                                                                                            ON              pmtl.ID_stg = ch.PaymentMethod_stg
                                                                                                            LEFT JOIN       DB_T_PROD_STAG.CC_VEHICLE CC_VEHICLE
                                                                                                            ON              INC.VEHICLEID_stg=CC_VEHICLE.ID_stg
                                                                                                            JOIN            DB_T_PROD_STAG.cctl_losscause lc
                                                                                                            ON              lc.ID_stg = clm.LossCause_stg
                                                                                                            LEFT JOIN       DB_T_PROD_STAG.cctl_losspartytype tl2
                                                                                                            ON              tl2.ID_stg =inc.LossParty_stg
                                                                                                            LEFT JOIN
                                                                                                                            (
                                                                                                                                   SELECT POLICYID_stg,
                                                                                                                                          NAIIPCITC_alfa_stg,
                                                                                                                                          ADDRESSID_stg
                                                                                                                                   FROM   (
                                                                                                                                                          SELECT DISTINCT POLICYID_stg,
                                                                                                                                                                          PrimaryLocation_stg,
                                                                                                                                                                          NAIIPCITC_alfa_stg,
                                                                                                                                                                          ADDRESSID_stg,
                                                                                                                                                                          ROW_NUMBER () over ( PARTITION BY policyid_stg ORDER BY PrimaryLocation_stg DESC,ADDRESSID_stg ,id_stg DESC)AS RNK
                                                                                                                                                          FROM            CC_POLICYLOCATION)pl
                                                                                                                                   WHERE  RNK=1)CC_PL
                                                                                                            ON              POL.ID_stg=CC_PL.POLICYID_stg
                                                                                                            LEFT JOIN       DB_T_PROD_STAG.CC_ADDRESS ADDR
                                                                                                            ON              ADDR.ID_stg=CC_PL.ADDRESSID_stg
                                                                                                            LEFT JOIN       DB_T_PROD_STAG.cctl_state c_st
                                                                                                            ON              c_st.id_stg=addr.state_stg
                                                                                                            LEFT JOIN       DB_T_PROD_STAG.cc_deductible DED_IND_CD
                                                                                                            ON              DED_IND_CD.CLaimID_stg = CLM.ID_stg
                                                                                                            AND             exps.CoverageID_stg = DED_IND_CD.CoverageID_stg
                                                                                                            LEFT JOIN
                                                                                                                            (
                                                                                                                                   SELECT *
                                                                                                                                   FROM  (
                                                                                                                                                    SELECT    cl.id_stg,
                                                                                                                                                              VEHID,
                                                                                                                                                              row_number() over( PARTITION BY Cl.ClaimNumber_stg ORDER BY VEHID DESC) AS RNK
                                                                                                                                                    FROM      DB_T_PROD_STAG.CC_CLAIM cl
                                                                                                                                                    LEFT JOIN
                                                                                                                                                              (
                                                                                                                                                                              SELECT DISTINCT cl.ID_stg AS ClaimID,
                                                                                                                                                                                              MAX(veh.VIN_stg) over ( PARTITION BY claimnumber_stg) AS VIN ,
                                                                                                                                                                                              pol.PolicySystemPeriodID_stg                   PolicySystemPeriodID,
                                                                                                                                                                                              SUBSTR(trim(veh.PolicySystemID_stg), 24, 8) AS VEHID
                                                                                                                                                                              FROM            DB_T_PROD_STAG.CC_CLAIM cl
                                                                                                                                                                              JOIN            DB_T_PROD_STAG.CC_POLICY pol
                                                                                                                                                                              ON              pol.ID_stg = cl.PolicyID_stg
                                                                                                                                                                              AND             pol.Verified_stg = 1
                                                                                                                                                                              JOIN            DB_T_PROD_STAG.CC_INCIDENT inc
                                                                                                                                                                              ON              inc.ClaimID_stg = cl.ID_stg
                                                                                                                                                                              JOIN            DB_T_PROD_STAG.CC_VEHICLE veh
                                                                                                                                                                              ON              veh.ID_stg = inc.VehicleID_stg
                                                                                                                                                                              AND             veh.PolicySystemID_stg IS NOT NULL) veh_sub
                                                                                                                                                    ON        veh_sub.ClaimID = cl.ID_stg)a
                                                                                                                                   WHERE  rnk=1) VEH_VIN
                                                                                                            ON              CLM.ID_stg=VEH_VIN.ID_stg
                                                                                                            WHERE           tl4.NAME_stg NOT IN (''Awaiting submission'',
                                                                                                                                                 ''Rejected'',
                                                                                                                                                 ''Submitting'',
                                                                                                                                                 ''Pending approval'') ) A
                                                                                   GROUP BY PolicySystemPeriodID,
                                                                                            VIN,
                                                                                            COMPANYNUMBER,
                                                                                            LOB,
                                                                                            StateCode,
                                                                                            CVGE_CODE,
                                                                                            CovRank,
                                                                                            CloseDate,
                                                                                            CALLYEAR,
                                                                                            ACCOUNTINGYEAR,
                                                                                            EXP_YR,
                                                                                            EXP_MTH,
                                                                                            EXP_DAY,
                                                                                            Losscause,
                                                                                            TERRITORYCODE,
                                                                                            ZIPCODE,
                                                                                            POLICYEFFECTIVEYEAR,
                                                                                            A.PolicySubType,
                                                                                            DED_IND_CODE,
                                                                                            DED_AMT,
                                                                                            Mf_Mdl_Yr,
                                                                                            CLAIMIDENTIFIER,
                                                                                            DB_T_PROD_CORE.CITY,
                                                                                            Policynumber,
                                                                                            EXPOSURENUMBER,
                                                                                            RateDriverClass_alfa ,
                                                                                            clm_veh_cnt,
                                                                                            VEHID,
                                                                                            ClaimantDenormID,
                                                                                            UnverifiedClaimType_alfa ,
                                                                                            ServiceCenterID_alfa,
                                                                                            EffectiveDate,
                                                                                            veh_cnt,
                                                                                            losscause_new,
                                                                                            st_ind) B )c_claim_src
                                              LEFT JOIN
                                                        (
                                                                 SELECT   pc_pavehmodifier.ID                   AS ID,
                                                                          pc_pavehmodifier.Editeffectivedate    AS Editeffectivedate,
                                                                          pc_pavehmodifier.NAIIPCICODE_ALFA_new AS NAIPCI_TERR,
                                                                          pc_pavehmodifier.ST                   AS STate,
                                                                          pc_pavehmodifier.cityinternal         AS CITY,
                                                                          pc_pavehmodifier.postalcodeinternal   AS PostalCode,
                                                                          pc_pavehmodifier.Policynumber         AS Policynumber
                                                                 FROM     (
                                                                                          SELECT DISTINCT PC_POLICYPERIOD.ID_stg                                                                        AS ID,
                                                                                                          PC_POLICYPERIOD.editeffectivedate_stg                                                         AS editeffectivedate ,
                                                                                                          pc_policyperiod.policynumber_stg                                                              AS policynumber,
                                                                                                          jd.typecode_stg                                                                               AS  st,
                                                                                                          plpol.cityinternal_stg                                                                        AS cityinternal,
                                                                                                          plpol.postalcodeinternal_stg                                                                  AS postalcodeinternal,
                                                                                                          COALESCE( PTCA.NAIIPCICODE_ALFA_stg,PTCA2.NAIIPCICODE_ALFA_stg, PTCA1.NAIIPCICODE_ALFA_stg)   AS NAIIPCICODE_ALFA_new
                                                                                          FROM            DB_T_PROD_STAG.PC_POLICYPERIOD PC_POLICYPERIOD
                                                                                          JOIN            DB_T_PROD_STAG.PC_POLICYLINE
                                                                                          ON              PC_POLICYPERIOD.ID_stg = PC_POLICYLINE.BRANCHID_stg
                                                                                          AND             PC_POLICYLINE.EXPIRATIONDATE_stg IS NULL
                                                                                          JOIN            DB_T_PROD_STAG.PCTL_PAPOLICYTYPE_ALFA
                                                                                          ON              PC_POLICYLINE.PAPOLICYTYPE_ALFA_stg = PCTL_PAPOLICYTYPE_ALFA.ID_stg
                                                                                          AND             PCTL_PAPOLICYTYPE_ALFA.TYPECODE_stg IN (''PPV'',
                                                                                                                                                  ''COMMERCIAL'',
                                                                                                                                                  ''PPV2'')
                                                                                          JOIN            DB_T_PROD_STAG.PC_POLICYTERM PT
                                                                                          ON              PT.ID_stg = PC_POLICYPERIOD.POLICYTERMID_stg
                                                                                          LEFT JOIN       DB_T_PROD_STAG.PC_EFFECTIVEDATEDFIELDS EFF
                                                                                          ON              EFF.BRANCHID_stg = PC_POLICYPERIOD.ID_stg
                                                                                          AND             EFF.EXPIRATIONDATE_stg IS NULL
                                                                                          LEFT JOIN       DB_T_PROD_STAG.PC_POLICYLOCATION PLPOL
                                                                                          ON              EFF.PRIMARYLOCATION_stg = PLPOL.ID_stg
                                                                                          AND             PLPOL.EXPIRATIONDATE_stg IS NULL
                                                                                          LEFT JOIN       DB_T_PROD_STAG.PC_TERRITORYCODE TC1
                                                                                          ON              TC1.POLICYLOCATION_stg=PLPOL.FIXEDID_stg
                                                                                          AND             TC1.BRANCHID_stg=PC_POLICYPERIOD.ID_stg
                                                                                          AND             TC1.EXPIRATIONDATE_stg IS NULL
                                                                                          JOIN            DB_T_PROD_STAG.PCTL_JURISDICTION JD
                                                                                          ON              PC_POLICYPERIOD.BASESTATE_stg=JD.ID_stg
                                                                                          LEFT JOIN
                                                                                                          (
                                                                                                                 SELECT NAIIPCICODE_ALFA_stg,
                                                                                                                        COUNTY_stg,
                                                                                                                        TERRITORYCODE_stg,
                                                                                                                        STATE_stg
                                                                                                                 FROM   (
                                                                                                                                 SELECT   NAIIPCICODE_ALFA_stg,
                                                                                                                                          COUNTY_stg,
                                                                                                                                          TERRITORYCODE_stg,
                                                                                                                                          STATE_stg,
                                                                                                                                          ROW_NUMBER() OVER ( PARTITION BY TERRITORYCODE_stg, COUNTY_stg,STATE_stg ORDER BY BEANVERSION_stg, UPDATETIME_stg DESC) AS RNK
                                                                                                                                 FROM     DB_T_PROD_STAG.PCX_PATERRITORYCODE_ALFA )A
                                                                                                                 WHERE  RNK = 1 ) PTCA
                                                                                          ON              JD.ID_stg=PTCA.STATE_stg
                                                                                          AND             UPPER(PTCA.TERRITORYCODE_stg
                                                                                                                          || ''-''
                                                                                                                          || PTCA.COUNTY_stg)= UPPER( TC1.CODE_stg
                                                                                                                          ||''-''
                                                                                                                          ||PLPOL.COUNTYINTERNAL_stg )
                                                                                          LEFT JOIN
                                                                                                          (
                                                                                                                 SELECT NAIIPCICODE_ALFA_stg,
                                                                                                                        COUNTY_stg,
                                                                                                                        TERRITORYCODE_stg,
                                                                                                                        state_stg
                                                                                                                 FROM   (
                                                                                                                                 SELECT   NAIIPCICODE_ALFA_stg,
                                                                                                                                          COUNTY_stg,
                                                                                                                                          TERRITORYCODE_stg,
                                                                                                                                          state_stg,
                                                                                                                                          ROW_NUMBER() OVER ( PARTITION BY COUNTY_stg,state_stg ORDER BY BEANVERSION_stg, UPDATETIME_stg DESC,TERRITORYCODE_stg ) AS RNK
                                                                                                                                 FROM     DB_T_PROD_STAG.PCX_PATERRITORYCODE_ALFA )A
                                                                                                                 WHERE  RNK = 1 ) PTCA1
                                                                                          ON              UPPER(PTCA1.COUNTY_stg)=UPPER( PLPOL.COUNTYINTERNAL_stg)
                                                                                          AND             JD.ID_stg=PTCA1.STATE_stg
                                                                                          LEFT JOIN
                                                                                                          (
                                                                                                                 SELECT NAIIPCICODE_ALFA_stg,
                                                                                                                        COUNTY_stg,
                                                                                                                        TERRITORYCODE_stg,
                                                                                                                        STATE_stg
                                                                                                                 FROM   (
                                                                                                                                 SELECT   NAIIPCICODE_ALFA_stg,
                                                                                                                                          COUNTY_stg,
                                                                                                                                          TERRITORYCODE_stg,
                                                                                                                                          STATE_stg,
                                                                                                                                          ROW_NUMBER() OVER ( PARTITION BY TERRITORYCODE_stg,STATE_stg ORDER BY BEANVERSION_stg, UPDATETIME_stg DESC,COUNTY_stg ) AS RNK
                                                                                                                                 FROM     DB_T_PROD_STAG.PCX_PATERRITORYCODE_ALFA )A
                                                                                                                 WHERE  RNK = 1 ) PTCA2
                                                                                          ON              UPPER(PTCA2.TERRITORYCODE_stg)=UPPER(TC1.CODE_stg)
                                                                                          AND             PTCA2.STATE_stg =JD.ID_stg) PC_PAVEHMODIFIER qualify row_number() over( PARTITION BY Policynumber ORDER BY Editeffectivedate DESC)=1 ) lkp_terr_mismatch
                                              ON        c_claim_src.POLICYNUMBER = lkp_terr_mismatch.Policynumber
                                              LEFT JOIN
                                                        (
                                                               SELECT pc_pavehmodifier.Age_flag   AS Age_flag,
                                                                      pc_pavehmodifier.o_Branchid AS o_Branchid
                                                               FROM   (
                                                                                      SELECT DISTINCT PCR.BRANCHID_stg                                                                                              o_Branchid,
                                                                                                      MIN(extract(YEAR FROM CAST(:CC_BOY AS TIMESTAMP))-extract( YEAR FROM CAST(CNT.DATEOFBIRTH_stg AS TIMESTAMP))) AGE_FLAG
                                                                                      FROM            DB_T_PROD_STAG.PC_POLICYCONTACTROLE PCR
                                                                                      JOIN            DB_T_PROD_STAG.PCTL_POLICYCONTACTROLE PCRL
                                                                                      ON              PCRL.ID_stg=PCR.SUBTYPE_stg
                                                                                      JOIN            DB_T_PROD_STAG.PC_CONTACT CNT
                                                                                      ON              CNT.ID_stg=PCR.CONTACTDENORM_stg
                                                                                      WHERE           PCRL.NAME_stg=''PolicyDriver''
                                                                                      GROUP BY        BRANCHID_stg
                                                                                      HAVING          MIN(extract(YEAR FROM CAST(:CC_BOY AS TIMESTAMP))-extract( YEAR FROM CAST(CNT.DATEOFBIRTH_stg AS TIMESTAMP)))<25 ) PC_PAVEHMODIFIER )lkp_age_flag
                                              ON        lkp_age_flag.o_Branchid=c_claim_src.PolicySystemPeriodID
                                              LEFT JOIN
                                                        (
                                                               SELECT pc_pavehmodifier.Territory  AS Territory,
                                                                      pc_pavehmodifier.County     AS County,
                                                                      pc_pavehmodifier.Postalcode AS Postalcode,
                                                                      pc_pavehmodifier.Typecode   AS Typecode,
                                                                      pc_pavehmodifier.statecode  AS St_code,
                                                                      pc_pavehmodifier.BranchCode AS BranchCode
                                                               FROM   (
                                                                             SELECT TYPECODE,
                                                                                    Territory,
                                                                                    County,
                                                                                    PostalCode,
                                                                                    BranchCode,
                                                                                    CASE
                                                                                           WHEN statecode =''AL'' THEN ''01''
                                                                                           WHEN statecode =''GA'' THEN ''10''
                                                                                           ELSE ''23''
                                                                                    END statecode
                                                                             FROM   (
                                                                                             SELECT   CAST(''PPV'' AS VARCHAR(30)) typecode ,
                                                                                                      Territory,
                                                                                                      County,
                                                                                                      PostalCode,
                                                                                                      BranchCode,
                                                                                                      rnk,
                                                                                                      statecode,
                                                                                                      rank() over( PARTITION BY BranchCode,statecode ORDER BY RNK) AS orderby
                                                                                             FROM     (
                                                                                                                      SELECT DISTINCT PTCA.County,
                                                                                                                                      addr.PostalCode_stg           AS PostalCode,
                                                                                                                                      BranchCode_stg                AS BranchCode,
                                                                                                                                      st.typecode_stg                  statecode,
                                                                                                                                      LEFT(PTCA.NAIIPCICode_alfa,2) AS Territory,
                                                                                                                                      CASE
                                                                                                                                                      WHEN PTCA.COUNTY IS NOT NULL
                                                                                                                                                      AND             PTCA.ZipCode IS NOT NULL
                                                                                                                                                      AND             PTCA.County = addr.County_stg
                                                                                                                                                      AND             PTCA.ZipCode = LEFT(addr.PostalCode_stg,5) THEN 1
                                                                                                                                                      WHEN PTCA.COUNTY IS NULL
                                                                                                                                                      AND             PTCA.ZipCode = LEFT(addr.PostalCode_stg,5) THEN 2
                                                                                                                                                      WHEN PTCA.ZIPCODE IS NULL
                                                                                                                                                      AND             PTCA.County = addr.County_stg THEN 3
                                                                                                                                                      WHEN addr.County_stg = ''ST. CLAIR''
                                                                                                                                                      AND             PTCA.County = ''SAINT CLAIR'' THEN 4
                                                                                                                                                      WHEN addr.County_stg = ''MURRY''
                                                                                                                                                      AND             PTCA.County = ''MURRAY'' THEN 5
                                                                                                                                      END AS rnk
                                                                                                                      FROM            DB_T_PROD_STAG.pc_group grp
                                                                                                                      JOIN            DB_T_PROD_STAG.PC_CONTACT con
                                                                                                                      ON              con.ID_stg = grp.Contact_alfa_stg
                                                                                                                      JOIN            DB_T_PROD_STAG.pc_address ADDR
                                                                                                                      ON              addr.ID_stg = con.PrimaryAddressID_stg
                                                                                                                      JOIN            DB_T_PROD_STAG.pctl_state st
                                                                                                                      ON              addr.state_stg =st.id_stg
                                                                                                                      LEFT JOIN
                                                                                                                                      (
                                                                                                                                             SELECT NAIIPCICODE_ALFA_stg AS NAIIPCICODE_ALFA,
                                                                                                                                                    County_stg           AS County,
                                                                                                                                                    TerritoryCode_stg    AS TerritoryCode,
                                                                                                                                                    ZipCode_stg          AS ZipCode,
                                                                                                                                                    st_cd
                                                                                                                                             FROM   (
                                                                                                                                                             SELECT   NAIIPCICODE_ALFA_stg,
                                                                                                                                                                      County_stg,
                                                                                                                                                                      Territorycode_stg,
                                                                                                                                                                      ZipCode_stg,
                                                                                                                                                                      jd.typecode_stg                                                                                                    st_cd,
                                                                                                                                                                      ROW_NUMBER () over ( PARTITION BY County_stg, ZipCode_stg,state_stg ORDER BY BeanVersion_stg,TerritoryCode_stg) AS RNK
                                                                                                                                                             FROM     DB_T_PROD_STAG.PCX_PATERRITORYCODE_ALFA
                                                                                                                                                             JOIN     DB_T_PROD_STAG.PCTL_JURISDICTION jd
                                                                                                                                                             ON       state_stg =jd.id_stg )A
                                                                                                                                             WHERE  RNK = 1 ) PTCA
                                                                                                                      ON              st_cd=st.typecode_stg
                                                                                                                      AND
                                                                                                                                      CASE
                                                                                                                                                      WHEN PTCA.COUNTY IS NOT NULL
                                                                                                                                                      AND             PTCA.ZipCode IS NOT NULL
                                                                                                                                                      AND             PTCA.County = addr.County_stg
                                                                                                                                                      AND             PTCA.ZipCode= LEFT(addr.PostalCode_stg,5) THEN 1
                                                                                                                                                      WHEN PTCA.COUNTY IS NULL
                                                                                                                                                      AND             PTCA.ZipCode= LEFT(addr.PostalCode_stg,5) THEN 1
                                                                                                                                                      WHEN PTCA.ZIPCODE IS NULL
                                                                                                                                                      AND             PTCA.County = addr.County_stg THEN 1
                                                                                                                                                      WHEN addr.County_stg = ''ST. CLAIR''
                                                                                                                                                      AND             PTCA.County = ''SAINT CLAIR'' THEN 1
                                                                                                                                                      WHEN addr.County_stg = ''MURRY''
                                                                                                                                                      AND             PTCA.County = ''MURRAY'' THEN 1
                                                                                                                                                      ELSE 0
                                                                                                                                      END = 1 )A
                                                                                             UNION
                                                                                             SELECT   CAST(''PPV2'' AS VARCHAR(30)) typecode ,
                                                                                                      Territory,
                                                                                                      County,
                                                                                                      PostalCode,
                                                                                                      BranchCode,
                                                                                                      rnk,
                                                                                                      statecode,
                                                                                                      rank() over( PARTITION BY BranchCode ,statecode ORDER BY RNK) AS orderby
                                                                                             FROM     (
                                                                                                                      SELECT DISTINCT PTCA.County,
                                                                                                                                      addr.PostalCode_stg           AS PostalCode,
                                                                                                                                      BranchCode_stg                AS BranchCode,
                                                                                                                                      st.typecode_stg                  statecode,
                                                                                                                                      LEFT(PTCA.NAIIPCICode_alfa,2) AS Territory,
                                                                                                                                      CASE
                                                                                                                                                      WHEN PTCA.COUNTY IS NOT NULL
                                                                                                                                                      AND             PTCA.ZipCode IS NOT NULL
                                                                                                                                                      AND             PTCA.County = addr.County_stg
                                                                                                                                                      AND             PTCA.ZipCode = LEFT(addr.PostalCode_stg,5) THEN 1
                                                                                                                                                      WHEN PTCA.COUNTY IS NULL
                                                                                                                                                      AND             PTCA.ZipCode = LEFT(addr.PostalCode_stg,5) THEN 2
                                                                                                                                                      WHEN PTCA.ZIPCODE IS NULL
                                                                                                                                                      AND             PTCA.County = addr.County_stg THEN 3
                                                                                                                                                      WHEN addr.County_stg = ''ST. CLAIR''
                                                                                                                                                      AND             PTCA.County = ''SAINT CLAIR'' THEN 4
                                                                                                                                                      WHEN addr.County_stg = ''MURRY''
                                                                                                                                                      AND             PTCA.County = ''MURRAY'' THEN 5
                                                                                                                                      END AS rnk
                                                                                                                      FROM            DB_T_PROD_STAG.pc_group grp
                                                                                                                      JOIN            DB_T_PROD_STAG.PC_CONTACT con
                                                                                                                      ON              con.ID_stg = grp.Contact_alfa_stg
                                                                                                                      JOIN            DB_T_PROD_STAG.pc_address ADDR
                                                                                                                      ON              addr.ID_stg = con.PrimaryAddressID_stg
                                                                                                                      JOIN            DB_T_PROD_STAG.pctl_state st
                                                                                                                      ON              addr.state_stg =st.id_stg
                                                                                                                      LEFT JOIN
                                                                                                                                      (
                                                                                                                                             SELECT NAIIPCICODE_ALFA_stg AS NAIIPCICODE_ALFA,
                                                                                                                                                    County_stg           AS County,
                                                                                                                                                    TerritoryCode_stg    AS TerritoryCode,
                                                                                                                                                    ZipCode_stg          AS ZipCode,
                                                                                                                                                    st_cd
                                                                                                                                             FROM   (
                                                                                                                                                             SELECT   NAIIPCICODE_ALFA_stg,
                                                                                                                                                                      County_stg,
                                                                                                                                                                      Territorycode_stg,
                                                                                                                                                                      ZipCode_stg,
                                                                                                                                                                      jd.typecode_stg                                                                                                    st_cd,
                                                                                                                                                                      ROW_NUMBER () over ( PARTITION BY County_stg, ZipCode_stg,state_stg ORDER BY BeanVersion_stg,TerritoryCode_stg) AS RNK
                                                                                                                                                             FROM     DB_T_PROD_STAG.PCX_PATERRITORYCODE_ALFA
                                                                                                                                                             JOIN     DB_T_PROD_STAG.PCTL_JURISDICTION jd
                                                                                                                                                             ON       state_stg =jd.id_stg )A
                                                                                                                                             WHERE  RNK = 1 ) PTCA
                                                                                                                      ON              st_cd=st.typecode_stg
                                                                                                                      AND
                                                                                                                                      CASE
                                                                                                                                                      WHEN PTCA.COUNTY IS NOT NULL
                                                                                                                                                      AND             PTCA.ZipCode IS NOT NULL
                                                                                                                                                      AND             PTCA.County = addr.County_stg
                                                                                                                                                      AND             PTCA.ZipCode = LEFT(addr.PostalCode_stg,5) THEN 1
                                                                                                                                                      WHEN PTCA.COUNTY IS NULL
                                                                                                                                                      AND             PTCA.ZipCode = LEFT(addr.PostalCode_stg,5) THEN 1
                                                                                                                                                      WHEN PTCA.ZIPCODE IS NULL
                                                                                                                                                      AND             PTCA.County = addr.County_stg THEN 1
                                                                                                                                                      WHEN addr.County_stg = ''ST. CLAIR''
                                                                                                                                                      AND             PTCA.County= ''SAINT CLAIR'' THEN 1
                                                                                                                                                      WHEN addr.County_stg = ''MURRY''
                                                                                                                                                      AND             PTCA.County = ''MURRAY'' THEN 1
                                                                                                                                                      ELSE 0
                                                                                                                                      END = 1 )A
                                                                                             UNION
                                                                                             SELECT   CAST(''COMMERCIAL'' AS VARCHAR(30)) typecode ,
                                                                                                      Territory,
                                                                                                      County,
                                                                                                      PostalCode,
                                                                                                      BranchCode,
                                                                                                      rnk,
                                                                                                      statecode,
                                                                                                      rank() over( PARTITION BY BranchCode ,statecode ORDER BY RNK) AS orderby
                                                                                             FROM     (
                                                                                                                      SELECT DISTINCT PTCA.County,
                                                                                                                                      addr.PostalCode_stg            AS PostalCode,
                                                                                                                                      BranchCode_stg                 AS BranchCode,
                                                                                                                                      st.typecode_stg                   statecode,
                                                                                                                                      RIGHT(PTCA.NAIIPCICode_alfa,2) AS Territory,
                                                                                                                                      CASE
                                                                                                                                                      WHEN PTCA.COUNTY IS NOT NULL
                                                                                                                                                      AND             PTCA.ZipCode IS NOT NULL
                                                                                                                                                      AND             PTCA.County = addr.County_stg
                                                                                                                                                      AND             PTCA.ZipCode = LEFT(addr.PostalCode_stg,5) THEN 1
                                                                                                                                                      WHEN PTCA.COUNTY IS NULL
                                                                                                                                                      AND             PTCA.ZipCode = LEFT(addr.PostalCode_stg,5) THEN 2
                                                                                                                                                      WHEN PTCA.ZIPCODE IS NULL
                                                                                                                                                      AND             PTCA.County = addr.County_stg THEN 3
                                                                                                                                                      WHEN addr.County_stg = ''ST. CLAIR''
                                                                                                                                                      AND             PTCA.County = ''SAINT CLAIR'' THEN 4
                                                                                                                                                      WHEN addr.County_stg = ''MURRY''
                                                                                                                                                      AND             PTCA.County= ''MURRAY'' THEN 5
                                                                                                                                      END AS rnk
                                                                                                                      FROM            DB_T_PROD_STAG.pc_group grp
                                                                                                                      JOIN            DB_T_PROD_STAG.PC_CONTACT con
                                                                                                                      ON              con.ID_stg = grp.Contact_alfa_stg
                                                                                                                      JOIN            DB_T_PROD_STAG.pc_address ADDR
                                                                                                                      ON              addr.ID_stg = con.PrimaryAddressID_stg
                                                                                                                      JOIN            DB_T_PROD_STAG.pctl_state st
                                                                                                                      ON              addr.state_stg =st.id_stg
                                                                                                                      LEFT JOIN
                                                                                                                                      (
                                                                                                                                             SELECT NAIIPCICODE_ALFA_stg AS NAIIPCICODE_ALFA,
                                                                                                                                                    County_stg           AS County,
                                                                                                                                                    TerritoryCode_stg    AS TerritoryCode,
                                                                                                                                                    ZipCode_stg          AS ZipCode,
                                                                                                                                                    st_cd
                                                                                                                                             FROM   (
                                                                                                                                                             SELECT   NAIIPCICODE_ALFA_stg,
                                                                                                                                                                      County_stg,
                                                                                                                                                                      Territorycode_stg,
                                                                                                                                                                      ZipCode_stg,
                                                                                                                                                                      jd.typecode_stg                                                                                                    st_cd,
                                                                                                                                                                      ROW_NUMBER () over ( PARTITION BY County_stg, ZipCode_stg,state_stg ORDER BY BeanVersion_stg,TerritoryCode_stg) AS RNK
                                                                                                                                                             FROM     DB_T_PROD_STAG.PCX_PATERRITORYCODE_ALFA
                                                                                                                                                             JOIN     DB_T_PROD_STAG.PCTL_JURISDICTION jd
                                                                                                                                                             ON       state_stg =jd.id_stg )A
                                                                                                                                             WHERE  RNK = 1 ) PTCA
                                                                                                                      ON              st_cd=st.typecode_stg
                                                                                                                      AND
                                                                                                                                      CASE
                                                                                                                                                      WHEN PTCA.COUNTY IS NOT NULL
                                                                                                                                                      AND             PTCA.ZipCode IS NOT NULL
                                                                                                                                                      AND             PTCA.County = addr.County_stg
                                                                                                                                                      AND             PTCA.ZipCode= LEFT(addr.PostalCode_stg,5) THEN 1
                                                                                                                                                      WHEN PTCA.COUNTY IS NULL
                                                                                                                                                      AND             PTCA.ZipCode = LEFT(addr.PostalCode_stg,5) THEN 1
                                                                                                                                                      WHEN PTCA.ZIPCODE IS NULL
                                                                                                                                                      AND             PTCA.County = addr.County_stg THEN 1
                                                                                                                                                      WHEN addr.County_stg = ''ST. CLAIR''
                                                                                                                                                      AND             PTCA.County = ''SAINT CLAIR'' THEN 1
                                                                                                                                                      WHEN addr.County_stg = ''MURRY''
                                                                                                                                                      AND             PTCA.County = ''MURRAY'' THEN 1
                                                                                                                                                      ELSE 0
                                                                                                                                      END = 1 )A) a
                                                                             WHERE  ORDERBY =1)pc_pavehmodifier) lkp_service_centre
                                              ON        c_claim_Src.PolicySubType=lkp_service_centre.Typecode
                                              AND       c_claim_Src.StateCode=lkp_service_centre.St_code
                                              AND       c_claim_src.ServiceCenterID_alfa=lkp_service_centre.BranchCode
                                              LEFT JOIN
                                                        (
                                                               SELECT pc_pavehmodifier.ST_CODE              AS ST_CODE,
                                                                      pc_pavehmodifier.POL_TYPE             AS POL_TYPE,
                                                                      pc_pavehmodifier.VEH_TYPE             AS VEH_TYPE,
                                                                      pc_pavehmodifier.RATEDRIVERCLASS_ALFA AS RATEDRIVECLASS_ALFA,
                                                                      pc_pavehmodifier.RADIUSOFUSE_ALFA     AS RADIUSOFUSE_ALFA,
                                                                      pc_pavehmodifier.TONNAGE_ALFA         AS TONNAGE_ALFA,
                                                                      pc_pavehmodifier.PPV2_Class_code      AS PPV2_CLASSCODe,
                                                                      pc_pavehmodifier.VEH_CNT              AS VEH_CNT,
                                                                      pc_pavehmodifier.PolicyNumber         AS PolicyNumber,
                                                                      pc_pavehmodifier.VIN                  AS VIN,
                                                                      pc_pavehmodifier.Editeffectivedate    AS Editeffectivedate,
                                                                      pc_pavehmodifier.rnk
                                                               FROM   (
                                                                             SELECT policynumber,
                                                                                    ID,
                                                                                    VIN,
                                                                                    ST_CODE,
                                                                                    POL_TYPE,
                                                                                    VEH_TYPE,
                                                                                    RATEDRIVERCLASS_ALFA,
                                                                                    RADIUSOFUSE_ALFA,
                                                                                    TONNAGE_ALFA,
                                                                                    PPV2_Class_code,
                                                                                    editeffectivedate,
                                                                                    VEH_CNT,
                                                                                    rnk
                                                                             FROM  (
                                                                                              SELECT    pp.policynumber_stg         AS policynumber,
                                                                                                        PP.ID_stg                   AS ID,
                                                                                                        PV.FIXEDID_stg              AS VIN,
                                                                                                        pp.editeffectivedate_stg    AS editeffectivedate,
                                                                                                        ''''                          AS ST_CODE,
                                                                                                        PTyp.TYPECODE_stg              POL_TYPE,
                                                                                                        PVT.TYPECODE_stg               VEH_TYPE,
                                                                                                        PV.RATEDRIVERCLASS_ALFA_stg AS RATEDRIVERCLASS_ALFA,
                                                                                                        rad.typecode_stg               RADIUSOFUSE_ALFA,
                                                                                                        ton.typecode_stg               TONNAGE_ALFA,
                                                                                                        PAM.PATTERNCODE_stg            PPV2_Class_code,
                                                                                                        VEH_CNT,
                                                                                                        row_number() over ( PARTITION BY policynumber,pv.fixedid_stg ORDER BY pv.updatetime_stg DESC,editeffectivedate DESC) AS RNK
                                                                                              FROM      DB_T_PROD_STAG.PC_PERSONALVEHICLE PV
                                                                                              JOIN      DB_T_PROD_STAG.PC_POLICYPERIOD PP
                                                                                              ON        PV.BRANCHID_stg=PP.ID_stg
                                                                                              AND       pv.expirationdate_stg IS NULL
                                                                                              JOIN      DB_T_PROD_STAG.pc_job j
                                                                                              ON        j.ID_stg = pp.JobID_stg
                                                                                              AND       pp.Status_stg = 9
                                                                                              LEFT JOIN DB_T_PROD_STAG.PCTL_VEHICLETYPE PVT
                                                                                              ON        PV.VEHICLETYPE_stg=PVT.ID_stg
                                                                                              LEFT JOIN DB_T_PROD_STAG.PC_POLICYLOCATION loc
                                                                                              ON        loc.ID_stg = pv.GarageLocation_stg
                                                                                              LEFT JOIN DB_T_PROD_STAG.pctl_state st
                                                                                              ON        st.ID_stg = loc.StateInternal_stg
                                                                                              JOIN      DB_T_PROD_STAG.PCTL_JURISDICTION jur
                                                                                              ON        jur.ID_stg = pp.BaseState_stg
                                                                                              JOIN      DB_T_PROD_STAG.PC_POLICYLINE PLN
                                                                                              ON        PLN.BRANCHID_stg=PP.ID_stg
                                                                                              LEFT JOIN DB_T_PROD_STAG.pctl_radiusofuse_alfa rad
                                                                                              ON        rad.ID_stg= pv.RadiusOfUse_alfa_stg
                                                                                              LEFT JOIN DB_T_PROD_STAG.pctl_tonnage_alfa ton
                                                                                              ON        ton.ID_stg= pv.Tonnage_alfa_stg
                                                                                              JOIN      DB_T_PROD_STAG.PCTL_PAPOLICYTYPE_ALFA PTyp
                                                                                              ON        PLN.PAPOLICYTYPE_ALFA_stg= PTyp.ID_stg
                                                                                              AND       PTyp.TYPECODE_stg IN (''PPV'',
                                                                                                                              ''COMMERCIAL'',
                                                                                                                              ''PPV2'')
                                                                                              LEFT JOIN
                                                                                                        (
                                                                                                                 SELECT   PP_VEH_CNT.id_stg       AS id,
                                                                                                                          COUNT(DISTINCT vin_stg)    VEH_CNT
                                                                                                                 FROM     DB_T_PROD_STAG.PC_POLICYPERIOD PP_VEH_CNT
                                                                                                                 JOIN     DB_T_PROD_STAG.PC_PERSONALVEHICLE PV
                                                                                                                 ON       PP_VEH_CNT.id_stg=pv.branchid_stg
                                                                                                                 WHERE    fixedid_stg IS NOT NULL
                                                                                                                 GROUP BY PP_VEH_CNT.id_stg )VEH_CNT
                                                                                              ON        VEH_CNT.ID = PP.ID_stg
                                                                                              LEFT JOIN DB_T_PROD_STAG.PC_PAMODIFIER PAM
                                                                                              ON        PP.ID_stg=PAM.BRANCHID_stg
                                                                                              AND       PAM .PATTERNCODE_stg =''PAAffinityDiscount_alfa'' )a) PC_PAVEHMODIFIER ) lkp_policy
                                              ON        c_claim_src.Policynumber=lkp_policy.PolicyNumber
                                              AND       CAST(c_claim_src.vin_max AS          INTEGER)=CAST(lkp_policy.VIN AS INTEGER)
                                              AND       CAST(lkp_policy.Editeffectivedate AS DATE)<=CAST(effectivedate AS DATE)
                                              LEFT JOIN
                                                        (
                                                                 SELECT   pc_pavehmodifier.ID                                                                             AS ID,
                                                                          pc_pavehmodifier.VEH_CNT                                                                        AS VEH_CNT,
                                                                          pc_pavehmodifier.Policynumber                                                                   AS Policynumber,
                                                                          pc_pavehmodifier.Editeffectivedate                                                              AS Editeffectivedate ,
                                                                          row_number() over( PARTITION BY Policynumber,Editeffectivedate ORDER BY Editeffectivedate DESC)    rnk
                                                                 FROM     (
                                                                                   SELECT   PP_VEH_CNT.policynumber_stg      AS policynumber,
                                                                                            PP_VEH_CNT.id_stg                AS id,
                                                                                            PP_VEH_CNT.editeffectivedate_stg AS editeffectivedate,
                                                                                            COUNT(DISTINCT vin_stg)             VEH_CNT
                                                                                   FROM     DB_T_PROD_STAG.PC_POLICYPERIOD PP_VEH_CNT
                                                                                   JOIN     DB_T_PROD_STAG.PC_PERSONALVEHICLE PV
                                                                                   ON       PP_VEH_CNT.id_stg=pv.branchid_stg
                                                                                   WHERE    fixedid_stg IS NOT NULL
                                                                                   AND      status_stg=''9''
                                                                                   GROUP BY PP_VEH_CNT.id_stg,
                                                                                            PP_VEH_CNT.policynumber_stg,
                                                                                            editeffectivedate_stg) PC_PAVEHMODIFIER
                                                                          /*order by editeffectivedate desc*/
                                                        )lkp_veh_count
                                              ON        c_claim_src.policynumber=lkp_veh_count.policynumber
                                              AND       lkp_veh_count.Editeffectivedate<=c_claim_src.EffectiveDate
                                              LEFT JOIN
                                                        (
                                                               SELECT pc_pavehmodifier.DefensiveDriver AS DefensiveDriver,
                                                                      pc_pavehmodifier.policynumber    AS policynumber
                                                               FROM   (
                                                                                SELECT    pP.PolicyNumber_stg AS PolicyNumber,
                                                                                          MAX(
                                                                                          CASE
                                                                                                    WHEN dis.PatternCode_stg IN (''PADriverTrainingDiscount_alfa'') THEN ''7''
                                                                                                    ELSE ''1''
                                                                                          END) AS DefensiveDriver
                                                                                FROM      DB_T_PROD_STAG.PC_POLICYPERIOD pp
                                                                                LEFT JOIN DB_T_PROD_STAG.PC_PAVEHMODIFIER dis
                                                                                ON        dis.BRANCHID_Stg = pp.ID_Stg
                                                                                AND       dis.ExpirationDate_stg IS NULL
                                                                                GROUP BY  policynumber_stg) pc_pavehmodifier) lkp_defensivedriver_class
                                              ON        c_claim_src.policynumber=lkp_defensivedriver_class.policynumber
                                              LEFT JOIN
                                                        (
                                                               SELECT pc_pavehmodifier.Ind      AS Ind,
                                                                      pc_pavehmodifier.Branchid AS Branchid
                                                               FROM   (
                                                                               SELECT   branchid_stg AS branchid,
                                                                                        MAX(
                                                                                        CASE
                                                                                                 WHEN PATTERNCODE_stg=''PADriverTrainingDiscount_alfa'' THEN ''7''
                                                                                                 ELSE ''1''
                                                                                        END) ind
                                                                               FROM     DB_T_PROD_STAG.PC_PAVEHMODIFIER
                                                                               WHERE    PATTERNCODE_stg=''PADriverTrainingDiscount_alfa''
                                                                               AND      ExpirationDate_stg IS NULL
                                                                               GROUP BY branchid_stg)pc_pavehmodifier) lkp_defensive_driver
                                              ON        lkp_defensive_driver.Branchid=c_claim_src.PolicySystemPeriodID qualify row_number() over( PARTITION BY claimidentifier, c_claim_src.policynumber, c_claim_src.policysystemperiodid , paidloss , c_claim_src. PaidClaims,c_claim_src.PaidALAE, c_claim_src.OutstandingLosses, c_claim_src.OutstandingClaims,c_claim_src.EXPOSURENUMBER ORDER BY COALESCE(lkp_policy.rnk,1) ,COALESCE(lkp_veh_count.rnk,1) )=1 ) SRC ) );
    -- Component EXPTRANS2, Type EXPRESSION
    CREATE
    OR
    REPLACE TEMPORARY TABLE EXPTRANS2 AS
    (
           SELECT SQ_cc_claim.CompanyNumber        AS CompanyNumber,
                  SQ_cc_claim.LOB                  AS LOB,
                  SQ_cc_claim.StateCode            AS StateCode,
                  SQ_cc_claim.CallYear             AS CallYear,
                  SQ_cc_claim.AccountingYear       AS AccountingYear,
                  SQ_cc_claim.Exp_Yr               AS Exp_Yr,
                  SQ_cc_claim.Exp_Mth              AS Exp_Mth,
                  SQ_cc_claim.Exp_Day              AS Exp_Day,
                  SQ_cc_claim.Cvge_Code            AS Cvge_Code,
                  SQ_cc_claim.LossCause            AS LossCause,
                  SQ_cc_claim.PolicyEffectiveYear  AS PolicyEffectiveYear,
                  SQ_cc_claim.Ded_Ind_Code         AS Ded_Ind_Code,
                  SQ_cc_claim.Ded_Amt              AS Ded_Amt,
                  SQ_cc_claim.ClaimIdentifier      AS ClaimIdentifier,
                  SQ_cc_claim.Paid_Loss            AS Paid_Loss,
                  SQ_cc_claim.Paid_Claim           AS Paid_Claim,
                  SQ_cc_claim.Paid_alae            AS Paid_alae,
                  SQ_cc_claim.Outstanding_Loss     AS Outstanding_Loss,
                  SQ_cc_claim.Outstanding_Claim    AS Outstanding_Claim,
                  SQ_cc_claim.ExposureNumber       AS ExposureNumber,
                  SQ_cc_claim.PolicyIdentifier     AS PolicyIdentifier,
                  SQ_cc_claim.VIN                  AS VIN,
                  SQ_cc_claim.Mf_Mdl_Yr            AS Mf_Mdl_Yr,
                  SQ_cc_claim.PolicySystemPeriodID AS PolicySystemPeriodID,
                  SQ_cc_claim.Ann_Stmt_LOB         AS Ann_Stmt_LOB,
                  SQ_cc_claim.Clm_Veh_Cnt          AS Clm_Veh_Cnt,
                  SQ_cc_claim.ClaimnatDenormiD     AS ClaimnatDenormiD,
                  SQ_cc_claim.Policynumber         AS Policynumber,
                  SQ_cc_claim.PolicySubType        AS PolicySubType,
                  SQ_cc_claim.UnverfiiedClaim      AS UnverfiiedClaim,
                  SQ_cc_claim.RateDriverClass_alfa AS RateDriverClass_alfa,
                  CASE
                         WHEN (
                                       SQ_cc_claim.st_ind = ''policy_state'' ) THEN SQ_cc_claim.lkp_terr_mis_NAIPCI_TERR
                         ELSE SQ_cc_claim.TerritoryCode
                  END AS NAIPCI_TERR_new,
                  CASE
                         WHEN (
                                       SQ_cc_claim.st_ind = ''policy_state'' ) THEN SQ_cc_claim.lkp_terr_mis_CITY
                         ELSE SQ_cc_claim.City
                  END AS city_new,
                  CASE
                         WHEN (
                                       SQ_cc_claim.st_ind = ''policy_state'' ) THEN SQ_cc_claim.lkp_terr_mis_PostalCode
                         ELSE SQ_cc_claim.ZipCode
                  END                                     AS Postal_new,
                  SQ_cc_claim.lkp_srv_Cen_Territory       AS lkp_srv_Cen_Territory,
                  SQ_cc_claim.lkp_srv_Cen_County          AS lkp_srv_Cen_County,
                  SQ_cc_claim.lkp_srv_Cen_PostalCode      AS lkp_srv_Cen_PostalCode,
                  SQ_cc_claim.lkp_VEH_CNT                 AS lkp_VEH_CNT,
                  SQ_cc_claim.lkp_pol_VEH_TYPE            AS lkp_pol_VEH_TYPE,
                  SQ_cc_claim.lkp_pol_RATEDRIVECLASS_ALFA AS lkp_pol_RATEDRIVECLASS_ALFA,
                  SQ_cc_claim.lkp_pol_RADIUSOFUSE_ALFA    AS lkp_pol_RADIUSOFUSE_ALFA,
                  SQ_cc_claim.lkp_pol_TONNAGE_ALFA        AS lkp_pol_TONNAGE_ALFA,
                  SQ_cc_claim.lkp_pol_PPV2_CLASSCODe      AS lkp_pol_PPV2_CLASSCODe,
                  SQ_cc_claim.lkp_pol_Editeffectivedate   AS lkp_pol_Editeffectivedate,
                  SQ_cc_claim.lkp_pol_Veh_Cnt             AS lkp_pol_Veh_Cnt,
                  SQ_cc_claim.lkp_pol_rnk                 AS lkp_pol_rnk,
                  SQ_cc_claim.lkp_age_flag                AS lkp_age_flag,
                  SQ_cc_claim.lkp_def_drv_policynumber    AS lkp_def_drv_policynumber,
                  SQ_cc_claim.source_record_id
           FROM   SQ_cc_claim );
    -- Component EXPTRANS3, Type EXPRESSION
    CREATE
    OR
    REPLACE TEMPORARY TABLE EXPTRANS3 AS
    (
           SELECT EXPTRANS2.CompanyNumber   AS CompanyNumber,
                  EXPTRANS2.LOB             AS LOB,
                  EXPTRANS2.StateCode       AS StateCode,
                  EXPTRANS2.CallYear        AS CallYear,
                  EXPTRANS2.AccountingYear  AS AccountingYear,
                  EXPTRANS2.Exp_Yr          AS Exp_Yr,
                  EXPTRANS2.Exp_Mth         AS Exp_Mth,
                  EXPTRANS2.Exp_Day         AS Exp_Day,
                  EXPTRANS2.Cvge_Code       AS Cvge_Code,
                  EXPTRANS2.StateCode       AS St_Code,
                  EXPTRANS2.city_new        AS CityInternal,
                  EXPTRANS2.lkp_pol_Veh_Cnt AS Veh_cnt_Fr_Id,
                  EXPTRANS2.LossCause       AS LossCause,
                  CASE
                         WHEN EXPTRANS2.NAIPCI_TERR_new IS NULL THEN (
                                CASE
                                       WHEN EXPTRANS2.UnverfiiedClaim IN ( 
                                                10003 ,
                                                10004 ) THEN (
                                              CASE
                                                     WHEN EXPTRANS2.lkp_srv_Cen_Territory IS NULL THEN DECODE ( EXPTRANS2.StateCode ,
                                                                                                               ''01'' , ''16'' ,
                                                                                                               ''10'' , ''19'' ,
                                                                                                               ''23'' , ''05'' )
                                                     ELSE EXPTRANS2.lkp_srv_Cen_Territory
                                              END )
                                       ELSE EXPTRANS2.NAIPCI_TERR_new
                                END )
                         ELSE EXPTRANS2.NAIPCI_TERR_new
                  END AS TerritoryCode1,
                  CASE
                         WHEN EXPTRANS2.Postal_new IS NULL THEN (
                                CASE
                                       WHEN EXPTRANS2.UnverfiiedClaim  IN ( 
                                                10003 ,
                                                10004 ) THEN (
                                              CASE
                                                     WHEN EXPTRANS2.lkp_srv_Cen_PostalCode IS NULL THEN DECODE ( EXPTRANS2.StateCode ,
                                                                                                                ''01'' , ''36116'' ,
                                                                                                                ''10'' , ''31763'' ,
                                                                                                                ''23'' , ''38930'' )
                                                     ELSE SUBSTR ( EXPTRANS2.lkp_srv_Cen_PostalCode , 1 , 5 )
                                              END )
                                       ELSE EXPTRANS2.Postal_new
                                END )
                         ELSE EXPTRANS2.Postal_new
                  END                                   AS ZipCode1,
                  EXPTRANS2.PolicyEffectiveYear         AS PolicyEffectiveYear,
                  EXPTRANS2.Ded_Ind_Code                AS Ded_Ind_Code,
                  EXPTRANS2.Ded_Amt                     AS Ded_Amt,
                  EXPTRANS2.Mf_Mdl_Yr                   AS Mf_MDL_Yr,
                  EXPTRANS2.lkp_def_drv_policynumber    AS Df_Drv_Code,
                  ''00''                                  AS PolicyTerm,
                  EXPTRANS2.ClaimIdentifier             AS ClaimIdentifier,
                  EXPTRANS2.ClaimnatDenormiD            AS ClaimantIdentifier,
                  EXPTRANS2.Paid_Loss                   AS Paid_Loss,
                  EXPTRANS2.Paid_Claim                  AS Paid_Claim,
                  EXPTRANS2.Paid_alae                   AS Paid_alae,
                  EXPTRANS2.Outstanding_Loss            AS Outstanding_Loss,
                  EXPTRANS2.Outstanding_Claim           AS Outstanding_Claim,
                  EXPTRANS2.ExposureNumber              AS ExposureNumber,
                  EXPTRANS2.PolicySubType               AS o_Policy_Type,
                  ''000000000000''                        AS WrittenExposure,
                  ''000000000000''                        AS WrittenPremium,
                  ''0''                                   AS PolicyNumber,
                  ''0''                                   AS PolicyPeriodID,
                  EXPTRANS2.PolicyIdentifier            AS PolicyIdentifier,
                  EXPTRANS2.VIN                         AS VIN,
                  EXPTRANS2.PolicySubType               AS Pol_Type,
                  EXPTRANS2.Ann_Stmt_LOB                AS v_Annual_stmt_LOB,
                  v_Annual_stmt_LOB                     AS o_Annual_stmt_LOB,
                  EXPTRANS2.LossCause                   AS v_TypeofLossCode,
                  v_TypeofLossCode                      AS o_TypeofLosscode,
                  EXPTRANS2.lkp_pol_PPV2_CLASSCODe      AS PatternCode_PPV2ClassCode,
                  EXPTRANS2.lkp_pol_VEH_TYPE            AS VehicleTypecode,
                  EXPTRANS2.RateDriverClass_alfa        AS in_Ratedriverclass_alfa,
                  EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA    AS Radiusofuse_alfa,
                  EXPTRANS2.lkp_pol_TONNAGE_ALFA        AS Tonnage_alfa,
                  EXPTRANS2.Clm_Veh_Cnt                 AS Clm_Veh_Cnt,
                  EXPTRANS2.lkp_pol_RATEDRIVECLASS_ALFA AS lkp_RATEDRIVERCLASS_ALFA,
                  CASE
                         WHEN EXPTRANS2.RateDriverClass_alfa IS NULL THEN EXPTRANS2.lkp_pol_RATEDRIVECLASS_ALFA
                         ELSE EXPTRANS2.RateDriverClass_alfa
                  END                    AS v_RATEDRIVERCLASS_ALFA,
                  EXPTRANS2.lkp_age_flag AS Age_Flag,
                  CASE
                         WHEN EXPTRANS2.lkp_age_flag IS NULL THEN ''N''
                         ELSE ''Y''
                  END AS v_Age_Flag,
                  CASE
                         WHEN EXPTRANS2.lkp_pol_Veh_Cnt IS NULL THEN EXPTRANS2.lkp_VEH_CNT
                         ELSE EXPTRANS2.lkp_pol_Veh_Cnt
                  END AS v_VEH_CNT,
                  /*DECODE ( TRUE ,
                          IN ( EXPTRANS2.UnverfiiedClaim ,
                              10003 ,
                              10004 )
                   AND    EXPTRANS2.PolicySubType = ''PPV'' , ''1220'' ,
                          IN ( EXPTRANS2.UnverfiiedClaim ,
                              10003 ,
                              10004 )
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL'' , ''2888'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    (
                                 v_RATEDRIVERCLASS_ALFA IS NULL
                          OR     IN ( v_RATEDRIVERCLASS_ALFA ,
                                     ''1A'' ,
                                     ''1B'' ) )
                   AND    EXPTRANS2.lkp_pol_VEH_TYPE = ''DB''
                   AND    v_Age_Flag = ''Y'' ) , ''9527'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    (
                                 v_RATEDRIVERCLASS_ALFA IS NULL
                          OR     IN ( v_RATEDRIVERCLASS_ALFA ,
                                     ''1A'' ,
                                     ''1B'' ) )
                   AND    EXPTRANS2.lkp_pol_VEH_TYPE = ''DB''
                   AND    v_Age_Flag = ''N'' ) , ''9529'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''1A'' ,
                              ''1B'' )
                   AND    IN ( EXPTRANS2.lkp_pol_VEH_TYPE ,
                              ''AN'' ,
                              ''CL'' )
                   AND    v_Age_Flag = ''Y'' ) , ''9587'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''1A'' ,
                              ''1B'' )
                   AND    IN ( EXPTRANS2.lkp_pol_VEH_TYPE ,
                              ''AN'' ,
                              ''CL'' )
                   AND    v_Age_Flag = ''N'' ) , ''9392'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''1A'' ,
                              ''1B'' )
                   AND    EXPTRANS2.lkp_pol_VEH_TYPE = ''MH''
                   AND    v_Age_Flag = ''Y'' ) , ''9547'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''1A'' ,
                              ''1B'' )
                   AND    EXPTRANS2.lkp_pol_VEH_TYPE = ''MH''
                   AND    v_Age_Flag = ''N'' ) , ''9340'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''1A'' ,
                              ''RL'' )
                   AND    EXPTRANS2.lkp_pol_VEH_TYPE = ''RT'' ) , ''9332'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''GO'' ,
                              ''MA'' )
                   AND    EXPTRANS2.lkp_pol_VEH_TYPE = ''AT''
                   AND    v_Age_Flag = ''Y'' ) , ''9507'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''GO'' ,
                              ''MA'' )
                   AND    EXPTRANS2.lkp_pol_VEH_TYPE = ''AT''
                   AND    v_Age_Flag = ''N'' ) , ''9509'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''MA'' ,
                              ''MY'' )
                   AND    IN ( EXPTRANS2.lkp_pol_VEH_TYPE ,
                              ''MC'' ,
                              ''MS'' )
                   AND    v_Age_Flag = ''Y'' ) , ''9597'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''MA'' ,
                              ''MY'' )
                   AND    IN ( EXPTRANS2.lkp_pol_VEH_TYPE ,
                              ''MC'' ,
                              ''MS'' )
                   AND    v_Age_Flag = ''N'' ) , ''9492'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    v_RATEDRIVERCLASS_ALFA = ''GO''
                   AND    EXPTRANS2.lkp_pol_VEH_TYPE = ''GO'' ) , ''9539'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    v_RATEDRIVERCLASS_ALFA = ''RL''
                   AND    IN ( EXPTRANS2.lkp_pol_VEH_TYPE ,
                              ''LT'' ,
                              ''TR'' ) ) , ''9331'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''1A'' ,
                              ''1B'' )
                   AND    v_VEH_CNT = 1 ) , ''1210'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''1A'' ,
                              ''1B'' )
                   AND    v_VEH_CNT > 1 ) , ''1212'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''6A'' ,
                              ''6B'' )
                   AND    v_VEH_CNT = 1 ) , ''1210'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''6A'' ,
                              ''6B'' )
                   AND    v_VEH_CNT > 1 ) , ''1212'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''1J'' ,
                              ''1K'' ,
                              ''1M'' ,
                              ''6J'' ,
                              ''6K'' )
                   AND    v_VEH_CNT = 1 ) , ''1213'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''1J'' ,
                              ''1K'' ,
                              ''1M'' ,
                              ''6J'' ,
                              ''6K'' )
                   AND    v_VEH_CNT > 1 ) , ''1211'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''1C'' ,
                              ''1D'' ,
                              ''6C'' ,
                              ''6D'' )
                   AND    v_VEH_CNT = 1 ) , ''1220'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''1C'' ,
                              ''1D'' ,
                              ''6C'' ,
                              ''6D'' )
                   AND    v_VEH_CNT > 1 ) , ''1222'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''1E'' ,
                              ''1F'' ,
                              ''6E'' ,
                              ''6F'' )
                   AND    v_VEH_CNT = 1 ) , ''1230'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''1E'' ,
                              ''1F'' ,
                              ''6E'' ,
                              ''6F'' )
                   AND    v_VEH_CNT > 1 ) , ''1232'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''1G'' ,
                              ''1H'' ,
                              ''1L'' ,
                              ''6G'' ,
                              ''6H'' )
                   AND    v_VEH_CNT = 1 ) , ''1400'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''1G'' ,
                              ''1H'' ,
                              ''1L'' ,
                              ''6G'' ,
                              ''6H'' )
                   AND    v_VEH_CNT > 1 ) , ''1402'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''2A'' ,
                              ''2B'' )
                   AND    v_VEH_CNT = 1 ) , ''1510'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''2A'' ,
                              ''2B'' )
                   AND    v_VEH_CNT > 1 ) , ''1512'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''2J'' ,
                              ''2K'' ,
                              ''S1'' ,
                              ''S2'' ,
                              ''S3'' ,
                              ''S4'' )
                   AND    v_VEH_CNT = 1 ) , ''1513'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''2J'' ,
                              ''2K'' ,
                              ''S1'' ,
                              ''S2'' ,
                              ''S3'' ,
                              ''S4'' )
                   AND    v_VEH_CNT > 1 ) , ''1511'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''2C'' ,
                              ''2D'' )
                   AND    v_VEH_CNT = 1 ) , ''1520'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''2C'' ,
                              ''2D'' )
                   AND    v_VEH_CNT > 1 ) , ''1522'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''2E'' ,
                              ''2F'' )
                   AND    v_VEH_CNT = 1 ) , ''1530'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''2E'' ,
                              ''2F'' )
                   AND    v_VEH_CNT > 1 ) , ''1532'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''2G'' ,
                              ''2H'' ,
                              ''21'' ,
                              ''22'' ,
                              ''23'' ,
                              ''24'' )
                   AND    v_VEH_CNT = 1 ) , ''1550'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''2G'' ,
                              ''2H'' ,
                              ''21'' ,
                              ''22'' ,
                              ''23'' ,
                              ''24'' )
                   AND    v_VEH_CNT > 1 ) , ''1552'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''8A'' ,
                              ''8B'' ,
                              ''8C'' ,
                              ''8D'' ,
                              ''8L'' ,
                              ''8N'' ,
                              ''8Y'' ,
                              ''81'' ,
                              ''82'' ,
                              ''83'' ,
                              ''84'' ,
                              ''85'' ,
                              ''86'' ,
                              ''87'' ,
                              ''88'' ,
                              ''89'' ) ) , ''1610'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''8J'' ,
                              ''8K'' ,
                              ''8M'' ,
                              ''8P'' ,
                              ''8Q'' ,
                              ''V1'' ,
                              ''V2'' ,
                              ''V3'' ,
                              ''V4'' ,
                              ''V5'' ,
                              ''V6'' ,
                              ''V7'' ,
                              ''V8'' ,
                              ''V9'' ) ) , ''1613'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''7A'' ,
                              ''7B'' ,
                              ''7C'' ,
                              ''7D'' ,
                              ''7E'' ,
                              ''7F'' ,
                              ''7G'' ,
                              ''7H'' ,
                              ''7N'' ,
                              ''7Y'' ,
                              ''71'' ,
                              ''72'' ,
                              ''73'' ,
                              ''74'' ,
                              ''75'' ,
                              ''76'' ,
                              ''77'' ,
                              ''78'' ,
                              ''79'' ) ) , ''1620'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''7J'' ,
                              ''7K'' ,
                              ''7P'' ,
                              ''7Q'' ,
                              ''7R'' ,
                              ''7X'' ,
                              ''T1'' ,
                              ''T2'' ,
                              ''T3'' ,
                              ''T4'' ,
                              ''T5'' ,
                              ''T6'' ,
                              ''T7'' ,
                              ''T8'' ,
                              ''T9'' ) ) , ''1623'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''9A'' ,
                              ''9B'' ,
                              ''9C'' ,
                              ''9D'' ,
                              ''9N'' ,
                              ''9Y'' ,
                              ''90'' ,
                              ''91'' ,
                              ''92'' ,
                              ''93'' ,
                              ''94'' ,
                              ''95'' ,
                              ''96'' ,
                              ''97'' ,
                              ''98'' ,
                              ''99'' ) ) , ''1630'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''9J'' ,
                              ''9K'' ,
                              ''9P'' ,
                              ''9Q'' ,
                              ''1V'' ,
                              ''2V'' ,
                              ''3V'' ,
                              ''4V'' ,
                              ''5V'' ,
                              ''6V'' ,
                              ''7V'' ,
                              ''8V'' ,
                              ''9V'' ) ) , ''1633'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''4A'' ,
                              ''4B'' ,
                              ''4C'' ,
                              ''4D'' ,
                              ''4N'' ,
                              ''4Y'' ,
                              ''5A'' ,
                              ''5B'' ,
                              ''5C'' ,
                              ''5D'' ,
                              ''5N'' ,
                              ''5Y'' ,
                              ''41'' ,
                              ''42'' ,
                              ''43'' ,
                              ''44'' ,
                              ''45'' ,
                              ''46'' ,
                              ''47'' ,
                              ''48'' ,
                              ''49'' ,
                              ''51'' ,
                              ''52'' ,
                              ''53'' ,
                              ''54'' ,
                              ''55'' ,
                              ''56'' ,
                              ''57'' ,
                              ''58'' ,
                              ''59'' ) ) , ''1640'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''4J'' ,
                              ''4K'' ,
                              ''4P'' ,
                              ''4Q'' ,
                              ''5J'' ,
                              ''5K'' ,
                              ''5P'' ,
                              ''5Q'' ,
                              ''U1'' ,
                              ''U2'' ,
                              ''U3'' ,
                              ''U4'' ,
                              ''U5'' ,
                              ''U6'' ,
                              ''U7'' ,
                              ''U8'' ,
                              ''U9'' ,
                              ''1U'' ,
                              ''2U'' ,
                              ''3U'' ,
                              ''4U'' ,
                              ''5U'' ,
                              ''6U'' ,
                              ''7U'' ,
                              ''8U'' ,
                              ''9U'' ) ) , ''1643'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''3A'' ,
                              ''3B'' ,
                              ''3C'' ,
                              ''3D'' ,
                              ''3E'' ,
                              ''3F'' ,
                              ''3G'' ,
                              ''3H'' ,
                              ''A1'' ,
                              ''A2'' ,
                              ''A3'' ,
                              ''A4'' ,
                              ''A5'' ,
                              ''A6'' ,
                              ''B1'' ,
                              ''B2'' ,
                              ''B3'' ,
                              ''B4'' ,
                              ''B4'' ,
                              ''B5'' ,
                              ''B6'' ,
                              ''C1'' ,
                              ''C2'' ,
                              ''C3'' ,
                              ''C4'' ,
                              ''C5'' ,
                              ''C6'' ,
                              ''AG'' ,
                              ''AH'' ,
                              ''BG'' ,
                              ''BH'' ,
                              ''CG'' ,
                              ''CH'' ) ) , ''1650'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    IN ( v_RATEDRIVERCLASS_ALFA ,
                              ''3J'' ,
                              ''3K'' ,
                              ''AJ'' ,
                              ''AK'' ,
                              ''BJ'' ,
                              ''BK'' ,
                              ''CJ'' ,
                              ''CK'' ) ) , ''1653'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    v_VEH_CNT = 1 ) , ''1220'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV''
                   AND    v_VEH_CNT > 1 ) , ''1222'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV2''
                   AND    EXPTRANS2.lkp_pol_PPV2_CLASSCODe = ''PAAffinityDiscount_alfa''
                   AND    v_VEH_CNT = 1 ) , ''1103'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV2''
                   AND    EXPTRANS2.lkp_pol_PPV2_CLASSCODe = ''PAAffinityDiscount_alfa''
                   AND    v_VEH_CNT > 1 ) , ''1101'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV2''
                   AND    EXPTRANS2.lkp_pol_PPV2_CLASSCODe IS NULL
                   AND    v_VEH_CNT = 1 ) , ''1100'' ,
                          ( EXPTRANS2.PolicySubType = ''PPV2''
                   AND    EXPTRANS2.lkp_pol_PPV2_CLASSCODe IS NULL
                   AND    v_VEH_CNT > 1 ) , ''1102'' ,
                          ( EXPTRANS2.StateCode = ''01''
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    v_RATEDRIVERCLASS_ALFA = ''J1''
                   AND    IN ( EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA ,
                              ''MileRadius100'' ,
                              ''MileRadius50Plus'' ) ) , ''2856'' ,
                          ( EXPTRANS2.StateCode = ''01''
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    v_RATEDRIVERCLASS_ALFA = ''J2''
                   AND    IN ( EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA ,
                              ''MilesRadius300'' ,
                              ''Over300Miles'' ) ) , ''2856'' ,
                          ( EXPTRANS2.StateCode = ''01''
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''LessThan1Ton''
                   AND    v_RATEDRIVERCLASS_ALFA = ''L4'' ) , ''8019'' ,
                          ( EXPTRANS2.StateCode = ''01''
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''LessThan1Ton''
                   AND    v_RATEDRIVERCLASS_ALFA = ''M4'' ) , ''8049'' ,
                          ( EXPTRANS2.StateCode = ''01''
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''1To2Ton''
                   AND    v_RATEDRIVERCLASS_ALFA = ''J5'' ) , ''2856'' ,
                          ( EXPTRANS2.StateCode = ''01''
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''1To2Ton''
                   AND    v_RATEDRIVERCLASS_ALFA = ''L5''
                   AND    IN ( EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA ,
                              ''MileRadius100'' ,
                              ''MileRadius50Plus'' ) ) , ''8229'' ,
                          ( EXPTRANS2.StateCode = ''01''
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''1To2Ton''
                   AND    v_RATEDRIVERCLASS_ALFA = ''M5''
                   AND    IN ( EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA ,
                              ''MileRadius100'' ,
                              ''MileRadius50Plus'' ) ) , ''8259'' ,
                          ( EXPTRANS2.StateCode = ''01''
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''1To2Ton''
                   AND    v_RATEDRIVERCLASS_ALFA = ''N5''
                   AND    IN ( EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA ,
                              ''MilesRadius300'' ,
                              ''Over300Miles'' ) ) , ''8299'' ,
                          ( EXPTRANS2.StateCode = ''01''
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''2To3.5Ton''
                   AND    v_RATEDRIVERCLASS_ALFA = ''J6'' ) , ''2856'' ,
                          ( EXPTRANS2.StateCode = ''01''
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''2To3.5Ton''
                   AND    v_RATEDRIVERCLASS_ALFA = ''L6''
                   AND    IN ( EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA ,
                              ''MileRadius100'' ,
                              ''MileRadius50Plus'' ) ) , ''8329'' ,
                          ( EXPTRANS2.StateCode = ''01''
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''2To3.5Ton''
                   AND    v_RATEDRIVERCLASS_ALFA = ''M6''
                   AND    IN ( EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA ,
                              ''MileRadius100'' ,
                              ''MileRadius50Plus'' ) ) , ''8359'' ,
                          ( EXPTRANS2.StateCode = ''01''
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''2To3.5Ton''
                   AND    v_RATEDRIVERCLASS_ALFA = ''N6''
                   AND    IN ( EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA ,
                              ''MilesRadius300'' ,
                              ''Over300Miles'' ) ) , ''8399'' ,
                          ( EXPTRANS2.StateCode = ''01''
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    v_RATEDRIVERCLASS_ALFA = ''Q4'' ) , ''5800'' ,
                          ( EXPTRANS2.StateCode = ''01''
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    v_RATEDRIVERCLASS_ALFA = ''RL'' ) , ''8939'' ,
                          ( IN ( EXPTRANS2.StateCode ,
                                ''10'' ,
                                ''23'' )
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    v_RATEDRIVERCLASS_ALFA = ''L1'' ) , ''2856'' ,
                          ( IN ( EXPTRANS2.StateCode ,
                                ''10'' ,
                                ''23'' )
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    v_RATEDRIVERCLASS_ALFA = ''M1'' ) , ''2856'' ,
                          ( IN ( EXPTRANS2.StateCode ,
                                ''10'' ,
                                ''23'' )
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    v_RATEDRIVERCLASS_ALFA = ''H1'' ) , ''2856'' ,
                          ( IN ( EXPTRANS2.StateCode ,
                                ''10'' ,
                                ''23'' )
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    IN ( EXPTRANS2.lkp_pol_TONNAGE_ALFA ,
                              ''LessThan1Ton'' ,
                              ''Upto1.5Ton'' )
                   AND    IN ( EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA ,
                              ''MilesRadius300'' ,
                              ''Over300Miles'' )
                   AND    v_RATEDRIVERCLASS_ALFA = ''L4'' ) , ''8039'' ,
                          ( IN ( EXPTRANS2.StateCode ,
                                ''10'' ,
                                ''23'' )
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''Upto1.5Ton''
                   AND    EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA = ''MileRadius100''
                   AND    v_RATEDRIVERCLASS_ALFA = ''L5'' ) , ''8029'' ,
                          ( IN ( EXPTRANS2.StateCode ,
                                ''10'' ,
                                ''23'' )
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''Upto1.5Ton''
                   AND    IN ( EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA ,
                              ''MilesRadius300'' ,
                              ''Over300Miles'' )
                   AND    v_RATEDRIVERCLASS_ALFA = ''L5'' ) , ''8039'' ,
                          ( IN ( EXPTRANS2.StateCode ,
                                ''10'' ,
                                ''23'' )
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''LessThan1Ton''
                   AND    IN ( EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA ,
                              ''MileRadius1-3'' ,
                              ''MileRadius4-10'' ,
                              ''MileRadius11-49'' )
                   AND    v_RATEDRIVERCLASS_ALFA = ''L5'' ) , ''8019'' ,
                          ( IN ( EXPTRANS2.StateCode ,
                                ''10'' ,
                                ''23'' )
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    IN ( EXPTRANS2.lkp_pol_TONNAGE_ALFA ,
                              ''LessThan1Ton'' ,
                              ''Over1.5To3.5Ton'' )
                   AND    EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA = ''MilesRadius300''
                   AND    v_RATEDRIVERCLASS_ALFA = ''M4'' ) , ''8239'' ,
                          ( IN ( EXPTRANS2.StateCode ,
                                ''10'' ,
                                ''23'' )
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''LessThan1Ton''
                   AND    IN ( EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA ,
                              ''MileRadius1-3'' ,
                              ''MileRadius4-10'' ,
                              ''MileRadius11-49'' )
                   AND    v_RATEDRIVERCLASS_ALFA = ''M5'' ) , ''8219'' ,
                          ( IN ( EXPTRANS2.StateCode ,
                                ''10'' ,
                                ''23'' )
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''Over1.5To3.5Ton''
                   AND    EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA = ''MileRadius100''
                   AND    v_RATEDRIVERCLASS_ALFA = ''M5'' ) , ''8229'' ,
                          ( IN ( EXPTRANS2.StateCode ,
                                ''10'' ,
                                ''23'' )
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    IN ( EXPTRANS2.lkp_pol_TONNAGE_ALFA ,
                              ''LessThan1Ton'' ,
                              ''Over3.5Ton'' )
                   AND    IN ( EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA ,
                              ''MilesRadius300'' ,
                              ''Over300Miles'' )
                   AND    v_RATEDRIVERCLASS_ALFA = ''H4'' ) , ''8339'' ,
                          ( IN ( EXPTRANS2.StateCode ,
                                ''10'' ,
                                ''23'' )
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''LessThan1Ton''
                   AND    IN ( EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA ,
                              ''MileRadius1-3'' ,
                              ''MileRadius4-10'' ,
                              ''MileRadius11-49'' )
                   AND    v_RATEDRIVERCLASS_ALFA = ''H5'' ) , ''8319'' ,
                          ( IN ( EXPTRANS2.StateCode ,
                                ''10'' ,
                                ''23'' )
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''Over3.5Ton''
                   AND    EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA = ''MileRadius100''
                   AND    v_RATEDRIVERCLASS_ALFA = ''H5'' ) , ''8329'' ,
                          ( IN ( EXPTRANS2.StateCode ,
                                ''10'' ,
                                ''23'' )
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    v_RATEDRIVERCLASS_ALFA = ''Q4'' ) , ''5800'' ,
                          ( IN ( EXPTRANS2.StateCode ,
                                ''10'' ,
                                ''23'' )
                   AND    EXPTRANS2.PolicySubType = ''COMMERCIAL''
                   AND    v_RATEDRIVERCLASS_ALFA = ''RL'' ) , ''8939'' ,
                          ''1220'' )                    AS Classification1,*/
                  CASE
                     WHEN EXPTRANS2.UnverfiiedClaim IN (10003, 10004) AND EXPTRANS2.PolicySubType = ''PPV'' 
                            THEN ''1220''
                     WHEN EXPTRANS2.UnverfiiedClaim IN (10003, 10004) AND EXPTRANS2.PolicySubType = ''COMMERCIAL'' 
                            THEN ''2888''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND (v_RATEDRIVERCLASS_ALFA IS NULL OR v_RATEDRIVERCLASS_ALFA IN (''1A'', ''1B''))
                            AND EXPTRANS2.lkp_pol_VEH_TYPE = ''DB''
                            AND v_Age_Flag = ''Y'' 
                            THEN ''9527''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND (v_RATEDRIVERCLASS_ALFA IS NULL OR v_RATEDRIVERCLASS_ALFA IN (''1A'', ''1B''))
                            AND EXPTRANS2.lkp_pol_VEH_TYPE = ''DB''
                            AND v_Age_Flag = ''N'' 
                            THEN ''9529''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''1A'', ''1B'')
                            AND EXPTRANS2.lkp_pol_VEH_TYPE IN (''AN'', ''CL'')
                            AND v_Age_Flag = ''Y'' 
                            THEN ''9587''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''1A'', ''1B'')
                            AND EXPTRANS2.lkp_pol_VEH_TYPE IN (''AN'', ''CL'')
                            AND v_Age_Flag = ''N'' 
                            THEN ''9392''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''1A'', ''1B'')
                            AND EXPTRANS2.lkp_pol_VEH_TYPE = ''MH''
                            AND v_Age_Flag = ''Y'' 
                            THEN ''9547''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''1A'', ''1B'')
                            AND EXPTRANS2.lkp_pol_VEH_TYPE = ''MH''
                            AND v_Age_Flag = ''N'' 
                            THEN ''9340''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''1A'', ''RL'')
                            AND EXPTRANS2.lkp_pol_VEH_TYPE = ''RT'' 
                            THEN ''9332''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''GO'', ''MA'')
                            AND EXPTRANS2.lkp_pol_VEH_TYPE = ''AT''
                            AND v_Age_Flag = ''Y'' 
                            THEN ''9507''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''GO'', ''MA'')
                            AND EXPTRANS2.lkp_pol_VEH_TYPE = ''AT''
                            AND v_Age_Flag = ''N'' 
                            THEN ''9509''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''MA'', ''MY'')
                            AND EXPTRANS2.lkp_pol_VEH_TYPE IN (''MC'', ''MS'')
                            AND v_Age_Flag = ''Y'' 
                            THEN ''9597''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''MA'', ''MY'')
                            AND EXPTRANS2.lkp_pol_VEH_TYPE IN (''MC'', ''MS'')
                            AND v_Age_Flag = ''N'' 
                            THEN ''9492''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA = ''GO''
                            AND EXPTRANS2.lkp_pol_VEH_TYPE = ''GO'' 
                            THEN ''9539''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA = ''RL''
                            AND EXPTRANS2.lkp_pol_VEH_TYPE IN (''LT'', ''TR'') 
                            THEN ''9331''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''1A'', ''1B'')
                            AND v_VEH_CNT = 1 
                            THEN ''1210''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''1A'', ''1B'')
                            AND v_VEH_CNT > 1 
                            THEN ''1212''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''6A'', ''6B'')
                            AND v_VEH_CNT = 1 
                            THEN ''1210''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''6A'', ''6B'')
                            AND v_VEH_CNT > 1 
                            THEN ''1212''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''1J'', ''1K'', ''1M'', ''6J'', ''6K'')
                            AND v_VEH_CNT = 1 
                            THEN ''1213''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''1J'', ''1K'', ''1M'', ''6J'', ''6K'')
                            AND v_VEH_CNT > 1 
                            THEN ''1211''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''1C'', ''1D'', ''6C'', ''6D'')
                            AND v_VEH_CNT = 1 
                            THEN ''1220''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''1C'', ''1D'', ''6C'', ''6D'')
                            AND v_VEH_CNT > 1 
                            THEN ''1222''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''1E'', ''1F'', ''6E'', ''6F'')
                            AND v_VEH_CNT = 1 
                            THEN ''1230''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''1E'', ''1F'', ''6E'', ''6F'')
                            AND v_VEH_CNT > 1 
                            THEN ''1232''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''1G'', ''1H'', ''1L'', ''6G'', ''6H'')
                            AND v_VEH_CNT = 1 
                            THEN ''1400''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''1G'', ''1H'', ''1L'', ''6G'', ''6H'')
                            AND v_VEH_CNT > 1 
                            THEN ''1402''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''2A'', ''2B'')
                            AND v_VEH_CNT = 1 
                            THEN ''1510''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''2A'', ''2B'')
                            AND v_VEH_CNT > 1 
                            THEN ''1512''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''2J'', ''2K'', ''S1'', ''S2'', ''S3'', ''S4'')
                            AND v_VEH_CNT = 1 
                            THEN ''1513''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''2J'', ''2K'', ''S1'', ''S2'', ''S3'', ''S4'')
                            AND v_VEH_CNT > 1 
                            THEN ''1511''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''2C'', ''2D'')
                            AND v_VEH_CNT = 1 
                            THEN ''1520''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''2C'', ''2D'')
                            AND v_VEH_CNT > 1 
                            THEN ''1522''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''2E'', ''2F'')
                            AND v_VEH_CNT = 1 
                            THEN ''1530''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''2E'', ''2F'')
                            AND v_VEH_CNT > 1 
                            THEN ''1532''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''2G'', ''2H'', ''21'', ''22'', ''23'', ''24'')
                            AND v_VEH_CNT = 1 
                            THEN ''1550''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''2G'', ''2H'', ''21'', ''22'', ''23'', ''24'')
                            AND v_VEH_CNT > 1 
                            THEN ''1552''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''8A'', ''8B'', ''8C'', ''8D'', ''8L'', ''8N'', ''8Y'', ''81'', ''82'', ''83'', ''84'', ''85'', ''86'', ''87'', ''88'', ''89'') 
                            THEN ''1610''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''8J'', ''8K'', ''8M'', ''8P'', ''8Q'', ''V1'', ''V2'', ''V3'', ''V4'', ''V5'', ''V6'', ''V7'', ''V8'', ''V9'') 
                            THEN ''1613''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''7A'', ''7B'', ''7C'', ''7D'', ''7E'', ''7F'', ''7G'', ''7H'', ''7N'', ''7Y'', ''71'', ''72'', ''73'', ''74'', ''75'', ''76'', ''77'', ''78'', ''79'') 
                            THEN ''1620''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''7J'', ''7K'', ''7P'', ''7Q'', ''7R'', ''7X'', ''T1'', ''T2'', ''T3'', ''T4'', ''T5'', ''T6'', ''T7'', ''T8'', ''T9'') 
                            THEN ''1623''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''9A'', ''9B'', ''9C'', ''9D'', ''9N'', ''9Y'', ''90'', ''91'', ''92'', ''93'', ''94'', ''95'', ''96'', ''97'', ''98'', ''99'') 
                            THEN ''1630''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''9J'', ''9K'', ''9P'', ''9Q'', ''1V'', ''2V'', ''3V'', ''4V'', ''5V'', ''6V'', ''7V'', ''8V'', ''9V'') 
                            THEN ''1633''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''4A'', ''4B'', ''4C'', ''4D'', ''4N'', ''4Y'', ''5A'', ''5B'', ''5C'', ''5D'', ''5N'', ''5Y'', ''41'', ''42'', ''43'', ''44'', ''45'', ''46'', ''47'', ''48'', ''49'', ''51'', ''52'', ''53'', ''54'', ''55'', ''56'', ''57'', ''58'', ''59'') 
                            THEN ''1640''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''4J'', ''4K'', ''4P'', ''4Q'', ''5J'', ''5K'', ''5P'', ''5Q'', ''U1'', ''U2'', ''U3'', ''U4'', ''U5'', ''U6'', ''U7'', ''U8'', ''U9'', ''1U'', ''2U'', ''3U'', ''4U'', ''5U'', ''6U'', ''7U'', ''8U'', ''9U'') 
                            THEN ''1643''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''3A'', ''3B'', ''3C'', ''3D'', ''3E'', ''3F'', ''3G'', ''3H'', ''A1'', ''A2'', ''A3'', ''A4'', ''A5'', ''A6'', ''B1'', ''B2'', ''B3'', ''B4'', ''B5'', ''B6'', ''C1'', ''C2'', ''C3'', ''C4'', ''C5'', ''C6'', ''AG'', ''AH'', ''BG'', ''BH'', ''CG'', ''CH'') 
                            THEN ''1650''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_RATEDRIVERCLASS_ALFA IN (''3J'', ''3K'', ''AJ'', ''AK'', ''BJ'', ''BK'', ''CJ'', ''CK'') 
                            THEN ''1653''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_VEH_CNT = 1 
                            THEN ''1220''
                     WHEN EXPTRANS2.PolicySubType = ''PPV''
                            AND v_VEH_CNT > 1 
                            THEN ''1222''
                     WHEN EXPTRANS2.PolicySubType = ''PPV2''
                            AND EXPTRANS2.lkp_pol_PPV2_CLASSCODe = ''PAAffinityDiscount_alfa''
                            AND v_VEH_CNT = 1 
                            THEN ''1103''
                     WHEN EXPTRANS2.PolicySubType = ''PPV2''
                            AND EXPTRANS2.lkp_pol_PPV2_CLASSCODe = ''PAAffinityDiscount_alfa''
                            AND v_VEH_CNT > 1 
                            THEN ''1101''
                     WHEN EXPTRANS2.PolicySubType = ''PPV2''
                            AND EXPTRANS2.lkp_pol_PPV2_CLASSCODe IS NULL
                            AND v_VEH_CNT = 1 
                            THEN ''1100''
                     WHEN EXPTRANS2.PolicySubType = ''PPV2''
                            AND EXPTRANS2.lkp_pol_PPV2_CLASSCODe IS NULL
                            AND v_VEH_CNT > 1 
                            THEN ''1102''
                     WHEN EXPTRANS2.StateCode = ''01''
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND v_RATEDRIVERCLASS_ALFA = ''J1''
                            AND EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA IN (''MileRadius100'', ''MileRadius50Plus'') 
                            THEN ''2856''
                     WHEN EXPTRANS2.StateCode = ''01''
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND v_RATEDRIVERCLASS_ALFA = ''J2''
                            AND EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA IN (''MilesRadius300'', ''Over300Miles'') 
                            THEN ''2856''
                     WHEN EXPTRANS2.StateCode = ''01''
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''LessThan1Ton''
                            AND v_RATEDRIVERCLASS_ALFA = ''L4'' 
                            THEN ''8019''
                     WHEN EXPTRANS2.StateCode = ''01''
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''LessThan1Ton''
                            AND v_RATEDRIVERCLASS_ALFA = ''M4'' 
                            THEN ''8049''
                     WHEN EXPTRANS2.StateCode = ''01''
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''1To2Ton''
                            AND v_RATEDRIVERCLASS_ALFA = ''J5'' 
                            THEN ''2856''
                     WHEN EXPTRANS2.StateCode = ''01''
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''1To2Ton''
                            AND v_RATEDRIVERCLASS_ALFA = ''L5''
                            AND EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA IN (''MileRadius100'', ''MileRadius50Plus'') 
                            THEN ''8229''
                     WHEN EXPTRANS2.StateCode = ''01''
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''1To2Ton''
                            AND v_RATEDRIVERCLASS_ALFA = ''M5''
                            AND EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA IN (''MileRadius100'', ''MileRadius50Plus'') 
                            THEN ''8259''
                     WHEN EXPTRANS2.StateCode = ''01''
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''1To2Ton''
                            AND v_RATEDRIVERCLASS_ALFA = ''N5''
                            AND EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA IN (''MilesRadius300'', ''Over300Miles'') 
                            THEN ''8299''
                     WHEN EXPTRANS2.StateCode = ''01''
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''2To3.5Ton''
                            AND v_RATEDRIVERCLASS_ALFA = ''J6'' 
                            THEN ''2856''
                     WHEN EXPTRANS2.StateCode = ''01''
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''2To3.5Ton''
                            AND v_RATEDRIVERCLASS_ALFA = ''L6''
                            AND EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA IN (''MileRadius100'', ''MileRadius50Plus'') 
                            THEN ''8329''
                     WHEN EXPTRANS2.StateCode = ''01''
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''2To3.5Ton''
                            AND v_RATEDRIVERCLASS_ALFA = ''M6''
                            AND EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA IN (''MileRadius100'', ''MileRadius50Plus'') 
                            THEN ''8359''
                     WHEN EXPTRANS2.StateCode = ''01''
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''2To3.5Ton''
                            AND v_RATEDRIVERCLASS_ALFA = ''N6''
                            AND EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA IN (''MilesRadius300'', ''Over300Miles'') 
                            THEN ''8399''
                     WHEN EXPTRANS2.StateCode = ''01''
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND v_RATEDRIVERCLASS_ALFA = ''Q4'' 
                            THEN ''5800''
                     WHEN EXPTRANS2.StateCode = ''01''
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND v_RATEDRIVERCLASS_ALFA = ''RL'' 
                            THEN ''8939''
                     WHEN EXPTRANS2.StateCode IN (''10'', ''23'')
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND v_RATEDRIVERCLASS_ALFA = ''L1'' 
                            THEN ''2856''
                     WHEN EXPTRANS2.StateCode IN (''10'', ''23'')
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND v_RATEDRIVERCLASS_ALFA = ''M1'' 
                            THEN ''2856''
                     WHEN EXPTRANS2.StateCode IN (''10'', ''23'')
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND v_RATEDRIVERCLASS_ALFA = ''H1'' 
                            THEN ''2856''
                     WHEN EXPTRANS2.StateCode IN (''10'', ''23'')
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA IN (''LessThan1Ton'', ''Upto1.5Ton'')
                            AND EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA IN (''MilesRadius300'', ''Over300Miles'')
                            AND v_RATEDRIVERCLASS_ALFA = ''L4'' 
                            THEN ''8039''
                     WHEN EXPTRANS2.StateCode IN (''10'', ''23'')
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''Upto1.5Ton''
                            AND EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA = ''MileRadius100''
                            AND v_RATEDRIVERCLASS_ALFA = ''L5'' 
                            THEN ''8029''
                     WHEN EXPTRANS2.StateCode IN (''10'', ''23'')
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''Upto1.5Ton''
                            AND EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA IN (''MilesRadius300'', ''Over300Miles'')
                            AND v_RATEDRIVERCLASS_ALFA = ''L5'' 
                            THEN ''8039''
                     WHEN EXPTRANS2.StateCode IN (''10'', ''23'')
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''LessThan1Ton''
                            AND EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA IN (''MileRadius1-3'', ''MileRadius4-10'', ''MileRadius11-49'')
                            AND v_RATEDRIVERCLASS_ALFA = ''L5'' 
                            THEN ''8019''
                     WHEN EXPTRANS2.StateCode IN (''10'', ''23'')
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA IN (''LessThan1Ton'', ''Over1.5To3.5Ton'')
                            AND EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA = ''MilesRadius300''
                            AND v_RATEDRIVERCLASS_ALFA = ''M4'' 
                            THEN ''8239''
                     WHEN EXPTRANS2.StateCode IN (''10'', ''23'')
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''LessThan1Ton''
                            AND EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA IN (''MileRadius1-3'', ''MileRadius4-10'', ''MileRadius11-49'')
                            AND v_RATEDRIVERCLASS_ALFA = ''M5'' 
                            THEN ''8219''
                     WHEN EXPTRANS2.StateCode IN (''10'', ''23'')
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''Over1.5To3.5Ton''
                            AND EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA = ''MileRadius100''
                            AND v_RATEDRIVERCLASS_ALFA = ''M5'' 
                            THEN ''8229''
                     WHEN EXPTRANS2.StateCode IN (''10'', ''23'')
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA IN (''LessThan1Ton'', ''Over3.5Ton'')
                            AND EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA IN (''MilesRadius300'', ''Over300Miles'')
                            AND v_RATEDRIVERCLASS_ALFA = ''H4'' 
                            THEN ''8339''
                     WHEN EXPTRANS2.StateCode IN (''10'', ''23'')
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''LessThan1Ton''
                            AND EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA IN (''MileRadius1-3'', ''MileRadius4-10'', ''MileRadius11-49'')
                            AND v_RATEDRIVERCLASS_ALFA = ''H5'' 
                            THEN ''8319''
                     WHEN EXPTRANS2.StateCode IN (''10'', ''23'')
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND EXPTRANS2.lkp_pol_TONNAGE_ALFA = ''Over3.5Ton''
                            AND EXPTRANS2.lkp_pol_RADIUSOFUSE_ALFA = ''MileRadius100''
                            AND v_RATEDRIVERCLASS_ALFA = ''H5'' 
                            THEN ''8329''
                     WHEN EXPTRANS2.StateCode IN (''10'', ''23'')
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND v_RATEDRIVERCLASS_ALFA = ''Q4'' 
                            THEN ''5800''
                     WHEN EXPTRANS2.StateCode IN (''10'', ''23'')
                            AND EXPTRANS2.PolicySubType = ''COMMERCIAL''
                            AND v_RATEDRIVERCLASS_ALFA = ''RL'' 
                            THEN ''8939''
                     ELSE ''1220''
                     END AS Classification1,
                  EXPTRANS2.PolicySystemPeriodID      AS PolicySystemPeriodID,
                  EXPTRANS2.lkp_pol_rnk               AS Rnk,
                  NULL                                AS in_ID,
                  EXPTRANS2.UnverfiiedClaim           AS UnverfiiedClaim,
                  EXPTRANS2.lkp_pol_Editeffectivedate AS Editeffectivedate,
                  EXPTRANS2.source_record_id
           FROM   EXPTRANS2 );
    -- Component exp_hold_data1, Type EXPRESSION
    CREATE
    OR
    REPLACE TEMPORARY TABLE exp_hold_data1 AS
    (
           SELECT EXPTRANS3.CompanyNumber       AS CompanyNumber,
                  EXPTRANS3.LOB                 AS LOB,
                  EXPTRANS3.StateCode           AS StateCode,
                  EXPTRANS3.CallYear            AS CallYear,
                  EXPTRANS3.AccountingYear      AS AccountingYear,
                  EXPTRANS3.Exp_Yr              AS Exp_Yr,
                  EXPTRANS3.Exp_Mth             AS Exp_Mth,
                  EXPTRANS3.Exp_Day             AS Exp_Day,
                  EXPTRANS3.Cvge_Code           AS Cvge_Code,
                  EXPTRANS3.St_Code             AS St_Code,
                  EXPTRANS3.CityInternal        AS CityInternal,
                  EXPTRANS3.Veh_cnt_Fr_Id       AS Veh_cnt_Fr_Id,
                  EXPTRANS3.TerritoryCode1      AS TerritoryCode,
                  EXPTRANS3.ZipCode1            AS ZipCode,
                  EXPTRANS3.PolicyEffectiveYear AS PolicyEffectiveYear,
                  EXPTRANS3.Ded_Ind_Code        AS Ded_Ind_Code,
                  EXPTRANS3.Ded_Amt             AS Ded_Amt,
                  EXPTRANS3.Mf_MDL_Yr           AS Mf_MDL_Yr,
                  EXPTRANS3.Df_Drv_Code         AS Df_Drv_Code,
                  EXPTRANS3.PolicyTerm          AS PolicyTerm,
                  EXPTRANS3.ClaimIdentifier     AS ClaimIdentifier,
                  EXPTRANS3.ClaimantIdentifier  AS ClaimantIdentifier,
                  EXPTRANS3.Paid_Loss           AS Paid_Loss,
                  EXPTRANS3.Paid_Claim          AS Paid_Claim,
                  EXPTRANS3.Paid_alae           AS Paid_alae,
                  EXPTRANS3.Outstanding_Loss    AS Outstanding_Loss,
                  EXPTRANS3.Outstanding_Claim   AS Outstanding_Claim,
                  EXPTRANS3.ExposureNumber      AS ExposureNumber,
                  EXPTRANS3.WrittenExposure     AS WrittenExposure,
                  EXPTRANS3.WrittenPremium      AS WrittenPremium,
                  EXPTRANS3.PolicyNumber        AS PolicyNumber,
                  EXPTRANS3.PolicyPeriodID      AS PolicyPeriodID,
                  EXPTRANS3.PolicyIdentifier    AS PolicyIdentifier,
                  EXPTRANS3.VIN                 AS VIN,
                  EXPTRANS3.Pol_Type            AS Pol_Type,
                  EXPTRANS3.o_Annual_stmt_LOB   AS Ann_Stmt_LOB,
                  EXPTRANS3.o_TypeofLosscode    AS o_TypeofLosscode,
                  EXPTRANS3.Clm_Veh_Cnt         AS Clm_Veh_Cnt,
                  EXPTRANS3.Classification1     AS Classification1,
                  EXPTRANS3.in_ID               AS in_ID,
                  EXPTRANS3.source_record_id
           FROM   EXPTRANS3 );
    -- Component EXPTRANS4, Type EXPRESSION
    CREATE
    OR
    REPLACE TEMPORARY TABLE EXPTRANS4 AS
    (
              SELECT
                        CASE
                                  WHEN exp_hold_data1.CompanyNumber IS NULL THEN ''''
                                  ELSE exp_hold_data1.CompanyNumber
                        END                AS v_CompanyNumber,
                        v_CompanyNumber    AS o_CompanyNumber,
                        exp_hold_data1.LOB AS LOB,
                        LKP_1.Classification
                        /* replaced lookup LKP_CLASSIFICATION */
                        AS v_lkp_claim,
                        LKP_2.Classification
                        /* replaced lookup LKP_CLASSIFICATION */
                        AS v_lkp_policy,
                        CASE
                                  WHEN (
                                                      exp_hold_data1.Classification1 IS NULL
                                            AND       exp_hold_data1.Pol_Type = ''PPV'' ) THEN ''1220''
                                  ELSE (
                                            CASE
                                                      WHEN (
                                                                          exp_hold_data1.Classification1 IS NULL
                                                                AND       exp_hold_data1.Pol_Type = ''COMMERCIAL'' ) THEN (
                                                                CASE
                                                                          WHEN exp_hold_data1.PolicyNumber = ''0'' THEN v_lkp_claim
                                                                          ELSE v_lkp_policy
                                                                END )
                                                      ELSE exp_hold_data1.Classification1
                                            END )
                        END               AS Classification121,
                        Classification121 AS Classification_out,
                        CASE
                                  WHEN exp_hold_data1.StateCode IS NULL THEN ''''
                                  ELSE exp_hold_data1.StateCode
                        END                           AS v_StateOfPrincipalGarage,
                        v_StateOfPrincipalGarage      AS o_StateOfPrincipalGarage,
                        exp_hold_data1.CallYear       AS CallYear,
                        exp_hold_data1.AccountingYear AS AccountingYear,
                        CASE
                                  WHEN exp_hold_data1.Exp_Yr IS NULL THEN ''''
                                  ELSE exp_hold_data1.Exp_Yr
                        END             AS v_ExpPeriodYear,
                        v_ExpPeriodYear AS o_ExpPeriodYear,
                        CASE
                                  WHEN exp_hold_data1.Exp_Mth IS NULL THEN ''''
                                  ELSE exp_hold_data1.Exp_Mth
                        END              AS v_ExpPeriodMonth,
                        v_ExpPeriodMonth AS o_ExpPeriodMonth,
                        CASE
                                  WHEN exp_hold_data1.Exp_Day IS NULL THEN ''''
                                  ELSE exp_hold_data1.Exp_Day
                        END            AS v_ExpPeriodDay,
                        v_ExpPeriodDay AS o_ExpPeriodDay,
                        CASE
                                  WHEN exp_hold_data1.Cvge_Code IS NULL THEN ''000''
                                  ELSE exp_hold_data1.Cvge_Code
                        END            AS v_CoverageCode,
                        v_CoverageCode AS o_CoverageCode,
                        CASE
                                  WHEN Classification121 IN ( 
                                           ''8299'' ,
                                           ''8399'' ,
                                           ''8939'' ,
                                           ''8239'' ,
                                           ''8339'' ) THEN ''00''
                                  ELSE lpad ( exp_hold_data1.TerritoryCode , 2 , ''0'' )
                        END AS v_TerritoryCode,
                        TO_CHAR (
                        CASE
                                  WHEN v_TerritoryCode IS NULL THEN ''00''
                                  ELSE v_TerritoryCode
                        END ) AS o_TerritoryCode,
                        CASE
                                  WHEN exp_hold_data1.ZipCode IS NULL THEN ''00000''
                                  ELSE lpad ( exp_hold_data1.ZipCode , 5 , ''0'' )
                        END       AS v_Zipcode,
                        v_Zipcode AS o_Zipcode,
                        CASE
                                  WHEN exp_hold_data1.PolicyEffectiveYear IS NULL THEN ''0000''
                                  ELSE exp_hold_data1.PolicyEffectiveYear
                        END             AS v_Policy_Eff_Yr,
                        v_Policy_Eff_Yr AS o_Policy_Eff_Yr,
                        ''00''            AS StateExceptionCode,
                        CASE
                                  WHEN exp_hold_data1.Ded_Ind_Code IS NULL THEN ''0''
                                  ELSE exp_hold_data1.Ded_Ind_Code
                        END            AS v_Ded_Ind_Code,
                        v_Ded_Ind_Code AS o_Ded_Ind_Code,
                        CASE
                                  WHEN exp_hold_data1.Ded_Amt IS NULL THEN 0
                                  ELSE exp_hold_data1.Ded_Amt
                        END                AS v_DeductibleAmount,
                        v_DeductibleAmount AS o_DeductibleAmount,
                        ''00''               AS SublineCode,
                        CASE
                                  WHEN exp_hold_data1.Mf_MDL_Yr IS NULL THEN ''0000''
                                  ELSE exp_hold_data1.Mf_MDL_Yr
                        END                        AS v_Mf_MDL_Yr,
                        v_Mf_MDL_Yr                AS o_Mf_MDL_Yr,
                        ''0''                        AS AgeGroupCode,
                        ''0''                        AS AntiTheftCode,
                        ''0''                        AS DayTimeRunninglampCode,
                        exp_hold_data1.Df_Drv_Code AS Df_Drv_Code,
                        ''0''                        AS ExceptionBCode,
                        CASE
                                  WHEN exp_hold_data1.PolicyTerm IS NULL THEN ''0''
                                  ELSE exp_hold_data1.PolicyTerm
                        END          AS v_PolicyTerm,
                        v_PolicyTerm AS o_PolicyTerm,
                        ''00''         AS PenaltyPoints,
                        ''0000''       AS PolicyLowerLimit,
                        ''0000''       AS PolicyUpperLimit,
                        ''10''         AS PolicyIDCode,
                        ''0''          AS PassiveRestraintCode,
                        ''0''          AS ForgivenessCode,
                        CASE
                                  WHEN Classification121 IN ( 
                                           ''8299'' ,
                                           ''8399'' ,
                                           ''8939'' ,
                                           ''8239'' ,
                                           ''8339'' )
                                  AND       UPPER ( exp_hold_data1.CityInternal ) <> ''ATLANTA'' THEN ''9''
                                  ELSE
                                            CASE
                                                      WHEN  Classification121 IN (
                                                               ''8299'' ,
                                                               ''8399'' ,
                                                               ''8939'' ,
                                                               ''8239'' ,
                                                               ''8339'' )
                                                      AND       UPPER ( exp_hold_data1.CityInternal ) = ''ATLANTA'' THEN ''1''
                                                      ELSE ''0''
                                            END
                        END              AS v_Ratingzonecode,
                        v_Ratingzonecode AS o_Ratingzonecode,
                        CASE
                                  WHEN Classification121 IN ( 
                                           ''8299'' ,
                                           ''8399'' ,
                                           ''8939'' ,
                                           ''8239'' ,
                                           ''8339'' )
                                  AND       UPPER ( exp_hold_data1.CityInternal ) <> ''ATLANTA'' THEN ''46''
                                  ELSE
                                            CASE
                                                      WHEN Classification121 IN ( 
                                                               ''8299'' ,
                                                               ''8399'' ,
                                                               ''8939'' ,
                                                               ''8239'' ,
                                                               ''8339'' )
                                                      AND       UPPER ( exp_hold_data1.CityInternal ) = ''ATLANTA'' THEN ''01''
                                                      ELSE ''00''
                                            END
                        END                               AS v_Terminalzonecode,
                        v_Terminalzonecode                AS o_Terminalzonecode,
                        CURRENT_TIMESTAMP                 AS CreateTS,
                        CURRENT_TIMESTAMP                 AS UpdateTS,
                        ''000000000000''                    AS OutStandingAllocLossAdjExp,
                        exp_hold_data1.ClaimIdentifier    AS ClaimNumber,
                        exp_hold_data1.ClaimantIdentifier AS ClaimantIdentifier,
                        CASE
                                  WHEN exp_hold_data1.Paid_Loss IS NULL THEN ''0''
                                  ELSE exp_hold_data1.Paid_Loss
                        END                       AS v_PaidLosses,
                        v_PaidLosses              AS o_PaidLosses,
                        exp_hold_data1.Paid_Claim AS PaidClaims,
                        CASE
                                  WHEN exp_hold_data1.Paid_alae IS NULL THEN ''0''
                                  ELSE exp_hold_data1.Paid_alae
                        END        AS v_PaidALAE,
                        v_PaidALAE AS o_PaidALAE,
                        CASE
                                  WHEN exp_hold_data1.Outstanding_Loss IS NULL THEN ''0''
                                  ELSE exp_hold_data1.Outstanding_Loss
                        END                              AS v_OutStandingLosses,
                        v_OutStandingLosses              AS o_OutStandingLosses,
                        exp_hold_data1.Outstanding_Claim AS OutStandingClaims,
                        CASE
                                  WHEN exp_hold_data1.WrittenExposure IS NULL THEN ''0''
                                  ELSE exp_hold_data1.WrittenExposure
                        END               AS v_WrittenExposure,
                        v_WrittenExposure AS o_WrittenExposure,
                        CASE
                                  WHEN exp_hold_data1.WrittenPremium IS NULL THEN ''0''
                                  ELSE exp_hold_data1.WrittenPremium
                        END                             AS v_WrittenPremium,
                        v_WrittenPremium                AS o_WrittenPremium,
                        exp_hold_data1.PolicyNumber     AS PolicyNumber,
                        exp_hold_data1.PolicyPeriodID   AS PolicyPeriodID,
                        exp_hold_data1.PolicyIdentifier AS PolilcyIdentifier,
                        CASE
                                  WHEN exp_hold_data1.VIN IS NULL THEN ''0''
                                  ELSE exp_hold_data1.VIN
                        END   AS v_VIN,
                        v_VIN AS o_VIN,
                        CASE
                                  WHEN exp_hold_data1.ExposureNumber IS NULL THEN ''0''
                                  ELSE exp_hold_data1.ExposureNumber
                        END              AS v_ExposureNumber,
                        v_ExposureNumber AS o_ExposureNumber,
                        CASE
                                  WHEN exp_hold_data1.Ann_Stmt_LOB IS NULL
                                  OR        exp_hold_data1.Ann_Stmt_LOB = ''0''
                                  OR        exp_hold_data1.Ann_Stmt_LOB = ''000'' THEN ''000''
                                  ELSE exp_hold_data1.Ann_Stmt_LOB
                        END            AS v_Ann_Stmt_LOB,
                        v_Ann_Stmt_LOB AS o_Ann_Stmt_LOB,
                        CASE
                                  WHEN exp_hold_data1.o_TypeofLosscode IS NULL THEN ''00''
                                  ELSE exp_hold_data1.o_TypeofLosscode
                        END              AS v_TypeofLossCode,
                        v_TypeofLossCode AS o_TypeofLossCode,
                        ''0''              AS CreationUID,
                        ''0''              AS UpdateUID,
                        exp_hold_data1.source_record_id,
                        row_number() over (PARTITION BY exp_hold_data1.source_record_id ORDER BY exp_hold_data1.source_record_id) AS RNK
              FROM      exp_hold_data1
              LEFT JOIN LKP_CLASSIFICATION LKP_1
              ON        LKP_1.PolicyNumber = exp_hold_data1.ClaimIdentifier
              LEFT JOIN LKP_CLASSIFICATION LKP_2
              ON        LKP_2.PolicyNumber = exp_hold_data1.PolicyNumber QUALIFY RNK = 1 );
    -- Component OUT_NAIIPCI_PA_claim, Type TARGET
    INSERT INTO DB_T_PROD_COMN.OUT_NAIIPCI_PA
                (
                            CompanyNumber,
                            LOB,
                            StateOfPrincipalGarage,
                            CallYear,
                            AccountingYear,
                            ExpPeriodYear,
                            ExpPeriodMonth,
                            ExpPeriodDay,
                            CoverageCode,
                            ClassificationCode,
                            TypeOfLossCode,
                            TerritoryCode,
                            ZipCode,
                            PolicyEffectiveYear,
                            StateExceptionCode,
                            AnnualStatementLOB,
                            DeductibleIndicatorCode,
                            DeductibleAmount,
                            SublineCode,
                            ManufactureModelYear,
                            AgeGroupCode,
                            AntiTheftCode,
                            DayTimeRunninglampCode,
                            DefenseDriverCode,
                            ExceptionBCode,
                            PolicyTerm,
                            PenaltyPoints,
                            PolicyLowerLimit,
                            PolicyUpperLimit,
                            PolicyIDCode,
                            PassiveRestraintCode,
                            RatingZoneCode,
                            TerminalZoneCode,
                            ForgivenessCode,
                            ClaimNumber,
                            ClaimantIdentifier,
                            WrittenExposure,
                            WrittenPremium,
                            PaidLosses,
                            PaidClaims,
                            PaidAllocatedLossAdjExp,
                            OutStandingLosses,
                            OutStandingClaims,
                            OutStandingAllocLossAdjExp,
                            PolicyNumber,
                            PolicyPeriodID,
                            CreationTS,
                            CreationUID,
                            UpdateTS,
                            UpdateUID,
                            PolicyIdentifier,
                            VIN,
                            ExposureNumber
                )
    SELECT EXPTRANS4.o_CompanyNumber            AS CompanyNumber,
           EXPTRANS4.LOB                        AS LOB,
           EXPTRANS4.o_StateOfPrincipalGarage   AS StateOfPrincipalGarage,
           EXPTRANS4.CallYear                   AS CallYear,
           EXPTRANS4.AccountingYear             AS AccountingYear,
           EXPTRANS4.o_ExpPeriodYear            AS ExpPeriodYear,
           EXPTRANS4.o_ExpPeriodMonth           AS ExpPeriodMonth,
           EXPTRANS4.o_ExpPeriodDay             AS ExpPeriodDay,
           EXPTRANS4.o_CoverageCode             AS CoverageCode,
           EXPTRANS4.Classification_out         AS ClassificationCode,
           EXPTRANS4.o_TypeofLossCode           AS TypeOfLossCode,
           EXPTRANS4.o_TerritoryCode            AS TerritoryCode,
           EXPTRANS4.o_Zipcode                  AS ZipCode,
           EXPTRANS4.o_Policy_Eff_Yr            AS PolicyEffectiveYear,
           EXPTRANS4.StateExceptionCode         AS StateExceptionCode,
           EXPTRANS4.o_Ann_Stmt_LOB             AS AnnualStatementLOB,
           EXPTRANS4.o_Ded_Ind_Code             AS DeductibleIndicatorCode,
           EXPTRANS4.o_DeductibleAmount         AS DeductibleAmount,
           EXPTRANS4.SublineCode                AS SublineCode,
           EXPTRANS4.o_Mf_MDL_Yr                AS ManufactureModelYear,
           EXPTRANS4.AgeGroupCode               AS AgeGroupCode,
           EXPTRANS4.AntiTheftCode              AS AntiTheftCode,
           EXPTRANS4.DayTimeRunninglampCode     AS DayTimeRunninglampCode,
           EXPTRANS4.Df_Drv_Code                AS DefenseDriverCode,
           EXPTRANS4.ExceptionBCode             AS ExceptionBCode,
           EXPTRANS4.o_PolicyTerm               AS PolicyTerm,
           EXPTRANS4.PenaltyPoints              AS PenaltyPoints,
           EXPTRANS4.PolicyLowerLimit           AS PolicyLowerLimit,
           EXPTRANS4.PolicyUpperLimit           AS PolicyUpperLimit,
           EXPTRANS4.PolicyIDCode               AS PolicyIDCode,
           EXPTRANS4.PassiveRestraintCode       AS PassiveRestraintCode,
           EXPTRANS4.o_Ratingzonecode           AS RatingZoneCode,
           EXPTRANS4.o_Terminalzonecode         AS TerminalZoneCode,
           EXPTRANS4.ForgivenessCode            AS ForgivenessCode,
           EXPTRANS4.ClaimNumber                AS ClaimNumber,
           EXPTRANS4.ClaimantIdentifier         AS ClaimantIdentifier,
           EXPTRANS4.o_WrittenExposure          AS WrittenExposure,
           EXPTRANS4.o_WrittenPremium           AS WrittenPremium,
           EXPTRANS4.o_PaidLosses               AS PaidLosses,
           EXPTRANS4.PaidClaims                 AS PaidClaims,
           EXPTRANS4.o_PaidALAE                 AS PaidAllocatedLossAdjExp,
           EXPTRANS4.o_OutStandingLosses        AS OutStandingLosses,
           EXPTRANS4.OutStandingClaims          AS OutStandingClaims,
           EXPTRANS4.OutStandingAllocLossAdjExp AS OutStandingAllocLossAdjExp,
           EXPTRANS4.PolicyNumber               AS PolicyNumber,
           EXPTRANS4.PolicyPeriodID             AS PolicyPeriodID,
           EXPTRANS4.CreateTS                   AS CreationTS,
           EXPTRANS4.CreationUID                AS CreationUID,
           EXPTRANS4.UpdateTS                   AS UpdateTS,
           EXPTRANS4.UpdateUID                  AS UpdateUID,
           EXPTRANS4.PolilcyIdentifier          AS PolicyIdentifier,
           EXPTRANS4.o_VIN                      AS VIN,
           EXPTRANS4.o_ExposureNumber           AS ExposureNumber
    FROM   EXPTRANS4;
    
    -- PIPELINE END FOR 1
    -- PIPELINE START FOR 2
    -- Component SQ_pc_policyperiod, Type SOURCE
    CREATE
    OR
    REPLACE TEMPORARY TABLE SQ_pc_policyperiod AS
    (
           SELECT
                  /* adding column aliases to ensure proper downstream column references */
                  $1  AS CompanyNumber,
                  $2  AS LOB,
                  $3  AS StateOfPrincipalGarage,
                  $4  AS CallYear,
                  $5  AS AccountingYear,
                  $6  AS ExpPeriodYear,
                  $7  AS ExpPeriodMonth,
                  $8  AS ExpPeriodDay,
                  $9  AS CoverageCode,
                  $10 AS Age_Flag,
                  $11 AS StateTypecode,
                  $12 AS VehicleTypecode,
                  $13 AS RateDriverClass_alfa,
                  $14 AS Radiusofuse_alfa,
                  $15 AS Tonnage_alfa,
                  $16 AS PolicyTypecode,
                  $17 AS CityInternal,
                  $18 AS Veh_Cnt_Fr_ID,
                  $19 AS PatternCode,
                  $20 AS TerritoryCode,
                  $21 AS Zipcode,
                  $22 AS Policy_Eff_Yr,
                  $23 AS Ded_Ind_Code,
                  $24 AS DeductibleAmount,
                  $25 AS Mf_MDL_Yr,
                  $26 AS Df_Drv_Code,
                  $27 AS PolicyTerm,
                  $28 AS PP_PeriodEnd,
                  $29 AS PP_EditEffDate,
                  $30 AS PolicyNumber,
                  $31 AS PolicyPeriodID,
                  $32 AS PolicyIdentifier,
                  $33 AS VIN,
                  $34 AS Cov_pattern_code,
                  $35 AS Cov_patternCODE,
                  $36 AS WrittenPremium,
                  $37 AS Classification,
                  $38 AS ID,
                  $39 AS lkp_defdrv_Branchid,
                  $40 AS lkp_terr_NAIPCICODE_ALFA,
                  $41 AS lkp_age_drv_VEH_CNT,
                  $42 AS lkp_ag_drv_Age_flag,
                  $43 AS source_record_id
           FROM   (
                           SELECT   SRC.*,
                                    row_number() over (ORDER BY 1) AS source_record_id
                           FROM     ( WITH hist AS
                                    (
                                             SELECT   POLICY_TERM_NBR,
                                                      POLICY_MODEL_NBR,
                                                      policy_nbr,
                                                      gl_extr_yr
                                             FROM     DB_T_PROD_COMN.gw_prem_trans_gl_hist
                                             WHERE    gl_extr_yr=EXTRACT(YEAR FROM CAST(:PC_EOY AS TIMESTAMP))
                                             GROUP BY 1,
                                                      2,
                                                      3,
                                                      4 )
                           SELECT    pc_policyperiod_src.COMPANYNUMBER,
                                     pc_policyperiod_src.LOB,
                                     pc_policyperiod_src.StateOfPrincipalGarage,
                                     pc_policyperiod_src.CALLYEAR,
                                     pc_policyperiod_src.ACCOUNTINGYEAR,
                                     pc_policyperiod_src.ExpPeriodYear,
                                     pc_policyperiod_src.ExpPeriodMonth,
                                     pc_policyperiod_src.ExpPeriodDay,
                                     pc_policyperiod_src.CoverageCode,
                                     pc_policyperiod_src.AGE_FLAG,
                                     DB_T_SHRD_PROD.STATE,
                                     pc_policyperiod_src.vehicletype,
                                     pc_policyperiod_src.RATEDRIVERCLASS_ALFA,
                                     pc_policyperiod_src.Radius,
                                     pc_policyperiod_src.Tonnage,
                                     pc_policyperiod_src.Poltype,
                                     pc_policyperiod_src.City,
                                     pc_policyperiod_src.VEH_CNT,
                                     pc_policyperiod_src.df_drv,
                                     lkp_terr_NAIPCICODE_ALFA TERRITORYCODE,
                                     pc_policyperiod_src.ZipCode,
                                     pc_policyperiod_src.POLICY_EFF_YR,
                                     pc_policyperiod_src.DED_IND_CODE,
                                     CAST(pc_policyperiod_src.DEDUCTIBLE_AMT AS VARCHAR(10)),
                                     trim(pc_policyperiod_src.MF_MDL_YR),
                                     pc_policyperiod_src.DF_DRV_CODE,
                                     ROUND(pc_policyperiod_src.POLICY_TERM),
                                     pc_policyperiod_src.PeriodEnd,
                                     pc_policyperiod_src.EditEffectiveDate,
                                     pc_policyperiod_src.POLICYNUMBER,
                                     pc_policyperiod_src.PolicyPeriodID,
                                     pc_policyperiod_src.PolicyIdentifier,
                                     pc_policyperiod_src.VIN,
                                     UPPER(pc_policyperiod_src.PATTERNCODE)     PATTERNCODE,
                                     UPPER(pc_policyperiod_src.PATTERNCODE_cov) PATTERNCODE_cov,
                                     SUM(pc_policyperiod_src.Premium)           Premium,
                                     pc_policyperiod_src.classification,
                                     pc_policyperiod_src.id,
                                     CASE
                                               WHEN lkp_defensive_driver.Branchid IS NULL THEN ''1''
                                               ELSE ''7''
                                     END                      AS lkp_defdrv_Branchid,
                                     lkp_terr.NAIPCICODE_ALFA AS lkp_terr_NAIPCICODE_ALFA,
                                     lkp_age_driver.VEH_CNT   AS lkp_age_drv_VEH_CNT,
                                     lkp_age_driver.Age_flag  AS lkp_ag_drv_Age_flag
                           FROM     (
                                              SELECT   COMPANYNUMBER,
                                                       LOB,
                                                       StateOfPrincipalGarage,
                                                       CALLYEAR,
                                                       ACCOUNTINGYEAR,
                                                       ExpPeriodYear,
                                                       ExpPeriodMonth,
                                                       ExpPeriodDay,
                                                       CoverageCode,
                                                       AGE_FLAG,
                                                       DB_T_SHRD_PROD.STATE ,
                                                       vehicletype,
                                                       RATEDRIVERCLASS_ALFA,
                                                       (Radius)Radius,
                                                       Tonnage,
                                                       Poltype,
                                                       City,
                                                       VEH_CNT,
                                                       df_drv,
                                                       '''' TERRITORYCODE,
                                                       ZipCode,
                                                       POLICY_EFF_YR,
                                                       DED_IND_CODE,
                                                       /*round(DEDUCTIBLE_AMT,0)*/
                                                       DEDUCTIBLE_AMT,
                                                       MF_MDL_YR,
                                                       DF_DRV_CODE,
                                                       POLICY_TERM,
                                                       PeriodEnd,
                                                       EditEffectiveDate,
                                                       POLICYNUMBER,
                                                       PolicyPeriodID,
                                                       PolicyIdentifier,
                                                       COALESCE(VIN,''0'')VIN,
                                                       PATTERNCODE,
                                                       CASE
                                                                WHEN UPPER(PATTERNCODE_cov) IN (''PAACCIDENTWAIVER_ALFA'',
                                                                                                ''PAADD_ALFA'' ) THEN UPPER(PATTERNCODE_cov)
                                                                ELSE NULL
                                                       END          PATTERNCODE_cov,
                                                       SUM(Premium) Premium,
                                                       classification,
                                                       id ,
                                                       optioncode_stg
                                              FROM     (
                                                                       SELECT DISTINCT peo.optioncode_stg,
                                                                                       CASE
                                                                                                       WHEN UWC.PUBLICID_stg=''AMI'' THEN ''0005''
                                                                                                       WHEN UWC.PUBLICID_stg=''AMG'' THEN ''0196''
                                                                                                       WHEN UWC.PUBLICID_stg=''AIC'' THEN ''0050''
                                                                                                       WHEN UWC.PUBLICID_stg=''AGI'' THEN ''0318''
                                                                                       END  AS COMPANYNUMBER,
                                                                                       ''01'' AS LOB,(
                                                                                       CASE
                                                                                                       WHEN (
                                                                                                                                       ST2.typecode_stg=JD.typecode_stg) THEN ST2.typecode_stg
                                                                                                       ELSE JD.typecode_stg
                                                                                       END) AS TYPECODE,
                                                                                       CASE TYPECODE
                                                                                                       WHEN ''AL'' THEN ''01''
                                                                                                       WHEN ''GA'' THEN ''10''
                                                                                                       WHEN ''MS'' THEN ''23''
                                                                                       END                                                StateOfPrincipalGarage,
                                                                                       extract(YEAR FROM CAST(:PC_EOY AS TIMESTAMP )) + 1 CALLYEAR,
                                                                                       extract(YEAR FROM CAST (:PC_EOY AS TIMESTAMP ))    ACCOUNTINGYEAR,
                                                                                       ''0000''                                             ExpPeriodYear,
                                                                                       ''00''                                               ExpPeriodMonth,
                                                                                       ''00''                                               ExpPeriodDay,
                                                                                       COALESCE(PVC.PATTERNCODE_stg, ppc.patterncode_stg) PATTERNCODE_COV,
                                                                                       CASE
                                                                                                       WHEN UPPER(COALESCE(PVC.PATTERNCODE_stg, ppc.patterncode_stg))IN (''PAADD_ALFA'',
                                                                                                                                                                         ''PABI_ALFA'') THEN ''001''
                                                                                                       WHEN UPPER(PVC.PATTERNCODE_stg)=''PAMEDICALPAYMENTS_ALFA'' THEN ''003''
                                                                                                       WHEN UPPER(PVC.PATTERNCODE_stg) IN ( ''PAPROPERTYDAMAGE_ALFA'' ,
                                                                                                                                           ''PAPROPERTYDAMAGEVEH_ALFA'') THEN ''004''
                                                                                                       WHEN UPPER(PVC.PATTERNCODE_stg) IN (''PASINGLELIMITS_ALFA'',
                                                                                                                                           ''PASINGLELIMITSINJURY_ALFA'',
                                                                                                                                           ''PASINGLELIMITSPROPERTY_ALFA'',
                                                                                                                                           ''PASINGLELIMITSVEHICLE_ALFA'',
                                                                                                                                           ''PAUMSLINJURY_ALFA'') THEN ''006''
                                                                                                       WHEN UPPER(COALESCE(PVC.PATTERNCODE_stg, ppc.patterncode_stg)) IN (''PAEXTENDEDNONOWNED_ALFA'',
                                                                                                                                                                          ''PALOSSOFINCOME_ALFA'',
                                                                                                                                                                          ''PAGOVERNMENT_ALFA'',
                                                                                                                                                                          ''PAUNINSUREDMOTORISTSL_ALFA'',
                                                                                                                                                                          ''PASPLITBIPDLIMITS_ALFA'') THEN ''009''
                                                                                                       WHEN UPPER(JD.TYPECODE_stg) IN (''AL'',
                                                                                                                                       ''MS'')
                                                                                                       AND             UPPER(PVC.PATTERNCODE_stg) =''PAUNINSUREDMOTORISTBI_ALFA'' THEN ''201''
                                                                                                       WHEN UPPER(JD.TYPECODE_stg) IN (''AL'',
                                                                                                                                       ''MS'')
                                                                                                       AND             UPPER(PVC.PATTERNCODE_stg) IN ( ''PAUNINSUREDMOTORISTPD_ALFA'',
                                                                                                                                                      ''PAUMSLVEHICLE'') THEN ''211''
                                                                                                       WHEN UPPER(JD.TYPECODE_stg) IN (''GA'')
                                                                                                       AND             (
                                                                                                                                       (
                                                                                                                                                       UPPER(PVC.PATTERNCODE_stg) IN ( ''PAUNINSUREDMOTORISTPD_ALFA'',
                                                                                                                                                                                      ''PAUMSLVEHICLE'',
                                                                                                                                                                                      ''PAUNINSUREDMOTORISTBI_ALFA'') )
                                                                                                                       OR              (
                                                                                                                                                       UPPER(PVC.PATTERNCODE_stg) IN (''PAUNINSUREDMOTORISTCOMMON_ALFA'' )
                                                                                                                                       AND             (
                                                                                                                                                                       UPPER(peo.optioncode_stg) =''ADDEDON''
                                                                                                                                                       OR              peo.optioncode_stg IS NULL)) ) THEN ''259''
                                                                                                       WHEN UPPER(JD.TYPECODE_stg) IN (''AL'',
                                                                                                                                       ''MS'')
                                                                                                       AND             UPPER(PVC.PATTERNCODE_stg) = ''PAUNINSUREDMOTORISTCOMMON_ALFA'' THEN ''221''
                                                                                                       WHEN UPPER(JD.TYPECODE_stg) IN (''GA'')
                                                                                                       AND             UPPER(PVC.PATTERNCODE_stg) = ''PAUNINSUREDMOTORISTCOMMON_ALFA''
                                                                                                       AND             peo.optioncode_stg =''REDUCED'' THEN ''249''
                                                                                                       WHEN UPPER(PVC.PATTERNCODE_stg) = ''PACOMPREHENSIVECOV'' THEN ''810''
                                                                                                       WHEN UPPER(PVC.PATTERNCODE_stg) = ''PALEASELOAN_ALFA'' THEN ''812''
                                                                                                       WHEN UPPER(PVC.PATTERNCODE_stg) =''PAFIREANDTHEFT_ALFA'' THEN ''821''
                                                                                                       WHEN UPPER(COALESCE(PVC.PATTERNCODE_stg, ppc.patterncode_stg)) IN ( ''PACAMPERSHELL_ALFA'',
                                                                                                                                                                          ''PACUSTOMIZED_ALFA'',
                                                                                                                                                                          ''PAACCIDENTWAIVER_ALFA'') THEN ''845''
                                                                                                       WHEN UPPER(PVC.PATTERNCODE_stg) = ''PATOWINGLABORCOV'' THEN ''846''
                                                                                                       WHEN UPPER(PVC.PATTERNCODE_stg) = ''PALOSSOFUSECOV_ALFA'' THEN ''848''
                                                                                                       WHEN UPPER(PVC.PATTERNCODE_stg) =''PACOLLISIONCOV'' THEN ''860''
                                                                                       END AS CoverageCode,
                                                                                       ''''     AGE_FLAG, (
                                                                                       CASE
                                                                                                       WHEN (
                                                                                                                                       ST2.typecode_stg=JD.typecode_stg) THEN ST2.typecode_stg
                                                                                                       ELSE JD.typecode_stg
                                                                                       END) AS TYPECODE1,
                                                                                       CASE TYPECODE1
                                                                                                       WHEN ''AL'' THEN ''01''
                                                                                                       WHEN ''GA'' THEN ''10''
                                                                                                       WHEN ''MS'' THEN ''23''
                                                                                       END                                 AS STATE ,
                                                                                       VT.TYPECODE_stg                        vehicletype,
                                                                                       pv.RATEDRIVERCLASS_ALFA_stg         AS RATEDRIVERCLASS_ALFA ,
                                                                                       rad.typecode_stg                    AS Radius,
                                                                                       ton.typecode_stg                    AS Tonnage,
                                                                                       pctl_papolicytype_alfa.TypeCode_stg    Poltype,
                                                                                       CASE
                                                                                                       WHEN ST2.TYPECODE_stg=JD.TYPECODE_stg THEN plveh.CityInternal_stg
                                                                                                       ELSE plpol.CityInternal_stg
                                                                                       END                  AS City,
                                                                                       0                       VEH_CNT,
                                                                                       PAM .PATTERNCODE_stg    df_drv,
                                                                                       CASE
                                                                                                       WHEN ST2.TYPECODE_stg=JD.TYPECODE_stg THEN LEFT(plveh.PostalCodeInternal_stg,5)
                                                                                                       ELSE LEFT(plpol.PostalCodeInternal_stg,5)
                                                                                       END AS ZipCode,
                                                                                       CASE
                                                                                                       WHEN PCTL_JOB.TYPECODE_stg=''Cancellation'' THEN YEAR(pc_policyperiod.CANCELLATIONDATE_stg)
                                                                                                       ELSE YEAR(pc_policyperiod.PERIODSTART_stg)
                                                                                       END AS POLICY_EFF_YR,
                                                                                       CASE
                                                                                                       WHEN UPPER(PVC.PATTERNCODE_stg) IN (''PACOMPREHENSIVECOV'',
                                                                                                                                           ''PAFIREANDTHEFT_ALFA'' ,
                                                                                                                                           ''PACOLLISIONCOV'') THEN ''D''
                                                                                                       ELSE ''0''
                                                                                       END AS DED_IND_CODE,
                                                                                       COALESCE(
                                                                                       CASE
                                                                                                       WHEN UPPER(PVC.PATTERNCODE_stg) IN (''PACOMPREHENSIVECOV'',
                                                                                                                                           ''PAFIREANDTHEFT_ALFA'' ,
                                                                                                                                           ''PACOLLISIONCOV'') THEN CAST(CAST(peo.Value_stg AS INTEGER) AS VARCHAR(10))
                                                                                                       ELSE ''0000''
                                                                                       END ,0) AS DEDUCTIBLE_AMT,
                                                                                       COALESCE(
                                                                                       CASE
                                                                                                       WHEN UPPER(PVC.PATTERNCODE_stg) IN (''PACOMPREHENSIVECOV'',
                                                                                                                                           ''PALEASELOAN_ALFA'',
                                                                                                                                           ''PAFIREANDTHEFT_ALFA'' ,
                                                                                                                                           ''PACAMPERSHELL_ALFA'',
                                                                                                                                           ''PACUSTOMIZED_ALFA'',
                                                                                                                                           ''PATOWINGLABORCOV'',
                                                                                                                                           ''PALOSSOFUSECOV_ALFA'',
                                                                                                                                           ''PACOLLISIONCOV'') THEN CAST(PV.YEAR_stg AS VARCHAR(4))
                                                                                                       ELSE ''0000''
                                                                                       END ,0)                 AS MF_MDL_YR ,
                                                                                       pc_patransaction.id_stg    PC_PATRANSACTION_ID,
                                                                                       ''''                         DF_DRV_CODE,
                                                                                       ABS(ROUND(
                                                                                       CASE
                                                                                                       WHEN pc_policyperiod.CancellationDate_stg IS NOT NULL THEN
                                                                                                                       CASE
                                                                                                                                       WHEN (
                                                                                                                                                                       MONTHS_BETWEEN(CAST(pc_policyperiod.Editeffectivedate_stg AS DATE), CAST(pc_policyperiod.CancellationDate_stg AS DATE)))=0 THEN 1
                                                                                                                                       ELSE (MONTHS_BETWEEN (CAST( pc_policyperiod.Editeffectivedate_stg AS                         DATE),CAST( pc_policyperiod.CancellationDate_stg AS DATE)))
                                                                                                                       END
                                                                                                       ELSE (MONTHS_BETWEEN (CAST( pc_policyperiod.Editeffectivedate_stg AS DATE), CAST(pc_policyperiod.PeriodEnd_stg AS DATE)))
                                                                                       END,1))                               AS POLICY_TERM,
                                                                                       pc_policyperiod.PeriodEnd_stg         AS PeriodEnd,
                                                                                       pc_policyperiod.EditEffectiveDate_stg AS EditEffectiveDate,
                                                                                       pc_policyperiod.POLICYNUMBER_stg      AS POLICYNUMBER,
                                                                                       pc_policyperiod.id_stg                   PolicyPeriodID,
                                                                                       PC_JOB.JOBNUMBER_stg                     PolicyIdentifier,
                                                                                       PV.VIN_stg                            AS VIN,
                                                                                       ''''                                       PATTERNCODE,
                                                                                       PVC.personalvehicle_stg               AS personalvehicle,
                                                                                       pc_patransaction.Amount_stg           AS Premium,
                                                                                       CASE
                                                                                                       WHEN pc_policyperiod.EditEffectiveDate_stg >= pc_policyperiod.ModelDate_stg
                                                                                                       AND             pc_policyperiod.EditEffectiveDate_stg>= COALESCE(PT.ConfirmationDate_alfa_stg, CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP)) THEN CAST(pc_policyperiod.EditEffectiveDate_stg AS TIMESTAMP)
                                                                                                       WHEN COALESCE(PT.ConfirmationDate_alfa_stg, CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP)) >= pc_policyperiod.ModelDate_stg THEN CAST(COALESCE(PT.ConfirmationDate_alfa_stg, CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP)) AS TIMESTAMP)
                                                                                                       ELSE CAST( pc_policyperiod.ModelDate_stg AS TIMESTAMP)
                                                                                       END                                                                                                                                                                                                        date_filter,
                                                                                       ''''                                                                                                                                                                                                        classification,
                                                                                       pc_policyperiod.id_stg                                                                                                                                                                                                        AS id,
                                                                                       row_number() over(PARTITION BY pc_patransaction.id_stg, ExpandedCostTable.id_stg ORDER BY COALESCE(pv.ExpirationDate_stg,CAST( ''9999-12-31 23:59:59.99999'' AS TIMESTAMP)) DESC, COALESCE(pvc.ExpirationDate_stg, CAST(''9999-12-31 23:59:59.99999'' AS TIMESTAMP))DESC , COALESCE(ppc.ExpirationDate_stg, CAST(''9999-12-31 23:59:59.99999'' AS TIMESTAMP))DESC )    rnk
                                                                       FROM            DB_T_PROD_STAG.pc_patransaction pc_patransaction
                                                                       JOIN            DB_T_PROD_STAG.PC_POLICYPERIOD PC_POLICYPERIOD
                                                                       ON              pc_patransaction.BranchID_stg = pc_policyperiod.ID_stg
                                                                       AND             pc_patransaction.ExpirationDate_stg IS NULL
                                                                       JOIN            hist
                                                                       ON              policynumber_stg=policy_nbr
                                                                       AND             termnumber_stg=POLICY_TERM_NBR
                                                                       AND             modelnumber_stg=POLICY_MODEL_NBR
                                                                       JOIN            DB_T_PROD_STAG.PC_POLICYLINE PC_POLICYLINE
                                                                       ON              pc_policyperiod.id_stg = pc_policyline.BranchID_stg
                                                                       AND             pc_policyline.ExpirationDate_stg IS NULL
                                                                       JOIN            DB_T_PROD_STAG.PCTL_PAPOLICYTYPE_ALFA PCTL_PAPOLICYTYPE_ALFA
                                                                       ON              pc_policyline.PAPolicyType_alfa_stg = pctl_papolicytype_alfa.ID_stg
                                                                       AND             pctl_papolicytype_alfa.TypeCode_stg IN (''PPV'',
                                                                                                                               ''COMMERCIAL'',
                                                                                                                               ''PPV2'')
                                                                       JOIN            DB_T_PROD_STAG.pctl_policyperiodstatus pctl_policyperiodstatus
                                                                       ON              pc_policyperiod.status_stg=pctl_policyperiodstatus.ID_stg
                                                                       JOIN            DB_T_PROD_STAG.PC_POLICYTERM pt
                                                                       ON              pt.ID_stg = pc_policyperiod.PolicyTermID_stg
                                                                       JOIN            DB_T_PROD_STAG.PC_JOB PC_JOB
                                                                       ON              pc_policyperiod.JobID_stg = pc_job.ID_stg
                                                                       JOIN            DB_T_PROD_STAG.PCTL_JOB PCTL_JOB
                                                                       ON              pc_job.Subtype_stg = pctl_job.ID_stg
                                                                       LEFT JOIN       DB_T_PROD_STAG.PC_EFFECTIVEDATEDFIELDS eff
                                                                       ON              eff.BranchID_stg = pc_policyperiod.ID_stg
                                                                       AND             eff.expirationdate_stg IS NULL
                                                                       LEFT JOIN       DB_T_PROD_STAG.PC_POLICYLOCATION plpol
                                                                       ON              eff.primarylocation_stg = plpol.id_stg
                                                                       AND             plpol.ExpirationDate_stg IS NULL
                                                                       LEFT JOIN       DB_T_PROD_STAG.PC_TERRITORYCODE TC1
                                                                       ON              TC1.POLICYLOCATION_stg=plpol.fixedid_stg
                                                                       AND             TC1.BRANCHID_stg=pc_policyperiod.ID_stg
                                                                       AND             tc1.ExpirationDate_stg IS NULL
                                                                       JOIN            DB_T_PROD_STAG.pc_pacost ExpandedCostTable
                                                                       ON              pc_patransaction.pacost_stg = expandedcosttable.ID_stg
                                                                       LEFT JOIN       DB_T_PROD_STAG.pc_personalvehiclecov pvc
                                                                       ON              ExpandedCostTable.PersonalVehicleCov_stg = pvc.FixedID_stg
                                                                       AND             ExpandedCostTable.BranchID_stg=pvc.BranchID_stg
                                                                       LEFT JOIN       DB_T_PROD_STAG.PC_PERSONALVEHICLE pv
                                                                       ON              pv.branchid_stg = pc_policyperiod.id_stg
                                                                       AND             pv.fixedID_stg = pvc.PersonalVehicle_stg
                                                                       LEFT JOIN       DB_T_PROD_STAG.PCTL_PAPOLICYTYPE_ALFA pt1
                                                                       ON              pt1.ID_stg = pc_policyline.PAPolicyType_alfa_stg
                                                                       LEFT JOIN       DB_T_PROD_STAG.pctl_vehicletype vt
                                                                       ON              vt.ID_stg = pv.VehicleType_stg
                                                                       LEFT JOIN       DB_T_PROD_STAG.PC_personalautocov PPC
                                                                       ON
                                                                                       /*PPC.FIXEDID_stg=PERSONALAUTOCOV_ALFA and*/
                                                                                       ppc.branchid_stg=pc_policyperiod.id_stg
                                                                       JOIN            DB_T_PROD_STAG.PC_UWCOMPANY UWC
                                                                       ON              pc_policyperiod.UWCOMPANY_stg=UWC.ID_stg
                                                                       JOIN            DB_T_PROD_STAG.PCTL_JURISDICTION JD
                                                                       ON              pc_policyperiod.BASESTATE_stg=JD.ID_stg
                                                                       LEFT JOIN       DB_T_PROD_STAG.PC_POLICYLOCATION plveh
                                                                       ON              plveh.ID_stg = pv.GarageLocation_stg
                                                                       AND             plveh.ExpirationDate_stg IS NULL
                                                                       LEFT JOIN       DB_T_PROD_STAG.PC_TERRITORYCODE TC
                                                                       ON              TC.BRANCHID_stg=PV.BRANCHID_stg
                                                                       AND             TC.POLICYLOCATION_stg=plveh.ID_stg
                                                                       AND             tc.ExpirationDate_stg IS NULL
                                                                       LEFT JOIN       DB_T_PROD_STAG.PCTL_STATE st2
                                                                       ON              st2.ID_stg = plveh.StateInternal_stg
                                                                       LEFT JOIN       DB_T_PROD_STAG.PC_ETLCLAUSEPATTERN pep
                                                                       ON              PVC.PATTERNCODE_stg=pep.PATTERNID_stg
                                                                       LEFT JOIN       DB_T_PROD_STAG.PC_ETLCOVTERMPATTERN petp
                                                                       ON              petp.CLAUSEPATTERNID_stg=pep.ID_stg
                                                                       AND             petp.COVTERMTYPE_stg <> ''bit''
                                                                       AND             petp.COVTERMTYPE_stg=''Option''
                                                                       AND             petp.MODELTYPE_stg=''Deductible''
                                                                       LEFT JOIN       DB_T_PROD_STAG.PC_ETLCOVTERMOPTION peo
                                                                       ON              peo.PATTERNID_stg=PVC.CHOICETERM1_stg
                                                                       LEFT JOIN       DB_T_PROD_STAG.PC_PAMODIFIER PAM
                                                                       ON              pc_policyperiod.ID_stg=PAM.BRANCHID_stg
                                                                       AND             PAM .PATTERNCODE_stg =''PAAffinityDiscount_alfa''
                                                                       AND             pam.ExpirationDate_stg IS NULL
                                                                       LEFT JOIN       DB_T_PROD_STAG.pctl_radiusofuse_alfa rad
                                                                       ON              rad.ID_stg = pv.RadiusOfUse_alfa_stg
                                                                       LEFT JOIN       DB_T_PROD_STAG.pctl_tonnage_alfa ton
                                                                       ON              ton.ID_stg = pv.Tonnage_alfa_stg
                                                                       WHERE           NOT EXISTS
                                                                                       (
                                                                                              SELECT pc_policyperiod2.policynumber_stg
                                                                                              FROM   DB_T_PROD_STAG.PC_POLICYPERIOD pc_policyperiod2
                                                                                              JOIN   DB_T_PROD_STAG.PC_POLICYTERM pt2
                                                                                              ON     pt2.ID_stg = pc_policyperiod2.PolicyTermID_stg
                                                                                              JOIN   DB_T_PROD_STAG.PC_POLICYLINE PC_POLICYLINE
                                                                                              ON     pc_policyperiod2.id_stg = pc_policyline.BranchID_stg
                                                                                              AND    pc_policyline.ExpirationDate_stg IS NULL
                                                                                              JOIN   DB_T_PROD_STAG.PCTL_PAPOLICYTYPE_ALFA PCTL_PAPOLICYTYPE_ALFA
                                                                                              ON     pc_policyline.PAPolicyType_alfa_stg = pctl_papolicytype_alfa.ID_stg
                                                                                              AND    pctl_papolicytype_alfa.TypeCode_stg IN (''PPV'',
                                                                                                                                             ''COMMERCIAL'',
                                                                                                                                             ''PPV2'')
                                                                                              JOIN   DB_T_PROD_STAG.PC_JOB job2
                                                                                              ON     job2.ID_stg = pc_policyperiod2.jobID_stg
                                                                                              JOIN   DB_T_PROD_STAG.PC_POLICYTERM pt
                                                                                              ON     pt.ID_stg = pc_policyperiod2.PolicyTermID_stg
                                                                                              JOIN   DB_T_PROD_STAG.PCTL_JOB pctl_job2
                                                                                              ON     pctl_job2.ID_stg = job2.Subtype_stg
                                                                                              WHERE  pctl_job2.Name_stg =''Renewal''
                                                                                              AND    (
                                                                                                            pt.ConfirmationDate_alfa_stg > :PC_EOY
                                                                                                     OR     pt.ConfirmationDate_alfa_stg IS NULL)
                                                                                              AND    pc_policyperiod2.PolicyNumber_stg = pc_policyperiod.PolicyNumber_stg
                                                                                              AND    pc_policyperiod2.TermNumber_stg = pc_policyperiod.TermNumber_stg)
                                                                       AND             date_filter BETWEEN CAST(:PC_BOY AS TIMESTAMP) AND             CAST(:PC_EOY AS TIMESTAMP) )src
                                              WHERE    rnk =1
                                              GROUP BY COMPANYNUMBER,
                                                       LOB,
                                                       StateOfPrincipalGarage,
                                                       CALLYEAR,
                                                       ACCOUNTINGYEAR,
                                                       ExpPeriodYear,
                                                       ExpPeriodMonth,
                                                       ExpPeriodDay,
                                                       CoverageCode,
                                                       AGE_FLAG,
                                                       DB_T_SHRD_PROD.STATE ,
                                                       vehicletype,
                                                       RATEDRIVERCLASS_ALFA,
                                                       /* Radius, */
                                                       Tonnage,
                                                       Poltype,
                                                       City,
                                                       VEH_CNT,
                                                       df_drv,
                                                       optioncode_stg,
                                                       ZipCode,
                                                       POLICY_EFF_YR,
                                                       DED_IND_CODE,
                                                       DEDUCTIBLE_AMT,
                                                       MF_MDL_YR,
                                                       DF_DRV_CODE,
                                                       POLICY_TERM,
                                                       PeriodEnd,
                                                       EditEffectiveDate,
                                                       POLICYNUMBER,
                                                       PolicyPeriodID,
                                                       PolicyIdentifier,
                                                       VIN,
                                                       UPPER(PATTERNCODE),
                                                       CASE
                                                                WHEN UPPER(PATTERNCODE_cov) IN (''PAACCIDENTWAIVER_ALFA'',
                                                                                                ''PAADD_ALFA'') THEN UPPER(PATTERNCODE_cov )
                                                                ELSE NULL
                                                       END,
                                                       Radius,
                                                       classification,
                                                       id ) pc_policyperiod_src
                           LEFT JOIN
                                     (
                                            SELECT pc_pavehmodifier.Age_flag AS Age_flag,
                                                   pc_pavehmodifier.VEH_CNT  AS VEH_CNT,
                                                   pc_pavehmodifier.Branchid AS Branchid
                                            FROM   (
                                                             SELECT    VEH_CNT,
                                                                       CASE
                                                                                 WHEN (
                                                                                                     DRIVER_AGE IS NOT NULL) THEN ''Y''
                                                                                 ELSE ''N''
                                                                       END        age_flag ,
                                                                       VEH_CNT.id Branchid
                                                             FROM      (
                                                                                SELECT   PP_VEH_CNT.id_stg       AS id ,
                                                                                         COUNT(DISTINCT vin_stg)    VEH_CNT
                                                                                FROM     DB_T_PROD_STAG.PC_POLICYPERIOD PP_VEH_CNT
                                                                                JOIN     DB_T_PROD_STAG.PC_PERSONALVEHICLE PV
                                                                                ON       PP_VEH_CNT.id_stg=pv.branchid_stg
                                                                                AND      pv.ExpirationDate_stg IS NULL
                                                                                WHERE    fixedid_stg IS NOT NULL
                                                                                GROUP BY PP_VEH_CNT.id_stg )VEH_CNT
                                                             LEFT JOIN
                                                                       (
                                                                                       SELECT DISTINCT PCR.BRANCHID_stg,
                                                                                                       MIN(extract(YEAR FROM CAST(:PC_BOY AS TIMESTAMP))-extract( YEAR FROM CAST(CNT.DATEOFBIRTH_stg AS TIMESTAMP))) AS DRIVER_AGE
                                                                                       FROM            DB_T_PROD_STAG.PC_POLICYCONTACTROLE PCR
                                                                                       JOIN            DB_T_PROD_STAG.PCTL_POLICYCONTACTROLE PCRL
                                                                                       ON              PCRL.ID_stg=PCR.SUBTYPE_stg
                                                                                       JOIN            DB_T_PROD_STAG.PC_CONTACT CNT
                                                                                       ON              CNT.ID_stg=PCR.CONTACTDENORM_stg
                                                                                       WHERE           PCRL.NAME_stg=''PolicyDriver''
                                                                                       GROUP BY        BRANCHID_stg
                                                                                       HAVING          MIN(extract(YEAR FROM CAST(:PC_BOY AS TIMESTAMP))-extract( YEAR FROM CAST(CNT.DATEOFBIRTH_stg AS TIMESTAMP)))<25 ) AGE
                                                             ON        AGE.BRANCHID_stg=VEH_CNT.ID)pc_pavehmodifier) lkp_age_driver
                           ON        pc_policyperiod_src.PolicyPeriodID = lkp_age_driver.Branchid
                           LEFT JOIN
                                     (
                                            SELECT pc_pavehmodifier.NAIPCICODE_ALFA AS NAIPCICODE_ALFA,
                                                   pc_pavehmodifier.ID              AS ID,
                                                   pc_pavehmodifier.VIN             AS VIN
                                            FROM   (
                                                            SELECT   id,
                                                                     COALESCE( vin ,''0'')       VIN ,
                                                                     MAX(NAIIPCICODE_ALFA_new) NAIPCICODE_ALFA
                                                            FROM     (
                                                                                     SELECT DISTINCT PC_POLICYPERIOD.ID_stg                                                                                                                                 AS id,
                                                                                                     COALESCE(VIN_stg, ''0'')                                                                                                                                    VIN ,
                                                                                                     (COALESCE( PTCA.NAIIPCICODE_ALFA,PTCA4.NAIIPCICODE_ALFA,PTCA5.NAIIPCICODE_ALFA, PTCA6.NAIIPCICODE_ALFA,PTCA2.NAIIPCICODE_ALFA,PTCA1.NAIIPCICODE_ALFA ))   NAIIPCICODE_ALFA_new
                                                                                     FROM            DB_T_PROD_STAG.PC_POLICYPERIOD
                                                                                     JOIN            DB_T_PROD_STAG.PC_POLICYLINE
                                                                                     ON              PC_POLICYPERIOD.ID_stg = PC_POLICYLINE.BRANCHID_stg
                                                                                     AND             PC_POLICYLINE.EXPIRATIONDATE_stg IS NULL
                                                                                     JOIN            DB_T_PROD_STAG.PCTL_PAPOLICYTYPE_ALFA
                                                                                     ON              PC_POLICYLINE.PAPOLICYTYPE_ALFA_stg = PCTL_PAPOLICYTYPE_ALFA.ID_stg
                                                                                     AND             PCTL_PAPOLICYTYPE_ALFA.TYPECODE_stg IN (''PPV'',
                                                                                                                                             ''COMMERCIAL'',
                                                                                                                                             ''PPV2'')
                                                                                     JOIN            DB_T_PROD_STAG.PC_POLICYTERM PT
                                                                                     ON              PT.ID_stg = PC_POLICYPERIOD.POLICYTERMID_stg
                                                                                     AND
                                                                                                     CASE
                                                                                                                     WHEN pc_policyperiod.EditEffectiveDate_stg >= pc_policyperiod.ModelDate_stg
                                                                                                                     AND             pc_policyperiod.EditEffectiveDate_stg>= COALESCE(CAST(PT.ConfirmationDate_alfa_stg AS TIMESTAMP), CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP)) THEN pc_policyperiod.EditEffectiveDate_stg
                                                                                                                     WHEN COALESCE(CAST(PT.ConfirmationDate_alfa_stg AS TIMESTAMP),CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP)) >= pc_policyperiod.ModelDate_stg THEN COALESCE(CAST(PT.ConfirmationDate_alfa_stg AS TIMESTAMP),CAST(''1900-01-01 00:00:00.000000'' AS TIMESTAMP))
                                                                                                                     ELSE pc_policyperiod.ModelDate_stg
                                                                                                     END BETWEEN :PC_BOY AND             :PC_EOY
                                                                                     LEFT JOIN       DB_T_PROD_STAG.PC_EFFECTIVEDATEDFIELDS EFF
                                                                                     ON              EFF.BRANCHID_stg = PC_POLICYPERIOD.ID_stg
                                                                                     AND             EFF.EXPIRATIONDATE_stg IS NULL
                                                                                     LEFT JOIN       DB_T_PROD_STAG.PC_POLICYLOCATION PLPOL
                                                                                     ON              EFF.PRIMARYLOCATION_stg = PLPOL.ID_stg
                                                                                     AND             PLPOL.EXPIRATIONDATE_stg IS NULL
                                                                                     LEFT JOIN       DB_T_PROD_STAG.PC_TERRITORYCODE TC1
                                                                                     ON              TC1.POLICYLOCATION_stg=PLPOL.FIXEDID_stg
                                                                                     AND             TC1.BRANCHID_stg=PC_POLICYPERIOD.ID_stg
                                                                                     AND             TC1.EXPIRATIONDATE_stg IS NULL
                                                                                     LEFT JOIN       DB_T_PROD_STAG.PC_PERSONALVEHICLE PV
                                                                                     ON              PV.BRANCHID_stg = PC_POLICYPERIOD.ID_stg
                                                                                     LEFT JOIN       DB_T_PROD_STAG.PCTL_PAPOLICYTYPE_ALFA PT1
                                                                                     ON              PT1.ID_stg = PC_POLICYLINE.PAPOLICYTYPE_ALFA_stg
                                                                                     LEFT JOIN       DB_T_PROD_STAG.PC_POLICYLOCATION PLVEH
                                                                                     ON              PLVEH.ID_stg = PV.GARAGELOCATION_stg
                                                                                     LEFT JOIN       DB_T_PROD_STAG.PC_TERRITORYCODE TC
                                                                                     ON              TC.BRANCHID_stg=PV.BRANCHID_stg
                                                                                     AND             TC.POLICYLOCATION_stg=PLVEH.ID_stg
                                                                                     LEFT JOIN       DB_T_PROD_STAG.PCTL_STATE ST2
                                                                                     ON              ST2.ID_stg = PLVEH.STATEINTERNAL_stg
                                                                                     JOIN            DB_T_PROD_STAG.PCTL_JURISDICTION JD
                                                                                     ON              PC_POLICYPERIOD.BASESTATE_stg=JD.ID_stg
                                                                                     LEFT JOIN
                                                                                                     (
                                                                                                            SELECT NAIIPCICODE_ALFA_stg AS NAIIPCICODE_ALFA,
                                                                                                                   COUNTY_stg           AS COUNTY,
                                                                                                                   TERRITORYCODE_stg    AS TERRITORYCODE,
                                                                                                                   STATE_stg            AS STATE,
                                                                                                                   zipcode_stg          AS zipcode
                                                                                                            FROM   (
                                                                                                                            SELECT   NAIIPCICODE_ALFA_stg,
                                                                                                                                     COUNTY_stg,
                                                                                                                                     TERRITORYCODE_stg,
                                                                                                                                     STATE_stg,
                                                                                                                                     zipcode_stg,
                                                                                                                                     ROW_NUMBER() OVER ( PARTITION BY TERRITORYCODE_stg, COUNTY_stg,STATE_stg,zipcode_stg ORDER BY BEANVERSION_stg DESC, UPDATETIME_stg DESC) AS RNK
                                                                                                                            FROM     DB_T_PROD_STAG.PCX_PATERRITORYCODE_ALFA )A
                                                                                                            WHERE  RNK = 1 ) PTCA
                                                                                     ON              JD.ID_stg=STATE
                                                                                     AND             UPPER(PTCA.TERRITORYCODE
                                                                                                                     || ''-''
                                                                                                                     || PTCA.COUNTY )= UPPER(
                                                                                                     CASE
                                                                                                                     WHEN (
                                                                                                                                                     ST2.TYPECODE_stg=JD.TYPECODE_stg ) THEN TC.CODE_stg
                                                                                                                                                     ||''-''
                                                                                                                                                     || PLVEH.COUNTYINTERNAL_stg
                                                                                                                     ELSE TC1.CODE_stg
                                                                                                                                                     ||''-''
                                                                                                                                                     ||PLPOL.COUNTYINTERNAL_stg
                                                                                                     END)
                                                                                     AND             COALESCE(ptca.zipcode,''~'')= (
                                                                                                     CASE
                                                                                                                     WHEN (
                                                                                                                                                     ST2.TYPECODE_stg=JD.TYPECODE_stg ) THEN LEFT(plveh.postalcodeinternal_stg,5)
                                                                                                                     ELSE LEFT(plpol.postalcodeinternal_stg,5)
                                                                                                     END)
                                                                                     LEFT JOIN
                                                                                                     (
                                                                                                            SELECT NAIIPCICODE_ALFA_stg AS NAIIPCICODE_ALFA,
                                                                                                                   COUNTY_stg           AS COUNTY,
                                                                                                                   TERRITORYCODE_stg    AS TERRITORYCODE,
                                                                                                                   STATE_stg            AS STATE,
                                                                                                                   zipcode_stg          AS zipcode
                                                                                                            FROM   (
                                                                                                                            SELECT   NAIIPCICODE_ALFA_stg,
                                                                                                                                     COUNTY_stg,
                                                                                                                                     TERRITORYCODE_stg,
                                                                                                                                     STATE_stg,
                                                                                                                                     zipcode_stg,
                                                                                                                                     ROW_NUMBER () OVER ( PARTITION BY TERRITORYCODE_stg, COUNTY_stg,STATE_stg,zipcode_stg ORDER BY BEANVERSION_stg DESC, UPDATETIME_stg DESC) AS RNK
                                                                                                                            FROM     DB_T_PROD_STAG.PCX_PATERRITORYCODE_ALFA )A
                                                                                                            WHERE  RNK = 1 ) PTCA4
                                                                                     ON              JD.ID_stg=STATE
                                                                                     AND             UPPER( PTCA4.TERRITORYCODE
                                                                                                                     ||''-''
                                                                                                                     ||PTCA4.COUNTY)=UPPER(TC1.CODE_stg
                                                                                                                     ||''-''
                                                                                                                     || PLPOL.COUNTYINTERNAL_stg)
                                                                                     AND             COALESCE(ptca4.zipcode,''~'')= LEFT(plpol.postalcodeinternal_stg,5)
                                                                                     LEFT JOIN
                                                                                                     (
                                                                                                            SELECT NAIIPCICODE_ALFA_stg AS NAIIPCICODE_ALFA,
                                                                                                                   COUNTY_stg           AS COUNTY,
                                                                                                                   TERRITORYCODE_stg    AS TERRITORYCODE,
                                                                                                                   STATE_stg            AS STATE
                                                                                                            FROM   (
                                                                                                                            SELECT   NAIIPCICODE_ALFA_stg,
                                                                                                                                     COUNTY_stg,
                                                                                                                                     TERRITORYCODE_stg,
                                                                                                                                     STATE_stg,
                                                                                                                                     ROW_NUMBER() OVER ( PARTITION BY TERRITORYCODE_stg, COUNTY_stg,STATE_stg ORDER BY BEANVERSION_stg DESC,
                                                                                                                                     CASE
                                                                                                                                              WHEN (
                                                                                                                                                                zipcode_stg IS NULL) THEN 1
                                                                                                                                              ELSE 2
                                                                                                                                     END) AS RNK
                                                                                                                            FROM     DB_T_PROD_STAG.PCX_PATERRITORYCODE_ALFA )A
                                                                                                            WHERE  RNK = 1 ) PTCA5
                                                                                     ON              JD.ID_stg=STATE
                                                                                     AND             UPPER(PTCA5.TERRITORYCODE
                                                                                                                     || ''-''
                                                                                                                     || PTCA5.COUNTY)= UPPER(
                                                                                                     CASE
                                                                                                                     WHEN (
                                                                                                                                                     ST2.TYPECODE_stg=JD.TYPECODE_stg) THEN TC.CODE_stg
                                                                                                                                                     ||''-''
                                                                                                                                                     || PLVEH.COUNTYINTERNAL_stg
                                                                                                                     ELSE TC1.CODE_stg
                                                                                                                                                     ||''-''
                                                                                                                                                     || PLPOL.COUNTYINTERNAL_stg
                                                                                                     END)
                                                                                     LEFT JOIN
                                                                                                     (
                                                                                                            SELECT NAIIPCICODE_ALFA_stg AS NAIIPCICODE_ALFA,
                                                                                                                   COUNTY_stg           AS COUNTY,
                                                                                                                   TERRITORYCODE_stg    AS TERRITORYCODE,
                                                                                                                   STATE_stg            AS STATE
                                                                                                            FROM   (
                                                                                                                            SELECT   NAIIPCICODE_ALFA_stg,
                                                                                                                                     COUNTY_stg,
                                                                                                                                     TERRITORYCODE_stg,
                                                                                                                                     STATE_stg,
                                                                                                                                     ROW_NUMBER () OVER ( PARTITION BY TERRITORYCODE_stg, COUNTY_stg,STATE_stg ORDER BY BEANVERSION_stg DESC,
                                                                                                                                     CASE
                                                                                                                                              WHEN (
                                                                                                                                                                zipcode_stg IS NULL) THEN 1
                                                                                                                                              ELSE 2
                                                                                                                                     END) AS RNK
                                                                                                                            FROM     DB_T_PROD_STAG.PCX_PATERRITORYCODE_ALFA )A
                                                                                                            WHERE  RNK = 1 ) PTCA6
                                                                                     ON              JD.ID_stg=STATE
                                                                                     AND             UPPER( PTCA6.TERRITORYCODE
                                                                                                                     ||''-''
                                                                                                                     || PTCA6.COUNTY)=UPPER(TC1.CODE_stg
                                                                                                                     ||''-''
                                                                                                                     || PLPOL.COUNTYINTERNAL_stg)
                                                                                     LEFT JOIN
                                                                                                     (
                                                                                                            SELECT NAIIPCICODE_ALFA_stg AS NAIIPCICODE_ALFA,
                                                                                                                   COUNTY_stg           AS COUNTY,
                                                                                                                   TERRITORYCODE_stg    AS TERRITORYCODE,
                                                                                                                   state_stg            AS STATE
                                                                                                            FROM   (
                                                                                                                            SELECT   NAIIPCICODE_ALFA_stg,
                                                                                                                                     COUNTY_stg,
                                                                                                                                     TERRITORYCODE_stg,
                                                                                                                                     state_stg,
                                                                                                                                     ROW_NUMBER() OVER ( PARTITION BY COUNTY_stg,state_stg ORDER BY BEANVERSION_stg, UPDATETIME_stg DESC,TERRITORYCODE_stg ) AS RNK
                                                                                                                            FROM     DB_T_PROD_STAG.PCX_PATERRITORYCODE_ALFA )A
                                                                                                            WHERE  RNK = 1 ) PTCA1
                                                                                     ON              UPPER(PTCA1.COUNTY)=UPPER(
                                                                                                     CASE
                                                                                                                     WHEN (
                                                                                                                                                     ST2.TYPECODE_stg=JD.TYPECODE_stg) THEN PLVEH.COUNTYINTERNAL_stg
                                                                                                                     ELSE PLPOL.COUNTYINTERNAL_stg
                                                                                                     END)
                                                                                     AND             JD.ID_stg=STATE
                                                                                     LEFT JOIN
                                                                                                     (
                                                                                                            SELECT NAIIPCICODE_ALFA_stg AS NAIIPCICODE_ALFA,
                                                                                                                   COUNTY_stg           AS COUNTY,
                                                                                                                   TERRITORYCODE_stg    AS TERRITORYCODE,
                                                                                                                   STATE_stg            AS STATE
                                                                                                            FROM   (
                                                                                                                            SELECT   NAIIPCICODE_ALFA_stg,
                                                                                                                                     COUNTY_stg,
                                                                                                                                     TERRITORYCODE_stg,
                                                                                                                                     STATE_stg,
                                                                                                                                     ROW_NUMBER() OVER ( PARTITION BY TERRITORYCODE_stg,STATE_stg ORDER BY BEANVERSION_stg DESC, UPDATETIME_stg DESC,COUNTY_stg) AS RNK
                                                                                                                            FROM     DB_T_PROD_STAG.PCX_PATERRITORYCODE_ALFA )A
                                                                                                            WHERE  RNK = 1 ) PTCA2
                                                                                     ON              UPPER(PTCA2.TERRITORYCODE)=UPPER(
                                                                                                     CASE
                                                                                                                     WHEN (
                                                                                                                                                     ST2.TYPECODE_stg=JD.TYPECODE_stg) THEN TC.CODE_stg
                                                                                                                     ELSE TC1.CODE_stg
                                                                                                     END )
                                                                                     AND             DB_T_SHRD_PROD.STATE=JD.ID_stg
                                                                                     UNION
                                                                                           (
                                                                                                           SELECT DISTINCT pac.branchid_stg,
                                                                                                                           ''0''                                                                             vin ,
                                                                                                                           COALESCE(ptca.NAIIPCICODE_ALFA, ptca2.NAIIPCICODE_ALFA, ptca1.NAIIPCICODE_ALFA) NAIIPCICODE_ALFA_new
                                                                                                           FROM            DB_T_PROD_STAG.PC_POLICYPERIOD
                                                                                                           LEFT JOIN       DB_T_PROD_STAG.PC_personalautocov pac
                                                                                                           ON              pc_policyperiod.id_stg=pac.branchid_stg
                                                                                                           LEFT JOIN       DB_T_PROD_STAG.PC_EFFECTIVEDATEDFIELDS EFF
                                                                                                           ON              EFF.BRANCHID_stg = PC_POLICYPERIOD.ID_stg
                                                                                                           AND             EFF.EXPIRATIONDATE_stg IS NULL
                                                                                                           LEFT JOIN       DB_T_PROD_STAG.PC_POLICYLOCATION PLPOL
                                                                                                           ON              EFF.PRIMARYLOCATION_stg = PLPOL.ID_stg
                                                                                                           AND             PLPOL.EXPIRATIONDATE_stg IS NULL
                                                                                                           LEFT JOIN       DB_T_PROD_STAG.PC_TERRITORYCODE TC1
                                                                                                           ON              TC1.POLICYLOCATION_stg=PLPOL.FIXEDID_stg
                                                                                                           AND             TC1.BRANCHID_stg=PC_POLICYPERIOD.ID_stg
                                                                                                           AND             TC1.EXPIRATIONDATE_stg IS NULL
                                                                                                           JOIN            DB_T_PROD_STAG.PCTL_JURISDICTION JD
                                                                                                           ON              PC_POLICYPERIOD.BASESTATE_stg=JD.ID_stg
                                                                                                           LEFT JOIN
                                                                                                                           (
                                                                                                                                  SELECT NAIIPCICODE_ALFA_stg AS NAIIPCICODE_ALFA,
                                                                                                                                         COUNTY_stg           AS COUNTY,
                                                                                                                                         TERRITORYCODE_stg    AS TERRITORYCODE,
                                                                                                                                         STATE_stg            AS STATE
                                                                                                                                  FROM   (
                                                                                                                                                  SELECT   NAIIPCICODE_ALFA_stg,
                                                                                                                                                           COUNTY_stg,
                                                                                                                                                           TERRITORYCODE_stg,
                                                                                                                                                           STATE_stg,
                                                                                                                                                           ROW_NUMBER() OVER ( PARTITION BY TERRITORYCODE_stg, COUNTY_stg,STATE_stg ORDER BY BEANVERSION_stg, UPDATETIME_stg DESC) AS RNK
                                                                                                                                                  FROM     DB_T_PROD_STAG.PCX_PATERRITORYCODE_ALFA )A
                                                                                                                                  WHERE  RNK = 1 ) PTCA
                                                                                                           ON              JD.ID_stg=STATE
                                                                                                           AND             UPPER(PTCA.TERRITORYCODE
                                                                                                                                           ||''-''
                                                                                                                                           ||
                                                                                                                           CASE
                                                                                                                                           WHEN PTCA.COUNTY=''SAINT CLAIR'' THEN ''ST. CLAIR''
                                                                                                                                           ELSE ptca.COUNTY
                                                                                                                           END )= UPPER(TC1.CODE_stg
                                                                                                                                           ||''-''
                                                                                                                                           ||PLPOL.COUNTYINTERNAL_stg )
                                                                                                           LEFT JOIN
                                                                                                                           (
                                                                                                                                  SELECT NAIIPCICODE_ALFA_stg AS NAIIPCICODE_ALFA,
                                                                                                                                         COUNTY_stg           AS COUNTY,
                                                                                                                                         TERRITORYCODE_stg    AS TERRITORYCODE,
                                                                                                                                         STATE_stg            AS STATE
                                                                                                                                  FROM   (
                                                                                                                                                  SELECT   NAIIPCICODE_ALFA_stg,
                                                                                                                                                           COUNTY_stg,
                                                                                                                                                           TERRITORYCODE_stg,
                                                                                                                                                           STATE_stg,
                                                                                                                                                           ROW_NUMBER() OVER ( PARTITION BY TERRITORYCODE_stg,STATE_stg ORDER BY BEANVERSION_stg, UPDATETIME_stg DESC,COUNTY_stg ) AS RNK
                                                                                                                                                  FROM     DB_T_PROD_STAG.PCX_PATERRITORYCODE_ALFA )A
                                                                                                                                  WHERE  RNK = 1 ) PTCA2
                                                                                                           ON              UPPER(PTCA2.TERRITORYCODE)=UPPER( TC1.CODE_stg )
                                                                                                           LEFT JOIN
                                                                                                                           (
                                                                                                                                  SELECT NAIIPCICODE_ALFA_stg AS NAIIPCICODE_ALFA,
                                                                                                                                         COUNTY_stg           AS COUNTY,
                                                                                                                                         TERRITORYCODE_stg    AS TERRITORYCODE,
                                                                                                                                         state_stg            AS STATE
                                                                                                                                  FROM   (
                                                                                                                                                  SELECT   NAIIPCICODE_ALFA_stg,
                                                                                                                                                           COUNTY_stg,
                                                                                                                                                           TERRITORYCODE_stg,
                                                                                                                                                           state_stg,
                                                                                                                                                           ROW_NUMBER() OVER ( PARTITION BY COUNTY_stg,state_stg ORDER BY BEANVERSION_stg, UPDATETIME_stg DESC,TERRITORYCODE_stg) AS RNK
                                                                                                                                                  FROM     DB_T_PROD_STAG.PCX_PATERRITORYCODE_ALFA )A
                                                                                                                                  WHERE  RNK = 1 ) PTCA1
                                                                                                           ON              UPPER(
                                                                                                                           CASE
                                                                                                                                           WHEN PTCA1.COUNTY=''SAINT CLAIR'' THEN ''ST. CLAIR''
                                                                                                                                           ELSE ptca1.COUNTY
                                                                                                                           END )=UPPER( PLPOL.COUNTYINTERNAL_stg )
                                                                                                           AND             JD.ID_stg=STATE
                                                                                                           WHERE           pac.branchid_stg IS NOT NULL ) )a
                                                            GROUP BY id,
                                                                     vin ) pc_pavehmodifier) lkp_terr
                           ON        lkp_terr.id =pc_policyperiod_src.id
                           AND       lkp_terr.vin=pc_policyperiod_src.vin
                           LEFT JOIN
                                     (
                                            SELECT pc_pavehmodifier.Ind      AS Ind,
                                                   pc_pavehmodifier.Branchid AS Branchid
                                            FROM   (
                                                            SELECT   branchid_stg AS branchid,
                                                                     MAX(
                                                                     CASE
                                                                              WHEN PATTERNCODE_stg=''PADriverTrainingDiscount_alfa'' THEN ''7''
                                                                              ELSE ''1''
                                                                     END) ind
                                                            FROM     DB_T_PROD_STAG.PC_PAVEHMODIFIER
                                                            WHERE    PATTERNCODE_stg=''PADriverTrainingDiscount_alfa''
                                                            AND      ExpirationDate_stg IS NULL
                                                            GROUP BY branchid_stg)pc_pavehmodifier)lkp_defensive_driver
                           ON        pc_policyperiod_src.PolicyPeriodID= lkp_defensive_driver.Branchid
                           GROUP BY  pc_policyperiod_src.COMPANYNUMBER,
                                     pc_policyperiod_src.LOB,
                                     pc_policyperiod_src.StateOfPrincipalGarage,
                                     pc_policyperiod_src.CALLYEAR,
                                     pc_policyperiod_src.ACCOUNTINGYEAR,
                                     pc_policyperiod_src.ExpPeriodYear,
                                     pc_policyperiod_src.ExpPeriodMonth,
                                     pc_policyperiod_src.ExpPeriodDay,
                                     pc_policyperiod_src.CoverageCode,
                                     pc_policyperiod_src.AGE_FLAG,
                                     DB_T_SHRD_PROD.STATE,
                                     pc_policyperiod_src.vehicletype,
                                     pc_policyperiod_src.RATEDRIVERCLASS_ALFA,
                                     pc_policyperiod_src.Radius,
                                     pc_policyperiod_src.Tonnage,
                                     pc_policyperiod_src.Poltype,
                                     pc_policyperiod_src.City,
                                     pc_policyperiod_src.VEH_CNT,
                                     pc_policyperiod_src.df_drv,
                                     lkp_terr_NAIPCICODE_ALFA ,
                                     pc_policyperiod_src.ZipCode,
                                     pc_policyperiod_src.POLICY_EFF_YR,
                                     pc_policyperiod_src.DED_IND_CODE,
                                     pc_policyperiod_src.DEDUCTIBLE_AMT,
                                     pc_policyperiod_src.MF_MDL_YR,
                                     pc_policyperiod_src.DF_DRV_CODE,
                                     pc_policyperiod_src.POLICY_TERM,
                                     pc_policyperiod_src.PeriodEnd,
                                     pc_policyperiod_src.EditEffectiveDate,
                                     pc_policyperiod_src.POLICYNUMBER,
                                     pc_policyperiod_src.PolicyPeriodID,
                                     pc_policyperiod_src.PolicyIdentifier,
                                     pc_policyperiod_src.VIN,
                                     UPPER(pc_policyperiod_src.PATTERNCODE),
                                     UPPER(pc_policyperiod_src.PATTERNCODE_cov),
                                     pc_policyperiod_src.classification,
                                     pc_policyperiod_src.id,
                                     CASE
                                               WHEN lkp_defensive_driver.Branchid IS NULL THEN ''1''
                                               ELSE ''7''
                                     END,
                                     lkp_terr.NAIPCICODE_ALFA ,
                                     lkp_age_driver.VEH_CNT ,
                                     lkp_age_driver.Age_flag
                           HAVING    SUM(pc_policyperiod_src.Premium)<>0.00 ) SRC ) );
    -- Component EXPTRANS1, Type EXPRESSION
    CREATE
    OR
    REPLACE TEMPORARY TABLE EXPTRANS1 AS
    (
           SELECT SQ_pc_policyperiod.CompanyNumber          AS CompanyNumber,
                  SQ_pc_policyperiod.LOB                    AS LOB,
                  SQ_pc_policyperiod.StateOfPrincipalGarage AS StateOfPrincipalGarage,
                  SQ_pc_policyperiod.CallYear               AS CallYear,
                  SQ_pc_policyperiod.AccountingYear         AS AccountingYear,
                  SQ_pc_policyperiod.ExpPeriodYear          AS ExpPeriodYear,
                  SQ_pc_policyperiod.ExpPeriodMonth         AS ExpPeriodMonth,
                  SQ_pc_policyperiod.ExpPeriodDay           AS ExpPeriodDay,
                  SQ_pc_policyperiod.CoverageCode           AS CoverageCode,
                  SQ_pc_policyperiod.StateTypecode          AS StateTypecode,
                  SQ_pc_policyperiod.VehicleTypecode        AS VehicleTypecode,
                  SQ_pc_policyperiod.RateDriverClass_alfa   AS RateDriverClass_alfa,
                  SQ_pc_policyperiod.Radiusofuse_alfa       AS Radiusofuse_alfa,
                  SQ_pc_policyperiod.Tonnage_alfa           AS Tonnage_alfa,
                  SQ_pc_policyperiod.PolicyTypecode         AS PolicyTypecode,
                  SQ_pc_policyperiod.CityInternal           AS CityInternal,
                  SQ_pc_policyperiod.PatternCode            AS PatternCode,
                  SQ_pc_policyperiod.Zipcode                AS Zipcode,
                  SQ_pc_policyperiod.Policy_Eff_Yr          AS Policy_Eff_Yr,
                  SQ_pc_policyperiod.Ded_Ind_Code           AS Ded_Ind_Code,
                  SQ_pc_policyperiod.DeductibleAmount       AS DeductibleAmount,
                  SQ_pc_policyperiod.Mf_MDL_Yr              AS Mf_MDL_Yr,
                  SQ_pc_policyperiod.PolicyTerm             AS PolicyTerm,
                  CASE
                         WHEN SQ_pc_policyperiod.WrittenPremium < 0 THEN ABS ( DATEDIFF(''MM'',SQ_pc_policyperiod.PP_EditEffDate,SQ_pc_policyperiod.PP_PeriodEnd) ) * - 1
                         ELSE (
                                CASE
                                       WHEN SQ_pc_policyperiod.WrittenPremium > 0 THEN ABS ( DATEDIFF(''MM'',SQ_pc_policyperiod.PP_EditEffDate,SQ_pc_policyperiod.PP_PeriodEnd) )
                                       ELSE 0
                                END )
                  END                                             AS v_WrittenExposure,
                  v_WrittenExposure                               AS O_WrittenExposure,
                  SQ_pc_policyperiod.WrittenPremium               AS WrittenPremium,
                  SQ_pc_policyperiod.PolicyNumber                 AS PolicyNumber,
                  SQ_pc_policyperiod.PolicyPeriodID               AS PolicyPeriodID,
                  SQ_pc_policyperiod.PolicyIdentifier             AS PolicyIdentifier,
                  SQ_pc_policyperiod.VIN                          AS VIN,
                  ROUND ( SQ_pc_policyperiod.WrittenPremium , 2 ) AS v_writtenPremium,
                  SQ_pc_policyperiod.Classification               AS Classification,
                  SQ_pc_policyperiod.Cov_patternCODE              AS Cov_patternCODE,
                  SQ_pc_policyperiod.lkp_defdrv_Branchid          AS lkp_defdrv_Branchid,
                  SQ_pc_policyperiod.lkp_terr_NAIPCICODE_ALFA     AS lkp_terr_NAIPCICODE_ALFA,
                  SQ_pc_policyperiod.lkp_age_drv_VEH_CNT          AS lkp_age_drv_VEH_CNT,
                  SQ_pc_policyperiod.lkp_ag_drv_Age_flag          AS lkp_ag_drv_Age_flag,
                  SQ_pc_policyperiod.source_record_id
           FROM   SQ_pc_policyperiod );
    -- Component EXPTRANS, Type EXPRESSION
    CREATE
    OR
    REPLACE TEMPORARY TABLE EXPTRANS AS
    (
           SELECT EXPTRANS1.CompanyNumber          AS CompanyNumber,
                  EXPTRANS1.LOB                    AS LOB,
                  EXPTRANS1.StateOfPrincipalGarage AS StateOfPrincipalGarage,
                  EXPTRANS1.CallYear               AS CallYear,
                  EXPTRANS1.AccountingYear         AS AccountingYear,
                  EXPTRANS1.ExpPeriodYear          AS ExpPeriodYear,
                  EXPTRANS1.ExpPeriodMonth         AS ExpPeriodMonth,
                  EXPTRANS1.ExpPeriodDay           AS ExpPeriodDay,
                  EXPTRANS1.CoverageCode           AS CoverageCode,
                  EXPTRANS1.StateTypecode          AS StateTypecode,
                  EXPTRANS1.CityInternal           AS CityInternal,
                  EXPTRANS1.lkp_age_drv_VEH_CNT    AS Veh_Cnt_Fr_ID,
                  EXPTRANS1.PatternCode            AS PatternCode,
                  ''00''                             AS TypeOfLossCode,
                  CASE
                         WHEN LENGTH ( EXPTRANS1.lkp_terr_NAIPCICODE_ALFA ) > 3 THEN (
                                CASE
                                       WHEN EXPTRANS1.PolicyTypecode = ''COMMERCIAL'' THEN SUBSTR ( EXPTRANS1.lkp_terr_NAIPCICODE_ALFA , 4 , 2 )
                                       ELSE SUBSTR ( EXPTRANS1.lkp_terr_NAIPCICODE_ALFA , 1 , 2 )
                                END )
                         ELSE EXPTRANS1.lkp_terr_NAIPCICODE_ALFA
                  END                      AS TerritoryCode,
                  EXPTRANS1.Zipcode        AS Zipcode,
                  EXPTRANS1.Policy_Eff_Yr  AS Policy_Eff_Yr,
                  EXPTRANS1.PolicyTypecode AS PolicyTypecode,
                  DECODE ( TRUE ,
                          ( EXPTRANS1.CoverageCode = ''001''
                   OR     EXPTRANS1.CoverageCode = ''003''
                   OR     EXPTRANS1.CoverageCode = ''004''
                   OR     EXPTRANS1.CoverageCode = ''006''
                   OR     EXPTRANS1.CoverageCode = ''009''
                   OR     EXPTRANS1.CoverageCode = ''201''
                   OR     EXPTRANS1.CoverageCode = ''211''
                   OR     EXPTRANS1.CoverageCode = ''259''
                   OR     EXPTRANS1.CoverageCode = ''249''
                   OR     EXPTRANS1.CoverageCode = ''221'' )
                   AND    (
                                 EXPTRANS1.PolicyTypecode = ''PPV''
                          OR     EXPTRANS1.PolicyTypecode = ''PPV2'' ) , 192 ,
                          ( EXPTRANS1.CoverageCode = ''001''
                   OR     EXPTRANS1.CoverageCode = ''003''
                   OR     EXPTRANS1.CoverageCode = ''004''
                   OR     EXPTRANS1.CoverageCode = ''006''
                   OR     EXPTRANS1.CoverageCode = ''009''
                   OR     EXPTRANS1.CoverageCode = ''201''
                   OR     EXPTRANS1.CoverageCode = ''211''
                   OR     EXPTRANS1.CoverageCode = ''259''
                   OR     EXPTRANS1.CoverageCode = ''249''
                   OR     EXPTRANS1.CoverageCode = ''221'' )
                   AND    (
                                 EXPTRANS1.PolicyTypecode = ''COMMERCIAL'' ) , 194 ,
                          ( EXPTRANS1.CoverageCode = ''810''
                   OR     EXPTRANS1.CoverageCode = ''812''
                   OR     EXPTRANS1.CoverageCode = ''821''
                   OR     EXPTRANS1.CoverageCode = ''845''
                   OR     EXPTRANS1.CoverageCode = ''846''
                   OR     EXPTRANS1.CoverageCode = ''848''
                   OR     EXPTRANS1.CoverageCode = ''860'' )
                   AND    (
                                 EXPTRANS1.PolicyTypecode = ''PPV''
                          OR     EXPTRANS1.PolicyTypecode = ''PPV2'' ) , 211 ,
                          ( EXPTRANS1.CoverageCode = ''810''
                   OR     EXPTRANS1.CoverageCode = ''812''
                   OR     EXPTRANS1.CoverageCode = ''821''
                   OR     EXPTRANS1.CoverageCode = ''845''
                   OR     EXPTRANS1.CoverageCode = ''846''
                   OR     EXPTRANS1.CoverageCode = ''848''
                   OR     EXPTRANS1.CoverageCode = ''860'' )
                   AND    (
                                 EXPTRANS1.PolicyTypecode = ''COMMERCIAL'' ) , 212 ,
                          000 )                           AS v_Annual_Stmt_LOB,
                  v_Annual_Stmt_LOB                       AS O_Annual_Stmt_LOB,
                  EXPTRANS1.Ded_Ind_Code                  AS Ded_Ind_Code,
                  EXPTRANS1.DeductibleAmount              AS DeductibleAmount,
                  EXPTRANS1.Mf_MDL_Yr                     AS Mf_MDL_Yr,
                  EXPTRANS1.lkp_defdrv_Branchid           AS Df_Drv_Code,
                  EXPTRANS1.PolicyTerm                    AS PolicyTerm,
                  ''000000000000000''                       AS ClaimNumber,
                  ''000''                                   AS ClaimantIdentifier,
                  ''000000000000000''                       AS PaidLosses,
                  ''000000000000000''                       AS PaidClaims,
                  ''000000000000000''                       AS PaidAllocatedLossAdjExp,
                  ''000000000000000''                       AS OutStandingLosses,
                  ''000000000000000''                       AS OutStandingClaims,
                  TO_CHAR ( EXPTRANS1.O_WrittenExposure ) AS WrittenExposure1,
                  TO_CHAR ( EXPTRANS1.WrittenPremium )    AS WrittenPremium1,
                  EXPTRANS1.PolicyNumber                  AS PolicyNumber,
                  EXPTRANS1.PolicyPeriodID                AS PolicyPeriodID,
                  EXPTRANS1.PolicyIdentifier              AS PolicyIdentifier,
                  EXPTRANS1.VIN                           AS VIN,
                  ''00''                                    AS ExposureNumber,
                  0                                       AS Clm_veh_cnt,
                  CASE
                         WHEN EXPTRANS1.lkp_ag_drv_Age_flag IS NULL THEN ''N''
                         ELSE EXPTRANS1.lkp_ag_drv_Age_flag
                  END                      AS V_Age_Flag,
                  EXPTRANS1.PolicyPeriodID AS ID,
                  /*DECODE ( TRUE ,
                          IN ( EXPTRANS1.Cov_patternCODE ,
                              ''PAACCIDENTWAIVER_ALFA'' ,
                              ''PAADD_ALFA'' ) , ''9414'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    (
                                 EXPTRANS1.RateDriverClass_alfa IS NULL
                          OR     IN ( EXPTRANS1.RateDriverClass_alfa ,
                                     ''1A'' ,
                                     ''1B'' ) )
                   AND    EXPTRANS1.VehicleTypecode = ''DB''
                   AND    V_Age_Flag = ''Y'' ) , ''9527'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    (
                                 EXPTRANS1.RateDriverClass_alfa IS NULL
                          OR     IN ( EXPTRANS1.RateDriverClass_alfa ,
                                     ''1A'' ,
                                     ''1B'' ) )
                   AND    EXPTRANS1.VehicleTypecode = ''DB''
                   AND    V_Age_Flag = ''N'' ) , ''9529'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''1A'' ,
                              ''1B'' )
                   AND    IN ( EXPTRANS1.VehicleTypecode ,
                              ''AN'' ,
                              ''CL'' )
                   AND    V_Age_Flag = ''Y'' ) , ''9587'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''1A'' ,
                              ''1B'' )
                   AND    IN ( EXPTRANS1.VehicleTypecode ,
                              ''AN'' ,
                              ''CL'' )
                   AND    V_Age_Flag = ''N'' ) , ''9392'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''1A'' ,
                              ''1B'' )
                   AND    EXPTRANS1.VehicleTypecode = ''MH''
                   AND    V_Age_Flag = ''Y'' ) , ''9547'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''1A'' ,
                              ''1B'' )
                   AND    EXPTRANS1.VehicleTypecode = ''MH''
                   AND    V_Age_Flag = ''N'' ) , ''9340'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''1A'' ,
                              ''RL'' )
                   AND    EXPTRANS1.VehicleTypecode = ''RT'' ) , ''9332'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''GO'' ,
                              ''MA'' )
                   AND    EXPTRANS1.VehicleTypecode = ''AT''
                   AND    V_Age_Flag = ''Y'' ) , ''9507'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''GO'' ,
                              ''MA'' )
                   AND    EXPTRANS1.VehicleTypecode = ''AT''
                   AND    V_Age_Flag = ''N'' ) , ''9509'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''MA'' ,
                              ''MY'' )
                   AND    IN ( EXPTRANS1.VehicleTypecode ,
                              ''MC'' ,
                              ''MS'' )
                   AND    V_Age_Flag = ''Y'' ) , ''9597'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''MA'' ,
                              ''MY'' )
                   AND    IN ( EXPTRANS1.VehicleTypecode ,
                              ''MC'' ,
                              ''MS'' )
                   AND    V_Age_Flag = ''N'' ) , ''9492'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''GO''
                   AND    EXPTRANS1.VehicleTypecode = ''GO'' ) , ''9539'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''RL''
                   AND    IN ( EXPTRANS1.VehicleTypecode ,
                              ''LT'' ,
                              ''TR'' ) ) , ''9331'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''1A'' ,
                              ''1B'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT = 1 ) , ''1210'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''1A'' ,
                              ''1B'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT > 1 ) , ''1212'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''6A'' ,
                              ''6B'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT = 1 ) , ''1210'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''6A'' ,
                              ''6B'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT > 1 ) , ''1212'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''1J'' ,
                              ''1K'' ,
                              ''1M'' ,
                              ''6J'' ,
                              ''6K'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT = 1 ) , ''1213'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''1J'' ,
                              ''1K'' ,
                              ''1M'' ,
                              ''6J'' ,
                              ''6K'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT > 1 ) , ''1211'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''1C'' ,
                              ''1D'' ,
                              ''6C'' ,
                              ''6D'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT = 1 ) , ''1220'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''1C'' ,
                              ''1D'' ,
                              ''6C'' ,
                              ''6D'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT > 1 ) , ''1222'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''1E'' ,
                              ''1F'' ,
                              ''6E'' ,
                              ''6F'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT = 1 ) , ''1230'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''1E'' ,
                              ''1F'' ,
                              ''6E'' ,
                              ''6F'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT > 1 ) , ''1232'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''1G'' ,
                              ''1H'' ,
                              ''1L'' ,
                              ''6G'' ,
                              ''6H'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT = 1 ) , ''1400'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''1G'' ,
                              ''1H'' ,
                              ''1L'' ,
                              ''6G'' ,
                              ''6H'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT > 1 ) , ''1402'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''2A'' ,
                              ''2B'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT = 1 ) , ''1510'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''2A'' ,
                              ''2B'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT > 1 ) , ''1512'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''2J'' ,
                              ''2K'' ,
                              ''S1'' ,
                              ''S2'' ,
                              ''S3'' ,
                              ''S4'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT = 1 ) , ''1513'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''2J'' ,
                              ''2K'' ,
                              ''S1'' ,
                              ''S2'' ,
                              ''S3'' ,
                              ''S4'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT > 1 ) , ''1511'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''2C'' ,
                              ''2D'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT = 1 ) , ''1520'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''2C'' ,
                              ''2D'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT > 1 ) , ''1522'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''2E'' ,
                              ''2F'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT = 1 ) , ''1530'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''2E'' ,
                              ''2F'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT > 1 ) , ''1532'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''2G'' ,
                              ''2H'' ,
                              ''21'' ,
                              ''22'' ,
                              ''23'' ,
                              ''24'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT = 1 ) , ''1550'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''2G'' ,
                              ''2H'' ,
                              ''21'' ,
                              ''22'' ,
                              ''23'' ,
                              ''24'' )
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT > 1 ) , ''1552'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''8A'' ,
                              ''8B'' ,
                              ''8C'' ,
                              ''8D'' ,
                              ''8L'' ,
                              ''8N'' ,
                              ''8Y'' ,
                              ''81'' ,
                              ''82'' ,
                              ''83'' ,
                              ''84'' ,
                              ''85'' ,
                              ''86'' ,
                              ''87'' ,
                              ''88'' ,
                              ''89'' ) ) , ''1610'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''8J'' ,
                              ''8K'' ,
                              ''8M'' ,
                              ''8P'' ,
                              ''8Q'' ,
                              ''V1'' ,
                              ''V2'' ,
                              ''V3'' ,
                              ''V4'' ,
                              ''V5'' ,
                              ''V6'' ,
                              ''V7'' ,
                              ''V8'' ,
                              ''V9'' ) ) , ''1613'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''7A'' ,
                              ''7B'' ,
                              ''7C'' ,
                              ''7D'' ,
                              ''7E'' ,
                              ''7F'' ,
                              ''7G'' ,
                              ''7H'' ,
                              ''7N'' ,
                              ''7Y'' ,
                              ''71'' ,
                              ''72'' ,
                              ''73'' ,
                              ''74'' ,
                              ''75'' ,
                              ''76'' ,
                              ''77'' ,
                              ''78'' ,
                              ''79'' ) ) , ''1620'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''7J'' ,
                              ''7K'' ,
                              ''7P'' ,
                              ''7Q'' ,
                              ''7R'' ,
                              ''7X'' ,
                              ''T1'' ,
                              ''T2'' ,
                              ''T3'' ,
                              ''T4'' ,
                              ''T5'' ,
                              ''T6'' ,
                              ''T7'' ,
                              ''T8'' ,
                              ''T9'' ) ) , ''1623'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''9A'' ,
                              ''9B'' ,
                              ''9C'' ,
                              ''9D'' ,
                              ''9N'' ,
                              ''9Y'' ,
                              ''90'' ,
                              ''91'' ,
                              ''92'' ,
                              ''93'' ,
                              ''94'' ,
                              ''95'' ,
                              ''96'' ,
                              ''97'' ,
                              ''98'' ,
                              ''99'' ) ) , ''1630'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''9J'' ,
                              ''9K'' ,
                              ''9P'' ,
                              ''9Q'' ,
                              ''1V'' ,
                              ''2V'' ,
                              ''3V'' ,
                              ''4V'' ,
                              ''5V'' ,
                              ''6V'' ,
                              ''7V'' ,
                              ''8V'' ,
                              ''9V'' ) ) , ''1633'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''4A'' ,
                              ''4B'' ,
                              ''4C'' ,
                              ''4D'' ,
                              ''4N'' ,
                              ''4Y'' ,
                              ''5A'' ,
                              ''5B'' ,
                              ''5C'' ,
                              ''5D'' ,
                              ''5N'' ,
                              ''5Y'' ,
                              ''41'' ,
                              ''42'' ,
                              ''43'' ,
                              ''44'' ,
                              ''45'' ,
                              ''46'' ,
                              ''47'' ,
                              ''48'' ,
                              ''49'' ,
                              ''51'' ,
                              ''52'' ,
                              ''53'' ,
                              ''54'' ,
                              ''55'' ,
                              ''56'' ,
                              ''57'' ,
                              ''58'' ,
                              ''59'' ) ) , ''1640'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''4J'' ,
                              ''4K'' ,
                              ''4P'' ,
                              ''4Q'' ,
                              ''5J'' ,
                              ''5K'' ,
                              ''5P'' ,
                              ''5Q'' ,
                              ''U1'' ,
                              ''U2'' ,
                              ''U3'' ,
                              ''U4'' ,
                              ''U5'' ,
                              ''U6'' ,
                              ''U7'' ,
                              ''U8'' ,
                              ''U9'' ,
                              ''1U'' ,
                              ''2U'' ,
                              ''3U'' ,
                              ''4U'' ,
                              ''5U'' ,
                              ''6U'' ,
                              ''7U'' ,
                              ''8U'' ,
                              ''9U'' ) ) , ''1643'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''3A'' ,
                              ''3B'' ,
                              ''3C'' ,
                              ''3D'' ,
                              ''3E'' ,
                              ''3F'' ,
                              ''3G'' ,
                              ''3H'' ,
                              ''A1'' ,
                              ''A2'' ,
                              ''A3'' ,
                              ''A4'' ,
                              ''A5'' ,
                              ''A6'' ,
                              ''B1'' ,
                              ''B2'' ,
                              ''B3'' ,
                              ''B4'' ,
                              ''B4'' ,
                              ''B5'' ,
                              ''B6'' ,
                              ''C1'' ,
                              ''C2'' ,
                              ''C3'' ,
                              ''C4'' ,
                              ''C5'' ,
                              ''C6'' ,
                              ''AG'' ,
                              ''AH'' ,
                              ''BG'' ,
                              ''BH'' ,
                              ''CG'' ,
                              ''CH'' ) ) , ''1650'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    IN ( EXPTRANS1.RateDriverClass_alfa ,
                              ''3J'' ,
                              ''3K'' ,
                              ''AJ'' ,
                              ''AK'' ,
                              ''BJ'' ,
                              ''BK'' ,
                              ''CJ'' ,
                              ''CK'' ) ) , ''1653'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT = 1 ) , ''1220'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV''
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT > 1 ) , ''1222'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV2''
                   AND    EXPTRANS1.PatternCode = ''PAAffinityDiscount_alfa''
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT = 1 ) , ''1103'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV2''
                   AND    EXPTRANS1.PatternCode = ''PAAffinityDiscount_alfa''
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT > 1 ) , ''1101'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV2''
                   AND    EXPTRANS1.PatternCode IS NULL
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT = 1 ) , ''1100'' ,
                          ( EXPTRANS1.PolicyTypecode = ''PPV2''
                   AND    EXPTRANS1.PatternCode IS NULL
                   AND    EXPTRANS1.lkp_age_drv_VEH_CNT > 1 ) , ''1102'' ,
                          ( EXPTRANS1.StateTypecode = 01
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''J1''
                   AND    IN ( EXPTRANS1.Radiusofuse_alfa ,
                              ''MileRadius100'' ,
                              ''MileRadius50Plus'' ) ) , ''2856'' ,
                          ( EXPTRANS1.StateTypecode = 01
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''J2''
                   AND    IN ( EXPTRANS1.Radiusofuse_alfa ,
                              ''MilesRadius300'' ,
                              ''Over300Miles'' ) ) , ''2856'' ,
                          ( EXPTRANS1.StateTypecode = 01
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.Tonnage_alfa = ''LessThan1Ton''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''L4'' ) , ''8019'' ,
                          ( EXPTRANS1.StateTypecode = 01
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.Tonnage_alfa = ''LessThan1Ton''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''M4'' ) , ''8049'' ,
                          ( EXPTRANS1.StateTypecode = 01
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.Tonnage_alfa = ''1To2Ton''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''J5'' ) , ''2856'' ,
                          ( EXPTRANS1.StateTypecode = 01
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.Tonnage_alfa = ''1To2Ton''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''L5''
                   AND    IN ( EXPTRANS1.Radiusofuse_alfa ,
                              ''MileRadius100'' ,
                              ''MileRadius50Plus'' ) ) , ''8229'' ,
                          ( EXPTRANS1.StateTypecode = 01
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.Tonnage_alfa = ''1To2Ton''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''M5''
                   AND    IN ( EXPTRANS1.Radiusofuse_alfa ,
                              ''MileRadius100'' ,
                              ''MileRadius50Plus'' ) ) , ''8259'' ,
                          ( EXPTRANS1.StateTypecode = 01
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.Tonnage_alfa = ''1To2Ton''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''N5''
                   AND    IN ( EXPTRANS1.Radiusofuse_alfa ,
                              ''MilesRadius300'' ,
                              ''Over300Miles'' ) ) , ''8299'' ,
                          ( EXPTRANS1.StateTypecode = 01
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.Tonnage_alfa = ''2To3.5Ton''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''J6'' ) , ''2856'' ,
                          ( EXPTRANS1.StateTypecode = 01
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.Tonnage_alfa = ''2To3.5Ton''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''L6''
                   AND    IN ( EXPTRANS1.Radiusofuse_alfa ,
                              ''MileRadius100'' ,
                              ''MileRadius50Plus'' ) ) , ''8329'' ,
                          ( EXPTRANS1.StateTypecode = 01
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.Tonnage_alfa = ''2To3.5Ton''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''M6''
                   AND    IN ( EXPTRANS1.Radiusofuse_alfa ,
                              ''MileRadius100'' ,
                              ''MileRadius50Plus'' ) ) , ''8359'' ,
                          ( EXPTRANS1.StateTypecode = 01
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.Tonnage_alfa = ''2To3.5Ton''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''N6''
                   AND    IN ( EXPTRANS1.Radiusofuse_alfa ,
                              ''MilesRadius300'' ,
                              ''Over300Miles'' ) ) , ''8399'' ,
                          ( EXPTRANS1.StateTypecode = 01
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''Q4'' ) , ''5800'' ,
                          ( EXPTRANS1.StateTypecode = 01
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''RL'' ) , ''8939'' ,
                          ( IN ( EXPTRANS1.StateTypecode ,
                                10 ,
                                23 )
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''L1'' ) , ''2856'' ,
                          ( IN ( EXPTRANS1.StateTypecode ,
                                10 ,
                                23 )
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''M1'' ) , ''2856'' ,
                          ( IN ( EXPTRANS1.StateTypecode ,
                                10 ,
                                23 )
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''H1'' ) , ''2856'' ,
                          ( IN ( EXPTRANS1.StateTypecode ,
                                10 ,
                                23 )
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    IN ( EXPTRANS1.Tonnage_alfa ,
                              ''LessThan1Ton'' ,
                              ''Upto1.5Ton'' )
                   AND    IN ( EXPTRANS1.Radiusofuse_alfa ,
                              ''MilesRadius300'' ,
                              ''Over300Miles'' )
                   AND    EXPTRANS1.RateDriverClass_alfa = ''L4'' ) , ''8039'' ,
                          ( IN ( EXPTRANS1.StateTypecode ,
                                10 ,
                                23 )
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.Tonnage_alfa = ''Upto1.5Ton''
                   AND    EXPTRANS1.Radiusofuse_alfa = ''MileRadius100''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''L5'' ) , ''8029'' ,
                          ( IN ( EXPTRANS1.StateTypecode ,
                                10 ,
                                23 )
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.Tonnage_alfa = ''Upto1.5Ton''
                   AND    EXPTRANS1.Radiusofuse_alfa = ''MileRadius300''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''L5'' ) , ''8039'' ,
                          ( IN ( EXPTRANS1.StateTypecode ,
                                10 ,
                                23 )
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.Tonnage_alfa = ''LessThan1Ton''
                   AND    IN ( EXPTRANS1.Radiusofuse_alfa ,
                              ''MileRadius1-3'' ,
                              ''MileRadius4-10'' ,
                              ''MileRadius11-49'' )
                   AND    EXPTRANS1.RateDriverClass_alfa = ''L5'' ) , ''8019'' ,
                          ( IN ( EXPTRANS1.StateTypecode ,
                                10 ,
                                23 )
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    IN ( EXPTRANS1.Tonnage_alfa ,
                              ''LessThan1Ton'' ,
                              ''Over1.5To3.5Ton'' )
                   AND    IN ( EXPTRANS1.Radiusofuse_alfa ,
                              ''MilesRadius300'' ,
                              ''Over300Miles'' )
                   AND    EXPTRANS1.RateDriverClass_alfa = ''M4'' ) , ''8239'' ,
                          ( IN ( EXPTRANS1.StateTypecode ,
                                10 ,
                                23 )
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.Tonnage_alfa = ''LessThan1Ton''
                   AND    IN ( EXPTRANS1.Radiusofuse_alfa ,
                              ''MileRadius1-3'' ,
                              ''MileRadius4-10'' ,
                              ''MileRadius11-49'' )
                   AND    EXPTRANS1.RateDriverClass_alfa = ''M5'' ) , ''8219'' ,
                          ( IN ( EXPTRANS1.StateTypecode ,
                                10 ,
                                23 )
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.Tonnage_alfa = ''Over1.5To3.5Ton''
                   AND    EXPTRANS1.Radiusofuse_alfa = ''MileRadius100''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''M5'' ) , ''8229'' ,
                          ( IN ( EXPTRANS1.StateTypecode ,
                                10 ,
                                23 )
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    IN ( EXPTRANS1.Tonnage_alfa ,
                              ''LessThan1Ton'' ,
                              ''Over3.5Ton'' )
                   AND    IN ( EXPTRANS1.Radiusofuse_alfa ,
                              ''MilesRadius300'' ,
                              ''Over300Miles'' )
                   AND    EXPTRANS1.RateDriverClass_alfa = ''H4'' ) , ''8339'' ,
                          ( IN ( EXPTRANS1.StateTypecode ,
                                10 ,
                                23 )
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.Tonnage_alfa = ''LessThan1Ton''
                   AND    IN ( EXPTRANS1.Radiusofuse_alfa ,
                              ''MileRadius1-3'' ,
                              ''MileRadius4-10'' ,
                              ''MileRadius11-49'' )
                   AND    EXPTRANS1.RateDriverClass_alfa = ''H5'' ) , ''8319'' ,
                          ( IN ( EXPTRANS1.StateTypecode ,
                                10 ,
                                23 )
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.Tonnage_alfa = ''Over3.5Ton''
                   AND    EXPTRANS1.Radiusofuse_alfa = ''MileRadius100''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''H5'' ) , ''8329'' ,
                          ( IN ( EXPTRANS1.StateTypecode ,
                                10 ,
                                23 )
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''Q4'' ) , ''5800'' ,
                          ( IN ( EXPTRANS1.StateTypecode ,
                                10 ,
                                23 )
                   AND    EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
                   AND    EXPTRANS1.RateDriverClass_alfa = ''RL'' ) , ''8939'' ,
                          NULL ) AS Out_Classification, */
                 CASE
    WHEN EXPTRANS1.Cov_patternCODE IN (''PAACCIDENTWAIVER_ALFA'', ''PAADD_ALFA'') 
        THEN ''9414''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND (EXPTRANS1.RateDriverClass_alfa IS NULL 
              OR EXPTRANS1.RateDriverClass_alfa IN (''1A'', ''1B''))
         AND EXPTRANS1.VehicleTypecode = ''DB''
         AND V_Age_Flag = ''Y'' 
        THEN ''9527''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND (EXPTRANS1.RateDriverClass_alfa IS NULL 
              OR EXPTRANS1.RateDriverClass_alfa IN (''1A'', ''1B''))
         AND EXPTRANS1.VehicleTypecode = ''DB''
         AND V_Age_Flag = ''N'' 
        THEN ''9529''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''1A'', ''1B'')
         AND EXPTRANS1.VehicleTypecode IN (''AN'', ''CL'')
         AND V_Age_Flag = ''Y'' 
        THEN ''9587''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''1A'', ''1B'')
         AND EXPTRANS1.VehicleTypecode IN (''AN'', ''CL'')
         AND V_Age_Flag = ''N'' 
        THEN ''9392''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''1A'', ''1B'')
         AND EXPTRANS1.VehicleTypecode = ''MH''
         AND V_Age_Flag = ''Y'' 
        THEN ''9547''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''1A'', ''1B'')
         AND EXPTRANS1.VehicleTypecode = ''MH''
         AND V_Age_Flag = ''N'' 
        THEN ''9340''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''1A'', ''RL'')
         AND EXPTRANS1.VehicleTypecode = ''RT'' 
        THEN ''9332''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''GO'', ''MA'')
         AND EXPTRANS1.VehicleTypecode = ''AT''
         AND V_Age_Flag = ''Y'' 
        THEN ''9507''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''GO'', ''MA'')
         AND EXPTRANS1.VehicleTypecode = ''AT''
         AND V_Age_Flag = ''N'' 
        THEN ''9509''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''MA'', ''MY'')
         AND EXPTRANS1.VehicleTypecode IN (''MC'', ''MS'')
         AND V_Age_Flag = ''Y'' 
        THEN ''9597''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''MA'', ''MY'')
         AND EXPTRANS1.VehicleTypecode IN (''MC'', ''MS'')
         AND V_Age_Flag = ''N'' 
        THEN ''9492''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa = ''GO''
         AND EXPTRANS1.VehicleTypecode = ''GO'' 
        THEN ''9539''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa = ''RL''
         AND EXPTRANS1.VehicleTypecode IN (''LT'', ''TR'') 
        THEN ''9331''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''1A'', ''1B'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT = 1 
        THEN ''1210''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''1A'', ''1B'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT > 1 
        THEN ''1212''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''6A'', ''6B'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT = 1 
        THEN ''1210''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''6A'', ''6B'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT > 1 
        THEN ''1212''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''1J'', ''1K'', ''1M'', ''6J'', ''6K'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT = 1 
        THEN ''1213''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''1J'', ''1K'', ''1M'', ''6J'', ''6K'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT > 1 
        THEN ''1211''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''1C'', ''1D'', ''6C'', ''6D'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT = 1 
        THEN ''1220''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''1C'', ''1D'', ''6C'', ''6D'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT > 1 
        THEN ''1222''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''1E'', ''1F'', ''6E'', ''6F'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT = 1 
        THEN ''1230''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''1E'', ''1F'', ''6E'', ''6F'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT > 1 
        THEN ''1232''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''1G'', ''1H'', ''1L'', ''6G'', ''6H'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT = 1 
        THEN ''1400''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''1G'', ''1H'', ''1L'', ''6G'', ''6H'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT > 1 
        THEN ''1402''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''2A'', ''2B'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT = 1 
        THEN ''1510''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''2A'', ''2B'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT > 1 
        THEN ''1512''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''2J'', ''2K'', ''S1'', ''S2'', ''S3'', ''S4'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT = 1 
        THEN ''1513''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''2J'', ''2K'', ''S1'', ''S2'', ''S3'', ''S4'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT > 1 
        THEN ''1511''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''2C'', ''2D'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT = 1 
        THEN ''1520''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''2C'', ''2D'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT > 1 
        THEN ''1522''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''2E'', ''2F'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT = 1 
        THEN ''1530''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''2E'', ''2F'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT > 1 
        THEN ''1532''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''2G'', ''2H'', ''21'', ''22'', ''23'', ''24'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT = 1 
        THEN ''1550''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''2G'', ''2H'', ''21'', ''22'', ''23'', ''24'')
         AND EXPTRANS1.lkp_age_drv_VEH_CNT > 1 
        THEN ''1552''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''8A'', ''8B'', ''8C'', ''8D'', ''8L'', ''8N'', ''8Y'', 
                                               ''81'', ''82'', ''83'', ''84'', ''85'', ''86'', ''87'', ''88'', ''89'')
        THEN ''1610''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''8J'', ''8K'', ''8M'', ''8P'', ''8Q'', 
                                               ''V1'', ''V2'', ''V3'', ''V4'', ''V5'', ''V6'', ''V7'', ''V8'', ''V9'')
        THEN ''1613''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''7A'', ''7B'', ''7C'', ''7D'', ''7E'', ''7F'', ''7G'', ''7H'', ''7N'', ''7Y'',
                                               ''71'', ''72'', ''73'', ''74'', ''75'', ''76'', ''77'', ''78'', ''79'')
        THEN ''1620''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''7J'', ''7K'', ''7P'', ''7Q'', ''7R'', ''7X'',
                                               ''T1'', ''T2'', ''T3'', ''T4'', ''T5'', ''T6'', ''T7'', ''T8'', ''T9'')
        THEN ''1623''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''9A'', ''9B'', ''9C'', ''9D'', ''9N'', ''9Y'',
                                               ''90'', ''91'', ''92'', ''93'', ''94'', ''95'', ''96'', ''97'', ''98'', ''99'')
        THEN ''1630''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''9J'', ''9K'', ''9P'', ''9Q'',
                                               ''1V'', ''2V'', ''3V'', ''4V'', ''5V'', ''6V'', ''7V'', ''8V'', ''9V'')
        THEN ''1633''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''4A'', ''4B'', ''4C'', ''4D'', ''4N'', ''4Y'',
                                               ''5A'', ''5B'', ''5C'', ''5D'', ''5N'', ''5Y'',
                                               ''41'', ''42'', ''43'', ''44'', ''45'', ''46'', ''47'', ''48'', ''49'',
                                               ''51'', ''52'', ''53'', ''54'', ''55'', ''56'', ''57'', ''58'', ''59'')
        THEN ''1640''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''4J'', ''4K'', ''4P'', ''4Q'', ''5J'', ''5K'', ''5P'', ''5Q'',
                                               ''U1'', ''U2'', ''U3'', ''U4'', ''U5'', ''U6'', ''U7'', ''U8'', ''U9'',
                                               ''1U'', ''2U'', ''3U'', ''4U'', ''5U'', ''6U'', ''7U'', ''8U'', ''9U'')
        THEN ''1643''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''3A'', ''3B'', ''3C'', ''3D'', ''3E'', ''3F'', ''3G'', ''3H'',
                                               ''A1'', ''A2'', ''A3'', ''A4'', ''A5'', ''A6'',
                                               ''B1'', ''B2'', ''B3'', ''B4'', ''B5'', ''B6'',
                                               ''C1'', ''C2'', ''C3'', ''C4'', ''C5'', ''C6'',
                                               ''AG'', ''AH'', ''BG'', ''BH'', ''CG'', ''CH'')
        THEN ''1650''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.RateDriverClass_alfa IN (''3J'', ''3K'', ''AJ'', ''AK'', ''BJ'', ''BK'', ''CJ'', ''CK'')
        THEN ''1653''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.lkp_age_drv_VEH_CNT = 1
        THEN ''1220''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV''
         AND EXPTRANS1.lkp_age_drv_VEH_CNT > 1
        THEN ''1222''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV2''
         AND EXPTRANS1.PatternCode = ''PAAffinityDiscount_alfa''
         AND EXPTRANS1.lkp_age_drv_VEH_CNT = 1
        THEN ''1103''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV2''
         AND EXPTRANS1.PatternCode = ''PAAffinityDiscount_alfa''
         AND EXPTRANS1.lkp_age_drv_VEH_CNT > 1
        THEN ''1101''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV2''
         AND EXPTRANS1.PatternCode IS NULL
         AND EXPTRANS1.lkp_age_drv_VEH_CNT = 1
        THEN ''1100''
    WHEN EXPTRANS1.PolicyTypecode = ''PPV2''
         AND EXPTRANS1.PatternCode IS NULL
         AND EXPTRANS1.lkp_age_drv_VEH_CNT > 1
        THEN ''1102''
    WHEN EXPTRANS1.StateTypecode = ''01''
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.RateDriverClass_alfa = ''J1''
         AND EXPTRANS1.Radiusofuse_alfa IN (''MileRadius100'', ''MileRadius50Plus'')
        THEN ''2856''
    WHEN EXPTRANS1.StateTypecode = ''01''
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.RateDriverClass_alfa = ''J2''
         AND EXPTRANS1.Radiusofuse_alfa IN (''MilesRadius300'', ''Over300Miles'')
        THEN ''2856''
    WHEN EXPTRANS1.StateTypecode = ''01''
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa = ''LessThan1Ton''
         AND EXPTRANS1.RateDriverClass_alfa = ''L4''
        THEN ''8019''
    WHEN EXPTRANS1.StateTypecode = ''01''
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa = ''LessThan1Ton''
         AND EXPTRANS1.RateDriverClass_alfa = ''M4''
        THEN ''8049''
    WHEN EXPTRANS1.StateTypecode = ''01''
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa = ''1To2Ton''
         AND EXPTRANS1.RateDriverClass_alfa = ''J5''
        THEN ''2856''
    WHEN EXPTRANS1.StateTypecode = ''01''
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa = ''1To2Ton''
         AND EXPTRANS1.RateDriverClass_alfa = ''L5''
         AND EXPTRANS1.Radiusofuse_alfa IN (''MileRadius100'', ''MileRadius50Plus'')
        THEN ''8229''
    WHEN EXPTRANS1.StateTypecode = ''01''
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa = ''1To2Ton''
         AND EXPTRANS1.RateDriverClass_alfa = ''M5''
         AND EXPTRANS1.Radiusofuse_alfa IN (''MileRadius100'', ''MileRadius50Plus'')
        THEN ''8259''
    WHEN EXPTRANS1.StateTypecode = ''01''
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa = ''1To2Ton''
         AND EXPTRANS1.RateDriverClass_alfa = ''N5''
         AND EXPTRANS1.Radiusofuse_alfa IN (''MilesRadius300'', ''Over300Miles'')
        THEN ''8299''
    WHEN EXPTRANS1.StateTypecode = ''01''
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa = ''2To3.5Ton''
         AND EXPTRANS1.RateDriverClass_alfa = ''J6''
        THEN ''2856''
    WHEN EXPTRANS1.StateTypecode = ''01''
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa = ''2To3.5Ton''
         AND EXPTRANS1.RateDriverClass_alfa = ''L6''
         AND EXPTRANS1.Radiusofuse_alfa IN (''MileRadius100'', ''MileRadius50Plus'')
        THEN ''8329''
    WHEN EXPTRANS1.StateTypecode = ''01''
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa = ''2To3.5Ton''
         AND EXPTRANS1.RateDriverClass_alfa = ''M6''
         AND EXPTRANS1.Radiusofuse_alfa IN (''MileRadius100'', ''MileRadius50Plus'')
        THEN ''8359''
    WHEN EXPTRANS1.StateTypecode = ''01''
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa = ''2To3.5Ton''
         AND EXPTRANS1.RateDriverClass_alfa = ''N6''
         AND EXPTRANS1.Radiusofuse_alfa IN (''MilesRadius300'', ''Over300Miles'')
        THEN ''8399''
    WHEN EXPTRANS1.StateTypecode = ''01''
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.RateDriverClass_alfa = ''Q4''
        THEN ''5800''
    WHEN EXPTRANS1.StateTypecode = ''01''
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.RateDriverClass_alfa = ''RL''
        THEN ''8939''
    WHEN EXPTRANS1.StateTypecode IN (''10'', ''23'')
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.RateDriverClass_alfa = ''L1''
        THEN ''2856''
    WHEN EXPTRANS1.StateTypecode IN (''10'', ''23'')
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.RateDriverClass_alfa = ''M1''
        THEN ''2856''
    WHEN EXPTRANS1.StateTypecode IN (''10'', ''23'')
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.RateDriverClass_alfa = ''H1''
        THEN ''2856''
    WHEN EXPTRANS1.StateTypecode IN (''10'', ''23'')
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa IN (''LessThan1Ton'', ''Upto1.5Ton'')
         AND EXPTRANS1.Radiusofuse_alfa IN (''MilesRadius300'', ''Over300Miles'')
         AND EXPTRANS1.RateDriverClass_alfa = ''L4''
        THEN ''8039''
    WHEN EXPTRANS1.StateTypecode IN (''10'', ''23'')
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa = ''Upto1.5Ton''
         AND EXPTRANS1.Radiusofuse_alfa = ''MileRadius100''
         AND EXPTRANS1.RateDriverClass_alfa = ''L5''
        THEN ''8029''
    WHEN EXPTRANS1.StateTypecode IN (''10'', ''23'')
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa = ''Upto1.5Ton''
         AND EXPTRANS1.Radiusofuse_alfa = ''MileRadius300''
         AND EXPTRANS1.RateDriverClass_alfa = ''L5''
        THEN ''8039''
    WHEN EXPTRANS1.StateTypecode IN (''10'', ''23'')
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa = ''LessThan1Ton''
         AND EXPTRANS1.Radiusofuse_alfa IN (''MileRadius1-3'', ''MileRadius4-10'', ''MileRadius11-49'')
         AND EXPTRANS1.RateDriverClass_alfa = ''L5''
        THEN ''8019''
    WHEN EXPTRANS1.StateTypecode IN (''10'', ''23'')
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa IN (''LessThan1Ton'', ''Over1.5To3.5Ton'')
         AND EXPTRANS1.Radiusofuse_alfa IN (''MilesRadius300'', ''Over300Miles'')
         AND EXPTRANS1.RateDriverClass_alfa = ''M4''
        THEN ''8239''
    WHEN EXPTRANS1.StateTypecode IN (''10'', ''23'')
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa = ''LessThan1Ton''
         AND EXPTRANS1.Radiusofuse_alfa IN (''MileRadius1-3'', ''MileRadius4-10'', ''MileRadius11-49'')
         AND EXPTRANS1.RateDriverClass_alfa = ''M5''
        THEN ''8219''
    WHEN EXPTRANS1.StateTypecode IN (''10'', ''23'')
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa = ''Over1.5To3.5Ton''
         AND EXPTRANS1.Radiusofuse_alfa = ''MileRadius100''
         AND EXPTRANS1.RateDriverClass_alfa = ''M5''
        THEN ''8229''
    WHEN EXPTRANS1.StateTypecode IN (''10'', ''23'')
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa IN (''LessThan1Ton'', ''Over3.5Ton'')
         AND EXPTRANS1.Radiusofuse_alfa IN (''MilesRadius300'', ''Over300Miles'')
         AND EXPTRANS1.RateDriverClass_alfa = ''H4''
        THEN ''8339''
    WHEN EXPTRANS1.StateTypecode IN (''10'', ''23'')
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa = ''LessThan1Ton''
         AND EXPTRANS1.Radiusofuse_alfa IN (''MileRadius1-3'', ''MileRadius4-10'', ''MileRadius11-49'')
         AND EXPTRANS1.RateDriverClass_alfa = ''H5''
        THEN ''8319''
    WHEN EXPTRANS1.StateTypecode IN (''10'', ''23'')
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.Tonnage_alfa = ''Over3.5Ton''
         AND EXPTRANS1.Radiusofuse_alfa = ''MileRadius100''
         AND EXPTRANS1.RateDriverClass_alfa = ''H5''
        THEN ''8329''
    WHEN EXPTRANS1.StateTypecode IN (''10'', ''23'')
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.RateDriverClass_alfa = ''Q4''
        THEN ''5800''
    WHEN EXPTRANS1.StateTypecode IN (''10'', ''23'')
         AND EXPTRANS1.PolicyTypecode = ''COMMERCIAL''
         AND EXPTRANS1.RateDriverClass_alfa = ''RL''
        THEN ''8939''
    ELSE NULL
END AS Out_Classification, 
                  EXPTRANS1.source_record_id
           FROM   EXPTRANS1 );
    -- Component EXPTRANS41, Type EXPRESSION
    CREATE
    OR
    REPLACE TEMPORARY TABLE EXPTRANS41 AS
    (
              SELECT
                        CASE
                                  WHEN EXPTRANS.CompanyNumber IS NULL THEN ''''
                                  ELSE EXPTRANS.CompanyNumber
                        END             AS v_CompanyNumber,
                        v_CompanyNumber AS o_CompanyNumber,
                        EXPTRANS.LOB    AS LOB,
                        LKP_1.Classification
                        /* replaced lookup LKP_CLASSIFICATION */
                        AS v_lkp_claim,
                        LKP_2.Classification
                        /* replaced lookup LKP_CLASSIFICATION */
                        AS v_lkp_policy,
                        CASE
                                  WHEN (
                                                      EXPTRANS.Out_Classification IS NULL
                                            AND       EXPTRANS.PolicyTypecode = ''PPV'' ) THEN ''1220''
                                  ELSE (
                                            CASE
                                                      WHEN (
                                                                          EXPTRANS.Out_Classification IS NULL
                                                                AND       EXPTRANS.PolicyTypecode = ''COMMERCIAL'' ) THEN (
                                                                CASE
                                                                          WHEN EXPTRANS.PolicyNumber = ''0'' THEN v_lkp_claim
                                                                          ELSE v_lkp_policy
                                                                END )
                                                      ELSE EXPTRANS.Out_Classification
                                            END )
                        END               AS Classification121,
                        Classification121 AS Classification_out,
                        CASE
                                  WHEN EXPTRANS.StateOfPrincipalGarage IS NULL THEN ''''
                                  ELSE EXPTRANS.StateOfPrincipalGarage
                        END                      AS v_StateOfPrincipalGarage,
                        v_StateOfPrincipalGarage AS o_StateOfPrincipalGarage,
                        EXPTRANS.CallYear        AS CallYear,
                        EXPTRANS.AccountingYear  AS AccountingYear,
                        CASE
                                  WHEN EXPTRANS.ExpPeriodYear IS NULL THEN ''''
                                  ELSE EXPTRANS.ExpPeriodYear
                        END             AS v_ExpPeriodYear,
                        v_ExpPeriodYear AS o_ExpPeriodYear,
                        CASE
                                  WHEN EXPTRANS.ExpPeriodMonth IS NULL THEN ''''
                                  ELSE EXPTRANS.ExpPeriodMonth
                        END              AS v_ExpPeriodMonth,
                        v_ExpPeriodMonth AS o_ExpPeriodMonth,
                        CASE
                                  WHEN EXPTRANS.ExpPeriodDay IS NULL THEN ''''
                                  ELSE EXPTRANS.ExpPeriodDay
                        END            AS v_ExpPeriodDay,
                        v_ExpPeriodDay AS o_ExpPeriodDay,
                        CASE
                                  WHEN EXPTRANS.CoverageCode IS NULL THEN ''000''
                                  ELSE EXPTRANS.CoverageCode
                        END            AS v_CoverageCode,
                        v_CoverageCode AS o_CoverageCode,
                        CASE
                                  WHEN Classification121 IN ( 
                                           ''8299'' ,
                                           ''8399'' ,
                                           ''8939'' ,
                                           ''8239'' ,
                                           ''8339'' ) THEN ''00''
                                  ELSE lpad ( EXPTRANS.TerritoryCode , 2 , ''0'' )
                        END AS v_TerritoryCode,
                        TO_CHAR (
                        CASE
                                  WHEN v_TerritoryCode IS NULL THEN ''00''
                                  ELSE v_TerritoryCode
                        END ) AS o_TerritoryCode,
                        CASE
                                  WHEN EXPTRANS.Zipcode IS NULL THEN ''00000''
                                  ELSE lpad ( EXPTRANS.Zipcode , 5 , ''0'' )
                        END       AS v_Zipcode,
                        v_Zipcode AS o_Zipcode,
                        CASE
                                  WHEN EXPTRANS.Policy_Eff_Yr IS NULL THEN ''0000''
                                  ELSE EXPTRANS.Policy_Eff_Yr
                        END             AS v_Policy_Eff_Yr,
                        v_Policy_Eff_Yr AS o_Policy_Eff_Yr,
                        ''00''            AS StateExceptionCode,
                        CASE
                                  WHEN EXPTRANS.Ded_Ind_Code IS NULL THEN ''0''
                                  ELSE EXPTRANS.Ded_Ind_Code
                        END            AS v_Ded_Ind_Code,
                        v_Ded_Ind_Code AS o_Ded_Ind_Code,
                        CASE
                                  WHEN EXPTRANS.DeductibleAmount IS NULL THEN 0
                                  ELSE EXPTRANS.DeductibleAmount
                        END                AS v_DeductibleAmount,
                        v_DeductibleAmount AS o_DeductibleAmount,
                        ''00''               AS SublineCode,
                        CASE
                                  WHEN EXPTRANS.Mf_MDL_Yr IS NULL THEN ''0000''
                                  ELSE EXPTRANS.Mf_MDL_Yr
                        END                  AS v_Mf_MDL_Yr,
                        v_Mf_MDL_Yr          AS o_Mf_MDL_Yr,
                        ''0''                  AS AgeGroupCode,
                        ''0''                  AS AntiTheftCode,
                        ''0''                  AS DayTimeRunninglampCode,
                        EXPTRANS.Df_Drv_Code AS Df_Drv_Code,
                        ''0''                  AS ExceptionBCode,
                        CASE
                                  WHEN EXPTRANS.PolicyTerm IS NULL THEN ''0''
                                  ELSE EXPTRANS.PolicyTerm
                        END          AS v_PolicyTerm,
                        v_PolicyTerm AS o_PolicyTerm,
                        ''00''         AS PenaltyPoints,
                        ''0000''       AS PolicyLowerLimit,
                        ''0000''       AS PolicyUpperLimit,
                        ''10''         AS PolicyIDCode,
                        ''0''          AS PassiveRestraintCode,
                        ''0''          AS ForgivenessCode,
                        CASE
                                  WHEN Classification121 IN ( 
                                           ''8299'' ,
                                           ''8399'' ,
                                           ''8939'' ,
                                           ''8239'' ,
                                           ''8339'' )
                                  AND       UPPER ( EXPTRANS.CityInternal ) <> ''ATLANTA'' THEN ''9''
                                  ELSE
                                            CASE
                                                      WHEN Classification121 IN ( 
                                                               ''8299'' ,
                                                               ''8399'' ,
                                                               ''8939'' ,
                                                               ''8239'' ,
                                                               ''8339'' )
                                                      AND       UPPER ( EXPTRANS.CityInternal ) = ''ATLANTA'' THEN ''1''
                                                      ELSE ''0''
                                            END
                        END              AS v_Ratingzonecode,
                        v_Ratingzonecode AS o_Ratingzonecode,
                        CASE
                                  WHEN Classification121 IN ( 
                                           ''8299'' ,
                                           ''8399'' ,
                                           ''8939'' ,
                                           ''8239'' ,
                                           ''8339'' )
                                  AND       UPPER ( EXPTRANS.CityInternal ) <> ''ATLANTA'' THEN ''46''
                                  ELSE
                                            CASE
                                                      WHEN Classification121 IN ( 
                                                               ''8299'' ,
                                                               ''8399'' ,
                                                               ''8939'' ,
                                                               ''8239'' ,
                                                               ''8339'' )
                                                      AND       UPPER ( EXPTRANS.CityInternal ) = ''ATLANTA'' THEN ''01''
                                                      ELSE ''00''
                                            END
                        END                         AS v_Terminalzonecode,
                        v_Terminalzonecode          AS o_Terminalzonecode,
                        CURRENT_TIMESTAMP           AS CreateTS,
                        CURRENT_TIMESTAMP           AS UpdateTS,
                        ''000000000000''              AS OutStandingAllocLossAdjExp,
                        EXPTRANS.ClaimNumber        AS ClaimNumber,
                        EXPTRANS.ClaimantIdentifier AS ClaimantIdentifier,
                        CASE
                                  WHEN EXPTRANS.PaidLosses IS NULL THEN ''0''
                                  ELSE EXPTRANS.PaidLosses
                        END                 AS v_PaidLosses,
                        v_PaidLosses        AS o_PaidLosses,
                        EXPTRANS.PaidClaims AS PaidClaims,
                        CASE
                                  WHEN EXPTRANS.PaidAllocatedLossAdjExp IS NULL THEN ''0''
                                  ELSE EXPTRANS.PaidAllocatedLossAdjExp
                        END        AS v_PaidALAE,
                        v_PaidALAE AS o_PaidALAE,
                        CASE
                                  WHEN EXPTRANS.OutStandingLosses IS NULL THEN ''0''
                                  ELSE EXPTRANS.OutStandingLosses
                        END                        AS v_OutStandingLosses,
                        v_OutStandingLosses        AS o_OutStandingLosses,
                        EXPTRANS.OutStandingClaims AS OutStandingClaims,
                        CASE
                                  WHEN EXPTRANS.WrittenExposure1 IS NULL THEN ''0''
                                  ELSE EXPTRANS.WrittenExposure1
                        END               AS v_WrittenExposure,
                        v_WrittenExposure AS o_WrittenExposure,
                        CASE
                                  WHEN EXPTRANS.WrittenPremium1 IS NULL THEN ''0''
                                  ELSE EXPTRANS.WrittenPremium1
                        END                       AS v_WrittenPremium,
                        v_WrittenPremium          AS o_WrittenPremium,
                        EXPTRANS.PolicyNumber     AS PolicyNumber,
                        EXPTRANS.PolicyPeriodID   AS PolicyPeriodID,
                        EXPTRANS.PolicyIdentifier AS PolilcyIdentifier,
                        CASE
                                  WHEN EXPTRANS.VIN IS NULL THEN ''0''
                                  ELSE EXPTRANS.VIN
                        END   AS v_VIN,
                        v_VIN AS o_VIN,
                        CASE
                                  WHEN EXPTRANS.ExposureNumber IS NULL THEN ''0''
                                  ELSE EXPTRANS.ExposureNumber
                        END              AS v_ExposureNumber,
                        v_ExposureNumber AS o_ExposureNumber,
                        CASE
                                  WHEN EXPTRANS.O_Annual_Stmt_LOB IS NULL
                                  OR        EXPTRANS.O_Annual_Stmt_LOB = ''0''
                                  OR        EXPTRANS.O_Annual_Stmt_LOB = ''000'' THEN ''000''
                                  ELSE EXPTRANS.O_Annual_Stmt_LOB
                        END            AS v_Ann_Stmt_LOB,
                        v_Ann_Stmt_LOB AS o_Ann_Stmt_LOB,
                        CASE
                                  WHEN EXPTRANS.TypeOfLossCode IS NULL THEN ''00''
                                  ELSE EXPTRANS.TypeOfLossCode
                        END              AS v_TypeofLossCode,
                        v_TypeofLossCode AS o_TypeofLossCode,
                        ''0''              AS CreationUID,
                        ''0''              AS UpdateUID,
                        EXPTRANS.source_record_id,
                        row_number() over (PARTITION BY EXPTRANS.source_record_id ORDER BY EXPTRANS.source_record_id) AS RNK
              FROM      EXPTRANS
              LEFT JOIN LKP_CLASSIFICATION LKP_1
              ON        LKP_1.PolicyNumber = EXPTRANS.ClaimNumber
              LEFT JOIN LKP_CLASSIFICATION LKP_2
              ON        LKP_2.PolicyNumber = EXPTRANS.PolicyNumber QUALIFY RNK = 1 );
    -- Component OUT_NAIIPCI_PA_policy, Type TARGET
    INSERT INTO DB_T_PROD_COMN.OUT_NAIIPCI_PA
                (
                            CompanyNumber,
                            LOB,
                            StateOfPrincipalGarage,
                            CallYear,
                            AccountingYear,
                            ExpPeriodYear,
                            ExpPeriodMonth,
                            ExpPeriodDay,
                            CoverageCode,
                            ClassificationCode,
                            TypeOfLossCode,
                            TerritoryCode,
                            ZipCode,
                            PolicyEffectiveYear,
                            StateExceptionCode,
                            AnnualStatementLOB,
                            DeductibleIndicatorCode,
                            DeductibleAmount,
                            SublineCode,
                            ManufactureModelYear,
                            AgeGroupCode,
                            AntiTheftCode,
                            DayTimeRunninglampCode,
                            DefenseDriverCode,
                            ExceptionBCode,
                            PolicyTerm,
                            PenaltyPoints,
                            PolicyLowerLimit,
                            PolicyUpperLimit,
                            PolicyIDCode,
                            PassiveRestraintCode,
                            RatingZoneCode,
                            TerminalZoneCode,
                            ForgivenessCode,
                            ClaimNumber,
                            ClaimantIdentifier,
                            WrittenExposure,
                            WrittenPremium,
                            PaidLosses,
                            PaidClaims,
                            PaidAllocatedLossAdjExp,
                            OutStandingLosses,
                            OutStandingClaims,
                            OutStandingAllocLossAdjExp,
                            PolicyNumber,
                            PolicyPeriodID,
                            CreationTS,
                            CreationUID,
                            UpdateTS,
                            UpdateUID,
                            PolicyIdentifier,
                            VIN,
                            ExposureNumber
                )
    SELECT EXPTRANS41.o_CompanyNumber            AS CompanyNumber,
           EXPTRANS41.LOB                        AS LOB,
           EXPTRANS41.o_StateOfPrincipalGarage   AS StateOfPrincipalGarage,
           EXPTRANS41.CallYear                   AS CallYear,
           EXPTRANS41.AccountingYear             AS AccountingYear,
           EXPTRANS41.o_ExpPeriodYear            AS ExpPeriodYear,
           EXPTRANS41.o_ExpPeriodMonth           AS ExpPeriodMonth,
           EXPTRANS41.o_ExpPeriodDay             AS ExpPeriodDay,
           EXPTRANS41.o_CoverageCode             AS CoverageCode,
           EXPTRANS41.Classification_out         AS ClassificationCode,
           EXPTRANS41.o_TypeofLossCode           AS TypeOfLossCode,
           EXPTRANS41.o_TerritoryCode            AS TerritoryCode,
           EXPTRANS41.o_Zipcode                  AS ZipCode,
           EXPTRANS41.o_Policy_Eff_Yr            AS PolicyEffectiveYear,
           EXPTRANS41.StateExceptionCode         AS StateExceptionCode,
           EXPTRANS41.o_Ann_Stmt_LOB             AS AnnualStatementLOB,
           EXPTRANS41.o_Ded_Ind_Code             AS DeductibleIndicatorCode,
           EXPTRANS41.o_DeductibleAmount         AS DeductibleAmount,
           EXPTRANS41.SublineCode                AS SublineCode,
           EXPTRANS41.o_Mf_MDL_Yr                AS ManufactureModelYear,
           EXPTRANS41.AgeGroupCode               AS AgeGroupCode,
           EXPTRANS41.AntiTheftCode              AS AntiTheftCode,
           EXPTRANS41.DayTimeRunninglampCode     AS DayTimeRunninglampCode,
           EXPTRANS41.Df_Drv_Code                AS DefenseDriverCode,
           EXPTRANS41.ExceptionBCode             AS ExceptionBCode,
           EXPTRANS41.o_PolicyTerm               AS PolicyTerm,
           EXPTRANS41.PenaltyPoints              AS PenaltyPoints,
           EXPTRANS41.PolicyLowerLimit           AS PolicyLowerLimit,
           EXPTRANS41.PolicyUpperLimit           AS PolicyUpperLimit,
           EXPTRANS41.PolicyIDCode               AS PolicyIDCode,
           EXPTRANS41.PassiveRestraintCode       AS PassiveRestraintCode,
           EXPTRANS41.o_Ratingzonecode           AS RatingZoneCode,
           EXPTRANS41.o_Terminalzonecode         AS TerminalZoneCode,
           EXPTRANS41.ForgivenessCode            AS ForgivenessCode,
           EXPTRANS41.ClaimNumber                AS ClaimNumber,
           EXPTRANS41.ClaimantIdentifier         AS ClaimantIdentifier,
           EXPTRANS41.o_WrittenExposure          AS WrittenExposure,
           EXPTRANS41.o_WrittenPremium           AS WrittenPremium,
           EXPTRANS41.o_PaidLosses               AS PaidLosses,
           EXPTRANS41.PaidClaims                 AS PaidClaims,
           EXPTRANS41.o_PaidALAE                 AS PaidAllocatedLossAdjExp,
           EXPTRANS41.o_OutStandingLosses        AS OutStandingLosses,
           EXPTRANS41.OutStandingClaims          AS OutStandingClaims,
           EXPTRANS41.OutStandingAllocLossAdjExp AS OutStandingAllocLossAdjExp,
           EXPTRANS41.PolicyNumber               AS PolicyNumber,
           EXPTRANS41.PolicyPeriodID             AS PolicyPeriodID,
           EXPTRANS41.CreateTS                   AS CreationTS,
           EXPTRANS41.CreationUID                AS CreationUID,
           EXPTRANS41.UpdateTS                   AS UpdateTS,
           EXPTRANS41.UpdateUID                  AS UpdateUID,
           EXPTRANS41.PolilcyIdentifier          AS PolicyIdentifier,
           EXPTRANS41.o_VIN                      AS VIN,
           EXPTRANS41.o_ExposureNumber           AS ExposureNumber
    FROM   EXPTRANS41;
    
    -- PIPELINE END FOR 2
  END;
  ';