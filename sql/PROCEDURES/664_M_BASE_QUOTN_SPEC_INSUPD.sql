-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_QUOTN_SPEC_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE 
run_id varchar;
start_dttm timestamp;
end_dttm timestamp;
prcs_id int;


BEGIN 
 run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
 PRCS_ID:=     (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);


-- Component LKP_TERADATA_ETL_REF_XLAT_AGMT_SPEC_TYPE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_AGMT_SPEC_TYPE_CD AS
(
SELECT  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL,  TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL 

FROM DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''AGMT_SPEC_TYPE''

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN( ''pctl_affinitygroup_alfa.typecode'' ,''derived'',''pc_effectivedatedfields.PriorCarrierExpDate_alfa'',''pc_effectivedatedfields.LatestDtOfLapses_alfa'',''pc_effectivedatedfields.LapsesInContService_alfa'',''pc_effectivedatedfields.othercarrier_alfa'',''pcx_puprenreviewdetails_alfa.IsSubmitted'')

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN ( ''DS'',''GW'')

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' -- ORDER BY SRC_IDNTFTN_VAL,TGT_IDNTFTN_VAL
);


-- Component LKP_TERADATA_ETL_REF_XLAT_SPEC_TYPE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_SPEC_TYPE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SPEC_TYPE''

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM

in ( ''pctl_number_alfa.TYPECODE'',''pctl_affinitygrouptype_alfa.typecode'',''pctl_agentassignmenttype_alfa.typecode'' ,''pctl_fopfarmingoperations.typecode'')

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS= ''GW''

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' --
);


-- PIPELINE START FOR 1

-- Component sq_pc_policyperiod, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_pc_policyperiod AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Jobnumber,
$2 as Branchnumber,
$3 as PolicyNumber,
$4 as PublicID,
$5 as SPEC_TYPE_CD,
$6 as EffectiveDate,
$7 as ExpirationDate,
$8 as TRANS_STRT_DTTM,
$9 as QUOTN_SPEC_STRT_DTTM,
$10 as QUOTN_SPEC_TYPE_CD,
$11 as QUOTN_SPEC_IND,
$12 as QUOTN_SPEC_DT,
$13 as QUOTN_SPEC_TXT,
$14 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT  jobnumber_stg,branchnumber_stg, PolicyNumber,cast(null as varchar(20)) PublicID,cast(SPEC_TYPE_CD as varchar(100))SPEC_TYPE_CD,

        EffectiveDate,ExpirationDate,TRANS_STRT_DTTM,QUOTN_SPEC_STRT_DTTM,cast(QUOTN_SPEC_TYPE_CD as varchar(100))QUOTN_SPEC_TYPE_CD,

        QUOTN_SPEC_IND,QUOTN_SPEC_DT,Quotn_SPEC_TXT

FROM    ( 

    SELECT  PJ.JobNumber_stg, a.branchnumber_stg, a.PolicyNumber_stg as PolicyNumber,

            cast(null as varchar(20))as PublicID, c.typecode_stg as SPEC_TYPE_CD,

            b.UpdateTime_stg as spec_updatetime, b.EffectiveDate_stg as EffectiveDate,

            b.ExpirationDate_stg as ExpirationDate, a.UpdateTime_stg as TRANS_STRT_DTTM,

            a.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, ''AGMT_SPEC1'' AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE )as QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

    FROM    DB_T_PROD_STAG.pc_policyperiod a

    JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=a.JOBID_STG

    JOIN DB_T_PROD_STAG.pc_policyline b 

        ON b.BranchID_stg = a.ID_stg 

        AND b.ExpirationDate_stg IS NULL

    JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ps 

        ON ps.ID_stg = a.Status_stg

     JOIN  DB_T_PROD_STAG.pctl_number_alfa c 

        ON c.ID_stg = b.LatePayCount_alfa_stg

    WHERE   ps.TYPECODE_stg <> ''Temporary''

        AND b.UpdateTime_stg >:start_dttm

        AND b.UpdateTime_stg <=:end_dttm   

    )quotn_spec_x  

where   QUOTN_SPEC_TYPE_CD =''AGMT_SPEC1''

    QUALIFY  ROW_NUMBER() OVER  (

partition by JobNumber_stg,branchnumber_stg

order by quotn_spec_x.spec_updatetime desc) =1



UNION

SELECT   jobnumber_stg,branchnumber_stg, PolicyNumber,

        

        cast(null as varchar(20))PublicID,

        SPEC_TYPE_CD,

        EffectiveDate,

       ExpirationDate,

        TRANS_STRT_DTTM,

        QUOTN_SPEC_STRT_DTTM,

        QUOTN_SPEC_TYPE_CD,

QUOTN_SPEC_IND,

QUOTN_SPEC_DT,Quotn_SPEC_TXT

FROM    (  

    SELECT   PJ.JobNumber_stg, a.branchnumber_stg,   a.PolicyNumber_stg as PolicyNumber,

            cast(null as varchar(20)) as PublicID, cast(null as varchar(50)) as SPEC_TYPE_CD,

            b.UpdateTime_stg as spec_updatetime, b.EffectiveDate_stg as EffectiveDate,

            b.ExpirationDate_stg as ExpirationDate, a.UpdateTime_stg as TRANS_STRT_DTTM,

            a.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, ''AGMT_SPEC2'' AS QUOTN_SPEC_TYPE_CD,

            cast(b.IsNamedPerilExistOnPolicy_alfa_stg as varchar(100)) as QUOTN_SPEC_IND,

            cast(NULL as DATE )as QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

    FROM    DB_T_PROD_STAG.pc_policyperiod a

    JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=a.JOBID_STG

    JOIN DB_T_PROD_STAG.pc_policyline b 

        ON b.BranchID_stg = a.ID_stg 

        AND b.ExpirationDate_stg IS NULL

    JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ps 

        ON ps.ID_stg = a.Status_stg

     

    WHERE   ps.TYPECODE_stg <> ''Temporary'' 

        and cast(b.IsNamedPerilExistOnPolicy_alfa_stg as varchar(100)) = ''1''

        AND b.UpdateTime_stg >:start_dttm

        AND b.UpdateTime_stg <=:end_dttm)quotn_spec_x  

where   QUOTN_SPEC_TYPE_CD =''AGMT_SPEC2''

    QUALIFY  ROW_NUMBER() OVER  (

partition by JobNumber_stg, branchnumber_stg 

order by quotn_spec_x.spec_updatetime desc) =1



UNION

SELECT jobnumber_stg,branchnumber_stg,   PolicyNumber,       

        cast(null as varchar(20)) PublicID,

        SPEC_TYPE_CD,

        EffectiveDate,

        ExpirationDate,

        TRANS_STRT_DTTM,

        QUOTN_SPEC_STRT_DTTM,

        QUOTN_SPEC_TYPE_CD,

        QUOTN_SPEC_IND

        ,QUOTN_SPEC_DT,Quotn_SPEC_TXT

FROM    (

  SELECT   PJ.JobNumber_stg, a.branchnumber_stg, CAST(a.PolicyNumber_stg AS VARCHAR(100))as PolicyNumber,

            cast(null as varchar(20)) as PublicID, CAST(c.typecode_stg AS VARCHAR(100))as SPEC_TYPE_CD,

            b.UpdateTime_stg as spec_updatetime, b.EffectiveDate_stg as EffectiveDate,

            b.ExpirationDate_stg as ExpirationDate, a.UpdateTime_stg as TRANS_STRT_DTTM,

            a.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, CAST(''PriorCarrierExpDate_alfa''AS VARCHAR(100)) AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            pce.PriorCarrierExpDate_alfa_stg  as QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

    FROM    DB_T_PROD_STAG.pc_policyperiod a

    JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=a.JOBID_STG

    JOIN DB_T_PROD_STAG.pc_effectivedatedfields pce 

        ON pce.BranchID_stg = a.ID_stg and pce.ExpirationDate_stg is null

    JOIN DB_T_PROD_STAG.pc_policyline b 

        ON b.BranchID_stg = a.ID_stg 

        AND b.ExpirationDate_stg IS NULL

    JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ps 

        ON ps.ID_stg = a.Status_stg

    LEFT JOIN  DB_T_PROD_STAG.pctl_number_alfa c 

        ON c.ID_stg = b.LatePayCount_alfa_stg

    WHERE   ps.TYPECODE_stg <> ''Temporary''

        AND b.UpdateTime_stg >:start_dttm

        AND b.UpdateTime_stg <=:end_dttm

        AND pce.PriorCarrierExpDate_alfa_stg is not null

        )quotn_spec_x  

        where QUOTN_SPEC_TYPE_CD =''PriorCarrierExpDate_alfa''

QUALIFY  ROW_NUMBER() OVER  (partition by JOBNUMBER_STG,BRANCHNUMBER_STG

 order by quotn_spec_x.spec_updatetime desc) =1



 

 

  /*Relationship DB_T_CORE_DM_PROD.discount counts of other DB_T_CORE_DM_PROD.policy types  */ 



union



select jobnumber_stg, branchnumber_stg,PolicyNumber, PublicID,SPEC_TYPE_CD,EffectiveDate,ExpirationDate,TRANS_STRT_DTTM,QUOTN_SPEC_STRT_DTTM,QUOTN_SPEC_TYPE_CD,QUOTN_SPEC_IND,QUOTN_SPEC_DT,

 Quotn_SPEC_TXT from ( 

select distinct   PJ.JobNumber_stg, pp.branchnumber_stg, 

        CAST(pp.PolicyNumber_stg AS  VARCHAR(100))as PolicyNumber,

            cast(null as varchar(20)) as PublicID, CAST(nbrHO3.typecode_stg AS VARCHAR(100))as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, cast(''AGMT_SPEC3''  as varchar(100)) AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and pl.ExpirationDate_stg is NULL and pp.Status_stg <>''2''

join DB_T_PROD_STAG.pctl_papolicytype_alfa pa on pa.ID_stg = pl.PAPolicyType_alfa_stg

join DB_T_PROD_STAG.pc_personalvehicle ppv on ppv.BranchID_stg = pp.ID_stg

join DB_T_PROD_STAG.pc_pamodifier dis on dis.BranchID_stg = pp.ID_stg and dis.ExpirationDate_stg is NULL

join DB_T_PROD_STAG.pcx_relationshipdetails_alfa rldet on rldet.ID_stg = pl.RelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrHO3 on nbrHO3.ID_stg = rldet.NumHO3pol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''PARelationshipDiscount_alfa''

and nbrHO3.typecode_stg is not null

AND pl.UpdateTime_stg >:start_dttm

AND pl.UpdateTime_stg <=:end_dttm

union

select distinct   PJ.JobNumber_stg, pp.branchnumber_stg,

        pp.PolicyNumber_stg as PolicyNumber,

            cast(null as varchar(20))as PublicID, nbrHO4.typecode_stg as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, ''AGMT_SPEC4'' AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and pl.ExpirationDate_stg is NULL and pp.Status_stg <>''2''

join DB_T_PROD_STAG.pctl_papolicytype_alfa pa on pa.ID_stg = pl.PAPolicyType_alfa_stg

join DB_T_PROD_STAG.pc_personalvehicle ppv on ppv.BranchID_stg = pp.ID_stg

join DB_T_PROD_STAG.pc_pamodifier dis on dis.BranchID_stg = pp.ID_stg and dis.ExpirationDate_stg is NULL

join DB_T_PROD_STAG.pcx_relationshipdetails_alfa rldet on rldet.ID_stg = pl.RelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrHO4 on nbrHO4.ID_stg = rldet.NumHO4pol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''PARelationshipDiscount_alfa''

and nbrHO4.typecode_stg is not null

AND pl.UpdateTime_stg >:start_dttm

AND pl.UpdateTime_stg <=:end_dttm

union 

select distinct   PJ.JobNumber_stg, pp.branchnumber_stg,

        pp.PolicyNumber_stg as PolicyNumber,

            cast(null as varchar(20)) as PublicID, nbrHO5.typecode_stg as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, ''AGMT_SPEC5'' AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and pl.ExpirationDate_stg is NULL and pp.Status_stg<>''2''

join DB_T_PROD_STAG.pctl_papolicytype_alfa pa on pa.ID_stg = pl.PAPolicyType_alfa_stg

join DB_T_PROD_STAG.pc_personalvehicle ppv on ppv.BranchID_stg = pp.ID_stg

join DB_T_PROD_STAG.pc_pamodifier dis on dis.BranchID_stg = pp.ID_stg and dis.ExpirationDate_stg is NULL

join DB_T_PROD_STAG.pcx_relationshipdetails_alfa rldet on rldet.ID_stg = pl.RelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrHO5 on nbrHO5.ID_stg = rldet.NumHO5pol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''PARelationshipDiscount_alfa''

and nbrHO5.typecode_stg is not null

AND pl.UpdateTime_stg >:start_dttm

AND pl.UpdateTime_stg <=:end_dttm

union

select distinct  

         PJ.JobNumber_stg, pp.branchnumber_stg,pp.PolicyNumber_stg as PolicyNumber,

            cast(null as varchar(20))as PublicID, nbrHO6.typecode_stg as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, ''AGMT_SPEC6'' AS QUOTN_SPEC_TYPE,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT 

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and pl.ExpirationDate_stg is NULL and pp.Status_stg <>''2''

join DB_T_PROD_STAG.pctl_papolicytype_alfa pa on pa.ID_stg = pl.PAPolicyType_alfa_stg

join DB_T_PROD_STAG.pc_personalvehicle ppv on ppv.BranchID_stg = pp.ID_stg

join DB_T_PROD_STAG.pc_pamodifier dis on dis.BranchID_stg = pp.ID_stg and dis.ExpirationDate_stg is NULL

join DB_T_PROD_STAG.pcx_relationshipdetails_alfa rldet on rldet.ID_stg = pl.RelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrHO6 on nbrHO6.ID_stg = rldet.NumHO6pol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''PARelationshipDiscount_alfa''

and nbrHO6.typecode_stg is not null

AND pl.UpdateTime_stg >:start_dttm

AND pl.UpdateTime_stg <=:end_dttm

union

select distinct  

         PJ.JobNumber_stg, pp.branchnumber_stg,pp.PolicyNumber_stg as PolicyNumber,

            cast(null as varchar(20)) as PublicID, nbrHO8.typecode_stg as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, ''AGMT_SPEC7'' AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT 

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and pl.ExpirationDate_stg is NULL and pp.Status_stg <>''2''

join DB_T_PROD_STAG.pctl_papolicytype_alfa pa on pa.ID_stg = pl.PAPolicyType_alfa_stg

join DB_T_PROD_STAG.pc_personalvehicle ppv on ppv.BranchID_stg = pp.ID_stg

join DB_T_PROD_STAG.pc_pamodifier dis on dis.BranchID_stg = pp.ID_stg and dis.ExpirationDate_stg is NULL

join DB_T_PROD_STAG.pcx_relationshipdetails_alfa rldet on rldet.ID_stg = pl.RelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrHO8 on nbrHO8.ID_stg = rldet.NumHO8pol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''PARelationshipDiscount_alfa''

and nbrHO8.typecode_stg is not null

AND pl.UpdateTime_stg >:start_dttm

AND pl.UpdateTime_stg <=:end_dttm

union

select distinct  

         PJ.JobNumber_stg, pp.branchnumber_stg,pp.PolicyNumber_stg as PolicyNumber,

            cast(null as varchar(20)) as PublicID, nbrSF.typecode_stg as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, ''AGMT_SPEC8'' AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and pl.ExpirationDate_stg is NULL and pp.Status_stg <>''2''

join DB_T_PROD_STAG.pctl_papolicytype_alfa pa on pa.ID_stg = pl.PAPolicyType_alfa_stg

join DB_T_PROD_STAG.pc_personalvehicle ppv on ppv.BranchID_stg = pp.ID_stg

join DB_T_PROD_STAG.pc_pamodifier dis on dis.BranchID_stg = pp.ID_stg and dis.ExpirationDate_stg is NULL

join DB_T_PROD_STAG.pcx_relationshipdetails_alfa rldet on rldet.ID_stg = pl.RelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrSF on nbrSF.ID_stg = rldet.NumSFpol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''PARelationshipDiscount_alfa''

and nbrSF.typecode_stg is not null

AND pl.UpdateTime_stg >:start_dttm

AND pl.UpdateTime_stg <=:end_dttm

union

select distinct  

     PJ.JobNumber_stg, pp.branchnumber_stg, pp.PolicyNumber_stg as PolicyNumber,

            cast(null as varchar(20)) as PublicID, nbrMH.typecode_stg as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, ''AGMT_SPEC9'' AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and pl.ExpirationDate_stg is NULL and pp.Status_stg <>''2''

join DB_T_PROD_STAG.pctl_papolicytype_alfa pa on pa.ID_stg = pl.PAPolicyType_alfa_stg

join DB_T_PROD_STAG.pc_personalvehicle ppv on ppv.BranchID_stg = pp.ID_stg

join DB_T_PROD_STAG.pc_pamodifier dis on dis.BranchID_stg = pp.ID_stg and dis.ExpirationDate_stg is NULL

join DB_T_PROD_STAG.pcx_relationshipdetails_alfa rldet on rldet.ID_stg = pl.RelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrMH on nbrMH.ID_stg = rldet.NumMHpol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''PARelationshipDiscount_alfa''

and nbrMH.typecode_stg is not null

AND pl.UpdateTime_stg >:start_dttm

AND pl.UpdateTime_stg <=:end_dttm

union

select distinct  

         PJ.JobNumber_stg, pp.branchnumber_stg,pp.PolicyNumber_stg as PolicyNumber,

            cast(null as varchar(20)) as PublicID, nbrLife.typecode_stg as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, ''AGMT_SPEC10'' AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and pl.ExpirationDate_stg is NULL and pp.Status_stg <>''2''

join DB_T_PROD_STAG.pctl_papolicytype_alfa pa on pa.ID_stg = pl.PAPolicyType_alfa_stg

join DB_T_PROD_STAG.pc_personalvehicle ppv on ppv.BranchID_stg = pp.ID_stg

join DB_T_PROD_STAG.pc_pamodifier dis on dis.BranchID_stg = pp.ID_stg and dis.ExpirationDate_stg is NULL

join DB_T_PROD_STAG.pcx_relationshipdetails_alfa rldet on rldet.ID_stg = pl.RelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrLife on nbrLife.ID_stg = rldet.NumLifepol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''PARelationshipDiscount_alfa''

and nbrLife.typecode_stg is not null

AND pl.UpdateTime_stg >:start_dttm

AND pl.UpdateTime_stg <=:end_dttm

union

select distinct  

         PJ.JobNumber_stg, pp.branchnumber_stg,pp.PolicyNumber_stg as PolicyNumber,

            cast(null as varchar(20)) as PublicID, nbrFarm.typecode_stg as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, ''AGMT_SPEC11'' AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and pl.ExpirationDate_stg is NULL and pp.Status_stg <>''2''

join DB_T_PROD_STAG.pctl_papolicytype_alfa pa on pa.ID_stg = pl.PAPolicyType_alfa_stg

join DB_T_PROD_STAG.pc_personalvehicle ppv on ppv.BranchID_stg = pp.ID_stg

join DB_T_PROD_STAG.pc_pamodifier dis on dis.BranchID_stg = pp.ID_stg and dis.ExpirationDate_stg is NULL

join DB_T_PROD_STAG.pcx_relationshipdetails_alfa rldet on rldet.ID_stg = pl.RelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrFarm on nbrFarm.ID_stg = rldet.NumFarmpol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''PARelationshipDiscount_alfa''

and nbrFarm.typecode_stg is not null

AND pl.UpdateTime_stg >:start_dttm

AND pl.UpdateTime_stg <=:end_dttm



/* EIM-50809 AutoInd */
union



select distinct  

         PJ.JobNumber_stg, pp.branchnumber_stg,pp.PolicyNumber_stg as PolicyNumber,

         cast(null as varchar(20)) as PublicID, cast(NULL as varchar(100)) as SPEC_TYPE_CD,

         pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

         pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

         pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, ''AGMT_SPEC24'' AS QUOTN_SPEC_TYPE_CD,

         cast(case when pcx_HOrelationshipdetail_alfa.AutoInd_stg = ''1'' then ''YES''else pcx_HOrelationshipdetail_alfa.AutoInd_stg end as varchar(100))as QUOTN_SPEC_IND,

         cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp

join  DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and pl.ExpirationDate_stg is NULL and pp.status_stg <> ''2'' 

join DB_T_PROD_STAG.pctl_hopolicytype_hoe on pctl_hopolicytype_hoe.ID_stg = pl.HOPolicyType_stg 

join DB_T_PROD_STAG.pcx_homodifier_hoe on pcx_homodifier_hoe.BranchID_stg = pp.ID_stg and pcx_homodifier_hoe.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pcx_HOrelationshipdetail_alfa on pcx_HOrelationshipdetail_alfa.ID_stg = pl.HORelationshipDetails_alfa_stg

where pcx_homodifier_hoe.Eligible_stg = 1

and pcx_homodifier_hoe.PatternCode_stg = ''HORelationshipDisc_alfa'' 

and pcx_HOrelationshipdetail_alfa.AutoInd_stg <> 0

AND pl.UpdateTime_stg >:start_dttm

AND pl.UpdateTime_stg <=:end_dttm

/* Eim-50810 */
union



select distinct jobnumber_stg, branchnumber_stg,policynumber_stg,

cast(a.PublicID_stg as varchar(20)) as PublicID, 

pctl.typecode_stg AS SPEC_TYPE_CD, 

a.UpdateTime_stg as spec_updatetime,

 ppl1.EffectiveDate_stg as EffectiveDate,

ppl1.ExpirationDate_stg as ExpirationDate, 

a.UpdateTime_stg as TRANS_STRT_DTTM, 

a.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, 

''AGMT_SPEC23'' AS QUOTN_SPEC_TYPE_CD, 

cast(NULL as varchar(100))as QUOTN_SPEC_IND,

cast(NULL as DATE )as QUOTN_SPEC_DT,

cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod a

JOIN DB_T_PROD_STAG.pc_policyline ppl1 ON ppl1.BranchID_stg = a.ID_stg AND ppl1.ExpirationDate_stg IS NULL

JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_stg=a.JOBID_stg

JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ps ON ps.ID_stg = a.Status_stg

JOIN DB_T_PROD_STAG.pcx_fopfarmingoperations pcx on pcx.BranchID_stg = a.ID_stg

left join DB_T_PROD_STAG.pctl_fopfarmingoperations pctl on pctl.id_stg = pcx.FarmingOperationType_stg

WHERE ps.Typecode_stg <> ''Temporary'' 

AND a.status_stg <> 2

AND a.UpdateTime_stg > :start_dttm 

AND a.UpdateTime_stg <= :end_dttm



UNION

/*  HO3  */


select distinct   PJ.JobNumber_stg, pp.branchnumber_stg, 

        CAST(pp.PolicyNumber_stg AS  VARCHAR(100))as PolicyNumber,

            cast(null as varchar(20)) as PublicID, CAST(nbrHO3.typecode_stg AS VARCHAR(100))as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, cast(''AGMT_SPEC3''  as varchar(100)) AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and pl.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pctl_hopolicytype_hoe pa on pa.ID_stg = pl.HOPolicyType_stg

join DB_T_PROD_STAG.pcx_homodifier_hoe dis on dis.BranchID_stg = pp.ID_stg and dis.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pcx_HOrelationshipdetail_alfa rldet on rldet.ID_stg = pl.HORelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrHO3 on nbrHO3.ID_stg = rldet.NumHO3pol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''HORelationshipDisc_alfa''

and nbrHO3.typecode_stg is not null

and pp.Status_stg <> 2

AND pl.UpdateTime_stg > :start_dttm

AND pl.UpdateTime_stg <= :end_dttm



UNION

/*  HO2 */


select distinct   PJ.JobNumber_stg, pp.branchnumber_stg, 

        CAST(pp.PolicyNumber_stg AS  VARCHAR(100))as PolicyNumber,

            cast(null as varchar(20)) as PublicID, CAST(nbrHO2.typecode_stg AS VARCHAR(100))as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, cast(''AGMT_SPEC25''  as varchar(100)) AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and  pl.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pctl_hopolicytype_hoe pa on pa.ID_stg = pl.HOPolicyType_stg

join DB_T_PROD_STAG.pcx_homodifier_hoe dis on dis.BranchID_stg = pp.ID_stg and   dis.ExpirationDate_stg is NULL

join DB_T_PROD_STAG.pcx_HOrelationshipdetail_alfa rldet on rldet.ID_stg = pl.HORelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrHO2 on nbrHO2.ID_stg = rldet.NumHO2pol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''HORelationshipDisc_alfa''

and nbrHO2.typecode_stg is not null

and pp.Status_stg <> 2

AND pl.UpdateTime_stg > :start_dttm

AND pl.UpdateTime_stg <= :end_dttm



UNION

/*  HO4 */


select distinct   PJ.JobNumber_stg, pp.branchnumber_stg, 

        CAST(pp.PolicyNumber_stg AS  VARCHAR(100))as PolicyNumber,

            cast(null as varchar(20)) as PublicID, CAST(nbrHO4.typecode_stg AS VARCHAR(100))as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, cast(''AGMT_SPEC4''  as varchar(100)) AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and  pl.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pctl_hopolicytype_hoe pa on pa.ID_stg = pl.HOPolicyType_stg

join DB_T_PROD_STAG.pcx_homodifier_hoe dis on dis.BranchID_stg = pp.ID_stg and   dis.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pcx_HOrelationshipdetail_alfa rldet on rldet.ID_stg = pl.HORelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrHO4 on nbrHO4.ID_stg = rldet.NumHO4pol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''HORelationshipDisc_alfa''

and nbrHO4.typecode_stg is not null

and pp.Status_stg <> 2

AND pl.UpdateTime_stg > :start_dttm

AND pl.UpdateTime_stg <= :end_dttm



UNION

/*  HO5 */


select distinct   PJ.JobNumber_stg, pp.branchnumber_stg, 

        CAST(pp.PolicyNumber_stg AS  VARCHAR(100))as PolicyNumber,

            cast(null as varchar(20)) as PublicID, CAST(nbrHO5.typecode_stg AS VARCHAR(100))as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, cast(''AGMT_SPEC5''  as varchar(100)) AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and  pl.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pctl_hopolicytype_hoe pa on pa.ID_stg = pl.HOPolicyType_stg

join DB_T_PROD_STAG.pcx_homodifier_hoe dis on dis.BranchID_stg = pp.ID_stg and   dis.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pcx_HOrelationshipdetail_alfa rldet on rldet.ID_stg = pl.HORelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrHO5 on nbrHO5.ID_stg = rldet.NumHO5pol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''HORelationshipDisc_alfa''

and nbrHO5.typecode_stg is not null

and pp.Status_stg <> 2

AND pl.UpdateTime_stg > :start_dttm

AND pl.UpdateTime_stg <= :end_dttm



UNION

/*  HO6  */


select distinct   PJ.JobNumber_stg, pp.branchnumber_stg, 

        CAST(pp.PolicyNumber_stg AS  VARCHAR(100))as PolicyNumber,

            cast(null as varchar(20)) as PublicID, CAST(nbrHO6.typecode_stg AS VARCHAR(100))as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, cast(''AGMT_SPEC6''  as varchar(100)) AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and  pl.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pctl_hopolicytype_hoe pa on pa.ID_stg = pl.HOPolicyType_stg

join DB_T_PROD_STAG.pcx_homodifier_hoe dis on dis.BranchID_stg = pp.ID_stg and   dis.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pcx_HOrelationshipdetail_alfa rldet on rldet.ID_stg = pl.HORelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrHO6 on nbrHO6.ID_stg = rldet.NumHO6pol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''HORelationshipDisc_alfa''

and nbrHO6.typecode_stg is not null

and pp.Status_stg <> 2

AND pl.UpdateTime_stg > :start_dttm

AND pl.UpdateTime_stg <= :end_dttm



UNION

/*  HO8  */




select distinct   PJ.JobNumber_stg, pp.branchnumber_stg, 

        CAST(pp.PolicyNumber_stg AS  VARCHAR(100))as PolicyNumber,

            cast(null as varchar(20)) as PublicID, CAST(nbrHO8.typecode_stg AS VARCHAR(100))as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, cast(''AGMT_SPEC7''  as varchar(100)) AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and  pl.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pctl_hopolicytype_hoe pa on pa.ID_stg = pl.HOPolicyType_stg

join DB_T_PROD_STAG.pcx_homodifier_hoe dis on dis.BranchID_stg = pp.ID_stg and  dis.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pcx_HOrelationshipdetail_alfa rldet on rldet.ID_stg = pl.HORelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrHO8 on nbrHO8.ID_stg = rldet.NumHO8pol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''HORelationshipDisc_alfa''

and nbrHO8.typecode_stg is not null

and pp.Status_stg <> 2

AND pl.UpdateTime_stg > :start_dttm

AND pl.UpdateTime_stg <= :end_dttm



UNION

/*  SF  */


select distinct   PJ.JobNumber_stg, pp.branchnumber_stg, 

        CAST(pp.PolicyNumber_stg AS  VARCHAR(100))as PolicyNumber,

            cast(null as varchar(20)) as PublicID, CAST(nbrSF.typecode_stg AS VARCHAR(100))as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, cast(''AGMT_SPEC8''  as varchar(100)) AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and  pl.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pctl_hopolicytype_hoe pa on pa.ID_stg = pl.HOPolicyType_stg

join DB_T_PROD_STAG.pcx_homodifier_hoe dis on dis.BranchID_stg = pp.ID_stg and   dis.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pcx_HOrelationshipdetail_alfa rldet on rldet.ID_stg = pl.HORelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrSF on nbrSF.ID_stg = rldet.NumSFpol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''HORelationshipDisc_alfa''

and nbrSF.typecode_stg is not null

and pp.Status_stg <> 2

AND pl.UpdateTime_stg > :start_dttm

AND pl.UpdateTime_stg <= :end_dttm



UNION

/*  MH  */


select distinct   PJ.JobNumber_stg, pp.branchnumber_stg, 

        CAST(pp.PolicyNumber_stg AS  VARCHAR(100))as PolicyNumber,

            cast(null as varchar(20)) as PublicID, CAST(nbrMH.typecode_stg AS VARCHAR(100))as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, cast(''AGMT_SPEC9''  as varchar(100)) AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and  pl.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pctl_hopolicytype_hoe pa on pa.ID_stg = pl.HOPolicyType_stg

join DB_T_PROD_STAG.pcx_homodifier_hoe dis on dis.BranchID_stg = pp.ID_stg and   dis.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pcx_HOrelationshipdetail_alfa rldet on rldet.ID_stg = pl.HORelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrMH on nbrMH.ID_stg = rldet.NumMHpol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''HORelationshipDisc_alfa''

and nbrMH.typecode_stg is not null

and pp.Status_stg <> 2

AND pl.UpdateTime_stg > :start_dttm

AND pl.UpdateTime_stg <= :end_dttm



UNION

/*  DB_T_STAG_MEMBXREF_PROD.Life */


select distinct   PJ.JobNumber_stg, pp.branchnumber_stg, 

        CAST(pp.PolicyNumber_stg AS  VARCHAR(100))as PolicyNumber,

            cast(null as varchar(20)) as PublicID, CAST(nbrLife.typecode_stg AS VARCHAR(100))as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, cast(''AGMT_SPEC10''  as varchar(100)) AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and pl.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pctl_hopolicytype_hoe pa on pa.ID_stg = pl.HOPolicyType_stg

join DB_T_PROD_STAG.pcx_homodifier_hoe dis on dis.BranchID_stg = pp.ID_stg and  dis.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pcx_HOrelationshipdetail_alfa rldet on rldet.ID_stg = pl.HORelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrLife on nbrLife.ID_stg = rldet.NumLifepol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''HORelationshipDisc_alfa''

and nbrLife.typecode_stg is not null

and pp.Status_stg <> 2

AND pl.UpdateTime_stg > :start_dttm

AND pl.UpdateTime_stg <= :end_dttm



UNION

/* Farm */


select distinct   PJ.JobNumber_stg, pp.branchnumber_stg, 

        CAST(pp.PolicyNumber_stg AS  VARCHAR(100))as PolicyNumber,

            cast(null as varchar(20)) as PublicID, CAST(nbrFarm.typecode_stg AS VARCHAR(100))as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, cast(''AGMT_SPEC11''  as varchar(100)) AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and  pl.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pctl_hopolicytype_hoe pa on pa.ID_stg = pl.HOPolicyType_stg

join DB_T_PROD_STAG.pcx_homodifier_hoe dis on dis.BranchID_stg = pp.ID_stg and   dis.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pcx_HOrelationshipdetail_alfa rldet on rldet.ID_stg = pl.HORelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrFarm on nbrFarm.ID_stg = rldet.NumFarmpol_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''HORelationshipDisc_alfa''

and nbrFarm.typecode_stg is not null

and pp.Status_stg <> 2

AND pl.UpdateTime_stg > :start_dttm

AND pl.UpdateTime_stg <= :end_dttm





UNION

/*  Watercraft  */


select distinct  PJ.JobNumber_stg, pp.branchnumber_stg, 

        CAST(pp.PolicyNumber_stg AS  VARCHAR(100))as PolicyNumber,

            CAST(pp.PublicID_stg AS VARCHAR(100)) as PublicID, CAST(NULL AS VARCHAR(100))as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, cast(''AGMT_SPEC13''  as varchar(100)) AS QUOTN_SPEC_TYPE_CD,

            cast(case when rldet.WatercraftInd_stg = ''1'' then ''YES'' else rldet.WatercraftInd_stg end as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT , cast(NULL as VARCHAR(255) )as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp  JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and  pl.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pctl_hopolicytype_hoe pa on pa.ID_stg = pl.HOPolicyType_stg

/* join DB_T_PROD_STAG.pc_personalvehicle ppv on ppv.BranchID_stg = pp.ID_stg */
join DB_T_PROD_STAG.pcx_homodifier_hoe dis on dis.BranchID_stg = pp.ID_stg and   dis.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pcx_HOrelationshipdetail_alfa rldet on rldet.ID_stg = pl.HORelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrWatercraft on nbrWatercraft.ID_stg = rldet.WatercraftInd_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''HORelationshipDisc_alfa''

and rldet.WatercraftInd_stg <> 0 

AND pl.UpdateTime_stg > :start_dttm

AND pl.UpdateTime_stg <= :end_dttm 



UNION

/*  DB_T_STAG_MEMBXREF_PROD.Umbrella  */


select distinct  PJ.JobNumber_stg, pp.branchnumber_stg, 

        CAST(pp.PolicyNumber_stg AS  VARCHAR(100))as PolicyNumber,

            CAST(pp.PublicID_stg AS VARCHAR(100)) as PublicID, CAST(NULL AS VARCHAR(100))as SPEC_TYPE_CD,

            pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

            pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

            pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, cast(''AGMT_SPEC14''  as varchar(100)) AS QUOTN_SPEC_TYPE_CD,

            cast(case when rldet.UmbrellaInd_stg = ''1'' then ''YES'' else rldet.UmbrellaInd_stg end as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE)as  QUOTN_SPEC_DT , cast(NULL as VARCHAR(255) )as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp  JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and  pl.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pctl_hopolicytype_hoe pa on pa.ID_stg = pl.HOPolicyType_stg

/* join DB_T_PROD_STAG.pc_personalvehicle ppv on ppv.BranchID_stg = pp.ID_stg */
join DB_T_PROD_STAG.pcx_homodifier_hoe dis on dis.BranchID_stg = pp.ID_stg and  dis.ExpirationDate_stg is NULL 

join DB_T_PROD_STAG.pcx_HOrelationshipdetail_alfa rldet on rldet.ID_stg = pl.HORelationshipDetails_alfa_stg

left join DB_T_PROD_STAG.pctl_number_alfa nbrUmbrella on nbrUmbrella.ID_stg = rldet.UmbrellaInd_stg

where dis.Eligible_stg = 1

and dis.PatternCode_stg = ''HORelationshipDisc_alfa''

and rldet.UmbrellaInd_stg <> 0 

AND pl.UpdateTime_stg > :start_dttm

AND pl.UpdateTime_stg <= :end_dttm 

 





union



SELECT   PJ.JobNumber_stg, a.branchnumber_stg,a.PolicyNumber_stg as PolicyNumber,

            cast(null as varchar(20)) as PublicID,CAST( Null AS VARCHAR(100))as SPEC_TYPE_CD,

            a.UpdateTime_stg as spec_updatetime, pce.EffectiveDate_stg as EffectiveDate,

            pce.ExpirationDate_stg as ExpirationDate, a.UpdateTime_stg as TRANS_STRT_DTTM,

            a.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, CAST( ''LatestDtOfLapses_alfa'' AS VARCHAR(100)) AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            pce.LatestDtOfLapses_alfa_stg  as QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

    FROM    DB_T_PROD_STAG.pc_policyperiod a JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=a.JOBID_STG

    JOIN DB_T_PROD_STAG.pc_effectivedatedfields pce 

        ON pce.BranchID_stg = a.ID_stg and pce.ExpirationDate_stg is null

        JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ps 

        ON ps.ID_stg = a.Status_stg

        WHERE   ps.TYPECODE_stg <> ''Temporary''

        AND a.UpdateTime_stg >:start_dttm

        AND a.UpdateTime_stg <=:end_dttm

        AND pce.LatestDtOfLapses_alfa_stg is not null

        UNION

        

SELECT   PJ.JobNumber_stg, a.branchnumber_stg,a.PolicyNumber_stg as PolicyNumber,

            cast(null as varchar(20)) as PublicID, CAST(LapsesInContService_alfa_STG AS VARCHAR(100)) as SPEC_TYPE_CD,

            a.UpdateTime_stg as spec_updatetime, pce.EffectiveDate_stg as EffectiveDate,

            pce.ExpirationDate_stg as ExpirationDate, a.UpdateTime_stg as TRANS_STRT_DTTM,

            a.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, ''LapsesInContService_alfa'' AS QUOTN_SPEC_TYPE_CD,

            cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            CAST(NULL AS DATE )  as QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

    FROM    DB_T_PROD_STAG.pc_policyperiod a JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=a.JOBID_STG

    JOIN DB_T_PROD_STAG.pc_effectivedatedfields pce 

        ON pce.BranchID_stg = a.ID_stg and pce.ExpirationDate_stg is null

        JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ps 

        ON ps.ID_stg = a.Status_stg

        WHERE   ps.TYPECODE_stg <> ''Temporary''

        AND a.UpdateTime_stg >:start_dttm

        AND a.UpdateTime_stg <=:end_dttm

        AND pce.LapsesInContService_alfa_STG is not null

        

        /*PMOP-9934 START*/

        

UNION 



select jobnumber_stg, branchnumber_stg,policynumber_stg,

cast(null as varchar(20)) as PublicID, /* 02 */
B.typecode_stg AS SPEC_TYPE_CD, /* 03  */
a.UpdateTime_stg as spec_updatetime,

ppl1.EffectiveDate_stg as EffectiveDate, /* 04 */
ppl1.ExpirationDate_stg as ExpirationDate, /* 05 */
a.UpdateTime_stg as TRANS_STRT_DTTM, /* 06 */
a.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, /* 07 */
''AGMT_SPEC12'' AS QUOTN_SPEC_TYPE_CD, /* 08 */
  cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE )as QUOTN_SPEC_DT,

            cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod a

join DB_T_PROD_STAG.pctl_AgentAssignmentType_alfa b on a.AgentAssignmentSource_alfa_stg = b.ID_stg

JOIN DB_T_PROD_STAG.pc_policyline ppl1 ON ppl1.BranchID_stg = a.ID_stg AND ppl1.ExpirationDate_stg IS NULL

JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_stg=a.JOBID_stg

JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ps ON ps.ID_stg = a.Status_stg

WHERE ps.Typecode_stg <> ''Temporary'' AND a.UpdateTime_stg >:start_dttm AND a.UpdateTime_stg <=:end_dttm

/*PMOP-9934 END*/

/*EIM-48135*/

UNION

SELECT PJ.JobNumber_stg, a.branchnumber_stg, a.PolicyNumber_stg as PolicyNumber,

       cast(null as varchar(20))as PublicID, 

       cast(null as varchar(50)) as SPEC_TYPE_CD,

       a.UpdateTime_stg as spec_updatetime, pce.EffectiveDate_stg as EffectiveDate,

       pce.ExpirationDate_stg as ExpirationDate, a.UpdateTime_stg as TRANS_STRT_DTTM,

       a.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM,CAST(''PriorCarrierOther'' AS VARCHAR(255)) AS Quotn_SPEC_TYPE_CD,

       cast(NULL as varchar(100))as QUOTN_SPEC_IND,

       cast(NULL as DATE )as QUOTN_SPEC_DT,pce.othercarrier_alfa_STG  as Quotn_SPEC_TXT

FROM  DB_T_PROD_STAG.pc_policyperiod a JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=a.JOBID_STG

JOIN DB_T_PROD_STAG.pc_effectivedatedfields pce ON pce.BranchID_stg = a.ID_stg and pce.ExpirationDate_stg is null

JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ps ON ps.ID_stg = a.Status_stg

WHERE   ps.TYPECODE_stg <> ''Temporary''

AND a.UpdateTime_stg >:start_dttm

AND a.UpdateTime_stg <=:end_dttm

AND pce.othercarrier_alfa_STG is not null

) SPEC_TYPE_CD_x

QUALIFY  ROW_NUMBER() OVER  (partition by  jobnumber_stg, branchnumber_stg,SPEC_TYPE_CD_x.QUOTN_SPEC_TYPE_CD

order by SPEC_TYPE_CD_x.spec_updatetime desc) =1



/*EIM - 49868 - Including PUP */



union



SELECT jobnumber_stg, branchnumber_stg,PolicyNumber, 

PublicID,SPEC_TYPE_CD,EffectiveDate,ExpirationDate,TRANS_STRT_DTTM,

QUOTN_SPEC_STRT_DTTM,QUOTN_SPEC_TYPE_CD,QUOTN_SPEC_IND,QUOTN_SPEC_DT,Quotn_SPEC_TXT

FROM 

(

select distinct PJ.JobNumber_stg, a.branchnumber_stg,

a.PolicyNumber_stg as PolicyNumber , 

a.PublicID_stg as PublicID,

cast(Null as varchar(100))AS SPEC_TYPE_CD, 

pup.pupeffectivedate_stg as EffectiveDate, 

b.ExpirationDate_stg as ExpirationDate,

a.UpdateTime_stg as TRANS_STRT_DTTM,

A.EDITEFFECTIVEDATE_STG as QUOTN_SPEC_STRT_DTTM, 

cast(''IsSubmitted'' as varchar(100))AS QUOTN_SPEC_TYPE_CD,

pup.UpdateTime_stg as spec_updatetime,

cast(case when pup.IsSubmitted_stg=1 then NULL else ''Yes'' end as varchar(100)) as QUOTN_SPEC_IND,

cast(NULL as DATE)as  QUOTN_SPEC_DT,

cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

FROM DB_T_PROD_STAG.pc_policyperiod a

JOIN DB_T_PROD_STAG.pc_policyline b ON b.BranchID_stg = a.ID_stg  AND b.ExpirationDate_stg IS NULL

JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=a.JOBID_STG 

JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ps  ON ps.ID_stg = a.Status_stg

JOIN DB_T_PROD_STAG.pc_policy pc on a.PolicyID_stg = pc.id_stg

JOIN DB_T_PROD_STAG.pcx_puprenreviewdetails_alfa pup on pup.AssociatedPUPPolicy_stg =pc.ID_stg

INNER JOIN DB_T_PROD_STAG.pctl_job pcj ON pcj.id_stg = pj.Subtype_stg

WHERE  ps.TYPECODE_stg <> ''Temporary'' AND pcj.typecode_stg = ''Renewal''

AND pup.UpdateTime_stg >:start_dttm AND pup.UpdateTime_stg <=:end_dttm

) quotn_spec_x 

QUALIFY ROW_NUMBER() OVER (partition by JobNumber_stg,branchnumber_stg ,quotn_spec_x.QUOTN_SPEC_TYPE_CD order by quotn_spec_x.spec_updatetime desc) =1





UNION 

/* EIM-50997 */
SELECT jobnumber_stg, branchnumber_stg,PolicyNumber, 

PublicID,SPEC_TYPE_CD,EffectiveDate,ExpirationDate,TRANS_STRT_DTTM,

QUOTN_SPEC_STRT_DTTM,QUOTN_SPEC_TYPE_CD,QUOTN_SPEC_IND,QUOTN_SPEC_DT,Quotn_SPEC_TXT

FROM (

select distinct  

         PJ.JobNumber_stg, pp.branchnumber_stg,pp.PolicyNumber_stg as PolicyNumber,

         cast(null as varchar(20)) as PublicID, cast(NULL as varchar(100)) as SPEC_TYPE_CD,

         pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

         pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

         pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, ''AGMT_SPEC26'' AS QUOTN_SPEC_TYPE_CD,

         cast(case when pl.IsSFTierRating_alfa_stg = ''1'' then ''YES''else pl.IsSFTierRating_alfa_stg end as varchar(100))as QUOTN_SPEC_IND,

         cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp

JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG 

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and pl.ExpirationDate_stg is NULL and pp.status_stg <> ''2'' 

where pl.IsSFTierRating_alfa_stg = 1 

AND pl.UpdateTime_stg >:start_dttm

AND pl.UpdateTime_stg <=:end_dttm

) quotn_spec_x 

QUALIFY ROW_NUMBER() OVER (partition by JobNumber_stg,branchnumber_stg ,quotn_spec_x.QUOTN_SPEC_TYPE_CD order by quotn_spec_x.spec_updatetime desc) =1



UNION 

/* EIM-51006 */
SELECT jobnumber_stg, branchnumber_stg,PolicyNumber, 

PublicID,SPEC_TYPE_CD,EffectiveDate,ExpirationDate,TRANS_STRT_DTTM,

QUOTN_SPEC_STRT_DTTM,QUOTN_SPEC_TYPE_CD,QUOTN_SPEC_IND,QUOTN_SPEC_DT,Quotn_SPEC_TXT

FROM (select distinct  

         PJ.JobNumber_stg, pp.branchnumber_stg,pp.PolicyNumber_stg as PolicyNumber,

         cast(null as varchar(20)) as PublicID, cast(NULL as varchar(100)) as SPEC_TYPE_CD,

         pl.UpdateTime_stg as spec_updatetime, pl.EffectiveDate_stg as EffectiveDate,

         pl.ExpirationDate_stg as ExpirationDate, pp.UpdateTime_stg as TRANS_STRT_DTTM,

         pp.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, ''AGMT_SPEC26'' AS QUOTN_SPEC_TYPE_CD,

         cast(case when pl.IsMHTierRating_alfa_stg = ''1'' then ''YES''else pl.IsMHTierRating_alfa_stg end as varchar(100))as QUOTN_SPEC_IND,

         cast(NULL as DATE)as  QUOTN_SPEC_DT,cast(NULL AS VARCHAR(255)) as Quotn_SPEC_TXT

from DB_T_PROD_STAG.pc_policyperiod pp

JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp.JOBID_STG 

join DB_T_PROD_STAG.pc_policyline pl on pl.BranchID_stg = pp.ID_stg and pl.ExpirationDate_stg is NULL and pp.status_stg <> ''2'' 

where pl.IsMHTierRating_alfa_stg = 1

AND pl.UpdateTime_stg >:start_dttm

AND pl.UpdateTime_stg <=:end_dttm

) quotn_spec_x 

QUALIFY ROW_NUMBER() OVER (partition by JobNumber_stg,branchnumber_stg ,quotn_spec_x.QUOTN_SPEC_TYPE_CD order by quotn_spec_x.spec_updatetime desc) =1
) SRC
)
);


-- Component exp_pass_from_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source AS
(
SELECT
sq_pc_policyperiod.QUOTN_SPEC_STRT_DTTM as QUOTN_SPEC_STRT_DTTM,
sq_pc_policyperiod.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SPEC_TYPE_CD */ as out_SPEC_TYPE_CD,
sq_pc_policyperiod.EffectiveDate as EffectiveDate,
sq_pc_policyperiod.ExpirationDate as ExpirationDate,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_SPEC_TYPE_CD */ as out_QUOTN_SPEC_TYPE_CD,
sq_pc_policyperiod.QUOTN_SPEC_IND as QUOTN_SPEC_IND,
sq_pc_policyperiod.QUOTN_SPEC_DT as QUOTN_SPEC_DT,
sq_pc_policyperiod.Jobnumber as Jobnumber,
sq_pc_policyperiod.Branchnumber as Branchnumber,
LTRIM ( RTRIM ( sq_pc_policyperiod.QUOTN_SPEC_TXT ) ) as out_QUOTN_SPEC_TXT,
sq_pc_policyperiod.source_record_id,
row_number() over (partition by sq_pc_policyperiod.source_record_id order by sq_pc_policyperiod.source_record_id) as RNK
FROM
sq_pc_policyperiod
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SPEC_TYPE_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_pc_policyperiod.SPEC_TYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_AGMT_SPEC_TYPE_CD LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = sq_pc_policyperiod.QUOTN_SPEC_TYPE_CD
QUALIFY RNK = 1
);


-- Component LKP_INSRNC_QUOTN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRNC_QUOTN AS
(
SELECT
LKP.QUOTN_ID,
exp_pass_from_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source.source_record_id ORDER BY LKP.QUOTN_ID asc) RNK
FROM
exp_pass_from_source
LEFT JOIN (
SELECT INSRNC_QUOTN.QUOTN_ID AS QUOTN_ID, INSRNC_QUOTN.NK_JOB_NBR AS NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR AS VERS_NBR FROM DB_T_PROD_CORE.INSRNC_QUOTN
QUALIFY ROW_NUMBER() OVER(PARTITION BY  INSRNC_QUOTN.NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR,  INSRNC_QUOTN.SRC_SYS_CD  ORDER BY INSRNC_QUOTN.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_JOB_NBR = exp_pass_from_source.Jobnumber AND LKP.VERS_NBR = exp_pass_from_source.Branchnumber
QUALIFY RNK = 1
);


-- Component LKP_QUOTN_SPEC, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_QUOTN_SPEC AS
(
SELECT
LKP.QUOTN_ID,
LKP.QUOTN_SPEC_TYPE_CD,
LKP.QUOTN_SPEC_STRT_DTTM,
LKP.SPEC_TYPE_CD,
LKP.QUOTN_SPEC_END_DTTM,
LKP.QUOTN_SPEC_DT,
LKP.QUOTN_SPEC_IND,
LKP.QUOTN_SPEC_TXT,
LKP.QUOTN_SPEC_AMT,
LKP.QUOTN_SPEC_CNT,
LKP.QUOTN_SPEC_QTY,
LKP.QUOTN_SPEC_RATE,
exp_pass_from_source.out_QUOTN_SPEC_TYPE_CD as out_QUOTN_SPEC_TYPE_CD,
exp_pass_from_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source.source_record_id ORDER BY LKP.QUOTN_ID asc,LKP.QUOTN_SPEC_TYPE_CD asc,LKP.QUOTN_SPEC_STRT_DTTM asc,LKP.SPEC_TYPE_CD asc,LKP.QUOTN_SPEC_END_DTTM asc,LKP.QUOTN_SPEC_DT asc,LKP.QUOTN_SPEC_IND asc,LKP.QUOTN_SPEC_TXT asc,LKP.QUOTN_SPEC_AMT asc,LKP.QUOTN_SPEC_CNT asc,LKP.QUOTN_SPEC_QTY asc,LKP.QUOTN_SPEC_RATE asc) RNK1
FROM
exp_pass_from_source
INNER JOIN LKP_INSRNC_QUOTN ON exp_pass_from_source.source_record_id = LKP_INSRNC_QUOTN.source_record_id
LEFT JOIN (
SELECT QUOTN_SPEC.QUOTN_ID as QUOTN_ID, 
QUOTN_SPEC.QUOTN_SPEC_TYPE_CD as QUOTN_SPEC_TYPE_CD,
QUOTN_SPEC.QUOTN_SPEC_STRT_DTTM as QUOTN_SPEC_STRT_DTTM, 
QUOTN_SPEC.SPEC_TYPE_CD as SPEC_TYPE_CD, 
QUOTN_SPEC.QUOTN_SPEC_END_DTTM as QUOTN_SPEC_END_DTTM,
QUOTN_SPEC.QUOTN_SPEC_DT as QUOTN_SPEC_DT, 
QUOTN_SPEC.QUOTN_SPEC_IND as QUOTN_SPEC_IND, 
TRIM(QUOTN_SPEC.QUOTN_SPEC_TXT) as QUOTN_SPEC_TXT, 
QUOTN_SPEC.QUOTN_SPEC_AMT AS QUOTN_SPEC_AMT,
QUOTN_SPEC.QUOTN_SPEC_CNT AS QUOTN_SPEC_CNT,
QUOTN_SPEC.QUOTN_SPEC_QTY AS QUOTN_SPEC_QTY,
QUOTN_SPEC.QUOTN_SPEC_RATE AS QUOTN_SPEC_RATE
FROM DB_T_PROD_CORE.QUOTN_SPEC
WHERE EDW_END_DTTM =''9999-12-31 23:59:59.999999''  
AND QUOTN_SPEC_TYPE_CD<>''AFFNTYGRP''
) LKP ON LKP.QUOTN_ID = LKP_INSRNC_QUOTN.QUOTN_ID AND LKP.QUOTN_SPEC_TYPE_CD = exp_pass_from_source.out_QUOTN_SPEC_TYPE_CD
QUALIFY RNK1 = 1
);


-- Component exp_pass_to_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_upd AS
(
SELECT
LKP_QUOTN_SPEC.QUOTN_ID as lkp_QUOTN_ID,
LKP_QUOTN_SPEC.QUOTN_SPEC_TYPE_CD as lkp_QUOTN_SPEC_TYPE_CD,
LKP_QUOTN_SPEC.QUOTN_SPEC_STRT_DTTM as lkp_QUOTN_SPEC_STRT_DTTM,
LKP_QUOTN_SPEC.SPEC_TYPE_CD as lkp_SPEC_TYPE_CD,
LKP_INSRNC_QUOTN.QUOTN_ID as in_QUOTN_ID,
exp_pass_from_source.out_QUOTN_SPEC_TYPE_CD as in_QUOTN_SPEC_TYPE_CD,
exp_pass_from_source.out_SPEC_TYPE_CD as in_SPEC_TYPE_CD,
exp_pass_from_source.QUOTN_SPEC_STRT_DTTM as in_QUOTN_SPEC_STRT_DTTM,
exp_pass_from_source.TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
LTRIM ( RTRIM ( exp_pass_from_source.QUOTN_SPEC_IND ) ) as v_QUOTN_SPEC_IND,
v_QUOTN_SPEC_IND as out_QUOTN_SPEC_IND,
exp_pass_from_source.QUOTN_SPEC_DT as in_QUOTN_SPEC_DT,
exp_pass_from_source.out_QUOTN_SPEC_TXT as in_QUOTN_SPEC_TXT,
NULL as in_QUOTN_SPEC_AMT,
NULL as in_QUOTN_SPEC_CNT,
NULL as in_QUOTN_SPEC_QTY,
NULL as in_QUOTN_SPEC_RATE,
md5 ( CASE WHEN LKP_QUOTN_SPEC.SPEC_TYPE_CD IS NULL THEN ''~'' ELSE LKP_QUOTN_SPEC.SPEC_TYPE_CD END || CASE WHEN LKP_QUOTN_SPEC.QUOTN_SPEC_TXT IS NULL THEN ''~'' ELSE LKP_QUOTN_SPEC.QUOTN_SPEC_TXT END || CASE WHEN LKP_QUOTN_SPEC.QUOTN_SPEC_DT IS NULL THEN ''~'' ELSE to_char ( LKP_QUOTN_SPEC.QUOTN_SPEC_DT ) END || CASE WHEN LKP_QUOTN_SPEC.QUOTN_SPEC_IND IS NULL THEN ''~'' ELSE LKP_QUOTN_SPEC.QUOTN_SPEC_IND END || CASE WHEN LKP_QUOTN_SPEC.QUOTN_SPEC_AMT IS NULL THEN ''~'' ELSE to_char ( LKP_QUOTN_SPEC.QUOTN_SPEC_AMT ) END || CASE WHEN LKP_QUOTN_SPEC.QUOTN_SPEC_CNT IS NULL THEN ''~'' ELSE to_char ( LKP_QUOTN_SPEC.QUOTN_SPEC_CNT ) END || CASE WHEN LKP_QUOTN_SPEC.QUOTN_SPEC_QTY IS NULL THEN ''~'' ELSE to_char ( LKP_QUOTN_SPEC.QUOTN_SPEC_QTY ) END || CASE WHEN LKP_QUOTN_SPEC.QUOTN_SPEC_RATE IS NULL THEN ''~'' ELSE to_char ( LKP_QUOTN_SPEC.QUOTN_SPEC_RATE ) END ) as Checksum_lkp,
md5 ( CASE WHEN exp_pass_from_source.out_SPEC_TYPE_CD IS NULL THEN ''~'' ELSE exp_pass_from_source.out_SPEC_TYPE_CD END || CASE WHEN exp_pass_from_source.out_QUOTN_SPEC_TXT IS NULL THEN ''~'' ELSE exp_pass_from_source.out_QUOTN_SPEC_TXT END || CASE WHEN exp_pass_from_source.QUOTN_SPEC_DT IS NULL THEN ''~'' ELSE to_char ( exp_pass_from_source.QUOTN_SPEC_DT ) END || CASE WHEN v_QUOTN_SPEC_IND IS NULL THEN ''~'' ELSE v_QUOTN_SPEC_IND END || CASE WHEN in_QUOTN_SPEC_AMT IS NULL THEN ''~'' ELSE to_char ( in_QUOTN_SPEC_AMT ) END || CASE WHEN in_QUOTN_SPEC_CNT IS NULL THEN ''~'' ELSE to_char ( in_QUOTN_SPEC_CNT ) END || CASE WHEN in_QUOTN_SPEC_QTY IS NULL THEN ''~'' ELSE to_char ( in_QUOTN_SPEC_QTY ) END || CASE WHEN in_QUOTN_SPEC_RATE IS NULL THEN ''~'' ELSE to_char ( in_QUOTN_SPEC_RATE ) END ) as Checksum_in,
CASE WHEN LKP_QUOTN_SPEC.QUOTN_ID IS NULL THEN ''I'' ELSE CASE WHEN Checksum_lkp != Checksum_in THEN ''U'' ELSE ''R'' END END as CDC_Flag,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_QUOTN_SPEC_END_DTTM,
:PRCS_ID as out_PRCS_ID,
CURRENT_TIMESTAMP as out_EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_EDW_END_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_TRANS_END_DTTM,
exp_pass_from_source.source_record_id
FROM
exp_pass_from_source
INNER JOIN LKP_INSRNC_QUOTN ON exp_pass_from_source.source_record_id = LKP_INSRNC_QUOTN.source_record_id
INNER JOIN LKP_QUOTN_SPEC ON LKP_INSRNC_QUOTN.source_record_id = LKP_QUOTN_SPEC.source_record_id
);


-- Component rtr_agmt_spec_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_agmt_spec_INSERT AS
(SELECT
exp_pass_to_upd.lkp_QUOTN_ID as lkp_QUOTN_ID,
exp_pass_to_upd.lkp_QUOTN_SPEC_TYPE_CD as lkp_QUOTN_SPEC_TYPE_CD,
exp_pass_to_upd.lkp_QUOTN_SPEC_STRT_DTTM as lkp_QUOTN_SPEC_STRT_DTTM,
NULL as lkp_QUOTN_SPEC_END_DTTM,
exp_pass_to_upd.lkp_SPEC_TYPE_CD as lkp_SPEC_TYPE_CD,
exp_pass_to_upd.out_TRANS_END_DTTM as TRANS_END_DTTM,
exp_pass_to_upd.CDC_Flag as CDC_Flag,
exp_pass_to_upd.in_QUOTN_ID as in_QUOTN_ID,
exp_pass_to_upd.in_QUOTN_SPEC_TYPE_CD as in_QUOTN_SPEC_TYPE_CD,
exp_pass_to_upd.in_SPEC_TYPE_CD as in_SPEC_TYPE_CD,
exp_pass_to_upd.out_QUOTN_SPEC_IND as out_QUOTN_SPEC_IND,
exp_pass_to_upd.in_QUOTN_SPEC_STRT_DTTM as in_QUOTN_SPEC_STRT_DTTM,
exp_pass_to_upd.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_pass_to_upd.out_PRCS_ID as in_PRCS_ID,
exp_pass_to_upd.out_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_pass_to_upd.out_EDW_END_DTTM as in_EDW_END_DTTM,
exp_pass_to_upd.out_QUOTN_SPEC_END_DTTM as in_QUOTN_SPEC_END_DTTM,
exp_pass_to_upd.in_QUOTN_SPEC_DT as in_QUOTN_SPEC_DT,
exp_pass_to_upd.in_QUOTN_SPEC_TXT as In_QUOTN_SPEC_TXT,
exp_pass_to_upd.source_record_id
FROM
exp_pass_to_upd
WHERE ( exp_pass_to_upd.CDC_Flag = ''I'' or exp_pass_to_upd.CDC_Flag = ''U'' ) AND exp_pass_to_upd.in_QUOTN_ID IS NOT NULL);


-- Component upd_ins_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_ins_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_agmt_spec_INSERT.in_QUOTN_ID as QUOTN_ID,
rtr_agmt_spec_INSERT.in_QUOTN_SPEC_TYPE_CD as QUOTN_SPEC_TYPE_CD,
rtr_agmt_spec_INSERT.in_QUOTN_SPEC_STRT_DTTM as QUOTN_SPEC_STRT_DTTM,
rtr_agmt_spec_INSERT.in_QUOTN_SPEC_END_DTTM as QUOTN_SPEC_END_DTTM,
rtr_agmt_spec_INSERT.in_SPEC_TYPE_CD as SPEC_TYPE_CD,
rtr_agmt_spec_INSERT.out_QUOTN_SPEC_IND as QUOTN_SPEC_IND,
rtr_agmt_spec_INSERT.in_PRCS_ID as PRCS_ID,
rtr_agmt_spec_INSERT.in_EDW_STRT_DTTM as EDW_STRT_DTTM,
rtr_agmt_spec_INSERT.in_EDW_END_DTTM as EDW_END_DTTM,
rtr_agmt_spec_INSERT.in_TRANS_STRT_DTTM as TRANS_STRT_DTTM,
rtr_agmt_spec_INSERT.TRANS_END_DTTM as TRANS_END_DTTM,
rtr_agmt_spec_INSERT.in_QUOTN_SPEC_DT as QUOTN_SPEC_DT,
rtr_agmt_spec_INSERT.In_QUOTN_SPEC_TXT as In_QUOTN_SPEC_TXT1,
0 as UPDATE_STRATEGY_ACTION,
rtr_agmt_spec_INSERT.source_record_id
FROM
rtr_agmt_spec_INSERT
);


-- Component exp_pass_to_target_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_ins AS
(
SELECT
upd_ins_upd.QUOTN_ID as QUOTN_ID,
upd_ins_upd.QUOTN_SPEC_TYPE_CD as QUOTN_SPEC_TYPE_CD,
upd_ins_upd.QUOTN_SPEC_STRT_DTTM as QUOTN_SPEC_STRT_DTTM,
upd_ins_upd.QUOTN_SPEC_END_DTTM as QUOTN_SPEC_END_DTTM,
upd_ins_upd.SPEC_TYPE_CD as SPEC_TYPE_CD,
NULL as QUOTN_SPEC_CNT,
upd_ins_upd.In_QUOTN_SPEC_TXT1 as QUOTN_SPEC_TXT,
NULL as QUOTN_SPEC_QTY,
NULL as QUOTN_SPEC_RATE,
NULL as QUOTN_SPEC_AMT,
upd_ins_upd.QUOTN_SPEC_DT as QUOTN_SPEC_DT,
upd_ins_upd.QUOTN_SPEC_IND as QUOTN_SPEC_IND,
upd_ins_upd.PRCS_ID as PRCS_ID,
upd_ins_upd.EDW_STRT_DTTM as EDW_STRT_DTTM,
upd_ins_upd.EDW_END_DTTM as EDW_END_DTTM,
upd_ins_upd.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
upd_ins_upd.TRANS_END_DTTM as TRANS_END_DTTM,
upd_ins_upd.source_record_id
FROM
upd_ins_upd
);


-- Component QUOTN_SPEC, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_SPEC
(
QUOTN_ID,
QUOTN_SPEC_TYPE_CD,
QUOTN_SPEC_STRT_DTTM,
SPEC_TYPE_CD,
QUOTN_SPEC_END_DTTM,
QUOTN_SPEC_CNT,
QUOTN_SPEC_TXT,
QUOTN_SPEC_QTY,
QUOTN_SPEC_RATE,
QUOTN_SPEC_AMT,
QUOTN_SPEC_DT,
QUOTN_SPEC_IND,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_target_ins.QUOTN_ID as QUOTN_ID,
exp_pass_to_target_ins.QUOTN_SPEC_TYPE_CD as QUOTN_SPEC_TYPE_CD,
exp_pass_to_target_ins.QUOTN_SPEC_STRT_DTTM as QUOTN_SPEC_STRT_DTTM,
exp_pass_to_target_ins.SPEC_TYPE_CD as SPEC_TYPE_CD,
exp_pass_to_target_ins.QUOTN_SPEC_END_DTTM as QUOTN_SPEC_END_DTTM,
exp_pass_to_target_ins.QUOTN_SPEC_CNT as QUOTN_SPEC_CNT,
exp_pass_to_target_ins.QUOTN_SPEC_TXT as QUOTN_SPEC_TXT,
exp_pass_to_target_ins.QUOTN_SPEC_QTY as QUOTN_SPEC_QTY,
exp_pass_to_target_ins.QUOTN_SPEC_RATE as QUOTN_SPEC_RATE,
exp_pass_to_target_ins.QUOTN_SPEC_AMT as QUOTN_SPEC_AMT,
exp_pass_to_target_ins.QUOTN_SPEC_DT as QUOTN_SPEC_DT,
exp_pass_to_target_ins.QUOTN_SPEC_IND as QUOTN_SPEC_IND,
exp_pass_to_target_ins.PRCS_ID as PRCS_ID,
exp_pass_to_target_ins.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_target_ins.EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_target_ins.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_pass_to_target_ins.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_to_target_ins;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component sq_pc_policyperiod1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_pc_policyperiod1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Jobnumber,
$2 as Branchnumber,
$3 as PolicyNumber,
$4 as PublicID,
$5 as SPEC_TYPE_CD,
$6 as EffectiveDate,
$7 as ExpirationDate,
$8 as TRANS_STRT_DTTM,
$9 as QUOTN_SPEC_STRT_DTTM,
$10 as QUOTN_SPEC_TYPE_CD,
$11 as QUOTN_SPEC_IND,
$12 as QUOTN_SPEC_DT,
$13 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT  jobnumber_stg,branchnumber_stg,PolicyNumber, 

        cast(null as varchar(20))PublicID,

        CAST(SPEC_TYPE_CD AS VARCHAR(100))SPEC_TYPE_CD,

        EffectiveDate,

        ExpirationDate,

        TRANS_STRT_DTTM,

        QUOTN_SPEC_STRT_DTTM,

        CAST(QUOTN_SPEC_TYPE_CD AS VARCHAR(30))QUOTN_SPEC_TYPE_CD,

        cast(NULL as varchar(100))as QUOTN_SPEC_IND,

            cast(NULL as DATE )as QUOTN_SPEC_DT

FROM 

(

SELECT PJ.JobNumber_stg, PP1.branchnumber_stg,

pp1.PolicyNumber_stg as PolicyNumber, /* 01  */
cast(null as varchar(20)) as PublicID, /* 02 */
cast(agType.Typecode_stg as varchar(100))AS SPEC_TYPE_CD, /* 03  */
ppl1.EffectiveDate_stg as EffectiveDate, /* 04 */
ppl1.ExpirationDate_stg as ExpirationDate, /* 05 */
pp1.UpdateTime_stg as TRANS_STRT_DTTM, /* 06 */
pp1.EditEffectiveDate_stg as QUOTN_SPEC_STRT_DTTM, /* 07 */
cast(pag.Typecode_stg as varchar(100))AS QUOTN_SPEC_TYPE_CD, /* 08 */
ag.UpdateTime_stg as spec_updatetime



FROM DB_T_PROD_STAG.pc_policyperiod pp1

JOIN DB_T_PROD_STAG.pc_policyline ppl1 ON ppl1.BranchID_stg = pp1.ID_stg AND ppl1.ExpirationDate_stg IS NULL

JOIN DB_T_PROD_STAG.pc_job PJ ON PJ.ID_STG=pp1.JOBID_STG

JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ps ON ps.ID_stg = pp1.Status_stg

JOIN DB_T_PROD_STAG.pcx_affinitygroup_alfa ag ON ag.PersonalAutoLine_stg = ppl1.ID_stg /* AND PP1.ID_STG=AG.BRANCHID_STG */
JOIN DB_T_PROD_STAG.pctl_affinitygrouptype_alfa agType ON agType.ID_stg = ag.AffinityType_stg

JOIN DB_T_PROD_STAG.pctl_affinitygroup_alfa pag ON pag.ID_stg = ag.subtype_stg

WHERE ps.Typecode_stg <> ''Temporary'' AND ag.UpdateTime_stg >:start_dttm AND ag.UpdateTime_stg <=:end_dttm



) quotn_spec_x 

join DB_T_PROD_STAG.pctl_affinitygroup_alfa on pctl_affinitygroup_alfa.Typecode_stg = quotn_spec_x.QUOTN_SPEC_TYPE_CD 

/*and 1=2 As Affinity Group has issues, we are adding filter to not to load any affinity data to QUOTN_SPEC*/

QUALIFY ROW_NUMBER() OVER (partition by JobNumber_stg,branchnumber_stg ,SPEC_TYPE_CD order by quotn_spec_x.spec_updatetime desc) =1
) SRC
)
);


-- Component exp_pass_from_source1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source1 AS
(
SELECT
sq_pc_policyperiod1.QUOTN_SPEC_STRT_DTTM as QUOTN_SPEC_STRT_DTTM,
sq_pc_policyperiod1.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SPEC_TYPE_CD */ as out_SPEC_TYPE_CD,
sq_pc_policyperiod1.EffectiveDate as EffectiveDate,
sq_pc_policyperiod1.ExpirationDate as ExpirationDate,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_SPEC_TYPE_CD */ as out_QUOTN_SPEC_TYPE_CD,
sq_pc_policyperiod1.QUOTN_SPEC_IND as QUOTN_SPEC_IND,
sq_pc_policyperiod1.QUOTN_SPEC_DT as QUOTN_SPEC_DT,
sq_pc_policyperiod1.Jobnumber as Jobnumber,
sq_pc_policyperiod1.Branchnumber as Branchnumber,
sq_pc_policyperiod1.source_record_id,
row_number() over (partition by sq_pc_policyperiod1.source_record_id order by sq_pc_policyperiod1.source_record_id) as RNK
FROM
sq_pc_policyperiod1
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SPEC_TYPE_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_pc_policyperiod1.SPEC_TYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_AGMT_SPEC_TYPE_CD LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = sq_pc_policyperiod1.QUOTN_SPEC_TYPE_CD
QUALIFY RNK = 1
);


-- Component LKP_INSRNC_QUOTN1, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRNC_QUOTN1 AS
(
SELECT
LKP.QUOTN_ID,
exp_pass_from_source1.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source1.source_record_id ORDER BY LKP.QUOTN_ID asc) RNK
FROM
exp_pass_from_source1
LEFT JOIN (
SELECT INSRNC_QUOTN.QUOTN_ID AS QUOTN_ID, INSRNC_QUOTN.NK_JOB_NBR AS NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR AS VERS_NBR FROM DB_T_PROD_CORE.INSRNC_QUOTN 
QUALIFY ROW_NUMBER() OVER(PARTITION BY  INSRNC_QUOTN.NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR,  INSRNC_QUOTN.SRC_SYS_CD  ORDER BY INSRNC_QUOTN.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_JOB_NBR = exp_pass_from_source1.Jobnumber AND LKP.VERS_NBR = exp_pass_from_source1.Branchnumber
QUALIFY RNK = 1
);


-- Component LKP_QUOTN_SPEC1, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_QUOTN_SPEC1 AS
(
SELECT
LKP.QUOTN_ID,
LKP.QUOTN_SPEC_TYPE_CD,
LKP.QUOTN_SPEC_STRT_DTTM,
LKP.SPEC_TYPE_CD,
LKP.QUOTN_SPEC_DT,
LKP.QUOTN_SPEC_IND,
exp_pass_from_source1.out_QUOTN_SPEC_TYPE_CD as out_QUOTN_SPEC_TYPE_CD,
exp_pass_from_source1.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source1.source_record_id ORDER BY LKP.QUOTN_ID asc,LKP.QUOTN_SPEC_TYPE_CD asc,LKP.QUOTN_SPEC_STRT_DTTM asc,LKP.SPEC_TYPE_CD asc,LKP.QUOTN_SPEC_DT asc,LKP.QUOTN_SPEC_IND asc) RNK1
FROM
exp_pass_from_source1
INNER JOIN LKP_INSRNC_QUOTN1 ON exp_pass_from_source1.source_record_id = LKP_INSRNC_QUOTN1.source_record_id
LEFT JOIN (
SELECT	
QUOTN_SPEC.QUOTN_ID as QUOTN_ID, /* 1 */
QUOTN_SPEC.QUOTN_SPEC_TYPE_CD as QUOTN_SPEC_TYPE_CD, /* 2 */
QUOTN_SPEC.QUOTN_SPEC_STRT_DTTM as QUOTN_SPEC_STRT_DTTM, /* 3 */
QUOTN_SPEC.SPEC_TYPE_CD as SPEC_TYPE_CD, /* 4 */
QUOTN_SPEC.QUOTN_SPEC_DT as QUOTN_SPEC_DT, /* 5 */
QUOTN_SPEC.QUOTN_SPEC_IND as QUOTN_SPEC_IND /* 6 */
FROM	DB_T_PROD_CORE.QUOTN_SPEC 
where	EDW_END_DTTM =''9999-12-31 23:59:59.999999''
and QUOTN_SPEC_TYPE_CD=''AFFNTYGRP''
) LKP ON LKP.QUOTN_ID = LKP_INSRNC_QUOTN1.QUOTN_ID AND LKP.QUOTN_SPEC_TYPE_CD = exp_pass_from_source1.out_QUOTN_SPEC_TYPE_CD AND LKP.SPEC_TYPE_CD = exp_pass_from_source1.out_SPEC_TYPE_CD
QUALIFY RNK1 = 1
);


-- Component exp_pass_to_upd1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_upd1 AS
(
SELECT
LKP_QUOTN_SPEC1.QUOTN_ID as lkp_QUOTN_ID,
LKP_QUOTN_SPEC1.QUOTN_SPEC_TYPE_CD as lkp_QUOTN_SPEC_TYPE_CD,
LKP_QUOTN_SPEC1.QUOTN_SPEC_STRT_DTTM as lkp_QUOTN_SPEC_STRT_DTTM,
LKP_QUOTN_SPEC1.SPEC_TYPE_CD as lkp_SPEC_TYPE_CD,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_QUOTN_SPEC_END_DTTM,
:PRCS_ID as out_PRCS_ID,
CURRENT_TIMESTAMP as out_EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_EDW_END_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_TRANS_END_DTTM,
LKP_INSRNC_QUOTN1.QUOTN_ID as in_QUOTN_ID,
exp_pass_from_source1.out_QUOTN_SPEC_TYPE_CD as in_QUOTN_SPEC_TYPE_CD,
exp_pass_from_source1.out_SPEC_TYPE_CD as in_SPEC_TYPE_CD,
exp_pass_from_source1.QUOTN_SPEC_STRT_DTTM as in_QUOTN_SPEC_STRT_DTTM,
exp_pass_from_source1.TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
LTRIM ( RTRIM ( exp_pass_from_source1.QUOTN_SPEC_IND ) ) as v_QUOTN_SPEC_IND,
CASE WHEN v_QUOTN_SPEC_IND = ''1'' THEN ''YES'' ELSE exp_pass_from_source1.QUOTN_SPEC_IND END as out_QUOTN_SPEC_IND,
exp_pass_from_source1.QUOTN_SPEC_DT as QUOTN_SPEC_DT,
exp_pass_from_source1.EffectiveDate as EffectiveDate,
exp_pass_from_source1.ExpirationDate as ExpirationDate,
LKP_QUOTN_SPEC1.QUOTN_SPEC_IND as QUOTN_SPEC_IND,
MD5 ( LKP_QUOTN_SPEC1.QUOTN_ID || CASE WHEN LKP_QUOTN_SPEC1.QUOTN_SPEC_DT IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) ELSE LKP_QUOTN_SPEC1.QUOTN_SPEC_DT END || CASE WHEN LKP_QUOTN_SPEC1.QUOTN_SPEC_IND IS NULL THEN ''~'' ELSE LKP_QUOTN_SPEC1.QUOTN_SPEC_IND END ) as Checksum_lkp,
MD5 ( LKP_INSRNC_QUOTN1.QUOTN_ID || CASE WHEN exp_pass_from_source1.QUOTN_SPEC_DT IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) ELSE exp_pass_from_source1.QUOTN_SPEC_DT END || ( CASE WHEN exp_pass_from_source1.QUOTN_SPEC_IND = ''1'' THEN ''YES'' ELSE CASE WHEN exp_pass_from_source1.QUOTN_SPEC_IND IS NULL THEN ''~'' ELSE exp_pass_from_source1.QUOTN_SPEC_IND END END ) ) as Checksum_in,
CASE WHEN Checksum_lkp IS NULL THEN ''I'' ELSE CASE WHEN Checksum_lkp != Checksum_in THEN ''U'' ELSE ''R'' END END as CDC_Flag,
exp_pass_from_source1.source_record_id
FROM
exp_pass_from_source1
INNER JOIN LKP_INSRNC_QUOTN1 ON exp_pass_from_source1.source_record_id = LKP_INSRNC_QUOTN1.source_record_id
INNER JOIN LKP_QUOTN_SPEC1 ON LKP_INSRNC_QUOTN1.source_record_id = LKP_QUOTN_SPEC1.source_record_id
);


-- Component rtr_agmt_spec1_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_agmt_spec1_INSERT AS
(SELECT
exp_pass_to_upd1.lkp_QUOTN_ID as lkp_QUOTN_ID,
exp_pass_to_upd1.lkp_QUOTN_SPEC_TYPE_CD as lkp_QUOTN_SPEC_TYPE_CD,
exp_pass_to_upd1.lkp_QUOTN_SPEC_STRT_DTTM as lkp_QUOTN_SPEC_STRT_DTTM,
NULL as lkp_QUOTN_SPEC_END_DTTM,
exp_pass_to_upd1.lkp_SPEC_TYPE_CD as lkp_SPEC_TYPE_CD,
NULL as lkp_QUOTN_SPEC_CNT,
NULL as lkp_QUOTN_SPEC_TXT,
NULL as lkp_QUOTN_SPEC_QTY,
NULL as lkp_QUOTN_SPEC_RATE,
NULL as lkp_QUOTN_SPEC_AMT,
NULL as lkp_QUOTN_SPEC_DT,
NULL as lkp_PRCS_ID,
NULL as lkp_EDW_STRT_DTTM,
NULL as lkp_EDW_END_DTTM,
NULL as lkp_TRANS_STRT_DTTM,
exp_pass_to_upd1.out_TRANS_END_DTTM as TRANS_END_DTTM,
exp_pass_to_upd1.CDC_Flag as CDC_Flag,
exp_pass_to_upd1.in_QUOTN_ID as in_QUOTN_ID,
exp_pass_to_upd1.in_QUOTN_SPEC_TYPE_CD as in_QUOTN_SPEC_TYPE,
exp_pass_to_upd1.in_SPEC_TYPE_CD as in_SPEC_TYPE_CD,
exp_pass_to_upd1.out_QUOTN_SPEC_IND as out_QUOTN_SPEC_IND,
exp_pass_to_upd1.in_QUOTN_SPEC_STRT_DTTM as in_QUOTN_SPEC_STRT_DTTM,
exp_pass_to_upd1.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_pass_to_upd1.out_PRCS_ID as in_PRCS_ID,
exp_pass_to_upd1.out_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_pass_to_upd1.out_EDW_END_DTTM as in_EDW_END_DTTM,
exp_pass_to_upd1.out_QUOTN_SPEC_END_DTTM as in_QUOTN_SPEC_END_DTTM,
exp_pass_to_upd1.QUOTN_SPEC_DT as in_QUOTN_SPEC_DT,
exp_pass_to_upd1.source_record_id
FROM
exp_pass_to_upd1
WHERE ( exp_pass_to_upd1.CDC_Flag = ''I'' or exp_pass_to_upd1.CDC_Flag = ''U'' ) AND exp_pass_to_upd1.in_QUOTN_ID IS NOT NULL);


-- Component upd_ins_upd1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_ins_upd1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_agmt_spec1_INSERT.in_QUOTN_ID as QUOTN_ID,
rtr_agmt_spec1_INSERT.in_QUOTN_SPEC_TYPE as QUOTN_SPEC_TYPE_CD,
rtr_agmt_spec1_INSERT.in_QUOTN_SPEC_STRT_DTTM as QUOTN_SPEC_STRT_DTTM,
rtr_agmt_spec1_INSERT.in_QUOTN_SPEC_END_DTTM as QUOTN_SPEC_END_DTTM,
rtr_agmt_spec1_INSERT.in_SPEC_TYPE_CD as SPEC_TYPE_CD,
rtr_agmt_spec1_INSERT.out_QUOTN_SPEC_IND as QUOTN_SPEC_IND,
rtr_agmt_spec1_INSERT.in_PRCS_ID as PRCS_ID,
rtr_agmt_spec1_INSERT.in_EDW_STRT_DTTM as EDW_STRT_DTTM,
rtr_agmt_spec1_INSERT.in_EDW_END_DTTM as EDW_END_DTTM,
rtr_agmt_spec1_INSERT.in_TRANS_STRT_DTTM as TRANS_STRT_DTTM,
rtr_agmt_spec1_INSERT.TRANS_END_DTTM as TRANS_END_DTTM,
rtr_agmt_spec1_INSERT.in_QUOTN_SPEC_DT as QUOTN_SPEC_DT,
0 as UPDATE_STRATEGY_ACTION,
rtr_agmt_spec1_INSERT.source_record_id
FROM
rtr_agmt_spec1_INSERT
);


-- Component exp_pass_to_target_ins1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_ins1 AS
(
SELECT
upd_ins_upd1.QUOTN_ID as QUOTN_ID,
upd_ins_upd1.QUOTN_SPEC_TYPE_CD as QUOTN_SPEC_TYPE_CD,
upd_ins_upd1.QUOTN_SPEC_STRT_DTTM as QUOTN_SPEC_STRT_DTTM,
upd_ins_upd1.QUOTN_SPEC_END_DTTM as QUOTN_SPEC_END_DTTM,
upd_ins_upd1.SPEC_TYPE_CD as SPEC_TYPE_CD,
NULL as QUOTN_SPEC_CNT,
NULL as QUOTN_SPEC_TXT,
NULL as QUOTN_SPEC_QTY,
NULL as QUOTN_SPEC_RATE,
NULL as QUOTN_SPEC_AMT,
upd_ins_upd1.QUOTN_SPEC_DT as QUOTN_SPEC_DT,
upd_ins_upd1.QUOTN_SPEC_IND as QUOTN_SPEC_IND,
upd_ins_upd1.PRCS_ID as PRCS_ID,
upd_ins_upd1.EDW_STRT_DTTM as EDW_STRT_DTTM,
upd_ins_upd1.EDW_END_DTTM as EDW_END_DTTM,
upd_ins_upd1.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
upd_ins_upd1.TRANS_END_DTTM as TRANS_END_DTTM,
upd_ins_upd1.source_record_id
FROM
upd_ins_upd1
);


-- Component QUOTN_SPEC_AFFINITY, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_SPEC
(
QUOTN_ID,
QUOTN_SPEC_TYPE_CD,
QUOTN_SPEC_STRT_DTTM,
SPEC_TYPE_CD,
QUOTN_SPEC_END_DTTM,
QUOTN_SPEC_CNT,
QUOTN_SPEC_TXT,
QUOTN_SPEC_QTY,
QUOTN_SPEC_RATE,
QUOTN_SPEC_AMT,
QUOTN_SPEC_DT,
QUOTN_SPEC_IND,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_target_ins1.QUOTN_ID as QUOTN_ID,
exp_pass_to_target_ins1.QUOTN_SPEC_TYPE_CD as QUOTN_SPEC_TYPE_CD,
exp_pass_to_target_ins1.QUOTN_SPEC_STRT_DTTM as QUOTN_SPEC_STRT_DTTM,
exp_pass_to_target_ins1.SPEC_TYPE_CD as SPEC_TYPE_CD,
exp_pass_to_target_ins1.QUOTN_SPEC_END_DTTM as QUOTN_SPEC_END_DTTM,
exp_pass_to_target_ins1.QUOTN_SPEC_CNT as QUOTN_SPEC_CNT,
exp_pass_to_target_ins1.QUOTN_SPEC_TXT as QUOTN_SPEC_TXT,
exp_pass_to_target_ins1.QUOTN_SPEC_QTY as QUOTN_SPEC_QTY,
exp_pass_to_target_ins1.QUOTN_SPEC_RATE as QUOTN_SPEC_RATE,
exp_pass_to_target_ins1.QUOTN_SPEC_AMT as QUOTN_SPEC_AMT,
exp_pass_to_target_ins1.QUOTN_SPEC_DT as QUOTN_SPEC_DT,
exp_pass_to_target_ins1.QUOTN_SPEC_IND as QUOTN_SPEC_IND,
exp_pass_to_target_ins1.PRCS_ID as PRCS_ID,
exp_pass_to_target_ins1.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_target_ins1.EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_target_ins1.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_pass_to_target_ins1.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_to_target_ins1;


-- PIPELINE END FOR 2
-- Component QUOTN_SPEC_AFFINITY, Type Post SQL 
UPDATE DB_T_PROD_CORE.QUOTN_SPEC FROM

(SELECT	distinct QUOTN_ID, QUOTN_SPEC_TYPE_CD, EDW_STRT_DTTM,

max(QUOTN_SPEC_STRT_DTTM) over(partition by QUOTN_ID, QUOTN_SPEC_TYPE_CD ORDER by TRANS_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''

as lead1,

max(EDW_STRT_DTTM) over (partition by QUOTN_ID, QUOTN_SPEC_TYPE_CD ORDER by TRANS_STRT_DTTM  ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead2,

max(TRANS_STRT_DTTM) over (partition by QUOTN_ID, QUOTN_SPEC_TYPE_CD  ORDER by TRANS_STRT_DTTM  ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead3

FROM	DB_T_PROD_CORE.QUOTN_SPEC

WHERE EDW_END_DTTM=cast(''9999-12-31'' as date)

AND QUOTN_SPEC_TYPE_CD<>''AFFNTYGRP''

 ) a

set 

QUOTN_SPEC_END_DTTM=A.lead1,

EDW_END_DTTM=A.lead2,

TRANS_END_DTTM=A.lead3

where  QUOTN_SPEC.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and QUOTN_SPEC.QUOTN_ID=A.QUOTN_ID 

AND QUOTN_SPEC.QUOTN_SPEC_TYPE_CD = A.QUOTN_SPEC_TYPE_CD

and lead1 is not null

and lead2 is not null

and lead3 is not null;



UPDATE DB_T_PROD_CORE.QUOTN_SPEC FROM

(SELECT	distinct QUOTN_ID, QUOTN_SPEC_TYPE_CD,SPEC_TYPE_CD, EDW_STRT_DTTM,

max(QUOTN_SPEC_STRT_DTTM) over(partition by QUOTN_ID, QUOTN_SPEC_TYPE_CD,SPEC_TYPE_CD ORDER by TRANS_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''

as lead1,

max(EDW_STRT_DTTM) over (partition by QUOTN_ID, QUOTN_SPEC_TYPE_CD,SPEC_TYPE_CD ORDER by TRANS_STRT_DTTM  ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead2,

max(TRANS_STRT_DTTM) over (partition by QUOTN_ID, QUOTN_SPEC_TYPE_CD ,SPEC_TYPE_CD ORDER by TRANS_STRT_DTTM  ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead3

FROM	DB_T_PROD_CORE.QUOTN_SPEC

WHERE EDW_END_DTTM=cast(''9999-12-31'' as date)

AND QUOTN_SPEC_TYPE_CD=''AFFNTYGRP''

 ) a

set 

QUOTN_SPEC_END_DTTM=A.lead1,

EDW_END_DTTM=A.lead2,

TRANS_END_DTTM=A.lead3

where  QUOTN_SPEC.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and QUOTN_SPEC.QUOTN_ID=A.QUOTN_ID 

AND QUOTN_SPEC.QUOTN_SPEC_TYPE_CD = A.QUOTN_SPEC_TYPE_CD

AND QUOTN_SPEC.SPEC_TYPE_CD=A.SPEC_TYPE_CD

and lead1 is not null

and lead2 is not null

and lead3 is not null;


END; 
';