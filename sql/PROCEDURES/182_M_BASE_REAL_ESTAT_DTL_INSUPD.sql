-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_REAL_ESTAT_DTL_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare
	start_dttm timestamp;
	end_dttm timestamp;
	PRCS_ID int;

BEGIN 

	start_dttm := current_timestamp();
	end_dttm := current_timestamp();
	prcs_id := 1;

-- Component LKP_ROOF_CNSTRCTN_TYPE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_ROOF_CNSTRCTN_TYPE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''ROOF_CNSTRCTN_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''pctl_rooftype.TYPECODE'' , ''pctl_foprooftype.TYPECODE'')

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_CLASFCN'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'' ,''pcx_holineschcovitemcov_alfa.ChoiceTerm1'', ''contentlineitemschedule.typecode'', ''pctl_bp7classificationproperty.typecode'')

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'', ''GW'') 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_ENCUMBRANCE_TYPE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ENCUMBRANCE_TYPE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''ENCMCE_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''pctl_additionalinteresttype.typecode'', ''derived'') 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''GW'', ''DS'') 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_SRC_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_SRC_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component sq_pcx_dwelling_hoe, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_pcx_dwelling_hoe AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as FixedID,
$2 as prty_asset_sbtype_cd,
$3 as Class_Cd,
$4 as Roof_year,
$5 as Roof_Cnstrctn_Cd,
$6 as Expiration_dt,
$7 as Effective_dt,
$8 as src_cd,
$9 as encumbrance_type_cd,
$10 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
with pcpolicyperiod as (

SELECT distinct 

	pc_policyperiod.PeriodEnd_stg as PeriodEnd,

	pc_policyperiod.PeriodStart_stg as PeriodStart,

	pc_policyperiod.ID_stg as ID



from 

DB_T_PROD_STAG.pc_policyperiod left outer join DB_T_PROD_STAG.pc_policy on pc_policy.ID_stg=pc_policyperiod.PolicyID_stg

left outer join DB_T_PROD_STAG.pc_job on pc_policyperiod.JobID_stg=pc_job.ID_stg

/* left outer join DB_T_PROD_STAG.pc_paymentplansummary on pc_paymentplansummary.policyperiod = pc_policyperiod.ID_stg */
left outer join DB_T_PROD_STAG.pc_policyline on pc_policyline.branchid_stg = pc_policyperiod.ID_stg 

left outer join DB_T_PROD_STAG.pc_account on pc_account.ID_stg = pc_policy.AccountID_stg

/* left outer join DB_T_PROD_STAG.pc_uwissuehistory on pc_uwissuehistory.PolicyPeriodID=pc_policyperiod.ID_stg */
/* left outer join DB_T_PROD_STAG.pctl_billingperiodicity on  --pctl_billingperiodicity.id=pc_paymentplansummary.invoicefrequency */
left outer join DB_T_PROD_STAG.pctl_policyline ON pc_policyline.subtype_stg = pctl_policyline.id_stg

left outer join DB_T_PROD_STAG.pc_uwcompany ON pc_uwcompany.ID_stg  = pc_policyperiod.UWCompany_stg

left outer join DB_T_PROD_STAG.pctl_billingmethod on pc_policyperiod.billingmethod_stg=pctl_billingmethod.id_stg

left outer join DB_T_PROD_STAG.pcx_palineratingfactor_alfa on pcx_palineratingfactor_alfa.branchid_stg=pc_policyperiod.ID_stg

WHERE

 pc_policyperiod.UpdateTime_stg > (:start_dttm)

	and pc_policyperiod.UpdateTime_stg <= (:end_dttm)



)



select distinct 

cast(pcx_bp7building.fixedid as varchar(100)) as id

, ''PRTY_ASSET_SBTYPE13'' as type_code

, pcx_bp7building.classificationcode as classification_code

, pcx_bp7building.BP7RoofYear_alfa as roof_year

, roof_cnstrctn_type as rooftype

, coalesce(pcx_bp7building.ExpirationDate, pcpolicyperiod.PeriodEnd) as exp_dt

, coalesce (pcx_bp7building.EffectiveDate, pcpolicyperiod.PeriodStart) as eff_dt

, ''SRC_SYS4'' as src_cd

, pcx_bp7building.encumbrance_type_cd

from (



SELECT DISTINCT

      c.FixedID_stg as FixedID,

	  c.BranchID_stg as BranchID

	  ,c.ExpirationDate_stg as ExpirationDate

      ,c.EffectiveDate_stg as EffectiveDate

      ,b.BP7RoofYear_alfa_stg as BP7RoofYear_alfa

       ,rt.TYPECODE_stg AS roof_cnstrctn_type

       ,ait.TYPECODE_stg AS Encumbrance_type_cd

,cp.TYPECODE_stg as classificationcode /* Classification_name */
FROM 

DB_T_PROD_STAG.pcx_bp7classification c

INNER JOIN (select b.*, rank() over (partition by b.FixedId_stg order by b.UpdateTime_stg desc) r from DB_T_PROD_STAG.pcx_bp7building b) b 

		on c.Building_stg = b.FixedId_stg

		and c.branchid_stg=b.branchid_stg

		and b.r = 1 

/** EIM-15651 INCLUDED DB_T_PROD_STAG.PC_BUILDING table to have Building description column ****/		

INNER JOIN DB_T_PROD_STAG.PC_BUILDING building  on building.id_stg = b.Building_stg		

INNER JOIN DB_T_PROD_STAG.pctl_bp7classificationproperty cp on cp.id_stg = c.bp7classpropertytype_stg

join DB_T_PROD_STAG.pctl_bp7classdescription d on c.bp7classdescription_stg = d.ID_stg 

INNER JOIN DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg = b.branchid_stg

INNER JOIN DB_T_PROD_STAG.pc_policy p on p.id_stg = pp.PolicyID_stg 

LEFT JOIN DB_T_PROD_STAG.pctl_rooftype rt on b.BP7RoofType_alfa_stg = rt.ID_stg

INNER JOIN (select l.*, rank() over (partition by l.Fixedid_stg order by l.UPDATETIME_stg desc) r from DB_T_PROD_STAG.pcx_bp7location l) l 

		on b.Location_stg = l.FixedID_stg

		and l.r = 1

LEFT JOIN DB_T_PROD_STAG.pc_addlinterestdetail aid on aid.BP7Building_stg = b.ID_stg

LEFT JOIN DB_T_PROD_STAG.pctl_additionalinteresttype ait on aid.AdditionalInterestType_stg = ait.ID_stg

LEFT JOIN DB_T_PROD_STAG.pctl_bp7constructiontype t on t.ID_stg = b.bp7constructiontype_stg

LEFT JOIN (   SELECT ba.BP7Building_stg,ba.QuestionCode_stg,ba.BooleanAnswer_stg,MAX( ba.updatetime_stg) AS updatetime

                     FROM DB_T_PROD_STAG.pcx_bp7buildinganswer_alfa ba

                     WHERE ba.BooleanAnswer_stg is not null 

                     GROUP BY ba.BP7Building_stg,ba.QuestionCode_stg,ba.BooleanAnswer_stg

)ba on ba.BP7Building_stg = b.ID_stg

LEFT JOIN DB_T_PROD_STAG.pctl_bp7coolingtype_alfa cta on cta.ID_stg = b.BP7PriCoolingType_alfa_stg

LEFT JOIN DB_T_PROD_STAG.pctl_bp7heatingtype_alfa hta on hta.ID_stg = b.BP7PriHeatingType_alfa_stg

LEFT JOIN DB_T_PROD_STAG.pc_territorycode tc on tc.branchid_stg = pp.id_stg

INNER JOIN DB_T_PROD_STAG.pc_policyline pol on pol.BranchID_stg = pp.id_stg

LEFT JOIN DB_T_PROD_STAG.pctl_bp7policytype_alfa pta on pta.ID_stg = pol.BP7PolicyType_alfa_stg

WHERE b.ExpirationDate_stg IS NULL

AND c.ExpirationDate_stg IS NULL

AND l.ExpirationDate_stg IS NULL

AND ((c.UPDATETIME_stg > (:start_dttm) AND c.UPDATETIME_stg <= (:end_dttm))

    OR (b.UpdateTime_stg > (:start_dttm) AND b.UpdateTime_stg <= (:end_dttm))

	OR (l.UPDATETIME_stg > (:start_dttm) AND l.UPDATETIME_stg <= (:end_dttm)))



) pcx_bp7building

inner join pcpolicyperiod on pcx_bp7building.BranchID = pcpolicyperiod.ID

where pcx_bp7building.fixedid is not null



union



select distinct 

cast(pcx_dwelling_hoe.fixedid  as varchar(100)) as id ,

 ''PRTY_ASSET_SBTYPE5''  as type_code ,

 ''PRTY_ASSET_CLASFCN1'' as classification_code

 ,pcx_dwelling_hoe.RoofYear_alfa as roof_year, 

pctl_rooftype.TYPECODE_stg as rooftype

,coalesce(pcx_Dwelling_hoe.ExpirationDate,pcpolicyperiod.PeriodEnd) as exp_dt

,coalesce (pcx_Dwelling_HOE.EffectiveDate,pcpolicyperiod.PeriodStart) as eff_dt

,''SRC_SYS4'' as src_cd

,encumbrance_type_cd

from (



SELECT	distinct 

		pcx_Dwelling_HOE.FixedID_stg as fixedid,

		pcx_dwelling_hoe.BranchID_stg as BranchID,

		pcx_Dwelling_HOE.RoofYear_alfa_stg as RoofYear_alfa,

		coalesce(pcx_Dwelling_HOE.ExpirationDate_stg,pc_policyperiod.PeriodEnd_stg) as ExpirationDate,

		coalesce(pcx_Dwelling_HOE.EffectiveDate_stg,pc_policyperiod.PeriodStart_stg) as EffectiveDate, 

/* pcx_Dwelling_HOE.EffectiveDate_stg IS NULL as EffectiveDate,   */
/* pcx_Dwelling_HOE.ExpirationDate_stg IS NULL as ExpirationDate, 		 */
		pctl_additionalinteresttype.TYPECODE_stg as Encumbrance_type_cd,

		pcx_Dwelling_HOE.rooftype_stg as rooftype

		

		

from	DB_T_PROD_STAG.pcx_Dwelling_HOE 

left outer join DB_T_PROD_STAG.pc_policyperiod on pc_policyperiod.id_stg = pcx_Dwelling_HOE.branchid_stg 

left outer join DB_T_PROD_STAG.pc_policy on pc_policy.ID_stg=pc_policyperiod.PolicyID_stg

left outer join (

 select pcx_Dwelling_HOE.FixedID_stg as homealerfixedid, pcx_Dwelling_HOE.branchid_stg as branchid, HomeAlertCode_stg as homealert_cd, HurrMitigationCreditAmt_stg 

from   DB_T_PROD_STAG.pc_policyperiod

inner join DB_T_PROD_STAG.pcx_Dwelling_HOE on pcx_Dwelling_HOE.branchid_stg=pc_policyperiod.id_stg

inner join DB_T_PROD_STAG.pcx_dwellingratingfactor_alfa on pcx_Dwelling_HOE.FixedID_stg  = pcx_dwellingratingfactor_alfa.Dwelling_HOE_stg and pcx_dwellingratingfactor_alfa.BranchID_stg=pc_policyperiod.id_stg

where pcx_Dwelling_HOE.ExpirationDate_stg is null and pcx_dwellingratingfactor_alfa.ExpirationDate_stg is null 

) homealert on pcx_Dwelling_HOE.FixedID_stg=homealert.homealerfixedid and pcx_Dwelling_HOE.branchid_stg=homealert.branchid



 left outer join  DB_T_PROD_STAG.pctl_rooftype ON pcx_Dwelling_HOE.RoofType_stg=pctl_Rooftype.ID_stg

join DB_T_PROD_STAG.pcx_holocation_hoe on pcx_Dwelling_HOE.holocation_stg = pcx_holocation_hoe.ID_stg

left outer join DB_T_PROD_STAG.pctl_holocation_hoe on pcx_holocation_hoe.subtype_stg = pctl_holocation_hoe.ID_stg

 left outer join DB_T_PROD_STAG.pc_addlinterestdetail on pc_addlinterestdetail.dwelling_stg = pcx_Dwelling_HOE.ID_stg

left outer join DB_T_PROD_STAG.pctl_additionalinteresttype on pc_addlinterestdetail.AdditionalInterestType_stg=pctl_additionalinteresttype.id_stg

left outer join DB_T_PROD_STAG.pctl_constructiontype_hoe on pctl_constructiontype_hoe.ID_stg= pcx_Dwelling_HOE.constructiontype_stg

left outer join 

( 

select 

Dwelling_HOE_stg,

pcx_dwellinganswer_alf.QuestionCode_stg,

 pcx_dwellinganswer_alf.BooleanAnswer_stg  ,

max(pcx_dwellinganswer_alf.UpdateTime_stg) as updatetime from DB_T_PROD_STAG.pcx_dwellinganswer_alf 

where pcx_dwellinganswer_alf.BooleanAnswer_stg is not null group by Dwelling_HOE_stg,pcx_dwellinganswer_alf.QuestionCode_stg,pcx_dwellinganswer_alf.BooleanAnswer_stg 

) dwellinganswer on dwellinganswer.Dwelling_HOE_stg=pcx_Dwelling_HOE.ID_stg

left outer join DB_T_PROD_STAG.pctl_coolingtype_alfa on pctl_coolingtype_alfa.id_stg=pcx_Dwelling_HOE.PrimaryCooling_alfa_stg

left outer join DB_T_PROD_STAG.pc_policylocation on pc_policylocation.ID_stg=pcx_holocation_hoe.PolicyLocation_stg

left join DB_T_PROD_STAG.pc_territorycode on pc_territorycode.BranchID_stg=pc_policyperiod.id_stg  

left join DB_T_PROD_STAG.pc_policyline on pc_policyline.BranchID_stg=pc_policyperiod.id_stg

  left join DB_T_PROD_STAG.pctl_hopolicytype_hoe on pctl_hopolicytype_hoe.ID_stg=pc_policyline.HOPolicyType_stg

  left outer join DB_T_PROD_STAG.pctl_territorycode on pctl_territorycode.ID_stg=pc_territorycode.subtype_stg

where pcx_Dwelling_HOE.ExpirationDate_stg is null

and

pcx_Dwelling_HOE.UpdateTime_stg>(:start_dttm) AND 	pcx_Dwelling_HOE.UpdateTime_stg <= (:end_dttm)



) pcx_Dwelling_HOE 

inner join pcpolicyperiod on pcx_dwelling_hoe.BranchID=pcpolicyperiod.ID

left outer join DB_T_PROD_STAG.pctl_rooftype on pcx_dwelling_hoe.RoofType=pctl_rooftype.id_stg



where pcx_dwelling_hoe.fixedid is not null



UNION



SELECT DISTINCT

	Cast(pcx_fopoutbuilding.fixedid AS VARCHAR(100)) AS id,

	''PRTY_ASSET_SBTYPE36'' AS type_code,

	''PRTY_ASSET_CLASFCN13'' AS classification_code,

	pcx_fopoutbuilding.RoofYear_alfa AS roof_year, 

	pcx_fopoutbuilding.RoofType AS rooftype,

	Coalesce(pcx_fopoutbuilding.ExpirationDate,pcpolicyperiod.PeriodEnd) AS exp_dt,

	Coalesce (pcx_fopoutbuilding.EffectiveDate,pcpolicyperiod.PeriodStart) AS eff_dt,

	''SRC_SYS4'' AS src_cd,

	pcx_fopoutbuilding.Encumbrance_type_cd

FROM (

	SELECT DISTINCT 

		pcx_fopoutbuilding.FixedID_stg AS fixedid,

		pcx_fopoutbuilding.BranchID_stg AS BranchID,

		''PRTY_ASSET_SBTYPE36'' AS type_code,

		''PRTY_ASSET_CLASFCN13'' AS classification_code,

		pcx_fopoutbuilding.RoofYr_stg AS RoofYear_alfa,

		pctl_foprooftype.typecode_stg AS RoofType,

		Coalesce(pcx_fopoutbuilding.ExpirationDate_stg, pc_policyperiod.PeriodEnd_stg) AS ExpirationDate,

		Coalesce(pcx_fopoutbuilding.EffectiveDate_stg, pc_policyperiod.PeriodStart_stg) AS EffectiveDate,

		''SRC_SYS4'' AS src_cd,

		Cast(NULL AS VARCHAR(50)) AS Encumbrance_type_cd

	FROM DB_T_PROD_STAG.pcx_fopoutbuilding

		LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.ID_stg = pcx_fopoutbuilding.BranchID_stg

		LEFT OUTER JOIN DB_T_PROD_STAG.pc_policy ON pc_policy.ID_stg = pc_policyperiod.PolicyID_stg

		LEFT JOIN DB_T_PROD_STAG.pctl_foprooftype ON pctl_foprooftype.id_stg = pcx_fopoutbuilding.RoofType_stg

	WHERE (pcx_fopoutbuilding.ExpirationDate_stg IS NULL OR pcx_fopoutbuilding.expirationdate_stg > pc_policyperiod.Editeffectivedate_stg)

		AND pcx_fopoutbuilding.UpdateTime_stg > (:start_dttm)

		AND pcx_fopoutbuilding.UpdateTime_stg < (:end_dttm)

	)  pcx_fopoutbuilding

INNER JOIN pcpolicyperiod ON pcx_fopoutbuilding.BranchID = pcpolicyperiod.ID

LEFT JOIN DB_T_PROD_STAG.pctl_foprooftype ON pcx_fopoutbuilding.RoofType = pctl_foprooftype.id_stg

WHERE pcx_fopoutbuilding.fixedid IS NOT NULL



UNION



SELECT DISTINCT

	Cast(pcx_fopdwelling.fixedid AS VARCHAR(100)) AS id,

	''PRTY_ASSET_SBTYPE37'' AS type_code,

	''PRTY_ASSET_CLASFCN15'' AS classification_code,

	pcx_fopdwelling.RoofYear_alfa AS roof_year,

	pcx_fopdwelling.RoofType AS rooftype,

	Coalesce(pcx_fopdwelling.ExpirationDate, pcpolicyperiod.PeriodEnd) AS exp_dt,

	Coalesce(pcx_fopdwelling.EffectiveDate, pcpolicyperiod.PeriodStart) AS eff_dt,

	''SRC_SYS4'' AS src_cd,

	pcx_fopdwelling.Encumbrance_type_cd

FROM (

	SELECT DISTINCT

		pcx_fopdwelling.FixedID_stg AS fixedid,

		pcx_fopdwelling.BranchID_stg AS BranchID,

		''PRTY_ASSET_SBTYPE37'' AS type_code,

		''PRTY_ASSET_CLASFCN15'' AS classification_code,

		pcx_fopdwelling.RoofYear_stg AS RoofYear_alfa,

		pctl_foprooftype.typecode_stg AS RoofType,

		Coalesce(pcx_fopdwelling.ExpirationDate_stg, pc_policyperiod.PeriodEnd_stg) AS ExpirationDate,

		Coalesce(pcx_fopdwelling.EffectiveDate_stg, pc_policyperiod.PeriodStart_stg) AS EffectiveDate,

		''SRC_SYS4'' AS src_cd,

		Cast(NULL AS VARCHAR(50)) AS Encumbrance_type_cd

	FROM DB_T_PROD_STAG.pcx_fopdwelling

		LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON pc_policyperiod.ID_stg = pcx_fopdwelling.BranchID_stg

		LEFT OUTER JOIN DB_T_PROD_STAG.pc_policy ON pc_policy.ID_stg = pc_policyperiod.PolicyID_stg

		LEFT JOIN DB_T_PROD_STAG.pctl_foprooftype ON pctl_foprooftype.id_stg = pcx_fopdwelling.RoofType_stg

	WHERE (pcx_fopdwelling.ExpirationDate_stg IS NULL OR pcx_fopdwelling.expirationdate_stg > pc_policyperiod.Editeffectivedate_stg)

		AND pcx_fopdwelling.UpdateTime_stg > (:start_dttm)

		AND pcx_fopdwelling.UpdateTime_stg < (:end_dttm)

	) pcx_fopdwelling

INNER JOIN pcpolicyperiod ON pcx_fopdwelling.BranchID = pcpolicyperiod.ID

LEFT JOIN DB_T_PROD_STAG.pctl_foprooftype ON pcx_fopdwelling.RoofType = pctl_foprooftype.id_stg

WHERE pcx_fopdwelling.fixedid IS NOT NULL
) SRC
)
);


-- Component agg_rem_dups, Type AGGREGATOR 
CREATE OR REPLACE TEMPORARY TABLE agg_rem_dups AS
(
SELECT
sq_pcx_dwelling_hoe.FixedID as FixedID,
sq_pcx_dwelling_hoe.prty_asset_sbtype_cd as prty_asset_sbtype_cd,
sq_pcx_dwelling_hoe.Class_Cd as Class_Cd,
MIN(sq_pcx_dwelling_hoe.Roof_year) as Roof_year,
MIN(sq_pcx_dwelling_hoe.Roof_Cnstrctn_Cd) as Roof_Cnstrctn_Cd,
MIN(sq_pcx_dwelling_hoe.Effective_dt) as Effective_dt,
MIN(sq_pcx_dwelling_hoe.Expiration_dt) as Expiration_dt,
MIN(sq_pcx_dwelling_hoe.src_cd) as src_cd,
MIN(sq_pcx_dwelling_hoe.encumbrance_type_cd) as encumbrance_type_cd,
MIN(sq_pcx_dwelling_hoe.source_record_id) as source_record_id
FROM
sq_pcx_dwelling_hoe
GROUP BY
sq_pcx_dwelling_hoe.FixedID,
sq_pcx_dwelling_hoe.prty_asset_sbtype_cd,
sq_pcx_dwelling_hoe.Class_Cd
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
agg_rem_dups.FixedID as FixedID,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ END as out_prty_asset_sbtype_cd,
CASE WHEN LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ IS NULL THEN ''UNK'' ELSE LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ END as out_class_cd,
agg_rem_dups.Roof_year as Roof_year,
agg_rem_dups.Roof_Cnstrctn_Cd as Roof_Cnstrctn_Type,
CASE WHEN agg_rem_dups.Effective_dt IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) ELSE agg_rem_dups.Effective_dt END as out_EffectiveDate,
CASE WHEN agg_rem_dups.Expiration_dt IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) ELSE agg_rem_dups.Expiration_dt END as out_Expiration_dt,
LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as o_src_cd,
CASE WHEN LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ENCUMBRANCE_TYPE_CD */ IS NULL THEN ''UNK'' ELSE LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ENCUMBRANCE_TYPE_CD */ END as out_encumbrance_type_cd,
agg_rem_dups.source_record_id,
row_number() over (partition by agg_rem_dups.source_record_id order by agg_rem_dups.source_record_id) as RNK
FROM
agg_rem_dups
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = agg_rem_dups.prty_asset_sbtype_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = agg_rem_dups.prty_asset_sbtype_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = agg_rem_dups.Class_Cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = agg_rem_dups.Class_Cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = agg_rem_dups.src_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ENCUMBRANCE_TYPE_CD LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = agg_rem_dups.encumbrance_type_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ENCUMBRANCE_TYPE_CD LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = agg_rem_dups.encumbrance_type_cd
QUALIFY RNK = 1
);


-- Component LKP_PRTY_ASSET_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_ASSET_ID AS
(
SELECT
LKP.PRTY_ASSET_ID,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_HOST_ID_VAL asc,LKP.PRTY_ASSET_SBTYPE_CD asc,LKP.PRTY_ASSET_CLASFCN_CD asc,LKP.ASSET_INSRNC_HIST_TYPE_CD asc,LKP.ASSET_DESC asc,LKP.PRTY_ASSET_NAME asc,LKP.PRTY_ASSET_STRT_DTTM asc,LKP.PRTY_ASSET_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc) RNK
FROM
exp_data_transformation
LEFT JOIN (
SELECT PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET.ASSET_INSRNC_HIST_TYPE_CD as ASSET_INSRNC_HIST_TYPE_CD, PRTY_ASSET.ASSET_DESC as ASSET_DESC, PRTY_ASSET.PRTY_ASSET_NAME as PRTY_ASSET_NAME, PRTY_ASSET.PRTY_ASSET_STRT_DTTM as PRTY_ASSET_STRT_DTTM, PRTY_ASSET.PRTY_ASSET_END_DTTM as PRTY_ASSET_END_DTTM, PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM, PRTY_ASSET.SRC_SYS_CD as SRC_SYS_CD, PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL, PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD, PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD 
FROM db_t_prod_core.PRTY_ASSET 
QUALIFY ROW_NUMBER() OVER(PARTITION BY  ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,PRTY_ASSET_CLASFCN_CD ORDER BY EDW_END_DTTM DESC) = 1
) LKP ON LKP.ASSET_HOST_ID_VAL = exp_data_transformation.FixedID AND LKP.PRTY_ASSET_SBTYPE_CD = exp_data_transformation.out_prty_asset_sbtype_cd AND LKP.PRTY_ASSET_CLASFCN_CD = exp_data_transformation.out_class_cd
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_HOST_ID_VAL asc,LKP.PRTY_ASSET_SBTYPE_CD asc,LKP.PRTY_ASSET_CLASFCN_CD asc,LKP.ASSET_INSRNC_HIST_TYPE_CD asc,LKP.ASSET_DESC asc,LKP.PRTY_ASSET_NAME asc,LKP.PRTY_ASSET_STRT_DTTM asc,LKP.PRTY_ASSET_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc)  
= 1
);


-- Component LKP_REAL_ESTAT_PA_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_REAL_ESTAT_PA_ID AS
(
SELECT
LKP.PRTY_ASSET_ID,
LKP.REAL_ESTAT_DTL_STRT_DTTM,
LKP.ROOF_CNSTRCTN_TYPE_CD,
LKP_PRTY_ASSET_ID.source_record_id,
ROW_NUMBER() OVER(PARTITION BY LKP_PRTY_ASSET_ID.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.REAL_ESTAT_DTL_STRT_DTTM asc,LKP.ROOF_CNSTRCTN_TYPE_CD asc) RNK
FROM
LKP_PRTY_ASSET_ID
LEFT JOIN (
SELECT REAL_ESTAT_DTL.PRTY_ASSET_ID,
 REAL_ESTAT_DTL.REAL_ESTAT_DTL_STRT_DTTM,
 REAL_ESTAT_DTL.ROOF_CNSTRCTN_TYPE_CD 
from
db_t_prod_core.real_estat_dtl where real_estat_dtl.REAL_ESTAT_DTL_END_DTTM is null
) LKP ON LKP.PRTY_ASSET_ID = LKP_PRTY_ASSET_ID.PRTY_ASSET_ID
QUALIFY ROW_NUMBER() OVER(PARTITION BY LKP_PRTY_ASSET_ID.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.REAL_ESTAT_DTL_STRT_DTTM asc,LKP.ROOF_CNSTRCTN_TYPE_CD asc) 
= 1
);


-- Component exp_data_trans, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_trans AS
(
SELECT
LKP_PRTY_ASSET_ID.PRTY_ASSET_ID as PRTY_ASSET_ID,
exp_data_transformation.out_EffectiveDate as src_EffectiveDate,
:PRCS_ID as out_PROCESS_ID,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_ROOF_CNSTRCTN_TYPE_CD */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_ROOF_CNSTRCTN_TYPE_CD */ END as out_roof_cnstrctn_type_cd,
exp_data_transformation.Roof_year as Roof_year,
exp_data_transformation.out_Expiration_dt as src_Expiration_dt,
exp_data_transformation.out_encumbrance_type_cd as encumbrance_type_cd,
exp_data_transformation.source_record_id,
row_number() over (partition by exp_data_transformation.source_record_id order by exp_data_transformation.source_record_id) as RNK
FROM
exp_data_transformation
INNER JOIN LKP_PRTY_ASSET_ID ON exp_data_transformation.source_record_id = LKP_PRTY_ASSET_ID.source_record_id
INNER JOIN LKP_REAL_ESTAT_PA_ID ON LKP_PRTY_ASSET_ID.source_record_id = LKP_REAL_ESTAT_PA_ID.source_record_id
LEFT JOIN LKP_ROOF_CNSTRCTN_TYPE_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = exp_data_transformation.Roof_Cnstrctn_Type
LEFT JOIN LKP_ROOF_CNSTRCTN_TYPE_CD LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = exp_data_transformation.Roof_Cnstrctn_Type
QUALIFY row_number() over (partition by exp_data_transformation.source_record_id order by exp_data_transformation.source_record_id) 
= 1
);


-- Component exp_SrcFields, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_SrcFields AS
(
SELECT
exp_data_trans.PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_data_trans.src_EffectiveDate as in_REAL_ESTST_DTL_SRTR_DTTM,
exp_data_trans.src_Expiration_dt as in_REAL_ESTAT_DTL_END_DTTM,
exp_data_trans.out_roof_cnstrctn_type_cd as in_ROOF_CNSTRCTN_TYPE_CD,
exp_data_trans.Roof_year as in_ROOF_YR,
exp_data_trans.encumbrance_type_cd as in_ENCMCE_TYPE_CD,
exp_data_trans.out_PROCESS_ID as in_PROCESS_ID,
exp_data_trans.source_record_id
FROM
exp_data_trans
);


-- Component LKP_REAL_ESTAT_DTL, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_REAL_ESTAT_DTL AS
(
SELECT
LKP.PRTY_ASSET_ID,
LKP.REAL_ESTAT_DTL_STRT_DTTM,
LKP.REAL_ESTAT_DTL_END_DTTM,
LKP.ROOF_CNSTRCTN_TYPE_CD,
LKP.ROOF_YR,
LKP.ENCMCE_TYPE_CD,
LKP.EDW_STRT_DTTM,
exp_SrcFields.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_SrcFields.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_SrcFields.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.REAL_ESTAT_DTL_STRT_DTTM asc,LKP.REAL_ESTAT_DTL_END_DTTM asc,LKP.ROOF_CNSTRCTN_TYPE_CD asc,LKP.ROOF_YR asc,LKP.ENCMCE_TYPE_CD asc,LKP.EDW_STRT_DTTM asc) RNK
FROM
exp_SrcFields
LEFT JOIN (
SELECT REAL_ESTAT_DTL.REAL_ESTAT_DTL_STRT_DTTM as REAL_ESTAT_DTL_STRT_DTTM, REAL_ESTAT_DTL.REAL_ESTAT_DTL_END_DTTM as REAL_ESTAT_DTL_END_DTTM, REAL_ESTAT_DTL.FIREPL_CNT as FIREPL_CNT, REAL_ESTAT_DTL.FIRE_EXTINGSHR_CNT as FIRE_EXTINGSHR_CNT, REAL_ESTAT_DTL.SMK_ALRM_IND as SMK_ALRM_IND, REAL_ESTAT_DTL.AGE_OF_STRC_NUM as AGE_OF_STRC_NUM, REAL_ESTAT_DTL.WOODSTV_CNT as WOODSTV_CNT, REAL_ESTAT_DTL.FIRE_ALRM_IND as FIRE_ALRM_IND, REAL_ESTAT_DTL.BRGLR_ALRM_IND as BRGLR_ALRM_IND, REAL_ESTAT_DTL.DEADBLT_LCK_IND as DEADBLT_LCK_IND, REAL_ESTAT_DTL.ROOF_CNSTRCTN_TYPE_CD as ROOF_CNSTRCTN_TYPE_CD, REAL_ESTAT_DTL.RECPTN_RM_CNT as RECPTN_RM_CNT, REAL_ESTAT_DTL.BDRM_CNT as BDRM_CNT, REAL_ESTAT_DTL.OUTBLDG_CNT as OUTBLDG_CNT, REAL_ESTAT_DTL.BLDG_STRY_CNT as BLDG_STRY_CNT, REAL_ESTAT_DTL.REAL_ESTAT_AREA_MEAS as REAL_ESTAT_AREA_MEAS, REAL_ESTAT_DTL.ROOF_YR as ROOF_YR, REAL_ESTAT_DTL.ENCMCE_TYPE_CD as ENCMCE_TYPE_CD, REAL_ESTAT_DTL.MORTGD_IND as MORTGD_IND, REAL_ESTAT_DTL.PRCS_ID as PRCS_ID, REAL_ESTAT_DTL.EDW_STRT_DTTM as EDW_STRT_DTTM, REAL_ESTAT_DTL.EDW_END_DTTM as EDW_END_DTTM, REAL_ESTAT_DTL.PRTY_ASSET_ID as PRTY_ASSET_ID FROM db_t_prod_core.REAL_ESTAT_DTL
QUALIFY ROW_NUMBER() OVER(PARTITION BY PRTY_ASSET_ID ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.PRTY_ASSET_ID = exp_SrcFields.in_PRTY_ASSET_ID
QUALIFY RNK = 1
);


-- Component exp_CDC_Check, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CDC_Check AS
(
SELECT
exp_SrcFields.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_SrcFields.in_REAL_ESTST_DTL_SRTR_DTTM as in_REAL_ESTST_DTL_SRTR_DTTM,
exp_SrcFields.in_REAL_ESTAT_DTL_END_DTTM as in_REAL_ESTAT_DTL_END_DTTM,
exp_SrcFields.in_ROOF_CNSTRCTN_TYPE_CD as in_ROOF_CNSTRCTN_TYPE_CD,
exp_SrcFields.in_ROOF_YR as in_ROOF_YR,
exp_SrcFields.in_ENCMCE_TYPE_CD as in_ENCMCE_TYPE_CD,
exp_SrcFields.in_PROCESS_ID as in_PROCESS_ID,
LKP_REAL_ESTAT_DTL.PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
LKP_REAL_ESTAT_DTL.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
MD5 ( ltrim ( rtrim ( exp_SrcFields.in_REAL_ESTST_DTL_SRTR_DTTM ) ) || ltrim ( rtrim ( exp_SrcFields.in_REAL_ESTAT_DTL_END_DTTM ) ) || ltrim ( rtrim ( exp_SrcFields.in_ROOF_CNSTRCTN_TYPE_CD ) ) || ltrim ( rtrim ( exp_SrcFields.in_ROOF_YR ) ) ) as v_MD5_Src,
MD5 ( ltrim ( rtrim ( LKP_REAL_ESTAT_DTL.REAL_ESTAT_DTL_STRT_DTTM ) ) || ltrim ( rtrim ( LKP_REAL_ESTAT_DTL.REAL_ESTAT_DTL_END_DTTM ) ) || ltrim ( rtrim ( LKP_REAL_ESTAT_DTL.ROOF_CNSTRCTN_TYPE_CD ) ) || ltrim ( rtrim ( LKP_REAL_ESTAT_DTL.ROOF_YR ) ) ) as v_MD5_Tgt,
CASE WHEN v_MD5_Tgt IS NULL THEN ''I'' ELSE CASE WHEN v_MD5_Src = v_MD5_Tgt THEN ''X'' ELSE ''U'' END END as o_Src_Tgt,
CURRENT_TIMESTAMP as StartDate,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EndDate,
exp_SrcFields.source_record_id
FROM
exp_SrcFields
INNER JOIN LKP_REAL_ESTAT_DTL ON exp_SrcFields.source_record_id = LKP_REAL_ESTAT_DTL.source_record_id
);


-- Component rtr_CDC_Insert, Type ROUTER Output Group Insert
create or replace temporary table rtr_CDC_Insert as
SELECT
exp_CDC_Check.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_CDC_Check.in_REAL_ESTST_DTL_SRTR_DTTM as in_REAL_ESTST_DTL_SRTR_DTTM,
exp_CDC_Check.in_REAL_ESTAT_DTL_END_DTTM as in_REAL_ESTAT_DTL_END_DTTM,
exp_CDC_Check.in_ROOF_CNSTRCTN_TYPE_CD as in_ROOF_CNSTRCTN_TYPE_CD,
exp_CDC_Check.in_ROOF_YR as in_ROOF_YR,
exp_CDC_Check.in_ENCMCE_TYPE_CD as in_ENCMCE_TYPE_CD,
exp_CDC_Check.in_PROCESS_ID as in_PROCESS_ID,
exp_CDC_Check.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_CDC_Check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check.o_Src_Tgt as o_Src_Tgt,
exp_CDC_Check.StartDate as StartDate,
exp_CDC_Check.EndDate as EndDate,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE ( exp_CDC_Check.o_Src_Tgt = ''I'' and exp_CDC_Check.in_PRTY_ASSET_ID IS NOT NULL ) or ( exp_CDC_Check.o_Src_Tgt = ''U'' );


-- Component tgt_REAL_ESTAT_DTL_NewInsert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.REAL_ESTAT_DTL
(
PRTY_ASSET_ID,
REAL_ESTAT_DTL_STRT_DTTM,
REAL_ESTAT_DTL_END_DTTM,
ROOF_CNSTRCTN_TYPE_CD,
ROOF_YR,
ENCMCE_TYPE_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
rtr_CDC_Insert.in_PRTY_ASSET_ID as PRTY_ASSET_ID,
rtr_CDC_Insert.in_REAL_ESTST_DTL_SRTR_DTTM as REAL_ESTAT_DTL_STRT_DTTM,
rtr_CDC_Insert.in_REAL_ESTAT_DTL_END_DTTM as REAL_ESTAT_DTL_END_DTTM,
rtr_CDC_Insert.in_ROOF_CNSTRCTN_TYPE_CD as ROOF_CNSTRCTN_TYPE_CD,
rtr_CDC_Insert.in_ROOF_YR as ROOF_YR,
rtr_CDC_Insert.in_ENCMCE_TYPE_CD as ENCMCE_TYPE_CD,
rtr_CDC_Insert.in_PROCESS_ID as PRCS_ID,
rtr_CDC_Insert.StartDate as EDW_STRT_DTTM,
rtr_CDC_Insert.EndDate as EDW_END_DTTM
FROM
rtr_CDC_Insert;


-- Component tgt_REAL_ESTAT_DTL_NewInsert, Type Post SQL 
UPDATE  db_t_prod_core.REAL_ESTAT_DTL  
set EDW_END_DTTM=A.lead1

FROM

(SELECT	distinct  PRTY_ASSET_ID,EDW_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by  PRTY_ASSET_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead1

FROM	db_t_prod_core.REAL_ESTAT_DTL

 ) a


where  REAL_ESTAT_DTL.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and REAL_ESTAT_DTL.PRTY_ASSET_ID=A.PRTY_ASSET_ID 

and CAST(REAL_ESTAT_DTL.EDW_END_DTTM AS DATE)=''9999-12-31''

and lead1 is not null;


END; ';