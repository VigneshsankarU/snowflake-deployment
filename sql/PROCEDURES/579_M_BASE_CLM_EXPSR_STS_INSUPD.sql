-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_EXPSR_STS_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE
  END_DTTM STRING;
  PRCS_ID STRING;
  START_DTTM STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);

  END_DTTM := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
  PRCS_ID := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);
  START_DTTM := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);
 

-- Component LKP_TERADATA_ETL_REF_XLAT_CLM_EXPSR_STS_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_CLM_EXPSR_STS_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''CLM_EXPSR_STS_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component sq_exposure_status, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_exposure_status AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Rank,
$2 as clm_expsr_nk,
$3 as dt,
$4 as sts_cd,
$5 as reason,
$6 as Trans_Strt_dttm,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
WITH ccexposure as (



SELECT	 distinct

		a.publicid_stg as publicid,	

		a.CreateTime_stg as createtime,

		a.reopendate_stg as reopendate,

		a.updatetime_stg as updatetime,

   		a.ReopenedReason_stg as reopenedreason,

        a.CloseDate_stg as closedate,

		a.ClosedOutcome_stg as closedoutcome

		

FROM

db_t_prod_stag.cc_exposure a

inner join (

	select	cc_claim.* 

	from	db_t_prod_stag.cc_claim 

	inner join db_t_prod_stag.cctl_claimstate 

		on cc_claim.State_stg= cctl_claimstate.id_stg 

	where	cctl_claimstate.name_stg <> ''Draft'')  b 

	on a.ClaimID_stg=b.ID_stg 

inner join db_t_prod_stag.cctl_exposuretype expotype 

	on expotype.ID_stg = a.ExposureType_stg 

and expotype.retired_stg=0/* EIM-17093 Adding new join to bring Exposuretypecode */
left outer join db_t_prod_stag.cc_contact c 

	on c.ID_stg = a.ClaimantDenormID_stg

left outer join db_t_prod_stag.cctl_contact cc 

	on c.subtype_stg = cc.ID_stg

/************************** EIM-16161   NET LOSS REPORT  Taking the latest cc_evaluation UpdateTime_stg record ***********************************************/

LEFT OUTER JOIN (

	select	cc_evaluation.*, rank() over(

	partition by exposureid_stg 

	order by UpdateTime_stg desc) rnk  

	from	db_t_prod_stag.cc_evaluation ) cc_evaluation

 on a.id_stg = cc_evaluation.exposureid_stg 

	and cc_evaluation.rnk=1 

left outer join 

(

	select	distinct cc_incident.id_stg as incid,

			cc_incident.subtype_stg, cctl_incident.typecode_stg as type1,

			case 

				when cctl_incident.typecode_stg = ''VehicleIncident'' then upper(insurable_key_veh)

				when cctl_incident.typecode_stg = ''InjuryIncident'' then upper(insurable_key_inj) 

				when cctl_incident.typecode_stg = ''FixedPropertyIncident'' 

		or cctl_incident.typecode_stg = ''OtherStructureIncident'' then upper(insurable_key_dwell)

				when cctl_incident.typecode_stg = ''DwellingIncident'' then upper(insurable_key_dwell_inc)

				when cctl_incident.typecode_stg = ''PropertyContentsIncident'' 

		or cctl_incident.typecode_stg = ''LivingExpensesIncident'' then upper(insurable_key_propcont)

	end as insurable_key

	from	db_t_prod_stag.cc_incident

	left join db_t_prod_stag.cctl_incident 

		on cc_incident.subtype_stg = cctl_incident.id_stg

	left outer join

	(

		select	cc_contact.PublicID_stg as insurable_key_inj,  cc_claimcontactrole.incidentID_stg 

		from	(

			select	* 

			from	db_t_prod_stag.cc_claimcontactrole 

			where	retired_stg = 0) cc_claimcontactrole

		join  db_t_prod_stag.cc_claimcontact  

			on cc_claimcontactrole.ClaimContactID_stg = cc_claimcontact.id_stg

		join db_t_prod_stag.cc_contact 

			on cc_claimcontact.ContactID_stg = cc_contact.ID_stg

		join db_t_prod_stag.cctl_contactrole 

			on cctl_contactrole.id_stg=cc_claimcontactrole.role_stg                

			and cctl_contactrole.typecode_stg=''injured''

	) injuredpartydetails 

		on cc_incident.id_stg = injuredpartydetails.IncidentID_stg

	left outer join 

	( 

		select	cc_vehicle.id_stg,

				

				case 

					when PolicySystemId_stg is not null then SUBSTR(PolicySystemId_stg, POSITION('':'',PolicySystemId_stg)+1)

					when (PolicySystemId_stg is null 

			and Vin_stg is not null) then ''VIN:''|| vin_stg 

					when (PolicySystemId_stg is null 

			and Vin_stg is null 

			and LicensePlate_stg is not null) then ''LP:'' || LicensePlate_stg 

					when (PolicySystemId_stg is null 

			and Vin_stg is null 

			and LicensePlate_stg is null) then PublicID_stg

		end as insurable_key_veh

		from	db_t_prod_stag.cc_vehicle 

	) veh 

		on cc_incident.VehicleID_stg = veh.ID_stg

	

/*  Avinash */
	left outer join 

	(

		select	cc_claim.ClaimNumber_stg,

				cc_incident.id_stg, cc_incident.subtype_stg, cctl_incident.name_stg,  cc_policylocation.PolicySystemId_stg,

				cc_incident.Description_stg, cc_address.addressline1_stg, 

		cc_address.AddressLine2_stg,

				 

		case 

					when PolicySystemId_stg is null then cc_incident.PublicID_stg 

					else 

		SUBSTR(cc_policylocation.PolicySystemId_stg, POSITION('':'',cc_policylocation.PolicySystemId_stg)+1) end

		as  insurable_key_dwell_inc

		from	 (

			select	cc_claim.* 

			from	db_t_prod_stag.cc_claim 

			inner join db_t_prod_stag.cctl_claimstate 

				on cc_claim.State_stg= cctl_claimstate.id_stg 

			where	cctl_claimstate.name_stg <> ''Draft'') cc_claim 

		left join db_t_prod_stag.cc_incident 

			on cc_claim.id_stg = cc_incident.ClaimID_stg

		left join db_t_prod_stag.cctl_incident 

			on cc_incident.subtype_stg = cctl_incident.id_stg

		left join db_t_prod_stag.cc_address 

			on cc_claim.LossLocationID_stg = cc_address.ID_stg

		left join db_t_prod_stag.cc_policylocation 

			on cc_policylocation.AddressID_stg= cc_address.ID_stg

		where	cctl_incident.name_stg =''DwellingIncident''

	) Dwelling_inc 

		on Dwelling_inc.ID_stg=cc_incident.id_stg

	left outer join 

	(

		select	cc_claim.ClaimNumber_stg,

				cc_incident.id_stg, cc_incident.subtype_stg, cctl_incident.name_stg,  cc_policylocation.PolicySystemId_stg,

				cc_incident.Description_stg, cc_address.addressline1_stg, 

		cc_address.AddressLine2_stg,

				 

		cc_incident.PublicID_stg as insurable_key_dwell

		from	(

			select	cc_claim.* 

			from	db_t_prod_stag.cc_claim 

			inner join db_t_prod_stag.cctl_claimstate 

				on cc_claim.State_stg= cctl_claimstate.id_stg 

			where	cctl_claimstate.name_stg <> ''Draft'') cc_claim 

		left join db_t_prod_stag.cc_incident 

			on cc_claim.id_stg = cc_incident.ClaimID_stg

		left join db_t_prod_stag.cctl_incident 

			on cc_incident.subtype_stg = cctl_incident.id_stg

		left join db_t_prod_stag.cc_address 

			on cc_claim.LossLocationID_stg = cc_address.ID_stg

		left join db_t_prod_stag.cc_policylocation 

			on cc_policylocation.AddressID_stg= cc_address.ID_stg

		where	cctl_incident.name_stg in (''OtherStructureIncident'',

				''FixedPropertyIncident'')

	) Dwelling 

		on Dwelling.ID_stg=cc_incident.id_stg

	left outer join 

	(

		

	Select ClaimNumber_stg,

				id_stg,subtype_stg,name_stg,PolicySystemId_stg,Description_stg,addressline1_stg,AddressLine2_stg,

				insurable_key_propcont 

FROM(/*  Added outer query as part of EIM- 20343, EIM-18973 */
		select	cc_claim.ClaimNumber_stg,

				cc_incident.id_stg, cc_incident.subtype_stg, cctl_incident.name_stg,  cc_riskunit.PolicySystemId_stg,

				cc_incident.Description_stg, cc_address.addressline1_stg, 

		cc_address.AddressLine2_stg,

				 

		case 

					when cc_riskunit.PolicySystemId_stg is null then cc_incident.PublicID_stg 

					else 

		SUBSTR(cc_riskunit.PolicySystemId_stg,POSITION('':'',cc_riskunit.PolicySystemId_stg)+1) end

		as  insurable_key_propcont

		,

				RANK() OVER(

		PARTITION BY cc_incident.id_stg 

		ORDER BY CC_INCIDENT.UpdateTime_stg DESC,CC_RISKUNIT.UpdateTime_stg DESC,

CC_EXPOSURE.UpdateTime_stg DESC,CC_COVERAGE.UpdateTime_stg DESC, CC_ADDRESS.UpdateTime_stg DESC) RNK   /*  Added as part of EIM-17289, Added exposure ,incident , coverage, address UpdateTime_stg as part of EIM-18973, 21273 */
		from	 (

			select	cc_claim.* 

			from	db_t_prod_stag.cc_claim 

			inner join db_t_prod_stag.cctl_claimstate 

				on cc_claim.State_stg= cctl_claimstate.id_stg 

			where	cctl_claimstate.name_stg <> ''Draft'') cc_claim 

		left join db_t_prod_stag.cc_exposure 

			on cc_claim.id_stg=cc_exposure.ClaimID_stg

		left join db_t_prod_stag.cc_incident 

			on cc_exposure.incidentID_stg = cc_incident.id_stg

		left join db_t_prod_stag.cc_coverage 

			on cc_coverage.ID_stg=cc_exposure.CoverageID_stg

		inner join db_t_prod_stag.cc_riskunit 

			on cc_riskunit.ID_stg=cc_coverage.RiskUnitID_stg

		left join db_t_prod_stag.cctl_incident 

			on cc_incident.subtype_stg = cctl_incident.id_stg

		left join db_t_prod_stag.cc_address 

			on cc_claim.LossLocationID_stg = cc_address.ID_stg

		where	cctl_incident.name_stg in (''PropertyContentsIncident'',

				''LivingExpensesIncident'')

) otr /*  Added outer query as part of EIM- 20343, EIM-18973 */
		WHERE	RNK=1 

/*  ------------------------------------ */
	) PropertyContents 

		on PropertyContents.id_stg=cc_incident.id_stg

) e 

	on a.IncidentID_stg = e.incid

/*  Avinash */
left outer join 

(

	select

	distinct

	claimnumber_stg,

			cc_exposure.id_stg,

regexp_substr(cc_coverage.PolicySystemId_stg,''[^.:]+'',1,2) as CoverageSubtype, /*  INSRNC_CVGE_TYPE_CD */
	cctl_coveragetype.typecode_stg as clausename,

/* COMN_FEAT_NAME */
''COV'' as clausetype, /* FEAT_INSRNC_SBTYPE_CD */
	''CL'' as feat_sbtype_cd

	from	(

		select	cc_claim.* 

		from	db_t_prod_stag.cc_claim 

		inner join db_t_prod_stag.cctl_claimstate 

			on cc_claim.State_stg= cctl_claimstate.id_stg 

		where	cctl_claimstate.name_stg <> ''Draft'') cc_claim  

	join db_t_prod_stag.cc_exposure 

		on cc_claim.id_stg=cc_exposure.ClaimID_stg

	join db_t_prod_stag.cc_coverage 

		on cc_exposure.CoverageID_stg=cc_coverage.ID_stg

	join db_t_prod_stag.cctl_coveragetype 

		on cc_coverage.Type_stg=cctl_coveragetype.ID_stg

	join db_t_prod_stag.cctl_lobcode 

		on cc_Claim.LOBCode_stg=cctl_lobcode.ID_stg

) f 

	on f.id_stg=a.id_stg

WHERE	 (a.UpdateTime_stg > (:start_dttm)    

	and a.UpdateTime_stg <= (:end_dttm)) 

	OR

(cc_evaluation.UpdateTime_stg > (:start_dttm) 

	and  cc_evaluation.UpdateTime_stg <= (:end_dttm))

	

)



/* ---------------------------------------------- */




select 

distinct rank() over (partition by ab.publicid order by ab.dt) rk,

ab.publicid as clm_expsr_nk,

ab.dt,

ab.status as statuscd,

ab.reason,

ab.Trans_Strt_dttm

from 

(



/* PART1 */
select  DISTINCT PublicID, createtime as dt,  cast(''CLM_EXPSR_STS_TYPE1''  as varchar(50) ) as status,  cast ('''' as varchar (200) ) as reason,

ccexposure.createtime as Trans_Strt_dttm

from ccexposure

/* where closedate is  null and ReOpenDate is  null  */
UNION 

select distinct ccexposure.PublicID, ccexposure.CloseDate as dt, cast(''CLM_EXPSR_STS_TYPE2'' as varchar(50)) as status,cast(cctlexp.TYPECODE_stg as varchar(200)) as reason,

ccexposure.updatetime as Trans_Strt_dttm



from

ccexposure



left outer join db_t_prod_stag.cctl_exposureclosedoutcometype  cctlexp on ccexposure.ClosedOutcome=cctlexp.ID_stg 

where closedate is not null and ReOpenDate is  null





UNION

select distinct ccexposure.PublicID,ccexposure.ReOpenDate as dt,cast(''CLM_EXPSR_STS_TYPE3'' as varchar(50)) as status, cast(cctlexpreopnrsn.TYPECODE_stg as varchar(200)) as reason,

ccexposure.updatetime as Trans_Strt_dttm

from 

ccexposure

left outer join db_t_prod_stag.cctl_exposurereopenedreason cctlexpreopnrsn on ccexposure.reopenedreason=cctlexpreopnrsn.ID_stg

where ReOpenDate is  not null  and closedate is null

) ab
) SRC
)
);


-- Component exp_all_sources, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_sources AS
(
SELECT
sq_exposure_status.clm_expsr_nk as clm_expsr_nk,
sq_exposure_status.dt as CreateTime,
sq_exposure_status.sts_cd as status,
sq_exposure_status.Rank as Rank,
sq_exposure_status.Trans_Strt_dttm as Trans_Strt_dttm,
sq_exposure_status.source_record_id
FROM
sq_exposure_status
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
exp_all_sources.clm_expsr_nk as clm_expsr_nk,
LTRIM ( RTRIM ( exp_all_sources.status ) ) as var_clm_expsr_sts_cd,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_EXPSR_STS_CD */ as var_lkp_expsr_sts_cd,
CASE WHEN TRIM(var_clm_expsr_sts_cd) = '''' OR var_clm_expsr_sts_cd IS NULL OR LENGTH ( var_clm_expsr_sts_cd ) = 0 OR var_lkp_expsr_sts_cd IS NULL THEN ''UNK'' ELSE var_lkp_expsr_sts_cd END as out_clm_expsr_sts_cd,
exp_all_sources.CreateTime as CLM_EXPSR_STS_DT,
exp_all_sources.Rank as Rank,
exp_all_sources.source_record_id,
row_number() over (partition by exp_all_sources.source_record_id order by exp_all_sources.source_record_id) as RNK
FROM
exp_all_sources
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CLM_EXPSR_STS_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = var_clm_expsr_sts_cd
QUALIFY row_number() over (partition by exp_all_sources.source_record_id order by exp_all_sources.source_record_id) = 1
);


-- Component LKP_CLM_EXPSR_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM_EXPSR_ID AS
(
SELECT
LKP.CLM_EXPSR_ID,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.CLM_EXPSR_ID desc,LKP.CLMNT_PRTY_ID desc,LKP.CLM_EXPSR_NAME desc,LKP.CLM_EXPSR_RPTD_DTTM desc,LKP.CLM_EXPSR_OTH_CARIER_CVGE_IND desc,LKP.CLM_ID desc,LKP.CVGE_FEAT_ID desc,LKP.INSRBL_INT_ID desc,LKP.PRCS_ID desc,LKP.COTTER_CLM_IND desc,LKP.LOSS_PRTY_TYPE_CD desc,LKP.NK_SRC_KEY desc,LKP.HOLDBACK_IND desc,LKP.HOLDBACK_AMT desc,LKP.HOLDBACK_REIMBURSED_IND desc,LKP.ROOF_RPLACEMT_IND desc,LKP.CLM_EXPSR_TYPE_CD desc,LKP.CLM_EXPSR_STRT_DTTM desc,LKP.CLM_EXPSR_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK
FROM
exp_data_transformation
LEFT JOIN (
SELECT CLM_EXPSR.CLM_EXPSR_ID as CLM_EXPSR_ID, CLM_EXPSR.CLMNT_PRTY_ID as CLMNT_PRTY_ID, CLM_EXPSR.CLM_EXPSR_NAME as CLM_EXPSR_NAME, CLM_EXPSR.CLM_EXPSR_RPTD_DTTM as CLM_EXPSR_RPTD_DTTM, CLM_EXPSR.CLM_EXPSR_OTH_CARIER_CVGE_IND as CLM_EXPSR_OTH_CARIER_CVGE_IND, CLM_EXPSR.CLM_ID as CLM_ID, CLM_EXPSR.CVGE_FEAT_ID as CVGE_FEAT_ID, CLM_EXPSR.INSRBL_INT_ID as INSRBL_INT_ID, CLM_EXPSR.PRCS_ID as PRCS_ID, CLM_EXPSR.COTTER_CLM_IND as COTTER_CLM_IND, CLM_EXPSR.LOSS_PRTY_TYPE_CD as LOSS_PRTY_TYPE_CD, CLM_EXPSR.HOLDBACK_IND as HOLDBACK_IND , CLM_EXPSR.HOLDBACK_AMT as HOLDBACK_AMT, CLM_EXPSR.HOLDBACK_REIMBURSED_IND as HOLDBACK_REIMBURSED_IND, CLM_EXPSR.ROOF_RPLACEMT_IND as ROOF_RPLACEMT_IND, CLM_EXPSR.CLM_EXPSR_TYPE_CD AS CLM_EXPSR_TYPE_CD,CLM_EXPSR.CLM_EXPSR_STRT_DTTM as CLM_EXPSR_STRT_DTTM, CLM_EXPSR.CLM_EXPSR_END_DTTM as CLM_EXPSR_END_DTTM, CLM_EXPSR.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM_EXPSR.EDW_END_DTTM as EDW_END_DTTM, CLM_EXPSR.NK_SRC_KEY as NK_SRC_KEY FROM db_t_prod_core.CLM_EXPSR 
QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM_EXPSR.NK_SRC_KEY  ORDER BY CLM_EXPSR.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_SRC_KEY = exp_data_transformation.clm_expsr_nk
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.CLM_EXPSR_ID desc,LKP.CLMNT_PRTY_ID desc,LKP.CLM_EXPSR_NAME desc,LKP.CLM_EXPSR_RPTD_DTTM desc,LKP.CLM_EXPSR_OTH_CARIER_CVGE_IND desc,LKP.CLM_ID desc,LKP.CVGE_FEAT_ID desc,LKP.INSRBL_INT_ID desc,LKP.PRCS_ID desc,LKP.COTTER_CLM_IND desc,LKP.LOSS_PRTY_TYPE_CD desc,LKP.NK_SRC_KEY desc,LKP.HOLDBACK_IND desc,LKP.HOLDBACK_AMT desc,LKP.HOLDBACK_REIMBURSED_IND desc,LKP.ROOF_RPLACEMT_IND desc,LKP.CLM_EXPSR_TYPE_CD desc,LKP.CLM_EXPSR_STRT_DTTM desc,LKP.CLM_EXPSR_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) = 1
);


-- Component exp_SrcFields, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_SrcFields AS
(
SELECT
LKP_CLM_EXPSR_ID.CLM_EXPSR_ID as in_CLM_EXPSR_ID,
exp_data_transformation.CLM_EXPSR_STS_DT as in_CLM_EXPSR_STS_DT,
exp_data_transformation.out_clm_expsr_sts_cd as in_CLM_EXPSR_STS_CD,
to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY'' ) as in_CLM_EXPSR_STS_END_DT,
:PRCS_ID as in_PRCS_ID,
CURRENT_TIMESTAMP as in_EDW_STRT_DTTM,
to_timestamp_ltz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as in_EDW_END_DTTM,
exp_data_transformation.Rank as Rank,
exp_all_sources.Trans_Strt_dttm as Trans_Strt_dttm,
exp_all_sources.source_record_id
FROM
exp_all_sources
INNER JOIN exp_data_transformation ON exp_all_sources.source_record_id = exp_data_transformation.source_record_id
INNER JOIN LKP_CLM_EXPSR_ID ON exp_data_transformation.source_record_id = LKP_CLM_EXPSR_ID.source_record_id
);


-- Component LKP_CLM_EXPSR_STS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM_EXPSR_STS AS
(
SELECT
LKP.CLM_EXPSR_ID,
LKP.CLM_EXPSR_STS_STRT_DTTM,
LKP.CLM_EXPSR_STS_CD,
exp_SrcFields.in_CLM_EXPSR_ID as in_CLM_EXPSR_ID,
exp_SrcFields.in_CLM_EXPSR_STS_CD as in_CLM_EXPSR_STS_CD,
exp_SrcFields.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_SrcFields.source_record_id ORDER BY LKP.CLM_EXPSR_ID asc,LKP.CLM_EXPSR_STS_STRT_DTTM asc,LKP.CLM_EXPSR_STS_CD asc) RNK
FROM
exp_SrcFields
LEFT JOIN (
SELECT	CLM_EXPSR_STS.CLM_EXPSR_STS_STRT_DTTM as CLM_EXPSR_STS_STRT_DTTM,
		CLM_EXPSR_STS.CLM_EXPSR_ID as CLM_EXPSR_ID,
		CLM_EXPSR_STS.CLM_EXPSR_STS_CD as CLM_EXPSR_STS_CD 
FROM	db_t_prod_core.CLM_EXPSR_STS
QUALIFY	ROW_NUMBER() OVER(
PARTITION BY CLM_EXPSR_ID  
ORDER BY EDW_END_DTTM DESC) = 1
) LKP ON LKP.CLM_EXPSR_ID = exp_SrcFields.in_CLM_EXPSR_ID
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_SrcFields.source_record_id ORDER BY LKP.CLM_EXPSR_ID asc,LKP.CLM_EXPSR_STS_STRT_DTTM asc,LKP.CLM_EXPSR_STS_CD asc) = 1
);


-- Component exp_CDC_Check, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CDC_Check AS
(
SELECT
exp_SrcFields.in_CLM_EXPSR_ID as in_CLM_EXPSR_ID,
exp_SrcFields.in_CLM_EXPSR_STS_DT as in_CLM_EXPSR_STS_STRT_DT,
exp_SrcFields.in_CLM_EXPSR_STS_CD as in_CLM_EXPSR_STS_CD,
exp_SrcFields.in_CLM_EXPSR_STS_END_DT as in_CLM_EXPSR_STS_END_DT,
exp_SrcFields.in_PRCS_ID as in_PRCS_ID,
exp_SrcFields.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_SrcFields.in_EDW_END_DTTM as in_EDW_END_DTTM,
LKP_CLM_EXPSR_STS.CLM_EXPSR_ID as lkp_CLM_EXPSR_ID,
LKP_CLM_EXPSR_STS.CLM_EXPSR_STS_STRT_DTTM as lkp_CLM_EXPSR_STS_STRT_DT,
LKP_CLM_EXPSR_STS.CLM_EXPSR_STS_CD as lkp_CLM_EXPSR_STS_CD,
NULL as lkp_EDW_STRT_DTTM,
md5 ( ltrim ( rtrim ( exp_SrcFields.in_CLM_EXPSR_STS_DT ) ) ) as v_SRC_MD5,
md5 ( ltrim ( rtrim ( LKP_CLM_EXPSR_STS.CLM_EXPSR_STS_STRT_DTTM ) ) ) as v_TGT_MD5,
CASE WHEN v_TGT_MD5 IS NULL THEN ''I'' ELSE CASE WHEN v_SRC_MD5 = v_TGT_MD5 THEN ''R'' ELSE ''U'' END END as o_CDC_Check,
exp_SrcFields.Rank as Rank,
DATEADD(''SECOND'', ( 2 * ( exp_SrcFields.Rank - 1 ) ), CURRENT_TIMESTAMP) as out_EDW_STRT_DTTM,
exp_SrcFields.Trans_Strt_dttm as Trans_Strt_dttm,
exp_SrcFields.source_record_id
FROM
exp_SrcFields
INNER JOIN LKP_CLM_EXPSR_STS ON exp_SrcFields.source_record_id = LKP_CLM_EXPSR_STS.source_record_id
);


-- Component rtr_claim_expsr_sts_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_claim_expsr_sts_INSERT AS
(	
SELECT
exp_CDC_Check.in_CLM_EXPSR_ID as in_CLM_EXPSR_ID,
exp_CDC_Check.in_CLM_EXPSR_STS_STRT_DT as in_CLM_EXPSR_STS_STRT_DT,
exp_CDC_Check.in_CLM_EXPSR_STS_CD as in_CLM_EXPSR_STS_CD,
exp_CDC_Check.in_CLM_EXPSR_STS_END_DT as in_CLM_EXPSR_STS_END_DT,
exp_CDC_Check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check.out_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_CDC_Check.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_CDC_Check.lkp_CLM_EXPSR_ID as lkp_CLM_EXPSR_ID,
exp_CDC_Check.lkp_CLM_EXPSR_STS_CD as lkp_CLM_EXPSR_STS_CD,
exp_CDC_Check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check.lkp_CLM_EXPSR_STS_STRT_DT as lkp_CLM_EXPSR_STS_STRT_DT,
exp_CDC_Check.o_CDC_Check as o_CDC_Check,
exp_CDC_Check.Trans_Strt_dttm as Trans_Strt_dttm,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE CASE WHEN exp_CDC_Check.lkp_CLM_EXPSR_ID IS NULL and exp_CDC_Check.in_CLM_EXPSR_ID IS NOT NULL THEN TRUE ELSE CASE WHEN ( exp_CDC_Check.lkp_CLM_EXPSR_ID IS NOT NULL AND exp_CDC_Check.in_CLM_EXPSR_ID IS NOT NULL and exp_CDC_Check.in_CLM_EXPSR_STS_CD <> exp_CDC_Check.lkp_CLM_EXPSR_STS_CD AND exp_CDC_Check.in_CLM_EXPSR_STS_STRT_DT > exp_CDC_Check.lkp_CLM_EXPSR_STS_STRT_DT ) THEN TRUE ELSE $3 END END
);



-- Component tgt_clm_expsr_sts_ins, Type TARGET 
INSERT INTO db_t_prod_core.CLM_EXPSR_STS
(
CLM_EXPSR_ID,
CLM_EXPSR_STS_STRT_DTTM,
CLM_EXPSR_STS_CD,
CLM_EXPSR_STS_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
rtr_claim_expsr_sts_INSERT.in_CLM_EXPSR_ID as CLM_EXPSR_ID,
rtr_claim_expsr_sts_INSERT.in_CLM_EXPSR_STS_STRT_DT as CLM_EXPSR_STS_STRT_DTTM,
rtr_claim_expsr_sts_INSERT.in_CLM_EXPSR_STS_CD as CLM_EXPSR_STS_CD,
rtr_claim_expsr_sts_INSERT.in_CLM_EXPSR_STS_END_DT as CLM_EXPSR_STS_END_DTTM,
rtr_claim_expsr_sts_INSERT.in_PRCS_ID as PRCS_ID,
rtr_claim_expsr_sts_INSERT.in_EDW_STRT_DTTM as EDW_STRT_DTTM,
rtr_claim_expsr_sts_INSERT.in_EDW_END_DTTM as EDW_END_DTTM,
rtr_claim_expsr_sts_INSERT.Trans_Strt_dttm as TRANS_STRT_DTTM
FROM
rtr_claim_expsr_sts_INSERT;


-- Component tgt_clm_expsr_sts_ins, Type Post SQL 
UPDATE  db_t_prod_core.CLM_EXPSR_STS  
set TRANS_END_DTTM =  A.lead, 
EDW_END_DTTM = A.lead1

FROM  

(

SELECT	distinct CLM_EXPSR_ID ,EDW_STRT_DTTM, CLM_EXPSR_STS_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by CLM_EXPSR_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''  as lead1, 

max(TRANS_STRT_DTTM) over (partition by CLM_EXPSR_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''  as lead

FROM db_t_prod_core.CLM_EXPSR_STS   

)  A


where CLM_EXPSR_STS.CLM_EXPSR_ID = A.CLM_EXPSR_ID 

and CLM_EXPSR_STS.EDW_STRT_DTTM = A.EDW_STRT_DTTM 

and CLM_EXPSR_STS.TRANS_STRT_DTTM <> CLM_EXPSR_STS.TRANS_END_DTTM

and A.lead is not null ;


END; ';