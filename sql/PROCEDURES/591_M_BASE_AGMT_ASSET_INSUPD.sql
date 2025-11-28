-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_ASSET_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    run_id STRING;
    workflow_name STRING;
    session_name STRING;
    start_dttm TIMESTAMP;
    end_dttm TIMESTAMP;
    in_out_router_flag STRING;
    PRCS_ID STRING;
    v_start_time TIMESTAMP;
BEGIN
    run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
    workflow_name := (SELECT workflow_name FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
    session_name := ''s_m_base_agmt_asset_insupd'';
    start_dttm := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);
    end_dttm := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
    in_out_router_flag := public.func_get_scoped_param(:run_id, ''in_out_router_flag'', :workflow_name, :worklet_name, :session_name);
    PRCS_ID := public.func_get_scoped_param(:run_id, ''PRCS_ID'', :workflow_name, :worklet_name, :session_name);
    v_start_time := CURRENT_TIMESTAMP();


-- Component LKP_PRTY_ASSET_ID, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_ASSET_ID AS
(
SELECT PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET.ASSET_INSRNC_HIST_TYPE_CD as ASSET_INSRNC_HIST_TYPE_CD, PRTY_ASSET.ASSET_DESC as ASSET_DESC, PRTY_ASSET.PRTY_ASSET_NAME as PRTY_ASSET_NAME, PRTY_ASSET.PRTY_ASSET_STRT_DTTM as PRTY_ASSET_STRT_DTTM, PRTY_ASSET.PRTY_ASSET_END_DTTM as PRTY_ASSET_END_DTTM, PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM, PRTY_ASSET.SRC_SYS_CD as SRC_SYS_CD, PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL, PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD, PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD FROM DB_T_PROD_CORE.PRTY_ASSET

QUALIFY ROW_NUMBER() OVER(PARTITION BY  ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,PRTY_ASSET_CLASFCN_CD ORDER BY EDW_END_DTTM DESC) = 1
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

   

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'', ''GW'') 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_CONTRACT_ROLE_SBTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_CONTRACT_ROLE_SBTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''ASSET_CNTRCT_ROLE_SBTYPE''

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'')

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


-- Component sq_pc_policy_asset_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_pc_policy_asset_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as policynumber,
$2 as partyassetid,
$3 as asset_start_dt,
$4 as asset_end_dt,
$5 as partyassettype,
$6 as agreementtype,
$7 as VehicleNumber,
$8 as nk_PublicID,
$9 as SYS_SRC_CD,
$10 as Retired,
$11 as Updatetime,
$12 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT	 DISTINCT a.policynumber,a. partyassetid, a.asset_start_dt,

		a. asset_end_dt,

                a.partyassettype, a.agreementtype,

		a.agmt_asset_ref_num, a.nk_publicid,a.SYS_SRC_CD,

a.Retired,

		DATE_TRUNC(DAY, a.updatetime) as updatetime

FROM(

	SELECT	

	distinct 

	''SRC_SYS4''as policynumber,

	pcx_Dwelling_HOE.fixedid_stg as partyassetid,

	pc_policyperiod.Editeffectivedate_stg as asset_start_dt,

	coalesce(pcx_Dwelling_HOE.ExpirationDate_stg, pc_policyperiod.PeriodEnd_stg) as asset_end_dt,

	cast(''PRTY_ASSET_SBTYPE5'' AS VARCHAR (60))  as partyassettype,

	cast(''PRTY_ASSET_CLASFCN1'' AS VARCHAR (60)) as agreementtype,

			

	cast(NULL  AS integer) as agmt_asset_ref_num,

	cast(pc_policyperiod.publicid_stg as varchar(64)) as nk_publicid,

			

	''SRC_SYS4'' as SYS_SRC_CD,

	pc_policyperiod.Retired_stg AS Retired,

	pc_policyperiod.Updatetime_stg as  Updatetime,

	(:start_dttm) as start_dttm,

	(:end_dttm) as end_dttm

	from	  DB_T_PROD_STAG.pc_PolicyPeriod 

	inner  join     DB_T_PROD_STAG.pcx_Dwelling_HOE 

	 on pc_PolicyPeriod.ID_stg=pcx_Dwelling_HOE.BranchID_stg

	 inner join   DB_T_PROD_STAG.pctl_policyperiodstatus 

		on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

	 inner join   DB_T_PROD_STAG.pc_job 

		on pc_job.id_stg=pc_policyperiod.JobID_stg

	inner join   DB_T_PROD_STAG.pctl_job 

		on pctl_job.id_stg=pc_job.Subtype_stg

	where	pctl_policyperiodstatus.TYPECODE_stg=''Bound'' 

		and pcx_Dwelling_HOE.fixedid_stg is not null 

		and 

	pc_policyperiod.UpdateTime_stg > (:start_dttm)

		and pc_policyperiod.UpdateTime_stg <= (:end_dttm) 

		and  pcx_Dwelling_HOE.ExpirationDate_stg is null

	    

	

	UNION

	/** Personal Property **/

	select	

	distinct 

	''SRC_SYS4''as policynumber,

	pcx_holineschedcovitem_alfa.FixedID_stg as partyassetid,

	pc_policyperiod.Editeffectivedate_stg as asset_start_dt,

	coalesce(pcx_holineschedcovitem_alfa.ExpirationDate_stg, pc_policyperiod.PeriodEnd_stg) as asset_end_dt,

			case 

				when pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

			''HOSI_SpecificOtherStructureExclItem_alfa'') then ''PRTY_ASSET_SBTYPE5'' 

				when pc_etlclausepattern.PatternID_stg=''HOSI_ScheduledPropertyItem_alfa'' then ''PRTY_ASSET_SBTYPE7''  /*''REALSP-PP''*/ 

			end as partyassettype,

	ChoiceTerm1_stg as agreementtype,

	cast(NULL  AS integer) as agmt_asset_ref_num,

	cast(pc_policyperiod.publicid_stg as varchar(64)) as nk_publicid ,

			

	''SRC_SYS4'' as SYS_SRC_CD,

	pc_policyperiod.Retired_stg AS Retired,

	pc_policyperiod.Updatetime_stg as  Updatetime,

	(:start_dttm) as start_dttm,

	        (:end_dttm) as end_dttm

	from	  DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa 

	inner join   DB_T_PROD_STAG.pc_etlclausepattern 

		on pc_etlclausepattern.PatternID_stg=pcx_holineschcovitemcov_alfa.PatternCode_stg

	inner join   DB_T_PROD_STAG.pcx_holineschedcovitem_alfa 

		on pcx_holineschedcovitem_alfa.ID_stg=pcx_holineschcovitemcov_alfa.HOLineSchCovItem_stg

	inner join   DB_T_PROD_STAG.pc_PolicyPeriod 

		on pc_policyperiod.id_stg=pcx_holineschcovitemcov_alfa.BranchID_stg

	inner join   DB_T_PROD_STAG.pctl_policyperiodstatus 

		on pc_policyperiod.Status_stg=pctl_policyperiodstatus.id_stg

	WHERE	pctl_policyperiodstatus.TYPECODE_stg=''Bound''

		AND 

	pc_etlclausepattern.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',

			''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'')

		and pc_policyperiod.UpdateTime_stg > (:start_dttm)

		and pc_policyperiod.UpdateTime_stg <= (:end_dttm)

		and pcx_holineschcovitemcov_alfa.ExpirationDate_stg is null

	

	/***Agreement and its related Asset Information for Vehicle***/

	UNION 

	SELECT	

	distinct 

	''SRC_SYS4''as policynumber,

	pc_personalvehicle.fixedid_stg as partyassetid,

	pc_policyperiod.Editeffectivedate_stg as asset_start_dt,

	coalesce(pc_personalvehicle.ExpirationDate_stg, pc_policyperiod.PeriodEnd_stg) as asset_end_dt,

	''PRTY_ASSET_SBTYPE4'' as partyassettype,

	''PRTY_ASSET_CLASFCN3'' as agreementtype,

	cast(pc_personalvehicle.VehicleNumber_stg AS integer) as agmt_asset_ref_num,

	cast(pc_policyperiod.publicid_stg as varchar(64)) as nk_publicid,

			

	 ''SRC_SYS4'' as SYS_SRC_CD,

	pc_policyperiod.Retired_stg AS Retired,

	pc_policyperiod.Updatetime_stg as  Updatetime,

	(:start_dttm) as start_dttm,

	        (:end_dttm) as end_dttm

	from	  DB_T_PROD_STAG.pc_PolicyPeriod 

	join     DB_T_PROD_STAG.pc_personalvehicle  

		on pc_PolicyPeriod.ID_stg=pc_personalvehicle.BranchID_stg

	 inner join   DB_T_PROD_STAG.pctl_policyperiodstatus 

		on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

	 inner join   DB_T_PROD_STAG.pc_job 

		on pc_job.id_stg=pc_policyperiod.JobID_stg

	inner join   DB_T_PROD_STAG.pctl_job 

		on pctl_job.id_stg=pc_job.Subtype_stg

	where	pctl_policyperiodstatus.TYPECODE_stg=''Bound'' 

		and pc_personalvehicle.fixedid_stg is not null

		and pc_policyperiod.UpdateTime_stg > (:start_dttm)

	    and pc_policyperiod.UpdateTime_stg <= (:end_dttm)

		and (pc_personalvehicle.ExpirationDate_stg is NULL 

		OR pc_personalvehicle.ExpirationDate_stg > pc_policyperiod.EditEffectiveDate_stg)

	qualify	ROW_NUMBER() OVER(

	PARTITION BY pc_PolicyPeriod.ID_stg,pc_personalvehicle.fixedid_stg

	ORDER BY coalesce(pc_personalvehicle.ExpirationDate_stg,

			cast(''9999-12-31 23:59:59.999999'' as timestamp)) desc)=1

	

	/***Agreement and its related Asset Information for DB_T_CORE_DM_PROD.Coverage for watercraft motor***/

	UNION

	SELECT	

	distinct 

	''SRC_SYS4''as policynumber,

	pcx_pawatercraftmotor_alfa.fixedid_stg as  partyassetid,

	pc_policyperiod.Editeffectivedate_stg as asset_start_dt,

	coalesce(pcx_pawatercraftmotor_alfa.ExpirationDate_stg, pc_policyperiod.PeriodEnd_stg) as asset_end_dt,

	''PRTY_ASSET_SBTYPE4'' partyassettype,

	''PRTY_ASSET_CLASFCN4'' as agreementtype,

	cast(NULL  AS integer) as agmt_asset_ref_num,

	cast(pc_policyperiod.publicid_stg as varchar(64)) as nk_publicid,

	''SRC_SYS4'' as SYS_SRC_CD,

	pc_policyperiod.Retired_stg AS Retired,

	pc_policyperiod.Updatetime_stg  as  Updatetime,

	(:start_dttm) as start_dttm,

	        (:end_dttm) as end_dttm

	from	  DB_T_PROD_STAG.pc_PolicyPeriod join     DB_T_PROD_STAG.pcx_pawatercraftmotor_alfa  

		on pc_PolicyPeriod.ID_stg=pcx_pawatercraftmotor_alfa.BranchID_stg

	 inner join   DB_T_PROD_STAG.pctl_policyperiodstatus 

		on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

	 inner join   DB_T_PROD_STAG.pc_job 

		on pc_job.id_stg=pc_policyperiod.JobID_stg

	inner join   DB_T_PROD_STAG.pctl_job 

		on pctl_job.id_stg=pc_job.Subtype_stg

	where	pctl_policyperiodstatus.TYPECODE_stg=''Bound''  

		and pcx_pawatercraftmotor_alfa.fixedid_stg is not null

		and pc_policyperiod.UpdateTime_stg > (:start_dttm)

		and pc_policyperiod.UpdateTime_stg <= (:end_dttm)

		and pcx_pawatercraftmotor_alfa.ExpirationDate_stg is null



	

	/***Agreement and its related Asset Information for DB_T_CORE_DM_PROD.Coverage for watercraft trailer***/

	UNION

	SELECT	

	distinct 

	''SRC_SYS4''as policynumber,

	pcx_pawatercrafttrailer_alfa.fixedid_stg as partyassetid,

	pc_policyperiod.Editeffectivedate_stg as asset_start_dt,

	coalesce(pcx_pawatercrafttrailer_alfa.ExpirationDate_stg, pc_policyperiod.PeriodEnd_stg) as asset_end_dt,

	''PRTY_ASSET_SBTYPE4'' partyassettype,

	''PRTY_ASSET_CLASFCN5'' as agreementtype,

	cast(NULL  AS integer) as agmt_asset_ref_num,

	cast(pc_policyperiod.publicid_stg as varchar(64)) as nk_publicid,

	''SRC_SYS4'' as SYS_SRC_CD,

	pc_policyperiod.Retired_stg AS Retired,

	pc_policyperiod.Updatetime_stg as  Updatetime,

	(:start_dttm) as start_dttm,

	        (:end_dttm) as end_dttm

	from	  DB_T_PROD_STAG.pc_PolicyPeriod join     DB_T_PROD_STAG.pcx_pawatercrafttrailer_alfa  

		on pc_PolicyPeriod.ID_stg=pcx_pawatercrafttrailer_alfa.BranchID_stg

	 inner join   DB_T_PROD_STAG.pctl_policyperiodstatus 

		on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

	 inner join   DB_T_PROD_STAG.pc_job 

		on pc_job.id_stg=pc_policyperiod.JobID_stg

	inner join   DB_T_PROD_STAG.pctl_job 

		on pctl_job.id_stg=pc_job.Subtype_stg

	where	pctl_policyperiodstatus.TYPECODE_stg=''Bound'' 

		and pcx_pawatercrafttrailer_alfa.fixedid_stg is not null

		and pc_policyperiod.UpdateTime_stg > (:start_dttm)

		and pc_policyperiod.UpdateTime_stg <= (:end_dttm)

		and pcx_pawatercrafttrailer_alfa.ExpirationDate_stg is null



	

	UNION

	

	/* BP7 */

	select	distinct 

	''SRC_SYS4''as policynumber,

	c.FixedID_stg as partyassetid,

	pp.EditEffectiveDate_stg as asset_start_dt,

	coalesce(c.ExpirationDate_stg, pp.PeriodEnd_stg) as asset_end_dt,

	''PRTY_ASSET_SBTYPE13'' as partyassettype,

	cp.TYPECODE_stg as agreementtype,

	cast(NULL  AS integer) as agmt_asset_ref_num,

	cast(pp.PublicID_stg as varchar(64)) as  nk_publicid,

	''SRC_SYS4'' as SYS_SRC_CD,

	pp.Retired_stg as Retired,

	pp.UpdateTime_stg as  Updatetime,

	(:start_dttm) as start_dttm,

	        (:end_dttm) as end_dttm

	from	  DB_T_PROD_STAG.pc_PolicyPeriod pp

	inner join   DB_T_PROD_STAG.pcx_bp7classification c 

		on pp.ID_stg = c.BranchID_stg

	inner join   DB_T_PROD_STAG.pctl_bp7classificationproperty cp 

		on c.bp7classpropertytype_stg = cp.ID_stg

	inner join   DB_T_PROD_STAG.pctl_policyperiodstatus pps 

		on pp.Status_stg = pps.ID_stg

	inner join   DB_T_PROD_STAG.pc_job j 

		on pp.JobID_stg = j.ID_stg

	inner join   DB_T_PROD_STAG.pctl_job jt 

		on j.Subtype_stg = jt.ID_stg

	where	pps.TYPECODE_stg = ''Bound''

		and c.FixedID_stg is not null

		and pp.UpdateTime_stg > (:start_dttm)

		and pp.UpdateTime_stg <= (:end_dttm)

		and c.ExpirationDate_stg is null

	

	/*Bring in Building as asset */

	

	union

	

	select	distinct 

	''SRC_SYS4''as policynumber,

	pb.FixedID_stg as partyassetid,

	pp.EditEffectiveDate_stg as asset_start_dt,

	coalesce(pb.ExpirationDate_stg, pp.PeriodEnd_stg) as asset_end_dt,

	''PRTY_ASSET_SBTYPE32'' as partyassettype,

	''PRTY_ASSET_CLASFCN10''  as agreementtype,

	cast(NULL  AS integer) as agmt_asset_ref_num,

	cast(pp.PublicID_stg as varchar(64)) as  nk_publicid,

	''SRC_SYS4'' as SYS_SRC_CD,

	pp.Retired_stg as Retired,

	pp.UpdateTime_stg as  Updatetime,

	(:start_dttm) as start_dttm,

	(:end_dttm) as end_dttm

	from	  DB_T_PROD_STAG.pc_PolicyPeriod pp

	inner join DB_T_PROD_STAG.pcx_bp7building pb 

		on pp.id_stg  = pb.BranchID_stg 

	inner join   DB_T_PROD_STAG.pctl_policyperiodstatus pps 

		on pp.Status_stg = pps.ID_stg

	where	pps.TYPECODE_stg = ''Bound''

		and pb.FixedID_stg is not null

		and pp.UpdateTime_stg > (:start_dttm)

		and pp.UpdateTime_stg <= (:end_dttm)

		and pb.ExpirationDate_stg is null

	

UNION





/* -------- EIM-48784 FARM CHANGES------------------------------------------- */
/* FOP DWELLING */


select distinct ''SRC_SYS4''as policynumber,pcx_fopdwelling.FixedID_stg as partyassetid,pc_policyperiod.EditEffectiveDate_stg as asset_start_dt,

coalesce(pcx_fopdwelling.ExpirationDate_stg, pc_policyperiod.PeriodEnd_stg) as asset_end_dt,

''PRTY_ASSET_SBTYPE37'' as assettype ,''PRTY_ASSET_CLASFCN15'' as agreementtype,cast(NULL  AS integer) as agmt_asset_ref_num,

cast(pc_policyperiod.PublicID_stg as varchar(64)) as  nk_publicid,''SRC_SYS4'' as SYS_SRC_CD,

pc_policyperiod.Retired_stg as Retired,

case when pcx_fopdwelling.Updatetime_stg>pc_policyperiod.updatetime_stg then pcx_fopdwelling.Updatetime_stg else pc_policyperiod.updatetime_stg end as  Updatetime,

(:start_dttm) as start_dttm,(:end_dttm) as end_dttm

from   DB_T_PROD_STAG.pc_PolicyPeriod

inner join DB_T_PROD_STAG.pcx_fopdwelling on pc_PolicyPeriod.ID_stg=pcx_fopdwelling.BranchID_stg

inner join   DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

inner join   DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join   DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

where pctl_policyperiodstatus.TYPECODE_stg=''Bound''

and pcx_fopdwelling.fixedid_stg is not null 

and (pcx_fopdwelling.UpdateTime_stg >(:start_dttm) AND  pcx_fopdwelling.UpdateTime_stg <= (:end_dttm)

or (pc_policyperiod.updatetime_stg>(:start_dttm)and pc_policyperiod.updatetime_stg<=(:end_dttm)))

and (pcx_fopdwelling.ExpirationDate_stg is null or pcx_fopdwelling.Expirationdate_stg>pc_policyperiod.modeldate_stg) 

QUALIFY	RANK() OVER(PARTITION BY nk_publicid,partyassetid,assettype 

order by coalesce(pcx_fopdwelling.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp))desc,

pcx_fopdwelling.updatetime_stg desc,pcx_fopdwelling.createtime_stg desc)=1



UNION



/* FOP OUTBUILDING */
select distinct ''SRC_SYS4''as policynumber,pcx_fopoutbuilding.FixedID_stg as partyassetid,pc_policyperiod.EditEffectiveDate_stg as asset_start_dt,

coalesce(pcx_fopoutbuilding.ExpirationDate_stg, pc_policyperiod.PeriodEnd_stg) as asset_end_dt,

cast(''PRTY_ASSET_SBTYPE36'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN13'' as varchar(50)) as agreementtype,cast(NULL  AS integer) as agmt_asset_ref_num,

cast(pc_policyperiod.PublicID_stg as varchar(64)) as  nk_publicid,''SRC_SYS4'' as SYS_SRC_CD,

pc_policyperiod.Retired_stg as Retired,

case when pcx_fopoutbuilding.Updatetime_stg>pc_policyperiod.updatetime_stg then pcx_fopoutbuilding.Updatetime_stg else pc_policyperiod.updatetime_stg end as  Updatetime,

(:start_dttm) as start_dttm,(:end_dttm) as end_dttm

from   DB_T_PROD_STAG.pc_PolicyPeriod

inner join DB_T_PROD_STAG.pcx_fopoutbuilding on pc_PolicyPeriod.ID_stg=pcx_fopoutbuilding.BranchID_stg

inner join   DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

inner join   DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join   DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

where pctl_policyperiodstatus.TYPECODE_stg=''Bound''

and pcx_fopoutbuilding.fixedid_stg is not null 

and (pcx_fopoutbuilding.UpdateTime_stg >(:start_dttm)and pcx_fopoutbuilding.UpdateTime_stg <= (:end_dttm)

or (pc_policyperiod.updatetime_stg>(:start_dttm)and pc_policyperiod.updatetime_stg<=(:end_dttm)))

and (pcx_fopoutbuilding.ExpirationDate_stg is null or pcx_fopoutbuilding.Expirationdate_stg>pc_policyperiod.modeldate_stg) 

QUALIFY	RANK() OVER(PARTITION BY nk_publicid,partyassetid,assettype

order by coalesce(pcx_fopoutbuilding.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp))desc,

pcx_fopoutbuilding.updatetime_stg desc,pcx_fopoutbuilding.createtime_stg desc)=1



UNION



/* FOP FEEDANDSEED */
select distinct ''SRC_SYS4''as policynumber,pcx_fopfeedandseed.FixedID_stg as partyassetid,pc_policyperiod.EditEffectiveDate_stg as asset_start_dt,

coalesce(pcx_fopfeedandseed.ExpirationDate_stg, pc_policyperiod.PeriodEnd_stg) as asset_end_dt,

cast(''PRTY_ASSET_SBTYPE33'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN11'' as varchar(50)) as agreementtype,cast(NULL  AS integer) as agmt_asset_ref_num,

cast(pc_policyperiod.PublicID_stg as varchar(64)) as  nk_publicid,''SRC_SYS4'' as SYS_SRC_CD,

pc_policyperiod.Retired_stg as Retired,

case when pcx_fopfeedandseed.Updatetime_stg>pc_policyperiod.updatetime_stg then pcx_fopfeedandseed.Updatetime_stg else pc_policyperiod.updatetime_stg end as  Updatetime,

(:start_dttm) as start_dttm,(:end_dttm) as end_dttm

from   DB_T_PROD_STAG.pc_PolicyPeriod

inner join DB_T_PROD_STAG.pcx_fopfeedandseed on pc_PolicyPeriod.ID_stg=pcx_fopfeedandseed.BranchID_stg

inner join   DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

inner join   DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join   DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

where pctl_policyperiodstatus.TYPECODE_stg=''Bound''

and pcx_fopfeedandseed.fixedid_stg is not null 

and (pcx_fopfeedandseed.UpdateTime_stg >(:start_dttm) and pcx_fopfeedandseed.UpdateTime_stg <= (:end_dttm)

or (pc_policyperiod.updatetime_stg>(:start_dttm)and pc_policyperiod.updatetime_stg<=(:end_dttm)))

and (pcx_fopfeedandseed.ExpirationDate_stg is null or pcx_fopfeedandseed.Expirationdate_stg>pc_policyperiod.modeldate_stg)

QUALIFY	RANK() OVER(PARTITION BY nk_publicid,partyassetid,assettype

order by coalesce(pcx_fopfeedandseed.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp))desc,

pcx_fopfeedandseed.updatetime_stg desc,pcx_fopfeedandseed.createtime_stg desc)=1



UNION



/* FOP LIVESTOCK */
select distinct ''SRC_SYS4''as policynumber,pcx_foplivestock.FixedID_stg as partyassetid,pc_policyperiod.EditEffectiveDate_stg as asset_start_dt,

coalesce(pcx_foplivestock.ExpirationDate_stg, pc_policyperiod.PeriodEnd_stg) as asset_end_dt,

cast(''PRTY_ASSET_SBTYPE35'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN14'' as varchar(50)) as agreementtype,cast(NULL  AS integer) as agmt_asset_ref_num,

cast(pc_policyperiod.PublicID_stg as varchar(64)) as  nk_publicid,''SRC_SYS4'' as SYS_SRC_CD,

pc_policyperiod.Retired_stg as Retired,

case when pcx_foplivestock.Updatetime_stg>pc_policyperiod.updatetime_stg then pcx_foplivestock.Updatetime_stg else pc_policyperiod.updatetime_stg end as  Updatetime,

(:start_dttm) as start_dttm,(:end_dttm) as end_dttm

from   DB_T_PROD_STAG.pc_PolicyPeriod

inner join DB_T_PROD_STAG.pcx_foplivestock on pc_PolicyPeriod.ID_stg=pcx_foplivestock.BranchID_stg

inner join   DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

inner join   DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join   DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

where pctl_policyperiodstatus.TYPECODE_stg=''Bound''

and pcx_foplivestock.fixedid_stg is not null 

and ( pcx_foplivestock.UpdateTime_stg >(:start_dttm)and pcx_foplivestock.UpdateTime_stg <= (:end_dttm)

or (pc_policyperiod.updatetime_stg>(:start_dttm)and pc_policyperiod.updatetime_stg<=(:end_dttm)))

and (pcx_foplivestock.ExpirationDate_stg is null or pcx_foplivestock.Expirationdate_stg>pc_policyperiod.modeldate_stg) 

QUALIFY	RANK() OVER(PARTITION BY nk_publicid,partyassetid,assettype

order by coalesce(pcx_foplivestock.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp))desc,

pcx_foplivestock.updatetime_stg desc,pcx_foplivestock.createtime_stg desc)=1



UNION



/* FOP MACHINERY */
select distinct ''SRC_SYS4''as policynumber,pcx_fopmachinery.FixedID_stg as partyassetid,pc_policyperiod.EditEffectiveDate_stg as asset_start_dt,

coalesce(pcx_fopmachinery.ExpirationDate_stg, pc_policyperiod.PeriodEnd_stg) as asset_end_dt,

cast(''PRTY_ASSET_SBTYPE34'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN12'' as varchar(50)) as agreementtype,cast(NULL  AS integer) as agmt_asset_ref_num,

cast(pc_policyperiod.PublicID_stg as varchar(64)) as  nk_publicid,''SRC_SYS4'' as SYS_SRC_CD,

pc_policyperiod.Retired_stg as Retired,

case when pcx_fopmachinery.Updatetime_stg>pc_policyperiod.updatetime_stg then pcx_fopmachinery.Updatetime_stg else pc_policyperiod.updatetime_stg end as  Updatetime,

(:start_dttm) as start_dttm,(:end_dttm) as end_dttm

from   DB_T_PROD_STAG.pc_PolicyPeriod

inner join DB_T_PROD_STAG.pcx_fopmachinery on pc_PolicyPeriod.ID_stg=pcx_fopmachinery.BranchID_stg

inner join   DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

inner join   DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join   DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

where pctl_policyperiodstatus.TYPECODE_stg=''Bound''

and pcx_fopmachinery.fixedid_stg is not null 

and ( pcx_fopmachinery.UpdateTime_stg >(:start_dttm) and pcx_fopmachinery.UpdateTime_stg <= (:end_dttm)

or (pc_policyperiod.updatetime_stg>(:start_dttm)and pc_policyperiod.updatetime_stg<=(:end_dttm)))

and (pcx_fopmachinery.ExpirationDate_stg is null or pcx_fopmachinery.Expirationdate_stg>pc_policyperiod.modeldate_stg) 

QUALIFY	RANK() OVER(PARTITION BY nk_publicid,partyassetid,assettype

order by coalesce(pcx_fopmachinery.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp))desc,

pcx_fopmachinery.updatetime_stg desc,pcx_fopmachinery.createtime_stg desc)=1



UNION



/* FOP DWELLINGSCHDCOVITEM */
select distinct ''SRC_SYS4''as policynumber,pcx_fopdwellingschdcovitem.FixedID_stg as partyassetid,pc_policyperiod.EditEffectiveDate_stg as asset_start_dt,

coalesce(pcx_fopdwellingschdcovitem.ExpirationDate_stg, pc_policyperiod.PeriodEnd_stg) as asset_end_dt,

cast(''PRTY_ASSET_SBTYPE38'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN16'' as varchar(50)) as agreementtype,cast(NULL  AS integer) as agmt_asset_ref_num,

cast(pc_policyperiod.PublicID_stg as varchar(64)) as  nk_publicid,''SRC_SYS4'' as SYS_SRC_CD,

pc_policyperiod.Retired_stg as Retired,

case when pcx_fopdwellingschdcovitem.Updatetime_stg>pc_policyperiod.updatetime_stg then pcx_fopdwellingschdcovitem.Updatetime_stg else pc_policyperiod.updatetime_stg end as  Updatetime,

(:start_dttm) as start_dttm,(:end_dttm) as end_dttm

from   DB_T_PROD_STAG.pc_PolicyPeriod

inner join DB_T_PROD_STAG.pcx_fopdwellingschdcovitem on pc_PolicyPeriod.ID_stg=pcx_fopdwellingschdcovitem.BranchID_stg

inner join   DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

inner join   DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join   DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

where pctl_policyperiodstatus.TYPECODE_stg=''Bound''

and pcx_fopdwellingschdcovitem.fixedid_stg is not null 

and (pcx_fopdwellingschdcovitem.UpdateTime_stg >(:start_dttm) and pcx_fopdwellingschdcovitem.UpdateTime_stg <= (:end_dttm)

or (pc_policyperiod.updatetime_stg>(:start_dttm)and pc_policyperiod.updatetime_stg<=(:end_dttm)))

and (pcx_fopdwellingschdcovitem.ExpirationDate_stg is null or pcx_fopdwellingschdcovitem.Expirationdate_stg>pc_policyperiod.modeldate_stg) 

QUALIFY	RANK() OVER(PARTITION BY nk_publicid,partyassetid,assettype

order by coalesce(pcx_fopdwellingschdcovitem.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp))desc,

pcx_fopdwellingschdcovitem.updatetime_stg desc,pcx_fopdwellingschdcovitem.createtime_stg desc)=1



UNION



/* FOP DWELLINGSCHDEXCLITEM */
select distinct ''SRC_SYS4''as policynumber,pcx_fopdwellingschdexclitem.FixedID_stg as partyassetid,pc_policyperiod.EditEffectiveDate_stg as asset_start_dt,

coalesce(pcx_fopdwellingschdexclitem.ExpirationDate_stg, pc_policyperiod.PeriodEnd_stg) as asset_end_dt,

cast(''PRTY_ASSET_SBTYPE40'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN18'' as varchar(50)) as agreementtype,cast(NULL  AS integer) as agmt_asset_ref_num,

cast(pc_policyperiod.PublicID_stg as varchar(64)) as  nk_publicid,''SRC_SYS4'' as SYS_SRC_CD,

pc_policyperiod.Retired_stg as Retired,

case when pcx_fopdwellingschdexclitem.Updatetime_stg>pc_policyperiod.updatetime_stg then pcx_fopdwellingschdexclitem.Updatetime_stg else pc_policyperiod.updatetime_stg end as  Updatetime,

(:start_dttm) as start_dttm,(:end_dttm) as end_dttm

from   DB_T_PROD_STAG.pc_PolicyPeriod

inner join DB_T_PROD_STAG.pcx_fopdwellingschdexclitem on pc_PolicyPeriod.ID_stg=pcx_fopdwellingschdexclitem.BranchID_stg

inner join   DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

inner join   DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join   DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

where pctl_policyperiodstatus.TYPECODE_stg=''Bound''

and pcx_fopdwellingschdexclitem.fixedid_stg is not null 

and (pcx_fopdwellingschdexclitem.UpdateTime_stg >(:start_dttm)and pcx_fopdwellingschdexclitem.UpdateTime_stg <= (:end_dttm)

or (pc_policyperiod.updatetime_stg>(:start_dttm)and pc_policyperiod.updatetime_stg<=(:end_dttm)))

and (pcx_fopdwellingschdexclitem.ExpirationDate_stg is null or pcx_fopdwellingschdexclitem.Expirationdate_stg>pc_policyperiod.modeldate_stg) 

QUALIFY	RANK() OVER(PARTITION BY nk_publicid,partyassetid,assettype

order by coalesce(pcx_fopdwellingschdexclitem.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp))desc,

pcx_fopdwellingschdexclitem.updatetime_stg desc,pcx_fopdwellingschdexclitem.createtime_stg desc)=1



/* FOP FARMOWNERSLISCHDCOVITEM */


UNION



select distinct ''SRC_SYS4''as policynumber,pcx_fopfarmownerslischdcovitem.FixedID_stg as partyassetid,pc_policyperiod.EditEffectiveDate_stg as asset_start_dt,

coalesce(pcx_fopfarmownerslischdcovitem.ExpirationDate_stg, pc_policyperiod.PeriodEnd_stg) as asset_end_dt,

cast(''PRTY_ASSET_SBTYPE41'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN19'' as varchar(50)) as agreementtype,cast(NULL  AS integer) as agmt_asset_ref_num,

cast(pc_policyperiod.PublicID_stg as varchar(64)) as  nk_publicid,''SRC_SYS4'' as SYS_SRC_CD,

pc_policyperiod.Retired_stg as Retired,

case when pcx_fopfarmownerslischdcovitem.Updatetime_stg>pc_policyperiod.updatetime_stg then pcx_fopfarmownerslischdcovitem.Updatetime_stg else pc_policyperiod.updatetime_stg end as  Updatetime,

(:start_dttm) as start_dttm,(:end_dttm) as end_dttm

from   DB_T_PROD_STAG.pc_PolicyPeriod

inner join DB_T_PROD_STAG.pcx_fopfarmownerslischdcovitem on pc_PolicyPeriod.ID_stg=pcx_fopfarmownerslischdcovitem.BranchID_stg

inner join   DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

inner join   DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join   DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

where pctl_policyperiodstatus.TYPECODE_stg=''Bound''

and pcx_fopfarmownerslischdcovitem.fixedid_stg is not null 

and (pcx_fopfarmownerslischdcovitem.UpdateTime_stg >(:start_dttm) and pcx_fopfarmownerslischdcovitem.UpdateTime_stg <= ( :end_dttm)

or (pc_policyperiod.updatetime_stg>(:start_dttm)and pc_policyperiod.updatetime_stg<=(:end_dttm)))

and (pcx_fopfarmownerslischdcovitem.ExpirationDate_stg is null or pcx_fopfarmownerslischdcovitem.Expirationdate_stg>pc_policyperiod.modeldate_stg) 

QUALIFY	RANK() OVER(PARTITION BY nk_publicid,partyassetid,assettype

order by coalesce(pcx_fopfarmownerslischdcovitem.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp))desc,

pcx_fopfarmownerslischdcovitem.updatetime_stg desc,pcx_fopfarmownerslischdcovitem.createtime_stg desc)=1



/* FOP LIABILITYSCHDCOVITEM */


UNION



select distinct ''SRC_SYS4''as policynumber,pcx_fopliabilityschdcovitem.FixedID_stg as partyassetid,pc_policyperiod.EditEffectiveDate_stg as asset_start_dt,

coalesce(pcx_fopliabilityschdcovitem.ExpirationDate_stg, pc_policyperiod.PeriodEnd_stg) as asset_end_dt,

cast(''PRTY_ASSET_SBTYPE42'' as varchar(50)) as assettype ,

cast(''PRTY_ASSET_CLASFCN20'' as varchar(50)) as agreementtype,cast(NULL  AS integer) as agmt_asset_ref_num,

cast(pc_policyperiod.PublicID_stg as varchar(64)) as  nk_publicid,''SRC_SYS4'' as SYS_SRC_CD,

pc_policyperiod.Retired_stg as Retired,

case when pcx_fopliabilityschdcovitem.Updatetime_stg>pc_policyperiod.updatetime_stg then pcx_fopliabilityschdcovitem.Updatetime_stg else pc_policyperiod.updatetime_stg end as  Updatetime,

(:start_dttm) as start_dttm,(:end_dttm) as end_dttm

from   DB_T_PROD_STAG.pc_PolicyPeriod

inner join DB_T_PROD_STAG.pcx_fopliabilityschdcovitem on pc_PolicyPeriod.ID_stg=pcx_fopliabilityschdcovitem.BranchID_stg

inner join   DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

inner join   DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join   DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

where pctl_policyperiodstatus.TYPECODE_stg=''Bound''

and pcx_fopliabilityschdcovitem.fixedid_stg is not null 

and (pcx_fopliabilityschdcovitem.UpdateTime_stg >(:start_dttm) and pcx_fopliabilityschdcovitem.UpdateTime_stg <= ( :end_dttm)

or (pc_policyperiod.updatetime_stg>(:start_dttm)and pc_policyperiod.updatetime_stg<=(:end_dttm)))

and (pcx_fopliabilityschdcovitem.ExpirationDate_stg is null or pcx_fopliabilityschdcovitem.Expirationdate_stg>pc_policyperiod.modeldate_stg) 

QUALIFY	RANK() OVER(PARTITION BY nk_publicid,partyassetid,assettype

order by coalesce(pcx_fopliabilityschdcovitem.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999'' as timestamp))desc,

pcx_fopliabilityschdcovitem.updatetime_stg desc,pcx_fopliabilityschdcovitem.createtime_stg desc)=1



/* --------------------FARM CHANGES END-------------------------------------------------------- */


) as a



QUALIFY	ROW_NUMBER() OVER(

PARTITION BY a.nk_publicid,a.partyassetid,a.partyassettype 

ORDER BY a.updatetime desc) = 1
) SRC
)
);


-- Component exp_pass_from_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source AS
(
SELECT
sq_pc_policy_asset_x.policynumber as policynumber,
sq_pc_policy_asset_x.partyassetid as partyassetid,
sq_pc_policy_asset_x.asset_start_dt as asset_start_dt,
sq_pc_policy_asset_x.asset_end_dt as asset_end_dt,
sq_pc_policy_asset_x.partyassettype as partyassettype,
sq_pc_policy_asset_x.agreementtype as agreementtype,
NULL as Class_cd,
sq_pc_policy_asset_x.VehicleNumber as VehicleNumber,
sq_pc_policy_asset_x.nk_PublicID as nk_PublicID,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as OUT_SRC_CD,
sq_pc_policy_asset_x.Retired as Retired,
sq_pc_policy_asset_x.Updatetime as Updatetime,
sq_pc_policy_asset_x.source_record_id,
row_number() over (partition by sq_pc_policy_asset_x.source_record_id order by sq_pc_policy_asset_x.source_record_id) as RNK
FROM
sq_pc_policy_asset_x
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_pc_policy_asset_x.SYS_SRC_CD
QUALIFY RNK = 1
);


-- Component LKP_AGMT_POL, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_POL AS
(
SELECT
LKP.AGMT_ID,
exp_pass_from_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.AGMT_TYPE_CD asc,LKP.NK_SRC_KEY asc) RNK
FROM
exp_pass_from_source
LEFT JOIN (
SELECT AGMT.AGMT_ID as AGMT_ID, AGMT.AGMT_TYPE_CD as AGMT_TYPE_CD, AGMT.NK_SRC_KEY as NK_SRC_KEY FROM db_t_prod_core.AGMT where AGMT.AGMT_TYPE_CD=''$p_agmt_type_cd_policy_version''   QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT.NK_SRC_KEY,AGMT.HOST_AGMT_NUM  ORDER BY AGMT.EDW_END_DTTM desc) = 1
) LKP ON LKP.NK_SRC_KEY = exp_pass_from_source.nk_PublicID
QUALIFY RNK = 1
);


-- Component exp_data_trans, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_trans AS
(
SELECT
exp_pass_from_source.policynumber as policynumber,
ltrim ( rtrim ( TO_CHAR ( exp_pass_from_source.partyassetid ) ) ) as v_partyassetid,
exp_pass_from_source.asset_start_dt as asset_start_dt,
exp_pass_from_source.asset_end_dt as asset_end_dt,
CASE WHEN exp_pass_from_source.asset_end_dt IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) ELSE exp_pass_from_source.asset_end_dt END as asset_end_dt1,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ END as v_prty_sbtype_cd,
CASE WHEN LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ IS NULL THEN ''UNK'' ELSE LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ END as v_prty_clsfctn_cd,
exp_pass_from_source.Class_cd as Class_cd,
exp_pass_from_source.OUT_SRC_CD as OUT_SRC_CD,
LKP_5.PRTY_ASSET_ID /* replaced lookup LKP_PRTY_ASSET_ID */ as v_prty_asset_id,
LKP_AGMT_POL.AGMT_ID as v_agmt_id,
v_prty_asset_id as out_party_asset_id,
v_agmt_id as out_agmt_id,
CASE WHEN exp_pass_from_source.asset_start_dt IS NULL THEN CURRENT_TIMESTAMP ELSE exp_pass_from_source.asset_start_dt END as out_start_dt,
exp_pass_from_source.VehicleNumber as VehicleNumber,
exp_pass_from_source.Retired as Retired,
exp_pass_from_source.Updatetime as Updatetime,
exp_pass_from_source.source_record_id,
row_number() over (partition by exp_pass_from_source.source_record_id order by exp_pass_from_source.source_record_id) as RNK
FROM
exp_pass_from_source
INNER JOIN LKP_AGMT_POL ON exp_pass_from_source.source_record_id = LKP_AGMT_POL.source_record_id
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = exp_pass_from_source.partyassettype
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = exp_pass_from_source.partyassettype
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = exp_pass_from_source.agreementtype
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = exp_pass_from_source.agreementtype
LEFT JOIN LKP_PRTY_ASSET_ID LKP_5 ON LKP_5.ASSET_HOST_ID_VAL = v_partyassetid AND LKP_5.PRTY_ASSET_SBTYPE_CD = v_prty_sbtype_cd AND LKP_5.PRTY_ASSET_CLASFCN_CD = v_prty_clsfctn_cd
QUALIFY row_number() over (partition by exp_pass_from_source.source_record_id order by exp_pass_from_source.source_record_id)
= 1
);


-- Component LKP_AGMT_ASSET, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_ASSET AS
(
SELECT
LKP.AGMT_ID,
LKP.PRTY_ASSET_ID,
LKP.AGMT_ASSET_STRT_DTTM,
LKP.AGMT_ASSET_END_DTTM,
LKP.AGMT_ASSET_REF_NUM,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_data_trans.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_trans.source_record_id ORDER BY LKP.AGMT_ID desc,LKP.PRTY_ASSET_ID desc,LKP.AGMT_ASSET_STRT_DTTM desc,LKP.AGMT_ASSET_END_DTTM desc,LKP.AGMT_ASSET_REF_NUM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK
FROM
exp_data_trans
LEFT JOIN (
SELECT	AGMT_ASSET.AGMT_ASSET_STRT_DTTM as AGMT_ASSET_STRT_DTTM,
		AGMT_ASSET.AGMT_ASSET_END_DTTM as AGMT_ASSET_END_DTTM, AGMT_ASSET.AGMT_ASSET_REF_NUM as AGMT_ASSET_REF_NUM,
		 AGMT_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM,
		AGMT_ASSET.EDW_END_DTTM as EDW_END_DTTM, AGMT_ASSET.AGMT_ID as AGMT_ID,
		AGMT_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID 
FROM	db_t_prod_core.AGMT_ASSET WHERE   
EDW_END_DTTM= ''9999-12-31 23:59:59.999999''/*  */
) LKP ON LKP.AGMT_ID = exp_data_trans.out_agmt_id AND LKP.PRTY_ASSET_ID = exp_data_trans.out_party_asset_id
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_trans.source_record_id ORDER BY LKP.AGMT_ID desc,LKP.PRTY_ASSET_ID desc,LKP.AGMT_ASSET_STRT_DTTM desc,LKP.AGMT_ASSET_END_DTTM desc,LKP.AGMT_ASSET_REF_NUM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) 
= 1
);


-- Component exp_ins_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_upd AS
(
SELECT
LKP_AGMT_ASSET.AGMT_ID as lkp_AGMT_ID,
LKP_AGMT_ASSET.PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
LKP_AGMT_ASSET.AGMT_ASSET_STRT_DTTM as lkp_AGMT_ASSET_STRT_DT,
LKP_AGMT_ASSET.AGMT_ASSET_END_DTTM as lkp_AGMT_ASSET_END_DT,
LKP_AGMT_ASSET.AGMT_ASSET_REF_NUM as lkp_AGMT_ASSET_REF_NUM,
LKP_AGMT_ASSET.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_AGMT_ASSET.EDW_END_DTTM as lkp_EDW_END_DTTM,
CURRENT_TIMESTAMP as out_EDW_STRT_DTTM,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as out_EDW_END_DTTM,
exp_data_trans.out_agmt_id as in_AGMT_ID,
exp_data_trans.out_party_asset_id as in_PARTY_ASSET_ID,
exp_data_trans.out_start_dt as asset_start_dt,
exp_data_trans.asset_end_dt1 as asset_end_dt,
:in_out_router_flag as in_out_router_flag,
exp_data_trans.VehicleNumber as VehicleNumber,
exp_data_trans.Class_cd as Class_cd,
MD5 ( TO_CHAR ( LKP_AGMT_ASSET.AGMT_ASSET_STRT_DTTM ) || TO_CHAR ( LKP_AGMT_ASSET.AGMT_ASSET_END_DTTM ) || TO_CHAR ( LKP_AGMT_ASSET.AGMT_ASSET_REF_NUM ) ) as var_orig_chksm,
MD5 ( to_char ( exp_data_trans.out_start_dt ) || to_char ( exp_data_trans.asset_end_dt1 ) || to_char ( exp_data_trans.VehicleNumber ) ) as var_calc_chksm,
CASE WHEN var_orig_chksm IS NULL THEN ''I'' ELSE CASE WHEN var_orig_chksm != var_calc_chksm THEN ''U'' ELSE ''R'' END END as out_ins_upd,
exp_data_trans.Retired as Retired,
exp_data_trans.Updatetime as Updatetime,
exp_data_trans.source_record_id
FROM
exp_data_trans
INNER JOIN LKP_AGMT_ASSET ON exp_data_trans.source_record_id = LKP_AGMT_ASSET.source_record_id
);


-- Component router_ins_out_agmt_asset_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE router_ins_out_agmt_asset_INSERT AS
SELECT
exp_ins_upd.lkp_AGMT_ID as lkp_AGMT_ID,
exp_ins_upd.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_ins_upd.lkp_AGMT_ASSET_STRT_DT as lkp_AGMT_ASSET_STRT_DT,
exp_ins_upd.lkp_AGMT_ASSET_END_DT as lkp_AGMT_ASSET_END_DT,
exp_ins_upd.lkp_AGMT_ASSET_REF_NUM as lkp_AGMT_ASSET_REF_NUM,
exp_ins_upd.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_ins_upd.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_ins_upd.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_ins_upd.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_ins_upd.out_ins_upd as out_ins_upd,
exp_ins_upd.in_out_router_flag as in_out_router_flag,
exp_ins_upd.in_AGMT_ID as in_AGMT_ID,
exp_ins_upd.in_PARTY_ASSET_ID as in_PARTY_ASSET_ID,
exp_ins_upd.asset_start_dt as asset_start_dt,
exp_ins_upd.asset_end_dt as asset_end_dt,
exp_ins_upd.VehicleNumber as VehicleNumber,
exp_ins_upd.Class_cd as Class_cd,
exp_ins_upd.Updatetime as Updatetime,
exp_ins_upd.Retired as Retired,
NULL as out_trans_end_dttm,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.in_AGMT_ID IS NOT NULL and exp_ins_upd.in_PARTY_ASSET_ID IS NOT NULL AND ( exp_ins_upd.out_ins_upd = ''I'' ) OR ( exp_ins_upd.Retired = 0 AND exp_ins_upd.lkp_EDW_END_DTTM != to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) ) /*- - exp_ins_upd.out_ins_upd = ''I'' - - exp_ins_upd.in_out_router_flag = ''INS'' and exp_ins_upd.in_AGMT_ID IS NOT NULL and exp_ins_upd.in_PARTY_ASSET_ID IS NOT NULL*/
;


-- Component router_ins_out_agmt_asset_RETIRE, Type ROUTER Output Group RETIRE
CREATE OR REPLACE TEMPORARY TABLE router_ins_out_agmt_asset_RETIRE AS
SELECT
exp_ins_upd.lkp_AGMT_ID as lkp_AGMT_ID,
exp_ins_upd.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_ins_upd.lkp_AGMT_ASSET_STRT_DT as lkp_AGMT_ASSET_STRT_DT,
exp_ins_upd.lkp_AGMT_ASSET_END_DT as lkp_AGMT_ASSET_END_DT,
exp_ins_upd.lkp_AGMT_ASSET_REF_NUM as lkp_AGMT_ASSET_REF_NUM,
exp_ins_upd.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_ins_upd.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_ins_upd.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_ins_upd.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_ins_upd.out_ins_upd as out_ins_upd,
exp_ins_upd.in_out_router_flag as in_out_router_flag,
exp_ins_upd.in_AGMT_ID as in_AGMT_ID,
exp_ins_upd.in_PARTY_ASSET_ID as in_PARTY_ASSET_ID,
exp_ins_upd.asset_start_dt as asset_start_dt,
exp_ins_upd.asset_end_dt as asset_end_dt,
exp_ins_upd.VehicleNumber as VehicleNumber,
exp_ins_upd.Class_cd as Class_cd,
exp_ins_upd.Updatetime as Updatetime,
exp_ins_upd.Retired as Retired,
NULL as out_trans_end_dttm,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.in_AGMT_ID IS NOT NULL AND exp_ins_upd.out_ins_upd = ''R'' and exp_ins_upd.Retired != 0 and exp_ins_upd.lkp_EDW_END_DTTM = to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' );


-- Component router_ins_out_agmt_asset_UPDATE, Type ROUTER Output Group UPDATE
CREATE OR REPLACE TEMPORARY TABLE router_ins_out_agmt_asset_UPDATE AS
SELECT
exp_ins_upd.lkp_AGMT_ID as lkp_AGMT_ID,
exp_ins_upd.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_ins_upd.lkp_AGMT_ASSET_STRT_DT as lkp_AGMT_ASSET_STRT_DT,
exp_ins_upd.lkp_AGMT_ASSET_END_DT as lkp_AGMT_ASSET_END_DT,
exp_ins_upd.lkp_AGMT_ASSET_REF_NUM as lkp_AGMT_ASSET_REF_NUM,
exp_ins_upd.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_ins_upd.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_ins_upd.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_ins_upd.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_ins_upd.out_ins_upd as out_ins_upd,
exp_ins_upd.in_out_router_flag as in_out_router_flag,
exp_ins_upd.in_AGMT_ID as in_AGMT_ID,
exp_ins_upd.in_PARTY_ASSET_ID as in_PARTY_ASSET_ID,
exp_ins_upd.asset_start_dt as asset_start_dt,
exp_ins_upd.asset_end_dt as asset_end_dt,
exp_ins_upd.VehicleNumber as VehicleNumber,
exp_ins_upd.Class_cd as Class_cd,
exp_ins_upd.Updatetime as Updatetime,
exp_ins_upd.Retired as Retired,
NULL as out_trans_end_dttm,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.in_AGMT_ID IS NOT NULL AND exp_ins_upd.out_ins_upd = ''U'' AND exp_ins_upd.lkp_EDW_END_DTTM = to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' );


-- Component EXP_ins_AGMT_ASSET, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_ins_AGMT_ASSET AS
(
SELECT
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CONTRACT_ROLE_SBTYPE */ as ASSET_CONTRACT_SBTYPE,
router_ins_out_agmt_asset_INSERT.in_AGMT_ID as in_AGMT_ID,
router_ins_out_agmt_asset_INSERT.in_PARTY_ASSET_ID as in_PARTY_ASSET_ID,
router_ins_out_agmt_asset_INSERT.asset_start_dt as asset_start_dt,
router_ins_out_agmt_asset_INSERT.asset_end_dt as asset_end_dt,
:PRCS_ID as PROCESS_ID,
router_ins_out_agmt_asset_INSERT.VehicleNumber as VehicleNumber,
router_ins_out_agmt_asset_INSERT.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM1,
CASE WHEN router_ins_out_agmt_asset_INSERT.Retired != 0 THEN CURRENT_TIMESTAMP ELSE router_ins_out_agmt_asset_INSERT.out_EDW_END_DTTM END as o_EDW_END_DTTM,
router_ins_out_agmt_asset_INSERT.Updatetime as Updatetime,
router_ins_out_agmt_asset_INSERT.source_record_id,
row_number() over (partition by router_ins_out_agmt_asset_INSERT.source_record_id order by router_ins_out_agmt_asset_INSERT.source_record_id) as RNK
FROM
router_ins_out_agmt_asset_INSERT
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CONTRACT_ROLE_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = ''ASSET_CNTRCT_ROLE_SBTYPE1''
QUALIFY RNK = 1
);


-- Component EXP_upd_AGMT_ASSET_upd_Retire_rejected, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_upd_AGMT_ASSET_upd_Retire_rejected AS
(
SELECT
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CONTRACT_ROLE_SBTYPE */ as ASSET_CONTRACT_SBTYPE,
router_ins_out_agmt_asset_RETIRE.lkp_AGMT_ID as in_AGMT_ID,
router_ins_out_agmt_asset_RETIRE.lkp_PRTY_ASSET_ID as in_PARTY_ASSET_ID,
router_ins_out_agmt_asset_RETIRE.lkp_AGMT_ASSET_STRT_DT as asset_start_dt,
router_ins_out_agmt_asset_RETIRE.lkp_AGMT_ASSET_END_DT as asset_end_dt,
:PRCS_ID as PROCESS_ID,
NULL as VehicleNumber,
router_ins_out_agmt_asset_RETIRE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
CURRENT_TIMESTAMP as out_EDW_END_DTTM31,
dateadd (second,-1, router_ins_out_agmt_asset_RETIRE.Updatetime  ) as out_trans_end_dttm4,
router_ins_out_agmt_asset_RETIRE.source_record_id,
row_number() over (partition by router_ins_out_agmt_asset_RETIRE.source_record_id order by router_ins_out_agmt_asset_RETIRE.source_record_id) as RNK
FROM
router_ins_out_agmt_asset_RETIRE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CONTRACT_ROLE_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = ''ASSET_CNTRCT_ROLE_SBTYPE1''
QUALIFY RNK = 1
);


-- Component upd_agmt_asset_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_agmt_asset_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
EXP_ins_AGMT_ASSET.ASSET_CONTRACT_SBTYPE as ASSET_CONTRACT_SBTYPE,
EXP_ins_AGMT_ASSET.in_AGMT_ID as in_AGMT_ID,
EXP_ins_AGMT_ASSET.in_PARTY_ASSET_ID as in_PARTY_ASSET_ID,
EXP_ins_AGMT_ASSET.asset_start_dt as asset_start_dt,
EXP_ins_AGMT_ASSET.asset_end_dt as asset_end_dt,
EXP_ins_AGMT_ASSET.PROCESS_ID as PROCESS_ID,
NULL as agreementtype,
EXP_ins_AGMT_ASSET.VehicleNumber as VehicleNumber,
EXP_ins_AGMT_ASSET.out_EDW_STRT_DTTM1 as out_EDW_STRT_DTTM1,
EXP_ins_AGMT_ASSET.o_EDW_END_DTTM as out_EDW_END_DTTM1,
EXP_ins_AGMT_ASSET.Updatetime as Updatetime,
0 as UPDATE_STRATEGY_ACTION
FROM
EXP_ins_AGMT_ASSET
);


-- Component EXP_upd_AGMT_ASSET_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_upd_AGMT_ASSET_upd AS
(
SELECT
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CONTRACT_ROLE_SBTYPE */ as ASSET_CONTRACT_SBTYPE,
router_ins_out_agmt_asset_UPDATE.lkp_AGMT_ID as in_AGMT_ID,
router_ins_out_agmt_asset_UPDATE.lkp_PRTY_ASSET_ID as in_PARTY_ASSET_ID,
router_ins_out_agmt_asset_UPDATE.asset_start_dt as asset_start_dt,
router_ins_out_agmt_asset_UPDATE.asset_end_dt as asset_end_dt,
:PRCS_ID as PROCESS_ID,
router_ins_out_agmt_asset_UPDATE.VehicleNumber as VehicleNumber,
router_ins_out_agmt_asset_UPDATE.Retired as Retired3,
router_ins_out_agmt_asset_UPDATE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
CASE WHEN router_ins_out_agmt_asset_UPDATE.Retired != 0 THEN CURRENT_TIMESTAMP ELSE dateadd (second,-1,  router_ins_out_agmt_asset_UPDATE.out_EDW_STRT_DTTM ) END as out_EDW_END_DTTM31,
router_ins_out_agmt_asset_UPDATE.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM3,
dateadd (second,-1, router_ins_out_agmt_asset_UPDATE.Updatetime) as out_trans_end_dttm3,
router_ins_out_agmt_asset_UPDATE.source_record_id,
row_number() over (partition by router_ins_out_agmt_asset_UPDATE.source_record_id order by router_ins_out_agmt_asset_UPDATE.source_record_id) as RNK
FROM
router_ins_out_agmt_asset_UPDATE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CONTRACT_ROLE_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = ''ASSET_CNTRCT_ROLE_SBTYPE1''
QUALIFY RNK = 1
);


-- Component EXP_upd_AGMT_ASSET_ins_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_upd_AGMT_ASSET_ins_upd AS
(
SELECT
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CONTRACT_ROLE_SBTYPE */ as ASSET_CONTRACT_SBTYPE,
router_ins_out_agmt_asset_UPDATE.in_AGMT_ID as in_AGMT_ID,
router_ins_out_agmt_asset_UPDATE.in_PARTY_ASSET_ID as in_PARTY_ASSET_ID,
router_ins_out_agmt_asset_UPDATE.asset_start_dt as asset_start_dt,
router_ins_out_agmt_asset_UPDATE.asset_end_dt as asset_end_dt,
:PRCS_ID as PROCESS_ID,
router_ins_out_agmt_asset_UPDATE.VehicleNumber as VehicleNumber,
router_ins_out_agmt_asset_UPDATE.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM3,
router_ins_out_agmt_asset_UPDATE.out_EDW_END_DTTM as out_EDW_END_DTTM3,
router_ins_out_agmt_asset_UPDATE.Updatetime as Updatetime,
router_ins_out_agmt_asset_UPDATE.Retired as Retired3,
router_ins_out_agmt_asset_UPDATE.source_record_id,
row_number() over (partition by router_ins_out_agmt_asset_UPDATE.source_record_id order by router_ins_out_agmt_asset_UPDATE.source_record_id) as RNK
FROM
router_ins_out_agmt_asset_UPDATE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CONTRACT_ROLE_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = ''ASSET_CNTRCT_ROLE_SBTYPE1''
QUALIFY RNK = 1
);


-- Component fil_agmt_asset_ins_upd, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_agmt_asset_ins_upd AS
(
SELECT
EXP_upd_AGMT_ASSET_ins_upd.ASSET_CONTRACT_SBTYPE as ASSET_CONTRACT_SBTYPE,
EXP_upd_AGMT_ASSET_ins_upd.in_AGMT_ID as in_AGMT_ID,
EXP_upd_AGMT_ASSET_ins_upd.in_PARTY_ASSET_ID as in_PARTY_ASSET_ID,
EXP_upd_AGMT_ASSET_ins_upd.asset_start_dt as asset_start_dt,
EXP_upd_AGMT_ASSET_ins_upd.asset_end_dt as asset_end_dt,
EXP_upd_AGMT_ASSET_ins_upd.PROCESS_ID as PROCESS_ID,
EXP_upd_AGMT_ASSET_ins_upd.VehicleNumber as VehicleNumber,
EXP_upd_AGMT_ASSET_ins_upd.out_EDW_STRT_DTTM3 as out_EDW_STRT_DTTM3,
EXP_upd_AGMT_ASSET_ins_upd.out_EDW_END_DTTM3 as out_EDW_END_DTTM3,
EXP_upd_AGMT_ASSET_ins_upd.Updatetime as Updatetime,
EXP_upd_AGMT_ASSET_ins_upd.Retired3 as Retired3,
EXP_upd_AGMT_ASSET_ins_upd.source_record_id
FROM
EXP_upd_AGMT_ASSET_ins_upd
WHERE EXP_upd_AGMT_ASSET_ins_upd.Retired3 = 0
);


-- Component fil_agmt_asset_upd_upd, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_agmt_asset_upd_upd AS
(
SELECT
EXP_upd_AGMT_ASSET_upd.ASSET_CONTRACT_SBTYPE as ASSET_CONTRACT_SBTYPE,
EXP_upd_AGMT_ASSET_upd.in_AGMT_ID as in_AGMT_ID,
EXP_upd_AGMT_ASSET_upd.in_PARTY_ASSET_ID as in_PARTY_ASSET_ID,
EXP_upd_AGMT_ASSET_upd.asset_start_dt as asset_start_dt,
EXP_upd_AGMT_ASSET_upd.asset_end_dt as asset_end_dt,
EXP_upd_AGMT_ASSET_upd.PROCESS_ID as PROCESS_ID,
EXP_upd_AGMT_ASSET_upd.VehicleNumber as VehicleNumber,
EXP_upd_AGMT_ASSET_upd.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
NULL as out_EDW_STRT_DTTM3,
EXP_upd_AGMT_ASSET_upd.out_EDW_END_DTTM31 as out_EDW_STRT_DTTM31,
EXP_upd_AGMT_ASSET_upd.lkp_EDW_END_DTTM3 as lkp_EDW_END_DTTM3,
EXP_upd_AGMT_ASSET_upd.Retired3 as Retired3,
EXP_upd_AGMT_ASSET_upd.out_trans_end_dttm3 as out_trans_end_dttm3,
EXP_upd_AGMT_ASSET_upd.source_record_id
FROM
EXP_upd_AGMT_ASSET_upd
WHERE EXP_upd_AGMT_ASSET_upd.lkp_EDW_END_DTTM3 = to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
);


-- Component AGMT_ASSET_INS, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_ASSET
(
AGMT_ID,
ASSET_CNTRCT_ROLE_SBTYPE_CD,
PRTY_ASSET_ID,
AGMT_ASSET_STRT_DTTM,
AGMT_ASSET_END_DTTM,
AGMT_ASSET_REF_NUM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
upd_agmt_asset_ins.in_AGMT_ID as AGMT_ID,
upd_agmt_asset_ins.ASSET_CONTRACT_SBTYPE as ASSET_CNTRCT_ROLE_SBTYPE_CD,
upd_agmt_asset_ins.in_PARTY_ASSET_ID as PRTY_ASSET_ID,
upd_agmt_asset_ins.asset_start_dt as AGMT_ASSET_STRT_DTTM,
upd_agmt_asset_ins.asset_end_dt as AGMT_ASSET_END_DTTM,
upd_agmt_asset_ins.VehicleNumber as AGMT_ASSET_REF_NUM,
upd_agmt_asset_ins.PROCESS_ID as PRCS_ID,
upd_agmt_asset_ins.out_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
upd_agmt_asset_ins.out_EDW_END_DTTM1 as EDW_END_DTTM,
upd_agmt_asset_ins.Updatetime as TRANS_STRT_DTTM
FROM
upd_agmt_asset_ins;


-- Component upd_agmt_asset_upd_Retire_rejected, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_agmt_asset_upd_Retire_rejected AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
EXP_upd_AGMT_ASSET_upd_Retire_rejected.ASSET_CONTRACT_SBTYPE as ASSET_CONTRACT_SBTYPE,
EXP_upd_AGMT_ASSET_upd_Retire_rejected.in_AGMT_ID as in_AGMT_ID,
EXP_upd_AGMT_ASSET_upd_Retire_rejected.in_PARTY_ASSET_ID as in_PARTY_ASSET_ID,
EXP_upd_AGMT_ASSET_upd_Retire_rejected.asset_start_dt as asset_start_dt,
EXP_upd_AGMT_ASSET_upd_Retire_rejected.asset_end_dt as asset_end_dt,
EXP_upd_AGMT_ASSET_upd_Retire_rejected.PROCESS_ID as PROCESS_ID,
NULL as agreementtype,
EXP_upd_AGMT_ASSET_upd_Retire_rejected.VehicleNumber as VehicleNumber,
EXP_upd_AGMT_ASSET_upd_Retire_rejected.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
EXP_upd_AGMT_ASSET_upd_Retire_rejected.out_EDW_END_DTTM31 as out_EDW_END_DTTM31,
EXP_upd_AGMT_ASSET_upd_Retire_rejected.out_trans_end_dttm4 as out_trans_end_dttm4,
1 as UPDATE_STRATEGY_ACTION
FROM
EXP_upd_AGMT_ASSET_upd_Retire_rejected
);


-- Component upd_agmt_asset_ins_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_agmt_asset_ins_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
fil_agmt_asset_ins_upd.ASSET_CONTRACT_SBTYPE as ASSET_CONTRACT_SBTYPE,
fil_agmt_asset_ins_upd.in_AGMT_ID as in_AGMT_ID,
fil_agmt_asset_ins_upd.in_PARTY_ASSET_ID as in_PARTY_ASSET_ID,
fil_agmt_asset_ins_upd.asset_start_dt as asset_start_dt,
fil_agmt_asset_ins_upd.asset_end_dt as asset_end_dt,
fil_agmt_asset_ins_upd.PROCESS_ID as PROCESS_ID,
NULL as agreementtype,
fil_agmt_asset_ins_upd.VehicleNumber as VehicleNumber,
fil_agmt_asset_ins_upd.out_EDW_STRT_DTTM3 as out_EDW_STRT_DTTM3,
fil_agmt_asset_ins_upd.out_EDW_END_DTTM3 as out_EDW_END_DTTM3,
fil_agmt_asset_ins_upd.Updatetime as Updatetime,
0 as UPDATE_STRATEGY_ACTION
FROM
fil_agmt_asset_ins_upd
);


-- Component AGMT_ASSET_UPD_Retire_rejected, Type TARGET 
/* Perform Updates */
MERGE INTO DB_T_PROD_CORE.AGMT_ASSET
USING upd_agmt_asset_upd_Retire_rejected ON (UPDATE_STRATEGY_ACTION = 1 AND AGMT_ASSET.AGMT_ID = upd_agmt_asset_upd_Retire_rejected.in_AGMT_ID AND AGMT_ASSET.ASSET_CNTRCT_ROLE_SBTYPE_CD = upd_agmt_asset_upd_Retire_rejected.ASSET_CONTRACT_SBTYPE AND AGMT_ASSET.PRTY_ASSET_ID = upd_agmt_asset_upd_Retire_rejected.in_PARTY_ASSET_ID AND AGMT_ASSET.EDW_STRT_DTTM = upd_agmt_asset_upd_Retire_rejected.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
EDW_END_DTTM = upd_agmt_asset_upd_Retire_rejected.out_EDW_END_DTTM31,
TRANS_END_DTTM = upd_agmt_asset_upd_Retire_rejected.out_trans_end_dttm4
;


-- Component upd_agmt_asset_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_agmt_asset_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
fil_agmt_asset_upd_upd.ASSET_CONTRACT_SBTYPE as ASSET_CONTRACT_SBTYPE,
fil_agmt_asset_upd_upd.in_AGMT_ID as in_AGMT_ID,
fil_agmt_asset_upd_upd.in_PARTY_ASSET_ID as in_PARTY_ASSET_ID,
fil_agmt_asset_upd_upd.asset_start_dt as asset_start_dt,
fil_agmt_asset_upd_upd.asset_end_dt as asset_end_dt,
fil_agmt_asset_upd_upd.PROCESS_ID as PROCESS_ID,
NULL as agreementtype,
fil_agmt_asset_upd_upd.VehicleNumber as VehicleNumber,
fil_agmt_asset_upd_upd.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
fil_agmt_asset_upd_upd.out_EDW_STRT_DTTM31 as out_EDW_STRT_DTTM31,
fil_agmt_asset_upd_upd.out_trans_end_dttm3 as out_trans_end_dttm3,
1 as UPDATE_STRATEGY_ACTION
FROM
fil_agmt_asset_upd_upd
);


-- Component AGMT_ASSET_INS_UPD, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_ASSET
(
AGMT_ID,
ASSET_CNTRCT_ROLE_SBTYPE_CD,
PRTY_ASSET_ID,
AGMT_ASSET_STRT_DTTM,
AGMT_ASSET_END_DTTM,
AGMT_ASSET_REF_NUM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
upd_agmt_asset_ins_upd.in_AGMT_ID as AGMT_ID,
upd_agmt_asset_ins_upd.ASSET_CONTRACT_SBTYPE as ASSET_CNTRCT_ROLE_SBTYPE_CD,
upd_agmt_asset_ins_upd.in_PARTY_ASSET_ID as PRTY_ASSET_ID,
upd_agmt_asset_ins_upd.asset_start_dt as AGMT_ASSET_STRT_DTTM,
upd_agmt_asset_ins_upd.asset_end_dt as AGMT_ASSET_END_DTTM,
upd_agmt_asset_ins_upd.VehicleNumber as AGMT_ASSET_REF_NUM,
upd_agmt_asset_ins_upd.PROCESS_ID as PRCS_ID,
upd_agmt_asset_ins_upd.out_EDW_STRT_DTTM3 as EDW_STRT_DTTM,
upd_agmt_asset_ins_upd.out_EDW_END_DTTM3 as EDW_END_DTTM,
upd_agmt_asset_ins_upd.Updatetime as TRANS_STRT_DTTM
FROM
upd_agmt_asset_ins_upd;


-- Component AGMT_ASSET_UPD, Type TARGET 
/* Perform Updates */
MERGE INTO DB_T_PROD_CORE.AGMT_ASSET
USING upd_agmt_asset_upd ON (UPDATE_STRATEGY_ACTION = 1 AND AGMT_ASSET.AGMT_ID = upd_agmt_asset_upd.in_AGMT_ID AND AGMT_ASSET.ASSET_CNTRCT_ROLE_SBTYPE_CD = upd_agmt_asset_upd.ASSET_CONTRACT_SBTYPE AND AGMT_ASSET.PRTY_ASSET_ID = upd_agmt_asset_upd.in_PARTY_ASSET_ID AND AGMT_ASSET.EDW_STRT_DTTM = upd_agmt_asset_upd.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
EDW_END_DTTM = upd_agmt_asset_upd.out_EDW_STRT_DTTM31,
TRANS_END_DTTM = upd_agmt_asset_upd.out_trans_end_dttm3
;


INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
SELECT :run_id, :worklet_name, ''m_base_agmt_asset_insupd'', ''SUCCEEDED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
  ''start_dttm'', :start_dttm,
  ''end_dttm'', :end_dttm,
  ''StartTime'', :v_start_time
);

EXCEPTION WHEN OTHER THEN
    INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
    SELECT :run_id, :worklet_name, ''m_base_agmt_asset_insupd'', ''FAILED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
        ''SQLERRM'', :sqlerrm
    );


END; ';