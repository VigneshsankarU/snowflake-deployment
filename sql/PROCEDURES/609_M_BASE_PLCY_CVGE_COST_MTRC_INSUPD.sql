-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PLCY_CVGE_COST_MTRC_INSUPD("WORKLET_NAME" VARCHAR)
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
 

-- Component SQ_PLCY_CVGE_COST_MTRC, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_PLCY_CVGE_COST_MTRC AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Tgt_PLCY_CVGE_MTRC_STRT_DTTM,
$2 as Src_AGMT_ID,
$3 as Src_FEAT_ID,
$4 as Src_COST_MTRC_TYPE_CD,
$5 as Src_AGMT_FEAT_STRT_DTTM,
$6 as Src_AGMT_FEAT_ROLE_CD,
$7 as Src_INSRNC_MTRC_TYPE_CD,
$8 as Src_PLCY_CVGE_MTRC_STRT_DTTM,
$9 as Src_PLCY_CVGE_MTRC_END_DTTM,
$10 as Src_TM_PRD_CD,
$11 as Src_PLCY_CVGE_COST_AMT,
$12 as Src_UOM_CD,
$13 as Src_CURY_CD,
$14 as Src_UOM_TYPE_CD,
$15 as Src_TRANS_STRT_DTTM,
$16 as SRC_MD5,
$17 as TGT_MD5,
$18 as ins_upd_flag,
$19 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
with  NON_POLTRM_TEMP1 as (

/* PPV Added as part of Umbrella */
SELECT PublicID,COV_TYPE_CD,UpdateTime,EditEffectiveDate,sum(AMOUNT) as AMOUNT, STATUS,PUPMILLIONTYPE

FROM(

SELECT Distinct pc_policyperiod.PublicID_stg as PublicID, EXPANDEDHOCOSTTABLE.Coverable_or_PolicyLine_CovPattern_stg as COV_TYPE_CD,

			    PCX_PUPTRANSACTION.UpdateTime_stg as UpdateTime, pc_policyperiod.EditEffectiveDate_stg as EditEffectiveDate,pcx_puptransaction.Amount_stg as AMOUNT,

			    pctl_policyperiodstatus.TYPECODE_stg as STATUS,PCX_PUPTRANSACTION.ID_STG,

				PCTL_PUPMILLIONTYPE_ALFA.Typecode_stg as PUPMILLIONTYPE

FROM db_t_prod_stag.PCX_PUPTRANSACTION

JOIN (    

SELECT DISTINCT PC_POLICYPERIOD.POLICYNUMBER_stg, pcx_puppersonalumbrellalinecov.personalumbrellaline_stg AS PUPID_STG,

CASE            

WHEN PCX_PUPCOST.puppersonalumbrellalinecov_stg IS NOT NULL THEN ''PC_POLICYLINE''      

END AS TABLE_NAME_FOR_FIXEDID_STG,      

CASE       

WHEN PCX_PUPCOST.puppersonalumbrellalinecov_stg IS NOT NULL THEN pcx_puppersonalumbrellalinecov.PATTERNCODE_stg          

END AS COVERABLE_OR_POLICYLINE_COVPATTERN_STG,      

CASE            

WHEN PCX_PUPCOST.puppersonalumbrellalinecov_stg IS NOT NULL THEN pcx_puppersonalumbrellalinecov.PATTERNCODE_stg     

END AS COVERABLE_OR_POLICYLINE_COVNAME_STG, PCX_PUPCOST.*    

FROM db_t_prod_stag.PCX_PUPCOST

JOIN db_t_prod_stag.PC_POLICYPERIOD ON PCX_PUPCOST.BRANCHID_stg=PC_POLICYPERIOD.ID_stg

LEFT JOIN db_t_prod_stag.PCX_PUPPERSONALUMBRELLALINECOV  ON PCX_PUPCOST.PUPPERSONALUMBRELLALINECOV_stg = PCX_PUPPERSONALUMBRELLALINECOV.FIXEDID_stg

AND PCX_PUPPERSONALUMBRELLALINECOV.BRANCHID_stg = PC_POLICYPERIOD.ID_stg

AND PCX_PUPPERSONALUMBRELLALINECOV.EXPIRATIONDATE_stg IS NULL

) EXPANDEDHOCOSTTABLE ON PCX_PUPTRANSACTION.COST_stg = EXPANDEDHOCOSTTABLE.ID_stg

LEFT JOIN db_t_prod_stag.PCTL_CHARGEPATTERN ON EXPANDEDHOCOSTTABLE.CHARGEPATTERN_stg = PCTL_CHARGEPATTERN.ID_stg

LEFT JOIN db_t_prod_stag.PCTL_PUPCOST ON EXPANDEDHOCOSTTABLE.SUBTYPE_stg = PCTL_PUPCOST.ID_stg

LEFT JOIN db_t_prod_stag.PCTL_PUPMILLIONTYPE_ALFA ON EXPANDEDHOCOSTTABLE.PUPMILLIONTYPE_ALFA_stg= PCTL_PUPMILLIONTYPE_ALFA.ID_stg

JOIN db_t_prod_stag.PC_POLICYPERIOD ON PCX_PUPTRANSACTION.BRANCHID_stg = PC_POLICYPERIOD.ID_stg

AND EXPANDEDHOCOSTTABLE.POLICYNUMBER_stg =PC_POLICYPERIOD.POLICYNUMBER_stg

JOIN db_t_prod_stag.PC_JOB ON PC_POLICYPERIOD.JOBID_stg = PC_JOB.ID_stg

LEFT JOIN db_t_prod_stag.PCTL_JOB ON PC_JOB.SUBTYPE_stg = PCTL_JOB.ID_stg

LEFT JOIN db_t_prod_stag.PC_POLICYLINE ON PC_POLICYPERIOD.ID_stg = PC_POLICYLINE.BRANCHID_stg

AND PC_POLICYLINE.EXPIRATIONDATE_stg IS NULL   

LEFT JOIN db_t_prod_stag.PCTL_PUPPOLICYTYPE ON PC_POLICYLINE.PUPPOLICYTYPE_stg = PCTL_PUPPOLICYTYPE.ID_stg

LEFT JOIN db_t_prod_stag.PC_POLICY ON PC_POLICYPERIOD.POLICYID_stg = PC_POLICY.ID_stg

LEFT JOIN db_t_prod_stag.PC_ACCOUNT ON PC_POLICY.ACCOUNTID_stg = PC_ACCOUNT.ID_stg

JOIN db_t_prod_stag.PCTL_POLICYPERIODSTATUS ON PC_POLICYPERIOD.STATUS_stg=PCTL_POLICYPERIODSTATUS.ID_stg

WHERE PCTL_CHARGEPATTERN.NAME_stg = ''Premium''

AND PC_POLICYPERIOD.STATUS_stg = 9

AND pc_policyperiod.UpdateTime_stg > (:start_dttm)     

AND pc_policyperiod.UpdateTime_stg <= (:end_dttm)

)A

group by PublicID, COV_TYPE_CD,UpdateTime,EditEffectiveDate,STATUS,PUPMILLIONTYPE

)

Select	TGT_PLCY_CVGE_COST.PLCY_CVGE_COST_MTRC_STRT_DTTM as TGT_PLCY_CVGE_COST_MTRC_STRT_DTTM,

		   xlat_src.Src_AGMT_ID as Src_AGMT_ID, 

		   xlat_src.Src_FEAT_ID as Src_FEAT_ID,

		   xlat_src.Src_COST_MTRC_TYPE_CD as Src_COST_MTRC_TYPE_CD,

		   xlat_src.Src_AGMT_FEAT_STRT_DTTM as Src_AGMT_FEAT_STRT_DTTM,

		   xlat_src.Src_AGMT_FEAT_ROLE_CD as Src_AGMT_FEAT_ROLE_CD,

		   xlat_src.Src_INSRNC_MTRC_TYPE_CD as Src_INSRNC_MTRC_TYPE_CD,

		   xlat_src.Src_PLCY_CVGE_COST_MTRC_STRT_DTTM as Src_PLCY_CVGE_COST_MTRC_STRT_DTTM,

		   xlat_src.Src_PLCY_CVGE_COST_MTRC_END_DTTM as Src_PLCY_CVGE_COST_MTRC_END_DTTM,

		   xlat_src.Src_TM_PRD_CD as Src_TM_PRD_CD, xlat_src.Src_PLCY_CVGE_COST_AMT as Src_PLCY_CVGE_COST_AMT,

		   xlat_src.Src_UOM_CD as Src_UOM_CD, xlat_src.Src_CURY_CD as Src_CURY_CD,

		   xlat_src.Src_UOM_TYPE_CD as Src_UOM_TYPE_CD, xlat_src.Src_TRANS_STRT_DTTM as Src_TRANS_STRT_DTTM,

		    /*Source MD5*/ 

			--CAST(TRIM(to_date(XLAT_SRC.Src_PLCY_CVGE_COST_MTRC_STRT_DTTM ,''yyyy-mm-dd'')) || TRIM(COALESCE(XLAT_SRC.Src_PLCY_CVGE_COST_AMT,0)) as Varchar(1100)) as SRC_MD5,
			TO_VARCHAR(	XLAT_SRC.Src_PLCY_CVGE_COST_MTRC_STRT_DTTM, ''YYYY-MM-DD'')
			|| TRIM(COALESCE(TO_VARCHAR(XLAT_SRC.Src_PLCY_CVGE_COST_AMT), ''0''))
			AS SRC_MD5,
		   /*Target MD5*/ 

		  -- CAST(TRIM(to_date(TGT_PLCY_CVGE_COST_MTRC_STRT_DTTM ,''yyyy-mm-dd'')) || TRIM(COALESCE(TGT_PLCY_CVGE_COST.PLCY_CVGE_COST_AMT,0)) as Varchar(1100)) as TGT_MD5,
			TO_VARCHAR(TGT_PLCY_CVGE_COST_MTRC_STRT_DTTM, ''YYYY-MM-DD'')
			|| TRIM(COALESCE(TO_VARCHAR(TGT_PLCY_CVGE_COST.PLCY_CVGE_COST_AMT), ''0''))
			AS TGT_MD5,
		   /*Flag*/     

		CASE      

			WHEN TGT_PLCY_CVGE_COST.FEAT_ID IS NULL    

	AND TGT_PLCY_CVGE_COST.AGMT_ID IS NULL    

	AND TGT_PLCY_CVGE_COST.AGMT_FEAT_STRT_DTTM IS NULL    

	AND TGT_PLCY_CVGE_COST.INSRNC_MTRC_TYPE_CD IS NULL    

	AND TGT_PLCY_CVGE_COST.COST_MTRC_TYPE_CD IS NULL 

	AND XLAT_SRC.Src_FEAT_ID IS NOT NULL    

	AND XLAT_SRC.Src_AGMT_ID IS NOT NULL    

	AND XLAT_SRC.Src_AGMT_FEAT_STRT_DTTM IS NOT NULL    

	AND XLAT_SRC.Src_INSRNC_MTRC_TYPE_CD IS NOT NULL    

	AND XLAT_SRC.Src_COST_MTRC_TYPE_CD IS NOT NULL

	THEN ''I''           

			WHEN TGT_PLCY_CVGE_COST.FEAT_ID IS NOT NULL    

	AND TGT_PLCY_CVGE_COST.AGMT_ID IS NOT NULL    

	AND TGT_PLCY_CVGE_COST.AGMT_FEAT_STRT_DTTM IS NOT NULL    

	AND TGT_PLCY_CVGE_COST.INSRNC_MTRC_TYPE_CD IS NOT NULL    

	AND TGT_PLCY_CVGE_COST.COST_MTRC_TYPE_CD IS NOT NULL 

	AND SRC_MD5 = TGT_MD5 THEN ''R''     

		end as ins_upd_flag   	   

		    

	from	( /* Source query*/   

 Select	Publicid, Agmt_type,     

		Case  when FE.FEAT_ID is null then ''9999''  else FE.FEAT_ID End as Src_FEAT_ID,    

		CAST(''1900-01-01'' AS DATE ) AS Src_AGMT_FEAT_STRT_DTTM,

        LKP_AGMT_PPV.AGMT_ID as Src_AGMT_ID, COALESCE(xlat_insrnc.TGT_IDNTFTN_VAL,''UNK'') as Src_Insrnc_mtrc_type_cd,

		COALESCE(xlat_cost.TGT_IDNTFTN_VAL,''UNK'') as Src_cost_mtrc_type_cd,

		COALESCE(busn_dt,to_timestamp_ntz(''1900-01-01 00:00:00.000001'', ''YYYY-MM-DDBHH:MI:SS.FF6'' )) As Src_PLCY_CVGE_COST_MTRC_STRT_DTTM,

		to_timestamp_ntz(''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DDBHH:MI:SS.FF6'' ) AS Src_PLCY_CVGE_COST_MTRC_END_DTTM,

		cast(''UNK'' as varchar(10)) as Src_TM_PRD_CD,

		CAST(PLCY_CVGE_COST_AMT AS DECIMAL(18,4)) AS Src_PLCY_CVGE_COST_AMT,

		trans_dt as Src_TRANS_STRT_DTTM , cast(''UNK'' as varchar(10)) as Src_UOM_CD,

		cast(''UNK'' as varchar(10)) as Src_CURY_CD, cast(''UNK'' as varchar(10)) as Src_UOM_TYPE_CD,

		cast(''UNK'' as varchar(10)) as Src_AGMT_FEAT_ROLE_CD    

FROM	( 

	SELECT	DISTINCT rtrim(ltrim(COV_TYPE_CD )) as COV_TYPE_CD , publicid ,termnumber , agmt_type , INSRNC_MTRC_TYPE_CD , PLCY_CVGE_COST_AMT,

			    busn_dt as busn_dt, trans_dt as trans_dt ,PUPMILLIONTYPE     

	FROM	(    

	select	cov_type_cd , publicid , Termnumber , agmt_type , INSRNC_MTRC_TYPE_CD ,

			    SUM(amount) as PLCY_CVGE_COST_AMT, busn_dt , trans_dt ,PUPMILLIONTYPE     

	from	 (     

		select	cov_type_cd, publicid, cast(NULL as varchar(64)) as Termnumber,

				     cast(''PPV'' as varchar(64)) as agmt_type, cast(''INSRNC_MTRC_TYPE16'' as varchar(64)) AS INSRNC_MTRC_TYPE_CD,

				     Amount, EditEffectiveDate as busn_dt,UpdateTime as trans_dt,  STATUS AS PolicyperiodStatus ,PUPMILLIONTYPE      

from	(  /* PPV    */
			SELECT	*  FROM	NON_POLTRM_TEMP1      

		) pc_plcy_writtn_prem_x1    

		where	PolicyperiodStatus=''Bound'' ) as pc_plcy_writtn_prem_x 

	group by cov_type_cd, publicid,Termnumber,agmt_type,INSRNC_MTRC_TYPE_CD, busn_dt, trans_dt ,PUPMILLIONTYPE 

				)a    

	QUALIFY	ROW_NUMBER() OVER(    

PARTITION BY COV_TYPE_CD, publicid,termnumber,agmt_type,INSRNC_MTRC_TYPE_CD,PUPMILLIONTYPE ORDER BY trans_dt DESC, busn_dt DESC) = 1 ) SRC /* EIM-47577 */
/*LKP_AGMT_PPV*/   

LEFT OUTER JOIN (    

	SELECT	AGMT.AGMT_ID as AGMT_ID, AGMT.HOST_AGMT_NUM as HOST_AGMT_NUM,

			    AGMT.NK_SRC_KEY as NK_SRC_KEY, AGMT.AGMT_TYPE_CD as AGMT_TYPE_CD,

			    AGMT.EDW_END_DTTM    

	FROM	--EVIEWDB_EDW.AGMT 
	db_t_prod_core.AGMT AS AGMT    

	WHERE	CAST(EDW_END_DTTM AS DATE)=''9999-12-31''     

		AND AGMT_TYPE_CD=''PPV'' ) LKP_AGMT_PPV    

	ON LKP_AGMT_PPV.NK_SRC_KEY=Src.PublicID    

	AND LKP_AGMT_PPV.AGMT_TYPE_CD=Src.agmt_type 

/*LKP_FEAT*/   

LEFT OUTER JOIN (    
	
	SELECT	FEAT_ID , NK_SRC_KEY    

	FROM	--EVIEWDB_EDW.FEAT 
	db_t_prod_core.FEAT AS FEAT    

	WHERE	CAST(EDW_END_DTTM AS DATE)=''9999-12-31'' ) FE    

	ON FE.NK_SRC_KEY=SRC.cov_type_cd  

/*LKP_XLAT_INSRNC_TYPE*/

LEFT OUTER JOIN (    

	SELECT	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL ,

			    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL    

	FROM	--EVIEWDB_EDW.TERADATA_ETL_REF_XLAT 
	db_t_prod_core.TERADATA_ETL_REF_XLAT AS TERADATA_ETL_REF_XLAT    

	WHERE	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INSRNC_MTRC_TYPE''     

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''DERIVED''     

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS''     

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' )xlat_insrnc    

	on xlat_insrnc.SRC_IDNTFTN_VAL=Src.INSRNC_MTRC_TYPE_CD 

/*LKP_XLAT_COST_TYPE*/

LEFT OUTER JOIN (    

	SELECT	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL ,

			    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL    

	FROM	--EVIEWDB_EDW.TERADATA_ETL_REF_XLAT 
	db_t_prod_core.TERADATA_ETL_REF_XLAT AS TERADATA_ETL_REF_XLAT    

	WHERE	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''COST_MTRC_TYPE''     

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_pupmilliontype_alfa.typecode''     

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW''     

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' )xlat_cost    

	on xlat_cost.SRC_IDNTFTN_VAL=Src.PUPMILLIONTYPE 	

	

	)XLAT_SRC 

	/* Target Lookup PLCY_CVGE_COST_MTRC*/   

LEFT OUTER JOIN(    

SELECT	AGMT_ID AS AGMT_ID, FEAT_ID AS FEAT_ID,COST_MTRC_TYPE_CD AS COST_MTRC_TYPE_CD, AGMT_FEAT_STRT_DTTM AS AGMT_FEAT_STRT_DTTM,

			    AGMT_FEAT_ROLE_CD AS AGMT_FEAT_ROLE_CD, INSRNC_MTRC_TYPE_CD AS INSRNC_MTRC_TYPE_CD,

			    PLCY_CVGE_COST_MTRC_STRT_DTTM AS PLCY_CVGE_COST_MTRC_STRT_DTTM, PLCY_CVGE_COST_AMT AS PLCY_CVGE_COST_AMT,

			    EDW_STRT_DTTM AS EDW_STRT_DTTM    

	FROM	--EVIEWDB_EDW.PLCY_CVGE_COST_MTRC    
	 db_t_prod_core.PLCY_CVGE_COST_MTRC 

	WHERE	INSRNC_MTRC_TYPE_CD =''TRANPREM''  

	AND CAST(EDW_END_DTTM AS DATE) = CAST(''9999-12-31'' AS DATE) ) TGT_PLCY_CVGE_COST    

	ON xlat_src.Src_AGMT_ID=TGT_PLCY_CVGE_COST.AGMT_ID    

	AND xlat_src.Src_AGMT_FEAT_STRT_DTTM=TGT_PLCY_CVGE_COST.AGMT_FEAT_STRT_DTTM    

	AND CAST(xlat_src.Src_FEAT_ID AS decimal(19,0))=Cast(TGT_PLCY_CVGE_COST.FEAT_ID AS decimal(19,0))    

	AND xlat_src.Src_Insrnc_mtrc_type_cd=TGT_PLCY_CVGE_COST.INSRNC_MTRC_TYPE_CD    

	AND xlat_src.Src_AGMT_FEAT_ROLE_CD=TGT_PLCY_CVGE_COST.AGMT_FEAT_ROLE_CD  

	AND xlat_src.Src_COST_MTRC_TYPE_CD= TGT_PLCY_CVGE_COST.COST_MTRC_TYPE_CD 

where ins_upd_flag=''I''
) SRC
)
);


-- Component exp_pass_frm_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_frm_source AS
(
SELECT
SQ_PLCY_CVGE_COST_MTRC.Tgt_PLCY_CVGE_MTRC_STRT_DTTM as TGT_PLCY_CVGE_MTRC_STRT_DTTM,
SQ_PLCY_CVGE_COST_MTRC.Src_AGMT_ID as Src_AGMT_ID,
SQ_PLCY_CVGE_COST_MTRC.Src_FEAT_ID as SRC_FEAT_ID,
SQ_PLCY_CVGE_COST_MTRC.Src_COST_MTRC_TYPE_CD as Src_COST_MTRC_TYPE_CD,
SQ_PLCY_CVGE_COST_MTRC.Src_AGMT_FEAT_STRT_DTTM as Src_AGMT_FEAT_STRT_DTTM,
SQ_PLCY_CVGE_COST_MTRC.Src_AGMT_FEAT_ROLE_CD as Src_AGMT_FEAT_ROLE_CD,
SQ_PLCY_CVGE_COST_MTRC.Src_INSRNC_MTRC_TYPE_CD as Src_INSRNC_MTRC_TYPE_CD,
SQ_PLCY_CVGE_COST_MTRC.Src_PLCY_CVGE_MTRC_STRT_DTTM as Src_PLCY_CVGE_MTRC_STRT_DTTM,
SQ_PLCY_CVGE_COST_MTRC.Src_PLCY_CVGE_MTRC_END_DTTM as Src_PLCY_CVGE_MTRC_END_DTTM,
SQ_PLCY_CVGE_COST_MTRC.Src_TM_PRD_CD as Src_TM_PRD_CD,
SQ_PLCY_CVGE_COST_MTRC.Src_PLCY_CVGE_COST_AMT as Src_PLCY_ASSET_CVGE_AMT,
SQ_PLCY_CVGE_COST_MTRC.Src_UOM_CD as Src_UOM_CD,
SQ_PLCY_CVGE_COST_MTRC.Src_CURY_CD as Src_CURY_CD,
SQ_PLCY_CVGE_COST_MTRC.Src_UOM_TYPE_CD as Src_UOM_TYPE_CD,
SQ_PLCY_CVGE_COST_MTRC.Src_TRANS_STRT_DTTM as Src_TRANS_STRT_DTTM,
SQ_PLCY_CVGE_COST_MTRC.ins_upd_flag as ins_upd_flag,
to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_EDW_END_DTTM,
:PRCS_ID as out_PRCS_ID,
SQ_PLCY_CVGE_COST_MTRC.source_record_id
FROM
SQ_PLCY_CVGE_COST_MTRC
);


-- Component rtr_insupd_Insert, Type ROUTER Output Group Insert
create or replace temporary table rtr_insupd_Insert as
(	
SELECT
exp_pass_frm_source.TGT_PLCY_CVGE_MTRC_STRT_DTTM as TGT_PLCY_CVGE_MTRC_STRT_DTTM,
exp_pass_frm_source.Src_AGMT_ID as Src_AGMT_ID,
exp_pass_frm_source.SRC_FEAT_ID as SRC_FEAT_ID,
exp_pass_frm_source.Src_COST_MTRC_TYPE_CD as Src_COST_MTRC_TYPE_CD,
exp_pass_frm_source.Src_AGMT_FEAT_STRT_DTTM as Src_AGMT_FEAT_STRT_DTTM,
exp_pass_frm_source.Src_AGMT_FEAT_ROLE_CD as Src_AGMT_FEAT_ROLE_CD,
exp_pass_frm_source.Src_INSRNC_MTRC_TYPE_CD as Src_INSRNC_MTRC_TYPE_CD,
exp_pass_frm_source.Src_PLCY_CVGE_MTRC_STRT_DTTM as Src_PLCY_CVGE_MTRC_STRT_DTTM,
exp_pass_frm_source.Src_PLCY_CVGE_MTRC_END_DTTM as Src_PLCY_CVGE_MTRC_END_DTTM,
exp_pass_frm_source.Src_TM_PRD_CD as Src_TM_PRD_CD,
exp_pass_frm_source.Src_PLCY_ASSET_CVGE_AMT as Src_PLCY_ASSET_CVGE_AMT,
exp_pass_frm_source.Src_UOM_CD as Src_UOM_CD,
exp_pass_frm_source.Src_CURY_CD as Src_CURY_CD,
exp_pass_frm_source.Src_UOM_TYPE_CD as Src_UOM_TYPE_CD,
exp_pass_frm_source.Src_TRANS_STRT_DTTM as Src_TRANS_STRT_DTTM,
exp_pass_frm_source.ins_upd_flag as ins_upd_flag,
exp_pass_frm_source.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_pass_frm_source.out_PRCS_ID as out_PRCS_ID,
exp_pass_frm_source.source_record_id
FROM
exp_pass_frm_source
WHERE exp_pass_frm_source.ins_upd_flag = ''I''
);


-- Component upd_insupd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_insupd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_insupd_Insert.Src_AGMT_ID as Src_AGMT_ID1,
rtr_insupd_Insert.SRC_FEAT_ID as SRC_FEAT_ID1,
rtr_insupd_Insert.Src_COST_MTRC_TYPE_CD as Src_COST_MTRC_TYPE_CD1,
rtr_insupd_Insert.Src_AGMT_FEAT_STRT_DTTM as Src_AGMT_FEAT_STRT_DTTM1,
rtr_insupd_Insert.Src_AGMT_FEAT_ROLE_CD as Src_AGMT_FEAT_ROLE_CD1,
rtr_insupd_Insert.Src_INSRNC_MTRC_TYPE_CD as Src_INSRNC_MTRC_TYPE_CD1,
rtr_insupd_Insert.Src_PLCY_CVGE_MTRC_STRT_DTTM as Src_PLCY_CVGE_MTRC_STRT_DTTM1,
rtr_insupd_Insert.Src_PLCY_CVGE_MTRC_END_DTTM as Src_PLCY_CVGE_MTRC_END_DTTM1,
rtr_insupd_Insert.Src_TM_PRD_CD as Src_TM_PRD_CD1,
rtr_insupd_Insert.Src_PLCY_ASSET_CVGE_AMT as Src_PLCY_ASSET_CVGE_AMT1,
rtr_insupd_Insert.Src_UOM_CD as Src_UOM_CD1,
rtr_insupd_Insert.Src_CURY_CD as Src_CURY_CD1,
rtr_insupd_Insert.Src_UOM_TYPE_CD as Src_UOM_TYPE_CD1,
rtr_insupd_Insert.Src_TRANS_STRT_DTTM as Src_TRANS_STRT_DTTM1,
rtr_insupd_Insert.out_EDW_END_DTTM as out_EDW_END_DTTM1,
rtr_insupd_Insert.out_PRCS_ID as out_PRCS_ID1,
rtr_insupd_Insert.source_record_id as source_record_id  
FROM
rtr_insupd_Insert
);


-- Component exp_pass_to_tgt_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_ins AS
(
SELECT
upd_insupd.Src_AGMT_ID1 as Src_AGMT_ID1,
upd_insupd.SRC_FEAT_ID1 as SRC_FEAT_ID1,
upd_insupd.Src_COST_MTRC_TYPE_CD1 as Src_COST_MTRC_TYPE_CD1,
upd_insupd.Src_AGMT_FEAT_STRT_DTTM1 as Src_AGMT_FEAT_STRT_DTTM1,
upd_insupd.Src_AGMT_FEAT_ROLE_CD1 as Src_AGMT_FEAT_ROLE_CD1,
upd_insupd.Src_INSRNC_MTRC_TYPE_CD1 as Src_INSRNC_MTRC_TYPE_CD1,
upd_insupd.Src_PLCY_CVGE_MTRC_STRT_DTTM1 as Src_PLCY_CVGE_MTRC_STRT_DTTM1,
upd_insupd.Src_PLCY_CVGE_MTRC_END_DTTM1 as Src_PLCY_CVGE_MTRC_END_DTTM1,
upd_insupd.Src_TM_PRD_CD1 as Src_TM_PRD_CD1,
upd_insupd.Src_PLCY_ASSET_CVGE_AMT1 as Src_PLCY_ASSET_CVGE_AMT1,
upd_insupd.Src_UOM_CD1 as Src_UOM_CD1,
upd_insupd.Src_CURY_CD1 as Src_CURY_CD1,
upd_insupd.Src_UOM_TYPE_CD1 as Src_UOM_TYPE_CD1,
upd_insupd.Src_TRANS_STRT_DTTM1 as Src_TRANS_STRT_DTTM1,
upd_insupd.out_EDW_END_DTTM1 as out_EDW_END_DTTM1,
upd_insupd.out_PRCS_ID1 as out_PRCS_ID1,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
upd_insupd.source_record_id
FROM
upd_insupd
);


-- Component PLCY_CVGE_COST_MTRC, Type TARGET 
INSERT INTO db_t_prod_core.PLCY_CVGE_COST_MTRC
(
AGMT_ID,
FEAT_ID,
COST_MTRC_TYPE_CD,
AGMT_FEAT_STRT_DTTM,
AGMT_FEAT_ROLE_CD,
INSRNC_MTRC_TYPE_CD,
PLCY_CVGE_COST_MTRC_STRT_DTTM,
PLCY_CVGE_COST_MTRC_END_DTTM,
TM_PRD_CD,
PLCY_CVGE_COST_AMT,
UOM_CD,
CURY_CD,
UOM_TYPE_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_pass_to_tgt_ins.Src_AGMT_ID1 as AGMT_ID,
exp_pass_to_tgt_ins.SRC_FEAT_ID1 as FEAT_ID,
exp_pass_to_tgt_ins.Src_COST_MTRC_TYPE_CD1 as COST_MTRC_TYPE_CD,
exp_pass_to_tgt_ins.Src_AGMT_FEAT_STRT_DTTM1 as AGMT_FEAT_STRT_DTTM,
exp_pass_to_tgt_ins.Src_AGMT_FEAT_ROLE_CD1 as AGMT_FEAT_ROLE_CD,
exp_pass_to_tgt_ins.Src_INSRNC_MTRC_TYPE_CD1 as INSRNC_MTRC_TYPE_CD,
exp_pass_to_tgt_ins.Src_PLCY_CVGE_MTRC_STRT_DTTM1 as PLCY_CVGE_COST_MTRC_STRT_DTTM,
exp_pass_to_tgt_ins.Src_PLCY_CVGE_MTRC_END_DTTM1 as PLCY_CVGE_COST_MTRC_END_DTTM,
exp_pass_to_tgt_ins.Src_TM_PRD_CD1 as TM_PRD_CD,
exp_pass_to_tgt_ins.Src_PLCY_ASSET_CVGE_AMT1 as PLCY_CVGE_COST_AMT,
exp_pass_to_tgt_ins.Src_UOM_CD1 as UOM_CD,
exp_pass_to_tgt_ins.Src_CURY_CD1 as CURY_CD,
exp_pass_to_tgt_ins.Src_UOM_TYPE_CD1 as UOM_TYPE_CD,
exp_pass_to_tgt_ins.out_PRCS_ID1 as PRCS_ID,
exp_pass_to_tgt_ins.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_tgt_ins.out_EDW_END_DTTM1 as EDW_END_DTTM,
exp_pass_to_tgt_ins.Src_TRANS_STRT_DTTM1 as TRANS_STRT_DTTM
FROM
exp_pass_to_tgt_ins;


END; ';