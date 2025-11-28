-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_QUOTN_ASSET_FEAT_PERIL_MTRC_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE 

start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
P_DEFAULT_STR_CD char;
var_ContactroleTypecode char;
BEGIN 
start_dttm := CURRENT_TIMESTAMP();
end_dttm := CURRENT_TIMESTAMP();
PRCS_ID := 1;  

-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_CLASFCN'' 

             --AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'' ,''pcx_holineschcovitemcov_alfa.ChoiceTerm1'', ''cctl_contentlineitemschedule'')

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'', ''GW'') 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_CMTRCT_ROLE_SBTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_CMTRCT_ROLE_SBTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM=''ASSET_CNTRCT_ROLE_SBTYPE''

         		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_INSRNC_MTRC, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_INSRNC_MTRC AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INSRNC_MTRC_TYPE''

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM=''derived''

			AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_PLCY_SECTION_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_PLCY_SECTION_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PLCY_SECTN_TYPE'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_RTG_PERIL, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_RTG_PERIL AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM=''RTG_PERIL_TYPE''

         		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_pc_Quotn_ast_ft_prl_mtrc, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_Quotn_ast_ft_prl_mtrc AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Jobnumber,
$2 as nk_branchnum,
$3 as FixedID,
$4 as Asset_sbtype,
$5 as Asset_Classification_code,
$6 as Feat_NKsrckey,
$7 as Inscrn_Mtrc_Type_CD,
$8 as Peril_type,
$9 as Amount,
$10 as Earnings_as_of_dt,
$11 as TRANS_STRT_DTTM,
$12 as cury_cd,
$13 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT  DISTINCT pc_quotn_ast_ft_prl_mtrc.JobNumber,  pc_quotn_ast_ft_prl_mtrc.branchnumber,

	        pc_quotn_ast_ft_prl_mtrc.Prty_asset_id, pc_quotn_ast_ft_prl_mtrc.assettype,

	        case when assettype=''PRTY_ASSET_SBTYPE5'' THEN ''PRTY_ASSET_CLASFCN1''

	                else pc_quotn_ast_ft_prl_mtrc.class_code end as classification_code,

	        pc_quotn_ast_ft_prl_mtrc.Cov_Type_CD,

	        pc_Quotn_ast_ft_prl_mtrc.Inscrn_Mtrc_Type_CD,

	        pc_quotn_ast_ft_prl_mtrc.Peril_Type,

	        sum(pc_quotn_ast_ft_prl_mtrc.amount) as Amount,

	        pc_quotn_ast_ft_prl_mtrc.EditEffectiveDate, pc_quotn_ast_ft_prl_mtrc.UpdateTime,

	        ''ISO_CURY_TYPE1'' as cury_cd

	        

	FROM(

	select distinct 

	JobNumber,

	branchnumber,

	cast(Prty_asset_id as varchar(100)) as prty_asset_id,Cov_Type_CD,

	cast((case when (Cov_Type_CD like (''%SpecificOtherStructure%'') or Cov_Type_CD like ''HODW%'' or Cov_Type_CD like ''HOSI%'') then ''PRTY_ASSET_SBTYPE5'' 

	when (Cov_Type_CD like (''%ScheduledProperty%'') or (Cov_Type_CD like ''HOLI%'')) then ''PRTY_ASSET_SBTYPE7'' 

	else NULL  end) as varchar(50))as assettype,class_code,cast(''INSRNC_MTRC_TYPE16'' as varchar(100)) as Inscrn_Mtrc_Type_CD,

	Peril_Type,

	EditEffectiveDate,

	UpdateTime,amount

	from

	(

	select distinct 

	 pc_policyperiod.PublicID_stg as policyperiodid, 

	Coverable_FixedID as Prty_asset_id,Table_Name_For_FixedID,class_code_stg as class_code,

	''GWPC'' as asset_src_cd_stg,ExpandedHOTable.Coverable_CovPattern as Cov_Type_CD,

	HOPerilType.TYPECODE_stg as Peril_Type,

	pc_policyperiod.UpdateTime_stg  as UpdateTime,

	pc_policyperiod.EditEffectiveDate_stg as EditEffectiveDate,

	pctl_policyperiodstatus.TYPECODE_stg as Typecode,

	pc_job.JobNumber_stg as Jobnumber,

	pc_policyperiod.branchnumber_stg as branchnumber,

	pcx_hotransaction_hoe.amount_stg as amount,

    ''INSRNC_MTRC_TYPE16''  as Inscrn_Mtrc_Type_CD 

	from DB_T_PROD_STAG.pc_policyperiod 

	    inner join DB_T_PROD_STAG.pcx_hotransaction_hoe on pcx_hotransaction_hoe.BranchID_stg = pc_policyperiod.ID_stg

	    and (pcx_hotransaction_hoe.ExpirationDate_stg is NULL or pcx_hotransaction_hoe.ExpirationDate_stg > pc_policyperiod.EditEffectiveDate_stg)

	   inner join

	  (

	  select distinct

	  case when pcx_homeownerscost_hoe.DwellingCov_stg is not null then ''pcx_dwelling_hoe''

	   when pcx_homeownerscost_hoe.SchedItemCov_stg is not null then ''pcx_holineschedcovitem_alfa''   

	  end as Table_Name_For_FixedID,

	

	  case when pcx_homeownerscost_hoe.DwellingCov_stg is not null then pcx_dwelling_hoe.FixedID_stg

	   when pcx_homeownerscost_hoe.SchedItemCov_stg is not null then pcx_holineschedcovitem_alfa.FixedID_stg   

	  end as Coverable_FixedID,

	  

	    case when pcx_homeownerscost_hoe.DwellingCov_stg is not null then DwellingCovPattern.PatternID_stg

	   when pcx_homeownerscost_hoe.SchedItemCov_stg is not null then SchedItemCovPattern.PatternID_stg     

	  end as Coverable_CovPattern,

	  

	  pcx_holineschcovitemcov_alfa.ExpirationDate_stg as HOLI_Expdt,

	  pcx_dwellingcov_hoe.ExpirationDate_stg as DWL_Expdt,    

	  pcx_holineschcovitemcov_alfa.ChoiceTerm1_stg as class_code_stg,  

	  pcx_homeownerscost_hoe.branchid_stg, pcx_homeownerscost_hoe.DwellingCov_stg, pcx_homeownerscost_hoe.SchedItemCov_stg,pcx_homeownerscost_hoe.ID_stg,

	  pcx_homeownerscost_hoe.PerilType_alfa_stg

	

	  from DB_T_PROD_STAG.pcx_homeownerscost_hoe

	   /*Asset level coverages*/

	   left join DB_T_PROD_STAG.pcx_dwellingcov_hoe on pcx_homeownerscost_hoe.DwellingCov_stg  = pcx_dwellingcov_hoe.FixedID_stg 

	   and pcx_dwellingcov_hoe.BranchID_stg = pcx_homeownerscost_hoe.branchid_stg

	    left join DB_T_PROD_STAG.pcx_dwelling_hoe on pcx_dwellingcov_hoe.Dwelling_stg = pcx_dwelling_hoe.ID_stg

	    left join DB_T_PROD_STAG.pc_etlclausepattern DwellingCovPattern on pcx_dwellingcov_hoe.PatternCode_stg = DwellingCovPattern.PatternID_stg

	

	   left join DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa on pcx_homeownerscost_hoe.SchedItemCov_stg  = pcx_holineschcovitemcov_alfa.FixedID_stg and 

	   pcx_holineschcovitemcov_alfa.BranchID_stg = pcx_homeownerscost_hoe.branchid_stg

	    left join DB_T_PROD_STAG.pcx_holineschedcovitem_alfa on pcx_holineschcovitemcov_alfa.HOLineSchCovItem_stg = pcx_holineschedcovitem_alfa.ID_stg

	    left join DB_T_PROD_STAG.pc_etlclausepattern SchedItemCovPattern on pcx_holineschcovitemcov_alfa.PatternCode_stg = SchedItemCovPattern.PatternID_stg

	    

	    where pcx_homeownerscost_hoe.DwellingCov_stg is not NULL or  (pcx_homeownerscost_hoe.SchedItemCov_stg is not NULL) 

	    

	  ) ExpandedHOTable on pcx_hotransaction_hoe.HomeownersCost_stg = ExpandedHOTable.ID_stg

	  

	  join DB_T_PROD_STAG.pctl_periltype_alfa HOPerilType on ExpandedHOTable.PerilType_alfa_stg = HOPerilType.ID_stg 

	  left join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.ID_stg=pc_policyperiod.Status_stg

	  inner join DB_T_PROD_STAG.pc_job on pc_policyperiod.JobID_stg = pc_job.ID_stg

	  inner join DB_T_PROD_STAG.pctl_job on pc_job.Subtype_stg = pctl_job.ID_stg   

	   

	where  HOPerilType.TYPECODE_stg is not null

	and( HOLI_Expdt is NULL or HOLI_Expdt > pc_policyperiod.EditEffectiveDate_stg) 

	    and ( DWL_Expdt is NULL or DWL_Expdt > pc_policyperiod.EditEffectiveDate_stg)  

	and pc_policyperiod.updatetime_stg > (:start_dttm)

	and  pc_policyperiod.updatetime_stg <= (:end_dttm) 

	

	)a

	)

	pc_quotn_ast_ft_prl_mtrc

	group by jobnumber,branchnumber,prty_asset_id,assettype,classification_code,cov_type_cd,inscrn_mtrc_type_CD,peril_type,editeffectivedate,updatetime
) SRC
)
);


-- Component exp_pass_frm_src, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_frm_src AS
(
SELECT
SQ_pc_Quotn_ast_ft_prl_mtrc.Jobnumber as Jobnumber,
SQ_pc_Quotn_ast_ft_prl_mtrc.Feat_NKsrckey as Feat_NKsrckey,
SQ_pc_Quotn_ast_ft_prl_mtrc.FixedID as FixedID,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ as o_Asset_sbtype,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ as o_Asset_Classification_code,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CMTRCT_ROLE_SBTYPE */ as ASSET_CNTRCT_ROLE_SBTYPE_CD,
SQ_pc_Quotn_ast_ft_prl_mtrc.Earnings_as_of_dt as Earnings_as_of_dt1,
CASE WHEN LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PLCY_SECTION_TYPE */ IS NULL THEN ''UNK'' ELSE LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PLCY_SECTION_TYPE */ END as Section_type1,
LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INSRNC_MTRC */ as o_Inscrn_Mtrc_Type_CD,
SQ_pc_Quotn_ast_ft_prl_mtrc.Amount as Amount,
CASE WHEN SQ_pc_Quotn_ast_ft_prl_mtrc.TRANS_STRT_DTTM IS NULL THEN TO_TIMESTAMP ( ''1900-01-01'' , ''yyyy-mm-dd'' ) ELSE SQ_pc_Quotn_ast_ft_prl_mtrc.TRANS_STRT_DTTM END as TRANS_STRT_DTTM1,
CAST(''9999-12-31 23:59:59.999999'' AS TIMESTAMP)
  AS TRANS_END_DTTM1,
CASE WHEN LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_RTG_PERIL */ IS NULL THEN ''UNK'' ELSE LKP_8.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_RTG_PERIL */ END as in_RTG_PERIL_TYPE_CD,
SQ_pc_Quotn_ast_ft_prl_mtrc.Earnings_as_of_dt as Earnings_as_of_dt,
SQ_pc_Quotn_ast_ft_prl_mtrc.nk_branchnum as nk_branchnum,
SQ_pc_Quotn_ast_ft_prl_mtrc.cury_cd as cury_cd,
SQ_pc_Quotn_ast_ft_prl_mtrc.source_record_id,
row_number() over (partition by SQ_pc_Quotn_ast_ft_prl_mtrc.source_record_id order by SQ_pc_Quotn_ast_ft_prl_mtrc.source_record_id) as RNK
FROM
SQ_pc_Quotn_ast_ft_prl_mtrc
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_pc_Quotn_ast_ft_prl_mtrc.Asset_sbtype
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_pc_Quotn_ast_ft_prl_mtrc.Asset_Classification_code
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CMTRCT_ROLE_SBTYPE LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = ''ASSET_CNTRCT_ROLE_SBTYPE1''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_PLCY_SECTION_TYPE LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = ''Section_type''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_PLCY_SECTION_TYPE LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = ''Section_type''
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_INSRNC_MTRC LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = SQ_pc_Quotn_ast_ft_prl_mtrc.Inscrn_Mtrc_Type_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_RTG_PERIL LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = SQ_pc_Quotn_ast_ft_prl_mtrc.Peril_type
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_RTG_PERIL LKP_8 ON LKP_8.SRC_IDNTFTN_VAL = SQ_pc_Quotn_ast_ft_prl_mtrc.Peril_type
QUALIFY RNK = 1
);


-- Component LKP_INSRNC_QUOTN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRNC_QUOTN AS
(
SELECT
LKP.QUOTN_ID,
exp_pass_frm_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_frm_src.source_record_id ORDER BY LKP.QUOTN_ID asc) RNK
FROM
exp_pass_frm_src
LEFT JOIN (
SELECT INSRNC_QUOTN.QUOTN_ID AS QUOTN_ID, INSRNC_QUOTN.NK_JOB_NBR AS NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR AS VERS_NBR FROM DB_T_PROD_CORE.INSRNC_QUOTN
QUALIFY ROW_NUMBER() OVER(PARTITION BY  INSRNC_QUOTN.NK_JOB_NBR, INSRNC_QUOTN.VERS_NBR,  INSRNC_QUOTN.SRC_SYS_CD  ORDER BY INSRNC_QUOTN.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_JOB_NBR = exp_pass_frm_src.Jobnumber AND LKP.VERS_NBR = exp_pass_frm_src.nk_branchnum
QUALIFY RNK = 1
);


-- Component LKP_FEAT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_FEAT AS
(
SELECT
LKP.FEAT_ID,
exp_pass_frm_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_frm_src.source_record_id ORDER BY LKP.FEAT_ID desc,LKP.FEAT_SBTYPE_CD desc,LKP.NK_SRC_KEY desc,LKP.FEAT_INSRNC_SBTYPE_CD desc,LKP.FEAT_CLASFCN_CD desc,LKP.FEAT_DESC desc,LKP.FEAT_NAME desc,LKP.COMN_FEAT_NAME desc,LKP.FEAT_LVL_SBTYPE_CNT desc,LKP.INSRNC_CVGE_TYPE_CD desc,LKP.INSRNC_LOB_TYPE_CD desc,LKP.PRCS_ID desc) RNK
FROM
exp_pass_frm_src
LEFT JOIN (
SELECT FEAT.FEAT_ID as FEAT_ID, FEAT.FEAT_INSRNC_SBTYPE_CD as FEAT_INSRNC_SBTYPE_CD, FEAT.FEAT_CLASFCN_CD as FEAT_CLASFCN_CD, FEAT.FEAT_DESC as FEAT_DESC, FEAT.FEAT_NAME as FEAT_NAME, FEAT.COMN_FEAT_NAME as COMN_FEAT_NAME, FEAT.FEAT_LVL_SBTYPE_CNT as FEAT_LVL_SBTYPE_CNT, FEAT.INSRNC_CVGE_TYPE_CD as INSRNC_CVGE_TYPE_CD, FEAT.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD, FEAT.PRCS_ID as PRCS_ID, FEAT.FEAT_SBTYPE_CD as FEAT_SBTYPE_CD, FEAT.NK_SRC_KEY as NK_SRC_KEY FROM DB_T_PROD_CORE.FEAT
QUALIFY ROW_NUMBER () OVER (PARTITION BY NK_SRC_KEY,FEAT_SBTYPE_CD  ORDER BY edw_end_dttm DESC)=1
) LKP ON LKP.FEAT_SBTYPE_CD = o_Asset_sbtype
--in_FEAT_SBTYPE_CD 
AND LKP.NK_SRC_KEY = exp_pass_frm_src.Feat_NKsrckey
QUALIFY RNK = 1
);


-- Component LKP_PRTY_ASSET_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_ASSET_ID AS
(
SELECT
LKP.PRTY_ASSET_ID,
exp_pass_frm_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_frm_src.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_HOST_ID_VAL asc,LKP.PRTY_ASSET_SBTYPE_CD asc,LKP.PRTY_ASSET_CLASFCN_CD asc,LKP.ASSET_INSRNC_HIST_TYPE_CD asc,LKP.ASSET_DESC asc,LKP.PRTY_ASSET_NAME asc,LKP.PRTY_ASSET_STRT_DTTM asc,LKP.PRTY_ASSET_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc) RNK
FROM
exp_pass_frm_src
LEFT JOIN (
SELECT PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET.ASSET_INSRNC_HIST_TYPE_CD as ASSET_INSRNC_HIST_TYPE_CD, PRTY_ASSET.ASSET_DESC as ASSET_DESC, PRTY_ASSET.PRTY_ASSET_NAME as PRTY_ASSET_NAME, PRTY_ASSET.PRTY_ASSET_STRT_DTTM as PRTY_ASSET_STRT_DTTM, PRTY_ASSET.PRTY_ASSET_END_DTTM as PRTY_ASSET_END_DTTM, PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM, PRTY_ASSET.SRC_SYS_CD as SRC_SYS_CD, PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL, PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD, PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD 
FROM DB_T_PROD_CORE.PRTY_ASSET 
QUALIFY ROW_NUMBER() OVER(PARTITION BY  ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,PRTY_ASSET_CLASFCN_CD ORDER BY EDW_END_DTTM DESC) = 1
) LKP ON LKP.ASSET_HOST_ID_VAL = exp_pass_frm_src.FixedID AND LKP.PRTY_ASSET_SBTYPE_CD = exp_pass_frm_src.o_Asset_sbtype AND LKP.PRTY_ASSET_CLASFCN_CD = exp_pass_frm_src.o_Asset_Classification_code
QUALIFY RNK = 1
);


-- Component LKP_TERADATA_ETL_REF_XLAT_CURY_CD, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_CURY_CD AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
exp_pass_frm_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_frm_src.source_record_id ORDER BY LKP.TGT_IDNTFTN_VAL desc,LKP.SRC_IDNTFTN_VAL desc) RNK
FROM
exp_pass_frm_src
LEFT JOIN (
SELECT 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL
	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 
FROM 
	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''ISO_CURY_TYPE''
             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM=''derived''
			AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
) LKP ON LKP.SRC_IDNTFTN_VAL = exp_pass_frm_src.cury_cd
QUALIFY RNK = 1
);


-- Component LKP_QUONT_ASSET_FEAT_PERIL_MTRC, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_QUONT_ASSET_FEAT_PERIL_MTRC AS
(
SELECT
LKP.PRTY_ASSET_ID,
LKP.QUOTN_ID,
LKP.FEAT_ID,
LKP.QUOTN_ASSET_FEAT_STRT_DTTM,
LKP.RTG_PERIL_TYPE_CD,
LKP.INSRNC_MTRC_TYPE_CD,
LKP.QAF_PERIL_MTRC_STRT_DTTM,
LKP.QAF_PERIL_MTRC_AMT,
LKP.EDW_STRT_DTTM,
exp_pass_frm_src.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_frm_src.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.QUOTN_ID asc,LKP.FEAT_ID asc,LKP.QUOTN_ASSET_FEAT_STRT_DTTM asc,LKP.RTG_PERIL_TYPE_CD asc,LKP.INSRNC_MTRC_TYPE_CD asc,LKP.QAF_PERIL_MTRC_STRT_DTTM asc,LKP.QAF_PERIL_MTRC_AMT asc,LKP.EDW_STRT_DTTM asc) RNK1
FROM
exp_pass_frm_src
INNER JOIN LKP_INSRNC_QUOTN ON exp_pass_frm_src.source_record_id = LKP_INSRNC_QUOTN.source_record_id
INNER JOIN LKP_FEAT ON LKP_INSRNC_QUOTN.source_record_id = LKP_FEAT.source_record_id
INNER JOIN LKP_PRTY_ASSET_ID ON LKP_FEAT.source_record_id = LKP_PRTY_ASSET_ID.source_record_id
LEFT JOIN (
SELECT QUOTN_ASSET_FEAT_PERIL_MTRC.PRTY_ASSET_ID as PRTY_ASSET_ID, QUOTN_ASSET_FEAT_PERIL_MTRC.QUOTN_ID as QUOTN_ID,QUOTN_ASSET_FEAT_PERIL_MTRC.FEAT_ID as FEAT_ID,QUOTN_ASSET_FEAT_PERIL_MTRC.QUOTN_ASSET_FEAT_STRT_DTTM as QUOTN_ASSET_FEAT_STRT_DTTM, QUOTN_ASSET_FEAT_PERIL_MTRC.RTG_PERIL_TYPE_CD as RTG_PERIL_TYPE_CD, QUOTN_ASSET_FEAT_PERIL_MTRC.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD, QUOTN_ASSET_FEAT_PERIL_MTRC.QAF_PERIL_MTRC_STRT_DTTM as QAF_PERIL_MTRC_STRT_DTTM, QUOTN_ASSET_FEAT_PERIL_MTRC.QAF_PERIL_MTRC_AMT as QAF_PERIL_MTRC_AMT, QUOTN_ASSET_FEAT_PERIL_MTRC.EDW_STRT_DTTM as EDW_STRT_DTTM  FROM DB_T_PROD_CORE.QUOTN_ASSET_FEAT_PERIL_MTRC QUOTN_ASSET_FEAT_PERIL_MTRC QUALIFY ROW_NUMBER() OVER(PARTITION BY   INSRNC_MTRC_TYPE_CD,RTG_PERIL_TYPE_CD,FEAT_ID,QUOTN_ID,PRTY_ASSET_ID ORDER BY QUOTN_ASSET_FEAT_PERIL_MTRC.EDW_END_DTTM DESC) = 1
) LKP ON LKP.QUOTN_ID = LKP_INSRNC_QUOTN.QUOTN_ID AND LKP.PRTY_ASSET_ID = LKP_PRTY_ASSET_ID.PRTY_ASSET_ID AND LKP.FEAT_ID = LKP_FEAT.FEAT_ID AND LKP.INSRNC_MTRC_TYPE_CD = exp_pass_frm_src.o_Inscrn_Mtrc_Type_CD AND LKP.RTG_PERIL_TYPE_CD = exp_pass_frm_src.in_RTG_PERIL_TYPE_CD
QUALIFY RNK1 = 1
);


-- Component exp_ins_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_upd AS
(
SELECT
LKP_QUONT_ASSET_FEAT_PERIL_MTRC.QUOTN_ID as lkp_QUOTN_ID,
LKP_QUONT_ASSET_FEAT_PERIL_MTRC.QAF_PERIL_MTRC_STRT_DTTM as lkp_QAF_PERIL_MTRC_DTTM,
LKP_QUONT_ASSET_FEAT_PERIL_MTRC.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_QUONT_ASSET_FEAT_PERIL_MTRC.FEAT_ID as lkp_FEAT_ID,
LKP_QUONT_ASSET_FEAT_PERIL_MTRC.PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
LKP_QUONT_ASSET_FEAT_PERIL_MTRC.RTG_PERIL_TYPE_CD as lkp_RTG_PERIL_TYPE_CD,
LKP_QUONT_ASSET_FEAT_PERIL_MTRC.INSRNC_MTRC_TYPE_CD as lkp_INSRNC_MTRC_TYPE_CD,
LKP_QUONT_ASSET_FEAT_PERIL_MTRC.QUOTN_ASSET_FEAT_STRT_DTTM as lkp_QUOTN_ASSET_FEAT_STRT_DTTM,
MD5 ( LKP_QUONT_ASSET_FEAT_PERIL_MTRC.QAF_PERIL_MTRC_AMT || TO_CHAR ( LKP_QUONT_ASSET_FEAT_PERIL_MTRC.QUOTN_ASSET_FEAT_STRT_DTTM ) ) as lkp_checksum,
LKP_FEAT.FEAT_ID as in_FEAT_ID,
LKP_PRTY_ASSET_ID.PRTY_ASSET_ID as in_PRTY_ASSET_ID,
LKP_INSRNC_QUOTN.QUOTN_ID as QUOTN_ID,
exp_pass_frm_src.o_Inscrn_Mtrc_Type_CD as in_Inscrn_Mtrc_Type_CD,
exp_pass_frm_src.in_RTG_PERIL_TYPE_CD as in_RTG_PERIL_TYPE_CD,
exp_pass_frm_src.ASSET_CNTRCT_ROLE_SBTYPE_CD as in_ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_pass_frm_src.Earnings_as_of_dt1 as Earnings_as_of_dt1,
exp_pass_frm_src.Section_type1 as Section_type,
exp_pass_frm_src.Earnings_as_of_dt as Earnings_as_of_dt,
exp_pass_frm_src.Amount as Amount,
MD5 ( exp_pass_frm_src.Amount || TO_CHAR ( exp_pass_frm_src.Earnings_as_of_dt ) ) as in_checksum,
exp_pass_frm_src.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_pass_frm_src.TRANS_END_DTTM1 as TRANS_END_DTTM,
CASE WHEN lkp_checksum IS NULL THEN ''I'' ELSE ( CASE WHEN lkp_checksum <> in_checksum THEN ''U'' ELSE ''R'' END ) END as ins_upd_flag,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) as AGMT_ASSET_STRT_DTTM,
:PRCS_ID as PRCS_ID,
LKP_TERADATA_ETL_REF_XLAT_CURY_CD.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL,
exp_pass_frm_src.source_record_id
FROM
exp_pass_frm_src
INNER JOIN LKP_INSRNC_QUOTN ON exp_pass_frm_src.source_record_id = LKP_INSRNC_QUOTN.source_record_id
INNER JOIN LKP_FEAT ON LKP_INSRNC_QUOTN.source_record_id = LKP_FEAT.source_record_id
INNER JOIN LKP_PRTY_ASSET_ID ON LKP_FEAT.source_record_id = LKP_PRTY_ASSET_ID.source_record_id
INNER JOIN LKP_TERADATA_ETL_REF_XLAT_CURY_CD ON LKP_PRTY_ASSET_ID.source_record_id = LKP_TERADATA_ETL_REF_XLAT_CURY_CD.source_record_id
INNER JOIN LKP_QUONT_ASSET_FEAT_PERIL_MTRC ON LKP_TERADATA_ETL_REF_XLAT_CURY_CD.source_record_id = LKP_QUONT_ASSET_FEAT_PERIL_MTRC.source_record_id
);


-- Component RTRTRANS_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE RTRTRANS_INSERT AS
(SELECT
exp_ins_upd.lkp_QUOTN_ID as lkp_QUOTN_ID,
exp_ins_upd.lkp_FEAT_ID as lkp_FEAT_ID,
exp_ins_upd.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_ins_upd.lkp_RTG_PERIL_TYPE_CD as lkp_RTG_PERIL_TYPE_CD,
exp_ins_upd.lkp_INSRNC_MTRC_TYPE_CD as lkp_INSRNC_MTRC_TYPE_CD,
exp_ins_upd.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_ins_upd.in_FEAT_ID as in_FEAT_ID,
exp_ins_upd.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_ins_upd.QUOTN_ID as QUOTN_ID,
exp_ins_upd.in_Inscrn_Mtrc_Type_CD as in_Inscrn_Mtrc_Type_CD,
exp_ins_upd.in_RTG_PERIL_TYPE_CD as in_RTG_PERIL_TYPE_CD,
exp_ins_upd.in_ASSET_CNTRCT_ROLE_SBTYPE_CD as in_ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd.Earnings_as_of_dt1 as Earnings_as_of_dt4,
exp_ins_upd.Section_type as Section_type,
exp_ins_upd.Earnings_as_of_dt as Earnings_as_of_dt,
exp_ins_upd.Amount as Amount,
exp_ins_upd.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_ins_upd.TRANS_END_DTTM as TRANS_END_DTTM,
exp_ins_upd.ins_upd_flag as ins_upd_flag,
exp_ins_upd.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins_upd.EDW_END_DTTM as EDW_END_DTTM,
exp_ins_upd.PRCS_ID as PRCS_ID,
exp_ins_upd.lkp_QAF_PERIL_MTRC_DTTM as lkp_QAF_PERIL_MTRC_DTTM,
exp_ins_upd.lkp_QUOTN_ASSET_FEAT_STRT_DTTM as lkp_QUOTN_ASSET_FEAT_STRT_DTTM,
exp_ins_upd.AGMT_ASSET_STRT_DTTM as AGMT_ASSET_STRT_DTTM,
exp_ins_upd.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.ins_upd_flag = ''I'' and exp_ins_upd.QUOTN_ID IS NOT NULL and exp_ins_upd.in_FEAT_ID IS NOT NULL and exp_ins_upd.in_PRTY_ASSET_ID IS NOT NULL);


-- Component RTRTRANS_UPDATE, Type ROUTER Output Group UPDATE
CREATE OR REPLACE TEMPORARY TABLE RTRTRANS_UPDATE AS
(SELECT
exp_ins_upd.lkp_QUOTN_ID as lkp_QUOTN_ID,
exp_ins_upd.lkp_FEAT_ID as lkp_FEAT_ID,
exp_ins_upd.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_ins_upd.lkp_RTG_PERIL_TYPE_CD as lkp_RTG_PERIL_TYPE_CD,
exp_ins_upd.lkp_INSRNC_MTRC_TYPE_CD as lkp_INSRNC_MTRC_TYPE_CD,
exp_ins_upd.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_ins_upd.in_FEAT_ID as in_FEAT_ID,
exp_ins_upd.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_ins_upd.QUOTN_ID as QUOTN_ID,
exp_ins_upd.in_Inscrn_Mtrc_Type_CD as in_Inscrn_Mtrc_Type_CD,
exp_ins_upd.in_RTG_PERIL_TYPE_CD as in_RTG_PERIL_TYPE_CD,
exp_ins_upd.in_ASSET_CNTRCT_ROLE_SBTYPE_CD as in_ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins_upd.Earnings_as_of_dt1 as Earnings_as_of_dt4,
exp_ins_upd.Section_type as Section_type,
exp_ins_upd.Earnings_as_of_dt as Earnings_as_of_dt,
exp_ins_upd.Amount as Amount,
exp_ins_upd.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_ins_upd.TRANS_END_DTTM as TRANS_END_DTTM,
exp_ins_upd.ins_upd_flag as ins_upd_flag,
exp_ins_upd.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins_upd.EDW_END_DTTM as EDW_END_DTTM,
exp_ins_upd.PRCS_ID as PRCS_ID,
exp_ins_upd.lkp_QAF_PERIL_MTRC_DTTM as lkp_QAF_PERIL_MTRC_DTTM,
exp_ins_upd.lkp_QUOTN_ASSET_FEAT_STRT_DTTM as lkp_QUOTN_ASSET_FEAT_STRT_DTTM,
exp_ins_upd.AGMT_ASSET_STRT_DTTM as AGMT_ASSET_STRT_DTTM,
exp_ins_upd.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL,
exp_ins_upd.source_record_id
FROM
exp_ins_upd
WHERE exp_ins_upd.ins_upd_flag = ''U'' and exp_ins_upd.QUOTN_ID IS NOT NULL and exp_ins_upd.in_FEAT_ID IS NOT NULL and exp_ins_upd.in_PRTY_ASSET_ID IS NOT NULL);


-- Component QUOTN_ASSET_FEAT_PERIL_MTRC_ins_new, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_ASSET_FEAT_PERIL_MTRC
(
PRTY_ASSET_ID,
QUOTN_ID,
ASSET_CNTRCT_ROLE_SBTYPE_CD,
QUOTN_ASSET_STRT_DTTM,
FEAT_ID,
QUOTN_ASSET_FEAT_STRT_DTTM,
RTG_PERIL_TYPE_CD,
QAF_PERIL_STRT_DTTM,
INSRNC_MTRC_TYPE_CD,
QAF_PERIL_MTRC_STRT_DTTM,
QAF_PERIL_MTRC_END_DTTM,
QAF_PERIL_MTRC_AMT,
CURY_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
RTRTRANS_INSERT.in_PRTY_ASSET_ID as PRTY_ASSET_ID,
RTRTRANS_INSERT.QUOTN_ID as QUOTN_ID,
RTRTRANS_INSERT.in_ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
RTRTRANS_INSERT.AGMT_ASSET_STRT_DTTM as QUOTN_ASSET_STRT_DTTM,
RTRTRANS_INSERT.in_FEAT_ID as FEAT_ID,
RTRTRANS_INSERT.Earnings_as_of_dt4 as QUOTN_ASSET_FEAT_STRT_DTTM,
RTRTRANS_INSERT.in_RTG_PERIL_TYPE_CD as RTG_PERIL_TYPE_CD,
RTRTRANS_INSERT.AGMT_ASSET_STRT_DTTM as QAF_PERIL_STRT_DTTM,
RTRTRANS_INSERT.in_Inscrn_Mtrc_Type_CD as INSRNC_MTRC_TYPE_CD,
RTRTRANS_INSERT.AGMT_ASSET_STRT_DTTM as QAF_PERIL_MTRC_STRT_DTTM,
RTRTRANS_INSERT.EDW_END_DTTM as QAF_PERIL_MTRC_END_DTTM,
RTRTRANS_INSERT.Amount as QAF_PERIL_MTRC_AMT,
RTRTRANS_INSERT.TGT_IDNTFTN_VAL as CURY_CD,
RTRTRANS_INSERT.PRCS_ID as PRCS_ID,
RTRTRANS_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM,
RTRTRANS_INSERT.EDW_END_DTTM as EDW_END_DTTM,
RTRTRANS_INSERT.TRANS_STRT_DTTM as TRANS_STRT_DTTM
FROM
RTRTRANS_INSERT;


-- Component exp_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_upd AS
(
SELECT
RTRTRANS_UPDATE.lkp_QUOTN_ID as lkp_QUOTN_ID,
RTRTRANS_UPDATE.lkp_FEAT_ID as lkp_FEAT_ID3,
RTRTRANS_UPDATE.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID3,
RTRTRANS_UPDATE.lkp_RTG_PERIL_TYPE_CD as lkp_RTG_PERIL_TYPE_CD3,
RTRTRANS_UPDATE.lkp_INSRNC_MTRC_TYPE_CD as lkp_INSRNC_MTRC_TYPE_CD3,
RTRTRANS_UPDATE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
DATEADD (SECOND, -1, RTRTRANS_UPDATE.EDW_STRT_DTTM) as EDW_END_DTTM,
DATEADD (SECOND, -1, RTRTRANS_UPDATE.TRANS_STRT_DTTM) as TRANS_END_DTTM,
RTRTRANS_UPDATE.lkp_QAF_PERIL_MTRC_DTTM as lkp_QAF_PERIL_MTRC_DTTM,
RTRTRANS_UPDATE.lkp_QUOTN_ASSET_FEAT_STRT_DTTM as lkp_QUOTN_ASSET_FEAT_STRT_DTTM,
RTRTRANS_UPDATE.source_record_id
FROM
RTRTRANS_UPDATE
);


-- Component exp_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins AS
(
SELECT
RTRTRANS_UPDATE.QUOTN_ID as QUOTN_ID,
RTRTRANS_UPDATE.in_FEAT_ID as FEAT_ID,
RTRTRANS_UPDATE.in_PRTY_ASSET_ID as PRTY_ASSET_ID,
RTRTRANS_UPDATE.in_ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
RTRTRANS_UPDATE.Earnings_as_of_dt4 as Earnings_as_of_dt,
RTRTRANS_UPDATE.in_RTG_PERIL_TYPE_CD as RTG_PERIL_TYPE_CD,
RTRTRANS_UPDATE.Earnings_as_of_dt4 as Earnings_as_of_dt1,
RTRTRANS_UPDATE.Earnings_as_of_dt4 as Earnings_as_of_dt2,
RTRTRANS_UPDATE.Section_type as AGMT_SECTN_CD,
RTRTRANS_UPDATE.in_Inscrn_Mtrc_Type_CD as INSRNC_MTRC_TYPE_CD,
RTRTRANS_UPDATE.Earnings_as_of_dt as Earnings_as_of_dt3,
RTRTRANS_UPDATE.Amount as AGMT_ASSET_FEAT_PERIL_AMT,
RTRTRANS_UPDATE.PRCS_ID as PRCS_ID,
RTRTRANS_UPDATE.EDW_STRT_DTTM as EDW_STRT_DTTM,
RTRTRANS_UPDATE.EDW_END_DTTM as EDW_END_DTTM,
RTRTRANS_UPDATE.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
RTRTRANS_UPDATE.TRANS_END_DTTM as TRANS_END_DTTM,
RTRTRANS_UPDATE.AGMT_ASSET_STRT_DTTM as AGMT_ASSET_STRT_DTTM3,
RTRTRANS_UPDATE.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL3,
RTRTRANS_UPDATE.source_record_id
FROM
RTRTRANS_UPDATE
);


-- Component upd_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_ins.QUOTN_ID as QUOTN_ID,
exp_ins.FEAT_ID as FEAT_ID,
exp_ins.PRTY_ASSET_ID as PRTY_ASSET_ID,
exp_ins.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
exp_ins.Earnings_as_of_dt as Earnings_as_of_dt,
exp_ins.RTG_PERIL_TYPE_CD as RTG_PERIL_TYPE_CD,
exp_ins.Earnings_as_of_dt1 as Earnings_as_of_dt1,
exp_ins.Earnings_as_of_dt2 as Earnings_as_of_dt2,
exp_ins.AGMT_SECTN_CD as AGMT_SECTN_CD,
exp_ins.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
exp_ins.Earnings_as_of_dt3 as Earnings_as_of_dt3,
exp_ins.AGMT_ASSET_FEAT_PERIL_AMT as AGMT_ASSET_FEAT_PERIL_AMT,
exp_ins.PRCS_ID as PRCS_ID,
exp_ins.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins.EDW_END_DTTM as EDW_END_DTTM,
exp_ins.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_ins.TRANS_END_DTTM as TRANS_END_DTTM,
exp_ins.AGMT_ASSET_STRT_DTTM3 as AGMT_ASSET_STRT_DTTM3,
exp_ins.TGT_IDNTFTN_VAL3 as TGT_IDNTFTN_VAL3,
0 as UPDATE_STRATEGY_ACTION,exp_ins.source_record_id
FROM
exp_ins
);


-- Component QUOTN_ASSET_FEAT_PERIL_MTRC_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.QUOTN_ASSET_FEAT_PERIL_MTRC
(
PRTY_ASSET_ID,
QUOTN_ID,
ASSET_CNTRCT_ROLE_SBTYPE_CD,
QUOTN_ASSET_STRT_DTTM,
FEAT_ID,
QUOTN_ASSET_FEAT_STRT_DTTM,
RTG_PERIL_TYPE_CD,
QAF_PERIL_STRT_DTTM,
INSRNC_MTRC_TYPE_CD,
QAF_PERIL_MTRC_STRT_DTTM,
QAF_PERIL_MTRC_END_DTTM,
QAF_PERIL_MTRC_AMT,
CURY_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
upd_ins.PRTY_ASSET_ID as PRTY_ASSET_ID,
upd_ins.QUOTN_ID as QUOTN_ID,
upd_ins.ASSET_CNTRCT_ROLE_SBTYPE_CD as ASSET_CNTRCT_ROLE_SBTYPE_CD,
upd_ins.AGMT_ASSET_STRT_DTTM3 as QUOTN_ASSET_STRT_DTTM,
upd_ins.FEAT_ID as FEAT_ID,
upd_ins.Earnings_as_of_dt as QUOTN_ASSET_FEAT_STRT_DTTM,
upd_ins.RTG_PERIL_TYPE_CD as RTG_PERIL_TYPE_CD,
upd_ins.AGMT_ASSET_STRT_DTTM3 as QAF_PERIL_STRT_DTTM,
upd_ins.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
upd_ins.AGMT_ASSET_STRT_DTTM3 as QAF_PERIL_MTRC_STRT_DTTM,
upd_ins.EDW_END_DTTM as QAF_PERIL_MTRC_END_DTTM,
upd_ins.AGMT_ASSET_FEAT_PERIL_AMT as QAF_PERIL_MTRC_AMT,
upd_ins.TGT_IDNTFTN_VAL3 as CURY_CD,
upd_ins.PRCS_ID as PRCS_ID,
upd_ins.EDW_STRT_DTTM as EDW_STRT_DTTM,
upd_ins.EDW_END_DTTM as EDW_END_DTTM,
upd_ins.TRANS_STRT_DTTM as TRANS_STRT_DTTM
FROM
upd_ins;


-- Component update_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE update_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_upd.lkp_QUOTN_ID as lkp_QUOTN_ID,
exp_upd.lkp_FEAT_ID3 as lkp_FEAT_ID3,
exp_upd.lkp_PRTY_ASSET_ID3 as lkp_PRTY_ASSET_ID3,
exp_upd.lkp_RTG_PERIL_TYPE_CD3 as lkp_RTG_PERIL_TYPE_CD3,
exp_upd.lkp_INSRNC_MTRC_TYPE_CD3 as lkp_INSRNC_MTRC_TYPE_CD3,
exp_upd.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
exp_upd.EDW_END_DTTM as EDW_END_DTTM,
exp_upd.TRANS_END_DTTM as TRANS_END_DTTM,
exp_upd.lkp_QAF_PERIL_MTRC_DTTM as lkp_QAF_PERIL_MTRC_DTTM,
exp_upd.lkp_QUOTN_ASSET_FEAT_STRT_DTTM as lkp_QUOTN_ASSET_FEAT_STRT_DTTM,
1 as UPDATE_STRATEGY_ACTION,
exp_upd.source_record_id
FROM
exp_upd
);


-- Component QUOTN_ASSET_FEAT_PERIL_MTRC_upd, Type TARGET 
/* Perform Updates */
MERGE INTO DB_T_PROD_CORE.QUOTN_ASSET_FEAT_PERIL_MTRC
USING update_upd ON (UPDATE_STRATEGY_ACTION = 1 AND QUOTN_ASSET_FEAT_PERIL_MTRC.PRTY_ASSET_ID = update_upd.lkp_PRTY_ASSET_ID3 AND QUOTN_ASSET_FEAT_PERIL_MTRC.QUOTN_ID = update_upd.lkp_QUOTN_ID AND QUOTN_ASSET_FEAT_PERIL_MTRC.FEAT_ID = update_upd.lkp_FEAT_ID3 AND QUOTN_ASSET_FEAT_PERIL_MTRC.INSRNC_MTRC_TYPE_CD = update_upd.lkp_INSRNC_MTRC_TYPE_CD3 AND QUOTN_ASSET_FEAT_PERIL_MTRC.EDW_STRT_DTTM = update_upd.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
RTG_PERIL_TYPE_CD = update_upd.lkp_RTG_PERIL_TYPE_CD3,
EDW_END_DTTM = update_upd.EDW_END_DTTM,
TRANS_END_DTTM = update_upd.TRANS_END_DTTM
;


END; ';