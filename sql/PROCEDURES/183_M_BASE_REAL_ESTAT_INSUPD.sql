-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_REAL_ESTAT_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare
	P_DEFAULT_STR_CD VARCHAR;
	start_dttm TIMESTAMP;
	end_dttm TIMESTAMP;
	prcs_id integer;

BEGIN 
P_DEFAULT_STR_CD := ''UNK'';
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

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_CLASFCN'' 

             --AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'' ,''pcx_holineschcovitemcov_alfa.ChoiceTerm1'', ''contentlineitemschedule.typecode'')

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


-- Component LKP_TERADATA_ETL_REF_XLAT_CNSTRCTN_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_CNSTRCTN_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''CNSTRCTN_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''pctl_constructiontype_hoe.typecode'',  ''cctl_constructiontype_alfa'',

			 ''pctl_bp7constructiontype.typecode'',''pctl_fopdwelconsttype.typecode'',''pctl_fopoutbldglconsttype.typecode'')

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_REAL_EST_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_REAL_EST_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''REAL_ESTAT_TYPE'' 

      --       AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''pctl_residencetype_hoe.typecode'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

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


-- Component SQ_real_state_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_real_state_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ID,
$2 as TYPE_CODE,
$3 as CLASSIFICATION_CODE,
$4 as SRC_CD,
$5 as CONSTRUCTION_DT,
$6 as CONSTRUCTION_TYPE_CD,
$7 as REAL_ESTATE_TYPE_CD,
$8 as MFG_HOME_PRK_CD,
$9 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT   distinct  ID, TYPE_CODE, CLASSIFICATION_CODE,   SRC_CD, CONSTRUCTION_DT, CONSTRUCTION_TYPE_CD,   

        REAL_ESTATE_TYPE_CD ,MFG_HOME_PRK_CD  

 from ( select                                              																					

distinct cast(cc.PublicID_stg as varchar(64)) as ID, cast(''PRTY_ASSET_SBTYPE5'' as varchar(50)) as TYPE_CODE ,    

cast(''PRTY_ASSET_CLASFCN1'' as varchar(50)) as CLASSIFICATION_CODE, cast(''SRC_SYS6'' as varchar(50)) as SRC_CD, cc.YearBuilt_stg as CONSTRUCTION_DT,cctl1.TYPECODE_stg as CONSTRUCTION_TYPE_CD,   

cast('''' as varchar(50)) as REAL_ESTATE_TYPE_CD, cast(null as varchar(50)) as MFG_HOME_PRK_CD        

from  DB_T_PROD_STAG.cc_incident cc inner join                                               																					

(select cc.* from DB_T_PROD_STAG.cc_claim cc inner join DB_T_PROD_STAG.cctl_claimstate cctl on cc.State_stg= cctl.id_stg where cctl.name_stg <> ''Draft'') clm      

on cc.ClaimID_stg=clm.ID_stg                                                																					

inner join DB_T_PROD_STAG.cctl_incident cctl on cc.Subtype_stg = cctl.id_stg 

left join DB_T_PROD_STAG.cc_address ccd on clm.LossLocationID_stg = ccd.ID_stg 

left join DB_T_PROD_STAG.cc_policylocation ccp on ccp.AddressID_stg= ccd.id_stg 

left join DB_T_PROD_STAG.cc_riskunit ccr on ccr.PolicyLocationID_stg =ccp.ID_stg 

left join DB_T_PROD_STAG.cctl_constructiontype_alfa cctl1 on ccr.ConstructionType_alfa_stg=cctl1.ID_stg  

where cctl.name_stg  in (''DwellingIncident'')       

and ccp.PolicySystemId_stg is null                                              																					

                                                																					

union                                               																					

select a.id,TYPE_CODE,CLASSIFICATION_CODE,SRC_CD,CONSTRUCTION_DT,CONSTRUCTION_TYPE_CD,REAL_ESTATE_TYPE_CD ,MFG_HOME_PRK_CD 

from (select distinct cast(pdh.fixedid_stg  as varchar(100))as ID , ''PRTY_ASSET_SBTYPE5''  as TYPE_CODE , 

''PRTY_ASSET_CLASFCN1'' as CLASSIFICATION_CODE, ''SRC_SYS4'' as SRC_CD ,cast(cast(pdh.YearBuilt_stg as varchar(50))||''-01-01'' as date) as CONSTRUCTION_DT, 

/* ,''''as construction_dt, */
pctlch.TYPECODE_stg as CONSTRUCTION_TYPE_CD,pctlrh.TYPECODE_stg as REAL_ESTATE_TYPE_CD,  

pcma.Code_stg as MFG_HOME_PRK_CD from DB_T_PROD_STAG.pcx_dwelling_hoe pdh 

left outer join DB_T_PROD_STAG.pctl_constructiontype_hoe pctlch on pctlch.ID_stg= pdh.constructiontype_stg 

left outer join DB_T_PROD_STAG.pctl_residenceType_hoe pctlrh on pdh.ResidenceType_stg=pctlrh.ID_stg 

left outer join DB_T_PROD_STAG.pcx_manhomeparkcode_alfa pcma on pdh.ManHomeParkCode_alfa_stg=pcma.ID_stg 

where pdh.UpdateTime_stg > (:start_dttm) and pdh.UpdateTime_stg <= (:end_dttm)) a                                             																					

                                                																					

union                                               																					

/**Dwelling Personal Property and Other Structure**/                                                																					

select  distinct cast(pcxh.FixedID_stg as varchar(100))as ID 

,case when pce.PatternID_stg in (''HOSI_SpecificOtherStructureItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'') then ''PRTY_ASSET_SBTYPE5''  

when pce.PatternID_stg=''HOSI_ScheduledPropertyItem_alfa'' then ''PRTY_ASSET_SBTYPE7''  /*''REALSP-PP''*/ end as TYPE_CODE ,ChoiceTerm1_stg as CLASSIFICATION_CODE,''SRC_SYS4'' as SRC_CD   

,cast(''1900-01-01'' as date) as CONSTRUCTION_DT, '''' as CONSTRUCTION_TYPE_CD,ChoiceTerm3_stg as REAL_ESTATE_TYPE_CD, cast(null as varchar(50)) as MFG_HOME_PRK_CD 

from DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pcx  inner join DB_T_PROD_STAG.pc_etlclausepattern pce on pce.PatternID_stg=pcx.PatternCode_stg                                             																					

inner  join DB_T_PROD_STAG.pcx_holineschedcovitem_alfa pcxh on pcxh.id_stg = pcx.HOLineSchCovItem_stg inner join DB_T_PROD_STAG.pc_policyperiod pp on pp.id_stg=pcx.BranchID_stg  

where pce.PatternID_stg in  (''HOSI_SpecificOtherStructureItem_alfa'',''HOSI_ScheduledPropertyItem_alfa'',''HOSI_SpecificOtherStructureExclItem_alfa'')                                             																					

                                                																					

union                                               																					

                                                																					

/** Business **/                                                																					

select distinct                   																					

cast(c.FixedId_stg as varchar(100)) as ID, ''PRTY_ASSET_SBTYPE13'' as TYPE_CODE,cp.TYPECODE_stg as CLASSIFICATION_CODE,                                             																					

''SRC_SYS4'' as SRC_CD, cast(cast(b.BP7YearBuilt_alfa_stg as varchar(50))||''-01-01'' as date) as CONSTRUCTION_DT, 

ct1.TYPECODE_stg as CONSTRUCTION_TYPE_CD, CAST(NULL AS VARCHAR(50))as REAL_ESTATE_TYPE_CD, 

cast(null as varchar(50)) as MFG_HOME_PRK_CD 

from DB_T_PROD_STAG.pcx_bp7classification c  inner join (select b.*, rank() over (partition by b.FixedId_stg order by b.UpdateTime_stg desc) r from DB_T_PROD_STAG.pcx_bp7building b) b     

        on c.Building_stg = b.FixedID_stg     and b.r = 1 and b.ExpirationDate_stg is null inner join DB_T_PROD_STAG.pctl_bp7classificationproperty cp on c.bp7classpropertytype_stg = cp.ID_stg                                              																					

left outer join DB_T_PROD_STAG.pctl_bp7constructiontype ct1 on b.bp7constructiontype_stg = ct1.ID_stg    

where ((c.UpdateTime_stg > (:start_dttm) and c.UpdateTime_stg <= (:end_dttm)) or (b.UpdateTime_stg > (:start_dttm) and b.UpdateTime_stg <= (:end_dttm)))                                                																					

union                                               																					

/* -building                                                 																					 */
SELECT DISTINCT                                             																					

    cast(a.fixedid_stg as varchar(100)) as id,  cast(''PRTY_ASSET_SBTYPE32'' as varchar(50)) as assettype,   

    cast(''PRTY_ASSET_CLASFCN10'' as varchar(50))as classification_code, ''SRC_SYS4'' as src_cd,  

   cast(''1900-01-01'' as date)as CONSTRUCTION_DT,  cast(null as varchar(100)) as reg_num,    

    CAST(NULL AS VARCHAR(50))as REAL_ESTATE_TYPE_CD, cast(null as varchar(50)) as MFG_HOME_PRK_CD   

from DB_T_PROD_STAG.pcx_bp7building a  join DB_T_PROD_STAG.pc_building b on b.FixedID_stg = a.Building_stg and b.BranchID_stg = a.BranchID_stg         

where a.expirationdate_stg is null and  ((a.updatetime_stg > (:start_dttm)  AND a.updatetime_stg <= (:end_dttm))      

    OR (b.updatetime_stg > (:start_dttm) AND b.updatetime_stg <= (:end_dttm)))

	union

/* -Farm changes */
SELECT   ID, TYPE_CODE, CLASSIFICATION_CODE,   SRC_CD, CONSTRUCTION_DT, CONSTRUCTION_TYPE_CD,   

        REAL_ESTATE_TYPE_CD ,MFG_HOME_PRK_CD FROM 	

(select distinct cast(a.fixedid_stg as varchar(100)) as id,cast(''PRTY_ASSET_SBTYPE37'' as varchar(50)) as TYPE_CODE,

cast(''PRTY_ASSET_CLASFCN15'' as varchar(50))as classification_code,''SRC_SYS4'' as  SRC_CD

,CASE when  length(trim(YearBuilt_stg))=4 then   cast( concat(cast(YearBuilt_stg as varchar(50)),''-01-01'') as date ) else null end  CONSTRUCTION_DT,

d.typecode_stg as CONSTRUCTION_TYPE_CD,

CAST(b.TYPECODE_stg  AS VARCHAR(50))as REAL_ESTATE_TYPE_CD, 

cast(null as varchar(50)) as MFG_HOME_PRK_CD,a.UpdateTime_stg

from DB_T_PROD_STAG.pcx_fopdwelling a

join DB_T_PROD_STAG.pc_policyperiod c on a.BranchID_stg  = c.ID_stg 

left join DB_T_PROD_STAG.pctl_fopresidencetype b on b.id_stg = a.ResidenceType_stg

left join DB_T_PROD_STAG.pctl_fopdwelconsttype d on d.id_stg = a.ConstructionType_stg

where (a.expirationdate_stg is null or a.expirationdate_stg>c.Editeffectivedate_stg)

AND a.UpdateTime_stg > (:start_dttm) and a.UpdateTime_stg <= (:end_dttm)

QUALIFY ROW_NUMBER() OVER(PARTITION BY ID,CLASSIFICATION_CODE,TYPE_CODE ORDER BY coalesce(a.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999''as timestamp(6)))desc,a.UpdateTime_stg DESC,a.CreateTime_stg desc) =1)A



union 

SELECT   ID, TYPE_CODE, CLASSIFICATION_CODE,   SRC_CD, CONSTRUCTION_DT, CONSTRUCTION_TYPE_CD,   

        REAL_ESTATE_TYPE_CD ,MFG_HOME_PRK_CD FROM 

(select distinct cast(a.fixedid_stg as varchar(100)) as id,cast(''PRTY_ASSET_SBTYPE36''as varchar(50)) as TYPE_CODE,

 cast(''PRTY_ASSET_CLASFCN13''as varchar(50))as classification_code,''SRC_SYS4'' as  SRC_CD,

 CASE when  length(trim(YearBuilt_stg))=4 then   cast( concat(cast(YearBuilt_stg as varchar(50)),''-01-01'') as date ) else null end as CONSTRUCTION_DT,

 d.typecode_stg as CONSTRUCTION_TYPE_CD,

CAST(b.TYPECODE_stg AS VARCHAR(50))as REAL_ESTATE_TYPE_CD, 

cast(null as varchar(50)) as MFG_HOME_PRK_CD,a.UpdateTime_stg

from DB_T_PROD_STAG.pcx_fopoutbuilding a

join DB_T_PROD_STAG.pc_policyperiod c on a.BranchID_stg  = c.ID_stg 

left join DB_T_PROD_STAG.pctl_fopresidencetype b on b.id_stg = a.OutbldgResType_stg

left join DB_T_PROD_STAG.pctl_fopoutbldglconsttype d on d.id_stg = a.ConstrType_stg

where (a.expirationdate_stg is null or a.expirationdate_stg>c.Editeffectivedate_stg)

AND a.UpdateTime_stg > (:start_dttm) and a.UpdateTime_stg <= (:end_dttm)

QUALIFY ROW_NUMBER() OVER(PARTITION BY ID,CLASSIFICATION_CODE,TYPE_CODE ORDER BY coalesce(a.ExpirationDate_stg,cast(''9999-12-31 23:59:59.999999''as timestamp(6)))desc,a.UpdateTime_stg DESC,a.CreateTime_stg desc) =1)B) as a   										

QUALIFY ROW_NUMBER() OVER(PARTITION BY ID,CLASSIFICATION_CODE,TYPE_CODE  ORDER BY CONSTRUCTION_DT DESC) =1
) SRC
)
);


-- Component exp_all_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source AS
(
SELECT
1 as Cntrl_id,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ END as out_prty_asset_sbtype_cd,
:PRCS_ID as Process_id,
CASE WHEN LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ IS NULL THEN ''UNK'' ELSE LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ END as out_class_cd,
UPPER ( SQ_real_state_x.ID ) as out_FixedID,
SQ_real_state_x.CONSTRUCTION_DT as in_YearBuilt,
CASE WHEN LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CNSTRCTN_TYPE */ IS NULL THEN :P_DEFAULT_STR_CD ELSE LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CNSTRCTN_TYPE */ END as out_cnstrctn_typecode,
CASE WHEN LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_REAL_EST_TYPE */ IS NULL THEN :P_DEFAULT_STR_CD ELSE LKP_8.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_REAL_EST_TYPE */ END as out_res_typecode,
LKP_9.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as out_src_cd,
SQ_real_state_x.MFG_HOME_PRK_CD as MFG_HOME_PRK_CD,
SQ_real_state_x.source_record_id,
row_number() over (partition by SQ_real_state_x.source_record_id order by SQ_real_state_x.source_record_id) as RNK
FROM
SQ_real_state_x
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_real_state_x.TYPE_CODE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_real_state_x.TYPE_CODE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = SQ_real_state_x.CLASSIFICATION_CODE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = SQ_real_state_x.CLASSIFICATION_CODE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CNSTRCTN_TYPE LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = SQ_real_state_x.CONSTRUCTION_TYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CNSTRCTN_TYPE LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = SQ_real_state_x.CONSTRUCTION_TYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_REAL_EST_TYPE LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = SQ_real_state_x.REAL_ESTATE_TYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_REAL_EST_TYPE LKP_8 ON LKP_8.SRC_IDNTFTN_VAL = SQ_real_state_x.REAL_ESTATE_TYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_9 ON LKP_9.SRC_IDNTFTN_VAL = SQ_real_state_x.SRC_CD
QUALIFY RNK = 1
);


-- Component LKP_PRTY_ASSET_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_ASSET_ID AS
(
SELECT
LKP.PRTY_ASSET_ID,
exp_all_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_HOST_ID_VAL asc,LKP.PRTY_ASSET_SBTYPE_CD asc,LKP.PRTY_ASSET_CLASFCN_CD asc,LKP.ASSET_INSRNC_HIST_TYPE_CD asc,LKP.ASSET_DESC asc,LKP.PRTY_ASSET_NAME asc,LKP.PRTY_ASSET_STRT_DTTM asc,LKP.PRTY_ASSET_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc) RNK
FROM
exp_all_source
LEFT JOIN (
SELECT PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET.ASSET_INSRNC_HIST_TYPE_CD as ASSET_INSRNC_HIST_TYPE_CD, PRTY_ASSET.ASSET_DESC as ASSET_DESC, PRTY_ASSET.PRTY_ASSET_NAME as PRTY_ASSET_NAME, PRTY_ASSET.PRTY_ASSET_STRT_DTTM as PRTY_ASSET_STRT_DTTM, PRTY_ASSET.PRTY_ASSET_END_DTTM as PRTY_ASSET_END_DTTM, PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM, PRTY_ASSET.SRC_SYS_CD as SRC_SYS_CD, PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL, PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD, PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD 
FROM db_t_prod_core.PRTY_ASSET 
QUALIFY ROW_NUMBER() OVER(PARTITION BY  ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,PRTY_ASSET_CLASFCN_CD ORDER BY EDW_END_DTTM DESC) = 1
) LKP ON LKP.ASSET_HOST_ID_VAL = exp_all_source.out_FixedID AND LKP.PRTY_ASSET_SBTYPE_CD = exp_all_source.out_prty_asset_sbtype_cd AND LKP.PRTY_ASSET_CLASFCN_CD = exp_all_source.out_class_cd
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_all_source.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_HOST_ID_VAL asc,LKP.PRTY_ASSET_SBTYPE_CD asc,LKP.PRTY_ASSET_CLASFCN_CD asc,LKP.ASSET_INSRNC_HIST_TYPE_CD asc,LKP.ASSET_DESC asc,LKP.PRTY_ASSET_NAME asc,LKP.PRTY_ASSET_STRT_DTTM asc,LKP.PRTY_ASSET_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc)  
= 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
exp_all_source.out_cnstrctn_typecode as Cnstrctn_typecode,
LKP_PRTY_ASSET_ID.PRTY_ASSET_ID as Party_asset_id,
exp_all_source.Cntrl_id as Cntrl_id,
exp_all_source.Process_id as Process_id,
exp_all_source.in_YearBuilt as YearBuilt,
exp_all_source.out_res_typecode as res_typecode,
exp_all_source.MFG_HOME_PRK_CD as MFG_HOME_PRK_CD,
exp_all_source.source_record_id
FROM
exp_all_source
INNER JOIN LKP_PRTY_ASSET_ID ON exp_all_source.source_record_id = LKP_PRTY_ASSET_ID.source_record_id
);


-- Component LKP_MFG_PRK_CD, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_MFG_PRK_CD AS
(
SELECT
LKP.MFG_HOME_PRK_ID,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.MFG_HOME_PRK_ID asc,LKP.MFG_HOME_PRK_CD asc,LKP.MFG_HOME_PRK_NAME asc,LKP.CNTY_ID asc,LKP.PRCS_ID asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_data_transformation
LEFT JOIN (
SELECT
MFG_HOME_PRK_ID,
MFG_HOME_PRK_CD,
MFG_HOME_PRK_NAME,
CNTY_ID,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
FROM db_t_prod_core.MFG_HOME_PRK
) LKP ON LKP.MFG_HOME_PRK_CD = exp_data_transformation.MFG_HOME_PRK_CD
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.MFG_HOME_PRK_ID asc,LKP.MFG_HOME_PRK_CD asc,LKP.MFG_HOME_PRK_NAME asc,LKP.CNTY_ID asc,LKP.PRCS_ID asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) 
= 1
);


-- Component exp_SrcFields, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_SrcFields AS
(
SELECT
exp_data_transformation.Party_asset_id as in_PRTY_ASSET_ID,
CASE WHEN exp_data_transformation.res_typecode IS NULL THEN ''UNK'' ELSE exp_data_transformation.res_typecode END as in_REAL_ESTAT_TYPE_CD,
CASE WHEN exp_data_transformation.Cnstrctn_typecode IS NULL THEN ''UNK'' ELSE exp_data_transformation.Cnstrctn_typecode END as in_CNSTRCTN_TYPE_CD,
CASE WHEN exp_data_transformation.YearBuilt IS NULL THEN SUBSTR ( to_char ( CURRENT_TIMESTAMP ) , 1 , 10 ) ELSE to_char ( concat ( ''01/01/'' , to_char ( exp_data_transformation.YearBuilt ) ) ) END as v_YearBuilt,
exp_data_transformation.YearBuilt as in_CNSTRCTN_DT,
:P_DEFAULT_STR_CD as in_REAL_ESTAT_ZN_TYPE_CD,
:P_DEFAULT_STR_CD as in_REAL_ESTAT_RGHTS_TYPE_CD,
exp_data_transformation.Process_id as in_PRCS_ID,
LKP_MFG_PRK_CD.MFG_HOME_PRK_ID as MFG_HOME_PRK_ID,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
INNER JOIN LKP_MFG_PRK_CD ON exp_data_transformation.source_record_id = LKP_MFG_PRK_CD.source_record_id
);


-- Component LKP_REAL_ESTAT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_REAL_ESTAT AS
(
SELECT
LKP.PRTY_ASSET_ID,
LKP.REAL_ESTAT_TYPE_CD,
LKP.CNSTRCTN_TYPE_CD,
LKP.CNSTRCTN_DT,
LKP.REAL_ESTAT_ZN_TYPE_CD,
LKP.REAL_ESTAT_RGHTS_TYPE_CD,
LKP.MFG_HOME_PRK_ID,
exp_SrcFields.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_SrcFields.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_SrcFields.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.REAL_ESTAT_TYPE_CD asc,LKP.CNSTRCTN_TYPE_CD asc,LKP.CNSTRCTN_DT asc,LKP.REAL_ESTAT_ZN_TYPE_CD asc,LKP.REAL_ESTAT_RGHTS_TYPE_CD asc,LKP.MFG_HOME_PRK_ID asc) RNK
FROM
exp_SrcFields
LEFT JOIN (
SELECT REAL_ESTAT.REAL_ESTAT_TYPE_CD as REAL_ESTAT_TYPE_CD, REAL_ESTAT.CNSTRCTN_TYPE_CD as CNSTRCTN_TYPE_CD, REAL_ESTAT.CNSTRCTN_DT as CNSTRCTN_DT, REAL_ESTAT.REAL_ESTAT_ZN_TYPE_CD as REAL_ESTAT_ZN_TYPE_CD, REAL_ESTAT.REAL_ESTAT_RGHTS_TYPE_CD as REAL_ESTAT_RGHTS_TYPE_CD, REAL_ESTAT.MFG_HOME_PRK_ID as MFG_HOME_PRK_ID, REAL_ESTAT.PRTY_ASSET_ID as PRTY_ASSET_ID FROM db_t_prod_core.REAL_ESTAT
QUALIFY ROW_NUMBER() OVER(PARTITION BY PRTY_ASSET_ID  ORDER BY EDW_END_DTTM DESC) = 1
) LKP ON LKP.PRTY_ASSET_ID = exp_SrcFields.in_PRTY_ASSET_ID
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_SrcFields.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.REAL_ESTAT_TYPE_CD asc,LKP.CNSTRCTN_TYPE_CD asc,LKP.CNSTRCTN_DT asc,LKP.REAL_ESTAT_ZN_TYPE_CD asc,LKP.REAL_ESTAT_RGHTS_TYPE_CD asc,LKP.MFG_HOME_PRK_ID asc)  
= 1
);


-- Component exp_CDC_Check, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CDC_Check AS
(
SELECT
exp_SrcFields.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_SrcFields.in_REAL_ESTAT_TYPE_CD as in_REAL_ESTAT_TYPE_CD,
exp_SrcFields.in_CNSTRCTN_TYPE_CD as in_CNSTRCTN_TYPE_CD,
exp_SrcFields.in_CNSTRCTN_DT as in_CNSTRCTN_DT,
exp_SrcFields.in_REAL_ESTAT_ZN_TYPE_CD as in_REAL_ESTAT_ZN_TYPE_CD,
exp_SrcFields.in_REAL_ESTAT_RGHTS_TYPE_CD as in_REAL_ESTAT_RGHTS_TYPE_CD,
exp_SrcFields.in_PRCS_ID as in_PRCS_ID,
LKP_REAL_ESTAT.PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
LKP_REAL_ESTAT.REAL_ESTAT_TYPE_CD as lkp_REAL_ESTAT_TYPE_CD,
exp_SrcFields.MFG_HOME_PRK_ID as MFG_HOME_PRK_ID,
NULL as lkp_EDW_STRT_DTTM,
MD5 ( ltrim ( rtrim ( exp_SrcFields.in_REAL_ESTAT_TYPE_CD ) ) || ltrim ( rtrim ( exp_SrcFields.in_CNSTRCTN_TYPE_CD ) ) || to_char ( exp_SrcFields.in_CNSTRCTN_DT ) || ltrim ( rtrim ( exp_SrcFields.in_REAL_ESTAT_ZN_TYPE_CD ) ) || ltrim ( rtrim ( exp_SrcFields.in_REAL_ESTAT_RGHTS_TYPE_CD ) ) || to_char ( exp_SrcFields.MFG_HOME_PRK_ID ) ) as v_MD5_SRC,
MD5 ( ltrim ( rtrim ( LKP_REAL_ESTAT.REAL_ESTAT_TYPE_CD ) ) || ltrim ( rtrim ( LKP_REAL_ESTAT.CNSTRCTN_TYPE_CD ) ) || TO_CHAR ( LKP_REAL_ESTAT.CNSTRCTN_DT ) || ltrim ( rtrim ( LKP_REAL_ESTAT.REAL_ESTAT_ZN_TYPE_CD ) ) || ltrim ( rtrim ( LKP_REAL_ESTAT.REAL_ESTAT_RGHTS_TYPE_CD ) ) || TO_CHAR ( LKP_REAL_ESTAT.MFG_HOME_PRK_ID ) ) as v_MD5_TGT,
CASE WHEN v_MD5_TGT IS NULL THEN ''I'' ELSE CASE WHEN v_MD5_SRC = v_MD5_TGT THEN ''X'' ELSE ''U'' END END as o_Src_Tgt,
CURRENT_TIMESTAMP as StartDate,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EndDate,
exp_SrcFields.source_record_id
FROM
exp_SrcFields
INNER JOIN LKP_REAL_ESTAT ON exp_SrcFields.source_record_id = LKP_REAL_ESTAT.source_record_id
);


-- Component rtr_CDC_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE rtr_CDC_Insert AS (
SELECT
exp_CDC_Check.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_CDC_Check.in_REAL_ESTAT_TYPE_CD as in_REAL_ESTAT_TYPE_CD,
exp_CDC_Check.in_CNSTRCTN_TYPE_CD as in_CNSTRCTN_TYPE_CD,
exp_CDC_Check.in_CNSTRCTN_DT as in_CNSTRCTN_DT,
exp_CDC_Check.in_REAL_ESTAT_ZN_TYPE_CD as in_REAL_ESTAT_ZN_TYPE_CD,
exp_CDC_Check.in_REAL_ESTAT_RGHTS_TYPE_CD as in_REAL_ESTAT_RGHTS_TYPE_CD,
exp_CDC_Check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check.lkp_PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,
exp_CDC_Check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check.o_Src_Tgt as o_Src_Tgt,
exp_CDC_Check.StartDate as StartDate,
exp_CDC_Check.EndDate as EndDate,
exp_CDC_Check.lkp_REAL_ESTAT_TYPE_CD as lkp_REAL_ESTAT_TYPE_CD,
exp_CDC_Check.MFG_HOME_PRK_ID as MFG_HOME_PRK_ID,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE --NewLookupRow = 1 
( exp_CDC_Check.o_Src_Tgt = ''I'' and exp_CDC_Check.in_PRTY_ASSET_ID IS NOT NULL ) OR exp_CDC_Check.o_Src_Tgt = ''U'');


-- Component exp_pass_to_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt AS
(
SELECT
rtr_CDC_Insert.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID1,
rtr_CDC_Insert.in_REAL_ESTAT_TYPE_CD as in_REAL_ESTAT_TYPE_CD1,
rtr_CDC_Insert.in_CNSTRCTN_TYPE_CD as in_CNSTRCTN_TYPE_CD1,
rtr_CDC_Insert.in_CNSTRCTN_DT as in_CNSTRCTN_DT1,
rtr_CDC_Insert.in_REAL_ESTAT_ZN_TYPE_CD as in_REAL_ESTAT_ZN_TYPE_CD1,
rtr_CDC_Insert.in_REAL_ESTAT_RGHTS_TYPE_CD as in_REAL_ESTAT_RGHTS_TYPE_CD1,
rtr_CDC_Insert.in_PRCS_ID as in_PRCS_ID1,
rtr_CDC_Insert.StartDate as StartDate1,
rtr_CDC_Insert.EndDate as EndDate1,
rtr_CDC_Insert.MFG_HOME_PRK_ID as MFG_HOME_PRK_ID1,
rtr_CDC_Insert.source_record_id
FROM
rtr_CDC_Insert
);


-- Component REAL_ESTAT_NewInsert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.REAL_ESTAT
(
PRTY_ASSET_ID,
REAL_ESTAT_TYPE_CD,
CNSTRCTN_TYPE_CD,
CNSTRCTN_DT,
REAL_ESTAT_ZN_TYPE_CD,
REAL_ESTAT_RGHTS_TYPE_CD,
MFG_HOME_PRK_ID,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_pass_to_tgt.in_PRTY_ASSET_ID1 as PRTY_ASSET_ID,
exp_pass_to_tgt.in_REAL_ESTAT_TYPE_CD1 as REAL_ESTAT_TYPE_CD,
exp_pass_to_tgt.in_CNSTRCTN_TYPE_CD1 as CNSTRCTN_TYPE_CD,
exp_pass_to_tgt.in_CNSTRCTN_DT1 as CNSTRCTN_DT,
exp_pass_to_tgt.in_REAL_ESTAT_ZN_TYPE_CD1 as REAL_ESTAT_ZN_TYPE_CD,
exp_pass_to_tgt.in_REAL_ESTAT_RGHTS_TYPE_CD1 as REAL_ESTAT_RGHTS_TYPE_CD,
exp_pass_to_tgt.MFG_HOME_PRK_ID1 as MFG_HOME_PRK_ID,
exp_pass_to_tgt.in_PRCS_ID1 as PRCS_ID,
exp_pass_to_tgt.StartDate1 as EDW_STRT_DTTM,
exp_pass_to_tgt.EndDate1 as EDW_END_DTTM
FROM
exp_pass_to_tgt;


-- Component REAL_ESTAT_NewInsert, Type Post SQL 
UPDATE  db_t_prod_core.REAL_ESTAT  
set EDW_END_DTTM=A.lead

FROM

(SELECT	distinct  PRTY_ASSET_ID,EDW_STRT_DTTM ,

max(EDW_STRT_DTTM) over (partition by  PRTY_ASSET_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as lead

 FROM	db_t_prod_core.REAL_ESTAT

 ) a

where  REAL_ESTAT.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and REAL_ESTAT.PRTY_ASSET_ID=A.PRTY_ASSET_ID 

and CAST(REAL_ESTAT.EDW_END_DTTM AS DATE)=''9999-12-31''

and lead is not null;


END; ';