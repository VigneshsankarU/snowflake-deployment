-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_INSRBL_INT_PRTY_DIAGN_CD_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' declare
	start_dttm timestamp;
	end_dttm timestamp;
    prcs_id int;
BEGIN 
set start_dttm  = current_timestamp;
set END_DTTM = current_timestamp;
set prcs_id= 1;  

-- Component LKP_INDIV_CLM_CTR, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_INDIV_CLM_CTR AS
(
SELECT 
	INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID, 
	INDIV.NK_PUBLC_ID as NK_PUBLC_ID 
FROM 
	DB_T_PROD_CORE.INDIV
WHERE
	INDIV.NK_PUBLC_ID IS NOT NULL
);


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

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'' ,''pcx_holineschcovitemcov_alfa.ChoiceTerm1'', ''cctl_contentlineitemschedule'')

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

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_CLM_SRC_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_CLM_SRC_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS''

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived''

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS= ''DS''

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_DIAGN, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_DIAGN AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''DIAGN_CD'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM = ''cctl_icdbodysystem.typecode''

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS = ''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_DIAGN_CLSFCTN_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_DIAGN_CLSFCTN_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM =''DIAGN_CLASFCN_TYPE''

     		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_cc_injurydiagnosis, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_cc_injurydiagnosis AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Comments,
$2 as ClaimNumber,
$3 as Code,
$4 as PublicID_contact,
$5 as Clm_src_cd,
$6 as PRTY_ASSET_SB_TYPE,
$7 as clasfcn_cd,
$8 as nk_vehicle,
$9 as updatetime_cc_injurydiagnosis,
$10 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT  

cc_incident.Comments_injurydiagnosis_stg, 

cc_incident.ClaimNumber_stg,

cc_incident.Code_icdcode_stg, 

cc_incident.PublicID_contact_stg,

''SRC_SYS6'' as clm_src_cd_stg, 

'''' AS PRTY_ASSET_SB_TYPE_stg, 

'''' as clasfcn_cd_stg,

'''' as nk_vehicle_stg,

updatetime_cc_injurydiagnosis_stg

FROM (SELECT	DISTINCT

cc_injurydiagnosis.Comments_stg   as Comments_injurydiagnosis_stg,

cc_claim.ClaimNumber_stg,

cctl_icdbodysystem.typecode_stg as Code_icdcode_stg,

 cc_contact.PublicID_stg as PublicID_contact_stg,

''SRC_SYS6'' as clm_src_cd_stg,

'''' AS PRTY_ASSET_SB_TYPE, 

'''' as clasfcn_cd,

'''' as nk_vehicle,

cc_injurydiagnosis.updatetime_stg as updatetime_cc_injurydiagnosis_stg,

cc_incident.Subtype_stg

FROM

DB_T_PROD_STAG.cc_incident 

inner join (

	select	cc_claim.ClaimNumber_stg,cc_claim.id_stg

	from	DB_T_PROD_STAG.cc_claim 

	inner join 

				DB_T_PROD_STAG.cctl_claimstate 

		on cc_claim.State_stg= cctl_claimstate.id_Stg 

where	cctl_claimstate.name_stg <> ''Draft'') cc_claim  /*  */
    on cc_claim.id_stg=cc_incident.claimid_stg

left outer join DB_T_PROD_STAG.cc_vehicle 

	on cc_incident.vehicleid_stg=cc_vehicle.id_stg

left outer join DB_T_PROD_STAG.cc_injurydiagnosis     

on cc_incident.ID_stg=cc_injurydiagnosis.InjuryIncidentID_stg /*  */
left outer join DB_T_PROD_STAG.cc_icdcode 

on cc_injurydiagnosis.ICDCode_stg=cc_icdcode.ID_stg /*  */
left outer join DB_T_PROD_STAG.cctl_icdbodysystem 

on  cctl_icdbodysystem.ID_stg=cc_icdcode.BodySystem_stg/*  */
left outer join DB_T_PROD_STAG.cc_claimcontactrole 

	on cc_incident.id_stg = cc_claimcontactrole.IncidentID_stg

left outer join DB_T_PROD_STAG.cc_claimcontact 

	on cc_claimcontactrole.claimcontactid_stg=cc_claimcontact.id_stg

left outer join DB_T_PROD_STAG.cc_contact 

	on cc_claimcontact.contactid_stg=cc_contact.id_stg

WHERE	

cc_incident.UpdateTime_stg > (:START_DTTM)

	and cc_incident.UpdateTime_stg <= (:END_DTTM)

and cc_incident.Subtype_stg=5

and updatetime_cc_injurydiagnosis_stg is not null

) cc_incident
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_cc_injurydiagnosis.Comments as Comments,
SQ_cc_injurydiagnosis.ClaimNumber as ClaimNumber,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DIAGN */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DIAGN */ END as Code_out,
to_date ( ''1900-01-01'' , ''YYYY-MM-DD'' ) as o_BusinesStartDdate,
LKP_3.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ as out_PublicID_contact,
LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_SRC_CD */ as out_CLM_SRC_CD,
''PERSON'' as out_ctgy_cd,
SQ_cc_injurydiagnosis.updatetime_cc_injurydiagnosis as updatetime_cc_injurydiagnosis,
SQ_cc_injurydiagnosis.source_record_id,
row_number() over (partition by SQ_cc_injurydiagnosis.source_record_id order by SQ_cc_injurydiagnosis.source_record_id) as RNK
FROM
SQ_cc_injurydiagnosis
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_DIAGN LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_cc_injurydiagnosis.Code
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_DIAGN LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_cc_injurydiagnosis.Code
LEFT JOIN LKP_INDIV_CLM_CTR LKP_3 ON LKP_3.NK_PUBLC_ID = SQ_cc_injurydiagnosis.PublicID_contact
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CLM_SRC_CD LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = SQ_cc_injurydiagnosis.Clm_src_cd
QUALIFY RNK = 1
);


-- Component LKP_INSRBL_INT_PRTYID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRBL_INT_PRTYID AS
(
SELECT
LKP.INSRBL_INT_ID,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.INSRBL_INT_ID desc,LKP.INSRBL_INT_CTGY_CD desc,LKP.CTSTRPH_EXPSR_IND desc,LKP.SRC_SYS_CD desc,LKP.INJURED_PRTY_ID desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK
FROM
exp_pass_through
LEFT JOIN (
SELECT INSRBL_INT.INSRBL_INT_ID as INSRBL_INT_ID, INSRBL_INT.CTSTRPH_EXPSR_IND as CTSTRPH_EXPSR_IND, INSRBL_INT.EDW_STRT_DTTM as EDW_STRT_DTTM, INSRBL_INT.EDW_END_DTTM as EDW_END_DTTM, INSRBL_INT.INSRBL_INT_CTGY_CD as INSRBL_INT_CTGY_CD, INSRBL_INT.SRC_SYS_CD as SRC_SYS_CD, INSRBL_INT.INJURED_PRTY_ID as INJURED_PRTY_ID FROM DB_T_PROD_CORE.INSRBL_INT
 where   INSRBL_INT.INSRBL_INT_CTGY_CD = ''PERSON''
AND  INSRBL_INT.INJURED_PRTY_ID  IS NOT NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY INSRBL_INT_CTGY_CD,INJURED_PRTY_ID,SRC_SYS_CD  ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.INSRBL_INT_CTGY_CD = exp_pass_through.out_ctgy_cd AND LKP.SRC_SYS_CD = exp_pass_through.out_CLM_SRC_CD AND LKP.INJURED_PRTY_ID = exp_pass_through.out_PublicID_contact
QUALIFY RNK = 1
);


-- Component LKP_CLM, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM AS
(
SELECT
LKP.CLM_ID,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.CLM_ID desc,LKP.CLM_TYPE_CD desc,LKP.CLM_MDIA_TYPE_CD desc,LKP.CLM_SUBMTL_TYPE_CD desc,LKP.ACDNT_TYPE_CD desc,LKP.CLM_CTGY_TYPE_CD desc,LKP.ADDL_INSRNC_PLN_IND desc,LKP.EMPLMT_RLTD_IND desc,LKP.ATTNY_INVLVMT_IND desc,LKP.CLM_NUM desc,LKP.CLM_PRIR_IND desc,LKP.PMT_MODE_CD desc,LKP.CLM_OBLGTN_TYPE_CD desc,LKP.SUBRGTN_ELGBL_CD desc,LKP.SUBRGTN_ELGBLY_RSN_CD desc,LKP.CURY_CD desc,LKP.INCDT_EV_ID desc,LKP.INSRD_AT_FAULT_IND desc,LKP.CVGE_IN_QUES_IND desc,LKP.EXTNT_OF_FIRE_DMG_TYPE_CD desc,LKP.VFYD_CLM_IND desc,LKP.PRCS_ID desc,LKP.CLM_STRT_DTTM desc,LKP.CLM_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.SRC_SYS_CD desc,LKP.TRANS_STRT_DTTM desc,LKP.LGCY_CLM_NUM desc) RNK
FROM
exp_pass_through
LEFT JOIN (
SELECT CLM.CLM_ID as CLM_ID, CLM.CLM_TYPE_CD as CLM_TYPE_CD, CLM.CLM_MDIA_TYPE_CD as CLM_MDIA_TYPE_CD, CLM.CLM_SUBMTL_TYPE_CD as CLM_SUBMTL_TYPE_CD, CLM.ACDNT_TYPE_CD as ACDNT_TYPE_CD, CLM.CLM_CTGY_TYPE_CD as CLM_CTGY_TYPE_CD, CLM.ADDL_INSRNC_PLN_IND as ADDL_INSRNC_PLN_IND, CLM.EMPLMT_RLTD_IND as EMPLMT_RLTD_IND, CLM.ATTNY_INVLVMT_IND as ATTNY_INVLVMT_IND, CLM.CLM_PRIR_IND as CLM_PRIR_IND, CLM.PMT_MODE_CD as PMT_MODE_CD, CLM.CLM_OBLGTN_TYPE_CD as CLM_OBLGTN_TYPE_CD, CLM.SUBRGTN_ELGBL_CD as SUBRGTN_ELGBL_CD, CLM.SUBRGTN_ELGBLY_RSN_CD as SUBRGTN_ELGBLY_RSN_CD, CLM.CURY_CD as CURY_CD, CLM.INCDT_EV_ID as INCDT_EV_ID, CLM.INSRD_AT_FAULT_IND as INSRD_AT_FAULT_IND, CLM.CVGE_IN_QUES_IND as CVGE_IN_QUES_IND, CLM.EXTNT_OF_FIRE_DMG_TYPE_CD as EXTNT_OF_FIRE_DMG_TYPE_CD, CLM.VFYD_CLM_IND as VFYD_CLM_IND, CLM.PRCS_ID as PRCS_ID, CLM.CLM_STRT_DTTM as CLM_STRT_DTTM, CLM.CLM_END_DTTM as CLM_END_DTTM, CLM.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM.EDW_END_DTTM as EDW_END_DTTM, CLM.TRANS_STRT_DTTM as TRANS_STRT_DTTM, CLM.LGCY_CLM_NUM as LGCY_CLM_NUM, CLM.CLM_NUM as CLM_NUM, CLM.SRC_SYS_CD as SRC_SYS_CD FROM DB_T_PROD_CORE.CLM  QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM.CLM_NUM,CLM.SRC_SYS_CD  ORDER BY CLM.EDW_END_DTTM desc) = 1
) LKP ON LKP.CLM_NUM = exp_pass_through.ClaimNumber AND LKP.SRC_SYS_CD = exp_pass_through.out_CLM_SRC_CD
QUALIFY RNK = 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
LKP_CLM.CLM_ID as CLM_ID,
LKP_INSRBL_INT_PRTYID.INSRBL_INT_ID as INSRBL_INT_ID,
exp_pass_through.Code_out as Code,
exp_pass_through.o_BusinesStartDdate as start_date,
exp_pass_through.Comments as Comments,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as end_date,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DIAGN_CLSFCTN_CD */ as diagn_clasfc_type_cd,
exp_pass_through.updatetime_cc_injurydiagnosis as updatetime_cc_injurydiagnosis,
exp_pass_through.source_record_id,
row_number() over (partition by exp_pass_through.source_record_id order by exp_pass_through.source_record_id) as RNK1
FROM
exp_pass_through
INNER JOIN LKP_INSRBL_INT_PRTYID ON exp_pass_through.source_record_id = LKP_INSRBL_INT_PRTYID.source_record_id
INNER JOIN LKP_CLM ON LKP_INSRBL_INT_PRTYID.source_record_id = LKP_CLM.source_record_id
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_DIAGN_CLSFCTN_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = ''DIAGN_CLASFCN_TYPE1''
QUALIFY RNK1 = 1
);


-- Component LKP_TARGET, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TARGET AS
(
SELECT
LKP.CLM_ID,
LKP.INSRBL_INT_ID,
LKP.DIAGN_CD,
LKP.DIAGN_CLASFCN_TYPE_CD,
LKP.CII_PRTY_DIAGN_CD_STRT_DT,
LKP.CII_PRTY_DIAGN_CD_END_DT,
LKP.CII_PRTY_DIAGN_COMT_TXT,
LKP.EDW_STRT_DTTM,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.CLM_ID asc,LKP.INSRBL_INT_ID asc,LKP.DIAGN_CD asc,LKP.DIAGN_CLASFCN_TYPE_CD asc,LKP.CII_PRTY_DIAGN_CD_STRT_DT asc,LKP.CII_PRTY_DIAGN_CD_END_DT asc,LKP.CII_PRTY_DIAGN_COMT_TXT asc,LKP.PRCS_ID asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.TRANS_STRT_DTTM asc,LKP.TRANS_END_DTTM asc) RNK
FROM
exp_data_transformation
LEFT JOIN (
SELECT CLM_INSRBL_INT_PRTY_DIAGN_CD.CII_PRTY_DIAGN_CD_END_DT as CII_PRTY_DIAGN_CD_END_DT, CLM_INSRBL_INT_PRTY_DIAGN_CD.CII_PRTY_DIAGN_COMT_TXT as CII_PRTY_DIAGN_COMT_TXT, CLM_INSRBL_INT_PRTY_DIAGN_CD.PRCS_ID as PRCS_ID, CLM_INSRBL_INT_PRTY_DIAGN_CD.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM_INSRBL_INT_PRTY_DIAGN_CD.EDW_END_DTTM as EDW_END_DTTM, CLM_INSRBL_INT_PRTY_DIAGN_CD.TRANS_STRT_DTTM as TRANS_STRT_DTTM, CLM_INSRBL_INT_PRTY_DIAGN_CD.TRANS_END_DTTM as TRANS_END_DTTM, CLM_INSRBL_INT_PRTY_DIAGN_CD.CLM_ID as CLM_ID, CLM_INSRBL_INT_PRTY_DIAGN_CD.INSRBL_INT_ID as INSRBL_INT_ID, CLM_INSRBL_INT_PRTY_DIAGN_CD.DIAGN_CD as DIAGN_CD, CLM_INSRBL_INT_PRTY_DIAGN_CD.DIAGN_CLASFCN_TYPE_CD as DIAGN_CLASFCN_TYPE_CD, CLM_INSRBL_INT_PRTY_DIAGN_CD.CII_PRTY_DIAGN_CD_STRT_DT as CII_PRTY_DIAGN_CD_STRT_DT 
FROM   DB_T_PROD_CORE.CLM_INSRBL_INT_PRTY_DIAGN_CD
QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM_ID,INSRBL_INT_ID 
ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.CLM_ID = exp_data_transformation.CLM_ID AND LKP.INSRBL_INT_ID = exp_data_transformation.INSRBL_INT_ID
QUALIFY RNK = 1
);


-- Component exp_check_flag, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_check_flag AS
(
SELECT
LKP_TARGET.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
MD5 ( to_char ( LKP_TARGET.CII_PRTY_DIAGN_CD_STRT_DT , ''MM/DD/YYYY'' ) || to_char ( LKP_TARGET.CII_PRTY_DIAGN_CD_END_DT , ''MM/DD/YYYY'' ) || ltrim ( rtrim ( LKP_TARGET.CII_PRTY_DIAGN_COMT_TXT ) ) || ltrim ( rtrim ( LKP_TARGET.DIAGN_CD ) ) || ltrim ( rtrim ( LKP_TARGET.DIAGN_CLASFCN_TYPE_CD ) ) ) as lkp_checksum,
exp_data_transformation.CLM_ID as in_CLM_ID,
exp_data_transformation.INSRBL_INT_ID as in_INSRBL_INT_ID,
exp_data_transformation.Code as in_DIAGN_CD,
exp_data_transformation.diagn_clasfc_type_cd as in_DIAGN_CLASFCN_TYPE_CD,
exp_data_transformation.start_date as in_CII_PRTY_DIAGN_CD_STRT_DT,
exp_data_transformation.end_date as in_CII_PRTY_DIAGN_CD_END_DT,
exp_data_transformation.Comments as in_CII_PRTY_DIAGN_COMT_TXT,
:PRCS_ID as in_PRCS_ID,
MD5 ( to_char ( exp_data_transformation.start_date , ''MM/DD/YYYY'' ) || to_char ( exp_data_transformation.end_date , ''MM/DD/YYYY'' ) || ltrim ( rtrim ( exp_data_transformation.Comments ) ) || ltrim ( rtrim ( exp_data_transformation.Code ) ) || ltrim ( rtrim ( exp_data_transformation.diagn_clasfc_type_cd ) ) ) as in_checksum,
CASE WHEN lkp_checksum IS NULL THEN ''I'' ELSE CASE WHEN lkp_checksum = in_checksum THEN ''R'' ELSE ''U'' END END as o_flag,
exp_data_transformation.updatetime_cc_injurydiagnosis as updatetime_cc_injurydiagnosis,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
INNER JOIN LKP_TARGET ON exp_data_transformation.source_record_id = LKP_TARGET.source_record_id
);


-- Component rtr_ins_upd_insert, Type ROUTER Output Group insert
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_insert AS
(
SELECT
exp_check_flag.in_CLM_ID as in_CLM_ID,
exp_check_flag.in_INSRBL_INT_ID as in_INSRBL_INT_ID,
exp_check_flag.in_DIAGN_CD as in_DIAGN_CD,
exp_check_flag.in_CII_PRTY_DIAGN_CD_STRT_DT as in_CII_PRTY_DIAGN_CD_STRT_DT,
exp_check_flag.in_CII_PRTY_DIAGN_COMT_TXT as in_CII_PRTY_DIAGN_COMT_TXT,
exp_check_flag.in_DIAGN_CLASFCN_TYPE_CD as in_DIAGN_CLASFCN_TYPE_CD,
exp_check_flag.in_CII_PRTY_DIAGN_CD_END_DT as in_CII_PRTY_DIAGN_CD_END_DT,
exp_check_flag.in_PRCS_ID as in_PRCS_ID,
exp_check_flag.o_flag as o_flag,
exp_check_flag.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_check_flag.updatetime_cc_injurydiagnosis as updatetime_cc_injurydiagnosis,
exp_check_flag.source_record_id
FROM
exp_check_flag
WHERE exp_check_flag.o_flag = ''I'' and exp_check_flag.in_CLM_ID IS NOT NULL and exp_check_flag.in_INSRBL_INT_ID IS NOT NULL);


-- Component rtr_ins_upd_update, Type ROUTER Output Group update
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_update AS
(
SELECT
exp_check_flag.in_CLM_ID as in_CLM_ID,
exp_check_flag.in_INSRBL_INT_ID as in_INSRBL_INT_ID,
exp_check_flag.in_DIAGN_CD as in_DIAGN_CD,
exp_check_flag.in_CII_PRTY_DIAGN_CD_STRT_DT as in_CII_PRTY_DIAGN_CD_STRT_DT,
exp_check_flag.in_CII_PRTY_DIAGN_COMT_TXT as in_CII_PRTY_DIAGN_COMT_TXT,
exp_check_flag.in_DIAGN_CLASFCN_TYPE_CD as in_DIAGN_CLASFCN_TYPE_CD,
exp_check_flag.in_CII_PRTY_DIAGN_CD_END_DT as in_CII_PRTY_DIAGN_CD_END_DT,
exp_check_flag.in_PRCS_ID as in_PRCS_ID,
exp_check_flag.o_flag as o_flag,
exp_check_flag.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_check_flag.updatetime_cc_injurydiagnosis as updatetime_cc_injurydiagnosis,
exp_check_flag.source_record_id
FROM
exp_check_flag
WHERE exp_check_flag.o_flag = ''U'');


-- Component exp_end_date_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_end_date_upd AS
(
SELECT
rtr_ins_upd_update.in_CLM_ID as in_CLM_ID,
rtr_ins_upd_update.in_INSRBL_INT_ID as in_INSRBL_INT_ID,
rtr_ins_upd_update.in_DIAGN_CD as in_DIAGN_CD,
rtr_ins_upd_update.in_CII_PRTY_DIAGN_CD_STRT_DT as in_CII_PRTY_DIAGN_CD_STRT_DT,
rtr_ins_upd_update.in_CII_PRTY_DIAGN_COMT_TXT as in_CII_PRTY_DIAGN_COMT_TXT,
rtr_ins_upd_update.in_DIAGN_CLASFCN_TYPE_CD as in_DIAGN_CLASFCN_TYPE_CD,
rtr_ins_upd_update.in_CII_PRTY_DIAGN_CD_END_DT as in_CII_PRTY_DIAGN_CD_END_DT,
DATEADD (SECOND, -1, CURRENT_TIMESTAMP()) as out_EDW_END_DTTM,
rtr_ins_upd_update.in_PRCS_ID as in_PRCS_ID,
rtr_ins_upd_update.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_ins_upd_update.source_record_id
FROM
rtr_ins_upd_update
);


-- Component exp_end_date_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_end_date_ins AS
(
SELECT
rtr_ins_upd_insert.in_CLM_ID as in_CLM_ID,
rtr_ins_upd_insert.in_INSRBL_INT_ID as in_INSRBL_INT_ID,
rtr_ins_upd_insert.in_DIAGN_CD as in_DIAGN_CD,
rtr_ins_upd_insert.in_CII_PRTY_DIAGN_CD_STRT_DT as in_CII_PRTY_DIAGN_CD_STRT_DT,
rtr_ins_upd_insert.in_CII_PRTY_DIAGN_COMT_TXT as in_CII_PRTY_DIAGN_COMT_TXT,
rtr_ins_upd_insert.in_DIAGN_CLASFCN_TYPE_CD as in_DIAGN_CLASFCN_TYPE_CD,
rtr_ins_upd_insert.in_CII_PRTY_DIAGN_CD_END_DT as in_CII_PRTY_DIAGN_CD_END_DT,
rtr_ins_upd_insert.in_PRCS_ID as in_PRCS_ID,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_EDW_END_DTTM,
rtr_ins_upd_insert.updatetime_cc_injurydiagnosis as TRANS_STRT_DTTM,
rtr_ins_upd_insert.source_record_id
FROM
rtr_ins_upd_insert
);


-- Component upd_update, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_update AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_end_date_upd.in_CLM_ID as in_CLM_ID,
exp_end_date_upd.in_INSRBL_INT_ID as in_INSRBL_INT_ID,
exp_end_date_upd.in_DIAGN_CD as in_DIAGN_CD,
exp_end_date_upd.in_CII_PRTY_DIAGN_CD_STRT_DT as in_CII_PRTY_DIAGN_CD_STRT_DT,
exp_end_date_upd.in_CII_PRTY_DIAGN_COMT_TXT as in_CII_PRTY_DIAGN_COMT_TXT,
exp_end_date_upd.in_DIAGN_CLASFCN_TYPE_CD as in_DIAGN_CLASFCN_TYPE_CD,
exp_end_date_upd.in_CII_PRTY_DIAGN_CD_END_DT as in_CII_PRTY_DIAGN_CD_END_DT,
exp_end_date_upd.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_end_date_upd.in_PRCS_ID as in_PRCS_ID,
exp_end_date_upd.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
1 as UPDATE_STRATEGY_ACTION
FROM
exp_end_date_upd
);


-- Component exp_end_date_upd1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_end_date_upd1 AS
(
SELECT
rtr_ins_upd_update.in_CLM_ID as in_CLM_ID,
rtr_ins_upd_update.in_INSRBL_INT_ID as in_INSRBL_INT_ID,
rtr_ins_upd_update.in_DIAGN_CD as in_DIAGN_CD,
rtr_ins_upd_update.in_CII_PRTY_DIAGN_CD_STRT_DT as in_CII_PRTY_DIAGN_CD_STRT_DT,
rtr_ins_upd_update.in_CII_PRTY_DIAGN_COMT_TXT as in_CII_PRTY_DIAGN_COMT_TXT,
rtr_ins_upd_update.in_DIAGN_CLASFCN_TYPE_CD as in_DIAGN_CLASFCN_TYPE_CD,
rtr_ins_upd_update.in_CII_PRTY_DIAGN_CD_END_DT as in_CII_PRTY_DIAGN_CD_END_DT,
rtr_ins_upd_update.in_PRCS_ID as in_PRCS_ID,
CURRENT_TIMESTAMP as out_EDW_STRT_DTTM,
TO_timestamp( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_EDW_END_DTTM,
rtr_ins_upd_update.updatetime_cc_injurydiagnosis as updatetime_cc_injurydiagnosis3,
rtr_ins_upd_update.source_record_id
FROM
rtr_ins_upd_update
);


-- Component CLM_INSRBL_INT_PRTY_DIAGN_CD_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_INSRBL_INT_PRTY_DIAGN_CD
(
CLM_ID,
INSRBL_INT_ID,
DIAGN_CD,
DIAGN_CLASFCN_TYPE_CD,
CII_PRTY_DIAGN_CD_STRT_DT,
CII_PRTY_DIAGN_CD_END_DT,
CII_PRTY_DIAGN_COMT_TXT,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_end_date_ins.in_CLM_ID as CLM_ID,
exp_end_date_ins.in_INSRBL_INT_ID as INSRBL_INT_ID,
exp_end_date_ins.in_DIAGN_CD as DIAGN_CD,
exp_end_date_ins.in_DIAGN_CLASFCN_TYPE_CD as DIAGN_CLASFCN_TYPE_CD,
exp_end_date_ins.in_CII_PRTY_DIAGN_CD_STRT_DT as CII_PRTY_DIAGN_CD_STRT_DT,
exp_end_date_ins.in_CII_PRTY_DIAGN_CD_END_DT as CII_PRTY_DIAGN_CD_END_DT,
exp_end_date_ins.in_CII_PRTY_DIAGN_COMT_TXT as CII_PRTY_DIAGN_COMT_TXT,
exp_end_date_ins.in_PRCS_ID as PRCS_ID,
exp_end_date_ins.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_end_date_ins.out_EDW_END_DTTM as EDW_END_DTTM,
exp_end_date_ins.TRANS_STRT_DTTM as TRANS_STRT_DTTM
FROM
exp_end_date_ins;


-- Component tgt_CLM_INSRBL_INT_PRTY_DIAGN_CD_upd, Type TARGET 
/* Perform Updates */
MERGE INTO DB_T_PROD_CORE.CLM_INSRBL_INT_PRTY_DIAGN_CD
USING upd_update ON (UPDATE_STRATEGY_ACTION = 1 AND CLM_INSRBL_INT_PRTY_DIAGN_CD.CLM_ID = upd_update.in_CLM_ID AND CLM_INSRBL_INT_PRTY_DIAGN_CD.INSRBL_INT_ID = upd_update.in_INSRBL_INT_ID AND CLM_INSRBL_INT_PRTY_DIAGN_CD.EDW_STRT_DTTM = upd_update.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
EDW_END_DTTM = upd_update.out_EDW_END_DTTM
;


-- Component clm_insrbl_int_prty_diagn_cd_insupd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE clm_insrbl_int_prty_diagn_cd_insupd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
exp_end_date_upd1.in_CLM_ID as in_CLM_ID,
exp_end_date_upd1.in_INSRBL_INT_ID as in_INSRBL_INT_ID,
exp_end_date_upd1.in_DIAGN_CD as in_DIAGN_CD,
exp_end_date_upd1.in_CII_PRTY_DIAGN_CD_STRT_DT as in_CII_PRTY_DIAGN_CD_STRT_DT,
exp_end_date_upd1.in_CII_PRTY_DIAGN_COMT_TXT as in_CII_PRTY_DIAGN_COMT_TXT,
exp_end_date_upd1.in_DIAGN_CLASFCN_TYPE_CD as in_DIAGN_CLASFCN_TYPE_CD,
exp_end_date_upd1.in_CII_PRTY_DIAGN_CD_END_DT as in_CII_PRTY_DIAGN_CD_END_DT,
exp_end_date_upd1.in_PRCS_ID as in_PRCS_ID,
exp_end_date_upd1.out_EDW_STRT_DTTM as out_EDW_STRT_DTTM,
exp_end_date_upd1.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_end_date_upd1.updatetime_cc_injurydiagnosis3 as updatetime_cc_injurydiagnosis3,
0 as UPDATE_STRATEGY_ACTION
FROM
exp_end_date_upd1
);


-- Component tgt_CLM_INSRBL_INT_PRTY_DIAGN_CD_insupd, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_INSRBL_INT_PRTY_DIAGN_CD
(
CLM_ID,
INSRBL_INT_ID,
DIAGN_CD,
DIAGN_CLASFCN_TYPE_CD,
CII_PRTY_DIAGN_CD_STRT_DT,
CII_PRTY_DIAGN_CD_END_DT,
CII_PRTY_DIAGN_COMT_TXT,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
clm_insrbl_int_prty_diagn_cd_insupd.in_CLM_ID as CLM_ID,
clm_insrbl_int_prty_diagn_cd_insupd.in_INSRBL_INT_ID as INSRBL_INT_ID,
clm_insrbl_int_prty_diagn_cd_insupd.in_DIAGN_CD as DIAGN_CD,
clm_insrbl_int_prty_diagn_cd_insupd.in_DIAGN_CLASFCN_TYPE_CD as DIAGN_CLASFCN_TYPE_CD,
clm_insrbl_int_prty_diagn_cd_insupd.in_CII_PRTY_DIAGN_CD_STRT_DT as CII_PRTY_DIAGN_CD_STRT_DT,
clm_insrbl_int_prty_diagn_cd_insupd.in_CII_PRTY_DIAGN_CD_END_DT as CII_PRTY_DIAGN_CD_END_DT,
clm_insrbl_int_prty_diagn_cd_insupd.in_CII_PRTY_DIAGN_COMT_TXT as CII_PRTY_DIAGN_COMT_TXT,
clm_insrbl_int_prty_diagn_cd_insupd.in_PRCS_ID as PRCS_ID,
clm_insrbl_int_prty_diagn_cd_insupd.out_EDW_STRT_DTTM as EDW_STRT_DTTM,
clm_insrbl_int_prty_diagn_cd_insupd.out_EDW_END_DTTM as EDW_END_DTTM,
clm_insrbl_int_prty_diagn_cd_insupd.updatetime_cc_injurydiagnosis3 as TRANS_STRT_DTTM
FROM
clm_insrbl_int_prty_diagn_cd_insupd;


END; ';