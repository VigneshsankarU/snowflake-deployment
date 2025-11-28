-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_LEGL_ACTN_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 

DECLARE
  END_DTTM timestamp;
  START_DTTM timestamp;
  run_id STRING;
  var_ContactroleTypecode char;
  prcs_id int;
  P_DEFAULT_STR_CD STRING;


BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
P_DEFAULT_STR_CD :=  (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''P_DEFAULT_STR_CD'' order by insert_ts desc limit 1);
var_ContactroleTypecode :=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''CONTACTROLETYPECODE'' order by insert_ts desc limit 1);


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


-- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''LEGL_ACTN_PRTY_ROLE_TYPE'' 

      AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM = ''cctl_contactrole.typecode''  

	AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

	AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component sq_prty_legl_action_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_prty_legl_action_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Matter_Name,
$2 as MTR_PublicID,
$3 as ClaimNumber,
$4 as CNT_PublicID,
$5 as ContactroleTypecode,
$6 as MatterTypecode,
$7 as prty_legl_action_start_date,
$8 as prty_legl_action_end_date,
$9 as Retired,
$10 as UpdateTime,
$11 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
Select Src.Matter_Name as Matter_Name,Src.MTR_PublicID as MTR_PublicID,Src.ClaimNumber as ClaimNumber,Src.CNT_PublicID as CNT_PublicID,Src.ContactroleTypecode as ContactroleTypecode,

Src.MatterTypecode as MatterTypecode,Src.prty_legl_action_start_date as prty_legl_action_start_date,

case when lkp_end_date.End_datetime is null  then Cast(''9999-12-31 23:59:59.999999''  AS TIMESTAMP)

else lkp_end_date.End_datetime end as prty_legl_action_end_date,Src.Retired as Retired,Src.UpdateTime as UpdateTime

from (

SELECT	cc_matter.Name_stg as Matter_Name, cc_matter.PublicID_stg AS MTR_PublicID, cc_claim.ClaimNumber_stg as ClaimNumber,

cc_contact.PublicID_stg AS CNT_PublicID, cctl_contactrole.typecode_stg as ContactroleTypecode, cc_claimcontactrole.UpdateTime_stg as UpdateTime,

cctl_mattertype.TYPECODE_stg as  MatterTypecode,cc_claimcontactrole.CreateTime_stg as prty_legl_action_start_date,

cc_claimcontactrole.ClaimContactID_stg as ClaimContactID,cc_claimcontactrole.Role_stg as Role1,

case when cc_claim.Retired_stg=0 	and cc_matter.Retired_stg=0 	and cc_contact.Retired_stg=0 then 0 else 1 end as Retired

FROM	DB_T_PROD_STAG.cc_claimcontactrole 

        JOIN DB_T_PROD_STAG.cc_matter 	ON cc_claimcontactrole.MatterID_stg=cc_matter.id_stg 

        JOIN DB_T_PROD_STAG.cc_claimcontact ON cc_claimcontactrole.ClaimContactID_stg = cc_claimcontact.id_stg 

        JOIN DB_T_PROD_STAG.cc_contact ON cc_claimcontact.ContactID_stg = cc_contact.ID_stg 

        JOIN ( 	select	cc_claim.id_stg,cc_claim.Retired_stg,cc_claim.ClaimNumber_stg from	DB_T_PROD_STAG.cc_claim inner join DB_T_PROD_STAG.cctl_claimstate 

		on cc_claim.State_stg= cctl_claimstate.id_stg 	where	cctl_claimstate.name_stg <> ''Draft'') cc_claim 

	    ON cc_matter.ClaimID_stg = cc_claim.id_stg

        JOIN DB_T_PROD_STAG.cctl_contactrole ON cctl_contactrole.ID_stg = cc_claimcontactrole.Role_stg 

        JOIN DB_T_PROD_STAG.cctl_contact on cctl_contact.id_stg=cc_contact.Subtype_stg

        JOIN DB_T_PROD_STAG.cctl_mattertype on cc_matter.MatterType_stg = cctl_mattertype.ID_stg

WHERE	cctl_contact.NAME_stg in (''Person'',''Adjudicator'',''User Contact'',''Vendor (Person)'',''Attorney'', ''Doctor'',	''Policy Person'',''Contact'') 

        and cc_claimcontactrole.UpdateTime_stg > (:START_DTTM)

	    and cc_claimcontactrole.UpdateTime_stg <= (:end_dttm)

	) Src

LEFT OUTER JOIN (SELECT  cc_claimcontactrole.ClaimContactID_stg as ClaimContactID, cc_claimcontactrole.Role_stg as Role2,

min(cc_claimcontactrole.UpdateTime_stg) as End_datetime

FROM 

	DB_T_PROD_STAG.cc_claimcontactrole 

	JOIN DB_T_PROD_STAG.cc_matter ON cc_claimcontactrole.MatterID_stg =cc_matter.id_stg 

	JOIN DB_T_PROD_STAG.cc_claimcontact ON cc_claimcontactrole.ClaimContactID_stg = cc_claimcontact.id_stg 

	JOIN DB_T_PROD_STAG.cc_contact ON cc_claimcontact.ContactID_stg = cc_contact.ID_stg 

	JOIN DB_T_PROD_STAG.cc_claim ON cc_matter.ClaimID_stg = cc_claim.id_stg

	JOIN DB_T_PROD_STAG.cctl_contactrole ON cctl_contactrole.ID_stg = cc_claimcontactrole.Role_stg 

	JOIN DB_T_PROD_STAG.cctl_contact on cctl_contact.id_stg =cc_contact.Subtype_stg

    JOIN DB_T_PROD_STAG.cctl_mattertype on cc_matter.MatterType_stg = cctl_mattertype.ID_stg

WHERE cctl_contact.NAME_stg in (''Person'',''Adjudicator'',''User Contact'',''Vendor (Person)'',''Attorney'', ''Doctor'',''Policy Person'',''Contact'') 

	and cc_claimcontactrole.Retired_stg <> 0

	group by ClaimContactID_stg, Role_stg ) lkp_end_date on Src.ClaimContactID = lkp_end_date.ClaimContactID

	and Src.Role1 = lkp_end_date.Role2
) SRC
)
);


-- Component LKP_TERADATA_ETL_REF_XLAT_CCTL_MATTERSTATUS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_CCTL_MATTERSTATUS AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
sq_prty_legl_action_x.source_record_id,
ROW_NUMBER() OVER(PARTITION BY sq_prty_legl_action_x.source_record_id ORDER BY LKP.TGT_IDNTFTN_VAL desc,LKP.SRC_IDNTFTN_VAL desc) RNK
FROM
sq_prty_legl_action_x
LEFT JOIN (
SELECT 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL
	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 
FROM 
	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''LEGL_ACTN_TYPE'' 
             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''cctl_mattertype.typecode'' 
		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 
		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
) LKP ON LKP.SRC_IDNTFTN_VAL = sq_prty_legl_action_x.MatterTypecode
QUALIFY RNK = 1
);


-- Component exp_flg_legl_actn_typecode, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_flg_legl_actn_typecode AS
(
SELECT
CASE WHEN LKP_TERADATA_ETL_REF_XLAT_CCTL_MATTERSTATUS.TGT_IDNTFTN_VAL IS NULL THEN ''UNK'' ELSE LKP_TERADATA_ETL_REF_XLAT_CCTL_MATTERSTATUS.TGT_IDNTFTN_VAL END as out_Typecode,
''SRC_SYS6'' as SRC_IDNTFTN_VAL,
sq_prty_legl_action_x.prty_legl_action_start_date as prty_legl_action_start_date,
sq_prty_legl_action_x.prty_legl_action_end_date as prty_legl_action_end_date,
sq_prty_legl_action_x.Retired as Retired,
sq_prty_legl_action_x.UpdateTime as UpdateTime,
sq_prty_legl_action_x.source_record_id
FROM
sq_prty_legl_action_x
INNER JOIN LKP_TERADATA_ETL_REF_XLAT_CCTL_MATTERSTATUS ON sq_prty_legl_action_x.source_record_id = LKP_TERADATA_ETL_REF_XLAT_CCTL_MATTERSTATUS.source_record_id
);


-- Component LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
exp_flg_legl_actn_typecode.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_flg_legl_actn_typecode.source_record_id ORDER BY LKP.TGT_IDNTFTN_VAL desc,LKP.SRC_IDNTFTN_VAL desc) RNK
FROM
exp_flg_legl_actn_typecode
LEFT JOIN (
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
) LKP ON LKP.SRC_IDNTFTN_VAL = exp_flg_legl_actn_typecode.SRC_IDNTFTN_VAL
QUALIFY RNK = 1
);


-- Component LKP_LEGL_ACTN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_LEGL_ACTN AS
(
SELECT
LKP.LEGL_ACTN_ID,
sq_prty_legl_action_x.source_record_id,
ROW_NUMBER() OVER(PARTITION BY sq_prty_legl_action_x.source_record_id ORDER BY LKP.LEGL_ACTN_ID asc,LKP.LEGL_ACTN_DESC asc,LKP.LEGL_ACTN_SUIT_NUM asc,LKP.LEGL_ACTN_STRT_DTTM asc,LKP.LEGL_ACTN_END_DTTM asc,LKP.COURT_LOC_LOCTR_ID asc,LKP.LEGL_ACTN_TYPE_CD asc,LKP.LEGL_ACTN_SUIT_TYPE_CD asc,LKP.CASE_NUM asc,LKP.BAD_FAITH_IND asc,LKP.SUBRGTN_RLTD_IND asc,LKP.PRCS_ID asc,LKP.SUBRGTN_LOAN_IND asc,LKP.WRT_OFF_AMT asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc) RNK
FROM
sq_prty_legl_action_x
INNER JOIN exp_flg_legl_actn_typecode ON sq_prty_legl_action_x.source_record_id = exp_flg_legl_actn_typecode.source_record_id
INNER JOIN LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD ON exp_flg_legl_actn_typecode.source_record_id = LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD.source_record_id
LEFT JOIN (
SELECT	LEGL_ACTN.LEGL_ACTN_ID as LEGL_ACTN_ID, LEGL_ACTN.LEGL_ACTN_DESC as LEGL_ACTN_DESC,
		LEGL_ACTN.LEGL_ACTN_STRT_DTTM as LEGL_ACTN_STRT_DTTM, LEGL_ACTN.LEGL_ACTN_END_DTTM as LEGL_ACTN_END_DTTM,
		LEGL_ACTN.COURT_LOC_LOCTR_ID as COURT_LOC_LOCTR_ID, LEGL_ACTN.LEGL_ACTN_SUIT_TYPE_CD as LEGL_ACTN_SUIT_TYPE_CD, 
		LEGL_ACTN.CASE_NUM AS CASE_NUM, LEGL_ACTN.BAD_FAITH_IND as BAD_FAITH_IND,
		LEGL_ACTN.SUBRGTN_RLTD_IND as SUBRGTN_RLTD_IND, LEGL_ACTN.PRCS_ID as PRCS_ID,
		LEGL_ACTN.SUBRGTN_LOAN_IND as SUBRGTN_LOAN_IND, LEGL_ACTN.WRT_OFF_AMT as WRT_OFF_AMT,
		LEGL_ACTN.EDW_STRT_DTTM as EDW_STRT_DTTM,
		LEGL_ACTN.EDW_END_DTTM as EDW_END_DTTM, LEGL_ACTN.LEGL_ACTN_SUIT_NUM as LEGL_ACTN_SUIT_NUM,
		LEGL_ACTN.SRC_SYS_CD as SRC_SYS_CD, LEGL_ACTN.LEGL_ACTN_TYPE_CD as LEGL_ACTN_TYPE_CD 
FROM	DB_T_PROD_CORE.LEGL_ACTN
qualify	row_number () over (
partition by LEGL_ACTN_SUIT_NUM,SRC_SYS_CD,LEGL_ACTN_TYPE_CD 
order by  EDW_END_DTTM desc)=1
) LKP ON LKP.LEGL_ACTN_SUIT_NUM = sq_prty_legl_action_x.MTR_PublicID AND LKP.SRC_SYS_CD = LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD.TGT_IDNTFTN_VAL AND LKP.LEGL_ACTN_TYPE_CD = exp_flg_legl_actn_typecode.out_Typecode
QUALIFY RNK = 1
);


-- Component exp_all_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source AS
(
SELECT
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ END as LEGL_ACTN_PRTY_ROLE_CD,
:PRCS_ID as PROCESS_ID,
LKP_LEGL_ACTN.LEGL_ACTN_ID as in_legl_actn_id,
CASE WHEN exp_flg_legl_actn_typecode.prty_legl_action_start_date IS NULL THEN TO_DATE ( ''01-01-1900'' , ''dd-mm-yyyy'' ) ELSE exp_flg_legl_actn_typecode.prty_legl_action_start_date END as o__LEGL_ACTN_PRTY_STRT_DT,
CASE WHEN LKP_3.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ IS NULL THEN 9999 ELSE LKP_4.INDIV_PRTY_ID /* replaced lookup LKP_INDIV_CLM_CTR */ END as in_PRTY_ID,
exp_flg_legl_actn_typecode.prty_legl_action_end_date as in_LEGL_PRTY_ACTN_END_DT,
CURRENT_TIMESTAMP as in_EDW_STRT_DTTM,
to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as in_EDW_END_DTTM,
exp_flg_legl_actn_typecode.Retired as Retired,
exp_flg_legl_actn_typecode.UpdateTime as Trans_strt_dt,
sq_prty_legl_action_x.source_record_id,
row_number() over (partition by sq_prty_legl_action_x.source_record_id order by sq_prty_legl_action_x.source_record_id) as RNK
FROM
sq_prty_legl_action_x
INNER JOIN exp_flg_legl_actn_typecode ON sq_prty_legl_action_x.source_record_id = exp_flg_legl_actn_typecode.source_record_id
INNER JOIN LKP_LEGL_ACTN ON exp_flg_legl_actn_typecode.source_record_id = LKP_LEGL_ACTN.source_record_id
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_prty_legl_action_x.ContactroleTypecode
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = sq_prty_legl_action_x.ContactroleTypecode
LEFT JOIN LKP_INDIV_CLM_CTR LKP_3 ON LKP_3.NK_PUBLC_ID = sq_prty_legl_action_x.CNT_PublicID
LEFT JOIN LKP_INDIV_CLM_CTR LKP_4 ON LKP_4.NK_PUBLC_ID = sq_prty_legl_action_x.CNT_PublicID
QUALIFY RNK = 1
);


-- Component LKP_PRTY_LEGL_ACTN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_LEGL_ACTN AS
(
SELECT
CASE WHEN LKP.PRTY_ID IS NULL THEN 1 ELSE 2 END as NewLookupRow,
--LKP.NewLookupRow,
LKP.LEGL_ACTN_ID,
LKP.LEGL_ACTN_PRTY_ROLE_CD,
LKP.LEGL_ACTN_PRTY_STRT_DTTM,
LKP.PRTY_ID,
LKP.LEGL_PRTY_ACTN_END_DTTM,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_all_source.in_PRTY_ID as in_PRTY_ID,
exp_all_source.in_LEGL_PRTY_ACTN_END_DT as in_LEGL_PRTY_ACTN_END_DT,
exp_all_source.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_all_source.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_all_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source.source_record_id ORDER BY  CASE WHEN LKP.PRTY_ID IS NULL THEN 1 ELSE 2 END asc,
LKP.LEGL_ACTN_ID asc,LKP.LEGL_ACTN_PRTY_ROLE_CD asc,LKP.LEGL_ACTN_PRTY_STRT_DTTM asc,LKP.PRTY_ID asc,LKP.LEGL_PRTY_ACTN_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_all_source
LEFT JOIN (
SELECT PRTY_LEGL_ACTN.LEGL_ACTN_PRTY_STRT_DTTM as LEGL_ACTN_PRTY_STRT_DTTM, PRTY_LEGL_ACTN.LEGL_PRTY_ACTN_END_DTTM as LEGL_PRTY_ACTN_END_DTTM, PRTY_LEGL_ACTN.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_LEGL_ACTN.EDW_END_DTTM as EDW_END_DTTM, PRTY_LEGL_ACTN.LEGL_ACTN_ID as LEGL_ACTN_ID, PRTY_LEGL_ACTN.PRTY_ID as PRTY_ID, PRTY_LEGL_ACTN.LEGL_ACTN_PRTY_ROLE_CD as LEGL_ACTN_PRTY_ROLE_CD FROM DB_T_PROD_CORE.PRTY_LEGL_ACTN QUALIFY ROW_NUMBER() OVER(PARTITION BY  PRTY_LEGL_ACTN.LEGL_ACTN_ID, PRTY_LEGL_ACTN.PRTY_ID , PRTY_LEGL_ACTN.LEGL_ACTN_PRTY_ROLE_CD  ORDER BY PRTY_LEGL_ACTN.EDW_END_DTTM DESC) = 1
) LKP ON LKP.LEGL_ACTN_ID = exp_all_source.in_legl_actn_id AND LKP.PRTY_ID = exp_all_source.in_PRTY_ID AND LKP.LEGL_ACTN_PRTY_ROLE_CD = exp_all_source.LEGL_ACTN_PRTY_ROLE_CD
QUALIFY RNK = 1
);


-- Component exp_src_lkp_rows, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_src_lkp_rows AS
(
SELECT

LKP_PRTY_LEGL_ACTN.NewLookupRow as NewLookupRow,
LKP_PRTY_LEGL_ACTN.LEGL_ACTN_ID as lkp_LEGL_ACTN_ID,
LKP_PRTY_LEGL_ACTN.LEGL_ACTN_PRTY_ROLE_CD as lkp_LEGL_ACTN_PRTY_ROLE_CD,
LKP_PRTY_LEGL_ACTN.LEGL_ACTN_PRTY_STRT_DTTM as lkp_LEGL_ACTN_PRTY_STRT_DT,
LKP_PRTY_LEGL_ACTN.PRTY_ID as lkp_PRTY_ID,
LKP_PRTY_LEGL_ACTN.LEGL_PRTY_ACTN_END_DTTM as lkp_LEGL_PRTY_ACTN_END_DT,
LKP_PRTY_LEGL_ACTN.EDW_STRT_DTTM as lk_EDW_STRT_DTTM,
LKP_PRTY_LEGL_ACTN.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_all_source.in_legl_actn_id as in_LEGL_ACTN_ID,
exp_all_source.LEGL_ACTN_PRTY_ROLE_CD as in_LEGL_ACTN_PRTY_ROLE_CD,
exp_all_source.o__LEGL_ACTN_PRTY_STRT_DT as in_LEGL_ACTN_PRTY_STRT_DT,
exp_all_source.in_PRTY_ID as in_PRTY_ID,
exp_all_source.in_LEGL_PRTY_ACTN_END_DT as in_LEGL_PRTY_ACTN_END_DT,
exp_all_source.PROCESS_ID as in_PROCESS_ID,
exp_all_source.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_all_source.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_all_source.Retired as Retired,
CASE WHEN LKP_PRTY_LEGL_ACTN.NewLookupRow = 1 THEN ''I'' ELSE CASE WHEN LKP_PRTY_LEGL_ACTN.NewLookupRow = 2 THEN ''U'' ELSE ''R'' END END as calc_ins_upd,
exp_all_source.Trans_strt_dt as Trans_strt_dt,
exp_all_source.source_record_id
FROM
exp_all_source
INNER JOIN LKP_PRTY_LEGL_ACTN ON exp_all_source.source_record_id = LKP_PRTY_LEGL_ACTN.source_record_id
);


-- Component rtr_insert_update_flag_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE rtr_insert_update_flag_Insert AS
(SELECT
exp_src_lkp_rows.lkp_LEGL_ACTN_ID as lkp_LEGL_ACTN_ID,
exp_src_lkp_rows.lkp_LEGL_ACTN_PRTY_ROLE_CD as lkp_LEGL_ACTN_PRTY_ROLE_CD,
exp_src_lkp_rows.lkp_LEGL_ACTN_PRTY_STRT_DT as lkp_LEGL_ACTN_PRTY_STRT_DT,
exp_src_lkp_rows.lkp_PRTY_ID as lkp_PRTY_ID,
exp_src_lkp_rows.lkp_LEGL_PRTY_ACTN_END_DT as lkp_LEGL_PRTY_ACTN_END_DT,
exp_src_lkp_rows.lk_EDW_STRT_DTTM as lk_EDW_STRT_DTTM,
exp_src_lkp_rows.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_src_lkp_rows.in_LEGL_ACTN_ID as in_LEGL_ACTN_ID,
exp_src_lkp_rows.in_LEGL_ACTN_PRTY_ROLE_CD as in_LEGL_ACTN_PRTY_ROLE_CD,
exp_src_lkp_rows.in_LEGL_ACTN_PRTY_STRT_DT as in_LEGL_ACTN_PRTY_STRT_DT,
exp_src_lkp_rows.in_PRTY_ID as in_PRTY_ID,
exp_src_lkp_rows.in_LEGL_PRTY_ACTN_END_DT as in_LEGL_PRTY_ACTN_END_DT,
exp_src_lkp_rows.in_PROCESS_ID as in_PROCESS_ID,
exp_src_lkp_rows.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_src_lkp_rows.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_src_lkp_rows.NewLookupRow as NewLookupRow,
exp_src_lkp_rows.Retired as Retired,
exp_src_lkp_rows.calc_ins_upd as calc_ins_upd,
exp_src_lkp_rows.Trans_strt_dt as Trans_strt_dt,
exp_src_lkp_rows.source_record_id
FROM
exp_src_lkp_rows
WHERE ( exp_src_lkp_rows.calc_ins_upd = ''I'' ) OR ( exp_src_lkp_rows.Retired = 0 AND exp_src_lkp_rows.lkp_EDW_END_DTTM != TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ));


-- Component rtr_insert_update_flag_Retire, Type ROUTER Output Group Retire
CREATE OR REPLACE TEMPORARY TABLE rtr_insert_update_flag_Retire AS
(SELECT
exp_src_lkp_rows.lkp_LEGL_ACTN_ID as lkp_LEGL_ACTN_ID,
exp_src_lkp_rows.lkp_LEGL_ACTN_PRTY_ROLE_CD as lkp_LEGL_ACTN_PRTY_ROLE_CD,
exp_src_lkp_rows.lkp_LEGL_ACTN_PRTY_STRT_DT as lkp_LEGL_ACTN_PRTY_STRT_DT,
exp_src_lkp_rows.lkp_PRTY_ID as lkp_PRTY_ID,
exp_src_lkp_rows.lkp_LEGL_PRTY_ACTN_END_DT as lkp_LEGL_PRTY_ACTN_END_DT,
exp_src_lkp_rows.lk_EDW_STRT_DTTM as lk_EDW_STRT_DTTM,
exp_src_lkp_rows.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_src_lkp_rows.in_LEGL_ACTN_ID as in_LEGL_ACTN_ID,
exp_src_lkp_rows.in_LEGL_ACTN_PRTY_ROLE_CD as in_LEGL_ACTN_PRTY_ROLE_CD,
exp_src_lkp_rows.in_LEGL_ACTN_PRTY_STRT_DT as in_LEGL_ACTN_PRTY_STRT_DT,
exp_src_lkp_rows.in_PRTY_ID as in_PRTY_ID,
exp_src_lkp_rows.in_LEGL_PRTY_ACTN_END_DT as in_LEGL_PRTY_ACTN_END_DT,
exp_src_lkp_rows.in_PROCESS_ID as in_PROCESS_ID,
exp_src_lkp_rows.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_src_lkp_rows.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_src_lkp_rows.NewLookupRow as NewLookupRow,
exp_src_lkp_rows.Retired as Retired,
exp_src_lkp_rows.calc_ins_upd as calc_ins_upd,
exp_src_lkp_rows.Trans_strt_dt as Trans_strt_dt,
exp_src_lkp_rows.source_record_id
FROM
exp_src_lkp_rows
WHERE exp_src_lkp_rows.calc_ins_upd = ''R'' and exp_src_lkp_rows.Retired != 0 and exp_src_lkp_rows.lkp_EDW_END_DTTM = TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component rtr_insert_update_flag_Update, Type ROUTER Output Group Update
CREATE OR REPLACE TEMPORARY TABLE rtr_insert_update_flag_Update AS
(SELECT
exp_src_lkp_rows.lkp_LEGL_ACTN_ID as lkp_LEGL_ACTN_ID,
exp_src_lkp_rows.lkp_LEGL_ACTN_PRTY_ROLE_CD as lkp_LEGL_ACTN_PRTY_ROLE_CD,
exp_src_lkp_rows.lkp_LEGL_ACTN_PRTY_STRT_DT as lkp_LEGL_ACTN_PRTY_STRT_DT,
exp_src_lkp_rows.lkp_PRTY_ID as lkp_PRTY_ID,
exp_src_lkp_rows.lkp_LEGL_PRTY_ACTN_END_DT as lkp_LEGL_PRTY_ACTN_END_DT,
exp_src_lkp_rows.lk_EDW_STRT_DTTM as lk_EDW_STRT_DTTM,
exp_src_lkp_rows.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_src_lkp_rows.in_LEGL_ACTN_ID as in_LEGL_ACTN_ID,
exp_src_lkp_rows.in_LEGL_ACTN_PRTY_ROLE_CD as in_LEGL_ACTN_PRTY_ROLE_CD,
exp_src_lkp_rows.in_LEGL_ACTN_PRTY_STRT_DT as in_LEGL_ACTN_PRTY_STRT_DT,
exp_src_lkp_rows.in_PRTY_ID as in_PRTY_ID,
exp_src_lkp_rows.in_LEGL_PRTY_ACTN_END_DT as in_LEGL_PRTY_ACTN_END_DT,
exp_src_lkp_rows.in_PROCESS_ID as in_PROCESS_ID,
exp_src_lkp_rows.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_src_lkp_rows.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_src_lkp_rows.NewLookupRow as NewLookupRow,
exp_src_lkp_rows.Retired as Retired,
exp_src_lkp_rows.calc_ins_upd as calc_ins_upd,
exp_src_lkp_rows.Trans_strt_dt as Trans_strt_dt,
exp_src_lkp_rows.source_record_id
FROM
exp_src_lkp_rows
WHERE exp_src_lkp_rows.calc_ins_upd = ''U'' AND exp_src_lkp_rows.lkp_EDW_END_DTTM = TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component upd_new_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_new_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_insert_update_flag_Insert.in_LEGL_ACTN_ID as in_LEGL_ACTN_ID,
rtr_insert_update_flag_Insert.in_LEGL_ACTN_PRTY_ROLE_CD as in_LEGL_ACTN_PRTY_ROLE_CD,
rtr_insert_update_flag_Insert.in_LEGL_ACTN_PRTY_STRT_DT as in_LEGL_ACTN_PRTY_STRT_DT,
rtr_insert_update_flag_Insert.in_PRTY_ID as in_PRTY_ID,
rtr_insert_update_flag_Insert.in_LEGL_PRTY_ACTN_END_DT as in_LEGL_PRTY_ACTN_END_DT,
rtr_insert_update_flag_Insert.in_PROCESS_ID as in_PROCESS_ID,
rtr_insert_update_flag_Insert.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
rtr_insert_update_flag_Insert.in_EDW_END_DTTM as in_EDW_END_DTTM,
rtr_insert_update_flag_Insert.Retired as Retired1,
rtr_insert_update_flag_Insert.Trans_strt_dt as Trans_strt_dt1,
0 as UPDATE_STRATEGY_ACTION,
rtr_insert_update_flag_Insert.source_record_id
FROM
rtr_insert_update_flag_Insert
);


-- Component upd_ins_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_ins_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_insert_update_flag_Update.lkp_LEGL_ACTN_ID as in_LEGL_ACTN_ID,
rtr_insert_update_flag_Update.lkp_LEGL_ACTN_PRTY_ROLE_CD as in_LEGL_ACTN_PRTY_ROLE_CD,
rtr_insert_update_flag_Update.in_LEGL_ACTN_PRTY_STRT_DT as in_LEGL_ACTN_PRTY_STRT_DT,
rtr_insert_update_flag_Update.in_PRTY_ID as in_PRTY_ID,
rtr_insert_update_flag_Update.in_LEGL_PRTY_ACTN_END_DT as in_LEGL_PRTY_ACTN_END_DT,
rtr_insert_update_flag_Update.in_PROCESS_ID as in_PROCESS_ID,
rtr_insert_update_flag_Update.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
rtr_insert_update_flag_Update.in_EDW_END_DTTM as in_EDW_END_DTTM,
rtr_insert_update_flag_Update.Retired as Retired3,
rtr_insert_update_flag_Update.Trans_strt_dt as Trans_strt_dt3,
0 as UPDATE_STRATEGY_ACTION,
rtr_insert_update_flag_Update.source_record_id
FROM
rtr_insert_update_flag_Update
);


-- Component upd_prty_legl_actn_Retire_Reject, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prty_legl_actn_Retire_Reject AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_insert_update_flag_Retire.lkp_LEGL_ACTN_ID as lkp_LEGL_ACTN_ID3,
rtr_insert_update_flag_Retire.lkp_LEGL_ACTN_PRTY_ROLE_CD as lkp_LEGL_ACTN_PRTY_ROLE_CD3,
rtr_insert_update_flag_Retire.lkp_PRTY_ID as lkp_PRTY_ID3,
rtr_insert_update_flag_Retire.lk_EDW_STRT_DTTM as lk_EDW_STRT_DTTM3,
rtr_insert_update_flag_Retire.in_PROCESS_ID as in_PROCESS_ID4,
rtr_insert_update_flag_Retire.Trans_strt_dt as Trans_strt_dt4,
1 as UPDATE_STRATEGY_ACTION,rtr_insert_update_flag_Retire.source_record_id
FROM
rtr_insert_update_flag_Retire
);


-- Component upd_update, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_update AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_insert_update_flag_Update.lkp_LEGL_ACTN_ID as lkp_LEGL_ACTN_ID3,
rtr_insert_update_flag_Update.lkp_LEGL_ACTN_PRTY_ROLE_CD as lkp_LEGL_ACTN_PRTY_ROLE_CD3,
rtr_insert_update_flag_Update.lkp_PRTY_ID as lkp_PRTY_ID3,
rtr_insert_update_flag_Update.lk_EDW_STRT_DTTM as lk_EDW_STRT_DTTM3,
rtr_insert_update_flag_Update.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM3,
rtr_insert_update_flag_Update.Retired as Retired3,
rtr_insert_update_flag_Update.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM3,
rtr_insert_update_flag_Update.in_PROCESS_ID as in_PROCESS_ID3,
rtr_insert_update_flag_Update.Trans_strt_dt as Trans_strt_dt3,
1 as UPDATE_STRATEGY_ACTION,
rtr_insert_update_flag_Update.source_record_id
FROM
rtr_insert_update_flag_Update
);


-- Component fil_prty_lgl_actn_upd_ins, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_prty_lgl_actn_upd_ins AS
(
SELECT
upd_ins_upd.in_LEGL_ACTN_ID as in_LEGL_ACTN_ID,
upd_ins_upd.in_LEGL_ACTN_PRTY_ROLE_CD as in_LEGL_ACTN_PRTY_ROLE_CD,
upd_ins_upd.in_LEGL_ACTN_PRTY_STRT_DT as in_LEGL_ACTN_PRTY_STRT_DT,
upd_ins_upd.in_PRTY_ID as in_PRTY_ID,
upd_ins_upd.in_LEGL_PRTY_ACTN_END_DT as in_LEGL_PRTY_ACTN_END_DT,
upd_ins_upd.in_PROCESS_ID as in_PROCESS_ID,
upd_ins_upd.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
upd_ins_upd.in_EDW_END_DTTM as in_EDW_END_DTTM,
upd_ins_upd.Retired3 as Retired3,
upd_ins_upd.Trans_strt_dt3 as Trans_strt_dt3,
upd_ins_upd.source_record_id
FROM
upd_ins_upd
WHERE upd_ins_upd.Retired3 = 0
);


-- Component exp_pass_to_target_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_insert AS
(
SELECT
upd_new_ins.in_LEGL_ACTN_ID as in_LEGL_ACTN_ID,
upd_new_ins.in_LEGL_ACTN_PRTY_ROLE_CD as in_LEGL_ACTN_PRTY_ROLE_CD,
upd_new_ins.in_LEGL_ACTN_PRTY_STRT_DT as in_LEGL_ACTN_PRTY_STRT_DT,
upd_new_ins.in_PRTY_ID as in_PRTY_ID,
upd_new_ins.in_LEGL_PRTY_ACTN_END_DT as in_LEGL_PRTY_ACTN_END_DT,
upd_new_ins.in_PROCESS_ID as in_PROCESS_ID,
upd_new_ins.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
CASE WHEN upd_new_ins.Retired1 != 0 THEN CURRENT_TIMESTAMP ELSE upd_new_ins.in_EDW_END_DTTM END as o_EDW_END_DTTM,
upd_new_ins.Trans_strt_dt1 as Trans_strt_dt1,
CASE WHEN upd_new_ins.Retired1 != 0 THEN upd_new_ins.Trans_strt_dt1 ELSE to_timestamp ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as Trans_end_dt,
upd_new_ins.source_record_id
FROM
upd_new_ins
);


-- Component exp_prty_legl_actn_Retire_Reject, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prty_legl_actn_Retire_Reject AS
(
SELECT
upd_prty_legl_actn_Retire_Reject.lkp_LEGL_ACTN_ID3 as lkp_LEGL_ACTN_ID3,
upd_prty_legl_actn_Retire_Reject.lkp_LEGL_ACTN_PRTY_ROLE_CD3 as lkp_LEGL_ACTN_PRTY_ROLE_CD3,
upd_prty_legl_actn_Retire_Reject.lkp_PRTY_ID3 as lkp_PRTY_ID3,
upd_prty_legl_actn_Retire_Reject.lk_EDW_STRT_DTTM3 as lk_EDW_STRT_DTTM3,
CURRENT_TIMESTAMP as o_EDW_END_DTTM31,
upd_prty_legl_actn_Retire_Reject.Trans_strt_dt4 as Trans_strt_dt41,
upd_prty_legl_actn_Retire_Reject.source_record_id
FROM
upd_prty_legl_actn_Retire_Reject
);


-- Component tgt_prty_legl_actn_Retire_Reject, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_LEGL_ACTN
USING exp_prty_legl_actn_Retire_Reject ON (PRTY_LEGL_ACTN.LEGL_ACTN_ID = exp_prty_legl_actn_Retire_Reject.lkp_LEGL_ACTN_ID3 AND PRTY_LEGL_ACTN.LEGL_ACTN_PRTY_ROLE_CD = exp_prty_legl_actn_Retire_Reject.lkp_LEGL_ACTN_PRTY_ROLE_CD3 AND PRTY_LEGL_ACTN.PRTY_ID = exp_prty_legl_actn_Retire_Reject.lkp_PRTY_ID3 AND PRTY_LEGL_ACTN.EDW_STRT_DTTM = exp_prty_legl_actn_Retire_Reject.lk_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
LEGL_ACTN_ID = exp_prty_legl_actn_Retire_Reject.lkp_LEGL_ACTN_ID3,
LEGL_ACTN_PRTY_ROLE_CD = exp_prty_legl_actn_Retire_Reject.lkp_LEGL_ACTN_PRTY_ROLE_CD3,
PRTY_ID = exp_prty_legl_actn_Retire_Reject.lkp_PRTY_ID3,
EDW_STRT_DTTM = exp_prty_legl_actn_Retire_Reject.lk_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_prty_legl_actn_Retire_Reject.o_EDW_END_DTTM31,
TRANS_END_DTTM = exp_prty_legl_actn_Retire_Reject.Trans_strt_dt41;


-- Component fil_prty_lgl_actn_upd_update, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_prty_lgl_actn_upd_update AS
(
SELECT
upd_update.lkp_LEGL_ACTN_ID3 as lkp_LEGL_ACTN_ID3,
upd_update.lkp_LEGL_ACTN_PRTY_ROLE_CD3 as lkp_LEGL_ACTN_PRTY_ROLE_CD3,
upd_update.lkp_PRTY_ID3 as lkp_PRTY_ID3,
upd_update.lk_EDW_STRT_DTTM3 as lk_EDW_STRT_DTTM3,
upd_update.in_EDW_STRT_DTTM3 as in_EDW_STRT_DTTM3,
upd_update.Retired3 as Retired3,
upd_update.lkp_EDW_END_DTTM3 as lkp_EDW_END_DTTM3,
upd_update.in_PROCESS_ID3 as in_PROCESS_ID3,
upd_update.Trans_strt_dt3 as Trans_strt_dt3,
upd_update.source_record_id
FROM
upd_update
WHERE upd_update.lkp_EDW_END_DTTM3 = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
);


-- Component exp_pass_to_target_upd_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_upd_ins AS
(
SELECT
fil_prty_lgl_actn_upd_ins.in_LEGL_ACTN_ID as in_LEGL_ACTN_ID,
fil_prty_lgl_actn_upd_ins.in_LEGL_ACTN_PRTY_ROLE_CD as in_LEGL_ACTN_PRTY_ROLE_CD,
fil_prty_lgl_actn_upd_ins.in_LEGL_ACTN_PRTY_STRT_DT as in_LEGL_ACTN_PRTY_STRT_DT,
fil_prty_lgl_actn_upd_ins.in_PRTY_ID as in_PRTY_ID,
fil_prty_lgl_actn_upd_ins.in_LEGL_PRTY_ACTN_END_DT as in_LEGL_PRTY_ACTN_END_DT,
fil_prty_lgl_actn_upd_ins.in_PROCESS_ID as in_PROCESS_ID,
fil_prty_lgl_actn_upd_ins.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
fil_prty_lgl_actn_upd_ins.in_EDW_END_DTTM as in_EDW_END_DTTM,
fil_prty_lgl_actn_upd_ins.Trans_strt_dt3 as Trans_strt_dt3,
fil_prty_lgl_actn_upd_ins.source_record_id
FROM
fil_prty_lgl_actn_upd_ins
);


-- Component tgt_prty_legl_actn_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_LEGL_ACTN
(
LEGL_ACTN_ID,
LEGL_ACTN_PRTY_ROLE_CD,
LEGL_ACTN_PRTY_STRT_DTTM,
PRTY_ID,
LEGL_PRTY_ACTN_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_target_insert.in_LEGL_ACTN_ID as LEGL_ACTN_ID,
exp_pass_to_target_insert.in_LEGL_ACTN_PRTY_ROLE_CD as LEGL_ACTN_PRTY_ROLE_CD,
exp_pass_to_target_insert.in_LEGL_ACTN_PRTY_STRT_DT as LEGL_ACTN_PRTY_STRT_DTTM,
exp_pass_to_target_insert.in_PRTY_ID as PRTY_ID,
exp_pass_to_target_insert.in_LEGL_PRTY_ACTN_END_DT as LEGL_PRTY_ACTN_END_DTTM,
exp_pass_to_target_insert.in_PROCESS_ID as PRCS_ID,
exp_pass_to_target_insert.in_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_target_insert.o_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_target_insert.Trans_strt_dt1 as TRANS_STRT_DTTM,
exp_pass_to_target_insert.Trans_end_dt as TRANS_END_DTTM
FROM
exp_pass_to_target_insert;


-- Component tgt_prty_legl_actn_upd_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_LEGL_ACTN
(
LEGL_ACTN_ID,
LEGL_ACTN_PRTY_ROLE_CD,
LEGL_ACTN_PRTY_STRT_DTTM,
PRTY_ID,
LEGL_PRTY_ACTN_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_pass_to_target_upd_ins.in_LEGL_ACTN_ID as LEGL_ACTN_ID,
exp_pass_to_target_upd_ins.in_LEGL_ACTN_PRTY_ROLE_CD as LEGL_ACTN_PRTY_ROLE_CD,
exp_pass_to_target_upd_ins.in_LEGL_ACTN_PRTY_STRT_DT as LEGL_ACTN_PRTY_STRT_DTTM,
exp_pass_to_target_upd_ins.in_PRTY_ID as PRTY_ID,
exp_pass_to_target_upd_ins.in_LEGL_PRTY_ACTN_END_DT as LEGL_PRTY_ACTN_END_DTTM,
exp_pass_to_target_upd_ins.in_PROCESS_ID as PRCS_ID,
exp_pass_to_target_upd_ins.in_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_target_upd_ins.in_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_target_upd_ins.Trans_strt_dt3 as TRANS_STRT_DTTM
FROM
exp_pass_to_target_upd_ins;


-- Component exp_pass_to_target_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_upd AS
(
SELECT
fil_prty_lgl_actn_upd_update.lkp_LEGL_ACTN_ID3 as lkp_LEGL_ACTN_ID3,
fil_prty_lgl_actn_upd_update.lkp_LEGL_ACTN_PRTY_ROLE_CD3 as lkp_LEGL_ACTN_PRTY_ROLE_CD3,
fil_prty_lgl_actn_upd_update.lkp_PRTY_ID3 as lkp_PRTY_ID3,
fil_prty_lgl_actn_upd_update.lk_EDW_STRT_DTTM3 as lk_EDW_STRT_DTTM3,
CASE
  WHEN fil_prty_lgl_actn_upd_update.Retired3 != 0 THEN CURRENT_TIMESTAMP()
  ELSE DATEADD (
    ''second'',
    -1,
    fil_prty_lgl_actn_upd_update.in_EDW_STRT_DTTM3
  )
END as o_EDW_END_DTTM31,
CASE
  WHEN fil_prty_lgl_actn_upd_update.Retired3 != 0 THEN CURRENT_TIMESTAMP()
  ELSE DATEADD (
    ''second'',
    -1,
    fil_prty_lgl_actn_upd_update.Trans_strt_dt3
  )
END as Trans_strt_dt31,
fil_prty_lgl_actn_upd_update.source_record_id
FROM
fil_prty_lgl_actn_upd_update
);


-- Component tgt_prty_legl_actn_upd, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_LEGL_ACTN
USING exp_pass_to_target_upd ON (PRTY_LEGL_ACTN.LEGL_ACTN_ID = exp_pass_to_target_upd.lkp_LEGL_ACTN_ID3 AND PRTY_LEGL_ACTN.LEGL_ACTN_PRTY_ROLE_CD = exp_pass_to_target_upd.lkp_LEGL_ACTN_PRTY_ROLE_CD3 AND PRTY_LEGL_ACTN.PRTY_ID = exp_pass_to_target_upd.lkp_PRTY_ID3 AND PRTY_LEGL_ACTN.EDW_STRT_DTTM = exp_pass_to_target_upd.lk_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
LEGL_ACTN_ID = exp_pass_to_target_upd.lkp_LEGL_ACTN_ID3,
LEGL_ACTN_PRTY_ROLE_CD = exp_pass_to_target_upd.lkp_LEGL_ACTN_PRTY_ROLE_CD3,
PRTY_ID = exp_pass_to_target_upd.lkp_PRTY_ID3,
EDW_STRT_DTTM = exp_pass_to_target_upd.lk_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_target_upd.o_EDW_END_DTTM31,
TRANS_END_DTTM = exp_pass_to_target_upd.Trans_strt_dt31;


END; 
';