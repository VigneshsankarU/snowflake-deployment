-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_CLM_EXPSR_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 

DECLARE
  END_DTTM timestamp;
  START_DTTM timestamp;
  run_id STRING;
  prcs_id int;
  P_DEFAULT_STR_CD STRING;


BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
P_DEFAULT_STR_CD :=  (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''P_DEFAULT_STR_CD'' order by insert_ts desc limit 1);

-- Component LKP_BUSN, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_BUSN AS
(
SELECT BUSN.BUSN_PRTY_ID as BUSN_PRTY_ID, BUSN.SRC_SYS_CD as SRC_SYS_CD, BUSN.TAX_BRAKT_CD as TAX_BRAKT_CD, BUSN.ORG_TYPE_CD as ORG_TYPE_CD, BUSN.GICS_SBIDSTRY_CD as GICS_SBIDSTRY_CD, BUSN.LIFCYCL_CD as LIFCYCL_CD, BUSN.PRTY_TYPE_CD as PRTY_TYPE_CD, BUSN.BUSN_END_DTTM as BUSN_END_DTTM, BUSN.BUSN_STRT_DTTM as BUSN_STRT_DTTM, BUSN.INC_IND as INC_IND, BUSN.EDW_STRT_DTTM as EDW_STRT_DTTM, BUSN.EDW_END_DTTM as EDW_END_DTTM, BUSN.BUSN_CTGY_CD as BUSN_CTGY_CD, BUSN.NK_BUSN_CD as NK_BUSN_CD 
FROM DB_T_PROD_CORE.BUSN 
QUALIFY ROW_NUMBER () OVER (PARTITION BY NK_BUSN_CD,BUSN_CTGY_CD ORDER BY EDW_END_DTTM DESC )=1
);


-- Component LKP_CLM, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM AS
(
SELECT CLM.CLM_ID as CLM_ID, CLM.CLM_TYPE_CD as CLM_TYPE_CD, CLM.CLM_MDIA_TYPE_CD as CLM_MDIA_TYPE_CD, CLM.CLM_SUBMTL_TYPE_CD as CLM_SUBMTL_TYPE_CD, CLM.ACDNT_TYPE_CD as ACDNT_TYPE_CD, CLM.CLM_CTGY_TYPE_CD as CLM_CTGY_TYPE_CD, CLM.ADDL_INSRNC_PLN_IND as ADDL_INSRNC_PLN_IND, CLM.EMPLMT_RLTD_IND as EMPLMT_RLTD_IND, CLM.ATTNY_INVLVMT_IND as ATTNY_INVLVMT_IND, CLM.CLM_PRIR_IND as CLM_PRIR_IND, CLM.PMT_MODE_CD as PMT_MODE_CD, CLM.CLM_OBLGTN_TYPE_CD as CLM_OBLGTN_TYPE_CD, CLM.SUBRGTN_ELGBL_CD as SUBRGTN_ELGBL_CD, CLM.SUBRGTN_ELGBLY_RSN_CD as SUBRGTN_ELGBLY_RSN_CD, CLM.CURY_CD as CURY_CD, CLM.INCDT_EV_ID as INCDT_EV_ID, CLM.INSRD_AT_FAULT_IND as INSRD_AT_FAULT_IND, CLM.CVGE_IN_QUES_IND as CVGE_IN_QUES_IND, CLM.EXTNT_OF_FIRE_DMG_TYPE_CD as EXTNT_OF_FIRE_DMG_TYPE_CD, CLM.VFYD_CLM_IND as VFYD_CLM_IND, CLM.PRCS_ID as PRCS_ID, CLM.CLM_STRT_DTTM as CLM_STRT_DTTM, CLM.CLM_END_DTTM as CLM_END_DTTM, CLM.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM.EDW_END_DTTM as EDW_END_DTTM, CLM.TRANS_STRT_DTTM as TRANS_STRT_DTTM, CLM.LGCY_CLM_NUM as LGCY_CLM_NUM, CLM.CLM_NUM as CLM_NUM, CLM.SRC_SYS_CD as SRC_SYS_CD FROM DB_T_PROD_CORE.CLM  QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM.CLM_NUM,CLM.SRC_SYS_CD  ORDER BY CLM.EDW_END_DTTM desc) = 1
);


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



-- Component LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY AS
(
SELECT 

 TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

 ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

 DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

 TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM IN (''BUSN_CTGY'',''ORG_TYPE'',''PRTY_TYPE'')

 AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''derived'', ''cctl_contact.typecode'',''cctl_contact.name'')

 AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''DS'',''GW'')

 AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_CONTACTROLE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_CONTACTROLE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_CLM_ROLE_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''cctl_contactrole.TYPECODE'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		---AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_SRC_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_SRC_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component SQ_cc_exp_party_x, Type SOURCE 
CREATE
OR REPLACE TEMPORARY TABLE SQ_cc_exp_party_x AS
SELECT
  $1 AS clm_expsr_nk,
  $2 AS claimnumber,
  $3 AS exposurepartyrolecode,
  $4 AS claimassignmentdate,
  $5 AS contactrole_startdate,
  $6 AS contactrole_updatedate,
  $7 AS exposureparty_key,
  $8 AS exposurepartytype,
  $9 AS src_cd,
  $10 AS Retired,
  $11 AS source_record_id
FROM
  (
    SELECT
      SRC.*,
      ROW_NUMBER() OVER (
        ORDER BY
          1
      ) AS source_record_id
    FROM
      (
        SELECT
          cc_exp_party_x.clm_expsr_nk,
          cc_exp_party_x.claimnumber,
          cc_exp_party_x.exposurepartyrolecode,
          cc_exp_party_x.claimassignmentdate,
          cc_exp_party_x.createTime,
          cc_exp_party_x.UpdateTime,
          cc_exp_party_x.exposureparty_key,
          cc_exp_party_x.exposurepartytype,
          ''SRC_SYS6'' AS src_cd,
          cc_exp_party_x.Retired
        FROM
          (
            SELECT
              cc_exposure.publicid_stg AS clm_expsr_nk,
              cc_claim.claimnumber_stg AS claimnumber,
              cctl_contactrole.TYPECODE_stg AS exposurepartyrolecode,
              cc_claim.assignmentdate_stg AS claimassignmentdate,
              cc_claimcontactrole.CreateTime_stg AS createtime,
              cc_claimcontactrole.UpdateTime_stg AS updatetime,
              UPPER(cc_contact.PublicID_stg) AS exposureparty_key,
              cctl_contact.name_stg AS exposurepartytype,
              CASE
                WHEN cc_claim.Retired_stg = 0
                AND cc_claimcontact.Retired_stg = 0
                AND cc_contact.Retired_stg = 0
                AND cc_exposure.Retired_stg = 0 THEN 0
                ELSE 1
              END AS Retired
            FROM
              DB_T_PROD_STAG.cc_exposure
              INNER JOIN (
                SELECT
                  cc_claim.id_stg AS claim_id_stg,
                  cc_claim.claimnumber_stg,
                  cc_claim.State_stg,
                  cc_claim.Retired_stg,
                  cc_claim.assignmentdate_stg
                FROM
                  DB_T_PROD_STAG.cc_claim
                  INNER JOIN DB_T_PROD_STAG.cctl_claimstate ON cc_claim.State_stg = cctl_claimstate.id_stg
                WHERE
                  cctl_claimstate.name_stg <> ''Draft''
              ) AS cc_claim ON cc_exposure.ClaimID_stg = cc_claim.claim_id_stg
              INNER JOIN (
                SELECT
                  ExposureID_stg,
                  CreateTime_stg,
                  UpdateTime_stg,
                  role_stg,
                  ClaimContactID_stg
                FROM
                  DB_T_PROD_STAG.cc_claimcontactrole
                WHERE
                  Retired_stg = 0
              ) AS cc_claimcontactrole ON cc_exposure.id_stg = cc_claimcontactrole.ExposureID_stg
              INNER JOIN DB_T_PROD_STAG.cctl_contactrole ON cc_claimcontactrole.role_stg = cctl_contactrole.id_stg
              INNER JOIN DB_T_PROD_STAG.cc_claimcontact ON cc_claimcontactrole.ClaimContactID_stg = cc_claimcontact.id_stg
              INNER JOIN DB_T_PROD_STAG.cc_contact ON cc_claimcontact.ContactID_stg = cc_contact.id_stg
              INNER JOIN DB_T_PROD_STAG.cctl_contact ON cc_contact.subtype_stg = cctl_contact.id_stg
            WHERE
              cc_claimcontactrole.UpdateTime_stg > :start_dttm
              AND cc_claimcontactrole.UpdateTime_stg <= :end_dttm
          ) AS cc_exp_party_x
      ) AS SRC
  );

-- Component exp_all_source_data, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source_data AS
(
SELECT
SQ_cc_exp_party_x.clm_expsr_nk as in_clm_expsr_nk,
SQ_cc_exp_party_x.exposurepartyrolecode as exposurepartyrolecode,
SQ_cc_exp_party_x.claimassignmentdate as claimassignmentdate,
SQ_cc_exp_party_x.contactrole_startdate as contactrole_startdate,
SQ_cc_exp_party_x.contactrole_updatedate as contactrole_updatedate,
SQ_cc_exp_party_x.exposureparty_key as exposureparty_key,
SQ_cc_exp_party_x.exposurepartytype as exposurepartytype,
SQ_cc_exp_party_x.Retired as Retired,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as v_src_cd,
LKP_2.CLM_ID /* replaced lookup LKP_CLM */ as out_clm_id,
SQ_cc_exp_party_x.source_record_id,
row_number() over (partition by SQ_cc_exp_party_x.source_record_id order by SQ_cc_exp_party_x.source_record_id) as RNK
FROM
SQ_cc_exp_party_x
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_cc_exp_party_x.src_cd
LEFT JOIN LKP_CLM LKP_2 ON LKP_2.CLM_NUM = SQ_cc_exp_party_x.claimnumber AND LKP_2.SRC_SYS_CD = v_src_cd
QUALIFY RNK = 1
);


-- Component LKP_CLM_EXPSR_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM_EXPSR_ID AS
(
SELECT
LKP.CLM_EXPSR_ID,
exp_all_source_data.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_source_data.source_record_id ORDER BY LKP.CLM_EXPSR_ID desc,LKP.CLMNT_PRTY_ID desc,LKP.CLM_EXPSR_NAME desc,LKP.CLM_EXPSR_RPTD_DTTM desc,LKP.CLM_EXPSR_OTH_CARIER_CVGE_IND desc,LKP.CLM_ID desc,LKP.CVGE_FEAT_ID desc,LKP.INSRBL_INT_ID desc,LKP.PRCS_ID desc,LKP.COTTER_CLM_IND desc,LKP.LOSS_PRTY_TYPE_CD desc,LKP.NK_SRC_KEY desc,LKP.HOLDBACK_IND desc,LKP.HOLDBACK_AMT desc,LKP.HOLDBACK_REIMBURSED_IND desc,LKP.ROOF_RPLACEMT_IND desc,LKP.CLM_EXPSR_TYPE_CD desc,LKP.CLM_EXPSR_STRT_DTTM desc,LKP.CLM_EXPSR_END_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK
FROM
exp_all_source_data
LEFT JOIN (
SELECT CLM_EXPSR.CLM_EXPSR_ID as CLM_EXPSR_ID, CLM_EXPSR.CLMNT_PRTY_ID as CLMNT_PRTY_ID, CLM_EXPSR.CLM_EXPSR_NAME as CLM_EXPSR_NAME, CLM_EXPSR.CLM_EXPSR_RPTD_DTTM as CLM_EXPSR_RPTD_DTTM, CLM_EXPSR.CLM_EXPSR_OTH_CARIER_CVGE_IND as CLM_EXPSR_OTH_CARIER_CVGE_IND, CLM_EXPSR.CLM_ID as CLM_ID, CLM_EXPSR.CVGE_FEAT_ID as CVGE_FEAT_ID, CLM_EXPSR.INSRBL_INT_ID as INSRBL_INT_ID, CLM_EXPSR.PRCS_ID as PRCS_ID, CLM_EXPSR.COTTER_CLM_IND as COTTER_CLM_IND, CLM_EXPSR.LOSS_PRTY_TYPE_CD as LOSS_PRTY_TYPE_CD, CLM_EXPSR.HOLDBACK_IND as HOLDBACK_IND , CLM_EXPSR.HOLDBACK_AMT as HOLDBACK_AMT, CLM_EXPSR.HOLDBACK_REIMBURSED_IND as HOLDBACK_REIMBURSED_IND, CLM_EXPSR.ROOF_RPLACEMT_IND as ROOF_RPLACEMT_IND, CLM_EXPSR.CLM_EXPSR_TYPE_CD AS CLM_EXPSR_TYPE_CD,CLM_EXPSR.CLM_EXPSR_STRT_DTTM as CLM_EXPSR_STRT_DTTM, CLM_EXPSR.CLM_EXPSR_END_DTTM as CLM_EXPSR_END_DTTM, CLM_EXPSR.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM_EXPSR.EDW_END_DTTM as EDW_END_DTTM, CLM_EXPSR.NK_SRC_KEY as NK_SRC_KEY FROM DB_T_PROD_CORE.CLM_EXPSR 
QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM_EXPSR.NK_SRC_KEY  ORDER BY CLM_EXPSR.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_SRC_KEY = exp_all_source_data.in_clm_expsr_nk
QUALIFY RNK = 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
LKP_CLM_EXPSR_ID.CLM_EXPSR_ID as lkp_CLM_EXPSR_ID,
exp_all_source_data.out_clm_id as lkp_CLM_ID,
CASE WHEN exp_all_source_data.claimassignmentdate IS NULL THEN to_date ( ''1900-01-01'' ) ELSE exp_all_source_data.claimassignmentdate END as out_ClaimAssignmentDate,
CASE WHEN exp_all_source_data.contactrole_startdate IS NULL THEN to_date ( ''1900-01-01'') ELSE exp_all_source_data.contactrole_startdate END as out_prty_clm_expsr_strt_dttm,
TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_prty_clm_expsr_end_dttm,
CASE WHEN exp_all_source_data.contactrole_updatedate IS NULL THEN TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ELSE exp_all_source_data.contactrole_updatedate END as out_prty_clm_expsr_trans_strt_dttm,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CONTACTROLE */ as out_prty_clm_expsr_role_cd,
exp_all_source_data.exposurepartytype as exposurepartytype,
CASE
  WHEN exp_all_source_data.exposurepartytype IN (''Person'') THEN LKP_2.INDIV_PRTY_ID
  ELSE LKP_3.BUSN_PRTY_ID
END as out_prty_id,
:PRCS_ID as out_prcs_id,
exp_all_source_data.Retired as Retired,
exp_all_source_data.source_record_id,
row_number() over (partition by exp_all_source_data.source_record_id order by exp_all_source_data.source_record_id) as RNK1
FROM
exp_all_source_data
INNER JOIN LKP_CLM_EXPSR_ID ON exp_all_source_data.source_record_id = LKP_CLM_EXPSR_ID.source_record_id
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CONTACTROLE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = exp_all_source_data.exposurepartyrolecode
LEFT JOIN LKP_INDIV_CLM_CTR LKP_2 ON LKP_2.NK_PUBLC_ID = exp_all_source_data.exposureparty_key
LEFT JOIN LKP_BUSN LKP_3 ON LKP_3.BUSN_CTGY_CD = exp_all_source_data.exposureparty_key  
--AND LKP_3.NK_BUSN_CD = =:LKP.LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY (exp_all_source_data.exposurepartytype)
left join LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY l on LKP_3.NK_BUSN_CD =l.SRC_IDNTFTN_VAL
QUALIFY RNK1 = 1
);


-- Component exp_SrcFields, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_SrcFields AS
(
SELECT
exp_data_transformation.lkp_CLM_EXPSR_ID as in_CLM_EXPSR_ID,
exp_data_transformation.lkp_CLM_ID as in_CLM_ID,
exp_data_transformation.out_prty_clm_expsr_role_cd as in_PRTY_CLM_ROLE_CD,
exp_data_transformation.out_ClaimAssignmentDate as in_PRTY_CLM_STRT_DTTM,
exp_data_transformation.out_prty_id as in_PRTY_ID,
exp_data_transformation.out_prty_clm_expsr_strt_dttm as in_PRTY_CLM_EXPSR_STRT_DTTM,
exp_data_transformation.out_prcs_id as in_PRCS_ID,
exp_data_transformation.out_prty_clm_expsr_end_dttm as in_PRTY_CLM_EXPSR_END_DTTM,
exp_data_transformation.Retired as Retired,
exp_data_transformation.out_prty_clm_expsr_trans_strt_dttm as in_prty_clm_expsr_trans_strt_dttm,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
);


-- Component LKP_PRTY_CLM_EXPR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_CLM_EXPR AS
(
SELECT
LKP.CLM_EXPSR_ID,
LKP.CLM_ID,
LKP.PRTY_CLM_ROLE_CD,
LKP.PRTY_CLM_STRT_DTTM,
LKP.PRTY_ID,
LKP.PRTY_CLM_EXPSR_STRT_DTTM,
LKP.PRTY_CLM_EXPSR_END_DTTM,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP.TRANS_STRT_DTTM,
exp_SrcFields.in_CLM_EXPSR_ID as in_CLM_EXPSR_ID,
exp_SrcFields.in_CLM_ID as in_CLM_ID,
exp_SrcFields.in_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD,
exp_SrcFields.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_SrcFields.source_record_id ORDER BY LKP.CLM_EXPSR_ID asc,LKP.CLM_ID asc,LKP.PRTY_CLM_ROLE_CD asc,LKP.PRTY_CLM_STRT_DTTM asc,LKP.PRTY_ID asc,LKP.PRTY_CLM_EXPSR_STRT_DTTM asc,LKP.PRTY_CLM_EXPSR_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.TRANS_STRT_DTTM asc) RNK
FROM
exp_SrcFields
LEFT JOIN (
SELECT 
PRTY_CLM_EXPSR.CLM_EXPSR_ID AS CLM_EXPSR_ID, 
PRTY_CLM_EXPSR.CLM_ID AS CLM_ID, 
PRTY_CLM_EXPSR.PRTY_CLM_ROLE_CD AS PRTY_CLM_ROLE_CD, 
PRTY_CLM_EXPSR.PRTY_CLM_STRT_DTTM AS PRTY_CLM_STRT_DTTM, 
PRTY_CLM_EXPSR.PRTY_ID AS PRTY_ID,
PRTY_CLM_EXPSR.PRTY_CLM_EXPSR_STRT_DTTM AS PRTY_CLM_EXPSR_STRT_DTTM, 
PRTY_CLM_EXPSR.PRTY_CLM_EXPSR_END_DTTM AS PRTY_CLM_EXPSR_END_DTTM, 
PRTY_CLM_EXPSR.EDW_STRT_DTTM AS EDW_STRT_DTTM,
PRTY_CLM_EXPSR.EDW_END_DTTM AS EDW_END_DTTM, 
PRTY_CLM_EXPSR.TRANS_STRT_DTTM AS TRANS_STRT_DTTM
FROM DB_T_PROD_CORE.PRTY_CLM_EXPSR 
QUALIFY ROW_NUMBER() OVER(PARTITION BY   PRTY_CLM_EXPSR.CLM_EXPSR_ID, PRTY_CLM_EXPSR.CLM_ID, PRTY_CLM_EXPSR.PRTY_CLM_ROLE_CD  ORDER BY PRTY_CLM_EXPSR.EDW_END_DTTM DESC) = 1
) LKP ON LKP.CLM_EXPSR_ID = exp_SrcFields.in_CLM_EXPSR_ID AND LKP.CLM_ID = exp_SrcFields.in_CLM_ID AND LKP.PRTY_CLM_ROLE_CD = exp_SrcFields.in_PRTY_CLM_ROLE_CD
QUALIFY RNK = 1
);


-- Component exp_CDC_Check, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CDC_Check AS
(
SELECT
LKP_PRTY_CLM_EXPR.CLM_EXPSR_ID as lkp_CLM_EXPSR_ID,
LKP_PRTY_CLM_EXPR.CLM_ID as lkp_CLM_ID,
LKP_PRTY_CLM_EXPR.PRTY_CLM_ROLE_CD as lkp_PRTY_CLM_ROLE_CD,
LKP_PRTY_CLM_EXPR.TRANS_STRT_DTTM as lkp_PRTY_CLM_EXPSR_TRANS_STRT_DTTM,
LKP_PRTY_CLM_EXPR.EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
LKP_PRTY_CLM_EXPR.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_SrcFields.in_CLM_EXPSR_ID as in_CLM_EXPSR_ID,
exp_SrcFields.in_CLM_ID as in_CLM_ID,
exp_SrcFields.in_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD,
exp_SrcFields.in_PRTY_CLM_STRT_DTTM as in_PRTY_CLM_STRT_DTTM,
exp_SrcFields.in_PRTY_ID as in_PRTY_ID,
exp_SrcFields.in_PRTY_CLM_EXPSR_STRT_DTTM as in_PRTY_CLM_EXPSR_STRT_DTTM,
exp_SrcFields.in_PRTY_CLM_EXPSR_END_DTTM as in_PRTY_CLM_EXPSR_END_DTTM,
exp_SrcFields.in_PRCS_ID as in_PRCS_ID,
MD5 ( ltrim ( rtrim ( exp_SrcFields.in_PRTY_CLM_STRT_DTTM ) ) || ltrim ( rtrim ( exp_SrcFields.in_PRTY_CLM_EXPSR_STRT_DTTM ) ) || ltrim ( rtrim ( exp_SrcFields.in_PRTY_CLM_EXPSR_END_DTTM ) ) || ltrim ( rtrim ( exp_SrcFields.in_PRTY_ID ) ) ) as v_MD5_src,
MD5 ( ltrim ( rtrim ( LKP_PRTY_CLM_EXPR.PRTY_CLM_STRT_DTTM ) ) || ltrim ( rtrim ( LKP_PRTY_CLM_EXPR.PRTY_CLM_EXPSR_STRT_DTTM ) ) || ltrim ( rtrim ( LKP_PRTY_CLM_EXPR.PRTY_CLM_EXPSR_END_DTTM ) ) || ltrim ( rtrim ( LKP_PRTY_CLM_EXPR.PRTY_ID ) ) ) as v_MD5_tgt,
CASE WHEN v_MD5_tgt IS NULL THEN ''I'' ELSE CASE WHEN v_MD5_src = v_MD5_tgt THEN ''R'' ELSE ''U'' END END as o_Ins_Upd,
CURRENT_TIMESTAMP as out_edw_strt_dttm,
TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_edw_end_dttm,
exp_SrcFields.in_prty_clm_expsr_trans_strt_dttm as in_PRTY_CLM_EXPSR_TRANS_STRT_DTTM,
exp_SrcFields.Retired as Retired,
exp_SrcFields.source_record_id
FROM
exp_SrcFields
INNER JOIN LKP_PRTY_CLM_EXPR ON exp_SrcFields.source_record_id = LKP_PRTY_CLM_EXPR.source_record_id
);


-- Component rtr_prty_clm_expsr_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_prty_clm_expsr_INSERT AS
( SELECT
exp_CDC_Check.lkp_CLM_EXPSR_ID as lkp_CLM_EXPSR_ID,
exp_CDC_Check.lkp_CLM_ID as lkp_CLM_ID,
exp_CDC_Check.lkp_PRTY_CLM_ROLE_CD as lkp_PRTY_CLM_ROLE_CD,
exp_CDC_Check.lkp_PRTY_CLM_EXPSR_TRANS_STRT_DTTM as lkp_PRTY_CLM_EXPSR_TRANS_STRT_DTTM,
exp_CDC_Check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_CDC_Check.in_CLM_EXPSR_ID as in_CLM_EXPSR_ID,
exp_CDC_Check.in_CLM_ID as in_CLM_ID,
exp_CDC_Check.in_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD,
exp_CDC_Check.in_PRTY_CLM_STRT_DTTM as in_PRTY_CLM_STRT_DTTM,
exp_CDC_Check.in_PRTY_ID as in_PRTY_ID,
exp_CDC_Check.in_PRTY_CLM_EXPSR_STRT_DTTM as in_PRTY_CLM_EXPSR_STRT_DTTM,
exp_CDC_Check.in_PRTY_CLM_EXPSR_END_DTTM as in_PRTY_CLM_EXPSR_END_DTTM,
exp_CDC_Check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check.o_Ins_Upd as o_Ins_Upd,
exp_CDC_Check.out_edw_strt_dttm as in_EDW_STRT_DTTM,
exp_CDC_Check.out_edw_end_dttm as in_EDW_END_DTTM,
exp_CDC_Check.in_PRTY_CLM_EXPSR_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_CDC_Check.Retired as Retired,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE exp_CDC_Check.in_PRTY_ID IS NOT NULL and exp_CDC_Check.in_CLM_EXPSR_ID IS NOT NULL and exp_CDC_Check.in_CLM_ID IS NOT NULL and ( exp_CDC_Check.o_Ins_Upd = ''I'' ) OR ( exp_CDC_Check.Retired = 0 AND exp_CDC_Check.lkp_EDW_END_DTTM != TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ));


-- Component rtr_prty_clm_expsr_RETIRE, Type ROUTER Output Group RETIRE
CREATE OR REPLACE TEMPORARY TABLE rtr_prty_clm_expsr_RETIRE AS
(SELECT
exp_CDC_Check.lkp_CLM_EXPSR_ID as lkp_CLM_EXPSR_ID,
exp_CDC_Check.lkp_CLM_ID as lkp_CLM_ID,
exp_CDC_Check.lkp_PRTY_CLM_ROLE_CD as lkp_PRTY_CLM_ROLE_CD,
exp_CDC_Check.lkp_PRTY_CLM_EXPSR_TRANS_STRT_DTTM as lkp_PRTY_CLM_EXPSR_TRANS_STRT_DTTM,
exp_CDC_Check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_CDC_Check.in_CLM_EXPSR_ID as in_CLM_EXPSR_ID,
exp_CDC_Check.in_CLM_ID as in_CLM_ID,
exp_CDC_Check.in_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD,
exp_CDC_Check.in_PRTY_CLM_STRT_DTTM as in_PRTY_CLM_STRT_DTTM,
exp_CDC_Check.in_PRTY_ID as in_PRTY_ID,
exp_CDC_Check.in_PRTY_CLM_EXPSR_STRT_DTTM as in_PRTY_CLM_EXPSR_STRT_DTTM,
exp_CDC_Check.in_PRTY_CLM_EXPSR_END_DTTM as in_PRTY_CLM_EXPSR_END_DTTM,
exp_CDC_Check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check.o_Ins_Upd as o_Ins_Upd,
exp_CDC_Check.out_edw_strt_dttm as in_EDW_STRT_DTTM,
exp_CDC_Check.out_edw_end_dttm as in_EDW_END_DTTM,
exp_CDC_Check.in_PRTY_CLM_EXPSR_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_CDC_Check.Retired as Retired,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE exp_CDC_Check.o_Ins_Upd = ''R'' and exp_CDC_Check.Retired != 0 and exp_CDC_Check.lkp_EDW_END_DTTM = TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component rtr_prty_clm_expsr_UPDATE, Type ROUTER Output Group UPDATE
CREATE OR REPLACE TEMPORARY TABLE rtr_prty_clm_expsr_UPDATE AS
( SELECT
exp_CDC_Check.lkp_CLM_EXPSR_ID as lkp_CLM_EXPSR_ID,
exp_CDC_Check.lkp_CLM_ID as lkp_CLM_ID,
exp_CDC_Check.lkp_PRTY_CLM_ROLE_CD as lkp_PRTY_CLM_ROLE_CD,
exp_CDC_Check.lkp_PRTY_CLM_EXPSR_TRANS_STRT_DTTM as lkp_PRTY_CLM_EXPSR_TRANS_STRT_DTTM,
exp_CDC_Check.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_CDC_Check.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_CDC_Check.in_CLM_EXPSR_ID as in_CLM_EXPSR_ID,
exp_CDC_Check.in_CLM_ID as in_CLM_ID,
exp_CDC_Check.in_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD,
exp_CDC_Check.in_PRTY_CLM_STRT_DTTM as in_PRTY_CLM_STRT_DTTM,
exp_CDC_Check.in_PRTY_ID as in_PRTY_ID,
exp_CDC_Check.in_PRTY_CLM_EXPSR_STRT_DTTM as in_PRTY_CLM_EXPSR_STRT_DTTM,
exp_CDC_Check.in_PRTY_CLM_EXPSR_END_DTTM as in_PRTY_CLM_EXPSR_END_DTTM,
exp_CDC_Check.in_PRCS_ID as in_PRCS_ID,
exp_CDC_Check.o_Ins_Upd as o_Ins_Upd,
exp_CDC_Check.out_edw_strt_dttm as in_EDW_STRT_DTTM,
exp_CDC_Check.out_edw_end_dttm as in_EDW_END_DTTM,
exp_CDC_Check.in_PRTY_CLM_EXPSR_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_CDC_Check.Retired as Retired,
exp_CDC_Check.source_record_id
FROM
exp_CDC_Check
WHERE exp_CDC_Check.in_PRTY_ID IS NOT NULL and exp_CDC_Check.in_CLM_EXPSR_ID IS NOT NULL and exp_CDC_Check.in_CLM_ID IS NOT NULL and exp_CDC_Check.o_Ins_Upd = ''U'' AND exp_CDC_Check.lkp_EDW_END_DTTM = TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component upd_prty_clm_expsr_upd_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prty_clm_expsr_upd_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_prty_clm_expsr_UPDATE.lkp_CLM_EXPSR_ID as lkp_CLM_EXPSR_ID3,
rtr_prty_clm_expsr_UPDATE.in_CLM_ID as in_CLM_ID3,
rtr_prty_clm_expsr_UPDATE.in_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD3,
rtr_prty_clm_expsr_UPDATE.in_PRTY_CLM_STRT_DTTM as in_PRTY_CLM_STRT_DTTM3,
rtr_prty_clm_expsr_UPDATE.in_PRTY_ID as in_PRTY_ID3,
rtr_prty_clm_expsr_UPDATE.in_PRTY_CLM_EXPSR_STRT_DTTM as in_PRTY_CLM_EXPSR_STRT_DTTM3,
rtr_prty_clm_expsr_UPDATE.in_PRTY_CLM_EXPSR_END_DTTM as in_PRTY_CLM_EXPSR_END_DTTM3,
rtr_prty_clm_expsr_UPDATE.in_PRCS_ID as in_PRCS_ID3,
rtr_prty_clm_expsr_UPDATE.in_EDW_STRT_DTTM as StartDate3,
rtr_prty_clm_expsr_UPDATE.in_EDW_END_DTTM as EndDate3,
rtr_prty_clm_expsr_UPDATE.in_TRANS_STRT_DTTM as TRANS_STRT_DTTM3,
rtr_prty_clm_expsr_UPDATE.Retired as Retired3,
0 as UPDATE_STRATEGY_ACTION,
rtr_prty_clm_expsr_UPDATE.source_record_id
FROM
rtr_prty_clm_expsr_UPDATE
);


-- Component fil_prty_clm_expsr_upd_ins, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_prty_clm_expsr_upd_ins AS
(
SELECT
upd_prty_clm_expsr_upd_ins.lkp_CLM_EXPSR_ID3 as lkp_CLM_EXPSR_ID3,
upd_prty_clm_expsr_upd_ins.in_CLM_ID3 as in_CLM_ID3,
upd_prty_clm_expsr_upd_ins.in_PRTY_CLM_ROLE_CD3 as in_PRTY_CLM_ROLE_CD3,
upd_prty_clm_expsr_upd_ins.in_PRTY_CLM_STRT_DTTM3 as in_PRTY_CLM_STRT_DTTM3,
upd_prty_clm_expsr_upd_ins.in_PRTY_ID3 as in_PRTY_ID3,
upd_prty_clm_expsr_upd_ins.in_PRTY_CLM_EXPSR_STRT_DTTM3 as in_PRTY_CLM_EXPSR_STRT_DTTM3,
upd_prty_clm_expsr_upd_ins.in_PRTY_CLM_EXPSR_END_DTTM3 as in_PRTY_CLM_EXPSR_END_DTTM3,
upd_prty_clm_expsr_upd_ins.in_PRCS_ID3 as in_PRCS_ID3,
upd_prty_clm_expsr_upd_ins.StartDate3 as StartDate3,
upd_prty_clm_expsr_upd_ins.EndDate3 as EndDate3,
upd_prty_clm_expsr_upd_ins.TRANS_STRT_DTTM3 as TRANS_STRT_DTTM3,
upd_prty_clm_expsr_upd_ins.Retired3 as Retired3,
upd_prty_clm_expsr_upd_ins.source_record_id
FROM
upd_prty_clm_expsr_upd_ins
WHERE upd_prty_clm_expsr_upd_ins.Retired3 = 0
);


-- Component tgt_prty_clm_expsr_UpdInsert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_CLM_EXPSR
(
CLM_EXPSR_ID,
CLM_ID,
PRTY_CLM_ROLE_CD,
PRTY_CLM_STRT_DTTM,
PRTY_ID,
PRTY_CLM_EXPSR_STRT_DTTM,
PRTY_CLM_EXPSR_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
fil_prty_clm_expsr_upd_ins.lkp_CLM_EXPSR_ID3 as CLM_EXPSR_ID,
fil_prty_clm_expsr_upd_ins.in_CLM_ID3 as CLM_ID,
fil_prty_clm_expsr_upd_ins.in_PRTY_CLM_ROLE_CD3 as PRTY_CLM_ROLE_CD,
fil_prty_clm_expsr_upd_ins.in_PRTY_CLM_STRT_DTTM3 as PRTY_CLM_STRT_DTTM,
fil_prty_clm_expsr_upd_ins.in_PRTY_ID3 as PRTY_ID,
fil_prty_clm_expsr_upd_ins.in_PRTY_CLM_EXPSR_STRT_DTTM3 as PRTY_CLM_EXPSR_STRT_DTTM,
fil_prty_clm_expsr_upd_ins.in_PRTY_CLM_EXPSR_END_DTTM3 as PRTY_CLM_EXPSR_END_DTTM,
fil_prty_clm_expsr_upd_ins.in_PRCS_ID3 as PRCS_ID,
fil_prty_clm_expsr_upd_ins.StartDate3 as EDW_STRT_DTTM,
fil_prty_clm_expsr_upd_ins.EndDate3 as EDW_END_DTTM,
fil_prty_clm_expsr_upd_ins.TRANS_STRT_DTTM3 as TRANS_STRT_DTTM
FROM
fil_prty_clm_expsr_upd_ins;


-- Component updstr_Update_prty_clm_expsr, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE updstr_Update_prty_clm_expsr AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_prty_clm_expsr_UPDATE.lkp_CLM_EXPSR_ID as lkp_CLM_EXPSR_ID3,
rtr_prty_clm_expsr_UPDATE.lkp_CLM_ID as lkp_CLM_ID3,
rtr_prty_clm_expsr_UPDATE.lkp_PRTY_CLM_ROLE_CD as lkp_PRTY_CLM_ROLE_CD3,
NULL as lkp_PRTY_CLM_STRT_DTTM3,
NULL as lkp_PRTY_ID3,
NULL as lkp_PRTY_CLM_EXPSR_STRT_DTTM3,
rtr_prty_clm_expsr_UPDATE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_prty_clm_expsr_UPDATE.Retired as Retired3,
rtr_prty_clm_expsr_UPDATE.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM3,
rtr_prty_clm_expsr_UPDATE.in_TRANS_STRT_DTTM as TRANS_STRT_DTTM3,
rtr_prty_clm_expsr_UPDATE.in_EDW_STRT_DTTM as StartDate3,
rtr_prty_clm_expsr_UPDATE.lkp_PRTY_CLM_EXPSR_TRANS_STRT_DTTM as lkp_PRTY_CLM_EXPSR_TRANS_STRT_DTTM3,
1 as UPDATE_STRATEGY_ACTION,
rtr_prty_clm_expsr_UPDATE.source_record_id
FROM
rtr_prty_clm_expsr_UPDATE
);


-- Component upd_prty_clm_expsr_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prty_clm_expsr_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_prty_clm_expsr_INSERT.in_CLM_EXPSR_ID as in_CLM_EXPSR_ID1,
rtr_prty_clm_expsr_INSERT.in_CLM_ID as in_CLM_ID1,
rtr_prty_clm_expsr_INSERT.in_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD1,
rtr_prty_clm_expsr_INSERT.in_PRTY_CLM_STRT_DTTM as in_PRTY_CLM_STRT_DTTM1,
rtr_prty_clm_expsr_INSERT.in_PRTY_ID as in_PRTY_ID1,
rtr_prty_clm_expsr_INSERT.in_PRTY_CLM_EXPSR_STRT_DTTM as in_PRTY_CLM_EXPSR_STRT_DTTM1,
rtr_prty_clm_expsr_INSERT.in_PRTY_CLM_EXPSR_END_DTTM as in_PRTY_CLM_EXPSR_END_DTTM1,
rtr_prty_clm_expsr_INSERT.in_PRCS_ID as in_PRCS_ID1,
rtr_prty_clm_expsr_INSERT.in_EDW_STRT_DTTM as StartDate1,
rtr_prty_clm_expsr_INSERT.in_EDW_END_DTTM as EndDate1,
rtr_prty_clm_expsr_INSERT.in_TRANS_STRT_DTTM as TRANS_STRT_DTTM1,
rtr_prty_clm_expsr_INSERT.Retired as Retired1,
0 as UPDATE_STRATEGY_ACTION,
rtr_prty_clm_expsr_INSERT.source_record_id
FROM
rtr_prty_clm_expsr_INSERT
);


-- Component fil_prty_clm_expsr_upd_update, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_prty_clm_expsr_upd_update AS
(
SELECT
updstr_Update_prty_clm_expsr.lkp_CLM_EXPSR_ID3 as lkp_CLM_EXPSR_ID3,
updstr_Update_prty_clm_expsr.lkp_CLM_ID3 as lkp_CLM_ID3,
updstr_Update_prty_clm_expsr.lkp_PRTY_CLM_ROLE_CD3 as lkp_PRTY_CLM_ROLE_CD3,
updstr_Update_prty_clm_expsr.lkp_PRTY_CLM_STRT_DTTM3 as lkp_PRTY_CLM_STRT_DTTM3,
updstr_Update_prty_clm_expsr.lkp_PRTY_ID3 as lkp_PRTY_ID3,
updstr_Update_prty_clm_expsr.lkp_PRTY_CLM_EXPSR_STRT_DTTM3 as lkp_PRTY_CLM_EXPSR_STRT_DTTM3,
NULL as lkp_PRCS_ID3,
updstr_Update_prty_clm_expsr.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
updstr_Update_prty_clm_expsr.Retired3 as Retired3,
updstr_Update_prty_clm_expsr.lkp_EDW_END_DTTM3 as lkp_EDW_END_DTTM3,
updstr_Update_prty_clm_expsr.TRANS_STRT_DTTM3 as TRANS_STRT_DTTM3,
updstr_Update_prty_clm_expsr.StartDate3 as StartDate3,
updstr_Update_prty_clm_expsr.lkp_PRTY_CLM_EXPSR_TRANS_STRT_DTTM3 as lkp_PRTY_CLM_EXPSR_TRANS_STRT_DTTM3,
updstr_Update_prty_clm_expsr.source_record_id
FROM
updstr_Update_prty_clm_expsr
WHERE updstr_Update_prty_clm_expsr.lkp_EDW_END_DTTM3 = TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
);


-- Component updstr_Update_prty_clm_expsr_Retire_Reject, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE updstr_Update_prty_clm_expsr_Retire_Reject AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_prty_clm_expsr_RETIRE.lkp_CLM_EXPSR_ID as lkp_CLM_EXPSR_ID3,
rtr_prty_clm_expsr_RETIRE.lkp_CLM_ID as lkp_CLM_ID3,
rtr_prty_clm_expsr_RETIRE.lkp_PRTY_CLM_ROLE_CD as lkp_PRTY_CLM_ROLE_CD3,
NULL as lkp_PRTY_CLM_STRT_DTTM3,
NULL as lkp_PRTY_ID3,
NULL as lkp_PRTY_CLM_EXPSR_STRT_DTTM3,
rtr_prty_clm_expsr_RETIRE.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
NULL as out_PRTY_CLM_STRT_DTTM3,
rtr_prty_clm_expsr_RETIRE.in_TRANS_STRT_DTTM as TRANS_STRT_DTTM4,
rtr_prty_clm_expsr_RETIRE.lkp_PRTY_CLM_EXPSR_TRANS_STRT_DTTM as lkp_PRTY_CLM_EXPSR_TRANS_STRT_DTTM3,
1 as UPDATE_STRATEGY_ACTION,
rtr_prty_clm_expsr_RETIRE.source_record_id
FROM
rtr_prty_clm_expsr_RETIRE
);


-- Component exp_UpdFields, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_UpdFields AS
(
SELECT
fil_prty_clm_expsr_upd_update.lkp_CLM_EXPSR_ID3 as lkp_CLM_EXPSR_ID3,
fil_prty_clm_expsr_upd_update.lkp_CLM_ID3 as lkp_CLM_ID3,
fil_prty_clm_expsr_upd_update.lkp_PRTY_CLM_ROLE_CD3 as lkp_PRTY_CLM_ROLE_CD3,
fil_prty_clm_expsr_upd_update.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
CASE WHEN fil_prty_clm_expsr_upd_update.Retired3 != 0 THEN fil_prty_clm_expsr_upd_update.lkp_EDW_STRT_DTTM3 ELSE DATEADD (
  SECOND,
  -1,
  fil_prty_clm_expsr_upd_update.StartDate3
) END as o_EDW_END_DTTM3,
CASE WHEN fil_prty_clm_expsr_upd_update.Retired3 != 0 THEN fil_prty_clm_expsr_upd_update.lkp_PRTY_CLM_EXPSR_TRANS_STRT_DTTM3 ELSE DATEADD (
  SECOND,
  -1,
  fil_prty_clm_expsr_upd_update.TRANS_STRT_DTTM3
) END as o_TRANS_END_DTTM3,
fil_prty_clm_expsr_upd_update.source_record_id
FROM
fil_prty_clm_expsr_upd_update
);


-- Component tgt_prty_clm_expsr_Update, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_CLM_EXPSR
USING exp_UpdFields ON (PRTY_CLM_EXPSR.CLM_EXPSR_ID = exp_UpdFields.lkp_CLM_EXPSR_ID3 AND PRTY_CLM_EXPSR.CLM_ID = exp_UpdFields.lkp_CLM_ID3 AND PRTY_CLM_EXPSR.PRTY_CLM_ROLE_CD = exp_UpdFields.lkp_PRTY_CLM_ROLE_CD3 AND PRTY_CLM_EXPSR.EDW_STRT_DTTM = exp_UpdFields.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
CLM_EXPSR_ID = exp_UpdFields.lkp_CLM_EXPSR_ID3,
CLM_ID = exp_UpdFields.lkp_CLM_ID3,
PRTY_CLM_ROLE_CD = exp_UpdFields.lkp_PRTY_CLM_ROLE_CD3,
EDW_STRT_DTTM = exp_UpdFields.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_UpdFields.o_EDW_END_DTTM3,
TRANS_END_DTTM = exp_UpdFields.o_TRANS_END_DTTM3;


-- Component exp_UpdFields_Retire_Reject, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_UpdFields_Retire_Reject AS
(
SELECT
updstr_Update_prty_clm_expsr_Retire_Reject.lkp_CLM_EXPSR_ID3 as lkp_CLM_EXPSR_ID3,
updstr_Update_prty_clm_expsr_Retire_Reject.lkp_CLM_ID3 as lkp_CLM_ID3,
updstr_Update_prty_clm_expsr_Retire_Reject.lkp_PRTY_CLM_ROLE_CD3 as lkp_PRTY_CLM_ROLE_CD3,
updstr_Update_prty_clm_expsr_Retire_Reject.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
updstr_Update_prty_clm_expsr_Retire_Reject.lkp_EDW_STRT_DTTM3 as o_EDW_END_DTTM,
updstr_Update_prty_clm_expsr_Retire_Reject.lkp_PRTY_CLM_EXPSR_TRANS_STRT_DTTM3 as out_TRANS_END_DTTM,
updstr_Update_prty_clm_expsr_Retire_Reject.source_record_id
FROM
updstr_Update_prty_clm_expsr_Retire_Reject
);


-- Component tgt_prty_clm_expsr_Update_Retire_Reject, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_CLM_EXPSR
USING exp_UpdFields_Retire_Reject ON (PRTY_CLM_EXPSR.CLM_EXPSR_ID = exp_UpdFields_Retire_Reject.lkp_CLM_EXPSR_ID3 AND PRTY_CLM_EXPSR.CLM_ID = exp_UpdFields_Retire_Reject.lkp_CLM_ID3 AND PRTY_CLM_EXPSR.PRTY_CLM_ROLE_CD = exp_UpdFields_Retire_Reject.lkp_PRTY_CLM_ROLE_CD3 AND PRTY_CLM_EXPSR.EDW_STRT_DTTM = exp_UpdFields_Retire_Reject.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
CLM_EXPSR_ID = exp_UpdFields_Retire_Reject.lkp_CLM_EXPSR_ID3,
CLM_ID = exp_UpdFields_Retire_Reject.lkp_CLM_ID3,
PRTY_CLM_ROLE_CD = exp_UpdFields_Retire_Reject.lkp_PRTY_CLM_ROLE_CD3,
EDW_STRT_DTTM = exp_UpdFields_Retire_Reject.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_UpdFields_Retire_Reject.o_EDW_END_DTTM,
TRANS_END_DTTM = exp_UpdFields_Retire_Reject.out_TRANS_END_DTTM;


-- Component exp_prty_clm_expsr_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prty_clm_expsr_insert AS
(
SELECT
upd_prty_clm_expsr_insert.in_CLM_EXPSR_ID1 as in_CLM_EXPSR_ID1,
upd_prty_clm_expsr_insert.in_CLM_ID1 as in_CLM_ID1,
upd_prty_clm_expsr_insert.in_PRTY_CLM_ROLE_CD1 as in_PRTY_CLM_ROLE_CD1,
upd_prty_clm_expsr_insert.in_PRTY_CLM_STRT_DTTM1 as in_PRTY_CLM_STRT_DTTM1,
upd_prty_clm_expsr_insert.in_PRTY_ID1 as in_PRTY_ID1,
upd_prty_clm_expsr_insert.in_PRTY_CLM_EXPSR_STRT_DTTM1 as in_PRTY_CLM_EXPSR_STRT_DTTM1,
upd_prty_clm_expsr_insert.in_PRTY_CLM_EXPSR_END_DTTM1 as in_PRTY_CLM_EXPSR_END_DTTM1,
upd_prty_clm_expsr_insert.in_PRCS_ID1 as in_PRCS_ID1,
upd_prty_clm_expsr_insert.StartDate1 as StartDate1,
CASE WHEN upd_prty_clm_expsr_insert.Retired1 != 0 THEN upd_prty_clm_expsr_insert.StartDate1 ELSE upd_prty_clm_expsr_insert.EndDate1 END as o_EDW_END_DTTM,
CASE WHEN upd_prty_clm_expsr_insert.Retired1 != 0 THEN upd_prty_clm_expsr_insert.TRANS_STRT_DTTM1 ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as o_TRANS_END_DTTM,
upd_prty_clm_expsr_insert.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM1,
upd_prty_clm_expsr_insert.source_record_id
FROM
upd_prty_clm_expsr_insert
);


-- Component tgt_prty_clm_expsr_NewInsert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_CLM_EXPSR
(
CLM_EXPSR_ID,
CLM_ID,
PRTY_CLM_ROLE_CD,
PRTY_CLM_STRT_DTTM,
PRTY_ID,
PRTY_CLM_EXPSR_STRT_DTTM,
PRTY_CLM_EXPSR_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_prty_clm_expsr_insert.in_CLM_EXPSR_ID1 as CLM_EXPSR_ID,
exp_prty_clm_expsr_insert.in_CLM_ID1 as CLM_ID,
exp_prty_clm_expsr_insert.in_PRTY_CLM_ROLE_CD1 as PRTY_CLM_ROLE_CD,
exp_prty_clm_expsr_insert.in_PRTY_CLM_STRT_DTTM1 as PRTY_CLM_STRT_DTTM,
exp_prty_clm_expsr_insert.in_PRTY_ID1 as PRTY_ID,
exp_prty_clm_expsr_insert.in_PRTY_CLM_EXPSR_STRT_DTTM1 as PRTY_CLM_EXPSR_STRT_DTTM,
exp_prty_clm_expsr_insert.in_PRTY_CLM_EXPSR_END_DTTM1 as PRTY_CLM_EXPSR_END_DTTM,
exp_prty_clm_expsr_insert.in_PRCS_ID1 as PRCS_ID,
exp_prty_clm_expsr_insert.StartDate1 as EDW_STRT_DTTM,
exp_prty_clm_expsr_insert.o_EDW_END_DTTM as EDW_END_DTTM,
exp_prty_clm_expsr_insert.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_prty_clm_expsr_insert.o_TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_prty_clm_expsr_insert;


END; 
';