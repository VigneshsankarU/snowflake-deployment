-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_LEGL_ACTN_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
FS_DATE date;
BEGIN 
start_dttm := CURRENT_TIMESTAMP();
end_dttm := CURRENT_TIMESTAMP();
PRCS_ID := 1;   


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


-- Component SQ_cc_matter, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_cc_matter AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ClaimNumber,
$2 as PublicID,
$3 as Typecode,
$4 as CLM_SRC_CD,
$5 as Retired,
$6 as updatetime,
$7 as CLM_LEGL_ACTN_STRT_DTTM,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select	ClaimNumber_stg,PublicID_stg,TYPECODE_stg,

		Clm_src_cd_stg,Retired_stg,updatetime_stg, min(strt_dttm_stg) 

from	(

	

Select	 cc_claim.ClaimNumber_STG,

	        cc_subrogationsummary.PublicID_stg,

	        CAST(''LEGL_ACTN_TYPE1'' as varchar(50)) as TYPECODE_stg,

			''SRC_SYS6'' as Clm_src_cd_stg,

	        case 

				when cc_claim.Retired_stg=0 

		        and cc_subrogationsummary.retired_stg=0 then 0 

				else 1 

			end Retired_stg,

/* 	cc_claim.Retired_stg, */
/*    cc_subrogationsummary.UpdateTime_stg, */
		CASE when CAST(cc_subrogationsummary.UpdateTime_stg as DATE) > CAST(cc_subrogation.UpdateTime_stg as DATE) then cc_subrogationsummary.UpdateTime_stg

                when CAST(cc_subrogationsummary.UpdateTime_stg as DATE) < CAST(cc_subrogation.UpdateTime_stg as DATE) then  cc_subrogation.UpdateTime_stg

                else cc_subrogationsummary.UpdateTime_stg

		END as UpdateTime_stg , 



		   CAST(NULL AS TIMESTAMP) as strt_dttm_stg

	FROM	   DB_T_PROD_STAG.cc_subrogationsummary

	join (

			select	 ClaimNumber_STG,

			      cc_claim.Retired_stg ,cc_claim.ID_stg

			from	DB_T_PROD_STAG.cc_claim

			inner join DB_T_PROD_STAG.cctl_claimstate 

				on  State_stg= cctl_claimstate.id_stg 

			where	cctl_claimstate.name_stg <> ''Draft'') cc_claim 

		on

	 cc_claim.ID_stg=cc_subrogationsummary.ClaimID_stg

join DB_T_PROD_STAG.cc_subrogation on cc_subrogationsummary.ID_stg=cc_subrogation.SubrogationSummaryID_stg   /*  ADDED FOR EIM-40666 */
	WHERE

	(

	(cc_subrogationsummary.UpdateTime_stg >  (:START_DTTM)

		and cc_subrogationsummary.UpdateTime_stg <= (:end_dttm) )

		OR

( cc_subrogation.UpdateTime_stg > (:START_DTTM)      /*  ADDED FOR EIM-40666 */
		and cc_subrogation.UpdateTime_stg <= (:end_dttm) )

	)

		

		

	UNION

	

	

	select	ccma.ClaimNumber_stg,

			ccma.PublicID_stg,tlma.Typecode_stg

	, ''SRC_SYS6'' as Clm_src_cd_stg,

			case 

				when ccma.claimretired_x_stg=0 

		and  ccma.retired_stg=0 then 0 

				else 1 

			end Retired_stg,

			updatetime_stg,

			case 

				when ccma.SubroRelated_stg=0 

		and tlma.typecode_stg=''Lawsuit'' then ccma.createtime_stg 

				else NULL 

			end as strt_dttm

				from

				

	( SELECT   ClaimNumber_stg ,cc_matter.PublicID_stg ,cc_claim.Retired_stg as claimretired_x_stg , 

	  cc_matter.retired_stg ,cc_matter.updatetime_stg ,

	  cc_matter.SubroRelated_stg ,

	  cc_matter.createtime_stg , cc_matter.MatterType_stg

	  FROM

 DB_T_PROD_STAG.cc_matter   join

 (select ClaimNumber_STG,

			      cc_claim.Retired_stg ,

				  cc_claim.ID_stg from DB_T_PROD_STAG.cc_claim 

				  inner join 

 DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg 

 where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

 on cc_matter.claimid_stg=cc_claim.id_stg

LEFT OUTER JOIN

DB_T_PROD_STAG.cc_litstatustypeline on

cc_matter.id_stg=cc_litstatustypeline.MatterID_stg

 where     cc_matter.UpdateTime_stg > (:START_DTTM)

and cc_matter.UpdateTime_stg <= (:end_dttm)

) as  ccma

left  outer join  DB_T_PROD_STAG.cctl_mattertype tlma 

		on	 ccma.MatterType_stg=tlma.ID_stg) x

		group by ClaimNumber_stg,PublicID_stg,TYPECODE_stg,Clm_src_cd_stg,Retired_stg,updatetime_stg
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_cc_matter.ClaimNumber as ClaimNumber,
SQ_cc_matter.PublicID as PublicID,
SQ_cc_matter.Typecode as typecode,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLM_SRC_CD */ as out_CLM_SRC_CD,
SQ_cc_matter.Retired as Retired,
SQ_cc_matter.CLM_LEGL_ACTN_STRT_DTTM as CLM_LEGL_ACTN_STRT_DTTM,
SQ_cc_matter.updatetime as updatetime,
SQ_cc_matter.source_record_id,
row_number() over (partition by SQ_cc_matter.source_record_id order by SQ_cc_matter.source_record_id) as RNK
FROM
SQ_cc_matter
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CLM_SRC_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_cc_matter.CLM_SRC_CD
QUALIFY RNK = 1
);


-- Component LKP_TERADATA_ETL_REF_XLAT_SBR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_SBR AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.TGT_IDNTFTN_VAL desc,LKP.SRC_IDNTFTN_VAL desc) RNK
FROM
exp_pass_through
LEFT JOIN (
SELECT 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL
	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 
FROM 
	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
WHERE 
	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''LEGL_ACTN_TYPE'' 
             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 
		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 
		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
) LKP ON LKP.SRC_IDNTFTN_VAL = exp_pass_through.typecode
QUALIFY RNK = 1
);


-- Component LKP_CLM, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM AS
(
SELECT
LKP.CLM_ID,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.CLM_ID desc,LKP.CLM_TYPE_CD desc,LKP.CLM_MDIA_TYPE_CD desc,LKP.CLM_SUBMTL_TYPE_CD desc,LKP.ACDNT_TYPE_CD desc,LKP.CLM_CTGY_TYPE_CD desc,LKP.ADDL_INSRNC_PLN_IND desc,LKP.EMPLMT_RLTD_IND desc,LKP.ATTNY_INVLVMT_IND desc,LKP.CLM_NUM desc,LKP.CLM_PRIR_IND desc,LKP.PMT_MODE_CD desc,LKP.CLM_OBLGTN_TYPE_CD desc,LKP.SUBRGTN_ELGBL_CD desc,LKP.SUBRGTN_ELGBLY_RSN_CD desc,LKP.CURY_CD desc,LKP.INCDT_EV_ID desc,LKP.INSRD_AT_FAULT_IND desc,LKP.CVGE_IN_QUES_IND desc,LKP.EXTNT_OF_FIRE_DMG_TYPE_CD desc,LKP.VFYD_CLM_IND desc,LKP.PRCS_ID desc
--,LKP.CLM_STRT_DTTM desc
--,LKP.CLM_END_DTTM desc
,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.SRC_SYS_CD desc
--,LKP.TRANS_STRT_DTTM desc
--,LKP.LGCY_CLM_NUM desc
) RNK
FROM
exp_pass_through
LEFT JOIN (
SELECT CLM.CLM_ID as CLM_ID, CLM.CLM_TYPE_CD as CLM_TYPE_CD, CLM.CLM_MDIA_TYPE_CD as CLM_MDIA_TYPE_CD, CLM.CLM_SUBMTL_TYPE_CD as CLM_SUBMTL_TYPE_CD, CLM.ACDNT_TYPE_CD as ACDNT_TYPE_CD, CLM.CLM_CTGY_TYPE_CD as CLM_CTGY_TYPE_CD, CLM.ADDL_INSRNC_PLN_IND as ADDL_INSRNC_PLN_IND, CLM.EMPLMT_RLTD_IND as EMPLMT_RLTD_IND, CLM.ATTNY_INVLVMT_IND as ATTNY_INVLVMT_IND, CLM.CLM_PRIR_IND as CLM_PRIR_IND, CLM.PMT_MODE_CD as PMT_MODE_CD, CLM.CLM_OBLGTN_TYPE_CD as CLM_OBLGTN_TYPE_CD, CLM.SUBRGTN_ELGBL_CD as SUBRGTN_ELGBL_CD, CLM.SUBRGTN_ELGBLY_RSN_CD as SUBRGTN_ELGBLY_RSN_CD, CLM.CURY_CD as CURY_CD, CLM.INCDT_EV_ID as INCDT_EV_ID, CLM.INSRD_AT_FAULT_IND as INSRD_AT_FAULT_IND, CLM.CVGE_IN_QUES_IND as CVGE_IN_QUES_IND, CLM.EXTNT_OF_FIRE_DMG_TYPE_CD as EXTNT_OF_FIRE_DMG_TYPE_CD, CLM.VFYD_CLM_IND as VFYD_CLM_IND, CLM.PRCS_ID as PRCS_ID, CLM.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM.EDW_END_DTTM as EDW_END_DTTM, CLM.CLM_NUM as CLM_NUM, CLM.SRC_SYS_CD as SRC_SYS_CD
FROM DB_T_PROD_CORE.CLM  /* WHERE CLM.EDW_END_DTTM=TO_DATE(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'') */
 QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM.CLM_NUM,CLM.SRC_SYS_CD  ORDER BY CLM.EDW_END_DTTM desc) = 1
) LKP ON LKP.CLM_NUM = exp_pass_through.ClaimNumber AND LKP.SRC_SYS_CD = exp_pass_through.out_CLM_SRC_CD
QUALIFY RNK = 1
);


-- Component LKP_TERADATA_ETL_REF_XLAT_CCTL_MATTERSTATUS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_CCTL_MATTERSTATUS AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.TGT_IDNTFTN_VAL desc,LKP.SRC_IDNTFTN_VAL desc) RNK
FROM
exp_pass_through
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
) LKP ON LKP.SRC_IDNTFTN_VAL = exp_pass_through.typecode
QUALIFY RNK = 1
);


-- Component exp_flg_legl_actn_typecode, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_flg_legl_actn_typecode AS
(
SELECT
CASE WHEN LKP_TERADATA_ETL_REF_XLAT_CCTL_MATTERSTATUS.TGT_IDNTFTN_VAL IS NULL THEN LKP_TERADATA_ETL_REF_XLAT_SBR.TGT_IDNTFTN_VAL ELSE LKP_TERADATA_ETL_REF_XLAT_CCTL_MATTERSTATUS.TGT_IDNTFTN_VAL END as v_Typecode,
CASE WHEN v_Typecode IS NULL THEN ''UNK'' ELSE v_Typecode END as out_Typecode,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
exp_pass_through.Retired as Retired,
CASE WHEN exp_pass_through.CLM_LEGL_ACTN_STRT_DTTM IS NULL THEN to_date ( ''01/01/1900'' , ''mm/dd/yyyy'' ) ELSE exp_pass_through.CLM_LEGL_ACTN_STRT_DTTM END as CLM_LEGL_ACTN_STRT_DTTM1,
exp_pass_through.updatetime as updatetime,
exp_pass_through.source_record_id
FROM
exp_pass_through
INNER JOIN LKP_TERADATA_ETL_REF_XLAT_SBR ON exp_pass_through.source_record_id = LKP_TERADATA_ETL_REF_XLAT_SBR.source_record_id
INNER JOIN LKP_TERADATA_ETL_REF_XLAT_CCTL_MATTERSTATUS ON LKP_TERADATA_ETL_REF_XLAT_SBR.source_record_id = LKP_TERADATA_ETL_REF_XLAT_CCTL_MATTERSTATUS.source_record_id
);


-- Component LKP_LEGL_ACTN, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_LEGL_ACTN AS
(
SELECT
LKP.LEGL_ACTN_ID,
exp_pass_through.PublicID as PublicID,
exp_flg_legl_actn_typecode.out_Typecode as typecode,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.LEGL_ACTN_ID asc,LKP.LEGL_ACTN_DESC asc,LKP.LEGL_ACTN_SUIT_NUM asc
--,LKP.LEGL_ACTN_STRT_DTTM asc
,LKP.LEGL_ACTN_END_DTTM asc,LKP.COURT_LOC_LOCTR_ID asc,LKP.LEGL_ACTN_TYPE_CD asc,LKP.PRCS_ID asc) RNK
FROM
exp_pass_through
INNER JOIN exp_flg_legl_actn_typecode ON exp_pass_through.source_record_id = exp_flg_legl_actn_typecode.source_record_id
LEFT JOIN (
SELECT
LEGL_ACTN_ID,
LEGL_ACTN_DESC,
LEGL_ACTN_SUIT_NUM,
LEGL_ACTN_STRT_DTTM,
LEGL_ACTN_END_DTTM,
COURT_LOC_LOCTR_ID,
LEGL_ACTN_TYPE_CD,
PRCS_ID
FROM DB_T_PROD_CORE.LEGL_ACTN
) LKP ON LKP.LEGL_ACTN_SUIT_NUM = exp_pass_through.PublicID AND LKP.LEGL_ACTN_TYPE_CD = exp_flg_legl_actn_typecode.out_Typecode
QUALIFY RNK = 1
);


-- Component LKP_TGT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TGT AS
(
SELECT
LKP.LEGL_ACTN_ID,
LKP.CLM_ID,
LKP.EDW_END_DTTM,
LKP_LEGL_ACTN.LEGL_ACTN_ID as LEGL_ACTN_ID1,
LKP_CLM.CLM_ID as CLM_ID1,
LKP_CLM.source_record_id,
ROW_NUMBER() OVER(PARTITION BY LKP_CLM.source_record_id ORDER BY LKP.LEGL_ACTN_ID asc,LKP.CLM_ID asc,LKP.EDW_END_DTTM asc) RNK1
FROM
LKP_CLM
INNER JOIN LKP_LEGL_ACTN ON LKP_CLM.source_record_id = LKP_LEGL_ACTN.source_record_id
LEFT JOIN (
SELECT CLM_LEGL_ACTN.EDW_END_DTTM as EDW_END_DTTM, CLM_LEGL_ACTN.LEGL_ACTN_ID as LEGL_ACTN_ID, CLM_LEGL_ACTN.CLM_ID as CLM_ID FROM DB_T_PROD_CORE.CLM_LEGL_ACTN
QUALIFY ROW_NUMBER() OVER(PARTITION BY LEGL_ACTN_ID,CLM_ID ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.LEGL_ACTN_ID = LKP_LEGL_ACTN.LEGL_ACTN_ID AND LKP.CLM_ID = LKP_CLM.CLM_ID
QUALIFY RNK1 = 1
);


-- Component exp_check_flag, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_check_flag AS
(
SELECT
LKP_TGT.CLM_ID1 as CLM_ID,
LKP_TGT.LEGL_ACTN_ID1 as LEGL_ACTN_ID,
CASE WHEN LKP_TGT.CLM_ID IS NULL ANd LKP_TGT.LEGL_ACTN_ID IS NULL THEN ''I'' ELSE ''R'' END as o_flag,
:PRCS_ID as Prcs_id,
exp_flg_legl_actn_typecode.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_flg_legl_actn_typecode.EDW_END_DTTM as EDW_END_DTTM,
to_date ( ''01/01/1900'' , ''mm/dd/yyyy'' ) as TRANS_STRT_DTTM,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as TRANS_END_DTTM,
exp_flg_legl_actn_typecode.Retired as Retired,
LKP_TGT.EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_flg_legl_actn_typecode.CLM_LEGL_ACTN_STRT_DTTM1 as CLM_LEGL_ACTN_STRT_DTTM,
exp_flg_legl_actn_typecode.updatetime as updatetime,
exp_flg_legl_actn_typecode.source_record_id
FROM
exp_flg_legl_actn_typecode
INNER JOIN LKP_TGT ON exp_flg_legl_actn_typecode.source_record_id = LKP_TGT.source_record_id
);


-- Component RTRTRANS_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE RTRTRANS_INSERT AS
(SELECT
exp_check_flag.CLM_ID as CLM_ID,
exp_check_flag.LEGL_ACTN_ID as LEGL_ACTN_ID,
exp_check_flag.o_flag as o_flag,
exp_check_flag.Prcs_id as Prcs_id,
exp_check_flag.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_check_flag.EDW_END_DTTM as EDW_END_DTTM,
exp_check_flag.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_check_flag.TRANS_END_DTTM as TRANS_END_DTTM,
exp_check_flag.Retired as Retired,
exp_check_flag.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_check_flag.CLM_LEGL_ACTN_STRT_DTTM as CLM_LEGL_ACTN_STRT_DTTM,
exp_check_flag.updatetime as updatetime,
exp_check_flag.source_record_id
FROM
exp_check_flag
WHERE exp_check_flag.o_flag = ''I'' AND exp_check_flag.CLM_ID IS NOT NULL AND exp_check_flag.LEGL_ACTN_ID IS NOT NULL OR ( exp_check_flag.lkp_EDW_END_DTTM != TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) and exp_check_flag.Retired = 0 ));


-- Component RTRTRANS_RETIRED, Type ROUTER Output Group RETIRED
CREATE OR REPLACE TEMPORARY TABLE RTRTRANS_RETIRED AS
(SELECT
exp_check_flag.CLM_ID as CLM_ID,
exp_check_flag.LEGL_ACTN_ID as LEGL_ACTN_ID,
exp_check_flag.o_flag as o_flag,
exp_check_flag.Prcs_id as Prcs_id,
exp_check_flag.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_check_flag.EDW_END_DTTM as EDW_END_DTTM,
exp_check_flag.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_check_flag.TRANS_END_DTTM as TRANS_END_DTTM,
exp_check_flag.Retired as Retired,
exp_check_flag.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_check_flag.CLM_LEGL_ACTN_STRT_DTTM as CLM_LEGL_ACTN_STRT_DTTM,
exp_check_flag.updatetime as updatetime,
exp_check_flag.source_record_id
FROM
exp_check_flag
WHERE exp_check_flag.o_flag = ''R'' and exp_check_flag.Retired != 0 and exp_check_flag.lkp_EDW_END_DTTM = TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
RTRTRANS_RETIRED.CLM_ID as CLM_ID3,
RTRTRANS_RETIRED.LEGL_ACTN_ID as LEGL_ACTN_ID3,
RTRTRANS_RETIRED.Prcs_id as Prcs_id3,
CURRENT_TIMESTAMP as EDW_END_DTTM,
RTRTRANS_RETIRED.updatetime as updatetime3,
RTRTRANS_RETIRED.source_record_id
FROM
RTRTRANS_RETIRED
);


-- Component EXPTRANS1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS1 AS
(
SELECT
RTRTRANS_INSERT.CLM_ID as CLM_ID1,
RTRTRANS_INSERT.LEGL_ACTN_ID as LEGL_ACTN_ID1,
RTRTRANS_INSERT.Prcs_id as Prcs_id1,
RTRTRANS_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM1,
CASE WHEN RTRTRANS_INSERT.Retired != 0 THEN RTRTRANS_INSERT.TRANS_STRT_DTTM ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as out_TRANS_END_DTTM1,
CASE WHEN RTRTRANS_INSERT.Retired = 0 THEN RTRTRANS_INSERT.EDW_END_DTTM ELSE CURRENT_TIMESTAMP END as EDW_END_DTTM11,
RTRTRANS_INSERT.CLM_LEGL_ACTN_STRT_DTTM as CLM_LEGL_ACTN_STRT_DTTM1,
RTRTRANS_INSERT.updatetime as updatetime1,
RTRTRANS_INSERT.source_record_id
FROM
RTRTRANS_INSERT
);


-- Component tgt_CLM_LEGL_ACTN_insert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_LEGL_ACTN
(
LEGL_ACTN_ID,
CLM_ID,
PRCS_ID,
CLM_LEGL_ACTN_STRT_DTTM,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
EXPTRANS1.LEGL_ACTN_ID1 as LEGL_ACTN_ID,
EXPTRANS1.CLM_ID1 as CLM_ID,
EXPTRANS1.Prcs_id1 as PRCS_ID,
EXPTRANS1.CLM_LEGL_ACTN_STRT_DTTM1 as CLM_LEGL_ACTN_STRT_DTTM,
EXPTRANS1.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
EXPTRANS1.EDW_END_DTTM11 as EDW_END_DTTM,
EXPTRANS1.updatetime1 as TRANS_STRT_DTTM,
EXPTRANS1.out_TRANS_END_DTTM1 as TRANS_END_DTTM
FROM
EXPTRANS1;


-- Component UPDTRANS, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE UPDTRANS AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
EXPTRANS.CLM_ID3 as CLM_ID3,
EXPTRANS.LEGL_ACTN_ID3 as LEGL_ACTN_ID3,
EXPTRANS.Prcs_id3 as Prcs_id3,
EXPTRANS.EDW_END_DTTM as EDW_END_DTTM,
EXPTRANS.updatetime3 as updatetime3,
1 as UPDATE_STRATEGY_ACTION,
EXPTRANS.source_record_id
FROM
EXPTRANS
);


-- Component CLM_LEGL_ACTN_retired, Type TARGET 
/* Perform Updates */
MERGE INTO DB_T_PROD_CORE.CLM_LEGL_ACTN
USING UPDTRANS ON (UPDATE_STRATEGY_ACTION = 1 AND CLM_LEGL_ACTN.LEGL_ACTN_ID = UPDTRANS.CLM_ID3)
WHEN MATCHED THEN UPDATE
SET
CLM_ID = UPDTRANS.LEGL_ACTN_ID3,
EDW_END_DTTM = UPDTRANS.EDW_END_DTTM
;


END; ';