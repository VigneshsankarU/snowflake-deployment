-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_LEGL_ACTN_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
 DECLARE 
--need to create seq_legl_actn_id.NEXTVAL
run_id varchar;
start_dttm timestamp;
end_dttm timestamp;
prcs_id int;


BEGIN 
 run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name)= upper(:worklet_name) order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
 PRCS_ID:=     (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);

-- Component LKP_TERADATA_ETL_REF_XLAT_MATTER, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_MATTER AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''LEGL_ACTN_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''CCTL_MATTERTYPE.TYPECODE'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_SBR, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_SBR AS
(
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
);


-- Component LKP_TERADATA_ETL_REF_XLAT_SUIT_TYPE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_SUIT_TYPE_CD AS
(
SELECT 	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM

DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

where src_idntftn_nm = ''cctl_typeofsuite_alfa.typecode''

and tgt_idntftn_nm = ''LEGL_ACTN_SUIT_TYPE''
);


-- Component sq_cc_matter, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_cc_matter AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Name,
$2 as CloseDate,
$3 as CreateTime,
$4 as PublicID,
$5 as TYPECODE,
$6 as BadFaithInd_alfa,
$7 as LEGL_ACTN_SUIT_TYPE_CD,
$8 as CASE_NUM,
$9 as SRC_IDNTFTN_VAL,
$10 as Retired,
$11 as SubroRelated,
$12 as updatetime,
$13 as SubrogationLoan_alfa,
$14 as WriteOff_alfa,
$15 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT	ccma.cc_matter_name_stg as Name,   

COALESCE(ccma.CloseDate_stg,CAST(''12/31/9999'' AS DATE)) as legl_actn_end_dt,

/*  COALESCE(ccma.FinalSettleDate,CAST(''01/01/1900'' AS DATE)) AS legl_actn_end_dt, */
       CASE tlma.Typecode_stg

           WHEN ''Arbitration'' THEN COALESCE(ccma.ArbitrationDate_stg,

		ccma.createtime_stg)

           WHEN ''Mediation'' THEN COALESCE(ccma.mediationdate_stg,

		ccma.CreateTime_stg)

           WHEN ''Hearing'' THEN COALESCE(ccma.HearingDate_stg,

		ccma.CreateTime_stg)

           WHEN ''Lawsuit'' THEN COALESCE(ccma.filedate_stg,

		ccma.CreateTime_stg)

           ELSE COALESCE(ccma.createtime_stg,

		CAST(''01/01/1900'' AS DATE))

       END AS legl_actn_strt_dt,

       cast(ccma.PublicID_stg as VARCHAR(64)) AS PublicID,

       tlma.Typecode_stg AS Typecode,

       CAST(ccma.BadFaithIND_Alfa_stg AS VARCHAR(3)) AS BadfaithIND_Alfa,

		CAST(ccma.cctl_typeofsuite_name_stg AS VARCHAR(256)) AS LEGL_ACTN_SUIT_TYPE_CD,

		CAST(ccma.CaseNumber_stg AS VARCHAR(128)) AS CASE_NUM,

       ''SRC_SYS6'' AS SRC_IDNTFTN_VAL,

       ccma.retired_stg AS Retired,

       cast(ccma.subrorelated_stg as varchar(25))AS subroindicator,

       ccma.updatetime_stg AS UpdateTime,

     SubrogationLoan_alfa,

     WriteOff_alfa

FROM	(

	SELECT	cc_matter.Name_stg as cc_matter_name_stg, 

	cc_matter.SubroRelated_stg,

	cc_matter.UpdateTime_stg,

			

	cc_matter.CloseDate_stg, 

	cc_matter.MediationDate_stg, 

	cc_matter.Retired_stg,

			

	cc_matter.FileDate_stg,

	cc_matter.PublicID_stg, 

	cc_matter.HearingDate_stg,

	cc_matter.MatterType_stg,

	cc_matter.CreateTime_stg,

	cc_matter.BadFaithIND_Alfa_stg,

	cc_matter.ArbitrationDate_stg,

	cc_matter.TypeofSuite_alfa_stg,

	cctl_typeofsuite_alfa.Typecode_stg as cctl_typeofsuite_name_stg,

	cc_matter.CaseNumber_stg,

	cast(NULL As VARCHAR(25)) as SubrogationLoan_alfa,

	cast( NULL As decimal(18,4)) as  WriteOff_alfa

	FROM

	DB_T_PROD_STAG.cc_matter

	join

	(

		select	DISTINCT cc_claim.ID_stg 

		from	DB_T_PROD_STAG.cc_claim 

		inner join DB_T_PROD_STAG.cctl_claimstate 

			on cc_claim.State_stg= cctl_claimstate.id_stg 

		where	cctl_claimstate.name_stg <> ''Draft'')  cc_claim 

	 on cc_matter.claimid_stg=cc_claim.id_stg

	 left join DB_T_PROD_STAG.cctl_typeofsuite_alfa

	 on cctl_typeofsuite_alfa.ID_stg = cc_matter.TypeofSuite_alfa_stg

	WHERE	cc_matter.UpdateTime_stg > (:start_dttm)

		AND cc_matter.UpdateTime_stg <= (:end_dttm)) ccma

LEFT  OUTER JOIN (

	select	* 

	from	DB_T_PROD_STAG.cctl_mattertype) tlma 

	ON ccma.MatterType_stg=tlma.ID_stg     



UNION ALL 



SELECT	'' '' AS Name,

       COALESCE(cc_subrogationsummary.CloseDate_stg,CAST(''12/31/9999'' AS DATE)) AS legl_actn_end_dt,

       COALESCE(cc_subrogationsummary.CreateTime_stg,CAST(''01/01/1900'' AS DATE)) AS legl_actn_strt_dt,

       cast(cc_subrogationsummary.PublicID  as VARCHAR(64)) AS PublicID,

       ''LEGL_ACTN_TYPE1'' AS Typecode,

       cast('' '' as varchar(3)) AS BadfaithIND_Alfa,

	   cast(NULL as varchar(256)) AS LEGL_ACTN_SUIT_TYPE_CD,

	   cast(NULL as varchar(128)) AS CASE_NUM,

       ''SRC_SYS6'' AS SRC_IDNTFTN_VAL,

       cc_subrogationsummary.retired_stg AS Retired,

       cast (null as VARCHAR(25)) AS subroindicator,

       cast(cc_subrogationsummary.updatetime_stg as timestamp) AS UpdateTime,

        SubrogationLoan_alfa_stg,

        WriteOff_alfa_stg

FROM	(

	SELECT

	cast(cc_subrogationsummary.PublicID_stg as VARCHAR(64)) as PublicID,

			

	cc_subrogationsummary.CreateTime_stg, 

	cc_subrogationsummary.Retired_stg,

			

	cc_subrogation.CloseDate_stg,

case                                                                                                                                                                       /*  EIM-40668 */
when cc_subrogationsummary.UpdateTime_stg > cc_subrogation.UpdateTime_stg         /*  EIM-40668 */
then cc_subrogationsummary.UpdateTime_stg                                                                             /*  EIM-40668 */
else cc_subrogation.UpdateTime_stg                                                                                               /*  EIM-40668 */
	end updatetime_stg,

/*  EIM-40668 */
/* cc_subrogationsummary.UpdateTime_stg as UpdateTime_stg,                                                 -- EIM-40668 */
	cast(SubrogationLoan_alfa_stg as varchar(25)) as SubrogationLoan_alfa_stg,

	cast(WriteOff_alfa_stg as decimal(18,4)) as WriteOff_alfa_stg

	FROM	DB_T_PROD_STAG.cc_subrogationsummary

	join ( 

		select	cc_subrogation.SubrogationSummaryID_stg, cc_subrogation.UpdateTime_stg ,

cc_subrogation.closedate_stg ,cc_subrogation.status_stg   /*  EIM-40668  */
from	DB_T_PROD_STAG.cc_subrogation                                                                                                                                                                                                                                                           /*  EIM-40668  */
		      ) cc_subrogation  

on cc_subrogationsummary.ID_stg = cc_subrogation.SubrogationSummaryID_stg                                                                                                 /*  EIM-40668 */
/* join DB_T_PROD_STAG.cc_subrogation on cc_subrogationsummary.ID_stg=cc_subrogation.SubrogationSummaryID_stg	                                                                                             -- EIM-40668		    */
	join 

	(

		select	DISTINCT cc_claim.ID_stg 

		from	DB_T_PROD_STAG.cc_claim 

		inner join DB_T_PROD_STAG.cctl_claimstate 

			on cc_claim.State_stg= cctl_claimstate.id_stg 

		where	cctl_claimstate.name_stg <> ''Draft'') cc_claim 

		ON cc_claim.ID_stg=cc_subrogationsummary.ClaimID_stg

	

	WHERE

(                                                                                                                                                                              /* EIM-40668 */
	  (cc_subrogationsummary.UpdateTime_stg > (:start_dttm)

	   and cc_subrogationsummary.UpdateTime_stg <= (:end_dttm))

or	                                                                                                                                                            /*  EIM-40668  */
(cc_subrogation.UpdateTime_stg > (:start_dttm)                                                                            /*  EIM-40668 */
and cc_subrogation.UpdateTime_stg <= (:end_dttm))                                                                 /*  EIM-40668 */
)	                                                                                                                                                                         /*  EIM-40668 */
				) cc_subrogationsummary
) SRC
)
);


-- Component exp_pass_from_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source AS
(
SELECT
CASE WHEN sq_cc_matter.Name IS NULL THEN sq_cc_matter.Name ELSE LTRIM ( RTRIM ( UPPER ( sq_cc_matter.Name ) ) ) END as Name_upper,
sq_cc_matter.PublicID as PublicID,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SBR */ as lkp_TYPECODE_sbr,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_MATTER */ as lkp_TYPECODE_matter,
CASE WHEN lkp_TYPECODE_sbr IS NULL THEN lkp_TYPECODE_matter ELSE lkp_TYPECODE_sbr END as v_TYPECODE,
CASE WHEN v_TYPECODE IS NULL THEN ''UNK'' ELSE v_TYPECODE END as out_TYPECODE,
sq_cc_matter.CreateTime as in_Legal_action_start_date,
sq_cc_matter.BadFaithInd_alfa as BadFaithInd_alfa,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SUIT_TYPE_CD */ as out_LEGL_ACTN_SUIT_TYPE_CD,
sq_cc_matter.CASE_NUM as CASE_NUM,
sq_cc_matter.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL,
CASE WHEN sq_cc_matter.CloseDate IS NULL THEN TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ELSE sq_cc_matter.CloseDate END as in_CloseDate,
sq_cc_matter.Retired as Retired,
sq_cc_matter.SubroRelated as SubroRelated,
CASE WHEN sq_cc_matter.SubrogationLoan_alfa = ''1'' THEN ''Y'' ELSE CASE WHEN sq_cc_matter.SubrogationLoan_alfa = ''0'' THEN ''N'' ELSE null END END as o_SubrogationLoan_alfa,
sq_cc_matter.WriteOff_alfa as WriteOff_alfa,
sq_cc_matter.source_record_id,
row_number() over (partition by sq_cc_matter.source_record_id order by sq_cc_matter.source_record_id) as RNK
FROM
sq_cc_matter
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SBR LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_cc_matter.TYPECODE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_MATTER LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = sq_cc_matter.TYPECODE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SUIT_TYPE_CD LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = sq_cc_matter.LEGL_ACTN_SUIT_TYPE_CD
QUALIFY RNK = 1
);


-- Component LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD AS
(
SELECT
LKP.TGT_IDNTFTN_VAL,
exp_pass_from_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source.source_record_id ORDER BY LKP.TGT_IDNTFTN_VAL desc,LKP.SRC_IDNTFTN_VAL desc) RNK
FROM
exp_pass_from_source
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
) LKP ON LKP.SRC_IDNTFTN_VAL = exp_pass_from_source.SRC_IDNTFTN_VAL
QUALIFY RNK = 1
);


-- Component LKP_LEGL_ACTN_TABLE, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_LEGL_ACTN_TABLE AS
(
SELECT
LKP.LEGL_ACTN_ID,
LKP.LEGL_ACTN_DESC,
LKP.LEGL_ACTN_SUIT_NUM,
LKP.LEGL_ACTN_STRT_DTTM,
LKP.LEGL_ACTN_END_DTTM,
LKP.COURT_LOC_LOCTR_ID,
LKP.LEGL_ACTN_TYPE_CD,
LKP.LEGL_ACTN_SUIT_TYPE_CD,
LKP.CASE_NUM,
LKP.BAD_FAITH_IND,
LKP.SUBRGTN_RLTD_IND,
LKP.PRCS_ID,
LKP.SUBRGTN_LOAN_IND,
LKP.WRT_OFF_AMT,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP.SRC_SYS_CD,
LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD.TGT_IDNTFTN_VAL as in_SYS_SRC_CD,
exp_pass_from_source.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_from_source.source_record_id ORDER BY LKP.LEGL_ACTN_ID asc,LKP.LEGL_ACTN_DESC asc,LKP.LEGL_ACTN_SUIT_NUM asc,LKP.LEGL_ACTN_STRT_DTTM asc,LKP.LEGL_ACTN_END_DTTM asc,LKP.COURT_LOC_LOCTR_ID asc,LKP.LEGL_ACTN_TYPE_CD asc,LKP.LEGL_ACTN_SUIT_TYPE_CD asc,LKP.CASE_NUM asc,LKP.BAD_FAITH_IND asc,LKP.SUBRGTN_RLTD_IND asc,LKP.PRCS_ID asc,LKP.SUBRGTN_LOAN_IND asc,LKP.WRT_OFF_AMT asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc) RNK1
FROM
exp_pass_from_source
INNER JOIN LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD ON exp_pass_from_source.source_record_id = LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD.source_record_id
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
) LKP ON LKP.LEGL_ACTN_SUIT_NUM = exp_pass_from_source.PublicID AND LKP.SRC_SYS_CD = LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD.TGT_IDNTFTN_VAL AND LKP.LEGL_ACTN_TYPE_CD = exp_pass_from_source.out_TYPECODE
QUALIFY RNK1 = 1
);


-- Component exp_CDC_Flag, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_CDC_Flag AS
(
SELECT
exp_pass_from_source.Name_upper as IN_LEGL_ACTN_DESC,
exp_pass_from_source.PublicID as IN_LEGL_ACTN_SUIT_NUM,
exp_pass_from_source.in_Legal_action_start_date as IN_LEGL_ACTN_STRT_DT,
exp_pass_from_source.in_CloseDate as IN_LEGL_ACTN_END_DT,
exp_pass_from_source.out_TYPECODE as IN_LEGL_ACTN_TYPE_CD,
exp_pass_from_source.SubroRelated as in_SubroRelated,
:PRCS_ID as IN_PROCESS_ID,
exp_pass_from_source.out_LEGL_ACTN_SUIT_TYPE_CD as IN_LEGL_ACTN_SUIT_TYPE_CD,
exp_pass_from_source.CASE_NUM as IN_CASE_NUM,
exp_pass_from_source.BadFaithInd_alfa as IN_BAD_FAITH_IND,
CURRENT_TIMESTAMP as IN_EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as IN_EDW_END_DTTM,
LKP_LEGL_ACTN_TABLE.in_SYS_SRC_CD as SRC_SYS_CD,
LTRIM ( RTRIM ( exp_pass_from_source.Name_upper ) ) || LTRIM ( RTRIM ( exp_pass_from_source.in_Legal_action_start_date ) ) || LTRIM ( RTRIM ( exp_pass_from_source.in_CloseDate ) ) || LTRIM ( RTRIM ( exp_pass_from_source.BadFaithInd_alfa ) ) || LTRIM ( RTRIM ( exp_pass_from_source.out_LEGL_ACTN_SUIT_TYPE_CD ) ) || LTRIM ( RTRIM ( exp_pass_from_source.CASE_NUM ) ) || LTRIM ( RTRIM ( exp_pass_from_source.SubroRelated ) ) || LTRIM ( RTRIM ( exp_pass_from_source.o_SubrogationLoan_alfa ) ) || LTRIM ( RTRIM ( exp_pass_from_source.WriteOff_alfa ) ) as value1,
MD5 ( LTRIM ( RTRIM ( value1 ) ) ) as v_src_checksum,
LKP_LEGL_ACTN_TABLE.LEGL_ACTN_ID as LKP_LEGL_ACTN_ID,
LKP_LEGL_ACTN_TABLE.EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
LKP_LEGL_ACTN_TABLE.EDW_END_DTTM as LKP_EDW_END_DTTM,
LTRIM ( RTRIM ( LKP_LEGL_ACTN_TABLE.LEGL_ACTN_DESC ) ) || LTRIM ( RTRIM ( LKP_LEGL_ACTN_TABLE.LEGL_ACTN_STRT_DTTM ) ) || LTRIM ( RTRIM ( LKP_LEGL_ACTN_TABLE.LEGL_ACTN_END_DTTM ) ) || LTRIM ( RTRIM ( LKP_LEGL_ACTN_TABLE.BAD_FAITH_IND ) ) || LTRIM ( RTRIM ( LKP_LEGL_ACTN_TABLE.LEGL_ACTN_SUIT_TYPE_CD ) ) || LTRIM ( RTRIM ( LKP_LEGL_ACTN_TABLE.CASE_NUM ) ) || LTRIM ( RTRIM ( LKP_LEGL_ACTN_TABLE.SUBRGTN_RLTD_IND ) ) || LTRIM ( RTRIM ( LKP_LEGL_ACTN_TABLE.SUBRGTN_LOAN_IND ) ) || LTRIM ( RTRIM ( LKP_LEGL_ACTN_TABLE.WRT_OFF_AMT ) ) as value2,
MD5 ( LTRIM ( RTRIM ( value2 ) ) ) as v_lkp_checksum,
CASE WHEN v_lkp_checksum IS NULL THEN ''I'' ELSE ( CASE WHEN v_lkp_checksum != v_src_checksum THEN ''U'' ELSE ''R'' END ) END as CDC_FLAG,
exp_pass_from_source.Retired as Retired,
sq_cc_matter.updatetime as updatetime,
CASE WHEN sq_cc_matter.updatetime IS NULL THEN TO_DATE ( ''01/01/1900'' , ''MM/DD/YYYY'' ) ELSE sq_cc_matter.updatetime END as TRANS_STRT_DTTM,
exp_pass_from_source.o_SubrogationLoan_alfa as IN_SubrogationLoan_alfa,
exp_pass_from_source.WriteOff_alfa as IN_WriteOff_alfa,
sq_cc_matter.source_record_id
FROM
sq_cc_matter
INNER JOIN exp_pass_from_source ON sq_cc_matter.source_record_id = exp_pass_from_source.source_record_id
INNER JOIN LKP_LEGL_ACTN_TABLE ON exp_pass_from_source.source_record_id = LKP_LEGL_ACTN_TABLE.source_record_id
);


-- Component rtr_legl_actn_ins_upd_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_legl_actn_ins_upd_INSERT AS
(SELECT
exp_CDC_Flag.IN_LEGL_ACTN_DESC as IN_LEGL_ACTN_DESC,
exp_CDC_Flag.IN_LEGL_ACTN_SUIT_NUM as IN_LEGL_ACTN_SUIT_NUM,
exp_CDC_Flag.IN_LEGL_ACTN_END_DT as IN_LEGL_ACTN_END_DT,
exp_CDC_Flag.IN_LEGL_ACTN_TYPE_CD as IN_LEGL_ACTN_TYPE_CD,
exp_CDC_Flag.IN_PROCESS_ID as IN_PROCESS_ID,
exp_CDC_Flag.IN_LEGL_ACTN_STRT_DT as IN_LEGL_ACTN_STRT_DT,
exp_CDC_Flag.IN_LEGL_ACTN_SUIT_TYPE_CD as IN_LEGL_ACTN_SUIT_TYPE_CD,
exp_CDC_Flag.IN_CASE_NUM as IN_CASE_NUM,
exp_CDC_Flag.IN_BAD_FAITH_IND as IN_BAD_FAITH_IND,
exp_CDC_Flag.IN_EDW_STRT_DTTM as IN_EDW_STRT_DTTM,
exp_CDC_Flag.IN_EDW_END_DTTM as IN_EDW_END_DTTM,
exp_CDC_Flag.SRC_SYS_CD as SRC_SYS_CD,
exp_CDC_Flag.LKP_LEGL_ACTN_ID as LKP_LEGL_ACTN_ID,
exp_CDC_Flag.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_CDC_Flag.CDC_FLAG as CDC_FLAG,
exp_CDC_Flag.Retired as Retired,
exp_CDC_Flag.LKP_EDW_END_DTTM as LKP_EDW_END_DTTM,
exp_CDC_Flag.in_SubroRelated as SubroRelated,
exp_CDC_Flag.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_CDC_Flag.IN_SubrogationLoan_alfa as IN_SubrogatioLoan_alfa,
exp_CDC_Flag.IN_WriteOff_alfa as IN_WriteOff_alfa,
exp_CDC_Flag.source_record_id
FROM
exp_CDC_Flag
WHERE exp_CDC_Flag.CDC_FLAG = ''I'' OR ( exp_CDC_Flag.Retired = 0 and exp_CDC_Flag.LKP_EDW_END_DTTM != TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ));


-- Component rtr_legl_actn_ins_upd_RETIRE, Type ROUTER Output Group RETIRE
CREATE OR REPLACE TEMPORARY TABLE rtr_legl_actn_ins_upd_RETIRE AS
(SELECT
exp_CDC_Flag.IN_LEGL_ACTN_DESC as IN_LEGL_ACTN_DESC,
exp_CDC_Flag.IN_LEGL_ACTN_SUIT_NUM as IN_LEGL_ACTN_SUIT_NUM,
exp_CDC_Flag.IN_LEGL_ACTN_END_DT as IN_LEGL_ACTN_END_DT,
exp_CDC_Flag.IN_LEGL_ACTN_TYPE_CD as IN_LEGL_ACTN_TYPE_CD,
exp_CDC_Flag.IN_PROCESS_ID as IN_PROCESS_ID,
exp_CDC_Flag.IN_LEGL_ACTN_STRT_DT as IN_LEGL_ACTN_STRT_DT,
exp_CDC_Flag.IN_LEGL_ACTN_SUIT_TYPE_CD as IN_LEGL_ACTN_SUIT_TYPE_CD,
exp_CDC_Flag.IN_CASE_NUM as IN_CASE_NUM,
exp_CDC_Flag.IN_BAD_FAITH_IND as IN_BAD_FAITH_IND,
exp_CDC_Flag.IN_EDW_STRT_DTTM as IN_EDW_STRT_DTTM,
exp_CDC_Flag.IN_EDW_END_DTTM as IN_EDW_END_DTTM,
exp_CDC_Flag.SRC_SYS_CD as SRC_SYS_CD,
exp_CDC_Flag.LKP_LEGL_ACTN_ID as LKP_LEGL_ACTN_ID,
exp_CDC_Flag.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_CDC_Flag.CDC_FLAG as CDC_FLAG,
exp_CDC_Flag.Retired as Retired,
exp_CDC_Flag.LKP_EDW_END_DTTM as LKP_EDW_END_DTTM,
exp_CDC_Flag.in_SubroRelated as SubroRelated,
exp_CDC_Flag.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_CDC_Flag.IN_SubrogationLoan_alfa as IN_SubrogatioLoan_alfa,
exp_CDC_Flag.IN_WriteOff_alfa as IN_WriteOff_alfa,
exp_CDC_Flag.source_record_id
FROM
exp_CDC_Flag
WHERE exp_CDC_Flag.CDC_FLAG = ''R'' and exp_CDC_Flag.Retired != 0 and exp_CDC_Flag.LKP_EDW_END_DTTM = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component rtr_legl_actn_ins_upd_UPDATE, Type ROUTER Output Group UPDATE
CREATE OR REPLACE TEMPORARY TABLE rtr_legl_actn_ins_upd_UPDATE AS
(SELECT
exp_CDC_Flag.IN_LEGL_ACTN_DESC as IN_LEGL_ACTN_DESC,
exp_CDC_Flag.IN_LEGL_ACTN_SUIT_NUM as IN_LEGL_ACTN_SUIT_NUM,
exp_CDC_Flag.IN_LEGL_ACTN_END_DT as IN_LEGL_ACTN_END_DT,
exp_CDC_Flag.IN_LEGL_ACTN_TYPE_CD as IN_LEGL_ACTN_TYPE_CD,
exp_CDC_Flag.IN_PROCESS_ID as IN_PROCESS_ID,
exp_CDC_Flag.IN_LEGL_ACTN_STRT_DT as IN_LEGL_ACTN_STRT_DT,
exp_CDC_Flag.IN_LEGL_ACTN_SUIT_TYPE_CD as IN_LEGL_ACTN_SUIT_TYPE_CD,
exp_CDC_Flag.IN_CASE_NUM as IN_CASE_NUM,
exp_CDC_Flag.IN_BAD_FAITH_IND as IN_BAD_FAITH_IND,
exp_CDC_Flag.IN_EDW_STRT_DTTM as IN_EDW_STRT_DTTM,
exp_CDC_Flag.IN_EDW_END_DTTM as IN_EDW_END_DTTM,
exp_CDC_Flag.SRC_SYS_CD as SRC_SYS_CD,
exp_CDC_Flag.LKP_LEGL_ACTN_ID as LKP_LEGL_ACTN_ID,
exp_CDC_Flag.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_CDC_Flag.CDC_FLAG as CDC_FLAG,
exp_CDC_Flag.Retired as Retired,
exp_CDC_Flag.LKP_EDW_END_DTTM as LKP_EDW_END_DTTM,
exp_CDC_Flag.in_SubroRelated as SubroRelated,
exp_CDC_Flag.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_CDC_Flag.IN_SubrogationLoan_alfa as IN_SubrogatioLoan_alfa,
exp_CDC_Flag.IN_WriteOff_alfa as IN_WriteOff_alfa,
exp_CDC_Flag.source_record_id
FROM
exp_CDC_Flag
WHERE exp_CDC_Flag.CDC_FLAG = ''U'' AND exp_CDC_Flag.LKP_EDW_END_DTTM = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component upd_legl_actn_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_legl_actn_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_legl_actn_ins_upd_UPDATE.LKP_LEGL_ACTN_ID as LKP_LEGL_ACTN_ID3,
rtr_legl_actn_ins_upd_UPDATE.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM3,
rtr_legl_actn_ins_upd_UPDATE.IN_EDW_STRT_DTTM as IN_EDW_STRT_DTTM3,
rtr_legl_actn_ins_upd_UPDATE.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
1 as UPDATE_STRATEGY_ACTION,
rtr_legl_actn_ins_upd_UPDATE.source_record_id
FROM
rtr_legl_actn_ins_upd_UPDATE
);


-- Component upd_legl_actn_upd_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_legl_actn_upd_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_legl_actn_ins_upd_UPDATE.IN_LEGL_ACTN_DESC as IN_LEGL_ACTN_DESC,
rtr_legl_actn_ins_upd_UPDATE.IN_LEGL_ACTN_SUIT_NUM as IN_LEGL_ACTN_SUIT_NUM,
rtr_legl_actn_ins_upd_UPDATE.IN_LEGL_ACTN_END_DT as IN_LEGL_ACTN_END_DT,
rtr_legl_actn_ins_upd_UPDATE.IN_LEGL_ACTN_TYPE_CD as IN_LEGL_ACTN_TYPE_CD,
rtr_legl_actn_ins_upd_UPDATE.IN_PROCESS_ID as IN_PROCESS_ID,
rtr_legl_actn_ins_upd_UPDATE.IN_LEGL_ACTN_STRT_DT as IN_LEGL_ACTN_STRT_DT,
rtr_legl_actn_ins_upd_UPDATE.LKP_LEGL_ACTN_ID as LEGL_ACTN_ID,
rtr_legl_actn_ins_upd_UPDATE.IN_LEGL_ACTN_SUIT_TYPE_CD as IN_LEGL_ACTN_SUIT_TYPE_CD,
rtr_legl_actn_ins_upd_UPDATE.IN_CASE_NUM as IN_CASE_NUM,
rtr_legl_actn_ins_upd_UPDATE.IN_BAD_FAITH_IND as IN_BAD_FAITH_IND,
rtr_legl_actn_ins_upd_UPDATE.IN_EDW_STRT_DTTM as IN_EDW_STRT_DTTM3,
rtr_legl_actn_ins_upd_UPDATE.IN_EDW_END_DTTM as IN_EDW_END_DTTM3,
rtr_legl_actn_ins_upd_UPDATE.SRC_SYS_CD as SRC_SYS_CD,
rtr_legl_actn_ins_upd_UPDATE.Retired as Retired3,
rtr_legl_actn_ins_upd_UPDATE.SubroRelated as SubroRelated3,
rtr_legl_actn_ins_upd_UPDATE.TRANS_STRT_DTTM as updatetime1,
rtr_legl_actn_ins_upd_UPDATE.IN_SubrogatioLoan_alfa as IN_SubrogatioLoan_alfa3,
rtr_legl_actn_ins_upd_UPDATE.IN_WriteOff_alfa as IN_WriteOff_alfa3,
0 as UPDATE_STRATEGY_ACTION,
rtr_legl_actn_ins_upd_UPDATE.source_record_id
FROM
rtr_legl_actn_ins_upd_UPDATE
);


-- Component upd_legl_actnretire, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_legl_actnretire AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_legl_actn_ins_upd_RETIRE.LKP_LEGL_ACTN_ID as LKP_LEGL_ACTN_ID3,
rtr_legl_actn_ins_upd_RETIRE.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM3,
rtr_legl_actn_ins_upd_RETIRE.IN_EDW_STRT_DTTM as IN_EDW_STRT_DTTM3,
rtr_legl_actn_ins_upd_RETIRE.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
1 as UPDATE_STRATEGY_ACTION,
rtr_legl_actn_ins_upd_RETIRE.source_record_id
FROM
rtr_legl_actn_ins_upd_RETIRE
);


-- Component upd_legl_actn_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_legl_actn_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_legl_actn_ins_upd_INSERT.IN_LEGL_ACTN_DESC as IN_LEGL_ACTN_DESC,
rtr_legl_actn_ins_upd_INSERT.IN_LEGL_ACTN_SUIT_NUM as IN_LEGL_ACTN_SUIT_NUM,
rtr_legl_actn_ins_upd_INSERT.IN_LEGL_ACTN_END_DT as IN_LEGL_ACTN_END_DT,
rtr_legl_actn_ins_upd_INSERT.IN_LEGL_ACTN_TYPE_CD as IN_LEGL_ACTN_TYPE_CD,
rtr_legl_actn_ins_upd_INSERT.IN_PROCESS_ID as IN_PROCESS_ID,
rtr_legl_actn_ins_upd_INSERT.IN_LEGL_ACTN_STRT_DT as IN_LEGL_ACTN_STRT_DT,
rtr_legl_actn_ins_upd_INSERT.IN_LEGL_ACTN_SUIT_TYPE_CD as IN_LEGL_ACTN_SUIT_TYPE_CD,
rtr_legl_actn_ins_upd_INSERT.IN_CASE_NUM as IN_CASE_NUM,
rtr_legl_actn_ins_upd_INSERT.IN_BAD_FAITH_IND as IN_BAD_FAITH_IND,
rtr_legl_actn_ins_upd_INSERT.IN_EDW_STRT_DTTM as IN_EDW_STRT_DTTM,
rtr_legl_actn_ins_upd_INSERT.IN_EDW_END_DTTM as IN_EDW_END_DTTM,
rtr_legl_actn_ins_upd_INSERT.SRC_SYS_CD as IN_SYS_SRC_CD1,
rtr_legl_actn_ins_upd_INSERT.Retired as Retired1,
rtr_legl_actn_ins_upd_INSERT.SubroRelated as SubroRelated1,
rtr_legl_actn_ins_upd_INSERT.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
rtr_legl_actn_ins_upd_INSERT.IN_SubrogatioLoan_alfa as IN_SubrogatioLoan_alfa1,
rtr_legl_actn_ins_upd_INSERT.IN_WriteOff_alfa as IN_WriteOff_alfa1,
0 as UPDATE_STRATEGY_ACTION,
rtr_legl_actn_ins_upd_INSERT.source_record_id
FROM
rtr_legl_actn_ins_upd_INSERT
);


-- Component exp_legl_actn_retire, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_legl_actn_retire AS
(
SELECT
upd_legl_actnretire.LKP_LEGL_ACTN_ID3 as LKP_LEGL_ACTN_ID3,
upd_legl_actnretire.LKP_EDW_STRT_DTTM3 as LKP_EDW_STRT_DTTM3,
CURRENT_TIMESTAMP as o_EDW_END_DTTM,
upd_legl_actnretire.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
upd_legl_actnretire.source_record_id
FROM
upd_legl_actnretire
);


-- Component FILTRANS, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE FILTRANS AS
(
SELECT
upd_legl_actn_upd_ins.IN_LEGL_ACTN_DESC as IN_LEGL_ACTN_DESC,
upd_legl_actn_upd_ins.IN_LEGL_ACTN_SUIT_NUM as IN_LEGL_ACTN_SUIT_NUM,
upd_legl_actn_upd_ins.IN_LEGL_ACTN_END_DT as IN_LEGL_ACTN_END_DT,
upd_legl_actn_upd_ins.IN_LEGL_ACTN_TYPE_CD as IN_LEGL_ACTN_TYPE_CD,
upd_legl_actn_upd_ins.IN_LEGL_ACTN_STRT_DT as IN_LEGL_ACTN_STRT_DT,
upd_legl_actn_upd_ins.LEGL_ACTN_ID as LEGL_ACTN_ID,
upd_legl_actn_upd_ins.IN_PROCESS_ID as IN_PROCESS_ID,
upd_legl_actn_upd_ins.IN_LEGL_ACTN_SUIT_TYPE_CD as IN_LEGL_ACTN_SUIT_TYPE_CD,
upd_legl_actn_upd_ins.IN_CASE_NUM as IN_CASE_NUM,
upd_legl_actn_upd_ins.IN_BAD_FAITH_IND as IN_BAD_FAITH_IND,
upd_legl_actn_upd_ins.IN_EDW_STRT_DTTM3 as IN_EDW_STRT_DTTM3,
upd_legl_actn_upd_ins.IN_EDW_END_DTTM3 as IN_EDW_END_DTTM3,
upd_legl_actn_upd_ins.SRC_SYS_CD as SRC_SYS_CD,
upd_legl_actn_upd_ins.Retired3 as Retired3,
upd_legl_actn_upd_ins.SubroRelated3 as SubroRelated3,
upd_legl_actn_upd_ins.updatetime1 as updatetime1,
upd_legl_actn_upd_ins.IN_SubrogatioLoan_alfa3 as IN_SubrogatioLoan_alfa3,
upd_legl_actn_upd_ins.IN_WriteOff_alfa3 as IN_WriteOff_alfa3,
upd_legl_actn_upd_ins.source_record_id
FROM
upd_legl_actn_upd_ins
WHERE upd_legl_actn_upd_ins.Retired3 = 0
);


-- Component exp_legl_actn_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_legl_actn_upd AS
(
SELECT
upd_legl_actn_upd.LKP_LEGL_ACTN_ID3 as LKP_LEGL_ACTN_ID3,
upd_legl_actn_upd.LKP_EDW_STRT_DTTM3 as LKP_EDW_STRT_DTTM3,
DATEADD (SECOND, -1, upd_legl_actn_upd.IN_EDW_STRT_DTTM3) as o_EDW_END_DTTM,
DATEADD (SECOND, -1, upd_legl_actn_upd.TRANS_STRT_DTTM) as TRANS_END_DTTM,
upd_legl_actn_upd.source_record_id
FROM
upd_legl_actn_upd
);


-- Component exp_pass_to_target_upd_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_upd_ins AS
(
SELECT
FILTRANS.IN_LEGL_ACTN_DESC as IN_LEGL_ACTN_DESC,
FILTRANS.IN_LEGL_ACTN_SUIT_NUM as IN_LEGL_ACTN_SUIT_NUM,
FILTRANS.IN_LEGL_ACTN_END_DT as IN_LEGL_ACTN_END_DT,
FILTRANS.IN_LEGL_ACTN_TYPE_CD as IN_LEGL_ACTN_TYPE_CD,
FILTRANS.IN_LEGL_ACTN_STRT_DT as IN_LEGL_ACTN_STRT_DT,
FILTRANS.LEGL_ACTN_ID as LEGL_ACTN_ID,
FILTRANS.IN_PROCESS_ID as IN_PROCESS_ID,
FILTRANS.IN_LEGL_ACTN_SUIT_TYPE_CD as IN_LEGL_ACTN_SUIT_TYPE_CD,
FILTRANS.IN_CASE_NUM as IN_CASE_NUM,
FILTRANS.IN_BAD_FAITH_IND as IN_BAD_FAITH_IND,
FILTRANS.IN_EDW_STRT_DTTM3 as IN_EDW_STRT_DTTM3,
FILTRANS.IN_EDW_END_DTTM3 as IN_EDW_END_DTTM3,
FILTRANS.SRC_SYS_CD as SRC_SYS_CD,
FILTRANS.SubroRelated3 as SubroRelated3,
FILTRANS.updatetime1 as updatetime1,
FILTRANS.IN_SubrogatioLoan_alfa3 as IN_SubrogatioLoan_alfa3,
FILTRANS.IN_WriteOff_alfa3 as IN_WriteOff_alfa3,
FILTRANS.source_record_id
FROM
FILTRANS
);


-- Component tgt_legl_actn_retire, Type TARGET 
MERGE INTO DB_T_PROD_CORE.LEGL_ACTN
USING exp_legl_actn_retire ON (LEGL_ACTN.LEGL_ACTN_ID = exp_legl_actn_retire.LKP_LEGL_ACTN_ID3)
WHEN MATCHED THEN UPDATE
SET
LEGL_ACTN_ID = exp_legl_actn_retire.LKP_LEGL_ACTN_ID3,
EDW_STRT_DTTM = exp_legl_actn_retire.LKP_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_legl_actn_retire.o_EDW_END_DTTM,
TRANS_END_DTTM = exp_legl_actn_retire.TRANS_STRT_DTTM;


-- Component exp_pass_to_target_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_ins AS
(
SELECT
upd_legl_actn_ins.IN_LEGL_ACTN_DESC as IN_LEGL_ACTN_DESC,
upd_legl_actn_ins.IN_LEGL_ACTN_SUIT_NUM as IN_LEGL_ACTN_SUIT_NUM,
upd_legl_actn_ins.IN_LEGL_ACTN_END_DT as IN_LEGL_ACTN_END_DT,
upd_legl_actn_ins.IN_LEGL_ACTN_TYPE_CD as IN_LEGL_ACTN_TYPE_CD,
upd_legl_actn_ins.IN_LEGL_ACTN_STRT_DT as IN_LEGL_ACTN_STRT_DT,
--seq_legl_actn_id.NEXTVAL 
upd_legl_actn_ins.source_record_id as seq_legl_actn_id,
upd_legl_actn_ins.IN_LEGL_ACTN_SUIT_TYPE_CD as IN_LEGL_ACTN_SUIT_TYPE_CD,
upd_legl_actn_ins.IN_CASE_NUM as IN_CASE_NUM,
upd_legl_actn_ins.IN_BAD_FAITH_IND as IN_BAD_FAITH_IND,
upd_legl_actn_ins.IN_PROCESS_ID as IN_PROCESS_ID,
upd_legl_actn_ins.IN_EDW_STRT_DTTM as IN_EDW_STRT_DTTM,
CASE WHEN upd_legl_actn_ins.Retired1 = 0 THEN upd_legl_actn_ins.IN_EDW_END_DTTM ELSE CURRENT_TIMESTAMP END as out_EDW_END_DTTM1,
upd_legl_actn_ins.IN_SYS_SRC_CD1 as IN_SYS_SRC_CD1,
upd_legl_actn_ins.SubroRelated1 as SubroRelated1,
upd_legl_actn_ins.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
CASE WHEN upd_legl_actn_ins.Retired1 != 0 THEN upd_legl_actn_ins.TRANS_STRT_DTTM ELSE to_timestamp ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as TRANS_END_DTTM,
upd_legl_actn_ins.IN_SubrogatioLoan_alfa1 as IN_SubrogatioLoan_alfa1,
upd_legl_actn_ins.IN_WriteOff_alfa1 as IN_WriteOff_alfa1,
upd_legl_actn_ins.source_record_id
FROM
upd_legl_actn_ins
);


-- Component tgt_legl_actn_upd_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.LEGL_ACTN
(
LEGL_ACTN_ID,
LEGL_ACTN_DESC,
LEGL_ACTN_SUIT_NUM,
LEGL_ACTN_STRT_DTTM,
LEGL_ACTN_END_DTTM,
LEGL_ACTN_TYPE_CD,
BAD_FAITH_IND,
SUBRGTN_RLTD_IND,
SRC_SYS_CD,
SUBRGTN_LOAN_IND,
WRT_OFF_AMT,
LEGL_ACTN_SUIT_TYPE_CD,
CASE_NUM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
exp_pass_to_target_upd_ins.LEGL_ACTN_ID as LEGL_ACTN_ID,
exp_pass_to_target_upd_ins.IN_LEGL_ACTN_DESC as LEGL_ACTN_DESC,
exp_pass_to_target_upd_ins.IN_LEGL_ACTN_SUIT_NUM as LEGL_ACTN_SUIT_NUM,
exp_pass_to_target_upd_ins.IN_LEGL_ACTN_STRT_DT as LEGL_ACTN_STRT_DTTM,
exp_pass_to_target_upd_ins.IN_LEGL_ACTN_END_DT as LEGL_ACTN_END_DTTM,
exp_pass_to_target_upd_ins.IN_LEGL_ACTN_TYPE_CD as LEGL_ACTN_TYPE_CD,
exp_pass_to_target_upd_ins.IN_BAD_FAITH_IND as BAD_FAITH_IND,
exp_pass_to_target_upd_ins.SubroRelated3 as SUBRGTN_RLTD_IND,
exp_pass_to_target_upd_ins.SRC_SYS_CD as SRC_SYS_CD,
exp_pass_to_target_upd_ins.IN_SubrogatioLoan_alfa3 as SUBRGTN_LOAN_IND,
exp_pass_to_target_upd_ins.IN_WriteOff_alfa3 as WRT_OFF_AMT,
exp_pass_to_target_upd_ins.IN_LEGL_ACTN_SUIT_TYPE_CD as LEGL_ACTN_SUIT_TYPE_CD,
exp_pass_to_target_upd_ins.IN_CASE_NUM as CASE_NUM,
exp_pass_to_target_upd_ins.IN_PROCESS_ID as PRCS_ID,
exp_pass_to_target_upd_ins.IN_EDW_STRT_DTTM3 as EDW_STRT_DTTM,
exp_pass_to_target_upd_ins.IN_EDW_END_DTTM3 as EDW_END_DTTM,
exp_pass_to_target_upd_ins.updatetime1 as TRANS_STRT_DTTM
FROM
exp_pass_to_target_upd_ins;


-- Component tgt_legl_actn_upd, Type TARGET 
MERGE INTO DB_T_PROD_CORE.LEGL_ACTN
USING exp_legl_actn_upd ON (LEGL_ACTN.LEGL_ACTN_ID = exp_legl_actn_upd.LKP_LEGL_ACTN_ID3)
WHEN MATCHED THEN UPDATE
SET
LEGL_ACTN_ID = exp_legl_actn_upd.LKP_LEGL_ACTN_ID3,
EDW_STRT_DTTM = exp_legl_actn_upd.LKP_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_legl_actn_upd.o_EDW_END_DTTM,
TRANS_END_DTTM = exp_legl_actn_upd.TRANS_END_DTTM;


-- Component tgt_legl_actn_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.LEGL_ACTN
(
LEGL_ACTN_ID,
LEGL_ACTN_DESC,
LEGL_ACTN_SUIT_NUM,
LEGL_ACTN_STRT_DTTM,
LEGL_ACTN_END_DTTM,
LEGL_ACTN_TYPE_CD,
BAD_FAITH_IND,
SUBRGTN_RLTD_IND,
SRC_SYS_CD,
SUBRGTN_LOAN_IND,
WRT_OFF_AMT,
LEGL_ACTN_SUIT_TYPE_CD,
CASE_NUM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_target_ins.seq_legl_actn_id as LEGL_ACTN_ID,
exp_pass_to_target_ins.IN_LEGL_ACTN_DESC as LEGL_ACTN_DESC,
exp_pass_to_target_ins.IN_LEGL_ACTN_SUIT_NUM as LEGL_ACTN_SUIT_NUM,
exp_pass_to_target_ins.IN_LEGL_ACTN_STRT_DT as LEGL_ACTN_STRT_DTTM,
exp_pass_to_target_ins.IN_LEGL_ACTN_END_DT as LEGL_ACTN_END_DTTM,
exp_pass_to_target_ins.IN_LEGL_ACTN_TYPE_CD as LEGL_ACTN_TYPE_CD,
exp_pass_to_target_ins.IN_BAD_FAITH_IND as BAD_FAITH_IND,
exp_pass_to_target_ins.SubroRelated1 as SUBRGTN_RLTD_IND,
exp_pass_to_target_ins.IN_SYS_SRC_CD1 as SRC_SYS_CD,
exp_pass_to_target_ins.IN_SubrogatioLoan_alfa1 as SUBRGTN_LOAN_IND,
exp_pass_to_target_ins.IN_WriteOff_alfa1 as WRT_OFF_AMT,
exp_pass_to_target_ins.IN_LEGL_ACTN_SUIT_TYPE_CD as LEGL_ACTN_SUIT_TYPE_CD,
exp_pass_to_target_ins.IN_CASE_NUM as CASE_NUM,
exp_pass_to_target_ins.IN_PROCESS_ID as PRCS_ID,
exp_pass_to_target_ins.IN_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_to_target_ins.out_EDW_END_DTTM1 as EDW_END_DTTM,
exp_pass_to_target_ins.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_pass_to_target_ins.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_to_target_ins;


END; 
';