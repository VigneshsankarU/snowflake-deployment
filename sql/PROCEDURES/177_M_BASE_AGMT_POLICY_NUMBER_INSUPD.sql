-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_POLICY_NUMBER_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 

declare
	start_dttm timestamp;
	end_dttm timestamp;
	prcs_id integer;
	p_agmt_type_cd_policy varchar;

BEGIN 
start_dttm :=current_timestamp();
end_dttm := current_timestamp();
prcs_id := 1;
p_agmt_type_cd_policy := ''1'';


-- Component LKP_TERADATA_ETL_REF_XLAT_DATA_SRC, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_DATA_SRC AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''DATA_SRC_TYPE''

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


-- Component LKP_XREF_AGMNT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_XREF_AGMNT AS
(
SELECT dir_agmt.AGMT_ID as AGMT_ID,

 	            ltrim(rtrim(dir_agmt.NK_SRC_KEY)) as NK_SRC_KEY, 

	            dir_agmt.TERM_NUM as TERM_NUM, 

                  ltrim(rtrim(dir_agmt.AGMT_TYPE_CD)) as AGMT_TYPE_CD 

FROM 

db_t_prod_core.DIR_AGMT WHERE AGMT_TYPE_CD=''POL''
);


-- Component SQ_pc_policy, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_policy AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as IssueDate,
$2 as UpdateTime,
$3 as OriginalEffectiveDate,
$4 as Retired,
$5 as PolicyNumber,
$6 as PeriodStart,
$7 as PeriodEnd,
$8 as IsPaperLessOpted_alfa,
$9 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT p.IssueDate_stg IssueDate

      ,max(p.UpdateTime_stg) UpdateTime

      ,p.OriginalEffectiveDate_stg OriginalEffectiveDate

      ,p.Retired_stg Retired

      ,pr.PolicyNumber_stg PolicyNumber

	  ,cast(''1900-01-01'' as date) Business_strt_dt

      ,cast(''1900-01-01'' as date) Business_end_dt

	  ,TRIM(p.IsPaperLessOpted_alfa_stg) IsPaperLessOpted_alfa

FROM	 DB_T_PROD_STAG.pc_policy p

inner join DB_T_PROD_STAG.pc_policyperiod pr on p.id_stg=pr.PolicyId_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus ps on ps.id_stg=pr.Status_stg  

where lower(ps.TYPECODE_stg)=''bound'' 

and pr.PolicyNumber_stg is not null

and p.UpdateTime_stg > (:start_dttm)

and p.UpdateTime_stg <= (:end_dttm)

group by p.IssueDate_stg,  p.OriginalEffectiveDate_stg, p.Retired_stg, pr.PolicyNumber_stg,TRIM(p.IsPaperLessOpted_alfa_stg)

order by OriginalEffectiveDate_stg
) SRC
)
);


-- Component exp_pass_to_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt AS
(
SELECT
CASE WHEN SQ_pc_policy.IssueDate IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) ELSE SQ_pc_policy.IssueDate END as IssueDate1,
CASE WHEN SQ_pc_policy.UpdateTime IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) ELSE SQ_pc_policy.UpdateTime END as UpdateTime1,
SQ_pc_policy.OriginalEffectiveDate as OriginalEffectiveDate,
SQ_pc_policy.Retired as Retired,
SQ_pc_policy.PolicyNumber as PolicyNumber,
to_char ( SQ_pc_policy.PeriodStart , ''YYYY-MM-DD'' ) as v_PeriodStart,
to_date ( v_PeriodStart , ''YYYY-MM-DD'' ) as o_PeriodStart,
to_char ( SQ_pc_policy.PeriodEnd , ''YYYY-MM-DD'' ) as v_periodEnd,
CASE WHEN to_date ( v_periodEnd , ''YYYY-MM-DD'' ) IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' ) ELSE to_date ( v_periodEnd , ''YYYY-MM-DD'' ) END as o_PeriodEnd,
SQ_pc_policy.IsPaperLessOpted_alfa as IsPaperLessOpted_alfa,
SQ_pc_policy.source_record_id
FROM
SQ_pc_policy
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
exp_pass_to_tgt.IssueDate1 as IssueDate,
exp_pass_to_tgt.OriginalEffectiveDate as OriginalEffectiveDate,
exp_pass_to_tgt.Retired as Retired,
exp_pass_to_tgt.PolicyNumber as PolicyNumber,
:PRCS_ID as PROCESS_ID,
:P_AGMT_TYPE_CD_POLICY as out_AGMT_TYPE_CD,
''SRC_SYS4'' as src_cd,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */ as out_AGMT_SRC_CD,
''UNK'' as out_agmt_cur_sts_cd,
''UNK'' as out_agmt_obtnd_cd,
''UNK'' as out_agmt_sbtype_cd,
''UNK'' as out_agmt_objtv_type_cd,
''UNK'' as out_mkt_risk_type_cd,
''UNK'' as out_ntwk_srvr_agmt_type_cd,
''UNK'' as out_frmlty_type_cd,
''UNK'' as out_agmt_idntftn_cd,
''UNK'' as out_trmtn_type_cd,
''UNK'' as out_int_pmt_meth_type_cd,
CURRENT_TIMESTAMP as o_EDW_STRT_DTTM,
exp_pass_to_tgt.o_PeriodStart as in_AGMT_EFF_DTTM,
exp_pass_to_tgt.o_PeriodEnd as in_AGMT_PLND_EXPN_DT,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DT,
exp_pass_to_tgt.UpdateTime1 as UpdateTime,
exp_pass_to_tgt.IsPaperLessOpted_alfa as IsPaperLessOpted_alfa,
exp_pass_to_tgt.source_record_id,
row_number() over (partition by exp_pass_to_tgt.source_record_id order by exp_pass_to_tgt.source_record_id) as RNK
FROM
exp_pass_to_tgt
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SRC_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = src_cd
QUALIFY RNK = 1
);


-- Component LKP_AGMT_POL_NEW, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_POL_NEW AS
(
SELECT
LKP.AGMT_ID,
LKP.HOST_AGMT_NUM,
LKP.AGMT_OPN_DTTM,
LKP.AGMT_PLND_EXPN_DTTM,
LKP.AGMT_SIGND_DTTM,
LKP.AGMT_TYPE_CD,
LKP.AGMT_EFF_DTTM,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP.TRANS_STRT_DTTM,
LKP.EDOCS_IND,
exp_data_transformation.in_AGMT_EFF_DTTM as in_AGMT_EFF_DTTM,
exp_data_transformation.in_AGMT_PLND_EXPN_DT as in_AGMT_PLND_EXPN_DT,
exp_data_transformation.o_EDW_STRT_DTTM as o_EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DT as in_EDW_END_DTTM,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.AGMT_ID desc,LKP.HOST_AGMT_NUM desc,LKP.AGMT_OPN_DTTM desc,LKP.AGMT_PLND_EXPN_DTTM desc,LKP.AGMT_SIGND_DTTM desc,LKP.AGMT_TYPE_CD desc,LKP.AGMT_EFF_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.TRANS_STRT_DTTM desc,LKP.SRC_SYS_CD desc,LKP.EDOCS_IND desc) RNK
FROM
exp_data_transformation
LEFT JOIN (
SELECT AGMT.AGMT_ID as AGMT_ID, AGMT.AGMT_OPN_DTTM as AGMT_OPN_DTTM, AGMT.AGMT_PLND_EXPN_DTTM as AGMT_PLND_EXPN_DTTM, AGMT.AGMT_SIGND_DTTM as AGMT_SIGND_DTTM, AGMT.AGMT_EFF_DTTM as AGMT_EFF_DTTM, AGMT.EDW_STRT_DTTM as EDW_STRT_DTTM, AGMT.EDW_END_DTTM as EDW_END_DTTM, AGMT.TRANS_STRT_DTTM as TRANS_STRT_DTTM, AGMT.SRC_SYS_CD as SRC_SYS_CD, AGMT.HOST_AGMT_NUM as HOST_AGMT_NUM, AGMT.AGMT_TYPE_CD as AGMT_TYPE_CD,TRIM(EDOCS_IND) AS EDOCS_IND FROM db_t_prod_core.AGMT WHERE AGMT_TYPE_CD=''POL'' 
QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT.HOST_AGMT_NUM,AGMT.AGMT_TYPE_CD ORDER BY AGMT.EDW_END_DTTM desc) = 1
) LKP ON LKP.HOST_AGMT_NUM = exp_data_transformation.PolicyNumber AND LKP.AGMT_TYPE_CD = exp_data_transformation.out_AGMT_TYPE_CD
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.AGMT_ID desc,LKP.HOST_AGMT_NUM desc,LKP.AGMT_OPN_DTTM desc,LKP.AGMT_PLND_EXPN_DTTM desc,LKP.AGMT_SIGND_DTTM desc,LKP.AGMT_TYPE_CD desc,LKP.AGMT_EFF_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc,LKP.TRANS_STRT_DTTM desc,LKP.SRC_SYS_CD desc,LKP.EDOCS_IND desc)  
= 1
);


-- Component exp_cdc, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_cdc AS
(
SELECT
LKP_AGMT_POL_NEW.AGMT_ID as lkp_AGMT_ID,
exp_data_transformation.out_AGMT_TYPE_CD as out_AGMT_TYPE_CD,
LKP_AGMT_POL_NEW.EDW_STRT_DTTM as lkp_EDW_START_DT,
exp_data_transformation.PolicyNumber as PolicyNumber,
exp_data_transformation.OriginalEffectiveDate as OriginalEffectiveDate,
exp_data_transformation.IssueDate as IssueDate,
exp_data_transformation.out_agmt_cur_sts_cd as out_agmt_cur_sts_cd,
exp_data_transformation.out_agmt_obtnd_cd as out_agmt_obtnd_cd,
exp_data_transformation.out_agmt_sbtype_cd as out_agmt_sbtype_cd,
exp_data_transformation.out_agmt_objtv_type_cd as out_agmt_objtv_type_cd,
exp_data_transformation.out_mkt_risk_type_cd as out_mkt_risk_type_cd,
exp_data_transformation.out_ntwk_srvr_agmt_type_cd as out_ntwk_srvr_agmt_type_cd,
exp_data_transformation.out_frmlty_type_cd as out_frmlty_type_cd,
exp_data_transformation.out_agmt_idntftn_cd as out_agmt_idntftn_cd,
exp_data_transformation.out_trmtn_type_cd as out_trmtn_type_cd,
exp_data_transformation.out_int_pmt_meth_type_cd as out_int_pmt_meth_type_cd,
exp_data_transformation.PROCESS_ID as PROCESS_ID,
exp_data_transformation.IsPaperLessOpted_alfa as IsPaperLessOpted_alfa,
exp_data_transformation.out_AGMT_SRC_CD as out_AGMT_SRC_CD,
LKP_AGMT_POL_NEW.AGMT_OPN_DTTM as lkp_AGMT_OPN_DTTM,
LKP_AGMT_POL_NEW.AGMT_EFF_DTTM as lkp_AGMT_EFF_DTTM,
LKP_AGMT_POL_NEW.AGMT_PLND_EXPN_DTTM as lkp_AGMT_PLND_EXPN_DT,
exp_data_transformation.in_AGMT_EFF_DTTM as in_AGMT_EFF_DTTM,
exp_data_transformation.in_AGMT_PLND_EXPN_DT as in_AGMT_PLND_EXPN_DT,
MD5 ( TO_CHAR ( exp_data_transformation.OriginalEffectiveDate ) || TO_CHAR ( exp_data_transformation.IssueDate ) || exp_data_transformation.in_AGMT_EFF_DTTM || exp_data_transformation.in_AGMT_PLND_EXPN_DT || exp_data_transformation.IsPaperLessOpted_alfa ) as v_MD5_SRC,
MD5 ( TO_CHAR ( LKP_AGMT_POL_NEW.AGMT_OPN_DTTM ) || TO_CHAR ( LKP_AGMT_POL_NEW.AGMT_SIGND_DTTM ) || LKP_AGMT_POL_NEW.AGMT_EFF_DTTM || LKP_AGMT_POL_NEW.AGMT_PLND_EXPN_DTTM || LKP_AGMT_POL_NEW.EDOCS_IND ) as v_MD5_LKP,
CASE WHEN LKP_AGMT_POL_NEW.AGMT_ID IS NULL THEN ''I'' ELSE CASE WHEN v_MD5_LKP != v_MD5_SRC THEN ''U'' ELSE ''R'' END END as o_ins_upd,
LKP_AGMT_POL_NEW.o_EDW_STRT_DTTM as EDW_START_DT,
LKP_AGMT_POL_NEW.in_EDW_END_DTTM as EDW_END_DT,
dateadd ( second, -1, CURRENT_TIMESTAMP ) as EDW_END_DT_exp,
TO_DATE ( ''1900-01-01'' , ''yyyy-mm-dd'' ) as BusinessDateDefault,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as BusinessEndDateDefault,
NULL as NewLookupRow,
LKP_AGMT_POL_NEW.HOST_AGMT_NUM as LKP_HOST_AGMT_NUM,
LKP_AGMT_POL_NEW.AGMT_TYPE_CD as LKP_AGMT_TYPE_CD,
exp_data_transformation.Retired as Retired,
LKP_AGMT_POL_NEW.EDW_END_DTTM as lkp_EDW_END_DTTM,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_DATA_SRC */ as AGMT_SRC_CD,
exp_data_transformation.UpdateTime as UpdateTime,
LKP_AGMT_POL_NEW.TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM,
exp_data_transformation.source_record_id,
row_number() over (partition by exp_data_transformation.source_record_id order by exp_data_transformation.source_record_id) as RNK
FROM
exp_data_transformation
INNER JOIN LKP_AGMT_POL_NEW ON exp_data_transformation.source_record_id = LKP_AGMT_POL_NEW.source_record_id
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_DATA_SRC LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = ''DATA_SRC_TYPE2''
QUALIFY row_number() over (partition by exp_data_transformation.source_record_id order by exp_data_transformation.source_record_id) 
= 1
);


-- Component RTRTRANS_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE RTRTRANS_INSERT AS
SELECT
exp_cdc.lkp_AGMT_ID as lkp_AGMT_ID,
exp_cdc.lkp_EDW_START_DT as lkp_EDW_START_DT,
exp_cdc.EDW_START_DT as EDW_START_DT,
exp_cdc.EDW_END_DT as EDW_END_DT,
exp_cdc.EDW_END_DT_exp as EDW_END_DT_exp,
exp_cdc.BusinessDateDefault as BusinessDateDefault,
exp_cdc.out_AGMT_TYPE_CD as out_AGMT_TYPE_CD,
exp_cdc.o_ins_upd as o_ins_upd,
exp_cdc.PolicyNumber as PolicyNumber,
exp_cdc.OriginalEffectiveDate as OriginalEffectiveDate,
exp_cdc.IssueDate as IssueDate,
exp_cdc.out_agmt_cur_sts_cd as out_agmt_cur_sts_cd,
exp_cdc.out_agmt_obtnd_cd as out_agmt_obtnd_cd,
exp_cdc.out_agmt_sbtype_cd as out_agmt_sbtype_cd,
exp_cdc.out_agmt_objtv_type_cd as out_agmt_objtv_type_cd,
exp_cdc.out_mkt_risk_type_cd as out_mkt_risk_type_cd,
exp_cdc.out_ntwk_srvr_agmt_type_cd as out_ntwk_srvr_agmt_type_cd,
exp_cdc.out_frmlty_type_cd as out_frmlty_type_cd,
exp_cdc.out_agmt_idntftn_cd as out_agmt_idntftn_cd,
exp_cdc.out_trmtn_type_cd as out_trmtn_type_cd,
exp_cdc.out_int_pmt_meth_type_cd as out_int_pmt_meth_type_cd,
exp_cdc.PROCESS_ID as PROCESS_ID,
exp_cdc.out_AGMT_SRC_CD as out_AGMT_SRC_CD,
exp_cdc.in_AGMT_EFF_DTTM as in_AGMT_EFF_DTTM,
exp_cdc.in_AGMT_PLND_EXPN_DT as in_AGMT_PLND_EXPN_DT,
exp_cdc.NewLookupRow as NewLookupRow,
exp_cdc.LKP_HOST_AGMT_NUM as LKP_HOST_AGMT_NUM,
exp_cdc.LKP_AGMT_TYPE_CD as LKP_AGMT_TYPE_CD,
exp_cdc.lkp_AGMT_OPN_DTTM as lkp_AGMT_OPN_DTTM,
exp_cdc.lkp_AGMT_EFF_DTTM as lkp_AGMT_EFF_DTTM,
exp_cdc.lkp_AGMT_PLND_EXPN_DT as lkp_AGMT_PLND_EXPN_DT,
exp_cdc.Retired as Retired,
exp_cdc.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_cdc.AGMT_SRC_CD as AGMT_SRC_CD,
exp_cdc.UpdateTime as UpdateTime,
exp_cdc.lkp_TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM,
exp_cdc.BusinessEndDateDefault as BusinessEndDateDefault,
exp_cdc.IsPaperLessOpted_alfa as IsPaperLessOpted_alfa,
exp_cdc.source_record_id
FROM
exp_cdc
WHERE exp_cdc.o_ins_upd = ''I'' 
-- OR ( exp_cdc.lkp_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) and exp_cdc.Retired = 0 ) 
-- exp_cdc.NewLookupRow = 1 
-- exp_cdc.o_ins_upd = ''I''
;


-- Component RTRTRANS_RETIRED, Type ROUTER Output Group RETIRED
CREATE OR REPLACE TEMPORARY TABLE RTRTRANS_RETIRED AS
SELECT
exp_cdc.lkp_AGMT_ID as lkp_AGMT_ID,
exp_cdc.lkp_EDW_START_DT as lkp_EDW_START_DT,
exp_cdc.EDW_START_DT as EDW_START_DT,
exp_cdc.EDW_END_DT as EDW_END_DT,
exp_cdc.EDW_END_DT_exp as EDW_END_DT_exp,
exp_cdc.BusinessDateDefault as BusinessDateDefault,
exp_cdc.out_AGMT_TYPE_CD as out_AGMT_TYPE_CD,
exp_cdc.o_ins_upd as o_ins_upd,
exp_cdc.PolicyNumber as PolicyNumber,
exp_cdc.OriginalEffectiveDate as OriginalEffectiveDate,
exp_cdc.IssueDate as IssueDate,
exp_cdc.out_agmt_cur_sts_cd as out_agmt_cur_sts_cd,
exp_cdc.out_agmt_obtnd_cd as out_agmt_obtnd_cd,
exp_cdc.out_agmt_sbtype_cd as out_agmt_sbtype_cd,
exp_cdc.out_agmt_objtv_type_cd as out_agmt_objtv_type_cd,
exp_cdc.out_mkt_risk_type_cd as out_mkt_risk_type_cd,
exp_cdc.out_ntwk_srvr_agmt_type_cd as out_ntwk_srvr_agmt_type_cd,
exp_cdc.out_frmlty_type_cd as out_frmlty_type_cd,
exp_cdc.out_agmt_idntftn_cd as out_agmt_idntftn_cd,
exp_cdc.out_trmtn_type_cd as out_trmtn_type_cd,
exp_cdc.out_int_pmt_meth_type_cd as out_int_pmt_meth_type_cd,
exp_cdc.PROCESS_ID as PROCESS_ID,
exp_cdc.out_AGMT_SRC_CD as out_AGMT_SRC_CD,
exp_cdc.in_AGMT_EFF_DTTM as in_AGMT_EFF_DTTM,
exp_cdc.in_AGMT_PLND_EXPN_DT as in_AGMT_PLND_EXPN_DT,
exp_cdc.NewLookupRow as NewLookupRow,
exp_cdc.LKP_HOST_AGMT_NUM as LKP_HOST_AGMT_NUM,
exp_cdc.LKP_AGMT_TYPE_CD as LKP_AGMT_TYPE_CD,
exp_cdc.lkp_AGMT_OPN_DTTM as lkp_AGMT_OPN_DTTM,
exp_cdc.lkp_AGMT_EFF_DTTM as lkp_AGMT_EFF_DTTM,
exp_cdc.lkp_AGMT_PLND_EXPN_DT as lkp_AGMT_PLND_EXPN_DT,
exp_cdc.Retired as Retired,
exp_cdc.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_cdc.AGMT_SRC_CD as AGMT_SRC_CD,
exp_cdc.UpdateTime as UpdateTime,
exp_cdc.lkp_TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM,
exp_cdc.BusinessEndDateDefault as BusinessEndDateDefault,
exp_cdc.IsPaperLessOpted_alfa as IsPaperLessOpted_alfa,
exp_cdc.source_record_id
FROM
exp_cdc
WHERE exp_cdc.o_ins_upd = ''R'' and exp_cdc.Retired != 0 and exp_cdc.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );


-- Component RTRTRANS_UPDATE, Type ROUTER Output Group UPDATE
CREATE OR REPLACE TEMPORARY TABLE RTRTRANS_UPDATE AS
SELECT
exp_cdc.lkp_AGMT_ID as lkp_AGMT_ID,
exp_cdc.lkp_EDW_START_DT as lkp_EDW_START_DT,
exp_cdc.EDW_START_DT as EDW_START_DT,
exp_cdc.EDW_END_DT as EDW_END_DT,
exp_cdc.EDW_END_DT_exp as EDW_END_DT_exp,
exp_cdc.BusinessDateDefault as BusinessDateDefault,
exp_cdc.out_AGMT_TYPE_CD as out_AGMT_TYPE_CD,
exp_cdc.o_ins_upd as o_ins_upd,
exp_cdc.PolicyNumber as PolicyNumber,
exp_cdc.OriginalEffectiveDate as OriginalEffectiveDate,
exp_cdc.IssueDate as IssueDate,
exp_cdc.out_agmt_cur_sts_cd as out_agmt_cur_sts_cd,
exp_cdc.out_agmt_obtnd_cd as out_agmt_obtnd_cd,
exp_cdc.out_agmt_sbtype_cd as out_agmt_sbtype_cd,
exp_cdc.out_agmt_objtv_type_cd as out_agmt_objtv_type_cd,
exp_cdc.out_mkt_risk_type_cd as out_mkt_risk_type_cd,
exp_cdc.out_ntwk_srvr_agmt_type_cd as out_ntwk_srvr_agmt_type_cd,
exp_cdc.out_frmlty_type_cd as out_frmlty_type_cd,
exp_cdc.out_agmt_idntftn_cd as out_agmt_idntftn_cd,
exp_cdc.out_trmtn_type_cd as out_trmtn_type_cd,
exp_cdc.out_int_pmt_meth_type_cd as out_int_pmt_meth_type_cd,
exp_cdc.PROCESS_ID as PROCESS_ID,
exp_cdc.out_AGMT_SRC_CD as out_AGMT_SRC_CD,
exp_cdc.in_AGMT_EFF_DTTM as in_AGMT_EFF_DTTM,
exp_cdc.in_AGMT_PLND_EXPN_DT as in_AGMT_PLND_EXPN_DT,
exp_cdc.NewLookupRow as NewLookupRow,
exp_cdc.LKP_HOST_AGMT_NUM as LKP_HOST_AGMT_NUM,
exp_cdc.LKP_AGMT_TYPE_CD as LKP_AGMT_TYPE_CD,
exp_cdc.lkp_AGMT_OPN_DTTM as lkp_AGMT_OPN_DTTM,
exp_cdc.lkp_AGMT_EFF_DTTM as lkp_AGMT_EFF_DTTM,
exp_cdc.lkp_AGMT_PLND_EXPN_DT as lkp_AGMT_PLND_EXPN_DT,
exp_cdc.Retired as Retired,
exp_cdc.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_cdc.AGMT_SRC_CD as AGMT_SRC_CD,
exp_cdc.UpdateTime as UpdateTime,
exp_cdc.lkp_TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM,
exp_cdc.BusinessEndDateDefault as BusinessEndDateDefault,
exp_cdc.IsPaperLessOpted_alfa as IsPaperLessOpted_alfa,
exp_cdc.source_record_id
FROM
exp_cdc
WHERE exp_cdc.o_ins_upd = ''U'' AND exp_cdc.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) /*- - exp_cdc.NewLookupRow = 2 - - exp_cdc.o_ins_upd = ''U''*/
;


-- Component upd_update_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_update_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTRTRANS_RETIRED.lkp_AGMT_ID as lkp_AGMT_ID,
RTRTRANS_RETIRED.lkp_EDW_START_DT as lkp_EDW_START_DT4,
RTRTRANS_RETIRED.PROCESS_ID as PROCESS_ID4,
RTRTRANS_RETIRED.LKP_HOST_AGMT_NUM as LKP_HOST_AGMT_NUM4,
RTRTRANS_RETIRED.LKP_AGMT_TYPE_CD as LKP_AGMT_TYPE_CD4,
RTRTRANS_RETIRED.lkp_AGMT_OPN_DTTM as lkp_AGMT_OPN_DTTM3,
RTRTRANS_RETIRED.lkp_AGMT_EFF_DTTM as lkp_AGMT_EFF_DTTM3,
RTRTRANS_RETIRED.lkp_AGMT_PLND_EXPN_DT as lkp_AGMT_PLND_EXPN_DT3,
RTRTRANS_RETIRED.UpdateTime as UpdateTime4,
1 as UPDATE_STRATEGY_ACTION,
RTRTRANS_RETIRED.source_record_id as source_record_id
FROM
RTRTRANS_RETIRED
);


-- Component upd_update, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_update AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTRTRANS_UPDATE.lkp_AGMT_ID as lkp_AGMT_ID,
RTRTRANS_UPDATE.lkp_EDW_START_DT as lkp_EDW_START_DT3,
RTRTRANS_UPDATE.EDW_START_DT as EDW_START_DT3,
RTRTRANS_UPDATE.EDW_END_DT as EDW_END_DT3,
RTRTRANS_UPDATE.EDW_END_DT_exp as EDW_END_DT_exp3,
RTRTRANS_UPDATE.BusinessDateDefault as BusinessDateDefault3,
RTRTRANS_UPDATE.out_AGMT_TYPE_CD as out_AGMT_TYPE_CD3,
RTRTRANS_UPDATE.o_ins_upd as o_ins_upd3,
RTRTRANS_UPDATE.PolicyNumber as PolicyNumber3,
RTRTRANS_UPDATE.OriginalEffectiveDate as OriginalEffectiveDate3,
RTRTRANS_UPDATE.IssueDate as IssueDate3,
RTRTRANS_UPDATE.out_agmt_cur_sts_cd as out_agmt_cur_sts_cd3,
RTRTRANS_UPDATE.out_agmt_obtnd_cd as out_agmt_obtnd_cd3,
RTRTRANS_UPDATE.out_agmt_sbtype_cd as out_agmt_sbtype_cd3,
RTRTRANS_UPDATE.out_agmt_objtv_type_cd as out_agmt_objtv_type_cd3,
RTRTRANS_UPDATE.out_mkt_risk_type_cd as out_mkt_risk_type_cd3,
RTRTRANS_UPDATE.out_ntwk_srvr_agmt_type_cd as out_ntwk_srvr_agmt_type_cd3,
RTRTRANS_UPDATE.out_frmlty_type_cd as out_frmlty_type_cd3,
RTRTRANS_UPDATE.out_agmt_idntftn_cd as out_agmt_idntftn_cd3,
RTRTRANS_UPDATE.out_trmtn_type_cd as out_trmtn_type_cd3,
RTRTRANS_UPDATE.out_int_pmt_meth_type_cd as out_int_pmt_meth_type_cd3,
RTRTRANS_UPDATE.PROCESS_ID as PROCESS_ID3,
RTRTRANS_UPDATE.out_AGMT_SRC_CD as out_AGMT_SRC_CD3,
RTRTRANS_UPDATE.LKP_HOST_AGMT_NUM as LKP_HOST_AGMT_NUM3,
RTRTRANS_UPDATE.LKP_AGMT_TYPE_CD as LKP_AGMT_TYPE_CD3,
RTRTRANS_UPDATE.lkp_AGMT_OPN_DTTM as lkp_AGMT_OPN_DTTM3,
RTRTRANS_UPDATE.lkp_AGMT_EFF_DTTM as lkp_AGMT_EFF_DTTM3,
RTRTRANS_UPDATE.lkp_AGMT_PLND_EXPN_DT as lkp_AGMT_PLND_EXPN_DT3,
NULL as lkp_AGMT_OPN_DTTM4,
NULL as lkp_AGMT_EFF_DTTM4,
NULL as lkp_AGMT_PLND_EXPN_DT4,
RTRTRANS_UPDATE.UpdateTime as UpdateTime3,
RTRTRANS_UPDATE.lkp_TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM3,
1 as UPDATE_STRATEGY_ACTION,
RTRTRANS_UPDATE.source_record_id as source_record_id
FROM
RTRTRANS_UPDATE
);


-- Component FILTRANS, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE FILTRANS AS
(
SELECT
RTRTRANS_UPDATE.lkp_EDW_START_DT as lkp_EDW_START_DT3,
RTRTRANS_UPDATE.EDW_START_DT as EDW_START_DT3,
RTRTRANS_UPDATE.EDW_END_DT as EDW_END_DT3,
RTRTRANS_UPDATE.EDW_END_DT_exp as EDW_END_DT_exp3,
RTRTRANS_UPDATE.BusinessDateDefault as BusinessDateDefault3,
RTRTRANS_UPDATE.out_AGMT_TYPE_CD as out_AGMT_TYPE_CD3,
RTRTRANS_UPDATE.PolicyNumber as PolicyNumber3,
RTRTRANS_UPDATE.OriginalEffectiveDate as OriginalEffectiveDate3,
RTRTRANS_UPDATE.IssueDate as IssueDate3,
RTRTRANS_UPDATE.out_agmt_cur_sts_cd as out_agmt_cur_sts_cd3,
RTRTRANS_UPDATE.out_agmt_obtnd_cd as out_agmt_obtnd_cd3,
RTRTRANS_UPDATE.out_agmt_sbtype_cd as out_agmt_sbtype_cd3,
RTRTRANS_UPDATE.out_agmt_objtv_type_cd as out_agmt_objtv_type_cd3,
RTRTRANS_UPDATE.out_mkt_risk_type_cd as out_mkt_risk_type_cd3,
RTRTRANS_UPDATE.out_ntwk_srvr_agmt_type_cd as out_ntwk_srvr_agmt_type_cd3,
RTRTRANS_UPDATE.out_frmlty_type_cd as out_frmlty_type_cd3,
RTRTRANS_UPDATE.out_agmt_idntftn_cd as out_agmt_idntftn_cd3,
RTRTRANS_UPDATE.out_trmtn_type_cd as out_trmtn_type_cd3,
RTRTRANS_UPDATE.out_int_pmt_meth_type_cd as out_int_pmt_meth_type_cd3,
RTRTRANS_UPDATE.PROCESS_ID as PROCESS_ID3,
RTRTRANS_UPDATE.out_AGMT_SRC_CD as out_AGMT_SRC_CD3,
RTRTRANS_UPDATE.in_AGMT_EFF_DTTM as in_AGMT_EFF_DTTM3,
RTRTRANS_UPDATE.in_AGMT_PLND_EXPN_DT as in_AGMT_PLND_EXPN_DT3,
NULL as NEXTVAL,
RTRTRANS_UPDATE.Retired as Retired3,
RTRTRANS_UPDATE.lkp_AGMT_ID as lkp_AGMT_ID3,
RTRTRANS_UPDATE.AGMT_SRC_CD as AGMT_SRC_CD3,
RTRTRANS_UPDATE.UpdateTime as UpdateTime3,
RTRTRANS_UPDATE.BusinessEndDateDefault as BusinessEndDateDefault3,
RTRTRANS_UPDATE.IsPaperLessOpted_alfa as IsPaperLessOpted_alfa3,
RTRTRANS_UPDATE.source_record_id
FROM
RTRTRANS_UPDATE
WHERE RTRTRANS_UPDATE.Retired = 0
);


-- Component exp_pass_to_tgt_update_retired, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_update_retired AS
(
SELECT
upd_update_retired.lkp_EDW_START_DT4 as lkp_EDW_START_DT4,
CURRENT_TIMESTAMP as EDW_END_DTTM,
upd_update_retired.LKP_HOST_AGMT_NUM4 as LKP_HOST_AGMT_NUM4,
upd_update_retired.LKP_AGMT_TYPE_CD4 as LKP_AGMT_TYPE_CD4,
upd_update_retired.lkp_AGMT_OPN_DTTM3 as lkp_AGMT_OPN_DTTM3,
upd_update_retired.lkp_AGMT_EFF_DTTM3 as lkp_AGMT_EFF_DTTM3,
upd_update_retired.lkp_AGMT_PLND_EXPN_DT3 as lkp_AGMT_PLND_EXPN_DT3,
upd_update_retired.UpdateTime4 as UpdateTime4,
upd_update_retired.source_record_id
FROM
upd_update_retired
);


-- Component upd_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
RTRTRANS_INSERT.lkp_AGMT_ID as lkp_AGMT_ID,
RTRTRANS_INSERT.lkp_EDW_START_DT as lkp_EDW_START_DT1,
RTRTRANS_INSERT.EDW_START_DT as EDW_START_DT1,
RTRTRANS_INSERT.EDW_END_DT as EDW_END_DT1,
RTRTRANS_INSERT.EDW_END_DT_exp as EDW_END_DT_exp1,
RTRTRANS_INSERT.BusinessDateDefault as BusinessDateDefault1,
RTRTRANS_INSERT.out_AGMT_TYPE_CD as out_AGMT_TYPE_CD1,
RTRTRANS_INSERT.PolicyNumber as PolicyNumber1,
RTRTRANS_INSERT.OriginalEffectiveDate as OriginalEffectiveDate1,
RTRTRANS_INSERT.IssueDate as IssueDate1,
RTRTRANS_INSERT.out_agmt_cur_sts_cd as out_agmt_cur_sts_cd1,
RTRTRANS_INSERT.out_agmt_obtnd_cd as out_agmt_obtnd_cd1,
RTRTRANS_INSERT.out_agmt_sbtype_cd as out_agmt_sbtype_cd1,
RTRTRANS_INSERT.out_agmt_objtv_type_cd as out_agmt_objtv_type_cd1,
RTRTRANS_INSERT.out_mkt_risk_type_cd as out_mkt_risk_type_cd1,
RTRTRANS_INSERT.out_ntwk_srvr_agmt_type_cd as out_ntwk_srvr_agmt_type_cd1,
RTRTRANS_INSERT.out_frmlty_type_cd as out_frmlty_type_cd1,
RTRTRANS_INSERT.out_agmt_idntftn_cd as out_agmt_idntftn_cd1,
RTRTRANS_INSERT.out_trmtn_type_cd as out_trmtn_type_cd1,
RTRTRANS_INSERT.out_int_pmt_meth_type_cd as out_int_pmt_meth_type_cd1,
RTRTRANS_INSERT.PROCESS_ID as PROCESS_ID,
RTRTRANS_INSERT.out_AGMT_SRC_CD as out_AGMT_SRC_CD1,
RTRTRANS_INSERT.in_AGMT_EFF_DTTM as in_AGMT_EFF_DTTM1,
RTRTRANS_INSERT.in_AGMT_PLND_EXPN_DT as in_AGMT_PLND_EXPN_DT1,
RTRTRANS_INSERT.Retired as Retired1,
RTRTRANS_INSERT.AGMT_SRC_CD as AGMT_SRC_CD1,
RTRTRANS_INSERT.UpdateTime as UpdateTime1,
RTRTRANS_INSERT.BusinessEndDateDefault as BusinessEndDateDefault1,
RTRTRANS_INSERT.IsPaperLessOpted_alfa as IsPaperLessOpted_alfa1,
0 as UPDATE_STRATEGY_ACTION,
RTRTRANS_INSERT.source_record_id as source_record_id
FROM
RTRTRANS_INSERT
);


-- Component exp_pass_to_tgt_update, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_update AS
(
SELECT
upd_update.lkp_EDW_START_DT3 as lkp_EDW_START_DT3,
upd_update.EDW_END_DT_exp3 as EDW_END_DT_exp3,
upd_update.LKP_HOST_AGMT_NUM3 as LKP_HOST_AGMT_NUM3,
upd_update.LKP_AGMT_TYPE_CD3 as LKP_AGMT_TYPE_CD3,
upd_update.lkp_AGMT_EFF_DTTM3 as lkp_AGMT_EFF_DTTM3,
upd_update.lkp_AGMT_PLND_EXPN_DT3 as lkp_AGMT_PLND_EXPN_DT3,
dateadd ( second, -1, upd_update.UpdateTime3 ) as TRANS_END_DTTM,
upd_update.source_record_id
FROM
upd_update
);


-- Component upd_ins_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_ins_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
FILTRANS.NEXTVAL as AGMT_ID3,
FILTRANS.lkp_EDW_START_DT3 as lkp_EDW_START_DT3,
FILTRANS.EDW_START_DT3 as EDW_START_DT3,
FILTRANS.EDW_END_DT3 as EDW_END_DT3,
FILTRANS.EDW_END_DT_exp3 as EDW_END_DT_exp3,
FILTRANS.BusinessDateDefault3 as BusinessDateDefault3,
FILTRANS.out_AGMT_TYPE_CD3 as out_AGMT_TYPE_CD3,
FILTRANS.PolicyNumber3 as PolicyNumber3,
FILTRANS.OriginalEffectiveDate3 as OriginalEffectiveDate3,
FILTRANS.IssueDate3 as IssueDate3,
FILTRANS.out_agmt_cur_sts_cd3 as out_agmt_cur_sts_cd3,
FILTRANS.out_agmt_obtnd_cd3 as out_agmt_obtnd_cd3,
FILTRANS.out_agmt_sbtype_cd3 as out_agmt_sbtype_cd3,
FILTRANS.out_agmt_objtv_type_cd3 as out_agmt_objtv_type_cd3,
FILTRANS.out_mkt_risk_type_cd3 as out_mkt_risk_type_cd3,
FILTRANS.out_ntwk_srvr_agmt_type_cd3 as out_ntwk_srvr_agmt_type_cd3,
FILTRANS.out_frmlty_type_cd3 as out_frmlty_type_cd3,
FILTRANS.out_agmt_idntftn_cd3 as out_agmt_idntftn_cd3,
FILTRANS.out_trmtn_type_cd3 as out_trmtn_type_cd3,
FILTRANS.out_int_pmt_meth_type_cd3 as out_int_pmt_meth_type_cd3,
FILTRANS.PROCESS_ID3 as PROCESS_ID3,
FILTRANS.out_AGMT_SRC_CD3 as out_AGMT_SRC_CD3,
FILTRANS.in_AGMT_EFF_DTTM3 as in_AGMT_EFF_DTTM3,
FILTRANS.in_AGMT_PLND_EXPN_DT3 as in_AGMT_PLND_EXPN_DT3,
FILTRANS.lkp_AGMT_ID3 as lkp_AGMT_ID3,
FILTRANS.AGMT_SRC_CD3 as AGMT_SRC_CD3,
FILTRANS.UpdateTime3 as UpdateTime3,
FILTRANS.BusinessEndDateDefault3 as BusinessEndDateDefault3,
FILTRANS.IsPaperLessOpted_alfa3 as IsPaperLessOpted_alfa3,
0 as UPDATE_STRATEGY_ACTION,
FILTRANS.source_record_id as source_record_id
FROM
FILTRANS
);


-- Component exp_pass_to_tgt_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_ins AS
(
SELECT
LKP_1.AGMT_ID /* replaced lookup LKP_XREF_AGMNT */ as AGMT_ID,
upd_ins.EDW_START_DT1 as EDW_START_DT1,
upd_ins.BusinessDateDefault1 as BusinessDateDefault1,
upd_ins.out_AGMT_TYPE_CD1 as out_AGMT_TYPE_CD1,
upd_ins.PolicyNumber1 as PolicyNumber1,
upd_ins.OriginalEffectiveDate1 as OriginalEffectiveDate1,
upd_ins.IssueDate1 as IssueDate1,
upd_ins.out_agmt_cur_sts_cd1 as out_agmt_cur_sts_cd1,
upd_ins.out_agmt_obtnd_cd1 as out_agmt_obtnd_cd1,
upd_ins.out_agmt_sbtype_cd1 as out_agmt_sbtype_cd1,
upd_ins.out_agmt_objtv_type_cd1 as out_agmt_objtv_type_cd1,
upd_ins.out_mkt_risk_type_cd1 as out_mkt_risk_type_cd1,
upd_ins.out_ntwk_srvr_agmt_type_cd1 as out_ntwk_srvr_agmt_type_cd1,
upd_ins.out_frmlty_type_cd1 as out_frmlty_type_cd1,
upd_ins.out_agmt_idntftn_cd1 as out_agmt_idntftn_cd1,
upd_ins.out_trmtn_type_cd1 as out_trmtn_type_cd1,
upd_ins.out_int_pmt_meth_type_cd1 as out_int_pmt_meth_type_cd1,
upd_ins.PROCESS_ID as PROCESS_ID,
upd_ins.out_AGMT_SRC_CD1 as out_AGMT_SRC_CD1,
upd_ins.in_AGMT_EFF_DTTM1 as in_AGMT_EFF_DTTM1,
upd_ins.in_AGMT_PLND_EXPN_DT1 as in_AGMT_PLND_EXPN_DT1,
CASE WHEN upd_ins.Retired1 = 0 THEN upd_ins.EDW_END_DT1 ELSE CURRENT_TIMESTAMP END as EDW_END_DT1,
upd_ins.AGMT_SRC_CD1 as AGMT_SRC_CD1,
upd_ins.UpdateTime1 as UpdateTime1,
CASE WHEN upd_ins.Retired1 <> 0 THEN upd_ins.UpdateTime1 ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as TRANS_END_DTTM,
upd_ins.BusinessEndDateDefault1 as BusinessEndDateDefault1,
upd_ins.IsPaperLessOpted_alfa1 as IsPaperLessOpted_alfa1,
upd_ins.source_record_id,
row_number() over (partition by upd_ins.source_record_id order by upd_ins.source_record_id) as RNK
FROM
upd_ins
LEFT JOIN LKP_XREF_AGMNT LKP_1 ON LKP_1.NK_SRC_KEY = ltrim ( rtrim ( upd_ins.PolicyNumber1 ) ) AND LKP_1.TERM_NUM = NULL AND LKP_1.AGMT_TYPE_CD = ltrim ( rtrim ( upd_ins.out_AGMT_TYPE_CD1 ) )
QUALIFY row_number() over (partition by upd_ins.source_record_id order by upd_ins.source_record_id) 
= 1
);


-- Component AGMT_INS, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT
(
AGMT_ID,
HOST_AGMT_NUM,
AGMT_OPN_DTTM,
AGMT_PLND_EXPN_DTTM,
AGMT_SIGND_DTTM,
AGMT_TYPE_CD,
AGMT_SRC_CD,
AGMT_CUR_STS_CD,
AGMT_OBTND_CD,
AGMT_SBTYPE_CD,
AGMT_PRCSG_DTTM,
AGMT_OBJTV_TYPE_CD,
MKT_RISK_TYPE_CD,
NTWK_SRVC_AGMT_TYPE_CD,
FRMLTY_TYPE_CD,
AGMT_IDNTFTN_CD,
TRMTN_TYPE_CD,
INT_PMT_METH_CD,
AGMT_EFF_DTTM,
MODL_EFF_DTTM,
PRCS_ID,
MODL_ACTL_END_DTTM,
CNTNUS_SRVC_DTTM,
NK_SRC_KEY,
SRC_SYS_CD,
EDW_STRT_DTTM,
EDW_END_DTTM,
EDOCS_IND,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_tgt_ins.AGMT_ID as AGMT_ID,
exp_pass_to_tgt_ins.PolicyNumber1 as HOST_AGMT_NUM,
exp_pass_to_tgt_ins.OriginalEffectiveDate1 as AGMT_OPN_DTTM,
exp_pass_to_tgt_ins.in_AGMT_PLND_EXPN_DT1 as AGMT_PLND_EXPN_DTTM,
exp_pass_to_tgt_ins.IssueDate1 as AGMT_SIGND_DTTM,
exp_pass_to_tgt_ins.out_AGMT_TYPE_CD1 as AGMT_TYPE_CD,
exp_pass_to_tgt_ins.AGMT_SRC_CD1 as AGMT_SRC_CD,
exp_pass_to_tgt_ins.out_agmt_cur_sts_cd1 as AGMT_CUR_STS_CD,
exp_pass_to_tgt_ins.out_agmt_obtnd_cd1 as AGMT_OBTND_CD,
exp_pass_to_tgt_ins.out_agmt_sbtype_cd1 as AGMT_SBTYPE_CD,
exp_pass_to_tgt_ins.BusinessDateDefault1 as AGMT_PRCSG_DTTM,
exp_pass_to_tgt_ins.out_agmt_objtv_type_cd1 as AGMT_OBJTV_TYPE_CD,
exp_pass_to_tgt_ins.out_mkt_risk_type_cd1 as MKT_RISK_TYPE_CD,
exp_pass_to_tgt_ins.out_ntwk_srvr_agmt_type_cd1 as NTWK_SRVC_AGMT_TYPE_CD,
exp_pass_to_tgt_ins.out_frmlty_type_cd1 as FRMLTY_TYPE_CD,
exp_pass_to_tgt_ins.out_agmt_idntftn_cd1 as AGMT_IDNTFTN_CD,
exp_pass_to_tgt_ins.out_trmtn_type_cd1 as TRMTN_TYPE_CD,
exp_pass_to_tgt_ins.out_int_pmt_meth_type_cd1 as INT_PMT_METH_CD,
exp_pass_to_tgt_ins.in_AGMT_EFF_DTTM1 as AGMT_EFF_DTTM,
exp_pass_to_tgt_ins.BusinessDateDefault1 as MODL_EFF_DTTM,
exp_pass_to_tgt_ins.PROCESS_ID as PRCS_ID,
exp_pass_to_tgt_ins.BusinessEndDateDefault1 as MODL_ACTL_END_DTTM,
exp_pass_to_tgt_ins.BusinessDateDefault1 as CNTNUS_SRVC_DTTM,
exp_pass_to_tgt_ins.PolicyNumber1 as NK_SRC_KEY,
exp_pass_to_tgt_ins.out_AGMT_SRC_CD1 as SRC_SYS_CD,
exp_pass_to_tgt_ins.EDW_START_DT1 as EDW_STRT_DTTM,
exp_pass_to_tgt_ins.EDW_END_DT1 as EDW_END_DTTM,
exp_pass_to_tgt_ins.IsPaperLessOpted_alfa1 as EDOCS_IND,
exp_pass_to_tgt_ins.UpdateTime1 as TRANS_STRT_DTTM,
exp_pass_to_tgt_ins.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_to_tgt_ins;


-- Component AGMT_UPDATE_retired, Type TARGET 
MERGE INTO DB_T_PROD_CORE.AGMT
USING exp_pass_to_tgt_update_retired ON (AGMT.HOST_AGMT_NUM = exp_pass_to_tgt_update_retired.LKP_HOST_AGMT_NUM4 AND AGMT.AGMT_OPN_DTTM = exp_pass_to_tgt_update_retired.lkp_AGMT_OPN_DTTM3 AND AGMT.AGMT_PLND_EXPN_DTTM = exp_pass_to_tgt_update_retired.lkp_AGMT_PLND_EXPN_DT3 AND AGMT.AGMT_TYPE_CD = exp_pass_to_tgt_update_retired.LKP_AGMT_TYPE_CD4 AND AGMT.AGMT_EFF_DTTM = exp_pass_to_tgt_update_retired.lkp_AGMT_EFF_DTTM3 AND AGMT.EDW_STRT_DTTM = exp_pass_to_tgt_update_retired.lkp_EDW_START_DT4)
WHEN MATCHED THEN UPDATE
SET
HOST_AGMT_NUM = exp_pass_to_tgt_update_retired.LKP_HOST_AGMT_NUM4,
AGMT_OPN_DTTM = exp_pass_to_tgt_update_retired.lkp_AGMT_OPN_DTTM3,
AGMT_PLND_EXPN_DTTM = exp_pass_to_tgt_update_retired.lkp_AGMT_PLND_EXPN_DT3,
AGMT_TYPE_CD = exp_pass_to_tgt_update_retired.LKP_AGMT_TYPE_CD4,
AGMT_EFF_DTTM = exp_pass_to_tgt_update_retired.lkp_AGMT_EFF_DTTM3,
EDW_STRT_DTTM = exp_pass_to_tgt_update_retired.lkp_EDW_START_DT4,
EDW_END_DTTM = exp_pass_to_tgt_update_retired.EDW_END_DTTM,
TRANS_END_DTTM = exp_pass_to_tgt_update_retired.UpdateTime4;


-- Component AGMT_UPDATE, Type TARGET 
MERGE INTO DB_T_PROD_CORE.AGMT
USING exp_pass_to_tgt_update ON (AGMT.HOST_AGMT_NUM = exp_pass_to_tgt_update.LKP_HOST_AGMT_NUM3 AND AGMT.AGMT_PLND_EXPN_DTTM = exp_pass_to_tgt_update.lkp_AGMT_PLND_EXPN_DT3 AND AGMT.AGMT_TYPE_CD = exp_pass_to_tgt_update.LKP_AGMT_TYPE_CD3 AND AGMT.AGMT_EFF_DTTM = exp_pass_to_tgt_update.lkp_AGMT_EFF_DTTM3 AND AGMT.EDW_STRT_DTTM = exp_pass_to_tgt_update.lkp_EDW_START_DT3)
WHEN MATCHED THEN UPDATE
SET
HOST_AGMT_NUM = exp_pass_to_tgt_update.LKP_HOST_AGMT_NUM3,
AGMT_PLND_EXPN_DTTM = exp_pass_to_tgt_update.lkp_AGMT_PLND_EXPN_DT3,
AGMT_TYPE_CD = exp_pass_to_tgt_update.LKP_AGMT_TYPE_CD3,
AGMT_EFF_DTTM = exp_pass_to_tgt_update.lkp_AGMT_EFF_DTTM3,
EDW_STRT_DTTM = exp_pass_to_tgt_update.lkp_EDW_START_DT3,
EDW_END_DTTM = exp_pass_to_tgt_update.EDW_END_DT_exp3,
TRANS_END_DTTM = exp_pass_to_tgt_update.TRANS_END_DTTM;


-- Component exp_pass_to_tgt_ins_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_ins_upd AS
(
SELECT
upd_ins_upd.EDW_START_DT3 as EDW_START_DT3,
upd_ins_upd.EDW_END_DT3 as EDW_END_DT3,
upd_ins_upd.BusinessDateDefault3 as BusinessDateDefault3,
upd_ins_upd.out_AGMT_TYPE_CD3 as out_AGMT_TYPE_CD3,
upd_ins_upd.PolicyNumber3 as PolicyNumber3,
upd_ins_upd.OriginalEffectiveDate3 as OriginalEffectiveDate3,
upd_ins_upd.IssueDate3 as IssueDate3,
upd_ins_upd.out_agmt_cur_sts_cd3 as out_agmt_cur_sts_cd3,
upd_ins_upd.out_agmt_obtnd_cd3 as out_agmt_obtnd_cd3,
upd_ins_upd.out_agmt_sbtype_cd3 as out_agmt_sbtype_cd3,
upd_ins_upd.out_agmt_objtv_type_cd3 as out_agmt_objtv_type_cd3,
upd_ins_upd.out_mkt_risk_type_cd3 as out_mkt_risk_type_cd3,
upd_ins_upd.out_ntwk_srvr_agmt_type_cd3 as out_ntwk_srvr_agmt_type_cd3,
upd_ins_upd.out_frmlty_type_cd3 as out_frmlty_type_cd3,
upd_ins_upd.out_agmt_idntftn_cd3 as out_agmt_idntftn_cd3,
upd_ins_upd.out_trmtn_type_cd3 as out_trmtn_type_cd3,
upd_ins_upd.out_int_pmt_meth_type_cd3 as out_int_pmt_meth_type_cd3,
upd_ins_upd.PROCESS_ID3 as PROCESS_ID3,
upd_ins_upd.out_AGMT_SRC_CD3 as out_AGMT_SRC_CD3,
upd_ins_upd.in_AGMT_EFF_DTTM3 as in_AGMT_EFF_DTTM3,
upd_ins_upd.in_AGMT_PLND_EXPN_DT3 as in_AGMT_PLND_EXPN_DT3,
upd_ins_upd.AGMT_SRC_CD3 as AGMT_SRC_CD3,
upd_ins_upd.UpdateTime3 as UpdateTime3,
upd_ins_upd.BusinessEndDateDefault3 as BusinessEndDateDefault3,
upd_ins_upd.IsPaperLessOpted_alfa3 as IsPaperLessOpted_alfa3,
upd_ins_upd.source_record_id
FROM
upd_ins_upd
);


-- Component AGMT_INS_UPD, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT
(
AGMT_ID,
HOST_AGMT_NUM,
AGMT_OPN_DTTM,
AGMT_PLND_EXPN_DTTM,
AGMT_SIGND_DTTM,
AGMT_TYPE_CD,
AGMT_SRC_CD,
AGMT_CUR_STS_CD,
AGMT_OBTND_CD,
AGMT_SBTYPE_CD,
AGMT_PRCSG_DTTM,
AGMT_OBJTV_TYPE_CD,
MKT_RISK_TYPE_CD,
NTWK_SRVC_AGMT_TYPE_CD,
FRMLTY_TYPE_CD,
AGMT_IDNTFTN_CD,
TRMTN_TYPE_CD,
INT_PMT_METH_CD,
AGMT_EFF_DTTM,
MODL_EFF_DTTM,
PRCS_ID,
MODL_ACTL_END_DTTM,
CNTNUS_SRVC_DTTM,
NK_SRC_KEY,
SRC_SYS_CD,
EDW_STRT_DTTM,
EDW_END_DTTM,
EDOCS_IND,
TRANS_STRT_DTTM
)
SELECT
upd_ins_upd.lkp_AGMT_ID3 as AGMT_ID,
exp_pass_to_tgt_ins_upd.PolicyNumber3 as HOST_AGMT_NUM,
exp_pass_to_tgt_ins_upd.OriginalEffectiveDate3 as AGMT_OPN_DTTM,
exp_pass_to_tgt_ins_upd.in_AGMT_PLND_EXPN_DT3 as AGMT_PLND_EXPN_DTTM,
exp_pass_to_tgt_ins_upd.IssueDate3 as AGMT_SIGND_DTTM,
exp_pass_to_tgt_ins_upd.out_AGMT_TYPE_CD3 as AGMT_TYPE_CD,
exp_pass_to_tgt_ins_upd.AGMT_SRC_CD3 as AGMT_SRC_CD,
exp_pass_to_tgt_ins_upd.out_agmt_cur_sts_cd3 as AGMT_CUR_STS_CD,
exp_pass_to_tgt_ins_upd.out_agmt_obtnd_cd3 as AGMT_OBTND_CD,
exp_pass_to_tgt_ins_upd.out_agmt_sbtype_cd3 as AGMT_SBTYPE_CD,
exp_pass_to_tgt_ins_upd.BusinessDateDefault3 as AGMT_PRCSG_DTTM,
exp_pass_to_tgt_ins_upd.out_agmt_objtv_type_cd3 as AGMT_OBJTV_TYPE_CD,
exp_pass_to_tgt_ins_upd.out_mkt_risk_type_cd3 as MKT_RISK_TYPE_CD,
exp_pass_to_tgt_ins_upd.out_ntwk_srvr_agmt_type_cd3 as NTWK_SRVC_AGMT_TYPE_CD,
exp_pass_to_tgt_ins_upd.out_frmlty_type_cd3 as FRMLTY_TYPE_CD,
exp_pass_to_tgt_ins_upd.out_agmt_idntftn_cd3 as AGMT_IDNTFTN_CD,
exp_pass_to_tgt_ins_upd.out_trmtn_type_cd3 as TRMTN_TYPE_CD,
exp_pass_to_tgt_ins_upd.out_int_pmt_meth_type_cd3 as INT_PMT_METH_CD,
exp_pass_to_tgt_ins_upd.in_AGMT_EFF_DTTM3 as AGMT_EFF_DTTM,
exp_pass_to_tgt_ins_upd.BusinessDateDefault3 as MODL_EFF_DTTM,
exp_pass_to_tgt_ins_upd.PROCESS_ID3 as PRCS_ID,
exp_pass_to_tgt_ins_upd.BusinessEndDateDefault3 as MODL_ACTL_END_DTTM,
exp_pass_to_tgt_ins_upd.BusinessDateDefault3 as CNTNUS_SRVC_DTTM,
exp_pass_to_tgt_ins_upd.PolicyNumber3 as NK_SRC_KEY,
exp_pass_to_tgt_ins_upd.out_AGMT_SRC_CD3 as SRC_SYS_CD,
exp_pass_to_tgt_ins_upd.EDW_START_DT3 as EDW_STRT_DTTM,
exp_pass_to_tgt_ins_upd.EDW_END_DT3 as EDW_END_DTTM,
exp_pass_to_tgt_ins_upd.IsPaperLessOpted_alfa3 as EDOCS_IND,
exp_pass_to_tgt_ins_upd.UpdateTime3 as TRANS_STRT_DTTM
FROM
upd_ins_upd
INNER JOIN exp_pass_to_tgt_ins_upd ON upd_ins_upd.source_record_id = exp_pass_to_tgt_ins_upd.source_record_id;


END; ';