-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_EXPSR_TRANS_EV_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare
	start_dttm timestamp;
	end_dttm timestamp;
	prcs_id int;




BEGIN 

start_dttm := current_timestamp();
end_dttm := current_timestamp();
prcs_id := 1;


-- Component LKP_TERADATA_ETL_REF_XLAT_EV_ACTVY_TYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_EV_ACTVY_TYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''EV_ACTVY_TYPE'' 

--             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in(''derived'',  ''CCTL_ACTIVITYCATEGORY.TYPECODE'' )



AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''GW'',''DS'' )

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERDATA_ETL_XLAT_SRC_SYS_EV_SBTYPE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERDATA_ETL_XLAT_SRC_SYS_EV_SBTYPE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''EV_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component sq_cc_transaction, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_cc_transaction AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ID,
$2 as CheckNumber,
$3 as Ev_act_type_cd,
$4 as Ev_Subtype,
$5 as Retired,
$6 as updatetime,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
with cc_transaction_temp (id_stg,Retired_stg,cc_transacion_updatetime_stg,

cc_check_UpdateTime_stg,

CheckNum_alfa_stg,Subtype_stg,exposureid_stg,eligible) as

(SELECT distinct a.id_stg,

a.Retired_stg,

a.updatetime_stg as cc_transaction_updatetime_stg,

cc_check.UpdateTime_stg as cc_check_updatetime_stg,

a.CheckNum_alfa_stg,

a.Subtype_stg,

a.exposureid_stg,

 case 

 when cctl_transactionstatus.typecode_stg = ''voided'' and GL_EventStaging_CC.payload_new_stg in (''voided_11'',''voided_15'')then ''N''  

 when cctl_transactionstatus.typecode_stg = ''transferred'' and GL_EventStaging_CC.payload_new_stg in (''transferred_11'',''transferred_13'',''cleared_13'') then ''N''

when cctl_transactionstatus.typecode_stg = ''recoded'' and GL_EventStaging_CC.payload_new_stg in (''recoded_14'',''recoded_11'',''issued_14'', ''cleared_14'',''requested_14'',''voided_14'',''transferred_14'') then ''N''/* EIM-41121 Added Transferred 14 */
 ELSE ''Y'' end as eligible

FROM DB_T_PROD_STAG.cc_transaction a

JOIN  DB_T_PROD_STAG.cctl_transactionstatus on a.status_stg= cctl_transactionstatus.ID_stg

LEFT OUTER JOIN DB_T_PROD_STAG.cc_check ON cc_check.id_stg = a.CheckID_stg

left outer JOIN DB_T_PROD_STAG.GL_EventStaging_CC ON a.publicid_stg=GL_EventStaging_CC.publicid_stg

WHERE 

 ((a.UpdateTime_stg >(:start_dttm)

	and a.UpdateTime_stg <= (:end_dttm))

or

(cc_check.UpdateTime_stg >(:start_dttm)

	and cc_check.UpdateTime_stg <= (:end_dttm)))



 and eligible = ''y''



) 



select distinct

a.id_stg as transactionid,

cast(a.id_stg as varchar(100)),

''EV_ACTVY_TYPE24'' as ev_act_type_cd,

''EV_SBTYPE2'' as ev_sbtype,

a.Retired_stg,

a.cc_transacion_updatetime_stg

from cc_transaction_temp a

inner join DB_T_PROD_STAG.cctl_transaction c on c.ID_stg = a.Subtype_stg

where c.TYPECODE_stg = ''Payment''

and a.exposureid_stg is not null



union

select distinct

a.id_stg as transactionid,

a.CheckNum_alfa_stg,

''EV_ACTVY_TYPE23'' as ev_act_type_cd,

''EV_SBTYPE2'' as ev_sbtype,

a.Retired_stg,

a.cc_transacion_updatetime_stg

from cc_transaction_temp a

inner join DB_T_PROD_STAG.cctl_transaction c on c.ID_stg = a.Subtype_stg

where c.TYPECODE_stg = ''Recovery''

and CheckNum_alfa_stg is not null

and a.exposureid_stg is not null
) SRC
)
);


-- Component exp_all_sourc, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_sourc AS
(
SELECT
sq_cc_transaction.ID as transactionid,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERDATA_ETL_XLAT_SRC_SYS_EV_SBTYPE_CD */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERDATA_ETL_XLAT_SRC_SYS_EV_SBTYPE_CD */ END as out_Ev_Subtype,
CASE WHEN LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_EV_ACTVY_TYPE */ IS NULL THEN ''UNK'' ELSE LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_EV_ACTVY_TYPE */ END as out_Ev_act_type_cd,
sq_cc_transaction.Retired as Retired,
CASE WHEN sq_cc_transaction.updatetime IS NULL THEN to_date ( ''1900-01-01'' , ''YYYY-MM-DD'' ) ELSE sq_cc_transaction.updatetime END as out_TRANS_STRT_DTTM1,
sq_cc_transaction.source_record_id,
row_number() over (partition by sq_cc_transaction.source_record_id order by sq_cc_transaction.source_record_id) as RNK
FROM
sq_cc_transaction
LEFT JOIN LKP_TERDATA_ETL_XLAT_SRC_SYS_EV_SBTYPE_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = sq_cc_transaction.Ev_Subtype
LEFT JOIN LKP_TERDATA_ETL_XLAT_SRC_SYS_EV_SBTYPE_CD LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = sq_cc_transaction.Ev_Subtype
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_EV_ACTVY_TYPE LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = sq_cc_transaction.Ev_act_type_cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_EV_ACTVY_TYPE LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = sq_cc_transaction.Ev_act_type_cd
QUALIFY RNK = 1
);


-- Component LKP_CLM_EXPSR_TRANS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM_EXPSR_TRANS AS
(
SELECT
LKP.CLM_EXPSR_TRANS_ID,
exp_all_sourc.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_sourc.source_record_id ORDER BY LKP.CLM_EXPSR_TRANS_ID desc,LKP.CLM_EXPSR_TRANS_SBTYPE_CD desc,LKP.CLM_EXPSR_ID desc,LKP.EXPSR_COST_TYPE_CD desc,LKP.EXPSR_COST_CTGY_TYPE_CD desc,LKP.PMT_TYPE_CD desc,LKP.CLM_EXPSR_TRANS_DTTM desc,LKP.CLM_EXPSR_TRANS_TXT desc,LKP.RCVRY_CTGY_TYPE_CD desc,LKP.DOES_NOT_ERODE_RSERV_IND desc,LKP.CRTD_BY_PRTY_ID desc,LKP.NK_CLM_EXPSR_TRANS_ID desc,LKP.GL_MTH_NUM desc,LKP.GL_YR_NUM desc,LKP.TRTY_CD desc,LKP.PRCS_ID desc,LKP.CLM_EXPSR_TRANS_STRT_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK
FROM
exp_all_sourc
LEFT JOIN (
SELECT	CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
		CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_SBTYPE_CD as CLM_EXPSR_TRANS_SBTYPE_CD,
		CLM_EXPSR_TRANS.CLM_EXPSR_ID as CLM_EXPSR_ID, CLM_EXPSR_TRANS.EXPSR_COST_TYPE_CD as EXPSR_COST_TYPE_CD,
		CLM_EXPSR_TRANS.EXPSR_COST_CTGY_TYPE_CD as EXPSR_COST_CTGY_TYPE_CD,
		CLM_EXPSR_TRANS.PMT_TYPE_CD as PMT_TYPE_CD,
		CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_DTTM as CLM_EXPSR_TRANS_DTTM,
		CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_TXT as CLM_EXPSR_TRANS_TXT, CLM_EXPSR_TRANS.RCVRY_CTGY_TYPE_CD as RCVRY_CTGY_TYPE_CD,
		CLM_EXPSR_TRANS.DOES_NOT_ERODE_RSERV_IND as DOES_NOT_ERODE_RSERV_IND,
		CLM_EXPSR_TRANS.CRTD_BY_PRTY_ID as CRTD_BY_PRTY_ID, 
		CLM_EXPSR_TRANS.GL_MTH_NUM as GL_MTH_NUM, CLM_EXPSR_TRANS.GL_YR_NUM as GL_YR_NUM,
		CLM_EXPSR_TRANS.TRTY_CD as TRTY_CD, CLM_EXPSR_TRANS.PRCS_ID as PRCS_ID,
		CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_STRT_DTTM as CLM_EXPSR_TRANS_STRT_DTTM,
		CLM_EXPSR_TRANS.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM_EXPSR_TRANS.EDW_END_DTTM as EDW_END_DTTM,
		CLM_EXPSR_TRANS.NK_CLM_EXPSR_TRANS_ID as NK_CLM_EXPSR_TRANS_ID 
FROM	db_t_prod_core.CLM_EXPSR_TRANS
QUALIFY	ROW_NUMBER() OVER(
PARTITION BY  CLM_EXPSR_TRANS.NK_CLM_EXPSR_TRANS_ID  
ORDER BY CLM_EXPSR_TRANS.EDW_END_DTTM DESC) = 1
) LKP ON LKP.NK_CLM_EXPSR_TRANS_ID = exp_all_sourc.transactionid
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_all_sourc.source_record_id ORDER BY LKP.CLM_EXPSR_TRANS_ID desc,LKP.CLM_EXPSR_TRANS_SBTYPE_CD desc,LKP.CLM_EXPSR_ID desc,LKP.EXPSR_COST_TYPE_CD desc,LKP.EXPSR_COST_CTGY_TYPE_CD desc,LKP.PMT_TYPE_CD desc,LKP.CLM_EXPSR_TRANS_DTTM desc,LKP.CLM_EXPSR_TRANS_TXT desc,LKP.RCVRY_CTGY_TYPE_CD desc,LKP.DOES_NOT_ERODE_RSERV_IND desc,LKP.CRTD_BY_PRTY_ID desc,LKP.NK_CLM_EXPSR_TRANS_ID desc,LKP.GL_MTH_NUM desc,LKP.GL_YR_NUM desc,LKP.TRTY_CD desc,LKP.PRCS_ID desc,LKP.CLM_EXPSR_TRANS_STRT_DTTM desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc)  
= 1
);


-- Component LKP_EV, Type LOOKUP 
-- QUALIFY fields removed: LKP.EV_DESC desc,LKP.EV_STRT_DTTM desc,LKP.EV_END_DTTM desc,LKP.EV_RSN_CD desc,LKP.AGMT_ID desc,LKP.PRCSD_SRC_SYS_CD desc,LKP.FUNC_CD desc,LKP.EV_DTTM desc,LKP.EDW_STRT_DTTM desc
CREATE OR REPLACE TEMPORARY TABLE LKP_EV AS
(
SELECT
LKP.EV_ID,
exp_all_sourc.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_all_sourc.source_record_id ORDER BY LKP.EV_ID desc,LKP.SRC_TRANS_ID desc,LKP.EV_SBTYPE_CD desc,LKP.EV_ACTVY_TYPE_CD desc) RNK
FROM
exp_all_sourc
LEFT JOIN (
SELECT DISTINCT
 EV.EV_ID AS EV_ID, EV.SRC_TRANS_ID AS SRC_TRANS_ID, EV.EV_SBTYPE_CD AS EV_SBTYPE_CD,
  EV.EV_ACTVY_TYPE_CD AS EV_ACTVY_TYPE_CD 
  FROM db_t_prod_core.EV WHERE EV_ACTVY_TYPE_CD  in (''CLAIMSAP'', ''CLAIMSAR'') QUALIFY ROW_NUMBER() OVER(PARTITION BY  EV.EV_SBTYPE_CD,EV.EV_ACTVY_TYPE_CD,EV.SRC_TRANS_ID ORDER BY EV.EDW_END_DTTM DESC) = 1
) LKP ON LKP.SRC_TRANS_ID = exp_all_sourc.transactionid AND LKP.EV_SBTYPE_CD = exp_all_sourc.out_Ev_Subtype AND LKP.EV_ACTVY_TYPE_CD = exp_all_sourc.out_Ev_act_type_cd
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_all_sourc.source_record_id ORDER BY LKP.EV_ID desc,LKP.SRC_TRANS_ID desc,LKP.EV_SBTYPE_CD desc,LKP.EV_ACTVY_TYPE_CD desc)  
= 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
LKP_EV.EV_ID as EV_ID,
LKP_CLM_EXPSR_TRANS.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
:PRCS_ID as out_PRCS_ID,
exp_all_sourc.Retired as Retired,
exp_all_sourc.out_TRANS_STRT_DTTM1 as in_TRANS_STRT_DTTM,
exp_all_sourc.source_record_id
FROM
exp_all_sourc
INNER JOIN LKP_CLM_EXPSR_TRANS ON exp_all_sourc.source_record_id = LKP_CLM_EXPSR_TRANS.source_record_id
INNER JOIN LKP_EV ON LKP_CLM_EXPSR_TRANS.source_record_id = LKP_EV.source_record_id
);


-- Component rtr_filter_invalid_record_VALID, Type ROUTER Output Group VALID
create or replace temporary table rtr_filter_invalid_record_VALID as
SELECT
exp_data_transformation.EV_ID as EV_ID,
exp_data_transformation.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
exp_data_transformation.out_PRCS_ID as out_PRCS_ID,
exp_data_transformation.Retired as Retired,
exp_data_transformation.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE CASE WHEN ( exp_data_transformation.CLM_EXPSR_TRANS_ID IS NOT NULL and exp_data_transformation.EV_ID IS NOT NULL ) THEN TRUE ELSE FALSE END;


-- Component LKP_CLM_EXPSR_TRANS_EV, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CLM_EXPSR_TRANS_EV AS
(
SELECT
LKP.EV_ID,
LKP.CLM_EXPSR_TRANS_ID,
LKP.EDW_END_DTTM,
rtr_filter_invalid_record_VALID.source_record_id,
ROW_NUMBER() OVER(PARTITION BY rtr_filter_invalid_record_VALID.source_record_id ORDER BY LKP.EV_ID asc,LKP.CLM_EXPSR_TRANS_ID asc,LKP.EDW_END_DTTM asc) RNK
FROM
rtr_filter_invalid_record_VALID
LEFT JOIN (
SELECT CLM_EXPSR_TRANS_EV.EDW_END_DTTM as EDW_END_DTTM, CLM_EXPSR_TRANS_EV.EV_ID as EV_ID, CLM_EXPSR_TRANS_EV.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID 
FROM db_t_prod_core.CLM_EXPSR_TRANS_EV
QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM_EXPSR_TRANS_ID,EV_ID ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.EV_ID = rtr_filter_invalid_record_VALID.EV_ID AND LKP.CLM_EXPSR_TRANS_ID = rtr_filter_invalid_record_VALID.CLM_EXPSR_TRANS_ID
QUALIFY ROW_NUMBER() OVER(PARTITION BY rtr_filter_invalid_record_VALID.source_record_id ORDER BY LKP.EV_ID asc,LKP.CLM_EXPSR_TRANS_ID asc,LKP.EDW_END_DTTM asc) 
= 1
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
LKP_CLM_EXPSR_TRANS_EV.EV_ID as lkp_EV_ID,
LKP_CLM_EXPSR_TRANS_EV.CLM_EXPSR_TRANS_ID as lkp_CLM_EXPSR_TRANS_ID,
rtr_filter_invalid_record_VALID.EV_ID as EV_ID,
rtr_filter_invalid_record_VALID.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
rtr_filter_invalid_record_VALID.out_PRCS_ID as PRCS_ID,
rtr_filter_invalid_record_VALID.Retired as Retired,
CASE WHEN LKP_CLM_EXPSR_TRANS_EV.CLM_EXPSR_TRANS_ID IS NULL and LKP_CLM_EXPSR_TRANS_EV.EV_ID IS NULL THEN ''I'' ELSE ''R'' END as Flag,
LKP_CLM_EXPSR_TRANS_EV.EDW_END_DTTM as lkp_EDW_END_DTTM,
rtr_filter_invalid_record_VALID.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM4,
rtr_filter_invalid_record_VALID.source_record_id
FROM
rtr_filter_invalid_record_VALID
INNER JOIN LKP_CLM_EXPSR_TRANS_EV ON rtr_filter_invalid_record_VALID.source_record_id = LKP_CLM_EXPSR_TRANS_EV.source_record_id
);


-- Component rtr_clm_expsr_trans_ev_INSERT, Type ROUTER Output Group INSERT
create or replace temporary table rtr_clm_expsr_trans_ev_INSERT as 
SELECT
EXPTRANS.lkp_EV_ID as lkp_EV_ID,
EXPTRANS.lkp_CLM_EXPSR_TRANS_ID as lkp_CLM_EXPSR_TRANS_ID,
EXPTRANS.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
EXPTRANS.EV_ID as EV_ID,
EXPTRANS.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
EXPTRANS.PRCS_ID as PRCS_ID,
EXPTRANS.in_TRANS_STRT_DTTM4 as in_TRANS_STRT_DTTM4,
EXPTRANS.Retired as Retired,
EXPTRANS.Flag as Flag,
EXPTRANS.source_record_id
FROM
EXPTRANS
WHERE EXPTRANS.Flag = ''I'' OR ( EXPTRANS.lkp_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) and EXPTRANS.Retired = 0 );


-- Component rtr_clm_expsr_trans_ev_RETIRED, Type ROUTER Output Group RETIRED
create or replace temporary table rtr_clm_expsr_trans_ev_RETIRED as
SELECT
EXPTRANS.lkp_EV_ID as lkp_EV_ID,
EXPTRANS.lkp_CLM_EXPSR_TRANS_ID as lkp_CLM_EXPSR_TRANS_ID,
EXPTRANS.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
EXPTRANS.EV_ID as EV_ID,
EXPTRANS.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
EXPTRANS.PRCS_ID as PRCS_ID,
EXPTRANS.in_TRANS_STRT_DTTM4 as in_TRANS_STRT_DTTM4,
EXPTRANS.Retired as Retired,
EXPTRANS.Flag as Flag,
EXPTRANS.source_record_id
FROM
EXPTRANS
WHERE EXPTRANS.Flag = ''R'' and EXPTRANS.Retired != 0 and EXPTRANS.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );


-- Component upstr_clm_expsr_trans_ev_ins1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upstr_clm_expsr_trans_ev_ins1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_clm_expsr_trans_ev_RETIRED.lkp_EV_ID as EV_ID,
rtr_clm_expsr_trans_ev_RETIRED.lkp_CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
rtr_clm_expsr_trans_ev_RETIRED.PRCS_ID as PRCS_ID,
1 as UPDATE_STRATEGY_ACTION,
source_record_id
FROM
rtr_clm_expsr_trans_ev_RETIRED
);


-- Component upstr_clm_expsr_trans_ev_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upstr_clm_expsr_trans_ev_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_clm_expsr_trans_ev_INSERT.EV_ID as EV_ID,
rtr_clm_expsr_trans_ev_INSERT.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
rtr_clm_expsr_trans_ev_INSERT.PRCS_ID as PRCS_ID,
rtr_clm_expsr_trans_ev_INSERT.Retired as Retired1,
rtr_clm_expsr_trans_ev_INSERT.in_TRANS_STRT_DTTM4 as TRANS_STRT_DTTM,
0 as UPDATE_STRATEGY_ACTION,
source_record_id
FROM
rtr_clm_expsr_trans_ev_INSERT
);


-- Component exp_clm_expsr_trans_ev_ins1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_clm_expsr_trans_ev_ins1 AS
(
SELECT
upstr_clm_expsr_trans_ev_ins1.EV_ID as EV_ID,
upstr_clm_expsr_trans_ev_ins1.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
upstr_clm_expsr_trans_ev_ins1.PRCS_ID as PRCS_ID,
CURRENT_TIMESTAMP as EDW_END_DTTM,
upstr_clm_expsr_trans_ev_ins1.source_record_id
FROM
upstr_clm_expsr_trans_ev_ins1
);


-- Component tgt_clm_expsr_trans_ev1, Type TARGET 
MERGE INTO DB_T_PROD_CORE.CLM_EXPSR_TRANS_EV
USING exp_clm_expsr_trans_ev_ins1 ON (CLM_EXPSR_TRANS_EV.EV_ID = exp_clm_expsr_trans_ev_ins1.EV_ID AND CLM_EXPSR_TRANS_EV.CLM_EXPSR_TRANS_ID = exp_clm_expsr_trans_ev_ins1.CLM_EXPSR_TRANS_ID)
WHEN MATCHED THEN UPDATE
SET
EV_ID = exp_clm_expsr_trans_ev_ins1.EV_ID,
CLM_EXPSR_TRANS_ID = exp_clm_expsr_trans_ev_ins1.CLM_EXPSR_TRANS_ID,
PRCS_ID = exp_clm_expsr_trans_ev_ins1.PRCS_ID,
EDW_END_DTTM = exp_clm_expsr_trans_ev_ins1.EDW_END_DTTM;


-- Component exp_clm_expsr_trans_ev_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_clm_expsr_trans_ev_ins AS
(
SELECT
upstr_clm_expsr_trans_ev_ins.EV_ID as EV_ID,
upstr_clm_expsr_trans_ev_ins.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
upstr_clm_expsr_trans_ev_ins.PRCS_ID as PRCS_ID,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
CASE WHEN upstr_clm_expsr_trans_ev_ins.Retired1 <> 0 THEN CURRENT_TIMESTAMP ELSE TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) END as EDW_END_DTTM,
upstr_clm_expsr_trans_ev_ins.TRANS_STRT_DTTM as TRANS_STRT_DTTM1,
CASE WHEN upstr_clm_expsr_trans_ev_ins.Retired1 != 0 THEN upstr_clm_expsr_trans_ev_ins.TRANS_STRT_DTTM ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as TRANS_END_DTTM,
upstr_clm_expsr_trans_ev_ins.source_record_id
FROM
upstr_clm_expsr_trans_ev_ins
);


-- Component tgt_clm_expsr_trans_ev, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_EXPSR_TRANS_EV
(
EV_ID,
CLM_EXPSR_TRANS_ID,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_clm_expsr_trans_ev_ins.EV_ID as EV_ID,
exp_clm_expsr_trans_ev_ins.CLM_EXPSR_TRANS_ID as CLM_EXPSR_TRANS_ID,
exp_clm_expsr_trans_ev_ins.PRCS_ID as PRCS_ID,
exp_clm_expsr_trans_ev_ins.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_clm_expsr_trans_ev_ins.EDW_END_DTTM as EDW_END_DTTM,
exp_clm_expsr_trans_ev_ins.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_clm_expsr_trans_ev_ins.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_clm_expsr_trans_ev_ins;


END; ';