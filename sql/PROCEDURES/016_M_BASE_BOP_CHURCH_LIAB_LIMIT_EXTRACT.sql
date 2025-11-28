-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_BOP_CHURCH_LIAB_LIMIT_EXTRACT("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_BOP_CHURCH_LIAB, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_BOP_CHURCH_LIAB AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as POLICY_NUM,
$2 as PLCY_TYPE,
$3 as STATE,
$4 as AGG_LIMIT,
$5 as OCC_LIMIT,
$6 as OS_LIMITS,
$7 as PLCY_AMT,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
/* 29. BOP and Church LIAB Limit Data */
/* -This is only looking for Business Liability Limit */
select distinct host_agmt_num as POLICY_NUM, prod_desc as PLCY_TYPE, BASE_ST as STATE,  

sum(case when FEAT_DESC like ''General%'' then FEAT_DTL_VAL end) over (partition by host_agmt_num) as AGG_LIMIT, 

sum(case when FEAT_DESC like ''Each Occurrence%'' then FEAT_DTL_VAL end) over (partition by host_agmt_num) as OCC_LIMIT,

case when OCC_LIMIT = 300000 and AGG_LIMIT = 600000  then ''300k/600k''  

          when OCC_LIMIT = 500000 and AGG_LIMIT = 1000000 then ''500k/1m''

          when OCC_LIMIT = 1000000 and AGG_LIMIT = 2000000 then ''1m/2m''

		  when OCC_LIMIT = 2000000 and AGG_LIMIT = 4000000 then ''2m/4m''

          end as OS_LIMITS , PLCY_AMT

from(

select distinct aa.host_agmt_num, aa.PROD_DESC, aa.BASE_ST, dd.PLCY_AMT, f2.FEAT_DTL_VAL, f2.FEAT_DESC

from DB_T_PROD_COMN.GW_INFORCE_SVC aa

join DB_T_PROD_CORE.agmt_feat bb on aa.agmt_id = bb.agmt_id

JOIN DB_T_PROD_CORE.FEAT_RLTD fr on bb.FEAT_ID = fr.RLTD_FEAT_ID

JOIN DB_T_PROD_CORE.FEAT f2 on f2.FEAT_ID = fr.RLTD_FEAT_ID

join DB_T_PROD_CORE.FEAT cc on cc.FEAT_ID = fr.feat_id

join DB_T_PROD_CORE.PLCY_MTRC dd on dd.AGMT_ID = aa.AGMT_ID

where aa.LOB_CD = ''BUSINESS OWNERS''

and aa.AGMT_STS_CD = ''inforce''

and cc.NK_SRC_KEY = ''BP7BusinessLiability''

and (f2.FEAT_DESC like ''General%'' or f2.FEAT_DESC like ''Each Occurrence%'')

and dd.PLCY_AMT <> 0

and dd.INSRNC_MTRC_TYPE_CD = ''prem''

and bb.edw_end_dttm = ''9999-12-31 23:59:59.999999''

and fr.edw_end_dttm = ''9999-12-31 23:59:59.999999''

and cc.edw_end_dttm = ''9999-12-31 23:59:59.999999''

and dd.edw_end_dttm = ''9999-12-31 23:59:59.999999''

)A
) SRC
)
);


-- Component Exp_trans, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE Exp_trans AS
(
SELECT
SQ_BOP_CHURCH_LIAB.POLICY_NUM as POLICY_NUM,
SQ_BOP_CHURCH_LIAB.PLCY_TYPE as PLCY_TYPE,
SQ_BOP_CHURCH_LIAB.STATE as STATE,
SQ_BOP_CHURCH_LIAB.AGG_LIMIT as AGG_LIMIT,
SQ_BOP_CHURCH_LIAB.OCC_LIMIT as OCC_LIMIT,
SQ_BOP_CHURCH_LIAB.OS_LIMITS as OS_LIMITS,
SQ_BOP_CHURCH_LIAB.PLCY_AMT as PLCY_AMT,
SQ_BOP_CHURCH_LIAB.source_record_id
FROM
SQ_BOP_CHURCH_LIAB
);


-- Component BOP_CHURCH_LIAB1, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE BOP_CHURCH_LIAB1 AS
(
SELECT
Exp_trans.POLICY_NUM as POLICY_NUM,
Exp_trans.PLCY_TYPE as PLCY_TYPE,
Exp_trans.STATE as STATE,
Exp_trans.AGG_LIMIT as AGG_LIMIT,
Exp_trans.OCC_LIMIT as OCC_LIMIT,
Exp_trans.OS_LIMITS as OS_LIMITS,
Exp_trans.PLCY_AMT as PLCY_AMT
FROM
Exp_trans
);


-- Component BOP_CHURCH_LIAB1, Type EXPORT_DATA Exporting data
;


END; ';