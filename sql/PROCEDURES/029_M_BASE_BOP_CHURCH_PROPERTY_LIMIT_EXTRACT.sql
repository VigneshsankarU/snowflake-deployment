-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_BOP_CHURCH_PROPERTY_LIMIT_EXTRACT("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_BOP_CHURCH_PROP, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_BOP_CHURCH_PROP AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as POLICY_NUM,
$2 as POLICY_TYPE,
$3 as STATE,
$4 as COV_LIMITS,
$5 as PLCY_AMT,
$6 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
/* 28. BOP and Church Property Limit Data */
Select POLICY_NUM,POLICY_TYPE,STATE,

case when Coverage between 0 and 50000 then ''0 to 50,000''  

		  when Coverage between 50001 and 100000 then ''50,001 to 100,000'' 

          when Coverage between 100001 and 200000 then ''100,001 to 200,000'' 

          when Coverage between 200001 and 300000 then ''200,001 to 300,000''  

          when Coverage between 300001 and 400000 then ''300,001 to 400,000''  

          when Coverage between 400001 and 500000 then ''400,001 to 500,000'' 

          when Coverage between 500001 and 600000 then ''500,001 to 600,000'' 

          when Coverage between 600001 and 700000 then ''600,001 to 700,000''

          when Coverage between 700001 and 800000 then ''700,001 to 800,000''

          when Coverage between 800001 and 900000 then ''800,001 to 900,000''

          when Coverage between 900001 and 1000000 then ''900,001 to 1,000,000''

          when Coverage between 1000001 and 1500000 then ''1,000,001 to 1,500,000''

		  when Coverage between 1500001 and 2000000 then ''1,500,001 to 2,000,000''

		  when Coverage between 2000001 and 2500000 then ''2,000,001 to 2,500,000''

		  when Coverage between 2500001 and 3000000 then ''2,500,001 to 3,000,000''

		  when Coverage between 3000001 and 3500000 then ''3,000,001 to 3,500,000''

		  when Coverage between 3500001 and 4000000 then ''3,500,001 to 4,000,000''

		  when Coverage between 4000001 and 4500000 then ''4,000,001 to 4,500,000''

		  when Coverage between 4500001 and 5000000 then ''4,500,001 to 5,000,000''

		  when Coverage between 5000001 and 5500000 then ''5,000,001 to 5,500,000''

		  when Coverage between 5500001 and 6000000 then ''5,500,001 to 6,000,000''

		  when Coverage > 6000001 then ''6,000,000+''

          end as COV_LIMITS,PLCY_AMT

from(

Select distinct b.host_agmt_num as POLICY_NUM, PROD_DESC as POLICY_TYPE, BASE_ST as STATE, sum (b.agmt_asset_feat_amt) as Coverage,  b.TOT_PREM as PLCY_AMT

from (

select distinct aa.host_agmt_num, bb.agmt_asset_feat_amt, aa.TOT_PREM, aa.prod_desc, aa.BASE_ST

from DB_T_PROD_COMN.GW_INFORCE_SVC aa

inner join DB_T_PROD_CORE.agmt_insrd_asset_feat bb on aa.agmt_id = bb.agmt_id

inner join DB_T_PROD_CORE.FEAT cc on cc.FEAT_ID = bb.feat_id

inner join DB_T_PROD_CORE.PLCY_MTRC dd on dd.AGMT_ID = aa.AGMT_ID

where aa.LOB_CD = ''BUSINESS OWNERS''

and aa.AGMT_STS_CD = ''inforce''

and cc.INSRNC_CVGE_TYPE_CD in (''BLDG'',''PP'')

and cc.feat_name = ''limit''

and dd.INSRNC_MTRC_TYPE_CD = ''prem''

and bb.edw_end_dttm = ''9999-12-31 23:59:59.999999''

and cc.edw_end_dttm = ''9999-12-31 23:59:59.999999''

and dd.edw_end_dttm = ''9999-12-31 23:59:59.999999''

)b

group by 1,2,3,5

)C

order by 1,4
) SRC
)
);


-- Component exp_trans, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_trans AS
(
SELECT
SQ_BOP_CHURCH_PROP.POLICY_NUM as POLICY_NUM,
SQ_BOP_CHURCH_PROP.POLICY_TYPE as POLICY_TYPE,
SQ_BOP_CHURCH_PROP.STATE as STATE,
SQ_BOP_CHURCH_PROP.COV_LIMITS as COV_LIMITS,
SQ_BOP_CHURCH_PROP.PLCY_AMT as PLCY_AMT,
SQ_BOP_CHURCH_PROP.source_record_id
FROM
SQ_BOP_CHURCH_PROP
);


-- Component BOP_CHURCH_PROP1, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE BOP_CHURCH_PROP1 AS
(
SELECT
exp_trans.POLICY_NUM as POLICY_NUM,
exp_trans.POLICY_TYPE as POLICY_TYPE,
exp_trans.STATE as STATE,
exp_trans.COV_LIMITS as COV_LIMITS,
exp_trans.PLCY_AMT as PLCY_AMT
FROM
exp_trans
);


-- Component BOP_CHURCH_PROP1, Type EXPORT_DATA Exporting data
;


END; ';