-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_BOP_CHURCH_LIMIT_EXTRACT("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_BOP_CHURCH, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_BOP_CHURCH AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as DWELLING_LIMITS,
$2 as POLICY_NUM,
$3 as POLICY_AMOUNT,
$4 as PRTY_ASSET_SPEC_VAL,
$5 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
/* -------BOP_CHURCH COMM Pricing---------------------------------------------- */
/* 27. BOP and Church Data */
select distinct case when Dwelling between 0 and 50000 then ''0 to 50,000''  

		  when Dwelling between 50001 and 100000 then ''50,001 to 100,000'' 

          when Dwelling between 100001 and 200000 then ''100,001 to 200,000'' 

          when Dwelling between 200001 and 300000 then ''200,001 to 300,000''  

          when Dwelling between 300001 and 400000 then ''300,001 to 400,000''  

          when Dwelling between 400001 and 500000 then ''400,001 to 500,000'' 

          when Dwelling between 500001 and 600000 then ''500,001 to 600,000'' 

          when Dwelling between 600001 and 700000 then ''600,001 to 700,000''

          when Dwelling between 700001 and 800000 then ''700,001 to 800,000''

          when Dwelling between 800001 and 900000 then ''800,001 to 900,000''

          when Dwelling between 900001 and 1000000 then ''900,001 to 1,000,000''

          when Dwelling between 1000001 and 1100000 then ''1,000,001 to 1,100,000''

		  when Dwelling > 1100000 then ''1,100,000+''

          end as DWELLING_LIMITS ,POLICY_NUM,POLICY_AMOUNT,PRTY_ASSET_SPEC_VAL

From

(

Select aa.host_agmt_num as POLICY_NUM,bb.agmt_feat_amt as Dwelling, dd.PLCY_AMT as POLICY_AMOUNT, F.PRTY_ASSET_SPEC_VAL as PRTY_ASSET_SPEC_VAL

from DB_T_PROD_COMN.GW_INFORCE_SVC aa

inner join DB_T_PROD_CORE.agmt_feat bb on aa.agmt_id = bb.agmt_id

inner join DB_T_PROD_CORE.FEAT cc on cc.FEAT_ID = bb.feat_id

inner join DB_T_PROD_CORE.PLCY_MTRC dd on dd.AGMT_ID = aa.AGMT_ID

INNER JOIN DB_T_PROD_CORE.agmt_asset aaa on aaa.AGMT_ID = aa.AGMT_ID

INNER JOIN DB_T_PROD_CORE.PRTY_ASSET E ON E.PRTY_ASSET_ID = aaa.PRTY_ASSET_ID

INNER JOIN DB_T_PROD_CORE.PRTY_ASSET_SPEC F ON F.PRTY_ASSET_ID = E.PRTY_ASSET_ID

INNER JOIN DB_T_PROD_CORE.AGMT_SPEC ASP on ASP.AGMT_ID = aa.AGMT_ID

where aa.LOB_CD = ''BUSINESS OWNERS''

and aa.PROD_DESC = ''Businessowners''

and aa.AGMT_STS_CD = ''inforce''

AND F.PRTY_ASSET_SPEC_TYPE_CD = ''PROT''

and cc.INSRNC_CVGE_TYPE_CD in (''LIAB'') and cc.feat_name <> ''Additional Limit''

and cc.feat_dtl_modl_type_name = ''Limit''

and dd.INSRNC_MTRC_TYPE_CD = ''prem''

and asp.AGMT_SPEC_TYPE_CD = ''NMPERILIND''

and bb.agmt_feat_amt is not null

Group by 1,2,3,4

)A
) SRC
)
);


-- Component exp_trans, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_trans AS
(
SELECT
SQ_BOP_CHURCH.DWELLING_LIMITS as DWELLING_LIMITS,
SQ_BOP_CHURCH.POLICY_NUM as POLICY_NUM,
SQ_BOP_CHURCH.POLICY_AMOUNT as POLICY_AMOUNT,
SQ_BOP_CHURCH.PRTY_ASSET_SPEC_VAL as PRTY_ASSET_SPEC_VAL,
SQ_BOP_CHURCH.source_record_id
FROM
SQ_BOP_CHURCH
);


-- Component BOP_CHURCH1, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE BOP_CHURCH1 AS
(
SELECT
exp_trans.DWELLING_LIMITS as DWELLING_LIMITS,
exp_trans.POLICY_NUM as POLICY_NUM,
exp_trans.POLICY_AMOUNT as POLICY_AMOUNT,
exp_trans.PRTY_ASSET_SPEC_VAL as PRTY_ASSET_SPEC_VAL
FROM
exp_trans
);


-- Component BOP_CHURCH1, Type EXPORT_DATA Exporting data
;


END; ';