-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_RENEWALS_PROPERTY_EXTRACT("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_WR_MULTILINE_METRICS_DTL1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_WR_MULTILINE_METRICS_DTL1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as CAL_MTH,
$2 as AL_TOTAL,
$3 as GA_TOTAL,
$4 as MS_TOTAL,
$5 as GRAND_TOTAL,
$6 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
/*  For Property Renewal : */
                                                                

SELECT TO_CHAR(SRC.CALENDAR_DATE, ''YYYYMM'') AS CAL_MTH,

SUM(case when SRC.SALES_ST_CD = ''AL''  then SRC.UNIT_CNT_END_IFO else 0 END ) as AL_TOTAL ,

SUM(case when SRC.SALES_ST_CD = ''GA''  then SRC.UNIT_CNT_END_IFO else 0 END ) as GA_TOTAL ,

SUM(case when SRC.SALES_ST_CD = ''MS''  then SRC.UNIT_CNT_END_IFO else 0 END ) as MS_TOTAL ,

SUM( SRC.UNIT_CNT_END_IFO ) as GRAND_TOTAL

from

(

/*

Join DB_V_PROD_AUDT.D_PLCY_PRD to DB_V_PROD_ACT.WR_MULTILINE_METRICS_DTL on PLCY_PRD_PLCY_NUM to HOST_AGMT_NUM

Need to get the distinct PLCY_INCPTN_DT and PLCY_PRD_TERM_EFF_DT

*/

SELECT  distinct  

B.PLCY_INCPTN_DT , B.PLCY_PRD_TERM_EFF_DT, A.HOST_AGMT_NUM,

A.CALENDAR_DATE , A.UNIT_CNT_END_IFO , A.SALES_ST_CD , A.PLCY_TYPE_CD

FROM DB_V_PROD_ACT.WR_MULTILINE_METRICS_DTL A

JOIN DB_V_PROD_AUDT.D_PLCY_PRD B ON A.HOST_AGMT_NUM = B.PLCY_PRD_PLCY_NUM

WHERE calendar_date >= $Report_Start_Date

and calendar_date <= $Report_End_Date

AND A.PROD_RLUP = ''PROPERTY''

AND ( A.CALENDAR_DATE - B.PLCY_INCPTN_DT ) > 365    /*Inception Date must be 365 days prior to DB_SP_PROD.Calendar Date*/

/*Policy Term Effective Date must be the same month and year of DB_SP_PROD.Calendar Date*/

AND EXTRACT(YEAR , B.PLCY_PRD_TERM_EFF_DT) = Extract(YEAR , A.CALENDAR_DATE)

AND EXTRACT(MONTH, B.PLCY_PRD_TERM_EFF_DT) = Extract(MONTH , A.CALENDAR_DATE) 


union


/*

Join DB_T_STAG_MEMBXREF_PROD.STG_FARMPOLS to DB_V_PROD_ACT.WR_MULTILINE_METRICS_DTL on SC_POLICY_NUM and HOST_AGMT_NUM

Need to get the distinct PC_INCEPT_DATE and PC_EFF_DATE

*/

SELECT  distinct  

B.PC_INCEPT_DATE , B.PC_EFF_DATE, A.HOST_AGMT_NUM,

A.CALENDAR_DATE , A.UNIT_CNT_END_IFO , A.SALES_ST_CD , A.PLCY_TYPE_CD

FROM DB_V_PROD_ACT.WR_MULTILINE_METRICS_DTL A

JOIN DB_T_STAG_MEMBXREF_PROD.STG_FARMPOLS B ON A.HOST_AGMT_NUM = B.SC_POLICY_NUM

WHERE calendar_date >= $Report_Start_Date

and calendar_date <= $Report_End_Date

AND A.PROD_RLUP = ''PROPERTY''

AND ( A.CALENDAR_DATE - B.PC_INCEPT_DATE ) > 365   /*Inception Date must be greater than 184 days prior to DB_SP_PROD.Calendar Date*/

/*Policy Term Effective Date must be the same month and year of DB_SP_PROD.Calendar Date*/

AND EXTRACT(YEAR , B.PC_EFF_DATE) = Extract(YEAR , A.CALENDAR_DATE)

AND EXTRACT(MONTH , B.PC_EFF_DATE) = Extract(MONTH , A.CALENDAR_DATE)

) src
 group by CAL_MTH
ORDER BY CAL_MTH

) SRC
)
);


-- Component exp_pass_through_property, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through_property AS
(
SELECT
SQ_WR_MULTILINE_METRICS_DTL1.CAL_MTH as CAL_MTH,
SQ_WR_MULTILINE_METRICS_DTL1.AL_TOTAL as AL_TOTAL,
SQ_WR_MULTILINE_METRICS_DTL1.GA_TOTAL as GA_TOTAL,
SQ_WR_MULTILINE_METRICS_DTL1.MS_TOTAL as MS_TOTAL,
SQ_WR_MULTILINE_METRICS_DTL1.GRAND_TOTAL as GRAND_TOTAL,
SQ_WR_MULTILINE_METRICS_DTL1.source_record_id
FROM
SQ_WR_MULTILINE_METRICS_DTL1
);


-- Component property_renewal_extract, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE property_renewal_extract AS
(
SELECT
exp_pass_through_property.CAL_MTH as CAL_MTH,
exp_pass_through_property.AL_TOTAL as AL_TOTAL,
exp_pass_through_property.GA_TOTAL as GA_TOTAL,
exp_pass_through_property.MS_TOTAL as MS_TOTAL,
exp_pass_through_property.GRAND_TOTAL as GRAND_TOTAL
FROM
exp_pass_through_property
);


-- Component property_renewal_extract, Type EXPORT_DATA Exporting data
;


END; ';