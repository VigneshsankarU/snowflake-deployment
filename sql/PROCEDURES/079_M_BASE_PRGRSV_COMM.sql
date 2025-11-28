-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRGRSV_COMM("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_pgrsv_comm_report_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_pgrsv_comm_report
(
MTHEND varchar(25),
PREFIX varchar(25),
CODE varchar(25),
AGT_NAME varchar(25),
POLICY varchar(25),
SUFFIX varchar(25),
NAME varchar(25),
TRAN_DT varchar(10),
MKTCOMM varchar(25),
TRAN varchar(25),
PROD varchar(25),
EFF_DT varchar(10),
EXP_DT varchar(10),
TERM varchar(25),
STATE varchar(25),
PERCENT varchar(13),
COHORT_DT varchar(10),
PREMIUM varchar(13),
CMSN varchar(13),
NETBAL varchar(13),
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_pgrsv_comm_report_SRC, Type IMPORT_DATA Importing Data
--;


-- Component expr_prgrsv_trans, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE expr_prgrsv_trans AS
(
SELECT
SQ_pgrsv_comm_report.MTHEND as MTHEND,
SQ_pgrsv_comm_report.PREFIX as PREFIX,
SQ_pgrsv_comm_report.CODE as CODE,
SQ_pgrsv_comm_report.AGT_NAME as AGT_NAME,
SQ_pgrsv_comm_report.POLICY as POLICY,
SQ_pgrsv_comm_report.SUFFIX as SUFFIX,
--CASE WHEN SUBSTR ( LTRIM ( RTRIM ( SQ_pgrsv_comm_report.NAME ) ) , INSTR ( LTRIM ( RTRIM ( SQ_pgrsv_comm_report.NAME ) ) , ''MVR FEE'' , 1 , 1 ) , 7 ) = ''MVR FEE'' THEN ''MVR FEE'' ELSE LTRIM ( RTRIM ( SQ_pgrsv_comm_report.NAME ) ) END as o_NAME,
CASE 
  WHEN SUBSTR(
         LTRIM(RTRIM(SQ_pgrsv_comm_report.NAME)),
         POSITION(''MVR FEE'' IN LTRIM(RTRIM(SQ_pgrsv_comm_report.NAME))),
         7
       ) = ''MVR FEE''
  THEN ''MVR FEE''
  ELSE LTRIM(RTRIM(SQ_pgrsv_comm_report.NAME))
END AS o_NAME,
TO_DATE ( SQ_pgrsv_comm_report.TRAN_DT , ''MM/DD/YYYY'' ) as o_TRAN_DT,
SQ_pgrsv_comm_report.MKTCOMM as MKTCOMM,
SQ_pgrsv_comm_report.TRAN as TRAN,
SQ_pgrsv_comm_report.PROD as PROD,
CASE WHEN  LTRIM ( RTRIM ( SQ_pgrsv_comm_report.EFF_DT ) ) in (''.'' , '','' , '''' ) THEN TO_DATE ( ''01/01/1900'' , ''MM/DD/YYYY'' ) ELSE TO_DATE ( SQ_pgrsv_comm_report.EFF_DT , ''MM/DD/YYYY'' ) END as o_EFF_DT,
CASE WHEN  LTRIM ( RTRIM ( SQ_pgrsv_comm_report.EXP_DT ) ) in  (''.'' , '','' , '''' ) THEN TO_DATE ( ''12/31/9999'' , ''MM/DD/YYYY'' ) ELSE TO_DATE ( SQ_pgrsv_comm_report.EXP_DT , ''MM/DD/YYYY'' ) END as o_EXP_DT,
SQ_pgrsv_comm_report.TERM as TERM,
SQ_pgrsv_comm_report.STATE as STATE,
IFNULL(TRY_TO_DECIMAL(SQ_pgrsv_comm_report.PERCENT), 0) * 100 as o_PERCENT,
CASE WHEN  LTRIM ( RTRIM ( SQ_pgrsv_comm_report.COHORT_DT ) ) in  (''.'' , '','' , '''' ) THEN TO_DATE ( ''01/01/1900'' , ''MM/DD/YYYY'' ) ELSE TO_DATE ( SQ_pgrsv_comm_report.COHORT_DT , ''MM/DD/YYYY'' ) END as o_COHORT_DT,
IFNULL(TRY_TO_DECIMAL(SQ_pgrsv_comm_report.PREMIUM), 0) as o_PREMIUM,
IFNULL(TRY_TO_DECIMAL(SQ_pgrsv_comm_report.CMSN), 0) as o_CMSN,
IFNULL(TRY_TO_DECIMAL(SQ_pgrsv_comm_report.NETBAL), 0) as o_NETBAL,
$PRCS_ID as PRCS_ID,
SQ_pgrsv_comm_report.source_record_id
FROM
SQ_pgrsv_comm_report
);


-- Component PRGRSV_COMM, Type TARGET 
INSERT INTO DB_T_PROD_COMN.PRGRSV_COMM
(
MTHEND,
PREFIX,
CODE,
AGT_NAME,
POLICY,
SUFFIX,
NAME,
TRAN_DT,
MKTCOMM,
TRAN,
PROD,
EFF_DT,
EXP_DT,
TERM,
STATE,
PCT,
COHORT_DT,
PREMIUM,
CMSN,
NETBAL,
PRCS_ID
)
SELECT
expr_prgrsv_trans.MTHEND as MTHEND,
expr_prgrsv_trans.PREFIX as PREFIX,
expr_prgrsv_trans.CODE as CODE,
expr_prgrsv_trans.AGT_NAME as AGT_NAME,
expr_prgrsv_trans.POLICY as POLICY,
expr_prgrsv_trans.SUFFIX as SUFFIX,
expr_prgrsv_trans.o_NAME as NAME,
expr_prgrsv_trans.o_TRAN_DT as TRAN_DT,
expr_prgrsv_trans.MKTCOMM as MKTCOMM,
expr_prgrsv_trans.TRAN as TRAN,
expr_prgrsv_trans.PROD as PROD,
expr_prgrsv_trans.o_EFF_DT as EFF_DT,
expr_prgrsv_trans.o_EXP_DT as EXP_DT,
expr_prgrsv_trans.TERM as TERM,
expr_prgrsv_trans.STATE as STATE,
expr_prgrsv_trans.o_PERCENT as PCT,
expr_prgrsv_trans.o_COHORT_DT as COHORT_DT,
expr_prgrsv_trans.o_PREMIUM as PREMIUM,
expr_prgrsv_trans.o_CMSN as CMSN,
expr_prgrsv_trans.o_NETBAL as NETBAL,
expr_prgrsv_trans.PRCS_ID as PRCS_ID
FROM
expr_prgrsv_trans;


END; ';