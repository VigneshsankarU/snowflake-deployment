-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_SVC_EXPNS("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_Service_Center_Expenses_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_Service_Center_Expenses
(
Dept decimal,
Account decimal,
Descr varchar(250),
Sum_Total_Amt decimal,
State varchar(50),
Year varchar(5),
Period varchar(5),
Product varchar(6),
Unit varchar(50),
Project varchar(50),
Reins_Cd varchar(50),
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_Service_Center_Expenses_SRC, Type IMPORT_DATA Importing Data
;


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
SQ_Service_Center_Expenses.Dept as Dept,
SQ_Service_Center_Expenses.Account as Account,
SQ_Service_Center_Expenses.Descr as Descr,
SQ_Service_Center_Expenses.Sum_Total_Amt as Sum_Total_Amt,
SQ_Service_Center_Expenses.State as State,
SQ_Service_Center_Expenses.Year as Year,
SQ_Service_Center_Expenses.Period as Period,
SQ_Service_Center_Expenses.Product as Product,
SQ_Service_Center_Expenses.Unit as Unit,
SQ_Service_Center_Expenses.Project as Project,
SQ_Service_Center_Expenses.Reins_Cd as Reins_Cd,
$PRCS_ID as prcs_id,
SQ_Service_Center_Expenses.source_record_id
FROM
SQ_Service_Center_Expenses
);


-- Component SVC_EXPNS, Type TARGET 
INSERT INTO DB_T_PROD_COMN.SVC_EXPNS
(
DEPT,
ACCT,
ACCT_DESC,
AMT,
STATE,
YR,
MTH,
PROD_CD,
UW_CMPY,
PROJ,
REINS_CD,
PRCS_ID
)
SELECT
EXPTRANS.Dept as DEPT,
EXPTRANS.Account as ACCT,
EXPTRANS.Descr as ACCT_DESC,
EXPTRANS.Sum_Total_Amt as AMT,
EXPTRANS.State as STATE,
EXPTRANS.Year as YR,
EXPTRANS.Period as MTH,
EXPTRANS.Product as PROD_CD,
EXPTRANS.Unit as UW_CMPY,
EXPTRANS.Project as PROJ,
EXPTRANS.Reins_Cd as REINS_CD,
EXPTRANS.prcs_id as PRCS_ID
FROM
EXPTRANS;


END; ';