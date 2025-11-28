-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_MANUAL_ENTRIES_LOSSES("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_Shortcut_to_Alf_Losses_Not_In_Interface_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_Shortcut_to_Alf_Losses_Not_In_Interface
(
Unit varchar(5),
Account decimal,
Source varchar(3),
Reins_Cd varchar(1),
Sum_Amount decimal,
Period decimal,
Year decimal,
DeptID decimal,
Product varchar(6),
State varchar(2),
Pool varchar(5),
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_Shortcut_to_Alf_Losses_Not_In_Interface_SRC, Type IMPORT_DATA Importing Data
;


-- Component exp_MOVE_DATA, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_MOVE_DATA AS
(
SELECT
SQ_Shortcut_to_Alf_Losses_Not_In_Interface.Unit as Unit,
SQ_Shortcut_to_Alf_Losses_Not_In_Interface.Account as Account,
SQ_Shortcut_to_Alf_Losses_Not_In_Interface.Source as Source,
SQ_Shortcut_to_Alf_Losses_Not_In_Interface.Reins_Cd as Reins_Cd,
SQ_Shortcut_to_Alf_Losses_Not_In_Interface.Sum_Amount as Sum_Amount,
SQ_Shortcut_to_Alf_Losses_Not_In_Interface.Period as Period,
SQ_Shortcut_to_Alf_Losses_Not_In_Interface.Year as Year,
SQ_Shortcut_to_Alf_Losses_Not_In_Interface.DeptID as DeptID,
SQ_Shortcut_to_Alf_Losses_Not_In_Interface.Product as Product,
SQ_Shortcut_to_Alf_Losses_Not_In_Interface.State as State,
SQ_Shortcut_to_Alf_Losses_Not_In_Interface.Pool as Pool,
SQ_Shortcut_to_Alf_Losses_Not_In_Interface.source_record_id
FROM
SQ_Shortcut_to_Alf_Losses_Not_In_Interface
);


-- Component Shortcut_to_STG_FIN_MANUAL_ENTRIES_LOSS_TXN, Type TARGET 
INSERT INTO DB_T_STAG_FIN_PROD.STG_FIN_MANUAL_ENTRIES_LOSS_TXN
(
BUSINESS_UNIT,
ACCOUNT1,
REINSURANCE_CODE,
MONTH_ID,
MONETARY_AMOUNT,
SOURCE1,
YEAR1,
DEPT_ID,
PRODUCT,
STATE,
POOL
)
SELECT
exp_MOVE_DATA.Unit as BUSINESS_UNIT,
exp_MOVE_DATA.Account as ACCOUNT1,
exp_MOVE_DATA.Reins_Cd as REINSURANCE_CODE,
exp_MOVE_DATA.Period as MONTH_ID,
exp_MOVE_DATA.Sum_Amount as MONETARY_AMOUNT,
exp_MOVE_DATA.Source as SOURCE1,
exp_MOVE_DATA.Year as YEAR1,
exp_MOVE_DATA.DeptID as DEPT_ID,
exp_MOVE_DATA.Product as PRODUCT,
exp_MOVE_DATA.State as STATE,
exp_MOVE_DATA.Pool as POOL
FROM
exp_MOVE_DATA;


END; ';