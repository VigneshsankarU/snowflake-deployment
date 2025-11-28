-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_PARAM_TRG_SHRD_HIER_ACTUARIAL("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_TRG_MASTER_HIER_ACTUARIAL_SRC, Type TABLE_DDL Creating an empty table
CREATE OR REPLACE TEMPORARY TABLE SQ_TRG_MASTER_HIER_ACTUARIAL
(
FEED_IND varchar(1),
FEED_DATE varchar(10),
source_record_id number autoincrement start 1 increment 1
);


-- Component SQ_TRG_MASTER_HIER_ACTUARIAL_SRC, Type IMPORT_DATA Importing Data
;


-- Component exp_assign_Param_val, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_assign_Param_val AS
(
SELECT
''[global]'' as out_global,
''$PMMergeSessParamFile=TRUE'' as out_sess_mrg,
''$FEED_IND='' || chr ( 39 ) || LTRIM ( RTRIM ( SQ_TRG_MASTER_HIER_ACTUARIAL.FEED_IND ) ) || chr ( 39 ) as out_FEED_IND,
''$FEED_DATE='' || chr ( 39 ) || LTRIM ( RTRIM ( SQ_TRG_MASTER_HIER_ACTUARIAL.FEED_DATE ) ) || chr ( 39 ) as out_FEED_DATE,
SQ_TRG_MASTER_HIER_ACTUARIAL.source_record_id
FROM
SQ_TRG_MASTER_HIER_ACTUARIAL
);


-- Component nrm_Param_Val, Type NORMALIZER 
CREATE OR REPLACE TEMPORARY TABLE nrm_Param_Val AS
(
SELECT  * FROM
( /* start of inner SQL */
SELECT
exp_assign_Param_val.out_global as param_item_in1,
exp_assign_Param_val.out_sess_mrg as param_item_in2,
exp_assign_Param_val.out_FEED_IND as param_item_in3,
exp_assign_Param_val.out_FEED_DATE as param_item_in4,
exp_assign_Param_val.source_record_id,
null param_item
FROM
exp_assign_Param_val
/* end of inner SQL */
)
--null param_item--UNPIVOT(param_item) FOR REC_NO IN (param_item_in1 AS REC1, param_item_in2 AS REC2, param_item_in3 AS REC3, param_item_in4 AS REC4) UNPIVOT_TBL
);


-- Component PROGRAM_NAMES_HIER, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE PROGRAM_NAMES_HIER AS
(
SELECT
nrm_Param_Val.param_item as ParameterLine
FROM
nrm_Param_Val
);


-- Component PROGRAM_NAMES_HIER, Type EXPORT_DATA Exporting data
;


END; ';