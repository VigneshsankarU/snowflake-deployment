-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_LKP_CONTROL_TABLES("IN_INPUT_MAPLET" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    NO_ACTIVE_BATCH_EXCEPTION EXCEPTION (-20002, ''There is no active batch available in ECTL_BATCH_INFO table'');
BEGIN 


-- Component exp_Maplet_Input, Type EXPRESSION 
EXECUTE IMMEDIATE ''
CREATE OR REPLACE TEMPORARY TABLE exp_Maplet_Input AS
(
SELECT
input_maplet.PROJECT_ID as PROJECT_ID,
input_maplet.PROGRAM_NM as PROGRAM_NM,
input_maplet.BATCH_ACTIVE_IND as BATCH_ACTIVE_IND,
input_maplet.TABLE_NAME as TABLE_NAME,
input_maplet.source_record_id
FROM
'' || in_input_maplet || '' input_maplet
);'';


-- Component LKP_TABLE_NAME, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TABLE_NAME AS
(
SELECT
LKP.ECTL_TABLE_ID,
exp_Maplet_Input.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_Maplet_Input.source_record_id ORDER BY LKP.ECTL_TABLE_ID asc,LKP.ECTL_TABLE_NM asc,LKP.ECTL_ORIG_INS_TS asc,LKP.ECTL_DATABASE_NM asc) RNK
FROM
exp_Maplet_Input
LEFT JOIN (
SELECT
ECTL_TABLE_ID,
ECTL_TABLE_NM,
ECTL_ORIG_INS_TS,
ECTL_DATABASE_NM
FROM db_t_ctrl_prod.ECTL_TABLE_XREF
) LKP ON LKP.ECTL_TABLE_NM = exp_Maplet_Input.TABLE_NAME
QUALIFY RNK = 1
);


-- Component LKP_ECTL_PRGM_INFO, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_ECTL_PRGM_INFO AS
(
SELECT
LKP.ECTL_PRGM_ID,
exp_Maplet_Input.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_Maplet_Input.source_record_id ORDER BY LKP.ECTL_PRGM_ID asc) RNK
FROM
exp_Maplet_Input
LEFT JOIN (
SELECT ECTL_PRGM_INFO.ECTL_PRGM_ID as ECTL_PRGM_ID, ECTL_PRGM_INFO.ECTL_PROJ_ID as ECTL_PROJ_ID, ECTL_PRGM_INFO.ECTL_PRGM_NM as ECTL_PRGM_NM 
FROM db_t_ctrl_prod.ECTL_PRGM_INFO
WHERE ECTL_ACTIVE_IND = ''Y''
) LKP ON LKP.ECTL_PROJ_ID = exp_Maplet_Input.PROJECT_ID AND LKP.ECTL_PRGM_NM = exp_Maplet_Input.PROGRAM_NM
QUALIFY RNK = 1
);


-- Component LKP_ECTL_BATCH_INFO, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_ECTL_BATCH_INFO AS
(
SELECT
LKP.ECTL_BATCH_ID,
exp_Maplet_Input.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_Maplet_Input.source_record_id ORDER BY LKP.ECTL_BATCH_ID asc) RNK
FROM
exp_Maplet_Input
LEFT JOIN (
SELECT
ECTL_BATCH_ID,
ECTL_PROJ_ID,
ECTL_ACTIVE_IND
FROM db_t_ctrl_prod.ECTL_BATCH_INFO
) LKP ON LKP.ECTL_PROJ_ID = exp_Maplet_Input.PROJECT_ID AND LKP.ECTL_ACTIVE_IND = TO_CHAR(exp_Maplet_Input.BATCH_ACTIVE_IND)
QUALIFY RNK = 1
);


-- Component exp_check_batch, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_check_batch AS
(
SELECT
LKP_ECTL_BATCH_INFO.ECTL_BATCH_ID as ECTL_BATCH_ID,
-- CASE WHEN LKP_ECTL_BATCH_INFO.ECTL_BATCH_ID IS NULL THEN RAISE_ERROR(''There is no active batch available in ECTL_BATCH_INFO table'') ELSE $3 END as var_check_batch,
COALESCE(LKP_ECTL_BATCH_INFO.ECTL_BATCH_ID, 1) as var_check_batch,
LKP_ECTL_BATCH_INFO.source_record_id
FROM
LKP_ECTL_BATCH_INFO
);

-- IF ((SELECT COUNT(*) FROM exp_check_batch WHERE var_check_batch = 1) > 0) then
--     RAISE NO_ACTIVE_BATCH_EXCEPTION;
-- end if;


-- Component output_maplet, Type OUTPUT_TRANSFORMATION 
-- Component output_maplet, Type MAPPLET 
CREATE OR REPLACE TEMPORARY TABLE m_lkp_Control_Tables AS
    (
    SELECT
    exp_check_batch.ECTL_BATCH_ID as mplt_BATCH_ID,
    LKP_ECTL_PRGM_INFO.ECTL_PRGM_ID as mplt_PRGM_ID,
    LKP_TABLE_NAME.ECTL_TABLE_ID as mplt_TABLE_ID,
    exp_Maplet_Input.PROJECT_ID as mplt_PROJ_ID,
    exp_Maplet_Input.source_record_id
    FROM
    exp_Maplet_Input
    LEFT JOIN LKP_TABLE_NAME ON exp_Maplet_Input.source_record_id = LKP_TABLE_NAME.source_record_id
    LEFT JOIN LKP_ECTL_PRGM_INFO ON LKP_TABLE_NAME.source_record_id = LKP_ECTL_PRGM_INFO.source_record_id
    LEFT JOIN exp_check_batch ON LKP_ECTL_PRGM_INFO.source_record_id = exp_check_batch.source_record_id
    );

END; ';