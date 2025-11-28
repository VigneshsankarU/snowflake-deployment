-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_RUN_CODE_VALID_ERR_CHK("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component sq_ECTL_ERR_LOG, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_ECTL_ERR_LOG AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ERR_DESC,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 	DISTINCT ERR_DESC FROM 	DB_T_CTRL_FIN_PROD.ECTL_ERR_LOG 

WHERE 	ERR_DESC IN (''Invalid LOB Code for this product'',''Invalid Account Number'',''Invalid LOB Code for this DB_T_CORE_DM_PROD.Coverage Type'') 

AND ECTL_BATCH_ID IN (SELECT ECTL_BATCH_ID FROM DB_T_CTRL_FIN_PROD.ECTL_BATCH_INFO WHERE ECTL_PROJ_ID = 1 AND ECTL_ACTIVE_IND = ''Y'')
) SRC
)
);


-- Component err_Abort, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE err_Abort AS
(
SELECT
sq_ECTL_ERR_LOG.ERR_DESC as ERR_DESC,
CASE WHEN sq_ECTL_ERR_LOG.ERR_DESC IS NOT NULL or sq_ECTL_ERR_LOG.ERR_DESC <> '''' THEN ''f'' ELSE ''s'' END as o_ERR_DESC,
CASE WHEN sq_ECTL_ERR_LOG.ERR_DESC IS NOT NULL or sq_ECTL_ERR_LOG.ERR_DESC <> '''' THEN 1/0
--RAISE_ERROR(''Validation Issues'') 
END as o_Abort,
sq_ECTL_ERR_LOG.source_record_id
FROM
sq_ECTL_ERR_LOG
);


-- Component fil_Succeed_Records, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_Succeed_Records AS
(
SELECT
err_Abort.ERR_DESC as ERR_DESC,
err_Abort.o_ERR_DESC as o_ERR_DESC,
err_Abort.o_Abort as o_Abort,
err_Abort.source_record_id
FROM
err_Abort
WHERE err_Abort.o_ERR_DESC = ''s''
);


-- Component exp_Pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_Pass_through AS
(
SELECT
fil_Succeed_Records.ERR_DESC as ERR_DESC,
fil_Succeed_Records.source_record_id
FROM
fil_Succeed_Records
);


-- Component Shortcut_to_ECTL_ERR_LOG, Type TARGET 
INSERT INTO DB_T_CTRL_FIN_PROD.ECTL_ERR_LOG
(
ERR_DESC
)
SELECT
exp_Pass_through.ERR_DESC as ERR_DESC
FROM
exp_Pass_through;


END; ';