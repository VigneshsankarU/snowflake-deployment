-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_DUMMY_LOAD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);
 

-- Component SQ_PREMIUM, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_PREMIUM AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as AGENT_NBR,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT PREMIUM.AGENT_NBR 

FROM

 db_t_prod_stag.PREMIUM

WHERE 1 = 2
) SRC
)
);


-- Component EXP_PASS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_PASS AS
(
SELECT
SQ_PREMIUM.AGENT_NBR as AGENT_NBR,
SQ_PREMIUM.source_record_id
FROM
SQ_PREMIUM
);


-- Component PREMIUM_TGT, Type TARGET 
INSERT INTO db_t_prod_comn.PREMIUM
(
AGENT_NBR
)
SELECT
EXP_PASS.AGENT_NBR as AGENT_NBR
FROM
EXP_PASS;


END; ';