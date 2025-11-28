-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BONUS_LR_BI_FEED("WORKLET_NAME" VARCHAR)
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
 

-- Component SQ_ACCTBLY_BONUS_BI_FEED, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_ACCTBLY_BONUS_BI_FEED AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PAY_AGENT_NBR,
$2 as ACCOUNTING_DT,
$3 as YTD_LR,
$4 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 

ACCTBLY_BONUS_BI_FEED.PAY_AGENT_NBR, 

ACCTBLY_BONUS_BI_FEED.ACCOUNTING_DT, 

ACCTBLY_BONUS_BI_FEED.YTD_LR 

FROM

db_t_prod_comn.ACCTBLY_BONUS_BI_FEED ACCTBLY_BONUS_BI_FEED

WHERE ACCTBLY_BONUS_BI_FEED.ACCOUNTING_DT=LAST_DAY(ADD_MONTHS(CURRENT_DATE, -1))
) SRC
)
);


-- Component exp_gather, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_gather AS
(
SELECT
SQ_ACCTBLY_BONUS_BI_FEED.PAY_AGENT_NBR as PAY_AGENT_NBR,
SQ_ACCTBLY_BONUS_BI_FEED.ACCOUNTING_DT as ACCOUNTING_DT,
SQ_ACCTBLY_BONUS_BI_FEED.YTD_LR as YTD_LR,
SQ_ACCTBLY_BONUS_BI_FEED.source_record_id
FROM
SQ_ACCTBLY_BONUS_BI_FEED
);


-- Component BONUS_LR_BI_FEED, Type TARGET 
INSERT INTO BONUS_LR_BI_FEED
(
AGENT_NBR,
DATE_WRITTEN,
BONUS_LR
)
SELECT
exp_gather.PAY_AGENT_NBR as AGENT_NBR,
exp_gather.ACCOUNTING_DT as DATE_WRITTEN,
exp_gather.YTD_LR as BONUS_LR
FROM
exp_gather;


END; ';