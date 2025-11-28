-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_LIFE_CURR_TIER("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
 DECLARE
  PRCS_ID int;
  run_id STRING;

BEGIN
run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);


-- Component SQ_life_curr_tier, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_life_curr_tier AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as AGENT_NBR,
$2 as ORDER_NBR,
$3 as CURR_YR,
$4 as CURR_MTH,
$5 as MONTH_LIT,
$6 as AMOUNT,
$7 as AS_OF_DT,
$8 as TIER_CD,
$9 as PRTCPTNG_IND,
$10 as VLDTNG_IND,
$11 as FORCED_IND,
$12 as ACTL_TIER_CD,
$13 as LIFE_CURR_TIER_PCT,
$14 as ORGNL_AMT,
$15 as ADJSTMNT_IND,
$16 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
life_curr_tier.AGENT_NBR,
life_curr_tier.ORDER_NBR,
life_curr_tier.CURR_YR,
life_curr_tier.CURR_MTH,
life_curr_tier.MONTH_LIT,
life_curr_tier.AMOUNT,
life_curr_tier.AS_OF_DT,
life_curr_tier.TIER_CD,
life_curr_tier.PRTCPTNG_IND,
life_curr_tier.VLDTNG_IND,
life_curr_tier.FORCED_IND,
life_curr_tier.ACTL_TIER_CD,
life_curr_tier.LIFE_CURR_TIER_PCT,
life_curr_tier.ORGNL_AMT,
life_curr_tier.ADJSTMNT_IND
FROM db_t_prod_stag.life_curr_tier
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_life_curr_tier.AGENT_NBR as AGENT_NBR,
SQ_life_curr_tier.ORDER_NBR as ORDER_NBR,
SQ_life_curr_tier.CURR_YR as CURR_YR,
SQ_life_curr_tier.CURR_MTH as CURR_MTH,
SQ_life_curr_tier.MONTH_LIT as MONTH_LIT,
SQ_life_curr_tier.AMOUNT as AMOUNT,
SQ_life_curr_tier.AS_OF_DT as AS_OF_DT,
SQ_life_curr_tier.TIER_CD as TIER_CD,
SQ_life_curr_tier.PRTCPTNG_IND as PRTCPTNG_IND,
SQ_life_curr_tier.VLDTNG_IND as VLDTNG_IND,
SQ_life_curr_tier.FORCED_IND as FORCED_IND,
SQ_life_curr_tier.ACTL_TIER_CD as ACTL_TIER_CD,
SQ_life_curr_tier.LIFE_CURR_TIER_PCT as LIFE_CURR_TIER_PCT,
SQ_life_curr_tier.ORGNL_AMT as ORGNL_AMT,
SQ_life_curr_tier.ADJSTMNT_IND as ADJSTMNT_IND,
:PRCS_ID as PRCS_ID,
SQ_life_curr_tier.source_record_id
FROM
SQ_life_curr_tier
);


-- Component life_curr_tier1, Type TARGET 
INSERT INTO db_t_prod_comn.life_curr_tier
(
AGENT_NBR,
ORDER_NBR,
CURR_YR,
CURR_MTH,
MONTH_LIT,
AMOUNT,
AS_OF_DT,
TIER_CD,
PRTCPTNG_IND,
VLDTNG_IND,
FORCED_IND,
ACTL_TIER_CD,
LIFE_CURR_TIER_PCT,
ORGNL_AMT,
ADJSTMNT_IND,
PRCS_ID
)
SELECT
exp_pass_through.AGENT_NBR as AGENT_NBR,
exp_pass_through.ORDER_NBR as ORDER_NBR,
exp_pass_through.CURR_YR as CURR_YR,
exp_pass_through.CURR_MTH as CURR_MTH,
exp_pass_through.MONTH_LIT as MONTH_LIT,
exp_pass_through.AMOUNT as AMOUNT,
exp_pass_through.AS_OF_DT as AS_OF_DT,
exp_pass_through.TIER_CD as TIER_CD,
exp_pass_through.PRTCPTNG_IND as PRTCPTNG_IND,
exp_pass_through.VLDTNG_IND as VLDTNG_IND,
exp_pass_through.FORCED_IND as FORCED_IND,
exp_pass_through.ACTL_TIER_CD as ACTL_TIER_CD,
exp_pass_through.LIFE_CURR_TIER_PCT as LIFE_CURR_TIER_PCT,
exp_pass_through.ORGNL_AMT as ORGNL_AMT,
exp_pass_through.ADJSTMNT_IND as ADJSTMNT_IND,
exp_pass_through.PRCS_ID as PRCS_ID
FROM
exp_pass_through;


END; ';