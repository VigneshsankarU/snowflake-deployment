-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BI_RISK_LOSS_LOCATION_DIM_INS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_cat_risk_loss_loc_d, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_cat_risk_loss_loc_d AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as risk_loss_loc_id,
$2 as risk_st_cd,
$3 as risk_cnty_name,
$4 as loss_st_cd,
$5 as loss_cnty_name,
$6 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
cat_risk_loss_loc_d.risk_loss_loc_id,
cat_risk_loss_loc_d.risk_st_cd,
cat_risk_loss_loc_d.risk_cnty_name,
cat_risk_loss_loc_d.loss_st_cd,
cat_risk_loss_loc_d.loss_cnty_name
FROM DB_T_PROD_STAG.cat_risk_loss_loc_d
) SRC
)
);


-- Component exp_pass_to_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt AS
(
SELECT
SQ_cat_risk_loss_loc_d.risk_loss_loc_id as risk_loss_loc_id,
SQ_cat_risk_loss_loc_d.risk_st_cd as risk_st_cd,
SQ_cat_risk_loss_loc_d.risk_cnty_name as risk_cnty_name,
SQ_cat_risk_loss_loc_d.loss_st_cd as loss_st_cd,
SQ_cat_risk_loss_loc_d.loss_cnty_name as loss_cnty_name,
SQ_cat_risk_loss_loc_d.source_record_id
FROM
SQ_cat_risk_loss_loc_d
);


-- Component RISK_LOSS_LOC_D, Type TARGET 
INSERT INTO DB_V_PROD_BASE.RISK_LOSS_LOC_D
(
RISK_LOSS_LOC_ID,
RISK_ST_CD,
RISK_CNTY_NAME,
LOSS_ST_CD,
LOSS_CNTY_NAME
)
SELECT
exp_pass_to_tgt.risk_loss_loc_id as RISK_LOSS_LOC_ID,
exp_pass_to_tgt.risk_st_cd as RISK_ST_CD,
exp_pass_to_tgt.risk_cnty_name as RISK_CNTY_NAME,
exp_pass_to_tgt.loss_st_cd as LOSS_ST_CD,
exp_pass_to_tgt.loss_cnty_name as LOSS_CNTY_NAME
FROM
exp_pass_to_tgt;


END; ';