-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_CAT_SERVICE_CENTER_DIM_INS("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component src_sq_cat_srvc_ctr_d, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE src_sq_cat_srvc_ctr_d AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as srvc_ctr_id,
$2 as srvc_ctr_num,
$3 as srvc_ctr_name,
$4 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
cat_srvc_ctr_d.srvc_ctr_id,
cat_srvc_ctr_d.srvc_ctr_num,
cat_srvc_ctr_d.srvc_ctr_name
FROM DB_T_PROD_STAG.cat_srvc_ctr_d
) SRC
)
);


-- Component exp_src_tgt_pass, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_src_tgt_pass AS
(
SELECT
src_sq_cat_srvc_ctr_d.srvc_ctr_id as srvc_ctr_id,
src_sq_cat_srvc_ctr_d.srvc_ctr_num as srvc_ctr_num,
src_sq_cat_srvc_ctr_d.srvc_ctr_name as srvc_ctr_name,
CURRENT_TIMESTAMP as out_SYSDATE,
src_sq_cat_srvc_ctr_d.source_record_id
FROM
src_sq_cat_srvc_ctr_d
);


-- Component tgt_cat_srvc_ctr_d, Type TARGET 
INSERT INTO DB_T_PROD_STAG.cat_srvc_ctr_d
(
srvc_ctr_id,
srvc_ctr_num,
srvc_ctr_name,
LOAD_DTTM
)
SELECT
exp_src_tgt_pass.srvc_ctr_id as srvc_ctr_id,
exp_src_tgt_pass.srvc_ctr_num as srvc_ctr_num,
exp_src_tgt_pass.srvc_ctr_name as srvc_ctr_name,
exp_src_tgt_pass.out_SYSDATE as LOAD_DTTM
FROM
exp_src_tgt_pass;


END; ';