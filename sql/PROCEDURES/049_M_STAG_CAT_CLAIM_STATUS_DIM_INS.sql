-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_CAT_CLAIM_STATUS_DIM_INS("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component src_sq_cat_clm_sts_d, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE src_sq_cat_clm_sts_d AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as clm_sts_type_cd,
$2 as clm_sts_type_desc,
$3 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
cat_clm_sts_d.clm_sts_type_cd,
cat_clm_sts_d.clm_sts_type_desc
FROM DB_T_PROD_STAG.cat_clm_sts_d
) SRC
)
);


-- Component exp_src_tgt_pass, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_src_tgt_pass AS
(
SELECT
src_sq_cat_clm_sts_d.clm_sts_type_cd as clm_sts_type_cd,
src_sq_cat_clm_sts_d.clm_sts_type_desc as clm_sts_type_desc,
CURRENT_TIMESTAMP as out_SYSDATE,
src_sq_cat_clm_sts_d.source_record_id
FROM
src_sq_cat_clm_sts_d
);


-- Component tgt_cat_clm_sts_d, Type TARGET 
INSERT INTO DB_T_PROD_STAG.cat_clm_sts_d
(
clm_sts_type_cd,
clm_sts_type_desc,
LOAD_DTTM
)
SELECT
exp_src_tgt_pass.clm_sts_type_cd as clm_sts_type_cd,
exp_src_tgt_pass.clm_sts_type_desc as clm_sts_type_desc,
exp_src_tgt_pass.out_SYSDATE as LOAD_DTTM
FROM
exp_src_tgt_pass;


END; ';