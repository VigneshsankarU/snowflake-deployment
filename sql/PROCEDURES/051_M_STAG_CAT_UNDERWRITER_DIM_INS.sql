-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_CAT_UNDERWRITER_DIM_INS("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component src_sq_cat_undrwrtr_d, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE src_sq_cat_undrwrtr_d AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as undrwrtr_id,
$2 as undrwrtr_cd,
$3 as undrwrtr_co_name,
$4 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
cat_undrwrtr_d.undrwrtr_id,
cat_undrwrtr_d.undrwrtr_cd,
cat_undrwrtr_d.undrwrtr_co_name
FROM DB_T_PROD_STAG.cat_undrwrtr_d
) SRC
)
);


-- Component exp_src_tgt_pass, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_src_tgt_pass AS
(
SELECT
src_sq_cat_undrwrtr_d.undrwrtr_id as undrwrtr_id,
src_sq_cat_undrwrtr_d.undrwrtr_cd as undrwrtr_cd,
src_sq_cat_undrwrtr_d.undrwrtr_co_name as undrwrtr_co_name,
CURRENT_TIMESTAMP as out_SYSDATE,
src_sq_cat_undrwrtr_d.source_record_id
FROM
src_sq_cat_undrwrtr_d
);


-- Component tgt_cat_undrwrtr_d, Type TARGET 
INSERT INTO DB_T_PROD_STAG.cat_undrwrtr_d
(
undrwrtr_id,
undrwrtr_cd,
undrwrtr_co_name,
LOAD_DTTM
)
SELECT
exp_src_tgt_pass.undrwrtr_id as undrwrtr_id,
exp_src_tgt_pass.undrwrtr_cd as undrwrtr_cd,
exp_src_tgt_pass.undrwrtr_co_name as undrwrtr_co_name,
exp_src_tgt_pass.out_SYSDATE as LOAD_DTTM
FROM
exp_src_tgt_pass;


END; ';