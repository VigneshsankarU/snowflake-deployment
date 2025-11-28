-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_CAT_CATASTROPHE_DIM_INS("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component src_sq_cat_ctstrph_d, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE src_sq_cat_ctstrph_d AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ctstrph_id,
$2 as ctstrph_strt_dt,
$3 as ctstrph_end_dt,
$4 as ctstrph_name,
$5 as ctstrph_num,
$6 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
cat_ctstrph_d.ctstrph_id,
cat_ctstrph_d.ctstrph_strt_dt,
cat_ctstrph_d.ctstrph_end_dt,
cat_ctstrph_d.ctstrph_name,
cat_ctstrph_d.ctstrph_num
FROM DB_T_PROD_STAG.cat_ctstrph_d
) SRC
)
);


-- Component exp_src_tgt_pass, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_src_tgt_pass AS
(
SELECT
src_sq_cat_ctstrph_d.ctstrph_id as ctstrph_id,
src_sq_cat_ctstrph_d.ctstrph_strt_dt as ctstrph_strt_dt,
src_sq_cat_ctstrph_d.ctstrph_end_dt as ctstrph_end_dt,
src_sq_cat_ctstrph_d.ctstrph_name as ctstrph_name,
src_sq_cat_ctstrph_d.ctstrph_num as ctstrph_num,
CURRENT_TIMESTAMP as out_SYSDATE,
src_sq_cat_ctstrph_d.source_record_id
FROM
src_sq_cat_ctstrph_d
);


-- Component tgt_cat_ctstrph_d, Type TARGET 
INSERT INTO DB_T_PROD_STAG.cat_ctstrph_d
(
ctstrph_id,
ctstrph_strt_dt,
ctstrph_end_dt,
ctstrph_name,
ctstrph_num,
LOAD_DTTM
)
SELECT
exp_src_tgt_pass.ctstrph_id as ctstrph_id,
exp_src_tgt_pass.ctstrph_strt_dt as ctstrph_strt_dt,
exp_src_tgt_pass.ctstrph_end_dt as ctstrph_end_dt,
exp_src_tgt_pass.ctstrph_name as ctstrph_name,
exp_src_tgt_pass.ctstrph_num as ctstrph_num,
exp_src_tgt_pass.out_SYSDATE as LOAD_DTTM
FROM
exp_src_tgt_pass;


END; ';