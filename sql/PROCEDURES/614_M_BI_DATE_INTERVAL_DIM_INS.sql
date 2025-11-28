-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BI_DATE_INTERVAL_DIM_INS("WORKLET_NAME" VARCHAR)
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
 

-- Component SQ_cat_clm_ctstrph_sumry_f, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_cat_clm_ctstrph_sumry_f AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as snpshot_intrval_dttm,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select snpshot_intrval_dttm from

(SELECT

max(cat_clm_ctstrph_sumry_f.snpshot_intrval_dttm) as  snpshot_intrval_dttm

FROM  db_t_prod_stag.cat_clm_ctstrph_sumry_f  ) temp  where  snpshot_intrval_dttm is not null
) SRC
)
);


-- Component exp_pass_to_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt AS
(
SELECT
SQ_cat_clm_ctstrph_sumry_f.snpshot_intrval_dttm as snpshot_intrval_dttm,
CASE WHEN DATE_PART(hour, TO_TIMESTAMP(SQ_cat_clm_ctstrph_sumry_f.snpshot_intrval_dttm)) <= 3 THEN '' Late Night '' ELSE CASE WHEN DATE_PART(hour, TO_TIMESTAMP(SQ_cat_clm_ctstrph_sumry_f.snpshot_intrval_dttm)) <= 6 THEN '' Twilight '' ELSE CASE WHEN DATE_PART(hour, TO_TIMESTAMP(SQ_cat_clm_ctstrph_sumry_f.snpshot_intrval_dttm)) <= 8 THEN ''Early Morning'' ELSE CASE WHEN DATE_PART(hour, TO_TIMESTAMP(SQ_cat_clm_ctstrph_sumry_f.snpshot_intrval_dttm)) <= 11 THEN ''Morning'' ELSE CASE WHEN DATE_PART(hour, TO_TIMESTAMP(SQ_cat_clm_ctstrph_sumry_f.snpshot_intrval_dttm)) <= 14 THEN ''Early Afternoon'' ELSE CASE WHEN DATE_PART(hour, TO_TIMESTAMP(SQ_cat_clm_ctstrph_sumry_f.snpshot_intrval_dttm)) <= 18 THEN ''Afternoon'' ELSE CASE WHEN DATE_PART(hour, TO_TIMESTAMP(SQ_cat_clm_ctstrph_sumry_f.snpshot_intrval_dttm)) <= 21 THEN ''Evening'' ELSE ''Night'' END END END END END END END as o_datePart,
DATE_PART(''yyyy'', TO_TIMESTAMP(SQ_cat_clm_ctstrph_sumry_f.snpshot_intrval_dttm)) as o_year_num,
DATE_PART(''mm'', TO_TIMESTAMP(SQ_cat_clm_ctstrph_sumry_f.snpshot_intrval_dttm)) as o_month_num,
DATE_PART(''DD'', TO_TIMESTAMP(SQ_cat_clm_ctstrph_sumry_f.snpshot_intrval_dttm)) as o_day_num,
SQ_cat_clm_ctstrph_sumry_f.source_record_id
FROM
SQ_cat_clm_ctstrph_sumry_f
);


-- Component LKP_CAT_DT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CAT_DT AS
(
SELECT
LKP.DT_ID,
exp_pass_to_tgt.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_to_tgt.source_record_id ORDER BY LKP.DT_ID asc,LKP.CAL_DT asc,LKP.WKEND_IND asc,LKP.MTH_NAME asc,LKP.MTH_NUM asc,LKP.YR_NUM asc,LKP.QTR_NUM asc,LKP.QTR_NAME asc,LKP.DOW_NUM asc,LKP.DOW_NAME asc,LKP.BUSN_DY_IND asc,LKP.DY_OF_YR_NUM asc,LKP.DOM_NUM asc,LKP.WKDAY_OF_MTH_NUM asc,LKP.WK_OF_MTH_NUM asc,LKP.WK_OF_YR_NUM asc,LKP.MTH_OF_QTR_NUM asc) RNK
FROM
exp_pass_to_tgt
LEFT JOIN (
SELECT
DT_ID,
CAL_DT,
WKEND_IND,
MTH_NAME,
MTH_NUM,
YR_NUM,
QTR_NUM,
QTR_NAME,
DOW_NUM,
DOW_NAME,
BUSN_DY_IND,
DY_OF_YR_NUM,
DOM_NUM,
WKDAY_OF_MTH_NUM,
WK_OF_MTH_NUM,
WK_OF_YR_NUM,
MTH_OF_QTR_NUM
FROM ZD_CAL_DT
) LKP ON LKP.YR_NUM = exp_pass_to_tgt.o_year_num AND LKP.MTH_NUM = exp_pass_to_tgt.o_month_num AND LKP.DOM_NUM = exp_pass_to_tgt.o_day_num
QUALIFY RNK = 1
);


-- Component exp_target, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_target AS
(
SELECT
exp_pass_to_tgt.snpshot_intrval_dttm as snpshot_intrval_dttm,
exp_pass_to_tgt.o_datePart as o_datePart,
CASE WHEN LKP_CAT_DT.DT_ID IS NULL THEN 1 ELSE LKP_CAT_DT.DT_ID END as o_DT,
exp_pass_to_tgt.source_record_id
FROM
exp_pass_to_tgt
INNER JOIN LKP_CAT_DT ON exp_pass_to_tgt.source_record_id = LKP_CAT_DT.source_record_id
);


-- Component DTTM_INTRVAL_D, Type TARGET 
INSERT INTO db_t_prod_base.DTTM_INTRVAL_D
(
DTTM_INTRVAL_DTTM,
DAYPT_CD,
DAYPT_DESC,
DT_ID
)
SELECT
exp_target.snpshot_intrval_dttm as DTTM_INTRVAL_DTTM,
exp_target.o_datePart as DAYPT_CD,
exp_target.o_datePart as DAYPT_DESC,
exp_target.o_DT as DT_ID
FROM
exp_target;


END; ';