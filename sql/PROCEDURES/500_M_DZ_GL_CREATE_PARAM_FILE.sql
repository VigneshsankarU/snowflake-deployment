-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_DZ_GL_CREATE_PARAM_FILE("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_ETL_PRCS_CTRL, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_ETL_PRCS_CTRL AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as gl_start_mth_id,
$2 as gl_end_mth_id,
$3 as gl_mth_start_dt,
$4 as gl_mth_end_dt,
$5 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
  YEAR(DATEADD(MONTH, 1, src_extract_dt)) * 100 + MONTH(DATEADD(MONTH, 1, src_extract_dt)) AS gl_start_mth_id,
  gl_start_mth_id AS gl_end_mth_id,
  DATE_FROM_PARTS(
    FLOOR(gl_start_mth_id / 100),
    gl_start_mth_id % 100,
    1
  ) AS gl_mth_start_dt,
  DATEADD(DAY, -1, DATEADD(MONTH, 1, gl_mth_start_dt)) AS gl_mth_end_dt
FROM
  DB_T_PROD_CORE.etl_prcs_ctrl where prcs_stag = ''BASE'' and prcs_nm = ''WF_BASE_EDW_GL_LOAD_COMPLETION_MTHLY'' 
and sub_prcs_nm = ''CMD_ARCHIVE_EDW_PARAM_DZ_GL_MTHLY'' and prcs_sts = ''SUCCEEDED''
) SRC
)
);


-- Component EXPTRANS1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS1 AS
(
SELECT
''[global]'' as out_global,
''$PMMergeSessParamFile=TRUE'' as out_sessionmerge,
''$GL_START_MTH_ID='' || LTRIM ( RTRIM ( TO_CHAR ( SQ_ETL_PRCS_CTRL.gl_start_mth_id ) ) ) as out_cal_start_mth_id,
''$GL_END_MTH_ID='' || LTRIM ( RTRIM ( TO_CHAR ( SQ_ETL_PRCS_CTRL.gl_end_mth_id ) ) ) as out_cal_end_mth_id,
''$GL_MTH_START_DT='' || TO_CHAR ( SQ_ETL_PRCS_CTRL.gl_mth_start_dt , ''YYYY-MM-DD'' ) as out_cal_start_dt,
''$GL_MTH_END_DT='' || TO_CHAR ( SQ_ETL_PRCS_CTRL.gl_mth_end_dt , ''YYYY-MM-DD'' ) as out_cal_end_dt,
SQ_ETL_PRCS_CTRL.gl_mth_end_dt as var_src_extract_dt,
SQ_ETL_PRCS_CTRL.source_record_id
FROM
SQ_ETL_PRCS_CTRL
);


-- Component NRMTRANS1, Type NORMALIZER 
CREATE
OR REPLACE TEMPORARY TABLE NRMTRANS1 AS
SELECT
  *
FROM
  (
    SELECT
      EXPTRANS1.out_global AS param_item_in1,
      EXPTRANS1.out_sessionmerge AS param_item_in2,
      EXPTRANS1.out_cal_start_mth_id AS param_item_in3,
      EXPTRANS1.out_cal_end_mth_id AS param_item_in4,
      EXPTRANS1.out_cal_start_dt AS param_item_in5,
      EXPTRANS1.out_cal_end_dt AS param_item_in6,
      EXPTRANS1.source_record_id
    FROM
      EXPTRANS1
  ) UNPIVOT (
    param_item FOR rec_no IN (
      param_item_in1,
      param_item_in2,
      param_item_in3,
      param_item_in4,
      param_item_in5,
      param_item_in6
    )
  );

-- Component tgt_edw_shared_append_file, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE tgt_edw_shared_append_file AS
(
SELECT
NRMTRANS1.param_item as Param_item
FROM
NRMTRANS1
);


-- Component tgt_edw_shared_append_file, Type EXPORT_DATA Exporting data
COPY INTO @my_internal_stage/my_export_folder/tgt_edw_shared_append_file_
FROM (SELECT * FROM tgt_edw_shared_append_file)
HEADER = TRUE
OVERWRITE = TRUE;


END; ';