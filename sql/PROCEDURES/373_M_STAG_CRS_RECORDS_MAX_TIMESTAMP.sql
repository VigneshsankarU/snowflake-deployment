-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_STAG_CRS_RECORDS_MAX_TIMESTAMP("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_CRS_Records_Max_Timestamp, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_CRS_Records_Max_Timestamp AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as UPDTTS,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT (case when MAX(UPDTTS) is null then try_TO_TIMESTAMP(''1900-01-01 00:00:00'', ''MM/DD/YYYY HH24:MI:SS.FF6'') else MAX(UPDTTS) end) - interval ''5 minute'' AS UpdateTS 

FROM DB_T_PROD_COMN.cust_rvw_shet_rec
) SRC
)
);


-- Component asgn_variable, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE asgn_variable AS
(
SELECT
''[global]'' || CHR ( 10 ) || '':PMMergeSessParamFile=TRUE'' || CHR ( 10 ) || '' * * Parameters passed to PORTAL_REGISTERED_POLS'' || CHR(10) as header,
'':MAX_UPDATE_TS = '' || CHR ( 39 ) || SQ_CRS_Records_Max_Timestamp.UPDTTS || CHR ( 39 ) as out_UpdateTS,
SQ_CRS_Records_Max_Timestamp.source_record_id
FROM
SQ_CRS_Records_Max_Timestamp
);


-- Component norm_param_file, Type NORMALIZER 
CREATE OR REPLACE TEMPORARY TABLE norm_param_file AS
SELECT 
  source_record_id,
  REC_NO,
  update_ts
FROM (
  SELECT
    header AS update_ts_in1,
    out_UpdateTS AS update_ts_in2,
    source_record_id
  FROM asgn_variable
)
UNPIVOT (
  update_ts FOR REC_NO IN (update_ts_in1, update_ts_in2)
);


-- Component CRS_Records_Max_Timestamp1, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE CRS_Records_Max_Timestamp1 AS
(
SELECT
norm_param_file.update_ts as UpdateTS
FROM
norm_param_file
);


-- Component CRS_Records_Max_Timestamp1, Type EXPORT_DATA Exporting data
;


END; ';