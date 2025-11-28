-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_PARAM_GENERATE_SP_DATES("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_GW_CLOSEOUT_CTL, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_GW_CLOSEOUT_CTL AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as BEGINNING_TS,
$2 as ENDING_TS,
$3 as EOM_DT,
$4 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT Min(ending_ts) AS start_dt,Max(ending_ts) AS end_dt,Max(eom_dt) AS eom_dt FROM

(

SELECT Cast(ending_ts AS DATE) AS ending_ts,eom_dt

FROM DB_T_PROD_COMN.GW_CLOSEOUT_CTL a

WHERE closeOUT_type=''P''

QUALIFY Row_Number() Over

(ORDER BY ACCOUNTING_YR DESC,ACCOUNTING_MO DESC) IN(1,2)

)ab
) SRC
)
);


-- Component exp_passthrough, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_passthrough AS
(
SELECT
TO_CHAR ( SQ_GW_CLOSEOUT_CTL.BEGINNING_TS , ''YYYY-MM-DD'' ) as v_BEGINNING_TS,
TO_CHAR ( SQ_GW_CLOSEOUT_CTL.ENDING_TS , ''YYYY-MM-DD'' ) as v_ENDING_TS,
TO_CHAR ( SQ_GW_CLOSEOUT_CTL.EOM_DT , ''YYYY-MM-DD'' ) as v_EOM_DT,
''[global]'' as out_global,
''$PMMergeSessParamFile=TRUE'' as out_sessionmerge,
''$GL_MTH_START_DT='' || LTRIM ( RTRIM ( v_BEGINNING_TS ) ) as out_BEGINNING_TS,
''$GL_MTH_END_DT='' || LTRIM ( RTRIM ( v_ENDING_TS ) ) as out_ENDING_TS,
''$GL_MTH_EOM_DT='' || LTRIM ( RTRIM ( v_EOM_DT ) ) as out_EOM_DT,
SQ_GW_CLOSEOUT_CTL.source_record_id
FROM
SQ_GW_CLOSEOUT_CTL
);


-- Component NRMTRANS1, Type NORMALIZER 
CREATE OR REPLACE TEMPORARY TABLE NRMTRANS1 AS
(
SELECT ''REC1'' AS REC_NO, param_item_in1 AS param_item, source_record_id FROM exp_passthrough
UNION ALL
SELECT ''REC2'', param_item_in2, source_record_id FROM exp_passthrough
UNION ALL
SELECT ''REC3'', param_item_in3, source_record_id FROM exp_passthrough
UNION ALL
SELECT ''REC4'', param_item_in4, source_record_id FROM exp_passthrough
UNION ALL
SELECT ''REC5'', param_item_in5, source_record_id FROM exp_passthrough
);



-- Component edw_param_sp_ar_monthly, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE edw_param_sp_ar_monthly AS
(
SELECT
NRMTRANS1.param_item as Param_line
FROM
NRMTRANS1
);


-- Component edw_param_sp_ar_monthly, Type EXPORT_DATA Exporting data
;


END; ';