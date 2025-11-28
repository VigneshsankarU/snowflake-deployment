-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_ALFA_LIFT_011_AIUA_CREATE_PARAM("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE CURRENT_DATE date;
BEGIN 

CURRENT_DATE:=(select current_date); 

-- Component SQ_GW_CLOSEOUT_CTL, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_GW_CLOSEOUT_CTL AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as pc_BOY,
$2 as pc_EOY,
$3 as cc_BOY,
$4 as cc_EOY,
$5 as cc_EOFQ,
$6 as pc_BOQ,
$7 as pc_EOQ,
$8 as cc_BOQ,
$9 as cc_EOQ,
$10 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select  
min(case when closeout_type=''P'' AND 
accounting_yr=EXTRACT(year FROM TO_DATE(current_date))-1 and 
accounting_mo = 1 then beginning_ts end)  as P_BOY,
MAX(case when closeout_type=''P'' AND  
accounting_yr=EXTRACT(year FROM TO_DATE(current_date))-1 and 
accounting_mo = 12 then ending_ts end) as P_EOY,

min(case when closeout_type=''C'' AND 
accounting_yr=EXTRACT(year FROM TO_DATE(current_date))-2 and 
accounting_mo = 12 then ending_ts end)
-- - 1 --interval ''1''  second  
 as C_BOY,
MAX(case when closeout_type=''C'' AND  accounting_yr=EXTRACT(year FROM TO_DATE(current_date))-1 and accounting_mo = 12 then ending_ts end) as C_EOY,
MAX(case when closeout_type=''C'' AND accounting_yr=case when EXTRACT(month FROM TO_DATE(current_date)) in (0,1,2,3) then  EXTRACT(year FROM TO_DATE(current_date))-1 ELSE EXTRACT(year FROM TO_DATE(current_date)) END and accounting_mo = (3) then ending_ts end) as C_EOfQ,
min(case when closeout_type=''P'' AND 
accounting_yr=case when EXTRACT(month FROM TO_DATE(current_date)) in (0,1,2,3) then  EXTRACT(year FROM TO_DATE(current_date))-1 ELSE EXTRACT(year FROM TO_DATE(current_date)) END
and 
accounting_mo = 1 then beginning_ts end)   as P_BOQ,
max(case when closeout_type=''P'' AND 
accounting_yr=case when EXTRACT(month FROM TO_DATE(current_date)) in (0,1,2,3) then  EXTRACT(year FROM TO_DATE(current_date))-1 ELSE EXTRACT(year FROM TO_DATE(current_date)) END
AND accounting_mo = (case when EXTRACT(month FROM TO_DATE(current_date)) in (0,1,2,3) then 12
when EXTRACT(month FROM TO_DATE(current_date)) in (4,5,6) then 3
when EXTRACT(month FROM TO_DATE(current_date)) in (7,8,9) then 6
when EXTRACT(month FROM TO_DATE(current_date)) in (10,11,12) then 9 end) then ending_ts end) as P_EOQ,

min(case when closeout_type=''C'' AND 
accounting_yr=case when EXTRACT(month FROM TO_DATE(current_date)) in (0,1,2,3) then  EXTRACT(year FROM TO_DATE(current_date))-2 ELSE EXTRACT(year FROM TO_DATE(current_date))-1 END and 
accounting_mo = 12 then ending_ts end)
-- +1 --+ interval ''1''  second 
as C_BOQ,
max(case when closeout_type=''C'' AND 
accounting_yr=case when EXTRACT(month FROM TO_DATE(current_date)) in (0,1,2,3) then  EXTRACT(year FROM TO_DATE(current_date))-1 ELSE EXTRACT(year FROM TO_DATE(current_date)) END
AND accounting_mo = (case when EXTRACT(month FROM TO_DATE(current_date)) in (0,1,2,3) then 12
when EXTRACT(month FROM TO_DATE(current_date)) in (4,5,6) then 3
when EXTRACT(month FROM TO_DATE(current_date)) in (7,8,9) then 6
when EXTRACT(month FROM TO_DATE(current_date)) in (10,11,12) then 9 end) then ending_ts end) as C_EOQ


from DB_T_PROD_COMN.gw_closeout_ctl
where closeout_type in (''P'', ''C'')

    and accounting_yr in (EXTRACT(year FROM TO_DATE(current_date)), EXTRACT(year FROM TO_DATE(current_date))-2, EXTRACT(year FROM TO_DATE(current_date))-1)
) SRC
)
);


-- Component exp_data, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data AS
(
SELECT
''[Global]'' as out_Global,
''$pc_BOY='' || CHR ( 39 ) || TO_CHAR ( SQ_GW_CLOSEOUT_CTL.pc_BOY , ''YYYY-MM-DD HH24:MI:SS.US'' ) || CHR ( 39 ) as o_pc_BOY,
''$pc_EOY='' || CHR ( 39 ) || TO_CHAR ( SQ_GW_CLOSEOUT_CTL.pc_EOY , ''YYYY-MM-DD HH24:MI:SS.US'' ) || CHR ( 39 ) as o_pc_EOY,
''$cc_BOY='' || CHR ( 39 ) || TO_CHAR ( SQ_GW_CLOSEOUT_CTL.cc_BOY , ''YYYY-MM-DD HH24:MI:SS.US'' ) || CHR ( 39 ) as o_cc_BOY,
''$cc_EOY='' || CHR ( 39 ) || TO_CHAR ( SQ_GW_CLOSEOUT_CTL.cc_EOY , ''YYYY-MM-DD HH24:MI:SS.US'' ) || CHR ( 39 ) as o_cc_EOY,
''$cc_EOFQ='' || CHR ( 39 ) || TO_CHAR ( SQ_GW_CLOSEOUT_CTL.cc_EOFQ , ''YYYY-MM-DD HH24:MI:SS.US'' ) || CHR ( 39 ) as o_cc_EOFQ,
''$pc_BOQ='' || CHR ( 39 ) || TO_CHAR ( SQ_GW_CLOSEOUT_CTL.pc_BOQ , ''YYYY-MM-DD HH24:MI:SS.US'' ) || CHR ( 39 ) as o_pc_BOQ,
''$pc_EOQ='' || CHR ( 39 ) || TO_CHAR ( SQ_GW_CLOSEOUT_CTL.pc_EOQ , ''YYYY-MM-DD HH24:MI:SS.US'' ) || CHR ( 39 ) as o_pc_EOQ,
''$cc_BOQ='' || CHR ( 39 ) || TO_CHAR ( SQ_GW_CLOSEOUT_CTL.cc_BOQ , ''YYYY-MM-DD HH24:MI:SS.US'' ) || CHR ( 39 ) as o_cc_BOQ,
''$cc_EOQ='' || CHR ( 39 ) || TO_CHAR ( SQ_GW_CLOSEOUT_CTL.cc_EOQ , ''YYYY-MM-DD HH24:MI:SS.US'' ) || CHR ( 39 ) as o_cc_EOQ,
TO_CHAR ( SQ_GW_CLOSEOUT_CTL.pc_EOY , ''YYYY'' ) as out_year,
out_year as var_year,
SQ_GW_CLOSEOUT_CTL.source_record_id
FROM
SQ_GW_CLOSEOUT_CTL
);


-- Component nrmzr_data, Type NORMALIZER 
CREATE OR REPLACE TEMPORARY TABLE nrmzr_data AS
(
SELECT  * FROM
( /* start of inner SQL */
SELECT
exp_data.out_Global as out_file_in1,
exp_data.o_pc_BOY as out_file_in2,
exp_data.o_pc_EOY as out_file_in3,
exp_data.o_cc_BOY as out_file_in4,
exp_data.o_cc_EOY as out_file_in5,
exp_data.o_cc_EOFQ as out_file_in6,
exp_data.o_pc_BOQ as out_file_in7,
exp_data.o_pc_EOQ as out_file_in8,
exp_data.o_cc_BOQ as out_file_in9,
exp_data.o_cc_EOQ as out_file_in10,
exp_data.source_record_id
FROM
exp_data
/* end of inner SQL */
)
--UNPIVOT(out_file) FOR REC_NO IN (out_file_in1 AS REC1, out_file_in2 AS REC2, out_file_in3 AS REC3, out_file_in4 AS REC4, out_file_in5 AS REC5, out_file_in6 AS REC6, out_file_in7 AS REC7, out_file_in8 AS REC8, out_file_in9 AS REC9, out_file_in10 AS REC10) UNPIVOT_TBL
);


-- Component EDW_AIUA_PARAM, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE EDW_AIUA_PARAM AS
(
SELECT *
--nrmzr_data.out_file as out_file
FROM
nrmzr_data
);

copy into @my_internal_stage/EDW_AIUA_PARAM from (select * from EDW_AIUA_PARAM)
header=true
overwrite=true;

-- Component EDW_AIUA_PARAM, Type EXPORT_DATA Exporting data
;


END; ';