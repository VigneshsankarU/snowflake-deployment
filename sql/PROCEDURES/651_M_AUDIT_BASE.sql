-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_AUDIT_BASE("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 

declare
run_id varchar;
start_dttm timestamp;
end_dttm timestamp;
prcs_id int;


BEGIN 

run_id :=   (SELECT run_id   FROM control_run_id where worklet_name=:worklet_name order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'');
START_DTTM:= (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'');
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'');


-- Component SQ_pc_policyterm_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_policyterm_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as LAYER,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT ''BASE'' AS LAYER
) SRC
)
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
''$PRCS_ID_1='' || :PRCS_ID as out_concat,
SQ_pc_policyterm_x.source_record_id
FROM
SQ_pc_policyterm_x
);


-- Component tg_def, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE tg_def AS
(
SELECT
EXPTRANS.out_concat as output
FROM
EXPTRANS
);


-- Component tg_def, Type EXPORT_DATA Exporting data
copy into @edw_stage/Parameter/edw_base/tg_def.txt
from 
(select output
from tg_def)
file_format = ''CSV_FORMAT''
OVERWRITE = TRUE
;


END; ';