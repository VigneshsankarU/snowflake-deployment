-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.SP_CONTROL_CLEANUP("WORKLET_NAME" VARCHAR, "DEBUG" BOOLEAN)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
  run_id STRING;
BEGIN
  run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);

  IF(:debug = FALSE) THEN
    DELETE FROM control_status WHERE run_id = :run_id;
    DELETE FROM control_params WHERE run_id = :run_id;
    DELETE FROM control_run_id WHERE run_id = :run_id;
  END IF;

END; ';