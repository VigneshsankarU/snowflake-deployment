-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.SP_SET_PARAM("RUN_ID" VARCHAR, "SCOPE_TYPE" VARCHAR, "SCOPE_NAME" VARCHAR, "PARAM_NAME" VARCHAR, "PARAM_VALUE" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '

MERGE INTO control_params AS target
USING (
  SELECT 
    :run_id AS run_id,
    :scope_type AS scope_type,
    :scope_name AS scope_name,
    :param_name AS param_name,
    :param_value AS param_value
) AS source
ON 
  target.run_id = source.run_id AND
  target.scope_type = source.scope_type AND
  target.scope_name = source.scope_name AND
  target.param_name = source.param_name

WHEN MATCHED THEN
  UPDATE SET target.param_value = source.param_value

WHEN NOT MATCHED THEN
  INSERT (
    run_id, scope_type, scope_name, param_name, param_value
  )
  VALUES (
    source.run_id, source.scope_type, source.scope_name, source.param_name, source.param_value
  );

';