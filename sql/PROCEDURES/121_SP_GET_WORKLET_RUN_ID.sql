-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.SP_GET_WORKLET_RUN_ID("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
    var input = arguments[0];  // get first parameter

    var sql_command = `
        SELECT run_id
        FROM control_worklet
        WHERE worklet_name = ?
        ORDER BY insert_ts DESC
        LIMIT 1
    `;

    var stmt = snowflake.createStatement({
        sqlText: sql_command,
        binds: [input]
    });

    var result = stmt.execute();
    var run_id = null;

    if (result.next()) {
        run_id = result.getColumnValue(1);
    }

    if (run_id !== null) {
        snowflake.execute({
            sqlText: "CALL SYSTEM$SET_RETURN_VALUE(?)",
            binds: [run_id]
        });
    }

    return run_id;
';