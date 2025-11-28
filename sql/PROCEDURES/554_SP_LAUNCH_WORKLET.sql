-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.SP_LAUNCH_WORKLET("WORKFLOW_NAME" VARCHAR, "WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
    -- run_id STRING;
    -- failed_count INTEGER;
    -- completed_count INTEGER;
    -- total_count INTEGER;
    -- poll_attempts INTEGER DEFAULT 0;
    -- max_attempts INTEGER DEFAULT 360; -- e.g. 360 * 10 sec = 60 min timeout
    -- STATEMENT_EXCEPTION EXCEPTION (-20001, ''''Worklet failed'''');
    -- TIMEOUT_EXCEPTION EXCEPTION (-20002, ''''Worklet timed out waiting for tasks'''');

    run_id STRING;
    root_run_id STRING;
    root_task_id STRING;
    root_db string;
    root_schema string;
    root_task string;
    parts array;
    graph_id string;
    attempt integer default 0;
    timeout_minutes integer := 120;
    max_attempts integer := (timeout_minutes * 6); -- poll every 10s
    final_state string;
    err_task string;
    err_code string;
    err_msg string;

    E_NOTFOUND exception (-20001, ''Could not locate CURRENT_TASK_GRAPHS'');
    E_FAILED   exception (-20002, ''Task graph failed'');
    E_TIMEOUT  exception (-20003, ''Timed out waiting for task graph'');
BEGIN
    root_db := CURRENT_DATABASE();
    root_schema := CURRENT_SCHEMA();
    root_task := UPPER(WORKLET_NAME) || ''_ROOT'';
    
    -- Get latest Run Id
    run_id := (
        SELECT run_id
        FROM control_run_id 
        WHERE workflow_name = :workflow_name
        ORDER BY insert_ts DESC 
        LIMIT 1
    );

    -- Insert tracking row
    INSERT INTO control_run_id (workflow_name, worklet_name, run_id, insert_ts)
    VALUES (:workflow_name, :worklet_name, :run_id, CURRENT_TIMESTAMP());

    -- Kick off root task
    EXECUTE IMMEDIATE ''EXECUTE TASK '' || root_db || ''.'' || root_schema || ''.'' || root_task;

    -- Small delay before polling to ensure DAG is scheduled
    SELECT SYSTEM$WAIT(5);

    -- Capture the running graph id (most recent for this root)
    select graph_run_group_id into :graph_id
    from table(information_schema.current_task_graphs())
    where root_task_name = :root_task
    qualify row_number() over (order by scheduled_time desc) = 1;
    
    -- Retry briefly if it hasn''t appeared yet
    while (graph_id is null and attempt < 6) do
        select system$wait(5);
        attempt := attempt + 1;
    
        select graph_run_group_id
            into :graph_id
        from table(information_schema.current_task_graphs())
        where root_task_name = :root_task
        qualify row_number() over (order by scheduled_time desc) = 1;
    end while;
    
    if (graph_id is null) then
        raise E_NOTFOUND;
    end if;

    -- Poll until graph disappears from CURRENT_TASK_GRAPHS (i.e., done)
    attempt := 0;
    while (attempt < max_attempts) do
        if (exists (
            select 1
            from table(information_schema.current_task_graphs())
            where graph_run_group_id = :graph_id
            limit 1
        )) then
            select system$wait(10);
            attempt := attempt + 1;
            continue;
        end if;
        
        -- Finished: read the final result from COMPLETE_TASK_GRAPHS
        select state, first_error_task_name, first_error_code, first_error_message
            into :final_state, :err_task, :err_code, :err_msg
        from table(information_schema.complete_task_graphs(result_limit => 50, root_task_name => :root_task))
        where graph_run_group_id = :graph_id
        qualify row_number() over (order by completed_time desc) = 1;
        
        if (final_state = ''SUCCEEDED'') then
            return ''SUCCEEDED'';
        elseif (final_state in (''FAILED'',''CANCELLED'')) then
            raise E_FAILED;
        else
            raise E_FAILED;
        end if;
    end while;
    
    raise E_TIMEOUT;
end;
';