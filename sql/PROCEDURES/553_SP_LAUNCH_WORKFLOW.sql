-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.SP_LAUNCH_WORKFLOW("ROOT_TASK_QUALIFIED" VARCHAR, "TIMEOUT_MINUTES" NUMBER(38,0) DEFAULT 120)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
declare
  root_db string;
  root_schema string;
  root_task string;
  parts array;
  graph_id string;
  attempt integer default 0;
  max_attempts integer := (timeout_minutes * 6); -- poll every 10s
  final_state string;
  err_task string;
  err_code string;
  err_msg string;

  E_FAILED  exception (-20001, ''Task graph failed'');
  E_TIMEOUT exception (-20002, ''Timed out waiting for task graph'');
begin
  -- Parse task name: accept DB.SCHEMA.TASK or just TASK (uses current DB/SCHEMA)
  parts := split(root_task_qualified, ''.'');
  root_db     := parts[0];
  root_schema := parts[1];
  root_task   := parts[2];
  
  -- Kick off the graph
  execute immediate ''execute task '' || root_db || ''.'' || root_schema || ''.'' || root_task;

  -- Let CURRENT_TASK_GRAPHS populate
  select system$wait(5);

  -- Capture the running graph id (most recent for this root)
  select graph_run_group_id into :graph_id
  from table(information_schema.current_task_graphs())
  where database_name = :root_db
    and schema_name   = :root_schema
    and root_task_name     = :root_task
  qualify row_number() over (order by scheduled_time desc) = 1;

  -- Retry briefly if it hasn''t appeared yet
  while (graph_id is null and attempt < 6) do
    select system$wait(5);
    attempt := attempt + 1;

    select graph_run_group_id
      into :graph_id
    from table(information_schema.current_task_graphs())
    where database_name = :root_db
      and schema_name   = :root_schema
      and root_task_name     = :root_task
    qualify row_number() over (order by scheduled_time desc) = 1;
  end while;

  if (graph_id is null) then
    raise E_FAILED; -- using message = ''Could not locate CURRENT_TASK_GRAPHS for '' || root_db || ''.'' || root_schema || ''.'' || root_task;
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
      raise E_FAILED; -- using message =
         -- ''Task graph ''||root_db||''.''||root_schema||''.''||root_task||'' ended ''||coalesce(final_state,''UNKNOWN'')
         -- || case when err_task is not null then '' (first_error_task=''||err_task||'')'' else '''' end
         -- || case when err_code is not null then '' code=''||err_code else '''' end
         -- || case when err_msg  is not null then '': ''||err_msg  else '''' end;
    else
      raise E_FAILED; -- using message = ''Unexpected final state: ''||coalesce(final_state,''NULL'');
    end if;
  end while;

  raise E_TIMEOUT; -- using message = ''Timed out after ''||timeout_minutes||'' minute(s) waiting for ''
                   --               ||root_db||''.''||root_schema||''.''||root_task;
end;
';