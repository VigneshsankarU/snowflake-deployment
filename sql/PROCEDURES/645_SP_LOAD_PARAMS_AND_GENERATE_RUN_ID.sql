-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.SP_LOAD_PARAMS_AND_GENERATE_RUN_ID("PARAM_FILE_NAME" VARCHAR, "WORKFLOW_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
    v_run_id STRING;
    current_scope_type STRING DEFAULT ''4_global'';
    current_scope_name STRING DEFAULT NULL;
    parsed_workflow_name STRING DEFAULT NULL;
    parsed_worklet_name STRING DEFAULT NULL;
    parsed_session_name STRING DEFAULT NULL;
    param_name STRING;
    param_value STRING;
    stage_sql STRING;
    stage_resultset RESULTSET;
    var_line VARCHAR;
    output_str STRING DEFAULT '''';
    last_qid STRING;
    resolved_value STRING;
    var_name STRING;
    var_value STRING;
    insert_ts STRING;

BEGIN
    v_run_id := UUID_STRING();
    stage_sql := ''SELECT TRIM($1) AS line_raw FROM @public.edw_stage'' || :param_file_name || '' (FILE_FORMAT => ''''param_file_format'''') WHERE $1 IS NOT NULL'';
    output_str := :output_str || :stage_sql || ''; '';
    stage_resultset := (EXECUTE IMMEDIATE :stage_sql);
    
    FOR stage_record IN stage_resultset DO

        var_line := stage_record.line_raw;
        
        IF (var_line IS NULL) THEN 
            EXIT;
        END IF;

        -- Skip comment lines starting with --
        IF(var_line = '''' OR var_line LIKE ''--%'' ) THEN
            CONTINUE;
        END IF;
        
        -- Scope header detection
        IF(var_line LIKE ''[%'' ) THEN
            parsed_session_name := NULL;
            current_scope_type := NULL;
            current_scope_name := NULL;

            IF(var_line = ''[global]'' ) THEN
                current_scope_type := ''4_global'';
                current_scope_name := NULL;
                parsed_workflow_name := NULL;
                parsed_worklet_name := NULL;

            ELSEIF(var_line LIKE ''%WF:%'' ) THEN
                parsed_workflow_name := REGEXP_SUBSTR(var_line, ''WF:([^\\\\\\\\.\\\\\\\\]]+)'', 1, 1, ''e'');

                IF(parsed_workflow_name = :workflow_name ) THEN
                    IF(var_line LIKE ''%WT:%'' ) THEN
                        parsed_worklet_name := REGEXP_SUBSTR(var_line, ''WT:([^\\\\\\\\.\\\\\\\\]]+)'', 1, 1, ''e'');
                        current_scope_type := ''2_worklet'';
                        current_scope_name := CONCAT(parsed_workflow_name, '':'', parsed_worklet_name);
                    ELSE
                        current_scope_type := ''3_workflow'';
                        current_scope_name := parsed_workflow_name;
                        parsed_worklet_name := NULL;
                    END IF;
                ELSE
                    current_scope_type := NULL;
                    current_scope_name := NULL;
                    parsed_worklet_name := NULL;
                END IF;

            ELSEIF(var_line LIKE ''[s_%]'' ) THEN
                parsed_session_name := SUBSTR(var_line, 2, LENGTH(var_line) - 2);
                current_scope_type := ''1_session'';
                current_scope_name := parsed_session_name;

            ELSE
                current_scope_type := ''4_global'';
                current_scope_name := NULL;
                parsed_workflow_name := NULL;
                parsed_worklet_name := NULL;
                parsed_session_name := NULL;
            END IF;

        ELSEIF(var_line LIKE ''\\$%'') THEN
            param_name := REGEXP_REPLACE(TRIM(SPLIT_PART(var_line, ''='', 1)), ''^[\\$]+'', '''');
            param_value := REGEXP_REPLACE(TRIM(SPLIT_PART(var_line, ''='', 2)), ''^['''']|['''']$'', '''');

            output_str := output_str || param_name || ''='' || param_value || ''; '';

            CALL sp_set_param(:v_run_id, :current_scope_type, :current_scope_name, :param_name, :param_value);
        END IF;

        output_str := :output_str || :var_line || ''; '';

    END FOR;

    -- Parse them again for variable replaement and query execution
    stage_sql := ''SELECT * FROM public.control_params WHERE run_id = \\'''' || :v_run_id || ''\\'''';  
    stage_resultset := (EXECUTE IMMEDIATE :stage_sql);

    FOR stage_record IN stage_resultset DO

        param_name := stage_record.param_name;
        param_value := stage_record.param_value;
        insert_ts := stage_record.insert_ts;

        resolved_value := param_value;
        WHILE (REGEXP_LIKE(resolved_value, ''.*\\\\$\\\\$.*'')) DO
            var_name := REGEXP_SUBSTR(:resolved_value, ''\\\\$\\\\$([A-Za-z0-9_]+)'', 1, 1, ''e'');
            var_value := (SELECT param_value FROM public.control_params WHERE param_name = :var_name AND run_id = :v_run_id);
            var_value := REPLACE(var_value, ''\\\\'', ''\\\\\\\\'');

            resolved_value := REGEXP_REPLACE(resolved_value, ''\\\\$\\\\$'' || var_name, var_value, 1, 1, ''e'');
        END WHILE;

        IF (LEFT(resolved_value, 7) = ''SELECT '') THEN
            -- Run dynamic query
            EXECUTE IMMEDIATE :resolved_value;
            last_qid := LAST_QUERY_ID();

            resolved_value := (SELECT $1 FROM TABLE(RESULT_SCAN(:last_qid)) LIMIT 1);
        END IF;

        output_str := output_str || :param_name || ''='' || :resolved_value || ''; '';

        UPDATE public.control_params 
        SET param_value = :resolved_value 
        WHERE param_name = :param_name AND run_id = :v_run_id AND insert_ts = :insert_ts;

    END FOR;

    output_str := output_str || v_run_id || ''; '';
    RETURN output_str;

END;
';