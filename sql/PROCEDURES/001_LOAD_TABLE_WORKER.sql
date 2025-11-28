-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.LOAD_TABLE_WORKER("P_EXECUTION_ID" VARCHAR, "P_WORKER_ID" VARCHAR, "P_FILE_STAGE" VARCHAR, "P_TABLE_NAME" VARCHAR, "P_TARGET_TABLE_PATH" VARCHAR, "P_FILE_PATH" VARCHAR, "P_FILE_FORMAT_NAME" VARCHAR, "P_ERROR_HANDLING" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS 'DECLARE
    /* Uncomment these parameters for testing in anonymous block */
    -- p_execution_id              VARCHAR := ''test_run_077'';
    -- p_worker_id                 VARCHAR := ''worker_table_1'';
    -- p_table_name                VARCHAR := ''table_1'';
    -- p_target_table_path         VARCHAR := ''PROD.DB_T_PROD_COMN.TABLE_1''; ;
    -- p_file_path                 VARCHAR := ''table_1/'';
    -- p_file_format_name          VARCHAR := ''alfa_csv_format'';
    -- p_error_handling            VARCHAR := ''CONTINUE'';
    -- p_file_stage                VARCHAR := ''edw_dev.public.sf_s3_dev_stage'';
    /* End of anonymous block testing parameters*/

    v_start_time                TIMESTAMP_NTZ;
    v_end_time                  TIMESTAMP_NTZ;
    v_duration_seconds          NUMBER;
    v_rows_per_second           NUMBER;
    v_sql2run                   VARCHAR;
    v_last_query_id             VARCHAR;
    v_status_check              VARCHAR;
    v_file_name                 VARCHAR     := '''';
    v_rows_parsed               NUMBER     DEFAULT 0;
    v_rows_loaded               NUMBER     DEFAULT 0;
    v_total_rows_parsed         NUMBER     DEFAULT 0;
    v_total_rows_loaded         NUMBER     DEFAULT 0;
    v_files_loaded              NUMBER     DEFAULT 0;
    v_error_rows                NUMBER     DEFAULT 0; 
    v_total_error_rows          NUMBER     DEFAULT 0;        
    v_error_limit               NUMBER     DEFAULT 0;        
    v_total_error_limit         NUMBER     DEFAULT 0; 
    v_first_error               VARCHAR;
    v_first_error_line          NUMBER     DEFAULT 0;
    v_first_error_character     VARCHAR;
    v_first_error_column        VARCHAR;
    v_file_count                NUMBER     DEFAULT 0;
    v_summary_status            VARCHAR    DEFAULT ''SUCCESS'';

BEGIN

    v_start_time := CURRENT_TIMESTAMP();

    -- Mark control row as IN_PROGRESS
    UPDATE edw_dev.public.load_control
    SET status = ''IN_PROGRESS'',
        status_date = :v_start_time
    WHERE table_name = :p_table_name and target_table_path = :p_target_table_path;  

    -- Construct COPY INTO SQL
    v_sql2run := $$COPY INTO $$ || :p_target_table_path || $$
        FROM @$$ || upper(:p_file_stage) || $$/$$ || upper(:p_file_path) || $$
        FILE_FORMAT = (FORMAT_NAME = $$ || :p_file_format_name || $$)
        ON_ERROR = ''$$ || :p_error_handling || $$'';$$;
    -- return :v_sql2run;
    -- end;

    -- Execute the COPY INTO statement
    EXECUTE IMMEDIATE v_sql2run;

    -- Capture the query ID of the COPY INTO
    v_last_query_id := LAST_QUERY_ID();                   

    
    LET result_cur cursor for SELECT * from TABLE(RESULT_SCAN(?));
    open result_cur using(v_last_query_id);

    v_end_time := CURRENT_TIMESTAMP();

-- Iterate over the result rows
    FOR row_rec IN result_cur DO
        LET v_status STRING := row_rec."status";
        IF (v_status = ''PARTIALLY_LOADED'' AND v_summary_status = ''SUCCESS'') THEN
            v_summary_status := ''PARTIAL'';
        ELSEIF (v_status = ''LOAD_FAILED'') THEN
            v_summary_status := ''FAILED'';
        END IF;
        CASE 
            WHEN v_status != ''Copy executed with 0 files processed.'' 
            THEN
                v_file_name                 := row_rec."file";
                v_files_loaded              := 1;
                v_rows_loaded               := row_rec."rows_loaded";
                v_rows_parsed               := row_rec."rows_parsed";
                v_error_rows                := row_rec."errors_seen";     
                v_error_limit               := row_rec."error_limit";
                v_total_rows_loaded         := :v_total_rows_loaded + row_rec."rows_loaded";
                v_total_rows_parsed         := :v_total_rows_parsed + row_rec."rows_parsed";
                v_total_error_rows          := :v_total_error_rows + row_rec."errors_seen";    
                v_total_error_limit         := :v_total_error_limit + row_rec."error_limit";
                v_first_error               := row_rec."first_error";        
                v_first_error_line          := row_rec."first_error_line";  
                v_first_error_character     := row_rec."first_error_character";
                v_first_error_column        := row_rec."first_error_column_name";
                v_file_count                := :v_file_count + 1;
            ELSE 
                v_file_name                 := ''No Files'';
                v_files_loaded              := 0;
                v_rows_parsed               := 0;
                v_rows_loaded               := 0;
                v_error_rows                := 0;
                v_error_limit               := 0;
                v_total_rows_loaded         := 0;
                v_total_rows_parsed         := 0;
                v_total_error_rows          := 0;   
                v_total_error_limit         := 0;
                v_first_error               := '''';        
                v_first_error_line          := 0;  
                v_first_error_character     := 0;
                v_first_error_column        := '''';
                v_file_count                := 0;
        END CASE;
    
        INSERT INTO edw_dev.public.load_log_detail (
            execution_id,
            worker_id,
            table_name,
            file_name,
            stage_file_path,
            start_time,
            end_time,
            rows_parsed,
            rows_loaded,
            error_limit,
            error_row_count,
            first_error,
            first_error_line,
            first_error_character,
            first_error_column,
            load_query_id,
            load_status
            )
        VALUES (
            :p_execution_id,
            :p_worker_id,
            :p_table_name,
            :v_file_name,
            ''@'' || :p_file_stage || ''/'' || :p_file_path,
            :v_start_time,
            :v_end_time,
            :v_rows_parsed,
            :v_rows_loaded,
            :v_error_limit,
            :v_error_rows,   
            :v_first_error,          
            :v_first_error_line ,    
            :v_first_error_character,
            :v_first_error_column ,
            :v_last_query_id,
            :v_status
        );
    END FOR;

    -- Mark control row as DONE
    UPDATE edw_dev.public.load_control
    SET status = ''DONE'',
        status_date = :v_end_time
    WHERE table_name = :p_table_name;
    
    -- End time and performance metrics
    
    v_duration_seconds := (select DATEDIFF(''second'', :v_start_time, :v_end_time));
    
    IF (v_duration_seconds > 0) THEN
        v_rows_per_second := :v_total_rows_loaded / :v_duration_seconds;
    ELSE
        v_rows_per_second := 0;
    END IF;
    
    -- Write summary log
    INSERT INTO edw_dev.public.load_log (
        execution_id,
        worker_id,
        table_name,
        stage_file_path,
        start_time,
        end_time,
        duration_seconds,
        files_processed,
        rows_parsed,
        rows_loaded,
        rows_per_second,
        error_limit,
        error_row_count,
        load_query_id,
        load_status
    )
    VALUES (
        :p_execution_id,
        :p_worker_id,
        :p_table_name,
        ''@'' || :p_file_stage || ''/'' || :p_file_path,
        :v_start_time,
        :v_end_time,
        :v_duration_seconds,
        :v_file_count,
        :v_total_rows_parsed,
        :v_total_rows_loaded,
        :v_rows_per_second,
        :v_total_error_limit,
        :v_total_error_rows,
        :v_last_query_id,
        :v_summary_status
    );


    RETURN ''Processed-1 '' || TO_VARCHAR(:v_file_count) || '' file(s) for '' || :p_target_table_path;
END';