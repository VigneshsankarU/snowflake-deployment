-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_EDW_CUTOFF_DATE("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);
 

-- Component SQ_EDW_RECON, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_EDW_RECON 
AS
WITH base AS (
  SELECT
    CTR.CUTOFF_DATE::DATE                         AS cutoff_date_only,
    CTR.CUTOFF_DATE::TIMESTAMP_NTZ                AS cutoff_date_time,
    DATE_TRUNC(''MONTH'', CTR.CUTOFF_DATE)::TIMESTAMP_NTZ AS cutoff_first_day,
    DATEADD(
      SECOND, -1,
      DATEADD(
        MONTH, 1,
        DATE_TRUNC(''MONTH'', CTR.CUTOFF_DATE)
      )::TIMESTAMP_NTZ
    )                                             AS cutoff_last_day,
    ''POLICY CENTER''                               AS app_ind
  FROM (
    SELECT
      DATEADD(
    SECOND,
    CAST(LKPTBL.StringValue_stg AS DECIMAL(35,6)) / 1000,
    CURRENT_TIMESTAMP(6)
  ) 
      AS cutoff_date
    FROM db_t_prod_stag.pc_parameter LKPTBL
    WHERE ParameterName_stg = ''TestingClock:CurrentTime''
  ) CTR
),
enriched AS (
  SELECT
    b.*,
    ROW_NUMBER() OVER (ORDER BY 1) AS source_record_id
  FROM base b
)
SELECT
  cutoff_date_only,
  cutoff_date_time,
  cutoff_first_day,
  cutoff_last_day,
  app_ind,
  source_record_id
FROM enriched;


-- Component EXP_RECON_SET_VARIABLES, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_RECON_SET_VARIABLES AS
(
SELECT
SQ_EDW_RECON.CUTOFF_DATE_TIME as CUTOFF_DATE_TIME,
SQ_EDW_RECON.APP_IND as APP_IND,
1 as ID,
CURRENT_TIMESTAMP as DTTM,
SQ_EDW_RECON.source_record_id
FROM
SQ_EDW_RECON
);


-- Component TGT_EDW_CUTOFF_DATE, Type TARGET 
INSERT INTO db_t_prod_stag.out_EDW_CutoffDate
(
ID_stg,
CreateTS_stg,
UpdateTS_stg,
APP_IND_stg,
CUTOFF_DATE_stg
)
SELECT
EXP_RECON_SET_VARIABLES.ID as ID_stg,
EXP_RECON_SET_VARIABLES.DTTM as CreateTS_stg,
EXP_RECON_SET_VARIABLES.DTTM as UpdateTS_stg,
EXP_RECON_SET_VARIABLES.APP_IND as APP_IND_stg,
EXP_RECON_SET_VARIABLES.CUTOFF_DATE_TIME as CUTOFF_DATE_stg
FROM
EXP_RECON_SET_VARIABLES;


END; ';