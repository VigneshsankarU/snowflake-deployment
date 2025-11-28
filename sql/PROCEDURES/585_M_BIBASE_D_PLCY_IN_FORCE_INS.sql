-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BIBASE_D_PLCY_IN_FORCE_INS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE
  LASTRUNDATE_PLCY_INFORCE STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);

  LASTRUNDATE_PLCY_INFORCE := public.func_get_scoped_param(:run_id, ''lastrundate_plcy_inforce'', :workflow_name, :worklet_name, :session_name);
 

-- Component SQ_AGMT, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_AGMT AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as MTH_NUM,
$2 as YR_NUM,
$3 as LOAD_DT,
$4 as PLCY_NUM,
$5 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select 

CAST(EXTRACT( MONTH , :lastrundate_plcy_inforce::date  )   AS BIGINT) AS MTH_NUM 

,CAST(EXTRACT( YEAR ,  :lastrundate_plcy_inforce::date  )   AS BIGINT) AS YR_NUM

,CURRENT_DATE  LOAD_DT

,HOST_AGMT_NUM PLCY_NUM



FROM 

(
SELECT * FROM --TCORE.AGMT
db_t_prod_core.agmt
WHERE CAST(MODL_EFF_DTTM AS DATE) <= :lastrundate_plcy_inforce::date 
AND MODL_CRTN_DTTM <=  :lastrundate_plcy_inforce::date 
QUALIFY RANK() OVER (PARTITION BY HOST_AGMT_NUM ORDER BY TERM_NUM DESC, MODL_NUM DESC) =1
) AG

JOIN (
SELECT * FROM --TCORE.AGMT_STS 
db_t_prod_core.agmt_sts
WHERE cast(AGMT_STS_STRT_DTTM as DATE) <=   :lastrundate_plcy_inforce::date 
QUALIFY RANK() OVER (PARTITION BY AGMT_ID ORDER BY AGMT_STS_STRT_DTTM DESC) = 1 
  ) AST ON AST.AGMT_ID = AG.AGMT_ID   AND AST.AGMT_STS_CD=''INFORCE''  

 AND AG.EDW_END_DTTM=''9999-12-31 23:59:59.999999''  AND AGMT_TYPE_CD=''PPV''

AND VFYD_PLCY_IND =''Y''
) SRC
)
);


-- Component exp_plcy_in_force, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_plcy_in_force AS
(
SELECT
SQ_AGMT.MTH_NUM as MTH_NUM,
SQ_AGMT.YR_NUM as YR_NUM,
SQ_AGMT.LOAD_DT as LOAD_DT,
SQ_AGMT.PLCY_NUM as PLCY_NUM,
SQ_AGMT.source_record_id
FROM
SQ_AGMT
);


-- Component D_PLCY_IN_FORCE, Type TARGET 
INSERT INTO db_v_prod_pres.D_PLCY_IN_FORCE
(
MTH_NUM,
YR_NUM,
LOAD_DT,
PLCY_NUM
)
SELECT
exp_plcy_in_force.MTH_NUM as MTH_NUM,
exp_plcy_in_force.YR_NUM as YR_NUM,
exp_plcy_in_force.LOAD_DT as LOAD_DT,
exp_plcy_in_force.PLCY_NUM as PLCY_NUM
FROM
exp_plcy_in_force;


END; ';