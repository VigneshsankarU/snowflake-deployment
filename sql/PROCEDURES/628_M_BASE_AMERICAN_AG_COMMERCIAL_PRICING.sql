-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AMERICAN_AG_COMMERCIAL_PRICING("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
    -- Use variables for repeated and complex date calculations to improve
    -- readability and maintainability.
    report_date_eod TIMESTAMP;
    eom_date_var DATE;
  PRCS_ID STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);

  PRCS_ID := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);

    -- Set the report date to the last microsecond of the previous year.
    report_date_eod := public.func_get_scoped_param(:run_id, ''report_date_eod'', :workflow_name, :worklet_name, :session_name);
    
    -- Set the end-of-month date to the last day of the previous year.
    eom_date_var := public.func_get_scoped_param(:run_id, ''eom_date_var'', :workflow_name, :worklet_name, :session_name);

-- Component SQ_pc_policyperiod, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_policyperiod AS
-- Use Common Table Expressions (CTEs) to break down the complex logic into
-- understandable, sequential steps.
WITH
-- 1. Find policy terms active at the end of the previous year.
TRM_CTE AS (
    SELECT HOST_AGMT_NUM, TERM_NUM, AGMT_ID
    FROM DB_T_PROD_CORE.AGMT
    WHERE AGMT_TYPE_CD = ''POLTRM''
      AND :report_date_eod BETWEEN AGMT_EFF_DTTM AND AGMT_PLND_EXPN_DTTM
    GROUP BY 1, 2, 3
),

-- 2. Find the most recent policy version (PPV) within each active term.
PPV_CTE AS (
    SELECT
        T1.HOST_AGMT_NUM,
        T1.TERM_NUM,
        T1.AGMT_ID,
        T1.AGMT_EFF_DTTM,
        T1.AGMT_PLND_EXPN_DTTM,
        T1.AGMT_OPN_DTTM,
        -- This alias is used in the WHERE clause below, which is a Snowflake feature
        IFF(T1.MODL_EFF_DTTM > T1.MODL_CRTN_DTTM, T1.MODL_EFF_DTTM, T1.MODL_CRTN_DTTM) AS NEW_AGMT_EFF_DTTM
    FROM DB_T_PROD_CORE.AGMT AS T1
    WHERE T1.AGMT_TYPE_CD = ''PPV''
      AND T1.SRC_SYS_CD = ''GWPC''
      AND :report_date_eod BETWEEN T1.AGMT_EFF_DTTM AND T1.AGMT_PLND_EXPN_DTTM
      -- This correlated subquery can be slow. Consider rewriting with a window function if performance is an issue.
      AND T1.TRANS_STRT_DTTM = (SELECT MIN(T2.TRANS_STRT_DTTM) FROM DB_T_PROD_CORE.AGMT AS T2 WHERE T1.AGMT_ID = T2.AGMT_ID)
      AND NEW_AGMT_EFF_DTTM <= :report_date_eod
    QUALIFY ROW_NUMBER() OVER (PARTITION BY T1.HOST_AGMT_NUM ORDER BY T1.MODL_CRTN_DTTM DESC, T1.TRANS_END_DTTM DESC) = 1
)
-- 3. Join all data sources together to build the final dataset.
-- The convoluted SELECT $1, $2... FROM (SELECT SRC.* ...) wrapper has been removed for clarity.
SELECT
    PPV.HOST_AGMT_NUM,
    mb1.MBRSHP_TYPE_CD,
    mb1.MBRSHP_NUM,
    CAST(PPV.AGMT_EFF_DTTM AS DATE) AS EFF_DTTM,
    CAST(PPV.AGMT_PLND_EXPN_DTTM AS DATE) AS EXP_DTTM,
    CAST(PPV.AGMT_OPN_DTTM AS DATE) AS INC_DT,
    TR.GEOGRCL_AREA_NAME AS BASE_ST,
    ILT.INSRNC_LOB_TYPE_DESC AS LOB_CD,
    PR.PROD_DESC,
    PPV.AGMT_ID,
    A_S.AGMT_STS_CD,
    pm.plcy_amt AS TOT_PREM,
    LTRIM(IO1.INTRNL_ORG_NUM,''0'') AS AGENT_NUM,
    LTRIM(IO3.INTRNL_ORG_NUM,''0'') AS SVC_NUM,
    IO1.PRTY_DESC AS AGENT_DESC,
    IO2.INTRNL_ORG_NUM AS CMPY,
    :eom_date_var AS EOM_DT,
    -- Generate a deterministic source record ID
    ROW_NUMBER() OVER (ORDER BY PPV.HOST_AGMT_NUM) AS source_record_id
FROM TRM_CTE AS TRM
INNER JOIN PPV_CTE AS PPV
    ON PPV.HOST_AGMT_NUM = TRM.HOST_AGMT_NUM AND PPV.TERM_NUM = TRM.TERM_NUM
/*FOR AGENT*/
LEFT JOIN DB_T_PROD_CORE.PRTY_AGMT AS PA13
    ON PPV.AGMT_ID = PA13.AGMT_ID
    AND PA13.PRTY_AGMT_ROLE_CD = ''PRDA''
    AND PA13.EDW_END_DTTM = ''9999-12-31 23:59:59.999999''
LEFT JOIN DB_T_PROD_CORE.INTRNL_ORG AS IO1
    ON IO1.INTRNL_ORG_PRTY_ID = PA13.PRTY_ID
    AND :report_date_eod BETWEEN IO1.TRANS_STRT_DTTM AND IO1.TRANS_END_DTTM
/*FOR COMPANY*/
-- Fixed: Added schema name DB_T_PROD_CORE to PRTY_AGMT
LEFT JOIN DB_T_PROD_CORE.PRTY_AGMT AS PA14
    ON PPV.AGMT_ID = PA14.AGMT_ID
    AND PA14.PRTY_AGMT_ROLE_CD = ''CMP''
    AND PA14.EDW_END_DTTM = ''9999-12-31 23:59:59.999999''
LEFT JOIN DB_T_PROD_CORE.INTRNL_ORG AS IO2
    ON IO2.INTRNL_ORG_PRTY_ID = PA14.PRTY_ID
    AND :report_date_eod BETWEEN IO2.TRANS_STRT_DTTM AND IO2.TRANS_END_DTTM
/*FOR SVC*/
-- Fixed: Corrected typo from DBT_PROD_CORE to DB_T_PROD_CORE
LEFT JOIN DB_T_PROD_CORE.PRTY_AGMT AS PA15
    ON PPV.AGMT_ID = PA15.AGMT_ID
    AND PA15.PRTY_AGMT_ROLE_CD = ''SVC''
    AND PA15.EDW_END_DTTM = ''9999-12-31 23:59:59.999999''
LEFT JOIN DB_T_PROD_CORE.INTRNL_ORG AS IO3
    ON IO3.INTRNL_ORG_PRTY_ID = PA15.PRTY_ID
    AND :report_date_eod BETWEEN IO3.TRANS_STRT_DTTM AND IO3.TRANS_END_DTTM
/*For LOB*/
LEFT JOIN DB_T_PROD_CORE.AGMT_PROD AS AP
    ON PPV.AGMT_ID = AP.AGMT_ID AND AP.EDW_END_DTTM = ''9999-12-31 23:59:59.999999''
INNER JOIN DB_T_PROD_CORE.PROD AS PR
    ON AP.PROD_ID = PR.PROD_ID
    AND PR.PROD_SBTYPE_CD IN (''PLCYTYPE'')
    AND PR.EDW_END_DTTM = ''9999-12-31 23:59:59.999999''
LEFT JOIN DB_T_PROD_CORE.INSRNC_LOB_TYPE AS ILT
    ON ILT.INSRNC_LOB_TYPE_CD = PR.INSRNC_LOB_TYPE_CD
    AND ILT.EDW_END_DTTM = ''9999-12-31 23:59:59.999999''
/*For Prem*/
LEFT JOIN DB_T_PROD_CORE.PLCY_MTRC AS pm
    ON pm.agmt_id = PPV.agmt_id
    AND insrnc_mtrc_type_cd = ''TOTTRMPREM''
    AND pm.EDW_END_DTTM = ''9999-12-31 23:59:59.999999''
/*For Membership*/
LEFT JOIN DB_T_PROD_CORE.AGMT_mbrshp AS AM1
    ON PPV.AGMT_ID = AM1.AGMT_ID AND AM1.EDW_END_DTTM = ''9999-12-31 23:59:59.999999''
LEFT JOIN DB_T_PROD_CORE.mbrshp AS mb1
    ON AM1.mbrshp_id = mb1.mbrshp_id AND mb1.EDW_END_DTTM = ''9999-12-31 23:59:59.999999''
/*FOR STATE*/
LEFT JOIN DB_T_PROD_CORE.AGMT_LOCTR AS AL1
    ON PPV.AGMT_ID = AL1.AGMT_ID AND AL1.EDW_END_DTTM = ''9999-12-31 23:59:59.999999''
LEFT JOIN DB_T_PROD_CORE.TERR AS TR
    ON TR.TERR_ID = AL1.LOC_ID
    AND AGMT_LOCTR_ROLE_TYPE_CD = ''AGTWINST''
    AND TR.EDW_END_DTTM = ''9999-12-31 23:59:59.999999''
/*For Status*/
INNER JOIN DB_T_PROD_CORE.AGMT_STS AS A_S
    ON TRM.AGMT_ID = A_S.AGMT_ID
    AND A_S.AGMT_STS_STRT_DTTM <= :report_date_eod
    AND A_S.AGMT_STS_CD <> ''CNFRMDDT''
-- Get the latest status for each agreement as of the report date
QUALIFY ROW_NUMBER() OVER (PARTITION BY PPV.AGMT_ID ORDER BY A_S.AGMT_STS_STRT_DTTM DESC) = 1;

-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
SQ_pc_policyperiod.HOST_AGMT_NUM as HOST_AGMT_NUM,
SQ_pc_policyperiod.MBRSHP_TYPE_CD as MBRSHP_TYPE_CD,
SQ_pc_policyperiod.MBRSHP_NUM as MBRSHP_NUM,
SQ_pc_policyperiod.EFF_DTTM as EFF_DTTM,
SQ_pc_policyperiod.EXP_DTTM as EXP_DTTM,
SQ_pc_policyperiod.INC_DT as INC_DT,
SQ_pc_policyperiod.BASE_ST as BASE_ST,
SQ_pc_policyperiod.LOB_CD as LOB_CD,
SQ_pc_policyperiod.PROD_DESC as PROD_DESC,
SQ_pc_policyperiod.AGMT_ID as AGMT_ID,
SQ_pc_policyperiod.AGMT_STS_CD as AGMT_STS_CD,
SQ_pc_policyperiod.TOT_PREM as TOT_PREM,
SQ_pc_policyperiod.AGENT_NUM as AGENT_NUM,
SQ_pc_policyperiod.SVC_NUM as SVC_NUM,
SQ_pc_policyperiod.AGENT_DESC as AGENT_DESC,
SQ_pc_policyperiod.CMPY as CMPY,
:PRCS_ID as PRCS_ID,
SQ_pc_policyperiod.EOM_DT as EOM_DT,
SQ_pc_policyperiod.source_record_id
FROM
SQ_pc_policyperiod
);


-- Component GW_INFORCE_SVC, Type TARGET 
INSERT INTO db_t_prod_comn.GW_INFORCE_SVC
(
HOST_AGMT_NUM,
MBRSHP_TYPE_CD,
MBRSHP_NUM,
EFF_DT,
EXP_DT,
INC_DT,
BASE_ST,
LOB_CD,
PROD_DESC,
AGMT_ID,
AGMT_STS_CD,
TOT_PREM,
AGENT_NBR,
SVC_NUM,
AGENT_DESC,
CMPY,
PRCS_ID,
EOM_DATE
)
SELECT
EXPTRANS.HOST_AGMT_NUM as HOST_AGMT_NUM,
EXPTRANS.MBRSHP_TYPE_CD as MBRSHP_TYPE_CD,
EXPTRANS.MBRSHP_NUM as MBRSHP_NUM,
EXPTRANS.EFF_DTTM as EFF_DT,
EXPTRANS.EXP_DTTM as EXP_DT,
EXPTRANS.INC_DT as INC_DT,
EXPTRANS.BASE_ST as BASE_ST,
EXPTRANS.LOB_CD as LOB_CD,
EXPTRANS.PROD_DESC as PROD_DESC,
EXPTRANS.AGMT_ID as AGMT_ID,
EXPTRANS.AGMT_STS_CD as AGMT_STS_CD,
EXPTRANS.TOT_PREM as TOT_PREM,
EXPTRANS.AGENT_NUM as AGENT_NBR,
EXPTRANS.SVC_NUM as SVC_NUM,
EXPTRANS.AGENT_DESC as AGENT_DESC,
EXPTRANS.CMPY as CMPY,
EXPTRANS.PRCS_ID as PRCS_ID,
EXPTRANS.EOM_DT as EOM_DATE
FROM
EXPTRANS;


END;
';