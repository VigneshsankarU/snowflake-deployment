-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BIBASE_W_AGNT_SUMRY_INS("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare
  CAL_START_DT DATE;
  CAL_END_DT DATE;
  in_state INTEGER;

BEGIN 
cal_start_dt := current_date() -1;
cal_end_dt := current_date();
in_state := 1;




-- Component LKP_AGENT_HIERARCHY, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGENT_HIERARCHY AS
(
SELECT 
A.AGNT_NBR as AGNT_NBR, 
B.CAL_DT AS CAL_DT,
TRIM(A.ST_CD)||'':''||UPPER(TRIM(A.RGN_CD))||'':''||TRIM(A.DSTRCT_CD)||'':''||TRIM(A.SRVC_CTR_CD)||'':'' AS ST_RGN_DSTRCT_SRVC 
FROM db_v_prod_pres.WR_AGNT_HIERY AS A ,
(SELECT MAX(CALENDAR_DATE) AS CAL_DT FROM db_v_prod_smntc.CALENDAR WHERE 
CALENDAR_DATE >= :CAL_START_DT AND  CALENDAR_DATE<= :CAL_END_DT GROUP BY YEAR_OF_CALENDAR,MONTH_OF_YEAR)  AS B
WHERE A.EFF_DT <=B.CAL_DT 
AND SUBSTR(TRIM(A.AGNT_NBR),1,1)<>''-'' 
AND A.EFF_DT<=A.EXPRY_DT 
QUALIFY RANK() OVER(PARTITION BY  A.AGNT_NBR,B.CAL_DT ORDER BY A.TRMTN_DT DESC, A.EFF_DT DESC) = 1
);


-- Component LKP_LIFE_CURR_TIER, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_LIFE_CURR_TIER AS
(
SELECT 
T.CAL_DT AS CAL_DT,
T.AGENT_NBR AS AGENT_NBR,  
T.TIER_CD  AS TIER_CD FROM 
(
SELECT B.DT  AS CAL_DT,
CAST(CAST(A.AGENT_NBR AS INTEGER) AS VARCHAR(10)) AS AGENT_NBR,
TRIM(A.TIER_CD) AS TIER_CD 
FROM db_t_prod_comn.LIFE_CURR_TIER AS A, 
(SELECT MAX(YEAR_OF_CALENDAR*100+MONTH_OF_YEAR) AS MO_ID,MAX(CALENDAR_DATE) AS DT FROM db_v_prod_smntc.CALENDAR 
WHERE CALENDAR_DATE BETWEEN CAST(:CAL_START_DT AS DATE) AND CAST(:CAL_END_DT AS DATE) 
GROUP BY YEAR_OF_CALENDAR*100+MONTH_OF_YEAR) AS B WHERE A.CURR_YR*100+A.CURR_MTH<=B.MO_ID
AND (A.AGENT_NBR,A.CURR_YR,A.CURR_MTH) NOT IN 
(SELECT AGENT_NBR,CURR_YR,CURR_MTH FROM db_t_prod_comn.LIFE_CURR_TIER GROUP BY 1,2,3 HAVING COUNT(DISTINCT TIER_CD)>1)
QUALIFY ROW_NUMBER() OVER(PARTITION BY A.AGENT_NBR,B.MO_ID ORDER BY A.CURR_YR DESC,A.CURR_MTH DESC) =1
) AS T
);


-- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT AS
(
SELECT 
TRIM(TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS) as SRC_IDNTFTN_SYS, 
TRIM(TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM) as SRC_IDNTFTN_NM, 
TRIM(TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL) as SRC_IDNTFTN_VAL, 
TRIM(TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM) as TGT_IDNTFTN_NM, 
TRIM(TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL) as TGT_IDNTFTN_VAL, 
TRIM(TERADATA_ETL_REF_XLAT.EXPN_DT) as EXPN_DT, 
TRIM(TERADATA_ETL_REF_XLAT.EFF_DT) as EFF_DT 
FROM db_t_prod_core.TERADATA_ETL_REF_XLAT
);


-- PIPELINE START FOR 1

-- Component sq_wr_agent_summ_dummy, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_wr_agent_summ_dummy AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as cal_dt,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT CAL_DT FROM  DB_V_PROD_PRES.WR_AGNT_SUMRY  WHERE CAL_DT BETWEEN :CAL_START_DT AND :CAL_END_DT GROUP BY CAL_DT
) SRC
)
);


-- Component del_wr_agnt_sumry, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE del_wr_agnt_sumry AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
sq_wr_agent_summ_dummy.cal_dt as cal_dt,
1 as UPDATE_STRATEGY_ACTION
FROM
sq_wr_agent_summ_dummy
);


-- Component tgt_wr_agnt_sumry_del, Type TARGET 
/* Perform Updates */

DELETE FROM
  DB_V_PROD_PRES.WR_AGNT_SUMRY USING del_wr_agnt_sumry
WHERE
  WR_AGNT_SUMRY.CAL_DT = del_wr_agnt_sumry.CAL_DT;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component sq_tpc00201_prdcrd_y, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_tpc00201_prdcrd_y AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as eom_yyyy,
$2 as eom_mm,
$3 as region,
$4 as district,
$5 as service_center,
$6 as agent,
$7 as app_credit,
$8 as prem_cred_change,
$9 as source_record_id
FROM (
SELECT
  SRC.*,
  ROW_NUMBER() OVER (
    ORDER BY
      1
  ) AS source_record_id
FROM
  (
    SELECT
      EOM_YYYY,
      EOM_MM,
      REGION,
      DISTRICT,
      SERVICE_CENTER,
      AGENT,
      SUM(APP_CREDIT) AS APP_CREDIT,
      SUM(PREM_CRED_CHANGE) AS PREM_CRED_CHANGE
    FROM
      DB_T_PROD_COMN.TPC00201_PRDCRD_Y
    WHERE
      ADD_MONTHS(
        TO_DATE(
          CAST(EOM_YYYY * 100 + EOM_MM AS VARCHAR),
          ''yyyymm''
        ),
        1
      ) - 1 >= :CAL_START_DT
      AND ADD_MONTHS(
        TO_DATE(
          CAST(EOM_YYYY * 100 + EOM_MM AS VARCHAR),
          ''yyyymm''
        ),
        1
      ) - 1 <= :CAL_END_DT
    GROUP BY
      EOM_YYYY,
      EOM_MM,
      REGION,
      DISTRICT,
      SERVICE_CENTER,
      AGENT
    HAVING
      SUM(APP_CREDIT) <> 0
      OR SUM(PREM_CRED_CHANGE) <> 0
  ) AS SRC
)
);


-- PIPELINE START FOR 2

-- Component sq_commissions, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_commissions AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as accounting_dt,
$2 as policy_symbol,
$3 as state_nbr,
$4 as service_center,
$5 as agent_nbr,
$6 as amount,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT  

ACCOUNTING_DT,

POLICY_SYMBOL,

STATE_NBR,

SERVICE_CENTER,

AGENT_NBR,

SUM(AMOUNT) AS AMOUNT

FROM DB_T_PROD_COMN.COMMISSIONS 

WHERE 

ACCOUNTING_DT >=:CAL_START_DT  

AND ACCOUNTING_DT <=:CAL_END_DT 

GROUP BY 

ACCOUNTING_DT,

POLICY_SYMBOL,

STATE_NBR,

SERVICE_CENTER,

AGENT_NBR 

HAVING SUM(AMOUNT) <> 0
) SRC
)
);


-- PIPELINE START FOR 2

-- Component sq_tpc00501_level, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_tpc00501_level AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as year1,
$2 as month1,
$3 as region,
$4 as district,
$5 as agent,
$6 as nb_prem,
$7 as nb_commpd,
$8 as source_record_id
FROM (
SELECT
  SRC.*,
  ROW_NUMBER() OVER (
    ORDER BY
      1
  ) AS source_record_id
FROM
  (
    SELECT
      YEAR1,
      MONTH1,
      REGION,
      DISTRICT,
      AGENT,
      SUM(NB_PREM) AS NB_PREM,
      SUM(NB_COMMPD) AS NB_COMMPD
    FROM
      DB_T_PROD_COMN.TPC00501_LEVEL
    WHERE
      ADD_MONTHS(
        TO_DATE(CAST(YEAR1 * 100 + MONTH1 AS VARCHAR), ''yyyymm''),
        1
      ) - 1 >= :CAL_START_DT
      AND ADD_MONTHS(
        TO_DATE(CAST(YEAR1 * 100 + MONTH1 AS VARCHAR), ''yyyymm''),
        1
      ) - 1 <= :CAL_END_DT
    GROUP BY
      YEAR1,
      MONTH1,
      REGION,
      DISTRICT,
      AGENT
    HAVING
      SUM(NB_PREM) <> 0
      OR SUM(NB_COMMPD) <> 0
  ) AS SRC
)
);


-- PIPELINE START FOR 2

-- Component sq_agt_auto, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_agt_auto AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as app_year,
$2 as app_month,
$3 as region,
$4 as district,
$5 as agent_nbr,
$6 as apps_a,
$7 as source_record_id
FROM (
SELECT
  SRC.*,
  ROW_NUMBER() OVER (
    ORDER BY
      1
  ) AS source_record_id
FROM
  (
    SELECT
      APP_YEAR,
      APP_MONTH,
      REGION,
      DISTRICT,
      AGENT_NBR,
      SUM(APPS_A) AS APPS_A
    FROM
      DB_T_PROD_COMN.AGT_AUTO
    WHERE
      ADD_MONTHS(
        TO_DATE(
          CAST(APP_YEAR * 100 + APP_MONTH + 200000 AS VARCHAR),
          ''yyyymm''
        ),
        1
      ) - 1 >= :CAL_START_DT
      AND ADD_MONTHS(
        TO_DATE(
          CAST(APP_YEAR * 100 + APP_MONTH + 200000 AS VARCHAR),
          ''yyyymm''
        ),
        1
      ) - 1 <= :CAL_END_DT
    GROUP BY
      APP_YEAR,
      APP_MONTH,
      REGION,
      DISTRICT,
      AGENT_NBR
    HAVING
      SUM(APPS_A) <> 0
  ) AS SRC
)
);


-- PIPELINE START FOR 2

-- Component sq_member_mstr_trans, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_member_mstr_trans AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as cal_dt,
$2 as memb_orig_sales_agent_sc_nbr,
$3 as memb_orig_sales_agent_nbr,
$4 as memb_orig_sales_user_id,
$5 as memb_num,
$6 as memb_eff_dt,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 

CAL_DT,

MEMB_ORIG_SALES_AGENT_SC_NBR, 

MEMB_ORIG_SALES_AGENT_NBR,

CASE WHEN UPPER(MEMB_ORIG_SALES_USER_ID) LIKE ''ALFACORP%'' THEN SUBSTR(UPPER(MEMB_ORIG_SALES_USER_ID),10,20) 

WHEN POSITION(''-'' IN MEMB_ORIG_SALES_USER_ID)>0 THEN SUBSTR(UPPER(MEMB_ORIG_SALES_USER_ID),1,POSITION(''-'' IN MEMB_ORIG_SALES_USER_ID)-1)

WHEN COALESCE(MEMB_ORIG_SALES_USER_ID,'''')='''' THEN ''UNK'' 

ELSE UPPER(MEMB_ORIG_SALES_USER_ID) END AS USER_ID,

X.MEMB_NUM, 

X.MEMB_EFF_DT

FROM (

SELECT 
ADD_MONTHS (
  TO_DATE (TO_CHAR (A.MEMB_EFF_DT, ''YYYYMM''), ''yyyymm''),
  1
) - 1 AS CAL_DT,

MEMB_ORIG_SALES_AGENT_SC_NBR, 

MEMB_ORIG_SALES_AGENT_NBR,

MEMB_ORIG_SALES_USER_ID,

A.MEMB_NUM,

cast(A.MEMB_EFF_DT  AS DATE) MEMB_EFF_DT

FROM (SELECT MEMB_SKEY, MEMB_NUM,MAX(COALESCE(MEMB_ORIG_SALES_AGENT_SC_NBR,0)) over(partition by MEMB_NUM order by MEMB_EFF_dt desc) MEMB_ORIG_SALES_AGENT_SC_NBR, 

MAX(COALESCE(MEMB_ORIG_SALES_AGENT_NBR,0))  over(partition by MEMB_NUM order by MEMB_EFF_dt desc)  MEMB_ORIG_SALES_AGENT_NBR,

MAX(COALESCE(MEMB_ORIG_SALES_USER_ID,''0''))  over(partition by MEMB_NUM order by MEMB_EFF_dt desc)  MEMB_ORIG_SALES_USER_ID,

MIN(MEMB_EFF_DT)  over(partition by MEMB_NUM order by MEMB_EFF_dt asc)  MEMB_EFF_DT, MAX(MEMB_EXP_DT)   over(partition by MEMB_NUM order by MEMB_EFF_dt desc) MEMB_EXP_DT 

FROM DB_T_CORE_PROD.MEMBER_MSTR WHERE MEMB_TREX_BUS_WRT_AGT <> ''TREXIS''  ) AS A, 

DB_T_CORE_PROD.MEMBER_TRANS AS B 

WHERE A.MEMB_SKEY = B.MEMB_SKEY 

AND B.STATUS_CD IN (0,1,9) 

/* AND B.COMM_CD1 || B.COMM_CD2 || B.COMM_CD3 <> ''XXY''  */
AND B.MEMB_TYPE IN (''A'', ''C'')

AND CAL_DT BETWEEN CAST(CHG_EFF_DT AS DATE) AND CAST(CHG_EXP_DT AS DATE)

AND CAL_DT BETWEEN CAST(:CAL_START_DT AS DATE) AND CAST(:CAL_END_DT AS DATE)

GROUP BY 1,2,3,4,5,6



UNION ALL 



SELECT 

REIN_ALL.CURR_CAL_END_DT AS CAL_DT, 

REIN_GT_365.SC_NUMBER,

REIN_GT_365.AGENT_NBR,

REIN_GT_365.USER_ID, 

REIN_ALL.MEMB_NUM,

REIN_ALL.MEMB_EFF_DT 

FROM 

(SELECT CURR_CAL_END_DT,MO_ID, A.MEMB_NUM,A.MEMB_SKEY, CAST(A.MEMB_EFF_DT AS DATE) MEMB_EFF_DT,T.PREV_CAL_END_DT,

MAX(CASE WHEN (CAST(A.MEMB_EXP_DT AS DATE) >T.PREV_CAL_END_DT AND T.PREV_CAL_END_DT BETWEEN CAST(B.CHG_EFF_DT AS DATE) AND CAST(B.CHG_EXP_DT AS DATE)) THEN ''Y'' ELSE ''N'' END) AS BIF,

MAX(CASE WHEN (CAST(A.MEMB_EXP_DT AS DATE) >T.CURR_CAL_END_DT AND T.CURR_CAL_END_DT BETWEEN CAST(B.CHG_EFF_DT AS DATE) AND CAST(B.CHG_EXP_DT AS DATE)) THEN ''Y'' ELSE ''N'' END) AS EIF,

CASE WHEN BIF=''N''  AND  EIF = ''Y'' AND CAST(A.MEMB_EFF_DT AS DATE)<=T.PREV_CAL_END_DT  THEN ''Y'' ELSE ''N'' END AS REIN

FROM 

(SELECT MEMB_SKEY, MEMB_NUM,MAX(COALESCE(MEMB_ORIG_SALES_AGENT_SC_NBR,0)) over(partition by MEMB_NUM order by MEMB_EFF_dt desc) MEMB_ORIG_SALES_AGENT_SC_NBR, 

MAX(COALESCE(MEMB_ORIG_SALES_AGENT_NBR,0))  over(partition by MEMB_NUM order by MEMB_EFF_dt desc)  MEMB_ORIG_SALES_AGENT_NBR,

MAX(COALESCE(MEMB_ORIG_SALES_USER_ID,''0''))  over(partition by MEMB_NUM order by MEMB_EFF_dt desc)  MEMB_ORIG_SALES_USER_ID,

MIN(MEMB_EFF_DT)  over(partition by MEMB_NUM order by MEMB_EFF_dt asc)  MEMB_EFF_DT, MAX(MEMB_EXP_DT)   over(partition by MEMB_NUM order by MEMB_EFF_dt desc) MEMB_EXP_DT 

FROM DB_T_CORE_PROD.MEMBER_MSTR WHERE MEMB_TREX_BUS_WRT_AGT <> ''TREXIS'' ) A, DB_T_CORE_PROD.MEMBER_TRANS B,

(SELECT EXTRACT(YEAR FROM CALENDAR_DATE)*100+EXTRACT(MONTH FROM CALENDAR_DATE) AS MO_ID, 

MIN(CALENDAR_DATE)-1 AS PREV_CAL_END_DT,

MAX(CALENDAR_DATE) AS CURR_CAL_END_DT

FROM db_v_prod_smntc.CALENDAR  

WHERE CALENDAR_DATE BETWEEN CAST(:CAL_START_DT AS DATE) AND CAST(:CAL_END_DT AS DATE)

GROUP BY MO_ID ) T 

WHERE A.MEMB_SKEY = B.MEMB_SKEY

AND (

(CAST(A.MEMB_EXP_DT AS DATE) >T.PREV_CAL_END_DT AND T.PREV_CAL_END_DT BETWEEN CAST(B.CHG_EFF_DT AS DATE) AND CAST(B.CHG_EXP_DT AS DATE)) OR 

(CAST(A.MEMB_EXP_DT AS DATE) >T.CURR_CAL_END_DT AND T.CURR_CAL_END_DT BETWEEN CAST(B.CHG_EFF_DT AS DATE) AND CAST(B.CHG_EXP_DT AS DATE)) 

)

AND B.STATUS_CD IN (0,1,9)

/* AND B.COMM_CD1 || B.COMM_CD2 || B.COMM_CD3 <> ''XXY'' */
AND B.MEMB_TYPE IN (''A'', ''C'')

GROUP BY 1,2,3,4,5,6

HAVING REIN = ''Y'' ) REIN_ALL 





 JOIN 



(SELECT EXTRACT( YEAR FROM NEW_CHG_EFF_DT)*100+EXTRACT( MONTH FROM NEW_CHG_EFF_DT) AS MO_ID, MEMB_NUM,AGENT_NBR,SC_NUMBER,USER_ID 

FROM (SELECT A.MEMB_SKEY,A.MEMB_NUM,CAST(A.MEMB_EFF_DT AS DATE)MEMB_EFF_DT ,AGENT_NBR,SC_NUMBER,USER_ID,

CASE WHEN CAST(A.MEMB_EXP_DT AS DATE)<CAST(B.CHG_EFF_DT AS DATE) THEN CAST(A.MEMB_EXP_DT AS DATE) ELSE CAST(B.CHG_EFF_DT AS DATE) END AS NEW_CHG_EFF_DT,

CASE WHEN CAST(A.MEMB_EXP_DT AS DATE)<CAST(B.CHG_EXP_DT AS DATE) THEN CAST(A.MEMB_EXP_DT AS DATE) ELSE CAST(B.CHG_EXP_DT AS DATE) END AS NEW_CHG_EXP_DT,

COALESCE(MAX(NEW_CHG_EXP_DT) OVER(PARTITION BY A.MEMB_SKEY ORDER BY NEW_CHG_EFF_DT ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING),CAST(MEMB_EFF_DT AS DATE)) AS PREV_CHG_EXP_DT,

NEW_CHG_EFF_DT-PREV_CHG_EXP_DT AS DAYS 

FROM (SELECT MEMB_SKEY, MEMB_NUM,MAX(COALESCE(MEMB_ORIG_SALES_AGENT_SC_NBR,0)) over(partition by MEMB_NUM order by MEMB_EFF_dt desc) MEMB_ORIG_SALES_AGENT_SC_NBR, 

MAX(COALESCE(MEMB_ORIG_SALES_AGENT_NBR,0))  over(partition by MEMB_NUM order by MEMB_EFF_dt desc)  MEMB_ORIG_SALES_AGENT_NBR,

MAX(COALESCE(MEMB_ORIG_SALES_USER_ID,''0''))  over(partition by MEMB_NUM order by MEMB_EFF_dt desc)  MEMB_ORIG_SALES_USER_ID,

MIN(MEMB_EFF_DT)  over(partition by MEMB_NUM order by MEMB_EFF_dt asc)  MEMB_EFF_DT, MAX(MEMB_EXP_DT)   over(partition by MEMB_NUM order by MEMB_EFF_dt desc) MEMB_EXP_DT 

FROM DB_T_CORE_PROD.MEMBER_MSTR WHERE MEMB_TREX_BUS_WRT_AGT <> ''TREXIS''  ) A, DB_T_CORE_PROD.MEMBER_TRANS B,

(SELECT CAST(MEMBERNUM AS INTEGER) AS MEMB_NUM,COALESCE(DATE_REQUESTED,CAST(''1900-01-01'' AS DATE)) AS STRT_DT,AGENT_NBR,SC_NUMBER,USER_ID,

COALESCE(MAX(DATE_REQUESTED) OVER(PARTITION BY MEMBERNUM ORDER BY MEMBER_SEQ ROWS BETWEEN 1 FOLLOWING AND 1 FOLLOWING)-1,CAST(''9999-12-31'' AS DATE)) AS END_DT

FROM DB_T_PROD_COMN.GETMEMBERNUM QUALIFY STRT_DT<=END_DT)C  

WHERE A.MEMB_SKEY = B.MEMB_SKEY

AND B.STATUS_CD IN (0,1,9)

/* AND B.COMM_CD1 || B.COMM_CD2 || B.COMM_CD3 <> ''XXY'' */
AND B.MEMB_TYPE IN (''A'', ''C'') 

AND A.MEMB_NUM=C.MEMB_NUM 

AND CAST(B.CHG_EFF_DT AS DATE) BETWEEN C.STRT_DT AND C.END_DT

GROUP BY 1,2,3,4,5,6,7,8

QUALIFY DAYS>365) T  

WHERE
  ADD_MONTHS(TO_DATE(CAST(MO_ID AS VARCHAR), ''YYYYMM''), 1) - 1 BETWEEN :CAL_START_DT
  AND :CAL_END_DT

GROUP BY 1,2,3,4,5

) REIN_GT_365

ON REIN_ALL.MO_ID=REIN_GT_365.MO_ID AND 

REIN_ALL.MEMB_NUM=REIN_GT_365.MEMB_NUM 

GROUP BY 1,2,3,4,5,6 

) X GROUP BY 1,2,3,4,5,6
) SRC
)
);


-- Component exp_cleanse_agt_auto, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_cleanse_agt_auto AS
(
SELECT
LAST_DAY ( TO_DATE ( TO_CHAR ( ( sq_agt_auto.app_year * 10000 ) + 20000000 + ( sq_agt_auto.app_month * 100 ) + 1 ) , ''YYYYMMDD'' ) ) as var_cal_dt,
var_cal_dt as out_cal_dt,
''PA'' as out_lob_cd,
LTRIM ( RTRIM ( TO_CHAR ( TO_NUMBER(sq_agt_auto.agent_nbr) ) ) ) as var_agent_nbr,
LTRIM ( RTRIM ( LKP_1.ST_RGN_DSTRCT_SRVC /* replaced lookup LKP_AGENT_HIERARCHY */ ) ) as var_lkp_st_rgn_dstrct_srvc,
POSITION('':'',var_lkp_st_rgn_dstrct_srvc) as var_lkp_colon_pos1,
REGEXP_INSTR ( var_lkp_st_rgn_dstrct_srvc , '':'' , 1 , 2 ) as var_lkp_colon_pos2,
REGEXP_INSTR ( var_lkp_st_rgn_dstrct_srvc , '':'' , 1 , 3 ) as var_lkp_colon_pos3,
REGEXP_INSTR ( var_lkp_st_rgn_dstrct_srvc , '':'' , 1 , 4 ) as var_lkp_colon_pos4,
SUBSTR ( var_lkp_st_rgn_dstrct_srvc , 1 , var_lkp_colon_pos1 - 1 ) as var_lkp_st_cd,
SUBSTR ( var_lkp_st_rgn_dstrct_srvc , var_lkp_colon_pos3 + 1 , var_lkp_colon_pos4 - var_lkp_colon_pos3 - 1 ) as var_lkp_srvc_ctr_cd,
LTRIM ( RTRIM ( sq_agt_auto.region ) ) as var_region,
CASE
  WHEN var_region IN (''1'', ''2'') THEN ''AL''
  WHEN var_region = ''3'' THEN ''AL''
  WHEN var_region = ''5'' THEN ''GA''
  WHEN var_region = ''6'' THEN ''MS''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL THEN var_lkp_st_cd
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NULL THEN ''UNK''
END as out_st_cd,
CASE
  WHEN var_region = ''1'' THEN ''ALABAMA NORTH MARKETING''
  WHEN var_region = ''2'' THEN ''ALABAMA SOUTH MARKETING''
  WHEN var_region = ''3'' THEN ''ALABAMA IE''
  WHEN var_region = ''5'' THEN ''GEORGIA MARKETING''
  WHEN var_region = ''6'' THEN ''MISSISSIPPI MARKETING''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL
  AND var_lkp_st_cd = ''AL'' THEN ''CRC-AL''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL
  AND var_lkp_st_cd = ''GA'' THEN ''CRC-GA''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL
  AND var_lkp_st_cd = ''MS'' THEN ''CRC-MS''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NULL THEN ''UNK''
END as out_rgn_cd,
LTRIM ( RTRIM ( sq_agt_auto.district ) ) as var_district,
--DECODE ( TRUE , IS_INTEGER(var_district) , var_district || ''-'' || var_region , var_district || var_region ) as out_dstrct_cd,
CASE
  WHEN try_to_number(var_district) IS NOT NULL THEN var_district || ''-'' || var_region
  ELSE var_district || var_region
END AS out_dstrct_cd,
CASE WHEN NOT ( var_lkp_st_rgn_dstrct_srvc IS NULL ) THEN var_lkp_srvc_ctr_cd ELSE ''UNK'' END as out_srvc_ctr_cd,
var_agent_nbr as out_agnt_nbr,
NULL as out_user_id,
NULL as out_mbrshp_type_cd,
NULL as out_mbrshp_num,
NULL as out_mbrshp_eff_dt,
0 as out_plcy_cnt_qut,
0 as out_plcy_cnt_new_busi,
0 as out_prev_mtd_plcy_cnt,
0 as out_persis_plcy_cnt,
sq_agt_auto.apps_a as out_apld_cd,
0 as out_wrtn_prem,
0 as out_pd_prem,
0 as out_new_busn_prem,
0 as out_pd_coms,
0 as out_new_mbrshp_cnt,
sq_agt_auto.source_record_id,
row_number() over (partition by sq_agt_auto.source_record_id order by sq_agt_auto.source_record_id) as RNK
FROM
sq_agt_auto
LEFT JOIN LKP_AGENT_HIERARCHY LKP_1 ON LKP_1.AGNT_NBR = var_agent_nbr AND LKP_1.CAL_DT = var_cal_dt
QUALIFY RNK = 1
);


-- PIPELINE START FOR 2

-- Component sq_prod_cred_Detail, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_prod_cred_Detail AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as eom_date,
$2 as policy_symbol,
$3 as state,
$4 as region,
$5 as district,
$6 as svc_cntr,
$7 as agent_nbr,
$8 as app_credit,
$9 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 

EOM_DATE	,

POLICY_SYMBOL, 

STATE,

REGION,

DISTRICT,

SVC_CNTR,

AGENT_NBR,

SUM(APP_CREDIT)  AS APP_CREDIT 

FROM   DB_T_PROD_COMN.PROD_CRED_DETAIL

WHERE 

EOM_DATE >=  :CAL_START_DT  

AND EOM_DATE <=  :CAL_END_DT  

AND POLICY_SYMBOL NOT IN (''A'',''A0'',''APV'',''G'',''G0'',''N'',''N0'',''F0'') 

GROUP BY 

EOM_DATE	,

POLICY_SYMBOL, 

STATE,

REGION,

DISTRICT,

SVC_CNTR,

AGENT_NBR 

HAVING SUM(APP_CREDIT) <> 0
) SRC
)
);


-- Component exp_cleanse_member_mstr_trans, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_cleanse_member_mstr_trans AS
(
SELECT
sq_member_mstr_trans.cal_dt as cal_dt,
''UNK'' as out_lob_cd,
LTRIM ( RTRIM ( TO_CHAR ( sq_member_mstr_trans.memb_orig_sales_agent_nbr ) ) ) as var_agnt_nbr,
LTRIM ( RTRIM ( LKP_1.ST_RGN_DSTRCT_SRVC /* replaced lookup LKP_AGENT_HIERARCHY */ ) ) as var_lkp_st_rgn_dstrct_srvc,
POSITION('':'',var_lkp_st_rgn_dstrct_srvc) as var_lkp_colon_pos1,
REGEXP_INSTR ( var_lkp_st_rgn_dstrct_srvc , '':'' , 1 , 2 ) as var_lkp_colon_pos2,
REGEXP_INSTR ( var_lkp_st_rgn_dstrct_srvc , '':'' , 1 , 3 ) as var_lkp_colon_pos3,
REGEXP_INSTR ( var_lkp_st_rgn_dstrct_srvc , '':'' , 1 , 4 ) as var_lkp_colon_pos4,
CASE WHEN var_lkp_st_rgn_dstrct_srvc IS NULL THEN ''UNK'' ELSE SUBSTR ( var_lkp_st_rgn_dstrct_srvc , 1 , var_lkp_colon_pos1 - 1 ) END as out_lkp_st_cd,
CASE WHEN var_lkp_st_rgn_dstrct_srvc IS NULL THEN ''UNK'' ELSE SUBSTR ( var_lkp_st_rgn_dstrct_srvc , var_lkp_colon_pos1 + 1 , var_lkp_colon_pos2 - var_lkp_colon_pos1 - 1 ) END as out_lkp_rgn_cd,
CASE WHEN var_lkp_st_rgn_dstrct_srvc IS NULL THEN ''UNK'' ELSE SUBSTR ( var_lkp_st_rgn_dstrct_srvc , var_lkp_colon_pos2 + 1 , var_lkp_colon_pos3 - var_lkp_colon_pos2 - 1 ) END as out_lkp_dstrct_cd,
SUBSTR ( var_lkp_st_rgn_dstrct_srvc , var_lkp_colon_pos3 + 1 , var_lkp_colon_pos4 - var_lkp_colon_pos3 - 1 ) as var_lkp_srvc_ctr_cd,
LTRIM ( RTRIM ( TO_CHAR ( sq_member_mstr_trans.memb_orig_sales_agent_sc_nbr ) ) ) as var_srvc_ctr_cd,
DECODE ( TRUE , var_srvc_ctr_cd <> ''0'' , var_srvc_ctr_cd , var_srvc_ctr_cd = ''0'' AND NOT ( var_lkp_st_rgn_dstrct_srvc IS NULL ) , var_lkp_srvc_ctr_cd , var_srvc_ctr_cd = ''0'' AND var_lkp_st_rgn_dstrct_srvc IS NULL , ''UNK'' , var_srvc_ctr_cd IS NULL , ''UNK'' ) as out_srvc_ctr_cd,
CASE WHEN var_agnt_nbr IS NULL THEN ''UNK'' ELSE var_agnt_nbr END as out_agnt_nbr,
LTRIM ( RTRIM ( sq_member_mstr_trans.memb_orig_sales_user_id ) ) as out_user_id,
''MBRSHP'' as out_mbrshp_type_cd,
LPAD ( TO_CHAR ( sq_member_mstr_trans.memb_num ) , 8 , ''0'' ) as out_mbrshp_num,
sq_member_mstr_trans.memb_eff_dt as mbrshp_eff_dt,
0 as out_plcy_cnt_qut,
0 as out_plcy_cnt_new_busi,
0 as out_prev_mtd_plcy_cnt,
0 as out_persis_plcy_cnt,
0 as out_apld_cd,
0 as out_wrtn_prem,
0 as out_pd_prem,
0 as out_new_busn_prem,
0 as out_pd_coms,
1 as out_new_mbrshp_cnt,
sq_member_mstr_trans.source_record_id,
row_number() over (partition by sq_member_mstr_trans.source_record_id order by sq_member_mstr_trans.source_record_id) as RNK
FROM
sq_member_mstr_trans
LEFT JOIN LKP_AGENT_HIERARCHY LKP_1 ON LKP_1.AGNT_NBR = var_agnt_nbr AND LKP_1.CAL_DT = sq_member_mstr_trans.cal_dt
QUALIFY RNK = 1
);


-- PIPELINE START FOR 2

-- Component sq_agt_fire, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_agt_fire AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as app_year,
$2 as app_month,
$3 as region,
$4 as district,
$5 as agent_nbr,
$6 as apps_f,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 

APP_YEAR,

APP_MONTH,

REGION,

DISTRICT,

AGENT_NBR,

SUM(APPS_F) AS APPS_F 

FROM DB_T_PROD_COMN.AGT_FIRE  

WHERE
  ADD_MONTHS(
    TO_DATE(
      CAST(APP_YEAR * 100 + APP_MONTH + 200000 AS VARCHAR),
      ''YYYYMM''
    ),
    1
  ) - 1 >= :CAL_START_DT
  AND ADD_MONTHS(
    TO_DATE(
      CAST(APP_YEAR * 100 + APP_MONTH + 200000 AS VARCHAR),
      ''YYYYMM''
    ),
    1
  ) - 1 <= :CAL_END_DT

GROUP BY 

APP_YEAR,

APP_MONTH,

REGION,

DISTRICT,

AGENT_NBR

HAVING 

SUM(APPS_F) <> 0
) SRC
)
);


-- PIPELINE START FOR 2

-- Component sq_wr_quotes_bounds_persistency, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_wr_quotes_bounds_persistency AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as calendar_date,
$2 as lob_cd,
$3 as st_cd,
$4 as rgn_cd,
$5 as dst_cd,
$6 as svc_cd,
$7 as agnt_nbr,
$8 as plcy_cnt_quot,
$9 as plcy_cnt_new_busi,
$10 as prev_mtd_plcy_cnt,
$11 as persis_plcy_cnt,
$12 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 

CAL_DT AS CALENDAR_DATE,

LOB_CD,

ST_CD,

RGN_CD,

DST_CD,

SVC_CD,

AGNT_NBR,

SUM(QT) AS PLCY_CNT_QUOT,

SUM(NB) AS PLCY_CNT_NEW_BUSI,

SUM(PREV_INF_CNT) AS PREV_MTD_PLCY_CNT,

SUM(PERSIS_PLCY_CNT) AS PERSISTENCY_PLCY_CNT 

FROM (

SELECT 

ADD_MONTHS (
  TO_DATE (
    TO_CHAR (
      DATE_PART (YEAR, CALENDAR_DATE) * 100 + DATE_PART (MONTH, CALENDAR_DATE),
      ''000000''
    ),
    ''yyyymm''
  ),
  1
) - 1 AS CAL_DT,

LOB_CD,

ST_CD,

RGN_CD,

DST_CD,

SVC_CD,

AGENT_NBR AS AGNT_NBR,

SUM(CASE WHEN QUOTE_IND = ''Q'' THEN PLCY_CNT ELSE 0 END) AS QT,

SUM(CASE WHEN QUOTE_IND = ''N'' THEN PLCY_CNT ELSE 0 END) AS NB,

CAST(0 AS BIGINT) AS PREV_INF_CNT,

CAST(0 AS BIGINT)  AS PERSIS_PLCY_CNT

FROM DB_V_PROD_ANLTC.WR_QUOTES_DTL_CD 

WHERE CALENDAR_DATE >= :CAL_START_DT AND CALENDAR_DATE <= :CAL_END_DT

GROUP BY 1,2,3,4,5,6,7

UNION ALL 

SELECT 

CAL_DT,

LOB_CD,

ST_CD,

RGN_CD,

DSTRCT_CD,

SRVC_CTR_CD,

AGNT_NUM,

0 AS PLCY_CNT_QUT,

0 AS PLCY_CNT_NEW_BUSI,

SUM(PREV_MTD_PLCY_CNT) AS PREV_INF_CNT,

SUM(PERSISTENCY_PLCY_CNT) AS PERSIS_PLCY_CNT 

FROM DB_V_PROD_PRES.WR_PERSISTENCY_DTL 

WHERE CAL_DT >= :CAL_START_DT AND CAL_DT <= :CAL_END_DT

GROUP BY 1,2,3,4,5,6,7) T 

GROUP BY 1,2,3,4,5,6,7 

HAVING 

PLCY_CNT_QUOT <> 0 OR

PLCY_CNT_NEW_BUSI <> 0 OR

PREV_MTD_PLCY_CNT <> 0 OR

PERSISTENCY_PLCY_CNT <> 0
) SRC
)
);


-- Component exp_cleanse_wr_quotes_bounds_persistency, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_cleanse_wr_quotes_bounds_persistency AS
(
SELECT
sq_wr_quotes_bounds_persistency.calendar_date as cal_dt,
CASE WHEN sq_wr_quotes_bounds_persistency.lob_cd IS NULL OR LTRIM ( RTRIM ( sq_wr_quotes_bounds_persistency.lob_cd ) ) = ''NA'' THEN ''UNK'' ELSE LTRIM ( RTRIM ( sq_wr_quotes_bounds_persistency.lob_cd ) ) END as out_lob_cd,
CASE WHEN sq_wr_quotes_bounds_persistency.st_cd IS NULL OR LTRIM ( RTRIM ( sq_wr_quotes_bounds_persistency.st_cd ) ) = ''NA'' THEN ''UNK'' ELSE LTRIM ( RTRIM ( sq_wr_quotes_bounds_persistency.st_cd ) ) END as out_st_cd,
CASE WHEN sq_wr_quotes_bounds_persistency.rgn_cd IS NULL OR LTRIM ( RTRIM ( sq_wr_quotes_bounds_persistency.rgn_cd ) ) = ''NA'' THEN ''UNK'' ELSE LTRIM ( RTRIM ( sq_wr_quotes_bounds_persistency.rgn_cd ) ) END as out_rgn_cd,
CASE WHEN sq_wr_quotes_bounds_persistency.dst_cd IS NULL OR LTRIM ( RTRIM ( sq_wr_quotes_bounds_persistency.dst_cd ) ) = ''NA'' THEN ''UNK'' ELSE LTRIM ( RTRIM ( sq_wr_quotes_bounds_persistency.dst_cd ) ) END as out_dstrct_cd,
CASE WHEN sq_wr_quotes_bounds_persistency.svc_cd IS NULL OR LTRIM ( RTRIM ( sq_wr_quotes_bounds_persistency.svc_cd ) ) = ''NA'' THEN ''UNK'' ELSE LTRIM ( RTRIM ( sq_wr_quotes_bounds_persistency.svc_cd ) ) END as out_srvc_ctr_cd,
CASE WHEN sq_wr_quotes_bounds_persistency.agnt_nbr IS NULL OR LTRIM ( RTRIM ( sq_wr_quotes_bounds_persistency.agnt_nbr ) ) = ''NA'' THEN ''UNK'' ELSE LTRIM ( RTRIM ( sq_wr_quotes_bounds_persistency.agnt_nbr ) ) END as out_agnt_nbr,
NULL as out_user_id,
NULL as out_mbrshp_type_cd,
NULL as out_mbrshp_num,
NULL as out_mbrshp_eff_dt,
sq_wr_quotes_bounds_persistency.plcy_cnt_quot as new_quot_cnt,
sq_wr_quotes_bounds_persistency.plcy_cnt_new_busi as new_bnd_cnt,
sq_wr_quotes_bounds_persistency.prev_mtd_plcy_cnt as pyr_infrc_plcy_cnt,
sq_wr_quotes_bounds_persistency.persis_plcy_cnt as prsistnt_plcy_cnt,
0 as out_apld_cr,
0 as out_wrtn_prem,
0 as out_pd_prem,
0 as out_new_busn_prem,
0 as out_pd_coms,
0 as out_new_mbrshp_cnt,
sq_wr_quotes_bounds_persistency.source_record_id
FROM
sq_wr_quotes_bounds_persistency
);


-- Component exp_cleanse_commissions, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_cleanse_commissions AS
(
SELECT
sq_commissions.accounting_dt as cal_dt,
LTRIM ( RTRIM ( sq_commissions.policy_symbol ) ) as var_policy_symbol,
CASE
  WHEN var_policy_symbol IN (''A'', ''A0'', ''APV'', ''G'', ''G0'') THEN ''PA''
  WHEN var_policy_symbol IN (''N'', ''N0'') THEN ''ASIC''
  WHEN var_policy_symbol = ''CP'' THEN ''COMPPERSL''
  WHEN var_policy_symbol = ''F0'' THEN ''SF''
  WHEN var_policy_symbol = ''FC'' THEN ''FCL''
  WHEN var_policy_symbol = ''GL'' THEN ''UMBRELLA''
  WHEN var_policy_symbol = ''H0'' THEN ''HO''
  WHEN var_policy_symbol = ''M0'' THEN ''MACHINERY''
  WHEN var_policy_symbol = ''R0'' THEN ''RFD''
  WHEN var_policy_symbol = ''SC'' THEN ''PORTFOLIO''
  WHEN var_policy_symbol = ''SM'' THEN ''BOP / CHURCH''
  WHEN var_policy_symbol = ''SP'' THEN ''SCHDPROP''
  WHEN var_policy_symbol = ''T0'' THEN ''MH''
  ELSE ''UNK''
END AS out_lob_cd,
LTRIM ( RTRIM ( TO_CHAR ( TO_NUMBER(sq_commissions.agent_nbr) ) ) ) as var_agnt_nbr,
LTRIM ( RTRIM ( LKP_1.ST_RGN_DSTRCT_SRVC /* replaced lookup LKP_AGENT_HIERARCHY */ ) ) as var_lkp_st_rgn_dstrct_srvc,
POSITION('':'',var_lkp_st_rgn_dstrct_srvc) as var_lkp_colon_pos1,
REGEXP_INSTR ( var_lkp_st_rgn_dstrct_srvc , '':'' , 1 , 2 ) as var_lkp_colon_pos2,
REGEXP_INSTR ( var_lkp_st_rgn_dstrct_srvc , '':'' , 1 , 3 ) as var_lkp_colon_pos3,
REGEXP_INSTR ( var_lkp_st_rgn_dstrct_srvc , '':'' , 1 , 4 ) as var_lkp_colon_pos4,
SUBSTR ( var_lkp_st_rgn_dstrct_srvc , 1 , var_lkp_colon_pos1 - 1 ) as var_lkp_st_cd,
SUBSTR ( var_lkp_st_rgn_dstrct_srvc , var_lkp_colon_pos1 + 1 , var_lkp_colon_pos2 - var_lkp_colon_pos1 - 1 ) as var_lkp_rgn_cd,
SUBSTR ( var_lkp_st_rgn_dstrct_srvc , var_lkp_colon_pos2 + 1 , var_lkp_colon_pos3 - var_lkp_colon_pos2 - 1 ) as var_lkp_dstrct_cd,
SUBSTR ( var_lkp_st_rgn_dstrct_srvc , var_lkp_colon_pos3 + 1 , var_lkp_colon_pos4 - var_lkp_colon_pos3 - 1 ) as var_lkp_srvc_ctr_cd,
DECODE ( sq_commissions.state_nbr , 1 , ''AL'' , 10 , ''GA'' , 28 , ''MS'' , ''UNK'' ) as var_st_cd,
var_st_cd as out_st_cd,
DECODE ( TRUE , NOT ( var_lkp_st_rgn_dstrct_srvc IS NULL ) , var_lkp_rgn_cd , var_lkp_st_rgn_dstrct_srvc IS NULL , ''UNK'' ) as out_rgn_cd,
DECODE ( TRUE , NOT ( var_lkp_st_rgn_dstrct_srvc IS NULL ) , var_lkp_dstrct_cd , var_lkp_st_rgn_dstrct_srvc IS NULL , ''UNK'' ) as out_dstrct_cd,
LTRIM ( RTRIM ( TO_CHAR ( TO_NUMBER(sq_commissions.service_center) ) ) ) as var_srvc_ctr_cd,
DECODE ( TRUE , var_srvc_ctr_cd = ''0'' AND NOT ( var_lkp_st_rgn_dstrct_srvc IS NULL ) , var_lkp_srvc_ctr_cd , var_srvc_ctr_cd = ''0'' AND var_lkp_st_rgn_dstrct_srvc IS NULL , ''UNK'' , var_srvc_ctr_cd ) as out_srvc_ctr_cd,
var_agnt_nbr as out_agnt_nbr,
NULL as out_user_id,
NULL as out_mbrshp_type_cd,
NULL as out_mbrshp_num,
NULL as out_mbrshp_eff_dt,
0 as plcy_cnt_qut,
0 as plcy_cnt_new_busi,
0 as prev_mtd_plcy_cnt,
0 as persis_plcy_cnt,
0 as apld_cd,
0 as wrtn_prem,
0 as pd_prem,
0 as new_busn_prem,
sq_commissions.amount as out_pd_coms,
0 as new_mbrshp_cnt,
sq_commissions.source_record_id,
row_number() over (partition by sq_commissions.source_record_id order by sq_commissions.source_record_id) as RNK
FROM
sq_commissions
LEFT JOIN LKP_AGENT_HIERARCHY LKP_1 ON LKP_1.AGNT_NBR = var_agnt_nbr AND LKP_1.CAL_DT = sq_commissions.accounting_dt
QUALIFY RNK = 1
);


-- Component exp_cleanse_tpc_00201_prdcrd_y, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_cleanse_tpc_00201_prdcrd_y AS
(
SELECT
LAST_DAY ( TO_DATE ( TO_CHAR ( ( sq_tpc00201_prdcrd_y.eom_yyyy * 10000 ) + ( sq_tpc00201_prdcrd_y.eom_mm * 100 ) + 1 ) , ''YYYYMMDD'' ) ) as var_cal_dt,
var_cal_dt as out_cal_dt,
''LIFE'' as out_lob_cd,
LTRIM ( RTRIM ( TO_CHAR ( TO_NUMBER(sq_tpc00201_prdcrd_y.agent) ) ) ) as var_agnt_nbr,
LTRIM ( RTRIM ( LKP_1.ST_RGN_DSTRCT_SRVC /* replaced lookup LKP_AGENT_HIERARCHY */ ) ) as var_lkp_st_rgn_dstrct_srvc,
POSITION('':'',var_lkp_st_rgn_dstrct_srvc) as var_lkp_colon_pos1,
SUBSTR ( var_lkp_st_rgn_dstrct_srvc , 1 , var_lkp_colon_pos1 - 1 ) as var_lkp_st_cd,
LTRIM ( RTRIM ( sq_tpc00201_prdcrd_y.region ) ) as var_region1,
DECODE ( TRUE , LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ IS NULL , ''UNK'' , var_region1 = ''9'' AND NOT ( var_lkp_st_rgn_dstrct_srvc IS NULL ) , ''CRC-'' || var_lkp_st_cd , var_region1 = ''9'' AND var_lkp_st_rgn_dstrct_srvc IS NULL , ''UNK'' , LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ ) as var_region2,
CASE
  WHEN var_region1 IN (''1'', ''2'') THEN ''AL''
  WHEN var_region1 = ''3'' THEN ''AL''
  WHEN var_region1 = ''5'' THEN ''GA''
  WHEN var_region1 = ''6'' THEN ''MS''
  WHEN var_region1 = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL THEN var_lkp_st_cd
  WHEN var_region1 = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NULL THEN ''UNK''
END as out_st_cd,
CASE
  WHEN var_region1 = ''9''
  AND :in_state = 1 THEN ''CRC-AL''
  WHEN var_region1 = ''9''
  AND :in_state = 10 THEN ''CRC-GA''
  WHEN var_region1 = ''9''
  AND :in_state = 28 THEN ''CRC-MS''
  WHEN var_region2 IS NOT NULL THEN var_region2
  ELSE ''UNK''
END as out_rgn_cd,
LTRIM ( RTRIM ( sq_tpc00201_prdcrd_y.district ) ) as var_district,
--DECODE ( TRUE , IS_INTEGER(var_district) , var_district || ''-'' || var_region1 , var_district || var_region1 ) as out_dstrct_cd,
CASE
  WHEN try_to_number(var_district) IS NOT NULL THEN var_district || ''-'' || var_region1
  ELSE var_district || var_region1
END AS out_dstrct_cd,

LTRIM ( RTRIM ( TO_CHAR ( TO_NUMBER(sq_tpc00201_prdcrd_y.service_center) ) ) ) as out_srvc_ctr_cd,
var_agnt_nbr as out_agnt_nbr,
NULL as out_user_id,
NULL as out_mbrshp_type_cd,
NULL as out_mbrshp_num,
NULL as out_mbrshp_eff_dt,
0 as out_plcy_cnt_qut,
0 as out_plcy_cnt_new_busi,
0 as out_prev_mtd_plcy_cnt,
0 as out_persis_plcy_cnt,
sq_tpc00201_prdcrd_y.app_credit as out_apld_cd,
sq_tpc00201_prdcrd_y.prem_cred_change as out_wrtn_prem,
0 as out_pd_prem,
0 as out_new_busn_prem,
0 as out_pd_coms,
0 as out_new_mbrshp_cnt,
sq_tpc00201_prdcrd_y.source_record_id,
row_number() over (partition by sq_tpc00201_prdcrd_y.source_record_id order by sq_tpc00201_prdcrd_y.source_record_id) as RNK
FROM
sq_tpc00201_prdcrd_y
LEFT JOIN LKP_AGENT_HIERARCHY LKP_1 ON LKP_1.AGNT_NBR = var_agnt_nbr AND LKP_1.CAL_DT = var_cal_dt
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_2 ON LKP_2.SRC_IDNTFTN_SYS = ''DB2'' AND LKP_2.SRC_IDNTFTN_NM = ''db_t_core_prod.agent_hier.sales_rgn_cd'' AND LKP_2.SRC_IDNTFTN_VAL = var_region1 AND LKP_2.TGT_IDNTFTN_NM = ''NOT_APPLICABLE'' AND LKP_2.EFF_DT <= var_cal_dt AND LKP_2.EXPN_DT >= var_cal_dt
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_3 ON LKP_3.SRC_IDNTFTN_SYS = ''DB2'' AND LKP_3.SRC_IDNTFTN_NM = ''db_t_core_prod.agent_hier.sales_rgn_cd'' AND LKP_3.SRC_IDNTFTN_VAL = var_region1 AND LKP_3.TGT_IDNTFTN_NM = ''NOT_APPLICABLE'' AND LKP_3.EFF_DT <= var_cal_dt AND LKP_3.EXPN_DT >= var_cal_dt
QUALIFY RNK = 1
);


-- Component exp_cleanse_tpc00501_level, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_cleanse_tpc00501_level AS
(
SELECT
LAST_DAY ( TO_DATE ( TO_CHAR ( ( sq_tpc00501_level.year1 * 10000 ) + ( sq_tpc00501_level.month1 * 100 ) + 1 ) , ''YYYYMMDD'' ) ) as var_cal_dt,
var_cal_dt as out_cal_dt,
''LIFE'' as out_lob_cd,
LTRIM ( RTRIM ( TO_CHAR ( TO_NUMBER(sq_tpc00501_level.agent) ) ) ) as var_agnt_nbr,
LTRIM ( RTRIM ( LKP_1.ST_RGN_DSTRCT_SRVC /* replaced lookup LKP_AGENT_HIERARCHY */ ) ) as var_lkp_st_rgn_dstrct_srvc,
POSITION('':'',var_lkp_st_rgn_dstrct_srvc) as var_lkp_colon_pos1,
REGEXP_INSTR ( var_lkp_st_rgn_dstrct_srvc , '':'' , 1 , 2 ) as var_lkp_colon_pos2,
REGEXP_INSTR ( var_lkp_st_rgn_dstrct_srvc , '':'' , 1 , 3 ) as var_lkp_colon_pos3,
REGEXP_INSTR ( var_lkp_st_rgn_dstrct_srvc , '':'' , 1 , 4 ) as var_lkp_colon_pos4,
SUBSTR ( var_lkp_st_rgn_dstrct_srvc , 1 , var_lkp_colon_pos1 - 1 ) as var_lkp_st_cd,
SUBSTR ( var_lkp_st_rgn_dstrct_srvc , var_lkp_colon_pos3 + 1 , var_lkp_colon_pos4 - var_lkp_colon_pos3 - 1 ) as var_lkp_srvc_ctr_cd,
LTRIM ( RTRIM ( sq_tpc00501_level.region ) ) as var_region,
CASE
  WHEN var_region IN (''1'', ''2'') THEN ''AL''
  WHEN var_region = ''3'' THEN ''AL''
  WHEN var_region = ''5'' THEN ''GA''
  WHEN var_region = ''6'' THEN ''MS''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL THEN var_lkp_st_cd
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NULL THEN ''UNK''
END as out_st_cd,
CASE
  WHEN var_region = ''1'' THEN ''ALABAMA NORTH MARKETING''
  WHEN var_region = ''2'' THEN ''ALABAMA SOUTH MARKETING''
  WHEN var_region = ''3'' THEN ''ALABAMA IE''
  WHEN var_region = ''5'' THEN ''GEORGIA MARKETING''
  WHEN var_region = ''6'' THEN ''MISSISSIPPI MARKETING''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL
  AND var_lkp_st_cd = ''AL'' THEN ''CRC-AL''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL
  AND var_lkp_st_cd = ''GA'' THEN ''CRC-GA''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL
  AND var_lkp_st_cd = ''MS'' THEN ''CRC-MS''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NULL THEN ''UNK''
END as out_rgn_cd,
TRIM(sq_tpc00501_level.district) as var_district,
CASE
  WHEN TRY_TO_NUMBER (TRIM(var_district)) IS NOT NULL THEN CONCAT (var_district, ''-'', var_region)
  ELSE CONCAT (var_district, var_region)
END as out_dstrct_cd,
CASE
  WHEN var_lkp_st_rgn_dstrct_srvc IS NOT NULL THEN var_lkp_srvc_ctr_cd
  ELSE ''UNK''
END as out_srvc_ctr_cd,
var_agnt_nbr as out_agnt_nbr,
NULL as out_user_id,
NULL as out_mbrshp_type_cd,
NULL as out_mbrshp_num,
NULL as out_mbrshp_eff_dt,
0 as out_plcy_cnt_qut,
0 as out_plcy_cnt_new_busi,
0 as out_prev_mtd_plcy_cnt,
0 as out_persis_plcy_cnt,
0 as out_apld_cd,
0 as out_wrtn_prem,
0 as out_pd_prem,
sq_tpc00501_level.nb_prem as out_new_busn_prem,
sq_tpc00501_level.nb_commpd as out_pd_coms,
0 as out_new_mbrshp_cnt,
sq_tpc00501_level.source_record_id,
row_number() over (partition by sq_tpc00501_level.source_record_id order by sq_tpc00501_level.source_record_id) as RNK
FROM
sq_tpc00501_level
LEFT JOIN LKP_AGENT_HIERARCHY LKP_1 ON LKP_1.AGNT_NBR = var_agnt_nbr AND LKP_1.CAL_DT = var_cal_dt
QUALIFY RNK = 1
);


-- PIPELINE START FOR 2

-- Component sq_tpc00401_pce_pd_y, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_tpc00401_pce_pd_y AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as eom_yyyy,
$2 as eom_mm,
$3 as region,
$4 as district,
$5 as service_center,
$6 as agent,
$7 as comm_anlzd_prem,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 

EOM_YYYY,

EOM_MM,

REGION,

DISTRICT,

SERVICE_CENTER,

AGENT,

SUM(COMM_ANLZD_PREM) AS COMM_ANLZD_PREM 

FROM DB_T_PROD_COMN.TPC00401_PCE_PD_Y  

WHERE
  ADD_MONTHS(
    TO_DATE(
      cast(EOM_YYYY * 100 + EOM_MM AS VARCHAR),
      ''YYYYMM''
    ),
    1
  ) - 1 >= :CAL_START_DT
  AND ADD_MONTHS(
    TO_DATE(
      CAST(EOM_YYYY * 100 + EOM_MM AS VARCHAR),
      ''YYYYMM''
    ),
    1
  ) - 1 <= :CAL_END_DT  
  /*   LAST_DAY(
    TO_DATE(EOM_YYYY || ''-'' || LPAD(EOM_MM, 2, ''0'') || ''-01'', 
    ''YYYY-MM-DD''
  ) BETWEEN :CAL_START_DT AND :CAL_END_DT*/ 

GROUP BY 

EOM_YYYY,

EOM_MM,

REGION,

DISTRICT,

SERVICE_CENTER,

AGENT 

HAVING SUM(COMM_ANLZD_PREM) <> 0
) SRC
)
);


-- PIPELINE START FOR 2

-- Component sq_agt_asic, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_agt_asic AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as app_year,
$2 as app_month,
$3 as region,
$4 as district,
$5 as agent_nbr,
$6 as apps_n,
$7 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 

APP_YEAR,

APP_MONTH,

REGION,

DISTRICT,

AGENT_NBR,

SUM(APPS_N) AS APPS_N 

FROM DB_T_PROD_COMN.AGT_ASIC 

WHERE
  ADD_MONTHS(
    TO_DATE(
      cast(APP_YEAR * 100 + APP_MONTH + 200000 AS VARCHAR),
      ''YYYYMM''
    ),
    1
  ) - 1 >= :CAL_START_DT
  AND ADD_MONTHS(
    TO_DATE(
      cast(APP_YEAR * 100 + APP_MONTH + 200000 AS VARCHAR),
      ''YYYYMM''
    ),
    1
  ) - 1 <= :CAL_END_DT

GROUP BY 

APP_YEAR,

APP_MONTH,

REGION,

DISTRICT,

AGENT_NBR

HAVING 

SUM(APPS_N) <> 0
) SRC
)
);


-- Component exp_cleanse_agt_asic, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_cleanse_agt_asic AS
(
SELECT
LAST_DAY ( TO_DATE ( TO_CHAR ( ( sq_agt_asic.app_year * 10000 ) + 20000000 + ( sq_agt_asic.app_month * 100 ) + 1 ) , ''YYYYMMDD'' ) ) as var_cal_cd,
var_cal_cd as out_cal_dt,
''ASIC'' as out_lob_cd,
LTRIM ( RTRIM ( TO_CHAR ( TO_NUMBER(sq_agt_asic.agent_nbr) ) ) ) as var_agent_nbr,
LTRIM ( RTRIM ( LKP_1.ST_RGN_DSTRCT_SRVC /* replaced lookup LKP_AGENT_HIERARCHY */ ) ) as var_lkp_st_rgn_dstrct_srvc,
POSITION('':'',var_lkp_st_rgn_dstrct_srvc) as var_lkp_colon_pos1,
REGEXP_INSTR ( var_lkp_st_rgn_dstrct_srvc , '':'' , 1 , 2 ) as var_lkp_colon_pos2,
REGEXP_INSTR ( var_lkp_st_rgn_dstrct_srvc , '':'' , 1 , 3 ) as var_lkp_colon_pos3,
REGEXP_INSTR ( var_lkp_st_rgn_dstrct_srvc , '':'' , 1 , 4 ) as var_lkp_colon_pos4,
SUBSTR ( var_lkp_st_rgn_dstrct_srvc , 1 , var_lkp_colon_pos1 - 1 ) as var_lkp_ST_CD,
SUBSTR ( var_lkp_st_rgn_dstrct_srvc , var_lkp_colon_pos3 + 1 , var_lkp_colon_pos4 - var_lkp_colon_pos3 - 1 ) as var_lkp_srvc_ctr_cd,
LTRIM ( RTRIM ( sq_agt_asic.region ) ) as var_region,
CASE
  WHEN var_region IN (''1'', ''2'') THEN ''AL''
  WHEN var_region = ''3'' THEN ''AL''
  WHEN var_region = ''5'' THEN ''GA''
  WHEN var_region = ''6'' THEN ''MS''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL THEN var_lkp_ST_CD
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NULL THEN ''UNK''
END as out_st_cd,
CASE
  WHEN var_region = ''1'' THEN ''ALABAMA NORTH MARKETING''
  WHEN var_region = ''2'' THEN ''ALABAMA SOUTH MARKETING''
  WHEN var_region = ''3'' THEN ''ALABAMA IE''
  WHEN var_region = ''5'' THEN ''GEORGIA MARKETING''
  WHEN var_region = ''6'' THEN ''MISSISSIPPI MARKETING''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL
  AND var_lkp_ST_CD = ''AL'' THEN ''CRC-AL''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL
  AND var_lkp_ST_CD = ''GA'' THEN ''CRC-GA''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL
  AND var_lkp_ST_CD = ''MS'' THEN ''CRC-MS''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NULL THEN ''UNK''
END as out_rgn_cd,
TRIM(sq_agt_asic.district) as var_district,
CASE
  WHEN TRY_TO_NUMBER (TRIM(var_district)) IS NOT NULL THEN CONCAT (var_district, ''-'', var_region)
  ELSE CONCAT (var_district, var_region)
END as out_dstrct_cd,
CASE WHEN NOT ( var_lkp_st_rgn_dstrct_srvc IS NULL ) THEN var_lkp_srvc_ctr_cd ELSE ''UNK'' END as out_srvc_ctr_cd,
var_agent_nbr as out_agnt_nbr,
NULL as out_user_id,
NULL as out_mbrshp_type_cd,
NULL as out_mbrshp_num,
NULL as out_mbrshp_eff_dt,
0 as out_plcy_cnt_qut,
0 as out_plcy_cnt_new_busi,
0 as out_prev_mtd_plcy_cnt,
0 as out_persis_plcy_cnt,
sq_agt_asic.apps_n as out_apld_cd,
0 as out_wrtn_prem,
0 as out_pd_prem,
0 as out_new_busn_prem,
0 as out_pd_coms,
0 as out_new_mbrshp_cnt,
sq_agt_asic.source_record_id,
row_number() over (partition by sq_agt_asic.source_record_id order by sq_agt_asic.source_record_id) as RNK
FROM
sq_agt_asic
LEFT JOIN LKP_AGENT_HIERARCHY LKP_1 ON LKP_1.AGNT_NBR = var_agent_nbr AND LKP_1.CAL_DT = var_cal_cd
QUALIFY RNK = 1
);


-- Component exp_cleanse_prod_cred_detail, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_cleanse_prod_cred_detail AS
(
SELECT
sq_prod_cred_Detail.eom_date as cal_dt,
LTRIM ( RTRIM ( sq_prod_cred_Detail.policy_symbol ) ) as var_policy_symbol,
DECODE ( TRUE , var_policy_symbol = ''CP'' , ''COMPPERSL'' , var_policy_symbol = ''FC'' , ''FCL'' , var_policy_symbol = ''GL'' , ''UMBRELLA'' , var_policy_symbol = ''H0'' , ''HO'' , var_policy_symbol = ''M0'' , ''MACHINERY'' , var_policy_symbol = ''R0'' , ''RFD'' , var_policy_symbol = ''SC'' , ''PORTFOLIO'' , var_policy_symbol = ''SM'' , ''BOP / CHURCH'' , var_policy_symbol = ''SP'' , ''SCHDPROP'' , var_policy_symbol = ''T0'' , ''MH'' , ''UNK'' ) as out_lob_cd,
DECODE ( sq_prod_cred_Detail.state , 1 , ''AL'' , 10 , ''GA'' , 28 , ''MS'' , ''UNK'' ) as var_st_cd,
var_st_cd as out_st_cd,
LTRIM ( RTRIM ( sq_prod_cred_Detail.region ) ) as var_region1,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */ END as var_region2,
DECODE ( TRUE , var_region1 = ''9'' AND sq_prod_cred_Detail.state = 1 , ''CRC-AL'' , var_region1 = ''9'' AND sq_prod_cred_Detail.state = 10 , ''CRC-GA'' , var_region1 = ''9'' AND sq_prod_cred_Detail.state = 28 , ''CRC-MS'' , NOT ( var_region2 IS NULL ) , var_region2 , ''UNK'' ) as out_rgn_cd,
LTRIM ( RTRIM ( sq_prod_cred_Detail.district ) ) as var_Ddistrict,
CASE
  WHEN NOT var_region1 IN (''1'', ''2'', ''5'', ''6'', ''9'') THEN ''UNK''
  WHEN NOT TRY_TO_NUMBER (var_Ddistrict) IS NULL THEN CONCAT (var_Ddistrict, ''-'', var_region1)
  ELSE CONCAT (var_Ddistrict, var_region1)
END AS out_dstrct_cd,
LTRIM ( RTRIM ( TO_CHAR ( TO_NUMBER(sq_prod_cred_Detail.svc_cntr) ) ) ) as out_srvc_ctr_cd,
LTRIM ( RTRIM ( TO_CHAR ( TO_NUMBER(sq_prod_cred_Detail.agent_nbr) ) ) ) as out_agnt_nbr,
NULL as out_user_id,
NULL as out_mbrshp_type_cd,
NULL as out_mbrshp_num,
NULL as out_mbrshp_eff_dt,
0 as out_new_quot_cnt,
0 as out_new_bnd_cnt,
0 as out_pyr_infrc_plcy_cnt,
0 as out_prsistnt_plcy_cnt,
sq_prod_cred_Detail.app_credit as out_apld_cr,
0 as out_wrtn_prem,
0 as out_pd_prem,
0 as out_new_busn_prem,
0 as out_pd_coms,
0 as out_new_mbrshp_cnt,
sq_prod_cred_Detail.source_record_id,
row_number() over (partition by sq_prod_cred_Detail.source_record_id order by sq_prod_cred_Detail.source_record_id) as RNK
FROM
sq_prod_cred_Detail
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_1 ON LKP_1.SRC_IDNTFTN_SYS = ''DB2'' AND LKP_1.SRC_IDNTFTN_NM = ''db_t_core_prod.agent_hier.sales_rgn_cd'' AND LKP_1.SRC_IDNTFTN_VAL = var_region1 AND LKP_1.TGT_IDNTFTN_NM = ''NOT_APPLICABLE'' AND LKP_1.EFF_DT <= sq_prod_cred_Detail.eom_date AND LKP_1.EXPN_DT >= sq_prod_cred_Detail.eom_date
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT LKP_2 ON LKP_2.SRC_IDNTFTN_SYS = ''DB2'' AND LKP_2.SRC_IDNTFTN_NM = ''db_t_core_prod.agent_hier.sales_rgn_cd'' AND LKP_2.SRC_IDNTFTN_VAL = var_region1 AND LKP_2.TGT_IDNTFTN_NM = ''NOT_APPLICABLE'' AND LKP_2.EFF_DT <= sq_prod_cred_Detail.eom_date AND LKP_2.EXPN_DT >= sq_prod_cred_Detail.eom_date
QUALIFY RNK = 1
);


-- Component exp_cleanse_agt_fire, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_cleanse_agt_fire AS
(
SELECT
LAST_DAY ( TO_DATE ( TO_CHAR ( ( sq_agt_fire.app_year * 10000 ) + 20000000 + ( sq_agt_fire.app_month * 100 ) + 1 ) , ''YYYYMMDD'' ) ) as var_cal_dt,
var_cal_dt as out_cal_dt,
''SF'' as out_lob_cd,
LTRIM ( RTRIM ( TO_CHAR ( TO_NUMBER(sq_agt_fire.agent_nbr) ) ) ) as var_agent_nbr,
LTRIM ( RTRIM ( LKP_1.ST_RGN_DSTRCT_SRVC /* replaced lookup LKP_AGENT_HIERARCHY */ ) ) as var_lkp_st_rgn_dstrct_srvc,
POSITION('':'',var_lkp_st_rgn_dstrct_srvc) as var_lkp_colon_pos1,
REGEXP_INSTR ( var_lkp_st_rgn_dstrct_srvc , '':'' , 1 , 2 ) as var_lkp_colon_pos2,
REGEXP_INSTR ( var_lkp_st_rgn_dstrct_srvc , '':'' , 1 , 3 ) as var_lkp_colon_pos3,
REGEXP_INSTR ( var_lkp_st_rgn_dstrct_srvc , '':'' , 1 , 4 ) as var_lkp_colon_pos4,
SUBSTR ( var_lkp_st_rgn_dstrct_srvc , 1 , var_lkp_colon_pos1 - 1 ) as var_lkp_st_cd,
SUBSTR ( var_lkp_st_rgn_dstrct_srvc , var_lkp_colon_pos3 + 1 , var_lkp_colon_pos4 - var_lkp_colon_pos3 - 1 ) as var_lkp_srvc_ctr_cd,
LTRIM ( RTRIM ( sq_agt_fire.region ) ) as var_region,
CASE
  WHEN var_region IN (''1'', ''2'') THEN ''AL''
  WHEN var_region = ''3'' THEN ''AL''
  WHEN var_region = ''5'' THEN ''GA''
  WHEN var_region = ''6'' THEN ''MS''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL THEN var_lkp_st_cd
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NULL THEN ''UNK''
END as out_st_cd,
CASE
  WHEN var_region = ''1'' THEN ''ALABAMA NORTH MARKETING''
  WHEN var_region = ''2'' THEN ''ALABAMA SOUTH MARKETING''
  WHEN var_region = ''3'' THEN ''ALABAMA IE''
  WHEN var_region = ''5'' THEN ''GEORGIA MARKETING''
  WHEN var_region = ''6'' THEN ''MISSISSIPPI MARKETING''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL
  AND var_lkp_st_cd = ''AL'' THEN ''CRC-AL''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL
  AND var_lkp_st_cd = ''GA'' THEN ''CRC-GA''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL
  AND var_lkp_st_cd = ''MS'' THEN ''CRC-MS''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NULL THEN ''UNK''
END as out_rgn_cd,
LTRIM ( RTRIM ( sq_agt_fire.district ) ) as var_district,
--DECODE ( TRUE , IS_INTEGER(var_district) , var_district || ''-'' || var_region , var_district || var_region ) as out_dstrct_cd,
CASE
  WHEN try_to_number(var_district) IS NOT NULL THEN var_district || ''-'' || var_region
  ELSE var_district || var_region
END AS out_dstrct_cd,
CASE WHEN NOT ( var_lkp_st_rgn_dstrct_srvc IS NULL ) THEN var_lkp_srvc_ctr_cd ELSE ''UNK'' END as out_srvc_ctr_cd,
var_agent_nbr as out_agnt_nbr,
NULL as out_user_id,
NULL as out_mbrshp_type_cd,
NULL as out_mbrshp_num,
NULL as out_mbrshp_eff_dt,
0 as out_plcy_cnt_qut,
0 as out_plcy_cnt_new_busi,
0 as out_prev_mtd_plcy_cnt,
0 as out_persis_plcy_cnt,
sq_agt_fire.apps_f as out_apld_cd,
0 as out_wrtn_prem,
0 as out_pd_prem,
0 as out_new_busn_prem,
0 as out_pd_coms,
0 as out_new_mbrshp_cnt,
sq_agt_fire.source_record_id,
row_number() over (partition by sq_agt_fire.source_record_id order by sq_agt_fire.source_record_id) as RNK
FROM
sq_agt_fire
LEFT JOIN LKP_AGENT_HIERARCHY LKP_1 ON LKP_1.AGNT_NBR = var_agent_nbr AND LKP_1.CAL_DT = var_cal_dt
QUALIFY RNK = 1
);


-- Component exp_cleanse_tpc00401_pce_pd_y, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_cleanse_tpc00401_pce_pd_y AS
(
SELECT
LAST_DAY ( TO_DATE ( TO_CHAR ( ( sq_tpc00401_pce_pd_y.eom_yyyy * 10000 ) + ( sq_tpc00401_pce_pd_y.eom_mm * 100 ) + 1 ) , ''YYYYMMDD'' ) ) as var_cal_dt,
var_cal_dt as out_cal_dt,
''LIFE'' as out_lob_cd,
LTRIM ( RTRIM ( TO_CHAR ( TO_NUMBER(sq_tpc00401_pce_pd_y.agent) ) ) ) as var_agnt_nbr,
LTRIM ( RTRIM ( LKP_1.ST_RGN_DSTRCT_SRVC /* replaced lookup LKP_AGENT_HIERARCHY */ ) ) as var_lkp_st_rgn_dstrct_srvc,
POSITION('':'',var_lkp_st_rgn_dstrct_srvc) as var_lkp_colon_pos1,
SUBSTR ( var_lkp_st_rgn_dstrct_srvc , 1 , var_lkp_colon_pos1 - 1 ) as var_lkp_st_cd,
LTRIM ( RTRIM ( sq_tpc00401_pce_pd_y.region ) ) as var_region,
CASE
  WHEN var_region IN (''1'', ''2'') THEN ''AL''
  WHEN var_region = ''3'' THEN ''AL''
  WHEN var_region = ''5'' THEN ''GA''
  WHEN var_region = ''6'' THEN ''MS''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL THEN var_lkp_st_cd
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NULL THEN ''UNK''
END as out_st_cd,
CASE
  WHEN var_region = ''1'' THEN ''ALABAMA NORTH MARKETING''
  WHEN var_region = ''3'' THEN ''ALABAMA IE''
  WHEN var_region = ''2'' THEN ''ALABAMA SOUTH MARKETING''
  WHEN var_region = ''5'' THEN ''GEORGIA MARKETING''
  WHEN var_region = ''6'' THEN ''MISSISSIPPI MARKETING''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL
  AND var_lkp_st_cd = ''AL'' THEN ''CRC-AL''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL
  AND var_lkp_st_cd = ''GA'' THEN ''CRC-GA''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NOT NULL
  AND var_lkp_st_cd = ''MS'' THEN ''CRC-MS''
  WHEN var_region = ''9''
  AND var_lkp_st_rgn_dstrct_srvc IS NULL THEN ''UNK''
END as out_rgn_cd,
LTRIM ( RTRIM ( sq_tpc00401_pce_pd_y.district ) ) as var_district,
CASE
  WHEN NOT TRY_TO_NUMBER (var_district) IS NULL THEN CONCAT (var_district, ''-'', var_region)
  ELSE CONCAT (var_district, var_region)
END AS out_dstrct_cd,
LTRIM ( RTRIM ( TO_CHAR ( TO_NUMBER(sq_tpc00401_pce_pd_y.service_center) ) ) ) as out_srvc_ctr_cd,
var_agnt_nbr as out_agnt_nbr,
NULL as out_user_id,
NULL as out_mbrshp_type_cd,
NULL as out_mbrshp_num,
NULL as out_mbrshp_eff_dt,
0 as out_plcy_cnt_qut,
0 as out_plcy_cnt_new_busi,
0 as out_prev_mtd_plcy_cnt,
0 as out_persis_plcy_cnt,
0 as out_apld_cd,
0 as out_wrtn_prem,
sq_tpc00401_pce_pd_y.comm_anlzd_prem as out_pd_prem,
0 as out_new_busn_prem,
0 as out_pd_coms,
0 as out_new_mbrshp_cnt,
sq_tpc00401_pce_pd_y.source_record_id,
row_number() over (partition by sq_tpc00401_pce_pd_y.source_record_id order by sq_tpc00401_pce_pd_y.source_record_id) as RNK
FROM
sq_tpc00401_pce_pd_y
LEFT JOIN LKP_AGENT_HIERARCHY LKP_1 ON LKP_1.AGNT_NBR = var_agnt_nbr AND LKP_1.CAL_DT = var_cal_dt
QUALIFY RNK = 1
);


-- Component un_all_sources, Type UNION_TRANSFORMATION 
CREATE OR REPLACE TEMPORARY TABLE un_all_sources AS
(
/* Union Group WR_AGENT_SUMM */
SELECT
exp_cleanse_wr_quotes_bounds_persistency.cal_dt,
exp_cleanse_wr_quotes_bounds_persistency.out_lob_cd as lob_cd,
exp_cleanse_wr_quotes_bounds_persistency.out_st_cd as st_cd,
exp_cleanse_wr_quotes_bounds_persistency.out_rgn_cd as rgn_cd,
exp_cleanse_wr_quotes_bounds_persistency.out_dstrct_cd as dstrct_cd,
exp_cleanse_wr_quotes_bounds_persistency.out_srvc_ctr_cd as srvc_ctr_cd,
exp_cleanse_wr_quotes_bounds_persistency.out_agnt_nbr as agnt_nbr,
exp_cleanse_wr_quotes_bounds_persistency.out_user_id as user_id,
exp_cleanse_wr_quotes_bounds_persistency.out_mbrshp_type_cd as mbrshp_type_cd,
exp_cleanse_wr_quotes_bounds_persistency.out_mbrshp_num as mbrshp_num,
exp_cleanse_wr_quotes_bounds_persistency.out_mbrshp_eff_dt as mbrshp_eff_dt,
exp_cleanse_wr_quotes_bounds_persistency.new_quot_cnt,
exp_cleanse_wr_quotes_bounds_persistency.new_bnd_cnt,
exp_cleanse_wr_quotes_bounds_persistency.pyr_infrc_plcy_cnt,
exp_cleanse_wr_quotes_bounds_persistency.prsistnt_plcy_cnt,
exp_cleanse_wr_quotes_bounds_persistency.out_apld_cr as apld_cd,
exp_cleanse_wr_quotes_bounds_persistency.out_wrtn_prem as wrtn_prem,
exp_cleanse_wr_quotes_bounds_persistency.out_pd_prem as pd_prem,
exp_cleanse_wr_quotes_bounds_persistency.out_new_busn_prem as new_busn_prem,
exp_cleanse_wr_quotes_bounds_persistency.out_pd_coms as pd_coms,
exp_cleanse_wr_quotes_bounds_persistency.out_new_mbrshp_cnt as new_mbrshp_cnt,
exp_cleanse_wr_quotes_bounds_persistency.source_record_id
FROM exp_cleanse_wr_quotes_bounds_persistency
UNION ALL
/* Union Group TPC00201_PRDCRD_Y */
SELECT
exp_cleanse_tpc_00201_prdcrd_y.out_cal_dt as cal_dt,
exp_cleanse_tpc_00201_prdcrd_y.out_lob_cd as lob_cd,
exp_cleanse_tpc_00201_prdcrd_y.out_st_cd as st_cd,
exp_cleanse_tpc_00201_prdcrd_y.out_rgn_cd as rgn_cd,
exp_cleanse_tpc_00201_prdcrd_y.out_dstrct_cd as dstrct_cd,
exp_cleanse_tpc_00201_prdcrd_y.out_srvc_ctr_cd as srvc_ctr_cd,
exp_cleanse_tpc_00201_prdcrd_y.out_agnt_nbr as agnt_nbr,
exp_cleanse_tpc_00201_prdcrd_y.out_user_id as user_id,
exp_cleanse_tpc_00201_prdcrd_y.out_mbrshp_type_cd as mbrshp_type_cd,
exp_cleanse_tpc_00201_prdcrd_y.out_mbrshp_num as mbrshp_num,
exp_cleanse_tpc_00201_prdcrd_y.out_mbrshp_eff_dt as mbrshp_eff_dt,
exp_cleanse_tpc_00201_prdcrd_y.out_plcy_cnt_qut as new_quot_cnt,
exp_cleanse_tpc_00201_prdcrd_y.out_plcy_cnt_new_busi as new_bnd_cnt,
exp_cleanse_tpc_00201_prdcrd_y.out_prev_mtd_plcy_cnt as pyr_infrc_plcy_cnt,
exp_cleanse_tpc_00201_prdcrd_y.out_persis_plcy_cnt as prsistnt_plcy_cnt,
exp_cleanse_tpc_00201_prdcrd_y.out_apld_cd as apld_cd,
exp_cleanse_tpc_00201_prdcrd_y.out_wrtn_prem as wrtn_prem,
exp_cleanse_tpc_00201_prdcrd_y.out_pd_prem as pd_prem,
exp_cleanse_tpc_00201_prdcrd_y.out_new_busn_prem as new_busn_prem,
exp_cleanse_tpc_00201_prdcrd_y.out_pd_coms as pd_coms,
exp_cleanse_tpc_00201_prdcrd_y.out_new_mbrshp_cnt as new_mbrshp_cnt,
exp_cleanse_tpc_00201_prdcrd_y.source_record_id
FROM exp_cleanse_tpc_00201_prdcrd_y
UNION ALL
/* Union Group AGT_AUTO */
SELECT
exp_cleanse_agt_auto.out_cal_dt as cal_dt,
exp_cleanse_agt_auto.out_lob_cd as lob_cd,
exp_cleanse_agt_auto.out_st_cd as st_cd,
exp_cleanse_agt_auto.out_rgn_cd as rgn_cd,
exp_cleanse_agt_auto.out_dstrct_cd as dstrct_cd,
exp_cleanse_agt_auto.out_srvc_ctr_cd as srvc_ctr_cd,
exp_cleanse_agt_auto.out_agnt_nbr as agnt_nbr,
exp_cleanse_agt_auto.out_user_id as user_id,
exp_cleanse_agt_auto.out_mbrshp_type_cd as mbrshp_type_cd,
exp_cleanse_agt_auto.out_mbrshp_num as mbrshp_num,
exp_cleanse_agt_auto.out_mbrshp_eff_dt as mbrshp_eff_dt,
exp_cleanse_agt_auto.out_plcy_cnt_qut as new_quot_cnt,
exp_cleanse_agt_auto.out_plcy_cnt_new_busi as new_bnd_cnt,
exp_cleanse_agt_auto.out_prev_mtd_plcy_cnt as pyr_infrc_plcy_cnt,
exp_cleanse_agt_auto.out_persis_plcy_cnt as prsistnt_plcy_cnt,
exp_cleanse_agt_auto.out_apld_cd as apld_cd,
exp_cleanse_agt_auto.out_wrtn_prem as wrtn_prem,
exp_cleanse_agt_auto.out_pd_prem as pd_prem,
exp_cleanse_agt_auto.out_new_busn_prem as new_busn_prem,
exp_cleanse_agt_auto.out_pd_coms as pd_coms,
exp_cleanse_agt_auto.out_new_mbrshp_cnt as new_mbrshp_cnt,
exp_cleanse_agt_auto.source_record_id
FROM exp_cleanse_agt_auto
UNION ALL
/* Union Group AGT_ASIC */
SELECT
exp_cleanse_agt_asic.out_cal_dt as cal_dt,
exp_cleanse_agt_asic.out_lob_cd as lob_cd,
exp_cleanse_agt_asic.out_st_cd as st_cd,
exp_cleanse_agt_asic.out_rgn_cd as rgn_cd,
exp_cleanse_agt_asic.out_dstrct_cd as dstrct_cd,
exp_cleanse_agt_asic.out_srvc_ctr_cd as srvc_ctr_cd,
exp_cleanse_agt_asic.out_agnt_nbr as agnt_nbr,
exp_cleanse_agt_asic.out_user_id as user_id,
exp_cleanse_agt_asic.out_mbrshp_type_cd as mbrshp_type_cd,
exp_cleanse_agt_asic.out_mbrshp_num as mbrshp_num,
exp_cleanse_agt_asic.out_mbrshp_eff_dt as mbrshp_eff_dt,
exp_cleanse_agt_asic.out_plcy_cnt_qut as new_quot_cnt,
exp_cleanse_agt_asic.out_plcy_cnt_new_busi as new_bnd_cnt,
exp_cleanse_agt_asic.out_prev_mtd_plcy_cnt as pyr_infrc_plcy_cnt,
exp_cleanse_agt_asic.out_persis_plcy_cnt as prsistnt_plcy_cnt,
exp_cleanse_agt_asic.out_apld_cd as apld_cd,
exp_cleanse_agt_asic.out_wrtn_prem as wrtn_prem,
exp_cleanse_agt_asic.out_pd_prem as pd_prem,
exp_cleanse_agt_asic.out_new_busn_prem as new_busn_prem,
exp_cleanse_agt_asic.out_pd_coms as pd_coms,
exp_cleanse_agt_asic.out_new_mbrshp_cnt as new_mbrshp_cnt,
exp_cleanse_agt_asic.source_record_id
FROM exp_cleanse_agt_asic
UNION ALL
/* Union Group AGT_FIRE */
SELECT
exp_cleanse_agt_fire.out_cal_dt as cal_dt,
exp_cleanse_agt_fire.out_lob_cd as lob_cd,
exp_cleanse_agt_fire.out_st_cd as st_cd,
exp_cleanse_agt_fire.out_rgn_cd as rgn_cd,
exp_cleanse_agt_fire.out_dstrct_cd as dstrct_cd,
exp_cleanse_agt_fire.out_srvc_ctr_cd as srvc_ctr_cd,
exp_cleanse_agt_fire.out_agnt_nbr as agnt_nbr,
exp_cleanse_agt_fire.out_user_id as user_id,
exp_cleanse_agt_fire.out_mbrshp_type_cd as mbrshp_type_cd,
exp_cleanse_agt_fire.out_mbrshp_num as mbrshp_num,
exp_cleanse_agt_fire.out_mbrshp_eff_dt as mbrshp_eff_dt,
exp_cleanse_agt_fire.out_plcy_cnt_qut as new_quot_cnt,
exp_cleanse_agt_fire.out_plcy_cnt_new_busi as new_bnd_cnt,
exp_cleanse_agt_fire.out_prev_mtd_plcy_cnt as pyr_infrc_plcy_cnt,
exp_cleanse_agt_fire.out_persis_plcy_cnt as prsistnt_plcy_cnt,
exp_cleanse_agt_fire.out_apld_cd as apld_cd,
exp_cleanse_agt_fire.out_wrtn_prem as wrtn_prem,
exp_cleanse_agt_fire.out_pd_prem as pd_prem,
exp_cleanse_agt_fire.out_new_busn_prem as new_busn_prem,
exp_cleanse_agt_fire.out_pd_coms as pd_coms,
exp_cleanse_agt_fire.out_new_mbrshp_cnt as new_mbrshp_cnt,
exp_cleanse_agt_fire.source_record_id
FROM exp_cleanse_agt_fire
UNION ALL
/* Union Group PROD_CRED_DETAIL */
SELECT
exp_cleanse_prod_cred_detail.cal_dt,
exp_cleanse_prod_cred_detail.out_lob_cd as lob_cd,
exp_cleanse_prod_cred_detail.out_st_cd as st_cd,
exp_cleanse_prod_cred_detail.out_rgn_cd as rgn_cd,
exp_cleanse_prod_cred_detail.out_dstrct_cd as dstrct_cd,
exp_cleanse_prod_cred_detail.out_srvc_ctr_cd as srvc_ctr_cd,
exp_cleanse_prod_cred_detail.out_agnt_nbr as agnt_nbr,
exp_cleanse_prod_cred_detail.out_user_id as user_id,
exp_cleanse_prod_cred_detail.out_mbrshp_type_cd as mbrshp_type_cd,
exp_cleanse_prod_cred_detail.out_mbrshp_num as mbrshp_num,
exp_cleanse_prod_cred_detail.out_mbrshp_eff_dt as mbrshp_eff_dt,
exp_cleanse_prod_cred_detail.out_new_quot_cnt as new_quot_cnt,
exp_cleanse_prod_cred_detail.out_new_bnd_cnt as new_bnd_cnt,
exp_cleanse_prod_cred_detail.out_pyr_infrc_plcy_cnt as pyr_infrc_plcy_cnt,
exp_cleanse_prod_cred_detail.out_prsistnt_plcy_cnt as prsistnt_plcy_cnt,
exp_cleanse_prod_cred_detail.out_apld_cr as apld_cd,
exp_cleanse_prod_cred_detail.out_wrtn_prem as wrtn_prem,
exp_cleanse_prod_cred_detail.out_pd_prem as pd_prem,
exp_cleanse_prod_cred_detail.out_new_busn_prem as new_busn_prem,
exp_cleanse_prod_cred_detail.out_pd_coms as pd_coms,
exp_cleanse_prod_cred_detail.out_new_mbrshp_cnt as new_mbrshp_cnt,
exp_cleanse_prod_cred_detail.source_record_id
FROM exp_cleanse_prod_cred_detail
UNION ALL
/* Union Group COMMISSIONS */
SELECT
exp_cleanse_commissions.cal_dt,
exp_cleanse_commissions.out_lob_cd as lob_cd,
exp_cleanse_commissions.out_st_cd as st_cd,
exp_cleanse_commissions.out_rgn_cd as rgn_cd,
exp_cleanse_commissions.out_dstrct_cd as dstrct_cd,
exp_cleanse_commissions.out_srvc_ctr_cd as srvc_ctr_cd,
exp_cleanse_commissions.out_agnt_nbr as agnt_nbr,
exp_cleanse_commissions.out_user_id as user_id,
exp_cleanse_commissions.out_mbrshp_type_cd as mbrshp_type_cd,
exp_cleanse_commissions.out_mbrshp_num as mbrshp_num,
exp_cleanse_commissions.out_mbrshp_eff_dt as mbrshp_eff_dt,
exp_cleanse_commissions.plcy_cnt_qut as new_quot_cnt,
exp_cleanse_commissions.plcy_cnt_new_busi as new_bnd_cnt,
exp_cleanse_commissions.prev_mtd_plcy_cnt as pyr_infrc_plcy_cnt,
exp_cleanse_commissions.persis_plcy_cnt as prsistnt_plcy_cnt,
exp_cleanse_commissions.apld_cd,
exp_cleanse_commissions.wrtn_prem,
exp_cleanse_commissions.pd_prem,
exp_cleanse_commissions.new_busn_prem,
exp_cleanse_commissions.out_pd_coms as pd_coms,
exp_cleanse_commissions.new_mbrshp_cnt,
exp_cleanse_commissions.source_record_id
FROM exp_cleanse_commissions
UNION ALL
/* Union Group MEMBERSHIP */
SELECT
exp_cleanse_member_mstr_trans.cal_dt,
exp_cleanse_member_mstr_trans.out_lob_cd as lob_cd,
exp_cleanse_member_mstr_trans.out_lkp_st_cd as st_cd,
exp_cleanse_member_mstr_trans.out_lkp_rgn_cd as rgn_cd,
exp_cleanse_member_mstr_trans.out_lkp_dstrct_cd as dstrct_cd,
exp_cleanse_member_mstr_trans.out_srvc_ctr_cd as srvc_ctr_cd,
exp_cleanse_member_mstr_trans.out_agnt_nbr as agnt_nbr,
exp_cleanse_member_mstr_trans.out_user_id as user_id,
exp_cleanse_member_mstr_trans.out_mbrshp_type_cd as mbrshp_type_cd,
exp_cleanse_member_mstr_trans.out_mbrshp_num as mbrshp_num,
exp_cleanse_member_mstr_trans.mbrshp_eff_dt,
exp_cleanse_member_mstr_trans.out_plcy_cnt_qut as new_quot_cnt,
exp_cleanse_member_mstr_trans.out_plcy_cnt_new_busi as new_bnd_cnt,
exp_cleanse_member_mstr_trans.out_prev_mtd_plcy_cnt as pyr_infrc_plcy_cnt,
exp_cleanse_member_mstr_trans.out_persis_plcy_cnt as prsistnt_plcy_cnt,
exp_cleanse_member_mstr_trans.out_apld_cd as apld_cd,
exp_cleanse_member_mstr_trans.out_wrtn_prem as wrtn_prem,
exp_cleanse_member_mstr_trans.out_pd_prem as pd_prem,
exp_cleanse_member_mstr_trans.out_new_busn_prem as new_busn_prem,
exp_cleanse_member_mstr_trans.out_pd_coms as pd_coms,
exp_cleanse_member_mstr_trans.out_new_mbrshp_cnt as new_mbrshp_cnt,
exp_cleanse_member_mstr_trans.source_record_id
FROM exp_cleanse_member_mstr_trans
UNION ALL
/* Union Group TPC00401_PCE_PD_Y */
SELECT
exp_cleanse_tpc00401_pce_pd_y.out_cal_dt as cal_dt,
exp_cleanse_tpc00401_pce_pd_y.out_lob_cd as lob_cd,
exp_cleanse_tpc00401_pce_pd_y.out_st_cd as st_cd,
exp_cleanse_tpc00401_pce_pd_y.out_rgn_cd as rgn_cd,
exp_cleanse_tpc00401_pce_pd_y.out_dstrct_cd as dstrct_cd,
exp_cleanse_tpc00401_pce_pd_y.out_srvc_ctr_cd as srvc_ctr_cd,
exp_cleanse_tpc00401_pce_pd_y.out_agnt_nbr as agnt_nbr,
exp_cleanse_tpc00401_pce_pd_y.out_user_id as user_id,
exp_cleanse_tpc00401_pce_pd_y.out_mbrshp_type_cd as mbrshp_type_cd,
exp_cleanse_tpc00401_pce_pd_y.out_mbrshp_num as mbrshp_num,
exp_cleanse_tpc00401_pce_pd_y.out_mbrshp_eff_dt as mbrshp_eff_dt,
exp_cleanse_tpc00401_pce_pd_y.out_plcy_cnt_qut as new_quot_cnt,
exp_cleanse_tpc00401_pce_pd_y.out_plcy_cnt_new_busi as new_bnd_cnt,
exp_cleanse_tpc00401_pce_pd_y.out_prev_mtd_plcy_cnt as pyr_infrc_plcy_cnt,
exp_cleanse_tpc00401_pce_pd_y.out_persis_plcy_cnt as prsistnt_plcy_cnt,
exp_cleanse_tpc00401_pce_pd_y.out_apld_cd as apld_cd,
exp_cleanse_tpc00401_pce_pd_y.out_wrtn_prem as wrtn_prem,
exp_cleanse_tpc00401_pce_pd_y.out_pd_prem as pd_prem,
exp_cleanse_tpc00401_pce_pd_y.out_new_busn_prem as new_busn_prem,
exp_cleanse_tpc00401_pce_pd_y.out_pd_coms as pd_coms,
exp_cleanse_tpc00401_pce_pd_y.out_new_mbrshp_cnt as new_mbrshp_cnt,
exp_cleanse_tpc00401_pce_pd_y.source_record_id
FROM exp_cleanse_tpc00401_pce_pd_y
UNION ALL
/* Union Group TPC00501_LEVEL */
SELECT
exp_cleanse_tpc00501_level.out_cal_dt as cal_dt,
exp_cleanse_tpc00501_level.out_lob_cd as lob_cd,
exp_cleanse_tpc00501_level.out_st_cd as st_cd,
exp_cleanse_tpc00501_level.out_rgn_cd as rgn_cd,
exp_cleanse_tpc00501_level.out_dstrct_cd as dstrct_cd,
exp_cleanse_tpc00501_level.out_srvc_ctr_cd as srvc_ctr_cd,
exp_cleanse_tpc00501_level.out_agnt_nbr as agnt_nbr,
exp_cleanse_tpc00501_level.out_user_id as user_id,
exp_cleanse_tpc00501_level.out_mbrshp_type_cd as mbrshp_type_cd,
exp_cleanse_tpc00501_level.out_mbrshp_num as mbrshp_num,
exp_cleanse_tpc00501_level.out_mbrshp_eff_dt as mbrshp_eff_dt,
exp_cleanse_tpc00501_level.out_plcy_cnt_qut as new_quot_cnt,
exp_cleanse_tpc00501_level.out_plcy_cnt_new_busi as new_bnd_cnt,
exp_cleanse_tpc00501_level.out_prev_mtd_plcy_cnt as pyr_infrc_plcy_cnt,
exp_cleanse_tpc00501_level.out_persis_plcy_cnt as prsistnt_plcy_cnt,
exp_cleanse_tpc00501_level.out_apld_cd as apld_cd,
exp_cleanse_tpc00501_level.out_wrtn_prem as wrtn_prem,
exp_cleanse_tpc00501_level.out_pd_prem as pd_prem,
exp_cleanse_tpc00501_level.out_new_busn_prem as new_busn_prem,
exp_cleanse_tpc00501_level.out_pd_coms as pd_coms,
exp_cleanse_tpc00501_level.out_new_mbrshp_cnt as new_mbrshp_cnt,
exp_cleanse_tpc00501_level.source_record_id
FROM exp_cleanse_tpc00501_level
);


-- Component exp_calc_tier_cd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_calc_tier_cd AS
(
SELECT
un_all_sources.cal_dt as cal_dt,
un_all_sources.lob_cd as lob_cd,
un_all_sources.st_cd as st_cd,
un_all_sources.rgn_cd as rgn_cd,
un_all_sources.dstrct_cd as dstrct_cd,
un_all_sources.srvc_ctr_cd as srvc_ctr_cd,
un_all_sources.agnt_nbr as agnt_nbr,
un_all_sources.user_id as user_id,
LKP_1.TIER_CD /* replaced lookup LKP_LIFE_CURR_TIER */ as out_tier_cd,
un_all_sources.mbrshp_type_cd as mbrshp_type_cd,
un_all_sources.mbrshp_num as mbrshp_num,
un_all_sources.mbrshp_eff_dt as mbrshp_eff_dt,
un_all_sources.new_quot_cnt as new_quot_cnt,
un_all_sources.new_bnd_cnt as new_bnd_cnt,
un_all_sources.pyr_infrc_plcy_cnt as pyr_infrc_plcy_cnt,
un_all_sources.prsistnt_plcy_cnt as prsistnt_plcy_cnt,
un_all_sources.apld_cd as apld_cr,
un_all_sources.wrtn_prem as wrtn_prem,
un_all_sources.pd_prem as pd_prem,
un_all_sources.new_busn_prem as new_busn_prem,
un_all_sources.pd_coms as pd_coms,
un_all_sources.new_mbrshp_cnt as new_mbrshp_cnt,
un_all_sources.source_record_id,
row_number() over (partition by un_all_sources.source_record_id order by un_all_sources.source_record_id) as RNK
FROM
un_all_sources
LEFT JOIN LKP_LIFE_CURR_TIER LKP_1 ON LKP_1.CAL_DT = un_all_sources.cal_dt AND LKP_1.AGENT_NBR = un_all_sources.agnt_nbr
QUALIFY RNK = 1
);


-- Component srt_all_dimensions, Type SORTER 
CREATE OR REPLACE TEMPORARY TABLE srt_all_dimensions AS
(
SELECT
exp_calc_tier_cd.cal_dt as cal_dt,
exp_calc_tier_cd.lob_cd as lob_cd,
exp_calc_tier_cd.st_cd as st_cd,
exp_calc_tier_cd.rgn_cd as rgn_cd,
exp_calc_tier_cd.dstrct_cd as dstrct_cd,
exp_calc_tier_cd.srvc_ctr_cd as srvc_ctr_cd,
exp_calc_tier_cd.agnt_nbr as agnt_nbr,
exp_calc_tier_cd.user_id as user_id,
exp_calc_tier_cd.out_tier_cd as out_tier_cd,
exp_calc_tier_cd.mbrshp_type_cd as mbrshp_type_cd,
exp_calc_tier_cd.mbrshp_num as mbrshp_num,
exp_calc_tier_cd.mbrshp_eff_dt as mbrshp_eff_dt,
exp_calc_tier_cd.new_quot_cnt as new_quot_cnt,
exp_calc_tier_cd.new_bnd_cnt as new_bnd_cnt,
exp_calc_tier_cd.pyr_infrc_plcy_cnt as pyr_infrc_plcy_cnt,
exp_calc_tier_cd.prsistnt_plcy_cnt as prsistnt_plcy_cnt,
exp_calc_tier_cd.apld_cr as apld_cr,
exp_calc_tier_cd.wrtn_prem as wrtn_prem,
exp_calc_tier_cd.pd_prem as pd_prem,
exp_calc_tier_cd.new_busn_prem as new_busn_prem,
exp_calc_tier_cd.pd_coms as pd_coms,
exp_calc_tier_cd.new_mbrshp_cnt as new_mbrshp_cnt,
exp_calc_tier_cd.source_record_id
FROM
exp_calc_tier_cd
ORDER BY cal_dt , lob_cd , st_cd , rgn_cd , dstrct_cd , srvc_ctr_cd , agnt_nbr , user_id , out_tier_cd , mbrshp_type_cd , mbrshp_num , mbrshp_eff_dt 
);


-- Component agg_groupby_columns, Type AGGREGATOR 
CREATE OR REPLACE TEMPORARY TABLE agg_groupby_columns AS
(
/*SELECT
srt_all_dimensions.cal_dt as cal_dt,
srt_all_dimensions.lob_cd as lob_cd,
srt_all_dimensions.st_cd as st_cd,
srt_all_dimensions.rgn_cd as rgn_cd,
srt_all_dimensions.dstrct_cd as dstrct_cd,
srt_all_dimensions.srvc_ctr_cd as srvc_ctr_cd,
srt_all_dimensions.agnt_nbr as agnt_nbr,
srt_all_dimensions.user_id as user_id,
srt_all_dimensions.out_tier_cd as tier_cd,
srt_all_dimensions.mbrshp_type_cd as mbrshp_type_cd,
srt_all_dimensions.mbrshp_num as mbrshp_num,
srt_all_dimensions.mbrshp_eff_dt as mbrshp_eff_dt,
MIN(srt_all_dimensions.new_quot_cnt) as in_new_quot_cnt,
sum(in_new_quot_cnt) as out_new_quot_cnt,
MIN(srt_all_dimensions.new_bnd_cnt) as in_new_bnd_cnt,
sum(in_new_bnd_cnt) as out_new_bnd_cnt,
MIN(srt_all_dimensions.pyr_infrc_plcy_cnt) as in_pyr_infrc_plcy_cnt,
sum(in_pyr_infrc_plcy_cnt) as out_pyr_infrc_plcy_cnt,
MIN(srt_all_dimensions.prsistnt_plcy_cnt) as in_prsistnt_plcy_cnt,
sum(in_prsistnt_plcy_cnt) as out_prsistnt_plcy_cnt,
MIN(srt_all_dimensions.apld_cr) as in_apld_cr,
sum(in_apld_cr) as out_apld_cr,
MIN(srt_all_dimensions.wrtn_prem) as in_wrtn_prem,
sum(in_wrtn_prem) as out_wrtn_prem,
MIN(srt_all_dimensions.pd_prem) as in_pd_prem,
sum(in_pd_prem) as out_pd_prem,
MIN(srt_all_dimensions.new_busn_prem) as in_new_busn_prem,
sum(in_new_busn_prem) as out_new_busn_prem,
MIN(srt_all_dimensions.pd_coms) as in_pd_coms,
sum(in_pd_coms) as out_pd_coms,
MIN(srt_all_dimensions.new_mbrshp_cnt) as in_new_mbrshp_cnt,
sum(in_new_mbrshp_cnt) as out_new_mbrshp_cnt,
MIN(srt_all_dimensions.source_record_id) as source_record_id
FROM
srt_all_dimensions
GROUP BY
srt_all_dimensions.cal_dt,
srt_all_dimensions.lob_cd,
srt_all_dimensions.st_cd,
srt_all_dimensions.rgn_cd,
srt_all_dimensions.dstrct_cd,
srt_all_dimensions.srvc_ctr_cd,
srt_all_dimensions.agnt_nbr,
srt_all_dimensions.user_id,
srt_all_dimensions.out_tier_cd,
srt_all_dimensions.mbrshp_type_cd,
srt_all_dimensions.mbrshp_num,
srt_all_dimensions.mbrshp_eff_dt*/
WITH inner_cte AS (
  SELECT
    cal_dt,
    lob_cd,
    st_cd,
    rgn_cd,
    dstrct_cd,
    srvc_ctr_cd,
    agnt_nbr,
    user_id,
    out_tier_cd AS tier_cd,
    mbrshp_type_cd,
    mbrshp_num,
    mbrshp_eff_dt,
    MIN(new_quot_cnt) AS in_new_quot_cnt,
    MIN(new_bnd_cnt)   AS in_new_bnd_cnt,
    MIN(pyr_infrc_plcy_cnt) AS in_pyr_infrc_plcy_cnt,
    MIN(prsistnt_plcy_cnt) AS in_prsistnt_plcy_cnt,
    MIN(apld_cr)       AS in_apld_cr,
    MIN(wrtn_prem)     AS in_wrtn_prem,
    MIN(pd_prem)       AS in_pd_prem,
    MIN(new_busn_prem) AS in_new_busn_prem,
    MIN(pd_coms)       AS in_pd_coms,
    MIN(new_mbrshp_cnt) AS in_new_mbrshp_cnt,
    MIN(source_record_id) AS source_record_id
  FROM srt_all_dimensions
  GROUP BY
    cal_dt, lob_cd, st_cd, rgn_cd, dstrct_cd,
    srvc_ctr_cd, agnt_nbr, user_id,
    out_tier_cd, mbrshp_type_cd, mbrshp_num, mbrshp_eff_dt
)
SELECT
  cal_dt,
  lob_cd,
  st_cd,
  rgn_cd,
  dstrct_cd,
  srvc_ctr_cd,
  agnt_nbr,
  user_id,
  tier_cd,
  mbrshp_type_cd,
  mbrshp_num,
  mbrshp_eff_dt,
  in_new_quot_cnt,
  SUM(in_new_quot_cnt)         AS out_new_quot_cnt,
  in_new_bnd_cnt,
  SUM(in_new_bnd_cnt)          AS out_new_bnd_cnt,
  in_pyr_infrc_plcy_cnt,
  SUM(in_pyr_infrc_plcy_cnt)   AS out_pyr_infrc_plcy_cnt,
  in_prsistnt_plcy_cnt,
  SUM(in_prsistnt_plcy_cnt)    AS out_prsistnt_plcy_cnt,
  in_apld_cr,
  SUM(in_apld_cr)              AS out_apld_cr,
  in_wrtn_prem,
  SUM(in_wrtn_prem)            AS out_wrtn_prem,
  in_pd_prem,
  SUM(in_pd_prem)              AS out_pd_prem,
  in_new_busn_prem,
  SUM(in_new_busn_prem)        AS out_new_busn_prem,
  in_pd_coms,
  SUM(in_pd_coms)              AS out_pd_coms,
  in_new_mbrshp_cnt,
  SUM(in_new_mbrshp_cnt)       AS out_new_mbrshp_cnt,
  source_record_id
FROM inner_cte
GROUP BY
  cal_dt, lob_cd, st_cd, rgn_cd, dstrct_cd,
  srvc_ctr_cd, agnt_nbr, user_id, tier_cd,
  mbrshp_type_cd, mbrshp_num, mbrshp_eff_dt,
  in_new_quot_cnt, in_new_bnd_cnt,
  in_pyr_infrc_plcy_cnt, in_prsistnt_plcy_cnt,
  in_apld_cr, in_wrtn_prem, in_pd_prem,
  in_new_busn_prem, in_pd_coms, in_new_mbrshp_cnt, source_record_id
);


-- Component fil_allow_non_zero_metrics, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_allow_non_zero_metrics AS
(
SELECT
agg_groupby_columns.cal_dt as cal_dt,
agg_groupby_columns.lob_cd as lob_cd,
agg_groupby_columns.st_cd as st_cd,
agg_groupby_columns.rgn_cd as rgn_cd,
agg_groupby_columns.dstrct_cd as dstrct_cd,
agg_groupby_columns.srvc_ctr_cd as srvc_ctr_cd,
agg_groupby_columns.agnt_nbr as agnt_nbr,
agg_groupby_columns.user_id as user_id,
agg_groupby_columns.tier_cd as tier_cd,
agg_groupby_columns.mbrshp_type_cd as mbrshp_type_cd,
agg_groupby_columns.mbrshp_num as mbrshp_num,
agg_groupby_columns.mbrshp_eff_dt as mbrshp_eff_dt,
agg_groupby_columns.out_new_quot_cnt as new_quot_cnt,
agg_groupby_columns.out_new_bnd_cnt as new_bnd_cnt,
agg_groupby_columns.out_pyr_infrc_plcy_cnt as pyr_infrc_plcy_cnt,
agg_groupby_columns.out_prsistnt_plcy_cnt as prsistnt_plcy_cnt,
agg_groupby_columns.out_apld_cr as apld_cr,
agg_groupby_columns.out_wrtn_prem as wrtn_prem,
agg_groupby_columns.out_pd_prem as pd_prem,
agg_groupby_columns.out_new_busn_prem as new_busn_prem,
agg_groupby_columns.out_pd_coms as pc_coms,
agg_groupby_columns.out_new_mbrshp_cnt as new_mbrshp_cnt,
agg_groupby_columns.source_record_id
FROM
agg_groupby_columns
WHERE agg_groupby_columns.out_new_bnd_cnt <> 0 or agg_groupby_columns.out_new_quot_cnt <> 0 or agg_groupby_columns.out_prsistnt_plcy_cnt <> 0 or agg_groupby_columns.out_pyr_infrc_plcy_cnt <> 0 or agg_groupby_columns.out_apld_cr <> 0 or agg_groupby_columns.out_wrtn_prem <> 0 or agg_groupby_columns.out_pd_prem <> 0 or agg_groupby_columns.out_new_busn_prem <> 0 or agg_groupby_columns.out_pd_coms <> 0 or agg_groupby_columns.out_new_mbrshp_cnt <> 0
);


-- Component tgt_wr_agnt_sumry_ins, Type TARGET 
INSERT INTO DB_V_PROD_PRES.WR_AGNT_SUMRY
(
CAL_DT,
LOB_CD,
ST_CD,
RGN_CD,
DSTRCT_CD,
SRVC_CTR_CD,
AGNT_NBR,
USER_ID,
TIER_CD,
MBRSHP_TYPE_CD,
MBRSHP_NUM,
MBRSHP_EFF_DT,
NEW_QUOT_CNT,
NEW_BND_CNT,
PYR_INFRC_PLCY_CNT,
PRSISTNT_PLCY_CNT,
APLD_CR,
WRTN_PREM,
PD_PREM,
NEW_BUSN_PREM,
PD_COMS,
NEW_MBRSHP_CNT
)
SELECT
fil_allow_non_zero_metrics.cal_dt as CAL_DT,
fil_allow_non_zero_metrics.lob_cd as LOB_CD,
fil_allow_non_zero_metrics.st_cd as ST_CD,
fil_allow_non_zero_metrics.rgn_cd as RGN_CD,
fil_allow_non_zero_metrics.dstrct_cd as DSTRCT_CD,
fil_allow_non_zero_metrics.srvc_ctr_cd as SRVC_CTR_CD,
fil_allow_non_zero_metrics.agnt_nbr as AGNT_NBR,
fil_allow_non_zero_metrics.user_id as USER_ID,
fil_allow_non_zero_metrics.tier_cd as TIER_CD,
fil_allow_non_zero_metrics.mbrshp_type_cd as MBRSHP_TYPE_CD,
fil_allow_non_zero_metrics.mbrshp_num as MBRSHP_NUM,
fil_allow_non_zero_metrics.mbrshp_eff_dt as MBRSHP_EFF_DT,
fil_allow_non_zero_metrics.new_quot_cnt as NEW_QUOT_CNT,
fil_allow_non_zero_metrics.new_bnd_cnt as NEW_BND_CNT,
fil_allow_non_zero_metrics.pyr_infrc_plcy_cnt as PYR_INFRC_PLCY_CNT,
fil_allow_non_zero_metrics.prsistnt_plcy_cnt as PRSISTNT_PLCY_CNT,
fil_allow_non_zero_metrics.apld_cr as APLD_CR,
fil_allow_non_zero_metrics.wrtn_prem as WRTN_PREM,
fil_allow_non_zero_metrics.pd_prem as PD_PREM,
fil_allow_non_zero_metrics.new_busn_prem as NEW_BUSN_PREM,
fil_allow_non_zero_metrics.pc_coms as PD_COMS,
fil_allow_non_zero_metrics.new_mbrshp_cnt as NEW_MBRSHP_CNT
FROM
fil_allow_non_zero_metrics;


END; ';