-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_MCAS_POLICIES_AUTO_DETAIL("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
  MCAS_END_DT STRING;
  MCAS_START_DT STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);

  MCAS_END_DT := public.func_get_scoped_param(:run_id, ''mcas_end_dt'', :workflow_name, :worklet_name, :session_name);
  MCAS_START_DT := public.func_get_scoped_param(:run_id, ''mcas_start_dt'', :workflow_name, :worklet_name, :session_name);
 

-- Component SQ_AGMT, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_AGMT AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as UNDRWRTG_CMPY,
$2 as RISK_STATE,
$3 as INSRNC_LOB_TYPE_CD,
$4 as TRANS_PREM_AMT,
$5 as INSRNC_MTRC_TYPE_CD,
$6 as MODL_NUM,
$7 as TERM_NUM,
$8 as AGMT_ID,
$9 as AGMT_STS_CD,
$10 as AGMT_STS_SRC_TYPE_CD,
$11 as AGMT_CUR_STS_RSN_CD,
$12 as HOST_AGMT_NUM,
$13 as AGMT_OPN_DTTM,
$14 as AGMT_EFF_DTTM,
$15 as NEW_AGMT_EFF_DTTM,
$16 as NON_RENEWAL_DTTM,
$17 as NON_RENEWAL_STS,
$18 as AGMT_PLND_EXPN_DTTM,
$19 as AGMT_CUR_STS_CD,
$20 as CAN_DTTM,
$21 as CAN_RSN_CD,
$22 as CAN_SRC,
$23 as REWRT_DTTM,
$24 as RNST_DTTM,
$25 as QUOTN_STS_TYPE_CD,
$26 as MODL_CRTN_DTTM,
$27 as MOTR_VEH_CNT,
$28 as IND,
$29 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
WITH MCAS_AUTO_DETAIL_CANCEL AS 

(

SELECT DISTINCT COMPANY,

STATE,LOB_TYPE,

CAST(''0.00'' AS DECIMAL(15,2)) AS PREMIUM,

CAST(INSRNC_MTRC_TYPE_CD AS VARCHAR(50))as INSRNC_MTRC_TYPE_CD ,

MODL_NUM,TERM_NUM,AGMT_ID,AGMT_STS_CD,AGMT_STS_SRC_TYPE_CD,

AGMT_CUR_STS_RSN_CD,

HOST_AGMT_NUM,

AGMT_OPN_DTTM,AGMT_EFF_DTTM,NEW_AGMT_EFF_DTTM,

NON_RENEWAL_DTTM,

CAST(NON_RENEWAL_STS AS VARCHAR(50)) AS NON_RENEWAL_STS,

AGMT_PLND_EXPN_DTTM,

CAST(AGMT_CUR_STS_CD AS VARCHAR(50)) AS AGMT_CUR_STS_CD,

CAN_DTTM,CAST(CAN_RSN_CD AS VARCHAR(50)) AS CAN_RSN_CD,CAST(CAN_SRC AS VARCHAR(50)) AS CAN_SRC,

REWRT_DTTM,RNST_DTTM,

CAST(QUOTN_STS_TYPE_CD AS VARCHAR(50)) AS QUOTN_STS_TYPE_CD,

MODL_CRTN_DTTM,

CAST(MOTR_VEH_CNT as INTEGER)AS MOTR_VEH_CNT,

CAST(''C'' AS VARCHAR(10)) AS IND

FROM (

SELECT DISTINCT AGT.*, CMP.COMPANY,RSKSTATE.GEOGRCL_AREA_SHRT_NAME AS STATE,REWRT.REWRT_DTTM,RNST.RNST_DTTM,

LOB.INSRNC_LOB_TYPE_CD as LOB_TYPE,

PMT.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,

NON_RENEWAL_STS,NON_RENEWAL_DTTM,

CAN_RSN_CD,CAN_DTTM,CAN_SRC,MOTR_VEH_CNT

FROM (

SELECT PPV.AGMT_ID

     ,PPV.HOST_AGMT_NUM

     ,PPV.AGMT_OPN_DTTM

     ,PPV.AGMT_EFF_DTTM

     ,PPV.AGMT_PLND_EXPN_DTTM

     ,PPV.TERM_NUM,PPV.MODL_NUM

     ,PPV.LGCY_PLCY_IND,PPV.AGMT_CUR_STS_CD

     ,PPV.AGMT_CUR_STS_RSN_CD,PPV.NEW_AGMT_EFF_DTTM,PPV.MODL_CRTN_DTTM

     ,IQ.QUOTN_STS_TYPE_CD

     ,PPV.AGMT_PRCSG_DTTM,A_S.AGMT_STS_STRT_DTTM,A_S.AGMT_STS_CD,A_S.AGMT_STS_SRC_TYPE_CD FROM (

    SELECT DISTINCT

      A.AGMT_ID

     ,A.HOST_AGMT_NUM

     ,A.AGMT_OPN_DTTM

     ,A.AGMT_EFF_DTTM

     ,A.AGMT_PLND_EXPN_DTTM

     ,A.TERM_NUM,A.MODL_NUM

     ,A.LGCY_PLCY_IND,A.AGMT_PRCSG_DTTM,A.MODL_CRTN_DTTM,

     A.AGMT_CUR_STS_CD,A.AGMT_CUR_STS_RSN_CD,

     CASE    WHEN A.MODL_EFF_DTTM > A.MODL_CRTN_DTTM THEN A.MODL_EFF_DTTM 

    ELSE    A.MODL_CRTN_DTTM 

    END     AS NEW_AGMT_EFF_DTTM

     FROM EVIEWDB_EDW.AGMT A

     INNER JOIN EVIEWDB_EDW.AGMT_PROD AP ON A.AGMT_ID=AP.AGMT_ID AND AP.AGMT_PROD_ROLE_CD=''PLCYTYPE''

     INNER JOIN EVIEWDB_EDW.PROD P ON AP.PROD_ID=P.PROD_ID AND P.INSRNC_LOB_TYPE_CD IN (''PA'')

     WHERE A.AGMT_TYPE_CD = ''PPV''

	 AND A.SRC_SYS_CD = ''GWPC'' 

	 AND PROD_NAME IN (''PPV'',''PPV2'')

    AND     A.TRANS_STRT_DTTM = 

    (

    SELECT  MIN(T2.TRANS_STRT_DTTM) 

    FROM    EVIEWDB_EDW.AGMT T2 

    WHERE   A.AGMT_ID = T2.AGMT_ID)   

     

    ) PPV

    LEFT JOIN 

    (

    SELECT  HOST_AGMT_NUM, TERM_NUM,AGMT_ID,AGMT_TYPE_CD 

    FROM    EVIEWDB_EDW.AGMT 

    WHERE   AGMT_TYPE_CD = ''POLTRM'' 

    AND     to_date(:MCAS_End_dt, ''MM/DD/YYYY'')  BETWEEN to_date(AGMT_EFF_DTTM, ''MM/DD/YYYY'') AND to_date(AGMT_PLND_EXPN_DTTM, ''MM/DD/YYYY'')

    GROUP   BY 1,2,3,4) TRM 

            ON      PPV.HOST_AGMT_NUM = TRM.HOST_AGMT_NUM 

            AND     PPV.TERM_NUM = TRM.TERM_NUM  

    INNER JOIN 

    EVIEWDB_EDW.AGMT_STS A_S 

            ON      PPV.AGMT_ID = A_S.AGMT_ID  

			AND to_date(A_S.AGMT_STS_STRT_DTTM, ''MM/DD/YYYY'')  <=  to_date(:MCAS_End_dt, ''MM/DD/YYYY'') 

            AND     A_S.AGMT_STS_CD <> ''CNFRMDDT'' 

    

    LEFT OUTER JOIN EVIEWDB_EDW.AGMT_ASSET AA ON AA.AGMT_ID = PPV.AGMT_ID  AND CAST(AA.EDW_END_DTTM AS DATE)=''9999-12-31'' 

    LEFT  OUTER JOIN EVIEWDB_EDW.APLCTN AP ON AP.HOST_APLCTN_NUM=PPV.HOST_AGMT_NUM AND CAST(AP.EDW_END_DTTM AS DATE)=''9999-12-31'' 

    LEFT OUTER JOIN EVIEWDB_EDW.INSRNC_QUOTN IQ ON AP.APLCTN_ID = IQ.APLCTN_ID AND CAST(IQ.EDW_END_DTTM AS DATE)=''9999-12-31'' 

    

    QUALIFY ROW_NUMBER() OVER(PARTITION BY PPV.AGMT_ID 

    ORDER   BY A_S.AGMT_STS_STRT_DTTM DESC) = 1 



)AGT





/* COMPANY  */
INNER JOIN  (

SELECT DISTINCT 

   PA.AGMT_ID

   ,INTRNL_ORG_NUM COMPANY

   ,PRTY_AGMT_ROLE_CD

   FROM EVIEWDB_EDW.PRTY_AGMT PA

   LEFT JOIN EVIEWDB_EDW. INTRNL_ORG IO ON PA.PRTY_ID = IO.INTRNL_ORG_PRTY_ID

   WHERE PA.PRTY_AGMT_ROLE_CD = ''CMP''

   AND CAST(IO.EDW_END_DTTM AS DATE) = ''9999-12-31''

   AND INTRNL_ORG_SBTYPE_CD = ''CO''

   AND CAST(PA.EDW_END_DTTM AS DATE) = ''9999-12-31''

) CMP ON AGT.AGMT_ID = CMP.AGMT_ID



/* RISK STATE  */
INNER JOIN 

(

SELECT  A.AGMT_ID, C.GEOGRCL_AREA_SHRT_NAME /* AS RISKSTATE */
FROM    

 ( 

SELECT DISTINCT AGMT_ID,LOC_ID, AGMT_LOCTR_ROLE_TYPE_CD

FROM    EVIEWDB_EDW.AGMT_LOCTR 

WHERE   CAST(EDW_END_DTTM AS DATE) = ''9999-12-31'')  A 

INNER JOIN EVIEWDB_EDW.LOCTR B 

    ON  (A.LOC_ID = B.LOCTR_ID 

    AND B.GEOGRCL_AREA_SBTYPE_CD = ''TER'')

INNER JOIN EVIEWDB_EDW.TERR C 

    ON  A.LOC_ID = C.TERR_ID 

    AND CAST(C.EDW_END_DTTM AS DATE) = ''9999-12-31''

WHERE   A.AGMT_LOCTR_ROLE_TYPE_CD = ''AGTWINST''    

) RSKSTATE  

    ON  AGT.AGMT_ID = RSKSTATE.AGMT_ID

    

/* LOB */
INNER JOIN 

(

SELECT  B.AGMT_ID, decode(C.INSRNC_LOB_TYPE_CD,''PA'',''A'') as INSRNC_LOB_TYPE_CD/*  LOB_TYPE */
FROM    

   ( 

SELECT  DISTINCT PROD_ID,AGMT_ID 

FROM    EVIEWDB_EDW.AGMT_PROD 

WHERE   CAST(EDW_END_DTTM AS DATE)=''9999-12-31'') B 

INNER JOIN EVIEWDB_EDW.PROD C 

    ON  B.PROD_ID = C.PROD_ID 

    and C.INSRNC_LOB_TYPE_CD in (''PA'') 

    AND CAST(C.EDW_END_DTTM AS DATE)=''9999-12-31''

) LOB 

    ON  agt.AGMT_ID = LOB.AGMT_ID 



/*  Date Rewritten */


LEFT JOIN ( 

	SELECT  HOST_AGMT_NUM ,AG.MODL_EFF_DTTM(DATE) AS REWRT_DTTM

FROM EVIEWDB_EDW.AGMT AG

JOIN EVIEWDB_EDW.EV_STS ES ON ES.AGMT_ID = AG.AGMT_ID       

JOIN EVIEWDB_EDW.EV EVV ON EVV.EV_ID =ES.EV_ID  AND EV_ACTVY_TYPE_CD=''REWRT''

WHERE  to_date(AG.MODL_EFF_DTTM, ''MM/DD/YYYY'') BETWEEN to_date(:MCAS_Start_dt, ''MM/DD/YYYY'') AND to_date(:MCAS_End_dt, ''MM/DD/YYYY'')

AND MODL_CRTN_DTTM(DATE) is not null

	) REWRT 

	on	agt.HOST_AGMT_NUM=REWRT.HOST_AGMT_NUM



/* Date Reinstated */


LEFT JOIN ( 

	SELECT  HOST_AGMT_NUM ,AG.MODL_EFF_DTTM(DATE) AS RNST_DTTM

FROM EVIEWDB_EDW.AGMT AG

JOIN EVIEWDB_EDW.EV_STS ES ON ES.AGMT_ID = AG.AGMT_ID         

JOIN EVIEWDB_EDW.EV EVV ON EVV.EV_ID =ES.EV_ID 

AND EV_ACTVY_TYPE_CD=''REINSTATE''

WHERE  to_date(AG.MODL_EFF_DTTM, ''MM/DD/YYYY'')  BETWEEN to_date(:MCAS_Start_dt, ''MM/DD/YYYY'') AND to_date(:MCAS_End_dt, ''MM/DD/YYYY'')

AND MODL_CRTN_DTTM(DATE)  is not null



) RNST 

	on	agt.HOST_AGMT_NUM=RNST.HOST_AGMT_NUM

	

/* Insurance Metric Type Code */


LEFT JOIN  

( SELECT AGMT_ID,INSRNC_MTRC_TYPE_CD FROM EVIEWDB_EDW.PLCY_MTRC PLCY_MTRC WHERE CAST(EDW_END_DTTM as date)=''9999-12-31'' and INSRNC_MTRC_TYPE_CD=''PREM''

) PMT

ON PMT.AGMT_ID=AGT.AGMT_ID  



/*  Non Renewal date and Non Renewal Status */


LEFT JOIN 

(

SELECT AG.HOST_AGMT_NUM as HOST_AGMT_NUM,QUOTN_STS_TYPE_CD as NON_RENEWAL_STS,

max(IQ.QUOTN_PLND_AGMT_OPN_DTTM) as NON_RENEWAL_DTTM

FROM EVIEWDB_EDW.INSRNC_QUOTN IQ

JOIN EVIEWDB_EDW.APLCTN AP ON AP.APLCTN_ID = IQ.APLCTN_ID

JOIN EVIEWDB_EDW.AGMT AG on AP.HOST_APLCTN_NUM = AG.HOST_AGMT_NUM

WHERE  QUOTN_STS_TYPE_CD = ''NONRNWD''

AND to_date(QUOTN_PLND_AGMT_OPN_DTTM, ''MM/DD/YYYY'') BETWEEN  to_date(:MCAS_Start_dt, ''MM/DD/YYYY'') AND to_date(:MCAS_End_dt, ''MM/DD/YYYY'') 

group by 1,2

)NON_DTTM

ON NON_DTTM.HOST_AGMT_NUM=AGT.HOST_AGMT_NUM



/*  Cancellation Date and Cancellation Reason code */


LEFT JOIN

(

SELECT max(AGMT_STS_STRT_DTTM) as CAN_DTTM,  AGMT_STS_RSN_CD as CAN_RSN_CD, 

          AGMT_STS_SRC_TYPE_CD as CAN_SRC, agmt.HOST_AGMT_NUM as HOST_AGMT_NUM,AGMT.AGMT_ID 

        FROM EVIEWDB_EDW.AGMT_STS AGMT_STS

        JOIN EVIEWDB_EDW.AGMT AGMT on AGMT_STS.AGMT_ID = AGMT.AGMT_ID   

        and AGMT_STS_CD = ''CNCLD''  

         group by AGMT.AGMT_ID,CAN_RSN_CD,CAN_SRC,HOST_AGMT_NUM

)CAN_DTTM

ON CAN_DTTM.AGMT_ID=AGT.AGMT_ID





/* MOTOR VEH */


INNER JOIN

(

SELECT AA.AGMT_ID, count(DISTINCT MOTR_VEH_SER_NUM) as MOTR_VEH_CNT 

FROM EVIEWDB_EDW.AGMT_ASSET AA

INNER JOIN EVIEWDB_EDW.PRTY_ASSET PA ON PA.PRTY_ASSET_ID = AA.PRTY_ASSET_ID

INNER JOIN EVIEWDB_EDW.MOTR_VEH MV ON PA.PRTY_ASSET_ID=MV.PRTY_ASSET_ID

WHERE CAST(PA.EDW_END_DTTM as date)=''9999-12-31'' AND CAST(MV.EDW_END_DTTM as date)=''9999-12-31''

AND cast(AA.EDW_END_DTTM as date)=''9999-12-31''

AND MV.MOTR_VEH_TYPE_CD IN(''Special'',''PU'',''PublicTransport'',''PV'',''VN'',''MH'',''MC'',''SV'',''RK'',''MS'',''PP'',''GM'',''other'',''auto'')

AND MV.MOTR_VEH_SER_NUM is not null

GROUP BY AA.AGMT_ID

) MVCNT

on MVCNT.AGMT_ID = AGT.AGMT_ID



    )OVERALL



),

	

MCAS_AUTO_DETAIL_NONRENEWAL AS 

(

SELECT DISTINCT COMPANY,

STATE,LOB_TYPE,

CAST(''0.00'' AS DECIMAL(15,2)) AS PREMIUM,

INSRNC_MTRC_TYPE_CD,

MODL_NUM,TERM_NUM,AGMT_ID,AGMT_STS_CD,AGMT_STS_SRC_TYPE_CD,

AGMT_CUR_STS_RSN_CD,

HOST_AGMT_NUM,

AGMT_OPN_DTTM,AGMT_EFF_DTTM,NEW_AGMT_EFF_DTTM,

NON_RENEWAL_DTTM,

NON_RENEWAL_STS,AGMT_PLND_EXPN_DTTM,AGMT_CUR_STS_CD,

CAN_DTTM,CAN_RSN_CD,CAN_SRC,

REWRT_DTTM,RNST_DTTM,

QUOTN_STS_TYPE_CD,

MODL_CRTN_DTTM,

MOTR_VEH_CNT,

CAST(''NR'' AS VARCHAR(10)) AS IND

FROM (

SELECT DISTINCT AGT.*, CMP.COMPANY,RSKSTATE.GEOGRCL_AREA_SHRT_NAME AS STATE,REWRT.REWRT_DTTM,RNST.RNST_DTTM,

LOB.INSRNC_LOB_TYPE_CD as LOB_TYPE,

PMT.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,

NON_RENEWAL_STS,NON_RENEWAL_DTTM,

CAN_RSN_CD,CAN_DTTM,CAN_SRC,MOTR_VEH_CNT

FROM (

SELECT PPV.AGMT_ID

     ,PPV.HOST_AGMT_NUM

     ,PPV.AGMT_OPN_DTTM

     ,PPV.AGMT_EFF_DTTM

     ,PPV.AGMT_PLND_EXPN_DTTM

     ,PPV.TERM_NUM,PPV.MODL_NUM

     ,PPV.LGCY_PLCY_IND,PPV.AGMT_CUR_STS_CD

     ,PPV.AGMT_CUR_STS_RSN_CD,PPV.NEW_AGMT_EFF_DTTM,PPV.MODL_CRTN_DTTM

     ,IQ.QUOTN_STS_TYPE_CD

     ,PPV.AGMT_PRCSG_DTTM,A_S.AGMT_STS_STRT_DTTM,A_S.AGMT_STS_CD,A_S.AGMT_STS_SRC_TYPE_CD,IQ.QUOTN_ID FROM (

    SELECT DISTINCT

      A.AGMT_ID

     ,A.HOST_AGMT_NUM

     ,A.AGMT_OPN_DTTM

     ,A.AGMT_EFF_DTTM

     ,A.AGMT_PLND_EXPN_DTTM

     ,A.TERM_NUM,A.MODL_NUM

     ,A.LGCY_PLCY_IND,A.AGMT_PRCSG_DTTM,A.MODL_CRTN_DTTM,

     A.AGMT_CUR_STS_CD,A.AGMT_CUR_STS_RSN_CD,

     CASE    WHEN A.MODL_EFF_DTTM > A.MODL_CRTN_DTTM THEN A.MODL_EFF_DTTM 

    ELSE    A.MODL_CRTN_DTTM 

    END     AS NEW_AGMT_EFF_DTTM

     FROM EVIEWDB_EDW.AGMT A

     INNER JOIN EVIEWDB_EDW.AGMT_PROD AP ON A.AGMT_ID=AP.AGMT_ID AND AP.AGMT_PROD_ROLE_CD=''PLCYTYPE''

     INNER JOIN EVIEWDB_EDW.PROD P ON AP.PROD_ID=P.PROD_ID AND P.INSRNC_LOB_TYPE_CD IN (''PA'')

     WHERE A.AGMT_TYPE_CD = ''PPV''

	 AND A.SRC_SYS_CD = ''GWPC'' 

	 AND PROD_NAME IN (''PPV'',''PPV2'')

    AND     A.TRANS_STRT_DTTM = 

    (

    SELECT  MIN(T2.TRANS_STRT_DTTM) 

    FROM    EVIEWDB_EDW.AGMT T2 

    WHERE   A.AGMT_ID = T2.AGMT_ID)   

     

    ) PPV

    LEFT JOIN 

    (

    SELECT  HOST_AGMT_NUM, TERM_NUM,AGMT_ID,AGMT_TYPE_CD 

    FROM    EVIEWDB_EDW.AGMT AGMT

    WHERE   AGMT_TYPE_CD = ''POLTRM'' 

	AND     to_date(:MCAS_End_dt, ''MM/DD/YYYY'')  BETWEEN to_date(AGMT_EFF_DTTM, ''MM/DD/YYYY'') AND to_date(AGMT_PLND_EXPN_DTTM, ''MM/DD/YYYY'')

    GROUP   BY 1,2,3,4) TRM 

            ON      PPV.HOST_AGMT_NUM = TRM.HOST_AGMT_NUM 

            AND     PPV.TERM_NUM = TRM.TERM_NUM  

    INNER JOIN 

    EVIEWDB_EDW.AGMT_STS A_S 

            ON      PPV.AGMT_ID = A_S.AGMT_ID  

            AND     A_S.AGMT_STS_STRT_DTTM <= to_date(:MCAS_End_dt, ''MM/DD/YYYY'')  

            AND     A_S.AGMT_STS_CD <> ''CNFRMDDT'' 

    

 

    LEFT  OUTER JOIN EVIEWDB_EDW.APLCTN AP ON AP.HOST_APLCTN_NUM=PPV.HOST_AGMT_NUM AND CAST(AP.EDW_END_DTTM AS DATE)=''9999-12-31'' 

    INNER JOIN EVIEWDB_EDW.INSRNC_QUOTN IQ ON AP.APLCTN_ID = IQ.APLCTN_ID AND CAST(IQ.EDW_END_DTTM AS DATE)=''9999-12-31'' AND QUOTN_STS_TYPE_CD = ''NONRNWD''

	INNER JOIN EVIEWDB_EDW.QUOTN_ASSET QA ON QA.QUOTN_ID=IQ.QUOTN_ID

INNER JOIN EVIEWDB_EDW.PRTY_ASSET PA ON PA.PRTY_ASSET_ID = QA.PRTY_ASSET_ID

INNER JOIN EVIEWDB_EDW.MOTR_VEH MV ON PA.PRTY_ASSET_ID=MV.PRTY_ASSET_ID

WHERE CAST(PA.EDW_END_DTTM as date)=''9999-12-31'' AND CAST(MV.EDW_END_DTTM as date)=''9999-12-31''

AND cast(QA.EDW_END_DTTM as date)=''9999-12-31''

AND MV.MOTR_VEH_TYPE_CD IN(''Special'',''PU'',''PublicTransport'',''PV'',''VN'',''MH'',''MC'',''SV'',''RK'',''MS'',''PP'',''GM'',''other'',''auto'')

    

    QUALIFY ROW_NUMBER() OVER(PARTITION BY PPV.AGMT_ID 

    ORDER   BY A_S.AGMT_STS_STRT_DTTM DESC) = 1 



)AGT





/* COMPANY  */
INNER JOIN  (

SELECT DISTINCT 

   PQ.QUOTN_ID

   ,INTRNL_ORG_NUM COMPANY

   ,PRTY_QUOTN_ROLE_CD 

   FROM EVIEWDB_EDW.PRTY_QUOTN PQ

   LEFT JOIN EVIEWDB_EDW. INTRNL_ORG IO ON PQ.PRTY_ID = IO.INTRNL_ORG_PRTY_ID

   WHERE PQ.PRTY_QUOTN_ROLE_CD = ''CMP''

   AND CAST(IO.EDW_END_DTTM AS DATE) = ''9999-12-31''

   AND INTRNL_ORG_SBTYPE_CD = ''CO''

   AND CAST(PQ.EDW_END_DTTM AS DATE) = ''9999-12-31''

) CMP ON AGT.QUOTN_ID = CMP.QUOTN_ID



/* RISK STATE  */
INNER JOIN 

(

SELECT  A.QUOTN_ID, C.GEOGRCL_AREA_SHRT_NAME /* AS RISKSTATE */
FROM    

 ( 

SELECT DISTINCT QUOTN_ID,LOC_ID, QUOTN_LOCTR_ROLE_TYPE_CD

FROM    EVIEWDB_EDW.QUOTN_LOCTR 

WHERE   CAST(EDW_END_DTTM AS DATE) = ''9999-12-31'')  A 

INNER JOIN EVIEWDB_EDW.LOCTR B 

    ON  (A.LOC_ID = B.LOCTR_ID 

    AND B.GEOGRCL_AREA_SBTYPE_CD = ''TER'')

INNER JOIN EVIEWDB_EDW.TERR C 

    ON  A.LOC_ID = C.TERR_ID 

    AND CAST(C.EDW_END_DTTM AS DATE) = ''9999-12-31''

WHERE   A.QUOTN_LOCTR_ROLE_TYPE_CD = ''AGTWINST''    

) RSKSTATE  

    ON  AGT.QUOTN_ID = RSKSTATE.QUOTN_ID

    

/* LOB */
INNER JOIN 

(

SELECT  B.AGMT_ID, decode(C.INSRNC_LOB_TYPE_CD,''PA'',''A'') as INSRNC_LOB_TYPE_CD/*  LOB_TYPE */
FROM    

   ( 

SELECT  DISTINCT PROD_ID,AGMT_ID 

FROM    EVIEWDB_EDW.AGMT_PROD 

WHERE   CAST(EDW_END_DTTM AS DATE)=''9999-12-31'') B 

INNER JOIN EVIEWDB_EDW.PROD C 

    ON  B.PROD_ID = C.PROD_ID 

    and C.INSRNC_LOB_TYPE_CD in (''PA'') 

    AND CAST(C.EDW_END_DTTM AS DATE)=''9999-12-31''

) LOB 

    ON  agt.AGMT_ID = LOB.AGMT_ID 



/*  Date Rewritten */


LEFT JOIN ( 

	SELECT  HOST_AGMT_NUM ,AG.MODL_EFF_DTTM(DATE) AS REWRT_DTTM

FROM EVIEWDB_EDW.AGMT AG

JOIN EVIEWDB_EDW.EV_STS ES ON ES.AGMT_ID = AG.AGMT_ID       

JOIN EVIEWDB_EDW.EV EVV ON EVV.EV_ID =ES.EV_ID  AND EV_ACTVY_TYPE_CD=''REWRT''

WHERE  to_date(AG.MODL_EFF_DTTM, ''MM/DD/YYYY'')  BETWEEN to_date(:MCAS_Start_dt, ''MM/DD/YYYY'') AND to_date(:MCAS_End_dt, ''MM/DD/YYYY'')

AND MODL_CRTN_DTTM(DATE) is not null

	) REWRT 

	on	agt.HOST_AGMT_NUM=REWRT.HOST_AGMT_NUM



/* Date Reinstated */


LEFT JOIN ( 

	SELECT  HOST_AGMT_NUM ,AG.MODL_EFF_DTTM(DATE) AS RNST_DTTM

FROM EVIEWDB_EDW.AGMT AG

JOIN EVIEWDB_EDW.EV_STS ES ON ES.AGMT_ID = AG.AGMT_ID         

JOIN EVIEWDB_EDW.EV EVV ON EVV.EV_ID =ES.EV_ID 

AND EV_ACTVY_TYPE_CD=''REINSTATE''

WHERE  to_date(AG.MODL_EFF_DTTM, ''MM/DD/YYYY'')   BETWEEN to_date(:MCAS_Start_dt, ''MM/DD/YYYY'') AND to_date(:MCAS_End_dt, ''MM/DD/YYYY'')

AND MODL_CRTN_DTTM(DATE)  is not null



) RNST 

	on	agt.HOST_AGMT_NUM=RNST.HOST_AGMT_NUM



/* Insurance Metric Type Code */


LEFT JOIN  

( SELECT AGMT_ID,INSRNC_MTRC_TYPE_CD FROM EVIEWDB_EDW.PLCY_MTRC PLCY_MTRC WHERE CAST(EDW_END_DTTM as date)=''9999-12-31'' and INSRNC_MTRC_TYPE_CD=''PREM''

) PMT

ON PMT.AGMT_ID=AGT.AGMT_ID  



/*  Non Renewal date and Non Renewal Status */


LEFT JOIN 

(

SELECT AG.HOST_AGMT_NUM as HOST_AGMT_NUM,QUOTN_STS_TYPE_CD as NON_RENEWAL_STS,IQ.QUOTN_ID,

max(IQ.QUOTN_PLND_AGMT_OPN_DTTM) as NON_RENEWAL_DTTM

FROM EVIEWDB_EDW.INSRNC_QUOTN IQ

JOIN EVIEWDB_EDW.APLCTN AP ON AP.APLCTN_ID = IQ.APLCTN_ID

JOIN EVIEWDB_EDW.AGMT AG on AP.HOST_APLCTN_NUM = AG.HOST_AGMT_NUM

WHERE  QUOTN_STS_TYPE_CD = ''NONRNWD''

AND to_date(QUOTN_PLND_AGMT_OPN_DTTM, ''MM/DD/YYYY'') BETWEEN to_date(:MCAS_Start_dt, ''MM/DD/YYYY'') AND to_date(:MCAS_End_dt, ''MM/DD/YYYY'')

group by 1,2,3

)NON_DTTM

ON NON_DTTM.HOST_AGMT_NUM=AGT.HOST_AGMT_NUM



/*  Cancellation Date and Cancellation Reason code */


LEFT JOIN

(

SELECT max(AGMT_STS_STRT_DTTM) as CAN_DTTM,  AGMT_STS_RSN_CD as CAN_RSN_CD, 

          AGMT_STS_SRC_TYPE_CD as CAN_SRC, agmt.HOST_AGMT_NUM as HOST_AGMT_NUM,AGMT.AGMT_ID 

        FROM EVIEWDB_EDW.AGMT_STS AGMT_STS

        JOIN EVIEWDB_EDW.AGMT AGMT on AGMT_STS.AGMT_ID=AGMT.AGMT_ID 

        and AGMT_STS_CD = ''CNCLD''  

        group by AGMT.AGMT_ID,CAN_RSN_CD,CAN_SRC,HOST_AGMT_NUM

)CAN_DTTM

ON CAN_DTTM.AGMT_ID=AGT.AGMT_ID





/* MOTOR VEH */


INNER JOIN

(

SELECT QA.QUOTN_ID, count(DISTINCT MOTR_VEH_SER_NUM) as MOTR_VEH_CNT 

FROM EVIEWDB_EDW.QUOTN_ASSET QA

INNER JOIN EVIEWDB_EDW.PRTY_ASSET PA ON PA.PRTY_ASSET_ID = QA.PRTY_ASSET_ID

INNER JOIN EVIEWDB_EDW.MOTR_VEH MV ON PA.PRTY_ASSET_ID=MV.PRTY_ASSET_ID

WHERE CAST(PA.EDW_END_DTTM as date)=''9999-12-31'' AND CAST(MV.EDW_END_DTTM as date)=''9999-12-31''

AND cast(QA.EDW_END_DTTM as date)=''9999-12-31''

AND MV.MOTR_VEH_TYPE_CD IN(''Special'',''PU'',''PublicTransport'',''PV'',''VN'',''MH'',''MC'',''SV'',''RK'',''MS'',''PP'',''GM'',''other'',''auto'')

AND MV.MOTR_VEH_SER_NUM is not null

GROUP BY QA.QUOTN_ID

) MVCNT

on MVCNT.QUOTN_ID = NON_DTTM.QUOTN_ID



    )OVERALL

	

	),

	

MCAS_AUTO_DETAIL_INFORCE AS 

(												

	SELECT DISTINCT COMPANY,												

	STATE,LOB_TYPE,												

	CAST(''0.00'' AS DECIMAL(15,2)) AS PREMIUM,												

	INSRNC_MTRC_TYPE_CD,												

	MODL_NUM,TERM_NUM,AGMT_ID,												

	CASE WHEN AGMT_STS_CD IN(''RNEWLLAPSD'',''PNDGCNFRMTN'',''INFORCE'') THEN ''INFORCE''       												

	WHEN AGMT_STS_CD IN(''CNCLD'',''EXPIRED'') THEN ''CNCLD''     												

	ELSE AGMT_STS_CD END AS AGMT_STS_CD,												

	AGMT_STS_SRC_TYPE_CD,												

	AGMT_CUR_STS_RSN_CD,												

	HOST_AGMT_NUM,												

	AGMT_OPN_DTTM,AGMT_EFF_DTTM,NEW_AGMT_EFF_DTTM,												

	NON_RENEWAL_DTTM,												

	NON_RENEWAL_STS,AGMT_PLND_EXPN_DTTM,AGMT_CUR_STS_CD,												

	CAN_DTTM,CAN_RSN_CD,CAN_SRC,												

	REWRT_DTTM,RNST_DTTM,												

	QUOTN_STS_TYPE_CD,												

	MODL_CRTN_DTTM,												

	MOTR_VEH_CNT,												

	CAST(''I'' as VARCHAR(10)) AS Ind												

	FROM (

	SELECT OVERALL.*,MOTR_VEH_CNT FROM (

	SELECT DISTINCT AGT.*, CMP.COMPANY,RSKSTATE.GEOGRCL_AREA_SHRT_NAME AS STATE,REWRT.REWRT_DTTM,RNST.RNST_DTTM,												

	LOB.INSRNC_LOB_TYPE_CD as LOB_TYPE,												

	PMT.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,												

	NON_RENEWAL_STS,NON_RENEWAL_DTTM,												

	CAN_RSN_CD,CAN_DTTM,CAN_SRC   												

	FROM (      												

	SELECT PPV.AGMT_ID      												

	     ,PPV.HOST_AGMT_NUM     												

	     ,PPV.AGMT_OPN_DTTM     												

	     ,PPV.AGMT_EFF_DTTM     												

	     ,PPV.AGMT_PLND_EXPN_DTTM       												

	     ,PPV.TERM_NUM,PPV.MODL_NUM     												

	     ,PPV.LGCY_PLCY_IND,PPV.AGMT_CUR_STS_CD     												

	     ,PPV.AGMT_CUR_STS_RSN_CD,PPV.NEW_AGMT_EFF_DTTM,PPV.MODL_CRTN_DTTM,PPV.MODL_EFF_DTTM        												

	     ,IQ.QUOTN_STS_TYPE_CD      												

	     ,PPV.AGMT_PRCSG_DTTM,A_S.AGMT_STS_STRT_DTTM,A_S.AGMT_STS_CD,A_S.AGMT_STS_SRC_TYPE_CD FROM (        												

	    SELECT DISTINCT     												

	      A.AGMT_ID     												

	     ,A.HOST_AGMT_NUM       												

	     ,A.AGMT_OPN_DTTM       												

	     ,A.AGMT_EFF_DTTM       												

	     ,A.AGMT_PLND_EXPN_DTTM     												

	     ,A.TERM_NUM,A.MODL_NUM     												

	     ,A.LGCY_PLCY_IND,A.AGMT_PRCSG_DTTM,A.MODL_CRTN_DTTM,A.MODL_EFF_DTTM,       												

	     A.AGMT_CUR_STS_CD,A.AGMT_CUR_STS_RSN_CD,       												

	     CASE    WHEN A.MODL_EFF_DTTM > A.MODL_CRTN_DTTM THEN A.MODL_EFF_DTTM       												

	    ELSE    A.MODL_CRTN_DTTM        												

	    END     AS NEW_AGMT_EFF_DTTM        												

	     FROM EVIEWDB_EDW.AGMT A      												

	     INNER JOIN EVIEWDB_EDW.AGMT_PROD AP ON A.AGMT_ID=AP.AGMT_ID AND AP.AGMT_PROD_ROLE_CD=''PLCYTYPE''      												

	     INNER JOIN EVIEWDB_EDW.PROD P ON AP.PROD_ID=P.PROD_ID AND P.INSRNC_LOB_TYPE_CD IN (''PA'')     												

	     WHERE A.AGMT_TYPE_CD = ''PPV''       												

	     AND A.SRC_SYS_CD = ''GWPC''      												

	     AND PROD_NAME IN (''PPV'',''PPV2'')        												

	    AND     to_date(:MCAS_End_dt, ''MM/DD/YYYY'')  BETWEEN to_date(AGMT_EFF_DTTM, ''MM/DD/YYYY'') AND to_date(AGMT_PLND_EXPN_DTTM, ''MM/DD/YYYY'')      												

	    AND     A.TRANS_STRT_DTTM =         												

	    (       												

	    SELECT  MIN(T2.TRANS_STRT_DTTM)         												

	    FROM    EVIEWDB_EDW.AGMT T2       												

	    WHERE   A.AGMT_ID = T2.AGMT_ID)         												

	    AND     to_date(NEW_AGMT_EFF_DTTM, ''MM/DD/YYYY'') BETWEEN to_date(:MCAS_Start_dt, ''MM/DD/YYYY'') AND to_date(:MCAS_End_dt, ''MM/DD/YYYY'')     												

	            												

	    ) PPV       												

	    INNER JOIN      												

	    (       												

	    SELECT  HOST_AGMT_NUM, TERM_NUM,AGMT_ID,AGMT_TYPE_CD        												

	    FROM    EVIEWDB_EDW.AGMT      												

	    WHERE   AGMT_TYPE_CD = ''POLTRM''         												

	    AND     to_date(:MCAS_End_dt, ''MM/DD/YYYY'')  BETWEEN to_date(AGMT_EFF_DTTM, ''MM/DD/YYYY'') AND to_date(AGMT_PLND_EXPN_DTTM, ''MM/DD/YYYY'')      												

	    GROUP   BY 1,2,3,4) TRM         												

	            ON      PPV.HOST_AGMT_NUM = TRM.HOST_AGMT_NUM       												

	            AND     PPV.TERM_NUM = TRM.TERM_NUM         												

	    INNER JOIN      												

	    EVIEWDB_EDW.AGMT_STS A_S      												

	            ON      TRM.AGMT_ID = A_S.AGMT_ID       												

	            AND     to_date(A_S.AGMT_STS_STRT_DTTM, ''MM/DD/YYYY'') <= to_date(:MCAS_End_dt, ''MM/DD/YYYY'')         												

	            AND     A_S.AGMT_STS_CD <> ''CNFRMDDT''       												

	            												

	    LEFT OUTER JOIN EVIEWDB_EDW.AGMT_ASSET AA ON AA.AGMT_ID = PPV.AGMT_ID  AND CAST(AA.EDW_END_DTTM AS DATE)=''9999-12-31''         												

	    LEFT  OUTER JOIN EVIEWDB_EDW.APLCTN AP ON AP.HOST_APLCTN_NUM=PPV.HOST_AGMT_NUM AND APLCTN_TYPE_CD = ''SBMSSN'' AND CAST(AP.EDW_END_DTTM AS DATE)=''9999-12-31''         												

        LEFT OUTER JOIN EVIEWDB_EDW.INSRNC_QUOTN IQ ON AP.APLCTN_ID = IQ.APLCTN_ID AND CAST(IQ.EDW_END_DTTM AS DATE)=''9999-12-31''             												        												

	  QUALIFY ROW_NUMBER() OVER(PARTITION BY PPV.AGMT_ID        												

	    ORDER   BY PPV.MODL_CRTN_DTTM DESC, PPV.MODL_EFF_DTTM DESC,A_S.AGMT_STS_STRT_DTTM DESC,A_S.EDW_END_DTTM DESC) = 1       												

	        												

	        												

	)AGT        												

	        												

/* COMPANY       												 */
	INNER JOIN  (       												

	SELECT DISTINCT         												

	   PA.AGMT_ID       												

	   ,INTRNL_ORG_NUM COMPANY      												

	   ,PRTY_AGMT_ROLE_CD       												

	   FROM EVIEWDB_EDW.PRTY_AGMT PA      												

	   LEFT JOIN EVIEWDB_EDW. INTRNL_ORG IO ON PA.PRTY_ID = IO.INTRNL_ORG_PRTY_ID     												

	   WHERE PA.PRTY_AGMT_ROLE_CD = ''CMP''       												

	   AND CAST(IO.EDW_END_DTTM AS DATE) = ''9999-12-31''     												

	   AND INTRNL_ORG_SBTYPE_CD = ''CO''      												

	   AND CAST(PA.EDW_END_DTTM AS DATE) = ''9999-12-31''     												

	) CMP ON AGT.AGMT_ID = CMP.AGMT_ID      												

	        												

/* RISK STATE        												 */
	INNER JOIN      												

	(       												

SELECT  A.AGMT_ID, C.GEOGRCL_AREA_SHRT_NAME /* AS RISKSTATE      												 */
	FROM            												

	 (      												

	SELECT DISTINCT AGMT_ID,LOC_ID, AGMT_LOCTR_ROLE_TYPE_CD     												

	FROM    EVIEWDB_EDW.AGMT_LOCTR        												

	WHERE   CAST(EDW_END_DTTM AS DATE) = ''9999-12-31'')  A       												

	INNER JOIN EVIEWDB_EDW.LOCTR B        												

	    ON  (A.LOC_ID = B.LOCTR_ID      												

	    AND B.GEOGRCL_AREA_SBTYPE_CD = ''TER'')       												

	INNER JOIN EVIEWDB_EDW.TERR C         												

	    ON  A.LOC_ID = C.TERR_ID        												

	    AND CAST(C.EDW_END_DTTM AS DATE) = ''9999-12-31''     												

	WHERE   A.AGMT_LOCTR_ROLE_TYPE_CD = ''AGTWINST''          												

	) RSKSTATE          												

	    ON  AGT.AGMT_ID = RSKSTATE.AGMT_ID      												

	            												

/* LOB       												 */
	INNER JOIN      												

	(       												

SELECT  B.AGMT_ID, decode(C.INSRNC_LOB_TYPE_CD,''PA'',''A'') as INSRNC_LOB_TYPE_CD/*  LOB_TYPE       												 */
	FROM            												

	   (        												

	SELECT  DISTINCT PROD_ID,AGMT_ID        												

	FROM    EVIEWDB_EDW.AGMT_PROD         												

	WHERE   CAST(EDW_END_DTTM AS DATE)=''9999-12-31'') B      												

	INNER JOIN EVIEWDB_EDW.PROD C         												

	    ON  B.PROD_ID = C.PROD_ID       												

	    and C.INSRNC_LOB_TYPE_CD in (''PA'')      												

	    AND CAST(C.EDW_END_DTTM AS DATE)=''9999-12-31''       												

	) LOB       												

	    ON  agt.AGMT_ID = LOB.AGMT_ID       												

	        												

/*  Date Rewritten       												 */
	        												

	LEFT JOIN (         												

	    SELECT  HOST_AGMT_NUM ,AG.MODL_EFF_DTTM(DATE) AS REWRT_DTTM        												

	FROM EVIEWDB_EDW.AGMT AG      												

	JOIN EVIEWDB_EDW.EV_STS ES ON ES.AGMT_ID = AG.AGMT_ID             												

	JOIN EVIEWDB_EDW.EV EVV ON EVV.EV_ID =ES.EV_ID  AND EV_ACTVY_TYPE_CD=''REWRT''      												

	WHERE  to_date(AG.MODL_EFF_DTTM, ''MM/DD/YYYY'')  BETWEEN to_date(:MCAS_Start_dt, ''MM/DD/YYYY'') AND to_date(:MCAS_End_dt, ''MM/DD/YYYY'')      												

	AND MODL_CRTN_DTTM(DATE) is not null  

	QUALIFY ROW_NUMBER() OVER(PARTITION BY AG.HOST_AGMT_NUM ORDER   BY  MODL_CRTN_DTTM DESC,MODL_EFF_DTTM DESC ) = 1

	    ) REWRT         												

	    on  agt.HOST_AGMT_NUM=REWRT.HOST_AGMT_NUM       												

	        												

/* Date Reinstated       												 */
	        												

	LEFT JOIN (         												

	    SELECT  HOST_AGMT_NUM ,AG.MODL_EFF_DTTM(DATE) AS RNST_DTTM     												

	FROM EVIEWDB_EDW.AGMT AG      												

	JOIN EVIEWDB_EDW.EV_STS ES ON ES.AGMT_ID = AG.AGMT_ID                 												

	JOIN EVIEWDB_EDW.EV EVV ON EVV.EV_ID =ES.EV_ID        												

	AND EV_ACTVY_TYPE_CD=''REINSTATE''        												

	WHERE  to_date(AG.MODL_EFF_DTTM, ''MM/DD/YYYY'')  BETWEEN to_date(:MCAS_Start_dt, ''MM/DD/YYYY'') AND to_date(:MCAS_End_dt, ''MM/DD/YYYY'')      												

	AND MODL_CRTN_DTTM(DATE)  is not null   

    QUALIFY ROW_NUMBER() OVER(PARTITION BY AG.HOST_AGMT_NUM ORDER   BY  MODL_CRTN_DTTM DESC,MODL_EFF_DTTM DESC ) = 1

	        												

	) RNST      												

	    on  agt.HOST_AGMT_NUM=RNST.HOST_AGMT_NUM        																								

													

/* Insurance Metric Type Code												 */
													

	LEFT JOIN  												

	( SELECT AGMT_ID,INSRNC_MTRC_TYPE_CD FROM EVIEWDB_EDW.PLCY_MTRC PLCY_MTRC WHERE CAST(EDW_END_DTTM as date)=''9999-12-31'' and INSRNC_MTRC_TYPE_CD=''PREM''												

	) PMT												

	ON PMT.AGMT_ID=AGT.AGMT_ID      												

/*  Non Renewal date and Non Renewal Status      												 */
	        												

	LEFT JOIN       												

	(       												

	SELECT AG.HOST_AGMT_NUM as HOST_AGMT_NUM,QUOTN_STS_TYPE_CD as NON_RENEWAL_STS,      												

	max(IQ.QUOTN_PLND_AGMT_OPN_DTTM) as NON_RENEWAL_DTTM        												

	FROM EVIEWDB_EDW.INSRNC_QUOTN IQ      												

	JOIN EVIEWDB_EDW.APLCTN AP ON AP.APLCTN_ID = IQ.APLCTN_ID     												

	JOIN EVIEWDB_EDW.AGMT AG on AP.HOST_APLCTN_NUM = AG.HOST_AGMT_NUM     												

	WHERE  QUOTN_STS_TYPE_CD = ''NONRNWD''        												

	AND to_date(QUOTN_PLND_AGMT_OPN_DTTM, ''MM/DD/YYYY'') BETWEEN to_date(:MCAS_Start_dt, ''MM/DD/YYYY'') AND to_date(:MCAS_End_dt, ''MM/DD/YYYY'')     												

	group by 1,2        												

	)NON_DTTM       												

	ON NON_DTTM.HOST_AGMT_NUM=AGT.HOST_AGMT_NUM     												

	        												

/*  Cancellation Date and Cancellation Reason code       												 */
	        												

	LEFT JOIN       												

	(       												

	SELECT AGMT_ID,AGMT_STS_RSN_CD as CAN_RSN_CD,AGMT_STS_SRC_TYPE_CD as CAN_SRC,AGMT_STS_STRT_DTTM as CAN_DTTM        												

	FROM EVIEWDB_EDW.AGMT_STS         												

	WHERE       												

	to_date(AGMT_STS_STRT_DTTM, ''MM/DD/YYYY'') BETWEEN to_date(:MCAS_Start_dt, ''MM/DD/YYYY'') AND to_date(:MCAS_End_dt, ''MM/DD/YYYY'')       												

	AND AGMT_STS_CD=''CNCLD''     												

	Qualify ROW_NUMBER() OVER(PARTITION BY AGMT_ID ORDER   BY  AGMT_STS_STRT_DTTM,EDW_END_DTTM  DESC ) = 1      												

	)CAN_DTTM       												

	ON CAN_DTTM.AGMT_ID=AGT.AGMT_ID     												

    QUALIFY ROW_NUMBER() OVER(PARTITION BY AGT.HOST_AGMT_NUM        												

	ORDER   BY  MODL_CRTN_DTTM DESC ) = 1          												

	)OVERALL	

	

/* MOTOR VEH     												 */
	        												

	INNER JOIN      												

	(       												

	SELECT AA.AGMT_ID, count(DISTINCT MOTR_VEH_SER_NUM) as MOTR_VEH_CNT         												

	FROM EVIEWDB_EDW.AGMT_ASSET AA        												

	INNER JOIN EVIEWDB_EDW.PRTY_ASSET PA ON PA.PRTY_ASSET_ID = AA.PRTY_ASSET_ID       												

	INNER JOIN EVIEWDB_EDW.MOTR_VEH MV ON PA.PRTY_ASSET_ID=MV.PRTY_ASSET_ID       												

	WHERE CAST(PA.EDW_END_DTTM AS DATE)=''9999-12-31'' AND CAST(MV.EDW_END_DTTM as date)=''9999-12-31''     												

	AND cast(AA.EDW_END_DTTM AS DATE)=''9999-12-31''      												

	AND MV.MOTR_VEH_TYPE_CD IN(''Special'',''PU'',''PublicTransport'',''PV'',''VN'',''MH'',''MC'',''SV'',''RK'',''MS'',''PP'',''GM'',''other'',''auto'')        												

	AND MV.MOTR_VEH_SER_NUM is not null     												

	AND to_date(AGMT_ASSET_END_DTTM, ''MM/DD/YYYY'')>= to_date(:MCAS_End_dt, ''MM/DD/YYYY'')     												

	GROUP BY AA.AGMT_ID     												

	) MVCNT     												

on MVCNT.AGMT_ID = OVERALL.AGMT_ID  /* EIM-50555   												 */
	)MC        																					

	),

	

MCAS_AUTO_DETAIL_DWP AS 

(

SELECT DISTINCT COMPANY,												

	STATE,CAST(NULL AS VARCHAR(50)) AS LOB_TYPE,												

	PREMIUM,												

	CAST(NULL AS VARCHAR(50)) AS INSRNC_MTRC_TYPE_CD,												

	CAST(NULL AS INTEGER) AS MODL_NUM,

	CAST(NULL AS INTEGER) AS TERM_NUM,

    CAST(NULL AS INTEGER) AS AGMT_ID,												

	CAST(NULL AS VARCHAR(50)) AS AGMT_STS_CD,												

	CAST(NULL AS VARCHAR(50)) AS AGMT_STS_SRC_TYPE_CD,												

	CAST(NULL AS VARCHAR(50)) AS AGMT_CUR_STS_RSN_CD,												

	PLCY_NUM,												

	CAST(''1900-01-01 00:01:00.000000'' as timestamp(6)) as AGMT_OPN_DTTM,

	CAST(''1900-01-01 00:01:00.000000'' as timestamp(6)) as AGMT_EFF_DTTM,

	CAST(''1900-01-01 00:01:00.000000'' as timestamp(6)) as NEW_AGMT_EFF_DTTM,												

	CAST(''1900-01-01 00:01:00.000000'' as timestamp(6)) as NON_RENEWAL_DTTM,												

	CAST(NULL as VARCHAR(50)) as NON_RENEWAL_STS,

	CAST(''1900-01-01 00:01:00.000000'' as timestamp(6)) as AGMT_PLND_EXPN_DTTM,

	CAST(NULL as varchar(50))as AGMT_CUR_STS_CD,												

	CAST(''1900-01-01 00:01:00.000000'' as timestamp(6))as CAN_DTTM,

	CAST(NULL as varchar(50)) as CAN_RSN_CD,CAST(NULL as varchar(50)) as CAN_SRC,												

	CAST(''1900-01-01 00:01:00.000000'' as timestamp(6))as REWRT_DTTM,

	CAST(''1900-01-01 00:01:00.000000'' as timestamp(6))as RNST_DTTM,												

	CAST(NULL as varchar(50)) as QUOTN_STS_TYPE_CD,												

	CAST(''1900-01-01 00:01:00.000000'' as timestamp(6))as MODL_CRTN_DTTM,												

	CAST(''0'' as INTEGER) as MOTR_VEH_CNT,												

	CAST(''DWP'' AS VARCHAR(10)) AS IND												

	FROM (													

SELECT COALESCE(RISK_STATE_CD,RSKSTATE.GEOGRCL_AREA_SHRT_NAME) AS STATE,

COALESCE(CASE WHEN MSTR_CO_NUM=''10'' THEN ''AMI''

WHEN MSTR_CO_NUM=''40'' THEN ''AIC''

WHEN MSTR_CO_NUM=''50'' THEN ''AGI''

WHEN MSTR_CO_NUM=''70'' THEN ''AMG'' 

WHEN MSTR_CO_NUM=''60'' THEN ''AMF'' 

END,COMPANY) AS COMPANY,

PLCY_NUM,

SUM(GW_PREM_TRANS_GL_INFO.TRANS_PREM_AMT) AS PREMIUM

FROM ECOMNDB_EDW.GW_PREM_TRANS_GL_INFO GW_PREM_TRANS_GL_INFO                                                

INNER JOIN (SELECT DISTINCT PLCY_INFO_ID,PLCY_NUM, PLCY_TERM_NUM,PLCY_MODL_NUM FROM ECOMNDB_EDW.GW_PREM_TRANS_PLCY_INFO) GW_PREM_TRANS_PLCY_INFO                                              

ON GW_PREM_TRANS_PLCY_INFO.PLCY_INFO_ID=GW_PREM_TRANS_GL_INFO.PLCY_INFO_ID

INNER JOIN EVIEWDB_EDW.AGMT A on GW_PREM_TRANS_PLCY_INFO.PLCY_NUM=A.HOST_AGMT_NUM 

AND A.TERM_NUM=CAST(GW_PREM_TRANS_PLCY_INFO.PLCY_TERM_NUM AS INTEGER)

AND A.MODL_NUM=CAST(GW_PREM_TRANS_PLCY_INFO.PLCY_MODL_NUM AS INTEGER)

AND A.AGMT_TYPE_CD = ''PPV''                                                      

AND A.SRC_SYS_CD = ''GWPC''and A.EDW_END_DTTM =''9999-12-31 23:59:59.999999''

INNER JOIN EVIEWDB_EDW.AGMT_PROD AP ON A.AGMT_ID=AP.AGMT_ID AND AP.AGMT_PROD_ROLE_CD=''PLCYTYPE''                                                    

INNER JOIN EVIEWDB_EDW.PROD P ON AP.PROD_ID=P.PROD_ID AND P.INSRNC_LOB_TYPE_CD IN (''PA'') AND PROD_NAME IN (''PPV'',''PPV2'') 

/* COMPANY                                                        */
LEFT JOIN  (                                                    

SELECT DISTINCT                                                         

   PA.AGMT_ID                                                       

   ,INTRNL_ORG_NUM COMPANY                                                      

   ,PRTY_AGMT_ROLE_CD                                                       

   FROM EVIEWDB_EDW.PRTY_AGMT PA                                                      

   LEFT JOIN EVIEWDB_EDW. INTRNL_ORG IO ON PA.PRTY_ID = IO.INTRNL_ORG_PRTY_ID                                                     

   WHERE PA.PRTY_AGMT_ROLE_CD = ''CMP''                                                       

   AND CAST(IO.EDW_END_DTTM AS DATE) = ''9999-12-31''                                                     

   AND INTRNL_ORG_SBTYPE_CD = ''CO''                                                      

   AND CAST(PA.EDW_END_DTTM AS DATE) = ''9999-12-31''                                                     

) CMP ON A.AGMT_ID = CMP.AGMT_ID                                                    

                                                        

/* RISK STATE                                                         */
LEFT JOIN                                                   

(                                                       

SELECT  A.AGMT_ID, C.GEOGRCL_AREA_SHRT_NAME /* AS RISKSTATE                                                       */
FROM                                                            

 (                                                      

SELECT DISTINCT AGMT_ID,LOC_ID, AGMT_LOCTR_ROLE_TYPE_CD                                                     

FROM    EVIEWDB_EDW.AGMT_LOCTR                                                        

WHERE   CAST(EDW_END_DTTM AS DATE) = ''9999-12-31''

)  A                                                    

INNER JOIN EVIEWDB_EDW.LOCTR B                                                        

    ON  (A.LOC_ID = B.LOCTR_ID                                                      

    AND B.GEOGRCL_AREA_SBTYPE_CD = ''TER'')                                                       

INNER JOIN EVIEWDB_EDW.TERR C                                                         

    ON  A.LOC_ID = C.TERR_ID                                                        

    AND CAST(C.EDW_END_DTTM AS DATE) = ''9999-12-31''                                                     

WHERE   A.AGMT_LOCTR_ROLE_TYPE_CD = ''AGTWINST''                                                          

) RSKSTATE                                                          

    ON  A.AGMT_ID = RSKSTATE.AGMT_ID      

WHERE    

GW_PREM_TRANS_GL_INFO.TRANS_PREM_AMT <> 0.00                                                

and GW_PREM_TRANS_GL_INFO.BUS_ACT_TRNS_ACT in (''WRT'', ''NEF'') 

and GL_EXTC_YR between substr (:MCAS_Start_dt,7,4) and substr(:MCAS_End_dt,7,4)                                               

and GL_EXTC_MTH between substr(:MCAS_Start_dt,1,2) and substr(:MCAS_End_dt,1,2)

AND PLCY_NUM IS NOT NULL 

GROUP BY 1,2,3

)OVERALL

)



SELECT * FROM MCAS_AUTO_DETAIL_NONRENEWAL 

UNION

SELECT * FROM MCAS_AUTO_DETAIL_CANCEL

UNION

SELECT * FROM MCAS_AUTO_DETAIL_INFORCE

UNION

SELECT * FROM MCAS_AUTO_DETAIL_DWP
) SRC
)
);


-- Component Exp_Src, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE Exp_Src AS
(
SELECT
SQ_AGMT.UNDRWRTG_CMPY as UNDRWRTG_CMPY,
SQ_AGMT.RISK_STATE as RISK_STATE,
SQ_AGMT.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD,
SQ_AGMT.TRANS_PREM_AMT as TRANS_PREM_AMT,
SQ_AGMT.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
SQ_AGMT.MODL_NUM as MODL_NUM,
SQ_AGMT.TERM_NUM as TERM_NUM,
SQ_AGMT.AGMT_ID as AGMT_ID,
SQ_AGMT.AGMT_STS_CD as AGMT_STS_CD,
SQ_AGMT.AGMT_STS_SRC_TYPE_CD as AGMT_STS_SRC_TYPE_CD,
SQ_AGMT.AGMT_CUR_STS_RSN_CD as AGMT_CUR_STS_RSN_CD,
SQ_AGMT.HOST_AGMT_NUM as HOST_AGMT_NUM,
SQ_AGMT.AGMT_OPN_DTTM as AGMT_OPN_DTTM,
SQ_AGMT.AGMT_EFF_DTTM as AGMT_EFF_DTTM,
SQ_AGMT.NEW_AGMT_EFF_DTTM as NEW_AGMT_EFF_DTTM,
SQ_AGMT.NON_RENEWAL_DTTM as NON_RENEWAL_DTTM,
SQ_AGMT.NON_RENEWAL_STS as NON_RENEWAL_STS,
SQ_AGMT.AGMT_PLND_EXPN_DTTM as AGMT_PLND_EXPN_DTTM,
SQ_AGMT.AGMT_CUR_STS_CD as AGMT_CUR_STS_CD,
SQ_AGMT.CAN_DTTM as CAN_DTTM,
SQ_AGMT.CAN_RSN_CD as CAN_RSN_CD,
SQ_AGMT.CAN_SRC as CAN_SRC,
SQ_AGMT.REWRT_DTTM as REWRT_DTTM,
SQ_AGMT.RNST_DTTM as RNST_DTTM,
SQ_AGMT.QUOTN_STS_TYPE_CD as QUOTN_STS_TYPE_CD,
SQ_AGMT.MODL_CRTN_DTTM as MODL_CRTN_DTTM,
SQ_AGMT.MOTR_VEH_CNT as MOTR_VEH_CNT,
SQ_AGMT.IND as IND,
SQ_AGMT.source_record_id
FROM
SQ_AGMT
);


-- Component MCAS_AUTO_DETAIL, Type TARGET 
INSERT INTO MCAS_AUTO_DETAIL
(
UNDRWRTG_CMPY,
RISK_STATE,
INSRNC_LOB_TYPE_CD,
TRANS_PREM_AMT,
INSRNC_MTRC_TYPE_CD,
MODL_NUM,
TERM_NUM,
AGMT_ID,
AGMT_STS_CD,
AGMT_STS_SRC_TYPE_CD,
AGMT_CUR_STS_RSN_CD,
HOST_AGMT_NUM,
AGMT_OPN_DTTM,
AGMT_EFF_DTTM,
NEW_AGMT_EFF_DTTM,
NON_RENEWAL_DTTM,
NON_RENEWAL_STS,
AGMT_PLND_EXPN_DTTM,
AGMT_CUR_STS_CD,
CAN_DTTM,
CAN_RSN_CD,
CAN_SRC,
REWRT_DTTM,
RNST_DTTM,
QUOTN_STS_TYPE_CD,
MODL_CRTN_DTTM,
MOTR_VEH_CNT,
IND
)
SELECT
Exp_Src.UNDRWRTG_CMPY as UNDRWRTG_CMPY,
Exp_Src.RISK_STATE as RISK_STATE,
Exp_Src.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD,
Exp_Src.TRANS_PREM_AMT as TRANS_PREM_AMT,
Exp_Src.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
Exp_Src.MODL_NUM as MODL_NUM,
Exp_Src.TERM_NUM as TERM_NUM,
Exp_Src.AGMT_ID as AGMT_ID,
Exp_Src.AGMT_STS_CD as AGMT_STS_CD,
Exp_Src.AGMT_STS_SRC_TYPE_CD as AGMT_STS_SRC_TYPE_CD,
Exp_Src.AGMT_CUR_STS_RSN_CD as AGMT_CUR_STS_RSN_CD,
Exp_Src.HOST_AGMT_NUM as HOST_AGMT_NUM,
Exp_Src.AGMT_OPN_DTTM as AGMT_OPN_DTTM,
Exp_Src.AGMT_EFF_DTTM as AGMT_EFF_DTTM,
Exp_Src.NEW_AGMT_EFF_DTTM as NEW_AGMT_EFF_DTTM,
Exp_Src.NON_RENEWAL_DTTM as NON_RENEWAL_DTTM,
Exp_Src.NON_RENEWAL_STS as NON_RENEWAL_STS,
Exp_Src.AGMT_PLND_EXPN_DTTM as AGMT_PLND_EXPN_DTTM,
Exp_Src.AGMT_CUR_STS_CD as AGMT_CUR_STS_CD,
Exp_Src.CAN_DTTM as CAN_DTTM,
Exp_Src.CAN_RSN_CD as CAN_RSN_CD,
Exp_Src.CAN_SRC as CAN_SRC,
Exp_Src.REWRT_DTTM as REWRT_DTTM,
Exp_Src.RNST_DTTM as RNST_DTTM,
Exp_Src.QUOTN_STS_TYPE_CD as QUOTN_STS_TYPE_CD,
Exp_Src.MODL_CRTN_DTTM as MODL_CRTN_DTTM,
Exp_Src.MOTR_VEH_CNT as MOTR_VEH_CNT,
Exp_Src.IND as IND
FROM
Exp_Src;


END; ';