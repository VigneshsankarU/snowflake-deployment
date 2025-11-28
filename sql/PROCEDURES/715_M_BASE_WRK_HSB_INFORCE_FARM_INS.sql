-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_WRK_HSB_INFORCE_FARM_INS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  run_id STRING;
  prcs_id int;
  CAL_END_DT DATE;


BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
CAL_END_DT :=   (SELECT left(param_value,10) FROM control_params where run_id = :run_id and upper(param_name)=''CAL_END_DT'' order by insert_ts desc limit 1);

  -- Component LKP_TERADATA_ETL_REF_XLAT_FOP, Type Prerequisite Lookup Object
  CREATE
  OR
  REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_FOP AS
  (
         SELECT TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL ,
                TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL
         FROM   db_t_prod_core.TERADATA_ETL_REF_XLAT
         WHERE  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''NOT_APPLICABLE''
         AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived''
         AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS''
         AND    TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' );
  -- PIPELINE START FOR 1
  -- Component SQ_HSB_INFORCE_FARM_POLICY_RECORD, Type SOURCE
  CREATE
  OR
  REPLACE TEMPORARY TABLE SQ_HSB_INFORCE_FARM_POLICY_RECORD AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS Product_Code,
                $2  AS Record_Type,
                $3  AS Contract_Number,
                $4  AS Policy_Number,
                $5  AS Previous_Policy_Number,
                $6  AS Policy_Effective_Date,
                $7  AS policy_Expiration_Date,
                $8  AS HSP_Coverage_Effective_Date,
                $9  AS Name_of_Insured,
                $10 AS Mailing_Address,
                $11 AS Mailing_Address_2,
                $12 AS Mailing_City,
                $13 AS Mailing_State,
                $14 AS Mailing_Zip_Code,
                $15 AS HSB_Coverage_Gross_Premium,
                $16 AS HS_Coverage_Net_Premium,
                $17 AS Occupancy_Code,
                $18 AS Occupancy_Description,
                $19 AS EB_Form_Number,
                $20 AS HSB_Coverage_Version,
                $21 AS HSP_Product_Form_Number,
                $22 AS SLC_Product_Form_Number,
                $23 AS Farm_Cyber_Product_Form_Number,
                $24 AS Client_Product_Name,
                $25 AS Farm_EB_Limit,
                $26 AS Farm_EB_Deductible_Amount,
                $27 AS HSP_Limit,
                $28 AS HSP_Deductible_Amount,
                $29 AS SLC_Limit,
                $30 AS SLC_Deductible_Amount,
                $31 AS Farm_Cyber_Limit,
                $32 AS Farm_Cyber_Deductible_Amount,
                $33 AS Branch_Code,
                $34 AS Agency_Code,
                $35 AS Pricing_Tier,
                $36 AS Insurance_Score,
                $37 AS Rating_Territory_Code,
                $38 AS Protection_Class_Code,
                $39 AS source_record_id
         FROM   (
                         SELECT   SRC.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  /* -------FARM_POLICY_RECORD_QUERY */
                                                  SELECT DISTINCT ''HSB_Company_Product_Id_FOP''                   AS Product_Code ,
                                                                  ''1''                                            AS Record_Type ,
                                                                  ''HSB_Contract_Nbr_FOP''                         AS Contract_Number ,
                                                                  A.POLICY_NUMBER                                AS Policy_Number ,
                                                                  ''Blanks''                                       AS Previous_Policy_Number ,
                                                                  A.POLICY_EFFECTIVE_DATE                        AS Policy_Effective_Date ,
                                                                  A.POLICY_EXP_DATE                              AS Policy_Expiration_Date ,
                                                                  CAST(BRKDWN.AGMT_FEAT_STRT_DTTM AS DATE)       AS Coverage_Effective_Date ,
                                                                  A.NAME_OF_INSURED                              AS Name_of_Insured ,
                                                                  ADDR.STREET_NAME                               AS Mailing_Address ,
                                                                  ''Blanks''                                       AS Mailing_Address_2 ,
                                                                  ADDR.MAILING_CITY                              AS Mailing_City ,
                                                                  ADDR.MAILING_STATE                             AS Mailing_State ,
                                                                  ADDR.MAILING_ZIP_CODE                          AS Mailing_Zip_Code ,
                                                                  CAST(A.EB_GROSS_PREMIUM AS DECIMAL(6,2))       AS HSP_Gross_Premium_Amount ,
                                                                  CAST(A.EB_GROSS_PREMIUM AS DECIMAL(6,2))       AS HSP_Net_Premium_Amount ,
                                                                  ''020''                                          AS Occupancy_Code ,
                                                                  COALESCE(ASP.SPEC_TYPE_CD, '' '')                AS Occupancy_Description ,
                                                                  ''FO81''                                         AS EB_Form_Number ,
                                                                  ''2.0''                                          AS HSB_Coverage_Version ,
                                                                  A.PROD_NAME                                    AS HSP_Product_Form_Number ,
                                                                  ''Blanks''                                       AS SLC_Product_Form_Number ,
                                                                  ''Blanks''                                       AS Farm_Cyber_Product_Form_Number ,
                                                                  ''Blanks''                                       AS Client_Product_Name ,
                                                                  CAST(BRKDWN_EB_LIMIT.AGMT_FEAT_AMT AS INTEGER) AS Farm_EB_Limit ,
                                                                  ''DEDUCT''                                       AS Farm_EB_Deductible_Amount ,
                                                                  ''Blanks''                                       AS HSP_Limit ,
                                                                  ''Blanks''                                       AS HSP_Deductible_Amount ,
                                                                  ''Blanks''                                       AS SLC_Limit ,
                                                                  ''Blanks''                                       AS SLC_Deductible_Amount ,
                                                                  ''Blanks''                                       AS Farm_Cyber_Limit ,
                                                                  ''Blanks''                                       AS Farm_Cyber_Deductible_Amount ,
                                                                  ''Blanks''                                       AS Branch_Code ,
                                                                  ''Blanks''                                       AS Agency_Code ,
                                                                  ''Blanks''                                       AS Pricing_Tier ,
                                                                  ''Blanks''                                       AS Insurance_Score ,
                                                                  ''Blanks''                                       AS Rating_Territory_Code ,
                                                                  ''Blanks''                                       AS Protection_Class_Code
                                                  FROM            (
                                                                                  SELECT DISTINCT PPV.HOST_AGMT_NUM                                       AS POLICY_NUMBER ,
                                                                                                  COALESCE(INAME.INDIV_FULL_NAME,ORG_NAME) AS NAME_OF_INSURED ,
                                                                                                  PPV.AGMT_ID,
                                                                                                  PPV.PROD_NAME ,
                                                                                                  COALESCE(PRM.PLCY_CVGE_AMT,0.00) AS EB_GROSS_PREMIUM ,
                                                                                                  CAST(PPV.AGMT_EFF_DTTM AS       DATE ) AS POLICY_EFFECTIVE_DATE ,
                                                                                                  CAST(PPV.AGMT_PLND_EXPN_DTTM AS DATE ) AS POLICY_EXP_DATE ,
                                                                                                  CAST(PPV.MODL_CRTN_DTTM AS      DATE)  AS CRTN_DTTM ,
                                                                                                  CAST(ES.EV_STS_STRT_DTTM AS     DATE)     EV_STS_STRT_DTTM
                                                                                  FROM            (
                                                                                                         SELECT PPV_AG.AGMT_ID,
                                                                                                                PPV_AG.HOST_AGMT_NUM,
                                                                                                                AGMT_PLND_EXPN_DTTM,
                                                                                                                MODL_CRTN_DTTM,
                                                                                                                AGMT_EFF_DTTM,
                                                                                                                PPV_AG.PROD_NAME
                                                                                                         FROM   (
                                                                                                                           SELECT     A_S.AGMT_STS_CD        AGMT_STS_CD ,
                                                                                                                                      PPV_INNR.AGMT_ID       AGMT_ID,
                                                                                                                                      PPV_INNR.HOST_AGMT_NUM HOST_AGMT_NUM,
                                                                                                                                      PPV_INNR.AGMT_PLND_EXPN_DTTM,
                                                                                                                                      PPV_INNR.AGMT_EFF_DTTM,
                                                                                                                                      PPV_INNR.MODL_CRTN_DTTM,
                                                                                                                                      P.PROD_NAME
                                                                                                                           FROM       (
                                                                                                                                               SELECT   T1.HOST_AGMT_NUM,
                                                                                                                                                        T1.TERM_NUM,
                                                                                                                                                        T1.AGMT_ID,
                                                                                                                                                        T1.MODL_CRTN_DTTM,
                                                                                                                                                        T1.MODL_EFF_DTTM,
                                                                                                                                                        T1.AGMT_EFF_DTTM,
                                                                                                                                                        AGMT_PLND_EXPN_DTTM,
                                                                                                                                                        CASE
                                                                                                                                                                 WHEN T1.MODL_EFF_DTTM > T1.MODL_CRTN_DTTM THEN T1.MODL_EFF_DTTM
                                                                                                                                                                 ELSE T1.MODL_CRTN_DTTM
                                                                                                                                                        END                 AS NEW_AGMT_EFF_DTTM
                                                                                                                                               FROM     DB_T_PROD_CORE.AGMT AS T1
                                                                                                                                               WHERE    T1.AGMT_TYPE_CD = ''PPV''
                                                                                                                                               AND      T1.SRC_SYS_CD = ''GWPC''
                                                                                                                                               AND      CAST (CAST (:CAL_END_DT AS DATE)+1 AS TIMESTAMP(6)) - INTERVAL ''0.000001 SECOND'' BETWEEN AGMT_EFF_DTTM AND      AGMT_PLND_EXPN_DTTM
                                                                                                                                               AND      T1.TRANS_STRT_DTTM =
                                                                                                                                                        (
                                                                                                                                                               SELECT MIN(T2.TRANS_STRT_DTTM)
                                                                                                                                                               FROM   DB_T_PROD_CORE.AGMT T2
                                                                                                                                                               WHERE  T1.AGMT_ID = T2.AGMT_ID)
                                                                                                                                               AND      NEW_AGMT_EFF_DTTM <= CAST(CAST(:CAL_END_DT AS DATE)+1 AS TIMESTAMP(6)) - INTERVAL ''0.000001 SECOND'' QUALIFY ROW_NUMBER () OVER (PARTITION BY T1.HOST_AGMT_NUM ORDER BY T1.MODL_CRTN_DTTM DESC) = 1) PPV_INNR
                                                                                                                           INNER JOIN
                                                                                                                                      (
                                                                                                                                               SELECT   HOST_AGMT_NUM,
                                                                                                                                                        TERM_NUM,
                                                                                                                                                        AGMT_ID
                                                                                                                                               FROM     DB_T_PROD_CORE.AGMT
                                                                                                                                               WHERE    AGMT_TYPE_CD = ''POLTRM''
                                                                                                                                               AND      CAST(CAST(:CAL_END_DT AS DATE)+1 AS TIMESTAMP(6)) - INTERVAL ''0.000001 SECOND'' BETWEEN AGMT_EFF_DTTM AND      AGMT_PLND_EXPN_DTTM
                                                                                                                                               GROUP BY 1,
                                                                                                                                                        2,
                                                                                                                                                        3) TRM
                                                                                                                           ON         PPV_INNR.HOST_AGMT_NUM = TRM.HOST_AGMT_NUM
                                                                                                                           AND        PPV_INNR.TERM_NUM = TRM.TERM_NUM
                                                                                                                           INNER JOIN DB_T_PROD_CORE.AGMT_STS A_S
                                                                                                                           ON         TRM.AGMT_ID = A_S.AGMT_ID
                                                                                                                           AND        A_S.AGMT_STS_STRT_DTTM <= CAST(CAST(:CAL_END_DT AS DATE)+1 AS TIMESTAMP(6)) - INTERVAL ''0.000001 SECOND''
                                                                                                                           AND        A_S.AGMT_STS_CD <> ''CNFRMDDT''
                                                                                                                           INNER JOIN DB_T_PROD_CORE.AGMT_PROD AP
                                                                                                                           ON         PPV_INNR.AGMT_ID=AP.AGMT_ID
                                                                                                                           JOIN       DB_T_PROD_CORE.PROD P
                                                                                                                           ON         AP.PROD_ID=P.PROD_ID
                                                                                                                                      /*   AND       INSRNC_TYPE_CD=''COMRCL'' */
                                                                                                                                      QUALIFY ROW_NUMBER() OVER(PARTITION BY PPV_INNR.AGMT_ID ORDER BY A_S.AGMT_STS_STRT_DTTM DESC) = 1
                                                                                                                           AND        A_S.AGMT_STS_CD IN( ''INFORCE'',
                                                                                                                                                         ''RNEWLLAPSD'',
                                                                                                                                                         ''PNDGCNFRMTN'') )PPV_AG )PPV
                                                                                  LEFT OUTER JOIN
                                                                                                  (
                                                                                                                  SELECT DISTINCT PRTY_ID,
                                                                                                                                  AGMT_ID
                                                                                                                  FROM            DB_T_PROD_CORE.PRTY_AGMT PA
                                                                                                                  WHERE           PA.PRTY_AGMT_ROLE_CD IN (''PLCYPRININS'')
                                                                                                                  AND             CAST(PA.EDW_END_DTTM AS DATE) = ''9999-12-31''
                                                                                                                  AND             CAST(:CAL_END_DT AS   DATE) BETWEEN CAST(PA.TRANS_STRT_DTTM AS DATE) AND             CAST(PA.TRANS_END_DTTM AS DATE)) PA
                                                                                  ON              PPV.AGMT_ID = PA.AGMT_ID
                                                                                  LEFT OUTER JOIN
                                                                                                  (
                                                                                                                  SELECT DISTINCT INAME.INDIV_PRTY_ID,
                                                                                                                                  INAME.INDIV_FULL_NAME
                                                                                                                  FROM            DB_T_PROD_CORE.INDIV_NAME INAME
                                                                                                                  WHERE           CAST(INAME.EDW_END_DTTM AS DATE) = ''9999-12-31'' ) INAME
                                                                                  ON              PA.PRTY_ID = INAME.INDIV_PRTY_ID
                                                                                  LEFT OUTER JOIN
                                                                                                  (
                                                                                                                  SELECT DISTINCT O.PRTY_ID,
                                                                                                                                  ORG_NAME
                                                                                                                  FROM            DB_T_PROD_CORE.ORG_NAME O
                                                                                                                  WHERE           CAST(O.EDW_END_DTTM AS DATE) = ''9999-12-31'') O
                                                                                  ON              PA.PRTY_ID=O.PRTY_ID
                                                                                  LEFT OUTER JOIN
                                                                                                  (
                                                                                                                  SELECT DISTINCT ES.AGMT_ID,
                                                                                                                                  ES.EV_STS_STRT_DTTM
                                                                                                                  FROM            DB_T_PROD_CORE.EV_STS ES
                                                                                                                  WHERE           ES.EV_STS_TYPE_CD = ''BOUND''
                                                                                                                  AND             CAST(ES.EDW_END_DTTM AS DATE)=''9999-12-31'') ES
                                                                                  ON              PPV.AGMT_ID = ES.AGMT_ID
                                                                                  LEFT OUTER JOIN
                                                                                                  (
                                                                                                         SELECT PA.AGMT_ID,
                                                                                                                INTRNL_ORG_NUM ,
                                                                                                                PRTY_AGMT_ROLE_CD
                                                                                                         FROM   DB_T_PROD_CORE.PRTY_AGMT PA
                                                                                                         JOIN   DB_T_PROD_CORE.INTRNL_ORG IO
                                                                                                         ON     PA.PRTY_ID = IO.INTRNL_ORG_PRTY_ID
                                                                                                         AND    CAST(IO.EDW_END_DTTM AS  DATE) = ''9999-12-31''
                                                                                                         WHERE  CAST (PA.EDW_END_DTTM AS DATE) = ''9999-12-31'' ) COMPANY
                                                                                  ON              PPV.AGMT_ID = COMPANY.AGMT_ID
                                                                                  INNER JOIN
                                                                                                  (
                                                                                                             SELECT     AGMT.HOST_AGMT_NUM,
                                                                                                                        F.FEAT_NAME,
                                                                                                                        SUM(PREM.PLCY_CVGE_AMT) PLCY_CVGE_AMT
                                                                                                             FROM       DB_T_PROD_CORE.PLCY_CVGE_MTRC PREM
                                                                                                             INNER JOIN DB_T_PROD_CORE.AGMT AGMT
                                                                                                             ON         PREM.AGMT_ID=AGMT.AGMT_ID
                                                                                                             JOIN       DB_T_PROD_CORE.FEAT F
                                                                                                             ON         F.FEAT_ID = PREM.FEAT_ID
                                                                                                             WHERE      F.NK_SRC_KEY = ''FOPFarmEquipBrkdwn''
                                                                                                             AND        INSRNC_MTRC_TYPE_CD = ''TRANPREM''
                                                                                                             AND        CAST(:CAL_END_DT AS     DATE) BETWEEN CAST(PREM.PLCY_CVGE_MTRC_STRT_DTTM AS DATE) AND        CAST(PREM.PLCY_CVGE_MTRC_END_DTTM AS DATE)
                                                                                                             AND        CAST(PREM.EDW_END_DTTM AS DATE) = ''9999-12-31''
                                                                                                             GROUP BY   AGMT.HOST_AGMT_NUM,
                                                                                                                        F.FEAT_NAME ) PRM
                                                                                  ON              PPV.HOST_AGMT_NUM = PRM.HOST_AGMT_NUM )A
                                                  LEFT OUTER JOIN
                                                                  (
                                                                         SELECT AGMT_ID ,
                                                                                PRTY_ID ,
                                                                                STREET_NAME         AS STREET_NAME ,
                                                                                CITY AS MAILING_CITY ,
                                                                                STATE               AS MAILING_STATE ,
                                                                                POSTL_CD_NUM        AS MAILING_ZIP_CODE
                                                                         FROM   (
                                                                                                SELECT DISTINCT A.HOST_AGMT_NUM,
                                                                                                                A.AGMT_ID,
                                                                                                                PA.PRTY_ID ,
                                                                                                                SA.ADDR_LN_1_TXT            AS STREET_NAME,
                                                                                                                CITY.GEOGRCL_AREA_SHRT_NAME AS CITY,
                                                                                                                ST.GEOGRCL_AREA_SHRT_NAME   AS STATE,
                                                                                                                ZIP.POSTL_CD_NUM            AS POSTL_CD_NUM
                                                                                                FROM            DB_T_PROD_CORE.AGMT A
                                                                                                INNER JOIN      DB_T_PROD_CORE.PRTY_AGMT PA
                                                                                                ON              A.AGMT_ID = PA.AGMT_ID
                                                                                                INNER JOIN
                                                                                                                (
                                                                                                                       SELECT (
                                                                                                                              CASE
                                                                                                                                     WHEN PL.LOCTR_USGE_TYPE_CD = ''MAILSTADRS'' THEN ''1''
                                                                                                                                     WHEN PL.LOCTR_USGE_TYPE_CD = ''BUSNSTADRS'' THEN ''2''
                                                                                                                                     WHEN PL.LOCTR_USGE_TYPE_CD = ''BILLSTADRS'' THEN ''3''
                                                                                                                                     WHEN PL.LOCTR_USGE_TYPE_CD = ''HOMESTADRS'' THEN ''4''
                                                                                                                                     WHEN PL.LOCTR_USGE_TYPE_CD = ''OTHSTADRS '' THEN ''5''
                                                                                                                              END) AS RNK ,
                                                                                                                              PL.PRTY_ID ,
                                                                                                                              LOC_ID ,
                                                                                                                              PL.TRANS_END_DTTM
                                                                                                                       FROM   DB_T_PROD_CORE.PRTY_LOCTR PL
                                                                                                                       WHERE  PL.LOCTR_USGE_TYPE_CD IN ( ''MAILSTADRS'',
                                                                                                                                                        ''BUSNSTADRS'',
                                                                                                                                                        ''BILLSTADRS'',
                                                                                                                                                        ''HOMESTADRS'',
                                                                                                                                                        ''OTHSTADRS'')
                                                                                                                       AND    CAST(PL.EDW_END_DTTM AS DATE) = ''9999-12-31''
                                                                                                                       AND    (
                                                                                                                                     PRTY_ID,RNK) IN
                                                                                                                              (
                                                                                                                                       SELECT   PRTY_ID,
                                                                                                                                                MIN(
                                                                                                                                                CASE
                                                                                                                                                         WHEN PL.LOCTR_USGE_TYPE_CD = ''MAILSTADRS'' THEN ''1''
                                                                                                                                                         WHEN PL.LOCTR_USGE_TYPE_CD = ''BUSNSTADRS'' THEN ''2''
                                                                                                                                                         WHEN PL.LOCTR_USGE_TYPE_CD = ''BILLSTADRS'' THEN ''3''
                                                                                                                                                         WHEN PL.LOCTR_USGE_TYPE_CD = ''HOMESTADRS'' THEN ''4''
                                                                                                                                                         WHEN PL.LOCTR_USGE_TYPE_CD = ''OTHSTADRS '' THEN ''5''
                                                                                                                                                END) AS RNK
                                                                                                                                       FROM     DB_T_PROD_CORE.PRTY_LOCTR PL
                                                                                                                                       
                                                                                                                                       WHERE    PL.LOCTR_USGE_TYPE_CD IN ( ''MAILSTADRS'',
                                                                                                                                                                          ''BUSNSTADRS'',
                                                                                                                                                                          ''BILLSTADRS'',
                                                                                                                                                                          ''HOMESTADRS'',
                                                                                                                                                                          ''OTHSTADRS'')
                                                                                                                                       AND      CAST(PL.EDW_END_DTTM AS DATE) = ''9999-12-31''
                                                                                                                                       GROUP BY PRTY_ID
                                                                                                                                       ) ) PL
                                                                                                ON              PA.PRTY_ID = PL.PRTY_ID
                                                                                                INNER JOIN      DB_T_PROD_CORE.STREET_ADDR SA
                                                                                                ON              PL.LOC_ID = SA.STREET_ADDR_ID
                                                                                                INNER JOIN
                                                                                                                (
                                                                                                                                SELECT DISTINCT C.CITY_ID,
                                                                                                                                                C.GEOGRCL_AREA_SHRT_NAME
                                                                                                                                FROM            DB_T_PROD_CORE.CITY C
                                                                                                                                WHERE           CAST(C.EDW_END_DTTM AS DATE) = ''9999-12-31'' ) CITY
                                                                                                ON              CITY.CITY_ID = SA.CITY_ID
                                                                                                INNER JOIN
                                                                                                                (
                                                                                                                                SELECT DISTINCT T.TERR_ID,
                                                                                                                                                T.GEOGRCL_AREA_SHRT_NAME
                                                                                                                                FROM            DB_T_PROD_CORE.TERR T
                                                                                                                                WHERE           CAST(T.EDW_END_DTTM AS DATE)=''9999-12-31'' ) ST
                                                                                                ON              ST.TERR_ID = SA.TERR_ID
                                                                                                INNER JOIN
                                                                                                                (
                                                                                                                                SELECT DISTINCT PC.POSTL_CD_ID,
                                                                                                                                                PC.POSTL_CD_NUM
                                                                                                                                FROM            DB_T_PROD_CORE.POSTL_CD PC
                                                                                                                                WHERE           CAST(PC.EDW_END_DTTM AS DATE)=''9999-12-31'') ZIP
                                                                                                ON              ZIP.POSTL_CD_ID = SA.POSTL_CD_ID
                                                                                                WHERE           CAST(PL.TRANS_END_DTTM AS DATE) = ''9999-12-31''
                                                                                                AND             CAST(A.EDW_END_DTTM AS    DATE)=''9999-12-31''
                                                                                                AND             CAST(SA.EDW_END_DTTM AS   DATE)=''9999-12-31''
                                                                                                AND             CAST(PA.EDW_END_DTTM AS   DATE)=''9999-12-31''
                                                                                                AND             PA.PRTY_AGMT_ROLE_CD = ''PLCYPRININS'' )INNR ) ADDR
                                                  ON              A.AGMT_ID = ADDR.AGMT_ID
                                                  INNER JOIN
                                                                  (
                                                                                  SELECT DISTINCT AF.AGMT_ID,
                                                                                                  F.FEAT_ID,
                                                                                                  AF.AGMT_FEAT_AMT,
                                                                                                  AF.AGMT_FEAT_STRT_DTTM ,
                                                                                                  AF.AGMT_FEAT_END_DTTM
                                                                                  FROM            DB_T_PROD_CORE.FEAT F
                                                                                  INNER JOIN      DB_T_PROD_CORE.FEAT_RLTD FR
                                                                                  ON              FR.FEAT_ID = F.FEAT_ID
                                                                                  INNER JOIN      DB_T_PROD_CORE.FEAT FEAT2
                                                                                  ON              FEAT2.FEAT_ID = FR.RLTD_FEAT_ID
                                                                                  INNER JOIN      DB_T_PROD_CORE.AGMT_FEAT AF
                                                                                  ON              AF.FEAT_ID = FEAT2.FEAT_ID
                                                                                  WHERE           F.NK_SRC_KEY = ''FOPFarmEquipBrkdwn''
                                                                                  AND             CAST(AF.EDW_END_DTTM AS DATE) = ''9999-12-31'' ) BRKDWN
                                                  ON              A.AGMT_ID=BRKDWN.AGMT_ID
                                                  LEFT JOIN
                                                                  (
                                                                                  SELECT DISTINCT AF.AGMT_ID,
                                                                                                  F.FEAT_ID,
                                                                                                  AF.AGMT_FEAT_AMT,
                                                                                                  AF.AGMT_FEAT_STRT_DTTM ,
                                                                                                  AF.AGMT_FEAT_END_DTTM
                                                                                  FROM            DB_T_PROD_CORE.FEAT F
                                                                                  INNER JOIN      DB_T_PROD_CORE.AGMT_FEAT AF
                                                                                  ON              AF.FEAT_ID =F.FEAT_ID
                                                                                  WHERE           F.NK_SRC_KEY = ''FOPFarmEquipBrkdwnTotCovAmt''
                                                                                  AND             CAST(AF.EDW_END_DTTM AS DATE) = ''9999-12-31'' ) BRKDWN_EB_LIMIT
                                                  ON              A.AGMT_ID=BRKDWN_EB_LIMIT.AGMT_ID
                                                  LEFT JOIN
                                                                  (
                                                                                  SELECT DISTINCT AGMT_ID,
                                                                                                  SPEC_TYPE_CD
                                                                                  FROM            DB_T_PROD_CORE.AGMT_SPEC
                                                                                  WHERE           AGMT_SPEC_TYPE_CD = ''FOPS''
                                                                                  AND             CAST(EDW_END_DTTM AS DATE) = ''9999-12-31'') ASP
                                                  ON              ASP.AGMT_ID = A.AGMT_ID ) SRC ) );
  -- Component EXP_POLICY, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE EXP_POLICY AS
  (
            SELECT    '' '' AS var_spaces,
                      LKP_1.TGT_IDNTFTN_VAL
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FOP */
                          AS out_Product_Code,
                      ''1'' AS out_Record_Type,
                      LKP_2.TGT_IDNTFTN_VAL
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FOP */
                                                                                          AS out_Contract_Number,
                      RPAD ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.Policy_Number , 20 , '' '' ) AS out_Policy_Number,
                      lpad ( var_spaces , 20 , '' '' )                                      AS out_Previous_Policy_Number,
                      SQ_HSB_INFORCE_FARM_POLICY_RECORD.Policy_Effective_Date             AS Policy_Effective_Date,
                      SUBSTR ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.Policy_Effective_Date , 1 , 4 )
                                || SUBSTR ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.Policy_Effective_Date , 6 , 2 )
                                || SUBSTR ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.Policy_Effective_Date , 9 , 2 ) AS out_Policy_Effective_Date,
                      /*to_date ( */ SQ_HSB_INFORCE_FARM_POLICY_RECORD.Policy_Effective_Date /*, ''YYYY-MM-DD'' )*/      AS var_Policy_Effective_Date,
                      SUBSTR ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.policy_Expiration_Date , 1 , 4 )
                                || SUBSTR ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.policy_Expiration_Date , 6 , 2 )
                                || SUBSTR ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.policy_Expiration_Date , 9 , 2 ) AS out_Policy_Expiration_Date,
                      SUBSTR ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.HSP_Coverage_Effective_Date , 1 , 4 )
                                || SUBSTR ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.HSP_Coverage_Effective_Date , 6 , 2 )
                                || SUBSTR ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.HSP_Coverage_Effective_Date , 9 , 2 ) AS OUT_HSP_Coverage_Effective_Date,
/*                     to_date ( */ SQ_HSB_INFORCE_FARM_POLICY_RECORD.HSP_Coverage_Effective_Date /*, ''YYYY-MM-DD'' ) */      AS OUT_HSP_CVGE_EFF_DT,
                      RPAD ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.Name_of_Insured , 55 , '' '' )                         AS out_Name_of_Insured,
                      RPAD ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.Mailing_Address , 55 , '' '' )                         AS out_Mailing_Address,
                      lpad ( var_spaces , 55 , '' '' )                                                                AS out_Mailing_Address_2,
                      RPAD ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.Mailing_City , 20 , '' '' )                            AS out_Mailing_City,
                      RPAD ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.Mailing_State , 55 , '' '' )                           AS out_Mailing_State,
                      RPAD ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.Mailing_Zip_Code , 10 , '' '' )                        AS out_Mailing_Zip_Code,
                      IFNULL(TRY_TO_DECIMAL(SQ_HSB_INFORCE_FARM_POLICY_RECORD.HSB_Coverage_Gross_Premium,6,2), 0)       AS var_HSB_Coverage_Gross_Premium,
                      LPAD ( var_HSB_Coverage_Gross_Premium , 11 , ''0'' )                                            AS out_HSB_Coverage_Gross_Premium,
                      IFNULL(TRY_TO_DECIMAL(SQ_HSB_INFORCE_FARM_POLICY_RECORD.HS_Coverage_Net_Premium,6,2), 0)          AS var_HSB_Coverage_Net_Premium,
                      LPAD ( var_HSB_Coverage_Net_Premium , 11 , ''0'' )                                              AS out_HSB_Coverage_Net_Premium,
                      SQ_HSB_INFORCE_FARM_POLICY_RECORD.Occupancy_Code                                              AS Occupancy_Code,
                      RPAD ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.Occupancy_Description , 15 , '' '' )                   AS out_Occupancy_Description,
                      RPAD ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.EB_Form_Number , 20 , '' '' )                          AS out_EB_Form_Number,
                      RPAD ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.HSB_Coverage_Version , 20 , '' '' )                    AS out_HSB_Coverage_Version,
                      DECODE ( SQ_HSB_INFORCE_FARM_POLICY_RECORD.HSP_Product_Form_Number ,
                              ''Farmowners'' , ''FO81'' ,
                              ''UNKN'' )                                AS var_HSP_Product_Form_Number,
                      RPAD ( var_HSP_Product_Form_Number , 20 , '' '' ) AS out_HSP_Product_Form_Number,
                      lpad ( var_spaces , 20 , '' '' )                  AS out_SLC_Product_Form_Number,
                      lpad ( var_spaces , 20 , '' '' )                  AS out_Farm_Cyber_Product_Form_Number,
                      ''''                                              AS out_Client_Product_Name,
                      SQ_HSB_INFORCE_FARM_POLICY_RECORD.Farm_EB_Limit AS Farm_EB_Limit,
                      ''''                                              AS out_HSP_Limit,
                      ''''                                              AS out_HSP_Deductible_Amount,
                      ''''                                              AS out_SLC_Limit,
                      ''''                                              AS out_SLC_Deductible_Amount,
                      ''''                                              AS out_Farm_Cyber_Limit,
                      ''''                                              AS out_Farm_Cyber_Deductible_Amount,
                      lpad ( var_spaces , 20 , '' '' )                  AS out_Branch_Code,
                      lpad ( var_spaces , 15 , '' '' )                  AS out_Agency_Code,
                      ''''                                              AS out_Pricing_Tier,
                      ''''                                              AS out_Insurance_Score,
                      ''''                                              AS out_Rating_Territory_Code,
                      ''''                                              AS out_Protection_Class_Code,
                      SQ_HSB_INFORCE_FARM_POLICY_RECORD.source_record_id,
                      row_number() over (PARTITION BY SQ_HSB_INFORCE_FARM_POLICY_RECORD.source_record_id ORDER BY SQ_HSB_INFORCE_FARM_POLICY_RECORD.source_record_id) AS RNK
            FROM      SQ_HSB_INFORCE_FARM_POLICY_RECORD
            LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FOP LKP_1
            ON        LKP_1.SRC_IDNTFTN_VAL = SQ_HSB_INFORCE_FARM_POLICY_RECORD.Product_Code
            LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FOP LKP_2
            ON        LKP_2.SRC_IDNTFTN_VAL = SQ_HSB_INFORCE_FARM_POLICY_RECORD.Contract_Number QUALIFY RNK = 1 );
  -- Component LKP_HSB_FOP_PREM_RTS, Type LOOKUP
  CREATE
  OR
  REPLACE TEMPORARY TABLE LKP_HSB_FOP_PREM_RTS AS
  (
            SELECT    LKP.FOP_DEDUCT,
                      LKP.HSB_FOP_PCT,
                      EXP_POLICY.source_record_id,
                      ROW_NUMBER() OVER(PARTITION BY EXP_POLICY.source_record_id ORDER BY LKP.FOP_DEDUCT ASC,LKP.HSB_FOP_PCT ASC,LKP.ALFA_FOP_PCT ASC,LKP.EFF_DT ASC,LKP.EXPN_DT ASC) RNK
            FROM      EXP_POLICY
            LEFT JOIN
                      (
                             SELECT HSB_FOP_PREM_RTS.FOP_DEDUCT   AS FOP_DEDUCT,
                                    HSB_FOP_PREM_RTS.HSB_FOP_PCT  AS HSB_FOP_PCT,
                                    HSB_FOP_PREM_RTS.ALFA_FOP_PCT AS ALFA_FOP_PCT,
                                    HSB_FOP_PREM_RTS.EFF_DT       AS EFF_DT,
                                    HSB_FOP_PREM_RTS.EXPN_DT      AS EXPN_DT
                             FROM   db_t_prod_wrk.HSB_FOP_PREM_RTS
                             WHERE  EXPN_DT = ''9999-12-31'' ) LKP
            ON        LKP.EFF_DT <= EXP_POLICY.OUT_HSP_CVGE_EFF_DT
            AND       LKP.EXPN_DT >= EXP_POLICY.OUT_HSP_CVGE_EFF_DT QUALIFY RNK = 1 );
  -- Component EXPTRANS, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE EXPTRANS AS
  (
             SELECT     LKP_HSB_FOP_PREM_RTS.FOP_DEDUCT                                              AS DEDUCT,
                        LKP_HSB_FOP_PREM_RTS.HSB_FOP_PCT / 100                                       AS var_HSB_PCT,
                        EXP_POLICY.out_HSB_Coverage_Net_Premium                                      AS var_HSP_Coverage_Net_Premium_Amount,
                        IFNULL(TRY_TO_DECIMAL((var_HSP_Coverage_Net_Premium_Amount  * var_HSB_PCT),38,11), 0) AS var_HSP_Coverage_Net_Premium_Amount_pct,
                        lpad ( var_HSP_Coverage_Net_Premium_Amount_pct , 11 , ''0'' )                  AS var_test2,
                        SUBSTR ( var_test2 , 9 , 1 )                                                 AS var_test3,
                        SUBSTR ( var_test2 , 10 , 1 )                                                AS var_test3a,
                        SUBSTR ( var_test2 , 11 , 1 )                                                AS var_test3b,
                        CASE
                                   WHEN var_test3 = ''.''
                                   OR         var_test3a = ''.''
                                   OR         var_test3b = ''.'' THEN ''N''
                                   ELSE ''Y''
                        END AS no_decimal,
                        CASE
                                   WHEN var_test3 = ''.'' THEN var_test2
                                   ELSE
                                              CASE
                                                         WHEN no_decimal = ''Y'' THEN SUBSTR ( var_test2 , 4 , 8 )
                                                                               || ''.00''
                                                         ELSE SUBSTR ( var_test2 , 2 , 10 )
                                                                               || ''0''
                                              END
                        END                                                           AS out_HSP_Net_Prem,
                        lpad ( EXP_POLICY.out_HSB_Coverage_Gross_Premium , 11 , ''0'' ) AS var_gross,
                        SUBSTR ( var_gross , 9 , 1 )                                  AS var_gross1,
                        SUBSTR ( var_gross , 10 , 1 )                                 AS var_gross2,
                        SUBSTR ( var_gross , 11 , 1 )                                 AS var_gross3,
                        CASE
                                   WHEN var_gross1 = ''.''
                                   OR         var_gross2 = ''.''
                                   OR         var_gross3 = ''.'' THEN ''N''
                                   ELSE ''Y''
                        END AS no_decimal1,
                        CASE
                                   WHEN var_gross1 = ''.'' THEN var_gross
                                   ELSE
                                              CASE
                                                         WHEN no_decimal1 = ''Y'' THEN SUBSTR ( var_gross , 4 , 8 )
                                                                               || ''.00''
                                                         ELSE SUBSTR ( var_gross , 2 , 10 )
                                                                               || ''0''
                                              END
                        END AS out_out_HSB_Coverage_Gross_Premium,
                        EXP_POLICY.source_record_id
             FROM       EXP_POLICY
             INNER JOIN LKP_HSB_FOP_PREM_RTS
             ON         EXP_POLICY.source_record_id = LKP_HSB_FOP_PREM_RTS.source_record_id );
  -- Component HSB_INFORCE_FARM_POLICY_RECORD1, Type TARGET_EXPORT_PREPARE Stage data before exporting
  CREATE
  OR
  REPLACE TEMPORARY TABLE HSB_INFORCE_FARM_POLICY_RECORD1 AS
  (
             SELECT     EXP_POLICY.out_Product_Code                   AS Product_Code,
                        EXP_POLICY.out_Record_Type                    AS Record_Type,
                        EXP_POLICY.out_Contract_Number                AS Contract_Number,
                        EXP_POLICY.out_Policy_Number                  AS Policy_Number,
                        EXP_POLICY.out_Previous_Policy_Number         AS Previous_Policy_Number,
                        EXP_POLICY.out_Policy_Effective_Date          AS Policy_Effective_Date,
                        EXP_POLICY.out_Policy_Expiration_Date         AS Policy_Expiration_Date,
                        EXP_POLICY.OUT_HSP_Coverage_Effective_Date    AS HSP_Coverage_Effective_Date,
                        EXP_POLICY.out_Name_of_Insured                AS Name_of_Insured,
                        EXP_POLICY.out_Mailing_Address                AS Mailing_Address,
                        EXP_POLICY.out_Mailing_Address_2              AS Mailing_Address_2,
                        EXP_POLICY.out_Mailing_City                   AS Mailing_City,
                        EXP_POLICY.out_Mailing_State                  AS Mailing_State,
                        EXP_POLICY.out_Mailing_Zip_Code               AS Mailing_Zip_Code,
                        EXPTRANS.out_out_HSB_Coverage_Gross_Premium   AS HSB_Coverage_Gross_Premium,
                        EXPTRANS.out_HSP_Net_Prem                     AS HS_Coverage_Net_Premium,
                        EXP_POLICY.Occupancy_Code                     AS Occupancy_Code,
                        EXP_POLICY.out_Occupancy_Description          AS Occupancy_Description,
                        EXP_POLICY.out_EB_Form_Number                 AS EB_Form_Number,
                        EXP_POLICY.out_HSB_Coverage_Version           AS HSB_Coverage_Version,
                        EXP_POLICY.out_HSP_Product_Form_Number        AS HSP_Product_Form_Number,
                        EXP_POLICY.out_SLC_Product_Form_Number        AS SLC_Product_Form_Number,
                        EXP_POLICY.out_Farm_Cyber_Product_Form_Number AS Farm_Cyber_Product_Form_Number,
                        EXP_POLICY.out_Client_Product_Name            AS Client_Product_Name,
                        EXP_POLICY.Farm_EB_Limit                      AS Farm_EB_Limit,
                        EXPTRANS.DEDUCT                               AS Farm_EB_Deductible_Amount,
                        EXP_POLICY.out_HSP_Limit                      AS HSP_Limit,
                        EXP_POLICY.out_HSP_Deductible_Amount          AS HSP_Deductible_Amount,
                        EXP_POLICY.out_SLC_Limit                      AS SLC_Limit,
                        EXP_POLICY.out_SLC_Deductible_Amount          AS SLC_Deductible_Amount,
                        EXP_POLICY.out_Farm_Cyber_Limit               AS Farm_Cyber_Limit,
                        EXP_POLICY.out_Farm_Cyber_Deductible_Amount   AS Farm_Cyber_Deductible_Amount,
                        EXP_POLICY.out_Branch_Code                    AS Branch_Code,
                        EXP_POLICY.out_Agency_Code                    AS Agency_Code,
                        EXP_POLICY.out_Pricing_Tier                   AS Pricing_Tier,
                        EXP_POLICY.out_Insurance_Score                AS Insurance_Score,
                        EXP_POLICY.out_Rating_Territory_Code          AS Rating_Territory_Code,
                        EXP_POLICY.out_Protection_Class_Code          AS Protection_Class_Code
             FROM       EXP_POLICY
             INNER JOIN EXPTRANS
             ON         EXP_POLICY.source_record_id = EXPTRANS.source_record_id );
  -- Component HSB_INFORCE_FARM_POLICY_RECORD1, Type EXPORT_DATA Exporting data
  ;
  -- PIPELINE END FOR 1
  -- PIPELINE START FOR 2
  -- Component SQ_HSB_INFORCE_FARM_LOCATION_RECORD, Type SOURCE
  CREATE
  OR
  REPLACE TEMPORARY TABLE SQ_HSB_INFORCE_FARM_LOCATION_RECORD AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS Product_Code,
                $2  AS Record_Type,
                $3  AS Contract_Number,
                $4  AS Policy_Number,
                $5  AS Location_ID,
                $6  AS Building_ID,
                $7  AS Location_Effective_Date,
                $8  AS Location_Expiration_Date,
                $9  AS Location_Name,
                $10 AS Address_1,
                $11 AS Address_2,
                $12 AS City,
                $13 AS State,
                $14 AS Postal_Code,
                $15 AS Inspection_Contact_Name,
                $16 AS Inspection_Contact_Phone_Number,
                $17 AS Inspection_Contact_E_mail_Address,
                $18 AS Occupancy_Code,
                $19 AS Occupancy_Description,
                $20 AS Coverage_A_Value,
                $21 AS Coverage_B_Value,
                $22 AS Coverage_C_Value,
                $23 AS Coverage_D_Value,
                $24 AS Year_Built,
                $25 AS Total_Living_Area,
                $26 AS Number_of_Units_in_Dwelling,
                $27 AS Heating_System_Updated_Year,
                $28 AS Electrical_System_Updated_Year,
                $29 AS Plumbing_System_Updated_Year,
                $30 AS Distance_to_Hydrant,
                $31 AS Residence_Type,
                $32 AS Occupancy,
                $33 AS Implements_Coverage,
                $34 AS Selected_Coverage_Limit,
                $35 AS Other_Farm_Structure_Limit,
                $36 AS Scheduled_Property_Value,
                $37 AS Unscheduled_Property_Value,
                $38 AS Equipment_Value,
                $39 AS BI_Value,
                $40 AS EE_Value,
                $41 AS Power_Generation_Indicator,
                $42 AS Power_Generation_KW,
                $43 AS source_record_id
         FROM   (
                         SELECT   SRC.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  /* -------FARM_POLICY_LOCATION_QUERY */
                                                  SELECT DISTINCT ''HSB_Company_Product_Id_FOP''             AS Product_Code ,
                                                                  ''2''                                      AS Record_Type ,
                                                                  ''HSB_Contract_Nbr_FOP''                   AS Contract_Number ,
                                                                  A.POLICY_NUMBER                          AS Policy_Number ,
                                                                  ADDR.LOC_ID                              AS Location_ID ,
                                                                  ''''                                       AS Building_ID ,
                                                                  CAST(BRKDWN.AGMT_FEAT_STRT_DTTM AS DATE) AS Location_Effective_date ,
                                                                  A.COVERAGE_EXP_DATE                      AS location_Expiration_Date ,
                                                                  ''''                                       AS Location_Name ,
                                                                  ADDR.STREET_NAME                         AS Address_1 ,
                                                                  ''''                                       AS Address_2 ,
                                                                  CITY                      AS CITY ,
                                                                  STATE                     AS STATE ,
                                                                  ADDR.POSTL_CD_NUM                        AS Postal_Code ,
                                                                  A.NAME_OF_INSURED                        AS Inspection_Contact_Name ,
                                                                  TLPHN_NUM                 AS Inspection_Contact_Phone_Number ,
                                                                  ''''                                       AS Inspection_Contact_Email_Address ,
                                                                  ''020''                                    AS Occupancy_Code ,
                                                                  '' ''                                      AS Occupancy_Description ,
                                                                  ''''                                       AS Coverage_A_Value ,
                                                                  ''''                                       AS Coverage_B_Value ,
                                                                  ''''                                       AS Coverage_C_Value ,
                                                                  ''''                                       AS Coverage_D_Value ,
                                                                  ''''                                       AS Year_Built ,
                                                                  ''''                                       AS Total_Living_Area ,
                                                                  ''''                                       AS Number_of_Units_in_Dwelling ,
                                                                  ''''                                       AS Heating_System_Updated_Year ,
                                                                  ''''                                       AS Electrical_System_Updated_Year ,
                                                                  ''''                                       AS Plumbing_System_Updated_Year ,
                                                                  ''''                                       AS Distance_to_Hydrant ,
                                                                  ''''                                       AS Residence_Type ,
                                                                  ''''                                       AS Occupancy ,(
                                                                  CASE
                                                                                  WHEN IMPLMNT.AGMT_FEAT_IND_IMP=1 THEN ''Y''
                                                                                  ELSE ''N''
                                                                  END )                                          AS Implements_Coverage ,
                                                                  CAST(SEL_IMPLMNT.AGMT_FEAT_AMT_IMP AS      INTEGER) AS Selected_Coverage_Limit ,
                                                                  CAST(FOP_OTH_STRUCT.AGMT_ASSET_FEAT_AMT AS INTEGER) AS Other_Farm_Structure_Limit ,
                                                                  ''''                                                  AS Scheduled_Property_Value ,
                                                                  ''''                                                  AS Unscheduled_Property_Value ,
                                                                  CAST(BRKDWN_EB_LIMIT.AGMT_FEAT_AMT AS INTEGER)      AS Equipment_Value ,
                                                                  ''25000''                                             AS BI_Value ,
                                                                  ''''                                                  AS EE_Value ,
                                                                  ''N''                                                 AS Power_Generation_Indicator ,
                                                                  ''''                                                  AS Power_Generation_KW
                                                  FROM            (
                                                                                  SELECT DISTINCT PPV.HOST_AGMT_NUM                                       AS POLICY_NUMBER ,
                                                                                                  COALESCE(INAME.INDIV_FULL_NAME,ORG_NAME) AS NAME_OF_INSURED ,
                                                                                                  PPV.AGMT_ID ,
                                                                                                  COALESCE(PRM.PLCY_CVGE_AMT,0.00) AS EB_GROSS_PREMIUM ,
                                                                                                  CAST(PPV.AGMT_EFF_DTTM AS       DATE ) AS COVERAGE_EFFECTIVE_DATE ,
                                                                                                  CAST(PPV.AGMT_PLND_EXPN_DTTM AS DATE ) AS COVERAGE_EXP_DATE ,
                                                                                                  CAST(PPV.MODL_CRTN_DTTM AS      DATE)  AS CRTN_DTTM ,
                                                                                                  CAST(ES.EV_STS_STRT_DTTM AS     DATE)     EV_STS_STRT_DTTM ,
                                                                                                  TLPHN_NUM               AS TLPHN_NUM
                                                                                  FROM            (
                                                                                                         SELECT PPV_AG.AGMT_ID,
                                                                                                                PPV_AG.HOST_AGMT_NUM,
                                                                                                                AGMT_PLND_EXPN_DTTM,
                                                                                                                MODL_CRTN_DTTM,
                                                                                                                AGMT_EFF_DTTM
                                                                                                         FROM   (
                                                                                                                           SELECT     A_S.AGMT_STS_CD        AGMT_STS_CD ,
                                                                                                                                      PPV_INNR.AGMT_ID       AGMT_ID,
                                                                                                                                      PPV_INNR.HOST_AGMT_NUM HOST_AGMT_NUM,
                                                                                                                                      PPV_INNR.AGMT_PLND_EXPN_DTTM,
                                                                                                                                      PPV_INNR.AGMT_EFF_DTTM,
                                                                                                                                      PPV_INNR.MODL_CRTN_DTTM
                                                                                                                           FROM       (
                                                                                                                                               SELECT   T1.HOST_AGMT_NUM,
                                                                                                                                                        T1.TERM_NUM,
                                                                                                                                                        T1.AGMT_ID,
                                                                                                                                                        T1.MODL_CRTN_DTTM,
                                                                                                                                                        T1.MODL_EFF_DTTM,
                                                                                                                                                        T1.AGMT_EFF_DTTM,
                                                                                                                                                        AGMT_PLND_EXPN_DTTM,
                                                                                                                                                        CASE
                                                                                                                                                                 WHEN T1.MODL_EFF_DTTM > T1.MODL_CRTN_DTTM THEN T1.MODL_EFF_DTTM
                                                                                                                                                                 ELSE T1.MODL_CRTN_DTTM
                                                                                                                                                        END                 AS NEW_AGMT_EFF_DTTM
                                                                                                                                               FROM     DB_T_PROD_CORE.AGMT AS T1
                                                                                                                                               WHERE    T1.AGMT_TYPE_CD = ''PPV''
                                                                                                                                               AND      T1.SRC_SYS_CD = ''GWPC''
                                                                                                                                               AND      CAST (CAST (:CAL_END_DT AS DATE)+1 AS TIMESTAMP(6)) - INTERVAL ''0.000001 SECOND'' BETWEEN AGMT_EFF_DTTM AND      AGMT_PLND_EXPN_DTTM
                                                                                                                                               AND      T1.TRANS_STRT_DTTM =
                                                                                                                                                        (
                                                                                                                                                               SELECT MIN(T2.TRANS_STRT_DTTM)
                                                                                                                                                               FROM   DB_T_PROD_CORE.AGMT T2
                                                                                                                                                               WHERE  T1.AGMT_ID = T2.AGMT_ID)
                                                                                                                                               AND      NEW_AGMT_EFF_DTTM <= CAST(CAST(:CAL_END_DT AS DATE)+1 AS TIMESTAMP(6)) - INTERVAL ''0.000001 SECOND'' QUALIFY ROW_NUMBER () OVER (PARTITION BY T1.HOST_AGMT_NUM ORDER BY T1.MODL_CRTN_DTTM DESC) = 1) PPV_INNR
                                                                                                                           INNER JOIN
                                                                                                                                      (
                                                                                                                                               SELECT   HOST_AGMT_NUM,
                                                                                                                                                        TERM_NUM,
                                                                                                                                                        AGMT_ID
                                                                                                                                               FROM     DB_T_PROD_CORE.AGMT
                                                                                                                                               WHERE    AGMT_TYPE_CD = ''POLTRM''
                                                                                                                                               AND      CAST(CAST(:CAL_END_DT AS DATE)+1 AS TIMESTAMP(6)) - INTERVAL ''0.000001 SECOND'' BETWEEN AGMT_EFF_DTTM AND      AGMT_PLND_EXPN_DTTM
                                                                                                                                               GROUP BY 1,
                                                                                                                                                        2,
                                                                                                                                                        3) TRM
                                                                                                                           ON         PPV_INNR.HOST_AGMT_NUM = TRM.HOST_AGMT_NUM
                                                                                                                           AND        PPV_INNR.TERM_NUM = TRM.TERM_NUM
                                                                                                                           INNER JOIN DB_T_PROD_CORE.AGMT_STS A_S
                                                                                                                           ON         TRM.AGMT_ID = A_S.AGMT_ID
                                                                                                                           AND        A_S.AGMT_STS_STRT_DTTM <= CAST(CAST(:CAL_END_DT AS DATE)+1 AS TIMESTAMP(6)) - INTERVAL ''0.000001 SECOND''
                                                                                                                           AND        A_S.AGMT_STS_CD <> ''CNFRMDDT''
                                                                                                                           INNER JOIN DB_T_PROD_CORE.AGMT_PROD AP
                                                                                                                           ON         PPV_INNR.AGMT_ID=AP.AGMT_ID
                                                                                                                           JOIN       DB_T_PROD_CORE.PROD P
                                                                                                                           ON         AP.PROD_ID=P.PROD_ID
                                                                                                                                      /*   AND       INSRNC_TYPE_CD=''COMRCL'' */
                                                                                                                                      QUALIFY ROW_NUMBER() OVER(PARTITION BY PPV_INNR.AGMT_ID ORDER BY A_S.AGMT_STS_STRT_DTTM DESC) = 1
                                                                                                                           AND        A_S.AGMT_STS_CD IN( ''INFORCE'',
                                                                                                                                                         ''RNEWLLAPSD'',
                                                                                                                                                         ''PNDGCNFRMTN'') )PPV_AG )PPV
                                                                                  LEFT OUTER JOIN
                                                                                                  (
                                                                                                                  SELECT DISTINCT PRTY_ID,
                                                                                                                                  AGMT_ID
                                                                                                                  FROM            DB_T_PROD_CORE.PRTY_AGMT PA
                                                                                                                  WHERE           PA.PRTY_AGMT_ROLE_CD IN (''PLCYPRININS'')
                                                                                                                  AND             CAST(PA.EDW_END_DTTM AS DATE) = ''9999-12-31''
                                                                                                                  AND             CAST(:CAL_END_DT AS   DATE) BETWEEN CAST(PA.TRANS_STRT_DTTM AS DATE) AND             CAST(PA.TRANS_END_DTTM AS DATE)) PA
                                                                                  ON              PPV.AGMT_ID = PA.AGMT_ID
                                                                                                  /* --Added Newly */
                                                                                  LEFT OUTER JOIN
                                                                                                  (
                                                                                                                  SELECT DISTINCT PR.PRTY_ID,
                                                                                                                                  PR.LOC_ID,
                                                                                                                                  RANK() OVER (PARTITION BY PRTY_ID ORDER BY PRTY_ADDR_USGE_TYPE_CD ASC) RNK
                                                                                                                  FROM            DB_T_PROD_CORE.PRTY_ADDR PR
                                                                                                                  WHERE           CAST(:CAL_END_DT AS DATE) BETWEEN CAST(PR.PRTY_ADDR_STRT_DTTM AS DATE) AND             CAST(PR.PRTY_ADDR_END_DTTM AS DATE)
                                                                                                                  AND             PR.PRTY_ADDR_USGE_TYPE_CD IN (''CELLPHN'',
                                                                                                                                                                ''WORKPHN'',
                                                                                                                                                                ''HOMEPHN'')
                                                                                                                  AND             CAST(PR.EDW_END_DTTM AS DATE) = ''9999-12-31'' QUALIFY RNK=1 )PR
                                                                                  ON              PA.PRTY_ID=PR.PRTY_ID
                                                                                  LEFT OUTER JOIN DB_T_PROD_CORE.TLPHN_NUM TN
                                                                                  ON              TN.TLPHN_NUM_ID=PR.LOC_ID
                                                                                  AND             CAST(tn.EDW_END_DTTM AS DATE) = ''9999-12-31''
                                                                                                  /* --Added Newly         */
                                                                                  LEFT OUTER JOIN
                                                                                                  (
                                                                                                                  SELECT DISTINCT INAME.INDIV_PRTY_ID,
                                                                                                                                  INAME.INDIV_FULL_NAME
                                                                                                                  FROM            DB_T_PROD_CORE.INDIV_NAME INAME
                                                                                                                  WHERE           CAST(INAME.EDW_END_DTTM AS DATE) = ''9999-12-31'' ) INAME
                                                                                  ON              PA.PRTY_ID = INAME.INDIV_PRTY_ID
                                                                                  LEFT OUTER JOIN
                                                                                                  (
                                                                                                                  SELECT DISTINCT O.PRTY_ID,
                                                                                                                                  ORG_NAME
                                                                                                                  FROM            DB_T_PROD_CORE.ORG_NAME O
                                                                                                                  WHERE           CAST(O.EDW_END_DTTM AS DATE) = ''9999-12-31'') O
                                                                                  ON              PA.PRTY_ID=O.PRTY_ID
                                                                                  LEFT OUTER JOIN
                                                                                                  (
                                                                                                                  SELECT DISTINCT ES.AGMT_ID,
                                                                                                                                  ES.EV_STS_STRT_DTTM
                                                                                                                  FROM            DB_T_PROD_CORE.EV_STS ES
                                                                                                                  WHERE           ES.EV_STS_TYPE_CD = ''BOUND''
                                                                                                                  AND             CAST(ES.EDW_END_DTTM AS DATE)=''9999-12-31'') ES
                                                                                  ON              PPV.AGMT_ID = ES.AGMT_ID
                                                                                  LEFT OUTER JOIN
                                                                                                  (
                                                                                                         SELECT PA.AGMT_ID,
                                                                                                                INTRNL_ORG_NUM ,
                                                                                                                PRTY_AGMT_ROLE_CD
                                                                                                         FROM   DB_T_PROD_CORE.PRTY_AGMT PA
                                                                                                         JOIN   DB_T_PROD_CORE.INTRNL_ORG IO
                                                                                                         ON     PA.PRTY_ID = IO.INTRNL_ORG_PRTY_ID
                                                                                                         AND    CAST(IO.EDW_END_DTTM AS  DATE) = ''9999-12-31''
                                                                                                         WHERE  CAST (PA.EDW_END_DTTM AS DATE) = ''9999-12-31''
                                                                                                         AND    PRTY_AGMT_ROLE_CD=''CMP'' ) COMPANY
                                                                                  ON              PPV.AGMT_ID = COMPANY.AGMT_ID
                                                                                  INNER JOIN
                                                                                                  (
                                                                                                             SELECT     AGMT.HOST_AGMT_NUM,
                                                                                                                        F.FEAT_NAME,
                                                                                                                        SUM(PREM.PLCY_CVGE_AMT) PLCY_CVGE_AMT
                                                                                                             FROM       DB_T_PROD_CORE.PLCY_CVGE_MTRC PREM
                                                                                                             INNER JOIN DB_T_PROD_CORE.AGMT AGMT
                                                                                                             ON         PREM.AGMT_ID=AGMT.AGMT_ID
                                                                                                             JOIN       DB_T_PROD_CORE.FEAT F
                                                                                                             ON         F.FEAT_ID = PREM.FEAT_ID
                                                                                                             WHERE      F.NK_SRC_KEY = ''FOPFarmEquipBrkdwn''
                                                                                                             AND        INSRNC_MTRC_TYPE_CD = ''TRANPREM''
                                                                                                             AND        CAST(:CAL_END_DT AS     DATE) BETWEEN CAST(PREM.PLCY_CVGE_MTRC_STRT_DTTM AS DATE) AND        CAST(PREM.PLCY_CVGE_MTRC_END_DTTM AS DATE)
                                                                                                             AND        CAST(PREM.EDW_END_DTTM AS DATE) = ''9999-12-31''
                                                                                                             GROUP BY   AGMT.HOST_AGMT_NUM,
                                                                                                                        F.FEAT_NAME ) PRM
                                                                                  ON              PPV.HOST_AGMT_NUM = PRM.HOST_AGMT_NUM )A
                                                  JOIN
                                                                  /*LOCATION +OTHER STRUCTURE*/
                                                                  /*LOCATION*/
                                                                  (
                                                                                  SELECT DISTINCT AA.AGMT_ID,
                                                                                                  PAL.PRTY_ASSET_ID AS PRTY_ASSET_ID_PAL,
                                                                                                  PAL.LOC_ID,
                                                                                                  SA.ADDR_LN_1_TXT            AS STREET_NAME,
                                                                                                  CITY.GEOGRCL_AREA_SHRT_NAME AS CITY,
                                                                                                  ST.GEOGRCL_AREA_SHRT_NAME   AS STATE,
                                                                                                  ZIP.POSTL_CD_NUM            AS POSTL_CD_NUM
                                                                                  FROM            DB_T_PROD_CORE.PRTY_ASSET_LOCTR PAL
                                                                                  INNER JOIN      DB_T_PROD_CORE.AGMT_ASSET AA
                                                                                  ON              AA.PRTY_ASSET_ID=PAL.PRTY_ASSET_ID
                                                                                  AND             CAST(AA.EDW_END_DTTM AS DATE) = ''9999-12-31''
                                                                                  INNER JOIN      DB_T_PROD_CORE.PRTY_ASSET PAS
                                                                                  ON              PAS.PRTY_ASSET_ID=PAL.PRTY_ASSET_ID
                                                                                  AND             CAST(paS.EDW_END_DTTM AS DATE) = ''9999-12-31''
                                                                                  AND             PRTY_ASSET_CLASFCN_CD = ''FOPDWELL''
                                                                                  INNER JOIN      DB_T_PROD_CORE.STREET_ADDR SA
                                                                                  ON              PAL.LOC_ID = SA.STREET_ADDR_ID
                                                                                  INNER JOIN
                                                                                                  (
                                                                                                                  SELECT DISTINCT C.CITY_ID,
                                                                                                                                  C.GEOGRCL_AREA_SHRT_NAME
                                                                                                                  FROM            DB_T_PROD_CORE.CITY C
                                                                                                                  WHERE           CAST(C.EDW_END_DTTM AS DATE) = ''9999-12-31'' ) CITY
                                                                                  ON              CITY.CITY_ID = SA.CITY_ID
                                                                                  INNER JOIN
                                                                                                  (
                                                                                                                  SELECT DISTINCT T.TERR_ID,
                                                                                                                                  T.GEOGRCL_AREA_SHRT_NAME
                                                                                                                  FROM            DB_T_PROD_CORE.TERR T
                                                                                                                  WHERE           CAST(T.EDW_END_DTTM AS DATE)=''9999-12-31'' ) ST
                                                                                  ON              ST.TERR_ID = SA.TERR_ID
                                                                                  INNER JOIN
                                                                                                  (
                                                                                                                  SELECT DISTINCT PC.POSTL_CD_ID,
                                                                                                                                  PC.POSTL_CD_NUM
                                                                                                                  FROM            DB_T_PROD_CORE.POSTL_CD PC
                                                                                                                  WHERE           CAST(PC.EDW_END_DTTM AS DATE)=''9999-12-31'') ZIP
                                                                                  ON              ZIP.POSTL_CD_ID = SA.POSTL_CD_ID
                                                                                  WHERE           CAST(PAL.TRANS_END_DTTM AS DATE) = ''9999-12-31''
                                                                                  AND             CAST(SA.EDW_END_DTTM AS    DATE)=''9999-12-31''
                                                                                  AND             CAST(PAS.EDW_END_DTTM AS   DATE)=''9999-12-31'' ) ADDR
                                                  ON              ADDR.AGMT_ID=A.AGMT_ID
                                                                  /*OTHER STRUCTURE*/
                                                  LEFT JOIN
                                                                  (
                                                                                  SELECT DISTINCT AIF.AGMT_ID,
                                                                                                  F.FEAT_ID,
                                                                                                  AIF.AGMT_ASSET_FEAT_AMT,
                                                                                                  AIF.AGMT_ASSET_FEAT_STRT_DTTM ,
                                                                                                  AIF.AGMT_ASSET_FEAT_END_DTTM,
                                                                                                  AIF.PRTY_ASSET_ID,
                                                                                                  HOST_AGMT_NUM
                                                                                  FROM            DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT AIF
                                                                                  INNER JOIN      DB_T_PROD_CORE.FEAT F
                                                                                  ON              AIF.FEAT_ID =F.FEAT_ID
                                                                                  INNER JOIN      DB_T_PROD_CORE.AGMT A
                                                                                  ON              A.AGMT_ID=AIF.AGMT_ID
                                                                                  WHERE           F.NK_SRC_KEY = ''FOPOthrStructLim''
                                                                                  AND             CAST(AIF.EDW_END_DTTM AS DATE) = ''9999-12-31'' )FOP_OTH_STRUCT
                                                  ON              FOP_OTH_STRUCT.PRTY_ASSET_ID=ADDR.PRTY_ASSET_ID_PAL
                                                  AND             ADDR.AGMT_ID=FOP_OTH_STRUCT.AGMT_ID
                                                  AND             COALESCE(FOP_OTH_STRUCT.PRTY_ASSET_ID,''~'') <>''~''
                                                  INNER JOIN
                                                                  (
                                                                                  SELECT DISTINCT AF.AGMT_ID,
                                                                                                  F.FEAT_ID,
                                                                                                  AF.AGMT_FEAT_STRT_DTTM ,
                                                                                                  AF.AGMT_FEAT_END_DTTM
                                                                                  FROM            DB_T_PROD_CORE.FEAT F
                                                                                  INNER JOIN      DB_T_PROD_CORE.FEAT_RLTD FR
                                                                                  ON              FR.FEAT_ID = F.FEAT_ID
                                                                                  INNER JOIN      DB_T_PROD_CORE.FEAT FEAT2
                                                                                  ON              FEAT2.FEAT_ID = FR.RLTD_FEAT_ID
                                                                                  INNER JOIN      DB_T_PROD_CORE.AGMT_FEAT AF
                                                                                  ON              AF.FEAT_ID = FEAT2.FEAT_ID
                                                                                  WHERE           F.NK_SRC_KEY = ''FOPFarmEquipBrkdwn''
                                                                                  AND             CAST(AF.EDW_END_DTTM AS DATE) = ''9999-12-31'' ) BRKDWN
                                                  ON              A.AGMT_ID=BRKDWN.AGMT_ID
                                                  LEFT JOIN
                                                                  (
                                                                                  SELECT DISTINCT AF.AGMT_ID,
                                                                                                  F.FEAT_ID,
                                                                                                  AF.AGMT_FEAT_AMT,
                                                                                                  AF.AGMT_FEAT_STRT_DTTM ,
                                                                                                  AF.AGMT_FEAT_END_DTTM
                                                                                  FROM            DB_T_PROD_CORE.FEAT F
                                                                                  INNER JOIN      DB_T_PROD_CORE.AGMT_FEAT AF
                                                                                  ON              AF.FEAT_ID =F.FEAT_ID
                                                                                  WHERE           F.NK_SRC_KEY = ''FOPFarmEquipBrkdwnTotCovAmt''
                                                                                  AND             CAST(AF.EDW_END_DTTM AS DATE) = ''9999-12-31'' ) BRKDWN_EB_LIMIT
                                                  ON              A.AGMT_ID=BRKDWN_EB_LIMIT.AGMT_ID
                                                  LEFT JOIN
                                                                  (
                                                                                  SELECT DISTINCT HOST_AGMT_NUM,
                                                                                                  aa.agmt_id,
                                                                                                  MAX(
                                                                                                  CASE
                                                                                                                  WHEN f.nk_src_key =''FOPFarmEquipBrkdwnTotCovAmt'' THEN agmt_feat_amt
                                                                                                  END) agmt_feat_amt_imp,
                                                                                                  MAX(
                                                                                                  CASE
                                                                                                                  WHEN f.nk_src_key =''FOPFarmEquipBrkdwnImplmnts'' THEN agmt_feat_ind
                                                                                                  END)agmt_feat_ind_imp
                                                                                  FROM            DB_T_PROD_CORE.FEAT F
                                                                                  JOIN            DB_T_PROD_CORE.AGMT_FEAT AF
                                                                                  ON              F.FEAT_ID=AF.FEAT_ID
                                                                                  JOIN            DB_T_PROD_CORE.AGMT AA
                                                                                  ON              AF.AGMT_ID=AA.AGMT_ID
                                                                                  JOIN            DB_T_PROD_CORE.FEAT_RLTD fr
                                                                                  ON              f.feat_id=fr.rltd_feat_id
                                                                                  JOIN            DB_T_PROD_CORE.FEAT f2
                                                                                  ON              fr.feat_id=f2.feat_id
                                                                                  WHERE           f2.nk_src_key =''FOPFarmEquipBrkdwn''
                                                                                  AND             CAST(AF.EDW_END_DTTM AS DATE) = ''9999-12-31''
                                                                                  GROUP BY        1,
                                                                                                  2 )IMPLMNT
                                                  ON              A.AGMT_ID=IMPLMNT.AGMT_ID
                                                  LEFT JOIN
                                                                  (
                                                                                  SELECT DISTINCT HOST_AGMT_NUM,
                                                                                                  aa.agmt_id,
                                                                                                  MAX(
                                                                                                  CASE
                                                                                                                  WHEN f.nk_src_key =''FOPFarmEquipBrkdwnTotCovAmt'' THEN agmt_feat_amt
                                                                                                  END) agmt_feat_amt_imp,
                                                                                                  MAX(
                                                                                                  CASE
                                                                                                                  WHEN f.nk_src_key =''FOPFarmEquipBrkdwnImplmnts'' THEN agmt_feat_ind
                                                                                                  END)agmt_feat_ind_imp
                                                                                  FROM            DB_T_PROD_CORE.FEAT F
                                                                                  JOIN            DB_T_PROD_CORE.AGMT_FEAT AF
                                                                                  ON              F.FEAT_ID=AF.FEAT_ID
                                                                                  JOIN            DB_T_PROD_CORE.AGMT AA
                                                                                  ON              AF.AGMT_ID=AA.AGMT_ID
                                                                                  JOIN            DB_T_PROD_CORE.FEAT_RLTD fr
                                                                                  ON              f.feat_id=fr.rltd_feat_id
                                                                                  JOIN            DB_T_PROD_CORE.FEAT f2
                                                                                  ON              fr.feat_id=f2.feat_id
                                                                                  WHERE           f2.nk_src_key =''FOPFarmEquipBrkdwn''
                                                                                  AND             CAST(AF.EDW_END_DTTM AS DATE) = ''9999-12-31''
                                                                                  GROUP BY        1,
                                                                                                  2
                                                                                  HAVING          agmt_feat_ind_imp=1)SEL_IMPLMNT
                                                  ON              A.AGMT_ID=SEL_IMPLMNT.AGMT_ID ) SRC ) );
  -- Component EXP_LOCATION, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE EXP_LOCATION AS
  (
            SELECT    LKP_1.TGT_IDNTFTN_VAL
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FOP */
                                                                      AS out_Product_Code,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Record_Type AS Record_Type,
                      LKP_2.TGT_IDNTFTN_VAL
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FOP */
                                                                        AS out_Contract_Number,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Policy_Number AS Policy_Number,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Location_ID   AS Location_ID,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Building_ID   AS Building_ID,
                      SUBSTR ( SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Location_Effective_Date , 1 , 4 )
                                || SUBSTR ( SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Location_Effective_Date , 6 , 2 )
                                || SUBSTR ( SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Location_Effective_Date , 9 , 2 ) AS out_Location_Effective_Date,
                      SUBSTR ( SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Location_Expiration_Date , 1 , 4 )
                                || SUBSTR ( SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Location_Expiration_Date , 6 , 2 )
                                || SUBSTR ( SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Location_Expiration_Date , 9 , 2 ) AS out_Location_Expiration_Date,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Location_Name                                            AS Location_Name,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Address_1                                                AS Address_1,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Address_2                                                AS Address_2,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.City                                                     AS City,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.State                                                    AS State,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Postal_Code                                              AS Postal_Code,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Inspection_Contact_Name                                  AS Inspection_Contact_Name,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Inspection_Contact_Phone_Number                          AS Inspection_Contact_Phone_Number,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Inspection_Contact_E_mail_Address                        AS Inspection_Contact_E_mail_Address,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Occupancy_Code                                           AS Occupancy_Code,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Occupancy_Description                                    AS Occupancy_Description,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Coverage_A_Value                                         AS Coverage_A_Value,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Coverage_B_Value                                         AS Coverage_B_Value,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Coverage_C_Value                                         AS Coverage_C_Value,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Coverage_D_Value                                         AS Coverage_D_Value,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Year_Built                                               AS Year_Built,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Total_Living_Area                                        AS Total_Living_Area,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Number_of_Units_in_Dwelling                              AS Number_of_Units_in_Dwelling,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Heating_System_Updated_Year                              AS Heating_System_Updated_Year,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Electrical_System_Updated_Year                           AS Electrical_System_Updated_Year,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Plumbing_System_Updated_Year                             AS Plumbing_System_Updated_Year,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Distance_to_Hydrant                                      AS Distance_to_Hydrant,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Residence_Type                                           AS Residence_Type,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Occupancy                                                AS Occupancy,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Implements_Coverage                                      AS Implements_Coverage,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Selected_Coverage_Limit                                  AS Selected_Coverage_Limit,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Other_Farm_Structure_Limit                               AS Other_Farm_Structure_Limit,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Scheduled_Property_Value                                 AS Scheduled_Property_Value,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Unscheduled_Property_Value                               AS Unscheduled_Property_Value,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Equipment_Value                                          AS Equipment_Value,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.BI_Value                                                 AS BI_Value,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.EE_Value                                                 AS EE_Value,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Power_Generation_Indicator                               AS Power_Generation_Indicator,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Power_Generation_KW                                      AS Power_Generation_KW,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD.source_record_id,
                      row_number() over (PARTITION BY SQ_HSB_INFORCE_FARM_LOCATION_RECORD.source_record_id ORDER BY SQ_HSB_INFORCE_FARM_LOCATION_RECORD.source_record_id) AS RNK
            FROM      SQ_HSB_INFORCE_FARM_LOCATION_RECORD
            LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FOP LKP_1
            ON        LKP_1.SRC_IDNTFTN_VAL = SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Product_Code
            LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FOP LKP_2
            ON        LKP_2.SRC_IDNTFTN_VAL = SQ_HSB_INFORCE_FARM_LOCATION_RECORD.Contract_Number QUALIFY RNK = 1 );
  -- Component HSB_INFORCE_FARM_LOCATION_RECORD1, Type TARGET_EXPORT_PREPARE Stage data before exporting
  CREATE
  OR
  REPLACE TEMPORARY TABLE HSB_INFORCE_FARM_LOCATION_RECORD1 AS
  (
         SELECT EXP_LOCATION.out_Product_Code                  AS Product_Code,
                EXP_LOCATION.Record_Type                       AS Record_Type,
                EXP_LOCATION.out_Contract_Number               AS Contract_Number,
                EXP_LOCATION.Policy_Number                     AS Policy_Number,
                EXP_LOCATION.Location_ID                       AS Location_ID,
                EXP_LOCATION.Building_ID                       AS Building_ID,
                EXP_LOCATION.out_Location_Effective_Date       AS Location_Effective_Date,
                EXP_LOCATION.out_Location_Expiration_Date      AS Location_Expiration_Date,
                EXP_LOCATION.Location_Name                     AS Location_Name,
                EXP_LOCATION.Address_1                         AS Address_1,
                EXP_LOCATION.Address_2                         AS Address_2,
                EXP_LOCATION.City                              AS City,
                EXP_LOCATION.State                             AS State,
                EXP_LOCATION.Postal_Code                       AS Postal_Code,
                EXP_LOCATION.Inspection_Contact_Name           AS Inspection_Contact_Name,
                EXP_LOCATION.Inspection_Contact_Phone_Number   AS Inspection_Contact_Phone_Number,
                EXP_LOCATION.Inspection_Contact_E_mail_Address AS Inspection_Contact_E_mail_Address,
                EXP_LOCATION.Occupancy_Code                    AS Occupancy_Code,
                EXP_LOCATION.Occupancy_Description             AS Occupancy_Description,
                EXP_LOCATION.Coverage_A_Value                  AS Coverage_A_Value,
                EXP_LOCATION.Coverage_B_Value                  AS Coverage_B_Value,
                EXP_LOCATION.Coverage_C_Value                  AS Coverage_C_Value,
                EXP_LOCATION.Coverage_D_Value                  AS Coverage_D_Value,
                EXP_LOCATION.Year_Built                        AS Year_Built,
                EXP_LOCATION.Total_Living_Area                 AS Total_Living_Area,
                EXP_LOCATION.Number_of_Units_in_Dwelling       AS Number_of_Units_in_Dwelling,
                EXP_LOCATION.Heating_System_Updated_Year       AS Heating_System_Updated_Year,
                EXP_LOCATION.Electrical_System_Updated_Year    AS Electrical_System_Updated_Year,
                EXP_LOCATION.Plumbing_System_Updated_Year      AS Plumbing_System_Updated_Year,
                EXP_LOCATION.Distance_to_Hydrant               AS Distance_to_Hydrant,
                EXP_LOCATION.Residence_Type                    AS Residence_Type,
                EXP_LOCATION.Occupancy                         AS Occupancy,
                EXP_LOCATION.Implements_Coverage               AS Implements_Coverage,
                EXP_LOCATION.Selected_Coverage_Limit           AS Selected_Coverage_Limit,
                EXP_LOCATION.Other_Farm_Structure_Limit        AS Other_Farm_Structure_Limit,
                EXP_LOCATION.Scheduled_Property_Value          AS Scheduled_Property_Value,
                EXP_LOCATION.Unscheduled_Property_Value        AS Unscheduled_Property_Value,
                EXP_LOCATION.Equipment_Value                   AS Equipment_Value,
                EXP_LOCATION.BI_Value                          AS BI_Value,
                EXP_LOCATION.EE_Value                          AS EE_Value,
                EXP_LOCATION.Power_Generation_Indicator        AS Power_Generation_Indicator,
                EXP_LOCATION.Power_Generation_KW               AS Power_Generation_KW
         FROM   EXP_LOCATION );
  -- Component HSB_INFORCE_FARM_LOCATION_RECORD1, Type EXPORT_DATA Exporting data
  ;
  -- PIPELINE END FOR 2
  -- PIPELINE START FOR 3
  -- Component SQ_HSB_INFORCE_FARM_POLICY_RECORD1_SRC, Type TABLE_DDL Creating an empty table
  CREATE
  OR
  REPLACE TEMPORARY TABLE SQ_HSB_INFORCE_FARM_POLICY_RECORD1 ( Product_Code VARCHAR(50), Record_Type VARCHAR(50), Contract_Number VARCHAR(50), Policy_Number VARCHAR(50), Previous_Policy_Number VARCHAR(50), Policy_Effective_Date VARCHAR(50), Policy_Expiration_Date VARCHAR(50), HSP_Coverage_Effective_Date VARCHAR(50), Name_of_Insured VARCHAR(50), Mailing_Address VARCHAR(50), Mailing_Address_2 VARCHAR(50), Mailing_City VARCHAR(50), Mailing_State VARCHAR(50), Mailing_Zip_Code VARCHAR(50), HSB_Coverage_Gross_Premium VARCHAR(50), HS_Coverage_Net_Premium VARCHAR(50), Occupancy_Code VARCHAR(50), Occupancy_Description VARCHAR(50), EB_Form_Number VARCHAR(50), HSB_Coverage_Version VARCHAR(50), HSP_Product_Form_Number VARCHAR(50), SLC_Product_Form_Number VARCHAR(50), Farm_Cyber_Product_Form_Number VARCHAR(50), Client_Product_Name VARCHAR(50), Farm_EB_Limit VARCHAR(50), Farm_EB_Deductible_Amount VARCHAR(50), HSP_Limit VARCHAR(50), HSP_Deductible_Amount VARCHAR(50), SLC_Limit VARCHAR(50), SLC_Deductible_Amount VARCHAR(50), Farm_Cyber_Limit VARCHAR(50), Farm_Cyber_Deductible_Amount VARCHAR(50), Branch_Code VARCHAR(50), Agency_Code VARCHAR(50), Pricing_Tier VARCHAR(50), Insurance_Score VARCHAR(50), Rating_Territory_Code VARCHAR(50), Protection_Class_Code VARCHAR(50), source_record_id NUMBER autoincrement START 1 INCREMENT 1 );
  
  -- Component SQ_HSB_INFORCE_FARM_POLICY_RECORD1_SRC, Type IMPORT_DATA Importing Data
  ;
  -- PIPELINE START FOR 3
  -- Component SQ_HSB_INFORCE_FARM_LOCATION_RECORD1_SRC, Type TABLE_DDL Creating an empty table
  CREATE
  OR
  REPLACE TEMPORARY TABLE SQ_HSB_INFORCE_FARM_LOCATION_RECORD1 ( Product_Code VARCHAR(50), Record_Type VARCHAR(50), Contract_Number VARCHAR(50), Policy_Number VARCHAR(50), Location_ID VARCHAR(50), Building_ID VARCHAR(50), Location_Effective_Date VARCHAR(50), Location_Expiration_Date VARCHAR(50), Location_Name VARCHAR(50), Address_1 VARCHAR(50), Address_2 VARCHAR(50), City VARCHAR(50), State VARCHAR(50), Postal_Code VARCHAR(50), Inspection_Contact_Name VARCHAR(50), Inspection_Contact_Phone_Number VARCHAR(50), Inspection_Contact_E_mail_Address VARCHAR(50), Occupancy_Code VARCHAR(50), Occupancy_Description VARCHAR(50), Coverage_A_Value VARCHAR(50), Coverage_B_Value VARCHAR(50), Coverage_C_Value VARCHAR(50), Coverage_D_Value VARCHAR(50), Year_Built VARCHAR(50), Total_Living_Area VARCHAR(50), Number_of_Units_in_Dwelling VARCHAR(50), Heating_System_Updated_Year VARCHAR(50), Electrical_System_Updated_Year VARCHAR(50), Plumbing_System_Updated_Year VARCHAR(50), Distance_to_Hydrant VARCHAR(50), Residence_Type VARCHAR(50), Occupancy VARCHAR(50), Implements_Coverage VARCHAR(50), Selected_Coverage_Limit VARCHAR(50), Other_Farm_Structure_Limit VARCHAR(50), Scheduled_Property_Value VARCHAR(50), Unscheduled_Property_Value VARCHAR(50), Equipment_Value VARCHAR(50), BI_Value VARCHAR(50), EE_Value VARCHAR(50), Power_Generation_Indicator VARCHAR(50), Power_Generation_KW VARCHAR(50), source_record_id NUMBER autoincrement START 1 INCREMENT 1 );
  
  -- Component SQ_HSB_INFORCE_FARM_LOCATION_RECORD1_SRC, Type IMPORT_DATA Importing Data
  ;
  
  -- Component Union, Type UNION_TRANSFORMATION
  CREATE
  OR
  REPLACE TEMPORARY TABLE TBL_UNION
        AS
        (
               /* Union Group POLICY_RECORD */
               SELECT SQ_HSB_INFORCE_FARM_POLICY_RECORD1.Product_Code,
                      SQ_HSB_INFORCE_FARM_POLICY_RECORD1.source_record_id
               FROM   SQ_HSB_INFORCE_FARM_POLICY_RECORD1
               UNION ALL
               /* Union Group LOCATION_RECORD */
               SELECT SQ_HSB_INFORCE_FARM_LOCATION_RECORD1.Product_Code,
                      SQ_HSB_INFORCE_FARM_LOCATION_RECORD1.source_record_id
               FROM   SQ_HSB_INFORCE_FARM_LOCATION_RECORD1 );
  
  -- Component AGG_CNT_RECORDS, Type AGGREGATOR
  CREATE
  OR
  REPLACE TEMPORARY TABLE AGG_CNT_RECORDS AS
  (
         /*SELECT MIN(TBL_UNION.Product_Code)     AS INPUT,
                COUNT(INPUT)                AS COUNTVAL,
                MIN(TBL_UNION.source_record_id) AS source_record_id
         FROM
         TBL_UNION */
        SELECT 
        MIN(TBL_UNION.Product_Code) AS Product_Code,
        COUNT(*) AS COUNTVAL,
        MIN(TBL_UNION.source_record_id) AS source_record_id
    FROM TBL_UNION
    GROUP BY TBL_UNION.Product_Code     
         
);
  -- Component EXP_TRAILER, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE EXP_TRAILER AS
  (
         SELECT ''5519''                   AS Company_Product_Code,
                ''EBF''                    AS Product_Name,
                ''CONTROL''                AS Record_Type,
                AGG_CNT_RECORDS.COUNTVAL AS Number_Of_Records,
                :CAL_END_DT              AS Inforce_Date1,
                SUBSTR ( Inforce_Date1 , 1 , 4 )
                       || SUBSTR ( Inforce_Date1 , 6 , 2 ) AS out_Inforce_Date,
                ''02.03''                                    AS Version_Number,
                AGG_CNT_RECORDS.source_record_id
         FROM   AGG_CNT_RECORDS );
  -- Component HSB_Inforce_Farm_Trailer, Type TARGET_EXPORT_PREPARE Stage data before exporting
  CREATE
  OR
  REPLACE TEMPORARY TABLE HSB_Inforce_Farm_Trailer AS
  (
         SELECT EXP_TRAILER.Company_Product_Code AS C1_Company_Product_Code,
                EXP_TRAILER.Product_Name         AS C1_Product_Name,
                EXP_TRAILER.Record_Type          AS C1_Record_Type,
                EXP_TRAILER.Number_Of_Records    AS C1_Number_of_Records,
                EXP_TRAILER.out_Inforce_Date     AS C1_Inforce_Date,
                EXP_TRAILER.Version_Number       AS C1_Version_Number
         FROM   EXP_TRAILER );
  -- Component HSB_Inforce_Farm_Trailer, Type EXPORT_DATA Exporting data
  ;
END;
';