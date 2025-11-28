-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_WRK_HSB_INFORCE_INS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
 DECLARE
run_id STRING;
prcs_id int;
CAL_END_DT date;



BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
CAL_END_DT :=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''CAL_END_DT'' order by insert_ts desc limit 1);

-- Component LKP_TERADATA_ETL_REF_XLAT_CLIENT_PRODUCT_NAME, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_CLIENT_PRODUCT_NAME AS
(
SELECT  

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

     ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''NOT_APPLICABLE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_CONTRACT_NUMBER, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_CONTRACT_NUMBER AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

     ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''NOT_APPLICABLE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_PRODUCT_CODE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_PRODUCT_CODE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

     ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''NOT_APPLICABLE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- PIPELINE START FOR 1

-- Component SQ_HSB_Inforce_File, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_HSB_Inforce_File AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Company_Product_Code,
$2 as Product_Name,
$3 as Contract_Number,
$4 as Policy_Number,
$5 as Coverage_Effective_Date,
$6 as Coverage_Expiration_Date,
$7 as Name_of_Insured,
$8 as Dwelling_Address,
$9 as Dwelling_City,
$10 as Dwelling_State,
$11 as Dwelling_Zip_Code,
$12 as HSP_Net_Premium_Amount,
$13 as HSP_Deductible_Amount,
$14 as Coverage_A_Value,
$15 as Homeowner_Policy_Form_Number,
$16 as HSP_Product_Form_Number,
$17 as Client_Product_Name,
$18 as Dwelling_Type,
$19 as Base_Homeowner_Premium,
$20 as Final_Homeowner_Premium,
$21 as Homeowner_Policy_Deductible_Amount,
$22 as Construction_Year,
$23 as Square_Footage,
$24 as Number_of_Dwelling_Units_in_Building,
$25 as Heating_System_Updated_Year,
$26 as Electrical_System_Updated_Year,
$27 as Plumbing_System_Updated_Year,
$28 as Pricing_Tier,
$29 as Insurance_Score,
$30 as Rating_Territory_Code,
$31 as Protection_Class_Code,
$32 as HSP_Coverage_Effective_Date,
$33 as Coverage_B_Value,
$34 as Coverage_C_Value,
$35 as Distance_to_Hydrant,
$36 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
WITH LOCATION_ADDRESS_PROPERTY  AS

(

SELECT   PRTY_ASSET_LOCTR.PRTY_ASSET_ID,

                    PRTY_ASSET_LOCTR.LOC_ID,

                    PRTY_ASSET_LOCTR.PRTY_ASSET_LOCTR_ROLE_CD,

                    CITY.GEOGRCL_AREA_NAME   CITY,

                    STREET_ADDR.ADDR_LN_1_TXT    PLCY_MAIL_ADDRESS_1,     

                    PRTY_ASSET_LOCTR.FIRE_DEPT_ID  ,

                    POSTL_CD.POSTL_CD_NUM   ZIP , 

                    TERR.GEOGRCL_AREA_SHRT_NAME 

           FROM DB_T_PROD_CORE.PRTY_ASSET_LOCTR

                 JOIN DB_T_PROD_CORE.STREET_ADDR ON STREET_ADDR.STREET_ADDR_ID=PRTY_ASSET_LOCTR.LOC_ID

                                     AND CAST(STREET_ADDR.EDW_END_DTTM AS DATE )=''12/31/9999''

                  LEFT JOIN DB_T_PROD_CORE.CITY ON CITY.CITY_ID=STREET_ADDR.CITY_ID 

                                     AND CAST(CITY.EDW_END_DTTM AS DATE )=''12/31/9999''

                  LEFT JOIN DB_T_PROD_CORE.POSTL_CD ON POSTL_CD.POSTL_CD_ID=STREET_ADDR.POSTL_CD_ID 

                                     AND CAST(POSTL_CD.EDW_END_DTTM AS DATE )=''12/31/9999''

                  LEFT JOIN DB_T_PROD_CORE.TERR ON TERR.TERR_ID=STREET_ADDR.TERR_ID 

                                     AND CAST(TERR.EDW_END_DTTM AS DATE )=''12/31/9999''

                  WHERE  PRTY_ASSET_LOCTR.PRTY_ASSET_LOCTR_ROLE_CD=''RSKSTADRS''

                                    AND  PRTY_ASSET_LOCTR.TRANS_END_DTTM =to_timestamp(''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'')   

)



SELECT distinct



/* a.agmt_id,  a.modl_num, a.term_num,  aa.prty_asset_id, */
/* as3.agmt_sts_cd, */


''HSB_Company_Product_ID''                                                        as Product_Code

,''HSB_Product_Code''                                                                      as Company_Product_ID

,''HSB_Contract_Number''                                                                as Contract_Number

,A.HOST_AGMT_NUM                                                                    as Policy_Number

,cast(A.AGMT_EFF_DTTM   as date)                                       as Coverage_Effective_Date

,cast(A.AGMT_PLND_EXPN_DTTM  as date)                       as Coverage_Expiration_Date

,NAME.NIN_NAME                                                                           as Name_of_Insured

,RISK_LOCATION.PLCY_MAIL_ADDRESS_1                        as Dwelling_Address

,CITY                                                                   as Dwelling_City

,Risk_LOCATION.GEOGRCL_AREA_SHRT_NAME           as Dwelling_State

,RISK_LOCATION.ZIP                                                                      as Dwelling_Zip_Code



,cast(PRM.PLCY_CVGE_AMT as DECIMAL(6,2))                  as HSP_Net_Premium_Amount

,''9999-12-31''                                                                                         as HSP_Deductible_Amount

,cast(Dwelling.AGMT_FEAT_AMT as int)                                 as Coverage_A_Value

,P.HOST_PROD_ID                                                                         as Homeowner_Policy_Form_Number

,P.PROD_NAME                                                                               as HSP_Product_Form_Number

/* ,F.feat_name                                                                                       as HSP_Product_Form_Number */
,''HSB_Client_Product_Name''                                                       as Client_Product_Name

,''Blanks''                                                                                                as  Dwelling_Type

/* ,ASSET_TYPE2.asset_dtl_cd                                                   as  Dwelling_Type */
,''Blanks''                                                                                                as Base_Homeowner_Premium

,cast(PM.plcy_amt  as decimal(8,2) )                                    as Final_Homeowner_Premium

,cast(Dwelling_Deductible.AGMT_FEAT_AMT as int)        as Homeowner_Policy_Deductible_Amount

,extract (Year from RS.CNSTRCTN_DT)                                 as Construction_Year

,cast (PASSP.PRTY_ASSET_SPEC_MEAS as int)             as Square_Footage

,''Blanks''                                                                                                as  Number_of_Dwelling_Units_in_Building

/* ,ASSET_TYPE.asset_dtl_cd                                                     as  Number_of_Dwelling_Units_in_Building */
,''Blanks''                                                                                                as Heating_System_Updated_Year

,''Blanks''                                                                                                as Electrical_System_Updated_Year

,''Blanks''                                                                                                as Plumbing_System_Updated_Year

,''Blanks''                                                                                                as Pricing_Tier

,''Blanks''                                                                                                as Insurance_Score

,''Blanks''                                                                                                as Rating_Territory_Code

,''Blanks''                                                                                                as Protection_Class_Code

,cast(HSP_COV.AGMT_FEAT_STRT_DTTM  as date)     as HSP_Coverage_Effective_Date

,cast(OTH_STRCT_LIMIT.AGMT_FEAT_AMT as int)         as Coverage_B_Value

,cast(PersonalP.AGMT_FEAT_AMT as int)                             as Coverage_C_Value

,''Blanks''                                                                                                as  Distance_to_Hydrant





from DB_T_PROD_CORE.AGMT A



inner join DB_T_PROD_CORE.agmt_STS           as3

   on A.AGMT_ID = AS3.AGMT_ID 

   AND CAST(AS3.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join DB_T_PROD_CORE.PRTY_AGMT      PA

   on A.AGMT_ID = PA.AGMT_ID

   AND CAST(PA.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join DB_T_PROD_CORE.AGMT_ASSET    AA

   on A.AGMT_ID = AA.AGMT_ID

   AND CAST(AA.EDW_END_DTTM AS DATE )=''12/31/9999''



 inner join DB_T_PROD_CORE.PRTY_ASSET PAS  

   on PAS.PRTY_ASSET_ID = AA.PRTY_ASSET_ID 

  and prty_asset_clasfcn_cd = ''MAIN'' 

   AND CAST(PAS.EDW_END_DTTM AS DATE )=''12/31/9999'' 



left join DB_T_PROD_CORE.REAL_ESTAT      RS

   on AA.PRTY_ASSET_ID = RS.PRTY_ASSET_ID

   AND CAST(RS.EDW_END_DTTM AS DATE )=''12/31/9999''



/*  

inner join DB_T_PROD_CORE.plcy_cvge_mtrc    prm 

   on a.agmt_id = prm.agmt_id

   AND prm.insrnc_mtrc_type_cd = ''TRANPREM''   

   AND CAST(prm.EDW_END_DTTM AS DATE )=''12/31/9999''*/

   

     

 inner join (select agmtx.HOST_AGMT_NUM as host_agmt_num, agmtx.TERM_NUM as term_num, agmtx.agmt_type_cd as agmt_type_cd, pcm.feat_id as FEAT_ID, max(cast(PcM.PLCY_cvge_AMT as DECIMAL(10,2))) as PLCY_CVGE_AMT

 			from DB_T_PROD_CORE.AGMT agmtx

 			join DB_T_PROD_CORE.plcy_cvge_mtrc pcm on pcm.agmt_id = agmtx.AGMT_ID

 			join DB_T_PROD_CORE.FEAT featx on featx.feat_id = pcm.feat_id and  featx.nk_src_key = ''HOLI_HomeSystemsProtection_alfa''

                   where pcm.insrnc_mtrc_type_cd = ''TRANPREM''   AND CAST(pcm.EDW_END_DTTM AS DATE )=''12/31/9999''  and cast(pcm.trans_strt_dttm as date) <= :CAL_END_DT    and cast(agmtx.modl_eff_dttm as date) <= :CAL_END_DT  

 			group by agmtx.HOST_AGMT_NUM, agmtx.TERM_NUM, agmtx.agmt_type_cd, pcm.feat_id) prm on prm.host_agmt_num = a.HOST_AGMT_NUM and prm.term_num = a.term_num and prm.agmt_type_cd = ''ppv''  



left join DB_T_PROD_CORE.PRTY_ASSET_SPEC    PASSP

   on  PAS.PRTY_ASSET_ID = PASSP.PRTY_ASSET_ID

   AND  PASSP.PRTY_ASSET_SPEC_TYPE_CD = ''SIZE''

   AND CAST(PASSP.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join DB_T_PROD_CORE.FEAT F

   on F.FEAT_ID = prm.FEAT_ID

   AND CAST(F.EDW_END_DTTM AS DATE )=''12/31/9999''



left join   ( select  distinct af1.AGMT_ID, af1.FEAT_ID , af1.AGMT_FEAT_AMT 

       FROM DB_T_PROD_CORE.AGMT_FEAT af1       

       JOIN DB_T_PROD_CORE.FEAT  F1   ON  F1.FEAT_ID = AF1.FEAT_ID 

                WHERE F1.nk_src_key = ''HODW_Dwelling_Limit_HOE''  

				AND CAST(af1.EDW_END_DTTM AS DATE )=''12/31/9999'') Dwelling

                                 on a.agmt_id = dwelling.agmt_id



 left join   ( select  distinct af2.AGMT_ID, af2.FEAT_ID , af2.AGMT_FEAT_AMT 

        FROM DB_T_PROD_CORE.AGMT_FEAT af2       

       JOIN DB_T_PROD_CORE.FEAT  F2   ON  F2.FEAT_ID = AF2.FEAT_ID 

                WHERE F2.nk_src_key = ''HODW_PersonalPropertyLimit_alfa''  

				AND CAST(af2.EDW_END_DTTM AS DATE )=''12/31/9999'') PersonalP

                                 on a.agmt_id = personalp.agmt_id



 left  join   ( select  distinct af3.AGMT_ID, af3.FEAT_ID , af3.AGMT_FEAT_AMT 

         FROM DB_T_PROD_CORE.AGMT_FEAT af3       

        JOIN DB_T_PROD_CORE.FEAT  F3   ON  F3.FEAT_ID = AF3.FEAT_ID 

                WHERE F3.nk_src_key = ''HODW_OTHERSTRUCTURESLIMIT_ALFA'' 

				AND CAST(af3.EDW_END_DTTM AS DATE )=''12/31/9999'') OTH_STRCT_LIMIT

                                 on a.agmt_id = oth_strct_limit.agmt_id



left  join   ( select  distinct af4.AGMT_ID, af4.FEAT_ID , af4.AGMT_FEAT_AMT 

          FROM DB_T_PROD_CORE.AGMT_FEAT af4       

         JOIN DB_T_PROD_CORE.FEAT  F4   ON  F4.FEAT_ID = AF4.FEAT_ID 

                WHERE F4.nk_src_key = ''HODW_OtherPerilsDedValue_alfa''  

				AND CAST(af4.EDW_END_DTTM AS DATE )=''12/31/9999'') Dwelling_Deductible 

                                 on a.agmt_id = Dwelling_Deductible.agmt_id

 

inner  join   ( select  distinct af5.AGMT_ID, af5.FEAT_ID , af5.AGMT_FEAT_AMT , af5.AGMT_FEAT_STRT_DTTM , af5.AGMT_FEAT_END_DTTM

          FROM DB_T_PROD_CORE.AGMT_FEAT af5       

         JOIN DB_T_PROD_CORE.FEAT  F5   ON  F5.FEAT_ID = AF5.FEAT_ID 

         

         LEFT JOIN DB_T_PROD_CORE.feat_rltd fr on fr.rltd_feat_id = F5.FEAT_ID

                      left join DB_T_PROD_CORE.FEAT f6 on f6.feat_id = fr.feat_id

         

                WHERE F6.nk_src_key = ''HOLI_HomeSystemsProtection_alfa''

				AND CAST(af5.EDW_END_DTTM AS DATE )=''12/31/9999'') HSP_COV 

                                   on a.agmt_id = HSP_COV.agmt_id                               

                           



inner join DB_T_PROD_CORE.AGMT_PROD AP

   on A.AGMT_ID = AP.AGMT_ID

   AND CAST(AP.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join DB_T_PROD_CORE.PROD P

   on AP.PROD_ID = P.PROD_ID

   AND CAST(P.EDW_END_DTTM AS DATE )=''12/31/9999''



/*inner join DB_T_PROD_CORE.INDIV_NAME INAM

   on PA.PRTY_ID = INAM.INDIV_PRTY_ID  

   AND  PA.PRTY_AGMT_ROLE_CD IN (''PLCYPRININS'')

   AND CAST(INAM.EDW_END_DTTM AS DATE )=''12/31/9999'' 

*/



inner join (select distinct inam.indiv_prty_id NIN_ID, inam.INDIV_FULL_NAME NIN_NAME 

                    FROM DB_T_PROD_CORE.INDIV_NAME INAM

                   where CAST(INAM.EDW_END_DTTM AS DATE )=''12/31/9999'' 

                                                                               union

                    select onam.prty_id NIN_ID, ORG_NAME NIN_NAME FROM  DB_T_PROD_CORE.ORG_NAME ONAM

                   where CAST(ONAM.EDW_END_DTTM AS DATE )=''12/31/9999'' ) NAME on name.nin_id = PA.PRTY_ID and PA.PRTY_AGMT_ROLE_CD IN (''PLCYPRININS'')





inner join DB_T_PROD_CORE.PLCY_MTRC PM

  on PM.AGMT_ID = A.AGMT_ID

  and PM.INSRNC_MTRC_TYPE_CD = ''PREM''

  AND CAST(PM.EDW_END_DTTM AS DATE )=''12/31/9999'' 





LEFT JOIN LOCATION_ADDRESS_PROPERTY   AS RISK_LOCATION 

                      ON    AA.PRTY_ASSET_ID=RISK_LOCATION.PRTY_ASSET_ID



   

where   

((A.HOST_AGMT_NUM, A.MODL_CRTN_DTTM) IN (SELECT A.HOST_AGMT_NUM, MAX(A.MODL_CRTN_DTTM) FROM DB_T_PROD_CORE.AGMT A

where cast(a.agmt_eff_dttm as date) <=  :CAL_END_DT                   /*  ''2018-11-30''   */
and cast(a.agmt_plnd_expn_dttm as date) >  :CAL_END_DT     /* ''2018-11-30'' */
and cast(a.modl_eff_dttm as date) <=  :CAL_END_DT                   /* ''2018-11-30'' */
and cast(a.trans_strt_dttm as date) <=  :CAL_END_DT                  /*  ''2018-11-30'' */
       group by  A.HOST_AGMT_NUM))



 /*  ((A.HOST_AGMT_NUM, A.MODL_NUM) IN (SELECT A.HOST_AGMT_NUM, MAX(A.MODL_NUM) FROM DB_T_PROD_CORE.AGMT A GROUP BY A.HOST_AGMT_NUM))

        AND

   ((A.HOST_AGMT_NUM, A.TERM_NUM) IN (SELECT A.HOST_AGMT_NUM, MAX(A.TERM_NUM) FROM DB_T_PROD_CORE.AGMT A GROUP BY A.HOST_AGMT_NUM))

*/

        AND f.nk_src_key = ''HOLI_HomeSystemsProtection_alfa''

AND cast(HSP_COV.AGMT_FEAT_STRT_DTTM  as date)  <=  :CAL_END_DT    /* ''2018-11-30'' */
AND cast(HSP_COV.AGMT_FEAT_END_DTTM  as date)    >=  :CAL_END_DT     /* ''2018-11-30'' */
        AND exists (select a2.agmt_id from DB_T_PROD_CORE.AGMT a2 

                 join DB_T_PROD_CORE.agmt_STS as2 on as2.agmt_id = a2.agmt_id 

                          where a.host_agmt_num = a2.host_agmt_num 

                              and a.term_num = a2.term_num 

                              and a2.agmt_type_cd = ''poltrm''  

 		                 and as2.agmt_sts_cd in(''INFORCE'',''RNEWLLAPSD'',''PNDGCNFRMTN'') 

                              and as2.agmt_sts_strt_dttm = (select max (as3.agmt_sts_strt_dttm) from DB_T_PROD_CORE.agmt_STS as3 where as3.agmt_id = a2.agmt_id 

                                                                     and cast  (as3.agmt_sts_strt_dttm as date) <= :CAL_END_DT and as3.agmt_sts_cd <> ''CNFRMDDT''))

																	 

					

	UNION

/* EIM-49185 FOP ADDITION */
	SELECT distinct



''HSB_Company_Product_ID''                                                        as Product_Code

,''HSB_Product_Code''                                                                      as Company_Product_ID

,''HSB_Contract_Number''                                                                as Contract_Number

,A.HOST_AGMT_NUM                                                                    as Policy_Number

,cast(A.AGMT_EFF_DTTM   as date)                                       as Coverage_Effective_Date

,cast(A.AGMT_PLND_EXPN_DTTM  as date)                       as Coverage_Expiration_Date

,NAME.NIN_NAME                                                                           as Name_of_Insured

,RISK_LOCATION.PLCY_MAIL_ADDRESS_1                        as Dwelling_Address

,CITY                                                                   as Dwelling_City

,Risk_LOCATION.GEOGRCL_AREA_SHRT_NAME           as Dwelling_State

,RISK_LOCATION.ZIP                                                                      as Dwelling_Zip_Code



,cast(PRM.PLCY_CVGE_AMT as DECIMAL(6,2))                  as HSP_Net_Premium_Amount

,''9999-12-31''                                                                                         as HSP_Deductible_Amount

,cast(Dwelling.AGMT_ASSET_FEAT_AMT as int)                                 as Coverage_A_Value

,P.HOST_PROD_ID                                                                         as Homeowner_Policy_Form_Number

,P.PROD_NAME                                                                               as HSP_Product_Form_Number

/* ,F.feat_name                                                                                       as HSP_Product_Form_Number */
,''HSB_Client_Product_Name''                                                       as Client_Product_Name

,''Blanks''                                                                                                as  Dwelling_Type

/* ,ASSET_TYPE2.asset_dtl_cd                                                   as  Dwelling_Type */
,''Blanks''                                                                                                as Base_Homeowner_Premium

,round(cast(PM.plcy_amt  as decimal(8,2) ))                                    as Final_Homeowner_Premium

,cast(Dwelling_Deductible.feat_Dtl_val as int)        as Homeowner_Policy_Deductible_Amount

,extract (Year from RS.CNSTRCTN_DT)                                 as Construction_Year

,cast (PASSP.PRTY_ASSET_SPEC_MEAS as int)             as Square_Footage

,''Blanks''                                                                                                as  Number_of_Dwelling_Units_in_Building

/* ,ASSET_TYPE.asset_dtl_cd                                                     as  Number_of_Dwelling_Units_in_Building */
,''Blanks''                                                                                                as Heating_System_Updated_Year

,''Blanks''                                                                                                as Electrical_System_Updated_Year

,''Blanks''                                                                                                as Plumbing_System_Updated_Year

,''Blanks''                                                                                                as Pricing_Tier

,''Blanks''                                                                                                as Insurance_Score

,''Blanks''                                                                                                as Rating_Territory_Code

,''Blanks''                                                                                                as Protection_Class_Code

,cast(FOP_COV.AGMT_ASSET_FEAT_STRT_DTTM  as date)     as HSP_Coverage_Effective_Date

,cast(OTH_STRCT_LIMIT.AGMT_ASSET_FEAT_AMT as int)         as Coverage_B_Value

,cast(PersonalP.AGMT_ASSET_FEAT_AMT as int)                as Coverage_C_Value

,''Blanks''                                                                                                as  Distance_to_Hydrant





from DB_T_PROD_CORE.AGMT A



inner join DB_T_PROD_CORE.agmt_STS           as3

   on A.AGMT_ID = AS3.AGMT_ID 

   AND CAST(AS3.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join DB_T_PROD_CORE.PRTY_AGMT      PA

   on A.AGMT_ID = PA.AGMT_ID

   AND CAST(PA.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join DB_T_PROD_CORE.AGMT_ASSET    AA

   on A.AGMT_ID = AA.AGMT_ID

   AND CAST(AA.EDW_END_DTTM AS DATE )=''12/31/9999''



 inner join DB_T_PROD_CORE.PRTY_ASSET PAS  

   on PAS.PRTY_ASSET_ID = AA.PRTY_ASSET_ID 

and PRTY_ASSET_CLASFCN_CD = ''FOPDWELL'' /* EIM-49185 */
   AND CAST(PAS.EDW_END_DTTM AS DATE )=''12/31/9999'' 



left join DB_T_PROD_CORE.REAL_ESTAT      RS

   on AA.PRTY_ASSET_ID = RS.PRTY_ASSET_ID

   AND CAST(RS.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join (select agmtx.HOST_AGMT_NUM as host_agmt_num, agmtx.TERM_NUM as term_num, agmtx.agmt_type_cd as agmt_type_cd, pcm.feat_id as FEAT_ID, max(cast(PcM.PLCY_cvge_AMT as DECIMAL(10,2))) as PLCY_CVGE_AMT

 			from DB_T_PROD_CORE.AGMT agmtx

 			join DB_T_PROD_CORE.plcy_cvge_mtrc pcm on pcm.agmt_id = agmtx.AGMT_ID

join DB_T_PROD_CORE.FEAT featx on featx.feat_id = pcm.feat_id and  featx.nk_src_key = ''FOPFarmDwelEquiBrkdown''/* EIM-49185 */
                   where pcm.insrnc_mtrc_type_cd = ''TRANPREM''   AND CAST(pcm.EDW_END_DTTM AS DATE )=''12/31/9999''  and cast(pcm.trans_strt_dttm as date) <= :CAL_END_DT    and cast(agmtx.modl_eff_dttm as date) <= :CAL_END_DT  

 			group by agmtx.HOST_AGMT_NUM, agmtx.TERM_NUM, agmtx.agmt_type_cd, pcm.feat_id) prm on prm.host_agmt_num = a.HOST_AGMT_NUM and prm.term_num = a.term_num and prm.agmt_type_cd = ''ppv''  



left join DB_T_PROD_CORE.PRTY_ASSET_SPEC    PASSP

   on  PAS.PRTY_ASSET_ID = PASSP.PRTY_ASSET_ID

   AND  PASSP.PRTY_ASSET_SPEC_TYPE_CD = ''SIZE''

   AND CAST(PASSP.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join DB_T_PROD_CORE.FEAT F

   on F.FEAT_ID = prm.FEAT_ID

   AND CAST(F.EDW_END_DTTM AS DATE )=''12/31/9999''

   

left join   ( 

select  distinct af5.AGMT_ID, af5.FEAT_ID,af5.prty_asset_id,af5.AGMT_ASSET_FEAT_AMT

          FROM DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT af5       

         JOIN DB_T_PROD_CORE.FEAT  F5   ON  F5.FEAT_ID = AF5.FEAT_ID 

         JOIN DB_T_PROD_CORE.AGMT AG ON AG.AGMT_ID=af5.AGMT_ID

          WHERE F5.nk_src_key = ''FOPDwelLim''

		  AND CAST(af5.EDW_END_DTTM AS DATE )=''12/31/9999''

) Dwelling

                                 on a.agmt_id = dwelling.agmt_id and pas.prty_asset_id=Dwelling.prty_asset_id



 left join   ( 

 select  distinct af5.AGMT_ID, af5.FEAT_ID,AF5.prty_asset_id,af5.AGMT_ASSET_FEAT_AMT

          FROM DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT af5       

         JOIN DB_T_PROD_CORE.FEAT  F5   ON  F5.FEAT_ID = AF5.FEAT_ID 

         JOIN DB_T_PROD_CORE.AGMT AG ON AG.AGMT_ID=af5.AGMT_ID

          WHERE F5.nk_src_key = ''FOPHHPersPropLimit''

		  AND CAST(af5.EDW_END_DTTM AS DATE )=''12/31/9999''

 ) PersonalP

                                 on a.agmt_id = personalp.agmt_id and pas.prty_asset_id=PersonalP.prty_asset_id



 left  join   (  

select  distinct af5.AGMT_ID, af5.FEAT_ID,AF5.prty_asset_id,af5.AGMT_ASSET_FEAT_AMT

          FROM DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT af5       

         JOIN DB_T_PROD_CORE.FEAT  F5   ON  F5.FEAT_ID = AF5.FEAT_ID 

         JOIN DB_T_PROD_CORE.AGMT AG ON AG.AGMT_ID=af5.AGMT_ID

          WHERE F5.nk_src_key = ''FOPOthrStructLim''

		  AND CAST(af5.EDW_END_DTTM AS DATE )=''12/31/9999''

 ) OTH_STRCT_LIMIT

                                 on a.agmt_id = oth_strct_limit.agmt_id and pas.prty_asset_id=OTH_STRCT_LIMIT.prty_asset_id



left  join   ( SELECT Distinct HOST_AGMT_NUM,aa.agmt_id,AF.prty_asset_id,F.FEAT_ID,F.feat_Dtl_val

FROM DB_T_PROD_CORE.FEAT F JOIN DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT AF ON F.FEAT_ID=AF.FEAT_ID 

JOIN DB_T_PROD_CORE.AGMT AA ON AF.AGMT_ID=AA.AGMT_ID

join DB_T_PROD_CORE.feat_rltd fr on F.feat_id=fr.rltd_feat_id

join DB_T_PROD_CORE.FEAT f2 on fr.feat_id=f2.feat_id

WHERE f2.nk_src_key =''FOPDed''

AND CAST(af.EDW_END_DTTM AS DATE )=''12/31/9999'') Dwelling_Deductible 

                                 on a.agmt_id = Dwelling_Deductible.agmt_id and pas.prty_asset_id=Dwelling_Deductible.prty_asset_id





inner  join   (select  distinct af5.AGMT_ID, af5.FEAT_ID,af5.prty_asset_id, af5.AGMT_ASSET_FEAT_STRT_DTTM , af5.AGMT_ASSET_FEAT_END_DTTM

          FROM DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT af5       

         JOIN DB_T_PROD_CORE.FEAT  F5   ON  F5.FEAT_ID = AF5.FEAT_ID 

         

         LEFT JOIN DB_T_PROD_CORE.feat_rltd fr on fr.rltd_feat_id = F5.FEAT_ID

                      left join DB_T_PROD_CORE.FEAT f6 on f6.feat_id = fr.feat_id

         

                WHERE F5.nk_src_key = ''FOPFarmDwelEquiBrkdown''

AND CAST(af5.EDW_END_DTTM AS DATE )=''12/31/9999'') FOP_COV /* EIM-49185 */
                                   on a.agmt_id = FOP_COV.agmt_id and pas.prty_asset_id=FOP_COV.prty_asset_id                               

                           



inner join DB_T_PROD_CORE.AGMT_PROD AP

   on A.AGMT_ID = AP.AGMT_ID

   AND CAST(AP.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join DB_T_PROD_CORE.PROD P

   on AP.PROD_ID = P.PROD_ID

   AND CAST(P.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join (select distinct inam.indiv_prty_id NIN_ID, inam.INDIV_FULL_NAME NIN_NAME 

                    FROM DB_T_PROD_CORE.INDIV_NAME INAM

                   where CAST(INAM.EDW_END_DTTM AS DATE )=''12/31/9999'' 

                                                                               union

                    select onam.prty_id NIN_ID, ORG_NAME NIN_NAME FROM  DB_T_PROD_CORE.ORG_NAME ONAM

                   where CAST(ONAM.EDW_END_DTTM AS DATE )=''12/31/9999'' ) NAME on name.nin_id = PA.PRTY_ID and PA.PRTY_AGMT_ROLE_CD IN (''PLCYPRININS'')





inner join DB_T_PROD_CORE.PLCY_MTRC PM

  on PM.AGMT_ID = A.AGMT_ID

  and PM.INSRNC_MTRC_TYPE_CD = ''PREM''

  AND CAST(PM.EDW_END_DTTM AS DATE )=''12/31/9999'' 





LEFT JOIN LOCATION_ADDRESS_PROPERTY   AS RISK_LOCATION 

                      ON    AA.PRTY_ASSET_ID=RISK_LOCATION.PRTY_ASSET_ID



   

where   

((A.HOST_AGMT_NUM, A.MODL_CRTN_DTTM) IN (SELECT A.HOST_AGMT_NUM, MAX(A.MODL_CRTN_DTTM) FROM DB_T_PROD_CORE.AGMT A

where cast(a.agmt_eff_dttm as date) <=  :CAL_END_DT                   /*  ''2018-11-30''   */
and cast(a.agmt_plnd_expn_dttm as date) >  :CAL_END_DT     /* ''2018-11-30'' */
and cast(a.modl_eff_dttm as date) <=  :CAL_END_DT                   /* ''2018-11-30'' */
and cast(a.trans_strt_dttm as date) <=  :CAL_END_DT                  /*  ''2018-11-30'' */
       group by  A.HOST_AGMT_NUM))



AND f.nk_src_key = ''FOPFarmDwelEquiBrkdown''/* EIM-49185 */
AND cast(FOP_COV.AGMT_ASSET_FEAT_STRT_DTTM  as date)  <=  :CAL_END_DT    /* ''2018-11-30'' */
AND cast(FOP_COV.AGMT_ASSET_FEAT_END_DTTM  as date)    >=  :CAL_END_DT     /* ''2018-11-30'' */
        AND exists (select a2.agmt_id from DB_T_PROD_CORE.AGMT a2 

                 join DB_T_PROD_CORE.agmt_STS as2 on as2.agmt_id = a2.agmt_id 

                          where a.host_agmt_num = a2.host_agmt_num 

                              and a.term_num = a2.term_num 

                              and a2.agmt_type_cd = ''poltrm''  

 		                 and as2.agmt_sts_cd in(''INFORCE'',''RNEWLLAPSD'',''PNDGCNFRMTN'') 

                              and as2.agmt_sts_strt_dttm = (select max (as3.agmt_sts_strt_dttm) from DB_T_PROD_CORE.agmt_STS as3 where as3.agmt_id = a2.agmt_id 

                                                                     and cast  (as3.agmt_sts_strt_dttm as date) <= :CAL_END_DT and as3.agmt_sts_cd <> ''CNFRMDDT''))
) SRC
)
);


-- Component exp_pass_to_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt AS
(
SELECT
:CAL_END_DT as var_date,
'' '' as var_spaces,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PRODUCT_CODE */ as out_Product_Code,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PRODUCT_CODE */ as out_Company_Product_ID,
LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CONTRACT_NUMBER */ as out_Contract_Number,
RPAD ( SQ_HSB_Inforce_File.Policy_Number , 20 , '' '' ) as out_Policy_Number,
SQ_HSB_Inforce_File.Coverage_Effective_Date as Coverage_Effective_Date,
SUBSTR ( SQ_HSB_Inforce_File.Coverage_Effective_Date , 1 , 4 ) || SUBSTR ( SQ_HSB_Inforce_File.Coverage_Effective_Date , 6 , 2 ) || SUBSTR ( SQ_HSB_Inforce_File.Coverage_Effective_Date , 9 , 2 ) as out_Coverage_Effective_Date,
to_date ( SQ_HSB_Inforce_File.Coverage_Effective_Date , ''YYYY-MM-DD'' ) as var_Coverage_Effective_Date,
SUBSTR ( SQ_HSB_Inforce_File.Coverage_Expiration_Date , 1 , 4 ) || SUBSTR ( SQ_HSB_Inforce_File.Coverage_Expiration_Date , 6 , 2 ) || SUBSTR ( SQ_HSB_Inforce_File.Coverage_Expiration_Date , 9 , 2 ) as out_Coverage_Expiration_Date,
RPAD ( SQ_HSB_Inforce_File.Name_of_Insured , 55 , '' '' ) as out_Name_of_Insured,
RPAD ( SQ_HSB_Inforce_File.Dwelling_Address , 55 , '' '' ) as out_Dwelling_Address,
RPAD ( SQ_HSB_Inforce_File.Dwelling_City , 20 , '' '' ) as out_Dwelling_City,
SQ_HSB_Inforce_File.Dwelling_State as out_Dwelling_State,
RPAD ( SQ_HSB_Inforce_File.Dwelling_Zip_Code , 10 , '' '' ) as out_Dwelling_Zip_Code,
SQ_HSB_Inforce_File.HSP_Net_Premium_Amount as HSP_Net_Premium_Amount,
IFNULL(TRY_TO_DECIMAL(SQ_HSB_Inforce_File.HSP_Net_Premium_Amount,23,2), 0) as var_HSP_NET_PREMIUM_AMOUNT,
var_HSP_Deductible_Amount,
LPAD ( SQ_HSB_Inforce_File.Coverage_A_Value , 9 , ''0'' ) as out_Coverage_A_Value,
RPAD ( SQ_HSB_Inforce_File.HSP_Product_Form_Number , 20 , '' '' ) as out_Homeowner_Policy_Form_Number,
DECODE ( SQ_HSB_Inforce_File.HSP_Product_Form_Number , ''MH3'' , ''MH27'' , ''HO3'' , ''HO24'' , ''HO5'' , ''HO24'' , ''HO6'' , ''HO25'' , ''HO8'' , ''HO26'' , ''Farmowners'' , ''FO84'' , ''UNKN'' ) as var_HSP_Product_Form_Number,
RPAD ( var_HSP_Product_Form_Number , 20 , '' '' ) as out_HSP_Product_Form_Number,
LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_CLIENT_PRODUCT_NAME */ as var_Client_Product_Name,
RPAD ( var_Client_Product_Name , 20 , '' '' ) as out_Client_Product_Name2,
DECODE ( SQ_HSB_Inforce_File.HSP_Product_Form_Number , ''HO6'' , ''282   '' , ''280   '' ) as var_Dwelling_Type,
var_Dwelling_Type as out_Dwelling_Type,
lpad ( var_spaces , 9 , '' '' ) as out_Base_Homeowner_Premium,
LPAD ( SQ_HSB_Inforce_File.Final_Homeowner_Premium , 9 , ''0'' ) as out_Final_Homeowner_Premium,
LPAD ( SQ_HSB_Inforce_File.Homeowner_Policy_Deductible_Amount , 9 , ''0'' ) as out_Homeowner_Policy_Deductible_Amount,
CASE WHEN length ( ltrim ( Rtrim ( SQ_HSB_Inforce_File.Construction_Year ) ) ) > 0 THEN LPAD ( SQ_HSB_Inforce_File.Construction_Year , 4 , '' '' ) ELSE ''    '' END as out_Construction_Year,
LPAD ( SQ_HSB_Inforce_File.Square_Footage , 15 , ''0'' ) as out_Square_Footage,
''ONERES'' as var_Number_of_Dwelling_Units_in_Building,
DECODE ( var_Number_of_Dwelling_Units_in_Building , ''ONERES'' , ''1     '' , ''TWORES'' , ''2     '' , ''THRFOURRES'' , ''3 or 4'' , ''FIVRES'' , ''5     '' , NULL ) as out_Number_of_Dwelling_Units_in_Building,
lpad ( var_spaces , 4 , '' '' ) as out_Heating_System_Updated_Year,
lpad ( var_spaces , 4 , '' '' ) as out_Electrical_System_Updated_Year,
lpad ( var_spaces , 4 , '' '' ) as out_Plumbing_System_Updated_Year,
lpad ( var_spaces , 20 , '' '' ) as out_Pricing_Tier,
lpad ( var_spaces , 20 , '' '' ) as out_Insurance_Score,
lpad ( var_spaces , 20 , '' '' ) as out_Rating_Territory_Code,
lpad ( var_spaces , 20 , '' '' ) as out_Protection_Class_Code,
SUBSTR ( SQ_HSB_Inforce_File.HSP_Coverage_Effective_Date , 1 , 4 ) || SUBSTR ( SQ_HSB_Inforce_File.HSP_Coverage_Effective_Date , 6 , 2 ) || SUBSTR ( SQ_HSB_Inforce_File.HSP_Coverage_Effective_Date , 9 , 2 ) as out_HSP_Coverage_Effective_Date,
to_date ( SQ_HSB_Inforce_File.HSP_Coverage_Effective_Date , ''YYYY-MM-DD'' ) as out_HSP_CVGE_EFF_DTE,
CASE WHEN length ( ltrim ( Rtrim ( SQ_HSB_Inforce_File.Coverage_B_Value ) ) ) > 0 THEN LPAD ( SQ_HSB_Inforce_File.Coverage_B_Value , 9 , ''0'' ) ELSE ''000000000'' END as out_Coverage_B_Value,
LPAD ( SQ_HSB_Inforce_File.Coverage_C_Value , 9 , ''0'' ) as out_Coverage_C_Value,
lpad ( var_spaces , 9 , '' '' ) as out_Distance_to_Hydrant,
SQ_HSB_Inforce_File.source_record_id,
row_number() over (partition by SQ_HSB_Inforce_File.source_record_id order by SQ_HSB_Inforce_File.source_record_id) as RNK
FROM
SQ_HSB_Inforce_File
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_PRODUCT_CODE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = SQ_HSB_Inforce_File.Company_Product_Code
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_PRODUCT_CODE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = SQ_HSB_Inforce_File.Product_Name
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CONTRACT_NUMBER LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = SQ_HSB_Inforce_File.Contract_Number
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_CLIENT_PRODUCT_NAME LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = SQ_HSB_Inforce_File.Client_Product_Name
QUALIFY RNK = 1
);


-- Component LKP_HSB_PREM_RTS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_HSB_PREM_RTS AS
(
SELECT
LKP.DEDUCT,
LKP.HSB_PCT,
exp_pass_to_tgt.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_to_tgt.source_record_id ORDER BY LKP.DEDUCT asc,LKP.HSB_PCT asc,LKP.ALFA_PCT asc,LKP.EFF_DT asc,LKP.EXPN_DT asc) RNK
FROM
exp_pass_to_tgt
LEFT JOIN (
SELECT HSB_PREM_RTS.DEDUCT     as DEDUCT, 
                   HSB_PREM_RTS.HSB_PCT   as HSB_PCT,
                   HSB_PREM_RTS.ALFA_PCT as ALFA_PCT,
                   HSB_PREM_RTS.EFF_DT       as EFF_DT,
                   HSB_PREM_RTS.EXPN_DT   as EXPN_DT 
      FROM db_t_prod_wrk.HSB_PREM_RTS
WHERE
       EXPN_DT > ''1991-12-31''
) LKP ON LKP.EFF_DT <= exp_pass_to_tgt.out_HSP_CVGE_EFF_DTE AND LKP.EXPN_DT >= exp_pass_to_tgt.out_HSP_CVGE_EFF_DTE
QUALIFY RNK = 1
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
LKP_HSB_PREM_RTS.DEDUCT as DEDUCT,
LKP_HSB_PREM_RTS.HSB_PCT / 100 as var_HSB_PCT,
exp_pass_to_tgt.HSP_Net_Premium_Amount as HSP_Net_Premium_Amount,
exp_pass_to_tgt.HSP_Net_Premium_Amount as var_HSP_Net_Premium_Amount,
IFNULL(TRY_TO_DECIMAL(var_HSP_Net_Premium_Amount * var_HSB_PCT), 0) as var_HSP_Net_Premium_Amount_pct,
lpad ( var_HSP_Net_Premium_Amount_pct , 11 , ''0'' ) as var_test2,
SUBSTR ( var_test2 , 9 , 1 ) as var_test3,
SUBSTR ( var_test2 , 10 , 1 ) as var_test3a,
SUBSTR ( var_test2 , 11 , 1 ) as var_test3b,
CASE WHEN var_test3 = ''.'' or var_test3a = ''.'' or var_test3b = ''.'' THEN ''N'' ELSE ''Y'' END as no_decimal,
CASE WHEN var_test3 = ''.'' THEN var_test2 ELSE CASE WHEN no_decimal = ''Y'' THEN SUBSTR ( var_test2 , 4 , 8 ) || ''.00'' ELSE SUBSTR ( var_test2 , 2 , 10 ) || ''0'' END END as out_HSP_Net_Prem,
exp_pass_to_tgt.source_record_id
FROM
exp_pass_to_tgt
INNER JOIN LKP_HSB_PREM_RTS ON exp_pass_to_tgt.source_record_id = LKP_HSB_PREM_RTS.source_record_id
);


-- Component HSB_Inforce_File, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE HSB_Inforce_File AS
(
SELECT
exp_pass_to_tgt.out_Product_Code as Company_Product_Code,
exp_pass_to_tgt.out_Company_Product_ID as Product_Name,
exp_pass_to_tgt.out_Contract_Number as Contract_Number,
exp_pass_to_tgt.out_Policy_Number as Policy_Number,
exp_pass_to_tgt.out_Coverage_Effective_Date as Coverage_Effective_Date,
exp_pass_to_tgt.out_Coverage_Expiration_Date as Coverage_Expiration_Date,
exp_pass_to_tgt.out_Name_of_Insured as Name_of_Insured,
exp_pass_to_tgt.out_Dwelling_Address as Dwelling_Address,
exp_pass_to_tgt.out_Dwelling_City as Dwelling_City,
exp_pass_to_tgt.out_Dwelling_State as Dwelling_State,
exp_pass_to_tgt.out_Dwelling_Zip_Code as Dwelling_Zip_Code,
EXPTRANS.out_HSP_Net_Prem as HSP_Net_Premium_Amount,
EXPTRANS.DEDUCT as HSP_Deductible_Amount,
exp_pass_to_tgt.out_Coverage_A_Value as Coverage_A_Value,
exp_pass_to_tgt.out_Homeowner_Policy_Form_Number as Homeowner_Policy_Form_Number,
exp_pass_to_tgt.out_HSP_Product_Form_Number as HSP_Product_Form_Number,
exp_pass_to_tgt.out_Client_Product_Name2 as Client_Product_Name,
exp_pass_to_tgt.out_Dwelling_Type as Dwelling_Type,
exp_pass_to_tgt.out_Base_Homeowner_Premium as Base_Homeowner_Premium,
exp_pass_to_tgt.out_Final_Homeowner_Premium as Final_Homeowner_Premium,
exp_pass_to_tgt.out_Homeowner_Policy_Deductible_Amount as Homeowner_Policy_Deductible_Amount,
exp_pass_to_tgt.out_Construction_Year as Construction_Year,
exp_pass_to_tgt.out_Square_Footage as Square_Footage,
exp_pass_to_tgt.out_Number_of_Dwelling_Units_in_Building as Number_of_Dwelling_Units_in_Building,
exp_pass_to_tgt.out_Heating_System_Updated_Year as Heating_System_Updated_Year,
exp_pass_to_tgt.out_Electrical_System_Updated_Year as Electrical_System_Updated_Year,
exp_pass_to_tgt.out_Plumbing_System_Updated_Year as Plumbing_System_Updated_Year,
exp_pass_to_tgt.out_Pricing_Tier as Pricing_Tier,
exp_pass_to_tgt.out_Insurance_Score as Insurance_Score,
exp_pass_to_tgt.out_Rating_Territory_Code as Rating_Territory_Code,
exp_pass_to_tgt.out_Protection_Class_Code as Protection_Class_Code,
exp_pass_to_tgt.out_HSP_Coverage_Effective_Date as HSP_Coverage_Effective_Date,
exp_pass_to_tgt.out_Coverage_B_Value as Coverage_B_Value,
exp_pass_to_tgt.out_Coverage_C_Value as Coverage_C_Value,
exp_pass_to_tgt.out_Distance_to_Hydrant as Distance_to_Hydrant
FROM
exp_pass_to_tgt
INNER JOIN EXPTRANS ON exp_pass_to_tgt.source_record_id = EXPTRANS.source_record_id
);


-- Component HSB_Inforce_File, Type EXPORT_DATA Exporting data
copy into @edw_stage/Parameter/edw_base/hsb_inforce_file.txt
from 
(select *
from hsb_inforce_file)
file_format = ''CSV_FORMAT''
OVERWRITE = TRUE;



-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_HSB_Inforce_File1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_HSB_Inforce_File1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Company_Product_Code,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
WITH LOCATION_ADDRESS_PROPERTY  AS

(

SELECT   PRTY_ASSET_LOCTR.PRTY_ASSET_ID,

                    PRTY_ASSET_LOCTR.LOC_ID,

                    PRTY_ASSET_LOCTR.PRTY_ASSET_LOCTR_ROLE_CD,

                    CITY.GEOGRCL_AREA_NAME   CITY,

                    STREET_ADDR.ADDR_LN_1_TXT    PLCY_MAIL_ADDRESS_1,     

                    PRTY_ASSET_LOCTR.FIRE_DEPT_ID  ,

                    POSTL_CD.POSTL_CD_NUM   ZIP , 

                    TERR.GEOGRCL_AREA_SHRT_NAME 

           FROM DB_T_PROD_CORE.PRTY_ASSET_LOCTR

                 JOIN DB_T_PROD_CORE.STREET_ADDR ON STREET_ADDR.STREET_ADDR_ID=PRTY_ASSET_LOCTR.LOC_ID

                                     AND CAST(STREET_ADDR.EDW_END_DTTM AS DATE )=''12/31/9999''

                  LEFT JOIN DB_T_PROD_CORE.CITY ON CITY.CITY_ID=STREET_ADDR.CITY_ID 

                                     AND CAST(CITY.EDW_END_DTTM AS DATE )=''12/31/9999''

                  LEFT JOIN DB_T_PROD_CORE.POSTL_CD ON POSTL_CD.POSTL_CD_ID=STREET_ADDR.POSTL_CD_ID 

                                     AND CAST(POSTL_CD.EDW_END_DTTM AS DATE )=''12/31/9999''

                  LEFT JOIN DB_T_PROD_CORE.TERR ON TERR.TERR_ID=STREET_ADDR.TERR_ID 

                                     AND CAST(TERR.EDW_END_DTTM AS DATE )=''12/31/9999''

                  WHERE  PRTY_ASSET_LOCTR.PRTY_ASSET_LOCTR_ROLE_CD=''RSKSTADRS''

                                    AND  PRTY_ASSET_LOCTR.TRANS_END_DTTM =to_timestamp(''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'')     

)



SELECT distinct



/* a.agmt_id,  a.modl_num, a.term_num,  aa.prty_asset_id, */
/* as3.agmt_sts_cd, */


''HSB_Company_Product_ID''                                                        as Product_Code

,''HSB_Product_Code''                                                                      as Company_Product_ID

,''HSB_Contract_Number''                                                                as Contract_Number

,A.HOST_AGMT_NUM                                                                    as Policy_Number

,cast(A.AGMT_EFF_DTTM   as date)                                       as Coverage_Effective_Date

,cast(A.AGMT_PLND_EXPN_DTTM  as date)                       as Coverage_Expiration_Date

,NAME.NIN_NAME                                                                           as Name_of_Insured

,RISK_LOCATION.PLCY_MAIL_ADDRESS_1                        as Dwelling_Address

,CITY                                                                   as Dwelling_City

,Risk_LOCATION.GEOGRCL_AREA_SHRT_NAME           as Dwelling_State

,RISK_LOCATION.ZIP                                                                      as Dwelling_Zip_Code



,cast(PRM.PLCY_CVGE_AMT as DECIMAL(6,2))                  as HSP_Net_Premium_Amount

,''9999-12-31''                                                                                         as HSP_Deductible_Amount

,cast(Dwelling.AGMT_FEAT_AMT as int)                                 as Coverage_A_Value

,P.HOST_PROD_ID                                                                         as Homeowner_Policy_Form_Number

,P.PROD_NAME                                                                               as HSP_Product_Form_Number

/* ,F.feat_name                                                                                       as HSP_Product_Form_Number */
,''HSB_Client_Product_Name''                                                       as Client_Product_Name

,''Blanks''                                                                                                as  Dwelling_Type

/* ,ASSET_TYPE2.asset_dtl_cd                                                   as  Dwelling_Type */
,''Blanks''                                                                                                as Base_Homeowner_Premium

,cast(PM.plcy_amt  as decimal(8,2) )                                    as Final_Homeowner_Premium

,cast(Dwelling_Deductible.AGMT_FEAT_AMT as int)        as Homeowner_Policy_Deductible_Amount

,extract (Year from RS.CNSTRCTN_DT)                                 as Construction_Year

,cast (PASSP.PRTY_ASSET_SPEC_MEAS as int)             as Square_Footage

,''Blanks''                                                                                                as  Number_of_Dwelling_Units_in_Building

/* ,ASSET_TYPE.asset_dtl_cd                                                     as  Number_of_Dwelling_Units_in_Building */
,''Blanks''                                                                                                as Heating_System_Updated_Year

,''Blanks''                                                                                                as Electrical_System_Updated_Year

,''Blanks''                                                                                                as Plumbing_System_Updated_Year

,''Blanks''                                                                                                as Pricing_Tier

,''Blanks''                                                                                                as Insurance_Score

,''Blanks''                                                                                                as Rating_Territory_Code

,''Blanks''                                                                                                as Protection_Class_Code

,cast(HSP_COV.AGMT_FEAT_STRT_DTTM  as date)     as HSP_Coverage_Effective_Date

,cast(OTH_STRCT_LIMIT.AGMT_FEAT_AMT as int)         as Coverage_B_Value

,cast(PersonalP.AGMT_FEAT_AMT as int)                             as Coverage_C_Value

,''Blanks''                                                                                                as  Distance_to_Hydrant





from DB_T_PROD_CORE.AGMT A



inner join DB_T_PROD_CORE.agmt_STS           as3

   on A.AGMT_ID = AS3.AGMT_ID 

   AND CAST(AS3.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join DB_T_PROD_CORE.PRTY_AGMT      PA

   on A.AGMT_ID = PA.AGMT_ID

   AND CAST(PA.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join DB_T_PROD_CORE.AGMT_ASSET    AA

   on A.AGMT_ID = AA.AGMT_ID

   AND CAST(AA.EDW_END_DTTM AS DATE )=''12/31/9999''



 inner join DB_T_PROD_CORE.PRTY_ASSET PAS  

   on PAS.PRTY_ASSET_ID = AA.PRTY_ASSET_ID 

  and prty_asset_clasfcn_cd = ''MAIN'' 

   AND CAST(PAS.EDW_END_DTTM AS DATE )=''12/31/9999'' 



left join DB_T_PROD_CORE.REAL_ESTAT      RS

   on AA.PRTY_ASSET_ID = RS.PRTY_ASSET_ID

   AND CAST(RS.EDW_END_DTTM AS DATE )=''12/31/9999''



/*  

inner join DB_T_PROD_CORE.plcy_cvge_mtrc    prm 

   on a.agmt_id = prm.agmt_id

   AND prm.insrnc_mtrc_type_cd = ''TRANPREM''   

   AND CAST(prm.EDW_END_DTTM AS DATE )=''12/31/9999''*/

   

     

 inner join (select agmtx.HOST_AGMT_NUM as host_agmt_num, agmtx.TERM_NUM as term_num, agmtx.agmt_type_cd as agmt_type_cd, pcm.feat_id as FEAT_ID, max(cast(PcM.PLCY_cvge_AMT as DECIMAL(10,2))) as PLCY_CVGE_AMT

 			from DB_T_PROD_CORE.AGMT agmtx

 			join DB_T_PROD_CORE.plcy_cvge_mtrc pcm on pcm.agmt_id = agmtx.AGMT_ID

 			join DB_T_PROD_CORE.FEAT featx on featx.feat_id = pcm.feat_id and  featx.nk_src_key = ''HOLI_HomeSystemsProtection_alfa''

                   where pcm.insrnc_mtrc_type_cd = ''TRANPREM''   AND CAST(pcm.EDW_END_DTTM AS DATE )=''12/31/9999''  and cast(pcm.trans_strt_dttm as date) <= :CAL_END_DT    and cast(agmtx.modl_eff_dttm as date) <= :CAL_END_DT  

 			group by agmtx.HOST_AGMT_NUM, agmtx.TERM_NUM, agmtx.agmt_type_cd, pcm.feat_id) prm on prm.host_agmt_num = a.HOST_AGMT_NUM and prm.term_num = a.term_num and prm.agmt_type_cd = ''ppv''  



left join DB_T_PROD_CORE.PRTY_ASSET_SPEC    PASSP

   on  PAS.PRTY_ASSET_ID = PASSP.PRTY_ASSET_ID

   AND  PASSP.PRTY_ASSET_SPEC_TYPE_CD = ''SIZE''

   AND CAST(PASSP.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join DB_T_PROD_CORE.FEAT F

   on F.FEAT_ID = prm.FEAT_ID

   AND CAST(F.EDW_END_DTTM AS DATE )=''12/31/9999''



left join   ( select  distinct af1.AGMT_ID, af1.FEAT_ID , af1.AGMT_FEAT_AMT 

       FROM DB_T_PROD_CORE.AGMT_FEAT af1       

       JOIN DB_T_PROD_CORE.FEAT  F1   ON  F1.FEAT_ID = AF1.FEAT_ID 

                WHERE F1.nk_src_key = ''HODW_Dwelling_Limit_HOE''  

				AND CAST(af1.EDW_END_DTTM AS DATE )=''12/31/9999'') Dwelling

                                 on a.agmt_id = dwelling.agmt_id



 left join   ( select  distinct af2.AGMT_ID, af2.FEAT_ID , af2.AGMT_FEAT_AMT 

        FROM DB_T_PROD_CORE.AGMT_FEAT af2       

       JOIN DB_T_PROD_CORE.FEAT  F2   ON  F2.FEAT_ID = AF2.FEAT_ID 

                WHERE F2.nk_src_key = ''HODW_PersonalPropertyLimit_alfa''  

				AND CAST(af2.EDW_END_DTTM AS DATE )=''12/31/9999'') PersonalP

                                 on a.agmt_id = personalp.agmt_id



 left  join   ( select  distinct af3.AGMT_ID, af3.FEAT_ID , af3.AGMT_FEAT_AMT 

         FROM DB_T_PROD_CORE.AGMT_FEAT af3       

        JOIN DB_T_PROD_CORE.FEAT  F3   ON  F3.FEAT_ID = AF3.FEAT_ID 

                WHERE F3.nk_src_key = ''HODW_OTHERSTRUCTURESLIMIT_ALFA'' 

				AND CAST(af3.EDW_END_DTTM AS DATE )=''12/31/9999'') OTH_STRCT_LIMIT

                                 on a.agmt_id = oth_strct_limit.agmt_id



left  join   ( select  distinct af4.AGMT_ID, af4.FEAT_ID , af4.AGMT_FEAT_AMT 

          FROM DB_T_PROD_CORE.AGMT_FEAT af4       

         JOIN DB_T_PROD_CORE.FEAT  F4   ON  F4.FEAT_ID = AF4.FEAT_ID 

                WHERE F4.nk_src_key = ''HODW_OtherPerilsDedValue_alfa''  

				AND CAST(af4.EDW_END_DTTM AS DATE )=''12/31/9999'') Dwelling_Deductible 

                                 on a.agmt_id = Dwelling_Deductible.agmt_id

 

inner  join   ( select  distinct af5.AGMT_ID, af5.FEAT_ID , af5.AGMT_FEAT_AMT , af5.AGMT_FEAT_STRT_DTTM , af5.AGMT_FEAT_END_DTTM

          FROM DB_T_PROD_CORE.AGMT_FEAT af5       

         JOIN DB_T_PROD_CORE.FEAT  F5   ON  F5.FEAT_ID = AF5.FEAT_ID 

         

         LEFT JOIN DB_T_PROD_CORE.feat_rltd fr on fr.rltd_feat_id = F5.FEAT_ID

                      left join DB_T_PROD_CORE.FEAT f6 on f6.feat_id = fr.feat_id

         

                WHERE F6.nk_src_key = ''HOLI_HomeSystemsProtection_alfa''

				AND CAST(af5.EDW_END_DTTM AS DATE )=''12/31/9999'') HSP_COV 

                                   on a.agmt_id = HSP_COV.agmt_id                               

                           



inner join DB_T_PROD_CORE.AGMT_PROD AP

   on A.AGMT_ID = AP.AGMT_ID

   AND CAST(AP.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join DB_T_PROD_CORE.PROD P

   on AP.PROD_ID = P.PROD_ID

   AND CAST(P.EDW_END_DTTM AS DATE )=''12/31/9999''



/*inner join DB_T_PROD_CORE.INDIV_NAME INAM

   on PA.PRTY_ID = INAM.INDIV_PRTY_ID  

   AND  PA.PRTY_AGMT_ROLE_CD IN (''PLCYPRININS'')

   AND CAST(INAM.EDW_END_DTTM AS DATE )=''12/31/9999'' 

*/



inner join (select distinct inam.indiv_prty_id NIN_ID, inam.INDIV_FULL_NAME NIN_NAME 

                    FROM DB_T_PROD_CORE.INDIV_NAME INAM

                   where CAST(INAM.EDW_END_DTTM AS DATE )=''12/31/9999'' 

                                                                               union

                    select onam.prty_id NIN_ID, ORG_NAME NIN_NAME FROM  DB_T_PROD_CORE.ORG_NAME ONAM

                   where CAST(ONAM.EDW_END_DTTM AS DATE )=''12/31/9999'' ) NAME on name.nin_id = PA.PRTY_ID and PA.PRTY_AGMT_ROLE_CD IN (''PLCYPRININS'')





inner join DB_T_PROD_CORE.PLCY_MTRC PM

  on PM.AGMT_ID = A.AGMT_ID

  and PM.INSRNC_MTRC_TYPE_CD = ''PREM''

  AND CAST(PM.EDW_END_DTTM AS DATE )=''12/31/9999'' 





LEFT JOIN LOCATION_ADDRESS_PROPERTY   AS RISK_LOCATION 

                      ON    AA.PRTY_ASSET_ID=RISK_LOCATION.PRTY_ASSET_ID



   

where   

((A.HOST_AGMT_NUM, A.MODL_CRTN_DTTM) IN (SELECT A.HOST_AGMT_NUM, MAX(A.MODL_CRTN_DTTM) FROM DB_T_PROD_CORE.AGMT A

where cast(a.agmt_eff_dttm as date) <=  :CAL_END_DT                   /*  ''2018-11-30''   */
and cast(a.agmt_plnd_expn_dttm as date) >  :CAL_END_DT     /* ''2018-11-30'' */
and cast(a.modl_eff_dttm as date) <=  :CAL_END_DT                   /* ''2018-11-30'' */
and cast(a.trans_strt_dttm as date) <=  :CAL_END_DT                  /*  ''2018-11-30'' */
       group by  A.HOST_AGMT_NUM))



 /*  ((A.HOST_AGMT_NUM, A.MODL_NUM) IN (SELECT A.HOST_AGMT_NUM, MAX(A.MODL_NUM) FROM DB_T_PROD_CORE.AGMT A GROUP BY A.HOST_AGMT_NUM))

        AND

   ((A.HOST_AGMT_NUM, A.TERM_NUM) IN (SELECT A.HOST_AGMT_NUM, MAX(A.TERM_NUM) FROM DB_T_PROD_CORE.AGMT A GROUP BY A.HOST_AGMT_NUM))

*/

        AND f.nk_src_key = ''HOLI_HomeSystemsProtection_alfa''

AND cast(HSP_COV.AGMT_FEAT_STRT_DTTM  as date)  <=  :CAL_END_DT    /* ''2018-11-30'' */
AND cast(HSP_COV.AGMT_FEAT_END_DTTM  as date)    >=  :CAL_END_DT     /* ''2018-11-30'' */
        AND exists (select a2.agmt_id from DB_T_PROD_CORE.AGMT a2 

                 join DB_T_PROD_CORE.agmt_STS as2 on as2.agmt_id = a2.agmt_id 

                          where a.host_agmt_num = a2.host_agmt_num 

                              and a.term_num = a2.term_num 

                              and a2.agmt_type_cd = ''poltrm''  

 		                 and as2.agmt_sts_cd in(''INFORCE'',''RNEWLLAPSD'',''PNDGCNFRMTN'') 

                              and as2.agmt_sts_strt_dttm = (select max (as3.agmt_sts_strt_dttm) from DB_T_PROD_CORE.agmt_STS as3 where as3.agmt_id = a2.agmt_id 

                                                                     and cast  (as3.agmt_sts_strt_dttm as date) <= :CAL_END_DT and as3.agmt_sts_cd <> ''CNFRMDDT''))

																	 

					

	UNION

/* EIM-49185 FOP ADDITION */
	SELECT distinct



''HSB_Company_Product_ID''                                                        as Product_Code

,''HSB_Product_Code''                                                                      as Company_Product_ID

,''HSB_Contract_Number''                                                                as Contract_Number

,A.HOST_AGMT_NUM                                                                    as Policy_Number

,cast(A.AGMT_EFF_DTTM   as date)                                       as Coverage_Effective_Date

,cast(A.AGMT_PLND_EXPN_DTTM  as date)                       as Coverage_Expiration_Date

,NAME.NIN_NAME                                                                           as Name_of_Insured

,RISK_LOCATION.PLCY_MAIL_ADDRESS_1                        as Dwelling_Address

,CITY                                                                   as Dwelling_City

,Risk_LOCATION.GEOGRCL_AREA_SHRT_NAME           as Dwelling_State

,RISK_LOCATION.ZIP                                                                      as Dwelling_Zip_Code



,cast(PRM.PLCY_CVGE_AMT as DECIMAL(6,2))                  as HSP_Net_Premium_Amount

,''9999-12-31''                                                                                         as HSP_Deductible_Amount

,cast(Dwelling.AGMT_ASSET_FEAT_AMT as int)                                 as Coverage_A_Value

,P.HOST_PROD_ID                                                                         as Homeowner_Policy_Form_Number

,P.PROD_NAME                                                                               as HSP_Product_Form_Number

/* ,F.feat_name                                                                                       as HSP_Product_Form_Number */
,''HSB_Client_Product_Name''                                                       as Client_Product_Name

,''Blanks''                                                                                                as  Dwelling_Type

/* ,ASSET_TYPE2.asset_dtl_cd                                                   as  Dwelling_Type */
,''Blanks''                                                                                                as Base_Homeowner_Premium

,round(cast(PM.plcy_amt  as decimal(8,2) ))                                    as Final_Homeowner_Premium

,cast(Dwelling_Deductible.feat_Dtl_val as int)        as Homeowner_Policy_Deductible_Amount

,extract (Year from RS.CNSTRCTN_DT)                                 as Construction_Year

,cast (PASSP.PRTY_ASSET_SPEC_MEAS as int)             as Square_Footage

,''Blanks''                                                                                                as  Number_of_Dwelling_Units_in_Building

/* ,ASSET_TYPE.asset_dtl_cd                                                     as  Number_of_Dwelling_Units_in_Building */
,''Blanks''                                                                                                as Heating_System_Updated_Year

,''Blanks''                                                                                                as Electrical_System_Updated_Year

,''Blanks''                                                                                                as Plumbing_System_Updated_Year

,''Blanks''                                                                                                as Pricing_Tier

,''Blanks''                                                                                                as Insurance_Score

,''Blanks''                                                                                                as Rating_Territory_Code

,''Blanks''                                                                                                as Protection_Class_Code

,cast(FOP_COV.AGMT_ASSET_FEAT_STRT_DTTM  as date)     as HSP_Coverage_Effective_Date

,cast(OTH_STRCT_LIMIT.AGMT_ASSET_FEAT_AMT as int)         as Coverage_B_Value

,cast(PersonalP.AGMT_ASSET_FEAT_AMT as int)                as Coverage_C_Value

,''Blanks''                                                                                                as  Distance_to_Hydrant





from DB_T_PROD_CORE.AGMT A



inner join DB_T_PROD_CORE.agmt_STS           as3

   on A.AGMT_ID = AS3.AGMT_ID 

   AND CAST(AS3.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join DB_T_PROD_CORE.PRTY_AGMT      PA

   on A.AGMT_ID = PA.AGMT_ID

   AND CAST(PA.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join DB_T_PROD_CORE.AGMT_ASSET    AA

   on A.AGMT_ID = AA.AGMT_ID

   AND CAST(AA.EDW_END_DTTM AS DATE )=''12/31/9999''



 inner join DB_T_PROD_CORE.PRTY_ASSET PAS  

   on PAS.PRTY_ASSET_ID = AA.PRTY_ASSET_ID 

and PRTY_ASSET_CLASFCN_CD = ''FOPDWELL'' /* EIM-49185 */
   AND CAST(PAS.EDW_END_DTTM AS DATE )=''12/31/9999'' 



left join DB_T_PROD_CORE.REAL_ESTAT      RS

   on AA.PRTY_ASSET_ID = RS.PRTY_ASSET_ID

   AND CAST(RS.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join (select agmtx.HOST_AGMT_NUM as host_agmt_num, agmtx.TERM_NUM as term_num, agmtx.agmt_type_cd as agmt_type_cd, pcm.feat_id as FEAT_ID, max(cast(PcM.PLCY_cvge_AMT as DECIMAL(10,2))) as PLCY_CVGE_AMT

 			from DB_T_PROD_CORE.AGMT agmtx

 			join DB_T_PROD_CORE.plcy_cvge_mtrc pcm on pcm.agmt_id = agmtx.AGMT_ID

join DB_T_PROD_CORE.FEAT featx on featx.feat_id = pcm.feat_id and  featx.nk_src_key = ''FOPFarmDwelEquiBrkdown''/* EIM-49185 */
                   where pcm.insrnc_mtrc_type_cd = ''TRANPREM''   AND CAST(pcm.EDW_END_DTTM AS DATE )=''12/31/9999''  and cast(pcm.trans_strt_dttm as date) <= :CAL_END_DT    and cast(agmtx.modl_eff_dttm as date) <= :CAL_END_DT  

 			group by agmtx.HOST_AGMT_NUM, agmtx.TERM_NUM, agmtx.agmt_type_cd, pcm.feat_id) prm on prm.host_agmt_num = a.HOST_AGMT_NUM and prm.term_num = a.term_num and prm.agmt_type_cd = ''ppv''  



left join DB_T_PROD_CORE.PRTY_ASSET_SPEC    PASSP

   on  PAS.PRTY_ASSET_ID = PASSP.PRTY_ASSET_ID

   AND  PASSP.PRTY_ASSET_SPEC_TYPE_CD = ''SIZE''

   AND CAST(PASSP.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join DB_T_PROD_CORE.FEAT F

   on F.FEAT_ID = prm.FEAT_ID

   AND CAST(F.EDW_END_DTTM AS DATE )=''12/31/9999''

   

left join   ( 

select  distinct af5.AGMT_ID, af5.FEAT_ID,af5.prty_asset_id,af5.AGMT_ASSET_FEAT_AMT

          FROM DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT af5       

         JOIN DB_T_PROD_CORE.FEAT  F5   ON  F5.FEAT_ID = AF5.FEAT_ID 

         JOIN DB_T_PROD_CORE.AGMT AG ON AG.AGMT_ID=af5.AGMT_ID

          WHERE F5.nk_src_key = ''FOPDwelLim''

		  AND CAST(af5.EDW_END_DTTM AS DATE )=''12/31/9999''

) Dwelling

                                 on a.agmt_id = dwelling.agmt_id and pas.prty_asset_id=Dwelling.prty_asset_id



 left join   ( 

 select  distinct af5.AGMT_ID, af5.FEAT_ID,AF5.prty_asset_id,af5.AGMT_ASSET_FEAT_AMT

          FROM DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT af5       

         JOIN DB_T_PROD_CORE.FEAT  F5   ON  F5.FEAT_ID = AF5.FEAT_ID 

         JOIN DB_T_PROD_CORE.AGMT AG ON AG.AGMT_ID=af5.AGMT_ID

          WHERE F5.nk_src_key = ''FOPHHPersPropLimit''

		  AND CAST(af5.EDW_END_DTTM AS DATE )=''12/31/9999''

 ) PersonalP

                                 on a.agmt_id = personalp.agmt_id and pas.prty_asset_id=PersonalP.prty_asset_id



 left  join   (  

select  distinct af5.AGMT_ID, af5.FEAT_ID,AF5.prty_asset_id,af5.AGMT_ASSET_FEAT_AMT

          FROM DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT af5       

         JOIN DB_T_PROD_CORE.FEAT  F5   ON  F5.FEAT_ID = AF5.FEAT_ID 

         JOIN DB_T_PROD_CORE.AGMT AG ON AG.AGMT_ID=af5.AGMT_ID

          WHERE F5.nk_src_key = ''FOPOthrStructLim''

		  AND CAST(af5.EDW_END_DTTM AS DATE )=''12/31/9999''

 ) OTH_STRCT_LIMIT

                                 on a.agmt_id = oth_strct_limit.agmt_id and pas.prty_asset_id=OTH_STRCT_LIMIT.prty_asset_id



left  join   ( SELECT Distinct HOST_AGMT_NUM,aa.agmt_id,AF.prty_asset_id,F.FEAT_ID,F.feat_Dtl_val

FROM DB_T_PROD_CORE.FEAT F JOIN DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT AF ON F.FEAT_ID=AF.FEAT_ID 

JOIN DB_T_PROD_CORE.AGMT AA ON AF.AGMT_ID=AA.AGMT_ID

join DB_T_PROD_CORE.feat_rltd fr on F.feat_id=fr.rltd_feat_id

join DB_T_PROD_CORE.FEAT f2 on fr.feat_id=f2.feat_id

WHERE f2.nk_src_key =''FOPDed''

AND CAST(af.EDW_END_DTTM AS DATE )=''12/31/9999'') Dwelling_Deductible 

                                 on a.agmt_id = Dwelling_Deductible.agmt_id and pas.prty_asset_id=Dwelling_Deductible.prty_asset_id





inner  join   (select  distinct af5.AGMT_ID, af5.FEAT_ID,af5.prty_asset_id, af5.AGMT_ASSET_FEAT_STRT_DTTM , af5.AGMT_ASSET_FEAT_END_DTTM

          FROM DB_T_PROD_CORE.AGMT_INSRD_ASSET_FEAT af5       

         JOIN DB_T_PROD_CORE.FEAT  F5   ON  F5.FEAT_ID = AF5.FEAT_ID 

         

         LEFT JOIN DB_T_PROD_CORE.feat_rltd fr on fr.rltd_feat_id = F5.FEAT_ID

                      left join DB_T_PROD_CORE.FEAT f6 on f6.feat_id = fr.feat_id

         

                WHERE F5.nk_src_key = ''FOPFarmDwelEquiBrkdown''

AND CAST(af5.EDW_END_DTTM AS DATE )=''12/31/9999'') FOP_COV /* EIM-49185 */
                                   on a.agmt_id = FOP_COV.agmt_id and pas.prty_asset_id=FOP_COV.prty_asset_id                               

                           



inner join DB_T_PROD_CORE.AGMT_PROD AP

   on A.AGMT_ID = AP.AGMT_ID

   AND CAST(AP.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join DB_T_PROD_CORE.PROD P

   on AP.PROD_ID = P.PROD_ID

   AND CAST(P.EDW_END_DTTM AS DATE )=''12/31/9999''



inner join (select distinct inam.indiv_prty_id NIN_ID, inam.INDIV_FULL_NAME NIN_NAME 

                    FROM DB_T_PROD_CORE.INDIV_NAME INAM

                   where CAST(INAM.EDW_END_DTTM AS DATE )=''12/31/9999'' 

                                                                               union

                    select onam.prty_id NIN_ID, ORG_NAME NIN_NAME FROM  DB_T_PROD_CORE.ORG_NAME ONAM

                   where CAST(ONAM.EDW_END_DTTM AS DATE )=''12/31/9999'' ) NAME on name.nin_id = PA.PRTY_ID and PA.PRTY_AGMT_ROLE_CD IN (''PLCYPRININS'')





inner join DB_T_PROD_CORE.PLCY_MTRC PM

  on PM.AGMT_ID = A.AGMT_ID

  and PM.INSRNC_MTRC_TYPE_CD = ''PREM''

  AND CAST(PM.EDW_END_DTTM AS DATE )=''12/31/9999'' 





LEFT JOIN LOCATION_ADDRESS_PROPERTY   AS RISK_LOCATION 

                      ON    AA.PRTY_ASSET_ID=RISK_LOCATION.PRTY_ASSET_ID



   

where   

((A.HOST_AGMT_NUM, A.MODL_CRTN_DTTM) IN (SELECT A.HOST_AGMT_NUM, MAX(A.MODL_CRTN_DTTM) FROM DB_T_PROD_CORE.AGMT A

where cast(a.agmt_eff_dttm as date) <=  :CAL_END_DT                   /*  ''2018-11-30''   */
and cast(a.agmt_plnd_expn_dttm as date) >  :CAL_END_DT     /* ''2018-11-30'' */
and cast(a.modl_eff_dttm as date) <=  :CAL_END_DT                   /* ''2018-11-30'' */
and cast(a.trans_strt_dttm as date) <=  :CAL_END_DT                  /*  ''2018-11-30'' */
       group by  A.HOST_AGMT_NUM))



AND f.nk_src_key = ''FOPFarmDwelEquiBrkdown''/* EIM-49185 */
AND cast(FOP_COV.AGMT_ASSET_FEAT_STRT_DTTM  as date)  <=  :CAL_END_DT    /* ''2018-11-30'' */
AND cast(FOP_COV.AGMT_ASSET_FEAT_END_DTTM  as date)    >=  :CAL_END_DT     /* ''2018-11-30'' */
        AND exists (select a2.agmt_id from DB_T_PROD_CORE.AGMT a2 

                 join DB_T_PROD_CORE.agmt_STS as2 on as2.agmt_id = a2.agmt_id 

                          where a.host_agmt_num = a2.host_agmt_num 

                              and a.term_num = a2.term_num 

                              and a2.agmt_type_cd = ''poltrm''  

 		                 and as2.agmt_sts_cd in(''INFORCE'',''RNEWLLAPSD'',''PNDGCNFRMTN'') 

                              and as2.agmt_sts_strt_dttm = (select max (as3.agmt_sts_strt_dttm) from DB_T_PROD_CORE.agmt_STS as3 where as3.agmt_id = a2.agmt_id 

                                                                     and cast  (as3.agmt_sts_strt_dttm as date) <= :CAL_END_DT and as3.agmt_sts_cd <> ''CNFRMDDT''))
) SRC
)
);


-- Component AGG_CNT_RECORDS, Type AGGREGATOR 
CREATE OR REPLACE TEMPORARY TABLE AGG_CNT_RECORDS AS
(
SELECT
MIN(SQ_HSB_Inforce_File1.Company_Product_Code) as INPUT,
count(INPUT) as COUNTVAL,
MIN(SQ_HSB_Inforce_File1.source_record_id) as source_record_id
FROM
SQ_HSB_Inforce_File1
);


-- Component EXP_TRAILER, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_TRAILER AS
(
SELECT
''4522'' as Company_Product_Code,
''HSP'' as Product_Name,
''CONTROL'' as Record_Type,
AGG_CNT_RECORDS.COUNTVAL as Number_Of_Records,
:CAL_END_DT as Inforce_Date1,
SUBSTR ( Inforce_Date1 , 1 , 4 ) || SUBSTR ( Inforce_Date1 , 6 , 2 ) || SUBSTR ( Inforce_Date1 , 9 , 2 ) as out_Inforce_Date,
''02.02'' as Version_Number,
AGG_CNT_RECORDS.source_record_id
FROM
AGG_CNT_RECORDS
);


-- Component HSB_Inforce_Trailer, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE HSB_Inforce_Trailer AS
(
SELECT
EXP_TRAILER.Company_Product_Code as C1_Company_Product_Code,
EXP_TRAILER.Product_Name as C1_Product_Name,
EXP_TRAILER.Record_Type as C1_Record_Type,
EXP_TRAILER.Number_Of_Records as C1_Number_Of_Records,
EXP_TRAILER.out_Inforce_Date as C1_Inforce_Date,
EXP_TRAILER.Version_Number as C1_Version_Number
FROM
EXP_TRAILER
);


-- Component HSB_Inforce_Trailer, Type EXPORT_DATA Exporting data
copy into @edw_stage/Parameter/edw_base/hsb_inforce_trailer.txt
from 
(select *
from hsb_inforce_trailer)
file_format = ''CSV_FORMAT''
OVERWRITE = TRUE;



-- PIPELINE END FOR 2

END; ';