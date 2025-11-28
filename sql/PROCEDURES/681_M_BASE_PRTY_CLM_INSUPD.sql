-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_CLM_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
  END_DTTM timestamp;
  START_DTTM timestamp;
  fs_date date;
  run_id STRING;
  prcs_id int;

BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
FS_DATE :=   (SELECT left(param_value,10) FROM control_params where run_id = :run_id and upper(param_name)=''FS_DATE'' order by insert_ts desc limit 1);


-- PIPELINE START FOR 1

-- Component SQ_prty_clm_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_prty_clm_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as SQ_CLM_ID,
$2 as SQ_PRTY_CLM_ROLE_CD,
$3 as SQ_out_AssignmentDate,
$4 as SQ_PRTY_ID,
$5 as SQ_PRTY_CLM_END_DTTM,
$6 as SQ_PRTY_CNTCT_PROHIBITED_IND,
$7 as SQ_EMPLE_IND,
$8 as TGT_CLM_ID,
$9 as TGT_PRTY_CLM_ROLE_CD,
$10 as TGT_PRTY_CLM_STRT_DTTM,
$11 as TGT_PRTY_ID,
$12 as TGT_EDW_STRT_DTTM,
$13 as SOURCE_DATA,
$14 as TARGET_DATA,
$15 as INS_UPD_FLAG,
$16 as TGT_PRTY_CNTCT_PROHIBITED_IND,
$17 as TGT_EMPLE_IND,
$18 as SRC_TRANS_STRT_DTTM,
$19 as TGT_TRANS_STRT_DTTM,
$20 as TGT_PRTY_CLM_END_DTTM,
$21 as TGT_EDW_END_DTTM,
$22 as SQ_ENDDATE,
$23 as SQ_RETIRED,
$24 as SQ_UNLISTD_OPRTR_IND,
$25 as SQ_RLTNSHP_TO_INSRD_TXT,
$26 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 

SQ.in_CLM_ID AS SQ_CLM_ID,

SQ.in_PRTY_CLM_ROLE_CD AS SQ_PRTY_CLM_ROLE_CD ,

SQ.out_AssignmentDate AS SQ_out_AssignmentDate,

SQ.in_PRTY_ID AS SQ_PRTY_ID,

SQ.in_PRTY_CLM_END_DTTM AS SQ_PRTY_CLM_END_DTTM ,

SQ.in_PRTY_CNTCT_PROHIBITED_IND AS SQ_PRTY_CNTCT_PROHIBITED_IND ,

SQ.in_EMPLE_IND AS SQ_EMPLE_IND,

LKP_TGT.CLM_ID AS TGT_CLM_ID,

LKP_TGT.PRTY_CLM_ROLE_CD AS TGT_PRTY_CLM_ROLE_CD,

LKP_TGT.PRTY_CLM_STRT_DTTM AS TGT_PRTY_CLM_STRT_DTTM,

LKP_TGT.PRTY_ID AS TGT_PRTY_ID,

LKP_TGT.EDW_STRT_DTTM AS TGT_EDW_STRT_DTTM,

/* ----INS_UPD FLAG-------------- */
/*SOURCE DATA*/                                 
CAST(
  COALESCE(TRIM(CAST(SQ_PRTY_ID AS VARCHAR)), '''') AS VARCHAR
) AS SOURCE_DATA,
CAST(
  COALESCE(TRIM(CAST(TGT_PRTY_ID AS VARCHAR)), '''') AS VARCHAR
) AS TARGET_DATA,
/*FLAG*/

CASE                                    

WHEN LENGTH(TARGET_DATA) =0  THEN ''I''                                  

WHEN TRIM(SOURCE_DATA) =TRIM( TARGET_DATA) THEN ''R''                                    

WHEN TRIM(TARGET_DATA) <> TRIM(SOURCE_DATA) THEN ''U'' END AS INS_UPD_FLAG, 
LKP_TGT.PRTY_CNTCT_PROHIBITED_IND AS TGT_PRTY_CNTCT_PROHIBITED_IND,
--COALESCE(TRIM(CAST(TGT_PRTY_CNTCT_PROHIBITED_IND as varchar(100))),'''') TGT_PRTY_CNTCT_PROHIBITED_IND, 
LKP_TGT.EMPLE_IND AS TGT_EMPLE_IND,
--COALESCE(TRIM(CAST(TGT_EMPLE_IND as varchar(100))),'''') TGT_EMPLE_IND,     
SQ.in_TRANS_STRT_DTTM AS SRC_TRANS_STRT_DTTM,
LKP_TGT.PRTY_CLM_END_DTTM AS TGT_PRTY_CLM_END_DTTM,
--LKP_TGT.TRANS_STRT_DTTM AS TGT_TRANS_STRT_DTTM,
COALESCE(TRIM(CAST(TO_CHAR(TGT_PRTY_CLM_END_DTTM) as varchar(100))),'''') TGT_PRTY_CLM_END_DTTM,   

LKP_TGT.EDW_END_DTTM AS TGT_EDW_END_DTTM,
SQ.ENDDATE AS SQ_ENDDATE,

SQ.RETIRED AS SQ_RETIRED,
CASE
  WHEN SQ.in_PRTY_CLM_ROLE_CD IN (''CVDPTY'', ''INSURD'') THEN ''1''
  ELSE ''0''
END AS SQ_UNLISTD_OPRTR_IND,
SQ.in_RLTNSHP_TO_INSRD_TXT AS SQ_RLTNSHP_TO_INSRD_TXT
--CAST(
 -- COALESCE(TRIM(TO_CHAR (SQ_out_AssignmentDate)), '''') AS ----VARCHAR
--) SQ_out_AssignmentDate,

--COALESCE(TRIM(CAST(TO_CHAR(SQ_PRTY_CLM_END_DTTM) as varchar(100))),'''')  SQ_PRTY_CLM_END_DTTM,                               

--COALESCE(TRIM(CAST(SQ_PRTY_CNTCT_PROHIBITED_IND as varchar(100))),'''') SQ_PRTY_CNTCT_PROHIBITED_IND,                                  

--COALESCE(TRIM(CAST(SQ_EMPLE_IND as varchar(100))),'''') SQ_EMPLE_IND,      



--COALESCE(TRIM(CAST(SQ_RLTNSHP_TO_INSRD_TXT as varchar(100))),'''') SQ_RLTNSHP_TO_INSRD_TXT,   



/*TARGET DATA*/                                 

--CAST(COALESCE(TRIM(TO_CHAR (TGT_PRTY_CLM_STRT_DTTM)), '''') AS VARCHAR) TGT_PRTY_CLM_STRT_DTTM,

                            


   

--COALESCE(TRIM(CAST(LKP_TGT.UNLISTD_OPRTR_IND as varchar(100))),'''') UNLISTD_OPRTR_IND, 

--COALESCE(TRIM(CAST(LKP_TGT.RLTNSHP_TO_INSRD_TXT as varchar(100))),'''') RLTNSHP_TO_INSRD_TXT,  


    



--


--,





--CASE
  --WHEN SQ_PRTY_CLM_ROLE_CD IN (''CVDPTY'', ''INSURD'') THEN ''1''
  --ELSE ''0''
--END AS SQ_UNLISTD_OPRTR_IND,





FROM((

SELECT LTRIM(RTRIM(src.typecode)) AS var_ContactroleTypecode,

CASE WHEN (var_ContactroleTypecode NOT LIKE ''%_%'' OR var_ContactroleTypecode IS NULL OR LENGTH(var_ContactroleTypecode)=0)

THEN ''UNK'' ELSE LKP_XLAT.TGT_IDNTFTN_VAL END var_Contactrole_Typecode2,

CASE WHEN var_Contactrole_Typecode2 IS NULL THEN ''UNK'' ELSE var_Contactrole_Typecode2 END AS out_ContactroleTypecode,

CASE WHEN SRC.createtime_stg IS NULL THEN CAST(CAST(''01-01-1900'' AS DATE ) as timestamp) ELSE SRC.createtime_stg END AS out_AssignmentDate,

LKP_CLM.CLM_ID AS in_CLM_ID,

out_ContactroleTypecode AS in_PRTY_CLM_ROLE_CD,

CASE WHEN UPPER(src.typecode)=''ASSIGNED GROUP'' THEN LKP_INTRNL_ORG.INTRNL_ORG_PRTY_ID 

WHEN LKP_INDIV.NK_PUBLC_ID IS NOT NULL THEN LKP_INDIV.INDIV_PRTY_ID

WHEN LKP_BUSN.NK_BUSN_CD IS NOT NULL THEN LKP_BUSN.BUSN_PRTY_ID ELSE NULL END AS in_PRTY_ID,

CASE WHEN SRC.closedate_stg IS NULL THEN CAST(CAST(''1900-01-01'' AS DATE ) as timestamp) ELSE SRC.closedate_stg END AS in_PRTY_CLM_END_DTTM,

contactprohibited_stg AS in_PRTY_CNTCT_PROHIBITED_IND,

EMPLE_IND_stg AS in_EMPLE_IND,

SRC.UpdateTime AS in_TRANS_STRT_DTTM,

CASE WHEN SRC.closedate_stg IS NULL THEN CAST(CAST(''01-01-1900'' AS DATE ) as timestamp) ELSE SRC.closedate_stg END AS ENDDATE, 

SRC.RETIRED_STG AS RETIRED,

CASE WHEN UPPER(src.typecode)=''ASSIGNED GROUP'' THEN NULL ELSE RLTNSHP_TO_INSRD_TXT_stg END AS in_RLTNSHP_TO_INSRD_TXT FROM 

(/* ---------------------------------------SOURCE QUERY STARTS----------------------------------------- */
select * from (

select distinct 

    cc_claim.ClaimNumber_stg AS ClaimNumber_stg , 

    cctl_contactrole.TYPECODE_stg as typecode,

    cc_claimcontact.createtime_stg as createtime_stg,

    cast(cc_contact.PublicID_stg as varchar(64)) as PublicID_stg,

    cc_claimcontact.contactprohibited_stg as contactprohibited_stg,

    cast(CASE WHEN cctl_claimsecuritytype.TYPECODE_stg =  ''employeeclaim'' then 1

    ELSE 0 END AS VARCHAR(3)) as EMPLE_IND_stg,

    cast(CASE WHEN cctl_contactrole.TYPECODE_stg =  ''reporter'' then cctl_personrelationtype.TYPECODE_stg

    ELSE NULL END AS VARCHAR(50))AS RLTNSHP_TO_INSRD_TXT_stg,

    ''SRC_SYS6'' AS SRC_CD_stg,

    cc_claimcontactrole.UpdateTime_stg as updatetime, 

    cast((''9999-12-31 23:59:59.999999'') AS TIMESTAMP(6))as closedate_stg,

    cc_claim.reporteddate_stg as reporteddate_stg,

    case when cc_claim.Retired_stg=0 and cc_claimcontact.Retired_stg=0 and cc_contact.Retired_stg=0 and cc_claimcontactrole.Retired_stg=0 then 0 else 1 end as Retired_stg



FROM 

    (select cc_claim.* from DB_T_PROD_STAG.cc_claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

    LEFT JOIN DB_T_PROD_STAG.cctl_claimsecuritytype on cc_claim.permissionrequired_stg = cctl_claimsecuritytype.ID_stg

    JOIN DB_T_PROD_STAG.cc_claimcontact ON cc_claim.id_stg = cc_claimcontact.ClaimID_stg 

    JOIN DB_T_PROD_STAG.cc_contact ON cc_claimcontact.ContactID_stg = cc_contact.ID_stg 

    JOIN DB_T_PROD_STAG.cctl_contact ON cc_contact.Subtype_stg = cctl_contact.id_stg 

    JOIN DB_T_PROD_STAG.cc_claimcontactrole ON cc_claimcontactrole.claimcontactID_stg = cc_claimcontact.id_stg 

    JOIN DB_T_PROD_STAG.cctl_contactrole ON cctl_contactrole.ID_stg = cc_claimcontactrole.Role_stg

    LEFT JOIN DB_T_PROD_STAG.cctl_personrelationtype ON cctl_personrelationtype.id_stg=cc_claim.ReportedByType_stg

WHERE

  cctl_contactrole.Typecode_stg NOT IN (''Assigned Group'',''PRTY_CLM_ROLE_TYPE1'',''PRTY_CLM_ROLE_TYPE5'') 

    and cc_claimcontactrole.UpdateTime_stg >  (:start_dttm)

    and cc_claimcontactrole.UpdateTime_stg <=  (:end_dttm) 

    



union 



SELECT

    cc_claim.ClaimNumber_stg,

    cctl_userrole.TYPECODE_stg as typecode,

    cc_userroleassign.createtime_stg,

    cast(cc_contact.PublicID_stg as varchar(64))as PublicID_stg, 

    NULL as contactprohibited_stg,

   cast( NULL as varchar(3))  as EMPLE_IND_stg , 

    cast( NULL AS VARCHAR(50) ) as RLTNSHP_TO_INSRD_TXT_stg,

     ''SRC_SYS6'' AS SRC_CD_stg,

    cc_userroleassign.UpdateTime_stg as updatetime,

    cast((''9999-12-31 23:59:59.999999'') AS TIMESTAMP(6))as closedate_stg,

    cc_claim.reporteddate_stg,

    case when cc_claim.Retired_stg=0 and cc_contact.Retired_stg=0 then 0 else 1 end as Retired_stg

FROM 

    (select cc_claim.* from DB_T_PROD_STAG.cc_claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

    JOIN DB_T_PROD_STAG.cc_userroleassign ON cc_claim.id_stg=cc_userroleassign.claimid_stg

    JOIN  DB_T_PROD_STAG.cc_user ON cc_user.id_stg=cc_userroleassign.AssignedUserID_stg

    JOIN DB_T_PROD_STAG.cc_contact ON cc_user.ContactID_stg=cc_contact.id_stg

    JOIN DB_T_PROD_STAG.cctl_contact ON cc_contact.Subtype_stg = cctl_contact.id_stg 

    JOIN DB_T_PROD_STAG.cctl_userrole ON cc_userroleassign.Role_stg = cctl_userrole.id_stg

WHERE

    cctl_contact.name_stg in (''Person'',''Adjudicator'',''User Contact'',''Vendor (Person)'',''Attorney'', ''Doctor'',''Policy Person'',''Contact'') 

    and cc_userroleassign.UpdateTime_stg  > (:start_dttm)

    and cc_userroleassign.UpdateTime_stg <= (:end_dttm)

     and cctl_userrole.TYPECODE_stg NOT IN (''Assigned Group'',''PRTY_CLM_ROLE_TYPE1'',''PRTY_CLM_ROLE_TYPE5'') 



    union

    

SELECT DISTINCT

    cc_claim.ClaimNumber_stg,                                               

    ''subrogationowner'' as typecode,                                             

    cc_subrogationsummary.createtime_stg,                                             

    cast(cc_contact.PublicID_stg as varchar(64)) as PublicID_stg,                                                

    NULL as contactprohibited_stg,                                              

   cast( NULL as varchar(3))  as EMPLE_IND_stg ,                                                

   cast( NULL AS VARCHAR(50) ) as RLTNSHP_TO_INSRD_TXT_stg,                                             

     ''SRC_SYS6'' AS SRC_CD_stg,                                              

    cc_subrogationsummary.UpdateTime_stg as updatetime,                                               

    cast((''9999-12-31 23:59:59.999999'') AS TIMESTAMP(6))as closedate_stg,                                               

    cc_claim.reporteddate_stg,                                              

    case when cc_claim.Retired_stg=0 and cc_contact.Retired_stg=0 then 0 else 1 end as Retired_stg                                              

    from DB_T_PROD_STAG.cc_claim cc_claim                                                                                                 

       left join DB_T_PROD_STAG.cc_subrogationsummary cc_subrogationsummary  on cc_subrogationsummary.ClaimID_stg = cc_claim.ID_stg                                               

       left join DB_T_PROD_STAG.cc_user cc_user on cc_user.ID_stg = cc_subrogationsummary.IntOwnedUserID_alfa_stg                                                

       left join DB_T_PROD_STAG.cc_contact cc_contact on cc_contact.ID_stg = cc_user.ContactID_stg                                               

       left join DB_T_PROD_STAG.cc_userrole cc_userrole on cc_userrole.UserID_stg = cc_user.ID_stg                                                

       left join DB_T_PROD_STAG.cc_role cc_role on cc_role.ID_stg = cc_userrole.RoleID_stg             

   WHERE  trim(lower(cc_role.Name_stg))=''subrogation specialist'' and

   cc_subrogationsummary.UpdateTime_stg  > (:start_dttm) 

   and cc_subrogationsummary.UpdateTime_stg <= (:end_dttm) 

   

     )   as temp

QUALIFY ROW_NUMBER () OVER (partition by temp.ClaimNumber_stg,temp.TYPECODE,temp.PublicID_stg order by temp.UpdateTime desc)=1)SRC

/* LKP_XLAT_SRC_CD */
LEFT OUTER JOIN

(SELECT 

    TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

    ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

    DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

    TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

        AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

        AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'')LKP_XLAT_SRC_CD ON LKP_XLAT_SRC_CD.SRC_IDNTFTN_VAL=SRC.SRC_CD_STG

/* LKP_CLM */
LEFT OUTER JOIN

(SELECT CLM.CLM_ID as CLM_ID, CLM.CLM_TYPE_CD as CLM_TYPE_CD, CLM.CLM_MDIA_TYPE_CD as CLM_MDIA_TYPE_CD,

        CLM.CLM_SUBMTL_TYPE_CD as CLM_SUBMTL_TYPE_CD, CLM.ACDNT_TYPE_CD as ACDNT_TYPE_CD,

        CLM.CLM_CTGY_TYPE_CD as CLM_CTGY_TYPE_CD, CLM.ADDL_INSRNC_PLN_IND as ADDL_INSRNC_PLN_IND,

        CLM.EMPLMT_RLTD_IND as EMPLMT_RLTD_IND, CLM.ATTNY_INVLVMT_IND as ATTNY_INVLVMT_IND,

        CLM.CLM_PRIR_IND as CLM_PRIR_IND, CLM.PMT_MODE_CD as PMT_MODE_CD,

        CLM.CLM_OBLGTN_TYPE_CD as CLM_OBLGTN_TYPE_CD, CLM.SUBRGTN_ELGBL_CD as SUBRGTN_ELGBL_CD,

        CLM.SUBRGTN_ELGBLY_RSN_CD as SUBRGTN_ELGBLY_RSN_CD, CLM.CURY_CD as CURY_CD,

        CLM.INCDT_EV_ID as INCDT_EV_ID, CLM.INSRD_AT_FAULT_IND as INSRD_AT_FAULT_IND,

        CLM.CVGE_IN_QUES_IND as CVGE_IN_QUES_IND, CLM.EXTNT_OF_FIRE_DMG_TYPE_CD as EXTNT_OF_FIRE_DMG_TYPE_CD,

        CLM.VFYD_CLM_IND as VFYD_CLM_IND, CLM.PRCS_ID as PRCS_ID, CLM.CLM_STRT_DTTM as CLM_STRT_DTTM,

        CLM.CLM_END_DTTM as CLM_END_DTTM, CLM.EDW_STRT_DTTM as EDW_STRT_DTTM,

        CLM.EDW_END_DTTM as EDW_END_DTTM, CLM.TRANS_STRT_DTTM as TRANS_STRT_DTTM,

        CLM.LGCY_CLM_NUM as LGCY_CLM_NUM, CLM.CLM_NUM as CLM_NUM, CLM.SRC_SYS_CD as SRC_SYS_CD 

FROM    DB_T_PROD_CORE.CLM  

QUALIFY ROW_NUMBER() OVER(

PARTITION BY CLM.CLM_NUM,CLM.SRC_SYS_CD  

ORDER BY CLM.EDW_END_DTTM desc) = 1)LKP_CLM ON LKP_CLM.CLM_NUM=SRC.CLAIMNUMBER_STG AND LKP_CLM.SRC_SYS_CD=LKP_XLAT_SRC_CD.TGT_IDNTFTN_VAL

/* LKP_INTRNL_ORG_TYPE */
LEFT OUTER JOIN

(SELECT 

    TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

    ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

    DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

    TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INTRNL_ORG_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

        AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

        AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'')LKP_INTRNL_ORG_TYPE ON LKP_INTRNL_ORG_TYPE.SRC_IDNTFTN_VAL=''INTRNL_ORG_TYPE15''

/* LKP_XLAT */
LEFT OUTER JOIN

(SELECT 

    TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

    ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

    DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

    TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_CLM_ROLE_TYPE'' 

       AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''cctl_claimanttype.typecode'',''cctl_contactrole.typecode'',''derived'',''cctl_userrole.typecode'',''cctl_losspartytype.typecode'')

AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''GW'' ,''DS'')/* AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' */
    qualify row_number() over(partition by SRC_IDNTFTN_VAL 

order by case when (SRC_IDNTFTN_VAL  in (''primaryadjuster'',''insured'')) then tgt_idntftn_val  else 1  end desc,tgt_idntftn_val asc)=1

    )LKP_XLAT ON LKP_XLAT.SRC_IDNTFTN_VAL =var_ContactroleTypecode

/* -LKP_INTRNL_ORG  */
LEFT OUTER JOIN

(SELECT INTRNL_ORG.INTRNL_ORG_PRTY_ID as INTRNL_ORG_PRTY_ID, INTRNL_ORG.INTRNL_ORG_TYPE_CD as INTRNL_ORG_TYPE_CD,

        INTRNL_ORG.INTRNL_ORG_SBTYPE_CD as INTRNL_ORG_SBTYPE_CD, INTRNL_ORG.INTRNL_ORG_NUM as INTRNL_ORG_NUM,

        INTRNL_ORG.SRC_SYS_CD as SRC_SYS_CD 

FROM    DB_T_PROD_CORE.INTRNL_ORG 

 qualify row_number () over (partition by INTRNL_ORG_NUM,INTRNL_ORG_TYPE_CD,INTRNL_ORG_SBTYPE_CD,SRC_SYS_CD order by EDW_END_DTTM desc)=1

 )LKP_INTRNL_ORG ON LKP_INTRNL_ORG.INTRNL_ORG_TYPE_CD=LKP_INTRNL_ORG_TYPE.TGT_IDNTFTN_VAL AND LKP_INTRNL_ORG.INTRNL_ORG_NUM=SRC.RLTNSHP_TO_INSRD_TXT_stg

 AND LKP_INTRNL_ORG.SRC_SYS_CD=LKP_XLAT_SRC_CD.TGT_IDNTFTN_VAL

/* -LKP_INDIV_CLM_CTR */
LEFT OUTER JOIN(

SELECT 

    INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID, 

    INDIV.NK_PUBLC_ID as NK_PUBLC_ID 

FROM 

    DB_T_PROD_CORE.INDIV

WHERE

    INDIV.NK_PUBLC_ID IS NOT NULL

    qualify row_number() over(partition by NK_PUBLC_ID order by edw_end_dttm desc,edw_strt_dttm desc)=1)LKP_INDIV 

    ON LKP_INDIV.NK_PUBLC_ID=SRC.PublicID_stg

/* -LKP_BUSN */
LEFT OUTER JOIN

(SELECT BUSN.BUSN_PRTY_ID as BUSN_PRTY_ID, BUSN.BUSN_STRT_DTTM as BUSN_STRT_DTTM,

        BUSN.BUSN_CTGY_CD as BUSN_CTGY_CD, BUSN.BUSN_LEGL_STRT_DT as BUSN_LEGL_STRT_DT,

        BUSN.BUSN_LEGL_END_DT as BUSN_LEGL_END_DT, BUSN.TAX_BRAKT_CD as TAX_BRAKT_CD,

        BUSN.STK_EXCH_LISTD_IND as STK_EXCH_LISTD_IND, BUSN.ORG_TYPE_CD as ORG_TYPE_CD,

        BUSN.ORG_ESTBLD_DTTM as ORG_ESTBLD_DTTM, BUSN.PARNT_ORG_PRTY_ID as PARNT_ORG_PRTY_ID,

        BUSN.ORG_SIZE_TYPE_CD as ORG_SIZE_TYPE_CD, BUSN.LEGL_CLASFCN_CD as LEGL_CLASFCN_CD,

        BUSN.OWNRSHP_TYPE_CD as OWNRSHP_TYPE_CD, BUSN.ORG_CLS_DT as ORG_CLS_DT,

        BUSN.ORG_OPRTN_DT as ORG_OPRTN_DT, BUSN.DUNS_ID as DUNS_ID, BUSN.GICS_SBIDSTRY_CD as GICS_SBIDSTRY_CD,

        BUSN.ORG_FSCL_MTH_NUM as ORG_FSCL_MTH_NUM, BUSN.ORG_FSCL_DY_NUM as ORG_FSCL_DY_NUM,

        BUSN.BIC_BUSN_CD as BIC_BUSN_CD, BUSN.PRTY_DESC as PRTY_DESC,

        BUSN.BUSN_END_DTTM as BUSN_END_DTTM, BUSN.LIFCYCL_CD as LIFCYCL_CD,

        BUSN.PRTY_TYPE_CD as PRTY_TYPE_CD, BUSN.INIT_DATA_SRC_TYPE_CD as INIT_DATA_SRC_TYPE_CD,

        BUSN.INC_IND as INC_IND, BUSN.TAX_ID_STS_CD as TAX_ID_STS_CD,

        BUSN.SRC_SYS_CD as SRC_SYS_CD, BUSN.PRCS_ID as PRCS_ID, BUSN.EDW_STRT_DTTM as EDW_STRT_DTTM,

        BUSN.EDW_END_DTTM as EDW_END_DTTM, BUSN.TRANS_STRT_DTTM as TRANS_STRT_DTTM,

        BUSN.TRANS_END_DTTM as TRANS_END_DTTM, BUSN.NK_BUSN_CD as NK_BUSN_CD 

FROM    DB_T_PROD_CORE.BUSN

QUALIFY ROW_NUMBER () OVER (

PARTITION BY NK_BUSN_CD 

ORDER BY EDW_END_DTTM DESC )=1)LKP_BUSN ON LKP_BUSN.NK_BUSN_CD=SRC.PublicID_stg

    )SQ

    )

/* --------------------------------------/*SOURCE QUERY ENDS*/--------------------------------------- */
/* -LKP_TGT */
LEFT OUTER JOIN

(SELECT PRTY_CLM.PRTY_CLM_STRT_DTTM AS PRTY_CLM_STRT_DTTM, PRTY_CLM.PRTY_CLM_END_DTTM AS PRTY_CLM_END_DTTM,

        PRTY_CLM.UNLISTD_OPRTR_IND AS UNLISTD_OPRTR_IND, PRTY_CLM.PRTY_CNTCT_PROHIBITED_IND AS PRTY_CNTCT_PROHIBITED_IND,

        PRTY_CLM.EMPLE_IND AS EMPLE_IND, PRTY_CLM.RLTNSHP_TO_INSRD_TXT AS RLTNSHP_TO_INSRD_TXT,

        PRTY_CLM.EDW_STRT_DTTM AS EDW_STRT_DTTM, PRTY_CLM.EDW_END_DTTM AS EDW_END_DTTM,

        PRTY_CLM.TRANS_STRT_DTTM AS TRANS_STRT_DTTM, PRTY_CLM.CLM_ID AS CLM_ID,

        PRTY_CLM.PRTY_CLM_ROLE_CD  AS PRTY_CLM_ROLE_CD, PRTY_CLM.PRTY_ID AS PRTY_ID 

FROM    DB_T_PROD_CORE.PRTY_CLM 

QUALIFY ROW_NUMBER() OVER(

PARTITION BY  PRTY_CLM.CLM_ID,PRTY_CLM.PRTY_CLM_ROLE_CD, PRTY_CLM.PRTY_ID 

ORDER BY PRTY_CLM.EDW_END_DTTM DESC) = 1)LKP_TGT ON LKP_TGT.CLM_ID=SQ.in_CLM_ID AND LKP_TGT.PRTY_CLM_ROLE_CD=SQ.IN_PRTY_CLM_ROLE_CD

AND LKP_TGT.PRTY_ID=SQ.IN_PRTY_ID
) SRC
)
);


-- Component exp_all_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source AS
(
SELECT
SQ_prty_clm_x.SQ_CLM_ID as SQ_CLM_ID,
SQ_prty_clm_x.SQ_PRTY_CLM_ROLE_CD as SQ_PRTY_CLM_ROLE_CD,
SQ_prty_clm_x.SQ_out_AssignmentDate as SQ_out_AssignmentDate,
SQ_prty_clm_x.SQ_PRTY_ID as SQ_PRTY_ID,
SQ_prty_clm_x.SQ_PRTY_CLM_END_DTTM as SQ_PRTY_CLM_END_DTTM,
:PRCS_ID as PRCS_ID,
SQ_prty_clm_x.SQ_PRTY_CNTCT_PROHIBITED_IND as SQ_PRTY_CNTCT_PROHIBITED_IND,
SQ_prty_clm_x.SQ_EMPLE_IND as SQ_EMPLE_IND,
SQ_prty_clm_x.TGT_CLM_ID as TGT_CLM_ID,
SQ_prty_clm_x.TGT_PRTY_CLM_ROLE_CD as TGT_PRTY_CLM_ROLE_CD,
SQ_prty_clm_x.TGT_PRTY_CLM_STRT_DTTM as TGT_PRTY_CLM_STRT_DTTM,
SQ_prty_clm_x.TGT_PRTY_ID as TGT_PRTY_ID,
SQ_prty_clm_x.TGT_EDW_STRT_DTTM as TGT_EDW_STRT_DTTM,
SQ_prty_clm_x.SQ_ENDDATE as SQ_ENDDATE,
SQ_prty_clm_x.INS_UPD_FLAG as INS_UPD_FLAG,
SQ_prty_clm_x.TGT_PRTY_CNTCT_PROHIBITED_IND as TGT_PRTY_CNTCT_PROHIBITED_IND,
SQ_prty_clm_x.TGT_EMPLE_IND as TGT_EMPLE_IND,
SQ_prty_clm_x.TGT_TRANS_STRT_DTTM as TGT_TRANS_STRT_DTTM,
SQ_prty_clm_x.TGT_PRTY_CLM_END_DTTM as TGT_PRTY_CLM_END_DTTM,
SQ_prty_clm_x.TGT_EDW_END_DTTM as TGT_EDW_END_DTTM,
SQ_prty_clm_x.SQ_RETIRED as SQ_RETIRED,
SQ_prty_clm_x.SQ_UNLISTD_OPRTR_IND as SQ_UNLISTD_OPRTR_IND,
SQ_prty_clm_x.SQ_RLTNSHP_TO_INSRD_TXT as SQ_RLTNSHP_TO_INSRD_TXT,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
DATEADD (''second'', -1, CURRENT_TIMESTAMP()) AS EDW_END_DTTM_exp,
COALESCE(SQ_prty_clm_x.SRC_TRANS_STRT_DTTM, CAST(''1900-01-01'' AS DATE)) AS o_updatetime,
SQ_prty_clm_x.source_record_id
FROM
SQ_prty_clm_x
);


-- Component rtr_Ins_Upd_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE rtr_Ins_Upd_Insert AS
(SELECT
exp_all_source.SQ_CLM_ID as in_CLM_ID,
exp_all_source.SQ_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD,
NULL as out_AssignmentDate,
exp_all_source.SQ_PRTY_ID as in_PRTY_ID,
exp_all_source.SQ_PRTY_CLM_END_DTTM as in_PRTY_CLM_END_DTTM,
exp_all_source.PRCS_ID as in_PRCS_ID,
exp_all_source.SQ_PRTY_CNTCT_PROHIBITED_IND as in_PRTY_CNTCT_PROHIBITED_IND,
exp_all_source.SQ_EMPLE_IND as in_EMPLE_IND,
exp_all_source.TGT_CLM_ID as lkp_CLM_ID,
exp_all_source.TGT_PRTY_CLM_ROLE_CD as lkp_PRTY_CLM_ROLE_CD,
exp_all_source.TGT_PRTY_CLM_STRT_DTTM as lkp_PRTY_CLM_STRT_DTTM,
exp_all_source.TGT_PRTY_ID as lkp_PRTY_ID,
exp_all_source.TGT_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
NULL as lkp_PRCS_ID,
exp_all_source.SQ_out_AssignmentDate as out_AssignmentDate1,
exp_all_source.SQ_ENDDATE as EndDate,
exp_all_source.INS_UPD_FLAG as o_Ins_Upd,
exp_all_source.TGT_PRTY_CNTCT_PROHIBITED_IND as lkp_PRTY_CNTCT_PROHIBITED_IND,
exp_all_source.TGT_EMPLE_IND as lkp_EMPLE_IND,
exp_all_source.o_updatetime as TRANS_STRT_DTTM,
exp_all_source.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_all_source.EDW_END_DTTM as EDW_END_DTTM,
exp_all_source.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_all_source.TGT_TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM,
exp_all_source.TGT_PRTY_CLM_STRT_DTTM as lkp_PRTY_CLM_STRT_DTTM4,
exp_all_source.TGT_PRTY_CLM_END_DTTM as lkp_PRTY_CLM_END_DTTM,
exp_all_source.TGT_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_all_source.SQ_RETIRED as Retired,
exp_all_source.SQ_UNLISTD_OPRTR_IND as UNLISTD_OPRTR_IND,
exp_all_source.SQ_RLTNSHP_TO_INSRD_TXT as RLTNSHP_TO_INSRD_TXT,
exp_all_source.source_record_id
FROM
exp_all_source
WHERE ( ( exp_all_source.INS_UPD_FLAG = ''I'' ) OR ( exp_all_source.SQ_RETIRED = 0 AND exp_all_source.TGT_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) ) AND exp_all_source.SQ_CLM_ID IS NOT NULL AND exp_all_source.SQ_PRTY_ID IS NOT NULL);


-- Component rtr_Ins_Upd_Retire, Type ROUTER Output Group Retire
CREATE OR REPLACE TEMPORARY TABLE rtr_Ins_Upd_Retire AS
(SELECT
exp_all_source.SQ_CLM_ID as in_CLM_ID,
exp_all_source.SQ_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD,
NULL as out_AssignmentDate,
exp_all_source.SQ_PRTY_ID as in_PRTY_ID,
exp_all_source.SQ_PRTY_CLM_END_DTTM as in_PRTY_CLM_END_DTTM,
exp_all_source.PRCS_ID as in_PRCS_ID,
exp_all_source.SQ_PRTY_CNTCT_PROHIBITED_IND as in_PRTY_CNTCT_PROHIBITED_IND,
exp_all_source.SQ_EMPLE_IND as in_EMPLE_IND,
exp_all_source.TGT_CLM_ID as lkp_CLM_ID,
exp_all_source.TGT_PRTY_CLM_ROLE_CD as lkp_PRTY_CLM_ROLE_CD,
exp_all_source.TGT_PRTY_CLM_STRT_DTTM as lkp_PRTY_CLM_STRT_DTTM,
exp_all_source.TGT_PRTY_ID as lkp_PRTY_ID,
exp_all_source.TGT_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
NULL as lkp_PRCS_ID,
exp_all_source.SQ_out_AssignmentDate as out_AssignmentDate1,
exp_all_source.SQ_ENDDATE as EndDate,
exp_all_source.INS_UPD_FLAG as o_Ins_Upd,
exp_all_source.TGT_PRTY_CNTCT_PROHIBITED_IND as lkp_PRTY_CNTCT_PROHIBITED_IND,
exp_all_source.TGT_EMPLE_IND as lkp_EMPLE_IND,
exp_all_source.o_updatetime as TRANS_STRT_DTTM,
exp_all_source.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_all_source.EDW_END_DTTM as EDW_END_DTTM,
exp_all_source.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_all_source.TGT_TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM,
exp_all_source.TGT_PRTY_CLM_STRT_DTTM as lkp_PRTY_CLM_STRT_DTTM4,
exp_all_source.TGT_PRTY_CLM_END_DTTM as lkp_PRTY_CLM_END_DTTM,
exp_all_source.TGT_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_all_source.SQ_RETIRED as Retired,
exp_all_source.SQ_UNLISTD_OPRTR_IND as UNLISTD_OPRTR_IND,
exp_all_source.SQ_RLTNSHP_TO_INSRD_TXT as RLTNSHP_TO_INSRD_TXT,
exp_all_source.source_record_id
FROM
exp_all_source
WHERE exp_all_source.INS_UPD_FLAG = ''R'' and exp_all_source.SQ_RETIRED != 0 and exp_all_source.TGT_EDW_END_DTTM = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component rtr_Ins_Upd_Update, Type ROUTER Output Group Update
CREATE OR REPLACE TEMPORARY TABLE rtr_Ins_Upd_Update AS
(SELECT
exp_all_source.SQ_CLM_ID as in_CLM_ID,
exp_all_source.SQ_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD,
NULL as out_AssignmentDate,
exp_all_source.SQ_PRTY_ID as in_PRTY_ID,
exp_all_source.SQ_PRTY_CLM_END_DTTM as in_PRTY_CLM_END_DTTM,
exp_all_source.PRCS_ID as in_PRCS_ID,
exp_all_source.SQ_PRTY_CNTCT_PROHIBITED_IND as in_PRTY_CNTCT_PROHIBITED_IND,
exp_all_source.SQ_EMPLE_IND as in_EMPLE_IND,
exp_all_source.TGT_CLM_ID as lkp_CLM_ID,
exp_all_source.TGT_PRTY_CLM_ROLE_CD as lkp_PRTY_CLM_ROLE_CD,
exp_all_source.TGT_PRTY_CLM_STRT_DTTM as lkp_PRTY_CLM_STRT_DTTM,
exp_all_source.TGT_PRTY_ID as lkp_PRTY_ID,
exp_all_source.TGT_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
NULL as lkp_PRCS_ID,
exp_all_source.SQ_out_AssignmentDate as out_AssignmentDate1,
exp_all_source.SQ_ENDDATE as EndDate,
exp_all_source.INS_UPD_FLAG as o_Ins_Upd,
exp_all_source.TGT_PRTY_CNTCT_PROHIBITED_IND as lkp_PRTY_CNTCT_PROHIBITED_IND,
exp_all_source.TGT_EMPLE_IND as lkp_EMPLE_IND,
exp_all_source.o_updatetime as TRANS_STRT_DTTM,
exp_all_source.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_all_source.EDW_END_DTTM as EDW_END_DTTM,
exp_all_source.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_all_source.TGT_TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM,
exp_all_source.TGT_PRTY_CLM_STRT_DTTM as lkp_PRTY_CLM_STRT_DTTM4,
exp_all_source.TGT_PRTY_CLM_END_DTTM as lkp_PRTY_CLM_END_DTTM,
exp_all_source.TGT_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_all_source.SQ_RETIRED as Retired,
exp_all_source.SQ_UNLISTD_OPRTR_IND as UNLISTD_OPRTR_IND,
exp_all_source.SQ_RLTNSHP_TO_INSRD_TXT as RLTNSHP_TO_INSRD_TXT,
exp_all_source.source_record_id
FROM
exp_all_source
WHERE ( exp_all_source.INS_UPD_FLAG = ''U'' AND exp_all_source.TGT_EDW_END_DTTM = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) AND exp_all_source.SQ_CLM_ID IS NOT NULL AND exp_all_source.SQ_PRTY_ID IS NOT NULL);


-- Component UPD_prty_clm_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE UPD_prty_clm_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_Ins_Upd_Insert.in_CLM_ID as in_CLM_ID1,
rtr_Ins_Upd_Insert.in_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD1,
rtr_Ins_Upd_Insert.out_AssignmentDate1 as out_AssignmentDate,
rtr_Ins_Upd_Insert.in_PRTY_ID as in_PRTY_ID1,
rtr_Ins_Upd_Insert.EndDate as EndDate1,
rtr_Ins_Upd_Insert.in_PRTY_CNTCT_PROHIBITED_IND as in_PRTY_CNTCT_PROHIBITED_IND1,
rtr_Ins_Upd_Insert.in_EMPLE_IND as in_EMPLE_IND1,
rtr_Ins_Upd_Insert.in_PRCS_ID as in_PRCS_ID1,
rtr_Ins_Upd_Insert.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_Ins_Upd_Insert.EDW_END_DTTM as EDW_END_DTTM1,
rtr_Ins_Upd_Insert.TRANS_STRT_DTTM as TRANS_STRT_DTTM1,
rtr_Ins_Upd_Insert.Retired as Retired1,
rtr_Ins_Upd_Insert.UNLISTD_OPRTR_IND as UNLISTD_OPRTR_IND1,
rtr_Ins_Upd_Insert.RLTNSHP_TO_INSRD_TXT as RLTNSHP_TO_INSRD_TXT1,
0 as UPDATE_STRATEGY_ACTION,
rtr_Ins_Upd_Insert.source_record_id
FROM
rtr_Ins_Upd_Insert
);


-- Component upd_prty_clm_Update, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prty_clm_Update AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_Ins_Upd_Update.lkp_CLM_ID as lkp_CLM_ID1,
rtr_Ins_Upd_Update.lkp_PRTY_CLM_ROLE_CD as lkp_PRTY_CLM_ROLE_CD1,
rtr_Ins_Upd_Update.lkp_PRTY_CLM_STRT_DTTM as lkp_PRTY_CLM_STRT_DTTM3,
rtr_Ins_Upd_Update.lkp_PRTY_ID as lkp_PRTY_ID1,
rtr_Ins_Upd_Update.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM1,
rtr_Ins_Upd_Update.lkp_PRCS_ID as lkp_PRCS_ID3,
rtr_Ins_Upd_Update.EDW_END_DTTM_exp as EDW_END_DTTM_exp3,
NULL as lkp_PRTY_CNTCT_PROHIBITED_IND1,
NULL as lkp_EMPLE_IND1,
rtr_Ins_Upd_Update.lkp_PRTY_CLM_STRT_DTTM4 as lkp_PRTY_CLM_STRT_DTTM4,
rtr_Ins_Upd_Update.lkp_PRTY_CLM_END_DTTM as lkp_PRTY_CLM_END_DTTM,
rtr_Ins_Upd_Update.lkp_TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM3,
rtr_Ins_Upd_Update.lkp_PRTY_CNTCT_PROHIBITED_IND as lkp_PRTY_CNTCT_PROHIBITED_IND11,
rtr_Ins_Upd_Update.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM3,
rtr_Ins_Upd_Update.Retired as Retired3,
rtr_Ins_Upd_Update.TRANS_STRT_DTTM as TRANS_STRT_DTTM3,
1 as UPDATE_STRATEGY_ACTION,
rtr_Ins_Upd_Update.source_record_id
FROM
rtr_Ins_Upd_Update
);


-- Component upd_prty_clm_upd_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prty_clm_upd_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_Ins_Upd_Update.in_CLM_ID as in_CLM_ID3,
rtr_Ins_Upd_Update.in_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD3,
rtr_Ins_Upd_Update.out_AssignmentDate1 as out_AssignmentDate,
rtr_Ins_Upd_Update.in_PRTY_ID as in_PRTY_ID3,
rtr_Ins_Upd_Update.EndDate as EndDate3,
rtr_Ins_Upd_Update.in_PRTY_CNTCT_PROHIBITED_IND as in_PRTY_CNTCT_PROHIBITED_IND3,
rtr_Ins_Upd_Update.in_EMPLE_IND as in_EMPLE_IND3,
rtr_Ins_Upd_Update.in_PRCS_ID as in_PRCS_ID3,
rtr_Ins_Upd_Update.EDW_STRT_DTTM as EDW_STRT_DTTM3,
rtr_Ins_Upd_Update.EDW_END_DTTM as EDW_END_DTTM3,
rtr_Ins_Upd_Update.TRANS_STRT_DTTM as TRANS_STRT_DTTM3,
rtr_Ins_Upd_Update.Retired as Retired3,
rtr_Ins_Upd_Update.UNLISTD_OPRTR_IND as UNLISTD_OPRTR_IND3,
rtr_Ins_Upd_Update.RLTNSHP_TO_INSRD_TXT as RLTNSHP_TO_INSRD_TXT3,
0 as UPDATE_STRATEGY_ACTION,
rtr_Ins_Upd_Update.source_record_id
FROM
rtr_Ins_Upd_Update
);


-- Component fil_prty_clm_upd_ins, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_prty_clm_upd_ins AS
(
SELECT
upd_prty_clm_upd_ins.in_CLM_ID3 as in_CLM_ID3,
upd_prty_clm_upd_ins.in_PRTY_CLM_ROLE_CD3 as in_PRTY_CLM_ROLE_CD3,
upd_prty_clm_upd_ins.out_AssignmentDate as out_AssignmentDate,
upd_prty_clm_upd_ins.in_PRTY_ID3 as in_PRTY_ID3,
upd_prty_clm_upd_ins.EndDate3 as EndDate3,
upd_prty_clm_upd_ins.in_PRTY_CNTCT_PROHIBITED_IND3 as in_PRTY_CNTCT_PROHIBITED_IND3,
upd_prty_clm_upd_ins.in_EMPLE_IND3 as in_EMPLE_IND3,
upd_prty_clm_upd_ins.in_PRCS_ID3 as in_PRCS_ID3,
upd_prty_clm_upd_ins.EDW_STRT_DTTM3 as EDW_STRT_DTTM3,
upd_prty_clm_upd_ins.EDW_END_DTTM3 as EDW_END_DTTM3,
upd_prty_clm_upd_ins.TRANS_STRT_DTTM3 as TRANS_STRT_DTTM3,
upd_prty_clm_upd_ins.Retired3 as Retired3,
upd_prty_clm_upd_ins.UNLISTD_OPRTR_IND3 as UNLISTD_OPRTR_IND3,
upd_prty_clm_upd_ins.RLTNSHP_TO_INSRD_TXT3 as RLTNSHP_TO_INSRD_TXT3,
upd_prty_clm_upd_ins.source_record_id
FROM
upd_prty_clm_upd_ins
WHERE upd_prty_clm_upd_ins.Retired3 = 0
);


-- Component fil_prty_clm_upd_update, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_prty_clm_upd_update AS
(
SELECT
upd_prty_clm_Update.lkp_CLM_ID1 as lkp_CLM_ID1,
upd_prty_clm_Update.lkp_PRTY_CLM_ROLE_CD1 as lkp_PRTY_CLM_ROLE_CD1,
upd_prty_clm_Update.lkp_PRTY_CLM_STRT_DTTM3 as lkp_PRTY_CLM_STRT_DTTM3,
upd_prty_clm_Update.lkp_PRTY_ID1 as lkp_PRTY_ID1,
upd_prty_clm_Update.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM1,
upd_prty_clm_Update.lkp_PRCS_ID3 as lkp_PRCS_ID3,
upd_prty_clm_Update.EDW_END_DTTM_exp3 as EDW_END_DTTM_exp3,
upd_prty_clm_Update.lkp_PRTY_CNTCT_PROHIBITED_IND1 as lkp_PRTY_CNTCT_PROHIBITED_IND1,
upd_prty_clm_Update.lkp_EMPLE_IND1 as lkp_EMPLE_IND1,
upd_prty_clm_Update.lkp_PRTY_CLM_STRT_DTTM4 as lkp_PRTY_CLM_STRT_DTTM4,
upd_prty_clm_Update.lkp_PRTY_CLM_END_DTTM as lkp_PRTY_CLM_END_DTTM,
upd_prty_clm_Update.lkp_TRANS_STRT_DTTM3 as lkp_TRANS_STRT_DTTM3,
upd_prty_clm_Update.lkp_PRTY_CNTCT_PROHIBITED_IND11 as lkp_PRTY_CNTCT_PROHIBITED_IND11,
upd_prty_clm_Update.lkp_EDW_END_DTTM3 as lkp_EDW_END_DTTM3,
upd_prty_clm_Update.Retired3 as Retired3,
upd_prty_clm_Update.TRANS_STRT_DTTM3 as TRANS_STRT_DTTM3,
upd_prty_clm_Update.source_record_id
FROM
upd_prty_clm_Update
WHERE upd_prty_clm_Update.lkp_EDW_END_DTTM3 = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
);


-- Component upd_prty_clm_Update_Retire_Reject, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prty_clm_Update_Retire_Reject AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_Ins_Upd_Retire.lkp_CLM_ID as lkp_CLM_ID1,
rtr_Ins_Upd_Retire.lkp_PRTY_CLM_ROLE_CD as lkp_PRTY_CLM_ROLE_CD1,
rtr_Ins_Upd_Retire.lkp_PRTY_CLM_STRT_DTTM as lkp_PRTY_CLM_STRT_DTTM3,
rtr_Ins_Upd_Retire.lkp_PRTY_ID as lkp_PRTY_ID1,
rtr_Ins_Upd_Retire.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM1,
rtr_Ins_Upd_Retire.lkp_PRCS_ID as lkp_PRCS_ID3,
NULL as lkp_PRTY_CNTCT_PROHIBITED_IND1,
NULL as lkp_EMPLE_IND1,
rtr_Ins_Upd_Retire.EDW_END_DTTM_exp as EDW_END_DTTM_exp3,
rtr_Ins_Upd_Retire.lkp_PRTY_CLM_STRT_DTTM4 as lkp_PRTY_CLM_STRT_DTTM4,
rtr_Ins_Upd_Retire.lkp_PRTY_CLM_END_DTTM as lkp_PRTY_CLM_END_DTTM,
rtr_Ins_Upd_Retire.lkp_TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM3,
rtr_Ins_Upd_Retire.lkp_PRTY_CNTCT_PROHIBITED_IND as lkp_PRTY_CNTCT_PROHIBITED_IND11,
rtr_Ins_Upd_Retire.TRANS_STRT_DTTM as TRANS_STRT_DTTM4,
1 as UPDATE_STRATEGY_ACTION,
rtr_Ins_Upd_Retire.source_record_id
FROM
rtr_Ins_Upd_Retire
);


-- Component exp_prty_clm_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prty_clm_insert AS
(
SELECT
UPD_prty_clm_insert.in_CLM_ID1 as in_CLM_ID1,
UPD_prty_clm_insert.in_PRTY_CLM_ROLE_CD1 as in_PRTY_CLM_ROLE_CD1,
UPD_prty_clm_insert.out_AssignmentDate as out_AssignmentDate,
UPD_prty_clm_insert.in_PRTY_ID1 as in_PRTY_ID1,
UPD_prty_clm_insert.EndDate1 as EndDate1,
UPD_prty_clm_insert.in_PRTY_CNTCT_PROHIBITED_IND1 as in_PRTY_CNTCT_PROHIBITED_IND1,
UPD_prty_clm_insert.in_EMPLE_IND1 as in_EMPLE_IND1,
UPD_prty_clm_insert.in_PRCS_ID1 as in_PRCS_ID1,
UPD_prty_clm_insert.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
CASE WHEN UPD_prty_clm_insert.Retired1 != 0 THEN CURRENT_TIMESTAMP ELSE UPD_prty_clm_insert.EDW_END_DTTM1 END as o_EDW_END_DTTM,
UPD_prty_clm_insert.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM1,
UPD_prty_clm_insert.UNLISTD_OPRTR_IND1 as UNLISTD_OPRTR_IND1,
CASE WHEN UPD_prty_clm_insert.Retired1 != 0 THEN UPD_prty_clm_insert.TRANS_STRT_DTTM1 ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as o_TRANS_END_DTTM,
UPD_prty_clm_insert.RLTNSHP_TO_INSRD_TXT1 as RLTNSHP_TO_INSRD_TXT1,
UPD_prty_clm_insert.source_record_id
FROM
UPD_prty_clm_insert
);


-- Component tgt_prty_clm_UpdInsert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_CLM
(
CLM_ID,
PRTY_CLM_ROLE_CD,
PRTY_CLM_STRT_DTTM,
PRTY_ID,
PRTY_CLM_END_DTTM,
UNLISTD_OPRTR_IND,
PRTY_CNTCT_PROHIBITED_IND,
EMPLE_IND,
RLTNSHP_TO_INSRD_TXT,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
fil_prty_clm_upd_ins.in_CLM_ID3 as CLM_ID,
fil_prty_clm_upd_ins.in_PRTY_CLM_ROLE_CD3 as PRTY_CLM_ROLE_CD,
fil_prty_clm_upd_ins.out_AssignmentDate as PRTY_CLM_STRT_DTTM,
fil_prty_clm_upd_ins.in_PRTY_ID3 as PRTY_ID,
fil_prty_clm_upd_ins.EndDate3 as PRTY_CLM_END_DTTM,
fil_prty_clm_upd_ins.UNLISTD_OPRTR_IND3 as UNLISTD_OPRTR_IND,
fil_prty_clm_upd_ins.in_PRTY_CNTCT_PROHIBITED_IND3 as PRTY_CNTCT_PROHIBITED_IND,
fil_prty_clm_upd_ins.in_EMPLE_IND3 as EMPLE_IND,
fil_prty_clm_upd_ins.RLTNSHP_TO_INSRD_TXT3 as RLTNSHP_TO_INSRD_TXT,
fil_prty_clm_upd_ins.in_PRCS_ID3 as PRCS_ID,
fil_prty_clm_upd_ins.EDW_STRT_DTTM3 as EDW_STRT_DTTM,
fil_prty_clm_upd_ins.EDW_END_DTTM3 as EDW_END_DTTM,
fil_prty_clm_upd_ins.TRANS_STRT_DTTM3 as TRANS_STRT_DTTM
FROM
fil_prty_clm_upd_ins;


-- Component tgt_prty_clm_NewInsert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_CLM
(
CLM_ID,
PRTY_CLM_ROLE_CD,
PRTY_CLM_STRT_DTTM,
PRTY_ID,
PRTY_CLM_END_DTTM,
UNLISTD_OPRTR_IND,
PRTY_CNTCT_PROHIBITED_IND,
EMPLE_IND,
RLTNSHP_TO_INSRD_TXT,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_prty_clm_insert.in_CLM_ID1 as CLM_ID,
exp_prty_clm_insert.in_PRTY_CLM_ROLE_CD1 as PRTY_CLM_ROLE_CD,
exp_prty_clm_insert.out_AssignmentDate as PRTY_CLM_STRT_DTTM,
exp_prty_clm_insert.in_PRTY_ID1 as PRTY_ID,
exp_prty_clm_insert.EndDate1 as PRTY_CLM_END_DTTM,
exp_prty_clm_insert.UNLISTD_OPRTR_IND1 as UNLISTD_OPRTR_IND,
exp_prty_clm_insert.in_PRTY_CNTCT_PROHIBITED_IND1 as PRTY_CNTCT_PROHIBITED_IND,
exp_prty_clm_insert.in_EMPLE_IND1 as EMPLE_IND,
exp_prty_clm_insert.RLTNSHP_TO_INSRD_TXT1 as RLTNSHP_TO_INSRD_TXT,
exp_prty_clm_insert.in_PRCS_ID1 as PRCS_ID,
exp_prty_clm_insert.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_prty_clm_insert.o_EDW_END_DTTM as EDW_END_DTTM,
exp_prty_clm_insert.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_prty_clm_insert.o_TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_prty_clm_insert;


-- Component exp_prty_clm_upd_update, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prty_clm_upd_update AS
(
SELECT
fil_prty_clm_upd_update.lkp_CLM_ID1 as lkp_CLM_ID1,
fil_prty_clm_upd_update.lkp_PRTY_CLM_ROLE_CD1 as lkp_PRTY_CLM_ROLE_CD1,
fil_prty_clm_upd_update.lkp_PRTY_ID1 as lkp_PRTY_ID1,
fil_prty_clm_upd_update.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM1,
CASE WHEN fil_prty_clm_upd_update.Retired3 != 0 THEN CURRENT_TIMESTAMP ELSE fil_prty_clm_upd_update.EDW_END_DTTM_exp3 END as o_EDW_END_DTTM,
fil_prty_clm_upd_update.lkp_PRTY_CLM_STRT_DTTM4 as lkp_PRTY_CLM_STRT_DTTM4,
fil_prty_clm_upd_update.lkp_PRTY_CLM_END_DTTM as lkp_PRTY_CLM_END_DTTM,
fil_prty_clm_upd_update.lkp_TRANS_STRT_DTTM3 as lkp_TRANS_STRT_DTTM3,
DATEADD(
  ''second'',
  -1,
  fil_prty_clm_upd_update.TRANS_STRT_DTTM3
) AS o_TRANS_END_DTTM,
fil_prty_clm_upd_update.source_record_id
FROM
fil_prty_clm_upd_update
);


-- Component exp_prty_clm_Retire_Reject, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prty_clm_Retire_Reject AS
(
SELECT
upd_prty_clm_Update_Retire_Reject.lkp_CLM_ID1 as lkp_CLM_ID1,
upd_prty_clm_Update_Retire_Reject.lkp_PRTY_CLM_ROLE_CD1 as lkp_PRTY_CLM_ROLE_CD1,
upd_prty_clm_Update_Retire_Reject.lkp_PRTY_ID1 as lkp_PRTY_ID1,
upd_prty_clm_Update_Retire_Reject.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM1,
CURRENT_TIMESTAMP as o_EndDate,
upd_prty_clm_Update_Retire_Reject.lkp_PRTY_CLM_STRT_DTTM4 as lkp_PRTY_CLM_STRT_DTTM4,
upd_prty_clm_Update_Retire_Reject.lkp_PRTY_CLM_END_DTTM as lkp_PRTY_CLM_END_DTTM,
upd_prty_clm_Update_Retire_Reject.lkp_TRANS_STRT_DTTM3 as lkp_TRANS_STRT_DTTM3,
upd_prty_clm_Update_Retire_Reject.TRANS_STRT_DTTM4 as o_TRANS_END_DTTM,
upd_prty_clm_Update_Retire_Reject.source_record_id
FROM
upd_prty_clm_Update_Retire_Reject
);


-- Component tgt_prty_clm_Update, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_CLM
USING exp_prty_clm_upd_update ON (PRTY_CLM.CLM_ID = exp_prty_clm_upd_update.lkp_CLM_ID1 AND PRTY_CLM.PRTY_CLM_ROLE_CD = exp_prty_clm_upd_update.lkp_PRTY_CLM_ROLE_CD1 AND PRTY_CLM.PRTY_CLM_STRT_DTTM = exp_prty_clm_upd_update.lkp_PRTY_CLM_STRT_DTTM4 AND PRTY_CLM.PRTY_ID = exp_prty_clm_upd_update.lkp_PRTY_ID1 AND PRTY_CLM.PRTY_CLM_END_DTTM = exp_prty_clm_upd_update.lkp_PRTY_CLM_END_DTTM AND PRTY_CLM.EDW_STRT_DTTM = exp_prty_clm_upd_update.lkp_EDW_STRT_DTTM1 AND PRTY_CLM.TRANS_STRT_DTTM = exp_prty_clm_upd_update.lkp_TRANS_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
CLM_ID = exp_prty_clm_upd_update.lkp_CLM_ID1,
PRTY_CLM_ROLE_CD = exp_prty_clm_upd_update.lkp_PRTY_CLM_ROLE_CD1,
PRTY_CLM_STRT_DTTM = exp_prty_clm_upd_update.lkp_PRTY_CLM_STRT_DTTM4,
PRTY_ID = exp_prty_clm_upd_update.lkp_PRTY_ID1,
PRTY_CLM_END_DTTM = exp_prty_clm_upd_update.lkp_PRTY_CLM_END_DTTM,
EDW_STRT_DTTM = exp_prty_clm_upd_update.lkp_EDW_STRT_DTTM1,
EDW_END_DTTM = exp_prty_clm_upd_update.o_EDW_END_DTTM,
TRANS_STRT_DTTM = exp_prty_clm_upd_update.lkp_TRANS_STRT_DTTM3,
TRANS_END_DTTM = exp_prty_clm_upd_update.o_TRANS_END_DTTM;


-- Component tgt_prty_clm_Update_Retire_Reject, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_CLM
USING exp_prty_clm_Retire_Reject ON (PRTY_CLM.CLM_ID = exp_prty_clm_Retire_Reject.lkp_CLM_ID1 AND PRTY_CLM.PRTY_CLM_ROLE_CD = exp_prty_clm_Retire_Reject.lkp_PRTY_CLM_ROLE_CD1 AND PRTY_CLM.PRTY_CLM_STRT_DTTM = exp_prty_clm_Retire_Reject.lkp_PRTY_CLM_STRT_DTTM4 AND PRTY_CLM.PRTY_ID = exp_prty_clm_Retire_Reject.lkp_PRTY_ID1 AND PRTY_CLM.PRTY_CLM_END_DTTM = exp_prty_clm_Retire_Reject.lkp_PRTY_CLM_END_DTTM AND PRTY_CLM.EDW_STRT_DTTM = exp_prty_clm_Retire_Reject.lkp_EDW_STRT_DTTM1 AND PRTY_CLM.TRANS_STRT_DTTM = exp_prty_clm_Retire_Reject.lkp_TRANS_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
CLM_ID = exp_prty_clm_Retire_Reject.lkp_CLM_ID1,
PRTY_CLM_ROLE_CD = exp_prty_clm_Retire_Reject.lkp_PRTY_CLM_ROLE_CD1,
PRTY_CLM_STRT_DTTM = exp_prty_clm_Retire_Reject.lkp_PRTY_CLM_STRT_DTTM4,
PRTY_ID = exp_prty_clm_Retire_Reject.lkp_PRTY_ID1,
PRTY_CLM_END_DTTM = exp_prty_clm_Retire_Reject.lkp_PRTY_CLM_END_DTTM,
EDW_STRT_DTTM = exp_prty_clm_Retire_Reject.lkp_EDW_STRT_DTTM1,
EDW_END_DTTM = exp_prty_clm_Retire_Reject.o_EndDate,
TRANS_STRT_DTTM = exp_prty_clm_Retire_Reject.lkp_TRANS_STRT_DTTM3,
TRANS_END_DTTM = exp_prty_clm_Retire_Reject.o_TRANS_END_DTTM;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_prty_clm_x1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_prty_clm_x1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as SQ_CLM_ID,
$2 as SQ_PRTY_CLM_ROLE_CD,
$3 as SQ_out_AssignmentDate,
$4 as SQ_PRTY_ID,
$5 as SQ_PRTY_CLM_END_DTTM,
$6 as SQ_PRTY_CNTCT_PROHIBITED_IND,
$7 as SQ_EMPLE_IND,
$8 as TGT_CLM_ID,
$9 as TGT_PRTY_CLM_ROLE_CD,
$10 as TGT_PRTY_CLM_STRT_DTTM,
$11 as TGT_PRTY_ID,
$12 as TGT_EDW_STRT_DTTM,
$13 as SOURCE_DATA,
$14 as TARGET_DATA,
$15 as INS_UPD_FLAG,
$16 as TGT_PRTY_CNTCT_PROHIBITED_IND,
$17 as TGT_EMPLE_IND,
$18 as SRC_TRANS_STRT_DTTM,
$19 as TGT_TRANS_STRT_DTTM,
$20 as TGT_PRTY_CLM_END_DTTM,
$21 as TGT_EDW_END_DTTM,
$22 as SQ_ENDDATE,
$23 as SQ_RETIRED,
$24 as SQ_UNLISTD_OPRTR_IND,
$25 as SQ_RLTNSHP_TO_INSRD_TXT,
$26 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT 

SQ.in_CLM_ID AS SQ_CLM_ID,

SQ.in_PRTY_CLM_ROLE_CD AS SQ_PRTY_CLM_ROLE_CD ,

SQ.out_AssignmentDate AS SQ_out_AssignmentDate,

SQ.in_PRTY_ID AS SQ_PRTY_ID,

SQ.in_PRTY_CLM_END_DTTM AS SQ_PRTY_CLM_END_DTTM ,

SQ.in_PRTY_CNTCT_PROHIBITED_IND AS SQ_PRTY_CNTCT_PROHIBITED_IND ,

SQ.in_EMPLE_IND AS SQ_EMPLE_IND,

LKP_TGT.CLM_ID AS TGT_CLM_ID,

LKP_TGT.PRTY_CLM_ROLE_CD AS TGT_PRTY_CLM_ROLE_CD,

LKP_TGT.PRTY_CLM_STRT_DTTM AS TGT_PRTY_CLM_STRT_DTTM,

LKP_TGT.PRTY_ID AS TGT_PRTY_ID,

LKP_TGT.EDW_STRT_DTTM AS TGT_EDW_STRT_DTTM,

/* ----/*INS_UPD FLAG*/-------------- */
/*SOURCE DATA*/                                 

CAST(CONCAT(COALESCE(TRIM(CAST(TO_CHAR(SQ_out_AssignmentDate) as varchar(100))),'''')	,

COALESCE(TRIM(CAST(TO_CHAR(SQ_PRTY_CLM_END_DTTM) as varchar(100))),'''')	,								

COALESCE(TRIM(CAST(SQ_PRTY_CNTCT_PROHIBITED_IND as varchar(100))),''''),									

COALESCE(TRIM(CAST(SQ_EMPLE_IND as varchar(100))),''''),		

--COALESCE(TRIM(CAST(SQ_UNLISTD_OPRTR_IND as varchar(100))),'''') SQ_UNLISTD_OPRTR_IND,	
--COALESCE(TRIM(CAST(SQ_RLTNSHP_TO_INSRD_TXT as varchar(100))),''''),	

COALESCE(TRIM(CAST(SQ_PRTY_ID as varchar(100))),'''')) as VARCHAR(1000)) as  SOURCE_DATA,

/*TARGET DATA*/                                 

CAST(CONCAT(COALESCE(TRIM(CAST(TO_CHAR(TGT_PRTY_CLM_STRT_DTTM) as varchar(100))),'''')	,

--COALESCE(TRIM(CAST(TO_CHAR(TGT_PRTY_CLM_END_DTTM) as varchar(100))),'''')	,								

--COALESCE(TRIM(CAST(TGT_PRTY_CNTCT_PROHIBITED_IND as varchar(100))),''''),									

--COALESCE(TRIM(CAST(TGT_EMPLE_IND as varchar(100))),''''),		

--COALESCE(TRIM(CAST(LKP_TGT.UNLISTD_OPRTR_IND as varchar(100))),''''),	

--COALESCE(TRIM(CAST(LKP_TGT.RLTNSHP_TO_INSRD_TXT as varchar(100))),''''),	

COALESCE(TRIM(CAST(TGT_PRTY_ID as varchar(100))),'''')) as VARCHAR(1000)) as  TARGET_DATA,

/*FLAG*/

CASE                                    

WHEN LENGTH(TARGET_DATA) =0  THEN ''I''                                  

WHEN TRIM(SOURCE_DATA) =TRIM( TARGET_DATA) THEN ''R''                                    

WHEN TRIM(TARGET_DATA) <> TRIM(SOURCE_DATA) THEN ''U'' END AS INS_UPD_FLAG,       

LKP_TGT.PRTY_CNTCT_PROHIBITED_IND AS TGT_PRTY_CNTCT_PROHIBITED_IND,

LKP_TGT.EMPLE_IND AS TGT_EMPLE_IND,

SQ.in_TRANS_STRT_DTTM,

LKP_TGT.TRANS_STRT_DTTM AS TGT_TRANS_STRT_DTTM,

LKP_TGT.PRTY_CLM_END_DTTM AS TGT_PRTY_CLM_END_DTTM,

LKP_TGT.EDW_END_DTTM AS TGT_EDW_END_DTTM,

SQ.ENDDATE AS SQ_ENDDATE,

SQ.RETIRED AS SQ_RETIRED,

CASE WHEN (SQ.in_PRTY_CLM_ROLE_CD= ''CVDPTY''  OR  SQ_PRTY_CLM_ROLE_CD=''INSURD'') THEN  ''1'' ELSE ''0'' END AS SQ_UNLISTD_OPRTR_IND,

SQ.in_RLTNSHP_TO_INSRD_TXT AS SQ_RLTNSHP_TO_INSRD_TXT



FROM((

SELECT LTRIM(RTRIM(src.typecode_stg)) AS var_ContactroleTypecode,

CASE WHEN (var_ContactroleTypecode NOT LIKE ''%_%'' OR var_ContactroleTypecode IS NULL OR LENGTH(var_ContactroleTypecode)=0)

THEN ''UNK'' ELSE LKP_XLAT.TGT_IDNTFTN_VAL END var_Contactrole_Typecode2,

CASE WHEN var_Contactrole_Typecode2 IS NULL THEN ''UNK'' ELSE var_Contactrole_Typecode2 END out_ContactroleTypecode,

CASE WHEN SRC.Assignmentdate_stg IS NULL THEN CAST(CAST(''01-01-1900'' AS DATE ) as timestamp) ELSE SRC.Assignmentdate_stg END AS out_AssignmentDate,

LKP_CLM.CLM_ID AS in_CLM_ID,

out_ContactroleTypecode AS in_PRTY_CLM_ROLE_CD,

CASE WHEN UPPER(src.typecode_stg)=''ASSIGNED GROUP'' THEN LKP_INTRNL_ORG.INTRNL_ORG_PRTY_ID ELSE LKP_INDIV.INDIV_PRTY_ID END AS in_PRTY_ID,

CASE WHEN SRC.closedate_stg IS NULL THEN CAST(CAST(''01-01-1900'' AS DATE ) as timestamp) ELSE SRC.closedate_stg END AS in_PRTY_CLM_END_DTTM,

contactprohibited_stg AS in_PRTY_CNTCT_PROHIBITED_IND,

EMPLE_IND_stg AS in_EMPLE_IND,

SRC.UpdateTime_stg AS in_TRANS_STRT_DTTM,

CASE WHEN SRC.closedate_stg IS NULL THEN CAST(CAST(''01-01-1900'' AS DATE ) as timestamp) ELSE SRC.closedate_stg END AS ENDDATE,

SRC.RETIRED_STG AS RETIRED,

CASE WHEN UPPER(src.typecode_stg)=''ASSIGNED GROUP'' THEN NULL ELSE RLTNSHP_TO_INSRD_TXT_stg END AS in_RLTNSHP_TO_INSRD_TXT FROM 

(/* ---------------------------------------/*SOURCE QUERY STARTS*/----------------------------------------- */
select * from (

    SELECT 

    cc_claim.claimnumber_stg as claimnumber_stg ,

    ''PRTY_CLM_ROLE_TYPE1'' AS typecode_stg,

    cc_claim.Assignmentdate_stg as Assignmentdate_stg,

    cast(cc_contact.PublicID_stg as varchar(64)) as PublicID_stg,

    NULL as contactprohibited_stg,

    cast( NULL as varchar(3))  as EMPLE_IND_stg , 

    cast( NULL AS VARCHAR(50) ) as RLTNSHP_TO_INSRD_TXT_stg,

    ''SRC_SYS6'' AS SRC_CD_stg,

    cc_claim.UpdateTime_stg as UpdateTime_stg, 

     cast((''9999-12-31 23:59:59.999999'') AS TIMESTAMP(6))as closedate_stg,

    cc_claim.reporteddate_stg as  reporteddate_stg,

    case when cc_claim.Retired_stg=0 and cc_contact.Retired_stg=0 then 0 else 1 end as Retired_stg 

FROM 

    (select cc_claim.* from DB_T_PROD_STAG.cc_claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

    JOIN DB_T_PROD_STAG.cc_user ON cc_claim.AssignedUserID_stg = cc_user.id_stg

    JOIN DB_T_PROD_STAG.cc_contact ON cc_user.ContactID_stg=cc_contact.id_stg

    JOIN DB_T_PROD_STAG.cctl_contact ON cc_contact.Subtype_stg = cctl_contact.id_stg 

WHERE

    cctl_contact.name_stg in (''Person'',''Adjudicator'',''User Contact'',''Vendor (Person)'',''Attorney'', ''Doctor'',''Policy Person'',''Contact'')  

    AND cc_claim.UpdateTime_stg > (:start_dttm)

  and cc_claim.UpdateTime_stg <= (:end_dttm)

  

UNION 



SELECT 

 cc_claim.claimnumber_stg,

''PRTY_CLM_ROLE_TYPE5'' AS typecode_stg,

cc_claim.Updatetime_stg,

cast(cc_contact.PublicID_stg as varchar(64)) as PublicID_stg,

NULL as contactprohibited_stg,

cast( NULL as varchar(3))  as EMPLE_IND_stg , 

cast( NULL AS VARCHAR(50) ) as RLTNSHP_TO_INSRD_TXT_stg,

''SRC_SYS6'' AS SRC_CD_stg,

cc_claim.UpdateTime_stg,

 cast((''9999-12-31 23:59:59.999999'') AS TIMESTAMP(6))as closedate_stg,

cc_claim.reporteddate_stg,

case when cc_claim.Retired_stg=0 and cc_contact.Retired_stg=0 then 0 else 1 end as Retired_stg

FROM 

 (select cc_claim.* from DB_T_PROD_STAG.cc_claim inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

 JOIN DB_T_PROD_STAG.cc_user ON cc_user.id_stg = cc_claim.createuserid_stg

JOIN DB_T_PROD_STAG.cc_credential on cc_credential.id_stg = cc_user.CredentialID_stg  

 JOIN DB_T_PROD_STAG.cc_contact ON cc_user.ContactID_stg=cc_contact.id_stg

   WHERE cc_claim.UpdateTime_stg > (:start_dttm)

  and cc_claim.UpdateTime_stg <= (:end_dttm)

UNION 

/******************Claim to Assigned Group***********************/

    SELECT 

    cc_claim.ClaimNumber_stg,

   ''Assigned Group'' AS typecode_stg,

    cc_claim.Updatetime_stg,

cctl_grouptype.TYPECODE_stg,	/* --- Internal Organization Key */
    NULL as contactprohibited_stg,

    cast( NULL as varchar(3))  as EMPLE_IND_stg , 

    cc_group.Name_stg as RLTNSHP_TO_INSRD_TXT_stg,

	''SRC_SYS6'' AS SRC_CD_stg,

    cc_claim.UpdateTime_stg,

    cast((''9999-12-31 23:59:59.999999'') AS TIMESTAMP(6))as closedate_stg,

    cc_claim.reporteddate_stg,

    cc_claim.Retired_stg AS Retired_stg

FROM 

    (select cc_claim.* from DB_T_PROD_STAG.cc_claim 

	inner join DB_T_PROD_STAG.cctl_claimstate on cc_claim.State_stg= cctl_claimstate.id_stg 

	where cctl_claimstate.name_stg <> ''Draft'') cc_claim 

    INNER JOIN DB_T_PROD_STAG.cc_group ON cc_group.id_stg=cc_claim.AssignedGroupID_stg

    INNER JOIN DB_T_PROD_STAG.cctl_grouptype on cctl_grouptype.id_stg=cc_group.GroupType_stg

WHERE cc_claim.UpdateTime_stg > (:start_dttm)

   and cc_claim.UpdateTime_stg <= (:end_dttm)  )   as temp

QUALIFY ROW_NUMBER () OVER (partition by temp.ClaimNumber_stg,temp.TYPECODE_stg,temp.PublicID_stg order by temp.UpdateTime_stg desc)=1)SRC

/* LKP_XLAT_SRC_CD */
LEFT OUTER JOIN

(SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'')LKP_XLAT_SRC_CD ON LKP_XLAT_SRC_CD.SRC_IDNTFTN_VAL=SRC.SRC_CD_STG

/* LKP_CLM */
LEFT OUTER JOIN

(SELECT	CLM.CLM_ID as CLM_ID, CLM.CLM_TYPE_CD as CLM_TYPE_CD, CLM.CLM_MDIA_TYPE_CD as CLM_MDIA_TYPE_CD,

		CLM.CLM_SUBMTL_TYPE_CD as CLM_SUBMTL_TYPE_CD, CLM.ACDNT_TYPE_CD as ACDNT_TYPE_CD,

		CLM.CLM_CTGY_TYPE_CD as CLM_CTGY_TYPE_CD, CLM.ADDL_INSRNC_PLN_IND as ADDL_INSRNC_PLN_IND,

		CLM.EMPLMT_RLTD_IND as EMPLMT_RLTD_IND, CLM.ATTNY_INVLVMT_IND as ATTNY_INVLVMT_IND,

		CLM.CLM_PRIR_IND as CLM_PRIR_IND, CLM.PMT_MODE_CD as PMT_MODE_CD,

		CLM.CLM_OBLGTN_TYPE_CD as CLM_OBLGTN_TYPE_CD, CLM.SUBRGTN_ELGBL_CD as SUBRGTN_ELGBL_CD,

		CLM.SUBRGTN_ELGBLY_RSN_CD as SUBRGTN_ELGBLY_RSN_CD, CLM.CURY_CD as CURY_CD,

		CLM.INCDT_EV_ID as INCDT_EV_ID, CLM.INSRD_AT_FAULT_IND as INSRD_AT_FAULT_IND,

		CLM.CVGE_IN_QUES_IND as CVGE_IN_QUES_IND, CLM.EXTNT_OF_FIRE_DMG_TYPE_CD as EXTNT_OF_FIRE_DMG_TYPE_CD,

		CLM.VFYD_CLM_IND as VFYD_CLM_IND, CLM.PRCS_ID as PRCS_ID, CLM.CLM_STRT_DTTM as CLM_STRT_DTTM,

		CLM.CLM_END_DTTM as CLM_END_DTTM, CLM.EDW_STRT_DTTM as EDW_STRT_DTTM,

		CLM.EDW_END_DTTM as EDW_END_DTTM, CLM.TRANS_STRT_DTTM as TRANS_STRT_DTTM,

		CLM.LGCY_CLM_NUM as LGCY_CLM_NUM, CLM.CLM_NUM as CLM_NUM, CLM.SRC_SYS_CD as SRC_SYS_CD 

FROM	DB_T_PROD_CORE.CLM  

QUALIFY	ROW_NUMBER() OVER(

PARTITION BY CLM.CLM_NUM,CLM.SRC_SYS_CD  

ORDER BY CLM.EDW_END_DTTM desc) = 1)LKP_CLM ON LKP_CLM.CLM_NUM=SRC.CLAIMNUMBER_STG AND LKP_CLM.SRC_SYS_CD=LKP_XLAT_SRC_CD.TGT_IDNTFTN_VAL

/* LKP_INTRNL_ORG_TYPE */
LEFT OUTER JOIN

(SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INTRNL_ORG_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'')LKP_INTRNL_ORG_TYPE ON LKP_INTRNL_ORG_TYPE.SRC_IDNTFTN_VAL=''INTRNL_ORG_TYPE15''

/* LKP_XLAT */
LEFT OUTER JOIN

(SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_CLM_ROLE_TYPE'' 

       AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''cctl_claimanttype.typecode'',''cctl_contactrole.typecode'',''derived'',''cctl_userrole.typecode'',''cctl_losspartytype.typecode'')

AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''GW'' ,''DS'')/* AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' */
	qualify	row_number() over(partition by SRC_IDNTFTN_VAL 

order by case when (SRC_IDNTFTN_VAL  in (''primaryadjuster'',''insured'')) then tgt_idntftn_val  else 1  end desc,tgt_idntftn_val asc)=1

	)LKP_XLAT ON LKP_XLAT.SRC_IDNTFTN_VAL =var_ContactroleTypecode

/* -LKP_INTRNL_ORG  */
LEFT OUTER JOIN

(SELECT	INTRNL_ORG.INTRNL_ORG_PRTY_ID as INTRNL_ORG_PRTY_ID, INTRNL_ORG.INTRNL_ORG_TYPE_CD as INTRNL_ORG_TYPE_CD,

		INTRNL_ORG.INTRNL_ORG_SBTYPE_CD as INTRNL_ORG_SBTYPE_CD, INTRNL_ORG.INTRNL_ORG_NUM as INTRNL_ORG_NUM,

		INTRNL_ORG.SRC_SYS_CD as SRC_SYS_CD 

FROM	DB_T_PROD_CORE.INTRNL_ORG 

 qualify row_number () over (partition by INTRNL_ORG_NUM,INTRNL_ORG_TYPE_CD,INTRNL_ORG_SBTYPE_CD,SRC_SYS_CD order by EDW_END_DTTM desc)=1

 )LKP_INTRNL_ORG ON LKP_INTRNL_ORG.INTRNL_ORG_TYPE_CD=LKP_INTRNL_ORG_TYPE.TGT_IDNTFTN_VAL AND LKP_INTRNL_ORG.INTRNL_ORG_NUM=SRC.RLTNSHP_TO_INSRD_TXT_stg

 AND LKP_INTRNL_ORG.SRC_SYS_CD=LKP_XLAT_SRC_CD.TGT_IDNTFTN_VAL

/* -LKP_INDIV_CLM_CTR */
LEFT OUTER JOIN(

SELECT 

	INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID, 

	INDIV.NK_PUBLC_ID as NK_PUBLC_ID 

FROM 

	DB_T_PROD_CORE.INDIV

WHERE

	INDIV.NK_PUBLC_ID IS NOT NULL

	qualify row_number() over(partition by NK_PUBLC_ID order by edw_end_dttm desc,edw_strt_dttm desc)=1)LKP_INDIV 

	ON LKP_INDIV.NK_PUBLC_ID=SRC.PublicID_stg

	)SQ

	)

/* --------------------------------------/*SOURCE QUERY ENDS*/--------------------------------------- */
/* -LKP_TGT */
 LEFT OUTER JOIN

 (SELECT	PRTY_CLM.PRTY_CLM_STRT_DTTM AS PRTY_CLM_STRT_DTTM, PRTY_CLM.PRTY_CLM_END_DTTM AS PRTY_CLM_END_DTTM,

		PRTY_CLM.UNLISTD_OPRTR_IND AS UNLISTD_OPRTR_IND, PRTY_CLM.PRTY_CNTCT_PROHIBITED_IND AS PRTY_CNTCT_PROHIBITED_IND,

		PRTY_CLM.EMPLE_IND AS EMPLE_IND, PRTY_CLM.RLTNSHP_TO_INSRD_TXT AS RLTNSHP_TO_INSRD_TXT,

		PRTY_CLM.EDW_STRT_DTTM AS EDW_STRT_DTTM, PRTY_CLM.EDW_END_DTTM AS EDW_END_DTTM,

		PRTY_CLM.TRANS_STRT_DTTM AS TRANS_STRT_DTTM, PRTY_CLM.CLM_ID AS CLM_ID,

		PRTY_CLM.PRTY_CLM_ROLE_CD AS PRTY_CLM_ROLE_CD, PRTY_CLM.PRTY_ID AS PRTY_ID 

FROM	DB_T_PROD_CORE.PRTY_CLM 

QUALIFY	ROW_NUMBER() OVER(

PARTITION BY  PRTY_CLM.CLM_ID,PRTY_CLM.PRTY_CLM_ROLE_CD 

ORDER BY PRTY_CLM.EDW_END_DTTM DESC) = 1)LKP_TGT ON LKP_TGT.CLM_ID=SQ.in_CLM_ID AND LKP_TGT.PRTY_CLM_ROLE_CD=SQ.in_PRTY_CLM_ROLE_CD
) SRC
)
);


-- Component exp_all_source1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_all_source1 AS
(
SELECT
SQ_prty_clm_x1.SQ_CLM_ID as SQ_CLM_ID,
SQ_prty_clm_x1.SQ_PRTY_CLM_ROLE_CD as SQ_PRTY_CLM_ROLE_CD,
SQ_prty_clm_x1.SQ_out_AssignmentDate as SQ_out_AssignmentDate,
SQ_prty_clm_x1.SQ_PRTY_ID as SQ_PRTY_ID,
SQ_prty_clm_x1.SQ_PRTY_CLM_END_DTTM as SQ_PRTY_CLM_END_DTTM,
:PRCS_ID as PRCS_ID,
SQ_prty_clm_x1.SQ_PRTY_CNTCT_PROHIBITED_IND as SQ_PRTY_CNTCT_PROHIBITED_IND,
SQ_prty_clm_x1.SQ_EMPLE_IND as SQ_EMPLE_IND,
SQ_prty_clm_x1.TGT_CLM_ID as TGT_CLM_ID,
SQ_prty_clm_x1.TGT_PRTY_CLM_ROLE_CD as TGT_PRTY_CLM_ROLE_CD,
SQ_prty_clm_x1.TGT_PRTY_CLM_STRT_DTTM as TGT_PRTY_CLM_STRT_DTTM,
SQ_prty_clm_x1.TGT_PRTY_ID as TGT_PRTY_ID,
SQ_prty_clm_x1.TGT_EDW_STRT_DTTM as TGT_EDW_STRT_DTTM,
SQ_prty_clm_x1.SQ_ENDDATE as SQ_ENDDATE,
SQ_prty_clm_x1.INS_UPD_FLAG as INS_UPD_FLAG,
SQ_prty_clm_x1.TGT_PRTY_CNTCT_PROHIBITED_IND as TGT_PRTY_CNTCT_PROHIBITED_IND,
SQ_prty_clm_x1.TGT_EMPLE_IND as TGT_EMPLE_IND,
SQ_prty_clm_x1.TGT_TRANS_STRT_DTTM as TGT_TRANS_STRT_DTTM,
SQ_prty_clm_x1.TGT_PRTY_CLM_END_DTTM as TGT_PRTY_CLM_END_DTTM,
SQ_prty_clm_x1.TGT_EDW_END_DTTM as TGT_EDW_END_DTTM,
SQ_prty_clm_x1.SQ_RETIRED as SQ_RETIRED,
SQ_prty_clm_x1.SQ_UNLISTD_OPRTR_IND as SQ_UNLISTD_OPRTR_IND,
SQ_prty_clm_x1.SQ_RLTNSHP_TO_INSRD_TXT as SQ_RLTNSHP_TO_INSRD_TXT,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_TIMESTAMP_NTZ (''9999-12-31 23:59:59.999999'') AS EDW_END_DTTM,
DATEADD (''second'', -1, CURRENT_TIMESTAMP()) AS EDW_END_DTTM_exp,
COALESCE(
  SQ_prty_clm_x1.SRC_TRANS_STRT_DTTM,
  TO_DATE (''1900-01-01'')
) AS o_updatetime,
SQ_prty_clm_x1.source_record_id
FROM
SQ_prty_clm_x1
);


-- Component rtr_Ins_Upd1_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE rtr_Ins_Upd1_Insert AS
(SELECT
exp_all_source1.SQ_CLM_ID as in_CLM_ID,
exp_all_source1.SQ_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD,
NULL as out_AssignmentDate,
exp_all_source1.SQ_PRTY_ID as in_PRTY_ID,
exp_all_source1.SQ_PRTY_CLM_END_DTTM as in_PRTY_CLM_END_DTTM,
exp_all_source1.PRCS_ID as in_PRCS_ID,
exp_all_source1.SQ_PRTY_CNTCT_PROHIBITED_IND as in_PRTY_CNTCT_PROHIBITED_IND,
exp_all_source1.SQ_EMPLE_IND as in_EMPLE_IND,
exp_all_source1.TGT_CLM_ID as lkp_CLM_ID,
exp_all_source1.TGT_PRTY_CLM_ROLE_CD as lkp_PRTY_CLM_ROLE_CD,
exp_all_source1.TGT_PRTY_CLM_STRT_DTTM as lkp_PRTY_CLM_STRT_DTTM,
exp_all_source1.TGT_PRTY_ID as lkp_PRTY_ID,
exp_all_source1.TGT_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
NULL as lkp_PRCS_ID,
exp_all_source1.SQ_out_AssignmentDate as out_AssignmentDate1,
exp_all_source1.SQ_ENDDATE as EndDate,
exp_all_source1.INS_UPD_FLAG as o_Ins_Upd,
exp_all_source1.TGT_PRTY_CNTCT_PROHIBITED_IND as lkp_PRTY_CNTCT_PROHIBITED_IND,
exp_all_source1.TGT_EMPLE_IND as lkp_EMPLE_IND,
exp_all_source1.o_updatetime as TRANS_STRT_DTTM,
exp_all_source1.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_all_source1.EDW_END_DTTM as EDW_END_DTTM,
exp_all_source1.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_all_source1.TGT_TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM,
exp_all_source1.TGT_PRTY_CLM_STRT_DTTM as lkp_PRTY_CLM_STRT_DTTM4,
exp_all_source1.TGT_PRTY_CLM_END_DTTM as lkp_PRTY_CLM_END_DTTM,
exp_all_source1.TGT_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_all_source1.SQ_RETIRED as Retired,
exp_all_source1.SQ_UNLISTD_OPRTR_IND as UNLISTD_OPRTR_IND,
exp_all_source1.SQ_RLTNSHP_TO_INSRD_TXT as RLTNSHP_TO_INSRD_TXT,
exp_all_source1.source_record_id
FROM
exp_all_source1
WHERE ( ( exp_all_source1.INS_UPD_FLAG = ''I'' ) OR ( exp_all_source1.SQ_RETIRED = 0 AND exp_all_source1.TGT_EDW_END_DTTM != TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) ) AND exp_all_source1.SQ_CLM_ID IS NOT NULL AND exp_all_source1.SQ_PRTY_ID IS NOT NULL);


-- Component rtr_Ins_Upd1_Retire, Type ROUTER Output Group Retire
CREATE OR REPLACE TEMPORARY TABLE rtr_Ins_Upd1_Retire AS
(SELECT
exp_all_source1.SQ_CLM_ID as in_CLM_ID,
exp_all_source1.SQ_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD,
NULL as out_AssignmentDate,
exp_all_source1.SQ_PRTY_ID as in_PRTY_ID,
exp_all_source1.SQ_PRTY_CLM_END_DTTM as in_PRTY_CLM_END_DTTM,
exp_all_source1.PRCS_ID as in_PRCS_ID,
exp_all_source1.SQ_PRTY_CNTCT_PROHIBITED_IND as in_PRTY_CNTCT_PROHIBITED_IND,
exp_all_source1.SQ_EMPLE_IND as in_EMPLE_IND,
exp_all_source1.TGT_CLM_ID as lkp_CLM_ID,
exp_all_source1.TGT_PRTY_CLM_ROLE_CD as lkp_PRTY_CLM_ROLE_CD,
exp_all_source1.TGT_PRTY_CLM_STRT_DTTM as lkp_PRTY_CLM_STRT_DTTM,
exp_all_source1.TGT_PRTY_ID as lkp_PRTY_ID,
exp_all_source1.TGT_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
NULL as lkp_PRCS_ID,
exp_all_source1.SQ_out_AssignmentDate as out_AssignmentDate1,
exp_all_source1.SQ_ENDDATE as EndDate,
exp_all_source1.INS_UPD_FLAG as o_Ins_Upd,
exp_all_source1.TGT_PRTY_CNTCT_PROHIBITED_IND as lkp_PRTY_CNTCT_PROHIBITED_IND,
exp_all_source1.TGT_EMPLE_IND as lkp_EMPLE_IND,
exp_all_source1.o_updatetime as TRANS_STRT_DTTM,
exp_all_source1.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_all_source1.EDW_END_DTTM as EDW_END_DTTM,
exp_all_source1.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_all_source1.TGT_TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM,
exp_all_source1.TGT_PRTY_CLM_STRT_DTTM as lkp_PRTY_CLM_STRT_DTTM4,
exp_all_source1.TGT_PRTY_CLM_END_DTTM as lkp_PRTY_CLM_END_DTTM,
exp_all_source1.TGT_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_all_source1.SQ_RETIRED as Retired,
exp_all_source1.SQ_UNLISTD_OPRTR_IND as UNLISTD_OPRTR_IND,
exp_all_source1.SQ_RLTNSHP_TO_INSRD_TXT as RLTNSHP_TO_INSRD_TXT,
exp_all_source1.source_record_id
FROM
exp_all_source1
WHERE exp_all_source1.INS_UPD_FLAG = ''R'' and exp_all_source1.SQ_RETIRED != 0 and exp_all_source1.TGT_EDW_END_DTTM = TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ));


-- Component rtr_Ins_Upd1_Update, Type ROUTER Output Group Update
CREATE OR REPLACE TEMPORARY TABLE rtr_Ins_Upd1_Update AS
(SELECT
exp_all_source1.SQ_CLM_ID as in_CLM_ID,
exp_all_source1.SQ_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD,
NULL as out_AssignmentDate,
exp_all_source1.SQ_PRTY_ID as in_PRTY_ID,
exp_all_source1.SQ_PRTY_CLM_END_DTTM as in_PRTY_CLM_END_DTTM,
exp_all_source1.PRCS_ID as in_PRCS_ID,
exp_all_source1.SQ_PRTY_CNTCT_PROHIBITED_IND as in_PRTY_CNTCT_PROHIBITED_IND,
exp_all_source1.SQ_EMPLE_IND as in_EMPLE_IND,
exp_all_source1.TGT_CLM_ID as lkp_CLM_ID,
exp_all_source1.TGT_PRTY_CLM_ROLE_CD as lkp_PRTY_CLM_ROLE_CD,
exp_all_source1.TGT_PRTY_CLM_STRT_DTTM as lkp_PRTY_CLM_STRT_DTTM,
exp_all_source1.TGT_PRTY_ID as lkp_PRTY_ID,
exp_all_source1.TGT_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
NULL as lkp_PRCS_ID,
exp_all_source1.SQ_out_AssignmentDate as out_AssignmentDate1,
exp_all_source1.SQ_ENDDATE as EndDate,
exp_all_source1.INS_UPD_FLAG as o_Ins_Upd,
exp_all_source1.TGT_PRTY_CNTCT_PROHIBITED_IND as lkp_PRTY_CNTCT_PROHIBITED_IND,
exp_all_source1.TGT_EMPLE_IND as lkp_EMPLE_IND,
exp_all_source1.o_updatetime as TRANS_STRT_DTTM,
exp_all_source1.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_all_source1.EDW_END_DTTM as EDW_END_DTTM,
exp_all_source1.EDW_END_DTTM_exp as EDW_END_DTTM_exp,
exp_all_source1.TGT_TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM,
exp_all_source1.TGT_PRTY_CLM_STRT_DTTM as lkp_PRTY_CLM_STRT_DTTM4,
exp_all_source1.TGT_PRTY_CLM_END_DTTM as lkp_PRTY_CLM_END_DTTM,
exp_all_source1.TGT_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_all_source1.SQ_RETIRED as Retired,
exp_all_source1.SQ_UNLISTD_OPRTR_IND as UNLISTD_OPRTR_IND,
exp_all_source1.SQ_RLTNSHP_TO_INSRD_TXT as RLTNSHP_TO_INSRD_TXT,
exp_all_source1.source_record_id
FROM
exp_all_source1
WHERE ( exp_all_source1.INS_UPD_FLAG = ''U'' AND exp_all_source1.TGT_EDW_END_DTTM = TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) AND exp_all_source1.SQ_CLM_ID IS NOT NULL AND exp_all_source1.SQ_PRTY_ID IS NOT NULL);


-- Component upd_prty_clm_Update1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prty_clm_Update1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_Ins_Upd1_Update.lkp_CLM_ID as lkp_CLM_ID1,
rtr_Ins_Upd1_Update.lkp_PRTY_CLM_ROLE_CD as lkp_PRTY_CLM_ROLE_CD1,
rtr_Ins_Upd1_Update.lkp_PRTY_CLM_STRT_DTTM as lkp_PRTY_CLM_STRT_DTTM3,
rtr_Ins_Upd1_Update.lkp_PRTY_ID as lkp_PRTY_ID1,
rtr_Ins_Upd1_Update.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM1,
rtr_Ins_Upd1_Update.lkp_PRCS_ID as lkp_PRCS_ID3,
rtr_Ins_Upd1_Update.EDW_END_DTTM_exp as EDW_END_DTTM_exp3,
NULL as lkp_PRTY_CNTCT_PROHIBITED_IND1,
NULL as lkp_EMPLE_IND1,
rtr_Ins_Upd1_Update.lkp_PRTY_CLM_STRT_DTTM4 as lkp_PRTY_CLM_STRT_DTTM4,
rtr_Ins_Upd1_Update.lkp_PRTY_CLM_END_DTTM as lkp_PRTY_CLM_END_DTTM,
rtr_Ins_Upd1_Update.lkp_TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM3,
rtr_Ins_Upd1_Update.lkp_PRTY_CNTCT_PROHIBITED_IND as lkp_PRTY_CNTCT_PROHIBITED_IND11,
rtr_Ins_Upd1_Update.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM3,
rtr_Ins_Upd1_Update.Retired as Retired3,
rtr_Ins_Upd1_Update.TRANS_STRT_DTTM as TRANS_STRT_DTTM3,
1 as UPDATE_STRATEGY_ACTION,
rtr_Ins_Upd1_Update.source_record_id
FROM
rtr_Ins_Upd1_Update
);


-- Component fil_prty_clm_upd_update1, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_prty_clm_upd_update1 AS
(
SELECT
upd_prty_clm_Update1.lkp_CLM_ID1 as lkp_CLM_ID1,
upd_prty_clm_Update1.lkp_PRTY_CLM_ROLE_CD1 as lkp_PRTY_CLM_ROLE_CD1,
upd_prty_clm_Update1.lkp_PRTY_CLM_STRT_DTTM3 as lkp_PRTY_CLM_STRT_DTTM3,
upd_prty_clm_Update1.lkp_PRTY_ID1 as lkp_PRTY_ID1,
upd_prty_clm_Update1.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM1,
upd_prty_clm_Update1.lkp_PRCS_ID3 as lkp_PRCS_ID3,
upd_prty_clm_Update1.EDW_END_DTTM_exp3 as EDW_END_DTTM_exp3,
upd_prty_clm_Update1.lkp_PRTY_CNTCT_PROHIBITED_IND1 as lkp_PRTY_CNTCT_PROHIBITED_IND1,
upd_prty_clm_Update1.lkp_EMPLE_IND1 as lkp_EMPLE_IND1,
upd_prty_clm_Update1.lkp_PRTY_CLM_STRT_DTTM4 as lkp_PRTY_CLM_STRT_DTTM4,
upd_prty_clm_Update1.lkp_PRTY_CLM_END_DTTM as lkp_PRTY_CLM_END_DTTM,
upd_prty_clm_Update1.lkp_TRANS_STRT_DTTM3 as lkp_TRANS_STRT_DTTM3,
upd_prty_clm_Update1.lkp_PRTY_CNTCT_PROHIBITED_IND11 as lkp_PRTY_CNTCT_PROHIBITED_IND11,
upd_prty_clm_Update1.lkp_EDW_END_DTTM3 as lkp_EDW_END_DTTM3,
upd_prty_clm_Update1.Retired3 as Retired3,
upd_prty_clm_Update1.TRANS_STRT_DTTM3 as TRANS_STRT_DTTM3,
upd_prty_clm_Update1.source_record_id
FROM
upd_prty_clm_Update1
WHERE upd_prty_clm_Update1.lkp_EDW_END_DTTM3 = TO_TIMESTAMP ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
);


-- Component upd_prty_clm_upd_ins1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prty_clm_upd_ins1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_Ins_Upd1_Update.in_CLM_ID as in_CLM_ID3,
rtr_Ins_Upd1_Update.in_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD3,
rtr_Ins_Upd1_Update.out_AssignmentDate1 as out_AssignmentDate,
rtr_Ins_Upd1_Update.in_PRTY_ID as in_PRTY_ID3,
rtr_Ins_Upd1_Update.EndDate as EndDate3,
rtr_Ins_Upd1_Update.in_PRTY_CNTCT_PROHIBITED_IND as in_PRTY_CNTCT_PROHIBITED_IND3,
rtr_Ins_Upd1_Update.in_EMPLE_IND as in_EMPLE_IND3,
rtr_Ins_Upd1_Update.in_PRCS_ID as in_PRCS_ID3,
rtr_Ins_Upd1_Update.EDW_STRT_DTTM as EDW_STRT_DTTM3,
rtr_Ins_Upd1_Update.EDW_END_DTTM as EDW_END_DTTM3,
rtr_Ins_Upd1_Update.TRANS_STRT_DTTM as TRANS_STRT_DTTM3,
rtr_Ins_Upd1_Update.Retired as Retired3,
rtr_Ins_Upd1_Update.UNLISTD_OPRTR_IND as UNLISTD_OPRTR_IND3,
rtr_Ins_Upd1_Update.RLTNSHP_TO_INSRD_TXT as RLTNSHP_TO_INSRD_TXT3,
0 as UPDATE_STRATEGY_ACTION,
rtr_Ins_Upd1_Update.source_record_id
FROM
rtr_Ins_Upd1_Update
);


-- Component upd_prty_clm_Update_Retire_Reject1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_prty_clm_Update_Retire_Reject1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_Ins_Upd1_Retire.lkp_CLM_ID as lkp_CLM_ID1,
rtr_Ins_Upd1_Retire.lkp_PRTY_CLM_ROLE_CD as lkp_PRTY_CLM_ROLE_CD1,
rtr_Ins_Upd1_Retire.lkp_PRTY_CLM_STRT_DTTM as lkp_PRTY_CLM_STRT_DTTM3,
rtr_Ins_Upd1_Retire.lkp_PRTY_ID as lkp_PRTY_ID1,
rtr_Ins_Upd1_Retire.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM1,
rtr_Ins_Upd1_Retire.lkp_PRCS_ID as lkp_PRCS_ID3,
NULL as lkp_PRTY_CNTCT_PROHIBITED_IND1,
NULL as lkp_EMPLE_IND1,
rtr_Ins_Upd1_Retire.EDW_END_DTTM_exp as EDW_END_DTTM_exp3,
rtr_Ins_Upd1_Retire.lkp_PRTY_CLM_STRT_DTTM4 as lkp_PRTY_CLM_STRT_DTTM4,
rtr_Ins_Upd1_Retire.lkp_PRTY_CLM_END_DTTM as lkp_PRTY_CLM_END_DTTM,
rtr_Ins_Upd1_Retire.lkp_TRANS_STRT_DTTM as lkp_TRANS_STRT_DTTM3,
rtr_Ins_Upd1_Retire.lkp_PRTY_CNTCT_PROHIBITED_IND as lkp_PRTY_CNTCT_PROHIBITED_IND11,
rtr_Ins_Upd1_Retire.TRANS_STRT_DTTM as TRANS_STRT_DTTM4,
1 as UPDATE_STRATEGY_ACTION,
rtr_Ins_Upd1_Retire.source_record_id
FROM
rtr_Ins_Upd1_Retire
);


-- Component exp_prty_clm_upd_update1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prty_clm_upd_update1 AS
(
SELECT
fil_prty_clm_upd_update1.lkp_CLM_ID1 as lkp_CLM_ID1,
fil_prty_clm_upd_update1.lkp_PRTY_CLM_ROLE_CD1 as lkp_PRTY_CLM_ROLE_CD1,
fil_prty_clm_upd_update1.lkp_PRTY_ID1 as lkp_PRTY_ID1,
fil_prty_clm_upd_update1.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM1,
CASE WHEN fil_prty_clm_upd_update1.Retired3 != 0 THEN CURRENT_TIMESTAMP ELSE fil_prty_clm_upd_update1.EDW_END_DTTM_exp3 END as o_EDW_END_DTTM,
fil_prty_clm_upd_update1.lkp_PRTY_CLM_STRT_DTTM4 as lkp_PRTY_CLM_STRT_DTTM4,
fil_prty_clm_upd_update1.lkp_PRTY_CLM_END_DTTM as lkp_PRTY_CLM_END_DTTM,
fil_prty_clm_upd_update1.lkp_TRANS_STRT_DTTM3 as lkp_TRANS_STRT_DTTM3,
DATEADD (
  SECOND,
  -1,
  fil_prty_clm_upd_update1.TRANS_STRT_DTTM3
) as o_TRANS_END_DTTM,
fil_prty_clm_upd_update1.source_record_id
FROM
fil_prty_clm_upd_update1
);


-- Component UPD_prty_clm_insert1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE UPD_prty_clm_insert1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_Ins_Upd1_Insert.in_CLM_ID as in_CLM_ID1,
rtr_Ins_Upd1_Insert.in_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD1,
rtr_Ins_Upd1_Insert.out_AssignmentDate1 as out_AssignmentDate,
rtr_Ins_Upd1_Insert.in_PRTY_ID as in_PRTY_ID1,
rtr_Ins_Upd1_Insert.EndDate as EndDate1,
rtr_Ins_Upd1_Insert.in_PRTY_CNTCT_PROHIBITED_IND as in_PRTY_CNTCT_PROHIBITED_IND1,
rtr_Ins_Upd1_Insert.in_EMPLE_IND as in_EMPLE_IND1,
rtr_Ins_Upd1_Insert.in_PRCS_ID as in_PRCS_ID1,
rtr_Ins_Upd1_Insert.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_Ins_Upd1_Insert.EDW_END_DTTM as EDW_END_DTTM1,
rtr_Ins_Upd1_Insert.TRANS_STRT_DTTM as TRANS_STRT_DTTM1,
rtr_Ins_Upd1_Insert.Retired as Retired1,
rtr_Ins_Upd1_Insert.UNLISTD_OPRTR_IND as UNLISTD_OPRTR_IND1,
rtr_Ins_Upd1_Insert.RLTNSHP_TO_INSRD_TXT as RLTNSHP_TO_INSRD_TXT1,
0 as UPDATE_STRATEGY_ACTION,
rtr_Ins_Upd1_Insert.source_record_id
FROM
rtr_Ins_Upd1_Insert
);


-- Component exp_prty_clm_insert1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prty_clm_insert1 AS
(
SELECT
UPD_prty_clm_insert1.in_CLM_ID1 as in_CLM_ID1,
UPD_prty_clm_insert1.in_PRTY_CLM_ROLE_CD1 as in_PRTY_CLM_ROLE_CD1,
UPD_prty_clm_insert1.out_AssignmentDate as out_AssignmentDate,
UPD_prty_clm_insert1.in_PRTY_ID1 as in_PRTY_ID1,
UPD_prty_clm_insert1.EndDate1 as EndDate1,
UPD_prty_clm_insert1.in_PRTY_CNTCT_PROHIBITED_IND1 as in_PRTY_CNTCT_PROHIBITED_IND1,
UPD_prty_clm_insert1.in_EMPLE_IND1 as in_EMPLE_IND1,
UPD_prty_clm_insert1.in_PRCS_ID1 as in_PRCS_ID1,
UPD_prty_clm_insert1.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
CASE WHEN UPD_prty_clm_insert1.Retired1 != 0 THEN CURRENT_TIMESTAMP ELSE UPD_prty_clm_insert1.EDW_END_DTTM1 END as o_EDW_END_DTTM,
UPD_prty_clm_insert1.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM1,
UPD_prty_clm_insert1.UNLISTD_OPRTR_IND1 as UNLISTD_OPRTR_IND1,
CASE WHEN UPD_prty_clm_insert1.Retired1 != 0 THEN UPD_prty_clm_insert1.TRANS_STRT_DTTM1 ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as o_TRANS_END_DTTM,
UPD_prty_clm_insert1.RLTNSHP_TO_INSRD_TXT1 as RLTNSHP_TO_INSRD_TXT1,
UPD_prty_clm_insert1.source_record_id
FROM
UPD_prty_clm_insert1
);


-- Component fil_prty_clm_upd_ins1, Type FILTER 
CREATE OR REPLACE TEMPORARY TABLE fil_prty_clm_upd_ins1 AS
(
SELECT
upd_prty_clm_upd_ins1.in_CLM_ID3 as in_CLM_ID3,
upd_prty_clm_upd_ins1.in_PRTY_CLM_ROLE_CD3 as in_PRTY_CLM_ROLE_CD3,
upd_prty_clm_upd_ins1.out_AssignmentDate as out_AssignmentDate,
upd_prty_clm_upd_ins1.in_PRTY_ID3 as in_PRTY_ID3,
upd_prty_clm_upd_ins1.EndDate3 as EndDate3,
upd_prty_clm_upd_ins1.in_PRTY_CNTCT_PROHIBITED_IND3 as in_PRTY_CNTCT_PROHIBITED_IND3,
upd_prty_clm_upd_ins1.in_EMPLE_IND3 as in_EMPLE_IND3,
upd_prty_clm_upd_ins1.in_PRCS_ID3 as in_PRCS_ID3,
upd_prty_clm_upd_ins1.EDW_STRT_DTTM3 as EDW_STRT_DTTM3,
upd_prty_clm_upd_ins1.EDW_END_DTTM3 as EDW_END_DTTM3,
upd_prty_clm_upd_ins1.TRANS_STRT_DTTM3 as TRANS_STRT_DTTM3,
upd_prty_clm_upd_ins1.Retired3 as Retired3,
upd_prty_clm_upd_ins1.UNLISTD_OPRTR_IND3 as UNLISTD_OPRTR_IND3,
upd_prty_clm_upd_ins1.RLTNSHP_TO_INSRD_TXT3 as RLTNSHP_TO_INSRD_TXT3,
upd_prty_clm_upd_ins1.source_record_id
FROM
upd_prty_clm_upd_ins1
WHERE upd_prty_clm_upd_ins1.Retired3 = 0
);


-- Component exp_prty_clm_Retire_Reject1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_prty_clm_Retire_Reject1 AS
(
SELECT
upd_prty_clm_Update_Retire_Reject1.lkp_CLM_ID1 as lkp_CLM_ID1,
upd_prty_clm_Update_Retire_Reject1.lkp_PRTY_CLM_ROLE_CD1 as lkp_PRTY_CLM_ROLE_CD1,
upd_prty_clm_Update_Retire_Reject1.lkp_PRTY_ID1 as lkp_PRTY_ID1,
upd_prty_clm_Update_Retire_Reject1.lkp_EDW_STRT_DTTM1 as lkp_EDW_STRT_DTTM1,
CURRENT_TIMESTAMP as o_EndDate,
upd_prty_clm_Update_Retire_Reject1.lkp_PRTY_CLM_STRT_DTTM4 as lkp_PRTY_CLM_STRT_DTTM4,
upd_prty_clm_Update_Retire_Reject1.lkp_PRTY_CLM_END_DTTM as lkp_PRTY_CLM_END_DTTM,
upd_prty_clm_Update_Retire_Reject1.lkp_TRANS_STRT_DTTM3 as lkp_TRANS_STRT_DTTM3,
upd_prty_clm_Update_Retire_Reject1.TRANS_STRT_DTTM4 as TRANS_STRT_DTTM4,
upd_prty_clm_Update_Retire_Reject1.source_record_id
FROM
upd_prty_clm_Update_Retire_Reject1
);


-- Component tgt_prty_clm_UpdInsert1, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_CLM
(
CLM_ID,
PRTY_CLM_ROLE_CD,
PRTY_CLM_STRT_DTTM,
PRTY_ID,
PRTY_CLM_END_DTTM,
UNLISTD_OPRTR_IND,
PRTY_CNTCT_PROHIBITED_IND,
EMPLE_IND,
RLTNSHP_TO_INSRD_TXT,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM
)
SELECT
fil_prty_clm_upd_ins1.in_CLM_ID3 as CLM_ID,
fil_prty_clm_upd_ins1.in_PRTY_CLM_ROLE_CD3 as PRTY_CLM_ROLE_CD,
fil_prty_clm_upd_ins1.out_AssignmentDate as PRTY_CLM_STRT_DTTM,
fil_prty_clm_upd_ins1.in_PRTY_ID3 as PRTY_ID,
fil_prty_clm_upd_ins1.EndDate3 as PRTY_CLM_END_DTTM,
fil_prty_clm_upd_ins1.UNLISTD_OPRTR_IND3 as UNLISTD_OPRTR_IND,
fil_prty_clm_upd_ins1.in_PRTY_CNTCT_PROHIBITED_IND3 as PRTY_CNTCT_PROHIBITED_IND,
fil_prty_clm_upd_ins1.in_EMPLE_IND3 as EMPLE_IND,
fil_prty_clm_upd_ins1.RLTNSHP_TO_INSRD_TXT3 as RLTNSHP_TO_INSRD_TXT,
fil_prty_clm_upd_ins1.in_PRCS_ID3 as PRCS_ID,
fil_prty_clm_upd_ins1.EDW_STRT_DTTM3 as EDW_STRT_DTTM,
fil_prty_clm_upd_ins1.EDW_END_DTTM3 as EDW_END_DTTM,
fil_prty_clm_upd_ins1.TRANS_STRT_DTTM3 as TRANS_STRT_DTTM
FROM
fil_prty_clm_upd_ins1;


-- Component tgt_prty_clm_NewInsert1, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_CLM
(
CLM_ID,
PRTY_CLM_ROLE_CD,
PRTY_CLM_STRT_DTTM,
PRTY_ID,
PRTY_CLM_END_DTTM,
UNLISTD_OPRTR_IND,
PRTY_CNTCT_PROHIBITED_IND,
EMPLE_IND,
RLTNSHP_TO_INSRD_TXT,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_prty_clm_insert1.in_CLM_ID1 as CLM_ID,
exp_prty_clm_insert1.in_PRTY_CLM_ROLE_CD1 as PRTY_CLM_ROLE_CD,
exp_prty_clm_insert1.out_AssignmentDate as PRTY_CLM_STRT_DTTM,
exp_prty_clm_insert1.in_PRTY_ID1 as PRTY_ID,
exp_prty_clm_insert1.EndDate1 as PRTY_CLM_END_DTTM,
exp_prty_clm_insert1.UNLISTD_OPRTR_IND1 as UNLISTD_OPRTR_IND,
exp_prty_clm_insert1.in_PRTY_CNTCT_PROHIBITED_IND1 as PRTY_CNTCT_PROHIBITED_IND,
exp_prty_clm_insert1.in_EMPLE_IND1 as EMPLE_IND,
exp_prty_clm_insert1.RLTNSHP_TO_INSRD_TXT1 as RLTNSHP_TO_INSRD_TXT,
exp_prty_clm_insert1.in_PRCS_ID1 as PRCS_ID,
exp_prty_clm_insert1.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_prty_clm_insert1.o_EDW_END_DTTM as EDW_END_DTTM,
exp_prty_clm_insert1.TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_prty_clm_insert1.o_TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_prty_clm_insert1;


-- Component tgt_prty_clm_Update1, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_CLM
USING exp_prty_clm_upd_update1 ON (PRTY_CLM.CLM_ID = exp_prty_clm_upd_update1.lkp_CLM_ID1 AND PRTY_CLM.PRTY_CLM_ROLE_CD = exp_prty_clm_upd_update1.lkp_PRTY_CLM_ROLE_CD1 AND PRTY_CLM.PRTY_CLM_STRT_DTTM = exp_prty_clm_upd_update1.lkp_PRTY_CLM_STRT_DTTM4 AND PRTY_CLM.PRTY_ID = exp_prty_clm_upd_update1.lkp_PRTY_ID1 AND PRTY_CLM.PRTY_CLM_END_DTTM = exp_prty_clm_upd_update1.lkp_PRTY_CLM_END_DTTM AND PRTY_CLM.EDW_STRT_DTTM = exp_prty_clm_upd_update1.lkp_EDW_STRT_DTTM1 AND PRTY_CLM.TRANS_STRT_DTTM = exp_prty_clm_upd_update1.lkp_TRANS_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
CLM_ID = exp_prty_clm_upd_update1.lkp_CLM_ID1,
PRTY_CLM_ROLE_CD = exp_prty_clm_upd_update1.lkp_PRTY_CLM_ROLE_CD1,
PRTY_CLM_STRT_DTTM = exp_prty_clm_upd_update1.lkp_PRTY_CLM_STRT_DTTM4,
PRTY_ID = exp_prty_clm_upd_update1.lkp_PRTY_ID1,
PRTY_CLM_END_DTTM = exp_prty_clm_upd_update1.lkp_PRTY_CLM_END_DTTM,
EDW_STRT_DTTM = exp_prty_clm_upd_update1.lkp_EDW_STRT_DTTM1,
EDW_END_DTTM = exp_prty_clm_upd_update1.o_EDW_END_DTTM,
TRANS_STRT_DTTM = exp_prty_clm_upd_update1.lkp_TRANS_STRT_DTTM3,
TRANS_END_DTTM = exp_prty_clm_upd_update1.o_TRANS_END_DTTM;


-- Component tgt_prty_clm_Update_Retire_Reject1, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_CLM
USING exp_prty_clm_Retire_Reject1 ON (PRTY_CLM.CLM_ID = exp_prty_clm_Retire_Reject1.lkp_CLM_ID1 AND PRTY_CLM.PRTY_CLM_ROLE_CD = exp_prty_clm_Retire_Reject1.lkp_PRTY_CLM_ROLE_CD1 AND PRTY_CLM.PRTY_CLM_STRT_DTTM = exp_prty_clm_Retire_Reject1.lkp_PRTY_CLM_STRT_DTTM4 AND PRTY_CLM.PRTY_ID = exp_prty_clm_Retire_Reject1.lkp_PRTY_ID1 AND PRTY_CLM.PRTY_CLM_END_DTTM = exp_prty_clm_Retire_Reject1.lkp_PRTY_CLM_END_DTTM AND PRTY_CLM.EDW_STRT_DTTM = exp_prty_clm_Retire_Reject1.lkp_EDW_STRT_DTTM1 AND PRTY_CLM.TRANS_STRT_DTTM = exp_prty_clm_Retire_Reject1.lkp_TRANS_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
CLM_ID = exp_prty_clm_Retire_Reject1.lkp_CLM_ID1,
PRTY_CLM_ROLE_CD = exp_prty_clm_Retire_Reject1.lkp_PRTY_CLM_ROLE_CD1,
PRTY_CLM_STRT_DTTM = exp_prty_clm_Retire_Reject1.lkp_PRTY_CLM_STRT_DTTM4,
PRTY_ID = exp_prty_clm_Retire_Reject1.lkp_PRTY_ID1,
PRTY_CLM_END_DTTM = exp_prty_clm_Retire_Reject1.lkp_PRTY_CLM_END_DTTM,
EDW_STRT_DTTM = exp_prty_clm_Retire_Reject1.lkp_EDW_STRT_DTTM1,
EDW_END_DTTM = exp_prty_clm_Retire_Reject1.o_EndDate,
TRANS_STRT_DTTM = exp_prty_clm_Retire_Reject1.lkp_TRANS_STRT_DTTM3,
TRANS_END_DTTM = exp_prty_clm_Retire_Reject1.TRANS_STRT_DTTM4;


-- PIPELINE END FOR 2

END; 
';