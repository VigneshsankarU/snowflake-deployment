-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_QUOTN_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 

declare
    start_dttm timestamp;
    end_dttm timestamp;
    prcs_id INTEGER;

BEGIN 

 start_dttm := current_timestamp();
 end_dttm := current_timestamp();
 prcs_id := 1;

-- PIPELINE START FOR 1

-- Component SQ_prty_quotn, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_prty_quotn AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as TGT_QUOTN_ID,
$2 as TGT_PRTY_QUOTN_ROLE_CD,
$3 as TGT_PRTY_QUOTN_STRT_DTTM,
$4 as TGT_PRTY_ID,
$5 as TGT_PRTY_QUOTN_END_DTTM,
$6 as TGT_TRANS_STRT_DTTM,
$7 as TGT_EDW_STRT_DTTM,
$8 as TGT_EDW_END_DTTM,
$9 as SRC_QUOTN_ID,
$10 as SRC_PRTY_QUOTN_ROLE_CD,
$11 as SRC_PRTY_QUOTN_STRT_DTTM,
$12 as SRC_PRTY_ID,
$13 as SRC_PRTY_QUOTN_END_DTTM,
$14 as SRC_TRANS_STRT_DTTM,
$15 as SOURCE_DATA,
$16 as TARGET_DATA,
$17 as FLAG,
$18 as RETIRED,
$19 as RANK,
$20 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT DISTINCT 

PQ.QUOTN_ID,

PQ.PRTY_QUOTN_ROLE_CD,

PQ.PRTY_QUOTN_STRT_DTTM,

PQ.PRTY_ID,

PQ.PRTY_QUOTN_END_DTTM,

PQ.TRANS_STRT_DTTM,

PQ.EDW_STRT_DTTM,

PQ.EDW_END_DTTM,

SQ.QUOTN_ID,

SQ.PRTY_QUOTN_ROLE_CD,

SQ.EFF_DT AS PRTY_QUOTN_STRT_DTTM,

SQ.INDIV_PRTY_ID AS PRTY_ID,

SQ.END_DT AS PRTY_QUOTN_END_DTTM,

SQ.TRANS_STRT_DTTM  AS TRANS_STRT_DTTM,         





/* SOURCEMD5DATA */
CAST(TRIM(CAST(SQ.EFF_DT AS VARCHAR(100)))||TRIM(CAST(SQ.END_DT AS VARCHAR(100))) AS VARCHAR(1000)) AS SOURCEDATA,

/* TARGETMD5DATA */
CAST(TRIM(CAST(PQ.PRTY_QUOTN_STRT_DTTM AS VARCHAR(100)))||TRIM(CAST(PQ.PRTY_QUOTN_END_DTTM AS VARCHAR(100)))  AS VARCHAR(1000)) AS TARGETDATA,



/* FLAG */


  CASE WHEN TARGETDATA IS NULL   THEN ''I''

            WHEN SOURCEDATA <> TARGETDATA THEN ''U''

            ELSE  ''R'' 

             END AS INS_UPD_FLAG ,

             RETIRED,RNK



FROM

(

/* SOURCE QUERY  */
SELECT	DISTINCT ADDRESSBOOKUID, PARTY_TYPE,SRC_QRY.JOBNUMBER,SRC_QRY.BRANCHNUMBER,

/* cast(EFF_DT AS TIMESTAMP(6) FORMAT ''YYYY-MM-DD-HH.MI.SS.S(6)'') EFF_DT */
		CAST(EFF_DT AS TIMESTAMP) EFF_DT

		,END_DT,TRANS_STRT_DTTM,XLAT_PRTY_QUOTN_ROLE.TYPECODE AS PRTY_QUOTN_ROLE_CD,

		SRC_QRY.TYPECODE,XLAT_SRC_CD.PC_SRC_CD,PRTY_TYPE,

	BUSN_CTGY,INS_QUOTN.QUOTN_ID,

		CASE WHEN (PARTY_TYPE collate ''en-ci''  IN (''PERSON'',''ADJUDICATOR'',''VENDOR (PERSON)'',''ATTORNEY'', ''DOCTOR'',''DB_T_CORE_DM_PROD.policy PERSON'',''CONTACT'')) THEN LKP_INDIV_MGR.INDIV_PRTY_ID

                  WHEN (PARTY_TYPE collate ''en-ci''  IN (''COMPANY'',''VENDOR (COMPANY)'',''AUTO REPAIR SHOP'',''AUTO TOWING AGCY'',''LAW FIRM'', ''MEDICAL CARE ORGANIZATION'')) THEN LKP_BUSN.BUSN_PRTY_ID

                  WHEN (PARTY_TYPE collate ''en-ci''  IN (''USERCONTACT'')) THEN LKP_INDIV.INDIV_PRTY_ID

                  WHEN (PRTY_TYPE  collate ''en-ci''  IN (''CO'',''PRDA'',''SRVCCTR'')) THEN INT_ORG.INTRNL_ORG_PRTY_ID

                  WHEN  (PARTY_TYPE  collate ''en-ci''  NOT IN (''PERSON'',''ADJUDICATOR'',''VENDOR (PERSON)'',''ATTORNEY'', ''DOCTOR'',''DB_T_CORE_DM_PROD.policy PERSON'',''CONTACT'',''USERCONTACT'',''COMPANY'',''VENDOR (COMPANY)'',''AUTO REPAIR SHOP'',''AUTO TOWING AGCY'',''LAW FIRM'', ''MEDICAL CARE ORGANIZATION'')) 

                  AND  (PRTY_TYPE collate ''en-ci''  NOT IN (''CO'',''PRDA'',''SRVCCTR'')) THEN 9999

		END AS INDIV_PRTY_ID,

		RETIRED , RNK

FROM	

(



SELECT	PRTY_QUOTN.ADDRESSBOOKUID, PRTY_QUOTN.Party_Type, PRTY_QUOTN.JOBNUMBER,

PRTY_QUOTN.BranchNumber, /*  ''INTRNL_ORG_TYPE15'' INTRNL_ORG_TYPE, */
case	when PRTY_QUOTN.eff_dt is null then 

/* CAST(''01/01/1900 00:00:00.000001''  AS TIMESTAMP(6) FORMAT ''MM-DD-YYYY-HH.MI.SS.S(6)'')  */
 TO_DATE (''01/01/1900'', ''mm/DD/yyyy'')

else	 PRTY_QUOTN.eff_dt 

end	as eff_dt , 
CASE
  WHEN PRTY_QUOTN.end_dt IS NULL THEN CAST(''9999-12-31 23:59:59.999999'' AS TIMESTAMP(6))
else	 PRTY_QUOTN.end_dt  

end	as end_dt, 

PRTY_QUOTN.trans_strt_dt as trans_strt_dttm, 

PRTY_QUOTN.typecode,  ''SRC_SYS4'' as PC_SRC_CD, Retired ,

 Rank()  OVER(PARTITION BY PRTY_QUOTN.ADDRESSBOOKUID,  PRTY_QUOTN.JOBNUMBER,

		PRTY_QUOTN.BranchNumber,PRTY_QUOTN.typecode  

ORDER	BY eff_dt desc)  as rnk 

FROM	

 (



 SELECT DISTINCT

    cast(CASE WHEN (MortgageeLienHolderNumber_alfa_stg IS NOT NULL AND pc_contact.AddressBookUID_stg NOT LIKE ''%MORT%''

	AND pc_contact.AddressBookUID_stg NOT LIKE ''%IRS%''

	 )THEN MortgageeLienHolderNumber_alfa_stg 													

      ELSE pc_contact.AddressBookUID_stg END as varchar(100)) as ADDRESSBOOKUID,

	    pctl_contact.name_stg PARTY_TYPE,

    pc_job.JobNumber_stg JOBNUMBER,

    pc_policycontactrole.CreateTime_stg as eff_dt

    ,pc_policyperiod.PeriodEnd_stg as end_dt

    ,pc_policycontactrole.CreateTime_stg as trans_strt_dt

    ,pctl_policycontactrole.typecode_stg as typecode

    ,NULL AS TL_CNT_Name

    ,pc_policyperiod.branchnumber_stg BRANCHNUMBER

    ,case when pc_contact.Retired_stg=0 and pc_policyperiod.Retired_stg=0 and pc_job.Retired_stg=0 then 0 else 1 end as retired,

    (:start_dttm) as start_dttm,

        (:end_dttm) as end_dttm

FROM

    DB_T_PROD_STAG.pc_contact

    inner join DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg 

    inner join DB_T_PROD_STAG.pc_policycontactrole on pc_contact.id_stg = pc_policycontactrole.contactdenorm_stg

    inner join DB_T_PROD_STAG.pc_policyperiod on pc_policycontactrole.branchid_stg = pc_policyperiod.id_stg

    inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

    inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

    inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

    inner join DB_T_PROD_STAG.pctl_policycontactrole on pc_policycontactrole.subtype_stg= pctl_policycontactrole.id_stg

WHERE pc_contact.AddressBookUID_stg is not null

and pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'') and pctl_policyperiodstatus.TYPECODE_stg<>''Temporary''

and pctl_policycontactrole.typecode_stg<>''PolicyPriNamedInsured''        /*  Excluding PrimaryNamedInsured(commented) */
and (

(pc_policycontactrole.UpdateTime_stg > (:start_dttm)   and pc_policycontactrole.UpdateTime_stg <= (:end_dttm))

or

(pc_policyperiod.UpdateTime_stg > (:start_dttm)    and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

or

(pc_job.UpdateTime_stg > (:start_dttm)     and pc_job.UpdateTime_stg <= (:end_dttm))

)



    



UNION

/*  only PrimaryNamedInsured */
SELECT DISTINCT

    cast(pc_contact.AddressBookUID_stg as  varchar(100)) as AddressBookUID_stg, 

    pctl_contact.name_stg,

    pc_job.JobNumber_stg,

    pc_policycontactrole.Createtime_stg as eff_dt

    ,pc_policyperiod.PeriodEnd_stg as end_dt

    ,pc_policycontactrole.UpdateTime_stg as trans_strt_dt

    ,pctl_policycontactrole.typecode_stg as typecode

    ,NULL AS TL_CNT_Name

    ,pc_policyperiod.branchnumber_stg

    ,case when pc_contact.Retired_stg=0 and pc_policyperiod.Retired_stg=0 and pc_job.Retired_stg=0 then 0 else 1 end as Retired,

    (:start_dttm) as start_dttm,

        (:end_dttm) as end_dttm

from DB_T_PROD_STAG.pc_policyperiod

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.ID_stg=pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_contact on pc_policyperiod.PNIContactDenorm_stg=pc_contact.ID_stg

inner join DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg

join DB_T_PROD_STAG.pc_policycontactrole on pc_policycontactrole.branchid_stg = pc_policyperiod.id_stg

join DB_T_PROD_STAG.pctl_policycontactrole on pctl_policycontactrole.ID_stg=pc_policycontactrole.Subtype_stg

    inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

    inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

WHERE pc_contact.AddressBookUID_stg is not null

and pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'') and pctl_policyperiodstatus.TYPECODE_stg<>''Temporary''

and pctl_policycontactrole.typecode_stg=''PolicyPriNamedInsured''

and (

(pc_policycontactrole.UpdateTime_stg > (:start_dttm)   and pc_policycontactrole.UpdateTime_stg <= (:end_dttm))

or

(pc_policyperiod.UpdateTime_stg > (:start_dttm)    and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

or

(pc_job.UpdateTime_stg > (:start_dttm)     and pc_job.UpdateTime_stg <= (:end_dttm))

)





UNION

/****** UW Company************/

SELECT DISTINCT

    pctl_uwcompanycode.TYPECODE_stg as prty_nk, 

    ''INTRNL_ORG_SBTYPE1'' party_type,

    pc_job.JobNumber_stg,

    pc_policyperiod.CreateTime_stg as eff_dt

    ,pc_policyperiod.PeriodEnd_stg as end_dt

    ,pc_policyperiod.UpdateTime_stg as trans_strt_dt

    ,''PRTY_QUOTN_ROLE3'' as typecode

    ,NULL AS TL_CNT_Name

    ,pc_policyperiod.branchnumber_stg

    ,case when pc_contact.Retired_stg=0 and pc_policyperiod.Retired_stg=0 and pc_job.Retired_stg=0 then 0 else 1 end as Retired,

    (:start_dttm) as start_dttm,       (:end_dttm) as end_dttm

from 

DB_T_PROD_STAG.pc_contact

    inner join DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg 

    inner join DB_T_PROD_STAG.pc_policycontactrole on pc_contact.id_stg = pc_policycontactrole.contactdenorm_stg

inner join DB_T_PROD_STAG.pc_policyperiod on pc_policycontactrole.branchid_stg = pc_policyperiod.id_stg 

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_uwcompany ON pc_uwcompany.ID_stg  = pc_policyperiod.UWCompany_stg

inner join DB_T_PROD_STAG.pctl_uwcompanycode on pctl_uwcompanycode.id_stg=pc_uwcompany.code_stg

inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

where pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'') and pctl_policyperiodstatus.TYPECODE_stg<>''Temporary''

and (

(pc_policyperiod.UpdateTime_stg > (:start_dttm)    and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

or

(pc_job.UpdateTime_stg > (:start_dttm)     and pc_job.UpdateTime_stg <= (:end_dttm))

)

union

/******Producer************/

select  DISTINCT

    pc_producercode.Code_stg as prty_nk, 

    ''INTRNL_ORG_SBTYPE2'' party_type,

    pc_job.JobNumber_stg,

    pc_policyperiod.CreateTime_stg as eff_dt,

    coalesce(pc_effectivedatedfields.ExpirationDate_stg,pc_policyperiod.PeriodEnd_stg) as end_dt,

    pc_policyperiod.UpdateTime_stg as trans_strt_dt

    ,''PRTY_QUOTN_ROLE6'' as typecode

    ,NULL AS TL_CNT_Name

    ,pc_policyperiod.branchnumber_stg

    ,case when pc_contact.Retired_stg=0 and pc_policyperiod.Retired_stg=0 and pc_job.Retired_stg=0 then 0 else 1 end as Retired,

    (:start_dttm) as start_dttm,

        (:end_dttm) as end_dttm

from 

DB_T_PROD_STAG.pc_contact

    inner join DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg 

    inner join DB_T_PROD_STAG.pc_policycontactrole on pc_contact.id_stg = pc_policycontactrole.contactdenorm_stg

inner join DB_T_PROD_STAG.pc_policyperiod on pc_policycontactrole.branchid_stg = pc_policyperiod.id_stg 

 inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_effectivedatedfields on pc_policyperiod.id_stg=pc_effectivedatedfields.BranchID_stg

inner join DB_T_PROD_STAG.pc_producercode ON pc_producercode.ID_stg  = pc_effectivedatedfields.ProducerCodeID_stg

inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

where pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'') and pctl_policyperiodstatus.TYPECODE_stg<>''Temporary''

and pc_effectivedatedfields.ExpirationDate_stg is null

and (

(pc_effectivedatedfields.UpdateTime_stg > (:start_dttm)    and pc_effectivedatedfields.UpdateTime_stg <= (:end_dttm))

or

(pc_policyperiod.UpdateTime_stg > (:start_dttm)    and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

or

(pc_job.UpdateTime_stg > (:start_dttm)     and pc_job.UpdateTime_stg <= (:end_dttm))

)

union

/******Service Center************/

select DISTINCT

    pc_group.name_stg AS NAICCode,

    UWD.party_type AS party_type,

    pc_job.JobNumber_stg,

    pc_policyperiod.CreateTime_stg as eff_dt,

    coalesce(pc_effectivedatedfields.ExpirationDate_stg,pc_policyperiod.periodend_stg) as end_dt,

    pc_policyperiod.UpdateTime_stg as trans_strt_dt

    ,''PRTY_QUOTN_ROLE5'' as typecode

    ,NULL AS TL_CNT_Name

    ,pc_policyperiod.branchnumber_stg

    ,case when pc_contact.Retired_stg=0 and pc_policyperiod.Retired_stg=0 and pc_job.Retired_stg=0 then 0 else 1 end as Retired ,

    (:start_dttm) as start_dttm,       (:end_dttm) as end_dttm

 from 

 DB_T_PROD_STAG.pc_contact

    inner join DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg 

    inner join DB_T_PROD_STAG.pc_policycontactrole on pc_contact.id_stg = pc_policycontactrole.contactdenorm_stg

inner join DB_T_PROD_STAG.pc_policyperiod on pc_policycontactrole.branchid_stg = pc_policyperiod.id_stg 

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_effectivedatedfields on pc_policyperiod.id_stg=pc_effectivedatedfields.BranchID_stg 

INNER JOIN DB_T_PROD_STAG.PC_GROUP on pc_group.ID_stg=pc_effectivedatedfields.ServiceCenter_alfa_stg



inner join (select SVC.ID_stg as ServiceCenter_GroupID, SVC.Name_stg as ServiceCenter_Name, UWDist.Name_stg as UWDistrictName,pctl_grouptype.typecode_stg as party_type

from DB_T_PROD_STAG.PC_GROUP as SVC

join DB_T_PROD_STAG.pctl_grouptype on pctl_grouptype.ID_stg=SVC.GroupType_stg

left join DB_T_PROD_STAG.pcx_uwparentgroup_alfa on pcx_uwparentgroup_alfa.OwnerID_stg=SVC.ID_stg

left join DB_T_PROD_STAG.PC_GROUP as UWDist on UWDist.ID_stg=pcx_uwparentgroup_alfa.ForeignEntityID_stg

where pctl_grouptype.TYPECODE_stg=''servicecenter_alfa'') UWD ON UWD.ServiceCenter_Name=pc_group.Name_stg



inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

where pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'') and pctl_policyperiodstatus.TYPECODE_stg <>''Temporary''

and pc_effectivedatedfields.ExpirationDate_stg is null 

and (

(pc_effectivedatedfields.UpdateTime_stg > (:start_dttm)    and pc_effectivedatedfields.UpdateTime_stg <= (:end_dttm))

or

(pc_policyperiod.UpdateTime_stg > (:start_dttm)    and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

or

(pc_job.UpdateTime_stg > (:start_dttm)     and pc_job.UpdateTime_stg <= (:end_dttm))

)union



/***********************Capture Users such as underwriter,producer*************************/

select publicid_stg, usercontact, jobnumber_stg, eff_dt, end_Dt,

trans_strt_dt, typecode, tl_cnt_name, branchnumber_stg, retired ,start_dttm,end_dttm

from (

select DISTINCT

       pc_contact.PublicID_stg,

       ''UserContact'' usercontact,

       pc_job.JobNumber_stg,

pc_jobuserroleassign.CreateTime_stg eff_dt,

pc_policyperiod.periodend_stg as end_dt,

pc_jobuserroleassign.UpdateTime_stg as trans_strt_dt,

pctl_userrole.TYPECODE_stg AS typecode,

NULL AS TL_CNT_Name

       ,pc_policyperiod.branchnumber_stg

       ,case when pc_contact.Retired_stg=0 and pc_policyperiod.Retired_stg=0 and pc_job.Retired_stg=0 then 0 else 1 end as Retired,

    (:start_dttm) as start_dttm

    ,(:end_dttm) as end_dttm,

    row_number() over(partition by  pc_job.jobnumber_stg ,pctl_userrole.TYPECODE_stg,branchnumber_stg order by

    case when pc_jobuserroleassign.closedate_stg is null then cast(''1900-01-01'' as date) else pc_jobuserroleassign.closedate_stg end desc, pc_jobuserroleassign.UpdateTime_stg desc) rnk

from DB_T_PROD_STAG.pc_policyperiod join DB_T_PROD_STAG.pc_policy on pc_policyperiod.policyid_stg = pc_policy.id_stg

inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

       join DB_T_PROD_STAG.pc_jobuserroleassign on pc_job.id_stg = pc_jobuserroleassign.JobID_stg

       join DB_T_PROD_STAG.pctl_userrole on pc_jobuserroleassign.role_stg = pctl_userrole.id_stg

       join DB_T_PROD_STAG.pc_user on pc_jobuserroleassign.AssignedUserID_stg = pc_user.id_stg

       join DB_T_PROD_STAG.pc_contact on pc_contact.id_stg = pc_user.ContactID_stg

          inner join DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=pc_contact.Subtype_stg

where pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'') and pctl_policyperiodstatus.TYPECODE_stg <>''Temporary'' 

and  pctl_userrole.TYPECODE_stg<>''Producer''

and (pctl_userrole.TYPECODE_stg = ''Underwriter'' or (pctl_userrole.TYPECODE_stg <> ''Underwriter'' and pc_jobuserroleassign.CloseDate_stg is not null))

/*EIM-13802 , remove closedate filter and add rnk to get latest record*/

/* and pc_jobuserroleassign.CloseDate is not null */
and (

(pc_policyperiod.UpdateTime_stg > (:start_dttm)    and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

or

(pc_job.UpdateTime_stg > (:start_dttm)     and pc_job.UpdateTime_stg <= (:end_dttm))

)) A where rnk=1 

UNION

/***********************CreateUser and UpdateUser*************************/



select DISTINCT pc_contact.PublicID_stg,  

/* pc_contact.AddressBookUID, */
    ''UserContact'',

    pc_job.JobNumber_stg,

    pc_job.CreateTime_stg  as eff_dt,

    pc_policyperiod.PeriodEnd_stg as end_dt,

    pc_job.UpdateTime_stg as trans_strt_dt,

''PRTY_QUOTN_ROLE8'' AS typecode,

NULL AS TL_CNT_Name

,pc_policyperiod.branchnumber_stg

,case when pc_contact.Retired_stg=0 and pc_policyperiod.Retired_stg=0 and pc_job.Retired_stg=0 then 0 else 1 end as Retired,

(:start_dttm) as start_dttm,

        (:end_dttm) as end_dttm

from 

DB_T_PROD_STAG.pc_job

inner join DB_T_PROD_STAG.pctl_job on (pc_job.subtype_stg = pctl_job.id_stg and pctl_job.name_stg = ''Submission'')

inner join DB_T_PROD_STAG.pc_user on pc_job.createuserid_stg = pc_user.id_stg

inner join DB_T_PROD_STAG.pc_credential on pc_user.credentialID_stg = pc_credential.id_stg

inner join DB_T_PROD_STAG.pc_contact on pc_user.ContactID_stg = pc_contact.id_stg

inner join DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg

inner join DB_T_PROD_STAG.pc_policyperiod on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

where pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'') and pctl_policyperiodstatus.TYPECODE_stg<>''Temporary''

and (

(pc_policyperiod.UpdateTime_stg > (:start_dttm)    and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

or

(pc_job.UpdateTime_stg > (:start_dttm)     and pc_job.UpdateTime_stg <= (:end_dttm))

)



UNION

select DISTINCT

    pc_contact.PublicID_stg, 

/* pc_contact.AddressBookUID, */
    ''UserContact'',

    pc_job.JobNumber_stg,

    pc_job.CreateTime_stg as eff_dt,

    pc_policyperiod.PeriodEnd_stg as end_dt,

    pc_job.UpdateTime_stg as trans_strt_dt,

''PRTY_QUOTN_ROLE9'' AS typecode,

NULL AS TL_CNT_Name

,pc_policyperiod.branchnumber_stg

,case when pc_contact.Retired_stg=0 and pc_policyperiod.Retired_stg=0 and pc_job.Retired_stg=0 then 0 else 1 end as Retired,

(:start_dttm) as start_dttm,

        (:end_dttm) as end_dttm

from 

DB_T_PROD_STAG.pc_job

inner join DB_T_PROD_STAG.pctl_job on (pc_job.subtype_stg = pctl_job.id_stg and pctl_job.name_stg = ''Submission'')

inner join DB_T_PROD_STAG.pc_user on pc_job.UpdateUserID_stg = pc_user.id_stg

inner join DB_T_PROD_STAG.pc_credential on pc_user.credentialID_stg = pc_credential.id_stg

inner join DB_T_PROD_STAG.pc_contact on pc_user.ContactID_stg = pc_contact.id_stg

inner join DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg

inner join DB_T_PROD_STAG.pc_policyperiod on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

where pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'') and pctl_policyperiodstatus.TYPECODE_stg <>''Temporary''

and (

(pc_policyperiod.UpdateTime_stg > (:start_dttm)    and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

or

(pc_job.UpdateTime_stg > (:start_dttm)     and pc_job.UpdateTime_stg <= (:end_dttm))

)



union 



/****** Prior Insurance carrier for all DB_T_CORE_DM_PROD.policy periods******/

select  

coalesce(ppca.TYPECODE_stg,priorcarrier_farm.TYPECODE_stg) as prty_nk,

''BUSN_CTGY6'' party_type,

pc_job.JobNumber_stg,

coalesce(pc_effectivedatedfields.effectivedate_stg,pc_policyperiod.PeriodStart_stg) as eff_dt,

pc_policyperiod.PeriodEnd_stg as end_dt,

pc_effectivedatedfields.UpdateTime_stg as trans_strt_dt,

''PRTY_QUOTN_ROLE10'' as typecode,

NULL AS TL_CNT_Name,

pc_policyperiod.branchnumber_stg,

case when pc_policyperiod.retired_stg=0  and (ppca.retired_stg=0 or priorcarrier_farm.retired_stg=0) then 0 else 1 end as retired, 

(:start_dttm) as start_dttm,

(:end_dttm) as end_dttm

from DB_T_PROD_STAG.pc_policyperiod 

inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.status_stg

inner join DB_T_PROD_STAG.pc_effectivedatedfields on pc_policyperiod.id_stg=pc_effectivedatedfields.branchid_stg

left join DB_T_PROD_STAG.pctl_priorcarrier_alfa ppca on ppca.id_stg=pc_effectivedatedfields.priorcarrier_alfa_stg

join  DB_T_PROD_STAG.pc_policyline on  pc_policyperiod.id_stg=pc_policyline.branchid_stg

left join  DB_T_PROD_STAG.pctl_priorcarrier_alfa priorcarrier_farm on  priorcarrier_farm.id_stg=pc_policyline.FOPPriorCarrier_stg

where pctl_job.typecode_stg  in (''Submission'',''PolicyChange'',''Renewal'') 

and pctl_policyperiodstatus.typecode_stg<>''Temporary''

and  pc_effectivedatedfields.expirationdate_stg is null

and (ppca.TYPECODE_stg is not null or priorcarrier_farm.TYPECODE_stg is not null)

and 

(

(pc_policyperiod.updatetime_stg > (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm))

or 

(pc_effectivedatedfields.updatetime_stg > (:start_dttm) and pc_effectivedatedfields.updatetime_stg <= (:end_dttm))

)

  )  

  PRTY_QUOTN

where	prty_quotn.AddressBookUID is not null 

	and	PRTY_QUOTN.typecode in (''PolicyAddlInsured'', ''PolicyAddlNamedInsured'',

		''PolicyDriver'',''HOPolOccLiabInsured_alfa'',''PolicyAddlInterest'') 

		qualify	row_number ()  over  ( partition by PRTY_QUOTN.ADDRESSBOOKUID,

		 PRTY_QUOTN.JOBNUMBER, PRTY_QUOTN.BranchNumber,PRTY_QUOTN.typecode 

order	by  PRTY_QUOTN.eff_dt desc   )=1



) SRC_QRY





/* TERADATA_ETL_REF_XLAT_PRTY_QUOTN_ROLE */
LEFT OUTER JOIN

(

SELECT	 DISTINCT

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL 

	,

CASE	WHEN (LTRIM(RTRIM(TGT_IDNTFTN_VAL)) IS NULL) THEN ''UNK''

	ELSE (LTRIM(RTRIM(TGT_IDNTFTN_VAL))) 

END	AS TYPECODE	 

FROM	

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT TERADATA_ETL_REF_XLAT

WHERE	

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_QUOTN_ROLE''

	AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''PCTL_ACCOUNTCONTACTROLE.TYPECODE'',

		''PCTL_USERROLE.TYPECODE'',''PCTL_POLICYCONTACTROLE.TYPECODE'',''PCTL_ADDITIONALINTERESTTYPE.TYPECODE'',

		''BCTL_ACCOUNTROLE.TYPECODE'',''DERIVED'') 

	AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''GW'',''DS'') 

	AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''

)XLAT_PRTY_QUOTN_ROLE



	ON	SRC_QRY.TYPECODE=XLAT_PRTY_QUOTN_ROLE.SRC_IDNTFTN_VAL

/* TERADATA_ETL_REF_XLAT_SRC_CD */


LEFT OUTER JOIN

(

SELECT	 DISTINCT

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS PC_SRC_CD

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL 

FROM	

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT TERADATA_ETL_REF_XLAT

WHERE	

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''DERIVED'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''

)XLAT_SRC_CD

	ON	SRC_QRY.PC_SRC_CD=XLAT_SRC_CD.SRC_IDNTFTN_VAL

	

/* TERADATA_ETL_REF_XLAT_PARTY_TYPE */
	LEFT OUTER JOIN

(

SELECT	 DISTINCT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS PRTY_TYPE

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL 

FROM	

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT TERADATA_ETL_REF_XLAT

WHERE	

		TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INTRNL_ORG_SBTYPE''

)XLAT_PRTY_TYPE



	ON	SRC_QRY.PARTY_TYPE=XLAT_PRTY_TYPE.SRC_IDNTFTN_VAL







LEFT OUTER JOIN

(

SELECT	 DISTINCT 

 TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS BUSN_CTGY

 ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL 

FROM	

 DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT TERADATA_ETL_REF_XLAT

WHERE	

 TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM IN (''BUSN_CTGY'',''ORG_TYPE'',

		''PRTY_TYPE'')

 AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''DERIVED'', ''CCTL_CONTACT.TYPECODE'',

		''CCTL_CONTACT.NAME'')

 AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''DS'',''GW'')

 AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''



)XLAT_BUSN_CTGY



	ON	SRC_QRY.PARTY_TYPE =XLAT_BUSN_CTGY.SRC_IDNTFTN_VAL

	

/* LKP_INDIV_CLM_CTR */


LEFT OUTER JOIN

(

SELECT	 DISTINCT

	INDIV.INDIV_PRTY_ID AS INDIV_PRTY_ID, 

	INDIV.NK_PUBLC_ID AS NK_PUBLC_ID 

FROM	

	DB_T_PROD_CORE.INDIV INDIV

WHERE	

	INDIV.NK_PUBLC_ID IS NOT NULL



)LKP_INDIV



	ON	SRC_QRY.ADDRESSBOOKUID=LKP_INDIV.NK_PUBLC_ID

	

/* LKP_INDIV_CNT_MGR */


LEFT OUTER JOIN

(

SELECT DISTINCT 

	INDIV.INDIV_PRTY_ID AS INDIV_PRTY_ID, 

	INDIV.NK_LINK_ID AS NK_LINK_ID 

FROM 

	DB_T_PROD_CORE.INDIV INDIV

WHERE

	INDIV.NK_PUBLC_ID IS NULL

	AND  CAST(EDW_END_DTTM AS DATE)=''9999-12-31''

)LKP_INDIV_MGR



ON SRC_QRY.ADDRESSBOOKUID=LKP_INDIV_MGR.NK_LINK_ID



LEFT OUTER JOIN

(

SELECT	DISTINCT BUSN.BUSN_PRTY_ID AS BUSN_PRTY_ID, BUSN.SRC_SYS_CD AS SRC_SYS_CD,

		BUSN.TAX_BRAKT_CD AS TAX_BRAKT_CD, BUSN.ORG_TYPE_CD AS ORG_TYPE_CD,

		BUSN.GICS_SBIDSTRY_CD AS GICS_SBIDSTRY_CD, BUSN.LIFCYCL_CD AS LIFCYCL_CD,

		BUSN.PRTY_TYPE_CD AS PRTY_TYPE_CD, BUSN.BUSN_END_DTTM AS BUSN_END_DTTM,

		BUSN.BUSN_STRT_DTTM AS BUSN_STRT_DTTM, BUSN.INC_IND AS INC_IND,

		BUSN.EDW_STRT_DTTM AS EDW_STRT_DTTM, BUSN.EDW_END_DTTM AS EDW_END_DTTM,

		BUSN.BUSN_CTGY_CD AS BUSN_CTGY_CD, BUSN.NK_BUSN_CD AS NK_BUSN_CD 

FROM	DB_T_PROD_CORE.BUSN BUSN

QUALIFY	ROW_NUMBER () OVER (PARTITION BY NK_BUSN_CD,BUSN_CTGY_CD 

ORDER	BY EDW_END_DTTM DESC )=1

)LKP_BUSN



	ON	SRC_QRY.ADDRESSBOOKUID=LKP_BUSN.NK_BUSN_CD 

	AND	BUSN_CTGY=LKP_BUSN.BUSN_CTGY_CD

	

	

/* DB_T_PROD_CORE.INTRNL_ORG */


LEFT OUTER JOIN

(

SELECT DISTINCT 	INTRNL_ORG.INTRNL_ORG_PRTY_ID AS INTRNL_ORG_PRTY_ID, INTRNL_ORG.INTRNL_ORG_TYPE_CD AS INTRNL_ORG_TYPE_CD,

		INTRNL_ORG.INTRNL_ORG_SBTYPE_CD AS INTRNL_ORG_SBTYPE_CD, INTRNL_ORG.INTRNL_ORG_NUM AS INTRNL_ORG_NUM,

		INTRNL_ORG.SRC_SYS_CD AS SRC_SYS_CD 

FROM	DB_T_PROD_CORE.INTRNL_ORG INTRNL_ORG 

WHERE	INTRNL_ORG_TYPE_CD=''INT''

AND  CAST(EDW_END_DTTM AS DATE)=''9999-12-31''

/* QUALIFY ROW_NUMBER () OVER (PARTITION BY INTRNL_ORG_NUM,INTRNL_ORG_TYPE_CD,		INTRNL_ORG_SBTYPE_CD,SRC_SYS_CD ORDER	BY EDW_END_DTTM DESC)=1 */
) INT_ORG



	ON	SRC_QRY.ADDRESSBOOKUID=INT_ORG.INTRNL_ORG_NUM 

	AND PRTY_TYPE=INT_ORG.INTRNL_ORG_SBTYPE_CD 

AND	 XLAT_SRC_CD.PC_SRC_CD=INT_ORG.SRC_SYS_CD 

	

	

/* DB_T_PROD_CORE.INSRNC_QUOTN */


LEFT OUTER JOIN

(

SELECT DISTINCT 	INSRNC_QUOTN.QUOTN_ID AS QUOTN_ID, INSRNC_QUOTN.NK_JOB_NBR AS JOBNUMBER,

		INSRNC_QUOTN.VERS_NBR AS VERS_NBR 

FROM	DB_T_PROD_CORE.INSRNC_QUOTN INSRNC_QUOTN

WHERE CAST(EDW_END_DTTM AS DATE)=''9999-12-31''

/* QUALIFY	ROW_NUMBER() OVER(PARTITION BY  INSRNC_QUOTN.NK_JOB_NBR,		INSRNC_QUOTN.VERS_NBR,  INSRNC_QUOTN.SRC_SYS_CD  ORDER	BY INSRNC_QUOTN.EDW_END_DTTM DESC) = 1 */
)INS_QUOTN



	ON	 SRC_QRY.JOBNUMBER=INS_QUOTN.JOBNUMBER

	AND	SRC_QRY.BRANCHNUMBER=INS_QUOTN.VERS_NBR



) SQ



/* DB_T_PROD_CORE.PRTY_QUOTN */


LEFT OUTER JOIN

(



SELECT	 DISTINCT PRTY_QUOTN.PRTY_QUOTN_STRT_DTTM AS PRTY_QUOTN_STRT_DTTM,

		PRTY_QUOTN.PRTY_QUOTN_END_DTTM AS PRTY_QUOTN_END_DTTM, PRTY_QUOTN.EDW_STRT_DTTM AS EDW_STRT_DTTM, PRTY_QUOTN.EDW_END_DTTM AS EDW_END_DTTM,

		PRTY_QUOTN.TRANS_STRT_DTTM AS TRANS_STRT_DTTM,

		PRTY_QUOTN.QUOTN_ID AS QUOTN_ID, PRTY_QUOTN.PRTY_ID AS PRTY_ID,

		PRTY_QUOTN.PRTY_QUOTN_ROLE_CD AS PRTY_QUOTN_ROLE_CD 

FROM	DB_T_PROD_CORE.PRTY_QUOTN PRTY_QUOTN

WHERE CAST(EDW_END_DTTM AS DATE)=''9999-12-31''

/* QUALIFY	ROW_NUMBER() OVER(PARTITION BY QUOTN_ID,PRTY_QUOTN_ROLE_CD,		PRTY_ID ORDER	BY EDW_END_DTTM DESC) = 1 */


)PQ



ON SQ.QUOTN_ID=PQ.QUOTN_ID

AND SQ.PRTY_QUOTN_ROLE_CD=PQ.PRTY_QUOTN_ROLE_CD

AND  SQ.INDIV_PRTY_ID=PQ.PRTY_ID
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_prty_quotn.TGT_QUOTN_ID as TGT_QUOTN_ID,
SQ_prty_quotn.TGT_PRTY_QUOTN_STRT_DTTM as TGT_PRTY_QUOTN_STRT_DTTM,
SQ_prty_quotn.TGT_PRTY_QUOTN_END_DTTM as TGT_PRTY_QUOTN_END_DTTM,
SQ_prty_quotn.TGT_EDW_STRT_DTTM as TGT_EDW_STRT_DTTM,
SQ_prty_quotn.TGT_EDW_END_DTTM as TGT_EDW_END_DTTM,
SQ_prty_quotn.SRC_QUOTN_ID as SRC_QUOTN_ID,
SQ_prty_quotn.SRC_PRTY_QUOTN_ROLE_CD as SRC_PRTY_QUOTN_ROLE_CD,
SQ_prty_quotn.SRC_PRTY_QUOTN_STRT_DTTM as SRC_PRTY_QUOTN_STRT_DTTM,
SQ_prty_quotn.SRC_PRTY_ID as SRC_PRTY_ID,
SQ_prty_quotn.SRC_PRTY_QUOTN_END_DTTM as SRC_PRTY_QUOTN_END_DTTM,
SQ_prty_quotn.SRC_TRANS_STRT_DTTM as SRC_TRANS_STRT_DTTM,
:PRCS_ID as out_PRCS_ID,
CURRENT_TIMESTAMP as out_EDW_STRT_DTTM,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as out_EDW_END_DTTM,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as out_TRANS_END_DTTM,
SQ_prty_quotn.FLAG as FLAG,
SQ_prty_quotn.RETIRED as RETIRED,
SQ_prty_quotn.RANK as RANK,
SQ_prty_quotn.source_record_id
FROM
SQ_prty_quotn
);


-- Component rtr_PRTY_QUOTN_FLG_INSERT, Type ROUTER Output Group INSERT
create or replace temporary table rtr_PRTY_QUOTN_FLG_INSERT AS
SELECT
exp_pass_through.TGT_QUOTN_ID as TGT_QUOTN_ID,
exp_pass_through.TGT_PRTY_QUOTN_STRT_DTTM as TGT_PRTY_QUOTN_STRT_DTTM,
exp_pass_through.TGT_PRTY_QUOTN_END_DTTM as TGT_PRTY_QUOTN_END_DTTM,
exp_pass_through.TGT_EDW_STRT_DTTM as TGT_EDW_STRT_DTTM,
exp_pass_through.TGT_EDW_END_DTTM as TGT_EDW_END_DTTM,
exp_pass_through.SRC_QUOTN_ID as SRC_QUOTN_ID,
exp_pass_through.SRC_PRTY_QUOTN_ROLE_CD as SRC_PRTY_QUOTN_ROLE_CD,
exp_pass_through.SRC_PRTY_QUOTN_STRT_DTTM as SRC_PRTY_QUOTN_STRT_DTTM,
exp_pass_through.SRC_PRTY_ID as SRC_PRTY_ID,
exp_pass_through.SRC_PRTY_QUOTN_END_DTTM as SRC_PRTY_QUOTN_END_DTTM,
exp_pass_through.SRC_TRANS_STRT_DTTM as SRC_TRANS_STRT_DTTM,
exp_pass_through.out_PRCS_ID as PRCS_ID,
exp_pass_through.out_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_through.out_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_through.out_TRANS_END_DTTM as TRANS_END_DTTM,
exp_pass_through.FLAG as FLAG,
exp_pass_through.RETIRED as RETIRED,
exp_pass_through.RANK as RANK,
exp_pass_through.source_record_id
FROM
exp_pass_through
WHERE exp_pass_through.FLAG = ''I'' AND exp_pass_through.SRC_PRTY_ID IS NOT NULL AND exp_pass_through.SRC_QUOTN_ID IS NOT NULL OR ( exp_pass_through.RETIRED = 0 AND exp_pass_through.TGT_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AND exp_pass_through.SRC_PRTY_ID IS NOT NULL ) /*- - old_QUOTN_ID IS NULL AND QUOTN_ID IS NOT NULL AND PRTY_ID IS NOT NULL*/
;


-- Component rtr_PRTY_QUOTN_FLG_RETIRED, Type ROUTER Output Group RETIRED
create or replace temporary table rtr_PRTY_QUOTN_FLG_RETIRED AS
SELECT
exp_pass_through.TGT_QUOTN_ID as TGT_QUOTN_ID,
exp_pass_through.TGT_PRTY_QUOTN_STRT_DTTM as TGT_PRTY_QUOTN_STRT_DTTM,
exp_pass_through.TGT_PRTY_QUOTN_END_DTTM as TGT_PRTY_QUOTN_END_DTTM,
exp_pass_through.TGT_EDW_STRT_DTTM as TGT_EDW_STRT_DTTM,
exp_pass_through.TGT_EDW_END_DTTM as TGT_EDW_END_DTTM,
exp_pass_through.SRC_QUOTN_ID as SRC_QUOTN_ID,
exp_pass_through.SRC_PRTY_QUOTN_ROLE_CD as SRC_PRTY_QUOTN_ROLE_CD,
exp_pass_through.SRC_PRTY_QUOTN_STRT_DTTM as SRC_PRTY_QUOTN_STRT_DTTM,
exp_pass_through.SRC_PRTY_ID as SRC_PRTY_ID,
exp_pass_through.SRC_PRTY_QUOTN_END_DTTM as SRC_PRTY_QUOTN_END_DTTM,
exp_pass_through.SRC_TRANS_STRT_DTTM as SRC_TRANS_STRT_DTTM,
exp_pass_through.out_PRCS_ID as PRCS_ID,
exp_pass_through.out_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_through.out_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_through.out_TRANS_END_DTTM as TRANS_END_DTTM,
exp_pass_through.FLAG as FLAG,
exp_pass_through.RETIRED as RETIRED,
exp_pass_through.RANK as RANK,
exp_pass_through.source_record_id
FROM
exp_pass_through
WHERE ( exp_pass_through.FLAG = ''R'' ) and exp_pass_through.RETIRED != 0 and exp_pass_through.TGT_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AND exp_pass_through.SRC_PRTY_ID IS NOT NULL -- old_QUOTN_ID IS NOT NULL AND QUOTN_ID IS NOT NULL AND PRTY_ID IS NOT NULL;
;

-- Component upd_stg_upd_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_stg_upd_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_PRTY_QUOTN_FLG_RETIRED.TGT_QUOTN_ID as QUOTN_ID,
rtr_PRTY_QUOTN_FLG_RETIRED.SRC_PRTY_QUOTN_ROLE_CD as PRTY_QUOTN_ROLE_CD,
rtr_PRTY_QUOTN_FLG_RETIRED.SRC_PRTY_QUOTN_STRT_DTTM as PRTY_QUOTN_STRT_DT,
rtr_PRTY_QUOTN_FLG_RETIRED.SRC_PRTY_ID as PRTY_ID,
NULL as PRTY_QUOTN_END_DT,
NULL as PRCS_ID,
NULL as EDW_expiry3,
rtr_PRTY_QUOTN_FLG_RETIRED.TGT_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_PRTY_QUOTN_FLG_RETIRED.TGT_PRTY_QUOTN_STRT_DTTM as lkp_PRTY_QUOTN_STRT_DT13,
rtr_PRTY_QUOTN_FLG_RETIRED.EDW_STRT_DTTM as in_EDW_STRT_DTTM4,
rtr_PRTY_QUOTN_FLG_RETIRED.SRC_TRANS_STRT_DTTM as TRANS_START_DTTM4,
1 as UPDATE_STRATEGY_ACTION,
source_record_id
FROM
rtr_PRTY_QUOTN_FLG_RETIRED
);


-- Component upd_stg_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_stg_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_PRTY_QUOTN_FLG_INSERT.SRC_QUOTN_ID as QUOTN_ID,
rtr_PRTY_QUOTN_FLG_INSERT.SRC_PRTY_QUOTN_ROLE_CD as PRTY_QUOTN_ROLE_CD,
rtr_PRTY_QUOTN_FLG_INSERT.SRC_PRTY_QUOTN_STRT_DTTM as PRTY_QUOTN_STRT_DT,
rtr_PRTY_QUOTN_FLG_INSERT.SRC_PRTY_ID as PRTY_ID,
rtr_PRTY_QUOTN_FLG_INSERT.SRC_PRTY_QUOTN_END_DTTM as PRTY_QUOTN_END_DT,
rtr_PRTY_QUOTN_FLG_INSERT.PRCS_ID as PRCS_ID,
rtr_PRTY_QUOTN_FLG_INSERT.EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
rtr_PRTY_QUOTN_FLG_INSERT.EDW_END_DTTM as in_EDW_END_DTTM1,
rtr_PRTY_QUOTN_FLG_INSERT.RETIRED as Retired1,
rtr_PRTY_QUOTN_FLG_INSERT.SRC_TRANS_STRT_DTTM as TRANS_START_DTTM1,
rtr_PRTY_QUOTN_FLG_INSERT.RANK as Rank1,
rtr_PRTY_QUOTN_FLG_INSERT.TRANS_END_DTTM as TRANS_END_DTTM1,
0 as UPDATE_STRATEGY_ACTION,
rtr_PRTY_QUOTN_FLG_INSERT.source_record_id
FROM
rtr_PRTY_QUOTN_FLG_INSERT
);


-- Component exp_pass_to_target_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_ins AS
(
SELECT
upd_stg_ins.QUOTN_ID as QUOTN_ID,
upd_stg_ins.PRTY_QUOTN_ROLE_CD as PRTY_QUOTN_ROLE_CD,
upd_stg_ins.PRTY_QUOTN_STRT_DT as PRTY_QUOTN_STRT_DT,
upd_stg_ins.PRTY_ID as PRTY_ID,
upd_stg_ins.PRTY_QUOTN_END_DT as PRTY_QUOTN_END_DT,
upd_stg_ins.PRCS_ID as PRCS_ID,
CASE WHEN upd_stg_ins.Retired1 = 0 THEN upd_stg_ins.in_EDW_END_DTTM1 ELSE CURRENT_TIMESTAMP END as out_EDW_END_DTTM,
upd_stg_ins.TRANS_START_DTTM1 as TRANS_START_DTTM1,
CASE WHEN upd_stg_ins.Retired1 <> 0 THEN upd_stg_ins.TRANS_START_DTTM1 ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as TRANS_END_DTTM11,
dateadd (second,  ( 2 * ( upd_stg_ins.Rank1 - 1 ) ),  CURRENT_TIMESTAMP  ) as in_EDW_STRT_DTTM1,
upd_stg_ins.source_record_id
FROM
upd_stg_ins
);


-- Component exp_pass_to_target_upd_retired, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_upd_retired AS
(
SELECT
upd_stg_upd_retired.QUOTN_ID as QUOTN_ID,
upd_stg_upd_retired.PRTY_QUOTN_ROLE_CD as PRTY_QUOTN_ROLE_CD,
upd_stg_upd_retired.PRTY_ID as PRTY_ID,
upd_stg_upd_retired.lkp_PRTY_QUOTN_STRT_DT13 as lkp_PRTY_QUOTN_STRT_DT13,
upd_stg_upd_retired.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
upd_stg_upd_retired.in_EDW_STRT_DTTM4 as in_EDW_STRT_DTTM4,
upd_stg_upd_retired.TRANS_START_DTTM4 as TRANS_START_DTTM4,
upd_stg_upd_retired.source_record_id
FROM
upd_stg_upd_retired
);


-- Component tgt_PRTY_QUOTN_upd_retired, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_QUOTN
USING exp_pass_to_target_upd_retired ON (PRTY_QUOTN.QUOTN_ID = exp_pass_to_target_upd_retired.QUOTN_ID AND PRTY_QUOTN.PRTY_QUOTN_ROLE_CD = exp_pass_to_target_upd_retired.PRTY_QUOTN_ROLE_CD AND PRTY_QUOTN.PRTY_QUOTN_STRT_DTTM = exp_pass_to_target_upd_retired.lkp_PRTY_QUOTN_STRT_DT13 AND PRTY_QUOTN.PRTY_ID = exp_pass_to_target_upd_retired.PRTY_ID AND PRTY_QUOTN.EDW_STRT_DTTM = exp_pass_to_target_upd_retired.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
QUOTN_ID = exp_pass_to_target_upd_retired.QUOTN_ID,
PRTY_QUOTN_ROLE_CD = exp_pass_to_target_upd_retired.PRTY_QUOTN_ROLE_CD,
PRTY_QUOTN_STRT_DTTM = exp_pass_to_target_upd_retired.lkp_PRTY_QUOTN_STRT_DT13,
PRTY_ID = exp_pass_to_target_upd_retired.PRTY_ID,
EDW_STRT_DTTM = exp_pass_to_target_upd_retired.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_target_upd_retired.in_EDW_STRT_DTTM4,
TRANS_END_DTTM = exp_pass_to_target_upd_retired.TRANS_START_DTTM4;


-- Component tgt_PRTY_QUOTN_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_QUOTN
(
QUOTN_ID,
PRTY_QUOTN_ROLE_CD,
PRTY_QUOTN_STRT_DTTM,
PRTY_ID,
PRTY_QUOTN_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_target_ins.QUOTN_ID as QUOTN_ID,
exp_pass_to_target_ins.PRTY_QUOTN_ROLE_CD as PRTY_QUOTN_ROLE_CD,
exp_pass_to_target_ins.PRTY_QUOTN_STRT_DT as PRTY_QUOTN_STRT_DTTM,
exp_pass_to_target_ins.PRTY_ID as PRTY_ID,
exp_pass_to_target_ins.PRTY_QUOTN_END_DT as PRTY_QUOTN_END_DTTM,
exp_pass_to_target_ins.PRCS_ID as PRCS_ID,
exp_pass_to_target_ins.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_target_ins.out_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_target_ins.TRANS_START_DTTM1 as TRANS_STRT_DTTM,
exp_pass_to_target_ins.TRANS_END_DTTM11 as TRANS_END_DTTM
FROM
exp_pass_to_target_ins;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_prty_quotn1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_prty_quotn1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as TGT_QUOTN_ID,
$2 as TGT_PRTY_QUOTN_ROLE_CD,
$3 as TGT_PRTY_QUOTN_STRT_DTTM,
$4 as TGT_PRTY_ID,
$5 as TGT_PRTY_QUOTN_END_DTTM,
$6 as TGT_TRANS_STRT_DTTM,
$7 as TGT_EDW_STRT_DTTM,
$8 as TGT_EDW_END_DTTM,
$9 as SRC_QUOTN_ID,
$10 as SRC_PRTY_QUOTN_ROLE_CD,
$11 as SRC_PRTY_QUOTN_STRT_DTTM,
$12 as SRC_PRTY_ID,
$13 as SRC_PRTY_QUOTN_END_DTTM,
$14 as SRC_TRANS_STRT_DTTM,
$15 as SOURCE_DATA,
$16 as TARGET_DATA,
$17 as FLAG,
$18 as RETIRED,
$19 as RANK,
$20 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT DISTINCT 

PQ.QUOTN_ID,

PQ.PRTY_QUOTN_ROLE_CD,

PQ.PRTY_QUOTN_STRT_DTTM,

PQ.PRTY_ID,

PQ.PRTY_QUOTN_END_DTTM,

PQ.TRANS_STRT_DTTM,

PQ.EDW_STRT_DTTM,

PQ.EDW_END_DTTM,

SQ.QUOTN_ID,

SQ.PRTY_QUOTN_ROLE_CD,

SQ.EFF_DT AS PRTY_QUOTN_STRT_DTTM,

SQ.INDIV_PRTY_ID AS PRTY_ID,

SQ.END_DT AS PRTY_QUOTN_END_DTTM,

SQ.TRANS_STRT_DTTM  AS TRANS_STRT_DTTM,     





/* SOURCEMD5DATA */
CAST(TRIM(CAST(SQ.EFF_DT AS VARCHAR(100)))||TRIM(CAST(SQ.END_DT AS VARCHAR(100)))|| TRIM(SQ.INDIV_PRTY_ID)  AS VARCHAR(1000)) AS SOURCEDATA,

/* TARGETMD5DATA */
CAST(TRIM(CAST(PQ.PRTY_QUOTN_STRT_DTTM AS VARCHAR(100)))||TRIM(CAST(PQ.PRTY_QUOTN_END_DTTM AS VARCHAR(100)))||TRIM(PQ.PRTY_ID)  AS VARCHAR(1000)) AS TARGETDATA,



/* FLAG */


  CASE WHEN TARGETDATA IS NULL    THEN ''I''

            WHEN SOURCEDATA <> TARGETDATA  THEN ''U''

            ELSE  ''R'' 

             END AS INS_UPD_FLAG ,

             RETIRED,RNK



FROM

(

/* SOURCE QUERY  */
SELECT	DISTINCT ADDRESSBOOKUID, PARTY_TYPE,SRC_QRY.JOBNUMBER,SRC_QRY.BRANCHNUMBER,

			CAST(EFF_DT AS TIMESTAMP) EFF_DT,

		END_DT,TRANS_STRT_DTTM,XLAT_PRTY_QUOTN_ROLE.TYPECODE AS PRTY_QUOTN_ROLE_CD,

		SRC_QRY.TYPECODE,XLAT_SRC_CD.PC_SRC_CD,PRTY_TYPE

		,BUSN_CTGY,INS_QUOTN.QUOTN_ID,

		CASE WHEN (SRC_QRY.PARTY_TYPE  collate ''en-ci''  IN (''PERSON'',''ADJUDICATOR'',''VENDOR (PERSON)'',''ATTORNEY'', ''DOCTOR'',''DB_T_CORE_DM_PROD.policy PERSON'',''CONTACT'')) THEN LKP_INDIV_MGR.INDIV_PRTY_ID

                  WHEN (SRC_QRY.PARTY_TYPE collate ''en-ci''  IN (''COMPANY'',''VENDOR (COMPANY)'',''AUTO REPAIR SHOP'',''AUTO TOWING AGCY'',''LAW FIRM'', ''MEDICAL CARE ORGANIZATION'')) THEN LKP_BUSN.BUSN_PRTY_ID

                  WHEN (BUSN_CTGY collate ''en-ci'' =''INSCAR'') THEN LKP_BUSN.BUSN_PRTY_ID

                  WHEN (SRC_QRY.PARTY_TYPE collate ''en-ci''  IN (''USERCONTACT'')) THEN LKP_INDIV.INDIV_PRTY_ID

                  WHEN (PRTY_TYPE collate ''en-ci''  IN (''CO'',''PRDA'',''SRVCCTR'')) THEN INT_ORG.INTRNL_ORG_PRTY_ID

                  WHEN ((SRC_QRY.PARTY_TYPE collate ''en-ci''  NOT IN (''PERSON'',''ADJUDICATOR'',''VENDOR (PERSON)'',''ATTORNEY'', ''DOCTOR'',''DB_T_CORE_DM_PROD.policy PERSON'',''CONTACT'',''COMPANY'',''VENDOR (COMPANY)'',''AUTO REPAIR SHOP'',''AUTO TOWING AGCY'',''LAW FIRM'', ''MEDICAL CARE ORGANIZATION'',''USERCONTACT''))

                  AND (BUSN_CTGY collate ''en-ci''  NOT IN (''INSCAR'')) AND (PRTY_TYPE NOT IN (''CO'',''PRDA'',''SRVCCTR''))) THEN ''9999''                

		END AS INDIV_PRTY_ID,

		RETIRED , RNK

		from 

 (



SELECT  PRTY_QUOTN.ADDRESSBOOKUID, PRTY_QUOTN.PARTY_TYPE, PRTY_QUOTN.JOBNUMBER,

PRTY_QUOTN.BRANCHNUMBER,/*  ''INTRNL_ORG_TYPE15'' DB_T_PROD_CORE.INTRNL_ORG_TYPE, */
CASE    WHEN PRTY_QUOTN.EFF_DT IS NULL then  TO_DATE (''01/01/1900'', ''mm/DD/yyyy'') 

else     PRTY_QUOTN.eff_dt 

end as eff_dt , 

CASE    WHEN PRTY_QUOTN.END_DT IS NULL THEN  TO_TIMESTAMP (
  ''12/31/9999 23:59:59.999999'',
  ''mm/DD/yyyy hh24:mi:ss.ff6''
)

ELSE     

PRTY_QUOTN.END_DT  

END AS END_DT, 

PRTY_QUOTN.TRANS_STRT_DT AS TRANS_STRT_DTTM, 

PRTY_QUOTN.TYPECODE,  ''SRC_SYS4'' AS PC_SRC_CD, RETIRED ,

 RANK()  OVER(PARTITION BY  PRTY_QUOTN.JOBNUMBER, PRTY_QUOTN.BRANCHNUMBER,

        PRTY_QUOTN.TYPECODE  

ORDER   BY EFF_DT DESC)  AS RNK  

FROM    

 (

  SELECT DISTINCT

    CAST(pc_contact.AddressBookUID_stg AS VARCHAR(100)) AS ADDRESSBOOKUID, 

    pctl_contact.name_stg PARTY_TYPE,

    pc_job.JobNumber_stg JOBNUMBER,

    pc_policycontactrole.CreateTime_stg as eff_dt

    ,pc_policyperiod.PeriodEnd_stg as end_dt

    ,pc_policycontactrole.CreateTime_stg as trans_strt_dt

    ,pctl_policycontactrole.typecode_stg as typecode

    ,NULL AS TL_CNT_Name

    ,pc_policyperiod.branchnumber_stg BRANCHNUMBER

    ,case when pc_contact.Retired_stg=0 and pc_policyperiod.Retired_stg=0 and pc_job.Retired_stg=0 then 0 else 1 end as retired,

    (:start_dttm) as start_dttm,

        (:end_dttm) as end_dttm

FROM

    DB_T_PROD_STAG.pc_contact

    inner join DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg 

    inner join DB_T_PROD_STAG.pc_policycontactrole on pc_contact.id_stg = pc_policycontactrole.contactdenorm_stg

    inner join DB_T_PROD_STAG.pc_policyperiod on pc_policycontactrole.branchid_stg = pc_policyperiod.id_stg

    inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

    inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

    inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

    inner join DB_T_PROD_STAG.pctl_policycontactrole on pc_policycontactrole.subtype_stg= pctl_policycontactrole.id_stg

WHERE pc_contact.AddressBookUID_stg is not null

and pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'') and pctl_policyperiodstatus.TYPECODE_stg<>''Temporary''

and pctl_policycontactrole.typecode_stg<>''PolicyPriNamedInsured''        /*  Excluding PrimaryNamedInsured(commented) */
and (

(pc_policycontactrole.UpdateTime_stg > (:start_dttm)   and pc_policycontactrole.UpdateTime_stg <= (:end_dttm))

or

(pc_policyperiod.UpdateTime_stg > (:start_dttm)    and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

or

(pc_job.UpdateTime_stg > (:start_dttm)     and pc_job.UpdateTime_stg <= (:end_dttm))

)



    



UNION

/*  only PrimaryNamedInsured */
SELECT DISTINCT

    CAST(pc_contact.AddressBookUID_stg AS VARCHAR(100)) AS AddressBookUID_stg  , 

    pctl_contact.name_stg,

    pc_job.JobNumber_stg,

    pc_policycontactrole.Createtime_stg as eff_dt

    ,pc_policyperiod.PeriodEnd_stg as end_dt

    ,pc_policycontactrole.UpdateTime_stg as trans_strt_dt

    ,pctl_policycontactrole.typecode_stg as typecode

    ,NULL AS TL_CNT_Name

    ,pc_policyperiod.branchnumber_stg

    ,case when pc_contact.Retired_stg=0 and pc_policyperiod.Retired_stg=0 and pc_job.Retired_stg=0 then 0 else 1 end as Retired,

    (:start_dttm) as start_dttm,

        (:end_dttm) as end_dttm

from DB_T_PROD_STAG.pc_policyperiod

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.ID_stg=pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_contact on pc_policyperiod.PNIContactDenorm_stg=pc_contact.ID_stg

inner join DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg

join DB_T_PROD_STAG.pc_policycontactrole on pc_policycontactrole.branchid_stg = pc_policyperiod.id_stg

join DB_T_PROD_STAG.pctl_policycontactrole on pctl_policycontactrole.ID_stg=pc_policycontactrole.Subtype_stg

    inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

    inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

WHERE pc_contact.AddressBookUID_stg is not null

and pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'') and pctl_policyperiodstatus.TYPECODE_stg<>''Temporary''

and pctl_policycontactrole.typecode_stg=''PolicyPriNamedInsured''

and (

(pc_policycontactrole.UpdateTime_stg > (:start_dttm)   and pc_policycontactrole.UpdateTime_stg <= (:end_dttm))

or

(pc_policyperiod.UpdateTime_stg > (:start_dttm)    and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

or

(pc_job.UpdateTime_stg > (:start_dttm)     and pc_job.UpdateTime_stg <= (:end_dttm))

)





UNION

/****** UW Company************/

SELECT DISTINCT

    pctl_uwcompanycode.TYPECODE_stg as prty_nk, 

    ''INTRNL_ORG_SBTYPE1'' party_type,

    pc_job.JobNumber_stg,

    pc_policyperiod.CreateTime_stg as eff_dt

    ,pc_policyperiod.PeriodEnd_stg as end_dt

    ,pc_policyperiod.UpdateTime_stg as trans_strt_dt

    ,''PRTY_QUOTN_ROLE3'' as typecode

    ,NULL AS TL_CNT_Name

    ,pc_policyperiod.branchnumber_stg

    ,case when pc_contact.Retired_stg=0 and pc_policyperiod.Retired_stg=0 and pc_job.Retired_stg=0 then 0 else 1 end as Retired,

    (:start_dttm) as start_dttm,       (:end_dttm) as end_dttm

from 

DB_T_PROD_STAG.pc_contact

    inner join DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg 

    inner join DB_T_PROD_STAG.pc_policycontactrole on pc_contact.id_stg = pc_policycontactrole.contactdenorm_stg

inner join DB_T_PROD_STAG.pc_policyperiod on pc_policycontactrole.branchid_stg = pc_policyperiod.id_stg 

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_uwcompany ON pc_uwcompany.ID_stg  = pc_policyperiod.UWCompany_stg

inner join DB_T_PROD_STAG.pctl_uwcompanycode on pctl_uwcompanycode.id_stg=pc_uwcompany.code_stg

inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

where pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'') and pctl_policyperiodstatus.TYPECODE_stg<>''Temporary''

and (

(pc_policyperiod.UpdateTime_stg > (:start_dttm)    and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

or

(pc_job.UpdateTime_stg > (:start_dttm)     and pc_job.UpdateTime_stg <= (:end_dttm))

)

union

/******Producer************/

select  DISTINCT

    pc_producercode.Code_stg as prty_nk, 

    ''INTRNL_ORG_SBTYPE2'' party_type,

    pc_job.JobNumber_stg,

    pc_policyperiod.CreateTime_stg as eff_dt,

    coalesce(pc_effectivedatedfields.ExpirationDate_stg,pc_policyperiod.PeriodEnd_stg) as end_dt,

    pc_policyperiod.UpdateTime_stg as trans_strt_dt

    ,''PRTY_QUOTN_ROLE6'' as typecode

    ,NULL AS TL_CNT_Name

    ,pc_policyperiod.branchnumber_stg

    ,case when pc_contact.Retired_stg=0 and pc_policyperiod.Retired_stg=0 and pc_job.Retired_stg=0 then 0 else 1 end as Retired,

    (:start_dttm) as start_dttm,

        (:end_dttm) as end_dttm

from 

DB_T_PROD_STAG.pc_contact

    inner join DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg 

    inner join DB_T_PROD_STAG.pc_policycontactrole on pc_contact.id_stg = pc_policycontactrole.contactdenorm_stg

inner join DB_T_PROD_STAG.pc_policyperiod on pc_policycontactrole.branchid_stg = pc_policyperiod.id_stg 

 inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_effectivedatedfields on pc_policyperiod.id_stg=pc_effectivedatedfields.BranchID_stg

inner join DB_T_PROD_STAG.pc_producercode ON pc_producercode.ID_stg  = pc_effectivedatedfields.ProducerCodeID_stg

inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

where pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'') and pctl_policyperiodstatus.TYPECODE_stg<>''Temporary''

and pc_effectivedatedfields.ExpirationDate_stg is null

and (

(pc_effectivedatedfields.UpdateTime_stg > (:start_dttm)    and pc_effectivedatedfields.UpdateTime_stg <= (:end_dttm))

or

(pc_policyperiod.UpdateTime_stg > (:start_dttm)    and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

or

(pc_job.UpdateTime_stg > (:start_dttm)     and pc_job.UpdateTime_stg <= (:end_dttm))

)

union

/******Service Center************/

select DISTINCT

    pc_group.name_stg AS NAICCode,

    UWD.party_type AS party_type,

    pc_job.JobNumber_stg,

    pc_policyperiod.CreateTime_stg as eff_dt,

    coalesce(pc_effectivedatedfields.ExpirationDate_stg,pc_policyperiod.periodend_stg) as end_dt,

    pc_policyperiod.UpdateTime_stg as trans_strt_dt

    ,''PRTY_QUOTN_ROLE5'' as typecode

    ,NULL AS TL_CNT_Name

    ,pc_policyperiod.branchnumber_stg

    ,case when pc_contact.Retired_stg=0 and pc_policyperiod.Retired_stg=0 and pc_job.Retired_stg=0 then 0 else 1 end as Retired ,

    (:start_dttm) as start_dttm,       (:end_dttm) as end_dttm

 from 

 DB_T_PROD_STAG.pc_contact

    inner join DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg 

    inner join DB_T_PROD_STAG.pc_policycontactrole on pc_contact.id_stg = pc_policycontactrole.contactdenorm_stg

inner join DB_T_PROD_STAG.pc_policyperiod on pc_policycontactrole.branchid_stg = pc_policyperiod.id_stg 

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pc_effectivedatedfields on pc_policyperiod.id_stg=pc_effectivedatedfields.BranchID_stg 

INNER JOIN DB_T_PROD_STAG.PC_GROUP on pc_group.ID_stg=pc_effectivedatedfields.ServiceCenter_alfa_stg



inner join (select SVC.ID_stg as ServiceCenter_GroupID, SVC.Name_stg as ServiceCenter_Name, UWDist.Name_stg as UWDistrictName,pctl_grouptype.typecode_stg as party_type

from DB_T_PROD_STAG.PC_GROUP as SVC

join DB_T_PROD_STAG.pctl_grouptype on pctl_grouptype.ID_stg=SVC.GroupType_stg

left join DB_T_PROD_STAG.pcx_uwparentgroup_alfa on pcx_uwparentgroup_alfa.OwnerID_stg=SVC.ID_stg

left join DB_T_PROD_STAG.PC_GROUP as UWDist on UWDist.ID_stg=pcx_uwparentgroup_alfa.ForeignEntityID_stg

where pctl_grouptype.TYPECODE_stg=''servicecenter_alfa'') UWD ON UWD.ServiceCenter_Name=pc_group.Name_stg



inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

where pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'') and pctl_policyperiodstatus.TYPECODE_stg <>''Temporary''

and pc_effectivedatedfields.ExpirationDate_stg is null 

and (

(pc_effectivedatedfields.UpdateTime_stg > (:start_dttm)    and pc_effectivedatedfields.UpdateTime_stg <= (:end_dttm))

or

(pc_policyperiod.UpdateTime_stg > (:start_dttm)    and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

or

(pc_job.UpdateTime_stg > (:start_dttm)     and pc_job.UpdateTime_stg <= (:end_dttm))

)union



/***********************Capture Users such as underwriter,producer*************************/

select publicid_stg, usercontact, jobnumber_stg, eff_dt, end_Dt,

trans_strt_dt, typecode, tl_cnt_name, branchnumber_stg, retired ,start_dttm,end_dttm

from (

select DISTINCT

       pc_contact.PublicID_stg,

       ''UserContact'' usercontact,

       pc_job.JobNumber_stg,

pc_jobuserroleassign.CreateTime_stg eff_dt,

pc_policyperiod.periodend_stg as end_dt,

pc_jobuserroleassign.UpdateTime_stg as trans_strt_dt,

pctl_userrole.TYPECODE_stg AS typecode,

NULL AS TL_CNT_Name

       ,pc_policyperiod.branchnumber_stg

       ,case when pc_contact.Retired_stg=0 and pc_policyperiod.Retired_stg=0 and pc_job.Retired_stg=0 then 0 else 1 end as Retired,

    (:start_dttm) as start_dttm

    ,(:end_dttm) as end_dttm,

    row_number() over(partition by  pc_job.jobnumber_stg ,pctl_userrole.TYPECODE_stg,branchnumber_stg order by

    case when pc_jobuserroleassign.closedate_stg is null then cast(''1900-01-01'' as date) else pc_jobuserroleassign.closedate_stg end desc, pc_jobuserroleassign.UpdateTime_stg desc) rnk

from DB_T_PROD_STAG.pc_policyperiod join DB_T_PROD_STAG.pc_policy on pc_policyperiod.policyid_stg = pc_policy.id_stg

inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

       join DB_T_PROD_STAG.pc_jobuserroleassign on pc_job.id_stg = pc_jobuserroleassign.JobID_stg

       join DB_T_PROD_STAG.pctl_userrole on pc_jobuserroleassign.role_stg = pctl_userrole.id_stg

       join DB_T_PROD_STAG.pc_user on pc_jobuserroleassign.AssignedUserID_stg = pc_user.id_stg

       join DB_T_PROD_STAG.pc_contact on pc_contact.id_stg = pc_user.ContactID_stg

          inner join DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=pc_contact.Subtype_stg

where pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'') and pctl_policyperiodstatus.TYPECODE_stg <>''Temporary'' 

and  pctl_userrole.TYPECODE_stg<>''Producer''

and (pctl_userrole.TYPECODE_stg = ''Underwriter'' or (pctl_userrole.TYPECODE_stg <> ''Underwriter'' and pc_jobuserroleassign.CloseDate_stg is not null))

/*EIM-13802 , remove closedate filter and add rnk to get latest record*/

/* and pc_jobuserroleassign.CloseDate is not null */
and (

(pc_policyperiod.UpdateTime_stg > (:start_dttm)    and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

or

(pc_job.UpdateTime_stg > (:start_dttm)     and pc_job.UpdateTime_stg <= (:end_dttm))

)) A where rnk=1 

UNION

/***********************CreateUser and UpdateUser*************************/



select DISTINCT pc_contact.PublicID_stg,  

/* pc_contact.AddressBookUID, */
    ''UserContact'',

    pc_job.JobNumber_stg,

    pc_job.CreateTime_stg  as eff_dt,

    pc_policyperiod.PeriodEnd_stg as end_dt,

    pc_job.UpdateTime_stg as trans_strt_dt,

''PRTY_QUOTN_ROLE8'' AS typecode,

NULL AS TL_CNT_Name

,pc_policyperiod.branchnumber_stg

,case when pc_contact.Retired_stg=0 and pc_policyperiod.Retired_stg=0 and pc_job.Retired_stg=0 then 0 else 1 end as Retired,

(:start_dttm) as start_dttm,

        (:end_dttm) as end_dttm

from 

DB_T_PROD_STAG.pc_job

inner join DB_T_PROD_STAG.pctl_job on (pc_job.subtype_stg = pctl_job.id_stg and pctl_job.name_stg = ''Submission'')

inner join DB_T_PROD_STAG.pc_user on pc_job.createuserid_stg = pc_user.id_stg

inner join DB_T_PROD_STAG.pc_credential on pc_user.credentialID_stg = pc_credential.id_stg

inner join DB_T_PROD_STAG.pc_contact on pc_user.ContactID_stg = pc_contact.id_stg

inner join DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg

inner join DB_T_PROD_STAG.pc_policyperiod on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

where pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'') and pctl_policyperiodstatus.TYPECODE_stg<>''Temporary''

and (

(pc_policyperiod.UpdateTime_stg > (:start_dttm)    and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

or

(pc_job.UpdateTime_stg > (:start_dttm)     and pc_job.UpdateTime_stg <= (:end_dttm))

)



UNION

select DISTINCT

    pc_contact.PublicID_stg, 

/* pc_contact.AddressBookUID, */
    ''UserContact'',

    pc_job.JobNumber_stg,

    pc_job.CreateTime_stg as eff_dt,

    pc_policyperiod.PeriodEnd_stg as end_dt,

    pc_job.UpdateTime_stg as trans_strt_dt,

''PRTY_QUOTN_ROLE9'' AS typecode,

NULL AS TL_CNT_Name

,pc_policyperiod.branchnumber_stg

,case when pc_contact.Retired_stg=0 and pc_policyperiod.Retired_stg=0 and pc_job.Retired_stg=0 then 0 else 1 end as Retired,

(:start_dttm) as start_dttm,

        (:end_dttm) as end_dttm

from 

DB_T_PROD_STAG.pc_job

inner join DB_T_PROD_STAG.pctl_job on (pc_job.subtype_stg = pctl_job.id_stg and pctl_job.name_stg = ''Submission'')

inner join DB_T_PROD_STAG.pc_user on pc_job.UpdateUserID_stg = pc_user.id_stg

inner join DB_T_PROD_STAG.pc_credential on pc_user.credentialID_stg = pc_credential.id_stg

inner join DB_T_PROD_STAG.pc_contact on pc_user.ContactID_stg = pc_contact.id_stg

inner join DB_T_PROD_STAG.pctl_contact ON pctl_contact.id_stg = pc_contact.subtype_stg

inner join DB_T_PROD_STAG.pc_policyperiod on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

where pctl_job.TYPECODE_stg  in (''Submission'',''PolicyChange'',''Renewal'') and pctl_policyperiodstatus.TYPECODE_stg <>''Temporary''

and (

(pc_policyperiod.UpdateTime_stg > (:start_dttm)    and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

or

(pc_job.UpdateTime_stg > (:start_dttm)     and pc_job.UpdateTime_stg <= (:end_dttm))

)



union 



/****** Prior Insurance carrier for all DB_T_CORE_DM_PROD.policy periods******/

select  

coalesce(ppca.TYPECODE_stg,priorcarrier_farm.TYPECODE_stg) as prty_nk,

''BUSN_CTGY6'' party_type,

pc_job.JobNumber_stg,

coalesce(pc_effectivedatedfields.effectivedate_stg,pc_policyperiod.PeriodStart_stg) as eff_dt,

pc_policyperiod.PeriodEnd_stg as end_dt,

pc_effectivedatedfields.UpdateTime_stg as trans_strt_dt,

''PRTY_QUOTN_ROLE10'' as typecode,

NULL AS TL_CNT_Name,

pc_policyperiod.branchnumber_stg,

case when pc_policyperiod.retired_stg=0  and (ppca.retired_stg=0 or priorcarrier_farm.retired_stg=0) then 0 else 1 end as retired, 

(:start_dttm) as start_dttm,

(:end_dttm) as end_dttm

from DB_T_PROD_STAG.pc_policyperiod 

inner join DB_T_PROD_STAG.pc_job on pc_job.id_stg=pc_policyperiod.JobID_stg

inner join DB_T_PROD_STAG.pctl_job on pctl_job.id_stg=pc_job.Subtype_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.status_stg

inner join DB_T_PROD_STAG.pc_effectivedatedfields on pc_policyperiod.id_stg=pc_effectivedatedfields.branchid_stg

left join DB_T_PROD_STAG.pctl_priorcarrier_alfa ppca on ppca.id_stg=pc_effectivedatedfields.priorcarrier_alfa_stg

join  DB_T_PROD_STAG.pc_policyline on  pc_policyperiod.id_stg=pc_policyline.branchid_stg

left join  DB_T_PROD_STAG.pctl_priorcarrier_alfa priorcarrier_farm on  priorcarrier_farm.id_stg=pc_policyline.FOPPriorCarrier_stg

where pctl_job.typecode_stg  in (''Submission'',''PolicyChange'',''Renewal'') 

and pctl_policyperiodstatus.typecode_stg<>''Temporary''

and  pc_effectivedatedfields.expirationdate_stg is null

and (ppca.TYPECODE_stg is not null or priorcarrier_farm.TYPECODE_stg is not null)

and 

(

(pc_policyperiod.updatetime_stg > (:start_dttm) and pc_policyperiod.updatetime_stg <= (:end_dttm))

or 

(pc_effectivedatedfields.updatetime_stg > (:start_dttm) and pc_effectivedatedfields.updatetime_stg <= (:end_dttm))

)  ) PRTY_QUOTN

WHERE   PRTY_QUOTN.ADDRESSBOOKUID IS NOT NULL   

    AND PRTY_QUOTN.TYPECODE NOT IN (''PRTY_QUOTN_ROLE2'' , ''PolicyAddlInsured'',

        ''PolicyAddlNamedInsured'', ''PolicyDriver'',''HOPolOccLiabInsured_alfa'',

        ''PolicyAddlInterest'')   

 QUALIFY ROW_NUMBER ()  OVER  ( PARTITION BY  PRTY_QUOTN.JOBNUMBER,

        PRTY_QUOTN.BRANCHNUMBER,PRTY_QUOTN.TYPECODE 

ORDER   BY PRTY_QUOTN.EFF_DT DESC   )=1







UNION	



SELECT   DISTINCT 

    PC_AGENT_X.PUBLIC_ID, 

    ''USERCONTACT'' AS PARTY_TYPE, 

    PC_AGENT_X.JOBNUMBER,

    PC_AGENT_X.BRANCHNUMBER, 

/* '''' AS DB_T_PROD_CORE.INTRNL_ORG_TYPE, */
    EDITEFFECTIVEDATE AS EFF_DT, 

    TO_TIMESTAMP (
  ''12/31/9999 23:59:59.999999'',
  ''mm/DD/yyyy hh24:mi:ss.ff6''
) AS END_DT,

                

    PC_AGENT_X.TRANS_STRT_DT AS TRANS_STRT_DTTM, 

    ''PRTY_QUOTN_ROLE2'' AS TYPECODE, 

         ''SRC_SYS4'' AS PC_SRC_CD, 

         CASE 

    WHEN    POLICY_RETIRED=0 

    AND AGENT_RETIRED=0 THEN 0 

ELSE    1 

END AS RETIRED, 

 RANK()  OVER(PARTITION BY  JOBNUMBER, BRANCHNUMBER, TYPECODE  

ORDER   BY TRANS_STRT_DTTM DESC)  AS RNK 

    

FROM    

(

select  distinct code_stg as agntnumber

,pc_contact.PublicID_stg PUBLIC_ID

,pc_job.JobNumber_stg JOBNUMBER

,pc_policyperiod.BranchNumber_stg BRANCHNUMBER

,pc_policyperiod.PublicID_stg as nk_agmt_key

,pctl_policyperiodstatus.TYPECODE_stg as policyperiodsts_typecd

,pctl_job.TYPECODE_stg as pc_job_typecode,

case when pc_producercode.Retired_stg=0 and pc_contact.Retired_stg=0 then 0 else 1 end as Agent_Retired,

case when pc_policyperiod.Retired_stg=0 then 0 else 1 end as Policy_Retired,

pc_policyperiod.UpdateTime_stg as trans_strt_dt

,(:start_dttm) as start_dttm,

(:end_dttm) as end_dttm,

pc_policyperiod.EditEffectiveDate_stg EDITEFFECTIVEDATE,

pc_producercode.createtime_stg,

case when (pc_producercode.updatetime_stg> pc_policyperiod.UpdateTime_stg)

     Then pc_producercode.updatetime_stg

else pc_policyperiod.UpdateTime_stg end as updatetime/*  Added for EIM-18200 */
from    DB_T_PROD_STAG.pc_producercode

left outer join DB_T_PROD_STAG.pc_effectivedatedfields      on  pc_producercode.id_stg = pc_effectivedatedfields.ProducerCodeID_stg

left outer join DB_T_PROD_STAG.pc_policyperiod on  pc_effectivedatedfields.BranchID_stg = pc_policyperiod.id_stg

left outer join DB_T_PROD_STAG.pc_job on pc_policyperiod.JobID_stg=pc_job.ID_stg

left outer join DB_T_PROD_STAG.pctl_job on pc_job.subtype_stg=pctl_job.id_stg

left outer join DB_T_PROD_STAG.pc_userproducercode on  pc_producercode.id_stg = pc_userproducercode.producercodeid_stg

left outer join DB_T_PROD_STAG.pc_user on  pc_userproducercode.UserID_stg = pc_user.id_stg

left outer join DB_T_PROD_STAG.pc_userrole on pc_user.id_stg = pc_userrole.UserID_stg

left outer join DB_T_PROD_STAG.pc_role UserRole on pc_userrole.RoleID_stg = UserRole.ID_stg

left outer join DB_T_PROD_STAG.pc_contact on  pc_user.ContactID_stg = pc_contact.id_stg

left outer join DB_T_PROD_STAG.pctl_contact on  pc_contact.Subtype_stg = pctl_contact.ID_stg

left outer join DB_T_PROD_STAG.pc_producercoderole on  pc_producercode.id_stg = pc_producercoderole.ProducerCodeID_stg

left outer join DB_T_PROD_STAG.pc_role on  pc_producercoderole.RoleID_stg = pc_role.ID_stg

left join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.id_stg=pc_policyperiod.Status_stg

left outer join DB_T_PROD_STAG.pc_role ProducerCodeRole on  pc_producercoderole.RoleID_stg = ProducerCodeRole.ID_stg

where /* pctl_policyperiodstatus.TYPECODE=''bound'' and */
  pctl_contact.name_stg=''User Contact''

AND pc_role.name_stg=''Agent''

AND UserRole.name_stg in (''CSR'', ''Agent'')

 and pc_effectivedatedfields.ExpirationDate_stg is null

 and ((pc_producercode.UpdateTime_stg > (:start_dttm) and pc_producercode.UpdateTime_stg <= (:end_dttm))

or (pc_policyperiod.UpdateTime_stg > (:start_dttm) and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

)  

 ) PC_AGENT_X

WHERE   JOBNUMBER IS NOT NULL  

    AND PUBLIC_ID IS NOT NULL 

    AND PC_JOB_TYPECODE IN (''Submission'',''PolicyChange'',''Renewal'') 

    AND POLICYPERIODSTS_TYPECD <>''Temporary'' 

		QUALIFY ROW_NUMBER() OVER(PARTITION BY  JOBNUMBER, BRANCHNUMBER,

        TYPECODE 

ORDER   BY TRANS_STRT_DTTM DESC) =1



) SRC_QRY



/* TERADATA_ETL_REF_XLAT_PRTY_QUOTN_ROLE */
LEFT OUTER JOIN

(

SELECT	 DISTINCT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL 

	,

CASE	WHEN (LTRIM(RTRIM(TGT_IDNTFTN_VAL)) IS NULL) THEN ''UNK''

	ELSE (LTRIM(RTRIM(TGT_IDNTFTN_VAL))) 

END	AS TYPECODE	 

FROM	

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT TERADATA_ETL_REF_XLAT

WHERE	

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_QUOTN_ROLE''

	AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''PCTL_ACCOUNTCONTACTROLE.TYPECODE'',

		''PCTL_USERROLE.TYPECODE'',''PCTL_POLICYCONTACTROLE.TYPECODE'',''PCTL_ADDITIONALINTERESTTYPE.TYPECODE'',

		''BCTL_ACCOUNTROLE.TYPECODE'',''DERIVED'') 

	AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''GW'',''DS'') 

	AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''

)XLAT_PRTY_QUOTN_ROLE



	ON	SRC_QRY.TYPECODE=XLAT_PRTY_QUOTN_ROLE.SRC_IDNTFTN_VAL



/* TERADATA_ETL_REF_XLAT_SRC_CD */


LEFT OUTER JOIN

(

SELECT	 DISTINCT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS PC_SRC_CD

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL 

FROM	

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT TERADATA_ETL_REF_XLAT

WHERE	

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''DERIVED'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''

)XLAT_SRC_CD

	ON	SRC_QRY.PC_SRC_CD=XLAT_SRC_CD.SRC_IDNTFTN_VAL



/* TERADATA_ETL_REF_XLAT_PARTY_TYPE */
LEFT OUTER JOIN

(

SELECT	 DISTINCT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS PRTY_TYPE

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL 

FROM	

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT TERADATA_ETL_REF_XLAT

WHERE	

		TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INTRNL_ORG_SBTYPE''

)XLAT_PRTY_TYPE



	ON	SRC_QRY.PARTY_TYPE=XLAT_PRTY_TYPE.SRC_IDNTFTN_VAL





/* TERADATA_ETL_REF_XLAT_INTRNL_ORG_TYPE_SBTYPE */


/*LEFT OUTER JOIN

(

SELECT	 DISTINCT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS DB_T_PROD_CORE.INTRNL_ORG_TYPE

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS XLAT_INTRNL_ORG 

FROM	

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT TERADATA_ETL_REF_XLAT

WHERE	

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INTRNL_ORG_TYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''DERIVED'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''

)XLAT_INTRNL_ORG



	ON	SRC_QRY.PARTY_TYPE=	XLAT_INTRNL_ORG.XLAT_INTRNL_ORG

*/

/* TERADATA_ETL_REF_XLAT_BUSN_CTGY */


LEFT OUTER JOIN

(

SELECT	 DISTINCT 

 TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS BUSN_CTGY

 ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL 

FROM	

 DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT TERADATA_ETL_REF_XLAT

WHERE	 

 TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM IN (''BUSN_CTGY'',''ORG_TYPE'',

		''PRTY_TYPE'')

 AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM IN (''DERIVED'', ''CCTL_CONTACT.TYPECODE'',

		''CCTL_CONTACT.NAME'')

 AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''DS'',''GW'')

 AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''



)XLAT_BUSN_CTGY



	ON	SRC_QRY.PARTY_TYPE =XLAT_BUSN_CTGY.SRC_IDNTFTN_VAL



/* LKP_INDIV_CLM_CTR */


LEFT OUTER JOIN

(

SELECT	 DISTINCT 

	INDIV.INDIV_PRTY_ID AS INDIV_PRTY_ID, 

	INDIV.NK_PUBLC_ID AS NK_PUBLC_ID 

FROM	

	DB_T_PROD_CORE.INDIV INDIV

WHERE	

	INDIV.NK_PUBLC_ID IS NOT NULL

)LKP_INDIV



	ON	SRC_QRY.ADDRESSBOOKUID=LKP_INDIV.NK_PUBLC_ID



/* LKP_INDIV_CNT_MGR */


LEFT OUTER JOIN

(

SELECT DISTINCT

	INDIV.INDIV_PRTY_ID AS INDIV_PRTY_ID, 

	INDIV.NK_LINK_ID AS NK_LINK_ID 

FROM 

	DB_T_PROD_CORE.INDIV INDIV

WHERE

	INDIV.NK_PUBLC_ID IS NULL

	AND  CAST(EDW_END_DTTM AS DATE)=''9999-12-31''

)LKP_INDIV_MGR



ON SRC_QRY.ADDRESSBOOKUID=LKP_INDIV_MGR.NK_LINK_ID



/* -DB_T_PROD_CORE.BUSN */


LEFT OUTER JOIN

(

SELECT	DISTINCT BUSN.BUSN_PRTY_ID AS BUSN_PRTY_ID, BUSN.SRC_SYS_CD AS SRC_SYS_CD,

		BUSN.TAX_BRAKT_CD AS TAX_BRAKT_CD, BUSN.ORG_TYPE_CD AS ORG_TYPE_CD,

		BUSN.GICS_SBIDSTRY_CD AS GICS_SBIDSTRY_CD, BUSN.LIFCYCL_CD AS LIFCYCL_CD,

		BUSN.PRTY_TYPE_CD AS PRTY_TYPE_CD, BUSN.BUSN_END_DTTM AS BUSN_END_DTTM,

		BUSN.BUSN_STRT_DTTM AS BUSN_STRT_DTTM, BUSN.INC_IND AS INC_IND,

		BUSN.EDW_STRT_DTTM AS EDW_STRT_DTTM, BUSN.EDW_END_DTTM AS EDW_END_DTTM,

		BUSN.BUSN_CTGY_CD AS BUSN_CTGY_CD, BUSN.NK_BUSN_CD AS NK_BUSN_CD 

FROM	DB_T_PROD_CORE.BUSN BUSN

QUALIFY	ROW_NUMBER () OVER (PARTITION BY NK_BUSN_CD,BUSN_CTGY_CD 

ORDER	BY EDW_END_DTTM DESC )=1

)LKP_BUSN



	ON	SRC_QRY.ADDRESSBOOKUID=LKP_BUSN.NK_BUSN_CD 

	AND	BUSN_CTGY=LKP_BUSN.BUSN_CTGY_CD





/* DB_T_PROD_CORE.INTRNL_ORG */


LEFT OUTER JOIN

(

SELECT DISTINCT 	INTRNL_ORG.INTRNL_ORG_PRTY_ID AS INTRNL_ORG_PRTY_ID, INTRNL_ORG.INTRNL_ORG_TYPE_CD AS INTRNL_ORG_TYPE_CD,

		INTRNL_ORG.INTRNL_ORG_SBTYPE_CD AS INTRNL_ORG_SBTYPE_CD, INTRNL_ORG.INTRNL_ORG_NUM AS INTRNL_ORG_NUM,

		INTRNL_ORG.SRC_SYS_CD AS SRC_SYS_CD 

FROM	DB_T_PROD_CORE.INTRNL_ORG INTRNL_ORG 

WHERE	INTRNL_ORG_TYPE_CD=''INT''

AND CAST(EDW_END_DTTM AS DATE)=''9999-12-31''

/* QUALIFY ROW_NUMBER () OVER (PARTITION BY INTRNL_ORG_NUM,INTRNL_ORG_TYPE_CD,		INTRNL_ORG_SBTYPE_CD,SRC_SYS_CD ORDER	BY EDW_END_DTTM DESC)=1 */
) INT_ORG



	ON	SRC_QRY.ADDRESSBOOKUID=INT_ORG.INTRNL_ORG_NUM 

	AND PRTY_TYPE=INT_ORG.INTRNL_ORG_SBTYPE_CD 

AND	 XLAT_SRC_CD.PC_SRC_CD=INT_ORG.SRC_SYS_CD 





/* DB_T_PROD_CORE.INSRNC_QUOTN */


LEFT OUTER JOIN

(

SELECT	 DISTINCT INSRNC_QUOTN.QUOTN_ID AS QUOTN_ID, INSRNC_QUOTN.NK_JOB_NBR AS JOBNUMBER,

		INSRNC_QUOTN.VERS_NBR AS VERS_NBR 

FROM	DB_T_PROD_CORE.INSRNC_QUOTN INSRNC_QUOTN

WHERE CAST(EDW_END_DTTM AS DATE)=''9999-12-31''

/* QUALIFY	ROW_NUMBER() OVER(PARTITION BY  INSRNC_QUOTN.NK_JOB_NBR,		INSRNC_QUOTN.VERS_NBR,  INSRNC_QUOTN.SRC_SYS_CD  ORDER	BY INSRNC_QUOTN.EDW_END_DTTM DESC) = 1 */
)INS_QUOTN



	ON	 SRC_QRY.JOBNUMBER=INS_QUOTN.JOBNUMBER

	AND	SRC_QRY.BRANCHNUMBER=INS_QUOTN.VERS_NBR



) SQ

/* DB_T_PROD_CORE.PRTY_QUOTN */


LEFT OUTER JOIN

(



SELECT DISTINCT PRTY_QUOTN.PRTY_QUOTN_STRT_DTTM as PRTY_QUOTN_STRT_DTTM, PRTY_QUOTN.PRTY_QUOTN_END_DTTM as PRTY_QUOTN_END_DTTM, 

PRTY_QUOTN.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_QUOTN.EDW_END_DTTM as EDW_END_DTTM, PRTY_QUOTN.QUOTN_ID as QUOTN_ID, PRTY_QUOTN.TRANS_STRT_DTTM AS TRANS_STRT_DTTM,

PRTY_QUOTN.PRTY_ID as PRTY_ID, PRTY_QUOTN.PRTY_QUOTN_ROLE_CD as PRTY_QUOTN_ROLE_CD FROM DB_T_PROD_CORE.PRTY_QUOTN PRTY_QUOTN

WHERE CAST(EDW_END_DTTM AS DATE)=''9999-12-31''

QUALIFY ROW_NUMBER() OVER(PARTITION BY QUOTN_ID,PRTY_QUOTN_ROLE_CD ORDER BY EDW_END_DTTM desc) = 1



)PQ

ON SQ.QUOTN_ID=PQ.QUOTN_ID

AND SQ.PRTY_QUOTN_ROLE_CD=PQ.PRTY_QUOTN_ROLE_CD
) SRC
)
);


-- Component exp_pass_through1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through1 AS
(
SELECT
SQ_prty_quotn1.TGT_QUOTN_ID as TGT_QUOTN_ID,
SQ_prty_quotn1.TGT_PRTY_QUOTN_STRT_DTTM as TGT_PRTY_QUOTN_STRT_DTTM,
SQ_prty_quotn1.TGT_PRTY_QUOTN_END_DTTM as TGT_PRTY_QUOTN_END_DTTM,
SQ_prty_quotn1.TGT_EDW_STRT_DTTM as TGT_EDW_STRT_DTTM,
SQ_prty_quotn1.TGT_EDW_END_DTTM as TGT_EDW_END_DTTM,
SQ_prty_quotn1.SRC_QUOTN_ID as SRC_QUOTN_ID,
SQ_prty_quotn1.SRC_PRTY_QUOTN_ROLE_CD as SRC_PRTY_QUOTN_ROLE_CD,
SQ_prty_quotn1.SRC_PRTY_QUOTN_STRT_DTTM as SRC_PRTY_QUOTN_STRT_DTTM,
SQ_prty_quotn1.SRC_PRTY_ID as SRC_PRTY_ID,
SQ_prty_quotn1.SRC_PRTY_QUOTN_END_DTTM as SRC_PRTY_QUOTN_END_DTTM,
SQ_prty_quotn1.SRC_TRANS_STRT_DTTM as SRC_TRANS_STRT_DTTM,
:PRCS_ID as out_PRCS_ID,
CURRENT_TIMESTAMP as out_EDW_STRT_DTTM,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as out_EDW_END_DTTM,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as out_TRANS_END_DTTM,
SQ_prty_quotn1.FLAG as FLAG,
SQ_prty_quotn1.RETIRED as RETIRED,
SQ_prty_quotn1.RANK as RANK,
SQ_prty_quotn1.source_record_id
FROM
SQ_prty_quotn1
);


-- Component rtr_PRTY_QUOTN_FLG1_INSERT, Type ROUTER Output Group INSERT
create or replace temporary table rtr_PRTY_QUOTN_FLG1_INSERT as
SELECT
exp_pass_through1.TGT_QUOTN_ID as TGT_QUOTN_ID,
exp_pass_through1.TGT_PRTY_QUOTN_STRT_DTTM as TGT_PRTY_QUOTN_STRT_DTTM,
exp_pass_through1.TGT_PRTY_QUOTN_END_DTTM as TGT_PRTY_QUOTN_END_DTTM,
exp_pass_through1.TGT_EDW_STRT_DTTM as TGT_EDW_STRT_DTTM,
exp_pass_through1.TGT_EDW_END_DTTM as TGT_EDW_END_DTTM,
exp_pass_through1.SRC_QUOTN_ID as SRC_QUOTN_ID,
exp_pass_through1.SRC_PRTY_QUOTN_ROLE_CD as SRC_PRTY_QUOTN_ROLE_CD,
exp_pass_through1.SRC_PRTY_QUOTN_STRT_DTTM as SRC_PRTY_QUOTN_STRT_DTTM,
exp_pass_through1.SRC_PRTY_ID as SRC_PRTY_ID,
exp_pass_through1.SRC_PRTY_QUOTN_END_DTTM as SRC_PRTY_QUOTN_END_DTTM,
exp_pass_through1.SRC_TRANS_STRT_DTTM as SRC_TRANS_STRT_DTTM,
exp_pass_through1.out_PRCS_ID as PRCS_ID,
exp_pass_through1.out_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_through1.out_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_through1.out_TRANS_END_DTTM as TRANS_END_DTTM,
exp_pass_through1.FLAG as FLAG,
exp_pass_through1.RETIRED as RETIRED,
exp_pass_through1.RANK as RANK,
exp_pass_through1.source_record_id
FROM
exp_pass_through1
WHERE ( exp_pass_through1.FLAG = ''I'' OR exp_pass_through1.FLAG = ''U'' ) AND exp_pass_through1.SRC_PRTY_ID IS NOT NULL AND exp_pass_through1.SRC_QUOTN_ID IS NOT NULL OR ( exp_pass_through1.RETIRED = 0 AND exp_pass_through1.TGT_EDW_END_DTTM != TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AND exp_pass_through1.SRC_PRTY_ID IS NOT NULL );


-- Component rtr_PRTY_QUOTN_FLG1_RETIRED, Type ROUTER Output Group RETIRED
create or replace temporary table rtr_PRTY_QUOTN_FLG1_RETIRED as
SELECT
exp_pass_through1.TGT_QUOTN_ID as TGT_QUOTN_ID,
exp_pass_through1.TGT_PRTY_QUOTN_STRT_DTTM as TGT_PRTY_QUOTN_STRT_DTTM,
exp_pass_through1.TGT_PRTY_QUOTN_END_DTTM as TGT_PRTY_QUOTN_END_DTTM,
exp_pass_through1.TGT_EDW_STRT_DTTM as TGT_EDW_STRT_DTTM,
exp_pass_through1.TGT_EDW_END_DTTM as TGT_EDW_END_DTTM,
exp_pass_through1.SRC_QUOTN_ID as SRC_QUOTN_ID,
exp_pass_through1.SRC_PRTY_QUOTN_ROLE_CD as SRC_PRTY_QUOTN_ROLE_CD,
exp_pass_through1.SRC_PRTY_QUOTN_STRT_DTTM as SRC_PRTY_QUOTN_STRT_DTTM,
exp_pass_through1.SRC_PRTY_ID as SRC_PRTY_ID,
exp_pass_through1.SRC_PRTY_QUOTN_END_DTTM as SRC_PRTY_QUOTN_END_DTTM,
exp_pass_through1.SRC_TRANS_STRT_DTTM as SRC_TRANS_STRT_DTTM,
exp_pass_through1.out_PRCS_ID as PRCS_ID,
exp_pass_through1.out_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_pass_through1.out_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_through1.out_TRANS_END_DTTM as TRANS_END_DTTM,
exp_pass_through1.FLAG as FLAG,
exp_pass_through1.RETIRED as RETIRED,
exp_pass_through1.RANK as RANK,
exp_pass_through1.source_record_id
FROM
exp_pass_through1
WHERE exp_pass_through1.FLAG = ''R'' and exp_pass_through1.RETIRED != 0 and exp_pass_through1.TGT_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AND exp_pass_through1.SRC_PRTY_ID IS NOT NULL;


-- Component upd_stg_ins1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_stg_ins1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_PRTY_QUOTN_FLG1_INSERT.SRC_QUOTN_ID as QUOTN_ID,
rtr_PRTY_QUOTN_FLG1_INSERT.SRC_PRTY_QUOTN_ROLE_CD as PRTY_QUOTN_ROLE_CD,
rtr_PRTY_QUOTN_FLG1_INSERT.SRC_PRTY_QUOTN_STRT_DTTM as PRTY_QUOTN_STRT_DT,
rtr_PRTY_QUOTN_FLG1_INSERT.SRC_PRTY_ID as PRTY_ID,
rtr_PRTY_QUOTN_FLG1_INSERT.SRC_PRTY_QUOTN_END_DTTM as PRTY_QUOTN_END_DT,
rtr_PRTY_QUOTN_FLG1_INSERT.PRCS_ID as PRCS_ID,
rtr_PRTY_QUOTN_FLG1_INSERT.EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
rtr_PRTY_QUOTN_FLG1_INSERT.EDW_END_DTTM as in_EDW_END_DTTM1,
rtr_PRTY_QUOTN_FLG1_INSERT.RETIRED as Retired1,
rtr_PRTY_QUOTN_FLG1_INSERT.SRC_TRANS_STRT_DTTM as TRANS_START_DTTM1,
rtr_PRTY_QUOTN_FLG1_INSERT.RANK as Rank1,
0 as UPDATE_STRATEGY_ACTION,
rtr_PRTY_QUOTN_FLG1_INSERT.source_record_id
FROM
rtr_PRTY_QUOTN_FLG1_INSERT
);


-- Component exp_pass_to_target_ins1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_ins1 AS
(
SELECT
upd_stg_ins1.QUOTN_ID as QUOTN_ID,
upd_stg_ins1.PRTY_QUOTN_ROLE_CD as PRTY_QUOTN_ROLE_CD,
upd_stg_ins1.PRTY_QUOTN_STRT_DT as PRTY_QUOTN_STRT_DT,
upd_stg_ins1.PRTY_ID as PRTY_ID,
upd_stg_ins1.PRTY_QUOTN_END_DT as PRTY_QUOTN_END_DT,
upd_stg_ins1.PRCS_ID as PRCS_ID,
CASE WHEN upd_stg_ins1.Retired1 = 0 THEN upd_stg_ins1.in_EDW_END_DTTM1 ELSE CURRENT_TIMESTAMP END as out_EDW_END_DTTM,
upd_stg_ins1.TRANS_START_DTTM1 as TRANS_START_DTTM1,
CASE WHEN upd_stg_ins1.Retired1 <> 0 THEN upd_stg_ins1.TRANS_START_DTTM1 ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) END as TRANS_END_DTTM1,
dateadd (second,( 2 * ( upd_stg_ins1.Rank1 - 1 ) ), CURRENT_TIMESTAMP  ) as in_EDW_STRT_DTTM1,
upd_stg_ins1.source_record_id
FROM
upd_stg_ins1
);


-- Component upd_stg_upd_retired1, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_stg_upd_retired1 AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_PRTY_QUOTN_FLG1_RETIRED.SRC_QUOTN_ID as QUOTN_ID,
rtr_PRTY_QUOTN_FLG1_RETIRED.SRC_PRTY_QUOTN_ROLE_CD as PRTY_QUOTN_ROLE_CD,
rtr_PRTY_QUOTN_FLG1_RETIRED.SRC_PRTY_QUOTN_STRT_DTTM as PRTY_QUOTN_STRT_DT,
rtr_PRTY_QUOTN_FLG1_RETIRED.SRC_PRTY_ID as PRTY_ID,
NULL as PRTY_QUOTN_END_DT,
NULL as PRCS_ID,
NULL as EDW_expiry3,
rtr_PRTY_QUOTN_FLG1_RETIRED.TGT_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_PRTY_QUOTN_FLG1_RETIRED.TGT_PRTY_QUOTN_STRT_DTTM as lkp_PRTY_QUOTN_STRT_DT13,
rtr_PRTY_QUOTN_FLG1_RETIRED.EDW_STRT_DTTM as in_EDW_STRT_DTTM4,
rtr_PRTY_QUOTN_FLG1_RETIRED.SRC_TRANS_STRT_DTTM as TRANS_START_DTTM4,
1 as UPDATE_STRATEGY_ACTION,
rtr_PRTY_QUOTN_FLG1_RETIRED.source_record_id
FROM
rtr_PRTY_QUOTN_FLG1_RETIRED
);


-- Component exp_pass_to_target_upd_retired1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_upd_retired1 AS
(
SELECT
upd_stg_upd_retired1.QUOTN_ID as QUOTN_ID,
upd_stg_upd_retired1.PRTY_QUOTN_ROLE_CD as PRTY_QUOTN_ROLE_CD,
upd_stg_upd_retired1.PRTY_ID as PRTY_ID,
upd_stg_upd_retired1.lkp_PRTY_QUOTN_STRT_DT13 as lkp_PRTY_QUOTN_STRT_DT13,
upd_stg_upd_retired1.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
upd_stg_upd_retired1.in_EDW_STRT_DTTM4 as in_EDW_STRT_DTTM4,
upd_stg_upd_retired1.TRANS_START_DTTM4 as TRANS_START_DTTM4,
upd_stg_upd_retired1.source_record_id
FROM
upd_stg_upd_retired1
);


-- Component tgt_PRTY_QUOTN_upd_retired1, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_QUOTN
USING exp_pass_to_target_upd_retired1 ON (PRTY_QUOTN.QUOTN_ID = exp_pass_to_target_upd_retired1.QUOTN_ID AND PRTY_QUOTN.PRTY_QUOTN_ROLE_CD = exp_pass_to_target_upd_retired1.PRTY_QUOTN_ROLE_CD AND PRTY_QUOTN.PRTY_QUOTN_STRT_DTTM = exp_pass_to_target_upd_retired1.lkp_PRTY_QUOTN_STRT_DT13 AND PRTY_QUOTN.PRTY_ID = exp_pass_to_target_upd_retired1.PRTY_ID AND PRTY_QUOTN.EDW_STRT_DTTM = exp_pass_to_target_upd_retired1.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
QUOTN_ID = exp_pass_to_target_upd_retired1.QUOTN_ID,
PRTY_QUOTN_ROLE_CD = exp_pass_to_target_upd_retired1.PRTY_QUOTN_ROLE_CD,
PRTY_QUOTN_STRT_DTTM = exp_pass_to_target_upd_retired1.lkp_PRTY_QUOTN_STRT_DT13,
PRTY_ID = exp_pass_to_target_upd_retired1.PRTY_ID,
EDW_STRT_DTTM = exp_pass_to_target_upd_retired1.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_target_upd_retired1.in_EDW_STRT_DTTM4,
TRANS_END_DTTM = exp_pass_to_target_upd_retired1.TRANS_START_DTTM4;


-- Component tgt_PRTY_QUOTN_ins1, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_QUOTN
(
QUOTN_ID,
PRTY_QUOTN_ROLE_CD,
PRTY_QUOTN_STRT_DTTM,
PRTY_ID,
PRTY_QUOTN_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_target_ins1.QUOTN_ID as QUOTN_ID,
exp_pass_to_target_ins1.PRTY_QUOTN_ROLE_CD as PRTY_QUOTN_ROLE_CD,
exp_pass_to_target_ins1.PRTY_QUOTN_STRT_DT as PRTY_QUOTN_STRT_DTTM,
exp_pass_to_target_ins1.PRTY_ID as PRTY_ID,
exp_pass_to_target_ins1.PRTY_QUOTN_END_DT as PRTY_QUOTN_END_DTTM,
exp_pass_to_target_ins1.PRCS_ID as PRCS_ID,
exp_pass_to_target_ins1.in_EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_target_ins1.out_EDW_END_DTTM as EDW_END_DTTM,
exp_pass_to_target_ins1.TRANS_START_DTTM1 as TRANS_STRT_DTTM,
exp_pass_to_target_ins1.TRANS_END_DTTM1 as TRANS_END_DTTM
FROM
exp_pass_to_target_ins1;


-- PIPELINE END FOR 2
-- Component tgt_PRTY_QUOTN_upd_retired1, Type Post SQL 
UPDATE  DB_T_PROD_CORE.PRTY_QUOTN 
set TRANS_END_DTTM =  A.lead, 

EDW_END_DTTM = A.lead1
FROM

(SELECT	distinct QUOTN_ID, PRTY_QUOTN_ROLE_CD, EDW_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by QUOTN_ID, PRTY_QUOTN_ROLE_CD ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''  lead1,

max(TRANS_STRT_DTTM) over (partition by QUOTN_ID, PRTY_QUOTN_ROLE_CD ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' lead

FROM	DB_T_PROD_CORE.PRTY_QUOTN where PRTY_QUOTN_ROLE_CD NOT IN (''PLCYADDINS'',''PLCYADDNMINS'',''PLCYDRVR'',''OCCLIABINS'',''PLCYADDINT'')

 ) a



where  PRTY_QUOTN.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and PRTY_QUOTN.QUOTN_ID=A.QUOTN_ID 

AND PRTY_QUOTN.PRTY_QUOTN_ROLE_CD=A.PRTY_QUOTN_ROLE_CD

and PRTY_QUOTN.TRANS_STRT_DTTM <>PRTY_QUOTN.TRANS_END_DTTM

and lead is not null;


END; ';