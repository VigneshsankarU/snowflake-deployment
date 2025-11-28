-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_PRTY_FEAT_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare
	start_dttm timestamp;
	end_dttm timestamp;
	prcs_id integer;
	P_AGMT_TYPE_CD_POLICY_VERSION varchar;

BEGIN 
start_dttm :=current_timestamp();
end_dttm := current_timestamp();
prcs_id := 1;
P_AGMT_TYPE_CD_POLICY_VERSION := ''1'';

-- Component LKP_BUSN, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_BUSN AS
(
SELECT BUSN.BUSN_PRTY_ID as BUSN_PRTY_ID, BUSN.SRC_SYS_CD as SRC_SYS_CD, BUSN.TAX_BRAKT_CD as TAX_BRAKT_CD, BUSN.ORG_TYPE_CD as ORG_TYPE_CD, BUSN.GICS_SBIDSTRY_CD as GICS_SBIDSTRY_CD, BUSN.LIFCYCL_CD as LIFCYCL_CD, BUSN.PRTY_TYPE_CD as PRTY_TYPE_CD, BUSN.BUSN_END_DTTM as BUSN_END_DTTM, BUSN.BUSN_STRT_DTTM as BUSN_STRT_DTTM, BUSN.INC_IND as INC_IND, BUSN.EDW_STRT_DTTM as EDW_STRT_DTTM, BUSN.EDW_END_DTTM as EDW_END_DTTM, BUSN.BUSN_CTGY_CD as BUSN_CTGY_CD, BUSN.NK_BUSN_CD as NK_BUSN_CD 
FROM db_t_prod_core.BUSN 
QUALIFY ROW_NUMBER () OVER (PARTITION BY NK_BUSN_CD,BUSN_CTGY_CD ORDER BY EDW_END_DTTM DESC )=1
);


-- Component LKP_INDIV, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_INDIV AS
(
SELECT INDIV.INDIV_PRTY_ID AS INDIV_PRTY_ID,
         INDIV.INDIV_STRT_DTTM AS INDIV_STRT_DTTM,
          INDIV.BIRTH_DT AS BIRTH_DT, 
          INDIV.DEATH_DT AS DEATH_DT, 
          INDIV.GNDR_TYPE_CD AS GNDR_TYPE_CD, 
          INDIV.MM_OBJT_ID AS MM_OBJT_ID, 
          INDIV.ETHCTY_TYPE_CD AS ETHCTY_TYPE_CD,
           INDIV.TAX_BRAKT_CD AS TAX_BRAKT_CD,
            INDIV.VIP_TYPE_CD AS VIP_TYPE_CD,
             INDIV.RETIRMT_DT AS RETIRMT_DT,
              INDIV.EMPLMT_STRT_DT AS EMPLMT_STRT_DT, 
              INDIV.NTLTY_CD AS NTLTY_CD, INDIV.
              PRTY_DESC AS PRTY_DESC, 
              INDIV.INDIV_END_DTTM AS INDIV_END_DTTM, 
              INDIV.LIFCYCL_CD AS LIFCYCL_CD,
               INDIV.PRTY_TYPE_CD AS PRTY_TYPE_CD, 
               INDIV.INIT_DATA_SRC_TYPE_CD AS INIT_DATA_SRC_TYPE_CD,
                INDIV.SSN_TAX_NUM AS SSN_TAX_NUM,
                 INDIV.TAX_FILG_TYPE_CD AS TAX_FILG_TYPE_CD,
                  INDIV.NK_LINK_ID AS NK_LINK_ID,
                   INDIV.PRCS_ID AS PRCS_ID, 
                   INDIV.TAX_ID_STS_CD AS TAX_ID_STS_CD,
                    INDIV.EDW_STRT_DTTM AS EDW_STRT_DTTM, 
                    INDIV.EDW_END_DTTM AS EDW_END_DTTM,
                    INDIV.INDIV_CTGY_CD AS INDIV_CTGY_CD, 
                   CASE  INDIV.SRC_SYS_CD WHEN ''GWCC'' THEN ltrim(rtrim(INDIV.NK_PUBLC_ID)) WHEN ''CM'' THEN ltrim(rtrim(INDIV.NK_LINK_ID)) WHEN ''GWPC'' THEN ltrim(rtrim(INDIV.NK_PUBLC_ID)) 
when ''GWBC'' THEN ltrim(rtrim(INDIV.NK_PUBLC_ID)) END AS NK_PUBLC_ID1,
                    ltrim(rtrim(INDIV.SRC_SYS_CD)) AS SRC_SYS_CD,
INDIV.TRANS_STRT_DTTM as TRANS_STRT_DTTM
                    FROM db_t_prod_core.INDIV 
qualify row_number () over (partition by NK_PUBLC_ID1, SRC_SYS_CD  order by EDW_END_DTTM desc)=1
);


-- Component LKP_INDIV_CNT_MGR, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_INDIV_CNT_MGR AS
(
SELECT 

	INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID, 

	LOWER(INDIV.NK_LINK_ID) as NK_LINK_ID 

FROM 

	db_t_prod_core.INDIV

WHERE

	INDIV.NK_PUBLC_ID IS NULL
);


-- Component LKP_TERADATA_ETL_REF_XLAT_FEAT_INSRNC_SBTYPE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_FEAT_INSRNC_SBTYPE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_AGMT_ROLE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in (''pctl_policycontactrole.TYPECODE'',''derived'')

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''GW'',''DS'') 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''FEAT_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_PRTY_AGMT_ROLE_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_PRTY_AGMT_ROLE_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_AGMT_ROLE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in (''pctl_policycontactrole.TYPECODE'',''derived'')

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''GW'',''DS'') 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component sq_agmt_prty_feat, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_agmt_prty_feat AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Public_id,
$2 as FEAT_SBTYPE_CD,
$3 as FEAT_INSRNC_SBTYPE_CD,
$4 as typecode,
$5 as nk_src_key,
$6 as ADDRESSBOOKUID,
$7 as PRTY_AGMT_STRT_DT,
$8 as AGMT_FEAT_STRT_DT,
$9 as AGMT_FEAT_END_DT,
$10 as CNTCT_TYPE_CD,
$11 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT	distinct agmt_prty_feat.Public_Id, 

        agmt_prty_feat.FEAT_SBTYPE_CD,

		agmt_prty_feat.FEAT_INSRNC_SBTYPE_CD, 

		agmt_prty_feat.typecode_stg,

		agmt_prty_feat.nk_src_key, 

		agmt_prty_feat.ADDRESSBOOKUID, 

        agmt_prty_feat.PRTY_AGMT_STRT_DT,

		agmt_prty_feat.AGMT_FEAT_STRT_DT, 

		agmt_prty_feat.AGMT_FEAT_END_DT ,

		agmt_prty_feat.CNTCT_TYPE_CD

FROM

(

select 

cast(pc_policyperiod.PublicID_stg as Varchar(64)) as Public_Id,

cast(AddressBookUID_stg as Varchar(64)) as AddressBookUID,

cast(pctl_exclusiontype_alfa.TYPECODE_stg as Varchar(255)) as nk_src_key,

Cast(''FEAT_INSRNC_SBTYPE3'' as Varchar(50)) as FEAT_INSRNC_SBTYPE_CD ,

Cast(''FEAT_SBTYPE13'' as Varchar(50)) as FEAT_SBTYPE_CD, 

Cast(pctl_policycontactrole.TYPECODE_stg as Varchar(100)) as TYPECODE_stg ,

Cast(pc_policyperiod.PeriodStart_stg as date) as prty_agmt_strt_dt,

case when pc_policycontactrole.EffectiveDate_stg is null then pc_policyperiod.PeriodStart_stg else pc_policycontactrole.EffectiveDate_stg end as agmt_feat_strt_dt,

case when pc_policycontactrole.ExpirationDate_stg is null then pc_policyperiod.PeriodEnd_stg else pc_policycontactrole.ExpirationDate_stg end as agmt_feat_end_dt,

pc_policyperiod.updatetime_stg as updatetime,

cast(pctl_contact.TYPECODE_stg as Varchar(50)) as CNTCT_TYPE_CD

from DB_T_PROD_STAG.pc_policyperiod

inner join DB_T_PROD_STAG.pc_policycontactrole on pc_policyperiod.id_stg=pc_policycontactrole.BranchID_stg

inner join DB_T_PROD_STAG.pctl_policycontactrole on pctl_policycontactrole.id_stg=pc_policycontactrole.Subtype_stg

inner join DB_T_PROD_STAG.pctl_exclusiontype_alfa on pc_policycontactrole.ExclusionType_alfa_stg=pctl_exclusiontype_alfa.id_stg

inner join DB_T_PROD_STAG.pc_contact on pc_contact.ID_stg=pc_policycontactrole.ContactDenorm_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.ID_stg=pc_policyperiod.Status_stg 

inner join DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=pc_contact.Subtype_stg

where  pctl_policyperiodstatus.TYPECODE_stg=''Bound'' 

and AddressBookUID_stg is not null

and  pc_policyperiod.updatetime_stg> (:start_dttm) AND pc_policyperiod.updatetime_stg <= (:end_dttm)



UNION



select 

pc_policyperiod.PublicID_stg  as Public_Id,

AddressBookUID_stg  as AddressBookUID,

pctl_waivedreason_alfa.TYPECODE_stg as nk_src_key ,

''FEAT_INSRNC_SBTYPE4'' as FEAT_INSRNC_SBTYPE_CD ,

''FEAT_SBTYPE14''  as FEAT_SBTYPE_CD,

pctl_policycontactrole.TYPECODE_stg as TYPECODE_stg ,

cast(pc_policyperiod.PeriodStart_stg as date) as prty_agmt_strt_dt,

case when pc_policycontactrole.EffectiveDate_stg is null then pc_policyperiod.PeriodStart_stg else pc_policycontactrole.EffectiveDate_stg end as agmt_feat_strt_dt,

case when pc_policycontactrole.ExpirationDate_stg is null then pc_policyperiod.PeriodEnd_stg else pc_policycontactrole.ExpirationDate_stg end as agmt_feat_end_dt,

pc_policyperiod.updatetime_stg as updatetime,

pctl_contact.TYPECODE_stg as CNTCT_TYPE_CD

from DB_T_PROD_STAG.pc_policyperiod

inner join DB_T_PROD_STAG.pc_policycontactrole on pc_policyperiod.id_stg=pc_policycontactrole.BranchID_stg

inner join DB_T_PROD_STAG.pctl_policycontactrole on pctl_policycontactrole.id_stg=pc_policycontactrole.Subtype_stg

inner join DB_T_PROD_STAG.pctl_waivedreason_alfa on pc_policycontactrole.WaivedReason_alfa_stg=pctl_waivedreason_alfa.id_stg

inner join DB_T_PROD_STAG.pc_contact on pc_contact.ID_stg=pc_policycontactrole.ContactDenorm_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus on pctl_policyperiodstatus.ID_stg=pc_policyperiod.Status_stg

inner join DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=pc_contact.Subtype_stg

where pctl_policyperiodstatus.TYPECODE_stg=''Bound'' 

and AddressBookUID_stg is not null

and  pc_policyperiod.updatetime_stg> (:start_dttm) AND pc_policyperiod.updatetime_stg <= (:end_dttm)

/**Fix for defect 13484**/



UNION



select distinct

k.publicid_stg as Public_Id ,

e.addressbookuid_stg as AddressBookUID,

a.formpatterncode_stg as nk_src_key,

m.typecode_stg as FEAT_INSRNC_SBTYPE_CD,

''FEAT_SBTYPE15'' as  FEAT_SBTYPE_CD, 

n.TYPECODE_stg as TYPECODE_stg,

cast(k.PeriodStart_stg as date) as prty_agmt_strt_dt,

case when a.EffectiveDate_stg is null then k.PeriodStart_stg else a.EffectiveDate_stg end as agmt_feat_strt_dt,

case when a.ExpirationDate_stg is null then k.PeriodEnd_stg else a.ExpirationDate_stg end as agmt_feat_end_dt,

k.updatetime_stg as updatetime,

pctl_contact.TYPECODE_stg as CNTCT_TYPE_CD

from 

DB_T_PROD_STAG.pc_policyperiod k

inner join DB_T_PROD_STAG.pc_form a on a.branchid_stg=k.id_stg

inner join DB_T_PROD_STAG.pc_formassociation b on b.Form_stg = a.id_stg

inner join DB_T_PROD_STAG.pctl_formassociation c on c.id_stg = b.Subtype_stg

left join DB_T_PROD_STAG.pc_policycontactrole d on d.id_stg = b.PolicyContactRole_stg

left join DB_T_PROD_STAG.pc_contact e on e.id_stg = d.ContactDenorm_stg

inner join DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=e.Subtype_stg

inner join DB_T_PROD_STAG.pc_formpattern l on l.code_stg=a.formpatterncode_stg

inner join DB_T_PROD_STAG.pctl_documenttype m on m.id_stg=l.documenttype_stg

inner join DB_T_PROD_STAG.pctl_policycontactrole n on n.id_stg=d.Subtype_stg

inner join DB_T_PROD_STAG.pctl_policyperiodstatus o on o.ID_stg=k.Status_stg

where  o.TYPECODE_stg=''Bound'' 

and AddressBookUID_stg is not null

and  k.updatetime_stg> (:start_dttm) AND k.updatetime_stg <= (:end_dttm)

and m.typecode_stg = ''endorsement_alfa''

and l.Retired_stg = 0

and b.id_stg is not null





/**Fix for defect 13484 ends**/



) agmt_prty_feat  



QUALIFY	ROW_NUMBER() OVER(PARTITION BY agmt_prty_feat.Public_Id,agmt_prty_feat.ADDRESSBOOKUID ,nk_src_key   ORDER BY agmt_prty_feat.updatetime,agmt_prty_feat.agmt_feat_end_dt desc) = 1
) SRC
)
);


-- Component exp_pass_from_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source AS
(
SELECT
sq_agmt_prty_feat.Public_id as Public_id,
sq_agmt_prty_feat.FEAT_SBTYPE_CD as FEAT_SBTYPE_CD,
sq_agmt_prty_feat.FEAT_INSRNC_SBTYPE_CD as FEAT_INSRNC_SBTYPE_CD,
sq_agmt_prty_feat.typecode as typecode,
sq_agmt_prty_feat.nk_src_key as nk_src_key,
sq_agmt_prty_feat.ADDRESSBOOKUID as ADDRESSBOOKUID,
sq_agmt_prty_feat.PRTY_AGMT_STRT_DT as PRTY_AGMT_STRT_DT,
sq_agmt_prty_feat.AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DT,
sq_agmt_prty_feat.AGMT_FEAT_END_DT as AGMT_FEAT_END_DT,
sq_agmt_prty_feat.CNTCT_TYPE_CD as CNTCT_TYPE_CD,
sq_agmt_prty_feat.source_record_id
FROM
sq_agmt_prty_feat
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
exp_pass_from_source.Public_id as Public_id,
LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD */ as out_FEAT_SBTYPE_CD,
exp_pass_from_source.nk_src_key as nk_src_key,
LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PRTY_AGMT_ROLE_CD */ as out_PRTY_AGMT_ROLE_CD,
exp_pass_from_source.PRTY_AGMT_STRT_DT as PRTY_AGMT_STRT_DT,
exp_pass_from_source.AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DT,
exp_pass_from_source.AGMT_FEAT_END_DT as AGMT_FEAT_END_DT,
:PRCS_ID as out_PRCS_ID,
:P_AGMT_TYPE_CD_POLICY_VERSION as AGMT_TYPE_CD,
''GWPC'' as SYS_SRC_CD,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
CASE
  WHEN exp_pass_from_source.CNTCT_TYPE_CD IN (
    ''Person'',
    ''Adjudicator'',
    ''PersonVendor'',
    ''Attorney'',
    ''Doctor'',
    ''Policy Person'',
    ''Contact''
  ) THEN LKP_3.INDIV_PRTY_ID
  WHEN exp_pass_from_source.CNTCT_TYPE_CD IN (''User Contact'') THEN LKP_4.INDIV_PRTY_ID
  WHEN exp_pass_from_source.CNTCT_TYPE_CD IN (
    ''Company'',
    ''CompanyVendor'',
    ''LegalVenue'',
    ''Place'',
    ''Auto Repair Shop'',
    ''Auto Towing Agcy'',
    ''Law Firm'',
    ''Medical Care Organization''
  ) THEN LKP_5.BUSN_PRTY_ID
END AS out_PRTY_ID,
exp_pass_from_source.source_record_id,
row_number() over (partition by exp_pass_from_source.source_record_id order by exp_pass_from_source.source_record_id) as RNK
FROM
exp_pass_from_source
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = exp_pass_from_source.FEAT_SBTYPE_CD
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_PRTY_AGMT_ROLE_CD LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = exp_pass_from_source.typecode
LEFT JOIN LKP_INDIV_CNT_MGR LKP_3 ON LKP_3.NK_LINK_ID = exp_pass_from_source.ADDRESSBOOKUID
LEFT JOIN LKP_INDIV LKP_4 ON LKP_4.NK_PUBLC_ID1 = exp_pass_from_source.Public_id AND LKP_4.SRC_SYS_CD = ''GWPC''
LEFT JOIN LKP_BUSN LKP_5 ON LKP_5.BUSN_CTGY_CD = ''CO'' AND LKP_5.NK_BUSN_CD = exp_pass_from_source.Public_id
QUALIFY RNK = 1
);


-- Component LKP_AGMT_PPV, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_PPV AS
(
SELECT
LKP.AGMT_ID,
LKP.EDW_STRT_DTTM,
LKP.EDW_END_DTTM,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.HOST_AGMT_NUM asc,LKP.AGMT_NAME asc,LKP.AGMT_OPN_DTTM asc,LKP.AGMT_CLS_DTTM asc,LKP.AGMT_PLND_EXPN_DTTM asc,LKP.AGMT_SIGND_DTTM asc,LKP.AGMT_TYPE_CD asc,LKP.AGMT_LEGLY_BINDG_IND asc,LKP.AGMT_SRC_CD asc,LKP.AGMT_CUR_STS_CD asc,LKP.AGMT_CUR_STS_RSN_CD asc,LKP.AGMT_OBTND_CD asc,LKP.AGMT_SBTYPE_CD asc,LKP.AGMT_PRCSG_DTTM asc,LKP.ALT_AGMT_NAME asc,LKP.ASSET_LIABTY_CD asc,LKP.BAL_SHET_CD asc,LKP.STMT_CYCL_CD asc,LKP.STMT_ML_TYPE_CD asc,LKP.PRPOSL_ID asc,LKP.AGMT_OBJTV_TYPE_CD asc,LKP.FINCL_AGMT_SBTYPE_CD asc,LKP.MKT_RISK_TYPE_CD asc,LKP.ORIGNL_MATURTY_DT asc,LKP.RISK_EXPSR_MTGNT_SBTYPE_CD asc,LKP.BNK_TRD_BK_CD asc,LKP.PRCG_METH_SBTYPE_CD asc,LKP.FINCL_AGMT_TYPE_CD asc,LKP.DY_CNT_BSS_CD asc,LKP.FRST_PREM_DUE_DT asc,LKP.INSRNC_AGMT_SBTYPE_CD asc,LKP.INSRNC_AGMT_TYPE_CD asc,LKP.NTWK_SRVC_AGMT_TYPE_CD asc,LKP.FRMLTY_TYPE_CD asc,LKP.CNTRCT_TERM_NUM asc,LKP.RATE_RPRCG_CYCL_MTH_NUM asc,LKP.CMPND_INT_CYCL_MTH_NUM asc,LKP.MDTERM_INT_PMT_CYCL_MTH_NUM asc,LKP.PREV_MDTERM_INT_PMT_DT asc,LKP.NXT_MDTERM_INT_PMT_DT asc,LKP.PREV_INT_RATE_RVSD_DT asc,LKP.NXT_INT_RATE_RVSD_DT asc,LKP.PREV_REF_DT_INT_RATE asc,LKP.NXT_REF_DT_FOR_INT_RATE asc,LKP.MDTERM_CNCLTN_DT asc,LKP.STK_FLOW_CLAS_IN_MTH_IND asc,LKP.STK_FLOW_CLAS_IN_TERM_IND asc,LKP.LGCY_DSCNT_IND asc,LKP.AGMT_IDNTFTN_CD asc,LKP.TRMTN_TYPE_CD asc,LKP.INT_PMT_METH_CD asc,LKP.LBR_AGMT_DESC asc,LKP.GUARTD_IMPRSNS_CNT asc,LKP.COST_PER_IMPRSN_AMT asc,LKP.GUARTD_CLKTHRU_CNT asc,LKP.COST_PER_CLKTHRU_AMT asc,LKP.BUSN_PRTY_ID asc,LKP.PMT_PLN_TYPE_CD asc,LKP.INVC_STREM_TYPE_CD asc,LKP.MODL_CRTN_DTTM asc,LKP.CNTNUS_SRVC_DTTM asc,LKP.BILG_METH_TYPE_CD asc,LKP.SRC_SYS_CD asc,LKP.AGMT_EFF_DTTM asc,LKP.MODL_EFF_DTTM asc,LKP.PRCS_ID asc,LKP.MODL_ACTL_END_DTTM asc,LKP.TIER_TYPE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.VFYD_PLCY_IND asc,LKP.SRC_OF_BUSN_CD asc,LKP.NK_SRC_KEY asc,LKP.OVRD_COMS_TYPE_CD asc,LKP.LGCY_PLCY_IND asc,LKP.TRANS_STRT_DTTM asc) RNK
FROM
exp_data_transformation
LEFT JOIN (
SELECT AGMT.AGMT_ID as AGMT_ID, AGMT.HOST_AGMT_NUM as HOST_AGMT_NUM, AGMT.AGMT_NAME as AGMT_NAME, AGMT.AGMT_OPN_DTTM as AGMT_OPN_DTTM, AGMT.AGMT_CLS_DTTM as AGMT_CLS_DTTM, AGMT.AGMT_PLND_EXPN_DTTM as AGMT_PLND_EXPN_DTTM, AGMT.AGMT_SIGND_DTTM as AGMT_SIGND_DTTM, AGMT.AGMT_LEGLY_BINDG_IND as AGMT_LEGLY_BINDG_IND, AGMT.AGMT_SRC_CD as AGMT_SRC_CD, AGMT.AGMT_CUR_STS_CD as AGMT_CUR_STS_CD, AGMT.AGMT_CUR_STS_RSN_CD as AGMT_CUR_STS_RSN_CD, AGMT.AGMT_OBTND_CD as AGMT_OBTND_CD, AGMT.AGMT_SBTYPE_CD as AGMT_SBTYPE_CD, AGMT.AGMT_PRCSG_DTTM as AGMT_PRCSG_DTTM, AGMT.ALT_AGMT_NAME as ALT_AGMT_NAME, AGMT.ASSET_LIABTY_CD as ASSET_LIABTY_CD, AGMT.BAL_SHET_CD as BAL_SHET_CD, AGMT.STMT_CYCL_CD as STMT_CYCL_CD, AGMT.STMT_ML_TYPE_CD as STMT_ML_TYPE_CD, AGMT.PRPOSL_ID as PRPOSL_ID, AGMT.AGMT_OBJTV_TYPE_CD as AGMT_OBJTV_TYPE_CD, AGMT.FINCL_AGMT_SBTYPE_CD as FINCL_AGMT_SBTYPE_CD, AGMT.MKT_RISK_TYPE_CD as MKT_RISK_TYPE_CD, AGMT.ORIGNL_MATURTY_DT as ORIGNL_MATURTY_DT, AGMT.RISK_EXPSR_MTGNT_SBTYPE_CD as RISK_EXPSR_MTGNT_SBTYPE_CD, AGMT.BNK_TRD_BK_CD as BNK_TRD_BK_CD, AGMT.PRCG_METH_SBTYPE_CD as PRCG_METH_SBTYPE_CD, AGMT.FINCL_AGMT_TYPE_CD as FINCL_AGMT_TYPE_CD, AGMT.DY_CNT_BSS_CD as DY_CNT_BSS_CD, AGMT.FRST_PREM_DUE_DT as FRST_PREM_DUE_DT, AGMT.INSRNC_AGMT_SBTYPE_CD as INSRNC_AGMT_SBTYPE_CD, AGMT.INSRNC_AGMT_TYPE_CD as INSRNC_AGMT_TYPE_CD, AGMT.NTWK_SRVC_AGMT_TYPE_CD as NTWK_SRVC_AGMT_TYPE_CD, AGMT.FRMLTY_TYPE_CD as FRMLTY_TYPE_CD, AGMT.CNTRCT_TERM_NUM as CNTRCT_TERM_NUM, AGMT.RATE_RPRCG_CYCL_MTH_NUM as RATE_RPRCG_CYCL_MTH_NUM, AGMT.CMPND_INT_CYCL_MTH_NUM as CMPND_INT_CYCL_MTH_NUM, AGMT.MDTERM_INT_PMT_CYCL_MTH_NUM as MDTERM_INT_PMT_CYCL_MTH_NUM, AGMT.PREV_MDTERM_INT_PMT_DT as PREV_MDTERM_INT_PMT_DT, AGMT.NXT_MDTERM_INT_PMT_DT as NXT_MDTERM_INT_PMT_DT, AGMT.PREV_INT_RATE_RVSD_DT as PREV_INT_RATE_RVSD_DT, AGMT.NXT_INT_RATE_RVSD_DT as NXT_INT_RATE_RVSD_DT, AGMT.PREV_REF_DT_INT_RATE as PREV_REF_DT_INT_RATE, AGMT.NXT_REF_DT_FOR_INT_RATE as NXT_REF_DT_FOR_INT_RATE, AGMT.MDTERM_CNCLTN_DT as MDTERM_CNCLTN_DT, AGMT.STK_FLOW_CLAS_IN_MTH_IND as STK_FLOW_CLAS_IN_MTH_IND, AGMT.STK_FLOW_CLAS_IN_TERM_IND as STK_FLOW_CLAS_IN_TERM_IND, AGMT.LGCY_DSCNT_IND as LGCY_DSCNT_IND, AGMT.AGMT_IDNTFTN_CD as AGMT_IDNTFTN_CD, AGMT.TRMTN_TYPE_CD as TRMTN_TYPE_CD, AGMT.INT_PMT_METH_CD as INT_PMT_METH_CD, AGMT.LBR_AGMT_DESC as LBR_AGMT_DESC, AGMT.GUARTD_IMPRSNS_CNT as GUARTD_IMPRSNS_CNT, AGMT.COST_PER_IMPRSN_AMT as COST_PER_IMPRSN_AMT, AGMT.GUARTD_CLKTHRU_CNT as GUARTD_CLKTHRU_CNT, AGMT.COST_PER_CLKTHRU_AMT as COST_PER_CLKTHRU_AMT, AGMT.BUSN_PRTY_ID as BUSN_PRTY_ID, AGMT.PMT_PLN_TYPE_CD as PMT_PLN_TYPE_CD, AGMT.INVC_STREM_TYPE_CD as INVC_STREM_TYPE_CD, AGMT.MODL_CRTN_DTTM as MODL_CRTN_DTTM, AGMT.CNTNUS_SRVC_DTTM as CNTNUS_SRVC_DTTM, AGMT.BILG_METH_TYPE_CD as BILG_METH_TYPE_CD, AGMT.SRC_SYS_CD as SRC_SYS_CD, AGMT.AGMT_EFF_DTTM as AGMT_EFF_DTTM, AGMT.MODL_EFF_DTTM as MODL_EFF_DTTM, AGMT.PRCS_ID as PRCS_ID, AGMT.MODL_ACTL_END_DTTM as MODL_ACTL_END_DTTM, AGMT.TIER_TYPE_CD as TIER_TYPE_CD, AGMT.EDW_STRT_DTTM as EDW_STRT_DTTM, AGMT.EDW_END_DTTM as EDW_END_DTTM, AGMT.VFYD_PLCY_IND as VFYD_PLCY_IND, AGMT.SRC_OF_BUSN_CD as SRC_OF_BUSN_CD, AGMT.OVRD_COMS_TYPE_CD as OVRD_COMS_TYPE_CD, AGMT.LGCY_PLCY_IND as LGCY_PLCY_IND, AGMT.TRANS_STRT_DTTM as TRANS_STRT_DTTM, AGMT.NK_SRC_KEY as NK_SRC_KEY, AGMT.AGMT_TYPE_CD as AGMT_TYPE_CD FROM db_t_prod_core.AGMT QUALIFY ROW_NUMBER() OVER(PARTITION BY AGMT.NK_SRC_KEY,AGMT.HOST_AGMT_NUM  ORDER BY AGMT.EDW_END_DTTM desc) = 1
) LKP ON LKP.NK_SRC_KEY = exp_data_transformation.Public_id AND LKP.AGMT_TYPE_CD = exp_data_transformation.AGMT_TYPE_CD
QUALIFY RNK = 1
);


-- Component LKP_FEAT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_FEAT AS
(
SELECT
LKP.FEAT_ID,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.FEAT_ID desc,LKP.FEAT_SBTYPE_CD desc,LKP.NK_SRC_KEY desc,LKP.FEAT_INSRNC_SBTYPE_CD desc,LKP.FEAT_CLASFCN_CD desc,LKP.FEAT_DESC desc,LKP.FEAT_NAME desc,LKP.COMN_FEAT_NAME desc,LKP.FEAT_LVL_SBTYPE_CNT desc,LKP.INSRNC_CVGE_TYPE_CD desc,LKP.INSRNC_LOB_TYPE_CD desc,LKP.PRCS_ID desc) RNK
FROM
exp_data_transformation
LEFT JOIN (
SELECT FEAT.FEAT_ID as FEAT_ID, FEAT.FEAT_INSRNC_SBTYPE_CD as FEAT_INSRNC_SBTYPE_CD, FEAT.FEAT_CLASFCN_CD as FEAT_CLASFCN_CD, FEAT.FEAT_DESC as FEAT_DESC, FEAT.FEAT_NAME as FEAT_NAME, FEAT.COMN_FEAT_NAME as COMN_FEAT_NAME, FEAT.FEAT_LVL_SBTYPE_CNT as FEAT_LVL_SBTYPE_CNT, FEAT.INSRNC_CVGE_TYPE_CD as INSRNC_CVGE_TYPE_CD, FEAT.INSRNC_LOB_TYPE_CD as INSRNC_LOB_TYPE_CD, FEAT.PRCS_ID as PRCS_ID, FEAT.FEAT_SBTYPE_CD as FEAT_SBTYPE_CD, FEAT.NK_SRC_KEY as NK_SRC_KEY FROM db_t_prod_core.FEAT QUALIFY ROW_NUMBER() OVER(PARTITION BY NK_SRC_KEY, FEAT_SBTYPE_CD 
ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.FEAT_SBTYPE_CD = exp_data_transformation.out_FEAT_SBTYPE_CD AND LKP.NK_SRC_KEY = exp_data_transformation.nk_src_key
QUALIFY RNK = 1
);


-- Component LKP_AGMT_PRTY_FEAT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_PRTY_FEAT AS
(
SELECT
LKP.AGMT_ID,
LKP.PRTY_AGMT_ROLE_CD,
LKP.PRTY_AGMT_STRT_DTTM,
LKP.FEAT_ID,
LKP.PRTY_AGMT_FEAT_STRT_DT,
'''' as PRTY_AGMT_FEAT_END_DT,
LKP.PRTY_ID,
LKP.EDW_STRT_DTTM,
LKP_AGMT_PPV.AGMT_ID as in_AGMT_ID,
exp_data_transformation.out_PRTY_AGMT_ROLE_CD as in_PRTY_AGMT_ROLE_CD,
exp_data_transformation.PRTY_AGMT_STRT_DT as PRTY_AGMT_STRT_DT1,
LKP_FEAT.FEAT_ID as in_FEAT_ID,
exp_data_transformation.AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DT,
exp_data_transformation.out_PRTY_ID as in_INDIV_PRTY_ID,
exp_data_transformation.AGMT_FEAT_END_DT as in_AGMT_FEAT_END_DT,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.PRTY_AGMT_ROLE_CD asc,LKP.PRTY_AGMT_STRT_DTTM asc,LKP.FEAT_ID asc,LKP.PRTY_AGMT_FEAT_STRT_DT asc,LKP.PRTY_ID asc,LKP.EDW_STRT_DTTM asc) RNK
FROM
exp_data_transformation
INNER JOIN LKP_AGMT_PPV ON exp_data_transformation.source_record_id = LKP_AGMT_PPV.source_record_id
INNER JOIN LKP_FEAT ON LKP_AGMT_PPV.source_record_id = LKP_FEAT.source_record_id
LEFT JOIN (
SELECT	AGMT_PRTY_FEAT.PRTY_AGMT_ROLE_CD as PRTY_AGMT_ROLE_CD, AGMT_PRTY_FEAT.PRTY_AGMT_STRT_DTTM as PRTY_AGMT_STRT_DTTM,
		AGMT_PRTY_FEAT.FEAT_ID as FEAT_ID, AGMT_PRTY_FEAT.PRTY_AGMT_FEAT_STRT_DT as PRTY_AGMT_FEAT_STRT_DT,
		AGMT_PRTY_FEAT.PRTY_AGMT_FEAT_END_DT as PRTY_AGMT_FEAT_END_DT,
		AGMT_PRTY_FEAT.PRTY_ID as PRTY_ID, AGMT_PRTY_FEAT.EDW_STRT_DTTM as EDW_STRT_DTTM,
		AGMT_PRTY_FEAT.AGMT_ID as AGMT_ID 
FROM	db_t_prod_core.AGMT_PRTY_FEAT as AGMT_PRTY_FEAT WHERE CAST(EDW_END_DTTM as DATE)=''9999-12-31''
) LKP ON LKP.AGMT_ID = LKP_AGMT_PPV.AGMT_ID AND LKP.PRTY_ID = exp_data_transformation.out_PRTY_ID AND LKP.FEAT_ID = LKP_FEAT.FEAT_ID
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.AGMT_ID asc,LKP.PRTY_AGMT_ROLE_CD asc,LKP.PRTY_AGMT_STRT_DTTM asc,LKP.FEAT_ID asc,LKP.PRTY_AGMT_FEAT_STRT_DT asc,LKP.PRTY_ID asc,LKP.EDW_STRT_DTTM asc) 
= 1
);


-- Component exp_cdc, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_cdc AS
(
SELECT
LKP_AGMT_PRTY_FEAT.AGMT_ID as old_AGMT_ID,
LKP_AGMT_PRTY_FEAT.PRTY_AGMT_ROLE_CD as old_PRTY_AGMT_ROLE_CD,
LKP_AGMT_PRTY_FEAT.PRTY_AGMT_STRT_DTTM as old_PRTY_AGMT_STRT_DT,
LKP_AGMT_PRTY_FEAT.FEAT_ID as old_FEAT_ID,
LKP_AGMT_PRTY_FEAT.PRTY_AGMT_FEAT_STRT_DT as old_PRTY_AGMT_FEAT_STRT_DT,
LKP_AGMT_PRTY_FEAT.EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
LKP_AGMT_PRTY_FEAT.PRTY_ID as old_PRTY_ID,
LKP_AGMT_PRTY_FEAT.in_AGMT_ID as AGMT_ID,
LKP_AGMT_PRTY_FEAT.in_PRTY_AGMT_ROLE_CD as PRTY_AGMT_ROLE_CD,
LKP_AGMT_PRTY_FEAT.PRTY_AGMT_STRT_DT1 as PRTY_AGMT_STRT_DT,
LKP_AGMT_PRTY_FEAT.in_FEAT_ID as FEAT_ID,
LKP_AGMT_PRTY_FEAT.AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DT,
LKP_AGMT_PRTY_FEAT.in_INDIV_PRTY_ID as PRTY_ID,
NULL as OVRDN_FEAT_ID,
LKP_AGMT_PRTY_FEAT.in_AGMT_FEAT_END_DT as AGMT_FEAT_END_DT,
MD5 ( LKP_AGMT_PRTY_FEAT.PRTY_AGMT_ROLE_CD || TO_CHAR ( LKP_AGMT_PRTY_FEAT.PRTY_AGMT_STRT_DTTM ) || TO_CHAR ( LKP_AGMT_PRTY_FEAT.PRTY_AGMT_FEAT_STRT_DT ) ) as lkp_md5,
MD5 ( LKP_AGMT_PRTY_FEAT.in_PRTY_AGMT_ROLE_CD || TO_CHAR ( LKP_AGMT_PRTY_FEAT.PRTY_AGMT_STRT_DT1 ) || TO_CHAR ( DATE_TRUNC(DAY, LKP_AGMT_PRTY_FEAT.AGMT_FEAT_STRT_DT) ) ) as src_md5,
CASE WHEN lkp_md5 IS NULL THEN ''I'' ELSE CASE WHEN lkp_md5 != src_md5 THEN ''U'' ELSE ''R'' END END as INS_UPD,
LKP_AGMT_PRTY_FEAT.source_record_id
FROM
LKP_AGMT_PRTY_FEAT
);


-- Component rtr_ins_upd_INSERT, Type ROUTER Output Group INSERT
create or replace temporary table rtr_ins_upd_INSERT AS
SELECT
exp_cdc.old_AGMT_ID as old_AGMT_ID,
exp_cdc.old_PRTY_AGMT_ROLE_CD as old_PRTY_AGMT_ROLE_CD,
exp_cdc.old_PRTY_AGMT_STRT_DT as old_PRTY_AGMT_STRT_DT,
exp_cdc.old_FEAT_ID as old_FEAT_ID,
exp_cdc.old_PRTY_AGMT_FEAT_STRT_DT as old_PRTY_AGMT_FEAT_STRT_DT,
exp_cdc.old_PRTY_ID as old_PRTY_ID,
exp_cdc.AGMT_ID as AGMT_ID,
exp_cdc.PRTY_AGMT_ROLE_CD as PRTY_AGMT_ROLE_CD,
exp_cdc.PRTY_AGMT_STRT_DT as PRTY_AGMT_STRT_DT,
exp_cdc.FEAT_ID as FEAT_ID,
exp_cdc.AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DT,
exp_cdc.PRTY_ID as PRTY_ID,
exp_cdc.OVRDN_FEAT_ID as OVRDN_FEAT_ID,
exp_cdc.AGMT_FEAT_END_DT as AGMT_FEAT_END_DT,
exp_data_transformation.out_PRCS_ID as PRCS_ID,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
exp_cdc.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_cdc.INS_UPD as INS_UPD,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
LEFT JOIN exp_cdc ON exp_data_transformation.source_record_id = exp_cdc.source_record_id
WHERE exp_cdc.INS_UPD = ''I'' AND exp_cdc.AGMT_ID IS NOT NULL AND exp_cdc.FEAT_ID IS NOT NULL AND exp_cdc.PRTY_ID IS NOT NULL 
-- exp_cdc.old_AGMT_ID IS NULL AND exp_cdc.AGMT_ID IS NOT NULL AND CLM_ID IS NOT NULL
;


-- Component rtr_ins_upd_UPDATE, Type ROUTER Output Group UPDATE
CREATE OR REPLACE TEMPORARY TABLE rtr_ins_upd_UPDATE AS
SELECT
exp_cdc.old_AGMT_ID as old_AGMT_ID,
exp_cdc.old_PRTY_AGMT_ROLE_CD as old_PRTY_AGMT_ROLE_CD,
exp_cdc.old_PRTY_AGMT_STRT_DT as old_PRTY_AGMT_STRT_DT,
exp_cdc.old_FEAT_ID as old_FEAT_ID,
exp_cdc.old_PRTY_AGMT_FEAT_STRT_DT as old_PRTY_AGMT_FEAT_STRT_DT,
exp_cdc.old_PRTY_ID as old_PRTY_ID,
exp_cdc.AGMT_ID as AGMT_ID,
exp_cdc.PRTY_AGMT_ROLE_CD as PRTY_AGMT_ROLE_CD,
exp_cdc.PRTY_AGMT_STRT_DT as PRTY_AGMT_STRT_DT,
exp_cdc.FEAT_ID as FEAT_ID,
exp_cdc.AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DT,
exp_cdc.PRTY_ID as PRTY_ID,
exp_cdc.OVRDN_FEAT_ID as OVRDN_FEAT_ID,
exp_cdc.AGMT_FEAT_END_DT as AGMT_FEAT_END_DT,
exp_data_transformation.out_PRCS_ID as PRCS_ID,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
exp_cdc.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM,
exp_cdc.INS_UPD as INS_UPD,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
LEFT JOIN exp_cdc ON exp_data_transformation.source_record_id = exp_cdc.source_record_id
WHERE exp_cdc.INS_UPD = ''U'' AND exp_cdc.AGMT_ID IS NOT NULL AND exp_cdc.FEAT_ID IS NOT NULL AND exp_cdc.PRTY_ID IS NOT NULL;


-- Component upd_stg_upd_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_stg_upd_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_UPDATE.AGMT_ID as AGMT_ID,
rtr_ins_upd_UPDATE.PRTY_AGMT_ROLE_CD as PRTY_AGMT_ROLE_CD,
rtr_ins_upd_UPDATE.PRTY_AGMT_STRT_DT as PRTY_AGMT_STRT_DT,
rtr_ins_upd_UPDATE.FEAT_ID as FEAT_ID,
rtr_ins_upd_UPDATE.AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DT,
rtr_ins_upd_UPDATE.PRTY_ID as PRTY_ID,
rtr_ins_upd_UPDATE.OVRDN_FEAT_ID as OVRDN_FEAT_ID,
rtr_ins_upd_UPDATE.AGMT_FEAT_END_DT as AGMT_FEAT_END_DT,
rtr_ins_upd_UPDATE.PRCS_ID as PRCS_ID,
rtr_ins_upd_UPDATE.EDW_STRT_DTTM as EDW_STRT_DTTM3,
rtr_ins_upd_UPDATE.EDW_END_DTTM as EDW_END_DTTM3,
0 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_UPDATE.source_record_id
FROM
rtr_ins_upd_UPDATE
);


-- Component upd_stg_upd_upd, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_stg_upd_upd AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_UPDATE.old_AGMT_ID as AGMT_ID,
rtr_ins_upd_UPDATE.old_PRTY_AGMT_ROLE_CD as PRTY_AGMT_ROLE_CD,
rtr_ins_upd_UPDATE.old_PRTY_AGMT_STRT_DT as PRTY_AGMT_STRT_DT,
rtr_ins_upd_UPDATE.old_FEAT_ID as FEAT_ID,
rtr_ins_upd_UPDATE.old_PRTY_AGMT_FEAT_STRT_DT as PRTY_AGMT_FEAT_STRT_DT,
rtr_ins_upd_UPDATE.old_PRTY_ID as PRTY_ID,
rtr_ins_upd_UPDATE.OVRDN_FEAT_ID as OVRDN_FEAT_ID,
rtr_ins_upd_UPDATE.AGMT_FEAT_END_DT as AGMT_FEAT_END_DT,
rtr_ins_upd_UPDATE.PRCS_ID as PRCS_ID,
rtr_ins_upd_UPDATE.LKP_EDW_STRT_DTTM as LKP_EDW_STRT_DTTM3,
rtr_ins_upd_UPDATE.EDW_STRT_DTTM as EDW_STRT_DTTM3,
1 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_UPDATE.source_record_id
FROM
rtr_ins_upd_UPDATE
);


-- Component exp_pass_to_target_upd_upd, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_upd_upd AS
(
SELECT
upd_stg_upd_upd.AGMT_ID as AGMT_ID,
upd_stg_upd_upd.FEAT_ID as FEAT_ID,
upd_stg_upd_upd.PRTY_ID as PRTY_ID,
upd_stg_upd_upd.LKP_EDW_STRT_DTTM3 as LKP_EDW_STRT_DTTM3,
dateadd ( second, -1, upd_stg_upd_upd.EDW_STRT_DTTM3  ) as EDW_END_DTTM,
upd_stg_upd_upd.source_record_id
FROM
upd_stg_upd_upd
);


-- Component upd_stg_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_stg_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_ins_upd_INSERT.AGMT_ID as AGMT_ID,
rtr_ins_upd_INSERT.PRTY_AGMT_ROLE_CD as PRTY_AGMT_ROLE_CD,
rtr_ins_upd_INSERT.PRTY_AGMT_STRT_DT as PRTY_AGMT_STRT_DT,
rtr_ins_upd_INSERT.FEAT_ID as FEAT_ID,
rtr_ins_upd_INSERT.AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DT,
rtr_ins_upd_INSERT.PRTY_ID as PRTY_ID,
rtr_ins_upd_INSERT.OVRDN_FEAT_ID as OVRDN_FEAT_ID,
rtr_ins_upd_INSERT.AGMT_FEAT_END_DT as AGMT_FEAT_END_DT,
rtr_ins_upd_INSERT.PRCS_ID as PRCS_ID,
rtr_ins_upd_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_ins_upd_INSERT.EDW_END_DTTM as EDW_END_DTTM1,
0 as UPDATE_STRATEGY_ACTION,
rtr_ins_upd_INSERT.source_record_id
FROM
rtr_ins_upd_INSERT
);


-- Component exp_pass_to_target_upd_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_upd_ins AS
(
SELECT
upd_stg_upd_ins.AGMT_ID as AGMT_ID,
upd_stg_upd_ins.PRTY_AGMT_ROLE_CD as PRTY_AGMT_ROLE_CD,
upd_stg_upd_ins.PRTY_AGMT_STRT_DT as PRTY_AGMT_STRT_DT,
upd_stg_upd_ins.FEAT_ID as FEAT_ID,
upd_stg_upd_ins.AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DT,
upd_stg_upd_ins.PRTY_ID as PRTY_ID,
upd_stg_upd_ins.OVRDN_FEAT_ID as OVRDN_FEAT_ID,
upd_stg_upd_ins.AGMT_FEAT_END_DT as AGMT_FEAT_END_DT,
upd_stg_upd_ins.PRCS_ID as PRCS_ID,
upd_stg_upd_ins.EDW_STRT_DTTM3 as EDW_STRT_DTTM3,
upd_stg_upd_ins.EDW_END_DTTM3 as EDW_END_DTTM3,
upd_stg_upd_ins.source_record_id
FROM
upd_stg_upd_ins
);


-- Component exp_pass_to_target_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_ins AS
(
SELECT
upd_stg_ins.AGMT_ID as AGMT_ID,
upd_stg_ins.PRTY_AGMT_ROLE_CD as PRTY_AGMT_ROLE_CD,
upd_stg_ins.PRTY_AGMT_STRT_DT as PRTY_AGMT_STRT_DT,
upd_stg_ins.FEAT_ID as FEAT_ID,
upd_stg_ins.AGMT_FEAT_STRT_DT as AGMT_FEAT_STRT_DT,
upd_stg_ins.PRTY_ID as PRTY_ID,
upd_stg_ins.OVRDN_FEAT_ID as OVRDN_FEAT_ID,
upd_stg_ins.AGMT_FEAT_END_DT as AGMT_FEAT_END_DT,
upd_stg_ins.PRCS_ID as PRCS_ID,
upd_stg_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
upd_stg_ins.EDW_END_DTTM1 as EDW_END_DTTM1,
upd_stg_ins.source_record_id
FROM
upd_stg_ins
);


-- Component AGMT_PRTY_FEAT_upd_upd, Type TARGET 
MERGE INTO DB_T_PROD_CORE.AGMT_PRTY_FEAT
USING exp_pass_to_target_upd_upd ON (AGMT_PRTY_FEAT.AGMT_ID = exp_pass_to_target_upd_upd.AGMT_ID AND AGMT_PRTY_FEAT.FEAT_ID = exp_pass_to_target_upd_upd.FEAT_ID AND AGMT_PRTY_FEAT.PRTY_ID = exp_pass_to_target_upd_upd.PRTY_ID AND AGMT_PRTY_FEAT.EDW_STRT_DTTM = exp_pass_to_target_upd_upd.LKP_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
AGMT_ID = exp_pass_to_target_upd_upd.AGMT_ID,
FEAT_ID = exp_pass_to_target_upd_upd.FEAT_ID,
PRTY_ID = exp_pass_to_target_upd_upd.PRTY_ID,
EDW_STRT_DTTM = exp_pass_to_target_upd_upd.LKP_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_pass_to_target_upd_upd.EDW_END_DTTM;


-- Component AGMT_PRTY_FEAT_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_PRTY_FEAT
(
AGMT_ID,
PRTY_AGMT_ROLE_CD,
PRTY_AGMT_STRT_DTTM,
FEAT_ID,
PRTY_AGMT_FEAT_STRT_DT,
PRTY_ID,
OVRDN_FEAT_ID,
PRTY_AGMT_FEAT_END_DT,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_pass_to_target_ins.AGMT_ID as AGMT_ID,
exp_pass_to_target_ins.PRTY_AGMT_ROLE_CD as PRTY_AGMT_ROLE_CD,
exp_pass_to_target_ins.PRTY_AGMT_STRT_DT as PRTY_AGMT_STRT_DTTM,
exp_pass_to_target_ins.FEAT_ID as FEAT_ID,
exp_pass_to_target_ins.AGMT_FEAT_STRT_DT as PRTY_AGMT_FEAT_STRT_DT,
exp_pass_to_target_ins.PRTY_ID as PRTY_ID,
exp_pass_to_target_ins.OVRDN_FEAT_ID as OVRDN_FEAT_ID,
exp_pass_to_target_ins.AGMT_FEAT_END_DT as PRTY_AGMT_FEAT_END_DT,
exp_pass_to_target_ins.PRCS_ID as PRCS_ID,
exp_pass_to_target_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_target_ins.EDW_END_DTTM1 as EDW_END_DTTM
FROM
exp_pass_to_target_ins;


-- Component AGMT_PRTY_FEAT_upd_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_PRTY_FEAT
(
AGMT_ID,
PRTY_AGMT_ROLE_CD,
PRTY_AGMT_STRT_DTTM,
FEAT_ID,
PRTY_AGMT_FEAT_STRT_DT,
PRTY_ID,
OVRDN_FEAT_ID,
PRTY_AGMT_FEAT_END_DT,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_pass_to_target_upd_ins.AGMT_ID as AGMT_ID,
exp_pass_to_target_upd_ins.PRTY_AGMT_ROLE_CD as PRTY_AGMT_ROLE_CD,
exp_pass_to_target_upd_ins.PRTY_AGMT_STRT_DT as PRTY_AGMT_STRT_DTTM,
exp_pass_to_target_upd_ins.FEAT_ID as FEAT_ID,
exp_pass_to_target_upd_ins.AGMT_FEAT_STRT_DT as PRTY_AGMT_FEAT_STRT_DT,
exp_pass_to_target_upd_ins.PRTY_ID as PRTY_ID,
exp_pass_to_target_upd_ins.OVRDN_FEAT_ID as OVRDN_FEAT_ID,
exp_pass_to_target_upd_ins.AGMT_FEAT_END_DT as PRTY_AGMT_FEAT_END_DT,
exp_pass_to_target_upd_ins.PRCS_ID as PRCS_ID,
exp_pass_to_target_upd_ins.EDW_STRT_DTTM3 as EDW_STRT_DTTM,
exp_pass_to_target_upd_ins.EDW_END_DTTM3 as EDW_END_DTTM
FROM
exp_pass_to_target_upd_ins;


END; ';