-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PLCY_LOCTR_PRTY_CVGE_MTRC_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE
  END_DTTM STRING;
  PRCS_ID STRING;
  START_DTTM STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);

  END_DTTM := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
  PRCS_ID := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);
  START_DTTM := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);
 

-- Component SQ_pc_policyperiod, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_policyperiod AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as AGMT_ID,
$2 as FEAT_ID,
$3 as LOC_ID,
$4 as PRTY_ID,
$5 as LOC_NUM,
$6 as INSRNC_MTRC_TYPE_CD,
$7 as PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM,
$8 as PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM,
$9 as PLCY_LOCTR_PRTY_CVGE_MTRC_AMT,
$10 as TRANS_STRT_DTTM,
$11 as TRANS_END_DTTM,
$12 as sourcedata,
$13 as targetdata,
$14 as ins_upd_flag,
$15 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT

SRC1.SRC_AGMT_ID AS AGMT_ID

,SRC1.SRC_FEAT_ID AS FEAT_ID

,SRC1.SRC_LOC_ID AS LOC_ID



,SRC1.SRC_PRTY_ID AS PRTY_ID

,SRC1.SRC_LOC_NUM AS LOC_NUM

,SRC1.SRC_INSRNC_MTRC_TYPE_CD AS INSRNC_MTRC_TYPE_CD

,SRC1.SRC_PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM AS PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM

,SRC1.SRC_PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM AS PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM

,SRC1.SRC_PLCY_LOCTR_PRTY_CVGE_MTRC_AMT AS PLCY_LOCTR_PRTY_CVGE_MTRC_AMT

,SRC1.SRC_TRANS_STRT_DTTM AS TRANS_STRT_DTTM

,SRC1.SRC_TRANS_END_DTTM AS TRANS_END_DTTM

/* SourceData */
,CAST(TO_CHAR(cast(SRC_PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM AS TIMESTAMP)) ||

TRIM(COALESCE(cast(SRC_PLCY_LOCTR_PRTY_CVGE_MTRC_AMT as decimal(18,4)),''0''))

 as varchar(1100)) as sourcedata



/* TargetData */
,CAST(TO_CHAR(cast(LKP_TGT.PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM AS TIMESTAMP)) ||

TRIM(COALESCE(cast(LKP_TGT.PLCY_LOCTR_PRTY_CVGE_MTRC_AMT as decimal(18,4)),''0'')) 

 AS VARCHAR(1100)) AS targetdata



/* flag */
,case when targetdata is null then ''I''

           when targetdata is not null and  SourceData <> TargetData then ''U''

           when targetdata  is not null and  SourceData = TargetData then ''R''  end as ins_upd_flag

from

(

Select distinct

SRC.PUBLICID,

SRC.PATTERNCODE,

SRC.LOC_NUM AS SRC_LOC_NUM,

SRC.INSRNC_MTRC_TYPE_CD,

LKP_INSRNC_MTRC.TGT_IDNTFTN_VAL as SRC_INSRNC_MTRC_TYPE_CD,

SRC.PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM as SRC_PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM,

SRC.PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM as SRC_PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM,

SRC.PLCY_LOCTR_PRTY_CVGE_MTRC_AMT as SRC_PLCY_LOCTR_PRTY_CVGE_MTRC_AMT,

SRC.TRANS_STRT_DTTM as SRC_TRANS_STRT_DTTM,

CAST(''9999-12-31 23:59:59.999999'' as TIMESTAMP) as SRC_TRANS_END_DTTM,

SRC.COUNTYINTERNAL,

SRC.POSTALCODEINTERNAL,

SRC.CITYINTERNAL,

SRC.ADDRESSLINE1INTERNAL,

SRC.ADDRESSLINE2INTERNAL,

SRC.ADDRESSLINE3INTERNAL,

SRC.STATETYPECODE,

SRC.COUNTRYTYPECODE

,LKP_CITY.CITY_ID

,LKP_TERR.TERR_ID

,LKP_POSTL_CD.POSTL_CD_ID

,LKP_CTRY.CTRY_ID

,LKP_CNTY.CNTY_ID

,LKP_STREET_ADDR.STREET_ADDR_ID AS SRC_LOC_ID

,LKP_AGMT_PPV.AGMT_ID AS SRC_AGMT_ID

,LKP_FEAT.FEAT_ID AS SRC_FEAT_ID

/* ,LKP_PRTY_ID.PRTY_ID AS SRC_PRTY_ID */
,CASE WHEN SRC.CMP_Name = ''Company'' THEN LKP_BUSN_PRTY.BUSN_PRTY_ID ELSE LKP_INDIV.INDIV_PRTY_ID END AS SRC_PRTY_ID

from 

(

SELECT 

PUBLICID

,PATTERNCODE

,Insrnc_Mtrc_Type_Cd

,LOC_NUM

,SUM(PLCY_LOCTR_PRTY_CVGE_MTRC_AMT) as PLCY_LOCTR_PRTY_CVGE_MTRC_AMT

,PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM

,PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM

,TRANS_STRT_DTTM

,addressbookUID

,CMP_Name

,addressline1internal

,addressline2internal

,addressline3internal

,countrytypecode

,postalcodeinternal

,statetypecode

,cityinternal

,countyinternal

FROM

(

/* Tranprem  */
select distinct pp.publicid_stg as "PUBLICID"  /*  AGMT_ID */
, locsch.patterncode_Stg  as PATTERNCODE  /* FEAT_ID */
, cast(''INSRNC_MTRC_TYPE16'' as varchar(100)) as Insrnc_Mtrc_Type_Cd /* For TRANPREM rows */
, ploc.locationnum_Stg as "LOC_NUM"

, tx.amount_stg as "PLCY_LOCTR_PRTY_CVGE_MTRC_AMT"

, pp.EditEffectiveDate_stg as PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM   

,CAST(''9999-12-31 23:59:59.999999'' as TIMESTAMP) as PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM

,case when (tx.UpdateTime_stg > pp.UpdateTime_stg ) THEN tx.UpdateTime_stg else pp.UpdateTime_stg end as TRANS_STRT_DTTM 

, CAST(''9999-12-31 23:59:59.999999'' as TIMESTAMP) as TRANS_END_DTTM 



/* ,c.addressbookUID_stg as addressbookUID  --PRTY_ID */


,case when (c.mortgageelienholdernumber_alfa_stg is not null and c.addressbookUID_stg NOT LIKE ''%MORT%'') then c.mortgageelienholdernumber_alfa_stg 

else c.addressbookUID_stg end as   addressbookUID   /* PRTY_ID */
								

,pctl.Name_stg as CMP_Name   /* PRTY_ID */
,ploc.addressline1internal_stg  as addressline1internal /* LOC_ID */
,ploc.addressline2internal_stg  as addressline2internal

,ploc.addressline3internal_stg  as addressline3internal

,pctl_country.Typecode_stg  as countrytypecode

,ploc.postalcodeinternal_stg  as postalcodeinternal

,pctl_state.typecode_stg  as statetypecode

,ploc.cityinternal_stg  as cityinternal

,ploc.countyinternal_stg   as countyinternal  /* LOC_ID */


from db_t_prod_stag.pc_policyperiod pp 

    join db_t_prod_stag.pc_policyline pl  on pl.branchid_stg = pp.id_stg

    join db_t_prod_stag.pcx_bp7transaction tx on tx.branchid_stg = pp.id_stg

    join db_t_prod_stag.pcx_bp7cost cost on cost.id_stg = tx.bp7cost_stg

join db_t_prod_stag.pcx_bp7locschedcovitemcov locsch on locsch.fixedid_stg = cost.LocSchedCovItemCov_stg and locsch.branchid_stg = cost.branchid_Stg /* location sch cov item */
    join db_t_prod_stag.pcx_BP7LocSchedCovItem covitm on covitm.fixedid_stg = locsch.locschedcovitem_stg and covitm.branchid_stg = pp.id_stg

    join db_t_prod_stag.pc_policyaddlinsureddetail aid on aid.fixedid_stg = covitm.additionalinsured_stg and aid.branchid_stg = pp.id_stg

    join db_t_prod_stag.pc_policycontactrole pcr on pcr.fixedid_stg = aid.policyaddlinsured_stg and pcr.branchid_stg = pp.id_stg

    join db_t_prod_stag.pcx_BP7LocationCov lc on lc.fixedid_stg = covitm.schedule_stg and lc.branchid_stg = pp.id_stg

    join db_t_prod_stag.pcx_bp7location bp7loc on bp7loc.fixedid_stg = lc.location_stg and bp7loc.branchid_stg = pp.id_stg and bp7loc.expirationdate_stg is null

    join db_t_prod_stag.pc_etlclausepattern clause2 on clause2.patternid_stg = locsch.patterncode_stg

    join db_t_prod_stag.pc_policylocation ploc on ploc.id_stg = bp7loc.location_Stg

	join db_t_prod_stag.pc_contact c on c.id_stg = pcr.contactdenorm_stg

	join db_t_prod_stag.pctl_contact pctl on pctl.id_stg = c.Subtype_stg


	left join db_t_prod_stag.pctl_country ON pctl_country.id_stg = ploc.CountryInternal_stg

	left join db_t_prod_stag.pctl_state ON ploc.StateInternal_stg = pctl_state.id_stg


where pp.status_stg = 9

  and pl.PatternCode_stg = ''BP7Line''

/* and pp.publicid_stg = ''prodpc:12557575''  */
/* and pp.publicid_stg = ''prodpc:11332779'' */
  and locsch.id_stg is not null

  and ((tx.updatetime_stg > (:start_dttm)

  and tx.updatetime_stg <= (:end_dttm)) OR 

  (pp.UpdateTime_stg > (:start_dttm)

  and pp.UpdateTime_stg <= (:end_dttm)))

/* order by 1,2,3 */
) a

GROUP BY PUBLICID

,PATTERNCODE

,Insrnc_Mtrc_Type_Cd

,LOC_NUM

,PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM

,PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM

,TRANS_STRT_DTTM

,addressbookUID

,CMP_NAME

,addressline1internal

,addressline2internal

,addressline3internal

,countrytypecode

,postalcodeinternal

,statetypecode

,cityinternal

,countyinternal

)SRC



LEFT JOIN

(

SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	--EVIEWDB_EDW.
  db_t_prod_core.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''INSRNC_MTRC_TYPE''

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM=''derived''

			AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''

)LKP_INSRNC_MTRC ON LKP_INSRNC_MTRC.SRC_IDNTFTN_VAL=SRC.INSRNC_MTRC_TYPE_CD





LEFT JOIN

(

SELECT CTRY.CTRY_ID as CTRY_ID, CTRY.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME 

FROM --EVIEWDB_EDW
db_t_prod_core.CTRY CTRY

WHERE CAST(EDW_END_DTTM AS DATE)=''9999-12-31''

) LKP_CTRY 

ON LKP_CTRY.GEOGRCL_AREA_SHRT_NAME = SRC.countrytypecode



LEFT JOIN 

(

SELECT POSTL_CD.POSTL_CD_ID as POSTL_CD_ID, POSTL_CD.CTRY_ID as CTRY_ID, POSTL_CD.POSTL_CD_NUM as POSTL_CD_NUM 

FROM --EVIEWDB_EDW.
db_t_prod_core.POSTL_CD POSTL_CD

WHERE CAST(EDW_END_DTTM AS DATE)=''9999-12-31''

) LKP_POSTL_CD

ON LKP_POSTL_CD.CTRY_ID = LKP_CTRY.CTRY_ID

AND LKP_POSTL_CD.POSTL_CD_NUM = SRC.postalcodeinternal



LEFT JOIN 

(

SELECT TERR.TERR_ID as TERR_ID, TERR.EDW_END_DTTM as EDW_END_DTTM, TERR.CTRY_ID as CTRY_ID, TERR.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME 

 FROM --EVIEWDB_EDW.
 db_t_prod_core.TERR TERR

 WHERE CAST(EDW_END_DTTM AS DATE)=''9999-12-31''

) LKP_TERR

ON LKP_TERR.CTRY_ID  =  LKP_CTRY.CTRY_ID

AND LKP_TERR.GEOGRCL_AREA_SHRT_NAME   =  SRC.statetypecode



LEFT JOIN 

(

SELECT CITY.CITY_ID as CITY_ID, CITY.EDW_END_DTTM as EDW_END_DTTM, CITY.TERR_ID as TERR_ID, 

CITY.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME 

FROM --EVIEWDB_EDW.CITY 
db_t_prod_core.CITY

WHERE CAST(EDW_END_DTTM AS DATE)=''9999-12-31''

) LKP_CITY

ON LKP_CITY.TERR_ID  = LKP_TERR.TERR_ID

AND LKP_CITY.GEOGRCL_AREA_SHRT_NAME =  SRC.cityinternal



LEFT JOIN 

(

SELECT CNTY.CNTY_ID as CNTY_ID,CNTY.EDW_END_DTTM as EDW_END_DTTM, CNTY.TERR_ID as TERR_ID, CNTY.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME 

FROM --EVIEWDB_EDW.CNTY 
db_t_prod_core.CNTY

QUALIFY ROW_NUMBER() OVER(PARTITION BY TERR_ID,GEOGRCL_AREA_SHRT_NAME ORDER BY EDW_END_DTTM desc) = 1



)LKP_CNTY

ON LKP_CNTY.TERR_ID = LKP_TERR.TERR_ID

AND LKP_CNTY.GEOGRCL_AREA_SHRT_NAME =  SRC.countyinternal



LEFT JOIN 

(

SELECT STREET_ADDR.STREET_ADDR_ID as STREET_ADDR_ID,

STREET_ADDR.EDW_STRT_DTTM as EDW_STRT_DTTM, STREET_ADDR.EDW_END_DTTM as EDW_END_DTTM,

STREET_ADDR.ADDR_LN_1_TXT as ADDR_LN_1_TXT, STREET_ADDR.ADDR_LN_2_TXT as ADDR_LN_2_TXT, STREET_ADDR.ADDR_LN_3_TXT as ADDR_LN_3_TXT, STREET_ADDR.CITY_ID as CITY_ID, 

 STREET_ADDR.TERR_ID as TERR_ID, STREET_ADDR.POSTL_CD_ID as POSTL_CD_ID, STREET_ADDR.CTRY_ID as CTRY_ID, STREET_ADDR.CNTY_ID as CNTY_ID 

 FROM --EVIEWDB_EDW.STREET_ADDR 
 db_t_prod_core.STREET_ADDR

 qualify row_number () over (partition by ADDR_LN_1_TXT,ADDR_LN_2_TXT,ADDR_LN_3_TXT, CITY_ID ,TERR_ID,POSTL_CD_ID,CTRY_ID ,CNTY_ID order by EDW_END_DTTM desc)=1

)

LKP_STREET_ADDR

ON COALESCE(LKP_STREET_ADDR.ADDR_LN_1_TXT  , ''~'')   =  COALESCE(  SRC.addressline1internal , ''~'') 

AND COALESCE(LKP_STREET_ADDR.ADDR_LN_2_TXT, ''~'' ) =  COALESCE(SRC.addressline2internal  , ''~'')

AND COALESCE(LKP_STREET_ADDR.ADDR_LN_3_TXT  , ''~'')  =  COALESCE(SRC.addressline3internal , ''~'') 

AND COALESCE(LKP_STREET_ADDR.CITY_ID, ''~'') =  COALESCE(LKP_CITY.CITY_ID, ''~'')

AND COALESCE(LKP_STREET_ADDR.TERR_ID, ''~'') =  COALESCE(LKP_TERR.TERR_ID, ''~'')

AND COALESCE(LKP_STREET_ADDR.POSTL_CD_ID, ''~'') =  COALESCE(LKP_POSTL_CD.POSTL_CD_ID, ''~'')

AND COALESCE(LKP_STREET_ADDR.CTRY_ID, ''~'') =   COALESCE(LKP_CTRY.CTRY_ID, ''~'')

AND COALESCE(LKP_STREET_ADDR.CNTY_ID, ''~'') = COALESCE(LKP_CNTY.CNTY_ID, ''~'')



LEFT OUTER JOIN(

SELECT  AGMT.AGMT_ID AS AGMT_ID,AGMT.NK_SRC_KEY AS NK_SRC_KEY, AGMT.AGMT_TYPE_CD AS AGMT_TYPE_CD 

FROM    --EVIEWDB_EDW.AGMT 
db_t_prod_core.AGMT 

QUALIFY Row_Number() Over(

PARTITION BY AGMT.NK_SRC_KEY,AGMT.HOST_AGMT_NUM  

ORDER BY AGMT.EDW_END_DTTM DESC) = 1)

LKP_AGMT_PPV

ON LKP_AGMT_PPV.NK_SRC_KEY= SRC.publicID

AND LKP_AGMT_PPV.AGMT_TYPE_CD=''PPV''



LEFT OUTER JOIN(

    SELECT  FEAT.FEAT_ID AS FEAT_ID,

        FEAT.FEAT_SBTYPE_CD AS FEAT_SBTYPE_CD, FEAT.NK_SRC_KEY AS NK_SRC_KEY 

FROM    --EVIEWDB_EDW.FEAT 
db_t_prod_core.FEAT

QUALIFY Row_Number () Over (

PARTITION BY NK_SRC_KEY,FEAT_SBTYPE_CD  

ORDER BY edw_end_dttm DESC)=1) 

LKP_FEAT

ON LKP_FEAT.NK_SRC_KEY  = SRC.PATTERNCODE



/*LEFT OUTER JOIN(

    SELECT  DIR_PRTY.PRTY_ID as PRTY_ID,

       DIR_PRTY.nk_busn_val as nk_busn_val, DIR_PRTY.nk_publc_id as nk_publc_id, DIR_PRTY.nk_lnk_id as nk_lnk_id	

FROM    EVIEWDB_EDW.DIR_PRTY

) 

LKP_PRTY_ID

ON LKP_PRTY_ID.nk_busn_val = SRC.addressbookUID 

OR LKP_PRTY_ID.nk_publc_id = SRC.addressbookUID

OR LKP_PRTY_ID.nk_lnk_id = SRC.addressbookUID

*/

/*
LEFT OUTER JOIN (      

SELECT	dir_prty.PRTY_ID as BUSN_PRTY_ID,  --  BUSN.BUSN_CTGY_CD as BUSN_CTGY, 
						dir_prty.NK_BUSN_val as NK_BUSN_CD      

				FROM	--EVIEWDB_EDW.dir_prty 
        db_t_prod_core.dir_prty

				where NK_BUSN_val is not null) LKP_BUSN_PRTY      			``	

				ON UPPER(LKP_BUSN_PRTY.NK_BUSN_CD) =UPPER( SRC.addressbookUID )

				

LEFT OUTER JOIN ( SELECT dir_prty.PRTY_ID as INDIV_PRTY_ID, dir_prty.NK_LNK_ID as NK_LINK_ID

				FROM --EVIEWDB_EDW.dir_prty 
        db_t_prod_core.dir_prty
        where NK_PUBLC_ID is null and NK_BUSN_val is null) LKP_INDIV

				ON UPPER(LKP_INDIV.NK_LINK_ID) =UPPER( SRC.addressbookUID)

*/				



LEFT OUTER JOIN (      

                SELECT  BUSN.BUSN_PRTY_ID as BUSN_PRTY_ID, BUSN.BUSN_CTGY_CD as BUSN_CTGY,

                        BUSN.NK_BUSN_CD as NK_BUSN_CD      

                FROM    --EVIEWDB_EDW.BUSN 
                db_t_prod_core.BUSN BUSN      

                QUALIFY ROW_NUMBER () OVER (     

                PARTITION BY NK_BUSN_CD,BUSN_CTGY      

                ORDER BY EDW_END_DTTM DESC,EDW_STRT_DTTM DESC )=1 ) LKP_BUSN_PRTY                   

                ON UPPER(LKP_BUSN_PRTY.NK_BUSN_CD) =UPPER( SRC.addressbookUID )

LEFT OUTER JOIN ( SELECT INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID, INDIV.NK_LINK_ID as NK_LINK_ID,

                         INDIV.NK_PUBLC_ID as NK_PUBLC_ID

                FROM --EVIEWDB_EDW.INDIV 
                db_t_prod_core.INDIV INDIV   
                where NK_PUBLC_ID is null QUALIFY  ROW_NUMBER () OVER (

                PARTITION BY nk_link_id    

                ORDER BY EDW_END_DTTM DESC,EDW_STRT_DTTM DESC )=1) LKP_INDIV

                ON UPPER(LKP_INDIV.NK_LINK_ID) =UPPER( SRC.addressbookUID)

)SRC1



LEFT OUTER JOIN

(

SELECT distinct PLCY_LOCTR_PRTY_CVGE_MTRC.AGMT_ID AS AGMT_ID,

PLCY_LOCTR_PRTY_CVGE_MTRC.FEAT_ID AS FEAT_ID, 

PLCY_LOCTR_PRTY_CVGE_MTRC.LOC_ID AS LOC_ID,

PLCY_LOCTR_PRTY_CVGE_MTRC.PRTY_ID AS PRTY_ID,

PLCY_LOCTR_PRTY_CVGE_MTRC.LOC_NUM AS LOC_NUM,

PLCY_LOCTR_PRTY_CVGE_MTRC.INSRNC_MTRC_TYPE_CD AS INSRNC_MTRC_TYPE_CD,

PLCY_LOCTR_PRTY_CVGE_MTRC.PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM AS PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM,

PLCY_LOCTR_PRTY_CVGE_MTRC.PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM AS PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM,

PLCY_LOCTR_PRTY_CVGE_MTRC.PLCY_LOCTR_PRTY_CVGE_MTRC_AMT AS PLCY_LOCTR_PRTY_CVGE_MTRC_AMT,

PLCY_LOCTR_PRTY_CVGE_MTRC.EDW_STRT_DTTM AS EDW_STRT_DTTM,

PLCY_LOCTR_PRTY_CVGE_MTRC.EDW_END_DTTM AS EDW_END_DTTM

FROM --EVIEWDB_EDW.PLCY_LOCTR_PRTY_CVGE_MTRC 
db_t_prod_core.PLCY_LOCTR_PRTY_CVGE_MTRC 

WHERE INSRNC_MTRC_TYPE_CD IN(''TRANPREM'')

QUALIFY	ROW_NUMBER() OVER(PARTITION BY  AGMT_ID, FEAT_ID, LOC_ID, LOC_NUM, PRTY_ID ,INSRNC_MTRC_TYPE_CD  ORDER	BY EDW_END_DTTM desc) = 1

)LKP_TGT ON LKP_TGT.AGMT_ID=SRC1.SRC_AGMT_ID

AND LKP_TGT.FEAT_ID=SRC1.SRC_FEAT_ID

AND LKP_TGT.LOC_ID=SRC1.SRC_LOC_ID

AND LKP_TGT.PRTY_ID=SRC1.SRC_PRTY_ID

AND LKP_TGT.LOC_NUM=SRC1.SRC_LOC_NUM 

AND LKP_TGT.INSRNC_MTRC_TYPE_CD=SRC1.SRC_INSRNC_MTRC_TYPE_CD

WHERE SRC_AGMT_ID IS NOT NULL AND SRC_FEAT_ID IS NOT NULL AND SRC_LOC_ID IS NOT NULL AND SRC_PRTY_ID IS NOT NULL

AND (INS_UPD_FLAG=''I'' OR INS_UPD_FLAG=''U'')
) SRC
)
);


-- Component exp_src, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_src AS
(
SELECT
SQ_pc_policyperiod.AGMT_ID as AGMT_ID,
SQ_pc_policyperiod.FEAT_ID as FEAT_ID,
SQ_pc_policyperiod.LOC_ID as LOC_ID,
SQ_pc_policyperiod.PRTY_ID as PRTY_ID,
SQ_pc_policyperiod.LOC_NUM as LOC_NUM,
SQ_pc_policyperiod.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
SQ_pc_policyperiod.PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM as PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM,
SQ_pc_policyperiod.PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM as PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM,
SQ_pc_policyperiod.PLCY_LOCTR_PRTY_CVGE_MTRC_AMT as PLCY_LOCTR_PRTY_CVGE_MTRC_AMT,
SQ_pc_policyperiod.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
SQ_pc_policyperiod.TRANS_END_DTTM as TRANS_END_DTTM,
SQ_pc_policyperiod.ins_upd_flag as ins_upd_flag,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
:PRCS_ID as PRCS_ID,
SQ_pc_policyperiod.source_record_id
FROM
SQ_pc_policyperiod
);


-- Component rtr_plcy_loctr_prty_cvge_mtrc_INSERT, Type ROUTER Output Group INSERT
CREATE OR REPLACE TEMPORARY TABLE rtr_plcy_loctr_prty_cvge_mtrc_INSERT AS
SELECT
exp_src.AGMT_ID as AGMT_ID,
exp_src.FEAT_ID as FEAT_ID,
exp_src.LOC_ID as LOC_ID,
exp_src.PRTY_ID as PRTY_ID,
exp_src.LOC_NUM as LOC_NUM,
exp_src.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
exp_src.PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM as PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM,
exp_src.PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM as PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM,
exp_src.PLCY_LOCTR_PRTY_CVGE_MTRC_AMT as PLCY_LOCTR_PRTY_CVGE_MTRC_AMT,
exp_src.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_src.TRANS_END_DTTM as TRANS_END_DTTM,
exp_src.ins_upd_flag as ins_upd_flag,
exp_src.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_src.EDW_END_DTTM as EDW_END_DTTM,
exp_src.PRCS_ID as PRCS_ID,
exp_src.source_record_id
FROM
exp_src
WHERE ( exp_src.ins_upd_flag = ''I'' OR exp_src.ins_upd_flag = ''U'' ) and exp_src.AGMT_ID IS NOT NULL and exp_src.FEAT_ID IS NOT NULL and exp_src.LOC_ID IS NOT NULL and exp_src.PRTY_ID IS NOT NULL and exp_src.LOC_NUM IS NOT NULL;


-- Component upd_update_insert_tgt, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_update_insert_tgt AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_plcy_loctr_prty_cvge_mtrc_INSERT.AGMT_ID as AGMT_ID,
rtr_plcy_loctr_prty_cvge_mtrc_INSERT.FEAT_ID as FEAT_ID,
rtr_plcy_loctr_prty_cvge_mtrc_INSERT.LOC_ID as LOC_ID,
rtr_plcy_loctr_prty_cvge_mtrc_INSERT.PRTY_ID as PRTY_ID,
rtr_plcy_loctr_prty_cvge_mtrc_INSERT.LOC_NUM as LOC_NUM,
rtr_plcy_loctr_prty_cvge_mtrc_INSERT.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
rtr_plcy_loctr_prty_cvge_mtrc_INSERT.PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM as PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM,
rtr_plcy_loctr_prty_cvge_mtrc_INSERT.PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM as PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM,
rtr_plcy_loctr_prty_cvge_mtrc_INSERT.PLCY_LOCTR_PRTY_CVGE_MTRC_AMT as PLCY_LOCTR_PRTY_CVGE_MTRC_AMT,
rtr_plcy_loctr_prty_cvge_mtrc_INSERT.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
rtr_plcy_loctr_prty_cvge_mtrc_INSERT.TRANS_END_DTTM as TRANS_END_DTTM,
rtr_plcy_loctr_prty_cvge_mtrc_INSERT.EDW_STRT_DTTM as EDW_STRT_DTTM,
rtr_plcy_loctr_prty_cvge_mtrc_INSERT.EDW_END_DTTM as EDW_END_DTTM,
rtr_plcy_loctr_prty_cvge_mtrc_INSERT.PRCS_ID as PRCS_ID,
0 as UPDATE_STRATEGY_ACTION,
rtr_plcy_loctr_prty_cvge_mtrc_INSERT.source_record_id as source_record_id
FROM
rtr_plcy_loctr_prty_cvge_mtrc_INSERT
);


-- Component exp_ins_upd_tgt, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_ins_upd_tgt AS
(
SELECT
upd_update_insert_tgt.AGMT_ID as AGMT_ID,
upd_update_insert_tgt.FEAT_ID as FEAT_ID,
upd_update_insert_tgt.LOC_ID as LOC_ID,
upd_update_insert_tgt.PRTY_ID as PRTY_ID,
upd_update_insert_tgt.LOC_NUM as LOC_NUM,
upd_update_insert_tgt.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
upd_update_insert_tgt.PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM as PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM,
upd_update_insert_tgt.PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM as PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM,
upd_update_insert_tgt.PLCY_LOCTR_PRTY_CVGE_MTRC_AMT as PLCY_LOCTR_PRTY_CVGE_MTRC_AMT,
upd_update_insert_tgt.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
upd_update_insert_tgt.TRANS_END_DTTM as TRANS_END_DTTM,
upd_update_insert_tgt.EDW_STRT_DTTM as EDW_STRT_DTTM,
upd_update_insert_tgt.EDW_END_DTTM as EDW_END_DTTM,
upd_update_insert_tgt.PRCS_ID as PRCS_ID,
upd_update_insert_tgt.source_record_id
FROM
upd_update_insert_tgt
);


-- Component PLCY_LOCTR_PRTY_CVGE_MTRC_upd_ins, Type TARGET 
INSERT INTO db_t_prod_core.PLCY_LOCTR_PRTY_CVGE_MTRC
(
AGMT_ID,
FEAT_ID,
LOC_ID,
LOC_NUM,
PRTY_ID,
INSRNC_MTRC_TYPE_CD,
PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM,
PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM,
PLCY_LOCTR_PRTY_CVGE_MTRC_AMT,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_ins_upd_tgt.AGMT_ID as AGMT_ID,
exp_ins_upd_tgt.FEAT_ID as FEAT_ID,
exp_ins_upd_tgt.LOC_ID as LOC_ID,
exp_ins_upd_tgt.LOC_NUM as LOC_NUM,
exp_ins_upd_tgt.PRTY_ID as PRTY_ID,
exp_ins_upd_tgt.INSRNC_MTRC_TYPE_CD as INSRNC_MTRC_TYPE_CD,
exp_ins_upd_tgt.PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM as PLCY_LOCTR_PRTY_CVGE_MTRC_STRT_DTTM,
exp_ins_upd_tgt.PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM as PLCY_LOCTR_PRTY_CVGE_MTRC_END_DTTM,
exp_ins_upd_tgt.PLCY_LOCTR_PRTY_CVGE_MTRC_AMT as PLCY_LOCTR_PRTY_CVGE_MTRC_AMT,
exp_ins_upd_tgt.PRCS_ID as PRCS_ID,
exp_ins_upd_tgt.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_ins_upd_tgt.EDW_END_DTTM as EDW_END_DTTM,
exp_ins_upd_tgt.TRANS_STRT_DTTM as TRANS_STRT_DTTM,
exp_ins_upd_tgt.TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_ins_upd_tgt;


-- Component PLCY_LOCTR_PRTY_CVGE_MTRC_upd_ins, Type Post SQL 
UPDATE db_t_prod_core.PLCY_LOCTR_PRTY_CVGE_MTRC
   set EDW_END_DTTM=A.lead1,
  TRANS_END_DTTM=A.lead2
FROM

            (SELECT   distinct  AGMT_ID,FEAT_ID,LOC_ID,LOC_NUM,PRTY_ID,INSRNC_MTRC_TYPE_CD,EDW_STRT_DTTM,TRANS_STRT_DTTM,

                        max(EDW_STRT_DTTM) over (partition by AGMT_ID,FEAT_ID,LOC_ID,LOC_NUM,PRTY_ID,INSRNC_MTRC_TYPE_CD

                        ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''

                        as lead1

                       ,max(TRANS_STRT_DTTM) over (partition by AGMT_ID,FEAT_ID,LOC_ID,LOC_NUM,PRTY_ID,INSRNC_MTRC_TYPE_CD

                       ORDER BY TRANS_STRT_DTTM  ASC rows between 1 following and 1 following)  - INTERVAL ''1 SECOND''

                        as lead2

                       FROM             db_t_prod_core.PLCY_LOCTR_PRTY_CVGE_MTRC WHERE INSRNC_MTRC_TYPE_CD IN(''PREM'',''TRANPREM'')

                      group by   AGMT_ID,FEAT_ID,LOC_ID,LOC_NUM,PRTY_ID,INSRNC_MTRC_TYPE_CD,EDW_STRT_DTTM,TRANS_STRT_DTTM

                          ) A


  where  PLCY_LOCTR_PRTY_CVGE_MTRC.EDW_STRT_DTTM = A.EDW_STRT_DTTM

                         and PLCY_LOCTR_PRTY_CVGE_MTRC.AGMT_ID=A.AGMT_ID

                          and PLCY_LOCTR_PRTY_CVGE_MTRC.FEAT_ID=A.FEAT_ID

                          and PLCY_LOCTR_PRTY_CVGE_MTRC.LOC_ID=A.LOC_ID 

						  and PLCY_LOCTR_PRTY_CVGE_MTRC.LOC_NUM=A.LOC_NUM 

						  and PLCY_LOCTR_PRTY_CVGE_MTRC.PRTY_ID=A.PRTY_ID

						  and PLCY_LOCTR_PRTY_CVGE_MTRC.INSRNC_MTRC_TYPE_CD=A.INSRNC_MTRC_TYPE_CD

                          and CAST(PLCY_LOCTR_PRTY_CVGE_MTRC.EDW_END_DTTM AS DATE)=''9999-12-31''

						  and PLCY_LOCTR_PRTY_CVGE_MTRC.INSRNC_MTRC_TYPE_CD IN(''PREM'',''TRANPREM'')

                          and lead1 IS NOT NULL

                          and lead2 IS NOT NULL;


END; ';