-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_ASSET_DTL_CODE_RAPA_XREF_INSUPD("WORKLET_NAME" VARCHAR)
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
 

-- PIPELINE START FOR 1

-- Component SQ_pcx_rapa_alfa, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pcx_rapa_alfa AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as lkp_AGMT_ID,
$2 as lkp_PRTY_ASSET_ID,
$3 as lkp_ASSET_DTL_CD,
$4 as lkp_AGMT_ASSET_DTL_XREF_STRT_DTTM,
$5 as lkp_AGMT_ASSET_DTL_TXT,
$6 as lkp_RAPA_VRSN,
$7 as Src_AGMT_ID,
$8 as Src_PRTY_ASSET_ID,
$9 as in_asset_dtl_cd,
$10 as out_XREF_STRT_DTTM,
$11 as asset_dtl_txt,
$12 as out_rapa_UpdateTime,
$13 as src_rapaversion,
$14 as out_INS_UPD_flag,
$15 as TARGET_DATA,
$16 as SOURCE_DATA,
$17 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
with temp as (select * from(

Select distinct PublicID_stg AS PublicID_in, FixedID_stg as FixedID_in,lkp_teradata_etl_ref_xlat_asset_sbtype.TGT_IDNTFTN_VAL as prty_asset_sbtype_cd_in,

EditEffectiveDate as EditEffectiveDate_in,

rapa_UpdateTime as rapa_UpdateTime_in,AD_TYPE1,

AD_TYPE2, AD_TYPE3, AD_TYPE4, AD_TYPE5, AD_TYPE6, AD_TYPE7, AD_TYPE8, AD_TYPE9,AD_TYPE10,

AD_TYPE11,AD_TYPE12, AD_TYPE14, AD_TYPE15,AD_TYPE16,AD_TYPE17,SLSym_stg,SLSymRel_stg,BISym_stg,BISymRel_stg,PDSym_stg,PDSymRel_stg,MPSym_stg,MPSymRel_stg,

CollSym_stg,CollSymRel_stg,CompSym_stg,CompSymRel_stg,UMPDSym_stg,UMBISym_stg,CollRatesym,CompRatesym,Rapaversion/* EIM-48448 */
from (

SELECT distinct 

pc_policyperiod.PublicID_stg,

cast(pc_personalvehicle.FixedID_stg as varchar(60)) as FixedID_stg,

cast(''PRTY_ASSET_SBTYPE4'' as varchar(60)) AS prty_asset_sbtype_cd,

Cast(pc_policyperiod.EditEffectiveDate_stg AS VARCHAR(60) ) AS EditEffectiveDate,

Cast(pcx_rapa_alfa.UpdateTime_stg AS VARCHAR(60)) AS rapa_UpdateTime,

''ASSET_DTL_TYPE1'' AS AD_TYPE1,

''ASSET_DTL_TYPE2'' AS AD_TYPE2,

''ASSET_DTL_TYPE3'' AS AD_TYPE3,

''ASSET_DTL_TYPE4'' AS AD_TYPE4,

''ASSET_DTL_TYPE5'' AS AD_TYPE5,

''ASSET_DTL_TYPE6'' AS AD_TYPE6,

''ASSET_DTL_TYPE7'' AS AD_TYPE7,

''ASSET_DTL_TYPE8'' AS AD_TYPE8,

''ASSET_DTL_TYPE9'' AS AD_TYPE9,

''ASSET_DTL_TYPE10'' AS AD_TYPE10,

''ASSET_DTL_TYPE11'' AS AD_TYPE11,

''ASSET_DTL_TYPE12'' AS AD_TYPE12,

''ASSET_DTL_TYPE14'' AS AD_TYPE14,

''ASSET_DTL_TYPE15'' AS AD_TYPE15,

cast(null as varchar(30)) as AD_TYPE16,cast(null as varchar(30)) as AD_TYPE17,

pcx_rapa_alfa.SLSym_stg as SLSym_stg,pcx_rapa_alfa.SLSymRel_stg as SLSymRel_stg,pcx_rapa_alfa.BISym_stg as BISym_stg,pcx_rapa_alfa.BISymRel_stg as BISymRel_stg,

pcx_rapa_alfa.PDSym_stg as PDSym_stg,pcx_rapa_alfa.PDSymRel_stg as PDSymRel_stg,pcx_rapa_alfa.MPSym_stg as MPSym_stg,pcx_rapa_alfa.MPSymRel_stg as MPSymRel_stg,

pcx_rapa_alfa.CollSym_stg as CollSym_stg,pcx_rapa_alfa.CollSymRel_stg as CollSymRel_stg,pcx_rapa_alfa.CompSym_stg as CompSym_stg,pcx_rapa_alfa.CompSymRel_stg as CompSymRel_stg,

pcx_rapa_alfa.UMPDSym_stg as UMPDSym_stg,pcx_rapa_alfa.UMBISym_stg as UMBISym_stg,cast(null as varchar(3)) AS CollRatesym,cast(null as varchar(3)) AS CompRatesym

,cast(pc_personalvehicle.RAPAVersion_alfa_stg as varchar(8)) as RAPAVersion /* EIM-48448 */
FROM DB_T_PROD_STAG.pc_policyperiod 

JOIN DB_T_PROD_STAG.pc_personalvehicle  ON pc_personalvehicle.BranchID_stg = pc_policyperiod.ID_stg

JOIN DB_T_PROD_STAG.pcx_rapa_alfa ON pcx_rapa_alfa.ID_stg = pc_personalvehicle.RAPA_alfa_stg 

JOIN DB_T_PROD_STAG.pctl_policyperiodstatus ON pctl_policyperiodstatus.ID_stg = pc_policyperiod.Status_stg

WHERE  (pctl_policyperiodstatus.TYPECODE_stg = ''Bound'') 

AND (pc_personalvehicle.ExpirationDate_stg IS NULL  OR pc_personalvehicle.ExpirationDate_stg > pc_policyperiod.EditEffectiveDate_stg )

and ((pcx_rapa_alfa.UpdateTime_stg > (:start_dttm) and pcx_rapa_alfa.UpdateTime_stg <= (:end_dttm))

    or (pc_policyperiod.UpdateTime_stg > (:start_dttm) and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

    or (pc_personalvehicle.UpdateTime_stg > (:start_dttm) and pc_personalvehicle.UpdateTime_stg <= (:end_dttm))

)QUALIFY Row_Number () Over ( PARTITION BY pc_policyperiod.PublicID_stg,pc_personalvehicle.FixedID_stg ORDER BY pc_policyperiod.UpdateTime_stg DESC,pc_personalvehicle.ExpirationDate_stg ASC)=1

UNION

SELECT distinct 

pc_policyperiod.PublicID_stg,

cast(pc_personalvehicle.FixedID_stg as varchar(50)) as FixedID_stg,

''PRTY_ASSET_SBTYPE4'' AS prty_asset_sbtype_cd,

Cast(pc_policyperiod.EditEffectiveDate_stg AS VARCHAR(60) ) AS EditEffectiveDate ,

Cast(pc_personalvehicle.Updatetime_stg AS VARCHAR(60)) AS UpdateTime,

cast(null as varchar(30)) AS AD_TYPE1,

cast(null as varchar(30)) AS AD_TYPE2,

cast(null as varchar(30)) AS AD_TYPE3,

cast(null as varchar(30)) AS AD_TYPE4,

cast(null as varchar(30)) AS AD_TYPE5,

cast(null as varchar(30)) AS AD_TYPE6,

cast(null as varchar(30)) AS AD_TYPE7,

cast(null as varchar(30)) AS AD_TYPE8,

cast(null as varchar(30)) AS AD_TYPE9,

cast(null as varchar(30)) AS AD_TYPE10,

cast(null as varchar(30)) AS AD_TYPE11,

cast(null as varchar(30)) AS AD_TYPE12,

cast(null as varchar(30)) AS AD_TYPE14,

cast(null as varchar(30)) AS AD_TYPE15,

cast(''ASSET_DTL_TYPE16'' as varchar(30)) AS AD_TYPE16,

cast(''ASSET_DTL_TYPE17'' as varchar(30)) AS AD_TYPE17,

cast(null as varchar(10)) AS SLSym_stg,

cast(null as varchar(10)) AS SLSymRel_stg,

cast(null as varchar(10)) AS BISym_stg,

cast(null as varchar(10)) AS BISymRel_stg,

cast(null as varchar(10)) AS PDSym_stg,

cast(null as varchar(10)) AS PDSymRel_stg,

cast(null as varchar(10)) AS MPSym_stg,

cast(null as varchar(10)) AS MPSymRel_stg,

cast(null as varchar(10)) AS CollSym_stg,

cast(null as varchar(10)) AS CollSymRel_stg,

cast(null as varchar(10)) AS CompSym_stg,

cast(null as varchar(10)) AS CompSymRel_stg,

cast(null as varchar(10)) AS UMPDSym_stg,

cast(null as varchar(10)) AS UMBISym_stg,

cast(pc_personalvehicle.RateSymbolCollision_alfa_stg as varchar(3)) as CollRatesym,

cast(pc_personalvehicle.RateSymbol_alfa_stg as varchar(3)) as CompRatesym

,cast(pc_personalvehicle.RAPAVersion_alfa_stg as varchar(8)) as RAPAVersion/* EIM-48448 */
FROM DB_T_PROD_STAG.pc_policyperiod 

JOIN DB_T_PROD_STAG.pc_personalvehicle  ON pc_personalvehicle.BranchID_stg = pc_policyperiod.ID_stg

AND (pc_personalvehicle.ExpirationDate_stg IS NULL  OR pc_personalvehicle.ExpirationDate_stg > case when 

pc_policyperiod.EditEffectiveDate_stg >= pc_policyperiod.ModelDate_stg then pc_policyperiod.EditEffectiveDate_stg else pc_policyperiod.ModelDate_stg end)

and ((pc_policyperiod.UpdateTime_stg > (:start_dttm) and pc_policyperiod.UpdateTime_stg <= (:end_dttm))

    or (pc_personalvehicle.UpdateTime_stg > (:start_dttm) and pc_personalvehicle.UpdateTime_stg <= (:end_dttm)))

      

QUALIFY Row_Number () Over ( PARTITION BY pc_policyperiod.PublicID_stg,pc_personalvehicle.FixedID_stg 

ORDER BY pc_personalvehicle.UpdateTime_stg DESC,pc_personalvehicle.ExpirationDate_stg ASC)=1

)aa

left join(

SELECT 

    TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

    ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

    DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

    TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

        AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

        AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'')lkp_teradata_etl_ref_xlat_asset_sbtype

        on lkp_teradata_etl_ref_xlat_asset_sbtype.SRC_IDNTFTN_VAL=aa.prty_asset_sbtype_cd

)Src

left join (

SELECT DIR_AGMT.AGMT_ID AS AGMT_ID,

       DIR_AGMT.NK_SRC_KEY AS NK_SRC_KEY,

       DIR_AGMT.AGMT_TYPE_CD AS AGMT_TYPE_CD

FROM DB_T_PROD_CORE.DIR_AGMT

WHERE AGMT_TYPE_CD = ''PPV'')lkp_dir_agmt

on Src.PublicID_in=lkp_dir_agmt.NK_SRC_KEY

and lkp_dir_agmt.AGMT_TYPE_CD=''PPV''



left join(

SELECT DIR_PRTY_ASSET.PRTY_ASSET_ID AS PRTY_ASSET_ID,

      DIR_PRTY_ASSET.ASSET_HOST_ID_VAL  AS ASSET_HOST_ID_VAL,

       DIR_PRTY_ASSET.PRTY_ASSET_SBTYPE_CD AS PRTY_ASSET_SBTYPE_CD

FROM DB_T_PROD_CORE.DIR_PRTY_ASSET

WHERE  PRTY_ASSET_SBTYPE_CD = ''MVEH''and prty_asset_clasfcn_cd=''MV'')lkp_dir_prty_asset

on Src.prty_asset_sbtype_cd_in=lkp_dir_prty_asset.PRTY_ASSET_SBTYPE_CD 

and Src.FixedID_in = lkp_dir_prty_asset.ASSET_HOST_ID_VAL

),

normalizer as (

Select agmt_id as agmt_id,prty_asset_id as prty_asset_id,  PublicID_in as PublicID, FixedID_in as FixedID,prty_asset_sbtype_cd_in as prty_asset_sbtype_cd, EditEffectiveDate_in as EditEffectiveDate,

rapa_UpdateTime_in as rapa_UpdateTime, cast(''ASSET_DTL_TYPE1'' as varchar(100)) as GCID_asset_dtl_cd, CAST(SLSym_stg AS VARCHAR(100)) as GCID_asset_dtl_txt,Rapaversion from temp where AD_TYPE15 IS NOT NULL

UNION ALL

Select agmt_id as agmt_id,prty_asset_id as prty_asset_id,  PublicID_in as PublicID, FixedID_in as FixedID,prty_asset_sbtype_cd_in as prty_asset_sbtype_cd, EditEffectiveDate_in as EditEffectiveDate,

rapa_UpdateTime_in as rapa_UpdateTime, ''ASSET_DTL_TYPE2'' as GCID_asset_dtl_cd, SLSymRel_stg as GCID_asset_dtl_txt,Rapaversion from temp where AD_TYPE15 IS NOT NULL

UNION ALL

Select agmt_id as agmt_id,prty_asset_id as prty_asset_id,  PublicID_in as PublicID, FixedID_in as FixedID,prty_asset_sbtype_cd_in as prty_asset_sbtype_cd, EditEffectiveDate_in as EditEffectiveDate,

rapa_UpdateTime_in as rapa_UpdateTime,''ASSET_DTL_TYPE3'' as GCID_asset_dtl_cd, BISym_stg  as GCID_asset_dtl_txt,Rapaversion from temp where AD_TYPE15 IS NOT NULL

UNION ALL

Select agmt_id as agmt_id,prty_asset_id as prty_asset_id,  PublicID_in as PublicID, FixedID_in as FixedID,prty_asset_sbtype_cd_in as prty_asset_sbtype_cd, EditEffectiveDate_in as EditEffectiveDate,

rapa_UpdateTime_in as rapa_UpdateTime, ''ASSET_DTL_TYPE4'' as GCID_asset_dtl_cd, BISymRel_stg  as GCID_asset_dtl_txt,Rapaversion from temp where AD_TYPE15 IS NOT NULL

UNION ALL

Select agmt_id as agmt_id,prty_asset_id as prty_asset_id,  PublicID_in as PublicID, FixedID_in as FixedID,prty_asset_sbtype_cd_in as prty_asset_sbtype_cd, EditEffectiveDate_in as EditEffectiveDate,

rapa_UpdateTime_in as rapa_UpdateTime, ''ASSET_DTL_TYPE5'' as GCID_asset_dtl_cd, PDSym_stg as GCID_asset_dtl_txt,Rapaversion from temp where AD_TYPE15 IS NOT NULL

  UNION ALL

Select agmt_id as agmt_id,prty_asset_id as prty_asset_id,  PublicID_in as PublicID, FixedID_in as FixedID,prty_asset_sbtype_cd_in as prty_asset_sbtype_cd, EditEffectiveDate_in as EditEffectiveDate,

rapa_UpdateTime_in as rapa_UpdateTime, ''ASSET_DTL_TYPE6'' as GCID_asset_dtl_cd, PDSymRel_stg as GCID_asset_dtl_txt,Rapaversion from temp where AD_TYPE15 IS NOT NULL

  UNION ALL

Select agmt_id as agmt_id,prty_asset_id as prty_asset_id,  PublicID_in as PublicID, FixedID_in as FixedID,prty_asset_sbtype_cd_in as prty_asset_sbtype_cd, EditEffectiveDate_in as EditEffectiveDate,

rapa_UpdateTime_in as rapa_UpdateTime, ''ASSET_DTL_TYPE7'' as GCID_asset_dtl_cd, MPSym_stg as GCID_asset_dtl_txt,Rapaversion from temp where AD_TYPE15 IS NOT NULL

  UNION ALL

Select agmt_id as agmt_id,prty_asset_id as prty_asset_id,  PublicID_in as PublicID, FixedID_in as FixedID,prty_asset_sbtype_cd_in as prty_asset_sbtype_cd, EditEffectiveDate_in as EditEffectiveDate,

rapa_UpdateTime_in as rapa_UpdateTime,''ASSET_DTL_TYPE8'' as GCID_asset_dtl_cd, MPSymRel_stg as GCID_asset_dtl_txt,Rapaversion from temp where AD_TYPE15 IS NOT NULL

  UNION ALL

Select agmt_id as agmt_id,prty_asset_id as prty_asset_id,  PublicID_in as PublicID, FixedID_in as FixedID,prty_asset_sbtype_cd_in as prty_asset_sbtype_cd, EditEffectiveDate_in as EditEffectiveDate,

rapa_UpdateTime_in as rapa_UpdateTime,''ASSET_DTL_TYPE9'' as GCID_asset_dtl_cd, CollSym_stg as GCID_asset_dtl_txt,Rapaversion from temp where AD_TYPE15 IS NOT NULL

  UNION ALL

Select agmt_id as agmt_id,prty_asset_id as prty_asset_id,  PublicID_in as PublicID, FixedID_in as FixedID,prty_asset_sbtype_cd_in as prty_asset_sbtype_cd, EditEffectiveDate_in as EditEffectiveDate,

rapa_UpdateTime_in as rapa_UpdateTime, ''ASSET_DTL_TYPE10'' as GCID_asset_dtl_cd, CollSymRel_stg as GCID_asset_dtl_txt,Rapaversion from temp where AD_TYPE15 IS NOT NULL

   UNION ALL

Select agmt_id as agmt_id,prty_asset_id as prty_asset_id,  PublicID_in as PublicID, FixedID_in as FixedID,prty_asset_sbtype_cd_in as prty_asset_sbtype_cd, EditEffectiveDate_in as EditEffectiveDate,

rapa_UpdateTime_in as rapa_UpdateTime, ''ASSET_DTL_TYPE11'' as GCID_asset_dtl_cd, CompSym_stg as GCID_asset_dtl_txt,Rapaversion from temp where AD_TYPE15 IS NOT NULL

   UNION ALL

Select agmt_id as agmt_id,prty_asset_id as prty_asset_id,  PublicID_in as PublicID, FixedID_in as FixedID,prty_asset_sbtype_cd_in as prty_asset_sbtype_cd, EditEffectiveDate_in as EditEffectiveDate,

rapa_UpdateTime_in as rapa_UpdateTime, ''ASSET_DTL_TYPE12'' as GCID_asset_dtl_cd, CompSymRel_stg as GCID_asset_dtl_txt,Rapaversion from temp where AD_TYPE15 IS NOT NULL

   UNION ALL

Select agmt_id as agmt_id,prty_asset_id as prty_asset_id,  PublicID_in as PublicID, FixedID_in as FixedID,prty_asset_sbtype_cd_in as prty_asset_sbtype_cd, EditEffectiveDate_in as EditEffectiveDate,

rapa_UpdateTime_in as rapa_UpdateTime, ''ASSET_DTL_TYPE14'' as GCID_asset_dtl_cd, UMPDSym_stg as GCID_asset_dtl_txt,Rapaversion from temp where AD_TYPE15 IS NOT NULL

   UNION ALL

Select agmt_id as agmt_id,prty_asset_id as prty_asset_id,  PublicID_in as PublicID, FixedID_in as FixedID,prty_asset_sbtype_cd_in as prty_asset_sbtype_cd, EditEffectiveDate_in as EditEffectiveDate,

rapa_UpdateTime_in as rapa_UpdateTime, ''ASSET_DTL_TYPE15'' as GCID_asset_dtl_cd, UMBISym_stg as GCID_asset_dtl_txt,Rapaversion from temp where AD_TYPE15 IS NOT NULL

   UNION ALL

Select agmt_id as agmt_id,prty_asset_id as prty_asset_id,  PublicID_in as PublicID, FixedID_in as FixedID,prty_asset_sbtype_cd_in as prty_asset_sbtype_cd, EditEffectiveDate_in as EditEffectiveDate,

rapa_UpdateTime_in as rapa_UpdateTime, ''ASSET_DTL_TYPE16'' as GCID_asset_dtl_cd, CollRatesym as GCID_asset_dtl_txt,Rapaversion from temp where AD_TYPE17 IS NOT NULL

   UNION ALL

Select agmt_id as agmt_id,prty_asset_id as prty_asset_id,  PublicID_in as PublicID, FixedID_in as FixedID,prty_asset_sbtype_cd_in as prty_asset_sbtype_cd, EditEffectiveDate_in as EditEffectiveDate,

rapa_UpdateTime_in as rapa_UpdateTime, ''ASSET_DTL_TYPE17'' as GCID_asset_dtl_cd, CompRatesym as GCID_asset_dtl_txt,Rapaversion from temp where AD_TYPE17 IS NOT NULL

)

/* - Main Query starts here----- */


SELECT * FROM (

Select

lkp_AGMT_ASSET_DTL_CD_XREF.AGMT_ID as lkp_AGMT_ID,

lkp_AGMT_ASSET_DTL_CD_XREF.PRTY_ASSET_ID as lkp_PRTY_ASSET_ID,

lkp_AGMT_ASSET_DTL_CD_XREF.ASSET_DTL_CD as lkp_ASSET_DTL_CD,

lkp_AGMT_ASSET_DTL_CD_XREF.AGMT_ASSET_DTL_XREF_STRT_DTTM as lkp_AGMT_ASSET_DTL_XREF_STRT_DTTM, 

lkp_AGMT_ASSET_DTL_CD_XREF.AGMT_ASSET_DTL_TXT as lkp_AGMT_ASSET_DTL_TXT,

lkp_AGMT_ASSET_DTL_CD_XREF.RAPA_VRSN as lkp_RAPA_VRSN,

Src_AGMT_ID as Src_AGMT_ID,

cast(Src_PRTY_ASSET_ID as INTEGER)as Src_PRTY_ASSET_ID,

lkp_teradata_etl_ref_xlat_asset_dtl_cd.TGT_IDNTFTN_VAL as in_asset_dtl_cd,

out_XREF_STRT_DTTM as out_XREF_STRT_DTTM,

cc.asset_dtl_txt as asset_dtl_txt,

rapa_UpdateTime as out_rapa_UpdateTime,

src_Rapaversion,/* EIM-48448 */

CAST(TRIM(CAST(coalesce(lkp_AGMT_ASSET_DTL_TXT,''~'') AS VARCHAR(100)))||TRIM(CAST(lkp_AGMT_ASSET_DTL_XREF_STRT_DTTM AS VARCHAR(100)))|| TRIM(CAST(coalesce(lkp_RAPA_VRSN,''~'') AS VARCHAR(100))) AS VARCHAR(1000)) AS TARGET_DATA,

CAST(TRIM(CAST(coalesce(asset_dtl_txt,''~'') AS VARCHAR(100)))||TRIM(CAST(out_XREF_STRT_DTTM AS VARCHAR(100)))|| TRIM(CAST(coalesce(src_Rapaversion,''~'') AS VARCHAR(100)))AS VARCHAR(1000)) AS SOURCE_DATA,
CASE
WHEN TARGET_DATA IS  NULL  THEN ''I'' 
WHEN SOURCE_DATA <> TARGET_DATA THEN ''U'' 
WHEN SOURCE_DATA = TARGET_DATA THEN ''R'' END AS out_INS_UPD_flag

from ( select Src_AGMT_ID,

Src_PRTY_ASSET_ID,

out_XREF_STRT_DTTM,

rapa_UpdateTime, 

GCID_asset_dtl_cd as asset_dtl_cd,

GCID_asset_dtl_txt as asset_dtl_txt,

Rapaversion as src_Rapaversion

from 

(select AGMT_ID as Src_AGMT_ID,

PRTY_ASSET_ID as Src_PRTY_ASSET_ID,

EditEffectiveDate as out_XREF_STRT_DTTM,rapa_UpdateTime,GCID_asset_dtl_cd,GCID_asset_dtl_txt,Rapaversion

from normalizer

) MSQ1 )cc

  left join (

SELECT 

        TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL,

        TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL

FROM 

    DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

    TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''ASSET_DTL_TYPE'' 

    AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' 

    AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

)lkp_teradata_etl_ref_xlat_asset_dtl_cd on lkp_teradata_etl_ref_xlat_asset_dtl_cd.SRC_IDNTFTN_VAL=cc.asset_dtl_cd

left join(

SELECT  AGMT_ASSET_DTL_CD_XREF.AGMT_ASSET_DTL_XREF_STRT_DTTM as AGMT_ASSET_DTL_XREF_STRT_DTTM,

        AGMT_ASSET_DTL_CD_XREF.AGMT_ASSET_DTL_TXT as AGMT_ASSET_DTL_TXT,

        AGMT_ASSET_DTL_CD_XREF.EDW_STRT_DTTM as EDW_STRT_DTTM, AGMT_ASSET_DTL_CD_XREF.EDW_END_DTTM as EDW_END_DTTM,

        AGMT_ASSET_DTL_CD_XREF.TRANS_STRT_DTTM as TRANS_STRT_DTTM, AGMT_ASSET_DTL_CD_XREF.TRANS_END_DTTM as TRANS_END_DTTM,

        AGMT_ASSET_DTL_CD_XREF.AGMT_ID as AGMT_ID, AGMT_ASSET_DTL_CD_XREF.PRTY_ASSET_ID as PRTY_ASSET_ID,

        AGMT_ASSET_DTL_CD_XREF.ASSET_DTL_CD as ASSET_DTL_CD,AGMT_ASSET_DTL_CD_XREF.RAPA_VRSN as RAPA_VRSN

FROM    

DB_T_PROD_CORE.AGMT_ASSET_DTL_CD_XREF  

JOIN DB_T_PROD_CORE.ASSET_DTL_TYPE ADTL ON ADTL.ASSET_DTL_CD=AGMT_ASSET_DTL_CD_XREF.ASSET_DTL_CD

WHERE ASSET_DTL_SCHM_TYPE_CD in (''RAPA'',''VEH'')

QUALIFY ROW_NUMBER( ) OVER (PARTITION BY AGMT_ASSET_DTL_CD_XREF.PRTY_ASSET_ID,AGMT_ASSET_DTL_CD_XREF.AGMT_ID,AGMT_ASSET_DTL_CD_XREF.ASSET_DTL_CD  

ORDER BY AGMT_ASSET_DTL_CD_XREF.EDW_END_DTTM DESC)=1 )lkp_AGMT_ASSET_DTL_CD_XREF

on lkp_AGMT_ASSET_DTL_CD_XREF.AGMT_ID=src_AGMT_ID

and lkp_AGMT_ASSET_DTL_CD_XREF.PRTY_ASSET_ID=src_PRTY_ASSET_ID

and lkp_AGMT_ASSET_DTL_CD_XREF.ASSET_DTL_CD= in_asset_dtl_cd)qq 

where src_agmt_id is not null and src_prty_asset_id is not null and out_INS_UPD_flag  in (''I'',''U'')
) SRC
)
);


-- Component exp_compare_data, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_compare_data AS
(
SELECT
SQ_pcx_rapa_alfa.Src_AGMT_ID as in_AGMT_ID,
SQ_pcx_rapa_alfa.Src_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
SQ_pcx_rapa_alfa.in_asset_dtl_cd as in_asset_dtl_cd,
SQ_pcx_rapa_alfa.out_XREF_STRT_DTTM as out_XREF_STRT_DTTM,
SQ_pcx_rapa_alfa.asset_dtl_txt as asset_dtl_txt,
SQ_pcx_rapa_alfa.src_rapaversion as in_RAPA_VERSION,
NULL as out_blank,
:PRCS_ID as out_prcs_id,
CURRENT_TIMESTAMP as out_EDW_START_DTTM,
to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as out_EDW_END_DTTM,
SQ_pcx_rapa_alfa.out_rapa_UpdateTime as rapa_UpdateTime,
to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) as out_TRANS_END_DTTM,
MD5 ( SQ_pcx_rapa_alfa.lkp_AGMT_ASSET_DTL_TXT || TO_CHAR ( ltrim ( rtrim ( SQ_pcx_rapa_alfa.lkp_AGMT_ASSET_DTL_XREF_STRT_DTTM ) ) ) ) as v_lkp_MD5,
MD5 ( SQ_pcx_rapa_alfa.asset_dtl_txt || TO_CHAR ( ltrim ( rtrim ( SQ_pcx_rapa_alfa.out_XREF_STRT_DTTM ) ) ) ) as v_in_MD5,
SQ_pcx_rapa_alfa.out_INS_UPD_flag as out_INS_UPD_flag,
SQ_pcx_rapa_alfa.source_record_id
FROM
SQ_pcx_rapa_alfa
);


-- Component rtr_agmt_asset_dtl_cd_xref_INSERT, Type ROUTER Output Group INSERT
create or replace temporary table rtr_agmt_asset_dtl_cd_xref_INSERT as
SELECT
exp_compare_data.in_AGMT_ID as in_AGMT_ID,
exp_compare_data.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID,
exp_compare_data.in_asset_dtl_cd as in_asset_dtl_cd,
exp_compare_data.out_XREF_STRT_DTTM as out_XREF_STRT_DTTM,
exp_compare_data.asset_dtl_txt as asset_dtl_txt,
exp_compare_data.in_RAPA_VERSION as in_RAPA_VERSION,
exp_compare_data.out_blank as out_blank,
exp_compare_data.out_prcs_id as out_prcs_id,
exp_compare_data.out_EDW_START_DTTM as out_EDW_START_DTTM,
exp_compare_data.out_EDW_END_DTTM as out_EDW_END_DTTM,
exp_compare_data.rapa_UpdateTime as out_rapa_UpdateTime,
exp_compare_data.out_TRANS_END_DTTM as out_TRANS_END_DTTM,
exp_compare_data.out_INS_UPD_flag as out_INS_UPD_flag,
exp_compare_data.source_record_id
FROM
exp_compare_data
WHERE ( exp_compare_data.out_INS_UPD_flag = ''I'' or exp_compare_data.out_INS_UPD_flag = ''U'' ) AND exp_compare_data.in_AGMT_ID IS NOT NULL AND exp_compare_data.in_PRTY_ASSET_ID IS NOT NULL AND exp_compare_data.in_asset_dtl_cd IS NOT NULL;


-- Component upd_AGMT_ASSET_DTL_CD_XREF_rapa, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_AGMT_ASSET_DTL_CD_XREF_rapa AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_agmt_asset_dtl_cd_xref_INSERT.in_PRTY_ASSET_ID as in_PRTY_ASSET_ID1,
rtr_agmt_asset_dtl_cd_xref_INSERT.in_AGMT_ID as in_AGMT_ID1,
rtr_agmt_asset_dtl_cd_xref_INSERT.in_asset_dtl_cd as in_asset_dtl_cd1,
rtr_agmt_asset_dtl_cd_xref_INSERT.out_XREF_STRT_DTTM as out_XREF_STRT_DTTM1,
rtr_agmt_asset_dtl_cd_xref_INSERT.asset_dtl_txt as asset_dtl_txt1,
rtr_agmt_asset_dtl_cd_xref_INSERT.in_RAPA_VERSION as in_RAPA_VERSION,
rtr_agmt_asset_dtl_cd_xref_INSERT.out_blank as out_blank1,
rtr_agmt_asset_dtl_cd_xref_INSERT.out_prcs_id as out_prcs_id1,
rtr_agmt_asset_dtl_cd_xref_INSERT.out_EDW_START_DTTM as out_EDW_START_DTTM1,
rtr_agmt_asset_dtl_cd_xref_INSERT.out_EDW_END_DTTM as out_EDW_END_DTTM1,
rtr_agmt_asset_dtl_cd_xref_INSERT.out_rapa_UpdateTime as out_rapa_UpdateTime1,
rtr_agmt_asset_dtl_cd_xref_INSERT.out_TRANS_END_DTTM as out_TRANS_END_DTTM1,
0 as UPDATE_STRATEGY_ACTION
FROM
rtr_agmt_asset_dtl_cd_xref_INSERT
);


-- Component AGMT_ASSET_DTL_CD_XREF, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_ASSET_DTL_CD_XREF
(
PRTY_ASSET_ID,
AGMT_ID,
ASSET_DTL_CD,
AGMT_ASSET_DTL_XREF_STRT_DTTM,
AGMT_ASSET_DTL_XREF_END_DTTM,
AGMT_ASSET_DTL_CNT,
AGMT_ASSET_DTL_TXT,
AGMT_ASSET_DTL_QTY,
AGMT_ASSET_DTL_RATE,
AGMT_ASSET_DTL_AMT,
AGMT_ASSET_DTL_DT,
UOM_CD,
UOM_TYPE_CD,
CURY_CD,
ASSET_DTL_CD_IND,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM,
RAPA_VRSN
)
SELECT
upd_AGMT_ASSET_DTL_CD_XREF_rapa.in_PRTY_ASSET_ID1 as PRTY_ASSET_ID,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.in_AGMT_ID1 as AGMT_ID,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.in_asset_dtl_cd1 as ASSET_DTL_CD,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.out_XREF_STRT_DTTM1 as AGMT_ASSET_DTL_XREF_STRT_DTTM,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.out_blank1 as AGMT_ASSET_DTL_XREF_END_DTTM,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.out_blank1 as AGMT_ASSET_DTL_CNT,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.asset_dtl_txt1 as AGMT_ASSET_DTL_TXT,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.out_blank1 as AGMT_ASSET_DTL_QTY,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.out_blank1 as AGMT_ASSET_DTL_RATE,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.out_blank1 as AGMT_ASSET_DTL_AMT,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.out_blank1 as AGMT_ASSET_DTL_DT,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.out_blank1 as UOM_CD,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.out_blank1 as UOM_TYPE_CD,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.out_blank1 as CURY_CD,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.out_blank1 as ASSET_DTL_CD_IND,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.out_prcs_id1 as PRCS_ID,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.out_EDW_START_DTTM1 as EDW_STRT_DTTM,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.out_EDW_END_DTTM1 as EDW_END_DTTM,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.out_rapa_UpdateTime1 as TRANS_STRT_DTTM,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.out_TRANS_END_DTTM1 as TRANS_END_DTTM,
upd_AGMT_ASSET_DTL_CD_XREF_rapa.in_RAPA_VERSION as RAPA_VRSN
FROM
upd_AGMT_ASSET_DTL_CD_XREF_rapa;


-- PIPELINE END FOR 1

-- PIPELINE START FOR 2

-- Component SQ_pcx_rapa_alfa1, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pcx_rapa_alfa1 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PublicID,
$2 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT publicid_stg

FROM

DB_T_PROD_STAG.pc_policyperiod where 1=2
) SRC
)
);


-- Component AGMT_ASSET_DTL_CD_XREF1, Type TARGET 
INSERT INTO DB_T_PROD_CORE.AGMT_ASSET_DTL_CD_XREF
(
PRTY_ASSET_ID
)
SELECT
SQ_pcx_rapa_alfa1.PublicID as PRTY_ASSET_ID
FROM
SQ_pcx_rapa_alfa1;


-- PIPELINE END FOR 2
-- Component AGMT_ASSET_DTL_CD_XREF1, Type Post SQL 
UPDATE  db_t_prod_core.AGMT_ASSET_DTL_CD_XREF  
set EDW_END_DTTM=A.LEAD1,
TRANS_END_DTTM=A.LEAD2

FROM  

(

SELECT	distinct PRTY_ASSET_ID,AGMT_ID,AGMT_ASSET_DTL_CD_XREF.ASSET_DTL_CD, ASSET_DTL_CD_IND,AGMT_ASSET_DTL_XREF_STRT_DTTM,

AGMT_ASSET_DTL_CD_XREF.EDW_STRT_DTTM,TRANS_STRT_DTTM,

max(AGMT_ASSET_DTL_CD_XREF.EDW_STRT_DTTM) over (partition by PRTY_ASSET_ID,AGMT_ID,AGMT_ASSET_DTL_CD_XREF.ASSET_DTL_CD, ASSET_DTL_CD_IND ORDER BY AGMT_ASSET_DTL_CD_XREF.EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as LEAD1,

 max(TRANS_STRT_DTTM) over (partition by PRTY_ASSET_ID,AGMT_ID,AGMT_ASSET_DTL_CD_XREF.ASSET_DTL_CD,ASSET_DTL_CD_IND  ORDER BY AGMT_ASSET_DTL_CD_XREF.EDW_STRT_DTTM  ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND'' 

 as LEAD2

FROM 

db_t_prod_core.AGMT_ASSET_DTL_CD_XREF  
  JOIN db_t_prod_core.ASSET_DTL_TYPE ADTL ON ADTL.ASSET_DTL_CD=AGMT_ASSET_DTL_CD_XREF.ASSET_DTL_CD

where CAST(AGMT_ASSET_DTL_CD_XREF.EDW_END_DTTM AS DATE)=''9999-12-31''

AND ASSET_DTL_SCHM_TYPE_CD in (''RAPA'',''VEH'')

)  A


where  

AGMT_ASSET_DTL_CD_XREF.PRTY_ASSET_ID=A.PRTY_ASSET_ID

and AGMT_ASSET_DTL_CD_XREF.AGMT_ID=A.AGMT_ID

and AGMT_ASSET_DTL_CD_XREF.ASSET_DTL_CD=A.ASSET_DTL_CD

and coalesce(AGMT_ASSET_DTL_CD_XREF.ASSET_DTL_CD_IND,''*'') = Coalesce(A.ASSET_DTL_CD_IND,''*'')

and AGMT_ASSET_DTL_CD_XREF.EDW_STRT_DTTM = A.EDW_STRT_DTTM

and LEAD1  is not null

and LEAD2 is not null;


END; ';