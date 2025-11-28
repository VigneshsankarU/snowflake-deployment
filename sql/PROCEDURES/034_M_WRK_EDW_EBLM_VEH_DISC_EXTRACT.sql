-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_WRK_EDW_EBLM_VEH_DISC_EXTRACT("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
  PRCS_ID STRING;
  CAL_END_DT STRING;
BEGIN
  SELECT 
    TRY_PARSE_JSON(:param_json):PRCS_ID::STRING,
	TRY_PARSE_JSON(:param_json):CAL_END_DT::STRING
  INTO
    PRCS_ID,
	CAL_END_DT;

-- Component sq_edw_eblm_veh_disc, Type Pre SQL 
DELETE FROM DB_WRK.EDW_EBLM_VEH_DISC WHERE MO_ID=EXTRACT(YEAR FROM CAST('':CAL_END_DT'' AS DATE))*100+EXTRACT(MONTH FROM CAST('':CAL_END_DT'' AS DATE));


-- Component sq_edw_eblm_veh_disc, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_edw_eblm_veh_disc AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as MO_ID,
$2 as PLCY_VEH_KEY,
$3 as DW_PLCY_SKEY,
$4 as VEH_CHG_EFF_DT,
$5 as VEH_CHG_EXP_DT,
$6 as COMBO_DSC,
$7 as MULTICAR_DSC,
$8 as VC3_DSC,
$9 as VC6_DSC,
$10 as AB_DSC,
$11 as AT_DSC,
$12 as NPIS_DSC,
$13 as RTU_DSC,
$14 as DTR_DSC,
$15 as HSD_DSC,
$16 as DDC_DSC,
$17 as HOD_DSC,
$18 as LFD_DSC,
$19 as CGHS_DSC,
$20 as ABS_DSC,
$21 as ISE_RATING_FACTOR,
$22 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT DISTINCT

EXTRACT(YEAR FROM '':CAL_END_DT'' )*100+EXTRACT(MONTH FRoM '':CAL_END_DT'') mo_id,

CAST(agmt.host_agmt_num||motr_veh.motr_veh_ser_num AS VARCHAR(200)) as plcy_veh_key, /*  Refer emblem DB_T_CORE_DM_PROD.POLICY feed */
	agmt.agmt_id,

agmt_asset.agmt_asset_strt_dttm, /*  populated as CURRENT_TIMESTAMP looks incorrect  */
agmt_asset.agmt_asset_end_dttm,  /*  populated as null */
/* DB_T_CORE_DM_PROD.POLICY DB_T_CORE_DM_PROD.discount */
	MAX(CASE WHEN  feat.feat_sbtype_cd = ''MOD'' AND feat.feat_desc = ''COMBO DISCOUNT'' THEN ''Y'' ELSE ''N'' END) COMBO_DSC,

MAX(CASE WHEN  feat.feat_sbtype_cd = ''MOD'' AND feat.feat_desc = ''MULTI-CAR DISCOUNT'' THEN ''Y'' ELSE ''N'' END) MULTICAR_DSC, /*  additional  date conditions */
/* MAX(CASE WHEN (DATE - CNTNUS_SRVC_DT) YEAR > 3 and (DATE - CNTNUS_SRVC_DT )YEAR < 6 then ''Y'' else ''N'' END)  */
/* ValueClient_Discount_percentage, */
       MAX(CASE WHEN (EXTRACT(YEAR FROM '':CAL_END_DT'')	- EXTRACT(YEAR FROM CNTNUS_SRVC_DTTM)) >= 3 AND 

                                       (EXTRACT(YEAR FROM '':CAL_END_DT'')	- EXTRACT(YEAR FROM CNTNUS_SRVC_DTTM)) < 6 THEN ''Y'' ELSE ''N'' END     

     )   VC3_DSC_new	,	

/*  added code for defect 19119 ID000045 END */
     MAX(CASE WHEN (EXTRACT(YEAR FROM '':CAL_END_DT'')	- EXTRACT(YEAR FROM CNTNUS_SRVC_DTTM)) >= 6  THEN ''Y'' ELSE ''N'' END     

     )   VC6_DSC_new	,	

/* DB_T_CORE_PROD.vehicle DB_T_CORE_DM_PROD.discount */
    MAX(CASE WHEN  feat_veh.feat_sbtype_cd = ''MOD'' AND feat_veh.feat_desc = ''AIRBAG DISCOUNT'' THEN ''Y'' ELSE ''N'' END) AB_DSC, 

    MAX(CASE WHEN  feat_veh.feat_sbtype_cd = ''MOD'' AND feat_veh.feat_desc = ''ANTI THEFT DISCOUNT'' THEN ''Y'' ELSE ''N'' END) AT_DSC,

MAX(CASE WHEN  feat_veh.feat_sbtype_cd = ''MOD'' AND feat_veh.feat_desc = ''NO PRIOR INSURANCE SURCHARGE'' THEN ''Y'' ELSE ''N'' END) NPIS_DSC, /*  mapping document needs to be updated */
    MAX(CASE WHEN  feat_veh.feat_sbtype_cd = ''MOD'' AND feat_veh.feat_desc = ''OUT OF DB_T_SHRD_PROD.STATE SURCHARGE'' THEN ''Y'' ELSE ''N'' END) RTU_DSC,

    MAX(CASE WHEN  feat_veh.feat_sbtype_cd = ''MOD'' AND feat_veh.feat_desc = ''DRIVER TRAINING DISCOUNT'' THEN ''Y'' ELSE ''N'' END) DTR_DSC,

    MAX(CASE WHEN  feat_veh.feat_sbtype_cd = ''MOD'' AND feat_veh.feat_desc = ''YOUTH HONOR STUDENT'' THEN ''Y'' ELSE ''N'' END) HSD_DSC,

    MAX(CASE WHEN  feat_veh.feat_sbtype_cd = ''MOD'' AND feat_veh.feat_desc = ''DRIVER TRAINING DISCOUNT'' THEN ''Y'' ELSE ''N'' END) DDC_DSC,

    

/* DB_T_CORE_DM_PROD.POLICY DB_T_CORE_DM_PROD.discount */
    MAX(CASE WHEN  feat.feat_sbtype_cd = ''MOD'' AND feat.feat_desc = ''HOMEOWNER DISCOUNT'' THEN ''Y'' ELSE ''N'' END) HOD_DSC,

    MAX(CASE WHEN  feat.feat_sbtype_cd = ''MOD'' AND feat.feat_desc = ''LIFE DB_T_CORE_DM_PROD.POLICY DISCOUNT'' THEN ''Y'' ELSE ''N'' END) LFD_DSC,

''N'' as CGHS_DSC, /* Mapping document says leave as 0 */
    MAX(CASE WHEN  feat_veh.feat_sbtype_cd = ''MOD'' AND feat_veh.feat_desc = ''ANTI-LOCK BRAKES'' THEN ''Y'' ELSE ''N'' END) ABS_DSC,

MAX(COALESCE(SCR_FCTR_RATE,'''')) AS ISE_RATING_FACTOR /*  one one record in DB_T_PROD_CORE.AGMT_SCR table */
FROM

 	(SELECT * FROM 

(SELECT  PPV.HOST_AGMT_NUM,PPV.AGMT_ID,PPV.AGMT_SRC_CD,MODL_CRTN_DTTM,  A_S.AGMT_STS_CD,A_S.AGMT_STS_RSN_CD,

CASE WHEN A_S.AGMT_STS_CD = ''CNCLD''  AND A_S.AGMT_STS_RSN_CD = ''CHGUWCMPY'' THEN ''Y'' END AS C2C_CANC_IND ,PPV.AGMT_EFF_DTTM,

PPV.AGMT_SIGND_DTTM,PPV.MODL_ACTL_END_DTTM,PPV.MODL_EFF_DTTM,PPV.CNTNUS_SRVC_DTTM,PPV.AGMT_PLND_EXPN_DTTM,AGMT_OPN_DTTM

FROM (SELECT  T1.HOST_AGMT_NUM,T1.TERM_NUM,T1.AGMT_ID,T1.MODL_CRTN_DTTM,T1.AGMT_SRC_CD,

  T1.MODL_EFF_DTTM,T1.AGMT_EFF_DTTM,T1.AGMT_SIGND_DTTM,T1.MODL_ACTL_END_DTTM,T1.CNTNUS_SRVC_DTTM,

  T1.AGMT_PLND_EXPN_DTTM,T1.AGMT_OPN_DTTM,

CASE WHEN T1.MODL_EFF_DTTM > T1.MODL_CRTN_DTTM THEN T1.MODL_EFF_DTTM ELSE T1.MODL_CRTN_DTTM END AS NEW_AGMT_EFF_DTTM

FROM DB_T_PROD_CORE.AGMT T1 WHERE   T1.AGMT_TYPE_CD = ''PPV''  AND T1.SRC_SYS_CD = ''GWPC'' 

 AND CAST('':CAL_END_DT'' AS DATE)  BETWEEN CAST(AGMT_EFF_DTTM AS DATE) AND CAST(AGMT_PLND_EXPN_DTTM AS DATE)

 AND T1.TRANS_STRT_DTTM = (SELECT  MIN(T2.TRANS_STRT_DTTM) FROM DB_T_PROD_CORE.AGMT T2 WHERE   T1.AGMT_ID = T2.AGMT_ID) 

 AND CAST(NEW_AGMT_EFF_DTTM AS DATE) <=  CAST('':CAL_END_DT'' AS DATE)  ) PPV 

INNER JOIN 

(SELECT  HOST_AGMT_NUM, TERM_NUM,AGMT_ID FROM DB_T_PROD_CORE.AGMT WHERE   AGMT_TYPE_CD = ''POLTRM'' 

 AND CAST('':CAL_END_DT'' AS DATE)  BETWEEN CAST(AGMT_EFF_DTTM AS DATE) AND CAST(AGMT_PLND_EXPN_DTTM AS DATE) GROUP   BY 1,2,3) TRM 

 ON  PPV.HOST_AGMT_NUM = TRM.HOST_AGMT_NUM  AND PPV.TERM_NUM = TRM.TERM_NUM  

INNER JOIN DB_T_PROD_CORE.AGMT_STS A_S 

 ON  TRM.AGMT_ID = A_S.AGMT_ID   AND CAST(A_S.AGMT_STS_STRT_DTTM AS DATE) <= CAST('':CAL_END_DT'' AS DATE)   AND A_S.AGMT_STS_CD <> ''CNFRMDDT'' 

QUALIFY ROW_NUMBER() OVER(PARTITION BY PPV.AGMT_ID ORDER   BY A_S.AGMT_STS_STRT_DTTM DESC) = 1 

)  A  QUALIFY ROW_NUMBER() OVER(PARTITION BY HOST_AGMT_NUM ORDER   BY  CASE WHEN (AGMT_STS_CD=''INFORCE'') THEN 1 ELSE 2 END ,MODL_CRTN_DTTM DESC ) = 1)AGMT

JOIN DB_T_PROD_CORE.AGMT_PROD  ON AGMT.AGMT_ID=AGMT_PROD.AGMT_ID AND AGMT_PROD.EDW_END_DTTM=''9999-12-31 23:59:59.999999''  

AND AGMT_STS_CD=''INFORCE''

JOIN DB_T_PROD_CORE.PROD  ON AGMT_PROD.PROD_ID=PROD.PROD_ID AND PROD.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

 and PROD.PROD_NAME IN (''PPV'', ''AUTO'',''PPV2'', ''COMMERCIAL'', ''PERSONAL AUTO'')   

 	INNER JOIN DB_T_PROD_CORE.agmt_asset ON agmt_asset.agmt_id=agmt.agmt_id  AND CAST(AGMT_ASSET.TRANS_STRT_DTTM AS DATE)<=CAST('':CAL_END_DT'' AS DATE) AND CAST(AGMT_ASSET.TRANS_END_DTTM AS DATE)>CAST('':CAL_END_DT'' AS DATE)



	LEFT JOIN DB_T_PROD_CORE.AGMT_SCR on agmt_scr.agmt_id = agmt.agmt_id AND AGMT_SCR.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' and MODL_ID = 4

	INNER JOIN DB_T_PROD_CORE.MOTR_VEH ON motr_veh.prty_asset_id=agmt_asset.prty_asset_id AND MOTR_VEH.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

	INNER JOIN DB_T_PROD_CORE.MOTR_VEH_DTL ON motr_veh.prty_asset_id = motr_veh_dtl.prty_asset_id AND MOTR_VEH_DTL.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

 	left JOIN  DB_T_PROD_CORE.AGMT_FEAT  ON agmt.agmt_id=agmt_feat.agmt_id AND AGMT_FEAT.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

	left JOIN  DB_T_PROD_CORE.FEAT ON agmt_feat.feat_id = feat.feat_id AND FEAT.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

left JOIN  DB_T_PROD_CORE.agmt_insrd_asset_feat  ON agmt.agmt_id=agmt_insrd_asset_feat.agmt_id/*  and agmt_asset.prty_asset_id=agmt_insrd_asset_feat.prty_asset_id  */
		AND agmt_insrd_asset_feat.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

	left JOIN  DB_T_PROD_CORE.FEAT feat_veh ON agmt_insrd_asset_feat.feat_id = feat_veh.feat_id AND feat_veh.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

	

WHERE

	plcy_veh_key IS NOT NULL



GROUP BY 

	plcy_veh_key, 

	agmt.agmt_id,

	agmt_asset.agmt_asset_strt_dttm,

	agmt_asset.agmt_asset_end_dttm , mo_id

/* 	ISE_RATING_FACTOR */
) SRC
)
);


-- Component exp_pass_to_target, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target AS
(
SELECT
sq_edw_eblm_veh_disc.PLCY_VEH_KEY as PLCY_VEH_KEY,
sq_edw_eblm_veh_disc.DW_PLCY_SKEY as DW_PLCY_SKEY,
sq_edw_eblm_veh_disc.VEH_CHG_EFF_DT as VEH_CHG_EFF_DT,
sq_edw_eblm_veh_disc.VEH_CHG_EXP_DT as VEH_CHG_EXP_DT,
sq_edw_eblm_veh_disc.COMBO_DSC as COMBO_DSC,
sq_edw_eblm_veh_disc.MULTICAR_DSC as MULTICAR_DSC,
sq_edw_eblm_veh_disc.VC3_DSC as VC3_DSC,
sq_edw_eblm_veh_disc.VC6_DSC as VC6_DSC,
sq_edw_eblm_veh_disc.AB_DSC as AB_DSC,
sq_edw_eblm_veh_disc.AT_DSC as AT_DSC,
sq_edw_eblm_veh_disc.NPIS_DSC as NPIS_DSC,
sq_edw_eblm_veh_disc.RTU_DSC as RTU_DSC,
sq_edw_eblm_veh_disc.DTR_DSC as DTR_DSC,
sq_edw_eblm_veh_disc.HSD_DSC as HSD_DSC,
sq_edw_eblm_veh_disc.DDC_DSC as DDC_DSC,
sq_edw_eblm_veh_disc.HOD_DSC as HOD_DSC,
sq_edw_eblm_veh_disc.LFD_DSC as LFD_DSC,
sq_edw_eblm_veh_disc.CGHS_DSC as CGHS_DSC,
sq_edw_eblm_veh_disc.ABS_DSC as ABS_DSC,
sq_edw_eblm_veh_disc.ISE_RATING_FACTOR as ISE_RATING_FACTOR,
:PRCS_ID as PRCS_ID,
CURRENT_TIMESTAMP as LOAD_DT,
sq_edw_eblm_veh_disc.MO_ID as MO_ID,
sq_edw_eblm_veh_disc.source_record_id
FROM
sq_edw_eblm_veh_disc
);


-- Component tgt_edw_eblm_veh_disc, Type TARGET 
INSERT INTO DB_T_PROD_WRK.EDW_EBLM_VEH_DISC
(
MO_ID,
PLCY_VEH_KEY,
DW_PLCY_SKEY,
VEH_CHG_EFF_DT,
VEH_CHG_EXP_DT,
COMBO_DSC,
MULTICAR_DSC,
VC3_DSC,
VC6_DSC,
AB_DSC,
AT_DSC,
NPIS_DSC,
RTU_DSC,
DTR_DSC,
HSD_DSC,
DDC_DSC,
HOD_DSC,
LFD_DSC,
CGHS_DSC,
ABS_DSC,
ISE_RATING_FACTOR,
PRCS_ID,
LOAD_DT
)
SELECT
exp_pass_to_target.MO_ID as MO_ID,
exp_pass_to_target.PLCY_VEH_KEY as PLCY_VEH_KEY,
exp_pass_to_target.DW_PLCY_SKEY as DW_PLCY_SKEY,
exp_pass_to_target.VEH_CHG_EFF_DT as VEH_CHG_EFF_DT,
exp_pass_to_target.VEH_CHG_EXP_DT as VEH_CHG_EXP_DT,
exp_pass_to_target.COMBO_DSC as COMBO_DSC,
exp_pass_to_target.MULTICAR_DSC as MULTICAR_DSC,
exp_pass_to_target.VC3_DSC as VC3_DSC,
exp_pass_to_target.VC6_DSC as VC6_DSC,
exp_pass_to_target.AB_DSC as AB_DSC,
exp_pass_to_target.AT_DSC as AT_DSC,
exp_pass_to_target.NPIS_DSC as NPIS_DSC,
exp_pass_to_target.RTU_DSC as RTU_DSC,
exp_pass_to_target.DTR_DSC as DTR_DSC,
exp_pass_to_target.HSD_DSC as HSD_DSC,
exp_pass_to_target.DDC_DSC as DDC_DSC,
exp_pass_to_target.HOD_DSC as HOD_DSC,
exp_pass_to_target.LFD_DSC as LFD_DSC,
exp_pass_to_target.CGHS_DSC as CGHS_DSC,
exp_pass_to_target.ABS_DSC as ABS_DSC,
exp_pass_to_target.ISE_RATING_FACTOR as ISE_RATING_FACTOR,
exp_pass_to_target.PRCS_ID as PRCS_ID,
exp_pass_to_target.LOAD_DT as LOAD_DT
FROM
exp_pass_to_target;


END; ';