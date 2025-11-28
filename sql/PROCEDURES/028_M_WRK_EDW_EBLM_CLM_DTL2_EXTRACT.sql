-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_WRK_EDW_EBLM_CLM_DTL2_EXTRACT("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
  PRCS_ID STRING;
BEGIN
  SELECT 
    TRY_PARSE_JSON(:param_json):PRCS_ID::STRING
  INTO
    PRCS_ID;

-- Component sq_edw_eblm_clm_dtl2, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_edw_eblm_clm_dtl2 AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PLCY_VEH_KEY,
$2 as CLM_NBR,
$3 as CLM_LOSS_DT,
$4 as ACC_ST_CD,
$5 as BI_LOSS_RES_AMT,
$6 as PD_LOSS_RES_AMT,
$7 as MED_LOSS_RES_AMT,
$8 as CMP_LOSS_RES_AMT,
$9 as COL_LOSS_RES_AMT,
$10 as UNB_LOSS_RES_AMT,
$11 as UND_LOSS_RES_AMT,
$12 as UNP_LOSS_RES_AMT,
$13 as SL_LOSS_RES_AMT,
$14 as UNSL_LOSS_RES_AMT,
$15 as LOI_LOSS_RES_AMT,
$16 as ERS_LOSS_RES_AMT,
$17 as LOU_LOSS_RES_AMT,
$18 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT	

/* CAST(TRIM(AG.agmt_id)||TRIM(agmt_asset.prty_asset_id)||TRIM(asset_cntrct_role_sbtype_cd)||TRIM(to_char(agmt_asset_strt_dttm,''MM/DD/YYYY'')) AS    ---VARCHAR(200)) as plcy_veh_key, */
	CAST(TRIM(AG.agmt_id)||TRIM(MOTR_VEH.MOTR_VEH_SER_NUM) AS VARCHAR(200)) as plcy_veh_key,

	 clm.clm_num, 

	clm_dt.clm_dttm AS clm_loss_dt,

	terr.geogrcl_area_shrt_name AS acc_st_cd,

	SUM(

 	CASE 

		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RESRV'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL'' 

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''Loss'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''BI''

 		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

	- 

 	CASE 

  		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL''

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''BI''

	AND clm_expsr_trans.does_not_erode_rserv_ind=''0''

   		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

 	) AS bi_loss_res_amt,              



  	SUM(

 	CASE 

		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RESRV'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL'' 

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''Loss'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''PD''

 		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

	- 

 	CASE 

  		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL''

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''PD''

	AND clm_expsr_trans.does_not_erode_rserv_ind=''0''

   		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

 	) AS pd_loss_res_amt,       



	SUM(

 	CASE 

		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RESRV'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL'' 

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''Loss'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''MED''

 		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

	- 

 	CASE 

  		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL''

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''MED''

/* 		AND clm_expsr_trans.does_not_erode_rserv_ind=''0'' */
   		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

 	) AS med_loss_res_amt,                



  	SUM(

 	CASE 

		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RESRV'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL'' 

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''Loss'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''CMP''

 		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

	- 

 	CASE 

  		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL''

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''CMP''

   AND clm_expsr_trans.does_not_erode_rserv_ind=''0''

   		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

 	) AS cmp_loss_res_amt,       

 	

 	SUM(

 	CASE 

		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RESRV'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL'' 

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''Loss'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''Loss''  

	AND	feat.insrnc_cvge_type_cd = ''COL''

 		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

	- 

 	CASE 

  		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL''

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''COL''

	AND clm_expsr_trans.does_not_erode_rserv_ind=''0''

   		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

 	) AS col_loss_res_amt,                



  	SUM(

 	CASE 

		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RESRV'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL'' 

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''Loss'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''UNB''

 		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

	- 

 	CASE 

  		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL''

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''UNB''

	AND clm_expsr_trans.does_not_erode_rserv_ind=''0''	

   		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

 	) AS unb_loss_res_amt,      

 	 

 	SUM(

 	CASE 

		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RESRV'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL'' 

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''Loss'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''UND''

 		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

	- 

 	CASE 

  		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL''

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''UND''

	AND clm_expsr_trans.does_not_erode_rserv_ind=''0''	

   		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

 	) AS und_loss_res_amt,                



  	SUM(

 	CASE 

		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RESRV'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL'' 

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''Loss'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''UNP''

 		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

	- 

 	CASE 

  		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL''

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''UNP''

	AND clm_expsr_trans.does_not_erode_rserv_ind=''0''	

   		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

 	) AS unp_loss_res_amt,    

 	   

 	SUM(

 	CASE 

		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RESRV'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL'' 

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''Loss'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''SL''

 		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

	- 

 	CASE 

  		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL''

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''Loss'' 

		AND clm_expsr_trans.does_not_erode_rserv_ind=''0''	

	AND	feat.insrnc_cvge_type_cd = ''SL''

   		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

 	) AS sl_loss_res_amt,                



  	SUM(

 	CASE 

		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RESRV'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL'' 

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''Loss'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''UNS''

 		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

	- 

 	CASE 

  		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL''

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''UNS''

	AND clm_expsr_trans.does_not_erode_rserv_ind=''0''	

   		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

 	) AS unsl_loss_res_amt,      

 	

 	SUM(

 	CASE 

		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RESRV'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL'' 

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''Loss'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''LOI''

 		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

	- 

 	CASE 

  		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL''

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''LOI''

		AND clm_expsr_trans.does_not_erode_rserv_ind=''0''	

   		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

 	) AS loi_loss_res_amt,                



     

 	SUM(

 	CASE 

		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RESRV'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL'' 

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''Loss'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''ERS''

 		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

	- 

 	CASE 

  		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL''

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''ERS''

	AND clm_expsr_trans.does_not_erode_rserv_ind=''0''	

   		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

 	) AS ers_loss_res_amt,                



  	SUM(

 	CASE 

		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''RESRV'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL'' 

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''Loss'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd=''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''LOU''

 		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

	- 

 	CASE 

  		WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd =''PYMNT'' 

	AND	clm_expsr_trans.rcvry_ctgy_type_cd IS NULL 

	AND	clm_expsr_trans.expsr_cost_type_cd=''PDL''

	AND	clm_expsr_trans.expsr_cost_ctgy_type_cd=''LOSS'' 

	AND	clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''Loss'' 

	AND	feat.insrnc_cvge_type_cd = ''LOU''

	AND clm_expsr_trans.does_not_erode_rserv_ind=''0''	

   		THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt 

ELSE	0 

END	

 	) AS lou_loss_res_amt

 	                

FROM    

    DB_T_PROD_CORE.CLM

    INNER JOIN DB_T_PROD_CORE.clm_expsr ON          clm.clm_id=clm_expsr.clm_id

    LEFT JOIN  DB_T_PROD_CORE.clm_dt ON  clm.clm_id=clm_dt.clm_id AND           clm_dt_type_cd=''LOSS'' AND clm_dt.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

    LEFT JOIN  DB_T_PROD_CORE.clm_loctr ON              clm.clm_id=clm_loctr.clm_id AND       clm_loctr_role_cd=''LOSSSTADRS'' AND clm_loctr.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

    INNER JOIN DB_T_PROD_CORE.street_addr ON clm_loctr.loc_id = street_addr.street_addr_id AND street_addr.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

    INNER JOIN DB_T_PROD_CORE.terr ON      terr.terr_id=street_addr.terr_id AND terr.EDW_END_DTTM=''9999-12-31 23:59:59.999999''  

    LEFT JOIN  DB_T_PROD_CORE.clm_expsr_trans ON  clm_expsr_trans.clm_expsr_id=clm_expsr.clm_expsr_id AND clm_expsr_trans.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

    LEFT JOIN  DB_T_PROD_CORE.clm_expsr_trans_lnitm ON        clm_expsr_trans_lnitm.clm_expsr_trans_id=clm_expsr_trans.clm_expsr_trans_id AND clm_expsr_trans_lnitm.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

    LEFT JOIN  DB_T_PROD_CORE.feat ON       clm_expsr.cvge_feat_id=feat.feat_id AND feat.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

    LEFT JOIN  DB_T_PROD_CORE.clm_insrbl_int ON      clm_insrbl_int.clm_id=clm.clm_id AND clm_insrbl_int.EDW_END_DTTM=''9999-12-31 23:59:59.999999''  and clm_insrbl_int.clm_insrbl_int_type_cd=''VEH''

    LEFT JOIN  DB_T_PROD_CORE.insrbl_int ON              clm_insrbl_int.insrbl_int_id=insrbl_int.insrbl_int_id AND insrbl_int.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

    

/* LEFT JOIN DB_T_PROD_CORE.prty_asset ON              prty_asset.prty_asset_id=insrbl_int.prty_asset_id AND     prty_asset_sbtype_cd=''MVEH'' AND                prty_asset_clasfcn_cd=''MV'' AND prty_asset.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' */
/*   LEFT JOIN  DB_T_PROD_CORE.motr_veh ON              prty_asset.prty_asset_id=motr_veh.prty_asset_id AND motr_veh.EDW_END_DTTM=''9999-12-31 23:59:59.999999''  */
       JOIN

(

Select AC.AGMT_ID,AC.CLM_ID,AG.HOST_AGMT_NUM from DB_T_PROD_CORE.AGMT_CLM AC

JOIN

DB_T_PROD_CORE.AGMT AG ON AG.AGMT_ID=AC.AGMT_ID AND AG.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 



AND       AG.modl_crtn_dttm = (

				SELECT                MAX(aa.modl_crtn_dttm)  

				FROM    DB_T_PROD_CORE.AGMT aa 

				WHERE aa.AGMT_TYPE_CD = ''PPV''

				AND aa.AGMT_CUR_STS_CD=''BOUND'' 

				AND       aa.host_agmt_num=AG.host_agmt_num)

/*  To capture only the most recent model for a DB_T_CORE_DM_PROD.policy  */
WHERE

AC.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

)  AG ON CLM.CLM_ID=AG.CLM_ID



/* LEFT JOIN  DB_T_PROD_CORE.agmt_asset ON          agmt_asset.prty_asset_id=prty_asset.prty_asset_id AND agmt_asset.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' */
JOIN  DB_T_PROD_CORE.agmt_asset ON          AG.AGMT_ID=agmt_asset.AGMT_ID AND agmt_asset.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

JOIN  DB_T_PROD_CORE.prty_asset  ON          agmt_asset.prty_asset_id=prty_asset.prty_asset_id AND agmt_asset.EDW_END_DTTM=''9999-12-31 23:59:59.999999''

JOIN  DB_T_PROD_CORE.motr_veh ON              prty_asset.prty_asset_id=motr_veh.prty_asset_id AND motr_veh.EDW_END_DTTM=''9999-12-31 23:59:59.999999'' 

JOIN DB_T_PROD_CORE.AGMT_PROD 											

	ON	AG.AGMT_ID=AGMT_PROD.AGMT_ID AND AGMT_PROD.EDW_END_DTTM=''9999-12-31 23:59:59.999999''									

JOIN DB_T_PROD_CORE.PROD 											

	ON	AGMT_PROD.PROD_ID=PROD.PROD_ID AND PROD.EDW_END_DTTM=''9999-12-31 23:59:59.999999''	

	and PROD.PROD_NAME IN (''PPV'', ''AUTO'', ''COMMERCIAL'', ''PERSONAL AUTO'') 



									



/* WHERE              clm.clm_num =  ''A0000000181''              */

GROUP BY 

                plcy_veh_key,

    clm_num,

    clm_loss_dt,

    acc_st_cd
	
HAVING                

                bi_loss_res_amt <> 0

                OR pd_loss_res_amt <> 0

                OR med_loss_res_amt <> 0

                OR cmp_loss_res_amt <> 0

                OR col_loss_res_amt <> 0

                OR unb_loss_res_amt <> 0

                OR und_loss_res_amt <> 0

                OR unp_loss_res_amt <> 0

                OR sl_loss_res_amt <> 0

                OR unsl_loss_res_amt <> 0

                OR loi_loss_res_amt <> 0

                OR bi_loss_res_amt <> 0

                OR ers_loss_res_amt <> 0

                OR lou_loss_res_amt <> 0
) SRC
)
);


-- Component exp_pass_to_target, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target AS
(
SELECT
sq_edw_eblm_clm_dtl2.PLCY_VEH_KEY as PLCY_VEH_KEY,
sq_edw_eblm_clm_dtl2.CLM_NBR as CLM_NBR,
sq_edw_eblm_clm_dtl2.CLM_LOSS_DT as CLM_LOSS_DT,
sq_edw_eblm_clm_dtl2.ACC_ST_CD as ACC_ST_CD,
sq_edw_eblm_clm_dtl2.BI_LOSS_RES_AMT as BI_LOSS_RES_AMT,
sq_edw_eblm_clm_dtl2.PD_LOSS_RES_AMT as PD_LOSS_RES_AMT,
sq_edw_eblm_clm_dtl2.MED_LOSS_RES_AMT as MED_LOSS_RES_AMT,
sq_edw_eblm_clm_dtl2.CMP_LOSS_RES_AMT as CMP_LOSS_RES_AMT,
sq_edw_eblm_clm_dtl2.COL_LOSS_RES_AMT as COL_LOSS_RES_AMT,
sq_edw_eblm_clm_dtl2.UNB_LOSS_RES_AMT as UNB_LOSS_RES_AMT,
sq_edw_eblm_clm_dtl2.UND_LOSS_RES_AMT as UND_LOSS_RES_AMT,
sq_edw_eblm_clm_dtl2.UNP_LOSS_RES_AMT as UNP_LOSS_RES_AMT,
sq_edw_eblm_clm_dtl2.SL_LOSS_RES_AMT as SL_LOSS_RES_AMT,
sq_edw_eblm_clm_dtl2.UNSL_LOSS_RES_AMT as UNSL_LOSS_RES_AMT,
sq_edw_eblm_clm_dtl2.LOI_LOSS_RES_AMT as LOI_LOSS_RES_AMT,
sq_edw_eblm_clm_dtl2.ERS_LOSS_RES_AMT as ERS_LOSS_RES_AMT,
sq_edw_eblm_clm_dtl2.LOU_LOSS_RES_AMT as LOU_LOSS_RES_AMT,
:PRCS_ID as prcs_id,
CURRENT_TIMESTAMP as load_dt,
sq_edw_eblm_clm_dtl2.source_record_id
FROM
sq_edw_eblm_clm_dtl2
);


-- Component tgt_edw_eblm_clm_dtl2, Type TARGET 
INSERT INTO DB_T_PROD_WRK.EDW_EBLM_CLM_DTL2
(
PLCY_VEH_KEY,
CLM_NBR,
CLM_LOSS_DT,
ACC_ST_CD,
BI_LOSS_RES_AMT,
PD_LOSS_RES_AMT,
MED_LOSS_RES_AMT,
CMP_LOSS_RES_AMT,
COL_LOSS_RES_AMT,
UNB_LOSS_RES_AMT,
UND_LOSS_RES_AMT,
UNP_LOSS_RES_AMT,
SL_LOSS_RES_AMT,
UNSL_LOSS_RES_AMT,
LOI_LOSS_RES_AMT,
ERS_LOSS_RES_AMT,
LOU_LOSS_RES_AMT,
PRCS_ID,
LOAD_DT
)
SELECT
exp_pass_to_target.PLCY_VEH_KEY as PLCY_VEH_KEY,
exp_pass_to_target.CLM_NBR as CLM_NBR,
exp_pass_to_target.CLM_LOSS_DT as CLM_LOSS_DT,
exp_pass_to_target.ACC_ST_CD as ACC_ST_CD,
exp_pass_to_target.BI_LOSS_RES_AMT as BI_LOSS_RES_AMT,
exp_pass_to_target.PD_LOSS_RES_AMT as PD_LOSS_RES_AMT,
exp_pass_to_target.MED_LOSS_RES_AMT as MED_LOSS_RES_AMT,
exp_pass_to_target.CMP_LOSS_RES_AMT as CMP_LOSS_RES_AMT,
exp_pass_to_target.COL_LOSS_RES_AMT as COL_LOSS_RES_AMT,
exp_pass_to_target.UNB_LOSS_RES_AMT as UNB_LOSS_RES_AMT,
exp_pass_to_target.UND_LOSS_RES_AMT as UND_LOSS_RES_AMT,
exp_pass_to_target.UNP_LOSS_RES_AMT as UNP_LOSS_RES_AMT,
exp_pass_to_target.SL_LOSS_RES_AMT as SL_LOSS_RES_AMT,
exp_pass_to_target.UNSL_LOSS_RES_AMT as UNSL_LOSS_RES_AMT,
exp_pass_to_target.LOI_LOSS_RES_AMT as LOI_LOSS_RES_AMT,
exp_pass_to_target.ERS_LOSS_RES_AMT as ERS_LOSS_RES_AMT,
exp_pass_to_target.LOU_LOSS_RES_AMT as LOU_LOSS_RES_AMT,
exp_pass_to_target.prcs_id as PRCS_ID,
exp_pass_to_target.load_dt as LOAD_DT
FROM
exp_pass_to_target;


END; ';