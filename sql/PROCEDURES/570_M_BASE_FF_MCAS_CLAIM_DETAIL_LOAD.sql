-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_FF_MCAS_CLAIM_DETAIL_LOAD("WORKLET_NAME" VARCHAR)
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
 


--set MCAS_START_DT = to_char(current_date -1,''mm/dd/yyyy'');
--set MCAS_END_DT = to_char(current_date ,''mm/dd/yyyy'');
--select to_date(:MCAS_START_DT,''mm/dd/yyyy'');


-- Component sq_clm_expsr, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_clm_expsr AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as UNDERWRITECOMPANY,
$2 as RISKSTATE,
$3 as LOB,
$4 as VEH_IND,
$5 as PROD_NAME,
$6 as FEAT_NAME,
$7 as CLM_NUM,
$8 as CLM_EXPSR_ID,
$9 as NK_SRC_KEY,
$10 as CLM_EXPSR_OPEN_DT,
$11 as CLM_EXPSR_ORIG_CLOSE_DT,
$12 as CLM_EXPSR_CLOSE_DT,
$13 as CLM_EXPSR_FINAL_PYMT_DT,
$14 as LAWSUIT_OP_BEG_COUNT,
$15 as LAWSUIT_OPEN_COUNT,
$16 as LAWSUIT_CLOSED_COUNT,
$17 as LAWSUIT_OPEN_CLOSED_COUNT,
$18 as STATUS,
$19 as CLM_OPEN_BEGIN,
$20 as CLM_OPEN_DURING,
$21 as CLM_OPEN_END,
$22 as CLM_CLOSED_WPAYMENT,
$23 as CLM_CLOSED_WOPAYMENT_DEDUCTIBLE,
$24 as CLM_CLOSED_WOPAYMENT,
$25 as DAYS_TO_FINAL_PMT,
$26 as CLMS_STLD_WP30,
$27 as CLMS_STLD_WP31_60,
$28 as CLMS_STLD_WP61_90,
$29 as CLMS_STLD_WP91_180,
$30 as CLMS_STLD_WP181_365,
$31 as CLMS_STLD_WP_366,
$32 as CLMS_STLD_WOP_30,
$33 as CLMS_STLD_WOP_31_60,
$34 as CLMS_STLD_WOP_61_90,
$35 as CLMS_STLD_WOP_91_180,
$36 as CLMS_STLD_WOP_181_365,
$37 as CLMS_STLD_WOP_366,
$38 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
With Undrwrte as

(

select	b.clm_id, e.intrnl_org_num ,b.clm_num from								

	( 									

	select	* from	clm where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'' ) b 						

	inner join  ( 									

	select	* from	agmt_clm where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') c 						

		on	b.clm_id = c.clm_id 							

	inner join  ( 									

	select	* from	prty_agmt where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') d 						

		on	( c.agmt_id = d.agmt_id 	and	prty_agmt_role_cd = ''cmp'')					

	inner join  ( 									

	select	* from	intrnl_org where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') e 						

		on	(e.intrnl_org_prty_id = d.prty_id and	e.intrnl_org_sbtype_cd = ''co'')						

	),

	

Rskstate as

	(

	select	d.clm_id, c.geogrcl_area_shrt_name as riskstate								

	from									

	( 									

	select	 *  from	agmt_loctr where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') a 						

	inner join ( 									

	select	* from	agmt_clm where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'')  d 						

		on	a.agmt_id = d.agmt_id							

	inner join loctr b 									

		on	(a.loc_id = b.loctr_id 							

		and	b.geogrcl_area_sbtype_cd = ''ter'')							

	inner join terr c 									

		on	a.loc_id = c.terr_id 							

		and	to_char(c.edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999''							

	),

	

Lob as

	(

	select	a.clm_id, c.insrnc_lob_type_cd as lob, prod_name								

	from	 ( 								

	select	* from	agmt_clm where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') a						

	inner join  ( 									

	select	* from	agmt_prod where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') b 						

		on	a.agmt_id = b.agmt_id							

	inner join prod c 									

		on	(b.prod_id = c.prod_id 							

		and	c.insrnc_lob_type_cd in (''mh'',''pa'',''ho'') 							

		and	to_char(c.edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'')

	),



Lwsuit_op_beg as 	

(

	select	a.clm_id, count(b.legl_actn_id) as lawsuit_op_beg_count  								

	from									

	(									

	select	clm_id, min(legl_actn_strt_dttm) as lawsuit_open_dt, min(legl_actn_end_dttm) as lawsuit_resolved_dt  								

	from									

	( 									

	select	* from	clm_legl_actn where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') clm_legl_actn						

	join  ( 									

	select	* from	legl_actn where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') legl_actn  						

		on	clm_legl_actn.legl_actn_id = legl_actn.legl_actn_id							

	where	legl_actn_type_cd = ''suit'' 								

	group	by clm_id) a								

	join ( 									

	select	* from	clm_legl_actn where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') b 						

		on	a.clm_id = b.clm_id							

	join ( 									

	select	* from	legl_actn where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') c 						

		on	b.legl_actn_id = c.legl_actn_id 							

		and	c. legl_actn_type_cd = ''suit'' 							

	where	 (to_char(lawsuit_open_dt,''mm/dd/yyyy'') < to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')

		and	(lawsuit_resolved_dt is null 							

		or	to_char(lawsuit_resolved_dt,''mm/dd/yyyy'') > to_date(:MCAS_Start_dt , ''MM/DD/YYYY'' )))							

	group	by a.clm_id								

	) ,

	

Lwsuit_open as

(

	select	a.clm_id, count(b.legl_actn_id) as lawsuit_open_count  								

	from									

	(									

	select	clm_id, min(legl_actn_strt_dttm) as lawsuit_open_dt, min(legl_actn_end_dttm) as lawsuit_resolved_dt  								

	from	( 								

	select	* from	clm_legl_actn where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') clm_legl_actn						

	join ( 									

	select	* from	legl_actn where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') legl_actn 						

		on	clm_legl_actn.legl_actn_id = legl_actn.legl_actn_id							

	where	legl_actn_type_cd = ''suit''								

	group	by clm_id) a								

	join ( 									

	select	* from	clm_legl_actn where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') b 						

		on	a.clm_id = b.clm_id							

	join ( 									

	select	* from	legl_actn where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') c 						

		on	b.legl_actn_id = c.legl_actn_id 							

		and	c. legl_actn_type_cd = ''suit'' 							

	where	 (to_char(lawsuit_open_dt,''mm/dd/yyyy'') between to_date(:MCAS_Start_dt , ''MM/DD/YYYY'') 								

		and	to_date(:MCAS_End_dt , ''mm/dd/yyyy''))							

	group	by a.clm_id	

	),



Lwsuit_closed as 

	(									

	select	a.clm_id, count(b.legl_actn_id) as lawsuit_closed_count  								

	from									

	(									

	select	clm_id, min(legl_actn_strt_dttm) as lawsuit_open_dt, min(legl_actn_end_dttm) as lawsuit_resolved_dt  								

	from	( 								

	select	* from	clm_legl_actn where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') clm_legl_actn						

	join ( 									

	select	* from	legl_actn where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') legl_actn 						

		on	clm_legl_actn.legl_actn_id = legl_actn.legl_actn_id							

	where	legl_actn_type_cd = ''suit''								

	group	by clm_id) a								

	join ( 									

	select	* from	clm_legl_actn where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') b 						

		on	a.clm_id = b.clm_id							

	join ( 									

	select	* from	legl_actn where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') c 						

		on	b.legl_actn_id = c.legl_actn_id 							

		and	c. legl_actn_type_cd = ''suit'' 							

	where	 (to_char(lawsuit_resolved_dt,''mm/dd/yyyy'') between to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')							

		and	to_date(:MCAS_End_dt , ''mm/dd/yyyy''))							

	group	by a.clm_id								

	),  



Lwsuit_open_closed as 

(									

	select	a.clm_id, count(b.legl_actn_id) as lawsuit_open_closed_count  								

	from									

	(									

	select	clm_id, min(legl_actn_strt_dttm) as lawsuit_open_dt, min(legl_actn_end_dttm) as lawsuit_resolved_dt  								

	from	( 								

	select	* from	clm_legl_actn where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') clm_legl_actn						

	join ( 									

	select	* from	legl_actn where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') legl_actn 						

		on	clm_legl_actn.legl_actn_id = legl_actn.legl_actn_id							

	where	legl_actn_type_cd = ''suit''								

	group	by clm_id) a								

	join ( 									

	select	* from	clm_legl_actn where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') b 						

		on	a.clm_id = b.clm_id							

	join ( 									

	select	* from	legl_actn where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') c 						

		on	b.legl_actn_id = c.legl_actn_id 							

		and	c. legl_actn_type_cd = ''suit'' 							

	where	((to_char(lawsuit_open_dt,''mm/dd/yyyy'') <= to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 								

		and	(lawsuit_open_dt is not null) 							

		and	(lawsuit_resolved_dt is null 							

or	to_char(lawsuit_resolved_dt,''mm/dd/yyyy'') > to_date(:MCAS_End_dt,  ''MM/DD/YYYY'' )))  /* eim-18751							 */
	group	by a.clm_id								

	),



MTR_VEH as	

(

SELECT distinct B.CLM_ID, (case when (MOTR_VEH_TYPE_CD IN (''GM'',''MC'',''MH'',''MS'',''PP'',''PU'',''PV'',''MT'',''RK'',''SV'',''SP'',''VN'',''AUTO'',''OTH'',''UNK'') OR MOTR_VEH_TYPE_CD IS NULL ) then ''INC'' else ''EXC'' end) as veh_ind

FROM 

( 



SELECT * FROM CLM WHERE TO_CHAR(EDW_END_DTTM,''MM/DD/YYYY'')=''12/31/9999'')  B 

INNER JOIN  ( SELECT * FROM CLM_EXPSR WHERE TO_CHAR(EDW_END_DTTM,''MM/DD/YYYY'')=''12/31/9999'') C ON B.CLM_ID = C.CLM_ID 

left JOIN  ( SELECT * FROM INSRBL_INT /*WHERE TO_CHAR(EDW_END_DTTM,''MM/DD/YYYY'')=''12/31/9999''*/) E ON ( C.INSRBL_INT_ID = E.INSRBL_INT_ID)

LEFT join ( SELECT * FROM MOTR_VEH WHERE TO_CHAR(EDW_END_DTTM,''MM/DD/YYYY'')=''12/31/9999'')MV on E.PRTY_ASSET_ID=MV.PRTY_ASSET_ID

)



select	 distinct 

	 underwritecompany									

	,riskstate									

	,lob

	,veh_ind

	,prod_name

	,feat_name									

	,clm_num									

	,clm_expsr_id									

	,nk_src_key									

	,clm_expsr_open_dt									

	,origclosedt									

	,closedt									

	,clm_expsr_final_pmnt_dt1									

	,lawsuit_op_beg_count									

	,lawsuit_open_count									

	,lawsuit_closed_count									

	,lawsuit_open_closed_count									

	,''OPEN'' AS STS									

	,									

	case	when (TO_CHAR(clm_expsr_open_dt,''MM/DD/YYYY'')< to_date(:MCAS_Start_dt , ''MM/DD/YYYY'') and	(( origclosedt is null ) or TO_CHAR(origclosedt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')))					

	then 1 									

	else	0 								

	end	as clm_open_begin								

	,									

	case	when ((TO_CHAR(clm_expsr_open_dt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')) and	(TO_CHAR(clm_expsr_open_dt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY''))) 							

	then 1 									

	else	0 								

	end	as clm_opened_during								

	,									

	case	when ((TO_CHAR(clm_expsr_open_dt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) and	(( origclosedt is null ) 	or	TO_CHAR(origclosedt,''MM/DD/YYYY'')>to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')))					

	then 1 									

	else	0 								

	end	as clm_open_end								

	,									

	case	when ((TO_CHAR(origclosedt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')) and	(TO_CHAR(origclosedt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	

		and	(clm_expsr_sum_amt1>0) )					

	 then 1 									

	else	0 								

	end	as clm_cl_wpmt								

	,	

0 as clm_cl_wopmt_deductible/* EIM-40210   */
	,

	case	when ((TO_CHAR(origclosedt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')) and	(TO_CHAR(origclosedt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 

	and	(clm_expsr_sum_amt1<=0 or clm_expsr_sum_amt1 is null )  )					

	 then 1 									

	else	0 								

	end	as clm_cl_wopmt								

	,										

	cast((coalesce((case	when ((TO_CHAR(origclosedt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')) and	(TO_CHAR(origclosedt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) )							

	 then ABS (clm_expsr_open_dt-clm_expsr_final_pmnt_dt1)+1

	else	0 								

	end),0)) as varchar(7))	as days_to_final_pmt									

	,									

	case	when (clm_cl_wpmt=1 AND ((TO_CHAR(origclosedt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')) and	(TO_CHAR(origclosedt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	(clm_expsr_sum_amt1>0) ) 	and	 (abs(clm_expsr_open_dt-clm_expsr_final_pmnt_dt1)+1<=30))			

	 then  1									

	else	0 								

	end	as clm_stldwp_30								

	,									

	case	when (clm_cl_wpmt=1 AND ((TO_CHAR(origclosedt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')) and	(TO_CHAR(origclosedt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) and	(clm_expsr_sum_amt1>0) ) 	and	 (abs(clm_expsr_open_dt-clm_expsr_final_pmnt_dt1)+1 between 31 and 60))				

	 then  1									

	else	0 								

	end	as clm_stldwp_31_60								

	,									

	case	when (clm_cl_wpmt=1 AND ((TO_CHAR(origclosedt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')) and	(TO_CHAR(origclosedt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	(clm_expsr_sum_amt1>0) ) 	and	 (abs(clm_expsr_open_dt-clm_expsr_final_pmnt_dt1)+1 between 61 and 90))			

	 then  1									

	else	0 								

	end	as clm_stldwp_61_90								

	,									

	case	when (clm_cl_wpmt=1 AND ((TO_CHAR(origclosedt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')) and	(TO_CHAR(origclosedt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	(clm_expsr_sum_amt1>0) ) 	and	 (abs(clm_expsr_open_dt-clm_expsr_final_pmnt_dt1)+1 between 91 and 180))			

	 then  1									

	else	0 								

	end	as clm_stldwp_91_180								

	,									

	case	when (clm_cl_wpmt=1 AND ((TO_CHAR(origclosedt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')) 	and	(TO_CHAR(origclosedt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	(clm_expsr_sum_amt1>0) ) 	and	 (abs(clm_expsr_open_dt-clm_expsr_final_pmnt_dt1)+1 between 181 and 365))		

	 then  1									

	else	0 								

	end	as clm_stldwp_181_365								

	,									

	case	when (clm_cl_wpmt=1 AND ((TO_CHAR(origclosedt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')) 	and	(TO_CHAR(origclosedt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	(clm_expsr_sum_amt1>0) ) 	and	 (abs(clm_expsr_open_dt-clm_expsr_final_pmnt_dt1)+1 >=366))		

	 then  1									

	else	0 								

	end	as clm_stldwp_366								

	,									

	case	when (clm_cl_wopmt=1 AND ((TO_CHAR(origclosedt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')) and	(TO_CHAR(origclosedt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	((clm_expsr_sum_amt1<=0) or (clm_expsr_sum_amt1 is null)) and	 (abs(clm_expsr_open_dt-origclosedt)+1<=30)))				

	 then  1									

	else	0 								

	end	as clm_stldwop_30								

	,									

	case	when (clm_cl_wopmt=1 AND ((TO_CHAR(origclosedt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')) and	(TO_CHAR(origclosedt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	((clm_expsr_sum_amt1<=0) or (clm_expsr_sum_amt1 is null)) 	and	 (abs(clm_expsr_open_dt-origclosedt)+1 between 31 and 60)))			

	 then  1									

	else	0 								

	end	as clm_stldwop_31_60								

	,									

	case	when (clm_cl_wopmt=1 and ((TO_CHAR(origclosedt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')) and	(TO_CHAR(origclosedt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	((clm_expsr_sum_amt1<=0) or (clm_expsr_sum_amt1 is null)) 	and	 (abs(clm_expsr_open_dt-origclosedt)+1 between 61 and 90)))			

	 then  1									

	else	0 								

	end	as clm_stldwop_61_90								

	,									

	case	when (clm_cl_wopmt=1 and ((TO_CHAR(origclosedt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')) and	(TO_CHAR(origclosedt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	((clm_expsr_sum_amt1<=0) or (clm_expsr_sum_amt1 is null)) 	and	 (abs(clm_expsr_open_dt-origclosedt)+1 between 91 and 180)))			

	 then  1									

	else	0 								

	end	as clm_stldwop_91_180								

	,									

	case	when (clm_cl_wopmt=1 and ((TO_CHAR(origclosedt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')) and	(TO_CHAR(origclosedt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	((clm_expsr_sum_amt1<=0) or (clm_expsr_sum_amt1 is null)) 	and	 (abs(clm_expsr_open_dt-origclosedt)+1 between 181 and 365)))			

	 then  1									

	else	0 								

	end	as clm_stldwop_181_365								

	,									

	case	when (clm_cl_wopmt=1 and ((TO_CHAR(origclosedt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')) and	(TO_CHAR(origclosedt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	((clm_expsr_sum_amt1<=0) or (clm_expsr_sum_amt1 is null)) 	and	 (abs(clm_expsr_open_dt-origclosedt)+1 >=366)))			

	 then  1									

	else	0 								

	end	as clm_stldwop_366								

										

	from									

	(									

	select	 distinct 								

	 undrwrte.intrnl_org_num as underwritecompany,									

	rskstate.riskstate,									

	lob.lob,									

	upper (feat.feat_name) as feat_name,									

	undrwrte.clm_num,									

	expsr.clm_expsr_id,									

	expsr.nk_src_key

	,veh_ind

	,prod_name

	,coalesce(lwsuit_op_beg.lawsuit_op_beg_count,0) as lawsuit_op_beg_count									

	,coalesce(lwsuit_open.lawsuit_open_count,0) as lawsuit_open_count									

	,coalesce(lwsuit_closed.lawsuit_closed_count, 0) as lawsuit_closed_count									

	,coalesce(lwsuit_open_closed.lawsuit_open_closed_count , 0) as lawsuit_open_closed_count									

	,cast(expsr_open.clm_expsr_open_dt as date) as clm_expsr_open_dt									

	,cast(									

	case	when (expsr_orig_close.clm_expsr_orig_close_dt > expsr_orig_reopen.clm_expsr_orig_reopen_dt  and expsr_orig_reopen.clm_expsr_orig_reopen_dt is not null)								

	                      then   expsr_orig_reopen.clm_expsr_orig_reopen_dt 									

	                when (expsr_orig_close.clm_expsr_orig_close_dt is null   and expsr_orig_reopen.clm_expsr_orig_reopen_dt is not null)									

	                      then   expsr_orig_reopen.clm_expsr_orig_reopen_dt 									

	                      else expsr_orig_close.clm_expsr_orig_close_dt									

	end	  as date)   as origclosedt,								

	clm_cl_wpaymnt.clm_expsr_sum_amt1 as clm_expsr_sum_amt1,									

	clm_stld_wpaymnt.clm_expsr_sum_amt1 as final_pmt_sum_amt1									

	,cast(									

	case	when (expsr_orig_reopen.clm_expsr_orig_reopen_dt is not null)								

	                      then  null 									

	                      else expsr_close.clm_expsr_close_dt   									

	 end as date) closedt									

	,cast((case when ((to_char(origclosedt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''mm/dd/yyyy'')) and	(origclosedt<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')))and (cast(final_pmnt1.clm_expsr_final_pmnt_dt1 as date)<=cast(expsr_orig_close.clm_expsr_orig_close_dt as date)) then final_pmnt1.clm_expsr_final_pmnt_dt1 else null end) as date) as clm_expsr_final_pmnt_dt1 									

	from	( 								

	select	* from	clm_expsr where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'' ) expsr						

	inner join undrwrte on	expsr.clm_id = undrwrte.clm_id 																

	inner join rskstate on	expsr.clm_id = rskstate.clm_id 							  									

	inner join feat on	expsr.cvge_feat_id = feat.feat_id							

	left join lob on	expsr.clm_id = lob.clm_id 	

	left join lwsuit_op_beg on	 expsr.clm_id = lwsuit_op_beg.clm_id 	

	left join lwsuit_open on expsr.clm_id = lwsuit_open.clm_id																		

	left join lwsuit_closed	on	 expsr.clm_id = lwsuit_closed.clm_id							

	left join	lwsuit_open_closed on	 expsr.clm_id = lwsuit_open_closed.clm_id							



	left outer join									

	(									

	select	clm_expsr_id, clm_expsr_sts_strt_dttm as clm_expsr_open_dt  								

	from	( 								

	select	* from	 clm_expsr_sts  where	clm_expsr_sts_cd = ''open'') clm_expsr_sts 

	) expsr_open						

		on	expsr.clm_expsr_id = expsr_open.clm_expsr_id 							

										

	left outer join									

	(									

	select	clm_expsr_id, min(clm_expsr_sts_strt_dttm) as clm_expsr_orig_close_dt 								

	from	clm_expsr_sts 								

	where	clm_expsr_sts_cd <> ''open''								

	group	by clm_expsr_id								

	) expsr_orig_close 									

		on	expsr.clm_expsr_id = expsr_orig_close.clm_expsr_id							

	

	left outer join									

	(									

	select	clm_expsr_id, max(clm_expsr_sts_strt_dttm) as clm_expsr_close_dt 								

	from	( 								

	select	* from	clm_expsr_sts where	clm_expsr_sts_cd = ''closed''  ) clm_expsr_sts						

	group	by clm_expsr_id								

	) expsr_close 									

		on	expsr.clm_expsr_id = expsr_close.clm_expsr_id 							

	

	left outer join									

	(									

	select	clm_expsr_id, min(clm_expsr_sts_strt_dttm) as clm_expsr_orig_reopen_dt 								

	from	( 								

	select	* from	clm_expsr_sts where	clm_expsr_sts_cd = ''reopened'' ) clm_expsr_sts						

	group	by clm_expsr_id								

	) expsr_orig_reopen 									

		on	expsr.clm_expsr_id = expsr_orig_reopen.clm_expsr_id											

										

	left outer join 									

	(									

		

select	exps.clm_expsr_id, sum(clm_expsr_lnitm_amt) as clm_expsr_sum_amt1								

	from									

	(									

	select	a.clm_expsr_lnitm_amt,a. clm_expsr_lnitm_dttm, b.clm_expsr_trans_dttm,								

			b.clm_expsr_id 							

	from	( 								

	select	* from	clm_expsr_trans_lnitm where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') a						

	inner join  ( 									

	select	* from	clm_expsr_trans where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') b 						

		on	a.clm_expsr_trans_id = b.clm_expsr_trans_id	

		inner 	join (select * from clm_expsr_trans_sts where clm_expsr_trans_sts_cd<>''VOIDED'' and to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999''

	) c on b.clm_expsr_trans_id=c.clm_expsr_trans_id	

		where	a.clm_expsr_lnitm_amt > 0 								

		and	b. clm_expsr_trans_sbtype_cd = ''pymnt'' 										

and	a.lnitm_ctgy_type_cd in  (''loss'',''dedrfnd'',''frmrded'',''ded'', ''DMNSHDVAL'') ) lnitm  	/* Added DMNSHDVAL as part of EIM-46309			 */
										

	inner join									

	(									

	select	clm_expsr_id, clm_expsr_sts_strt_dttm as clm_expsr_open_date 								

	from	( 								

	select	* from	clm_expsr_sts  where	clm_expsr_sts_cd = ''open'' 

	)clm_expsr_sts						

	) exps									

		on	lnitm.clm_expsr_id = exps.clm_expsr_id		

		

	left outer join									

	(									

	select	clm_expsr_id, min(clm_expsr_sts_strt_dttm) as clm_expsr_orig_reopen_dt 								

	from	( 								

	select	* from	clm_expsr_sts where	clm_expsr_sts_cd = ''reopened'' ) clm_expsr_sts						

	group	by clm_expsr_id								

	) expsr_orig_reopen 									

		on	lnitm.clm_expsr_id = expsr_orig_reopen.clm_expsr_id	

										

	left outer join									

	(									

	select	clm_expsr_id, min(clm_expsr_sts_strt_dttm) as clm_expsr_orig_close_dt 								

	from	( 								

	select	* from	clm_expsr_sts where	clm_expsr_sts_cd = ''closed'' 

	) clm_expsr_sts						

	group	by clm_expsr_id) exps1								

		on	lnitm.clm_expsr_id = exps1.clm_expsr_id							

	where	

	cast(clm_expsr_lnitm_dttm as date) between cast(exps.clm_expsr_open_date as date) and	

	cast(case	when (exps1.clm_expsr_orig_close_dt > expsr_orig_reopen.clm_expsr_orig_reopen_dt  and expsr_orig_reopen.clm_expsr_orig_reopen_dt is not null)								

	                      then   expsr_orig_reopen.clm_expsr_orig_reopen_dt 									

	                when (exps1.clm_expsr_orig_close_dt is null   and expsr_orig_reopen.clm_expsr_orig_reopen_dt is not null)									

	                      then   expsr_orig_reopen.clm_expsr_orig_reopen_dt 									

	                      else exps1.clm_expsr_orig_close_dt									

	end	  as date)

	group	by exps.clm_expsr_id								

										

	)clm_cl_wpaymnt									

		on	expsr.clm_expsr_id = clm_cl_wpaymnt.clm_expsr_id							

										

	left outer join 									

	(									

	select	exps.clm_expsr_id, sum(clm_expsr_lnitm_amt) as clm_expsr_sum_amt1								

	from									

	(									

	select	a.clm_expsr_lnitm_amt,a. clm_expsr_lnitm_dttm, b.clm_expsr_trans_dttm,								

			b.clm_expsr_id 							

	from	( 								

	select	* from	clm_expsr_trans_lnitm where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') a						

	inner join  ( 									

	select	* from	clm_expsr_trans where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') b 						

		on	a.clm_expsr_trans_id = b.clm_expsr_trans_id	

		inner 	join (select * from clm_expsr_trans_sts where clm_expsr_trans_sts_cd<>''VOIDED'' and to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999''

	) c on b.clm_expsr_trans_id=c.clm_expsr_trans_id	

		

	where	a.clm_expsr_lnitm_amt > 0 								

		and	b. clm_expsr_trans_sbtype_cd = ''pymnt'' 							

		and	a.lnitm_ctgy_type_cd =''loss'' ) lnitm 							

										

	inner join									

	(									

	select	clm_expsr_id, clm_expsr_sts_strt_dttm as clm_expsr_open_date 								

	from	( 								

	select	* from	clm_expsr_sts  where	clm_expsr_sts_cd = ''open'' )clm_expsr_sts						

	) exps									

		on	lnitm.clm_expsr_id = exps.clm_expsr_id							

										

	left outer join									

	(									

	select	clm_expsr_id, min(clm_expsr_sts_strt_dttm) as clm_expsr_orig_close_dt 								

	from	( 								

	select	* from	clm_expsr_sts where	clm_expsr_sts_cd = ''closed'') clm_expsr_sts						

	group	by clm_expsr_id) exps1								

		on	lnitm.clm_expsr_id = exps1.clm_expsr_id							

	where									

		cast(clm_expsr_lnitm_dttm as date) between cast(exps.clm_expsr_open_date as date) and	cast(exps1.clm_expsr_orig_close_dt as date)				

	group	by exps.clm_expsr_id								

										

	)clm_stld_wpaymnt									

		on	expsr.clm_expsr_id = clm_stld_wpaymnt.clm_expsr_id							

										

	left outer join									

	(									

	select	coalesce(exps.clm_expsr_id,expsr_orig_reopen.clm_expsr_id) as clm_expsr_id, max(clm_expsr_lnitm_dttm) as clm_expsr_final_pmnt_dt1								

	from									

	(									

	select	a. clm_expsr_lnitm_dttm, b.clm_expsr_trans_dttm, b.clm_expsr_id 								

	from	( 								

	select	* from	clm_expsr_trans_lnitm where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'')  a						

	inner join ( 									

	select	* from	clm_expsr_trans where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') b 						

		on	a.clm_expsr_trans_id = b.clm_expsr_trans_id		

		inner 	join (select * from clm_expsr_trans_sts where clm_expsr_trans_sts_cd<>''VOIDED'' and to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999''

	) c on b.clm_expsr_trans_id=c.clm_expsr_trans_id							

	where	a.clm_expsr_lnitm_amt > 0 								

		and	b. clm_expsr_trans_sbtype_cd = ''pymnt'' 

and	a.lnitm_ctgy_type_cd in (''loss'',''dedrfnd'',''frmrded'',''ded'', ''DMNSHDVAL'')) lnitm /* eim-18751	--Added DMNSHDVAL as part of EIM-46309						 */
	

	left outer join									

	(									

	select	clm_expsr_id, min(clm_expsr_sts_strt_dttm) as clm_expsr_orig_close_dt 								

	from	(								

	select	* from	clm_expsr_sts where	clm_expsr_sts_cd  = ''closed'') clm_expsr_sts 						

	group	by clm_expsr_id) exps								

		on	lnitm.clm_expsr_id = exps.clm_expsr_id	

		

	left outer join									

	(									

	select	clm_expsr_id, min(clm_expsr_sts_strt_dttm) as clm_expsr_orig_reopen_dt 								

	from	( 								

	select	* from	clm_expsr_sts where	clm_expsr_sts_cd = ''reopened'' ) clm_expsr_sts						

	group	by clm_expsr_id								

	) expsr_orig_reopen 									

		on	lnitm.clm_expsr_id = expsr_orig_reopen.clm_expsr_id			

		

	where	

	cast(lnitm.clm_expsr_lnitm_dttm as date) <= cast(case	when (exps.clm_expsr_orig_close_dt > expsr_orig_reopen.clm_expsr_orig_reopen_dt  and expsr_orig_reopen.clm_expsr_orig_reopen_dt is not null)								

	                      then   expsr_orig_reopen.clm_expsr_orig_reopen_dt 									

	                when (exps.clm_expsr_orig_close_dt is null   and expsr_orig_reopen.clm_expsr_orig_reopen_dt is not null)									

	                      then   expsr_orig_reopen.clm_expsr_orig_reopen_dt 									

	                      else exps.clm_expsr_orig_close_dt									

	end	  as date)  

	group	by coalesce(exps.clm_expsr_id,expsr_orig_reopen.clm_expsr_id)

	)final_pmnt1 	

										

		on	expsr.clm_expsr_id = final_pmnt1.clm_expsr_id 							



left join MTR_VEH ON EXPSR.CLM_ID = MTR_VEH.CLM_ID 

		

	where	

	((expsr_open.clm_expsr_open_dt is not null 								

		and	TO_CHAR(expsr_open.clm_expsr_open_dt,''MM/DD/YYYY'')<= to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')							

		and	   TO_CHAR(expsr_close.clm_expsr_close_dt,''MM/DD/YYYY'') >= to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')					

		or	(expsr_open.clm_expsr_open_dt is not null 							

		and	TO_CHAR(expsr_open.clm_expsr_open_dt,''MM/DD/YYYY'')<= to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')

		and	 expsr_close.clm_expsr_close_dt is null)))	

		

		and (LOB.LOB in (''MH'',''HO'')

or (LOB.LOB = ''PA'' and veh_ind = ''INC''))

and

(upper (FEAT.FEAT_NAME) in ( ''BODILY INJURY'',  ''COLLISION'', ''COMPREHENSIVE'', ''MEDICAL PAYMENTS'', ''PROPERTY DAMAGE'', ''SINGLE LIMITS'', ''UNINSURED MOTORIST - BODILY INJURY'',

  ''UNINSURED MOTORIST - PROPERTY DAMAGE'' , ''MEDICAL PAYMENTS TO OTHERS'', ''PERSONAL LIABILITY'', ''DWELLING'', ''PERSONAL PROPERTY'') 

  or (upper(FEAT.FEAT_NAME) = ''LOSS OF USE'' and (LOB.LOB = ''HO'' or LOB.LOB = ''MH'')))

and PROD_NAME IN (''PPV'',''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'',''MH3'',''MH4'',''MH7'',''MH9'')

				

	  )open_rec																		

	where 							  											

	 	 (							

		clm_open_begin>0 							

		or	clm_opened_during>0 							

		or	clm_open_end >0 							

		or	clm_cl_wpmt>0 							

		or	clm_cl_wopmt>0 							

		)

	

	union

	

			select 	

	  distinct 	  

	 underwritecompany									

	,riskstate									

	,lob

	,veh_ind

	,prod_name

	,feat_name									

	,clm_num									

	,clm_expsr_id									

	,nk_src_key 									

	,clm_expsr_orig_reopen_dt									

	,origclosedt									

	,closedt	

	,clm_expsr_final_pmnt_dt2									

	,lawsuit_op_beg_count									

	,lawsuit_open_count									

	,lawsuit_closed_count									

	,lawsuit_open_closed_count									

	,''REOPEN'' AS STS	

		,case	when (TO_CHAR(clm_expsr_orig_reopen_dt,''MM/DD/YYYY'')< to_date(:MCAS_Start_dt , ''MM/DD/YYYY'') 	and	(( closedt is null ) 	or	TO_CHAR(closedt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')))				

	then 1 									

	else	0 								

	end	as clm_open_begin								

	,									

	case	when ((TO_CHAR(clm_expsr_orig_reopen_dt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')) 	and	(TO_CHAR(clm_expsr_orig_reopen_dt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')))						

	then 1 									

	else	0 								

	end	as clm_opened_during								

	,									

	case	when ((TO_CHAR(clm_expsr_orig_reopen_dt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	(( calc_dt is null ) 	or	TO_CHAR(calc_dt,''MM/DD/YYYY'')>to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')))				

	then 1 									

	else	0 								

	end	as clm_open_end		

	

		,									

	case	when (clm_expsr_orig_reopen_dt is not null) 	and TO_CHAR(calc_dt,''MM/DD/YYYY'') between to_date(:MCAS_Start_dt , ''MM/DD/YYYY'') and to_date(:MCAS_End_dt,  ''MM/DD/YYYY'') 

	and (clm_expsr_final_pmnt_dt2 is not null and (extract(year from clm_expsr_final_pmnt_dt2)=extract(year from (to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')))))	and	(clm_expsr_sum_amt2>0) 		

	 then 1 									

	else	0 								

	end	as clm_cl_wpmt	

	

	,case       when ((clm_expsr_orig_reopen_dt is not null)     and TO_CHAR(calc_dt,''MM/DD/YYYY'') between to_date(:MCAS_Start_dt , ''MM/DD/YYYY'') and to_date(:MCAS_End_dt,  ''MM/DD/YYYY'') 

                and (clm_expsr_final_pmnt_dt2 is not null and (extract(year from clm_expsr_final_pmnt_dt2)=extract(year from (to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')))))          and        (clm_expsr_sum_amt2>0)           

                and clm_sts_rsn_type_cd=''NCLTDEDUCT'' 

				and lob=''PA'')

                then 1                                                                                                                                  

                else        0                                                                                                                             

end        as clm_cl_wopmt_deductible  /* EIM-40210    */
																				

		

	,case	when (clm_expsr_orig_reopen_dt is not null) 	and	((TO_CHAR(calc_dt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'') and	TO_CHAR(calc_dt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) /*or ( origclosedt>clm_expsr_final_pmnt_dt2)*/) 	

	  and (clm_expsr_final_pmnt_dt2 is  null or (extract(year from clm_expsr_final_pmnt_dt2)<extract(year from (to_date(:MCAS_Start_dt , ''MM/DD/YYYY''))))) and (clm_expsr_sum_amt3<=0 or clm_expsr_sum_amt3 is null )	

	 then 1 									

	else	0 								

	end	as clm_cl_wopmt

	

	,											

	cast((coalesce((case	when (clm_expsr_orig_reopen_dt is not null)  	and	 (TO_CHAR(calc_dt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')) and	(TO_CHAR(calc_dt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 					

	 then  ABS(clm_expsr_orig_reopen_dt-clm_expsr_final_pmnt_dt2)+1 								

	else	0 								

	end),0)) as varchar(7))	as days_to_final_pmt										

	,									

	case	when (clm_cl_wpmt=1 and (clm_expsr_orig_reopen_dt is not null) 	and	(clm_expsr_sum_amt2>0)  and	(TO_CHAR(calc_dt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'') 	and	TO_CHAR(calc_dt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	 (abs (clm_expsr_orig_reopen_dt-clm_expsr_final_pmnt_dt2)+1<=30))				

	 then  1									

	else	0 								

	end	as clm_stldwp_30								

	,									

	case	when (clm_cl_wpmt=1 and (clm_expsr_orig_reopen_dt is not null) 	and	(clm_expsr_sum_amt2>0) and	(TO_CHAR(calc_dt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'') 	and	TO_CHAR(calc_dt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY''))  and	 (abs(clm_expsr_orig_reopen_dt-clm_expsr_final_pmnt_dt2)+1 between 31 and 60))					

	 then  1									

	else	0 								

	end	as clm_stldwp_31_60								

	,									

	case	when (clm_cl_wpmt=1 and (clm_expsr_orig_reopen_dt is not null) 	and	(clm_expsr_sum_amt2>0)  and	(TO_CHAR(calc_dt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'') 	and	TO_CHAR(calc_dt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	 (abs(clm_expsr_orig_reopen_dt-clm_expsr_final_pmnt_dt2)+1 between 61 and 90))				

	 then  1									

	else	0 								

	end	as clm_stldwp_61_90								

	,									

	case	when (clm_cl_wpmt=1 and (clm_expsr_orig_reopen_dt is not null) 	and	(clm_expsr_sum_amt2>0)  and	(TO_CHAR(calc_dt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'') 	and	TO_CHAR(calc_dt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	 (abs(clm_expsr_orig_reopen_dt-clm_expsr_final_pmnt_dt2)+1 between 91 and 180))				

	 then  1									

	else	0 								

	end	as clm_stldwp_91_180								

	,									

	case	when (clm_cl_wpmt=1 and (clm_expsr_orig_reopen_dt is not null) 	and	(clm_expsr_sum_amt2>0)  and	(TO_CHAR(calc_dt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'') 	and	TO_CHAR(calc_dt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	 (abs(clm_expsr_orig_reopen_dt-clm_expsr_final_pmnt_dt2)+1 between 181 and 365))				

	 then  1									

	else	0 								

	end	as clm_stldwp_181_365								

	,									

	case	when (clm_cl_wpmt=1 and (clm_expsr_orig_reopen_dt is not null) 	and	(clm_expsr_sum_amt2>0)  and	(TO_CHAR(calc_dt,''MM/DD/YYYY'')>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'') 	and	TO_CHAR(calc_dt,''MM/DD/YYYY'')<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	 (abs(clm_expsr_orig_reopen_dt-clm_expsr_final_pmnt_dt2)+1>=366))				

	 then  1									

	else	0 								

	end	as clm_stldwp_366								

	,									

	case	when (clm_cl_wopmt=1 and ((clm_expsr_orig_reopen_dt is not null) 	and	((clm_expsr_sum_amt3<=0)  or (clm_expsr_sum_amt3 is null)) and	(calc_dt>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'') 	and	calc_dt<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY''))  and	 (abs(clm_expsr_orig_reopen_dt-calc_dt)+1<=30)))					

	 then  1									

	else	0 								

	end	as clm_stldwop_30								

	,									

	case	when (clm_cl_wopmt=1 and ((clm_expsr_orig_reopen_dt is not null) 	and	((clm_expsr_sum_amt3<=0)  or (clm_expsr_sum_amt3 is null)) and	(calc_dt>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'') 	and	calc_dt<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	 (abs(clm_expsr_orig_reopen_dt-calc_dt)+1 between 31 and 60)))				

	 then  1									

	else	0 								

	end	as clm_stldwop_31_60								

	,									

	case	when (clm_cl_wopmt=1 and ((clm_expsr_orig_reopen_dt is not null) 	and	((clm_expsr_sum_amt3<=0) or (clm_expsr_sum_amt3 is null))  and	(calc_dt>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'') 	and	calc_dt<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	 (abs(clm_expsr_orig_reopen_dt-calc_dt)+1 between 61 and 90)))				

	 then  1									

	else	0 								

	end	as clm_stldwop_61_90								

	,									

	case	when (clm_cl_wopmt=1 and (clm_cl_wopmt=1 and ((clm_expsr_orig_reopen_dt is not null) 	and	((clm_expsr_sum_amt3<=0) or (clm_expsr_sum_amt3 is null)) and	(calc_dt>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'') 	and	calc_dt<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY''))  and	 (abs(clm_expsr_orig_reopen_dt-calc_dt)+1 between 91 and 180))))					

	 then  1									

	else	0 								

	end	as clm_stldwop_91_180								

	,									

	case	when (clm_cl_wopmt=1 and (clm_expsr_orig_reopen_dt is not null) and	((clm_expsr_sum_amt3<=0) or (clm_expsr_sum_amt3 is null))  and	(calc_dt>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'') 	and	calc_dt<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	 (abs(clm_expsr_orig_reopen_dt-calc_dt)+1 between 181 and 365))				

	 then  1									

	else	0 								

	end	as clm_stldwop_181_365								

	,									

	case	when (clm_cl_wopmt=1 and ((clm_expsr_orig_reopen_dt is not null) 	and	((clm_expsr_sum_amt3<=0) or (clm_expsr_sum_amt3 is null))  and	(calc_dt>=to_date(:MCAS_Start_dt , ''MM/DD/YYYY'') 	and	calc_dt<=to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')) 	and	 (abs(clm_expsr_orig_reopen_dt-calc_dt)+1 >=366)))				

	  then  1									

	else	0 								

	end	as clm_stldwop_366								

	

	from

	(		

		select 	

	  distinct 

	 underwritecompany									

	,riskstate									

	,lob

	,veh_ind

	,prod_name

	,feat_name									

	,clm_num									

	,clm_expsr_id									

	,nk_src_key 									

	,clm_expsr_orig_reopen_dt									

	,origclosedt									

	,closedt

	,clm_expsr_final_pmnt_dt2									

	,lawsuit_op_beg_count									

	,lawsuit_open_count									

	,lawsuit_closed_count									

	,lawsuit_open_closed_count	

	,case when closedt is not null then closedt 

	when origclosedt>clm_expsr_final_pmnt_dt2 then origclosedt

	when closedt is null and clm_expsr_final_pmnt_dt2 is not null then clm_expsr_final_pmnt_dt2

	end as calc_dt

	,clm_expsr_sum_amt2

	,clm_expsr_sum_amt3

	,clm_sts_rsn_type_cd

									



	from									

	(									

	select	 distinct 								

	 undrwrte.intrnl_org_num as underwritecompany,									

	rskstate.riskstate,									

	lob.lob,

	veh_ind,

	prod_name,

	upper (feat.feat_name) as feat_name,									

	undrwrte.clm_num,									

	expsr.clm_expsr_id,									

	expsr.nk_src_key									

	,coalesce(lwsuit_op_beg.lawsuit_op_beg_count,0) as lawsuit_op_beg_count									

	,coalesce(lwsuit_open.lawsuit_open_count,0) as lawsuit_open_count									

	,coalesce(lwsuit_closed.lawsuit_closed_count, 0) as lawsuit_closed_count									

	,coalesce(lwsuit_open_closed.lawsuit_open_closed_count , 0) as lawsuit_open_closed_count									

	,cast(

	case	 when ((cast(expsr_orig_close.clm_expsr_orig_close_dt as date) is null) and (cast(expsr_close.clm_expsr_close_dt as date)>cast(expsr_orig_reopen.clm_expsr_orig_reopen_dt as date)))

	              			then expsr_close.clm_expsr_close_dt  

	                      else expsr_orig_close.clm_expsr_orig_close_dt 								

	 end as date) as origclosedt								

	,clm_cl_wpaymnt.clm_expsr_sum_amt2 as clm_expsr_sum_amt2

	,clm_cl_wopaymnt.clm_expsr_sum_amt3 as clm_expsr_sum_amt3

/* ,clm_stld_wpaymnt.clm_expsr_sum_amt2 as final_pmt_sum_amt2									 */
	,cast(									

	case	when (cast(expsr_close.clm_expsr_close_dt as date)=cast(expsr_orig_close.clm_expsr_orig_close_dt as date))								

	                      then  expsr_close.clm_expsr_close_dt   									

	              when ((cast(expsr_orig_close.clm_expsr_orig_close_dt as date) is null) and (cast(expsr_close.clm_expsr_close_dt as date)>cast(expsr_orig_reopen.clm_expsr_orig_reopen_dt as date)))

	              			then expsr_close.clm_expsr_close_dt  

	                      else null									

	 end as date) as closedt 

	 ,cast(expsr_orig_reopen.clm_expsr_orig_reopen_dt as date)   as clm_expsr_orig_reopen_dt									

	,

	case when (clm_expsr_orig_reopen_dt is not null) and (cast(final_pmnt2.clm_expsr_final_pmnt_dt2 as date)>=cast(expsr_orig_reopen.clm_expsr_orig_reopen_dt as date) and cast(final_pmnt2.clm_expsr_final_pmnt_dt2 as date)<=cast( expsr_orig_close.clm_expsr_orig_close_dt as date))

	then 

	cast(final_pmnt2.clm_expsr_final_pmnt_dt2 as date) 

	else null end as clm_expsr_final_pmnt_dt2

	,clm_sts_rsn_type_cd

	from	( 								

	select	* from	clm_expsr where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'' ) expsr						

	inner join  undrwrte 									

		on	expsr.clm_id = undrwrte.clm_id 							

	inner join 	rskstate  									

		on	expsr.clm_id = rskstate.clm_id 							

	inner join feat 									

		on	expsr.cvge_feat_id = feat.feat_id							

	left join lob 									

		on	expsr.clm_id = lob.clm_id							

		

	left join lwsuit_op_beg												

		on	 expsr.clm_id = lwsuit_op_beg.clm_id							

										

	left join lwsuit_open 									

		on	 expsr.clm_id = lwsuit_open.clm_id							

										

	left join lwsuit_closed 									

		on	 expsr.clm_id = lwsuit_closed.clm_id							

										

	left join lwsuit_open_closed 									

		on	 expsr.clm_id = lwsuit_open_closed.clm_id							

										

	left outer join									

	(									

	select	clm_expsr_id, (clm_expsr_sts_strt_dttm) as clm_expsr_orig_reopen_dt,ROW_NUMBER() OVER (PARTITION BY CLM_EXPSR_ID ORDER BY CLM_EXPSR_STS_STRT_DTTM ASC) AS RN  								

	from	( 								

	select	* from	clm_expsr_sts where	clm_expsr_sts_cd = ''reopened'' ) clm_expsr_sts									

	) expsr_orig_reopen 									

		on	expsr.clm_expsr_id = expsr_orig_reopen.clm_expsr_id							

										

	left outer join									

	(									

		

	select clm_expsr_id,
    clm_expsr_orig_close_dt,
   (case when mod(sts, 2)=1 then RN+1 else RN end) as RN

	from

	(

	select	c.clm_expsr_id, (clm_expsr_sts_strt_dttm)  as clm_expsr_orig_close_dt ,cnt.sts,	

	ROW_NUMBER() OVER (PARTITION BY c.CLM_EXPSR_ID ORDER BY CLM_EXPSR_STS_STRT_DTTM ASC)-1 AS RN							

	from	clm_expsr_sts c inner join 	(select	clm_expsr_id	,count(clm_expsr_sts_cd	) sts from	clm_expsr_sts 

	Group	by clm_expsr_id) cnt	

	on c.clm_expsr_id=cnt.clm_expsr_id				 

	where	clm_expsr_sts_cd=''closed''					

	Group	by c.clm_expsr_id,clm_expsr_sts_strt_dttm,cnt.sts	

	)otr	

								

	) expsr_orig_close 									

		on	expsr.clm_expsr_id = expsr_orig_close.clm_expsr_id							

		and expsr_orig_reopen.RN=expsr_orig_close.RN		

		and expsr_orig_close.clm_expsr_orig_close_dt>=expsr_orig_reopen.clm_expsr_orig_reopen_dt

										

	left outer join									

	(									

	select	clm_expsr_id, max(clm_expsr_sts_strt_dttm) as clm_expsr_close_dt 								

	from	( 								

	select	* from	clm_expsr_sts where	clm_expsr_sts_cd = ''closed''  ) clm_expsr_sts 					

	group	by clm_expsr_id								

	) expsr_close 									

		on	expsr.clm_expsr_id = expsr_close.clm_expsr_id 							

										

	left outer join 									

	(									

	select	exps.clm_expsr_id, sum(clm_expsr_lnitm_amt) as clm_expsr_sum_amt2								

	from									

	(									

	select	a.clm_expsr_lnitm_amt,a. clm_expsr_lnitm_dttm, b.clm_expsr_trans_dttm,								

			b.clm_expsr_id 							

	from	( 								

	select	* from	clm_expsr_trans_lnitm where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') a						

	inner join  ( 									

	select	*  from	clm_expsr_trans where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') b 						

		on	a.clm_expsr_trans_id = b.clm_expsr_trans_id		

		inner 	join (select * from clm_expsr_trans_sts where clm_expsr_trans_sts_cd<>''VOIDED'' and to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999''

	) c on b.clm_expsr_trans_id=c.clm_expsr_trans_id	

	where	a.clm_expsr_lnitm_amt > 0  and								

			b. clm_expsr_trans_sbtype_cd = ''pymnt'' 							

and	a.lnitm_ctgy_type_cd in  (''loss'',''dedrfnd'',''frmrded'',''ded'', ''DMNSHDVAL'') ) lnitm  /* EIM 18751	--Added DMNSHDVAL as part of EIM-46309			 */
										

	inner join									

	(									

	select	clm_expsr_id, (clm_expsr_sts_strt_dttm) as clm_expsr_reopen_dt  								

	from	( 								

	select	* from	clm_expsr_sts  where	clm_expsr_sts_cd = ''reopened'' )clm_expsr_sts						

	) exps									

		on	lnitm.clm_expsr_id = exps.clm_expsr_id							

										

	left outer join									

	(									

	select	clm_expsr_id, (clm_expsr_sts_strt_dttm) as clm_expsr_close_dt 								

	from	( 								

	select	* from	clm_expsr_sts where	clm_expsr_sts_cd = ''closed'') clm_expsr_sts						

	) exps1									

		on	lnitm.clm_expsr_id = exps1.clm_expsr_id							

	where	

	cast(clm_expsr_lnitm_dttm as date) between cast(exps.clm_expsr_reopen_dt as date) and	cast(exps1.clm_expsr_close_dt as date)										

	group	by exps.clm_expsr_id								

										

	)clm_cl_wpaymnt									

		on	expsr.clm_expsr_id = clm_cl_wpaymnt.clm_expsr_id

		

		left outer join 									

	(									

	select	exps_o.clm_expsr_id, sum(clm_expsr_lnitm_amt) as clm_expsr_sum_amt3								

	from									

	(									

	select	a.clm_expsr_lnitm_amt,a. clm_expsr_lnitm_dttm, b.clm_expsr_trans_dttm,								

			b.clm_expsr_id 							

	from	( 								

	select	* from	clm_expsr_trans_lnitm where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') a						

	inner join  ( 									

	select	*  from	clm_expsr_trans where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') b 						

		on	a.clm_expsr_trans_id = b.clm_expsr_trans_id		

		inner 	join (select * from clm_expsr_trans_sts where clm_expsr_trans_sts_cd<>''VOIDED'' and to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999''

	) c on b.clm_expsr_trans_id=c.clm_expsr_trans_id	

	where	a.clm_expsr_lnitm_amt <= 0  and								

			b. clm_expsr_trans_sbtype_cd = ''pymnt'' 							

and	a.lnitm_ctgy_type_cd in  (''loss'',''dedrfnd'',''frmrded'',''ded'', ''DMNSHDVAL'') ) lnitm_o  /* EIM 18751	--Added DMNSHDVAL as part of EIM-46309			 */
										

	inner join									

	(									

	select	clm_expsr_id, (clm_expsr_sts_strt_dttm) as clm_expsr_reopen_dt  								

	from	( 								

	select	* from	clm_expsr_sts  where	clm_expsr_sts_cd = ''reopened'' )clm_expsr_sts						

	) exps_o									

		on	lnitm_o.clm_expsr_id = exps_o.clm_expsr_id							

										

	left outer join									

	(									

	select	clm_expsr_id, (clm_expsr_sts_strt_dttm) as clm_expsr_close_dt 								

	from	( 								

	select	* from	clm_expsr_sts where	clm_expsr_sts_cd = ''closed'') clm_expsr_sts						

	) exps1_o									

		on	lnitm_o.clm_expsr_id = exps1_o.clm_expsr_id							

	where	

	cast(clm_expsr_lnitm_dttm as date) between cast(exps_o.clm_expsr_reopen_dt as date) and	cast(exps1_o.clm_expsr_close_dt as date)										

	group	by exps_o.clm_expsr_id								

										

	)clm_cl_wopaymnt									

		on	expsr.clm_expsr_id = clm_cl_wopaymnt.clm_expsr_id

										

	left outer join									

	(									

	select clm_expsr_id,max(clm_expsr_final_pmnt_dt2) as clm_expsr_final_pmnt_dt2

	from

	(

	select clm_expsr_id,(clm_expsr_final_pmnt_dt2) as clm_expsr_final_pmnt_dt2

	from

	(

	select	distinct exps.clm_expsr_id, clm_expsr_lnitm_dttm as clm_expsr_final_pmnt_dt2								

	from									

	(									

	select	a. clm_expsr_lnitm_dttm, b.clm_expsr_trans_dttm, b.clm_expsr_id 								

	from	( 								

	select	* from	clm_expsr_trans_lnitm where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'')  a						

	inner join ( 									

	select	* from	clm_expsr_trans where	to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999'') b 						

		on	a.clm_expsr_trans_id = b.clm_expsr_trans_id	

			inner 	join (select * from clm_expsr_trans_sts where clm_expsr_trans_sts_cd<>''VOIDED'' and to_char(edw_end_dttm,''mm/dd/yyyy'')=''12/31/9999''

	) c on b.clm_expsr_trans_id=c.clm_expsr_trans_id							

	where	a.clm_expsr_lnitm_amt > 0 								

		and	b. clm_expsr_trans_sbtype_cd = ''pymnt'' 

and	a.lnitm_ctgy_type_cd in (''loss'',''dedrfnd'',''frmrded'',''ded'', ''DMNSHDVAL'')) lnitm /* eim-18751	--Added DMNSHDVAL as part of EIM-46309						 */
										

	left outer join									

	(									

		

	select clm_expsr_id,clm_expsr_close_dt,(case when (mod(sts,  2)=1) then RN+1 else RN end) as RN

	from

	(

	select	c.clm_expsr_id, (clm_expsr_sts_strt_dttm)  as clm_expsr_close_dt ,cnt.sts,	

	ROW_NUMBER() OVER (PARTITION BY c.CLM_EXPSR_ID ORDER BY CLM_EXPSR_STS_STRT_DTTM ASC)-1 AS RN							

	from	clm_expsr_sts c inner join 	(select	clm_expsr_id	,count(clm_expsr_sts_cd	) sts from	clm_expsr_sts 

	Group	by clm_expsr_id) cnt	

	on c.clm_expsr_id=cnt.clm_expsr_id				 

	where	c.clm_expsr_sts_cd=''closed''	

	Group	by c.clm_expsr_id,clm_expsr_sts_strt_dttm,cnt.sts	

	)otr	

		

	) exps	

		on lnitm.clm_expsr_id = exps.clm_expsr_id			

	

	left outer join									

	(

	select	clm_expsr_id, (clm_expsr_sts_strt_dttm) as clm_expsr_orig_reopen_dt,ROW_NUMBER() OVER (PARTITION BY CLM_EXPSR_ID ORDER BY CLM_EXPSR_STS_STRT_DTTM ASC) AS RN  								

	from	( 								

	select	* from	clm_expsr_sts where	clm_expsr_sts_cd = ''reopened'') clm_expsr_sts	

		) expsr_orig_reopen 	

		on	lnitm.clm_expsr_id = expsr_orig_reopen.clm_expsr_id	

		and expsr_orig_reopen.RN=exps.RN		

		and exps.clm_expsr_close_dt>=expsr_orig_reopen.clm_expsr_orig_reopen_dt

		

	left outer join									

	(									

	select clm_expsr_id, max(clm_expsr_sts_strt_dttm) as clm_expsr_close_dt from ( select * from clm_expsr_sts where clm_expsr_sts_cd = ''closed''

	 ) clm_expsr_sts 																

	group by clm_expsr_id) exps1									

	on lnitm.clm_expsr_id = exps1.clm_expsr_id									

	where 

	  cast(clm_expsr_lnitm_dttm  as date)  <= (case when expsr_orig_reopen.RN=exps.RN then cast(exps1.clm_expsr_close_dt as date) else cast(exps.clm_expsr_close_dt as date) end) 

	 )final_pmnt2_inr 										

	)inr							

	group by clm_expsr_id		

	)final_pmnt2 		

		on	expsr.clm_expsr_id = final_pmnt2.clm_expsr_id 

	 left join (

select * from clm_sts where to_date(clm_sts_strt_dttm ) --, ''mm/dd/yyyy'')
       between to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')

and to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')and clm_sts_rsn_type_CD=''NCLTDEDUCT'')cl_sts on Undrwrte.clm_id=cl_sts.clm_id/* EIM-40210 */
	

left join MTR_VEH ON EXPSR.CLM_ID = MTR_VEH.CLM_ID 	

	where

	((expsr_orig_reopen.clm_expsr_orig_reopen_dt is not null 									

	and to_char(expsr_orig_reopen.clm_expsr_orig_reopen_dt,''MM/DD/YYYY'')<= to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')							

	and    TO_CHAR(expsr_orig_close.clm_expsr_orig_close_dt,''MM/DD/YYYY'') >= to_date(:MCAS_Start_dt , ''MM/DD/YYYY'')

	or (expsr_orig_reopen.clm_expsr_orig_reopen_dt is not null 									

	and TO_CHAR(expsr_orig_reopen.clm_expsr_orig_reopen_dt,''MM/DD/YYYY'')<= to_date(:MCAS_End_dt,  ''MM/DD/YYYY'')								

	and  expsr_orig_close.clm_expsr_orig_close_dt is null))		

	

	and (LOB.LOB in (''MH'',''HO'')

or (LOB.LOB = ''PA'' and veh_ind = ''INC''))

and (upper (FEAT.FEAT_NAME) in ( ''BODILY INJURY'',  ''COLLISION'', ''COMPREHENSIVE'', ''MEDICAL PAYMENTS'', ''PROPERTY DAMAGE'', ''SINGLE LIMITS'', ''UNINSURED MOTORIST - BODILY INJURY'',

  ''UNINSURED MOTORIST - PROPERTY DAMAGE'' , ''MEDICAL PAYMENTS TO OTHERS'', ''PERSONAL LIABILITY'', ''DWELLING'', ''PERSONAL PROPERTY'') 

  or (upper(FEAT.FEAT_NAME) = ''LOSS OF USE'' and (LOB.LOB = ''HO'' or LOB.LOB = ''MH'')))

	and PROD_NAME IN (''PPV'',''HO2'',''HO3'',''HO4'',''HO5'',''HO6'',''HO8'',''MH3'',''MH4'',''MH7'',''MH9'')

		) 							

	  )reopen_inr

	  )reopen_rec	

	 where 						  					

	  	 (							

		clm_open_begin>0 							

		or	clm_opened_during>0 							

		or	clm_open_end >0 							

		or	clm_cl_wpmt>0 							

		or	clm_cl_wopmt>0 							

		)

 order by 7,8,18
) SRC
)
);


-- Component exp_pass_from_source, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_from_source AS
(
SELECT
sq_clm_expsr.CLM_NUM as CLM_NUM,
sq_clm_expsr.CLM_EXPSR_ID as CLM_EXPSR_ID,
sq_clm_expsr.CLM_OPEN_BEGIN as in_out_M_CLAIMS_OPEN_BEGIN,
sq_clm_expsr.CLM_OPEN_DURING as in_out__M_CLAIMS_OPENED_DURING,
sq_clm_expsr.CLM_OPEN_END as in_out_M_CLAIMS_OPEN_END,
sq_clm_expsr.CLM_CLOSED_WPAYMENT as in_out_M_CLAIMS_CLOSED_WITH_PMT,
sq_clm_expsr.CLM_CLOSED_WOPAYMENT_DEDUCTIBLE as Iin_out_M_CLMS_CLOSED_WOPAYMENT_DEDUCTIBLE,
sq_clm_expsr.CLM_CLOSED_WOPAYMENT as in_out_M_CLAIMS_CLOSED_WITHOUT_PMT,
sq_clm_expsr.DAYS_TO_FINAL_PMT as in_out_M_DAYS_TO_FINAL_PMT,
sq_clm_expsr.CLMS_STLD_WP30 as in_out_M_CLAIMS_SETTLED_0_30_DAYS,
sq_clm_expsr.CLMS_STLD_WP31_60 as in_out_M_CLAIMS_SETTLED_31_60_DAYS,
sq_clm_expsr.CLMS_STLD_WP61_90 as in_out_M_CLAIMS_SETTLED_61_90_DAYS,
sq_clm_expsr.CLMS_STLD_WP91_180 as in_out_M_CLAIMS_SETTLED_91_180_DAYS,
sq_clm_expsr.CLMS_STLD_WP181_365 as in_out_M_CLAIMS_SETTLED_181_365_DAYS,
sq_clm_expsr.CLMS_STLD_WP_366 as in_out_M_CLAIMS_SETTLED_BEYOND_366_DAYS,
sq_clm_expsr.CLMS_STLD_WOP_30 as in_out_M_CLMS_STLD_WOPMT_0_30_DAYS,
sq_clm_expsr.CLMS_STLD_WOP_31_60 as in_out_M_CLMS_STLD_WOPMT_31_60_DAYS,
sq_clm_expsr.CLMS_STLD_WOP_61_90 as in_out_M_CLMS_STLD_WOPMT_61_90_DAYS,
sq_clm_expsr.CLMS_STLD_WOP_91_180 as in_out_M_CLMS_STLD_WOPMT_91_180_DAYS,
sq_clm_expsr.CLMS_STLD_WOP_181_365 as in_out_M_CLMS_STLD_WOPMT_181_365_DAYS,
sq_clm_expsr.CLMS_STLD_WOP_366 as in_out_M_CLMS_STLD_WOPMT_BEYOND_366_DAYS,
DECODE ( TRUE , sq_clm_expsr.LAWSUIT_OP_BEG_COUNT IS NULL , 0 , sq_clm_expsr.LAWSUIT_OP_BEG_COUNT ) as v_M_SUITS_OPEN_BEGIN,
DECODE ( TRUE , sq_clm_expsr.LAWSUIT_OPEN_COUNT IS NULL , 0 , sq_clm_expsr.LAWSUIT_OPEN_COUNT ) as v_M_SUITS_OPENED_DURING,
DECODE ( TRUE , sq_clm_expsr.LAWSUIT_CLOSED_COUNT IS NULL , 0 , sq_clm_expsr.LAWSUIT_CLOSED_COUNT ) as v_M_SUITS_CLOSED_DURING,
DECODE ( TRUE , sq_clm_expsr.LAWSUIT_OPEN_CLOSED_COUNT IS NULL , 0 , sq_clm_expsr.LAWSUIT_OPEN_CLOSED_COUNT ) as v_M_SUITS_OPENED_END,
RPAD ( CASE WHEN sq_clm_expsr.UNDERWRITECOMPANY IS NULL THEN '' '' ELSE sq_clm_expsr.UNDERWRITECOMPANY END , 5 , '' '' ) as out_UNDERWRITE_CD,
RPAD ( CASE WHEN sq_clm_expsr.RISKSTATE IS NULL THEN '' '' ELSE sq_clm_expsr.RISKSTATE END , 2 , '' '' ) as out_RISKSTATE,
DECODE ( TRUE , sq_clm_expsr.LOB = ''PA'' AND sq_clm_expsr.FEAT_NAME = ''BODILY INJURY'' , ''C'' , sq_clm_expsr.LOB = ''PA'' AND sq_clm_expsr.FEAT_NAME = ''COLLISION'' , ''A'' , sq_clm_expsr.LOB = ''PA'' AND sq_clm_expsr.FEAT_NAME = ''COMPREHENSIVE'' , ''B'' , sq_clm_expsr.LOB = ''PA'' AND sq_clm_expsr.FEAT_NAME = ''MEDICAL PAYMENTS'' , ''G'' , sq_clm_expsr.LOB = ''PA'' AND sq_clm_expsr.FEAT_NAME = ''PROPERTY DAMAGE'' , ''D'' , sq_clm_expsr.LOB = ''PA'' AND sq_clm_expsr.FEAT_NAME = ''SINGLE LIMITS'' , ''H'' , sq_clm_expsr.LOB = ''PA'' AND sq_clm_expsr.FEAT_NAME = ''UNINSURED MOTORIST - BODILY INJURY'' , ''E'' , sq_clm_expsr.LOB = ''PA'' AND sq_clm_expsr.FEAT_NAME = ''UNINSURED MOTORIST - PROPERTY DAMAGE'' , ''F'' , sq_clm_expsr.LOB = ''HO'' AND sq_clm_expsr.FEAT_NAME = ''MEDICAL PAYMENTS TO OTHERS'' , ''D'' , sq_clm_expsr.LOB = ''HO'' AND sq_clm_expsr.FEAT_NAME = ''PERSONAL LIABILITY'' , ''C'' , sq_clm_expsr.LOB = ''HO'' AND sq_clm_expsr.FEAT_NAME = ''DWELLING'' , ''A'' , sq_clm_expsr.LOB = ''HO'' AND sq_clm_expsr.FEAT_NAME = ''LOSS OF USE'' , ''E'' , sq_clm_expsr.LOB = ''HO'' AND sq_clm_expsr.FEAT_NAME = ''OTHER STRUCTURES'' , ''A'' , sq_clm_expsr.LOB = ''HO'' AND sq_clm_expsr.FEAT_NAME = ''PERSONAL PROPERTY'' , ''B'' , sq_clm_expsr.LOB = ''MH'' AND sq_clm_expsr.FEAT_NAME = ''MEDICAL PAYMENTS TO OTHERS'' , ''D'' , sq_clm_expsr.LOB = ''MH'' AND sq_clm_expsr.FEAT_NAME = ''PERSONAL LIABILITY'' , ''C'' , sq_clm_expsr.LOB = ''MH'' AND sq_clm_expsr.FEAT_NAME = ''DWELLING'' , ''A'' , sq_clm_expsr.LOB = ''MH'' AND sq_clm_expsr.FEAT_NAME = ''LOSS OF USE'' , ''E'' , sq_clm_expsr.LOB = ''MH'' AND sq_clm_expsr.FEAT_NAME = ''OTHER STRUCTURES'' , ''A'' , sq_clm_expsr.LOB = ''MH'' AND sq_clm_expsr.FEAT_NAME = ''PERSONAL PROPERTY'' , ''B'' , '' '' ) as out_M_PRODUCT_ID,
DECODE ( TRUE , sq_clm_expsr.LOB = ''PA'' , ''A'' , sq_clm_expsr.LOB = ''HO'' , ''H'' , sq_clm_expsr.LOB = ''MH'' , ''H'' , '' '' ) as out_M_SCHEDULE_ID,
v_M_SUITS_OPEN_BEGIN as out_M_SUITS_OPEN_BEGIN,
v_M_SUITS_OPENED_DURING as out_M_SUITS_OPENED_DURING,
v_M_SUITS_CLOSED_DURING as out_M_SUITS_CLOSED_DURING,
v_M_SUITS_OPENED_END as out_M_SUITS_OPENED_END,
sq_clm_expsr.source_record_id
FROM
sq_clm_expsr
);


-- Component MCAS_CLAIMS_DETAIL, Type TARGET 
INSERT INTO db_t_prod_comn.MCAS_CLAIMS_DETAIL
(
UNDRWRTG_CMPY,
RISK_STATE,
LOB,
CLM_NUM,
EXPSR_ID,
FEAT_NAME,
LAWSUIT_OP_BEG_COUNT,
LAWSUIT_OPEN_COUNT,
LAWSUIT_CLOSED_COUNT,
LAWSUIT_OPEN_CLOSED_COUNT,
CLM_OPEN_BEGIN,
CLM_OPEN_DURIN,
CLM_OPEN_END,
CLM_CLOSED_WPAYMENT,
CLM_CLOSED_WOPAYMENT_DEDUCTIBLE,
CLM_CLOSED_WOPAYMENT,
DAYS_TO_FINAL_PMT,
CLMS_STLD_WP30,
CLMS_STLD_WP31_60,
CLMS_STLD_WP61_90,
CLMS_STLD_WP91_180,
CLMS_STLD_WP181_365,
CLMS_STLD_WP366,
CLMS_STLD_WOP30,
CLMS_STLD_WOP31_60,
CLMS_STLD_WOP61_90,
CLMS_STLD_WOP91_180,
CLMS_STLD_WOP181_365,
CLMS_STLD_WOP366
)
SELECT
exp_pass_from_source.out_UNDERWRITE_CD as UNDRWRTG_CMPY,
exp_pass_from_source.out_RISKSTATE as RISK_STATE,
exp_pass_from_source.out_M_SCHEDULE_ID as LOB,
exp_pass_from_source.CLM_NUM as CLM_NUM,
exp_pass_from_source.CLM_EXPSR_ID as EXPSR_ID,
exp_pass_from_source.out_M_PRODUCT_ID as FEAT_NAME,
exp_pass_from_source.out_M_SUITS_OPEN_BEGIN as LAWSUIT_OP_BEG_COUNT,
exp_pass_from_source.out_M_SUITS_OPENED_DURING as LAWSUIT_OPEN_COUNT,
exp_pass_from_source.out_M_SUITS_CLOSED_DURING as LAWSUIT_CLOSED_COUNT,
exp_pass_from_source.out_M_SUITS_OPENED_END as LAWSUIT_OPEN_CLOSED_COUNT,
exp_pass_from_source.in_out_M_CLAIMS_OPEN_BEGIN as CLM_OPEN_BEGIN,
exp_pass_from_source.in_out__M_CLAIMS_OPENED_DURING as CLM_OPEN_DURIN,
exp_pass_from_source.in_out_M_CLAIMS_OPEN_END as CLM_OPEN_END,
exp_pass_from_source.in_out_M_CLAIMS_CLOSED_WITH_PMT as CLM_CLOSED_WPAYMENT,
exp_pass_from_source.Iin_out_M_CLMS_CLOSED_WOPAYMENT_DEDUCTIBLE as CLM_CLOSED_WOPAYMENT_DEDUCTIBLE,
exp_pass_from_source.in_out_M_CLAIMS_CLOSED_WITHOUT_PMT as CLM_CLOSED_WOPAYMENT,
exp_pass_from_source.in_out_M_DAYS_TO_FINAL_PMT as DAYS_TO_FINAL_PMT,
exp_pass_from_source.in_out_M_CLAIMS_SETTLED_0_30_DAYS as CLMS_STLD_WP30,
exp_pass_from_source.in_out_M_CLAIMS_SETTLED_31_60_DAYS as CLMS_STLD_WP31_60,
exp_pass_from_source.in_out_M_CLAIMS_SETTLED_61_90_DAYS as CLMS_STLD_WP61_90,
exp_pass_from_source.in_out_M_CLAIMS_SETTLED_91_180_DAYS as CLMS_STLD_WP91_180,
exp_pass_from_source.in_out_M_CLAIMS_SETTLED_181_365_DAYS as CLMS_STLD_WP181_365,
exp_pass_from_source.in_out_M_CLAIMS_SETTLED_BEYOND_366_DAYS as CLMS_STLD_WP366,
exp_pass_from_source.in_out_M_CLMS_STLD_WOPMT_0_30_DAYS as CLMS_STLD_WOP30,
exp_pass_from_source.in_out_M_CLMS_STLD_WOPMT_31_60_DAYS as CLMS_STLD_WOP31_60,
exp_pass_from_source.in_out_M_CLMS_STLD_WOPMT_61_90_DAYS as CLMS_STLD_WOP61_90,
exp_pass_from_source.in_out_M_CLMS_STLD_WOPMT_91_180_DAYS as CLMS_STLD_WOP91_180,
exp_pass_from_source.in_out_M_CLMS_STLD_WOPMT_181_365_DAYS as CLMS_STLD_WOP181_365,
exp_pass_from_source.in_out_M_CLMS_STLD_WOPMT_BEYOND_366_DAYS as CLMS_STLD_WOP366
FROM
exp_pass_from_source;


END; ';