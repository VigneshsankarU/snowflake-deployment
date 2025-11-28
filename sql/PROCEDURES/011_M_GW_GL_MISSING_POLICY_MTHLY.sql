-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_GW_GL_MISSING_POLICY_MTHLY("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
  CAL_END_DT STRING;
  PMWorkflowName STRING;
  PMSessionName STRING;
BEGIN
  SELECT 
    TRY_PARSE_JSON(:param_json):CAL_END_DT::STRING,
    TRY_PARSE_JSON(:param_json):PMWorkflowName::STRING,
    ''s_m_bibase_wr_plcy_clm_list_ins''
  INTO
    CAL_END_DT,
    PMWorkflowName,
    PMSessionName;

-- Component SQ_ADD_BAL_PLCY, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_ADD_BAL_PLCY AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as HOST_AGMT_NUM,
$2 as EV_ACTVY_TYPE_CD,
$3 as PRM,
$4 as MODL_EFF_DTTM,
$5 as MODL_CRTN_DTTM,
$6 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select     host_agmt_num,ev_actvy_type_cd, prm, modl_eff_dttm, modl_crtn_dttm

from   

(

select    

host_agmt_num, e.ev_actvy_type_cd, modl_eff_dttm,  modl_crtn_dttm,

case   when e.ev_actvy_type_cd in (''sbmssn'', ''renewal'',''rewrt'') 

                and amt.fincl_ev_amt_type_cd = ''ttlprem''  then  ev_trans_amt                                             

         when  e.ev_actvy_type_cd in (''reinstate'',''cancltn'') 

                and amt.fincl_ev_amt_type_cd = ''chgpremium'' then ev_trans_amt                                     

          when  e.ev_actvy_type_cd in (''plcychg'') 

                and prty_idntftn_num <> ''su'' 

                and amt.fincl_ev_amt_type_cd = ''chgpremium'' then ev_trans_amt                                                                                                                                        

          when  e.ev_actvy_type_cd in (''plcychg'')                                                                                                

                and prty_idntftn_num = ''su'' 

                and amt.fincl_ev_amt_type_cd = ''chgpremium'' then 0 

         else 0 

end prm

from   DB_T_PROD_CORE.agmt a

join DB_T_PROD_CORE.fincl_ev_amt amt on amt.agmt_id = a.agmt_id

join DB_T_PROD_CORE.fincl_ev ev on ev.ev_id = amt.ev_id

join DB_T_PROD_CORE.ev e on e.ev_id = ev.ev_id

join DB_T_PROD_CORE.agmt_sts sts on sts.agmt_id = a.agmt_id

join DB_T_PROD_CORE.ev_prty prty on prty.ev_id = e.ev_id

join DB_T_PROD_CORE.ev_prty_role rol on rol.ev_prty_role_cd = prty.ev_prty_role_cd

join DB_T_PROD_CORE.prty_idntftn id on id.prty_id = prty.prty_id

where    ev.edw_end_dttm = ''9999-12-31 23:59:59.999999'' 

    and a.edw_end_dttm = ''9999-12-31 23:59:59.999999'' 

    and amt.edw_end_dttm = ''9999-12-31 23:59:59.999999'' 

    and e.edw_end_dttm = ''9999-12-31 23:59:59.999999''

    and sts.edw_end_dttm = ''9999-12-31 23:59:59.999999''                                                                       

    and prty.edw_end_dttm = ''9999-12-31 23:59:59.999999''

    and rol.edw_end_dttm = ''9999-12-31 23:59:59.999999''

    and id.edw_end_dttm = ''9999-12-31 23:59:59.999999''

    and cast(a.modl_eff_dttm as date) <   '':CAL_END_DT''                                                                      

    and cast(a.modl_crtn_dttm as date) <  '':CAL_END_DT''                                                       

    and  agmt_type_cd = ''ppv''

    and fincl_ev_amt_type_cd in (''ttlprem'',''chgpremium'')        

    and id.prty_idntftn_type_cd = ''gwid''                                                                                                                                    

    and a.host_agmt_num in (  

                                                         select    host_agmt_num

                                                         from DB_T_PROD_CORE.agmt a1

                                                         join DB_T_PROD_CORE.fincl_ev_amt amt1 on amt1.agmt_id = a1.agmt_id                      

                                                         join DB_T_PROD_CORE.fincl_ev ev1 on ev1.ev_id = amt1.ev_id

                                                         join DB_T_PROD_CORE.ev e1 on e1.ev_id = ev1.ev_id

                                                         where ev_actvy_type_cd in (''rewrt'', ''sbmssn'') 

                                                         and a1.term_num = a.term_num         

                                                     )                                     

   and ev.gl_mth_num is null

   and ev.gl_yr_num is null

)trans

where    trans.prm <> 0                                                                                                                               





union



select     host_agmt_num,ev_actvy_type_cd, prm, modl_eff_dttm, modl_crtn_dttm

from   

(

select    

host_agmt_num, e.ev_actvy_type_cd, modl_eff_dttm,  modl_crtn_dttm,

case   when e.ev_actvy_type_cd in (''sbmssn'', ''renewal'',''rewrt'') 

                and amt.fincl_ev_amt_type_cd = ''ttlprem''  then  ev_trans_amt                                                                               

         when  e.ev_actvy_type_cd in (''reinstate'',''cancltn'') 

                and amt.fincl_ev_amt_type_cd = ''chgpremium'' then ev_trans_amt                                                                            

          when  e.ev_actvy_type_cd in (''plcychg'') 

                and prty_idntftn_num <> ''su'' 

                and amt.fincl_ev_amt_type_cd = ''chgpremium'' then ev_trans_amt                                                                                                                                                                                      

          when  e.ev_actvy_type_cd in (''plcychg'')                                                                                                                                   

                and prty_idntftn_num = ''su'' 

                and amt.fincl_ev_amt_type_cd = ''chgpremium'' then 0 

         else 0 

end prm

from   DB_T_PROD_CORE.agmt a

join DB_T_PROD_CORE.fincl_ev_amt amt on amt.agmt_id = a.agmt_id

join DB_T_PROD_CORE.fincl_ev ev on ev.ev_id = amt.ev_id

join DB_T_PROD_CORE.ev e on e.ev_id = ev.ev_id

join DB_T_PROD_CORE.agmt_sts sts on sts.agmt_id = a.agmt_id

join DB_T_PROD_CORE.ev_prty prty on prty.ev_id = e.ev_id

join DB_T_PROD_CORE.ev_prty_role rol on rol.ev_prty_role_cd = prty.ev_prty_role_cd

join DB_T_PROD_CORE.prty_idntftn id on id.prty_id = prty.prty_id

where    ev.edw_end_dttm = ''9999-12-31 23:59:59.999999'' 

    and a.edw_end_dttm = ''9999-12-31 23:59:59.999999'' 

    and amt.edw_end_dttm = ''9999-12-31 23:59:59.999999'' 

    and e.edw_end_dttm = ''9999-12-31 23:59:59.999999''

    and sts.edw_end_dttm = ''9999-12-31 23:59:59.999999''                                                                                                      

    and prty.edw_end_dttm = ''9999-12-31 23:59:59.999999''

    and rol.edw_end_dttm = ''9999-12-31 23:59:59.999999''

    and id.edw_end_dttm = ''9999-12-31 23:59:59.999999''

    and cast(a.modl_eff_dttm as date) <  '':CAL_END_DT''                                                                                                              

    and cast(a.modl_crtn_dttm as date) <  '':CAL_END_DT''                                                                                                            

    and  agmt_type_cd = ''ppv''

    and fincl_ev_amt_type_cd in (''ttlprem'',''chgpremium'')                                                                                       

 	and  a.host_agmt_num in (  

                                                        select    host_agmt_num

                                                        from DB_T_PROD_CORE.agmt a1

                                                        join DB_T_PROD_CORE.fincl_ev_amt amt1 on amt1.agmt_id = a1.agmt_id

                                                        join DB_T_PROD_CORE.fincl_ev ev1 on ev1.ev_id = amt1.ev_id

                                                        join DB_T_PROD_CORE.ev e1 on e1.ev_id = ev1.ev_id

                                                        where ev_actvy_type_cd in (''renewal'') 

                                                        and a1.term_num = a.term_num

                                                        and exists (

                                                                                select    host_agmt_num

                                                                                from DB_T_PROD_CORE.agmt a2

                                                                                join DB_T_PROD_CORE.fincl_ev_amt amt2 on amt2.agmt_id = a2.agmt_id                               

                                                                                join DB_T_PROD_CORE.fincl_ev ev2 on ev2.ev_id = amt2.ev_id                                                    

                                                                                join DB_T_PROD_CORE.ev e2 on e2.ev_id = ev2.ev_id

                                                                                join DB_T_PROD_CORE.agmt_sts sts2 on sts2.agmt_id = a2.agmt_id

                                                                                where a2.host_agmt_num = a1.host_agmt_num

                                                                                and a2.term_num = a1.term_num

                                                                                and agmt_sts_cd = ''cnfrmddt'' 

                                                                                and cast(sts2.agmt_sts_strt_dttm as date) <  '':CAL_END_DT'' 

                                                                              )         

                                                        )    

                                                                                                                                                                                                                                                                                        

    and ev.gl_mth_num is null

    and ev.gl_yr_num is null

)trans

where    trans.prm <> 0
) SRC
)
);


-- Component SRTTRANS_PLCY, Type SORTER 
CREATE OR REPLACE TEMPORARY TABLE SRTTRANS_PLCY AS
(
SELECT
SQ_ADD_BAL_PLCY.HOST_AGMT_NUM as HOST_AGMT_NUM,
SQ_ADD_BAL_PLCY.EV_ACTVY_TYPE_CD as EV_ACTVY_TYPE_CD,
SQ_ADD_BAL_PLCY.PRM as PRM,
SQ_ADD_BAL_PLCY.MODL_EFF_DTTM as MODL_EFF_DTTM,
SQ_ADD_BAL_PLCY.MODL_CRTN_DTTM as MODL_CRTN_DTTM,
SQ_ADD_BAL_PLCY.source_record_id
FROM
SQ_ADD_BAL_PLCY
ORDER BY HOST_AGMT_NUM 
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
SRTTRANS_PLCY.HOST_AGMT_NUM as HOST_AGMT_NUM,
SRTTRANS_PLCY.EV_ACTVY_TYPE_CD as EV_ACTVY_TYPE_CD,
SRTTRANS_PLCY.PRM as PRM,
SRTTRANS_PLCY.MODL_EFF_DTTM as MODL_EFF_DTTM,
SRTTRANS_PLCY.MODL_CRTN_DTTM as MODL_CRTN_DTTM,
SRTTRANS_PLCY.source_record_id
FROM
SRTTRANS_PLCY
);


-- Component ADD_BAL_ACCOUNTING_PLCY, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE ADD_BAL_ACCOUNTING_PLCY AS
(
SELECT
EXPTRANS.HOST_AGMT_NUM as HOST_AGMT_NUM,
EXPTRANS.EV_ACTVY_TYPE_CD as EV_ACTVY_TYPE_CD,
EXPTRANS.PRM as PRM,
EXPTRANS.MODL_EFF_DTTM as MODL_EFF_DTTM,
EXPTRANS.MODL_CRTN_DTTM as MODL_CRTN_DTTM
FROM
EXPTRANS
);


-- Component ADD_BAL_ACCOUNTING_PLCY, Type EXPORT_DATA Exporting data
;


END; ';