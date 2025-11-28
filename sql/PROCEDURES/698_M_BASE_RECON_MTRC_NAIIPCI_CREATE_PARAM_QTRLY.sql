-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_RECON_MTRC_NAIIPCI_CREATE_PARAM_QTRLY("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
 DECLARE
  run_id STRING;
  prcs_id int; 
  current_date date;

BEGIN
run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
current_date :=   (SELECT param_value::date FROM control_params where run_id = :run_id and upper(param_name)=''CURRENT_DATE'' order by insert_ts desc limit 1);

  -- Component SQ_GW_CLOSEOUT_CTL, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_gw_closeout_ctl AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                p_accounting_yr,
                p_accounting_mo,
                p_boy AS p_begin_year,
                p_eoy  AS p_end_year,
                p_boq  AS p_begin_qtr,
                p_eoq  AS p_end_qtr,
                c_accounting_yr,
                c_accounting_mo,
                c_boy AS c_begin_year,
                c_eoy AS c_end_year,
                c_boq AS c_begin_qtr,
                c_eoq AS c_end_qtr,
                c_eofq AS c_f_end_qtr,
                source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT 
                                                min(
                                                CASE
                                                       WHEN closeout_type=''P''
                                                       AND    accounting_yr=extract(year, to_date(:CURRENT_DATE))-1
                                                       AND    accounting_mo = 1 THEN beginning_ts
                                                END ) AS p_boy,
                                                max(
                                                CASE
                                                       WHEN closeout_type=''P''
                                                       AND    accounting_yr=extract(year, to_date(:CURRENT_DATE))-1
                                                       AND    accounting_mo = 12 THEN ending_ts
                                                END) AS p_eoy,
                                                min(
                                                CASE
                                                       WHEN closeout_type=''P''
                                                       AND    accounting_yr= (
                                                              CASE
                                                                     WHEN extract(month, to_date(:CURRENT_DATE)) IN (0,1,2,3) THEN extract(year, to_date(:CURRENT_DATE))-1
                                                                     ELSE extract(year, to_date(:CURRENT_DATE))
                                                              END)
                                                       AND    accounting_mo = 1 THEN beginning_ts
                                                END  ) AS p_boq,
                                  max(
                                  CASE
                                           WHEN closeout_type=''P''
                                           AND      accounting_yr=(
                                                    CASE
                                                             WHEN extract(month, to_date(:CURRENT_DATE)) IN (0,1,2,3) THEN extract(year, to_date(:CURRENT_DATE))-1
                                                             ELSE extract(year, to_date(:CURRENT_DATE))
                                                    END)
                                           AND      accounting_mo = (
                                                    CASE
                                                             WHEN extract(month, to_date(:CURRENT_DATE)) IN (0,1,2,3) THEN 12
                                                             WHEN extract(month, to_date(:CURRENT_DATE)) IN (4,5,6) THEN 3
                                                             WHEN extract(month, to_date(:CURRENT_DATE)) IN (7,8,9) THEN 6
                                                             WHEN extract(month, to_date(:CURRENT_DATE)) IN (10,11,12) THEN 9
                                                    END) THEN ending_ts
                                  END)                   AS p_eoq,
                                  
                                  min(
                                  CASE
                                           WHEN closeout_type=''C''
                                           AND      accounting_yr=extract(year, to_date(:CURRENT_DATE))-2
                                           AND      accounting_mo = 12 THEN ending_ts
                                  END) + interval ''1  second''  AS c_boy,
                                  max(
                                  CASE
                                           WHEN closeout_type=''C''
                                           AND      accounting_yr=extract(year, to_date(:CURRENT_DATE))-1
                                           AND      accounting_mo = 12 THEN ending_ts
                                  END) AS c_eoy,
                                  
                                  min(
                                  CASE
                                           WHEN closeout_type=''C''
                                           AND      accounting_yr=
                                                    CASE
                                                             WHEN extract(month, to_date(:CURRENT_DATE)) IN (0,1,2,3) THEN extract(year, to_date(:CURRENT_DATE))-2
                                                             ELSE extract(year, to_date(:CURRENT_DATE))-1
                                                    END
                                           AND      accounting_mo = 12 THEN ending_ts
                                  END) + interval ''1  second''  AS c_boq,
                                  max(
                                  CASE
                                           WHEN closeout_type=''C''
                                           AND      accounting_yr=
                                                    CASE
                                                             WHEN extract(month, to_date(:CURRENT_DATE)) IN (0,1,2,3) THEN extract(year, to_date(:CURRENT_DATE))-1
                                                             ELSE extract(year, to_date(:CURRENT_DATE))
                                                    END
                                           AND      accounting_mo = (
                                                    CASE
                                                             WHEN extract(month, to_date(:CURRENT_DATE)) IN (0,1,2,3) THEN 12
                                                             WHEN extract(month, to_date(:CURRENT_DATE)) IN (4,5,6) THEN 3
                                                             WHEN extract(month, to_date(:CURRENT_DATE)) IN (7,8,9) THEN 6
                                                             WHEN extract(month, to_date(:CURRENT_DATE)) IN (10,11,12) THEN 9
                                                    END) THEN ending_ts
                                  END) AS c_eoq,
                                  max(
                                  CASE
                                           WHEN closeout_type=''C''
                                           AND      accounting_yr=
                                                    CASE
                                                             WHEN extract(month, to_date(:CURRENT_DATE)) IN (0,1,2,3) THEN extract(year, to_date(:CURRENT_DATE))-1
                                                             ELSE extract(year, to_date(:CURRENT_DATE))
                                                    END
                                           AND      accounting_mo = (3) THEN ending_ts
                                  END) AS c_eofq,
                                  extract (year, p_eoq)  AS p_accounting_yr,
                                   extract (month, p_eoq) AS p_accounting_mo,
                                   extract (year, c_eoq)  AS c_accounting_yr,
                                  extract (month, c_eoq) AS c_accounting_mo
                         FROM     db_t_prod_comn.gw_closeout_ctl
                         WHERE    closeout_type IN (''P'',
                                                    ''C'')
                         AND      accounting_yr IN (extract(year, to_date(:CURRENT_DATE)),
                                                    extract(year, to_date(:CURRENT_DATE))-2,
                                                    extract(year, to_date(:CURRENT_DATE))-1) ) src ) );
-- Component EXP_PARM_DATE, Type EXPRESSION
CREATE
OR
replace TEMPORARY TABLE exp_parm_date AS
(
       SELECT ''[global]''                   AS global,
              ''$PMMergeSessParamFile=TRUE'' AS sesion,
              ''$P_BEGIN_YR=''
                     || chr ( 39 )
                     || to_char ( sq_gw_closeout_ctl.p_begin_year , ''YYYY-MM-DD'' )
                     || chr ( 39 ) AS o_p_begin_year,
              ''$P_END_YR=''
                     || chr ( 39 )
                     || to_char ( sq_gw_closeout_ctl.p_end_year , ''YYYY-MM-DD'' )
                     || chr ( 39 ) AS o_p_end_year,
              ''$P_BOQ=''
                     || chr ( 39 )
                     || to_char ( sq_gw_closeout_ctl.p_begin_qtr , ''YYYY-MM-DD HH24:MI:SS.US'' )
                     || chr ( 39 ) AS o_p_begin_qtr,
              ''$P_EOQ=''
                     || chr ( 39 )
                     || to_char ( sq_gw_closeout_ctl.p_end_qtr , ''YYYY-MM-DD HH24:MI:SS.US'' )
                     || chr ( 39 ) AS o_p_end_qtr,
              ''$P_YEAR=''
                     || chr ( 39 )
                     || to_char ( to_number(sq_gw_closeout_ctl.p_accounting_yr) )
                     || chr ( 39 ) AS o_p_accounting_yr,
              ''$P_MONTH=''
                     || chr ( 39 )
                     || to_char ( to_number(sq_gw_closeout_ctl.p_accounting_mo) )
                     || chr ( 39 ) AS o_p_accounting_mo,
              ''$C_BEGIN_YR=''
                     || chr ( 39 )
                     || to_char ( sq_gw_closeout_ctl.c_begin_year , ''YYYY-MM-DD'' )
                     || chr ( 39 ) AS o_c_begin_year,
              ''$C_END_YR=''
                     || chr ( 39 )
                     || to_char ( sq_gw_closeout_ctl.c_end_year , ''YYYY-MM-DD'' )
                     || chr ( 39 ) AS o_c_end_year,
              ''$C_BOQ=''
                     || chr ( 39 )
                     || to_char ( sq_gw_closeout_ctl.c_begin_qtr , ''YYYY-MM-DD HH24:MI:SS.US'' )
                     || chr ( 39 ) AS o_c_begin_qtr,
              ''$C_EOQ=''
                     || chr ( 39 )
                     || to_char ( sq_gw_closeout_ctl.c_end_qtr , ''YYYY-MM-DD HH24:MI:SS.US'' )
                     || chr ( 39 ) AS o_c_end_qtr,
              ''$C_fEOQ=''
                     || chr ( 39 )
                     || to_char ( sq_gw_closeout_ctl.c_f_end_qtr , ''YYYY-MM-DD HH24:MI:SS.US'' )
                     || chr ( 39 ) AS o_c_f_end_qtr,
              ''$C_YEAR=''
                     || chr ( 39 )
                     || to_char ( to_number(sq_gw_closeout_ctl.c_accounting_yr) )
                     || chr ( 39 ) AS o_c_accounting_yr,
              ''$C_MONTH=''
                     || chr ( 39 )
                     || to_char ( to_number(sq_gw_closeout_ctl.c_accounting_mo) )
                     || chr ( 39 ) AS o_c_accounting_mo,
              sq_gw_closeout_ctl.source_record_id
       FROM   sq_gw_closeout_ctl );
-- Component NRM_SRC_PARAM_VALUE, Type NORMALIZER
CREATE
OR
replace TEMPORARY TABLE nrm_src_param_value AS
SELECT *
FROM   (
              SELECT exp_parm_date."GLOBAL"          AS parameter_in1,
                     exp_parm_date.sesion            AS parameter_in2,
                     exp_parm_date.o_p_begin_year    AS parameter_in3,
                     exp_parm_date.o_p_end_year      AS parameter_in4,
                     exp_parm_date.o_p_begin_qtr     AS parameter_in5,
                     exp_parm_date.o_p_end_qtr       AS parameter_in6,
                     exp_parm_date.o_p_accounting_yr AS parameter_in7,
                     exp_parm_date.o_p_accounting_mo AS parameter_in8,
                     exp_parm_date.o_c_begin_year    AS parameter_in9,
                     exp_parm_date.o_c_end_year      AS parameter_in10,
                     exp_parm_date.o_c_begin_qtr     AS parameter_in11,
                     exp_parm_date.o_c_end_qtr       AS parameter_in12,
                     exp_parm_date.o_c_accounting_yr AS parameter_in13,
                     exp_parm_date.o_c_accounting_mo AS parameter_in14,
                     exp_parm_date.o_c_f_end_qtr     AS parameter_in15,
                     exp_parm_date.source_record_id
              FROM   exp_parm_date ) src unpivot ( parameter FOR rec_no IN ( parameter_in1,
                                                                            parameter_in2,
                                                                            parameter_in3,
                                                                            parameter_in4,
                                                                            parameter_in5,
                                                                            parameter_in6,
                                                                            parameter_in7,
                                                                            parameter_in8,
                                                                            parameter_in9,
                                                                            parameter_in10,
                                                                            parameter_in11,
                                                                            parameter_in12,
                                                                            parameter_in13,
                                                                            parameter_in14,
                                                                            parameter_in15 ) );

-- Component NAIIPCI_PARAM_FILE, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE
OR
replace TEMPORARY TABLE naiipci_param_file AS
(
       SELECT nrm_src_param_value.parameter AS output
       FROM   nrm_src_param_value );

-- Component NAIIPCI_PARAM_FILE, Type EXPORT_DATA Exporting data
copy into @edw_stage/Parameter/edw_base/naiipci_param_file.txt
from 
(select output
from naiipci_param_file)
file_format = ''CSV_FORMAT''
OVERWRITE = TRUE;

END;
';