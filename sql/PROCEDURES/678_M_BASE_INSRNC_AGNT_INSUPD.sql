-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_INSRNC_AGNT_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM STRING;
  START_DTTM STRING;
  run_id STRING;
  prcs_id int;


BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);


  -- Component LKP_INDIV_CLM_CTR, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_indiv_clm_ctr AS
  (
         SELECT indiv.indiv_prty_id AS indiv_prty_id,
                indiv.nk_publc_id   AS nk_publc_id
         FROM   db_t_prod_core.indiv
         WHERE  indiv.nk_publc_id IS NOT NULL );
  -- Component sq_insrnc_agnt_x, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_insrnc_agnt_x AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS addressbookuid,
                $2 AS code,
                $3 AS updatetime,
                $4 AS retired,
                $5 AS rnk,
                $6 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT insrnc_agnt_x.addressbookuid_stg,
                                                                  insrnc_agnt_x.code_stg,
                                                                  CASE
                                                                                  WHEN insrnc_agnt_x.updatetime_stg IS NULL THEN to_date(''1900-01-01'',''yyyy-mm-dd'')
                                                                                  ELSE insrnc_agnt_x.updatetime_stg
                                                                  END                                                                       AS updatetime_stg,
                                                                  insrnc_agnt_x.retired_stg                                                 AS retired_stg,
                                                                  rank() over(PARTITION BY addressbookuid_stg ORDER BY updatetime_stg DESC) AS rnk
                                                  FROM            (
                                                                                  SELECT DISTINCT pc_contact.publicid_stg AS addressbookuid_stg ,
                                                                                                  pc_producercode.code_stg ,
                                                                                                  pc_producercode.updatetime_stg,
                                                                                                  CASE
                                                                                                                  WHEN pc_producercode.retired_stg=0
                                                                                                                  AND             pc_contact.retired_stg=0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END AS retired_stg
                                                                                  FROM            db_t_prod_stag.pc_producercode
                                                                                  join            db_t_prod_stag.pc_userproducercode
                                                                                  ON              pc_producercode.id_stg=pc_userproducercode.producercodeid_stg
                                                                                  join            db_t_prod_stag.pc_user
                                                                                  ON              pc_user.id_stg=pc_userproducercode.userid_stg
                                                                                  join            db_t_prod_stag.pc_contact
                                                                                  ON              pc_contact.id_stg=pc_user.contactid_stg
                                                                                  WHERE           pc_producercode.updatetime_stg > cast (:START_DTTM AS timestamp)
                                                                                  AND             pc_producercode.updatetime_stg <= cast(:END_DTTM AS timestamp) ) insrnc_agnt_x
                                                  WHERE           addressbookuid_stg IS NOT NULL qualify row_number() over(PARTITION BY addressbookuid_stg ORDER BY updatetime_stg DESC) = 1 ) src ) );
  -- Component exp_all_sources, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_all_sources AS
  (
            SELECT    sq_insrnc_agnt_x.code AS code,
                      decode ( TRUE ,
                              lkp_1.indiv_prty_id
                              /* replaced lookup LKP_INDIV_CLM_CTR */
                              IS NULL , 9999 ,
                              lkp_2.indiv_prty_id
                              /* replaced lookup LKP_INDIV_CLM_CTR */
                              )                                                              AS var_insrnc_agnt_prty_id,
                      var_insrnc_agnt_prty_id                                                AS out_insrnc_agnt_prty_id,
                      sq_insrnc_agnt_x.code                                                  AS out_insrnc_agnt_cd,
                      :prcs_id                                                               AS prcs_id,
                      current_timestamp                                                      AS edw_strt_dttm,
                      to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                      sq_insrnc_agnt_x.updatetime                                            AS updatetime,
                      sq_insrnc_agnt_x.retired                                               AS retired,
                      --sq_insrnc_agnt_x.rnk                                                   AS rnk,
                      sq_insrnc_agnt_x.source_record_id,
                      row_number() over (PARTITION BY sq_insrnc_agnt_x.source_record_id ORDER BY sq_insrnc_agnt_x.source_record_id) AS rnk
            FROM      sq_insrnc_agnt_x
            left join lkp_indiv_clm_ctr lkp_1
            ON        lkp_1.nk_publc_id = sq_insrnc_agnt_x.addressbookuid
            left join lkp_indiv_clm_ctr lkp_2
            ON        lkp_2.nk_publc_id = sq_insrnc_agnt_x.addressbookuid qualify rnk = 1 );
  -- Component LKP_INSRNC_AGNT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_insrnc_agnt AS
  (
            SELECT    lkp.insrnc_agnt_prty_id,
                      lkp.undrwrtg_authrt_strt_dt,
                      lkp.insrnc_agnt_cd,
                      lkp.edw_strt_dttm,
                      lkp.edw_end_dttm,
                      exp_all_sources.out_insrnc_agnt_prty_id AS in_insrnc_agnt_prty_id,
                      exp_all_sources.source_record_id,
                      row_number() over(PARTITION BY exp_all_sources.source_record_id ORDER BY lkp.insrnc_agnt_prty_id ASC,lkp.undrwrtg_authrt_strt_dt ASC,lkp.insrnc_agnt_cd ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) rnk
            FROM      exp_all_sources
            left join
                      (
                               SELECT   insrnc_agnt.undrwrtg_authrt_strt_dt AS undrwrtg_authrt_strt_dt,
                                        insrnc_agnt.insrnc_agnt_cd          AS insrnc_agnt_cd,
                                        insrnc_agnt.edw_strt_dttm           AS edw_strt_dttm,
                                        insrnc_agnt.edw_end_dttm            AS edw_end_dttm,
                                        insrnc_agnt.insrnc_agnt_prty_id     AS insrnc_agnt_prty_id
                               FROM     db_t_prod_core.insrnc_agnt qualify row_number() over( PARTITION BY insrnc_agnt.insrnc_agnt_prty_id ORDER BY insrnc_agnt.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.insrnc_agnt_prty_id = exp_all_sources.out_insrnc_agnt_prty_id qualify rnk = 1 );
  -- Component exp_compare_data, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_compare_data AS
  (
             SELECT     lkp_insrnc_agnt.insrnc_agnt_prty_id AS lkp_insrnc_agnt_prty_id,
                        lkp_insrnc_agnt.insrnc_agnt_cd      AS lkp_insrnc_agnt_cd,
                        md5 ( ltrim ( rtrim ( lkp_insrnc_agnt.insrnc_agnt_cd ) )
                                   || to_char ( lkp_insrnc_agnt.undrwrtg_authrt_strt_dt ) ) AS chksum_lkp,
                        lkp_insrnc_agnt.edw_strt_dttm                                       AS lkp_edw_strt_dttm,
                        lkp_insrnc_agnt.in_insrnc_agnt_prty_id                              AS in_insrnc_agnt_prty_id,
                        exp_all_sources.prcs_id                                             AS prcs_id,
                        exp_all_sources.edw_strt_dttm                                       AS edw_strt_dttm,
                        exp_all_sources.edw_end_dttm                                        AS edw_end_dttm,
                        exp_all_sources.out_insrnc_agnt_cd                                  AS in_insrnc_agnt_cd,
                        exp_all_sources.updatetime                                          AS updatetime,
                        md5 ( ltrim ( rtrim ( exp_all_sources.out_insrnc_agnt_cd ) )
                                   || to_char ( exp_all_sources.updatetime ) ) AS chksum_inp,
                        CASE
                                   WHEN chksum_lkp IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN chksum_inp != chksum_lkp THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                          AS calc_ins_upd,
                        exp_all_sources.retired      AS retired,
                        lkp_insrnc_agnt.edw_end_dttm AS lkp_edw_end_dttm,
                        exp_all_sources.rnk          AS rnk,
                        exp_all_sources.source_record_id
             FROM       exp_all_sources
             inner join lkp_insrnc_agnt
             ON         exp_all_sources.source_record_id = lkp_insrnc_agnt.source_record_id );
  -- Component rtr_insert_output_flag_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_insert_output_flag_insert as
  SELECT exp_compare_data.lkp_insrnc_agnt_prty_id AS lkp_insrnc_agnt_prty_id,
         exp_compare_data.lkp_edw_strt_dttm       AS lkp_edw_strt_dttm,
         exp_compare_data.in_insrnc_agnt_prty_id  AS in_insrnc_agnt_prty_id,
         exp_compare_data.prcs_id                 AS in_prcs_id,
         exp_compare_data.edw_strt_dttm           AS in_edw_strt_dttm,
         exp_compare_data.edw_end_dttm            AS in_edw_end_dttm,
         exp_compare_data.in_insrnc_agnt_cd       AS in_insrnc_agnt_cd,
         exp_compare_data.calc_ins_upd            AS calc_ins_upd,
         exp_compare_data.updatetime              AS updatetime,
         exp_compare_data.lkp_insrnc_agnt_cd      AS lkp_insrnc_agnt_cd,
         exp_compare_data.retired                 AS retired,
         exp_compare_data.lkp_edw_end_dttm        AS lkp_edw_end_dttm,
         exp_compare_data.rnk                     AS rnk,
         exp_compare_data.source_record_id
  FROM   exp_compare_data
  WHERE  (
                exp_compare_data.calc_ins_upd = ''I'' )
  OR     (
                exp_compare_data.retired = 0
         AND    exp_compare_data.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) )
  OR     (
                exp_compare_data.calc_ins_upd = ''U'' );
  
  -- Component rtr_insert_output_flag_RETIRE, Type ROUTER Output Group RETIRE
  create or replace temporary table rtr_insert_output_flag_retire as
  SELECT exp_compare_data.lkp_insrnc_agnt_prty_id AS lkp_insrnc_agnt_prty_id,
         exp_compare_data.lkp_edw_strt_dttm       AS lkp_edw_strt_dttm,
         exp_compare_data.in_insrnc_agnt_prty_id  AS in_insrnc_agnt_prty_id,
         exp_compare_data.prcs_id                 AS in_prcs_id,
         exp_compare_data.edw_strt_dttm           AS in_edw_strt_dttm,
         exp_compare_data.edw_end_dttm            AS in_edw_end_dttm,
         exp_compare_data.in_insrnc_agnt_cd       AS in_insrnc_agnt_cd,
         exp_compare_data.calc_ins_upd            AS calc_ins_upd,
         exp_compare_data.updatetime              AS updatetime,
         exp_compare_data.lkp_insrnc_agnt_cd      AS lkp_insrnc_agnt_cd,
         exp_compare_data.retired                 AS retired,
         exp_compare_data.lkp_edw_end_dttm        AS lkp_edw_end_dttm,
         exp_compare_data.rnk                     AS rnk,
         exp_compare_data.source_record_id
  FROM   exp_compare_data
  WHERE  exp_compare_data.calc_ins_upd = ''R''
  AND    exp_compare_data.retired != 0
  AND    exp_compare_data.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_tgt_insrnc_agnt_insert, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_tgt_insrnc_agnt_insert AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insert_output_flag_insert.in_insrnc_agnt_prty_id AS in_insrnc_agnt_prty_id1,
                rtr_insert_output_flag_insert.in_prcs_id             AS in_prcs_id1,
                rtr_insert_output_flag_insert.in_edw_strt_dttm       AS in_edw_strt_dttm1,
                rtr_insert_output_flag_insert.in_edw_end_dttm        AS in_edw_end_dttm1,
                rtr_insert_output_flag_insert.in_insrnc_agnt_cd      AS in_insrnc_agnt_cd1,
                rtr_insert_output_flag_insert.updatetime             AS updatetime1,
                rtr_insert_output_flag_insert.retired                AS retired1,
                rtr_insert_output_flag_insert.rnk                    AS rnk,
                0                                                    AS update_strategy_action,
				rtr_insert_output_flag_insert.source_record_id
         FROM   rtr_insert_output_flag_insert );
  -- Component exp_tgt_insrnc_agnt_insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_tgt_insrnc_agnt_insert AS
  (
         SELECT upd_tgt_insrnc_agnt_insert.in_insrnc_agnt_prty_id1                               AS in_insrnc_agnt_prty_id1,
                upd_tgt_insrnc_agnt_insert.in_prcs_id1                                           AS in_prcs_id1,
                dateadd(''second'', ( 2 * ( upd_tgt_insrnc_agnt_insert.rnk - 1 ) ), current_timestamp) AS in_edw_strt_dttm1,
                CASE
                       WHEN upd_tgt_insrnc_agnt_insert.retired1 != 0 THEN current_timestamp
                       ELSE upd_tgt_insrnc_agnt_insert.in_edw_end_dttm1
                END                                           AS o_edw_end_dttm,
                upd_tgt_insrnc_agnt_insert.in_insrnc_agnt_cd1 AS in_insrnc_agnt_cd1,
                upd_tgt_insrnc_agnt_insert.updatetime1        AS updatetime1,
                upd_tgt_insrnc_agnt_insert.source_record_id
         FROM   upd_tgt_insrnc_agnt_insert );
  -- Component upd_tgt_insrnc_agnt_update_retire_reject, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_tgt_insrnc_agnt_update_retire_reject AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insert_output_flag_retire.lkp_insrnc_agnt_prty_id AS lkp_insrnc_agnt_prty_id3,
                rtr_insert_output_flag_retire.lkp_edw_strt_dttm       AS lkp_edw_strt_dttm3,
                rtr_insert_output_flag_retire.lkp_insrnc_agnt_cd      AS lkp_insrnc_agnt_cd3,
                rtr_insert_output_flag_retire.retired                 AS retired3,
                rtr_insert_output_flag_retire.lkp_edw_end_dttm        AS lkp_edw_end_dttm3,
                1                                                     AS update_strategy_action,
				rtr_insert_output_flag_retire.source_record_id
         FROM   rtr_insert_output_flag_retire );
  -- Component tgt_insrnc_agnt_insert, Type TARGET
  INSERT INTO db_t_prod_core.insrnc_agnt
              (
                          insrnc_agnt_prty_id,
                          undrwrtg_authrt_strt_dt,
                          insrnc_agnt_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_tgt_insrnc_agnt_insert.in_insrnc_agnt_prty_id1 AS insrnc_agnt_prty_id,
         exp_tgt_insrnc_agnt_insert.updatetime1             AS undrwrtg_authrt_strt_dt,
         exp_tgt_insrnc_agnt_insert.in_insrnc_agnt_cd1      AS insrnc_agnt_cd,
         exp_tgt_insrnc_agnt_insert.in_prcs_id1             AS prcs_id,
         exp_tgt_insrnc_agnt_insert.in_edw_strt_dttm1       AS edw_strt_dttm,
         exp_tgt_insrnc_agnt_insert.o_edw_end_dttm          AS edw_end_dttm
  FROM   exp_tgt_insrnc_agnt_insert;
  
  -- Component tgt_insrnc_agnt_insert, Type Post SQL
  /*
(
SELECT distinct INSRNC_AGNT_PRTY_ID,UNDRWRTG_AUTHRT_STRT_DT,EDW_STRT_DTTM, INSRNC_AGNT_CD
FROM INSRNC_AGNT
WHERE EDW_END_DTTM=TO_DATE(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'') --and CLM_ID=34767 AND CLM_DT_TYPE_CD=''view''
QUALIFY ROW_NUMBER() OVER(PARTITION BY INSRNC_AGNT_PRTY_ID ORDER BY UNDRWRTG_AUTHRT_STRT_DT DESC) >1
)  A
SET EDW_END_DTTM= A.EDW_STRT_DTTM+ INTERVAL ''1'' SECOND
WHERE  INSRNC_AGNT.INSRNC_AGNT_PRTY_ID=A.INSRNC_AGNT_PRTY_ID
AND INSRNC_AGNT.INSRNC_AGNT_CD=A.INSRNC_AGNT_CD
and INSRNC_AGNT.UNDRWRTG_AUTHRT_STRT_DT=A.UNDRWRTG_AUTHRT_STRT_DT
AND  INSRNC_AGNT.EDW_STRT_DTTM=A.EDW_STRT_DTTM
AND  INSRNC_AGNT.EDW_END_DTTM=TO_DATE(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'');
*/
  UPDATE db_t_prod_core.insrnc_agnt
    SET    edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT insrnc_agnt_prty_id,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY insrnc_agnt_prty_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.insrnc_agnt )a

  WHERE  insrnc_agnt.insrnc_agnt_prty_id=a.insrnc_agnt_prty_id
  AND    insrnc_agnt.edw_strt_dttm=a.edw_strt_dttm
  AND    lead1 IS NOT NULL;
  
  -- Component exp_tgt_insrnc_agnt_update_retire_reject, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_tgt_insrnc_agnt_update_retire_reject AS
  (
         SELECT upd_tgt_insrnc_agnt_update_retire_reject.lkp_insrnc_agnt_prty_id3 AS lkp_insrnc_agnt_prty_id3,
                upd_tgt_insrnc_agnt_update_retire_reject.lkp_edw_strt_dttm3       AS lkp_edw_strt_dttm3,
                current_timestamp                                                 AS edw_end_dttm,
                upd_tgt_insrnc_agnt_update_retire_reject.lkp_insrnc_agnt_cd3      AS lkp_insrnc_agnt_cd3,
                upd_tgt_insrnc_agnt_update_retire_reject.source_record_id
         FROM   upd_tgt_insrnc_agnt_update_retire_reject );
  -- Component tgt_insrnc_agnt_update_retire_reject, Type TARGET
  merge
  INTO         db_t_prod_core.insrnc_agnt
  USING        exp_tgt_insrnc_agnt_update_retire_reject
  ON (
                            insrnc_agnt.insrnc_agnt_prty_id = exp_tgt_insrnc_agnt_update_retire_reject.lkp_insrnc_agnt_prty_id3
               AND          insrnc_agnt.insrnc_agnt_cd = exp_tgt_insrnc_agnt_update_retire_reject.lkp_insrnc_agnt_cd3
               AND          insrnc_agnt.edw_strt_dttm = exp_tgt_insrnc_agnt_update_retire_reject.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    insrnc_agnt_prty_id = exp_tgt_insrnc_agnt_update_retire_reject.lkp_insrnc_agnt_prty_id3,
         insrnc_agnt_cd = exp_tgt_insrnc_agnt_update_retire_reject.lkp_insrnc_agnt_cd3,
         edw_strt_dttm = exp_tgt_insrnc_agnt_update_retire_reject.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_tgt_insrnc_agnt_update_retire_reject.edw_end_dttm;

END;
';