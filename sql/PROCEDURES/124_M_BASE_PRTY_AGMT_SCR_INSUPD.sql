-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_AGMT_SCR_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component LKP_CREDIT_SCR_LKUP, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_credit_scr_lkup AS
  (
         SELECT credit_scr_lkup.level_cd        AS level_cd,
                credit_scr_lkup.st_cd           AS st_cd,
                credit_scr_lkup.credit_scr_lim1 AS credit_scr_lim1,
                credit_scr_lkup.credit_scr_lim2 AS credit_scr_lim2
         FROM   db_t_shrd_prod.credit_scr_lkup
         WHERE  credit_scr_lkup.exp_dt =''9999-12-31'' );
  -- Component SQ_pc_modl_run_x, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_modl_run_x AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS modelname,
                $2 AS modelrundttm,
                $3 AS score,
                $4 AS partyagreement,
                $5 AS state_cd,
                $6 AS updatetime,
                $7 AS publicid_stg,
                $8 AS rank,
                $9 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT pc_modl_run_x.model_name_stg                                                                                                                                       AS modelname,
                                                                  pc_modl_run_x.model_run_dttm_stg                                                                                                                                   AS modelrundttm,
                                                                  pc_modl_run_x.score_stg                                                                                                                                            AS score,
                                                                  pc_modl_run_x.party_agreement_stg                                                                                                                                  AS partyagreement,
                                                                  pc_modl_run_x.state_stg                                                                                                                                            AS state_cd,
                                                                  pc_modl_run_x.updatetime_stg                                                                                                                                       AS updatetime,
                                                                  pc_modl_run_x.publicid_stg                                                                                                                                         AS publicid,
                                                                  rank() over (PARTITION BY party_agreement_stg, publicid_stg, model_name_stg, model_run_dttm_stg ORDER BY pc_modl_run_x.updatetime_stg ,pc_modl_run_x.id_stg DESC )    rk
                                                  FROM            (
                                                                                  SELECT DISTINCT ''LEXIS NEXIS''                                                                                               AS model_name_stg ,
                                                                                                  coalesce(cast(pcx_insurancereport_alfa.insurancescoredate_stg AS VARCHAR(30)),''1900-01-01 00:00:00.000000'') AS model_run_dttm_stg ,
                                                                                                  coalesce(cast(pcx_insurancereport_alfa.insurancescore_stg AS     DECIMAL(19,2)),0.00)                       AS score_stg ,
                                                                                                  pc_contact.addressbookuid_stg                                                                                  party_agreement_stg ,
                                                                                                  pctl_jurisdiction.typecode_stg                                                                              AS state_stg ,
                                                                                                  pctl_policycontactrole.typecode_stg                                                                         AS prty_role_cd ,
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  pc_policyperiod.updatetime_stg>pcx_insurancereport_alfa.updatetime_stg) THEN pc_policyperiod.updatetime_stg
                                                                                                                  ELSE pcx_insurancereport_alfa.updatetime_stg
                                                                                                  END updatetime_stg ,
                                                                                                  pc_policyperiod.publicid_stg,
                                                                                                  pcx_insurancereport_alfa.id_stg AS id_stg
                                                                                  FROM            db_t_prod_stag.pc_job pc_job
                                                                                  left join       db_t_prod_stag.pc_policyperiod pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                                                  inner join      db_t_prod_stag.pc_policy
                                                                                  ON              pc_policy.id_stg=pc_policyperiod.policyid_stg
                                                                                  left outer join db_t_prod_stag.pctl_policyperiodstatus pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                                                  join            db_t_prod_stag.pcx_insurancereport_alfa pcx_insurancereport_alfa
                                                                                  ON              pc_policyperiod.id_stg=pcx_insurancereport_alfa.branchid_stg
                                                                                  join            db_t_prod_stag.pctl_jurisdiction pctl_jurisdiction
                                                                                  ON              pc_policyperiod.basestate_stg=pctl_jurisdiction.id_stg
                                                                                  join            db_t_prod_stag.pc_policycontactrole pc_policycontactrole
                                                                                  ON              pc_policycontactrole.id_stg=pcx_insurancereport_alfa.policycontactroleid_stg
                                                                                  join            db_t_prod_stag.pc_contact pc_contact
                                                                                  ON              pc_contact.id_stg=pc_policycontactrole.contactdenorm_stg
                                                                                  join            db_t_prod_stag.pctl_policycontactrole pctl_policycontactrole
                                                                                  ON              pctl_policycontactrole.id_stg=pc_policycontactrole.subtype_stg
                                                                                  join            db_t_prod_stag.pctl_contact pctl_contact
                                                                                  ON              pctl_contact.id_stg=pc_contact.subtype_stg
                                                                                  join            db_t_prod_stag.pctl_job pctl_job
                                                                                  ON              pctl_job.id_stg=pc_job.subtype_stg
                                                                                  WHERE           pc_contact.addressbookuid_stg IS NOT NULL
                                                                                  AND             insurancescoredate_stg IS NOT NULL
                                                                                  AND             insurancescore_stg IS NOT NULL
                                                                                  AND             pctl_contact.name_stg IN (''Person'',
                                                                                                                            ''Adjudicator'',
                                                                                                                            ''User Contact'',
                                                                                                                            ''Vendor (Person)'',
                                                                                                                            ''Attorney'',
                                                                                                                            ''Doctor'',
                                                                                                                            ''Policy Person'',
                                                                                                                            ''Contact'',
                                                                                                                            ''Lodging (Person)'')
                                                                                  AND             pctl_policyperiodstatus.typecode_stg<>''Temporary''
                                                                                  AND             pctl_job.typecode_stg IN (''Submission'',
                                                                                                                            ''Renewal'',
                                                                                                                            ''PolicyChange'')
                                                                                  AND            ((
                                                                                                                                  pcx_insurancereport_alfa.updatetime_stg> cast($start_dttm AS timestamp)
                                                                                                                  AND             pcx_insurancereport_alfa.updatetime_stg <= cast($end_dttm AS timestamp))
                                                                                                  OR              (
                                                                                                                                  pc_policyperiod.updatetime_stg> cast($start_dttm AS timestamp)
                                                                                                                  AND             pc_policyperiod.updatetime_stg <= cast($end_dttm AS timestamp)))
                                                                                  AND             pctl_policyperiodstatus.typecode_stg=''Bound'' ) AS pc_modl_run_x
                                                  WHERE           pc_modl_run_x.model_name_stg=''LEXIS NEXIS'' qualify row_number() over( PARTITION BY party_agreement_stg, publicid_stg, model_name_stg, model_run_dttm_stg ORDER BY pc_modl_run_x.updatetime_stg ,pc_modl_run_x.id_stg DESC)=1 ) src ) );
  -- Component exp_pass_from_src, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_src AS
  (
            SELECT    sq_pc_modl_run_x.modelname        AS modelname,
                      sq_pc_modl_run_x.modelrundttm     AS modelrundttm,
                      to_number(sq_pc_modl_run_x.score) AS var_score,
                      sq_pc_modl_run_x.partyagreement   AS partyagreement,
                      CASE
                                WHEN sq_pc_modl_run_x.score IS NULL THEN ''0.00''
                                ELSE sq_pc_modl_run_x.score
                      END AS out_score,
                      lkp_1.level_cd
                      /* replaced lookup LKP_CREDIT_SCR_LKUP */
                                                                        AS lvl_cd,
                      sq_pc_modl_run_x.updatetime                       AS updatetime,
                      rtrim ( ltrim ( sq_pc_modl_run_x.publicid_stg ) ) AS o_publicid_stg1,
                      sq_pc_modl_run_x.rank                             AS rank,
                      sq_pc_modl_run_x.source_record_id,
                      row_number() over (PARTITION BY sq_pc_modl_run_x.source_record_id ORDER BY sq_pc_modl_run_x.source_record_id) AS rnk
            FROM      sq_pc_modl_run_x
            left join lkp_credit_scr_lkup lkp_1
            ON        lkp_1.st_cd = sq_pc_modl_run_x.state_cd
            AND       lkp_1.credit_scr_lim1 <= var_score
            AND       lkp_1.credit_scr_lim2 >= var_score qualify rnk = 1 );
  -- Component LKP_AGMT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt AS
  (
            SELECT    lkp.agmt_id,
                      exp_pass_from_src.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_src.source_record_id ORDER BY lkp.agmt_id ASC,lkp.nk_src_key ASC) rnk
            FROM      exp_pass_from_src
            left join
                      (
                             SELECT agmt.agmt_id    AS agmt_id,
                                    agmt.nk_src_key AS nk_src_key
                             FROM   db_t_prod_core.agmt
                             WHERE  agmt.agmt_type_cd=''PPV'' ) lkp
            ON        lkp.nk_src_key = exp_pass_from_src.o_publicid_stg1 qualify rnk = 1 );
  -- Component LKP_INDIV_CNT_MGR, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_indiv_cnt_mgr AS
  (
            SELECT    lkp.indiv_prty_id,
                      exp_pass_from_src.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_src.source_record_id ORDER BY lkp.indiv_prty_id DESC,lkp.nk_link_id DESC) rnk
            FROM      exp_pass_from_src
            left join
                      (
                             SELECT indiv.indiv_prty_id AS indiv_prty_id,
                                    indiv.nk_link_id    AS nk_link_id
                             FROM   db_t_prod_core.indiv
                             WHERE  indiv.nk_publc_id IS NULL ) lkp
            ON        lkp.nk_link_id = exp_pass_from_src.partyagreement qualify rnk = 1 );
  -- Component LKP_ANLTCL_MODL, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_anltcl_modl AS
  (
            SELECT    lkp.modl_id,
                      exp_pass_from_src.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_src.source_record_id ORDER BY lkp.modl_id ASC,lkp.modl_name ASC) rnk
            FROM      exp_pass_from_src
            left join
                      (
                               SELECT   anltcl_modl.modl_id   AS modl_id,
                                        anltcl_modl.modl_name AS modl_name
                               FROM     db_t_prod_core.anltcl_modl
                               ORDER BY modl_from_dttm DESC
                                        /*  */
                      ) lkp
            ON        lkp.modl_name = exp_pass_from_src.modelname qualify rnk = 1 );
  -- Component LKP_MODL_RUN, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_modl_run AS
  (
             SELECT     lkp.modl_id,
                        lkp.modl_run_id,
                        exp_pass_from_src.source_record_id,
                        row_number() over(PARTITION BY exp_pass_from_src.source_record_id ORDER BY lkp.modl_id ASC,lkp.modl_run_id ASC,lkp.modl_run_dttm ASC) rnk
             FROM       exp_pass_from_src
             inner join lkp_anltcl_modl
             ON         exp_pass_from_src.source_record_id = lkp_anltcl_modl.source_record_id
             left join
                        (
                               SELECT modl_id,
                                      modl_run_id,
                                      modl_run_dttm
                               FROM   db_t_prod_core.modl_run ) lkp
             ON         lkp.modl_id = lkp_anltcl_modl.modl_id
             AND        lkp.modl_run_dttm = exp_pass_from_src.modelrundttm 
             qualify row_number() over(PARTITION BY exp_pass_from_src.source_record_id ORDER BY lkp.modl_id ASC,lkp.modl_run_id ASC,lkp.modl_run_dttm ASC) 
             = 1 );
  -- Component LKP_PRTY_AGMT_SCR, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_prty_agmt_scr AS
  (
             SELECT     lkp.prty_id,
                        lkp.agmt_id,
                        lkp.modl_id,
                        lkp.modl_run_id,
                        lkp.prty_agmt_scr_val,
                        lkp.lvl_num,
                        lkp.edw_strt_dttm,
                        lkp_agmt.source_record_id,
                        row_number() over(PARTITION BY lkp_agmt.source_record_id ORDER BY lkp.prty_id ASC,lkp.agmt_id ASC,lkp.modl_id ASC,lkp.modl_run_id ASC,lkp.prty_agmt_scr_val ASC,lkp.lvl_num ASC,lkp.edw_strt_dttm ASC) rnk
             FROM       lkp_agmt
             inner join lkp_indiv_cnt_mgr
             ON         lkp_agmt.source_record_id = lkp_indiv_cnt_mgr.source_record_id
             inner join lkp_anltcl_modl
             ON         lkp_indiv_cnt_mgr.source_record_id = lkp_anltcl_modl.source_record_id
             inner join lkp_modl_run
             ON         lkp_anltcl_modl.source_record_id = lkp_modl_run.source_record_id
             left join
                        (
                                 SELECT   prty_agmt_scr.agmt_id           AS agmt_id,
                                          prty_agmt_scr.prty_agmt_scr_val AS prty_agmt_scr_val,
                                          prty_agmt_scr.lvl_num           AS lvl_num,
                                          prty_agmt_scr.edw_strt_dttm     AS edw_strt_dttm,
                                          prty_agmt_scr.prty_id           AS prty_id,
                                          prty_agmt_scr.modl_id           AS modl_id,
                                          prty_agmt_scr.modl_run_id       AS modl_run_id
                                 FROM     db_t_prod_core.prty_agmt_scr qualify row_number() over( PARTITION BY agmt_id,prty_id,modl_run_id, modl_id ORDER BY edw_end_dttm DESC) = 1 ) lkp
             ON         lkp.prty_id = lkp_indiv_cnt_mgr.indiv_prty_id
             AND        lkp.agmt_id = lkp_agmt.agmt_id
             AND        lkp.modl_id = lkp_anltcl_modl.modl_id
             AND        lkp.modl_run_id = lkp_modl_run.modl_run_id 
             qualify row_number() over(PARTITION BY lkp_agmt.source_record_id ORDER BY lkp.prty_id ASC,lkp.agmt_id ASC,lkp.modl_id ASC,lkp.modl_run_id ASC,lkp.prty_agmt_scr_val ASC,lkp.lvl_num ASC,lkp.edw_strt_dttm ASC) 
             = 1 );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
             SELECT     lkp_prty_agmt_scr.modl_id       AS lkp_modl_id,
                        lkp_prty_agmt_scr.modl_run_id   AS lkp_modl_run_id,
                        lkp_prty_agmt_scr.prty_id       AS lkp_prty_id,
                        lkp_prty_agmt_scr.edw_strt_dttm AS lkp_edw_strt_dttm,
                        lkp_anltcl_modl.modl_id         AS in_modl_id,
                        lkp_modl_run.modl_run_id        AS in_modl_run_id,
                        exp_pass_from_src.out_score     AS in_prty_agmt_scr_val,
                        lkp_indiv_cnt_mgr.indiv_prty_id AS in_prty_id,
                        lkp_agmt.agmt_id                AS in_agmt_id,
                        exp_pass_from_src.lvl_cd        AS in_lvl_num,
                        current_timestamp               AS in_edw_strt_dttm,
                        md5 ( ltrim ( rtrim ( lkp_prty_agmt_scr.prty_agmt_scr_val ) )
                                   || ltrim ( rtrim ( lkp_prty_agmt_scr.lvl_num ) ) ) AS var_orig_chksm,
                        md5 ( ltrim ( rtrim ( exp_pass_from_src.out_score ) )
                                   || ltrim ( rtrim ( exp_pass_from_src.lvl_cd ) ) ) AS var_calc_chksm,
                        CASE
                                   WHEN lkp_prty_agmt_scr.agmt_id IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN var_orig_chksm != var_calc_chksm THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                          AS out_ins_upd,
                        $prcs_id                     AS out_prcs_id,
                        exp_pass_from_src.rank       AS rank,
                        exp_pass_from_src.updatetime AS updatetime,
                        exp_pass_from_src.source_record_id
             FROM       exp_pass_from_src
             inner join lkp_agmt
             ON         exp_pass_from_src.source_record_id = lkp_agmt.source_record_id
             inner join lkp_indiv_cnt_mgr
             ON         lkp_agmt.source_record_id = lkp_indiv_cnt_mgr.source_record_id
             inner join lkp_anltcl_modl
             ON         lkp_indiv_cnt_mgr.source_record_id = lkp_anltcl_modl.source_record_id
             inner join lkp_modl_run
             ON         lkp_anltcl_modl.source_record_id = lkp_modl_run.source_record_id
             inner join lkp_prty_agmt_scr
             ON         lkp_modl_run.source_record_id = lkp_prty_agmt_scr.source_record_id );
  -- Component rtr_pty_scr_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_pty_scr_insert as
  SELECT exp_data_transformation.lkp_modl_id          AS lkp_modl_id,
         exp_data_transformation.lkp_modl_run_id      AS lkp_modl_run_id,
         exp_data_transformation.lkp_prty_id          AS lkp_prty_id,
         exp_data_transformation.lkp_edw_strt_dttm    AS lkp_edw_strt_dttm,
         exp_data_transformation.in_modl_id           AS in_modl_id,
         exp_data_transformation.in_modl_run_id       AS in_modl_run_id,
         exp_data_transformation.in_prty_agmt_scr_val AS in_prty_agmt_scr_val,
         exp_data_transformation.in_prty_id           AS in_prty_id,
         exp_data_transformation.in_agmt_id           AS in_agmt_id,
         exp_data_transformation.in_edw_strt_dttm     AS in_edw_strt_dttm,
         exp_data_transformation.in_lvl_num           AS in_lvl_num,
         exp_data_transformation.out_ins_upd          AS out_ins_upd,
         exp_data_transformation.out_prcs_id          AS in_prcs_id,
         exp_data_transformation.rank                 AS rank,
         exp_data_transformation.updatetime           AS updatetime,
         exp_data_transformation.source_record_id
  FROM   exp_data_transformation
  WHERE  exp_data_transformation.out_ins_upd = ''I''
  AND    exp_data_transformation.in_prty_id IS NOT NULL;
  
  -- Component rtr_pty_scr_UPDATE, Type ROUTER Output Group UPDATE
  create or replace temporary table rtr_pty_scr_update as
  SELECT exp_data_transformation.lkp_modl_id          AS lkp_modl_id,
         exp_data_transformation.lkp_modl_run_id      AS lkp_modl_run_id,
         exp_data_transformation.lkp_prty_id          AS lkp_prty_id,
         exp_data_transformation.lkp_edw_strt_dttm    AS lkp_edw_strt_dttm,
         exp_data_transformation.in_modl_id           AS in_modl_id,
         exp_data_transformation.in_modl_run_id       AS in_modl_run_id,
         exp_data_transformation.in_prty_agmt_scr_val AS in_prty_agmt_scr_val,
         exp_data_transformation.in_prty_id           AS in_prty_id,
         exp_data_transformation.in_agmt_id           AS in_agmt_id,
         exp_data_transformation.in_edw_strt_dttm     AS in_edw_strt_dttm,
         exp_data_transformation.in_lvl_num           AS in_lvl_num,
         exp_data_transformation.out_ins_upd          AS out_ins_upd,
         exp_data_transformation.out_prcs_id          AS in_prcs_id,
         exp_data_transformation.rank                 AS rank,
         exp_data_transformation.updatetime           AS updatetime,
         exp_data_transformation.source_record_id
  FROM   exp_data_transformation
  WHERE  exp_data_transformation.out_ins_upd = ''U''
  AND    exp_data_transformation.in_prty_id IS NOT NULL;
  
  -- Component upd_prty_scr_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_prty_scr_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_pty_scr_insert.in_modl_id           AS in_modl_id,
                rtr_pty_scr_insert.in_modl_run_id       AS in_modl_run_id,
                rtr_pty_scr_insert.in_prty_id           AS in_prty_id,
                rtr_pty_scr_insert.in_prty_agmt_scr_val AS in_prty_agmt_scr_val,
                rtr_pty_scr_insert.in_prcs_id           AS prcs_id,
                rtr_pty_scr_insert.in_lvl_num           AS in_lvl_num1,
                rtr_pty_scr_insert.rank                 AS rank1,
                rtr_pty_scr_insert.updatetime           AS updatetime1,
                rtr_pty_scr_insert.in_agmt_id           AS agmt_id1,
                rtr_pty_scr_insert.in_edw_strt_dttm     AS in_edw_strt_dttm1,
                0                                       AS update_strategy_action,
                rtr_pty_scr_insert.source_record_id
         FROM   rtr_pty_scr_insert );
  -- Component upd_prty_scr_upd_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_prty_scr_upd_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_pty_scr_update.in_modl_id           AS in_modl_id3,
                rtr_pty_scr_update.in_modl_run_id       AS in_modl_run_id3,
                rtr_pty_scr_update.in_prty_agmt_scr_val AS in_prty_agmt_scr_val,
                rtr_pty_scr_update.in_prty_id           AS in_prty_id3,
                rtr_pty_scr_update.in_edw_strt_dttm     AS in_edw_strt_dttm3,
                rtr_pty_scr_update.in_lvl_num           AS in_lvl_num3,
                rtr_pty_scr_update.updatetime           AS updatetime3,
                rtr_pty_scr_update.in_prcs_id           AS in_prcs_id3,
                rtr_pty_scr_update.in_agmt_id           AS agmt_id3,
                0                                       AS update_strategy_action,
                rtr_pty_scr_update.source_record_id
         FROM   rtr_pty_scr_update );
  -- Component exp_pass_to_tgt_upd_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_upd_ins AS
  (
         SELECT upd_prty_scr_upd_ins.in_modl_id3                                       AS modl_id,
                upd_prty_scr_upd_ins.in_modl_run_id3                                   AS modl_run_id,
                upd_prty_scr_upd_ins.in_prty_id3                                       AS prty_id,
                upd_prty_scr_upd_ins.agmt_id3                                          AS agmt_id3,
                upd_prty_scr_upd_ins.in_prty_agmt_scr_val                              AS in_prty_agmt_scr_val,
                upd_prty_scr_upd_ins.in_lvl_num3                                       AS lvl_num,
                upd_prty_scr_upd_ins.in_prcs_id3                                       AS in_prcs_id3,
                upd_prty_scr_upd_ins.in_edw_strt_dttm3                                 AS in_edw_strt_dttm3,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS o_edw_end_dttm,
                upd_prty_scr_upd_ins.updatetime3                                       AS updatetime3,
                to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS o_trans_end_dttm,
                upd_prty_scr_upd_ins.source_record_id
         FROM   upd_prty_scr_upd_ins );
  -- Component upd_prty_scr_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_prty_scr_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_pty_scr_update.in_modl_id           AS in_modl_id3,
                rtr_pty_scr_update.in_modl_run_id       AS in_modl_run_id3,
                rtr_pty_scr_update.in_prty_id           AS in_prty_id3,
                rtr_pty_scr_update.in_prcs_id           AS in_prcs_id3,
                rtr_pty_scr_update.lkp_edw_strt_dttm    AS lkp_edw_strt_dttm3,
                rtr_pty_scr_update.updatetime           AS updatetime3,
                rtr_pty_scr_update.in_edw_strt_dttm     AS in_edw_strt_dttm1,
                rtr_pty_scr_update.in_agmt_id           AS agmt_id3,
                rtr_pty_scr_update.in_prty_agmt_scr_val AS in_prty_agmt_scr_val3,
                rtr_pty_scr_update.in_lvl_num           AS in_lvl_num3,
                1                                       AS update_strategy_action,
                rtr_pty_scr_update.source_record_id
         FROM   rtr_pty_scr_update );
  -- Component exp_pass_to_tgt_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_ins AS
  (
         SELECT upd_prty_scr_ins.in_modl_id                                                                        AS modl_id,
                upd_prty_scr_ins.in_modl_run_id                                                                    AS modl_run_id,
                upd_prty_scr_ins.in_prty_id                                                                        AS prty_id,
                upd_prty_scr_ins.agmt_id1                                                                          AS agmt_id1,
                upd_prty_scr_ins.in_prty_agmt_scr_val                                                              AS in_prty_agmt_scr_val,
                upd_prty_scr_ins.prcs_id                                                                           AS prcs_id,
                upd_prty_scr_ins.in_lvl_num1                                                                       AS in_lvl_num1,
                dateadd (second, ( 2 * ( upd_prty_scr_ins.rank1 - 1 ) ),upd_prty_scr_ins.in_edw_strt_dttm1 ) AS out_edw_strt_date,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )                             AS o_edw_end_dttm,
                upd_prty_scr_ins.updatetime1                                                                       AS updatetime1,
                to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )                             AS o_trans_end_dttm,
                upd_prty_scr_ins.source_record_id
         FROM   upd_prty_scr_ins );
  -- Component exp_pass_to_tgt_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt_upd AS
  (
         SELECT upd_prty_scr_upd.in_modl_id3                                    AS modl_id,
                upd_prty_scr_upd.in_modl_run_id3                                AS modl_run_id,
                upd_prty_scr_upd.in_prty_id3                                    AS prty_id,
                upd_prty_scr_upd.agmt_id3                                       AS agmt_id3,
                upd_prty_scr_upd.lkp_edw_strt_dttm3                             AS edw_strt_dttm,
                dateadd ( second, -1, upd_prty_scr_upd.in_edw_strt_dttm1 ) AS edw_end_dttm,
                dateadd (second,-1, upd_prty_scr_upd.updatetime3  ) AS o_trans_end_dttm,
                upd_prty_scr_upd.source_record_id
         FROM   upd_prty_scr_upd );
  -- Component PRTY_AGMT_SCR_update1, Type TARGET
  INSERT INTO db_t_prod_core.prty_agmt_scr
              (
                          prty_id,
                          agmt_id,
                          modl_id,
                          modl_run_id,
                          prty_agmt_scr_val,
                          lvl_num,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_tgt_upd_ins.prty_id              AS prty_id,
         exp_pass_to_tgt_upd_ins.agmt_id3             AS agmt_id,
         exp_pass_to_tgt_upd_ins.modl_id              AS modl_id,
         exp_pass_to_tgt_upd_ins.modl_run_id          AS modl_run_id,
         exp_pass_to_tgt_upd_ins.in_prty_agmt_scr_val AS prty_agmt_scr_val,
         exp_pass_to_tgt_upd_ins.lvl_num              AS lvl_num,
         exp_pass_to_tgt_upd_ins.in_prcs_id3          AS prcs_id,
         exp_pass_to_tgt_upd_ins.in_edw_strt_dttm3    AS edw_strt_dttm,
         exp_pass_to_tgt_upd_ins.o_edw_end_dttm       AS edw_end_dttm,
         exp_pass_to_tgt_upd_ins.updatetime3          AS trans_strt_dttm,
         exp_pass_to_tgt_upd_ins.o_trans_end_dttm     AS trans_end_dttm
  FROM   exp_pass_to_tgt_upd_ins;
  
  -- Component PRTY_AGMT_SCR_insert, Type TARGET
  INSERT INTO db_t_prod_core.prty_agmt_scr
              (
                          prty_id,
                          agmt_id,
                          modl_id,
                          modl_run_id,
                          prty_agmt_scr_val,
                          lvl_num,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_tgt_ins.prty_id              AS prty_id,
         exp_pass_to_tgt_ins.agmt_id1             AS agmt_id,
         exp_pass_to_tgt_ins.modl_id              AS modl_id,
         exp_pass_to_tgt_ins.modl_run_id          AS modl_run_id,
         exp_pass_to_tgt_ins.in_prty_agmt_scr_val AS prty_agmt_scr_val,
         exp_pass_to_tgt_ins.in_lvl_num1          AS lvl_num,
         exp_pass_to_tgt_ins.prcs_id              AS prcs_id,
         exp_pass_to_tgt_ins.out_edw_strt_date    AS edw_strt_dttm,
         exp_pass_to_tgt_ins.o_edw_end_dttm       AS edw_end_dttm,
         exp_pass_to_tgt_ins.updatetime1          AS trans_strt_dttm,
         exp_pass_to_tgt_ins.o_trans_end_dttm     AS trans_end_dttm
  FROM   exp_pass_to_tgt_ins;
  
  -- Component PRTY_AGMT_SCR_update, Type TARGET
  merge
  INTO         db_t_prod_core.prty_agmt_scr
  USING        exp_pass_to_tgt_upd
  ON (
                            prty_agmt_scr.prty_id = exp_pass_to_tgt_upd.prty_id
               AND          prty_agmt_scr.agmt_id = exp_pass_to_tgt_upd.agmt_id3
               AND          prty_agmt_scr.modl_id = exp_pass_to_tgt_upd.modl_id
               AND          prty_agmt_scr.modl_run_id = exp_pass_to_tgt_upd.modl_run_id
               AND          prty_agmt_scr.edw_strt_dttm = exp_pass_to_tgt_upd.edw_strt_dttm)
  WHEN matched THEN
  UPDATE
  SET    prty_id = exp_pass_to_tgt_upd.prty_id,
         agmt_id = exp_pass_to_tgt_upd.agmt_id3,
         modl_id = exp_pass_to_tgt_upd.modl_id,
         modl_run_id = exp_pass_to_tgt_upd.modl_run_id,
         edw_strt_dttm = exp_pass_to_tgt_upd.edw_strt_dttm,
         edw_end_dttm = exp_pass_to_tgt_upd.edw_end_dttm,
         trans_end_dttm = exp_pass_to_tgt_upd.o_trans_end_dttm;

END;
';