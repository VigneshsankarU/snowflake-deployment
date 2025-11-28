-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_MBRSHP_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
declare
start_dttm timestamp;
end_dttm timestamp;
prcs_id int;
run_id string;


BEGIN 
run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);

  -- Component SQ_pc_effectivedatedfields, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_effectivedatedfields AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS clientid_alfa,
                $2 AS typecode,
                $3 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT pc_effectivedatedfields.clientid_alfa_stg,
                                                                  pctl_clientidtype_alfa.typecode_stg
                                                  FROM            (
                                                                                  SELECT DISTINCT effdt.clientid_alfa_stg,
                                                                                                  effdt.clientidtype_alfa_stg
                                                                                  FROM            db_t_prod_stag.pc_effectivedatedfields AS effdt
                                                                                  left outer join db_t_prod_stag.pc_policyperiod pp
                                                                                  ON              pp.id_stg=effdt.branchid_stg
                                                                                  left outer join db_t_prod_stag.pc_policycontactrole pcr
                                                                                  ON              pp.id_stg=pcr.branchid_stg
                                                                                  AND             pcr.fixedid_stg=effdt.primarynamedinsured_stg
                                                                                  left outer join db_t_prod_stag.pc_contact cnt
                                                                                  ON              cnt.id_stg=pcr.contactdenorm_stg
                                                                                  left outer join db_t_prod_stag.pctl_contact
                                                                                  ON              pctl_contact.id_stg= cnt.subtype_stg
                                                                                  left outer join db_t_prod_stag.pc_job pcj
                                                                                  ON              pcj.id_stg = pp.jobid_stg
                                                                                  left outer join db_t_prod_stag.pctl_job pctlj
                                                                                  ON              pctlj.id_stg=pcj.subtype_stg
                                                                                  left join       db_t_prod_stag.pctl_policyperiodstatus pps
                                                                                  ON              pps.id_stg = pp.status_stg
                                                                                  WHERE           effdt.expirationdate_stg IS NULL
                                                                                  AND             pctlj.typecode_stg IN (''Cancellation'',
                                                                                                                         ''PolicyChange'',
                                                                                                                         ''Reinstatement'',
                                                                                                                         ''Renewal'',
                                                                                                                         ''Rewrite'',
                                                                                                                         ''Submission'')
                                                                                  AND             pps.typecode_stg<>''Temporary''
                                                                                  AND             effdt.updatetime_stg > (:start_dttm)
                                                                                  AND             effdt.updatetime_stg <= (:end_dttm) ) pc_effectivedatedfields
                                                  inner join      db_t_prod_stag.pctl_clientidtype_alfa
                                                  ON              pc_effectivedatedfields.clientidtype_alfa_stg=pctl_clientidtype_alfa.id_stg
                                                  WHERE           pc_effectivedatedfields.clientid_alfa_stg IS NOT NULL ) src ) );
  -- Component exp_pass_through_mapping, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_through_mapping AS
  (
         SELECT sq_pc_effectivedatedfields.clientid_alfa AS clientid_alfa,
                sq_pc_effectivedatedfields.typecode      AS clientidtype_alfa,
                sq_pc_effectivedatedfields.source_record_id
         FROM   sq_pc_effectivedatedfields );
  -- Component LKP_TERADATA_ETL_REF_XLAT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat AS
  (
            SELECT    lkp.tgt_idntftn_val,
                      exp_pass_through_mapping.source_record_id,
                      row_number() over(PARTITION BY exp_pass_through_mapping.source_record_id ORDER BY lkp.tgt_idntftn_val DESC,lkp.src_idntftn_val DESC) rnk
            FROM      exp_pass_through_mapping
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''MBRSHP_TYPE''
                             AND    teradata_etl_ref_xlat.src_idntftn_nm= ''pctl_clientidtype_alfa.typecode''
                             AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' 
			     QUALIFY ROW_NUMBER() OVER (PARTITION BY SRC_IDNTFTN_VAL ORDER BY TGT_IDNTFTN_VAL DESC)=1) lkp
            ON        lkp.src_idntftn_val = exp_pass_through_mapping.clientidtype_alfa 
			qualify rnk = 1 
			);
  -- Component EXPTRANS, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exptrans AS
  (
             SELECT     exp_pass_through_mapping.clientid_alfa AS clientid_alfa,
                        CASE
                                   WHEN lkp_teradata_etl_ref_xlat.tgt_idntftn_val IS NULL THEN ''UNK''
                                   ELSE lkp_teradata_etl_ref_xlat.tgt_idntftn_val
                        end AS mbrshp_type_cd,
                        exp_pass_through_mapping.source_record_id
             FROM       exp_pass_through_mapping
             inner join lkp_teradata_etl_ref_xlat
             ON         exp_pass_through_mapping.source_record_id = lkp_teradata_etl_ref_xlat.source_record_id );
  -- Component LKP_MBRSHP, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_mbrshp AS
  (
            SELECT    lkp.mbrshp_id,
                      lkp.mbrshp_type_cd,
                      exptrans.source_record_id,
                      row_number() over(PARTITION BY exptrans.source_record_id ORDER BY lkp.mbrshp_id ASC,lkp.mbrshp_type_cd ASC) rnk
            FROM      exptrans
            left join
                      (
                               SELECT   mbrshp.mbrshp_id      AS mbrshp_id,
                                        mbrshp.mbrshp_type_cd AS mbrshp_type_cd,
                                        mbrshp.mbrshp_num     AS mbrshp_num
                               FROM     db_t_prod_core.mbrshp qualify row_number() over(PARTITION BY mbrshp_num, mbrshp_type_cd, mbrshp_id ORDER BY edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.mbrshp_num = exptrans.clientid_alfa
            AND       lkp.mbrshp_type_cd = exptrans.mbrshp_type_cd qualify rnk = 1 );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
             SELECT     lkp_mbrshp.mbrshp_id                   AS lkp_mbrshp_id,
                        NULL                                   AS indiv_prty_id,
                        exp_pass_through_mapping.clientid_alfa AS in_clientid_alfa,
                        1                                      AS out_org_prty_id,
                        :prcs_id                               AS out_prsc_id,
                        CASE
                                   WHEN lkp_teradata_etl_ref_xlat.tgt_idntftn_val IS NULL THEN ''UNK''
                                   ELSE lkp_teradata_etl_ref_xlat.tgt_idntftn_val
                        end AS out_clienttype,
                        CASE
                                   WHEN lkp_teradata_etl_ref_xlat.tgt_idntftn_val IS NULL THEN ''UNK''
                                   ELSE lkp_teradata_etl_ref_xlat.tgt_idntftn_val
                        end                                                                    AS v_clienttype,
                        current_timestamp                                                      AS edw_start_dttm,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                        CASE
                                   WHEN lkp_mbrshp.mbrshp_id IS NULL THEN ''I''
                                   ELSE (
                                              CASE
                                                         WHEN v_clienttype <> lkp_mbrshp.mbrshp_type_cd THEN ''U''
                                                         ELSE ''R''
                                              end )
                        end AS out_flag_ins_upd,
                        exp_pass_through_mapping.source_record_id
             FROM       exp_pass_through_mapping
             inner join lkp_teradata_etl_ref_xlat
             ON         exp_pass_through_mapping.source_record_id = lkp_teradata_etl_ref_xlat.source_record_id
             inner join lkp_mbrshp
             ON         lkp_teradata_etl_ref_xlat.source_record_id = lkp_mbrshp.source_record_id );
  -- Component rtr_MBRSHP_INSERT, Type ROUTER Output Group INSERT
  create or replace TEMPORARY TABLE rtr_mbrshp_insert AS
  SELECT exp_data_transformation.indiv_prty_id    AS indiv_prty_id,
         exp_data_transformation.out_clienttype   AS clienttype,
         exp_data_transformation.in_clientid_alfa AS clientid_alfa,
         exp_data_transformation.lkp_mbrshp_id    AS mbrshp_id,
         exp_data_transformation.out_prsc_id      AS out_prsc_id,
         exp_data_transformation.out_org_prty_id  AS out_org_prty_id,
         exp_data_transformation.edw_start_dttm   AS edw_start_dttm,
         exp_data_transformation.edw_end_dttm     AS edw_end_dttm,
         exp_data_transformation.out_flag_ins_upd AS out_flag_ins_upd,
         exp_data_transformation.source_record_id
  FROM   exp_data_transformation
  WHERE  exp_data_transformation.out_flag_ins_upd = ''I''
  OR     exp_data_transformation.out_flag_ins_upd = ''U'';
  
  -- Component rtr_MBRSHP_UPDATE, Type ROUTER Output Group UPDATE
  create or replace TEMPORARY TABLE rtr_mbrshp_update AS
  SELECT exp_data_transformation.indiv_prty_id    AS indiv_prty_id,
         exp_data_transformation.out_clienttype   AS clienttype,
         exp_data_transformation.in_clientid_alfa AS clientid_alfa,
         exp_data_transformation.lkp_mbrshp_id    AS mbrshp_id,
         exp_data_transformation.out_prsc_id      AS out_prsc_id,
         exp_data_transformation.out_org_prty_id  AS out_org_prty_id,
         exp_data_transformation.edw_start_dttm   AS edw_start_dttm,
         exp_data_transformation.edw_end_dttm     AS edw_end_dttm,
         exp_data_transformation.out_flag_ins_upd AS out_flag_ins_upd,
         exp_data_transformation.source_record_id
  FROM   exp_data_transformation
  WHERE  exp_data_transformation.out_flag_ins_upd = ''U'';
  
  -- Component upd_mbrshp_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_mbrshp_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_mbrshp_insert.indiv_prty_id   AS indiv_prty_id1,
                rtr_mbrshp_insert.clienttype      AS client_type1,
                rtr_mbrshp_insert.clientid_alfa   AS clientid_alfa1,
                seq_mbrshp.NEXTVAL                AS mbrshp_id1,
                rtr_mbrshp_insert.out_prsc_id     AS out_prsc_id1,
                rtr_mbrshp_insert.out_org_prty_id AS out_org_prty_id1,
                rtr_mbrshp_insert.edw_start_dttm  AS edw_start_dttm3,
                rtr_mbrshp_insert.edw_end_dttm    AS edw_end_dttm3,
                0                                 AS update_strategy_action,
				rtr_mbrshp_insert.source_record_id
         FROM   rtr_mbrshp_insert );
  -- Component ext_pass_through_target_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE ext_pass_through_target_ins AS
  (
         SELECT upd_mbrshp_ins.indiv_prty_id1   AS indiv_prty_id1,
                upd_mbrshp_ins.client_type1     AS client_type1,
                upd_mbrshp_ins.clientid_alfa1   AS clientid_alfa1,
                upd_mbrshp_ins.mbrshp_id1       AS mbrshp_id1,
                upd_mbrshp_ins.out_prsc_id1     AS out_prsc_id1,
                upd_mbrshp_ins.out_org_prty_id1 AS out_org_prty_id1,
                upd_mbrshp_ins.edw_start_dttm3  AS edw_start_dttm3,
                upd_mbrshp_ins.edw_end_dttm3    AS edw_end_dttm3,
                upd_mbrshp_ins.source_record_id
         FROM   upd_mbrshp_ins );
  -- Component upd_mbrshp_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_mbrshp_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_mbrshp_update.indiv_prty_id   AS indiv_prty_id3,
                rtr_mbrshp_update.clienttype      AS client_type3,
                rtr_mbrshp_update.clientid_alfa   AS clientid_alfa3,
                rtr_mbrshp_update.mbrshp_id       AS mbrshp_id3,
                rtr_mbrshp_update.out_prsc_id     AS out_prsc_id3,
                rtr_mbrshp_update.out_org_prty_id AS out_org_prty_id3,
                rtr_mbrshp_update.edw_start_dttm  AS edw_start_dttm3,
                rtr_mbrshp_update.edw_end_dttm    AS edw_end_dttm3,
                1                                 AS update_strategy_action,
				rtr_mbrshp_update.source_record_id
         FROM   rtr_mbrshp_update );
  -- Component ext_pass_through_target_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE ext_pass_through_target_upd AS
  (
         SELECT upd_mbrshp_upd.mbrshp_id3                          AS mbrshp_id3,
                dateadd(''second'', - 1, upd_mbrshp_upd.edw_start_dttm3) AS edw_end_dttm31,
                upd_mbrshp_upd.source_record_id
         FROM   upd_mbrshp_upd );
  -- Component MBRSHP_ins, Type TARGET
  INSERT INTO db_t_prod_core.mbrshp
              (
                          mbrshp_id,
                          mbrshp_num,
                          indiv_prty_id,
                          org_prty_id,
                          mbrshp_type_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT ext_pass_through_target_ins.mbrshp_id1       AS mbrshp_id,
         ext_pass_through_target_ins.clientid_alfa1   AS mbrshp_num,
         ext_pass_through_target_ins.indiv_prty_id1   AS indiv_prty_id,
         ext_pass_through_target_ins.out_org_prty_id1 AS org_prty_id,
         ext_pass_through_target_ins.client_type1     AS mbrshp_type_cd,
         ext_pass_through_target_ins.out_prsc_id1     AS prcs_id,
         ext_pass_through_target_ins.edw_start_dttm3  AS edw_strt_dttm,
         ext_pass_through_target_ins.edw_end_dttm3    AS edw_end_dttm
  FROM   ext_pass_through_target_ins;
  
  -- Component MBRSHP_upd, Type TARGET
  merge
  INTO         db_t_prod_core.mbrshp
  USING        ext_pass_through_target_upd
  ON (
                            mbrshp.mbrshp_id = ext_pass_through_target_upd.mbrshp_id3)
  WHEN matched THEN
  UPDATE
  SET    mbrshp_id = ext_pass_through_target_upd.mbrshp_id3,
         edw_end_dttm = ext_pass_through_target_upd.edw_end_dttm31;

end;
';