-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_SPEC_AFNTYGRP_INS("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component LKP_TERADATA_ETL_REF_XLAT_AGMT_SPEC_TYPE_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_agmt_spec_type_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''AGMT_SPEC_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''pctl_affinitygroup_alfa.typecode''
         AND    teradata_etl_ref_xlat.src_idntftn_sys= ''GW''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' 
  );
  -- Component LKP_TERADATA_ETL_REF_XLAT_SPEC_TYPE_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_spec_type_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SPEC_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''pctl_affinitygrouptype_alfa.typecode''
         AND    teradata_etl_ref_xlat.src_idntftn_sys= ''GW''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' 
  );
  -- Component sq_pc_policyperiod, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_policyperiod AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS policynumber,
                $2 AS publicid,
                $3 AS afntygrptype_cd,
                $4 AS effectivedate,
                $5 AS expirationdate,
                $6 AS updatetime,
                $7 AS editeffectivedate,
                $8 AS agmt_spec_type,
                $9 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   policynumber,
                                                    publicid,
                                                    agmt_spec_type_cd,
                                                    effectivedate,
                                                    expirationdate,
                                                    policyperiod_updatetime,
                                                    editeffectivedate,
                                                    agmt_spec_type
                                           FROM     (
                                                           SELECT pp1.policynumber_stg AS policynumber,
                                                                  /* 01  */
                                                                  pp1.publicid_stg AS publicid,
                                                                  /* 02 */
                                                                  agtype.typecode_stg AS agmt_spec_type_cd,
                                                                  /* 03  */
                                                                  ppl1.effectivedate_stg AS effectivedate,
                                                                  /* 04 */
                                                                  ppl1.expirationdate_stg AS expirationdate,
                                                                  /* 05 */
                                                                  pp1.updatetime_stg AS policyperiod_updatetime,
                                                                  /* 06 */
                                                                  pp1.editeffectivedate_stg AS editeffectivedate,
                                                                  /* 07 */
                                                                  pag.typecode_stg AS agmt_spec_type,
                                                                  /* 08 */
                                                                  ag.updatetime_stg AS spec_updatetime
                                                           FROM   db_t_prod_stag.pc_policyperiod pp1
                                                           join   db_t_prod_stag.pc_policyline ppl1
                                                           ON     ppl1.branchid_stg = pp1.id_stg
                                                           AND    ppl1.expirationdate_stg IS NULL
                                                           join   db_t_prod_stag.pctl_policyperiodstatus ps
                                                           ON     ps.id_stg = pp1.status_stg
                                                           join   db_t_prod_stag.pcx_affinitygroup_alfa ag
                                                           ON     ag.personalautoline_stg = ppl1.id_stg
                                                           join   db_t_prod_stag.pctl_affinitygrouptype_alfa agtype
                                                           ON     agtype.id_stg = ag.affinitytype_stg
                                                           join   db_t_prod_stag.pctl_affinitygroup_alfa pag
                                                           ON     pag.id_stg = ag.subtype_stg
                                                           WHERE  ps.typecode_stg = ''Bound''
                                                           AND    ag.updatetime_stg >$start_dttm
                                                           AND    ag.updatetime_stg <=$end_dttm ) agmt_spec_x
                                           join     db_t_prod_stag.pctl_affinitygroup_alfa
                                           ON       pctl_affinitygroup_alfa.typecode_stg = agmt_spec_type 
										   qualify row_number() over (PARTITION BY agmt_spec_x.publicid , agmt_spec_x.agmt_spec_type_cd ORDER BY agmt_spec_x.spec_updatetime DESC) =1 ) src ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
            SELECT    sq_pc_policyperiod.editeffectivedate AS editeffectivedate,
                      sq_pc_policyperiod.updatetime        AS updatetime,
                      sq_pc_policyperiod.publicid          AS publicid,
                      lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SPEC_TYPE_CD */
                      AS out_spec_type_cd,
                      lkp_2.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_SPEC_TYPE_CD */
                                                     AS out_agmt_spec_type,
                      $p_agmt_type_cd_policy_version AS out_agmt_type_cd,
                      sq_pc_policyperiod.source_record_id,
                      row_number() over (PARTITION BY sq_pc_policyperiod.source_record_id ORDER BY sq_pc_policyperiod.source_record_id) AS rnk
            FROM      sq_pc_policyperiod
            left join lkp_teradata_etl_ref_xlat_spec_type_cd lkp_1
            ON        lkp_1.src_idntftn_val = sq_pc_policyperiod.afntygrptype_cd
            left join lkp_teradata_etl_ref_xlat_agmt_spec_type_cd lkp_2
            ON        lkp_2.src_idntftn_val = sq_pc_policyperiod.agmt_spec_type qualify rnk = 1 );
  -- Component LKP_XREF_AGMT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_xref_agmt AS
  (
            SELECT    lkp.agmt_id,
                      exp_pass_from_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.agmt_id DESC,lkp.nk_src_key DESC,lkp.term_num DESC,lkp.agmt_type_cd DESC) rnk
            FROM      exp_pass_from_source
            left join
                      (
                             SELECT dir_agmt.agmt_id                    AS agmt_id,
                                    ltrim(rtrim(dir_agmt.nk_src_key))   AS nk_src_key,
                                    dir_agmt.term_num                   AS term_num,
                                    ltrim(rtrim(dir_agmt.agmt_type_cd)) AS agmt_type_cd
                             FROM   db_t_prod_core.dir_agmt ) lkp
            ON        lkp.nk_src_key = exp_pass_from_source.publicid
            AND       lkp.term_num = $in_term_num
            AND       lkp.agmt_type_cd = exp_pass_from_source.out_agmt_type_cd 
			qualify row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.agmt_id DESC,lkp.nk_src_key DESC,lkp.term_num DESC,lkp.agmt_type_cd DESC) 
			= 1 );
  -- Component LKP_AGMT_SPEC, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt_spec AS
  (
             SELECT     lkp.agmt_id,
                        lkp.spec_type_cd,
                        lkp_xref_agmt.agmt_id                   AS in_agmt_id,
                        exp_pass_from_source.out_agmt_spec_type AS in_agmt_spec_type,
                        exp_pass_from_source.out_spec_type_cd   AS in_spec_type_cd,
                        exp_pass_from_source.source_record_id,
                        row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.agmt_id ASC,lkp.spec_type_cd ASC) rnk
             FROM       exp_pass_from_source
             inner join lkp_xref_agmt
             ON         exp_pass_from_source.source_record_id = lkp_xref_agmt.source_record_id
             left join
                        (
                               SELECT agmt_spec.agmt_id           AS agmt_id,
                                      agmt_spec.agmt_spec_type_cd AS agmt_spec_type_cd,
                                      agmt_spec.spec_type_cd      AS spec_type_cd
                               FROM   db_t_prod_core.agmt_spec
                               WHERE  edw_end_dttm=cast(''9999-12-31'' AS DATE)
                               AND    agmt_spec_type_cd=''AFFNTYGRP''
                                      /*  */
                        ) lkp
             ON         lkp.agmt_id = lkp_xref_agmt.agmt_id
             AND        lkp.agmt_spec_type_cd = exp_pass_from_source.out_agmt_spec_type
             AND        lkp.spec_type_cd = exp_pass_from_source.out_spec_type_cd 
			 qualify row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.agmt_id ASC,lkp.spec_type_cd ASC) 
			 = 1 );
  -- Component exp_pass_to_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_upd AS
  (
             SELECT     lkp_agmt_spec.agmt_id                                                 AS lkp_agmt_id,
                        lkp_agmt_spec.spec_type_cd                                            AS lkp_spec_type_cd,
                        to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_agmt_spec_end_dttm,
                        $prcs_id                                                              AS out_prcs_id,
                        current_timestamp                                                     AS out_edw_strt_dttm,
                        to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                        to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_trans_end_dttm,
                        CASE
                                   WHEN lkp_agmt_spec.agmt_id IS NULL THEN ''I''
                                   ELSE NULL
                        END                                    AS ins_flag,
                        lkp_agmt_spec.in_agmt_id               AS in_agmt_id,
                        lkp_agmt_spec.in_agmt_spec_type        AS in_agmt_spec_type,
                        lkp_agmt_spec.in_spec_type_cd          AS in_spec_type_cd,
                        exp_pass_from_source.editeffectivedate AS in_editeffectivedate,
                        exp_pass_from_source.updatetime        AS in_updatetime,
                        exp_pass_from_source.source_record_id
             FROM       exp_pass_from_source
             inner join lkp_agmt_spec
             ON         exp_pass_from_source.source_record_id = lkp_agmt_spec.source_record_id );
  
  -- Component rtr_agmt_spec_INSERT, Type ROUTER Output Group INSERT
  create or replace TEMPORARY table rtr_agmt_spec_INSERT as
  SELECT exp_pass_to_upd.lkp_agmt_id            AS lkp_agmt_id,
         NULL                                   AS lkp_agmt_spec_type_cd,
         NULL                                   AS lkp_agmt_spec_strt_dttm,
         NULL                                   AS lkp_agmt_spec_end_dttm,
         exp_pass_to_upd.lkp_spec_type_cd       AS lkp_spec_type_cd,
         NULL                                   AS lkp_agmt_spec_cnt,
         NULL                                   AS lkp_agmt_spec_txt,
         NULL                                   AS lkp_agmt_spec_qty,
         NULL                                   AS lkp_agmt_spec_rate,
         NULL                                   AS lkp_agmt_spec_amt,
         NULL                                   AS lkp_agmt_spec_dt,
         NULL                                   AS lkp_prcs_id,
         NULL                                   AS lkp_edw_strt_dttm,
         NULL                                   AS lkp_edw_end_dttm,
         NULL                                   AS lkp_trans_strt_dttm,
         exp_pass_to_upd.out_trans_end_dttm     AS trans_end_dttm,
         exp_pass_to_upd.ins_flag               AS ins_flag,
         exp_pass_to_upd.in_agmt_id             AS in_agmt_id,
         exp_pass_to_upd.in_agmt_spec_type      AS in_agmt_spec_type,
         exp_pass_to_upd.in_spec_type_cd        AS in_spec_type_cd,
         exp_pass_to_upd.in_editeffectivedate   AS in_editeffectivedate,
         exp_pass_to_upd.in_updatetime          AS in_updatetime,
         exp_pass_to_upd.out_prcs_id            AS in_prcs_id,
         exp_pass_to_upd.out_edw_strt_dttm      AS in_edw_strt_dttm,
         exp_pass_to_upd.out_edw_end_dttm       AS in_edw_end_dttm,
         exp_pass_to_upd.out_agmt_spec_end_dttm AS in_agmt_spec_end_dttm,
         exp_pass_to_upd.source_record_id
  FROM   exp_pass_to_upd
  WHERE  exp_pass_to_upd.ins_flag = ''I''
  AND    exp_pass_to_upd.in_agmt_id IS NOT NULL
  AND    exp_pass_to_upd.in_spec_type_cd IS NOT NULL;
  
  -- Component upd_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_agmt_spec_insert.in_agmt_id            AS agmt_id,
                rtr_agmt_spec_insert.in_agmt_spec_type     AS agmt_spec_type_cd,
                rtr_agmt_spec_insert.in_editeffectivedate  AS agmt_spec_strt_dttm,
                rtr_agmt_spec_insert.in_agmt_spec_end_dttm AS agmt_spec_end_dttm,
                rtr_agmt_spec_insert.in_spec_type_cd       AS in_spec_type_cd,
                rtr_agmt_spec_insert.in_prcs_id            AS prcs_id,
                rtr_agmt_spec_insert.in_edw_strt_dttm      AS edw_strt_dttm,
                rtr_agmt_spec_insert.in_edw_end_dttm       AS edw_end_dttm,
                rtr_agmt_spec_insert.in_updatetime         AS trans_strt_dttm,
                rtr_agmt_spec_insert.trans_end_dttm        AS trans_end_dttm,
                0                                          AS update_strategy_action,
				rtr_agmt_spec_insert.source_record_id
         FROM   rtr_agmt_spec_insert );
  -- Component exp_pass_to_target_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ins AS
  (
         SELECT upd_ins.agmt_id             AS agmt_id,
                upd_ins.agmt_spec_type_cd   AS agmt_spec_type_cd,
                upd_ins.agmt_spec_strt_dttm AS agmt_spec_strt_dttm,
                upd_ins.agmt_spec_end_dttm  AS agmt_spec_end_dttm,
                upd_ins.in_spec_type_cd     AS in_spec_type_cd,
                NULL                        AS agmt_spec_cnt,
                NULL                        AS agmt_spec_txt,
                NULL                        AS agmt_spec_qty,
                NULL                        AS agmt_spec_rate,
                NULL                        AS agmt_spec_amt,
                NULL                        AS agmt_spec_dt,
                upd_ins.prcs_id             AS prcs_id,
                upd_ins.edw_strt_dttm       AS edw_strt_dttm,
                upd_ins.edw_end_dttm        AS edw_end_dttm,
                upd_ins.trans_strt_dttm     AS trans_strt_dttm,
                upd_ins.trans_end_dttm      AS trans_end_dttm,
                upd_ins.source_record_id
         FROM   upd_ins );
  -- Component AGMT_SPEC, Type TARGET
  INSERT INTO db_t_prod_core.agmt_spec
              (
                          agmt_id,
                          agmt_spec_type_cd,
                          agmt_spec_strt_dttm,
                          agmt_spec_end_dttm,
                          spec_type_cd,
                          agmt_spec_cnt,
                          agmt_spec_txt,
                          agmt_spec_qty,
                          agmt_spec_rate,
                          agmt_spec_amt,
                          agmt_spec_dt,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_target_ins.agmt_id             AS agmt_id,
         exp_pass_to_target_ins.agmt_spec_type_cd   AS agmt_spec_type_cd,
         exp_pass_to_target_ins.agmt_spec_strt_dttm AS agmt_spec_strt_dttm,
         exp_pass_to_target_ins.agmt_spec_end_dttm  AS agmt_spec_end_dttm,
         exp_pass_to_target_ins.in_spec_type_cd     AS spec_type_cd,
         exp_pass_to_target_ins.agmt_spec_cnt       AS agmt_spec_cnt,
         exp_pass_to_target_ins.agmt_spec_txt       AS agmt_spec_txt,
         exp_pass_to_target_ins.agmt_spec_qty       AS agmt_spec_qty,
         exp_pass_to_target_ins.agmt_spec_rate      AS agmt_spec_rate,
         exp_pass_to_target_ins.agmt_spec_amt       AS agmt_spec_amt,
         exp_pass_to_target_ins.agmt_spec_dt        AS agmt_spec_dt,
         exp_pass_to_target_ins.prcs_id             AS prcs_id,
         exp_pass_to_target_ins.edw_strt_dttm       AS edw_strt_dttm,
         exp_pass_to_target_ins.edw_end_dttm        AS edw_end_dttm,
         exp_pass_to_target_ins.trans_strt_dttm     AS trans_strt_dttm,
         exp_pass_to_target_ins.trans_end_dttm      AS trans_end_dttm
  FROM   exp_pass_to_target_ins;

END;
';