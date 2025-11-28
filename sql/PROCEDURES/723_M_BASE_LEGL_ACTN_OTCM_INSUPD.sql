-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_LEGL_ACTN_OTCM_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
declare		
    run_id varchar;
	start_dttm timestamp;
	end_dttm timestamp;
    prcs_id int;


BEGIN 
 run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
 PRCS_ID:=     (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);

  -- Component SQ_cc_matter, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_cc_matter AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS publicid,
                $2 AS legl_actn_typecode,
                $3 AS src_sys_cd,
                $4 AS legl_actn_otcm_typecode,
                $5 AS legl_actn_fnl_cst,
                $6 AS updatetime,
                $7 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         /* daily_load mapping Query EIM-40665 */
                                         SELECT src.publicid        AS publicid,
                                                src.typecode        AS legl_actn_typecode,
                                                src.src_idntftn_val AS src_sys_cd,
                                                src.typecode1       AS legl_actn_otcm_typecode,
                                                CASE
                                                       WHEN src.finallegalcost_stg IS NULL THEN 0
                                                       ELSE src.finallegalcost_stg
                                                END            AS legl_actn_fnl_cst,
                                                src.updatetime AS updatetime
                                         FROM   (
                                                           SELECT     cc_matter_x.publicid_stg         AS publicid,
                                                                      cctl_mattertype.typecode_stg     AS typecode,
                                                                      ''SRC_SYS6''                       AS src_idntftn_val,
                                                                      cctl_resolutiontype.typecode_stg AS typecode1,
                                                                      cc_matter_x.finallegalcost_stg   AS finallegalcost_stg,
                                                                      cc_matter_x.updatetime_stg       AS updatetime
                                                           FROM       (
                                                                             SELECT cast(cc_matter.publicid_stg AS VARCHAR(64)) AS publicid_stg,
                                                                                    cc_matter.mattertype_stg,
                                                                                    cc_matter.resolution_stg,
                                                                                    cc_matter.finallegalcost_stg AS finallegalcost_stg,
                                                                                    cc_matter.updatetime_stg
                                                                             FROM   db_t_prod_stag.cc_matter
                                                                             join
                                                                                    (
                                                                                               SELECT     cc_claim.id_stg
                                                                                               FROM       db_t_prod_stag.cc_claim
                                                                                               inner join db_t_prod_stag.cctl_claimstate
                                                                                               ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                               WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                             ON     cc_matter.claimid_stg=cc_claim.id_stg
                                                                             WHERE  cc_matter.updatetime_stg > (:start_dttm)
                                                                             AND    cc_matter.updatetime_stg <= (:end_dttm) ) cc_matter_x
                                                           inner join db_t_prod_stag.cctl_mattertype
                                                           ON         cc_matter_x.mattertype_stg=cctl_mattertype.id_stg
                                                           inner join db_t_prod_stag.cctl_resolutiontype
                                                           ON         cctl_resolutiontype.id_stg=cc_matter_x.resolution_stg
                                                           WHERE      cc_matter_x.resolution_stg IS NOT NULL
                                                           UNION
                                                           SELECT subsum.publicid_stg AS publicid,
                                                                  ''LEGL_ACTN_TYPE1''   AS typecode,
                                                                  ''SRC_SYS6''          AS src_idntftn_val,
                                                                  suboc.typecode_stg  AS typecode1,
                                                                  NULL                AS finallegalcost_stg,
                                                                  CASE
                                                                         WHEN cast(subsum.updatetime_stg AS DATE) > cast(sub.updatetime_stg AS DATE) THEN subsum.updatetime_stg
                                                                         WHEN cast(sub.updatetime_stg AS    DATE) > cast(subsum.updatetime_stg AS DATE) THEN sub.updatetime_stg
                                                                         WHEN cast(subsum.updatetime_stg AS DATE) = cast(sub.updatetime_stg AS DATE) THEN subsum.updatetime_stg
                                                                  END AS updatetime
                                                                  /* ,sub.updatetime_stg as updtime */
                                                           FROM   db_t_prod_stag.cc_subrogationsummary subsum
                                                           join
                                                                  (
                                                                         SELECT clm.id_stg
                                                                         FROM   db_t_prod_stag.cc_claim clm
                                                                         join   db_t_prod_stag.cctl_claimstate
                                                                         ON     clm.state_stg= cctl_claimstate.id_stg
                                                                         WHERE  cctl_claimstate.name_stg <> ''Draft'' ) cc_claim
                                                           ON     cc_claim.id_stg=subsum.claimid_stg
                                                           join   db_t_prod_stag.cc_subrogation sub
                                                           ON     subsum.id_stg=sub.subrogationsummaryid_stg
                                                           join   db_t_prod_stag.cctl_subroclosedoutcome suboc
                                                           ON     suboc.id_stg = sub.outcome_stg
                                                           WHERE  (
                                                                         subsum.updatetime_stg > (:start_dttm)
                                                                  AND    subsum.updatetime_stg <= (:end_dttm))
                                                           OR     (
                                                                         sub.updatetime_stg > (:start_dttm)
                                                                  AND    sub.updatetime_stg <= (:end_dttm)) )src ) src ) );
  -- Component exp_ADD_PRCS_ID, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_add_prcs_id AS
  (
         SELECT sq_cc_matter.publicid                AS publicid,
                sq_cc_matter.legl_actn_typecode      AS legl_actn_typecode,
                sq_cc_matter.src_sys_cd              AS src_sys_cd,
                sq_cc_matter.legl_actn_otcm_typecode AS legl_actn_otcm_typecode,
                sq_cc_matter.legl_actn_fnl_cst       AS legl_actn_fnl_cst,
                sq_cc_matter.updatetime              AS updatetime,
                :PRCS_ID                             AS prcs_id,
                sq_cc_matter.source_record_id
         FROM   sq_cc_matter );
  -- Component LKP_TERADATA_ETL_REF_XLAT_LEGL_ACTN_OTCM_TYPE, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_legl_actn_otcm_type AS
  (
            SELECT    lkp.tgt_idntftn_val,
                      exp_add_prcs_id.source_record_id,
                      row_number() over(PARTITION BY exp_add_prcs_id.source_record_id ORDER BY lkp.tgt_idntftn_val DESC,lkp.src_idntftn_val DESC) rnk
            FROM      exp_add_prcs_id
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''LEGL_ACTN_OTCM_TYPE''
                             AND    teradata_etl_ref_xlat.src_idntftn_nm IN (''cctl_subroclosedoutcome.typecode'',
                                                                             ''cctl_resolutiontype.typecode'')
                             AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = exp_add_prcs_id.legl_actn_otcm_typecode qualify rnk = 1 );
  -- Component LKP_TERADATA_ETL_REF_XLAT_LEGL_ACTN_TYPE, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_legl_actn_type AS
  (
            SELECT    lkp.tgt_idntftn_val,
                      exp_add_prcs_id.source_record_id,
                      row_number() over(PARTITION BY exp_add_prcs_id.source_record_id ORDER BY lkp.tgt_idntftn_val DESC,lkp.src_idntftn_val DESC) rnk
            FROM      exp_add_prcs_id
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''LEGL_ACTN_TYPE''
                                    /* AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''cctl_mattertype.typecode''  */
                                    /* AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''GW''  */
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = exp_add_prcs_id.legl_actn_typecode qualify rnk = 1 );
  -- Component LKP_TERADATA_ETL_REF_XLAT_COUTCOME_CD, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_coutcome_cd AS
  (
            SELECT    lkp.tgt_idntftn_val,
                      exp_add_prcs_id.source_record_id,
                      row_number() over(PARTITION BY exp_add_prcs_id.source_record_id ORDER BY lkp.tgt_idntftn_val DESC,lkp.src_idntftn_val DESC) rnk
            FROM      exp_add_prcs_id
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
                             AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                             AND    teradata_etl_ref_xlat.src_idntftn_sys= ''DS''
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = exp_add_prcs_id.src_sys_cd qualify rnk = 1 );
  -- Component exp_pass_through, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_through AS
  (
             SELECT     exp_add_prcs_id.publicid                                      AS publicid,
                        lkp_teradata_etl_ref_xlat_coutcome_cd.tgt_idntftn_val         AS src_sys_cd,
                        lkp_teradata_etl_ref_xlat_legl_actn_type.tgt_idntftn_val      AS legl_actn_typecode,
                        lkp_teradata_etl_ref_xlat_legl_actn_otcm_type.tgt_idntftn_val AS legl_actn_otcm_typecode,
                        exp_add_prcs_id.legl_actn_fnl_cst                             AS legl_actn_fnl_cst,
                        exp_add_prcs_id.updatetime                                    AS updatetime,
                        exp_add_prcs_id.prcs_id                                       AS prcs_id,
                        exp_add_prcs_id.source_record_id
             FROM       exp_add_prcs_id
             inner join lkp_teradata_etl_ref_xlat_legl_actn_otcm_type
             ON         exp_add_prcs_id.source_record_id = lkp_teradata_etl_ref_xlat_legl_actn_otcm_type.source_record_id
             inner join lkp_teradata_etl_ref_xlat_legl_actn_type
             ON         lkp_teradata_etl_ref_xlat_legl_actn_otcm_type.source_record_id = lkp_teradata_etl_ref_xlat_legl_actn_type.source_record_id
             inner join lkp_teradata_etl_ref_xlat_coutcome_cd
             ON         lkp_teradata_etl_ref_xlat_legl_actn_type.source_record_id = lkp_teradata_etl_ref_xlat_coutcome_cd.source_record_id );
  -- Component LKP_LEGL_ACTN_TABLE, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_legl_actn_table AS
  (
            SELECT    lkp.legl_actn_id,
                      lkp.prcs_id,
                      exp_pass_through.source_record_id,
                      row_number() over(PARTITION BY exp_pass_through.source_record_id ORDER BY lkp.legl_actn_id ASC,lkp.legl_actn_desc ASC,lkp.legl_actn_suit_num ASC,lkp.legl_actn_strt_dttm ASC,lkp.legl_actn_end_dttm ASC,lkp.court_loc_loctr_id ASC,lkp.legl_actn_type_cd ASC,lkp.legl_actn_suit_type_cd ASC,lkp.case_num ASC,lkp.bad_faith_ind ASC,lkp.subrgtn_rltd_ind ASC,lkp.prcs_id ASC,lkp.subrgtn_loan_ind ASC,lkp.wrt_off_amt ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.src_sys_cd ASC) rnk
            FROM      exp_pass_through
            left join
                      (
                               SELECT   legl_actn.legl_actn_id           AS legl_actn_id,
                                        legl_actn.legl_actn_desc         AS legl_actn_desc,
                                        legl_actn.legl_actn_strt_dttm    AS legl_actn_strt_dttm,
                                        legl_actn.legl_actn_end_dttm     AS legl_actn_end_dttm,
                                        legl_actn.court_loc_loctr_id     AS court_loc_loctr_id,
                                        legl_actn.legl_actn_suit_type_cd AS legl_actn_suit_type_cd,
                                        legl_actn.case_num               AS case_num,
                                        legl_actn.bad_faith_ind          AS bad_faith_ind,
                                        legl_actn.subrgtn_rltd_ind       AS subrgtn_rltd_ind,
                                        legl_actn.prcs_id                AS prcs_id,
                                        legl_actn.subrgtn_loan_ind       AS subrgtn_loan_ind,
                                        legl_actn.wrt_off_amt            AS wrt_off_amt,
                                        legl_actn.edw_strt_dttm          AS edw_strt_dttm,
                                        legl_actn.edw_end_dttm           AS edw_end_dttm,
                                        legl_actn.legl_actn_suit_num     AS legl_actn_suit_num,
                                        legl_actn.src_sys_cd             AS src_sys_cd,
                                        legl_actn.legl_actn_type_cd      AS legl_actn_type_cd
                               FROM     db_t_prod_core.legl_actn qualify row_number () over ( PARTITION BY legl_actn_suit_num,src_sys_cd,legl_actn_type_cd ORDER BY edw_end_dttm DESC)=1 ) lkp
            ON        lkp.legl_actn_suit_num = exp_pass_through.publicid
            AND       lkp.src_sys_cd = exp_pass_through.src_sys_cd
            AND       lkp.legl_actn_type_cd = exp_pass_through.legl_actn_typecode qualify rnk = 1 );
  -- Component LKP_LEGL_ACTN_OTCM_CDC, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_legl_actn_otcm_cdc AS
  (
            SELECT    lkp.legl_actn_id,
                      lkp.legl_actn_otcm_cd,
                      lkp.legl_actn_fnl_cst,
                      lkp.edw_strt_dttm,
                      lkp_legl_actn_table.legl_actn_id AS in_legl_actn_id,
                      lkp_legl_actn_table.source_record_id,
                      row_number() over(PARTITION BY lkp_legl_actn_table.source_record_id ORDER BY lkp.legl_actn_id DESC,lkp.legl_actn_otcm_cd DESC,lkp.legl_actn_fnl_cst DESC,lkp.edw_strt_dttm DESC) rnk
            FROM      lkp_legl_actn_table
            left join
                      (
                               SELECT   legl_actn_otcm.legl_actn_id      AS legl_actn_id,
                                        legl_actn_otcm.legl_actn_otcm_cd AS legl_actn_otcm_cd,
                                        legl_actn_otcm.legl_actn_fnl_cst AS legl_actn_fnl_cst,
                                        legl_actn_otcm.edw_strt_dttm     AS edw_strt_dttm
                               FROM     db_t_prod_core.legl_actn_otcm qualify row_number () over (PARTITION BY legl_actn_id ORDER BY edw_end_dttm DESC)=1 ) lkp
            ON        lkp.legl_actn_id = lkp_legl_actn_table.legl_actn_id qualify rnk = 1 );
  -- Component exp, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp AS
  (
             SELECT     lkp_legl_actn_otcm_cdc.in_legl_actn_id   AS legl_actn_id,
                        exp_pass_through.legl_actn_otcm_typecode AS legl_actn_otcm_typecode,
                        exp_pass_through.legl_actn_fnl_cst       AS legl_actn_fnl_cst,
                        lkp_legl_actn_otcm_cdc.edw_strt_dttm     AS lkp_edw_strt_dttm1,
                        CASE
                                   WHEN (
                                                         ltrim ( rtrim ( lkp_legl_actn_otcm_cdc.legl_actn_id ) ) IS NULL
                                              OR         to_char ( ltrim ( rtrim ( lkp_legl_actn_otcm_cdc.legl_actn_id ) ) ) = '''' )
                                   AND        (
                                                         ltrim ( rtrim ( lkp_legl_actn_otcm_cdc.legl_actn_otcm_cd ) ) IS NULL
                                              OR         to_char ( ltrim ( rtrim ( lkp_legl_actn_otcm_cdc.legl_actn_otcm_cd ) ) ) = '''' ) THEN 1
                                   ELSE 0
                        END               AS o_flag,
                        current_timestamp AS edw_strt_dttm,
                        CASE
                                   WHEN exp_pass_through.updatetime IS NULL THEN to_date ( ''01/01/1900'' , ''MM/DD/YYYY'' )
                                   ELSE exp_pass_through.updatetime
                        END AS out_trans_strt_dttm,
                        md5 ( ltrim ( rtrim ( lkp_legl_actn_otcm_cdc.legl_actn_otcm_cd ) )
                                   || ltrim ( rtrim ( lkp_legl_actn_otcm_cdc.legl_actn_fnl_cst ) ) ) AS var_orig_chksm,
                        md5 ( ltrim ( rtrim ( exp_pass_through.legl_actn_otcm_typecode ) )
                                   || ltrim ( rtrim ( exp_pass_through.legl_actn_fnl_cst ) ) ) AS var_calc_chksm,
                        CASE
                                   WHEN var_orig_chksm IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN var_orig_chksm != var_calc_chksm THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                      AS out_ins_upd,
                        exp_pass_through.prcs_id AS prcs_id,
                        exp_pass_through.source_record_id
             FROM       exp_pass_through
             inner join lkp_legl_actn_otcm_cdc
             ON         exp_pass_through.source_record_id = lkp_legl_actn_otcm_cdc.source_record_id );
  -- Component rtr_check_flag_Insert, Type ROUTER Output Group Insert
  create or replace temporary table rtr_check_flag_insert as
    SELECT exp.legl_actn_id            AS legl_actn_id,
         exp.legl_actn_otcm_typecode AS legl_actn_otcm_typecode,
         exp.legl_actn_fnl_cst       AS legl_actn_fnl_cst,
         exp.o_flag                  AS o_flag,
         exp.edw_strt_dttm           AS edw_strt_dttm,
         exp.out_trans_strt_dttm     AS trans_strt_dttm1,
         exp.out_ins_upd             AS out_ins_upd,
         exp.lkp_edw_strt_dttm1      AS lkp_edw_strt_dttm1,
         exp.prcs_id                 AS prcs_id,
         exp.source_record_id
  FROM   exp
  WHERE  exp.out_ins_upd = ''I'' -- exp.o_flag = 1;
  ;
  
  -- Component rtr_check_flag_Update, Type ROUTER Output Group Update
  create or replace temporary table rtr_check_flag_update as
  SELECT exp.legl_actn_id            AS legl_actn_id,
         exp.legl_actn_otcm_typecode AS legl_actn_otcm_typecode,
         exp.legl_actn_fnl_cst       AS legl_actn_fnl_cst,
         exp.o_flag                  AS o_flag,
         exp.edw_strt_dttm           AS edw_strt_dttm,
         exp.out_trans_strt_dttm     AS trans_strt_dttm1,
         exp.out_ins_upd             AS out_ins_upd,
         exp.lkp_edw_strt_dttm1      AS lkp_edw_strt_dttm1,
         exp.prcs_id                 AS prcs_id,
         exp.source_record_id
  FROM   exp
  WHERE  exp.out_ins_upd = ''U'' -- exp.o_flag = 0;
  ;

  -- Component upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_check_flag_update.legl_actn_id            AS legl_actn_id3,
                rtr_check_flag_update.legl_actn_otcm_typecode AS legl_actn_otcm_typecode,
                rtr_check_flag_update.legl_actn_fnl_cst       AS legl_actn_fnl_cst,
                rtr_check_flag_update.lkp_edw_strt_dttm1      AS lkp_edw_strt_dttm13,
                rtr_check_flag_update.trans_strt_dttm1        AS trans_strt_dttm13,
                rtr_check_flag_update.prcs_id                 AS prcs_id3,
                1                                             AS update_strategy_action,
				rtr_check_flag_update.source_record_id
         FROM   rtr_check_flag_update );
  -- Component exp_legl_actn_otcm_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_legl_actn_otcm_upd AS
  (
         SELECT upd.legl_actn_id3                         AS legl_actn_id3,
                upd.lkp_edw_strt_dttm13                   AS lkp_edw_strt_dttm13,
                dateadd(''second'', - 1, upd.trans_strt_dttm13) AS o_trans_end_dttm,
                dateadd(''second'', - 1, current_timestamp)     AS out_edw_end_dttm,
                upd.prcs_id3                              AS prcs_id3,
                upd.source_record_id
         FROM   upd );
  -- Component legl_actn_otcm_insert, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE legl_actn_otcm_insert AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_check_flag_insert.legl_actn_id            AS legl_actn_id1,
                rtr_check_flag_insert.legl_actn_otcm_typecode AS legl_actn_otcm_typecode,
                rtr_check_flag_insert.legl_actn_fnl_cst       AS legl_actn_fnl_cst,
                rtr_check_flag_insert.edw_strt_dttm           AS edw_strt_dttm1,
                rtr_check_flag_insert.trans_strt_dttm1        AS trans_strt_dttm11,
                rtr_check_flag_insert.prcs_id                 AS prcs_id1,
                0                                             AS update_strategy_action,
				rtr_check_flag_insert.source_record_id
         FROM   rtr_check_flag_insert );
  -- Component legl_actn_otcm_ins_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE legl_actn_otcm_ins_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_check_flag_update.legl_actn_id            AS legl_actn_id3,
                rtr_check_flag_update.legl_actn_otcm_typecode AS legl_actn_otcm_typecode,
                rtr_check_flag_update.legl_actn_fnl_cst       AS legl_actn_fnl_cst,
                rtr_check_flag_update.edw_strt_dttm           AS edw_strt_dttm3,
                rtr_check_flag_update.trans_strt_dttm1        AS trans_strt_dttm13,
                rtr_check_flag_update.prcs_id                 AS prcs_id3,
                0                                             AS update_strategy_action,
				rtr_check_flag_update.source_record_id
         FROM   rtr_check_flag_update );
  -- Component exp_legl_actn_otcm_ins_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_legl_actn_otcm_ins_upd AS
  (
         SELECT legl_actn_otcm_ins_upd.legl_actn_id3                                   AS legl_actn_id3,
                legl_actn_otcm_ins_upd.legl_actn_otcm_typecode                         AS legl_actn_otcm_typecode,
                legl_actn_otcm_ins_upd.legl_actn_fnl_cst                               AS legl_actn_fnl_cst,
                legl_actn_otcm_ins_upd.edw_strt_dttm3                                  AS edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                legl_actn_otcm_ins_upd.trans_strt_dttm13                               AS trans_strt_dttm13,
                legl_actn_otcm_ins_upd.prcs_id3                                        AS prcs_id3,
                legl_actn_otcm_ins_upd.source_record_id
         FROM   legl_actn_otcm_ins_upd );
  -- Component LEGL_ACTN_OTCM_upd, Type TARGET
  merge
  INTO         db_t_prod_core.legl_actn_otcm
  USING        exp_legl_actn_otcm_upd
  ON (
                            legl_actn_otcm.legl_actn_id = exp_legl_actn_otcm_upd.legl_actn_id3
               AND          legl_actn_otcm.edw_strt_dttm = exp_legl_actn_otcm_upd.lkp_edw_strt_dttm13)
  WHEN matched THEN
  UPDATE
  SET    legl_actn_id = exp_legl_actn_otcm_upd.legl_actn_id3,
         prcs_id = exp_legl_actn_otcm_upd.prcs_id3,
         edw_strt_dttm = exp_legl_actn_otcm_upd.lkp_edw_strt_dttm13,
         edw_end_dttm = exp_legl_actn_otcm_upd.out_edw_end_dttm,
         trans_end_dttm = exp_legl_actn_otcm_upd.o_trans_end_dttm;
  
  -- Component exp_legl_actn_otcm_insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_legl_actn_otcm_insert AS
  (
         SELECT legl_actn_otcm_insert.legl_actn_id1                                    AS legl_actn_id1,
                legl_actn_otcm_insert.legl_actn_otcm_typecode                          AS legl_actn_otcm_typecode,
                legl_actn_otcm_insert.legl_actn_fnl_cst                                AS legl_actn_fnl_cst,
                legl_actn_otcm_insert.edw_strt_dttm1                                   AS edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                legl_actn_otcm_insert.trans_strt_dttm11                                AS trans_strt_dttm11,
                legl_actn_otcm_insert.prcs_id1                                         AS prcs_id1,
                legl_actn_otcm_insert.source_record_id
         FROM   legl_actn_otcm_insert );
  -- Component LEGL_ACTN_OTCM_ins_upd, Type TARGET
  INSERT INTO db_t_prod_core.legl_actn_otcm
              (
                          legl_actn_id,
                          legl_actn_otcm_cd,
                          legl_actn_fnl_cst,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT exp_legl_actn_otcm_ins_upd.legl_actn_id3           AS legl_actn_id,
         exp_legl_actn_otcm_ins_upd.legl_actn_otcm_typecode AS legl_actn_otcm_cd,
         exp_legl_actn_otcm_ins_upd.legl_actn_fnl_cst       AS legl_actn_fnl_cst,
         exp_legl_actn_otcm_ins_upd.prcs_id3                AS prcs_id,
         exp_legl_actn_otcm_ins_upd.edw_strt_dttm           AS edw_strt_dttm,
         exp_legl_actn_otcm_ins_upd.out_edw_end_dttm        AS edw_end_dttm,
         exp_legl_actn_otcm_ins_upd.trans_strt_dttm13       AS trans_strt_dttm
  FROM   exp_legl_actn_otcm_ins_upd;
  
  -- Component LEGL_ACTN_OTCM_ins, Type TARGET
  INSERT INTO db_t_prod_core.legl_actn_otcm
              (
                          legl_actn_id,
                          legl_actn_otcm_cd,
                          legl_actn_fnl_cst,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT exp_legl_actn_otcm_insert.legl_actn_id1           AS legl_actn_id,
         exp_legl_actn_otcm_insert.legl_actn_otcm_typecode AS legl_actn_otcm_cd,
         exp_legl_actn_otcm_insert.legl_actn_fnl_cst       AS legl_actn_fnl_cst,
         exp_legl_actn_otcm_insert.prcs_id1                AS prcs_id,
         exp_legl_actn_otcm_insert.edw_strt_dttm           AS edw_strt_dttm,
         exp_legl_actn_otcm_insert.out_edw_end_dttm        AS edw_end_dttm,
         exp_legl_actn_otcm_insert.trans_strt_dttm11       AS trans_strt_dttm
  FROM   exp_legl_actn_otcm_insert;

END;
';