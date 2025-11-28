-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PLCY_MTRC_GL_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component LKP_TERADATA_ETL_REF_XLAT_INSRNC_MTRC, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_insrnc_mtrc AS
  (
         SELECT src_idntftn_val AS src_idntftn_val,
                tgt_idntftn_val AS tgt_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  tgt_idntftn_nm= ''INSRNC_MTRC_TYPE''
         AND    src_idntftn_nm= ''DERIVED''
         AND    src_idntftn_sys=''DS''
         AND    expn_dt=''9999-12-31'' );
  -- PIPELINE START FOR 1
  -- Component sq_gw_premium_trans, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_gw_premium_trans AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS policy_nbr,
                $2 AS policy_term_nbr,
                $3 AS policy_model_nbr,
                $4 AS trns_writ_prem_amt,
                $5 AS trns_earn_prem_amt,
                $6 AS trns_unearn_prem_amt,
                $7 AS gl_extr_mo,
                $8 AS gl_extr_yr,
                $9 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   policy_nbr,
                                                    policy_term_nbr,
                                                    policy_model_nbr,
                                                    SUM(premium_trans_amt)    AS trns_writ_prem_amt,
                                                    SUM(trns_earn_prem_amt)   AS trns_earn_prem_amt,
                                                    SUM(trns_unearn_prem_amt) AS trns_unearn_prem_amt,
                                                    gl_extr_mo,
                                                    gl_extr_yr
                                           FROM     db_t_prod_stag.gw_premium_trans
                                           WHERE    bus_act_trns_act <> ''adv''
                                           GROUP BY 1,
                                                    2,
                                                    3,
                                                    7,
                                                    8 ) src ) );
  -- Component exp_data_cleansing, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_cleansing AS
  (
            SELECT    ltrim ( rtrim ( sq_gw_premium_trans.policy_nbr ) ) AS out_policy_nbr,
                      sq_gw_premium_trans.policy_term_nbr                AS policy_term_nbr,
                      sq_gw_premium_trans.policy_model_nbr               AS policy_model_nbr,
                      lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INSRNC_MTRC */
                                                             AS out_insrnc_mtrc_type_cd_writ,
                      sq_gw_premium_trans.trns_writ_prem_amt AS out_trns_writ_prem_amt,
                      lkp_2.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INSRNC_MTRC */
                                                             AS out_insrnc_mtrc_type_cd_earn,
                      sq_gw_premium_trans.trns_earn_prem_amt AS out_trns_earn_prem_amt,
                      lkp_3.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_INSRNC_MTRC */
                                                               AS out_insrnc_mtrc_type_cd_unearn,
                      sq_gw_premium_trans.trns_unearn_prem_amt AS out_trns_unearn_prem_amt,
                      sq_gw_premium_trans.gl_extr_mo           AS gl_extr_mo,
                      sq_gw_premium_trans.gl_extr_yr           AS gl_extr_yr,
                      sq_gw_premium_trans.source_record_id,
                      row_number() over (PARTITION BY sq_gw_premium_trans.source_record_id ORDER BY sq_gw_premium_trans.source_record_id) AS rnk
            FROM      sq_gw_premium_trans
            left join lkp_teradata_etl_ref_xlat_insrnc_mtrc lkp_1
            ON        lkp_1.src_idntftn_val = ''INSRNC_MTRC_TYPE18''
            left join lkp_teradata_etl_ref_xlat_insrnc_mtrc lkp_2
            ON        lkp_2.src_idntftn_val = ''INSRNC_MTRC_TYPE19''
            left join lkp_teradata_etl_ref_xlat_insrnc_mtrc lkp_3
            ON        lkp_3.src_idntftn_val = ''INSRNC_MTRC_TYPE20'' qualify rnk = 1 );
  -- Component LKP_AGMT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt AS
  (
            SELECT    lkp.agmt_id,
                      exp_data_cleansing.source_record_id,
                      row_number() over(PARTITION BY exp_data_cleansing.source_record_id ORDER BY lkp.agmt_id ASC) rnk
            FROM      exp_data_cleansing
            left join
                      (
                               SELECT   agmt_id       AS agmt_id,
                                        host_agmt_num AS host_agmt_num,
                                        term_num      AS term_num,
                                        modl_num      AS modl_num
                               FROM     db_t_prod_core.agmt
                               WHERE    agmt_type_cd = trim(''$p_agmt_type_cd_policy_version'')
                               GROUP BY 1,
                                        2,
                                        3,
                                        4 ) lkp
            ON        lkp.host_agmt_num = exp_data_cleansing.out_policy_nbr
            AND       lkp.term_num = exp_data_cleansing.policy_term_nbr
            AND       lkp.modl_num = exp_data_cleansing.policy_model_nbr qualify rnk = 1 );
  -- Component nrm_gl_erned_unernd_written, Type NORMALIZER
 /* CREATE
  OR
  replace TEMPORARY TABLE nrm_gl_erned_unernd_written AS
  (
         SELECT agmt_id_in    AS agmt_id,
                gl_extr_mo_in AS gl_extr_mo,
                gl_extr_yr_in AS gl_extr_yr,
                *
         FROM   (
                          -- start of inner SQL 
                          SELECT    lkp_agmt.agmt_id                                  AS agmt_id_in,
                                    exp_data_cleansing.out_insrnc_mtrc_type_cd_writ   AS insrnc_mtrc_type_cd_in1,
                                    exp_data_cleansing.out_insrnc_mtrc_type_cd_earn   AS insrnc_mtrc_type_cd_in2,
                                    exp_data_cleansing.out_insrnc_mtrc_type_cd_unearn AS insrnc_mtrc_type_cd_in3,
                                    exp_data_cleansing.out_trns_writ_prem_amt         AS trns_prem_amt_in1,
                                    exp_data_cleansing.out_trns_earn_prem_amt         AS trns_prem_amt_in2,
                                    exp_data_cleansing.out_trns_unearn_prem_amt       AS trns_prem_amt_in3,
                                    exp_data_cleansing.gl_extr_mo                     AS gl_extr_mo_in,
                                    exp_data_cleansing.gl_extr_yr                     AS gl_extr_yr_in,
                                    exp_data_cleansing.source_record_id
                          FROM      exp_data_cleansing
                          left join lkp_agmt
                          ON        exp_data_cleansing.source_record_id = lkp_agmt.source_record_id
                                    -- end of inner SQL 
                ) unpivot((insrnc_mtrc_type_cd,trns_prem_amt)) FOR rec_no IN ((insrnc_mtrc_type_cd_in1,
                                                                               trns_prem_amt_in1)                         AS rec1,
                                                                              (insrnc_mtrc_type_cd_in2,trns_prem_amt_in2) AS rec2,
                                                                             (insrnc_mtrc_type_cd_in3,trns_prem_amt_in3) AS rec3) unpivot_tbl );
 */

  CREATE OR REPLACE TEMPORARY TABLE nrm_gl_erned_unernd_written AS

WITH base AS (
  SELECT
    lkp_agmt.agmt_id               AS agmt_id_in,
    exp.out_insrnc_mtrc_type_cd_writ   AS insrnc_mtrc_type_cd_in1,
    exp.out_insrnc_mtrc_type_cd_earn   AS insrnc_mtrc_type_cd_in2,
    exp.out_insrnc_mtrc_type_cd_unearn AS insrnc_mtrc_type_cd_in3,
    exp.out_trns_writ_prem_amt         AS trns_prem_amt_in1,
    exp.out_trns_earn_prem_amt         AS trns_prem_amt_in2,
    exp.out_trns_unearn_prem_amt       AS trns_prem_amt_in3,
    exp.gl_extr_mo                     AS gl_extr_mo_in,
    exp.gl_extr_yr                     AS gl_extr_yr_in,
    exp.source_record_id
  FROM exp_data_cleansing exp
  LEFT JOIN lkp_agmt ON exp.source_record_id = lkp_agmt.source_record_id
)

SELECT
  agmt_id_in   AS agmt_id,
  gl_extr_mo_in AS gl_extr_mo,
  gl_extr_yr_in AS gl_extr_yr,
  ''WRIT'' AS rec_type,
  insrnc_mtrc_type_cd_in1 AS insrnc_mtrc_type_cd,
  trns_prem_amt_in1 AS trns_prem_amt,
  source_record_id
FROM base

UNION ALL

SELECT
  agmt_id_in, gl_extr_mo_in, gl_extr_yr_in,
  ''EARN'',
  insrnc_mtrc_type_cd_in2,
  trns_prem_amt_in2,
  source_record_id
FROM base

UNION ALL

SELECT
  agmt_id_in, gl_extr_mo_in, gl_extr_yr_in,
  ''UNEAR'',
  insrnc_mtrc_type_cd_in3,
  trns_prem_amt_in3,
  source_record_id
FROM base;

  
  
  -- Component exp_data_cleansing1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_cleansing1 AS
  (
         SELECT nrm_gl_erned_unernd_written.agmt_id                                                                                                            AS agmt_id,
                nrm_gl_erned_unernd_written.insrnc_mtrc_type_cd                                                                                                AS insrnc_mtrc_type_cd,
                nrm_gl_erned_unernd_written.trns_prem_amt                                                                                                      AS trns_prem_amt,
                to_date ( to_char ( ( nrm_gl_erned_unernd_written.gl_extr_yr * 10000 ) + ( nrm_gl_erned_unernd_written.gl_extr_mo * 100 ) + 1 ) , ''yyyymmdd'' ) AS var_gl_start_dt,
                var_gl_start_dt                                                                                                                                AS out_gl_start_dttm,
                dateadd (second, -1, dateadd ( month, 1, var_gl_start_dt) )                                                                     AS out_gl_end_dttm,
                nrm_gl_erned_unernd_written.source_record_id
         FROM   nrm_gl_erned_unernd_written );
  -- Component LKP_PLCY_MTRC, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_plcy_mtrc AS
  (
            SELECT    lkp.agmt_id,
                      lkp.insrnc_mtrc_type_cd,
                      lkp.plcy_mtrc_strt_dttm,
                      lkp.plcy_mtrc_end_dttm,
                      lkp.plcy_amt,
                      exp_data_cleansing1.source_record_id,
                      row_number() over(PARTITION BY exp_data_cleansing1.source_record_id ORDER BY lkp.agmt_id ASC,lkp.insrnc_mtrc_type_cd ASC,lkp.plcy_mtrc_strt_dttm ASC,lkp.plcy_mtrc_end_dttm ASC,lkp.plcy_amt ASC) rnk
            FROM      exp_data_cleansing1
            left join
                      (
                             SELECT agmt_id             AS agmt_id,
                                    insrnc_mtrc_type_cd AS insrnc_mtrc_type_cd,
                                    plcy_mtrc_strt_dttm AS plcy_mtrc_strt_dttm,
                                    plcy_mtrc_end_dttm  AS plcy_mtrc_end_dttm,
                                    plcy_amt            AS plcy_amt
                             FROM   db_t_prod_core.plcy_mtrc
                             WHERE  insrnc_mtrc_type_cd IN
                                    (
                                           SELECT tgt_idntftn_val
                                           FROM   db_t_prod_core.teradata_etl_ref_xlat
                                           WHERE  tgt_idntftn_nm= ''INSRNC_MTRC_TYPE''
                                           AND    src_idntftn_val IN (''INSRNC_MTRC_TYPE18'',
                                                                      ''INSRNC_MTRC_TYPE19'',
                                                                      ''INSRNC_MTRC_TYPE20'')
                                           AND    src_idntftn_nm= ''DERIVED''
                                           AND    src_idntftn_sys=''DS''
                                           AND    expn_dt=''9999-12-31'')
                             AND    plcy_mtrc_strt_dttm<plcy_mtrc_end_dttm ) lkp
            ON        lkp.agmt_id = exp_data_cleansing1.agmt_id
            AND       lkp.insrnc_mtrc_type_cd = exp_data_cleansing1.insrnc_mtrc_type_cd
            AND       lkp.plcy_amt = exp_data_cleansing1.trns_prem_amt
            AND       lkp.plcy_mtrc_strt_dttm = exp_data_cleansing1.out_gl_start_dttm
            AND       lkp.plcy_mtrc_end_dttm = exp_data_cleansing1.out_gl_end_dttm qualify rnk = 1 );
  -- Component exp_assign_defaults, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_assign_defaults AS
  (
             SELECT     exp_data_cleansing1.agmt_id             AS agmt_id,
                        exp_data_cleansing1.insrnc_mtrc_type_cd AS insrnc_mtrc_type_cd,
                        exp_data_cleansing1.trns_prem_amt       AS trns_prem_amt,
                        exp_data_cleansing1.out_gl_start_dttm   AS gl_start_dttm,
                        exp_data_cleansing1.out_gl_end_dttm     AS gl_end_dttm,
                        decode ( TRUE ,
                                exp_data_cleansing1.agmt_id IS NULL , ''R'' ,
                                lkp_plcy_mtrc.agmt_id IS NULL , ''I'' ,
                                NOT (
                                           lkp_plcy_mtrc.agmt_id IS NULL )
                     AND        lkp_plcy_mtrc.plcy_amt <> exp_data_cleansing1.trns_prem_amt , ''U'' ,
                                ''R'' )                                                         AS out_flg,
                        current_timestamp                                                     AS out_session_start_dttm,
                        to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS out_end_dttm,
                        ''UNK''                                                                 AS out_unknown,
                        $prcs_id                                                              AS out_prcs_id,
                        exp_data_cleansing1.source_record_id
             FROM       exp_data_cleansing1
             inner join lkp_plcy_mtrc
             ON         exp_data_cleansing1.source_record_id = lkp_plcy_mtrc.source_record_id );
  -- Component fil_rejects, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE fil_rejects AS
  (
         SELECT exp_assign_defaults.agmt_id                AS agmt_id,
                exp_assign_defaults.insrnc_mtrc_type_cd    AS insrnc_mtrc_type_cd,
                exp_assign_defaults.trns_prem_amt          AS trns_prem_amt,
                exp_assign_defaults.gl_start_dttm          AS gl_start_dttm,
                exp_assign_defaults.gl_end_dttm            AS gl_end_dttm,
                exp_assign_defaults.out_flg                AS flg,
                exp_assign_defaults.out_session_start_dttm AS session_start_dttm,
                exp_assign_defaults.out_end_dttm           AS end_dttm,
                exp_assign_defaults.out_unknown            AS unknown,
                exp_assign_defaults.out_prcs_id            AS prcs_id,
                exp_assign_defaults.source_record_id
         FROM   exp_assign_defaults
         WHERE  (
                       exp_assign_defaults.out_flg = ''I''
                OR     exp_assign_defaults.out_flg = ''U'' )
         AND    exp_assign_defaults.trns_prem_amt <> 0 );
  -- Component PLCY_MTRC, Type TARGET
  INSERT INTO db_t_prod_core.plcy_mtrc
              (
                          agmt_id,
                          insrnc_mtrc_type_cd,
                          plcy_mtrc_strt_dttm,
                          plcy_mtrc_end_dttm,
                          tm_prd_cd,
                          plcy_amt,
                          uom_cd,
                          uom_type_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT fil_rejects.agmt_id             AS agmt_id,
         fil_rejects.insrnc_mtrc_type_cd AS insrnc_mtrc_type_cd,
         fil_rejects.gl_start_dttm       AS plcy_mtrc_strt_dttm,
         fil_rejects.gl_end_dttm         AS plcy_mtrc_end_dttm,
         fil_rejects.unknown             AS tm_prd_cd,
         fil_rejects.trns_prem_amt       AS plcy_amt,
         fil_rejects.unknown             AS uom_cd,
         fil_rejects.unknown             AS uom_type_cd,
         fil_rejects.prcs_id             AS prcs_id,
         fil_rejects.session_start_dttm  AS edw_strt_dttm,
         fil_rejects.end_dttm            AS edw_end_dttm
  FROM   fil_rejects;
  
  -- PIPELINE END FOR 1
  -- PIPELINE START FOR 2
  -- Component sq_gw_premium_trans1, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_gw_premium_trans1 AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS policy_nbr,
                $2 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT gw_premium_trans.policy_nbr AS policy_nbr
                                         FROM   db_t_prod_stag.gw_premium_trans
                                         WHERE  1=2 ) src ) );
  -- Component PLCY_MTRC_post_sql, Type TARGET
  INSERT INTO db_t_prod_core.plcy_mtrc
              (
                          agmt_id
              )
  SELECT sq_gw_premium_trans1.policy_nbr AS agmt_id
  FROM   sq_gw_premium_trans1;
  
  -- PIPELINE END FOR 2
  -- Component PLCY_MTRC_post_sql, Type Post SQL
  UPDATE db_t_prod_core.plcy_mtrc tgt
  SET    plcy_mtrc_end_dttm = new_plcy_mtrc_end_dttm,
         edw_end_dttm = new_edw_end_dttm
  FROM    
         (
                    SELECT     agmt_id ,
                               insrnc_mtrc_type_cd,
                               plcy_mtrc_strt_dttm ,
                               edw_strt_dttm,
                               CASE
                                          WHEN row_number() over(PARTITION BY agmt_id , insrnc_mtrc_type_cd, plcy_mtrc_strt_dttm ORDER BY edw_strt_dttm DESC) <> 1 THEN plcy_mtrc_strt_dttm - interval ''0.000001 second''
                                          ELSE plcy_mtrc_end_dttm
                               END new_plcy_mtrc_end_dttm,
                               --case when row_number() over(partition by agmt_id , insrnc_mtrc_type_cd order by plcy_mtrc_strt_dttm desc,edw_strt_dttm desc) <> 1 then edw_end_dttm else
                               max(edw_strt_dttm) over(PARTITION BY agmt_id , insrnc_mtrc_type_cd ORDER BY plcy_mtrc_strt_dttm,edw_strt_dttm ASC ROWS BETWEEN 1 following AND        1 following) - interval ''0.000001 second'' AS new_edw_end_dttm
                    FROM       db_t_prod_core.plcy_mtrc                                                                                                                                                                                       AS p
                    inner join db_t_prod_core.teradata_etl_ref_xlat                                                                                                                                                                           AS t
                    ON         p.insrnc_mtrc_type_cd = t.tgt_idntftn_val
                    WHERE      t.tgt_idntftn_nm= ''INSRNC_MTRC_TYPE''
                    AND        t.src_idntftn_val IN (''INSRNC_MTRC_TYPE18'',
                                                     ''INSRNC_MTRC_TYPE19'',
                                                     ''INSRNC_MTRC_TYPE20'')
                    AND        t.src_idntftn_nm= ''DERIVED''
                    AND        t.src_idntftn_sys=''DS''
                    AND        t.expn_dt=''9999-12-31''
                               --and p.agmt_id=''756009''
                               qualify new_plcy_mtrc_end_dttm <> plcy_mtrc_end_dttm
                    OR         new_edw_end_dttm <> edw_end_dttm ) AS updt
  WHERE  tgt.agmt_id = updt.agmt_id
  AND    tgt.insrnc_mtrc_type_cd = updt.insrnc_mtrc_type_cd
  AND    tgt.plcy_mtrc_strt_dttm = updt.plcy_mtrc_strt_dttm
  AND    tgt.edw_strt_dttm = updt.edw_strt_dttm
  AND    tgt.edw_end_dttm=to_date(''9999-12-31'',''yyyy-mm-dd'');

END;
';