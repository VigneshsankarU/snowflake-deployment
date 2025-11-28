-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PLCY_ASSET_CVGE_MTRC_GL_INSUPD("RUN_ID" VARCHAR)
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
                $1  AS policy_nbr,
                $2  AS policy_term_nbr,
                $3  AS policy_model_nbr,
                $4  AS unit_reference_nbr,
                $5  AS unit_type_cd,
                $6  AS cov_type_cd,
                $7  AS trns_writ_prem_amt,
                $8  AS trns_earn_prem_amt,
                $9  AS trns_unearn_prem_amt,
                $10 AS gl_extr_mo,
                $11 AS gl_extr_yr,
                $12 AS feat_coverage_type,
                $13 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   gw.policy_nbr,
                                                    gw.policy_term_nbr,
                                                    gw.policy_model_nbr,
                                                    gw.unit_reference_nbr,
                                                    gw.unit_type_cd,
                                                    gw.cov_type_cd,
                                                    SUM(gw.premium_trans_amt)    AS trns_writ_prem_amt,
                                                    SUM(gw.trns_earn_prem_amt)   AS trns_earn_prem_amt,
                                                    SUM(gw.trns_unearn_prem_amt) AS trns_unearn_prem_amt,
                                                    gw.gl_extr_mo,
                                                    gw.gl_extr_yr,
                                                    f.feat_coverable_type_txt
                                                    /* EIM-49319 Added as part of Farm */
                                           FROM     db_t_prod_stag.gw_premium_trans gw
                                           join     db_t_prod_core.feat f
                                           ON       f.nk_src_key= gw.cov_type_cd
                                                    /* EIM-49319 Added as part of Farm */
                                           WHERE    bus_act_trns_act <> ''adv''
                                           AND      (
                                                             unit_type_cd IN (''ENTITY.DWELLING_HOE'',
                                                                              ''ENTITY.HOLINESCHCOVITEM_A'')
                                                    OR       unit_type_cd NOT LIKE ''ENTITY.%'')
                                           AND      feat_coverable_type_txt NOT IN(''FOPLiability'',
                                                                                   ''FOPFarmownersLine'',
                                                                                   ''FOPBlanket'')
                                                    /* EIM-49319 Added as part of Farm */
                                           GROUP BY 1,
                                                    2,
                                                    3,
                                                    4,
                                                    5,
                                                    6,
                                                    10,
                                                    11,
                                                    12 ) src ) );
  -- Component exp_data_cleansing, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_cleansing AS
  (
            SELECT    ltrim ( rtrim ( sq_gw_premium_trans.policy_nbr ) )         AS out_policy_nbr,
                      sq_gw_premium_trans.policy_term_nbr                        AS policy_term_nbr,
                      sq_gw_premium_trans.policy_model_nbr                       AS policy_model_nbr,
                      ltrim ( rtrim ( sq_gw_premium_trans.unit_reference_nbr ) ) AS out_unit_reference_nbr,
                      decode ( TRUE ,
                              ltrim ( rtrim ( sq_gw_premium_trans.unit_type_cd ) ) = ''ENTITY.DWELLING_HOE''
                    OR        ltrim ( rtrim ( sq_gw_premium_trans.unit_type_cd ) ) = ''ENTITY.HOLINESCHCOVITEM_A'' , ltrim ( rtrim ( sq_gw_premium_trans.unit_type_cd ) ) ,
                              substr ( ltrim ( rtrim ( sq_gw_premium_trans.unit_type_cd ) ) , 1 , 3 ) = ''FOP'' , sq_gw_premium_trans.feat_coverage_type ,
                              ''PA'' )                                      AS out_unit_type_cd,
                      ltrim ( rtrim ( sq_gw_premium_trans.cov_type_cd ) ) AS out_cov_type_cd,
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
                               FROM     db_t_prod_core.agmt agmt
                               WHERE    edw_end_dttm=''9999-12-31 23:59:59.999999''
                               AND      trim(agmt_type_cd) = ''PPV''
                               GROUP BY 1,
                                        2,
                                        3,
                                        4 ) lkp
            ON        lkp.host_agmt_num = exp_data_cleansing.out_policy_nbr
            AND       lkp.term_num = exp_data_cleansing.policy_term_nbr
            AND       lkp.modl_num = exp_data_cleansing.policy_model_nbr qualify rnk = 1 );
  -- Component LKP_FEAT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_feat AS
  (
            SELECT    lkp.feat_id,
                      exp_data_cleansing.source_record_id,
                      row_number() over(PARTITION BY exp_data_cleansing.source_record_id ORDER BY lkp.feat_id ASC) rnk
            FROM      exp_data_cleansing
            left join
                      (
                             SELECT feat_id,
                                    nk_src_key
                             FROM   db_t_prod_core.feat ) lkp
            ON        lkp.nk_src_key = exp_data_cleansing.out_cov_type_cd qualify rnk = 1 );
  -- Component LKP_PRTY_ASSET, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_prty_asset AS
  (
            SELECT    lkp.prty_asset_id,
                      exp_data_cleansing.source_record_id,
                      row_number() over(PARTITION BY exp_data_cleansing.source_record_id ORDER BY lkp.prty_asset_id ASC) rnk
            FROM      exp_data_cleansing
            left join
                      (
                               SELECT   prty_asset_id     AS prty_asset_id,
                                        asset_host_id_val AS asset_host_id_val,
                                        CASE
                                                 WHEN prty_asset_sbtype_cd = ''REALDW''
                                                 AND      prty_asset_clasfcn_cd = ''MAIN'' THEN ''ENTITY.DWELLING_HOE''
                                                 WHEN prty_asset_sbtype_cd IN (''REALDW'',
                                                                               ''REALSP'') THEN ''ENTITY.HOLINESCHCOVITEM_A''
                                                 WHEN prty_asset_clasfcn_cd=''FOPDWELL'' THEN ''FOPDwelling''
                                                 WHEN prty_asset_clasfcn_cd=''FOPOUTBLDG'' THEN ''FOPOutbuilding''
                                                 WHEN prty_asset_clasfcn_cd=''FOPFDSD'' THEN ''FOPFeedAndSeed''
                                                 WHEN prty_asset_clasfcn_cd=''FOPMCH'' THEN ''FOPMachinery''
                                                 WHEN prty_asset_clasfcn_cd=''FOPLVSTCK'' THEN ''FOPLivestock''
                                                 WHEN prty_asset_clasfcn_cd=''FOPDWELSP'' THEN ''FOPDwellingScheduleCovItem''
                                                 WHEN prty_asset_clasfcn_cd=''FOPLIABSP'' THEN ''FOPLiabilityScheduleCovItem''
                                                 WHEN prty_asset_clasfcn_cd=''FOPLNSP'' THEN ''FOPFarmownersLineScheduleCovItem''
                                                 WHEN prty_asset_clasfcn_cd=''FOPDWLSPEX'' THEN ''FOPDwellingScheduleExclItem''
                                                 ELSE ''PA''
                                        END AS unit_type_cd
                               FROM     db_t_prod_core.prty_asset prty_asset
                               WHERE    edw_end_dttm=''9999-12-31 23:59:59.999999''
                               AND      (
                                                 prty_asset_sbtype_cd = ''MVEH''
                                        AND      prty_asset_clasfcn_cd = ''MV'')
                               OR       prty_asset_sbtype_cd IN (''REALDW'',
                                                                 ''REALSP'')
                               OR       prty_asset_clasfcn_cd LIKE ''FOP%''
                               GROUP BY 1,
                                        2,
                                        3 ) lkp
            ON        lkp.asset_host_id_val = exp_data_cleansing.out_unit_reference_nbr
            AND       lkp.unit_type_cd = exp_data_cleansing.out_unit_type_cd qualify rnk = 1 );
  -- Component EXPTRANS, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exptrans AS
  (
             SELECT     lkp_agmt.agmt_id                                  AS agmt_id,
                        lkp_prty_asset.prty_asset_id                      AS prty_asset_id,
                        lkp_feat.feat_id                                  AS feat_id,
                        exp_data_cleansing.out_insrnc_mtrc_type_cd_writ   AS insrnc_mtrc_type_cd_in1,
                        exp_data_cleansing.out_insrnc_mtrc_type_cd_earn   AS insrnc_mtrc_type_cd_in2,
                        exp_data_cleansing.out_insrnc_mtrc_type_cd_unearn AS insrnc_mtrc_type_cd_in3,
                        exp_data_cleansing.out_trns_writ_prem_amt         AS trns_prem_amt_in1,
                        exp_data_cleansing.out_trns_earn_prem_amt         AS trns_prem_amt_in2,
                        exp_data_cleansing.out_trns_unearn_prem_amt       AS trns_prem_amt_in3,
                        exp_data_cleansing.gl_extr_mo                     AS gl_extr_mo,
                        exp_data_cleansing.gl_extr_yr                     AS gl_extr_yr,
                        exp_data_cleansing.source_record_id
             FROM       exp_data_cleansing
             inner join lkp_agmt
             ON         exp_data_cleansing.source_record_id = lkp_agmt.source_record_id
             inner join lkp_feat
             ON         lkp_agmt.source_record_id = lkp_feat.source_record_id
             inner join lkp_prty_asset
             ON         lkp_feat.source_record_id = lkp_prty_asset.source_record_id );
  -- Component AGGTRANS, Type AGGREGATOR
  CREATE
  OR
  replace TEMPORARY TABLE aggtrans AS
  (
           SELECT   exptrans.agmt_id                 AS agmt_id,
                    exptrans.prty_asset_id           AS prty_asset_id,
                    exptrans.feat_id                 AS feat_id,
                    exptrans.insrnc_mtrc_type_cd_in1 AS insrnc_mtrc_type_cd_in1,
                    exptrans.insrnc_mtrc_type_cd_in2 AS insrnc_mtrc_type_cd_in2,
                    exptrans.insrnc_mtrc_type_cd_in3 AS insrnc_mtrc_type_cd_in3,
                    min(exptrans.trns_prem_amt_in1)  AS trns_prem_amt_in1,
                    SUM(trns_prem_amt_in1)           AS trns_prem_amt_in1_sum,
                    min(exptrans.trns_prem_amt_in2)  AS trns_prem_amt_in2,
                    SUM(trns_prem_amt_in2)           AS trns_prem_amt_in2_sum,
                    min(exptrans.trns_prem_amt_in3)  AS trns_prem_amt_in3,
                    SUM(trns_prem_amt_in3)           AS trns_prem_amt_in3_sum,
                    min(exptrans.gl_extr_mo)         AS gl_extr_mo,
                    min(exptrans.gl_extr_yr)         AS gl_extr_yr,
                    min(exptrans.source_record_id)   AS source_record_id
           FROM     exptrans
           GROUP BY exptrans.agmt_id,
                    exptrans.prty_asset_id,
                    exptrans.feat_id,
                    exptrans.insrnc_mtrc_type_cd_in1,
                    exptrans.insrnc_mtrc_type_cd_in2,
                    exptrans.insrnc_mtrc_type_cd_in3 );
  -- Component EXPTRANS1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exptrans1 AS
  (
         SELECT aggtrans.agmt_id                 AS agmt_id,
                aggtrans.prty_asset_id           AS prty_asset_id,
                aggtrans.feat_id                 AS feat_id,
                aggtrans.insrnc_mtrc_type_cd_in1 AS insrnc_mtrc_type_cd_in1,
                aggtrans.insrnc_mtrc_type_cd_in2 AS insrnc_mtrc_type_cd_in2,
                aggtrans.insrnc_mtrc_type_cd_in3 AS insrnc_mtrc_type_cd_in3,
                aggtrans.trns_prem_amt_in1_sum   AS trns_prem_amt_in1,
                aggtrans.trns_prem_amt_in2_sum   AS trns_prem_amt_in2,
                aggtrans.trns_prem_amt_in3_sum   AS trns_prem_amt_in3,
                aggtrans.gl_extr_mo              AS gl_extr_mo,
                aggtrans.gl_extr_yr              AS gl_extr_yr,
                aggtrans.source_record_id
         FROM   aggtrans );
  -- Component nrm_gl_erned_unernd_written, Type NORMALIZER
 /* CREATE
  OR
  replace TEMPORARY TABLE nrm_gl_erned_unernd_written AS
  (
         SELECT agmt_id_in       AS agmt_id,
                prty_asset_id_in AS prty_asset_id,
                feat_id_in       AS feat_id,
                gl_extr_mo_in    AS gl_extr_mo,
                gl_extr_yr_in    AS gl_extr_yr,
                *
         FROM   (
                       -- start of inner SQL 
                       SELECT exptrans1.agmt_id                 AS agmt_id_in,
                              exptrans1.prty_asset_id           AS prty_asset_id_in,
                              exptrans1.feat_id                 AS feat_id_in,
                              exptrans1.insrnc_mtrc_type_cd_in1 AS insrnc_mtrc_type_cd_in1,
                              exptrans1.insrnc_mtrc_type_cd_in2 AS insrnc_mtrc_type_cd_in2,
                              exptrans1.insrnc_mtrc_type_cd_in3 AS insrnc_mtrc_type_cd_in3,
                              exptrans1.trns_prem_amt_in1       AS trns_prem_amt_in1,
                              exptrans1.trns_prem_amt_in2       AS trns_prem_amt_in2,
                              exptrans1.trns_prem_amt_in3       AS trns_prem_amt_in3,
                              exptrans1.gl_extr_mo              AS gl_extr_mo_in,
                              exptrans1.gl_extr_yr              AS gl_extr_yr_in,
                              exptrans1.source_record_id
                       FROM   exptrans1
                             --  end of inner SQL 
                ) unpivot((insrnc_mtrc_type_cd,trns_prem_amt)) FOR rec_no IN ((insrnc_mtrc_type_cd_in1,
                                                                               trns_prem_amt_in1)                         AS rec1,
                                                                              (insrnc_mtrc_type_cd_in2,trns_prem_amt_in2) AS rec2,
                                                                              (insrnc_mtrc_type_cd_in3,trns_prem_amt_in3) AS rec3) unpivot_tbl );
*/  
CREATE OR REPLACE TEMPORARY TABLE nrm_gl_erned_unernd_written AS
SELECT
    agmt_id        AS agmt_id,
    prty_asset_id  AS prty_asset_id,
    feat_id        AS feat_id,
    gl_extr_mo     AS gl_extr_mo,
    gl_extr_yr     AS gl_extr_yr,
    ''rec1''         AS rec_no,
    insrnc_mtrc_type_cd_in1 AS insrnc_mtrc_type_cd,
    trns_prem_amt_in1       AS trns_prem_amt,
    source_record_id
FROM exptrans1

UNION ALL

SELECT
    agmt_id,
    prty_asset_id,
    feat_id,
    gl_extr_mo,
    gl_extr_yr,
    ''rec2'' AS rec_no,
    insrnc_mtrc_type_cd_in2,
    trns_prem_amt_in2,
    source_record_id
FROM exptrans1

UNION ALL

SELECT
    agmt_id,
    prty_asset_id,
    feat_id,
    gl_extr_mo,
    gl_extr_yr,
    ''rec3'' AS rec_no,
    insrnc_mtrc_type_cd_in3,
    trns_prem_amt_in3,
    source_record_id
FROM exptrans1;

  
  
  -- Component exp_data_cleansing1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_cleansing1 AS
  (
         SELECT nrm_gl_erned_unernd_written.agmt_id                                                                                                            AS agmt_id,
                nrm_gl_erned_unernd_written.prty_asset_id                                                                                                      AS prty_asset_id,
                nrm_gl_erned_unernd_written.feat_id                                                                                                            AS feat_id,
                nrm_gl_erned_unernd_written.insrnc_mtrc_type_cd                                                                                                AS insrnc_mtrc_type_cd,
                nrm_gl_erned_unernd_written.trns_prem_amt                                                                                                      AS trns_prem_amt,
                to_date ( to_char ( ( nrm_gl_erned_unernd_written.gl_extr_yr * 10000 ) + ( nrm_gl_erned_unernd_written.gl_extr_mo * 100 ) + 1 ) , ''yyyymmdd'' ) AS var_gl_start_dt,
                var_gl_start_dt                                                                                                                                AS out_gl_start_dttm,
                --add_to_date ( add_to_date ( var_gl_start_dt , ''MONTH'' , 1 ) , ''US'' , - 1 )                                                                     AS out_gl_end_dttm,
            DATEADD(DAY, -1, DATEADD(MONTH, 1, var_gl_start_dt))                                                                                               AS out_gl_end_dttm,
                nrm_gl_erned_unernd_written.source_record_id
         FROM   nrm_gl_erned_unernd_written );
  -- Component LKP_PLCY_ASSET_CVGE_MTRC_GL, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_plcy_asset_cvge_mtrc_gl AS
  (
            SELECT    lkp.agmt_id,
                      lkp.prty_asset_id,
                      lkp.feat_id,
                      lkp.insrnc_mtrc_type_cd,
                      lkp.plcy_asset_cvge_mtrc_strt_dttm,
                      lkp.plcy_asset_cvge_mtrc_end_dttm,
                      lkp.plcy_asset_cvge_amt,
                      exp_data_cleansing1.source_record_id,
                      row_number() over(PARTITION BY exp_data_cleansing1.source_record_id ORDER BY lkp.agmt_id ASC,lkp.prty_asset_id ASC,lkp.feat_id ASC,lkp.insrnc_mtrc_type_cd ASC,lkp.plcy_asset_cvge_mtrc_strt_dttm ASC,lkp.plcy_asset_cvge_mtrc_end_dttm ASC,lkp.plcy_asset_cvge_amt ASC) rnk
            FROM      exp_data_cleansing1
            left join
                      (
                             SELECT agmt_id                        AS agmt_id,
                                    prty_asset_id                  AS prty_asset_id,
                                    feat_id                        AS feat_id,
                                    insrnc_mtrc_type_cd            AS insrnc_mtrc_type_cd,
                                    plcy_asset_cvge_mtrc_strt_dttm AS plcy_asset_cvge_mtrc_strt_dttm,
                                    plcy_asset_cvge_mtrc_end_dttm  AS plcy_asset_cvge_mtrc_end_dttm,
                                    plcy_asset_cvge_amt            AS plcy_asset_cvge_amt
                             FROM   db_t_prod_core.plcy_asset_cvge_mtrc plcy_asset_cvge_mtrc
                             WHERE  edw_end_dttm=''9999-12-31 23:59:59.999999''
                             AND    insrnc_mtrc_type_cd IN
                                    (
                                           SELECT tgt_idntftn_val
                                           FROM   db_t_prod_core.teradata_etl_ref_xlat teradata_etl_ref_xlat
                                           WHERE  tgt_idntftn_nm= ''INSRNC_MTRC_TYPE''
                                           AND    src_idntftn_val IN (''INSRNC_MTRC_TYPE18'',
                                                                      ''INSRNC_MTRC_TYPE19'',
                                                                      ''INSRNC_MTRC_TYPE20'')
                                           AND    src_idntftn_nm= ''DERIVED''
                                           AND    src_idntftn_sys=''DS''
                                           AND    expn_dt=''9999-12-31'')
                             AND    plcy_asset_cvge_mtrc_strt_dttm<plcy_asset_cvge_mtrc_end_dttm ) lkp
            ON        lkp.agmt_id = exp_data_cleansing1.agmt_id
            AND       lkp.prty_asset_id = exp_data_cleansing1.prty_asset_id
            AND       lkp.feat_id = exp_data_cleansing1.feat_id
            AND       lkp.insrnc_mtrc_type_cd = exp_data_cleansing1.insrnc_mtrc_type_cd
            AND       lkp.plcy_asset_cvge_amt = exp_data_cleansing1.trns_prem_amt
            AND       lkp.plcy_asset_cvge_mtrc_strt_dttm = exp_data_cleansing1.out_gl_start_dttm
            AND       lkp.plcy_asset_cvge_mtrc_end_dttm = exp_data_cleansing1.out_gl_end_dttm qualify rnk = 1 );
  -- Component exp_assign_defaults, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_assign_defaults AS
  (
             SELECT     exp_data_cleansing1.agmt_id             AS agmt_id,
                        exp_data_cleansing1.prty_asset_id       AS prty_asset_id,
                        exp_data_cleansing1.feat_id             AS feat_id,
                        exp_data_cleansing1.insrnc_mtrc_type_cd AS insrnc_mtrc_type_cd,
                        exp_data_cleansing1.trns_prem_amt       AS trns_prem_amt,
                        exp_data_cleansing1.out_gl_start_dttm   AS gl_start_dttm,
                        exp_data_cleansing1.out_gl_end_dttm     AS gl_end_dttm,
                        decode ( TRUE ,
                                exp_data_cleansing1.agmt_id IS NULL
                     OR         exp_data_cleansing1.prty_asset_id IS NULL
                     OR         exp_data_cleansing1.feat_id IS NULL , ''R'' ,
                                lkp_plcy_asset_cvge_mtrc_gl.agmt_id IS NULL , ''I'' ,
                                NOT (
                                           lkp_plcy_asset_cvge_mtrc_gl.agmt_id IS NULL )
                     AND        lkp_plcy_asset_cvge_mtrc_gl.plcy_asset_cvge_amt <> exp_data_cleansing1.trns_prem_amt , ''U'' ,
                                ''R'' )                                                         AS out_flg,
                        current_timestamp                                                     AS out_session_start_dttm,
                        to_timestamp_ntz ( ''1900-01-01 00:00:00.000000'' , ''YYYY-MM-DD HH24:MI:SS.US'' ) AS out_start_dttm,
                        to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.US'' ) AS out_end_dttm,
                        ''UNK''                                                                 AS out_unknown,
                        $prcs_id                                                              AS out_prcs_id,
                        exp_data_cleansing1.source_record_id
             FROM       exp_data_cleansing1
             inner join lkp_plcy_asset_cvge_mtrc_gl
             ON         exp_data_cleansing1.source_record_id = lkp_plcy_asset_cvge_mtrc_gl.source_record_id );
  -- Component fil_rejects, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE fil_rejects AS
  (
         SELECT exp_assign_defaults.agmt_id                AS agmt_id,
                exp_assign_defaults.prty_asset_id          AS prty_asset_id,
                exp_assign_defaults.feat_id                AS feat_id,
                exp_assign_defaults.insrnc_mtrc_type_cd    AS insrnc_mtrc_type_cd,
                exp_assign_defaults.trns_prem_amt          AS trns_prem_amt,
                exp_assign_defaults.gl_start_dttm          AS gl_start_dttm,
                exp_assign_defaults.gl_end_dttm            AS gl_end_dttm,
                exp_assign_defaults.out_flg                AS flg,
                exp_assign_defaults.out_session_start_dttm AS session_start_dttm,
                exp_assign_defaults.out_start_dttm         AS start_dttm,
                exp_assign_defaults.out_end_dttm           AS end_dttm,
                exp_assign_defaults.out_unknown            AS unknown,
                exp_assign_defaults.out_prcs_id            AS prcs_id,
                exp_assign_defaults.source_record_id
         FROM   exp_assign_defaults
         WHERE  (
                       exp_assign_defaults.out_flg = ''I''
                OR     exp_assign_defaults.out_flg = ''U'' )
         AND    exp_assign_defaults.trns_prem_amt <> 0 );
  -- Component PLCY_ASSET_CVGE_MTRC, Type TARGET
  INSERT INTO db_t_prod_core.plcy_asset_cvge_mtrc
              (
                          feat_id,
                          agmt_asset_feat_strt_dttm,
                          asset_cntrct_role_sbtype_cd,
                          prty_asset_id,
                          agmt_asset_strt_dttm,
                          agmt_id,
                          insrnc_mtrc_type_cd,
                          plcy_asset_cvge_mtrc_strt_dttm,
                          plcy_asset_cvge_mtrc_end_dttm,
                          tm_prd_cd,
                          plcy_asset_cvge_amt,
                          uom_cd,
                          cury_cd,
                          uom_type_cd,
                          nk_src_key,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT fil_rejects.feat_id             AS feat_id,
         fil_rejects.start_dttm          AS agmt_asset_feat_strt_dttm,
         fil_rejects.unknown             AS asset_cntrct_role_sbtype_cd,
         fil_rejects.prty_asset_id       AS prty_asset_id,
         fil_rejects.start_dttm          AS agmt_asset_strt_dttm,
         fil_rejects.agmt_id             AS agmt_id,
         fil_rejects.insrnc_mtrc_type_cd AS insrnc_mtrc_type_cd,
         fil_rejects.gl_start_dttm       AS plcy_asset_cvge_mtrc_strt_dttm,
         fil_rejects.gl_end_dttm         AS plcy_asset_cvge_mtrc_end_dttm,
         fil_rejects.unknown             AS tm_prd_cd,
         fil_rejects.trns_prem_amt       AS plcy_asset_cvge_amt,
         fil_rejects.unknown             AS uom_cd,
         fil_rejects.unknown             AS cury_cd,
         fil_rejects.unknown             AS uom_type_cd,
         fil_rejects.unknown             AS nk_src_key,
         fil_rejects.prcs_id             AS prcs_id,
         fil_rejects.session_start_dttm  AS edw_strt_dttm,
         fil_rejects.end_dttm            AS edw_end_dttm,
         fil_rejects.gl_start_dttm       AS trans_strt_dttm,
         fil_rejects.gl_end_dttm         AS trans_end_dttm
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
                                           SELECT   gw_premium_trans.policy_nbr AS policy_nbr
                                           FROM     db_t_prod_stag.gw_premium_trans
                                           WHERE    1=2
                                           ORDER BY 1 ) src ) );
  -- Component PLCY_ASSET_CVGE_MTRC_post_sql, Type TARGET
  INSERT INTO db_t_prod_core.plcy_asset_cvge_mtrc
              (
                          feat_id
              )
  SELECT sq_gw_premium_trans1.policy_nbr AS feat_id
  FROM   sq_gw_premium_trans1;
  
  -- PIPELINE END FOR 2
  -- Component PLCY_ASSET_CVGE_MTRC_post_sql, Type Post SQL
  UPDATE db_t_prod_core.plcy_asset_cvge_mtrc as tgt
  SET    plcy_asset_cvge_mtrc_end_dttm = new_plcy_asset_cvge_mtrc_end_dttm,
         edw_end_dttm = new_edw_end_dttm
  FROM   
         (
                    SELECT     agmt_id ,
                               prty_asset_id ,
                               feat_id ,
                               insrnc_mtrc_type_cd,
                               plcy_asset_cvge_mtrc_strt_dttm ,
                               edw_strt_dttm,
                               CASE
                                          WHEN row_number() over(PARTITION BY agmt_id , prty_asset_id, feat_id ,insrnc_mtrc_type_cd, plcy_asset_cvge_mtrc_strt_dttm ORDER BY edw_strt_dttm DESC) <> 1 THEN plcy_asset_cvge_mtrc_strt_dttm - interval ''0.000001 second''
                                          ELSE plcy_asset_cvge_mtrc_end_dttm
                               END new_plcy_asset_cvge_mtrc_end_dttm,
                               --case when row_number() over(partition by agmt_id , prty_asset_id, feat_id ,insrnc_mtrc_type_cd order by plcy_asset_cvge_mtrc_strt_dttm desc,edw_strt_dttm desc) <> 1 then edw_end_dttm else
                               max(edw_strt_dttm) over(PARTITION BY agmt_id , prty_asset_id, feat_id ,insrnc_mtrc_type_cd ORDER BY plcy_asset_cvge_mtrc_strt_dttm,edw_strt_dttm ASC ROWS BETWEEN 1 following AND        1 following) - interval ''0.000001 second'' AS new_edw_end_dttm
                    FROM       db_t_prod_core.plcy_asset_cvge_mtrc                                                                                                                                                                                                        AS p
                    inner join db_t_prod_core.teradata_etl_ref_xlat                                                                                                                                                                                                        AS t
                    ON         p.insrnc_mtrc_type_cd = t.tgt_idntftn_val
                    WHERE      t.tgt_idntftn_nm= ''INSRNC_MTRC_TYPE''
                    AND        t.src_idntftn_val IN (''INSRNC_MTRC_TYPE18'',
                                                     ''INSRNC_MTRC_TYPE19'',
                                                     ''INSRNC_MTRC_TYPE20'')
                    AND        t.src_idntftn_nm= ''DERIVED''
                    AND        t.src_idntftn_sys=''DS''
                    AND        t.expn_dt=''9999-12-31'' qualify new_plcy_asset_cvge_mtrc_end_dttm <> plcy_asset_cvge_mtrc_end_dttm
                    OR         new_edw_end_dttm <> edw_end_dttm ) AS updt

  WHERE  tgt.agmt_id = updt.agmt_id
  AND    tgt.prty_asset_id = updt.prty_asset_id
  AND    tgt.feat_id = updt.feat_id
  AND    tgt.insrnc_mtrc_type_cd = updt.insrnc_mtrc_type_cd
  AND    tgt.plcy_asset_cvge_mtrc_strt_dttm = updt.plcy_asset_cvge_mtrc_strt_dttm
  AND    tgt.edw_strt_dttm = updt.edw_strt_dttm
  AND    tgt.edw_end_dttm=to_date(''9999-12-31'' ,''yyyy-mm-dd'');

END;
';