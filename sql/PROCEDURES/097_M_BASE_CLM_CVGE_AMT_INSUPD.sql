-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_CVGE_AMT_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component SQ_cc_claim, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_cc_claim AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS claimnumber,
                $2  AS transactionamount,
                $3  AS feat_clasfcn_cd,
                $4  AS feature_insurance_subtype_cd,
                $5  AS coveragetype,
                $6  AS lob,
                $7  AS typecode,
                $8  AS feat_sbtype_cd,
                $9  AS clm_src_cd,
                $10 AS retired,
                $11 AS feat_id,
                $12 AS clm_id,
                $13 AS lkp_clm_id,
                $14 AS lkp_cvge_feat_id,
                $15 AS lkp_clm_amt_type_cd,
                $16 AS lkp_clm_cvge_amt_val,
                $17 AS lkp_edw_strt_dttm,
                $18 AS lkp_edw_end_dttm,
                $19 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT          src_mstr.claimnumber,
                                                                  src_mstr.transactionamount,
                                                                  src_mstr.feat_clasfcn_cd,
                                                                  src_mstr.feature_insurance_subtype_cd,
                                                                  src_mstr.coveragetype,
                                                                  src_mstr.lob,
                                                                  src_mstr.typecode,
                                                                  src_mstr.feat_sbtype_cd,
                                                                  src_mstr.clm_src_cd,
                                                                  src_mstr.retired,
                                                                  src_mstr.feat_id,
                                                                  src_mstr.clm_id,
                                                                  tgt_lkp_clm_cvge_amt.clm_id           AS lkp_clm_id,
                                                                  tgt_lkp_clm_cvge_amt.cvge_feat_id     AS lkp_cvge_feat_id,
                                                                  tgt_lkp_clm_cvge_amt.clm_amt_type_cd  AS lkp_clm_amt_type_cd,
                                                                  tgt_lkp_clm_cvge_amt.clm_cvge_amt_val AS lkp_clm_cvge_amt_val,
                                                                  tgt_lkp_clm_cvge_amt.edw_strt_dttm    AS lkp_edw_strt_dttm,
                                                                  tgt_lkp_clm_cvge_amt.edw_end_dttm     AS lkp_edw_end_dttm
                                                  FROM            (
                                                                                  /*select SRC_AGG.ClaimNumber,SUM(TransactionAmount) AS TransactionAmount,SRC_AGG.FEAT_CLASFCN_CD,SRC_AGG.Feature_Insurance_Subtype_Cd,
SRC_AGG.coveragetype,SRC_AGG.LOB,SRC_AGG.TYPECODE,SRC_AGG.feat_sbtype_cd,SRC_AGG.CLM_SRC_CD,SRC_AGG.Retired,SRC_AGG.FEAT_ID,
SRC_AGG.CLM_IDfrom (*/
                                                                                  SELECT          src_xlat.claimnumber,
                                                                                                  src_xlat.transactionamount,
                                                                                                  src_xlat.feat_clasfcn_cd,
                                                                                                  src_xlat.feature_insurance_subtype_cd,
                                                                                                  src_xlat.coveragetype,
                                                                                                  src_xlat.lob,
                                                                                                  src_xlat.typecode,
                                                                                                  src_xlat.feat_sbtype_cd,
                                                                                                  src_xlat.clm_src_cd,
                                                                                                  src_xlat.retired,
                                                                                                  lkp_feat.feat_id AS feat_id,
                                                                                                  lkp_clm.clm_id   AS clm_id
                                                                                  FROM            (
                                                                                                                  SELECT DISTINCT src.claimnumber,
                                                                                                                                  src.transactionamount,
                                                                                                                                  src.feat_clasfcn_cd,
                                                                                                                                  src.feature_insurance_subtype_cd,
                                                                                                                                  src.clausename AS coveragetype,
                                                                                                                                  src.name       AS lob,
                                                                                                                                  src.typecode,
                                                                                                                                  src.feat_sbtype_cd,
                                                                                                                                  lkp_src_cd.tgt_idntftn_val AS clm_src_cd,
                                                                                                                                  src.retired
                                                                                                                  FROM            (
                                                                                                                                         SELECT cc_transactionlineitem.claimnumber,
                                                                                                                                                transactionamount,
                                                                                                                                                cast(''DEDUCTIBLE'' AS VARCHAR(100)) AS feat_clasfcn_cd,
                                                                                                                                                cast(''COVERAGE'' AS   VARCHAR(100)) AS feature_insurance_subtype_cd,
                                                                                                                                                cctl_coveragetype.typecode_stg     AS clausename,
                                                                                                                                                cctl_lobcode.name_stg              AS name,
                                                                                                                                                cctl_transaction.typecode_stg      AS typecode,
                                                                                                                                                cast(''CL'' AS       VARCHAR(50))          AS feat_sbtype_cd,
                                                                                                                                                cast(''SRC_SYS6'' AS VARCHAR(50))          AS src_cd,
                                                                                                                                                cc_transactionlineitem.retired_stg       AS retired
                                                                                                                                         FROM   (
                                                                                                                                                                SELECT          cc_transactionlineitem.publicid_stg,
                                                                                                                                                                                cc_transactionlineitem.retired_stg,
                                                                                                                                                                                cc_transactionlineitem.transactionamount_stg AS transactionamount,
                                                                                                                                                                                cc_transaction.subtype_stg,
                                                                                                                                                                                cc_claim.claimnumber_stg AS claimnumber,
                                                                                                                                                                                cc_coverage.type_stg,
                                                                                                                                                                                cc_claim.lobcode_stg,
                                                                                                                                                                                cc_coverage.deductible_stg
                                                                                                                                                                FROM            db_t_prod_stag.cc_transactionlineitem
                                                                                                                                                                inner join      db_t_prod_stag.cc_transaction
                                                                                                                                                                ON              cc_transactionlineitem.transactionid_stg =cc_transaction.id_stg
                                                                                                                                                                left outer join db_t_prod_stag.cc_check
                                                                                                                                                                ON              cc_check.id_stg = cc_transaction.checkid_stg
                                                                                                                                                                inner join
                                                                                                                                                                                (
                                                                                                                                                                                           SELECT
                                                                                                                                                                                                      /* cc_claim.* */
                                                                                                                                                                                                      cc_claim.id_stg,
                                                                                                                                                                                                      cc_claim.state_stg,
                                                                                                                                                                                                      cc_claim.claimnumber_stg,
                                                                                                                                                                                                      cc_claim.lobcode_stg
                                                                                                                                                                                           FROM       db_t_prod_stag.cc_claim
                                                                                                                                                                                           inner join db_t_prod_stag.cctl_claimstate
                                                                                                                                                                                           ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                                                                                           WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                                                                                ON              cc_claim.id_stg=cc_transaction.claimid_stg
                                                                                                                                                                inner join      db_t_prod_stag.cc_exposure
                                                                                                                                                                ON              cc_transaction.exposureid_stg=cc_exposure.id_stg
                                                                                                                                                                left outer join db_t_prod_stag.cc_coverage
                                                                                                                                                                ON              cc_exposure.coverageid_stg=cc_coverage.id_stg
                                                                                                                                                                WHERE           cc_transactionlineitem.updatetime_stg > ($start_dttm)
                                                                                                                                                                AND             cc_transactionlineitem.updatetime_stg <= ($end_dttm)
                                                                                                                                                                                /*order by cc_transaction.publicid_stg*/
                                                                                                                                                ) cc_transactionlineitem
                                                                                                                                         join   db_t_prod_stag.cctl_coveragetype
                                                                                                                                         ON     cc_transactionlineitem.type_stg=cctl_coveragetype.id_stg
                                                                                                                                         join   db_t_prod_stag.cctl_lobcode
                                                                                                                                         ON     cc_transactionlineitem.lobcode_stg=cctl_lobcode.id_stg
                                                                                                                                         join   db_t_prod_stag.cctl_transaction
                                                                                                                                         ON     cc_transactionlineitem.subtype_stg=cctl_transaction.id_stg
                                                                                                                                         WHERE  cc_transactionlineitem.deductible_stg IS NOT NULL
                                                                                                                                         UNION
                                                                                                                                         SELECT cc_transactionlineitem.claimnumber,
                                                                                                                                                cc_transactionlineitem.transactionamount,
                                                                                                                                                cast(''LIMIT'' AS    VARCHAR(100)) AS feat_clasfcn_cd,
                                                                                                                                                cast(''COVERAGE'' AS VARCHAR(100)) AS feature_insurance_subtype_cd,
                                                                                                                                                cctl_coveragetype.typecode_stg   AS clausename,
                                                                                                                                                cctl_lobcode.name_stg            AS name,
                                                                                                                                                cctl_transaction.typecode_stg    AS typecode,
                                                                                                                                                cast(''CL'' AS       VARCHAR(50))        AS feat_sbtype_cd,
                                                                                                                                                cast(''SRC_SYS6'' AS VARCHAR(50))        AS src_cd,
                                                                                                                                                cc_transactionlineitem.retired_stg     AS retired
                                                                                                                                         FROM   (
                                                                                                                                                                SELECT          cc_transactionlineitem.retired_stg,
                                                                                                                                                                                cc_transactionlineitem.transactionamount_stg AS transactionamount,
                                                                                                                                                                                cc_transaction.subtype_stg,
                                                                                                                                                                                cc_claim.claimnumber_stg AS claimnumber,
                                                                                                                                                                                cc_coverage.type_stg,
                                                                                                                                                                                cc_claim.lobcode_stg,
                                                                                                                                                                                cc_coverage.exposurelimit_stg
                                                                                                                                                                FROM            db_t_prod_stag.cc_transactionlineitem
                                                                                                                                                                inner join      db_t_prod_stag.cc_transaction
                                                                                                                                                                ON              cc_transactionlineitem.transactionid_stg =cc_transaction.id_stg
                                                                                                                                                                inner join      db_t_prod_stag.cctl_transactionstatus
                                                                                                                                                                ON              cctl_transactionstatus.id_stg = cc_transaction.status_stg
                                                                                                                                                                left outer join db_t_prod_stag.cc_check
                                                                                                                                                                ON              cc_check.id_stg = cc_transaction.checkid_stg
                                                                                                                                                                inner join
                                                                                                                                                                                (
                                                                                                                                                                                           SELECT
                                                                                                                                                                                                      /* cc_claim.* */
                                                                                                                                                                                                      cc_claim.id_stg,
                                                                                                                                                                                                      cc_claim.state_stg,
                                                                                                                                                                                                      cc_claim.claimnumber_stg,
                                                                                                                                                                                                      cc_claim.lobcode_stg
                                                                                                                                                                                           FROM       db_t_prod_stag.cc_claim
                                                                                                                                                                                           inner join db_t_prod_stag.cctl_claimstate
                                                                                                                                                                                           ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                                                                                           WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                                                                                                ON              cc_claim.id_stg=cc_transaction.claimid_stg
                                                                                                                                                                inner join      db_t_prod_stag.cc_exposure
                                                                                                                                                                ON              cc_transaction.exposureid_stg=cc_exposure.id_stg
                                                                                                                                                                left outer join db_t_prod_stag.cc_coverage
                                                                                                                                                                ON              cc_exposure.coverageid_stg=cc_coverage.id_stg
                                                                                                                                                                WHERE           cc_transactionlineitem.updatetime_stg > ($start_dttm)
                                                                                                                                                                AND             cc_transactionlineitem.updatetime_stg <= ($end_dttm)
                                                                                                                                                                                /*order by cc_transaction.publicid_stg*/
                                                                                                                                                ) cc_transactionlineitem
                                                                                                                                         join   db_t_prod_stag.cctl_coveragetype
                                                                                                                                         ON     cc_transactionlineitem.type_stg=cctl_coveragetype.id_stg
                                                                                                                                         join   db_t_prod_stag.cctl_lobcode
                                                                                                                                         ON     cc_transactionlineitem.lobcode_stg=cctl_lobcode.id_stg
                                                                                                                                         join   db_t_prod_stag.cctl_transaction
                                                                                                                                         ON     cc_transactionlineitem.subtype_stg=cctl_transaction.id_stg
                                                                                                                                         WHERE  cc_transactionlineitem.exposurelimit_stg IS NOT NULL ) src
                                                                                                                  left outer join
                                                                                                                                  (
                                                                                                                                         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                                                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                                                         FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                                                                                         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
                                                                                                                                         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                                                                                                         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                                                                                                         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) AS lkp_src_cd
                                                                                                                  ON              lkp_src_cd.src_idntftn_val=src.src_cd ) src_xlat
                                                                                  left outer join
                                                                                                  (
                                                                                                           SELECT   clm.clm_id     AS clm_id,
                                                                                                                    clm.clm_num    AS clm_num,
                                                                                                                    clm.src_sys_cd AS src_sys_cd
                                                                                                           FROM     db_t_prod_core.clm qualify row_number() over(PARTITION BY clm.clm_num,clm.src_sys_cd ORDER BY clm.edw_end_dttm DESC) = 1 ) AS lkp_clm
                                                                                  ON              lkp_clm.clm_num=src_xlat.claimnumber
                                                                                  AND             lkp_clm.src_sys_cd=src_xlat.clm_src_cd
                                                                                  left outer join
                                                                                                  (
                                                                                                           SELECT   feat.feat_id        AS feat_id,
                                                                                                                    feat.feat_sbtype_cd AS feat_sbtype_cd,
                                                                                                                    feat.nk_src_key     AS nk_src_key
                                                                                                           FROM     db_t_prod_core.feat qualify row_number () over ( PARTITION BY nk_src_key,feat_sbtype_cd ORDER BY edw_end_dttm DESC)=1 ) AS lkp_feat
                                                                                  ON              lkp_feat.feat_sbtype_cd=src_xlat.feat_sbtype_cd
                                                                                  AND             lkp_feat.nk_src_key=src_xlat.coveragetype
                                                                                                  /* ) AS  SRC_AGG group by 1,3,4,5,6,7,8,9,10,11,12   */
                                                                  ) AS src_mstr
                                                  left outer join
                                                                  (
                                                                           SELECT   clm_cvge_amt.clm_cvge_amt_val AS clm_cvge_amt_val,
                                                                                    clm_cvge_amt.edw_strt_dttm    AS edw_strt_dttm,
                                                                                    clm_cvge_amt.edw_end_dttm     AS edw_end_dttm,
                                                                                    clm_cvge_amt.clm_id           AS clm_id,
                                                                                    clm_cvge_amt.cvge_feat_id     AS cvge_feat_id,
                                                                                    clm_cvge_amt.clm_amt_type_cd  AS clm_amt_type_cd
                                                                           FROM     db_t_prod_core.clm_cvge_amt qualify row_number() over( PARTITION BY clm_cvge_amt.clm_id,clm_cvge_amt.cvge_feat_id, clm_cvge_amt.clm_amt_type_cd ORDER BY clm_cvge_amt.edw_end_dttm DESC) = 1 ) AS tgt_lkp_clm_cvge_amt
                                                  ON              tgt_lkp_clm_cvge_amt.clm_id=src_mstr.clm_id
                                                  AND             tgt_lkp_clm_cvge_amt.cvge_feat_id=src_mstr.feat_id
                                                  AND             tgt_lkp_clm_cvge_amt.clm_amt_type_cd=src_mstr.typecode ) src ) );
  -- Component exp_pass_through, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_through AS
  (
         SELECT sq_cc_claim.transactionamount    AS transactionamount,
                sq_cc_claim.typecode             AS typecode,
                sq_cc_claim.retired              AS retired,
                sq_cc_claim.feat_id              AS feat_id,
                sq_cc_claim.clm_id               AS clm_id,
                sq_cc_claim.lkp_clm_id           AS lkp_clm_id,
                sq_cc_claim.lkp_cvge_feat_id     AS lkp_cvge_feat_id,
                sq_cc_claim.lkp_clm_amt_type_cd  AS lkp_clm_amt_type_cd,
                sq_cc_claim.lkp_clm_cvge_amt_val AS lkp_clm_cvge_amt_val,
                sq_cc_claim.lkp_edw_strt_dttm    AS lkp_edw_strt_dttm,
                sq_cc_claim.lkp_edw_end_dttm     AS lkp_edw_end_dttm,
                sq_cc_claim.source_record_id
         FROM   sq_cc_claim );

  -- Component exp_set_flags, Type EXPRESSION
  CREATE OR replace TEMPORARY TABLE exp_set_flags AS
  (
         SELECT md5 ( trim (ifnull(exp_pass_through.lkp_clm_cvge_amt_val, ''0'') ) ) AS chksum_lkp,
                md5 ( trim (ifnull(exp_pass_through.transactionamount, ''0'')) )      AS chksum_inp,
                CASE
                       WHEN chksum_lkp IS NULL THEN ''I''
                       ELSE
                              CASE
                                     WHEN chksum_lkp != chksum_inp THEN ''U''
                                     ELSE ''R''
                              END
                END                                                                    AS o_flag,
                exp_pass_through.clm_id                                                AS clm_id,
                exp_pass_through.feat_id                                               AS feat_id,
                $prcs_id                                                               AS prcs_id,
                exp_pass_through.transactionamount                                     AS transactionamount,
                exp_pass_through.typecode                                              AS typecode,
                exp_pass_through.lkp_edw_strt_dttm                                     AS lkp_edw_strt_dttm,
                exp_pass_through.lkp_edw_end_dttm                                      AS lkp_edw_end_dttm,
                current_timestamp                                                      AS edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                dateadd(''second'', - 1, current_timestamp)                                  AS edw_end_dttm_exp,
                exp_pass_through.retired                                               AS retired,
                exp_pass_through.source_record_id
         FROM   exp_pass_through );
  -- Component agg_grp_clm_id, Type AGGREGATOR
  CREATE
  OR
  replace TEMPORARY TABLE agg_grp_clm_id AS
  (
           SELECT   exp_set_flags.clm_id                 AS clm_id,
                    exp_set_flags.feat_id                AS feat_id,
                    min(exp_set_flags.prcs_id)           AS prcs_id,
                    min(exp_set_flags.transactionamount) AS transactionamount,
                    min(exp_set_flags.typecode)          AS typecode,
                    min(exp_set_flags.o_flag)            AS o_flag,
                    min(exp_set_flags.lkp_edw_strt_dttm) AS lkp_edw_strt_dttm,
                    min(exp_set_flags.edw_strt_dttm)     AS edw_strt_dttm,
                    min(exp_set_flags.edw_end_dttm)      AS edw_end_dttm,
                    min(exp_set_flags.edw_end_dttm_exp)  AS edw_end_dttm_exp,
                    min(exp_set_flags.retired)           AS retired,
                    min(exp_set_flags.lkp_edw_end_dttm)  AS lkp_edw_end_dttm,
                    min(exp_set_flags.source_record_id)  AS source_record_id
           FROM     exp_set_flags
           GROUP BY exp_set_flags.clm_id,
                    exp_set_flags.feat_id );
  -- Component rtr_ins_upd_insert, Type ROUTER Output Group insert
  create or replace temporary table rtr_ins_upd_insert as
  SELECT agg_grp_clm_id.clm_id            AS clm_id,
         agg_grp_clm_id.feat_id           AS feat_id,
         agg_grp_clm_id.o_flag            AS o_flag,
         agg_grp_clm_id.prcs_id           AS prcs_id,
         agg_grp_clm_id.transactionamount AS transactionamount,
         agg_grp_clm_id.typecode          AS typecode,
         agg_grp_clm_id.lkp_edw_strt_dttm AS lkp_edw_strt_dttm,
         agg_grp_clm_id.edw_strt_dttm     AS edw_strt_dttm,
         agg_grp_clm_id.edw_end_dttm      AS edw_end_dttm,
         agg_grp_clm_id.edw_end_dttm_exp  AS edw_end_dttm_exp,
         agg_grp_clm_id.retired           AS retired,
         agg_grp_clm_id.lkp_edw_end_dttm  AS lkp_edw_end_dttm,
         agg_grp_clm_id.source_record_id
  FROM   agg_grp_clm_id
  WHERE  agg_grp_clm_id.o_flag = ''I''
  AND    agg_grp_clm_id.clm_id IS NOT NULL
  AND    agg_grp_clm_id.feat_id IS NOT NULL
  OR     (
                agg_grp_clm_id.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
         AND    agg_grp_clm_id.retired = 0 )  -- agg_grp_clm_id.o_flag = ''I''
  AND    agg_grp_clm_id.feat_id IS NOT NULL;
  
  -- Component rtr_ins_upd_retired, Type ROUTER Output Group retired
  create or replace temporary table rtr_ins_upd_retired as
  SELECT agg_grp_clm_id.clm_id            AS clm_id,
         agg_grp_clm_id.feat_id           AS feat_id,
         agg_grp_clm_id.o_flag            AS o_flag,
         agg_grp_clm_id.prcs_id           AS prcs_id,
         agg_grp_clm_id.transactionamount AS transactionamount,
         agg_grp_clm_id.typecode          AS typecode,
         agg_grp_clm_id.lkp_edw_strt_dttm AS lkp_edw_strt_dttm,
         agg_grp_clm_id.edw_strt_dttm     AS edw_strt_dttm,
         agg_grp_clm_id.edw_end_dttm      AS edw_end_dttm,
         agg_grp_clm_id.edw_end_dttm_exp  AS edw_end_dttm_exp,
         agg_grp_clm_id.retired           AS retired,
         agg_grp_clm_id.lkp_edw_end_dttm  AS lkp_edw_end_dttm,
         agg_grp_clm_id.source_record_id
  FROM   agg_grp_clm_id
  WHERE  agg_grp_clm_id.o_flag = ''R''
  AND    agg_grp_clm_id.retired != 0
  AND    agg_grp_clm_id.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component rtr_ins_upd_update, Type ROUTER Output Group update
  create or replace temporary table rtr_ins_upd_update as
  SELECT agg_grp_clm_id.clm_id            AS clm_id,
         agg_grp_clm_id.feat_id           AS feat_id,
         agg_grp_clm_id.o_flag            AS o_flag,
         agg_grp_clm_id.prcs_id           AS prcs_id,
         agg_grp_clm_id.transactionamount AS transactionamount,
         agg_grp_clm_id.typecode          AS typecode,
         agg_grp_clm_id.lkp_edw_strt_dttm AS lkp_edw_strt_dttm,
         agg_grp_clm_id.edw_strt_dttm     AS edw_strt_dttm,
         agg_grp_clm_id.edw_end_dttm      AS edw_end_dttm,
         agg_grp_clm_id.edw_end_dttm_exp  AS edw_end_dttm_exp,
         agg_grp_clm_id.retired           AS retired,
         agg_grp_clm_id.lkp_edw_end_dttm  AS lkp_edw_end_dttm,
         agg_grp_clm_id.source_record_id
  FROM   agg_grp_clm_id
  WHERE  agg_grp_clm_id.o_flag = ''U''
  AND    agg_grp_clm_id.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) -- agg_grp_clm_id.o_flag = ''U''
  AND    agg_grp_clm_id.feat_id IS NOT NULL;
  
  -- Component upd_retired, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_retired AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ins_upd_retired.clm_id            AS clm_id4,
                rtr_ins_upd_retired.feat_id           AS feat_id4,
                rtr_ins_upd_retired.typecode          AS typecode4,
                rtr_ins_upd_retired.lkp_edw_strt_dttm AS lkp_edw_strt_dttm4,
                1                                     AS update_strategy_action,
                source_record_id
         FROM   rtr_ins_upd_retired );
  -- Component upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ins_upd_update.clm_id            AS clm_id3,
                rtr_ins_upd_update.feat_id           AS feat_id3,
                rtr_ins_upd_update.prcs_id           AS prcs_id,
                rtr_ins_upd_update.transactionamount AS transactionamount,
                rtr_ins_upd_update.typecode          AS typecode3,
                rtr_ins_upd_update.lkp_edw_strt_dttm AS lkp_edw_strt_dttm3,
                rtr_ins_upd_update.edw_end_dttm_exp  AS edw_end_dttm_exp3,
                rtr_ins_upd_update.lkp_edw_end_dttm  AS lkp_edw_end_dttm3,
                rtr_ins_upd_update.edw_strt_dttm     AS edw_strt_dttm3,
                1                                    AS update_strategy_action,
                rtr_ins_upd_update.source_record_id
         FROM   rtr_ins_upd_update );
  -- Component upd_insert, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_insert AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ins_upd_update.clm_id            AS clm_id3,
                rtr_ins_upd_update.feat_id           AS feat_id3,
                rtr_ins_upd_update.o_flag            AS o_flag3,
                rtr_ins_upd_update.prcs_id           AS prcs_id3,
                rtr_ins_upd_update.transactionamount AS transactionamount3,
                rtr_ins_upd_update.typecode          AS typecode3,
                rtr_ins_upd_update.lkp_edw_strt_dttm AS lkp_edw_strt_dttm3,
                rtr_ins_upd_update.edw_strt_dttm     AS edw_strt_dttm3,
                rtr_ins_upd_update.edw_end_dttm      AS edw_end_dttm3,
                rtr_ins_upd_update.edw_end_dttm_exp  AS edw_end_dttm_exp3,
                rtr_ins_upd_update.retired           AS retired3,
                rtr_ins_upd_update.lkp_edw_end_dttm  AS lkp_edw_end_dttm3,
                0                                    AS update_strategy_action,
                rtr_ins_upd_update.source_record_id
         FROM   rtr_ins_upd_update );
  -- Component insert, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE
  ins_insert AS
         (
                /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
                SELECT rtr_ins_upd_insert.clm_id            AS clm_id1,
                       rtr_ins_upd_insert.feat_id           AS feat_id1,
                       rtr_ins_upd_insert.o_flag            AS o_flag1,
                       rtr_ins_upd_insert.prcs_id           AS prcs_id1,
                       rtr_ins_upd_insert.transactionamount AS transactionamount1,
                       rtr_ins_upd_insert.typecode          AS typecode1,
                       rtr_ins_upd_insert.lkp_edw_strt_dttm AS lkp_edw_strt_dttm1,
                       rtr_ins_upd_insert.edw_strt_dttm     AS edw_strt_dttm1,
                       rtr_ins_upd_insert.edw_end_dttm      AS edw_end_dttm1,
                       rtr_ins_upd_insert.edw_end_dttm_exp  AS edw_end_dttm_exp1,
                       rtr_ins_upd_insert.retired           AS retired1,
                       rtr_ins_upd_insert.lkp_edw_end_dttm  AS lkp_edw_end_dttm1,
                       0                                    AS update_strategy_action,
                       rtr_ins_upd_insert.source_record_id
                FROM   rtr_ins_upd_insert
         );
  
  -- Component fil_CLM_CVGE_AMT_upd_update, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE fil_clm_cvge_amt_upd_update AS
  (
         SELECT upd.clm_id3            AS clm_id3,
                upd.feat_id3           AS feat_id3,
                upd.prcs_id            AS prcs_id,
                upd.transactionamount  AS transactionamount,
                upd.typecode3          AS typecode3,
                upd.lkp_edw_strt_dttm3 AS lkp_edw_strt_dttm3,
                upd.edw_end_dttm_exp3  AS edw_end_dttm_exp3,
                upd.lkp_edw_end_dttm3  AS lkp_edw_end_dttm3,
                upd.edw_strt_dttm3     AS edw_strt_dttm3,
                upd.source_record_id
         FROM   upd
         WHERE  upd.lkp_edw_end_dttm3 = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
  )
  ;
  -- Component exp_retired, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_retired AS
  (
         SELECT upd_retired.clm_id4            AS clm_id4,
                upd_retired.feat_id4           AS feat_id4,
                upd_retired.typecode4          AS typecode4,
                upd_retired.lkp_edw_strt_dttm4 AS lkp_edw_strt_dttm4,
                current_timestamp              AS edw_end_dttm,
                upd_retired.source_record_id
         FROM   upd_retired
  )
  ;
  -- Component exp_CLM_CVGE_AMT_upd_update, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_clm_cvge_amt_upd_update AS
  (
         SELECT fil_clm_cvge_amt_upd_update.clm_id3                            AS clm_id3,
                fil_clm_cvge_amt_upd_update.feat_id3                           AS feat_id3,
                fil_clm_cvge_amt_upd_update.typecode3                          AS typecode1,
                fil_clm_cvge_amt_upd_update.lkp_edw_strt_dttm3                 AS lkp_edw_strt_dttm3,
                dateadd(''second'', - 1, fil_clm_cvge_amt_upd_update.edw_strt_dttm3) AS edw_end_dttm,
                fil_clm_cvge_amt_upd_update.source_record_id
         FROM   fil_clm_cvge_amt_upd_update
  )
  ;
  -- Component fil_CLM_CVGE_AMT_upd_insert, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE fil_clm_cvge_amt_upd_insert AS
  (
         SELECT upd_insert.clm_id3            AS clm_id3,
                upd_insert.feat_id3           AS feat_id3,
                upd_insert.prcs_id3           AS prcs_id,
                upd_insert.transactionamount3 AS transactionamount,
                upd_insert.typecode3          AS typecode3,
                upd_insert.lkp_edw_strt_dttm3 AS lkp_edw_strt_dttm3,
                upd_insert.edw_end_dttm_exp3  AS edw_end_dttm_exp3,
                upd_insert.lkp_edw_end_dttm3  AS lkp_edw_end_dttm3,
                upd_insert.edw_strt_dttm3     AS edw_strt_dttm3,
                upd_insert.retired3           AS retired3,
                upd_insert.edw_end_dttm3      AS edw_end_dttm3,
                upd_insert.source_record_id
         FROM   upd_insert
         WHERE  upd_insert.retired3 = 0
  )
  ;
  -- Component CLM_CVGE_AMT_upd, Type TARGET
  merge
  INTO         db_t_prod_core.clm_cvge_amt
  USING        exp_clm_cvge_amt_upd_update
  ON (
                            clm_cvge_amt.clm_id = exp_clm_cvge_amt_upd_update.clm_id3
               AND          clm_cvge_amt.cvge_feat_id = exp_clm_cvge_amt_upd_update.feat_id3
               AND          clm_cvge_amt.clm_amt_type_cd = exp_clm_cvge_amt_upd_update.typecode1
               AND          clm_cvge_amt.edw_strt_dttm = exp_clm_cvge_amt_upd_update.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    clm_id = exp_clm_cvge_amt_upd_update.clm_id3,
         cvge_feat_id = exp_clm_cvge_amt_upd_update.feat_id3,
         clm_amt_type_cd = exp_clm_cvge_amt_upd_update.typecode1,
         edw_strt_dttm = exp_clm_cvge_amt_upd_update.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_clm_cvge_amt_upd_update.edw_end_dttm;
  
  -- Component CLM_CVGE_AMT_retired, Type TARGET
  merge
  INTO         db_t_prod_core.clm_cvge_amt
  USING        exp_retired
  ON (
                            clm_cvge_amt.clm_id = exp_retired.clm_id4
               AND          clm_cvge_amt.cvge_feat_id = exp_retired.feat_id4
               AND          clm_cvge_amt.clm_amt_type_cd = exp_retired.typecode4
               AND          clm_cvge_amt.edw_strt_dttm = exp_retired.lkp_edw_strt_dttm4)
  WHEN matched THEN
  UPDATE
  SET    clm_id = exp_retired.clm_id4,
         cvge_feat_id = exp_retired.feat_id4,
         clm_amt_type_cd = exp_retired.typecode4,
         edw_strt_dttm = exp_retired.lkp_edw_strt_dttm4,
         edw_end_dttm = exp_retired.edw_end_dttm;
  
  -- Component exp_clm_cvge_amt_insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_clm_cvge_amt_insert AS
  (
         SELECT ins_insert.clm_id1            AS clm_id1,
                ins_insert.feat_id1           AS feat_id1,
                ins_insert.prcs_id1           AS prcs_id1,
                ins_insert.transactionamount1 AS transactionamount1,
                ins_insert.typecode1          AS typecode1,
                ins_insert.edw_strt_dttm1     AS edw_strt_dttm1,
                CASE
                       WHEN ins_insert.retired1 = 0 THEN ins_insert.edw_end_dttm1
                       ELSE current_timestamp
                END AS edw_end_dttm,
                ins_insert.source_record_id
         FROM  ins_insert );
  -- Component exp_CLM_CVGE_AMT_upd_insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_clm_cvge_amt_upd_insert AS
  (
         SELECT fil_clm_cvge_amt_upd_insert.clm_id3           AS clm_id3,
                fil_clm_cvge_amt_upd_insert.feat_id3          AS feat_id3,
                fil_clm_cvge_amt_upd_insert.prcs_id           AS prcs_id,
                fil_clm_cvge_amt_upd_insert.transactionamount AS transactionamount,
                fil_clm_cvge_amt_upd_insert.typecode3         AS typecode3,
                fil_clm_cvge_amt_upd_insert.edw_strt_dttm3    AS edw_strt_dttm3,
                CASE
                       WHEN fil_clm_cvge_amt_upd_insert.retired3 != 0 THEN current_timestamp
                       ELSE fil_clm_cvge_amt_upd_insert.edw_end_dttm3
                END AS edw_end_dttm,
                fil_clm_cvge_amt_upd_insert.source_record_id
         FROM   fil_clm_cvge_amt_upd_insert );
  -- Component CLM_CVGE_AMT_ins, Type TARGET
  INSERT INTO db_t_prod_core.clm_cvge_amt
              (
                          clm_id,
                          cvge_feat_id,
                          clm_amt_type_cd,
                          clm_cvge_amt_val,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_clm_cvge_amt_insert.clm_id1            AS clm_id,
         exp_clm_cvge_amt_insert.feat_id1           AS cvge_feat_id,
         exp_clm_cvge_amt_insert.typecode1          AS clm_amt_type_cd,
         exp_clm_cvge_amt_insert.transactionamount1 AS clm_cvge_amt_val,
         exp_clm_cvge_amt_insert.prcs_id1           AS prcs_id,
         exp_clm_cvge_amt_insert.edw_strt_dttm1     AS edw_strt_dttm,
         exp_clm_cvge_amt_insert.edw_end_dttm       AS edw_end_dttm
  FROM   exp_clm_cvge_amt_insert;
  
  -- Component CLM_CVGE_AMT_ins_upd, Type TARGET
  INSERT INTO db_t_prod_core.clm_cvge_amt
              (
                          clm_id,
                          cvge_feat_id,
                          clm_amt_type_cd,
                          clm_cvge_amt_val,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_clm_cvge_amt_upd_insert.clm_id3           AS clm_id,
         exp_clm_cvge_amt_upd_insert.feat_id3          AS cvge_feat_id,
         exp_clm_cvge_amt_upd_insert.typecode3         AS clm_amt_type_cd,
         exp_clm_cvge_amt_upd_insert.transactionamount AS clm_cvge_amt_val,
         exp_clm_cvge_amt_upd_insert.prcs_id           AS prcs_id,
         exp_clm_cvge_amt_upd_insert.edw_strt_dttm3    AS edw_strt_dttm,
         exp_clm_cvge_amt_upd_insert.edw_end_dttm      AS edw_end_dttm
  FROM   exp_clm_cvge_amt_upd_insert;

END;
';