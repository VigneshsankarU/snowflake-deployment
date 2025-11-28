-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRIOR_LOSS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
	run_id STRING;
       workflow_name STRING;
       session_name STRING;
       start_dttm TIMESTAMP;
       end_dttm TIMESTAMP;
	v_start_time TIMESTAMP;
BEGIN
       run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
       workflow_name := (SELECT workflow_name FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
       session_name := ''s_m_base_prior_loss'';
       start_dttm := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);
       end_dttm := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
	v_start_time := CURRENT_TIMESTAMP();

-- Component SQ_priorloss, Type SOURCE
CREATE OR replace TEMPORARY TABLE sq_priorloss AS
(
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS id,
                $2  AS prior_loss_summary_type_cd,
                $3  AS claimdate,
                $4  AS claimnum,
                $5  AS policynum,
                $6  AS policycompany,
                $7  AS policynumber,
                $8  AS agmt_type_cd,
                $9  AS jobnumber,
                $10 AS branchnumber,
                $11 AS src_sys,
                $12 AS prior_loss_src_cd,
                $13 AS atfault,
                $14 AS policyholdername,
                $15 AS dob_alfa,
                $16 AS ssn_alfa,
                $17 AS address,
                $18 AS city,
                $19 AS state,
                $20 AS zip,
                $21 AS updatetime,
                $22 AS createtime,
                $23 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                            SELECT    pcx_priorlossext.id_stg AS id,
                                                      CASE
                                                                WHEN pctl_priorlossext.typecode_stg =''PAPriorLossExt'' THEN ''PRIOR_LOSS_SUMRY_TYPE1''
                                                                WHEN pctl_priorlossext.typecode_stg = ''HOPriorLossExt'' THEN ''PRIOR_LOSS_SUMRY_TYPE2''
                                                                ELSE ''0''
                                                      END                                                                    AS prior_loss_summary_type_cd,
                                                      pcx_priorlossext.claimdate_stg                                         AS claimdate,
                                                      pcx_priorlossext.claimnum_stg                                          AS claimnum,
                                                      pcx_priorlossext.policynum_stg                                         AS policynum,
                                                      pcx_priorlossext.policycompany_stg                                     AS policycompany,
                                                      pc_policyperiod.publicid_stg                                           AS policynumber,
                                                      ''AGMT_TYPE5''                                                           AS agmt_type_cd,
                                                      pc_job.jobnumber_stg                                                   AS jobnumber,
                                                      pc_policyperiod.branchnumber_stg                                       AS branchnumber,
                                                      ''SRC_SYS4''                                                             AS src_sys,
                                                      cast(pcx_priorlossext.fromlegacyorclaimcenter_alfa_stg AS VARCHAR(60)) AS prior_loss_src_cd,
                                                      left(pcx_priorlossext.atfault_stg, 3)                                  AS atfault,
                                                      pcx_priorlossext.policyholdername_stg                                  AS policyholdername,
                                                      pcx_priorlossext.dob_alfa_stg                                          AS dob_alfa,
                                                      pcx_priorlossext.ssn_alfa_stg                                          AS ssn_alfa,
                                                      pcx_priorlossext.address_stg                                           AS address,
                                                      pcx_priorlossext.city_stg                                              AS city,
                                                      pcx_priorlossext.state_stg                                             AS state,
                                                      pcx_priorlossext.zip_stg                                               AS zip,
                                                      pcx_priorlossext.updatetime_stg                                        AS updatedtime,
                                                      pcx_priorlossext.updatetime_stg                                        AS createtime
                                            FROM      db_t_prod_stag.pcx_priorlossext
                                                      /*left join DB_T_PROD_STAG.pc_policyline on (pc_policyline.id = pcx_priorlossext.HomeownersLine_HOEID
or  pc_policyline.id = pcx_priorlossext.PersonalAutoLineID)
left join DB_T_PROD_STAG.pctl_papolicytype_alfa on pctl_papolicytype_alfa.id = pc_policyline.PAPolicyType_alfa
left join DB_T_PROD_STAG.pctl_hopolicytype_hoe on pctl_hopolicytype_hoe.id = pc_policyline.HOPolicyType*/
                                            left join db_t_prod_stag.pc_policyperiod
                                            ON        pc_policyperiod.publicid_stg = pcx_priorlossext.policyperiodid_alfa_stg
                                            left join db_t_prod_stag.pctl_priorlossext
                                            ON        pctl_priorlossext.id_stg = pcx_priorlossext.subtype_stg
                                            left join db_t_prod_stag.pc_job
                                            ON        pc_job.id_stg = pc_policyperiod.jobid_stg
                                            WHERE     (
                                                                pcx_priorlossext.updatetime_stg >(:start_dttm)
                                                      AND       pcx_priorlossext.updatetime_stg <= (:end_dttm))
                                            OR        (
                                                                pc_policyperiod.updatetime_stg > (:start_dttm)
                                                      AND       pc_policyperiod.updatetime_stg <= (:end_dttm)) ) src ) 
);
 
-- Component LKP_XLAT_AGMT_TYPE, Type LOOKUP
CREATE OR replace TEMPORARY TABLE lkp_xlat_agmt_type AS
(
            SELECT    lkp.tgt_idntftn_val,
                      sq_priorloss.source_record_id,
                      row_number() over(PARTITION BY sq_priorloss.source_record_id ORDER BY lkp.src_idntftn_sys ASC,lkp.src_idntftn_nm ASC,lkp.src_idntftn_val ASC,lkp.tgt_idntftn_nm ASC,lkp.tgt_idntftn_val ASC,lkp.expn_dt ASC,lkp.eff_dt ASC) rnk
            FROM      sq_priorloss
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_sys AS src_idntftn_sys ,
                                    teradata_etl_ref_xlat.src_idntftn_nm AS src_idntftn_nm ,
                                    teradata_etl_ref_xlat.tgt_idntftn_nm AS tgt_idntftn_nm ,
                                    teradata_etl_ref_xlat.expn_dt AS expn_dt ,
                                    teradata_etl_ref_xlat.eff_dt AS eff_dt
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''AGMT_TYPE''
                             AND    lower(teradata_etl_ref_xlat.src_idntftn_nm) IN ( ''derived '',
                                                                                    ''pctl_sourceofbusiness_alfa.typecode'')
                             AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''DS'' ,
                                                                              ''GW'')
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = sq_priorloss.agmt_type_cd qualify rnk = 1 
);

-- Component LKP_XLAT_PRIOR_LOSS_SMRY_TYP, Type LOOKUP
CREATE OR replace TEMPORARY TABLE lkp_xlat_prior_loss_smry_typ AS
(
            SELECT    lkp.tgt_idntftn_val,
                      sq_priorloss.source_record_id,
                      row_number() over(PARTITION BY sq_priorloss.source_record_id ORDER BY lkp.src_idntftn_sys ASC,lkp.src_idntftn_nm ASC,lkp.src_idntftn_val ASC,lkp.tgt_idntftn_nm ASC,lkp.tgt_idntftn_val ASC,lkp.expn_dt ASC,lkp.eff_dt ASC) rnk
            FROM      sq_priorloss
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_sys AS src_idntftn_sys ,
                                    teradata_etl_ref_xlat.src_idntftn_nm AS src_idntftn_nm ,
                                    teradata_etl_ref_xlat.tgt_idntftn_nm AS tgt_idntftn_nm ,
                                    teradata_etl_ref_xlat.expn_dt AS expn_dt ,
                                    teradata_etl_ref_xlat.eff_dt AS eff_dt
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRIOR_LOSS_SUMRY_TYPE''
                             AND    lower(teradata_etl_ref_xlat.src_idntftn_nm) IN ( ''derived '',
                                                                                    ''pctl_sourceofbusiness_alfa.typecode'',
                                                                                    ''bctl_sourceofbusiness_alfa.typecode'')
                             AND    teradata_etl_ref_xlat.src_idntftn_sys = ''DS''
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = sq_priorloss.prior_loss_summary_type_cd qualify rnk = 1 
);

-- Component LKP_XLAT_SRC_SYS, Type LOOKUP
CREATE OR replace TEMPORARY TABLE lkp_xlat_src_sys AS
(
            SELECT    lkp.tgt_idntftn_val,
                      sq_priorloss.source_record_id,
                      row_number() over(PARTITION BY sq_priorloss.source_record_id ORDER BY lkp.src_idntftn_sys ASC,lkp.src_idntftn_nm ASC,lkp.src_idntftn_val ASC,lkp.tgt_idntftn_nm ASC,lkp.tgt_idntftn_val ASC,lkp.expn_dt ASC,lkp.eff_dt ASC) rnk
            FROM      sq_priorloss
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_sys AS src_idntftn_sys ,
                                    teradata_etl_ref_xlat.src_idntftn_nm AS src_idntftn_nm ,
                                    teradata_etl_ref_xlat.tgt_idntftn_nm AS tgt_idntftn_nm ,
                                    teradata_etl_ref_xlat.expn_dt AS expn_dt ,
                                    teradata_etl_ref_xlat.eff_dt AS eff_dt
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
                             AND    lower(teradata_etl_ref_xlat.src_idntftn_nm) IN ( ''derived '',
                                                                                    ''pctl_sourceofbusiness_alfa.typecode'',
                                                                                    ''bctl_sourceofbusiness_alfa.typecode'')
                             AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''DS'' ,
                                                                              ''GW'')
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = sq_priorloss.src_sys qualify rnk = 1 
);

-- Component LKP_XLAT_PRIOR_LOSS_SRC, Type LOOKUP
CREATE OR replace TEMPORARY TABLE lkp_xlat_prior_loss_src AS
(
            SELECT    lkp.tgt_idntftn_val,
                      sq_priorloss.source_record_id,
                      row_number() over(PARTITION BY sq_priorloss.source_record_id ORDER BY lkp.src_idntftn_sys ASC,lkp.src_idntftn_nm ASC,lkp.src_idntftn_val ASC,lkp.tgt_idntftn_nm ASC,lkp.tgt_idntftn_val ASC,lkp.expn_dt ASC,lkp.eff_dt ASC) rnk
            FROM      sq_priorloss
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_sys AS src_idntftn_sys ,
                                    teradata_etl_ref_xlat.src_idntftn_nm AS src_idntftn_nm ,
                                    teradata_etl_ref_xlat.tgt_idntftn_nm AS tgt_idntftn_nm ,
                                    teradata_etl_ref_xlat.expn_dt AS expn_dt ,
                                    teradata_etl_ref_xlat.eff_dt AS eff_dt
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRIOR_LOSS_SRC''
                             AND    lower(teradata_etl_ref_xlat.src_idntftn_nm) = ''pcx_priorlossext.fromlegacyorclaimcenter_alfa''
                             AND    teradata_etl_ref_xlat.src_idntftn_sys = ''GW''
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = sq_priorloss.prior_loss_src_cd qualify rnk = 1 
);

-- Component exp_pass_from_src, Type EXPRESSION
CREATE OR replace TEMPORARY TABLE exp_pass_from_src AS
(
             SELECT     sq_priorloss.id                                                        AS id,
                        lkp_xlat_prior_loss_smry_typ.tgt_idntftn_val                           AS prior_loss_summary_type_cd,
                        sq_priorloss.claimdate                                                 AS claimdate,
                        sq_priorloss.claimnum                                                  AS claimnum,
                        sq_priorloss.policynum                                                 AS policynum,
                        sq_priorloss.policycompany                                             AS policycompany,
                        sq_priorloss.policynumber                                              AS policynumber,
                        lkp_xlat_agmt_type.tgt_idntftn_val                                     AS agmt_type_cd,
                        sq_priorloss.jobnumber                                                 AS jobnumber,
                        sq_priorloss.branchnumber                                              AS branchnumber,
                        lkp_xlat_src_sys.tgt_idntftn_val                                       AS src_sys,
                        lkp_xlat_prior_loss_src.tgt_idntftn_val                                AS prior_loss_src_cd,
                        sq_priorloss.atfault                                                   AS atfault,
                        sq_priorloss.policyholdername                                          AS policyholdername,
                        sq_priorloss.dob_alfa                                                  AS dob_alfa,
                        sq_priorloss.ssn_alfa                                                  AS ssn_alfa,
                        sq_priorloss.address                                                   AS address,
                        sq_priorloss.city                                                      AS city,
                        sq_priorloss.state                                                     AS state,
                        sq_priorloss.zip                                                       AS zip,
                        current_timestamp                                                      AS start_dttm,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS end_dttm,
                        sq_priorloss.updatetime                                                AS updatetime,
                        sq_priorloss.createtime                                                AS createtime,
                        sq_priorloss.source_record_id
             FROM       sq_priorloss
             inner join lkp_xlat_agmt_type
             ON         sq_priorloss.source_record_id = lkp_xlat_agmt_type.source_record_id
             inner join lkp_xlat_prior_loss_smry_typ
             ON         lkp_xlat_agmt_type.source_record_id = lkp_xlat_prior_loss_smry_typ.source_record_id
             inner join lkp_xlat_src_sys
             ON         lkp_xlat_prior_loss_smry_typ.source_record_id = lkp_xlat_src_sys.source_record_id
             inner join lkp_xlat_prior_loss_src
             ON         lkp_xlat_src_sys.source_record_id = lkp_xlat_prior_loss_src.source_record_id 
);

-- Component LKP_QUOTN_ID, Type LOOKUP
CREATE OR replace TEMPORARY TABLE lkp_quotn_id AS
(
            SELECT    lkp.quotn_id,
                      exp_pass_from_src.src_sys AS src_sys,
                      exp_pass_from_src.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_src.source_record_id ORDER BY lkp.quotn_id ASC,lkp.nk_job_nbr ASC,lkp.vers_nbr ASC,lkp.src_sys_cd ASC) rnk
            FROM      exp_pass_from_src
            left join
                      (
                                      SELECT DISTINCT insrnc_quotn.quotn_id   AS quotn_id,
                                                      insrnc_quotn.nk_job_nbr AS nk_job_nbr,
                                                      insrnc_quotn.vers_nbr   AS vers_nbr,
                                                      insrnc_quotn.src_sys_cd AS src_sys_cd
                                      FROM            db_t_prod_core.insrnc_quotn ) lkp
            ON        lkp.nk_job_nbr = exp_pass_from_src.jobnumber
            AND       lkp.vers_nbr = exp_pass_from_src.branchnumber
            AND       lkp.src_sys_cd = exp_pass_from_src.src_sys qualify rnk = 1 
);

-- Component LKP_AGMT_ID, Type LOOKUP
CREATE OR replace TEMPORARY TABLE lkp_agmt_id AS
(
            SELECT    lkp.agmt_id,
                      exp_pass_from_src.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_src.source_record_id ORDER BY lkp.agmt_id ASC,lkp.host_agmt_num ASC,lkp.agmt_type_cd ASC) rnk
            FROM      exp_pass_from_src
            left join
                      (
                                      SELECT DISTINCT agmt.agmt_id      AS agmt_id,
                                                      agmt.nk_src_key   AS host_agmt_num,
                                                      agmt.agmt_type_cd AS agmt_type_cd
                                      FROM            db_t_prod_core.agmt ) lkp
            ON        lkp.host_agmt_num = exp_pass_from_src.policynumber
            AND       lkp.agmt_type_cd = exp_pass_from_src.agmt_type_cd qualify rnk = 1 
);

-- Component exp, Type EXPRESSION
CREATE OR replace TEMPORARY TABLE exp AS
(
             SELECT     exp_pass_from_src.id                         AS id,
                        exp_pass_from_src.prior_loss_summary_type_cd AS prior_loss_summary_type_cd,
                        exp_pass_from_src.claimdate                  AS claimdate,
                        exp_pass_from_src.claimnum                   AS claimnum,
                        exp_pass_from_src.policynum                  AS policynum,
                        exp_pass_from_src.policycompany              AS policycompany,
                        exp_pass_from_src.prior_loss_src_cd          AS prior_loss_src_cd,
                        exp_pass_from_src.atfault                    AS atfault,
                        exp_pass_from_src.policyholdername           AS policyholdername,
                        exp_pass_from_src.dob_alfa                   AS dob_alfa,
                        exp_pass_from_src.ssn_alfa                   AS ssn_alfa,
                        exp_pass_from_src.address                    AS address,
                        exp_pass_from_src.city                       AS city,
                        exp_pass_from_src.state                      AS state,
                        exp_pass_from_src.zip                        AS zip,
                        exp_pass_from_src.start_dttm                 AS start_dttm,
                        exp_pass_from_src.end_dttm                   AS end_dttm,
                        exp_pass_from_src.updatetime                 AS updatetime,
                        lkp_agmt_id.agmt_id                          AS agmt_id,
                        lkp_quotn_id.quotn_id                        AS quotn_id,
                        exp_pass_from_src.createtime                 AS createtime,
                        exp_pass_from_src.src_sys                    AS src_sys,
                        exp_pass_from_src.source_record_id
             FROM       exp_pass_from_src
             inner join lkp_quotn_id
             ON         exp_pass_from_src.source_record_id = lkp_quotn_id.source_record_id
             inner join lkp_agmt_id
             ON         lkp_quotn_id.source_record_id = lkp_agmt_id.source_record_id 
);

-- Component LKP_TGT, Type LOOKUP
CREATE OR replace TEMPORARY TABLE lkp_tgt AS
(
            SELECT    lkp.prior_loss_sumry_id,
                      exp.id AS id,
                      exp.source_record_id,
                      row_number() over(PARTITION BY exp.source_record_id ORDER BY lkp.prior_loss_sumry_id ASC) rnk
            FROM      exp
            left join
                      (
                             SELECT prior_loss_sumry_id
                             FROM   db_t_prod_core.prior_loss_sumry ) lkp
            ON        lkp.prior_loss_sumry_id = exp.id qualify rnk = 1 
);

-- Component exp1, Type EXPRESSION
CREATE OR replace TEMPORARY TABLE exp1 AS
(
             SELECT     exp.id                         AS id,
                        exp.prior_loss_summary_type_cd AS prior_loss_summary_type_cd,
                        exp.claimdate                  AS claimdate,
                        exp.claimnum                   AS claimnum,
                        exp.policynum                  AS policynum,
                        exp.policycompany              AS policycompany,
                        exp.prior_loss_src_cd          AS prior_loss_src_cd,
                        exp.atfault                    AS atfault,
                        exp.policyholdername           AS policyholdername,
                        exp.dob_alfa                   AS dob_alfa,
                        exp.ssn_alfa                   AS ssn_alfa,
                        exp.address                    AS address,
                        exp.city                       AS city,
                        exp.state                      AS state,
                        exp.zip                        AS zip,
                        exp.start_dttm                 AS start_dttm,
                        exp.end_dttm                   AS end_dttm,
                        exp.updatetime                 AS updatetime,
                        exp.agmt_id                    AS agmt_id,
                        exp.quotn_id                   AS quotn_id,
                        lkp_tgt.prior_loss_sumry_id    AS prior_loss_sumry_id,
                        exp.createtime                 AS createtime,
                        CASE
                                   WHEN lkp_tgt.prior_loss_sumry_id IS NULL THEN ''I''
                                   ELSE ''R''
                        END         AS o_flag,
                        exp.src_sys AS src_sys,
                        exp.source_record_id
             FROM       exp
             inner join lkp_tgt
             ON         exp.source_record_id = lkp_tgt.source_record_id 
);

-- Component fil, Type FILTER
CREATE OR replace TEMPORARY TABLE fil AS
(
         SELECT exp1.id                         AS id,
                exp1.prior_loss_summary_type_cd AS prior_loss_summary_type_cd,
                exp1.claimdate                  AS claimdate,
                exp1.claimnum                   AS claimnum,
                exp1.policynum                  AS policynum,
                exp1.policycompany              AS policycompany,
                exp1.prior_loss_src_cd          AS prior_loss_src_cd,
                exp1.atfault                    AS atfault,
                exp1.policyholdername           AS policyholdername,
                exp1.dob_alfa                   AS dob_alfa,
                exp1.ssn_alfa                   AS ssn_alfa,
                exp1.address                    AS address,
                exp1.city                       AS city,
                exp1.state                      AS state,
                exp1.zip                        AS zip,
                exp1.start_dttm                 AS start_dttm,
                exp1.end_dttm                   AS end_dttm,
                exp1.updatetime                 AS updatetime,
                exp1.agmt_id                    AS agmt_id,
                exp1.quotn_id                   AS quotn_id,
                exp1.prior_loss_sumry_id        AS prior_loss_sumry_id,
                exp1.o_flag                     AS o_flag,
                exp1.createtime                 AS createtime,
                exp1.src_sys                    AS src_sys,
                exp1.source_record_id
         FROM   exp1
         WHERE  exp1.o_flag = ''I'' 
);

-- Component PRIOR_LOSS_SUMRY, Type TARGET
INSERT INTO db_t_prod_core.prior_loss_sumry
(
                          prior_loss_sumry_id,
                          prior_loss_sumry_type_cd,
                          clm_dttm,
                          clm_num,
                          plcy_num,
                          plcy_insrr_name,
                          agmt_id,
                          quotn_id,
                          prior_loss_src_cd,
                          at_fault_ind,
                          plcy_hldr_name,
                          plcy_hldr_birth_dt,
                          plcy_hldr_ssn_tax_num,
                          plcy_hldr_street_addr_txt,
                          plcy_hldr_city_name,
                          plcy_hldr_st_name,
                          plcy_hldr_postl_cd_num,
                          src_sys_cd,
                          crtd_dttm,
                          updt_dttm
)
SELECT fil.id                         AS prior_loss_sumry_id,
         fil.prior_loss_summary_type_cd AS prior_loss_sumry_type_cd,
         fil.claimdate                  AS clm_dttm,
         fil.claimnum                   AS clm_num,
         fil.policynum                  AS plcy_num,
         fil.policycompany              AS plcy_insrr_name,
         fil.agmt_id                    AS agmt_id,
         fil.quotn_id                   AS quotn_id,
         fil.prior_loss_src_cd          AS prior_loss_src_cd,
         fil.atfault                    AS at_fault_ind,
         fil.policyholdername           AS plcy_hldr_name,
         fil.dob_alfa                   AS plcy_hldr_birth_dt,
         fil.ssn_alfa                   AS plcy_hldr_ssn_tax_num,
         fil.address                    AS plcy_hldr_street_addr_txt,
         fil.city                       AS plcy_hldr_city_name,
         fil.state                      AS plcy_hldr_st_name,
         fil.zip                        AS plcy_hldr_postl_cd_num,
         fil.src_sys                    AS src_sys_cd,
         fil.createtime                 AS crtd_dttm,
         fil.updatetime                 AS updt_dttm
FROM   fil;


INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
SELECT :run_id, :worklet_name, ''m_base_prior_loss'', ''SUCCEEDED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
  ''start_dttm'', :start_dttm,
  ''end_dttm'', :end_dttm,
  ''StartTime'', :v_start_time,
  ''SrcSuccessRows'', (SELECT COUNT(*) FROM sq_priorloss),
  ''TgtSuccessRows'', (SELECT COUNT(*) FROM fil)
);

EXCEPTION WHEN OTHER THEN
    INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
    SELECT :run_id, :worklet_name, ''m_base_prior_loss'', ''FAILED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
        ''SQLERRM'', :sqlerrm
    );

END;
';