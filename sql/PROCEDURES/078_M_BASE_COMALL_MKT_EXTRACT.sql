-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_COMALL_MKT_EXTRACT("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component SQ_AGT_COMMINFO_HIST, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_agt_comminfo_hist AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS rgn,
                $2  AS dstrct,
                $3  AS service_center,
                $4  AS pay_agent_nbr,
                $5  AS agent_nbr,
                $6  AS orig_emp_date,
                $7  AS term_date,
                $8  AS val_mo,
                $9  AS val_yr,
                $10 AS yr,
                $11 AS mth,
                $12 AS property_nb_prem,
                $13 AS auto_nb_prem,
                $14 AS commercial_nb_prem,
                $15 AS asic_nb_prem,
                $16 AS life_nb_prem,
                $17 AS trexis_prem,
                $18 AS property_nb_comm,
                $19 AS auto_nb_comm,
                $20 AS commercial_nb_comm,
                $21 AS asic_nb_comm,
                $22 AS life_nb_comm,
                $23 AS asic_ren_comm,
                $24 AS commercial_ren_pp_prem,
                $25 AS auto_ren_asign_comm,
                $26 AS commercial_rewrite_prem,
                $27 AS commercial_ren_asign_prem,
                $28 AS asic_renewal_prem,
                $29 AS commercial_rewrite_comm,
                $30 AS commercial_ren_comm,
                $31 AS property_ren_pp_comm,
                $32 AS property_ren_pp_prem,
                $33 AS asic_other,
                $34 AS auto_ren_asign_prem,
                $35 AS auto_other,
                $36 AS auto_ren_pp_comm,
                $37 AS property_rewrite_comm,
                $38 AS property_other,
                $39 AS auto_renewal_prem,
                $40 AS commercial_other,
                $41 AS property_renewal_prem,
                $42 AS commercial_ren_pp_comm,
                $43 AS auto_ren_comm,
                $44 AS property_ren_comm,
                $45 AS property_ren_asign_comm,
                $46 AS commercial_renewal_prem,
                $47 AS asic_nb_comm_,
                $48 AS commercial_ren_asign_comm,
                $49 AS auto_ren_pp_prem,
                $50 AS property_rewrite_prem,
                $51 AS property_ren_asign_prem,
                $52 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     ( WITH commall_w_comm AS
                                  (
                                           SELECT   state_nbr,
                                                    agent_nbr,
                                                    extract(year, accounting_dt)  AS yr,
                                                    extract(month, accounting_dt) AS mth,
                                                    field_type,
                                                    CASE
                                                             WHEN field_type = ''001'' THEN ''NB PREM''
                                                             WHEN field_type = ''003'' THEN ''REN PP PREM''
                                                             WHEN field_type = ''004'' THEN ''REN ASIGN PREM''
                                                             WHEN field_type = ''005'' THEN ''REWRITE PREM''
                                                             WHEN field_type = ''002'' THEN ''RENEWAL PREM''
                                                             WHEN field_type = ''007'' THEN ''NB COMM''
                                                             WHEN field_type = ''009'' THEN ''REN PP COMM''
                                                             WHEN field_type = ''010'' THEN ''REN ASIGN COMM''
                                                             WHEN field_type = ''011'' THEN ''REWRITE COMM''
                                                             WHEN field_type = ''008'' THEN ''REN COMM''
                                                             ELSE ''OTHER''
                                                    END AS field_desc,
                                                    CASE
                                                             WHEN policy_symbol IN (''a0'',
                                                                                    ''g0'',
                                                                                    ''apv'') THEN ''AUTO''
                                                             WHEN policy_symbol IN (''n0'') THEN ''ASIC''
                                                             WHEN policy_symbol IN (''cp'',
                                                                                    ''H0'',
                                                                                    ''T0'',
                                                                                    ''F0'',
                                                                                    ''fc'',
                                                                                    ''M0'',
                                                                                    ''SP'',
                                                                                    ''wtc'',
                                                                                    ''r0'') THEN ''PROPERTY''
                                                             ELSE ''COMMERCIAL''
                                                    END AS pol_type,
                                                    pol_type
                                                             ||'' ''
                                                             ||field_desc AS field_name,
                                                    SUM(amount)           AS amount
                                           FROM     db_t_prod_comn.premium
                                           WHERE    accounting_dt >= cast(cast(extract( year, add_months($run_date,-1) )-$lookback_yr AS VARCHAR(4))
                                                             ||''-01-01'' AS VARCHAR(10))
                                           AND      accounting_dt <= LAST_DAY(TO_DATE($run_date, ''YYYY-MM-DD'') - INTERVAL ''1 MONTH'')

                                           GROUP BY 1,
                                                    2,
                                                    3,
                                                    4,
                                                    5,
                                                    6,
                                                    7,
                                                    8
                                           UNION
                                           SELECT   state_nbr,
                                                    agent_nbr,
                                                    extract(year, accounting_dt)  AS yr,
                                                    extract(month, accounting_dt) AS mth,
                                                    field_type,
                                                    CASE
                                                             WHEN field_type = ''001'' THEN ''NB PREM''
                                                             WHEN field_type = ''003'' THEN ''REN PP PREM''
                                                             WHEN field_type = ''004'' THEN ''REN ASIGN PREM''
                                                             WHEN field_type = ''005'' THEN ''REWRITE PREM''
                                                             WHEN field_type = ''002'' THEN ''RENEWAL PREM''
                                                             WHEN field_type = ''007'' THEN ''NB COMM''
                                                             WHEN field_type = ''009'' THEN ''REN PP COMM''
                                                             WHEN field_type = ''010'' THEN ''REN ASIGN COMM''
                                                             WHEN field_type = ''011'' THEN ''REWRITE COMM''
                                                             WHEN field_type = ''008'' THEN ''REN COMM''
                                                             ELSE ''OTHER''
                                                    END AS field_desc,
                                                    CASE
                                                             WHEN policy_symbol IN (''a0'',
                                                                                    ''g0'',
                                                                                    ''apv'') THEN ''AUTO''
                                                             WHEN policy_symbol IN (''n0'') THEN ''ASIC''
                                                             WHEN policy_symbol IN (''cp'',
                                                                                    ''H0'',
                                                                                    ''T0'',
                                                                                    ''F0'',
                                                                                    ''fc'',
                                                                                    ''M0'',
                                                                                    ''SP'',
                                                                                    ''wtc'',
                                                                                    ''r0'') THEN ''PROPERTY''
                                                             ELSE ''COMMERCIAL''
                                                    END AS pol_type,
                                                    pol_type
                                                             ||'' ''
                                                             ||field_desc AS field_name,
                                                    SUM(amount)           AS amount
                                           FROM     db_t_prod_comn.commissions
                                           WHERE    accounting_dt >= cast(cast(extract( year, add_months($run_date,-1) )-$lookback_yr AS VARCHAR(4))
                                                             ||''-01-01'' AS VARCHAR(10))
                                           AND     accounting_dt <= LAST_DAY(TO_DATE($run_date, ''YYYY-MM-DD'') - INTERVAL ''1 MONTH'')

                                           GROUP BY 1,
                                                    2,
                                                    3,
                                                    4,
                                                    5,
                                                    6,
                                                    7,
                                                    8 ), commall_w_comm_piv AS
                                  (
                                         SELECT agent_nbr,
                                                yr,
                                                mth,
                                                field_name,
                                                amount
                                         FROM   commall_w_comm ), commall_w_comm_final AS
                                  (
                                         SELECT *
                                         FROM   commall_w_comm pivot (SUM(amount) FOR field_name IN (''PROPERTY NB PREM'',
                                                                                                     ''ASIC RENEWAL PREM'',
                                                                                                     ''AUTO NB COMM'',
                                                                                                     ''COMMERCIAL REN PP PREM'',
                                                                                                     ''AUTO REN ASIGN COMM'',
                                                                                                     ''AUTO NB PREM'',
                                                                                                     ''COMMERCIAL REWRITE PREM'',
                                                                                                     ''COMMERCIAL REN ASIGN PREM'',
                                                                                                     ''ASIC REN COMM'',
                                                                                                     ''COMMERCIAL REWRITE COMM'',
                                                                                                     ''PROPERTY NB COMM'',
                                                                                                     ''ASIC NB PREM'',
                                                                                                     ''COMMERCIAL REN COMM'',
                                                                                                     ''PROPERTY REN PP COMM'',
                                                                                                     ''COMMERCIAL NB PREM'',
                                                                                                     ''PROPERTY REN PP PREM'',
                                                                                                     ''ASIC OTHER'',
                                                                                                     ''AUTO REN ASIGN PREM'',
                                                                                                     ''COMMERCIAL NB COMM'',
                                                                                                     ''AUTO OTHER'',
                                                                                                     ''AUTO REN PP COMM'',
                                                                                                     ''PROPERTY REWRITE COMM'',
                                                                                                     ''PROPERTY OTHER'',
                                                                                                     ''AUTO RENEWAL PREM'',
                                                                                                     ''COMMERCIAL OTHER'',
                                                                                                     ''PROPERTY RENEWAL PREM'',
                                                                                                     ''COMMERCIAL REN PP COMM'',
                                                                                                     ''AUTO REN COMM'',
                                                                                                     ''PROPERTY REN COMM'',
                                                                                                     ''PROPERTY REN ASIGN COMM'',
                                                                                                     ''COMMERCIAL RENEWAL PREM'',
                                                                                                     ''ASIC NB COMM'',
                                                                                                     ''COMMERCIAL REN ASIGN COMM'',
                                                                                                     ''AUTO REN PP PREM'',
                                                                                                     ''PROPERTY REWRITE PREM'',
                                                                                                     ''PROPERTY REN ASIGN PREM'' ) ) tmp ), trexis_prem_2 AS
                                  (
                                         SELECT a.*,
                                                extract(year, date_written)  yr,
                                                extract(month, date_written) mth
                                         FROM
                                                /* db_t_prod_anltc_sb.TREXIS_PREM  */
                                                db_t_prod_comn.trexis_agt_acctbly_prem a
                                         WHERE date_written BETWEEN 
                                                TO_DATE( (EXTRACT(YEAR, ADD_MONTHS(TO_DATE($run_date, ''YYYY-MM-DD''), -1)) - $lookback_yr) || ''-01-01'', ''YYYY-MM-DD'' )
                                                AND LAST_DAY(ADD_MONTHS(TO_DATE($run_date, ''YYYY-MM-DD''), -1))

                                                /* and cast(cast(extract( year from ADD_MONTHS($RUN_DATE,-1) )-0 as varchar(4)) ||''-12-31'' as varchar(10)) */
                                                /* ''2020-01-01'' and ''2021-12-31'' */
                                  )
                           SELECT    acomm.rgn,
                                     acomm.dstrct,
                                     acomm.service_center,
                                     acomm.pay_agent_nbr,
                                     a.agent_nbr,
                                     acomm.orig_emp_date,
                                     acomm.term_date,
                                     acomm.val_mo,
                                     acomm.val_yr,
                                     a.yr,
                                     a.mth,
                                     CASE
                                               WHEN SUM("''PROPERTY NB PREM''") IS NULL THEN 0
                                               ELSE SUM("''PROPERTY NB PREM''")
                                     END property_nb_prem,
                                     CASE
                                               WHEN SUM("''AUTO NB PREM''") IS NULL THEN 0
                                               ELSE SUM("''AUTO NB PREM''")
                                     END auto_nb_prem ,
                                     CASE
                                               WHEN SUM("''COMMERCIAL NB PREM''")IS NULL THEN 0
                                               ELSE SUM("''COMMERCIAL NB PREM''")
                                     END commercial_nb_prem ,
                                     CASE
                                               WHEN SUM("''ASIC NB PREM''") IS NULL THEN 0
                                               ELSE SUM("''ASIC NB PREM''")
                                     END asic_nb_prem ,
                                     CASE
                                               WHEN b.nb_prem IS NULL THEN 0
                                               ELSE b.nb_prem
                                     END life_nb_prem,
                                     /* case when c.NB_PREM_AMT is null then 0 else c.NB_PREM_AMT end  TREXIS_NB_PREM, */
                                     CASE
                                               WHEN c.premium_amt IS NULL THEN 0
                                               ELSE c.premium_amt
                                     END trexis_nb_prem,
                                     CASE
                                               WHEN SUM("''PROPERTY NB COMM''") IS NULL THEN 0
                                               ELSE SUM("''PROPERTY NB COMM''")
                                     END property_nb_comm ,
                                     CASE
                                               WHEN SUM("''AUTO NB COMM''") IS NULL THEN 0
                                               ELSE SUM("''AUTO NB COMM''")
                                     END auto_nb_comm ,
                                     CASE
                                               WHEN SUM("''COMMERCIAL NB COMM''") IS NULL THEN 0
                                               ELSE SUM("''COMMERCIAL NB COMM''")
                                     END commercial_nb_comm ,
                                     CASE
                                               WHEN SUM("''ASIC NB COMM''") IS NULL THEN 0
                                               ELSE SUM("''ASIC NB COMM''")
                                     END asic_nb_comm,
                                     CASE
                                               WHEN b.nb_commpd IS NULL THEN 0
                                               ELSE b.nb_commpd
                                     END life_nb_comm,
                                     CASE
                                               WHEN SUM("''ASIC REN COMM''") IS NULL THEN 0
                                               ELSE SUM("''ASIC REN COMM''")
                                     END asic_ren_comm,
                                     CASE
                                               WHEN SUM("''COMMERCIAL REN PP PREM''") IS NULL THEN 0
                                               ELSE SUM("''COMMERCIAL REN PP PREM''")
                                     END commercial_ren_pp_prem,
                                     CASE
                                               WHEN SUM("''AUTO REN ASIGN COMM''") IS NULL THEN 0
                                               ELSE SUM("''AUTO REN ASIGN COMM''")
                                     END auto_ren_asign_comm,
                                     CASE
                                               WHEN SUM("''COMMERCIAL REWRITE PREM''") IS NULL THEN 0
                                               ELSE SUM("''COMMERCIAL REWRITE PREM''")
                                     END commercial_rewrite_prem,
                                     CASE
                                               WHEN SUM("''COMMERCIAL REN ASIGN PREM''") IS NULL THEN 0
                                               ELSE SUM("''COMMERCIAL REN ASIGN PREM''")
                                     END commercial_ren_asign_prem,
                                     CASE
                                               WHEN SUM("''ASIC RENEWAL PREM''") IS NULL THEN 0
                                               ELSE SUM("''ASIC RENEWAL PREM''")
                                     END asic_renewal_prem,
                                     CASE
                                               WHEN SUM("''COMMERCIAL REWRITE COMM''") IS NULL THEN 0
                                               ELSE SUM("''COMMERCIAL REWRITE COMM''")
                                     END commercial_rewrite_comm,
                                     CASE
                                               WHEN SUM("''COMMERCIAL REN COMM''") IS NULL THEN 0
                                               ELSE SUM("''COMMERCIAL REN COMM''")
                                     END commercial_ren_comm,
                                     CASE
                                               WHEN SUM("''PROPERTY REN PP COMM''") IS NULL THEN 0
                                               ELSE SUM("''PROPERTY REN PP COMM''")
                                     END property_ren_pp_comm,
                                     CASE
                                               WHEN SUM("''PROPERTY REN PP PREM''") IS NULL THEN 0
                                               ELSE SUM("''PROPERTY REN PP PREM''")
                                     END property_ren_pp_prem,
                                     CASE
                                               WHEN SUM("''ASIC OTHER''") IS NULL THEN 0
                                               ELSE SUM("''ASIC OTHER''")
                                     END asic_other,
                                     CASE
                                               WHEN SUM("''AUTO REN ASIGN PREM''") IS NULL THEN 0
                                               ELSE SUM("''AUTO REN ASIGN PREM''")
                                     END auto_ren_asign_prem,
                                     CASE
                                               WHEN SUM("''AUTO OTHER''") IS NULL THEN 0
                                               ELSE SUM("''AUTO OTHER''")
                                     END auto_other,
                                     CASE
                                               WHEN SUM("''AUTO REN PP COMM''") IS NULL THEN 0
                                               ELSE SUM("''AUTO REN PP COMM''")
                                     END auto_ren_pp_comm,
                                     CASE
                                               WHEN SUM("''PROPERTY REWRITE COMM''") IS NULL THEN 0
                                               ELSE SUM("''PROPERTY REWRITE COMM''")
                                     END property_rewrite_comm,
                                     CASE
                                               WHEN SUM("''PROPERTY OTHER''") IS NULL THEN 0
                                               ELSE SUM("''PROPERTY OTHER''")
                                     END property_other,
                                     CASE
                                               WHEN SUM("''AUTO RENEWAL PREM''") IS NULL THEN 0
                                               ELSE SUM("''AUTO RENEWAL PREM''")
                                     END auto_renewal_prem,
                                     CASE
                                               WHEN SUM("''COMMERCIAL OTHER''") IS NULL THEN 0
                                               ELSE SUM("''COMMERCIAL OTHER''")
                                     END commercial_other,
                                     CASE
                                               WHEN SUM("''PROPERTY RENEWAL PREM''") IS NULL THEN 0
                                               ELSE SUM("''PROPERTY RENEWAL PREM''")
                                     END property_renewal_prem,
                                     CASE
                                               WHEN SUM("''COMMERCIAL REN PP COMM''") IS NULL THEN 0
                                               ELSE SUM("''COMMERCIAL REN PP COMM''")
                                     END commercial_ren_pp_comm,
                                     CASE
                                               WHEN SUM("''AUTO REN COMM''") IS NULL THEN 0
                                               ELSE SUM("''AUTO REN COMM''")
                                     END auto_ren_comm,
                                     CASE
                                               WHEN SUM("''PROPERTY REN COMM''") IS NULL THEN 0
                                               ELSE SUM("''PROPERTY REN COMM''")
                                     END property_ren_comm,
                                     CASE
                                               WHEN SUM("''PROPERTY REN ASIGN COMM''") IS NULL THEN 0
                                               ELSE SUM("''PROPERTY REN ASIGN COMM''")
                                     END property_ren_asign_comm,
                                     CASE
                                               WHEN SUM("''COMMERCIAL RENEWAL PREM''") IS NULL THEN 0
                                               ELSE SUM("''COMMERCIAL RENEWAL PREM''")
                                     END commercial_renewal_prem,
                                     CASE
                                               WHEN SUM("''ASIC NB COMM''") IS NULL THEN 0
                                               ELSE SUM("''ASIC NB COMM''")
                                     END asic_nb_comm,
                                     CASE
                                               WHEN SUM("''COMMERCIAL REN ASIGN COMM''") IS NULL THEN 0
                                               ELSE SUM("''COMMERCIAL REN ASIGN COMM''")
                                     END commercial_ren_asign_comm,
                                     CASE
                                               WHEN SUM("''AUTO REN PP PREM''") IS NULL THEN 0
                                               ELSE SUM("''AUTO REN PP PREM''")
                                     END auto_ren_pp_prem,
                                     CASE
                                               WHEN SUM("''PROPERTY REWRITE PREM''") IS NULL THEN 0
                                               ELSE SUM("''PROPERTY REWRITE PREM''")
                                     END property_rewrite_prem,
                                     CASE
                                               WHEN SUM("''PROPERTY REN ASIGN PREM''") IS NULL THEN 0
                                               ELSE SUM("''PROPERTY REN ASIGN PREM''")
                                     END property_ren_asign_prem
                           FROM      commall_w_comm_final a
                           left join db_t_prod_comn.tpc00501_level b
                           ON        b.agent = a.agent_nbr
                           AND       b.month1 = a.mth
                           AND       b.year1 = a.yr
                           left join trexis_prem_2 c
                           ON        c.agent_nbr = a.agent_nbr
                           AND       c.mth = a.mth
                           AND       c.yr = a.yr
                           left join db_v_prod_base.agt_comminfo_hst_bi acomm
                           ON        acomm.agent_nbr = a.agent_nbr
                           AND       extract(month, acomm.accounting_dt) = a.mth
                           AND       extract(year, acomm.accounting_dt) = a.yr
                           GROUP BY  1,
                                     2,
                                     3,
                                     4,
                                     5,
                                     6,
                                     7,
                                     8,
                                     9,
                                     10,
                                     11,
                                     16,
                                     17,
                                     22
                           ORDER BY  5,
                                     10,
                                     11 ) src ) );
  -- Component exp_src_tgt, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_src_tgt AS
  (
         SELECT sq_agt_comminfo_hist.rgn                       AS rgn,
                sq_agt_comminfo_hist.dstrct                    AS dstrct,
                sq_agt_comminfo_hist.service_center            AS service_center,
                sq_agt_comminfo_hist.pay_agent_nbr             AS pay_agent_nbr,
                sq_agt_comminfo_hist.agent_nbr                 AS agent_nbr,
                sq_agt_comminfo_hist.orig_emp_date             AS orig_emp_date,
                sq_agt_comminfo_hist.term_date                 AS term_date,
                sq_agt_comminfo_hist.val_mo                    AS val_mo,
                sq_agt_comminfo_hist.val_yr                    AS val_yr,
                sq_agt_comminfo_hist.yr                        AS yr,
                sq_agt_comminfo_hist.mth                       AS mth,
                sq_agt_comminfo_hist.property_nb_prem          AS property_nb_prem,
                sq_agt_comminfo_hist.auto_nb_prem              AS auto_nb_prem,
                sq_agt_comminfo_hist.commercial_nb_prem        AS commercial_nb_prem,
                sq_agt_comminfo_hist.asic_nb_prem              AS asic_nb_prem,
                sq_agt_comminfo_hist.life_nb_prem              AS life_nb_prem,
                sq_agt_comminfo_hist.trexis_prem               AS trexis_prem,
                sq_agt_comminfo_hist.property_nb_comm          AS property_nb_comm,
                sq_agt_comminfo_hist.auto_nb_comm              AS auto_nb_comm,
                sq_agt_comminfo_hist.commercial_nb_comm        AS commercial_nb_comm,
                sq_agt_comminfo_hist.asic_nb_comm              AS asic_nb_comm,
                sq_agt_comminfo_hist.life_nb_comm              AS life_nb_comm,
                sq_agt_comminfo_hist.asic_ren_comm             AS asic_ren_comm,
                sq_agt_comminfo_hist.commercial_ren_pp_prem    AS commercial_ren_pp_prem,
                sq_agt_comminfo_hist.auto_ren_asign_comm       AS auto_ren_asign_comm,
                sq_agt_comminfo_hist.commercial_rewrite_prem   AS commercial_rewrite_prem,
                sq_agt_comminfo_hist.commercial_ren_asign_prem AS commercial_ren_asign_prem,
                sq_agt_comminfo_hist.asic_renewal_prem         AS asic_renewal_prem,
                sq_agt_comminfo_hist.commercial_rewrite_comm   AS commercial_rewrite_comm,
                sq_agt_comminfo_hist.commercial_ren_comm       AS commercial_ren_comm,
                sq_agt_comminfo_hist.property_ren_pp_comm      AS property_ren_pp_comm,
                sq_agt_comminfo_hist.property_ren_pp_prem      AS property_ren_pp_prem,
                sq_agt_comminfo_hist.asic_other                AS asic_other,
                sq_agt_comminfo_hist.auto_ren_asign_prem       AS auto_ren_asign_prem,
                sq_agt_comminfo_hist.auto_other                AS auto_other,
                sq_agt_comminfo_hist.auto_ren_pp_comm          AS auto_ren_pp_comm,
                sq_agt_comminfo_hist.property_rewrite_comm     AS property_rewrite_comm,
                sq_agt_comminfo_hist.property_other            AS property_other,
                sq_agt_comminfo_hist.auto_renewal_prem         AS auto_renewal_prem,
                sq_agt_comminfo_hist.commercial_other          AS commercial_other,
                sq_agt_comminfo_hist.property_renewal_prem     AS property_renewal_prem,
                sq_agt_comminfo_hist.commercial_ren_pp_comm    AS commercial_ren_pp_comm,
                sq_agt_comminfo_hist.auto_ren_comm             AS auto_ren_comm,
                sq_agt_comminfo_hist.property_ren_comm         AS property_ren_comm,
                sq_agt_comminfo_hist.property_ren_asign_comm   AS property_ren_asign_comm,
                sq_agt_comminfo_hist.commercial_renewal_prem   AS commercial_renewal_prem,
                sq_agt_comminfo_hist.asic_nb_comm_             AS asic_nb_comm_,
                sq_agt_comminfo_hist.commercial_ren_asign_comm AS commercial_ren_asign_comm,
                sq_agt_comminfo_hist.auto_ren_pp_prem          AS auto_ren_pp_prem,
                sq_agt_comminfo_hist.property_rewrite_prem     AS property_rewrite_prem,
                sq_agt_comminfo_hist.property_ren_asign_prem   AS property_ren_asign_prem,
                sq_agt_comminfo_hist.source_record_id
         FROM   sq_agt_comminfo_hist );
  -- Component MKT_COMM, Type TARGET_EXPORT_PREPARE Stage data before exporting
  CREATE
  OR
  replace TEMPORARY TABLE mkt_comm AS
  (
         SELECT exp_src_tgt.rgn                       AS rgn,
                exp_src_tgt.dstrct                    AS dstrct,
                exp_src_tgt.service_center            AS service_center,
                exp_src_tgt.pay_agent_nbr             AS pay_agent_nbr,
                exp_src_tgt.agent_nbr                 AS agent_nbr,
                exp_src_tgt.orig_emp_date             AS orig_emp_date,
                exp_src_tgt.term_date                 AS term_date,
                exp_src_tgt.val_mo                    AS val_mo,
                exp_src_tgt.val_yr                    AS val_yr,
                exp_src_tgt.yr                        AS yr,
                exp_src_tgt.mth                       AS mth,
                exp_src_tgt.property_nb_prem          AS property_nb_prem,
                exp_src_tgt.auto_nb_prem              AS auto_nb_prem,
                exp_src_tgt.commercial_nb_prem        AS commercial_nb_prem,
                exp_src_tgt.asic_nb_prem              AS asic_nb_prem,
                exp_src_tgt.life_nb_prem              AS life_nb_prem,
                exp_src_tgt.trexis_prem               AS trexis_prem,
                exp_src_tgt.property_nb_comm          AS property_nb_comm,
                exp_src_tgt.auto_nb_comm              AS auto_nb_comm,
                exp_src_tgt.commercial_nb_comm        AS commercial_nb_comm,
                exp_src_tgt.asic_nb_comm              AS asic_nb_comm,
                exp_src_tgt.life_nb_comm              AS life_nb_comm,
                exp_src_tgt.asic_ren_comm             AS asic_ren_comm,
                exp_src_tgt.commercial_ren_pp_prem    AS commercial_ren_pp_prem,
                exp_src_tgt.auto_ren_asign_comm       AS auto_ren_asign_comm,
                exp_src_tgt.commercial_rewrite_prem   AS commercial_rewrite_prem,
                exp_src_tgt.commercial_ren_asign_prem AS commercial_ren_asign_prem,
                exp_src_tgt.asic_renewal_prem         AS asic_renewal_prem,
                exp_src_tgt.commercial_rewrite_comm   AS commercial_rewrite_comm,
                exp_src_tgt.commercial_ren_comm       AS commercial_ren_comm,
                exp_src_tgt.property_ren_pp_comm      AS property_ren_pp_comm,
                exp_src_tgt.property_ren_pp_prem      AS property_ren_pp_prem,
                exp_src_tgt.asic_other                AS asic_other,
                exp_src_tgt.auto_ren_asign_prem       AS auto_ren_asign_prem,
                exp_src_tgt.auto_other                AS auto_other,
                exp_src_tgt.auto_ren_pp_comm          AS auto_ren_pp_comm,
                exp_src_tgt.property_rewrite_comm     AS property_rewrite_comm,
                exp_src_tgt.property_other            AS property_other,
                exp_src_tgt.auto_renewal_prem         AS auto_renewal_prem,
                exp_src_tgt.commercial_other          AS commercial_other,
                exp_src_tgt.property_renewal_prem     AS property_renewal_prem,
                exp_src_tgt.commercial_ren_pp_comm    AS commercial_ren_pp_comm,
                exp_src_tgt.auto_ren_comm             AS auto_ren_comm,
                exp_src_tgt.property_ren_comm         AS property_ren_comm,
                exp_src_tgt.property_ren_asign_comm   AS property_ren_asign_comm,
                exp_src_tgt.commercial_renewal_prem   AS commercial_renewal_prem,
                exp_src_tgt.asic_nb_comm_             AS asic_nb_comm_,
                exp_src_tgt.commercial_ren_asign_comm AS commercial_ren_asign_comm,
                exp_src_tgt.auto_ren_pp_prem          AS auto_ren_pp_prem,
                exp_src_tgt.property_rewrite_prem     AS property_rewrite_prem,
                exp_src_tgt.property_ren_asign_prem   AS property_ren_asign_prem
         FROM   exp_src_tgt );
  -- Component MKT_COMM, Type EXPORT_DATA Exporting data
 -- ;
END;
';