-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AMERICAN_AG_LARGEST_LOSS_CLAIMS_VIEW("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component SQ_GW_Largest_Loss_Claims_View, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_gw_largest_loss_claims_view AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS plcy_nbr,
                $2  AS clm_nbr,
                $3  AS clm_loss_dt,
                $4  AS total_incurred,
                $5  AS expense,
                $6  AS total_reserves,
                $7  AS paid,
                $8  AS cause_desc,
                $9  AS lob_cd,
                $10 AS clm_sts_type_cd,
                $11 AS dwelling,
                $12 AS personal_property,
                $13 AS other_structure,
                $14 AS personal_liability,
                $15 AS loss_of_use,
                $16 AS medical_payments_to_others,
                $17 AS home_systems_protection,
                $18 AS min_comp_ded,
                $19 AS max_comp_ded,
                $20 AS min_coll_ded,
                $21 AS max_coll_ded,
                $22 AS min_med_limit,
                $23 AS max_med_limit,
                $24 AS min_bi_limit,
                $25 AS max_bi_limit,
                $26 AS min_pd_limit,
                $27 AS max_pd_limit,
                $28 AS min_sl_limit,
                $29 AS max_sl_limit,
                $30 AS min_lou_limit,
                $31 AS max_lou_limit,
                $32 AS min_umbi_limit,
                $33 AS max_umbi_limit,
                $34 AS min_ers_limit,
                $35 AS max_ers_limit,
                $36 AS each_occurrence,
                $37 AS damage_to_prem,
                $38 AS gen_agg,
                $39 AS prod_comp_ops,
                $40 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT DISTINCT b.plcy_nbr,
                                                                  b.clm_nbr,
                                                                  clm_loss_dt,
                                                                  total_incurred,
                                                                  expense,
                                                                  total_reserves,
                                                                  paid,
                                                                  cause_desc,
                                                                  lob_cd ,
                                                                  clm_sts_type_cd ,
                                                                  dwelling,
                                                                  personal_property,
                                                                  other_structure,
                                                                  personal_liability,
                                                                  loss_of_use,
                                                                  medical_payments_to_others,
                                                                  home_systems_protection ,
                                                                  CASE
                                                                                  WHEN f.min_comp_ded IS NOT NULL THEN f.min_comp_ded
                                                                                  ELSE e.min_comp_ded
                                                                  END AS min_comp_ded ,
                                                                  CASE
                                                                                  WHEN f.max_comp_ded IS NOT NULL THEN f.max_comp_ded
                                                                                  ELSE e.max_comp_ded
                                                                  END AS max_comp_ded ,
                                                                  CASE
                                                                                  WHEN f.min_coll_ded IS NOT NULL THEN f.min_coll_ded
                                                                                  ELSE e.min_coll_ded
                                                                  END AS min_coll_ded ,
                                                                  CASE
                                                                                  WHEN f.max_coll_ded IS NOT NULL THEN f.max_coll_ded
                                                                                  ELSE e.max_coll_ded
                                                                  END AS max_coll_ded ,
                                                                  CASE
                                                                                  WHEN f.min_med_limit IS NOT NULL THEN f.min_med_limit
                                                                                  ELSE e.min_med_limit
                                                                  END AS min_med_limit ,
                                                                  CASE
                                                                                  WHEN f.max_med_limit IS NOT NULL THEN f.max_med_limit
                                                                                  ELSE e.max_med_limit
                                                                  END AS max_med_limit ,
                                                                  CASE
                                                                                  WHEN f.min_bi_limit IS NOT NULL THEN f.min_bi_limit
                                                                                  ELSE e.min_bi_limit
                                                                  END AS min_bi_limit ,
                                                                  CASE
                                                                                  WHEN f.max_bi_limit IS NOT NULL THEN f.max_bi_limit
                                                                                  ELSE e.max_bi_limit
                                                                  END AS max_bi_limit ,
                                                                  CASE
                                                                                  WHEN f.min_pd_limit IS NOT NULL THEN f.min_pd_limit
                                                                                  ELSE e.min_pd_limit
                                                                  END AS min_pd_limit ,
                                                                  CASE
                                                                                  WHEN f.max_pd_limit IS NOT NULL THEN f.max_pd_limit
                                                                                  ELSE e.max_pd_limit
                                                                  END AS max_pd_limit ,
                                                                  CASE
                                                                                  WHEN f.min_sl_limit IS NOT NULL THEN f.min_sl_limit
                                                                                  ELSE e.min_sl_limit
                                                                  END AS min_sl_limit ,
                                                                  CASE
                                                                                  WHEN f.max_sl_limit IS NOT NULL THEN f.max_sl_limit
                                                                                  ELSE e.max_sl_limit
                                                                  END AS max_sl_limit ,
                                                                  CASE
                                                                                  WHEN f.min_lou_limit IS NOT NULL THEN f.min_lou_limit
                                                                                  ELSE e.min_lou_limit
                                                                  END AS min_lou_limit ,
                                                                  CASE
                                                                                  WHEN f.max_lou_limit IS NOT NULL THEN f.max_lou_limit
                                                                                  ELSE e.max_lou_limit
                                                                  END AS max_lou_limit ,
                                                                  CASE
                                                                                  WHEN f.min_umbi_limit IS NOT NULL THEN f.min_umbi_limit
                                                                                  ELSE e.min_umbi_limit
                                                                  END AS min_umbi_limit ,
                                                                  CASE
                                                                                  WHEN f.max_umbi_limit IS NOT NULL THEN f.max_umbi_limit
                                                                                  ELSE e.max_umbi_limit
                                                                  END AS max_umbi_limit ,
                                                                  CASE
                                                                                  WHEN f.min_ers_limit IS NOT NULL THEN f.min_ers_limit
                                                                                  ELSE e.min_ers_limit
                                                                  END AS min_ers_limit ,
                                                                  CASE
                                                                                  WHEN f.max_ers_limit IS NOT NULL THEN f.max_ers_limit
                                                                                  ELSE e.max_ers_limit
                                                                  END AS max_ers_limit ,
                                                                  each_occurrence,
                                                                  damage_to_prem,
                                                                  gen_agg,
                                                                  prod_comp_ops
                                                  FROM            (
                                                                         SELECT plcy_nbr,
                                                                                clm_nbr,
                                                                                clm_loss_dt,
                                                                                total_incurred,
                                                                                total_reserves,
                                                                                plcy_type_cd,
                                                                                SUM(total_incurred - total_reserves) over (PARTITION BY clm_nbr) AS paid,
                                                                                cause_desc ,
                                                                                lob_cd
                                                                         FROM   (
                                                                                                SELECT DISTINCT plcy_nbr,
                                                                                                                clm_nbr,
                                                                                                                clm_loss_dt ,
                                                                                                                SUM(loss_ncat_amt + loss_cat_amt) over (PARTITION BY clm_nbr) AS total_incurred ,
                                                                                                                SUM(
                                                                                                                CASE
                                                                                                                                WHEN mo_id = (extract(year, to_date($american_ag_cal_dt))*100+extract(month, to_date($american_ag_cal_dt))) THEN reserve_ncat_amt + reserve_cat_amt
                                                                                                                                ELSE 0
                                                                                                                END) over (PARTITION BY clm_nbr) AS total_reserves
                                                                                                                /* !!!!!! = to <=              */
                                                                                                                ,
                                                                                                                max(cause_desc) over (PARTITION BY clm_nbr ORDER BY mo_id DESC) AS cause_desc ,
                                                                                                                lob_cd,
                                                                                                                plcy_type_cd
                                                                                                FROM            db_t_prod_anprod.lr_loss l
                                                                                                WHERE           l.mo_id > (extract(year, add_months($american_ag_cal_dt,-1))-10)*100+extract(month, to_date($american_ag_cal_dt))
                                                                                                AND             l.mo_id <=(extract(year, to_date($american_ag_cal_dt))*100+extract(month, to_date($american_ag_cal_dt)))
                                                                                                AND             l.source_sys_cd = ''GW''
                                                                                                AND             plcy_type_cd NOT LIKE ''UMBRELLA%''
                                                                                                AND             clm_nbr <> ''''
                                                                                                                /* and clm_nbr = ''C0000372387''              */
                                                                                ) a
                                                                         WHERE  total_incurred >= 500000 )b
                                                  left join
                                                                  (
                                                                                  SELECT DISTINCT c.clm_num,
                                                                                                  s.clm_sts_type_cd
                                                                                  FROM            db_v_prod_audt.d_clm c
                                                                                  join            db_v_prod_pres.d_clm_sts s
                                                                                  ON              s.clm_id = c.clm_id
                                                                                  WHERE           s.clm_sts_end_dt = ''9999-12-31'' ) c
                                                  ON              c.clm_num = b.clm_nbr
                                                  left join
                                                                  (
                                                                                  SELECT DISTINCT p.plcy_nbr ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN p.coverage_nm = ''DWELLING''
                                                                                                                  AND             p.term_nm = ''LIMIT'' THEN p.val_amount
                                                                                                                  ELSE 0
                                                                                                  END) over (PARTITION BY p.plcy_nbr ORDER BY p.mo_id DESC) AS dwelling ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN p.coverage_nm = ''PERSONAL PROPERTY''
                                                                                                                  AND             p.term_nm = ''LIMIT'' THEN p.val_amount
                                                                                                                  ELSE 0
                                                                                                  END) over (PARTITION BY p.plcy_nbr ORDER BY p.mo_id DESC) AS personal_property ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN p.coverage_nm = ''OTHER STRUCTURES''
                                                                                                                  AND             p.term_nm = ''LIMIT'' THEN p.val_amount
                                                                                                                  ELSE 0
                                                                                                  END) over (PARTITION BY p.plcy_nbr ORDER BY p.mo_id DESC) AS other_structure ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN p.coverage_nm = ''PERSONAL LIABILITY''
                                                                                                                  AND             p.term_nm = ''LIMIT'' THEN p.val_amount
                                                                                                                  ELSE 0
                                                                                                  END) over (PARTITION BY p.plcy_nbr ORDER BY p.mo_id DESC) AS personal_liability ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN p.coverage_nm = ''LOSS OF USE''
                                                                                                                  AND             p.term_nm = ''LIMIT'' THEN p.val_amount
                                                                                                                  ELSE 0
                                                                                                  END) over (PARTITION BY p.plcy_nbr ORDER BY p.mo_id DESC) AS loss_of_use ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN p.coverage_nm = ''MEDICAL PAYMENTS TO OTHERS''
                                                                                                                  AND             p.term_nm = ''LIMIT'' THEN p.val_amount
                                                                                                                  ELSE 0
                                                                                                  END) over (PARTITION BY p.plcy_nbr ORDER BY p.mo_id DESC) AS medical_payments_to_others ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN p.coverage_nm = ''HOME SYSTEMS PROTECTION''
                                                                                                                  AND             p.term_nm = ''LIMIT'' THEN p.val_amount
                                                                                                                  ELSE 0
                                                                                                  END) over (PARTITION BY p.plcy_nbr ORDER BY p.mo_id DESC) AS home_systems_protection
                                                                                  FROM            db_t_prod_anprod.property_details p
                                                                                  WHERE           p.mo_id > (extract(year, add_months($american_ag_cal_dt,-1))-10)*100+extract(month, to_date($american_ag_cal_dt))
                                                                                  AND             p.mo_id <=(extract(year, to_date($american_ag_cal_dt))*100+extract(month, to_date($american_ag_cal_dt))) ) d
                                                  ON              d.plcy_nbr = b.plcy_nbr
                                                  left join
                                                                  (
                                                                                  SELECT DISTINCT z.plcy_nbr ,
                                                                                                  min(z.comp_ded) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)     AS min_comp_ded,
                                                                                                  max(z.comp_ded) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)     AS max_comp_ded ,
                                                                                                  min(z.coll_ded) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)     AS min_coll_ded,
                                                                                                  max(z.coll_ded) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)     AS max_coll_ded ,
                                                                                                  min(z.med_limit) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)    AS min_med_limit,
                                                                                                  max(z.med_limit) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)    AS max_med_limit ,
                                                                                                  min(z.min_bi_lim_1) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC) AS min_bi_limit,
                                                                                                  max(z.max_bi_lim_1) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC) AS max_bi_limit ,
                                                                                                  min(z.pd_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)       AS min_pd_limit,
                                                                                                  max(z.pd_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)       AS max_pd_limit ,
                                                                                                  min(z.sl_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)       AS min_sl_limit,
                                                                                                  max(z.sl_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)       AS max_sl_limit ,
                                                                                                  min(z.lou_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)      AS min_lou_limit,
                                                                                                  max(z.lou_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)      AS max_lou_limit ,
                                                                                                  min(z.min_umbi_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC) AS min_umbi_limit,
                                                                                                  max(z.max_umbi_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC) AS max_umbi_limit ,
                                                                                                  min(z.ers_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)      AS min_ers_limit,
                                                                                                  max(z.ers_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)      AS max_ers_limit
                                                                                  FROM            (
                                                                                                         SELECT a.plcy_nbr,
                                                                                                                a.mo_id ,
                                                                                                                a.comp_ded  AS comp_ded ,
                                                                                                                a.coll_ded  AS coll_ded ,
                                                                                                                a.med_limit AS med_limit ,
                                                                                                                CASE
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',1) = ''25k'' THEN ''25000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',1) = ''50k'' THEN ''50000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',1) = ''100k'' THEN ''100000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',1) = ''250k'' THEN ''250000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',1) = ''300k'' THEN ''300000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',1) = ''500k'' THEN ''500000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',1) = ''1m'' THEN ''1000000''
                                                                                                                       ELSE ''0''
                                                                                                                END AS min_bi_lim_1 ,
                                                                                                                CASE
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',2) = ''50k'' THEN ''50000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',2) = ''100k'' THEN ''100000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',2) = ''300k'' THEN ''300000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',2) = ''500k'' THEN ''500000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',2) = ''1m'' THEN ''1000000''
                                                                                                                       ELSE ''0''
                                                                                                                END       AS max_bi_lim_1 ,
                                                                                                                a.pd_lim  AS pd_lim ,
                                                                                                                a.sl_lim  AS sl_lim ,
                                                                                                                a.lou_lim AS lou_lim ,
                                                                                                                CASE
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',1) = ''25k'' THEN ''25000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',1) = ''50k'' THEN ''50000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',1) = ''100k'' THEN ''100000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',1) = ''250k'' THEN ''250000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',1) = ''300k'' THEN ''300000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',1) = ''500k'' THEN ''500000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',1) = ''1m'' THEN ''1000000''
                                                                                                                       ELSE ''0''
                                                                                                                END AS min_umbi_lim ,
                                                                                                                CASE
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',2) = ''50k'' THEN ''50000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',2) = ''100k'' THEN ''100000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',2) = ''300k'' THEN ''300000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',2) = ''500k'' THEN ''500000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',2) = ''1m'' THEN ''1000000''
                                                                                                                       ELSE ''0''
                                                                                                                END       AS max_umbi_lim ,
                                                                                                                a.ers_lim AS ers_lim
                                                                                                         FROM   db_t_prod_anprod.auto_details a
                                                                                                         WHERE  a.mo_id > (extract(year, add_months($american_ag_cal_dt,-1))-10)*100+extract(month, to_date($american_ag_cal_dt))
                                                                                                         AND    a.mo_id <=(extract(year, to_date($american_ag_cal_dt))*100+extract(month, to_date($american_ag_cal_dt))) ) z ) e
                                                  ON              e.plcy_nbr = b.plcy_nbr
                                                  left join
                                                                  (
                                                                                  SELECT DISTINCT x.plcy_nbr ,
                                                                                                  min(x.comp_ded) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)     AS min_comp_ded,
                                                                                                  max(x.comp_ded) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)     AS max_comp_ded ,
                                                                                                  min(x.coll_ded) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)     AS min_coll_ded,
                                                                                                  max(x.coll_ded) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)     AS max_coll_ded ,
                                                                                                  min(x.med_limit) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)    AS min_med_limit,
                                                                                                  max(x.med_limit) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)    AS max_med_limit ,
                                                                                                  min(x.min_bi_lim_1) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC) AS min_bi_limit,
                                                                                                  max(x.max_bi_lim_1) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC) AS max_bi_limit ,
                                                                                                  min(x.pd_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)       AS min_pd_limit,
                                                                                                  max(x.pd_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)       AS max_pd_limit ,
                                                                                                  min(x.sl_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)       AS min_sl_limit,
                                                                                                  max(x.sl_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)       AS max_sl_limit ,
                                                                                                  min(x.lou_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)      AS min_lou_limit,
                                                                                                  max(x.lou_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)      AS max_lou_limit ,
                                                                                                  min(x.min_umbi_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC) AS min_umbi_limit,
                                                                                                  max(x.max_umbi_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC) AS max_umbi_limit ,
                                                                                                  min(x.ers_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)      AS min_ers_limit,
                                                                                                  max(x.ers_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)      AS max_ers_limit
                                                                                  FROM            (
                                                                                                         SELECT v.plcy_nbr,
                                                                                                                v.mo_id ,
                                                                                                                v.comp_ded  AS comp_ded ,
                                                                                                                v.coll_ded  AS coll_ded ,
                                                                                                                v.med_limit AS med_limit ,
                                                                                                                CASE
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',1) = ''25k'' THEN ''25000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',1) = ''50k'' THEN ''50000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',1) = ''100k'' THEN ''100000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',1) = ''250k'' THEN ''250000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',1) = ''300k'' THEN ''300000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',1) = ''500k'' THEN ''500000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',1) = ''1m'' THEN ''1000000''
                                                                                                                       ELSE ''0''
                                                                                                                END AS min_bi_lim_1 ,
                                                                                                                CASE
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',2) = ''50k'' THEN ''50000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',2) = ''100k'' THEN ''100000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',2) = ''300k'' THEN ''300000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',2) = ''500k'' THEN ''500000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',2) = ''1m'' THEN ''1000000''
                                                                                                                       ELSE ''0''
                                                                                                                END       AS max_bi_lim_1 ,
                                                                                                                v.pd_lim  AS pd_lim ,
                                                                                                                v.sl_lim  AS sl_lim ,
                                                                                                                v.lou_lim AS lou_lim ,
                                                                                                                CASE
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',1) = ''25k'' THEN ''25000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',1) = ''50k'' THEN ''50000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',1) = ''100k'' THEN ''100000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',1) = ''250k'' THEN ''250000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',1) = ''300k'' THEN ''300000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',1) = ''500k'' THEN ''500000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',1) = ''1m'' THEN ''1000000''
                                                                                                                       ELSE ''0''
                                                                                                                END AS min_umbi_lim ,
                                                                                                                CASE
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',2) = ''50k'' THEN ''50000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',2) = ''100k'' THEN ''100000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',2) = ''300k'' THEN ''300000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',2) = ''500k'' THEN ''500000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',2) = ''1m'' THEN ''1000000''
                                                                                                                       ELSE ''0''
                                                                                                                END       AS max_umbi_lim ,
                                                                                                                v.ers_lim AS ers_lim
                                                                                                         FROM   db_t_prod_anprod.auto_veh_details v
                                                                                                         WHERE  v.mo_id > (extract(year, add_months($american_ag_cal_dt,-1))-10)*100+extract(month, to_date($american_ag_cal_dt))
                                                                                                         AND    v.mo_id <=(extract(year, to_date($american_ag_cal_dt))*100+extract(month, to_date($american_ag_cal_dt))) ) x ) f
                                                  ON              f.plcy_nbr = b.plcy_nbr
                                                  left join
                                                                  (
                                                                                  SELECT DISTINCT a.host_agmt_num ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN feat2.feat_desc = ''EACH OCCURRENCE LIMIT-LIMIT''
                                                                                                                  AND             feat2.feat_sbtype_cd = ''TERM'' THEN feat.feat_dtl_val
                                                                                                  END) over (PARTITION BY a.host_agmt_num ORDER BY a.term_num DESC) AS each_occurrence ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN feat.feat_desc IN (''- Damage To Premises Rented To You-Limit'')
                                                                                                                  AND             feat.feat_sbtype_cd = ''TERM'' THEN af.agmt_feat_amt
                                                                                                  END) over (PARTITION BY a.host_agmt_num ORDER BY a.term_num DESC) AS damage_to_prem ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN feat2.feat_desc = ''GENERAL AGGREGATE LIMIT-LIMIT''
                                                                                                                  AND             feat2.feat_sbtype_cd = ''TERM'' THEN feat.feat_dtl_val
                                                                                                  END) over (PARTITION BY a.host_agmt_num ORDER BY a.term_num DESC) AS gen_agg ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN feat2.feat_desc = ''PRODUCTS/COMPLETED OPS. AGGREGATE LIMIT-LIMIT''
                                                                                                                  AND             feat2.feat_sbtype_cd = ''TERM'' THEN feat.feat_dtl_val
                                                                                                  END) over (PARTITION BY a.host_agmt_num ORDER BY a.term_num DESC) AS prod_comp_ops
                                                                                  FROM            db_t_prod_core.agmt a
                                                                                  join            db_t_prod_core.agmt_prod ap
                                                                                  ON              ap.agmt_id = a.agmt_id
                                                                                  join            db_t_prod_core.prod pr
                                                                                  ON              pr.prod_id = ap.prod_id
                                                                                  join            db_t_prod_core.agmt_feat af
                                                                                  ON              af.agmt_id = a.agmt_id
                                                                                  join            db_t_prod_core.feat 
                                                                                  ON              feat.feat_id = af.feat_id
                                                                                  join            db_t_prod_core.feat_rltd fr
                                                                                  ON              fr.rltd_feat_id = feat.feat_id
                                                                                  join            db_t_prod_core.feat feat2
                                                                                  ON              feat2.feat_id = fr.feat_id
                                                                                  AND             feat2.feat_desc IN (''Each Occurrence Limit-Limit'',
                                                                                                                      ''General Aggregate Limit-Limit'',
                                                                                                                      ''Medical Expenses - Per Person Limit-Limit'',
                                                                                                                      ''Products/Completed Ops. Aggregate Limit-Limit'',
                                                                                                                      ''Damage To Premises Rented To You-Limit OR Increased Damage To Premises Rented To You-Limit'',
                                                                                                                      ''Business Liability'')
                                                                                  WHERE           prod_name IN (''BUSINESSOWNERS'' ,
                                                                                                                ''CHURCH'') ) g
                                                  ON              g.host_agmt_num = b.plcy_nbr
                                                  left join
                                                                  (
                                                                           SELECT   clm_num,
                                                                                    SUM(mntary_amt) AS expense
                                                                           FROM     db_v_prod_act.w_loss_info_act
                                                                           WHERE    acct_cd IN (''521004'',
                                                                                                ''523004'',
                                                                                                ''525004'',
                                                                                                ''526554'')
                                                                           AND      plcy_type IS NOT NULL
                                                                           AND      mo_id > (extract(year, add_months($american_ag_cal_dt,-1))-10)*100+extract(month, to_date($american_ag_cal_dt))
                                                                           AND      mo_id <=(extract(year, to_date($american_ag_cal_dt))*100+extract(month, to_date($american_ag_cal_dt)))
                                                                                    /* clm_num = ''H0000487282''              */
                                                                           GROUP BY clm_num ) h
                                                  ON              h.clm_num = b.clm_nbr
                                                  /* and H.PLCY_NUM = B.PLCY_NBR              */
                                                  UNION
                                                  SELECT DISTINCT b.plcy_nbr,
                                                                  b.clm_nbr,
                                                                  clm_loss_dt,
                                                                  total_incurred,
                                                                  expense,
                                                                  total_reserves,
                                                                  paid,
                                                                  cause_desc,
                                                                  lob_cd ,
                                                                  clm_sts_type_cd ,
                                                                  dwelling,
                                                                  personal_property,
                                                                  other_structure,
                                                                  personal_liability,
                                                                  loss_of_use,
                                                                  medical_payments_to_others,
                                                                  home_systems_protection ,
                                                                  CASE
                                                                                  WHEN f.min_comp_ded IS NOT NULL THEN f.min_comp_ded
                                                                                  ELSE e.min_comp_ded
                                                                  END AS min_comp_ded ,
                                                                  CASE
                                                                                  WHEN f.max_comp_ded IS NOT NULL THEN f.max_comp_ded
                                                                                  ELSE e.max_comp_ded
                                                                  END AS max_comp_ded ,
                                                                  CASE
                                                                                  WHEN f.min_coll_ded IS NOT NULL THEN f.min_coll_ded
                                                                                  ELSE e.min_coll_ded
                                                                  END AS min_coll_ded ,
                                                                  CASE
                                                                                  WHEN f.max_coll_ded IS NOT NULL THEN f.max_coll_ded
                                                                                  ELSE e.max_coll_ded
                                                                  END AS max_coll_ded ,
                                                                  CASE
                                                                                  WHEN f.min_med_limit IS NOT NULL THEN f.min_med_limit
                                                                                  ELSE e.min_med_limit
                                                                  END AS min_med_limit ,
                                                                  CASE
                                                                                  WHEN f.max_med_limit IS NOT NULL THEN f.max_med_limit
                                                                                  ELSE e.max_med_limit
                                                                  END AS max_med_limit ,
                                                                  CASE
                                                                                  WHEN f.min_bi_limit IS NOT NULL THEN f.min_bi_limit
                                                                                  ELSE e.min_bi_limit
                                                                  END AS min_bi_limit ,
                                                                  CASE
                                                                                  WHEN f.max_bi_limit IS NOT NULL THEN f.max_bi_limit
                                                                                  ELSE e.max_bi_limit
                                                                  END AS max_bi_limit ,
                                                                  CASE
                                                                                  WHEN f.min_pd_limit IS NOT NULL THEN f.min_pd_limit
                                                                                  ELSE e.min_pd_limit
                                                                  END AS min_pd_limit ,
                                                                  CASE
                                                                                  WHEN f.max_pd_limit IS NOT NULL THEN f.max_pd_limit
                                                                                  ELSE e.max_pd_limit
                                                                  END AS max_pd_limit ,
                                                                  CASE
                                                                                  WHEN f.min_sl_limit IS NOT NULL THEN f.min_sl_limit
                                                                                  ELSE e.min_sl_limit
                                                                  END AS min_sl_limit ,
                                                                  CASE
                                                                                  WHEN f.max_sl_limit IS NOT NULL THEN f.max_sl_limit
                                                                                  ELSE e.max_sl_limit
                                                                  END AS max_sl_limit ,
                                                                  CASE
                                                                                  WHEN f.min_lou_limit IS NOT NULL THEN f.min_lou_limit
                                                                                  ELSE e.min_lou_limit
                                                                  END AS min_lou_limit ,
                                                                  CASE
                                                                                  WHEN f.max_lou_limit IS NOT NULL THEN f.max_lou_limit
                                                                                  ELSE e.max_lou_limit
                                                                  END AS max_lou_limit ,
                                                                  CASE
                                                                                  WHEN f.min_umbi_limit IS NOT NULL THEN f.min_umbi_limit
                                                                                  ELSE e.min_umbi_limit
                                                                  END AS min_umbi_limit ,
                                                                  CASE
                                                                                  WHEN f.max_umbi_limit IS NOT NULL THEN f.max_umbi_limit
                                                                                  ELSE e.max_umbi_limit
                                                                  END AS max_umbi_limit ,
                                                                  CASE
                                                                                  WHEN f.min_ers_limit IS NOT NULL THEN f.min_ers_limit
                                                                                  ELSE e.min_ers_limit
                                                                  END AS min_ers_limit ,
                                                                  CASE
                                                                                  WHEN f.max_ers_limit IS NOT NULL THEN f.max_ers_limit
                                                                                  ELSE e.max_ers_limit
                                                                  END AS max_ers_limit ,
                                                                  each_occurrence,
                                                                  damage_to_prem,
                                                                  gen_agg,
                                                                  prod_comp_ops
                                                  FROM            (
                                                                         SELECT plcy_nbr,
                                                                                clm_nbr,
                                                                                clm_loss_dt,
                                                                                total_incurred,
                                                                                total_reserves,
                                                                                plcy_type_cd,
                                                                                SUM(total_incurred - total_reserves) over (PARTITION BY clm_nbr) AS paid,
                                                                                cause_desc ,
                                                                                lob_cd
                                                                         FROM   (
                                                                                                SELECT DISTINCT plcy_nbr,
                                                                                                                clm_nbr,
                                                                                                                clm_loss_dt ,
                                                                                                                SUM(loss_ncat_amt + loss_cat_amt) over (PARTITION BY clm_nbr) AS total_incurred ,
                                                                                                                SUM(
                                                                                                                CASE
                                                                                                                                WHEN mo_id = (extract(year, to_date($american_ag_cal_dt))*100+extract(month, to_date($american_ag_cal_dt))) THEN reserve_ncat_amt + reserve_cat_amt
                                                                                                                                ELSE 0
                                                                                                                END) over (PARTITION BY clm_nbr) AS total_reserves
                                                                                                                /* !!!!!! = to <=              */
                                                                                                                ,
                                                                                                                max(cause_desc) over (PARTITION BY clm_nbr ORDER BY mo_id DESC) AS cause_desc ,
                                                                                                                lob_cd,
                                                                                                                plcy_type_cd
                                                                                                FROM            db_t_prod_anprod.lr_loss l
                                                                                                WHERE           l.mo_id > (extract(year, add_months($american_ag_cal_dt,-1))-10)*100+extract(month, to_date($american_ag_cal_dt))
                                                                                                AND             l.mo_id <=(extract(year, to_date($american_ag_cal_dt))*100+extract(month, to_date($american_ag_cal_dt)))
                                                                                                AND             l.source_sys_cd = ''GW''
                                                                                                AND             plcy_type_cd LIKE ''UMBRELLA%''
                                                                                                AND             clm_nbr <> ''''
                                                                                                                /* and clm_nbr = ''C0000372387''              */
                                                                                ) a
                                                                                /* where TOTAL_INCURRED >= 300000              */
                                                                  )b
                                                  left join
                                                                  (
                                                                                  SELECT DISTINCT c.clm_num,
                                                                                                  s.clm_sts_type_cd
                                                                                  FROM            db_v_prod_audt.d_clm c
                                                                                  join            db_v_prod_pres.d_clm_sts s
                                                                                  ON              s.clm_id = c.clm_id
                                                                                  WHERE           s.clm_sts_end_dt = ''9999-12-31'' ) c
                                                  ON              c.clm_num = b.clm_nbr
                                                  left join
                                                                  (
                                                                                  SELECT DISTINCT p.plcy_nbr ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN p.coverage_nm = ''DWELLING''
                                                                                                                  AND             p.term_nm = ''LIMIT'' THEN p.val_amount
                                                                                                                  ELSE 0
                                                                                                  END) over (PARTITION BY p.plcy_nbr ORDER BY p.mo_id DESC) AS dwelling ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN p.coverage_nm = ''PERSONAL PROPERTY''
                                                                                                                  AND             p.term_nm = ''LIMIT'' THEN p.val_amount
                                                                                                                  ELSE 0
                                                                                                  END) over (PARTITION BY p.plcy_nbr ORDER BY p.mo_id DESC) AS personal_property ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN p.coverage_nm = ''OTHER STRUCTURES''
                                                                                                                  AND             p.term_nm = ''LIMIT'' THEN p.val_amount
                                                                                                                  ELSE 0
                                                                                                  END) over (PARTITION BY p.plcy_nbr ORDER BY p.mo_id DESC) AS other_structure ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN p.coverage_nm = ''PERSONAL LIABILITY''
                                                                                                                  AND             p.term_nm = ''LIMIT'' THEN p.val_amount
                                                                                                                  ELSE 0
                                                                                                  END) over (PARTITION BY p.plcy_nbr ORDER BY p.mo_id DESC) AS personal_liability ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN p.coverage_nm = ''LOSS OF USE''
                                                                                                                  AND             p.term_nm = ''LIMIT'' THEN p.val_amount
                                                                                                                  ELSE 0
                                                                                                  END) over (PARTITION BY p.plcy_nbr ORDER BY p.mo_id DESC) AS loss_of_use ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN p.coverage_nm = ''MEDICAL PAYMENTS TO OTHERS''
                                                                                                                  AND             p.term_nm = ''LIMIT'' THEN p.val_amount
                                                                                                                  ELSE 0
                                                                                                  END) over (PARTITION BY p.plcy_nbr ORDER BY p.mo_id DESC) AS medical_payments_to_others ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN p.coverage_nm = ''HOME SYSTEMS PROTECTION''
                                                                                                                  AND             p.term_nm = ''LIMIT'' THEN p.val_amount
                                                                                                                  ELSE 0
                                                                                                  END) over (PARTITION BY p.plcy_nbr ORDER BY p.mo_id DESC) AS home_systems_protection
                                                                                  FROM            db_t_prod_anprod.property_details p
                                                                                  WHERE           p.mo_id > (extract(year, add_months($american_ag_cal_dt,-1))-10)*100+extract(month, to_date($american_ag_cal_dt))
                                                                                  AND             p.mo_id <=(extract(year, to_date($american_ag_cal_dt))*100+extract(month, to_date($american_ag_cal_dt))) ) d
                                                  ON              d.plcy_nbr = b.plcy_nbr
                                                  left join
                                                                  (
                                                                                  SELECT DISTINCT z.plcy_nbr ,
                                                                                                  min(z.comp_ded) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)     AS min_comp_ded,
                                                                                                  max(z.comp_ded) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)     AS max_comp_ded ,
                                                                                                  min(z.coll_ded) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)     AS min_coll_ded,
                                                                                                  max(z.coll_ded) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)     AS max_coll_ded ,
                                                                                                  min(z.med_limit) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)    AS min_med_limit,
                                                                                                  max(z.med_limit) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)    AS max_med_limit ,
                                                                                                  min(z.min_bi_lim_1) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC) AS min_bi_limit,
                                                                                                  max(z.max_bi_lim_1) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC) AS max_bi_limit ,
                                                                                                  min(z.pd_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)       AS min_pd_limit,
                                                                                                  max(z.pd_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)       AS max_pd_limit ,
                                                                                                  min(z.sl_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)       AS min_sl_limit,
                                                                                                  max(z.sl_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)       AS max_sl_limit ,
                                                                                                  min(z.lou_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)      AS min_lou_limit,
                                                                                                  max(z.lou_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)      AS max_lou_limit ,
                                                                                                  min(z.min_umbi_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC) AS min_umbi_limit,
                                                                                                  max(z.max_umbi_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC) AS max_umbi_limit ,
                                                                                                  min(z.ers_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)      AS min_ers_limit,
                                                                                                  max(z.ers_lim) over (PARTITION BY z.plcy_nbr ORDER BY z.mo_id DESC)      AS max_ers_limit
                                                                                  FROM            (
                                                                                                         SELECT a.plcy_nbr,
                                                                                                                a.mo_id ,
                                                                                                                a.comp_ded  AS comp_ded ,
                                                                                                                a.coll_ded  AS coll_ded ,
                                                                                                                a.med_limit AS med_limit ,
                                                                                                                CASE
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',1) = ''25k'' THEN ''25000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',1) = ''50k'' THEN ''50000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',1) = ''100k'' THEN ''100000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',1) = ''250k'' THEN ''250000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',1) = ''300k'' THEN ''300000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',1) = ''500k'' THEN ''500000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',1) = ''1m'' THEN ''1000000''
                                                                                                                       ELSE ''0''
                                                                                                                END AS min_bi_lim_1 ,
                                                                                                                CASE
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',2) = ''50k'' THEN ''50000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',2) = ''100k'' THEN ''100000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',2) = ''300k'' THEN ''300000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',2) = ''500k'' THEN ''500000''
                                                                                                                       WHEN strtok(a.bi_lim_1, ''_'',2) = ''1m'' THEN ''1000000''
                                                                                                                       ELSE ''0''
                                                                                                                END       AS max_bi_lim_1 ,
                                                                                                                a.pd_lim  AS pd_lim ,
                                                                                                                a.sl_lim  AS sl_lim ,
                                                                                                                a.lou_lim AS lou_lim ,
                                                                                                                CASE
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',1) = ''25k'' THEN ''25000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',1) = ''50k'' THEN ''50000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',1) = ''100k'' THEN ''100000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',1) = ''250k'' THEN ''250000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',1) = ''300k'' THEN ''300000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',1) = ''500k'' THEN ''500000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',1) = ''1m'' THEN ''1000000''
                                                                                                                       ELSE ''0''
                                                                                                                END AS min_umbi_lim ,
                                                                                                                CASE
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',2) = ''50k'' THEN ''50000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',2) = ''100k'' THEN ''100000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',2) = ''300k'' THEN ''300000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',2) = ''500k'' THEN ''500000''
                                                                                                                       WHEN strtok(a.umbi_lim, ''_'',2) = ''1m'' THEN ''1000000''
                                                                                                                       ELSE ''0''
                                                                                                                END       AS max_umbi_lim ,
                                                                                                                a.ers_lim AS ers_lim
                                                                                                         FROM   db_t_prod_anprod.auto_details a
                                                                                                         WHERE  a.mo_id > (extract(year, add_months($american_ag_cal_dt,-1))-10)*100+extract(month, to_date($american_ag_cal_dt))
                                                                                                         AND    a.mo_id <=(extract(year, to_date($american_ag_cal_dt))*100+extract(month, to_date($american_ag_cal_dt))) ) z ) e
                                                  ON              e.plcy_nbr = b.plcy_nbr
                                                  left join
                                                                  (
                                                                                  SELECT DISTINCT x.plcy_nbr ,
                                                                                                  min(x.comp_ded) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)     AS min_comp_ded,
                                                                                                  max(x.comp_ded) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)     AS max_comp_ded ,
                                                                                                  min(x.coll_ded) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)     AS min_coll_ded,
                                                                                                  max(x.coll_ded) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)     AS max_coll_ded ,
                                                                                                  min(x.med_limit) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)    AS min_med_limit,
                                                                                                  max(x.med_limit) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)    AS max_med_limit ,
                                                                                                  min(x.min_bi_lim_1) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC) AS min_bi_limit,
                                                                                                  max(x.max_bi_lim_1) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC) AS max_bi_limit ,
                                                                                                  min(x.pd_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)       AS min_pd_limit,
                                                                                                  max(x.pd_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)       AS max_pd_limit ,
                                                                                                  min(x.sl_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)       AS min_sl_limit,
                                                                                                  max(x.sl_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)       AS max_sl_limit ,
                                                                                                  min(x.lou_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)      AS min_lou_limit,
                                                                                                  max(x.lou_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)      AS max_lou_limit ,
                                                                                                  min(x.min_umbi_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC) AS min_umbi_limit,
                                                                                                  max(x.max_umbi_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC) AS max_umbi_limit ,
                                                                                                  min(x.ers_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)      AS min_ers_limit,
                                                                                                  max(x.ers_lim) over (PARTITION BY x.plcy_nbr ORDER BY x.mo_id DESC)      AS max_ers_limit
                                                                                  FROM            (
                                                                                                         SELECT v.plcy_nbr,
                                                                                                                v.mo_id ,
                                                                                                                v.comp_ded  AS comp_ded ,
                                                                                                                v.coll_ded  AS coll_ded ,
                                                                                                                v.med_limit AS med_limit ,
                                                                                                                CASE
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',1) = ''25k'' THEN ''25000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',1) = ''50k'' THEN ''50000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',1) = ''100k'' THEN ''100000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',1) = ''250k'' THEN ''250000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',1) = ''300k'' THEN ''300000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',1) = ''500k'' THEN ''500000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',1) = ''1m'' THEN ''1000000''
                                                                                                                       ELSE ''0''
                                                                                                                END AS min_bi_lim_1 ,
                                                                                                                CASE
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',2) = ''50k'' THEN ''50000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',2) = ''100k'' THEN ''100000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',2) = ''300k'' THEN ''300000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',2) = ''500k'' THEN ''500000''
                                                                                                                       WHEN strtok(v.bi_lim_1, ''_'',2) = ''1m'' THEN ''1000000''
                                                                                                                       ELSE ''0''
                                                                                                                END       AS max_bi_lim_1 ,
                                                                                                                v.pd_lim  AS pd_lim ,
                                                                                                                v.sl_lim  AS sl_lim ,
                                                                                                                v.lou_lim AS lou_lim ,
                                                                                                                CASE
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',1) = ''25k'' THEN ''25000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',1) = ''50k'' THEN ''50000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',1) = ''100k'' THEN ''100000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',1) = ''250k'' THEN ''250000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',1) = ''300k'' THEN ''300000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',1) = ''500k'' THEN ''500000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',1) = ''1m'' THEN ''1000000''
                                                                                                                       ELSE ''0''
                                                                                                                END AS min_umbi_lim ,
                                                                                                                CASE
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',2) = ''50k'' THEN ''50000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',2) = ''100k'' THEN ''100000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',2) = ''300k'' THEN ''300000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',2) = ''500k'' THEN ''500000''
                                                                                                                       WHEN strtok(v.umbi_lim, ''_'',2) = ''1m'' THEN ''1000000''
                                                                                                                       ELSE ''0''
                                                                                                                END       AS max_umbi_lim ,
                                                                                                                v.ers_lim AS ers_lim
                                                                                                         FROM   db_t_prod_anprod.auto_veh_details v
                                                                                                         WHERE  v.mo_id > (extract(year, add_months($american_ag_cal_dt,-1))-10)*100+extract(month, to_date($american_ag_cal_dt))
                                                                                                         AND    v.mo_id <=(extract(year, to_date($american_ag_cal_dt))*100+extract(month, to_date($american_ag_cal_dt))) ) x ) f
                                                  ON              f.plcy_nbr = b.plcy_nbr
                                                  left join
                                                                  (
                                                                                  SELECT DISTINCT a.host_agmt_num ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN feat2.feat_desc = ''EACH OCCURRENCE LIMIT-LIMIT''
                                                                                                                  AND             feat2.feat_sbtype_cd = ''TERM'' THEN feat.feat_dtl_val
                                                                                                  END) over (PARTITION BY a.host_agmt_num ORDER BY a.term_num DESC) AS each_occurrence ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN feat.feat_desc IN (''- Damage To Premises Rented To You-Limit'')
                                                                                                                  AND             feat.feat_sbtype_cd = ''TERM'' THEN af.agmt_feat_amt
                                                                                                  END) over (PARTITION BY a.host_agmt_num ORDER BY a.term_num DESC) AS damage_to_prem ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN feat2.feat_desc = ''GENERAL AGGREGATE LIMIT-LIMIT''
                                                                                                                  AND             feat2.feat_sbtype_cd = ''TERM'' THEN feat.feat_dtl_val
                                                                                                  END) over (PARTITION BY a.host_agmt_num ORDER BY a.term_num DESC) AS gen_agg ,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN feat2.feat_desc = ''PRODUCTS/COMPLETED OPS. AGGREGATE LIMIT-LIMIT''
                                                                                                                  AND             feat2.feat_sbtype_cd = ''TERM'' THEN feat.feat_dtl_val
                                                                                                  END) over (PARTITION BY a.host_agmt_num ORDER BY a.term_num DESC) AS prod_comp_ops
                                                                                  FROM            db_t_prod_core.agmt a
                                                                                  join            db_t_prod_core.agmt_prod ap
                                                                                  ON              ap.agmt_id = a.agmt_id
                                                                                  join            db_t_prod_core.prod pr
                                                                                  ON              pr.prod_id = ap.prod_id
                                                                                  join            db_t_prod_core.agmt_feat af
                                                                                  ON              af.agmt_id = a.agmt_id
                                                                                  join            db_t_prod_core.feat 
                                                                                  ON              feat.feat_id = af.feat_id
                                                                                  join            db_t_prod_core.feat_rltd fr
                                                                                  ON              fr.rltd_feat_id = feat.feat_id
                                                                                  join            db_t_prod_core.feat feat2
                                                                                  ON              feat2.feat_id = fr.feat_id
                                                                                  AND             feat2.feat_desc IN (''Each Occurrence Limit-Limit'',
                                                                                                                      ''General Aggregate Limit-Limit'',
                                                                                                                      ''Medical Expenses - Per Person Limit-Limit'',
                                                                                                                      ''Products/Completed Ops. Aggregate Limit-Limit'',
                                                                                                                      ''Damage To Premises Rented To You-Limit OR Increased Damage To Premises Rented To You-Limit'',
                                                                                                                      ''Business Liability'')
                                                                                  WHERE           prod_name IN (''BUSINESSOWNERS'' ,
                                                                                                                ''CHURCH'') ) g
                                                  ON              g.host_agmt_num = b.plcy_nbr
                                                  left join
                                                                  (
                                                                           SELECT   clm_num,
                                                                                    SUM(mntary_amt) AS expense
                                                                           FROM     db_v_prod_act.w_loss_info_act
                                                                           WHERE    acct_cd IN (''521004'',
                                                                                                ''523004'',
                                                                                                ''525004'',
                                                                                                ''526554'')
                                                                           AND      plcy_type IS NOT NULL
                                                                           AND      mo_id > (extract(year, add_months($american_ag_cal_dt,-1))-10)*100+extract(month, to_date($american_ag_cal_dt))
                                                                           AND      mo_id <=(extract(year, to_date($american_ag_cal_dt))*100+extract(month, to_date($american_ag_cal_dt)))
                                                                                    /* clm_num = ''H0000487282''              */
                                                                           GROUP BY clm_num ) h
                                                  ON              h.clm_num = b.clm_nbr
                                                                  /* and H.PLCY_NUM = B.PLCY_NBR              */
                                                  ORDER BY        1,
                                                                  2 ) src ) );
  -- Component American_AG_Largest_Loss_Claims_View, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE american_ag_largest_loss_claims_view AS
  (
         SELECT sq_gw_largest_loss_claims_view.plcy_nbr                   AS plcy_nbr,
                sq_gw_largest_loss_claims_view.clm_nbr                    AS clm_nbr,
                sq_gw_largest_loss_claims_view.clm_loss_dt                AS clm_loss_dt,
                sq_gw_largest_loss_claims_view.total_incurred             AS total_incurred,
                sq_gw_largest_loss_claims_view.expense                    AS expense,
                sq_gw_largest_loss_claims_view.total_reserves             AS total_reserves,
                sq_gw_largest_loss_claims_view.paid                       AS paid,
                sq_gw_largest_loss_claims_view.cause_desc                 AS cause_desc,
                sq_gw_largest_loss_claims_view.lob_cd                     AS lob_cd,
                sq_gw_largest_loss_claims_view.clm_sts_type_cd            AS clm_sts_type_cd,
                sq_gw_largest_loss_claims_view.dwelling                   AS dwelling,
                sq_gw_largest_loss_claims_view.personal_property          AS personal_property,
                sq_gw_largest_loss_claims_view.other_structure            AS other_structure,
                sq_gw_largest_loss_claims_view.personal_liability         AS personal_liability,
                sq_gw_largest_loss_claims_view.loss_of_use                AS loss_of_use,
                sq_gw_largest_loss_claims_view.medical_payments_to_others AS medical_payments_to_others,
                sq_gw_largest_loss_claims_view.home_systems_protection    AS home_systems_protection,
                sq_gw_largest_loss_claims_view.min_comp_ded               AS min_comp_ded,
                sq_gw_largest_loss_claims_view.max_comp_ded               AS max_comp_ded,
                sq_gw_largest_loss_claims_view.min_coll_ded               AS min_coll_ded,
                sq_gw_largest_loss_claims_view.max_coll_ded               AS max_coll_ded,
                sq_gw_largest_loss_claims_view.min_med_limit              AS min_med_limit,
                sq_gw_largest_loss_claims_view.max_med_limit              AS max_med_limit,
                sq_gw_largest_loss_claims_view.min_bi_limit               AS min_bi_limit,
                sq_gw_largest_loss_claims_view.max_bi_limit               AS max_bi_limit,
                sq_gw_largest_loss_claims_view.min_pd_limit               AS min_pd_limit,
                sq_gw_largest_loss_claims_view.max_pd_limit               AS max_pd_limit,
                sq_gw_largest_loss_claims_view.min_sl_limit               AS min_sl_limit,
                sq_gw_largest_loss_claims_view.max_sl_limit               AS max_sl_limit,
                sq_gw_largest_loss_claims_view.min_lou_limit              AS min_lou_limit,
                sq_gw_largest_loss_claims_view.max_lou_limit              AS max_lou_limit,
                sq_gw_largest_loss_claims_view.min_umbi_limit             AS min_umbi_limit,
                sq_gw_largest_loss_claims_view.max_umbi_limit             AS max_umbi_limit,
                sq_gw_largest_loss_claims_view.min_ers_limit              AS min_ers_limit,
                sq_gw_largest_loss_claims_view.max_ers_limit              AS max_ers_limit,
                sq_gw_largest_loss_claims_view.each_occurrence            AS each_occurrence,
                sq_gw_largest_loss_claims_view.damage_to_prem             AS damage_to_prem,
                sq_gw_largest_loss_claims_view.gen_agg                    AS gen_agg,
                sq_gw_largest_loss_claims_view.prod_comp_ops              AS prod_comp_ops,
                sq_gw_largest_loss_claims_view.source_record_id
         FROM   sq_gw_largest_loss_claims_view );
  -- Component TGT_GW_Largest_Loss_Claims_View, Type TARGET_EXPORT_PREPARE Stage data before exporting
  CREATE
  OR
  replace TEMPORARY TABLE tgt_gw_largest_loss_claims_view AS
  (
         SELECT american_ag_largest_loss_claims_view.plcy_nbr                   AS plcy_nbr,
                american_ag_largest_loss_claims_view.clm_nbr                    AS clm_nbr,
                american_ag_largest_loss_claims_view.clm_loss_dt                AS clm_loss_dt,
                american_ag_largest_loss_claims_view.total_incurred             AS total_incurred,
                american_ag_largest_loss_claims_view.expense                    AS expense,
                american_ag_largest_loss_claims_view.total_reserves             AS total_reserves,
                american_ag_largest_loss_claims_view.paid                       AS paid,
                american_ag_largest_loss_claims_view.cause_desc                 AS cause_desc,
                american_ag_largest_loss_claims_view.lob_cd                     AS lob_cd,
                american_ag_largest_loss_claims_view.clm_sts_type_cd            AS clm_sts_type_cd,
                american_ag_largest_loss_claims_view.dwelling                   AS dwelling,
                american_ag_largest_loss_claims_view.personal_property          AS personal_property,
                american_ag_largest_loss_claims_view.other_structure            AS other_structure,
                american_ag_largest_loss_claims_view.personal_liability         AS personal_liability,
                american_ag_largest_loss_claims_view.loss_of_use                AS loss_of_use,
                american_ag_largest_loss_claims_view.medical_payments_to_others AS medical_payments_to_others,
                american_ag_largest_loss_claims_view.home_systems_protection    AS home_systems_protection,
                american_ag_largest_loss_claims_view.min_comp_ded               AS min_comp_ded,
                american_ag_largest_loss_claims_view.max_comp_ded               AS max_comp_ded,
                american_ag_largest_loss_claims_view.min_coll_ded               AS min_coll_ded,
                american_ag_largest_loss_claims_view.max_coll_ded               AS max_coll_ded,
                american_ag_largest_loss_claims_view.min_med_limit              AS min_med_limit,
                american_ag_largest_loss_claims_view.max_med_limit              AS max_med_limit,
                american_ag_largest_loss_claims_view.min_bi_limit               AS min_bi_limit,
                american_ag_largest_loss_claims_view.max_bi_limit               AS max_bi_limit,
                american_ag_largest_loss_claims_view.min_pd_limit               AS min_pd_limit,
                american_ag_largest_loss_claims_view.max_pd_limit               AS max_pd_limit,
                american_ag_largest_loss_claims_view.min_sl_limit               AS min_sl_limit,
                american_ag_largest_loss_claims_view.max_sl_limit               AS max_sl_limit,
                american_ag_largest_loss_claims_view.min_lou_limit              AS min_lou_limit,
                american_ag_largest_loss_claims_view.max_lou_limit              AS max_lou_limit,
                american_ag_largest_loss_claims_view.min_umbi_limit             AS min_umbi_limit,
                american_ag_largest_loss_claims_view.max_umbi_limit             AS max_umbi_limit,
                american_ag_largest_loss_claims_view.min_ers_limit              AS min_ers_limit,
                american_ag_largest_loss_claims_view.max_ers_limit              AS max_ers_limit,
                american_ag_largest_loss_claims_view.each_occurrence            AS each_occurrence,
                american_ag_largest_loss_claims_view.damage_to_prem             AS damage_to_prem,
                american_ag_largest_loss_claims_view.gen_agg                    AS gen_agg,
                american_ag_largest_loss_claims_view.prod_comp_ops              AS prod_comp_ops
         FROM   american_ag_largest_loss_claims_view );
  -- Component TGT_GW_Largest_Loss_Claims_View, Type EXPORT_DATA Exporting data
 -- ;
END;
';