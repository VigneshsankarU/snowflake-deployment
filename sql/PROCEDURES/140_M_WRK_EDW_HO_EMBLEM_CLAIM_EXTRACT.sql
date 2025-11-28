-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_WRK_EDW_HO_EMBLEM_CLAIM_EXTRACT("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
  DECLARE
    run_id string;
    prcs_id string;
    v_start_time timestamp;
  BEGIN
    run_id :=
    (
             SELECT   run_id
             FROM     control_worklet
             WHERE    worklet_name = :worklet_name
             ORDER BY insert_ts DESC limit 1);
    prcs_id :=
    (
           SELECT param_value
           FROM   control_params
           WHERE  run_id = :run_id
           AND    param_name = ''PRCS_ID'' limit 1);
    v_start_time := current_timestamp();
    -- Component SQ_HO_EMBLEM_CLM_STG, Type SOURCE
    CREATE
    OR
    replace TEMPORARY TABLE sq_ho_emblem_clm_stg AS
    (
           SELECT
                  /* adding column aliases to ensure proper downstream column references */
                  $1  AS agmt_id,
                  $2  AS host_agmt_num,
                  $3  AS loss_date,
                  $4  AS loss_reported_date,
                  $5  AS accounting_date,
                  $6  AS claim_status,
                  $7  AS reserv_amt,
                  $8  AS bnk_drft_num,
                  $9  AS draft_amt,
                  $10 AS host_ctstrph_ref_num,
                  $11 AS suit,
                  $12 AS nbr_claimants,
                  $13 AS coverage,
                  $14 AS cotter,
                  $15 AS adjuster_name,
                  $16 AS loss_cause,
                  $17 AS account_nbr,
                  $18 AS deductible,
                  $19 AS clm_expsr_trans_sbtype_cd,
                  $20 AS sec_res_ind,
                  $21 AS plcy_mail_state,
                  $22 AS plcy_company,
                  $23 AS member_number,
                  $24 AS member_type,
                  $25 AS agent_nbr,
                  $26 AS svc_nbr,
                  $27 AS cov_limit,
                  $28 AS plcy_lob,
                  $29 AS prty_asset_id,
                  $30 AS protection_class,
                  $31 AS nbr_families,
                  $32 AS construction_type,
                  $33 AS encumbrance_cd,
                  $34 AS zip,
                  $35 AS cnstrctn_dt,
                  $36 AS fire_prot_service,
                  $37 AS plcy_risk_county,
                  $38 AS prev_agent,
                  $39 AS ho_01_ind,
                  $40 AS ho_45_ind,
                  $41 AS mun_tax,
                  $42 AS source_record_id
           FROM   (
                           SELECT   src.*,
                                    row_number() over (ORDER BY 1) AS source_record_id
                           FROM     ( WITH address_hierarchy AS (
    SELECT DISTINCT 
        clm_loctr.clm_id AS claim_id,
        clm_loctr.loc_id AS claim_loc_id,
        street_addr.addr_ln_1_txt AS plcy_mail_address_1,
        street_addr.addr_ln_2_txt AS plcy_mail_address_2,
        city.geogrcl_area_name AS city,
        terr.geogrcl_area_name AS state,
        postl_cd.postl_cd_num AS zip,
        cnty.geogrcl_area_name AS county,
        tax_loc.geogrcl_shrt_name AS mun_tax
    FROM db_t_prod_core.clm_loctr
    JOIN db_t_prod_core.street_addr
        ON clm_loctr.loc_id = street_addr.street_addr_id
        AND street_addr.edw_end_dttm = ''9999-12-31''
        AND clm_loctr.clm_loctr_role_cd = ''LOSSSTADRS''
    LEFT JOIN db_t_prod_core.city
        ON street_addr.city_id = city.city_id
        AND city.edw_end_dttm = ''9999-12-31''
    LEFT JOIN db_t_prod_core.terr
        ON street_addr.terr_id = terr.terr_id
        AND terr.edw_end_dttm = ''9999-12-31''
    LEFT JOIN db_t_prod_core.postl_cd
        ON street_addr.postl_cd_id = postl_cd.postl_cd_id
        AND postl_cd.edw_end_dttm = ''9999-12-31''
    LEFT JOIN db_t_prod_core.cnty
        ON street_addr.cnty_id = cnty.cnty_id
        AND cnty.edw_end_dttm = ''9999-12-31''
    LEFT JOIN db_t_prod_core.ctry
        ON street_addr.ctry_id = ctry.ctry_id
        AND ctry.edw_end_dttm = ''9999-12-31''
    LEFT JOIN db_t_prod_core.tax_loc
        ON street_addr.tax_loc_id = tax_loc.tax_loc_id
        AND tax_loc.edw_end_dttm = ''9999-12-31''
    WHERE clm_loctr.edw_end_dttm = ''9999-12-31''
),

account_policy AS (
    SELECT 
        policyaccount.host_agmt_num AS policyaccount_nbr,
        agmt_rltd.rltd_agmt_id AS policy_id
    FROM db_t_prod_core.agmt AS policyaccount
    JOIN db_t_prod_core.agmt_rltd
        ON policyaccount.agmt_id = agmt_rltd.agmt_id
        AND agmt_rltd.agmt_rltd_rsn_cd = ''ACCTTOPLCY''
        AND agmt_rltd.trans_end_dttm = ''9999-12-31 23:59:59.999999''
    WHERE policyaccount.edw_end_dttm = ''9999-12-31''
),

coverage_value AS (
    SELECT 
        agmt_insrd_asset_feat.agmt_id,
        feat_rltd.feat_id AS cov_id,
        agmt_insrd_asset_feat.prty_asset_id,
        feat.feat_dtl_modl_type_name,
        MAX(
            CASE 
                WHEN feat.feat_dtl_val > agmt_insrd_asset_feat.agmt_asset_feat_amt 
                THEN feat.feat_dtl_val 
                ELSE agmt_insrd_asset_feat.agmt_asset_feat_amt 
            END
        ) AS cov_val
    FROM db_t_prod_core.agmt_insrd_asset_feat
    JOIN db_t_prod_core.feat
        ON feat.feat_id = agmt_insrd_asset_feat.feat_id
    JOIN db_t_prod_core.feat_rltd
        ON feat_rltd.rltd_feat_id = feat.feat_id
        AND feat_rltd.feat_rltnshp_type_cd IN (''COVPKG'', ''COVOPTT'', ''COVT'')
    WHERE 
        agmt_insrd_asset_feat.trans_end_dttm = ''9999-12-31 23:59:59.999999''
        AND feat.edw_end_dttm = ''9999-12-31''
        AND feat_rltd.trans_end_dttm = ''9999-12-31 23:59:59.999999''
    GROUP BY 1,2,3,4
),

asset_detail AS (
    SELECT 
        asset_dtl_cd_xref.asset_dtl_cd,
        asset_dtl_type.asset_dtl_desc,
        asset_dtl_type.asset_dtl_schm_type_cd,
        asset_dtl_cd_xref.prty_asset_id
    FROM db_t_prod_core.asset_dtl_cd_xref
    JOIN db_t_prod_core.asset_dtl_type
        ON asset_dtl_type.asset_dtl_cd = asset_dtl_cd_xref.asset_dtl_cd
    WHERE 
        asset_dtl_cd_xref.edw_end_dttm = ''9999-12-31''
        AND asset_dtl_cd_xref.asset_dtl_cd <> ''UNK''
    GROUP BY 1,2,3,4
),

service_center AS (
    SELECT 
        MAX(org_name) AS org_name,
        pa.agmt_id
    FROM db_t_prod_core.org_name o
    JOIN db_t_prod_core.prty_agmt pa
        ON o.prty_id = pa.prty_id
    WHERE 
        pa.prty_agmt_role_cd = ''SVC''
        AND pa.trans_end_dttm = ''9999-12-31 23:59:59.999999''
        AND o.edw_end_dttm = ''9999-12-31''
    GROUP BY pa.agmt_id
),

agent AS (
    SELECT 
        MAX(io.intrnl_org_num) AS intrnl_org_num,
        pa.agmt_id
    FROM db_t_prod_core.prty_agmt pa
    JOIN db_t_prod_core.intrnl_org io
        ON pa.prty_id = io.intrnl_org_prty_id
    WHERE 
        pa.prty_agmt_role_cd = ''PRDA''
        AND io.intrnl_org_sbtype_cd = ''PRDA''
        AND pa.trans_end_dttm = ''9999-12-31 23:59:59.999999''
        AND io.edw_end_dttm = ''9999-12-31''
    GROUP BY pa.agmt_id
),

membership AS (
    SELECT 
        m.mbrshp_num,
        am.agmt_id,
        m.mbrshp_type_cd
    FROM db_t_prod_core.agmt_mbrshp am
    JOIN db_t_prod_core.mbrshp m
        ON am.mbrshp_id = m.mbrshp_id
    WHERE 
        am.edw_end_dttm = ''9999-12-31''
        AND m.edw_end_dttm = ''9999-12-31''
),

underwriting AS (
    SELECT 
        MAX(o.intrnl_org_num) AS prty_desc,
        pa.agmt_id
    FROM db_t_prod_core.prty_agmt pa
    JOIN db_t_prod_core.intrnl_org o
        ON o.intrnl_org_prty_id = pa.prty_id
    WHERE 
        pa.prty_agmt_role_cd = ''CMP''
        AND pa.trans_end_dttm = ''9999-12-31 23:59:59.999999''
        AND o.edw_end_dttm = ''9999-12-31''
    GROUP BY pa.agmt_id
),

location_address AS (
    SELECT 
        prty_asset_loctr.prty_asset_id,
        prty_asset_loctr.loc_id,
        prty_asset_loctr.prty_asset_loctr_role_cd,
        cnty.geogrcl_area_name AS county,
        street_addr.addr_ln_1_txt AS plcy_mail_address_1,
        prty_asset_loctr.fire_dept_id,
        postl_cd.postl_cd_num AS zip
    FROM db_t_prod_core.prty_asset_loctr
    JOIN db_t_prod_core.street_addr
        ON street_addr.street_addr_id = prty_asset_loctr.loc_id
        AND street_addr.edw_end_dttm = ''9999-12-31''
    LEFT JOIN db_t_prod_core.cnty
        ON cnty.cnty_id = street_addr.cnty_id
        AND cnty.edw_end_dttm = ''9999-12-31''
    LEFT JOIN db_t_prod_core.postl_cd
        ON postl_cd.postl_cd_id = street_addr.postl_cd_id
        AND postl_cd.edw_end_dttm = ''9999-12-31''
    WHERE prty_asset_loctr.trans_end_dttm = ''9999-12-31''
),

risk_state AS (
    SELECT 
        a.agmt_id,
        d.geogrcl_area_shrt_name,
        prty_asset_loctr_role_cd
    FROM db_t_prod_core.agmt a
    JOIN db_t_prod_core.agmt_asset b
        ON b.agmt_id = a.agmt_id
    JOIN db_t_prod_core.prty_asset_loctr c
        ON c.prty_asset_id = b.prty_asset_id
        AND c.prty_asset_loctr_role_cd = ''RSKST''
    JOIN db_t_prod_core.terr d
        ON d.terr_id = c.loc_id
    WHERE 
        a.edw_end_dttm = ''9999-12-31''
        AND b.edw_end_dttm = ''9999-12-31''
        AND c.edw_end_dttm = ''9999-12-31''
        AND d.edw_end_dttm = ''9999-12-31''
),

risk_cnty AS (
    SELECT 
        a.agmt_id,
        e.geogrcl_area_shrt_name
    FROM db_t_prod_core.agmt a
    JOIN db_t_prod_core.agmt_asset b
        ON b.agmt_id = a.agmt_id
    JOIN db_t_prod_core.prty_asset_loctr c
        ON c.prty_asset_id = b.prty_asset_id
        AND c.prty_asset_loctr_role_cd = ''RSKCNTY''
    JOIN db_t_prod_core.cnty e
        ON e.cnty_id = c.loc_id
    WHERE 
        a.edw_end_dttm = ''9999-12-31''
        AND b.edw_end_dttm = ''9999-12-31''
        AND c.edw_end_dttm = ''9999-12-31''
        AND e.edw_end_dttm = ''9999-12-31''
),

comn_feat_name AS (
    SELECT 
        agmt.agmt_id,
        feat.comn_feat_name,
        ''Y'' AS ind
    FROM db_t_prod_core.agmt
    JOIN db_t_prod_core.agmt_feat
        ON agmt.agmt_id = agmt_feat.agmt_id
    JOIN db_t_prod_core.feat
        ON feat.feat_id = agmt_feat.feat_id
    WHERE 
        agmt.edw_end_dttm = ''9999-12-31''
        AND agmt_feat.edw_end_dttm = ''9999-12-31''
        AND feat.edw_end_dttm = ''9999-12-31''
),

-- Precomputation CTEs for correlated subqueries
loss_dates AS (
    SELECT 
        clm_id,
        MAX(CASE WHEN clm_dt_type_cd = ''LOSS'' THEN clm_dttm END) AS loss_date,
        MAX(CASE WHEN clm_dt_type_cd = ''REPORTED'' THEN clm_dttm END) AS loss_reported_date
    FROM db_t_prod_core.clm_dt
    WHERE 
        clm_dt_type_cd IN (''LOSS'', ''REPORTED'')
        AND trans_end_dttm = ''9999-12-31 23:59:59.999999''
    GROUP BY clm_id
),

claim_status AS (
    SELECT 
        clm_id,
        clm_sts_type_cd
    FROM db_t_prod_core.clm_sts
    WHERE trans_end_dttm = ''9999-12-31 23:59:59.999999''
),

claimant_count AS (
    SELECT 
        clm_id,
        COUNT(DISTINCT prty_id) AS nbr_claimants
    FROM db_t_prod_core.prty_clm
    WHERE 
        prty_clm_role_cd = ''CLMNT''
        AND trans_end_dttm = ''9999-12-31 23:59:59.999999''
    GROUP BY clm_id
),

-- FIXED: Use correct column name indiv_prty_id instead of prty_id
adjuster_names AS (
    SELECT 
        indiv_prty_id AS prty_id,  -- Corrected column name with alias
        MAX(indiv_full_name) AS adjuster_name
    FROM db_t_prod_core.indiv_name
    WHERE trans_end_dttm = ''9999-12-31 23:59:59.999999''
    GROUP BY indiv_prty_id
),

loss_cause AS (
    SELECT 
        clm_id,
        MAX(peril_type_cd) AS peril_type_cd
    FROM db_t_prod_core.clm_peril
    WHERE trans_end_dttm = ''9999-12-31 23:59:59.999999''
    GROUP BY clm_id
),

account_policy_agg AS (
    SELECT 
        policy_id,
        MAX(policyaccount_nbr) AS policyaccount_nbr
    FROM account_policy
    GROUP BY policy_id
),

deductible_values AS (
    SELECT 
        agmt_id,
        cov_id,
        prty_asset_id,
        cov_val
    FROM coverage_value
    WHERE feat_dtl_modl_type_name = ''DEDUCTIBLE''
),

limit_values AS (
    SELECT 
        agmt_id,
        cov_id,
        prty_asset_id,
        cov_val
    FROM coverage_value
    WHERE feat_dtl_modl_type_name = ''LIMIT''
),

sec_res_ind AS (
    SELECT 
        prty_asset_id,
        MAX(CASE WHEN asset_dtl_cd = ''SECNDRY'' THEN ''1'' ELSE ''0'' END) AS sec_res_ind
    FROM asset_detail
    WHERE asset_dtl_schm_type_cd = ''DWELUSE''
    GROUP BY prty_asset_id
),

risk_state_agg AS (
    SELECT 
        agmt_id,
        MAX(geogrcl_area_shrt_name) AS plcy_mail_state
    FROM risk_state
    GROUP BY agmt_id
),

membership_agg AS (
    SELECT 
        agmt_id,
        MAX(mbrshp_num) AS member_number,
        MAX(mbrshp_type_cd) AS member_type
    FROM membership
    GROUP BY agmt_id
),

protection_class AS (
    SELECT 
        prty_asset_id,
        MAX(prty_asset_spec_val) AS protection_class
    FROM db_t_prod_core.prty_asset_spec
    WHERE prty_asset_spec_type_cd = ''PROT''
    GROUP BY prty_asset_id
),

nbr_families AS (
    SELECT 
        prty_asset_id,
        MAX(asset_dtl_desc) AS nbr_families
    FROM asset_detail
    WHERE asset_dtl_schm_type_cd = ''RESD''
    GROUP BY prty_asset_id
),

encumbrance_cd AS (
    SELECT 
        red.prty_asset_id,
        MAX(et.encmce_type_desc) AS encumbrance_cd
    FROM db_t_prod_core.real_estat_dtl red
    JOIN db_t_prod_core.encmce_type et
        ON et.encmce_type_cd = red.encmce_type_cd
    WHERE red.edw_end_dttm = ''9999-12-31''
    GROUP BY red.prty_asset_id
),

fire_prot_service AS (
    SELECT 
        prty_asset_id,
        MAX(asset_dtl_cd) AS fire_prot_service
    FROM asset_detail
    WHERE asset_dtl_schm_type_cd = ''DWEL''
    GROUP BY prty_asset_id
),

risk_cnty_agg AS (
    SELECT 
        agmt_id,
        MAX(geogrcl_area_shrt_name) AS plcy_risk_county
    FROM risk_cnty
    GROUP BY agmt_id
),

address_agg AS (
    SELECT 
        claim_id,
        MAX(zip) AS zip,
        MAX(mun_tax) AS mun_tax
    FROM address_hierarchy
    GROUP BY claim_id
),

prev_agent AS (
    SELECT 
        curr.agmt_id,
        prev.intrnl_org_num
    FROM (
        SELECT 
            a.agmt_id,
            host_agmt_num,
            intrnl_org_num,
            modl_eff_dttm,
            ROW_NUMBER() OVER(PARTITION BY a.agmt_id ORDER BY modl_eff_dttm DESC) AS rn
        FROM db_t_prod_core.prty_agmt pa
        JOIN db_t_prod_core.intrnl_org io
            ON pa.prty_id = io.intrnl_org_prty_id
            AND pa.prty_agmt_role_cd = ''PRDA''
            AND io.intrnl_org_sbtype_cd = ''PRDA''
        JOIN db_t_prod_core.agmt a
            ON pa.agmt_id = a.agmt_id
            AND agmt_type_cd = ''PPV''
        WHERE pa.edw_end_dttm = ''9999-12-31''
        AND io.edw_end_dttm = ''9999-12-31''
    ) curr
    JOIN (
        SELECT 
            host_agmt_num,
            intrnl_org_num,
            modl_eff_dttm
        FROM db_t_prod_core.prty_agmt pa
        JOIN db_t_prod_core.intrnl_org io
            ON pa.prty_id = io.intrnl_org_prty_id
            AND pa.prty_agmt_role_cd = ''PRDA''
            AND io.intrnl_org_sbtype_cd = ''PRDA''
        JOIN db_t_prod_core.agmt a
            ON pa.agmt_id = a.agmt_id
            AND agmt_type_cd = ''PPV''
        WHERE pa.edw_end_dttm = ''9999-12-31''
        AND io.edw_end_dttm = ''9999-12-31''
    ) prev
        ON curr.host_agmt_num = prev.host_agmt_num
        AND curr.modl_eff_dttm > prev.modl_eff_dttm
        AND curr.intrnl_org_num <> prev.intrnl_org_num
    WHERE curr.rn = 1
)

-- Main Query
SELECT DISTINCT
    policyperiod.agmt_id,
    policyperiod.host_agmt_num,
    ld.loss_date,
    ld.loss_reported_date,
    TO_DATE(clm_expsr_trans.clm_expsr_trans_dttm) AS accounting_date,
    cs.clm_sts_type_cd AS claim_status,
    -- Reserv_amt calculation
    CASE
        WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd = ''RESRV''
            AND clm_expsr_trans.rcvry_ctgy_type_cd IS NULL
            AND clm_expsr_trans.expsr_cost_type_cd = ''PDL''
            AND clm_expsr_trans.expsr_cost_ctgy_type_cd = ''Loss''
            AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''Loss''
        THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt
        ELSE 0
    END -
    CASE
        WHEN clm_expsr_trans.clm_expsr_trans_sbtype_cd = ''PYMNT''
            AND clm_expsr_trans.rcvry_ctgy_type_cd IS NULL
            AND clm_expsr_trans.expsr_cost_type_cd = ''PDL''
            AND clm_expsr_trans.expsr_cost_ctgy_type_cd = ''Loss''
            AND clm_expsr_trans_lnitm.lnitm_ctgy_type_cd = ''Loss''
            AND clm_expsr_trans.does_not_erode_rserv_ind = ''0''
        THEN clm_expsr_trans_lnitm.clm_expsr_lnitm_amt
        ELSE 0
    END AS reserv_amt,
    bnk_drft.bnk_drft_num,
    bnk_drft.bnk_drft_amt AS draft_amt,
    cat_code.host_ctstrph_ref_num,
    CASE WHEN clm_lawsuit.clm_id IS NOT NULL THEN ''Y'' ELSE ''N'' END AS suit,
    cc.nbr_claimants,
    feat.feat_name AS coverage,
    CASE WHEN clm_expsr.cotter_clm_ind = ''1'' THEN ''Y'' ELSE ''N'' END AS cotter,
    an.adjuster_name,
    lc.peril_type_cd AS loss_cause,
    apa.policyaccount_nbr AS account_nbr,
    dv.cov_val AS deductible,
    clm_expsr_trans.clm_expsr_trans_sbtype_cd,
    sri.sec_res_ind,
    rsa.plcy_mail_state,
    uw.prty_desc AS plcy_company,
    ma.member_number,
    ma.member_type,
    ag.intrnl_org_num AS agent_nbr,
    sc.org_name AS svc_nbr,
    lv.cov_val AS cov_limit,
    prod.prod_name AS plcy_lob,
    insrbl_int.prty_asset_id,
    pc.protection_class,
    nf.nbr_families,
    ct.cnstrctn_type_desc AS construction_type,
    ec.encumbrance_cd,
    aa.zip,
    real_estat.cnstrctn_dt,
    fps.fire_prot_service,
    rca.plcy_risk_county,
    pa.intrnl_org_num AS prev_agent,
    COALESCE(ho_01.ind, ''N'') AS ho_01_ind,
    COALESCE(ho_45.ind, ''N'') AS ho_45_ind,
    aa.mun_tax
FROM db_t_prod_core.clm_expsr_trans_lnitm
JOIN db_t_prod_core.clm_expsr_trans
    ON clm_expsr_trans_lnitm.clm_expsr_trans_id = clm_expsr_trans.clm_expsr_trans_id
    AND clm_expsr_trans_lnitm.edw_end_dttm = ''9999-12-31''
    AND clm_expsr_trans.edw_end_dttm = ''9999-12-31''
JOIN db_t_prod_core.clm_expsr
    ON clm_expsr.clm_expsr_id = clm_expsr_trans.clm_expsr_id
    AND clm_expsr.edw_end_dttm = ''9999-12-31''
JOIN db_t_prod_core.clm
    ON clm_expsr.clm_id = clm.clm_id
    AND clm.edw_end_dttm = ''9999-12-31''
LEFT JOIN loss_dates ld
    ON ld.clm_id = clm.clm_id
LEFT JOIN claim_status cs
    ON cs.clm_id = clm.clm_id
LEFT JOIN claimant_count cc
    ON cc.clm_id = clm.clm_id
LEFT JOIN loss_cause lc
    ON lc.clm_id = clm.clm_id
LEFT JOIN db_t_prod_core.agmt_clm
    ON clm.clm_id = agmt_clm.clm_id
    AND agmt_clm.agmt_clm_rltnshp_type_cd = ''PLCYCLM''
    AND agmt_clm.edw_end_dttm = ''9999-12-31''
LEFT JOIN db_t_prod_core.agmt AS policyperiod
    ON agmt_clm.agmt_id = policyperiod.agmt_id
    AND policyperiod.edw_end_dttm = ''9999-12-31''
LEFT JOIN db_t_prod_core.agmt AS policy
    ON policy.host_agmt_num = policyperiod.host_agmt_num
    AND policy.edw_end_dttm = ''9999-12-31''
    AND policy.agmt_type_cd = ''POL''
LEFT JOIN db_t_prod_core.agmt AS risk_agmt
    ON agmt_clm.agmt_id = risk_agmt.agmt_id
    AND risk_agmt.edw_end_dttm = ''9999-12-31''
LEFT JOIN db_t_prod_core.clm_expsr_trans_sts
    ON clm_expsr_trans_sts.clm_expsr_trans_id = clm_expsr_trans.clm_expsr_trans_id
    AND clm_expsr_trans_sts.trans_end_dttm = ''9999-12-31 23:59:59.999999''
LEFT JOIN db_t_prod_core.clm_expsr_trans_ev
    ON clm_expsr_trans_ev.clm_expsr_trans_id = clm_expsr_trans.clm_expsr_trans_id
    AND clm_expsr_trans_ev.trans_end_dttm = ''9999-12-31 23:59:59.999999''
LEFT JOIN db_t_prod_core.pmt_bnk_drft_xref
    ON pmt_bnk_drft_xref.pmt_ev_id = clm_expsr_trans_ev.ev_id
    AND pmt_bnk_drft_xref.edw_end_dttm = ''9999-12-31''
LEFT JOIN db_t_prod_core.bnk_drft
    ON bnk_drft.bnk_drft_doc_id = pmt_bnk_drft_xref.bnk_drft_doc_id
    AND bnk_drft.edw_end_dttm = ''9999-12-31''
LEFT JOIN (
    SELECT 
        clm_ev.clm_id,
        incdt.host_ctstrph_ref_num
    FROM db_t_prod_core.clm_ev
    JOIN db_t_prod_core.incdt
        ON incdt.incdt_ev_id = clm_ev.ev_id
) AS cat_code
    ON cat_code.clm_id = clm.clm_id
LEFT JOIN (
    SELECT DISTINCT clm_legl_actn.clm_id
    FROM db_t_prod_core.legl_actn
    JOIN db_t_prod_core.clm_legl_actn
        ON clm_legl_actn.legl_actn_id = legl_actn.legl_actn_id
    WHERE legl_actn.legl_actn_type_cd = ''SUIT''
        AND subrgtn_rltd_ind <> 1
) AS clm_lawsuit
    ON clm_lawsuit.clm_id = clm.clm_id
LEFT JOIN db_t_prod_core.prty_clm AS adjuster
    ON adjuster.clm_id = clm.clm_id
    AND adjuster.trans_end_dttm = ''9999-12-31 23:59:59.999999''
    AND adjuster.prty_clm_role_cd = ''ADJSTR''
-- FIXED JOIN: Now uses correct prty_id relationship
LEFT JOIN adjuster_names an
    ON an.prty_id = adjuster.prty_id
LEFT JOIN db_t_prod_core.feat
    ON feat.feat_id = clm_expsr.cvge_feat_id
    AND feat.edw_end_dttm = ''9999-12-31''
LEFT JOIN db_t_prod_core.insrbl_int
    ON insrbl_int.insrbl_int_id = clm_expsr.insrbl_int_id
    AND insrbl_int_ctgy_cd = ''ASSET''
    AND insrbl_int.edw_end_dttm = ''9999-12-31''
LEFT JOIN db_t_prod_core.real_estat
    ON insrbl_int.prty_asset_id = real_estat.prty_asset_id
    AND real_estat.edw_end_dttm = ''9999-12-31''
LEFT JOIN db_t_prod_core.prty_agmt priinsured_prty
    ON policyperiod.agmt_id = priinsured_prty.agmt_id
    AND priinsured_prty.prty_agmt_role_cd = ''PLCYPRININS''
    AND priinsured_prty.edw_end_dttm = ''9999-12-31''
LEFT JOIN db_t_prod_core.agmt_prod
    ON policyperiod.agmt_id = agmt_prod.agmt_id
    AND agmt_prod.edw_end_dttm = ''9999-12-31''
LEFT JOIN db_t_prod_core.prod
    ON agmt_prod.prod_id = prod.prod_id
    AND prod.edw_end_dttm = ''9999-12-31''
LEFT JOIN account_policy_agg apa
    ON apa.policy_id = policy.agmt_id
LEFT JOIN deductible_values dv
    ON dv.agmt_id = policyperiod.agmt_id
    AND dv.cov_id = feat.feat_id
    AND dv.prty_asset_id = insrbl_int.prty_asset_id
LEFT JOIN sec_res_ind sri
    ON sri.prty_asset_id = insrbl_int.prty_asset_id
LEFT JOIN risk_state_agg rsa
    ON rsa.agmt_id = risk_agmt.agmt_id
LEFT JOIN underwriting uw
    ON uw.agmt_id = policyperiod.agmt_id
LEFT JOIN membership_agg ma
    ON ma.agmt_id = policyperiod.agmt_id
LEFT JOIN agent ag
    ON ag.agmt_id = policyperiod.agmt_id
LEFT JOIN service_center sc
    ON sc.agmt_id = policyperiod.agmt_id
LEFT JOIN limit_values lv
    ON lv.agmt_id = policyperiod.agmt_id
    AND lv.cov_id = feat.feat_id
    AND lv.prty_asset_id = insrbl_int.prty_asset_id
LEFT JOIN protection_class pc
    ON pc.prty_asset_id = insrbl_int.prty_asset_id
LEFT JOIN nbr_families nf
    ON nf.prty_asset_id = insrbl_int.prty_asset_id
LEFT JOIN db_t_prod_core.cnstrctn_type ct
    ON ct.cnstrctn_type_cd = real_estat.cnstrctn_type_cd
LEFT JOIN encumbrance_cd ec
    ON ec.prty_asset_id = insrbl_int.prty_asset_id
LEFT JOIN address_agg aa
    ON aa.claim_id = clm.clm_id
LEFT JOIN fire_prot_service fps
    ON fps.prty_asset_id = insrbl_int.prty_asset_id
LEFT JOIN risk_cnty_agg rca
    ON rca.agmt_id = risk_agmt.agmt_id
LEFT JOIN prev_agent pa
    ON pa.agmt_id = policyperiod.agmt_id
LEFT JOIN comn_feat_name ho_01
    ON ho_01.agmt_id = policyperiod.agmt_id
    AND ho_01.comn_feat_name = ''DWELLING REPLACEMENT COST ENDORSEMENT''
LEFT JOIN comn_feat_name ho_45
    ON ho_45.agmt_id = policyperiod.agmt_id
    AND ho_45.comn_feat_name = ''PERSONAL PROPERTY REPLACEMENT COST ENDORSEMENT''
WHERE prod.insrnc_lob_type_cd IN (''HO'', ''MH'', ''SF'') ) src ) );
-- Component LKP_ASSET_DTL_CD_XREF, Type LOOKUP
CREATE
OR
replace TEMPORARY TABLE lkp_asset_dtl_cd_xref AS
(
          SELECT    lkp.prty_asset_id,
                    lkp.asset_dtl_txt,
                    sq_ho_emblem_clm_stg.source_record_id,
                    row_number() over(PARTITION BY sq_ho_emblem_clm_stg.source_record_id ORDER BY lkp.prty_asset_id ASC,lkp.asset_dtl_txt ASC) rnk
          FROM      sq_ho_emblem_clm_stg
          left join
                    (
                           SELECT asset_dtl_cd_xref.asset_dtl_txt AS asset_dtl_txt,
                                  asset_dtl_cd_xref.prty_asset_id AS prty_asset_id
                           FROM   db_t_prod_core.asset_dtl_cd_xref
                           WHERE  asset_dtl_cd_xref.asset_dtl_cd IN (''TC'',
                                                                     ''HOTC'')
                           AND    edw_end_dttm =''9999-12-31 23:59:59.999999'' ) lkp
          ON        lkp.prty_asset_id = sq_ho_emblem_clm_stg.prty_asset_id qualify rnk = 1 );
-- Component exp_data_trans, Type EXPRESSION
CREATE
OR
replace TEMPORARY TABLE exp_data_trans AS
(
           SELECT
                      CASE
                                 WHEN date_part(''MM'', to_timestamp(current_timestamp)) > 9 THEN concat ( ( date_part(''YYYY'', to_timestamp(current_timestamp)) ) , ( date_part(''MM'', to_timestamp(current_timestamp)) ) )
                                 ELSE date_part(''YYYY'', to_timestamp(current_timestamp))
                                                       || ''0''
                                                       || date_part(''MM'', to_timestamp(current_timestamp))
                      END                                AS mo_id,
                      sq_ho_emblem_clm_stg.agmt_id       AS agmt_id,
                      sq_ho_emblem_clm_stg.host_agmt_num AS host_agmt_num,
                      sq_ho_emblem_clm_stg.loss_date     AS loss_date,
                      CASE
                                 WHEN date_part(''MM'', to_timestamp(sq_ho_emblem_clm_stg.loss_date)) > 9 THEN concat ( ( date_part(''YYYY'', to_timestamp(sq_ho_emblem_clm_stg.loss_date)) ) , ( date_part(''MM'', to_timestamp(sq_ho_emblem_clm_stg.loss_date)) ) )
                                 ELSE date_part(''YYYY'', to_timestamp(sq_ho_emblem_clm_stg.loss_date))
                                                       || ''0''
                                                       || date_part(''MM'', to_timestamp(sq_ho_emblem_clm_stg.loss_date))
                      END AS loss_dt,
                      CASE
                                 WHEN date_part(''MM'', to_timestamp(sq_ho_emblem_clm_stg.loss_reported_date)) > 9 THEN concat ( ( date_part(''YYYY'', to_timestamp(sq_ho_emblem_clm_stg.loss_reported_date)) ) , ( date_part(''MM'', to_timestamp(sq_ho_emblem_clm_stg.loss_reported_date)) ) )
                                 ELSE date_part(''YYYY'', to_timestamp(sq_ho_emblem_clm_stg.loss_reported_date))
                                                       || ''0''
                                                       || date_part(''MM'', to_timestamp(sq_ho_emblem_clm_stg.loss_reported_date))
                      END                                  AS o_loss_reported_date,
                      sq_ho_emblem_clm_stg.accounting_date AS accounting_date,
                      CASE
                                 WHEN sq_ho_emblem_clm_stg.claim_status = ''CLOSED''
                                 AND        sq_ho_emblem_clm_stg.clm_expsr_trans_sbtype_cd = ''PYMNT'' THEN 2
                                 ELSE
                                            CASE
                                                       WHEN sq_ho_emblem_clm_stg.claim_status = ''CLOSED''
                                                       AND        sq_ho_emblem_clm_stg.clm_expsr_trans_sbtype_cd <> ''PYMNT'' THEN 1
                                                       ELSE
                                                                  CASE
                                                                             WHEN sq_ho_emblem_clm_stg.claim_status <> ''CLOSED'' THEN NULL
                                                                             ELSE 3 --$3
                                                                  END
                                            END
                      END                                       AS close_cd,
                      sq_ho_emblem_clm_stg.reserv_amt           AS reserv_amt,
                      sq_ho_emblem_clm_stg.bnk_drft_num         AS bnk_drft_num,
                      sq_ho_emblem_clm_stg.draft_amt            AS draft_amt,
                      sq_ho_emblem_clm_stg.host_ctstrph_ref_num AS host_ctstrph_ref_num,
                      sq_ho_emblem_clm_stg.suit                 AS suit,
                      decode ( sq_ho_emblem_clm_stg.suit ,
                              ''Y'' , ''1'' ,
                              ''N'' , ''0'' )                AS out_suit,
                      sq_ho_emblem_clm_stg.nbr_claimants AS nbr_claimants,
                      sq_ho_emblem_clm_stg.coverage      AS coverage,
                      decode ( sq_ho_emblem_clm_stg.cotter ,
                              ''Y'' , 1 ,
                              ''N'' , 0 )                  AS out_cotter,
                      sq_ho_emblem_clm_stg.adjuster_name AS adjuster_name,
                      sq_ho_emblem_clm_stg.loss_cause    AS loss_cause,
                      sq_ho_emblem_clm_stg.account_nbr   AS account_nbr,
                      sq_ho_emblem_clm_stg.deductible    AS deductible,
                      CASE
                                 WHEN sq_ho_emblem_clm_stg.clm_expsr_trans_sbtype_cd = ''RESRV'' THEN ''1''
                                 ELSE
                                            CASE
                                                       WHEN sq_ho_emblem_clm_stg.clm_expsr_trans_sbtype_cd = ''PYMNT'' THEN ''2''
                                                       ELSE ''0''
                                            END
                      END AS amt_type,
                      CASE
                                 WHEN sq_ho_emblem_clm_stg.sec_res_ind = ''1'' THEN ''Y''
                                 ELSE ''N''
                      END                                  AS sec_res_ind_out,
                      sq_ho_emblem_clm_stg.plcy_mail_state AS plcy_mail_state,
                      sq_ho_emblem_clm_stg.plcy_company    AS plcy_company,
                      sq_ho_emblem_clm_stg.member_number   AS member_number,
                      sq_ho_emblem_clm_stg.member_type     AS member_type,
                      sq_ho_emblem_clm_stg.agent_nbr       AS agent_nbr,
                      sq_ho_emblem_clm_stg.svc_nbr         AS svc_nbr,
                      sq_ho_emblem_clm_stg.cov_limit       AS cov_limit,
                      CASE
                                 WHEN upper ( sq_ho_emblem_clm_stg.coverage ) = ''DWELLING'' THEN sq_ho_emblem_clm_stg.cov_limit
                                 ELSE NULL
                      END AS v_dwelling_limit,
                      decode ( TRUE ,
                              0 <= v_dwelling_limit
                   AND        v_dwelling_limit < 150000 , ''O'' ,
                              150001 <= v_dwelling_limit
                   AND        v_dwelling_limit <= 250000 , ''E'' ,
                              250001 <= v_dwelling_limit
                   AND        v_dwelling_limit <= 300000 , ''F'' ,
                              300001 <= v_dwelling_limit
                   AND        v_dwelling_limit <= 350000 , ''G'' ,
                              350001 <= v_dwelling_limit
                   AND        v_dwelling_limit <= 500000 , ''H'' ,
                              500001 <= v_dwelling_limit
                   AND        v_dwelling_limit <= 750000 , ''I'' ,
                              750001 <= v_dwelling_limit
                   AND        v_dwelling_limit <= 1000000 , ''J'' ,
                              1000001 <= v_dwelling_limit
                   AND        v_dwelling_limit <= 1250000 , ''K'' ,
                              1250001 <= v_dwelling_limit
                   AND        v_dwelling_limit <= 1500000 , ''L'' ,
                              1500001 <= v_dwelling_limit
                   AND        v_dwelling_limit <= 1750000 , ''M'' ,
                              1750001 <= v_dwelling_limit
                   AND        v_dwelling_limit <= 2000000 , ''N'' ,
                              2000001 <= v_dwelling_limit
                   AND        v_dwelling_limit <= 2500000 , ''P'' ,
                              2500001 <= v_dwelling_limit
                   AND        v_dwelling_limit <= 3000000 , ''Q'' ,
                              3000001 <= v_dwelling_limit
                   AND        v_dwelling_limit <= 4000000 , ''R'' ,
                              4000001 <= v_dwelling_limit
                   AND        v_dwelling_limit <= 5000000 , ''S'' ,
                              5000001 <= v_dwelling_limit
                   AND        v_dwelling_limit <= 6000000 , ''T'' ,
                              ''Z'' )                 AS reins_cd,
                      sq_ho_emblem_clm_stg.plcy_lob AS plcy_lob,
                      CASE
                                 WHEN replace(regexp_replace(sq_ho_emblem_clm_stg.plcy_lob,''[^0-9]'',''/''),''/'',chr ( 0 )) = '''' THEN NULL
                                 ELSE replace(regexp_replace(sq_ho_emblem_clm_stg.plcy_lob,''[^0-9]'',''/''),''/'',chr ( 0 ))
                      END                                   AS FORM,
                      sq_ho_emblem_clm_stg.nbr_families     AS nbr_families,
                      sq_ho_emblem_clm_stg.protection_class AS protection_class,
                      sq_ho_emblem_clm_stg.encumbrance_cd   AS encumbrance_cd,
                      CASE
                                 WHEN upper ( sq_ho_emblem_clm_stg.coverage ) = ''DWELLING'' THEN sq_ho_emblem_clm_stg.cov_limit
                                 ELSE 0
                      END                                                               AS dwl_amt_ins,
                      date_part(''YYYY'', to_timestamp(sq_ho_emblem_clm_stg.cnstrctn_dt)) AS cnstr_yr,
                      sq_ho_emblem_clm_stg.fire_prot_service                            AS fire_prot_service,
                      CASE
                                 WHEN upper ( sq_ho_emblem_clm_stg.coverage ) = ''PERSONAL PROPERTY'' THEN sq_ho_emblem_clm_stg.cov_limit
                                 ELSE 0
                      END                                          AS pers_prop_amt_ins,
                      sq_ho_emblem_clm_stg.plcy_risk_county        AS plcy_risk_county,
                      sq_ho_emblem_clm_stg.construction_type       AS construction_type,
                      ltrim ( rtrim ( sq_ho_emblem_clm_stg.zip ) ) AS zip_out,
                      :PRCS_ID                                     AS prcs_id,
                      current_timestamp                            AS load_dt,
                      sq_ho_emblem_clm_stg.prev_agent              AS prev_agent,
                      CASE
                                 WHEN sq_ho_emblem_clm_stg.ho_01_ind = ''Y'' THEN ''Y''
                                 ELSE ''N''
                      END AS ho_01_ind_out,
                      CASE
                                 WHEN sq_ho_emblem_clm_stg.ho_45_ind = ''Y'' THEN ''Y''
                                 ELSE ''N''
                      END                                            AS ho_45_ind_out,
                      sq_ho_emblem_clm_stg.mun_tax                   AS mun_tax,
                      sq_ho_emblem_clm_stg.prty_asset_id             AS prty_asset_id,
                      to_char ( sq_ho_emblem_clm_stg.prty_asset_id ) AS v_prty_asset_id,
                      CASE
                                 WHEN lkp_asset_dtl_cd_xref.asset_dtl_txt IS NULL THEN v_prty_asset_id
                                 ELSE lkp_asset_dtl_cd_xref.asset_dtl_txt
                      END             AS v_asset_dtl_txt,
                      v_asset_dtl_txt AS out_asset_dtl_txt,
                      sq_ho_emblem_clm_stg.source_record_id
           FROM       sq_ho_emblem_clm_stg
           inner join lkp_asset_dtl_cd_xref
           ON         sq_ho_emblem_clm_stg.source_record_id = lkp_asset_dtl_cd_xref.source_record_id );
-- Component EDW_HO_EMBLEM_CLAIM, Type TARGET
INSERT INTO db_t_prod_wrk.edw_ho_emblem_claim
            (
                        mo_id,
                        dw_plcy_skey,
                        plcy_nbr,
                        state_cd,
                        cmpy_cd,
                        cust_memb_num,
                        cust_memb_type,
                        loss_dt,
                        date_of_loss,
                        rpt_dt,
                        acct_dt,
                        agnt_nbr,
                        svc_nbr,
                        close_cd,
                        res_amt,
                        drft_nbr,
                        drft_amt,
                        reins_cd,
                        cat_cd,
                        FORM,
                        nbr_families,
                        prot_class,
                        ho_zone,
                        constr_desc,
                        enc_cd,
                        dwl_amt_ins,
                        suit,
                        nbr_claimants,
                        coverage,
                        cotter,
                        adjuster,
                        cause_cd,
                        acct_nbr,
                        mun_tax,
                        zip_cd,
                        deduct,
                        cnstr_yr,
                        amt_type,
                        sec_res_cd,
                        fire_prot_service,
                        ho_45_ind,
                        ho_01_ind,
                        prev_agent,
                        pers_prop_amt_ins,
                        loc_county,
                        prcs_id,
                        load_dt
            )
SELECT exp_data_trans.mo_id                AS mo_id,
       exp_data_trans.agmt_id              AS dw_plcy_skey,
       exp_data_trans.host_agmt_num        AS plcy_nbr,
       exp_data_trans.plcy_mail_state      AS state_cd,
       exp_data_trans.plcy_company         AS cmpy_cd,
       exp_data_trans.member_number        AS cust_memb_num,
       exp_data_trans.member_type          AS cust_memb_type,
       exp_data_trans.loss_dt              AS loss_dt,
       exp_data_trans.loss_date            AS date_of_loss,
       exp_data_trans.o_loss_reported_date AS rpt_dt,
       exp_data_trans.accounting_date      AS acct_dt,
       exp_data_trans.agent_nbr            AS agnt_nbr,
       exp_data_trans.svc_nbr              AS svc_nbr,
       exp_data_trans.close_cd             AS close_cd,
       exp_data_trans.reserv_amt           AS res_amt,
       exp_data_trans.bnk_drft_num         AS drft_nbr,
       exp_data_trans.draft_amt            AS drft_amt,
       exp_data_trans.reins_cd             AS reins_cd,
       exp_data_trans.host_ctstrph_ref_num AS cat_cd,
       exp_data_trans.FORM                 AS FORM,
       exp_data_trans.nbr_families         AS nbr_families,
       exp_data_trans.protection_class     AS prot_class,
       exp_data_trans.out_asset_dtl_txt    AS ho_zone,
       exp_data_trans.construction_type    AS constr_desc,
       exp_data_trans.encumbrance_cd       AS enc_cd,
       exp_data_trans.dwl_amt_ins          AS dwl_amt_ins,
       exp_data_trans.out_suit             AS suit,
       exp_data_trans.nbr_claimants        AS nbr_claimants,
       exp_data_trans.coverage             AS coverage,
       exp_data_trans.out_cotter           AS cotter,
       exp_data_trans.adjuster_name        AS adjuster,
       exp_data_trans.loss_cause           AS cause_cd,
       exp_data_trans.account_nbr          AS acct_nbr,
       exp_data_trans.mun_tax              AS mun_tax,
       exp_data_trans.zip_out              AS zip_cd,
       exp_data_trans.deductible           AS deduct,
       exp_data_trans.cnstr_yr             AS cnstr_yr,
       exp_data_trans.amt_type             AS amt_type,
       exp_data_trans.sec_res_ind_out      AS sec_res_cd,
       exp_data_trans.fire_prot_service    AS fire_prot_service,
       exp_data_trans.ho_45_ind_out        AS ho_45_ind,
       exp_data_trans.ho_01_ind_out        AS ho_01_ind,
       exp_data_trans.prev_agent           AS prev_agent,
       exp_data_trans.pers_prop_amt_ins    AS pers_prop_amt_ins,
       exp_data_trans.plcy_risk_county     AS loc_county,
       exp_data_trans.prcs_id              AS prcs_id,
       exp_data_trans.load_dt              AS load_dt
FROM   exp_data_trans;

END;
';