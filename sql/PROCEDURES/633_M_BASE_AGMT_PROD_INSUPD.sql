-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_PROD_INSUPD("WORKLET_NAME" VARCHAR)
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
  ho_innovn STRING;
  prcs_id STRING;
  p_agmt_type_cd_policy_version STRING;
BEGIN
    run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
    workflow_name := (SELECT workflow_name FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
    session_name := ''s_m_base_agmt_prod_insupd'';
    start_dttm := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);
    end_dttm := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
    ho_innovn := public.func_get_scoped_param(:run_id, ''ho_innovn'', :workflow_name, :worklet_name, :session_name);
    prcs_id := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);
    p_agmt_type_cd_policy_version := public.func_get_scoped_param(:run_id, ''p_agmt_type_cd_policy_version'', :workflow_name, :worklet_name, :session_name);
	v_start_time := CURRENT_TIMESTAMP();


  -- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm IN (''AGMT_PROD_ROLE_TYPE'' ,
                                                         ''PROD_SBTYPE'')
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_SRC_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_src_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component SQ_pc_policyline, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_policyline AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS nk_publicid,
                $2 AS customautosymboldesc,
                $3 AS src_cd,
                $4 AS createtime,
                $5 AS updatetime,
                $6 AS agmt_prod_role_cd,
                $7 AS prod_sbtype_cd,
                $8 AS agmt_prod_cvge_lvl_desc,
                $9 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT *
                                         FROM   (
                                                                SELECT DISTINCT cast (a.nk_publicid AS VARCHAR (64))                                                                                                                                                                              AS nk_publicid,
                                                                                ltrim(rtrim(coalesce(pctl_hopolicytype_hoe.typecode_stg, pctl_papolicytype_alfa.typecode_stg, pctl_puppolicytype.typecode_stg, pctl_bp7policytype_alfa.typecode_stg, prod_name,pctl_foppolicytype.typecode_stg))) AS productname,
                                                                                ''SRC_SYS4''                                                                                                                                                                                                        AS src_cd,
                                                                                a.policyperiod_editeffectivedt,
                                                                                policyperiod_updatetime,
                                                                                cast (''AGMT_PROD_ROLE_TYPE1'' AS VARCHAR (60)) AS agmt_prod_role_cd,
                                                                                cast (''POLICY TYPE'' AS          VARCHAR (60)) AS prod_sbtype_cd,
                                                                                ltrim(rtrim(a.bp7whatisinsured_alfa))         AS agmt_prod_cvge_lvl_desc
                                                                FROM
                                                                                /* DB_T_PROD_STAG.pc_PolicyLine  */
                                                                                (
                                                                                                SELECT          pc_policyperiod.publicid_stg AS nk_publicid ,
                                                                                                                pc_policyperiod.status_stg,
                                                                                                                pc_policyperiod.editeffectivedate_stg AS policyperiod_editeffectivedt,
                                                                                                                pc_policyperiod.updatetime_stg        AS policyperiod_updatetime,
                                                                                                                /* pctl_bp7whatisinsured_alfa.NAME_stg AS BP7WhatIsInsured_alfa, */
                                                                                                                CASE
                                                                                                                                WHEN pc_policyline.ishoinnovationpol_alfa_stg = 1 THEN :ho_innovn
                                                                                                                                ELSE pctl_bp7whatisinsured_alfa.name_stg
                                                                                                                END AS bp7whatisinsured_alfa,
                                                                                                                pc_policyline.puppolicytype_stg,
                                                                                                                /* Added for DB_T_STAG_MEMBXREF_PROD.Umbrella */
                                                                                                                pc_policyline.papolicytype_alfa_stg,
                                                                                                                pc_policyline.hopolicytype_stg,
                                                                                                                pc_policyline.bp7policytype_alfa_stg,
                                                                                                                pc_policyline.foppolicytype_stg,
                                                                                                                CASE
                                                                                                                                                /*Added as part of Farm*/
                                                                                                                                WHEN (
                                                                                                                                                                pc_policyline.machinerycoverableexists_stg = 1
                                                                                                                                                OR              pc_policyline.livestockcoverableexists_stg = 1)
                                                                                                                                AND             (
                                                                                                                                                                pc_policyline.blanketcoverableexists_stg =0
                                                                                                                                                OR              pc_policyline.blanketcoverableexists_stg IS NULL)
                                                                                                                                AND             (
                                                                                                                                                                pc_policyline.dwellingcoverableexists_stg =0
                                                                                                                                                OR              pc_policyline.dwellingcoverableexists_stg IS NULL)
                                                                                                                                AND             (
                                                                                                                                                                pc_policyline.feedandseedcoverableexists_stg =0
                                                                                                                                                OR              pc_policyline.feedandseedcoverableexists_stg IS NULL)
                                                                                                                                AND             (
                                                                                                                                                                pc_policyline.liabilitycoverableexists_stg =0
                                                                                                                                                OR              pc_policyline.liabilitycoverableexists_stg IS NULL)
                                                                                                                                AND             (
                                                                                                                                                                pc_policyline.outbuildingcoverableexists_stg =0
                                                                                                                                                OR              pc_policyline.outbuildingcoverableexists_stg IS NULL) THEN ''Machinery''
                                                                                                                                WHEN (
                                                                                                                                                                pc_policyline.liabilitycoverableexists_stg = 1)
                                                                                                                                AND             (
                                                                                                                                                                pc_policyline.blanketcoverableexists_stg =0
                                                                                                                                                OR              pc_policyline.blanketcoverableexists_stg IS NULL)
                                                                                                                                AND             (
                                                                                                                                                                pc_policyline.dwellingcoverableexists_stg =0
                                                                                                                                                OR              pc_policyline.dwellingcoverableexists_stg IS NULL)
                                                                                                                                AND             (
                                                                                                                                                                pc_policyline.feedandseedcoverableexists_stg =0
                                                                                                                                                OR              pc_policyline.feedandseedcoverableexists_stg IS NULL)
                                                                                                                                AND             (
                                                                                                                                                                pc_policyline.machinerycoverableexists_stg =0
                                                                                                                                                OR              pc_policyline.machinerycoverableexists_stg IS NULL)
                                                                                                                                AND             (
                                                                                                                                                                pc_policyline.livestockcoverableexists_stg =0
                                                                                                                                                OR              pc_policyline.livestockcoverableexists_stg IS NULL)
                                                                                                                                AND             (
                                                                                                                                                                pc_policyline.outbuildingcoverableexists_stg =0
                                                                                                                                                OR              pc_policyline.outbuildingcoverableexists_stg IS NULL) THEN ''FCL''
                                                                                                                END prod_name,
                                                                                                                /*Added as part of Farm*/
                                                                                                                pc_policyline.expirationdate_stg
                                                                                                FROM            db_t_prod_stag.pc_policyline
                                                                                                left outer join db_t_prod_stag.pc_policyperiod
                                                                                                ON              pc_policyline.branchid_stg=pc_policyperiod.id_stg
                                                                                                left join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                ON              pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                left join       db_t_prod_stag.pctl_papolicytype_alfa
                                                                                                ON              pc_policyline.papolicytype_alfa_stg = pctl_papolicytype_alfa.id_stg
                                                                                                left join       db_t_prod_stag.pctl_bp7whatisinsured_alfa
                                                                                                ON              pc_policyline.bp7whatisinsured_alfa_stg = pctl_bp7whatisinsured_alfa.id_stg
                                                                                                left join       db_t_prod_stag.pctl_bp7propertytype
                                                                                                ON              pc_policyline.bp7linebusinesstype_stg = pctl_bp7propertytype.id_stg
                                                                                                left join       db_t_prod_stag.pctl_number_alfa
                                                                                                ON              pctl_number_alfa.id_stg = pc_policyline.latepaycount_alfa_stg
                                                                                                WHERE           pc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                                AND             pc_policyperiod.updatetime_stg <= (:end_dttm) ) a
                                                                left outer join db_t_prod_stag.pctl_papolicytype_alfa
                                                                ON              a.papolicytype_alfa_stg=pctl_papolicytype_alfa.id_stg
                                                                left outer join db_t_prod_stag.pctl_hopolicytype_hoe
                                                                ON              a.hopolicytype_stg=pctl_hopolicytype_hoe.id_stg
                                                                left outer join db_t_prod_stag.pctl_bp7policytype_alfa
                                                                ON              a.bp7policytype_alfa_stg=pctl_bp7policytype_alfa.id_stg
                                                                left join       db_t_prod_stag.pctl_puppolicytype
                                                                ON              a.puppolicytype_stg = pctl_puppolicytype.id_stg
                                                                                /*  Added for DB_T_STAG_MEMBXREF_PROD.Umbrella */
                                                                left join       db_t_prod_stag.pctl_foppolicytype
                                                                ON              a.foppolicytype_stg = pctl_foppolicytype.id_stg
                                                                inner join      db_t_prod_stag.pctl_policyperiodstatus
                                                                ON              pctl_policyperiodstatus.id_stg=a.status_stg
                                                                WHERE           pctl_policyperiodstatus.typecode_stg=''Bound''
                                                                AND             a.expirationdate_stg IS NULL qualify row_number() over(PARTITION BY nk_publicid,productname ORDER BY policyperiod_editeffectivedt DESC) = 1 ) src
                                         UNION
                                         SELECT DISTINCT cast (b.id_stg AS VARCHAR (64)) AS id,
                                                         /* cast (cc_policy.PolicyTypeCode_alfa as varchar (60)) PolicyTypeCode_alfa , */
                                                         cast (b.policysubtype_alfa AS VARCHAR (60))    policysubtype_alfa ,
                                                         ''NULL''                                      AS src_cd,
                                                         b.createtime_stg,
                                                         b.updatetime_stg,
                                                         cast (''AGMT_PROD_ROLE_TYPE1'' AS VARCHAR (60)) AS agmt_prod_role_cd,
                                                         cast (''POLICY TYPE'' AS          VARCHAR (60)) AS prod_sbtype_cd,
                                                         cast(NULL AS                    VARCHAR(50))  AS agmt_prod_cvge_lvl_desc
                                         FROM
                                                         /* DB_T_PROD_STAG.cc_policy  */
                                                         (
                                                                         SELECT          cc_policy.verified_stg,
                                                                                         cc_policy.createtime_stg,
                                                                                         cc_policy.updatetime_stg,
                                                                                         cc_policy.id_stg,
                                                                                         cctl_policysubtype_alfa.typecode_stg AS policysubtype_alfa,
                                                                                         CASE
                                                                                                         WHEN (
                                                                                                                                         coalesce(cc_policy.legacypolind_alfa_stg,0)=1) THEN ''Y''
                                                                                                         ELSE ''N''
                                                                                         END legacypolind_alfa
                                                                         FROM            db_t_prod_stag.cc_policy
                                                                         left outer join db_t_prod_stag.cctl_policysubtype_alfa
                                                                         ON              cc_policy.policysubtype_alfa_stg=cctl_policysubtype_alfa.id_stg
                                                                                         /* LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod ON cc_policy.PolicyNumber= pc_policyperiod.PolicyNumber */
                                                                         WHERE           cc_policy.updatetime_stg > (:start_dttm)
                                                                         AND             cc_policy.updatetime_stg <= (:end_dttm) ) b
                                         WHERE           ( (
                                                                                         b.verified_stg = 0
                                                                         AND             coalesce (b.legacypolind_alfa,''N'') <>''Y'')
                                                         OR              coalesce (b.legacypolind_alfa,''N'') =''Y'')
                                         AND             b.policysubtype_alfa IS NOT NULL qualify row_number() over(PARTITION BY id_stg,policysubtype_alfa ORDER BY createtime_stg DESC) = 1
                                         /* and cc_policy.PolicyTypeCode_alfa is NOT NULL */
                                         UNION
                                         SELECT *
                                         FROM   (
                                                                SELECT DISTINCT cast (c.nk_publicid AS VARCHAR (64))            AS nk_publicid,
                                                                                ltrim(rtrim(pctl_bp7propertytype.typecode_stg)) AS productname,
                                                                                ''SRC_SYS4''                                      AS src_cd,
                                                                                c.policyperiod_editeffectivedt,
                                                                                c.policyperiod_updatetime,
                                                                                cast (''AGMT_PROD_ROLE_TYPE2'' AS VARCHAR (60)) AS agmt_prod_role_cd,
                                                                                cast (''BUSINESS TYPE'' AS        VARCHAR (60)) AS prod_sbtype_cd,
                                                                                cast(NULL AS                    VARCHAR(50))  AS agmt_prod_cvge_lvl_desc
                                                                FROM
                                                                                /* DB_T_PROD_STAG.pc_PolicyLine  */
                                                                                (
                                                                                                SELECT          pc_policyperiod.publicid_stg AS nk_publicid,
                                                                                                                pc_policyperiod.policynumber_stg,
                                                                                                                pc_policyperiod.status_stg,
                                                                                                                pc_policyperiod.editeffectivedate_stg AS policyperiod_editeffectivedt,
                                                                                                                pc_policyperiod.updatetime_stg        AS policyperiod_updatetime,
                                                                                                                pc_policyline.bp7policytype_alfa_stg,
                                                                                                                pctl_bp7whatisinsured_alfa.name_stg AS bp7whatisinsured_alfa,
                                                                                                                pctl_bp7propertytype.name_stg       AS bp7linebusinesstype,
                                                                                                                pctl_number_alfa.typecode_stg       AS latepaycount,
                                                                                                                pc_policyline.expirationdate_stg
                                                                                                FROM            db_t_prod_stag.pc_policyline
                                                                                                left outer join db_t_prod_stag.pc_policyperiod
                                                                                                ON              pc_policyline.branchid_stg=pc_policyperiod.id_stg
                                                                                                left join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                ON              pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                left join       db_t_prod_stag.pctl_papolicytype_alfa
                                                                                                ON              pc_policyline.papolicytype_alfa_stg = pctl_papolicytype_alfa.id_stg
                                                                                                left join       db_t_prod_stag.pctl_bp7whatisinsured_alfa
                                                                                                ON              pc_policyline.bp7whatisinsured_alfa_stg = pctl_bp7whatisinsured_alfa.id_stg
                                                                                                left join       db_t_prod_stag.pctl_bp7propertytype
                                                                                                ON              pc_policyline.bp7linebusinesstype_stg = pctl_bp7propertytype.id_stg
                                                                                                left join       db_t_prod_stag.pctl_number_alfa
                                                                                                ON              pctl_number_alfa.id_stg = pc_policyline.latepaycount_alfa_stg
                                                                                                WHERE           pc_policyperiod.updatetime_stg > (:start_dttm)
                                                                                                AND             pc_policyperiod.updatetime_stg <= (:end_dttm) ) c
                                                                left outer join db_t_prod_stag.pctl_bp7propertytype
                                                                ON              c.bp7linebusinesstype=pctl_bp7propertytype.name_stg
                                                                inner join      db_t_prod_stag.pctl_policyperiodstatus
                                                                ON              pctl_policyperiodstatus.id_stg=c.status_stg
                                                                WHERE           c.bp7linebusinesstype IS NOT NULL
                                                                AND             pctl_policyperiodstatus.typecode_stg=''Bound''
                                                                AND             c.expirationdate_stg IS NULL qualify row_number() over(PARTITION BY nk_publicid,productname ORDER BY policyperiod_editeffectivedt DESC) = 1 ) src2 ) src ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
            SELECT    sq_pc_policyline.nk_publicid          AS nk_publicid,
                      sq_pc_policyline.customautosymboldesc AS product_name,
                      sq_pc_policyline.src_cd               AS src_cd,
                      sq_pc_policyline.createtime           AS createtime,
                      sq_pc_policyline.updatetime           AS updatetime,
                      lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */
                      AS out_agmt_prod_role_cd,
                      lkp_2.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */
                                                               AS out_prod_sbtype_cd,
                      sq_pc_policyline.agmt_prod_cvge_lvl_desc AS agmt_prod_cvge_lvl_desc,
                      sq_pc_policyline.source_record_id,
                      row_number() over (PARTITION BY sq_pc_policyline.source_record_id ORDER BY sq_pc_policyline.source_record_id) AS rnk
            FROM      sq_pc_policyline
            left join lkp_teradata_etl_ref_xlat lkp_1
            ON        lkp_1.src_idntftn_val = sq_pc_policyline.agmt_prod_role_cd
            left join lkp_teradata_etl_ref_xlat lkp_2
            ON        lkp_2.src_idntftn_val = sq_pc_policyline.prod_sbtype_cd qualify rnk = 1 );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
            SELECT    exp_pass_from_source.nk_publicid           AS nkpublicid,
                      exp_pass_from_source.product_name          AS product_name,
                      exp_pass_from_source.out_agmt_prod_role_cd AS agmt_prod_role_cd,
                      exp_pass_from_source.out_prod_sbtype_cd    AS prod_sbtype_cd,
                      exp_pass_from_source.createtime            AS agmt_prod_strt_dttm,
                      to_date ( ''31/12/9999'' , ''DD/MM/YYYY'' )    AS agmt_prod_end_dttm,
                      :prcs_id                                   AS out_prcs_id,
                      :p_agmt_type_cd_policy_version             AS out_agmt_type_cd,
                      lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */
                                                                   AS out_src_cd,
                      exp_pass_from_source.updatetime              AS updatetime,
                      exp_pass_from_source.agmt_prod_cvge_lvl_desc AS agmt_prod_cvge_lvl_desc,
                      exp_pass_from_source.source_record_id,
                      row_number() over (PARTITION BY exp_pass_from_source.source_record_id ORDER BY exp_pass_from_source.source_record_id) AS rnk
            FROM      exp_pass_from_source
            left join lkp_teradata_etl_ref_xlat_src_cd lkp_1
            ON        lkp_1.src_idntftn_val = exp_pass_from_source.src_cd qualify rnk = 1 );
  -- Component LKP_PROD, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_prod AS
  (
            SELECT    lkp.prod_id,
                      exp_data_transformation.source_record_id,
                      row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.prod_id ASC,lkp.prod_scrp_id ASC,lkp.prod_sbtype_cd ASC,lkp.prod_desc ASC,lkp.prod_name ASC,lkp.host_prod_id ASC,lkp.prod_strt_dttm ASC,lkp.prod_end_dttm ASC,lkp.prod_pkg_type_cd ASC,lkp.fincl_prod_ind ASC,lkp.prod_txt ASC,lkp.prod_crtn_dt ASC,lkp.insrnc_type_cd ASC,lkp.dy_cnt_bss_cd ASC,lkp.sry_lvl_cd ASC,lkp.cury_cd ASC,lkp.prcs_id ASC,lkp.insrnc_lob_type_cd ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) rnk
            FROM      exp_data_transformation
            left join
                      (
                             SELECT prod_id,
                                    prod_scrp_id,
                                    prod_sbtype_cd,
                                    prod_desc,
                                    prod_name,
                                    host_prod_id,
                                    prod_strt_dttm,
                                    prod_end_dttm,
                                    prod_pkg_type_cd,
                                    fincl_prod_ind,
                                    prod_txt,
                                    prod_crtn_dt,
                                    insrnc_type_cd,
                                    dy_cnt_bss_cd,
                                    sry_lvl_cd,
                                    cury_cd,
                                    prcs_id,
                                    insrnc_lob_type_cd,
                                    edw_strt_dttm,
                                    edw_end_dttm
                             FROM   db_t_prod_core.prod ) lkp
            ON        lkp.prod_name = exp_data_transformation.product_name qualify rnk = 1 );
  -- Component LKP_AGMT_NEW, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt_new AS
  (
            SELECT    lkp.agmt_id,
                      exp_data_transformation.source_record_id,
                      row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.agmt_id ASC,lkp.host_agmt_num ASC,lkp.agmt_name ASC,lkp.agmt_opn_dttm ASC,lkp.agmt_cls_dttm ASC,lkp.agmt_plnd_expn_dttm ASC,lkp.agmt_signd_dttm ASC,lkp.agmt_type_cd ASC,lkp.agmt_legly_bindg_ind ASC,lkp.agmt_src_cd ASC,lkp.agmt_cur_sts_cd ASC,lkp.agmt_cur_sts_rsn_cd ASC,lkp.agmt_obtnd_cd ASC,lkp.agmt_sbtype_cd ASC,lkp.agmt_prcsg_dttm ASC,lkp.alt_agmt_name ASC,lkp.asset_liabty_cd ASC,lkp.bal_shet_cd ASC,lkp.stmt_cycl_cd ASC,lkp.stmt_ml_type_cd ASC,lkp.prposl_id ASC,lkp.agmt_objtv_type_cd ASC,lkp.fincl_agmt_sbtype_cd ASC,lkp.mkt_risk_type_cd ASC,lkp.orignl_maturty_dt ASC,lkp.risk_expsr_mtgnt_sbtype_cd ASC,lkp.bnk_trd_bk_cd ASC,lkp.prcg_meth_sbtype_cd ASC,lkp.fincl_agmt_type_cd ASC,lkp.dy_cnt_bss_cd ASC,lkp.frst_prem_due_dt ASC,lkp.insrnc_agmt_sbtype_cd ASC,lkp.insrnc_agmt_type_cd ASC,lkp.ntwk_srvc_agmt_type_cd ASC,lkp.frmlty_type_cd ASC,lkp.cntrct_term_num ASC,lkp.rate_rprcg_cycl_mth_num ASC,lkp.cmpnd_int_cycl_mth_num ASC,lkp.mdterm_int_pmt_cycl_mth_num ASC,lkp.prev_mdterm_int_pmt_dt ASC,lkp.nxt_mdterm_int_pmt_dt ASC,lkp.prev_int_rate_rvsd_dt ASC,lkp.nxt_int_rate_rvsd_dt ASC,lkp.prev_ref_dt_int_rate ASC,lkp.nxt_ref_dt_for_int_rate ASC,lkp.mdterm_cncltn_dt ASC,lkp.stk_flow_clas_in_mth_ind ASC,lkp.stk_flow_clas_in_term_ind ASC,lkp.lgcy_dscnt_ind ASC,lkp.agmt_idntftn_cd ASC,lkp.trmtn_type_cd ASC,lkp.int_pmt_meth_cd ASC,lkp.lbr_agmt_desc ASC,lkp.guartd_imprsns_cnt ASC,lkp.cost_per_imprsn_amt ASC,lkp.guartd_clkthru_cnt ASC,lkp.cost_per_clkthru_amt ASC,lkp.busn_prty_id ASC,lkp.pmt_pln_type_cd ASC,lkp.invc_strem_type_cd ASC,lkp.modl_crtn_dttm ASC,lkp.cntnus_srvc_dttm ASC,lkp.bilg_meth_type_cd ASC,lkp.src_sys_cd ASC,lkp.agmt_eff_dttm ASC,lkp.modl_eff_dttm ASC,lkp.prcs_id ASC,lkp.modl_actl_end_dttm ASC,lkp.tier_type_cd ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.vfyd_plcy_ind ASC,lkp.src_of_busn_cd ASC,lkp.nk_src_key ASC,lkp.ovrd_coms_type_cd ASC,lkp.lgcy_plcy_ind ASC,lkp.trans_strt_dttm ASC) rnk
            FROM      exp_data_transformation
            left join
                      (
                               SELECT   agmt.agmt_id                     AS agmt_id,
                                        agmt.host_agmt_num               AS host_agmt_num,
                                        agmt.agmt_name                   AS agmt_name,
                                        agmt.agmt_opn_dttm               AS agmt_opn_dttm,
                                        agmt.agmt_cls_dttm               AS agmt_cls_dttm,
                                        agmt.agmt_plnd_expn_dttm         AS agmt_plnd_expn_dttm,
                                        agmt.agmt_signd_dttm             AS agmt_signd_dttm,
                                        agmt.agmt_legly_bindg_ind        AS agmt_legly_bindg_ind,
                                        agmt.agmt_src_cd                 AS agmt_src_cd,
                                        agmt.agmt_cur_sts_cd             AS agmt_cur_sts_cd,
                                        agmt.agmt_cur_sts_rsn_cd         AS agmt_cur_sts_rsn_cd,
                                        agmt.agmt_obtnd_cd               AS agmt_obtnd_cd,
                                        agmt.agmt_sbtype_cd              AS agmt_sbtype_cd,
                                        agmt.agmt_prcsg_dttm             AS agmt_prcsg_dttm,
                                        agmt.alt_agmt_name               AS alt_agmt_name,
                                        agmt.asset_liabty_cd             AS asset_liabty_cd,
                                        agmt.bal_shet_cd                 AS bal_shet_cd,
                                        agmt.stmt_cycl_cd                AS stmt_cycl_cd,
                                        agmt.stmt_ml_type_cd             AS stmt_ml_type_cd,
                                        agmt.prposl_id                   AS prposl_id,
                                        agmt.agmt_objtv_type_cd          AS agmt_objtv_type_cd,
                                        agmt.fincl_agmt_sbtype_cd        AS fincl_agmt_sbtype_cd,
                                        agmt.mkt_risk_type_cd            AS mkt_risk_type_cd,
                                        agmt.orignl_maturty_dt           AS orignl_maturty_dt,
                                        agmt.risk_expsr_mtgnt_sbtype_cd  AS risk_expsr_mtgnt_sbtype_cd,
                                        agmt.bnk_trd_bk_cd               AS bnk_trd_bk_cd,
                                        agmt.prcg_meth_sbtype_cd         AS prcg_meth_sbtype_cd,
                                        agmt.fincl_agmt_type_cd          AS fincl_agmt_type_cd,
                                        agmt.dy_cnt_bss_cd               AS dy_cnt_bss_cd,
                                        agmt.frst_prem_due_dt            AS frst_prem_due_dt,
                                        agmt.insrnc_agmt_sbtype_cd       AS insrnc_agmt_sbtype_cd,
                                        agmt.insrnc_agmt_type_cd         AS insrnc_agmt_type_cd,
                                        agmt.ntwk_srvc_agmt_type_cd      AS ntwk_srvc_agmt_type_cd,
                                        agmt.frmlty_type_cd              AS frmlty_type_cd,
                                        agmt.cntrct_term_num             AS cntrct_term_num,
                                        agmt.rate_rprcg_cycl_mth_num     AS rate_rprcg_cycl_mth_num,
                                        agmt.cmpnd_int_cycl_mth_num      AS cmpnd_int_cycl_mth_num,
                                        agmt.mdterm_int_pmt_cycl_mth_num AS mdterm_int_pmt_cycl_mth_num,
                                        agmt.prev_mdterm_int_pmt_dt      AS prev_mdterm_int_pmt_dt,
                                        agmt.nxt_mdterm_int_pmt_dt       AS nxt_mdterm_int_pmt_dt,
                                        agmt.prev_int_rate_rvsd_dt       AS prev_int_rate_rvsd_dt,
                                        agmt.nxt_int_rate_rvsd_dt        AS nxt_int_rate_rvsd_dt,
                                        agmt.prev_ref_dt_int_rate        AS prev_ref_dt_int_rate,
                                        agmt.nxt_ref_dt_for_int_rate     AS nxt_ref_dt_for_int_rate,
                                        agmt.mdterm_cncltn_dt            AS mdterm_cncltn_dt,
                                        agmt.stk_flow_clas_in_mth_ind    AS stk_flow_clas_in_mth_ind,
                                        agmt.stk_flow_clas_in_term_ind   AS stk_flow_clas_in_term_ind,
                                        agmt.lgcy_dscnt_ind              AS lgcy_dscnt_ind,
                                        agmt.agmt_idntftn_cd             AS agmt_idntftn_cd,
                                        agmt.trmtn_type_cd               AS trmtn_type_cd,
                                        agmt.int_pmt_meth_cd             AS int_pmt_meth_cd,
                                        agmt.lbr_agmt_desc               AS lbr_agmt_desc,
                                        agmt.guartd_imprsns_cnt          AS guartd_imprsns_cnt,
                                        agmt.cost_per_imprsn_amt         AS cost_per_imprsn_amt,
                                        agmt.guartd_clkthru_cnt          AS guartd_clkthru_cnt,
                                        agmt.cost_per_clkthru_amt        AS cost_per_clkthru_amt,
                                        agmt.busn_prty_id                AS busn_prty_id,
                                        agmt.pmt_pln_type_cd             AS pmt_pln_type_cd,
                                        agmt.invc_strem_type_cd          AS invc_strem_type_cd,
                                        agmt.modl_crtn_dttm              AS modl_crtn_dttm,
                                        agmt.cntnus_srvc_dttm            AS cntnus_srvc_dttm,
                                        agmt.bilg_meth_type_cd           AS bilg_meth_type_cd,
                                        agmt.src_sys_cd                  AS src_sys_cd,
                                        agmt.agmt_eff_dttm               AS agmt_eff_dttm,
                                        agmt.modl_eff_dttm               AS modl_eff_dttm,
                                        agmt.prcs_id                     AS prcs_id,
                                        agmt.modl_actl_end_dttm          AS modl_actl_end_dttm,
                                        agmt.tier_type_cd                AS tier_type_cd,
                                        agmt.edw_strt_dttm               AS edw_strt_dttm,
                                        agmt.edw_end_dttm                AS edw_end_dttm,
                                        agmt.vfyd_plcy_ind               AS vfyd_plcy_ind,
                                        agmt.src_of_busn_cd              AS src_of_busn_cd,
                                        agmt.ovrd_coms_type_cd           AS ovrd_coms_type_cd,
                                        agmt.lgcy_plcy_ind               AS lgcy_plcy_ind,
                                        agmt.trans_strt_dttm             AS trans_strt_dttm,
                                        agmt.nk_src_key                  AS nk_src_key,
                                        agmt.agmt_type_cd                AS agmt_type_cd
                               FROM     db_t_prod_core.agmt qualify row_number() over(PARTITION BY agmt.nk_src_key,agmt.host_agmt_num ORDER BY agmt.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.nk_src_key = exp_data_transformation.nkpublicid
            AND       lkp.agmt_type_cd = exp_data_transformation.out_agmt_type_cd qualify rnk = 1 );
  -- Component exp_SrcFields, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_srcfields AS
  (
             SELECT     lkp_agmt_new.agmt_id                                                  AS in_agmt_id,
                        lkp_prod.prod_id                                                      AS in_prod_id,
                        exp_data_transformation.agmt_prod_role_cd                             AS in_agmt_prod_role_cd,
                        exp_data_transformation.agmt_prod_strt_dttm                           AS in_agmt_prod_strt_dt,
                        exp_data_transformation.agmt_prod_end_dttm                            AS in_agmt_prod_end_dt,
                        exp_data_transformation.out_prcs_id                                   AS in_prcs_id,
                        current_timestamp                                                     AS in_edw_strt_dttm,
                        to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS in_edw_end_dttm,
                        exp_data_transformation.updatetime                                    AS in_trans_strt_dttm,
                        to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS in_trans_end_dttm,
                        exp_data_transformation.agmt_prod_cvge_lvl_desc                       AS in_agmt_prod_cvge_lvl_desc,
                        exp_data_transformation.source_record_id
             FROM       exp_data_transformation
             inner join lkp_prod
             ON         exp_data_transformation.source_record_id = lkp_prod.source_record_id
             inner join lkp_agmt_new
             ON         lkp_prod.source_record_id = lkp_agmt_new.source_record_id );
  -- Component LKP_AGMT_PROD, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt_prod AS
  (
            SELECT    lkp.agmt_id,
                      lkp.prod_id,
                      lkp.agmt_prod_role_cd,
                      lkp.agmt_prod_strt_dttm,
                      lkp.agmt_prod_end_dttm,
                      lkp.agmt_prod_cvge_lvl_desc,
                      lkp.edw_strt_dttm,
                      exp_srcfields.in_agmt_id           AS in_agmt_id,
                      exp_srcfields.in_prod_id           AS in_prod_id,
                      exp_srcfields.in_agmt_prod_role_cd AS in_agmt_prod_role_cd,
                      exp_srcfields.source_record_id,
                      row_number() over(PARTITION BY exp_srcfields.source_record_id ORDER BY lkp.agmt_id ASC,lkp.prod_id ASC,lkp.agmt_prod_role_cd ASC,lkp.agmt_prod_strt_dttm ASC,lkp.agmt_prod_end_dttm ASC,lkp.agmt_prod_cvge_lvl_desc ASC,lkp.edw_strt_dttm ASC) rnk
            FROM      exp_srcfields
            left join
                      (
                               SELECT   agmt_prod_strt_dttm               AS agmt_prod_strt_dttm ,
                                        agmt_prod.agmt_prod_end_dttm      AS agmt_prod_end_dttm,
                                        agmt_prod.agmt_prod_cvge_lvl_desc AS agmt_prod_cvge_lvl_desc,
                                        agmt_prod.edw_strt_dttm           AS edw_strt_dttm,
                                        agmt_prod.agmt_id                 AS agmt_id,
                                        agmt_prod.prod_id                 AS prod_id,
                                        agmt_prod.agmt_prod_role_cd       AS agmt_prod_role_cd
                               FROM     db_t_prod_core.agmt_prod qualify row_number() over(PARTITION BY agmt_id,prod_id,agmt_prod_role_cd ORDER BY edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.agmt_id = exp_srcfields.in_agmt_id
            AND       lkp.prod_id = exp_srcfields.in_prod_id
            AND       lkp.agmt_prod_role_cd = exp_srcfields.in_agmt_prod_role_cd qualify rnk = 1 );
  -- Component exp_CDC_Check, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_cdc_check AS
  (
             SELECT     exp_srcfields.in_agmt_id                 AS in_agmt_id,
                        exp_srcfields.in_prod_id                 AS in_prod_id,
                        exp_srcfields.in_agmt_prod_role_cd       AS in_agmt_prod_role_cd,
                        exp_srcfields.in_agmt_prod_strt_dt       AS in_agmt_prod_strt_dt,
                        exp_srcfields.in_agmt_prod_end_dt        AS in_agmt_prod_end_dt,
                        exp_srcfields.in_agmt_prod_cvge_lvl_desc AS in_agmt_prod_cvge_lvl_desc,
                        exp_srcfields.in_prcs_id                 AS in_prcs_id,
                        exp_srcfields.in_edw_strt_dttm           AS in_edw_strt_dttm,
                        exp_srcfields.in_edw_end_dttm            AS in_edw_end_dttm,
                        exp_srcfields.in_trans_strt_dttm         AS in_trans_strt_dttm,
                        exp_srcfields.in_trans_end_dttm          AS in_trans_end_dttm,
                        lkp_agmt_prod.agmt_id                    AS lkp_agmt_id,
                        lkp_agmt_prod.prod_id                    AS lkp_prod_id,
                        lkp_agmt_prod.agmt_prod_role_cd          AS lkp_agmt_prod_role_cd,
                        lkp_agmt_prod.agmt_prod_cvge_lvl_desc    AS lkp_agmt_prod_cvge_lvl_desc,
                        lkp_agmt_prod.edw_strt_dttm              AS lkp_edw_strt_dttm,
                        md5 ( ltrim ( rtrim ( to_char ( exp_srcfields.in_agmt_prod_strt_dt ) ) )
                                   || ltrim ( rtrim ( to_char ( exp_srcfields.in_agmt_prod_end_dt ) ) )
                                   || ltrim ( rtrim ( exp_srcfields.in_agmt_prod_cvge_lvl_desc ) ) ) AS v_src_md5,
                        md5 ( ltrim ( rtrim ( to_char ( lkp_agmt_prod.agmt_prod_strt_dttm ) ) )
                                   || ltrim ( rtrim ( to_char ( lkp_agmt_prod.agmt_prod_end_dttm ) ) )
                                   || ltrim ( rtrim ( lkp_agmt_prod.agmt_prod_cvge_lvl_desc ) ) ) AS v_tgt_md5,
                        CASE
                                   WHEN v_tgt_md5 IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_src_md5 = v_tgt_md5 THEN ''R''
                                                         ELSE ''U''
                                              END
                        END AS o_src_tgt,
                        exp_srcfields.source_record_id
             FROM       exp_srcfields
             inner join lkp_agmt_prod
             ON         exp_srcfields.source_record_id = lkp_agmt_prod.source_record_id );
  -- Component rtr_AGMT_PROD_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_agmt_prod_insert as
    SELECT exp_cdc_check.in_agmt_id                  AS in_agmt_id,
         exp_cdc_check.in_prod_id                  AS in_prod_id,
         exp_cdc_check.in_agmt_prod_role_cd        AS in_agmt_prod_role_cd,
         exp_cdc_check.in_agmt_prod_strt_dt        AS in_agmt_prod_strt_dt,
         exp_cdc_check.in_agmt_prod_end_dt         AS in_agmt_prod_end_dt,
         exp_cdc_check.in_prcs_id                  AS in_prcs_id,
         exp_cdc_check.in_edw_strt_dttm            AS in_edw_strt_dttm,
         exp_cdc_check.in_edw_end_dttm             AS in_edw_end_dttm,
         exp_cdc_check.in_trans_strt_dttm          AS in_trans_strt_dttm,
         exp_cdc_check.in_trans_end_dttm           AS in_trans_end_dttm,
         exp_cdc_check.lkp_agmt_id                 AS lkp_agmt_id,
         exp_cdc_check.lkp_prod_id                 AS lkp_prod_id,
         exp_cdc_check.lkp_agmt_prod_role_cd       AS lkp_agmt_prod_role_cd,
         exp_cdc_check.lkp_edw_strt_dttm           AS lkp_edw_strt_dttm,
         exp_cdc_check.o_src_tgt                   AS o_src_tgt,
         NULL                                      AS out_trans_end_dttm,
         exp_cdc_check.in_agmt_prod_cvge_lvl_desc  AS in_agmt_prod_cvge_lvl_desc,
         exp_cdc_check.lkp_agmt_prod_cvge_lvl_desc AS lkp_agmt_prod_cvge_lvl_desc,
         exp_cdc_check.source_record_id
  FROM   exp_cdc_check
  WHERE  exp_cdc_check.o_src_tgt = ''I''
  AND    exp_cdc_check.in_agmt_id IS NOT NULL 
  AND    exp_cdc_check.in_prod_id IS NOT NULL;
  
  -- Component rtr_AGMT_PROD_UPDATE, Type ROUTER Output Group UPDATE
  create or replace temporary table rtr_agmt_prod_update as
  SELECT exp_cdc_check.in_agmt_id                  AS in_agmt_id,
         exp_cdc_check.in_prod_id                  AS in_prod_id,
         exp_cdc_check.in_agmt_prod_role_cd        AS in_agmt_prod_role_cd,
         exp_cdc_check.in_agmt_prod_strt_dt        AS in_agmt_prod_strt_dt,
         exp_cdc_check.in_agmt_prod_end_dt         AS in_agmt_prod_end_dt,
         exp_cdc_check.in_prcs_id                  AS in_prcs_id,
         exp_cdc_check.in_edw_strt_dttm            AS in_edw_strt_dttm,
         exp_cdc_check.in_edw_end_dttm             AS in_edw_end_dttm,
         exp_cdc_check.in_trans_strt_dttm          AS in_trans_strt_dttm,
         exp_cdc_check.in_trans_end_dttm           AS in_trans_end_dttm,
         exp_cdc_check.lkp_agmt_id                 AS lkp_agmt_id,
         exp_cdc_check.lkp_prod_id                 AS lkp_prod_id,
         exp_cdc_check.lkp_agmt_prod_role_cd       AS lkp_agmt_prod_role_cd,
         exp_cdc_check.lkp_edw_strt_dttm           AS lkp_edw_strt_dttm,
         exp_cdc_check.o_src_tgt                   AS o_src_tgt,
         NULL                                      AS out_trans_end_dttm,
         exp_cdc_check.in_agmt_prod_cvge_lvl_desc  AS in_agmt_prod_cvge_lvl_desc,
         exp_cdc_check.lkp_agmt_prod_cvge_lvl_desc AS lkp_agmt_prod_cvge_lvl_desc,
         exp_cdc_check.source_record_id
  FROM   exp_cdc_check
  WHERE  exp_cdc_check.o_src_tgt = ''U'' 
  AND    exp_cdc_check.in_agmt_id IS NOT NULL
  AND    exp_cdc_check.in_prod_id IS NOT NULL;
  
  -- Component AGMT_PROD_ins, Type TARGET
  INSERT INTO db_t_prod_core.agmt_prod
              (
                          agmt_id,
                          prod_id,
                          agmt_prod_role_cd,
                          agmt_prod_cvge_lvl_desc,
                          agmt_prod_strt_dttm,
                          agmt_prod_end_dttm,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT rtr_agmt_prod_insert.in_agmt_id                 AS agmt_id,
         rtr_agmt_prod_insert.in_prod_id                 AS prod_id,
         rtr_agmt_prod_insert.in_agmt_prod_role_cd       AS agmt_prod_role_cd,
         rtr_agmt_prod_insert.in_agmt_prod_cvge_lvl_desc AS agmt_prod_cvge_lvl_desc,
         rtr_agmt_prod_insert.in_agmt_prod_strt_dt       AS agmt_prod_strt_dttm,
         rtr_agmt_prod_insert.in_agmt_prod_end_dt        AS agmt_prod_end_dttm,
         rtr_agmt_prod_insert.in_prcs_id                 AS prcs_id,
         rtr_agmt_prod_insert.in_edw_strt_dttm           AS edw_strt_dttm,
         rtr_agmt_prod_insert.in_edw_end_dttm            AS edw_end_dttm,
         rtr_agmt_prod_insert.in_trans_strt_dttm         AS trans_strt_dttm
  FROM   rtr_agmt_prod_insert;
  
  -- Component AGMT_PROD_updins, Type TARGET
  INSERT INTO db_t_prod_core.agmt_prod
              (
                          agmt_id,
                          prod_id,
                          agmt_prod_role_cd,
                          agmt_prod_cvge_lvl_desc,
                          agmt_prod_strt_dttm,
                          agmt_prod_end_dttm,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT rtr_agmt_prod_update.in_agmt_id                 AS agmt_id,
         rtr_agmt_prod_update.in_prod_id                 AS prod_id,
         rtr_agmt_prod_update.in_agmt_prod_role_cd       AS agmt_prod_role_cd,
         rtr_agmt_prod_update.in_agmt_prod_cvge_lvl_desc AS agmt_prod_cvge_lvl_desc,
         rtr_agmt_prod_update.in_agmt_prod_strt_dt       AS agmt_prod_strt_dttm,
         rtr_agmt_prod_update.in_agmt_prod_end_dt        AS agmt_prod_end_dttm,
         rtr_agmt_prod_update.in_prcs_id                 AS prcs_id,
         rtr_agmt_prod_update.in_edw_strt_dttm           AS edw_strt_dttm,
         rtr_agmt_prod_update.in_edw_end_dttm            AS edw_end_dttm,
         rtr_agmt_prod_update.in_trans_strt_dttm         AS trans_strt_dttm
  FROM   rtr_agmt_prod_update;
  
  -- Component upd_stg_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_stg_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_agmt_prod_update.lkp_agmt_id           AS lkp_agmt_id3,
                rtr_agmt_prod_update.lkp_prod_id           AS lkp_prod_id3,
                rtr_agmt_prod_update.lkp_agmt_prod_role_cd AS lkp_agmt_prod_role_cd3,
                rtr_agmt_prod_update.lkp_edw_strt_dttm     AS lkp_edw_strt_dttm3,
                rtr_agmt_prod_update.out_trans_end_dttm    AS out_trans_end_dttm3,
                rtr_agmt_prod_update.in_trans_strt_dttm    AS in_trans_strt_dttm3,
                1                                          AS update_strategy_action,
                rtr_agmt_prod_update.source_record_id
         FROM   rtr_agmt_prod_update );
  -- Component exp_pass_to_target_ins11, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ins11 AS
  (
         SELECT upd_stg_upd.lkp_agmt_id3                            AS lkp_agmt_id3,
                upd_stg_upd.lkp_prod_id3                            AS lkp_prod_id3,
                upd_stg_upd.lkp_agmt_prod_role_cd3                  AS lkp_agmt_prod_role_cd3,
                upd_stg_upd.lkp_edw_strt_dttm3                      AS lkp_edw_strt_dttm3,
                dateadd(''second'', - 1, current_timestamp)               AS o_dateexpiry,
                dateadd(''second'', - 1, upd_stg_upd.in_trans_strt_dttm3) AS out_trans_end_dttm3,
                upd_stg_upd.source_record_id
         FROM   upd_stg_upd );
  -- Component AGMT_PROD_upd, Type TARGET
  merge
  INTO         db_t_prod_core.agmt_prod
  USING        exp_pass_to_target_ins11
  ON (
                            agmt_prod.agmt_id = exp_pass_to_target_ins11.lkp_agmt_id3
               AND          agmt_prod.prod_id = exp_pass_to_target_ins11.lkp_prod_id3
               AND          agmt_prod.agmt_prod_role_cd = exp_pass_to_target_ins11.lkp_agmt_prod_role_cd3
               AND          agmt_prod.edw_strt_dttm = exp_pass_to_target_ins11.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    agmt_id = exp_pass_to_target_ins11.lkp_agmt_id3,
         prod_id = exp_pass_to_target_ins11.lkp_prod_id3,
         agmt_prod_role_cd = exp_pass_to_target_ins11.lkp_agmt_prod_role_cd3,
         edw_strt_dttm = exp_pass_to_target_ins11.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_target_ins11.o_dateexpiry,
         trans_end_dttm = exp_pass_to_target_ins11.out_trans_end_dttm3;



INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
SELECT :run_id, :worklet_name, ''m_base_agmt_prod_insupd'', ''SUCCEEDED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
  ''start_dttm'', :start_dttm,
  ''end_dttm'', :end_dttm,
  ''StartTime'', :v_start_time,
  ''SrcSuccessRows'', (SELECT COUNT(*) FROM sq_priorloss),
  ''TgtSuccessRows'', (SELECT COUNT(*) FROM fil)
);

EXCEPTION WHEN OTHER THEN
    INSERT INTO control_status (run_id, worklet_name, task_name, task_status, task_start_dttm, task_end_dttm, var_json)
    SELECT :run_id, :worklet_name, ''m_base_agmt_prod_insupd'', ''FAILED'', :v_start_time, CURRENT_TIMESTAMP(), OBJECT_CONSTRUCT(
        ''SQLERRM'', :sqlerrm
    );


END;
';