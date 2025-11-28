-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_QUOTN_PRTY_FEAT_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '

  declare
       run_id STRING;
       start_dttm TIMESTAMP;
       end_dttm TIMESTAMP;
       PRCS_ID STRING;

BEGIN

       run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
       start_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''start_dttm'' LIMIT 1);
       end_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''end_dttm'' LIMIT 1);
       PRCS_ID := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''PRCS_ID'' LIMIT 1);
  -- Component LKP_BUSN, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_busn AS
  (
           SELECT   busn.busn_prty_id     AS busn_prty_id,
                    busn.src_sys_cd       AS src_sys_cd,
                    busn.tax_brakt_cd     AS tax_brakt_cd,
                    busn.org_type_cd      AS org_type_cd,
                    busn.gics_sbidstry_cd AS gics_sbidstry_cd,
                    busn.lifcycl_cd       AS lifcycl_cd,
                    busn.prty_type_cd     AS prty_type_cd,
                    busn.busn_end_dttm    AS busn_end_dttm,
                    busn.busn_strt_dttm   AS busn_strt_dttm,
                    busn.inc_ind          AS inc_ind,
                    busn.edw_strt_dttm    AS edw_strt_dttm,
                    busn.edw_end_dttm     AS edw_end_dttm,
                    busn.busn_ctgy_cd     AS busn_ctgy_cd,
                    busn.nk_busn_cd       AS nk_busn_cd
           FROM     db_t_prod_core.busn qualify row_number () over (PARTITION BY nk_busn_cd,busn_ctgy_cd ORDER BY edw_end_dttm DESC )=1 );
  -- Component LKP_INDIV_CLM_CTR, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_indiv_clm_ctr AS
  (
         SELECT indiv.indiv_prty_id AS indiv_prty_id,
                indiv.nk_publc_id   AS nk_publc_id
         FROM   db_t_prod_core.indiv
         WHERE  indiv.nk_publc_id IS NOT NULL );
  -- Component LKP_INDIV_CNT_MGR, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_indiv_cnt_mgr AS
  (
         SELECT indiv.indiv_prty_id     AS indiv_prty_id,
                lower(indiv.nk_link_id) AS nk_link_id
         FROM   db_t_prod_core.indiv
         WHERE  indiv.nk_publc_id IS NULL );
  -- Component LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_feat_sbtype_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''FEAT_SBTYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_PRTY_QUOTN_ROLE_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_prty_quotn_role_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRTY_QUOTN_ROLE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm IN (''pctl_policycontactrole.TYPECODE'',
                                                         ''derived'',
                                                         ''pctl_userrole.typecode'',
                                                         ''pctl_additionalinteresttype.typecode'',
                                                         ''pc_job.updateuserid'',
                                                         ''pctl_accountcontactrole.typecode'',
                                                         ''pc_job.createuserid'',
                                                         ''bctl_accountrole.typecode'')
         AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''GW'',
                                                          ''DS'')
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component SQ_quotn_prty_feat, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_quotn_prty_feat AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS jobnumber,
                $2  AS branchnumber,
                $3  AS feat_sbtype_cd,
                $4  AS feat_insrnc_sbtype_cd,
                $5  AS typecode,
                $6  AS nk_src_key,
                $7  AS addressbookuid,
                $8  AS prty_quotn_strt_dt,
                $9  AS quotn_feat_strt_dt,
                $10 AS quotn_feat_end_dt,
                $11 AS cntct_type_cd,
                $12 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   quotn_prty_feat.jobnumber,
                                                    quotn_prty_feat.branchnumber,
                                                    quotn_prty_feat.feat_sbtype_cd,
                                                    quotn_prty_feat.feat_insrnc_sbtype_cd,
                                                    quotn_prty_feat.typecode,
                                                    quotn_prty_feat.nk_src_key,
                                                    quotn_prty_feat.addressbookuid,
                                                    cast(quotn_prty_feat.prty_quotn_strt_dt AS DATE) AS prty_quotn_strt_dt,
                                                    cast(quotn_prty_feat.quotn_feat_strt_dt AS DATE) AS quotn_feat_strt_dt,
                                                    quotn_prty_feat.quotn_feat_end_dt,
                                                    quotn_prty_feat.cntct_type_cd
                                           FROM     (
                                                               /* This section following is from DZ SQ added through optimization task EIM-23175*/
                                                               SELECT     job.jobnumber_stg AS jobnumber,
                                                                          /* 01 */
                                                                          pp.branchnumber_stg AS branchnumber,
                                                                          /* 02 */
                                                                          ''FEAT_SBTYPE13'' AS feat_sbtype_cd ,
                                                                          /* 03 */
                                                                          ''FEAT_INSRNC_SBTYPE3'' AS feat_insrnc_sbtype_cd ,
                                                                          /* feat_insrnc_sbtype3 is exclsn (exclusion) --04 */
                                                                          tl1.typecode_stg AS typecode,
                                                                          /* 05 */
                                                                          tl2.typecode_stg AS nk_src_key,
                                                                          /* 06 */
                                                                          contact.addressbookuid_stg AS addressbookuid,
                                                                          /* 07 */
                                                                          pp.periodstart_stg AS prty_quotn_strt_dt,
                                                                          /* 08 */
                                                                          pp.editeffectivedate_stg AS quotn_feat_strt_dt,
                                                                          /* 09 */
                                                                          cast(NULL AS timestamp) AS quotn_feat_end_dt,
                                                                          /* 10 */
                                                                          tl4.typecode_stg AS cntct_type_cd,
                                                                          /* 11 */
                                                                          pp.updatetime_stg AS updatetime
                                                               FROM       db_t_prod_stag.pc_policyperiod pp
                                                               inner join db_t_prod_stag.pc_policycontactrole pcr
                                                               ON         pp.id_stg = pcr.branchid_stg
                                                               inner join db_t_prod_stag.pctl_policycontactrole tl1
                                                               ON         tl1.id_stg = pcr.subtype_stg
                                                               inner join db_t_prod_stag.pctl_exclusiontype_alfa tl2
                                                               ON         pcr.exclusiontype_alfa_stg = tl2.id_stg
                                                               inner join db_t_prod_stag.pc_contact contact
                                                               ON         contact.id_stg = pcr.contactdenorm_stg
                                                               inner join db_t_prod_stag.pctl_policyperiodstatus tl3
                                                               ON         tl3.id_stg = pp.status_stg
                                                               inner join db_t_prod_stag.pctl_contact tl4
                                                               ON         tl4.id_stg = contact.subtype_stg
                                                               inner join db_t_prod_stag.pc_job job
                                                               ON         job.id_stg = pp.jobid_stg
                                                               inner join db_t_prod_stag.pctl_job tl
                                                               ON         tl.id_stg = job.subtype_stg
                                                               WHERE      lower(tl.name_stg) IN (''renewal'',
                                                                                                 ''submission'',
                                                                                                 ''policy change'')
                                                               AND        contact.addressbookuid_stg IS NOT NULL
                                                               AND        pp.updatetime_stg > (:start_dttm)
                                                               AND        pp.updatetime_stg <= (:end_dttm)
                                                               /* ------------------------------------------------------------------------------------------- */
                                                               /* ----------------------------------------- */
                                                               UNION
                                                               /* ---- waive reasons ------------------------------------------------------------------------------------------------------------------- */
                                                               SELECT     job.jobnumber_stg,
                                                                          pp.branchnumber_stg,
                                                                          ''FEAT_SBTYPE14''       AS feat_sbtype_cd ,
                                                                          ''FEAT_INSRNC_SBTYPE4'' AS feat_insrnc_sbtype_cd ,
                                                                          tl1.typecode_stg ,
                                                                          tl2.typecode_stg AS nk_src_key ,
                                                                          contact.addressbookuid_stg,
                                                                          pp.periodstart_stg                                  AS prty_agmt_strt_dt,
                                                                          coalesce(pcr.effectivedate_stg,pp.periodstart_stg)  AS agmt_feat_strt_dt,
                                                                          coalesce(pcr.expirationdate_stg,pp.periodstart_stg) AS agmt_feat_end_dt,
                                                                          tl4.typecode_stg,
                                                                          pp.updatetime_stg
                                                               FROM       db_t_prod_stag.pc_policyperiod pp
                                                               inner join db_t_prod_stag.pc_policycontactrole pcr
                                                               ON         pp.id_stg = pcr.branchid_stg
                                                               inner join db_t_prod_stag.pctl_policycontactrole tl1
                                                               ON         tl1.id_stg = pcr.subtype_stg
                                                               inner join db_t_prod_stag.pctl_waivedreason_alfa tl2
                                                               ON         pcr.waivedreason_alfa_stg = tl2.id_stg
                                                               inner join db_t_prod_stag.pc_contact contact
                                                               ON         contact.id_stg = pcr.contactdenorm_stg
                                                               inner join db_t_prod_stag.pctl_policyperiodstatus tl3
                                                               ON         tl3.id_stg = pp.status_stg
                                                               inner join db_t_prod_stag.pctl_contact tl4
                                                               ON         tl4.id_stg = contact.subtype_stg
                                                               inner join db_t_prod_stag.pc_job job
                                                               ON         job.id_stg = pp.jobid_stg
                                                               inner join db_t_prod_stag.pctl_job tl
                                                               ON         tl.id_stg = job.subtype_stg
                                                               WHERE      lower(tl.name_stg) IN (''renewal'',
                                                                                                 ''submission'',
                                                                                                 ''policy change'')
                                                               AND        contact.addressbookuid_stg IS NOT NULL
                                                               AND        pp.updatetime_stg > (:start_dttm)
                                                               AND        pp.updatetime_stg <= (:end_dttm)
                                                               /* -------------------------------------------------------------------------------------------------------------------------------------- */
                                                               UNION
                                                               /* ---- endorsements -------------------------------------------------------------------------------------------------------------------- */
                                                               SELECT     job.jobnumber_stg,
                                                                          pp.branchnumber_stg,
                                                                          ''FEAT_SBTYPE15''  AS feat_sbtype_cd,
                                                                          tl3.typecode_stg AS feat_insrnc_sbtype_cd,
                                                                          tl4.typecode_stg,
                                                                          FORM.formpatterncode_stg AS nk_src_key,
                                                                          contact.addressbookuid_stg,
                                                                          pp.periodstart_stg                                   AS prty_agmt_strt_dt,
                                                                          coalesce(FORM.effectivedate_stg,pp.periodstart_stg)  AS agmt_feat_strt_dt,
                                                                          coalesce(FORM.expirationdate_stg,pp.periodstart_stg) AS agmt_feat_end_dt,
                                                                          tl2.typecode_stg,
                                                                          pp.updatetime_stg
                                                               FROM       db_t_prod_stag.pc_policyperiod pp
                                                               inner join db_t_prod_stag.pc_form FORM
                                                               ON         FORM.branchid_stg = pp.id_stg
                                                               inner join db_t_prod_stag.pc_formassociation fa
                                                               ON         fa.form_stg = FORM.id_stg
                                                               inner join db_t_prod_stag.pctl_formassociation tl1
                                                               ON         tl1.id_stg = fa.subtype_stg
                                                               left join  db_t_prod_stag.pc_policycontactrole pcr
                                                               ON         pcr.id_stg = fa.policycontactrole_stg
                                                               left join  db_t_prod_stag.pc_contact contact
                                                               ON         contact.id_stg = pcr.contactdenorm_stg
                                                               inner join db_t_prod_stag.pctl_contact tl2
                                                               ON         tl2.id_stg = contact.subtype_stg
                                                               inner join db_t_prod_stag.pc_formpattern fp
                                                               ON         fp.code_stg = FORM.formpatterncode_stg
                                                               inner join db_t_prod_stag.pctl_documenttype tl3
                                                               ON         tl3.id_stg = fp.documenttype_stg
                                                               inner join db_t_prod_stag.pctl_policycontactrole tl4
                                                               ON         tl4.id_stg = pcr.subtype_stg
                                                               inner join db_t_prod_stag.pctl_policyperiodstatus tl5
                                                               ON         tl5.id_stg= pp.status_stg
                                                               inner join db_t_prod_stag.pc_job job
                                                               ON         job.id_stg = pp.jobid_stg
                                                               inner join db_t_prod_stag.pctl_job tl
                                                               ON         tl.id_stg = job.subtype_stg
                                                               WHERE      lower(tl.name_stg) IN (''renewal'',
                                                                                                 ''submission'',
                                                                                                 ''policy change'')
                                                               AND        contact.addressbookuid_stg IS NOT NULL
                                                               AND        tl3.typecode_stg = ''endorsement_alfa''
                                                               AND        fp.retired_stg = 0
                                                               AND        fa.id_stg IS NOT NULL
                                                               AND        pp.updatetime_stg > (:start_dttm)
                                                               AND        pp.updatetime_stg <= (:end_dttm)
                                                                          /* This above section is from DZ SQ added through optimization task EIM-23175*/
                                                    ) quotn_prty_feat
                                           WHERE    addressbookuid IS NOT NULL
                                                    /*This filter is from DZ mapping EIM-23175*/
                                                    qualify row_number() over(PARTITION BY quotn_prty_feat.jobnumber, quotn_prty_feat.branchnumber, quotn_prty_feat.addressbookuid, quotn_prty_feat.nk_src_key, quotn_prty_feat.feat_sbtype_cd ORDER BY quotn_prty_feat.updatetime DESC) = 1 ) src ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
         SELECT ltrim ( rtrim ( sq_quotn_prty_feat.jobnumber ) ) AS out_jobnumber,
                sq_quotn_prty_feat.branchnumber                  AS branchnumber,
                sq_quotn_prty_feat.feat_sbtype_cd                AS feat_sbtype_cd,
                sq_quotn_prty_feat.feat_insrnc_sbtype_cd         AS feat_insrnc_sbtype_cd,
                sq_quotn_prty_feat.typecode                      AS typecode,
                sq_quotn_prty_feat.nk_src_key                    AS nk_src_key,
                sq_quotn_prty_feat.addressbookuid                AS addressbookuid,
                sq_quotn_prty_feat.prty_quotn_strt_dt            AS prty_quotn_strt_dt,
                sq_quotn_prty_feat.quotn_feat_strt_dt            AS quotn_feat_strt_dt,
                sq_quotn_prty_feat.quotn_feat_end_dt             AS quotn_feat_end_dt,
                sq_quotn_prty_feat.cntct_type_cd                 AS cntct_type_cd,
                sq_quotn_prty_feat.source_record_id
         FROM   sq_quotn_prty_feat );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
            SELECT    exp_pass_from_source.out_jobnumber AS out_jobnumber,
                      exp_pass_from_source.branchnumber  AS branchnumber,
                      lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_FEAT_SBTYPE_CD */
                                                      AS out_feat_sbtype_cd,
                      exp_pass_from_source.nk_src_key AS nk_src_key,
                      lkp_2.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PRTY_QUOTN_ROLE_CD */
                                                                                             AS out_prty_quotn_role_cd,
                      exp_pass_from_source.prty_quotn_strt_dt                                AS prty_quotn_strt_dt,
                      exp_pass_from_source.quotn_feat_strt_dt                                AS quotn_prty_feat_strt_dt,
                      0                                                                      AS overriden_feat_id,
                      to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS out_quotn_pry_feat_end_dt,
                      ''GWPC''                                                                 AS sys_src_cd,
						CASE
							WHEN exp_pass_from_source.cntct_type_cd IN (
								''Person'',
								''Adjudicator'',
								''Vendor (Person)'',
								''Attorney'',
								''Doctor'',
								''Policy Person'',
								''Contact'',
								''PersonVendor'',
								''User Contact''
							) THEN lkp_3.indiv_prty_id   -- replaced lookup LKP_INDIV_CNT_MGR

							WHEN exp_pass_from_source.cntct_type_cd IN (
								''Company'',
								''Vendor (Company)'',
								''Auto Repair Shop'',
								''Auto Towing Agcy'',
								''Law Firm'',
								''Medical Care Organization'',
								''CompanyVendor'',
								''LegalVenue''
							) THEN lkp_4.busn_prty_id     -- replaced lookup LKP_BUSN

							WHEN exp_pass_from_source.cntct_type_cd IN (
								''UserContact''
							) THEN lkp_5.indiv_prty_id    -- replaced lookup LKP_INDIV_CLM_CTR
						END AS v_prty_id,

                      CASE
                                WHEN v_prty_id IS NULL THEN to_char ( CASE
																	WHEN exp_pass_from_source.cntct_type_cd IN (''Company'', ''Vendor (Company)'')
																		THEN lkp_6.busn_prty_id
																END )
                                ELSE v_prty_id
                      END AS prty_id,
                      exp_pass_from_source.source_record_id,
                      row_number() over (PARTITION BY exp_pass_from_source.source_record_id ORDER BY exp_pass_from_source.source_record_id) AS rnk
            FROM      exp_pass_from_source
            left join lkp_teradata_etl_ref_xlat_feat_sbtype_cd lkp_1
            ON        lkp_1.src_idntftn_val = exp_pass_from_source.feat_sbtype_cd
            left join lkp_teradata_etl_ref_xlat_prty_quotn_role_cd lkp_2
            ON        lkp_2.src_idntftn_val = exp_pass_from_source.typecode
            left join lkp_indiv_cnt_mgr lkp_3
            ON        lkp_3.nk_link_id = exp_pass_from_source.addressbookuid
            left join lkp_busn lkp_4
            ON        lkp_4.busn_ctgy_cd = ''CO''
            AND       lkp_4.nk_busn_cd = exp_pass_from_source.addressbookuid
            left join lkp_indiv_clm_ctr lkp_5
            ON        lkp_5.nk_publc_id = exp_pass_from_source.addressbookuid
            left join lkp_busn lkp_6
            ON        lkp_6.busn_ctgy_cd = ''COV''
            AND       lkp_6.nk_busn_cd = exp_pass_from_source.addressbookuid qualify row_number() over (PARTITION BY exp_pass_from_source.source_record_id ORDER BY exp_pass_from_source.source_record_id) 
			= 1 );
  -- Component LKP_INSRNC_QUOTN, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_insrnc_quotn AS
  (
            SELECT    lkp.quotn_id,
                      exp_data_transformation.source_record_id,
                      row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.quotn_id ASC,lkp.reg_prem_prd_cd ASC,lkp.insrnc_type_cd ASC,lkp.pmt_mode_cd ASC,lkp.dvatn_pct ASC,lkp.dscnt_amt ASC,lkp.dscnt_rsn_desc ASC,lkp.prem_amt ASC,lkp.reg_prem_prd_num ASC,lkp.quotn_sbtype_cd ASC,lkp.campn_id ASC,lkp.quotn_sts_type_cd ASC,lkp.quotn_cls_rsn_cd ASC,lkp.quotn_orign_cd ASC,lkp.aplctn_id ASC,lkp.quotn_amt ASC,lkp.cur_quotn_sts_strt_dt ASC,lkp.quotn_opn_dttm ASC,lkp.quotn_expn_dt ASC,lkp.quotn_cls_dttm ASC,lkp.quotn_ceil_amt ASC,lkp.agmt_cury_quotn_amt ASC,lkp.agmt_cury_ceil_amt ASC,lkp.quotn_plnd_agmt_opn_dttm ASC,lkp.quotn_plnd_agmt_cls_dttm ASC,lkp.agmt_objtv_type_cd ASC,lkp.nk_job_nbr ASC,lkp.vers_nbr ASC,lkp.quotn_rtd_ind ASC,lkp.quot_src_txt ASC,lkp.rtd_dttm ASC,lkp.quotn_updt_dttm ASC,lkp.tier_type_cd ASC,lkp.quotn_slctd_ind ASC,lkp.rtd_insrnc_scr_val ASC,lkp.cntnus_srvc_dt ASC,lkp.prior_clm_free_ind ASC,lkp.prior_insrnc_ind ASC,lkp.stmt_cycl_cd ASC,lkp.src_sys_cd ASC,lkp.src_of_busn_cd ASC,lkp.prcs_id ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.trans_strt_dttm ASC,lkp.trans_end_dttm ASC) rnk
            FROM      exp_data_transformation
            left join
                      (
                               SELECT   insrnc_quotn.quotn_id                 AS quotn_id,
                                        insrnc_quotn.reg_prem_prd_cd          AS reg_prem_prd_cd,
                                        insrnc_quotn.insrnc_type_cd           AS insrnc_type_cd,
                                        insrnc_quotn.pmt_mode_cd              AS pmt_mode_cd,
                                        insrnc_quotn.dvatn_pct                AS dvatn_pct,
                                        insrnc_quotn.dscnt_amt                AS dscnt_amt,
                                        insrnc_quotn.dscnt_rsn_desc           AS dscnt_rsn_desc,
                                        insrnc_quotn.prem_amt                 AS prem_amt,
                                        insrnc_quotn.reg_prem_prd_num         AS reg_prem_prd_num,
                                        insrnc_quotn.quotn_sbtype_cd          AS quotn_sbtype_cd,
                                        insrnc_quotn.campn_id                 AS campn_id,
                                        insrnc_quotn.quotn_sts_type_cd        AS quotn_sts_type_cd,
                                        insrnc_quotn.quotn_cls_rsn_cd         AS quotn_cls_rsn_cd,
                                        insrnc_quotn.quotn_orign_cd           AS quotn_orign_cd,
                                        insrnc_quotn.aplctn_id                AS aplctn_id,
                                        insrnc_quotn.quotn_amt                AS quotn_amt,
                                        insrnc_quotn.cur_quotn_sts_strt_dt    AS cur_quotn_sts_strt_dt,
                                        insrnc_quotn.quotn_opn_dttm           AS quotn_opn_dttm,
                                        insrnc_quotn.quotn_expn_dt            AS quotn_expn_dt,
                                        insrnc_quotn.quotn_cls_dttm           AS quotn_cls_dttm,
                                        insrnc_quotn.quotn_ceil_amt           AS quotn_ceil_amt,
                                        insrnc_quotn.agmt_cury_quotn_amt      AS agmt_cury_quotn_amt,
                                        insrnc_quotn.agmt_cury_ceil_amt       AS agmt_cury_ceil_amt,
                                        insrnc_quotn.quotn_plnd_agmt_opn_dttm AS quotn_plnd_agmt_opn_dttm,
                                        insrnc_quotn.quotn_plnd_agmt_cls_dttm AS quotn_plnd_agmt_cls_dttm,
                                        insrnc_quotn.agmt_objtv_type_cd       AS agmt_objtv_type_cd,
                                        insrnc_quotn.quotn_rtd_ind            AS quotn_rtd_ind,
                                        insrnc_quotn.quot_src_txt             AS quot_src_txt,
                                        insrnc_quotn.rtd_dttm                 AS rtd_dttm,
                                        insrnc_quotn.quotn_updt_dttm          AS quotn_updt_dttm,
                                        insrnc_quotn.tier_type_cd             AS tier_type_cd,
                                        insrnc_quotn.quotn_slctd_ind          AS quotn_slctd_ind,
                                        insrnc_quotn.rtd_insrnc_scr_val       AS rtd_insrnc_scr_val,
                                        insrnc_quotn.cntnus_srvc_dt           AS cntnus_srvc_dt,
                                        insrnc_quotn.prior_clm_free_ind       AS prior_clm_free_ind,
                                        insrnc_quotn.prior_insrnc_ind         AS prior_insrnc_ind,
                                        insrnc_quotn.stmt_cycl_cd             AS stmt_cycl_cd,
                                        insrnc_quotn.src_of_busn_cd           AS src_of_busn_cd,
                                        insrnc_quotn.prcs_id                  AS prcs_id,
                                        insrnc_quotn.edw_strt_dttm            AS edw_strt_dttm,
                                        insrnc_quotn.edw_end_dttm             AS edw_end_dttm,
                                        insrnc_quotn.trans_strt_dttm          AS trans_strt_dttm,
                                        insrnc_quotn.trans_end_dttm           AS trans_end_dttm,
                                        insrnc_quotn.nk_job_nbr               AS nk_job_nbr,
                                        insrnc_quotn.vers_nbr                 AS vers_nbr,
                                        insrnc_quotn.src_sys_cd               AS src_sys_cd
                               FROM     db_t_prod_core.insrnc_quotn qualify row_number () over (PARTITION BY nk_job_nbr,vers_nbr ORDER BY edw_end_dttm DESC)=1 ) lkp
            ON        lkp.nk_job_nbr = exp_data_transformation.out_jobnumber
            AND       lkp.vers_nbr = exp_data_transformation.branchnumber
            AND       lkp.src_sys_cd = exp_data_transformation.sys_src_cd qualify row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.quotn_id ASC,lkp.reg_prem_prd_cd ASC,lkp.insrnc_type_cd ASC,lkp.pmt_mode_cd ASC,lkp.dvatn_pct ASC,lkp.dscnt_amt ASC,lkp.dscnt_rsn_desc ASC,lkp.prem_amt ASC,lkp.reg_prem_prd_num ASC,lkp.quotn_sbtype_cd ASC,lkp.campn_id ASC,lkp.quotn_sts_type_cd ASC,lkp.quotn_cls_rsn_cd ASC,lkp.quotn_orign_cd ASC,lkp.aplctn_id ASC,lkp.quotn_amt ASC,lkp.cur_quotn_sts_strt_dt ASC,lkp.quotn_opn_dttm ASC,lkp.quotn_expn_dt ASC,lkp.quotn_cls_dttm ASC,lkp.quotn_ceil_amt ASC,lkp.agmt_cury_quotn_amt ASC,lkp.agmt_cury_ceil_amt ASC,lkp.quotn_plnd_agmt_opn_dttm ASC,lkp.quotn_plnd_agmt_cls_dttm ASC,lkp.agmt_objtv_type_cd ASC,lkp.nk_job_nbr ASC,lkp.vers_nbr ASC,lkp.quotn_rtd_ind ASC,lkp.quot_src_txt ASC,lkp.rtd_dttm ASC,lkp.quotn_updt_dttm ASC,lkp.tier_type_cd ASC,lkp.quotn_slctd_ind ASC,lkp.rtd_insrnc_scr_val ASC,lkp.cntnus_srvc_dt ASC,lkp.prior_clm_free_ind ASC,lkp.prior_insrnc_ind ASC,lkp.stmt_cycl_cd ASC,lkp.src_sys_cd ASC,lkp.src_of_busn_cd ASC,lkp.prcs_id ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.trans_strt_dttm ASC,lkp.trans_end_dttm ASC)  
			= 1 );
  -- Component LKP_FEAT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_feat AS
  (
            SELECT    lkp.feat_id,
                      exp_data_transformation.source_record_id,
                      row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.feat_id DESC,lkp.feat_sbtype_cd DESC,lkp.nk_src_key DESC,lkp.feat_insrnc_sbtype_cd DESC,lkp.feat_clasfcn_cd DESC,lkp.feat_desc DESC,lkp.feat_name DESC,lkp.comn_feat_name DESC,lkp.feat_lvl_sbtype_cnt DESC,lkp.insrnc_cvge_type_cd DESC,lkp.insrnc_lob_type_cd DESC,lkp.prcs_id DESC) rnk
            FROM      exp_data_transformation
            left join
                      (
                               SELECT   feat.feat_id               AS feat_id,
                                        feat.feat_insrnc_sbtype_cd AS feat_insrnc_sbtype_cd,
                                        feat.feat_clasfcn_cd       AS feat_clasfcn_cd,
                                        feat.feat_desc             AS feat_desc,
                                        feat.feat_name             AS feat_name,
                                        feat.comn_feat_name        AS comn_feat_name,
                                        feat.feat_lvl_sbtype_cnt   AS feat_lvl_sbtype_cnt,
                                        feat.insrnc_cvge_type_cd   AS insrnc_cvge_type_cd,
                                        feat.insrnc_lob_type_cd    AS insrnc_lob_type_cd,
                                        feat.prcs_id               AS prcs_id,
                                        feat.feat_sbtype_cd        AS feat_sbtype_cd,
                                        feat.nk_src_key            AS nk_src_key
                               FROM     db_t_prod_core.feat qualify row_number() over(PARTITION BY nk_src_key, feat_sbtype_cd ORDER BY edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.feat_sbtype_cd = exp_data_transformation.out_feat_sbtype_cd
            AND       lkp.nk_src_key = exp_data_transformation.nk_src_key qualify row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.feat_id DESC,lkp.feat_sbtype_cd DESC,lkp.nk_src_key DESC,lkp.feat_insrnc_sbtype_cd DESC,lkp.feat_clasfcn_cd DESC,lkp.feat_desc DESC,lkp.feat_name DESC,lkp.comn_feat_name DESC,lkp.feat_lvl_sbtype_cnt DESC,lkp.insrnc_cvge_type_cd DESC,lkp.insrnc_lob_type_cd DESC,lkp.prcs_id DESC)  
			= 1 );
  -- Component LKP_QUOTN_PRTY_FEAT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_quotn_prty_feat AS
  (
             SELECT     lkp.prty_quotn_role_cd,
                        lkp.prty_quotn_strt_dttm,
                        lkp.quotn_prty_feat_strt_dttm,
                        lkp.overriden_feat_id,
                        lkp.quotn_prty_feat_end_dttm,
                        lkp.edw_end_dttm,
                        lkp_insrnc_quotn.quotn_id                         AS in_quotn_id,
                        exp_data_transformation.out_prty_quotn_role_cd    AS in_prty_quotn_role_cd,
                        exp_data_transformation.prty_quotn_strt_dt        AS in_prty_quotn_strt_dttm,
                        exp_data_transformation.prty_id                   AS in_prty_id,
                        lkp_feat.feat_id                                  AS in_feat_id,
                        exp_data_transformation.quotn_prty_feat_strt_dt   AS in_quotn_prty_feat_strt_dttm,
                        exp_data_transformation.overriden_feat_id         AS in_overriden_feat_id,
                        exp_data_transformation.out_quotn_pry_feat_end_dt AS in_quotn_pry_feat_end_dttm,
                        exp_data_transformation.source_record_id,
                        row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.prty_quotn_role_cd ASC,lkp.prty_quotn_strt_dttm ASC,lkp.quotn_prty_feat_strt_dttm ASC,lkp.overriden_feat_id ASC,lkp.quotn_prty_feat_end_dttm ASC,lkp.edw_end_dttm ASC) rnk
             FROM       exp_data_transformation
             inner join lkp_insrnc_quotn
             ON         exp_data_transformation.source_record_id = lkp_insrnc_quotn.source_record_id
             inner join lkp_feat
             ON         lkp_insrnc_quotn.source_record_id = lkp_feat.source_record_id
             left join
                        (
                                 SELECT   quotn_prty_feat.prty_quotn_role_cd        AS prty_quotn_role_cd,
                                          quotn_prty_feat.prty_quotn_strt_dttm      AS prty_quotn_strt_dttm,
                                          quotn_prty_feat.quotn_prty_feat_strt_dttm AS quotn_prty_feat_strt_dttm,
                                          quotn_prty_feat.overriden_feat_id         AS overriden_feat_id,
                                          quotn_prty_feat.quotn_prty_feat_end_dttm  AS quotn_prty_feat_end_dttm,
                                          quotn_prty_feat.edw_end_dttm              AS edw_end_dttm,
                                          quotn_prty_feat.quotn_id                  AS quotn_id,
                                          quotn_prty_feat.prty_id                   AS prty_id,
                                          quotn_prty_feat.feat_id                   AS feat_id
                                 FROM     db_t_prod_core.quotn_prty_feat qualify row_number() over(PARTITION BY quotn_prty_feat.quotn_id,quotn_prty_feat.prty_id,quotn_prty_feat.prty_id ORDER BY quotn_prty_feat.edw_end_dttm DESC) = 1 ) lkp
             ON         lkp.quotn_id = lkp_insrnc_quotn.quotn_id
             AND        lkp.prty_id = exp_data_transformation.prty_id
             AND        lkp.feat_id = lkp_feat.feat_id qualify row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.prty_quotn_role_cd ASC,lkp.prty_quotn_strt_dttm ASC,lkp.quotn_prty_feat_strt_dttm ASC,lkp.overriden_feat_id ASC,lkp.quotn_prty_feat_end_dttm ASC,lkp.edw_end_dttm ASC)  
			 = 1 );
  -- Component EXPTRANS1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exptrans1 AS
  (
         SELECT lkp_quotn_prty_feat.edw_end_dttm                                       AS lkp_edw_end_dttm,
                lkp_quotn_prty_feat.in_quotn_id                                        AS in_quotn_id,
                lkp_quotn_prty_feat.in_prty_quotn_role_cd                              AS in_prty_quotn_role_cd,
                lkp_quotn_prty_feat.in_prty_quotn_strt_dttm                            AS in_prty_quotn_strt_dttm,
                lkp_quotn_prty_feat.in_prty_id                                         AS in_prty_id,
                lkp_quotn_prty_feat.in_feat_id                                         AS in_feat_id,
                lkp_quotn_prty_feat.in_quotn_prty_feat_strt_dttm                       AS in_quotn_prty_feat_strt_dttm,
                lkp_quotn_prty_feat.in_overriden_feat_id                               AS in_overriden_feat_id,
                lkp_quotn_prty_feat.in_quotn_pry_feat_end_dttm                         AS in_quotn_prty_feat_end_dttm,
                :prcs_id                                                               AS prcs_id,
                current_timestamp                                                      AS edw_strt_dttm,
                to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                md5 ( rtrim ( ltrim ( lkp_quotn_prty_feat.prty_quotn_role_cd ) )
                       || to_char ( lkp_quotn_prty_feat.prty_quotn_strt_dttm , ''MM/DD/YYYY'' )
                       || to_char ( lkp_quotn_prty_feat.quotn_prty_feat_strt_dttm , ''MM/DD/YYYY'' )
                       || rtrim ( ltrim ( lkp_quotn_prty_feat.overriden_feat_id ) )
                       || to_char ( lkp_quotn_prty_feat.quotn_prty_feat_end_dttm ) ) AS lkp_md5,
                md5 ( rtrim ( ltrim ( lkp_quotn_prty_feat.in_prty_quotn_role_cd ) )
                       || to_char ( lkp_quotn_prty_feat.in_prty_quotn_strt_dttm , ''MM/DD/YYYY'' )
                       || to_char ( lkp_quotn_prty_feat.in_quotn_prty_feat_strt_dttm , ''MM/DD/YYYY'' )
                       || rtrim ( ltrim ( lkp_quotn_prty_feat.in_overriden_feat_id ) )
                       || to_char ( lkp_quotn_prty_feat.in_quotn_pry_feat_end_dttm ) ) AS in_md5,
                CASE
                       WHEN lkp_quotn_prty_feat.in_prty_id IS NULL THEN ''R''
                       ELSE
                              CASE
                                     WHEN lkp_md5 IS NULL THEN ''I''
                                     ELSE
                                            CASE
                                                   WHEN lkp_md5 != in_md5 THEN ''U''
                                                   ELSE ''R''
                                            END
                              END
                END AS ins_upd,
                lkp_quotn_prty_feat.source_record_id
         FROM   lkp_quotn_prty_feat );
  -- Component RTRTRANS_INS, Type ROUTER Output Group INS
  create or replace TEMPORARY table RTRTRANS_INS as
  SELECT exptrans1.lkp_edw_end_dttm             AS lkp_edw_end_dttm,
         exptrans1.in_quotn_id                  AS in_quotn_id,
         exptrans1.in_prty_quotn_role_cd        AS in_prty_quotn_role_cd,
         exptrans1.in_prty_quotn_strt_dttm      AS in_prty_quotn_strt_dttm,
         exptrans1.in_prty_id                   AS in_prty_id,
         exptrans1.in_feat_id                   AS in_feat_id,
         exptrans1.in_quotn_prty_feat_strt_dttm AS in_quotn_prty_feat_strt_dttm,
         exptrans1.in_overriden_feat_id         AS in_overriden_feat_id,
         exptrans1.in_quotn_prty_feat_end_dttm  AS in_quotn_prty_feat_end_dttm,
         exptrans1.prcs_id                      AS prcs_id,
         exptrans1.edw_strt_dttm                AS edw_strt_dttm,
         exptrans1.edw_end_dttm                 AS edw_end_dttm,
         exptrans1.ins_upd                      AS ins_upd,
         exptrans1.source_record_id
  FROM   exptrans1
  WHERE  (
                exptrans1.ins_upd = ''I'' )
  OR     (
                exptrans1.ins_upd = ''U''
         AND    exptrans1.lkp_edw_end_dttm = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  
  -- Component UPDTRANS, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE updtrans AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtrtrans_ins.in_quotn_id                  AS quotn_id,
                rtrtrans_ins.in_prty_quotn_role_cd        AS prty_quotn_role_cd,
                rtrtrans_ins.in_prty_quotn_strt_dttm      AS prty_quotn_strt_dttm,
                rtrtrans_ins.in_prty_id                   AS prty_id,
                rtrtrans_ins.in_feat_id                   AS feat_id,
                rtrtrans_ins.in_quotn_prty_feat_strt_dttm AS quotn_prty_feat_strt_dttm,
                rtrtrans_ins.in_overriden_feat_id         AS overriden_feat_id,
                rtrtrans_ins.in_quotn_prty_feat_end_dttm  AS quotn_prty_feat_end_dttm,
                rtrtrans_ins.prcs_id                      AS prcs_id,
                rtrtrans_ins.edw_strt_dttm                AS edw_strt_dttm,
                rtrtrans_ins.edw_end_dttm                 AS edw_end_dttm,
				source_record_id
         FROM   rtrtrans_ins );
  -- Component EXPTRANS, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exptrans AS
  (
         SELECT updtrans.quotn_id                  AS quotn_id,
                updtrans.prty_quotn_role_cd        AS prty_quotn_role_cd,
                updtrans.prty_quotn_strt_dttm      AS prty_quotn_strt_dttm,
                updtrans.prty_id                   AS prty_id,
                updtrans.feat_id                   AS feat_id,
                updtrans.quotn_prty_feat_strt_dttm AS quotn_prty_feat_strt_dttm,
                updtrans.overriden_feat_id         AS overriden_feat_id,
                updtrans.quotn_prty_feat_end_dttm  AS quotn_prty_feat_end_dttm,
                updtrans.prcs_id                   AS prcs_id,
                updtrans.edw_strt_dttm             AS edw_strt_dttm,
                updtrans.edw_end_dttm              AS edw_end_dttm,
                updtrans.source_record_id
         FROM   updtrans );
  -- Component QUOTN_PRTY_FEAT_ins, Type TARGET
  INSERT INTO db_t_prod_core.quotn_prty_feat
              (
                          quotn_id,
                          prty_quotn_role_cd,
                          prty_quotn_strt_dttm,
                          prty_id,
                          feat_id,
                          quotn_prty_feat_strt_dttm,
                          overriden_feat_id,
                          quotn_prty_feat_end_dttm,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exptrans.quotn_id                  AS quotn_id,
         exptrans.prty_quotn_role_cd        AS prty_quotn_role_cd,
         exptrans.prty_quotn_strt_dttm      AS prty_quotn_strt_dttm,
         exptrans.prty_id                   AS prty_id,
         exptrans.feat_id                   AS feat_id,
         exptrans.quotn_prty_feat_strt_dttm AS quotn_prty_feat_strt_dttm,
         exptrans.overriden_feat_id         AS overriden_feat_id,
         exptrans.quotn_prty_feat_end_dttm  AS quotn_prty_feat_end_dttm,
         exptrans.prcs_id                   AS prcs_id,
         exptrans.edw_strt_dttm             AS edw_strt_dttm,
         exptrans.edw_end_dttm              AS edw_end_dttm
  FROM   exptrans
  WHERE quotn_id IS NOT NULL AND PRTY_QUOTN_ROLE_CD  IS NOT NULL;
  
  -- Component QUOTN_PRTY_FEAT_ins, Type Post SQL
  UPDATE db_t_prod_core.quotn_prty_feat
    SET    edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT quotn_id,
                                         prty_id,
                                         feat_id,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY quotn_id,prty_id,feat_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.quotn_prty_feat ) a

  WHERE  quotn_prty_feat.edw_strt_dttm = a.edw_strt_dttm
  AND    quotn_prty_feat.quotn_id=a.quotn_id
  AND    quotn_prty_feat.prty_id=a.prty_id
  AND    quotn_prty_feat.feat_id=a.feat_id
  AND    lead1 IS NOT NULL
  AND quotn_prty_feat.quotn_id IS NOT NULL AND a.quotn_id IS NOT NULL;

END;
';