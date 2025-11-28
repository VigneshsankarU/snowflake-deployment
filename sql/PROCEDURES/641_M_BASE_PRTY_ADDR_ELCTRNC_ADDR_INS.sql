-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_ADDR_ELCTRNC_ADDR_INS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  PRCS_ID STRING;
  START_DTTM TIMESTAMP;
  END_DTTM TIMESTAMP;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);
  END_DTTM := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
  PRCS_ID := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);
  START_DTTM := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);
  
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
  -- Component LKP_ELCTRN_ADDR, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_elctrn_addr AS
  (
           SELECT   elctrnc_addr.elctrnc_addr_id  AS elctrnc_addr_id,
                    elctrnc_addr.elctrnc_addr_txt AS elctrnc_addr_txt
           FROM     db_t_prod_core.elctrnc_addr qualify row_number() over(PARTITION BY elctrnc_addr_txt ORDER BY edw_end_dttm DESC) = 1 );
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
         SELECT indiv.indiv_prty_id AS indiv_prty_id,
                indiv.nk_link_id    AS nk_link_id
         FROM   db_t_prod_core.indiv
         WHERE  indiv.nk_publc_id IS NULL );
  -- Component LKP_TERADATA_ETL_REF_XLAT, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRTY_ADDR_USGE_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_busn_ctgy_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm IN (''BUSN_CTGY'',
                                                         ''ORG_TYPE'',
                                                         ''PRTY_TYPE'')
         AND    teradata_etl_ref_xlat.src_idntftn_nm IN (''derived'',
                                                         ''cctl_contact.typecode'',
                                                         ''cctl_contact.name'')
         AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''DS'',
                                                          ''GW'')
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component sq_pc_contact, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_contact AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS emailaddress,
                $2  AS src_idntftn_val,
                $3  AS addressbookuid,
                $4  AS source,
                $5  AS tl_cnt_name,
                $6  AS updatetime,
                $7  AS createtime,
                $8  AS retired,
                $9  AS rnk,
                $10 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   emailaddress,
                                                    src_idntftn_val,
                                                    addressbookuid,
                                                    source,
                                                    tl_cnt_name,
                                                    updatetime,
                                                    createtime,
                                                    retired,
                                                    rank() over( PARTITION BY src_idntftn_val, addressbookuid, source, tl_cnt_name ORDER BY updatetime, createtime) AS rnk
                                                    /*  Added as a part of ticket EIM-47592 */
                                           FROM     (
                                                           SELECT pc_contact.emailaddress1 AS emailaddress,
                                                                  ''PRTY_ADDR_USGE_TYPE3''   AS src_idntftn_val,
                                                                  addressbookuid,
                                                                  ''ContactManager''  AS source,
                                                                  pctl_contact.name AS tl_cnt_name,
                                                                  pc_contact.updatetime,
                                                                  pc_contact.createtime,
                                                                  pc_contact.retired
                                                           FROM  (
                                                                         SELECT pc_contact.updatetime_stg     AS updatetime,
                                                                                pc_contact.retired_stg        AS retired,
                                                                                pc_contact.createtime_stg     AS createtime,
                                                                                pc_contact.addressbookuid_stg AS addressbookuid,
                                                                                pc_contact.emailaddress1_stg  AS emailaddress1,
                                                                                pc_contact.subtype_stg        AS SUBTYPE
                                                                         FROM   db_t_prod_stag.pc_contact
                                                                         WHERE  pc_contact.updatetime_stg> (:start_dttm)
                                                                         AND    pc_contact.updatetime_stg <= (:end_dttm) ) pc_contact
                                                           join
                                                                  (
                                                                         SELECT pctl_contact.name_stg AS name,
                                                                                pctl_contact.id_stg   AS id
                                                                         FROM   db_t_prod_stag.pctl_contact) pctl_contact
                                                           ON     pc_contact.SUBTYPE = pctl_contact.id
                                                           WHERE  tl_cnt_name IN (''Person'',
                                                                                  ''Adjudicator'',
                                                                                  ''User Contact'',
                                                                                  ''Vendor (Person)'',
                                                                                  ''Attorney'',
                                                                                  ''Doctor'',
                                                                                  ''Policy Person'',
                                                                                  ''Contact'',
                                                                                  ''Company'',
                                                                                  ''CompanyVendor'',
                                                                                  ''AutoRepairShop'',
                                                                                  ''AutoTowingAgcy'',
                                                                                  ''LawFirm'',
                                                                                  ''MedicalCareOrg'')
                                                           UNION
                                                           SELECT cc_contact.emailaddress1 AS emailaddress,
                                                                  ''PRTY_ADDR_USGE_TYPE3''   AS src_idntftn_val,
                                                                  publicid,
                                                                  ''ClaimCenter''     AS source,
                                                                  cctl_contact.name AS tl_cnt_name,
                                                                  cc_contact.updatetime,
                                                                  cc_contact.createtime,
                                                                  cc_contact.retired
                                                           FROM  (
                                                                         SELECT cc_contact.updatetime_stg    AS updatetime,
                                                                                cc_contact.publicid_stg      AS publicid,
                                                                                cc_contact.emailaddress1_stg AS emailaddress1,
                                                                                cc_contact.subtype_stg       AS SUBTYPE,
                                                                                cc_contact.retired_stg       AS retired,
                                                                                cc_contact.createtime_stg    AS createtime
                                                                         FROM   db_t_prod_stag.cc_contact
                                                                         WHERE  cc_contact.updatetime_stg > (:start_dttm)
                                                                         AND    cc_contact.updatetime_stg <= (:end_dttm) ) cc_contact
                                                           join
                                                                  (
                                                                         SELECT cctl_contact.name_stg AS name,
                                                                                cctl_contact.id_stg   AS id
                                                                         FROM   db_t_prod_stag.cctl_contact) cctl_contact
                                                           ON     cc_contact.SUBTYPE = cctl_contact.id
                                                           WHERE  tl_cnt_name IN (''Person'',
                                                                                  ''Adjudicator'',
                                                                                  ''User Contact'',
                                                                                  ''Vendor (Person)'',
                                                                                  ''Attorney'',
                                                                                  ''Doctor'',
                                                                                  ''Policy Person'',
                                                                                  ''Contact'',
                                                                                  ''Company'',
                                                                                  ''CompanyVendor'',
                                                                                  ''AutoRepairShop'',
                                                                                  ''AutoTowingAgcy'',
                                                                                  ''LawFirm'',
                                                                                  ''MedicalCareOrg'')
                                                           UNION
                                                           SELECT pc_contact.emailaddress2 AS emailaddress,
                                                                  ''PRTY_ADDR_USGE_TYPE5''   AS src_idntftn_val,
                                                                  addressbookuid,
                                                                  ''ContactManager''  AS source,
                                                                  pctl_contact.name AS tl_cnt_name,
                                                                  pc_contact.updatetime,
                                                                  pc_contact.createtime,
                                                                  pc_contact.retired
                                                           FROM  (
                                                                         SELECT pc_contact.updatetime_stg     AS updatetime,
                                                                                pc_contact.retired_stg        AS retired,
                                                                                pc_contact.createtime_stg     AS createtime,
                                                                                pc_contact.addressbookuid_stg AS addressbookuid,
                                                                                pc_contact.emailaddress2_stg  AS emailaddress2,
                                                                                pc_contact.subtype_stg        AS SUBTYPE
                                                                         FROM   db_t_prod_stag.pc_contact
                                                                         WHERE  pc_contact.updatetime_stg> (:start_dttm)
                                                                         AND    pc_contact.updatetime_stg <= (:end_dttm) ) pc_contact
                                                           join
                                                                  (
                                                                         SELECT pctl_contact.name_stg AS name,
                                                                                pctl_contact.id_stg   AS id
                                                                         FROM   db_t_prod_stag.pctl_contact) pctl_contact
                                                           ON     pc_contact.SUBTYPE = pctl_contact.id
                                                           WHERE  tl_cnt_name IN (''Person'',
                                                                                  ''Adjudicator'',
                                                                                  ''User Contact'',
                                                                                  ''Vendor (Person)'',
                                                                                  ''Attorney'',
                                                                                  ''Doctor'',
                                                                                  ''Policy Person'',
                                                                                  ''Contact'',
                                                                                  ''Company'',
                                                                                  ''CompanyVendor'',
                                                                                  ''AutoRepairShop'',
                                                                                  ''AutoTowingAgcy'',
                                                                                  ''LawFirm'',
                                                                                  ''MedicalCareOrg'')
                                                           UNION
                                                           SELECT cc_contact.emailaddress2 AS emailaddress,
                                                                  ''PRTY_ADDR_USGE_TYPE5''   AS src_idntftn_val,
                                                                  publicid,
                                                                  ''ClaimCenter''     AS source,
                                                                  cctl_contact.name AS tl_cnt_name,
                                                                  cc_contact.updatetime,
                                                                  cc_contact.createtime,
                                                                  cc_contact.retired
                                                           FROM  (
                                                                         SELECT cc_contact.updatetime_stg    AS updatetime,
                                                                                cc_contact.publicid_stg      AS publicid,
                                                                                cc_contact.emailaddress2_stg AS emailaddress2,
                                                                                cc_contact.subtype_stg       AS SUBTYPE,
                                                                                cc_contact.retired_stg       AS retired,
                                                                                cc_contact.createtime_stg    AS createtime
                                                                         FROM   db_t_prod_stag.cc_contact
                                                                         WHERE  cc_contact.updatetime_stg > (:start_dttm)
                                                                         AND    cc_contact.updatetime_stg <= (:end_dttm) ) cc_contact
                                                           join
                                                                  (
                                                                         SELECT cctl_contact.name_stg AS name,
                                                                                cctl_contact.id_stg   AS id
                                                                         FROM   db_t_prod_stag.cctl_contact) cctl_contact
                                                           ON     cc_contact.SUBTYPE = cctl_contact.id
                                                           WHERE  tl_cnt_name IN (''Person'',
                                                                                  ''Adjudicator'',
                                                                                  ''User Contact'',
                                                                                  ''Vendor (Person)'',
                                                                                  ''Attorney'',
                                                                                  ''Doctor'',
                                                                                  ''Policy Person'',
                                                                                  ''Contact'',
                                                                                  ''Company'',
                                                                                  ''CompanyVendor'',
                                                                                  ''AutoRepairShop'',
                                                                                  ''AutoTowingAgcy'',
                                                                                  ''LawFirm'',
                                                                                  ''MedicalCareOrg'') ) AS src
                                           WHERE    emailaddress IS NOT NULL
                                           AND      retired = 0
                                                    /* Temporary Condition for SIT failure and it needs to be removed*/
                                                    qualify row_number() over( PARTITION BY emailaddress COLLATE ''en-ci'' , src_idntftn_val, addressbookuid, source, tl_cnt_name ORDER BY updatetime DESC, createtime DESC) = 1
                                           ORDER BY updatetime ,
                                                    createtime ) src ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
            SELECT    decode ( TRUE ,
                              lkp_1.tgt_idntftn_val
                              /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */
                              IS NOT NULL , lkp_2.tgt_idntftn_val
                              /* replaced lookup LKP_TERADATA_ETL_REF_XLAT */
                              ,
                              ''UNK'' )                                                        AS var_prty_addr_usge_type_cd,
                      var_prty_addr_usge_type_cd                                             AS out_prty_addr_usge_type_cd1,
                      current_timestamp                                                      AS var_prty_addr_strt_dt,
                      TO_TIMESTAMP_NTZ ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_prty_addr_end_dt,
                      decode ( TRUE ,
                              lkp_3.elctrnc_addr_id
                              /* replaced lookup LKP_ELCTRN_ADDR */
                              IS NOT NULL , lkp_4.elctrnc_addr_id
                              /* replaced lookup LKP_ELCTRN_ADDR */
                              ,
                              9999 ) AS var_loc_id,
                      var_loc_id     AS out_loc_id,
                      decode ( TRUE ,
                              lkp_5.tgt_idntftn_val
                              /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY_CD */
                              IS NOT NULL , lkp_6.tgt_idntftn_val
                              /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY_CD */
                              ,
                              ''UNK'' ) AS v_busn_ctgy_cd,
                      decode ( TRUE ,
                              sq_pc_contact.source = ''ContactManager''
                    AND     sq_pc_contact.tl_cnt_name  IN (
                                  ''Person'' ,
                                  ''Adjudicator'' ,
                                  ''User Contact'' ,
                                  ''UserContact'' ,
                                  ''Vendor (Person)'' ,
                                  ''Attorney'' ,
                                  ''Doctor'' ,
                                  ''Policy Person'' ,
                                  ''Contact'' ) , lkp_7.indiv_prty_id
                              /* replaced lookup LKP_INDIV_CNT_MGR */
                              ,
                              decode ( TRUE ,
                                      sq_pc_contact.source = ''ClaimCenter''
                            AND    sq_pc_contact.tl_cnt_name    IN ( 
                                          ''Person'' ,
                                          ''Adjudicator'' ,
                                          ''User Contact'' ,
                                          ''UserContact'' ,
                                          ''Vendor (Person)'' ,
                                          ''Attorney'' ,
                                          ''Doctor'' ,
                                          ''Policy Person'' ,
                                          ''Contact'' ) , lkp_8.indiv_prty_id
                                      /* replaced lookup LKP_INDIV_CLM_CTR */
                                      ,
                                      decode ( TRUE ,
                                              sq_pc_contact.source = ''ContactManager''
                                    AND     sq_pc_contact.tl_cnt_name   IN ( 
                                                  ''Company'' ,
                                                  ''CompanyVendor'' ,
                                                  ''AutoRepairShop'' ,
                                                  ''AutoTowingAgcy'' ,
                                                  ''LawFirm'' ,
                                                  ''MedicalCareOrg'' ) , lkp_9.busn_prty_id
                                              /* replaced lookup LKP_BUSN */
                                              ,
                                              decode ( TRUE ,
                                                      sq_pc_contact.source = ''ClaimCenter''
                                            AND    sq_pc_contact.tl_cnt_name   IN (  
                                                          ''Company'' ,
                                                          ''CompanyVendor'' ,
                                                          ''AutoRepairShop'' ,
                                                          ''AutoTowingAgcy'' ,
                                                          ''LawFirm'' ,
                                                          ''MedicalCareOrg'' ) , lkp_10.busn_prty_id
                                                      /* replaced lookup LKP_BUSN */
                                                      ) ) ) ) AS var_prty_id,
                      decode ( TRUE ,
                              var_prty_id IS NOT NULL , var_prty_id ,
                              9999 ) AS out_prty_id,
                      :prcs_id       AS prcs_id,
                      CASE
                                WHEN sq_pc_contact.updatetime IS NULL THEN to_date ( ''01/01/1900'' , ''MM/DD/YYYY'' )
                                ELSE sq_pc_contact.updatetime
                      END AS out_updatetime,
                      CASE
                                WHEN sq_pc_contact.createtime IS NULL THEN to_date ( ''01/01/1900'' , ''MM/DD/YYYY'' )
                                ELSE sq_pc_contact.createtime
                      END                   AS out_createtime,
                      sq_pc_contact.retired AS retired,
                      --sq_pc_contact.rnk     AS rnk,
                      sq_pc_contact.source_record_id,
                      row_number() over (PARTITION BY sq_pc_contact.source_record_id ORDER BY sq_pc_contact.source_record_id) AS rnk
            FROM      sq_pc_contact
            left join lkp_teradata_etl_ref_xlat lkp_1
            ON        lkp_1.src_idntftn_val = sq_pc_contact.src_idntftn_val
            left join lkp_teradata_etl_ref_xlat lkp_2
            ON        lkp_2.src_idntftn_val = sq_pc_contact.src_idntftn_val
            left join lkp_elctrn_addr lkp_3
            ON        lkp_3.elctrnc_addr_txt = sq_pc_contact.emailaddress
            left join lkp_elctrn_addr lkp_4
            ON        lkp_4.elctrnc_addr_txt = sq_pc_contact.emailaddress
            left join lkp_teradata_etl_ref_xlat_busn_ctgy_cd lkp_5
            ON        lkp_5.src_idntftn_val = sq_pc_contact.tl_cnt_name
            left join lkp_teradata_etl_ref_xlat_busn_ctgy_cd lkp_6
            ON        lkp_6.src_idntftn_val = sq_pc_contact.tl_cnt_name
            left join lkp_indiv_cnt_mgr lkp_7
            ON        lkp_7.nk_link_id = sq_pc_contact.addressbookuid
            left join lkp_indiv_clm_ctr lkp_8
            ON        lkp_8.nk_publc_id = sq_pc_contact.addressbookuid
            left join lkp_busn lkp_9
            ON        lkp_9.busn_ctgy_cd = v_busn_ctgy_cd
            AND       lkp_9.nk_busn_cd = sq_pc_contact.addressbookuid
            left join lkp_busn lkp_10
            ON        lkp_10.busn_ctgy_cd = v_busn_ctgy_cd
            AND       lkp_10.nk_busn_cd = sq_pc_contact.addressbookuid qualify rnk = 1 );
  -- Component LKP_PRTY_ADDR, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_prty_addr AS
  (
            SELECT    lkp.prty_addr_usge_type_cd,
                      lkp.prty_addr_strt_dttm,
                      lkp.loc_id,
                      lkp.prty_id,
                      lkp.prty_addr_end_dttm,
                      lkp.edw_strt_dttm,
                      lkp.edw_end_dttm,
                      exp_pass_from_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.prty_addr_usge_type_cd ASC,lkp.prty_addr_strt_dttm ASC,lkp.loc_id ASC,lkp.prty_id ASC,lkp.prty_addr_end_dttm ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) rnk
            FROM      exp_pass_from_source
            left join
                      (
                               SELECT   prty_addr.prty_addr_strt_dttm    AS prty_addr_strt_dttm,
                                        prty_addr.prty_addr_end_dttm     AS prty_addr_end_dttm,
                                        prty_addr.edw_strt_dttm          AS edw_strt_dttm,
                                        prty_addr.edw_end_dttm           AS edw_end_dttm,
                                        prty_addr.prty_addr_usge_type_cd AS prty_addr_usge_type_cd,
                                        prty_addr.loc_id                 AS loc_id,
                                        prty_addr.prty_id                AS prty_id
                               FROM     db_t_prod_core.prty_addr qualify row_number() over( PARTITION BY prty_addr_usge_type_cd,prty_id ORDER BY edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.prty_addr_usge_type_cd = exp_pass_from_source.out_prty_addr_usge_type_cd1
            AND       lkp.prty_id = exp_pass_from_source.out_prty_id qualify rnk = 1 );
  -- Component ecp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE ecp_data_transformation AS
  (
             SELECT     lkp_prty_addr.prty_addr_usge_type_cd             AS lkp_prty_addr_usge_type_cd,
                        lkp_prty_addr.prty_addr_strt_dttm                AS lkp_prty_addr_strt_dttm,
                        lkp_prty_addr.loc_id                             AS lkp_loc_id,
                        lkp_prty_addr.prty_id                            AS lkp_prty_id,
                        lkp_prty_addr.prty_addr_end_dttm                 AS lkp_prty_addr_end_dttm,
                        lkp_prty_addr.edw_strt_dttm                      AS lkp_edw_strt_dttm,
                        lkp_prty_addr.edw_end_dttm                       AS lkp_edw_end_dttm,
                        exp_pass_from_source.out_prty_addr_usge_type_cd1 AS out_prty_addr_usge_type_cd1,
                        exp_pass_from_source.out_loc_id                  AS out_loc_id,
                        exp_pass_from_source.out_prty_id                 AS out_prty_id,
                        exp_pass_from_source.out_createtime              AS prty_addr_strt_dttm,
                        exp_pass_from_source.out_prty_addr_end_dt        AS prty_addr_end_dttm,
                        exp_pass_from_source.out_updatetime              AS prty_addr_trans_dttm,
                        exp_pass_from_source.prcs_id                     AS prcs_id,
                        exp_pass_from_source.retired                     AS retired,
                        md5 ( ltrim ( rtrim ( lkp_prty_addr.prty_addr_strt_dttm ) )
                                   || ltrim ( rtrim ( lkp_prty_addr.prty_addr_end_dttm ) )
                                   || ltrim ( rtrim ( lkp_prty_addr.loc_id ) ) ) AS var_chksum_lkp,
                        md5 ( ltrim ( rtrim ( exp_pass_from_source.out_createtime ) )
                                   || ltrim ( rtrim ( exp_pass_from_source.out_prty_addr_end_dt ) )
                                   || ltrim ( rtrim ( exp_pass_from_source.out_loc_id ) ) ) AS var_chksum_inp,
                        CASE
                                   WHEN var_chksum_lkp IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN var_chksum_lkp != var_chksum_inp THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                                                                                 AS ou_ins_upd,
                        exp_pass_from_source.rnk                                                            AS rnk,
                        dateadd (second, ( 2 * ( exp_pass_from_source.rnk - 1 ) ), current_timestamp  ) AS out_edw_strt_dttm,
                        exp_pass_from_source.source_record_id
             FROM       exp_pass_from_source
             inner join lkp_prty_addr
             ON         exp_pass_from_source.source_record_id = lkp_prty_addr.source_record_id );
  -- Component rtr_insert_update_flg_insert, Type ROUTER Output Group insert
  create or replace temporary table rtr_insert_update_flg_insert as
  SELECT ecp_data_transformation.out_prty_addr_usge_type_cd1 AS out_prty_addr_usge_type_cd1,
         ecp_data_transformation.prty_addr_strt_dttm         AS out_prty_addr_strt_dt,
         ecp_data_transformation.out_loc_id                  AS out_loc_id,
         ecp_data_transformation.out_prty_id                 AS out_prty_id,
         ecp_data_transformation.prcs_id                     AS prcs_id,
         ecp_data_transformation.ou_ins_upd                  AS out_flag,
         ecp_data_transformation.prty_addr_end_dttm          AS out_prty_addr_end_dt,
         ecp_data_transformation.prty_addr_trans_dttm        AS updatetime,
         ecp_data_transformation.out_edw_strt_dttm           AS out_edw_strt_dttm,
         ecp_data_transformation.retired                     AS retired,
         ecp_data_transformation.lkp_prty_addr_usge_type_cd  AS lkp_prty_addr_usge_type_cd,
         ecp_data_transformation.lkp_prty_addr_strt_dttm     AS lkp_prty_addr_strt_dttm,
         ecp_data_transformation.lkp_loc_id                  AS lkp_loc_id,
         ecp_data_transformation.lkp_prty_id                 AS lkp_prty_id,
         ecp_data_transformation.lkp_prty_addr_end_dttm      AS lkp_prty_addr_end_dttm,
         ecp_data_transformation.lkp_edw_strt_dttm           AS lkp_edw_strt_dttm,
         ecp_data_transformation.lkp_edw_end_dttm            AS lkp_edw_end_dttm,
         ecp_data_transformation.source_record_id
  FROM   ecp_data_transformation
  WHERE  (
                ecp_data_transformation.ou_ins_upd = ''I''
         AND    ecp_data_transformation.out_prty_id <> 9999
         AND    ecp_data_transformation.out_prty_id IS NOT NULL )
  OR     (
                ecp_data_transformation.retired = 0
         AND    ecp_data_transformation.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
         AND    ecp_data_transformation.out_prty_id <> 9999
         AND    ecp_data_transformation.out_prty_id IS NOT NULL );
  
  -- Component rtr_insert_update_flg_retired, Type ROUTER Output Group retired
  create or replace temporary table rtr_insert_update_flg_retired as
  SELECT ecp_data_transformation.out_prty_addr_usge_type_cd1 AS out_prty_addr_usge_type_cd1,
         ecp_data_transformation.prty_addr_strt_dttm         AS out_prty_addr_strt_dt,
         ecp_data_transformation.out_loc_id                  AS out_loc_id,
         ecp_data_transformation.out_prty_id                 AS out_prty_id,
         ecp_data_transformation.prcs_id                     AS prcs_id,
         ecp_data_transformation.ou_ins_upd                  AS out_flag,
         ecp_data_transformation.prty_addr_end_dttm          AS out_prty_addr_end_dt,
         ecp_data_transformation.prty_addr_trans_dttm        AS updatetime,
         ecp_data_transformation.out_edw_strt_dttm           AS out_edw_strt_dttm,
         ecp_data_transformation.retired                     AS retired,
         ecp_data_transformation.lkp_prty_addr_usge_type_cd  AS lkp_prty_addr_usge_type_cd,
         ecp_data_transformation.lkp_prty_addr_strt_dttm     AS lkp_prty_addr_strt_dttm,
         ecp_data_transformation.lkp_loc_id                  AS lkp_loc_id,
         ecp_data_transformation.lkp_prty_id                 AS lkp_prty_id,
         ecp_data_transformation.lkp_prty_addr_end_dttm      AS lkp_prty_addr_end_dttm,
         ecp_data_transformation.lkp_edw_strt_dttm           AS lkp_edw_strt_dttm,
         ecp_data_transformation.lkp_edw_end_dttm            AS lkp_edw_end_dttm,
         ecp_data_transformation.source_record_id
  FROM   ecp_data_transformation
  WHERE  ecp_data_transformation.ou_ins_upd = ''R''
  AND    ecp_data_transformation.retired != 0
  AND    ecp_data_transformation.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
  AND    ecp_data_transformation.out_prty_id IS NOT NULL;
  
  -- Component rtr_insert_update_flg_update, Type ROUTER Output Group update
  create or replace temporary table rtr_insert_update_flg_update as
  SELECT ecp_data_transformation.out_prty_addr_usge_type_cd1 AS out_prty_addr_usge_type_cd1,
         ecp_data_transformation.prty_addr_strt_dttm         AS out_prty_addr_strt_dt,
         ecp_data_transformation.out_loc_id                  AS out_loc_id,
         ecp_data_transformation.out_prty_id                 AS out_prty_id,
         ecp_data_transformation.prcs_id                     AS prcs_id,
         ecp_data_transformation.ou_ins_upd                  AS out_flag,
         ecp_data_transformation.prty_addr_end_dttm          AS out_prty_addr_end_dt,
         ecp_data_transformation.prty_addr_trans_dttm        AS updatetime,
         ecp_data_transformation.out_edw_strt_dttm           AS out_edw_strt_dttm,
         ecp_data_transformation.retired                     AS retired,
         ecp_data_transformation.lkp_prty_addr_usge_type_cd  AS lkp_prty_addr_usge_type_cd,
         ecp_data_transformation.lkp_prty_addr_strt_dttm     AS lkp_prty_addr_strt_dttm,
         ecp_data_transformation.lkp_loc_id                  AS lkp_loc_id,
         ecp_data_transformation.lkp_prty_id                 AS lkp_prty_id,
         ecp_data_transformation.lkp_prty_addr_end_dttm      AS lkp_prty_addr_end_dttm,
         ecp_data_transformation.lkp_edw_strt_dttm           AS lkp_edw_strt_dttm,
         ecp_data_transformation.lkp_edw_end_dttm            AS lkp_edw_end_dttm,
         ecp_data_transformation.source_record_id
  FROM   ecp_data_transformation
  WHERE  ecp_data_transformation.ou_ins_upd = ''U''
  AND    ecp_data_transformation.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
  AND    ecp_data_transformation.out_prty_id IS NOT NULL;
  
  -- Component exp_pass_to_target_insupd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_insupd AS
  (
         SELECT rtr_insert_update_flg_update.out_prty_addr_usge_type_cd1 AS out_prty_addr_usge_type_cd11,
                rtr_insert_update_flg_update.out_prty_addr_strt_dt       AS out_prty_addr_strt_dt1,
                rtr_insert_update_flg_update.out_loc_id                  AS out_loc_id1,
                rtr_insert_update_flg_update.out_prty_id                 AS out_prty_id1,
                rtr_insert_update_flg_update.prcs_id                     AS prcs_id1,
                rtr_insert_update_flg_update.out_prty_addr_end_dt        AS out_prty_addr_end_dt1,
                rtr_insert_update_flg_update.updatetime                  AS trans_strt_dttm1,
                rtr_insert_update_flg_update.out_edw_strt_dttm           AS edw_strt_dttm1,
                CASE
                       WHEN rtr_insert_update_flg_update.retired = 0 THEN to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                       ELSE rtr_insert_update_flg_update.out_edw_strt_dttm
                END AS out_edw_end_dttm1,
                CASE
                       WHEN rtr_insert_update_flg_update.retired != 0 THEN rtr_insert_update_flg_update.updatetime
                       ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END AS out_trans_end_dttm1,
                rtr_insert_update_flg_update.source_record_id
         FROM   rtr_insert_update_flg_update );
  -- Component upd_prty_addr_retire, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_prty_addr_retire AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insert_update_flg_retired.lkp_prty_addr_usge_type_cd AS lkp_prty_addr_usge_type_cd3,
                rtr_insert_update_flg_retired.lkp_prty_addr_strt_dttm    AS lkp_prty_addr_strt_dttm3,
                rtr_insert_update_flg_retired.lkp_loc_id                 AS lkp_loc_id3,
                rtr_insert_update_flg_retired.lkp_prty_id                AS lkp_prty_id3,
                rtr_insert_update_flg_retired.lkp_edw_strt_dttm          AS lkp_edw_strt_dttm3,
                rtr_insert_update_flg_retired.out_edw_strt_dttm          AS out_edw_strt_dttm3,
                rtr_insert_update_flg_retired.updatetime                 AS trans_strt_dttm3,
                1                                                        AS update_strategy_action,
				rtr_insert_update_flg_retired.source_record_id
         FROM   rtr_insert_update_flg_retired );
  -- Component exp_pass_to_target_retire, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_retire AS
  (
         SELECT upd_prty_addr_retire.lkp_prty_addr_usge_type_cd3                     AS lkp_prty_addr_usge_type_cd3,
                upd_prty_addr_retire.lkp_prty_id3                                    AS lkp_prty_id3,
                upd_prty_addr_retire.lkp_edw_strt_dttm3                              AS lkp_edw_strt_dttm3,
                dateadd ( second, -1, upd_prty_addr_retire.out_edw_strt_dttm3  ) AS out_edw_end_dttm3,
                dateadd (second, -1,  upd_prty_addr_retire.trans_strt_dttm3  ) AS out_trans_end_dttm3,
                upd_prty_addr_retire.source_record_id
         FROM   upd_prty_addr_retire );
  -- Component exp_pass_to_target, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target AS
  (
         SELECT rtr_insert_update_flg_insert.out_prty_addr_usge_type_cd1 AS out_prty_addr_usge_type_cd11,
                rtr_insert_update_flg_insert.out_prty_addr_strt_dt       AS out_prty_addr_strt_dt1,
                rtr_insert_update_flg_insert.out_loc_id                  AS out_loc_id1,
                rtr_insert_update_flg_insert.out_prty_id                 AS out_prty_id1,
                rtr_insert_update_flg_insert.prcs_id                     AS prcs_id1,
                rtr_insert_update_flg_insert.out_prty_addr_end_dt        AS out_prty_addr_end_dt1,
                rtr_insert_update_flg_insert.updatetime                  AS trans_strt_dttm1,
                rtr_insert_update_flg_insert.out_edw_strt_dttm           AS edw_strt_dttm1,
                CASE
                       WHEN rtr_insert_update_flg_insert.retired = 0 THEN to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                       ELSE rtr_insert_update_flg_insert.out_edw_strt_dttm
                END AS out_edw_end_dttm1,
                CASE
                       WHEN rtr_insert_update_flg_insert.retired != 0 THEN rtr_insert_update_flg_insert.updatetime
                       ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END AS out_trans_end_dttm1,
                rtr_insert_update_flg_insert.source_record_id
         FROM   rtr_insert_update_flg_insert );
  -- Component tgt_prty_addr_insupd, Type TARGET
  INSERT INTO db_t_prod_core.prty_addr
              (
                          prty_addr_usge_type_cd,
                          prty_addr_strt_dttm,
                          loc_id,
                          prty_id,
                          prty_addr_end_dttm,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_target_insupd.out_prty_addr_usge_type_cd11 AS prty_addr_usge_type_cd,
         exp_pass_to_target_insupd.out_prty_addr_strt_dt1       AS prty_addr_strt_dttm,
         exp_pass_to_target_insupd.out_loc_id1                  AS loc_id,
         exp_pass_to_target_insupd.out_prty_id1                 AS prty_id,
         exp_pass_to_target_insupd.out_prty_addr_end_dt1        AS prty_addr_end_dttm,
         exp_pass_to_target_insupd.prcs_id1                     AS prcs_id,
         exp_pass_to_target_insupd.edw_strt_dttm1               AS edw_strt_dttm,
         exp_pass_to_target_insupd.out_edw_end_dttm1            AS edw_end_dttm,
         exp_pass_to_target_insupd.trans_strt_dttm1             AS trans_strt_dttm,
         exp_pass_to_target_insupd.out_trans_end_dttm1          AS trans_end_dttm
  FROM   exp_pass_to_target_insupd;
  
  -- Component tgt_prty_addr_insupd, Type Post SQL
  UPDATE db_t_prod_core.prty_addr
  SET    trans_end_dttm= a.lead1,
         edw_end_dttm = a.lead
  FROM   (
                         SELECT DISTINCT prty_addr_usge_type_cd,
                                         prty_id,
                                         edw_strt_dttm,
                                         trans_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY prty_addr_usge_type_cd,prty_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead,
                                         max(trans_strt_dttm) over (PARTITION BY prty_addr_usge_type_cd,prty_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.prty_addr
                         WHERE           prty_addr_usge_type_cd IN (''PREMAIL'',
                                                                    ''SCEMAIL'') ) a

  WHERE  prty_addr.edw_strt_dttm = a.edw_strt_dttm
  AND    prty_addr.prty_id=a.prty_id
  AND    prty_addr.prty_addr_usge_type_cd=a.prty_addr_usge_type_cd
  AND    prty_addr.trans_strt_dttm <>prty_addr.trans_end_dttm
  AND    prty_addr.prty_addr_usge_type_cd IN (''PREMAIL'',
                                              ''SCEMAIL'')
  AND    lead IS NOT NULL;
  
  -- Component tgt_prty_addr_retire, Type TARGET
  merge
  INTO         db_t_prod_core.prty_addr
  USING        exp_pass_to_target_retire
  ON (
                            prty_addr.prty_addr_usge_type_cd = exp_pass_to_target_retire.lkp_prty_addr_usge_type_cd3
               AND          prty_addr.prty_id = exp_pass_to_target_retire.lkp_prty_id3
               AND          prty_addr.edw_strt_dttm = exp_pass_to_target_retire.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    prty_addr_usge_type_cd = exp_pass_to_target_retire.lkp_prty_addr_usge_type_cd3,
         prty_id = exp_pass_to_target_retire.lkp_prty_id3,
         edw_strt_dttm = exp_pass_to_target_retire.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_target_retire.out_edw_end_dttm3,
         trans_end_dttm = exp_pass_to_target_retire.out_trans_end_dttm3;
  
  -- Component tgt_prty_addr_insert, Type TARGET
  INSERT INTO db_t_prod_core.prty_addr
              (
                          prty_addr_usge_type_cd,
                          prty_addr_strt_dttm,
                          loc_id,
                          prty_id,
                          prty_addr_end_dttm,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_target.out_prty_addr_usge_type_cd11 AS prty_addr_usge_type_cd,
         exp_pass_to_target.out_prty_addr_strt_dt1       AS prty_addr_strt_dttm,
         exp_pass_to_target.out_loc_id1                  AS loc_id,
         exp_pass_to_target.out_prty_id1                 AS prty_id,
         exp_pass_to_target.out_prty_addr_end_dt1        AS prty_addr_end_dttm,
         exp_pass_to_target.prcs_id1                     AS prcs_id,
         exp_pass_to_target.edw_strt_dttm1               AS edw_strt_dttm,
         exp_pass_to_target.out_edw_end_dttm1            AS edw_end_dttm,
         exp_pass_to_target.trans_strt_dttm1             AS trans_strt_dttm,
         exp_pass_to_target.out_trans_end_dttm1          AS trans_end_dttm
  FROM   exp_pass_to_target;

END;
';