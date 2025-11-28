-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_ADDR_TLPHN_NUM_INS("WORKLET_NAME" VARCHAR)
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
  
  -- Component SQ_ab_abcontact, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_ab_abcontact AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS retired,
                $2  AS prty_addr_usge_type_cd,
                $3  AS loc_id,
                $4  AS var_prty_id,
                $5  AS in_prty_addr_trans_dttm,
                $6  AS in_prty_addr_strt_dttm,
                $7  AS out_prty_addr_end_dt,
                $8  AS lkp_prty_addr_usge_type_cd,
                $9  AS lkp_prty_addr_strt_dttm,
                $10 AS lkp_loc_id,
                $11 AS lkp_prty_id,
                $12 AS lkp_prty_addr_end_dttm,
                $13 AS lkp_edw_strt_dttm,
                $14 AS lkp_edw_end_dttm,
                $15 AS var_chksum_lkp,
                $16 AS var_chksum_inp,
                $17 AS ins_upd_flag,
                $18 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT          src_mstr.retired,
                                                                  src_mstr.prty_addr_usge_type_cd      AS prty_addr_usge_type_cd ,
                                                                  src_mstr.loc_id                      AS loc_id,
                                                                  src_mstr.var_prty_id                 AS var_prty_id,
                                                                  src_mstr.in_prty_addr_trans_dttm     AS in_prty_addr_trans_dttm,
                                                                  src_mstr.in_prty_addr_strt_dttm      AS in_prty_addr_strt_dttm,
                                                                  src_mstr.out_prty_addr_end_dt        AS out_prty_addr_end_dt ,
                                                                  lkp_prty_addr.prty_addr_usge_type_cd AS lkp_prty_addr_usge_type_cd,
                                                                  lkp_prty_addr.prty_addr_strt_dttm    AS lkp_prty_addr_strt_dttm,
                                                                  lkp_prty_addr.loc_id                 AS lkp_loc_id,
                                                                  lkp_prty_addr.prty_id                AS lkp_prty_id,
                                                                  lkp_prty_addr.prty_addr_end_dttm     AS lkp_prty_addr_end_dttm,
                                                                  lkp_prty_addr.edw_strt_dttm          AS lkp_edw_strt_dttm,
                                                                  lkp_prty_addr.edw_end_dttm           AS lkp_edw_end_dttm,
                                                                  /* Target data */
                                                                  cast(( trim(to_char(to_char (lkp_prty_addr.prty_addr_strt_dttm , ''YYYY-MM-DDBHH:MI:SS.FF6'')))
                                                                                  ||trim(to_char(to_char (lkp_prty_addr.prty_addr_end_dttm , ''YYYY-MM-DDBHH:MI:SS.FF6'')))
                                                                                  ||trim(lkp_prty_addr.loc_id)) AS VARCHAR(1100)) AS var_chksum_lkp
                                                                  /* Source data        */
                                                                  ,
                                                                  cast((trim(to_char((cast(in_prty_addr_strt_dttm AS timestamp))))
                                                                                  ||trim(to_char(cast(out_prty_addr_end_dt AS timestamp)))
                                                                                  ||trim(src_mstr.loc_id)) AS VARCHAR(1100)) AS var_chksum_inp
                                                                  /* Flag */
                                                                  ,
                                                                  CASE
                                                                                  WHEN var_chksum_lkp IS NULL THEN ''I''
                                                                                  WHEN (
                                                                                                                  var_chksum_lkp=var_chksum_inp) THEN ''R''
                                                                                  ELSE ''U''
                                                                  END AS ins_upd_flag
                                                  FROM            (
                                                                         SELECT lkp_mstr.* ,
                                                                                CASE
                                                                                       WHEN var_prty_id IS NOT NULL THEN var_prty_id
                                                                                       ELSE 9999
                                                                                END AS out_prty_id
                                                                         FROM   (
                                                                                                SELECT DISTINCT src_xlat.phone,
                                                                                                                src_xlat.src_idntftn_val,
                                                                                                                src_xlat.addressbookuid,
                                                                                                                src_xlat.source,
                                                                                                                src_xlat.tl_cnt_name,
                                                                                                                src_xlat.updatetime,
                                                                                                                src_xlat.createtime,
                                                                                                                src_xlat.retired ,
                                                                                                                src_xlat.busn_ctgy_cd,
                                                                                                                src_xlat.prty_addr_usge_type_cd,
                                                                                                                src_xlat.loc_id ,
                                                                                                                CASE
                                                                                                                                WHEN updatetime IS NULL THEN to_timestamp_ntz (''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DDBHH:MI:SS.FF6'')
                                                                                                                                ELSE updatetime
                                                                                                                END AS in_prty_addr_trans_dttm ,
                                                                                                                CASE
                                                                                                                                WHEN createtime IS NULL THEN to_timestamp_ntz (''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DDBHH:MI:SS.FF6'')
                                                                                                                                ELSE createtime
                                                                                                                END                                                                                AS in_prty_addr_strt_dttm ,
                                                                                                                to_timestamp_ntz (''9999-12-31 23:59:59.999999'', ''YYYY-MM-DDBHH:MI:SS.FF6'') AS out_prty_addr_end_dt ,
                                                                                                                CASE
                                                                                                                                WHEN src_xlat.source = ''ContactManager''
                                                                                                                                AND             src_xlat.tl_cnt_name IN (''Person'',
                                                                                                                                                                         ''Adjudicator'',
                                                                                                                                                                         ''User Contact'',
                                                                                                                                                                         ''UserContact'',
                                                                                                                                                                         ''Vendor (Person)'',
                                                                                                                                                                         ''Attorney'',
                                                                                                                                                                         ''Doctor'',
                                                                                                                                                                         ''Policy Person'',
                                                                                                                                                                         ''Contact'') THEN lkp_indiv_cnt_mgr.indiv_prty_id
                                                                                                                                ELSE
                                                                                                                                                CASE
                                                                                                                                                                WHEN src_xlat.source = ''ClaimCenter''
                                                                                                                                                                AND             src_xlat.tl_cnt_name IN (''Person'',
                                                                                                                                                                                                        ''Adjudicator'',
                                                                                                                                                                                                        ''User Contact'',
                                                                                                                                                                                                        ''UserContact'',
                                                                                                                                                                                                        ''Vendor (Person)'',
                                                                                                                                                                                                        ''Attorney'',
                                                                                                                                                                                                        ''Doctor'',
                                                                                                                                                                                                        ''Policy Person'',
                                                                                                                                                                                                        ''Contact'') THEN lkp_indiv_clm_ctr.indiv_prty_id
                                                                                                                                                                ELSE
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN src_xlat.source = ''ContactManager''
                                                                                                                                                                                                AND             src_xlat.tl_cnt_name IN (''Company'',
                                                                                                                                                                                                        ''CompanyVendor'',
                                                                                                                                                                                                        ''AutoRepairShop'',
                                                                                                                                                                                                        ''AutoTowingAgcy'',
                                                                                                                                                                                                        ''LawFirm'',
                                                                                                                                                                                                        ''MedicalCareOrg'') THEN lkp_busn.busn_prty_id
                                                                                                                                                                                                ELSE
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN src_xlat.source = ''ClaimCenter''
                                                                                                                                                                                                        AND             src_xlat.tl_cnt_name IN (''Company'',
                                                                                                                                                                                                        ''CompanyVendor'',
                                                                                                                                                                                                        ''AutoRepairShop'',
                                                                                                                                                                                                        ''AutoTowingAgcy'',
                                                                                                                                                                                                        ''LawFirm'',
                                                                                                                                                                                                        ''MedicalCareOrg'') THEN lkp_busn.busn_prty_id
                                                                                                                                                                                                        END
                                                                                                                                                                                END
                                                                                                                                                END
                                                                                                                END AS var_prty_id
                                                                                                FROM            (
                                                                                                                                SELECT DISTINCT src.phone,
                                                                                                                                                src.src_idntftn_val,
                                                                                                                                                src.addressbookuid,
                                                                                                                                                src.source,
                                                                                                                                                src.tl_cnt_name,
                                                                                                                                                src.updatetime,
                                                                                                                                                src.createtime,
                                                                                                                                                src.retired ,
                                                                                                                                                CASE
                                                                                                                                                                WHEN lkp_xlat_busn_ctgry_cd.tgt_idntftn_val IS NOT NULL THEN lkp_xlat_busn_ctgry_cd.tgt_idntftn_val
                                                                                                                                                                ELSE ''UNK''
                                                                                                                                                END AS busn_ctgy_cd ,
                                                                                                                                                CASE
                                                                                                                                                                WHEN lkp_teradata_etl_ref_xlat.tgt_idntftn_val IS NOT NULL THEN lkp_teradata_etl_ref_xlat.tgt_idntftn_val
                                                                                                                                                                ELSE ''UNK''
                                                                                                                                                END AS prty_addr_usge_type_cd ,
                                                                                                                                                CASE
                                                                                                                                                                WHEN lkp_tlphn_num.tlphn_num_id IS NOT NULL THEN lkp_tlphn_num.tlphn_num_id
                                                                                                                                                                ELSE 9999
                                                                                                                                                END AS loc_id
                                                                                                                                FROM            (
                                                                                                                                                       SELECT
                                                                                                                                                              CASE
                                                                                                                                                                     WHEN phone='''' THEN NULL
                                                                                                                                                                     ELSE phone
                                                                                                                                                              END phone,
                                                                                                                                                              /*  phone, */
                                                                                                                                                              x.src_idntftn_val,
                                                                                                                                                              addressbookuid,
                                                                                                                                                              /* Source, */
                                                                                                                                                              substr(x.source,position(''-'',x.source)+1) AS source,
                                                                                                                                                              CASE
                                                                                                                                                                     WHEN (
                                                                                                                                                                                   (
                                                                                                                                                                                          position(''-'',x.source)=0)
                                                                                                                                                                            OR     (
                                                                                                                                                                                          x.source IS NULL)
                                                                                                                                                                            OR     (
                                                                                                                                                                                          x.source='' '')
                                                                                                                                                                            OR     (
                                                                                                                                                                                          length(x.source)=0)) THEN x.source
                                                                                                                                                                     ELSE (substr(x.source,1,position(''-'',x.source)-1))
                                                                                                                                                              END AS tl_cnt_name,
                                                                                                                                                              updatetime,
                                                                                                                                                              createtime,
                                                                                                                                                              retired
                                                                                                                                                       FROM   (
                                                                                                                                                                       SELECT   phone,
                                                                                                                                                                                src_idntftn_val,
                                                                                                                                                                                addressbookuid,
                                                                                                                                                                                source,
                                                                                                                                                                                updatetime,
                                                                                                                                                                                createtime,
                                                                                                                                                                                retired
                                                                                                                                                                       FROM     (
                                                                                                                                                                                           SELECT     cast(pc_contact.homephone_stg AS VARCHAR(60)) AS phone,
                                                                                                                                                                                                      cast(''Home'' AS                   VARCHAR(60)) AS src_idntftn_val,
                                                                                                                                                                                                      pc_contact.addressbookuid_stg                 AS addressbookuid,
                                                                                                                                                                                                      pctl_contact.name_stg
                                                                                                                                                                                                        ||''-ContactManager'' AS source,
                                                                                                                                                                                                      pc_contact.updatetime_stg      AS updatetime,
                                                                                                                                                                                                      pc_contact.createtime_stg      AS createtime,
                                                                                                                                                                                                      pc_contact.retired_stg         AS retired
                                                                                                                                                                                           FROM       db_t_prod_stag.pc_contact
                                                                                                                                                                                           inner join db_t_prod_stag.pctl_contact
                                                                                                                                                                                           ON         pctl_contact.id_stg=pc_contact.subtype_stg
                                                                                                                                                                                           WHERE      pc_contact.updatetime_stg> (:start_dttm)
                                                                                                                                                                                           AND        pc_contact.updatetime_stg <= ( :end_dttm)
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT     cast(pc_contact.cellphone_stg AS VARCHAR(60)) AS phone,
                                                                                                                                                                                                      cast(''Cell'' AS                   VARCHAR(60)) AS src_idntftn_val,
                                                                                                                                                                                                      pc_contact.addressbookuid_stg                 AS addressbookuid,
                                                                                                                                                                                                      pctl_contact.name_stg
                                                                                                                                                                                                        ||''-ContactManager'' AS source,
                                                                                                                                                                                                      pc_contact.updatetime_stg      AS updatetime,
                                                                                                                                                                                                      pc_contact.createtime_stg      AS createtime,
                                                                                                                                                                                                      pc_contact.retired_stg         AS retired
                                                                                                                                                                                           FROM       db_t_prod_stag.pc_contact
                                                                                                                                                                                           inner join db_t_prod_stag.pctl_contact
                                                                                                                                                                                           ON         pctl_contact.id_stg=pc_contact.subtype_stg
                                                                                                                                                                                           WHERE      pc_contact.updatetime_stg> (:start_dttm)
                                                                                                                                                                                           AND        pc_contact.updatetime_stg <= ( :end_dttm)
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT
                                                                                                                                                                                                      CASE
                                                                                                                                                                                                        WHEN cast(pc_contact.workphone_stg AS VARCHAR(60)) = '' '' THEN NULL
                                                                                                                                                                                                        ELSE cast(pc_contact.workphone_stg AS VARCHAR(60))
                                                                                                                                                                                                      END                           AS phone,
                                                                                                                                                                                                      cast(''Work'' AS VARCHAR(60))   AS src_idntftn_val,
                                                                                                                                                                                                      pc_contact.addressbookuid_stg AS addressbookuid,
                                                                                                                                                                                                      pctl_contact.name_stg
                                                                                                                                                                                                        ||''-ContactManager'' AS source,
                                                                                                                                                                                                      pc_contact.updatetime_stg      AS updatetime,
                                                                                                                                                                                                      pc_contact.createtime_stg      AS createtime,
                                                                                                                                                                                                      pc_contact.retired_stg         AS retired
                                                                                                                                                                                           FROM       db_t_prod_stag.pc_contact
                                                                                                                                                                                           inner join db_t_prod_stag.pctl_contact
                                                                                                                                                                                           ON         pctl_contact.id_stg=pc_contact.subtype_stg
                                                                                                                                                                                           WHERE      pc_contact.updatetime_stg> (:start_dttm)
                                                                                                                                                                                           AND        pc_contact.updatetime_stg <= ( :end_dttm)
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT     cast(pc_contact.primaryphone_stg AS VARCHAR(60)) AS phone,
                                                                                                                                                                                                      cast(''PRTY_ADDR_USGE_TYPE4'' AS      VARCHAR(60)) AS src_idntftn_val,
                                                                                                                                                                                                      pc_contact.addressbookuid_stg                    AS addressbookuid,
                                                                                                                                                                                                      pctl_contact.name_stg
                                                                                                                                                                                                        ||''-ContactManager'' AS source,
                                                                                                                                                                                                      pc_contact.updatetime_stg      AS updatetime,
                                                                                                                                                                                                      pc_contact.createtime_stg      AS createtime,
                                                                                                                                                                                                      pc_contact.retired_stg         AS retired
                                                                                                                                                                                           FROM       db_t_prod_stag.pc_contact
                                                                                                                                                                                           inner join db_t_prod_stag.pctl_contact
                                                                                                                                                                                           ON         pctl_contact.id_stg=pc_contact.subtype_stg
                                                                                                                                                                                           WHERE      pc_contact.updatetime_stg> (:start_dttm)
                                                                                                                                                                                           AND        pc_contact.updatetime_stg <= ( :end_dttm)
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT     cast(cc_contact.homephone_stg AS VARCHAR(60)) AS phone,
                                                                                                                                                                                                      ''Home''                                        AS src_idntftn_val,
                                                                                                                                                                                                      publicid_stg                                  AS addressbookuid,
                                                                                                                                                                                                      cctl_contact.name_stg
                                                                                                                                                                                                        ||''-ClaimCenter'' AS source,
                                                                                                                                                                                                      cc_contact.updatetime_stg   AS updatetime,
                                                                                                                                                                                                      cc_contact.createtime_stg   AS createtime,
                                                                                                                                                                                                      cc_contact.retired_stg      AS retired
                                                                                                                                                                                           FROM       db_t_prod_stag.cc_contact
                                                                                                                                                                                           inner join db_t_prod_stag.cctl_contact
                                                                                                                                                                                           ON         cctl_contact.id_stg=cc_contact.subtype_stg
                                                                                                                                                                                           WHERE      cc_contact.updatetime_stg > (:start_dttm)
                                                                                                                                                                                           AND        cc_contact.updatetime_stg <= ( :end_dttm)
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT     cast(cc_contact.cellphone_stg AS VARCHAR(60)) AS phone,
                                                                                                                                                                                                      cast(''Cell'' AS                   VARCHAR(60)) AS src_idntftn_val,
                                                                                                                                                                                                      publicid_stg                                  AS addressbookuid,
                                                                                                                                                                                                      cctl_contact.name_stg
                                                                                                                                                                                                        ||''-ClaimCenter''AS source,
                                                                                                                                                                                                      cc_contact.updatetime_stg  AS updatetime,
                                                                                                                                                                                                      cc_contact.createtime_stg  AS createtime,
                                                                                                                                                                                                      cc_contact.retired_stg     AS retired
                                                                                                                                                                                           FROM       db_t_prod_stag.cc_contact
                                                                                                                                                                                           inner join db_t_prod_stag.cctl_contact
                                                                                                                                                                                           ON         cctl_contact.id_stg=cc_contact.subtype_stg
                                                                                                                                                                                           WHERE      cc_contact.updatetime_stg > (:start_dttm)
                                                                                                                                                                                           AND        cc_contact.updatetime_stg <= ( :end_dttm)
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT     cast(cc_contact.workphone_stg AS VARCHAR(60)) AS phone,
                                                                                                                                                                                                      cast(''Work'' AS                   VARCHAR(60)) AS src_idntftn_val,
                                                                                                                                                                                                      publicid_stg                                  AS addressbookuid,
                                                                                                                                                                                                      cctl_contact.name_stg
                                                                                                                                                                                                        ||''-ClaimCenter'' AS source,
                                                                                                                                                                                                      cc_contact.updatetime_stg   AS updatetime,
                                                                                                                                                                                                      cc_contact.createtime_stg   AS createtime,
                                                                                                                                                                                                      cc_contact.retired_stg      AS retired
                                                                                                                                                                                           FROM       db_t_prod_stag.cc_contact
                                                                                                                                                                                           inner join db_t_prod_stag.cctl_contact
                                                                                                                                                                                           ON         cctl_contact.id_stg=cc_contact.subtype_stg
                                                                                                                                                                                           WHERE      cc_contact.updatetime_stg > (:start_dttm)
                                                                                                                                                                                           AND        cc_contact.updatetime_stg <= ( :end_dttm)
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT     cast(cc_contact.primaryphone_stg AS VARCHAR(60)) AS phone,
                                                                                                                                                                                                      cast(''PRTY_ADDR_USGE_TYPE4'' AS      VARCHAR(60)) AS src_idntftn_val,
                                                                                                                                                                                                      publicid_stg                                     AS addressbookuid,
                                                                                                                                                                                                      cctl_contact.name_stg
                                                                                                                                                                                                        ||''-ClaimCenter'' AS source,
                                                                                                                                                                                                      cc_contact.updatetime_stg   AS updatetime,
                                                                                                                                                                                                      cc_contact.createtime_stg   AS createtime,
                                                                                                                                                                                                      cc_contact.retired_stg      AS retired
                                                                                                                                                                                           FROM       db_t_prod_stag.cc_contact
                                                                                                                                                                                           inner join db_t_prod_stag.cctl_contact
                                                                                                                                                                                           ON         cctl_contact.id_stg=cc_contact.subtype_stg
                                                                                                                                                                                           WHERE      cc_contact.updatetime_stg > (:start_dttm)
                                                                                                                                                                                           AND        cc_contact.updatetime_stg <= ( :end_dttm)
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT cast(bc_contact.homephone_stg AS VARCHAR(60)) AS phone,
                                                                                                                                                                                                  cast(''Home'' AS                   VARCHAR(60)) AS src_idntftn_val,
                                                                                                                                                                                                  bc_contact.addressbookuid_stg                 AS addressbookuid,
                                                                                                                                                                                                  ''ContactManager''                              AS source,
                                                                                                                                                                                                  bc_contact.updatetime_stg                     AS updatetime,
                                                                                                                                                                                                  bc_contact.createtime_stg                     AS createtime,
                                                                                                                                                                                                  bc_contact.retired_stg                        AS retired
                                                                                                                                                                                           FROM   db_t_prod_stag.bc_contact
                                                                                                                                                                                           WHERE  bc_contact.updatetime_stg > (:start_dttm)
                                                                                                                                                                                           AND    bc_contact.updatetime_stg <= ( :end_dttm)
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT bc_contact.cellphone_stg      AS phone,
                                                                                                                                                                                                  cast(''Cell'' AS VARCHAR(60))   AS src_idntftn_val,
                                                                                                                                                                                                  bc_contact.addressbookuid_stg AS addressbookuid,
                                                                                                                                                                                                  ''ContactManager''              AS source,
                                                                                                                                                                                                  bc_contact.updatetime_stg     AS updatetime,
                                                                                                                                                                                                  bc_contact.createtime_stg     AS createtime,
                                                                                                                                                                                                  bc_contact.retired_stg        AS retired
                                                                                                                                                                                           FROM   db_t_prod_stag.bc_contact
                                                                                                                                                                                           WHERE  bc_contact.updatetime_stg > (:start_dttm)
                                                                                                                                                                                           AND    bc_contact.updatetime_stg <= ( :end_dttm)
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT cast(bc_contact.workphone_stg AS VARCHAR(60)) AS phone,
                                                                                                                                                                                                  cast(''Work'' AS                   VARCHAR(60)) AS src_idntftn_val,
                                                                                                                                                                                                  bc_contact.addressbookuid_stg                 AS addressbookuid,
                                                                                                                                                                                                  ''ContactManager''                              AS source,
                                                                                                                                                                                                  bc_contact.updatetime_stg                     AS updatetime,
                                                                                                                                                                                                  bc_contact.createtime_stg                     AS createtime,
                                                                                                                                                                                                  bc_contact.retired_stg                        AS retired
                                                                                                                                                                                           FROM   db_t_prod_stag.bc_contact
                                                                                                                                                                                           WHERE  bc_contact.updatetime_stg > (:start_dttm)
                                                                                                                                                                                           AND    bc_contact.updatetime_stg <= ( :end_dttm)
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT cast(bc_contact.primaryphone_stg AS VARCHAR(60)) AS phone,
                                                                                                                                                                                                  cast(''PRTY_ADDR_USGE_TYPE4'' AS      VARCHAR(60)) AS src_idntftn_val,
                                                                                                                                                                                                  bc_contact.addressbookuid_stg                    AS addressbookuid,
                                                                                                                                                                                                  ''ContactManager''                                 AS source,
                                                                                                                                                                                                  bc_contact.updatetime_stg                        AS updatetime,
                                                                                                                                                                                                  bc_contact.createtime_stg                        AS createtime,
                                                                                                                                                                                                  bc_contact.retired_stg                           AS retired
                                                                                                                                                                                           FROM   db_t_prod_stag.bc_contact
                                                                                                                                                                                           WHERE  bc_contact.updatetime_stg > (:start_dttm)
                                                                                                                                                                                           AND    bc_contact.updatetime_stg <= ( :end_dttm) ) x
                                                                                                                                                                       WHERE    phone IS NOT NULL
                                                                                                                                                                       AND      addressbookuid IS NOT NULL
                                                                                                                                                                       AND      retired = 0
                                                                                                                                                                                /* Temporary Condition for SIT failure and it needs to be removed*/
                                                                                                                                                                                qualify row_number() over( PARTITION BY src_idntftn_val, addressbookuid, source ORDER BY updatetime DESC, createtime DESC) = 1
                                                                                                                                                                                /* order by updatetime_stg , createtime_stg */
                                                                                                                                                              )x ) src
                                                                                                                                                /* ---------------------------xlat lookup INFORMATION_SCHEMA.tables LKP_TERADATA_ETL_REF_XLAT_BUSN_CTGY_CD---------- */
                                                                                                                                left outer join
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
                                                                                                                                                       AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) AS lkp_xlat_busn_ctgry_cd
                                                                                                                                ON              lkp_xlat_busn_ctgry_cd.src_idntftn_val = src.tl_cnt_name
                                                                                                                                                /* ---------------------------xlat lookup INFORMATION_SCHEMA.tables LKP_TERADATA_ETL_REF_XLAT---------- */
                                                                                                                                left outer join
                                                                                                                                                (
                                                                                                                                                       SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                                                              teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                                                                       FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                                                                                                                       WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRTY_ADDR_USGE_TYPE''
                                                                                                                                                       AND    teradata_etl_ref_xlat.src_idntftn_nm IN (''pctl_phonetype.typecode'',
                                                                                                                                                                                                       ''derived'')
                                                                                                                                                       AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''GW'',
                                                                                                                                                                                                        ''DS'')
                                                                                                                                                       AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) AS lkp_teradata_etl_ref_xlat
                                                                                                                                ON              lkp_teradata_etl_ref_xlat.src_idntftn_val=src.src_idntftn_val
                                                                                                                                                /* ------------------LKP_TLPHN_NUM ------------------------ */
                                                                                                                                                /*DECODE(TRUE, :LKP.LKP_TLPHN_NUM(Phone) IS NOT NULL,  :LKP.LKP_TLPHN_NUM(Phone),9999)*/
                                                                                                                                left outer join
                                                                                                                                                (
                                                                                                                                                                SELECT DISTINCT tlphn_num.tlphn_num_id   AS tlphn_num_id,
                                                                                                                                                                                tlphn_num AS tlphn_num
                                                                                                                                                                FROM            db_t_prod_core.tlphn_num
                                                                                                                                                                                /* WHERE   CAST( EDW_END_DTTM AS DATE)=''9999-12-31'' */
                                                                                                                                                                                qualify row_number () over (PARTITION BY tlphn_num ORDER BY edw_end_dttm DESC )=1 ) AS lkp_tlphn_num
                                                                                                                                ON              tlphn_num = src.phone ) AS src_xlat
                                                                                                                /* ------------------------MASTER LOOKUP------------------------------------------------------------------------- */
                                                                                                left outer join
                                                                                                                (
                                                                                                                       SELECT indiv.indiv_prty_id AS indiv_prty_id,
                                                                                                                              indiv.nk_link_id    AS nk_link_id
                                                                                                                       FROM   db_t_prod_core.indiv
                                                                                                                       WHERE  indiv.nk_publc_id IS NULL) AS lkp_indiv_cnt_mgr
                                                                                                ON              upper(lkp_indiv_cnt_mgr.nk_link_id) = upper(src_xlat.addressbookuid)
                                                                                                left outer join
                                                                                                                (
                                                                                                                       SELECT indiv.indiv_prty_id AS indiv_prty_id,
                                                                                                                              indiv.nk_publc_id   AS nk_publc_id
                                                                                                                       FROM   db_t_prod_core.indiv
                                                                                                                       WHERE  indiv.nk_publc_id IS NOT NULL ) AS lkp_indiv_clm_ctr
                                                                                                ON              upper(lkp_indiv_clm_ctr.nk_publc_id) = upper(src_xlat.addressbookuid)
                                                                                                left outer join
                                                                                                                (
                                                                                                                         SELECT   busn.busn_prty_id AS busn_prty_id ,
                                                                                                                                  busn.busn_ctgy_cd AS busn_ctgy_cd,
                                                                                                                                  busn.nk_busn_cd   AS nk_busn_cd
                                                                                                                         FROM     db_t_prod_core.busn qualify row_number () over ( PARTITION BY nk_busn_cd, busn_ctgy_cd ORDER BY edw_end_dttm DESC )=1 ) AS lkp_busn
                                                                                                ON              lkp_busn.busn_ctgy_cd = src_xlat.busn_ctgy_cd
                                                                                                AND             upper(lkp_busn.nk_busn_cd)=upper(src_xlat.addressbookuid) ) AS lkp_mstr ) AS src_mstr
                                                                  /* ----------------TARGET LOOKUP ------------------------- */
                                                  left outer join
                                                                  (
                                                                                  SELECT DISTINCT prty_addr.prty_addr_strt_dttm    AS prty_addr_strt_dttm,
                                                                                                  prty_addr.loc_id                 AS loc_id,
                                                                                                  prty_addr.prty_addr_end_dttm     AS prty_addr_end_dttm,
                                                                                                  prty_addr.edw_strt_dttm          AS edw_strt_dttm,
                                                                                                  prty_addr.edw_end_dttm           AS edw_end_dttm,
                                                                                                  prty_addr.prty_addr_usge_type_cd AS prty_addr_usge_type_cd,
                                                                                                  prty_addr.prty_id                AS prty_id
                                                                                  FROM            db_t_prod_core.prty_addr
                                                                                  WHERE           cast( edw_end_dttm AS DATE)=''9999-12-31'' ) AS lkp_prty_addr
                                                  ON              lkp_prty_addr.prty_addr_usge_type_cd=src_mstr.prty_addr_usge_type_cd
                                                  AND             lkp_prty_addr.prty_id=src_mstr.out_prty_id ) src ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
         SELECT sq_ab_abcontact.prty_addr_usge_type_cd     AS out_prty_addr_usge_type_cd1,
                sq_ab_abcontact.out_prty_addr_end_dt       AS out_prty_addr_end_dt,
                sq_ab_abcontact.loc_id                     AS out_loc_id,
                sq_ab_abcontact.var_prty_id                AS out_prty_id,
                :prcs_id                                   AS prcs_id,
                sq_ab_abcontact.retired                    AS retired,
                sq_ab_abcontact.lkp_prty_addr_usge_type_cd AS tgt_prty_addr_usge_type_cd,
                sq_ab_abcontact.lkp_prty_addr_strt_dttm    AS tgt_prty_addr_strt_dttm,
                sq_ab_abcontact.lkp_loc_id                 AS tgt_loc_id,
                sq_ab_abcontact.lkp_prty_id                AS tgt_prty_id,
                sq_ab_abcontact.lkp_prty_addr_end_dttm     AS tgt_prty_addr_end_dttm,
                sq_ab_abcontact.lkp_edw_strt_dttm          AS tgt_edw_strt_dttm,
                sq_ab_abcontact.lkp_edw_end_dttm           AS tgt_edw_end_dttm,
                sq_ab_abcontact.in_prty_addr_trans_dttm    AS in_prty_addr_trans_dttm,
                sq_ab_abcontact.in_prty_addr_strt_dttm     AS in_prty_addr_strt_dttm,
                sq_ab_abcontact.var_chksum_lkp             AS var_chksum_lkp,
                sq_ab_abcontact.var_chksum_inp             AS var_chksum_inp,
                sq_ab_abcontact.ins_upd_flag               AS ins_upd_flag,
                sq_ab_abcontact.source_record_id
         FROM   sq_ab_abcontact );
  -- Component ecp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE ecp_data_transformation AS
  (
         SELECT exp_pass_from_source.tgt_prty_addr_usge_type_cd  AS lkp_prty_addr_usge_type_cd,
                exp_pass_from_source.tgt_prty_addr_strt_dttm     AS lkp_prty_addr_strt_dttm,
                exp_pass_from_source.tgt_loc_id                  AS lkp_loc_id,
                exp_pass_from_source.tgt_prty_id                 AS lkp_prty_id,
                exp_pass_from_source.tgt_prty_addr_end_dttm      AS lkp_prty_addr_end_dttm,
                exp_pass_from_source.tgt_edw_strt_dttm           AS lkp_edw_strt_dttm,
                exp_pass_from_source.tgt_edw_end_dttm            AS lkp_edw_end_dttm,
                exp_pass_from_source.out_prty_addr_usge_type_cd1 AS out_prty_addr_usge_type_cd1,
                exp_pass_from_source.out_loc_id                  AS out_loc_id,
                exp_pass_from_source.out_prty_id                 AS out_prty_id,
                exp_pass_from_source.in_prty_addr_strt_dttm      AS prty_addr_strt_dttm,
                exp_pass_from_source.out_prty_addr_end_dt        AS prty_addr_end_dttm,
                exp_pass_from_source.in_prty_addr_trans_dttm     AS prty_addr_trans_dttm,
                exp_pass_from_source.prcs_id                     AS prcs_id,
                exp_pass_from_source.retired                     AS retired,
                current_timestamp                                AS out_edw_strt_dttm,
                exp_pass_from_source.ins_upd_flag                AS ins_upd_flag,
                exp_pass_from_source.source_record_id
         FROM   exp_pass_from_source );
  -- Component rtr_insert_update_flg_insert, Type ROUTER Output Group insert
  create or replace temporary table rtr_insert_update_flg_insert as
  SELECT ecp_data_transformation.out_prty_addr_usge_type_cd1 AS out_prty_addr_usge_type_cd1,
         ecp_data_transformation.prty_addr_strt_dttm         AS out_prty_addr_strt_dt,
         ecp_data_transformation.out_loc_id                  AS out_loc_id,
         ecp_data_transformation.out_prty_id                 AS out_prty_id,
         ecp_data_transformation.prcs_id                     AS prcs_id,
         ecp_data_transformation.ins_upd_flag                AS out_flag,
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
                ecp_data_transformation.ins_upd_flag = ''I''
         AND    ecp_data_transformation.out_prty_id <> 9999
         AND    ecp_data_transformation.out_prty_id IS NOT NULL )
  OR     (
                ecp_data_transformation.retired = 0
         AND    ecp_data_transformation.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
         AND    ecp_data_transformation.out_prty_id <> 9999
         AND    ecp_data_transformation.out_prty_id IS NOT NULL )
  OR     (
                ecp_data_transformation.ins_upd_flag = ''U''
         AND    ecp_data_transformation.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
         AND    ecp_data_transformation.out_prty_id IS NOT NULL );
  
  -- Component rtr_insert_update_flg_retired, Type ROUTER Output Group retired
  create or replace temporary table rtr_insert_update_flg_retired as
  SELECT ecp_data_transformation.out_prty_addr_usge_type_cd1 AS out_prty_addr_usge_type_cd1,
         ecp_data_transformation.prty_addr_strt_dttm         AS out_prty_addr_strt_dt,
         ecp_data_transformation.out_loc_id                  AS out_loc_id,
         ecp_data_transformation.out_prty_id                 AS out_prty_id,
         ecp_data_transformation.prcs_id                     AS prcs_id,
         ecp_data_transformation.ins_upd_flag                AS out_flag,
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
  WHERE  ecp_data_transformation.ins_upd_flag = ''R''
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
         ecp_data_transformation.ins_upd_flag                AS out_flag,
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
  WHERE  1 = 2 -- ecp_data_transformation.ins_upd_flag = ''U''
  AND    ecp_data_transformation.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
  AND    ecp_data_transformation.out_prty_id IS NOT NULL;
  
  -- Component upd_prty_addr_upd, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_prty_addr_upd AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insert_update_flg_update.lkp_prty_addr_usge_type_cd AS lkp_prty_addr_usge_type_cd3,
                rtr_insert_update_flg_update.lkp_prty_addr_strt_dttm    AS lkp_prty_addr_strt_dttm3,
                rtr_insert_update_flg_update.lkp_loc_id                 AS lkp_loc_id3,
                rtr_insert_update_flg_update.lkp_prty_id                AS lkp_prty_id3,
                rtr_insert_update_flg_update.lkp_edw_strt_dttm          AS lkp_edw_strt_dttm3,
                rtr_insert_update_flg_update.out_edw_strt_dttm          AS out_edw_strt_dttm3,
                rtr_insert_update_flg_update.updatetime                 AS trans_strt_dttm3,
                1                                                       AS update_strategy_action,
                rtr_insert_update_flg_update.source_record_id
         FROM   rtr_insert_update_flg_update );
  -- Component exp_pass_to_target_update, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_update AS
  (
         SELECT upd_prty_addr_upd.lkp_prty_addr_usge_type_cd3                     AS lkp_prty_addr_usge_type_cd3,
                upd_prty_addr_upd.lkp_prty_id3                                    AS lkp_prty_id3,
                upd_prty_addr_upd.lkp_edw_strt_dttm3                              AS lkp_edw_strt_dttm3,
                dateadd ( second, -1, upd_prty_addr_upd.out_edw_strt_dttm3 ) AS out_edw_end_dttm3,
                dateadd ( second, -1,upd_prty_addr_upd.trans_strt_dttm3  ) AS out_trans_end_dttm3,
                upd_prty_addr_upd.source_record_id
         FROM   upd_prty_addr_upd );
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
                dateadd (second,-1, upd_prty_addr_retire.out_edw_strt_dttm3  ) AS out_edw_end_dttm3,
                dateadd (second, -1, upd_prty_addr_retire.trans_strt_dttm3  ) AS out_trans_end_dttm3,
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
  -- Component tgt_prty_addr_update, Type TARGET
  merge
  INTO         db_t_prod_core.prty_addr
  USING        exp_pass_to_target_update
  ON (
                            prty_addr.prty_addr_usge_type_cd = exp_pass_to_target_update.lkp_prty_addr_usge_type_cd3
               AND          prty_addr.prty_id = exp_pass_to_target_update.lkp_prty_id3
               AND          prty_addr.edw_strt_dttm = exp_pass_to_target_update.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    prty_addr_usge_type_cd = exp_pass_to_target_update.lkp_prty_addr_usge_type_cd3,
         prty_id = exp_pass_to_target_update.lkp_prty_id3,
         edw_strt_dttm = exp_pass_to_target_update.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_target_update.out_edw_end_dttm3,
         trans_end_dttm = exp_pass_to_target_update.out_trans_end_dttm3;
  
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
  
  -- Component tgt_prty_addr_insert, Type Post SQL
  UPDATE db_t_prod_core.prty_addr
    SET    trans_end_dttm= a.lead,
         edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT prty_addr_usge_type_cd,
                                         prty_id,
                                         edw_strt_dttm,
                                         max(trans_strt_dttm) over (PARTITION BY prty_addr_usge_type_cd, prty_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead,
                                         max(edw_strt_dttm) over (PARTITION BY prty_addr_usge_type_cd, prty_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.prty_addr ) a

  WHERE  prty_addr.edw_strt_dttm = a.edw_strt_dttm
         --and PRTY_ADDR.LOC_ID=A.LOC_ID
  AND    prty_addr.prty_addr_usge_type_cd=a.prty_addr_usge_type_cd
  AND    prty_addr.prty_id=a.prty_id
  AND    prty_addr.trans_strt_dttm <>prty_addr.trans_end_dttm
  AND    lead IS NOT NULL;
  
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

END;
';