-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_INDIV_NAME_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM timestamp;
  START_DTTM timestamp;
  run_id STRING;
  prcs_id int;


BEGIN

run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);


  -- Component sq_ab_abcontact1, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_ab_abcontact1 AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS src_firstname,
                $2  AS src_middlename,
                $3  AS src_lastname,
                $4  AS src_prefix,
                $5  AS src_suffix,
                $6  AS src_name,
                $7  AS src_name_type_cd,
                $8  AS src_strt_dt,
                $9  AS src_end_dt,
                $10 AS src_indiv_name_strt_dttm,
                $11 AS src_indiv_name_end_dttm,
                $12 AS src_retired,
                $13 AS src_indiv_prty_id,
                $14 AS lkup_indiv_prty_id,
                $15 AS lkup_name_type_cd,
                $16 AS lkup_indiv_name_strt_dttm,
                $17 AS lkup_gvn_name,
                $18 AS lkup_mdl_name,
                $19 AS lkup_fmly_name,
                $20 AS lkup_name_prefix_txt,
                $21 AS lkup_name_sfx_txt,
                $22 AS lkup_indiv_name_end_dttm,
                $23 AS lkup_indiv_full_name,
                $24 AS lkup_edw_strt_dttm,
                $25 AS lkup_edw_end_dttm,
                $26 AS sourcedata,
                $27 AS targetdata,
                $28 AS ins_upd_flag,
                $29 AS source_record_id
         FROM   (
                                SELECT          src.*,
                                                row_number() over (ORDER BY 1) AS source_record_id
                                FROM            ( select src_firstname, src_middlename, src_lastname, src_prefix, src_suffix, src_name, src_name_type_cd, src_strt_dt, src_end_dt, src_indiv_name_strt_dttm, src_indiv_name_end_dttm, src_retired, src_indiv_prty_id, tgt_lkup.indiv_prty_id, tgt_lkup.name_type_cd, tgt_lkup.indiv_name_strt_dttm, tgt_lkup.gvn_name, tgt_lkup.mdl_name, tgt_lkup.fmly_name, tgt_lkup.name_prefix_txt, tgt_lkup.name_sfx_txt, tgt_lkup.indiv_name_end_dttm, tgt_lkup.indiv_full_name, tgt_lkup.edw_strt_dttm, tgt_lkup.edw_end_dttm,
                                                /*Sourcedata*/
                                                cast(trim(coalesce(cast(cast(src_indiv_name_strt_dttm AS timestamp) AS VARCHAR(30)),0))
                                                                ||trim(coalesce(upper(src_firstname),''UNK''))
                                                                ||trim(coalesce(upper(src_middlename),''UNK''))
                                                                ||trim(coalesce(upper(src_lastname),''UNK''))
                                                                ||trim(coalesce(upper(src_prefix),''UNK''))
                                                                ||trim(coalesce(upper(src_suffix),''UNK''))
                                                                ||trim(coalesce(cast(cast(src_indiv_name_end_dttm AS timestamp) AS VARCHAR(30)),0))
                                                                ||trim(coalesce(upper(src_name),''UNK'')) AS VARCHAR(1100)) AS sourcedata,
                                                /*Targetdata*/
                                                cast(trim(coalesce(cast(to_char(tgt_lkup.indiv_name_strt_dttm , ''YYYY-MM-DDBHH:MI:SS'') AS VARCHAR(30)),0))
                                                                ||trim(coalesce(upper(tgt_lkup.gvn_name),''UNK''))
                                                                ||trim(coalesce(upper(tgt_lkup.mdl_name),''UNK''))
                                                                ||trim(coalesce(upper(tgt_lkup.fmly_name),''UNK''))
                                                                ||trim(coalesce(upper(tgt_lkup.name_prefix_txt),''UNK''))
                                                                ||trim(coalesce(upper(tgt_lkup.name_sfx_txt),''UNK''))
                                                                ||trim(coalesce(cast(to_char(tgt_lkup.indiv_name_end_dttm , ''YYYY-MM-DDBHH:MI:SS'') AS VARCHAR(30)),0))
                                                                ||trim(coalesce(upper(tgt_lkup.indiv_full_name),''UNK'')) AS VARCHAR(1100)) AS targetdata,
                                                /*Flag*/
                                                CASE
                                                                WHEN (
                                                                                                targetdata IS NULL
                                                                                OR              tgt_lkup.indiv_prty_id IS NULL) THEN ''I''
                                                                WHEN targetdata IS NOT NULL
                                                                AND             sourcedata <> targetdata THEN ''U''
                                                                WHEN targetdata IS NOT NULL
                                                                AND             sourcedata = targetdata THEN ''R''
                                                END AS ins_upd_flag FROM (
                                                       SELECT src.firstname  AS src_firstname,
                                                              src.middlename AS src_middlename,
                                                              src.lastname   AS src_lastname,
                                                              src.prefix     AS src_prefix,
                                                              src.suffix     AS src_suffix,
                                                              CASE
                                                                     WHEN src.name IS NULL
                                                                     OR     src.name='''' THEN stg_firstname
                                                                                   ||stg_middlename
                                                                                   ||stg_lastname
                                                                     ELSE upper(ltrim(rtrim(src.name)))
                                                              END                      AS src_name,
                                                              src.formername           AS src_formername,
                                                              src.name_type_cd         AS src_name_type_cd,
                                                              src.src_strt_dt          AS src_strt_dt,
                                                              src.src_end_dt           AS src_end_dt,
                                                              src.indiv_name_strt_dttm AS src_indiv_name_strt_dttm,
                                                              src.indiv_name_end_dttm  AS src_indiv_name_end_dttm,
                                                              src.retired              AS src_retired,
                                                              CASE
                                                                     WHEN src.indiv_prty_id IS NOT NULL THEN src.indiv_prty_id
                                                                     ELSE 9999
                                                              END AS src_indiv_prty_id
                                                       FROM   (
                                                                              SELECT DISTINCT stag.linkid,
                                                                                              stag.firstname,
                                                                                              stag.middlename,
                                                                                              stag.lastname,
                                                                                              stag.prefix,
                                                                                              stag.suffix,
                                                                                              stag.name,
                                                                                              stag.formername,
                                                                                              stag.name_type_cd,
                                                                                              stag.publicid,
                                                                                              stag.src,
                                                                                              stag.src_strt_dt,
                                                                                              stag.src_end_dt,
                                                                                              stag.indiv_name_strt_dttm,
                                                                                              stag.indiv_name_end_dttm,
                                                                                              stag.retired ,
                                                                                              CASE
                                                                                                              WHEN src=''ContactManager'' THEN lkp_indiv_cnt_mgr.indiv_prty_id
                                                                                                              WHEN src=''ClaimCenter'' THEN lkp_indiv_clm_ctr.indiv_prty_id
                                                                                              END AS indiv_prty_id,
                                                                                              CASE
                                                                                                              WHEN firstname IS NULL
                                                                                                              OR              firstname='''' THEN ''''
                                                                                                              ELSE upper(ltrim(rtrim(firstname)))
                                                                                                                                              ||'' ''
                                                                                              END AS stg_firstname,
                                                                                              CASE
                                                                                                              WHEN middlename IS NULL
                                                                                                              OR              middlename='''' THEN ''''
                                                                                                              ELSE upper(ltrim(rtrim(middlename)))
                                                                                                                                              ||'' ''
                                                                                              END AS stg_middlename,
                                                                                              CASE
                                                                                                              WHEN lastname IS NULL
                                                                                                              OR              lastname='''' THEN ''''
                                                                                                              ELSE upper(ltrim(rtrim(lastname)))
                                                                                              END AS stg_lastname
                                                                              FROM            (
                                                                                                       SELECT   aa.linkid,
                                                                                                                aa.firstname,
                                                                                                                aa.middlename,
                                                                                                                aa.lastname,
                                                                                                                aa.prefix,
                                                                                                                aa.suffix,
                                                                                                                aa.name,
                                                                                                                aa.formername,
                                                                                                                aa.name_type_cd,
                                                                                                                aa.publicid,
                                                                                                                aa.src,
                                                                                                                max(aa.src_strt_dt) AS src_strt_dt,
                                                                                                                aa.src_end_dt,
                                                                                                                max(aa.indiv_name_strt_dttm) AS indiv_name_strt_dttm,
                                                                                                                aa.indiv_name_end_dttm,
                                                                                                                aa.retired
                                                                                                       FROM     (
                                                                                                                                SELECT DISTINCT cast(upper(ab_abcontact.linkid) AS VARCHAR(100))AS linkid ,
                                                                                                                                                CASE
                                                                                                                                                                WHEN ab_abcontact.firstname IS NULL THEN ''UNK''
                                                                                                                                                                ELSE ab_abcontact.firstname
                                                                                                                                                END AS firstname ,
                                                                                                                                                ab_abcontact.middlename ,
                                                                                                                                                CASE
                                                                                                                                                                WHEN ab_abcontact.lastname IS NULL THEN ''UNK''
                                                                                                                                                                ELSE ab_abcontact.lastname
                                                                                                                                                END                          AS lastname ,
                                                                                                                                                ab_abcontact.name_prefix_txt AS prefix ,
                                                                                                                                                ab_abcontact.name_sfx_txt    AS suffix ,
                                                                                                                                                ab_abcontact.name ,
                                                                                                                                                ab_abcontact.formername ,
                                                                                                                                                CASE
                                                                                                                                                                WHEN ab_abcontact.firstname IS NOT NULL
                                                                                                                                                                AND             ab_abcontact.lastname IS NOT NULL
                                                                                                                                                                AND             ab_abcontact.dbaname_alfa IS NOT NULL THEN ''DBA''
                                                                                                                                                                WHEN ab_abcontact.firstname IS NOT NULL
                                                                                                                                                                AND             ab_abcontact.lastname IS NOT NULL THEN ''LGL''
                                                                                                                                                                ELSE ''UNK''
                                                                                                                                                END                             AS name_type_cd ,
                                                                                                                                                cast(publicid AS VARCHAR(100))  AS publicid ,
                                                                                                                                                source                          AS src,
                                                                                                                                                ab_abcontact.updatetime         AS src_strt_dt,
                                                                                                                                                to_date(''99991231'', ''YYYYMMDD'') AS src_end_dt,
                                                                                                                                                ab_abcontact.createtime         AS indiv_name_strt_dttm,
                                                                                                                                                to_date(''99991231'',''YYYYMMDD'')  AS indiv_name_end_dttm,
                                                                                                                                                retired
                                                                                                                                FROM            (
                                                                                                                                                       SELECT ab_cnt_inner.updatetime,
                                                                                                                                                              ab_cnt_inner.dbaname_alfa,
                                                                                                                                                              ab_cnt_inner.lastname ,
                                                                                                                                                              ab_cnt_inner.firstname ,
                                                                                                                                                              ab_cnt_inner.publicid,
                                                                                                                                                              ab_cnt_inner.formername ,
                                                                                                                                                              ab_cnt_inner.name ,
                                                                                                                                                              ab_cnt_inner.linkid ,
                                                                                                                                                              ab_cnt_inner.middlename,
                                                                                                                                                              ab_cnt_inner.retired,
                                                                                                                                                              ab_cnt_inner.createtime ,
                                                                                                                                                              ab_cnt_inner.name_prefix_txt,
                                                                                                                                                              ab_cnt_inner.name_sfx_txt,
                                                                                                                                                              ab_cnt_inner.source,
                                                                                                                                                              ab_cnt_inner.tl_cnt_name
                                                                                                                                                       FROM   (
                                                                                                                                                                              /*- DB_T_PROD_STAG.bc_contact dropzone query */
                                                                                                                                                                              SELECT          bc_contact.updatetime_stg                           AS updatetime,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255))                          AS dbaname_alfa,
                                                                                                                                                                                              bc_contact.lastname_stg                             AS lastname,
                                                                                                                                                                                              bc_contact.firstname_stg                            AS firstname,
                                                                                                                                                                                              cast(bc_contact.publicid_stg AS VARCHAR(100))       AS publicid,
                                                                                                                                                                                              bc_contact.formername_stg                           AS formername,
                                                                                                                                                                                              bc_contact.name_stg                                 AS name,
                                                                                                                                                                                              cast(bc_contact.addressbookuid_stg AS VARCHAR(100)) AS linkid,
                                                                                                                                                                                              bc_contact.middlename_stg                           AS middlename,
                                                                                                                                                                                              bc_contact.retired_stg                              AS retired,
                                                                                                                                                                                              bc_contact.createtime_stg                           AS createtime,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))                           AS name_prefix_txt,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))                           AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255))                          AS source,
                                                                                                                                                                                              bctl_contact.name_stg                                        AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.bc_contact
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_contact
                                                                                                                                                                              ON              bctl_contact.id_stg = bc_contact.subtype_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_gendertype
                                                                                                                                                                              ON              bc_contact.gender_stg = bctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxfilingstatustype
                                                                                                                                                                              ON              bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxstatus
                                                                                                                                                                              ON              bc_contact.taxstatus_stg = bctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_maritalstatus
                                                                                                                                                                              ON              bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_nameprefix
                                                                                                                                                                              ON              bc_contact.prefix_stg = bctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_namesuffix
                                                                                                                                                                              ON              bc_contact.suffix_stg = bctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_user
                                                                                                                                                                              ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_credential
                                                                                                                                                                              ON              bc_user.credentialid_stg = bc_credential.id_stg
                                                                                                                                                                              WHERE           bctl_contact.typecode_stg = (''UserContact'')
                                                                                                                                                                              AND
                                                                                                                                                                                              /*  below condition added to avoid duplicates*/
                                                                                                                                                                                              bc_contact.publicid_stg NOT IN (''default_data:1'',
                                                                                                                                                                                                        ''systemTables:1'',
                                                                                                                                                                                                        ''systemTables:2'')
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        bc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_contact.updatetime_stg <=( :END_DTTM))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        bc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_user.updatetime_stg <= ( :END_DTTM)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /* Primary and Secondary Payer contact (this is at the Account level)*/
                                                                                                                                                                              SELECT          bc_contact.updatetime_stg  AS updatetime,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255)) AS dbaname_alfa,
                                                                                                                                                                                              bc_contact.lastname_stg    AS lastname,
                                                                                                                                                                                              bc_contact.firstname_stg   AS firstname,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN(
                                                                                                                                                                                                        bc_contact.externalid_stg IS NOT NULL) THEN bc_contact.externalid_stg
                                                                                                                                                                                                        ELSE bc_contact.publicid_stg
                                                                                                                                                                                              END                           AS publicid,
                                                                                                                                                                                              bc_contact.formername_stg     AS formername,
                                                                                                                                                                                              bc_contact.name_stg           AS name,
                                                                                                                                                                                              bc_contact.addressbookuid_stg AS linkid,
                                                                                                                                                                                              bc_contact.middlename_stg     AS middlename,
                                                                                                                                                                                              bc_contact.retired_stg        AS retired,
                                                                                                                                                                                              bc_contact.createtime_stg     AS createtime,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))     AS name_prefix_txt,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))     AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255))    AS source,
                                                                                                                                                                                              bctl_contact.name_stg                  AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.bc_account a
                                                                                                                                                                              inner join      db_t_prod_stag.bc_accountcontact h
                                                                                                                                                                              ON              h.accountid_stg = a.id_stg
                                                                                                                                                                              inner join      db_t_prod_stag.bc_contact
                                                                                                                                                                              ON              bc_contact.id_stg = h.contactid_stg
                                                                                                                                                                              join            db_t_prod_stag.bctl_contact
                                                                                                                                                                              ON              bctl_contact.id_stg=bc_contact.subtype_stg
                                                                                                                                                                              left join       db_t_prod_stag.bc_accountcontactrole i
                                                                                                                                                                              ON              i.accountcontactid_stg = h.id_stg
                                                                                                                                                                              left join       db_t_prod_stag.bctl_accountrole j
                                                                                                                                                                              ON              j.id_stg = i.role_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_gendertype
                                                                                                                                                                              ON              bc_contact.gender_stg = bctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxfilingstatustype
                                                                                                                                                                              ON              bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxstatus
                                                                                                                                                                              ON              bc_contact.taxstatus_stg = bctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_maritalstatus
                                                                                                                                                                              ON              bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_nameprefix
                                                                                                                                                                              ON              bc_contact.prefix_stg = bctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_namesuffix
                                                                                                                                                                              ON              bc_contact.suffix_stg = bctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_user
                                                                                                                                                                              ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_credential
                                                                                                                                                                              ON              bc_user.credentialid_stg = bc_credential.id_stg
                                                                                                                                                                              WHERE           ((
                                                                                                                                                                                                        h.primarypayer_stg = 1)
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        j.name_stg = ''Payer''))
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        bc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_contact.updatetime_stg <=( :END_DTTM))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        bc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_user.updatetime_stg <= ( :END_DTTM)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /* Primary Payer and Overiding Payer Contact (this is at the Invoicestream level) */
                                                                                                                                                                              SELECT          bc_contact.updatetime_stg  AS updatetime,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255)) AS dbaname_alfa,
                                                                                                                                                                                              bc_contact.lastname_stg    AS lastname,
                                                                                                                                                                                              bc_contact.firstname_stg   AS firstname,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN (
                                                                                                                                                                                                        bc_contact.externalid_stg IS NULL) THEN bc_contact.publicid_stg
                                                                                                                                                                                                        ELSE bc_contact.externalid_stg
                                                                                                                                                                                              END                           AS publicid,
                                                                                                                                                                                              bc_contact.formername_stg     AS formername,
                                                                                                                                                                                              bc_contact.name_stg           AS name,
                                                                                                                                                                                              bc_contact.addressbookuid_stg AS linkid,
                                                                                                                                                                                              bc_contact.middlename_stg     AS middlename,
                                                                                                                                                                                              bc_contact.retired_stg        AS retired,
                                                                                                                                                                                              bc_contact.createtime_stg     AS createtime,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))     AS name_prefix_txt,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))     AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255))    AS source,
                                                                                                                                                                                              bctl_contact.name_stg                  AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.bc_account a
                                                                                                                                                                              inner join      db_t_prod_stag.bc_invoicestream b
                                                                                                                                                                              ON              a.id_stg = b.accountid_stg
                                                                                                                                                                              inner join      db_t_prod_stag.bc_accountcontact c
                                                                                                                                                                              ON              c.accountid_stg=a.id_stg
                                                                                                                                                                              inner join      db_t_prod_stag.bc_contact
                                                                                                                                                                              ON              bc_contact.id_stg = c.contactid_stg
                                                                                                                                                                              join            db_t_prod_stag.bctl_contact
                                                                                                                                                                              ON              bctl_contact.id_stg=bc_contact.subtype_stg
                                                                                                                                                                              left join       db_t_prod_stag.bc_accountcontactrole f
                                                                                                                                                                              ON              f.accountcontactid_stg = c.id_stg
                                                                                                                                                                              left join       db_t_prod_stag.bctl_accountrole g
                                                                                                                                                                              ON              g.id_stg = f.role_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_gendertype
                                                                                                                                                                              ON              bc_contact.gender_stg = bctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxfilingstatustype
                                                                                                                                                                              ON              bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxstatus
                                                                                                                                                                              ON              bc_contact.taxstatus_stg = bctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_maritalstatus
                                                                                                                                                                              ON              bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_nameprefix
                                                                                                                                                                              ON              bc_contact.prefix_stg = bctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_namesuffix
                                                                                                                                                                              ON              bc_contact.suffix_stg = bctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_user
                                                                                                                                                                              ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_credential
                                                                                                                                                                              ON              bc_user.credentialid_stg = bc_credential.id_stg
                                                                                                                                                                              WHERE           ((
                                                                                                                                                                                                        b.overridingpayer_alfa_stg IS NULL
                                                                                                                                                                                                        AND             c.primarypayer_stg = 1)
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        b.overridingpayer_alfa_stg IS NOT NULL))
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        bc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_contact.updatetime_stg <=( :END_DTTM))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        bc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_user.updatetime_stg <= ( :END_DTTM)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /* DB_T_PROD_STAG.pc_contact dropzone query*/
                                                                                                                                                                              SELECT          pc_contact.updatetime_stg           AS updatetime,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255))          AS dbaname_alfa,
                                                                                                                                                                                              pc_contact.lastname_stg             AS lastname,
                                                                                                                                                                                              pc_contact.firstname_stg            AS firstname,
                                                                                                                                                                                              pc_contact.publicid_stg             AS publicid,
                                                                                                                                                                                              pc_contact.formername_stg           AS formername,
                                                                                                                                                                                              pc_contact.name_stg                 AS name,
                                                                                                                                                                                              pc_contact.addressbookuid_stg       AS linkid,
                                                                                                                                                                                              pc_contact.middlename_stg           AS middlename,
                                                                                                                                                                                              pc_contact.retired_stg              AS retired,
                                                                                                                                                                                              pc_contact.createtime_stg           AS createtime,
                                                                                                                                                                                              pctl_nameprefix.typecode_stg        AS name_prefix_txt,
                                                                                                                                                                                              pctl_namesuffix.typecode_stg        AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255)) AS source,
                                                                                                                                                                                              pctl_contact.name_stg               AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.pc_contact
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_contact
                                                                                                                                                                              ON              pctl_contact.id_stg = pc_contact.subtype_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_gendertype
                                                                                                                                                                              ON              pc_contact.gender_stg = pctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_taxfilingstatustype
                                                                                                                                                                              ON              pc_contact.taxfilingstatus_stg = pctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_taxstatus
                                                                                                                                                                              ON              pc_contact.taxstatus_stg = pctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_maritalstatus
                                                                                                                                                                              ON              pc_contact.maritalstatus_stg = pctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_nameprefix
                                                                                                                                                                              ON              pc_contact.prefix_stg = pctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_namesuffix
                                                                                                                                                                              ON              pc_contact.suffix_stg = pctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_user
                                                                                                                                                                              ON              pc_user.contactid_stg = pc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_credential
                                                                                                                                                                              ON              pc_user.credentialid_stg = pc_credential.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_policyperiod
                                                                                                                                                                              ON              pc_policyperiod.pnicontactdenorm_stg = pc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_effectivedatedfields
                                                                                                                                                                              ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_producercode
                                                                                                                                                                              ON              pc_producercode.id_stg=pc_effectivedatedfields.producercodeid_stg
                                                                                                                                                                              WHERE           pctl_contact.typecode_stg = (''UserContact'')
                                                                                                                                                                              AND
                                                                                                                                                                                              /*  below condition added to avoid duplicates*/
                                                                                                                                                                                              pc_contact.publicid_stg NOT IN (''default_data:1'',
                                                                                                                                                                                                        ''systemTables:1'',
                                                                                                                                                                                                        ''systemTables:2'')
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        pc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             pc_contact.updatetime_stg <= ( :END_DTTM))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        pc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             pc_user.updatetime_stg <= ( :END_DTTM)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /* DB_T_PROD_STAG.cc_contact dropzone query */
                                                                                                                                                                              SELECT DISTINCT cc_contact.updatetime_stg           AS updatetime,
                                                                                                                                                                                              cc_contact.dbaname_alfa_stg         AS dbaname_alfa,
                                                                                                                                                                                              cc_contact.lastname_stg             AS lastname,
                                                                                                                                                                                              cc_contact.firstname_stg            AS firstname,
                                                                                                                                                                                              cc_contact.publicid_stg             AS publicid,
                                                                                                                                                                                              cc_contact.formername_stg           AS formername,
                                                                                                                                                                                              cc_contact.name_stg                 AS name,
                                                                                                                                                                                              cc_contact.addressbookuid_stg       AS linkid,
                                                                                                                                                                                              cc_contact.middlename_stg           AS middlename,
                                                                                                                                                                                              cc_contact.retired_stg              AS retired,
                                                                                                                                                                                              cc_contact.createtime_stg           AS createtime,
                                                                                                                                                                                              cctl_nameprefix.typecode_stg        AS name_prefix_txt,
                                                                                                                                                                                              cctl_namesuffix.typecode_stg        AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255)) AS source,
                                                                                                                                                                                              cctl_contact.name_stg               AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.cc_contact
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_contact
                                                                                                                                                                              ON              cctl_contact.id_stg = cc_contact.subtype_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_gendertype
                                                                                                                                                                              ON              cc_contact.gender_stg = cctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_taxfilingstatustype
                                                                                                                                                                              ON              cc_contact.taxfilingstatus_stg = cctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_taxstatus
                                                                                                                                                                              ON              cc_contact.taxstatus_stg = cctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_maritalstatus
                                                                                                                                                                              ON              cc_contact.maritalstatus_stg = cctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_nameprefix
                                                                                                                                                                              ON              cc_contact.prefix_stg = cctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_namesuffix
                                                                                                                                                                              ON              cc_contact.suffix_stg = cctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_user
                                                                                                                                                                              ON              cc_user.contactid_stg = cc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_credential
                                                                                                                                                                              ON              cc_user.credentialid_stg = cc_credential.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_claimcontact
                                                                                                                                                                              ON              cc_contact.id_stg=cc_claimcontact.contactid_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_claimcontactrole
                                                                                                                                                                              ON              cc_claimcontact.id_stg=cc_claimcontactrole.claimcontactid_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_incident
                                                                                                                                                                              ON              cc_claimcontactrole.claimcontactid_stg =cc_incident.id_stg
                                                                                                                                                                              WHERE           (
                                                                                                                                                                                                        cc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                              AND             cc_contact.updatetime_stg <= ( :END_DTTM) )
                                                                                                                                                                              OR              (
                                                                                                                                                                                                        cc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                              AND             cc_user.updatetime_stg <=( :END_DTTM) )
                                                                                                                                                                              UNION
                                                                                                                                                                              /* ab_contact dropzone query*/
                                                                                                                                                                              SELECT          ab_abcontact.updatetime_stg            AS updatetime,
                                                                                                                                                                                              ab_abcontact.dbaname_alfa_stg          AS dbaname_alfa,
                                                                                                                                                                                              ab_abcontact.lastname_stg              AS lastname,
                                                                                                                                                                                              ab_abcontact.firstname_stg             AS firstname,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255))             AS publicid,
                                                                                                                                                                                              ab_abcontact.formername_stg            AS formername,
                                                                                                                                                                                              ab_abcontact.name_stg                  AS name,
                                                                                                                                                                                              ab_abcontact.linkid_stg                AS linkid,
                                                                                                                                                                                              ab_abcontact.middlename_stg            AS middlename,
                                                                                                                                                                                              ab_abcontact.retired_stg               AS retired,
                                                                                                                                                                                              ab_abcontact.createtime_stg            AS createtime,
                                                                                                                                                                                              abtl_nameprefix.typecode_stg           AS name_prefix_txt,
                                                                                                                                                                                              abtl_namesuffix.typecode_stg           AS name_sfx_txt,
                                                                                                                                                                                              cast(''ContactManager'' AS VARCHAR(255)) AS source,
                                                                                                                                                                                              abtl_abcontact.name_stg                AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.ab_abcontact
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_abcontact
                                                                                                                                                                              ON              abtl_abcontact.id_stg = ab_abcontact.subtype_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_gendertype
                                                                                                                                                                              ON              ab_abcontact.gender_stg = abtl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_taxfilingstatustype
                                                                                                                                                                              ON              ab_abcontact.taxfilingstatus_stg = abtl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_taxstatus
                                                                                                                                                                              ON              ab_abcontact.taxstatus_stg = abtl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_maritalstatus
                                                                                                                                                                              ON              ab_abcontact.maritalstatus_stg = abtl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_nameprefix
                                                                                                                                                                              ON              ab_abcontact.prefix_stg = abtl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_namesuffix
                                                                                                                                                                              ON              ab_abcontact.suffix_stg = abtl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.ab_user
                                                                                                                                                                              ON              ab_user.contactid_stg = ab_abcontact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.ab_credential
                                                                                                                                                                              ON              ab_user.credentialid_stg = ab_credential.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_occupation
                                                                                                                                                                              ON              abtl_occupation.id_stg = ab_abcontact.occupation_alfa_stg
                                                                                                                                                                              WHERE           ab_abcontact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                              AND             ab_abcontact.updatetime_stg <= ( :END_DTTM) ) ab_cnt_inner ) ab_abcontact
                                                                                                                                WHERE           ab_abcontact.tl_cnt_name IN (''Person'',
                                                                                                                                                                             ''Adjudicator'',
                                                                                                                                                                             ''User Contact'',
                                                                                                                                                                             ''Vendor (Person)'',
                                                                                                                                                                             ''Attorney'',
                                                                                                                                                                             ''Doctor'',
                                                                                                                                                                             ''Policy Person'',
                                                                                                                                                                             ''Lodging (Person)'')
                                                                                                                                AND             (
                                                                                                                                                                source = ''ClaimCenter''
                                                                                                                                                AND             publicid IS NOT NULL) qualify row_number() over ( PARTITION BY publicid ORDER BY src_strt_dt DESC)=1
                                                                                                                                UNION
                                                                                                                                SELECT DISTINCT upper(ab_abcontact.linkid) AS linkid ,
                                                                                                                                                CASE
                                                                                                                                                                WHEN ab_abcontact.firstname IS NULL THEN ''UNK''
                                                                                                                                                                ELSE ab_abcontact.firstname
                                                                                                                                                END AS firstname ,
                                                                                                                                                ab_abcontact.middlename ,
                                                                                                                                                CASE
                                                                                                                                                                WHEN ab_abcontact.lastname IS NULL THEN ''UNK''
                                                                                                                                                                ELSE ab_abcontact.lastname
                                                                                                                                                END                          AS lastname ,
                                                                                                                                                ab_abcontact.name_prefix_txt AS prefix ,
                                                                                                                                                ab_abcontact.name_sfx_txt    AS suffix ,
                                                                                                                                                ab_abcontact.name ,
                                                                                                                                                ab_abcontact.formername ,
                                                                                                                                                CASE
                                                                                                                                                                WHEN ab_abcontact.firstname IS NOT NULL
                                                                                                                                                                AND             ab_abcontact.lastname IS NOT NULL
                                                                                                                                                                AND             ab_abcontact.dbaname_alfa IS NOT NULL THEN ''DBA''
                                                                                                                                                                WHEN ab_abcontact.firstname IS NOT NULL
                                                                                                                                                                AND             ab_abcontact.lastname IS NOT NULL THEN ''LGL''
                                                                                                                                                                ELSE ''UNK''
                                                                                                                                                END                            AS name_type_cd ,
                                                                                                                                                publicid                       AS publicid ,
                                                                                                                                                source                         AS src,
                                                                                                                                                ab_abcontact.updatetime        AS src_strt_dt,
                                                                                                                                                to_date(''99991231'',''YYYYMMDD'') AS src_end_dt,
                                                                                                                                                ab_abcontact.createtime        AS indiv_name_strt_dttm,
                                                                                                                                                to_date(''99991231'',''YYYYMMDD'') AS indiv_name_end_dttm,
                                                                                                                                                retired
                                                                                                                                FROM            (
                                                                                                                                                       SELECT ab_cnt_inner.updatetime,
                                                                                                                                                              ab_cnt_inner.dbaname_alfa,
                                                                                                                                                              ab_cnt_inner.lastname ,
                                                                                                                                                              ab_cnt_inner.firstname ,
                                                                                                                                                              ab_cnt_inner.publicid,
                                                                                                                                                              ab_cnt_inner.formername ,
                                                                                                                                                              ab_cnt_inner.name ,
                                                                                                                                                              ab_cnt_inner.linkid ,
                                                                                                                                                              ab_cnt_inner.middlename,
                                                                                                                                                              ab_cnt_inner.retired,
                                                                                                                                                              ab_cnt_inner.createtime ,
                                                                                                                                                              ab_cnt_inner.name_prefix_txt,
                                                                                                                                                              ab_cnt_inner.name_sfx_txt,
                                                                                                                                                              ab_cnt_inner.source,
                                                                                                                                                              ab_cnt_inner.tl_cnt_name
                                                                                                                                                       FROM   (
                                                                                                                                                                              /* DB_T_PROD_STAG.bc_contact dropzone query */
                                                                                                                                                                              SELECT          bc_contact.updatetime_stg                           AS updatetime,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255))                          AS dbaname_alfa,
                                                                                                                                                                                              bc_contact.lastname_stg                             AS lastname,
                                                                                                                                                                                              bc_contact.firstname_stg                            AS firstname,
                                                                                                                                                                                              cast(bc_contact.publicid_stg AS VARCHAR(100))       AS publicid,
                                                                                                                                                                                              bc_contact.formername_stg                           AS formername,
                                                                                                                                                                                              bc_contact.name_stg                                 AS name,
                                                                                                                                                                                              cast(bc_contact.addressbookuid_stg AS VARCHAR(100)) AS linkid,
                                                                                                                                                                                              bc_contact.middlename_stg                           AS middlename,
                                                                                                                                                                                              bc_contact.retired_stg                              AS retired,
                                                                                                                                                                                              bc_contact.createtime_stg                           AS createtime,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))                           AS name_prefix_txt,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))                           AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255))                          AS source,
                                                                                                                                                                                              bctl_contact.name_stg                                        AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.bc_contact
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_contact
                                                                                                                                                                              ON              bctl_contact.id_stg = bc_contact.subtype_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_gendertype
                                                                                                                                                                              ON              bc_contact.gender_stg = bctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxfilingstatustype
                                                                                                                                                                              ON              bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxstatus
                                                                                                                                                                              ON              bc_contact.taxstatus_stg = bctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_maritalstatus
                                                                                                                                                                              ON              bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_nameprefix
                                                                                                                                                                              ON              bc_contact.prefix_stg = bctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_namesuffix
                                                                                                                                                                              ON              bc_contact.suffix_stg = bctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_user
                                                                                                                                                                              ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_credential
                                                                                                                                                                              ON              bc_user.credentialid_stg = bc_credential.id_stg
                                                                                                                                                                              WHERE           bctl_contact.typecode_stg = (''UserContact'')
                                                                                                                                                                              AND
                                                                                                                                                                                              /*  below condition added to avoid duplicates*/
                                                                                                                                                                                              bc_contact.publicid_stg NOT IN (''default_data:1'',
                                                                                                                                                                                                        ''systemTables:1'',
                                                                                                                                                                                                        ''systemTables:2'')
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        bc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_contact.updatetime_stg <=( :END_DTTM))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        bc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_user.updatetime_stg <= ( :END_DTTM)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /* Primary and Secondary Payer contact (this is at the Account level)  */
                                                                                                                                                                              SELECT          bc_contact.updatetime_stg  AS updatetime,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255)) AS dbaname_alfa,
                                                                                                                                                                                              bc_contact.lastname_stg    AS lastname,
                                                                                                                                                                                              bc_contact.firstname_stg   AS firstname,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN(
                                                                                                                                                                                                        bc_contact.externalid_stg IS NOT NULL) THEN bc_contact.externalid_stg
                                                                                                                                                                                                        ELSE bc_contact.publicid_stg
                                                                                                                                                                                              END                           AS publicid,
                                                                                                                                                                                              bc_contact.formername_stg     AS formername,
                                                                                                                                                                                              bc_contact.name_stg           AS name,
                                                                                                                                                                                              bc_contact.addressbookuid_stg AS linkid,
                                                                                                                                                                                              bc_contact.middlename_stg     AS middlename,
                                                                                                                                                                                              bc_contact.retired_stg        AS retired,
                                                                                                                                                                                              bc_contact.createtime_stg     AS createtime,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))     AS name_prefix_txt,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))     AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255))    AS source,
                                                                                                                                                                                              bctl_contact.name_stg                  AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.bc_account a
                                                                                                                                                                              inner join      db_t_prod_stag.bc_accountcontact h
                                                                                                                                                                              ON              h.accountid_stg = a.id_stg
                                                                                                                                                                              inner join      db_t_prod_stag.bc_contact
                                                                                                                                                                              ON              bc_contact.id_stg = h.contactid_stg
                                                                                                                                                                              join            db_t_prod_stag.bctl_contact
                                                                                                                                                                              ON              bctl_contact.id_stg=bc_contact.subtype_stg
                                                                                                                                                                              left join       db_t_prod_stag.bc_accountcontactrole i
                                                                                                                                                                              ON              i.accountcontactid_stg = h.id_stg
                                                                                                                                                                              left join       db_t_prod_stag.bctl_accountrole j
                                                                                                                                                                              ON              j.id_stg = i.role_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_gendertype
                                                                                                                                                                              ON              bc_contact.gender_stg = bctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxfilingstatustype
                                                                                                                                                                              ON              bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxstatus
                                                                                                                                                                              ON              bc_contact.taxstatus_stg = bctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_maritalstatus
                                                                                                                                                                              ON              bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_nameprefix
                                                                                                                                                                              ON              bc_contact.prefix_stg = bctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_namesuffix
                                                                                                                                                                              ON              bc_contact.suffix_stg = bctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_user
                                                                                                                                                                              ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_credential
                                                                                                                                                                              ON              bc_user.credentialid_stg = bc_credential.id_stg
                                                                                                                                                                              WHERE           ((
                                                                                                                                                                                                        h.primarypayer_stg = 1)
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        j.name_stg = ''Payer''))
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        bc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_contact.updatetime_stg <=( :END_DTTM))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        bc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_user.updatetime_stg <= ( :END_DTTM)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /* Primary Payer and Overiding Payer Contact (this is at the Invoicestream level)*/
                                                                                                                                                                              SELECT          bc_contact.updatetime_stg  AS updatetime,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255)) AS dbaname_alfa,
                                                                                                                                                                                              bc_contact.lastname_stg    AS lastname,
                                                                                                                                                                                              bc_contact.firstname_stg   AS firstname,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN (
                                                                                                                                                                                                        bc_contact.externalid_stg IS NULL) THEN bc_contact.publicid_stg
                                                                                                                                                                                                        ELSE bc_contact.externalid_stg
                                                                                                                                                                                              END                           AS publicid,
                                                                                                                                                                                              bc_contact.formername_stg     AS formername,
                                                                                                                                                                                              bc_contact.name_stg           AS name,
                                                                                                                                                                                              bc_contact.addressbookuid_stg AS linkid,
                                                                                                                                                                                              bc_contact.middlename_stg     AS middlename,
                                                                                                                                                                                              bc_contact.retired_stg        AS retired,
                                                                                                                                                                                              bc_contact.createtime_stg     AS createtime,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))     AS name_prefix_txt,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))     AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255))    AS source,
                                                                                                                                                                                              bctl_contact.name_stg                  AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.bc_account a
                                                                                                                                                                              inner join      db_t_prod_stag.bc_invoicestream b
                                                                                                                                                                              ON              a.id_stg = b.accountid_stg
                                                                                                                                                                              inner join      db_t_prod_stag.bc_accountcontact c
                                                                                                                                                                              ON              c.accountid_stg=a.id_stg
                                                                                                                                                                              inner join      db_t_prod_stag.bc_contact
                                                                                                                                                                              ON              bc_contact.id_stg = c.contactid_stg
                                                                                                                                                                              join            db_t_prod_stag.bctl_contact
                                                                                                                                                                              ON              bctl_contact.id_stg=bc_contact.subtype_stg
                                                                                                                                                                              left join       db_t_prod_stag.bc_accountcontactrole f
                                                                                                                                                                              ON              f.accountcontactid_stg = c.id_stg
                                                                                                                                                                              left join       db_t_prod_stag.bctl_accountrole g
                                                                                                                                                                              ON              g.id_stg = f.role_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_gendertype
                                                                                                                                                                              ON              bc_contact.gender_stg = bctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxfilingstatustype
                                                                                                                                                                              ON              bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxstatus
                                                                                                                                                                              ON              bc_contact.taxstatus_stg = bctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_maritalstatus
                                                                                                                                                                              ON              bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_nameprefix
                                                                                                                                                                              ON              bc_contact.prefix_stg = bctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_namesuffix
                                                                                                                                                                              ON              bc_contact.suffix_stg = bctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_user
                                                                                                                                                                              ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_credential
                                                                                                                                                                              ON              bc_user.credentialid_stg = bc_credential.id_stg
                                                                                                                                                                              WHERE           ((
                                                                                                                                                                                                        b.overridingpayer_alfa_stg IS NULL
                                                                                                                                                                                                        AND             c.primarypayer_stg = 1)
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        b.overridingpayer_alfa_stg IS NOT NULL))
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        bc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_contact.updatetime_stg <=( :END_DTTM))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        bc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_user.updatetime_stg <= ( :END_DTTM)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /* DB_T_PROD_STAG.pc_contact dropzone query */
                                                                                                                                                                              SELECT          pc_contact.updatetime_stg           AS updatetime,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255))          AS dbaname_alfa,
                                                                                                                                                                                              pc_contact.lastname_stg             AS lastname,
                                                                                                                                                                                              pc_contact.firstname_stg            AS firstname,
                                                                                                                                                                                              pc_contact.publicid_stg             AS publicid,
                                                                                                                                                                                              pc_contact.formername_stg           AS formername,
                                                                                                                                                                                              pc_contact.name_stg                 AS name,
                                                                                                                                                                                              pc_contact.addressbookuid_stg       AS linkid,
                                                                                                                                                                                              pc_contact.middlename_stg           AS middlename,
                                                                                                                                                                                              pc_contact.retired_stg              AS retired,
                                                                                                                                                                                              pc_contact.createtime_stg           AS createtime,
                                                                                                                                                                                              pctl_nameprefix.typecode_stg        AS name_prefix_txt,
                                                                                                                                                                                              pctl_namesuffix.typecode_stg        AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255)) AS source,
                                                                                                                                                                                              pctl_contact.name_stg               AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.pc_contact
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_contact
                                                                                                                                                                              ON              pctl_contact.id_stg = pc_contact.subtype_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_gendertype
                                                                                                                                                                              ON              pc_contact.gender_stg = pctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_taxfilingstatustype
                                                                                                                                                                              ON              pc_contact.taxfilingstatus_stg = pctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_taxstatus
                                                                                                                                                                              ON              pc_contact.taxstatus_stg = pctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_maritalstatus
                                                                                                                                                                              ON              pc_contact.maritalstatus_stg = pctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_nameprefix
                                                                                                                                                                              ON              pc_contact.prefix_stg = pctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_namesuffix
                                                                                                                                                                              ON              pc_contact.suffix_stg = pctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_user
                                                                                                                                                                              ON              pc_user.contactid_stg = pc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_credential
                                                                                                                                                                              ON              pc_user.credentialid_stg = pc_credential.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_policyperiod
                                                                                                                                                                              ON              pc_policyperiod.pnicontactdenorm_stg = pc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_effectivedatedfields
                                                                                                                                                                              ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_producercode
                                                                                                                                                                              ON              pc_producercode.id_stg=pc_effectivedatedfields.producercodeid_stg
                                                                                                                                                                              WHERE           pctl_contact.typecode_stg = (''UserContact'')
                                                                                                                                                                              AND
                                                                                                                                                                                              /*  below condition added to avoid duplicates*/
                                                                                                                                                                                              pc_contact.publicid_stg NOT IN (''default_data:1'',
                                                                                                                                                                                                        ''systemTables:1'',
                                                                                                                                                                                                        ''systemTables:2'')
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        pc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             pc_contact.updatetime_stg <= ( :END_DTTM))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        pc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             pc_user.updatetime_stg <= ( :END_DTTM)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /* DB_T_PROD_STAG.cc_contact dropzone query */
                                                                                                                                                                              SELECT DISTINCT cc_contact.updatetime_stg           AS updatetime,
                                                                                                                                                                                              cc_contact.dbaname_alfa_stg         AS dbaname_alfa,
                                                                                                                                                                                              cc_contact.lastname_stg             AS lastname,
                                                                                                                                                                                              cc_contact.firstname_stg            AS firstname,
                                                                                                                                                                                              cc_contact.publicid_stg             AS publicid,
                                                                                                                                                                                              cc_contact.formername_stg           AS formername,
                                                                                                                                                                                              cc_contact.name_stg                 AS name,
                                                                                                                                                                                              cc_contact.addressbookuid_stg       AS linkid,
                                                                                                                                                                                              cc_contact.middlename_stg           AS middlename,
                                                                                                                                                                                              cc_contact.retired_stg              AS retired,
                                                                                                                                                                                              cc_contact.createtime_stg           AS createtime,
                                                                                                                                                                                              cctl_nameprefix.typecode_stg        AS name_prefix_txt,
                                                                                                                                                                                              cctl_namesuffix.typecode_stg        AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255)) AS source,
                                                                                                                                                                                              cctl_contact.name_stg               AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.cc_contact
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_contact
                                                                                                                                                                              ON              cctl_contact.id_stg = cc_contact.subtype_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_gendertype
                                                                                                                                                                              ON              cc_contact.gender_stg = cctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_taxfilingstatustype
                                                                                                                                                                              ON              cc_contact.taxfilingstatus_stg = cctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_taxstatus
                                                                                                                                                                              ON              cc_contact.taxstatus_stg = cctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_maritalstatus
                                                                                                                                                                              ON              cc_contact.maritalstatus_stg = cctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_nameprefix
                                                                                                                                                                              ON              cc_contact.prefix_stg = cctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_namesuffix
                                                                                                                                                                              ON              cc_contact.suffix_stg = cctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_user
                                                                                                                                                                              ON              cc_user.contactid_stg = cc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_credential
                                                                                                                                                                              ON              cc_user.credentialid_stg = cc_credential.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_claimcontact
                                                                                                                                                                              ON              cc_contact.id_stg=cc_claimcontact.contactid_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_claimcontactrole
                                                                                                                                                                              ON              cc_claimcontact.id_stg=cc_claimcontactrole.claimcontactid_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_incident
                                                                                                                                                                              ON              cc_claimcontactrole.claimcontactid_stg =cc_incident.id_stg
                                                                                                                                                                              WHERE           (
                                                                                                                                                                                                        cc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                              AND             cc_contact.updatetime_stg <= ( :END_DTTM) )
                                                                                                                                                                              OR              (
                                                                                                                                                                                                        cc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                              AND             cc_user.updatetime_stg <=( :END_DTTM) )
                                                                                                                                                                              UNION
                                                                                                                                                                              /* ab_contact dropzone query */
                                                                                                                                                                              SELECT          ab_abcontact.updatetime_stg            AS updatetime,
                                                                                                                                                                                              ab_abcontact.dbaname_alfa_stg          AS dbaname_alfa,
                                                                                                                                                                                              ab_abcontact.lastname_stg              AS lastname,
                                                                                                                                                                                              ab_abcontact.firstname_stg             AS firstname,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255))             AS publicid,
                                                                                                                                                                                              ab_abcontact.formername_stg            AS formername,
                                                                                                                                                                                              ab_abcontact.name_stg                  AS name,
                                                                                                                                                                                              ab_abcontact.linkid_stg                AS linkid,
                                                                                                                                                                                              ab_abcontact.middlename_stg            AS middlename,
                                                                                                                                                                                              ab_abcontact.retired_stg               AS retired,
                                                                                                                                                                                              ab_abcontact.createtime_stg            AS createtime,
                                                                                                                                                                                              abtl_nameprefix.typecode_stg           AS name_prefix_txt,
                                                                                                                                                                                              abtl_namesuffix.typecode_stg           AS name_sfx_txt,
                                                                                                                                                                                              cast(''ContactManager'' AS VARCHAR(255)) AS source,
                                                                                                                                                                                              abtl_abcontact.name_stg                AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.ab_abcontact
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_abcontact
                                                                                                                                                                              ON              abtl_abcontact.id_stg = ab_abcontact.subtype_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_gendertype
                                                                                                                                                                              ON              ab_abcontact.gender_stg = abtl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_taxfilingstatustype
                                                                                                                                                                              ON              ab_abcontact.taxfilingstatus_stg = abtl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_taxstatus
                                                                                                                                                                              ON              ab_abcontact.taxstatus_stg = abtl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_maritalstatus
                                                                                                                                                                              ON              ab_abcontact.maritalstatus_stg = abtl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_nameprefix
                                                                                                                                                                              ON              ab_abcontact.prefix_stg = abtl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_namesuffix
                                                                                                                                                                              ON              ab_abcontact.suffix_stg = abtl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.ab_user
                                                                                                                                                                              ON              ab_user.contactid_stg = ab_abcontact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.ab_credential
                                                                                                                                                                              ON              ab_user.credentialid_stg = ab_credential.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_occupation
                                                                                                                                                                              ON              abtl_occupation.id_stg = ab_abcontact.occupation_alfa_stg
                                                                                                                                                                              WHERE           ab_abcontact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                              AND             ab_abcontact.updatetime_stg <= ( :END_DTTM) ) ab_cnt_inner ) ab_abcontact
                                                                                                                                WHERE           ab_abcontact.tl_cnt_name IN (''Person'',
                                                                                                                                                                             ''Adjudicator'',
                                                                                                                                                                             ''User Contact'',
                                                                                                                                                                             ''Vendor (Person)'',
                                                                                                                                                                             ''Attorney'',
                                                                                                                                                                             ''Doctor'',
                                                                                                                                                                             ''Policy Person'',
                                                                                                                                                                             ''Lodging (Person)'')
                                                                                                                                AND             (
                                                                                                                                                                source = ''ContactManager''
                                                                                                                                                AND             linkid IS NOT NULL) qualify row_number() over ( PARTITION BY linkid ORDER BY src_strt_dt DESC)=1
                                                                                                                                UNION
                                                                                                                                /* ALIAS FLOW */
                                                                                                                                SELECT DISTINCT cast(upper(ab_abcontact.linkid) AS VARCHAR(100))AS linkid ,
                                                                                                                                                ab_abcontact.firstname ,
                                                                                                                                                ab_abcontact.middlename ,
                                                                                                                                                ab_abcontact.formername      AS lastname ,
                                                                                                                                                ab_abcontact.name_prefix_txt AS prefix ,
                                                                                                                                                ab_abcontact.name_sfx_txt    AS suffix ,
                                                                                                                                                ab_abcontact.name ,
                                                                                                                                                ab_abcontact.formername ,
                                                                                                                                                CASE
                                                                                                                                                                WHEN ab_abcontact.firstname IS NOT NULL
                                                                                                                                                                AND             ab_abcontact.formername IS NOT NULL THEN ''ALS''
                                                                                                                                                                ELSE ''UNK''
                                                                                                                                                END                             AS name_type_cd ,
                                                                                                                                                cast(publicid AS VARCHAR(100))     publicid ,
                                                                                                                                                source                          AS src,
                                                                                                                                                ab_abcontact.updatetime         AS src_strt_dt,
                                                                                                                                                to_date(''99991231'', ''YYYYMMDD'') AS src_end_dt,
                                                                                                                                                ab_abcontact.createtime         AS indiv_name_strt_dttm,
                                                                                                                                                to_date(''99991231'',''YYYYMMDD'')  AS indiv_name_end_dttm,
                                                                                                                                                retired
                                                                                                                                FROM            (
                                                                                                                                                       SELECT ab_cnt_inner.updatetime,
                                                                                                                                                              ab_cnt_inner.dbaname_alfa,
                                                                                                                                                              ab_cnt_inner.lastname ,
                                                                                                                                                              ab_cnt_inner.firstname ,
                                                                                                                                                              ab_cnt_inner.publicid,
                                                                                                                                                              ab_cnt_inner.formername ,
                                                                                                                                                              ab_cnt_inner.name ,
                                                                                                                                                              ab_cnt_inner.linkid ,
                                                                                                                                                              ab_cnt_inner.middlename,
                                                                                                                                                              ab_cnt_inner.retired,
                                                                                                                                                              ab_cnt_inner.createtime ,
                                                                                                                                                              ab_cnt_inner.name_prefix_txt,
                                                                                                                                                              ab_cnt_inner.name_sfx_txt,
                                                                                                                                                              ab_cnt_inner.source,
                                                                                                                                                              ab_cnt_inner.tl_cnt_name
                                                                                                                                                       FROM   (
                                                                                                                                                                              /* DB_T_PROD_STAG.bc_contact dropzone query  */
                                                                                                                                                                              SELECT          bc_contact.updatetime_stg                           AS updatetime,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255))                          AS dbaname_alfa,
                                                                                                                                                                                              bc_contact.lastname_stg                             AS lastname,
                                                                                                                                                                                              bc_contact.firstname_stg                            AS firstname,
                                                                                                                                                                                              cast(bc_contact.publicid_stg AS VARCHAR(100))       AS publicid,
                                                                                                                                                                                              bc_contact.formername_stg                           AS formername,
                                                                                                                                                                                              bc_contact.name_stg                                 AS name,
                                                                                                                                                                                              cast(bc_contact.addressbookuid_stg AS VARCHAR(100)) AS linkid,
                                                                                                                                                                                              bc_contact.middlename_stg                           AS middlename,
                                                                                                                                                                                              bc_contact.retired_stg                              AS retired,
                                                                                                                                                                                              bc_contact.createtime_stg                           AS createtime,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))                           AS name_prefix_txt,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))                           AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255))                          AS source,
                                                                                                                                                                                              bctl_contact.name_stg                                        AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.bc_contact
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_contact
                                                                                                                                                                              ON              bctl_contact.id_stg = bc_contact.subtype_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_gendertype
                                                                                                                                                                              ON              bc_contact.gender_stg = bctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxfilingstatustype
                                                                                                                                                                              ON              bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxstatus
                                                                                                                                                                              ON              bc_contact.taxstatus_stg = bctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_maritalstatus
                                                                                                                                                                              ON              bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_nameprefix
                                                                                                                                                                              ON              bc_contact.prefix_stg = bctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_namesuffix
                                                                                                                                                                              ON              bc_contact.suffix_stg = bctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_user
                                                                                                                                                                              ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_credential
                                                                                                                                                                              ON              bc_user.credentialid_stg = bc_credential.id_stg
                                                                                                                                                                              WHERE           bctl_contact.typecode_stg = (''UserContact'')
                                                                                                                                                                              AND
                                                                                                                                                                                              /*  below condition added to avoid duplicates*/
                                                                                                                                                                                              bc_contact.publicid_stg NOT IN (''default_data:1'',
                                                                                                                                                                                                        ''systemTables:1'',
                                                                                                                                                                                                        ''systemTables:2'')
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        bc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_contact.updatetime_stg <=( :END_DTTM))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        bc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_user.updatetime_stg <= ( :END_DTTM)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /* Primary and Secondary Payer contact (this is at the Account level)  */
                                                                                                                                                                              SELECT          bc_contact.updatetime_stg  AS updatetime,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255)) AS dbaname_alfa,
                                                                                                                                                                                              bc_contact.lastname_stg    AS lastname,
                                                                                                                                                                                              bc_contact.firstname_stg   AS firstname,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN(
                                                                                                                                                                                                        bc_contact.externalid_stg IS NOT NULL) THEN bc_contact.externalid_stg
                                                                                                                                                                                                        ELSE bc_contact.publicid_stg
                                                                                                                                                                                              END                           AS publicid,
                                                                                                                                                                                              bc_contact.formername_stg     AS formername,
                                                                                                                                                                                              bc_contact.name_stg           AS name,
                                                                                                                                                                                              bc_contact.addressbookuid_stg AS linkid,
                                                                                                                                                                                              bc_contact.middlename_stg     AS middlename,
                                                                                                                                                                                              bc_contact.retired_stg        AS retired,
                                                                                                                                                                                              bc_contact.createtime_stg     AS createtime,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))     AS name_prefix_txt,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))     AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255))    AS source,
                                                                                                                                                                                              bctl_contact.name_stg                  AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.bc_account a
                                                                                                                                                                              inner join      db_t_prod_stag.bc_accountcontact h
                                                                                                                                                                              ON              h.accountid_stg = a.id_stg
                                                                                                                                                                              inner join      db_t_prod_stag.bc_contact
                                                                                                                                                                              ON              bc_contact.id_stg = h.contactid_stg
                                                                                                                                                                              join            db_t_prod_stag.bctl_contact
                                                                                                                                                                              ON              bctl_contact.id_stg=bc_contact.subtype_stg
                                                                                                                                                                              left join       db_t_prod_stag.bc_accountcontactrole i
                                                                                                                                                                              ON              i.accountcontactid_stg = h.id_stg
                                                                                                                                                                              left join       db_t_prod_stag.bctl_accountrole j
                                                                                                                                                                              ON              j.id_stg = i.role_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_gendertype
                                                                                                                                                                              ON              bc_contact.gender_stg = bctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxfilingstatustype
                                                                                                                                                                              ON              bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxstatus
                                                                                                                                                                              ON              bc_contact.taxstatus_stg = bctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_maritalstatus
                                                                                                                                                                              ON              bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_nameprefix
                                                                                                                                                                              ON              bc_contact.prefix_stg = bctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_namesuffix
                                                                                                                                                                              ON              bc_contact.suffix_stg = bctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_user
                                                                                                                                                                              ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_credential
                                                                                                                                                                              ON              bc_user.credentialid_stg = bc_credential.id_stg
                                                                                                                                                                              WHERE           ((
                                                                                                                                                                                                        h.primarypayer_stg = 1)
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        j.name_stg = ''Payer''))
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        bc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_contact.updatetime_stg <=( :END_DTTM))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        bc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_user.updatetime_stg <= ( :END_DTTM)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /* Primary Payer and Overiding Payer Contact (this is at the Invoicestream level) */
                                                                                                                                                                              SELECT          bc_contact.updatetime_stg  AS updatetime,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255)) AS dbaname_alfa,
                                                                                                                                                                                              bc_contact.lastname_stg    AS lastname,
                                                                                                                                                                                              bc_contact.firstname_stg   AS firstname,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN (
                                                                                                                                                                                                        bc_contact.externalid_stg IS NULL) THEN bc_contact.publicid_stg
                                                                                                                                                                                                        ELSE bc_contact.externalid_stg
                                                                                                                                                                                              END                           AS publicid,
                                                                                                                                                                                              bc_contact.formername_stg     AS formername,
                                                                                                                                                                                              bc_contact.name_stg           AS name,
                                                                                                                                                                                              bc_contact.addressbookuid_stg AS linkid,
                                                                                                                                                                                              bc_contact.middlename_stg     AS middlename,
                                                                                                                                                                                              bc_contact.retired_stg        AS retired,
                                                                                                                                                                                              bc_contact.createtime_stg     AS createtime,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))     AS name_prefix_txt,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))     AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255))    AS source,
                                                                                                                                                                                              bctl_contact.name_stg                  AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.bc_account a
                                                                                                                                                                              inner join      db_t_prod_stag.bc_invoicestream b
                                                                                                                                                                              ON              a.id_stg = b.accountid_stg
                                                                                                                                                                              inner join      db_t_prod_stag.bc_accountcontact c
                                                                                                                                                                              ON              c.accountid_stg=a.id_stg
                                                                                                                                                                              inner join      db_t_prod_stag.bc_contact
                                                                                                                                                                              ON              bc_contact.id_stg = c.contactid_stg
                                                                                                                                                                              join            db_t_prod_stag.bctl_contact
                                                                                                                                                                              ON              bctl_contact.id_stg=bc_contact.subtype_stg
                                                                                                                                                                              left join       db_t_prod_stag.bc_accountcontactrole f
                                                                                                                                                                              ON              f.accountcontactid_stg = c.id_stg
                                                                                                                                                                              left join       db_t_prod_stag.bctl_accountrole g
                                                                                                                                                                              ON              g.id_stg = f.role_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_gendertype
                                                                                                                                                                              ON              bc_contact.gender_stg = bctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxfilingstatustype
                                                                                                                                                                              ON              bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxstatus
                                                                                                                                                                              ON              bc_contact.taxstatus_stg = bctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_maritalstatus
                                                                                                                                                                              ON              bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_nameprefix
                                                                                                                                                                              ON              bc_contact.prefix_stg = bctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_namesuffix
                                                                                                                                                                              ON              bc_contact.suffix_stg = bctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_user
                                                                                                                                                                              ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_credential
                                                                                                                                                                              ON              bc_user.credentialid_stg = bc_credential.id_stg
                                                                                                                                                                              WHERE           ((
                                                                                                                                                                                                        b.overridingpayer_alfa_stg IS NULL
                                                                                                                                                                                                        AND             c.primarypayer_stg = 1)
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        b.overridingpayer_alfa_stg IS NOT NULL))
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        bc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_contact.updatetime_stg <=( :END_DTTM))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        bc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_user.updatetime_stg <= ( :END_DTTM)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /* DB_T_PROD_STAG.pc_contact dropzone query */
                                                                                                                                                                              SELECT          pc_contact.updatetime_stg           AS updatetime,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255))          AS dbaname_alfa,
                                                                                                                                                                                              pc_contact.lastname_stg             AS lastname,
                                                                                                                                                                                              pc_contact.firstname_stg            AS firstname,
                                                                                                                                                                                              pc_contact.publicid_stg             AS publicid,
                                                                                                                                                                                              pc_contact.formername_stg           AS formername,
                                                                                                                                                                                              pc_contact.name_stg                 AS name,
                                                                                                                                                                                              pc_contact.addressbookuid_stg       AS linkid,
                                                                                                                                                                                              pc_contact.middlename_stg           AS middlename,
                                                                                                                                                                                              pc_contact.retired_stg              AS retired,
                                                                                                                                                                                              pc_contact.createtime_stg           AS createtime,
                                                                                                                                                                                              pctl_nameprefix.typecode_stg        AS name_prefix_txt,
                                                                                                                                                                                              pctl_namesuffix.typecode_stg        AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255)) AS source,
                                                                                                                                                                                              pctl_contact.name_stg               AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.pc_contact
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_contact
                                                                                                                                                                              ON              pctl_contact.id_stg = pc_contact.subtype_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_gendertype
                                                                                                                                                                              ON              pc_contact.gender_stg = pctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_taxfilingstatustype
                                                                                                                                                                              ON              pc_contact.taxfilingstatus_stg = pctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_taxstatus
                                                                                                                                                                              ON              pc_contact.taxstatus_stg = pctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_maritalstatus
                                                                                                                                                                              ON              pc_contact.maritalstatus_stg = pctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_nameprefix
                                                                                                                                                                              ON              pc_contact.prefix_stg = pctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_namesuffix
                                                                                                                                                                              ON              pc_contact.suffix_stg = pctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_user
                                                                                                                                                                              ON              pc_user.contactid_stg = pc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_credential
                                                                                                                                                                              ON              pc_user.credentialid_stg = pc_credential.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_policyperiod
                                                                                                                                                                              ON              pc_policyperiod.pnicontactdenorm_stg = pc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_effectivedatedfields
                                                                                                                                                                              ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_producercode
                                                                                                                                                                              ON              pc_producercode.id_stg=pc_effectivedatedfields.producercodeid_stg
                                                                                                                                                                              WHERE           pctl_contact.typecode_stg = (''UserContact'')
                                                                                                                                                                              AND
                                                                                                                                                                                              /*  below condition added to avoid duplicates*/
                                                                                                                                                                                              pc_contact.publicid_stg NOT IN (''default_data:1'',
                                                                                                                                                                                                        ''systemTables:1'',
                                                                                                                                                                                                        ''systemTables:2'')
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        pc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             pc_contact.updatetime_stg <= ( :END_DTTM))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        pc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             pc_user.updatetime_stg <= ( :END_DTTM)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /* DB_T_PROD_STAG.cc_contact dropzone query  */
                                                                                                                                                                              SELECT DISTINCT cc_contact.updatetime_stg           AS updatetime,
                                                                                                                                                                                              cc_contact.dbaname_alfa_stg         AS dbaname_alfa,
                                                                                                                                                                                              cc_contact.lastname_stg             AS lastname,
                                                                                                                                                                                              cc_contact.firstname_stg            AS firstname,
                                                                                                                                                                                              cc_contact.publicid_stg             AS publicid,
                                                                                                                                                                                              cc_contact.formername_stg           AS formername,
                                                                                                                                                                                              cc_contact.name_stg                 AS name,
                                                                                                                                                                                              cc_contact.addressbookuid_stg       AS linkid,
                                                                                                                                                                                              cc_contact.middlename_stg           AS middlename,
                                                                                                                                                                                              cc_contact.retired_stg              AS retired,
                                                                                                                                                                                              cc_contact.createtime_stg           AS createtime,
                                                                                                                                                                                              cctl_nameprefix.typecode_stg        AS name_prefix_txt,
                                                                                                                                                                                              cctl_namesuffix.typecode_stg        AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255)) AS source,
                                                                                                                                                                                              cctl_contact.name_stg               AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.cc_contact
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_contact
                                                                                                                                                                              ON              cctl_contact.id_stg = cc_contact.subtype_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_gendertype
                                                                                                                                                                              ON              cc_contact.gender_stg = cctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_taxfilingstatustype
                                                                                                                                                                              ON              cc_contact.taxfilingstatus_stg = cctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_taxstatus
                                                                                                                                                                              ON              cc_contact.taxstatus_stg = cctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_maritalstatus
                                                                                                                                                                              ON              cc_contact.maritalstatus_stg = cctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_nameprefix
                                                                                                                                                                              ON              cc_contact.prefix_stg = cctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_namesuffix
                                                                                                                                                                              ON              cc_contact.suffix_stg = cctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_user
                                                                                                                                                                              ON              cc_user.contactid_stg = cc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_credential
                                                                                                                                                                              ON              cc_user.credentialid_stg = cc_credential.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_claimcontact
                                                                                                                                                                              ON              cc_contact.id_stg=cc_claimcontact.contactid_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_claimcontactrole
                                                                                                                                                                              ON              cc_claimcontact.id_stg=cc_claimcontactrole.claimcontactid_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_incident
                                                                                                                                                                              ON              cc_claimcontactrole.claimcontactid_stg =cc_incident.id_stg
                                                                                                                                                                              WHERE           (
                                                                                                                                                                                                        cc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                              AND             cc_contact.updatetime_stg <= ( :END_DTTM) )
                                                                                                                                                                              OR              (
                                                                                                                                                                                                        cc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                              AND             cc_user.updatetime_stg <=( :END_DTTM) )
                                                                                                                                                                              UNION
                                                                                                                                                                              /* ab_contact dropzone query */
                                                                                                                                                                              SELECT          ab_abcontact.updatetime_stg            AS updatetime,
                                                                                                                                                                                              ab_abcontact.dbaname_alfa_stg          AS dbaname_alfa,
                                                                                                                                                                                              ab_abcontact.lastname_stg              AS lastname,
                                                                                                                                                                                              ab_abcontact.firstname_stg             AS firstname,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255))             AS publicid,
                                                                                                                                                                                              ab_abcontact.formername_stg            AS formername,
                                                                                                                                                                                              ab_abcontact.name_stg                  AS name,
                                                                                                                                                                                              ab_abcontact.linkid_stg                AS linkid,
                                                                                                                                                                                              ab_abcontact.middlename_stg            AS middlename,
                                                                                                                                                                                              ab_abcontact.retired_stg               AS retired,
                                                                                                                                                                                              ab_abcontact.createtime_stg            AS createtime,
                                                                                                                                                                                              abtl_nameprefix.typecode_stg           AS name_prefix_txt,
                                                                                                                                                                                              abtl_namesuffix.typecode_stg           AS name_sfx_txt,
                                                                                                                                                                                              cast(''ContactManager'' AS VARCHAR(255)) AS source,
                                                                                                                                                                                              abtl_abcontact.name_stg                AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.ab_abcontact
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_abcontact
                                                                                                                                                                              ON              abtl_abcontact.id_stg = ab_abcontact.subtype_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_gendertype
                                                                                                                                                                              ON              ab_abcontact.gender_stg = abtl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_taxfilingstatustype
                                                                                                                                                                              ON              ab_abcontact.taxfilingstatus_stg = abtl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_taxstatus
                                                                                                                                                                              ON              ab_abcontact.taxstatus_stg = abtl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_maritalstatus
                                                                                                                                                                              ON              ab_abcontact.maritalstatus_stg = abtl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_nameprefix
                                                                                                                                                                              ON              ab_abcontact.prefix_stg = abtl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_namesuffix
                                                                                                                                                                              ON              ab_abcontact.suffix_stg = abtl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.ab_user
                                                                                                                                                                              ON              ab_user.contactid_stg = ab_abcontact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.ab_credential
                                                                                                                                                                              ON              ab_user.credentialid_stg = ab_credential.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_occupation
                                                                                                                                                                              ON              abtl_occupation.id_stg = ab_abcontact.occupation_alfa_stg
                                                                                                                                                                              WHERE           ab_abcontact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                              AND             ab_abcontact.updatetime_stg <= ( :END_DTTM) ) ab_cnt_inner ) ab_abcontact
                                                                                                                                WHERE           ab_abcontact.tl_cnt_name IN (''Person'',
                                                                                                                                                                             ''Adjudicator'',
                                                                                                                                                                             ''User Contact'',
                                                                                                                                                                             ''Vendor (Person)'',
                                                                                                                                                                             ''Attorney'',
                                                                                                                                                                             ''Doctor'',
                                                                                                                                                                             ''Policy Person'',
                                                                                                                                                                             ''Lodging (Person)'')
                                                                                                                                AND             (
                                                                                                                                                                source = ''ClaimCenter''
                                                                                                                                                AND             publicid IS NOT NULL)
                                                                                                                                AND             ab_abcontact.formername IS NOT NULL qualify row_number() over ( PARTITION BY publicid ORDER BY src_strt_dt DESC)=1
                                                                                                                                UNION
                                                                                                                                /* ALIAS FLOW */
                                                                                                                                SELECT DISTINCT upper(ab_abcontact.linkid) AS linkid ,
                                                                                                                                                ab_abcontact.firstname ,
                                                                                                                                                ab_abcontact.middlename ,
                                                                                                                                                ab_abcontact.formername      AS lastname ,
                                                                                                                                                ab_abcontact.name_prefix_txt AS prefix ,
                                                                                                                                                ab_abcontact.name_sfx_txt    AS suffix ,
                                                                                                                                                ab_abcontact.name ,
                                                                                                                                                ab_abcontact.formername ,
                                                                                                                                                CASE
                                                                                                                                                                WHEN ab_abcontact.firstname IS NOT NULL
                                                                                                                                                                AND             ab_abcontact.formername IS NOT NULL THEN ''ALS''
                                                                                                                                                                ELSE ''UNK''
                                                                                                                                                END AS name_type_cd ,
                                                                                                                                                publicid ,
                                                                                                                                                source                         AS src,
                                                                                                                                                ab_abcontact.updatetime        AS src_strt_dt,
                                                                                                                                                to_date(''99991231'',''YYYYMMDD'') AS src_end_dt,
                                                                                                                                                ab_abcontact.createtime        AS indiv_name_strt_dttm,
                                                                                                                                                to_date(''99991231'',''YYYYMMDD'') AS indiv_name_end_dttm,
                                                                                                                                                retired
                                                                                                                                FROM            (
                                                                                                                                                       SELECT ab_cnt_inner.updatetime,
                                                                                                                                                              ab_cnt_inner.dbaname_alfa,
                                                                                                                                                              ab_cnt_inner.lastname ,
                                                                                                                                                              ab_cnt_inner.firstname ,
                                                                                                                                                              ab_cnt_inner.publicid,
                                                                                                                                                              ab_cnt_inner.formername ,
                                                                                                                                                              ab_cnt_inner.name ,
                                                                                                                                                              ab_cnt_inner.linkid ,
                                                                                                                                                              ab_cnt_inner.middlename,
                                                                                                                                                              ab_cnt_inner.retired,
                                                                                                                                                              ab_cnt_inner.createtime ,
                                                                                                                                                              ab_cnt_inner.name_prefix_txt,
                                                                                                                                                              ab_cnt_inner.name_sfx_txt,
                                                                                                                                                              ab_cnt_inner.source,
                                                                                                                                                              ab_cnt_inner.tl_cnt_name
                                                                                                                                                       FROM   (
                                                                                                                                                                              /* DB_T_PROD_STAG.bc_contact dropzone query */
                                                                                                                                                                              SELECT          bc_contact.updatetime_stg                           AS updatetime,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255))                          AS dbaname_alfa,
                                                                                                                                                                                              bc_contact.lastname_stg                             AS lastname,
                                                                                                                                                                                              bc_contact.firstname_stg                            AS firstname,
                                                                                                                                                                                              cast(bc_contact.publicid_stg AS VARCHAR(100))       AS publicid,
                                                                                                                                                                                              bc_contact.formername_stg                           AS formername,
                                                                                                                                                                                              bc_contact.name_stg                                 AS name,
                                                                                                                                                                                              cast(bc_contact.addressbookuid_stg AS VARCHAR(100)) AS linkid,
                                                                                                                                                                                              bc_contact.middlename_stg                           AS middlename,
                                                                                                                                                                                              bc_contact.retired_stg                              AS retired,
                                                                                                                                                                                              bc_contact.createtime_stg                           AS createtime,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))                           AS name_prefix_txt,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))                           AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255))                          AS source,
                                                                                                                                                                                              bctl_contact.name_stg                                        AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.bc_contact
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_contact
                                                                                                                                                                              ON              bctl_contact.id_stg = bc_contact.subtype_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_gendertype
                                                                                                                                                                              ON              bc_contact.gender_stg = bctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxfilingstatustype
                                                                                                                                                                              ON              bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxstatus
                                                                                                                                                                              ON              bc_contact.taxstatus_stg = bctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_maritalstatus
                                                                                                                                                                              ON              bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_nameprefix
                                                                                                                                                                              ON              bc_contact.prefix_stg = bctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_namesuffix
                                                                                                                                                                              ON              bc_contact.suffix_stg = bctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_user
                                                                                                                                                                              ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_credential
                                                                                                                                                                              ON              bc_user.credentialid_stg = bc_credential.id_stg
                                                                                                                                                                              WHERE           bctl_contact.typecode_stg = (''UserContact'')
                                                                                                                                                                              AND
                                                                                                                                                                                              /*  below condition added to avoid duplicates*/
                                                                                                                                                                                              bc_contact.publicid_stg NOT IN (''default_data:1'',
                                                                                                                                                                                                        ''systemTables:1'',
                                                                                                                                                                                                        ''systemTables:2'')
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        bc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_contact.updatetime_stg <=( :END_DTTM))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        bc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_user.updatetime_stg <= ( :END_DTTM)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /* Primary and Secondary Payer contact (this is at the Account level) */
                                                                                                                                                                              SELECT          bc_contact.updatetime_stg  AS updatetime,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255)) AS dbaname_alfa,
                                                                                                                                                                                              bc_contact.lastname_stg    AS lastname,
                                                                                                                                                                                              bc_contact.firstname_stg   AS firstname,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN(
                                                                                                                                                                                                        bc_contact.externalid_stg IS NOT NULL) THEN bc_contact.externalid_stg
                                                                                                                                                                                                        ELSE bc_contact.publicid_stg
                                                                                                                                                                                              END                           AS publicid,
                                                                                                                                                                                              bc_contact.formername_stg     AS formername,
                                                                                                                                                                                              bc_contact.name_stg           AS name,
                                                                                                                                                                                              bc_contact.addressbookuid_stg AS linkid,
                                                                                                                                                                                              bc_contact.middlename_stg     AS middlename,
                                                                                                                                                                                              bc_contact.retired_stg        AS retired,
                                                                                                                                                                                              bc_contact.createtime_stg     AS createtime,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))     AS name_prefix_txt,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))     AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255))    AS source,
                                                                                                                                                                                              bctl_contact.name_stg                  AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.bc_account a
                                                                                                                                                                              inner join      db_t_prod_stag.bc_accountcontact h
                                                                                                                                                                              ON              h.accountid_stg = a.id_stg
                                                                                                                                                                              inner join      db_t_prod_stag.bc_contact
                                                                                                                                                                              ON              bc_contact.id_stg = h.contactid_stg
                                                                                                                                                                              join            db_t_prod_stag.bctl_contact
                                                                                                                                                                              ON              bctl_contact.id_stg=bc_contact.subtype_stg
                                                                                                                                                                              left join       db_t_prod_stag.bc_accountcontactrole i
                                                                                                                                                                              ON              i.accountcontactid_stg = h.id_stg
                                                                                                                                                                              left join       db_t_prod_stag.bctl_accountrole j
                                                                                                                                                                              ON              j.id_stg = i.role_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_gendertype
                                                                                                                                                                              ON              bc_contact.gender_stg = bctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxfilingstatustype
                                                                                                                                                                              ON              bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxstatus
                                                                                                                                                                              ON              bc_contact.taxstatus_stg = bctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_maritalstatus
                                                                                                                                                                              ON              bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_nameprefix
                                                                                                                                                                              ON              bc_contact.prefix_stg = bctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_namesuffix
                                                                                                                                                                              ON              bc_contact.suffix_stg = bctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_user
                                                                                                                                                                              ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_credential
                                                                                                                                                                              ON              bc_user.credentialid_stg = bc_credential.id_stg
                                                                                                                                                                              WHERE           ((
                                                                                                                                                                                                        h.primarypayer_stg = 1)
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        j.name_stg = ''Payer''))
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        bc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_contact.updatetime_stg <=( :END_DTTM))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        bc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_user.updatetime_stg <= ( :END_DTTM)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /* Primary Payer and Overiding Payer Contact (this is at the Invoicestream level) */
                                                                                                                                                                              SELECT          bc_contact.updatetime_stg  AS updatetime,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255)) AS dbaname_alfa,
                                                                                                                                                                                              bc_contact.lastname_stg    AS lastname,
                                                                                                                                                                                              bc_contact.firstname_stg   AS firstname,
                                                                                                                                                                                              CASE
                                                                                                                                                                                                        WHEN (
                                                                                                                                                                                                        bc_contact.externalid_stg IS NULL) THEN bc_contact.publicid_stg
                                                                                                                                                                                                        ELSE bc_contact.externalid_stg
                                                                                                                                                                                              END                           AS publicid,
                                                                                                                                                                                              bc_contact.formername_stg     AS formername,
                                                                                                                                                                                              bc_contact.name_stg           AS name,
                                                                                                                                                                                              bc_contact.addressbookuid_stg AS linkid,
                                                                                                                                                                                              bc_contact.middlename_stg     AS middlename,
                                                                                                                                                                                              bc_contact.retired_stg        AS retired,
                                                                                                                                                                                              bc_contact.createtime_stg     AS createtime,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))     AS name_prefix_txt,
                                                                                                                                                                                              cast(NULL AS          VARCHAR(60))     AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255))    AS source,
                                                                                                                                                                                              bctl_contact.name_stg                  AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.bc_account a
                                                                                                                                                                              inner join      db_t_prod_stag.bc_invoicestream b
                                                                                                                                                                              ON              a.id_stg = b.accountid_stg
                                                                                                                                                                              inner join      db_t_prod_stag.bc_accountcontact c
                                                                                                                                                                              ON              c.accountid_stg=a.id_stg
                                                                                                                                                                              inner join      db_t_prod_stag.bc_contact
                                                                                                                                                                              ON              bc_contact.id_stg = c.contactid_stg
                                                                                                                                                                              join            db_t_prod_stag.bctl_contact
                                                                                                                                                                              ON              bctl_contact.id_stg=bc_contact.subtype_stg
                                                                                                                                                                              left join       db_t_prod_stag.bc_accountcontactrole f
                                                                                                                                                                              ON              f.accountcontactid_stg = c.id_stg
                                                                                                                                                                              left join       db_t_prod_stag.bctl_accountrole g
                                                                                                                                                                              ON              g.id_stg = f.role_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_gendertype
                                                                                                                                                                              ON              bc_contact.gender_stg = bctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxfilingstatustype
                                                                                                                                                                              ON              bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_taxstatus
                                                                                                                                                                              ON              bc_contact.taxstatus_stg = bctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_maritalstatus
                                                                                                                                                                              ON              bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_nameprefix
                                                                                                                                                                              ON              bc_contact.prefix_stg = bctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bctl_namesuffix
                                                                                                                                                                              ON              bc_contact.suffix_stg = bctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_user
                                                                                                                                                                              ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.bc_credential
                                                                                                                                                                              ON              bc_user.credentialid_stg = bc_credential.id_stg
                                                                                                                                                                              WHERE           ((
                                                                                                                                                                                                        b.overridingpayer_alfa_stg IS NULL
                                                                                                                                                                                                        AND             c.primarypayer_stg = 1)
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        b.overridingpayer_alfa_stg IS NOT NULL))
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        bc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_contact.updatetime_stg <=( :END_DTTM))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        bc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             bc_user.updatetime_stg <= ( :END_DTTM)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /* DB_T_PROD_STAG.pc_contact dropzone query */
                                                                                                                                                                              SELECT          pc_contact.updatetime_stg           AS updatetime,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255))          AS dbaname_alfa,
                                                                                                                                                                                              pc_contact.lastname_stg             AS lastname,
                                                                                                                                                                                              pc_contact.firstname_stg            AS firstname,
                                                                                                                                                                                              pc_contact.publicid_stg             AS publicid,
                                                                                                                                                                                              pc_contact.formername_stg           AS formername,
                                                                                                                                                                                              pc_contact.name_stg                 AS name,
                                                                                                                                                                                              pc_contact.addressbookuid_stg       AS linkid,
                                                                                                                                                                                              pc_contact.middlename_stg           AS middlename,
                                                                                                                                                                                              pc_contact.retired_stg              AS retired,
                                                                                                                                                                                              pc_contact.createtime_stg           AS createtime,
                                                                                                                                                                                              pctl_nameprefix.typecode_stg        AS name_prefix_txt,
                                                                                                                                                                                              pctl_namesuffix.typecode_stg        AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255)) AS source,
                                                                                                                                                                                              pctl_contact.name_stg               AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.pc_contact
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_contact
                                                                                                                                                                              ON              pctl_contact.id_stg = pc_contact.subtype_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_gendertype
                                                                                                                                                                              ON              pc_contact.gender_stg = pctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_taxfilingstatustype
                                                                                                                                                                              ON              pc_contact.taxfilingstatus_stg = pctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_taxstatus
                                                                                                                                                                              ON              pc_contact.taxstatus_stg = pctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_maritalstatus
                                                                                                                                                                              ON              pc_contact.maritalstatus_stg = pctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_nameprefix
                                                                                                                                                                              ON              pc_contact.prefix_stg = pctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pctl_namesuffix
                                                                                                                                                                              ON              pc_contact.suffix_stg = pctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_user
                                                                                                                                                                              ON              pc_user.contactid_stg = pc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_credential
                                                                                                                                                                              ON              pc_user.credentialid_stg = pc_credential.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_policyperiod
                                                                                                                                                                              ON              pc_policyperiod.pnicontactdenorm_stg = pc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_effectivedatedfields
                                                                                                                                                                              ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.pc_producercode
                                                                                                                                                                              ON              pc_producercode.id_stg=pc_effectivedatedfields.producercodeid_stg
                                                                                                                                                                              WHERE           pctl_contact.typecode_stg = (''UserContact'')
                                                                                                                                                                              AND
                                                                                                                                                                                              /*  below condition added to avoid duplicates*/
                                                                                                                                                                                              pc_contact.publicid_stg NOT IN (''default_data:1'',
                                                                                                                                                                                                        ''systemTables:1'',
                                                                                                                                                                                                        ''systemTables:2'')
                                                                                                                                                                              AND             ((
                                                                                                                                                                                                        pc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             pc_contact.updatetime_stg <= ( :END_DTTM))
                                                                                                                                                                                              OR              (
                                                                                                                                                                                                        pc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                                        AND             pc_user.updatetime_stg <= ( :END_DTTM)))
                                                                                                                                                                              UNION
                                                                                                                                                                              /* DB_T_PROD_STAG.cc_contact dropzone query  */
                                                                                                                                                                              SELECT DISTINCT cc_contact.updatetime_stg           AS updatetime,
                                                                                                                                                                                              cc_contact.dbaname_alfa_stg         AS dbaname_alfa,
                                                                                                                                                                                              cc_contact.lastname_stg             AS lastname,
                                                                                                                                                                                              cc_contact.firstname_stg            AS firstname,
                                                                                                                                                                                              cc_contact.publicid_stg             AS publicid,
                                                                                                                                                                                              cc_contact.formername_stg           AS formername,
                                                                                                                                                                                              cc_contact.name_stg                 AS name,
                                                                                                                                                                                              cc_contact.addressbookuid_stg       AS linkid,
                                                                                                                                                                                              cc_contact.middlename_stg           AS middlename,
                                                                                                                                                                                              cc_contact.retired_stg              AS retired,
                                                                                                                                                                                              cc_contact.createtime_stg           AS createtime,
                                                                                                                                                                                              cctl_nameprefix.typecode_stg        AS name_prefix_txt,
                                                                                                                                                                                              cctl_namesuffix.typecode_stg        AS name_sfx_txt,
                                                                                                                                                                                              cast(''ClaimCenter'' AS VARCHAR(255)) AS source,
                                                                                                                                                                                              cctl_contact.name_stg               AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.cc_contact
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_contact
                                                                                                                                                                              ON              cctl_contact.id_stg = cc_contact.subtype_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_gendertype
                                                                                                                                                                              ON              cc_contact.gender_stg = cctl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_taxfilingstatustype
                                                                                                                                                                              ON              cc_contact.taxfilingstatus_stg = cctl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_taxstatus
                                                                                                                                                                              ON              cc_contact.taxstatus_stg = cctl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_maritalstatus
                                                                                                                                                                              ON              cc_contact.maritalstatus_stg = cctl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_nameprefix
                                                                                                                                                                              ON              cc_contact.prefix_stg = cctl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cctl_namesuffix
                                                                                                                                                                              ON              cc_contact.suffix_stg = cctl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_user
                                                                                                                                                                              ON              cc_user.contactid_stg = cc_contact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_credential
                                                                                                                                                                              ON              cc_user.credentialid_stg = cc_credential.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_claimcontact
                                                                                                                                                                              ON              cc_contact.id_stg=cc_claimcontact.contactid_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_claimcontactrole
                                                                                                                                                                              ON              cc_claimcontact.id_stg=cc_claimcontactrole.claimcontactid_stg
                                                                                                                                                                              left outer join db_t_prod_stag.cc_incident
                                                                                                                                                                              ON              cc_claimcontactrole.claimcontactid_stg =cc_incident.id_stg
                                                                                                                                                                              WHERE           (
                                                                                                                                                                                                        cc_contact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                              AND             cc_contact.updatetime_stg <= ( :END_DTTM) )
                                                                                                                                                                              OR              (
                                                                                                                                                                                                        cc_user.updatetime_stg>(:START_DTTM)
                                                                                                                                                                                              AND             cc_user.updatetime_stg <=( :END_DTTM) )
                                                                                                                                                                              UNION
                                                                                                                                                                              /* ab_contact dropzone query  */
                                                                                                                                                                              SELECT          ab_abcontact.updatetime_stg            AS updatetime,
                                                                                                                                                                                              ab_abcontact.dbaname_alfa_stg          AS dbaname_alfa,
                                                                                                                                                                                              ab_abcontact.lastname_stg              AS lastname,
                                                                                                                                                                                              ab_abcontact.firstname_stg             AS firstname,
                                                                                                                                                                                              cast(NULL AS VARCHAR(255))             AS publicid,
                                                                                                                                                                                              ab_abcontact.formername_stg            AS formername,
                                                                                                                                                                                              ab_abcontact.name_stg                  AS name,
                                                                                                                                                                                              ab_abcontact.linkid_stg                AS linkid,
                                                                                                                                                                                              ab_abcontact.middlename_stg            AS middlename,
                                                                                                                                                                                              ab_abcontact.retired_stg               AS retired,
                                                                                                                                                                                              ab_abcontact.createtime_stg            AS createtime,
                                                                                                                                                                                              abtl_nameprefix.typecode_stg           AS name_prefix_txt,
                                                                                                                                                                                              abtl_namesuffix.typecode_stg           AS name_sfx_txt,
                                                                                                                                                                                              cast(''ContactManager'' AS VARCHAR(255)) AS source,
                                                                                                                                                                                              abtl_abcontact.name_stg                AS tl_cnt_name
                                                                                                                                                                              FROM            db_t_prod_stag.ab_abcontact
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_abcontact
                                                                                                                                                                              ON              abtl_abcontact.id_stg = ab_abcontact.subtype_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_gendertype
                                                                                                                                                                              ON              ab_abcontact.gender_stg = abtl_gendertype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_taxfilingstatustype
                                                                                                                                                                              ON              ab_abcontact.taxfilingstatus_stg = abtl_taxfilingstatustype.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_taxstatus
                                                                                                                                                                              ON              ab_abcontact.taxstatus_stg = abtl_taxstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_maritalstatus
                                                                                                                                                                              ON              ab_abcontact.maritalstatus_stg = abtl_maritalstatus.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_nameprefix
                                                                                                                                                                              ON              ab_abcontact.prefix_stg = abtl_nameprefix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_namesuffix
                                                                                                                                                                              ON              ab_abcontact.suffix_stg = abtl_namesuffix.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.ab_user
                                                                                                                                                                              ON              ab_user.contactid_stg = ab_abcontact.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.ab_credential
                                                                                                                                                                              ON              ab_user.credentialid_stg = ab_credential.id_stg
                                                                                                                                                                              left outer join db_t_prod_stag.abtl_occupation
                                                                                                                                                                              ON              abtl_occupation.id_stg = ab_abcontact.occupation_alfa_stg
                                                                                                                                                                              WHERE           ab_abcontact.updatetime_stg>(:START_DTTM)
                                                                                                                                                                              AND             ab_abcontact.updatetime_stg <= ( :END_DTTM) ) ab_cnt_inner ) ab_abcontact
                                                                                                                                WHERE           ab_abcontact.tl_cnt_name IN (''Person'',
                                                                                                                                                                             ''Adjudicator'',
                                                                                                                                                                             ''User Contact'',
                                                                                                                                                                             ''Vendor (Person)'',
                                                                                                                                                                             ''Attorney'',
                                                                                                                                                                             ''Doctor'',
                                                                                                                                                                             ''Policy Person'',
                                                                                                                                                                             ''Lodging (Person)'')
                                                                                                                                AND             (
                                                                                                                                                                source = ''ContactManager''
                                                                                                                                                AND             linkid IS NOT NULL)
                                                                                                                                AND             ab_abcontact.formername IS NOT NULL qualify row_number() over ( PARTITION BY linkid ORDER BY src_strt_dt DESC)=1 )aa
                                                                                                       GROUP BY aa.linkid,
                                                                                                                aa.firstname,
                                                                                                                aa.middlename,
                                                                                                                aa.lastname,
                                                                                                                aa.prefix,
                                                                                                                aa.suffix,
                                                                                                                aa.name,
                                                                                                                aa.formername,
                                                                                                                aa.name_type_cd,
                                                                                                                aa.publicid,
                                                                                                                aa.src,
                                                                                                                aa.src_end_dt,
                                                                                                                aa.indiv_name_end_dttm,
                                                                                                                aa.retired ) stag
                                                                              left outer join
                                                                                              (
                                                                                                     SELECT indiv.indiv_prty_id AS indiv_prty_id,
                                                                                                            indiv.nk_link_id    AS nk_link_id
                                                                                                     FROM   db_t_prod_core.indiv 
                                                                                                     WHERE  indiv.nk_publc_id IS NULL )lkp_indiv_cnt_mgr
                                                                              ON              lkp_indiv_cnt_mgr.nk_link_id=stag.linkid
                                                                              left outer join
                                                                                              (
                                                                                                     SELECT indiv.indiv_prty_id AS indiv_prty_id,
                                                                                                            indiv.nk_publc_id   AS nk_publc_id
                                                                                                     FROM   db_t_prod_core.indiv 
                                                                                                     WHERE  indiv.nk_publc_id IS NOT NULL )lkp_indiv_clm_ctr
                                                                              ON              lkp_indiv_clm_ctr.nk_publc_id=stag.publicid ) src )temp
                                left outer join
                                                (
                                                         SELECT   indiv_name.indiv_name_strt_dttm   AS indiv_name_strt_dttm ,
                                                                  upper(indiv_name.gvn_name)        AS gvn_name ,
                                                                  upper(indiv_name.mdl_name)        AS mdl_name ,
                                                                  upper(indiv_name.fmly_name)       AS fmly_name ,
                                                                  upper(indiv_name.name_prefix_txt) AS name_prefix_txt ,
                                                                  upper(indiv_name.name_sfx_txt)    AS name_sfx_txt ,
                                                                  indiv_name.indiv_name_end_dttm    AS indiv_name_end_dttm ,
                                                                  upper(indiv_name.indiv_full_name) AS indiv_full_name ,
                                                                  indiv_name.edw_strt_dttm          AS edw_strt_dttm ,
                                                                  indiv_name.edw_end_dttm           AS edw_end_dttm ,
                                                                  indiv_name.indiv_prty_id          AS indiv_prty_id ,
                                                                  indiv_name.name_type_cd           AS name_type_cd
                                                         FROM     db_t_prod_core.indiv_name  qualify row_number () over (PARTITION BY indiv_prty_id ORDER BY edw_end_dttm DESC)=1 )tgt_lkup
                                ON              tgt_lkup.indiv_prty_id=temp.src_indiv_prty_id ) src ) );
  -- Component exp_all_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_all_source AS
  (
         SELECT sq_ab_abcontact1.src_firstname                                         AS src_firstname,
                sq_ab_abcontact1.src_middlename                                        AS src_middlename,
                sq_ab_abcontact1.src_lastname                                          AS src_lastname,
                sq_ab_abcontact1.src_prefix                                            AS src_prefix,
                sq_ab_abcontact1.src_suffix                                            AS src_suffix,
                sq_ab_abcontact1.src_name                                              AS src_name,
                sq_ab_abcontact1.src_name_type_cd                                      AS src_name_type_cd,
                sq_ab_abcontact1.src_strt_dt                                           AS src_strt_dt,
                sq_ab_abcontact1.src_end_dt                                            AS src_end_dt,
                sq_ab_abcontact1.src_indiv_name_strt_dttm                              AS src_indiv_name_strt_dttm,
                sq_ab_abcontact1.src_indiv_name_end_dttm                               AS src_indiv_name_end_dttm,
                sq_ab_abcontact1.src_retired                                           AS src_retired,
                sq_ab_abcontact1.src_indiv_prty_id                                     AS src_indiv_prty_id,
                sq_ab_abcontact1.lkup_indiv_prty_id                                    AS lkup_indiv_prty_id,
                sq_ab_abcontact1.lkup_name_type_cd                                     AS lkup_name_type_cd,
                sq_ab_abcontact1.lkup_indiv_name_strt_dttm                             AS lkup_indiv_name_strt_dttm,
                sq_ab_abcontact1.lkup_edw_strt_dttm                                    AS lkup_edw_strt_dttm,
                sq_ab_abcontact1.lkup_edw_end_dttm                                     AS lkup_edw_end_dttm,
                sq_ab_abcontact1.ins_upd_flag                                          AS ins_upd_flag,
                :prcs_id                                                               AS process_id,
                current_timestamp                                                      AS edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                sq_ab_abcontact1.source_record_id
         FROM   sq_ab_abcontact1 );
  -- Component rtr_insert_update_flag_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_insert_update_flag_insert as
  SELECT exp_all_source.lkup_indiv_prty_id        AS lkp_indiv_prty_id,
         exp_all_source.lkup_name_type_cd         AS lkp_name_type_cd,
         exp_all_source.lkup_indiv_name_strt_dttm AS lkp_indiv_name_strt_dt,
         exp_all_source.lkup_edw_strt_dttm        AS lkp_edw_strt_dttm,
         exp_all_source.src_name_type_cd          AS in_name_type_cd,
         exp_all_source.src_indiv_prty_id         AS in_indiv_prty_id,
         exp_all_source.src_prefix                AS in_prefix,
         exp_all_source.src_middlename            AS in_middlename,
         exp_all_source.src_lastname              AS in_lastname,
         exp_all_source.src_firstname             AS in_firstname,
         exp_all_source.src_suffix                AS in_suffix,
         exp_all_source.src_name                  AS in_name,
         exp_all_source.process_id                AS in_process_id,
         exp_all_source.edw_strt_dttm             AS in_edw_strt_dttm,
         exp_all_source.edw_end_dttm              AS in_edw_end_dttm,
         exp_all_source.src_strt_dt               AS in_src_strt_dt,
         exp_all_source.src_end_dt                AS in_src_end_dt,
         exp_all_source.ins_upd_flag              AS calc_ins_upd,
         exp_all_source.src_retired               AS retired,
         exp_all_source.lkup_edw_end_dttm         AS lkp_edw_end_dttm,
         exp_all_source.src_indiv_name_strt_dttm  AS indiv_name_strt_dttm,
         exp_all_source.src_indiv_name_end_dttm   AS indiv_name_end_dttm,
         exp_all_source.source_record_id
  FROM   exp_all_source
  WHERE  (
                exp_all_source.ins_upd_flag = ''I''
         OR     (
                       exp_all_source.src_retired = 0
                AND    exp_all_source.lkup_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) )
  OR     (
                exp_all_source.ins_upd_flag = ''U'' );
  
  -- Component rtr_insert_update_flag_RETIRE, Type ROUTER Output Group RETIRE
  create or replace temporary table rtr_insert_update_flag_retire as
  SELECT exp_all_source.lkup_indiv_prty_id        AS lkp_indiv_prty_id,
         exp_all_source.lkup_name_type_cd         AS lkp_name_type_cd,
         exp_all_source.lkup_indiv_name_strt_dttm AS lkp_indiv_name_strt_dt,
         exp_all_source.lkup_edw_strt_dttm        AS lkp_edw_strt_dttm,
         exp_all_source.src_name_type_cd          AS in_name_type_cd,
         exp_all_source.src_indiv_prty_id         AS in_indiv_prty_id,
         exp_all_source.src_prefix                AS in_prefix,
         exp_all_source.src_middlename            AS in_middlename,
         exp_all_source.src_lastname              AS in_lastname,
         exp_all_source.src_firstname             AS in_firstname,
         exp_all_source.src_suffix                AS in_suffix,
         exp_all_source.src_name                  AS in_name,
         exp_all_source.process_id                AS in_process_id,
         exp_all_source.edw_strt_dttm             AS in_edw_strt_dttm,
         exp_all_source.edw_end_dttm              AS in_edw_end_dttm,
         exp_all_source.src_strt_dt               AS in_src_strt_dt,
         exp_all_source.src_end_dt                AS in_src_end_dt,
         exp_all_source.ins_upd_flag              AS calc_ins_upd,
         exp_all_source.src_retired               AS retired,
         exp_all_source.lkup_edw_end_dttm         AS lkp_edw_end_dttm,
         exp_all_source.src_indiv_name_strt_dttm  AS indiv_name_strt_dttm,
         exp_all_source.src_indiv_name_end_dttm   AS indiv_name_end_dttm,
         exp_all_source.source_record_id
  FROM   exp_all_source
  WHERE  exp_all_source.ins_upd_flag = ''R''
  AND    exp_all_source.src_retired != 0
  AND    exp_all_source.lkup_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_tgt_indiv_name_insert, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_tgt_indiv_name_insert AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insert_update_flag_insert.in_indiv_prty_id     AS in_indiv_prty_id,
                rtr_insert_update_flag_insert.in_name_type_cd      AS in_name_type_cd,
                rtr_insert_update_flag_insert.in_src_strt_dt       AS in_src_strt_dt,
                rtr_insert_update_flag_insert.in_firstname         AS in_firstname,
                rtr_insert_update_flag_insert.in_middlename        AS in_middlename,
                rtr_insert_update_flag_insert.in_lastname          AS in_lastname,
                rtr_insert_update_flag_insert.in_prefix            AS in_prefix,
                rtr_insert_update_flag_insert.in_suffix            AS in_suffix,
                rtr_insert_update_flag_insert.in_name              AS in_name,
                rtr_insert_update_flag_insert.in_process_id        AS in_process_id,
                rtr_insert_update_flag_insert.in_edw_strt_dttm     AS in_edw_strt_dttm,
                rtr_insert_update_flag_insert.in_edw_end_dttm      AS in_edw_end_dttm,
                rtr_insert_update_flag_insert.retired              AS retired,
                rtr_insert_update_flag_insert.indiv_name_strt_dttm AS indiv_name_strt_dttm1,
                rtr_insert_update_flag_insert.indiv_name_end_dttm  AS indiv_name_end_dttm1,
                0                                                  AS update_strategy_action,
				rtr_insert_update_flag_insert.source_record_id
         FROM   rtr_insert_update_flag_insert );
  -- Component exp_indiv_name_insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_indiv_name_insert AS
  (
         SELECT upd_tgt_indiv_name_insert.in_indiv_prty_id      AS in_indiv_prty_id,
                upd_tgt_indiv_name_insert.in_name_type_cd       AS in_name_type_cd,
                upd_tgt_indiv_name_insert.in_src_strt_dt        AS in_src_strt_dt,
                upd_tgt_indiv_name_insert.in_firstname          AS in_firstname,
                upd_tgt_indiv_name_insert.in_middlename         AS in_middlename,
                upd_tgt_indiv_name_insert.in_lastname           AS in_lastname,
                upd_tgt_indiv_name_insert.in_prefix             AS in_prefix,
                upd_tgt_indiv_name_insert.in_suffix             AS in_suffix,
                upd_tgt_indiv_name_insert.in_name               AS in_name,
                upd_tgt_indiv_name_insert.in_process_id         AS in_process_id,
                upd_tgt_indiv_name_insert.in_edw_strt_dttm      AS in_edw_strt_dttm,
                upd_tgt_indiv_name_insert.indiv_name_strt_dttm1 AS indiv_name_strt_dttm1,
                upd_tgt_indiv_name_insert.indiv_name_end_dttm1  AS indiv_name_end_dttm1,
                CASE
                       WHEN upd_tgt_indiv_name_insert.retired = 0 THEN upd_tgt_indiv_name_insert.in_edw_end_dttm
                       ELSE current_timestamp
                END AS o_edw_end_dttm,
                CASE
                       WHEN upd_tgt_indiv_name_insert.retired != 0 THEN upd_tgt_indiv_name_insert.in_src_strt_dt
                       ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END AS trans_end_dttm,
                upd_tgt_indiv_name_insert.source_record_id
         FROM   upd_tgt_indiv_name_insert );
  -- Component tgt_indiv_name_insert, Type TARGET
  INSERT INTO db_t_prod_core.indiv_name
              (
                          indiv_prty_id,
                          name_type_cd,
                          indiv_name_strt_dttm,
                          gvn_name,
                          mdl_name,
                          fmly_name,
                          name_prefix_txt,
                          name_sfx_txt,
                          indiv_name_end_dttm,
                          indiv_full_name,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_indiv_name_insert.in_indiv_prty_id      AS indiv_prty_id,
         exp_indiv_name_insert.in_name_type_cd       AS name_type_cd,
         exp_indiv_name_insert.indiv_name_strt_dttm1 AS indiv_name_strt_dttm,
         exp_indiv_name_insert.in_firstname          AS gvn_name,
         exp_indiv_name_insert.in_middlename         AS mdl_name,
         exp_indiv_name_insert.in_lastname           AS fmly_name,
         exp_indiv_name_insert.in_prefix             AS name_prefix_txt,
         exp_indiv_name_insert.in_suffix             AS name_sfx_txt,
         exp_indiv_name_insert.indiv_name_end_dttm1  AS indiv_name_end_dttm,
         exp_indiv_name_insert.in_name               AS indiv_full_name,
         exp_indiv_name_insert.in_process_id         AS prcs_id,
         exp_indiv_name_insert.in_edw_strt_dttm      AS edw_strt_dttm,
         exp_indiv_name_insert.o_edw_end_dttm        AS edw_end_dttm,
         exp_indiv_name_insert.in_src_strt_dt        AS trans_strt_dttm,
         exp_indiv_name_insert.trans_end_dttm        AS trans_end_dttm
  FROM   exp_indiv_name_insert;
  
  -- Component upd_tgt_indiv_name_retire, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_tgt_indiv_name_retire AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_insert_update_flag_retire.lkp_indiv_prty_id      AS lkp_indiv_prty_id3,
                rtr_insert_update_flag_retire.lkp_name_type_cd       AS lkp_name_type_cd3,
                rtr_insert_update_flag_retire.lkp_edw_strt_dttm      AS lkp_edw_strt_dttm3,
                rtr_insert_update_flag_retire.lkp_indiv_name_strt_dt AS lkp_indiv_name_strt_dt3,
                rtr_insert_update_flag_retire.in_src_strt_dt         AS in_src_strt_dt4,
                1                                                    AS update_strategy_action,
				source_record_id
         FROM   rtr_insert_update_flag_retire );
  -- Component exp_indiv_name_retire, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_indiv_name_retire AS
  (
         SELECT upd_tgt_indiv_name_retire.lkp_indiv_prty_id3      AS lkp_indiv_prty_id3,
                upd_tgt_indiv_name_retire.lkp_name_type_cd3       AS lkp_name_type_cd3,
                upd_tgt_indiv_name_retire.lkp_edw_strt_dttm3      AS lkp_edw_strt_dttm3,
                upd_tgt_indiv_name_retire.lkp_indiv_name_strt_dt3 AS lkp_indiv_name_strt_dt3,
                upd_tgt_indiv_name_retire.in_src_strt_dt4         AS in_src_strt_dt4,
                current_timestamp                                 AS edw_end_dttm,
                upd_tgt_indiv_name_retire.source_record_id
         FROM   upd_tgt_indiv_name_retire );
  -- Component tgt_indiv_name_retire, Type TARGET
  merge
  INTO         db_t_prod_core.indiv_name
  USING        exp_indiv_name_retire
  ON (
                            indiv_name.indiv_prty_id = exp_indiv_name_retire.lkp_indiv_prty_id3
               AND          indiv_name.name_type_cd = exp_indiv_name_retire.lkp_name_type_cd3
               AND          indiv_name.indiv_name_strt_dttm = exp_indiv_name_retire.lkp_indiv_name_strt_dt3
               AND          indiv_name.edw_strt_dttm = exp_indiv_name_retire.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    indiv_prty_id = exp_indiv_name_retire.lkp_indiv_prty_id3,
         name_type_cd = exp_indiv_name_retire.lkp_name_type_cd3,
         indiv_name_strt_dttm = exp_indiv_name_retire.lkp_indiv_name_strt_dt3,
         edw_strt_dttm = exp_indiv_name_retire.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_indiv_name_retire.edw_end_dttm,
         trans_end_dttm = exp_indiv_name_retire.in_src_strt_dt4;
  
  -- Component tgt_indiv_name_retire, Type Post SQL
  UPDATE db_t_prod_core.indiv_name
    SET    edw_end_dttm=a.lead1,
         trans_end_dttm=a.lead2
  FROM   (
                         SELECT DISTINCT indiv_prty_id,
                                         name_type_cd,
                                         indiv_name_strt_dttm,
                                         edw_strt_dttm,
                                         trans_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY indiv_prty_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1,
                                         max(trans_strt_dttm) over (PARTITION BY indiv_prty_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead2
                         FROM            db_t_prod_core.indiv_name
                         GROUP BY        indiv_prty_id,
                                         name_type_cd,
                                         indiv_name_strt_dttm,
                                         edw_strt_dttm,
                                         trans_strt_dttm ) a
  WHERE  indiv_name.edw_strt_dttm = a.edw_strt_dttm
  AND    indiv_name.trans_strt_dttm = a.trans_strt_dttm
  AND    indiv_name.indiv_prty_id = a.indiv_prty_id
  AND    indiv_name.name_type_cd = a.name_type_cd
  AND    indiv_name.indiv_name_strt_dttm = a.indiv_name_strt_dttm
  AND    cast(edw_end_dttm AS   DATE)=''9999-12-31''
  AND    cast(trans_end_dttm AS DATE)=''9999-12-31''
  AND    lead1 IS NOT NULL
  AND    lead2 IS NOT NULL;

END;
';