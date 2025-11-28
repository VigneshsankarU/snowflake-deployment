-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_INDIV_OCPTN_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  END_DTTM STRING;
  START_DTTM STRING;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
  prcs_id int;
  GL_END_MTH_ID int;
  P_DEFAULT_STR_CD STRING;

BEGIN

 run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name)= upper(:worklet_name) order by insert_ts desc limit 1);   
 END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
 START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
GL_END_MTH_ID :=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''GL_END_MTH_ID'' order by insert_ts desc limit 1);
P_DEFAULT_STR_CD :=  (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''P_DEFAULT_STR_CD'' order by insert_ts desc limit 1);


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
  -- Component sq_indv_ocptn, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_indv_ocptn AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS addressbookuid,
                $2 AS occupation,
                $3 AS code,
                $4 AS updatetime,
                $5 AS src_end_dt,
                $6 AS retired,
                $7 AS createtime,
                $8 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   src_key,
                                                    occupation,
                                                    code_stg,
                                                    src_strt_dt,
                                                    src_end_dt,
                                                    retired_stg,
                                                    createtime_stg
                                           FROM     (
                                                           SELECT cast(ab.linkid_stg AS VARCHAR(100)) AS SRC_KEY,
                                                                  ab.typecode_stg                     AS Occupation,
                                                                  ''pc''                                AS code_stg,
                                                                  CASE
                                                                         WHEN ab.updatetime_stg IS NULL THEN cast(''1900-01-01 00:00:00.000000'' AS timestamp(6))
                                                                         ELSE ab.updatetime_stg
                                                                  END                                                AS src_strt_dt,
                                                                  cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS src_end_dt,
                                                                  ab.retired_stg,
                                                                  ab.createtime_stg
                                                           FROM   (
                                                                                  SELECT DISTINCT cast(ab_abcontact.linkid_stg AS VARCHAR(100))   linkid_stg,
                                                                                                  ''ContactManager''                             AS source_stg,
                                                                                                  ab_abcontact.updatetime_stg,
                                                                                                  ab_abcontact.subtype_stg,
                                                                                                  ab_abcontact.retired_stg,
                                                                                                  ab_abcontact.createtime_stg,
                                                                                                  abtl_abcontact.name_stg AS tl_cnt_name_stg,
                                                                                                  abtl_occupation.typecode_stg
                                                                                                  /* cctl_contact.typecode_stg */
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
                                                                                  WHERE           ab_abcontact.updatetime_stg>cast(:START_DTTM AS timestamp)
                                                                                  AND             ab_abcontact.updatetime_stg <= cast(:END_DTTM AS timestamp)
                                                                                  AND             (
                                                                                                                  source_stg = ''ContactManager''
                                                                                                  AND             ab_abcontact.linkid_stg IS NOT NULL)
                                                                                  AND             ab_abcontact.occupation_alfa_stg IS NOT NULL ) AS ab
                                                           WHERE  ab.tl_cnt_name_stg IN (''Person'',
                                                                                         ''Adjudicator'',
                                                                                         ''User Contact'',
                                                                                         ''Vendor (Person)'',
                                                                                         ''Attorney'',
                                                                                         ''Doctor'',
                                                                                         ''Policy Person'',
                                                                                         ''Contact'',
                                                                                         ''Lodging (Person)'')
                                                           UNION ALL
                                                           SELECT cc.publicid_stg AS SRC_KEY,
                                                                  cc.occupation,
                                                                  cc.code_stg,
                                                                  CASE
                                                                         WHEN cc.updatetime_stg IS NULL THEN cast(''1900-01-01 00:00:00.000000'' AS timestamp(6))
                                                                         ELSE cc.updatetime_stg
                                                                  END                                                AS src_strt_dt,
                                                                  cast(''9999-12-31 23:59:59.999999'' AS timestamp(6)) AS src_end_dt,
                                                                  cc.retired_stg,
                                                                  cc.createtime_stg
                                                           FROM   (
                                                                            SELECT    cast(cc_contact.publicid_stg AS VARCHAR(100)) publicid_stg,
                                                                                      cctl_occupation_alfa.typecode_stg             AS Occupation,
                                                                                      ''cc''                                          AS code_stg,
                                                                                      cc_contact.updatetime_stg,
                                                                                      cc_contact.retired_stg,
                                                                                      cc_contact.createtime_stg
                                                                            FROM      db_t_prod_stag.cc_contact
                                                                            left join db_t_prod_stag.cctl_vendoravailtype_alfa
                                                                            ON        cc_contact.vendoravailability_alfa_stg = cctl_vendoravailtype_alfa.id_stg
                                                                            join      db_t_prod_stag.cctl_occupation_alfa
                                                                            ON        cctl_occupation_alfa.id_stg = cc_contact.occupation_alfa_stg
                                                                            join      db_t_prod_stag.cctl_contact
                                                                            ON        cctl_contact.id_stg = cc_contact.subtype_stg
                                                                            WHERE     cc_contact.updatetime_stg > (:START_DTTM)
                                                                            AND       cc_contact.updatetime_stg <= (:END_DTTM)
                                                                            AND       cctl_contact.typecode_stg IN (''Person'',
                                                                                                                    ''Adjudicator'',
                                                                                                                    ''Contact'',
                                                                                                                    ''User Contact'')
                                                                            AND       cctl_occupation_alfa.typecode_stg IS NOT NULL ) AS cc ) AS a qualify row_number () over (PARTITION BY src_key ORDER BY src_strt_dt DESC)=1 ) src ) );
  -- Component LKP_TERADATA_ETL_REF_XLAT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat AS
  (
            SELECT    lkp.tgt_idntftn_val,
                      sq_indv_ocptn.source_record_id,
                      row_number() over(PARTITION BY sq_indv_ocptn.source_record_id ORDER BY lkp.tgt_idntftn_val DESC,lkp.src_idntftn_val DESC) rnk
            FROM      sq_indv_ocptn
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''OCPTN_TYPE''
                             AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = sq_indv_ocptn.occupation qualify rnk = 1 );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
             SELECT
                        CASE
                                   WHEN lkp_teradata_etl_ref_xlat.tgt_idntftn_val IS NULL THEN :P_DEFAULT_STR_CD
                                   ELSE lkp_teradata_etl_ref_xlat.tgt_idntftn_val
                        END AS out_occupation,
                        decode ( sq_indv_ocptn.code ,
                                ''cc'' , lkp_1.indiv_prty_id
                                /* replaced lookup LKP_INDIV_CLM_CTR */
                                ,
                                ''pc'' , lkp_2.indiv_prty_id
                                /* replaced lookup LKP_INDIV_CNT_MGR */
                                )                                                              AS out_prty_id,
                        :prcs_id                                                               AS out_prcs_id,
                        current_timestamp                                                      AS edw_strt_dttm,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                        CASE
                                   WHEN sq_indv_ocptn.updatetime IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )
                                   ELSE sq_indv_ocptn.updatetime
                        END AS o_src_strt_dt,
                        CASE
                                   WHEN sq_indv_ocptn.src_end_dt IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )
                                   ELSE sq_indv_ocptn.src_end_dt
                        END                      AS o_src_end_dt1,
                        sq_indv_ocptn.retired    AS retired,
                        sq_indv_ocptn.createtime AS createtime,
                        sq_indv_ocptn.source_record_id,
                        row_number() over (PARTITION BY sq_indv_ocptn.source_record_id ORDER BY sq_indv_ocptn.source_record_id) AS rnk
             FROM       sq_indv_ocptn
             inner join lkp_teradata_etl_ref_xlat
             ON         sq_indv_ocptn.source_record_id = lkp_teradata_etl_ref_xlat.source_record_id
             left join  lkp_indiv_clm_ctr lkp_1
             ON         lkp_1.nk_publc_id = sq_indv_ocptn.addressbookuid
             left join  lkp_indiv_cnt_mgr lkp_2
             ON         lkp_2.nk_link_id = sq_indv_ocptn.addressbookuid qualify rnk = 1 );
  -- Component LKP_INDIV_OCPTN, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_indiv_ocptn AS
  (
            SELECT    lkp.indiv_prty_id,
                      lkp.ocptn_type_cd,
                      lkp.indiv_ocptn_strt_dttm,
                      lkp.edw_strt_dttm,
                      lkp.edw_end_dttm,
                      exp_pass_from_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.indiv_prty_id ASC,lkp.ocptn_type_cd ASC,lkp.indiv_ocptn_strt_dttm ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) rnk
            FROM      exp_pass_from_source
            left join
                      (
                               SELECT   indiv_ocptn.ocptn_type_cd         AS ocptn_type_cd,
                                        indiv_ocptn.indiv_ocptn_strt_dttm AS indiv_ocptn_strt_dttm,
                                        indiv_ocptn.edw_strt_dttm         AS edw_strt_dttm,
                                        indiv_ocptn.edw_end_dttm          AS edw_end_dttm,
                                        indiv_ocptn.indiv_prty_id         AS indiv_prty_id
                               FROM     db_t_prod_core.indiv_ocptn qualify row_number() over( PARTITION BY indiv_prty_id ORDER BY edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.indiv_prty_id = exp_pass_from_source.out_prty_id qualify rnk = 1 );
  -- Component exp_compare_data, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_compare_data AS
  (
             SELECT     lkp_indiv_ocptn.indiv_prty_id         AS lkp_indiv_prty_id,
                        lkp_indiv_ocptn.ocptn_type_cd         AS lkp_ocptn_type_cd,
                        lkp_indiv_ocptn.indiv_ocptn_strt_dttm AS lkp_indiv_ocptn_strt_dt,
                        lkp_indiv_ocptn.edw_strt_dttm         AS lkp_edw_strt_dttm,
                        lkp_indiv_ocptn.edw_end_dttm          AS lkp_edw_end_dttm,
                        md5 ( ltrim ( rtrim ( to_char ( lkp_indiv_ocptn.indiv_ocptn_strt_dttm , ''MM/DD/YYYY'' ) ) )
                                   || ltrim ( rtrim ( to_char ( lkp_indiv_ocptn.ocptn_type_cd ) ) ) ) AS v_lkp_checksum,
                        exp_pass_from_source.out_occupation                                           AS in_ocptn_type_cd,
                        exp_pass_from_source.out_prty_id                                              AS in_indiv_prty_id,
                        exp_pass_from_source.out_prcs_id                                              AS in_prcs_id,
                        exp_pass_from_source.edw_strt_dttm                                            AS in_edw_strt_dttm,
                        exp_pass_from_source.edw_end_dttm                                             AS in_edw_end_dttm,
                        exp_pass_from_source.o_src_strt_dt                                            AS in_src_strt_dt,
                        exp_pass_from_source.o_src_end_dt1                                            AS in_src_end_dt,
                        md5 ( ltrim ( rtrim ( to_char ( exp_pass_from_source.createtime , ''MM/DD/YYYY'' ) ) )
                                   || ltrim ( rtrim ( to_char ( exp_pass_from_source.out_occupation ) ) ) ) AS v_in_checksum,
                        CASE
                                   WHEN v_lkp_checksum IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_lkp_checksum != v_in_checksum THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                             AS calc_ins_upd,
                        exp_pass_from_source.retired    AS retired,
                        exp_pass_from_source.createtime AS in_createtime,
                        exp_pass_from_source.source_record_id
             FROM       exp_pass_from_source
             inner join lkp_indiv_ocptn
             ON         exp_pass_from_source.source_record_id = lkp_indiv_ocptn.source_record_id );
  -- Component rtr_indiv_ocptn_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_indiv_ocptn_insert as
  SELECT exp_compare_data.in_indiv_prty_id        AS in_indiv_prty_id,
         exp_compare_data.in_ocptn_type_cd        AS in_ocptn_type_cd,
         exp_compare_data.in_src_strt_dt          AS in_src_strt_dt,
         exp_compare_data.in_src_end_dt           AS in_src_end_dt,
         exp_compare_data.in_prcs_id              AS in_prcs_id,
         exp_compare_data.in_edw_strt_dttm        AS in_edw_strt_dttm,
         exp_compare_data.in_edw_end_dttm         AS in_edw_end_dttm,
         exp_compare_data.lkp_indiv_prty_id       AS lkp_indiv_prty_id,
         exp_compare_data.lkp_ocptn_type_cd       AS lkp_ocptn_type_cd,
         exp_compare_data.lkp_edw_strt_dttm       AS lkp_edw_strt_dttm,
         exp_compare_data.calc_ins_upd            AS calc_ins_upd,
         exp_compare_data.lkp_indiv_ocptn_strt_dt AS lkp_indiv_ocptn_strt_dt,
         exp_compare_data.retired                 AS retired,
         exp_compare_data.in_createtime           AS in_createtime,
         exp_compare_data.lkp_edw_end_dttm        AS lkp_edw_end_dttm,
         exp_compare_data.source_record_id
  FROM   exp_compare_data
  WHERE  exp_compare_data.in_indiv_prty_id IS NOT NULL
  AND    ( (
                       exp_compare_data.calc_ins_upd = ''I'' )
         OR     (
                       exp_compare_data.retired = 0
                AND    exp_compare_data.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) )
         OR     (
                       exp_compare_data.calc_ins_upd = ''U''
                AND    exp_compare_data.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) );
  
  -- Component rtr_indiv_ocptn_RETIRE, Type ROUTER Output Group RETIRE
  create or replace temporary table rtr_indiv_ocptn_retire as
  SELECT exp_compare_data.in_indiv_prty_id        AS in_indiv_prty_id,
         exp_compare_data.in_ocptn_type_cd        AS in_ocptn_type_cd,
         exp_compare_data.in_src_strt_dt          AS in_src_strt_dt,
         exp_compare_data.in_src_end_dt           AS in_src_end_dt,
         exp_compare_data.in_prcs_id              AS in_prcs_id,
         exp_compare_data.in_edw_strt_dttm        AS in_edw_strt_dttm,
         exp_compare_data.in_edw_end_dttm         AS in_edw_end_dttm,
         exp_compare_data.lkp_indiv_prty_id       AS lkp_indiv_prty_id,
         exp_compare_data.lkp_ocptn_type_cd       AS lkp_ocptn_type_cd,
         exp_compare_data.lkp_edw_strt_dttm       AS lkp_edw_strt_dttm,
         exp_compare_data.calc_ins_upd            AS calc_ins_upd,
         exp_compare_data.lkp_indiv_ocptn_strt_dt AS lkp_indiv_ocptn_strt_dt,
         exp_compare_data.retired                 AS retired,
         exp_compare_data.in_createtime           AS in_createtime,
         exp_compare_data.lkp_edw_end_dttm        AS lkp_edw_end_dttm,
         exp_compare_data.source_record_id
  FROM   exp_compare_data
  WHERE  exp_compare_data.calc_ins_upd = ''R''
  AND    exp_compare_data.retired != 0
  AND    exp_compare_data.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_indiv_ocptn_insert, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_indiv_ocptn_insert AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_indiv_ocptn_insert.in_indiv_prty_id AS in_indiv_prty_id,
                rtr_indiv_ocptn_insert.in_ocptn_type_cd AS in_ocptn_type_cd,
                rtr_indiv_ocptn_insert.in_src_strt_dt   AS in_src_strt_dt,
                rtr_indiv_ocptn_insert.in_src_end_dt    AS in_src_end_dt,
                rtr_indiv_ocptn_insert.in_prcs_id       AS in_prcs_id,
                rtr_indiv_ocptn_insert.in_edw_strt_dttm AS in_edw_strt_dttm,
                rtr_indiv_ocptn_insert.in_edw_end_dttm  AS in_edw_end_dttm,
                rtr_indiv_ocptn_insert.retired          AS retired1,
                rtr_indiv_ocptn_insert.in_createtime    AS in_createtime3,
                0                                       AS update_strategy_action,
				source_record_id
         FROM   rtr_indiv_ocptn_insert );
  -- Component upd_indiv_ocptn_retire, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_indiv_ocptn_retire AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_indiv_ocptn_retire.in_prcs_id              AS in_prcs_id3,
                rtr_indiv_ocptn_retire.in_edw_strt_dttm        AS in_edw_strt_dttm3,
                rtr_indiv_ocptn_retire.lkp_indiv_prty_id       AS lkp_indiv_prty_id3,
                rtr_indiv_ocptn_retire.lkp_ocptn_type_cd       AS lkp_ocptn_type_cd3,
                rtr_indiv_ocptn_retire.lkp_edw_strt_dttm       AS lkp_edw_strt_dttm3,
                rtr_indiv_ocptn_retire.lkp_indiv_ocptn_strt_dt AS lkp_indiv_ocptn_strt_dt3,
                NULL                                           AS lkp_indiv_ocptn_end_dt3,
                rtr_indiv_ocptn_retire.in_createtime           AS in_createtime4,
                rtr_indiv_ocptn_retire.in_src_strt_dt          AS in_src_strt_dt4,
                rtr_indiv_ocptn_retire.in_src_end_dt           AS in_src_end_dt4,
                1                                              AS update_strategy_action,
				source_record_id
         FROM   rtr_indiv_ocptn_retire );
  -- Component exp_indiv_ocptn_update1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_indiv_ocptn_update1 AS
  (
         SELECT upd_indiv_ocptn_retire.lkp_indiv_prty_id3 AS lkp_indiv_prty_id3,
                upd_indiv_ocptn_retire.lkp_edw_strt_dttm3 AS lkp_edw_strt_dttm3,
                current_timestamp                         AS edw_end_dttm,
                upd_indiv_ocptn_retire.in_src_strt_dt4    AS in_src_strt_dt4,
                upd_indiv_ocptn_retire.source_record_id
         FROM   upd_indiv_ocptn_retire );
  -- Component exp_indiv_ocptn_insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_indiv_ocptn_insert AS
  (
         SELECT upd_indiv_ocptn_insert.in_indiv_prty_id AS in_indiv_prty_id,
                upd_indiv_ocptn_insert.in_ocptn_type_cd AS in_ocptn_type_cd,
                upd_indiv_ocptn_insert.in_src_strt_dt   AS in_src_strt_dt,
                upd_indiv_ocptn_insert.in_src_end_dt    AS in_src_end_dt,
                upd_indiv_ocptn_insert.in_prcs_id       AS in_prcs_id,
                upd_indiv_ocptn_insert.in_edw_strt_dttm AS in_edw_strt_dttm,
                CASE
                       WHEN upd_indiv_ocptn_insert.retired1 = 0 THEN upd_indiv_ocptn_insert.in_edw_end_dttm
                       ELSE current_timestamp
                END AS out_edw_end_dttm1,
                CASE
                       WHEN upd_indiv_ocptn_insert.retired1 != 0 THEN upd_indiv_ocptn_insert.in_src_strt_dt
                       ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END                                   AS trns_end_dt,
                upd_indiv_ocptn_insert.in_createtime3 AS in_createtime3,
                upd_indiv_ocptn_insert.source_record_id
         FROM   upd_indiv_ocptn_insert );
  -- Component tgt_indiv_ocptn_retire, Type TARGET
  merge
  INTO         db_t_prod_core.indiv_ocptn
  USING        exp_indiv_ocptn_update1
  ON (
                            indiv_ocptn.indiv_prty_id = exp_indiv_ocptn_update1.lkp_indiv_prty_id3
               AND          indiv_ocptn.edw_strt_dttm = exp_indiv_ocptn_update1.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    indiv_prty_id = exp_indiv_ocptn_update1.lkp_indiv_prty_id3,
         edw_strt_dttm = exp_indiv_ocptn_update1.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_indiv_ocptn_update1.edw_end_dttm,
         trans_end_dttm = exp_indiv_ocptn_update1.in_src_strt_dt4;
  
  -- Component tgt_indiv_ocptn_retire, Type Post SQL
  UPDATE db_t_prod_core.indiv_ocptn
    SET    edw_end_dttm=a.lead1 ,
         trans_end_dttm=a.lead2
  FROM   (
                         SELECT DISTINCT indiv_prty_id,
                                         edw_strt_dttm ,
                                         trans_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY indiv_prty_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)     - interval ''1 second'' AS lead1,
                                         max(trans_strt_dttm) over (PARTITION BY indiv_prty_id ORDER BY trans_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead2
                         FROM            db_t_prod_core.indiv_ocptn ) a
  WHERE  indiv_ocptn.indiv_prty_id=a.indiv_prty_id
  AND    indiv_ocptn.edw_strt_dttm=a.edw_strt_dttm
  AND    cast(indiv_ocptn.edw_end_dttm AS   DATE)=''9999-12-31''
  AND    cast(indiv_ocptn.trans_end_dttm AS DATE)=''9999-12-31''
  AND    lead1 IS NOT NULL
  AND    lead2 IS NOT NULL;
  
  -- Component tgt_indiv_ocptn_insert, Type TARGET
  INSERT INTO db_t_prod_core.indiv_ocptn
              (
                          indiv_prty_id,
                          ocptn_type_cd,
                          indiv_ocptn_strt_dttm,
                          indiv_ocptn_end_dttm,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_indiv_ocptn_insert.in_indiv_prty_id  AS indiv_prty_id,
         exp_indiv_ocptn_insert.in_ocptn_type_cd  AS ocptn_type_cd,
         exp_indiv_ocptn_insert.in_createtime3    AS indiv_ocptn_strt_dttm,
         exp_indiv_ocptn_insert.in_src_end_dt     AS indiv_ocptn_end_dttm,
         exp_indiv_ocptn_insert.in_prcs_id        AS prcs_id,
         exp_indiv_ocptn_insert.in_edw_strt_dttm  AS edw_strt_dttm,
         exp_indiv_ocptn_insert.out_edw_end_dttm1 AS edw_end_dttm,
         exp_indiv_ocptn_insert.in_src_strt_dt    AS trans_strt_dttm,
         exp_indiv_ocptn_insert.trns_end_dt       AS trans_end_dttm
  FROM   exp_indiv_ocptn_insert;

END;
';