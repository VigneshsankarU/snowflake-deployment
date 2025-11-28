-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CNTCT_XREF_INS("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component LKP_DIR_PRTY_BC_COMPANY, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_dir_prty_bc_company AS
  (
         SELECT prty_id,
                nk_busn_val
         FROM   db_t_prod_core.dir_prty );
  -- Component LKP_DIR_PRTY_BC_COMPANY_RLTD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_dir_prty_bc_company_rltd AS
  (
         SELECT prty_id,
                nk_busn_val
         FROM   db_t_prod_core.dir_prty );
  -- Component LKP_DIR_PRTY_BC_PERSON, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_dir_prty_bc_person AS
  (
         SELECT prty_id,
                nk_publc_id
         FROM   db_t_prod_core.dir_prty );
  -- Component LKP_DIR_PRTY_BC_PERSON_RLTD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_dir_prty_bc_person_rltd AS
  (
         SELECT prty_id,
                nk_lnk_id
         FROM   db_t_prod_core.dir_prty );
  -- Component sq_CNTCT_XREF, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_cntct_xref AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS src_id,
                $2 AS pc_id,
                $3 AS contact_typ,
                $4 AS contact_src,
                $5 AS rltnshp_typ,
                $6 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     ( select cast(trim(src_publicid) AS VARCHAR(64)) AS src_id ,cast(trim(linkid) AS VARCHAR(64)) AS pc_id, upper(typecode_stg) AS contact_typ ,cast(source AS VARCHAR(50)) AS contact_src ,cast(rltd AS VARCHAR(50)) rltnshp_typ FROM (
                                                  SELECT DISTINCT
                                                                  CASE
                                                                                  WHEN bc.externalid_stg IS NOT NULL THEN bc.externalid_stg
                                                                                  ELSE bc.publicid_stg
                                                                  END                   AS src_publicid,
                                                                  bc.addressbookuid_stg AS linkid,
                                                                  bctl.typecode_stg,
                                                                  ''BC''     AS source,
                                                                  ''BCTOCM'' AS rltd
                                                  FROM            db_t_prod_stag.bc_contact bc
                                                  join            db_t_prod_stag.pc_contact pc
                                                  ON              pc.addressbookuid_stg =bc.addressbookuid_stg
                                                  join            db_t_prod_stag.bctl_contact bctl
                                                  ON              bctl.id_stg = bc.subtype_stg
                                                  WHERE           (
                                                                                  bc.updatetime_stg > $start_dttm
                                                                  AND             bc.updatetime_stg <= $end_dttm )
                                                  OR              (
                                                                                  pc.updatetime_stg > $start_dttm
                                                                  AND             pc.updatetime_stg <= $end_dttm )
                                                  UNION
                                                  SELECT DISTINCT cc.publicid_stg       AS src_publicid,
                                                                  cc.addressbookuid_stg AS linkid,
                                                                  cctl.typecode_stg,
                                                                  ''CC''     AS source,
                                                                  ''CCTOCM'' AS rltd
                                                  FROM            db_t_prod_stag.cc_contact cc
                                                  join            db_t_prod_stag.pc_contact pc
                                                  ON              pc.addressbookuid_stg=cc.addressbookuid_stg
                                                  join            db_t_prod_stag.cctl_contact cctl
                                                  ON              cctl.id_stg =cc.subtype_stg
                                                  WHERE           (
                                                                                  cc.updatetime_stg > $start_dttm
                                                                  AND             cc.updatetime_stg <= $end_dttm)
                                                  OR              (
                                                                                  pc.updatetime_stg > $start_dttm
                                                                  AND             pc.updatetime_stg <= $end_dttm)
                                                  UNION
                                                  SELECT DISTINCT ab.linkid_stg         AS src_publicid,
                                                                  pc.addressbookuid_stg AS linkid,
                                                                  abtl.typecode_stg,
                                                                  ''CM''     AS source,
                                                                  ''CMTOPC'' AS rltd
                                                  FROM            db_t_prod_stag.ab_abcontact ab
                                                  join            db_t_prod_stag.pc_contact pc
                                                  ON              ab.linkid_stg = pc.addressbookuid_stg
                                                  join            db_t_prod_stag.abtl_abcontact abtl
                                                  ON              abtl.id_stg = ab.subtype_stg
                                                  WHERE           (
                                                                                  ab.updatetime_stg > $start_dttm
                                                                  AND             ab.updatetime_stg <= $end_dttm )
                                                  OR              (
                                                                                  pc.updatetime_stg > $start_dttm
                                                                  AND             pc.updatetime_stg <= $end_dttm ) ) src ORDER BY 1,2,3,4,5 ) src ) );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
            SELECT    sq_cntct_xref.contact_typ AS contact_typ,
                      sq_cntct_xref.rltnshp_typ AS cntct_rltnshp_type_cd,
                      decode ( TRUE ,
                              sq_cntct_xref.contact_src = ''BC''
                    AND       sq_cntct_xref.contact_typ = ''PERSON'' , lkp_1.prty_id
                              /* replaced lookup LKP_DIR_PRTY_BC_PERSON */
                              ,
                              decode ( TRUE ,
                                      sq_cntct_xref.contact_src = ''BC''
                            AND       sq_cntct_xref.contact_typ = ''COMPANY'' , lkp_2.prty_id
                                      /* replaced lookup LKP_DIR_PRTY_BC_COMPANY */
                                      ,
                                      decode ( TRUE ,
                                              sq_cntct_xref.contact_src = ''CC''
                                    AND       sq_cntct_xref.contact_typ = ''PERSON'' , lkp_3.prty_id
                                              /* replaced lookup LKP_DIR_PRTY_BC_PERSON */
                                              ,
                                              decode ( TRUE ,
                                                      sq_cntct_xref.contact_src = ''CC''
                                            AND       sq_cntct_xref.contact_typ = ''COMPANY'' , lkp_4.prty_id
                                                      /* replaced lookup LKP_DIR_PRTY_BC_COMPANY */
                                                      ,
                                                      NULL ) ) ) ) AS prty_id,
                      decode ( TRUE ,
                              sq_cntct_xref.contact_src = ''BC''
                    AND       sq_cntct_xref.contact_typ = ''PERSON'' , lkp_5.prty_id
                              /* replaced lookup LKP_DIR_PRTY_BC_PERSON_RLTD */
                              ,
                              decode ( TRUE ,
                                      sq_cntct_xref.contact_src = ''BC''
                            AND       sq_cntct_xref.contact_typ = ''COMPANY'' , lkp_6.prty_id
                                      /* replaced lookup LKP_DIR_PRTY_BC_COMPANY_RLTD */
                                      ,
                                      decode ( TRUE ,
                                              sq_cntct_xref.contact_src = ''CC''
                                    AND       sq_cntct_xref.contact_typ = ''PERSON'' , lkp_7.prty_id
                                              /* replaced lookup LKP_DIR_PRTY_BC_PERSON_RLTD */
                                              ,
                                              decode ( TRUE ,
                                                      sq_cntct_xref.contact_src = ''CC''
                                            AND       sq_cntct_xref.contact_typ = ''COMPANY'' , lkp_8.prty_id
                                                      /* replaced lookup LKP_DIR_PRTY_BC_COMPANY_RLTD */
                                                      ,
                                                      NULL ) ) ) )                           AS rltd_prty_id,
                      $prcs_id                                                               AS prcs_id,
                      current_timestamp                                                      AS edw_strt_dttm,
                      to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                      sq_cntct_xref.source_record_id,
                      row_number() over (PARTITION BY sq_cntct_xref.source_record_id ORDER BY sq_cntct_xref.source_record_id) AS rnk
            FROM      sq_cntct_xref
            left join lkp_dir_prty_bc_person lkp_1
            ON        lkp_1.nk_publc_id = sq_cntct_xref.src_id
            left join lkp_dir_prty_bc_company lkp_2
            ON        lkp_2.nk_busn_val = sq_cntct_xref.src_id
            left join lkp_dir_prty_bc_person lkp_3
            ON        lkp_3.nk_publc_id = sq_cntct_xref.src_id
            left join lkp_dir_prty_bc_company lkp_4
            ON        lkp_4.nk_busn_val = sq_cntct_xref.src_id
            left join lkp_dir_prty_bc_person_rltd lkp_5
            ON        lkp_5.nk_lnk_id = sq_cntct_xref.pc_id
            left join lkp_dir_prty_bc_company_rltd lkp_6
            ON        lkp_6.nk_busn_val = sq_cntct_xref.pc_id
            left join lkp_dir_prty_bc_person_rltd lkp_7
            ON        lkp_7.nk_lnk_id = sq_cntct_xref.pc_id
            left join lkp_dir_prty_bc_company_rltd lkp_8
            ON        lkp_8.nk_busn_val = sq_cntct_xref.pc_id qualify rnk = 1 );
  -- Component LKP_CNT_XREF, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_cnt_xref AS
  (
            SELECT    lkp.prty_id,
                      lkp.rltd_prty_id,
                      exp_data_transformation.source_record_id,
                      row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.prty_id ASC,lkp.rltd_prty_id ASC) rnk
            FROM      exp_data_transformation
            left join
                      (
                             SELECT cntct_xref.prty_id      AS prty_id,
                                    cntct_xref.rltd_prty_id AS rltd_prty_id
                             FROM   db_t_prod_core.cntct_xref
                             WHERE  cast(edw_end_dttm AS DATE)=''9999-12-31'' ) lkp
            ON        lkp.prty_id = exp_data_transformation.prty_id
            AND       lkp.rltd_prty_id = exp_data_transformation.rltd_prty_id 
            qualify row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.prty_id ASC,lkp.rltd_prty_id ASC) 
            = 1 );
  -- Component exp_CDC_check, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_cdc_check AS
  (
             SELECT     exp_data_transformation.prty_id               AS prty_id,
                        exp_data_transformation.rltd_prty_id          AS rltd_prty_id,
                        exp_data_transformation.contact_typ           AS cntct_type_cd,
                        exp_data_transformation.cntct_rltnshp_type_cd AS cntct_rltnshp_type_cd,
                        exp_data_transformation.prcs_id               AS prcs_id,
                        exp_data_transformation.edw_strt_dttm         AS edw_strt_dttm,
                        exp_data_transformation.edw_end_dttm          AS edw_end_dttm,
                        CASE
                                   WHEN lkp_cnt_xref.prty_id IS NULL
                                   AND        lkp_cnt_xref.rltd_prty_id IS NULL THEN ''I''
                                   ELSE ''R''
                        END AS cdc_chk,
                        exp_data_transformation.source_record_id
             FROM       exp_data_transformation
             inner join lkp_cnt_xref
             ON         exp_data_transformation.source_record_id = lkp_cnt_xref.source_record_id );
  -- Component fltr_Ins_Flag, Type FILTER
  CREATE
  OR
  replace TEMPORARY TABLE fltr_ins_flag AS
  (
         SELECT exp_cdc_check.prty_id               AS prty_id,
                exp_cdc_check.rltd_prty_id          AS rltd_prty_id,
                exp_cdc_check.cntct_type_cd         AS cntct_type_cd,
                exp_cdc_check.cntct_rltnshp_type_cd AS cntct_rltnshp_type_cd,
                exp_cdc_check.prcs_id               AS prcs_id,
                exp_cdc_check.edw_strt_dttm         AS edw_strt_dttm,
                exp_cdc_check.edw_end_dttm          AS edw_end_dttm,
                exp_cdc_check.cdc_chk               AS cdc_chk,
                exp_cdc_check.source_record_id
         FROM   exp_cdc_check
         WHERE  exp_cdc_check.prty_id IS NOT NULL
         AND    exp_cdc_check.rltd_prty_id IS NOT NULL
         AND    exp_cdc_check.cdc_chk = ''I'' );
  -- Component CNTCT_XREF_ins, Type TARGET
  INSERT INTO db_t_prod_core.cntct_xref
              (
                          prty_id,
                          rltd_prty_id,
                          cntct_type_cd,
                          cntct_rltnshp_type_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT fltr_ins_flag.prty_id               AS prty_id,
         fltr_ins_flag.rltd_prty_id          AS rltd_prty_id,
         fltr_ins_flag.cntct_type_cd         AS cntct_type_cd,
         fltr_ins_flag.cntct_rltnshp_type_cd AS cntct_rltnshp_type_cd,
         fltr_ins_flag.prcs_id               AS prcs_id,
         fltr_ins_flag.edw_strt_dttm         AS edw_strt_dttm,
         fltr_ins_flag.edw_end_dttm          AS edw_end_dttm
  FROM   fltr_ins_flag;

END;
';