-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_ELCTRNC_ADDR_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component LKP_TERADATA_ETL_REF_XLAT_ADDRESS_SBTYPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_address_sbtype AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''ADDR_SBTYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_loctr_sbtype AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''LOCTR_SBTYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component sq_pc_contact, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_contact AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS email,
                $2 AS createtime,
                $3 AS retired,
                $4 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   email,
                                                    createtime,
                                                    retired
                                           FROM    (
                                                           SELECT pc_contact.emailaddress1_stg AS email,
                                                                  pc_contact.createtime_stg    AS createtime,
                                                                  pc_contact.retired_stg       AS retired
                                                           FROM   db_t_prod_stag.pc_contact
                                                           WHERE  pc_contact.updatetime_stg> ($start_dttm)
                                                           AND    pc_contact.updatetime_stg <= ($end_dttm)
                                                           AND    pc_contact.retired_stg=0
                                                           AND    pc_contact.emailaddress1_stg IS NOT NULL
                                                           UNION
                                                           SELECT bc_contact.emailaddress1_stg AS email,
                                                                  bc_contact.createtime_stg    AS createtime,
                                                                  bc_contact.retired_stg       AS retired
                                                           FROM   db_t_prod_stag.bc_contact
                                                           WHERE  bc_contact.updatetime_stg > ($start_dttm)
                                                           AND    bc_contact.updatetime_stg <= ($end_dttm)
                                                           AND    bc_contact.retired_stg=0
                                                           AND    bc_contact.emailaddress1_stg IS NOT NULL
                                                           UNION
                                                           SELECT cc_contact.emailaddress1_stg AS email,
                                                                  cc_contact.createtime_stg    AS createtime,
                                                                  cc_contact.retired_stg       AS retired
                                                           FROM   db_t_prod_stag.cc_contact
                                                           WHERE  cc_contact.updatetime_stg > ($start_dttm)
                                                           AND    cc_contact.updatetime_stg <= ($end_dttm)
                                                           AND    cc_contact.retired_stg=0
                                                           AND    cc_contact.emailaddress1_stg IS NOT NULL
                                                           UNION
                                                           SELECT pc_contact.emailaddress2_stg AS email,
                                                                  pc_contact.createtime_stg    AS createtime,
                                                                  pc_contact.retired_stg       AS retired
                                                           FROM   db_t_prod_stag.pc_contact
                                                           WHERE  pc_contact.updatetime_stg> ($start_dttm)
                                                           AND    pc_contact.updatetime_stg <= ($end_dttm)
                                                           AND    pc_contact.retired_stg=0
                                                           AND    pc_contact.emailaddress2_stg IS NOT NULL
                                                           UNION
                                                           SELECT bc_contact.emailaddress2_stg AS email,
                                                                  bc_contact.createtime_stg    AS createtime,
                                                                  bc_contact.retired_stg       AS retired
                                                           FROM   db_t_prod_stag.bc_contact
                                                           WHERE  bc_contact.updatetime_stg > ($start_dttm)
                                                           AND    bc_contact.updatetime_stg <= ($end_dttm)
                                                           AND    bc_contact.retired_stg=0
                                                           AND    bc_contact.emailaddress2_stg IS NOT NULL
                                                           UNION
                                                           SELECT cc_contact.emailaddress2_stg AS email,
                                                                  cc_contact.createtime_stg    AS createtime,
                                                                  cc_contact.retired_stg       AS retired
                                                           FROM   db_t_prod_stag.cc_contact
                                                           WHERE  cc_contact.updatetime_stg > ($start_dttm)
                                                           AND    cc_contact.updatetime_stg <= ($end_dttm)
                                                           AND    cc_contact.retired_stg=0
                                                           AND    cc_contact.emailaddress2_stg IS NOT NULL ) t1 qualify row_number ( ) over ( PARTITION BY email COLLATE ''en-ci'' ORDER BY createtime ASC ) =1
                                                    /*  included qualify DB_T_PROD_CORE.stmt to store email creation date */
                                  ) src ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
         SELECT sq_pc_contact.email      AS emailaddress1,
                sq_pc_contact.createtime AS updatetime,
                sq_pc_contact.retired    AS retired,
                sq_pc_contact.source_record_id
         FROM   sq_pc_contact );
  -- Component LKP_ELCTRN_ADDR, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_elctrn_addr AS
  (
            SELECT    lkp.elctrnc_addr_id,
                      lkp.loctr_sbtype_cd,
                      lkp.addr_sbtype_cd,
                      lkp.elctrnc_addr_strt_dttm,
                      exp_pass_from_source.emailaddress1 AS emailaddress1,
                      exp_pass_from_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.elctrnc_addr_id ASC,lkp.loctr_sbtype_cd ASC,lkp.addr_sbtype_cd ASC,lkp.elctrnc_addr_strt_dttm ASC) rnk
            FROM      exp_pass_from_source
            left join
                      (
                             SELECT elctrnc_addr.elctrnc_addr_id        AS elctrnc_addr_id,
                                    elctrnc_addr.loctr_sbtype_cd        AS loctr_sbtype_cd,
                                    elctrnc_addr.addr_sbtype_cd         AS addr_sbtype_cd,
                                    elctrnc_addr.elctrnc_addr_strt_dttm AS elctrnc_addr_strt_dttm,
                                    elctrnc_addr.elctrnc_addr_txt       AS elctrnc_addr_txt
                             FROM   db_t_prod_core.elctrnc_addr
                             WHERE  cast(elctrnc_addr.edw_end_dttm AS DATE)=cast(''9999-12-31'' AS DATE) ) lkp
            ON        lkp.elctrnc_addr_txt = exp_pass_from_source.emailaddress1 
			qualify row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.elctrnc_addr_id ASC,lkp.loctr_sbtype_cd ASC,lkp.addr_sbtype_cd ASC,lkp.elctrnc_addr_strt_dttm ASC) 
			= 1 );
  -- Component exp_elctrn_addr, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_elctrn_addr AS
  (
             SELECT     exp_pass_from_source.emailaddress1 AS emailaddress1,
                        exp_pass_from_source.updatetime    AS updatetime,
                        seq_elctrnc_addr.NEXTVAL                           AS var_elctrnc_addr_id,
                        ''LOCTR_SBTYPE1''                    AS var_loctr_sbtype_val,
                        ''ADDR_SBTYPE4''                     AS var_address_sbtype_val,
                        CASE
                                   WHEN lkp_elctrn_addr.elctrnc_addr_id IS NULL THEN var_elctrnc_addr_id
                                   ELSE lkp_elctrn_addr.elctrnc_addr_id
                        END AS out_elctrnc_addr_id,
                        lkp_1.tgt_idntftn_val
                        /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_LOCTR_SBTYPE */
                                       AS v_loctr_sbtype,
                        v_loctr_sbtype AS out_loctr_sbtype,
                        lkp_2.tgt_idntftn_val
                        /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ADDRESS_SBTYPE */
                                         AS v_address_sbtype,
                        v_address_sbtype AS out_address_sbtype,
                        $prcs_id         AS out_process_id,
                        md5 ( rtrim ( ltrim ( v_loctr_sbtype ) )
                                   || rtrim ( ltrim ( v_address_sbtype ) )
                                   || ltrim ( rtrim ( exp_pass_from_source.updatetime ) ) ) AS calc_chksm,
                        md5 ( rtrim ( ltrim ( lkp_elctrn_addr.loctr_sbtype_cd ) )
                                   || rtrim ( ltrim ( lkp_elctrn_addr.addr_sbtype_cd ) )
                                   || ltrim ( rtrim ( lkp_elctrn_addr.elctrnc_addr_strt_dttm ) ) ) AS orig_chksm,
                        CASE
                                   WHEN orig_chksm IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN orig_chksm != calc_chksm THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                                                                    AS out_ins_upd,
                        current_timestamp                                                      AS edw_strt_dttm,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                        CASE
                                   WHEN exp_pass_from_source.updatetime IS NULL THEN to_date ( ''01/01/1000'' , ''MM/DD/YYYY'' )
                                   ELSE exp_pass_from_source.updatetime
                        END                          AS elctrnc_addr_strt_dt,
                        exp_pass_from_source.retired AS retired,
                        exp_pass_from_source.source_record_id,
                        row_number() over (PARTITION BY exp_pass_from_source.source_record_id ORDER BY exp_pass_from_source.source_record_id) AS rnk
             FROM       exp_pass_from_source
             inner join lkp_elctrn_addr
             ON         exp_pass_from_source.source_record_id = lkp_elctrn_addr.source_record_id
             left join  lkp_teradata_etl_ref_xlat_loctr_sbtype lkp_1
             ON         lkp_1.src_idntftn_val = var_loctr_sbtype_val
             left join  lkp_teradata_etl_ref_xlat_address_sbtype lkp_2
             ON         lkp_2.src_idntftn_val = var_address_sbtype_val 
			 qualify row_number() over (PARTITION BY exp_pass_from_source.source_record_id ORDER BY exp_pass_from_source.source_record_id) 
			 = 1 );
  -- Component rtr_elctrn_addr_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_elctrn_addr_insert as
  SELECT exp_elctrn_addr.emailaddress1        AS emailaddress1,
         exp_elctrn_addr.out_ins_upd          AS out_ins_upd,
         exp_elctrn_addr.out_process_id       AS out_process_id,
         exp_elctrn_addr.out_elctrnc_addr_id  AS out_elctrnc_addr_id,
         exp_elctrn_addr.out_loctr_sbtype     AS out_loctr_sbtype,
         exp_elctrn_addr.out_address_sbtype   AS out_address_sbtype,
         exp_elctrn_addr.edw_strt_dttm        AS edw_strt_dttm,
         exp_elctrn_addr.edw_end_dttm         AS edw_end_dttm,
         exp_elctrn_addr.elctrnc_addr_strt_dt AS elctrnc_addr_strt_dt,
         exp_elctrn_addr.retired              AS retired,
         exp_elctrn_addr.source_record_id
  FROM   exp_elctrn_addr
  WHERE  exp_elctrn_addr.out_ins_upd = ''I'';
  
  -- Component upd_elctrn_addr_ins_new, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_elctrn_addr_ins_new AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_elctrn_addr_insert.emailaddress1        AS emailaddress1,
                rtr_elctrn_addr_insert.out_process_id       AS o_process_id1,
                rtr_elctrn_addr_insert.out_elctrnc_addr_id  AS out_elctrnc_addr_id1,
                rtr_elctrn_addr_insert.out_loctr_sbtype     AS out_loctr_sbtype1,
                rtr_elctrn_addr_insert.out_address_sbtype   AS out_address_sbtype1,
                rtr_elctrn_addr_insert.edw_strt_dttm        AS edw_strt_dttm1,
                rtr_elctrn_addr_insert.edw_end_dttm         AS edw_end_dttm1,
                rtr_elctrn_addr_insert.elctrnc_addr_strt_dt AS elctrnc_addr_strt_dt1,
                rtr_elctrn_addr_insert.retired              AS retired1,
                0                                           AS update_strategy_action,
				rtr_elctrn_addr_insert.source_record_id
         FROM   rtr_elctrn_addr_insert );
  -- Component exp_elctrn_addr_ID_insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_elctrn_addr_id_insert AS
  (
         SELECT upd_elctrn_addr_ins_new.emailaddress1         AS emailaddress1,
                upd_elctrn_addr_ins_new.o_process_id1         AS o_process_id1,
                upd_elctrn_addr_ins_new.out_elctrnc_addr_id1  AS out_elctrnc_addr_id1,
                upd_elctrn_addr_ins_new.out_loctr_sbtype1     AS out_loctr_sbtype1,
                upd_elctrn_addr_ins_new.out_address_sbtype1   AS out_address_sbtype1,
                upd_elctrn_addr_ins_new.edw_strt_dttm1        AS edw_strt_dttm1,
                upd_elctrn_addr_ins_new.edw_end_dttm1         AS edw_end_dttm1,
                upd_elctrn_addr_ins_new.elctrnc_addr_strt_dt1 AS elctrnc_addr_strt_dt1,
                upd_elctrn_addr_ins_new.source_record_id
         FROM   upd_elctrn_addr_ins_new );
  -- Component trgt_elctrn_addr_ins_new, Type TARGET
  INSERT INTO db_t_prod_core.elctrnc_addr
              (
                          elctrnc_addr_id,
                          elctrnc_addr_txt,
                          loctr_sbtype_cd,
                          addr_sbtype_cd,
                          prcs_id,
                          elctrnc_addr_strt_dttm,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_elctrn_addr_id_insert.out_elctrnc_addr_id1  AS elctrnc_addr_id,
         exp_elctrn_addr_id_insert.emailaddress1         AS elctrnc_addr_txt,
         exp_elctrn_addr_id_insert.out_loctr_sbtype1     AS loctr_sbtype_cd,
         exp_elctrn_addr_id_insert.out_address_sbtype1   AS addr_sbtype_cd,
         exp_elctrn_addr_id_insert.o_process_id1         AS prcs_id,
         exp_elctrn_addr_id_insert.elctrnc_addr_strt_dt1 AS elctrnc_addr_strt_dttm,
         exp_elctrn_addr_id_insert.edw_strt_dttm1        AS edw_strt_dttm,
         exp_elctrn_addr_id_insert.edw_end_dttm1         AS edw_end_dttm
  FROM   exp_elctrn_addr_id_insert;

END;
';