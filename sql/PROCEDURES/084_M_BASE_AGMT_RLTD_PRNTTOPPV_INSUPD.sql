-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_RLTD_PRNTTOPPV_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component LKP_TERADATA_ETL_REF_XLAT_AGMT_TYPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_agmt_type AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''AGMT_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_RSN_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_rsn_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''AGMT_RLTD_RSN''
         AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component SQ_pc_policyperiod, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_policyperiod AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS publicid,
                $2  AS policynumber,
                $3  AS updatetime,
                $4  AS periodstart,
                $5  AS periodend,
                $6  AS parentpolicynbr,
                $7  AS effectivedate,
                $8  AS expirationdate,
                $9  AS prnt_agmt_type,
                $10 AS ppv_agmt_type,
                $11 AS agmt_rltd_rsn_cd,
                $12 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT pc_policyperiod.publicid AS publicid,
                                                pc_policyperiod.policynumber,
                                                pc_policyperiod.updatetime,
                                                pc_policyperiod.periodstart ,
                                                pc_policyperiod.periodend,
                                                pcx_prefilladddata_alfa.policynumber AS parentpolicynbr,
                                                pc_effectivedatedfields.effectivedate ,
                                                pc_effectivedatedfields.expirationdate,
                                                ''AGMT_TYPE4''     AS prnt_agmt_type,
                                                ''AGMT_TYPE5''     AS ppv_agmt_type,
                                                ''AGMT_RLTD_RSN5'' AS agmt_rltd_rsn_cd
                                         FROM   (
                                                                SELECT DISTINCT pc_policyperiod.updatetime_stg   AS updatetime,
                                                                                pc_policyperiod.id_stg           AS id,
                                                                                pc_policyperiod.periodstart_stg  AS periodstart,
                                                                                pc_policyperiod.publicid_stg     AS publicid,
                                                                                pc_policyperiod.status_stg       AS status,
                                                                                pc_policyperiod.periodend_stg    AS periodend,
                                                                                pc_policyperiod.policynumber_stg AS policynumber
                                                                FROM            db_t_prod_stag.pc_policyperiod
                                                                WHERE           pc_policyperiod.updatetime_stg > ($start_dttm)
                                                                AND             pc_policyperiod.updatetime_stg <= ($end_dttm) ) pc_policyperiod
                                         join
                                                (
                                                                SELECT DISTINCT effdt.effectivedate_stg  AS effectivedate,
                                                                                effdt.expirationdate_stg AS expirationdate,
                                                                                CASE
                                                                                                WHEN effdt.legacydiscount_alfa_stg =''0'' THEN ''F''
                                                                                                WHEN effdt.legacydiscount_alfa_stg =''1'' THEN ''T''
                                                                                                ELSE NULL
                                                                                END                           AS legacydiscount_alfa,
                                                                                effdt.branchid_stg            AS branchid,
                                                                                effdt.prefilladddata_alfa_stg AS prefilladddata_alfa
                                                                                /*  EIM-17743 -  New Auto Product */
                                                                FROM            db_t_prod_stag.pc_effectivedatedfields AS effdt
                                                                left outer join db_t_prod_stag.pc_policyperiod pp
                                                                ON              pp.id_stg=effdt.branchid_stg
                                                                left outer join db_t_prod_stag.pc_job pcj
                                                                ON              pcj.id_stg = pp.jobid_stg
                                                                left outer join db_t_prod_stag.pctl_job pctlj
                                                                ON              pctlj.id_stg=pcj.subtype_stg
                                                                left join       db_t_prod_stag.pctl_policyperiodstatus pps
                                                                ON              pps.id_stg = pp.status_stg
                                                                WHERE           effdt.expirationdate_stg IS NULL
                                                                AND             pctlj.typecode_stg IN (''Cancellation'',
                                                                                                       ''PolicyChange'',
                                                                                                       ''Reinstatement'',
                                                                                                       ''Renewal'',
                                                                                                       ''Rewrite'',
                                                                                                       ''Submission'')
                                                                AND             pps.typecode_stg<>''Temporary''
                                                                AND             effdt.updatetime_stg > ($start_dttm)
                                                                AND             effdt.updatetime_stg <= ($end_dttm) ) pc_effectivedatedfields
                                         ON     pc_effectivedatedfields.branchid = pc_policyperiod.id
                                         join
                                                (
                                                       SELECT pcx_prefilladddata_alfa.id_stg           AS id,
                                                              pcx_prefilladddata_alfa.policynumber_stg AS policynumber
                                                       FROM   db_t_prod_stag.pcx_prefilladddata_alfa
                                                       WHERE  pcx_prefilladddata_alfa.updatetime_stg > ($start_dttm)
                                                       AND    pcx_prefilladddata_alfa.updatetime_stg <= ($end_dttm) ) pcx_prefilladddata_alfa
                                         ON     pcx_prefilladddata_alfa.id = pc_effectivedatedfields.prefilladddata_alfa
                                         join
                                                (
                                                       SELECT id_stg       AS id,
                                                              typecode_stg AS typecode
                                                       FROM   db_t_prod_stag.pctl_policyperiodstatus ) pctl_policyperiodstatus
                                         ON     pctl_policyperiodstatus.id = pc_policyperiod.status
                                         WHERE  pctl_policyperiodstatus.typecode = ''Bound''
                                         AND    pc_effectivedatedfields.legacydiscount_alfa = ''T''
                                         AND    pc_effectivedatedfields.expirationdate IS NULL ) src ) );
  -- Component exp_source_data, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_source_data AS
  (
            SELECT    sq_pc_policyperiod.publicid        AS publicid,
                      sq_pc_policyperiod.parentpolicynbr AS parentpolicynbr,
                      sq_pc_policyperiod.updatetime      AS pp_updatetime,
                      sq_pc_policyperiod.periodstart     AS pp_periodstart,
                      sq_pc_policyperiod.periodend       AS pp_periodend,
                      sq_pc_policyperiod.effectivedate   AS effectivedate,
                      sq_pc_policyperiod.expirationdate  AS expirationdate,
                      lkp_1.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_RSN_CD */
                      AS out_agmt_rltd_rsn_cd,
                      lkp_2.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_TYPE */
                      AS out_prnt_agmt_type,
                      lkp_3.tgt_idntftn_val
                      /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_TYPE */
                      AS out_ppv_agmt_type,
                      sq_pc_policyperiod.source_record_id,
                      row_number() over (PARTITION BY sq_pc_policyperiod.source_record_id ORDER BY sq_pc_policyperiod.source_record_id) AS rnk
            FROM      sq_pc_policyperiod
            left join lkp_teradata_etl_ref_xlat_rsn_cd lkp_1
            ON        lkp_1.src_idntftn_val = sq_pc_policyperiod.agmt_rltd_rsn_cd
            left join lkp_teradata_etl_ref_xlat_agmt_type lkp_2
            ON        lkp_2.src_idntftn_val = sq_pc_policyperiod.prnt_agmt_type
            left join lkp_teradata_etl_ref_xlat_agmt_type lkp_3
            ON        lkp_3.src_idntftn_val = sq_pc_policyperiod.ppv_agmt_type qualify rnk = 1 );
  -- Component LKP_AGMT_PPV_PLCY, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt_ppv_plcy AS
  (
            SELECT    lkp.agmt_id,
                      exp_source_data.source_record_id,
                      row_number() over(PARTITION BY exp_source_data.source_record_id ORDER BY lkp.agmt_id ASC) rnk
            FROM      exp_source_data
            left join
                      (
                                      SELECT DISTINCT agmt.agmt_id      AS agmt_id,
                                                      agmt.nk_src_key   AS nk_src_key,
                                                      agmt.agmt_type_cd AS agmt_type_cd
                                      FROM            db_t_prod_core.agmt
                                      WHERE           agmt_type_cd = ''PPV''
                                      AND             cast(agmt.edw_end_dttm AS DATE) = cast(''9999-12-31'' AS DATE)
                                                      /*  */
                      ) lkp
            ON        lkp.nk_src_key = exp_source_data.publicid
            AND       lkp.agmt_type_cd = exp_source_data.out_ppv_agmt_type 
			qualify row_number() over(PARTITION BY exp_source_data.source_record_id ORDER BY lkp.agmt_id ASC) 
			= 1 );
  -- Component LKP_AGMT_PRNT_PLCY, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt_prnt_plcy AS
  (
            SELECT    lkp.agmt_id,
                      exp_source_data.source_record_id,
                      row_number() over(PARTITION BY exp_source_data.source_record_id ORDER BY lkp.agmt_id ASC) rnk
            FROM      exp_source_data
            left join
                      (
                                      SELECT DISTINCT agmt.agmt_id       AS agmt_id,
                                                      agmt.host_agmt_num AS host_agmt_num,
                                                      agmt.agmt_type_cd  AS agmt_type_cd
                                      FROM            db_t_prod_core.agmt
                                      WHERE           agmt_type_cd = ''POL''
                                      AND             cast(agmt.edw_end_dttm AS DATE) = ''9999-12-31''
                                                      /*  */
                      ) lkp
            ON        lkp.host_agmt_num = exp_source_data.parentpolicynbr
            AND       lkp.agmt_type_cd = exp_source_data.out_prnt_agmt_type 
			qualify row_number() over(PARTITION BY exp_source_data.source_record_id ORDER BY lkp.agmt_id ASC) 
			= 1 );
  -- Component LKP_AGMT_RLTD, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt_rltd AS
  (
             SELECT     lkp.agmt_id,
                        lkp.rltd_agmt_id,
                        lkp.agmt_rltd_rsn_cd,
                        lkp.agmt_rltd_strt_dttm,
                        lkp.agmt_rltd_end_dttm,
                        lkp_agmt_ppv_plcy.agmt_id            AS in_rltd_agmt_id,
                        exp_source_data.out_agmt_rltd_rsn_cd AS in_rltd_rsn_cd,
                        exp_source_data.source_record_id,
                        row_number() over(PARTITION BY exp_source_data.source_record_id ORDER BY lkp.agmt_id ASC,lkp.rltd_agmt_id ASC,lkp.agmt_rltd_rsn_cd ASC,lkp.agmt_rltd_strt_dttm ASC,lkp.agmt_rltd_end_dttm ASC) rnk
             FROM       exp_source_data
             inner join lkp_agmt_ppv_plcy
             ON         exp_source_data.source_record_id = lkp_agmt_ppv_plcy.source_record_id
             left join
                        (
                               SELECT agmt_rltd.agmt_id             AS agmt_id,
                                      agmt_rltd.agmt_rltd_strt_dttm AS agmt_rltd_strt_dttm,
                                      agmt_rltd.agmt_rltd_end_dttm  AS agmt_rltd_end_dttm,
                                      agmt_rltd.rltd_agmt_id        AS rltd_agmt_id,
                                      agmt_rltd.agmt_rltd_rsn_cd    AS agmt_rltd_rsn_cd
                               FROM   db_t_prod_core.agmt_rltd
                               WHERE  agmt_rltd_rsn_cd=''PRNTTOPPV''
                               AND    cast(agmt_rltd.edw_end_dttm AS DATE) = ''9999-12-31''
                                      /*  */
                        ) lkp
             ON         lkp.rltd_agmt_id = lkp_agmt_ppv_plcy.agmt_id
             AND        lkp.agmt_rltd_rsn_cd = exp_source_data.out_agmt_rltd_rsn_cd 
			 qualify row_number() over(PARTITION BY exp_source_data.source_record_id ORDER BY lkp.agmt_id ASC,lkp.rltd_agmt_id ASC,lkp.agmt_rltd_rsn_cd ASC,lkp.agmt_rltd_strt_dttm ASC,lkp.agmt_rltd_end_dttm ASC) 
			 = 1 );
  -- Component exp_data_compare, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_compare AS
  (
             SELECT     lkp_agmt_prnt_plcy.agmt_id    AS in_agmt_id,
                        lkp_agmt_rltd.in_rltd_agmt_id AS in_rltd_agmt_id,
                        lkp_agmt_rltd.in_rltd_rsn_cd  AS in_rltd_rsn_cd,
                        CASE
                                   WHEN exp_source_data.effectivedate IS NULL THEN exp_source_data.pp_periodstart
                                   ELSE exp_source_data.effectivedate
                        END AS v_agmt_rltd_strt_dttm,
                        CASE
                                   WHEN exp_source_data.expirationdate IS NULL THEN exp_source_data.pp_periodend
                                   ELSE exp_source_data.expirationdate
                        END                                                                   AS v_agmt_rltd_end_dttm,
                        v_agmt_rltd_strt_dttm                                                 AS out_agmt_rltd_strt_dttm,
                        v_agmt_rltd_end_dttm                                                  AS out_agmt_rltd_end_dttm,
                        $prcs_id                                                              AS out_prcs_id,
                        current_timestamp                                                     AS out_edw_strt_dttm,
                        to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                        exp_source_data.pp_updatetime                                         AS pp_updatetime,
                        to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS out_trans_end_dttm,
                        md5 ( to_char ( lkp_agmt_rltd.agmt_id )
                                   || to_char ( lkp_agmt_rltd.agmt_rltd_strt_dttm , ''YYYY-MM-DD HH24:MI:SS.NS'' )
                                   || to_char ( lkp_agmt_rltd.agmt_rltd_end_dttm , ''YYYY-MM-DD HH24:MI:SS.NS'' ) ) AS lkp_checksum,
                        md5 ( to_char ( lkp_agmt_prnt_plcy.agmt_id )
                                   || to_char ( v_agmt_rltd_strt_dttm , ''YYYY-MM-DD HH24:MI:SS.NS'' )
                                   || to_char ( v_agmt_rltd_end_dttm , ''YYYY-MM-DD HH24:MI:SS.NS'' ) ) AS in_checksum,
                        CASE
                                   WHEN lkp_checksum IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN lkp_checksum != in_checksum THEN ''U''
                                                         ELSE ''R''
                                              END
                        END AS in_upd_flag,
                        exp_source_data.source_record_id
             FROM       exp_source_data
             inner join lkp_agmt_prnt_plcy
             ON         exp_source_data.source_record_id = lkp_agmt_prnt_plcy.source_record_id
             inner join lkp_agmt_rltd
             ON         lkp_agmt_prnt_plcy.source_record_id = lkp_agmt_rltd.source_record_id );
  
  -- Component rtr_AGMT_RLTD_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_agmt_rltd_insert as
  SELECT exp_data_compare.in_agmt_id              AS in_agmt_id,
         exp_data_compare.in_rltd_agmt_id         AS in_rltd_agmt_id,
         exp_data_compare.in_rltd_rsn_cd          AS in_rltd_rsn_cd,
         exp_data_compare.out_agmt_rltd_strt_dttm AS out_agmt_rltd_strt_dttm,
         exp_data_compare.out_agmt_rltd_end_dttm  AS out_agmt_rltd_end_dttm,
         exp_data_compare.out_prcs_id             AS out_prcs_id,
         exp_data_compare.out_edw_strt_dttm       AS out_edw_strt_dttm,
         exp_data_compare.out_edw_end_dttm        AS out_edw_end_dttm,
         exp_data_compare.pp_updatetime           AS pp_updatetime,
         exp_data_compare.out_trans_end_dttm      AS out_trans_end_dttm,
         exp_data_compare.in_upd_flag             AS in_upd_flag,
         exp_data_compare.source_record_id
  FROM   exp_data_compare
  WHERE  (
                exp_data_compare.in_upd_flag = ''I''
         OR     exp_data_compare.in_upd_flag = ''U'' )
  AND    exp_data_compare.in_agmt_id IS NOT NULL
  AND    exp_data_compare.in_rltd_agmt_id IS NOT NULL;
  
  -- Component upd_agmt_rltd_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_agmt_rltd_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_agmt_rltd_insert.in_agmt_id              AS in_agmt_id1,
                rtr_agmt_rltd_insert.in_rltd_agmt_id         AS in_rltd_agmt_id1,
                rtr_agmt_rltd_insert.in_rltd_rsn_cd          AS in_rltd_rsn_cd1,
                rtr_agmt_rltd_insert.out_agmt_rltd_strt_dttm AS effectivedate1,
                rtr_agmt_rltd_insert.out_agmt_rltd_end_dttm  AS expirationdate1,
                rtr_agmt_rltd_insert.out_prcs_id             AS out_prcs_id1,
                rtr_agmt_rltd_insert.out_edw_strt_dttm       AS out_edw_strt_dttm1,
                rtr_agmt_rltd_insert.out_edw_end_dttm        AS out_edw_end_dttm1,
                rtr_agmt_rltd_insert.pp_updatetime           AS pp_updatetime1,
                rtr_agmt_rltd_insert.out_trans_end_dttm      AS out_trans_end_dttm1,
                0                                            AS update_strategy_action,
				rtr_agmt_rltd_insert.source_record_id
         FROM   rtr_agmt_rltd_insert );
  -- Component AGMT_RLTD, Type TARGET
  INSERT INTO db_t_prod_core.agmt_rltd
              (
                          agmt_id,
                          rltd_agmt_id,
                          agmt_rltd_rsn_cd,
                          agmt_rltd_strt_dttm,
                          agmt_rltd_end_dttm,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT upd_agmt_rltd_ins.in_agmt_id1         AS agmt_id,
         upd_agmt_rltd_ins.in_rltd_agmt_id1    AS rltd_agmt_id,
         upd_agmt_rltd_ins.in_rltd_rsn_cd1     AS agmt_rltd_rsn_cd,
         upd_agmt_rltd_ins.effectivedate1      AS agmt_rltd_strt_dttm,
         upd_agmt_rltd_ins.expirationdate1     AS agmt_rltd_end_dttm,
         upd_agmt_rltd_ins.out_prcs_id1        AS prcs_id,
         upd_agmt_rltd_ins.out_edw_strt_dttm1  AS edw_strt_dttm,
         upd_agmt_rltd_ins.out_edw_end_dttm1   AS edw_end_dttm,
         upd_agmt_rltd_ins.pp_updatetime1      AS trans_strt_dttm,
         upd_agmt_rltd_ins.out_trans_end_dttm1 AS trans_end_dttm
  FROM   upd_agmt_rltd_ins;

END;
';