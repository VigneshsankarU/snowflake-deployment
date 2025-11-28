-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_LEGL_ACTN_STS_INSUPD("WORKLET_NAME" VARCHAR)
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

  -- Component SQ_cc_matter, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_cc_matter AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS publicid,
                $2 AS mattertype_typecode,
                $3 AS matterstatus_typecode,
                $4 AS legl_actn_sts_strt_dttm,
                $5 AS legl_actn_sts_end_dttm,
                $6 AS updatetime,
                $7 AS rnk,
                $8 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   publicid,
                                                    typecode_matter,
                                                    sts,
                                                    sts_dt,
                                                    end_dt,
                                                    updatetime,
                                                    rank() over( PARTITION BY publicid,typecode_matter ORDER BY sts_dt ) AS rnk
                                           FROM     (
                                                           SELECT cast(cc_subrogationsummary.publicid AS VARCHAR(64))  AS publicid ,
                                                                  cast (''LEGL_ACTN_TYPE1'' AS             VARCHAR (50)) AS typecode_matter ,
                                                                  CASE
                                                                         WHEN cc_subrogationsummary.description_stg = ''Subrogation Status changed to Open'' THEN ''Open''
                                                                                /*  EIM-40667 */
                                                                         WHEN cc_subrogationsummary.description_stg = ''Subrogation Status changed to Closed'' THEN ''Closed''
                                                                                /*  EIM-40667 */
                                                                         WHEN cc_subrogationsummary.description_stg = ''Subrogation Status changed to Closed - Opened in error'' THEN ''closed_open_error_alfa''
                                                                                /*  EIM-40667 */
                                                                         ELSE NULL
                                                                                /*  EIM-40667 */
                                                                  END sts,
                                                                  /*  EIM-40667  */
                                                                  cc_subrogationsummary.eventtimestamp_stg AS sts_dt,
                                                                  /*  EIM-40667 */
                                                                  cast(NULL AS timestamp)                  AS end_dt,
                                                                  cc_subrogationsummary.eventtimestamp_stg AS updatetime
                                                                  /*  EIM-40667 */
                                                           FROM   (
                                                                         SELECT cast(cc_subrogationsummary.publicid_stg AS VARCHAR(64)) AS publicid,
                                                                                cc_history.description_stg,
                                                                                /*  EIM-40667 */
                                                                                cc_history.eventtimestamp_stg
                                                                                /*  EIM-40667 */
                                                                         FROM   db_t_prod_stag.cc_subrogationsummary
                                                                         join
                                                                                (
                                                                                           SELECT     cc_claim.claimnumber_stg,
                                                                                                      cc_claim.datesubrogationopen_alfa_stg,
                                                                                                      /*cc_claim.SubrogationStatus_stg,*/
                                                                                                      cc_claim.state_stg,
                                                                                                      cc_claim.id_stg
                                                                                           FROM       db_t_prod_stag.cc_claim
                                                                                           inner join db_t_prod_stag.cctl_claimstate
                                                                                           ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                           WHERE      cctl_claimstate.name_stg <> ''Draft'' ) cc_claim
                                                                         ON     cc_claim.id_stg=cc_subrogationsummary.claimid_stg
                                                                         join   db_t_prod_stag.cc_subrogation
                                                                         ON     cc_subrogationsummary.id_stg=cc_subrogation.subrogationsummaryid_stg
                                                                         join   db_t_prod_stag.cc_history
                                                                         ON     cc_history.claimid_stg = cc_claim.id_stg
                                                                                /*  EIM-40667 */
                                                                         WHERE
                                                                                /*  EIM-40667 */
                                                                                (
                                                                                       /*  EIM-40667 */
                                                                                       (
                                                                                              cc_history.eventtimestamp_stg > (:START_DTTM)
                                                                                              /*  EIM-40667 */
                                                                                       AND    cc_history.eventtimestamp_stg <= (:END_DTTM))
                                                                                       /*  EIM-40667 */
                                                                                AND
                                                                                       /*  EIM-40667 */
                                                                                       (
                                                                                              cc_history.description_stg IN( ''Subrogation Status changed to Open'' ,
                                                                                                                            ''Subrogation Status changed to Closed'' ,
                                                                                                                            ''Subrogation Status changed to Closed - Opened in error'' ))
                                                                                       /*  EIM-40667 */
                                                                                )
                                                                                /*  EIM-40667 */
                                                                  ) cc_subrogationsummary
                                                           UNION
                                                           SELECT     cast(cc_matter.publicid AS VARCHAR(64))        AS publicid ,
                                                                      cctl_mattertype.typecode_stg                   AS typecode_matter,
                                                                      cctl_matterstatus.typecode_stg                 AS sts ,
                                                                      updatetime_litstatustypeline                   AS sts_dt,
                                                                      closedate                                      AS end_dt ,
                                                                      to_date(''01/01/1900'' , ''dd/mm/yyyy'') AS updatetime
                                                           FROM       (
                                                                                      SELECT          cast(cc_matter.publicid_stg AS VARCHAR(64)) AS publicid,
                                                                                                      cc_litstatustypeline.updatetime_stg         AS updatetime_litstatustypeline,
                                                                                                      cc_matter.claimid_stg                       AS claimid,
                                                                                                      cc_matter.id_stg                            AS id,
                                                                                                      cc_matter.closedate_stg                     AS closedate,
                                                                                                      cc_litstatustypeline.litigationstatus_stg   AS litigationstatus,
                                                                                                      cc_matter.mattertype_stg                    AS mattertype
                                                                                      FROM            db_t_prod_stag.cc_matter
                                                                                      join
                                                                                                      (
                                                                                                                 SELECT     cc_claim.id_stg
                                                                                                                 FROM       db_t_prod_stag.cc_claim
                                                                                                                 inner join db_t_prod_stag.cctl_claimstate
                                                                                                                 ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                                                 WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                      ON              cc_matter.claimid_stg=cc_claim.id_stg
                                                                                      left outer join db_t_prod_stag.cc_litstatustypeline
                                                                                      ON              cc_matter.id_stg=cc_litstatustypeline.matterid_stg
                                                                                      WHERE           cc_matter.updatetime_stg > (:START_DTTM)
                                                                                      AND             cc_matter.updatetime_stg <= (:END_DTTM) ) cc_matter
                                                           inner join db_t_prod_stag.cctl_mattertype
                                                           ON         cctl_mattertype.id_stg=cc_matter.mattertype
                                                           inner join db_t_prod_stag.cctl_matterstatus
                                                           ON         cctl_matterstatus.id_stg=cc_matter.litigationstatus )x ) src ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
         SELECT sq_cc_matter.publicid              AS claimnumber,
                sq_cc_matter.mattertype_typecode   AS mattertype_typecode,
                sq_cc_matter.matterstatus_typecode AS matterstatus_typecode,
                CASE
                       WHEN sq_cc_matter.legl_actn_sts_strt_dttm IS NULL THEN to_date ( ''1900/01/01'' , ''YYYY/MM/DD'' )
                       ELSE sq_cc_matter.legl_actn_sts_strt_dttm
                END AS o_legl_actn_sts_strt_dttm,
                CASE
                       WHEN sq_cc_matter.legl_actn_sts_end_dttm IS NULL THEN to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                       ELSE sq_cc_matter.legl_actn_sts_end_dttm
                END                     AS o_legl_actn_sts_end_dttm,
                ''SRC_SYS6''              AS src_idntftn_val,
                sq_cc_matter.updatetime AS updatetime,
                sq_cc_matter.rnk        AS rnk,
                sq_cc_matter.source_record_id
         FROM   sq_cc_matter );
  -- Component LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_sys_src_cd AS
  (
            SELECT    lkp.tgt_idntftn_val,
                      exp_pass_from_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.tgt_idntftn_val DESC,lkp.src_idntftn_val DESC) rnk
            FROM      exp_pass_from_source
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
                             AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                             AND    teradata_etl_ref_xlat.src_idntftn_sys= ''DS''
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = exp_pass_from_source.src_idntftn_val qualify rnk = 1 );
  -- Component LKP_TERADATA_ETL_REF_XLAT_CCTL_MATTERSTATUS, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_cctl_matterstatus AS
  (
            SELECT    lkp.tgt_idntftn_val,
                      lkp.src_idntftn_val,
                      exp_pass_from_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.tgt_idntftn_val DESC,lkp.src_idntftn_val DESC) rnk
            FROM      exp_pass_from_source
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''LEGL_ACTN_TYPE''
                             AND    teradata_etl_ref_xlat.src_idntftn_nm= ''cctl_mattertype.typecode''
                             AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = exp_pass_from_source.mattertype_typecode qualify rnk = 1 );
  -- Component LKP_TERADATA_ETL_REF_XLAT_CCTL_SUBROGATIONSTATUS, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_cctl_subrogationstatus AS
  (
            SELECT    lkp.tgt_idntftn_val,
                      lkp.src_idntftn_val,
                      exp_pass_from_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.tgt_idntftn_val DESC,lkp.src_idntftn_val DESC) rnk
            FROM      exp_pass_from_source
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''LEGL_ACTN_STS_TYPE''
                             AND    teradata_etl_ref_xlat.src_idntftn_nm= ''CCTL_SUBROGATIONSTATUS.TYPECODE''
                             AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = exp_pass_from_source.matterstatus_typecode qualify rnk = 1 );
  -- Component LKP_TERADATA_ETL_REF_XLAT11, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat11 AS
  (
            SELECT    lkp.tgt_idntftn_val,
                      lkp.src_idntftn_val,
                      exp_pass_from_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.tgt_idntftn_val DESC,lkp.src_idntftn_val DESC) rnk
            FROM      exp_pass_from_source
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''LEGL_ACTN_STS_TYPE''
                             AND    teradata_etl_ref_xlat.src_idntftn_nm= ''cctl_matterstatus.typecode''
                             AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = exp_pass_from_source.matterstatus_typecode qualify rnk = 1 );
  -- Component LKP_TERADATA_ETL_REF_XLAT_SBR, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_sbr AS
  (
            SELECT    lkp.tgt_idntftn_val,
                      lkp.src_idntftn_val,
                      exp_pass_from_source.source_record_id,
                      row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.tgt_idntftn_val DESC,lkp.src_idntftn_val DESC) rnk
            FROM      exp_pass_from_source
            left join
                      (
                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''LEGL_ACTN_TYPE''
                             AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                             AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) lkp
            ON        lkp.src_idntftn_val = exp_pass_from_source.mattertype_typecode qualify rnk = 1 );
  -- Component exp_flg_legl_actn_typecode, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_flg_legl_actn_typecode AS
  (
             SELECT
                        CASE
                                   WHEN lkp_teradata_etl_ref_xlat_cctl_matterstatus.tgt_idntftn_val IS NULL THEN lkp_teradata_etl_ref_xlat_sbr.tgt_idntftn_val
                                   ELSE lkp_teradata_etl_ref_xlat_cctl_matterstatus.tgt_idntftn_val
                        END AS v_typecode,
                        CASE
                                   WHEN v_typecode IS NULL THEN ''UNK''
                                   ELSE v_typecode
                        END AS out_typecode,
                        exp_pass_from_source.source_record_id
             FROM       exp_pass_from_source
             inner join lkp_teradata_etl_ref_xlat_cctl_matterstatus
             ON         exp_pass_from_source.source_record_id = lkp_teradata_etl_ref_xlat_cctl_matterstatus.source_record_id
             inner join lkp_teradata_etl_ref_xlat_sbr
             ON         lkp_teradata_etl_ref_xlat_cctl_matterstatus.source_record_id = lkp_teradata_etl_ref_xlat_sbr.source_record_id );
  -- Component LKP_LEGL_ACTN, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_legl_actn AS
  (
             SELECT     lkp.legl_actn_id,
                        exp_pass_from_source.source_record_id,
                        row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.legl_actn_id ASC,lkp.legl_actn_desc ASC,lkp.legl_actn_suit_num ASC,lkp.legl_actn_strt_dttm ASC,lkp.legl_actn_end_dttm ASC,lkp.court_loc_loctr_id ASC,lkp.legl_actn_type_cd ASC,lkp.legl_actn_suit_type_cd ASC,lkp.case_num ASC,lkp.bad_faith_ind ASC,lkp.subrgtn_rltd_ind ASC,lkp.prcs_id ASC,lkp.subrgtn_loan_ind ASC,lkp.wrt_off_amt ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.src_sys_cd ASC) rnk
             FROM       exp_pass_from_source
             inner join lkp_teradata_etl_ref_xlat_sys_src_cd
             ON         exp_pass_from_source.source_record_id = lkp_teradata_etl_ref_xlat_sys_src_cd.source_record_id
             inner join exp_flg_legl_actn_typecode
             ON         lkp_teradata_etl_ref_xlat_sys_src_cd.source_record_id = exp_flg_legl_actn_typecode.source_record_id
             left join
                        (
                                 SELECT   legl_actn.legl_actn_id           AS legl_actn_id,
                                          legl_actn.legl_actn_desc         AS legl_actn_desc,
                                          legl_actn.legl_actn_strt_dttm    AS legl_actn_strt_dttm,
                                          legl_actn.legl_actn_end_dttm     AS legl_actn_end_dttm,
                                          legl_actn.court_loc_loctr_id     AS court_loc_loctr_id,
                                          legl_actn.legl_actn_suit_type_cd AS legl_actn_suit_type_cd,
                                          legl_actn.case_num               AS case_num,
                                          legl_actn.bad_faith_ind          AS bad_faith_ind,
                                          legl_actn.subrgtn_rltd_ind       AS subrgtn_rltd_ind,
                                          legl_actn.prcs_id                AS prcs_id,
                                          legl_actn.subrgtn_loan_ind       AS subrgtn_loan_ind,
                                          legl_actn.wrt_off_amt            AS wrt_off_amt,
                                          legl_actn.edw_strt_dttm          AS edw_strt_dttm,
                                          legl_actn.edw_end_dttm           AS edw_end_dttm,
                                          legl_actn.legl_actn_suit_num     AS legl_actn_suit_num,
                                          legl_actn.src_sys_cd             AS src_sys_cd,
                                          legl_actn.legl_actn_type_cd      AS legl_actn_type_cd
                                 FROM     db_t_prod_core.legl_actn qualify row_number () over ( PARTITION BY legl_actn_suit_num,src_sys_cd,legl_actn_type_cd ORDER BY edw_end_dttm DESC)=1 ) lkp
             ON         lkp.legl_actn_suit_num = exp_pass_from_source.claimnumber
             AND        lkp.src_sys_cd = lkp_teradata_etl_ref_xlat_sys_src_cd.tgt_idntftn_val
             AND        lkp.legl_actn_type_cd = exp_flg_legl_actn_typecode.out_typecode 
			 qualify row_number() over(PARTITION BY exp_pass_from_source.source_record_id ORDER BY lkp.legl_actn_id ASC,lkp.legl_actn_desc ASC,lkp.legl_actn_suit_num ASC,lkp.legl_actn_strt_dttm ASC,lkp.legl_actn_end_dttm ASC,lkp.court_loc_loctr_id ASC,lkp.legl_actn_type_cd ASC,lkp.legl_actn_suit_type_cd ASC,lkp.case_num ASC,lkp.bad_faith_ind ASC,lkp.subrgtn_rltd_ind ASC,lkp.prcs_id ASC,lkp.subrgtn_loan_ind ASC,lkp.wrt_off_amt ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.src_sys_cd ASC) 
			 = 1 );
  -- Component exp_flg_legl_actn_sts_typecode, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_flg_legl_actn_sts_typecode AS
  (
             SELECT
                        CASE
                                   WHEN lkp_teradata_etl_ref_xlat11.tgt_idntftn_val IS NULL THEN ''UNK''
                                   ELSE lkp_teradata_etl_ref_xlat11.tgt_idntftn_val
                        END AS v_typecode_matter,
                        CASE
                                   WHEN lkp_teradata_etl_ref_xlat_cctl_subrogationstatus.tgt_idntftn_val IS NULL THEN ''UNK''
                                   ELSE lkp_teradata_etl_ref_xlat_cctl_subrogationstatus.tgt_idntftn_val
                        END AS v_typecode_subrg,
                        CASE
                                   WHEN v_typecode_matter = ''UNK'' THEN v_typecode_subrg
                                   ELSE v_typecode_matter
                        END AS legl_actn_sts_cd,
                        exp_pass_from_source.source_record_id
             FROM       exp_pass_from_source
             inner join lkp_teradata_etl_ref_xlat_cctl_subrogationstatus
             ON         exp_pass_from_source.source_record_id = lkp_teradata_etl_ref_xlat_cctl_subrogationstatus.source_record_id
             inner join lkp_teradata_etl_ref_xlat11
             ON         lkp_teradata_etl_ref_xlat_cctl_subrogationstatus.source_record_id = lkp_teradata_etl_ref_xlat11.source_record_id );
  -- Component exp_SRC_Fields, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_src_fields AS
  (
             SELECT     lkp_legl_actn.legl_actn_id                                             AS in_legl_actn_id,
                        exp_flg_legl_actn_sts_typecode.legl_actn_sts_cd                        AS in_legl_actn_sts_cd,
                        exp_pass_from_source.o_legl_actn_sts_strt_dttm                         AS in_legl_actn_sts_strt_dttm,
                        exp_pass_from_source.o_legl_actn_sts_end_dttm                          AS in_legl_actn_sts_end_dttm,
                        :prcs_id                                                               AS in_prcs_id,
                        current_timestamp                                                      AS in_edw_strt_dttm,
                        to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS in_edw_end_dttm,
                        exp_pass_from_source.updatetime                                        AS updatetime,
                        exp_pass_from_source.rnk                                               AS rnk,
                        exp_pass_from_source.source_record_id
             FROM       exp_pass_from_source
             inner join lkp_legl_actn
             ON         exp_pass_from_source.source_record_id = lkp_legl_actn.source_record_id
             inner join exp_flg_legl_actn_sts_typecode
             ON         lkp_legl_actn.source_record_id = exp_flg_legl_actn_sts_typecode.source_record_id );
  -- Component LKP_LEGL_ACTN_STS, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_legl_actn_sts AS
  (
            SELECT    lkp.legl_actn_id,
                      lkp.legl_actn_sts_cd,
                      lkp.legl_actn_sts_strt_dttm,
                      lkp.legl_actn_sts_end_dttm,
                      exp_src_fields.source_record_id,
                      row_number() over(PARTITION BY exp_src_fields.source_record_id ORDER BY lkp.legl_actn_id ASC,lkp.legl_actn_sts_cd ASC,lkp.legl_actn_sts_strt_dttm ASC,lkp.legl_actn_sts_end_dttm ASC) rnk
            FROM      exp_src_fields
            left join
                      (
                               SELECT   legl_actn_sts.legl_actn_id            AS legl_actn_id,
                                        legl_actn_sts.legl_actn_sts_cd        AS legl_actn_sts_cd,
                                        legl_actn_sts.legl_actn_sts_strt_dttm AS legl_actn_sts_strt_dttm,
                                        legl_actn_sts.legl_actn_sts_end_dttm  AS legl_actn_sts_end_dttm
                               FROM     db_t_prod_core.legl_actn_sts qualify row_number() over(PARTITION BY legl_actn_id ORDER BY edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.legl_actn_id = exp_src_fields.in_legl_actn_id qualify rnk = 1 );
  -- Component exp_CDC_Check, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_cdc_check AS
  (
             SELECT     exp_src_fields.in_legl_actn_id            AS in_legl_actn_id,
                        exp_src_fields.in_legl_actn_sts_cd        AS in_legl_actn_sts_cd,
                        exp_src_fields.in_legl_actn_sts_strt_dttm AS in_legl_actn_sts_strt_dttm,
                        exp_src_fields.in_legl_actn_sts_end_dttm  AS in_legl_actn_sts_end_dttm,
                        exp_src_fields.in_prcs_id                 AS in_prcs_id,
                        exp_src_fields.in_edw_strt_dttm           AS in_edw_strt_dttm,
                        exp_src_fields.in_edw_end_dttm            AS in_edw_end_dttm,
                        lkp_legl_actn_sts.legl_actn_sts_strt_dttm AS lkp_legl_actn_sts_strt_dttm,
                        NULL                                      AS lkp_edw_strt_dttm,
                        md5 ( exp_src_fields.in_legl_actn_sts_strt_dttm
                                   || exp_src_fields.in_legl_actn_sts_end_dttm
                                   || exp_src_fields.in_legl_actn_sts_cd ) AS v_src_md5,
                        md5 ( lkp_legl_actn_sts.legl_actn_sts_strt_dttm
                                   || lkp_legl_actn_sts.legl_actn_sts_end_dttm
                                   || lkp_legl_actn_sts.legl_actn_sts_cd ) AS v_tgt_md5,
                        CASE
                                   WHEN v_tgt_md5 IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN v_src_md5 = v_tgt_md5 THEN ''R''
                                                         ELSE ''U''
                                              END
                        END                AS o_cdc_check,
                        exp_src_fields.rnk AS rnk,
                        exp_src_fields.source_record_id
             FROM       exp_src_fields
             inner join lkp_legl_actn_sts
             ON         exp_src_fields.source_record_id = lkp_legl_actn_sts.source_record_id );
  -- Component rtr_legl_actn_sts_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_legl_actn_sts_insert as
  SELECT    exp_cdc_check.in_legl_actn_id             AS in_legl_actn_id,
            exp_cdc_check.in_legl_actn_sts_cd         AS in_legl_actn_sts_cd,
            exp_cdc_check.in_legl_actn_sts_strt_dttm  AS in_legl_actn_sts_strt_dttm,
            exp_cdc_check.in_legl_actn_sts_end_dttm   AS in_legl_actn_sts_end_dttm,
            exp_cdc_check.in_prcs_id                  AS in_prcs_id,
            exp_cdc_check.in_edw_strt_dttm            AS in_edw_strt_dttm,
            exp_cdc_check.in_edw_end_dttm             AS in_edw_end_dttm,
            exp_cdc_check.lkp_edw_strt_dttm           AS lkp_edw_strt_dttm,
            exp_src_fields.updatetime                 AS updatetime,
            exp_cdc_check.o_cdc_check                 AS o_cdc_check,
            exp_cdc_check.rnk                         AS rnk,
            exp_cdc_check.lkp_legl_actn_sts_strt_dttm AS lkp_legl_actn_sts_strt_dttm,
            exp_src_fields.source_record_id
  FROM      exp_src_fields
  left join exp_cdc_check
  ON        exp_src_fields.source_record_id = exp_cdc_check.source_record_id
  WHERE     (
                      exp_cdc_check.o_cdc_check = ''I'' )
  OR        (
                      exp_cdc_check.o_cdc_check = ''U''
            AND       exp_cdc_check.in_legl_actn_sts_strt_dttm > exp_cdc_check.lkp_legl_actn_sts_strt_dttm );
  
  -- Component exp_pass_to_tgt, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_tgt AS
  (
         SELECT rtr_legl_actn_sts_insert.in_legl_actn_id                                       AS in_legl_actn_id2,
                rtr_legl_actn_sts_insert.in_legl_actn_sts_cd                                   AS in_legl_actn_sts_cd2,
                rtr_legl_actn_sts_insert.in_legl_actn_sts_strt_dttm                            AS in_legl_actn_sts_strt_dttm2,
                rtr_legl_actn_sts_insert.in_legl_actn_sts_end_dttm                             AS in_legl_actn_sts_end_dttm2,
                rtr_legl_actn_sts_insert.in_prcs_id                                            AS in_prcs_id2,
                dateadd(''second'', ( 2 * ( rtr_legl_actn_sts_insert.rnk - 1 ) ), current_timestamp) AS in_edw_strt_dttm2,
                rtr_legl_actn_sts_insert.in_edw_end_dttm                                       AS in_edw_end_dttm2,
                rtr_legl_actn_sts_insert.updatetime                                            AS updatetime2,
                rtr_legl_actn_sts_insert.source_record_id
         FROM   rtr_legl_actn_sts_insert );
  -- Component tgt_legl_actn_sts_ins, Type TARGET
  INSERT INTO db_t_prod_core.legl_actn_sts
              (
                          legl_actn_id,
                          legl_actn_sts_cd,
                          legl_actn_sts_strt_dttm,
                          legl_actn_sts_end_dttm,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm
              )
  SELECT exp_pass_to_tgt.in_legl_actn_id2            AS legl_actn_id,
         exp_pass_to_tgt.in_legl_actn_sts_cd2        AS legl_actn_sts_cd,
         exp_pass_to_tgt.in_legl_actn_sts_strt_dttm2 AS legl_actn_sts_strt_dttm,
         exp_pass_to_tgt.in_legl_actn_sts_end_dttm2  AS legl_actn_sts_end_dttm,
         exp_pass_to_tgt.in_prcs_id2                 AS prcs_id,
         exp_pass_to_tgt.in_edw_strt_dttm2           AS edw_strt_dttm,
         exp_pass_to_tgt.in_edw_end_dttm2            AS edw_end_dttm,
         exp_pass_to_tgt.updatetime2                 AS trans_strt_dttm
  FROM   exp_pass_to_tgt;
  
  -- Component tgt_legl_actn_sts_ins, Type Post SQL
  UPDATE db_t_prod_core.legl_actn_sts
    SET    trans_end_dttm= a.lead,
         edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT legl_actn_id,
                                         legl_actn_sts_cd,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY legl_actn_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1,
                                         max(trans_strt_dttm) over (PARTITION BY legl_actn_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead
                         FROM            db_t_prod_core.legl_actn_sts ) a

  WHERE  legl_actn_sts.edw_strt_dttm = a.edw_strt_dttm
  AND    legl_actn_sts.legl_actn_id=a.legl_actn_id
  AND    legl_actn_sts.legl_actn_sts_cd=a.legl_actn_sts_cd
  AND    legl_actn_sts.trans_strt_dttm <>legl_actn_sts.trans_end_dttm
  AND    lead IS NOT NULL;

END;
';