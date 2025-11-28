-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AGMT_STS_POLICYNUMBER_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_RSN_CD, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_agmt_sts_rsn_cd AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''AGMT_STS_RSN_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm IN (''pctl_reasoncode.typecode'',
                                                         ''pctl_reasoncode.TYPECODE'')
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_SRC_TYPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_agmt_sts_src_type AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''AGMT_STS_SRC_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm IN (''pctl_cancellationsource.typecode'')
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'' );
  -- Component LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_TYPE, Type Prerequisite Lookup Object
  CREATE
  OR
  replace TEMPORARY TABLE lkp_teradata_etl_ref_xlat_agmt_sts_type AS
  (
         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
         FROM   db_t_prod_core.teradata_etl_ref_xlat
         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''AGMT_STS_TYPE''
         AND    teradata_etl_ref_xlat.src_idntftn_nm IN (''out_EDWPolicyStatus_PC.PolicyStatus'',
                                                         ''cctl_policystatus.typecode'')
         AND    teradata_etl_ref_xlat.src_idntftn_sys=''GW''
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
  -- Component SQ_out_EDWPolicyStatus_PC, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_out_edwpolicystatus_pc AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS policynumber,
                $2  AS policystatus,
                $3  AS policystatus_dttm,
                $4  AS modelnumber,
                $5  AS termnumber,
                $6  AS creationts,
                $7  AS policyperiodid,
                $8  AS src_cd,
                $9  AS source_name,
                $10 AS typecode,
                $11 AS retired,
                $12 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                           SELECT   cast (policynumber AS VARCHAR (60)) AS policynumber,
                                                    cast (policystatus AS VARCHAR (60)) AS policystatus,
                                                    policystatus_dttm,
                                                    cast (modelnumber AS VARCHAR (60)) AS modelnumber,
                                                    cast (termnumber AS  VARCHAR (60)) AS termnumber,
                                                    creationts                         AS creationts,
                                                    policyperiodid,
                                                    ''SRC_SYS4'' AS src_cd,
                                                    source_name,
                                                    typecode,
                                                    retired
                                           FROM     (
                                                              SELECT
                                                                        /*a.ID, BatchID, RootEntity, EventName, CreationTS, CreationUID, UpdateTS,
UpdateUID, */
                                                                        policynumber,
                                                                        /*AccountNumber,*/
                                                                        termnumber,
                                                                        /*PolicyPeriodID, PolicyEffectiveDate,
PolicyExpirationDate,*/
                                                                        upper(policystatus) policystatus ,
                                                                        CASE
                                                                                  WHEN (
                                                                                                      policystatus = ''CANCELLED''
                                                                                            OR        policystatus =''CANCELED'') THEN cancellationdate
                                                                                  WHEN policystatus = ''IN FORCE'' THEN editeffectivedate
                                                                                  WHEN policystatus = ''EXPIRED'' THEN periodend
                                                                                  WHEN (
                                                                                                      policystatus IN (''PENDING CONFIRMATION'',
                                                                                                                       ''RENEWAL LAPSED'',
                                                                                                                       ''NON-RENEWED'',
                                                                                                                       ''CONFIRMED'',
                                                                                                                       ''SCHEDULED'')) THEN createtime
                                                                                  ELSE createtime
                                                                        END         policystatus_dttm ,
                                                                        cncl_source source_name,
                                                                        modelnumber,
                                                                        c.typecode_stg typecode,
                                                                        b.retired,
                                                                        a.creationts_stg creationts,
                                                                        policyperiodid
                                                              FROM      (
                                                                                  SELECT    gl_eventstaging_pc.batchid_stg,
                                                                                            gl_eventstaging_pc.rootentity_stg,
                                                                                            gl_eventstaging_pc.eventname_stg ,
                                                                                            gl_eventstaging_pc.creationts_stg,
                                                                                            gl_eventstaging_pc.creationuid_stg,
                                                                                            gl_eventstaging_pc.updatets_stg,
                                                                                            gl_eventstaging_pc.updateuid_stg,
                                                                                            gl_eventstaging_pc.policynumber_stg    policynumber,
                                                                                            NULL                                AS accountnumber_stg,
                                                                                            NULL                                AS termnumber,
                                                                                            gl_eventstaging_pc.publicid_stg     AS policyperiodid,
                                                                                            NULL                                AS policyeffectivedate,
                                                                                            NULL                                AS policyexpirationdate,
                                                                                            CASE
                                                                                                      WHEN out_edwpolicystatus_pc.policystatus_stg IS NULL THEN gl_eventstaging_pc.policystatus_stg
                                                                                                      ELSE out_edwpolicystatus_pc.policystatus_stg
                                                                                            END AS policystatus
                                                                                  FROM      db_t_prod_stag.gl_eventstaging_pc
                                                                                  left join
                                                                                            (
                                                                                                      SELECT    pc_policyperiod.policynumber_stg      AS policynumber,
                                                                                                                pc_account.accountnumber_stg          AS accountnumber,
                                                                                                                pc_policyperiod.termnumber_stg        AS termnumber,
                                                                                                                pc_policyperiod.publicid_stg          AS policyperiodid_stg,
                                                                                                                pc_policyperiod.editeffectivedate_stg AS policyeffectivedate,
                                                                                                                pc_policyperiod.periodend_stg         AS policyexpirationdate,
                                                                                                                CASE
                                                                                                                          WHEN pc_policyperiod.cancellationdate_stg IS NOT NULL
                                                                                                                          AND       cast(pc_policyperiod.cancellationdate_stg AS DATE) <= cast(cutoff_date_stg AS DATE) THEN ''CANCELED''
                                                                                                                          WHEN pc_policyperiod.cancellationdate_stg IS NOT NULL
                                                                                                                          AND       cast(pc_policyperiod.cancellationdate_stg AS  DATE) > cast(cutoff_date_stg AS DATE)
                                                                                                                          AND       cast(pc_policyperiod.editeffectivedate_stg AS DATE)<= cast(cutoff_date_stg AS DATE) THEN ''IN FORCE''
                                                                                                                          WHEN cast(cutoff_date_stg AS                            DATE) > cast(pc_policyperiod.periodstart_stg AS DATE)
                                                                                                                          AND       pc_policyterm.bound_stg = 0 THEN (
                                                                                                                                    CASE
                                                                                                                                              WHEN (
                                                                                                                                                                  pctl_papolicytype_alfa.id_stg IS NOT NULL
                                                                                                                                                        AND       pctl_job.typecode_stg = ''Renewal''
                                                                                                                                                        AND       pctl_policyperiodsrctype_alfa.typecode_stg=''AutoConverted''
                                                                                                                                                        AND       pcx_migrationpolinfo_ext.legacyduedate_alfa_stg IS NOT NULL
                                                                                                                                                        AND
                                                                                                                                                                  CASE
                                                                                                                                                                            WHEN pcx_migrationpolinfo_ext.legacyduedate_alfa_stg IS NOT NULL THEN ( cast(pcx_migrationpolinfo_ext.legacyduedate_alfa_stg AS DATE)-cast(cutoff_date_stg AS DATE))
                                                                                                                                                                  END <=30 )THEN ''IN FORCE''
                                                                                                                                              ELSE ''RENEWAL LAPSED''
                                                                                                                                    END)
                                                                                                                          WHEN cast(cutoff_date_stg AS DATE) <= cast(pc_policyperiod.periodstart_stg AS DATE)
                                                                                                                          AND       pc_policyterm.bound_stg = 0 THEN ''PENDING CONFIRMATION''
                                                                                                                          WHEN cast(cutoff_date_stg AS DATE) < cast(pc_policyperiod.periodstart_stg AS DATE)
                                                                                                                          AND       pc_policyterm.bound_stg = 1
                                                                                                                          AND       (
                                                                                                                                              pctl_job.typecode_stg = ''Renewal''
                                                                                                                                    OR        (
                                                                                                                                                        e.typecode_stg=''Renewal''
                                                                                                                                              AND       (
                                                                                                                                                                  cast(cutoff_date_stg AS DATE)< cast(c.periodstart_stg AS DATE) ) )) THEN ''CONFIRMED''
                                                                                                                          WHEN cast(cutoff_date_stg AS DATE) < cast(pc_policyperiod.periodstart_stg AS DATE)
                                                                                                                          AND       pc_policyterm.bound_stg = 1
                                                                                                                          AND       (
                                                                                                                                              (
                                                                                                                                                        pctl_job.typecode_stg<>''Renewal'')) THEN ''SCHEDULED''
                                                                                                                          WHEN cast(cutoff_date_stg AS      DATE)>= cast(pc_policyperiod.periodstart_stg AS DATE)
                                                                                                                          AND       cast(cutoff_date_stg AS DATE)>=cast(pc_policyperiod.periodend_stg AS DATE)
                                                                                                                          AND       pc_policyterm.bound_stg = 1 THEN ''EXPIRED''
                                                                                                                          ELSE ''IN FORCE''
                                                                                                                END AS policystatus_stg
                                                                                                      FROM      db_t_prod_stag.pc_policyperiod
                                                                                                      join      db_t_prod_stag.out_edw_cutoffdate
                                                                                                      ON        1=1
                                                                                                      join      db_t_prod_stag.pc_policy
                                                                                                      ON        pc_policyperiod.policyid_stg=pc_policy.id_stg
                                                                                                      join      db_t_prod_stag.pc_policyterm
                                                                                                      ON        pc_policyperiod.policytermid_stg=pc_policyterm.id_stg
                                                                                                      join      db_t_prod_stag.pc_account
                                                                                                      ON        pc_policy.accountid_stg=pc_account.id_stg
                                                                                                      join      db_t_prod_stag.pc_job
                                                                                                      ON        pc_job.id_stg=pc_policyperiod.jobid_stg
                                                                                                      join      db_t_prod_stag.pctl_job
                                                                                                      ON        pctl_job.id_stg=pc_job.subtype_stg
                                                                                                      left join db_t_prod_stag.pctl_policyperiodsrctype_alfa
                                                                                                      ON        pctl_policyperiodsrctype_alfa.id_stg=pc_policyperiod.policyperiodsource_stg
                                                                                                      left join db_t_prod_stag.pcx_migrationpolinfo_ext
                                                                                                      ON        pc_policy.id_stg = pcx_migrationpolinfo_ext.id_stg
                                                                                                      join      db_t_prod_stag.pctl_policyperiodstatus
                                                                                                      ON        pctl_policyperiodstatus.id_stg=pc_policyperiod.status_stg
                                                                                                      left join db_t_prod_stag.pc_policyperiod b
                                                                                                      ON        pc_policyperiod.basedonid_stg=b.id_stg
                                                                                                      join      db_t_prod_stag.pc_policyline
                                                                                                      ON        pc_policyline.branchid_stg=pc_policyperiod.id_stg
                                                                                                      left join db_t_prod_stag.pctl_papolicytype_alfa
                                                                                                      ON        pctl_papolicytype_alfa.id_stg=pc_policyline.papolicytype_alfa_stg
                                                                                                      left join db_t_prod_stag.pc_policyperiod c
                                                                                                      ON        c.periodid_stg=pc_policyperiod.periodid_stg
                                                                                                      AND       c.modelnumber_stg=1
                                                                                                      join      db_t_prod_stag.pc_job d
                                                                                                      ON        d.id_stg=c.jobid_stg
                                                                                                      join      db_t_prod_stag.pctl_job e
                                                                                                      ON        e.id_stg=d.subtype_stg
                                                                                                      WHERE     pc_policyperiod.mostrecentmodel_stg=1
                                                                                                      AND       pctl_policyperiodstatus.typecode_stg=''Bound''
                                                                                                      AND       (
                                                                                                                          pc_policyline.effectivedate_stg IS NULL
                                                                                                                OR        (
                                                                                                                                    pc_policyline.effectivedate_stg<= (cutoff_date_stg )))
                                                                                                      AND       (
                                                                                                                          pc_policyline.expirationdate_stg IS NULL
                                                                                                                OR        (
                                                                                                                                    pc_policyline.expirationdate_stg>= (cutoff_date_stg )))) out_edwpolicystatus_pc
                                                                                  ON        out_edwpolicystatus_pc.policyperiodid_stg=gl_eventstaging_pc.publicid_stg
                                                                                  WHERE     gl_eventstaging_pc.source_stg=''POL STATUS''
                                                                                            /* and GL_EventStaging_PC.CreationTS_stg > ($start_dttm ) and GL_EventStaging_PC.CreationTS_stg <=  ($end_dttm) */
                                                                        ) a
                                                              left join
                                                                        (
                                                                                        SELECT          pc_policyperiod.modelnumber_stg       AS modelnumber,
                                                                                                        pc_policyperiod.cancellationdate_stg  AS cancellationdate,
                                                                                                        pc_policyperiod.editeffectivedate_stg AS editeffectivedate,
                                                                                                        pc_policyperiod.periodend_stg         AS periodend,
                                                                                                        pc_policyperiod.updatetime_stg        AS updatetime,
                                                                                                        pc_policyperiod.createtime_stg        AS createtime,
                                                                                                        pctl_cancellationsource.typecode_stg  AS cncl_source,
                                                                                                        pc_job.cancelreasoncode_stg           AS cancelreasoncode,
                                                                                                        pc_policyperiod.retired_stg           AS retired,
                                                                                                        pc_policyperiod.publicid_stg          AS publicid
                                                                                        FROM            db_t_prod_stag.pc_policyperiod
                                                                                        left outer join db_t_prod_stag.pc_job
                                                                                        ON              pc_policyperiod.jobid_stg =pc_job.id_stg
                                                                                        left outer join db_t_prod_stag.pctl_cancellationsource
                                                                                        ON              pc_job.source_stg= pctl_cancellationsource.id_stg
                                                                                        left join       db_t_prod_stag.pctl_job
                                                                                        ON              pctl_job.id_stg = pc_job.subtype_stg ) b
                                                              ON        a.policyperiodid=b.publicid
                                                              left join db_t_prod_stag.pctl_reasoncode c
                                                              ON        c.id_stg= b.cancelreasoncode) out_edwpolicystatus_pc
                                           WHERE    policynumber IS NOT NULL qualify row_number() over(PARTITION BY policyperiodid ORDER BY creationts DESC) =1
                                           /* ORDER BY 1,5,4 */
                                           UNION
                                           SELECT     cast (NULL AS                           VARCHAR (60)) AS policynumber,
                                                      cast (cctl_policystatus.typecode_stg AS VARCHAR (60)) AS policystatus,
                                                      cc_policy.updatetime_stg,
                                                      cast (NULL AS             VARCHAR (60)) AS modelnumber,
                                                      cast (NULL AS             VARCHAR (60)) AS termnumber,
                                                      cast (''1900-01-01'' AS     DATE)         AS creationts,
                                                      cast (cc_policy.id_stg AS VARCHAR (60)) AS id,
                                                      /* cast (cctl_policystatus.typecode as varchar (60)) as typecode, */
                                                      ''SRC_SYS6'' AS src_cd,
                                                      ''''         AS source_name,
                                                      ''''         AS typecode ,
                                                      cc_policy.retired_stg
                                           FROM       (
                                                                      SELECT
                                                                                      /*cc_policy.LoadCommandID_stg, cc_policy.Participation_stg, cc_policy.Notes_stg, cc_policy.PolicySuffix, cc_policy.PublicID, cc_policy.WCOtherStates,*/
                                                                                      cc_policy.verified_stg,
                                                                                      cc_policy.updatetime_stg,
                                                                                      cc_policy.id_stg,
                                                                                      cctl_policysubtype_alfa.typecode_stg AS policysubtype_alfa,
                                                                                      CASE
                                                                                                      WHEN (
                                                                                                                                      coalesce(cc_policy.legacypolind_alfa_stg,0)=1) THEN ''Y''
                                                                                                      ELSE ''N''
                                                                                      END legacypolind_alfa,
                                                                                      cc_policy.retired_stg,
                                                                                      pc_policyperiod.publicid publicid_pc,
                                                                                      cc_policy.policynumber_stg ,
                                                                                      cc_policy.status_stg
                                                                      FROM            db_t_prod_stag.cc_policy
                                                                      left outer join db_t_prod_stag.cctl_policysubtype_alfa
                                                                      ON              cc_policy.policysubtype_alfa_stg=cctl_policysubtype_alfa.id_stg
                                                                      left join
                                                                                      (
                                                                                             SELECT cancellationdate AS cancellationdate ,
                                                                                                    periodstart      AS periodstart,
                                                                                                    publicid_stg     AS publicid,
                                                                                                    policynumber_stg AS policynumber
                                                                                             FROM   (
                                                                                                               SELECT     pol.policynumber_stg,
                                                                                                                          pol.cancellationdate_stg AS cancellationdate,
                                                                                                                          pol.periodstart_stg      AS periodstart,
                                                                                                                          pol.modelnumber_stg,
                                                                                                                          pol.termnumber_stg,
                                                                                                                          pol.publicid_stg ,
                                                                                                                          row_number() over(PARTITION BY pol.policynumber_stg,pol.termnumber_stg ORDER BY pol.policynumber_stg,pol.modelnumber_stg DESC) AS r
                                                                                                               FROM       db_t_prod_stag.pc_policyperiod pol
                                                                                                               inner join db_t_prod_stag.pctl_policyperiodstatus
                                                                                                               ON         pol.status_stg=pctl_policyperiodstatus.id_stg
                                                                                                               WHERE      pctl_policyperiodstatus.typecode_stg=''Bound'' )x
                                                                                             WHERE  r=1 ) pc_policyperiod
                                                                      ON              cc_policy.policynumber_stg =pc_policyperiod.policynumber
                                                                      WHERE           cc_policy.updatetime_stg > ($start_dttm )
                                                                      AND             cc_policy.updatetime_stg <= ($end_dttm)) cc_policy
                                           inner join db_t_prod_stag.cctl_policystatus
                                           ON         cctl_policystatus.id_stg=cc_policy.status_stg
                                           WHERE      cc_policy.verified_stg = 0 qualify row_number() over(PARTITION BY cc_policy.policynumber_stg ORDER BY updatetime_stg DESC) =1 ) src ) );
  -- Component exp_pass_from_source, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_from_source AS
  (
            SELECT    NULL                                                      AS rownumber,
                      sq_out_edwpolicystatus_pc.policynumber                    AS policynumber,
                      sq_out_edwpolicystatus_pc.policystatus                    AS agmt_status,
                      sq_out_edwpolicystatus_pc.policystatus_dttm               AS policystatus_dttm,
                      sq_out_edwpolicystatus_pc.creationts                      AS creationts,
                      sq_out_edwpolicystatus_pc.policyperiodid                  AS policyperiodid,
                      sq_out_edwpolicystatus_pc.src_cd                          AS src_cd,
                      ltrim ( rtrim ( sq_out_edwpolicystatus_pc.source_name ) ) AS var_source_name,
                      CASE
                                WHEN trim(var_source_name) = ''''
                                OR        var_source_name IS NULL
                                OR        length ( var_source_name ) = 0 THEN ''UNK''
                                ELSE lkp_1.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_SRC_TYPE */
                      END AS out_source_name,
                      CASE
                                WHEN lkp_2.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_RSN_CD */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_3.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_RSN_CD */
                      END                               AS out_reasoncode,
                      sq_out_edwpolicystatus_pc.retired AS retired,
                      sq_out_edwpolicystatus_pc.source_record_id,
                      row_number() over (PARTITION BY sq_out_edwpolicystatus_pc.source_record_id ORDER BY sq_out_edwpolicystatus_pc.source_record_id) AS rnk
            FROM      sq_out_edwpolicystatus_pc
            left join lkp_teradata_etl_ref_xlat_agmt_sts_src_type lkp_1
            ON        lkp_1.src_idntftn_val = sq_out_edwpolicystatus_pc.source_name
            left join lkp_teradata_etl_ref_xlat_agmt_sts_rsn_cd lkp_2
            ON        lkp_2.src_idntftn_val = sq_out_edwpolicystatus_pc.typecode
            left join lkp_teradata_etl_ref_xlat_agmt_sts_rsn_cd lkp_3
            ON        lkp_3.src_idntftn_val = sq_out_edwpolicystatus_pc.typecode qualify rnk = 1 );
  -- Component exp_data_transformation, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_data_transformation AS
  (
            SELECT
                      CASE
                                WHEN lkp_1.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_TYPE */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_2.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_AGMT_STS_TYPE */
                      END                                                                   AS out_agmt_status_type,
                      exp_pass_from_source.policystatus_dttm                                AS policystatus_dttm,
                      $p_agmt_type_cd_policy_version                                        AS out_agmt_type_cd,
                      to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_agmt_sts_end_dttm,
                      to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm,
                      $prcs_id                                                              AS out_prcs_id,
                      exp_pass_from_source.creationts                                       AS creationts,
                      exp_pass_from_source.policyperiodid                                   AS policyperiodid,
                      CASE
                                WHEN lkp_3.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */
                                          IS NULL THEN ''UNK''
                                ELSE lkp_4.tgt_idntftn_val
                                          /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SRC_CD */
                      END                                  AS out_agmt_src_cd,
                      exp_pass_from_source.out_source_name AS source_name,
                      exp_pass_from_source.out_reasoncode  AS out_reasoncode,
                      exp_pass_from_source.retired         AS retired,
                      exp_pass_from_source.source_record_id,
                      row_number() over (PARTITION BY exp_pass_from_source.source_record_id ORDER BY exp_pass_from_source.source_record_id) AS rnk
            FROM      exp_pass_from_source
            left join lkp_teradata_etl_ref_xlat_agmt_sts_type lkp_1
            ON        lkp_1.src_idntftn_val = exp_pass_from_source.agmt_status
            left join lkp_teradata_etl_ref_xlat_agmt_sts_type lkp_2
            ON        lkp_2.src_idntftn_val = exp_pass_from_source.agmt_status
            left join lkp_teradata_etl_ref_xlat_src_cd lkp_3
            ON        lkp_3.src_idntftn_val = exp_pass_from_source.src_cd
            left join lkp_teradata_etl_ref_xlat_src_cd lkp_4
            ON        lkp_4.src_idntftn_val = exp_pass_from_source.src_cd 
            qualify row_number() over (PARTITION BY exp_pass_from_source.source_record_id ORDER BY exp_pass_from_source.source_record_id) 
            = 1 );
  -- Component LKP_AGMT, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt AS
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
                               FROM     db_t_prod_core.agmt
                               WHERE    agmt.agmt_type_cd=''PPV'' qualify row_number() over(PARTITION BY agmt.nk_src_key,agmt.host_agmt_num ORDER BY agmt.edw_end_dttm DESC) = 1 ) lkp
            ON        lkp.nk_src_key = exp_data_transformation.policyperiodid
            AND       lkp.agmt_type_cd = exp_data_transformation.out_agmt_type_cd 
            qualify row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.agmt_id ASC,lkp.host_agmt_num ASC,lkp.agmt_name ASC,lkp.agmt_opn_dttm ASC,lkp.agmt_cls_dttm ASC,lkp.agmt_plnd_expn_dttm ASC,lkp.agmt_signd_dttm ASC,lkp.agmt_type_cd ASC,lkp.agmt_legly_bindg_ind ASC,lkp.agmt_src_cd ASC,lkp.agmt_cur_sts_cd ASC,lkp.agmt_cur_sts_rsn_cd ASC,lkp.agmt_obtnd_cd ASC,lkp.agmt_sbtype_cd ASC,lkp.agmt_prcsg_dttm ASC,lkp.alt_agmt_name ASC,lkp.asset_liabty_cd ASC,lkp.bal_shet_cd ASC,lkp.stmt_cycl_cd ASC,lkp.stmt_ml_type_cd ASC,lkp.prposl_id ASC,lkp.agmt_objtv_type_cd ASC,lkp.fincl_agmt_sbtype_cd ASC,lkp.mkt_risk_type_cd ASC,lkp.orignl_maturty_dt ASC,lkp.risk_expsr_mtgnt_sbtype_cd ASC,lkp.bnk_trd_bk_cd ASC,lkp.prcg_meth_sbtype_cd ASC,lkp.fincl_agmt_type_cd ASC,lkp.dy_cnt_bss_cd ASC,lkp.frst_prem_due_dt ASC,lkp.insrnc_agmt_sbtype_cd ASC,lkp.insrnc_agmt_type_cd ASC,lkp.ntwk_srvc_agmt_type_cd ASC,lkp.frmlty_type_cd ASC,lkp.cntrct_term_num ASC,lkp.rate_rprcg_cycl_mth_num ASC,lkp.cmpnd_int_cycl_mth_num ASC,lkp.mdterm_int_pmt_cycl_mth_num ASC,lkp.prev_mdterm_int_pmt_dt ASC,lkp.nxt_mdterm_int_pmt_dt ASC,lkp.prev_int_rate_rvsd_dt ASC,lkp.nxt_int_rate_rvsd_dt ASC,lkp.prev_ref_dt_int_rate ASC,lkp.nxt_ref_dt_for_int_rate ASC,lkp.mdterm_cncltn_dt ASC,lkp.stk_flow_clas_in_mth_ind ASC,lkp.stk_flow_clas_in_term_ind ASC,lkp.lgcy_dscnt_ind ASC,lkp.agmt_idntftn_cd ASC,lkp.trmtn_type_cd ASC,lkp.int_pmt_meth_cd ASC,lkp.lbr_agmt_desc ASC,lkp.guartd_imprsns_cnt ASC,lkp.cost_per_imprsn_amt ASC,lkp.guartd_clkthru_cnt ASC,lkp.cost_per_clkthru_amt ASC,lkp.busn_prty_id ASC,lkp.pmt_pln_type_cd ASC,lkp.invc_strem_type_cd ASC,lkp.modl_crtn_dttm ASC,lkp.cntnus_srvc_dttm ASC,lkp.bilg_meth_type_cd ASC,lkp.src_sys_cd ASC,lkp.agmt_eff_dttm ASC,lkp.modl_eff_dttm ASC,lkp.prcs_id ASC,lkp.modl_actl_end_dttm ASC,lkp.tier_type_cd ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC,lkp.vfyd_plcy_ind ASC,lkp.src_of_busn_cd ASC,lkp.nk_src_key ASC,lkp.ovrd_coms_type_cd ASC,lkp.lgcy_plcy_ind ASC,lkp.trans_strt_dttm ASC )
                        = 1 );
  -- Component LKP_AGMT_STS, Type LOOKUP
  CREATE
  OR
  replace TEMPORARY TABLE lkp_agmt_sts AS
  (
             SELECT     lkp.agmt_id,
                        lkp.agmt_sts_cd,
                        lkp.agmt_sts_rsn_cd,
                        lkp.agmt_sts_strt_dttm,
                        lkp.agmt_sts_end_dttm,
                        lkp.edw_strt_dttm,
                        lkp.edw_end_dttm,
                        exp_data_transformation.source_record_id,
                        row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.agmt_id ASC,lkp.agmt_sts_cd ASC,lkp.agmt_sts_rsn_cd ASC,lkp.agmt_sts_strt_dttm ASC,lkp.agmt_sts_end_dttm ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) rnk
             FROM       exp_data_transformation
             inner join lkp_agmt
             ON         exp_data_transformation.source_record_id = lkp_agmt.source_record_id
             left join
                        (
                                 SELECT   agmt_sts.agmt_sts_rsn_cd    AS agmt_sts_rsn_cd,
                                          agmt_sts.agmt_sts_strt_dttm AS agmt_sts_strt_dttm,
                                          agmt_sts.agmt_sts_end_dttm  AS agmt_sts_end_dttm,
                                          agmt_sts.edw_strt_dttm      AS edw_strt_dttm,
                                          agmt_sts.edw_end_dttm       AS edw_end_dttm,
                                          agmt_sts.agmt_id            AS agmt_id,
                                          agmt_sts.agmt_sts_cd        AS agmt_sts_cd
                                 FROM     db_t_prod_core.agmt_sts qualify row_number() over(PARTITION BY agmt_id ORDER BY edw_end_dttm DESC) = 1 ) lkp
             ON         lkp.agmt_id = lkp_agmt.agmt_id
             AND        lkp.agmt_sts_cd = exp_data_transformation.out_agmt_status_type 
             qualify row_number() over(PARTITION BY exp_data_transformation.source_record_id ORDER BY lkp.agmt_id ASC,lkp.agmt_sts_cd ASC,lkp.agmt_sts_rsn_cd ASC,lkp.agmt_sts_strt_dttm ASC,lkp.agmt_sts_end_dttm ASC,lkp.edw_strt_dttm ASC,lkp.edw_end_dttm ASC) 
             = 1 );
  -- Component exp_insert_update, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_insert_update AS
  (
             SELECT     lkp_agmt_sts.agmt_id                                                     AS lkp_agmt_id,
                        lkp_agmt_sts.agmt_sts_cd                                                 AS lkp_agmt_sts,
                        lkp_agmt_sts.agmt_sts_rsn_cd                                             AS lkp_agmt_sts_rsn_cd,
                        lkp_agmt_sts.agmt_sts_strt_dttm                                          AS lkp_agmt_sts_strt_dttm,
                        lkp_agmt_sts.edw_strt_dttm                                               AS lkp_edw_strt_dttm,
                        lkp_agmt_sts.edw_end_dttm                                                AS lkp_edw_end_dttm,
                        md5 ( ltrim ( rtrim ( lkp_agmt_sts.agmt_sts_cd ) ) )                     AS orig_chksm,
                        lkp_agmt.agmt_id                                                         AS agmt_id,
                        exp_data_transformation.out_agmt_status_type                             AS src_agmt_sts_cd,
                        exp_data_transformation.policystatus_dttm                                AS policystatus_dttm,
                        exp_data_transformation.out_agmt_sts_end_dttm                            AS agmt_sts_end_dttm,
                        exp_data_transformation.creationts                                       AS creationts,
                        md5 ( ltrim ( rtrim ( exp_data_transformation.out_agmt_status_type ) ) ) AS calc_chksm,
                        exp_data_transformation.out_prcs_id                                      AS prcs_id,
                        exp_data_transformation.out_reasoncode                                   AS agmt_sts_rsn_cd,
                        exp_data_transformation.out_edw_end_dttm                                 AS out_edw_end_dttm,
                        exp_data_transformation.source_name                                      AS source_name,
                        CASE
                                   WHEN orig_chksm IS NULL THEN ''I''
                                   ELSE
                                              CASE
                                                         WHEN orig_chksm != calc_chksm THEN ''U''
                                                         ELSE ''R''
                                              END
                        END                             AS out_ins_upd,
                        current_timestamp               AS out_edw_strt_dttm,
                        exp_data_transformation.retired AS retired,
                        exp_data_transformation.source_record_id
             FROM       exp_data_transformation
             inner join lkp_agmt
             ON         exp_data_transformation.source_record_id = lkp_agmt.source_record_id
             inner join lkp_agmt_sts
             ON         lkp_agmt.source_record_id = lkp_agmt_sts.source_record_id );
  -- Component exp_check, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_check AS
  (
         SELECT exp_insert_update.agmt_id                AS agmt_id,
                exp_insert_update.src_agmt_sts_cd        AS agmt_sts_cd,
                exp_insert_update.policystatus_dttm      AS policystatus_dttm,
                exp_insert_update.agmt_sts_end_dttm      AS agmt_sts_end_dttm,
                exp_insert_update.lkp_agmt_id            AS lkp_agmt_id,
                exp_insert_update.lkp_agmt_sts           AS lkp_agmt_sts,
                exp_insert_update.lkp_agmt_sts_strt_dttm AS lkp_agmt_sts_strt_dttm,
                exp_insert_update.prcs_id                AS prcs_id,
                exp_insert_update.agmt_sts_rsn_cd        AS agmt_sts_rsn_cd,
                exp_insert_update.out_edw_strt_dttm      AS out_edw_strt_dttm,
                exp_insert_update.out_edw_end_dttm       AS out_edw_end_dttm,
                exp_insert_update.out_ins_upd            AS out_ins_upd,
                exp_insert_update.lkp_edw_strt_dttm      AS lkp_edw_strt_dttm,
                exp_insert_update.lkp_edw_end_dttm       AS lkp_edw_end_dttm,
                exp_insert_update.retired                AS retired,
                exp_insert_update.source_record_id
         FROM   exp_insert_update );
  -- Component rtr_AGMT_STS_INSERT, Type ROUTER Output Group INSERT
  create or replace TEMPORARY TABLE rtr_agmt_sts_insert AS
    SELECT    exp_check.agmt_id                AS agmt_id,
            exp_check.agmt_sts_cd            AS agmt_sts_cd,
            exp_check.policystatus_dttm      AS creationts,
            exp_check.agmt_sts_rsn_cd        AS agmt_sts_rsn_cd,
            exp_check.prcs_id                AS prcs_id,
            exp_check.out_edw_strt_dttm      AS out_edw_strt_dttm,
            exp_check.out_edw_end_dttm       AS out_edw_end_dttm,
            exp_check.out_ins_upd            AS out_ins_upd,
            exp_insert_update.source_name    AS source_name,
            exp_check.retired                AS retired,
            exp_check.lkp_agmt_id            AS lkp_agmt_id,
            exp_check.agmt_sts_end_dttm      AS agmt_sts_end_dttm4,
            exp_check.lkp_agmt_sts           AS lkp_agmt_sts,
            exp_check.lkp_agmt_sts_strt_dttm AS lkp_agmt_sts_strt_dttm,
            NULL                             AS agmt_id_static,
            exp_check.lkp_edw_strt_dttm      AS lkp_edw_strt_dttm,
            exp_check.lkp_edw_end_dttm       AS lkp_edw_end_dttm,
            exp_insert_update.source_record_id
  FROM      exp_insert_update
  left join exp_check
  ON        exp_insert_update.source_record_id = exp_check.source_record_id
  WHERE     exp_check.out_ins_upd = ''I''
  AND       exp_check.agmt_id IS NOT NULL
  OR        (
                      exp_check.lkp_edw_end_dttm != to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
            AND       exp_check.retired = 0 )
  OR        (
                      exp_check.out_ins_upd = ''U''
            AND       exp_check.lkp_edw_end_dttm = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  
  -- Component rtr_AGMT_STS_RETIRED, Type ROUTER Output Group RETIRED
  create or replace TEMPORARY TABLE rtr_agmt_sts_retired AS
  SELECT    exp_check.agmt_id                AS agmt_id,
            exp_check.agmt_sts_cd            AS agmt_sts_cd,
            exp_check.policystatus_dttm      AS creationts,
            exp_check.agmt_sts_rsn_cd        AS agmt_sts_rsn_cd,
            exp_check.prcs_id                AS prcs_id,
            exp_check.out_edw_strt_dttm      AS out_edw_strt_dttm,
            exp_check.out_edw_end_dttm       AS out_edw_end_dttm,
            exp_check.out_ins_upd            AS out_ins_upd,
            exp_insert_update.source_name    AS source_name,
            exp_check.retired                AS retired,
            exp_check.lkp_agmt_id            AS lkp_agmt_id,
            exp_check.agmt_sts_end_dttm      AS agmt_sts_end_dttm4,
            exp_check.lkp_agmt_sts           AS lkp_agmt_sts,
            exp_check.lkp_agmt_sts_strt_dttm AS lkp_agmt_sts_strt_dttm,
            NULL                             AS agmt_id_static,
            exp_check.lkp_edw_strt_dttm      AS lkp_edw_strt_dttm,
            exp_check.lkp_edw_end_dttm       AS lkp_edw_end_dttm,
            exp_insert_update.source_record_id
  FROM      exp_insert_update
  left join exp_check
  ON        exp_insert_update.source_record_id = exp_check.source_record_id
  WHERE     exp_check.out_ins_upd = ''R''
  AND       exp_check.retired != 0
  AND       exp_check.lkp_edw_end_dttm = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_AGMT_STS_upd_upd1, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_agmt_sts_upd_upd1 AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_agmt_sts_retired.lkp_agmt_id            AS lkp_agmt_id,
                rtr_agmt_sts_retired.lkp_agmt_sts           AS lkp_agmt_sts,
                rtr_agmt_sts_retired.lkp_agmt_sts_strt_dttm AS lkp_agmt_sts_strt_dttm,
                rtr_agmt_sts_retired.out_edw_strt_dttm      AS out_edw_strt_dttm4,
                rtr_agmt_sts_retired.lkp_edw_strt_dttm      AS lkp_edw_strt_dttm3,
                1                                           AS update_strategy_action,
                rtr_agmt_sts_retired.source_record_id
        FROM   rtr_agmt_sts_retired );
  -- Component upd_AGMT_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_agmt_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_agmt_sts_insert.agmt_id            AS agmt_id,
                rtr_agmt_sts_insert.agmt_sts_cd        AS agmt_sts_cd,
                rtr_agmt_sts_insert.creationts         AS creationts,
                rtr_agmt_sts_insert.prcs_id            AS prcs_id,
                rtr_agmt_sts_insert.out_edw_strt_dttm  AS out_edw_strt_dttm,
                rtr_agmt_sts_insert.out_edw_end_dttm   AS out_edw_end_dttm1,
                rtr_agmt_sts_insert.agmt_sts_end_dttm4 AS agmt_sts_end_dttm41,
                rtr_agmt_sts_insert.source_name        AS source_name1,
                rtr_agmt_sts_insert.agmt_sts_rsn_cd    AS agmt_sts_rsn_cd1,
                rtr_agmt_sts_insert.retired            AS retired1,
                0                                      AS update_strategy_action,
                rtr_agmt_sts_insert.source_record_id
         FROM   rtr_agmt_sts_insert );
  -- Component exp_pass_to_target_retired, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_retired AS
  (
         SELECT upd_agmt_sts_upd_upd1.lkp_agmt_id        AS lkp_agmt_id,
                upd_agmt_sts_upd_upd1.out_edw_strt_dttm4 AS out_edw_strt_dttm,
                upd_agmt_sts_upd_upd1.lkp_edw_strt_dttm3 AS lkp_edw_strt_dttm3,
                upd_agmt_sts_upd_upd1.source_record_id
         FROM   upd_agmt_sts_upd_upd1 );
  -- Component exp_pass_tgt, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_tgt AS
  (
         SELECT upd_agmt_ins.agmt_id             AS agmt_id,
                upd_agmt_ins.agmt_sts_cd         AS agmt_sts_cd,
                upd_agmt_ins.creationts          AS creationts,
                upd_agmt_ins.prcs_id             AS prcs_id,
                upd_agmt_ins.out_edw_strt_dttm   AS out_edw_strt_dttm,
                upd_agmt_ins.agmt_sts_end_dttm41 AS agmt_sts_end_dttm41,
                upd_agmt_ins.source_name1        AS source_name1,
                upd_agmt_ins.agmt_sts_rsn_cd1    AS agmt_sts_rsn_cd1,
                CASE
                       WHEN upd_agmt_ins.retired1 = 0 THEN upd_agmt_ins.out_edw_end_dttm1
                       ELSE current_timestamp
                END AS out_edw_end_dttm11,
                upd_agmt_ins.source_record_id
         FROM   upd_agmt_ins );
  -- Component tgt_AGMT_STS_retired, Type TARGET
  merge
  INTO         db_t_prod_core.agmt_sts
  USING        exp_pass_to_target_retired
  ON (
                            agmt_sts.agmt_id = exp_pass_to_target_retired.lkp_agmt_id
               AND          agmt_sts.edw_strt_dttm = exp_pass_to_target_retired.lkp_edw_strt_dttm3)
  WHEN matched THEN
  UPDATE
  SET    agmt_id = exp_pass_to_target_retired.lkp_agmt_id,
         edw_strt_dttm = exp_pass_to_target_retired.lkp_edw_strt_dttm3,
         edw_end_dttm = exp_pass_to_target_retired.out_edw_strt_dttm;
  
  -- Component tgt_AGMT_STS_retired, Type Post SQL
  UPDATE db_t_prod_core.agmt_sts
  SET    edw_end_dttm=a.lead
  FROM   (
                         SELECT DISTINCT agmt_id,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY agmt_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead
                         FROM            db_t_prod_core.agmt_sts
                         WHERE           agmt_id IN
                                         (
                                                  SELECT   agmt_id
                                                  FROM     db_t_prod_core.agmt
                                                  WHERE    agmt_type_cd = ''ppv''
                                                  GROUP BY agmt_id) ) a
   WHERE  agmt_sts.edw_strt_dttm = a.edw_strt_dttm
  AND    agmt_sts.agmt_id=a.agmt_id
  AND    cast(agmt_sts.edw_end_dttm AS DATE)=''9999-12-31''
  AND    lead IS NOT NULL;
  
  -- Component tgt_AGMT_STS_ins_new, Type TARGET
  INSERT INTO db_t_prod_core.agmt_sts
              (
                          agmt_id,
                          agmt_sts_cd,
                          agmt_sts_strt_dttm,
                          agmt_sts_rsn_cd,
                          agmt_sts_end_dttm,
                          agmt_sts_src_type_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm
              )
  SELECT exp_pass_tgt.agmt_id             AS agmt_id,
         exp_pass_tgt.agmt_sts_cd         AS agmt_sts_cd,
         exp_pass_tgt.creationts          AS agmt_sts_strt_dttm,
         exp_pass_tgt.agmt_sts_rsn_cd1    AS agmt_sts_rsn_cd,
         exp_pass_tgt.agmt_sts_end_dttm41 AS agmt_sts_end_dttm,
         exp_pass_tgt.source_name1        AS agmt_sts_src_type_cd,
         exp_pass_tgt.prcs_id             AS prcs_id,
         exp_pass_tgt.out_edw_strt_dttm   AS edw_strt_dttm,
         exp_pass_tgt.out_edw_end_dttm11  AS edw_end_dttm
  FROM   exp_pass_tgt;

END;
';