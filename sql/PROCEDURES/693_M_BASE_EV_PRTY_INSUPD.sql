-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_EV_PRTY_INSUPD("WORKLET_NAME" VARCHAR)
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


  -- Component sq_bc_basemoneyreceived, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_bc_basemoneyreceived AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS lkp_ev_id,
                $2  AS lkp_ev_prty_role_cd,
                $3  AS lkp_prty_id,
                $4  AS lkp_ev_prty_strt_dttm,
                $5  AS lkp_edw_strt_dttm,
                $6  AS lkp_edw_end_dttm,
                $7  AS src_ev_id,
                $8  AS src_ev_prty_role_cd,
                $9  AS src_prty_id,
                $10 AS src_ev_prty_strt_dttm,
                $11 AS src_ev_prty_end_dttm,
                $12 AS src_prty_idntftn_type_cd,
                $13 AS trans_strt_dttm,
                $14 AS rnk,
                $15 AS retired,
                $16 AS sourcedata,
                $17 AS targetdata,
                $18 AS ins_upd_flag,
                $19 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     ( WITH ev_prty_intrm AS
                                  (
                                                  SELECT DISTINCT ev_act_type_code AS ev_act_type_cd,
                                                                  key1             AS ev_key,
                                                                  SUBTYPE          AS ev_subtype,
                                                                  nk_prty,
                                                                  role_cd                                            AS ev_prty_role_cd,
                                                                  strt_tm                                            AS ev_prty_strt_dttm,
                                                                  cast(''1900-01-01 00:00:00.000000'' AS timestamp(6)) AS ev_prty_end_dttm,
                                                                  tns_strt                                           AS trans_strt_dttm,
                                                                  retired,
                                                                  cast(NULL AS VARCHAR(50))                                                                                             AS party_type,
                                                                  rank() over(PARTITION BY key1,role_cd,ev_act_type_code,SUBTYPE ORDER BY tns_strt,nk_prty,strt_tm,retired,party_type ) AS rnk
                                                  FROM            (
                                                                                  /* original bc DB_T_PROD_STAG.ev_prty_x + filters */
                                                                                  /* *** UNION QUERY: 01 *** */
                                                                                  SELECT DISTINCT cast(''EV_ACTVY_TYPE14'' AS VARCHAR(60)) AS ev_act_type_code,
                                                                                                  cast(bbr.id_stg AS        VARCHAR(60)) AS key1,
                                                                                                  cast(''EV_SBTYPE2'' AS      VARCHAR(50)) AS SUBTYPE,
                                                                                                  bc.publicid_stg                        AS nk_prty,
                                                                                                  cast(''EV_PRTY_ROLE1'' AS VARCHAR(50))   AS role_cd,
                                                                                                  bbr.createtime_stg                     AS strt_tm,
                                                                                                  CASE
                                                                                                                  WHEN bbr.updatetime_stg>bu.updatetime_stg THEN bbr.updatetime_stg
                                                                                                                  ELSE bu.updatetime_stg
                                                                                                  END                             AS tns_strt,
                                                                                                  cast(''SRC_SYS5'' AS VARCHAR(50)) AS src_cd,
                                                                                                  cast(NULL AS       VARCHAR(50)) AS party_type,
                                                                                                  CASE
                                                                                                                  WHEN bbr.retired_stg=0
                                                                                                                  AND             bc.retired_stg=0
                                                                                                                  AND             bu.retired_stg=0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END AS retired
                                                                                  FROM            db_t_prod_stag.bc_basemoneyreceived bbr
                                                                                  inner join      db_t_prod_stag.bctl_basemoneyreceived bcbbr
                                                                                  ON              bcbbr.id_stg=bbr.subtype_stg
                                                                                  left outer join db_t_prod_stag.bc_user bu
                                                                                  ON              bu.id_stg=bbr.createuserid_stg
                                                                                  left outer join db_t_prod_stag.bc_contact bc
                                                                                  ON              bc.id_stg=bu.contactid_stg
                                                                                  WHERE           bcbbr.typecode_stg IN (''PaymentMoneyReceived'',
                                                                                                                         ''DirectBillMoneyRcvd'' ,
                                                                                                                         ''ZeroDollarDMR'',
                                                                                                                         ''ZeroDollarReversal'')
                                                                                                  /* PI */
                                                                                  AND             ((
                                                                                                                                  bbr.updatetime_stg>:START_DTTM
                                                                                                                  AND             bbr.updatetime_stg<=:END_DTTM)
                                                                                                  OR             (
                                                                                                                                  bu.updatetime_stg>:START_DTTM
                                                                                                                  AND             bu.updatetime_stg<=:END_DTTM)
                                                                                                  OR             (
                                                                                                                                  bc.updatetime_stg>:START_DTTM
                                                                                                                  AND             bc.updatetime_stg<=:END_DTTM))
                                                                                  UNION ALL
                                                                                  /* *** UNION QUERY: 02 *** */
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE30''               AS ev_act_type_code,
                                                                                                  cast(bsp.id_stg AS VARCHAR(60)) AS key1,
                                                                                                  ''EV_SBTYPE2''                    AS SUBTYPE,
                                                                                                  bc.publicid_stg                 AS nk_prty,
                                                                                                  ''EV_PRTY_ROLE1''                 AS role_cd,
                                                                                                  bsp.createtime_stg              AS strt_tm,
                                                                                                  CASE
                                                                                                                  WHEN bsp.updatetime_stg>bu.updatetime_stg THEN bsp.updatetime_stg
                                                                                                                  ELSE bu.updatetime_stg
                                                                                                  END                       AS tns_strt,
                                                                                                  ''SRC_SYS5''                AS src_cd,
                                                                                                  cast(NULL AS VARCHAR(50)) AS party_type,
                                                                                                  CASE
                                                                                                                  WHEN bsp.retired_stg=0
                                                                                                                  AND             bc.retired_stg=0
                                                                                                                  AND             bu.retired_stg=0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END AS retired
                                                                                  FROM            db_t_prod_stag.bc_suspensepayment bsp
                                                                                  left outer join db_t_prod_stag.bc_user bu
                                                                                  ON              bu.id_stg=bsp.createuserid_stg
                                                                                  left outer join db_t_prod_stag.bc_contact bc
                                                                                  ON              bc.id_stg=bu.contactid_stg
                                                                                  left join       db_t_prod_stag.bc_credential bcc
                                                                                  ON              bu.credentialid_stg=bcc.id_stg
                                                                                                  /* PI */
                                                                                  WHERE           ((
                                                                                                                                  bsp.updatetime_stg>:START_DTTM
                                                                                                                  AND             bsp.updatetime_stg<=:END_DTTM)
                                                                                                  OR              (
                                                                                                                                  bu.updatetime_stg>:START_DTTM
                                                                                                                  AND             bu.updatetime_stg<=:END_DTTM)
                                                                                                  OR             (
                                                                                                                                  bc.updatetime_stg>:START_DTTM
                                                                                                                  AND             bc.updatetime_stg<=:END_DTTM))
                                                                                  UNION ALL
                                                                                  /* *** UNION QUERY: 03 *** */
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE32''              AS ev_act_type_code,
                                                                                                  cast(bd.id_stg AS VARCHAR(60)) AS key1,
                                                                                                  ''EV_SBTYPE2''                   AS SUBTYPE,
                                                                                                  bc.publicid_stg                AS nk_prty,
                                                                                                  ''EV_PRTY_ROLE1''                AS role_cd,
                                                                                                  bd.updatetime_stg,
                                                                                                  CASE
                                                                                                                  WHEN bd.updatetime_stg>bu.updatetime_stg THEN bd.updatetime_stg
                                                                                                                  ELSE bu.updatetime_stg
                                                                                                  END                       AS tns_strt,
                                                                                                  ''SRC_SYS5''                AS src_cd,
                                                                                                  cast(NULL AS VARCHAR(50)) AS party_type,
                                                                                                  CASE
                                                                                                                  WHEN bd.retired_stg=0
                                                                                                                  AND             bc.retired_stg=0
                                                                                                                  AND             bu.retired_stg=0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END AS retired
                                                                                  FROM            db_t_prod_stag.bc_disbursement bd
                                                                                  left outer join db_t_prod_stag.bc_user bu
                                                                                  ON              bu.id_stg=bd.createuserid_stg
                                                                                  left outer join db_t_prod_stag.bc_contact bc
                                                                                  ON              bc.id_stg=bu.contactid_stg
                                                                                                  /* PI */
                                                                                  WHERE           ((
                                                                                                                                  bd.updatetime_stg>:START_DTTM
                                                                                                                  AND             bd.updatetime_stg<=:END_DTTM)
                                                                                                  OR              (
                                                                                                                                  bu.updatetime_stg>:START_DTTM
                                                                                                                  AND             bu.updatetime_stg<=:END_DTTM)
                                                                                                  OR             (
                                                                                                                                  bc.updatetime_stg>:START_DTTM
                                                                                                                  AND             bc.updatetime_stg<=:END_DTTM))
                                                                                  UNION ALL
                                                                                  /* *** UNION QUERY: 04 *** */
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE34''              AS ev_act_type_code,
                                                                                                  cast(bw.id_stg AS VARCHAR(60)) AS key1,
                                                                                                  ''EV_SBTYPE2''                   AS SUBTYPE,
                                                                                                  bc.publicid_stg                AS nk_prty,
                                                                                                  ''EV_PRTY_ROLE1''                AS role_cd,
                                                                                                  bw.updatetime_stg,
                                                                                                  CASE
                                                                                                                  WHEN bw.updatetime_stg>bu.updatetime_stg THEN bw.updatetime_stg
                                                                                                                  ELSE bu.updatetime_stg
                                                                                                  END AS tns_strt,
                                                                                                  /*  bc_writeoff.updatetime AS tns_strt, */
                                                                                                  ''SRC_SYS5''                AS src_cd,
                                                                                                  cast(NULL AS VARCHAR(50)) AS party_type,
                                                                                                  CASE
                                                                                                                  WHEN bw.retired_stg=0
                                                                                                                  AND             bc.retired_stg=0
                                                                                                                  AND             bu.retired_stg=0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END AS retired
                                                                                  FROM            (
                                                                                                         SELECT id_stg,
                                                                                                                updatetime_stg,
                                                                                                                retired_stg,
                                                                                                                updateuserid_stg
                                                                                                         FROM   db_t_prod_stag.bc_writeoff
                                                                                                         WHERE  id_stg IN
                                                                                                                (
                                                                                                                       SELECT ownerid_stg
                                                                                                                       FROM   db_t_prod_stag.bc_revwriteoff)) bw
                                                                                  left outer join db_t_prod_stag.bc_user bu
                                                                                  ON              bu.id_stg=bw.updateuserid_stg
                                                                                  left outer join db_t_prod_stag.bc_contact bc
                                                                                  ON              bc.id_stg=bu.contactid_stg
                                                                                  WHERE           bw.updatetime_stg>(:START_DTTM)
                                                                                  AND             bw.updatetime_stg<=(:END_DTTM)
                                                                                  /* where reversed=1 */
                                                                                  UNION ALL
                                                                                  /* *** UNION QUERY: 05 *** */
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE33''              AS ev_act_type_code,
                                                                                                  cast(bw.id_stg AS VARCHAR(60)) AS key1,
                                                                                                  ''EV_SBTYPE2''                   AS SUBTYPE,
                                                                                                  bc.publicid_stg                AS nk_prty,
                                                                                                  ''EV_PRTY_ROLE1''                AS role_cd,
                                                                                                  bw.updatetime_stg,
                                                                                                  CASE
                                                                                                                  WHEN bw.updatetime_stg>bu.updatetime_stg THEN bw.updatetime_stg
                                                                                                                  ELSE bu.updatetime_stg
                                                                                                  END                       AS tns_strt,
                                                                                                  ''SRC_SYS5''                AS src_cd,
                                                                                                  cast(NULL AS VARCHAR(50)) AS party_type,
                                                                                                  CASE
                                                                                                                  WHEN bw.retired_stg=0
                                                                                                                  AND             bc.retired_stg=0
                                                                                                                  AND             bu.retired_stg=0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END AS retired
                                                                                  FROM            (
                                                                                                         SELECT id_stg,
                                                                                                                updatetime_stg,
                                                                                                                retired_stg,
                                                                                                                updateuserid_stg
                                                                                                         FROM   db_t_prod_stag.bc_writeoff
                                                                                                         WHERE  id_stg NOT IN
                                                                                                                (
                                                                                                                       SELECT ownerid_stg
                                                                                                                       FROM   db_t_prod_stag.bc_revwriteoff)) bw
                                                                                  inner join      db_t_prod_stag.bc_user bu
                                                                                  ON              bu.id_stg=bw.updateuserid_stg
                                                                                  inner join      db_t_prod_stag.bc_contact bc
                                                                                  ON              bc.id_stg=bu.contactid_stg
                                                                                  WHERE           bw.updatetime_stg>(:START_DTTM)
                                                                                  AND             bw.updatetime_stg<=(:END_DTTM)
                                                                                  /* where Reversed=0 */
                                                                                  UNION ALL
                                                                                  /******************Billing Transaction *****************/
                                                                                  /* *** UNION QUERY: 06 *** */
                                                                                  SELECT DISTINCT bct.typecode_stg                  AS ev_act_type_code,
                                                                                                  cast (bt1.id_stg AS VARCHAR (50)) AS key1,
                                                                                                  ''EV_SBTYPE2''                      AS SUBTYPE,
                                                                                                  bc.publicid_stg                   AS nk_prty,
                                                                                                  ''EV_PRTY_ROLE1''                   AS role_cd,
                                                                                                  bt1.createtime_stg,
                                                                                                  CASE
                                                                                                                  WHEN bt1.updatetime_stg>bu.updatetime_stg THEN bt1.updatetime_stg
                                                                                                                  ELSE bu.updatetime_stg
                                                                                                  END AS tns_strt,
                                                                                                  /*  bc_transaction.updatetime AS tns_strt, */
                                                                                                  ''SRC_SYS5''                AS src_cd ,
                                                                                                  cast(NULL AS VARCHAR(50)) AS party_type,
                                                                                                  CASE
                                                                                                                  WHEN bt1.retired_stg=0
                                                                                                                  AND             bc.retired_stg=0
                                                                                                                  AND             bu.retired_stg=0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END AS retired
                                                                                  FROM            db_t_prod_stag.bc_transaction bt1
                                                                                  inner join      db_t_prod_stag.bctl_transaction bct
                                                                                  ON              bct.id_stg=bt1.subtype_stg
                                                                                  inner join      db_t_prod_stag.bc_user bu
                                                                                  ON              bu.id_stg=bt1.createuserid_stg
                                                                                  inner join      db_t_prod_stag.bc_contact bc
                                                                                  ON              bc.id_stg=bu.contactid_stg
                                                                                                  /* PI */
                                                                                  WHERE           ((
                                                                                                                                  bt1.updatetime_stg>:START_DTTM
                                                                                                                  AND             bt1.updatetime_stg<=:END_DTTM)
                                                                                                  OR              (
                                                                                                                                  bu.updatetime_stg>:START_DTTM
                                                                                                                  AND             bu.updatetime_stg<=:END_DTTM)
                                                                                                  OR             (
                                                                                                                                  bc.updatetime_stg>:START_DTTM
                                                                                                                  AND             bc.updatetime_stg<=:END_DTTM))
                                                                                  UNION ALL
                                                                                  /* *** UNION QUERY: 07 *** */
                                                                                  SELECT DISTINCT ''rvrs''
                                                                                                                  ||''-''
                                                                                                                  ||bct.typecode_stg AS ev_act_type_code,
                                                                                                  cast (bt1.id_stg AS VARCHAR (50))  AS key1,
                                                                                                  ''EV_SBTYPE2''                       AS SUBTYPE,
                                                                                                  bc.publicid_stg                    AS nk_prty,
                                                                                                  ''EV_PRTY_ROLE1''                    AS role_cd,
                                                                                                  bt1.createtime_stg,
                                                                                                  CASE
                                                                                                                  WHEN bt1.updatetime_stg>bu.updatetime_stg THEN bt1.updatetime_stg
                                                                                                                  ELSE bu.updatetime_stg
                                                                                                  END                       AS tns_strt,
                                                                                                  ''SRC_SYS5''                AS src_cd,
                                                                                                  cast(NULL AS VARCHAR(50)) AS party_type,
                                                                                                  CASE
                                                                                                                  WHEN bt1.retired_stg=0
                                                                                                                  AND             bc.retired_stg=0
                                                                                                                  AND             bu.retired_stg=0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END AS retired
                                                                                  FROM            db_t_prod_stag.bc_transaction bt1
                                                                                  inner join      db_t_prod_stag.bctl_transaction bct
                                                                                  ON              bct.id_stg=bt1.subtype_stg
                                                                                  inner join      db_t_prod_stag.bc_user bu
                                                                                  ON              bu.id_stg=bt1.createuserid_stg
                                                                                  inner join      db_t_prod_stag.bc_contact bc
                                                                                  ON              bc.id_stg=bu.contactid_stg
                                                                                  inner join      db_t_prod_stag.bc_revtrans br
                                                                                  ON              bt1.id_stg=br.ownerid_stg
                                                                                                  /* PI */
                                                                                  WHERE           ((
                                                                                                                                  bt1.updatetime_stg>:START_DTTM
                                                                                                                  AND             bt1.updatetime_stg<=:END_DTTM)
                                                                                                  OR              (
                                                                                                                                  bu.updatetime_stg>:START_DTTM
                                                                                                                  AND             bu.updatetime_stg<=:END_DTTM)
                                                                                                  OR             (
                                                                                                                                  bc.updatetime_stg>:START_DTTM
                                                                                                                  AND             bc.updatetime_stg<=:END_DTTM))
                                                                                  /************** Payment Payor **********************/
                                                                                  UNION ALL
                                                                                  /* *** UNION QUERY: 08 *** */
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE35''                 AS ev_act_type_code,
                                                                                                  cast (bis.id_stg AS VARCHAR (50)) AS key1,
                                                                                                  ''EV_SBTYPE2''                      AS SUBTYPE,
                                                                                                  bc.publicid_stg           AS nk_prty,
                                                                                                  ''EV_PRTY_ROLE6''                   AS role_cd,
                                                                                                  bis.createtime_stg,
                                                                                                  bis.updatetime_stg        AS tns_strt,
                                                                                                  ''SRC_SYS5''                AS src_cd ,
                                                                                                  cast(NULL AS VARCHAR(50)) AS party_type,
                                                                                                  CASE
                                                                                                                  WHEN bis.retired_stg=0
                                                                                                                  AND             bc.retired_stg=0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END AS retired
                                                                                  FROM            db_t_prod_stag.bc_invoicestream bis
                                                                                  join            db_t_prod_stag.bc_accountcontact bac
                                                                                  ON              bis.overridingpayer_alfa_stg=bac.id_stg
                                                                                  join            db_t_prod_stag.bc_contact bc
                                                                                  ON              bac.contactid_stg=bc.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_contact bcc
                                                                                  ON              bcc.id_stg=bc.subtype_stg
                                                                                  WHERE           overridingpayer_alfa_stg IS NOT NULL
                                                                                  AND             bcc.typecode_stg=(''UserContact'')
                                                                                                  /* PI */
                                                                                  AND             ((
                                                                                                                                  bis.updatetime_stg>:START_DTTM
                                                                                                                  AND             bis.updatetime_stg<=:END_DTTM)
                                                                                                  OR              (
                                                                                                                                  bac.updatetime_stg>:START_DTTM
                                                                                                                  AND             bac.updatetime_stg<=:END_DTTM)
                                                                                                  OR             (
                                                                                                                                  bc.updatetime_stg>:START_DTTM
                                                                                                                  AND             bc.updatetime_stg<=:END_DTTM))
                                                                                  UNION ALL
                                                                                  /************** DB_T_PROD_STAG.bc_paymentrequest **********************/
                                                                                  /* *** UNION QUERY: 09 *** */
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE35''               AS ev_act_type_code,
                                                                                                  cast(bpr.id_stg AS VARCHAR(60)) AS key1,
                                                                                                  ''EV_SBTYPE2''                    AS SUBTYPE,
                                                                                                  bc.publicid_stg                 AS nk_prty,
                                                                                                  ''EV_PRTY_ROLE1''                 AS role_cd,
                                                                                                  bpr.updatetime_stg,
                                                                                                  CASE
                                                                                                                  WHEN bpr.updatetime_stg>bu.updatetime_stg THEN bpr.updatetime_stg
                                                                                                                  ELSE bu.updatetime_stg
                                                                                                  END          AS tns_strt,
                                                                                                  ''SRC_SYS5''   AS src_cd,
                                                                                                  typecode_stg AS party_type,
                                                                                                  CASE
                                                                                                                  WHEN bpr.retired_stg=0
                                                                                                                  AND             bu.retired_stg=0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END AS retired
                                                                                  FROM            db_t_prod_stag.bc_paymentrequest bpr
                                                                                  inner join      db_t_prod_stag.bc_user bu
                                                                                  ON              bu.id_stg=bpr.updateuserid_stg
                                                                                  inner join      db_t_prod_stag.bc_contact bc
                                                                                  ON              bc.id_stg=bu.contactid_stg
                                                                                  inner join      db_t_prod_stag.bctl_contact bcc
                                                                                  ON              bc.subtype_stg=bcc.id_stg
                                                                                  WHERE           ((
                                                                                                                                  bpr.updatetime_stg>:START_DTTM
                                                                                                                  AND             bpr.updatetime_stg<=:END_DTTM)
                                                                                                  OR              (
                                                                                                                                  bc.updatetime_stg>:START_DTTM
                                                                                                                  AND             bc.updatetime_stg<=:END_DTTM))
                                                                                  UNION ALL
                                                                                  /* *** UNION QUERY: 10 *** */
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE35''               AS ev_act_type_code,
                                                                                                  cast(bpr.id_stg AS VARCHAR(60)) AS key1,
                                                                                                  ''EV_SBTYPE2''                    AS SUBTYPE,
                                                                                                  bc.addressbookuid_stg           AS nk_prty,
                                                                                                  ''EV_PRTY_ROLE6''                 AS role_cd,
                                                                                                  bpr.updatetime_stg,
                                                                                                  bpr.updatetime_stg AS tns_strt ,
                                                                                                  ''SRC_SYS5''         AS src_cd,
                                                                                                  typecode_stg       AS party_type,
                                                                                                  CASE
                                                                                                                  WHEN bpr.retired_stg=0
                                                                                                                  AND             bc.retired_stg=0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END AS retired
                                                                                  FROM            db_t_prod_stag.bc_account ba
                                                                                  left join       db_t_prod_stag.bc_invoicestream bi
                                                                                  ON              ba.id_stg=bi.accountid_stg
                                                                                  left join       db_t_prod_stag.bc_accountcontact bac
                                                                                  ON              bac.accountid_stg=ba.id_stg
                                                                                  left join       db_t_prod_stag.bc_contact bc
                                                                                  ON              bc.id_stg=bac.contactid_stg
                                                                                  inner join      db_t_prod_stag.bc_paymentrequest bpr
                                                                                  ON              bi.billingreferencenumber_alfa_stg=bpr.billingreferencenumber_alfa_stg
                                                                                  inner join      db_t_prod_stag.bctl_contact bcc
                                                                                  ON              bc.subtype_stg=bcc.id_stg
                                                                                  WHERE           primarypayer_stg<>0
                                                                                  AND             ((
                                                                                                                                  ba.updatetime_stg>:START_DTTM
                                                                                                                  AND             ba.updatetime_stg<=:END_DTTM)
                                                                                                  OR              (
                                                                                                                                  bi.updatetime_stg >:START_DTTM
                                                                                                                  AND             bi.updatetime_stg <=:END_DTTM)
                                                                                                  OR              (
                                                                                                                                  bac.updatetime_stg>:START_DTTM
                                                                                                                  AND             bac.updatetime_stg<=:END_DTTM)
                                                                                                  OR              (
                                                                                                                                  bc.updatetime_stg>:START_DTTM
                                                                                                                  AND             bc.updatetime_stg<=:END_DTTM)
                                                                                                  OR              (
                                                                                                                                  bpr.updatetime_stg>:START_DTTM
                                                                                                                  AND             bpr.updatetime_stg<=:END_DTTM))
                                                                                  /* PI */
                                                                                  UNION ALL
                                                                                  /* *** UNION QUERY: 11 *** */
                                                                                  SELECT          cast(''EV_ACTVY_TYPE36'' AS VARCHAR(60)) AS ev_act_type_code,
                                                                                                  cast(bua.id_stg AS        VARCHAR(60)) AS key1,
                                                                                                  ''EV_SBTYPE2''                           AS SUBTYPE,
                                                                                                  bc.publicid_stg                        AS nk_prty,
                                                                                                  ''EV_PRTY_ROLE7''                        AS role_cd,
                                                                                                  bua.updatetime_stg                     AS strt_tm,
                                                                                                  bua.updatetime_stg                     AS tns_strt,
                                                                                                  ''SRC_SYS5''                             AS src_cd,
                                                                                                  cast(NULL AS VARCHAR(50))              AS party_type,
                                                                                                  0                                      AS retired
                                                                                  FROM            db_t_prod_stag.bc_unappliedfund bua
                                                                                  left outer join db_t_prod_stag.bc_user bu
                                                                                  ON              bu.id_stg=bua.createuserid_stg
                                                                                  left outer join db_t_prod_stag.bc_contact bc
                                                                                  ON              bc.id_stg=bu.contactid_stg
                                                                                                  /* PI */
                                                                                  WHERE           ((
                                                                                                                                  bua.updatetime_stg>:START_DTTM
                                                                                                                  AND             bua.updatetime_stg<=:END_DTTM)
                                                                                                  OR              (
                                                                                                                                  bu.updatetime_stg>:START_DTTM
                                                                                                                  AND             bu.updatetime_stg<=:END_DTTM)
                                                                                                  OR             (
                                                                                                                                  bc.updatetime_stg>:START_DTTM
                                                                                                                  AND             bc.updatetime_stg<=:END_DTTM))
                                                                                  UNION ALL
                                                                                  /* *** UNION QUERY: 12 *** */
                                                                                  SELECT DISTINCT pcj.typecode_stg          AS ev_act_type_code,
                                                                                                  pj.jobnumber_stg          AS key1,
                                                                                                  ''EV_SBTYPE3''              AS SUBTYPE,
                                                                                                  pc.publicid_stg           AS nk_prty,
                                                                                                  ''EV_PRTY_ROLE1''           AS role_cd,
                                                                                                  pj.createtime_stg         AS strt_tm,
                                                                                                  pj.updatetime_stg         AS tns_strt,
                                                                                                  ''SRC_SYS4''                AS src_cd,
                                                                                                  cast(NULL AS VARCHAR(50)) AS party_type,
                                                                                                  CASE
                                                                                                                  WHEN pj.retired_stg=0
                                                                                                                  AND             pc.retired_stg=0
                                                                                                                  AND             pp.retired_stg=0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END retired
                                                                                  FROM            db_t_prod_stag.pc_job pj
                                                                                  inner join      db_t_prod_stag.pctl_job pcj
                                                                                  ON              pcj.id_stg=pj.subtype_stg
                                                                                  left outer join db_t_prod_stag.pc_user pu
                                                                                  ON              pj.createuserid_stg=pu.id_stg
                                                                                  left outer join db_t_prod_stag.pc_contact pc
                                                                                  ON              pu.contactid_stg=pc.id_stg
                                                                                  left outer join db_t_prod_stag.pc_policyperiod pp
                                                                                  ON              pj.id_stg=pp.jobid_stg
                                                                                  left join       db_t_prod_stag.pctl_policyperiodstatus pps
                                                                                  ON              pps.id_stg=pp.status_stg
                                                                                  left outer join db_t_prod_stag.pc_effectivedatedfields pe
                                                                                  ON              pe.branchid_stg=pp.id_stg
                                                                                  left outer join db_t_prod_stag.pcx_holineratingfactor_alfa pha
                                                                                  ON              pp.id_stg=pha.branchid_stg
                                                                                  WHERE           pp.policynumber_stg IS NOT NULL
                                                                                                  /* PI */
                                                                                  AND             ((
                                                                                                                                  pj.updatetime_stg>:START_DTTM
                                                                                                                  AND             pj.updatetime_stg<=:END_DTTM)
                                                                                                  OR              (
                                                                                                                                  pu.updatetime_stg>:START_DTTM
                                                                                                                  AND             pu.updatetime_stg<=:END_DTTM)
                                                                                                  OR             (
                                                                                                                                  pc.updatetime_stg>:START_DTTM
                                                                                                                  AND             pc.updatetime_stg<=:END_DTTM)
                                                                                                  OR             (
                                                                                                                                  pp.updatetime_stg>:START_DTTM
                                                                                                                  AND             pp.updatetime_stg<=:END_DTTM))
                                                                                  UNION ALL
                                                                                  /* *** UNION QUERY: 13 *** */
                                                                                  /* EIM-47738 Bring New column from GW DB_T_PROD_STAG.pc_job to EDW BASE for Track CSR Commissionable Prem */
                                                                                  SELECT          pcj.typecode_stg          AS ev_act_type_code,
                                                                                                  pj.jobnumber_stg          AS key1,
                                                                                                  ''EV_SBTYPE3''              AS SUBTYPE,
                                                                                                  pc.publicid_stg           AS nk_prty,
                                                                                                  ''EV_PRTY_ROLE8''           AS role_cd,
                                                                                                  pj.createtime_stg         AS strt_tm,
                                                                                                  pj.updatetime_stg         AS tns_strt,
                                                                                                  ''SRC_SYS4''                AS src_cd,
                                                                                                  cast(NULL AS VARCHAR(50)) AS party_type,
                                                                                                  CASE
                                                                                                                  WHEN pj.retired_stg=0
                                                                                                                  AND             pc.retired_stg=0
                                                                                                                  AND             pp.retired_stg=0
                                                                                                                  AND             pcred.retired_stg=0
                                                                                                                  AND             pu.retired_stg=0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END retired
                                                                                  FROM            db_t_prod_stag.pc_job pj
                                                                                  inner join      db_t_prod_stag.pctl_job pcj
                                                                                  ON              pcj.id_stg=pj.subtype_stg
                                                                                  left outer join db_t_prod_stag.pc_credential pcred
                                                                                  ON              pcred.username_stg = pj.binduser_alfa_stg
                                                                                  left outer join db_t_prod_stag.pc_user pu
                                                                                  ON              pcred.id_stg =pu.credentialid_stg
                                                                                  left outer join db_t_prod_stag.pc_contact pc
                                                                                  ON              pu.contactid_stg=pc.id_stg
                                                                                  left outer join db_t_prod_stag.pc_policyperiod pp
                                                                                  ON              pj.id_stg=pp.jobid_stg
                                                                                  left join       db_t_prod_stag.pctl_policyperiodstatus pps
                                                                                  ON              pps.id_stg=pp.status_stg
                                                                                  left outer join db_t_prod_stag.pc_effectivedatedfields pe
                                                                                  ON              pe.branchid_stg=pp.id_stg
                                                                                  left outer join db_t_prod_stag.pcx_holineratingfactor_alfa pha
                                                                                  ON              pp.id_stg=pha.branchid_stg
                                                                                  WHERE           pp.policynumber_stg IS NOT NULL
                                                                                  AND             pj.binduser_alfa_stg IS NOT NULL
                                                                                  AND             ((
                                                                                                                                  pj.updatetime_stg>:START_DTTM
                                                                                                                  AND             pj.updatetime_stg<=:END_DTTM)
                                                                                                  OR              (
                                                                                                                                  pu.updatetime_stg>:START_DTTM
                                                                                                                  AND             pu.updatetime_stg<=:END_DTTM)
                                                                                                  OR             (
                                                                                                                                  pc.updatetime_stg>:START_DTTM
                                                                                                                  AND             pc.updatetime_stg<=:END_DTTM)
                                                                                                  OR             (
                                                                                                                                  pp.updatetime_stg>:START_DTTM
                                                                                                                  AND             pp.updatetime_stg<=:END_DTTM)
                                                                                                  OR             (
                                                                                                                                  pcred.updatetime_stg>:START_DTTM
                                                                                                                  AND             pcred.updatetime_stg<=:END_DTTM))
                                                                                  UNION ALL
                                                                                  /* cc DB_T_PROD_STAG.ev_prty_x */
                                                                                  /* *** UNION QUERY: 14 *** */
                                                                                  SELECT DISTINCT ''EV_ACTVY_TYPE24''               AS ev_act_type_code,
                                                                                                  cast(cct.id_stg AS VARCHAR(60)) AS key1,
                                                                                                  ''EV_SBTYPE2''                    AS SUBTYPE,
                                                                                                  cc.publicid_stg                 AS nk_prty,
                                                                                                  ''EV_PRTY_ROLE1''                 AS role_cd,
                                                                                                  cct.createtime_stg              AS strt_tm,
                                                                                                  cct.updatetime_stg              AS tns_strt,
                                                                                                  ''SRC_SYS6''                      AS src_cd,
                                                                                                  cast(NULL AS VARCHAR(50))       AS party_type,
                                                                                                  CASE
                                                                                                                  WHEN cct.retired_stg=0
                                                                                                                  AND             cc.retired_stg=0
                                                                                                                  AND             cu.retired_stg=0 THEN 0
                                                                                                                  ELSE 1
                                                                                                  END retired
                                                                                  FROM            db_t_prod_stag.cc_transaction cct
                                                                                  join            db_t_prod_stag.cctl_transaction cctl
                                                                                  ON              cctl.id_stg=cct.subtype_stg
                                                                                                  /* DB_T_PROD_STAG.CC_CHECK  */
                                                                                  join            db_t_prod_stag.cc_user cu
                                                                                  ON              cct.createuserid_stg=cu.id_stg
                                                                                  join            db_t_prod_stag.cc_contact cc
                                                                                  ON              cc.id_stg=cu.contactid_stg
                                                                                  WHERE           typecode_stg=''Payment''
                                                                                                  /* WHERE CheckNumber is not null */
                                                                                  AND             ((
                                                                                                                                  cct.updatetime_stg>:START_DTTM
                                                                                                                  AND             cct.updatetime_stg<=:END_DTTM)
                                                                                                  OR             (
                                                                                                                                  cu.updatetime_stg>:START_DTTM
                                                                                                                  AND             cu.updatetime_stg<=:END_DTTM)
                                                                                                  OR             (
                                                                                                                                  cc.updatetime_stg>:START_DTTM
                                                                                                                  AND             cc.updatetime_stg<=:END_DTTM))
                                                                                  UNION ALL
                                                                                  /* *** UNION QUERY: 15 *** */
                                                                                  SELECT ''EV_ACTVY_TYPE23''               AS ev_act_type_code,
                                                                                         cast(cct.id_stg AS VARCHAR(60)) AS key1,
                                                                                         ''EV_SBTYPE2''                    AS SUBTYPE,
                                                                                         cc.publicid_stg                 AS nk_prty,
                                                                                         ''EV_PRTY_ROLE1''                 AS role_cd,
                                                                                         cct.createtime_stg              AS strt_tm,
                                                                                         cct.updatetime_stg              AS tns_strt,
                                                                                         ''SRC_SYS6''                      AS src_cd,
                                                                                         cast(NULL AS VARCHAR(50))       AS party_type,
                                                                                         CASE
                                                                                                WHEN cct.retired_stg=0
                                                                                                AND    cc.retired_stg=0
                                                                                                AND    cu.retired_stg=0 THEN 0
                                                                                                ELSE 1
                                                                                         END retired
                                                                                  FROM   db_t_prod_stag.cc_transaction cct
                                                                                  join   db_t_prod_stag.cctl_transaction cctl
                                                                                  ON     cctl.id_stg=cct.subtype_stg
                                                                                         /* DB_T_PROD_STAG.CC_CHECK  */
                                                                                  join   db_t_prod_stag.cc_user cu
                                                                                  ON     cct.createuserid_stg=cu.id_stg
                                                                                  join   db_t_prod_stag.cc_contact cc
                                                                                  ON     cc.id_stg=cu.contactid_stg
                                                                                  WHERE  typecode_stg=''Recovery''
                                                                                  AND    ((
                                                                                                       cct.updatetime_stg>:START_DTTM
                                                                                                AND    cct.updatetime_stg<=:END_DTTM)
                                                                                         OR    (
                                                                                                       cu.updatetime_stg>:START_DTTM
                                                                                                AND    cu.updatetime_stg<=:END_DTTM)
                                                                                         OR    (
                                                                                                       cc.updatetime_stg>:START_DTTM
                                                                                                AND    cc.updatetime_stg<=:END_DTTM))) AS a )
                                  /*  ----------------------> Source Query ends here <--------------------------- */
                                  
                         SELECT          tgt_lkp_ev_prty.ev_id             AS lkp_ev_id,
                                         tgt_lkp_ev_prty.ev_prty_role_cd   AS lkp_ev_prty_role_cd,
                                         tgt_lkp_ev_prty.prty_id           AS lkp_prty_id,
                                         tgt_lkp_ev_prty.ev_prty_strt_dttm AS lkp_ev_prty_strt_dttm,
                                         tgt_lkp_ev_prty.edw_strt_dttm     AS lkp_edw_strt_dttm,
                                         tgt_lkp_ev_prty.edw_end_dttm      AS lkp_edw_end_dttm,
                                         xlat_src.ev_id                    AS src_ev_id,
                                         xlat_src.out_ev_prty_role_cd      AS src_ev_prty_role_cd,
                                         xlat_src.src_prty_id              AS src_prty_id,
                                         xlat_src.ev_prty_strt_dttm        AS src_ev_prty_strt_dttm,
                                         xlat_src.ev_prty_end_dttm         AS src_ev_prty_end_dttm,
                                         xlat_src.prty_idntftn_type_cd     AS src_prty_idntftn_type_cd,
                                         xlat_src.trans_strt_dttm          AS trans_strt_dttm,
                                         xlat_src.rnk,
                                         xlat_src.retired AS retired,
                                         /* SourceData */
                                         cast (cast(coalesce(cast(xlat_src.ev_prty_strt_dttm AS timestamp), cast(''1900-01-01 00:00:00.000000'' AS timestamp)) AS           VARCHAR(60))
                                                         || cast(coalesce(cast(xlat_src.ev_prty_end_dttm AS timestamp),cast(''9999-12-31 00:00:00.000000'' AS timestamp))AS VARCHAR(60))
                                                         || trim(coalesce(xlat_src.src_prty_id,0)) AS VARCHAR(100)) AS sourcedata,
                                         /* -TargetData */
                                         cast (cast(coalesce(cast(tgt_lkp_ev_prty.ev_prty_strt_dttm AS timestamp), cast(''1900-01-01 00:00:00.000000'' AS timestamp)) AS            VARCHAR(60))
                                                         || cast(coalesce(cast(tgt_lkp_ev_prty.ev_prty_end_dttm AS timestamp),cast(''9999-12-31 00:00:00.000000'' AS timestamp)) AS VARCHAR(60))
                                                         || trim(coalesce(tgt_lkp_ev_prty.prty_id,0)) AS VARCHAR(100)) AS targetdata,
                                         /* Flag */
                                         CASE
                                                         WHEN tgt_lkp_ev_prty.ev_id IS NULL
                                                         AND             (
                                                                                         xlat_src.ev_id) IS NOT NULL
                                                         AND             (
                                                                                         xlat_src.src_prty_id) IS NOT NULL THEN ''I''
                                                         WHEN tgt_lkp_ev_prty.ev_id IS NOT NULL
                                                         AND             sourcedata<>targetdata THEN ''U''
                                                         WHEN tgt_lkp_ev_prty.ev_id IS NOT NULL
                                                         AND             sourcedata=targetdata THEN ''R''
                                         END AS ins_upd_flag
                         FROM
                                         /*source query with Expression*/
                                         (
                                                         SELECT          coalesce (xlat_ev_prty_role_cd.tgt_idntftn_val,''UNK'') AS out_ev_prty_role_cd,
                                                                         src.retired,
                                                                         src.ev_prty_strt_dttm                                                              AS ev_prty_strt_dttm,
                                                                         to_timestamp_ntz(''12/31/9999 23:59:59.999999'' ,''MM/DD/YYYY HH:MI:SS.FF6'' ) AS ev_prty_end_dttm,
                                                                         src.trans_strt_dttm                                                                AS trans_strt_dttm,
                                                                         src.rnk                                                                            AS rnk,
                                                                         ev_id_lkp.ev_id,
                                                                         CASE
                                                                                         WHEN upper(src.party_type)=''COMPANY'' THEN busn_prty_id_lkp.busn_prty_id
                                                                                         WHEN upper(src.party_type)=''PERSON'' THEN indiv_lkp_b.indiv_prty_id
                                                                                         WHEN upper(src.party_type)=''USERCONTACT'' THEN indiv_prty_id_lkp.indiv_prty_id
                                                                                         WHEN (
                                                                                                                         src.party_type) IS NULL THEN indiv_prty_id_lkp.indiv_prty_id
                                                                         END           AS src_prty_id,
                                                                         ''UNK''         AS prty_idntftn_type_cd
                                                         FROM            ev_prty_intrm AS src
                                                                         /*source query*/
                                                         left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_subtype
                                                         ON              xlat_subtype.src_idntftn_val=src.ev_subtype
                                                         left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_ev_activity_type_code
                                                         ON              xlat_ev_activity_type_code.src_idntftn_val=src.ev_act_type_cd
                                                         AND             xlat_ev_activity_type_code.tgt_idntftn_nm=''EV_ACTVY_TYPE''
                                                         AND             xlat_ev_activity_type_code.src_idntftn_nm IN ( ''derived'',
                                                                                                                       ''CCTL_ACTIVITYCATEGORY.TYPECODE'',
                                                                                                                       ''bctl_transaction.TYPECODE'',
                                                                                                                       ''bctl_activitycategory.typecode'',
                                                                                                                       ''bctl_activitytype.typecode'',
                                                                                                                       ''cctl_activitycategory.typecode'',
                                                                                                                       ''cctl_castastrophetype.typecode'',
                                                                                                                       ''pctl_activitycategory.typecode'',
                                                                                                                       ''pctl_activitytype.typecode'',
                                                                                                                       ''pctl_job.Typecode'')
                                                         AND             xlat_ev_activity_type_code.src_idntftn_sys IN (''GW'',
                                                                                                                        ''DS'' )
                                                         AND             xlat_ev_activity_type_code.expn_dt=''9999-12-31''
                                                         left outer join db_t_prod_core.teradata_etl_ref_xlat AS xlat_ev_prty_role_cd
                                                         ON              xlat_ev_prty_role_cd.src_idntftn_val=src.ev_prty_role_cd
                                                         AND             xlat_ev_prty_role_cd.tgt_idntftn_nm=''EV_PRTY_ROLE''
                                                         AND             xlat_ev_prty_role_cd.src_idntftn_nm=''derived''
                                                         AND             xlat_ev_prty_role_cd.src_idntftn_sys=''DS''
                                                         AND             xlat_ev_prty_role_cd.expn_dt=''9999-12-31''
                                                         left outer join
                                                                         (
                                                                                  SELECT   busn.busn_prty_id AS busn_prty_id,
                                                                                           busn.busn_ctgy_cd AS busn_ctgy_cd,
                                                                                           busn.nk_busn_cd   AS nk_busn_cd
                                                                                  FROM     db_t_prod_core.busn qualify row_number () over (PARTITION BY nk_busn_cd,busn_ctgy_cd ORDER BY edw_end_dttm DESC )=1 ) AS busn_prty_id_lkp
                                                         ON              busn_prty_id_lkp.busn_ctgy_cd=''CO''
                                                         AND             busn_prty_id_lkp.nk_busn_cd=src.nk_prty
                                                         left outer join
                                                                         (
                                                                                         SELECT DISTINCT indiv_prty_id,
                                                                                                         nk_publc_id
                                                                                         FROM            db_t_prod_core.indiv qualify row_number () over (PARTITION BY nk_publc_id ORDER BY edw_end_dttm DESC )=1 ) AS indiv_prty_id_lkp
                                                         ON              indiv_prty_id_lkp.nk_publc_id=src.nk_prty
                                                         AND             indiv_prty_id_lkp.nk_publc_id IS NOT NULL
                                                         left outer join
                                                                         (
                                                                                         SELECT DISTINCT indiv_prty_id ,
                                                                                                         nk_link_id ,
                                                                                                         nk_publc_id
                                                                                         FROM            db_t_prod_core.indiv qualify row_number () over (PARTITION BY nk_link_id ,nk_publc_id ORDER BY edw_end_dttm DESC )=1 ) AS indiv_lkp_b
                                                         ON              indiv_lkp_b.nk_link_id=src.nk_prty
                                                         AND             indiv_lkp_b.nk_publc_id IS NULL
                                                         left outer join
                                                                         (
                                                                                  SELECT   ev_id ,
                                                                                           ev_desc ,
                                                                                           ev_strt_dttm ,
                                                                                           ev_end_dttm ,
                                                                                           ev_rsn_cd ,
                                                                                           agmt_id ,
                                                                                           prcsd_src_sys_cd ,
                                                                                           func_cd ,
                                                                                           ev_dttm ,
                                                                                           edw_strt_dttm ,
                                                                                           trim(src_trans_id)     AS src_trans_id ,
                                                                                           trim(ev_sbtype_cd)     AS ev_sbtype_cd ,
                                                                                           trim(ev_actvy_type_cd) AS ev_actvy_type_cd
                                                                                  FROM     db_t_prod_core.ev
                                                                                  WHERE    trim(ev.src_trans_id) IN
                                                                                                                     (
                                                                                                                     SELECT DISTINCT trim(ev_key)
                                                                                                                     FROM            ev_prty_intrm) qualify row_number() over(PARTITION BY ev_sbtype_cd,ev_actvy_type_cd,src_trans_id ORDER BY edw_end_dttm DESC)=1) AS ev_id_lkp
                                                         ON              ev_id_lkp.src_trans_id=trim(src.ev_key)
                                                         AND             ev_id_lkp.ev_sbtype_cd=coalesce(trim(xlat_subtype.tgt_idntftn_val),''UNK'')
                                                         AND             ev_id_lkp.ev_actvy_type_cd=trim(xlat_ev_activity_type_code.tgt_idntftn_val) ) AS xlat_src
                         left outer join
                                         (
                                                  SELECT   ev_prty.ev_prty_strt_dttm AS ev_prty_strt_dttm,
                                                           ev_prty.ev_prty_end_dttm  AS ev_prty_end_dttm,
                                                           ev_prty.edw_strt_dttm     AS edw_strt_dttm,
                                                           ev_prty.edw_end_dttm      AS edw_end_dttm,
                                                           ev_prty.ev_id             AS ev_id,
                                                           ev_prty.ev_prty_role_cd   AS ev_prty_role_cd,
                                                           ev_prty.prty_id           AS prty_id
                                                  FROM     db_t_prod_core.ev_prty
                                                  WHERE    ev_id IN
                                                                     (
                                                                     SELECT DISTINCT ev_id
                                                                     FROM            db_t_prod_core.ev
                                                                     WHERE           ev.src_trans_id IN
                                                                                     (
                                                                                            SELECT ev_key
                                                                                            FROM   ev_prty_intrm)) qualify row_number() over(PARTITION BY ev_prty_role_cd,ev_id ORDER BY edw_end_dttm DESC)=1) AS tgt_lkp_ev_prty
                         ON              tgt_lkp_ev_prty.ev_id=xlat_src.ev_id
                         AND             tgt_lkp_ev_prty.ev_prty_role_cd=xlat_src.out_ev_prty_role_cd ) src ) );
  -- Component exp_ins_upd_flag, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ins_upd_flag AS
  (
         SELECT :prcs_id                                                               AS in_prcs_id,
                sq_bc_basemoneyreceived.rnk                                            AS rnk,
                sq_bc_basemoneyreceived.lkp_ev_id                                      AS lkp_ev_id,
                sq_bc_basemoneyreceived.lkp_ev_prty_role_cd                            AS lkp_ev_prty_role_cd,
                sq_bc_basemoneyreceived.lkp_prty_id                                    AS lkp_prty_id,
                sq_bc_basemoneyreceived.lkp_ev_prty_strt_dttm                          AS lkp_ev_prty_strt_dttm,
                sq_bc_basemoneyreceived.lkp_edw_strt_dttm                              AS lkp_edw_strt_dttm,
                sq_bc_basemoneyreceived.lkp_edw_end_dttm                               AS lkp_edw_end_dttm,
                sq_bc_basemoneyreceived.src_ev_id                                      AS src_ev_id,
                sq_bc_basemoneyreceived.src_ev_prty_role_cd                            AS src_ev_prty_role_cd,
                sq_bc_basemoneyreceived.src_prty_id                                    AS src_prty_id,
                sq_bc_basemoneyreceived.src_ev_prty_strt_dttm                          AS src_ev_prty_strt_dttm,
                sq_bc_basemoneyreceived.src_ev_prty_end_dttm                           AS src_ev_prty_end_dttm,
                sq_bc_basemoneyreceived.src_prty_idntftn_type_cd                       AS src_prty_idntftn_type_cd,
                current_timestamp                                                      AS edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS edw_end_dttm,
                sq_bc_basemoneyreceived.trans_strt_dttm                                AS trans_strt_dttm,
                sq_bc_basemoneyreceived.retired                                        AS retired,
                sq_bc_basemoneyreceived.ins_upd_flag                                   AS ins_upd_flag,
                sq_bc_basemoneyreceived.source_record_id
         FROM   sq_bc_basemoneyreceived );
  -- Component rtr_ev_prty_insupd_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_ev_prty_insupd_INSERT as
  SELECT exp_ins_upd_flag.lkp_ev_id                AS lkp_ev_prty_ev_id,
         exp_ins_upd_flag.lkp_ev_prty_role_cd      AS lkp_ev_prty_role_cd,
         exp_ins_upd_flag.lkp_prty_id              AS lkp_prty_id,
         exp_ins_upd_flag.lkp_edw_strt_dttm        AS lkp_edw_strt_dttm,
         exp_ins_upd_flag.src_ev_id                AS ev_id,
         exp_ins_upd_flag.src_ev_prty_role_cd      AS ev_prty_role_cd,
         exp_ins_upd_flag.src_prty_id              AS prty_id,
         exp_ins_upd_flag.src_ev_prty_strt_dttm    AS ev_prty_strt_dttm,
         exp_ins_upd_flag.src_ev_prty_end_dttm     AS ev_prty_end_dttm,
         exp_ins_upd_flag.src_prty_idntftn_type_cd AS prty_idntftn_type_cd,
         exp_ins_upd_flag.in_prcs_id               AS prcs_id,
         exp_ins_upd_flag.edw_strt_dttm            AS edw_strt_dttm,
         exp_ins_upd_flag.edw_end_dttm             AS edw_end_dttm,
         exp_ins_upd_flag.trans_strt_dttm          AS trans_strt_dttm,
         exp_ins_upd_flag.ins_upd_flag             AS ins_upd_flag,
         exp_ins_upd_flag.retired                  AS retired,
         exp_ins_upd_flag.lkp_edw_end_dttm         AS lkp_edw_end_dttm,
         exp_ins_upd_flag.lkp_ev_prty_strt_dttm    AS lkp_ev_prty_strt_dttm,
         exp_ins_upd_flag.rnk                      AS rnk,
         exp_ins_upd_flag.source_record_id
  FROM   exp_ins_upd_flag
  WHERE  exp_ins_upd_flag.src_ev_id IS NOT NULL
  AND    exp_ins_upd_flag.src_prty_id IS NOT NULL
  AND    (
                exp_ins_upd_flag.ins_upd_flag = ''I''
         OR     exp_ins_upd_flag.ins_upd_flag = ''U''
         OR     (
                       exp_ins_upd_flag.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                AND    exp_ins_upd_flag.retired = 0 ) );
  
  -- Component rtr_ev_prty_insupd_RETIRED, Type ROUTER Output Group RETIRED
  create or replace temporary table rtr_ev_prty_insupd_retired as
  SELECT exp_ins_upd_flag.lkp_ev_id                AS lkp_ev_prty_ev_id,
         exp_ins_upd_flag.lkp_ev_prty_role_cd      AS lkp_ev_prty_role_cd,
         exp_ins_upd_flag.lkp_prty_id              AS lkp_prty_id,
         exp_ins_upd_flag.lkp_edw_strt_dttm        AS lkp_edw_strt_dttm,
         exp_ins_upd_flag.src_ev_id                AS ev_id,
         exp_ins_upd_flag.src_ev_prty_role_cd      AS ev_prty_role_cd,
         exp_ins_upd_flag.src_prty_id              AS prty_id,
         exp_ins_upd_flag.src_ev_prty_strt_dttm    AS ev_prty_strt_dttm,
         exp_ins_upd_flag.src_ev_prty_end_dttm     AS ev_prty_end_dttm,
         exp_ins_upd_flag.src_prty_idntftn_type_cd AS prty_idntftn_type_cd,
         exp_ins_upd_flag.in_prcs_id               AS prcs_id,
         exp_ins_upd_flag.edw_strt_dttm            AS edw_strt_dttm,
         exp_ins_upd_flag.edw_end_dttm             AS edw_end_dttm,
         exp_ins_upd_flag.trans_strt_dttm          AS trans_strt_dttm,
         exp_ins_upd_flag.ins_upd_flag             AS ins_upd_flag,
         exp_ins_upd_flag.retired                  AS retired,
         exp_ins_upd_flag.lkp_edw_end_dttm         AS lkp_edw_end_dttm,
         exp_ins_upd_flag.lkp_ev_prty_strt_dttm    AS lkp_ev_prty_strt_dttm,
         exp_ins_upd_flag.rnk                      AS rnk,
         exp_ins_upd_flag.source_record_id
  FROM   exp_ins_upd_flag
  WHERE  exp_ins_upd_flag.ins_upd_flag = ''R''
  AND    exp_ins_upd_flag.retired != 0
  AND    exp_ins_upd_flag.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_ev_prty_updclose_retire, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ev_prty_updclose_retire AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ev_prty_insupd_retired.lkp_ev_prty_ev_id     AS ev_id,
                rtr_ev_prty_insupd_retired.lkp_ev_prty_role_cd   AS ev_prty_role_cd,
                rtr_ev_prty_insupd_retired.lkp_prty_id           AS prty_id,
                rtr_ev_prty_insupd_retired.prcs_id               AS prcs_id3,
                rtr_ev_prty_insupd_retired.lkp_edw_strt_dttm     AS edw_strt_dttm,
                rtr_ev_prty_insupd_retired.lkp_ev_prty_strt_dttm AS lkp_ev_prty_strt_dttm4,
                rtr_ev_prty_insupd_retired.trans_strt_dttm       AS trans_strt_dttm4,
                1                                                AS update_strategy_action,
				rtr_ev_prty_insupd_retired.source_record_id
         FROM   rtr_ev_prty_insupd_retired );
  -- Component exp_pass_to_target_updclose1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_updclose1 AS
  (
         SELECT upd_ev_prty_updclose_retire.ev_id                  AS ev_id,
                upd_ev_prty_updclose_retire.ev_prty_role_cd        AS ev_prty_role_cd,
                current_timestamp                                  AS out_edw_end_dttm,
                upd_ev_prty_updclose_retire.edw_strt_dttm          AS edw_strt_dttm,
                upd_ev_prty_updclose_retire.lkp_ev_prty_strt_dttm4 AS lkp_ev_prty_strt_dttm4,
                upd_ev_prty_updclose_retire.trans_strt_dttm4       AS trans_strt_dttm4,
                upd_ev_prty_updclose_retire.source_record_id
         FROM   upd_ev_prty_updclose_retire );
  -- Component tgt_ev_prty_updclose1, Type TARGET
  merge
  INTO         db_t_prod_core.ev_prty
  USING        exp_pass_to_target_updclose1
  ON (
                            ev_prty.ev_id = exp_pass_to_target_updclose1.ev_id
               AND          ev_prty.ev_prty_role_cd = exp_pass_to_target_updclose1.ev_prty_role_cd
               AND          ev_prty.edw_strt_dttm = exp_pass_to_target_updclose1.edw_strt_dttm)
  WHEN matched THEN
  UPDATE
  SET    ev_id = exp_pass_to_target_updclose1.ev_id,
         ev_prty_role_cd = exp_pass_to_target_updclose1.ev_prty_role_cd,
         ev_prty_strt_dttm = exp_pass_to_target_updclose1.lkp_ev_prty_strt_dttm4,
         edw_strt_dttm = exp_pass_to_target_updclose1.edw_strt_dttm,
         edw_end_dttm = exp_pass_to_target_updclose1.out_edw_end_dttm,
         trans_end_dttm = exp_pass_to_target_updclose1.trans_strt_dttm4;
  
  -- Component tgt_ev_prty_updclose1, Type Post SQL
  UPDATE db_t_prod_core.ev_prty
    SET    trans_end_dttm= a.lead,
         edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT ev_id,
                                         ev_prty_role_cd,
                                         edw_strt_dttm,
                                         max(trans_strt_dttm) over (PARTITION BY ev_id,ev_prty_role_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead,
                                         max(edw_strt_dttm) over (PARTITION BY ev_id,ev_prty_role_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.ev_prty ) a
  WHERE  ev_prty.edw_strt_dttm = a.edw_strt_dttm
  AND    ev_prty.ev_id=a.ev_id
  AND    ev_prty.ev_prty_role_cd=a.ev_prty_role_cd
  AND    ev_prty.trans_strt_dttm <>ev_prty.trans_end_dttm
  AND    lead IS NOT NULL;
  
  -- Component upd_ev_prty_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ev_prty_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ev_prty_insupd_insert.ev_id                AS ev_id,
                rtr_ev_prty_insupd_insert.ev_prty_role_cd      AS ev_prty_role_cd,
                rtr_ev_prty_insupd_insert.prty_id              AS prty_id,
                rtr_ev_prty_insupd_insert.ev_prty_strt_dttm    AS ev_prty_strt_dttm,
                rtr_ev_prty_insupd_insert.ev_prty_end_dttm     AS ev_prty_end_dttm,
                rtr_ev_prty_insupd_insert.prcs_id              AS prcs_id,
                rtr_ev_prty_insupd_insert.edw_strt_dttm        AS edw_strt_dttm,
                rtr_ev_prty_insupd_insert.edw_end_dttm         AS edw_end_dttm,
                rtr_ev_prty_insupd_insert.prty_idntftn_type_cd AS prty_idntftn_type_cd1,
                rtr_ev_prty_insupd_insert.trans_strt_dttm      AS trans_strt_dttm1,
                rtr_ev_prty_insupd_insert.retired              AS retired1,
                rtr_ev_prty_insupd_insert.rnk                  AS rnk1,
                0                                              AS update_strategy_action,
				rtr_ev_prty_insupd_insert.source_record_id
         FROM   rtr_ev_prty_insupd_insert );
  -- Component exp_pass_to_target_insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_insert AS
  (
         SELECT upd_ev_prty_ins.ev_id                 AS ev_id,
                upd_ev_prty_ins.ev_prty_role_cd       AS ev_prty_role_cd,
                upd_ev_prty_ins.prty_id               AS prty_id,
                upd_ev_prty_ins.ev_prty_strt_dttm     AS ev_prty_strt_dttm,
                upd_ev_prty_ins.ev_prty_end_dttm      AS ev_prty_end_dttm,
                upd_ev_prty_ins.prcs_id               AS prcs_id,
                upd_ev_prty_ins.prty_idntftn_type_cd1 AS prty_idntftn_type_cd1,
                upd_ev_prty_ins.trans_strt_dttm1      AS trans_strt_dttm1,
                CASE
                       WHEN upd_ev_prty_ins.retired1 = 0 THEN upd_ev_prty_ins.edw_end_dttm
                       ELSE upd_ev_prty_ins.edw_strt_dttm
                END AS edw_end_dttm1,
                CASE
                       WHEN upd_ev_prty_ins.retired1 <> 0 THEN upd_ev_prty_ins.trans_strt_dttm1
                       ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END AS trans_end_dttm,
                CASE
                       WHEN upd_ev_prty_ins.retired1 = 0 THEN dateadd(''second'', ( 2 * ( upd_ev_prty_ins.rnk1 - 1 ) ), current_timestamp)
                       ELSE current_timestamp
                END AS o_edw_date,
                upd_ev_prty_ins.source_record_id
         FROM   upd_ev_prty_ins );
  -- Component tgt_ev_prty_ins, Type TARGET
  INSERT INTO db_t_prod_core.ev_prty
              (
                          ev_id,
                          ev_prty_role_cd,
                          prty_id,
                          ev_prty_strt_dttm,
                          ev_prty_end_dttm,
                          prty_idntftn_type_cd,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_pass_to_target_insert.ev_id                 AS ev_id,
         exp_pass_to_target_insert.ev_prty_role_cd       AS ev_prty_role_cd,
         exp_pass_to_target_insert.prty_id               AS prty_id,
         exp_pass_to_target_insert.ev_prty_strt_dttm     AS ev_prty_strt_dttm,
         exp_pass_to_target_insert.ev_prty_end_dttm      AS ev_prty_end_dttm,
         exp_pass_to_target_insert.prty_idntftn_type_cd1 AS prty_idntftn_type_cd,
         exp_pass_to_target_insert.prcs_id               AS prcs_id,
         exp_pass_to_target_insert.o_edw_date            AS edw_strt_dttm,
         exp_pass_to_target_insert.edw_end_dttm1         AS edw_end_dttm,
         exp_pass_to_target_insert.trans_strt_dttm1      AS trans_strt_dttm,
         exp_pass_to_target_insert.trans_end_dttm        AS trans_end_dttm
  FROM   exp_pass_to_target_insert;

END;
';