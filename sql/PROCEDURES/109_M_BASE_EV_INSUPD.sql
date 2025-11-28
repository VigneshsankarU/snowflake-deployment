-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_EV_INSUPD("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component sq_cc_catastrophe, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_cc_catastrophe AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS ev_strt_dt,
                $2  AS out_agmt_ev_type_cd,
                $3  AS ev_end_dt,
                $4  AS retired,
                $5  AS transactionnumber,
                $6  AS rnk,
                $7  AS ev_rsn_cd,
                $8  AS src_trans_id,
                $9  AS out_ev_sbtype_cd,
                $10 AS out_ev_actvy_type_cd,
                $11 AS prcsd_src_sys_cd,
                $12 AS out_ev_ctgy_type,
                $13 AS func_cd,
                $14 AS src_sys_cd,
                $15 AS agmt_id,
                $16 AS agmt_ev_dttm,
                $17 AS xref_ev_id,
                $18 AS lkp_ev_id,
                $19 AS lkp_agmt_ev_type_cd,
                $20 AS lkp_ev_strt_dttm,
                $21 AS lkp_ev_end_dttm,
                $22 AS lkp_ev_rsn_cd,
                $23 AS lkp_ev_num,
                $24 AS lkp_agmt_id,
                $25 AS lkp_prcsd_src_sys_cd,
                $26 AS lkp_func_cd,
                $27 AS lkp_ev_ctgy_type_cd,
                $28 AS lkp_ev_dttm,
                $29 AS lkp_edw_end_dttm,
                $30 AS lkp_edw_strt_dttm_upd,
                $31 AS lkp_aplctn_id,
                $32 AS aplctn_id,
                $33 AS md5_src,
                $34 AS md5_tgt,
                $35 AS out_ins_upd,
                $36 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     ( WITH ev_intrm1 AS
                                  (
                                                  SELECT DISTINCT cast(''EV_ACTVY_TYPE14'' AS VARCHAR(50))                 AS ev_act_type_code ,
                                                                  cast(NULL AS              VARCHAR(50))                 AS agmt_ev_type_cd,
                                                                  cast(bc_basemoneyreceived.createtime_stg AS timestamp) AS ev_strt_dt ,
                                                                  cast(NULL AS timestamp)                                AS ev_end_dt ,
                                                                  cast( bc_basemoneyreceived.id_stg AS VARCHAR(60))      AS key1 ,
                                                                  cast($ev_sbtype2_financl AS          VARCHAR(50))      AS SUBTYPE ,
                                                                  cast(
                                                                  CASE
                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                  ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                  END AS VARCHAR(100))AS agmt_host_id,
                                                                  CASE
                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN cast(''ACT''AS VARCHAR(50))
                                                                                  ELSE cast(''INV''AS                                                               VARCHAR(50))
                                                                  END                                     AS agmt_type ,
                                                                  bctl_paymentreversalreason.typecode_stg AS reason ,
                                                                  cast(NULL AS timestamp)                 AS agmt_evnt_dttm ,
                                                                  $src_sys5_gwbc                          AS src_sys ,
                                                                  cast(NULL AS VARCHAR(50))               AS trm_nbr ,
                                                                  bc_basemoneyreceived.retired_stg        AS retired ,
                                                                  bctl_paymentsource_alfa.typecode_stg    AS bill_payment_src ,
                                                                  cast(NULL AS                  VARCHAR(255))              AS transactionnumber ,
                                                                  cast($ctgy_type2_detltrans AS VARCHAR(50))               AS ev_ctgy_type ,
                                                                  cast($func_type1_bill AS      VARCHAR(50))               AS func_cd
                                                  FROM            db_t_prod_stag.bc_basemoneyreceived
                                                  inner join      db_t_prod_stag.bctl_basemoneyreceived
                                                  ON              bctl_basemoneyreceived.id_stg=bc_basemoneyreceived.subtype_stg
                                                  left outer join db_t_prod_stag.bctl_paymentreversalreason
                                                  ON              bctl_paymentreversalreason.id_stg=bc_basemoneyreceived.reversalreason_stg
                                                  left outer join db_t_prod_stag.bctl_paymentsource_alfa
                                                  ON              bc_basemoneyreceived.paymentsource_alfa_stg=bctl_paymentsource_alfa.id_stg
                                                  left outer join db_t_prod_stag.bc_account
                                                  ON              bc_basemoneyreceived.accountid_stg = bc_account.id_stg
                                                  left outer join db_t_prod_stag.bc_unappliedfund
                                                  ON              bc_unappliedfund.id_stg=bc_basemoneyreceived.unappliedfundid_stg
                                                  left outer join db_t_prod_stag.bc_invoicestream
                                                  ON              bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                  WHERE           bctl_basemoneyreceived.typecode_stg IN (''PaymentMoneyReceived'',
                                                                                                          ''DirectBillMoneyRcvd'' ,
                                                                                                          ''ZeroDollarDMR'',
                                                                                                          ''ZeroDollarReversal'')
                                                  AND             bc_basemoneyreceived.updatetime_stg > ( $start_dttm)
                                                  AND             bc_basemoneyreceived.updatetime_stg <= ( $end_dttm)
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE25''                                      AS ev_act_type_code ,
                                                                  cast(NULL AS VARCHAR(50))                              AS agmt_ev_type_cd,
                                                                  cast(bc_basemoneyreceived.createtime_stg AS timestamp) AS ev_strt_dt ,
                                                                  cast(NULL AS timestamp)                                AS ev_end_dt ,
                                                                  cast( bc_basemoneyreceived.id_stg AS VARCHAR(60))      AS key1 ,
                                                                  cast($ev_sbtype2_financl AS          VARCHAR(50))      AS SUBTYPE ,
                                                                  cast(
                                                                  CASE
                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                  ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                  END AS VARCHAR(100)) AS agmt_host_id ,
                                                                  CASE
                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN cast(''ACT''AS VARCHAR(50))
                                                                                  ELSE cast(''INV''AS                                                               VARCHAR(50))
                                                                  END                                     AS agmt_type ,
                                                                  bctl_paymentreversalreason.typecode_stg AS reason ,
                                                                  cast(NULL AS timestamp)                 AS agmt_evnt_dttm ,
                                                                  $src_sys5_gwbc                          AS src_sys ,
                                                                  cast(NULL AS VARCHAR(50))               AS trm_nbr ,
                                                                  bc_basemoneyreceived.retired_stg        AS retired ,
                                                                  bctl_paymentsource_alfa.typecode_stg    AS bill_payment_src ,
                                                                  cast(NULL AS                  VARCHAR(255))              AS transactionnumber ,
                                                                  cast($ctgy_type2_detltrans AS VARCHAR(50))               AS ev_ctgy_type ,
                                                                  cast($func_type1_bill AS      VARCHAR(50))               AS func_cd
                                                  FROM            db_t_prod_stag.bc_basemoneyreceived
                                                  inner join      db_t_prod_stag.bctl_basemoneyreceived
                                                  ON              bctl_basemoneyreceived.id_stg=bc_basemoneyreceived.subtype_stg
                                                  left outer join db_t_prod_stag.bctl_paymentreversalreason
                                                  ON              bctl_paymentreversalreason.id_stg=bc_basemoneyreceived.reversalreason_stg
                                                  left outer join db_t_prod_stag.bctl_paymentsource_alfa
                                                  ON              bc_basemoneyreceived.paymentsource_alfa_stg=bctl_paymentsource_alfa.id_stg
                                                  left outer join db_t_prod_stag.bc_account
                                                  ON              bc_basemoneyreceived.accountid_stg = bc_account.id_stg
                                                  left outer join db_t_prod_stag.bc_unappliedfund
                                                  ON              bc_unappliedfund.id_stg=bc_basemoneyreceived.unappliedfundid_stg
                                                  left outer join db_t_prod_stag.bc_invoicestream
                                                  ON              bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                  WHERE           bctl_basemoneyreceived.typecode_stg IN (''PaymentMoneyReceived'',
                                                                                                          ''DirectBillMoneyRcvd'' ,
                                                                                                          ''ZeroDollarDMR'',
                                                                                                          ''ZeroDollarReversal'')
                                                  AND             bc_basemoneyreceived.reversaldate_stg IS NOT NULL
                                                  AND             bc_basemoneyreceived.updatetime_stg > ( $start_dttm)
                                                  AND             bc_basemoneyreceived.updatetime_stg <= ( $end_dttm)
                                                  /****************************************bc_basemoneyreceived****************************/
                                                  UNION
                                                  /***************************bc_basedistitem****************************/
                                                  SELECT DISTINCT cast(''EV_ACTVY_TYPE35'' AS VARCHAR(50))                                  AS ev_act_type_code ,
                                                                  cast(NULL AS              VARCHAR(50))                                  AS agmt_ev_type_cd,
                                                                  cast(createtime_stg AS timestamp)                                       AS ev_strt_dt ,
                                                                  cast(NULL AS timestamp)                                                 AS ev_end_dt ,
                                                                  cast( bc_paymentrequest.id_stg AS                         VARCHAR(60))  AS key1 ,
                                                                  cast($ev_sbtype2_financl AS                               VARCHAR(50))  AS SUBTYPE ,
                                                                  cast(bc_paymentrequest.billingreferencenumber_alfa_stg AS VARCHAR(100)) AS agmt_host_id ,
                                                                  cast(''INV'' AS                                             VARCHAR(50))  AS agmt_type ,
                                                                  cast('''' AS                                                VARCHAR(60))  AS reason ,
                                                                  cast(NULL AS timestamp)                                                 AS agmt_evnt_dttm ,
                                                                  $src_sys5_gwbc                                                          AS src_sys ,
                                                                  cast(NULL AS VARCHAR(50))                                               AS trm_nbr,
                                                                  bc_paymentrequest.retired_stg                                           AS retired ,
                                                                  cast('''' AS                    VARCHAR(255))                                                AS bill_payment_src ,
                                                                  cast(NULL AS                  VARCHAR(255))                                                AS transactionnumber ,
                                                                  cast($ctgy_type2_detltrans AS VARCHAR(50))                                                 AS ev_ctgy_type ,
                                                                  cast($func_type1_bill AS      VARCHAR(50))                                                 AS func_cd
                                                  FROM            db_t_prod_stag.bc_paymentrequest
                                                  WHERE           bc_paymentrequest.updatetime_stg > ( $start_dttm)
                                                  AND             bc_paymentrequest.updatetime_stg <= ( $end_dttm)
                                                  UNION
                                                  SELECT    cast(''EV_ACTVY_TYPE36'' AS VARCHAR(50))             AS ev_act_type_code ,
                                                            cast( NULL AS             VARCHAR(50))             AS agmt_ev_type_cd,
                                                            cast(bc_unappliedfund.createtime_stg AS timestamp) AS ev_strt_dt ,
                                                            cast(NULL AS timestamp)                            AS ev_end_dt ,
                                                            cast( bc_unappliedfund.id_stg AS VARCHAR(60))      AS key1 ,
                                                            cast($ev_sbtype2_financl AS      VARCHAR(50))      AS SUBTYPE ,
                                                            cast(
                                                            CASE
                                                                      WHEN billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                      ELSE bc_unappliedfund.billingreferencenumber_alfa_stg
                                                            END AS VARCHAR(100)) AS agmt_host_id ,
                                                            CASE
                                                                      WHEN billingreferencenumber_alfa_stg IS NULL THEN ''ACT''
                                                                      ELSE''INV''
                                                            END                        AS agmt_type ,
                                                            cast('''' AS VARCHAR(60))    AS reason ,
                                                            cast(NULL AS timestamp)    AS agmt_evnt_dttm ,
                                                            $src_sys5_gwbc             AS src_sys ,
                                                            cast(NULL AS                  VARCHAR(50))  AS trm_nbr,
                                                            cast(''0'' AS                   INT)          AS retired,
                                                            cast('''' AS                    VARCHAR(255)) AS bill_payment_src ,
                                                            cast(NULL AS                  VARCHAR(255)) AS transactionnumber ,
                                                            cast($ctgy_type2_detltrans AS VARCHAR(50))  AS ev_ctgy_type ,
                                                            cast($func_type1_bill AS      VARCHAR(50))  AS func_cd
                                                  FROM      db_t_prod_stag.bc_unappliedfund
                                                  left join db_t_prod_stag.bc_account
                                                  ON        bc_unappliedfund.accountid_stg = bc_account.id_stg
                                                  left join db_t_prod_stag.bc_taccount ba
                                                  ON        bc_unappliedfund.taccountid_stg = ba.id_stg
                                                  WHERE     ((
                                                                                bc_unappliedfund.updatetime_stg > ( $start_dttm)
                                                                      AND       bc_unappliedfund.updatetime_stg <= ( $end_dttm))
                                                            OR        (
                                                                                ba.updatetime_stg > ( $start_dttm)
                                                                      AND       ba.updatetime_stg <= ( $end_dttm)))
                                                  UNION
                                                  /***************************bc_basenonrecdistitem****************************/
                                                  SELECT DISTINCT cast(''EV_ACTVY_TYPE28'' AS VARCHAR(50) )ev_act_type_code ,
                                                                  cast( NULL AS             VARCHAR(50))                  AS agmt_ev_type_cd,
                                                                  cast(bc_basenonrecdistitem.createtime_stg AS timestamp) AS ev_strt_dt ,
                                                                  cast(releaseddate_stg AS timestamp)                     AS ev_end_dt ,
                                                                  cast(bc_basenonrecdistitem.id_stg AS VARCHAR(60))       AS key1 ,
                                                                  cast($ev_sbtype2_financl AS          VARCHAR(50))       AS SUBTYPE ,
                                                                  cast(
                                                                  CASE
                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                  ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                  END AS VARCHAR(100))AS agmt_host_id ,
                                                                  cast(
                                                                  CASE
                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN ''ACT''
                                                                                  ELSE ''INV''
                                                                  END AS     VARCHAR(60))           AS agmt_type ,
                                                                  cast('''' AS VARCHAR(60))           AS reason ,
                                                                  cast(NULL AS timestamp)           AS agmt_evnt_dttm ,
                                                                  $src_sys5_gwbc                    AS src_sys ,
                                                                  cast(NULL AS VARCHAR(50))         AS trm_nbr ,
                                                                  bc_basenonrecdistitem.retired_stg AS retired ,
                                                                  cast('''' AS                    VARCHAR(255))          AS bill_payment_src ,
                                                                  cast(NULL AS                  VARCHAR(255))          AS transactionnumber ,
                                                                  cast($ctgy_type2_detltrans AS VARCHAR(50))           AS ev_ctgy_type,
                                                                  cast($func_type1_bill AS      VARCHAR(50))           AS func_cd
                                                  FROM            db_t_prod_stag.bc_basenonrecdistitem
                                                  left join       db_t_prod_stag.bc_basedist
                                                  ON              bc_basedist.id_stg=bc_basenonrecdistitem.activedistid_stg
                                                  left join       db_t_prod_stag.bc_basemoneyreceived
                                                  ON              bc_basemoneyreceived.basedistid_stg=bc_basedist.id_stg
                                                  left join       db_t_prod_stag.bc_unappliedfund
                                                  ON              bc_basemoneyreceived.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                  left join       db_t_prod_stag.bc_invoicestream
                                                  ON              bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                  left join       db_t_prod_stag.bc_account
                                                  ON              bc_basemoneyreceived.accountid_stg = bc_account.id_stg
                                                  WHERE           bc_basenonrecdistitem.updatetime_stg > ( $start_dttm)
                                                  AND             bc_basenonrecdistitem.updatetime_stg <= ( $end_dttm)
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE29'' ev_act_type_code ,
                                                                  cast(NULL AS VARCHAR(50))                               AS agmt_ev_type_cd,
                                                                  cast(bc_basenonrecdistitem.createtime_stg AS timestamp) AS ev_strt_dt,
                                                                  cast(releaseddate_stg AS timestamp)                     AS ev_end_dt ,
                                                                  cast(bc_basenonrecdistitem.id_stg AS VARCHAR(60))       AS key1,
                                                                  cast($ev_sbtype2_financl AS          VARCHAR(50))       AS SUBTYPE ,
                                                                  cast(
                                                                  CASE
                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                  ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                  END AS VARCHAR(100))AS agmt_host_id ,
                                                                  CASE
                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN cast(''ACT''AS VARCHAR(50))
                                                                                  ELSE cast(''INV''AS                                                               VARCHAR(50))
                                                                  END                               AS agmt_type ,
                                                                  cast('''' AS VARCHAR(60))           AS reason ,
                                                                  cast(NULL AS timestamp)           AS agmt_evnt_dttm ,
                                                                  $src_sys5_gwbc                    AS src_sys ,
                                                                  cast(NULL AS VARCHAR(50))         AS trm_nbr ,
                                                                  bc_basenonrecdistitem.retired_stg AS retired ,
                                                                  cast('''' AS                    VARCHAR(255))          AS bill_payment_src ,
                                                                  cast(NULL AS                  VARCHAR(255))          AS transactionnumber ,
                                                                  cast($ctgy_type2_detltrans AS VARCHAR(50))           AS ev_ctgy_type ,
                                                                  cast($func_type1_bill AS      VARCHAR(50))           AS func_cd
                                                  FROM            db_t_prod_stag.bc_basenonrecdistitem
                                                  left join       db_t_prod_stag.bc_basedist
                                                  ON              bc_basedist.id_stg=bc_basenonrecdistitem.activedistid_stg
                                                  left join       db_t_prod_stag.bc_basemoneyreceived
                                                  ON              bc_basemoneyreceived.basedistid_stg=bc_basedist.id_stg
                                                  left join       db_t_prod_stag.bc_unappliedfund
                                                  ON              bc_basemoneyreceived.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                  left join       db_t_prod_stag.bc_invoicestream
                                                  ON              bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                  left join       db_t_prod_stag.bc_account
                                                  ON              bc_basemoneyreceived.accountid_stg = bc_account.id_stg
                                                  WHERE           bc_basenonrecdistitem.updatetime_stg > ( $start_dttm)
                                                  AND             bc_basenonrecdistitem.updatetime_stg <= ( $end_dttm)
                                                  AND             bc_basenonrecdistitem.reverseddate_stg IS NOT NULL
                                                  /***************************bc_basenonrecdistitem****************************/
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE31''             AS ev_act_type_code ,
                                                                  cast( NULL AS VARCHAR(50))    AS agmt_ev_type_cd,
                                                                  a.createtime_stg              AS ev_strt_dt ,
                                                                  cast(NULL AS timestamp)       AS ev_end_dt ,
                                                                  cast(a.id_stg AS            VARCHAR(60)) AS key1 ,
                                                                  cast($ev_sbtype2_financl AS VARCHAR(50)) AS SUBTYPE ,
                                                                  cast(
                                                                  CASE
                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN acc.accountnumber_stg
                                                                                  ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                  END AS VARCHAR(100)) AS agmt_host_id ,
                                                                  CASE
                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN ''ACT''
                                                                                  ELSE ''INV''
                                                                  END                        agmt_type ,
                                                                  cast('''' AS VARCHAR(60))   AS reason ,
                                                                  cast(NULL AS timestamp)   AS agmt_evnt_dttm ,
                                                                  $src_sys5_gwbc            AS src_sys ,
                                                                  cast(NULL AS VARCHAR(50)) AS trm_nbr ,
                                                                  a.retired_stg             AS retired ,
                                                                  cast('''' AS                    VARCHAR(255))  AS bill_payment_src ,
                                                                  cast(NULL AS                  VARCHAR(255))  AS transactionnumber ,
                                                                  cast($ctgy_type2_detltrans AS VARCHAR(50))   AS ev_ctgy_type ,
                                                                  cast($func_type1_bill AS      VARCHAR(50))   AS func_cd
                                                  FROM            (
                                                                                  SELECT          bc_outgoingpayment.*,
                                                                                                  bc_paymentinstrument.paymentmethod_stg,
                                                                                                  bctl_paymentmethod.typecode_stg AS fund_trnsfr_mthd_typ_stg
                                                                                  FROM            db_t_prod_stag.bc_outgoingpayment
                                                                                  left outer join db_t_prod_stag.bc_paymentinstrument
                                                                                  ON              bc_outgoingpayment.paymentinstrumentid_stg = bc_paymentinstrument.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_paymentmethod
                                                                                  ON              bctl_paymentmethod.id_stg = bc_paymentinstrument.paymentmethod_stg) a
                                                  join
                                                                  (
                                                                                  SELECT          bc_outgoingpayment.*,
                                                                                                  bc_disbursement.status_stg AS bcdisbursementstatus_stg
                                                                                  FROM            db_t_prod_stag.bc_outgoingpayment
                                                                                  left outer join db_t_prod_stag.bc_disbursement
                                                                                  ON              bc_outgoingpayment.disbursementid_stg = bc_disbursement.id_stg) b
                                                  ON              a.id_stg=b.id_stg
                                                  AND             a.updatetime_stg > ( $start_dttm)
                                                  AND             a.updatetime_stg <= ( $end_dttm)
                                                  left join       db_t_prod_stag.bc_disbursement
                                                  ON              bc_disbursement.id_stg=b.disbursementid_stg
                                                  left join       db_t_prod_stag.bc_unappliedfund
                                                  ON              bc_unappliedfund.id_stg=bc_disbursement.unappliedfundid_stg
                                                  left join       db_t_prod_stag.bc_invoicestream
                                                  ON              bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                  left join       db_t_prod_stag.bc_account
                                                  ON              (
                                                                                  bc_account.id_stg = bc_invoicestream. policyid_stg
                                                                  AND             bc_disbursement.accountid_stg = bc_account.id_stg)
                                                  left join       db_t_prod_stag.bc_accountcontact
                                                  ON              bc_account.id_stg = bc_accountcontact.accountid_stg
                                                  left join       db_t_prod_stag.bc_contact
                                                  ON              bc_accountcontact.contactid_stg = bc_contact.id_stg
                                                  left join       db_t_prod_stag.bc_account acc
                                                  ON              bc_disbursement.accountid_stg = acc.id_stg
                                                  left outer join db_t_prod_stag.bctl_contact
                                                  ON              bctl_contact.id_stg = bc_contact.subtype_stg
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE32''                           AS ev_act_type_code ,
                                                                  cast( NULL AS VARCHAR(50))                  AS agmt_ev_type_cd,
                                                                  bc_disbursement.createtime_stg              AS ev_strt_dt ,
                                                                  cast(NULL AS timestamp)                     AS ev_end_dt ,
                                                                  cast(bc_disbursement.id_stg AS VARCHAR(60)) AS key1 ,
                                                                  cast($ev_sbtype2_financl AS    VARCHAR(50)) AS SUBTYPE ,
                                                                  cast(
                                                                  CASE
                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                  ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                  END AS VARCHAR(100)) AS agmt_host_id ,
                                                                  CASE
                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN cast(''ACT''AS VARCHAR(50))
                                                                                  ELSE cast(''INV''AS                                                               VARCHAR(50))
                                                                  END                         AS agmt_type ,
                                                                  bctl_reason.typecode_stg    AS reason ,
                                                                  cast(NULL AS timestamp)     AS agmt_evnt_dttm ,
                                                                  $src_sys5_gwbc              AS src_sys ,
                                                                  cast(NULL AS VARCHAR(50))   AS trm_nbr ,
                                                                  bc_disbursement.retired_stg AS retired ,
                                                                  cast('''' AS                    VARCHAR(255))    AS bill_payment_src ,
                                                                  cast(NULL AS                  VARCHAR(255))    AS transactionnumber ,
                                                                  cast($ctgy_type2_detltrans AS VARCHAR(50))     AS ev_ctgy_type ,
                                                                  cast($func_type1_bill AS      VARCHAR(50))     AS func_cd
                                                  FROM            db_t_prod_stag.bc_disbursement
                                                  left join       db_t_prod_stag.bc_unappliedfund
                                                  ON              bc_unappliedfund.id_stg=bc_disbursement.unappliedfundid_stg
                                                  left join       db_t_prod_stag.bc_invoicestream
                                                  ON              bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                  left join       db_t_prod_stag.bc_account
                                                  ON              bc_disbursement.accountid_stg = bc_account.id_stg
                                                  left join       db_t_prod_stag.bctl_reason
                                                  ON              bctl_reason.id_stg=bc_disbursement.reason_stg
                                                  WHERE           bc_disbursement.updatetime_stg > ( $start_dttm)
                                                  AND             bc_disbursement.updatetime_stg <= ( $end_dttm)
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE31''             AS ev_act_type_code ,
                                                                  cast( NULL AS VARCHAR(50))    AS agmt_ev_type_cd,
                                                                  a.createtime_stg              AS ev_strt_dt ,
                                                                  cast(NULL AS timestamp)       AS ev_end_dt ,
                                                                  cast(a.id_stg AS            VARCHAR(60)) AS key1 ,
                                                                  cast($ev_sbtype2_financl AS VARCHAR(50)) AS SUBTYPE ,
                                                                  cast(
                                                                  CASE
                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN acc.accountnumber_stg
                                                                                  ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                  END AS VARCHAR(100)) AS agmt_host_id ,
                                                                  CASE
                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN ''ACT''
                                                                                  ELSE ''INV''
                                                                  END                        agmt_type ,
                                                                  cast('''' AS VARCHAR(60))   AS reason ,
                                                                  cast(NULL AS timestamp)   AS agmt_evnt_dttm ,
                                                                  $src_sys5_gwbc            AS src_sys ,
                                                                  cast(NULL AS VARCHAR(50)) AS trm_nbr ,
                                                                  a.retired_stg             AS retired ,
                                                                  cast('''' AS                    VARCHAR(255))  AS bill_payment_src ,
                                                                  cast(NULL AS                  VARCHAR(255))  AS transactionnumber ,
                                                                  cast($ctgy_type2_detltrans AS VARCHAR(50))   AS ev_ctgy_type ,
                                                                  cast($func_type1_bill AS      VARCHAR(50))   AS func_cd
                                                  FROM            (
                                                                                  SELECT          bc_outgoingpayment.*,
                                                                                                  bc_paymentinstrument.paymentmethod_stg,
                                                                                                  bctl_paymentmethod.typecode_stg AS fund_trnsfr_mthd_typ_stg
                                                                                  FROM            db_t_prod_stag.bc_outgoingpayment
                                                                                  left outer join db_t_prod_stag.bc_paymentinstrument
                                                                                  ON              bc_outgoingpayment.paymentinstrumentid_stg = bc_paymentinstrument.id_stg
                                                                                  left outer join db_t_prod_stag.bctl_paymentmethod
                                                                                  ON              bctl_paymentmethod.id_stg = bc_paymentinstrument.paymentmethod_stg) a
                                                  join
                                                                  (
                                                                                  SELECT          bc_outgoingpayment.*,
                                                                                                  bc_disbursement.status_stg AS bcdisbursementstatus_stg
                                                                                  FROM            db_t_prod_stag.bc_outgoingpayment
                                                                                  left outer join db_t_prod_stag.bc_disbursement
                                                                                  ON              bc_outgoingpayment.disbursementid_stg = bc_disbursement.id_stg) b
                                                  ON              a.id_stg=b.id_stg
                                                  AND             a.updatetime_stg > ( $start_dttm)
                                                  AND             a.updatetime_stg <= ( $end_dttm)
                                                  left join       db_t_prod_stag.bc_disbursement
                                                  ON              bc_disbursement.id_stg=b.disbursementid_stg
                                                  left join       db_t_prod_stag.bc_unappliedfund
                                                  ON              bc_unappliedfund.id_stg=bc_disbursement.unappliedfundid_stg
                                                  left join       db_t_prod_stag.bc_invoicestream
                                                  ON              bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                  left join       db_t_prod_stag.bc_account
                                                  ON              (
                                                                                  bc_account.id_stg = bc_invoicestream. policyid_stg
                                                                  AND             bc_disbursement.accountid_stg = bc_account.id_stg)
                                                  left join       db_t_prod_stag.bc_accountcontact
                                                  ON              bc_account.id_stg = bc_accountcontact.accountid_stg
                                                  left join       db_t_prod_stag.bc_contact
                                                  ON              bc_accountcontact.contactid_stg = bc_contact.id_stg
                                                  left join       db_t_prod_stag.bc_account acc
                                                  ON              bc_disbursement.accountid_stg = acc.id_stg
                                                  left outer join db_t_prod_stag.bctl_contact
                                                  ON              bctl_contact.id_stg = bc_contact.subtype_stg
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE32''                           AS ev_act_type_code ,
                                                                  cast(NULL AS VARCHAR(50))                   AS agmt_ev_type_cd,
                                                                  bc_disbursement.createtime_stg              AS ev_strt_dt ,
                                                                  cast(NULL AS timestamp)                     AS ev_end_dt ,
                                                                  cast(bc_disbursement.id_stg AS VARCHAR(60)) AS key1 ,
                                                                  cast($ev_sbtype2_financl AS    VARCHAR(50)) AS SUBTYPE ,
                                                                  cast(
                                                                  CASE
                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN bc_account.accountnumber_stg
                                                                                  ELSE bc_invoicestream.billingreferencenumber_alfa_stg
                                                                  END AS VARCHAR(100))AS agmt_host_id ,
                                                                  CASE
                                                                                  WHEN bc_invoicestream.billingreferencenumber_alfa_stg IS NULL THEN cast(''ACT''AS VARCHAR(50))
                                                                                  ELSE cast(''INV''AS                                                               VARCHAR(50))
                                                                  END                         AS agmt_type ,
                                                                  bctl_reason.typecode_stg    AS reason ,
                                                                  cast(NULL AS timestamp)     AS agmt_evnt_dttm ,
                                                                  $src_sys5_gwbc              AS src_sys ,
                                                                  cast(NULL AS VARCHAR(50))   AS trm_nbr ,
                                                                  bc_disbursement.retired_stg AS retired ,
                                                                  cast('''' AS                    VARCHAR(255))    AS bill_payment_src ,
                                                                  cast(NULL AS                  VARCHAR(255))    AS transactionnumber ,
                                                                  cast($ctgy_type2_detltrans AS VARCHAR(50))     AS ev_ctgy_type ,
                                                                  cast($func_type1_bill AS      VARCHAR(50))     AS func_cd
                                                  FROM            db_t_prod_stag.bc_disbursement
                                                  left join       db_t_prod_stag.bc_unappliedfund
                                                  ON              bc_unappliedfund.id_stg=bc_disbursement.unappliedfundid_stg
                                                  left join       db_t_prod_stag.bc_invoicestream
                                                  ON              bc_invoicestream.unappliedfundid_stg=bc_unappliedfund.id_stg
                                                  left join       db_t_prod_stag.bc_account
                                                  ON              bc_disbursement.accountid_stg = bc_account.id_stg
                                                  left join       db_t_prod_stag.bctl_reason
                                                  ON              bctl_reason.id_stg=bc_disbursement.reason_stg
                                                  WHERE           bc_disbursement.updatetime_stg > ( $start_dttm)
                                                  AND             bc_disbursement.updatetime_stg <= ( $end_dttm)
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE30''                               AS ev_act_type_code ,
                                                                  cast( NULL AS VARCHAR(50))                      AS agmt_ev_type_cd,
                                                                  cast(createtime_stg AS timestamp)               AS ev_strt_dt ,
                                                                  cast(NULL AS timestamp)                         AS ev_end_dt ,
                                                                  cast( bc_suspensepayment.id_stg AS VARCHAR(60)) AS key1 ,
                                                                  cast($ev_sbtype2_financl AS        VARCHAR(50)) AS SUBTYPE ,
                                                                  CASE
                                                                                  WHEN bc_suspensepayment.policynumber_stg IS NOT NULL THEN bc_suspensepayment.policynumber_stg
                                                                                  WHEN bc_suspensepayment.billingreferencenumber_alfa_stg IS NOT NULL THEN bc_suspensepayment.billingreferencenumber_alfa_stg
                                                                                  WHEN bc_suspensepayment.accountnumber_stg IS NOT NULL THEN bc_suspensepayment.accountnumber_stg
                                                                                  ELSE NULL
                                                                  END AS agmhostid ,
                                                                  CASE
                                                                                  WHEN bc_suspensepayment.policynumber_stg IS NOT NULL THEN ''POL''
                                                                                  WHEN bc_suspensepayment.billingreferencenumber_alfa_stg IS NOT NULL THEN ''INV''
                                                                                  WHEN bc_suspensepayment.accountnumber_stg IS NOT NULL THEN ''ACT''
                                                                                  ELSE ''UNK''
                                                                  END                            AS agmttype ,
                                                                  cast('''' AS VARCHAR(60))        AS reason ,
                                                                  cast(NULL AS timestamp)        AS agmt_evnt_dttm ,
                                                                  $src_sys5_gwbc                 AS src_sys ,
                                                                  cast(NULL AS VARCHAR(50))      AS trm_nbr ,
                                                                  bc_suspensepayment.retired_stg AS retired ,
                                                                  cast('''' AS                    VARCHAR(255))       AS bill_payment_src ,
                                                                  cast(NULL AS                  VARCHAR(255))       AS transactionnumber ,
                                                                  cast($ctgy_type2_detltrans AS VARCHAR(50))        AS ev_ctgy_type ,
                                                                  cast($func_type1_bill AS      VARCHAR(50))        AS func_cd
                                                  FROM            db_t_prod_stag.bc_suspensepayment
                                                  WHERE           bc_suspensepayment.updatetime_stg > ( $start_dttm)
                                                  AND             bc_suspensepayment.updatetime_stg <= ( $end_dttm)
                                                  UNION
                                                  /********************************************************Policy Transactions*************************************************************/
                                                  SELECT DISTINCT pctl_job.typecode_stg                                  AS ev_act_type_code ,
                                                                  cast( NULL AS VARCHAR(50))                             AS agmt_ev_type_cd,
                                                                  pc_job.createtime_stg                                  AS ev_strt_dt ,
                                                                  pc_job.closedate_stg                                   AS ev_end_dt ,
                                                                  pc_job.jobnumber_stg                                   AS key1 ,
                                                                  $ev_sbtype3_plcytrns                                   AS SUBTYPE ,
                                                                  cast(pc_policyperiod.policynumber_stg AS VARCHAR(100)) AS agmt_host_id ,
                                                                  cast(''POL'' AS                            VARCHAR(50))     agmt_type ,
                                                                  ''''                                                     AS reason ,
                                                                  pc_job.createtime_stg                                  AS agmt_evnt_dttm ,
                                                                  $src_sys4_gwpc                                         AS src_sys ,
                                                                  cast(NULL AS VARCHAR(50))                              AS trm_nbr ,
                                                                  pc_job.retired_stg                                     AS retired ,
                                                                  pctl_cancellationsource.typecode_stg                   AS bill_payment_src ,
                                                                  cast(NULL AS                  VARCHAR(255))                             AS transactionnumber ,
                                                                  cast($ctgy_type2_detltrans AS VARCHAR(50))                              AS ev_ctgy_type ,
                                                                  cast($func_type3_pol AS       VARCHAR(50) )                             AS func_cd
                                                  FROM            db_t_prod_stag.pc_job
                                                  inner join      db_t_prod_stag.pctl_job
                                                  ON              pctl_job.id_stg=pc_job.subtype_stg
                                                  left outer join db_t_prod_stag.pc_policyperiod
                                                  ON              pc_job.id_stg = pc_policyperiod.jobid_stg
                                                  left join       db_t_prod_stag.pctl_cancellationsource
                                                  ON              pc_job.source_stg = pctl_cancellationsource.id_stg
                                                  left join       db_t_prod_stag.pctl_policyperiodstatus
                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.status_stg
                                                  left outer join db_t_prod_stag.pc_effectivedatedfields
                                                  ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                  left outer join db_t_prod_stag.pcx_holineratingfactor_alfa
                                                  ON              pc_policyperiod.id_stg=pcx_holineratingfactor_alfa.branchid_stg
                                                  WHERE           pc_policyperiod.updatetime_stg > ( $start_dttm)
                                                  AND             pc_policyperiod.updatetime_stg <= ( $end_dttm)
                                                  AND             pctl_policyperiodstatus.typecode_stg<>''Temporary''
                                                  AND             pc_effectivedatedfields.expirationdate_stg IS NULL
                                                  AND             pcx_holineratingfactor_alfa.expirationdate_stg IS NULL
                                                  AND             pc_policyperiod.policynumber_stg IS NOT NULL ), ev_intrm2 AS
                                  (
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE26''                                 AS ev_act_type_code ,
                                                                  cast( NULL AS VARCHAR(50))                        AS agmt_ev_type_cd,
                                                                  cast(bc_basedistitem.createtime_stg AS timestamp) AS ev_strt_dt ,
                                                                  cast(NULL AS timestamp)                           AS ev_end_dt ,
                                                                  cast(bc_basedistitem.id_stg AS           VARCHAR(60))       AS key1 ,
                                                                  cast($ev_sbtype2_financl AS              VARCHAR(50))       AS SUBTYPE ,
                                                                  cast(bc_policyperiod.policynumber_stg AS VARCHAR(100))      AS agmt_host_id ,
                                                                  ''POLTRM''                                                    AS agmt_type ,
                                                                  cast('''' AS VARCHAR(60))                                     AS reason ,
                                                                  cast(NULL AS timestamp)                                     AS agmt_evnt_dttm ,
                                                                  $src_sys5_gwbc                                              AS src_sys ,
                                                                  cast(bc_policyperiod.termnumber_stg AS VARCHAR(50))         AS trm_nbr ,
                                                                  bc_basedistitem.retired_stg                                 AS retired ,
                                                                  cast('''' AS                    VARCHAR(255))                                    AS bill_payment_src ,
                                                                  cast(NULL AS                  VARCHAR(255))                                    AS transactionnumber ,
                                                                  cast($ctgy_type2_detltrans AS VARCHAR(50))                                     AS ev_ctgy_type ,
                                                                  cast($func_type1_bill AS      VARCHAR(50))                                     AS func_cd
                                                  FROM            db_t_prod_stag.bc_basedistitem
                                                  join            db_t_prod_stag.bc_invoiceitem
                                                  ON              bc_basedistitem.invoiceitemid_stg=bc_invoiceitem.id_stg
                                                  join            db_t_prod_stag.bc_invoice
                                                  ON              bc_invoice.id_stg=bc_invoiceitem.invoiceid_stg
                                                  join            db_t_prod_stag.bctl_invoiceitemtype
                                                  ON              bctl_invoiceitemtype.id_stg=bc_invoiceitem.type_stg
                                                  join            db_t_prod_stag.bc_charge
                                                  ON              bc_charge.id_stg=bc_invoiceitem.chargeid_stg
                                                  join            db_t_prod_stag.bc_chargepattern
                                                  ON              bc_chargepattern.id_stg=bc_charge.chargepatternid_stg
                                                  join            db_t_prod_stag.bctl_chargecategory
                                                  ON              bctl_chargecategory.id_stg=bc_chargepattern.category_stg
                                                  left join       db_t_prod_stag.bc_policyperiod
                                                  ON              bc_basedistitem.policyperiodid_stg=bc_policyperiod.id_stg
                                                  WHERE           bc_basedistitem.updatetime_stg > ( $start_dttm)
                                                  AND             bc_basedistitem.updatetime_stg <= ( $end_dttm)
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE27''                                 AS ev_act_type_code ,
                                                                  cast( NULL AS VARCHAR(50))                        AS agmt_ev_type_cd,
                                                                  cast(bc_basedistitem.createtime_stg AS timestamp) AS ev_strt_dt ,
                                                                  cast(NULL AS timestamp)                           AS ev_end_dt ,
                                                                  cast(bc_basedistitem.id_stg AS           VARCHAR(60))       AS key1 ,
                                                                  cast($ev_sbtype2_financl AS              VARCHAR(50))       AS SUBTYPE ,
                                                                  cast(bc_policyperiod.policynumber_stg AS VARCHAR(100))      AS agmt_host_id ,
                                                                  ''POLTRM''                                                    AS agmt_type ,
                                                                  cast('''' AS VARCHAR(60))                                     AS reason ,
                                                                  cast(NULL AS timestamp)                                     AS agmt_evnt_dttm ,
                                                                  $src_sys5_gwbc                                              AS src_sys ,
                                                                  cast(bc_policyperiod.termnumber_stg AS VARCHAR(50))         AS trm_nbr ,
                                                                  bc_basedistitem.retired_stg                                 AS retired ,
                                                                  cast('''' AS                    VARCHAR(255))                                    AS bill_payment_src ,
                                                                  cast(NULL AS                  VARCHAR(255))                                    AS transactionnumber ,
                                                                  cast($ctgy_type2_detltrans AS VARCHAR(50))                                     AS ev_ctgy_type ,
                                                                  cast($func_type1_bill AS      VARCHAR(50))                                     AS func_cd
                                                  FROM            db_t_prod_stag.bc_basedistitem
                                                  join            db_t_prod_stag.bc_invoiceitem
                                                  ON              bc_basedistitem.invoiceitemid_stg=bc_invoiceitem.id_stg
                                                  join            db_t_prod_stag.bc_invoice
                                                  ON              bc_invoice.id_stg=bc_invoiceitem.invoiceid_stg
                                                  join            db_t_prod_stag.bctl_invoiceitemtype
                                                  ON              bctl_invoiceitemtype.id_stg=bc_invoiceitem.type_stg
                                                  join            db_t_prod_stag.bc_charge
                                                  ON              bc_charge.id_stg=bc_invoiceitem.chargeid_stg
                                                  join            db_t_prod_stag.bc_chargepattern
                                                  ON              bc_chargepattern.id_stg=bc_charge.chargepatternid_stg
                                                  join            db_t_prod_stag.bctl_chargecategory
                                                  ON              bctl_chargecategory.id_stg=bc_chargepattern.category_stg
                                                  left join       db_t_prod_stag.bc_policyperiod
                                                  ON              bc_basedistitem.policyperiodid_stg=bc_policyperiod.id_stg
                                                  WHERE           bc_basedistitem.updatetime_stg > ( $start_dttm)
                                                  AND             bc_basedistitem.updatetime_stg <= ( $end_dttm)
                                                  AND             bc_basedistitem.reverseddate_stg IS NOT NULL
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE33''                             AS ev_act_type_code ,
                                                                  cast( NULL AS VARCHAR(50))                    AS agmt_ev_type_cd,
                                                                  cast(bc_writeoff.createtime_stg AS timestamp) AS ev_strt_dt ,
                                                                  cast(NULL AS timestamp)                       AS ev_end_dt ,
                                                                  cast(bc_writeoff.id_stg AS  VARCHAR(60))       AS key1 ,
                                                                  cast($ev_sbtype2_financl AS VARCHAR(50))       AS SUBTYPE ,
                                                                  cast(a.policynumber_stg AS  VARCHAR(100))      AS agmt_host_id ,
                                                                  ''POLTRM''                                       AS agmt_type ,
                                                                  bctl_writeoffreason.typecode_stg               AS reason ,
                                                                  cast(NULL AS timestamp)                        AS agmt_evnt_dttm ,
                                                                  $src_sys5_gwbc                                 AS src_sys ,
                                                                  cast(a.termnumber_stg AS VARCHAR(50))          AS trm_nbr ,
                                                                  bc_writeoff.retired_stg                        AS retired ,
                                                                  cast('''' AS                    VARCHAR(255))                       AS bill_payment_src ,
                                                                  cast(NULL AS                  VARCHAR(255))                       AS transactionnumber ,
                                                                  cast($ctgy_type2_detltrans AS VARCHAR(50))                        AS ev_ctgy_type ,
                                                                  cast($func_type1_bill AS      VARCHAR(50))                        AS func_cd
                                                  FROM            db_t_prod_stag.bc_writeoff
                                                  left join       db_t_prod_stag.bc_invoiceitem
                                                  ON              bc_invoiceitem.id_stg=bc_writeoff.itemwritenoff_alfa_stg
                                                  left join       db_t_prod_stag.bc_policyperiod a
                                                  ON              a.id_stg=bc_invoiceitem.policyperiodid_stg
                                                  left join       db_t_prod_stag.bctl_writeoffreason
                                                  ON              bctl_writeoffreason.id_stg=bc_writeoff.reason_stg
                                                  WHERE           bc_writeoff.id_stg NOT IN
                                                                  (
                                                                         SELECT ownerid_stg
                                                                         FROM   db_t_prod_stag.bc_revwriteoff)
                                                  AND             bc_writeoff.updatetime_stg > ( $start_dttm)
                                                  AND             bc_writeoff.updatetime_stg <= ( $end_dttm)
                                                  UNION
                                                  SELECT DISTINCT cast(''EV_ACTVY_TYPE34'' AS VARCHAR(50))        AS ev_act_type_code ,
                                                                  cast( NULL AS             VARCHAR(50))        AS agmt_ev_type_cd,
                                                                  cast(bc_writeoff.createtime_stg AS timestamp) AS ev_strt_dt ,
                                                                  cast(NULL AS timestamp)                       AS ev_end_dt ,
                                                                  cast(bc_writeoff.id_stg AS               VARCHAR(60))       AS key1 ,
                                                                  cast($ev_sbtype2_financl AS              VARCHAR(50))       AS SUBTYPE ,
                                                                  cast(a.policynumber_stg AS               VARCHAR(100))      AS agmt_host_id ,
                                                                  cast(''POLTRM'' AS                         VARCHAR(60))       AS agmt_type ,
                                                                  cast(bctl_writeoffreason.typecode_stg AS VARCHAR(50))       AS reason ,
                                                                  cast(NULL AS timestamp)                                     AS agmt_evnt_dttm ,
                                                                  $src_sys5_gwbc                                              AS src_sys ,
                                                                  cast(a.termnumber_stg AS VARCHAR(50))                       AS trm_nbr ,
                                                                  bc_writeoff.retired_stg                                     AS retired ,
                                                                  cast('''' AS                    VARCHAR(255))                                    AS bill_payment_src ,
                                                                  cast(NULL AS                  VARCHAR(255))                                    AS transactionnumber ,
                                                                  cast($ctgy_type2_detltrans AS VARCHAR(50))                                     AS ev_ctgy_type ,
                                                                  cast($func_type1_bill AS      VARCHAR(50))                                     AS func_cd
                                                  FROM            db_t_prod_stag.bc_writeoff
                                                  left join       db_t_prod_stag.bc_invoiceitem
                                                  ON              bc_invoiceitem.id_stg=bc_writeoff.itemwritenoff_alfa_stg
                                                  left join       db_t_prod_stag.bc_policyperiod a
                                                  ON              a.id_stg=bc_invoiceitem.policyperiodid_stg
                                                  left join       db_t_prod_stag.bctl_writeoffreason
                                                  ON              bctl_writeoffreason.id_stg=bc_writeoff.reason_stg
                                                  WHERE           bc_writeoff.id_stg IN
                                                                  (
                                                                         SELECT ownerid_stg
                                                                         FROM   db_t_prod_stag.bc_revwriteoff)
                                                  AND             bc_writeoff.updatetime_stg > ( $start_dttm)
                                                  AND             bc_writeoff.updatetime_stg <= ( $end_dttm) ), ev_intrm3 AS
                                  (
                                                  /******************* Catastrophe Event***********/
                                                  SELECT DISTINCT tlcat.typecode_stg          AS ev_act_type_code ,
                                                                  cast( NULL AS VARCHAR(50))  AS agmt_ev_type_cd,
                                                                  cccat.createtime_stg        AS ev_strt_dt ,
                                                                  cast(NULL AS timestamp)     AS ev_end_dt ,
                                                                  cccat.catastrophenumber_stg AS key1 ,
                                                                  $ev_sbtype1_catstrph        AS SUBTYPE ,
                                                                  cast('''' AS   VARCHAR(100))    AS agmt_host_id ,
                                                                  cast(NULL AS VARCHAR(50))     AS agmt_type ,
                                                                  ''''                            AS reason ,
                                                                  cast(NULL AS timestamp)       AS agmt_evnt_dttm ,
                                                                  $src_sys6_gwcc                AS src_sys ,
                                                                  cast(NULL AS VARCHAR(50))     AS trm_nbr ,
                                                                  cccat.retired_stg             AS retired ,
                                                                  cast('''' AS                    VARCHAR(255))      AS bill_payment_src ,
                                                                  cast(NULL AS                  VARCHAR(255))      AS transactionnumber ,
                                                                  cast($ctgy_type2_detltrans AS VARCHAR(50))       AS ev_ctgy_type ,
                                                                  cast(''CLM'' AS                 VARCHAR(50))       AS func_cd
                                                  FROM            db_t_prod_stag.cc_catastrophe cccat,
                                                                  db_t_prod_stag.cctl_catastrophetype tlcat
                                                  WHERE           cccat.type_stg=tlcat.id_stg
                                                  AND             cccat.updatetime_stg>( $start_dttm)
                                                  AND             cccat.updatetime_stg <= ( $end_dttm) ), 
                                    ev_intrm4 AS
                                    (
                                         SELECT ev_act_type_code,
                                                agmt_ev_type_cd,
                                                ev_strt_dt,
                                                ev_end_dt,
                                                key1,
                                                SUBTYPE,
                                                cast(ltrim(rtrim(agmt_host_id)) AS VARCHAR(100))AS agmt_host_id,
                                                agmt_type,
                                                reason,
                                                agmt_evnt_dttm,
                                                src_sys,
                                                trm_nbr,
                                                retired,
                                                bill_payment_src,
                                                transactionnumber,
                                                ev_ctgy_type,
                                                func_cd
                                         FROM  (
                                                                SELECT DISTINCT cast(''EV_ACTVY_TYPE24'' AS VARCHAR(250)) AS ev_act_type_code,
                                                                                cast( NULL AS             VARCHAR(50))  AS agmt_ev_type_cd,
                                                                                cast(a.createtime_stg AS timestamp)     AS ev_strt_dt,
                                                                                cast(NULL AS timestamp)                 AS ev_end_dt,
                                                                                cast(a.id_stg AS            VARCHAR(60))           AS key1,
                                                                                cast($ev_sbtype2_financl AS VARCHAR(50))           AS SUBTYPE ,
                                                                                cast(
                                                                                CASE
                                                                                                WHEN cast (cc_policy.verified_stg AS VARCHAR (20))=''0'' THEN cc_policy.id_stg
                                                                                                                /* Added as part of EIM-33951   */
                                                                                                WHEN cast (cc_policy.verified_stg AS VARCHAR (20)) <>''0'' THEN pp.publicid_stg
                                                                                END AS        VARCHAR(100)) AS agmt_host_id ,
                                                                                cast(''PPV'' AS VARCHAR(50))  AS agmt_type ,
                                                                                cast('''' AS    VARCHAR(250)) AS reason ,
                                                                                cast(NULL AS timestamp)     AS agmt_evnt_dttm ,
                                                                                $src_sys6_gwcc              AS src_sys ,
                                                                                cast(NULL AS VARCHAR(50))   AS trm_nbr ,
                                                                                a.retired_stg               AS retired ,
                                                                                cast('''' AS                    VARCHAR(255))    AS bill_payment_src ,
                                                                                cast(NULL AS                  VARCHAR(255))    AS transactionnumber ,
                                                                                cast($ctgy_type1_gnrltrans AS VARCHAR(50))     AS ev_ctgy_type ,
                                                                                cast($func_type2_clm AS       VARCHAR(50))     AS func_cd ,
                                                                                CASE
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''voided''
                                                                                                AND             cc.payload_new_stg=''voided_11'' THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''voided''
                                                                                                AND             cc.payload_new_stg= ''voided_15'' THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''transferred''
                                                                                                AND             cc.payload_new_stg= ''transferred_11''THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''transferred''
                                                                                                AND             cc.payload_new_stg= ''transferred_13'' THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg ='' transferred''
                                                                                                AND             cc.payload_new_stg=''cleared_13'' THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                AND             cc.payload_new_stg=''recoded_11'' THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                AND             cc.payload_new_stg = ''recoded_14'' THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                AND             cc.payload_new_stg=''issued_14'' THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                AND             cc.payload_new_stg= ''cleared_14'' THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                AND             cc.payload_new_stg= ''requested_14'' THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                AND             cc.payload_new_stg= ''voided_14'' THEN ''N''
                                                                                                ELSE ''Y''
                                                                                END AS eligible
                                                                FROM            db_t_prod_stag.cc_transaction a
                                                                join            db_t_prod_stag.cctl_transactionstatus
                                                                ON              a.status_stg= db_t_prod_stag.cctl_transactionstatus . id_stg
                                                                join
                                                                                (
                                                                                           SELECT     cc_claim.*
                                                                                           FROM       db_t_prod_stag.cc_claim
                                                                                           inner join db_t_prod_stag.cctl_claimstate
                                                                                           ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                           WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                ON              cc_claim.id_stg=a.claimid_stg
                                                                join            db_t_prod_stag.cc_policy
                                                                ON              cc_claim.policyid_stg=cc_policy.id_stg
                                                                join            db_t_prod_stag.cctl_transaction tl
                                                                ON              tl.id_stg=a.subtype_stg
                                                                left join       db_t_prod_stag.gl_eventstaging_cc cc
                                                                ON              cc.publicid_stg=a.publicid_stg
                                                                left join       db_t_prod_stag.pc_policyperiod pp
                                                                ON              pp.id_stg=cc_policy.policysystemperiodid_stg
                                                                left outer join db_t_prod_stag.cc_check
                                                                ON              cc_check.id_stg = a.checkid_stg
                                                                WHERE           ((
                                                                                                                a.updatetime_stg >( $start_dttm)
                                                                                                AND             a.updatetime_stg <= ( $end_dttm))
                                                                                OR              (
                                                                                                                cc_check.updatetime_stg >( $start_dttm)
                                                                                                AND             cc_check.updatetime_stg <= ( $end_dttm)))
                                                                AND             tl.typecode_stg=''Payment'') AS a
                                         WHERE  a.eligible=''Y''
                                         UNION
                                         /*************************Claim Recovery Event*******************/
                                         SELECT ev_act_type_code,
                                                cast( NULL AS VARCHAR(50)) AS agmt_ev_type_cd,
                                                ev_strt_dt,
                                                ev_end_dt,
                                                key1,
                                                SUBTYPE,
                                                cast(ltrim(rtrim(agmt_host_id)) AS VARCHAR(100)) AS agmt_host_id,
                                                agmt_type,
                                                reason,
                                                agmt_evnt_dttm,
                                                src_sys,
                                                trm_nbr,
                                                retired,
                                                bill_payment_src,
                                                transactionnumber,
                                                ev_ctgy_type,
                                                func_cd
                                         FROM  (
                                                                SELECT DISTINCT ''EV_ACTVY_TYPE23''                   AS ev_act_type_code,
                                                                                cast( NULL AS VARCHAR(50))          AS agmt_ev_type_cd,
                                                                                cast(a.createtime_stg AS timestamp) AS ev_strt_dt,
                                                                                cast(NULL AS timestamp)             AS ev_end_dt,
                                                                                cast(a.id_stg AS            VARCHAR(60))       AS key1,
                                                                                cast($ev_sbtype2_financl AS VARCHAR(50))       AS SUBTYPE ,
                                                                                cast(
                                                                                CASE
                                                                                                WHEN cast (cc_policy.verified_stg AS VARCHAR (20))=''0'' THEN cc_policy.id_stg
                                                                                                                /* Added as part of EIM-33951 */
                                                                                                WHEN cast (cc_policy.verified_stg AS VARCHAR (20)) <>''0'' THEN pp.publicid_stg
                                                                                END AS        VARCHAR(100)) AS agmt_host_id ,
                                                                                cast(''PPV'' AS VARCHAR(50))  AS agmt_type ,
                                                                                ''''                          AS reason ,
                                                                                cast(NULL AS timestamp)     AS agmt_evnt_dttm ,
                                                                                $src_sys6_gwcc              AS src_sys ,
                                                                                cast(NULL AS VARCHAR(50))   AS trm_nbr ,
                                                                                a.retired_stg               AS retired ,
                                                                                cast('''' AS                    VARCHAR(255))    AS bill_payment_src ,
                                                                                cast(NULL AS                  VARCHAR(255))    AS transactionnumber ,
                                                                                cast($ctgy_type1_gnrltrans AS VARCHAR(50))     AS ev_ctgy_type ,
                                                                                cast($func_type2_clm AS       VARCHAR(50))     AS func_cd ,
                                                                                CASE
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''voided''
                                                                                                AND             cc.payload_new_stg=''voided_11'' THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''voided''
                                                                                                AND             cc.payload_new_stg= ''voided_15'' THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''transferred''
                                                                                                AND             cc.payload_new_stg= ''transferred_11''THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''transferred''
                                                                                                AND             cc.payload_new_stg= ''transferred_13'' THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg ='' transferred''
                                                                                                AND             cc.payload_new_stg=''cleared_13'' THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                AND             cc.payload_new_stg=''recoded_11'' THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                AND             cc.payload_new_stg = ''recoded_14'' THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                AND             cc.payload_new_stg=''issued_14'' THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                AND             cc.payload_new_stg= ''cleared_14'' THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                AND             cc.payload_new_stg= ''requested_14'' THEN ''N''
                                                                                                WHEN cctl_transactionstatus.typecode_stg = ''recoded''
                                                                                                AND             cc.payload_new_stg= ''voided_14'' THEN ''N''
                                                                                                ELSE ''Y''
                                                                                END AS eligible
                                                                FROM            db_t_prod_stag.cc_transaction a
                                                                join            db_t_prod_stag.cctl_transactionstatus
                                                                ON              a.status_stg= db_t_prod_stag.cctl_transactionstatus . id_stg
                                                                join
                                                                                (
                                                                                           SELECT     cc_claim.*
                                                                                           FROM       db_t_prod_stag.cc_claim
                                                                                           inner join db_t_prod_stag.cctl_claimstate
                                                                                           ON         cc_claim.state_stg= cctl_claimstate.id_stg
                                                                                           WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                ON              cc_claim.id_stg=a.claimid_stg
                                                                join            db_t_prod_stag.cc_policy
                                                                ON              cc_claim.policyid_stg=cc_policy.id_stg
                                                                join            db_t_prod_stag.cctl_transaction tl
                                                                ON              tl.id_stg=a.subtype_stg
                                                                left join       db_t_prod_stag.gl_eventstaging_cc cc
                                                                ON              cc.publicid_stg=a.publicid_stg
                                                                left join       db_t_prod_stag.pc_policyperiod pp
                                                                ON              pp.id_stg=cc_policy.policysystemperiodid_stg
                                                                left outer join db_t_prod_stag.cc_check
                                                                ON              cc_check.id_stg = a.checkid_stg
                                                                WHERE           ((
                                                                                                                a.updatetime_stg >( $start_dttm)
                                                                                                AND             a.updatetime_stg <= ( $end_dttm))
                                                                                OR              (
                                                                                                                cc_check.updatetime_stg >( $start_dttm)
                                                                                                AND             cc_check.updatetime_stg <= ( $end_dttm)))
                                                                AND             tl.typecode_stg=''Recovery''
                                                                AND             a.checknum_alfa_stg IS NOT NULL) AS a
                                         WHERE  a.eligible=''Y'' ), ev_intrm5 AS
                                  (
                                                  SELECT DISTINCT bctl_transaction.typecode_stg                    AS ev_act_type_code,
                                                                  bctl_billinginstruction.typecode_stg             AS agmt_ev_type_cd,
                                                                  cast(bc_transaction.createtime_stg AS timestamp) AS ev_strt_dt,
                                                                  cast(NULL AS timestamp)                          AS ev_end_dt,
                                                                  cast (bc_transaction.id_stg AS VARCHAR (50))     AS key1 ,
                                                                  cast($ev_sbtype2_financl AS    VARCHAR(50))      AS SUBTYPE,
                                                                  cast(
                                                                  CASE
                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''PolTAcctContainer'' THEN bc_policyperiod.policynumber_stg
                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''AcctTAcctContainer'' THEN bc_account.accountnumber_stg
                                                                                  ELSE ''''
                                                                  END AS VARCHAR(100)) AS agmt_host_id ,
                                                                  CASE
                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''PolTAcctContainer'' THEN cast(''POLTRM'' AS VARCHAR(50))
                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''AcctTAcctContainer'' THEN cast(''ACT'' AS   VARCHAR(50))
                                                                                  ELSE NULL
                                                                  END                     AS agmt_type ,
                                                                  ''''                      AS reason ,
                                                                  cast(NULL AS timestamp) AS agmt_evnt_dttm ,
                                                                  $src_sys5_gwbc          AS src_sys ,
                                                                  cast(
                                                                  CASE
                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''PolTAcctContainer'' THEN bc_policyperiod.termnumber_stg
                                                                                  ELSE NULL
                                                                  END AS VARCHAR(50))        AS trm_nbr,
                                                                  bc_transaction.retired_stg AS retired ,
                                                                  cast('''' AS VARCHAR(255))   AS bill_payment_src ,
                                                                  bc_transaction.transactionnumber_stg,
                                                                  cast($ctgy_type1_gnrltrans AS VARCHAR(50)) AS ev_ctgy_type ,
                                                                  cast($func_type1_bill AS      VARCHAR(50)) AS func_cd
                                                  FROM            db_t_prod_stag.bc_transaction
                                                  inner join      db_t_prod_stag.bctl_transaction
                                                  ON              bctl_transaction.id_stg=bc_transaction.subtype_stg
                                                  left outer join db_t_prod_stag.bc_lineitem
                                                  ON              bc_lineitem.transactionid_stg=bc_transaction.id_stg
                                                  left outer join db_t_prod_stag.bc_taccount
                                                  ON              bc_taccount.id_stg=bc_lineitem.taccountid_stg
                                                  left outer join db_t_prod_stag.bctl_taccount
                                                  ON              bctl_taccount.id_stg=bc_taccount.subtype_stg
                                                  left outer join db_t_prod_stag.bc_taccountcontainer
                                                  ON              bc_taccountcontainer.id_stg=bc_taccount.taccountcontainerid_stg
                                                  left outer join db_t_prod_stag.bctl_taccountcontainer
                                                  ON              bctl_taccountcontainer.id_stg=bc_taccountcontainer.subtype_stg
                                                  left join       db_t_prod_stag.bc_account
                                                  ON              bc_taccountcontainer.id_stg = bc_account.hiddentaccountcontainerid_stg
                                                  left join       db_t_prod_stag.bc_policyperiod
                                                  ON              bc_taccountcontainer.id_stg = bc_policyperiod.hiddentaccountcontainerid_stg
                                                  left join       db_t_prod_stag.bc_chargeinstancecontext
                                                  ON              bc_chargeinstancecontext.transactionid_stg = bc_transaction.id_stg
                                                  left join       db_t_prod_stag.bc_charge
                                                  ON              bc_charge.id_stg = bc_chargeinstancecontext.chargeid_stg
                                                  left join       db_t_prod_stag.bc_billinginstruction
                                                  ON              bc_billinginstruction.id_stg = bc_charge.billinginstructionid_stg
                                                  left join       db_t_prod_stag.bctl_billinginstruction
                                                  ON              bctl_billinginstruction.id_stg = bc_billinginstruction.subtype_stg
                                                  WHERE           ((
                                                                                                  bc_transaction.updatetime_stg > ( $start_dttm)
                                                                                  AND             bc_transaction.updatetime_stg <= ( $end_dttm))
                                                                  OR              (
                                                                                                  bc_policyperiod.updatetime_stg > ( $start_dttm)
                                                                                  AND             bc_policyperiod.updatetime_stg <= ( $end_dttm)))
                                                  UNION
                                                  SELECT DISTINCT ''rvrs''
                                                                                  || ''-''
                                                                                  || bctl_transaction.typecode_stg AS ev_act_type_code,
                                                                  bctl_billinginstruction.typecode_stg             AS agmt_ev_type_cd,
                                                                  cast(bc_transaction.createtime_stg AS timestamp) AS ev_strt_dt ,
                                                                  cast(NULL AS timestamp)                          AS ev_end_dt ,
                                                                  cast (bc_transaction.id_stg AS VARCHAR (50))     AS key1 ,
                                                                  cast($ev_sbtype2_financl AS    VARCHAR(50))      AS SUBTYPE ,
                                                                  cast(
                                                                  CASE
                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''PolTAcctContainer'' THEN bc_policyperiod.policynumber_stg
                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''AcctTAcctContainer'' THEN bc_account.accountnumber_stg
                                                                                  ELSE ''''
                                                                  END AS VARCHAR(100)) AS agmt_host_id ,
                                                                  CASE
                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''PolTAcctContainer'' THEN ''POLTRM''
                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''AcctTAcctContainer'' THEN ''ACT''
                                                                                  ELSE NULL
                                                                  END                     AS agmt_type ,
                                                                  ''''                      AS reason ,
                                                                  cast(NULL AS timestamp) AS agmt_evnt_dttm ,
                                                                  $src_sys5_gwbc          AS src_sys ,
                                                                  cast(
                                                                  CASE
                                                                                  WHEN bctl_taccountcontainer.typecode_stg=''PolTAcctContainer'' THEN bc_policyperiod.termnumber_stg
                                                                                  ELSE NULL
                                                                  END AS VARCHAR(50))        AS trm_nbr,
                                                                  bc_transaction.retired_stg AS retired ,
                                                                  cast('''' AS VARCHAR(255))   AS bill_payment_src ,
                                                                  bc_transaction.transactionnumber_stg,
                                                                  cast($ctgy_type1_gnrltrans AS VARCHAR(50)) AS ev_ctgy_type ,
                                                                  cast($func_type1_bill AS      VARCHAR(50)) AS func_cd
                                                  FROM            db_t_prod_stag.bc_transaction
                                                  inner join      db_t_prod_stag.bctl_transaction
                                                  ON              bctl_transaction.id_stg=bc_transaction.subtype_stg
                                                  left outer join db_t_prod_stag.bc_lineitem
                                                  ON              bc_lineitem.transactionid_stg=bc_transaction.id_stg
                                                  left outer join db_t_prod_stag.bc_taccount
                                                  ON              bc_taccount.id_stg=bc_lineitem.taccountid_stg
                                                  left outer join db_t_prod_stag.bctl_taccount
                                                  ON              bctl_taccount.id_stg=bc_taccount.subtype_stg
                                                  left outer join db_t_prod_stag.bc_taccountcontainer
                                                  ON              bc_taccountcontainer.id_stg=bc_taccount.taccountcontainerid_stg
                                                  left outer join db_t_prod_stag.bctl_taccountcontainer
                                                  ON              bctl_taccountcontainer.id_stg=bc_taccountcontainer.subtype_stg
                                                  left join       db_t_prod_stag.bc_account
                                                  ON              bc_taccountcontainer.id_stg = bc_account.hiddentaccountcontainerid_stg
                                                  left join       db_t_prod_stag.bc_policyperiod
                                                  ON              bc_taccountcontainer.id_stg = bc_policyperiod.hiddentaccountcontainerid_stg
                                                  left join       db_t_prod_stag.bc_revtrans
                                                  ON              bc_transaction.id_stg = bc_revtrans.ownerid_stg
                                                  left join       db_t_prod_stag.bc_chargeinstancecontext
                                                  ON              bc_chargeinstancecontext.transactionid_stg = bc_transaction.id_stg
                                                  left join       db_t_prod_stag.bc_charge
                                                  ON              bc_charge.id_stg = bc_chargeinstancecontext.chargeid_stg
                                                  left join       db_t_prod_stag.bc_billinginstruction
                                                  ON              bc_billinginstruction.id_stg = bc_charge.billinginstructionid_stg
                                                  left join       db_t_prod_stag.bctl_billinginstruction
                                                  ON              bctl_billinginstruction.id_stg = bc_billinginstruction.subtype_stg
                                                  WHERE           bc_revtrans.ownerid_stg IS NOT NULL
                                                  AND             ((
                                                                                                  bc_transaction.updatetime_stg > ( $start_dttm)
                                                                                  AND             bc_transaction.updatetime_stg <= ( $end_dttm))
                                                                  OR              (
                                                                                                  bc_policyperiod.updatetime_stg > ( $start_dttm)
                                                                                  AND             bc_policyperiod.updatetime_stg <= ( $end_dttm))))
                                  /* ----------------------> cc <--------------------------- */
                                  
                         SELECT          cast(xlat_src.ev_strt_dt AS timestamp) ev_strt_dt,
                                         xlat_src.out_agmt_ev_type_cd,
                                         CASE
                                                         WHEN (
                                                                                         xlat_src.ev_end_dt =''9999-12-31 23:59:59.999999'') THEN xlat_src.ev_end_dt
                                                         ELSE cast(substr(cast(xlat_src.ev_end_dt AS VARCHAR(50)),1,20)
                                                                                         ||''000000'' AS timestamp)
                                         END                              ev_end_dt,
                                         cast(xlat_src.retired AS INTEGER)retired,
                                         xlat_src.transactionnumber,
                                         xlat_src.rnk,
                                         CASE
                                                         WHEN xlat_src.out_ev_actvy_type_cd IN (''RVRSDWRTOFFPMT'',
                                                                                                ''WRTOFFPMT'')
                                                         AND             xlat_src.reason=''UNK'' THEN NULL
                                                         ELSE xlat_src.reason
                                         END           AS ev_rsn_cd,
                                         xlat_src.key1 AS src_trans_id,
                                         xlat_src.out_ev_sbtype_cd,
                                         xlat_src.out_ev_actvy_type_cd,
                                         CASE
                                                         WHEN (
                                                                                         xlat_src.prcsd_src_sys_cd IN ('''',
                                                                                                                       ''UNK'') )THEN NULL
                                                         ELSE xlat_src.prcsd_src_sys_cd
                                         END prcsd_src_sys_cd,
                                         xlat_src.out_ev_ctgy_type,
                                         xlat_src.func_cd,
                                         xlat_src.src_sys_cd,
                                         cast(xlat_src.agmt_id AS INTEGER)agmt_id,
                                         cast(substr(cast(xlat_src.agmt_ev_dttm AS VARCHAR(50)),1,20)
                                                         ||''000000'' AS timestamp)   agmt_ev_dttm,
                                         cast(xlat_src.xref_ev_id AS INTEGER)    AS xref_ev_id,
                                         cast( tgt_ev.ev_id AS       INTEGER)    AS lkp_ev_id,
                                         tgt_ev.agmt_ev_type_cd                  AS lkp_agmt_ev_type_cd,
                                         tgt_ev.ev_strt_dttm                     AS lkp_ev_strt_dttm,
                                         tgt_ev.ev_end_dttm                      AS lkp_ev_end_dttm,
                                         tgt_ev.ev_rsn_cd                        AS lkp_ev_rsn_cd,
                                         tgt_ev.ev_num                           AS lkp_ev_num,
                                         cast(tgt_ev.agmt_id AS INTEGER)         AS lkp_agmt_id,
                                         tgt_ev.prcsd_src_sys_cd                 AS lkp_prcsd_src_sys_cd,
                                         tgt_ev.func_cd1                         AS lkp_func_cd,
                                         tgt_ev.ev_ctgy_type_cd                  AS lkp_ev_ctgy_type_cd,
                                         tgt_ev.ev_dttm                          AS lkp_ev_dttm,
                                         tgt_ev.edw_end_dttm                     AS lkp_edw_end_dttm,
                                         tgt_ev.edw_strt_dttm                    AS lkp_edw_strt_dttm_upd,
                                         cast(tgt_ev.aplctn_id AS   INTEGER)       AS lkp_aplctn_id,
                                         cast(xlat_src.aplctn_id AS INTEGER),
                                         (cast(cast(xlat_src.ev_strt_dt AS                 DATE)AS VARCHAR(50))
                                                         ||cast(cast(xlat_src.ev_end_dt AS DATE)AS VARCHAR(50))
                                                         || coalesce(
                                         CASE
                                                         WHEN rtrim(ltrim(xlat_src.reason))='''' THEN ''UNK''
                                                         ELSE rtrim(ltrim(xlat_src.reason))
                                         END,''UNK'')
                                                         ||cast(coalesce(xlat_src.agmt_id,-1) AS VARCHAR(50))
                                                         || cast(coalesce(
                                         CASE
                                                         WHEN rtrim(ltrim(xlat_src.prcsd_src_sys_cd))='''' THEN ''UNK''
                                                         ELSE rtrim(ltrim(xlat_src.prcsd_src_sys_cd))
                                         END,''UNK'') AS                                                             VARCHAR(50))
                                                         ||cast(coalesce(ltrim(ltrim(xlat_src.func_cd)), ''UNK'') AS VARCHAR(50))
                                                         ||cast(cast(xlat_src.agmt_ev_dttm AS                         DATE)AS VARCHAR(50))
                                                         || coalesce(cast(rtrim(ltrim(xlat_src.transactionnumber)) AS VARCHAR(50)), ''UNK'')
                                                         || coalesce(cast(xlat_src.aplctn_id AS                       VARCHAR(50)), ''UNK'')
                                                         || coalesce(cast(xlat_src.out_ev_ctgy_type AS                VARCHAR(50)), ''UNK'')
                                                         || coalesce(cast(xlat_src.out_agmt_ev_type_cd AS             VARCHAR(50)),''UNK''))AS md5_src,
                                         cast(coalesce(cast(tgt_ev.ev_strt_dttm AS                                    DATE) , cast(''1900-01-01'' AS DATE)) AS VARCHAR(50))
                                                         ||cast(coalesce(cast(tgt_ev.ev_end_dttm AS                   DATE) , cast(''9999-12-31'' AS DATE)) AS VARCHAR(50))
                                                         ||coalesce(rtrim(ltrim(tgt_ev.ev_rsn_cd)), ''UNK'')
                                                         ||cast(coalesce(tgt_ev.agmt_id,-1) AS                             VARCHAR(50))
                                                         || cast(coalesce(rtrim(ltrim(tgt_ev.prcsd_src_sys_cd)), ''UNK'') AS VARCHAR(50))
                                                         ||coalesce(cast(ltrim(ltrim(tgt_ev.func_cd1)) AS VARCHAR(50)), ''UNK'')
                                                         ||cast(coalesce(cast(tgt_ev.ev_dttm AS DATE),cast(''1900-01-01'' AS DATE))AS VARCHAR(50))
                                                         ||coalesce(cast(rtrim(ltrim(tgt_ev.ev_num)) AS VARCHAR(50)), ''UNK'')
                                                         || coalesce(cast(tgt_ev.aplctn_id AS           VARCHAR(50)),''UNK'')
                                                         ||coalesce(cast(tgt_ev.ev_ctgy_type_cd AS      VARCHAR(50)), ''UNK'')
                                                         || coalesce(cast(tgt_ev.agmt_ev_type_cd AS     VARCHAR(50)),''UNK'') AS md5_tgt,
                                         CASE
                                                         WHEN tgt_ev.ev_id IS NULL THEN ''I''
                                                         WHEN md5_src<>md5_tgt THEN ''U''
                                                         ELSE ''R''
                                         END out_ins_upd
                         FROM            (
                                                         SELECT DISTINCT coalesce (src.SUBTYPE,''UNK'')            AS out_ev_sbtype_cd,
                                                                         coalesce(blngins.tgt_idntftn_val,''UNK'') AS out_agmt_ev_type_cd,
                                                                         coalesce (actye.tgt_idntftn_val,''UNK'')  AS out_ev_actvy_type_cd,
                                                                         coalesce(srccd.tgt_idntftn_val,''UNK'')   AS prcsd_src_sys_cd,
                                                                         coalesce (src.ev_ctgy_type, ''UNK'')      AS out_ev_ctgy_type,
                                                                         coalesce (src.func_cd, ''UNK'')           AS func_cd1,
                                                                         coalesce (src_sys,''UNK'') AS src_sys_cd,
                                                                         coalesce (xref.ev_id,0)                 AS xref_ev_id,
                                                                         aplc.aplctn_id                          AS aplctn_id,
                                                                         src.ev_act_type_code,
                                                                         coalesce(src.ev_strt_dt, cast(''1900-01-01 00:00:00.000000'' AS timestamp))ev_strt_dt,
                                                                         coalesce(src.ev_end_dt, cast(''9999-12-31 23:59:59.999999''AS timestamp))  ev_end_dt,
                                                                         src.key1,
                                                                         src.SUBTYPE,
                                                                         src.agmt_host_id,
                                                                         agmt_type,
                                                                         rsncd.tgt_idntftn_val reason,
                                                                         src.agmt_evnt_dttm,
                                                                         src_sys,
                                                                         src.trm_nbr,
                                                                         src.retired,
                                                                         src.bill_payment_src,
                                                                         src.transactionnumber ,
                                                                         src.ev_ctgy_type,
                                                                         func_cd,
                                                                         rank() over( PARTITION BY src.ev_act_type_code,src.key1,src.SUBTYPE ORDER BY src.ev_strt_dt ) AS rnk,
                                                                         CASE
                                                                                         WHEN agmt_type=''ACT'' THEN lkp_agmt_inv_act_pol.agmt_id
                                                                                         WHEN agmt_type=''INV'' THEN lkp_agmt_inv_act_pol.agmt_id
                                                                                         WHEN agmt_type=''PPV'' THEN lkp_agmt_ppv.agmt_id
                                                                                         WHEN agmt_type=''POLTRM'' THEN lkp_agmt_poltrm.agmt_id
                                                                                         WHEN agmt_type=''POL'' THEN lkp_agmt_inv_act_pol.agmt_id
                                                                                         ELSE -1
                                                                         END                      AS agmt_id,
                                                                         agmt_type                   agmt_type_new,
                                                                         lkp_agmt_ppv.agmt_id        aggmt_id_ppv,
                                                                         lkp_agmt_poltrm.agmt_id     agmt_poltrm,
                                                                         CASE
                                                                                         WHEN src.agmt_evnt_dttm IS NULL THEN cast(''1900-01-01 00:00:00.000000''AS timestamp)
                                                                                         ELSE src.agmt_evnt_dttm
                                                                         END AS agmt_ev_dttm
                                                         FROM            (
                                                                                         SELECT DISTINCT ev_act_type_code,
                                                                                                         agmt_ev_type_cd,
                                                                                                         ev_strt_dt,
                                                                                                         ev_end_dt,
                                                                                                         key1,
                                                                                                         SUBTYPE,
                                                                                                         agmt_host_id,
                                                                                                         agmt_type,
                                                                                                         reason,
                                                                                                         agmt_evnt_dttm,
                                                                                                         src_sys,
                                                                                                         trm_nbr,
                                                                                                         retired,
                                                                                                         bill_payment_src,
                                                                                                         transactionnumber,
                                                                                                         ev_ctgy_type,
                                                                                                         func_cd,
                                                                                                         rank() over( PARTITION BY ev_act_type_code,key1,SUBTYPE ORDER BY ev_strt_dt ) AS rnk
                                                                                         FROM            (
                                                                                                                SELECT *
                                                                                                                FROM   ev_intrm1
                                                                                                                UNION ALL
                                                                                                                SELECT *
                                                                                                                FROM   ev_intrm2
                                                                                                                UNION ALL
                                                                                                                SELECT *
                                                                                                                FROM   ev_intrm3
                                                                                                                UNION ALL
                                                                                                                SELECT *
                                                                                                                FROM   ev_intrm4
                                                                                                                UNION ALL
                                                                                                                SELECT *
                                                                                                                FROM   ev_intrm5) temp qualify row_number() over ( PARTITION BY ev_act_type_code,key1,SUBTYPE ORDER BY ev_strt_dt DESC,agmt_host_id DESC,agmt_type DESC,reason DESC, agmt_evnt_dttm DESC,trm_nbr DESC,retired DESC,bill_payment_src DESC, transactionnumber DESC )=1 ) AS src
                                                         left outer join db_t_prod_core.teradata_etl_ref_xlat rsncd
                                                         ON              rsncd.src_idntftn_val=src.reason
                                                         AND             rsncd.tgt_idntftn_nm= ''EV_RSN''
                                                         AND             rsncd.src_idntftn_nm IN (''bctl_paymentreversalreason.typecode'',
                                                                                                  ''bctl_reason.typecode'',
                                                                                                  ''bctl_writeoffreason.typecode'',
                                                                                                  ''bctl_writeoffreversalreason.typecode'')
                                                         AND             rsncd.src_idntftn_sys=''GW''
                                                         AND             rsncd.expn_dt=''9999-12-31''
                                                         left outer join db_t_prod_core.teradata_etl_ref_xlat actye
                                                         ON              actye.src_idntftn_val=src.ev_act_type_code
                                                         AND             actye.tgt_idntftn_nm= ''EV_ACTVY_TYPE''
                                                         AND             actye.src_idntftn_sys IN (''GW'',
                                                                                                   ''DS'')
                                                         AND             actye.expn_dt=''9999-12-31''
                                                         left outer join db_t_prod_core.teradata_etl_ref_xlat aptyp
                                                         ON              aptyp.src_idntftn_val=src.ev_act_type_code
                                                         AND             aptyp.tgt_idntftn_nm= ''APLCTN_TYPE''
                                                         AND             aptyp.src_idntftn_nm= ''pctl_job.Typecode''
                                                         AND             aptyp.src_idntftn_sys= ''GW''
                                                         AND             aptyp.expn_dt=''9999-12-31''
                                                         left outer join db_t_prod_core.teradata_etl_ref_xlat blngins
                                                         ON              blngins.src_idntftn_val=src.agmt_ev_type_cd
                                                         AND             blngins.tgt_idntftn_nm= ''AGMT_EV_TYPE''
                                                         AND             blngins.src_idntftn_nm= ''bctl_billinginstruction.typecode''
                                                         AND             blngins.src_idntftn_sys IN (''GW'')
                                                         AND             blngins.expn_dt=''9999-12-31''
                                                         left outer join db_t_prod_core.teradata_etl_ref_xlat AS srccd
                                                         ON              srccd.src_idntftn_val=src.bill_payment_src
                                                         AND             srccd.tgt_idntftn_nm IN (''AGMT_STS_SRC_TYPE'',
                                                                                                  ''SRC_SYS'')
                                                         AND             srccd.src_idntftn_nm IN (''pctl_cancellationsource.typecode'',
                                                                                                  ''bctl_paymentsource_alfa.typecode'')
                                                         AND             srccd.src_idntftn_sys=''GW''
                                                         AND             srccd.expn_dt=''9999-12-31''
                                                         join
                                                                         (
                                                                                  SELECT   ev_id,
                                                                                           src_trans_id,
                                                                                           ev_sbtype_cd,
                                                                                           ev_actvy_type_cd
                                                                                  FROM     db_t_prod_core.dir_ev qualify row_number() over( PARTITION BY src_trans_id, ev_sbtype_cd,ev_actvy_type_cd ORDER BY load_dttm ASC)=1) AS xref
                                                         ON              xref.src_trans_id=src.key1
                                                         AND             xref.ev_sbtype_cd=src.SUBTYPE
                                                         AND             xref.ev_actvy_type_cd=actye.tgt_idntftn_val
                                                         left outer join
                                                                         (
                                                                                  SELECT   aplctn.aplctn_id      AS aplctn_id,
                                                                                           aplctn.host_aplctn_id AS host_aplctn_id,
                                                                                           aplctn.aplctn_type_cd AS aplctn_type_cd,
                                                                                           aplctn.src_sys_cd     AS src_sys_cd
                                                                                  FROM     db_t_prod_core.aplctn  qualify row_number () over ( PARTITION BY host_aplctn_id,src_sys_cd ORDER BY edw_end_dttm DESC)=1
                                                                                           /*WHERE EDW_END_DTTM =''9999-12-31 23:59:59.999999''*/
                                                                         ) AS aplc
                                                         ON              aplc.aplctn_type_cd=aptyp.tgt_idntftn_val
                                                         AND             aplc.host_aplctn_id=src.key1
                                                         AND             aplc.src_sys_cd=src_sys
                                                         left outer join
                                                                         (
                                                                                SELECT agmt.agmt_id       AS agmt_id,
                                                                                       agmt.host_agmt_num AS host_agmt_num,
                                                                                       agmt.term_num      AS term_num,
                                                                                       agmt.nk_src_key    AS nk_src_key,
                                                                                       agmt.agmt_type_cd  AS agmt_type_cd,
                                                                                       agmt.edw_end_dttm
                                                                                FROM   db_t_prod_core.agmt 
                                                                                WHERE  agmt_type_cd=''POLTRM''
                                                                                AND    cast(edw_end_dttm AS DATE)=''9999-12-31''
                                                                                AND    host_agmt_num IN
                                                                                                         (
                                                                                                         SELECT DISTINCT agmt_host_id
                                                                                                         FROM            ev_intrm2
                                                                                                         WHERE           agmt_host_id IS NOT NULL
                                                                                                         OR              agmt_host_id<>''''
                                                                                                         UNION
                                                                                                         SELECT DISTINCT agmt_host_id
                                                                                                         FROM            ev_intrm5
                                                                                                         WHERE           agmt_host_id IS NOT NULL
                                                                                                         OR              agmt_host_id<>'''' ) )lkp_agmt_poltrm
                                                         ON              lkp_agmt_poltrm.host_agmt_num=src.agmt_host_id
                                                         AND             lkp_agmt_poltrm.term_num=src.trm_nbr
                                                         AND             lkp_agmt_poltrm.agmt_type_cd=agmt_type
                                                         left outer join
                                                                         (
                                                                                  SELECT   agmt.agmt_id       AS agmt_id,
                                                                                           agmt.host_agmt_num AS host_agmt_num,
                                                                                           agmt.nk_src_key    AS nk_src_key,
                                                                                           agmt.agmt_type_cd  AS agmt_type_cd,
                                                                                           agmt.edw_end_dttm,
                                                                                           src_sys_cd
                                                                                  FROM     db_t_prod_core.agmt 
                                                                                  WHERE    agmt_type_cd IN (''PPV'')
                                                                                  AND      nk_src_key   IN
                                                                                                            (
                                                                                                            SELECT DISTINCT agmt_host_id
                                                                                                            FROM            ev_intrm4
                                                                                                            WHERE           agmt_host_id IS NOT NULL
                                                                                                            OR              agmt_host_id<>'''' ) qualify row_number() over( PARTITION BY agmt.nk_src_key,agmt_type_cd ORDER BY agmt.edw_end_dttm DESC) = 1 ) lkp_agmt_ppv
                                                         ON              lkp_agmt_ppv.nk_src_key=src.agmt_host_id
                                                         left outer join
                                                                         (
                                                                                  SELECT   agmt.agmt_id       AS agmt_id,
                                                                                           agmt.host_agmt_num AS host_agmt_num,
                                                                                           agmt.nk_src_key    AS nk_src_key,
                                                                                           agmt.agmt_type_cd  AS agmt_type_cd,
                                                                                           agmt.edw_end_dttm,
                                                                                           src_sys_cd
                                                                                  FROM     db_t_prod_core.agmt 
                                                                                  WHERE    agmt_type_cd IN (''INV'',
                                                                                                            ''ACT'' ,
                                                                                                            ''POL'')
                                                                                  AND      nk_src_key IN
                                                                                                          (
                                                                                                          SELECT DISTINCT agmt_host_id
                                                                                                          FROM            ev_intrm1
                                                                                                          WHERE           agmt_host_id IS NOT NULL
                                                                                                          OR              agmt_host_id<>''''
                                                                                                          UNION
                                                                                                          SELECT DISTINCT agmt_host_id
                                                                                                          FROM            ev_intrm5
                                                                                                          WHERE           agmt_host_id IS NOT NULL ) qualify row_number() over( PARTITION BY agmt.nk_src_key,agmt_type_cd ORDER BY agmt.edw_end_dttm DESC) = 1 ) lkp_agmt_inv_act_pol
                                                         ON              lkp_agmt_inv_act_pol.nk_src_key=src.agmt_host_id
                                                         AND             agmt_type IN (''INV'',
                                                                                                      ''ACT'' ,
                                                                                                      ''POL'')
                                                         AND             lkp_agmt_inv_act_pol.agmt_type_cd=agmt_type) xlat_src
                         left outer join
                                         (
                                                  SELECT   ev.ev_id            AS ev_id,
                                                           ev.ev_strt_dttm     AS ev_strt_dttm,
                                                           ev.ev_end_dttm      AS ev_end_dttm,
                                                           ev.ev_rsn_cd        AS ev_rsn_cd,
                                                           ev.ev_num           AS ev_num,
                                                           ev.agmt_id          AS agmt_id,
                                                           ev.prcsd_src_sys_cd AS prcsd_src_sys_cd,
                                                           ev.func_cd          AS func_cd1,
                                                           ev.ev_ctgy_type_cd  AS ev_ctgy_type_cd,
                                                           ev.ev_dttm          AS ev_dttm,
                                                           ev.edw_strt_dttm    AS edw_strt_dttm,
                                                           ev.edw_end_dttm     AS edw_end_dttm,
                                                           ev.aplctn_id        AS aplctn_id,
                                                           ev.ev_sbtype_cd     AS ev_sbtype_cd,
                                                           ev.agmt_ev_type_cd  AS agmt_ev_type_cd,
                                                           ev.ev_actvy_type_cd AS ev_actvy_type_cd,
                                                           ev.src_trans_id     AS src_trans_id
                                                  FROM     db_t_prod_core.ev 
                                                  WHERE    src_trans_id IN
                                                           (
                                                                  SELECT key1
                                                                  FROM   ev_intrm1
                                                                  UNION
                                                                  SELECT key1
                                                                  FROM   ev_intrm2
                                                                  UNION
                                                                  SELECT key1
                                                                  FROM   ev_intrm3
                                                                  UNION
                                                                  SELECT key1
                                                                  FROM   ev_intrm4
                                                                  UNION
                                                                  SELECT key1
                                                                  FROM   ev_intrm5) qualify row_number() over( PARTITION BY ev.ev_sbtype_cd,ev.ev_actvy_type_cd,ev.src_trans_id ORDER BY ev.edw_end_dttm DESC) = 1 ) AS tgt_ev
                         ON              xlat_src.out_ev_sbtype_cd=tgt_ev.ev_sbtype_cd
                         AND             xlat_src.out_ev_actvy_type_cd=tgt_ev.ev_actvy_type_cd
                         AND             xlat_src.key1=tgt_ev.src_trans_id
                         WHERE           ((
                                                                         out_ins_upd =''I'')
                                         OR              (
                                                                         out_ins_upd =''U''
                                                         AND             xlat_src.out_ev_sbtype_cd =''PLCYTRNS'' )
                                         OR              (
                                                                         out_ins_upd =''U''
                                                         AND             xlat_src.out_ev_sbtype_cd =''CATSTRPH''
                                                         AND             ev_strt_dttm > tgt_ev.ev_strt_dttm )
                                         OR              (
                                                                         out_ins_upd =''U''
                                                         AND             xlat_src.out_ev_sbtype_cd =cast($ev_sbtype2_financl AS VARCHAR(50))
                                                         AND             xlat_src.out_ev_actvy_type_cd <> ''SUSPITM''
                                                         AND             cast(xlat_src.ev_strt_dt AS timestamp) > tgt_ev.ev_strt_dttm )
                                         OR              (
                                                                         out_ins_upd =''U''
                                                         AND             xlat_src.out_ev_sbtype_cd =cast($ev_sbtype2_financl AS VARCHAR(50))
                                                         AND             xlat_src.out_ev_actvy_type_cd = ''DSBPMT''
                                                         AND             cast(xlat_src.agmt_id AS INTEGER) <>cast(tgt_ev.agmt_id AS INTEGER))
                                         OR              (
                                                                         out_ins_upd =''U''
                                                         AND             xlat_src.out_ev_sbtype_cd =cast($ev_sbtype2_financl AS VARCHAR(50))
                                                         AND             xlat_src.out_ev_actvy_type_cd = ''SUSPITM'' )
                                         OR              (
                                                                         retired<>0
                                                         AND             out_ins_upd =''R''
                                                         AND             tgt_ev.edw_end_dttm<>cast(''9999-12-31 23:59:59.999999'' AS timestamp))) ) src ) );
  -- Component exp_ins_upd, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ins_upd AS
  (
         SELECT sq_cc_catastrophe.lkp_ev_id           AS lkp_ev_id,
                sq_cc_catastrophe.lkp_agmt_ev_type_cd AS lkp_agmt_ev_type_cd,
                sq_cc_catastrophe.aplctn_id           AS aplctn_id,
                sq_cc_catastrophe.out_agmt_ev_type_cd AS agmt_ev_type_cd,
                CASE
                       WHEN sq_cc_catastrophe.ev_strt_dt IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )
                       ELSE sq_cc_catastrophe.ev_strt_dt
                END AS ev_strt_dttm_out,
                CASE
                       WHEN sq_cc_catastrophe.ev_end_dt IS NULL THEN to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                       ELSE sq_cc_catastrophe.ev_end_dt
                END AS ev_end_dttm_out,
                ltrim ( rtrim (
                CASE
                       WHEN sq_cc_catastrophe.out_ev_actvy_type_cd IS NULL THEN ''UNK''
                       ELSE sq_cc_catastrophe.out_ev_actvy_type_cd
                END ) ) AS ev_actvy_type_cd_out,
                CASE
                       WHEN (
                                   sq_cc_catastrophe.out_ev_actvy_type_cd  IN ( 
                                         ''RVRSDWRTOFFPMT'' ,
                                         ''WRTOFFPMT'' )
                              AND    (
                                            sq_cc_catastrophe.ev_rsn_cd IS NULL
                                     OR     sq_cc_catastrophe.ev_rsn_cd = ''UNK'' ) ) THEN NULL
                       ELSE
                              CASE
                                     WHEN sq_cc_catastrophe.ev_rsn_cd IS NULL
                                     OR     ltrim ( rtrim ( sq_cc_catastrophe.ev_rsn_cd ) ) = '''' THEN ''UNK ''
                                     ELSE sq_cc_catastrophe.ev_rsn_cd
                              END
                END                            AS ev_rsn_cd_out,
                sq_cc_catastrophe.src_trans_id AS src_trans_id,
                ltrim ( rtrim (
                CASE
                       WHEN sq_cc_catastrophe.out_ev_sbtype_cd IS NULL THEN ''UNK''
                       ELSE sq_cc_catastrophe.out_ev_sbtype_cd
                END ) )                            AS ev_sbtype_cd_out,
                sq_cc_catastrophe.prcsd_src_sys_cd AS prcsd_src_sys_cd_out,
                CASE
                       WHEN sq_cc_catastrophe.agmt_id IS NULL THEN - 1
                       ELSE sq_cc_catastrophe.agmt_id
                END                                AS agmt_id1,
                sq_cc_catastrophe.out_ev_ctgy_type AS out_ev_ctgy_type,
                $prcs_id                           AS prcs_id,
                sq_cc_catastrophe.func_cd          AS func_cd,
                CASE
                       WHEN sq_cc_catastrophe.agmt_ev_dttm IS NULL THEN to_date ( ''1900-01-01'' , ''yyyy-mm-dd'' )
                       ELSE sq_cc_catastrophe.agmt_ev_dttm
                END                                 AS agmt_ev_dttm_out,
                sq_cc_catastrophe.transactionnumber AS transactionnumber,
                md5 ( to_char ( sq_cc_catastrophe.ev_strt_dt )
                       || to_char ( sq_cc_catastrophe.ev_end_dt )
                       || rtrim ( ltrim ( sq_cc_catastrophe.ev_rsn_cd ) )
                       || to_char ( sq_cc_catastrophe.agmt_id )
                       || rtrim ( ltrim ( sq_cc_catastrophe.prcsd_src_sys_cd ) )
                       || ltrim ( ltrim ( sq_cc_catastrophe.func_cd ) )
                       || to_char ( sq_cc_catastrophe.agmt_ev_dttm )
                       || rtrim ( ltrim ( sq_cc_catastrophe.transactionnumber ) )
                       || to_char ( sq_cc_catastrophe.aplctn_id )
                       || sq_cc_catastrophe.out_ev_ctgy_type ) AS calc_chksm,
                sq_cc_catastrophe.lkp_ev_strt_dttm             AS lkp_ev_strt_dttm,
                sq_cc_catastrophe.lkp_ev_end_dttm              AS lkp_ev_end_dttm,
                sq_cc_catastrophe.lkp_ev_rsn_cd                AS lkp_ev_rsn_cd,
                sq_cc_catastrophe.lkp_agmt_id                  AS lkp_agmt_id,
                sq_cc_catastrophe.lkp_prcsd_src_sys_cd         AS lkp_prcsd_src_sys_cd,
                sq_cc_catastrophe.lkp_edw_end_dttm             AS lkp_edw_end_dttm,
                md5 ( to_char ( sq_cc_catastrophe.lkp_ev_strt_dttm )
                       || to_char ( sq_cc_catastrophe.lkp_ev_end_dttm )
                       || rtrim ( ltrim ( sq_cc_catastrophe.lkp_ev_rsn_cd ) )
                       || to_char ( sq_cc_catastrophe.lkp_agmt_id )
                       || rtrim ( ltrim ( sq_cc_catastrophe.lkp_prcsd_src_sys_cd ) )
                       || rtrim ( ltrim ( sq_cc_catastrophe.lkp_func_cd ) )
                       || to_char ( sq_cc_catastrophe.lkp_ev_dttm )
                       || rtrim ( ltrim ( sq_cc_catastrophe.lkp_ev_num ) )
                       || to_char ( sq_cc_catastrophe.lkp_aplctn_id )
                       || sq_cc_catastrophe.lkp_ev_ctgy_type_cd )                      AS orig_chksm,
                NULL                                                                   AS out_edw_strt_dttm,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_edw_end_dttm_out,
                sq_cc_catastrophe.lkp_edw_strt_dttm_upd                                AS lkp_edw_strt_dttm_upd,
                sq_cc_catastrophe.retired                                              AS retired,
                sq_cc_catastrophe.src_sys_cd                                           AS src_sys_cd,
                sq_cc_catastrophe.rnk                                                  AS rnk,
                sq_cc_catastrophe.xref_ev_id                                           AS xref_ev_id,
                sq_cc_catastrophe.out_ins_upd                                          AS out_ins_upd1,
                sq_cc_catastrophe.source_record_id
         FROM   sq_cc_catastrophe );
  -- Component rtr_ev_ins_upd_INSERT, Type ROUTER Output Group INSERT
  create or replace temporary table rtr_ev_ins_upd_INSERT as
  SELECT exp_ins_upd.lkp_ev_id             AS lkp_ev_id,
         exp_ins_upd.lkp_agmt_ev_type_cd   AS lkp_agmt_ev_type_cd,
         exp_ins_upd.aplctn_id             AS aplctn_id,
         exp_ins_upd.agmt_ev_type_cd       AS agmt_ev_type_cd,
         exp_ins_upd.ev_strt_dttm_out      AS ev_strt_dttm,
         exp_ins_upd.ev_end_dttm_out       AS ev_end_dttm,
         exp_ins_upd.ev_actvy_type_cd_out  AS ev_actvy_type_cd,
         exp_ins_upd.ev_rsn_cd_out         AS ev_rsn_cd,
         exp_ins_upd.src_trans_id          AS src_trans_id,
         exp_ins_upd.ev_sbtype_cd_out      AS ev_sbtype_cd,
         exp_ins_upd.prcsd_src_sys_cd_out  AS prcsd_src_sys_cd,
         exp_ins_upd.agmt_id1              AS agmt_id,
         exp_ins_upd.prcs_id               AS prcs_id,
         exp_ins_upd.func_cd               AS func_cd,
         exp_ins_upd.agmt_ev_dttm_out      AS agmt_ev_dttm,
         exp_ins_upd.out_ins_upd1          AS out_ins_upd,
         exp_ins_upd.out_edw_strt_dttm     AS out_edw_strt_dttm,
         exp_ins_upd.out_edw_end_dttm_out  AS out_edw_end_dttm,
         exp_ins_upd.lkp_edw_strt_dttm_upd AS edw_strt_dttm_upd,
         exp_ins_upd.lkp_ev_strt_dttm      AS lkp_ev_strt_dttm,
         exp_ins_upd.lkp_ev_end_dttm       AS lkp_ev_end_dttm,
         exp_ins_upd.lkp_ev_rsn_cd         AS lkp_ev_rsn_cd,
         exp_ins_upd.lkp_agmt_id           AS lkp_agmt_id,
         exp_ins_upd.lkp_prcsd_src_sys_cd  AS lkp_prcsd_src_sys_cd,
         exp_ins_upd.lkp_edw_end_dttm      AS lkp_edw_end_dttm,
         exp_ins_upd.retired               AS retired,
         exp_ins_upd.src_sys_cd            AS src_sys_cd,
         exp_ins_upd.transactionnumber     AS transactionnumber,
         exp_ins_upd.xref_ev_id            AS xref_ev_id,
         exp_ins_upd.rnk                   AS rnk,
         exp_ins_upd.out_ev_ctgy_type      AS out_ev_ctgy_type,
         exp_ins_upd.source_record_id
  FROM   exp_ins_upd
  WHERE  ( (
                       exp_ins_upd.out_ins_upd1 = ''I'' )
         OR     (
                       exp_ins_upd.out_ins_upd1 = ''U''
                AND    exp_ins_upd.ev_sbtype_cd_out = ''PLCYTRNS'' )
         OR     (
                       exp_ins_upd.out_ins_upd1 = ''U''
                AND    exp_ins_upd.ev_sbtype_cd_out = ''CATSTRPH''
                AND    exp_ins_upd.ev_strt_dttm_out > exp_ins_upd.lkp_ev_strt_dttm )
         OR     (
                       exp_ins_upd.out_ins_upd1 = ''U''
                AND    exp_ins_upd.ev_sbtype_cd_out = ''FINANCL''
                AND    exp_ins_upd.ev_actvy_type_cd_out <> ''SUSPITM''
                AND    exp_ins_upd.ev_strt_dttm_out > exp_ins_upd.lkp_ev_strt_dttm )
         OR     (
                       exp_ins_upd.out_ins_upd1 = ''U''
                AND    exp_ins_upd.ev_sbtype_cd_out = ''FINANCL''
                AND    exp_ins_upd.ev_actvy_type_cd_out = ''DSBPMT''
                AND    exp_ins_upd.agmt_id1 <> exp_ins_upd.lkp_agmt_id )
         OR     (
                       exp_ins_upd.out_ins_upd1 = ''U''
                AND    exp_ins_upd.ev_sbtype_cd_out = ''FINANCL''
                AND    exp_ins_upd.ev_actvy_type_cd_out = ''SUSPITM'' )
         OR     (
                       exp_ins_upd.retired = 0
                AND    exp_ins_upd.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) );
  
  -- Component rtr_ev_ins_upd_RETIRE, Type ROUTER Output Group RETIRE
  create or replace temporary table rtr_ev_ins_upd_RETIRE as
  SELECT exp_ins_upd.lkp_ev_id             AS lkp_ev_id,
         exp_ins_upd.lkp_agmt_ev_type_cd   AS lkp_agmt_ev_type_cd,
         exp_ins_upd.aplctn_id             AS aplctn_id,
         exp_ins_upd.agmt_ev_type_cd       AS agmt_ev_type_cd,
         exp_ins_upd.ev_strt_dttm_out      AS ev_strt_dttm,
         exp_ins_upd.ev_end_dttm_out       AS ev_end_dttm,
         exp_ins_upd.ev_actvy_type_cd_out  AS ev_actvy_type_cd,
         exp_ins_upd.ev_rsn_cd_out         AS ev_rsn_cd,
         exp_ins_upd.src_trans_id          AS src_trans_id,
         exp_ins_upd.ev_sbtype_cd_out      AS ev_sbtype_cd,
         exp_ins_upd.prcsd_src_sys_cd_out  AS prcsd_src_sys_cd,
         exp_ins_upd.agmt_id1              AS agmt_id,
         exp_ins_upd.prcs_id               AS prcs_id,
         exp_ins_upd.func_cd               AS func_cd,
         exp_ins_upd.agmt_ev_dttm_out      AS agmt_ev_dttm,
         exp_ins_upd.out_ins_upd1          AS out_ins_upd,
         exp_ins_upd.out_edw_strt_dttm     AS out_edw_strt_dttm,
         exp_ins_upd.out_edw_end_dttm_out  AS out_edw_end_dttm,
         exp_ins_upd.lkp_edw_strt_dttm_upd AS edw_strt_dttm_upd,
         exp_ins_upd.lkp_ev_strt_dttm      AS lkp_ev_strt_dttm,
         exp_ins_upd.lkp_ev_end_dttm       AS lkp_ev_end_dttm,
         exp_ins_upd.lkp_ev_rsn_cd         AS lkp_ev_rsn_cd,
         exp_ins_upd.lkp_agmt_id           AS lkp_agmt_id,
         exp_ins_upd.lkp_prcsd_src_sys_cd  AS lkp_prcsd_src_sys_cd,
         exp_ins_upd.lkp_edw_end_dttm      AS lkp_edw_end_dttm,
         exp_ins_upd.retired               AS retired,
         exp_ins_upd.src_sys_cd            AS src_sys_cd,
         exp_ins_upd.transactionnumber     AS transactionnumber,
         exp_ins_upd.xref_ev_id            AS xref_ev_id,
         exp_ins_upd.rnk                   AS rnk,
         exp_ins_upd.out_ev_ctgy_type      AS out_ev_ctgy_type,
         exp_ins_upd.source_record_id
  FROM   exp_ins_upd
  WHERE  (
                exp_ins_upd.out_ins_upd1 = ''R''
         AND    exp_ins_upd.retired != 0
         AND    exp_ins_upd.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
  
  -- Component upd_ev_ins, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ev_ins AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ev_ins_upd_insert.aplctn_id         AS aplctn_id1,
                rtr_ev_ins_upd_insert.agmt_ev_type_cd   AS agmt_ev_type_cd,
                rtr_ev_ins_upd_insert.ev_strt_dttm      AS ev_strt_dttm,
                rtr_ev_ins_upd_insert.ev_end_dttm       AS ev_end_dttm,
                rtr_ev_ins_upd_insert.ev_actvy_type_cd  AS ev_actvy_type_cd,
                rtr_ev_ins_upd_insert.ev_rsn_cd         AS ev_rsn_cd,
                rtr_ev_ins_upd_insert.src_trans_id      AS src_trans_id,
                rtr_ev_ins_upd_insert.ev_sbtype_cd      AS ev_sbtype_cd,
                rtr_ev_ins_upd_insert.prcs_id           AS prcs_id,
                rtr_ev_ins_upd_insert.prcsd_src_sys_cd  AS prcsd_src_sys_cd,
                rtr_ev_ins_upd_insert.agmt_id           AS agmt_id,
                rtr_ev_ins_upd_insert.func_cd           AS func_cd,
                rtr_ev_ins_upd_insert.agmt_ev_dttm      AS agmt_ev_dttm,
                rtr_ev_ins_upd_insert.out_edw_strt_dttm AS out_edw_strt_dttm1,
                rtr_ev_ins_upd_insert.out_edw_end_dttm  AS out_edw_end_dttm1,
                rtr_ev_ins_upd_insert.lkp_ev_id         AS lkp_ev_id,
                rtr_ev_ins_upd_insert.retired           AS retired1,
                rtr_ev_ins_upd_insert.src_sys_cd        AS src_sys_cd1,
                rtr_ev_ins_upd_insert.transactionnumber AS transactionnumber1,
                rtr_ev_ins_upd_insert.rnk               AS rnk1,
                rtr_ev_ins_upd_insert.xref_ev_id        AS xref_ev_id1,
                rtr_ev_ins_upd_insert.out_ev_ctgy_type  AS out_ev_ctgy_type1,
                0                                       AS update_strategy_action,
                rtr_ev_ins_upd_insert.source_record_id
         FROM   rtr_ev_ins_upd_insert );
  -- Component upd_ev_Update_Retire_rejected, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_ev_update_retire_rejected AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ev_ins_upd_retire.edw_strt_dttm_upd    AS edw_strt_dttm_upd3,
                rtr_ev_ins_upd_retire.lkp_ev_id            AS lkp_ev_id,
                rtr_ev_ins_upd_retire.out_edw_end_dttm     AS out_edw_end_dttm3,
                rtr_ev_ins_upd_retire.lkp_ev_strt_dttm     AS lkp_ev_strt_dttm3,
                rtr_ev_ins_upd_retire.lkp_ev_end_dttm      AS lkp_ev_end_dttm3,
                rtr_ev_ins_upd_retire.lkp_ev_rsn_cd        AS lkp_ev_rsn_cd3,
                rtr_ev_ins_upd_retire.lkp_prcsd_src_sys_cd AS lkp_prcsd_src_sys_cd3,
                rtr_ev_ins_upd_retire.lkp_agmt_ev_type_cd  AS lkp_agmt_ev_type_cd3,
                rtr_ev_ins_upd_retire.retired              AS retired3,
                1                                          AS update_strategy_action,
                rtr_ev_ins_upd_retire.source_record_id
         FROM   rtr_ev_ins_upd_retire );
  -- Component exp_pass_to_target_ins, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_pass_to_target_ins AS
  (
         SELECT
                upd_ev_ins.ev_actvy_type_cd                                            AS v_prev_ev_actvy_type_cd,
                upd_ev_ins.src_trans_id                                                AS v_prev_src_trans_id,
                upd_ev_ins.ev_sbtype_cd                                                AS v_prev_ev_sbtype_cd,
                CASE
                       WHEN upd_ev_ins.lkp_ev_id > 0 THEN upd_ev_ins.lkp_ev_id
                       ELSE
                              CASE
                                     WHEN upd_ev_ins.ev_actvy_type_cd = v_prev_ev_actvy_type_cd
                                     AND    upd_ev_ins.src_trans_id = v_prev_src_trans_id
                                     AND    upd_ev_ins.ev_sbtype_cd = v_prev_ev_sbtype_cd THEN var_prev_ev_id
                                     ELSE upd_ev_ins.xref_ev_id1
                              END
                END                         AS var_new_ev_id,
                var_new_ev_id               AS out_ev_id,
                upd_ev_ins.aplctn_id1       AS aplctn_id1,
                upd_ev_ins.agmt_ev_type_cd  AS agmt_ev_type_cd,
                upd_ev_ins.ev_strt_dttm     AS ev_strt_dttm,
                upd_ev_ins.ev_end_dttm      AS ev_end_dttm,
                upd_ev_ins.ev_actvy_type_cd AS ev_actvy_type_cd,
                upd_ev_ins.ev_rsn_cd        AS ev_rsn_cd,
                upd_ev_ins.src_trans_id     AS src_trans_id,
                upd_ev_ins.ev_sbtype_cd     AS ev_sbtype_cd,
                upd_ev_ins.prcs_id          AS prcs_id,
                upd_ev_ins.prcsd_src_sys_cd AS prcsd_src_sys_cd,
                upd_ev_ins.agmt_id          AS agmt_id,
                upd_ev_ins.func_cd          AS func_cd,
                upd_ev_ins.agmt_ev_dttm     AS agmt_ev_dttm,
                CASE
                       WHEN upd_ev_ins.retired1 = 0 THEN dateadd(''second'', ( 2 * ( upd_ev_ins.rnk1 - 1 ) ), current_timestamp)
                       ELSE current_timestamp
                END AS out_edw_strt_dttm1,
                CASE
                       WHEN upd_ev_ins.retired1 != 0 THEN current_timestamp
                       ELSE upd_ev_ins.out_edw_end_dttm1
                END                                                                    AS o_edw_end_dttm,
                upd_ev_ins.src_sys_cd1                                                 AS src_sys_cd1,
                upd_ev_ins.transactionnumber1                                          AS transactionnumber1,

                var_new_ev_id                                                          AS var_prev_ev_id,
                upd_ev_ins.out_ev_ctgy_type1                                           AS out_ev_ctgy_type1,
                to_timestamp_ntz ( ''1900-01-01 00:00:00.000000'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS out_ev_gmt_strt_dttm,
                upd_ev_ins.source_record_id
         FROM   upd_ev_ins );
  -- Component exp_ev_Update_Retire_rejected, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_ev_update_retire_rejected AS
  (
         SELECT upd_ev_update_retire_rejected.edw_strt_dttm_upd3 AS edw_strt_dttm_upd3,
                upd_ev_update_retire_rejected.lkp_ev_id          AS lkp_ev_id,
                current_timestamp                                AS out_edw_end_dttm,
                upd_ev_update_retire_rejected.source_record_id
         FROM   upd_ev_update_retire_rejected );
  -- Component tgt_ev_ins, Type TARGET
  INSERT INTO db_t_prod_core.ev
              (
                          ev_id,
                          ev_strt_dttm,
                          ev_end_dttm,
                          ev_gmt_strt_dttm,
                          ev_actvy_type_cd,
                          ev_rsn_cd,
                          src_trans_id,
                          ev_sbtype_cd,
                          agmt_ev_type_cd,
                          agmt_id,
                          prcsd_src_sys_cd,
                          func_cd,
                          ev_dttm,
                          ev_ctgy_type_cd,
                          ev_num,
                          aplctn_id,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          src_sys_cd
              )
  SELECT exp_pass_to_target_ins.out_ev_id            AS ev_id,
         exp_pass_to_target_ins.ev_strt_dttm         AS ev_strt_dttm,
         exp_pass_to_target_ins.ev_end_dttm          AS ev_end_dttm,
         exp_pass_to_target_ins.out_ev_gmt_strt_dttm AS ev_gmt_strt_dttm,
         exp_pass_to_target_ins.ev_actvy_type_cd     AS ev_actvy_type_cd,
         exp_pass_to_target_ins.ev_rsn_cd            AS ev_rsn_cd,
         exp_pass_to_target_ins.src_trans_id         AS src_trans_id,
         exp_pass_to_target_ins.ev_sbtype_cd         AS ev_sbtype_cd,
         exp_pass_to_target_ins.agmt_ev_type_cd      AS agmt_ev_type_cd,
         exp_pass_to_target_ins.agmt_id              AS agmt_id,
         exp_pass_to_target_ins.prcsd_src_sys_cd     AS prcsd_src_sys_cd,
         exp_pass_to_target_ins.func_cd              AS func_cd,
         exp_pass_to_target_ins.agmt_ev_dttm         AS ev_dttm,
         exp_pass_to_target_ins.out_ev_ctgy_type1    AS ev_ctgy_type_cd,
         exp_pass_to_target_ins.transactionnumber1   AS ev_num,
         exp_pass_to_target_ins.aplctn_id1           AS aplctn_id,
         exp_pass_to_target_ins.prcs_id              AS prcs_id,
         exp_pass_to_target_ins.out_edw_strt_dttm1   AS edw_strt_dttm,
         exp_pass_to_target_ins.o_edw_end_dttm       AS edw_end_dttm,
         exp_pass_to_target_ins.src_sys_cd1          AS src_sys_cd
  FROM   exp_pass_to_target_ins;
  
  -- Component tgt_ev_Update_Retire_rejected, Type TARGET
  merge
  INTO         db_t_prod_core.ev
  USING        exp_ev_update_retire_rejected
  ON (
                            ev.ev_id = exp_ev_update_retire_rejected.lkp_ev_id
               AND          ev.edw_strt_dttm = exp_ev_update_retire_rejected.edw_strt_dttm_upd3)
  WHEN matched THEN
  UPDATE
  SET    ev_id = exp_ev_update_retire_rejected.lkp_ev_id,
         edw_strt_dttm = exp_ev_update_retire_rejected.edw_strt_dttm_upd3,
         edw_end_dttm = exp_ev_update_retire_rejected.out_edw_end_dttm;
  
  -- Component tgt_ev_Update_Retire_rejected, Type Post SQL
  UPDATE db_t_prod_core.ev
    SET    edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT ev_actvy_type_cd,
                                         src_trans_id,
                                         ev_sbtype_cd,
                                         edw_strt_dttm,
                                         max(edw_strt_dttm) over (PARTITION BY ev_actvy_type_cd,src_trans_id,ev_sbtype_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.ev ) a

  WHERE  ev.edw_strt_dttm = a.edw_strt_dttm
  AND    ev.src_trans_id=a.src_trans_id
  AND    ev.ev_sbtype_cd=a.ev_sbtype_cd
  AND    ev.ev_actvy_type_cd=a.ev_actvy_type_cd
  AND    lead1 IS NOT NULL;

END;
';