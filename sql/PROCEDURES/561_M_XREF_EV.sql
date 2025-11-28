-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_XREF_EV("WORKLET_NAME" VARCHAR)
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
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);

  END_DTTM := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
  START_DTTM := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);

  -- Component sq_ev, Type SOURCE
  CREATE
  OR
  REPLACE TEMPORARY TABLE sq_ev AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS src_trans_id,
                $2 AS ev_sbtype_cd,
                $3 AS ev_actvy_type_cd,
                $4 AS src_cd,
                $5 AS ins_upd_flad,
                $6 AS source_record_id
         FROM   (
                         SELECT   SRC.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     ( WITH temp1 AS
                                  (
                                                  /****************************************bc_basemoneyreceived****************************/
                                                  SELECT DISTINCT CAST(''EV_ACTVY_TYPE14'' AS            VARCHAR(50)) AS ev_act_type_code ,
                                                                  CAST( bc_basemoneyreceived.id_stg AS VARCHAR(60)) AS key1 ,
                                                                  CAST(''EV_SBTYPE2'' AS                 VARCHAR(50)) AS SUBTYPE ,
                                                                  ''SRC_SYS5''                                        AS src_sys
                                                  FROM            (
                                                                         SELECT bc_basemoneyreceived.ID_stg,
                                                                                bc_basemoneyreceived.Subtype_stg,
                                                                                bc_basemoneyreceived.ReversalReason_stg
                                                                         FROM   DB_T_PROD_STAG.bc_basemoneyreceived
                                                                         WHERE  bc_basemoneyreceived.UpdateTime_stg > (:start_dttm)
                                                                         AND    bc_basemoneyreceived.UpdateTime_stg <= (:end_dttm) ) bc_basemoneyreceived
                                                  INNER JOIN      DB_T_PROD_STAG.bctl_basemoneyreceived
                                                  ON              bctl_basemoneyreceived.id_stg=bc_basemoneyreceived.Subtype_stg
                                                                  /* removed below join for performance tuning EIM-32693 */
                                                                  /*  LEFT OUTER JOIN DB_T_PROD_STAG.bctl_paymentreversalreason ON bctl_paymentreversalreason.id_stg=bc_basemoneyreceived.ReversalReason_stg  */
                                                  WHERE           bctl_basemoneyreceived.typecode_stg IN (''PaymentMoneyReceived'',
                                                                                                          ''DirectBillMoneyRcvd'',
                                                                                                          ''ZeroDollarDMR'',
                                                                                                          ''ZeroDollarReversal'')
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE25''                                 AS ev_act_type_code ,
                                                                  CAST( bc_basemoneyreceived.id_stg AS VARCHAR(60)) AS key1 ,
                                                                  ''EV_SBTYPE2''                                      AS SUBTYPE ,
                                                                  ''SRC_SYS5''                                        AS src_sys
                                                  FROM            (
                                                                         SELECT bc_basemoneyreceived.ID_stg,
                                                                                bc_basemoneyreceived.Subtype_stg,
                                                                                bc_basemoneyreceived.ReversalReason_stg,
                                                                                bc_basemoneyreceived.ReversalDate_stg
                                                                         FROM   DB_T_PROD_STAG.bc_basemoneyreceived
                                                                         WHERE  bc_basemoneyreceived.UpdateTime_stg > (:start_dttm)
                                                                         AND    bc_basemoneyreceived.UpdateTime_stg <= (:end_dttm) ) bc_basemoneyreceived
                                                  INNER JOIN      DB_T_PROD_STAG.bctl_basemoneyreceived
                                                  ON              bctl_basemoneyreceived.id_stg=bc_basemoneyreceived.Subtype_stg
                                                                  /* removed below join for performance tuning EIM-32693 */
                                                                  /*  LEFT OUTER JOIN DB_T_PROD_STAG.bctl_paymentreversalreason ON bctl_paymentreversalreason.id_stg=bc_basemoneyreceived.ReversalReason_stg  */
                                                  WHERE           bctl_basemoneyreceived.typecode_stg IN (''PaymentMoneyReceived'',
                                                                                                          ''DirectBillMoneyRcvd'',
                                                                                                          ''ZeroDollarDMR'',
                                                                                                          ''ZeroDollarReversal'')
                                                  AND             bc_basemoneyreceived.reversaldate_stg IS NOT NULL ), temp2 AS
                                  (
                                                  /****************************************bc_basemoneyreceived****************************/
                                                  /* UNION */
                                                  /***************************bc_basedistitem****************************/
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE26''                       AS ev_act_type_code ,
                                                                  CAST(bc_basedistitem_ID AS VARCHAR(60)) AS key1 ,
                                                                  ''EV_SBTYPE2''                            AS SUBTYPE ,
                                                                  ''SRC_SYS5''                              AS src_sys
                                                  FROM            (
                                                                         SELECT *
                                                                         FROM   (
                                                                                          SELECT    CAST( ''TRANSACTION'' AS VARCHAR(50)) AS TRANSACTION_ID,
                                                                                                    bc_basedistitem.ID_stg                 bc_basedistitem_ID,
                                                                                                    bc_transaction.PublicID_stg            bc_tran_PublicID
                                                                                          FROM      DB_T_PROD_STAG.bc_basedistitem
                                                                                                    /* removed below join for performance tuning EIM-32693 */
                                                                                                    /* join DB_T_PROD_STAG.bc_invoiceitem
on bc_basedistitem.InvoiceItemID_stg=bc_invoiceitem.ID_stg
join DB_T_PROD_STAG.bc_invoice
on bc_invoice.id_stg=bc_invoiceitem.InvoiceID_stg
join DB_T_PROD_STAG.bctl_invoiceitemtype
on bctl_invoiceitemtype.id_stg=bc_invoiceitem.Type_stg
join DB_T_PROD_STAG.bc_charge
on bc_charge.id_stg=bc_invoiceitem.ChargeID_stg
join DB_T_PROD_STAG.bc_chargepattern
on bc_chargepattern.id_stg=bc_charge.ChargePatternID_stg
join DB_T_PROD_STAG.bctl_chargecategory
on bctl_chargecategory.id_stg=bc_chargepattern.Category_stg
left join DB_T_PROD_STAG.bc_policyperiod
on bc_basedistitem.policyperiodid_stg=bc_policyperiod.ID_stg*/
                                                                                          LEFT JOIN DB_T_PROD_STAG.bc_chargeinstancecontext
                                                                                          ON        bc_chargeinstancecontext.DirectBillPaymentItemID_stg = bc_basedistitem.id_stg
                                                                                          LEFT JOIN DB_T_PROD_STAG.bc_transaction
                                                                                          ON        bc_transaction.id_stg=bc_chargeinstancecontext.TransactionID_stg
                                                                                          WHERE     bc_basedistitem.UpdateTime_stg > (:start_dttm)
                                                                                          AND       bc_basedistitem.UpdateTime_stg <= (:end_dttm)
                                                                                          UNION ALL
                                                                                          SELECT    CAST(''TRANSACTION-REVERSED'' AS VARCHAR(50)) TRANSACTION_ID,
                                                                                                    bc_basedistitem.ID_stg                      bc_basedistitem_ID,
                                                                                                    bc_transaction.PublicID_stg                 bc_tran_PublicID
                                                                                          FROM      DB_T_PROD_STAG.bc_basedistitem
                                                                                                    /* removed below join for performance tuning EIM-32693 */
                                                                                                    /*    join DB_T_PROD_STAG.bc_invoiceitem
on bc_basedistitem.InvoiceItemID_stg=bc_invoiceitem.ID_stg
join DB_T_PROD_STAG.bc_invoice
on bc_invoice.id_stg=bc_invoiceitem.InvoiceID_stg
join DB_T_PROD_STAG.bctl_invoiceitemtype
on bctl_invoiceitemtype.id_stg=bc_invoiceitem.Type_stg
join DB_T_PROD_STAG.bc_charge
on bc_charge.id_stg=bc_invoiceitem.ChargeID_stg
join DB_T_PROD_STAG.bc_chargepattern
on bc_chargepattern.id_stg=bc_charge.ChargePatternID_stg
join DB_T_PROD_STAG.bctl_chargecategory
on bctl_chargecategory.id_stg=bc_chargepattern.Category_stg
left join DB_T_PROD_STAG.bc_policyperiod
on bc_basedistitem.policyperiodid_stg=bc_policyperiod.ID_stg*/
                                                                                          LEFT JOIN DB_T_PROD_STAG.bc_chargeinstancecontext
                                                                                          ON        bc_chargeinstancecontext.DirectBillPaymentItemID_stg = bc_basedistitem.id_stg
                                                                                          LEFT JOIN DB_T_PROD_STAG.bc_transaction
                                                                                          ON        bc_transaction.id_stg=bc_chargeinstancecontext.TransactionID_stg
                                                                                          WHERE     bc_basedistitem.UpdateTime_stg > (:start_dttm)
                                                                                          AND       bc_basedistitem.UpdateTime_stg <= (:end_dttm)
                                                                                          AND       ReversedDistID_stg IS NOT NULL ) a
                                                                                /* order by bc_tran_PublicID,TRANSACTION_ID */
                                                                  ) bc_basedistitem
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE27''                       AS ev_act_type_code ,
                                                                  CAST(bc_basedistitem_ID AS VARCHAR(60)) AS key1 ,
                                                                  ''EV_SBTYPE2''                            AS SUBTYPE ,
                                                                  ''SRC_SYS5''                              AS src_sys
                                                  FROM            (
                                                                         SELECT bc_basedistitem.ID_stg bc_basedistitem_ID,
                                                                                bc_basedistitem.ReversedDate_stg,
                                                                                bc_basedistitem.UpdateTime_stg
                                                                         FROM   DB_T_PROD_STAG.bc_basedistitem
                                                                         WHERE  bc_basedistitem.UpdateTime_stg > (:start_dttm)
                                                                         AND    bc_basedistitem.UpdateTime_stg <= (:end_dttm)
                                                                         UNION ALL
                                                                         SELECT
                                                                                /* cast(''TRANSACTION-REVERSED'' as varchar(50)) TRANSACTION_ID, */
                                                                                bc_basedistitem.ID_stg bc_basedistitem_ID,
                                                                                /* bc_transaction.PublicID_stg  bc_tran_PublicID, */
                                                                                bc_basedistitem.ReversedDate_stg,
                                                                                bc_basedistitem.UpdateTime_stg
                                                                         FROM   DB_T_PROD_STAG.bc_basedistitem
                                                                         WHERE  bc_basedistitem.UpdateTime_stg > (:start_dttm)
                                                                         AND    bc_basedistitem.UpdateTime_stg <= (:end_dttm)
                                                                         AND    ReversedDistID_stg IS NOT NULL
                                                                                /* ) a  */
                                                                                /* order by bc_tran_PublicID,TRANSACTION_ID */
                                                                  ) bc_basedistitem
                                                  WHERE           bc_basedistitem.ReversedDate_stg IS NOT NULL ), temp3 AS
                                  (
                                                  /* UNION */
                                                  /***************************bc_basenonrecdistitem****************************/
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE28'' ev_act_type_code ,
                                                                  CAST(bc_basenonrecdistitem.id_stg AS VARCHAR(60)) AS key1 ,
                                                                  ''EV_SBTYPE2''                                      AS SUBTYPE ,
                                                                  ''SRC_SYS5''                                        AS src_sys
                                                  FROM            (
                                                                         SELECT bc_basenonrecdistitem.ReversedDate_stg,
                                                                                bc_basenonrecdistitem.ID_stg
                                                                         FROM   DB_T_PROD_STAG.bc_basenonrecdistitem
                                                                         WHERE  bc_basenonrecdistitem.UpdateTime_stg > (:start_dttm)
                                                                         AND    bc_basenonrecdistitem.UpdateTime_stg <= (:end_dttm)) bc_basenonrecdistitem
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE29'' ev_act_type_code ,
                                                                  CAST(bc_basenonrecdistitem.id_stg AS VARCHAR(60)) AS key1 ,
                                                                  ''EV_SBTYPE2''                                      AS SUBTYPE ,
                                                                  ''SRC_SYS5''                                        AS src_sys
                                                  FROM            (
                                                                         SELECT bc_basenonrecdistitem.ReversedDate_stg,
                                                                                bc_basenonrecdistitem.ID_stg
                                                                         FROM   DB_T_PROD_STAG.bc_basenonrecdistitem
                                                                         WHERE  bc_basenonrecdistitem.UpdateTime_stg > (:start_dttm)
                                                                         AND    bc_basenonrecdistitem.UpdateTime_stg <= (:end_dttm)) bc_basenonrecdistitem
                                                  WHERE           bc_basenonrecdistitem.ReversedDate_stg IS NOT NULL
                                                  /***************************bc payments****************************/
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE30''                               AS ev_act_type_code ,
                                                                  CAST( bc_suspensepayment.id_stg AS VARCHAR(60)) AS key1 ,
                                                                  ''EV_SBTYPE2''                                    AS SUBTYPE ,
                                                                  ''SRC_SYS5''                                      AS src_sys
                                                  FROM            (
                                                                         SELECT bc_suspensepayment.ID_stg
                                                                         FROM   DB_T_PROD_STAG.bc_suspensepayment
                                                                         WHERE  bc_suspensepayment.UpdateTime_stg > (:start_dttm)
                                                                         AND    bc_suspensepayment.UpdateTime_stg <= (:end_dttm) ) bc_suspensepayment
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE31''                              AS ev_act_type_code ,
                                                                  CAST(bc_outgoingpayment.id_stg AS VARCHAR(60)) AS key1 ,
                                                                  ''EV_SBTYPE2''                                   AS SUBTYPE ,
                                                                  ''SRC_SYS5''                                     AS src_sys
                                                  FROM            (
                                                                         SELECT bc_outgoingpayment.ID_stg
                                                                                /* FROM */
                                                                                /* (Select bc_outgoingpayment.* */
                                                                                /* , bc_paymentinstrument.PaymentMethod_stg as PaymentMethod, bctl_paymentmethod.typecode_stg as fund_trnsfr_mthd_typ */
                                                                         FROM   DB_T_PROD_STAG.bc_outgoingpayment
                                                                         WHERE  bc_outgoingpayment.UpdateTime_stg > (:start_dttm)
                                                                         AND    bc_outgoingpayment.UpdateTime_stg <= (:end_dttm)
                                                                                /* )inr */
                                                                  ) bc_outgoingpayment
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE32''                           AS ev_act_type_code ,
                                                                  CAST(bc_disbursement.id_stg AS VARCHAR(60)) AS key1 ,
                                                                  ''EV_SBTYPE2''                                AS SUBTYPE ,
                                                                  ''SRC_SYS5''                                  AS src_sys
                                                  FROM            (
                                                                         SELECT bc_disbursement.ID_stg,
                                                                                bc_disbursement.Reason_stg
                                                                         FROM   DB_T_PROD_STAG.bc_disbursement
                                                                         WHERE  bc_disbursement.UpdateTime_stg > (:start_dttm)
                                                                         AND    bc_disbursement.UpdateTime_stg <= (:end_dttm) ) bc_disbursement
                                                  LEFT JOIN       DB_T_PROD_STAG.bctl_reason
                                                  ON              bctl_reason.id_stg=bc_disbursement.Reason_stg ), temp4 AS
                                  (
                                                  /* UNION */
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE33''                       AS ev_act_type_code ,
                                                                  CAST(bc_writeoff.id_stg AS VARCHAR(60)) AS key1 ,
                                                                  ''EV_SBTYPE2''                            AS SUBTYPE ,
                                                                  ''SRC_SYS5''                              AS src_sys
                                                  FROM            (
                                                                         SELECT bc_writeoff.Reversed_stg,
                                                                                bc_writeoff.Reason_stg,
                                                                                bc_writeoff.ID_stg,
                                                                                ''WRITEOFF'' AS WRITEOFFFLAG
                                                                         FROM   DB_T_PROD_STAG.bc_writeoff
                                                                         WHERE  bc_writeoff.id_stg NOT IN
                                                                                (
                                                                                       SELECT OwnerID_stg
                                                                                       FROM   db_t_prod_stag.bc_revwriteoff)
                                                                         AND    bc_writeoff.UpdateTime_stg > (:start_dttm)
                                                                         AND    bc_writeoff.UpdateTime_stg <= (:end_dttm) ) bc_writeoff
                                                  LEFT JOIN       DB_T_PROD_STAG.bctl_writeoffreason
                                                  ON              bctl_writeoffreason.id_stg=bc_writeoff.Reason_stg
                                                  WHERE           writeoffflag=''WRITEOFF''
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE34''                       AS ev_act_type_code ,
                                                                  CAST(bc_writeoff.id_stg AS VARCHAR(60)) AS key1 ,
                                                                  ''EV_SBTYPE2''                            AS SUBTYPE ,
                                                                  ''SRC_SYS5''                              AS src_sys
                                                  FROM            (
                                                                         SELECT bc_writeoff.Reversed_stg,
                                                                                bc_writeoff.Reason_stg,
                                                                                bc_writeoff.ID_stg,
                                                                                ''REVWRITEOFF'' AS WRITEOFFFLAG
                                                                         FROM   DB_T_PROD_STAG.bc_writeoff
                                                                         WHERE  bc_writeoff.id_stg IN
                                                                                (
                                                                                       SELECT OwnerID_stg
                                                                                       FROM   db_t_prod_stag.bc_revwriteoff)
                                                                         AND    bc_writeoff.UpdateTime_stg > (:start_dttm)
                                                                         AND    bc_writeoff.UpdateTime_stg <= (:end_dttm) ) bc_writeoff
                                                  LEFT JOIN       DB_T_PROD_STAG.bctl_writeoffreason
                                                  ON              bctl_writeoffreason.id_stg=bc_writeoff.Reason_stg
                                                  WHERE           writeoffflag=''REVWRITEOFF''
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE33''                       AS ev_act_type_code ,
                                                                  CAST(bc_writeoff.id_stg AS VARCHAR(60)) AS key1 ,
                                                                  ''EV_SBTYPE2''                            AS SUBTYPE ,
                                                                  ''SRC_SYS5''                              AS src_sys
                                                  FROM            (
                                                                         SELECT bc_writeoff.Reversed_stg,
                                                                                bc_writeoff.Reason_stg,
                                                                                bc_writeoff.ID_stg,
                                                                                ''WRITEOFF'' AS WRITEOFFFLAG
                                                                         FROM   DB_T_PROD_STAG.bc_writeoff
                                                                         WHERE  bc_writeoff.id_stg NOT IN
                                                                                (
                                                                                       SELECT OwnerID_stg
                                                                                       FROM   db_t_prod_stag.bc_revwriteoff)
                                                                         AND    bc_writeoff.UpdateTime_stg > (:start_dttm)
                                                                         AND    bc_writeoff.UpdateTime_stg <= (:end_dttm)
                                                                         UNION
                                                                         SELECT bc_writeoff.Reversed_stg,
                                                                                bc_writeoff.Reason_stg,
                                                                                bc_writeoff.ID_stg,
                                                                                ''REVWRITEOFF'' AS WRITEOFFFLAG
                                                                         FROM   DB_T_PROD_STAG.bc_writeoff
                                                                         WHERE  bc_writeoff.id_stg IN
                                                                                (
                                                                                       SELECT OwnerID_stg
                                                                                       FROM   db_t_prod_stag.bc_revwriteoff)
                                                                         AND    bc_writeoff.UpdateTime_stg > (:start_dttm)
                                                                         AND    bc_writeoff.UpdateTime_stg <= (:end_dttm) ) bc_writeoff
                                                  LEFT JOIN       DB_T_PROD_STAG.bctl_writeoffreason
                                                  ON              bctl_writeoffreason.id_stg=bc_writeoff.Reason_stg
                                                  WHERE           reversed_stg=0
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE34''                       AS ev_act_type_code ,
                                                                  CAST(bc_writeoff.id_stg AS VARCHAR(60)) AS key1 ,
                                                                  ''EV_SBTYPE2''                            AS SUBTYPE ,
                                                                  ''SRC_SYS5''                              AS src_sys
                                                  FROM            (
                                                                         SELECT bc_writeoff.Reversed_stg,
                                                                                bc_writeoff.Reason_stg,
                                                                                bc_writeoff.ID_stg,
                                                                                ''WRITEOFF'' AS WRITEOFFFLAG
                                                                         FROM   DB_T_PROD_STAG.bc_writeoff
                                                                         WHERE  bc_writeoff.id_stg NOT IN
                                                                                (
                                                                                       SELECT OwnerID_stg
                                                                                       FROM   db_t_prod_stag.bc_revwriteoff)
                                                                         AND    bc_writeoff.UpdateTime_stg > (:start_dttm)
                                                                         AND    bc_writeoff.UpdateTime_stg <= (:end_dttm)
                                                                         UNION
                                                                         SELECT bc_writeoff.Reversed_stg,
                                                                                bc_writeoff.Reason_stg,
                                                                                bc_writeoff.ID_stg,
                                                                                ''REVWRITEOFF'' AS WRITEOFFFLAG
                                                                         FROM   DB_T_PROD_STAG.bc_writeoff
                                                                         WHERE  bc_writeoff.id_stg IN
                                                                                (
                                                                                       SELECT OwnerID_stg
                                                                                       FROM   db_t_prod_stag.bc_revwriteoff)
                                                                         AND    bc_writeoff.UpdateTime_stg > (:start_dttm)
                                                                         AND    bc_writeoff.UpdateTime_stg <= (:end_dttm) ) bc_writeoff
                                                  LEFT JOIN       DB_T_PROD_STAG.bctl_writeoffreason
                                                  ON              bctl_writeoffreason.id_stg=bc_writeoff.Reason_stg
                                                  WHERE           reversed_stg=1 ), temp5 AS
                                  (
                                                  /* UNION */
                                                  /******************* Catastrophe Event***********/
                                                  SELECT DISTINCT tlcat.typecode_stg    AS ev_act_type_code ,
                                                                  catastrophenumber_stg AS key1 ,
                                                                  ''EV_SBTYPE1''          AS SUBTYPE ,
                                                                  ''SRC_SYS6''            AS src_sys
                                                  FROM            (
                                                                         SELECT cccat.CatastropheNumber_stg,
                                                                                cccat.Type_stg
                                                                         FROM   DB_T_PROD_STAG.cc_catastrophe cccat
                                                                         WHERE  cccat.updatetime_stg>(:start_dttm)
                                                                         AND    cccat.updatetime_stg <= (:end_dttm) ) cc_catastrophe
                                                  JOIN            DB_T_PROD_STAG.cctl_catastrophetype tlcat
                                                  ON              cc_catastrophe.Type_stg=tlcat.id_stg
                                                  UNION
                                                  /*************************Claim Check Event*******************/
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE24''             AS ev_act_type_code,
                                                                  CAST(t.ID_stg AS VARCHAR(60)) AS key1,
                                                                  ''EV_SBTYPE2''                  AS SUBTYPE ,
                                                                  ''SRC_SYS6''                    AS src_sys
                                                  FROM            (
                                                                                  SELECT          a.ID_stg,
                                                                                                  a.Subtype_stg,
                                                                                                  cctl_transactionstatus.typecode_stg AS typecode_stg
                                                                                  FROM            DB_T_PROD_STAG.cc_transaction a
                                                                                  JOIN            DB_T_PROD_STAG.cctl_transactionstatus
                                                                                  ON              a .status_stg= DB_T_PROD_STAG.cctl_transactionstatus .ID_stg
                                                                                  JOIN
                                                                                                  (
                                                                                                             SELECT     cc_claim.*
                                                                                                             FROM       DB_T_PROD_STAG.cc_claim
                                                                                                             INNER JOIN DB_T_PROD_STAG.cctl_claimstate
                                                                                                             ON         cc_claim.State_stg= cctl_claimstate.id_stg
                                                                                                             WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                  ON              cc_claim.ID_stg=a.ClaimID_stg
                                                                                  JOIN            DB_T_PROD_STAG.cc_policy
                                                                                  ON              cc_claim.PolicyID_stg=cc_policy.ID_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.cc_exposure
                                                                                  ON              cc_exposure.id_stg=a.exposureid_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.cc_check
                                                                                  ON              cc_check.id_stg = a.CheckID_stg
                                                                                                  /* removed below join for performance tuning EIM-32693 */
                                                                                                  /*  LEFT OUTER JOIN DB_T_PROD_STAG.cc_user ON a.CreateUserID_stg = cc_user.id_stg
LEFT OUTER JOIN DB_T_PROD_STAG.cc_contact  ON cc_user.ContactID_stg = cc_contact.id_stg */
                                                                                  WHERE           ((
                                                                                                                                  a.UpdateTime_stg >(:start_dttm)
                                                                                                                  AND             a.UpdateTime_stg <= (:end_dttm))
                                                                                                  OR              (
                                                                                                                                  cc_check.UpdateTime_stg >(:start_dttm)
                                                                                                                  AND             cc_check.UpdateTime_stg <= (:end_dttm)))
                                                                                                  /* order by publicid_stg */
                                                                  ) t
                                                  JOIN            DB_T_PROD_STAG.cctl_transaction tl
                                                  ON              tl.id_stg=t.subtype_stg
                                                  WHERE           tl.typecode_stg=''Payment''
                                                  UNION
                                                  /*****************PolicyTransactions*********/
                                                  SELECT DISTINCT pctl_job.TYPECODE_stg AS ev_act_type_code ,
                                                                  pc_job.JobNumber_stg  AS key1 ,
                                                                  ''EV_SBTYPE3''          AS SUBTYPE ,
                                                                  ''SRC_SYS4''            AS src_sys
                                                  FROM            (
                                                                                  SELECT DISTINCT pc_job.JobNumber_stg ,
                                                                                                  pctl_policyperiodstatus.TYPECODE_stg AS TYPECODE_policyperiodstatus_stg,
                                                                                                  pc_policyperiod.PolicyNumber_stg,
                                                                                                  pc_job.Subtype_stg
                                                                                  FROM            DB_T_PROD_STAG.pc_job
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.pc_policyperiod
                                                                                  ON              pc_job.id_stg = pc_policyperiod.JobID_stg
                                                                                  LEFT JOIN       DB_T_PROD_STAG.pctl_policyperiodstatus
                                                                                  ON              pctl_policyperiodstatus.id_stg = pc_policyperiod.Status_stg
                                                                                  INNER JOIN      DB_T_PROD_STAG.pctl_job
                                                                                  ON              pctl_job.id_stg = pc_job.Subtype_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.pc_effectivedatedfields
                                                                                  ON              pc_effectivedatedfields.branchid_stg = pc_policyperiod.id_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.pcx_holineratingfactor_alfa
                                                                                  ON              pc_policyperiod.id_stg=pcx_holineratingfactor_alfa.branchid_stg
                                                                                                  /* removed below join for performance tuning EIM-32693 */
                                                                                                  /*  left outer join DB_T_PROD_STAG.pctl_billingperiodicity on pcx_holineratingfactor_alfa.AutoLatePayBillingPeriodicity_stg=pctl_billingperiodicity.ID_stg */
                                                                                  WHERE           pc_policyperiod.UpdateTime_stg > (:start_dttm)
                                                                                  AND             pc_policyperiod.UpdateTime_stg <= (:end_dttm)
                                                                                  AND             pctl_policyperiodstatus.typecode_stg<>''Temporary''
                                                                                  AND             pc_effectivedatedfields.expirationdate_stg IS NULL
                                                                                  AND             pcx_holineratingfactor_alfa.ExpirationDate_stg IS NULL ) pc_job
                                                  INNER JOIN      DB_T_PROD_STAG.pctl_job
                                                  ON              pctl_job.id_stg=pc_job.Subtype_stg
                                                  WHERE           pc_job.policynumber_stg IS NOT NULL
                                                  AND             TYPECODE_policyperiodstatus_stg<>''TEMPORARY''
                                                  UNION
                                                  /******************************Billing Transaction*****************************************/
                                                  SELECT DISTINCT bctl_transaction.TYPECODE_stg                AS ev_act_type_code ,
                                                                  CAST (bc_transaction.id_stg AS VARCHAR (50)) AS key1 ,
                                                                  CAST(''EV_SBTYPE2'' AS           VARCHAR(50))  AS SUBTYPE ,
                                                                  ''SRC_SYS5''                                   AS src_sys
                                                  FROM            (
                                                                                  SELECT DISTINCT bc_transaction.ID_stg,
                                                                                                  bc_transaction.Subtype_stg,
                                                                                                  bc_revtrans.ownerid_stg
                                                                                  FROM            DB_T_PROD_STAG.bc_transaction
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.bctl_transaction
                                                                                  ON              bctl_transaction.id_stg=bc_transaction.Subtype_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.bc_lineitem
                                                                                  ON              bc_lineitem.TransactionID_stg=bc_transaction.ID_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.bc_taccount
                                                                                  ON              bc_taccount.id_stg=bc_lineitem.TAccountID_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taccount
                                                                                  ON              bctl_taccount.id_stg=bc_TAccount.Subtype_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.bc_TAccountContainer
                                                                                  ON              bc_TAccountContainer.id_stg=bc_taccount.TAccountContainerID_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taccountcontainer
                                                                                  ON              bctl_taccountcontainer.id_stg=bc_TAccountContainer.Subtype_stg
                                                                                  LEFT JOIN       DB_T_PROD_STAG.bc_account
                                                                                  ON              bc_TAccountContainer.id_stg = bc_account.HiddenTAccountContainerID_stg
                                                                                  LEFT JOIN       DB_T_PROD_STAG.bc_policyperiod
                                                                                  ON              bc_TAccountContainer.id_stg = bc_policyperiod.HiddenTAccountContainerID_stg
                                                                                                  /* removed below join for performance tuning EIM-32693 */
                                                                                                  /*  LEFT JOIN DB_T_PROD_STAG.bctl_ledgerside
ON bctl_ledgerside.id_stg=bc_lineitem.Type_stg
left outer join DB_T_PROD_STAG.bc_itemevent
on bc_itemevent.TransactionID_stg = bc_transaction.id_stg
left outer join DB_T_PROD_STAG.bc_invoiceitem
on bc_invoiceitem.id_stg = bc_itemevent.InvoiceItemID_stg
LEFT OUTER JOIN DB_T_PROD_STAG.bc_invoice
ON bc_invoice.id_stg = bc_invoiceitem.InvoiceID_stg
LEFT OUTER JOIN DB_T_PROD_STAG.bc_charge
ON bc_invoiceitem.ChargeID_stg = bc_charge.ID_stg
LEFT OUTER JOIN DB_T_PROD_STAG.bc_chargepattern
ON  bc_charge.ChargePatternID_stg = bc_chargepattern.ID_stg
LEFT OUTER JOIN DB_T_PROD_STAG.bctl_chargecategory
ON bctl_chargecategory.ID_stg = bc_chargepattern.Category_stg
left join DB_T_PROD_STAG.bc_TAccountPattern
on bc_taccount.TAccountPatternID_stg = bc_TAccountPattern.id_stg
left join DB_T_PROD_STAG.bc_TAcctOwnerPattern
on bc_TAccountPattern.TAccountOwnerPatternID_stg = bc_TAcctOwnerPattern.id_stg
left join DB_T_PROD_STAG.bctl_taccountpatternsuffix
on bc_TAccountPattern.Suffix_stg = bctl_taccountpatternsuffix.ID_stg
left join DB_T_PROD_STAG.bc_invoicestream
on bc_policyperiod.PrimaryInvoiceStream_alfa_stg = bc_invoicestream.ID_stg
left join DB_T_PROD_STAG.bctl_taccounttype
on bc_TAccountPattern.TAccountType_stg = bctl_taccounttype.ID_stg*/
                                                                                  LEFT JOIN       DB_T_PROD_STAG.bc_revtrans
                                                                                  ON              bc_transaction.id_stg = bc_revtrans.ownerid_stg
                                                                                  WHERE           ((
                                                                                                                                  bc_transaction.UpdateTime_stg > (:start_dttm)
                                                                                                                  AND             bc_transaction.UpdateTime_stg <= (:end_dttm))
                                                                                                  OR              bc_policyperiod.UpdateTime_stg > (:start_dttm)
                                                                                                  AND             bc_policyperiod.UpdateTime_stg <= (:end_dttm))
                                                                                                  /* order by bc_transaction.PublicID_stg,bctl_transaction.TYPECODE_stg */
                                                                  ) bc_transaction
                                                  INNER JOIN      DB_T_PROD_STAG.bctl_transaction
                                                  ON              bctl_transaction.id_stg=bc_transaction.Subtype_stg ), temp6 AS
                                  (
                                                  /* UNION */
                                                  SELECT DISTINCT ''rvrs''
                                                                                  || ''-''
                                                                                  || bctl_transaction.TYPECODE_stg AS ev_act_type_code ,
                                                                  CAST (bc_transaction.id_stg AS VARCHAR (50))     AS key1 ,
                                                                  CAST(''EV_SBTYPE2'' AS           VARCHAR(50))      AS SUBTYPE ,
                                                                  ''SRC_SYS5''                                       AS src_sys
                                                  FROM            (
                                                                                  SELECT DISTINCT bc_transaction.ID_stg,
                                                                                                  bc_transaction.Subtype_stg,
                                                                                                  bc_revtrans.ownerid_stg
                                                                                  FROM            DB_T_PROD_STAG.bc_transaction
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.bctl_transaction
                                                                                  ON              bctl_transaction.id_stg=bc_transaction.Subtype_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.bc_lineitem
                                                                                  ON              bc_lineitem.TransactionID_stg=bc_transaction.ID_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.bc_taccount
                                                                                  ON              bc_taccount.id_stg=bc_lineitem.TAccountID_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taccount
                                                                                  ON              bctl_taccount.id_stg=bc_TAccount.Subtype_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.bc_TAccountContainer
                                                                                  ON              bc_TAccountContainer.id_stg=bc_taccount.TAccountContainerID_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.bctl_taccountcontainer
                                                                                  ON              bctl_taccountcontainer.id_stg=bc_TAccountContainer.Subtype_stg
                                                                                  LEFT JOIN       DB_T_PROD_STAG.bc_account
                                                                                  ON              bc_TAccountContainer.id_stg = bc_account.HiddenTAccountContainerID_stg
                                                                                  LEFT JOIN       DB_T_PROD_STAG.bc_policyperiod
                                                                                  ON              bc_TAccountContainer.id_stg = bc_policyperiod.HiddenTAccountContainerID_stg
                                                                                                  /* removed below join for performance tuning EIM-32693 */
                                                                                                  /*  LEFT JOIN DB_T_PROD_STAG.bctl_ledgerside
ON bctl_ledgerside.id_stg=bc_lineitem.Type_stg
left outer join DB_T_PROD_STAG.bc_itemevent
on bc_itemevent.TransactionID_stg = bc_transaction.id_stg
left outer join DB_T_PROD_STAG.bc_invoiceitem
on bc_invoiceitem.id_stg = bc_itemevent.InvoiceItemID_stg
LEFT OUTER JOIN DB_T_PROD_STAG.bc_invoice
ON bc_invoice.id_stg = bc_invoiceitem.InvoiceID_stg
LEFT OUTER JOIN DB_T_PROD_STAG.bc_charge
ON bc_invoiceitem.ChargeID_stg = bc_charge.ID_stg
LEFT OUTER JOIN DB_T_PROD_STAG.bc_chargepattern
ON  bc_charge.ChargePatternID_stg = bc_chargepattern.ID_stg
LEFT OUTER JOIN DB_T_PROD_STAG.bctl_chargecategory
ON bctl_chargecategory.ID_stg = bc_chargepattern.Category_stg
left join DB_T_PROD_STAG.bc_TAccountPattern
on bc_taccount.TAccountPatternID_stg = bc_TAccountPattern.id_stg
left join DB_T_PROD_STAG.bc_TAcctOwnerPattern
on bc_TAccountPattern.TAccountOwnerPatternID_stg = bc_TAcctOwnerPattern.id_stg
left join DB_T_PROD_STAG.bctl_taccountpatternsuffix
on bc_TAccountPattern.Suffix_stg = bctl_taccountpatternsuffix.ID_stg
left join DB_T_PROD_STAG.bc_invoicestream
on bc_policyperiod.PrimaryInvoiceStream_alfa_stg = bc_invoicestream.ID_stg
left join DB_T_PROD_STAG.bctl_taccounttype
on bc_TAccountPattern.TAccountType_stg = bctl_taccounttype.ID_stg*/
                                                                                  LEFT JOIN       DB_T_PROD_STAG.bc_revtrans
                                                                                  ON              bc_transaction.id_stg = bc_revtrans.ownerid_stg
                                                                                  WHERE           ((
                                                                                                                                  bc_transaction.UpdateTime_stg > (:start_dttm)
                                                                                                                  AND             bc_transaction.UpdateTime_stg <= (:end_dttm))
                                                                                                  OR              bc_policyperiod.UpdateTime_stg > (:start_dttm)
                                                                                                  AND             bc_policyperiod.UpdateTime_stg <= (:end_dttm))
                                                                                                  /* order by bc_transaction.PublicID_stg,bctl_transaction.TYPECODE_stg */
                                                                  ) bc_transaction
                                                  INNER JOIN      DB_T_PROD_STAG.bctl_transaction
                                                  ON              bctl_transaction.id_stg=bc_transaction.Subtype_stg
                                                  WHERE           bc_transaction.ownerid_stg IS NOT NULL
                                                  /********Reversed billing Transaction***************************/
                                                  UNION
                                                  /*************************Claim Recovery Event*******************/
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE23''             AS ev_act_type_code,
                                                                  CAST(t.ID_stg AS VARCHAR(60)) AS key1,
                                                                  ''EV_SBTYPE2''                  AS SUBTYPE ,
                                                                  ''SRC_SYS6''                    AS src_sys
                                                  FROM            (
                                                                                  SELECT          a.ID_stg,
                                                                                                  a.Subtype_stg,
                                                                                                  cctl_transactionstatus.typecode_stg AS typecode_stg,
                                                                                                  a.CheckNum_alfa_stg
                                                                                  FROM            DB_T_PROD_STAG.cc_transaction a
                                                                                  JOIN            DB_T_PROD_STAG.cctl_transactionstatus
                                                                                  ON              a .status_stg= DB_T_PROD_STAG.cctl_transactionstatus .ID_stg
                                                                                  JOIN
                                                                                                  (
                                                                                                             SELECT
                                                                                                                        /*  cc_claim.*  */
                                                                                                                        cc_claim.ID_stg,
                                                                                                                        cc_claim.PolicyID_stg
                                                                                                             FROM       DB_T_PROD_STAG.cc_claim
                                                                                                             INNER JOIN DB_T_PROD_STAG.cctl_claimstate
                                                                                                             ON         cc_claim.State_stg= cctl_claimstate.id_stg
                                                                                                             WHERE      cctl_claimstate.name_stg <> ''Draft'') cc_claim
                                                                                  ON              cc_claim.ID_stg=a.ClaimID_stg
                                                                                  JOIN            DB_T_PROD_STAG.cc_policy
                                                                                  ON              cc_claim.PolicyID_stg=cc_policy.ID_stg
                                                                                  LEFT OUTER JOIN DB_T_PROD_STAG.cc_check
                                                                                  ON              cc_check.id_stg = a.CheckID_stg
                                                                                                  /* removed below join for performance tuning EIM-32693 */
                                                                                                  /*  LEFT OUTER JOIN DB_T_PROD_STAG.cc_exposure    ON cc_exposure.id_stg=a.exposureid_stg
LEFT OUTER JOIN DB_T_PROD_STAG.cc_user    ON a.CreateUserID_stg = cc_user.id_stg
LEFT OUTER JOIN DB_T_PROD_STAG.cc_contact   ON cc_user.ContactID_stg = cc_contact.id_stg*/
                                                                                  WHERE           ((
                                                                                                                                  a.UpdateTime_stg >(:start_dttm)
                                                                                                                  AND             a.UpdateTime_stg <= (:end_dttm))
                                                                                                  OR              (
                                                                                                                                  cc_check.UpdateTime_stg >(:start_dttm)
                                                                                                                  AND             cc_check.UpdateTime_stg <= (:end_dttm)))
                                                                                                  /* order by publicid_stg */
                                                                  ) t
                                                  JOIN            DB_T_PROD_STAG.cctl_transaction tl
                                                  ON              tl.id_stg=t.subtype_stg
                                                  AND             tl.typecode_stg=''Recovery''
                                                  WHERE           t.CheckNum_alfa_stg IS NOT NULL
                                                  /**********************************************************************************************/
                                                  UNION
                                                  /****************************************Payment Request****************************/
                                                  SELECT DISTINCT CAST(''EV_ACTVY_TYPE35'' AS        VARCHAR(50)) AS ev_act_type_code ,
                                                                  CAST( bc_invoicestream.id_stg AS VARCHAR(60)) AS key1 ,
                                                                  CAST(''EV_SBTYPE2'' AS             VARCHAR(50)) AS SUBTYPE ,
                                                                  ''SRC_SYS5''                                    AS src_sys
                                                  FROM            (
                                                                         SELECT bc_invoicestream.ID_stg,
                                                                                bc_invoicestream.OverridingPayer_alfa_stg
                                                                         FROM   DB_T_PROD_STAG.bc_invoicestream
                                                                         WHERE  bc_invoicestream.UpdateTime_stg > (:start_dttm)
                                                                         AND    bc_invoicestream.UpdateTime_stg <= (:end_dttm) ) bc_invoicestream
                                                  WHERE           OverridingPayer_alfa_stg IS NOT NULL
                                                  UNION
                                                  SELECT DISTINCT CAST(''EV_ACTVY_TYPE35'' AS         VARCHAR(50)) AS ev_act_type_code ,
                                                                  CAST( bc_paymentrequest.id_stg AS VARCHAR(60)) AS key1 ,
                                                                  CAST(''EV_SBTYPE2'' AS              VARCHAR(50)) AS SUBTYPE ,
                                                                  ''SRC_SYS5''                                     AS src_sys
                                                  FROM            (
                                                                         SELECT bc_paymentrequest.ID_stg
                                                                         FROM   DB_T_PROD_STAG.bc_paymentrequest
                                                                         JOIN   DB_T_PROD_STAG.bctl_paymentrequeststatus
                                                                         ON     bc_paymentrequest.Status_stg = bctl_paymentrequeststatus.ID_stg
                                                                                /* removed below join for performance tuning EIM-32693 */
                                                                                /*   left join DB_T_PROD_STAG.bc_paymentinstrument    on bc_paymentrequest.paymentinstrumentid_stg = bc_paymentinstrument.id_stg
left join DB_T_PROD_STAG.bctl_paymentmethod    on bc_paymentinstrument.paymentmethod_stg = bctl_paymentmethod.id_stg
left join DB_T_PROD_STAG.bc_invoice    on bc_paymentrequest.invoiceid_stg=bc_invoice.ID_stg*/
                                                                         WHERE  bc_paymentrequest.UpdateTime_stg > (:start_dttm)
                                                                         AND    bc_paymentrequest.UpdateTime_stg <= (:end_dttm) ) bc_paymentrequest
                                                  UNION
                                                  SELECT DISTINCT ''EV_ACTVY_TYPE36''            AS ev_act_type_code ,
                                                                  CAST( id_stg AS VARCHAR(60)) AS key1 ,
                                                                  ''EV_SBTYPE2''                 AS SUBTYPE ,
                                                                  ''SRC_SYS5''                   AS src_sys
                                                  FROM            (
                                                                                  SELECT DISTINCT bc_unappliedfund.ID_stg
                                                                                  FROM            DB_T_PROD_STAG.bc_unappliedfund
                                                                                  LEFT JOIN       DB_T_PROD_STAG.bc_taccount
                                                                                  ON              bc_unappliedfund.taccountid_stg = bc_taccount.id_stg
                                                                                                  /* removed below join for performance tuning EIM-32693 */
                                                                                                  /*  left join  DB_T_PROD_STAG.bc_account    ON bc_unappliedfund.AccountID_stg = bc_account.ID_stg
left join DB_T_PROD_STAG.bc_TAccountPattern    on bc_taccount.TAccountPatternID_stg = bc_TAccountPattern.id_stg
left join DB_T_PROD_STAG.bc_TAcctOwnerPattern on bc_TAccountPattern.TAccountOwnerPatternID_stg = bc_TAcctOwnerPattern.id_stg
left join DB_T_PROD_STAG.bctl_taccountpatternsuffix on bc_TAccountPattern.Suffix_stg = bctl_taccountpatternsuffix.ID_stg
left join DB_T_PROD_STAG.bctl_taccounttype on bc_TAccountPattern.TAccountType_stg = bctl_taccounttype.ID_stg */
                                                                                  WHERE           (
                                                                                                                  bc_unappliedfund.UpdateTime_stg > (:start_dttm)
                                                                                                  AND             bc_unappliedfund.UpdateTime_stg <= (:end_dttm))
                                                                                  OR              (
                                                                                                                  bc_taccount.UpdateTime_stg > (:start_dttm)
                                                                                                  AND             bc_taccount.UpdateTime_stg <= (:end_dttm)) ) bc_unappliedfund )
                  SELECT src_trans_id,
                         ev_sbtype_cd,
                         ev_actvy_type_cd,
                         src_cd,
                         ins_upd_flag
                  FROM   (
                                         SELECT DISTINCT in_src_trans_id    AS src_trans_id ,
                                                         o_ev_sbtype_cd     AS ev_sbtype_cd ,
                                                         o_ev_actvy_type_cd AS ev_actvy_type_cd ,
                                                         o_src_cd           AS src_cd
                                                         /* ,TGT_DIR_EV.ev_id as dir_ev_id */
                                                         ,
                                                         CASE
                                                                         WHEN TGT_DIR_EV.EV_ID IS NULL THEN ''I''
                                                                         WHEN TGT_DIR_EV.EV_ID IS NOT NULL THEN ''R''
                                                         END AS ins_upd_flag
                                         FROM            (
                                                                         SELECT          SRC.ev_act_type_code AS in_ev_actvy_type_cd ,
                                                                                         SRC.key1             AS in_src_trans_id ,
                                                                                         SRC.SUBTYPE          AS in_ev_sbtype_cd ,
                                                                                         src_sys ,
                                                                                         XLAT_SRC_CD.TGT_IDNTFTN_VAL        AS o_src_cd ,
                                                                                         XLAT_EV_SBTYPE.TGT_IDNTFTN_VAL     AS o_ev_sbtype_cd ,
                                                                                         XLAT_EV_ACTVY_TYPE.TGT_IDNTFTN_VAL AS o_ev_actvy_type_cd
                                                                         FROM            (
                                                                                                SELECT ev_act_type_code,
                                                                                                       key1,
                                                                                                       SUBTYPE,
                                                                                                       src_sys 
                                                                                                FROM   (
                                                                                                              SELECT *
                                                                                                              FROM   temp1
                                                                                                              UNION ALL
                                                                                                              SELECT *
                                                                                                              FROM   temp2
                                                                                                              UNION ALL
                                                                                                              SELECT *
                                                                                                              FROM   temp3
                                                                                                              UNION ALL
                                                                                                              SELECT *
                                                                                                              FROM   temp4
                                                                                                              UNION ALL
                                                                                                              SELECT *
                                                                                                              FROM   temp5
                                                                                                              UNION ALL
                                                                                                              SELECT *
                                                                                                              FROM   temp6 )x ) SRC
                                                                                         /* LKP_TERDATA_ETL_XLAT_SRC_CD */
                                                                         LEFT OUTER JOIN
                                                                                         (
                                                                                                SELECT TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL ,
                                                                                                       TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL
                                                                                                FROM   DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
                                                                                                WHERE  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS''
                                                                                                AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived''
                                                                                                AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS''
                                                                                                AND    TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' ) XLAT_SRC_CD
                                                                         ON                     src_sys=XLAT_SRC_CD.SRC_IDNTFTN_VAL
                                                                                         /* LKP_TERDATA_ETL_XLAT_EV_SBTYPE   */
                                                                         LEFT OUTER JOIN
                                                                                         (
                                                                                                SELECT TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL ,
                                                                                                       TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL
                                                                                                FROM   DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
                                                                                                WHERE  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''EV_SBTYPE''
                                                                                                AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived''
                                                                                                AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS''
                                                                                                AND    TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' ) XLAT_EV_SBTYPE
                                                                         ON              SRC.SUBTYPE=XLAT_EV_SBTYPE.SRC_IDNTFTN_VAL
                                                                                         /* LKP_TERDATA_ETL_XLAT_EV_ACTVY_TYPE   */
                                                                         LEFT OUTER JOIN
                                                                                         (
                                                                                                SELECT TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL ,
                                                                                                       TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL
                                                                                                FROM   DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
                                                                                                WHERE  TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''EV_ACTVY_TYPE''
                                                                                                AND    TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''GW'',
                                                                                                                                                 ''DS'' )
                                                                                                AND    TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'' ) XLAT_EV_ACTVY_TYPE
                                                                         ON              SRC.ev_act_type_code=XLAT_EV_ACTVY_TYPE.SRC_IDNTFTN_VAL ) XLAT_SRC
                                         LEFT OUTER JOIN
                                                         (
                                                                SELECT DIR_EV.EV_ID            AS EV_ID,
                                                                       DIR_EV.SRC_TRANS_ID     AS SRC_TRANS_ID,
                                                                       DIR_EV.EV_SBTYPE_CD     AS EV_SBTYPE_CD,
                                                                       DIR_EV.EV_ACTVY_TYPE_CD AS EV_ACTVY_TYPE_CD
                                                                FROM   DB_T_PROD_CORE.DIR_EV ) TGT_DIR_EV
                                         ON              TGT_DIR_EV.SRC_TRANS_ID=XLAT_SRC.in_src_trans_id
                                         AND             TGT_DIR_EV.EV_SBTYPE_CD=XLAT_SRC.o_ev_sbtype_cd
                                         AND             TGT_DIR_EV.EV_ACTVY_TYPE_CD=XLAT_SRC.o_ev_actvy_type_cd )a
                  WHERE  a.ins_upd_flag=''I'' ) SRC ) );
  -- Component exp_doc_trans, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE exp_doc_trans AS
  (
         SELECT sq_ev.src_trans_id     AS src_trans_id,
                sq_ev.ev_sbtype_cd     AS ev_sbtype_cd,
                sq_ev.ev_actvy_type_cd AS ev_actvy_type_cd,
                sq_ev.src_cd           AS src_cd,
                sq_ev.ins_upd_flad     AS ins_upd_flag,
                CURRENT_TIMESTAMP      AS load_dttm,
                sq_ev.source_record_id
         FROM   sq_ev );
  -- Component fil_xref_ev, Type FILTER
  CREATE
  OR
  REPLACE TEMPORARY TABLE fil_xref_ev AS
  (
         SELECT exp_doc_trans.src_trans_id     AS src_trans_id,
                exp_doc_trans.ev_sbtype_cd     AS ev_sbtype_cd,
                exp_doc_trans.ev_actvy_type_cd AS ev_actvy_type_cd,
                exp_doc_trans.src_cd           AS src_cd,
                exp_doc_trans.load_dttm        AS load_dttm,
                exp_doc_trans.ins_upd_flag     AS ins_upd_flag,
                exp_doc_trans.source_record_id
         FROM   exp_doc_trans
         WHERE  exp_doc_trans.ins_upd_flag = ''I'' );
  -- Component DIR_EV, Type TARGET
  INSERT INTO DB_T_PROD_CORE.DIR_EV
              (
                          EV_ID,
                          SRC_TRANS_ID,
                          EV_SBTYPE_CD,
                          EV_ACTVY_TYPE_CD,
                          SRC_SYS_CD,
                          LOAD_DTTM
              )
  SELECT   row_number() over (ORDER BY 1) AS EV_ID,
           fil_xref_ev.src_trans_id       AS SRC_TRANS_ID,
           fil_xref_ev.ev_sbtype_cd       AS EV_SBTYPE_CD,
           fil_xref_ev.ev_actvy_type_cd   AS EV_ACTVY_TYPE_CD,
           fil_xref_ev.src_cd             AS SRC_SYS_CD,
           fil_xref_ev.load_dttm          AS LOAD_DTTM
  FROM     fil_xref_ev;

END;
';