-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_EV_RLTD_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
FS_DATE date;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);
 
start_dttm := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);
end_dttm := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
PRCS_ID := public.func_get_scoped_param(:run_id, ''PRCS_ID'', :workflow_name, :worklet_name, :session_name);  
FS_DATE := public.func_get_scoped_param(:run_id, ''FS_DATE'', :workflow_name, :worklet_name, :session_name);

  CREATE OR REPLACE TEMPORARY TABLE SQ_bc_ev_rltd_x AS (
    SELECT  RLTD_RSN_CD,STRT_DT,END_DT,RETIRED,OUT_EV_ID,OUT_EV_RLTD_ID,EV_ID, RLTD_EV_ID,EV_RLTD_RSN_CD,EDW_STRT_DTTM,EDW_END_DTTM,IN_EV_RLTD_END_DTM,IN_EDW_END_DTM,
      CASE 
            WHEN V_LKP_CHECKUSUM IS NULL THEN ''I''
              WHEN V_IN_CHECKUSUM = V_LKP_CHECKUSUM THEN ''R''
              ELSE ''U''
            END AS FLAG FROM (

      SELECT  RLTD_RSN_CD,STRT_DT,END_DT,RETIRED,OUT_EV_ID,OUT_EV_RLTD_ID,EV_ID, RLTD_EV_ID,EV_RLTD_RSN_CD,EV_RLTD_STRT_DTTM,EDW_STRT_DTTM,EDW_END_DTTM,IN_EV_RLTD_END_DTM,IN_EDW_END_DTM,
              (CAST(STRT_DT AS VARCHAR(50))) AS V_IN_CHECKUSUM,
              (CAST(EV_RLTD_STRT_DTTM AS VARCHAR(50))) AS V_LKP_CHECKUSUM FROM (

      SELECT PARENT_EV_ACT_TYPE_CD,PARENTKEY,PARENT_SUBTYPE,CHILD_EV_ACT_TYPE_CD,CHILDKEY, CHILD_SUBTYPE,RLTD_RSN_CD
      ,STRT_DT,END_DT,RETIRED, PARENT_EV_ID.EV_ID AS OUT_EV_ID,CHILD_EV_RLTD.EV_ID AS OUT_EV_RLTD_ID,IN_EV_RLTD_END_DTM,IN_EDW_END_DTM


      FROM ( SELECT
        XLAT_PARENT_EV_ACT_TYPE_CD.TGT_IDNTFTN_VAL AS PARENT_EV_ACT_TYPE_CD
      , EV_X.PARENTKEY AS PARENTKEY
      , XLAT_PARENT_SUBTYPE.TGT_IDNTFTN_VAL AS PARENT_SUBTYPE
      , XLAT_CHILD_EV_ACT_TYPE_CD.TGT_IDNTFTN_VAL AS CHILD_EV_ACT_TYPE_CD
      , EV_X.CHILDKEY AS CHILDKEY
      , XLAT_CHILD_SUBTYPE.TGT_IDNTFTN_VAL AS CHILD_SUBTYPE 
      , XLAT_RLTD_RSN_CD.TGT_IDNTFTN_VAL AS RLTD_RSN_CD
      , EV_X.STRT_DT AS STRT_DT
      , EV_X.END_DT AS END_DT
      , EV_X.RETIRED AS RETIRED
      ,TO_TIMESTAMP_NTZ(''9999-12-31 23:59:59.999999'') AS IN_EV_RLTD_END_DTM
      ,TO_TIMESTAMP_NTZ(''9999-12-31 23:59:59.999999'') AS IN_EDW_END_DTM
      FROM
      (
        
      SELECT  parent_ev_act_type_code_stg AS parent_ev_act_type_cd
      ,parent_key1_stg AS parentKey
      ,parent_subtype_stg AS parent_subtype
      ,child_ev_act_type_code_stg AS child_ev_act_type_cd
      ,child_key1_stg AS childkey
      ,child_subtype_stg AS child_subtype
      ,rltd_rsn_cd_stg AS rltd_rsn_cd
      ,strt_dt_stg AS strt_dt
      ,end_dt_stg AS end_dt
      ,Retired_stg AS Retired

      FROM 
      (
      SELECT
      Cast(''EV_ACTVY_TYPE14''  AS VARCHAR(100)) AS parent_ev_act_type_code_stg
      ,bc_basemoneyreceived.id_stg AS parent_key1_stg
      ,Cast( ''EV_SBTYPE2'' AS VARCHAR(100)) AS parent_subtype_stg
      ,Cast( ''EV_ACTVY_TYPE25'' AS VARCHAR(100)) AS child_ev_act_type_code_stg
      , bc_basemoneyreceived.id_stg AS child_key1_stg
      ,Cast(''EV_SBTYPE2'' AS VARCHAR(100)) AS child_subtype_stg
      ,Cast(''EV_RLTD_RSN5'' AS VARCHAR(100)) AS rltd_rsn_cd_stg
      ,bc_basemoneyreceived.createtime_stg AS strt_dt_stg
      ,bc_basemoneyreceived.UPDATETIME_stg AS end_dt_stg,
      bc_basemoneyreceived.Retired_stg AS Retired_stg
      FROM    bc_basemoneyreceived 
      INNER JOIN bctl_basemoneyreceived 
          ON bctl_basemoneyreceived.id_stg =bc_basemoneyreceived.Subtype_stg
      WHERE   bctl_basemoneyreceived.typecode_stg IN (''PaymentMoneyReceived'',
              ''DirectBillMoneyRcvd'',''ZeroDollarDMR'',''ZeroDollarReversal'')
          AND bc_basemoneyreceived.reversaldate_stg IS NOT NULL
      AND bc_basemoneyreceived.UpdateTime_stg > TO_TIMESTAMP_NTZ(:start_dttm)
          AND bc_basemoneyreceived.UpdateTime_stg <= TO_TIMESTAMP_NTZ(:end_dttm)

      UNION

      SELECT
      Cast(''EV_ACTVY_TYPE14'' AS VARCHAR(100)) AS parent_ev_act_type_code_stg
      ,bc_basemoneyreceived.id_stg AS parent_key1_stg
      ,Cast(''EV_SBTYPE2'' AS VARCHAR(100)) AS parent_subtype_stg
      ,Cast(''EV_ACTVY_TYPE26''  AS VARCHAR(100)) AS child_ev_act_type_code_stg
      ,bc_basedistitem.id_stg AS child_key1_stg
      ,Cast(''EV_SBTYPE2'' AS VARCHAR(100)) AS child_subtype_stg
      ,Cast(''EV_RLTD_RSN5'' AS VARCHAR(100)) AS EV_RLTD_RSN5_stg
      ,bc_basemoneyreceived.createtime_stg AS strt_dt_stg
      , bc_basemoneyreceived.UPDATETIME_stg AS end_dt_stg,
      CASE  WHEN bc_basemoneyreceived.Retired_stg=0 AND bc_basedistitem.Retired_stg=0 AND bc_basedist.Retired_stg=0 THEN 0 
      ELSE 1 
      end AS Retired_stg
      FROM    bc_basemoneyreceived 
      INNER JOIN bctl_basemoneyreceived 
          ON bctl_basemoneyreceived.id_stg =bc_basemoneyreceived.Subtype_stg
      INNER JOIN bc_basedist 
          ON bc_basedist.id_stg=bc_basemoneyreceived.BaseDistID_stg
      INNER JOIN bc_basedistitem 
          ON bc_basedistitem.ActiveDistID_stg=bc_basedist.id_stg
      WHERE   bctl_basemoneyreceived.typecode_stg IN (''PaymentMoneyReceived'',
              ''DirectBillMoneyRcvd'',''ZeroDollarDMR'',''ZeroDollarReversal'')
      AND bc_basemoneyreceived.UpdateTime_stg > TO_TIMESTAMP_NTZ(:start_dttm)
          AND bc_basemoneyreceived.UpdateTime_stg <= TO_TIMESTAMP_NTZ(:end_dttm)


      UNION

      SELECT
      Cast(''EV_ACTVY_TYPE14'' AS VARCHAR(100)) AS parent_ev_act_type_code_stg
      ,bc_basemoneyreceived.id_stg AS parent_key1_stg
      ,Cast(''EV_SBTYPE2'' AS VARCHAR(100)) AS parent_subtype_stg
      ,Cast(''EV_ACTVY_TYPE27'' AS VARCHAR(100)) AS child_ev_act_type_code_stg
      ,bc_basedistitem.id_stg AS child_key1_stg
      ,Cast(''EV_SBTYPE2'' AS VARCHAR(100)) AS child_subtype_stg
      ,Cast(''EV_RLTD_RSN5''  AS VARCHAR(100)) AS rltd_rsn_cd_stg
      ,bc_basemoneyreceived.createtime_stg AS strt_dt_stg
      , bc_basemoneyreceived.updatetime_stg AS end_dt_stg,
      CASE  WHEN bc_basemoneyreceived.Retired_stg=0  AND bc_basedistitem.Retired_stg=0  AND bc_basedist.Retired_stg=0 THEN 0 
      ELSE 1 
      end AS Retired_stg
      FROM    bc_basemoneyreceived 
      INNER JOIN bctl_basemoneyreceived 
          ON bctl_basemoneyreceived.id_stg=bc_basemoneyreceived.Subtype_stg
      INNER JOIN bc_basedist 
          ON bc_basedist.id_stg=bc_basemoneyreceived.BaseDistID_stg
      INNER JOIN bc_basedistitem 
          ON bc_basedistitem.ReversedDistID_stg=bc_basedist.id_stg
      WHERE   bctl_basemoneyreceived.typecode_stg IN (''PaymentMoneyReceived'',
              ''DirectBillMoneyRcvd'',''ZeroDollarDMR'',''ZeroDollarReversal'')
          AND bc_basedistitem.ReversedDate_stg IS NOT NULL
        AND bc_basemoneyreceived.UpdateTime_stg > TO_TIMESTAMP_NTZ(:start_dttm)
          AND bc_basemoneyreceived.UpdateTime_stg <= TO_TIMESTAMP_NTZ(:end_dttm)


      UNION

      SELECT
      Cast(''EV_ACTVY_TYPE28''  AS VARCHAR(100)) AS parent_ev_act_type_code_stg
      , bc_basenonrecdistitem.id_stg AS parent_key1_stg
      ,Cast(''EV_SBTYPE2'' AS VARCHAR(100)) AS parent_subtype_stg
      ,Cast(''EV_ACTVY_TYPE29''  AS VARCHAR(100)) AS child_ev_act_type_code_stg
      ,bc_basenonrecdistitem.id_stg AS  child_key1_stg
      ,Cast(''EV_SBTYPE2'' AS VARCHAR(100)) AS child_subtype_stg
      ,Cast(''EV_RLTD_RSN6'' AS VARCHAR(100)) AS rltd_rsn_cd_stg
      ,bc_basenonrecdistitem.createtime_stg AS strt_dt_stg
      , bc_basenonrecdistitem.updatetime_stg AS end_dt_stg,
      bc_basenonrecdistitem.Retired_stg AS Retired_stg
      FROM    bc_basenonrecdistitem
      WHERE   bc_basenonrecdistitem.ReversedDate_stg IS NOT NULL
        AND bc_basenonrecdistitem.UpdateTime_stg > TO_TIMESTAMP_NTZ(:start_dttm)
          AND bc_basenonrecdistitem.UpdateTime_stg <= TO_TIMESTAMP_NTZ(:end_dttm)


      UNION

      SELECT
      Cast(''EV_ACTVY_TYPE32'' AS VARCHAR(100)) AS parent_ev_act_type_code_stg
      , bc_disbursement.id_stg AS parent_key1_stg
      ,Cast(''EV_SBTYPE2'' AS VARCHAR(100)) AS parent_subtype_stg
      ,Cast(''EV_ACTVY_TYPE31''  AS VARCHAR(100)) AS child_ev_act_type_code_stg
      , bc_outgoingpayment.id_stg AS child_key1_stg
      , Cast(''EV_SBTYPE2''  AS VARCHAR(100)) AS child_subtype_stg
      ,Cast(''EV_RLTD_RSN7'' AS VARCHAR(100)) AS rltd_rsn_cd_stg
      ,bc_outgoingpayment.createtime_stg AS strt_dt_stg
      , bc_outgoingpayment.updatetime_stg AS end_dt_stg,
      CASE WHEN bc_outgoingpayment.Retired_stg=0 AND bc_disbursement.Retired_stg=0 THEN 0  ELSE 1 
      end AS Retired_stg
      FROM    bc_outgoingpayment 
      INNER JOIN bc_disbursement 
          ON bc_disbursement.id_stg=bc_outgoingpayment.DisbursementID_stg
      WHERE   bc_outgoingpayment.UpdateTime_stg > TO_TIMESTAMP_NTZ(:start_dttm)
          AND bc_outgoingpayment.UpdateTime_stg <= TO_TIMESTAMP_NTZ(:end_dttm)


      UNION

      SELECT
      Cast(''EV_ACTVY_TYPE33'' AS VARCHAR(100)) AS parent_ev_act_type_code_stg
      ,bc_writeoff.id_stg AS parent_key1_stg
      ,Cast(''EV_SBTYPE2''  AS VARCHAR(100)) AS parent_subtype_stg
      ,Cast(''EV_ACTVY_TYPE34''  AS VARCHAR(100)) AS child_ev_act_type_code_stg
      ,reversal.id_stg AS child_key1_stg
      ,Cast(''EV_SBTYPE2'' AS VARCHAR(100)) AS child_subtype_stg
      ,Cast(''EV_RLTD_RSN10'' AS VARCHAR(100)) AS rltd_rsn_cd_stg
      ,bc_writeoff.createtime_stg AS  strt_dt_stg
      ,bc_writeoff.updatetime_stg  AS end_dt_stg,
      CASE WHEN bc_writeoff.Retired_stg=0 AND reversal.Retired_stg=0 THEN 0 
      ELSE 1 
      end AS Retired_stg
      FROM    (
          SELECT  * 
          FROM    bc_writeoff 
          WHERE   bc_writeoff.id_stg  IN (
              SELECT  OwnerID_stg
              FROM    bc_revwriteoff)--reversed=1
      ) reversal
      INNER JOIN
      bc_revwriteoff 
          ON bc_revwriteoff.OwnerID_stg=reversal.id_stg
      INNER JOIN
      (
          SELECT  * 
          FROM    bc_writeoff 
          WHERE   bc_writeoff.id_stg NOT IN (
              SELECT  OwnerID_stg 
              FROM    bc_revwriteoff) ) bc_writeoff 
          ON bc_writeoff.id_stg=bc_revwriteoff.ForeignEntityID_stg
      WHERE   bc_writeoff.UpdateTime_stg > TO_TIMESTAMP_NTZ(:start_dttm)
          AND bc_writeoff.UpdateTime_stg <= TO_TIMESTAMP_NTZ(:end_dttm)
        

      UNION

      /***************************Billing Transaction*************************/
      SELECT  DISTINCT
      bctl_transaction.TYPECODE_stg AS parent_ev_act_type_code_stg
      , bc_transaction.id_stg AS parent_key1_stg
      ,Cast(''EV_SBTYPE2''  AS VARCHAR(100))AS parent_subtype_stg
      , Cast(''rvrs-''||bctl_transaction.TYPECODE_stg AS VARCHAR(100)) AS child_ev_act_type_code_stg
      , bc_transaction.id_stg AS child_key1_stg
      ,Cast(''EV_SBTYPE2''  AS VARCHAR(100))AS child_subtype_stg
      , Cast(''EV_RLTD_RSN9'' AS VARCHAR(100)) AS rltd_rsn_cd_stg                            --RVRSTRANS
      , bc_transaction.createtime_stg AS strt_dt_stg
      , bc_transaction.updatetime_stg AS end_dt_stg,
      bc_transaction.Retired_stg AS Retired_stg
      FROM    bc_transaction 
      INNER JOIN bctl_transaction 
          ON bctl_transaction.id_stg=bc_transaction.Subtype_stg
      LEFT JOIN bc_revtrans 
          ON bc_transaction.id_stg = bc_revtrans.ownerid_stg
      WHERE   bc_transaction.UpdateTime_stg > TO_TIMESTAMP_NTZ(:start_dttm)
          AND bc_transaction.UpdateTime_stg <= TO_TIMESTAMP_NTZ(:end_dttm) 
      /*******************************************************************************/
      union
      SELECT distinct Cast(''AcctUnappliedSuspTxn'' AS VARCHAR(100)) AS parent_ev_act_type_code_stg ,
      max(case when ( bctt.typecode_stg =''AcctUnappliedSuspTxn'') then tx.id_stg end) over(partition by nrdi.id_stg  order by 1)AS parent_key1_stg ,
      Cast(''EV_SBTYPE2'' AS VARCHAR(100)) AS parent_subtype_stg ,
      Cast(''AcctSuspenseReleaseTxn'' AS VARCHAR(100)) AS child_ev_act_type_code_stg ,
      max(case when ( bctt.typecode_stg =''AcctSuspenseReleaseTxn'') then tx.id_stg end) over(partition by nrdi.id_stg  order by 1) AS child_key1_stg ,
      Cast(''EV_SBTYPE2'' AS VARCHAR(100)) AS child_subtype_stg ,
      Cast(''EV_RLTD_RSN6'' AS VARCHAR(100)) AS rltd_rsn_cd_stg ,
      nrdi.createtime_stg AS strt_dt_stg ,
      nrdi.updatetime_stg AS end_dt_stg,
      case when nrdi.Retired_stg<>0  then min(tx.retired_stg) over(partition by nrdi.id_stg  order by 1) else nrdi.Retired_stg end AS Retired_stg
      FROM bc_basenonrecdistitem nrdi
      join bc_nonreceivableitemctx ctx on ctx.BaseNonReceivableDistItemID_stg = nrdi.ID_stg
      join bc_transaction tx on tx.ID_stg = ctx.TransactionID_stg
      join bctl_transaction bctt on bctt.id_stg=tx.subtype_stg
      WHERE nrdi.ReleasedDate_stg IS NOT NULL --and parent_key1_stg is not null and child_key1_stg is not null
      and nrdi.UpdateTime_stg > TO_TIMESTAMP_NTZ(:start_dttm)
          AND nrdi.UpdateTime_stg <= TO_TIMESTAMP_NTZ(:end_dttm)


      UNION
      SELECT  DISTINCT 
      Cast(''EV_ACTVY_TYPE14'' AS VARCHAR(100)) AS parent_ev_act_type_code_stg
      , bc_basemoneyreceived.id_stg AS parent_key1_stg
      ,Cast(''EV_SBTYPE2''  AS VARCHAR(100)) AS parent_subtype_stg
      , Cast(''EV_ACTVY_TYPE36''  AS VARCHAR(100)) AS child_ev_act_type_code_stg
      ,bc_unappliedfund.id_stg AS child_key1_stg
      , Cast(''EV_SBTYPE2''  AS VARCHAR(100)) AS child_subtype_stg
      ,Cast(''EV_RLTD_RSN5'' AS VARCHAR(100)) AS rltd_rsn_cd_stg
      , bc_basemoneyreceived.createtime_stg AS strt_dt_stg
      , bc_basemoneyreceived.UPDATETIME_stg AS end_dt_stg,
      bc_basemoneyreceived.Retired_stg AS Retired_stg
      FROM    bc_basemoneyreceived
      INNER JOIN bc_unappliedfund 
          ON bc_basemoneyreceived.UnappliedFundID_stg=bc_unappliedfund.ID_stg 
      AND bc_basemoneyreceived.UpdateTime_stg > TO_TIMESTAMP_NTZ(:start_dttm)
          AND bc_basemoneyreceived.UpdateTime_stg <= TO_TIMESTAMP_NTZ(:end_dttm)

      UNION
      SELECT
      Cast(''EV_ACTVY_TYPE26''  AS VARCHAR(100)) AS parent_ev_act_type_code_stg
      , bc_basedistitem.id_stg AS parent_key1_stg
      ,Cast(''EV_SBTYPE2''  AS VARCHAR(100)) AS parent_subtype_stg
      ,Cast(''EV_ACTVY_TYPE27''  AS VARCHAR(100)) AS child_ev_act_type_code_stg
      ,rev_dis.id_stg AS child_key1_stg
      , Cast(''EV_SBTYPE2'' AS VARCHAR(100)) AS child_subtype_stg
      ,Cast(''EV_RLTD_RSN5''  AS VARCHAR(100)) AS EV_RLTD_RSN5_stg
      , bc_basedistitem.createtime_stg AS strt_dt_stg
      , bc_basedistitem.UPDATETIME_stg AS end_dt_stg,
      bc_basedistitem.Retired_stg
      FROM
      bc_basedistitem 
      JOIN (
          SELECT  * 
          FROM    bc_basedistitem 
          WHERE   bc_basedistitem.ReversedDate_stg IS NOT NULL) rev_dis
          ON rev_dis.id_stg=bc_basedistitem.id_stg
        AND bc_basedistitem.UpdateTime_stg > TO_TIMESTAMP_NTZ(:start_dttm)
          AND bc_basedistitem.UpdateTime_stg <= TO_TIMESTAMP_NTZ(:end_dttm)
        
      UNION

      ---EIM_46113 New Union for reversed suspense items ev_rltd join 
      SELECT distinct Cast(''AcctUnappliedSuspTxn'' AS VARCHAR(100)) AS parent_ev_act_type_code_stg,
      max(case when ( bctt.typecode_stg =''AcctUnappliedSuspTxn'' and tx.reversed_stg = 1) then tx.id_stg end) over(partition by nrdi.id_stg order by 1)AS parent_key1_stg ,
      Cast(''EV_SBTYPE2'' AS VARCHAR(100)) AS parent_subtype_stg ,
      Cast(''AcctUnappliedSuspTxn'' AS VARCHAR(100)) AS child_ev_act_type_code_stg ,
      max(case when ( bctt.typecode_stg =''AcctUnappliedSuspTxn'' and tx.reversed_stg = 0 ) then tx.id_stg end) over(partition by nrdi.id_stg order by 1) AS child_key1_stg ,
      Cast(''EV_SBTYPE2'' AS VARCHAR(100)) AS child_subtype_stg , 
      Cast(''EV_RLTD_RSN6'' AS VARCHAR(100)) AS rltd_rsn_cd_stg , 
      nrdi.createtime_stg AS strt_dt_stg , 
      nrdi.updatetime_stg AS end_dt_stg,
      case when nrdi.Retired_stg<>0 then min(tx.retired_stg) over(partition by nrdi.id_stg order by 1) else nrdi.Retired_stg end AS Retired_stg
      FROM bc_basenonrecdistitem nrdi
      join bc_nonreceivableitemctx ctx on ctx.BaseNonReceivableDistItemID_stg = nrdi.ID_stg
      join bc_transaction tx on tx.ID_stg = ctx.TransactionID_stg
      join bctl_transaction bctt on bctt.id_stg=tx.subtype_stg
      WHERE nrdi.ReleasedDate_stg IS NULL
      and nrdi.reverseddistid_stg IS NOT NULL 
      and nrdi.UpdateTime_stg > TO_TIMESTAMP_NTZ(:start_dttm)
      AND nrdi.UpdateTime_stg <= TO_TIMESTAMP_NTZ(:end_dttm)	
      )a

      )
      EV_X
      LEFT OUTER JOIN 
      (SELECT 
        TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL
        ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL 
      FROM 
        DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
      WHERE 
        TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''EV_ACTVY_TYPE'' 
          AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''GW'',''DS'' )
          AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'') XLAT_CHILD_EV_ACT_TYPE_CD
          ON XLAT_CHILD_EV_ACT_TYPE_CD.SRC_IDNTFTN_VAL=EV_X.CHILD_EV_ACT_TYPE_CD
      LEFT OUTER JOIN (SELECT 
        TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL
        ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL 
      FROM 	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
      WHERE 
        TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''EV_SBTYPE'' 
              AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM = ''DERIVED''
          AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 
          AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'')XLAT_PARENT_SUBTYPE
          ON XLAT_PARENT_SUBTYPE.SRC_IDNTFTN_VAL=EV_X.PARENT_SUBTYPE
      LEFT OUTER JOIN (SELECT 
        TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL
        ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL 
      FROM 
        DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
      WHERE 
        TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''EV_SBTYPE'' 
                  AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM = ''DERIVED''
          AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 
          AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'')XLAT_CHILD_SUBTYPE	
      ON XLAT_CHILD_SUBTYPE.SRC_IDNTFTN_VAL=EV_X.CHILD_SUBTYPE
      LEFT OUTER JOIN ( SELECT 
        TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL
        ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL 
      FROM 
        DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
      WHERE 
        TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''EV_ACTVY_TYPE'' 
          AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS IN (''GW'',''DS'' )
          AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'')XLAT_PARENT_EV_ACT_TYPE_CD 
          ON  XLAT_PARENT_EV_ACT_TYPE_CD.SRC_IDNTFTN_VAL=EV_X.PARENT_EV_ACT_TYPE_CD
      LEFT OUTER JOIN(SELECT 
        TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL AS TGT_IDNTFTN_VAL
        ,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL AS SRC_IDNTFTN_VAL 
      FROM 
        DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT
      WHERE 
        TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM=  ''EV_RLTD_RSN'' 
                  AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''DERIVED'' 
          AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 
          AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31'') XLAT_RLTD_RSN_CD
          ON XLAT_RLTD_RSN_CD.SRC_IDNTFTN_VAL=EV_X.RLTD_RSN_CD

      )IN_LOOP
        
      LEFT OUTER JOIN ( SELECT	EV1.EV_ID AS EV_ID,EV1.SRC_TRANS_ID AS SRC_TRANS_ID,
          EV1.EV_SBTYPE_CD AS EV_SBTYPE_CD, EV1.EV_ACTVY_TYPE_CD AS EV_ACTVY_TYPE_CD 
      FROM	DB_T_PROD_CORE.EV  EV1
      QUALIFY	ROW_NUMBER() OVER(
      PARTITION BY  EV1.EV_SBTYPE_CD,EV1.EV_ACTVY_TYPE_CD,EV1.SRC_TRANS_ID 
      ORDER BY EV1.EDW_END_DTTM DESC) = 1)PARENT_EV_ID
      ON PARENT_EV_ID.SRC_TRANS_ID = TRIM(IN_LOOP.PARENTKEY)
      AND PARENT_EV_ID.EV_SBTYPE_CD=TRIM(IN_LOOP.PARENT_SUBTYPE)
      AND PARENT_EV_ID.EV_ACTVY_TYPE_CD=TRIM(IN_LOOP.PARENT_EV_ACT_TYPE_CD)

      LEFT OUTER JOIN (SELECT	EV2.EV_ID AS EV_ID, EV2.SRC_TRANS_ID AS SRC_TRANS_ID,
          EV2.EV_SBTYPE_CD AS EV_SBTYPE_CD, EV2.EV_ACTVY_TYPE_CD AS EV_ACTVY_TYPE_CD 
      FROM	DB_T_PROD_CORE.EV EV2 
      QUALIFY	ROW_NUMBER() OVER(
      PARTITION BY  EV2.EV_SBTYPE_CD,EV2.EV_ACTVY_TYPE_CD,EV2.SRC_TRANS_ID 
      ORDER BY EV2.EDW_END_DTTM DESC) = 1)CHILD_EV_RLTD
      ON TRIM(IN_LOOP.CHILDKEY)=CHILD_EV_RLTD.SRC_TRANS_ID
      AND TRIM(IN_LOOP.CHILD_SUBTYPE)=CHILD_EV_RLTD.EV_SBTYPE_CD
      AND TRIM(IN_LOOP.CHILD_EV_ACT_TYPE_CD)=CHILD_EV_RLTD.EV_ACTVY_TYPE_CD
      ) SRC
      LEFT OUTER JOIN 
      ( SELECT	EV_RLTD.EV_RLTD_STRT_DTTM AS EV_RLTD_STRT_DTTM, 
          EV_RLTD.EDW_STRT_DTTM AS EDW_STRT_DTTM, EV_RLTD.EDW_END_DTTM AS EDW_END_DTTM,
          EV_RLTD.EV_ID AS EV_ID, EV_RLTD.RLTD_EV_ID AS RLTD_EV_ID, EV_RLTD.EV_RLTD_RSN_CD AS EV_RLTD_RSN_CD 
      FROM	DB_T_PROD_CORE.EV_RLTD 
      WHERE CAST(EDW_END_DTTM AS DATE)=''9999-12-31'' ) LKP_EV_RLTD
      ON SRC.RLTD_RSN_CD=LKP_EV_RLTD.EV_RLTD_RSN_CD
      AND SRC.OUT_EV_ID=LKP_EV_RLTD.EV_ID
      AND SRC.OUT_EV_RLTD_ID=LKP_EV_RLTD.RLTD_EV_ID)X
  );

  CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS (
    SELECT
      rltd_rsn_cd                            AS rltd_rsn_cd,
      strt_dt                                AS strt_dt,
      end_dt                                 AS end_dt,
      Retired                                AS Retired,
      in_EV_ID                               AS out_ev_id,       
      in_EV_RLTD_ID                          AS out_rltd_ev_id,  
      LKP_EV_ID                              AS EV_ID,
      LKP_RLTD_EV_ID                         AS RLTD_EV_ID,
      LKP_EV_RLTD_RSN_CD                     AS EV_RLTD_RSN_CD,
      LKP_EDW_STRT_DTTM                      AS EDW_STRT_DTTM,
      LKP_EDW_END_DTTM                       AS EDW_END_DTTM,
      IN_EV_RLTD_END_DTM                     AS IN_EV_RLTD_END_DTM,
      IN_EDW_END_DTM                         AS IN_EDW_END_DTM,
      O_FLAG                                 AS O_FLAG
    FROM SQ_bc_ev_rltd_x
  );


  CREATE OR REPLACE TEMPORARY TABLE exp_CDC_Check AS (
    SELECT
      EV_ID,
      RLTD_EV_ID,
      EV_RLTD_RSN_CD,
      EDW_STRT_DTTM,
      COALESCE(NULL, :PRCS_ID)            AS in_PRCS_ID,        
      CURRENT_TIMESTAMP()                   AS in_EDW_STRT_DTTM,
      CURRENT_TIMESTAMP()                   AS in_TRANS_STRT_DTTM, 
      NULL                                  AS in_TRANS_END_DTTM,
      Retired                               AS Retired,
      LKP_EV_ID,
      LKP_RLTD_EV_ID,
      LKP_EV_RLTD_RSN_CD,
      LKP_EDW_STRT_DTTM,
      LKP_EDW_END_DTTM,
      IN_EV_RLTD_END_DTM,
      IN_EDW_END_DTM,
      O_FLAG
    FROM exp_data_transformation
  );


  CREATE OR REPLACE TEMPORARY TABLE __sentinel AS (SELECT TO_TIMESTAMP_NTZ(''9999-12-31 23:59:59.999999'') AS sentinel_ts);

  CREATE OR REPLACE TEMPORARY TABLE rtr_INSERT AS (
    SELECT c.*
    FROM exp_CDC_Check c,
         __sentinel s
    WHERE NOT (c.EV_ID IS NULL) 
      AND NOT (c.RLTD_EV_ID IS NULL)
      AND (
           -- o_CDC_Check = ''I'' OR (Retired = 0 AND lkp_EDW_END_DTTM != sentinel)
           (c.O_FLAG = ''I'') OR (c.Retired = 0 AND (c.LKP_EDW_END_DTTM IS NULL OR c.LKP_EDW_END_DTTM <> s.sentinel_ts))
      )
  );

  CREATE OR REPLACE TEMPORARY TABLE rtr_UPDATE AS (
    SELECT c.*
    FROM exp_CDC_Check c,
         __sentinel s
    WHERE NOT (c.EV_ID IS NULL)
      AND NOT (c.RLTD_EV_ID IS NULL)
      AND (c.O_FLAG = ''U'' AND (c.LKP_EDW_END_DTTM IS NOT NULL AND c.LKP_EDW_END_DTTM = s.sentinel_ts))
  );

  CREATE OR REPLACE TEMPORARY TABLE rtr_RETIRE AS (
    SELECT c.*
    FROM exp_CDC_Check c,
         __sentinel s
    WHERE c.O_FLAG = ''R''
      AND c.Retired != 0
      AND (c.LKP_EDW_END_DTTM IS NOT NULL AND c.LKP_EDW_END_DTTM = s.sentinel_ts)
  );

  CREATE OR REPLACE TEMPORARY TABLE rtr_DEFAULT1 AS (
    SELECT c.*
    FROM exp_CDC_Check c
    WHERE 1=1
      AND NOT EXISTS (SELECT 1 FROM rtr_INSERT r WHERE r.EV_ID = c.EV_ID AND r.RLTD_EV_ID = c.RLTD_EV_ID)
      AND NOT EXISTS (SELECT 1 FROM rtr_UPDATE r WHERE r.EV_ID = c.EV_ID AND r.RLTD_EV_ID = c.RLTD_EV_ID)
      AND NOT EXISTS (SELECT 1 FROM rtr_RETIRE r WHERE r.EV_ID = c.EV_ID AND r.RLTD_EV_ID = c.RLTD_EV_ID)
  );

  CREATE OR REPLACE TEMPORARY TABLE upd_ev_rltd_insert AS (
    SELECT
      r.EV_ID,
      r.RLTD_EV_ID,
      r.EV_RLTD_RSN_CD,
      r.in_PRCS_ID         AS in_PRCS_ID1,
      r.in_EDW_STRT_DTTM   AS in_EDW_STRT_DTTM1,
      r.in_TRANS_STRT_DTTM AS in_TRANS_STRT_DTTM1,
      r.Retired            AS Retired1,
      CASE WHEN r.Retired != 0 THEN CURRENT_TIMESTAMP() ELSE r.IN_EDW_END_DTM END AS o_EDW_END_DTTM,
      CASE WHEN r.Retired != 0 THEN r.in_TRANS_STRT_DTTM
           ELSE TO_TIMESTAMP_NTZ(''9999-12-31 23:59:59.999999'') END AS in_TRANS_END_DTTM1,
      0 as UPDATE_STRATEGY_ACTION
    FROM rtr_INSERT r
  );

  CREATE OR REPLACE TEMPORARY TABLE exp_ev_rltd_insert AS (
    SELECT
      EV_ID                          AS in_EV_ID1,
      RLTD_EV_ID                     AS in_RLTD_EV_ID1,
      EV_RLTD_RSN_CD                 AS in_EV_RLTD_RSN_CD1,
      in_TRANS_STRT_DTTM1            AS in_TRANS_STRT_DTTM1,
      in_TRANS_END_DTTM1             AS in_TRANS_END_DTTM1,
      in_EDW_STRT_DTTM1              AS in_EDW_STRT_DTTM1,
      o_EDW_END_DTTM                 AS o_EDW_END_DTTM,
      in_PRCS_ID1                    AS in_PRCS_ID1
    FROM upd_ev_rltd_insert
  );

  CREATE OR REPLACE TEMPORARY TABLE fil_ev_rltd_upd_ins AS (
    SELECT
      in_EV_ID3      AS in_EV_ID3,
      in_RLTD_EV_ID3 AS in_RLTD_EV_ID3,
      in_EV_RLTD_RSN_CD3,
      in_EV_RLTD_STRT_DTTM3,
      in_EV_RLTD_END_DTM3,
      in_PRCS_ID3,
      in_EDW_STRT_DTTM3,
      in_EDW_END_DTTM3,
      in_TRANS_STRT_DTTM3,
      in_TRANS_END_DTTM3,
      Retired3
    FROM (
      SELECT
        EV_ID    AS in_EV_ID3,
        RLTD_EV_ID AS in_RLTD_EV_ID3,
        EV_RLTD_RSN_CD AS in_EV_RLTD_RSN_CD3,
        IN_EV_RLTD_END_DTM AS in_EV_RLTD_END_DTM3,
        IN_EDW_END_DTM AS in_EDW_END_DTM3,
        in_PRCS_ID AS in_PRCS_ID3,
        in_EDW_STRT_DTTM AS in_EDW_STRT_DTTM3,
        in_TRANS_STRT_DTTM AS in_TRANS_STRT_DTTM3,
        in_TRANS_END_DTTM AS in_TRANS_END_DTTM3,
        Retired AS Retired3
      FROM rtr_UPDATE  
    )
    WHERE Retired3 = 0
  );

  CREATE OR REPLACE TEMPORARY TABLE fil_ev_rltd_upd_update AS (
    SELECT
      in_EV_ID3,
      in_RLTD_EV_ID3,
      in_EV_RLTD_RSN_CD3,
      in_EV_RLTD_STRT_DTTM3,
      in_EV_RLTD_END_DTM3,
      in_PRCS_ID3,
      in_EDW_STRT_DTTM3,
      in_EDW_END_DTTM3,
      in_TRANS_STRT_DTTM3,
      in_TRANS_END_DTTM3,
      Retired3,
      sentinel_ts
    FROM rtr_UPDATE r,
         __sentinel s
    WHERE r.LKP_EDW_END_DTTM = s.sentinel_ts
  );

  CREATE OR REPLACE TEMPORARY TABLE exp_ev_rltd_upd_update AS (
    SELECT
      in_EV_ID3,
      in_RLTD_EV_ID3,
      in_EV_RLTD_RSN_CD3,
      in_EV_RLTD_STRT_DTTM3,
      in_EV_RLTD_END_DTM3,
      in_PRCS_ID3,
      in_EDW_STRT_DTTM3,
      in_EDW_END_DTTM3,
      in_TRANS_STRT_DTTM3,
      in_TRANS_END_DTTM3,
      Retired3,
      sentinel_ts,
      CASE WHEN r.Retired != 0 THEN CURRENT_TIMESTAMP()
           ELSE DATEADD(second, -1, CURRENT_TIMESTAMP()) END AS o_DateExpiry,
      DATEADD(second, -1, r.in_TRANS_STRT_DTTM) AS TRANS_END_DTTM
    FROM fil_ev_rltd_upd_update r
  );

  CREATE OR REPLACE TEMPORARY TABLE exp_ev_rltd_upd_Retire_Reject AS (
    SELECT
      lkp_EV_ID3     AS lkp_EV_ID3,
      lkp_RLTD_EV_ID3 AS lkp_RLTD_EV_ID3,
      lkp_EV_RLTD_RSN_CD3,
      lkp_EDW_STRT_DTTM3,
      CURRENT_TIMESTAMP() AS o_DateExpiry,
      in_PRCS_ID4,
      in_TRANS_STRT_DTTM4
    FROM rtr_RETIRE
  );

  CREATE OR REPLACE TEMPORARY TABLE upd_ev_rltd_upd_ins AS (
    SELECT 
      in_EV_ID3,
      in_RLTD_EV_ID3,
      in_EV_RLTD_RSN_CD3,
      in_EV_RLTD_STRT_DTTM3,
      in_EV_RLTD_END_DTM3,
      in_PRCS_ID3,
      in_EDW_STRT_DTTM3,
      in_EDW_END_DTTM3,
      in_TRANS_STRT_DTTM3,
      in_TRANS_END_DTTM3,
      Retired3,
      0 as UPDATE_STRATEGY_ACTION
    FROM fil_ev_rltd_upd_ins
  );

  CREATE OR REPLACE TEMPORARY TABLE upd_ev_rltd_upd AS (
    SELECT
      in_EV_ID3,
      in_RLTD_EV_ID3,
      in_EV_RLTD_RSN_CD3,
      in_EV_RLTD_STRT_DTTM3,
      in_EV_RLTD_END_DTM3,
      in_PRCS_ID3,
      in_EDW_STRT_DTTM3,
      in_EDW_END_DTTM3,
      in_TRANS_STRT_DTTM3,
      in_TRANS_END_DTTM3,
      Retired3,
      sentinel_ts,
      o_DateExpiry,
      TRANS_END_DTTM,
      1 as UPDATE_STRATEGY_ACTION
    FROM exp_ev_rltd_upd_update
  );

  CREATE OR REPLACE TEMPORARY TABLE upd_ev_rltd_upd_Retire_Reject AS (
    SELECT 
      lkp_EV_ID3,
      lkp_RLTD_EV_ID3,
      lkp_EV_RLTD_RSN_CD3,
      lkp_EDW_STRT_DTTM3,
      CURRENT_TIMESTAMP() AS o_DateExpiry,
      in_PRCS_ID4,
      in_TRANS_STRT_DTTM4,
      1 as UPDATE_STRATEGY_ACTION
    FROM exp_ev_rltd_upd_Retire_Reject
  );

  INSERT INTO EV_RLTD (
      EV_ID,
      RLTD_EV_ID,
      EV_RLTD_RSN_CD,
      EV_RLTD_STRT_DTTM,
      EV_RLTD_END_DTTM,
      EV_RLTD_CORRL_PCT,
      PRCS_ID,
      EDW_STRT_DTTM,
      EDW_END_DTTM,
      TRANS_STRT_DTTM,
      TRANS_END_DTTM
  )
  SELECT
      r.EV_ID,
      r.RLTD_EV_ID,
      r.EV_RLTD_RSN_CD,
      r.in_EDW_STRT_DTTM1, 
      NULL,                
      NULL,                
      r.in_PRCS_ID1,
      r.in_EDW_STRT_DTTM1,
      r.o_EDW_END_DTTM,
      r.in_TRANS_STRT_DTTM1,
      r.in_TRANS_END_DTTM1
  FROM upd_ev_rltd_insert r;

  INSERT INTO EV_RLTD (
      EV_ID,
      RLTD_EV_ID,
      EV_RLTD_RSN_CD,
      EV_RLTD_STRT_DTTM,
      EV_RLTD_END_DTTM,
      EV_RLTD_CORRL_PCT,
      PRCS_ID,
      EDW_STRT_DTTM,
      EDW_END_DTTM,
      TRANS_STRT_DTTM,
      TRANS_END_DTTM
  )
  SELECT
      u.in_EV_ID3,
      u.in_RLTD_EV_ID3,
      u.in_EV_RLTD_RSN_CD3,
      u.in_EV_RLTD_STRT_DTTM3,
      u.in_EV_RLTD_END_DTM3,
      NULL, 
      u.in_PRCS_ID3,
      u.in_EDW_STRT_DTTM3,
      u.in_EDW_END_DTTM3,
      u.in_TRANS_STRT_DTTM3,
      u.in_TRANS_END_DTTM3
  FROM upd_ev_rltd_upd_ins u;

  UPDATE EV_RLTD tgt
  SET
      EDW_END_DTTM   = src.o_DateExpiry,
      PRCS_ID        = src.in_PRCS_ID4,
      TRANS_END_DTTM = src.in_TRANS_STRT_DTTM4
  FROM upd_ev_rltd_upd_Retire_Reject src
  WHERE tgt.EV_ID              = src.lkp_EV_ID3
    AND tgt.RLTD_EV_ID         = src.lkp_RLTD_EV_ID3
    AND tgt.EV_RLTD_RSN_CD     = src.lkp_EV_RLTD_RSN_CD3
    AND tgt.EDW_STRT_DTTM      = src.lkp_EDW_STRT_DTTM3;

  UPDATE EV_RLTD tgt
  SET
      EV_RLTD_END_DTTM = src.in_EV_RLTD_END_DTM3,
      PRCS_ID          = src.in_PRCS_ID3,
      EDW_END_DTTM     = src.in_EDW_END_DTTM3,
      TRANS_END_DTTM   = src.in_TRANS_END_DTTM3
  FROM upd_ev_rltd_upd src
  WHERE tgt.EV_ID              = src.in_EV_ID3
    AND tgt.RLTD_EV_ID         = src.in_RLTD_EV_ID3
    AND tgt.EV_RLTD_RSN_CD     = src.in_EV_RLTD_RSN_CD3
    AND tgt.EV_RLTD_STRT_DTTM  = src.in_EV_RLTD_STRT_DTTM3
    AND tgt.EDW_STRT_DTTM      = src.in_EDW_STRT_DTTM3;

END;
';