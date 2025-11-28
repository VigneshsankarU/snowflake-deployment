-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PCI_CREATE_PARAM("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN
  -- Component SQ_GW_CLOSEOUT_CTL, Type SOURCE
  CREATE
  OR
  REPLACE TEMPORARY TABLE SQ_GW_CLOSEOUT_CTL AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS pc_BOY,
                $2  AS pc_EOY,
                $3  AS cc_BOY,
                $4  AS cc_EOY,
                $5  AS cc_EOFQ,
                $6  AS pc_BOQ,
                $7  AS pc_EOQ,
                $8  AS cc_BOQ,
                $9  AS cc_EOQ,
                $10 AS source_record_id
         FROM   (
                         SELECT   SRC.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT MIN(
                                               CASE
                                                       WHEN closeout_type=''P''
                                                       AND    accounting_yr=EXTRACT(YEAR, TO_DATE($current_date))-1
                                                       AND    accounting_mo = 1 THEN beginning_ts
                                                END ) AS P_BOY,
                                                MAX(
                                                CASE
                                                       WHEN closeout_type=''P''
                                                       AND    accounting_yr=EXTRACT(YEAR, TO_DATE($current_date))-1
                                                       AND    accounting_mo = 12 THEN ending_ts
                                                END) AS P_EOY,
                                                MIN(
                                                CASE
                                                       WHEN closeout_type=''C''
                                                       AND    accounting_yr=EXTRACT(YEAR, TO_DATE($current_date))-2
                                                       AND    accounting_mo = 12 THEN ending_ts
                                                END) + interval ''1  second'' AS C_BOY,
                                                MAX(
                                                CASE
                                                       WHEN closeout_type=''C''
                                                       AND    accounting_yr=EXTRACT(YEAR, TO_DATE($current_date))-1
                                                       AND    accounting_mo = 12 THEN ending_ts
                                                END) AS C_EOY,
                                                MAX(
                                                CASE
                                                       WHEN closeout_type=''C''
                                                       AND    accounting_yr=
                                                              CASE
                                                                     WHEN EXTRACT(MONTH, TO_DATE($current_date)) IN (0,1,2,3) THEN EXTRACT(YEAR, TO_DATE($current_date))-1
                                                                     ELSE EXTRACT(YEAR, TO_DATE($current_date))
                                                              END
                                                       AND    accounting_mo = (3) THEN ending_ts
                                                END) AS C_EOfQ,
                                                MIN(
                                                CASE
                                                       WHEN closeout_type=''P''
                                                       AND    accounting_yr=
                                                              CASE
                                                                     WHEN EXTRACT(MONTH, TO_DATE($current_date)) IN (0,1,2,3) THEN EXTRACT(YEAR, TO_DATE($current_date))-1
                                                                     ELSE EXTRACT(YEAR, TO_DATE($current_date))
                                                              END
                                                       AND    accounting_mo = 1 THEN beginning_ts
                                                END) AS P_BOQ,
                                                MAX(
                                                CASE
                                                       WHEN closeout_type=''P''
                                                       AND    accounting_yr=
                                                              CASE
                                                                     WHEN EXTRACT(MONTH, TO_DATE($current_date)) IN (0,1,2,3) THEN EXTRACT(YEAR, TO_DATE($current_date))-1
                                                                     ELSE EXTRACT(YEAR, TO_DATE($current_date))
                                                              END
                                                       AND    accounting_mo = (
                                                              CASE
                                                                     WHEN EXTRACT(MONTH, TO_DATE($current_date)) IN (0,1,2,3) THEN 12
                                                                     WHEN EXTRACT(MONTH, TO_DATE($current_date)) IN (4,5,6) THEN 3
                                                                     WHEN EXTRACT(MONTH, TO_DATE($current_date)) IN (7,8,9) THEN 6
                                                                     WHEN EXTRACT(MONTH, TO_DATE($current_date)) IN (10,11,12) THEN 9
                                                              END) THEN ending_ts
                                                END) AS P_EOQ,
                                                MIN(
                                                CASE
                                                       WHEN closeout_type=''C''
                                                       AND    accounting_yr=
                                                              CASE
                                                                     WHEN EXTRACT(MONTH, TO_DATE($current_date)) IN (0,1,2,3) THEN EXTRACT(YEAR, TO_DATE($current_date))-2
                                                                     ELSE EXTRACT(YEAR, TO_DATE($current_date))-1
                                                              END
                                                       AND    accounting_mo = 12 THEN ending_ts
                                                END) + interval ''1  second'' AS C_BOQ,
                                                MAX(
                                                CASE
                                                       WHEN closeout_type=''C''
                                                       AND    accounting_yr=
                                                              CASE
                                                                     WHEN EXTRACT(MONTH, TO_DATE($current_date)) IN (0,1,2,3) THEN EXTRACT(YEAR, TO_DATE($current_date))-1
                                                                     ELSE EXTRACT(YEAR, TO_DATE($current_date))
                                                              END
                                                       AND    accounting_mo = (
                                                              CASE
                                                                     WHEN EXTRACT(MONTH, TO_DATE($current_date)) IN (0,1,2,3) THEN 12
                                                                     WHEN EXTRACT(MONTH, TO_DATE($current_date)) IN (4,5,6) THEN 3
                                                                     WHEN EXTRACT(MONTH, TO_DATE($current_date)) IN (7,8,9) THEN 6
                                                                     WHEN EXTRACT(MONTH, TO_DATE($current_date)) IN (10,11,12) THEN 9
                                                              END) THEN ending_ts
                                                END) AS C_EOQ
                                         FROM   DB_T_PROD_COMN.gw_closeout_ctl
                                         WHERE  closeout_type IN (''P'',
                                                                  ''C'')
                                         AND    accounting_yr IN (EXTRACT(YEAR, TO_DATE($current_date)),
                                                                  EXTRACT(YEAR, TO_DATE($current_date))-2,
                                                                  EXTRACT(YEAR, TO_DATE($current_date))-1) ) SRC ) );
  -- Component exp_data, Type EXPRESSION
  CREATE
  OR
  REPLACE TEMPORARY TABLE exp_data AS
  (
         SELECT ''[Global]'' AS out_Global,
                ''$pc_BOY=''
                       || CHR ( 39 )
                       || TO_CHAR ( SQ_GW_CLOSEOUT_CTL.pc_BOY , ''YYYY-MM-DD HH24:MI:SS.US'' )
                       || CHR ( 39 ) AS o_pc_BOY,
                ''$pc_EOY=''
                       || CHR ( 39 )
                       || TO_CHAR ( SQ_GW_CLOSEOUT_CTL.pc_EOY , ''YYYY-MM-DD HH24:MI:SS.US'' )
                       || CHR ( 39 ) AS o_pc_EOY,
                ''$cc_BOY=''
                       || CHR ( 39 )
                       || TO_CHAR ( SQ_GW_CLOSEOUT_CTL.cc_BOY , ''YYYY-MM-DD HH24:MI:SS.US'' )
                       || CHR ( 39 ) AS o_cc_BOY,
                ''$cc_EOY=''
                       || CHR ( 39 )
                       || TO_CHAR ( SQ_GW_CLOSEOUT_CTL.cc_EOY , ''YYYY-MM-DD HH24:MI:SS.US'' )
                       || CHR ( 39 ) AS o_cc_EOY,
                ''$cc_EOFQ=''
                       || CHR ( 39 )
                       || TO_CHAR ( SQ_GW_CLOSEOUT_CTL.cc_EOFQ , ''YYYY-MM-DD HH24:MI:SS.US'' )
                       || CHR ( 39 ) AS o_cc_EOFQ,
                ''$pc_BOQ=''
                       || CHR ( 39 )
                       || TO_CHAR ( SQ_GW_CLOSEOUT_CTL.pc_BOQ , ''YYYY-MM-DD HH24:MI:SS.US'' )
                       || CHR ( 39 ) AS o_pc_BOQ,
                ''$pc_EOQ=''
                       || CHR ( 39 )
                       || TO_CHAR ( SQ_GW_CLOSEOUT_CTL.pc_EOQ , ''YYYY-MM-DD HH24:MI:SS.US'' )
                       || CHR ( 39 ) AS o_pc_EOQ,
                ''$cc_BOQ=''
                       || CHR ( 39 )
                       || TO_CHAR ( SQ_GW_CLOSEOUT_CTL.cc_BOQ , ''YYYY-MM-DD HH24:MI:SS.US'' )
                       || CHR ( 39 ) AS o_cc_BOQ,
                ''$cc_EOQ=''
                       || CHR ( 39 )
                       || TO_CHAR ( SQ_GW_CLOSEOUT_CTL.cc_EOQ , ''YYYY-MM-DD HH24:MI:SS.US'' )
                       || CHR ( 39 ) AS o_cc_EOQ,
                SQ_GW_CLOSEOUT_CTL.source_record_id
         FROM   SQ_GW_CLOSEOUT_CTL );
  -- Component nrmzr_data, Type NORMALIZER
  CREATE OR REPLACE TEMPORARY TABLE nrmzr_data AS
SELECT *
FROM (
  SELECT 
    exp_data.out_Global     AS out_file_in1,
    exp_data.o_pc_BOY       AS out_file_in2,
    exp_data.o_pc_EOY       AS out_file_in3,
    exp_data.o_cc_BOY       AS out_file_in4,
    exp_data.o_cc_EOY       AS out_file_in5,
    exp_data.o_cc_EOFQ      AS out_file_in6,
    exp_data.o_pc_BOQ       AS out_file_in7,
    exp_data.o_pc_EOQ       AS out_file_in8,
    exp_data.o_cc_BOQ       AS out_file_in9,
    exp_data.o_cc_EOQ       AS out_file_in10,
    exp_data.source_record_id
  FROM exp_data
)
UNPIVOT(out_file FOR rec_no IN (
  out_file_in1,
  out_file_in2,
  out_file_in3,
  out_file_in4,
  out_file_in5,
  out_file_in6,
  out_file_in7,
  out_file_in8,
  out_file_in9,
  out_file_in10
));

  -- Component edw_param_pci_pa, Type TARGET_EXPORT_PREPARE Stage data before exporting
  CREATE
  OR
  REPLACE TEMPORARY TABLE edw_param_pci_pa AS
  (
         SELECT nrmzr_data.out_file AS out_file
         FROM   nrmzr_data );
  -- Component edw_param_pci_pa, Type EXPORT_DATA Exporting data
  ;
END;
';