-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_EFT_PAC_NON_PAC_MNTHLY("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_bc_policyperiod, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_bc_policyperiod AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as MemberNumber,
$2 as CreateTime,
$3 as AuthorizedToDraft,
$4 as FarmPAC,
$5 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select distinct b.ClientId_alfa_stg as Member_number,

d.CreateTime_stg , f.AuthorizedToDraft_stg, f.FarmPAC_stg

from DB_T_PROD_STAG.bc_policyperiod b

inner join DB_T_PROD_STAG.bc_invoicestream c on c.ID_stg = b.PrimaryInvoiceStream_alfa_stg

inner join DB_T_PROD_STAG.bc_paymentinstrument d on d.ID_stg = c.OverridingPaymentInstrumentID_stg

inner join DB_T_PROD_STAG.bcx_ach_alfa f on f.ID_stg= d.Ach_alfaID_stg

where b.ClientIdType_alfa_stg = 10001 /* Policies with DB_T_STAG_MEMBXREF_PROD.member number */
and b.PolicyType_alfa_stg in (''COMMERCIAL'',''PPV'',''PPV2'') /* Commercial, Personal auto policies */
and b.PaymentPlanID_stg in (5,6,7) /* Monthly pay plans */
and d.Invalidated_alfa_stg = 0

and f.AuthorizedToDraft_stg = 1 /* Membership authorized */
and (f.FarmPAC_stg = 0 or f.FarmPAC_stg = 1) /* FarmPAC election yes or no */
and d.Retired_stg = 0

and f.Retired_stg = 0

order by d.CreateTime_stg
) SRC
)
);


-- Component exp, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp AS
(
SELECT
SQ_bc_policyperiod.MemberNumber as MemberNumber,
SQ_bc_policyperiod.CreateTime as CreateTime,
SQ_bc_policyperiod.AuthorizedToDraft as AuthorizedToDraft,
SQ_bc_policyperiod.FarmPAC as FarmPAC,
SQ_bc_policyperiod.source_record_id
FROM
SQ_bc_policyperiod
);


-- Component eft_pac_non_pac_mthly, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE eft_pac_non_pac_mthly AS
(
SELECT
exp.MemberNumber as MemberNumber,
exp.CreateTime as CreateTime,
exp.AuthorizedToDraft as AuthorizedToDraft,
exp.FarmPAC as FarmPAC
FROM
exp
);


-- Component eft_pac_non_pac_mthly, Type EXPORT_DATA Exporting data
;


END; ';