-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_LOAD_FNOL_ADJ_REVIEW("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component FNOL_ADJ_REVIEW, Type Pre SQL 
insert into DB_T_STAG_MEMBXREF_PROD.FNOL_ADJ_REVIEW_BKUP
select * from DB_T_STAG_MEMBXREF_PROD.FNOL_ADJ_REVIEW;
Delete from DB_T_STAG_MEMBXREF_PROD.FNOL_ADJ_REVIEW;


-- Component FNOL_ADJ_REVIEW, Type TRUNCATE_TABLE 
TRUNCATE TABLE DB_T_STAG_MEMBXREF_PROD.FNOL_ADJ_REVIEW;


-- Component SQ_FNOL_WH_STAG, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_FNOL_WH_STAG AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as CLIENT_REF_NBR,
$2 as Policy_nbr,
$3 as CLM_CLAIM_NBR,
$4 as CLM_CSR_CLAIM_NBR,
$5 as Date_LOADED,
$6 as Date_Report_Produced,
$7 as DOL,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT	
clmcd.Client_ref_nbr as Member_nub
, a.policy_nbr as policy_NBR
, clmcd.clm_claim_nbr as cd_claim_num
, trim ( clmcd.clm_csr_claim_nbr) as CSR_CLAIM_NUM
, a.DATE_LOADED
, current_timestamp as DATE_REPORT_PRODUCED
, a.DOL as DATE_OF_LOSS
FROM	DB_T_STAG_MEMBXREF_PROD.FNOL_WH_STAG a inner join
DB_T_STAG_MEMBXREF_PROD.claim_tab_object_pol_out2 clmcd
on a.claim_num2 = clmcd.clm_claim_nbr
where a.date_report_produced is null and matched_cd = ''y''
group by 1,2,3,4,5,6,7
) SRC
)
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
SQ_FNOL_WH_STAG.CLIENT_REF_NBR as CLIENT_REF_NBR,
SQ_FNOL_WH_STAG.Policy_nbr as Policy_nbr,
SQ_FNOL_WH_STAG.CLM_CLAIM_NBR as CLM_CLAIM_NBR,
SQ_FNOL_WH_STAG.CLM_CSR_CLAIM_NBR as CLM_CSR_CLAIM_NBR,
SQ_FNOL_WH_STAG.Date_LOADED as Date_LOADED,
SQ_FNOL_WH_STAG.Date_Report_Produced as Date_Report_Produced,
SQ_FNOL_WH_STAG.DOL as DOL,
SQ_FNOL_WH_STAG.source_record_id
FROM
SQ_FNOL_WH_STAG
);


-- Component FNOL_ADJ_REVIEW, Type TARGET 
INSERT INTO DB_T_STAG_MEMBXREF_PROD.FNOL_ADJ_REVIEW
(
MEMBER_NUM,
POLICY_NUM,
Claim_num,
CSR_Claim_num,
Date_LOADED,
Date_Report_Produced,
DOL
)
SELECT
EXPTRANS.CLIENT_REF_NBR as MEMBER_NUM,
EXPTRANS.Policy_nbr as POLICY_NUM,
EXPTRANS.CLM_CLAIM_NBR as Claim_num,
EXPTRANS.CLM_CSR_CLAIM_NBR as CSR_Claim_num,
EXPTRANS.Date_LOADED as Date_LOADED,
EXPTRANS.Date_Report_Produced as Date_Report_Produced,
EXPTRANS.DOL as DOL
FROM
EXPTRANS;


END; ';