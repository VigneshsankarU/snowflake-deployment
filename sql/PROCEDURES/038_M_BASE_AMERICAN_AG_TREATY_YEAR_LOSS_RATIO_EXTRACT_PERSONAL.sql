-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AMERICAN_AG_TREATY_YEAR_LOSS_RATIO_EXTRACT_PERSONAL("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_LR_LOSS_PERSONAL, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_LR_LOSS_PERSONAL AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as YR,
$2 as PLCY_TYPE_CD,
$3 as Prem,
$4 as Non_Storm_Incurred_Loss,
$5 as Non_Storm_LR,
$6 as Storm_Incurred_loss,
$7 as Storm_Incurred_loss_LR,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select src.YR , ''PERSONAL'' as PLCY_TYPE_CD , src.prem , src.Non_Storm_Incurred_Loss , (src.Non_Storm_Incurred_Loss/src.prem)*100 as Non_Storm_LR , src.Storm_Incurred_loss , (src.Storm_Incurred_loss/src.prem)*100 as Storm_Incurred_loss_LR

from

(select extract (year from clm_loss_dt) as YR,prem , sum(LOSS_NCAT_AMT) as Non_Storm_Incurred_Loss , sum(LOSS_CAT_AMT) as Storm_Incurred_loss

from DB_T_PROD_ANPROD.LR_Loss loss

join (

select substr (trim(mo_id),1,4) as YR,sum(earned_prem) Prem

from DB_T_PROD_ANPROD.LR_Premium

where LOB_CD in (''HO'', ''MH'', ''SF'', ''Persartfl'', ''PA'', ''WTC'')

group by YR

) prem on prem.YR = extract (year from clm_loss_dt)

where LOB_CD in (''HO'', ''MH'', ''SF'', ''Persartfl'', ''PA'', ''WTC'')

and extract (year from clm_loss_dt) >= (extract (year from $American_AG_CAL_DT))-5

and extract (year from clm_loss_dt) < (extract (year from $American_AG_CAL_DT))

group by extract (year from clm_loss_dt) , prem

) src

order by src.YR
) SRC
)
);


-- Component EXPTRANS1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS1 AS
(
SELECT
SQ_LR_LOSS_PERSONAL.YR as YR,
SQ_LR_LOSS_PERSONAL.PLCY_TYPE_CD as PLCY_TYPE_CD,
SQ_LR_LOSS_PERSONAL.Prem as Prem,
SQ_LR_LOSS_PERSONAL.Non_Storm_Incurred_Loss as Non_Storm_Incurred_Loss,
SQ_LR_LOSS_PERSONAL.Non_Storm_LR as Non_Storm_LR,
SQ_LR_LOSS_PERSONAL.Storm_Incurred_loss as Storm_Incurred_loss,
SQ_LR_LOSS_PERSONAL.Storm_Incurred_loss_LR as Storm_Incurred_loss_LR,
SQ_LR_LOSS_PERSONAL.source_record_id
FROM
SQ_LR_LOSS_PERSONAL
);


-- Component LossRatioPersonal, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE LossRatioPersonal AS
(
SELECT
EXPTRANS1.YR as Year,
EXPTRANS1.PLCY_TYPE_CD as Line_of_Business,
EXPTRANS1.Prem as Earned_Premium,
EXPTRANS1.Non_Storm_Incurred_Loss as Non_Storm_Incurred_Loss,
EXPTRANS1.Non_Storm_LR as Non_Storm_Loss_Ratio,
EXPTRANS1.Storm_Incurred_loss as Storm_Incurred_Loss,
EXPTRANS1.Storm_Incurred_loss_LR as Storm_Loss_Ratio
FROM
EXPTRANS1
);


-- Component LossRatioPersonal, Type EXPORT_DATA Exporting data
;


END; ';