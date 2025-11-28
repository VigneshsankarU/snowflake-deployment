-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_AMERICAN_AG_TREATY_YEAR_LOSS_RATIO_EXTRACT_COMMERCIAL("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_LR_LOSS_COMMERCIAL, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_LR_LOSS_COMMERCIAL AS
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
select src.YR , ''BUSINESSOWNER'' as PLCY_TYPE_CD , src.prem , src.Non_Storm_Incurred_Loss , (src.Non_Storm_Incurred_Loss/src.prem)*100 as Non_Storm_LR , src.Storm_Incurred_loss , (src.Storm_Incurred_loss/src.prem)*100 as Storm_Incurred_loss_LR

from

(select extract (year from clm_loss_dt) as YR,prem , sum(LOSS_NCAT_AMT) as Non_Storm_Incurred_Loss , sum(LOSS_CAT_AMT) as Storm_Incurred_loss

from DB_T_PROD_ANPROD.LR_Loss loss

join (

select substr (trim(mo_id),1,4) as YR,sum(earned_prem) Prem

from DB_T_PROD_ANPROD.LR_Premium

where LOB_CD in (''BO'', ''PORTFOLIO'')

/* and substr (trim(mo_id),1,4) = ''2019'' */
group by YR

) prem on prem.YR = extract (year from clm_loss_dt)

where LOB_CD in (''BO'', ''PORTFOLIO'')

and extract (year from clm_loss_dt) >= (extract (year from $American_AG_CAL_DT))-5

and extract (year from clm_loss_dt) < (extract (year from $American_AG_CAL_DT))

group by extract (year from clm_loss_dt) , prem

) src

order by src.YR
) SRC
)
);


-- Component EXPTRANS111, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS111 AS
(
SELECT
SQ_LR_LOSS_COMMERCIAL.YR as YR,
SQ_LR_LOSS_COMMERCIAL.PLCY_TYPE_CD as PLCY_TYPE_CD,
SQ_LR_LOSS_COMMERCIAL.Prem as Prem,
SQ_LR_LOSS_COMMERCIAL.Non_Storm_Incurred_Loss as Non_Storm_Incurred_Loss,
SQ_LR_LOSS_COMMERCIAL.Non_Storm_LR as Non_Storm_LR,
SQ_LR_LOSS_COMMERCIAL.Storm_Incurred_loss as Storm_Incurred_loss,
SQ_LR_LOSS_COMMERCIAL.Storm_Incurred_loss_LR as Storm_Incurred_loss_LR,
SQ_LR_LOSS_COMMERCIAL.source_record_id
FROM
SQ_LR_LOSS_COMMERCIAL
);


-- Component LossRatioCommercial, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE LossRatioCommercial AS
(
SELECT
EXPTRANS111.YR as Year,
EXPTRANS111.PLCY_TYPE_CD as Line_of_Business,
EXPTRANS111.Prem as Earned_Premium,
EXPTRANS111.Non_Storm_Incurred_Loss as Non_Storm_Incurred_Loss,
EXPTRANS111.Non_Storm_LR as Non_Storm_Loss_Ratio,
EXPTRANS111.Storm_Incurred_loss as Storm_Incurred_Loss,
EXPTRANS111.Storm_Incurred_loss_LR as Storm_Loss_Ratio
FROM
EXPTRANS111
);


-- Component LossRatioCommercial, Type EXPORT_DATA Exporting data
;


END; ';