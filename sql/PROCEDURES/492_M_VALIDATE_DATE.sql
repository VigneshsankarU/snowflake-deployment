-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_VALIDATE_DATE("IN_INPUT_MAPLET" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
BEGIN 

-- Component exp_validate, Type EXPRESSION 
EXECUTE IMMEDIATE ''
CREATE OR REPLACE TEMPORARY TABLE exp_validate AS
(
SELECT
substr ( input_Maplet.YYYY , 1 , 2 ) as CC,
substr ( input_Maplet.YYYY , 3 , 2 ) as YY,
CASE WHEN ( YY = 0 and ( mod ( CC , 4 ) = 0 ) ) or ( YY != 0 and ( mod ( YY , 4 ) = 0 ) ) THEN ''''Y'''' ELSE ''''N'''' END as var_leap_year,
CASE WHEN input_Maplet.MM = ''''02'''' and var_leap_year != ''''Y'''' and TO_NUMBER(input_Maplet.DD) > 28 THEN ''''28'''' ELSE CASE WHEN input_Maplet.MM = ''''02'''' and var_leap_year = ''''Y'''' and TO_NUMBER(input_Maplet.DD) > 29 THEN ''''29'''' ELSE CASE WHEN ( input_Maplet.MM = ''''04'''' or input_Maplet.MM = ''''06'''' or input_Maplet.MM = ''''09'''' or input_Maplet.MM = ''''11'''' ) and TO_NUMBER(input_Maplet.DD) > 30 THEN ''''30'''' ELSE input_Maplet.DD END END END as var_DD,
input_Maplet.YYYY || input_Maplet.MM || var_DD as var_DATE,
CASE WHEN var_DATE = ''''00000000'''' or try_to_date ( var_DATE , ''''YYYYMMDD'''' ) IS NOT NULL THEN ''''Y'''' ELSE ''''N'''' END as var_is_date_valid,
var_is_date_valid as out_is_date_valid,
CASE WHEN var_DATE != ''''00000000'''' and var_is_date_valid = ''''Y'''' THEN var_DATE ELSE null END as out_DATE_string,
CASE WHEN var_DATE != ''''00000000'''' and var_is_date_valid = ''''Y'''' THEN to_date ( var_DATE , ''''YYYYMMDD'''' ) ELSE null END as out_DATE_dt,
input_Maplet.source_record_id
FROM
'' || :in_input_maplet || '' input_maplet
)
'';


-- Component output_date, Type OUTPUT_TRANSFORMATION 
-- Component output_date, Type MAPPLET 
CREATE OR REPLACE TEMPORARY TABLE m_validate_date AS
    (
    SELECT
    exp_validate.out_is_date_valid as out_is_date_valid,
    exp_validate.out_DATE_string as out_DATE_S,
    exp_validate.out_DATE_dt as out_DATE_DT,
    source_record_id
    FROM
    exp_validate
    );


END; ';