-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_UMBRELLA("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component Umbrella, Type TRUNCATE_TABLE 
TRUNCATE TABLE DB_T_STAG_MEMBXREF_PROD.Umbrella;


-- Component SQ_view_Umbrella, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_view_Umbrella AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as POLICY,
$2 as OTH_TYPE,
$3 as EFFECTIVE_DATE,
$4 as NEXT_RENEWAL_DATE,
$5 as YR,
$6 as MODEL,
$7 as LIABILITY_LIMITS,
$8 as TOTAL_VALUE,
$9 as COLLISION,
$10 as DWELLING,
$11 as BODILY_INJURY,
$12 as PROPERTY_DAMAGE,
$13 as MEDICAL_PAYMENTS,
$14 as UMBI,
$15 as TOTAL_PREMIUM,
$16 as MEMBER_NUMBER,
$17 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT
view_Umbrella.POLICY,
view_Umbrella.OTH_TYPE,
view_Umbrella.EFFECTIVE_DATE,
view_Umbrella.NEXT_RENEWAL_DATE,
view_Umbrella.YR,
view_Umbrella.MODEL,
view_Umbrella.LIABILITY_LIMITS,
view_Umbrella.TOTAL_VALUE,
view_Umbrella.COLLISION,
view_Umbrella.DWELLING,
view_Umbrella.BODILY_INJURY,
view_Umbrella.PROPERTY_DAMAGE,
view_Umbrella.MEDICAL_PAYMENTS,
view_Umbrella.UMBI,
view_Umbrella.TOTAL_PREMIUM,
view_Umbrella.MEMBER_NUMBER
FROM DB_T_STAG_MEMBXREF_PROD.view_Umbrella
) SRC
)
);


-- Component exp_umbrella, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_umbrella AS
(
SELECT
SQ_view_Umbrella.POLICY as POLICY,
SQ_view_Umbrella.OTH_TYPE as OTH_TYPE,
SQ_view_Umbrella.EFFECTIVE_DATE as EFFECTIVE_DATE,
SQ_view_Umbrella.NEXT_RENEWAL_DATE as NEXT_RENEWAL_DATE,
SQ_view_Umbrella.YR as YR,
SQ_view_Umbrella.MODEL as MODEL,
SQ_view_Umbrella.LIABILITY_LIMITS as LIABILITY_LIMITS,
SQ_view_Umbrella.TOTAL_VALUE as TOTAL_VALUE,
SQ_view_Umbrella.COLLISION as COLLISION,
SQ_view_Umbrella.DWELLING as DWELLING,
SQ_view_Umbrella.BODILY_INJURY as BODILY_INJURY,
SQ_view_Umbrella.PROPERTY_DAMAGE as PROPERTY_DAMAGE,
SQ_view_Umbrella.MEDICAL_PAYMENTS as MEDICAL_PAYMENTS,
SQ_view_Umbrella.UMBI as UMBI,
SQ_view_Umbrella.TOTAL_PREMIUM as TOTAL_PREMIUM,
SQ_view_Umbrella.MEMBER_NUMBER as MEMBER_NUMBER,
SQ_view_Umbrella.source_record_id
FROM
SQ_view_Umbrella
);


-- Component Umbrella, Type TARGET 
INSERT INTO DB_T_STAG_MEMBXREF_PROD.Umbrella
(
POLICY,
OTH_TYPE,
EFFECTIVE_DATE,
NEXT_RENEWAL_DATE,
YR,
MODEL,
LIABILITY_LIMITS,
TOTAL_VALUE,
COLLISION,
DWELLING,
BODILY_INJURY,
PROPERTY_DAMAGE,
MEDICAL_PAYMENTS,
UMBI,
TOTAL_PREMIUM,
MEMBER_NUMBER
)
SELECT
exp_umbrella.POLICY as POLICY,
exp_umbrella.OTH_TYPE as OTH_TYPE,
exp_umbrella.EFFECTIVE_DATE as EFFECTIVE_DATE,
exp_umbrella.NEXT_RENEWAL_DATE as NEXT_RENEWAL_DATE,
exp_umbrella.YR as YR,
exp_umbrella.MODEL as MODEL,
exp_umbrella.LIABILITY_LIMITS as LIABILITY_LIMITS,
exp_umbrella.TOTAL_VALUE as TOTAL_VALUE,
exp_umbrella.COLLISION as COLLISION,
exp_umbrella.DWELLING as DWELLING,
exp_umbrella.BODILY_INJURY as BODILY_INJURY,
exp_umbrella.PROPERTY_DAMAGE as PROPERTY_DAMAGE,
exp_umbrella.MEDICAL_PAYMENTS as MEDICAL_PAYMENTS,
exp_umbrella.UMBI as UMBI,
exp_umbrella.TOTAL_PREMIUM as TOTAL_PREMIUM,
exp_umbrella.MEMBER_NUMBER as MEMBER_NUMBER
FROM
exp_umbrella;


END; ';