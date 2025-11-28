-- Object Type: FUNCTIONS
CREATE OR REPLACE FUNCTION ALFA_EDW_DEV.PUBLIC.TRUNC_UDF("INPUT" NUMBER(38,0), "SCALE" NUMBER(38,0))
RETURNS NUMBER(38,0)
LANGUAGE SQL
IMMUTABLE
COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"udf\",  \"convertedOn\": \"07/03/2025\",  \"domain\": \"alfains\" }}'
AS '
    TRUNC(INPUT, SCALE)
';