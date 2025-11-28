-- Object Type: FUNCTIONS
CREATE OR REPLACE FUNCTION ALFA_EDW_DEV.PUBLIC.TRUNC_UDF("INPUT" TIMESTAMP_LTZ(9))
RETURNS DATE
LANGUAGE SQL
IMMUTABLE
COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 11,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"udf\",  \"convertedOn\": \"07/03/2025\",  \"domain\": \"alfains\" }}'
AS '
    INPUT::DATE
';