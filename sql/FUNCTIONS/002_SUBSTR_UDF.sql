-- Object Type: FUNCTIONS
CREATE OR REPLACE FUNCTION ALFA_EDW_DEV.PUBLIC.SUBSTR_UDF("BASE_EXPRESSION" VARCHAR, "START_POSITION" FLOAT)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
IMMUTABLE
COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 7,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"udf\",  \"convertedOn\": \"06/02/2025\",  \"domain\": \"alfains\" }}'
AS '
  return START_POSITION > 0 ? BASE_EXPRESSION.substr(START_POSITION - 1) : BASE_EXPRESSION.substr(0);
';