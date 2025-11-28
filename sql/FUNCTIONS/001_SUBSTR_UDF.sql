-- Object Type: FUNCTIONS
CREATE OR REPLACE FUNCTION ALFA_EDW_DEV.PUBLIC.SUBSTR_UDF("BASE_EXPRESSION" VARCHAR, "START_POSITION" FLOAT, "LENGTH" FLOAT)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
IMMUTABLE
COMMENT='{ \"origin\": \"sf_sc\", \"name\": \"snowconvert\", \"version\": {  \"major\": 1,  \"minor\": 7,  \"patch\": \"0.0\" }, \"attributes\": {  \"component\": \"udf\",  \"convertedOn\": \"06/02/2025\",  \"domain\": \"alfains\" }}'
AS '
  if (START_POSITION > 0) {
      return BASE_EXPRESSION.substr(START_POSITION -1, LENGTH);
  } else if (START_POSITION == 0 ) {
      return BASE_EXPRESSION.substr(START_POSITION, LENGTH - 1);
  } else {
      return BASE_EXPRESSION.substr(0, LENGTH + START_POSITION - 1);
  }
';