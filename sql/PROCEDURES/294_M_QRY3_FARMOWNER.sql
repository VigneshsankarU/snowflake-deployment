-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_QRY3_FARMOWNER("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component QRY3_FARMOWNER, Type TRUNCATE_TABLE 
TRUNCATE TABLE DB_T_STAG_MEMBXREF_PROD.QRY3_FARMOWNER;


-- Component SQ_HomeFarmFire, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_HomeFarmFire AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as MEMBER_NUMBER,
$2 as AGENT,
$3 as NAME_1,
$4 as ADDRESS_1,
$5 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT DISTINCT 
A.MEMBER_NUMBER, 
A.AGENT,
B.name_1,
B.address_1
FROM DB_T_STAG_MEMBXREF_PROD.HomeFarmFire AS A
INNER JOIN DB_T_STAG_MEMBXREF_PROD.member_xref AS B
ON A.member_number =  B.memb_num
WHERE SUBSTR(a.policy,1,1) = ''R''
) SRC
)
);


-- Component exp_qry3_farmowner, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_qry3_farmowner AS
(
SELECT
SQ_HomeFarmFire.MEMBER_NUMBER as MEMBER_NUMBER,
SQ_HomeFarmFire.AGENT as AGENT,
SQ_HomeFarmFire.NAME_1 as NAME_1,
SQ_HomeFarmFire.ADDRESS_1 as ADDRESS_1,
SQ_HomeFarmFire.source_record_id
FROM
SQ_HomeFarmFire
);


-- Component QRY3_FARMOWNER, Type TARGET 
INSERT INTO DB_T_STAG_MEMBXREF_PROD.QRY3_FARMOWNER
(
MEMBER_NUMBER,
AGENT,
NAME_1,
ADDRESS_1
)
SELECT
exp_qry3_farmowner.MEMBER_NUMBER as MEMBER_NUMBER,
exp_qry3_farmowner.AGENT as AGENT,
exp_qry3_farmowner.NAME_1 as NAME_1,
exp_qry3_farmowner.ADDRESS_1 as ADDRESS_1
FROM
exp_qry3_farmowner;


END; ';