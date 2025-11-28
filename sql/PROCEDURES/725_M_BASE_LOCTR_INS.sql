-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_LOCTR_INS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
 declare
run_id varchar;
start_dttm timestamp;
end_dttm timestamp;
prcs_id int;


BEGIN 
run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
END_DTTM:=   (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''END_DTTM'' order by insert_ts desc limit 1);
START_DTTM:=     (SELECT left(param_value,19) FROM control_params where run_id = :run_id and upper(param_name)=''START_DTTM'' order by insert_ts desc limit 1);
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);

-- Component SQ_ELCTRNC_ADDR, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_ELCTRNC_ADDR AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as LOCTR_ID,
$2 as LOCTR_SBTYPE_CD,
$3 as ADDR_SBTYPE_CD,
$4 as GEOGRCL_AREA_SBTYPE_CD,
$5 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select 

t.loctr_id as loctr_id,

t.loctr_sbtype_cd as loctr_sbtype_cd,

t.addr_sbtype_cd as addr_sbtype_cd,

t.geogrcl_area_sbtype_cd as geogrcl_area_sbtype_cd 

from 

(select terr_id as loctr_id,cast(loctr_sbtype_cd as varchar(50)) as loctr_sbtype_cd,cast(null as varchar(50)) as addr_sbtype_cd,cast(geogrcl_area_sbtype_cd as varchar(50)) as geogrcl_area_sbtype_cd from db_t_prod_core.terr

union 

select ctry_id,loctr_sbtype_cd as loctr_sbtype_cd,cast(null as varchar(50)) as addr_sbtype_cd,geogrcl_area_sbtype_cd as geogrcl_area_sbtype_cd from db_t_prod_core.ctry

union

select cnty_id,loctr_sbtype_cd as loctr_sbtype_cd,cast(null as varchar(50)) as addr_sbtype_cd,geogrcl_area_sbtype_cd as geogrcl_area_sbtype_cd from db_t_prod_core.cnty

union

select city_id,loctr_sbtype_cd as loctr_sbtype_cd,cast(null as varchar(50)) as addr_sbtype_cd,geogrcl_area_sbtype_cd as geogrcl_area_sbtype_cd from db_t_prod_core.city

union

select tax_loc_id,loctr_sbtype_cd as loctr_sbtype_cd,cast(null as varchar(50)) as addr_sbtype_cd,geogrcl_area_sbtype_cd as geogrcl_area_sbtype_cd from db_t_prod_core.tax_loc

union

select postl_cd_id,loctr_sbtype_cd as loctr_sbtype_cd,cast(null as varchar(50)) as addr_sbtype_cd,geogrcl_area_sbtype_cd as geogrcl_area_sbtype_cd from db_t_prod_core.postl_cd

union

select street_addr_id,loctr_sbtype_cd as loctr_sbtype_cd,addr_sbtype_cd as addr_sbtype_cd,cast(null as varchar(50)) as geogrcl_area_sbtype_cd from db_t_prod_core.street_addr

union

select tlphn_num_id,loctr_sbtype_cd as loctr_sbtype_cd,addr_sbtype_cd as addr_sbtype_cd,cast(null as varchar(50)) as geogrcl_area_sbtype_cd from db_t_prod_core.tlphn_num

union

select elctrnc_addr_id,loctr_sbtype_cd as loctr_sbtype_cd,addr_sbtype_cd as addr_sbtype_cd,cast(null as varchar(50)) as geogrcl_area_sbtype_cd from db_t_prod_core.elctrnc_addr

) as t 

left outer join db_t_prod_core.loctr as l on 

t.loctr_id = l.loctr_id and 

t.loctr_sbtype_cd = l.loctr_sbtype_cd and 

coalesce(t.addr_sbtype_cd,''~'') = coalesce(l.addr_sbtype_cd,''~'') and 

coalesce(t.geogrcl_area_sbtype_cd,''~'') = coalesce(l.geogrcl_area_sbtype_cd,''~'')

where l.prcs_id is null
) SRC
)
);


-- Component exp, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp AS
(
SELECT
SQ_ELCTRNC_ADDR.LOCTR_ID as LOCTR_ID,
SQ_ELCTRNC_ADDR.LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD,
SQ_ELCTRNC_ADDR.ADDR_SBTYPE_CD as ADDR_SBTYPE_CD,
SQ_ELCTRNC_ADDR.GEOGRCL_AREA_SBTYPE_CD as GEOGRCL_AREA_SBTYPE_CD,
:PRCS_ID as PRCS_ID,
SQ_ELCTRNC_ADDR.source_record_id
FROM
SQ_ELCTRNC_ADDR
);


-- Component tgt_loctr_missed_id, Type TARGET 
INSERT INTO db_t_prod_core.LOCTR
(
LOCTR_ID,
LOCTR_SBTYPE_CD,
ADDR_SBTYPE_CD,
GEOGRCL_AREA_SBTYPE_CD,
PRCS_ID
)
SELECT
exp.LOCTR_ID as LOCTR_ID,
exp.LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD,
exp.ADDR_SBTYPE_CD as ADDR_SBTYPE_CD,
exp.GEOGRCL_AREA_SBTYPE_CD as GEOGRCL_AREA_SBTYPE_CD,
exp.PRCS_ID as PRCS_ID
FROM
exp;


END; 
';