-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_EDW_FEDERATION_COUNTYCHANGE_EXTRACT("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 

DECLARE RUN_DATE date;
BEGIN 

RUN_DATE:=(select current_date);

-- Component SQ_Memb_Cust, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_Memb_Cust AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as Year,
$2 as county,
$3 as Lost,
$4 as Gained,
$5 as Net_Change,
$6 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select
Year_calc,
COUNTY_NAME,
Lost,
Gained,
(Gained - Lost) as Net_Change
from(
select Year_calc,c.PHY_COUNTY_DESC AS COUNTY_NAME, sum(Lost) as Lost, sum(Gained) as Gained
from
(select
Year_calc,
prev_month as Month_calc,
old_county, count(member_number) as Lost,
old_county as new_county, cast(0 as int)as Gained
from
(
select member_number, Old_county, New_County,
case when cast(right(prev_mo_id,2) as int ) in(1) then ''01'' 
when cast(right(prev_mo_id,2) as int ) in(2) then ''02''
when cast(right(prev_mo_id,2) as int ) in(3) then ''03''
when cast(right(prev_mo_id,2) as int ) in(4) then ''04'' 
when cast(right(prev_mo_id,2) as int ) in(5) then ''05''
when cast(right(prev_mo_id,2) as int ) in(6) then ''06''
when cast(right(prev_mo_id,2) as int ) in(7) then ''07''
when cast(right(prev_mo_id,2) as int ) in(8) then ''08''
when cast(right(prev_mo_id,2) as int ) in(9) then ''09''
when cast(right(prev_mo_id,2) as int ) in(10) then ''10''
when cast(right(prev_mo_id,2) as int ) in(11) then ''11''
when cast(right(prev_mo_id,2) as int ) in(12) then ''12''
else 0 end as Prev_Month,
case when cast(right(current_mo_id,2) as int ) in(1) then ''01''
when cast(right(current_mo_id,2) as int ) in(2) then ''02''
when cast(right(current_mo_id,2) as int ) in(3) then ''03''
when cast(right(current_mo_id,2) as int ) in(4) then ''04'' 
when cast(right(current_mo_id,2) as int ) in(5) then ''05''
when cast(right(current_mo_id,2) as int ) in(6) then ''06''
when cast(right(current_mo_id,2) as int ) in(7) then ''07''
when cast(right(current_mo_id,2) as int ) in(8) then ''08''
when cast(right(current_mo_id,2) as int ) in(9) then ''09''
when cast(right(current_mo_id,2) as int ) in(10) then ''10''
when cast(right(current_mo_id,2) as int ) in(11) then ''11''
when cast(right(current_mo_id,2) as int ) in(12) then ''12''
else 0 end as Current_Month,
cast(left(current_mo_id,4) as int ) as Year_calc
from(           
select member_number, Old_county, New_County, prev_mo_id, min(current_mo_id) as current_mo_id
from(
select m1.member_number, m1.county_cd as Old_county, m2.county_cd as New_County,
replace(max(Cast(M1.MO_ID || ''01'' AS DATE )),''-'','''' ) as prev_mo_id,
replace((Cast(M2.MO_ID || ''01'' AS DATE )),''-'','''' ) as current_mo_id
from DB_T_CORE_AN_PROD.MEMB_CUST M1
left join DB_T_CORE_AN_PROD.MEMB_CUST M2 on M1.member_number=M2.member_number and m1.mo_id<M2.mo_id
where 
m1.county_cd<>m2.county_cd
and m1.st_cd=''AL''
and m2.st_cd=''AL''
group by 1,2,3,5
)iq
group by 1,2,3,4
) z
where
(( cast(right(current_mo_id,2) as int )- cast(right(prev_mo_id,2) as int) =1) and (cast(left(current_mo_id,4) as int )-cast(left(prev_mo_id,4) as int )=0) or ((cast(right(current_mo_id,2) as int )-cast(right(prev_mo_id,2) as int )=-11) and (cast(left(current_mo_id,4) as int )-cast(left(prev_mo_id,4) as int )=1)))
and (current_mo_id)=(year(cast(:Run_date as date ))-1)
)x
group by 1,2,3,5

union all

select
Year_calc,
current_month as Month_calc,
new_county as old_county, cast(0 as int)as Lost,
new_county, 
count(member_number) as Gained
from
(select member_number, Old_county, New_County,
case when cast(right(prev_mo_id,2) as int ) in(1) then ''01'' 
when cast(right(prev_mo_id,2) as int ) in(2) then ''02''
when cast(right(prev_mo_id,2) as int ) in(3) then ''03''
when cast(right(prev_mo_id,2) as int ) in(4) then ''04'' 
when cast(right(prev_mo_id,2) as int ) in(5) then ''05''
when cast(right(prev_mo_id,2) as int ) in(6) then ''06''
when cast(right(prev_mo_id,2) as int ) in(7) then ''07''
when cast(right(prev_mo_id,2) as int ) in(8) then ''08''
when cast(right(prev_mo_id,2) as int ) in(9) then ''09''
when cast(right(prev_mo_id,2) as int ) in(10) then ''10''
when cast(right(prev_mo_id,2) as int ) in(11) then ''11''
when cast(right(prev_mo_id,2) as int ) in(12) then ''12''
else 0 end as Prev_Month,
case when cast(right(current_mo_id,2) as int ) in(1) then ''01''
when cast(right(current_mo_id,2) as int ) in(2) then ''02''
when cast(right(current_mo_id,2) as int ) in(3) then ''03''
when cast(right(current_mo_id,2) as int ) in(4) then ''04'' 
when cast(right(current_mo_id,2) as int ) in(5) then ''05''
when cast(right(current_mo_id,2) as int ) in(6) then ''06''
when cast(right(current_mo_id,2) as int ) in(7) then ''07''
when cast(right(current_mo_id,2) as int ) in(8) then ''08''
when cast(right(current_mo_id,2) as int ) in(9) then ''09''
when cast(right(current_mo_id,2) as int ) in(10) then ''10''
when cast(right(current_mo_id,2) as int ) in(11) then ''11''
when cast(right(current_mo_id,2) as int ) in(12) then ''12''
else 0 end as Current_Month,
cast(left(current_mo_id,4) as int ) as Year_calc
from(           
select member_number, Old_county, New_County, prev_mo_id, min(current_mo_id) as current_mo_id
from(
select m1.member_number, m1.county_cd as Old_county, m2.county_cd as New_County,
replace(max(Cast(M1.MO_ID || ''01'' AS DATE )),''-'','''') as prev_mo_id,
replace((Cast(M2.MO_ID || ''01'' AS DATE )),''-'','''') as current_mo_id
from DB_T_CORE_AN_PROD.MEMB_CUST M1
left join DB_T_CORE_AN_PROD.MEMB_CUST M2 on M1.member_number=M2.member_number and m1.mo_id<M2.mo_id
where 
m1.county_cd<>m2.county_cd
and m1.st_cd=''AL''
and m2.st_cd=''AL''
group by 1,2,3,5
)iq
group by 1,2,3,4
) z
where
(( cast(right(current_mo_id,2) as int )- cast(right(prev_mo_id,2) as int) =0) and (cast(left(current_mo_id,4) as int )-cast(left(prev_mo_id,4) as int )=0) or ((cast(right(current_mo_id,2) as int )-cast(right(prev_mo_id,2) as int )=1) and (cast(left(current_mo_id,4) as int )-cast(left(prev_mo_id,4) as int )=1)))
and (current_mo_id)=(year(cast(:Run_date as date ))-1)
--((month(current_mo_id)-Month(prev_mo_id)=1) and (year(current_mo_id)-year(prev_mo_id)=0) or ((month(current_mo_id)-Month(prev_mo_id)=-11) and (year(current_mo_id)-year(prev_mo_id)=1)))
and cast(left(current_mo_id,4) as int )=(year(cast(:Run_date as date ))-1)
)y
group by 1,2,3,4,5
)x
 left join (select distinct PHY_COUNTY_CD,PHY_COUNTY_DESC,STATE_CD from DB_T_SHRD_PROD.COUNTY_LOOKUP where STATE_CD = ''AL'') c on c.PHY_COUNTY_CD=cast(x.old_county as varchar(10))
where c.STATE_CD = ''AL''
group by 1,2)w
order by 1,2
) SRC
)
);


-- Component exp_dtype_conversion, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_dtype_conversion AS
(
SELECT
TO_NUMBER(SQ_Memb_Cust.Year) as o_year,
SQ_Memb_Cust.county as county,
SQ_Memb_Cust.Lost as Lost,
SQ_Memb_Cust.Gained as Gained,
SQ_Memb_Cust.Net_Change as Net_Change,
SQ_Memb_Cust.source_record_id
FROM
SQ_Memb_Cust
);


-- Component FF_CountyChange, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE FF_CountyChange AS
(
SELECT
exp_dtype_conversion.o_year as Year,
exp_dtype_conversion.county as County,
exp_dtype_conversion.Lost as Lost,
exp_dtype_conversion.Gained as Gained,
exp_dtype_conversion.Net_Change as NetChange
FROM
exp_dtype_conversion
);

copy into @my_internal_stage/FF_CountyChange from (select * from FF_CountyChange)
header=true
overwrite=true;

-- Component FF_CountyChange, Type EXPORT_DATA Exporting data
;


END; ';