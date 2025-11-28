-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_ALFA_LIFT_011_AIUA_AL_FIRE_FARM_BREEDHO("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' DECLARE PC_EOY varchar;
PC_BOY varchar;
BEGIN 

PC_EOY:=''1900-01-01''; 
PC_BOY:=''1900-01-01''; 

-- Component SQ_pc_policyperiod, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_policyperiod AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PolicyNumber,
$2 as Premium,
$3 as PolicyType,
$4 as State,
$5 as County,
$6 as ModelDate,
$7 as EditEffectivveDate,
$8 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELect	POLICYNUMBER_STG AS PolicyNumber ,
		writtenPremium AS Premium,
		PolicyType,State,County,MODELDATE_STG AS ModelDate,
		EditeffectiveDate
FROM	(

		select	DISTINCT  MODELDATE_STG,(

--        case
	--	                when 1 = (
		--		        select	 1
			--	        where	  exists(
				--		            select	 pc_policyperiod2.policynumber_stg
					--	            from	   DB_T_PROD_STAG.pc_policyperiod pc_policyperiod2 join DB_T_PROD_STAG.pc_policyterm pt2
					--	                on pt2.ID_stg = pc_policyperiod2.PolicyTermID_stg join DB_T_PROD_STAG.pc_policyline
						      --          on pc_policyperiod2.id_stg = pc_policyline.BranchID_stg join DB_T_PROD_STAG.pctl_hopolicytype_hoe
						    --            on pc_policyline.HOPolicyType_stg = pctl_hopolicytype_hoe.ID_stg
						  --              and pctl_hopolicytype_hoe.TypeCode_stg in (''SF1'',
						--		''SF2'',''SF3'',''SF4'') join DB_T_PROD_STAG.pc_job job2
					--	                on job2.ID_stg = pc_policyperiod2.jobID_stg join DB_T_PROD_STAG.pctl_job pctl_job2
					--	                on pctl_job2.ID_stg = job2.Subtype_stg
				--		            where	  pctl_job2.Name_stg = ''Renewal''
			--			                and (pt.ConfirmationDate_alfa_stg > :PC_EOY
		--				                or pt.ConfirmationDate_alfa_stg is NULL)
	--					                and pc_policyperiod2.PolicyNumber_stg = pp.PolicyNumber_stg
--						                and pc_policyperiod2.TermNumber_stg = pp.TermNumber_stg )) then 0
--		                else  pp.TransactionPremiumRPT_stg  end
                        
                        nvl(pp.TransactionPremiumRPT_stg,0)
                        ) as writtenPremium,
				pp.POLICYNUMBER_stg,phth.id_stg,ph.typecode_stg as PolicyType,
				JD.TYPECODE_stg as  State,pl.CountyInternal_stg as County,
			pp.EditEffectiveDate_stg as EditeffectiveDate
		    FROM	   DB_T_PROD_STAG.pcx_hotransaction_hoe PHTH JOIN DB_T_PROD_STAG.pcx_homeownerscost_hoe PHCH
			ON PHTH.HomeownersCost_stg = PHCH.ID_stg JOIN DB_T_PROD_STAG.pc_policyperiod pp
		    ON PHTH.BRANCHID_stg =PP.ID_stg JOIN DB_T_PROD_STAG.PC_UWCOMPANY UWC
			ON pp.UWCOMPANY_stg=UWC.ID_stg JOIN DB_T_PROD_STAG.PCTL_JURISDICTION JD
		    ON pp.BASESTATE_stg=JD.ID_stg JOIN DB_T_PROD_STAG.pcx_dwelling_hoe pdh
		    ON pdh.BranchID_stg=pp.ID_stg
		    and pdh.expirationdate_stg is null
		    left JOIN DB_T_PROD_STAG.PCTL_PERILTYPE_ALFA PERIL
		    ON PERILTYPE_ALFA_stg=PERIL.ID_stg
		    left JOIN DB_T_PROD_STAG.pctl_dwellingoccupancytype_hoe OCC_TYPE
		        ON PDH.OCCUPANCY_stg =OCC_TYPE.ID_stg
		    left join DB_T_PROD_STAG.pctl_dwellingusage_hoe pcdh
		        on pcdh.id_stg =pdh.DwellingUsage_stg
		        join DB_T_PROD_STAG.pcx_holineschcovitemcov_alfa pha
		        on pha.BranchID_stg = pp.ID_stg
		    join  DB_T_PROD_STAG.pcx_homeownerslinecov_hoe phh
		        on phh.BranchID_stg = pp.ID_stg
		        and phh.PatternCode_stg = ''HOLI_SpecificOtherStructureSchedule_alfa''
		        and pha.ChoiceTerm3_stg   in (''barn'',
				''breedho'',''broilho'',''layho'',''dwlgbarn'',''grnstorgbin'',''polebarn'',
				''silo'')
		    left join  DB_T_PROD_STAG.pc_effectivedatedfields eff
		           on eff.BranchID_stg = pp.ID_stg 
			and eff.expirationdate_stg is null
		    join DB_T_PROD_STAG.pc_policylocation pl
		        on eff.primarylocation_stg = pl.id_stg
		    join DB_T_PROD_STAG.pc_policyline     
		          on pp.id_stg = pc_policyline.BranchID_stg  
		    join DB_T_PROD_STAG.pctl_hopolicytype_hoe ph    
		          on pc_policyline.HOPolicyType_stg = ph.ID_stg  
			and ph.TypeCode_stg in ( ''SF1'',
			''SF2'',''SF3'',''SF4'')                           
		    LEFT JOIN DB_T_PROD_STAG.pctl_constructiontype_hoe PCT
		        ON PCT.ID_stg=constructiontype_stg join DB_T_PROD_STAG.pctl_policyperiodstatus
		        on pp.status_stg=pctl_policyperiodstatus.ID_stg
		        and pctl_policyperiodstatus.typecode_stg=''Bound'' join DB_T_PROD_STAG.pc_policyterm pt
		        on pt.ID_stg = pp.PolicyTermID_stg
		        and
		            Case
		                when pp.EditEffectiveDate_stg >= pp.ModelDate_stg
		        AND pp.EditEffectiveDate_stg>= coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp),
		            CAST(''1900-01-01 00:00:00.000000''AS timestamp)) then pp.EditEffectiveDate_stg
		                when coalesce(CAST(PT.ConfirmationDate_alfa_stg as timestamp),
		            CAST(''1900-01-01 00:00:00.000000'' as timestamp)) >= pp.ModelDate_stg THEN coalesce(CAST(PT.ConfirmationDate_alfa_stg AS timestamp),
		            CAST(''1900-01-01 00:00:00.000000'' as timestamp))
		                else pp.ModelDate_stg
		            end      between :PC_BOY        
		        and :PC_EOY
			AND JD.TYPECODE_STG =''AL'')A 
GROUP BY 1,2,3,4,5,6,7
--		WHERE	 DB_T_PROD_COMN.Premium <>0
) SRC
)
);


-- Component EXPTRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXPTRANS AS
(
SELECT
LTRIM ( RTRIM ( SQ_pc_policyperiod.PolicyNumber ) ) as o_PolicyNumber,
LTRIM ( RTRIM ( SQ_pc_policyperiod.Premium ) ) as o_Premium,
LTRIM ( RTRIM ( SQ_pc_policyperiod.PolicyType ) ) as o_Policy_type,
LTRIM ( RTRIM ( SQ_pc_policyperiod.State ) ) as o_state,
LTRIM ( RTRIM ( SQ_pc_policyperiod.County ) ) as o_County,
SQ_pc_policyperiod.ModelDate as ModelDate,
SQ_pc_policyperiod.EditEffectivveDate as EditEffectivveDate,
SQ_pc_policyperiod.source_record_id
FROM
SQ_pc_policyperiod
);


-- Component Sorter, Type SORTER 
CREATE OR REPLACE TEMPORARY TABLE Sorter AS
(
SELECT
EXPTRANS.o_PolicyNumber as o_PolicyNumber,
EXPTRANS.o_Premium as o_Premium,
EXPTRANS.o_Policy_type as o_Policy_type,
EXPTRANS.o_state as o_state,
EXPTRANS.o_County as o_County,
EXPTRANS.ModelDate as ModelDate,
EXPTRANS.EditEffectivveDate as EditEffectivveDate,
EXPTRANS.source_record_id
FROM
EXPTRANS
ORDER BY o_PolicyNumber 
);


-- Component FF_AIUA_BREED_HO, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE FF_AIUA_BREED_HO AS
(
SELECT
Sorter.o_PolicyNumber as PolicyName,
Sorter.o_Premium as Premium,
Sorter.o_Policy_type as PolicyType,
Sorter.o_state as State,
Sorter.o_County as County,
Sorter.ModelDate as ModelDate,
Sorter.EditEffectivveDate as EffectiveDate
FROM
Sorter
);

copy into @my_internal_stage/FF_AIUA_BREED_HO from (select * from FF_AIUA_BREED_HO)
header=true
overwrite=true;

-- Component FF_AIUA_BREED_HO, Type EXPORT_DATA Exporting data
;


END; ';