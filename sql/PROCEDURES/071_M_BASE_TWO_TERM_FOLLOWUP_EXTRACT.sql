-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_TWO_TERM_FOLLOWUP_EXTRACT("PARAM_JSON" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' BEGIN 

-- Component SQ_Two_Term_Followup, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_Two_Term_Followup AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as policynumber,
$2 as jobnumber,
$3 as user,
$4 as role,
$5 as date,
$6 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select	

		case 

			when Policy_Number is null then ''No data'' 

			else Policy_Number 

		end ,coalesce( Job_number,'''') Job_number, 

coalesce(name,'''') name,

		coalesce(role1,'''')role1,cast(CloseDate as timestamp) 

from	DB_SP_PROD.calendar  

left join   (

	select	   distinct pp.PolicyNumber_stg as Policy_Number ,

			job.JobNumber_stg as Job_number,

	             concat(cont.FirstName_stg,

			'' '',cont.LastName_stg) name, r.Name_stg as Role1,job.CloseDate_stg  as CloseDate

	

	

	

	from	   DB_T_PROD_STAG.pc_job job

	     left join DB_T_PROD_STAG.pctl_job tl 

	    on job.Subtype_stg = tl.ID_stg

	     left join DB_T_PROD_STAG.pc_policyperiod pp 

	    on pp.JobID_stg = job.ID_stg

	     left join DB_T_PROD_STAG.pctl_policyperiodstatus pptl 

	    on pptl.ID_stg = pp.Status_stg

	     inner join DB_T_PROD_STAG.pc_user u 

	    on u.Id_stg = job.UpdateUserID_stg

	        left join DB_T_PROD_STAG.pc_contact cont 

	    on cont.ID_stg = u.ContactID_stg

	        left join DB_T_PROD_STAG.pc_userrole ur 

	    on ur.UserID_stg= u.ID_stg

	        left join DB_T_PROD_STAG.pc_role r 

	    on r.ID_stg= ur.RoleID_stg

	

	

	

	inner join(

		    select	   distinct pp.PolicyNumber_stg,

				pp.EditEffectiveDate_stg

		                from	   DB_T_PROD_STAG.pc_job job

		         left join DB_T_PROD_STAG.pctl_job tl 

		        on job.Subtype_stg= tl.ID_stg

		         left join DB_T_PROD_STAG.pc_policyperiod pp 

		        on pp.JobID_stg = job.ID_stg

		         where	   tl.Name_stg=''Renewal'')iq 

	    on iq.PolicyNumber_stg=pp.PolicyNumber_stg 

	    and iq.EditEffectiveDate_stg=pp.EditEffectiveDate_stg

	

	

	

	where	   isappliedtofuturerenewal_alfa_stg=1

	    and tl.Name_stg=''Policy Change''

	    and pptl.Name_stg = ''Withdrawn''

	    and job.CloseDate_stg >=$start_dttm

	    and job.CloseDate_stg <$end_dttm

	

)a 

	on 1=1 

where	 calendar_date =current_date

order by closedate
) SRC
)
);


-- Component EXP_PASSTHRU, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_PASSTHRU AS
(
SELECT
SQ_Two_Term_Followup.policynumber as policynumber,
SQ_Two_Term_Followup.jobnumber as jobnumber,
SQ_Two_Term_Followup.date as date,
SQ_Two_Term_Followup.user as user,
SQ_Two_Term_Followup.role as role,
SQ_Two_Term_Followup.source_record_id
FROM
SQ_Two_Term_Followup
);


-- Component Two_Term_Followup_FF, Type TARGET_EXPORT_PREPARE Stage data before exporting
CREATE OR REPLACE TEMPORARY TABLE Two_Term_Followup_FF AS
(
SELECT
EXP_PASSTHRU.policynumber as Policy_Number,
EXP_PASSTHRU.jobnumber as Job_Number,
EXP_PASSTHRU.date as Withdrawn_Date,
EXP_PASSTHRU.user as User_Name,
EXP_PASSTHRU.role as Role
FROM
EXP_PASSTHRU
);


-- Component Two_Term_Followup_FF, Type EXPORT_DATA Exporting data
;


END; ';