-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_DRVG_HIST_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 

declare
    start_dttm timestamp;
    end_dttm timestamp;
    PRCS_ID int;
BEGIN 

start_dttm := current_timestamp();
end_dttm := current_timestamp();
PRCS_ID := 1;


-- Component LKP_PCTL_NUMBEROFACCIDENTS, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_PCTL_NUMBEROFACCIDENTS AS
(
SELECT pctl_numberofaccidents.NAME_stg as NAME, pctl_numberofaccidents.L_en_US_stg as L_en_US, pctl_numberofaccidents.PRIORITY_stg as PRIORITY, pctl_numberofaccidents.TYPECODE_stg as TYPECODE, pctl_numberofaccidents.S_en_US_stg as S_en_US, pctl_numberofaccidents.RETIRED_stg as RETIRED, pctl_numberofaccidents.DESCRIPTION_stg as DESCRIPTION, pctl_numberofaccidents.ID_stg as ID FROM db_t_prod_stag.pctl_numberofaccidents
);


-- Component SQ_pc_policydrivermvr, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_pc_policydrivermvr AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as rank,
$2 as Points,
$3 as AddressBookUID,
$4 as ReportDate,
$5 as licensestate,
$6 as NumberofAccidents,
$7 as NumberOfMajorViolations_alfa,
$8 as NumberOfMinorViolations_alfa,
$9 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select  distinct rank() over ( partition by AddressBookUID order by EffectiveDate) rk, a.* 

from ( SELECT    distinct pc_policydrivermvr.Points, 

    pc_policydrivermvr.AddressBookUID as AddressBookUID,

    pc_policydrivermvr.EffectiveDate as EffectiveDate, pc_policydrivermvr.licensestate,

    pc_policydrivermvr.NumberofAccidents, pc_policydrivermvr.NumberOfMajorViolations_alfa,  pc_policydrivermvr.NumberOfMinorViolations_alfa

    FROM    (   SELECT distinct pc_policydrivermvr.Points_stg as Points

,case when pc_policydrivermvr.effectivedate_stg is NULL then pc_policyperiod.periodstart_stg           /*  EIM-36012  */
/*  ,case when pc_policydrivermvr.effectivedate_stg is NULL then pc_policydrivermvr.CreateTime_stg    -- EIM-36012 */
       else pc_policydrivermvr.effectivedate_stg  end as EffectiveDate

,pc_policydrivermvr.NumberOfAccidents_stg as NumberOfAccidents

,UPPER(pc_contact.AddressBookUID_stg) as AddressBookUID

,pctl_jurisdiction.TYPECODE_stg as licensestate

,pc_policydrivermvr.UpdateTime_stg as UpdateTime

,pc_policycontactrole.NumberOfViolations_stg as NumberOfMajorViolations_alfa    /* EIM-18379 */
,pc_policycontactrole.NumberOfMinorViolations_alfa_stg as NumberOfMinorViolations_alfa /* EIM-18379 */
FROM DB_T_PROD_STAG.pc_policydrivermvr

left outer join DB_T_PROD_STAG.pc_policycontactrole on pc_policycontactrole.id_stg = pc_policydrivermvr.PolicyDriver_stg

/* left outer join DB_T_PROD_STAG.pctl_policycontactrole on pc_policycontactrole.Subtype=pctl_policycontactrole.id */
left outer join DB_T_PROD_STAG.pc_contact on pc_policycontactrole.ContactDenorm_stg=pc_contact.ID_stg

inner join DB_T_PROD_STAG.pctl_contact on pctl_contact.id_stg=pc_contact.Subtype_stg

join DB_T_PROD_STAG.pc_policyperiod on pc_policyperiod.ID_stg=pc_policydrivermvr.BranchID_stg

and (pc_policydrivermvr.ExpirationDate_stg is NULL    /*  EIM-36012 start */
                  or pc_policydrivermvr.ExpirationDate_stg >            

                       case when pc_policyperiod.EditEffectiveDate_stg >= pc_policyperiod.ModelDate_stg then pc_policyperiod.EditEffectiveDate_stg 

                                    else pc_policyperiod.ModelDate_stg     

end)                                                                                /*  EIM-36012 end */
left join DB_T_PROD_STAG.pctl_jurisdiction on pc_contact.LicenseState_stg=pctl_jurisdiction.ID_stg

WHERE (  pc_policydrivermvr.UpdateTime_stg > (:start_dttm)

and pc_policydrivermvr.UpdateTime_stg <= ( :end_dttm) )

and pctl_contact.name_stg in (''Person'',''Adjudicator'',''User Contact'',''Vendor (Person)'',''Attorney'', ''Doctor'',''Policy Person'',''Contact'')

/* -and pc_policyperiod.status_stg = ''9'' */
and (pc_policydrivermvr.ExpirationDate_stg is null or pc_policydrivermvr.ExpirationDate_stg >:start_dttm)  /* EIM-29485 */
) as pc_policydrivermvr

    where   AddressBookUID is not NULL

QUALIFY ROW_NUMBER() OVER( PARTITION BY pc_policydrivermvr.AddressBookUID,CAST( pc_policydrivermvr.EffectiveDate  AS DATE) ORDER BY pc_policydrivermvr.Points desc, pc_policydrivermvr.EffectiveDate Desc, pc_policydrivermvr.UpdateTime Desc,pc_policydrivermvr.NumberOfAccidents DESC, pc_policydrivermvr.NumberOfMajorViolations_alfa DESC ,pc_policydrivermvr.NumberOfMinorViolations_alfa DESC ) = 1 /* EIM-29485 */
   )a order by 1,2
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
SQ_pc_policydrivermvr.Points as Points,
SQ_pc_policydrivermvr.AddressBookUID as AddressBookUID,
SQ_pc_policydrivermvr.ReportDate as ReportDate,
SQ_pc_policydrivermvr.licensestate as licensestate,
''US'' as COUNTRY_CD,
SQ_pc_policydrivermvr.rank as rank,
SQ_pc_policydrivermvr.NumberofAccidents as NumberofAccidents,
SQ_pc_policydrivermvr.NumberOfMajorViolations_alfa as NumberOfMajorViolations_alfa,
SQ_pc_policydrivermvr.NumberOfMinorViolations_alfa as NumberOfMinorViolations_alfa,
SQ_pc_policydrivermvr.source_record_id
FROM
SQ_pc_policydrivermvr
);


-- Component LKP_CTRY, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_CTRY AS
(
SELECT
LKP.CTRY_ID,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.CTRY_ID asc,LKP.GEOGRCL_AREA_SHRT_NAME asc,LKP.GEOGRCL_AREA_NAME asc,LKP.GEOGRCL_AREA_DESC asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_pass_through
LEFT JOIN (
SELECT CTRY.CTRY_ID as CTRY_ID, CTRY.GEOGRCL_AREA_NAME as GEOGRCL_AREA_NAME, CTRY.GEOGRCL_AREA_DESC as GEOGRCL_AREA_DESC, CTRY.EDW_STRT_DTTM as EDW_STRT_DTTM, CTRY.EDW_END_DTTM as EDW_END_DTTM, CTRY.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME 
FROM db_t_prod_core.CTRY
QUALIFY ROW_NUMBER() OVER(PARTITION BY GEOGRCL_AREA_SHRT_NAME 
ORDER BY EDW_END_DTTM desc) = 1
/* WHERE CTRY.EDW_END_DTTM=TO_DATE(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'') */
) LKP ON LKP.GEOGRCL_AREA_SHRT_NAME = exp_pass_through.COUNTRY_CD
QUALIFY RNK = 1
);


-- Component LKP_INDIV_CNT_MGR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INDIV_CNT_MGR AS
(
SELECT
LKP.INDIV_PRTY_ID,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.INDIV_PRTY_ID desc,LKP.NK_LINK_ID desc) RNK
FROM
exp_pass_through
LEFT JOIN (
SELECT 
	INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID, 
	INDIV.NK_LINK_ID as NK_LINK_ID 
FROM 
	db_t_prod_core.INDIV
WHERE
	INDIV.NK_PUBLC_ID IS NULL
) LKP ON LKP.NK_LINK_ID = exp_pass_through.AddressBookUID
QUALIFY RNK = 1
);


-- Component LKP_TERR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERR AS
(
SELECT
LKP.TERR_ID,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.TERR_ID asc,LKP.CTRY_ID asc,LKP.GEOGRCL_AREA_SHRT_NAME asc,LKP.GEOGRCL_AREA_NAME asc,LKP.GEOGRCL_AREA_DESC asc,LKP.GEOGRCL_AREA_STRT_DTTM asc,LKP.GEOGRCL_AREA_END_DTTM asc,LKP.LOCTR_SBTYPE_CD asc,LKP.GEOGRCL_AREA_SBTYPE_CD asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc) RNK
FROM
exp_pass_through
INNER JOIN LKP_CTRY ON exp_pass_through.source_record_id = LKP_CTRY.source_record_id
LEFT JOIN (
SELECT TERR.TERR_ID as TERR_ID, TERR.GEOGRCL_AREA_NAME as GEOGRCL_AREA_NAME, TERR.GEOGRCL_AREA_DESC as GEOGRCL_AREA_DESC, TERR.GEOGRCL_AREA_STRT_DTTM as GEOGRCL_AREA_STRT_DTTM, TERR.GEOGRCL_AREA_END_DTTM as GEOGRCL_AREA_END_DTTM, TERR.LOCTR_SBTYPE_CD as LOCTR_SBTYPE_CD, TERR.GEOGRCL_AREA_SBTYPE_CD as GEOGRCL_AREA_SBTYPE_CD, TERR.EDW_STRT_DTTM as EDW_STRT_DTTM, TERR.EDW_END_DTTM as EDW_END_DTTM, TERR.CTRY_ID as CTRY_ID, TERR.GEOGRCL_AREA_SHRT_NAME as GEOGRCL_AREA_SHRT_NAME FROM db_t_prod_core.TERR
QUALIFY ROW_NUMBER () OVER (PARTITION BY CTRY_ID,GEOGRCL_AREA_SHRT_NAME ORDER BY edw_end_dttm DESC)=1
) LKP ON LKP.CTRY_ID = LKP_CTRY.CTRY_ID AND LKP.GEOGRCL_AREA_SHRT_NAME = exp_pass_through.licensestate
QUALIFY RNK = 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
LKP_INDIV_CNT_MGR.INDIV_PRTY_ID as INDIV_PRTY_ID,
exp_pass_through.ReportDate as ReportDate,
exp_pass_through.Points as Points,
LKP_TERR.TERR_ID as TERR_ID,
CURRENT_TIMESTAMP as in_EDW_STRT_DTTM,
to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as in_EDW_END_DTTM,
exp_pass_through.rank as rank,
exp_pass_through.NumberofAccidents as out_ACDNT_CNT,
TO_NUMBER(LKP_1.NAME /* replaced lookup LKP_PCTL_NUMBEROFACCIDENTS */) as out_MAJ_CNVICTN_CNT,
TO_NUMBER(LKP_2.NAME /* replaced lookup LKP_PCTL_NUMBEROFACCIDENTS */) as out_MINOR_CNVICTN_CNT,
exp_pass_through.source_record_id,
row_number() over (partition by exp_pass_through.source_record_id order by exp_pass_through.source_record_id) as RNK
FROM
exp_pass_through
INNER JOIN LKP_INDIV_CNT_MGR ON exp_pass_through.source_record_id = LKP_INDIV_CNT_MGR.source_record_id
INNER JOIN LKP_TERR ON LKP_INDIV_CNT_MGR.source_record_id = LKP_TERR.source_record_id
LEFT JOIN LKP_PCTL_NUMBEROFACCIDENTS LKP_1 ON LKP_1.ID = exp_pass_through.NumberOfMajorViolations_alfa
LEFT JOIN LKP_PCTL_NUMBEROFACCIDENTS LKP_2 ON LKP_2.ID = exp_pass_through.NumberOfMinorViolations_alfa
QUALIFY row_number() over (partition by exp_pass_through.source_record_id order by exp_pass_through.source_record_id) 
= 1
);


-- Component LKP_PRTY_DRVG_HIST1, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_DRVG_HIST1 AS
(
SELECT
LKP.INDIV_PRTY_ID,
LKP.DRVRS_LIC_TERR_ID,
LKP.DRVG_HIST_DTTM,
LKP.PNLTY_PNTS_CNT,
LKP.ACDNT_CNT,
LKP.MAJ_CNVICTN_CNT,
LKP.MINOR_CNVICTN_CNT,
exp_data_transformation.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.INDIV_PRTY_ID asc,LKP.DRVRS_LIC_TERR_ID asc,LKP.DRVG_HIST_DTTM asc,LKP.PNLTY_PNTS_CNT asc,LKP.ACDNT_CNT asc,LKP.MAJ_CNVICTN_CNT asc,LKP.MINOR_CNVICTN_CNT asc) RNK
FROM
exp_data_transformation
LEFT JOIN (
SELECT PRTY_DRVG_HIST.DRVRS_LIC_TERR_ID as DRVRS_LIC_TERR_ID, PRTY_DRVG_HIST.DRVG_HIST_DTTM as DRVG_HIST_DTTM, -/* EIM-29485 */
PRTY_DRVG_HIST.PNLTY_PNTS_CNT as PNLTY_PNTS_CNT, PRTY_DRVG_HIST.ACDNT_CNT as ACDNT_CNT, PRTY_DRVG_HIST.MAJ_CNVICTN_CNT as MAJ_CNVICTN_CNT, PRTY_DRVG_HIST.MINOR_CNVICTN_CNT as MINOR_CNVICTN_CNT, /*  Added for EIM-18379 */
PRTY_DRVG_HIST.EDW_END_DTTM as EDW_END_DTTM, PRTY_DRVG_HIST.INDIV_PRTY_ID as INDIV_PRTY_ID FROM db_t_prod_core.PRTY_DRVG_HIST
QUALIFY ROW_NUMBER() OVER(PARTITION BY  INDIV_PRTY_ID , DRVG_HIST_DTTM ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.INDIV_PRTY_ID = exp_data_transformation.INDIV_PRTY_ID AND LKP.DRVG_HIST_DTTM = exp_data_transformation.ReportDate
QUALIFY ROW_NUMBER() OVER(PARTITION BY exp_data_transformation.source_record_id ORDER BY LKP.INDIV_PRTY_ID asc,LKP.DRVRS_LIC_TERR_ID asc,LKP.DRVG_HIST_DTTM asc,LKP.PNLTY_PNTS_CNT asc,LKP.ACDNT_CNT asc,LKP.MAJ_CNVICTN_CNT asc,LKP.MINOR_CNVICTN_CNT asc)  
= 1
);


-- Component exp, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp AS
(
SELECT
exp_data_transformation.INDIV_PRTY_ID as in_INDIV_PRTY_ID,
exp_data_transformation.ReportDate as in_DRVG_HIST_DTTM,
exp_data_transformation.Points as in_PNLTY_PNTS_CNT,
exp_data_transformation.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp_data_transformation.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp_data_transformation.out_ACDNT_CNT as in_ACDNT_CNT,
exp_data_transformation.out_MAJ_CNVICTN_CNT as in_MAJ_CNVICTN_CNT,
exp_data_transformation.out_MINOR_CNVICTN_CNT as in_MINOR_CNVICTN_CNT,
CASE WHEN exp_data_transformation.Points IS NULL and not ( exp_data_transformation.INDIV_PRTY_ID IS NULL ) THEN 0 ELSE exp_data_transformation.Points END as Points_var,
exp_data_transformation.TERR_ID as in_TERR_ID,
:PRCS_ID as in_PRCS_ID,
LKP_PRTY_DRVG_HIST1.INDIV_PRTY_ID as lkp_INDIV_PRTY_ID,
LKP_PRTY_DRVG_HIST1.PNLTY_PNTS_CNT as lkp_PNLTY_PNTS_CNT,
LKP_PRTY_DRVG_HIST1.DRVRS_LIC_TERR_ID as lkp_TERR_ID,
md5 ( ltrim ( rtrim ( to_char ( exp_data_transformation.Points ) ) ) || ltrim ( rtrim ( to_char ( exp_data_transformation.TERR_ID ) ) ) || ltrim ( rtrim ( to_char ( exp_data_transformation.out_ACDNT_CNT ) ) ) || ltrim ( rtrim ( to_char ( exp_data_transformation.out_MAJ_CNVICTN_CNT ) ) ) || ltrim ( rtrim ( to_char ( exp_data_transformation.out_MINOR_CNVICTN_CNT ) ) ) || ltrim ( rtrim ( exp_data_transformation.ReportDate ) ) ) as v_md5_src,
md5 ( ltrim ( rtrim ( to_char ( LKP_PRTY_DRVG_HIST1.PNLTY_PNTS_CNT ) ) ) || ltrim ( rtrim ( to_char ( LKP_PRTY_DRVG_HIST1.DRVRS_LIC_TERR_ID ) ) ) || ltrim ( rtrim ( to_char ( LKP_PRTY_DRVG_HIST1.ACDNT_CNT ) ) ) || ltrim ( rtrim ( to_char ( LKP_PRTY_DRVG_HIST1.MAJ_CNVICTN_CNT ) ) ) || ltrim ( rtrim ( to_char ( LKP_PRTY_DRVG_HIST1.MINOR_CNVICTN_CNT ) ) ) || ltrim ( rtrim ( LKP_PRTY_DRVG_HIST1.DRVG_HIST_DTTM ) ) ) as v_md5_tgt,
CASE WHEN v_md5_tgt IS NULL THEN ''I'' ELSE CASE WHEN v_md5_tgt != v_md5_src THEN ''U'' ELSE ''R'' END END as o_Ins_Upd,
exp_data_transformation.rank as rank,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
INNER JOIN LKP_PRTY_DRVG_HIST1 ON exp_data_transformation.source_record_id = LKP_PRTY_DRVG_HIST1.source_record_id
);


-- Component rtr_prty_autmbl_drvg_hist_INSERT, Type ROUTER Output Group INSERT
create OR REPLACE TEMPORARY TABLE rtr_prty_autmbl_drvg_hist_INSERT AS
SELECT
exp.in_INDIV_PRTY_ID as in_INDIV_PRTY_ID,
exp.in_DRVG_HIST_DTTM as in_DRVG_HIST_DTTM,
exp.in_PNLTY_PNTS_CNT as in_PNLTY_PNTS_CNT,
exp.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM,
exp.in_EDW_END_DTTM as in_EDW_END_DTTM,
exp.in_ACDNT_CNT as in_ACDNT_CNT,
exp.in_MAJ_CNVICTN_CNT as in_MAJ_CNVICTN_CNT,
exp.in_MINOR_CNVICTN_CNT as in_MINOR_CNVICTN_CNT,
exp.in_TERR_ID as in_TERR_ID,
exp.in_PRCS_ID as in_PRCS_ID,
exp.lkp_INDIV_PRTY_ID as lkp_INDIV_PRTY_ID,
exp.lkp_PNLTY_PNTS_CNT as lkp_PNLTY_PNTS_CNT,
exp.lkp_TERR_ID as lkp_TERR_ID,
exp.o_Ins_Upd as o_Ins_Upd,
exp.rank as rank,
exp.source_record_id
FROM
exp
WHERE ( exp.o_Ins_Upd = ''U'' and exp.in_INDIV_PRTY_ID IS NOT NULL ) or ( exp.o_Ins_Upd = ''I'' and exp.in_INDIV_PRTY_ID IS NOT NULL );


-- Component upd_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_prty_autmbl_drvg_hist_INSERT.in_INDIV_PRTY_ID as in_INDIV_PRTY_ID1,
rtr_prty_autmbl_drvg_hist_INSERT.in_DRVG_HIST_DTTM as in_DRVG_HIST_DTTM1,
rtr_prty_autmbl_drvg_hist_INSERT.in_TERR_ID as in_TERR_ID1,
rtr_prty_autmbl_drvg_hist_INSERT.in_PNLTY_PNTS_CNT as in_PNLTY_PNTS_CNT1,
rtr_prty_autmbl_drvg_hist_INSERT.in_EDW_STRT_DTTM as in_EDW_STRT_DTTM1,
rtr_prty_autmbl_drvg_hist_INSERT.in_EDW_END_DTTM as in_EDW_END_DTTM1,
rtr_prty_autmbl_drvg_hist_INSERT.in_PRCS_ID as in_PRCS_ID1,
rtr_prty_autmbl_drvg_hist_INSERT.rank as rank1,
rtr_prty_autmbl_drvg_hist_INSERT.in_ACDNT_CNT as in_ACDNT_CNT1,
rtr_prty_autmbl_drvg_hist_INSERT.in_MAJ_CNVICTN_CNT as in_MAJ_CNVICTN_CNT1,
rtr_prty_autmbl_drvg_hist_INSERT.in_MINOR_CNVICTN_CNT as in_MINOR_CNVICTN_CNT1,
0 as UPDATE_STRATEGY_ACTION,
rtr_prty_autmbl_drvg_hist_INSERT.source_record_id as source_record_id
FROM
rtr_prty_autmbl_drvg_hist_INSERT
);


-- Component exp_pass_to_tgt_Ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_tgt_Ins AS
(
SELECT
upd_insert.in_INDIV_PRTY_ID1 as in_INDIV_PRTY_ID1,
upd_insert.in_DRVG_HIST_DTTM1 as in_DRVG_HIST_DTTM1,
upd_insert.in_TERR_ID1 as in_TERR_ID1,
upd_insert.in_PNLTY_PNTS_CNT1 as in_PNLTY_PNTS_CNT1,
upd_insert.in_EDW_END_DTTM1 as in_EDW_END_DTTM1,
upd_insert.in_PRCS_ID1 as in_PRCS_ID1,
dateadd ( second, ( 2 * ( upd_insert.rank1 - 1 ) ), CURRENT_TIMESTAMP  ) as out_EDW_START_DTTM,
upd_insert.in_ACDNT_CNT1 as in_ACDNT_CNT1,
upd_insert.in_MAJ_CNVICTN_CNT1 as in_MAJ_CNVICTN_CNT1,
upd_insert.in_MINOR_CNVICTN_CNT1 as in_MINOR_CNVICTN_CNT1,
upd_insert.source_record_id
FROM
upd_insert
);


-- Component PRTY_DRVG_HIST_ins, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_DRVG_HIST
(
INDIV_PRTY_ID,
DRVG_HIST_DTTM,
ACDNT_CNT,
MINOR_CNVICTN_CNT,
PNLTY_PNTS_CNT,
MAJ_CNVICTN_CNT,
DRVRS_LIC_TERR_ID,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
exp_pass_to_tgt_Ins.in_INDIV_PRTY_ID1 as INDIV_PRTY_ID,
exp_pass_to_tgt_Ins.in_DRVG_HIST_DTTM1 as DRVG_HIST_DTTM,
exp_pass_to_tgt_Ins.in_ACDNT_CNT1 as ACDNT_CNT,
exp_pass_to_tgt_Ins.in_MINOR_CNVICTN_CNT1 as MINOR_CNVICTN_CNT,
exp_pass_to_tgt_Ins.in_PNLTY_PNTS_CNT1 as PNLTY_PNTS_CNT,
exp_pass_to_tgt_Ins.in_MAJ_CNVICTN_CNT1 as MAJ_CNVICTN_CNT,
exp_pass_to_tgt_Ins.in_TERR_ID1 as DRVRS_LIC_TERR_ID,
exp_pass_to_tgt_Ins.in_PRCS_ID1 as PRCS_ID,
exp_pass_to_tgt_Ins.out_EDW_START_DTTM as EDW_STRT_DTTM,
exp_pass_to_tgt_Ins.in_EDW_END_DTTM1 as EDW_END_DTTM
FROM
exp_pass_to_tgt_Ins;


-- Component PRTY_DRVG_HIST_ins, Type Post SQL 
UPDATE  db_t_prod_core.PRTY_DRVG_HIST  
SET EDW_END_DTTM= A.lead1
FROM  

(

SELECT	distinct INDIV_PRTY_ID, EDW_STRT_DTTM,

max(EDW_STRT_DTTM) over (partition by INDIV_PRTY_ID ORDER BY EDW_STRT_DTTM ASC rows between 1 following and 1 following) - INTERVAL ''1 SECOND''  as lead1

FROM	db_t_prod_core.PRTY_DRVG_HIST

)  A


WHERE  PRTY_DRVG_HIST.INDIV_PRTY_ID=A.INDIV_PRTY_ID

AND  PRTY_DRVG_HIST.EDW_STRT_DTTM=A.EDW_STRT_DTTM

and A.lead1 is not null;


END; ';