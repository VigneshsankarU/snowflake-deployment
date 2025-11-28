-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_CLM_EXPSR_FEAT_TERM_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
       run_id STRING;
       start_dttm STRING;
       end_dttm STRING;
       PRCS_ID STRING;
       SRC_SYS6_GWCC STRING;
	   v_start_time TIMESTAMP;
BEGIN
       run_id := (SELECT run_id FROM control_worklet WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
       start_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''start_dttm'' LIMIT 1);
       end_dttm := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''end_dttm'' LIMIT 1);
       PRCS_ID := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''PRCS_ID'' LIMIT 1);
       SRC_SYS6_GWCC := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''SRC_SYS6_GWCC'' LIMIT 1);
	   v_start_time := CURRENT_TIMESTAMP();

-- Component SQ_cc_clm_expsr_feat_term_x, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE SQ_cc_clm_expsr_feat_term_x AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as PolicyNumber,
$2 as claimnumber,
$3 as Exposure_ID,
$4 as Incident_Term,
$5 as Exposure_Term,
$6 as Deductible_Term,
$7 as Incident_Direct_Amt,
$8 as Exposure_Direct_Amt,
$9 as Deductible_Direct_Amt,
$10 as SRC_SYS_CD,
$11 as LKP_CLM_EXPSR_FEAT_TERM_ID,
$12 as LKP_EDW_END_DTTM,
$13 as LKP_INCIDENT_FEAT_ID,
$14 as LKP_EXPOSURE_FEAT_ID,
$15 as LKP_DEDUCTIBLE_FEAT_ID,
$16 as LKP_INCIDENT_AMT,
$17 as LKP_EXPOSURE_AMT,
$18 as LKP_DEDUCTIBLE_AMT,
$19 as FEAT_ID,
$20 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
select PolicyNumber,claimnumber,CLM_EXPS.CLM_EXPSR_ID Exposure_ID,Incident_Term,Exposure_Term,Deductible_Term,

Incident_Direct_Amt,Exposure_Direct_Amt,Deductible_Direct_Amt,/*''GWCC''*/:SRC_SYS6_GWCC,TGT.CLM_EXPSR_FEAT_TERM_ID,

TGT.EDW_STRT_DTTM,

INC_TERM.FEAT_ID INC_TERM_FEAT_ID, EXP_TERM.FEAT_ID EXP_TERM_FEAT_ID, DED_TERM.FEAT_ID DED_TERM_FEAT_ID,

TGT.INCDT_DIR_AMT,TGT.EXPSR_DIR_AMT,TGT.DEDCTB_DIR_AMT,

COALESCE(INC_TERM_FEAT_ID,EXP_TERM_FEAT_ID,DED_TERM_FEAT_ID)FEAT_ID

 from (SELECT cc_clm_expsr_feat_term_x.PolicyNumber, 

cc_clm_expsr_feat_term_x.claimnumber,

cc_clm_expsr_feat_term_x.Exposure_ID, 

max(cc_clm_expsr_feat_term_x.Incident_Term) as Incident_Term, 

max(cc_clm_expsr_feat_term_x.Exposure_Term) as Exposure_Term, 

max(cc_clm_expsr_feat_term_x.Deductible_Term) as Deductible_Term, 

max(cc_clm_expsr_feat_term_x.Incident_Direct_Amt) as Incident_Direct_Amt, 

max(cc_clm_expsr_feat_term_x.Exposure_Direct_Amt) as Exposure_Direct_Amt, 

max(cc_clm_expsr_feat_term_x.Deductible_Direct_Amt) as Deductible_Direct_Amt 

FROM

(

SELECT SRC.PolicyNumber

,SRC.ClaimNumber

, pc.CovTermType_stg as CovTermType

,SRC.Exposure_ID as Exposure_ID

,SRC.PackTerm

,SRC.Allamounts

,case when (SRC.Amount_Type = ''INCIDENT LIMIT'')then PackTerm else NULL end as Incident_Term

,case when (SRC.Amount_Type = ''EXPOSURE LIMIT'') then PackTerm else NULL end as Exposure_Term

,case when (SRC.Amount_Type = ''DEDUCTIBLE'') then PackTerm else NULL end as Deductible_Term

,CAST(case when (pc.CovTermType_stg = ''Direct'' and SRC.Amount_Type = ''INCIDENT LIMIT'') then SRC.Allamounts else 0.00 end as dec(9,2)) as Incident_Direct_Amt

,CAST(case when (pc.CovTermType_stg = ''Direct'' and SRC.Amount_Type = ''EXPOSURE LIMIT'') then SRC.Allamounts else 0.00 end as dec(9,2)) as Exposure_Direct_Amt

,CAST(case when (pc.CovTermType_stg = ''Direct'' and SRC.Amount_Type = ''DEDUCTIBLE'') then SRC.Allamounts else 0.00 end as dec(9,2)) as Deductible_Direct_Amt

FROM (

select 

e.PolicyNumber_stg as PolicyNumber

,f.ClaimNumber_stg as ClaimNumber

,h.publicid_stg as Exposure_ID

,case when (SUBSTR(b.PolicySystemID_stg, position(''.'' in b.PolicySystemID_stg) + 1 )) not like ''%.%'' then 

(SUBSTR(b.PolicySystemID_stg, position(''.'' in b.PolicySystemID_stg) + 1 )) end as PackTerm

,SUBSTR(b.PolicySystemID_stg, position(''.'' in b.PolicySystemID_stg) + 1 ) as CovTermType1

, case when(g.TYPECODE_stg) = ''FinancialCovTerm'' then cast(b.FinancialAmount_stg as varchar(255)) else case when (g.TYPECODE_stg) = ''NumericCovTerm'' then cast(b.NumericValue_stg as varchar(255)) else 

case when(g.TYPECODE_stg) = ''ClassificationCovTerm'' then b.Description_stg else null end end end as allamounts

,case when (( g.TYPECODE_stg ) <> ''ClassificationCovTerm'' and Allamounts = a.ExposureLimit_stg) then ''EXPOSURE LIMIT'' else

case when((g.TYPECODE_stg)<> ''ClassificationCovTerm'' and Allamounts = a.Deductible_stg) then ''DEDUCTIBLE'' else

case when( (g.TYPECODE_stg) <> ''ClassificationCovTerm'' and Allamounts = a.IncidentLimit_stg) then ''INCIDENT LIMIT'' end end end as AMOUNT_TYPE



from DB_T_PROD_STAG.cc_coverage a

inner join DB_T_PROD_STAG.cc_coverageterms b on b.CoverageID_stg = a.ID_stg

inner join DB_T_PROD_STAG.cctl_coveragetype c on c.ID_stg = a.Type_stg

inner join DB_T_PROD_STAG.cctl_covtermpattern d on d.ID_stg = b.CovTermPattern_stg

inner join DB_T_PROD_STAG.cc_policy e on e.ID_stg = a.PolicyID_stg

inner join DB_T_PROD_STAG.cc_claim f on f.PolicyID_stg = e.ID_stg

inner join DB_T_PROD_STAG.cctl_covterm g on g.ID_stg = b.Subtype_stg

inner join DB_T_PROD_STAG.cc_exposure h on h.CoverageID_stg = a.ID_stg

where SUBSTR(b.PolicySystemID_stg, position(''.'' in b.PolicySystemID_stg) + 1 ) like ''%.%'' and

d.name_stg in (''Limit'', ''Deductible'')

and h.UpdateTime_stg > (:start_dttm) and h.UpdateTime_stg <= (:end_dttm)

) SRC

/* ----------------lookup DB_T_PROD_STAG.pc_etlcovtermpattern */
LEFT OUTER JOIN DB_T_PROD_STAG.pc_etlcovtermpattern pc on SRC.CovTermType1=pc.PatternID_stg



UNION



SELECT SRC.PolicyNumber

,SRC.claimnumber as claimnumber

, lkp_pc.CovTermType_stg as CovTermType

,SRC.Exposure_ID as Exposure_ID

, case when (pc.PatternID_h is NOT NULL) then pc.PatternID_h else

case when (pc.PatternID_i is NOT NULL) then pc.PatternID_i else

case when (pc.PatternID_k is NOT NULL) then pc.PatternID_k else

case when (lkp_pc.CovTermType_stg in(''Direct'',''Option'')) then SRC.TYPECODE else

case when ( lkp_pc.CovTermType_stg = ''bit'') then SRC.TYPECODE else

case when (lkp_pc.CovTermType_stg= ''shorttext'') then SRC.TYPECODE else

''X'' end end end end end end as PackTerm

,SRC.Allamounts

,case when (SRC.Amount_Type = ''INCIDENT LIMIT'')then PackTerm else NULL end as Incident_Term

,case when (SRC.Amount_Type = ''EXPOSURE LIMIT'') then PackTerm else NULL end as Exposure_Term

,case when (SRC.Amount_Type = ''DEDUCTIBLE'') then PackTerm else NULL end as Deductible_Term

,CAST(case when (lkp_pc.CovTermType_stg in(''Direct'',''Option'') and SRC.Amount_Type = ''INCIDENT LIMIT'') then SRC.Allamounts else 0.00 end as dec(9,2)) as Incident_Direct_Amt

,CAST(case when (lkp_pc.CovTermType_stg in(''Direct'',''Option'') and SRC.Amount_Type = ''EXPOSURE LIMIT'') then SRC.Allamounts else 0.00 end as dec(9,2)) as Exposure_Direct_Amt

,CAST(case when (lkp_pc.CovTermType_stg in(''Direct'',''Option'') and SRC.Amount_Type = ''DEDUCTIBLE'') then SRC.Allamounts else 0.00 end as dec(9,2)) as Deductible_Direct_Amt

FROM (

select e.PolicyNumber_stg as PolicyNumber

, f.claimnumber_stg as claimnumber

,h.publicid_stg as Exposure_ID

, b.FinancialAmount_stg as PackTerm1

,SUBSTR(b.PolicySystemID_stg, position(''.'' in b.PolicySystemID_stg) + 1 ) as CovTermType1

, case when(g.TYPECODE_stg) = ''FinancialCovTerm'' then cast(b.FinancialAmount_stg as varchar(255)) else case when (g.TYPECODE_stg) = ''NumericCovTerm'' then cast(b.NumericValue_stg as varchar(255)) else 

case when(g.TYPECODE_stg) = ''ClassificationCovTerm'' then b.Description_stg else null end end end as allamounts

,case when (( g.TYPECODE_stg ) <> ''ClassificationCovTerm'' and Allamounts = a.ExposureLimit_stg) then ''EXPOSURE LIMIT'' else

case when((g.TYPECODE_stg)<> ''ClassificationCovTerm'' and Allamounts = a.Deductible_stg) then ''DEDUCTIBLE'' else

case when( (g.TYPECODE_stg) <> ''ClassificationCovTerm'' and Allamounts = a.IncidentLimit_stg) then ''INCIDENT LIMIT'' end end end as AMOUNT_TYPE

, a.Deductible_stg as Deductible

, a.IncidentLimit_stg as IncidentLimit

,d.typecode_stg as TYPECODE

,g.TYPECODE_stg as TYPECODE2



from DB_T_PROD_STAG.cc_coverage a

inner join DB_T_PROD_STAG.cc_coverageterms b on b.CoverageID_stg = a.ID_stg

inner join DB_T_PROD_STAG.cctl_coveragetype c on c.ID_stg = a.Type_stg

inner join DB_T_PROD_STAG.cctl_covtermpattern d on d.ID_stg = b.CovTermPattern_stg

inner join DB_T_PROD_STAG.cc_policy e on e.ID_stg = a.PolicyID_stg

inner join DB_T_PROD_STAG.cc_claim f on f.PolicyID_stg = e.ID_stg

inner join DB_T_PROD_STAG.cctl_covterm g on g.ID_stg = b.Subtype_stg

inner join DB_T_PROD_STAG.cc_exposure h on h.CoverageID_stg = a.ID_stg

where SUBSTR(b.PolicySystemID_stg, position(''.'' in b.PolicySystemID_stg) + 1 ) not like ''%.%'' and 

b.code_stg is null

and b.NumericValue_stg is null

and a.retired_stg = 0 

/* and d.name_stg in (''Limit'', ''Deductible'') */
and h.UpdateTime_stg > (:start_dttm) and h.UpdateTime_stg <= (:end_dttm)

) SRC

/* ----------------lookup pc_etlcovtermpattern1 */
LEFT OUTER JOIN (

SELECT i.PatternID_stg as PatternID_i, k.PatternID_stg as PatternID_k, g.PatternID_stg as PatternID_g, g.CovTermType_stg as CovTermType, h.PatternID_stg as PatternID_h, h.Value_stg as Value_h, i.Value_stg as Value_i, k.Value_stg as Value_k 

FROM DB_T_PROD_STAG.pc_etlcovtermpattern g

   left join DB_T_PROD_STAG.pc_etlcovtermoption h on h.CoverageTermPatternID_stg = g.ID_stg 

   left join DB_T_PROD_STAG.pc_etlcovtermoption i on i.CoverageTermPatternID_stg = g.ID_stg 

   left join DB_T_PROD_STAG.pc_etlcovtermoption k on k.CoverageTermPatternID_stg = g.ID_stg

   ) pc ON pc.PatternID_g=SRC.CovTermType1

   AND pc.Value_h=SRC.PackTerm1

   AND pc.Value_i=SRC.IncidentLimit

   AND pc.Value_k=SRC.Deductible

/* ----------------lookup pc_etlcovtermpattern2 */
LEFT OUTER JOIN DB_T_PROD_STAG.pc_etlcovtermpattern lkp_pc on SRC.CovTermType1=lkp_pc.PatternID_stg

)  cc_clm_expsr_feat_term_x

group by PolicyNumber, claimnumber, Exposure_ID

/*order by PolicyNumber, Exposure_ID*/)SRC

LEFT JOIN 

(SELECT CLM_EXPSR.CLM_EXPSR_ID AS CLM_EXPSR_ID, CLM_EXPSR.NK_SRC_KEY AS NK_SRC_KEY FROM DB_T_PROD_CORE.CLM_EXPSR

QUALIFY ROW_NUMBER() OVER(PARTITION BY CLM_EXPSR.NK_SRC_KEY  ORDER BY CLM_EXPSR.EDW_END_DTTM DESC) = 1)clm_EXPS

ON CLM_EXPS.NK_SRC_KEY=Exposure_ID

left join DB_T_PROD_CORE.FEAT INC_TERM ON INC_TERM.NK_SRC_KEY =INCIDENT_TERM AND CAST(INC_TERM.EDW_END_DTTM AS DATE)=''9999-12-31''

left join DB_T_PROD_CORE.FEAT EXP_TERM ON EXP_TERM.NK_SRC_KEY =Exposure_Term AND CAST(EXP_TERM.EDW_END_DTTM AS DATE)=''9999-12-31''

left join DB_T_PROD_CORE.FEAT DED_TERM ON DED_TERM.NK_SRC_KEY =Deductible_Term AND CAST(DED_TERM.EDW_END_DTTM AS DATE)=''9999-12-31''

LEFT JOIN (SELECT CLM_EXPSR_FEAT_TERM.CLM_EXPSR_FEAT_TERM_ID as CLM_EXPSR_FEAT_TERM_ID,

CLM_EXPSR_FEAT_TERM.INCDT_DIR_AMT as INCDT_DIR_AMT, CLM_EXPSR_FEAT_TERM.EXPSR_DIR_AMT as EXPSR_DIR_AMT, CLM_EXPSR_FEAT_TERM.DEDCTB_DIR_AMT as DEDCTB_DIR_AMT, 

CLM_EXPSR_FEAT_TERM.EDW_STRT_DTTM as EDW_STRT_DTTM, CLM_EXPSR_FEAT_TERM.CLM_EXPSR_ID as CLM_EXPSR_ID, CLM_EXPSR_FEAT_TERM.INCDT_TERM_FEAT_ID as INCDT_TERM_FEAT_ID, 

CLM_EXPSR_FEAT_TERM.EXPSR_TERM_FEAT_ID as EXPSR_TERM_FEAT_ID, CLM_EXPSR_FEAT_TERM.DEDCTB_TERM_FEAT_ID as DEDCTB_TERM_FEAT_ID FROM DB_T_PROD_CORE.CLM_EXPSR_FEAT_TERM CLM_EXPSR_FEAT_TERM) TGT ON

COALESCE(TGT.INCDT_TERM_FEAT_ID,0)= COALESCE(INC_TERM.FEAT_ID,0) AND COALESCE(TGT.EXPSR_TERM_FEAT_ID,0)=COALESCE(EXP_TERM.FEAT_ID,0)

AND COALESCE(DEDCTB_TERM_FEAT_ID,0)=COALESCE(DED_TERM.FEAT_ID,0) AND COALESCE(TGT.CLM_EXPSR_ID,0)=COALESCE(clm_EXPS.CLM_EXPSR_ID,0)
) SRC
)
);


-- Component exp_data_transformation1, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation1 AS
(
SELECT
SQ_cc_clm_expsr_feat_term_x.PolicyNumber as PolicyNumber,
SQ_cc_clm_expsr_feat_term_x.claimnumber as claimnumber,
SQ_cc_clm_expsr_feat_term_x.Exposure_ID as Exposure_ID,
SQ_cc_clm_expsr_feat_term_x.Incident_Term as Incident_Term,
SQ_cc_clm_expsr_feat_term_x.Exposure_Term as Exposure_Term,
SQ_cc_clm_expsr_feat_term_x.Deductible_Term as Deductible_Term,
SQ_cc_clm_expsr_feat_term_x.Incident_Direct_Amt as Incident_Direct_Amt,
SQ_cc_clm_expsr_feat_term_x.Exposure_Direct_Amt as Exposure_Direct_Amt,
SQ_cc_clm_expsr_feat_term_x.Deductible_Direct_Amt as Deductible_Direct_Amt,
SQ_cc_clm_expsr_feat_term_x.SRC_SYS_CD as SRC_SYS_CD,
SQ_cc_clm_expsr_feat_term_x.LKP_CLM_EXPSR_FEAT_TERM_ID as lkp_CLM_EXPSR_FEAT_TERM_ID,
SQ_cc_clm_expsr_feat_term_x.LKP_EDW_END_DTTM as lkp_EDW_STRT_DTTM,
SQ_cc_clm_expsr_feat_term_x.LKP_INCIDENT_FEAT_ID as lkp_incident_term_feat_id,
SQ_cc_clm_expsr_feat_term_x.LKP_EXPOSURE_FEAT_ID as lkp_exposure_term_feat_id,
SQ_cc_clm_expsr_feat_term_x.LKP_DEDUCTIBLE_FEAT_ID as lkp_deductible_term_feat_id,
SQ_cc_clm_expsr_feat_term_x.FEAT_ID as Feat_id,
CURRENT_TIMESTAMP as out_EDW_STRT_DTTM,
MD5 ( ltrim ( rtrim ( SQ_cc_clm_expsr_feat_term_x.LKP_INCIDENT_AMT ) ) || ltrim ( rtrim ( SQ_cc_clm_expsr_feat_term_x.LKP_EXPOSURE_AMT ) ) || ltrim ( rtrim ( SQ_cc_clm_expsr_feat_term_x.LKP_DEDUCTIBLE_AMT ) ) ) as var_orig_chksm,
MD5 ( ltrim ( rtrim ( SQ_cc_clm_expsr_feat_term_x.Incident_Direct_Amt ) ) || ltrim ( rtrim ( SQ_cc_clm_expsr_feat_term_x.Exposure_Direct_Amt ) ) || ltrim ( rtrim ( SQ_cc_clm_expsr_feat_term_x.Deductible_Direct_Amt ) ) ) as var_calc_chksm,
CASE WHEN var_orig_chksm IS NULL and SQ_cc_clm_expsr_feat_term_x.LKP_CLM_EXPSR_FEAT_TERM_ID IS NULL THEN ''I'' ELSE CASE WHEN var_orig_chksm != var_calc_chksm THEN ''U'' ELSE ''R'' END END as out_ins_upd_flag,
SQ_cc_clm_expsr_feat_term_x.source_record_id
FROM
SQ_cc_clm_expsr_feat_term_x
);


-- Component rtr_clm_expsr_feat_term_Insert, Type ROUTER Output Group Insert
CREATE OR REPLACE TEMPORARY TABLE rtr_clm_expsr_feat_term_Insert AS (
SELECT
exp_data_transformation1.PolicyNumber as PolicyNumber,
exp_data_transformation1.claimnumber as claimnumber,
exp_data_transformation1.Exposure_ID as Exposure_ID,
exp_data_transformation1.Incident_Term as Incident_Term,
exp_data_transformation1.Exposure_Term as Exposure_Term,
exp_data_transformation1.Deductible_Term as Deductible_Term,
exp_data_transformation1.Incident_Direct_Amt as Incident_Direct_Amt,
exp_data_transformation1.Exposure_Direct_Amt as Exposure_Direct_Amt,
exp_data_transformation1.Deductible_Direct_Amt as Deductible_Direct_Amt,
exp_data_transformation1.SRC_SYS_CD as SRC_SYS_CD,
exp_data_transformation1.Feat_id as Feat_id,
exp_data_transformation1.out_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation1.lkp_CLM_EXPSR_FEAT_TERM_ID as lkp_CLM_EXPSR_FEAT_TERM_ID,
exp_data_transformation1.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation1.lkp_incident_term_feat_id as lkp_incident_term_feat_id,
exp_data_transformation1.lkp_exposure_term_feat_id as lkp_exposure_term_feat_id,
exp_data_transformation1.lkp_deductible_term_feat_id as lkp_deductible_term_feat_id,
exp_data_transformation1.out_ins_upd_flag as out_ins_upd_flag,
exp_data_transformation1.source_record_id
FROM
exp_data_transformation1
WHERE exp_data_transformation1.out_ins_upd_flag = ''I'' AND exp_data_transformation1.Exposure_ID IS NOT NULL
);


-- Component rtr_clm_expsr_feat_term_Update, Type ROUTER Output Group Update
CREATE OR REPLACE TEMPORARY TABLE rtr_clm_expsr_feat_term_Update AS (
SELECT
exp_data_transformation1.PolicyNumber as PolicyNumber,
exp_data_transformation1.claimnumber as claimnumber,
exp_data_transformation1.Exposure_ID as Exposure_ID,
exp_data_transformation1.Incident_Term as Incident_Term,
exp_data_transformation1.Exposure_Term as Exposure_Term,
exp_data_transformation1.Deductible_Term as Deductible_Term,
exp_data_transformation1.Incident_Direct_Amt as Incident_Direct_Amt,
exp_data_transformation1.Exposure_Direct_Amt as Exposure_Direct_Amt,
exp_data_transformation1.Deductible_Direct_Amt as Deductible_Direct_Amt,
exp_data_transformation1.SRC_SYS_CD as SRC_SYS_CD,
exp_data_transformation1.Feat_id as Feat_id,
exp_data_transformation1.out_EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation1.lkp_CLM_EXPSR_FEAT_TERM_ID as lkp_CLM_EXPSR_FEAT_TERM_ID,
exp_data_transformation1.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM,
exp_data_transformation1.lkp_incident_term_feat_id as lkp_incident_term_feat_id,
exp_data_transformation1.lkp_exposure_term_feat_id as lkp_exposure_term_feat_id,
exp_data_transformation1.lkp_deductible_term_feat_id as lkp_deductible_term_feat_id,
exp_data_transformation1.out_ins_upd_flag as out_ins_upd_flag,
exp_data_transformation1.source_record_id
FROM
exp_data_transformation1
WHERE exp_data_transformation1.out_ins_upd_flag = ''U'' AND exp_data_transformation1.Exposure_ID IS NOT NULL -- exp_data_transformation1.lkp_CLM_EXPSR_FEAT_TERM_ID IS NOT NULL
);


-- Component upd_clm_expsr_feat_term_update, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_clm_expsr_feat_term_update AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_clm_expsr_feat_term_Update.PolicyNumber as PolicyNumber3,
rtr_clm_expsr_feat_term_Update.claimnumber as claimnumber3,
rtr_clm_expsr_feat_term_Update.Exposure_ID as Exposure_ID3,
rtr_clm_expsr_feat_term_Update.Incident_Term as Incident_Term3,
rtr_clm_expsr_feat_term_Update.Exposure_Term as Exposure_Term3,
rtr_clm_expsr_feat_term_Update.Deductible_Term as Deductible_Term3,
rtr_clm_expsr_feat_term_Update.Incident_Direct_Amt as Incident_Direct_Amt3,
rtr_clm_expsr_feat_term_Update.Exposure_Direct_Amt as Exposure_Direct_Amt3,
rtr_clm_expsr_feat_term_Update.Deductible_Direct_Amt as Deductible_Direct_Amt3,
rtr_clm_expsr_feat_term_Update.SRC_SYS_CD as SRC_SYS_CD3,
rtr_clm_expsr_feat_term_Update.Feat_id as Feat_id3,
rtr_clm_expsr_feat_term_Update.EDW_STRT_DTTM as EDW_STRT_DTTM3,
rtr_clm_expsr_feat_term_Update.lkp_EDW_STRT_DTTM as lkp_EDW_STRT_DTTM3,
rtr_clm_expsr_feat_term_Update.lkp_CLM_EXPSR_FEAT_TERM_ID as lkp_CLM_EXPSR_FEAT_TERM_ID3,
rtr_clm_expsr_feat_term_Update.lkp_incident_term_feat_id as lkp_incident_term_feat_id3,
rtr_clm_expsr_feat_term_Update.lkp_exposure_term_feat_id as lkp_exposure_term_feat_id3,
rtr_clm_expsr_feat_term_Update.lkp_deductible_term_feat_id as lkp_deductible_term_feat_id3,
rtr_clm_expsr_feat_term_Update.source_record_id,
1 as UPDATE_STRATEGY_ACTION
FROM
rtr_clm_expsr_feat_term_Update
);


-- Component exp_clm_expsr_feat_term_update, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_clm_expsr_feat_term_update AS
(
SELECT
upd_clm_expsr_feat_term_update.Exposure_ID3 as Exposure_ID1,
upd_clm_expsr_feat_term_update.Incident_Direct_Amt3 as Incident_Direct_Amt1,
upd_clm_expsr_feat_term_update.Exposure_Direct_Amt3 as Exposure_Direct_Amt1,
upd_clm_expsr_feat_term_update.Deductible_Direct_Amt3 as Deductible_Direct_Amt1,
upd_clm_expsr_feat_term_update.lkp_EDW_STRT_DTTM3 as lkp_EDW_STRT_DTTM3,
upd_clm_expsr_feat_term_update.lkp_incident_term_feat_id3 as lkp_incident_term_feat_id3,
upd_clm_expsr_feat_term_update.lkp_exposure_term_feat_id3 as lkp_exposure_term_feat_id3,
upd_clm_expsr_feat_term_update.lkp_deductible_term_feat_id3 as lkp_deductible_term_feat_id3,
dateadd (second, -1,  CURRENT_TIMESTAMP ) as out_EDW_END_DTTM,
upd_clm_expsr_feat_term_update.source_record_id
FROM
upd_clm_expsr_feat_term_update
);


-- Component upd_clm_expsr_feat_term_insert, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE upd_clm_expsr_feat_term_insert AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_clm_expsr_feat_term_Insert.PolicyNumber as PolicyNumber1,
rtr_clm_expsr_feat_term_Insert.claimnumber as claimnumber1,
rtr_clm_expsr_feat_term_Insert.Exposure_ID as Exposure_ID1,
rtr_clm_expsr_feat_term_Insert.Incident_Term as Incident_Term1,
rtr_clm_expsr_feat_term_Insert.Exposure_Term as Exposure_Term1,
rtr_clm_expsr_feat_term_Insert.Deductible_Term as Deductible_Term1,
rtr_clm_expsr_feat_term_Insert.Incident_Direct_Amt as Incident_Direct_Amt1,
rtr_clm_expsr_feat_term_Insert.Exposure_Direct_Amt as Exposure_Direct_Amt1,
rtr_clm_expsr_feat_term_Insert.Deductible_Direct_Amt as Deductible_Direct_Amt1,
rtr_clm_expsr_feat_term_Insert.SRC_SYS_CD as SRC_SYS_CD1,
rtr_clm_expsr_feat_term_Insert.Feat_id as Feat_id1,
rtr_clm_expsr_feat_term_Insert.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_clm_expsr_feat_term_Insert.lkp_incident_term_feat_id as lkp_incident_term_feat_id1,
rtr_clm_expsr_feat_term_Insert.lkp_exposure_term_feat_id as lkp_exposure_term_feat_id1,
rtr_clm_expsr_feat_term_Insert.lkp_deductible_term_feat_id as lkp_deductible_term_feat_id1,
rtr_clm_expsr_feat_term_Insert.source_record_id
FROM
rtr_clm_expsr_feat_term_Insert
);


-- Component exp_clm_expsr_feat_term_insert, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_clm_expsr_feat_term_insert AS
(
SELECT
upd_clm_expsr_feat_term_insert.Exposure_ID1 as Exposure_ID1,
upd_clm_expsr_feat_term_insert.Incident_Direct_Amt1 as Incident_Direct_Amt1,
upd_clm_expsr_feat_term_insert.Exposure_Direct_Amt1 as Exposure_Direct_Amt1,
upd_clm_expsr_feat_term_insert.Deductible_Direct_Amt1 as Deductible_Direct_Amt1,
upd_clm_expsr_feat_term_insert.SRC_SYS_CD1 as SRC_SYS_CD1,
upd_clm_expsr_feat_term_insert.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
upd_clm_expsr_feat_term_insert.lkp_incident_term_feat_id1 as lkp_incident_term_feat_id1,
upd_clm_expsr_feat_term_insert.lkp_exposure_term_feat_id1 as lkp_exposure_term_feat_id1,
upd_clm_expsr_feat_term_insert.lkp_deductible_term_feat_id1 as lkp_deductible_term_feat_id1,
:PRCS_ID as PRCS_ID,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as out_EDW_END_DTTM,
upd_clm_expsr_feat_term_insert.source_record_id
FROM
upd_clm_expsr_feat_term_insert
);


-- Component tgt_CLM_EXPSR_FEAT_TERM_update, Type TARGET 
MERGE INTO DB_T_PROD_CORE.CLM_EXPSR_FEAT_TERM
USING exp_clm_expsr_feat_term_update ON (CLM_EXPSR_FEAT_TERM.EDW_STRT_DTTM = exp_clm_expsr_feat_term_update.lkp_EDW_STRT_DTTM3)
WHEN MATCHED THEN UPDATE
SET
INCDT_TERM_FEAT_ID = exp_clm_expsr_feat_term_update.lkp_incident_term_feat_id3,
EXPSR_TERM_FEAT_ID = exp_clm_expsr_feat_term_update.lkp_exposure_term_feat_id3,
DEDCTB_TERM_FEAT_ID = exp_clm_expsr_feat_term_update.lkp_deductible_term_feat_id3,
CLM_EXPSR_ID = exp_clm_expsr_feat_term_update.Exposure_ID1,
INCDT_DIR_AMT = exp_clm_expsr_feat_term_update.Incident_Direct_Amt1,
EXPSR_DIR_AMT = exp_clm_expsr_feat_term_update.Exposure_Direct_Amt1,
DEDCTB_DIR_AMT = exp_clm_expsr_feat_term_update.Deductible_Direct_Amt1,
EDW_STRT_DTTM = exp_clm_expsr_feat_term_update.lkp_EDW_STRT_DTTM3,
EDW_END_DTTM = exp_clm_expsr_feat_term_update.out_EDW_END_DTTM;


-- Component tgt_CLM_EXPSR_FEAT_TERM_insert, Type TARGET 
INSERT INTO DB_T_PROD_CORE.CLM_EXPSR_FEAT_TERM
(
CLM_EXPSR_FEAT_TERM_ID,
INCDT_TERM_FEAT_ID,
EXPSR_TERM_FEAT_ID,
DEDCTB_TERM_FEAT_ID,
CLM_EXPSR_ID,
INCDT_DIR_AMT,
EXPSR_DIR_AMT,
DEDCTB_DIR_AMT,
SRC_SYS_CD,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM
)
SELECT
row_number() over (order by 1) as CLM_EXPSR_FEAT_TERM_ID,
exp_clm_expsr_feat_term_insert.lkp_incident_term_feat_id1 as INCDT_TERM_FEAT_ID,
exp_clm_expsr_feat_term_insert.lkp_exposure_term_feat_id1 as EXPSR_TERM_FEAT_ID,
exp_clm_expsr_feat_term_insert.lkp_deductible_term_feat_id1 as DEDCTB_TERM_FEAT_ID,
exp_clm_expsr_feat_term_insert.Exposure_ID1 as CLM_EXPSR_ID,
exp_clm_expsr_feat_term_insert.Incident_Direct_Amt1 as INCDT_DIR_AMT,
exp_clm_expsr_feat_term_insert.Exposure_Direct_Amt1 as EXPSR_DIR_AMT,
exp_clm_expsr_feat_term_insert.Deductible_Direct_Amt1 as DEDCTB_DIR_AMT,
exp_clm_expsr_feat_term_insert.SRC_SYS_CD1 as SRC_SYS_CD,
exp_clm_expsr_feat_term_insert.PRCS_ID as PRCS_ID,
exp_clm_expsr_feat_term_insert.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_clm_expsr_feat_term_insert.out_EDW_END_DTTM as EDW_END_DTTM
FROM
exp_clm_expsr_feat_term_insert;


END; ';