-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_INSRBL_INT_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE 

start_dttm TIMESTAMP;
end_dttm TIMESTAMP;
PRCS_ID INTEGER;
P_DEFAULT_STR_CD char;
var_ContactroleTypecode char;
BEGIN 
start_dttm := CURRENT_TIMESTAMP();
end_dttm := CURRENT_TIMESTAMP();
PRCS_ID := 1;  

-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_CLASFCN'' 

            -- AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM in ( ''derived'' ,''pcx_holineschcovitemcov_alfa.ChoiceTerm1'', ''cctl_contentlineitemschedule'')

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'', ''GW'') 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_ASSET_SBTYPE'' 

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived'' 

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS=''DS'' 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_PRTY_CLM_ROLE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_PRTY_CLM_ROLE AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,upper(TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL) as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''PRTY_CLM_ROLE_TYPE'' 

and TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''cctl_contactrole.typecode'' 

        AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS in (''DS'', ''GW'') 

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD AS
(
SELECT 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_VAL as TGT_IDNTFTN_VAL

	,TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_VAL as SRC_IDNTFTN_VAL 

FROM 

	DB_T_PROD_CORE.TERADATA_ETL_REF_XLAT

WHERE 

	TERADATA_ETL_REF_XLAT.TGT_IDNTFTN_NM= ''SRC_SYS''

             AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_NM= ''derived''

		AND TERADATA_ETL_REF_XLAT.SRC_IDNTFTN_SYS= ''DS''

		AND TERADATA_ETL_REF_XLAT.EXPN_DT=''9999-12-31''
);


-- Component sq_cc_contact, Type SOURCE 
CREATE OR REPLACE TEMPORARY TABLE sq_cc_contact AS
(
SELECT /* adding column aliases to ensure proper downstream column references */
$1 as ID,
$2 as PublicID,
$3 as insrbl_int_key,
$4 as SOURCE,
$5 as TYPECODE,
$6 as PRTY_ASSET_SB_TYPE,
$7 as INSRBL_INT_TYPE_CD,
$8 as Classification_Cd,
$9 as SRC_SYS,
$10 as CreateTime,
$11 as UpdateTime,
$12 as Retired,
$13 as source_record_id
FROM (
SELECT SRC.*, row_number() over (order by 1) AS source_record_id FROM (
SELECT

prty_insrbl_int_x.ID_stg,

prty_insrbl_int_x.publicid_stg,

prty_insrbl_int_x.insrbl_int_key_stg,

prty_insrbl_int_x.SOURCE_stg,

prty_insrbl_int_x.TYPECODE_stg,

prty_insrbl_int_x.PRTY_ASSET_SB_TYPE_stg,

prty_insrbl_int_x.INSRBL_INT_TYPE_CD_stg,

prty_insrbl_int_x.Classification_Cd_stg,

prty_insrbl_int_x.SRC_SYS_stg,

prty_insrbl_int_x.Createtime_stg,

prty_insrbl_int_x.UpdateTime_stg,

prty_insrbl_int_x.Retired_stg

FROM

(

    SELECT   ID_stg,

    PublicID_stg,

    insrbl_int_key_stg,

    SOURCE_stg,

    TYPECODE_stg,

    PRTY_ASSET_SB_TYPE_stg,

    INSRBL_INT_TYPE_CD_stg,

    Classification_Cd_stg,

    SRC_SYS_stg,

    CreateTime_stg,

    UpdateTime_stg,

     retired_stg

    FROM    (

            SELECT   DISTINCT

            cc_incident.ID_stg,

            cc_contact.PublicID_stg AS PublicID_stg,

           CASE 

             WHEN cc_vehicle.PolicySystemId_stg IS NOT NULL THEN Substr(cc_vehicle.policysystemid_stg,

                POSITION('':'',cc_vehicle.policysystemid_stg)+1)

                        WHEN (cc_vehicle.PolicySystemId_stg IS NULL 

                AND cc_vehicle.Vin_stg IS NOT NULL) THEN ''VIN:''||cc_vehicle.vin_stg

                        WHEN (cc_vehicle.PolicySystemId_stg IS NULL 

                AND cc_vehicle.Vin_stg IS NULL

                AND cc_vehicle.LicensePlate_stg IS NOT NULL) THEN  ''LP:''||cc_vehicle.licenseplate_stg

                        WHEN (cc_vehicle.ID_stg IS NOT NULL 

                AND cc_vehicle.PolicySystemId_stg IS NULL

                AND cc_vehicle.Vin_stg IS NULL

                AND cc_vehicle.LicensePlate_stg IS NULL) THEN cc_vehicle.PublicID_stg

                        WHEN cctl_incident.NAME_stg = ''InjuryIncident'' THEN cc_contact.PublicID_stg

                        ELSE cc_incident.PublicID_stg

                    end AS  insrbl_int_key_stg,

            ''ClaimCenter'' AS SOURCE_stg,

            cctl_contactrole.TYPECODE_stg,

             CASE

                        WHEN cctl_incident.NAME_stg  = ''InjuryIncident'' THEN ''''

             WHEN cctl_incident.NAME_stg  = ''VehicleIncident'' THEN ''PRTY_ASSET_SBTYPE4''

             WHEN cctl_incident.NAME_stg IN ( ''FixedPropertyIncident'',

                    ''DwellingIncident'') THEN ''PRTY_ASSET_SBTYPE5''

                        WHEN cctl_incident.NAME_stg  IN (''OtherStructureIncident'',

                ''PropertyContentsIncident'') THEN ''PRTY_ASSET_SBTYPE11''

                    end AS PRTY_ASSET_SB_TYPE_stg,

             CASE

                        WHEN cctl_incident.NAME_stg  = ''InjuryIncident'' THEN ''PERSON''

             WHEN cctl_incident.NAME_stg  IN( ''VehicleIncident'',

                    ''FixedPropertyIncident'',''OtherStructureIncident'',''DwellingIncident'',

                    ''PropertyContentsIncident'' ) THEN ''ASSET''

                    end AS INSRBL_INT_TYPE_CD_stg,

             CASE

                        WHEN cctl_incident.NAME_stg  = ''InjuryIncident'' THEN ''''

             WHEN cctl_incident.NAME_stg  = ''VehicleIncident'' THEN ''PRTY_ASSET_CLASFCN3''

             WHEN cctl_incident.NAME_stg IN (''FixedPropertyIncident'',

                    ''DwellingIncident'') THEN ''PRTY_ASSET_CLASFCN1''

                        WHEN cctl_incident.NAME_stg =''OtherStructureIncident'' THEN ''PRTY_ASSET_CLASFCN7''

                        WHEN cctl_incident.NAME_stg=''PropertyContentsIncident'' THEN cctl_contentlineitemschedule.TYPECODE_stg

                    end AS Classification_Cd_stg,

            ''SRC_SYS6'' AS SRC_SYS_stg,

            cc_incident.CreateTime_stg,

            cc_incident.UpdateTime_stg,

                    CASE

                        WHEN cc_incident.retired_stg=0

                AND cc_claim.retired_stg=0

                AND cc_claimcontact.retired_stg=0 THEN 0

                        ELSE 1

                    end AS retired_stg,

             Rank() Over (

            PARTITION BY cc_incident.ID_stg,cc_contact.PublicID_stg 

            ORDER BY cc_incident.UpdateTime_stg  DESC) rk_stg

            FROM

            DB_T_PROD_STAG.cc_incident

            INNER JOIN (

                    SELECT   cc_claim.*

                    FROM    DB_T_PROD_STAG.cc_claim

                    INNER JOIN DB_T_PROD_STAG.cctl_claimstate

                        ON cc_claim.State_stg = cctl_claimstate.id_stg

                    WHERE     cctl_claimstate.NAME_stg <> ''Draft'') cc_claim

                ON cc_claim.id_stg =cc_incident.claimid_stg

            INNER JOIN DB_T_PROD_STAG.cc_claimcontactrole

                ON cc_incident.id_stg=cc_claimcontactrole.incidentid_stg

            INNER JOIN DB_T_PROD_STAG.cctl_incident

                ON cctl_incident.id_stg=cc_incident.Subtype_stg

            LEFT JOIN DB_T_PROD_STAG.cc_vehicle

                ON cc_incident.vehicleid_stg=cc_vehicle.id_stg

            LEFT OUTER JOIN DB_T_PROD_STAG.cc_claimcontact 

                ON cc_claimcontactrole.claimcontactid_stg=cc_claimcontact.id_stg

            LEFT OUTER JOIN DB_T_PROD_STAG.cc_contact

                ON cc_claimcontact.contactid_stg=cc_contact.id_stg

            INNER JOIN DB_T_PROD_STAG.cctl_contact 

                ON cctl_contact.id_stg=cc_contact.Subtype_stg

            LEFT OUTER JOIN DB_T_PROD_STAG.cctl_contactrole

                ON cc_claimcontactrole.ROLE_stg = cctl_contactrole.id_stg

            LEFT OUTER JOIN DB_T_PROD_STAG.cc_assessmentcontentitem

                ON cc_incident.id_stg = cc_assessmentcontentitem.IncidentID_stg

            LEFT JOIN DB_T_PROD_STAG.cctl_contentlineitemschedule

                ON cc_assessmentcontentitem.ContentSchedule_stg = cctl_contentlineitemschedule.ID_stg

   

   WHERE ( cc_incident.UpdateTime_stg > Cast(:START_DTTM AS TIMESTAMP)

        AND  cc_incident.UpdateTime_stg <= Cast(:END_DTTM AS TIMESTAMP))

    ) AS A

    WHERE  rk_stg=1



)  prty_insrbl_int_x  

QUALIFY Row_Number() Over(

PARTITION BY  insrbl_int_key_stg, publicid_stg,Typecode_stg 

ORDER BY updatetime_stg DESC,createtime_stg DESC)=1
) SRC
)
);


-- Component exp_pass_through, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_through AS
(
SELECT
sq_cc_contact.PublicID as PublicID,
sq_cc_contact.SOURCE as SOURCE,
UPPER ( sq_cc_contact.TYPECODE ) as v_TYPECODE,
CASE WHEN LKP_1.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PRTY_CLM_ROLE */ IS NULL THEN ''UNK'' ELSE LKP_2.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_PRTY_CLM_ROLE */ END as o_TYPECODE,
sq_cc_contact.CreateTime as CreateTime,
sq_cc_contact.UpdateTime as UpdateTime,
sq_cc_contact.Retired as Retired,
sq_cc_contact.insrbl_int_key as insrbl_int_key,
CASE WHEN LKP_3.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ IS NULL THEN ''UNK'' ELSE LKP_4.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE */ END as o_PRTY_ASSET_SB_TYPE,
sq_cc_contact.INSRBL_INT_TYPE_CD as INSRBL_INT_TYPE_CD,
CASE WHEN LKP_5.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ IS NULL THEN ''UNK'' ELSE LKP_6.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN */ END as o_Classification_Cd,
CASE WHEN LKP_7.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD */ IS NULL THEN sq_cc_contact.SRC_SYS ELSE LKP_8.TGT_IDNTFTN_VAL /* replaced lookup LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD */ END as o_SRC_SYS,
sq_cc_contact.source_record_id,
row_number() over (partition by sq_cc_contact.source_record_id order by sq_cc_contact.source_record_id) as RNK
FROM
sq_cc_contact
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_PRTY_CLM_ROLE LKP_1 ON LKP_1.SRC_IDNTFTN_VAL = v_TYPECODE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_PRTY_CLM_ROLE LKP_2 ON LKP_2.SRC_IDNTFTN_VAL = v_TYPECODE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_3 ON LKP_3.SRC_IDNTFTN_VAL = sq_cc_contact.PRTY_ASSET_SB_TYPE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_SBTYPE LKP_4 ON LKP_4.SRC_IDNTFTN_VAL = sq_cc_contact.PRTY_ASSET_SB_TYPE
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_5 ON LKP_5.SRC_IDNTFTN_VAL = sq_cc_contact.Classification_Cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_ASSET_CLASFCN LKP_6 ON LKP_6.SRC_IDNTFTN_VAL = sq_cc_contact.Classification_Cd
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD LKP_7 ON LKP_7.SRC_IDNTFTN_VAL = sq_cc_contact.SRC_SYS
LEFT JOIN LKP_TERADATA_ETL_REF_XLAT_SYS_SRC_CD LKP_8 ON LKP_8.SRC_IDNTFTN_VAL = sq_cc_contact.SRC_SYS
QUALIFY RNK = 1
);


-- Component LKP_PRTY_ASSET_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_ASSET_ID AS
(
SELECT
LKP.PRTY_ASSET_ID,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_HOST_ID_VAL asc,LKP.PRTY_ASSET_SBTYPE_CD asc,LKP.PRTY_ASSET_CLASFCN_CD asc,LKP.ASSET_INSRNC_HIST_TYPE_CD asc,LKP.ASSET_DESC asc,LKP.PRTY_ASSET_NAME asc,LKP.PRTY_ASSET_STRT_DTTM asc,LKP.PRTY_ASSET_END_DTTM asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.SRC_SYS_CD asc) RNK
FROM
exp_pass_through
LEFT JOIN (
SELECT PRTY_ASSET.PRTY_ASSET_ID as PRTY_ASSET_ID, PRTY_ASSET.ASSET_INSRNC_HIST_TYPE_CD as ASSET_INSRNC_HIST_TYPE_CD, PRTY_ASSET.ASSET_DESC as ASSET_DESC, PRTY_ASSET.PRTY_ASSET_NAME as PRTY_ASSET_NAME, PRTY_ASSET.PRTY_ASSET_STRT_DTTM as PRTY_ASSET_STRT_DTTM, PRTY_ASSET.PRTY_ASSET_END_DTTM as PRTY_ASSET_END_DTTM, PRTY_ASSET.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_ASSET.EDW_END_DTTM as EDW_END_DTTM, PRTY_ASSET.SRC_SYS_CD as SRC_SYS_CD, PRTY_ASSET.ASSET_HOST_ID_VAL as ASSET_HOST_ID_VAL, PRTY_ASSET.PRTY_ASSET_SBTYPE_CD as PRTY_ASSET_SBTYPE_CD, PRTY_ASSET.PRTY_ASSET_CLASFCN_CD as PRTY_ASSET_CLASFCN_CD 
FROM DB_T_PROD_CORE.PRTY_ASSET 
QUALIFY ROW_NUMBER() OVER(PARTITION BY  ASSET_HOST_ID_VAL,PRTY_ASSET_SBTYPE_CD,PRTY_ASSET_CLASFCN_CD ORDER BY EDW_END_DTTM DESC) = 1
) LKP ON LKP.ASSET_HOST_ID_VAL = exp_pass_through.insrbl_int_key AND LKP.PRTY_ASSET_SBTYPE_CD = exp_pass_through.o_PRTY_ASSET_SB_TYPE AND LKP.PRTY_ASSET_CLASFCN_CD = exp_pass_through.o_Classification_Cd
QUALIFY RNK = 1
);


-- Component LKP_INDIV_CLM_CTR_INSRBL_INT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INDIV_CLM_CTR_INSRBL_INT AS
(
SELECT
LKP.INDIV_PRTY_ID,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.INDIV_PRTY_ID desc,LKP.NK_PUBLC_ID desc) RNK
FROM
exp_pass_through
LEFT JOIN (
SELECT 
	INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID, 
	INDIV.NK_PUBLC_ID as NK_PUBLC_ID 
FROM 
	DB_T_PROD_CORE.INDIV
WHERE
	INDIV.NK_PUBLC_ID IS NOT NULL
) LKP ON LKP.NK_PUBLC_ID = exp_pass_through.insrbl_int_key
QUALIFY RNK = 1
);


-- Component LKP_INSRBL_INT_PRTY_ASSET_ID, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRBL_INT_PRTY_ASSET_ID AS
(
SELECT
LKP.INSRBL_INT_ID,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.INSRBL_INT_ID desc,LKP.INSRBL_INT_CTGY_CD desc,LKP.CTSTRPH_EXPSR_IND desc,LKP.SRC_SYS_CD desc,LKP.PRTY_ASSET_ID desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK1
FROM
exp_pass_through
INNER JOIN LKP_PRTY_ASSET_ID ON exp_pass_through.source_record_id = LKP_PRTY_ASSET_ID.source_record_id
LEFT JOIN (
SELECT INSRBL_INT.INSRBL_INT_ID as INSRBL_INT_ID, INSRBL_INT.CTSTRPH_EXPSR_IND as CTSTRPH_EXPSR_IND, INSRBL_INT.EDW_STRT_DTTM as EDW_STRT_DTTM, INSRBL_INT.EDW_END_DTTM as EDW_END_DTTM, INSRBL_INT.INSRBL_INT_CTGY_CD as INSRBL_INT_CTGY_CD, INSRBL_INT.SRC_SYS_CD as SRC_SYS_CD, INSRBL_INT.PRTY_ASSET_ID as PRTY_ASSET_ID FROM DB_T_PROD_CORE.INSRBL_INT
 where  INSRBL_INT.INSRBL_INT_CTGY_CD = ''ASSET''
AND INSRBL_INT.PRTY_ASSET_ID IS NOT NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY INSRBL_INT_CTGY_CD,PRTY_ASSET_ID,SRC_SYS_CD  ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.INSRBL_INT_CTGY_CD = exp_pass_through.INSRBL_INT_TYPE_CD AND LKP.SRC_SYS_CD = exp_pass_through.o_SRC_SYS AND LKP.PRTY_ASSET_ID = LKP_PRTY_ASSET_ID.PRTY_ASSET_ID
QUALIFY RNK1 = 1
);


-- Component LKP_INDIV_CLM_CTR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INDIV_CLM_CTR AS
(
SELECT
LKP.INDIV_PRTY_ID,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.INDIV_PRTY_ID desc,LKP.NK_PUBLC_ID desc) RNK
FROM
exp_pass_through
LEFT JOIN (
SELECT 
	INDIV.INDIV_PRTY_ID as INDIV_PRTY_ID, 
	INDIV.NK_PUBLC_ID as NK_PUBLC_ID 
FROM 
	DB_T_PROD_CORE.INDIV
WHERE
	INDIV.NK_PUBLC_ID IS NOT NULL
) LKP ON LKP.NK_PUBLC_ID = exp_pass_through.PublicID
QUALIFY RNK = 1
);


-- Component exp_data_transformation_prty_id, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation_prty_id AS
(
SELECT
LKP_INDIV_CLM_CTR.INDIV_PRTY_ID as INDIV_PRTY_ID_prty_id,
LKP_INDIV_CLM_CTR_INSRBL_INT.INDIV_PRTY_ID as INDIV_PRTY_ID_prty_id_int,
exp_pass_through.SOURCE as SOURCE,
exp_pass_through.o_TYPECODE as o_TYPECODE,
exp_pass_through.CreateTime as CreateTime,
exp_pass_through.UpdateTime as UpdateTime,
exp_pass_through.Retired as Retired,
exp_pass_through.source_record_id
FROM
exp_pass_through
INNER JOIN LKP_INDIV_CLM_CTR_INSRBL_INT ON exp_pass_through.source_record_id = LKP_INDIV_CLM_CTR_INSRBL_INT.source_record_id
INNER JOIN LKP_INDIV_CLM_CTR ON LKP_INDIV_CLM_CTR_INSRBL_INT.source_record_id = LKP_INDIV_CLM_CTR.source_record_id
);


-- Component LKP_INSRBL_INT_PRTYID_INSRBL_INT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_INSRBL_INT_PRTYID_INSRBL_INT AS
(
SELECT
LKP.INSRBL_INT_ID,
exp_pass_through.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_pass_through.source_record_id ORDER BY LKP.INSRBL_INT_ID desc,LKP.INSRBL_INT_CTGY_CD desc,LKP.CTSTRPH_EXPSR_IND desc,LKP.SRC_SYS_CD desc,LKP.INJURED_PRTY_ID desc,LKP.EDW_STRT_DTTM desc,LKP.EDW_END_DTTM desc) RNK
FROM
exp_pass_through
INNER JOIN exp_data_transformation_prty_id ON exp_pass_through.source_record_id = exp_data_transformation_prty_id.source_record_id
LEFT JOIN (
SELECT INSRBL_INT.INSRBL_INT_ID as INSRBL_INT_ID, INSRBL_INT.CTSTRPH_EXPSR_IND as CTSTRPH_EXPSR_IND, INSRBL_INT.EDW_STRT_DTTM as EDW_STRT_DTTM, INSRBL_INT.EDW_END_DTTM as EDW_END_DTTM, INSRBL_INT.INSRBL_INT_CTGY_CD as INSRBL_INT_CTGY_CD, INSRBL_INT.SRC_SYS_CD as SRC_SYS_CD, INSRBL_INT.INJURED_PRTY_ID as INJURED_PRTY_ID FROM DB_T_PROD_CORE.INSRBL_INT
 where   INSRBL_INT.INSRBL_INT_CTGY_CD = ''PERSON''
AND  INSRBL_INT.INJURED_PRTY_ID  IS NOT NULL
QUALIFY ROW_NUMBER() OVER(PARTITION BY INSRBL_INT_CTGY_CD,INJURED_PRTY_ID,SRC_SYS_CD  ORDER BY EDW_END_DTTM desc) = 1
) LKP ON LKP.INSRBL_INT_CTGY_CD = exp_pass_through.INSRBL_INT_TYPE_CD AND LKP.SRC_SYS_CD = exp_pass_through.o_SRC_SYS AND LKP.INJURED_PRTY_ID = exp_data_transformation_prty_id.INDIV_PRTY_ID_prty_id_int
QUALIFY RNK = 1
);


-- Component exp_data_transformation_int_id, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation_int_id AS
(
SELECT
CASE WHEN LKP_INSRBL_INT_PRTY_ASSET_ID.INSRBL_INT_ID IS NOT NULL THEN LKP_INSRBL_INT_PRTY_ASSET_ID.INSRBL_INT_ID ELSE CASE WHEN LKP_INSRBL_INT_PRTYID_INSRBL_INT.INSRBL_INT_ID IS NOT NULL THEN LKP_INSRBL_INT_PRTYID_INSRBL_INT.INSRBL_INT_ID ELSE NULL END END as v_INSRBL_INT_ID,
v_INSRBL_INT_ID as O_INSRBL_INT_ID,
exp_data_transformation_prty_id.INDIV_PRTY_ID_prty_id as INDIV_PRTY_ID,
exp_data_transformation_prty_id.SOURCE as SOURCE,
exp_data_transformation_prty_id.o_TYPECODE as o_TYPECODE,
exp_data_transformation_prty_id.Retired as Retired,
CASE WHEN exp_data_transformation_prty_id.UpdateTime IS NULL THEN TO_timestamp ( ''01/01/1900 00:00:00.000000'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ELSE exp_data_transformation_prty_id.UpdateTime END as TRANS_STRT_DTTM,
TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as TRANS_END_DTTM,
CASE WHEN exp_data_transformation_prty_id.CreateTime IS NULL THEN TO_timestamp ( ''01/01/1900 00:00:00.000000'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ELSE exp_data_transformation_prty_id.CreateTime END as PRTY_INSRBL_INT_STRT_DTTM,
LKP_INSRBL_INT_PRTY_ASSET_ID.source_record_id
FROM
LKP_INSRBL_INT_PRTY_ASSET_ID
INNER JOIN exp_data_transformation_prty_id ON LKP_INSRBL_INT_PRTY_ASSET_ID.source_record_id = exp_data_transformation_prty_id.source_record_id
INNER JOIN LKP_INSRBL_INT_PRTYID_INSRBL_INT ON exp_data_transformation_prty_id.source_record_id = LKP_INSRBL_INT_PRTYID_INSRBL_INT.source_record_id
);


-- Component LKP_PRTY_INSRBL_INT, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_PRTY_INSRBL_INT AS
(
SELECT
LKP.PRTY_ID,
LKP.INSRBL_INT_ID,
LKP.PRTY_CLM_ROLE_CD,
LKP.PRTY_INSRBL_INT_STRT_DTTM,
LKP.EDW_END_DTTM,
LKP.TRANS_STRT_DTTM,
LKP.TRANS_END_DTTM,
exp_data_transformation_int_id.source_record_id,
ROW_NUMBER() OVER(PARTITION BY exp_data_transformation_int_id.source_record_id ORDER BY LKP.PRTY_ID asc,LKP.INSRBL_INT_ID asc,LKP.PRTY_CLM_ROLE_CD asc,LKP.PRTY_INSRBL_INT_STRT_DTTM asc,LKP.PRTY_INSRBL_INT_END_DTTM asc,LKP.PRCS_ID asc,LKP.EDW_STRT_DTTM asc,LKP.EDW_END_DTTM asc,LKP.TRANS_STRT_DTTM asc,LKP.TRANS_END_DTTM asc) RNK
FROM
exp_data_transformation_int_id
LEFT JOIN (
SELECT PRTY_INSRBL_INT.PRTY_INSRBL_INT_STRT_DTTM as PRTY_INSRBL_INT_STRT_DTTM, PRTY_INSRBL_INT.PRTY_INSRBL_INT_END_DTTM as PRTY_INSRBL_INT_END_DTTM, PRTY_INSRBL_INT.PRCS_ID as PRCS_ID, PRTY_INSRBL_INT.EDW_STRT_DTTM as EDW_STRT_DTTM, PRTY_INSRBL_INT.EDW_END_DTTM as EDW_END_DTTM, PRTY_INSRBL_INT.TRANS_STRT_DTTM as TRANS_STRT_DTTM, PRTY_INSRBL_INT.TRANS_END_DTTM as TRANS_END_DTTM, PRTY_INSRBL_INT.PRTY_ID as PRTY_ID, PRTY_INSRBL_INT.INSRBL_INT_ID as INSRBL_INT_ID, PRTY_INSRBL_INT.PRTY_CLM_ROLE_CD as PRTY_CLM_ROLE_CD FROM DB_T_PROD_CORE.PRTY_INSRBL_INT
qualify row_number() over(partition by PRTY_INSRBL_INT.PRTY_ID,PRTY_INSRBL_INT.INSRBL_INT_ID ,PRTY_INSRBL_INT.PRTY_CLM_ROLE_CD  order by 
PRTY_INSRBL_INT.EDW_END_DTTM  desc)=1
) LKP ON LKP.PRTY_ID = exp_data_transformation_int_id.INDIV_PRTY_ID AND LKP.INSRBL_INT_ID = exp_data_transformation_int_id.O_INSRBL_INT_ID AND LKP.PRTY_CLM_ROLE_CD = exp_data_transformation_int_id.o_TYPECODE
QUALIFY RNK = 1
);


-- Component exp_data_transformation, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_data_transformation AS
(
SELECT
exp_data_transformation_int_id.O_INSRBL_INT_ID as in_insrbl_int_id,
exp_data_transformation_int_id.INDIV_PRTY_ID as in_INDIV_PRTY_ID,
exp_data_transformation_int_id.TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_data_transformation_int_id.TRANS_END_DTTM as in_TRANS_END_DTTM,
exp_data_transformation_int_id.PRTY_INSRBL_INT_STRT_DTTM as in_PRTY_INSRBL_INT_STRT_DTTM,
exp_data_transformation_int_id.o_TYPECODE as in_PRTY_CLM_ROLE_CD,
exp_data_transformation_int_id.Retired as Retired,
LKP_PRTY_INSRBL_INT.EDW_END_DTTM as lkp_EDW_END_DTTM,
:PRCS_ID as out_PRCS_ID,
CURRENT_TIMESTAMP as EDW_STRT_DTTM,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as EDW_END_DTTM,
CASE WHEN LKP_PRTY_INSRBL_INT.PRTY_ID IS NULL THEN ''I'' ELSE ''R'' END as Insupd_flag,
exp_data_transformation_int_id.source_record_id
FROM
exp_data_transformation_int_id
INNER JOIN LKP_PRTY_INSRBL_INT ON exp_data_transformation_int_id.source_record_id = LKP_PRTY_INSRBL_INT.source_record_id
);


-- Component rtr_prty_insrbl_int_INSUPD, Type ROUTER Output Group INSUPD
CREATE OR REPLACE TEMPORARY TABLE rtr_prty_insrbl_int_INSUPD AS
(SELECT
exp_data_transformation.in_insrbl_int_id as in_insrbl_int_id,
exp_data_transformation.in_INDIV_PRTY_ID as in_INDIV_PRTY_ID,
exp_data_transformation.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_data_transformation.in_TRANS_END_DTTM as in_TRANS_END_DTTM,
exp_data_transformation.in_PRTY_INSRBL_INT_STRT_DTTM as in_PRTY_INSRBL_INT_STRT_DTTM,
exp_data_transformation.in_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD,
exp_data_transformation.Retired as Retired,
exp_data_transformation.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformation.out_PRCS_ID as out_PRCS_ID,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformation.Insupd_flag as Insupd_flag,
NULL as o_LOSSPARTY_TYPECODE,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE ( exp_data_transformation.Insupd_flag = ''I'' ) and ( exp_data_transformation.in_INDIV_PRTY_ID IS NOT NULL AND exp_data_transformation.in_insrbl_int_id IS NOT NULL ));


-- Component rtr_prty_insrbl_int_RETIRE, Type ROUTER Output Group RETIRE
CREATE OR REPLACE TEMPORARY TABLE rtr_prty_insrbl_int_RETIRE AS
(SELECT
exp_data_transformation.in_insrbl_int_id as in_insrbl_int_id,
exp_data_transformation.in_INDIV_PRTY_ID as in_INDIV_PRTY_ID,
exp_data_transformation.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM,
exp_data_transformation.in_TRANS_END_DTTM as in_TRANS_END_DTTM,
exp_data_transformation.in_PRTY_INSRBL_INT_STRT_DTTM as in_PRTY_INSRBL_INT_STRT_DTTM,
exp_data_transformation.in_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD,
exp_data_transformation.Retired as Retired,
exp_data_transformation.lkp_EDW_END_DTTM as lkp_EDW_END_DTTM,
exp_data_transformation.out_PRCS_ID as out_PRCS_ID,
exp_data_transformation.EDW_STRT_DTTM as EDW_STRT_DTTM,
exp_data_transformation.EDW_END_DTTM as EDW_END_DTTM,
exp_data_transformation.Insupd_flag as Insupd_flag,
NULL as o_LOSSPARTY_TYPECODE,
exp_data_transformation.source_record_id
FROM
exp_data_transformation
WHERE exp_data_transformation.Insupd_flag = ''R'' and exp_data_transformation.Retired != 0 and ( exp_data_transformation.lkp_EDW_END_DTTM = TO_timestamp ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ) and ( exp_data_transformation.in_INDIV_PRTY_ID IS NOT NULL AND exp_data_transformation.in_insrbl_int_id IS NOT NULL ));


-- Component updstr_insrbl_int_ins, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE updstr_insrbl_int_ins AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_prty_insrbl_int_INSUPD.in_INDIV_PRTY_ID as INDIV_PRTY_ID,
rtr_prty_insrbl_int_INSUPD.in_insrbl_int_id as in_insrbl_int_id1,
rtr_prty_insrbl_int_INSUPD.in_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD1,
rtr_prty_insrbl_int_INSUPD.in_PRTY_INSRBL_INT_STRT_DTTM as in_PRTY_INSRBL_INT_STRT_DTTM1,
rtr_prty_insrbl_int_INSUPD.out_PRCS_ID as out_PRCS_ID,
rtr_prty_insrbl_int_INSUPD.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_prty_insrbl_int_INSUPD.EDW_END_DTTM as EDW_END_DTTM1,
rtr_prty_insrbl_int_INSUPD.Retired as Retired1,
rtr_prty_insrbl_int_INSUPD.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM1,
rtr_prty_insrbl_int_INSUPD.in_TRANS_END_DTTM as in_TRANS_END_DTTM1,
rtr_prty_insrbl_int_INSUPD.o_LOSSPARTY_TYPECODE as o_LOSSPARTY_TYPECODE1,
0 as UPDATE_STRATEGY_ACTION,
rtr_prty_insrbl_int_INSUPD.source_record_id
FROM
rtr_prty_insrbl_int_INSUPD
);


-- Component updstr_insrbl_int_upd_retired, Type UPDATE 
CREATE OR REPLACE TEMPORARY TABLE updstr_insrbl_int_upd_retired AS
(
/* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
SELECT
rtr_prty_insrbl_int_RETIRE.in_INDIV_PRTY_ID as INDIV_PRTY_ID,
rtr_prty_insrbl_int_RETIRE.in_insrbl_int_id as INSRBL_INT_ID3,
rtr_prty_insrbl_int_RETIRE.in_PRTY_CLM_ROLE_CD as in_PRTY_CLM_ROLE_CD3,
rtr_prty_insrbl_int_RETIRE.out_PRCS_ID as out_PRCS_ID,
rtr_prty_insrbl_int_RETIRE.EDW_STRT_DTTM as EDW_STRT_DTTM1,
rtr_prty_insrbl_int_RETIRE.in_TRANS_STRT_DTTM as in_TRANS_STRT_DTTM3,
rtr_prty_insrbl_int_RETIRE.Retired as Retired3,
1 as UPDATE_STRATEGY_ACTION,rtr_prty_insrbl_int_RETIRE.source_record_id
FROM
rtr_prty_insrbl_int_RETIRE
);


-- Component exp_pass_to_target_ins, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_ins AS
(
SELECT
updstr_insrbl_int_ins.INDIV_PRTY_ID as INDIV_PRTY_ID,
updstr_insrbl_int_ins.in_insrbl_int_id1 as INSRBL_INT_ID,
updstr_insrbl_int_ins.in_PRTY_CLM_ROLE_CD1 as in_PRTY_CLM_ROLE_CD1,
updstr_insrbl_int_ins.in_PRTY_INSRBL_INT_STRT_DTTM1 as in_PRTY_INSRBL_INT_STRT_DTTM1,
TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) as in_PRTY_INSRBL_INT_END_DTTM,
updstr_insrbl_int_ins.out_PRCS_ID as PRCS_ID,
updstr_insrbl_int_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM1,
CASE WHEN updstr_insrbl_int_ins.Retired1 = 0 THEN TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) ELSE CURRENT_TIMESTAMP END as out_EDW_END_DTTM1,
updstr_insrbl_int_ins.in_TRANS_STRT_DTTM1 as in_TRANS_STRT_DTTM1,
CASE WHEN updstr_insrbl_int_ins.Retired1 = 0 THEN updstr_insrbl_int_ins.in_TRANS_END_DTTM1 ELSE updstr_insrbl_int_ins.in_TRANS_STRT_DTTM1 END as o_TRANS_END_DTTM,
updstr_insrbl_int_ins.source_record_id
FROM
updstr_insrbl_int_ins
);


-- Component exp_pass_to_target_upd_retired, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE exp_pass_to_target_upd_retired AS
(
SELECT
updstr_insrbl_int_upd_retired.INDIV_PRTY_ID as INDIV_PRTY_ID,
updstr_insrbl_int_upd_retired.INSRBL_INT_ID3 as INSRBL_INT_ID,
updstr_insrbl_int_upd_retired.in_PRTY_CLM_ROLE_CD3 as in_PRTY_CLM_ROLE_CD3,
updstr_insrbl_int_upd_retired.EDW_STRT_DTTM1 as in_EDW_STRT_DTTM,
DATEADD (
  SECOND,
  -1,
  updstr_insrbl_int_upd_retired.EDW_STRT_DTTM1
) AS EDW_END_DTTM1,
updstr_insrbl_int_upd_retired.in_TRANS_STRT_DTTM3 AS in_TRANS_STRT_DTTM,
DATEADD (
  SECOND,
  -1,
  updstr_insrbl_int_upd_retired.in_TRANS_STRT_DTTM3
) AS TRANS_END_DTTM,
updstr_insrbl_int_upd_retired.source_record_id
FROM
updstr_insrbl_int_upd_retired
);


-- Component PRTY_INSRBL_INT_retire, Type TARGET 
MERGE INTO DB_T_PROD_CORE.PRTY_INSRBL_INT
USING exp_pass_to_target_upd_retired ON (PRTY_INSRBL_INT.PRTY_ID = exp_pass_to_target_upd_retired.INDIV_PRTY_ID AND PRTY_INSRBL_INT.INSRBL_INT_ID = exp_pass_to_target_upd_retired.INSRBL_INT_ID AND PRTY_INSRBL_INT.PRTY_CLM_ROLE_CD = exp_pass_to_target_upd_retired.in_PRTY_CLM_ROLE_CD3 AND PRTY_INSRBL_INT.EDW_STRT_DTTM = exp_pass_to_target_upd_retired.in_EDW_STRT_DTTM)
WHEN MATCHED THEN UPDATE
SET
PRTY_ID = exp_pass_to_target_upd_retired.INDIV_PRTY_ID,
INSRBL_INT_ID = exp_pass_to_target_upd_retired.INSRBL_INT_ID,
PRTY_CLM_ROLE_CD = exp_pass_to_target_upd_retired.in_PRTY_CLM_ROLE_CD3,
EDW_STRT_DTTM = exp_pass_to_target_upd_retired.in_EDW_STRT_DTTM,
EDW_END_DTTM = exp_pass_to_target_upd_retired.EDW_END_DTTM1,
TRANS_STRT_DTTM = exp_pass_to_target_upd_retired.in_TRANS_STRT_DTTM,
TRANS_END_DTTM = exp_pass_to_target_upd_retired.TRANS_END_DTTM;


-- Component PRTY_INSRBL_INT_insupd, Type TARGET 
INSERT INTO DB_T_PROD_CORE.PRTY_INSRBL_INT
(
PRTY_ID,
INSRBL_INT_ID,
PRTY_CLM_ROLE_CD,
PRTY_INSRBL_INT_STRT_DTTM,
PRTY_INSRBL_INT_END_DTTM,
PRCS_ID,
EDW_STRT_DTTM,
EDW_END_DTTM,
TRANS_STRT_DTTM,
TRANS_END_DTTM
)
SELECT
exp_pass_to_target_ins.INDIV_PRTY_ID as PRTY_ID,
exp_pass_to_target_ins.INSRBL_INT_ID as INSRBL_INT_ID,
exp_pass_to_target_ins.in_PRTY_CLM_ROLE_CD1 as PRTY_CLM_ROLE_CD,
exp_pass_to_target_ins.in_PRTY_INSRBL_INT_STRT_DTTM1 as PRTY_INSRBL_INT_STRT_DTTM,
exp_pass_to_target_ins.in_PRTY_INSRBL_INT_END_DTTM as PRTY_INSRBL_INT_END_DTTM,
exp_pass_to_target_ins.PRCS_ID as PRCS_ID,
exp_pass_to_target_ins.EDW_STRT_DTTM1 as EDW_STRT_DTTM,
exp_pass_to_target_ins.out_EDW_END_DTTM1 as EDW_END_DTTM,
exp_pass_to_target_ins.in_TRANS_STRT_DTTM1 as TRANS_STRT_DTTM,
exp_pass_to_target_ins.o_TRANS_END_DTTM as TRANS_END_DTTM
FROM
exp_pass_to_target_ins;


END; ';