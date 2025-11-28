-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_WRK_EDW_HO_EMBLEM_POLICY_EXTRACT("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS ' 
DECLARE
       run_id STRING;
       PRCS_ID STRING;
	   v_start_time TIMESTAMP;
BEGIN
       run_id := (SELECT run_id FROM control_run_id WHERE worklet_name = :worklet_name ORDER BY insert_ts DESC LIMIT 1);
       PRCS_ID := (SELECT param_value FROM control_params WHERE run_id = :run_id AND param_name = ''PRCS_ID'' LIMIT 1);
	   v_start_time := CURRENT_TIMESTAMP();

-- Component LKP_AGMT_COVERAGES_COVPATTERN_CODE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_COVERAGES_COVPATTERN_CODE AS
(
SELECT trim(VAL) AS VAL,HOST_AGMT_NUM AS HOST_AGMT_NUM,TERM_NAME AS TERM_NAME,COV_NAME AS COV_NAME,
COV_PATTERNCODE AS COV_PATTERNCODE,AGMT_ID AS AGMT_ID,TERM_PATTERNCODE AS TERM_PATTERNCODE FROM (
SELECT  CASE 
WHEN f.FEAT_SBTYPE_CD=''OPT'' AND f.FEAT_DTL_CD_NAME  IN (''Actual'', ''Replacement'') THEN f.FEAT_DTL_CD_NAME  
WHEN f.FEAT_SBTYPE_CD=''OPT'' AND f.FEAT_DTL_CD_NAME NOT IN (''Actual'', ''Replacement'') THEN CAST(f.FEAT_DTL_VAL AS varchar) 
WHEN f.FEAT_SBTYPE_CD=''TERM'' AND LENGTH(AGMT_FEAT_TXT)>0 THEN AGMT_FEAT.AGMT_FEAT_TXT
WHEN f.FEAT_SBTYPE_CD=''TERM'' AND (LENGTH(AGMT_FEAT_TXT) =0 OR AGMT_FEAT_TXT IS NULL) THEN cast(AGMT_FEAT.AGMT_FEAT_AMT as varchar)
WHEN f.FEAT_SBTYPE_CD=''PKG'' THEN  f.FEAT_DTL_CD_NAME
END AS VAL , 
AGMT.HOST_AGMT_NUM AS HOST_AGMT_NUM,
CASE 
WHEN f.FEAT_SBTYPE_CD=''OPT'' THEN f2.FEAT_NAME
WHEN f.FEAT_SBTYPE_CD=''TERM'' THEN f.FEAT_NAME
WHEN f.FEAT_SBTYPE_CD=''PKG'' THEN f4.FEAT_NAME
END AS TERM_NAME, 
CASE WHEN f.FEAT_SBTYPE_CD=''TERM'' THEN f1.FEAT_NAME
WHEN f.FEAT_SBTYPE_CD=''OPT'' THEN f3.FEAT_NAME
WHEN f.FEAT_SBTYPE_CD=''PKG'' THEN f5.FEAT_NAME
END AS COV_NAME, 
CASE WHEN f.FEAT_SBTYPE_CD=''TERM'' THEN f1.NK_SRC_KEY
WHEN f.FEAT_SBTYPE_CD=''OPT'' THEN f3.NK_SRC_KEY
WHEN f.FEAT_SBTYPE_CD=''PKG'' THEN f5.NK_SRC_KEY
END AS COV_PATTERNCODE, 
AGMT.AGMT_ID as AGMT_ID,
CASE WHEN f.FEAT_SBTYPE_CD=''OPT'' THEN f2.NK_SRC_KEY
WHEN f.FEAT_SBTYPE_CD=''TERM'' THEN f.NK_SRC_KEY
WHEN f.FEAT_SBTYPE_CD=''PKG'' THEN f4.NK_SRC_KEY
END AS TERM_PATTERNCODE
FROM db_t_prod_core.FEAT f        
JOIN db_t_prod_core.AGMT_FEAT ON f.FEAT_ID=AGMT_FEAT.FEAT_ID  AND AGMT_FEAT.TRANS_END_DTTM  =  TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
JOIN db_t_prod_core.AGMT ON AGMT_FEAT.AGMT_ID=AGMT.AGMT_ID AND	AGMT.EDW_END_DTTM=TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
LEFT JOIN db_t_prod_core.FEAT_RLTD fr1 ON f.FEAT_ID=fr1.RLTD_FEAT_ID AND   fr1.FEAT_RLTNSHP_TYPE_CD =''COVT'' and  fr1.TRANS_END_DTTM= TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
LEFT JOIN db_t_prod_core.FEAT f1 ON fr1.FEAT_ID=f1.FEAT_ID
LEFT JOIN db_t_prod_core.FEAT_RLTD fr2 ON f.FEAT_ID=fr2.RLTD_FEAT_ID AND   fr2.FEAT_RLTNSHP_TYPE_CD =''TERMOPT'' and  fr2.TRANS_END_DTTM= TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
LEFT JOIN db_t_prod_core.FEAT f2 ON fr2.FEAT_ID=f2.FEAT_ID
LEFT JOIN db_t_prod_core.FEAT_RLTD fr3 ON f.FEAT_ID=fr3.RLTD_FEAT_ID AND   fr3.FEAT_RLTNSHP_TYPE_CD =''COVOPTT'' AND fr3.TRANS_END_DTTM= TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
LEFT JOIN db_t_prod_core.FEAT f3 ON fr3.FEAT_ID=f3.FEAT_ID
LEFT JOIN db_t_prod_core.FEAT_RLTD fr4 ON f.FEAT_ID=fr4.RLTD_FEAT_ID AND   fr4.FEAT_RLTNSHP_TYPE_CD =''TERMPKG'' and fr4.TRANS_END_DTTM= TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
LEFT JOIN db_t_prod_core.FEAT f4 ON fr4.FEAT_ID=f4.FEAT_ID
LEFT JOIN db_t_prod_core.FEAT_RLTD fr5 ON f.FEAT_ID=fr5.RLTD_FEAT_ID AND   fr5.FEAT_RLTNSHP_TYPE_CD =''COVPKG'' AND fr5.TRANS_END_DTTM= TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
LEFT JOIN db_t_prod_core.FEAT f5 ON fr5.FEAT_ID=f5.FEAT_ID
WHERE f.FEAT_SBTYPE_CD IN (''TERM'', ''OPT'', ''PKG'')
AND CASE WHEN f.FEAT_SBTYPE_CD=''TERM'' THEN f1.NK_SRC_KEY
WHEN f.FEAT_SBTYPE_CD=''OPT'' THEN f3.NK_SRC_KEY
WHEN f.FEAT_SBTYPE_CD=''PKG'' THEN f5.NK_SRC_KEY
END  NOT IN (''HOLI_SCHEDULEDPROPERTYDEDUCTIBLESITEM_ALFA'', ''HOSI_SCHEDULEDPROPERTYITEM_ALFA'')
UNION SELECT ARTICLE_TYPE, A.HOST_AGMT_NUM, ''ARTICLE_TYPE'' AS TERM_NAME, ''SCHEDULED PERSONAL PROPERTY ITEM'' AS COV_NAME , 
''HOSI_SCHEDULEDPROPERTYITEM_ALFA''  AS COV_PATTERNCODE,A.AGMT_ID, ''HOSI_ScheduledPropertyItemArticleType_alfa'' AS TERM_PATTERNCODE 
FROM  
(SELECT AGMT.AGMT_ID, AGMT.HOST_AGMT_NUM, PRTY_ASSET_CLASFCN.PRTY_ASSET_CLASFCN_DESC ARTICLE_TYPE , PRTY_ASSET.ASSET_DESC AS DESCRIPTION
FROM db_t_prod_core.AGMT
JOIN db_t_prod_core.AGMT_ASSET ON AGMT.AGMT_ID=AGMT_ASSET.AGMT_ID AND    AGMT_ASSET.TRANS_END_DTTM=TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
JOIN db_t_prod_core.PRTY_ASSET ON AGMT_ASSET.PRTY_ASSET_ID=PRTY_ASSET.PRTY_ASSET_ID   AND CURRENT_DATE BETWEEN PRTY_ASSET.PRTY_ASSET_STRT_DTTM AND PRTY_ASSET.PRTY_ASSET_END_DTTM 
JOIN db_t_prod_core.PRTY_ASSET_CLASFCN ON PRTY_ASSET_CLASFCN.PRTY_ASSET_CLASFCN_CD=PRTY_ASSET.PRTY_ASSET_CLASFCN_CD          
WHERE  PRTY_ASSET_SBTYPE_CD=''REALSP''
 AND	AGMT.EDW_END_DTTM=TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')            
 ) A
UNION 
SELECT  DESCRIPTION,B.HOST_AGMT_NUM, ''DESCRIPTION'' AS TERM_NAME, ''SCHEDULED PERSONAL PROPERTY ITEM'' AS COV_NAME , 
''HOSI_SCHEDULEDPROPERTYITEM_ALFA''  AS COV_PATTERNCODE, B.AGMT_ID,''HOSI_ScheduledPropertyItemArticleDescr_alfa'' AS TERM_PATTERNCODE
FROM  
(
SELECT AGMT.AGMT_ID, AGMT.HOST_AGMT_NUM, PRTY_ASSET_CLASFCN.PRTY_ASSET_CLASFCN_DESC ARTICLE_TYPE , PRTY_ASSET.ASSET_DESC AS DESCRIPTION
FROM db_t_prod_core.AGMT
JOIN db_t_prod_core.AGMT_ASSET ON AGMT.AGMT_ID=AGMT_ASSET.AGMT_ID AND    AGMT_ASSET.TRANS_END_DTTM=TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
JOIN db_t_prod_core.PRTY_ASSET ON AGMT_ASSET.PRTY_ASSET_ID=PRTY_ASSET.PRTY_ASSET_ID   AND CURRENT_DATE BETWEEN PRTY_ASSET.PRTY_ASSET_STRT_DTTM AND PRTY_ASSET.PRTY_ASSET_END_DTTM 
JOIN db_t_prod_core.PRTY_ASSET_CLASFCN ON PRTY_ASSET_CLASFCN.PRTY_ASSET_CLASFCN_CD=PRTY_ASSET.PRTY_ASSET_CLASFCN_CD
WHERE  PRTY_ASSET.PRTY_ASSET_SBTYPE_CD=''REALSP''
 AND	AGMT.EDW_END_DTTM=TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')    
) B )X/*  */
);


-- Component LKP_AGMT_COVERAGES_TERMPATTERN_CODE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_COVERAGES_TERMPATTERN_CODE AS
(
SELECT trim(VAL) AS VAL,HOST_AGMT_NUM AS HOST_AGMT_NUM,TERM_NAME AS TERM_NAME,COV_NAME AS COV_NAME,
COV_PATTERNCODE AS COV_PATTERNCODE,AGMT_ID AS AGMT_ID,TERM_PATTERNCODE AS TERM_PATTERNCODE FROM (
SELECT  CASE 
WHEN f.FEAT_SBTYPE_CD=''OPT'' AND f.FEAT_DTL_CD_NAME  IN (''Actual'', ''Replacement'') THEN f.FEAT_DTL_CD_NAME  
WHEN f.FEAT_SBTYPE_CD=''OPT'' AND f.FEAT_DTL_CD_NAME NOT IN (''Actual'', ''Replacement'') THEN CAST(f.FEAT_DTL_VAL AS varchar) 
WHEN f.FEAT_SBTYPE_CD=''TERM'' AND LENGTH(AGMT_FEAT_TXT)>0 THEN AGMT_FEAT.AGMT_FEAT_TXT
WHEN f.FEAT_SBTYPE_CD=''TERM'' AND (LENGTH(AGMT_FEAT_TXT) =0 OR AGMT_FEAT_TXT IS NULL) THEN cast(AGMT_FEAT.AGMT_FEAT_AMT as varchar)
WHEN f.FEAT_SBTYPE_CD=''PKG'' THEN  f.FEAT_DTL_CD_NAME
END AS VAL , 
AGMT.HOST_AGMT_NUM AS HOST_AGMT_NUM,
CASE 
WHEN f.FEAT_SBTYPE_CD=''OPT'' THEN f2.FEAT_NAME
WHEN f.FEAT_SBTYPE_CD=''TERM'' THEN f.FEAT_NAME
WHEN f.FEAT_SBTYPE_CD=''PKG'' THEN f4.FEAT_NAME
END AS TERM_NAME, 
CASE WHEN f.FEAT_SBTYPE_CD=''TERM'' THEN f1.FEAT_NAME
WHEN f.FEAT_SBTYPE_CD=''OPT'' THEN f3.FEAT_NAME
WHEN f.FEAT_SBTYPE_CD=''PKG'' THEN f5.FEAT_NAME
END AS COV_NAME, 
CASE WHEN f.FEAT_SBTYPE_CD=''TERM'' THEN f1.NK_SRC_KEY
WHEN f.FEAT_SBTYPE_CD=''OPT'' THEN f3.NK_SRC_KEY
WHEN f.FEAT_SBTYPE_CD=''PKG'' THEN f5.NK_SRC_KEY
END AS COV_PATTERNCODE, 
AGMT.AGMT_ID as AGMT_ID,
CASE WHEN f.FEAT_SBTYPE_CD=''OPT'' THEN f2.NK_SRC_KEY
WHEN f.FEAT_SBTYPE_CD=''TERM'' THEN f.NK_SRC_KEY
WHEN f.FEAT_SBTYPE_CD=''PKG'' THEN f4.NK_SRC_KEY
END AS TERM_PATTERNCODE
FROM db_t_prod_core.FEAT f        
JOIN db_t_prod_core.AGMT_FEAT ON f.FEAT_ID=AGMT_FEAT.FEAT_ID  AND AGMT_FEAT.TRANS_END_DTTM  =  TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
JOIN db_t_prod_core.AGMT ON AGMT_FEAT.AGMT_ID=AGMT.AGMT_ID AND	AGMT.EDW_END_DTTM=TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
LEFT JOIN db_t_prod_core.FEAT_RLTD fr1 ON f.FEAT_ID=fr1.RLTD_FEAT_ID AND   fr1.FEAT_RLTNSHP_TYPE_CD =''COVT'' and  fr1.TRANS_END_DTTM= TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
LEFT JOIN db_t_prod_core.FEAT f1 ON fr1.FEAT_ID=f1.FEAT_ID
LEFT JOIN db_t_prod_core.FEAT_RLTD fr2 ON f.FEAT_ID=fr2.RLTD_FEAT_ID AND   fr2.FEAT_RLTNSHP_TYPE_CD =''TERMOPT'' and  fr2.TRANS_END_DTTM= TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
LEFT JOIN db_t_prod_core.FEAT f2 ON fr2.FEAT_ID=f2.FEAT_ID
LEFT JOIN db_t_prod_core.FEAT_RLTD fr3 ON f.FEAT_ID=fr3.RLTD_FEAT_ID AND   fr3.FEAT_RLTNSHP_TYPE_CD =''COVOPTT'' AND fr3.TRANS_END_DTTM= TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
LEFT JOIN db_t_prod_core.FEAT f3 ON fr3.FEAT_ID=f3.FEAT_ID
LEFT JOIN db_t_prod_core.FEAT_RLTD fr4 ON f.FEAT_ID=fr4.RLTD_FEAT_ID AND   fr4.FEAT_RLTNSHP_TYPE_CD =''TERMPKG'' and fr4.TRANS_END_DTTM= TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
LEFT JOIN db_t_prod_core.FEAT f4 ON fr4.FEAT_ID=f4.FEAT_ID
LEFT JOIN db_t_prod_core.FEAT_RLTD fr5 ON f.FEAT_ID=fr5.RLTD_FEAT_ID AND   fr5.FEAT_RLTNSHP_TYPE_CD =''COVPKG'' AND fr5.TRANS_END_DTTM= TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
LEFT JOIN db_t_prod_core.FEAT f5 ON fr5.FEAT_ID=f5.FEAT_ID
WHERE f.FEAT_SBTYPE_CD IN (''TERM'', ''OPT'', ''PKG'')
AND CASE WHEN f.FEAT_SBTYPE_CD=''TERM'' THEN f1.NK_SRC_KEY
WHEN f.FEAT_SBTYPE_CD=''OPT'' THEN f3.NK_SRC_KEY
WHEN f.FEAT_SBTYPE_CD=''PKG'' THEN f5.NK_SRC_KEY
END  NOT IN (''HOLI_SCHEDULEDPROPERTYDEDUCTIBLESITEM_ALFA'', ''HOSI_SCHEDULEDPROPERTYITEM_ALFA'')
UNION SELECT ARTICLE_TYPE, A.HOST_AGMT_NUM, ''ARTICLE_TYPE'' AS TERM_NAME, ''SCHEDULED PERSONAL PROPERTY ITEM'' AS COV_NAME , 
''HOSI_SCHEDULEDPROPERTYITEM_ALFA''  AS COV_PATTERNCODE,A.AGMT_ID, ''HOSI_ScheduledPropertyItemArticleType_alfa'' AS TERM_PATTERNCODE 
FROM  
(SELECT AGMT.AGMT_ID, AGMT.HOST_AGMT_NUM, PRTY_ASSET_CLASFCN.PRTY_ASSET_CLASFCN_DESC ARTICLE_TYPE , PRTY_ASSET.ASSET_DESC AS DESCRIPTION
FROM db_t_prod_core.AGMT
JOIN db_t_prod_core.AGMT_ASSET ON AGMT.AGMT_ID=AGMT_ASSET.AGMT_ID AND    AGMT_ASSET.TRANS_END_DTTM=TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
JOIN db_t_prod_core.PRTY_ASSET ON AGMT_ASSET.PRTY_ASSET_ID=PRTY_ASSET.PRTY_ASSET_ID   AND CURRENT_DATE BETWEEN PRTY_ASSET.PRTY_ASSET_STRT_DTTM AND PRTY_ASSET.PRTY_ASSET_END_DTTM 
JOIN db_t_prod_core.PRTY_ASSET_CLASFCN ON PRTY_ASSET_CLASFCN.PRTY_ASSET_CLASFCN_CD=PRTY_ASSET.PRTY_ASSET_CLASFCN_CD          
WHERE  PRTY_ASSET_SBTYPE_CD=''REALSP''
 AND	AGMT.EDW_END_DTTM=TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')            
 ) A
UNION 
SELECT  DESCRIPTION,B.HOST_AGMT_NUM, ''DESCRIPTION'' AS TERM_NAME, ''SCHEDULED PERSONAL PROPERTY ITEM'' AS COV_NAME , 
''HOSI_SCHEDULEDPROPERTYITEM_ALFA''  AS COV_PATTERNCODE, B.AGMT_ID,''HOSI_ScheduledPropertyItemArticleDescr_alfa'' AS TERM_PATTERNCODE
FROM  
(
SELECT AGMT.AGMT_ID, AGMT.HOST_AGMT_NUM, PRTY_ASSET_CLASFCN.PRTY_ASSET_CLASFCN_DESC ARTICLE_TYPE , PRTY_ASSET.ASSET_DESC AS DESCRIPTION
FROM db_t_prod_core.AGMT
JOIN db_t_prod_core.AGMT_ASSET ON AGMT.AGMT_ID=AGMT_ASSET.AGMT_ID AND    AGMT_ASSET.TRANS_END_DTTM=TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
JOIN db_t_prod_core.PRTY_ASSET ON AGMT_ASSET.PRTY_ASSET_ID=PRTY_ASSET.PRTY_ASSET_ID   AND CURRENT_DATE BETWEEN PRTY_ASSET.PRTY_ASSET_STRT_DTTM AND PRTY_ASSET.PRTY_ASSET_END_DTTM 
JOIN db_t_prod_core.PRTY_ASSET_CLASFCN ON PRTY_ASSET_CLASFCN.PRTY_ASSET_CLASFCN_CD=PRTY_ASSET.PRTY_ASSET_CLASFCN_CD
WHERE  PRTY_ASSET.PRTY_ASSET_SBTYPE_CD=''REALSP''
 AND	AGMT.EDW_END_DTTM=TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')    
) B )X/*  */
);


-- Component LKP_AGMT_COVERAGES_TERM_COV_CODE, Type Prerequisite Lookup Object 
CREATE OR REPLACE TEMPORARY TABLE LKP_AGMT_COVERAGES_TERM_COV_CODE AS
(
SELECT trim(VAL) AS VAL,HOST_AGMT_NUM AS HOST_AGMT_NUM,TERM_NAME AS TERM_NAME,COV_NAME AS COV_NAME,
COV_PATTERNCODE AS COV_PATTERNCODE,AGMT_ID AS AGMT_ID,TERM_PATTERNCODE AS TERM_PATTERNCODE FROM (
SELECT  CASE 
WHEN f.FEAT_SBTYPE_CD=''OPT'' AND f.FEAT_DTL_CD_NAME  IN (''Actual'', ''Replacement'') THEN f.FEAT_DTL_CD_NAME  
WHEN f.FEAT_SBTYPE_CD=''OPT'' AND f.FEAT_DTL_CD_NAME NOT IN (''Actual'', ''Replacement'') THEN CAST(f.FEAT_DTL_VAL AS varchar) 
WHEN f.FEAT_SBTYPE_CD=''TERM'' AND LENGTH(AGMT_FEAT_TXT)>0 THEN AGMT_FEAT.AGMT_FEAT_TXT
WHEN f.FEAT_SBTYPE_CD=''TERM'' AND (LENGTH(AGMT_FEAT_TXT) =0 OR AGMT_FEAT_TXT IS NULL) THEN cast(AGMT_FEAT.AGMT_FEAT_AMT as varchar)
WHEN f.FEAT_SBTYPE_CD=''PKG'' THEN  f.FEAT_DTL_CD_NAME
END AS VAL , 
AGMT.HOST_AGMT_NUM AS HOST_AGMT_NUM,
CASE 
WHEN f.FEAT_SBTYPE_CD=''OPT'' THEN f2.FEAT_NAME
WHEN f.FEAT_SBTYPE_CD=''TERM'' THEN f.FEAT_NAME
WHEN f.FEAT_SBTYPE_CD=''PKG'' THEN f4.FEAT_NAME
END AS TERM_NAME, 
CASE WHEN f.FEAT_SBTYPE_CD=''TERM'' THEN f1.FEAT_NAME
WHEN f.FEAT_SBTYPE_CD=''OPT'' THEN f3.FEAT_NAME
WHEN f.FEAT_SBTYPE_CD=''PKG'' THEN f5.FEAT_NAME
END AS COV_NAME, 
CASE WHEN f.FEAT_SBTYPE_CD=''TERM'' THEN f1.NK_SRC_KEY
WHEN f.FEAT_SBTYPE_CD=''OPT'' THEN f3.NK_SRC_KEY
WHEN f.FEAT_SBTYPE_CD=''PKG'' THEN f5.NK_SRC_KEY
END AS COV_PATTERNCODE, 
AGMT.AGMT_ID as AGMT_ID,
CASE WHEN f.FEAT_SBTYPE_CD=''OPT'' THEN f2.NK_SRC_KEY
WHEN f.FEAT_SBTYPE_CD=''TERM'' THEN f.NK_SRC_KEY
WHEN f.FEAT_SBTYPE_CD=''PKG'' THEN f4.NK_SRC_KEY
END AS TERM_PATTERNCODE
FROM db_t_prod_core.FEAT f        
JOIN db_t_prod_core.AGMT_FEAT ON f.FEAT_ID=AGMT_FEAT.FEAT_ID  AND AGMT_FEAT.TRANS_END_DTTM  =  TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
JOIN db_t_prod_core.AGMT ON AGMT_FEAT.AGMT_ID=AGMT.AGMT_ID AND	AGMT.EDW_END_DTTM=TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
LEFT JOIN db_t_prod_core.FEAT_RLTD fr1 ON f.FEAT_ID=fr1.RLTD_FEAT_ID AND   fr1.FEAT_RLTNSHP_TYPE_CD =''COVT'' and  fr1.TRANS_END_DTTM= TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
LEFT JOIN db_t_prod_core.FEAT f1 ON fr1.FEAT_ID=f1.FEAT_ID
LEFT JOIN db_t_prod_core.FEAT_RLTD fr2 ON f.FEAT_ID=fr2.RLTD_FEAT_ID AND   fr2.FEAT_RLTNSHP_TYPE_CD =''TERMOPT'' and  fr2.TRANS_END_DTTM= TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
LEFT JOIN db_t_prod_core.FEAT f2 ON fr2.FEAT_ID=f2.FEAT_ID
LEFT JOIN db_t_prod_core.FEAT_RLTD fr3 ON f.FEAT_ID=fr3.RLTD_FEAT_ID AND   fr3.FEAT_RLTNSHP_TYPE_CD =''COVOPTT'' AND fr3.TRANS_END_DTTM= TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
LEFT JOIN db_t_prod_core.FEAT f3 ON fr3.FEAT_ID=f3.FEAT_ID
LEFT JOIN db_t_prod_core.FEAT_RLTD fr4 ON f.FEAT_ID=fr4.RLTD_FEAT_ID AND   fr4.FEAT_RLTNSHP_TYPE_CD =''TERMPKG'' and fr4.TRANS_END_DTTM= TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
LEFT JOIN db_t_prod_core.FEAT f4 ON fr4.FEAT_ID=f4.FEAT_ID
LEFT JOIN db_t_prod_core.FEAT_RLTD fr5 ON f.FEAT_ID=fr5.RLTD_FEAT_ID AND   fr5.FEAT_RLTNSHP_TYPE_CD =''COVPKG'' AND fr5.TRANS_END_DTTM= TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
LEFT JOIN db_t_prod_core.FEAT f5 ON fr5.FEAT_ID=f5.FEAT_ID
WHERE f.FEAT_SBTYPE_CD IN (''TERM'', ''OPT'', ''PKG'')
AND CASE WHEN f.FEAT_SBTYPE_CD=''TERM'' THEN f1.NK_SRC_KEY
WHEN f.FEAT_SBTYPE_CD=''OPT'' THEN f3.NK_SRC_KEY
WHEN f.FEAT_SBTYPE_CD=''PKG'' THEN f5.NK_SRC_KEY
END  NOT IN (''HOLI_SCHEDULEDPROPERTYDEDUCTIBLESITEM_ALFA'', ''HOSI_SCHEDULEDPROPERTYITEM_ALFA'')
UNION SELECT ARTICLE_TYPE, A.HOST_AGMT_NUM, ''ARTICLE_TYPE'' AS TERM_NAME, ''SCHEDULED PERSONAL PROPERTY ITEM'' AS COV_NAME , 
''HOSI_SCHEDULEDPROPERTYITEM_ALFA''  AS COV_PATTERNCODE,A.AGMT_ID, ''HOSI_ScheduledPropertyItemArticleType_alfa'' AS TERM_PATTERNCODE 
FROM  
(SELECT AGMT.AGMT_ID, AGMT.HOST_AGMT_NUM, PRTY_ASSET_CLASFCN.PRTY_ASSET_CLASFCN_DESC ARTICLE_TYPE , PRTY_ASSET.ASSET_DESC AS DESCRIPTION
FROM db_t_prod_core.AGMT
JOIN db_t_prod_core.AGMT_ASSET ON AGMT.AGMT_ID=AGMT_ASSET.AGMT_ID AND    AGMT_ASSET.TRANS_END_DTTM=TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
JOIN db_t_prod_core.PRTY_ASSET ON AGMT_ASSET.PRTY_ASSET_ID=PRTY_ASSET.PRTY_ASSET_ID   AND CURRENT_DATE BETWEEN PRTY_ASSET.PRTY_ASSET_STRT_DTTM AND PRTY_ASSET.PRTY_ASSET_END_DTTM 
JOIN db_t_prod_core.PRTY_ASSET_CLASFCN ON PRTY_ASSET_CLASFCN.PRTY_ASSET_CLASFCN_CD=PRTY_ASSET.PRTY_ASSET_CLASFCN_CD          
WHERE  PRTY_ASSET_SBTYPE_CD=''REALSP''
 AND	AGMT.EDW_END_DTTM=TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')            
 ) A
UNION 
SELECT  DESCRIPTION,B.HOST_AGMT_NUM, ''DESCRIPTION'' AS TERM_NAME, ''SCHEDULED PERSONAL PROPERTY ITEM'' AS COV_NAME , 
''HOSI_SCHEDULEDPROPERTYITEM_ALFA''  AS COV_PATTERNCODE, B.AGMT_ID,''HOSI_ScheduledPropertyItemArticleDescr_alfa'' AS TERM_PATTERNCODE
FROM  
(
SELECT AGMT.AGMT_ID, AGMT.HOST_AGMT_NUM, PRTY_ASSET_CLASFCN.PRTY_ASSET_CLASFCN_DESC ARTICLE_TYPE , PRTY_ASSET.ASSET_DESC AS DESCRIPTION
FROM db_t_prod_core.AGMT
JOIN db_t_prod_core.AGMT_ASSET ON AGMT.AGMT_ID=AGMT_ASSET.AGMT_ID AND    AGMT_ASSET.TRANS_END_DTTM=TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')
JOIN db_t_prod_core.PRTY_ASSET ON AGMT_ASSET.PRTY_ASSET_ID=PRTY_ASSET.PRTY_ASSET_ID   AND CURRENT_DATE BETWEEN PRTY_ASSET.PRTY_ASSET_STRT_DTTM AND PRTY_ASSET.PRTY_ASSET_END_DTTM 
JOIN db_t_prod_core.PRTY_ASSET_CLASFCN ON PRTY_ASSET_CLASFCN.PRTY_ASSET_CLASFCN_CD=PRTY_ASSET.PRTY_ASSET_CLASFCN_CD
WHERE  PRTY_ASSET.PRTY_ASSET_SBTYPE_CD=''REALSP''
 AND	AGMT.EDW_END_DTTM=TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'', ''MM/DD/YYYY HH24:MI:SS.FF6'')    
) B )X/*  */
);


-- Component SQ_HO_EMBLEM_PLCY_STG, Type SOURCE 

CREATE OR REPLACE TEMPORARY TABLE sq_ho_emblem_plcy_stg AS
WITH
plcy_rated_score AS (
    SELECT agmt_id,
           MAX(insrnc_quotn.rtd_insrnc_scr_val) AS rtd_insrnc_scr_val
    FROM db_t_prod_core.quotn_agmt
    JOIN db_t_prod_core.insrnc_quotn
      ON quotn_agmt.quotn_id = insrnc_quotn.quotn_id
    WHERE insrnc_quotn.rtd_insrnc_scr_val IS NOT NULL
      AND TO_CHAR(quotn_agmt.trans_end_dttm,''MM-DD-YYYY'') = ''12-31-9999''
      AND insrnc_quotn.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    GROUP BY agmt_id
),

indiv_name_stg AS (
    SELECT indiv_prty_id,
           MAX(gvn_name)  AS gvn_name,
           MAX(mdl_name)  AS mdl_name,
           MAX(fmly_name) AS fmly_name
    FROM db_t_prod_core.indiv_name
    WHERE edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    GROUP BY indiv_prty_id
),

mortgagee_name AS (
    SELECT MAX(indiv_name.indiv_full_name) AS indiv_full_name,
           prty_to_prty_asset.prty_asset_id
    FROM db_t_prod_core.prty_to_prty_asset
    LEFT JOIN db_t_prod_core.indiv_name
      ON indiv_name.indiv_prty_id = prty_to_prty_asset.prty_id
    WHERE asset_role_cd = ''MRTGEE''
      AND indiv_name.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
      AND prty_to_prty_asset.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    GROUP BY prty_to_prty_asset.prty_asset_id
),

agmt_discounts AS (
    SELECT af.agmt_id,
           f.nk_src_key,
           f.feat_name,
           af.agmt_feat_rate
    FROM db_t_prod_core.agmt_feat af
    JOIN db_t_prod_core.feat f
      ON af.feat_id = f.feat_id
    WHERE af.agmt_feat_role_cd IN (''POL'',''PPV'')
      AND f.feat_sbtype_cd = ''MOD''
      AND af.trans_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
      AND f.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
),

location_address AS (
    SELECT pal.prty_asset_id,
           pal.loc_id,
           pal.prty_asset_loctr_role_cd,
           cnty.geogrcl_area_name AS county,
           street_addr.addr_ln_1_txt AS plcy_mail_address_1,
           pal.fire_dept_id,
           postl_cd.postl_cd_num AS zip
    FROM db_t_prod_core.prty_asset_loctr pal
    JOIN db_t_prod_core.street_addr
      ON street_addr.street_addr_id = pal.loc_id
     AND street_addr.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    LEFT JOIN db_t_prod_core.cnty
      ON cnty.cnty_id = street_addr.cnty_id
     AND cnty.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    LEFT JOIN db_t_prod_core.postl_cd
      ON postl_cd.postl_cd_id = street_addr.postl_cd_id
     AND postl_cd.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    WHERE pal.prty_asset_loctr_role_cd = ''RSKLCTN''
      AND TO_CHAR(pal.trans_end_dttm,''MM/DD/YYYY'') = ''12/31/9999''
),

email_address AS (
    SELECT MAX(el.elctrnc_addr_txt) AS elctrnc_addr_txt,
           prty_addr.prty_id
    FROM db_t_prod_core.prty_addr
    JOIN db_t_prod_core.elctrnc_addr el
      ON prty_addr.loc_id = el.elctrnc_addr_id
    WHERE prty_addr.prty_addr_usge_type_cd = ''PREMAIL''
      AND prty_addr.trans_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
      AND el.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    GROUP BY prty_addr.prty_id
),

prev_carrier AS (
    SELECT MAX(org_name) AS org_name,
           prty_rltd.prty_id
    FROM db_t_prod_core.prty_rltd
    JOIN db_t_prod_core.org_name
      ON prty_rltd.rltd_prty_id = org_name.prty_id
    WHERE prty_rltd.prty_rltd_role_cd = ''PRIINSCAR''
      AND prty_rltd.trans_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
      AND org_name.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    GROUP BY prty_rltd.prty_id
),

mh_park AS (
    SELECT MAX(m.mfg_home_prk_name) AS mfg_home_prk_name,
           r.prty_asset_id
    FROM db_t_prod_core.real_estat r
    JOIN db_t_prod_core.mfg_home_prk m
      ON r.mfg_home_prk_id = m.mfg_home_prk_id
    WHERE m.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    GROUP BY r.prty_asset_id
),

asset_detail AS (
    SELECT x.asset_dtl_cd,
           t.asset_dtl_desc,
           t.asset_dtl_schm_type_cd,
           x.prty_asset_id
    FROM db_t_prod_core.asset_dtl_cd_xref x
    JOIN db_t_prod_core.asset_dtl_type t
      ON t.asset_dtl_cd = x.asset_dtl_cd
    WHERE x.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
      AND x.asset_dtl_cd <> ''UNK''
),

prty_asset_spec_staging AS (
    SELECT *
    FROM db_t_prod_core.prty_asset_spec
    WHERE edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
),

service_center AS (
    SELECT MAX(o.org_name) AS org_name,
           pa.agmt_id
    FROM db_t_prod_core.org_name o
    JOIN db_t_prod_core.prty_agmt pa
      ON o.prty_id = pa.prty_id
    WHERE pa.prty_agmt_role_cd = ''SVC''
      AND pa.trans_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
      AND o.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    GROUP BY pa.agmt_id
),

agent AS (
    SELECT MAX(io.intrnl_org_num) AS intrnl_org_num,
           pa.agmt_id
    FROM db_t_prod_core.prty_agmt pa
    JOIN db_t_prod_core.intrnl_org io
      ON pa.prty_id = io.intrnl_org_prty_id
    WHERE pa.prty_agmt_role_cd = ''PRDA''
      AND io.intrnl_org_sbtype_cd = ''PRDA''
      AND pa.trans_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
      AND io.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    GROUP BY pa.agmt_id
),

membership AS (
    SELECT m.mbrshp_num,
           am.agmt_id,
           m.mbrshp_type_cd
    FROM db_t_prod_core.agmt_mbrshp am
    JOIN db_t_prod_core.mbrshp m
      ON am.mbrshp_id = m.mbrshp_id
    WHERE am.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
      AND m.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
),

underwriting AS (
    SELECT MAX(o.intrnl_org_num) AS prty_desc,
           pa.agmt_id
    FROM db_t_prod_core.intrnl_org o
    JOIN db_t_prod_core.prty_agmt pa
      ON o.intrnl_org_prty_id = pa.prty_id
    WHERE pa.prty_agmt_role_cd = ''CMP''
      AND pa.trans_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
      AND o.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    GROUP BY pa.agmt_id
),

prty_score_hierarchy AS (
    SELECT a.prty_id,
           a.modl_run_dttm,
           a.modl_name,
           LTRIM(b.prty_scr_val) AS prty_scr_val
    FROM (
           SELECT prty_scr.prty_id,
                  MAX(modl_run.modl_run_dttm) AS modl_run_dttm,
                  anltcl_modl.modl_name
           FROM db_t_prod_core.prty_scr
           JOIN db_t_prod_core.modl_run
             ON prty_scr.modl_run_id = modl_run.modl_run_id
           JOIN db_t_prod_core.anltcl_modl
             ON prty_scr.modl_id = anltcl_modl.modl_id
           WHERE modl_run.modl_run_dttm < CURRENT_DATE
             AND modl_run.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
             AND anltcl_modl.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
             AND prty_scr.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
           GROUP BY prty_scr.prty_id, anltcl_modl.modl_name
         ) a
    JOIN (
           SELECT prty_scr.prty_id,
                  modl_run.modl_run_dttm,
                  anltcl_modl.modl_name,
                  CASE WHEN TO_NUMBER(prty_scr_val) IS NULL THEN prty_scr_val
                       ELSE CAST(prty_scr_val AS INTEGER)
                  END AS prty_scr_val
           FROM db_t_prod_core.prty_scr
           JOIN db_t_prod_core.modl_run
             ON prty_scr.modl_run_id = modl_run.modl_run_id
           JOIN db_t_prod_core.anltcl_modl
             ON prty_scr.modl_id = anltcl_modl.modl_id
           WHERE modl_run.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
             AND anltcl_modl.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
             AND prty_scr.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
         ) b
      ON a.prty_id = b.prty_id
     AND a.modl_name = b.modl_name
     AND a.modl_run_dttm = b.modl_run_dttm
),

address_hierarchy AS (
    SELECT loctr.loctr_id,
           street_addr.addr_ln_1_txt AS plcy_mail_address_1,
           street_addr.addr_ln_2_txt AS plcy_mail_address_2,
           city.geogrcl_area_name AS city,
           terr.geogrcl_area_name AS state,
           postl_cd.postl_cd_num AS zip,
           cnty.geogrcl_area_name AS county
    FROM db_t_prod_core.loctr
    JOIN db_t_prod_core.street_addr
      ON loctr.loctr_id = street_addr.street_addr_id
     AND street_addr.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    LEFT JOIN db_t_prod_core.city
      ON street_addr.city_id = city.city_id
     AND city.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    LEFT JOIN db_t_prod_core.terr
      ON street_addr.terr_id = terr.terr_id
     AND terr.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    LEFT JOIN db_t_prod_core.postl_cd
      ON street_addr.postl_cd_id = postl_cd.postl_cd_id
     AND postl_cd.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    LEFT JOIN db_t_prod_core.cnty
      ON street_addr.cnty_id = cnty.cnty_id
     AND cnty.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    LEFT JOIN db_t_prod_core.ctry ctry
      ON street_addr.ctry_id = ctry.ctry_id
     AND ctry.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    WHERE loctr.loctr_sbtype_cd = ''AD''
      AND loctr.addr_sbtype_cd  = ''STADR''
),

addinsured_prty AS (
    SELECT pa.agmt_id,
           prty_to_prty_asset.asset_role_cd
    FROM db_t_prod_core.prty_to_prty_asset
    JOIN db_t_prod_core.prty_agmt pa
      ON prty_to_prty_asset.prty_id = pa.prty_id
     AND pa.prty_agmt_role_cd = ''PLCYADDINS''
    WHERE prty_to_prty_asset.asset_role_cd = ''NONOCCOWN''
      AND TO_CHAR(prty_to_prty_asset.trans_end_dttm,''MM/DD/YYYY'') = ''12/31/9999''
      AND TO_CHAR(pa.trans_end_dttm,''MM/DD/YYYY'') = ''12/31/9999''
),

risk_state AS (
    SELECT a.agmt_id,
           d.geogrcl_area_shrt_name,
           c.prty_asset_loctr_role_cd
    FROM db_t_prod_core.agmt a
    JOIN db_t_prod_core.agmt_asset b ON b.agmt_id = a.agmt_id
    JOIN db_t_prod_core.prty_asset_loctr c ON c.prty_asset_id = b.prty_asset_id
      AND c.prty_asset_loctr_role_cd = ''RSKST''
    JOIN db_t_prod_core.terr d ON d.terr_id = c.loc_id
    WHERE a.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
      AND b.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
      AND c.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
      AND d.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
),

risk_cnty AS (
    SELECT a.agmt_id,
           e.geogrcl_area_shrt_name
    FROM db_t_prod_core.agmt a
    JOIN db_t_prod_core.agmt_asset b ON b.agmt_id = a.agmt_id
    JOIN db_t_prod_core.prty_asset_loctr c ON c.prty_asset_id = b.prty_asset_id
      AND c.prty_asset_loctr_role_cd = ''RSKCNTY''
    JOIN db_t_prod_core.cnty e ON e.cnty_id = c.loc_id
    WHERE a.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
      AND b.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
      AND c.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
      AND e.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
),

plcy_mail_address AS (
    SELECT prty_addr.prty_id,
           ah.plcy_mail_address_1,
           ah.plcy_mail_address_2,
           ah.city,
           ah.state,
           ah.zip,
           ah.county
    FROM db_t_prod_core.prty_addr
    JOIN address_hierarchy ah ON prty_addr.loc_id = ah.loctr_id
    WHERE prty_addr.prty_addr_usge_type_cd IN (''OTHRADR'',''MAILADR'')
      AND TO_CHAR(prty_addr.edw_end_dttm,''MM/DD/YYYY'') = ''12/31/9999''
),

membership_agg AS (
    SELECT agmt_id,
           MAX(mbrshp_num)     AS member_number,
           MAX(mbrshp_type_cd) AS member_type
    FROM membership
    GROUP BY agmt_id
),

prty_asset_spec_agg AS (
    SELECT prty_asset_id,
           MAX(CASE WHEN prty_asset_spec_type_cd = ''SIZE'' AND prty_asset_spec_uom_cd = ''SQFT''
                    THEN CAST(prty_asset_spec_meas AS INTEGER) END) AS square_footage,
           MAX(CASE WHEN prty_asset_spec_type_cd IN (''POOLEXST'',''TRMPEXST'') THEN prty_asset_ind END) AS pool_hot_tub_ind,
           MAX(CASE WHEN prty_asset_spec_type_cd = ''LIABEXPSR'' THEN prty_asset_ind END) AS liability_exp_ind,
           MAX(CASE WHEN prty_asset_spec_type_cd = ''FDDT'' THEN prty_asset_ind END) AS miles_to_fire_dept,
           MAX(CASE WHEN prty_asset_spec_type_cd = ''FHT'' THEN prty_asset_ind END) AS feet_to_fire_hyd,
           MAX(CASE WHEN prty_asset_spec_type_cd = ''FLOOR'' THEN prty_asset_spec_cnt END) AS nbr_stories,
           MAX(CASE WHEN prty_asset_spec_type_cd = ''MHMFR'' THEN prty_asset_spec_val END) AS manufacturer,
           MAX(CASE WHEN prty_asset_spec_type_cd = ''MHUNDRPN'' THEN prty_asset_ind END) AS mh_underpinning_credit
    FROM prty_asset_spec_staging
    GROUP BY prty_asset_id
),

asset_detail_agg AS (
    SELECT prty_asset_id,
           MAX(CASE WHEN asset_dtl_schm_type_cd = ''HEAT'' THEN asset_dtl_cd END)   AS primary_heating_type,
           MAX(CASE WHEN asset_dtl_schm_type_cd = ''HEAT'' THEN asset_dtl_desc END) AS primary_heating_desc,
           MAX(CASE WHEN asset_dtl_schm_type_cd = ''DWELUSE'' AND asset_dtl_cd = ''SECNDRY'' THEN 1 ELSE 0 END) AS has_secondary_res,
           MAX(CASE WHEN asset_dtl_schm_type_cd = ''DWEL'' THEN asset_dtl_cd END)   AS fire_prot_service,
           MAX(CASE WHEN asset_dtl_schm_type_cd = ''RESD'' THEN asset_dtl_desc END) AS nbr_families,
           MAX(CASE WHEN asset_dtl_schm_type_cd = ''FOUND'' THEN asset_dtl_desc END) AS substructure
    FROM asset_detail
    GROUP BY prty_asset_id
),

prty_asset_spec_prot AS (
    SELECT prty_asset_id,
           MAX(prty_asset_spec_val) AS protection_class
    FROM db_t_prod_core.prty_asset_spec
    WHERE prty_asset_spec.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
      AND prty_asset_spec_type_cd = ''PROT''
    GROUP BY prty_asset_id
),

agmt_discounts_agg AS (
    SELECT agmt_id,
           MAX(CASE WHEN nk_src_key = ''HOConstructionYear_alfa'' THEN agmt_feat_rate * 100 END)   AS nwhm_pct,
           MAX(CASE WHEN nk_src_key = ''HOMultiPolicyDiscount_alfa'' THEN ''1'' END)                 AS auto_dsc_ind,
           MAX(CASE WHEN nk_src_key = ''HOHomeAlert_alfa'' THEN agmt_feat_rate * 100 END)          AS hapc_pct,
           MAX(CASE WHEN nk_src_key = ''HOLengthOfService_alfa'' THEN agmt_feat_rate * 100 END)    AS vclp_pct,
           MAX(CASE WHEN nk_src_key = ''HOSprinklerDiscount_alfa'' THEN agmt_feat_rate * 100 END)  AS sprnklr_discount_pct,
           MAX(CASE WHEN nk_src_key = ''HOClaimsFreeDiscount_alfa'' THEN ''1'' END)                  AS clm_free_ind
    FROM agmt_discounts
    GROUP BY agmt_id
),

ev_rewrite_agg AS (
    SELECT agmt_id,
           MAX(ev_end_dttm) AS rewrite_dt
    FROM db_t_prod_core.ev
    WHERE ev_actvy_type_cd = ''REWRT''
      AND ev_end_dttm < CURRENT_DATE
    GROUP BY agmt_id
),

endorsement_zone_agg AS (
    SELECT d.agmt_id,
           MAX(CASE WHEN h.feat_name=''DEDUCTIBLE'' THEN f.feat_dtl_val END) AS endorsement,
           MAX(CASE WHEN h.feat_name=''ZONE'' THEN f.feat_dtl_val END) AS zon,
           MAX(a.feat_id) AS feat_id
    FROM db_t_prod_core.feat a
    INNER JOIN db_t_prod_core.feat_rltd b ON a.feat_id = b.feat_id
    INNER JOIN db_t_prod_core.feat f ON f.feat_id = b.rltd_feat_id
    INNER JOIN db_t_prod_core.agmt_insrd_asset_feat e ON e.feat_id = f.feat_id
    INNER JOIN db_t_prod_core.agmt d ON d.agmt_id = e.agmt_id
    INNER JOIN db_t_prod_core.feat_rltd g ON g.rltd_feat_id = f.feat_id
    INNER JOIN db_t_prod_core.feat h ON h.feat_id = g.feat_id
      AND h.feat_sbtype_cd = ''TERM''
    WHERE a.nk_src_key = ''HODW_EARTHQUAKE_HOE''
    GROUP BY d.agmt_id
),

prty_score_lexis AS (
    SELECT prty_id,
           MAX(prty_scr_val) AS prty_scr_val
    FROM prty_score_hierarchy
    WHERE modl_name = ''LEXIS NEXIS''
    GROUP BY prty_id
),

addinsured_agg AS (
    SELECT agmt_id,
           MAX(asset_role_cd) AS non_occ_deed_owner
    FROM addinsured_prty
    GROUP BY agmt_id
),

encumbrance_agg AS (
    SELECT rdt.prty_asset_id,
           MAX(et.encmce_type_desc) AS encumbrance_desc
    FROM db_t_prod_core.real_estat_dtl rdt
    LEFT JOIN db_t_prod_core.encmce_type et
      ON et.encmce_type_cd = rdt.encmce_type_cd
     AND et.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    WHERE rdt.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    GROUP BY rdt.prty_asset_id
),

indiv_mrtl_sts_agg AS (
    SELECT indiv_prty_id,
           MAX(mrtl_sts_cd) AS mrtl_sts_cd
    FROM db_t_prod_core.indiv_mrtl_sts
    WHERE edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    GROUP BY indiv_prty_id
),

agmt_asset_main AS (
    SELECT aa.agmt_id,
           MAX(aa.prty_asset_id) AS prty_asset_id
    FROM db_t_prod_core.agmt_asset aa
    JOIN db_t_prod_core.prty_asset pa
      ON aa.prty_asset_id = pa.prty_asset_id
    WHERE pa.prty_asset_sbtype_cd IN (''RE'',''REALDW'')
      AND pa.prty_asset_clasfcn_cd = ''MAIN''
      AND aa.trans_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
      AND pa.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    GROUP BY aa.agmt_id
),

cnstrctn_type_lkp AS (
    SELECT cnstrctn_type_cd,
           cnstrctn_type_desc
    FROM db_t_prod_core.cnstrctn_type
    WHERE edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
),

fire_dept_lkp AS (
    SELECT fire_dept_id,
           fire_dept_cd
    FROM db_t_prod_core.fire_dept
    WHERE edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
),

agmt_max_modl AS (
    SELECT host_agmt_num,
           MAX(modl_crtn_dttm) AS max_modl_crtn_dttm
    FROM db_t_prod_core.agmt
    WHERE agmt_type_cd = ''PPV''
      AND agmt_cur_sts_cd = ''BOUND''
    GROUP BY host_agmt_num
)

SELECT
    CURRENT_DATE                                                     AS eom_date,
    agmt.agmt_id                                                     AS agmt_id,
    asset.prty_asset_id                                              AS prty_asset_id,
    insrnc_quotn.quotn_id                                            AS quotn_id,
    insrnc_quotn.aplctn_id                                           AS aplctn_id,
    ''GW''                                                             AS plcy_system,
    underwriting.prty_desc                                           AS plcy_company,
    prod.prod_name                                                   AS plcy_lob,
    UPPER(prod.prod_desc)                                            AS policy_type,
    membership_agg.member_number                                     AS member_number,
    membership_agg.member_type                                       AS member_type,
    agmt.host_agmt_num                                               AS plcy_nbr,
    agmt.agmt_eff_dttm                                               AS original_icp_dt,
    agmt.agmt_eff_dttm                                               AS term_eff_dt,
    agmt.modl_eff_dttm                                               AS period_eff_dt,
    agmt.agmt_plnd_expn_dttm                                         AS plcy_exp_dt,
    ev_rewrite_agg.rewrite_dt                                        AS rewrite_dt,
    agent.intrnl_org_num                                             AS agent_nbr,
    service_center.org_name                                          AS svc_nbr,
    prty_asset_spec_agg.square_footage                               AS square_footage,
    asset_detail_agg.primary_heating_type                            AS primary_heating_type,
    asset_detail_agg.primary_heating_desc                            AS primary_heating_desc,
    prty_asset_spec_agg.pool_hot_tub_ind                             AS pool_hot_tub_ind,
    prty_asset_spec_agg.liability_exp_ind                            AS liability_exp_ind,
    prty_asset_spec_agg.miles_to_fire_dept                           AS miles_to_fire_dept_above_threshold,
    prty_asset_spec_agg.feet_to_fire_hyd                             AS feet_to_fire_hyd_above_threshold,
    fd.fire_dept_cd                                                  AS fire_dept_cd,
    CASE WHEN asset_detail_agg.has_secondary_res = 1 THEN ''1'' ELSE ''0'' END AS sec_res_ind,
    prty_asset_spec_agg.nbr_stories                                  AS nbr_stories,
    plcy_mail_address.plcy_mail_address_1                            AS plcy_mail_address_1,
    plcy_mail_address.plcy_mail_address_2                            AS plcy_mail_address_2,
    plcy_mail_address.city                                            AS plcy_mail_city,
    plcy_mail_address.state                                           AS plcy_mail_state,
    plcy_mail_address.zip                                             AS plcy_mail_zip,
    risk_state.geogrcl_area_shrt_name                                AS plcy_risk_state,
    risk_cnty.geogrcl_area_shrt_name                                 AS plcy_risk_county,
    location_address.zip                                              AS prop_zip_cd,
    agmt_discounts_agg.nwhm_pct                                      AS nwhm_pct,
    agmt_discounts_agg.auto_dsc_ind                                  AS auto_dsc_ind,
    agmt_discounts_agg.hapc_pct                                      AS hapc_pct,
    agmt_discounts_agg.vclp_pct                                      AS vclp_pct,
    agmt_discounts_agg.sprnklr_discount_pct                          AS sprnklr_discount_pct,
    agmt_discounts_agg.clm_free_ind                                  AS clm_free_ind,
    prty_asset_spec_prot.protection_class                            AS protection_class,
    asset_detail_agg.fire_prot_service                               AS fire_prot_service,
    asset_detail_agg.nbr_families                                    AS nbr_families,
    location_address.plcy_mail_address_1                             AS loc_desc,
    location_address.county                                           AS loc_cnty,
    real_estat.cnstrctn_dt                                            AS cnstrctn_dt,
    cnstrctn_type_lkp.cnstrctn_type_desc                             AS construction_type,
    asset_detail_agg.substructure                                     AS substructure,
    real_estat_dtl.roof_yr                                            AS roof_yr,
    mortgagee_name.indiv_full_name                                   AS mortgagee_name,
    encumbrance_agg.encumbrance_desc                                 AS encumbrance_cd,
    agmt.bilg_meth_type_cd                                           AS bilg_meth_type_cd,
    prty_asset_spec_agg.manufacturer                                 AS manufacturer,
    prty_asset_spec_agg.mh_underpinning_credit                       AS mh_underpinning_credit,
    mh_park.mfg_home_prk_name                                        AS mh_park_name,
    COALESCE(indiv_name_stg.gvn_name, priinsured_company_name.org_name) AS insured_first_nm,
    indiv_name_stg.fmly_name                                         AS insured_last_nm,
    priinsured_details.ssn_tax_num                                   AS insured_ssn,
    priinsured_details.birth_dt                                      AS insured_dob,
    priinsured_details.gndr_type_cd                                  AS insured_gender,
    indiv_mrtl_sts_agg.mrtl_sts_cd                                   AS insured_marital_status,
    email_address.elctrnc_addr_txt                                   AS insured_email_address,
    priinsured_occup.ocptn_type_cd                                   AS insured_occupation,
    priinsured_occup.ocptn_type_desc                                 AS insured_occupation_desc,
    prev_carrier.org_name                                             AS prior_carrier,
    ''Available in R1.X''                                               AS years_with_prior,
    plcy_rated_score.rtd_insrnc_scr_val                              AS plcy_rated_ins_score,
    psl1.prty_scr_val                                                AS primary_insured_ins_score,
    psl2.prty_scr_val                                                AS secondary_insured_ins_score,
    addinsured_agg.non_occ_deed_owner                                AS non_occ_deed_owner,
    CASE WHEN endorsement_zone_agg.endorsement = ''0.0200'' THEN ''Y'' ELSE ''N'' END AS end_054,
    CASE WHEN endorsement_zone_agg.endorsement = ''0.0200'' THEN endorsement_zone_agg.zon ELSE NULL END AS end_054_zone,
    CASE WHEN endorsement_zone_agg.endorsement = ''0.0500'' THEN ''Y'' ELSE ''N'' END AS end_055,
    CASE WHEN endorsement_zone_agg.endorsement = ''0.0500'' THEN endorsement_zone_agg.zon ELSE NULL END AS end_055_zone,
    CASE WHEN endorsement_zone_agg.endorsement = ''0.1000'' THEN ''Y'' ELSE ''N'' END AS end_056,
    CASE WHEN endorsement_zone_agg.endorsement = ''0.1000'' THEN endorsement_zone_agg.zon ELSE NULL END AS end_056_zone,
    (CURRENT_DATE - CAST(agmt.cntnus_srvc_dttm AS DATE)) / 365     AS years_with_alfa,
    ROW_NUMBER() OVER (ORDER BY agmt.agmt_id)                     AS source_record_id
FROM db_t_prod_core.agmt agmt

LEFT JOIN db_t_prod_core.prty_agmt priinsured_prty
  ON agmt.agmt_id = priinsured_prty.agmt_id
 AND priinsured_prty.prty_agmt_role_cd = ''PLCYPRININS''
 AND priinsured_prty.trans_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')

LEFT JOIN db_t_prod_core.prty_agmt secinsured_prty
  ON agmt.agmt_id = secinsured_prty.agmt_id
 AND secinsured_prty.prty_agmt_role_cd = ''PLCYSECNINS''
 AND secinsured_prty.trans_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')

LEFT JOIN agmt_asset_main asset
  ON asset.agmt_id = agmt.agmt_id

LEFT JOIN db_t_prod_core.real_estat
  ON asset.prty_asset_id = real_estat.prty_asset_id
 AND real_estat.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')

LEFT JOIN db_t_prod_core.real_estat_dtl
  ON asset.prty_asset_id = real_estat_dtl.prty_asset_id
 AND real_estat_dtl.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')

LEFT JOIN plcy_mail_address
  ON priinsured_prty.prty_id = plcy_mail_address.prty_id

LEFT JOIN location_address
  ON asset.prty_asset_id = location_address.prty_asset_id

LEFT JOIN indiv_name_stg
  ON priinsured_prty.prty_id = indiv_name_stg.indiv_prty_id

LEFT JOIN db_t_prod_core.org_name priinsured_company_name
  ON priinsured_company_name.prty_id = priinsured_prty.prty_id
 AND priinsured_company_name.name_type_cd = ''DBA''
 AND priinsured_company_name.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')

LEFT JOIN db_t_prod_core.indiv priinsured_details
  ON priinsured_details.indiv_prty_id = priinsured_prty.prty_id
 AND priinsured_details.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')

LEFT JOIN (
    SELECT indiv_ocptn.indiv_prty_id,
           MAX(indiv_ocptn.ocptn_type_cd)  AS ocptn_type_cd,
           MAX(ocptn_type.ocptn_type_desc) AS ocptn_type_desc
    FROM db_t_prod_core.indiv_ocptn
    JOIN db_t_prod_core.ocptn_type
      ON indiv_ocptn.ocptn_type_cd = ocptn_type.ocptn_type_cd
    WHERE indiv_ocptn.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
    GROUP BY indiv_ocptn.indiv_prty_id
) priinsured_occup
  ON priinsured_occup.indiv_prty_id = priinsured_prty.prty_id

LEFT JOIN db_t_prod_core.quotn_agmt
  ON quotn_agmt.agmt_id = agmt.agmt_id
 AND quotn_agmt.trans_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')

LEFT JOIN db_t_prod_core.insrnc_quotn
  ON insrnc_quotn.quotn_id = quotn_agmt.quotn_id
 AND insrnc_quotn.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')

LEFT JOIN db_t_prod_core.aplctn
  ON aplctn.aplctn_id = insrnc_quotn.aplctn_id
 AND aplctn.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')

JOIN db_t_prod_core.agmt_prod
  ON agmt.agmt_id = agmt_prod.agmt_id
 AND agmt_prod.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')

JOIN db_t_prod_core.prod
  ON agmt_prod.prod_id = prod.prod_id
 AND prod.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')

LEFT JOIN endorsement_zone_agg
  ON endorsement_zone_agg.agmt_id = agmt.agmt_id

LEFT JOIN membership_agg
  ON membership_agg.agmt_id = agmt.agmt_id

LEFT JOIN underwriting
  ON underwriting.agmt_id = agmt.agmt_id

LEFT JOIN agent
  ON agent.agmt_id = agmt.agmt_id

LEFT JOIN service_center
  ON service_center.agmt_id = agmt.agmt_id

LEFT JOIN prty_asset_spec_agg
  ON prty_asset_spec_agg.prty_asset_id = asset.prty_asset_id

LEFT JOIN asset_detail_agg
  ON asset_detail_agg.prty_asset_id = asset.prty_asset_id

LEFT JOIN prty_asset_spec_prot
  ON prty_asset_spec_prot.prty_asset_id = asset.prty_asset_id

LEFT JOIN mortgagee_name
  ON mortgagee_name.prty_asset_id = asset.prty_asset_id

LEFT JOIN mh_park
  ON mh_park.prty_asset_id = asset.prty_asset_id

LEFT JOIN agmt_discounts_agg
  ON agmt_discounts_agg.agmt_id = agmt.agmt_id

LEFT JOIN prty_asset_spec_staging pas_spec
  ON pas_spec.prty_asset_id = asset.prty_asset_id

LEFT JOIN encumbrance_agg
  ON encumbrance_agg.prty_asset_id = asset.prty_asset_id

LEFT JOIN prev_carrier
  ON prev_carrier.prty_id = priinsured_prty.prty_id

LEFT JOIN email_address
  ON email_address.prty_id = priinsured_prty.prty_id

LEFT JOIN plcy_rated_score
  ON plcy_rated_score.agmt_id = agmt.agmt_id

LEFT JOIN prty_score_lexis psl1
  ON psl1.prty_id = priinsured_prty.prty_id

LEFT JOIN prty_score_lexis psl2
  ON psl2.prty_id = secinsured_prty.prty_id

LEFT JOIN addinsured_agg
  ON addinsured_agg.agmt_id = agmt.agmt_id

LEFT JOIN ev_rewrite_agg
  ON ev_rewrite_agg.agmt_id = agmt.agmt_id

LEFT JOIN db_t_prod_core.fire_dept fd
  ON fd.fire_dept_id = location_address.fire_dept_id
 AND fd.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')

LEFT JOIN indiv_mrtl_sts_agg
  ON indiv_mrtl_sts_agg.indiv_prty_id = priinsured_prty.prty_id

LEFT JOIN risk_state
  ON risk_state.agmt_id = agmt.agmt_id

LEFT JOIN risk_cnty
  ON risk_cnty.agmt_id = agmt.agmt_id

LEFT JOIN cnstrctn_type_lkp
  ON cnstrctn_type_lkp.cnstrctn_type_cd = real_estat.cnstrctn_type_cd

JOIN agmt_max_modl agm
  ON agm.host_agmt_num = agmt.host_agmt_num
 AND agm.max_modl_crtn_dttm = agmt.modl_crtn_dttm

WHERE agmt.agmt_type_cd       IN ( ''PPV'' )
  AND prod.insrnc_lob_type_cd IN ( ''HO'', ''MH'', ''SF'' )
  AND agmt.edw_end_dttm = TO_TIMESTAMP_NTZ(''12/31/9999 23:59:59.999999'',''MM/DD/YYYY HH24:MI:SS.FF6'')
;


-- Component LKP_NBR_REINST, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_NBR_REINST AS
(
SELECT
LKP.CNT,
SQ_HO_EMBLEM_PLCY_STG.source_record_id,
ROW_NUMBER() OVER(PARTITION BY SQ_HO_EMBLEM_PLCY_STG.source_record_id ORDER BY LKP.CNT asc,LKP.HOST_AGMT_NUM asc) RNK
FROM
SQ_HO_EMBLEM_PLCY_STG
LEFT JOIN (
select count(1) as cnt,AGMT.HOST_AGMT_NUM AS HOST_AGMT_NUM from  db_t_prod_core.AGMT join db_t_prod_core.EV
on EV.AGMT_ID=AGMT.AGMT_ID
/* join  DB_T_BIDEV_WRK.HO_EMBLEM_PLCY_STG  on HO_EMBLEM_PLCY_STG.PLCY_NBR=AGMT.host_agmt_num */
where AGMT_TYPE_CD=''POL''
and EV.EV_ACTVY_TYPE_CD=''REINSTATE''
AND  AGMT.EDW_END_DTTM =''9999-12-31 23:59:59.999999''
AND EV.EDW_END_DTTM =''9999-12-31 23:59:59.999999''
/* and  TO_DATE(AGMT.TRANS_END_DTTM, ''MM-DD-YYYY'') = ''12-31-9999'' */
/* and  TO_DATE(EV.TRANS_END_DTTM, ''MM-DD-YYYY'') = ''12-31-9999'' */
group by AGMT.HOST_AGMT_NUM
) LKP ON LKP.HOST_AGMT_NUM = SQ_HO_EMBLEM_PLCY_STG.PLCY_NBR
QUALIFY RNK = 1
);


-- Component LKP_APLCTN_RSPNS_IND, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_APLCTN_RSPNS_IND AS
(
SELECT
LKP.APLCTN_RSPNS_IND,
LKP.APLCTN_ID,
SQ_HO_EMBLEM_PLCY_STG.source_record_id,
ROW_NUMBER() OVER(PARTITION BY SQ_HO_EMBLEM_PLCY_STG.source_record_id ORDER BY LKP.APLCTN_RSPNS_IND asc,LKP.APLCTN_ID asc) RNK
FROM
SQ_HO_EMBLEM_PLCY_STG
LEFT JOIN (
select APLCTN_RSPNS_IND as APLCTN_RSPNS_IND,APLCTN_QUES_RSPNS.APLCTN_ID as APLCTN_ID from  db_t_prod_core.APLCTN_QUES_RSPNS 
where APLCTN_QUES_RSPNS.APLCTN_FRM_QUES_NUM=''HODwellingConstructionAddlHasSolidFuelHeater''
AND APLCTN_QUES_RSPNS.EDW_END_DTTM =''9999-12-31 23:59:59.999999'' 
AND  to_char(TRANS_END_DTTM, ''MM-DD-YYYY'') = ''12-31-9999''
) LKP ON LKP.APLCTN_ID = SQ_HO_EMBLEM_PLCY_STG.APLCTN_ID
QUALIFY RNK = 1
);


-- Component LKP_ASSET_DTL_CD_XREF, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_ASSET_DTL_CD_XREF AS
(
SELECT
LKP.PRTY_ASSET_ID,
LKP.ASSET_DTL_TXT,
SQ_HO_EMBLEM_PLCY_STG.source_record_id,
ROW_NUMBER() OVER(PARTITION BY SQ_HO_EMBLEM_PLCY_STG.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.ASSET_DTL_TXT asc) RNK
FROM
SQ_HO_EMBLEM_PLCY_STG
LEFT JOIN (
SELECT ASSET_DTL_CD_XREF.ASSET_DTL_TXT as ASSET_DTL_TXT, ASSET_DTL_CD_XREF.PRTY_ASSET_ID as PRTY_ASSET_ID FROM db_t_prod_core.ASSET_DTL_CD_XREF where ASSET_DTL_CD_XREF.ASSET_DTL_CD IN (''TC'', ''HOTC'') and 
 EDW_END_DTTM =''9999-12-31 23:59:59.999999''
) LKP ON LKP.PRTY_ASSET_ID = SQ_HO_EMBLEM_PLCY_STG.PRTY_ASSET_ID
QUALIFY RNK = 1
);


-- Component LKP_NBR_OUTBLDGS, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_NBR_OUTBLDGS AS
(
SELECT
LKP.PRTY_ASSET_ID,
LKP.AGMT_ID,
LKP.NBR_OUTBLDGS,
SQ_HO_EMBLEM_PLCY_STG.source_record_id,
ROW_NUMBER() OVER(PARTITION BY SQ_HO_EMBLEM_PLCY_STG.source_record_id ORDER BY LKP.PRTY_ASSET_ID asc,LKP.AGMT_ID asc,LKP.NBR_OUTBLDGS asc) RNK
FROM
SQ_HO_EMBLEM_PLCY_STG
LEFT JOIN (
SELECT  count(1) as NBR_OUTBLDGS, count(PRTY_ASSET.PRTY_ASSET_ID) as PRTY_ASSET_ID, agmt_id as AGMT_ID
   from db_t_prod_core.PRTY_ASSET
   join db_t_prod_core.agmt_asset ON PRTY_ASSET.PRTY_ASSET_ID = agmt_asset.PRTY_ASSET_ID
   where PRTY_ASSET_CLASFCN_CD = ''PRVOB''
   and PRTY_ASSET.EDW_END_DTTM =''9999-12-31 23:59:59.999999''
   GROUP BY AGMT_ID
) LKP ON LKP.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID
QUALIFY RNK = 1
);


-- Component LKP_EV_FINCL_EV, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_EV_FINCL_EV AS
(
SELECT
LKP.DATE_STRING,
LKP.AGMT_ID,
SQ_HO_EMBLEM_PLCY_STG.source_record_id,
ROW_NUMBER() OVER(PARTITION BY SQ_HO_EMBLEM_PLCY_STG.source_record_id ORDER BY LKP.DATE_STRING asc,LKP.AGMT_ID asc) RNK
FROM
SQ_HO_EMBLEM_PLCY_STG
LEFT JOIN (
  SELECT 
      date_string AS date_string,
      agmt_id AS agmt_id
  FROM (
      SELECT 
          CAST(FINCL_EV.ACCNTG_MTH_NUM || ''/'' || FINCL_EV.ACCNTG_DY_NUM || ''/'' || FINCL_EV.ACCNTG_YR_NUM AS VARCHAR(100)) AS date_string,
          EV.AGMT_ID AS AGMT_ID,
          ROW_NUMBER() OVER (PARTITION BY EV.AGMT_ID ORDER BY EV.AGMT_ID NULLS LAST) AS r
      FROM DB_T_PROD_CORE.EV EV
      JOIN DB_T_PROD_CORE.FINCL_EV FINCL_EV
          ON EV.EV_ID = FINCL_EV.EV_ID
      WHERE EV.EDW_END_DTTM = TO_TIMESTAMP_NTZ(''9999-12-31 23:59:59.999999'', ''YYYY-MM-DD HH24:MI:SS.FF6'')
        AND FINCL_EV.EDW_END_DTTM = TO_TIMESTAMP_NTZ(''9999-12-31 23:59:59.999999'', ''YYYY-MM-DD HH24:MI:SS.FF6'')
  ) x
  WHERE r = 1
) LKP ON LKP.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID
QUALIFY RNK = 1
);


-- Component LKP_APLCTN_RSPNS_LIAB_EXPSR, Type LOOKUP 
CREATE OR REPLACE TEMPORARY TABLE LKP_APLCTN_RSPNS_LIAB_EXPSR AS
(
SELECT
LKP.APLCTN_RSPNS_IND,
LKP.APLCTN_ID,
SQ_HO_EMBLEM_PLCY_STG.source_record_id,
ROW_NUMBER() OVER(PARTITION BY SQ_HO_EMBLEM_PLCY_STG.source_record_id ORDER BY LKP.APLCTN_RSPNS_IND asc,LKP.APLCTN_ID asc) RNK
FROM
SQ_HO_EMBLEM_PLCY_STG
LEFT JOIN (
SELECT APLCTN_QUES_RSPNS.APLCTN_RSPNS_IND as APLCTN_RSPNS_IND, APLCTN_QUES_RSPNS.APLCTN_ID as APLCTN_ID 
FROM db_t_prod_core.APLCTN_QUES_RSPNS 
where APLCTN_QUES_RSPNS.APLCTN_FRM_QUES_NUM IN (''HOGenDangerousBreedExists_alfa'',
''HOGenDangerousAnimalsExist_alfa'',
''HOGenStepsAndRails_alfa'')
and APLCTN_QUES_RSPNS.APLCTN_RSPNS_IND=''1''
AND APLCTN_QUES_RSPNS.EDW_END_DTTM =''9999-12-31 23:59:59.999999''
AND to_char(TRANS_END_DTTM, ''MM-DD-YYYY'') = ''12-31-9999''
) LKP ON LKP.APLCTN_ID = SQ_HO_EMBLEM_PLCY_STG.APLCTN_ID
QUALIFY RNK = 1
);


-- Component EXP_DATA_TRANS, Type EXPRESSION 
CREATE OR REPLACE TEMPORARY TABLE EXP_DATA_TRANS AS
(
SELECT
CASE WHEN DATE_PART(''MM'', TO_TIMESTAMP(SQ_HO_EMBLEM_PLCY_STG.EOM_DATE)) > 9 THEN CONCAT ( ( DATE_PART(''YYYY'', TO_TIMESTAMP(CURRENT_TIMESTAMP)) ) , ( DATE_PART(''MM'', TO_TIMESTAMP(CURRENT_TIMESTAMP)) ) ) ELSE DATE_PART(''YYYY'', TO_TIMESTAMP(CURRENT_TIMESTAMP)) || ''0'' || DATE_PART(''MM'', TO_TIMESTAMP(CURRENT_TIMESTAMP)) END as MO_ID,
SQ_HO_EMBLEM_PLCY_STG.AGMT_ID as AGMT_ID,
SQ_HO_EMBLEM_PLCY_STG.PLCY_NBR as PLCY_NBR,
TO_DATE ( LKP_EV_FINCL_EV.DATE_STRING , ''MM/DD/YYYY'' ) as out_DATE_STRING,
SQ_HO_EMBLEM_PLCY_STG.TERM_EFF_DT as TERM_EFF_DT,
SQ_HO_EMBLEM_PLCY_STG.PLCY_EXP_DT as PLCY_EXP_DT,
SQ_HO_EMBLEM_PLCY_STG.PLCY_MAIL_STATE as PLCY_MAIL_STATE,
SQ_HO_EMBLEM_PLCY_STG.PLCY_RISK_COUNTY as PLCY_RISK_COUNTY,
LKP_ASSET_DTL_CD_XREF.ASSET_DTL_TXT as ASSET_DTL_TXT,
SQ_HO_EMBLEM_PLCY_STG.PLCY_MAIL_ZIP as PLCY_MAIL_ZIP,
SQ_HO_EMBLEM_PLCY_STG.PLCY_COMPANY as PLCY_COMPANY,
SQ_HO_EMBLEM_PLCY_STG.MEMBER_NUMBER as MEMBER_NUMBER,
SQ_HO_EMBLEM_PLCY_STG.MEMBER_TYPE as MEMBER_TYPE,
LKP_1.VAL /* replaced lookup LKP_AGMT_COVERAGES_TERMPATTERN_CODE */ as DWELL_COV,
LKP_2.VAL /* replaced lookup LKP_AGMT_COVERAGES_TERMPATTERN_CODE */ as PERS_PROP_COV,
LKP_3.VAL /* replaced lookup LKP_AGMT_COVERAGES_TERMPATTERN_CODE */ as PER_LIAB_LIM,
LKP_4.VAL /* replaced lookup LKP_AGMT_COVERAGES_TERMPATTERN_CODE */ as MEDICAL_COV,
IFNULL(TRY_TO_DECIMAL(LKP_5.VAL /* replaced lookup LKP_AGMT_COVERAGES_TERMPATTERN_CODE */), 0) as APP_STRUCT_COV,
IFNULL(TRY_TO_DECIMAL(LKP_6.VAL /* replaced lookup LKP_AGMT_COVERAGES_TERMPATTERN_CODE */), 0) as LIVING_EXP_COV,
CASE WHEN REPLACE(REGEXP_REPLACE(SQ_HO_EMBLEM_PLCY_STG.PLCY_LOB,''[^0-9]'',''/''),''/'',chr ( 0 )) = '''' THEN NULL ELSE REPLACE(REGEXP_REPLACE(SQ_HO_EMBLEM_PLCY_STG.PLCY_LOB,''[^0-9]'',''/''),''/'',chr ( 0 )) END as FORM,
SQ_HO_EMBLEM_PLCY_STG.PROTECTION_CLASS as PROTECTION_CLASS,
CASE WHEN UPPER ( SQ_HO_EMBLEM_PLCY_STG.ENCUMBRANCE_CD ) = ''MORTGAGEE'' AND SQ_HO_EMBLEM_PLCY_STG.BILG_METH_TYPE_CD = ''DIRECT'' THEN 2 ELSE CASE WHEN UPPER ( SQ_HO_EMBLEM_PLCY_STG.ENCUMBRANCE_CD ) = ''MORTGAGEE'' AND SQ_HO_EMBLEM_PLCY_STG.BILG_METH_TYPE_CD = ''LIST'' THEN 3 ELSE CASE WHEN UPPER ( SQ_HO_EMBLEM_PLCY_STG.ENCUMBRANCE_CD ) <> ''MORTGAGEE'' THEN 1 ELSE /*$3*/ NULL END END END as out_ENCUMBRANCE_CD,
SQ_HO_EMBLEM_PLCY_STG.CONSTRUCTION_TYPE as CONSTRUCTION_TYPE,
DECODE ( UPPER ( SQ_HO_EMBLEM_PLCY_STG.FIRE_PROT_SERVICE ) , ''CITY'' , ''C'' , ''FIRE'' , ''F'' , ''NONE'' , ''N'' , ''PROT'' , ''P'' , ''CERTCLASS10'' , ''T'' , ''OUTSIDECITY'' , ''O'' ) as out_FIRE_PROT_SERVICE,
DATE_PART(''YY'', TO_TIMESTAMP(SQ_HO_EMBLEM_PLCY_STG.CNSTRCTN_DT)) as out_CNSTRCTN_DT,
SQ_HO_EMBLEM_PLCY_STG.NBR_FAMILIES as NBR_FAMILIES,
SQ_HO_EMBLEM_PLCY_STG.ROOF_YR as ROOF_YR,
CASE WHEN ( LTRIM ( RTRIM ( SQ_HO_EMBLEM_PLCY_STG.SEC_RES_IND ) ) = ''1'' ) THEN ''Y'' ELSE ''N'' END as SEC_RES_IND_out,
( DATE_PART(''YY'', TO_TIMESTAMP(CURRENT_TIMESTAMP)) ) - ( DATE_PART(''YY'', TO_TIMESTAMP(SQ_HO_EMBLEM_PLCY_STG.INSURED_DOB)) ) as AGE,
SQ_HO_EMBLEM_PLCY_STG.INSURED_GENDER as APP_GENDER,
SQ_HO_EMBLEM_PLCY_STG.INSURED_MARITAL_STATUS as INSURED_MARITAL_STATUS,
SQ_HO_EMBLEM_PLCY_STG.INSURED_OCCUPATION as INSURED_OCCUPATION,
SQ_HO_EMBLEM_PLCY_STG.ORIGINAL_ICP_DT as ORIGINAL_ICP_DT,
SQ_HO_EMBLEM_PLCY_STG.PLCY_EXP_DT as PLCY_EXP_DT1,
CASE WHEN LTRIM ( RTRIM ( SQ_HO_EMBLEM_PLCY_STG.MILES_TO_FIRE_DEPT_ABOVE_THRESHOLD ) ) = ''1'' THEN ''Y'' ELSE ''N'' END as MILES_TO_FIRE_DEPT_out,
SQ_HO_EMBLEM_PLCY_STG.FEET_TO_FIRE_HYD_ABOVE_THRESHOLD as FEET_TO_FIRE_HYD_ABOVE_THRESHO,
SQ_HO_EMBLEM_PLCY_STG.SQUARE_FOOTAGE as SQUARE_FOOTAGE,
SQ_HO_EMBLEM_PLCY_STG.SUBSTRUCTURE as SUBSTRUCTURE,
CASE WHEN To_Char ( RLIKE(UPPER ( SQ_HO_EMBLEM_PLCY_STG.PRIMARY_HEATING_DESC ),''.*WALL.*'') ) = ''1'' THEN ''Y'' ELSE ''N'' END as WALL_HTRS,
CASE WHEN To_Char ( RLIKE(UPPER ( SQ_HO_EMBLEM_PLCY_STG.PRIMARY_HEATING_DESC ),''.*SPACE.*'') ) = ''1'' THEN ''Y'' ELSE ''N'' END as SPACE_HTRS,
CASE WHEN ( SQ_HO_EMBLEM_PLCY_STG.POOL_HOT_TUB_IND = ''1'' ) THEN ''Y'' ELSE ''N'' END as POOL_HOT_TUB_IND_OUT,
CASE WHEN LKP_APLCTN_RSPNS_LIAB_EXPSR.APLCTN_RSPNS_IND IS NULL THEN ''N'' ELSE ''Y'' END as out_LIAB_EXPOSURE,
SQ_HO_EMBLEM_PLCY_STG.NBR_STORIES as NBR_STORIES,
SQ_HO_EMBLEM_PLCY_STG.YEARS_WITH_ALFA as YEARS_WITH_PRIOR,
CASE WHEN ( SQ_HO_EMBLEM_PLCY_STG.CLM_FREE_IND = 1 ) THEN ''Y'' ELSE ''N'' END as CLM_FREE_IND_out,
CASE WHEN ( SQ_HO_EMBLEM_PLCY_STG.AUTO_DSC_IND = 1 ) THEN ''Y'' ELSE ''N'' END as AUTO_DSC_IND_out,
SQ_HO_EMBLEM_PLCY_STG.NWHM_PCT as NWHM_PCT,
SQ_HO_EMBLEM_PLCY_STG.HAPC_PCT as HAPC_PCT,
SQ_HO_EMBLEM_PLCY_STG.VCLP_PCT as VCLP_PCT,
SQ_HO_EMBLEM_PLCY_STG.SPRNKLR_DISCOUNT_PCT as SPRNKLR_DISCOUNT_PCT,
SQ_HO_EMBLEM_PLCY_STG.QUOTN_ID as QUOTN_ID,
SQ_HO_EMBLEM_PLCY_STG.APLCTN_ID as APLCTN_ID,
SQ_HO_EMBLEM_PLCY_STG.PLCY_SYSTEM as PLCY_SYSTEM,
SQ_HO_EMBLEM_PLCY_STG.POLICY_TYPE as POLICY_TYPE,
SQ_HO_EMBLEM_PLCY_STG.PERIOD_EFF_DT as PERIOD_EFF_DT,
SQ_HO_EMBLEM_PLCY_STG.REWRITE_DT as REWRITE_DT,
SQ_HO_EMBLEM_PLCY_STG.AGENT_NBR as AGENT_NBR,
SQ_HO_EMBLEM_PLCY_STG.SVC_NBR as SVC_NBR,
SQ_HO_EMBLEM_PLCY_STG.PRIMARY_HEATING_TYPE as PRIMARY_HEATING_TYPE,
SQ_HO_EMBLEM_PLCY_STG.LIABILITY_EXP_IND as LIABILITY_EXP_IND,
SQ_HO_EMBLEM_PLCY_STG.FIRE_DEPT_CD as FIRE_DEPT_CD,
SQ_HO_EMBLEM_PLCY_STG.PLCY_MAIL_ADDRESS_1 as PLCY_MAIL_ADDRESS_1,
SQ_HO_EMBLEM_PLCY_STG.PLCY_MAIL_ADDRESS_2 as PLCY_MAIL_ADDRESS_2,
SQ_HO_EMBLEM_PLCY_STG.PLCY_MAIL_CITY as PLCY_MAIL_CITY,
SQ_HO_EMBLEM_PLCY_STG.PLCY_RISK_STATE as PLCY_RISK_STATE,
SQ_HO_EMBLEM_PLCY_STG.PROP_ZIP_CD as PROP_ZIP_CD,
SQ_HO_EMBLEM_PLCY_STG.LOC_DESC as LOC_DESC,
SQ_HO_EMBLEM_PLCY_STG.LOC_CNTY as LOC_CNTY,
SQ_HO_EMBLEM_PLCY_STG.MORTGAGEE_NAME as MORTGAGEE_NAME,
SQ_HO_EMBLEM_PLCY_STG.MANUFACTURER as MANUFACTURER,
SQ_HO_EMBLEM_PLCY_STG.MH_UNDERPINNING_CREDIT as MH_UNDERPINNING_CREDIT,
SQ_HO_EMBLEM_PLCY_STG.MH_PARK_NAME as MH_PARK_NAME,
SQ_HO_EMBLEM_PLCY_STG.INSURED_FIRST_NM as INSURED_FIRST_NM,
SQ_HO_EMBLEM_PLCY_STG.INSURED_LAST_NM as INSURED_LAST_NM,
SQ_HO_EMBLEM_PLCY_STG.INSURED_SSN as INSURED_SSN,
SQ_HO_EMBLEM_PLCY_STG.INSURED_EMAIL_ADDRESS as INSURED_EMAIL_ADDRESS,
SQ_HO_EMBLEM_PLCY_STG.INSURED_OCCUPATION_DESC as INSURED_OCCUPATION_DESC,
SQ_HO_EMBLEM_PLCY_STG.PRIOR_CARRIER as PRIOR_CARRIER,
SQ_HO_EMBLEM_PLCY_STG.PLCY_RATED_INS_SCORE as PLCY_RATED_INS_SCORE,
SQ_HO_EMBLEM_PLCY_STG.PRIMARY_INSURED_INS_SCORE as PRIMARY_INSURED_INS_SCORE,
SQ_HO_EMBLEM_PLCY_STG.SECONDARY_INSURED_INS_SCORE as SECONDARY_INSURED_INS_SCORE,
SQ_HO_EMBLEM_PLCY_STG.NON_OCC_DEED_OWNER as NON_OCC_DEED_OWNER,
:PRCS_ID as PRCS_ID,
CASE WHEN LKP_7.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_001,
CASE WHEN LKP_8.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_002,
CASE WHEN LKP_9.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_003,
LKP_10.VAL /* replaced lookup LKP_AGMT_COVERAGES_TERM_COV_CODE */ as v_END,
CASE WHEN SQ_HO_EMBLEM_PLCY_STG.PLCY_LOB = ''HO3'' or SQ_HO_EMBLEM_PLCY_STG.PLCY_LOB = ''HO5'' THEN v_END ELSE /*$3*/ NULL END as v_END_006,
CASE WHEN v_END_006 = 0.1 THEN ''Y'' ELSE ''N'' END as o_END_006,
CASE WHEN SQ_HO_EMBLEM_PLCY_STG.PLCY_LOB = ''HO4'' or SQ_HO_EMBLEM_PLCY_STG.PLCY_LOB = ''HO6'' THEN v_END ELSE /*$3*/ NULL END as v_END_007,
CASE WHEN v_END_007 = 0.1 THEN ''Y'' ELSE ''N'' END as o_END_007,
CASE WHEN SQ_HO_EMBLEM_PLCY_STG.PLCY_LOB = ''HO3'' or SQ_HO_EMBLEM_PLCY_STG.PLCY_LOB = ''HO5'' THEN v_END ELSE /*$3*/ NULL END as v_END_008,
CASE WHEN v_END_008 = 0.05 THEN ''Y'' ELSE ''N'' END as o_END_008,
CASE WHEN SQ_HO_EMBLEM_PLCY_STG.PLCY_LOB = ''HO4'' or SQ_HO_EMBLEM_PLCY_STG.PLCY_LOB = ''HO6'' THEN v_END ELSE /*$3*/ NULL END as v_END_009,
CASE WHEN v_END_009 = 0.05 THEN ''Y'' ELSE ''N'' END as o_END_009,
CASE WHEN SQ_HO_EMBLEM_PLCY_STG.PLCY_LOB = ''HO3'' or SQ_HO_EMBLEM_PLCY_STG.PLCY_LOB = ''HO5'' THEN v_END ELSE /*$3*/ NULL END as v_END_010,
CASE WHEN v_END_009 = 0.02 THEN ''Y'' ELSE ''N'' END as o_END_010,
CASE WHEN SQ_HO_EMBLEM_PLCY_STG.PLCY_LOB = ''HO4'' or SQ_HO_EMBLEM_PLCY_STG.PLCY_LOB = ''HO6'' THEN v_END ELSE /*$3*/ NULL END as v_END_011,
CASE WHEN v_END_009 = 0.02 THEN ''Y'' ELSE ''N'' END as o_END_011,
CURRENT_TIMESTAMP as LOAD_DT,
CASE WHEN LKP_11.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_013,
CASE WHEN LKP_12.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_014,
CASE WHEN LKP_13.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_015,
CASE WHEN LKP_14.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_017,
CASE WHEN LKP_15.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_019,
CASE WHEN LKP_16.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_023,
CASE WHEN LKP_17.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NOT NULL OR LKP_18.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NOT NULL THEN ''Y'' ELSE ''N'' END as END_028,
CASE WHEN LKP_19.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_033,
CASE WHEN LKP_20.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_035,
CASE WHEN LKP_21.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_040,
CASE WHEN ADDINSURED_PRTY_ID IS NOT NULL AND SQ_HO_EMBLEM_PLCY_STG.NON_OCC_DEED_OWNER IS NOT NULL THEN ''Y'' ELSE ''N'' END as END_041,
CASE WHEN LKP_22.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_042,
CASE WHEN LKP_23.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_045,
CASE WHEN LKP_24.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_046,
CASE WHEN LKP_25.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_048,
CASE WHEN LKP_26.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_049,
CASE WHEN LKP_27.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_050,
CASE WHEN LKP_28.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_052,
CASE WHEN LKP_29.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_053,
CASE WHEN LKP_30.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN ''N'' ELSE ''Y'' END as END_061,
CASE WHEN LKP_31.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NULL THEN NULL ELSE ''Scheduled Personal Property Item'' END as END61_000_DESC,
LKP_32.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ as v_Scheduled,
CASE WHEN v_Scheduled = ''JEWELRY'' THEN ''Y'' ELSE ''N'' END as END61_001,
CASE WHEN v_Scheduled = ''JEWELRY'' THEN ''JEWELRY'' ELSE NULL END as END61_001_DESC,
CASE WHEN v_Scheduled = ''FURS'' THEN ''Y'' ELSE ''N'' END as END61_002,
CASE WHEN v_Scheduled = ''FURS'' THEN ''FURS'' ELSE NULL END as END61_002_DESC,
CASE WHEN v_Scheduled = ''CAMERAS'' THEN ''Y'' ELSE ''N'' END as END61_003,
CASE WHEN v_Scheduled = ''CAMERAS'' THEN ''CAMERAS'' ELSE NULL END as END61_003_DESC,
CASE WHEN v_Scheduled = ''MUSICAL INSTRUMENTS'' THEN ''Y'' ELSE ''N'' END as END61_004,
CASE WHEN v_Scheduled = ''MUSICAL INSTRUMENTS'' THEN ''MUSICAL INSTRUMENTS'' ELSE NULL END as END61_004_DESC,
CASE WHEN v_Scheduled = ''SILVERWARE'' THEN ''Y'' ELSE ''N'' END as END61_005,
CASE WHEN v_Scheduled = ''SILVERWARE'' THEN ''SILVERWARE'' ELSE NULL END as END61_005_DESC,
CASE WHEN v_Scheduled = ''GOLFER'' || CHR ( 39 ) || ''S EQUIPMENT'' THEN ''Y'' ELSE ''N'' END as END61_006,
CASE WHEN v_Scheduled = ''GOLFER'' || CHR ( 39 ) || ''S EQUIPMENT'' THEN ''GOLFER'' || CHR ( 39 ) || ''S EQUIPMENT'' ELSE NULL END as END61_006_DESC,
CASE WHEN v_Scheduled = ''FINE ARTS'' OR v_Scheduled = ''FINE ARTS WITH BREAKAGE'' THEN ''Y'' ELSE ''N'' END as END61_007,
CASE WHEN v_Scheduled = ''FINE ARTS'' OR v_Scheduled = ''FINE ARTS WITH BREAKAGE'' THEN ''FINE ARTS'' ELSE NULL END as END61_007_DESC,
CASE WHEN v_Scheduled = ''POSTAGE STAMPS'' THEN ''Y'' ELSE ''N'' END as END61_009,
CASE WHEN v_Scheduled = ''POSTAGE STAMPS'' THEN ''POSTAGE STAMPS'' ELSE NULL END as END61_009_DESC,
CASE WHEN v_Scheduled = ''RARE AND CURRENT COINS'' THEN ''Y'' ELSE ''N'' END as END61_010,
CASE WHEN v_Scheduled = ''RARE AND CURRENT COINS'' THEN ''RARE AND CURRENT COINS'' ELSE NULL END as END61_010_DESC,
CASE WHEN v_Scheduled = ''GUNS'' THEN ''Y'' ELSE ''N'' END as END61_011,
CASE WHEN v_Scheduled = ''GUNS'' THEN ''GUNS'' ELSE NULL END as END61_011_DESC,
CASE WHEN v_Scheduled = ''COLLECTIBLES'' THEN ''Y'' ELSE ''N'' END as END61_012,
CASE WHEN v_Scheduled = ''COLLECTIBLES'' THEN ''COLLECTIBLES'' ELSE NULL END as END61_012_DESC,
CASE WHEN v_Scheduled = ''LAPTOP COMPUTERS'' THEN ''Y'' ELSE ''N'' END as END61_013,
CASE WHEN v_Scheduled = ''LAPTOP COMPUTERS'' THEN ''LAPTOP COMPUTERS'' ELSE NULL END as END61_013_DESC,
CASE WHEN LKP_33.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NOT NULL THEN ''Y'' ELSE ''N'' END as END_066,
CASE WHEN LKP_34.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NOT NULL THEN ''Y'' ELSE ''N'' END as END_067,
CASE WHEN LKP_35.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NOT NULL THEN ''Y'' ELSE ''N'' END as END_070,
CASE WHEN LKP_36.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NOT NULL THEN ''Y'' ELSE ''N'' END as END_071,
CASE WHEN LKP_37.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NOT NULL THEN ''Y'' ELSE ''N'' END as END_072,
to_number ( LKP_38.VAL /* replaced lookup LKP_AGMT_COVERAGES_TERMPATTERN_CODE */ ) as v_END_075,
CASE WHEN v_END_075 = 0.005 THEN ''Y'' ELSE ''N'' END as o_END_075,
CASE WHEN v_END_075 = 0.01 THEN ''Y'' ELSE ''N'' END as o_END_076,
CASE WHEN v_END_075 = 0.02 THEN ''Y'' ELSE ''N'' END as o_END_077,
CASE WHEN v_END_075 = 0.05 THEN ''Y'' ELSE ''N'' END as o_END_078,
CASE WHEN v_END_075 = 0.1 THEN ''Y'' ELSE ''N'' END as o_END_079,
CASE WHEN LKP_39.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NOT NULL THEN ''Y'' ELSE ''N'' END as END_085,
CASE WHEN LKP_40.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NOT NULL THEN ''Y'' ELSE ''N'' END as END_087,
CASE WHEN LKP_41.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NOT NULL THEN ''Y'' ELSE ''N'' END as END_088,
CASE WHEN LKP_42.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NOT NULL THEN ''Y'' ELSE ''N'' END as END_091,
CASE WHEN LKP_43.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NOT NULL THEN ''Y'' ELSE ''N'' END as END_092,
CASE WHEN LKP_44.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NOT NULL THEN ''Y'' ELSE ''N'' END as END_093,
CASE WHEN LKP_45.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NOT NULL THEN ''Y'' ELSE ''N'' END as END_095,
CASE WHEN LKP_46.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NOT NULL THEN ''Y'' ELSE ''N'' END as END_096,
CASE WHEN SQ_HO_EMBLEM_PLCY_STG.FIRE_DEPT_CD IS NOT NULL THEN ''Y'' ELSE ''N'' END as END_097,
CASE WHEN LKP_47.VAL /* replaced lookup LKP_AGMT_COVERAGES_COVPATTERN_CODE */ IS NOT NULL THEN ''Y'' ELSE ''N'' END as END_099,
LKP_NBR_REINST.CNT as NBR_REINST,
CASE WHEN LKP_APLCTN_RSPNS_IND.APLCTN_RSPNS_IND = ''1'' THEN ''Y'' ELSE '' '' END as o_SOLID_FUEL_HEAT,
CASE WHEN LKP_NBR_OUTBLDGS.NBR_OUTBLDGS IS NULL THEN ''0'' ELSE TO_CHAR ( LKP_NBR_OUTBLDGS.NBR_OUTBLDGS ) END as NBR_OUTBLDGS_out,
SQ_HO_EMBLEM_PLCY_STG.END_054 as END_054,
SQ_HO_EMBLEM_PLCY_STG.END_054_zone as END_054_zone,
SQ_HO_EMBLEM_PLCY_STG.END_055 as END_055,
SQ_HO_EMBLEM_PLCY_STG.END_055_zone as END_055_zone,
SQ_HO_EMBLEM_PLCY_STG.END_056 as END_056,
SQ_HO_EMBLEM_PLCY_STG.END_056_zone as END_056_zone,
SQ_HO_EMBLEM_PLCY_STG.source_record_id,
row_number() over (partition by SQ_HO_EMBLEM_PLCY_STG.source_record_id order by SQ_HO_EMBLEM_PLCY_STG.source_record_id) as RNK
FROM
SQ_HO_EMBLEM_PLCY_STG
INNER JOIN LKP_NBR_REINST ON SQ_HO_EMBLEM_PLCY_STG.source_record_id = LKP_NBR_REINST.source_record_id
INNER JOIN LKP_APLCTN_RSPNS_IND ON LKP_NBR_REINST.source_record_id = LKP_APLCTN_RSPNS_IND.source_record_id
INNER JOIN LKP_ASSET_DTL_CD_XREF ON LKP_APLCTN_RSPNS_IND.source_record_id = LKP_ASSET_DTL_CD_XREF.source_record_id
INNER JOIN LKP_NBR_OUTBLDGS ON LKP_ASSET_DTL_CD_XREF.source_record_id = LKP_NBR_OUTBLDGS.source_record_id
INNER JOIN LKP_EV_FINCL_EV ON LKP_NBR_OUTBLDGS.source_record_id = LKP_EV_FINCL_EV.source_record_id
INNER JOIN LKP_APLCTN_RSPNS_LIAB_EXPSR ON LKP_EV_FINCL_EV.source_record_id = LKP_APLCTN_RSPNS_LIAB_EXPSR.source_record_id
LEFT JOIN LKP_AGMT_COVERAGES_TERMPATTERN_CODE LKP_1 ON LKP_1.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_1.TERM_PATTERNCODE = ''HODW_Dwelling_Limit_HOE''
LEFT JOIN LKP_AGMT_COVERAGES_TERMPATTERN_CODE LKP_2 ON LKP_2.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_2.TERM_PATTERNCODE = ''HODW_PersonalPropertyLimit_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_TERMPATTERN_CODE LKP_3 ON LKP_3.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_3.TERM_PATTERNCODE = ''HOLI_Personal_Liability_HOELimit_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_TERMPATTERN_CODE LKP_4 ON LKP_4.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_4.TERM_PATTERNCODE = ''HOLI_MedPay_Limit_HOE''
LEFT JOIN LKP_AGMT_COVERAGES_TERMPATTERN_CODE LKP_5 ON LKP_5.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_5.TERM_PATTERNCODE = ''HODW_OtherStructuresLimit_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_TERMPATTERN_CODE LKP_6 ON LKP_6.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_6.TERM_PATTERNCODE = ''HODW_LossOfUseLimit_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_7 ON LKP_7.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_7.COV_PATTERNCODE = ''HODW_DwellingReplacementCost_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_8 ON LKP_8.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_8.COV_PATTERNCODE = ''HODW_FungiCov_HOE''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_9 ON LKP_9.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_9.COV_PATTERNCODE = ''HODW_FungiLiab_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_TERM_COV_CODE LKP_10 ON LKP_10.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_10.COV_PATTERNCODE = ''HODW_SectionI_Ded_HOE'' AND LKP_10.TERM_PATTERNCODE = ''HODW_Hurricane_Ded_HOE''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_11 ON LKP_11.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_11.COV_PATTERNCODE = ''HODW_BuildersRisk_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_12 ON LKP_12.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_12.COV_PATTERNCODE = ''HODW_TheftNewDwelling_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_13 ON LKP_13.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_13.COV_PATTERNCODE = ''HODW_BuildersRisk_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_14 ON LKP_14.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_14.COV_PATTERNCODE = ''HOLI_SpecificOtherStructureExclSchedule_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_15 ON LKP_15.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_15.COV_PATTERNCODE = ''HOLI_ExistingDamageToPropertyExclSchedule_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_16 ON LKP_16.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_16.COV_PATTERNCODE = ''HODW_CovADwellingRoofExcl_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_17 ON LKP_17.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_17.COV_PATTERNCODE = ''HOLI_SpecificOtherStructureExclSchedule_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_18 ON LKP_18.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_18.COV_PATTERNCODE = ''HOSI_SpecificOtherStructureExclItem_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_19 ON LKP_19.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_19.COV_PATTERNCODE = ''HODW_UnitOwnerRentalToOthers_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_20 ON LKP_20.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_20.COV_PATTERNCODE = ''HODW_LossAssessment_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_21 ON LKP_21.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_21.COV_PATTERNCODE = ''HOSI_SpecificOtherStructureItemRentedToOthers_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_22 ON LKP_22.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_22.COV_PATTERNCODE = ''HOLI_BusinessOnPremisesLiab_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_23 ON LKP_23.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_23.COV_PATTERNCODE = ''HODW_PersonalPropertyReplacementCost_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_24 ON LKP_24.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_24.COV_PATTERNCODE = ''HOSI_SpecificOtherStructureItemACVEndorsement_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_25 ON LKP_25.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_25.COV_PATTERNCODE = ''HOSI_SpecificOtherStructureItem_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_26 ON LKP_26.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_26.COV_PATTERNCODE = ''HOLI_BusinessOnPremisesProperty_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_27 ON LKP_27.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_27.COV_PATTERNCODE = ''HODW_PersonalPropertyOffResidenceSchedule_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_28 ON LKP_28.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_28.COV_PATTERNCODE = ''HOLI_CanineLiabilityExcl_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_29 ON LKP_29.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_29.COV_PATTERNCODE = ''HOLI_ForgeryAndCounterfeitIncrLimits_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_30 ON LKP_30.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_30.COV_PATTERNCODE = ''HOSI_ScheduledPropertyItem_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_31 ON LKP_31.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_31.COV_PATTERNCODE = ''HOSI_ScheduledPropertyItem_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_32 ON LKP_32.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_32.COV_PATTERNCODE = ''HOSI_ScheduledPropertyItemArticleType_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_33 ON LKP_33.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_33.COV_PATTERNCODE = ''HOLI_SpecificOtherStructureExclSchedule_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_34 ON LKP_34.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_34.COV_PATTERNCODE = ''HOLI_SpecificOtherStructureExclSchedule_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_35 ON LKP_35.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_35.COV_PATTERNCODE = ''HODW_ResidenceRentedToOthersItem_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_36 ON LKP_36.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_36.COV_PATTERNCODE = ''HOLI_OccupationalLiabSchedule_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_37 ON LKP_37.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_37.COV_PATTERNCODE = ''HOLI_FarmersCompLiabSchedule_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_TERMPATTERN_CODE LKP_38 ON LKP_38.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_38.TERM_PATTERNCODE = ''HODW_WindHail_Ded_HOE''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_39 ON LKP_39.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_39.COV_PATTERNCODE = ''HODW_ACVRoof_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_40 ON LKP_40.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_40.COV_PATTERNCODE = ''HOLI_PersonalInjury_HOE''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_41 ON LKP_41.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_41.COV_PATTERNCODE = ''HODW_SatelliteDishAndRelated_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_42 ON LKP_42.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_42.COV_PATTERNCODE = ''HODW_Merchandise_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_43 ON LKP_43.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_43.COV_PATTERNCODE = ''HODW_FirearmsIncreasedLimits_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_44 ON LKP_44.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_44.COV_PATTERNCODE = ''HOLI_HomeDayCareLiab_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_45 ON LKP_45.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_45.COV_PATTERNCODE = ''HODW_HomeComputerIncreasedLimits_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_46 ON LKP_46.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_46.COV_PATTERNCODE = ''HODW_Sinkhole_alfa''
LEFT JOIN LKP_AGMT_COVERAGES_COVPATTERN_CODE LKP_47 ON LKP_47.AGMT_ID = SQ_HO_EMBLEM_PLCY_STG.AGMT_ID AND LKP_47.COV_PATTERNCODE = ''HOLI_OtherInsuredResidenceLiabSchedule_alfa''
QUALIFY row_number() over (partition by SQ_HO_EMBLEM_PLCY_STG.source_record_id order by SQ_HO_EMBLEM_PLCY_STG.source_record_id)  
= 1
);


-- Component tgt_EDW_HO_EMBLEM_POLICY1, Type TARGET 
INSERT INTO DB_T_PROD_WRK.EDW_HO_EMBLEM_POLICY
(
MO_ID,
PLCY_NBR,
ACCT_DT,
EFF_DT,
EXP_DT,
STATE_CD,
LOC_COUNTY,
HO_ZONE,
LOC_ZIP,
CMPY_CD,
CUST_MEMB_NUM,
CUST_MEMB_TYPE,
DWEL_COV,
PERS_PROP_COV,
PERS_LIAB_COV,
MEDICAL_COV,
APP_STRUCT_COV,
LIVING_EXP_COV,
FORM,
PROT_CLASS,
ENC_CD,
FIRE_PROT_SERVICE,
CONSTR_DESC,
CNSTR_YR,
NBR_FAMILIES,
SOLID_FUEL_HEAT,
ROOF_YR_CY,
SEC_RES_CD,
AGE,
APP_GENDER,
APP_MARITAL_STAT,
APP_OCC,
END_001,
END_002,
END_003,
END_006,
END_007,
END_008,
END_009,
END_010,
END_011,
END_013,
END_014,
END_015,
END_017,
END_019,
END_023,
END_028,
END_033,
END_035,
END_040,
END_041,
END_042,
END_045,
END_046,
END_048,
END_049,
END_050,
END_052,
END_053,
END_054,
END_054_ZONE,
END_055,
END_055_ZONE,
END_056,
END_056_ZONE,
END_061,
END61_000_DESC,
END61_001,
END61_001_DESC,
END61_002,
END61_002_DESC,
END61_003,
END61_003_DESC,
END61_004,
END61_004_DESC,
END61_005,
END61_005_DESC,
END61_006,
END61_006_DESC,
END61_007,
END61_007_DESC,
END61_009,
END61_009_DESC,
END61_010,
END61_010_DESC,
END61_011,
END61_011_DESC,
END61_012,
END61_012_DESC,
END61_013,
END61_013_DESC,
END_066,
END_067,
END_070,
END_071,
END_072,
END_075,
END_076,
END_077,
END_078,
END_079,
END_085,
END_087,
END_088,
END_091,
END_092,
END_093,
END_095,
END_096,
END_097,
END_099,
PLCY_OGN_EFF_DT,
PLCY_PLN_EXP_DT,
MILES_TO_FIRE_DEPT,
FT_TO_FIRE_HYDRANT,
SQ_FOOTAGE,
SUBSTRUCTURE,
WALL_HTRS,
SPACE_HTRS,
NBR_OUTBLDGS,
POOL_HOT_TUB,
LIAB_EXPOSURE,
NBR_STORIES,
NBR_REINST,
YEARS_WITH_ALFA,
CLAIM_FREE,
AUTO_DISC,
NEW_HOME_DSC_PCT,
HOME_ALERT_DSC_PCT,
VAL_CLIENT_DSC_PCT,
SPRNK_DSC_PCT,
PRCS_ID,
LOAD_DT
)
SELECT
EXP_DATA_TRANS.MO_ID as MO_ID,
EXP_DATA_TRANS.PLCY_NBR as PLCY_NBR,
EXP_DATA_TRANS.out_DATE_STRING as ACCT_DT,
EXP_DATA_TRANS.TERM_EFF_DT as EFF_DT,
EXP_DATA_TRANS.PLCY_EXP_DT as EXP_DT,
EXP_DATA_TRANS.PLCY_RISK_STATE as STATE_CD,
EXP_DATA_TRANS.PLCY_RISK_COUNTY as LOC_COUNTY,
EXP_DATA_TRANS.ASSET_DTL_TXT as HO_ZONE,
EXP_DATA_TRANS.PLCY_MAIL_ZIP as LOC_ZIP,
EXP_DATA_TRANS.PLCY_COMPANY as CMPY_CD,
EXP_DATA_TRANS.MEMBER_NUMBER as CUST_MEMB_NUM,
EXP_DATA_TRANS.MEMBER_TYPE as CUST_MEMB_TYPE,
EXP_DATA_TRANS.DWELL_COV as DWEL_COV,
EXP_DATA_TRANS.PERS_PROP_COV as PERS_PROP_COV,
EXP_DATA_TRANS.PER_LIAB_LIM as PERS_LIAB_COV,
EXP_DATA_TRANS.MEDICAL_COV as MEDICAL_COV,
EXP_DATA_TRANS.APP_STRUCT_COV as APP_STRUCT_COV,
EXP_DATA_TRANS.LIVING_EXP_COV as LIVING_EXP_COV,
EXP_DATA_TRANS.FORM as FORM,
EXP_DATA_TRANS.PROTECTION_CLASS as PROT_CLASS,
EXP_DATA_TRANS.out_ENCUMBRANCE_CD as ENC_CD,
EXP_DATA_TRANS.out_FIRE_PROT_SERVICE as FIRE_PROT_SERVICE,
EXP_DATA_TRANS.CONSTRUCTION_TYPE as CONSTR_DESC,
EXP_DATA_TRANS.out_CNSTRCTN_DT as CNSTR_YR,
EXP_DATA_TRANS.NBR_FAMILIES as NBR_FAMILIES,
EXP_DATA_TRANS.o_SOLID_FUEL_HEAT as SOLID_FUEL_HEAT,
EXP_DATA_TRANS.ROOF_YR as ROOF_YR_CY,
EXP_DATA_TRANS.SEC_RES_IND_out as SEC_RES_CD,
EXP_DATA_TRANS.AGE as AGE,
EXP_DATA_TRANS.APP_GENDER as APP_GENDER,
EXP_DATA_TRANS.INSURED_MARITAL_STATUS as APP_MARITAL_STAT,
EXP_DATA_TRANS.INSURED_OCCUPATION_DESC as APP_OCC,
EXP_DATA_TRANS.END_001 as END_001,
EXP_DATA_TRANS.END_002 as END_002,
EXP_DATA_TRANS.END_003 as END_003,
EXP_DATA_TRANS.o_END_006 as END_006,
EXP_DATA_TRANS.o_END_007 as END_007,
EXP_DATA_TRANS.o_END_008 as END_008,
EXP_DATA_TRANS.o_END_009 as END_009,
EXP_DATA_TRANS.o_END_010 as END_010,
EXP_DATA_TRANS.o_END_011 as END_011,
EXP_DATA_TRANS.END_013 as END_013,
EXP_DATA_TRANS.END_014 as END_014,
EXP_DATA_TRANS.END_015 as END_015,
EXP_DATA_TRANS.END_017 as END_017,
EXP_DATA_TRANS.END_019 as END_019,
EXP_DATA_TRANS.END_023 as END_023,
EXP_DATA_TRANS.END_028 as END_028,
EXP_DATA_TRANS.END_033 as END_033,
EXP_DATA_TRANS.END_035 as END_035,
EXP_DATA_TRANS.END_040 as END_040,
EXP_DATA_TRANS.END_041 as END_041,
EXP_DATA_TRANS.END_042 as END_042,
EXP_DATA_TRANS.END_045 as END_045,
EXP_DATA_TRANS.END_046 as END_046,
EXP_DATA_TRANS.END_048 as END_048,
EXP_DATA_TRANS.END_049 as END_049,
EXP_DATA_TRANS.END_050 as END_050,
EXP_DATA_TRANS.END_052 as END_052,
EXP_DATA_TRANS.END_053 as END_053,
EXP_DATA_TRANS.END_054 as END_054,
EXP_DATA_TRANS.END_054_zone as END_054_ZONE,
EXP_DATA_TRANS.END_055 as END_055,
EXP_DATA_TRANS.END_055_zone as END_055_ZONE,
EXP_DATA_TRANS.END_056 as END_056,
EXP_DATA_TRANS.END_056_zone as END_056_ZONE,
EXP_DATA_TRANS.END_061 as END_061,
EXP_DATA_TRANS.END61_000_DESC as END61_000_DESC,
EXP_DATA_TRANS.END61_001 as END61_001,
EXP_DATA_TRANS.END61_001_DESC as END61_001_DESC,
EXP_DATA_TRANS.END61_002 as END61_002,
EXP_DATA_TRANS.END61_002_DESC as END61_002_DESC,
EXP_DATA_TRANS.END61_003 as END61_003,
EXP_DATA_TRANS.END61_003_DESC as END61_003_DESC,
EXP_DATA_TRANS.END61_004 as END61_004,
EXP_DATA_TRANS.END61_004_DESC as END61_004_DESC,
EXP_DATA_TRANS.END61_005 as END61_005,
EXP_DATA_TRANS.END61_005_DESC as END61_005_DESC,
EXP_DATA_TRANS.END61_006 as END61_006,
EXP_DATA_TRANS.END61_006_DESC as END61_006_DESC,
EXP_DATA_TRANS.END61_007 as END61_007,
EXP_DATA_TRANS.END61_007_DESC as END61_007_DESC,
EXP_DATA_TRANS.END61_009 as END61_009,
EXP_DATA_TRANS.END61_009_DESC as END61_009_DESC,
EXP_DATA_TRANS.END61_010 as END61_010,
EXP_DATA_TRANS.END61_010_DESC as END61_010_DESC,
EXP_DATA_TRANS.END61_011 as END61_011,
EXP_DATA_TRANS.END61_011_DESC as END61_011_DESC,
EXP_DATA_TRANS.END61_012 as END61_012,
EXP_DATA_TRANS.END61_012_DESC as END61_012_DESC,
EXP_DATA_TRANS.END61_013 as END61_013,
EXP_DATA_TRANS.END61_013_DESC as END61_013_DESC,
EXP_DATA_TRANS.END_066 as END_066,
EXP_DATA_TRANS.END_067 as END_067,
EXP_DATA_TRANS.END_070 as END_070,
EXP_DATA_TRANS.END_071 as END_071,
EXP_DATA_TRANS.END_072 as END_072,
EXP_DATA_TRANS.o_END_075 as END_075,
EXP_DATA_TRANS.o_END_076 as END_076,
EXP_DATA_TRANS.o_END_077 as END_077,
EXP_DATA_TRANS.o_END_078 as END_078,
EXP_DATA_TRANS.o_END_079 as END_079,
EXP_DATA_TRANS.END_085 as END_085,
EXP_DATA_TRANS.END_087 as END_087,
EXP_DATA_TRANS.END_088 as END_088,
EXP_DATA_TRANS.END_091 as END_091,
EXP_DATA_TRANS.END_092 as END_092,
EXP_DATA_TRANS.END_093 as END_093,
EXP_DATA_TRANS.END_095 as END_095,
EXP_DATA_TRANS.END_096 as END_096,
EXP_DATA_TRANS.END_097 as END_097,
EXP_DATA_TRANS.END_099 as END_099,
EXP_DATA_TRANS.ORIGINAL_ICP_DT as PLCY_OGN_EFF_DT,
EXP_DATA_TRANS.PLCY_EXP_DT1 as PLCY_PLN_EXP_DT,
EXP_DATA_TRANS.MILES_TO_FIRE_DEPT_out as MILES_TO_FIRE_DEPT,
EXP_DATA_TRANS.FEET_TO_FIRE_HYD_ABOVE_THRESHO as FT_TO_FIRE_HYDRANT,
EXP_DATA_TRANS.SQUARE_FOOTAGE as SQ_FOOTAGE,
EXP_DATA_TRANS.SUBSTRUCTURE as SUBSTRUCTURE,
EXP_DATA_TRANS.WALL_HTRS as WALL_HTRS,
EXP_DATA_TRANS.SPACE_HTRS as SPACE_HTRS,
EXP_DATA_TRANS.NBR_OUTBLDGS_out as NBR_OUTBLDGS,
EXP_DATA_TRANS.POOL_HOT_TUB_IND_OUT as POOL_HOT_TUB,
EXP_DATA_TRANS.out_LIAB_EXPOSURE as LIAB_EXPOSURE,
EXP_DATA_TRANS.NBR_STORIES as NBR_STORIES,
EXP_DATA_TRANS.NBR_REINST as NBR_REINST,
EXP_DATA_TRANS.YEARS_WITH_PRIOR as YEARS_WITH_ALFA,
EXP_DATA_TRANS.CLM_FREE_IND_out as CLAIM_FREE,
EXP_DATA_TRANS.AUTO_DSC_IND_out as AUTO_DISC,
EXP_DATA_TRANS.NWHM_PCT as NEW_HOME_DSC_PCT,
EXP_DATA_TRANS.HAPC_PCT as HOME_ALERT_DSC_PCT,
EXP_DATA_TRANS.VCLP_PCT as VAL_CLIENT_DSC_PCT,
EXP_DATA_TRANS.SPRNKLR_DISCOUNT_PCT as SPRNK_DSC_PCT,
EXP_DATA_TRANS.PRCS_ID as PRCS_ID,
EXP_DATA_TRANS.LOAD_DT as LOAD_DT
FROM
EXP_DATA_TRANS;


END; ';