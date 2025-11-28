-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_ADDR_INSUPD("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  PRCS_ID STRING;
  START_DTTM TIMESTAMP;
  END_DTTM TIMESTAMP;
  run_id STRING;
  workflow_name STRING;
  session_name STRING;
BEGIN
  run_id := public.func_get_scoped_param(:run_id, ''run_id'', :workflow_name, :worklet_name, :session_name);
  workflow_name := public.func_get_scoped_param(:run_id, ''workflow_name'', :workflow_name, :worklet_name, :session_name);
  session_name := public.func_get_scoped_param(:run_id, ''session_name'', :workflow_name, :worklet_name, :session_name);
  END_DTTM := public.func_get_scoped_param(:run_id, ''end_dttm'', :workflow_name, :worklet_name, :session_name);
  PRCS_ID := public.func_get_scoped_param(:run_id, ''prcs_id'', :workflow_name, :worklet_name, :session_name);
  START_DTTM := public.func_get_scoped_param(:run_id, ''start_dttm'', :workflow_name, :worklet_name, :session_name);
  
  -- PIPELINE START FOR 1
  -- Component sq_prty_add_loctr_x, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_prty_add_loctr_x AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS loc_id,
                $2  AS prty_id,
                $3  AS prty_addr_usge_type_cd,
                $4  AS prty_addr_usge_type_cd_new,
                $5  AS updatetime,
                $6  AS createtime,
                $7  AS retired,
                $8  AS rnk,
                $9  AS rnk1,
                $10 AS chksum_src,
                $11 AS chksum_tgt,
                $12 AS out_flag,
                $13 AS lkp_edw_end_dttm,
                $14 AS lkp_edw_strt_dttm,
                $15 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     ( WITH prty_addr_temp AS
                                  (
                                            SELECT
                                                      CASE
                                                                WHEN (
                                                                                    source_stg<>'''') THEN substr(source_stg,position(''-'',source_stg)+1)
                                                                ELSE ''''
                                                      END v_source,
                                                      CASE
                                                                WHEN (
                                                                                    source_stg<>'''') THEN substr(source_stg,1,position(''-'',source_stg)-1)
                                                                ELSE ''''
                                                      END tl_cnt_name,
                                                      source_stg,
                                                      cast(
                                                      CASE
                                                                WHEN v_source = ''ContactManager''
                                                                AND       (
                                                                                    tl_cnt_name)IN (''Person'',
                                                                                                    ''Adjudicator'',
                                                                                                    ''UserContact'',
                                                                                                    ''User Contact'',
                                                                                                    ''Vendor (Person)'',
                                                                                                    ''Attorney'',
                                                                                                    ''Doctor'',
                                                                                                    ''Policy Person'',
                                                                                                    ''Contact'',
                                                                                                    ''Lodging (Person)'') THEN lkp_indiv_cnt_mgr.indiv_prty_id
                                                                WHEN v_source = ''ClaimCenter''
                                                                AND       (
                                                                                    tl_cnt_name)IN (''Person'',
                                                                                                    ''Adjudicator'',
                                                                                                    ''UserContact'',
                                                                                                    ''User Contact'',
                                                                                                    ''Vendor (Person)'',
                                                                                                    ''Attorney'',
                                                                                                    ''Doctor'',
                                                                                                    ''Policy Person'',
                                                                                                    ''Contact'',
                                                                                                    ''Lodging (Person)'') THEN lkp_indiv_clm_ctr.indiv_prty_id
                                                                WHEN tl_cnt_name IN (''Company'',
                                                                                     ''Vendor (Company)'',
                                                                                     ''Auto Repair Shop'',
                                                                                     ''Auto Towing Agcy'',
                                                                                     ''Law Firm'',
                                                                                     ''Medical Care Organization'',
                                                                                     ''Lodging (Company)'',
                                                                                     ''Lodging Provider (Org)'')
                                                                AND       v_source IN ( ''ContactManager'',
                                                                                       ''ClaimCenter'') THEN busn_prty_id
                                                                ELSE coalesce(lkp_intrnl_org.intrnl_org_prty_id,''999'')
                                                      END AS INTEGER) prty_id,
                                                      addtype_typecode,
                                                      lkp_ctry.ctry_id,
                                                      lkp_terr.terr_id,
                                                      lkp_city.city_id,
                                                      lkp_cnty.cnty_id,
                                                      lkp_postl_cd.postl_cd_id,
                                                      lkp_street_addr.street_addr_id,
                                                      updatetime,
                                                      createtime,
                                                      retired,
                                                      rnk
                                            FROM      (
                                                                      SELECT DISTINCT state,
                                                                                      county,
                                                                                      postalcode,
                                                                                      city,
                                                                                      addressline1,
                                                                                      addressline2,
                                                                                      country_id,
                                                                                      state_id,
                                                                                      state_typecode,
                                                                                      country_typecode,
                                                                                      addtype_typecode,
                                                                                      addressbookuid,
                                                                                      source_stg,
                                                                                      updatetime,
                                                                                      retired,
                                                                                      org_key,
                                                                                      org_type,
                                                                                      org_subtype,
                                                                                      sys_src_cd,
                                                                                      createtime,
                                                                                      rank() over (PARTITION BY county, postalcode, city, addressline1, addressline2, country_id, state_id, state_typecode, country_typecode, addtype_typecode, addressbookuid, org_key, org_type, org_subtype ORDER BY updatetime ) AS rnk
                                                                      FROM            (
                                                                                             SELECT cast (state AS              VARCHAR(60)) AS state,
                                                                                                    cast (prty_add_loctr_x.county AS           VARCHAR(60)) AS county ,
                                                                                                    cast (prty_add_loctr_x.postalcode AS       VARCHAR(60)) AS postalcode ,
                                                                                                    cast (city AS               VARCHAR(60)) AS city,
                                                                                                    cast (prty_add_loctr_x.addressline1 AS     VARCHAR(60)) AS addressline1,
                                                                                                    cast (prty_add_loctr_x.addressline2 AS     VARCHAR(60)) AS addressline2,
                                                                                                    cast (prty_add_loctr_x.country_id AS       VARCHAR(60)) AS country_id,
                                                                                                    cast (prty_add_loctr_x.state_id AS         VARCHAR(60)) AS state_id,
                                                                                                    cast (prty_add_loctr_x.state_typecode AS   VARCHAR(60)) AS state_typecode,
                                                                                                    cast (prty_add_loctr_x.country_typecode AS VARCHAR(60)) AS country_typecode,
                                                                                                    cast (prty_add_loctr_x.addtype_typecode AS VARCHAR(60)) AS addtype_typecode,
                                                                                                    cast (prty_add_loctr_x.addressbookuid AS   VARCHAR(64)) AS addressbookuid,
                                                                                                    cast (prty_add_loctr_x.source_stg AS       VARCHAR(60)) AS source_stg,
                                                                                                    prty_add_loctr_x.updatetime,
                                                                                                    cast(prty_add_loctr_x.retired AS bigint) AS retired,
                                                                                                    prty_add_loctr_x.org_key,
                                                                                                    org_type,
                                                                                                    prty_add_loctr_x.org_subtype,
                                                                                                    prty_add_loctr_x.sys_src_cd ,
                                                                                                    CASE
                                                                                                           WHEN right(cast(extract(second FROM prty_add_loctr_x.createtime) AS VARCHAR(24)),4) BETWEEN 1000 AND    1499 THEN cast(cast(prty_add_loctr_x.createtime AS VARCHAR(22))
                                                                                                                         ||''0000'' AS timestamp(6))
                                                                                                           WHEN right(cast(extract(second FROM prty_add_loctr_x.createtime) AS VARCHAR(24)),4) BETWEEN 1500 AND    4499 THEN cast(cast(prty_add_loctr_x.createtime AS VARCHAR(22))
                                                                                                                         ||''3000'' AS timestamp(6))
                                                                                                           WHEN right(cast(extract(second FROM prty_add_loctr_x.createtime) AS VARCHAR(24)),4) BETWEEN 4500 AND    8499 THEN cast(cast(prty_add_loctr_x.createtime AS VARCHAR(22))
                                                                                                                         ||''7000'' AS timestamp(6))
                                                                                                           WHEN right(cast(extract(second FROM prty_add_loctr_x.createtime) AS VARCHAR(24)),4) BETWEEN 8500 AND    9999 THEN cast(cast(prty_add_loctr_x.createtime AS VARCHAR(22))
                                                                                                                         ||''0000'' AS timestamp(6)) + interval ''0.010 second''
                                                                                                           ELSE prty_add_loctr_x.createtime
                                                                                                    END AS createtime
                                                                                             FROM  (
                                                                                                                    /* DB_T_PROD_STAG.pc_address  */
                                                                                                                    SELECT DISTINCT cast(pc_address.state_stg AS          INTEGER)     AS state ,
                                                                                                                                    cast(pc_address.county_stg AS         VARCHAR(60)) AS county,
                                                                                                                                    cast(pctl_country.id_stg AS           INTEGER)     AS country_id,
                                                                                                                                    cast(pctl_state.id_stg AS             bigint)      AS state_id,
                                                                                                                                    cast(pctl_state.typecode_stg AS       VARCHAR(50)) AS state_typecode,
                                                                                                                                    cast(pctl_country.typecode_stg AS     VARCHAR(50)) AS country_typecode,
                                                                                                                                    cast(pc_address.postalcode_stg AS     VARCHAR(60)) AS postalcode ,
                                                                                                                                    cast(pc_address.city_stg AS           VARCHAR(60)) AS city,
                                                                                                                                    cast(pc_address.addressline1_stg AS   VARCHAR(60)) AS addressline1,
                                                                                                                                    cast(pc_address.addressline2_stg AS   VARCHAR(60)) AS addressline2 ,
                                                                                                                                    cast(pctl_addresstype.typecode_stg AS VARCHAR(50)) AS addtype_typecode ,
                                                                                                                                    cast(pc_contact.addressbookuid_stg AS VARCHAR(64)) AS addressbookuid,
                                                                                                                                    CASE
                                                                                                                                                    WHEN pc_address.updatetime_stg > pc_contact.updatetime_stg THEN pc_address.updatetime_stg
                                                                                                                                                    ELSE pc_contact.updatetime_stg
                                                                                                                                    END AS updatetime,
                                                                                                                                    cast(pctl_contact.typecode_stg
                                                                                                                                                    || ''-ContactManager'' AS VARCHAR(100)) AS source_stg,
                                                                                                                                    cast(NULL AS                            VARCHAR(50))  AS org_key ,
                                                                                                                                    cast(NULL AS                            VARCHAR(50))  AS org_type,
                                                                                                                                    cast(NULL AS                            VARCHAR(50))  AS org_subtype ,
                                                                                                                                    cast(''SRC_SYS4'' AS                      VARCHAR(50))  AS sys_src_cd ,
                                                                                                                                    pc_address.createtime_stg                             AS prty_addr_start_date,
                                                                                                                                    CASE
                                                                                                                                                    WHEN pc_contact.retired_stg=0
                                                                                                                                                    AND             pc_address.retired_stg=0 THEN 0
                                                                                                                                                    ELSE 1
                                                                                                                                    END                                              AS retired,
                                                                                                                                    cast(pc_contact.primaryphone_stg AS INTEGER)     AS primaryphone ,
                                                                                                                                    cast(pc_contact.cellphone_stg AS    VARCHAR(30)) AS cellphone,
                                                                                                                                    cast(pc_contact.homephone_stg AS    VARCHAR(30)) AS homephone,
                                                                                                                                    cast(pc_contact.workphone_stg AS    VARCHAR(30)) AS workphone,
                                                                                                                                    pc_contact.createtime_stg                        AS createtime
                                                                                                                    FROM            db_t_prod_stag.pctl_state,
                                                                                                                                    db_t_prod_stag.pctl_country,
                                                                                                                                    db_t_prod_stag.pc_address ,
                                                                                                                                    db_t_prod_stag.pctl_addresstype,
                                                                                                                                    db_t_prod_stag.pc_contact,
                                                                                                                                    db_t_prod_stag.pctl_contact
                                                                                                                    WHERE           pc_address.state_stg=pctl_state.id_stg
                                                                                                                    AND             pctl_country.id_stg=pc_address.country_stg
                                                                                                                    AND             pc_address.addresstype_stg = pctl_addresstype.id_stg
                                                                                                                    AND             pc_contact.primaryaddressid_stg = pc_address.id_stg
                                                                                                                    AND             pctl_contact.id_stg=pc_contact.subtype_stg
                                                                                                                    AND             ((
                                                                                                                                                                    pc_address.updatetime_stg > (:start_dttm)
                                                                                                                                                    AND             pc_address.updatetime_stg <= (:END_DTTM))
                                                                                                                                    OR              (
                                                                                                                                                                    pc_contact.updatetime_stg > (:start_dttm)
                                                                                                                                                    AND             pc_contact.updatetime_stg <= (:END_DTTM)))
                                                                                                                    UNION
                                                                                                                    SELECT          pc_address.state_stg,
                                                                                                                                    pc_address.county_stg ,
                                                                                                                                    cast(''10001'' AS INTEGER) AS ctry_id,
                                                                                                                                    pctl_state.id_stg        AS state_id ,
                                                                                                                                    pctl_state.typecode_stg  AS state_typecode,
                                                                                                                                    ''US''                     AS ctry_typecode,
                                                                                                                                    pc_address.postalcode_stg,
                                                                                                                                    pc_address.city_stg,
                                                                                                                                    pc_address.addressline1_stg,
                                                                                                                                    pc_address.addressline2_stg,
                                                                                                                                    ''PRTY_ADDR_USGE_TYPE8'' AS addtype_typecode,
                                                                                                                                    pc_contact.addressbookuid_stg,
                                                                                                                                    CASE
                                                                                                                                                    WHEN pc_address.updatetime_stg > pc_contact.updatetime_stg THEN pc_address.updatetime_stg
                                                                                                                                                    ELSE pc_contact.updatetime_stg
                                                                                                                                    END                                      AS updatetime,
                                                                                                                                    ''''                                       AS source_stg,
                                                                                                                                    pc_group.name_stg                        AS org_key,
                                                                                                                                    cast(''INTRNL_ORG_TYPE15'' AS VARCHAR(50)) AS org_type,
                                                                                                                                    pctl_grouptype.typecode_stg              AS org_subtype,
                                                                                                                                    cast(''SRC_SYS4'' AS VARCHAR(50))          AS sys_src_cd,
                                                                                                                                    pc_address.createtime_stg,
                                                                                                                                    CASE
                                                                                                                                                    WHEN pc_contact.retired_stg=0
                                                                                                                                                    AND             pc_address.retired_stg=0 THEN 0
                                                                                                                                                    ELSE 1
                                                                                                                                    END AS retired,
                                                                                                                                    pc_contact.primaryphone_stg,
                                                                                                                                    pc_contact.cellphone_stg,
                                                                                                                                    pc_contact.homephone_stg,
                                                                                                                                    pc_contact.workphone_stg,
                                                                                                                                    pc_contact.createtime_stg
                                                                                                                    FROM            db_t_prod_stag.pc_group
                                                                                                                    join            db_t_prod_stag.pc_contact
                                                                                                                    ON              pc_group.contact_alfa_stg=pc_contact.id_stg
                                                                                                                    join            db_t_prod_stag.pc_address
                                                                                                                    ON              pc_contact.primaryaddressid_stg = pc_address.id_stg
                                                                                                                    join            db_t_prod_stag.pctl_addresstype
                                                                                                                    ON              pc_address.addresstype_stg = pctl_addresstype.id_stg
                                                                                                                    join            db_t_prod_stag.pctl_contact
                                                                                                                    ON              pctl_contact.id_stg=pc_contact.subtype_stg
                                                                                                                    left outer join db_t_prod_stag.pctl_state
                                                                                                                    ON              pctl_state.id_stg=pc_address.state_stg
                                                                                                                    left outer join db_t_prod_stag.pctl_country
                                                                                                                    ON              pctl_country.id_stg=pc_address.country_stg
                                                                                                                    inner join      db_t_prod_stag.pctl_grouptype
                                                                                                                    ON              pctl_grouptype.id_stg=pc_group.grouptype_stg
                                                                                                                    WHERE           pctl_grouptype.typecode_stg =''servicecenter_alfa''
                                                                                                                    AND             pctl_state.typecode_stg IS NOT NULL
                                                                                                                    AND             ((
                                                                                                                                                                    pc_address.updatetime_stg > (:start_dttm)
                                                                                                                                                    AND             pc_address.updatetime_stg <= (:END_DTTM))
                                                                                                                                    OR              (
                                                                                                                                                                    pc_contact.updatetime_stg > (:start_dttm)
                                                                                                                                                    AND             pc_contact.updatetime_stg <= (:END_DTTM)))
                                                                                                                    UNION ALL
                                                                                                                    /* DB_T_PROD_STAG.cc_address */
                                                                                                                    SELECT DISTINCT cast(cc_address.state_stg AS          INTEGER)     AS state,
                                                                                                                                    cast(cc_address.county_stg AS         VARCHAR(60)) AS county,
                                                                                                                                    cast(cctl_country.id_stg AS           INTEGER)     AS ctry_id,
                                                                                                                                    cast(cctl_state.id_stg AS             bigint)      AS state_id,
                                                                                                                                    cast(cctl_state.typecode_stg AS       VARCHAR(50)) AS state_typecode,
                                                                                                                                    cast(cctl_country.typecode_stg AS     VARCHAR(50)) AS ctry_typecode,
                                                                                                                                    cast(cc_address.postalcode_stg AS     VARCHAR(60)) AS postalcode,
                                                                                                                                    cast(cc_address.city_stg AS           VARCHAR(60)) AS city,
                                                                                                                                    cast(cc_address.addressline1_stg AS   VARCHAR(60)) AS addressline1 ,
                                                                                                                                    cast(cc_address.addressline2_stg AS   VARCHAR(60)) AS addressline2,
                                                                                                                                    cast(cctl_addresstype.typecode_stg AS VARCHAR(50)) AS typecode_stg ,
                                                                                                                                    cast(cc_contact.publicid_stg AS       VARCHAR(64)) AS publicid ,
                                                                                                                                    CASE
                                                                                                                                                    WHEN cc_address.updatetime_stg>cc_contact.updatetime_stg THEN cc_address.updatetime_stg
                                                                                                                                                    ELSE cc_contact.updatetime_stg
                                                                                                                                    END AS updatetime,
                                                                                                                                    cast(cctl_contact.name_stg
                                                                                                                                                    || ''-ClaimCenter'' AS VARCHAR(100))AS source_stg,
                                                                                                                                    cast(NULL AS                         VARCHAR(50)) AS org_key ,
                                                                                                                                    cast(NULL AS                         VARCHAR(50)) AS org_type,
                                                                                                                                    cast(NULL AS                         VARCHAR(50)) AS org_subtype ,
                                                                                                                                    cast(''SRC_SYS6'' AS                   VARCHAR(50)) AS sys_src_cd,
                                                                                                                                    cc_address.createtime_stg,
                                                                                                                                    CASE
                                                                                                                                                    WHEN cc_contact.retired_stg=0
                                                                                                                                                    AND             cc_address.retired_stg=0 THEN 0
                                                                                                                                                    ELSE 1
                                                                                                                                    END                                              AS retired,
                                                                                                                                    cast(cc_contact.primaryphone_stg AS INTEGER)     AS primaryphone,
                                                                                                                                    cast(cc_contact.cellphone_stg AS    VARCHAR(30)) AS cellphone,
                                                                                                                                    cast(cc_contact.homephone_stg AS    VARCHAR(30)) AS homephone,
                                                                                                                                    cast(cc_contact.workphone_stg AS    VARCHAR(30)) AS workphone,
                                                                                                                                    cc_contact.createtime_stg
                                                                                                                    FROM            db_t_prod_stag.cctl_state,
                                                                                                                                    db_t_prod_stag.cctl_country,
                                                                                                                                    db_t_prod_stag.cc_address ,
                                                                                                                                    db_t_prod_stag.cctl_addresstype,
                                                                                                                                    db_t_prod_stag.cc_contact,
                                                                                                                                    db_t_prod_stag.cctl_contact
                                                                                                                    WHERE           cc_address.state_stg=cctl_state.id_stg
                                                                                                                    AND             cctl_country.id_stg=cc_address.country_stg
                                                                                                                    AND             cc_address.addresstype_stg = cctl_addresstype.id_stg
                                                                                                                    AND             cc_contact.primaryaddressid_stg = cc_address.id_stg
                                                                                                                    AND             cctl_contact.id_stg=cc_contact.subtype_stg
                                                                                                                    AND             ((
                                                                                                                                                                    cc_address.updatetime_stg > (:start_dttm)
                                                                                                                                                    AND             cc_address.updatetime_stg <= (:END_DTTM))
                                                                                                                                    OR              (
                                                                                                                                                                    cc_contact.updatetime_stg > (:start_dttm)
                                                                                                                                                    AND             cc_contact.updatetime_stg <= (:END_DTTM)))
                                                                                                                    UNION ALL
                                                                                                                    /* DB_T_PROD_STAG.bc_address */
                                                                                                                    SELECT DISTINCT cast(bc_address.state_stg AS        INTEGER)     AS state,
                                                                                                                                    cast(bc_address.county_stg AS       VARCHAR(60)) AS county,
                                                                                                                                    cast(bctl_country.id_stg AS         INTEGER)     AS ctry_id,
                                                                                                                                    cast(bctl_state.id_stg AS           bigint)      AS state_id,
                                                                                                                                    cast(bctl_state.typecode_stg AS     VARCHAR(50)) AS state_typecode,
                                                                                                                                    cast(bctl_country.typecode_stg AS   VARCHAR(50)) AS ctry_typecode,
                                                                                                                                    cast(bc_address.postalcode_stg AS   VARCHAR(60)) AS postalcode ,
                                                                                                                                    cast(bc_address.city_stg AS         VARCHAR(60)) AS city,
                                                                                                                                    cast(bc_address.addressline1_stg AS VARCHAR(60)) AS addressline1,
                                                                                                                                    cast(bc_address.addressline2_stg AS VARCHAR(60)) AS addressline2 ,
                                                                                                                                    CASE
                                                                                                                                                    WHEN (
                                                                                                                                                                                    bctl_addresstype.typecode_stg IS NULL) THEN ''billing''
                                                                                                                                                    ELSE bctl_addresstype.typecode_stg
                                                                                                                                    END AS typecode,
                                                                                                                                    CASE
                                                                                                                                                    WHEN (
                                                                                                                                                                                    e.externalid_stg IS NULL) THEN e.publicid_stg
                                                                                                                                                    ELSE e.externalid_stg
                                                                                                                                    END AS prty_nk,
                                                                                                                                    CASE
                                                                                                                                                    WHEN e.updatetime_stg > a.updatetime_stg THEN e.updatetime_stg
                                                                                                                                                    ELSE a.updatetime_stg
                                                                                                                                    END AS updatetime,
                                                                                                                                    cast(bctl_contact.typecode_stg
                                                                                                                                                    || ''-ClaimCenter'' AS VARCHAR(100)) AS source_stg,
                                                                                                                                    cast(NULL AS                         VARCHAR(50))  AS org_key ,
                                                                                                                                    cast(NULL AS                         VARCHAR(50))  AS org_type,
                                                                                                                                    cast(NULL AS                         VARCHAR(50))  AS org_subtype ,
                                                                                                                                    cast(''SRC_SYS5'' AS                   VARCHAR(50))  AS sys_src_cd ,
                                                                                                                                    bc_address.createtime_stg,
                                                                                                                                    CASE
                                                                                                                                                    WHEN e.retired_stg=0
                                                                                                                                                    AND             bc_address.retired_stg=0 THEN 0
                                                                                                                                                    ELSE 1
                                                                                                                                    END                        AS retired,
                                                                                                                                    cast( NULL AS INTEGER)     AS primaryphone,
                                                                                                                                    cast( NULL AS VARCHAR(30)) AS cellphone,
                                                                                                                                    cast( NULL AS VARCHAR(30)) AS homephone,
                                                                                                                                    cast( NULL AS VARCHAR(30)) AS workphone,
                                                                                                                                    e.createtime_stg           AS createtime_new
                                                                                                                    FROM            db_t_prod_stag.bc_account a
                                                                                                                    left join       db_t_prod_stag.bc_invoicestream b
                                                                                                                    ON              a.id_stg = b.accountid_stg
                                                                                                                    inner join      db_t_prod_stag.bc_accountcontact c
                                                                                                                    ON              c.accountid_stg = a.id_stg
                                                                                                                    inner join      db_t_prod_stag.bc_contact e
                                                                                                                    ON              e.id_stg = c.contactid_stg
                                                                                                                    join            db_t_prod_stag.bctl_contact t
                                                                                                                    ON              t.id_stg=e.subtype_stg
                                                                                                                    left join       db_t_prod_stag.bc_accountcontactrole f
                                                                                                                    ON              f.accountcontactid_stg = c.id_stg
                                                                                                                    left join       db_t_prod_stag.bctl_accountrole g
                                                                                                                    ON              g.id_stg = f.role_stg
                                                                                                                    left join       db_t_prod_stag.bc_address
                                                                                                                    ON              e.primaryaddressid_stg=bc_address.id_stg
                                                                                                                    inner join      db_t_prod_stag.bctl_country
                                                                                                                    ON              bctl_country.id_stg=bc_address.country_stg
                                                                                                                    inner join      db_t_prod_stag.bctl_state
                                                                                                                    ON              bc_address.state_stg=bctl_state.id_stg
                                                                                                                    inner join      db_t_prod_stag.bctl_contact
                                                                                                                    ON              bctl_contact.id_stg=e.subtype_stg
                                                                                                                    left join       db_t_prod_stag.bctl_addresstype
                                                                                                                    ON              bc_address.addresstype_stg = bctl_addresstype.id_stg
                                                                                                                    WHERE           b.overridingpayer_alfa_stg IS NULL
                                                                                                                    AND             c.primarypayer_stg = 1
                                                                                                                    AND             ((
                                                                                                                                                                    bc_address.updatetime_stg>(:start_dttm)
                                                                                                                                                    AND             bc_address.updatetime_stg <=(:END_DTTM))
                                                                                                                                    OR              (
                                                                                                                                                                    e.updatetime_stg>(:start_dttm)
                                                                                                                                                    AND             e.updatetime_stg <= (:END_DTTM))
                                                                                                                                    OR              (
                                                                                                                                                                    a.updatetime_stg>(:start_dttm)
                                                                                                                                                    AND             a.updatetime_stg <= (:END_DTTM)))
                                                                                                                    UNION
                                                                                                                    SELECT DISTINCT bc_address.state_stg,
                                                                                                                                    bc_address.county_stg,
                                                                                                                                    bctl_country.id_stg       AS ctry_id,
                                                                                                                                    bctl_state.id_stg         AS state_id,
                                                                                                                                    bctl_state.typecode_stg   AS state_typecode,
                                                                                                                                    bctl_country.typecode_stg AS ctry_typecode,
                                                                                                                                    bc_address.postalcode_stg,
                                                                                                                                    bc_address.city_stg,
                                                                                                                                    bc_address.addressline1_stg,
                                                                                                                                    bc_address.addressline2_stg,
                                                                                                                                    CASE
                                                                                                                                                    WHEN (
                                                                                                                                                                                    bctl_addresstype.typecode_stg IS NULL) THEN ''billing''
                                                                                                                                                    ELSE bctl_addresstype.typecode_stg
                                                                                                                                    END AS typecode,
                                                                                                                                    CASE
                                                                                                                                                    WHEN (
                                                                                                                                                                                    e.externalid_stg IS NULL) THEN e.publicid_stg
                                                                                                                                                    ELSE e.externalid_stg
                                                                                                                                    END AS prty_nk,
                                                                                                                                    CASE
                                                                                                                                                    WHEN a.updatetime_stg>e.updatetime_stg THEN a.updatetime_stg
                                                                                                                                                    ELSE e.updatetime_stg
                                                                                                                                    END AS updatetime,
                                                                                                                                    bctl_contact.typecode_stg
                                                                                                                                                    || ''-ClaimCenter'' AS source_stg,
                                                                                                                                    cast(NULL AS       VARCHAR(50))         AS org_key ,
                                                                                                                                    cast(NULL AS       VARCHAR(50))         AS org_type,
                                                                                                                                    cast(NULL AS       VARCHAR(50))         AS org_subtype ,
                                                                                                                                    cast(''SRC_SYS5'' AS VARCHAR(50))         AS sys_src_cd ,
                                                                                                                                    bc_address.createtime_stg,
                                                                                                                                    CASE
                                                                                                                                                    WHEN e.retired_stg=0
                                                                                                                                                    AND             bc_address.retired_stg=0 THEN 0
                                                                                                                                                    ELSE 1
                                                                                                                                    END                        AS retired,
                                                                                                                                    cast( NULL AS INTEGER)     AS primaryphone,
                                                                                                                                    cast( NULL AS VARCHAR(30)) AS cellphone,
                                                                                                                                    cast( NULL AS VARCHAR(30)) AS homephone,
                                                                                                                                    cast( NULL AS VARCHAR(30)) AS workphone,
                                                                                                                                    e.createtime_stg           AS createtime_new
                                                                                                                    FROM            db_t_prod_stag.bc_account a
                                                                                                                    inner join      db_t_prod_stag.bc_invoicestream b
                                                                                                                    ON              a.id_stg = b.accountid_stg
                                                                                                                    inner join      db_t_prod_stag.bc_accountcontact c
                                                                                                                    ON              c.id_stg = b.overridingpayer_alfa_stg
                                                                                                                    inner join      db_t_prod_stag.bc_contact e
                                                                                                                    ON              e.id_stg = c.contactid_stg
                                                                                                                    join            db_t_prod_stag.bctl_contact t
                                                                                                                    ON              t.id_stg=e.subtype_stg
                                                                                                                    left join       db_t_prod_stag.bc_accountcontactrole f
                                                                                                                    ON              f.accountcontactid_stg = c.id_stg
                                                                                                                    left join       db_t_prod_stag.bctl_accountrole g
                                                                                                                    ON              g.id_stg = f.role_stg
                                                                                                                    left join       db_t_prod_stag.bc_address
                                                                                                                    ON              e.primaryaddressid_stg=bc_address.id_stg
                                                                                                                    join            db_t_prod_stag.bctl_country
                                                                                                                    ON              bctl_country.id_stg=bc_address.country_stg
                                                                                                                    join            db_t_prod_stag.bctl_state
                                                                                                                    ON              bc_address.state_stg=bctl_state.id_stg
                                                                                                                    join            db_t_prod_stag.bctl_contact
                                                                                                                    ON              bctl_contact.id_stg=e.subtype_stg
                                                                                                                    left join       db_t_prod_stag.bctl_addresstype
                                                                                                                    ON              bc_address.addresstype_stg = bctl_addresstype.id_stg
                                                                                                                    WHERE           b.overridingpayer_alfa_stg IS NOT NULL
                                                                                                                    AND             ((
                                                                                                                                                                    bc_address.updatetime_stg>(:start_dttm)
                                                                                                                                                    AND             bc_address.updatetime_stg <=(:END_DTTM))
                                                                                                                                    OR              (
                                                                                                                                                                    e.updatetime_stg>(:start_dttm)
                                                                                                                                                    AND             e.updatetime_stg <= (:END_DTTM))
                                                                                                                                    OR              (
                                                                                                                                                                    a.updatetime_stg>(:start_dttm)
                                                                                                                                                    AND             a.updatetime_stg <= (:END_DTTM)))
                                                                                                                    UNION
                                                                                                                    SELECT DISTINCT bc_address.state_stg,
                                                                                                                                    bc_address.county_stg,
                                                                                                                                    bctl_country.id_stg       AS ctry_id,
                                                                                                                                    bctl_state.id_stg         AS state_id,
                                                                                                                                    bctl_state.typecode_stg   AS state_typecode,
                                                                                                                                    bctl_country.typecode_stg AS ctry_typecode,
                                                                                                                                    bc_address.postalcode_stg,
                                                                                                                                    bc_address.city_stg,
                                                                                                                                    bc_address.addressline1_stg,
                                                                                                                                    bc_address.addressline2_stg,
                                                                                                                                    bctl_addresstype.typecode_stg,
                                                                                                                                    bc_contact.publicid_stg,
                                                                                                                                    CASE
                                                                                                                                                    WHEN bc_address.updatetime_stg>bc_contact.updatetime_stg THEN bc_address.updatetime_stg
                                                                                                                                                    ELSE bc_contact.updatetime_stg
                                                                                                                                    END AS updatetime,
                                                                                                                                    bctl_contact.typecode_stg
                                                                                                                                                    || ''-ClaimCenter'' AS source_stg,
                                                                                                                                    cast(NULL AS       VARCHAR(50))         AS org_key ,
                                                                                                                                    cast(NULL AS       VARCHAR(50))         AS org_type,
                                                                                                                                    cast(NULL AS       VARCHAR(50))         AS org_subtype ,
                                                                                                                                    cast(''SRC_SYS5'' AS VARCHAR(50))         AS sys_src_cd ,
                                                                                                                                    bc_address.createtime_stg,
                                                                                                                                    CASE
                                                                                                                                                    WHEN bc_contact.retired_stg=0
                                                                                                                                                    AND             bc_address.retired_stg=0 THEN 0
                                                                                                                                                    ELSE 1
                                                                                                                                    END                        AS retired,
                                                                                                                                    cast( NULL AS INTEGER)     AS primaryphone,
                                                                                                                                    cast( NULL AS VARCHAR(30)) AS cellphone,
                                                                                                                                    cast( NULL AS VARCHAR(30)) AS homephone,
                                                                                                                                    cast( NULL AS VARCHAR(30)) AS workphone,
                                                                                                                                    bc_contact.createtime_stg  AS createtime_new
                                                                                                                    FROM            db_t_prod_stag.bc_address
                                                                                                                    join            db_t_prod_stag.bctl_state
                                                                                                                    ON              bc_address.state_stg=bctl_state.id_stg
                                                                                                                    join            db_t_prod_stag.bctl_country
                                                                                                                    ON              bctl_country.id_stg=bc_address.country_stg
                                                                                                                    left join       db_t_prod_stag.bctl_addresstype
                                                                                                                    ON              bc_address.addresstype_stg = bctl_addresstype.id_stg
                                                                                                                    join            db_t_prod_stag.bc_contact
                                                                                                                    ON              bc_contact.primaryaddressid_stg = bc_address.id_stg
                                                                                                                    join            db_t_prod_stag.bctl_contact
                                                                                                                    ON              bctl_contact.id_stg=bc_contact.subtype_stg
                                                                                                                    AND             ((
                                                                                                                                                                    bc_address.updatetime_stg > (:start_dttm)
                                                                                                                                                    AND             bc_address.updatetime_stg <= (:END_DTTM))
                                                                                                                                    OR              (
                                                                                                                                                                    bc_contact.updatetime_stg > (:start_dttm)
                                                                                                                                                    AND             bc_contact.updatetime_stg <= (:END_DTTM)))
                                                                                                                    UNION
                                                                                                                    SELECT DISTINCT bc_address.state_stg,
                                                                                                                                    bc_address.county_stg,
                                                                                                                                    bctl_country.id_stg       AS ctry_id,
                                                                                                                                    bctl_state.id_stg         AS state_id,
                                                                                                                                    bctl_state.typecode_stg   AS state_typecode,
                                                                                                                                    bctl_country.typecode_stg AS ctry_typecode,
                                                                                                                                    bc_address.postalcode_stg,
                                                                                                                                    bc_address.city_stg,
                                                                                                                                    bc_address.addressline1_stg,
                                                                                                                                    bc_address.addressline2_stg,
                                                                                                                                    CASE
                                                                                                                                                    WHEN (
                                                                                                                                                                                    bctl_addresstype.typecode_stg IS NULL) THEN ''billing''
                                                                                                                                                    ELSE bctl_addresstype.typecode_stg
                                                                                                                                    END AS typecode,
                                                                                                                                    CASE
                                                                                                                                                    WHEN (
                                                                                                                                                                                    d.externalid_stg IS NULL) THEN d.publicid_stg
                                                                                                                                                    ELSE d.externalid_stg
                                                                                                                                    END AS prty_nk,
                                                                                                                                    CASE
                                                                                                                                                    WHEN a.updatetime_stg>d.updatetime_stg THEN a.updatetime_stg
                                                                                                                                                    ELSE d.updatetime_stg
                                                                                                                                    END AS updatetime,
                                                                                                                                    t.typecode_stg
                                                                                                                                                    || ''-ClaimCenter'' AS source_stg,
                                                                                                                                    cast(NULL AS       VARCHAR(50))         AS org_key ,
                                                                                                                                    cast(NULL AS       VARCHAR(50))         AS org_type,
                                                                                                                                    cast(NULL AS       VARCHAR(50))         AS org_subtype ,
                                                                                                                                    cast(''SRC_SYS5'' AS VARCHAR(50))         AS sys_src_cd ,
                                                                                                                                    bc_address.createtime_stg,
                                                                                                                                    CASE
                                                                                                                                                    WHEN a.retired_stg=0
                                                                                                                                                    AND             bc_address.retired_stg=0 THEN 0
                                                                                                                                                    ELSE 1
                                                                                                                                    END                        AS retired,
                                                                                                                                    cast( NULL AS INTEGER)     AS primaryphone,
                                                                                                                                    cast( NULL AS VARCHAR(30)) AS cellphone,
                                                                                                                                    cast( NULL AS VARCHAR(30)) AS homephone,
                                                                                                                                    cast( NULL AS VARCHAR(30)) AS workphone,
                                                                                                                                    a.createtime_stg           AS createtime_new
                                                                                                                    FROM            db_t_prod_stag.bc_account a
                                                                                                                    inner join      db_t_prod_stag.bc_accountcontact h
                                                                                                                    ON              h.accountid_stg = a.id_stg
                                                                                                                    inner join      db_t_prod_stag.bc_contact d
                                                                                                                    ON              d.id_stg = h.contactid_stg
                                                                                                                    join            db_t_prod_stag.bctl_contact t
                                                                                                                    ON              t.id_stg=d.subtype_stg
                                                                                                                    left join       db_t_prod_stag.bc_accountcontactrole i
                                                                                                                    ON              i.accountcontactid_stg = h.id_stg
                                                                                                                    left join       db_t_prod_stag.bctl_accountrole j
                                                                                                                    ON              j.id_stg = i.role_stg
                                                                                                                    join            db_t_prod_stag.bc_address
                                                                                                                    ON              d.primaryaddressid_stg = bc_address.id_stg
                                                                                                                    join            db_t_prod_stag.bctl_state
                                                                                                                    ON              bc_address.state_stg=bctl_state.id_stg
                                                                                                                    join            db_t_prod_stag.bctl_country
                                                                                                                    ON              bctl_country.id_stg=bc_address.country_stg
                                                                                                                    left join       db_t_prod_stag.bctl_addresstype
                                                                                                                    ON              bc_address.addresstype_stg = bctl_addresstype.id_stg
                                                                                                                    WHERE           ((
                                                                                                                                                                    h.primarypayer_stg = 1)
                                                                                                                                    OR              (
                                                                                                                                                                    j.name_stg = ''Payer''))
                                                                                                                    AND             ((
                                                                                                                                                                    d.updatetime_stg > (:start_dttm)
                                                                                                                                                    AND             d.updatetime_stg <= (:END_DTTM))
                                                                                                                                    OR              (
                                                                                                                                                                    a.updatetime_stg > (:start_dttm)
                                                                                                                                                    AND             a.updatetime_stg <= (:END_DTTM))) ) prty_add_loctr_x
                                                                                             WHERE  prty_add_loctr_x.addressbookuid IS NOT NULL
                                                                                             UNION
                                                                                             /* DB_T_PROD_STAG.ab_abcontact */
                                                                                             SELECT cast(NULL AS VARCHAR(60)) AS state ,
                                                                                                    cast(NULL AS VARCHAR(60)) AS county ,
                                                                                                    cast(NULL AS VARCHAR(60)) AS postalcode ,
                                                                                                    cast(NULL AS VARCHAR(60)) AS city ,
                                                                                                    cast(NULL AS VARCHAR(60)) AS addressline1 ,
                                                                                                    cast(NULL AS VARCHAR(60)) AS addressline2 ,
                                                                                                    cast(NULL AS VARCHAR(60)) AS country_id ,
                                                                                                    cast(NULL AS VARCHAR(60)) AS state_id ,
                                                                                                    cast(NULL AS VARCHAR(60)) AS state_typecode ,
                                                                                                    cast(NULL AS VARCHAR(60)) AS country_typecode ,
                                                                                                    ''PRTY_ADDR_USGE_TYPE3''    AS addtype_typecode,
                                                                                                    cast(
                                                                                                    CASE
                                                                                                           WHEN source=''ContactManager'' THEN linkid
                                                                                                           WHEN source=''ClaimCenter'' THEN publicid
                                                                                                    END AS VARCHAR(64)) AS addressbookuid ,
                                                                                                    cast(ab_abcontact.tl_cnt_name
                                                                                                           ||''-''
                                                                                                           ||ab_abcontact.source AS VARCHAR(60)) AS source_stg,
                                                                                                    ab_abcontact.updatetime,
                                                                                                    ab_abcontact.retired ,
                                                                                                    cast(NULL AS VARCHAR(60)) AS org_key ,
                                                                                                    cast(NULL AS VARCHAR(60)) AS org_type ,
                                                                                                    cast(NULL AS VARCHAR(60)) AS org_subtype,
                                                                                                    ''SRC_SYS6''                AS sys_src_cd ,
                                                                                                    createtime
                                                                                             FROM   (
                                                                                                                    SELECT          bc_contact.updatetime_stg                          AS updatetime,
                                                                                                                                    bc_contact.publicid_stg                            AS publicid,
                                                                                                                                    cast(bc_contact.addressbookuid_stg AS VARCHAR(64)) AS linkid,
                                                                                                                                    bc_contact.retired_stg                             AS retired,
                                                                                                                                    bc_contact.createtime_stg                          AS createtime,
                                                                                                                                    bctl_contact.name_stg                              AS tl_cnt_name,
                                                                                                                                    cast(''ClaimCenter'' AS VARCHAR(100))                AS source
                                                                                                                    FROM            db_t_prod_stag.bc_contact
                                                                                                                    left outer join db_t_prod_stag.bctl_contact
                                                                                                                    ON              bctl_contact.id_stg = bc_contact.subtype_stg
                                                                                                                    left outer join db_t_prod_stag.bc_user
                                                                                                                    ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                                                    WHERE           bctl_contact.typecode_stg = (''UserContact'')
                                                                                                                    AND
                                                                                                                                    /*  below condition added to avoid duplicates */
                                                                                                                                    bc_contact.publicid_stg NOT IN (''default_data:1'',
                                                                                                                                                                    ''systemTables:1'',
                                                                                                                                                                    ''systemTables:2'')
                                                                                                                    AND             ((
                                                                                                                                                                    bc_contact.updatetime_stg>(:start_dttm)
                                                                                                                                                    AND             bc_contact.updatetime_stg <=(:END_DTTM))
                                                                                                                                    OR              (
                                                                                                                                                                    bc_user.updatetime_stg>(:start_dttm)
                                                                                                                                                    AND             bc_user.updatetime_stg <= (:END_DTTM)))
                                                                                                                    UNION
                                                                                                                    /*  Primary and Secondary Payer contact (this is at the Account level) */
                                                                                                                    SELECT
                                                                                                                                    CASE
                                                                                                                                                    WHEN bc_contact.updatetime_stg > a.updatetime_stg THEN bc_contact.updatetime_stg
                                                                                                                                                    ELSE a.updatetime_stg
                                                                                                                                    END AS updatetime,
                                                                                                                                    CASE
                                                                                                                                                    WHEN(
                                                                                                                                                                                    bc_contact.externalid_stg IS NOT NULL) THEN bc_contact.externalid_stg
                                                                                                                                                    ELSE bc_contact.publicid_stg
                                                                                                                                    END                                 AS publicid,
                                                                                                                                    bc_contact.addressbookuid_stg       AS linkid,
                                                                                                                                    bc_contact.retired_stg              AS retired,
                                                                                                                                    bc_contact.createtime_stg           AS createtime,
                                                                                                                                    bctl_contact.name_stg               AS tl_cnt_name,
                                                                                                                                    cast(''ClaimCenter'' AS VARCHAR(100)) AS source
                                                                                                                    FROM            db_t_prod_stag.bc_account a
                                                                                                                    inner join      db_t_prod_stag.bc_accountcontact h
                                                                                                                    ON              h.accountid_stg = a.id_stg
                                                                                                                    inner join      db_t_prod_stag.bc_contact
                                                                                                                    ON              bc_contact.id_stg = h.contactid_stg
                                                                                                                    join            db_t_prod_stag.bctl_contact
                                                                                                                    ON              bctl_contact.id_stg=bc_contact.subtype_stg
                                                                                                                    left join       db_t_prod_stag.bc_accountcontactrole i
                                                                                                                    ON              i.accountcontactid_stg = h.id_stg
                                                                                                                    left join       db_t_prod_stag.bctl_accountrole j
                                                                                                                    ON              j.id_stg = i.role_stg
                                                                                                                    left outer join db_t_prod_stag.bc_user
                                                                                                                    ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                                                    WHERE           ((
                                                                                                                                                                    h.primarypayer_stg = 1)
                                                                                                                                    OR              (
                                                                                                                                                                    j.name_stg = ''Payer''))
                                                                                                                    AND             ((
                                                                                                                                                                    bc_contact.updatetime_stg>(:start_dttm)
                                                                                                                                                    AND             bc_contact.updatetime_stg <=(:END_DTTM))
                                                                                                                                    OR              (
                                                                                                                                                                    bc_user.updatetime_stg>(:start_dttm)
                                                                                                                                                    AND             bc_user.updatetime_stg <= (:END_DTTM))
                                                                                                                                    OR              (
                                                                                                                                                                    a.updatetime_stg>(:start_dttm)
                                                                                                                                                    AND             a.updatetime_stg <= (:END_DTTM)))
                                                                                                                    UNION
                                                                                                                    /*  Primary Payer and Overiding Payer Contact (this is at the Invoicestream level) */
                                                                                                                    SELECT
                                                                                                                                    CASE
                                                                                                                                                    WHEN bc_contact.updatetime_stg > a.updatetime_stg THEN bc_contact.updatetime_stg
                                                                                                                                                    ELSE a.updatetime_stg
                                                                                                                                    END AS updatetime,
                                                                                                                                    CASE
                                                                                                                                                    WHEN (
                                                                                                                                                                                    bc_contact.externalid_stg IS NULL) THEN bc_contact.publicid_stg
                                                                                                                                                    ELSE bc_contact.externalid_stg
                                                                                                                                    END                                 AS publicid,
                                                                                                                                    bc_contact.addressbookuid_stg       AS linkid,
                                                                                                                                    bc_contact.retired_stg              AS retired,
                                                                                                                                    bc_contact.createtime_stg           AS createtime,
                                                                                                                                    bctl_contact.name_stg               AS tl_cnt_name,
                                                                                                                                    cast(''ClaimCenter'' AS VARCHAR(100)) AS source
                                                                                                                    FROM            db_t_prod_stag.bc_account a
                                                                                                                    inner join      db_t_prod_stag.bc_invoicestream b
                                                                                                                    ON              a.id_stg = b.accountid_stg
                                                                                                                    inner join      db_t_prod_stag.bc_accountcontact c
                                                                                                                    ON              c.accountid_stg=a.id_stg
                                                                                                                    inner join      db_t_prod_stag.bc_contact
                                                                                                                    ON              bc_contact.id_stg = c.contactid_stg
                                                                                                                    join            db_t_prod_stag.bctl_contact
                                                                                                                    ON              bctl_contact.id_stg=bc_contact.subtype_stg
                                                                                                                    left outer join db_t_prod_stag.bc_user
                                                                                                                    ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                                                    WHERE           ((
                                                                                                                                                                    b.overridingpayer_alfa_stg IS NULL
                                                                                                                                                    AND             c.primarypayer_stg = 1)
                                                                                                                                    OR              (
                                                                                                                                                                    b.overridingpayer_alfa_stg IS NOT NULL))
                                                                                                                    AND             ((
                                                                                                                                                                    bc_contact.updatetime_stg>(:start_dttm)
                                                                                                                                                    AND             bc_contact.updatetime_stg <=(:END_DTTM))
                                                                                                                                    OR              (
                                                                                                                                                                    bc_user.updatetime_stg>(:start_dttm)
                                                                                                                                                    AND             bc_user.updatetime_stg <= (:END_DTTM))
                                                                                                                                    OR              (
                                                                                                                                                                    a.updatetime_stg>(:start_dttm)
                                                                                                                                                    AND             a.updatetime_stg <= (:END_DTTM)))
                                                                                                                    UNION
                                                                                                                    SELECT          pc_contact.updatetime_stg           AS updatetime,
                                                                                                                                    pc_contact.publicid_stg             AS publicid,
                                                                                                                                    pc_contact.addressbookuid_stg       AS linkid,
                                                                                                                                    pc_contact.retired_stg              AS retired,
                                                                                                                                    pc_contact.createtime_stg           AS createtime,
                                                                                                                                    pctl_contact.name_stg               AS tl_cnt_name,
                                                                                                                                    cast(''ClaimCenter'' AS VARCHAR(100)) AS source
                                                                                                                    FROM            db_t_prod_stag.pc_contact
                                                                                                                    left outer join db_t_prod_stag.pctl_contact
                                                                                                                    ON              pctl_contact.id_stg = pc_contact.subtype_stg
                                                                                                                    left outer join db_t_prod_stag.pc_user
                                                                                                                    ON              pc_user.contactid_stg = pc_contact.id_stg
                                                                                                                    WHERE           pctl_contact.typecode_stg = (''UserContact'')
                                                                                                                    AND
                                                                                                                                    /*  below condition added to avoid duplicates */
                                                                                                                                    pc_contact.publicid_stg NOT IN (''default_data:1'',
                                                                                                                                                                    ''systemTables:1'',
                                                                                                                                                                    ''systemTables:2'')
                                                                                                                    AND             ((
                                                                                                                                                                    pc_contact.updatetime_stg>(:start_dttm)
                                                                                                                                                    AND             pc_contact.updatetime_stg <= (:END_DTTM))
                                                                                                                                    OR              (
                                                                                                                                                                    pc_user.updatetime_stg>(:start_dttm)
                                                                                                                                                    AND             pc_user.updatetime_stg <= (:END_DTTM)))
                                                                                                                    UNION
                                                                                                                    SELECT DISTINCT cc_contact.updatetime_stg           AS updatetime,
                                                                                                                                    cc_contact.publicid_stg             AS publicid,
                                                                                                                                    cc_contact.addressbookuid_stg       AS linkid,
                                                                                                                                    cc_contact.retired_stg              AS retired,
                                                                                                                                    cc_contact.createtime_stg           AS createtime,
                                                                                                                                    cctl_contact.name_stg               AS tl_cnt_name,
                                                                                                                                    cast(''ClaimCenter'' AS VARCHAR(100)) AS source
                                                                                                                    FROM            db_t_prod_stag.cc_contact
                                                                                                                    left outer join db_t_prod_stag.cctl_contact
                                                                                                                    ON              cctl_contact.id_stg = cc_contact.subtype_stg
                                                                                                                    left outer join db_t_prod_stag.cc_user
                                                                                                                    ON              cc_user.contactid_stg = cc_contact.id_stg
                                                                                                                    WHERE           (
                                                                                                                                                    cc_contact.updatetime_stg>(:start_dttm)
                                                                                                                                    AND             cc_contact.updatetime_stg <= (:END_DTTM) )
                                                                                                                    OR              (
                                                                                                                                                    cc_user.updatetime_stg>(:start_dttm)
                                                                                                                                    AND             cc_user.updatetime_stg <=(:END_DTTM) )
                                                                                                                    UNION
                                                                                                                    SELECT          ab_abcontact.updatetime_stg            AS updatetime,
                                                                                                                                    cast(NULL AS VARCHAR(100))                publicid,
                                                                                                                                    ab_abcontact.linkid_stg                AS linkid,
                                                                                                                                    ab_abcontact.retired_stg               AS retired,
                                                                                                                                    ab_abcontact.createtime_stg            AS createtime,
                                                                                                                                    abtl_abcontact.name_stg                AS tl_cnt_name,
                                                                                                                                    cast(''ContactManager'' AS VARCHAR(100)) AS source
                                                                                                                    FROM            db_t_prod_stag.ab_abcontact
                                                                                                                    left outer join db_t_prod_stag.abtl_abcontact
                                                                                                                    ON              abtl_abcontact.id_stg = ab_abcontact.subtype_stg
                                                                                                                    WHERE           ab_abcontact.updatetime_stg>(:start_dttm)
                                                                                                                    AND             ab_abcontact.updatetime_stg <= (:END_DTTM) ) ab_abcontact
                                                                                             WHERE  addressbookuid IS NOT NULL ) x qualify row_number() over(PARTITION BY state,county,postalcode,city,addressline1,addressline2,country_id,state_id,state_typecode,country_typecode,addtype_typecode,addressbookuid,source_stg ORDER BY updatetime DESC, createtime DESC) = 1 )src
                                            left join
                                                      (
                                                                      SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                      teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                      FROM            db_t_prod_core.teradata_etl_ref_xlat
                                                                      WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm IN (''BUSN_CTGY'',
                                                                                                                               ''ORG_TYPE'',
                                                                                                                               ''PRTY_TYPE'')
                                                                      AND             teradata_etl_ref_xlat.src_idntftn_nm IN (''derived'',
                                                                                                                               ''cctl_contact.typecode'',
                                                                                                                               ''cctl_contact.name'',
                                                                                                                               ''abtl_abcontact.name'')
                                                                      AND             teradata_etl_ref_xlat.src_idntftn_sys IN (''DS'',
                                                                                                                                ''GW'')
                                                                      AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'') lkp_teradata_etl_ref_xlat_busn_ctgy_cd
                                            ON        lkp_teradata_etl_ref_xlat_busn_ctgy_cd.src_idntftn_val =tl_cnt_name
                                            left join
                                                      (
                                                                      SELECT DISTINCT indiv.indiv_prty_id AS indiv_prty_id,
                                                                                      indiv.nk_link_id    AS nk_link_id
                                                                      FROM            db_t_prod_core.indiv
                                                                      WHERE           indiv.nk_publc_id IS NULL
                                                                      AND             cast(edw_end_dttm AS DATE)=''9999-12-31'') lkp_indiv_cnt_mgr
                                            ON        lkp_indiv_cnt_mgr.nk_link_id=addressbookuid
                                            left join
                                                      (
                                                                      SELECT DISTINCT indiv.indiv_prty_id AS indiv_prty_id,
                                                                                      indiv.nk_publc_id   AS nk_publc_id
                                                                      FROM            db_t_prod_core.indiv
                                                                      WHERE           indiv.nk_publc_id IS NOT NULL)lkp_indiv_clm_ctr
                                            ON        lkp_indiv_clm_ctr.nk_publc_id=addressbookuid
                                            left join
                                                      (
                                                               SELECT   busn.busn_prty_id     AS busn_prty_id,
                                                                        busn.src_sys_cd       AS src_sys_cd,
                                                                        busn.tax_brakt_cd     AS tax_brakt_cd,
                                                                        busn.org_type_cd      AS org_type_cd,
                                                                        busn.gics_sbidstry_cd AS gics_sbidstry_cd,
                                                                        busn.lifcycl_cd       AS lifcycl_cd,
                                                                        busn.prty_type_cd     AS prty_type_cd,
                                                                        busn.busn_end_dttm    AS busn_end_dttm,
                                                                        busn.busn_strt_dttm   AS busn_strt_dttm,
                                                                        busn.inc_ind          AS inc_ind,
                                                                        busn.edw_strt_dttm    AS edw_strt_dttm,
                                                                        busn.edw_end_dttm     AS edw_end_dttm,
                                                                        busn.busn_ctgy_cd     AS busn_ctgy_cd,
                                                                        busn.nk_busn_cd       AS nk_busn_cd
                                                               FROM     db_t_prod_core.busn qualify row_number () over (PARTITION BY nk_busn_cd,busn_ctgy_cd ORDER BY edw_end_dttm DESC )=1) lkp_busn
                                            ON        lkp_busn.busn_ctgy_cd=lkp_teradata_etl_ref_xlat_busn_ctgy_cd.tgt_idntftn_val
                                            AND       lkp_busn.nk_busn_cd=addressbookuid
                                            left join
                                                      (
                                                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''INTRNL_ORG_TYPE''
                                                             AND    teradata_etl_ref_xlat.src_idntftn_nm= ''derived''
                                                             AND    teradata_etl_ref_xlat.src_idntftn_sys=''DS''
                                                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'')teradata_etl_ref_xlat_intrnl_org_type
                                            ON        teradata_etl_ref_xlat_intrnl_org_type.src_idntftn_val=org_type
                                            left join
                                                      (
                                                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''INTRNL_ORG_SBTYPE'') teradata_etl_ref_xlat_intrnl_org_sbtype
                                            ON        teradata_etl_ref_xlat_intrnl_org_sbtype.src_idntftn_val=org_subtype
                                            left join
                                                      (
                                                             SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                    teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                             FROM   db_t_prod_core.teradata_etl_ref_xlat
                                                             WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''SRC_SYS''
                                                             AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'')teradata_etl_ref_xlat_src_sys
                                            ON        teradata_etl_ref_xlat_src_sys.src_idntftn_val=sys_src_cd
                                            left join
                                                      (
                                                             SELECT intrnl_org.intrnl_org_prty_id   AS intrnl_org_prty_id,
                                                                    intrnl_org.intrnl_org_type_cd   AS intrnl_org_type_cd,
                                                                    intrnl_org.intrnl_org_sbtype_cd AS intrnl_org_sbtype_cd,
                                                                    intrnl_org.intrnl_org_num       AS intrnl_org_num,
                                                                    intrnl_org.src_sys_cd           AS src_sys_cd
                                                             FROM   db_t_prod_core.intrnl_org
                                                                    /* qualify row_number () over (partition by INTRNL_ORG_NUM,INTRNL_ORG_TYPE_CD,INTRNL_ORG_SBTYPE_CD,SRC_SYS_CD order by EDW_END_DTTM desc)=1 */
                                                             WHERE  cast(edw_end_dttm AS DATE)=''9999-12-31'')lkp_intrnl_org
                                            ON        teradata_etl_ref_xlat_intrnl_org_type.tgt_idntftn_val=intrnl_org_type_cd
                                            AND       teradata_etl_ref_xlat_intrnl_org_sbtype.tgt_idntftn_val=intrnl_org_sbtype_cd
                                            AND       teradata_etl_ref_xlat_src_sys.tgt_idntftn_val=lkp_intrnl_org.src_sys_cd
                                            AND       intrnl_org_num=org_key
                                            left join
                                                      (
                                                             SELECT ctry.ctry_id                AS ctry_id,
                                                                    ctry.geogrcl_area_name      AS geogrcl_area_name,
                                                                    ctry.geogrcl_area_desc      AS geogrcl_area_desc,
                                                                    ctry.geogrcl_area_shrt_name AS geogrcl_area_shrt_name
                                                             FROM   db_t_prod_core.ctry
                                                             WHERE  cast(edw_end_dttm AS DATE)=''9999-12-31'') lkp_ctry
                                            ON        lkp_ctry.geogrcl_area_shrt_name=country_typecode
                                            left join
                                                      (
                                                             SELECT terr.terr_id                AS terr_id,
                                                                    terr.ctry_id                AS ctry_id,
                                                                    terr.geogrcl_area_shrt_name AS geogrcl_area_shrt_name
                                                             FROM   db_t_prod_core.terr
                                                             WHERE  cast(edw_end_dttm AS DATE)=''9999-12-31'')lkp_terr
                                            ON        lkp_terr.geogrcl_area_shrt_name=state_typecode
                                            AND       lkp_terr.ctry_id=lkp_ctry.ctry_id
                                            left join
                                                      (
                                                             SELECT city.city_id                AS city_id,
                                                                    city.terr_id                AS terr_id,
                                                                    city.geogrcl_area_shrt_name AS geogrcl_area_shrt_name
                                                             FROM   db_t_prod_core.city
                                                             WHERE  cast(edw_end_dttm AS DATE)=''9999-12-31'')lkp_city
                                            ON        lkp_city.terr_id=lkp_terr.terr_id
                                            AND       lkp_city.geogrcl_area_shrt_name=city
                                            left join
                                                      (
                                                               SELECT   cnty.cnty_id                AS cnty_id,
                                                                        cnty.terr_id                AS terr_id,
                                                                        cnty.geogrcl_area_shrt_name AS geogrcl_area_shrt_name
                                                               FROM     db_t_prod_core.cnty
                                                               WHERE    cast(edw_end_dttm AS DATE)=''9999-12-31'' qualify row_number() over(PARTITION BY geogrcl_area_shrt_name,cnty.terr_id ORDER BY edw_end_dttm DESC, edw_strt_dttm ASC)=1 )lkp_cnty
                                            ON        upper(lkp_cnty.geogrcl_area_shrt_name)=upper(county )
                                            AND       lkp_cnty.terr_id=lkp_terr.terr_id
                                            left join
                                                      (
                                                             SELECT postl_cd.postl_cd_id  AS postl_cd_id,
                                                                    postl_cd.ctry_id      AS ctry_id,
                                                                    postl_cd.postl_cd_num AS postl_cd_num
                                                             FROM   db_t_prod_core.postl_cd
                                                             WHERE  cast(edw_end_dttm AS DATE)=''9999-12-31'') lkp_postl_cd
                                            ON        lkp_postl_cd.ctry_id=lkp_ctry.ctry_id
                                            AND       postl_cd_num=postalcode
                                            left join
                                                      (
                                                             SELECT street_addr.street_addr_id      AS street_addr_id,
                                                                    street_addr.dwlng_type_cd       AS dwlng_type_cd,
                                                                    street_addr.carier_rte_txt      AS carier_rte_txt,
                                                                    street_addr.sptl_pnt            AS sptl_pnt,
                                                                    street_addr.loctr_sbtype_cd     AS loctr_sbtype_cd,
                                                                    street_addr.addr_sbtype_cd      AS addr_sbtype_cd,
                                                                    street_addr.geocode_sts_type_cd AS geocode_sts_type_cd,
                                                                    street_addr.addr_stdzn_type_cd  AS addr_stdzn_type_cd,
                                                                    street_addr.prcs_id             AS prcs_id,
                                                                    street_addr.edw_strt_dttm       AS edw_strt_dttm,
                                                                    street_addr.edw_end_dttm        AS edw_end_dttm,
                                                                    street_addr.addr_ln_1_txt       AS addr_ln_1_txt,
                                                                    street_addr.addr_ln_2_txt       AS addr_ln_2_txt,
                                                                    street_addr.addr_ln_3_txt       AS addr_ln_3_txt,
                                                                    street_addr.city_id             AS city_id,
                                                                    street_addr.terr_id             AS terr_id,
                                                                    street_addr.postl_cd_id         AS postl_cd_id,
                                                                    street_addr.ctry_id             AS ctry_id,
                                                                    street_addr.cnty_id             AS cnty_id
                                                             FROM   db_t_prod_core.street_addr
                                                                    /* qualify row_number () over (partition by ADDR_LN_1_TXT,ADDR_LN_2_TXT,ADDR_LN_3_TXT, CITY_ID ,TERR_ID,POSTL_CD_ID,CTRY_ID ,CNTY_ID order by EDW_END_DTTM desc)=1 */
                                                             WHERE  cast(edw_end_dttm AS DATE)=''9999-12-31'')lkp_street_addr
                                            ON        upper(lkp_street_addr.addr_ln_1_txt)=upper(addressline1)
                                            AND       upper(coalesce(lkp_street_addr.addr_ln_2_txt,''''))=upper( coalesce(addressline2,'''') )
                                            AND       lkp_street_addr.city_id=lkp_city.city_id
                                            AND       lkp_street_addr.terr_id=lkp_terr.terr_id
                                            AND       lkp_street_addr.ctry_id=lkp_ctry.ctry_id
                                            AND       lkp_street_addr.postl_cd_id=lkp_postl_cd.postl_cd_id
                                            AND       coalesce( lkp_street_addr.cnty_id,''~'')=coalesce(lkp_cnty.cnty_id,''~'') )
                        SELECT    src.loc_id,
                                  src.prty_id,
                                  CASE
                                            WHEN gcid_loc_id=2
                                            AND       src.prty_addr_usge_type_cd=''billing'' THEN ''PRTY_ADDR_USGE_TYPE9''
                                            WHEN gcid_loc_id=2
                                            AND       src.prty_addr_usge_type_cd=''business''THEN ''PRTY_ADDR_USGE_TYPE29''
                                            WHEN gcid_loc_id=2
                                            AND       src.prty_addr_usge_type_cd=''home''THEN ''PRTY_ADDR_USGE_TYPE14''
                                            WHEN gcid_loc_id=2
                                            AND       src.prty_addr_usge_type_cd=''Mailing_alfa''THEN ''PRTY_ADDR_USGE_TYPE19''
                                            WHEN gcid_loc_id=2
                                            AND       src.prty_addr_usge_type_cd=''other''THEN ''PRTY_ADDR_USGE_TYPE24''
                                            WHEN gcid_loc_id=3
                                            AND       src.prty_addr_usge_type_cd=''billing''THEN ''PRTY_ADDR_USGE_TYPE11''
                                            WHEN gcid_loc_id=3
                                            AND       src.prty_addr_usge_type_cd=''business''THEN ''PRTY_ADDR_USGE_TYPE31 ''
                                            WHEN gcid_loc_id=3
                                            AND       src.prty_addr_usge_type_cd=''home''THEN ''PRTY_ADDR_USGE_TYPE16''
                                            WHEN gcid_loc_id=3
                                            AND       src.prty_addr_usge_type_cd=''Mailing_alfa''THEN ''PRTY_ADDR_USGE_TYPE21''
                                            WHEN gcid_loc_id=3
                                            AND       src.prty_addr_usge_type_cd=''other''THEN ''PRTY_ADDR_USGE_TYPE26''
                                            WHEN gcid_loc_id=4
                                            AND       src.prty_addr_usge_type_cd=''billing''THEN ''PRTY_ADDR_USGE_TYPE12''
                                            WHEN gcid_loc_id=4
                                            AND       src.prty_addr_usge_type_cd=''business''THEN ''PRTY_ADDR_USGE_TYPE32''
                                            WHEN gcid_loc_id=4
                                            AND       src.prty_addr_usge_type_cd=''home''THEN ''PRTY_ADDR_USGE_TYPE17''
                                            WHEN gcid_loc_id=4
                                            AND       src.prty_addr_usge_type_cd=''Mailing_alfa''THEN ''PRTY_ADDR_USGE_TYPE22''
                                            WHEN gcid_loc_id=4
                                            AND       src.prty_addr_usge_type_cd=''other''THEN ''PRTY_ADDR_USGE_TYPE27''
                                            WHEN gcid_loc_id=5
                                            AND       src.prty_addr_usge_type_cd=''billing''THEN ''PRTY_ADDR_USGE_TYPE10''
                                            WHEN gcid_loc_id=5
                                            AND       src.prty_addr_usge_type_cd=''business''THEN ''PRTY_ADDR_USGE_TYPE30''
                                            WHEN gcid_loc_id=5
                                            AND       src.prty_addr_usge_type_cd=''home''THEN ''PRTY_ADDR_USGE_TYPE15''
                                            WHEN gcid_loc_id=5
                                            AND       src.prty_addr_usge_type_cd=''Mailing_alfa''THEN ''PRTY_ADDR_USGE_TYPE20''
                                            WHEN gcid_loc_id=5
                                            AND       src.prty_addr_usge_type_cd=''other''THEN ''PRTY_ADDR_USGE_TYPE25''
                                            WHEN gcid_loc_id=6
                                            AND       src.prty_addr_usge_type_cd=''billing''THEN ''PRTY_ADDR_USGE_TYPE13''
                                            WHEN gcid_loc_id=6
                                            AND       src.prty_addr_usge_type_cd=''business''THEN ''PRTY_ADDR_USGE_TYPE33''
                                            WHEN gcid_loc_id=6
                                            AND       src.prty_addr_usge_type_cd=''home''THEN ''PRTY_ADDR_USGE_TYPE18''
                                            WHEN gcid_loc_id=6
                                            AND       src.prty_addr_usge_type_cd=''Mailing_alfa''THEN ''PRTY_ADDR_USGE_TYPE23''
                                            WHEN gcid_loc_id=6
                                            AND       src.prty_addr_usge_type_cd=''other''THEN ''PRTY_ADDR_USGE_TYPE28''
                                            WHEN src.prty_addr_usge_type_cd=''PRTY_ADDR_USGE_TYPE3''THEN ''PRTY_ADDR_USGE_TYPE3''
                                            WHEN gcid_loc_id=2
                                            AND       src.prty_addr_usge_type_cd=''PRTY_ADDR_USGE_TYPE8''THEN ''PRTY_ADDR_USGE_TYPE29''
                                            WHEN gcid_loc_id=3
                                            AND       src.prty_addr_usge_type_cd=''PRTY_ADDR_USGE_TYPE8''THEN ''PRTY_ADDR_USGE_TYPE31''
                                            WHEN gcid_loc_id=4
                                            AND       src.prty_addr_usge_type_cd=''PRTY_ADDR_USGE_TYPE8''THEN ''PRTY_ADDR_USGE_TYPE32''
                                            WHEN gcid_loc_id=5
                                            AND       src.prty_addr_usge_type_cd=''PRTY_ADDR_USGE_TYPE8''THEN ''PRTY_ADDR_USGE_TYPE30''
                                            WHEN gcid_loc_id=6
                                            AND       src.prty_addr_usge_type_cd=''PRTY_ADDR_USGE_TYPE8''THEN ''PRTY_ADDR_USGE_TYPE33''
                                            ELSE ''UNK''
                                  END                           prty_addr_usge_type_cd_new,
                                  lkp_prty_addr.tgt_idntftn_val prty_addr_usge_type_cd_new1,
                                  updatetime,
                                  createtime,
                                  retired,
                                  rnk ,
                                  row_number() over(PARTITION BY src.prty_id, prty_addr_usge_type_cd_new ORDER BY updatetime ASC,ind) rankindex,
                                  (cast(src.loc_id AS                                                 VARCHAR(10))
                                            ||cast(src.createtime AS                                  VARCHAR(30))
                                            ||cast(cast(''9999-12-31 23:59:59.999999'' AS timestamp) AS VARCHAR(30))) chksum_src,
                                  (cast(lkp_tgt.loc_id AS                                             VARCHAR(10))
                                            ||cast(lkp_tgt.prty_addr_strt_dttm AS                     VARCHAR(30))
                                            ||cast(lkp_tgt.prty_addr_end_dttm AS                      VARCHAR(30))) chksum_tgt,
                                  CASE
                                            WHEN lkp_tgt.prty_id IS NULL THEN ''I''
                                            WHEN (
                                                                cast(lkp_tgt.loc_id AS                                    VARCHAR(10))
                                                                          ||cast(lkp_tgt.prty_addr_strt_dttm AS           VARCHAR(30))
                                                                          ||cast(lkp_tgt.prty_addr_end_dttm AS            VARCHAR(30))) <> (cast(src.loc_id AS VARCHAR(10))
                                                                ||cast(src.createtime AS                                  VARCHAR(30))
                                                                ||cast(cast(''9999-12-31 23:59:59.999999'' AS timestamp) AS VARCHAR(30))) THEN ''U''
                                            ELSE ''R''
                                  END                     out_flag,
                                  lkp_tgt.edw_end_dttm  AS lkp_edw_end_dttm,
                                  lkp_tgt.edw_strt_dttm AS lkp_edw_strt_dttm
                        FROM      (
                                         SELECT ctry_id        loc_id ,
                                                street_addr_id ind,
                                                1              gcid_loc_id,
                                                prty_id,
                                                addtype_typecode prty_addr_usge_type_cd,
                                                updatetime,
                                                createtime,
                                                retired,
                                                rnk
                                         FROM   prty_addr_temp
                                         UNION
                                         SELECT terr_id,
                                                street_addr_id,
                                                2 gcid_loc_id ,
                                                prty_id,
                                                addtype_typecode,
                                                updatetime,
                                                createtime,
                                                retired,
                                                rnk
                                         FROM   prty_addr_temp
                                         UNION
                                         SELECT cnty_id,
                                                street_addr_id,
                                                3 gcid_loc_id,
                                                prty_id,
                                                addtype_typecode,
                                                updatetime,
                                                createtime,
                                                retired,
                                                rnk
                                         FROM   prty_addr_temp
                                         UNION
                                         SELECT postl_cd_id,
                                                street_addr_id,
                                                4 gcid_loc_id,
                                                prty_id,
                                                addtype_typecode,
                                                updatetime,
                                                createtime,
                                                retired,
                                                rnk
                                         FROM   prty_addr_temp
                                         UNION
                                         SELECT city_id,
                                                street_addr_id,
                                                5 gcid_loc_id,
                                                prty_id,
                                                addtype_typecode,
                                                updatetime,
                                                createtime,
                                                retired,
                                                rnk
                                         FROM   prty_addr_temp
                                         UNION
                                         SELECT street_addr_id,
                                                street_addr_id ind,
                                                6              gcid_loc_id,
                                                prty_id,
                                                addtype_typecode,
                                                updatetime,
                                                createtime,
                                                retired,
                                                rnk
                                         FROM   prty_addr_temp )src
                        left join
                                  (
                                         SELECT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                         FROM   db_t_prod_core.teradata_etl_ref_xlat
                                         WHERE  teradata_etl_ref_xlat.tgt_idntftn_nm= ''PRTY_ADDR_USGE_TYPE''
                                         AND    teradata_etl_ref_xlat.src_idntftn_nm IN (''pctl_addresstype.typecode'' ,
                                                                                         ''derived'')
                                         AND    teradata_etl_ref_xlat.src_idntftn_sys IN (''GW'' ,
                                                                                          ''DS'')
                                         AND    teradata_etl_ref_xlat.expn_dt=''9999-12-31'') lkp_prty_addr
                        ON        prty_addr_usge_type_cd_new= lkp_prty_addr.src_idntftn_val
                        left join
                                  (
                                           SELECT   prty_addr.prty_addr_strt_dttm    AS prty_addr_strt_dttm,
                                                    prty_addr.prty_addr_end_dttm     AS prty_addr_end_dttm,
                                                    prty_addr.edw_strt_dttm          AS edw_strt_dttm,
                                                    prty_addr.edw_end_dttm           AS edw_end_dttm,
                                                    prty_addr.loc_id                 AS loc_id,
                                                    prty_addr.prty_addr_usge_type_cd AS prty_addr_usge_type_cd,
                                                    prty_addr.prty_id                AS prty_id
                                           FROM     db_t_prod_core.prty_addr qualify row_number() over(PARTITION BY prty_addr_usge_type_cd,prty_id ORDER BY edw_end_dttm DESC) = 1)lkp_tgt
                        ON        lkp_tgt.prty_id=src.prty_id
                        AND       lkp_tgt.prty_addr_usge_type_cd=prty_addr_usge_type_cd_new1
                        WHERE     src.loc_id IS NOT NULL
                        AND       src.prty_id IS NOT NULL qualify rankindex = 1
                        AND       gcid_loc_id <> 1 ) src ) );
  -- Component exp_hold_data, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_hold_data AS
  (
         SELECT sq_prty_add_loctr_x.loc_id                                             AS loc_id,
                sq_prty_add_loctr_x.prty_id                                            AS prty_id,
                sq_prty_add_loctr_x.prty_addr_usge_type_cd_new                         AS prty_addr_usge_type_cd,
                sq_prty_add_loctr_x.updatetime                                         AS updatetime,
                sq_prty_add_loctr_x.createtime                                         AS createtime,
                sq_prty_add_loctr_x.retired                                            AS retired,
                sq_prty_add_loctr_x.rnk                                                AS rnk,
                sq_prty_add_loctr_x.chksum_src                                         AS chksum_src,
                sq_prty_add_loctr_x.chksum_tgt                                         AS chksum_tgt,
                sq_prty_add_loctr_x.out_flag                                           AS out_flag,
                to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) AS out_prty_addr_end_dttm,
                sq_prty_add_loctr_x.lkp_edw_end_dttm                                   AS lkp_edw_end_dttm,
                sq_prty_add_loctr_x.lkp_edw_strt_dttm                                  AS lkp_edw_strt_dttm,
                sq_prty_add_loctr_x.source_record_id
         FROM   sq_prty_add_loctr_x );
  -- Component exp_set_insupd_flag, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_set_insupd_flag AS
  (
         SELECT exp_hold_data.prty_addr_usge_type_cd AS prty_addr_usge_type_cd,
                exp_hold_data.loc_id                 AS loc_id,
                exp_hold_data.prty_id                AS prty_id,
                exp_hold_data.updatetime             AS o_updatetime,
                exp_hold_data.out_prty_addr_end_dttm AS out_prty_addr_end_dt,
                exp_hold_data.out_flag               AS out_ins_upd,
                exp_hold_data.lkp_edw_strt_dttm      AS lkp_edw_strt_dttm,
                exp_hold_data.lkp_edw_end_dttm       AS lkp_edw_end_dttm,
                exp_hold_data.retired                AS retired,
                exp_hold_data.rnk                    AS rank,
                exp_hold_data.createtime             AS createtime,
                exp_hold_data.source_record_id
         FROM   exp_hold_data );
  -- Component rtr_ins_upd_prty_addr_INSERT, Type ROUTER Output Group INSERT
  create or replace table rtr_ins_upd_prty_addr_INSERT as
  SELECT exp_set_insupd_flag.prty_addr_usge_type_cd AS prty_addr_usge_type_cd,
         exp_set_insupd_flag.loc_id                 AS loc_id,
         exp_set_insupd_flag.prty_id                AS prty_id,
         exp_set_insupd_flag.createtime             AS out_prty_addr_strt_dt,
         exp_set_insupd_flag.out_prty_addr_end_dt   AS out_prty_addr_end_dt,
         exp_set_insupd_flag.lkp_edw_strt_dttm      AS lkp_edw_strt_dttm,
         exp_set_insupd_flag.o_updatetime           AS trans_strt_dttm,
         exp_set_insupd_flag.retired                AS retired,
         exp_set_insupd_flag.out_ins_upd            AS out_ins_upd,
         exp_set_insupd_flag.rank                   AS rank,
         exp_set_insupd_flag.createtime             AS createtime,
         exp_set_insupd_flag.lkp_edw_end_dttm       AS lkp_edw_end_dttm,
         exp_set_insupd_flag.source_record_id
  FROM   exp_set_insupd_flag
  WHERE  ( (
                       exp_set_insupd_flag.out_ins_upd = ''I''
                AND    (
                              exp_set_insupd_flag.loc_id <> 9999
                       AND    exp_set_insupd_flag.prty_id <> 9999
                       AND    exp_set_insupd_flag.loc_id IS NOT NULL
                       AND    exp_set_insupd_flag.prty_id IS NOT NULL
                       AND    exp_set_insupd_flag.prty_addr_usge_type_cd <> ''UNK'' )
                OR     (
                              exp_set_insupd_flag.out_ins_upd = ''U''
                       AND    exp_set_insupd_flag.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                       AND    exp_set_insupd_flag.prty_addr_usge_type_cd <> ''UNK'' ) )
         OR     (
                       exp_set_insupd_flag.retired = 0
                AND    exp_set_insupd_flag.lkp_edw_end_dttm != to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                AND    exp_set_insupd_flag.prty_addr_usge_type_cd <> ''UNK'' ) )
  AND    exp_set_insupd_flag.loc_id IS NOT NULL
  AND    exp_set_insupd_flag.prty_id IS NOT NULL;
  
  -- Component rtr_ins_upd_prty_addr_RETIRED, Type ROUTER Output Group RETIRED
  create or replace table rtr_ins_upd_prty_addr_RETIRED as
  SELECT exp_set_insupd_flag.prty_addr_usge_type_cd AS prty_addr_usge_type_cd,
         exp_set_insupd_flag.loc_id                 AS loc_id,
         exp_set_insupd_flag.prty_id                AS prty_id,
         exp_set_insupd_flag.createtime             AS out_prty_addr_strt_dt,
         exp_set_insupd_flag.out_prty_addr_end_dt   AS out_prty_addr_end_dt,
         exp_set_insupd_flag.lkp_edw_strt_dttm      AS lkp_edw_strt_dttm,
         exp_set_insupd_flag.o_updatetime           AS trans_strt_dttm,
         exp_set_insupd_flag.retired                AS retired,
         exp_set_insupd_flag.out_ins_upd            AS out_ins_upd,
         exp_set_insupd_flag.rank                   AS rank,
         exp_set_insupd_flag.createtime             AS createtime,
         exp_set_insupd_flag.lkp_edw_end_dttm       AS lkp_edw_end_dttm,
         exp_set_insupd_flag.source_record_id
  FROM   exp_set_insupd_flag
  WHERE  (
                exp_set_insupd_flag.out_ins_upd = ''R''
         AND    exp_set_insupd_flag.retired != 0
         AND    exp_set_insupd_flag.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
         AND    exp_set_insupd_flag.loc_id IS NOT NULL
         AND    exp_set_insupd_flag.prty_addr_usge_type_cd <> ''UNK'' ) -- o_flag = ''R''
  AND    exp_set_insupd_flag.retired != 0
  AND    exp_set_insupd_flag.lkp_edw_end_dttm = to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
  
  -- Component upd_str_upd_retired, Type UPDATE
  CREATE
  OR
  replace TEMPORARY TABLE upd_str_upd_retired AS
  (
         /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
         SELECT rtr_ins_upd_prty_addr_retired.prty_addr_usge_type_cd AS prty_addr_usge_type_cd3,
                rtr_ins_upd_prty_addr_retired.out_prty_addr_strt_dt  AS out_prty_addr_strt_dt3,
                rtr_ins_upd_prty_addr_retired.loc_id                 AS loc_id3,
                rtr_ins_upd_prty_addr_retired.prty_id                AS prty_id3,
                rtr_ins_upd_prty_addr_retired.lkp_edw_strt_dttm      AS lkp_edw_strt_dttm13,
                rtr_ins_upd_prty_addr_retired.out_prty_addr_strt_dt  AS out_prty_addr_end_dt3,
                NULL                                                 AS lkp_prty_addr_strt_dt31,
                rtr_ins_upd_prty_addr_retired.trans_strt_dttm        AS trans_strt_dttm4,
                1                                                    AS update_strategy_action,
				rtr_ins_upd_prty_addr_retired.source_record_id
         FROM   rtr_ins_upd_prty_addr_retired );
  -- Component retired, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE retired AS
  (
         SELECT dateadd ( second ,-1,  upd_str_upd_retired.trans_strt_dttm4 ) AS trans_end_date,
                dateadd ( second , -1, current_timestamp  ) AS edw_end_dttm,
                upd_str_upd_retired.source_record_id
         FROM   upd_str_upd_retired );
  -- Component tgt_prty_addr_upd_retired, Type TARGET
  /* Perform Updates */
  merge
  INTO         db_t_prod_core.prty_addr
  USING        upd_str_upd_retired 
  ON (
                            update_strategy_action = 1
               AND          prty_addr.prty_addr_usge_type_cd = upd_str_upd_retired.prty_addr_usge_type_cd3
               AND          prty_addr.loc_id = upd_str_upd_retired.loc_id3
               AND          prty_addr.prty_id = upd_str_upd_retired.prty_id3
               AND          prty_addr.edw_strt_dttm = upd_str_upd_retired.lkp_edw_strt_dttm13)
  WHEN matched THEN
  UPDATE
  SET    edw_end_dttm = dateadd ( second , -1, current_timestamp  ),
         trans_end_dttm = dateadd ( second ,-1,  upd_str_upd_retired.trans_strt_dttm4 ) ;
  
  -- Component exp_prty_addr_insert, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_prty_addr_insert AS
  (
         SELECT rtr_ins_upd_prty_addr_insert.prty_addr_usge_type_cd                                          AS prty_addr_usge_type_cd1,
                rtr_ins_upd_prty_addr_insert.loc_id                                                          AS loc_id1,
                rtr_ins_upd_prty_addr_insert.prty_id                                                         AS prty_id1,
                :PRCS_ID                                                                                     AS prcs_id1,
                rtr_ins_upd_prty_addr_insert.out_prty_addr_end_dt                                            AS out_prty_addr_end_dt1,
                dateadd ( second , ( 2 * ( rtr_ins_upd_prty_addr_insert.rank - 1 ) ), current_timestamp  ) AS v_edw_strt_dttm,
                v_edw_strt_dttm                                                                              AS out_edw_strt_dttm,
                CASE
                       WHEN rtr_ins_upd_prty_addr_insert.retired != 0 THEN v_edw_strt_dttm
                       ELSE to_timestamp_ntz ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
                END                                          AS out_edw_end_dttm,
                rtr_ins_upd_prty_addr_insert.trans_strt_dttm AS trans_strt_dttm1,
                CASE
                       WHEN rtr_ins_upd_prty_addr_insert.retired != 0 THEN rtr_ins_upd_prty_addr_insert.trans_strt_dttm
                       ELSE to_timestamp_ntz ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                END                                     AS trns_end_dttm,
                rtr_ins_upd_prty_addr_insert.createtime AS createtime1,
                rtr_ins_upd_prty_addr_insert.source_record_id
         FROM   rtr_ins_upd_prty_addr_insert );
  -- Component tgt_prty_addr_ins, Type TARGET
  INSERT INTO db_t_prod_core.prty_addr
              (
                          prty_addr_usge_type_cd,
                          prty_addr_strt_dttm,
                          loc_id,
                          prty_id,
                          prty_addr_end_dttm,
                          prcs_id,
                          edw_strt_dttm,
                          edw_end_dttm,
                          trans_strt_dttm,
                          trans_end_dttm
              )
  SELECT exp_prty_addr_insert.prty_addr_usge_type_cd1 AS prty_addr_usge_type_cd,
         exp_prty_addr_insert.createtime1             AS prty_addr_strt_dttm,
         exp_prty_addr_insert.loc_id1                 AS loc_id,
         exp_prty_addr_insert.prty_id1                AS prty_id,
         exp_prty_addr_insert.out_prty_addr_end_dt1   AS prty_addr_end_dttm,
         exp_prty_addr_insert.prcs_id1                AS prcs_id,
         exp_prty_addr_insert.out_edw_strt_dttm       AS edw_strt_dttm,
         exp_prty_addr_insert.out_edw_end_dttm        AS edw_end_dttm,
         exp_prty_addr_insert.trans_strt_dttm1        AS trans_strt_dttm,
         exp_prty_addr_insert.trns_end_dttm           AS trans_end_dttm
  FROM   exp_prty_addr_insert;
  
  -- PIPELINE END FOR 1
  -- Component tgt_prty_addr_ins, Type Post SQL
  UPDATE db_t_prod_core.prty_addr
    SET    trans_end_dttm= a.lead,
         edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT prty_addr_usge_type_cd,
                                         prty_id,
                                         edw_strt_dttm,
                                         max(trans_strt_dttm) over (PARTITION BY prty_addr_usge_type_cd, prty_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead,
                                         max(edw_strt_dttm) over (PARTITION BY prty_addr_usge_type_cd, prty_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.prty_addr --where LOC_ID <> 9999 and  prty_id <> 9999
         ) a

  WHERE  prty_addr.edw_strt_dttm = a.edw_strt_dttm
         --and PRTY_ADDR.LOC_ID=A.LOC_ID
  AND    prty_addr.prty_addr_usge_type_cd=a.prty_addr_usge_type_cd
  AND    prty_addr.prty_id=a.prty_id
  AND    prty_addr.trans_strt_dttm <>prty_addr.trans_end_dttm
  AND    lead IS NOT NULL;
  
  -- PIPELINE START FOR 2
  -- Component sq_prty_add_loctr_x1, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_prty_add_loctr_x1 AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1 AS org_key,
                $2 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                         SELECT publicid_stg
                                         FROM   db_t_prod_stag.pc_policyperiod
                                         WHERE  1=2 ) src ) );
  -- Component tgt_prty_addr_upd_post_sql, Type TARGET
  INSERT INTO db_t_prod_core.prty_addr
              (
                          prty_addr_usge_type_cd
              )
  SELECT sq_prty_add_loctr_x1.org_key AS prty_addr_usge_type_cd
  FROM   sq_prty_add_loctr_x1;
  
  -- PIPELINE END FOR 2
  -- Component tgt_prty_addr_upd_post_sql, Type Post SQL
  UPDATE db_t_prod_core.prty_addr
    SET    trans_end_dttm= a.lead,
         edw_end_dttm=a.lead1
  FROM   (
                         SELECT DISTINCT prty_addr_usge_type_cd,
                                         prty_id,
                                         edw_strt_dttm,
                                         max(trans_strt_dttm) over (PARTITION BY prty_addr_usge_type_cd, prty_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 second'' AS lead,
                                         max(edw_strt_dttm) over (PARTITION BY prty_addr_usge_type_cd, prty_id ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 second'' AS lead1
                         FROM            db_t_prod_core.prty_addr --where LOC_ID <> 9999 and  prty_id <> 9999
         ) a

  WHERE  prty_addr.edw_strt_dttm = a.edw_strt_dttm
         --and PRTY_ADDR.LOC_ID=A.LOC_ID
  AND    prty_addr.prty_addr_usge_type_cd=a.prty_addr_usge_type_cd
  AND    prty_addr.prty_id=a.prty_id
  AND    prty_addr.trans_strt_dttm <>prty_addr.trans_end_dttm
  AND    lead IS NOT NULL;

END;
';