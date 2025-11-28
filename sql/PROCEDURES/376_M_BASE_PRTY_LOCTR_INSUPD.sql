-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_PRTY_LOCTR_INSUPD("RUN_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
  DECLARE
    start_dttm timestamp;
    end_dttm timestamp;
    prcs_id INTEGER;
  BEGIN
    start_dttm := current_timestamp();
    end_dttm := current_timestamp();
    prcs_id :=1;
    -- Component sq_prty_add_loctr_x, Type SOURCE
    CREATE
    OR
    replace TEMPORARY TABLE sq_prty_add_loctr_x AS
    (
           SELECT
                  /* adding column aliases to ensure proper downstream column references */
                  $1  AS lkp_loc_id,
                  $2  AS lkp_loctr_usge_type_cd,
                  $3  AS lkp_prty_loctr_strt_dttm,
                  $4  AS lkp_prty_id,
                  $5  AS lkp_data_qlty_cd,
                  $6  AS lkp_edw_strt_dttm,
                  $7  AS lkp_edw_end_dttm,
                  $8  AS in_loc_id,
                  $9  AS in_prty_addr_usge_type_cd,
                  $10 AS in_prty_id,
                  $11 AS in_prty_loctr_strt_dttm,
                  $12 AS in_data_qlty_cd,
                  $13 AS in_edw_strt_dttm,
                  $14 AS in_edw_end_dttm,
                  $15 AS in_prcs_id,
                  $16 AS var_orig_chksm,
                  $17 AS var_calc_chksm,
                  $18 AS calc_ins_upd,
                  $19 AS busn_end_dttm,
                  $20 AS retired,
                  $21 AS trans_strt_dttm,
                  $22 AS out_createtime,
                  $23 AS rankindex,
                  $24 AS source_record_id
           FROM   (
                           SELECT   src.*,
                                    row_number() over (ORDER BY 1) AS source_record_id
                           FROM     ( WITH terr AS
                                    (
                                              SELECT
                                                        CASE
                                                                  WHEN out_prty_id IS NULL THEN lkp_intrnl_org.intrnl_org_prty_id
                                                                  ELSE out_prty_id
                                                        END AS var_prty_id,
                                                        CASE
                                                                  WHEN var_prty_id IS NOT NULL THEN var_prty_id
                                                                  ELSE 9999
                                                        END                         AS in_prty_id,
                                                        lkp_tlphn_num1.tlphn_num_id AS loc_id_in7,
                                                        lkp_tlphn_num2.tlphn_num_id AS loc_id_in8,
                                                        lkp_tlphn_num3.tlphn_num_id AS loc_id_in9,
                                                        lkp_tlphn_num4.tlphn_num_id AS loc_id_in10,
                                                        out_terr_id                 AS loc_id_in2,
                                                        out_cnty_id                 AS loc_id_in3,
                                                        out_postl_cd_id             AS loc_id_in4,
                                                        out_city_id                 AS loc_id_in5,
                                                        out_street_addr_id          AS loc_id_in6,
                                                        prty_addr_start_datetime1   AS prty_addr_start_datetime_in,
                                                        prty_addr_start_datetime,
                                                        out_prty_addr_usge_type_cd,
                                                        state,
                                                        out_ctry_id AS loc_id_in1,
                                                        state_id,
                                                        state_typecode,
                                                        ctry_typecode,
                                                        county,
                                                        postalcode,
                                                        city,
                                                        addressline1,
                                                        addressline2,
                                                        addrtype_typecode AS prty_addr_usge_type_cd_in,
                                                        addressbookuid,
                                                        out_prty_id,
                                                        org_key    AS in_org_key,
                                                        org_type   AS in_org_type,
                                                        org_sbtype AS in_org_sbtype,
                                                        src_sys,
                                                        retired AS retired_in,
                                                        primaryphone,
                                                        cellphone,
                                                        homephone,
                                                        workphone,
                                                        updatetime                                  AS updatetime_in,
                                                        cast(cast(updatetime AS timestamp) AS DATE) AS updatetime1,
                                                        createtime                                  AS createtime_in
                                              FROM      (
                                                        (
                                                                  SELECT    lkp_street_addr.street_addr_id                              AS out_street_addr_id,
                                                                            xlat_intrnl_org_type.tgt_idntftn_val                        AS v_org_type,
                                                                            xlat_intrnl_org_sbtype.tgt_idntftn_val                      AS v_org_sbtype,
                                                                            cast( cast(prty_addr_start_datetime AS timestamp) AS DATE ) AS prty_addr_start_datetime1,
                                                                            prty_addr_start_datetime,
                                                                            CASE
                                                                                      WHEN teradata_etl_ref_xlat.tgt_idntftn_val IS NULL THEN ''UNK''
                                                                                      ELSE teradata_etl_ref_xlat.tgt_idntftn_val
                                                                            END AS out_prty_addr_usge_type_cd,
                                                                            CASE
                                                                                      WHEN xlat_src_sys.tgt_idntftn_val IS NULL THEN ''UNK''
                                                                                      ELSE xlat_src_sys.tgt_idntftn_val
                                                                            END AS var_src_sys,
                                                                            out_cnty_id,
                                                                            out_postl_cd_id,
                                                                            out_city_id,
                                                                            out_terr_id,
                                                                            state,
                                                                            out_ctry_id,
                                                                            state_id,
                                                                            state_typecode,
                                                                            ctry_typecode,
                                                                            county,
                                                                            postalcode,
                                                                            city,
                                                                            addressline1,
                                                                            addressline2,
                                                                            addrtype_typecode,
                                                                            addressbookuid,
                                                                            out_prty_id,
                                                                            org_key,
                                                                            org_type,
                                                                            org_sbtype,
                                                                            src_sys,
                                                                            retired,
                                                                            primaryphone,
                                                                            cellphone,
                                                                            homephone,
                                                                            workphone,
                                                                            updatetime,
                                                                            createtime
                                                                  FROM      (
                                                                            (
                                                                                      SELECT    cnty.cnty_id             AS out_cnty_id,
                                                                                                lkp_postl_cd.postl_cd_id AS out_postl_cd_id,
                                                                                                lkp_city.city_id         AS out_city_id,
                                                                                                out_terr_id,
                                                                                                state,
                                                                                                out_ctry_id,
                                                                                                state_id,
                                                                                                state_typecode,
                                                                                                ctry_typecode,
                                                                                                county,
                                                                                                postalcode,
                                                                                                city,
                                                                                                addressline1,
                                                                                                addressline2,
                                                                                                typecode AS addrtype_typecode,
                                                                                                addressbookuid,
                                                                                                out_prty_id,
                                                                                                org_key,
                                                                                                org_type,
                                                                                                org_sbtype,
                                                                                                src_sys,
                                                                                                prty_addr_start_datetime,
                                                                                                retired,
                                                                                                primaryphone,
                                                                                                cellphone,
                                                                                                homephone,
                                                                                                workphone,
                                                                                                updatetime,
                                                                                                createtime
                                                                                      FROM      (
                                                                                                (
                                                                                                          SELECT    terr.terr_id AS out_terr_id,
                                                                                                                    state,
                                                                                                                    out_ctry_id,
                                                                                                                    state_id,
                                                                                                                    state_typecode,
                                                                                                                    ctry_typecode,
                                                                                                                    county,
                                                                                                                    postalcode,
                                                                                                                    city,
                                                                                                                    addressline1,
                                                                                                                    addressline2,
                                                                                                                    typecode,
                                                                                                                    addressbookuid,
                                                                                                                    out_prty_id,
                                                                                                                    org_key,
                                                                                                                    org_type,
                                                                                                                    org_sbtype,
                                                                                                                    src_sys,
                                                                                                                    prty_addr_start_datetime,
                                                                                                                    retired,
                                                                                                                    primaryphone,
                                                                                                                    cellphone,
                                                                                                                    homephone,
                                                                                                                    workphone,
                                                                                                                    updatetime,
                                                                                                                    createtime
                                                                                                          FROM      (
                                                                                                                    (
                                                                                                                              SELECT    state,
                                                                                                                                        ctry.ctry_id AS out_ctry_id,
                                                                                                                                        state_id,
                                                                                                                                        state_typecode,
                                                                                                                                        ctry_typecode,
                                                                                                                                        county,
                                                                                                                                        postalcode,
                                                                                                                                        city,
                                                                                                                                        addressline1,
                                                                                                                                        addressline2,
                                                                                                                                        typecode,
                                                                                                                                        addressbookuid,
                                                                                                                                        CASE
                                                                                                                                                  WHEN source = ''ContactManager''
                                                                                                                                                  AND       tl_cnt_name IN (''Person'',
                                                                                                                                                                            ''Adjudicator'',
                                                                                                                                                                            ''UserContact'',
                                                                                                                                                                            ''User Contact'',
                                                                                                                                                                            ''Vendor (Person)'',
                                                                                                                                                                            ''Attorney'',
                                                                                                                                                                            ''Doctor'',
                                                                                                                                                                            ''Policy Person'',
                                                                                                                                                                            ''Contact'',
                                                                                                                                                                            ''Lodging (Person)'') THEN indiv_cnt_mgr.indiv_prty_id
                                                                                                                                                  WHEN source = ''ClaimCenter''
                                                                                                                                                  AND       tl_cnt_name IN (''Person'',
                                                                                                                                                                            ''Adjudicator'',
                                                                                                                                                                            ''UserContact'',
                                                                                                                                                                            ''User Contact'',
                                                                                                                                                                            ''Vendor (Person)'',
                                                                                                                                                                            ''Attorney'',
                                                                                                                                                                            ''Doctor'',
                                                                                                                                                                            ''Policy Person'',
                                                                                                                                                                            ''Contact'',
                                                                                                                                                                            ''Lodging (Person)'') THEN indiv_clm_ctr.indiv_prty_id
                                                                                                                                                  WHEN source = ''ContactManager''
                                                                                                                                                  AND       tl_cnt_name IN (''Company'',
                                                                                                                                                                            ''Vendor (Company)'',
                                                                                                                                                                            ''Auto Repair Shop'',
                                                                                                                                                                            ''Auto Towing Agcy'',
                                                                                                                                                                            ''Law Firm'',
                                                                                                                                                                            ''Medical Care Organization'',
                                                                                                                                                                            ''Lodging (Company)'',
                                                                                                                                                                            ''Lodging Provider (Org)'') THEN lkp_busn.busn_prty_id
                                                                                                                                                  WHEN source = ''ClaimCenter''
                                                                                                                                                  AND       tl_cnt_name IN (''Company'',
                                                                                                                                                                            ''Vendor (Company)'',
                                                                                                                                                                            ''Auto Repair Shop'',
                                                                                                                                                                            ''Auto Towing Agcy'',
                                                                                                                                                                            ''Law Firm'',
                                                                                                                                                                            ''Medical Care Organization'',
                                                                                                                                                                            ''Lodging (Company)'',
                                                                                                                                                                            ''Lodging Provider (Org)'') THEN lkp_busn.busn_prty_id
                                                                                                                                        END AS out_prty_id,
                                                                                                                                        org_key,
                                                                                                                                        org_type,
                                                                                                                                        org_sbtype,
                                                                                                                                        src_sys,
                                                                                                                                        prty_addr_start_datetime,
                                                                                                                                        retired,
                                                                                                                                        primaryphone,
                                                                                                                                        cellphone,
                                                                                                                                        homephone,
                                                                                                                                        workphone,
                                                                                                                                        updatetime,
                                                                                                                                        createtime
                                                                                                                              FROM      (
                                                                                                                                        (
                                                                                                                                               SELECT state,
                                                                                                                                                      ctry_id,
                                                                                                                                                      state_id,
                                                                                                                                                      state_typecode,
                                                                                                                                                      ctry_typecode,
                                                                                                                                                      county,
                                                                                                                                                      postalcode,
                                                                                                                                                      city,
                                                                                                                                                      addressline1,
                                                                                                                                                      addressline2,
                                                                                                                                                      typecode,
                                                                                                                                                      addressbookuid,
                                                                                                                                                      source,
                                                                                                                                                      tl_cnt_name,
                                                                                                                                                      CASE
                                                                                                                                                             WHEN xlat_busn_ctgy_cd.tgt_idntftn_val IS NOT NULL THEN tgt_idntftn_val
                                                                                                                                                             ELSE ''UNK''
                                                                                                                                                      END AS busn_ctgy_cd,
                                                                                                                                                      org_key,
                                                                                                                                                      org_type,
                                                                                                                                                      org_sbtype,
                                                                                                                                                      src_sys,
                                                                                                                                                      prty_addr_start_datetime,
                                                                                                                                                      retired,
                                                                                                                                                      primaryphone,
                                                                                                                                                      cellphone,
                                                                                                                                                      homephone,
                                                                                                                                                      workphone,
                                                                                                                                                      updatetime,
                                                                                                                                                      createtime
                                                                                                                                               FROM   (
                                                                                                                                                      (
                                                                                                                                                                SELECT    state,
                                                                                                                                                                          country_id AS ctry_id,
                                                                                                                                                                          state_id,
                                                                                                                                                                          state_typecode   AS state_typecode,
                                                                                                                                                                          country_typecode AS ctry_typecode,
                                                                                                                                                                          county,
                                                                                                                                                                          postalcode,
                                                                                                                                                                          city,
                                                                                                                                                                          addressline1,
                                                                                                                                                                          addressline2,
                                                                                                                                                                          addtype_typecode AS typecode,
                                                                                                                                                                          addressbookuid,
                                                                                                                                                                          substr(source,position(''-'' IN source)+1) AS source,
                                                                                                                                                                          substr(source,1,
                                                                                                                                                                          CASE
                                                                                                                                                                                    WHEN position(''-'' IN source)-1<1 THEN 1
                                                                                                                                                                                    ELSE position(''-'' IN source)-1
                                                                                                                                                                          END)        AS tl_cnt_name,
                                                                                                                                                                          org_key     AS org_key,
                                                                                                                                                                          org_type    AS org_type,
                                                                                                                                                                          org_subtype AS org_sbtype,
                                                                                                                                                                          sys_src_cd  AS src_sys,
                                                                                                                                                                          prty_addr_start_datetime,
                                                                                                                                                                          retired,
                                                                                                                                                                          primaryphone,
                                                                                                                                                                          cellphone,
                                                                                                                                                                          homephone,
                                                                                                                                                                          workphone,
                                                                                                                                                                          updatetime,
                                                                                                                                                                          createtime
                                                                                                                                                                FROM      (
                                                                                                                                                                          (
                                                                                                                                                                                   /* **********************************************************************************
SQ Query*/
                                                                                                                                                                                   SELECT   cast (src1.state_stg AS          VARCHAR(60)) AS state,
                                                                                                                                                                                            cast (src1.county_stg AS         VARCHAR(60)) AS county ,
                                                                                                                                                                                            cast (src1.postalcode_stg AS     VARCHAR(60)) AS postalcode ,
                                                                                                                                                                                            cast (src1.city_stg AS           VARCHAR(60)) AS city,
                                                                                                                                                                                            cast (src1.addressline1_stg AS   VARCHAR(60)) AS addressline1,
                                                                                                                                                                                            cast (src1.addressline2_stg AS   VARCHAR(60)) AS addressline2,
                                                                                                                                                                                            cast (src1.ctry_id AS            VARCHAR(60)) AS country_id,
                                                                                                                                                                                            cast (src1.state_id AS           VARCHAR(60)) AS state_id,
                                                                                                                                                                                            cast (src1.state_typecode AS     VARCHAR(60)) AS state_typecode,
                                                                                                                                                                                            cast (src1.ctry_typecode AS      VARCHAR(60)) AS country_typecode,
                                                                                                                                                                                            cast (src1.addtype_typecode AS   VARCHAR(60)) AS addtype_typecode,
                                                                                                                                                                                            cast (src1.addressbookuid_stg AS VARCHAR(64)) AS addressbookuid,
                                                                                                                                                                                            cast (src1.updatetime AS         VARCHAR(60)) AS updatetime ,
                                                                                                                                                                                            cast (src1.source AS             VARCHAR(60)) AS source,
                                                                                                                                                                                            cast(src1.org_key AS             VARCHAR(50)) AS org_key,
                                                                                                                                                                                            org_type,
                                                                                                                                                                                            src1.org_subtype,
                                                                                                                                                                                            src1.sys_src_cd ,
                                                                                                                                                                                            cast (src1.createtime_stg AS  VARCHAR (60)) AS prty_addr_start_datetime,
                                                                                                                                                                                            cast (src1.retired AS         VARCHAR(60))  AS retired ,
                                                                                                                                                                                            cast(src1.primaryphone_stg AS VARCHAR(60))  AS primaryphone ,
                                                                                                                                                                                            cast(src1.cellphone_stg AS    VARCHAR(60))  AS cellphone ,
                                                                                                                                                                                            cast(src1.homephone_stg AS    VARCHAR(60))  AS homephone ,
                                                                                                                                                                                            cast(src1.workphone_stg AS    VARCHAR(60))  AS workphone ,
                                                                                                                                                                                            CASE
                                                                                                                                                                                                     WHEN right(cast(extract(second FROM src1.createtime) AS VARCHAR(24)),4) BETWEEN 1000 AND      1499 THEN cast(cast(src1.createtime AS VARCHAR(22))
                                                                                                                                                                                                        ||''0000'' AS timestamp(6))
                                                                                                                                                                                                     WHEN right(cast(extract(second FROM src1.createtime) AS VARCHAR(24)),4) BETWEEN 1500 AND      4499 THEN cast(cast(src1.createtime AS VARCHAR(22))
                                                                                                                                                                                                        ||''3000'' AS timestamp(6))
                                                                                                                                                                                                     WHEN right(cast(extract(second FROM src1.createtime) AS VARCHAR(24)),4) BETWEEN 4500 AND      8499 THEN cast(cast(src1.createtime AS VARCHAR(22))
                                                                                                                                                                                                        ||''7000'' AS timestamp(6))
                                                                                                                                                                                                     WHEN right(cast(extract(second FROM src1.createtime) AS VARCHAR(24)),4) BETWEEN 8500 AND      9999 THEN cast(cast(src1.createtime AS VARCHAR(22))
                                                                                                                                                                                                        ||''0000'' AS timestamp(6)) + interval ''0.010 SECOND''
                                                                                                                                                                                                     ELSE src1.createtime
                                                                                                                                                                                            END AS createtime
                                                                                                                                                                                   FROM     (
                                                                                                                                                                                                        SELECT DISTINCT pc_address.state_stg,
                                                                                                                                                                                                        pc_address.county_stg,
                                                                                                                                                                                                        pctl_country.id_stg       AS ctry_id,
                                                                                                                                                                                                        pctl_state.id_stg         AS state_id,
                                                                                                                                                                                                        pctl_state.typecode_stg   AS state_typecode,
                                                                                                                                                                                                        pctl_country.typecode_stg AS ctry_typecode,
                                                                                                                                                                                                        pc_address.postalcode_stg,
                                                                                                                                                                                                        pc_address.city_stg,
                                                                                                                                                                                                        pc_address.addressline1_stg,
                                                                                                                                                                                                        pc_address.addressline2_stg,
                                                                                                                                                                                                        pctl_addresstype.typecode_stg AS addtype_typecode,
                                                                                                                                                                                                        pc_contact.addressbookuid_stg,
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN pc_address.updatetime_stg > pc_contact.updatetime_stg THEN pc_address.updatetime_stg
                                                                                                                                                                                                        ELSE pc_contact.updatetime_stg
                                                                                                                                                                                                        END AS updatetime,
                                                                                                                                                                                                        pctl_contact.typecode_stg
                                                                                                                                                                                                        || cast(''-ContactManager'' AS VARCHAR(50)) AS source,
                                                                                                                                                                                                        cast(NULL AS                                 VARCHAR(50)) AS org_key,
                                                                                                                                                                                                        cast(NULL AS                                 VARCHAR(50)) AS org_type,
                                                                                                                                                                                                        cast(NULL AS                                 VARCHAR(50)) AS org_subtype,
                                                                                                                                                                                                        cast(''SRC_SYS4'' AS                           VARCHAR(50)) AS sys_src_cd ,
                                                                                                                                                                                                        pc_address.createtime_stg,
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN pc_contact.retired_stg=0
                                                                                                                                                                                                        AND             pc_address.retired_stg=0 THEN 0
                                                                                                                                                                                                        ELSE 1
                                                                                                                                                                                                        END AS retired,
                                                                                                                                                                                                        pc_contact.primaryphone_stg,
                                                                                                                                                                                                        pc_contact.cellphone_stg,
                                                                                                                                                                                                        pc_contact.homephone_stg,
                                                                                                                                                                                                        pc_contact.workphone_stg ,
                                                                                                                                                                                                        pc_contact.createtime_stg AS createtime
                                                                                                                                                                                                        FROM            db_t_prod_stag.pctl_state pctl_state,
                                                                                                                                                                                                        db_t_prod_stag.pctl_country pctl_country,
                                                                                                                                                                                                        db_t_prod_stag.pc_address pc_address,
                                                                                                                                                                                                        db_t_prod_stag.pctl_addresstype pctl_addresstype,
                                                                                                                                                                                                        db_t_prod_stag.pc_contact pc_contact,
                                                                                                                                                                                                        db_t_prod_stag.pctl_contact pctl_contact
                                                                                                                                                                                                        WHERE           pc_address.state_stg=pctl_state.id_stg
                                                                                                                                                                                                        AND             pctl_country.id_stg=pc_address.country_stg
                                                                                                                                                                                                        AND             pc_address.addresstype_stg = pctl_addresstype.id_stg
                                                                                                                                                                                                        AND             pc_contact.primaryaddressid_stg = pc_address.id_stg
                                                                                                                                                                                                        AND             pctl_contact.id_stg=pc_contact.subtype_stg
                                                                                                                                                                                                        AND             ((
                                                                                                                                                                                                        pc_address.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                        AND             pc_address.updatetime_stg <= (:end_dttm))
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        pc_contact.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                        AND             pc_contact.updatetime_stg <= (:end_dttm)))
                                                                                                                                                                                                        UNION
                                                                                                                                                                                                        SELECT          pc_address.state_stg,
                                                                                                                                                                                                        pc_address.county_stg,
                                                                                                                                                                                                        cast(''10001'' AS INTEGER)  AS ctry_id,
                                                                                                                                                                                                        pctl_state.id_stg         AS state_id,
                                                                                                                                                                                                        pctl_state.typecode_stg   AS state_typecode,
                                                                                                                                                                                                        cast(''US'' AS VARCHAR(50)) AS ctry_typecode,
                                                                                                                                                                                                        pc_address.postalcode_stg,
                                                                                                                                                                                                        pc_address.city_stg,
                                                                                                                                                                                                        pc_address.addressline1_stg,
                                                                                                                                                                                                        pc_address.addressline2_stg,
                                                                                                                                                                                                        cast(''PRTY_ADDR_USGE_TYPE8'' AS VARCHAR(50)) AS addtype_typecode,
                                                                                                                                                                                                        pc_contact.addressbookuid_stg,
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN pc_address.updatetime_stg > pc_contact.updatetime_stg THEN pc_address.updatetime_stg
                                                                                                                                                                                                        ELSE pc_contact.updatetime_stg
                                                                                                                                                                                                        END                      AS updatetime,
                                                                                                                                                                                                        cast('' '' AS                 VARCHAR(50)) AS source,
                                                                                                                                                                                                        cast(pc_group.name_stg AS   VARCHAR(50)) AS org_key,
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
                                                                                                                                                                                                        pc_contact.workphone_stg ,
                                                                                                                                                                                                        pc_contact.createtime_stg AS createtime
                                                                                                                                                                                                        FROM            db_t_prod_stag.pc_group pc_group
                                                                                                                                                                                                        join            db_t_prod_stag.pc_contact pc_contact
                                                                                                                                                                                                        ON              pc_group.contact_alfa_stg=pc_contact.id_stg
                                                                                                                                                                                                        join            db_t_prod_stag.pc_address pc_address
                                                                                                                                                                                                        ON              pc_contact.primaryaddressid_stg = pc_address.id_stg
                                                                                                                                                                                                        join            db_t_prod_stag.pctl_addresstype pctl_addresstype
                                                                                                                                                                                                        ON              pc_address.addresstype_stg = pctl_addresstype.id_stg
                                                                                                                                                                                                        join            db_t_prod_stag.pctl_contact pctl_contact
                                                                                                                                                                                                        ON              pctl_contact.id_stg=pc_contact.subtype_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.pctl_state pctl_state
                                                                                                                                                                                                        ON              pctl_state.id_stg=pc_address.state_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.pctl_country pctl_country
                                                                                                                                                                                                        ON              pctl_country.id_stg=pc_address.country_stg
                                                                                                                                                                                                        inner join      db_t_prod_stag.pctl_grouptype pctl_grouptype
                                                                                                                                                                                                        ON              pctl_grouptype.id_stg=pc_group.grouptype_stg
                                                                                                                                                                                                        WHERE           pctl_grouptype.typecode_stg =''servicecenter_alfa''
                                                                                                                                                                                                        AND             pctl_state.typecode_stg IS NOT NULL
                                                                                                                                                                                                        AND             ((
                                                                                                                                                                                                        pc_address.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                        AND             pc_address.updatetime_stg <= (:end_dttm))
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        pc_contact.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                        AND             pc_contact.updatetime_stg <= (:end_dttm)))
                                                                                                                                                                                                        UNION
                                                                                                                                                                                                        SELECT DISTINCT cc_address.state_stg,
                                                                                                                                                                                                        cc_address.county_stg,
                                                                                                                                                                                                        cctl_country.id_stg       AS ctry_id,
                                                                                                                                                                                                        cctl_state.id_stg         AS state_id,
                                                                                                                                                                                                        cctl_state.typecode_stg   AS state_typecode,
                                                                                                                                                                                                        cctl_country.typecode_stg AS ctry_typecode,
                                                                                                                                                                                                        cc_address.postalcode_stg,
                                                                                                                                                                                                        cc_address.city_stg,
                                                                                                                                                                                                        cc_address.addressline1_stg,
                                                                                                                                                                                                        cc_address.addressline2_stg,
                                                                                                                                                                                                        cctl_addresstype.typecode_stg,
                                                                                                                                                                                                        cc_contact.publicid_stg,
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN cc_address.updatetime_stg > cc_contact.updatetime_stg THEN cc_address.updatetime_stg
                                                                                                                                                                                                        ELSE cc_contact.updatetime_stg
                                                                                                                                                                                                        END AS updatetime,
                                                                                                                                                                                                        cctl_contact.name_stg
                                                                                                                                                                                                        || ''-ClaimCenter'' AS source,
                                                                                                                                                                                                        cast(NULL AS       VARCHAR(50))         AS org_key,
                                                                                                                                                                                                        cast(NULL AS       VARCHAR(50))         AS org_type,
                                                                                                                                                                                                        cast(NULL AS       VARCHAR(50))         AS org_subtype,
                                                                                                                                                                                                        cast(''SRC_SYS6'' AS VARCHAR(50))         AS sys_src_cd,
                                                                                                                                                                                                        cc_address.createtime_stg,
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN cc_contact.retired_stg=0
                                                                                                                                                                                                        AND             cc_address.retired_stg=0 THEN 0
                                                                                                                                                                                                        ELSE 1
                                                                                                                                                                                                        END AS retired,
                                                                                                                                                                                                        cc_contact.primaryphone_stg,
                                                                                                                                                                                                        cc_contact.cellphone_stg,
                                                                                                                                                                                                        cc_contact.homephone_stg,
                                                                                                                                                                                                        cc_contact.workphone_stg ,
                                                                                                                                                                                                        cc_contact.createtime_stg AS createtime
                                                                                                                                                                                                        FROM            db_t_prod_stag.cctl_state cctl_state,
                                                                                                                                                                                                        db_t_prod_stag.cctl_country cctl_country,
                                                                                                                                                                                                        db_t_prod_stag.cc_address cc_address,
                                                                                                                                                                                                        db_t_prod_stag.cctl_addresstype cctl_addresstype,
                                                                                                                                                                                                        db_t_prod_stag.cc_contact cc_contact,
                                                                                                                                                                                                        db_t_prod_stag.cctl_contact cctl_contact
                                                                                                                                                                                                        WHERE           cc_address.state_stg=cctl_state.id_stg
                                                                                                                                                                                                        AND             cctl_country.id_stg=cc_address.country_stg
                                                                                                                                                                                                        AND             cc_address.addresstype_stg = cctl_addresstype.id_stg
                                                                                                                                                                                                        AND             cc_contact.primaryaddressid_stg = cc_address.id_stg
                                                                                                                                                                                                        AND             cctl_contact.id_stg=cc_contact.subtype_stg
                                                                                                                                                                                                        AND             ((
                                                                                                                                                                                                        cc_address.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                        AND             cc_address.updatetime_stg <= (:end_dttm))
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        cc_contact.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                        AND             cc_contact.updatetime_stg <= (:end_dttm)))
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
                                                                                                                                                                                                        WHEN bc_address.updatetime_stg > e.updatetime_stg THEN bc_address.updatetime_stg
                                                                                                                                                                                                        ELSE e.updatetime_stg
                                                                                                                                                                                                        END AS updatetime,
                                                                                                                                                                                                        bctl_contact.typecode_stg
                                                                                                                                                                                                        || ''-ClaimCenter'' AS source,
                                                                                                                                                                                                        cast(NULL AS       VARCHAR(50))         AS org_key,
                                                                                                                                                                                                        cast(NULL AS       VARCHAR(50))         AS org_type,
                                                                                                                                                                                                        cast(NULL AS       VARCHAR(50))         AS org_subtype,
                                                                                                                                                                                                        cast(''SRC_SYS5'' AS VARCHAR(50))         AS sys_src_cd ,
                                                                                                                                                                                                        bc_address.createtime_stg,
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN e.retired_stg=0
                                                                                                                                                                                                        AND             bc_address.retired_stg=0 THEN 0
                                                                                                                                                                                                        ELSE 1
                                                                                                                                                                                                        END                       AS retired,
                                                                                                                                                                                                        cast(NULL AS INTEGER)     AS primaryphone_stg,
                                                                                                                                                                                                        cast(NULL AS VARCHAR(50)) AS cellphone_stg,
                                                                                                                                                                                                        cast(NULL AS VARCHAR(50)) AS homephone_stg,
                                                                                                                                                                                                        cast(NULL AS VARCHAR(50)) AS workphone_stg ,
                                                                                                                                                                                                        e.createtime_stg          AS createtime
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
                                                                                                                                                                                                        left join       db_t_prod_stag.bc_address bc_address
                                                                                                                                                                                                        ON              e.primaryaddressid_stg=bc_address.id_stg
                                                                                                                                                                                                        inner join      db_t_prod_stag.bctl_country bctl_country
                                                                                                                                                                                                        ON              bctl_country.id_stg=bc_address.country_stg
                                                                                                                                                                                                        inner join      db_t_prod_stag.bctl_state bctl_state
                                                                                                                                                                                                        ON              bc_address.state_stg=bctl_state.id_stg
                                                                                                                                                                                                        inner join      db_t_prod_stag.bctl_contact bctl_contact
                                                                                                                                                                                                        ON              bctl_contact.id_stg=e.subtype_stg
                                                                                                                                                                                                        left join       db_t_prod_stag.bctl_addresstype bctl_addresstype
                                                                                                                                                                                                        ON              bc_address.addresstype_stg = bctl_addresstype.id_stg
                                                                                                                                                                                                        WHERE           b.overridingpayer_alfa_stg IS NULL
                                                                                                                                                                                                        AND             c.primarypayer_stg = 1
                                                                                                                                                                                                        AND             ((
                                                                                                                                                                                                        bc_address.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                        AND             bc_address.updatetime_stg <= (:end_dttm))
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        e.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                        AND             e.updatetime_stg <= (:end_dttm)))
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
                                                                                                                                                                                                        WHEN bc_address.updatetime_stg > e.updatetime_stg THEN bc_address.updatetime_stg
                                                                                                                                                                                                        ELSE e.updatetime_stg
                                                                                                                                                                                                        END AS updatetime,
                                                                                                                                                                                                        bctl_contact.typecode_stg
                                                                                                                                                                                                        || ''-ClaimCenter'' AS source,
                                                                                                                                                                                                        cast(NULL AS       VARCHAR(50))         AS org_key,
                                                                                                                                                                                                        cast(NULL AS       VARCHAR(50))         AS org_type,
                                                                                                                                                                                                        cast(NULL AS       VARCHAR(50))         AS org_subtype,
                                                                                                                                                                                                        cast(''SRC_SYS5'' AS VARCHAR(50))         AS sys_src_cd ,
                                                                                                                                                                                                        bc_address.createtime_stg,
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN e.retired_stg=0
                                                                                                                                                                                                        AND             bc_address.retired_stg=0 THEN 0
                                                                                                                                                                                                        ELSE 1
                                                                                                                                                                                                        END                       AS retired,
                                                                                                                                                                                                        cast(NULL AS INTEGER)     AS primaryphone_stg,
                                                                                                                                                                                                        cast(NULL AS VARCHAR(50)) AS cellphone_stg,
                                                                                                                                                                                                        cast(NULL AS VARCHAR(50)) AS homephone_stg,
                                                                                                                                                                                                        cast(NULL AS VARCHAR(50)) AS workphone_stg ,
                                                                                                                                                                                                        e.createtime_stg          AS createtime
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
                                                                                                                                                                                                        left join       db_t_prod_stag.bc_address bc_address
                                                                                                                                                                                                        ON              e.primaryaddressid_stg=bc_address.id_stg
                                                                                                                                                                                                        join            db_t_prod_stag.bctl_country bctl_country
                                                                                                                                                                                                        ON              bctl_country.id_stg=bc_address.country_stg
                                                                                                                                                                                                        join            db_t_prod_stag.bctl_state bctl_state
                                                                                                                                                                                                        ON              bc_address.state_stg=bctl_state.id_stg
                                                                                                                                                                                                        join            db_t_prod_stag.bctl_contact bctl_contact
                                                                                                                                                                                                        ON              bctl_contact.id_stg=e.subtype_stg
                                                                                                                                                                                                        left join       db_t_prod_stag.bctl_addresstype bctl_addresstype
                                                                                                                                                                                                        ON              bc_address.addresstype_stg = bctl_addresstype.id_stg
                                                                                                                                                                                                        WHERE           b.overridingpayer_alfa_stg IS NOT NULL
                                                                                                                                                                                                        AND             ((
                                                                                                                                                                                                        bc_address.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                        AND             bc_address.updatetime_stg <= (:end_dttm))
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        e.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                        AND             e.updatetime_stg <= (:end_dttm)))
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
                                                                                                                                                                                                        WHEN bc_address.updatetime_stg >bc_contact.updatetime_stg THEN bc_address.updatetime_stg
                                                                                                                                                                                                        ELSE bc_contact.updatetime_stg
                                                                                                                                                                                                        END AS updatetime,
                                                                                                                                                                                                        bctl_contact.typecode_stg
                                                                                                                                                                                                        || ''-ClaimCenter'' AS source,
                                                                                                                                                                                                        cast(NULL AS       VARCHAR(50))         AS org_key,
                                                                                                                                                                                                        cast(NULL AS       VARCHAR(50))         AS org_type,
                                                                                                                                                                                                        cast(NULL AS       VARCHAR(50))         AS org_subtype,
                                                                                                                                                                                                        cast(''SRC_SYS5'' AS VARCHAR(50))         AS sys_src_cd ,
                                                                                                                                                                                                        bc_address.createtime_stg,
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN bc_contact.retired_stg=0
                                                                                                                                                                                                        AND             bc_address.retired_stg=0 THEN 0
                                                                                                                                                                                                        ELSE 1
                                                                                                                                                                                                        END                       AS retired,
                                                                                                                                                                                                        cast(NULL AS INTEGER)     AS primaryphone_stg,
                                                                                                                                                                                                        cast(NULL AS VARCHAR(50)) AS cellphone_stg,
                                                                                                                                                                                                        cast(NULL AS VARCHAR(50)) AS homephone_stg,
                                                                                                                                                                                                        cast(NULL AS VARCHAR(50)) AS workphone_stg ,
                                                                                                                                                                                                        bc_contact.createtime_stg AS createtime
                                                                                                                                                                                                        FROM            db_t_prod_stag.bc_address bc_address
                                                                                                                                                                                                        join            db_t_prod_stag.bctl_state bctl_state
                                                                                                                                                                                                        ON              bc_address.state_stg=bctl_state.id_stg
                                                                                                                                                                                                        join            db_t_prod_stag.bctl_country bctl_country
                                                                                                                                                                                                        ON              bctl_country.id_stg=bc_address.country_stg
                                                                                                                                                                                                        left join       db_t_prod_stag.bctl_addresstype bctl_addresstype
                                                                                                                                                                                                        ON              bc_address.addresstype_stg = bctl_addresstype.id_stg
                                                                                                                                                                                                        join            db_t_prod_stag.bc_contact bc_contact
                                                                                                                                                                                                        ON              bc_contact.primaryaddressid_stg = bc_address.id_stg
                                                                                                                                                                                                        join            db_t_prod_stag.bctl_contact bctl_contact
                                                                                                                                                                                                        ON              bctl_contact.id_stg=bc_contact.subtype_stg
                                                                                                                                                                                                        AND             ((
                                                                                                                                                                                                        bc_address.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                        AND             bc_address.updatetime_stg <= (:end_dttm))
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        bc_contact.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                        AND             bc_contact.updatetime_stg <= (:end_dttm)))
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
                                                                                                                                                                                                        WHEN bc_address.updatetime_stg > d.updatetime_stg THEN bc_address.updatetime_stg
                                                                                                                                                                                                        ELSE d.updatetime_stg
                                                                                                                                                                                                        END AS updatetime,
                                                                                                                                                                                                        t.typecode_stg
                                                                                                                                                                                                        || ''-ClaimCenter'' AS source,
                                                                                                                                                                                                        cast(NULL AS       VARCHAR(50))         AS org_key,
                                                                                                                                                                                                        cast(NULL AS       VARCHAR(50))         AS org_type,
                                                                                                                                                                                                        cast(NULL AS       VARCHAR(50))         AS org_subtype,
                                                                                                                                                                                                        cast(''SRC_SYS5'' AS VARCHAR(50))         AS sys_src_cd ,
                                                                                                                                                                                                        bc_address.createtime_stg,
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN a.retired_stg=0
                                                                                                                                                                                                        AND             bc_address.retired_stg=0 THEN 0
                                                                                                                                                                                                        ELSE 1
                                                                                                                                                                                                        END                       AS retired,
                                                                                                                                                                                                        cast(NULL AS INTEGER)     AS primaryphone_stg,
                                                                                                                                                                                                        cast(NULL AS VARCHAR(50)) AS cellphone_stg,
                                                                                                                                                                                                        cast(NULL AS VARCHAR(50)) AS homephone_stg,
                                                                                                                                                                                                        cast(NULL AS VARCHAR(50)) AS workphone_stg ,
                                                                                                                                                                                                        a.createtime_stg          AS createtime
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
                                                                                                                                                                                                        join            db_t_prod_stag.bc_address bc_address
                                                                                                                                                                                                        ON              d.primaryaddressid_stg = bc_address.id_stg
                                                                                                                                                                                                        join            db_t_prod_stag.bctl_state bctl_state
                                                                                                                                                                                                        ON              bc_address.state_stg=bctl_state.id_stg
                                                                                                                                                                                                        join            db_t_prod_stag.bctl_country bctl_country
                                                                                                                                                                                                        ON              bctl_country.id_stg=bc_address.country_stg
                                                                                                                                                                                                        left join       db_t_prod_stag.bctl_addresstype bctl_addresstype
                                                                                                                                                                                                        ON              bc_address.addresstype_stg = bctl_addresstype.id_stg
                                                                                                                                                                                                        WHERE           ((
                                                                                                                                                                                                        h.primarypayer_stg = 1)
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        j.name_stg = ''Payer''))
                                                                                                                                                                                                        AND             d.updatetime_stg > (:start_dttm)
                                                                                                                                                                                                        AND             d.updatetime_stg <= (:end_dttm) ) src1 qualify row_number() over(PARTITION BY state,county,postalcode,city,addressline1,addressline2,country_id,state_id,state_typecode,country_typecode,addtype_typecode,addressbookuid,source ORDER BY updatetime DESC ) = 1 )
                                                                                                                                                                 UNION
                                                                                                                                                                          (
                                                                                                                                                                                 SELECT cast(NULL AS               VARCHAR(60)) AS state ,
                                                                                                                                                                                        cast(NULL AS               VARCHAR(60)) AS county ,
                                                                                                                                                                                        cast(NULL AS               VARCHAR(60)) AS postalcode ,
                                                                                                                                                                                        cast(NULL AS               VARCHAR(60)) AS city ,
                                                                                                                                                                                        cast(NULL AS               VARCHAR(60)) AS addressline1 ,
                                                                                                                                                                                        cast(NULL AS               VARCHAR(60)) AS addressline2 ,
                                                                                                                                                                                        cast(NULL AS               VARCHAR(60)) AS country_id ,
                                                                                                                                                                                        cast(NULL AS               VARCHAR(60)) AS state_id ,
                                                                                                                                                                                        cast(NULL AS               VARCHAR(60)) AS state_typecode ,
                                                                                                                                                                                        cast(NULL AS               VARCHAR(60)) AS country_typecode ,
                                                                                                                                                                                        cast(''LOCTR_USGE_TYPE3'' AS VARCHAR(50)) AS addtype_typecode,
                                                                                                                                                                                        cast(
                                                                                                                                                                                        CASE
                                                                                                                                                                                               WHEN src.source=''ContactManager'' THEN linkid
                                                                                                                                                                                               WHEN src.source=''ClaimCenter'' THEN publicid
                                                                                                                                                                                        END AS                  VARCHAR(64)) AS addressbookuid ,
                                                                                                                                                                                        cast (src.updatetime AS VARCHAR(60)) AS updatetime ,
                                                                                                                                                                                        cast(src.tl_cnt_name
                                                                                                                                                                                               ||''-''
                                                                                                                                                                                               ||src.source AS VARCHAR(60)) AS source ,
                                                                                                                                                                                        cast(NULL AS           VARCHAR(50)) AS org_key ,
                                                                                                                                                                                        cast(NULL AS           VARCHAR(60)) AS org_type ,
                                                                                                                                                                                        cast(NULL AS           VARCHAR(60)) AS org_subtype,
                                                                                                                                                                                        cast(''SRC_SYS6'' AS     VARCHAR(50)) AS sys_src_cd ,
                                                                                                                                                                                        cast(NULL AS           VARCHAR(60)) AS prty_addr_start_datetime,
                                                                                                                                                                                        cast (src.retired AS   VARCHAR(60)) AS retired ,
                                                                                                                                                                                        cast(NULL AS           VARCHAR(60)) AS primaryphone ,
                                                                                                                                                                                        cast(NULL AS           VARCHAR(60)) AS cellphone ,
                                                                                                                                                                                        cast(NULL AS           VARCHAR(60)) AS homephone ,
                                                                                                                                                                                        cast(NULL AS           VARCHAR(60)) AS workphone ,
                                                                                                                                                                                        src.createtime
                                                                                                                                                                                 FROM   (
                                                                                                                                                                                                        SELECT          bc_contact.updatetime_stg             AS updatetime ,
                                                                                                                                                                                                        bc_contact.publicid_stg               AS publicid,
                                                                                                                                                                                                        bc_contact.addressbookuid_stg         AS linkid,
                                                                                                                                                                                                        bc_contact.retired_stg                AS retired,
                                                                                                                                                                                                        bc_contact.createtime_stg             AS createtime,
                                                                                                                                                                                                        bc_contact.dateofbirth_stg            AS dateofbirth,
                                                                                                                                                                                                        bctl_gendertype.typecode_stg          AS gndr_type_cd,
                                                                                                                                                                                                        bctl_taxfilingstatustype.typecode_stg AS tax_filg_type_cd,
                                                                                                                                                                                                        bctl_contact.name_stg                 AS tl_cnt_name,
                                                                                                                                                                                                        bctl_taxstatus.typecode_stg           AS tax_id_sts_cd,
                                                                                                                                                                                                        ''SRC_SYS5''                            AS sys_src_cd,
                                                                                                                                                                                                        cast(''ClaimCenter'' AS VARCHAR(50))    AS source
                                                                                                                                                                                                        FROM            db_t_prod_stag.bc_contact bc_contact
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_contact bctl_contact
                                                                                                                                                                                                        ON              bctl_contact.id_stg = bc_contact.subtype_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_gendertype bctl_gendertype
                                                                                                                                                                                                        ON              bc_contact.gender_stg = bctl_gendertype.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_taxfilingstatustype bctl_taxfilingstatustype
                                                                                                                                                                                                        ON              bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_taxstatus bctl_taxstatus
                                                                                                                                                                                                        ON              bc_contact.taxstatus_stg = bctl_taxstatus.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_maritalstatus bctl_maritalstatus
                                                                                                                                                                                                        ON              bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_nameprefix bctl_nameprefix
                                                                                                                                                                                                        ON              bc_contact.prefix_stg = bctl_nameprefix.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_namesuffix bctl_namesuffix
                                                                                                                                                                                                        ON              bc_contact.suffix_stg = bctl_namesuffix.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bc_user bc_user
                                                                                                                                                                                                        ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bc_credential bc_credential
                                                                                                                                                                                                        ON              bc_user.credentialid_stg = bc_credential.id_stg
                                                                                                                                                                                                        WHERE           bctl_contact.typecode_stg = (''UserContact'')
                                                                                                                                                                                                        AND             bc_contact.publicid_stg NOT IN (''default_data:1'',
                                                                                                                                                                                                        ''systemTables:1'',
                                                                                                                                                                                                        ''systemTables:2'')
                                                                                                                                                                                                        AND             ((
                                                                                                                                                                                                        bc_contact.updatetime_stg>(:start_dttm)
                                                                                                                                                                                                        AND             bc_contact.updatetime_stg <=(:end_dttm))
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        bc_user.updatetime_stg>(:start_dttm)
                                                                                                                                                                                                        AND             bc_user.updatetime_stg <= (:end_dttm)))
                                                                                                                                                                                                        UNION
                                                                                                                                                                                                        SELECT          bc_contact.updatetime_stg AS updatetime,
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN(
                                                                                                                                                                                                        bc_contact.externalid_stg IS NOT NULL) THEN bc_contact.externalid_stg
                                                                                                                                                                                                        ELSE bc_contact.publicid_stg
                                                                                                                                                                                                        END                                   AS publicid,
                                                                                                                                                                                                        bc_contact.addressbookuid_stg         AS linkid,
                                                                                                                                                                                                        bc_contact.retired_stg                AS retired,
                                                                                                                                                                                                        bc_contact.createtime_stg             AS createtime,
                                                                                                                                                                                                        bc_contact.dateofbirth_stg            AS dateofbirth,
                                                                                                                                                                                                        bctl_gendertype.typecode_stg          AS gndr_type_cd,
                                                                                                                                                                                                        bctl_taxfilingstatustype.typecode_stg AS tax_filg_type_cd,
                                                                                                                                                                                                        bctl_contact.name_stg                 AS tl_cnt_name,
                                                                                                                                                                                                        bctl_taxstatus.typecode_stg           AS tax_id_sts_cd,
                                                                                                                                                                                                        ''SRC_SYS5''                            AS sys_src_cd,
                                                                                                                                                                                                        cast(''ClaimCenter'' AS VARCHAR(50))    AS source
                                                                                                                                                                                                        FROM            db_t_prod_stag.bc_account a
                                                                                                                                                                                                        inner join      db_t_prod_stag.bc_accountcontact h
                                                                                                                                                                                                        ON              h.accountid_stg = a.id_stg
                                                                                                                                                                                                        inner join      db_t_prod_stag.bc_contact bc_contact
                                                                                                                                                                                                        ON              bc_contact.id_stg = h.contactid_stg
                                                                                                                                                                                                        join            db_t_prod_stag.bctl_contact bctl_contact
                                                                                                                                                                                                        ON              bctl_contact.id_stg=bc_contact.subtype_stg
                                                                                                                                                                                                        left join       db_t_prod_stag.bc_accountcontactrole i
                                                                                                                                                                                                        ON              i.accountcontactid_stg = h.id_stg
                                                                                                                                                                                                        left join       db_t_prod_stag.bctl_accountrole j
                                                                                                                                                                                                        ON              j.id_stg = i.role_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_gendertype bctl_gendertype
                                                                                                                                                                                                        ON              bc_contact.gender_stg = bctl_gendertype.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_taxfilingstatustype bctl_taxfilingstatustype
                                                                                                                                                                                                        ON              bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_taxstatus bctl_taxstatus
                                                                                                                                                                                                        ON              bc_contact.taxstatus_stg = bctl_taxstatus.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_maritalstatus bctl_maritalstatus
                                                                                                                                                                                                        ON              bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_nameprefix bctl_nameprefix
                                                                                                                                                                                                        ON              bc_contact.prefix_stg = bctl_nameprefix.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_namesuffix bctl_namesuffix
                                                                                                                                                                                                        ON              bc_contact.suffix_stg = bctl_namesuffix.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bc_user bc_user
                                                                                                                                                                                                        ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bc_credential bc_credential
                                                                                                                                                                                                        ON              bc_user.credentialid_stg = bc_credential.id_stg
                                                                                                                                                                                                        WHERE           ((
                                                                                                                                                                                                        h.primarypayer_stg = 1)
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        j.name_stg = ''Payer''))
                                                                                                                                                                                                        AND             ((
                                                                                                                                                                                                        bc_contact.updatetime_stg>(:start_dttm)
                                                                                                                                                                                                        AND             bc_contact.updatetime_stg <=(:end_dttm))
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        bc_user.updatetime_stg>(:start_dttm)
                                                                                                                                                                                                        AND             bc_user.updatetime_stg <= (:end_dttm)))
                                                                                                                                                                                                        UNION
                                                                                                                                                                                                        SELECT          bc_contact.updatetime_stg AS updatetime,
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN (
                                                                                                                                                                                                        bc_contact.externalid_stg IS NULL) THEN bc_contact.publicid_stg
                                                                                                                                                                                                        ELSE bc_contact.externalid_stg
                                                                                                                                                                                                        END                                   AS publicid,
                                                                                                                                                                                                        bc_contact.addressbookuid_stg         AS linkid,
                                                                                                                                                                                                        bc_contact.retired_stg                AS retired,
                                                                                                                                                                                                        bc_contact.createtime_stg             AS createtime,
                                                                                                                                                                                                        bc_contact.dateofbirth_stg            AS dateofbirth,
                                                                                                                                                                                                        bctl_gendertype.typecode_stg          AS gndr_type_cd,
                                                                                                                                                                                                        bctl_taxfilingstatustype.typecode_stg AS tax_filg_type_cd,
                                                                                                                                                                                                        bctl_contact.name_stg                 AS tl_cnt_name,
                                                                                                                                                                                                        bctl_taxstatus.typecode_stg           AS tax_id_sts_cd,
                                                                                                                                                                                                        ''SRC_SYS5''                            AS sys_src_cd,
                                                                                                                                                                                                        cast(''ClaimCenter'' AS VARCHAR(50))    AS source
                                                                                                                                                                                                        FROM            db_t_prod_stag.bc_account a
                                                                                                                                                                                                        inner join      db_t_prod_stag.bc_invoicestream b
                                                                                                                                                                                                        ON              a.id_stg = b.accountid_stg
                                                                                                                                                                                                        inner join      db_t_prod_stag.bc_accountcontact c
                                                                                                                                                                                                        ON              c.accountid_stg=a.id_stg
                                                                                                                                                                                                        inner join      db_t_prod_stag.bc_contact bc_contact
                                                                                                                                                                                                        ON              bc_contact.id_stg = c.contactid_stg
                                                                                                                                                                                                        join            db_t_prod_stag.bctl_contact bctl_contact
                                                                                                                                                                                                        ON              bctl_contact.id_stg=bc_contact.subtype_stg
                                                                                                                                                                                                        left join       db_t_prod_stag.bc_accountcontactrole f
                                                                                                                                                                                                        ON              f.accountcontactid_stg = c.id_stg
                                                                                                                                                                                                        left join       db_t_prod_stag.bctl_accountrole g
                                                                                                                                                                                                        ON              g.id_stg = f.role_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_gendertype bctl_gendertype
                                                                                                                                                                                                        ON              bc_contact.gender_stg = bctl_gendertype.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_taxfilingstatustype bctl_taxfilingstatustype
                                                                                                                                                                                                        ON              bc_contact.taxfilingstatus_stg = bctl_taxfilingstatustype.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_taxstatus bctl_taxstatus
                                                                                                                                                                                                        ON              bc_contact.taxstatus_stg = bctl_taxstatus.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_maritalstatus bctl_maritalstatus
                                                                                                                                                                                                        ON              bc_contact.maritalstatus_stg = bctl_maritalstatus.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_nameprefix bctl_nameprefix
                                                                                                                                                                                                        ON              bc_contact.prefix_stg = bctl_nameprefix.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bctl_namesuffix bctl_namesuffix
                                                                                                                                                                                                        ON              bc_contact.suffix_stg = bctl_namesuffix.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bc_user bc_user
                                                                                                                                                                                                        ON              bc_user.contactid_stg = bc_contact.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.bc_credential bc_credential
                                                                                                                                                                                                        ON              bc_user.credentialid_stg = bc_credential.id_stg
                                                                                                                                                                                                        WHERE           ((
                                                                                                                                                                                                        b.overridingpayer_alfa_stg IS NULL
                                                                                                                                                                                                        AND             c.primarypayer_stg = 1)
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        b.overridingpayer_alfa_stg IS NOT NULL))
                                                                                                                                                                                                        AND             ((
                                                                                                                                                                                                        bc_contact.updatetime_stg>(:start_dttm)
                                                                                                                                                                                                        AND             bc_contact.updatetime_stg <=(:end_dttm))
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        bc_user.updatetime_stg>(:start_dttm)
                                                                                                                                                                                                        AND             bc_user.updatetime_stg <= (:end_dttm)))
                                                                                                                                                                                                        UNION
                                                                                                                                                                                                        SELECT          pc_contact.updatetime_stg             AS updatetime,
                                                                                                                                                                                                        pc_contact.publicid_stg               AS publicid,
                                                                                                                                                                                                        pc_contact.addressbookuid_stg         AS linkid,
                                                                                                                                                                                                        pc_contact.retired_stg                AS retired,
                                                                                                                                                                                                        pc_contact.createtime_stg             AS createtime,
                                                                                                                                                                                                        pc_contact.dateofbirth_stg            AS dateofbirth,
                                                                                                                                                                                                        pctl_gendertype.typecode_stg          AS gndr_type_cd,
                                                                                                                                                                                                        pctl_taxfilingstatustype.typecode_stg AS tax_filg_type_cd,
                                                                                                                                                                                                        pctl_contact.name_stg                 AS tl_cnt_name,
                                                                                                                                                                                                        pctl_taxstatus.typecode_stg           AS tax_id_sts_cd,
                                                                                                                                                                                                        ''SRC_SYS4''                            AS sys_src_cd,
                                                                                                                                                                                                        cast(''ClaimCenter'' AS VARCHAR(50))    AS source
                                                                                                                                                                                                        FROM            db_t_prod_stag.pc_contact pc_contact
                                                                                                                                                                                                        left outer join db_t_prod_stag.pctl_contact pctl_contact
                                                                                                                                                                                                        ON              pctl_contact.id_stg = pc_contact.subtype_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.pctl_gendertype pctl_gendertype
                                                                                                                                                                                                        ON              pc_contact.gender_stg = pctl_gendertype.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.pctl_taxfilingstatustype pctl_taxfilingstatustype
                                                                                                                                                                                                        ON              pc_contact.taxfilingstatus_stg = pctl_taxfilingstatustype.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.pctl_taxstatus pctl_taxstatus
                                                                                                                                                                                                        ON              pc_contact.taxstatus_stg = pctl_taxstatus.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.pctl_maritalstatus pctl_maritalstatus
                                                                                                                                                                                                        ON              pc_contact.maritalstatus_stg = pctl_maritalstatus.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.pctl_nameprefix pctl_nameprefix
                                                                                                                                                                                                        ON              pc_contact.prefix_stg = pctl_nameprefix.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.pctl_namesuffix pctl_namesuffix
                                                                                                                                                                                                        ON              pc_contact.suffix_stg = pctl_namesuffix.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.pc_user pc_user
                                                                                                                                                                                                        ON              pc_user.contactid_stg = pc_contact.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.pc_credential pc_credential
                                                                                                                                                                                                        ON              pc_user.credentialid_stg = pc_credential.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.pc_policyperiod pc_policyperiod
                                                                                                                                                                                                        ON              pc_policyperiod.pnicontactdenorm_stg = pc_contact.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.pc_effectivedatedfields pc_effectivedatedfields
                                                                                                                                                                                                        ON              pc_effectivedatedfields.branchid_stg=pc_policyperiod.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.pc_producercode pc_producercode
                                                                                                                                                                                                        ON              pc_producercode.id_stg=pc_effectivedatedfields.producercodeid_stg
                                                                                                                                                                                                        WHERE           pctl_contact.typecode_stg = (''UserContact'')
                                                                                                                                                                                                        AND             pc_contact.publicid_stg NOT IN (''default_data:1'',
                                                                                                                                                                                                        ''systemTables:1'',
                                                                                                                                                                                                        ''systemTables:2'')
                                                                                                                                                                                                        AND             ((
                                                                                                                                                                                                        pc_contact.updatetime_stg>(:start_dttm)
                                                                                                                                                                                                        AND             pc_contact.updatetime_stg <= (:end_dttm))
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        pc_user.updatetime_stg>(:start_dttm)
                                                                                                                                                                                                        AND             pc_user.updatetime_stg <= (:end_dttm)))
                                                                                                                                                                                                        UNION
                                                                                                                                                                                                        SELECT DISTINCT cc_contact.updatetime_stg             AS updatetime,
                                                                                                                                                                                                        cc_contact.publicid_stg               AS publicid,
                                                                                                                                                                                                        cc_contact.addressbookuid_stg         AS linkid,
                                                                                                                                                                                                        cc_contact.retired_stg                AS retired,
                                                                                                                                                                                                        cc_contact.createtime_stg             AS createtime,
                                                                                                                                                                                                        cc_contact.dateofbirth_stg            AS dateofbirth,
                                                                                                                                                                                                        cctl_gendertype.typecode_stg          AS gndr_type_cd,
                                                                                                                                                                                                        cctl_taxfilingstatustype.typecode_stg AS tax_filg_type_cd,
                                                                                                                                                                                                        cctl_contact.name_stg                 AS tl_cnt_name,
                                                                                                                                                                                                        cctl_taxstatus.typecode_stg           AS tax_id_sts_cd,
                                                                                                                                                                                                        ''SRC_SYS6''                            AS sys_src_cd,
                                                                                                                                                                                                        cast(''ClaimCenter'' AS VARCHAR(50))    AS source
                                                                                                                                                                                                        FROM            db_t_prod_stag.cc_contact cc_contact
                                                                                                                                                                                                        left outer join db_t_prod_stag.cctl_contact cctl_contact
                                                                                                                                                                                                        ON              cctl_contact.id_stg = cc_contact.subtype_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.cctl_gendertype cctl_gendertype
                                                                                                                                                                                                        ON              cc_contact.gender_stg = cctl_gendertype.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.cctl_taxfilingstatustype cctl_taxfilingstatustype
                                                                                                                                                                                                        ON              cc_contact.taxfilingstatus_stg = cctl_taxfilingstatustype.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.cctl_taxstatus cctl_taxstatus
                                                                                                                                                                                                        ON              cc_contact.taxstatus_stg = cctl_taxstatus.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.cctl_maritalstatus cctl_maritalstatus
                                                                                                                                                                                                        ON              cc_contact.maritalstatus_stg = cctl_maritalstatus.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.cctl_nameprefix cctl_nameprefix
                                                                                                                                                                                                        ON              cc_contact.prefix_stg = cctl_nameprefix.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.cctl_namesuffix cctl_namesuffix
                                                                                                                                                                                                        ON              cc_contact.suffix_stg = cctl_namesuffix.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.cc_user cc_user
                                                                                                                                                                                                        ON              cc_user.contactid_stg = cc_contact.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.cc_credential cc_credential
                                                                                                                                                                                                        ON              cc_user.credentialid_stg = cc_credential.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.cc_claimcontact cc_claimcontact
                                                                                                                                                                                                        ON              cc_contact.id_stg=cc_claimcontact.contactid_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.cc_claimcontactrole cc_claimcontactrole
                                                                                                                                                                                                        ON              cc_claimcontact.id_stg=cc_claimcontactrole.claimcontactid_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.cc_incident cc_incident
                                                                                                                                                                                                        ON              cc_claimcontactrole.claimcontactid_stg =cc_incident.id_stg
                                                                                                                                                                                                        WHERE           (
                                                                                                                                                                                                        cc_contact.updatetime_stg>(:start_dttm)
                                                                                                                                                                                                        AND             cc_contact.updatetime_stg <= (:end_dttm) )
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        cc_user.updatetime_stg>(:start_dttm)
                                                                                                                                                                                                        AND             cc_user.updatetime_stg <=(:end_dttm) )
                                                                                                                                                                                                        UNION
                                                                                                                                                                                                        SELECT          ab_abcontact.updatetime_stg           AS updatetime,
                                                                                                                                                                                                        ''''                                    AS publicid,
                                                                                                                                                                                                        ab_abcontact.linkid_stg               AS linkid,
                                                                                                                                                                                                        ab_abcontact.retired_stg              AS retired,
                                                                                                                                                                                                        ab_abcontact.createtime_stg           AS createtime,
                                                                                                                                                                                                        ab_abcontact.dateofbirth_stg          AS dateofbirth,
                                                                                                                                                                                                        abtl_gendertype.typecode_stg          AS gndr_type_cd,
                                                                                                                                                                                                        abtl_taxfilingstatustype.typecode_stg AS tax_filg_type_cd,
                                                                                                                                                                                                        abtl_abcontact.name_stg               AS tl_cnt_name,
                                                                                                                                                                                                        abtl_taxstatus.typecode_stg           AS tax_id_sts_cd,
                                                                                                                                                                                                        ''SRC_SYS7''                            AS sys_src_cd,
                                                                                                                                                                                                        cast(''ContactManager'' AS VARCHAR(50)) AS source
                                                                                                                                                                                                        FROM            db_t_prod_stag.ab_abcontact ab_abcontact
                                                                                                                                                                                                        left outer join db_t_prod_stag.abtl_abcontact abtl_abcontact
                                                                                                                                                                                                        ON              abtl_abcontact.id_stg = ab_abcontact.subtype_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.abtl_gendertype abtl_gendertype
                                                                                                                                                                                                        ON              ab_abcontact.gender_stg = abtl_gendertype.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.abtl_taxfilingstatustype abtl_taxfilingstatustype
                                                                                                                                                                                                        ON              ab_abcontact.taxfilingstatus_stg = abtl_taxfilingstatustype.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.abtl_taxstatus abtl_taxstatus
                                                                                                                                                                                                        ON              ab_abcontact.taxstatus_stg = abtl_taxstatus.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.abtl_maritalstatus abtl_maritalstatus
                                                                                                                                                                                                        ON              ab_abcontact.maritalstatus_stg = abtl_maritalstatus.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.abtl_nameprefix abtl_nameprefix
                                                                                                                                                                                                        ON              ab_abcontact.prefix_stg = abtl_nameprefix.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.abtl_namesuffix abtl_namesuffix
                                                                                                                                                                                                        ON              ab_abcontact.suffix_stg = abtl_namesuffix.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.ab_user ab_user
                                                                                                                                                                                                        ON              ab_user.contactid_stg = ab_abcontact.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.ab_credential ab_credential
                                                                                                                                                                                                        ON              ab_user.credentialid_stg = ab_credential.id_stg
                                                                                                                                                                                                        left outer join db_t_prod_stag.abtl_occupation abtl_occupation
                                                                                                                                                                                                        ON              abtl_occupation.id_stg = ab_abcontact.occupation_alfa_stg
                                                                                                                                                                                                        WHERE           (
                                                                                                                                                                                                        ab_abcontact.updatetime_stg>(:start_dttm)
                                                                                                                                                                                                        AND             ab_abcontact.updatetime_stg <= (:end_dttm)) ) src
                                                                                                                                                                                 WHERE  addressbookuid IS NOT NULL ) ) x qualify row_number() over(PARTITION BY state,county,postalcode,city,addressline1,addressline2,country_id,state_id,state_typecode,country_typecode,addtype_typecode,addressbookuid,source ORDER BY updatetime DESC, createtime DESC) = 1 ) src_qry
                                                                                                                                                      /* *****************************************************************************
Source Query ends here
lkp_teradata_etl_ref_xlat_busn_ctgy_cd*/
                                                                                                                                            left join
                                                                                                                                                      (
                                                                                                                                                                      SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                                                                                      teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                                                                                      FROM            db_t_prod_core.teradata_etl_ref_xlat teradata_etl_ref_xlat
                                                                                                                                                                      WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm IN (''BUSN_CTGY'',
                                                                                                                                                                                                        ''ORG_TYPE'',
                                                                                                                                                                                                        ''PRTY_TYPE'')
                                                                                                                                                                      AND             teradata_etl_ref_xlat.src_idntftn_nm IN (''derived'',
                                                                                                                                                                                                        ''cctl_contact.typecode'',
                                                                                                                                                                                                        ''cctl_contact.name'',
                                                                                                                                                                                                        ''abtl_abcontact.name'')
                                                                                                                                                                      AND             teradata_etl_ref_xlat.src_idntftn_sys IN (''DS'',
                                                                                                                                                                                                        ''GW'')
                                                                                                                                                                      AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) xlat_busn_ctgy_cd
                                                                                                                                               ON     src_qry.tl_cnt_name = xlat_busn_ctgy_cd.src_idntftn_val )) sq1
                                                                                                                                        /*lkp_indiv_cnt_mgr*/
                                                                                                                              left join
                                                                                                                                        (
                                                                                                                                                        SELECT DISTINCT indiv.indiv_prty_id AS indiv_prty_id,
                                                                                                                                                                        indiv.nk_link_id    AS nk_link_id
                                                                                                                                                        FROM            db_t_prod_core.indiv indiv
                                                                                                                                                        WHERE           indiv.nk_publc_id IS NULL qualify row_number() over(PARTITION BY nk_link_id ORDER BY edw_end_dttm DESC,edw_strt_dttm DESC)=1 ) indiv_cnt_mgr
                                                                                                                              ON        sq1.addressbookuid = indiv_cnt_mgr.nk_link_id
                                                                                                                                        /*lkp_indiv_clm_ctr*/
                                                                                                                              left join
                                                                                                                                        (
                                                                                                                                                        SELECT DISTINCT indiv.indiv_prty_id AS indiv_prty_id,
                                                                                                                                                                        indiv.nk_publc_id   AS nk_publc_id
                                                                                                                                                        FROM            db_t_prod_core.indiv indiv
                                                                                                                                                        WHERE           indiv.nk_publc_id IS NOT NULL qualify row_number() over(PARTITION BY nk_publc_id ORDER BY edw_end_dttm DESC,edw_strt_dttm DESC)=1 ) indiv_clm_ctr
                                                                                                                              ON        sq1.addressbookuid = indiv_clm_ctr.nk_publc_id
                                                                                                                                        /*lkp_busn*/
                                                                                                                              left join
                                                                                                                                        (
                                                                                                                                                        SELECT DISTINCT busn.busn_prty_id     AS busn_prty_id,
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
                                                                                                                                                        FROM            db_t_prod_core.busn busn qualify row_number () over ( PARTITION BY nk_busn_cd,busn_ctgy_cd ORDER BY edw_end_dttm DESC )=1 ) lkp_busn
                                                                                                                              ON        sq1.busn_ctgy_cd = lkp_busn.busn_ctgy_cd
                                                                                                                              AND       sq1.addressbookuid = lkp_busn.nk_busn_cd
                                                                                                                                        /*lkp_ctry*/
                                                                                                                              left join
                                                                                                                                        (
                                                                                                                                                        SELECT DISTINCT ctry_id,
                                                                                                                                                                        geogrcl_area_shrt_name
                                                                                                                                                        FROM            db_t_prod_core.ctry ctry qualify row_number() over(PARTITION BY geogrcl_area_shrt_name ORDER BY edw_end_dttm DESC,edw_strt_dttm DESC)=1 ) ctry
                                                                                                                              ON        sq1.ctry_typecode = ctry.geogrcl_area_shrt_name )) sq2
                                                                                                                    /*lkp_terr*/
                                                                                                          left join
                                                                                                                    (
                                                                                                                                    SELECT DISTINCT terr_id,
                                                                                                                                                    ctry_id,
                                                                                                                                                    geogrcl_area_shrt_name
                                                                                                                                    FROM            db_t_prod_core.terr terr
                                                                                                                                    WHERE           terr.edw_end_dttm=''9999-12-31 23:59:59.999999'' qualify row_number() over(PARTITION BY ctry_id,geogrcl_area_shrt_name ORDER BY edw_end_dttm DESC,edw_strt_dttm DESC)=1 ) terr
                                                                                                          ON        sq2.out_ctry_id = terr.ctry_id
                                                                                                          AND       sq2.state_typecode = terr.geogrcl_area_shrt_name )) sq3
                                                                                                /*lkp_cnty*/
                                                                                      left join
                                                                                                (
                                                                                                         SELECT   cnty_id,
                                                                                                                  terr_id,
                                                                                                                  geogrcl_area_shrt_name
                                                                                                         FROM     db_t_prod_core.cnty cnty
                                                                                                         WHERE    edw_end_dttm=''9999-12-31 23:59:59.999999'' qualify row_number() over(PARTITION BY terr_id,geogrcl_area_shrt_name ORDER BY edw_end_dttm DESC,edw_strt_dttm DESC)=1 ) cnty
                                                                                      ON        sq3.out_terr_id = cnty.terr_id
                                                                                      AND       sq3.county = cnty.geogrcl_area_shrt_name
                                                                                                /* lkp_postl_cd*/
                                                                                      left join
                                                                                                (
                                                                                                                SELECT DISTINCT postl_cd_id,
                                                                                                                                ctry_id,
                                                                                                                                postl_cd_num
                                                                                                                FROM            db_t_prod_core.postl_cd postl_cd
                                                                                                                WHERE           edw_end_dttm=''9999-12-31 23:59:59.999999'' qualify row_number() over(PARTITION BY ctry_id,postl_cd_num ORDER BY edw_end_dttm DESC,edw_strt_dttm DESC)=1 ) lkp_postl_cd
                                                                                      ON        sq3.out_ctry_id = lkp_postl_cd.ctry_id
                                                                                      AND       sq3.postalcode = lkp_postl_cd.postl_cd_num
                                                                                                /* lkp_city */
                                                                                      left join
                                                                                                (
                                                                                                                SELECT DISTINCT city_id,
                                                                                                                                terr_id,
                                                                                                                                geogrcl_area_shrt_name
                                                                                                                FROM            db_t_prod_core.city city
                                                                                                                WHERE           edw_end_dttm=''9999-12-31 23:59:59.999999'' qualify row_number() over(PARTITION BY terr_id,geogrcl_area_shrt_name ORDER BY edw_end_dttm DESC,edw_strt_dttm DESC)=1 ) lkp_city
                                                                                      ON        sq3.out_terr_id = lkp_city.terr_id
                                                                                      AND       city = lkp_city.geogrcl_area_shrt_name )) sq4
                                                                            /*lkp_street_addr*/
                                                                  left join
                                                                            (
                                                                                     SELECT   street_addr.street_addr_id      AS street_addr_id,
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
                                                                                     FROM     db_t_prod_core.street_addr street_addr
                                                                                     WHERE    street_addr.edw_end_dttm=''9999-12-31 23:59:59.999999''
                                                                                     AND      street_addr.addr_ln_3_txt IS NULL qualify row_number() over(PARTITION BY addr_ln_1_txt,addr_ln_2_txt,addr_ln_3_txt,city_id,terr_id,postl_cd_id,ctry_id,cnty_id,edw_end_dttm ORDER BY edw_end_dttm DESC,edw_strt_dttm DESC)=1 ) lkp_street_addr
                                                                  ON        lkp_street_addr.addr_ln_1_txt = sq4.addressline1
                                                                  AND       coalesce(lkp_street_addr.addr_ln_2_txt, ''~'') = coalesce(sq4.addressline2, ''~'')
                                                                  AND       lkp_street_addr.city_id = sq4.out_city_id
                                                                  AND       lkp_street_addr.terr_id = sq4.out_terr_id
                                                                  AND       lkp_street_addr.postl_cd_id = sq4.out_postl_cd_id
                                                                  AND       lkp_street_addr.ctry_id = sq4.out_ctry_id
                                                                  AND       coalesce(lkp_street_addr.cnty_id, ''~'') = coalesce(sq4.out_cnty_id, ''~'')
                                                                            /*lkp_teradata_etl_ref_xlat_intrnl_org_type*/
                                                                  left join db_t_prod_core.teradata_etl_ref_xlat xlat_intrnl_org_type
                                                                  ON        org_type = xlat_intrnl_org_type.src_idntftn_val
                                                                  AND       xlat_intrnl_org_type.tgt_idntftn_nm= ''INTRNL_ORG_TYPE''
                                                                  AND       xlat_intrnl_org_type.src_idntftn_nm= ''derived''
                                                                  AND       xlat_intrnl_org_type.src_idntftn_sys=''DS''
                                                                  AND       xlat_intrnl_org_type.expn_dt=''9999-12-31''
                                                                            /*lkp_teradata_etl_ref_xlat_intrnl_org_sbtype*/
                                                                  left join db_t_prod_core.teradata_etl_ref_xlat xlat_intrnl_org_sbtype
                                                                  ON        sq4.org_sbtype = xlat_intrnl_org_sbtype.src_idntftn_val
                                                                  AND       xlat_intrnl_org_sbtype.tgt_idntftn_nm= ''INTRNL_ORG_SBTYPE''
                                                                  AND       xlat_intrnl_org_type.expn_dt=''9999-12-31''
                                                                            /*lkp_teradata_etl_ref_xlat*/
                                                                  left join
                                                                            (
                                                                                            SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                            teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                            FROM            db_t_prod_core.teradata_etl_ref_xlat teradata_etl_ref_xlat
                                                                                            WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''LOCTR_USGE_TYPE''
                                                                                            AND             teradata_etl_ref_xlat.src_idntftn_sys IN (''GW'' ,
                                                                                                                                                      ''DS'')
                                                                                            AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) teradata_etl_ref_xlat
                                                                  ON        sq4.addrtype_typecode = teradata_etl_ref_xlat.src_idntftn_val
                                                                            /*lkp_teradata_etl_ref_xlat_src_sys*/
                                                                  left join db_t_prod_core.teradata_etl_ref_xlat xlat_src_sys
                                                                  ON        src_sys = xlat_src_sys.src_idntftn_val
                                                                  AND       xlat_src_sys.tgt_idntftn_nm= ''SRC_SYS''
                                                                  AND       xlat_src_sys.expn_dt=''9999-12-31'' )) sq5
                                                        /*lkp_intrnl_org*/
                                              left join
                                                        (
                                                                        SELECT DISTINCT intrnl_org.intrnl_org_prty_id   AS intrnl_org_prty_id,
                                                                                        intrnl_org.intrnl_org_type_cd   AS intrnl_org_type_cd,
                                                                                        intrnl_org.intrnl_org_sbtype_cd AS intrnl_org_sbtype_cd,
                                                                                        intrnl_org.intrnl_org_num       AS intrnl_org_num,
                                                                                        intrnl_org.src_sys_cd           AS src_sys_cd
                                                                        FROM            db_t_prod_core.intrnl_org intrnl_org
                                                                        WHERE           edw_end_dttm=''9999-12-31 23:59:59.999999'' qualify row_number() over(PARTITION BY intrnl_org_type_cd,intrnl_org_sbtype_cd,intrnl_org_num ORDER BY edw_end_dttm DESC, edw_strt_dttm DESC)=1 ) lkp_intrnl_org
                                              ON        lkp_intrnl_org.intrnl_org_type_cd = sq5.v_org_type
                                              AND       lkp_intrnl_org.intrnl_org_sbtype_cd = sq5.v_org_sbtype
                                              AND       lkp_intrnl_org.intrnl_org_num = sq5.org_key
                                                        /*lkp_tlphn_num*/
                                              left join
                                                        (
                                                                        SELECT DISTINCT tlphn_num_id,
                                                                                        tlphn_num
                                                                        FROM            db_t_prod_core.tlphn_num
                                                                        WHERE           edw_end_dttm=''9999-12-31 23:59:59.999999'' qualify row_number() over(PARTITION BY tlphn_num ORDER BY edw_end_dttm DESC, edw_strt_dttm DESC)=1 ) lkp_tlphn_num1
                                              ON        sq5.primaryphone = lkp_tlphn_num1.tlphn_num
                                                        /*lkp_tlphn_num*/
                                              left join
                                                        (
                                                                        SELECT DISTINCT tlphn_num_id,
                                                                                        tlphn_num
                                                                        FROM            db_t_prod_core.tlphn_num
                                                                        WHERE           edw_end_dttm=''9999-12-31 23:59:59.999999'' qualify row_number() over(PARTITION BY tlphn_num ORDER BY edw_end_dttm DESC, edw_strt_dttm DESC)=1 ) lkp_tlphn_num2
                                              ON        sq5.cellphone = lkp_tlphn_num2.tlphn_num
                                                        /*lkp_tlphn_num*/
                                              left join
                                                        (
                                                                        SELECT DISTINCT tlphn_num_id,
                                                                                        tlphn_num
                                                                        FROM            db_t_prod_core.tlphn_num
                                                                        WHERE           edw_end_dttm=''9999-12-31 23:59:59.999999'' qualify row_number() over(PARTITION BY tlphn_num ORDER BY edw_end_dttm DESC, edw_strt_dttm DESC)=1 ) lkp_tlphn_num3
                                              ON        sq5.homephone = lkp_tlphn_num3.tlphn_num
                                                        /*lkp_tlphn_num*/
                                              left join
                                                        (
                                                                        SELECT DISTINCT tlphn_num_id,
                                                                                        tlphn_num
                                                                        FROM            db_t_prod_core.tlphn_num
                                                                        WHERE           edw_end_dttm=''9999-12-31 23:59:59.999999'' qualify row_number() over(PARTITION BY tlphn_num ORDER BY edw_end_dttm DESC, edw_strt_dttm DESC)=1 ) lkp_tlphn_num4
                                              ON        sq5.workphone = lkp_tlphn_num4.tlphn_num ) ), normalizer AS
                                    (
                                           SELECT prty_addr_usge_type_cd_in   AS prty_addr_usge_type_cd,
                                                  loc_id_in1                  AS loc_id,
                                                  in_prty_id                  AS prty_id,
                                                  prty_addr_start_datetime_in AS prty_addr_start_datetime,
                                                  retired_in                  AS retired,
                                                  updatetime_in               AS updatetime,
                                                  createtime_in               AS createtime,
                                                  1                           AS gcid_loc_id
                                           FROM   terr
                                           UNION ALL
                                           SELECT prty_addr_usge_type_cd_in   AS prty_addr_usge_type_cd,
                                                  loc_id_in2                  AS loc_id,
                                                  in_prty_id                  AS prty_id,
                                                  prty_addr_start_datetime_in AS prty_addr_start_datetime,
                                                  retired_in                  AS retired,
                                                  updatetime_in               AS updatetime,
                                                  createtime_in               AS createtime,
                                                  2                           AS gcid_loc_id
                                           FROM   terr
                                           UNION ALL
                                           SELECT prty_addr_usge_type_cd_in   AS prty_addr_usge_type_cd,
                                                  loc_id_in3                  AS loc_id,
                                                  in_prty_id                  AS prty_id,
                                                  prty_addr_start_datetime_in AS prty_addr_start_datetime,
                                                  retired_in                  AS retired,
                                                  updatetime_in               AS updatetime,
                                                  createtime_in               AS createtime,
                                                  3                           AS gcid_loc_id
                                           FROM   terr
                                           UNION ALL
                                           SELECT prty_addr_usge_type_cd_in   AS prty_addr_usge_type_cd,
                                                  loc_id_in4                  AS loc_id,
                                                  in_prty_id                  AS prty_id,
                                                  prty_addr_start_datetime_in AS prty_addr_start_datetime,
                                                  retired_in                  AS retired,
                                                  updatetime_in               AS updatetime,
                                                  createtime_in               AS createtime,
                                                  4                           AS gcid_loc_id
                                           FROM   terr
                                           UNION ALL
                                           SELECT prty_addr_usge_type_cd_in   AS prty_addr_usge_type_cd,
                                                  loc_id_in5                  AS loc_id,
                                                  in_prty_id                  AS prty_id,
                                                  prty_addr_start_datetime_in AS prty_addr_start_datetime,
                                                  retired_in                  AS retired,
                                                  updatetime_in               AS updatetime,
                                                  createtime_in               AS createtime,
                                                  5                           AS gcid_loc_id
                                           FROM   terr
                                           UNION ALL
                                           SELECT prty_addr_usge_type_cd_in   AS prty_addr_usge_type_cd,
                                                  loc_id_in6                  AS loc_id,
                                                  in_prty_id                  AS prty_id,
                                                  prty_addr_start_datetime_in AS prty_addr_start_datetime,
                                                  retired_in                  AS retired,
                                                  updatetime_in               AS updatetime,
                                                  createtime_in               AS createtime,
                                                  6                           AS gcid_loc_id
                                           FROM   terr
                                           UNION ALL
                                           SELECT prty_addr_usge_type_cd_in   AS prty_addr_usge_type_cd,
                                                  loc_id_in7                  AS loc_id,
                                                  in_prty_id                  AS prty_id,
                                                  prty_addr_start_datetime_in AS prty_addr_start_datetime,
                                                  retired_in                  AS retired,
                                                  updatetime_in               AS updatetime,
                                                  createtime_in               AS createtime,
                                                  7                           AS gcid_loc_id
                                           FROM   terr
                                           UNION ALL
                                           SELECT prty_addr_usge_type_cd_in   AS prty_addr_usge_type_cd,
                                                  loc_id_in8                  AS loc_id,
                                                  in_prty_id                  AS prty_id,
                                                  prty_addr_start_datetime_in AS prty_addr_start_datetime,
                                                  retired_in                  AS retired,
                                                  updatetime_in               AS updatetime,
                                                  createtime_in               AS createtime,
                                                  8                           AS gcid_loc_id
                                           FROM   terr
                                           UNION ALL
                                           SELECT prty_addr_usge_type_cd_in   AS prty_addr_usge_type_cd,
                                                  loc_id_in9                  AS loc_id,
                                                  in_prty_id                  AS prty_id,
                                                  prty_addr_start_datetime_in AS prty_addr_start_datetime,
                                                  retired_in                  AS retired,
                                                  updatetime_in               AS updatetime,
                                                  createtime_in               AS createtime,
                                                  9                           AS gcid_loc_id
                                           FROM   terr
                                           UNION ALL
                                           SELECT prty_addr_usge_type_cd_in   AS prty_addr_usge_type_cd,
                                                  loc_id_in10                 AS loc_id,
                                                  in_prty_id                  AS prty_id,
                                                  prty_addr_start_datetime_in AS prty_addr_start_datetime,
                                                  retired_in                  AS retired,
                                                  updatetime_in               AS updatetime,
                                                  createtime_in               AS createtime,
                                                  10                          AS gcid_loc_id
                                           FROM   terr )
                             /*Main Query*/
                             SELECT DISTINCT target_lkp_prty_loctr.loc_id               AS lkp_loc_id,
                                             target_lkp_prty_loctr.loctr_usge_type_cd   AS lkp_loctr_usge_type_cd,
                                             target_lkp_prty_loctr.prty_loctr_strt_dttm AS lkp_prty_loctr_strt_dttm,
                                             target_lkp_prty_loctr.prty_id              AS lkp_prty_id,
                                             target_lkp_prty_loctr.data_qlty_cd         AS lkp_data_qlty_cd,
                                             target_lkp_prty_loctr.edw_strt_dttm        AS lkp_edw_strt_dttm,
                                             target_lkp_prty_loctr.edw_end_dttm         AS lkp_edw_end_dttm,
                                             msq5.loc_id                                AS in_loc_id,
                                             msq5.prty_addr_usge_type_cd                AS in_prty_addr_usge_type_cd,
                                             msq5.prty_id                               AS in_prty_id,
                                             msq5.out_createtime                        AS in_prty_loctr_strt_dttm,
                                             msq5.data_qlty_cd                          AS in_data_qlty_cd,
                                             msq5.edw_strt_dttm                         AS in_edw_strt_dttm,
                                             msq5.edw_end_dttm                          AS in_edw_end_dttm,
                                             msq5.out_prcs_id                           AS in_prcs_id,
                                             cast(trim(cast(lkp_loc_id AS                             VARCHAR(100)))
                                                             || trim(cast(lkp_prty_loctr_strt_dttm AS VARCHAR(100)))
                                                             || trim(cast(lkp_data_qlty_cd AS         VARCHAR(100))) AS VARCHAR(100)) AS var_orig_chksm,
                                             cast(trim(cast(in_loc_id AS                              VARCHAR(100)))
                                                             || trim(cast(in_prty_loctr_strt_dttm AS  VARCHAR(100)))
                                                             || trim(cast(in_data_qlty_cd AS          VARCHAR(100))) AS VARCHAR(100)) AS var_calc_chksm,
                                             CASE
                                                             WHEN var_orig_chksm IS NULL THEN ''I''
                                                             WHEN var_orig_chksm <> var_calc_chksm THEN ''U''
                                                             ELSE ''R''
                                             END                                             AS calc_ins_upd,
                                             cast(''9999-12-31 23:59:59.999999'' AS timestamp) AS busn_end_dttm,
                                             msq5.retired                                    AS retired,
                                             msq5.out_updatetime                             AS trans_strt_dttm,
                                             msq5.out_createtime                             AS out_createtime,
                                             msq5.rankindex                                  AS rankindex
                             FROM            (
                                             (
                                                    SELECT loc_id,
                                                           prty_id,
                                                           out_prcs_id,
                                                           data_qlty_cd,
                                                           edw_strt_dttm,
                                                           edw_end_dttm,
                                                           o_prty_addr_start_datetime,
                                                           retired,
                                                           gcid_loc_id,
                                                           o_prty_addr_usge_type_cd AS prty_addr_usge_type_cd,
                                                           out_updatetime,
                                                           out_createtime,
                                                           rankindex
                                                    FROM   (
                                                           (
                                                                    SELECT   loc_id,
                                                                             prty_id,
                                                                             out_prcs_id,
                                                                             data_qlty_cd,
                                                                             edw_strt_dttm,
                                                                             edw_end_dttm,
                                                                             o_prty_addr_start_datetime,
                                                                             retired,
                                                                             gcid_loc_id,
                                                                             o_prty_addr_usge_type_cd,
                                                                             out_updatetime,
                                                                             out_createtime ,
                                                                             rank() over(PARTITION BY prty_id, o_prty_addr_usge_type_cd ORDER BY out_updatetime DESC, gcid_loc_id ASC) AS rankindex
                                                                    FROM     (
                                                                             (
                                                                                       SELECT    loc_id,
                                                                                                 prty_id,
                                                                                                 out_prcs_id,
                                                                                                 data_qlty_cd,
                                                                                                 edw_strt_dttm,
                                                                                                 edw_end_dttm,
                                                                                                 o_prty_addr_start_datetime,
                                                                                                 retired,
                                                                                                 gcid_loc_id,
                                                                                                 CASE
                                                                                                           WHEN etl_ref_xlat_msq3.tgt_idntftn_val IS NULL THEN ''UNK''
                                                                                                           ELSE etl_ref_xlat_msq3.tgt_idntftn_val
                                                                                                 END        AS o_prty_addr_usge_type_cd,
                                                                                                 updatetime AS updatetime,
                                                                                                 CASE
                                                                                                           WHEN updatetime IS NULL THEN cast(cast(''1900-01-01'' AS DATE ) AS timestamp)
                                                                                                           ELSE cast(updatetime AS timestamp)
                                                                                                 END        AS out_updatetime,
                                                                                                 createtime AS createtime,
                                                                                                 CASE
                                                                                                           WHEN createtime IS NULL THEN cast(cast(''1900-01-01'' AS DATE) AS timestamp)
                                                                                                           ELSE cast(createtime AS timestamp)
                                                                                                 END AS out_createtime
                                                                                       FROM      (
                                                                                                 (
                                                                                                           SELECT    loc_id,
                                                                                                                     prty_id,
                                                                                                                     :PRCS_ID                                        AS out_prcs_id,
                                                                                                                     ''UNK''                                           AS data_qlty_cd,
                                                                                                                     CURRENT_DATE                                    AS edw_strt_dttm,
                                                                                                                     cast(''9999-12-31 23:59:59.999999'' AS timestamp) AS edw_end_dttm,
                                                                                                                     CASE
                                                                                                                               WHEN prty_addr_start_datetime IS NULL THEN cast(''1900-01-01'' AS DATE)
                                                                                                                               ELSE prty_addr_start_datetime
                                                                                                                     END AS o_prty_addr_start_datetime,
                                                                                                                     retired,
                                                                                                                     gcid_loc_id,
                                                                                                                     CASE
                                                                                                                               WHEN gcid_loc_id=7 THEN etl_ref_xlat_generic.tgt_idntftn_val
                                                                                                                               WHEN gcid_loc_id=8 THEN etl_ref_xlat_cell.tgt_idntftn_val
                                                                                                                               WHEN gcid_loc_id=9 THEN etl_ref_xlat_home.tgt_idntftn_val
                                                                                                                               WHEN gcid_loc_id=10 THEN etl_ref_xlat_work.tgt_idntftn_val
                                                                                                                               ELSE ''UNK''
                                                                                                                     END AS prty_addr_usge_type_cd_phn,
                                                                                                                     CASE
                                                                                                                               WHEN gcid_loc_id=2
                                                                                                                               AND       prty_addr_usge_type_cd=''billing'' THEN ''LOCTR_USGE_TYPE9''
                                                                                                                               WHEN gcid_loc_id=2
                                                                                                                               AND       prty_addr_usge_type_cd=''business'' THEN ''LOCTR_USGE_TYPE29''
                                                                                                                               WHEN gcid_loc_id=2
                                                                                                                               AND       prty_addr_usge_type_cd=''home'' THEN ''LOCTR_USGE_TYPE14''
                                                                                                                               WHEN gcid_loc_id=2
                                                                                                                               AND       prty_addr_usge_type_cd=''Mailing_alfa'' THEN ''LOCTR_USGE_TYPE19''
                                                                                                                               WHEN gcid_loc_id=2
                                                                                                                               AND       prty_addr_usge_type_cd=''other'' THEN ''LOCTR_USGE_TYPE24''
                                                                                                                               WHEN gcid_loc_id=3
                                                                                                                               AND       prty_addr_usge_type_cd=''billing'' THEN ''LOCTR_USGE_TYPE11''
                                                                                                                               WHEN gcid_loc_id=3
                                                                                                                               AND       prty_addr_usge_type_cd=''business'' THEN ''LOCTR_USGE_TYPE31''
                                                                                                                               WHEN gcid_loc_id=3
                                                                                                                               AND       prty_addr_usge_type_cd=''home'' THEN ''LOCTR_USGE_TYPE16''
                                                                                                                               WHEN gcid_loc_id=3
                                                                                                                               AND       prty_addr_usge_type_cd=''Mailing_alfa'' THEN ''LOCTR_USGE_TYPE21''
                                                                                                                               WHEN gcid_loc_id=3
                                                                                                                               AND       prty_addr_usge_type_cd=''other'' THEN ''LOCTR_USGE_TYPE26''
                                                                                                                               WHEN gcid_loc_id=4
                                                                                                                               AND       prty_addr_usge_type_cd=''billing'' THEN ''LOCTR_USGE_TYPE12''
                                                                                                                               WHEN gcid_loc_id=4
                                                                                                                               AND       prty_addr_usge_type_cd=''business'' THEN ''LOCTR_USGE_TYPE32''
                                                                                                                               WHEN gcid_loc_id=4
                                                                                                                               AND       prty_addr_usge_type_cd=''home'' THEN ''LOCTR_USGE_TYPE17''
                                                                                                                               WHEN gcid_loc_id=4
                                                                                                                               AND       prty_addr_usge_type_cd=''Mailing_alfa'' THEN ''LOCTR_USGE_TYPE22''
                                                                                                                               WHEN gcid_loc_id=4
                                                                                                                               AND       prty_addr_usge_type_cd=''other'' THEN ''LOCTR_USGE_TYPE27''
                                                                                                                               WHEN gcid_loc_id=5
                                                                                                                               AND       prty_addr_usge_type_cd=''billing'' THEN ''LOCTR_USGE_TYPE10''
                                                                                                                               WHEN gcid_loc_id=5
                                                                                                                               AND       prty_addr_usge_type_cd=''business'' THEN ''LOCTR_USGE_TYPE30''
                                                                                                                               WHEN gcid_loc_id=5
                                                                                                                               AND       prty_addr_usge_type_cd=''home'' THEN ''LOCTR_USGE_TYPE15''
                                                                                                                               WHEN gcid_loc_id=5
                                                                                                                               AND       prty_addr_usge_type_cd=''Mailing_alfa'' THEN ''LOCTR_USGE_TYPE20''
                                                                                                                               WHEN gcid_loc_id=5
                                                                                                                               AND       prty_addr_usge_type_cd=''other'' THEN ''LOCTR_USGE_TYPE25''
                                                                                                                               WHEN gcid_loc_id=6
                                                                                                                               AND       prty_addr_usge_type_cd=''billing'' THEN ''LOCTR_USGE_TYPE13''
                                                                                                                               WHEN gcid_loc_id=6
                                                                                                                               AND       prty_addr_usge_type_cd=''business'' THEN ''LOCTR_USGE_TYPE33''
                                                                                                                               WHEN gcid_loc_id=6
                                                                                                                               AND       prty_addr_usge_type_cd=''home'' THEN ''LOCTR_USGE_TYPE18''
                                                                                                                               WHEN gcid_loc_id=6
                                                                                                                               AND       prty_addr_usge_type_cd=''Mailing_alfa'' THEN ''LOCTR_USGE_TYPE23''
                                                                                                                               WHEN gcid_loc_id=6
                                                                                                                               AND       prty_addr_usge_type_cd=''other'' THEN ''LOCTR_USGE_TYPE28''
                                                                                                                               WHEN gcid_loc_id=2
                                                                                                                               AND       prty_addr_usge_type_cd=''PRTY_ADDR_USGE_TYPE8'' THEN ''LOCTR_USGE_TYPE29''
                                                                                                                               WHEN gcid_loc_id=3
                                                                                                                               AND       prty_addr_usge_type_cd=''PRTY_ADDR_USGE_TYPE8'' THEN ''LOCTR_USGE_TYPE31''
                                                                                                                               WHEN gcid_loc_id=4
                                                                                                                               AND       prty_addr_usge_type_cd=''PRTY_ADDR_USGE_TYPE8'' THEN ''LOCTR_USGE_TYPE32''
                                                                                                                               WHEN gcid_loc_id=5
                                                                                                                               AND       prty_addr_usge_type_cd=''PRTY_ADDR_USGE_TYPE8'' THEN ''LOCTR_USGE_TYPE30''
                                                                                                                               WHEN gcid_loc_id=6
                                                                                                                               AND       prty_addr_usge_type_cd=''PRTY_ADDR_USGE_TYPE8'' THEN ''LOCTR_USGE_TYPE33''
                                                                                                                               WHEN prty_addr_usge_type_cd=''LOCTR_USGE_TYPE3'' THEN ''LOCTR_USGE_TYPE3''
                                                                                                                               WHEN gcid_loc_id=7 THEN ''LOCTR_USGE_TYPE4''
                                                                                                                               WHEN gcid_loc_id=8 THEN ''Cell''
                                                                                                                               WHEN gcid_loc_id=9 THEN ''Home''
                                                                                                                               WHEN gcid_loc_id=10 THEN ''Work''
                                                                                                                               ELSE ''UNK''
                                                                                                                     END AS var_prty_addr_usge_type_cd,
                                                                                                                     updatetime,
                                                                                                                     createtime
                                                                                                           FROM      (
                                                                                                                     /*Main Query Result after Normalizer*/
                                                                                                                     (
                                                                                                                            SELECT prty_addr_usge_type_cd,
                                                                                                                                   loc_id,
                                                                                                                                   prty_id,
                                                                                                                                   prty_addr_start_datetime,
                                                                                                                                   retired,
                                                                                                                                   updatetime,
                                                                                                                                   createtime,
                                                                                                                                   gcid_loc_id
                                                                                                                            FROM   normalizer ) msq1
                                                                                                                     /*lkp_teradata_etl_ref_xlat Generic*/
                                                                                                           left join
                                                                                                                     (
                                                                                                                                     SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                                                     teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                                                     FROM            db_t_prod_core.teradata_etl_ref_xlat teradata_etl_ref_xlat
                                                                                                                                     WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''LOCTR_USGE_TYPE''
                                                                                                                                     AND             teradata_etl_ref_xlat.src_idntftn_sys IN (''GW'' ,
                                                                                                                                                                                               ''DS'')
                                                                                                                                     AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) etl_ref_xlat_generic
                                                                                                           ON        etl_ref_xlat_generic.src_idntftn_val = ''Generic''
                                                                                                                     /*lkp_teradata_etl_ref_xlat Cell*/
                                                                                                           left join
                                                                                                                     (
                                                                                                                                     SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                                                     teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                                                     FROM            db_t_prod_core.teradata_etl_ref_xlat teradata_etl_ref_xlat
                                                                                                                                     WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''LOCTR_USGE_TYPE''
                                                                                                                                     AND             teradata_etl_ref_xlat.src_idntftn_sys IN (''GW'' ,
                                                                                                                                                                                               ''DS'')
                                                                                                                                     AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) etl_ref_xlat_cell
                                                                                                           ON        etl_ref_xlat_cell.src_idntftn_val = ''Cell''
                                                                                                                     /*lkp_teradata_etl_ref_xlat Home*/
                                                                                                           left join
                                                                                                                     (
                                                                                                                                     SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                                                     teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                                                     FROM            db_t_prod_core.teradata_etl_ref_xlat teradata_etl_ref_xlat
                                                                                                                                     WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''LOCTR_USGE_TYPE''
                                                                                                                                     AND             teradata_etl_ref_xlat.src_idntftn_sys IN (''GW'' ,
                                                                                                                                                                                               ''DS'')
                                                                                                                                     AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) etl_ref_xlat_home
                                                                                                           ON        etl_ref_xlat_home.src_idntftn_val = ''Home''
                                                                                                                     /*lkp_teradata_etl_ref_xlat Work*/
                                                                                                           left join
                                                                                                                     (
                                                                                                                                     SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                                                     teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                                                     FROM            db_t_prod_core.teradata_etl_ref_xlat teradata_etl_ref_xlat
                                                                                                                                     WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''LOCTR_USGE_TYPE''
                                                                                                                                     AND             teradata_etl_ref_xlat.src_idntftn_sys IN (''GW'' ,
                                                                                                                                                                                               ''DS'')
                                                                                                                                     AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) etl_ref_xlat_work
                                                                                                           ON        etl_ref_xlat_work.src_idntftn_val = ''Work'' )) msq2
                                                                                                 /*lkp_teradata_etl_ref_xlat*/
                                                                                       left join
                                                                                                 (
                                                                                                                 SELECT DISTINCT teradata_etl_ref_xlat.tgt_idntftn_val AS tgt_idntftn_val ,
                                                                                                                                 teradata_etl_ref_xlat.src_idntftn_val AS src_idntftn_val
                                                                                                                 FROM            db_t_prod_core.teradata_etl_ref_xlat teradata_etl_ref_xlat
                                                                                                                 WHERE           teradata_etl_ref_xlat.tgt_idntftn_nm= ''LOCTR_USGE_TYPE''
                                                                                                                 AND             teradata_etl_ref_xlat.src_idntftn_sys IN (''GW'' ,
                                                                                                                                                                           ''DS'')
                                                                                                                 AND             teradata_etl_ref_xlat.expn_dt=''9999-12-31'' ) etl_ref_xlat_msq3
                                                                                       ON        msq2.var_prty_addr_usge_type_cd = etl_ref_xlat_msq3.src_idntftn_val )) msq3 )) msq4 )
                                                    WHERE  rankindex = 1
                                                    AND    gcid_loc_id <> 1 ) msq5
                                             /*target (lkp_prty_loctr)*/
                             left join
                                             (
                                                      SELECT   prty_loctr.prty_loctr_strt_dttm AS prty_loctr_strt_dttm,
                                                               prty_loctr.data_qlty_cd         AS data_qlty_cd,
                                                               prty_loctr.edw_strt_dttm        AS edw_strt_dttm,
                                                               prty_loctr.edw_end_dttm         AS edw_end_dttm,
                                                               prty_loctr.loc_id               AS loc_id,
                                                               prty_loctr.loctr_usge_type_cd   AS loctr_usge_type_cd,
                                                               prty_loctr.prty_id              AS prty_id
                                                      FROM     db_t_prod_core.prty_loctr prty_loctr qualify row_number() over( PARTITION BY prty_loctr.loctr_usge_type_cd,prty_loctr.prty_id ORDER BY prty_loctr.edw_end_dttm DESC) = 1 ) target_lkp_prty_loctr
                             ON              target_lkp_prty_loctr.loctr_usge_type_cd = msq5.prty_addr_usge_type_cd
                             AND             target_lkp_prty_loctr.prty_id = msq5.prty_id ) ) src ) );
    -- Component exp_input_lookup, Type EXPRESSION
    CREATE
    OR
    replace TEMPORARY TABLE exp_input_lookup AS
    (
           SELECT sq_prty_add_loctr_x.lkp_loc_id                AS lkp_loc_id,
                  sq_prty_add_loctr_x.lkp_loctr_usge_type_cd    AS lkp_loctr_usge_type_cd,
                  sq_prty_add_loctr_x.lkp_prty_loctr_strt_dttm  AS lkp_prty_loctr_strt_dttm,
                  sq_prty_add_loctr_x.lkp_prty_id               AS lkp_prty_id,
                  sq_prty_add_loctr_x.lkp_data_qlty_cd          AS lkp_data_qlty_cd,
                  sq_prty_add_loctr_x.lkp_edw_strt_dttm         AS lkp_edw_strt_dttm,
                  sq_prty_add_loctr_x.lkp_edw_end_dttm          AS lkp_edw_end_dttm,
                  sq_prty_add_loctr_x.in_loc_id                 AS in_loc_id,
                  sq_prty_add_loctr_x.in_prty_addr_usge_type_cd AS in_prty_addr_usge_type_cd,
                  sq_prty_add_loctr_x.in_prty_id                AS in_prty_id,
                  sq_prty_add_loctr_x.in_prty_loctr_strt_dttm   AS in_prty_loctr_strt_dttm,
                  sq_prty_add_loctr_x.in_data_qlty_cd           AS in_data_qlty_cd,
                  sq_prty_add_loctr_x.in_edw_strt_dttm          AS in_edw_strt_dttm,
                  sq_prty_add_loctr_x.in_edw_end_dttm           AS in_edw_end_dttm,
                  sq_prty_add_loctr_x.in_prcs_id                AS in_prcs_id,
                  sq_prty_add_loctr_x.retired                   AS retired,
                  sq_prty_add_loctr_x.calc_ins_upd              AS calc_ins_upd,
                  sq_prty_add_loctr_x.trans_strt_dttm           AS trans_strt_dttm,
                  sq_prty_add_loctr_x.out_createtime            AS out_createtime,
                  sq_prty_add_loctr_x.busn_end_dttm             AS busn_end_dttm,
                  sq_prty_add_loctr_x.rankindex                 AS rankindex,
                  sq_prty_add_loctr_x.source_record_id
           FROM   sq_prty_add_loctr_x );
    -- Component rtr_cdc_Insert, Type ROUTER Output Group Insert
    CREATE
    OR
    replace TEMPORARY TABLE rtr_cdc_insert AS
    SELECT exp_input_lookup.lkp_loc_id                AS lkp_loc_id,
           exp_input_lookup.lkp_loctr_usge_type_cd    AS lkp_loctr_usge_type_cd,
           exp_input_lookup.lkp_prty_loctr_strt_dttm  AS lkp_prty_loctr_strt_dttm,
           exp_input_lookup.lkp_prty_id               AS lkp_prty_id,
           exp_input_lookup.lkp_data_qlty_cd          AS lkp_data_qlty_cd,
           exp_input_lookup.lkp_edw_strt_dttm         AS lkp_edw_strt_dttm,
           exp_input_lookup.lkp_edw_end_dttm          AS lkp_edw_end_dttm,
           exp_input_lookup.in_loc_id                 AS in_loc_id,
           exp_input_lookup.in_prty_addr_usge_type_cd AS in_prty_addr_usge_type_cd,
           exp_input_lookup.in_prty_id                AS in_prty_id,
           exp_input_lookup.in_prty_loctr_strt_dttm   AS in_prty_loctr_strt_dttm,
           exp_input_lookup.in_data_qlty_cd           AS in_data_qlty_cd,
           exp_input_lookup.in_edw_strt_dttm          AS in_edw_strt_dttm,
           exp_input_lookup.in_edw_end_dttm           AS in_edw_end_dttm,
           exp_input_lookup.in_prcs_id                AS in_prcs_id,
           exp_input_lookup.retired                   AS retired,
           exp_input_lookup.calc_ins_upd              AS calc_ins_upd,
           exp_input_lookup.trans_strt_dttm           AS trans_strt_dttm,
           exp_input_lookup.out_createtime            AS out_createtime,
           exp_input_lookup.rankindex                 AS rankindex,
           exp_input_lookup.busn_end_dttm             AS busn_end_dttm,
           exp_input_lookup.source_record_id
    FROM   exp_input_lookup
    WHERE  ( (
                         exp_input_lookup.calc_ins_upd = ''I''
                  AND    exp_input_lookup.in_prty_id <> 9999
                  AND    exp_input_lookup.in_prty_id IS NOT NULL
                  AND    exp_input_lookup.in_loc_id IS NOT NULL )
           OR     exp_input_lookup.calc_ins_upd = ''U''
           AND    exp_input_lookup.lkp_edw_end_dttm = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' )
           AND    exp_input_lookup.in_prty_id IS NOT NULL
           AND    exp_input_lookup.in_loc_id IS NOT NULL )
    OR     (
                  exp_input_lookup.retired = 0
           AND    exp_input_lookup.lkp_edw_end_dttm != to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
    
    -- Component rtr_cdc_Retire, Type ROUTER Output Group Retire
    CREATE
    OR
    replace TEMPORARY TABLE rtr_cdc_retire AS
    SELECT exp_input_lookup.lkp_loc_id                AS lkp_loc_id,
           exp_input_lookup.lkp_loctr_usge_type_cd    AS lkp_loctr_usge_type_cd,
           exp_input_lookup.lkp_prty_loctr_strt_dttm  AS lkp_prty_loctr_strt_dttm,
           exp_input_lookup.lkp_prty_id               AS lkp_prty_id,
           exp_input_lookup.lkp_data_qlty_cd          AS lkp_data_qlty_cd,
           exp_input_lookup.lkp_edw_strt_dttm         AS lkp_edw_strt_dttm,
           exp_input_lookup.lkp_edw_end_dttm          AS lkp_edw_end_dttm,
           exp_input_lookup.in_loc_id                 AS in_loc_id,
           exp_input_lookup.in_prty_addr_usge_type_cd AS in_prty_addr_usge_type_cd,
           exp_input_lookup.in_prty_id                AS in_prty_id,
           exp_input_lookup.in_prty_loctr_strt_dttm   AS in_prty_loctr_strt_dttm,
           exp_input_lookup.in_data_qlty_cd           AS in_data_qlty_cd,
           exp_input_lookup.in_edw_strt_dttm          AS in_edw_strt_dttm,
           exp_input_lookup.in_edw_end_dttm           AS in_edw_end_dttm,
           exp_input_lookup.in_prcs_id                AS in_prcs_id,
           exp_input_lookup.retired                   AS retired,
           exp_input_lookup.calc_ins_upd              AS calc_ins_upd,
           exp_input_lookup.trans_strt_dttm           AS trans_strt_dttm,
           exp_input_lookup.out_createtime            AS out_createtime,
           exp_input_lookup.rankindex                 AS rankindex,
           exp_input_lookup.busn_end_dttm             AS busn_end_dttm,
           exp_input_lookup.source_record_id
    FROM   exp_input_lookup
    WHERE  exp_input_lookup.calc_ins_upd = ''R''
    AND    exp_input_lookup.retired != 0
    AND    exp_input_lookup.lkp_edw_end_dttm = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' );
    
    -- Component rtr_cdc_Update, Type ROUTER Output Group Update
    CREATE
    OR
    replace TEMPORARY TABLE rtr_cdc_update AS
    SELECT exp_input_lookup.lkp_loc_id                AS lkp_loc_id,
           exp_input_lookup.lkp_loctr_usge_type_cd    AS lkp_loctr_usge_type_cd,
           exp_input_lookup.lkp_prty_loctr_strt_dttm  AS lkp_prty_loctr_strt_dttm,
           exp_input_lookup.lkp_prty_id               AS lkp_prty_id,
           exp_input_lookup.lkp_data_qlty_cd          AS lkp_data_qlty_cd,
           exp_input_lookup.lkp_edw_strt_dttm         AS lkp_edw_strt_dttm,
           exp_input_lookup.lkp_edw_end_dttm          AS lkp_edw_end_dttm,
           exp_input_lookup.in_loc_id                 AS in_loc_id,
           exp_input_lookup.in_prty_addr_usge_type_cd AS in_prty_addr_usge_type_cd,
           exp_input_lookup.in_prty_id                AS in_prty_id,
           exp_input_lookup.in_prty_loctr_strt_dttm   AS in_prty_loctr_strt_dttm,
           exp_input_lookup.in_data_qlty_cd           AS in_data_qlty_cd,
           exp_input_lookup.in_edw_strt_dttm          AS in_edw_strt_dttm,
           exp_input_lookup.in_edw_end_dttm           AS in_edw_end_dttm,
           exp_input_lookup.in_prcs_id                AS in_prcs_id,
           exp_input_lookup.retired                   AS retired,
           exp_input_lookup.calc_ins_upd              AS calc_ins_upd,
           exp_input_lookup.trans_strt_dttm           AS trans_strt_dttm,
           exp_input_lookup.out_createtime            AS out_createtime,
           exp_input_lookup.rankindex                 AS rankindex,
           exp_input_lookup.busn_end_dttm             AS busn_end_dttm,
           exp_input_lookup.source_record_id
    FROM   exp_input_lookup
    WHERE  1 = 2
           /*-- exp_input_lookup.calc_ins_upd = ''U'' AND exp_input_lookup.lkp_EDW_END_DTTM = TO_DATE ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) and exp_input_lookup.in_PRTY_ID <> 9999*/
           ;
    
    -- Component upd_ins_upd, Type UPDATE
    CREATE
    OR
    replace TEMPORARY TABLE upd_ins_upd AS
    (
           /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
           SELECT rtr_cdc_update.in_loc_id                 AS in_loc_id,
                  rtr_cdc_update.in_prty_addr_usge_type_cd AS in_prty_addr_usge_type_cd,
                  rtr_cdc_update.in_prty_id                AS in_prty_id,
                  rtr_cdc_update.in_prty_loctr_strt_dttm   AS in_prty_loctr_strt_dttm,
                  rtr_cdc_update.in_data_qlty_cd           AS in_data_qlty_cd,
                  rtr_cdc_update.in_edw_strt_dttm          AS in_edw_strt_dttm,
                  rtr_cdc_update.in_edw_end_dttm           AS in_edw_end_dttm,
                  rtr_cdc_update.in_prcs_id                AS in_prcs_id1,
                  rtr_cdc_update.retired                   AS retired3,
                  rtr_cdc_update.trans_strt_dttm           AS trans_strt_dttm1,
                  0                                        AS update_strategy_action,
                  source_record_id
           FROM   rtr_cdc_update );
    -- Component upd_update, Type UPDATE
    CREATE
    OR
    replace TEMPORARY TABLE upd_update AS
    (
           /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
           SELECT rtr_cdc_update.lkp_loc_id             AS lkp_loc_id3,
                  rtr_cdc_update.lkp_loctr_usge_type_cd AS lkp_loctr_usge_type_cd3,
                  rtr_cdc_update.lkp_prty_id            AS lkp_prty_id3,
                  rtr_cdc_update.lkp_edw_strt_dttm      AS lkp_edw_strt_dttm3,
                  rtr_cdc_update.in_edw_strt_dttm       AS in_edw_strt_dttm3,
                  rtr_cdc_update.retired                AS retired3,
                  rtr_cdc_update.lkp_edw_end_dttm       AS lkp_edw_end_dttm3,
                  rtr_cdc_update.in_prcs_id             AS in_prcs_id3,
                  NULL                                  AS in_prty_loctr_strt_dttm4,
                  rtr_cdc_update.trans_strt_dttm        AS trans_strt_dttm3,
                  1                                     AS update_strategy_action,
                  rtr_cdc_update.source_record_id
           FROM   rtr_cdc_update );
    -- Component fil_prty_loctr_upd_update, Type FILTER
    CREATE
    OR
    replace TEMPORARY TABLE fil_prty_loctr_upd_update AS
    (
           SELECT upd_update.lkp_loc_id3             AS lkp_loc_id3,
                  upd_update.lkp_loctr_usge_type_cd3 AS lkp_loctr_usge_type_cd3,
                  upd_update.lkp_prty_id3            AS lkp_prty_id3,
                  upd_update.lkp_edw_strt_dttm3      AS lkp_edw_strt_dttm3,
                  upd_update.in_edw_strt_dttm3       AS in_edw_strt_dttm3,
                  upd_update.retired3                AS retired3,
                  upd_update.lkp_edw_end_dttm3       AS lkp_edw_end_dttm3,
                  upd_update.in_prcs_id3             AS in_prcs_id3,
                  upd_update.trans_strt_dttm3        AS trans_strt_dttm3,
                  upd_update.source_record_id
           FROM   upd_update
           WHERE  upd_update.lkp_edw_end_dttm3 = to_date ( ''12/31/9999 23:59:59.999999'' , ''MM/DD/YYYY HH24:MI:SS.FF6'' ) );
    -- Component upd_prty_loctr_Update_Retire_Reject, Type UPDATE
    CREATE
    OR
    replace TEMPORARY TABLE upd_prty_loctr_update_retire_reject AS
    (
           /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
           SELECT rtr_cdc_retire.lkp_loc_id             AS lkp_loc_id3,
                  rtr_cdc_retire.lkp_loctr_usge_type_cd AS lkp_loctr_usge_type_cd3,
                  rtr_cdc_retire.lkp_prty_id            AS lkp_prty_id3,
                  rtr_cdc_retire.lkp_edw_strt_dttm      AS lkp_edw_strt_dttm3,
                  rtr_cdc_retire.in_edw_strt_dttm       AS in_edw_strt_dttm3,
                  rtr_cdc_retire.in_prcs_id             AS in_prcs_id4,
                  rtr_cdc_retire.trans_strt_dttm        AS trans_strt_dttm4,
                  rtr_cdc_retire.out_createtime         AS out_createtime4,
                  1                                     AS update_strategy_action,
                  rtr_cdc_retire.source_record_id
           FROM   rtr_cdc_retire );
    -- Component upd_ins, Type UPDATE
    CREATE
    OR
    replace TEMPORARY TABLE upd_ins AS
    (
           /* UPDATE_STRATEGY_ACTION = 0 FOR INSERT / UPDATE_STRATEGY_ACTION = 1 FOR UPDATE / UPDATE_STRATEGY_ACTION = 2 FOR DELETE / UPDATE_STRATEGY_ACTION = 3 FOR REJECT */
           SELECT rtr_cdc_insert.in_loc_id                 AS in_loc_id,
                  rtr_cdc_insert.in_prty_addr_usge_type_cd AS in_prty_addr_usge_type_cd,
                  rtr_cdc_insert.in_prty_id                AS in_prty_id,
                  rtr_cdc_insert.in_prty_loctr_strt_dttm   AS in_prty_loctr_strt_dttm,
                  rtr_cdc_insert.in_data_qlty_cd           AS in_data_qlty_cd,
                  rtr_cdc_insert.in_edw_strt_dttm          AS in_edw_strt_dttm,
                  rtr_cdc_insert.in_edw_end_dttm           AS in_edw_end_dttm,
                  rtr_cdc_insert.in_prcs_id                AS in_prcs_id1,
                  rtr_cdc_insert.retired                   AS retired1,
                  rtr_cdc_insert.trans_strt_dttm           AS trans_strt_dttm1,
                  rtr_cdc_insert.out_createtime            AS out_createtime1,
                  rtr_cdc_insert.busn_end_dttm             AS busn_end_dttm1,
                  rtr_cdc_insert.rankindex                 AS rankindex1,
                  0                                        AS update_strategy_action,
                  rtr_cdc_insert.source_record_id
           FROM   rtr_cdc_insert );
    -- Component exp_pass_tgt_ins, Type EXPRESSION
    CREATE
    OR
    replace TEMPORARY TABLE exp_pass_tgt_ins AS
    (
           SELECT upd_ins.in_loc_id                 AS in_loc_id,
                  upd_ins.in_prty_addr_usge_type_cd AS in_prty_addr_usge_type_cd,
                  upd_ins.in_prty_id                AS in_prty_id,
                  upd_ins.in_data_qlty_cd           AS in_data_qlty_cd,
                  /*CASE
                         WHEN upd_ins.retired1 != 0 THEN v_edw_strt_dttm
                         ELSE upd_ins.in_edw_end_dttm
                  END                      AS o_edw_end_dttm,*/
                  upd_ins.in_prcs_id1      AS in_prcs_id1,
                  upd_ins.trans_strt_dttm1 AS trans_strt_dttm1,
                  CASE
                         WHEN upd_ins.retired1 != 0 THEN upd_ins.trans_strt_dttm1
                         ELSE to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' )
                  END                                                                       AS out_trans_end_dttm,
                  upd_ins.out_createtime1                                                   AS out_createtime1,
                  upd_ins.busn_end_dttm1                                                    AS busn_end_dttm1,
                  dateadd (second, ( 2 * ( upd_ins.rankindex1 - 1 ) ) , current_timestamp ) AS v_edw_strt_dttm,
                  CASE
                         WHEN upd_ins.retired1 != 0 THEN v_edw_strt_dttm
                         ELSE upd_ins.in_edw_end_dttm
                  END                                                                       AS o_edw_end_dttm,
                  v_edw_strt_dttm                                                           AS edw_strt_dttm,
                  upd_ins.source_record_id
           FROM   upd_ins );
    -- Component fil_prty_loctr_upd_ins, Type FILTER
    CREATE
    OR
    replace TEMPORARY TABLE fil_prty_loctr_upd_ins AS
    (
           SELECT upd_ins_upd.in_loc_id                 AS in_loc_id,
                  upd_ins_upd.in_prty_addr_usge_type_cd AS in_prty_addr_usge_type_cd,
                  upd_ins_upd.in_prty_id                AS in_prty_id,
                  upd_ins_upd.in_prty_loctr_strt_dttm   AS in_prty_loctr_strt_dttm,
                  upd_ins_upd.in_data_qlty_cd           AS in_data_qlty_cd,
                  upd_ins_upd.in_edw_strt_dttm          AS in_edw_strt_dttm,
                  upd_ins_upd.in_edw_end_dttm           AS in_edw_end_dttm,
                  upd_ins_upd.in_prcs_id1               AS in_prcs_id1,
                  upd_ins_upd.retired3                  AS retired3,
                  upd_ins_upd.trans_strt_dttm1          AS trans_strt_dttm1,
                  upd_ins_upd.source_record_id
           FROM   upd_ins_upd
           WHERE  upd_ins_upd.retired3 = 0 );
    -- Component exp_pass_tgt_upd, Type EXPRESSION
    CREATE
    OR
    replace TEMPORARY TABLE exp_pass_tgt_upd AS
    (
           SELECT fil_prty_loctr_upd_update.lkp_loc_id3                             AS lkp_loc_id3,
                  fil_prty_loctr_upd_update.lkp_loctr_usge_type_cd3                 AS lkp_loctr_usge_type_cd3,
                  fil_prty_loctr_upd_update.lkp_prty_id3                            AS lkp_prty_id3,
                  fil_prty_loctr_upd_update.lkp_edw_strt_dttm3                      AS lkp_edw_strt_dttm3,
                  dateadd (second,-1, fil_prty_loctr_upd_update.in_edw_strt_dttm3 ) AS o_edw_end_dttm31,
                  dateadd (second,-1, fil_prty_loctr_upd_update.trans_strt_dttm3 )  AS trans_strt_dttm31,
                  fil_prty_loctr_upd_update.source_record_id
           FROM   fil_prty_loctr_upd_update );
    -- Component exp_pass_tgt_upd_ins, Type EXPRESSION
    CREATE
    OR
    replace TEMPORARY TABLE exp_pass_tgt_upd_ins AS
    (
           SELECT fil_prty_loctr_upd_ins.in_loc_id                                       AS in_loc_id,
                  fil_prty_loctr_upd_ins.in_prty_addr_usge_type_cd                       AS in_prty_addr_usge_type_cd,
                  fil_prty_loctr_upd_ins.in_prty_id                                      AS in_prty_id,
                  fil_prty_loctr_upd_ins.in_prty_loctr_strt_dttm                         AS in_prty_loctr_strt_dttm,
                  fil_prty_loctr_upd_ins.in_data_qlty_cd                                 AS in_data_qlty_cd,
                  fil_prty_loctr_upd_ins.in_edw_strt_dttm                                AS in_edw_strt_dttm,
                  fil_prty_loctr_upd_ins.in_edw_end_dttm                                 AS in_edw_end_dttm,
                  fil_prty_loctr_upd_ins.in_prcs_id1                                     AS in_prcs_id1,
                  fil_prty_loctr_upd_ins.trans_strt_dttm1                                AS trans_strt_dttm1,
                  to_date ( ''9999-12-31 23:59:59.999999'' , ''YYYY-MM-DD HH24:MI:SS.FF6'' ) AS o_trans_end_dttm,
                  fil_prty_loctr_upd_ins.source_record_id
           FROM   fil_prty_loctr_upd_ins );
    -- Component tgt_prty_loctr_upd_ins, Type TARGET
    INSERT INTO db_t_prod_core.prty_loctr
                (
                            loc_id,
                            loctr_usge_type_cd,
                            prty_loctr_strt_dttm,
                            prty_id,
                            data_qlty_cd,
                            prcs_id,
                            edw_strt_dttm,
                            edw_end_dttm,
                            trans_strt_dttm,
                            trans_end_dttm
                )
    SELECT exp_pass_tgt_upd_ins.in_loc_id                 AS loc_id,
           exp_pass_tgt_upd_ins.in_prty_addr_usge_type_cd AS loctr_usge_type_cd,
           exp_pass_tgt_upd_ins.in_prty_loctr_strt_dttm   AS prty_loctr_strt_dttm,
           exp_pass_tgt_upd_ins.in_prty_id                AS prty_id,
           exp_pass_tgt_upd_ins.in_data_qlty_cd           AS data_qlty_cd,
           exp_pass_tgt_upd_ins.in_prcs_id1               AS prcs_id,
           exp_pass_tgt_upd_ins.in_edw_strt_dttm          AS edw_strt_dttm,
           exp_pass_tgt_upd_ins.in_edw_end_dttm           AS edw_end_dttm,
           exp_pass_tgt_upd_ins.trans_strt_dttm1          AS trans_strt_dttm,
           exp_pass_tgt_upd_ins.o_trans_end_dttm          AS trans_end_dttm
    FROM   exp_pass_tgt_upd_ins;
    
    -- Component exp_prty_loctr_Update_Retire_Reject, Type EXPRESSION
    CREATE
    OR
    replace TEMPORARY TABLE exp_prty_loctr_update_retire_reject AS
    (
           SELECT upd_prty_loctr_update_retire_reject.lkp_loctr_usge_type_cd3 AS lkp_loctr_usge_type_cd3,
                  upd_prty_loctr_update_retire_reject.lkp_prty_id3            AS lkp_prty_id3,
                  upd_prty_loctr_update_retire_reject.lkp_edw_strt_dttm3      AS lkp_edw_strt_dttm3,
                  current_timestamp                                           AS o_edw_end_dttm31,
                  upd_prty_loctr_update_retire_reject.trans_strt_dttm4        AS trans_strt_dttm41,
                  upd_prty_loctr_update_retire_reject.source_record_id
           FROM   upd_prty_loctr_update_retire_reject );
    -- Component tgt_prty_loctr_upd, Type TARGET
    merge
    INTO         db_t_prod_core.prty_loctr
    USING        exp_pass_tgt_upd
    ON (
                              prty_loctr.loctr_usge_type_cd = exp_pass_tgt_upd.lkp_loctr_usge_type_cd3
                 AND          prty_loctr.prty_id = exp_pass_tgt_upd.lkp_prty_id3
                 AND          prty_loctr.edw_strt_dttm = exp_pass_tgt_upd.lkp_edw_strt_dttm3)
    WHEN matched THEN
    UPDATE
    SET    loc_id = exp_pass_tgt_upd.lkp_loc_id3,
           loctr_usge_type_cd = exp_pass_tgt_upd.lkp_loctr_usge_type_cd3,
           prty_id = exp_pass_tgt_upd.lkp_prty_id3,
           edw_strt_dttm = exp_pass_tgt_upd.lkp_edw_strt_dttm3,
           edw_end_dttm = exp_pass_tgt_upd.o_edw_end_dttm31,
           trans_end_dttm = exp_pass_tgt_upd.trans_strt_dttm31;
    
    -- Component tgt_prty_loctr_Update_Retire_Reject, Type TARGET
    merge
    INTO         db_t_prod_core.prty_loctr
    USING        exp_prty_loctr_update_retire_reject
    ON (
                              prty_loctr.loctr_usge_type_cd = exp_prty_loctr_update_retire_reject.lkp_loctr_usge_type_cd3
                 AND          prty_loctr.prty_id = exp_prty_loctr_update_retire_reject.lkp_prty_id3
                 AND          prty_loctr.edw_strt_dttm = exp_prty_loctr_update_retire_reject.lkp_edw_strt_dttm3)
    WHEN matched THEN
    UPDATE
    SET    loctr_usge_type_cd = exp_prty_loctr_update_retire_reject.lkp_loctr_usge_type_cd3,
           prty_id = exp_prty_loctr_update_retire_reject.lkp_prty_id3,
           edw_strt_dttm = exp_prty_loctr_update_retire_reject.lkp_edw_strt_dttm3,
           edw_end_dttm = exp_prty_loctr_update_retire_reject.o_edw_end_dttm31,
           trans_end_dttm = exp_prty_loctr_update_retire_reject.trans_strt_dttm41;
    
    -- Component tgt_prty_loctr_ins, Type TARGET
    INSERT INTO db_t_prod_core.prty_loctr
                (
                            loc_id,
                            loctr_usge_type_cd,
                            prty_loctr_strt_dttm,
                            prty_id,
                            prty_loctr_end_dttm,
                            data_qlty_cd,
                            prcs_id,
                            edw_strt_dttm,
                            edw_end_dttm,
                            trans_strt_dttm,
                            trans_end_dttm
                )
    SELECT exp_pass_tgt_ins.in_loc_id                 AS loc_id,
           exp_pass_tgt_ins.in_prty_addr_usge_type_cd AS loctr_usge_type_cd,
           exp_pass_tgt_ins.out_createtime1           AS prty_loctr_strt_dttm,
           exp_pass_tgt_ins.in_prty_id                AS prty_id,
           exp_pass_tgt_ins.busn_end_dttm1            AS prty_loctr_end_dttm,
           exp_pass_tgt_ins.in_data_qlty_cd           AS data_qlty_cd,
           exp_pass_tgt_ins.in_prcs_id1               AS prcs_id,
           exp_pass_tgt_ins.edw_strt_dttm             AS edw_strt_dttm,
           exp_pass_tgt_ins.o_edw_end_dttm            AS edw_end_dttm,
           exp_pass_tgt_ins.trans_strt_dttm1          AS trans_strt_dttm,
           exp_pass_tgt_ins.out_trans_end_dttm        AS trans_end_dttm
    FROM   exp_pass_tgt_ins;
    
    -- Component tgt_prty_loctr_ins, Type Post SQL
    UPDATE db_t_prod_core.prty_loctr
    SET    trans_end_dttm= a.lead,
           edw_end_dttm=a.lead1
    FROM   (
                           SELECT DISTINCT prty_id,
                                           loctr_usge_type_cd,
                                           edw_strt_dttm,
                                           max(edw_strt_dttm) over (PARTITION BY prty_id,loctr_usge_type_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following)   - interval ''1 SECOND'' AS lead1,
                                           max(trans_strt_dttm) over (PARTITION BY prty_id,loctr_usge_type_cd ORDER BY edw_strt_dttm ASC ROWS BETWEEN 1 following AND             1 following) - interval ''1 SECOND'' AS lead
                           FROM            db_t_prod_core.prty_loctr ) a
    WHERE  prty_loctr.edw_strt_dttm = a.edw_strt_dttm
    AND    prty_loctr.prty_id=a.prty_id
    AND    prty_loctr.loctr_usge_type_cd=a.loctr_usge_type_cd
    AND    prty_loctr.trans_strt_dttm <>prty_loctr.trans_end_dttm
    AND    lead IS NOT NULL;
  
  END;
  ';