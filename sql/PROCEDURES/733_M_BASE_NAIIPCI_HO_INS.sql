-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_NAIIPCI_HO_INS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '

DECLARE 
run_id string;
PRCS_ID INTEGER;
cc_EOY TIMESTAMP;
cc_BOY TIMESTAMP;
pc_EOY TIMESTAMP;
pc_BOY TIMESTAMP;
BEGIN 
run_id :=   (SELECT run_id   FROM control_run_id where upper(worklet_name) = upper(:worklet_name) order by insert_ts desc limit 1);   
PRCS_ID:=   (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PRCS_ID'' order by insert_ts desc limit 1);
CC_BOY := (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''CC_BOY'' order by insert_ts desc limit 1);
CC_EOY := (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''CC_EOY'' order by insert_ts desc limit 1);
PC_EOY := (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PC_EOY'' order by insert_ts desc limit 1);
PC_BOY := (SELECT param_value FROM control_params where run_id = :run_id and upper(param_name)=''PC_BOY'' order by insert_ts desc limit 1);

  -- PIPELINE START FOR 1
  -- Component SQ_pc_policyperiod, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_pc_policyperiod AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS companynumber,
                $2  AS lineofbusinesscode,
                $3  AS statecode,
                $4  AS callyear,
                $5  AS accountingyear,
                $6  AS expperiodyear,
                $7  AS expperiodmonth,
                $8  AS expeperiodday,
                $9  AS coveragecode,
                $10 AS classificationcode,
                $11 AS territorycode,
                $12 AS stateexceptionind,
                $13 AS zipcode,
                $14 AS policyeffectiveyear,
                $15 AS newrecordformat,
                $16 AS aslob,
                $17 AS itemcode,
                $18 AS sublinecode,
                $19 AS policyprogramcode,
                $20 AS policyformcode,
                $21 AS numberoffamilycodes,
                $22 AS constructioncode,
                $23 AS protectioncasscode,
                $24 AS exceptioncode,
                $25 AS typeofdeductiblecode,
                $26 AS policytermcode,
                $27 AS typeoflosscode,
                $28 AS stateofexceptionb,
                $29 AS amountofinsurance,
                $30 AS yeaofconstructionliablt,
                $31 AS coveragecodeordlaw,
                $32 AS exposurecode,
                $33 AS leadpoisoningliability,
                $34 AS deductibleindicator,
                $35 AS deductibleamount,
                $36 AS deductibleindicatorws,
                $37 AS deductibleamountws,
                $38 AS claimidentifier,
                $39 AS claimantidentifier,
                $40 AS writtenexposure,
                $41 AS writtenpremium,
                $42 AS paidlosses,
                $43 AS paidnumberofclaims,
                $44 AS outstandinglosses,
                $45 AS outstandingnoofclaims,
                $46 AS policynumber,
                $47 AS policyperiodid,
                $48 AS policyidentifier,
                $49 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     ( WITH terr AS
                                  (
                                            SELECT    branchid_stg                                                                                                             id,
                                                      max(cast(coalesce(old_code,pht1.naiipcicode_alfa_stg, pht2.naiipcicode_alfa_stg, pht3.naiipcicode_alfa_stg) AS INTEGER) )code,
                                                      max(postalcodeinternal)                                                                                                  postalcodeinternal
                                            FROM      (
                                                                SELECT    e.branchid_stg,
                                                                          policynumber_stg,
                                                                          c.code_stg,
                                                                          g.typecode_stg,
                                                                          c.countycode_alfa_stg,
                                                                          pc_policyline.hopolicytype_stg,
                                                                          coalesce(postalcodeinternal_stg, postalcode_stg)postalcodeinternal,
                                                                          row_number() over( PARTITION BY e.branchid_stg,policynumber_stg, c.code_stg,g.typecode_stg, c.countycode_alfa_stg, pc_policyline.hopolicytype_stg ORDER BY
                                                                          CASE
                                                                                    WHEN (
                                                                                                        c.policylocation_stg = b.id_stg) THEN 1
                                                                                    ELSE 2
                                                                          END) ROWNUM,
                                                                          cityinternal_stg,
                                                                          countyinternal_stg,
                                                                          county_stg,
                                                                          CASE
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       upper(cityinternal_stg)= ''BIRMINGHAM''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''JEFFERSON'' THEN ''32''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       upper(cityinternal_stg)= ''HUNTSVILLE''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''MADISON'' THEN ''35''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       upper(cityinternal_stg)= ''MONTGOMERY''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''MONTGOMERY'' THEN ''37''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       upper(cityinternal_stg)= ''MOBILE''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''MOBILE'' THEN ''30''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''AUTAUGA'',
                                                                                                                                                   ''ELMORE'',
                                                                                                                                                   ''MONTGOMERY'') THEN ''38''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''BALDWIN''
                                                                                    AND       cast(c.code_stg AS INTEGER)=26 THEN ''41''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''BALDWIN''
                                                                                    AND       cast(c.code_stg AS INTEGER)=28 THEN ''31''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''BALDWIN''
                                                                                    AND       (
                                                                                                        cast(c.code_stg AS INTEGER)=11
                                                                                              OR        cast(c.code_stg AS INTEGER) IS NULL
                                                                                              OR        cast(c.code_stg AS INTEGER) IN (1,2,3) ) THEN ''05''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''BARBOUR'',
                                                                                                                                                   ''BIBB'',
                                                                                                                                                   ''BLOUNT'',
                                                                                                                                                   ''BULLOCK'',
                                                                                                                                                   ''BUTLER'',
                                                                                                                                                   ''CHAMBERS'',
                                                                                                                                                   ''CHEROKEE'',
                                                                                                                                                   ''CHILTON'',
                                                                                                                                                   ''CHOCTAW'',
                                                                                                                                                   ''CLARKE'',
                                                                                                                                                   ''CLAY'',
                                                                                                                                                   ''CLEBURNE'',
                                                                                                                                                   ''COFFEE'',
                                                                                                                                                   ''CONECUH'',
                                                                                                                                                   ''COOSA'',
                                                                                                                                                   ''COVINGTON'',
                                                                                                                                                   ''CRENSHAW'',
                                                                                                                                                   ''CULLMAN'',
                                                                                                                                                   ''DALE'',
                                                                                                                                                   ''DALLAS'',
                                                                                                                                                   ''DE KALB'',
                                                                                                                                                   ''DEKALB'',
                                                                                                                                                   ''ESCAMBIA'',
                                                                                                                                                   ''FAYETTE'',
                                                                                                                                                   ''FRANKLIN'',
                                                                                                                                                   ''GENEVA'',
                                                                                                                                                   ''GREENE'',
                                                                                                                                                   ''HALE'',
                                                                                                                                                   ''HENRY'',
                                                                                                                                                   ''HOUSTON'',
                                                                                                                                                   ''JACKSON'',
                                                                                                                                                   ''LAMAR'',
                                                                                                                                                   ''LAWRENCE'',
                                                                                                                                                   ''LEE'',
                                                                                                                                                   ''LOWNDES'',
                                                                                                                                                   ''MACON'',
                                                                                                                                                   ''MONROE'',
                                                                                                                                                   ''MARENGO'',
                                                                                                                                                   ''MARION'',
                                                                                                                                                   ''MARSHALL'',
                                                                                                                                                   ''PERRY'',
                                                                                                                                                   ''PICKENS'',
                                                                                                                                                   ''PIKE'',
                                                                                                                                                   ''RANDOLPH'',
                                                                                                                                                   ''RUSSELL'',
                                                                                                                                                   ''SAINT CLAIR'',
                                                                                                                                                   ''ST. CLAIR'',
                                                                                                                                                   ''SUMTER'',
                                                                                                                                                   ''TALLADEGA'',
                                                                                                                                                   ''TALLAPOOSA'',
                                                                                                                                                   ''WASHINGTON'',
                                                                                                                                                   ''WILCOX'',
                                                                                                                                                   ''WINSTON'') THEN ''41''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''CALHOUN'',
                                                                                                                                                   ''ETOWAH'') THEN ''40''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''COLBERT'',
                                                                                                                                                   ''LAUDERDALE'',
                                                                                                                                                   ''LIMESTONE'',
                                                                                                                                                   ''MADISON'',
                                                                                                                                                   ''MORGAN'') THEN ''36''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''JEFFERSON'' THEN ''33''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''MOBILE''
                                                                                    AND       (
                                                                                                        cast(c.code_stg AS INTEGER)IN (2,1,26,3 )
                                                                                              OR        cast(c.code_stg AS INTEGER)IS NULL) THEN ''41''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''MOBILE''
                                                                                    AND       cast(c.code_stg AS INTEGER)=28 THEN ''31''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''MOBILE''
                                                                                    AND       cast(c.code_stg AS INTEGER)=11 THEN ''05''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''SHELBY'',
                                                                                                                                                   ''WALKER'') THEN ''34''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''TUSCALOOSA'' THEN ''39''
                                                                                    /*WHEN g.typecode_stg=''AL''
                                                                                    AND       (
                                                                                                        substring (b.postalcodeinternal_stg,0,INDEX(''-'',b.postalcodeinternal_stg))=''36511''
                                                                                              OR        b.postalcodeinternal_stg=''36511'')
                                                                                    AND       upper(cityinternal_stg)= ''BON SECOUR'' THEN ''06''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       (
                                                                                                        substring (b.postalcodeinternal_stg,0,INDEX(''-'',b.postalcodeinternal_stg))=''36528''
                                                                                              OR        b.postalcodeinternal_stg=''36528'')
                                                                                    AND       upper(cityinternal_stg)= ''DAUPHIN ISLAND'' THEN ''06''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       (
                                                                                                        substring (b.postalcodeinternal_stg,0,INDEX(''-'',b.postalcodeinternal_stg)) IN (''36542'',
                                                                                                                                                                                       ''36547'')
                                                                                              OR        b.postalcodeinternal_stg IN (''36542'',
                                                                                                                                     ''36547''))
                                                                                    AND       upper(cityinternal_stg)= ''GULF SHORES'' THEN ''06''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       (
                                                                                                        substring (b.postalcodeinternal_stg,0,INDEX(''-'',b.postalcodeinternal_stg))=''36561''
                                                                                              OR        b.postalcodeinternal_stg=''36561'')
                                                                                    AND       upper(cityinternal_stg)= ''ORANGE BEACH'' THEN ''06'' */
																					
																					WHEN g.typecode_stg = ''AL''
																						AND (
																						IFF(POSITION(''-'' IN b.postalcodeinternal_stg) > 0,
																							SUBSTRING(b.postalcodeinternal_stg, 1, POSITION(''-'' IN b.postalcodeinternal_stg) - 1),
																							b.postalcodeinternal_stg
																						) = ''36511''
																						OR b.postalcodeinternal_stg = ''36511''
																						)
																						AND UPPER(cityinternal_stg) = ''BON SECOUR''
																						THEN ''06''

																					WHEN g.typecode_stg = ''AL''
																						AND (
																						IFF(POSITION(''-'' IN b.postalcodeinternal_stg) > 0,
																							SUBSTRING(b.postalcodeinternal_stg, 1, POSITION(''-'' IN b.postalcodeinternal_stg) - 1),
																							b.postalcodeinternal_stg
																						) = ''36528''
																						OR b.postalcodeinternal_stg = ''36528''
																						)
																						AND UPPER(cityinternal_stg) = ''DAUPHIN ISLAND''
																						THEN ''06''

																					WHEN g.typecode_stg = ''AL''
																						AND (
																						IFF(POSITION(''-'' IN b.postalcodeinternal_stg) > 0,
																							SUBSTRING(b.postalcodeinternal_stg, 1, POSITION(''-'' IN b.postalcodeinternal_stg) - 1),
																							b.postalcodeinternal_stg
																						) IN (''36542'', ''36547'')
																						OR b.postalcodeinternal_stg IN (''36542'', ''36547'')
																						)
																						AND UPPER(cityinternal_stg) = ''GULF SHORES''
																						THEN ''06''

																					WHEN g.typecode_stg = ''AL''
																						AND (
																						IFF(POSITION(''-'' IN b.postalcodeinternal_stg) > 0,
																							SUBSTRING(b.postalcodeinternal_stg, 1, POSITION(''-'' IN b.postalcodeinternal_stg) - 1),
																							b.postalcodeinternal_stg
																						) = ''36561''
																						OR b.postalcodeinternal_stg = ''36561''
																						)
																						AND UPPER(cityinternal_stg) = ''ORANGE BEACH''
																						THEN ''06''


                                                                                    WHEN g.typecode_stg=''MS''
                                                                                    AND       upper(cityinternal_stg)= ''JACKSON''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''HINDS'',
                                                                                                                                                   ''RANKIN'') THEN ''30''
                                                                                    WHEN g.typecode_stg=''MS''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''AMITE'',
                                                                                                                                                   ''FORREST'',
                                                                                                                                                   ''GREENE'',
                                                                                                                                                   ''LAMAR'',
                                                                                                                                                   ''MARION'',
                                                                                                                                                   ''PERRY'',
                                                                                                                                                   ''PIKE'',
                                                                                                                                                   ''WALTHALL'',
                                                                                                                                                   ''WILKINSON'') THEN ''03''
                                                                                    WHEN g.typecode_stg=''MS''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''GEORGE'',
                                                                                                                                                   ''PEARL RIVER'',
                                                                                                                                                   ''STONE'') THEN ''05''
                                                                                    WHEN g.typecode_stg=''MS''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''HANCOCK'',
                                                                                                                                                   ''HARRISON'',
                                                                                                                                                   ''JACKSON'') THEN ''06''
                                                                                    WHEN g.typecode_stg=''MS''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''HINDS'',
                                                                                                                                                   ''MADISON'',
                                                                                                                                                   ''RANKIN'') THEN ''31''
                                                                                    WHEN g.typecode_stg=''MS''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''ADAMS'',
                                                                                                                                                   ''ALCORN'',
                                                                                                                                                   ''ATTALA'',
                                                                                                                                                   ''BENTON'',
                                                                                                                                                   ''BOLIVAR'',
                                                                                                                                                   ''CALHOUN'',
                                                                                                                                                   ''CARROLL'',
                                                                                                                                                   ''CHICKASAW'',
                                                                                                                                                   ''CHOCTAW'',
                                                                                                                                                   ''CLAIBORNE'',
                                                                                                                                                   ''CLARKE'',
                                                                                                                                                   ''CLAY'',
                                                                                                                                                   ''COAHOMA'',
                                                                                                                                                   ''COPIAH'',
                                                                                                                                                   ''COVINGTON'',
                                                                                                                                                   ''DESOTO'',
                                                                                                                                                   ''FRANKLIN'',
                                                                                                                                                   ''GRENADA'',
                                                                                                                                                   ''HOLMES'',
                                                                                                                                                   ''HUMPHREYS'',
                                                                                                                                                   ''ISSAQUENA'',
                                                                                                                                                   ''ITAWAMBA'',
                                                                                                                                                   ''JASPER'',
                                                                                                                                                   ''JEFFERSON'',
                                                                                                                                                   ''JEFFERSON DAVIS'',
                                                                                                                                                   ''JONES'',
                                                                                                                                                   ''KEMPER'',
                                                                                                                                                   ''LAFAYETTE'',
                                                                                                                                                   ''LAUDERDALE'',
                                                                                                                                                   ''LAWRENCE'',
                                                                                                                                                   ''LEAKE'',
                                                                                                                                                   ''LEE'',
                                                                                                                                                   ''LEFLORE'',
                                                                                                                                                   ''LINCOLN'',
                                                                                                                                                   ''LOWNDES'',
                                                                                                                                                   ''MARSHALL'',
                                                                                                                                                   ''MONROE'',
                                                                                                                                                   ''MONTGOMERY'',
                                                                                                                                                   ''NESHOBA'',
                                                                                                                                                   ''NEWTON'',
                                                                                                                                                   ''NOXUBEE'',
                                                                                                                                                   ''OKTIBBEHA'',
                                                                                                                                                   ''PANOLA'',
                                                                                                                                                   ''PONTOTOC'',
                                                                                                                                                   ''PRENTISS'',
                                                                                                                                                   ''QUITMAN'',
                                                                                                                                                   ''SCOTT'',
                                                                                                                                                   ''SHARKEY'',
                                                                                                                                                   ''SIMPSON'',
                                                                                                                                                   ''SMITH'',
                                                                                                                                                   ''SUNFLOWER'',
                                                                                                                                                   ''TALLAHATCHIE'',
                                                                                                                                                   ''TATE'',
                                                                                                                                                   ''TIPPAH'',
                                                                                                                                                   ''TISHOMINGO'',
                                                                                                                                                   ''TUNICA'',
                                                                                                                                                   ''UNION'',
                                                                                                                                                   ''WARREN'',
                                                                                                                                                   ''WASHINGTON'',
                                                                                                                                                   ''WAYNE'',
                                                                                                                                                   ''WEBSTER'',
                                                                                                                                                   ''WINSTON'',
                                                                                                                                                   ''YALOBUSHA'',
                                                                                                                                                   ''YAZOO'') THEN ''32''
                                                                                    WHEN g.typecode_stg=''GA''
                                                                                    AND       upper(cityinternal_stg)= ''ATLANTA''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''DE KALB'',
                                                                                                                                                   ''DEKALB'',
                                                                                                                                                   ''FULTON'') THEN ''32''
                                                                                    WHEN g.typecode_stg=''GA''
                                                                                    AND       upper(cityinternal_stg)= ''MACON''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''BIBB'' THEN ''35''
                                                                                    WHEN g.typecode_stg=''GA''
                                                                                    AND       upper(cityinternal_stg)= ''SAVANNAH''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''CHATHAM'' THEN ''30''
                                                                                    WHEN g.typecode_stg=''GA''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''DE KALB'',
                                                                                                                                                   ''DEKALB'',
                                                                                                                                                   ''FULTON'') THEN ''33''
                                                                                    WHEN g.typecode_stg=''GA''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''BRYAN'',
                                                                                                                                                   ''CAMDEN'',
                                                                                                                                                   ''CHATHAM'',
                                                                                                                                                   ''GLYNN'',
                                                                                                                                                   ''LIBERTY'',
                                                                                                                                                   ''MCINTOSH'') THEN ''31''
                                                                                    WHEN g.typecode_stg=''GA''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''DE KALB'',
                                                                                                                                                   ''DEKALB'',
                                                                                                                                                   ''FULTON'') THEN ''33''
                                                                                    WHEN g.typecode_stg=''GA''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''CLAYTON'',
                                                                                                                                                   ''COBB'',
                                                                                                                                                   ''GWINNETT'') THEN ''34''
                                                                                    WHEN g.typecode_stg=''GA''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''CATOOSA'',
                                                                                                                                                   ''WALKER'',
                                                                                                                                                   ''WHITFIELD'') THEN ''36''
                                                                                    WHEN g.typecode_stg=''GA''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) =''RICHMOND'' THEN ''37''
                                                                                    WHEN g.typecode_stg=''GA''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''CHATTAHOOCHEE'',
                                                                                                                                                   ''MUSCOGEE'') THEN ''38''
                                                                                    WHEN g.typecode_stg=''GA''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''BUTTS'',
                                                                                                                                                   ''CHEROKEE'',
                                                                                                                                                   ''DOUGLAS'',
                                                                                                                                                   ''FAYETTE'',
                                                                                                                                                   ''FORSYTH'',
                                                                                                                                                   ''HENRY'',
                                                                                                                                                   ''NEWTON'',
                                                                                                                                                   ''PAULDING'',
                                                                                                                                                   ''ROCKDALE'',
                                                                                                                                                   ''WALTON'') THEN ''39''
                                                                                    WHEN g.typecode_stg=''GA''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''BALDWIN'',
                                                                                                                                                   ''BANKS'',
                                                                                                                                                   ''BARROW'',
                                                                                                                                                   ''BARTOW'',
                                                                                                                                                   ''CARROLL'',
                                                                                                                                                   ''CHATTOOGA'',
                                                                                                                                                   ''CLARKE'',
                                                                                                                                                   ''COLUMBIA'',
                                                                                                                                                   ''COWETA'',
                                                                                                                                                   ''DADE'',
                                                                                                                                                   ''DAWSON'',
                                                                                                                                                   ''ELBERT'',
                                                                                                                                                   ''FANNIN'',
                                                                                                                                                   ''FLOYD'',
                                                                                                                                                   ''FRANKLIN'',
                                                                                                                                                   ''GILMER'',
                                                                                                                                                   ''GORDON'',
                                                                                                                                                   ''GREENE'',
                                                                                                                                                   ''HABERSHAM'',
                                                                                                                                                   ''HALL'',
                                                                                                                                                   ''HANCOCK'',
                                                                                                                                                   ''HARALSON'',
                                                                                                                                                   ''HART'',
                                                                                                                                                   ''HEARD'',
                                                                                                                                                   ''JACKSON'',
                                                                                                                                                   ''JASPER'',
                                                                                                                                                   ''JONES'',
                                                                                                                                                   ''LAMAR'',
                                                                                                                                                   ''LINCOLN'',
                                                                                                                                                   ''LUMPKIN'',
                                                                                                                                                   ''MADISON'',
                                                                                                                                                   ''MCDUFFIE'',
                                                                                                                                                   ''MERIWETHER'',
                                                                                                                                                   ''MONROE'',
                                                                                                                                                   ''MORGAN'',
                                                                                                                                                   ''MURRAY'',
                                                                                                                                                   ''OCONEE'',
                                                                                                                                                   ''OGLETHORPE'',
                                                                                                                                                   ''PICKENS'',
                                                                                                                                                   ''PIKE'',
                                                                                                                                                   ''POLK'',
                                                                                                                                                   ''PUTNAM'',
                                                                                                                                                   ''RABUN'',
                                                                                                                                                   ''SPALDING'',
                                                                                                                                                   ''STEPHENS'',
                                                                                                                                                   ''TALIAFERRO'',
                                                                                                                                                   ''TOWNS'',
                                                                                                                                                   ''TROUP'',
                                                                                                                                                   ''UNION'',
                                                                                                                                                   ''WARREN'',
                                                                                                                                                   ''WHITE'',
                                                                                                                                                   ''WILKES'') THEN ''40''
                                                                                    WHEN g.typecode_stg=''GA''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''BAKER'',
                                                                                                                                                   ''BIBB'',
                                                                                                                                                   ''BROOKS'',
                                                                                                                                                   ''CALHOUN'',
                                                                                                                                                   ''CLAY'',
                                                                                                                                                   ''COLQUITT'',
                                                                                                                                                   ''CRAWFORD'',
                                                                                                                                                   ''CRISP'',
                                                                                                                                                   ''DECATUR'',
                                                                                                                                                   ''DOOLY'',
                                                                                                                                                   ''DOUGHERTY'',
                                                                                                                                                   ''EARLY'',
                                                                                                                                                   ''GRADY'',
                                                                                                                                                   ''HARRIS'',
                                                                                                                                                   ''HOUSTON'',
                                                                                                                                                   ''LEE'',
                                                                                                                                                   ''MACON'',
                                                                                                                                                   ''MARION'',
                                                                                                                                                   ''MILLER'',
                                                                                                                                                   ''MITCHELL'',
                                                                                                                                                   ''PEACH'',
                                                                                                                                                   ''QUITMAN'',
                                                                                                                                                   ''RANDOLPH'',
                                                                                                                                                   ''SCHLEY'',
                                                                                                                                                   ''SEMINOLE'',
                                                                                                                                                   ''STEWART'',
                                                                                                                                                   ''SUMTER'',
                                                                                                                                                   ''TALBOT'',
                                                                                                                                                   ''TAYLOR'',
                                                                                                                                                   ''TERRELL'',
                                                                                                                                                   ''THOMAS'',
                                                                                                                                                   ''TIFT'',
                                                                                                                                                   ''TURNER'',
                                                                                                                                                   ''UPSON'',
                                                                                                                                                   ''WEBSTER'',
                                                                                                                                                   ''WORTH'') THEN ''41''
                                                                                    WHEN g.typecode_stg=''GA''
                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''APPLING'',
                                                                                                                                                   ''ATKINSON'',
                                                                                                                                                   ''BACON'',
                                                                                                                                                   ''BEN HILL'',
                                                                                                                                                   ''BERRIEN'',
                                                                                                                                                   ''BLECKLEY'',
                                                                                                                                                   ''BRANTLEY'',
                                                                                                                                                   ''BULLOCH'',
                                                                                                                                                   ''BURKE'',
                                                                                                                                                   ''CANDLER'',
                                                                                                                                                   ''CHARLTON'',
                                                                                                                                                   ''CLINCH'',
                                                                                                                                                   ''COFFEE'',
                                                                                                                                                   ''COOK'',
                                                                                                                                                   ''DODGE'',
                                                                                                                                                   ''ECHOLS'',
                                                                                                                                                   ''EFFINGHAM'',
                                                                                                                                                   ''EMANUEL'',
                                                                                                                                                   ''EVANS'',
                                                                                                                                                   ''GLASCOCK'',
                                                                                                                                                   ''IRWIN'',
                                                                                                                                                   ''JEFF DAVIS'',
                                                                                                                                                   ''JEFFERSON'',
                                                                                                                                                   ''JENKINS'',
                                                                                                                                                   ''JOHNSON'',
                                                                                                                                                   ''LANIER'',
                                                                                                                                                   ''LAURENS'',
                                                                                                                                                   ''LONG'',
                                                                                                                                                   ''LOWNDES'',
                                                                                                                                                   ''MONTGOMERY'',
                                                                                                                                                   ''PIERCE'',
                                                                                                                                                   ''PULASKI'',
                                                                                                                                                   ''SCREVEN'',
                                                                                                                                                   ''TATTNALL'',
                                                                                                                                                   ''TELFAIR'',
                                                                                                                                                   ''TOOMBS'',
                                                                                                                                                   ''TREUTlength'',
                                                                                                                                                   ''TWIGGS'',
                                                                                                                                                   ''WARE'',
                                                                                                                                                   ''WASHINGTON'',
                                                                                                                                                   ''WAYNE'',
                                                                                                                                                   ''WHEELER'',
                                                                                                                                                   ''WILCOX'',
                                                                                                                                                   ''WILKINSON'') THEN ''42''
                                                                          END AS old_code
                                                                FROM      db_t_prod_stag.pcx_holocation_hoe a
                                                                join      db_t_prod_stag.pcx_dwelling_hoe e
                                                                ON        e.holocation_stg=a.id_stg
                                                                join      db_t_prod_stag.pc_policyperiod f
                                                                ON        e.branchid_stg =f.id_stg
                                                                left join db_t_prod_stag.pc_effectivedatedfields eff
                                                                ON        eff.branchid_stg = f.id_stg
                                                                AND       eff.expirationdate_stg IS NULL
                                                                left join db_t_prod_stag.pc_policylocation b
                                                                ON        b.id_stg= eff.primarylocation_stg
                                                                AND       b.expirationdate_stg IS NULL
                                                                join      db_t_prod_stag.pc_territorycode c
                                                                ON        c.branchid_stg = f.id_stg
                                                                join      db_t_prod_stag.pctl_territorycode d
                                                                ON        c.subtype_stg=d.id_stg
                                                                AND       d.typecode_stg = ''HOTerritoryCode_alfa''
                                                                left join db_t_prod_stag.pc_contact pc
                                                                ON        pc.id_stg =pnicontactdenorm_stg
                                                                left join db_t_prod_stag.pc_address
                                                                ON        pc.primaryaddressid_stg = pc_address.id_stg
                                                                join      db_t_prod_stag.pc_policyline
                                                                ON        f.id_stg = pc_policyline.branchid_stg
                                                                AND       pc_policyline.expirationdate_stg IS NULL
                                                                join      db_t_prod_stag.pctl_hopolicytype_hoe
                                                                ON        pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                AND       pctl_hopolicytype_hoe.typecode_stg LIKE ''HO%''
                                                                join      db_t_prod_stag.pctl_jurisdiction g
                                                                ON        basestate_stg=g.id_stg
                                                                AND       g.typecode_stg IN (''AL'',
                                                                                             ''GA'',
                                                                                             ''MS'')) loc
                                            left join db_t_prod_stag.pcx_hodbterritory_alfa pht1
                                            ON        pht1.code_stg =loc.code_stg
                                            AND       cast(pht1.countycode_alfa_stg AS VARCHAR(50))=cast(loc.countycode_alfa_stg AS VARCHAR(50))
                                            AND       substring(pht1.publicid_stg,7,2) =loc.typecode_stg
                                            AND       pht1.hopolicytype_hoe_stg=loc.hopolicytype_stg
                                            left join
                                                      (
                                                                      SELECT DISTINCT naiipcicode_alfa_stg,
                                                                                      substring(publicid_stg,7,2) state,
                                                                                      code_stg                    territory_code,
                                                                                      hopolicytype_hoe_stg,
                                                                                      rank() over( PARTITION BY naiipcicode_alfa_stg, substring(publicid_stg,7,2) , code_stg , hopolicytype_hoe_stg ORDER BY naiipcicode_alfa_stg,countycode_alfa_stg )row1
                                                                      FROM            db_t_prod_stag.pcx_hodbterritory_alfa
                                                                      WHERE           publicid_stg LIKE ''%ho%'')pht2
                                            ON        pht2.row1=1
                                            AND       pht2.territory_code =loc.code_stg
                                            AND       pht2.state =loc.typecode_stg
                                            AND       pht2.hopolicytype_hoe_stg=loc.hopolicytype_stg
                                            left join
                                                      (
                                                                      SELECT DISTINCT naiipcicode_alfa_stg,
                                                                                      substring(publicid_stg,7,2) state,
                                                                                      countycode_alfa_stg         countycode_alfa_stg,
                                                                                      hopolicytype_hoe_stg,
                                                                                      rank() over( PARTITION BY naiipcicode_alfa_stg, substring(publicid_stg,7,2) , countycode_alfa_stg , hopolicytype_hoe_stg ORDER BY naiipcicode_alfa_stg,code_stg )row1
                                                                      FROM            db_t_prod_stag.pcx_hodbterritory_alfa
                                                                      WHERE           publicid_stg LIKE ''%ho%'')pht3
                                            ON        pht3.row1=1
                                            AND       cast(pht3.countycode_alfa_stg AS VARCHAR(50))=cast(loc.countycode_alfa_stg AS VARCHAR(50))
                                            AND       pht3.state =loc.typecode_stg
                                            AND       pht3.hopolicytype_hoe_stg=loc.hopolicytype_stg
                                            WHERE     ROWNUM=1
                                            GROUP BY  branchid_stg 
                                   ) ,   -- END OF TERR CTE
                                   cov AS
                                  (
                                                  SELECT DISTINCT branchid_stg,
                                                                  max(
                                                                  CASE
                                                                                  WHEN covterm.covtermpatternid=''HODW_Dwelling_Limit_HOE'' THEN (lpad(cast(cast(round(cast(polcov.val/1000 AS DECIMAL(18,4)), 0)AS INTEGER) AS VARCHAR(10)) ,5,''0''))
                                                                  END)dw_limit,
                                                                  max(
                                                                  CASE
                                                                                  WHEN covterm.covtermpatternid=''HODW_DwellingAdditionalLimit_alfa'' THEN cast(polcov.val AS DECIMAL(18,4))
                                                                  END )lia_limit,
                                                                  max(
                                                                  CASE
                                                                                  WHEN covterm.covtermpatternid=''HODW_PersonalPropertyLimit_alfa'' THEN
                                                                                                  CASE
                                                                                                                  WHEN length(polcov.val)>12 THEN 0000
                                                                                                                  ELSE substring(''00000'',1,(5-length( cast(cast(round(cast(polcov.val AS DECIMAL(18,4))/1000,0)AS INTEGER) AS VARCHAR(10)))))
                                                                                                                                                  || cast(cast(round(cast(polcov.val AS DECIMAL(18,4))/1000, 0)AS INTEGER) AS VARCHAR(10))
                                                                                                  END
                                                                  END )pp_limit,
                                                                  max(
                                                                  CASE
                                                                                  WHEN covterm.covtermpatternid=''HODW_OtherPerils_Ded_HOE'' THEN (
                                                                                                  CASE
                                                                                                                  WHEN substring(name1 ,length(name1),1)=''%'' THEN ''F''
                                                                                                                  ELSE ''D''
                                                                                                  END)
                                                                  END)perils_limit_ind,
                                                                  max(
                                                                  CASE
                                                                                  WHEN covterm.covtermpatternid=''HODW_OtherPerils_Ded_HOE'' THEN (
                                                                                                  CASE
                                                                                                                  WHEN substring(name1 ,length(name1),1)=''%'' THEN substring(''0000000'',1, (7-length(cast(cast(cast(cast(value1 AS DECIMAL(18,4)) *10000 AS DECIMAL(18,4)) AS INTEGER) AS VARCHAR(10)))))
                                                                                                                                                  ||cast(cast(cast(cast(value1 AS DECIMAL(18,4)) *10000 AS DECIMAL(18,4)) AS INTEGER) AS VARCHAR(10))
                                                                                                                  ELSE substring(''0000000'',1,(7-length(cast(cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                  ||cast(cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER) AS VARCHAR(10))
                                                                                                  END)
                                                                  END)perils_limit,
                                                                  max(
                                                                  CASE
                                                                                  WHEN patterncode =''HODW_Earthquake_HOE'' THEN patterncode
                                                                  END) earthquake,
                                                                  max(
                                                                  CASE
                                                                                  WHEN patterncode =''HODW_PersonalPropertyReplacementCost_alfa'' THEN patterncode
                                                                  END) replacement,
                                                                  max(
                                                                  CASE
                                                                                  WHEN (
                                                                                                                  covtermpatternid =''HODW_WindHail_Ded_HOE'') THEN covtermpatternid
                                                                  END )windhail,
                                                                  max(
                                                                  CASE
                                                                                  WHEN (
                                                                                                                  covtermpatternid =''HODW_Hurricane_Ded_HOE'' ) THEN covtermpatternid
                                                                  END )hurricane,
                                                                  max(
                                                                  CASE
                                                                                  WHEN (
                                                                                                                  covtermpatternid =''HODW_SectionI_DedWindAndHailExcl_alfa'') THEN covtermpatternid
                                                                  END )windstormhailexcl,
                                                                  max(
                                                                  CASE
                                                                                  WHEN (
                                                                                                                  covtermpatternid =''HODW_SectionI_DedWindAndHailExcl_alfa'') THEN coalesce( polcov.val, value1)
                                                                  END )windstormhailexcl_amt,
                                                                  max(
                                                                  CASE
                                                                                  WHEN covtermpatternid IN (''HODW_WindHail_Ded_HOE'',
                                                                                                            ''HODW_Hurricane_Ded_HOE'',
                                                                                                            ''HODW_SectionI_DedWindstorm_alfa'')
                                                                                  AND             polcov.columnname LIKE ''%direct%'' THEN polcov.val
                                                                                  WHEN covtermpatternid IN (''HODW_WindHail_Ded_HOE'',
                                                                                                            ''HODW_Hurricane_Ded_HOE'',
                                                                                                            ''HODW_SectionI_DedWindstorm_alfa'')
                                                                                  AND             polcov.columnname NOT LIKE ''%direct%'' THEN (
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  cast(value1 AS DECIMAL(18,4))<=1.0000 ) THEN substring(''0000000'',1, (7-length(cast(cast(cast(value1 AS DECIMAL(18,4))*10000 AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                  ||cast(cast(cast(value1 AS DECIMAL(18,4))                             *10000 AS INTEGER) AS VARCHAR(10))
                                                                                                                  WHEN value1 IS NULL
                                                                                                                  OR              value1 =0 THEN 0
                                                                                                                  ELSE substring(''0000000'',1,(7                                             -length(cast(cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                  ||cast(cast(cast(value1 AS DECIMAL(18,4)) *10000 AS INTEGER) AS VARCHAR(10))
                                                                                                  END )
                                                                  END ) AS deductibleamountws,
                                                                  max(
                                                                  CASE
                                                                                  WHEN covtermpatternid IN (''HODW_WindHail_Ded_HOE'',
                                                                                                            ''HODW_Hurricane_Ded_HOE'',
                                                                                                            ''HODW_SectionI_DedWindstorm_alfa'')
                                                                                  AND             polcov.columnname LIKE ''%direct%'' THEN polcov.val
                                                                                  WHEN covtermpatternid IN (''HODW_WindHail_Ded_HOE'',
                                                                                                            ''HODW_Hurricane_Ded_HOE'',
                                                                                                            ''HODW_SectionI_DedWindstorm_alfa'')
                                                                                  AND             polcov.columnname NOT LIKE ''%direct%'' THEN (
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  cast(value1 AS DECIMAL(18,4))<=1.0000) THEN ''F''
                                                                                                                  WHEN value1 IS NULL
                                                                                                                  OR              value1=0 THEN NULL
                                                                                                                  ELSE ''D''
                                                                                                  END )
                                                                  END ) AS deductiblews
                                                  FROM           (
                                                                             SELECT     cast(''DirectTerm1'' AS                       VARCHAR(100)) AS columnname,
                                                                                        cast(directterm1_stg AS                     VARCHAR(255)) AS val,
                                                                                        cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid_stg,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                        pcx_dwellingcov_hoe.          expirationdate_stg,
                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                             join       db_t_prod_stag.pc_policyline
                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                               ''HO3'',
                                                                                                                               ''HO4'',
                                                                                                                               ''HO5'',
                                                                                                                               ''HO6'',
                                                                                                                               ''HO8'')
                                                                             WHERE      directterm1avl_stg = 1
                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                             UNION
                                                                             SELECT     ''Clause''                   AS columnname,
                                                                                        cast(NULL AS VARCHAR(255)) AS val,
                                                                                        pcx_dwellingcov_hoe.patterncode_stg,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                        pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                             join       db_t_prod_stag.pc_policyline
                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                               ''HO3'',
                                                                                                                               ''HO4'',
                                                                                                                               ''HO5'',
                                                                                                                               ''HO6'',
                                                                                                                               ''HO8'')
                                                                             WHERE      pcx_dwellingcov_hoe.patterncode_stg= ''HODW_PersonalPropertyReplacementCost_alfa''
                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                             UNION
                                                                             SELECT     ''DirectTerm2''                         AS columnname,
                                                                                        cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                                                        pcx_dwellingcov_hoe.patterncode_stg,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                        pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                             join       db_t_prod_stag.pc_policyline
                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                               ''HO3'',
                                                                                                                               ''HO4'',
                                                                                                                               ''HO5'',
                                                                                                                               ''HO6'',
                                                                                                                               ''HO8'')
                                                                             WHERE      directterm2avl_stg = 1
                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                             UNION
                                                                             SELECT     ''DirectTerm3''                         AS columnname,
                                                                                        cast(directterm3_stg AS VARCHAR(255)) AS val,
                                                                                        pcx_dwellingcov_hoe.patterncode_stg,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                        pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                             join       db_t_prod_stag.pc_policyline
                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                               ''HO3'',
                                                                                                                               ''HO4'',
                                                                                                                               ''HO5'',
                                                                                                                               ''HO6'',
                                                                                                                               ''HO8'')
                                                                             WHERE      directterm3avl_stg = 1
                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                             UNION
                                                                             SELECT     ''DirectTerm4''                         AS columnname,
                                                                                        cast(directterm4_stg AS VARCHAR(255)) AS val,
                                                                                        pcx_dwellingcov_hoe.patterncode_stg,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                        pcx_dwellingcov_hoe.          expirationdate_stg,
                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                             join       db_t_prod_stag.pc_policyline
                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                               ''HO3'',
                                                                                                                               ''HO4'',
                                                                                                                               ''HO5'',
                                                                                                                               ''HO6'',
                                                                                                                               ''HO8'')
                                                                             WHERE      directterm4avl_stg = 1
                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                             UNION
                                                                             SELECT     ''ChoiceTerm2''                         AS columnname,
                                                                                        cast(choiceterm2_stg AS VARCHAR(255)) AS val,
                                                                                        pcx_dwellingcov_hoe.patterncode_stg,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                        pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                             join       db_t_prod_stag.pc_policyline
                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                               ''HO3'',
                                                                                                                               ''HO4'',
                                                                                                                               ''HO5'',
                                                                                                                               ''HO6'',
                                                                                                                               ''HO8'')
                                                                             WHERE      choiceterm2avl_stg = 1
                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                             UNION
                                                                             SELECT     ''ChoiceTerm3''                         AS columnname,
                                                                                        cast(choiceterm3_stg AS VARCHAR(255)) AS val,
                                                                                        pcx_dwellingcov_hoe.patterncode_stg,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        pcx_dwellingcov_hoe. effectivedate_stg,
                                                                                        pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                             join       db_t_prod_stag.pc_policyline
                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                               ''HO3'',
                                                                                                                               ''HO4'',
                                                                                                                               ''HO5'',
                                                                                                                               ''HO6'',
                                                                                                                               ''HO8'')
                                                                             WHERE      choiceterm3avl_stg = 1
                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                             UNION
                                                                             SELECT     cast(''ChoiceTerm1'' AS                       VARCHAR(250)) AS columnname,
                                                                                        cast(choiceterm1_stg AS                     VARCHAR(255)) AS val,
                                                                                        cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid_stg,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        pcx_dwellingcov_hoe. effectivedate_stg,
                                                                                        pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                             join       db_t_prod_stag.pc_policyline
                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                               ''HO3'',
                                                                                                                               ''HO4'',
                                                                                                                               ''HO5'',
                                                                                                                               ''HO6'',
                                                                                                                               ''HO8'')
                                                                             WHERE      choiceterm1avl_stg = 1
                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                             UNION
                                                                             SELECT     cast(''ChoiceTerm4'' AS                       VARCHAR(250)) AS columnname,
                                                                                        cast(choiceterm4_stg AS                     VARCHAR(255)) AS val,
                                                                                        cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid_stg,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                        pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                             join       db_t_prod_stag.pc_policyline
                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                               ''HO3'',
                                                                                                                               ''HO4'',
                                                                                                                               ''HO5'',
                                                                                                                               ''HO6'',
                                                                                                                               ''HO8'')
                                                                             WHERE      choiceterm4avl_stg = 1
                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                             UNION
                                                                             SELECT     cast(''BooleanTerm1'' AS                      VARCHAR(250)) AS columnname,
                                                                                        cast(booleanterm1_stg AS                    VARCHAR(255)) AS val,
                                                                                        cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid_stg,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                        pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                             join       db_t_prod_stag.pc_policyline
                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                               ''HO3'',
                                                                                                                               ''HO4'',
                                                                                                                               ''HO5'',
                                                                                                                               ''HO6'',
                                                                                                                               ''HO8'')
                                                                             WHERE      pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                             AND        pcx_dwellingcov_hoe.patterncode_stg =''HODW_SectionI_Ded_HOE'' ) polcov
                                                  left join
                                                                  (
                                                                         SELECT pcl.patternid_stg     clausepatternid,
                                                                                pcv.patternid_stg     covtermpatternid,
                                                                                pcv.columnname_stg  AS columnname,
                                                                                pcv.covtermtype_stg AS covtermtype,
                                                                                pcl.name_stg           clausename
                                                                         FROM   db_t_prod_stag.pc_etlclausepattern pcl
                                                                         join   db_t_prod_stag.pc_etlcovtermpattern pcv
                                                                         ON     pcl.id_stg = pcv.clausepatternid_stg ) covterm
                                                  ON              covterm.clausepatternid = polcov.patterncode
                                                  AND             covterm.columnname = polcov.columnname
                                                  left outer join
                                                                  (
                                                                             SELECT     pco.patternid_stg                      optionpatternid,
                                                                                        pco.optioncode_stg                     name1,
                                                                                        cast(pco.value_stg AS VARCHAR(255)) AS value1,
                                                                                        pcp.valuetype_stg                   AS valuetype
                                                                             FROM       db_t_prod_stag.pc_etlcovtermpattern pcp
                                                                             inner join db_t_prod_stag.pc_etlcovtermoption pco
                                                                             ON         pcp.id_stg = pco.coveragetermpatternid_stg ) optn
                                                  ON              optn.optionpatternid = polcov.val
                                                  GROUP BY        branchid_stg 
                                   )  -- end of cov cte
                  SELECT   companynumber,
                           lob,
                           statecode,
                           callyear,
                           accountingyear,
                           expperiodyear,
                           expperiodmonth,
                           expperiodday,
                           coveragecode,
                           ''00'' classificationcode,
                           territorycode,
                           ''0''stateexceptionind,
                           zipcode,
                           policy_eff_yr,
                           newrecordformat,
                           aslob,
                           itemcode,
                           sublinecode,
                           ''0'' policyprogramcode,
                           policy_form_code,
                           no_of_family_codes,
                           construction,
                           lpad(protectionclasscode,2,''0'') protectionclasscode,
                           ''00''                            exceptioncode,
                           typeofdeductiblecode,
                           abs(round(writtenexposure)) policytermcode,
                           losscode,
                           ''00'' stateexceptionb,
                           amountofinsurance,
                           cast(trim(yearofmanufacture) AS VARCHAR(4)) yearofmanufacture,
                           ''0''                                         coveragecodeordlaw,
                           ''0''                                         exposurecodes,
                           ''0''                                         leadpoisoningliability,
                           ded_ind,
                           ded_amount,
                           deductibleindicatorws,
                           deductibleamountws,
                           claimidentifier,
                           claimantidentifier,
                           round(writtenexposure)                                          writtenexposure,
                           cast( SUM(cast(writtenpremium AS DECIMAL(18,7))) AS VARCHAR(40))writtenpremium,
                           paidlosses,
                           paidnumberofclaims,
                           outstandinglosses,
                           outstandingnumberofclaims,
                           policynumber_stg,
                           policyperiodid,
                           policyidentifier
                  FROM     (
                                           SELECT DISTINCT
                                                           CASE
                                                                           WHEN uwc.publicid_stg=''AMI'' THEN ''0005''
                                                                           WHEN uwc.publicid_stg=''AMG'' THEN ''0196''
                                                                           WHEN uwc.publicid_stg=''AIC'' THEN ''0050''
                                                                           ELSE ''0318''
                                                           END  AS companynumber,
                                                           ''18'' AS lob,
                                                           CASE
                                                                           WHEN jd.typecode_stg=''AL'' THEN ''01''
                                                                           WHEN jd.typecode_stg=''GA'' THEN ''10''
                                                                           WHEN jd.typecode_stg=''MS'' THEN ''23''
                                                           END                                                statecode,
                                                           extract(year FROM cast(:pc_eoy AS timestamp )) + 1 callyear,
                                                           extract(year FROM cast (:pc_eoy AS timestamp ))    accountingyear,
                                                           ''0000''                                             expperiodyear,
                                                           ''00''                                               expperiodmonth,
                                                           ''00''                                               expperiodday,
                                                           CASE
                                                                           WHEN pcdh.name_stg= ''Secondary'' THEN ''07''
                                                                           ELSE ''01''
                                                           END                                                       coveragecode,
                                                           cast(terr.code AS VARCHAR(2))                             AS territorycode,
                                                           coalesce(substring( terr.postalcodeinternal,1,5) ,''00000'')AS zipcode,
                                                           CASE
                                                                           WHEN pcj.typecode_stg=''Cancellation'' THEN year(pp.cancellationdate_stg)
                                                                           ELSE year(pp.periodstart_stg)
                                                           END   AS policy_eff_yr,
                                                           ''D''   AS newrecordformat,
                                                           ''040'' AS aslob,
                                                           CASE
                                                                           WHEN earthquake =''HODW_Earthquake_HOE'' THEN ''01''
                                                                           WHEN ph.typecode_stg =''HO4'' THEN ''02''
                                                                           ELSE ''03''
                                                           END AS itemcode,
                                                           CASE
                                                                           WHEN earthquake =''HODW_Earthquake_HOE'' THEN ''60''
                                                                           WHEN replacement =''HODW_PersonalPropertyReplacementCost_alfa''
                                                                           AND             ph.typecode_stg<>''HO8'' THEN ''03''
                                                                           WHEN ph.typecode_stg=''HO8'' THEN ''02''
                                                                           ELSE ''02''
                                                           END AS sublinecode,
                                                           CASE
                                                                           WHEN ph.typecode_stg=''HO2'' THEN ''02''
                                                                           WHEN ph.typecode_stg=''HO4'' THEN ''04''
                                                                           WHEN ph.typecode_stg=''HO5'' THEN ''05''
                                                                           WHEN ph.typecode_stg=''HO6'' THEN ''06''
                                                                           WHEN ph.typecode_stg=''HO8'' THEN ''08''
                                                                           ELSE ''03''
                                                           END policy_form_code,
                                                           CASE
                                                                           WHEN ph.typecode_stg=''HO4'' THEN ''2''
                                                                           WHEN pr.typecode_stg IN (''Apt'' ,
                                                                                                    ''Condo'',
                                                                                                    ''Coop'',
                                                                                                    ''Duplex'',
                                                                                                    ''Mobile'',
                                                                                                    ''Modular_alfa'',
                                                                                                    ''TownRow'',
                                                                                                    ''Fam1'',
                                                                                                    ''Fam2'',
                                                                                                    ''Fam3'',
                                                                                                    ''Fam3To4_alfa'',
                                                                                                    ''Fam4'') THEN ''1''
                                                                           ELSE ''2''
                                                           END no_of_family_codes,
                                                           CASE
                                                                           WHEN pct.typecode_stg IN (''ADB'',
                                                                                                     ''AOD'',
                                                                                                     ''CLP'',
                                                                                                     ''COM'',
                                                                                                     ''CUS'',
                                                                                                     ''DOM'',
                                                                                                     ''F'',
                                                                                                     ''FRM'',
                                                                                                     ''FST'',
                                                                                                     ''GLA'',
                                                                                                     ''HEA'',
                                                                                                     ''L'',
                                                                                                     ''LOG'',
                                                                                                     ''LIG'',
                                                                                                     ''NON'',
                                                                                                     ''OTH'',
                                                                                                     ''OTHER'',
                                                                                                     ''PFR'',
                                                                                                     ''STU'',
                                                                                                     ''STW'',
                                                                                                     ''TLU'',
                                                                                                     ''UNK'',
                                                                                                     ''WOO'',
                                                                                                     ''WSC'') THEN ''1''
                                                                           WHEN pct.typecode_stg IN (''BCB'',
                                                                                                     ''BLB'',
                                                                                                     ''BRC'',
                                                                                                     ''BRF'',
                                                                                                     ''BRS'',
                                                                                                     ''BST'',
                                                                                                     ''FRY'',
                                                                                                     ''SRO'',
                                                                                                     ''STV'',
                                                                                                     ''TBM'',
                                                                                                     ''WBR'',
                                                                                                     ''WCB'',
                                                                                                     ''WSN'') THEN ''2''
                                                                           WHEN pct.typecode_stg IN (''BRK'',
                                                                                                     ''CCM'',
                                                                                                     ''CNB'',
                                                                                                     ''CND'',
                                                                                                     ''CRE'',
                                                                                                     ''FLX'',
                                                                                                     ''M'',
                                                                                                     ''MAS'',
                                                                                                     ''MTU'',
                                                                                                     ''TUC'',
                                                                                                     ''SFM'') THEN ''3''
                                                                           WHEN pct.typecode_stg IN (''BLM'',
                                                                                                     ''BLS'',
                                                                                                     ''BRL'',
                                                                                                     ''BRM'',
                                                                                                     ''CCS'',
                                                                                                     ''MET'',
                                                                                                     ''PRM'',
                                                                                                     ''S'',
                                                                                                     ''STE'',
                                                                                                     ''STS'') THEN ''4''
                                                                           WHEN pct.typecode_stg IN (''ALF'',
                                                                                                     ''ALS'',
                                                                                                     ''ALV'',
                                                                                                     ''FRS'',
                                                                                                     ''WMT'',
                                                                                                     ''WSL'') THEN ''5''
                                                                           WHEN pct.typecode_stg IN (''MAN'') THEN ''6''
                                                           END construction,
                                                           CASE
                                                                           WHEN phh.dwellingprotectionclasscode_stg IS NULL
                                                                           AND             jd.typecode_stg =''AL'' THEN ''05''
                                                                           WHEN phh.dwellingprotectionclasscode_stg IS NULL
                                                                           AND             jd.typecode_stg =''GA'' THEN ''03''
                                                                           WHEN phh.dwellingprotectionclasscode_stg IS NULL
                                                                           AND             jd.typecode_stg =''MS'' THEN ''10''
                                                                           ELSE coalesce( phh.dwellingprotectionclasscode_stg, ''00'')
                                                           END AS protectionclasscode,
                                                           CASE
                                                                           WHEN jd.typecode_stg=''AL'' THEN
                                                                                           CASE
                                                                                                           WHEN (
                                                                                                                                           windhail =''HODW_WindHail_Ded_HOE'')
                                                                                                           AND             deductibleamountws >0 THEN ''35''
                                                                                                           WHEN (
                                                                                                                                           hurricane =''HODW_Hurricane_Ded_HOE''
                                                                                                                           AND             terr.code IN (''05'',
                                                                                                                                                         ''06'',
                                                                                                                                                         ''30'',
                                                                                                                                                         ''31''))
                                                                                                           AND             deductibleamountws >0 THEN ''55''
                                                                                                           WHEN (
                                                                                                                                           hurricane =''HODW_Hurricane_Ded_HOE''
                                                                                                                           AND             terr.code NOT IN (''05'',
                                                                                                                                                             ''06'',
                                                                                                                                                             ''30'',
                                                                                                                                                             ''31''))
                                                                                                           AND             deductibleamountws >0 THEN ''35''
                                                                                                           WHEN (
                                                                                                                                           windstormhailexcl =''HODW_SectionI_DedWindAndHailExcl_alfa'')
                                                                                                           AND             (
                                                                                                                                           windstormhailexcl_amt <> 0
                                                                                                                           AND             windstormhailexcl_amt IS NOT NULL) THEN ''07''
                                                                                                           ELSE ''05''
                                                                                           END
                                                                           WHEN jd.typecode_stg IN (''GA'',
                                                                                                    ''MS'') THEN
                                                                                           CASE
                                                                                                           WHEN (
                                                                                                                                           (
                                                                                                                                                           windhail =''HODW_WindHail_Ded_HOE'')
                                                                                                                           OR              (
                                                                                                                                                           hurricane =''HODW_Hurricane_Ded_HOE'' ) )
                                                                                                           AND             deductibleamountws >0 THEN ''05''
                                                                                                           WHEN (
                                                                                                                                           windstormhailexcl =''HODW_SectionI_DedWindAndHailExcl_alfa'')
                                                                                                           AND             (
                                                                                                                                           windstormhailexcl_amt <> 0
                                                                                                                           AND             windstormhailexcl_amt IS NOT NULL) THEN ''07''
                                                                                                           ELSE ''05''
                                                                                           END
                                                           END AS typeofdeductiblecode,
                                                           CASE
                                                                           WHEN pp.cancellationdate_stg IS NOT NULL THEN months_between (cast( pp.periodstart_stg AS DATE),cast( pp.cancellationdate_stg AS DATE) )
                                                                           WHEN pp.cancellationdate_stg IS NULL THEN months_between (cast( pp.periodstart_stg AS     DATE),cast( pp.cancellationdate_stg AS DATE) )
                                                           END                          AS policytermcode,
                                                           ''00''                         AS losscode,
                                                           pdh.manhomeparkcode_alfa_stg AS locationcode,
                                                           coalesce((
                                                           CASE
                                                                           WHEN ph.typecode_stg IN (''HO4'' ,
                                                                                                    ''HO6'') THEN pp_limit
                                                                           ELSE dw_limit
                                                           END),''00000'') AS amountofinsurance,
                                                           coalesce(
                                                           CASE
                                                                           WHEN (
                                                                                                           ph.typecode_stg IN (''HO2'',
                                                                                                                               ''HO3'' ,
                                                                                                                               ''HO5'',
                                                                                                                               ''HO8''))
                                                                           AND             (
                                                                                                           pdh.yearbuilt_stg<=1959) THEN ''1959''
                                                                           WHEN (
                                                                                                           ph.typecode_stg IN (''HO2'',
                                                                                                                               ''HO3'' ,
                                                                                                                               ''HO5'',
                                                                                                                               ''HO8''))
                                                                           AND             (
                                                                                                           pdh.yearbuilt_stg>1959) THEN pdh.yearbuilt_stg
                                                                           WHEN (
                                                                                                           ph.typecode_stg IN (''HO4'')) THEN ''0000''
                                                                           WHEN ph.typecode_stg IN (''HO6'')
                                                                           AND             lia_limit BETWEEN 1001.00 AND             9999.00 THEN ''0002''
                                                                           WHEN ph.typecode_stg IN (''HO6'')
                                                                           AND             lia_limit BETWEEN 10000.00 AND             19999.00 THEN ''0003''
                                                                           WHEN ph.typecode_stg IN (''HO6'')
                                                                           AND             lia_limit BETWEEN 20000.00 AND             29999.00 THEN ''0004''
                                                                           WHEN ph.typecode_stg IN (''HO6'')
                                                                           AND             lia_limit BETWEEN 30000.00 AND             39999.00 THEN ''0005''
                                                                           WHEN ph.typecode_stg IN (''HO6'')
                                                                           AND             lia_limit BETWEEN 40000.00 AND             49999.00 THEN ''0006''
                                                                           WHEN ph.typecode_stg IN (''HO6'')
                                                                           AND             lia_limit BETWEEN 50000.00 AND             59999.00 THEN ''0007''
                                                                           WHEN ph.typecode_stg IN (''HO6'')
                                                                           AND             lia_limit BETWEEN 60000.00 AND             69999.00 THEN ''0008''
                                                                           WHEN ph.typecode_stg IN (''HO6'')
                                                                           AND             lia_limit >70000.00 THEN ''0009''
                                                                           ELSE ''0001''
                                                           END ,''0000'')                     AS yearofmanufacture,
                                                           coalesce(perils_limit_ind , ''0'')    ded_ind,
                                                           coalesce(perils_limit,''0000000'')    ded_amount,
                                                           ''3''                              AS tiedowncode,
                                                           coalesce(
                                                           CASE
                                                                           WHEN jd.typecode_stg=''AL'' THEN deductiblews
                                                                           ELSE ''0''
                                                           END,''0'') AS deductibleindicatorws,
                                                           coalesce(
                                                           CASE
                                                                           WHEN jd.typecode_stg=''AL'' THEN cast(cast(deductibleamountws AS INTEGER) AS VARCHAR(7) )
                                                                           ELSE ''0''
                                                           END ,''0000000'')AS deductibleamountws,
                                                           ''0''            AS claimidentifier,
                                                           ''0''            AS claimantidentifier,
                                                           /*cast(
                                                           CASE
                                                                           WHEN phth.amount_stg <=0 THEN
                                                                                           CASE
                                                                                                           WHEN (
                                                                                                                                           month(editeffectivedate_stg) IN (1,2,3)
                                                                                                                           AND             month(periodend_stg)         IN (1,2,3)
                                                                                                                           AND             abs(cast( pp.periodend_stg AS DATE)-cast(pp.editeffectivedate_stg AS DATE)) BETWEEN 0 AND             29) THEN -1
                                                                                                           WHEN (
                                                                                                                                           month(editeffectivedate_stg) NOT IN (1,2,3)
                                                                                                                           AND             month(periodend_stg) NOT         IN (1,2,3)
                                                                                                                           AND             abs(cast( pp.periodend_stg AS DATE)-cast(pp.editeffectivedate_stg AS DATE)) BETWEEN 0 AND             30) THEN -1
                                                                                                           ELSE cast(( (pp.periodend_stg- pp.editeffectivedate_stg )month )*-1 AS INTEGER)
                                                                                           END
                                                                           WHEN phth.amount_stg > 0 THEN
                                                                                           CASE
                                                                                                           WHEN (
                                                                                                                                           month(editeffectivedate_stg) IN (1,2,3)
                                                                                                                           AND             month(periodend_stg)         IN (1,2,3)
                                                                                                                           AND             abs(cast( pp.periodend_stg AS DATE)-cast(pp.editeffectivedate_stg AS DATE)) BETWEEN 0 AND             29) THEN 1
                                                                                                           WHEN (
                                                                                                                                           month(editeffectivedate_stg) NOT IN (1,2,3)
                                                                                                                           AND             month(periodend_stg)NOT          IN (1,2,3)
                                                                                                                           AND             abs(cast( pp.periodend_stg AS DATE)-cast(pp.editeffectivedate_stg AS DATE)) BETWEEN 0 AND             30) THEN 1
                                                                                                           ELSE cast(((pp.periodend_stg- pp.editeffectivedate_stg )month) AS INTEGER )
                                                                                           END
                                                                           ELSE 0
                                                           END AS INTEGER ) AS writtenexposure, */

															CAST(
															CASE
																WHEN phth.amount_stg <= 0 THEN
																CASE
																	WHEN MONTH(pp.editeffectivedate_stg) IN (1, 2, 3)
																	AND MONTH(pp.periodend_stg) IN (1, 2, 3)
																	AND ABS(DATEDIFF(day, pp.editeffectivedate_stg, pp.periodend_stg)) BETWEEN 0 AND 29
																	THEN -1

																	WHEN MONTH(pp.editeffectivedate_stg) NOT IN (1, 2, 3)
																	AND MONTH(pp.periodend_stg) NOT IN (1, 2, 3)
																	AND ABS(DATEDIFF(day, pp.editeffectivedate_stg, pp.periodend_stg)) BETWEEN 0 AND 30
																	THEN -1

																	ELSE -1 * DATEDIFF(month, pp.editeffectivedate_stg, pp.periodend_stg)
																END

																WHEN phth.amount_stg > 0 THEN
																CASE
																	WHEN MONTH(pp.editeffectivedate_stg) IN (1, 2, 3)
																	AND MONTH(pp.periodend_stg) IN (1, 2, 3)
																	AND ABS(DATEDIFF(day, pp.editeffectivedate_stg, pp.periodend_stg)) BETWEEN 0 AND 29
																	THEN 1

																	WHEN MONTH(pp.editeffectivedate_stg) NOT IN (1, 2, 3)
																	AND MONTH(pp.periodend_stg) NOT IN (1, 2, 3)
																	AND ABS(DATEDIFF(day, pp.editeffectivedate_stg, pp.periodend_stg)) BETWEEN 0 AND 30
																	THEN 1

																	ELSE DATEDIFF(month, pp.editeffectivedate_stg, pp.periodend_stg)
																END

																ELSE 0
															END AS INTEGER
															) AS writtenexposure,


								/*/						   (
                                                           CASE
                                                                           WHEN 1 =
                                                                                           (
                                                                                                  SELECT 1
                                                                                                  WHERE  EXISTS
                                                                                                         (
                                                                                                                SELECT pc_policyperiod2.policynumber_stg
                                                                                                                FROM   db_t_prod_stag.pc_policyperiod pc_policyperiod2
                                                                                                                join   db_t_prod_stag.pc_policyterm pt2
                                                                                                                ON     pt2.id_stg = pc_policyperiod2.policytermid_stg
                                                                                                                join   db_t_prod_stag.pc_policyline
                                                                                                                ON     pc_policyperiod2.id_stg = pc_policyline.branchid_stg
                                                                                                                AND    pc_policyline.expirationdate_stg IS NULL
                                                                                                                join   db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                                ON     pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                                AND    pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                              ''HO3'',
                                                                                                                                                              ''HO4'',
                                                                                                                                                              ''HO5'',
                                                                                                                                                              ''HO6'',
                                                                                                                                                              ''HO8'')
                                                                                                                join   db_t_prod_stag.pc_job job2
                                                                                                                ON     job2.id_stg = pc_policyperiod2.jobid_stg
                                                                                                                join   db_t_prod_stag.pctl_job pctl_job2
                                                                                                                ON     pctl_job2.id_stg = job2.subtype_stg
                                                                                                                WHERE  pctl_job2.name_stg = ''Renewal''
                                                                                                                AND    (
                                                                                                                              pt.confirmationdate_alfa_stg > :pc_eoy
                                                                                                                       OR     pt.confirmationdate_alfa_stg IS NULL)
                                                                                                                AND    pc_policyperiod2.policynumber_stg = pp.policynumber_stg
                                                                                                                AND    pc_policyperiod2.termnumber_stg = pp.termnumber_stg )) THEN 0
                                                                           ELSE phth.amount_stg
                                                           END) AS writtenpremium, */
                                                           CASE
                                                               WHEN EXISTS (
                                                               SELECT 1
                                                               FROM db_t_prod_stag.pc_policyperiod pc_policyperiod2
                                                               JOIN db_t_prod_stag.pc_policyterm pt2
                                                               ON pt2.id_stg = pc_policyperiod2.policytermid_stg
                                                               JOIN db_t_prod_stag.pc_policyline
                                                               ON pc_policyperiod2.id_stg = pc_policyline.branchid_stg
                                                               AND pc_policyline.expirationdate_stg IS NULL
                                                               JOIN db_t_prod_stag.pctl_hopolicytype_hoe
                                                               ON pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                               AND pctl_hopolicytype_hoe.typecode_stg IN (''HO2'', ''HO3'', ''HO4'', ''HO5'', ''HO6'', ''HO8'')
                                                               JOIN db_t_prod_stag.pc_job job2
                                                               ON job2.id_stg = pc_policyperiod2.jobid_stg
                                                               JOIN db_t_prod_stag.pctl_job pctl_job2
                                                               ON pctl_job2.id_stg = job2.subtype_stg
                                                               WHERE pctl_job2.name_stg = ''Renewal''
                                                               AND (
                                                                      pt2.confirmationdate_alfa_stg > :pc_eoy OR pt2.confirmationdate_alfa_stg IS NULL
                                                               )
                                                               AND pc_policyperiod2.policynumber_stg = pp.policynumber_stg
                                                               AND pc_policyperiod2.termnumber_stg = pp.termnumber_stg
                                                               )
                                                               THEN 0
                                                               ELSE phth.amount_stg
                                                         END AS writtenpremium, 

                                                           ''0''  AS paidlosses,
                                                           ''0''  AS paidnumberofclaims,
                                                           ''0''  AS outstandinglosses,
                                                           ''0''  AS outstandingnumberofclaims,
                                                           pp.policynumber_stg,
                                                           cast(pp.publicid_stg AS VARCHAR(64))AS policyperiodid,
                                                           pj.jobnumber_stg                       policyidentifier ,
                                                           phth.id_stg
                                           FROM            db_t_prod_stag.pcx_hotransaction_hoe phth
                                           join            db_t_prod_stag.pcx_homeownerscost_hoe phch
                                           ON              phth.homeownerscost_stg = phch.id_stg
                                           join            db_t_prod_stag.pc_policyperiod pp
                                           ON              phth.branchid_stg =pp.id_stg
                                           join            db_t_prod_stag.pc_uwcompany uwc
                                           ON              pp.uwcompany_stg=uwc.id_stg
                                           join            db_t_prod_stag.pctl_jurisdiction jd
                                           ON              pp.basestate_stg=jd.id_stg
                                           join            db_t_prod_stag.pcx_dwelling_hoe pdh
                                           ON              pdh.branchid_stg=pp.id_stg
                                           AND             pdh.expirationdate_stg IS NULL
                                           left join       db_t_prod_stag.pctl_dwellingusage_hoe pcdh
                                           ON              pcdh.id_stg =pdh.dwellingusage_stg
                                           left join       terr  -- using cte
                                           ON              terr.id=pp.id_stg
                                           join            db_t_prod_stag.pc_job pj
                                           ON              pp.jobid_stg = pj.id_stg
                                           join            db_t_prod_stag.pctl_job pcj
                                           ON              pj.subtype_stg = pcj.id_stg
                                           join            db_t_prod_stag.pcx_holocation_hoe phh
                                           ON              pdh.holocation_stg= phh.id_stg
                                           join            db_t_prod_stag.pctl_hopolicytype_hoe ph
                                           ON              ph.id_stg=pdh.hopolicytype_stg
                                           AND             ph.typecode_stg LIKE ''HO%''
                                           join            cov  -- using cte
                                           ON              to_number(cov.branchid_stg) =pp.id_stg
                                           left join       db_t_prod_stag.pctl_residencetype_hoe pr
                                           ON              residencetype_stg= pr.id_stg
                                           left join       db_t_prod_stag.pctl_constructiontype_hoe pct
                                           ON              pct.id_stg=constructiontype_stg
                                           join            db_t_prod_stag.pctl_policyperiodstatus
                                           ON              pp.status_stg=pctl_policyperiodstatus.id_stg
                                           AND             pctl_policyperiodstatus.typecode_stg=''Bound''
                                           join            db_t_prod_stag.pc_policyterm pt
                                           ON              pt.id_stg = pp.policytermid_stg
                                           AND
                                                           CASE
                                                                           WHEN pp.editeffectivedate_stg >= pp.modeldate_stg
                                                                           AND             pp.editeffectivedate_stg>= coalesce(cast(pt.confirmationdate_alfa_stg AS timestamp), cast(''1900-01-01 00:00:00.000000'' AS timestamp)) THEN pp.editeffectivedate_stg
                                                                           WHEN coalesce(cast(pt.confirmationdate_alfa_stg AS timestamp), cast(''1900-01-01 00:00:00.000000''AS timestamp)) >= pp.modeldate_stg THEN coalesce(cast(pt.confirmationdate_alfa_stg AS timestamp), cast(''1900-01-01 00:00:00.000000''AS timestamp))
                                                                           ELSE pp.modeldate_stg
                                                           END BETWEEN :pc_boy AND             :pc_eoy)a
                  GROUP BY companynumber,
                           lob,
                           statecode,
                           callyear,
                           accountingyear,
                           expperiodyear,
                           expperiodmonth,
                           expperiodday,
                           coveragecode,
                           territorycode,
                           zipcode,
                           policy_eff_yr,
                           newrecordformat,
                           aslob,
                           itemcode,
                           sublinecode,
                           policy_form_code,
                           no_of_family_codes,
                           construction,
                           protectionclasscode,
                           typeofdeductiblecode,
                           policytermcode,
                           losscode,
                           locationcode,
                           amountofinsurance,
                           yearofmanufacture,
                           ded_ind,
                           ded_amount,
                           tiedowncode,
                           deductibleindicatorws,
                           deductibleamountws,
                           claimidentifier,
                           claimantidentifier,
                           writtenexposure,
                           paidlosses,
                           paidnumberofclaims,
                           outstandinglosses,
                           outstandingnumberofclaims,
                           policynumber_stg,
                           policyperiodid,
                           policyidentifier
                  HAVING   SUM(cast(writtenpremium AS DECIMAL(18,4)))<>0.00 ) src ) );
  -- Component exp_policy_pass_through, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_policy_pass_through AS
  (
         SELECT sq_pc_policyperiod.companynumber           AS companynumber,
                sq_pc_policyperiod.lineofbusinesscode      AS lineofbusinesscode,
                sq_pc_policyperiod.statecode               AS statecode,
                sq_pc_policyperiod.callyear                AS callyear,
                sq_pc_policyperiod.accountingyear          AS accountingyear,
                sq_pc_policyperiod.expperiodyear           AS expperiodyear,
                sq_pc_policyperiod.expperiodmonth          AS expperiodmonth,
                sq_pc_policyperiod.expeperiodday           AS expeperiodday,
                sq_pc_policyperiod.coveragecode            AS coveragecode,
                sq_pc_policyperiod.classificationcode      AS classificationcode,
                sq_pc_policyperiod.territorycode           AS territorycode,
                sq_pc_policyperiod.stateexceptionind       AS stateexceptionind,
                sq_pc_policyperiod.zipcode                 AS zipcode,
                sq_pc_policyperiod.policyeffectiveyear     AS policyeffectiveyear,
                sq_pc_policyperiod.newrecordformat         AS newrecordformat,
                sq_pc_policyperiod.aslob                   AS aslob,
                sq_pc_policyperiod.itemcode                AS itemcode,
                sq_pc_policyperiod.sublinecode             AS sublinecode,
                sq_pc_policyperiod.policyprogramcode       AS policyprogramcode,
                sq_pc_policyperiod.policyformcode          AS policyformcode,
                sq_pc_policyperiod.numberoffamilycodes     AS numberoffamilycodes,
                sq_pc_policyperiod.constructioncode        AS constructioncode,
                sq_pc_policyperiod.protectioncasscode      AS protectioncasscode,
                sq_pc_policyperiod.exceptioncode           AS exceptioncode,
                sq_pc_policyperiod.typeofdeductiblecode    AS typeofdeductiblecode,
                sq_pc_policyperiod.policytermcode          AS policytermcode,
                sq_pc_policyperiod.typeoflosscode          AS typeoflosscode,
                sq_pc_policyperiod.stateofexceptionb       AS stateofexceptionb,
                sq_pc_policyperiod.amountofinsurance       AS amountofinsurance,
                sq_pc_policyperiod.yeaofconstructionliablt AS yeaofconstructionliablt,
                sq_pc_policyperiod.coveragecodeordlaw      AS coveragecodeordlaw,
                sq_pc_policyperiod.exposurecode            AS exposurecode,
                sq_pc_policyperiod.leadpoisoningliability  AS leadpoisoningliability,
                sq_pc_policyperiod.deductibleindicator     AS deductibleindicator,
                sq_pc_policyperiod.deductibleamount        AS deductibleamount,
                sq_pc_policyperiod.deductibleindicatorws   AS deductibleindicatorws,
                sq_pc_policyperiod.deductibleamountws      AS deductibleamountws,
                sq_pc_policyperiod.writtenexposure         AS writtenexposure,
                sq_pc_policyperiod.writtenpremium          AS writtenpremium,
                sq_pc_policyperiod.paidlosses              AS paidlosses,
                sq_pc_policyperiod.paidnumberofclaims      AS paidnumberofclaims,
                sq_pc_policyperiod.outstandinglosses       AS outstandinglosses,
                sq_pc_policyperiod.outstandingnoofclaims   AS outstandingnoofclaims,
                sq_pc_policyperiod.policynumber            AS policynumber,
                sq_pc_policyperiod.policyperiodid          AS policyperiodid,
                sq_pc_policyperiod.policyidentifier        AS policyidentifier,
                sq_pc_policyperiod.source_record_id
         FROM   sq_pc_policyperiod );
  -- Component exp_default, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_default AS
  (
         SELECT exp_policy_pass_through.companynumber      AS companynumber,
                exp_policy_pass_through.lineofbusinesscode AS lineofbusinesscode,
                exp_policy_pass_through.statecode          AS statecode,
                exp_policy_pass_through.callyear           AS callyear,
                exp_policy_pass_through.accountingyear     AS accountingyear,
                exp_policy_pass_through.expperiodyear      AS expperiodyear,
                exp_policy_pass_through.expperiodmonth     AS expperiodmonth,
                exp_policy_pass_through.expeperiodday      AS expeperiodday,
                exp_policy_pass_through.coveragecode       AS coveragecode,
                exp_policy_pass_through.classificationcode AS classificationcode,
                CASE
                       WHEN exp_policy_pass_through.territorycode IS NULL THEN ''00''
                       ELSE substr ( ''00'' , 1 , 2 - length ( exp_policy_pass_through.territorycode ) )
                                     || exp_policy_pass_through.territorycode
                END                                          AS territorycode1,
                exp_policy_pass_through.stateexceptionind    AS stateexceptioncodeb,
                exp_policy_pass_through.zipcode              AS zipcode,
                exp_policy_pass_through.policyeffectiveyear  AS policyeffectiveyear,
                exp_policy_pass_through.newrecordformat      AS newrecordformat,
                exp_policy_pass_through.aslob                AS aslob,
                exp_policy_pass_through.itemcode             AS itemcode,
                exp_policy_pass_through.sublinecode          AS sublinecode,
                exp_policy_pass_through.policyprogramcode    AS policyprogramcode,
                exp_policy_pass_through.policyformcode       AS policyformcode,
                exp_policy_pass_through.numberoffamilycodes  AS numberoffamilies,
                exp_policy_pass_through.constructioncode     AS constructioncode,
                exp_policy_pass_through.protectioncasscode   AS protectionclasscosse,
                exp_policy_pass_through.exceptioncode        AS exceptioncode,
                exp_policy_pass_through.typeofdeductiblecode AS typeofdeductible,
                CASE
                       WHEN exp_policy_pass_through.policytermcode IS NULL THEN ''00''
                       ELSE substr ( ''00'' , 1 , 2 - length ( exp_policy_pass_through.policytermcode ) )
                                     || exp_policy_pass_through.policytermcode
                END                                       AS policytermcode1,
                exp_policy_pass_through.typeoflosscode    AS typeoflosscode,
                exp_policy_pass_through.stateofexceptionb AS stateofexceptioncode,
                CASE
                       WHEN exp_policy_pass_through.amountofinsurance IS NULL THEN ''00000''
                       ELSE substr ( ''00000'' , 1 , 5 - length ( exp_policy_pass_through.amountofinsurance ) )
                                     || exp_policy_pass_through.amountofinsurance
                END AS amountofinsurance1,
                CASE
                       WHEN exp_policy_pass_through.yeaofconstructionliablt IS NULL THEN ''0000''
                       ELSE substr ( ''0000'' , 1 , 4 - length ( exp_policy_pass_through.yeaofconstructionliablt ) )
                                     || exp_policy_pass_through.yeaofconstructionliablt
                END                                            AS yearofconstru,
                exp_policy_pass_through.coveragecodeordlaw     AS coveragecodeb,
                exp_policy_pass_through.exposurecode           AS exposurecode,
                exp_policy_pass_through.leadpoisoningliability AS leadpoisoningindicator,
                exp_policy_pass_through.deductibleindicator    AS deductibleindicator,
                exp_policy_pass_through.deductibleamount       AS deductibleamount,
                exp_policy_pass_through.deductibleindicatorws  AS deductiblewindindicator,
                CASE
                       WHEN exp_policy_pass_through.deductibleamountws IS NULL THEN ''0000000''
                       ELSE substr ( ''0000000'' , 1 , 7 - length ( exp_policy_pass_through.deductibleamountws ) )
                                     || exp_policy_pass_through.deductibleamountws
                END                                           AS deductiblewindamount1,
                ''000000000000000''                             AS claimidentifer,
                ''000''                                         AS claimantidentifier,
                exp_policy_pass_through.writtenexposure       AS writtenexposure,
                exp_policy_pass_through.writtenpremium        AS writtenpremium,
                exp_policy_pass_through.paidlosses            AS paidlosses,
                exp_policy_pass_through.paidnumberofclaims    AS paidnumberofclaims,
                exp_policy_pass_through.outstandinglosses     AS outstandingloesses,
                exp_policy_pass_through.outstandingnoofclaims AS outstandingnoofclaims,
                exp_policy_pass_through.policynumber          AS policynumber,
                exp_policy_pass_through.policyperiodid        AS policyperiodid,
                exp_policy_pass_through.policyidentifier      AS policyidentifier1,
                current_timestamp                             AS creationts,
                ''0''                                           AS creationid,
                current_timestamp                             AS updatets,
                ''0''                                           AS updateid,
                exp_policy_pass_through.source_record_id
         FROM   exp_policy_pass_through );
  -- Component OUT_NAIIPCI_HO_policy, Type TARGET
  INSERT INTO db_t_prod_comn.out_naiipci_ho
              (
                          companynumber,
                          lob,
                          stateofprincipalgarage,
                          callyear,
                          accountingyear,
                          expperiodyear,
                          expperiodmonth,
                          expperiodday,
                          coveragecode,
                          classificationcode,
                          territorycode,
                          stateexceptionind,
                          zipcode,
                          policyeffectiveyear,
                          newrecordformat,
                          aslob,
                          itemcode,
                          sublinecode,
                          policyprogramcode,
                          policyformcode,
                          numberoffamilycodes,
                          constructioncode,
                          protectionclasscode,
                          exceptioncode,
                          typeofdeductiblecode,
                          policytermcode,
                          typelosscode,
                          stateofexceptionb,
                          amountofinsurance,
                          yearofconstnliablt,
                          coveragecodeordlaw,
                          exposurecodes,
                          leadpoisoningliability,
                          deductibleindicator,
                          deductibleamount,
                          deductindwindstorm,
                          deductamountwindstorm,
                          claimidentifier,
                          claimantidentifier,
                          writtenexposure,
                          writtenpremium,
                          paidlosses,
                          paidnumberofclaims,
                          outstandinglosses,
                          outstandingnoofclaims,
                          policynumber,
                          policyperiodid,
                          creationts,
                          creationuid,
                          updatets,
                          updateuid,
                          policyidentifier
              )
  SELECT exp_default.companynumber           AS companynumber,
         exp_default.lineofbusinesscode      AS lob,
         exp_default.statecode               AS stateofprincipalgarage,
         exp_default.callyear                AS callyear,
         exp_default.accountingyear          AS accountingyear,
         exp_default.expperiodyear           AS expperiodyear,
         exp_default.expperiodmonth          AS expperiodmonth,
         exp_default.expeperiodday           AS expperiodday,
         exp_default.coveragecode            AS coveragecode,
         exp_default.classificationcode      AS classificationcode,
         exp_default.territorycode1          AS territorycode,
         exp_default.stateexceptioncodeb     AS stateexceptionind,
         exp_default.zipcode                 AS zipcode,
         exp_default.policyeffectiveyear     AS policyeffectiveyear,
         exp_default.newrecordformat         AS newrecordformat,
         exp_default.aslob                   AS aslob,
         exp_default.itemcode                AS itemcode,
         exp_default.sublinecode             AS sublinecode,
         exp_default.policyprogramcode       AS policyprogramcode,
         exp_default.policyformcode          AS policyformcode,
         exp_default.numberoffamilies        AS numberoffamilycodes,
         exp_default.constructioncode        AS constructioncode,
         exp_default.protectionclasscosse    AS protectionclasscode,
         exp_default.exceptioncode           AS exceptioncode,
         exp_default.typeofdeductible        AS typeofdeductiblecode,
         exp_default.policytermcode1         AS policytermcode,
         exp_default.typeoflosscode          AS typelosscode,
         exp_default.stateofexceptioncode    AS stateofexceptionb,
         exp_default.amountofinsurance1      AS amountofinsurance,
         exp_default.yearofconstru           AS yearofconstnliablt,
         exp_default.coveragecodeb           AS coveragecodeordlaw,
         exp_default.exposurecode            AS exposurecodes,
         exp_default.leadpoisoningindicator  AS leadpoisoningliability,
         exp_default.deductibleindicator     AS deductibleindicator,
         exp_default.deductibleamount        AS deductibleamount,
         exp_default.deductiblewindindicator AS deductindwindstorm,
         exp_default.deductiblewindamount1   AS deductamountwindstorm,
         exp_default.claimidentifer          AS claimidentifier,
         exp_default.claimantidentifier      AS claimantidentifier,
         exp_default.writtenexposure         AS writtenexposure,
         exp_default.writtenpremium          AS writtenpremium,
         exp_default.paidlosses              AS paidlosses,
         exp_default.paidnumberofclaims      AS paidnumberofclaims,
         exp_default.outstandingloesses      AS outstandinglosses,
         exp_default.outstandingnoofclaims   AS outstandingnoofclaims,
         exp_default.policynumber            AS policynumber,
         exp_default.policyperiodid          AS policyperiodid,
         exp_default.creationts              AS creationts,
         exp_default.creationid              AS creationuid,
         exp_default.updatets                AS updatets,
         exp_default.updateid                AS updateuid,
         exp_default.policyidentifier1       AS policyidentifier
  FROM   exp_default;
  
  -- PIPELINE END FOR 1
  -- PIPELINE START FOR 2
  -- Component SQ_cc_claim, Type SOURCE
  CREATE
  OR
  replace TEMPORARY TABLE sq_cc_claim AS
  (
         SELECT
                /* adding column aliases to ensure proper downstream column references */
                $1  AS companynumber,
                $2  AS lineofbusinesscode,
                $3  AS statecode,
                $4  AS callyear,
                $5  AS accountingyear,
                $6  AS expperiodyear,
                $7  AS expperiodmonth,
                $8  AS expeperiodday,
                $9  AS coveragecode,
                $10 AS classificationcode,
                $11 AS territorycode,
                $12 AS stateexceptionind,
                $13 AS zipcode,
                $14 AS policyeffectiveyear,
                $15 AS newrecordformat,
                $16 AS aslob,
                $17 AS itemcode,
                $18 AS sublinecode,
                $19 AS policyprogramcode,
                $20 AS policyformcode,
                $21 AS no_of_family_codes,
                $22 AS construction,
                $23 AS protectioncode,
                $24 AS exceptioncode,
                $25 AS typeofdeductiblecode,
                $26 AS policytermcode,
                $27 AS typeoflosscode,
                $28 AS stateexceptionb,
                $29 AS amountofinsurance,
                $30 AS yeaofmanufacture,
                $31 AS coveragecodeb,
                $32 AS exposurecodes,
                $33 AS leadpoisioning,
                $34 AS dedindicator,
                $35 AS dedamount,
                $36 AS deductibleindicatorws,
                $37 AS deductibleamountws,
                $38 AS claimidentifier,
                $39 AS claimantidentifier,
                $40 AS writtenexposure,
                $41 AS writtenpremium,
                $42 AS paidlosses,
                $43 AS paidnumberofclaims,
                $44 AS outstandinglosses,
                $45 AS outstandingnoofclaims,
                $46 AS policynumber,
                $47 AS policyperiodid,
                $48 AS policysystemperiodid,
                $49 AS territorycode_pc,
                $50 AS zipcode_pc,
                $51 AS itemcode_pc,
                $52 AS sublinecode_pc,
                $53 AS numberoffamilycodes_pc,
                $54 AS constructioncode_pc,
                $55 AS protectioncasscode_pc,
                $56 AS typeofdeductiblecode_pc,
                $57 AS amountofinsurance_pc,
                $58 AS yeaofconstructionliabblt_pc,
                $59 AS deductibleindicator_pc,
                $60 AS deductibleamount_pc,
                $61 AS deductibleindicatorws_pc,
                $62 AS deductibleamountws_pc,
                $63 AS policynumber_pc,
                $64 AS policyidentifier_pc,
                $65 AS coveragecode_pc,
                $66 AS territorycode_pclkp,
                $67 AS zipcode_pclkp,
                $68 AS itemcode_pclkp,
                $69 AS sublinecode_pclkp,
                $70 AS no_of_family_code_pclkp,
                $71 AS construction_pclkp,
                $72 AS protectionclass_pclkp,
                $73 AS typeofdeductible_pclkp,
                $74 AS amountofinsurance_pclkp,
                $75 AS yeaofmanufacture_pclkp,
                $76 AS ded_ind_pclkp,
                $77 AS ded_amt_pclkp,
                $78 AS ded_wind_ind_pclkp,
                $79 AS ded_wind_amt_pclkp,
                $80 AS policynumber_pclkp,
                $81 AS coverage_pclkp,
                $82 AS policyperiodid_pclkp,
                $83 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                                  SELECT          sq_clm.companynumber                            AS companynumber,
                                                                  sq_clm.lob                                      AS lob,
                                                                  sq_clm.statecode                                AS statecode,
                                                                  sq_clm.callyear                                 AS callyear,
                                                                  sq_clm.accountingyear                           AS accountingyear,
                                                                  sq_clm.expperiodyear                            AS expperiodyear,
                                                                  sq_clm.expperiodmonth                           AS expperiodmonth ,
                                                                  sq_clm.expperiodday                             AS expperiodday,
                                                                  sq_clm.coveragecode                             AS coveragecode,
                                                                  sq_clm.classificationcode                       AS classificationcode,
                                                                  sq_clm.territorycode                            AS territorycode,
                                                                  sq_clm.stateexceptionind                        AS stateexceptionind,
                                                                  sq_clm.zipcode                                  AS zipcode,
                                                                  sq_clm.policy_eff_yr                            AS policy_eff_yr,
                                                                  sq_clm.newrecordformat                          AS newrecordformat,
                                                                  sq_clm.aslob                                    AS aslob,
                                                                  sq_clm.itemcode                                 AS itemcode,
                                                                  sq_clm.sublinecode                              AS sublinecode,
                                                                  sq_clm.policyprogramcode                        AS policyprogramcode,
                                                                  sq_clm.policy_form_code                         AS policy_form_code,
                                                                  sq_clm.no_of_family_codes                       AS no_of_family_codes,
                                                                  sq_clm.construction_new                         AS construction_new,
                                                                  sq_clm.protectionclasscode                      AS protectionclasscode,
                                                                  sq_clm.exceptioncode                            AS exceptioncode,
                                                                  sq_clm.typeofdeductiblecode                     AS typeofdeductiblecode,
                                                                  sq_clm.policytermcode                           AS policytermcode,
                                                                  sq_clm.losscode                                 AS losscode,
                                                                  sq_clm.stateexceptionb                          AS stateexceptionb,
                                                                  sq_clm.amountofinsurance                        AS amountofinsurance,
                                                                  sq_clm.yearofmanufacture                        AS yearofmanufacture,
                                                                  sq_clm.coveragecodeordlaw                       AS coveragecodeordlaw,
                                                                  sq_clm.exposurecodes                            AS exposurecodes,
                                                                  sq_clm.leadpoisoningliability                   AS leadpoisoningliability,
                                                                  sq_clm.ded_ind                                  AS ded_ind,
                                                                  sq_clm.ded_amount                               AS ded_amount,
                                                                  sq_clm.deductibleindicatorws                    AS deductibleindicatorws,
                                                                  sq_clm.deductibleamountws                       AS deductibleamountws,
                                                                  sq_clm.claimidentifier                          AS claimidentifier,
                                                                  sq_clm.claimantidentifier                       AS claimantidentifier,
                                                                  sq_clm.writtenexposure                          AS writtenexposure,
                                                                  sq_clm.writtenpremium                           AS writtenpremium,
                                                                  cast(sq_clm.paidlosses AS DECIMAL(18,2))        AS paidlosses,
                                                                  sq_clm.paidnumberofclaims                       AS paidnumberofclaims,
                                                                  cast(sq_clm.outstandinglosses AS DECIMAL(18,2)) AS outstandinglosses,
                                                                  sq_clm.outstandingclaims                        AS outstandingclaims,
                                                                  sq_clm.policynumber                             AS policynumber,
                                                                  sq_clm.policyperiodid                           AS policyperiodid,
                                                                  sq_clm.policyidentifier                         AS policyidentifier,
                                                                  lpad(trim(sq_pc.territorycode),2,''0'')           AS territorycode_pc ,
                                                                  substring(sq_pc.zipcode,1,5)                    AS zipcode_pc,
                                                                  sq_pc.itemcode                                  AS itemcode_pc,
                                                                  sq_pc.sublinecode                               AS sublinecode_pc ,
                                                                  sq_pc.no_of_family_codes                        AS no_of_family_codes_pc,
                                                                  sq_pc.construction                              AS construction_pc ,
                                                                  lpad(trim(sq_pc.protectionclasscode ),2,''0'')    AS protectionclasscode_pc,
                                                                  sq_pc.typeofdeductiblecode                      AS typeofdeductiblecode_pc,
                                                                  sq_pc.amountofinsurance                         AS amountofinsurance_pc,
                                                                  lpad(trim(sq_pc.yearofmanufacture ),4,''0'')      AS yearofmanufacture_pc,
                                                                  sq_pc.ded_ind                                   AS ded_ind_pc,
                                                                  sq_pc.ded_amount                                AS ded_amount_pc,
                                                                  sq_pc.deductibleindicatorws                     AS deductibleindicatorws_pc,
                                                                  lpad(trim(sq_pc.deductibleamountws ),7, ''0'')    AS deductibleamountws_pc,
                                                                  sq_pc.policynumber                              AS policynumber_pc,
                                                                  sq_pc.policyperiodid                            AS policyperiodid_pc,
                                                                  sq_pc.coverage                      AS coverage_pc,
                                                                  lpad(trim(lkp_pc.territorycode ),2,''0'')         AS territorycode_pclkp ,
                                                                  substring(lkp_pc.zipcode ,1,5)                  AS zipcode_pclkp,
                                                                  lkp_pc.itemcode                                 AS itemcode_pclkp,
                                                                  lkp_pc.sublinecode                              AS sublinecode_pclkp ,
                                                                  lkp_pc.no_of_family_code                        AS no_of_family_codes_pclkp,
                                                                  lkp_pc.construction                             AS construction_pclkp ,
                                                                  lpad(trim(lkp_pc.protectionclass ),2, ''0'')      AS protectionclasscode_pclkp,
                                                                  lkp_pc.typeofdeductible                         AS typeofdeductiblecode_pclkp,
                                                                  lkp_pc.amountofinsurance                        AS amountofinsurance_pclkp,
                                                                  lpad(trim( lkp_pc.yearofmanufacutre ),4, ''0'')   AS yearofmanufacture_pclkp,
                                                                  lkp_pc.ded_ind                                  AS ded_ind_pclkp,
                                                                  lkp_pc.ded_amt                                  AS ded_amount_pclkp,
                                                                  lkp_pc.ded_wind_ind                             AS ded_wind_ind_pclkp,
                                                                  lpad(trim( lkp_pc.ded_wind_amt ),7,''0'')         AS ded_wind_amt_pclkp,
                                                                  lkp_pc.policynumber                             AS policynumber_pclkp,
                                                                  lkp_pc.coverage                      AS coverage_pclkp ,
                                                                  lkp_pc.policyperiodid                           AS policyperiodid_pclkp
                                                  FROM            (
                                                                  (
                                                                                  SELECT DISTINCT companynumber,
                                                                                                  lob,
                                                                                                  statecode,
                                                                                                  callyear,
                                                                                                  accountingyear,
                                                                                                  expperiodyear,
                                                                                                  expperiodmonth,
                                                                                                  expperiodday,
                                                                                                  coveragecode,
                                                                                                  ''00'' classificationcode,
                                                                                                  ''0''  territorycode,
                                                                                                  ''0''  stateexceptionind,
                                                                                                  ''0''  zipcode,
                                                                                                  policy_eff_yr,
                                                                                                  ''D''  newrecordformat,
                                                                                                  ''040''aslob,
                                                                                                  ''0''  itemcode,
                                                                                                  ''0''  sublinecode,
                                                                                                  ''0''  policyprogramcode,
                                                                                                  policy_form_code,
                                                                                                  ''0''  no_of_family_codes,
                                                                                                  ''0''  construction_new,
                                                                                                  ''0''  protectionclasscode,
                                                                                                  ''00'' exceptioncode,
                                                                                                  0    typeofdeductiblecode,
                                                                                                  ''00'' policytermcode,
                                                                                                  losscode,
                                                                                                  ''00'' stateexceptionb,
                                                                                                  0    amountofinsurance,
                                                                                                  0    yearofmanufacture,
                                                                                                  ''0''  coveragecodeordlaw,
                                                                                                  ''0''  exposurecodes,
                                                                                                  ''0''  leadpoisoningliability,
                                                                                                  0    ded_ind,
                                                                                                  0    ded_amount,
                                                                                                  0    deductibleindicatorws,
                                                                                                  0    deductibleamountws,
                                                                                                  claimidentifier,
                                                                                                  claimantidentifier,
                                                                                                  0                                                      writtenexposure,
                                                                                                  cast(''0'' AS VARCHAR(40))                               writtenpremium,
                                                                                                  cast(cast(paidlosses AS DECIMAL(18,2)) AS VARCHAR(40)) paidlosses,
                                                                                                  paidnumberofclaims,
                                                                                                  cast(cast(outstandinglosses AS DECIMAL(18,2)) AS VARCHAR(40)) outstandinglosses,
                                                                                                  outstandingclaims,
                                                                                                  policynumber,
                                                                                                  policyperiodid,
                                                                                                  policyperiodid policyidentifier,
                                                                                                  effectivedate
                                                                                  FROM            (
                                                                                                           SELECT   policysystemperiodid AS policyperiodid,
                                                                                                                    companynumber,
                                                                                                                    lob,
                                                                                                                    statecode,
                                                                                                                    callyear,
                                                                                                                    accountingyear,
                                                                                                                    exp_yr    expperiodyear,
                                                                                                                    exp_mth   expperiodmonth,
                                                                                                                    exp_day   expperiodday,
                                                                                                                    cvge_code coveragecode,
                                                                                                                    territorycode,
                                                                                                                    zipcode,
                                                                                                                    policyeffectiveyear policy_eff_yr,
                                                                                                                    claimidentifier,
                                                                                                                    policynumber,
                                                                                                                    city_stg,
                                                                                                                    ann_stmt_lob,
                                                                                                                    CASE
                                                                                                                             WHEN policysubtype=''HO2'' THEN ''02''
                                                                                                                             WHEN policysubtype=''HO4'' THEN ''04''
                                                                                                                             WHEN policysubtype=''HO5'' THEN ''05''
                                                                                                                             WHEN policysubtype=''HO6'' THEN ''06''
                                                                                                                             WHEN policysubtype=''HO8'' THEN ''08''
                                                                                                                             ELSE ''03''
                                                                                                                    END              policy_form_code,
                                                                                                                    losscause        losscode,
                                                                                                                    claimantdenormid claimantidentifier,
                                                                                                                    SUM(paidloss)    paidlosses,
                                                                                                                    CASE
                                                                                                                             WHEN(
                                                                                                                                               max(closedate) > cast(:cc_boy AS timestamp)
                                                                                                                                      AND      max(closedate) < cast(:cc_eoy AS timestamp)
                                                                                                                                      AND      SUM(paidloss) > 0
                                                                                                                                      AND      max(covrank) >= 1) THEN 1
                                                                                                                             ELSE 0
                                                                                                                    END         AS paidnumberofclaims,
                                                                                                                    SUM(outres)    outstandinglosses,
                                                                                                                    CASE
                                                                                                                             WHEN(
                                                                                                                                               max(closedate) IS NULL
                                                                                                                                      OR       max(closedate) > cast(:cc_boy AS timestamp) )
                                                                                                                             AND      SUM (outres)>0
                                                                                                                             AND      max(covrank) >= 1 THEN 1
                                                                                                                             ELSE 0
                                                                                                                    END AS outstandingclaims,
                                                                                                                    effectivedate,
                                                                                                                    legacypolind_alfa_stg ,
                                                                                                                    yearbuilt_alfa_stg,
                                                                                                                    no_of_family_codes_stg,
                                                                                                                    construction,
                                                                                                                    protectionclasscode
                                                                                                           FROM     (
                                                                                                                             SELECT   policysystemperiodid,
                                                                                                                                      companynumber,
                                                                                                                                      lob,
                                                                                                                                      statecode,
                                                                                                                                      callyear,
                                                                                                                                      accountingyear,
                                                                                                                                      exp_yr,
                                                                                                                                      exp_mth,
                                                                                                                                      exp_day,
                                                                                                                                      cvge_code,
                                                                                                                                      territorycode,
                                                                                                                                      zipcode,
                                                                                                                                      policyeffectiveyear,
                                                                                                                                      claimidentifier,
                                                                                                                                      covrank,
                                                                                                                                      closedate,
                                                                                                                                      SUM(acct500104 - acct500204 + acct500214 - acct500304 + acct500314) AS paidloss,
                                                                                                                                      SUM(acct521004)                                                     AS paidalae,
                                                                                                                                      SUM(a."[Outstanding Reserves as of EOM]"+a.losspaymentscurrmo)      AS outres ,
                                                                                                                                      legacypolind_alfa_stg,
                                                                                                                                      policynumber,
                                                                                                                                      losscause,
                                                                                                                                      city_stg,
                                                                                                                                      ''040'' AS ann_stmt_lob,
                                                                                                                                      policysubtype,
                                                                                                                                      claimantdenormid,
                                                                                                                                      effectivedate,
                                                                                                                                      yearbuilt_alfa_stg,
                                                                                                                                      no_of_family_codes_stg,
                                                                                                                                      a.construction,
                                                                                                                                      protectionclasscode
                                                                                                                             FROM     (
                                                                                                                                                      SELECT DISTINCT pol.policysystemperiodid_stg AS policysystemperiodid,
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN uwc.typecode_stg=''AMI'' THEN ''0005''
                                                                                                                                                                                      WHEN uwc.typecode_stg=''AMG'' THEN ''0196''
                                                                                                                                                                                      WHEN uwc.typecode_stg=''AIC'' THEN ''0050''
                                                                                                                                                                                      WHEN uwc.typecode_stg=''AGI'' THEN ''0318''
                                                                                                                                                                      END  AS companynumber,
                                                                                                                                                                      ''18'' AS lob,
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN jd.typecode_stg=''AL'' THEN ''01''
                                                                                                                                                                                      WHEN jd.typecode_stg=''GA'' THEN ''10''
                                                                                                                                                                                      WHEN jd.typecode_stg=''MS'' THEN ''23''
                                                                                                                                                                      END                                                AS statecode,
                                                                                                                                                                      extract(year FROM cast(:cc_eoy AS timestamp )) + 1 AS callyear,
                                                                                                                                                                      extract(year FROM cast(:cc_eoy AS timestamp))      AS accountingyear,
                                                                                                                                                                      extract(year FROM clm.lossdate_stg)                AS exp_yr,
                                                                                                                                                                      right(''00''
                                                                                                                                                                                      || cast(extract(month FROM clm.lossdate_stg) AS VARCHAR(2)), 2) AS exp_mth,
                                                                                                                                                                      right(''00''
                                                                                                                                                                                      || cast(extract(day FROM clm.lossdate_stg)AS VARCHAR(2)), 2) AS exp_day,
                                                                                                                                                                      ''01''                                                                            cvge_code,
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN lc.name_stg LIKE ''%Fire%''
                                                                                                                                                                                      OR              lc.name_stg LIKE ''%Lightning%'' THEN ''01''
                                                                                                                                                                                      WHEN lc.name_stg LIKE ''%Wind%''
                                                                                                                                                                                      OR              lc.name_stg LIKE ''%Hail%''
                                                                                                                                                                                      OR              lc.name_stg IN (''Earthquake'',
                                                                                                                                                                                                        ''EC'') THEN ''02''
                                                                                                                                                                                      WHEN lc.name_stg IN ( ''Water / Frozen Pipes'',
                                                                                                                                                                                                        ''Water / Other'') THEN ''03''
                                                                                                                                                                                      WHEN lc.name_stg LIKE ''%Theft%'' THEN ''04''
                                                                                                                                                                                      WHEN lc.name_stg IN (''Vandalism/Malicious Mischief'',
                                                                                                                                                                                                        ''V & MM'',
                                                                                                                                                                                                        ''PD'',
                                                                                                                                                                                                        ''Other Perils'',
                                                                                                                                                                                                        ''Other Winter Weather'') THEN ''05''
                                                                                                                                                                                      WHEN lc.name_stg IN (''Dog Bite'',
                                                                                                                                                                                                        ''Liability / Other'',
                                                                                                                                                                                                        ''Liability/Other'',
                                                                                                                                                                                                        ''Liability/Other'',
                                                                                                                                                                                                        ''Slip / Fall'') THEN ''06''
                                                                                                                                                                                      WHEN lc.name_stg LIKE ''%Liability Medical%'' THEN ''08''
                                                                                                                                                                                      ELSE ''05''
                                                                                                                                                                      END                                                       losscause,
                                                                                                                                                                      ''00''                                                      AS territorycode,
                                                                                                                                                                      left ((cast(addr.postalcodedenorm_stg AS VARCHAR(40))),5) AS zipcode,
                                                                                                                                                                      extract(year FROM pol.effectivedate_stg)                  AS policyeffectiveyear,
                                                                                                                                                                      clm.claimnumber_stg                                       AS claimidentifier,
                                                                                                                                                                      legacypolind_alfa_stg,
                                                                                                                                                                      rank() over( PARTITION BY clm.claimnumber_stg ORDER BY cov.typecode_stg ASC) AS covrank,
                                                                                                                                                                      clm.closedate_stg                                                            AS closedate, (
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                                                      AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Deductible''
                                                                                                                                                                                                      AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                      AND             (
                                                                                                                                                                                                        (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp))
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                                                      AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Deductible Refund''
                                                                                                                                                                                                      AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                      AND             (
                                                                                                                                                                                                        (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp))
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                                                      AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Former Deductible''
                                                                                                                                                                                                      AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                      AND             (
                                                                                                                                                                                                        (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp))
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                                                      AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Loss''
                                                                                                                                                                                                      AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                      AND             (
                                                                                                                                                                                                        (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp))
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                                                                      /* Added as part of ticket EIM-46304 */
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                                                      AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Diminished Value''
                                                                                                                                                                                                      AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                      AND             (
                                                                                                                                                                                                        (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp))
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                                                      AND             rctl.name_stg = ''Credit to loss''
                                                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Deductible''
                                                                                                                                                                                                      AND             txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                      AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                                                      AND             rctl.name_stg = ''Credit to loss''
                                                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Loss''
                                                                                                                                                                                                      AND             txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                      AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                                                      AND             rctl.name_stg = ''Credit to loss''
                                                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Recovery''
                                                                                                                                                                                                      AND             txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                      AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                                                      ELSE 0
                                                                                                                                                                      END ) AS acct500104, (
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                                                      AND             rctl.name_stg = ''Salvage''
                                                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Recovery''
                                                                                                                                                                                                      AND             txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                      AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)) THEN txli.transactionamount_stg
                                                                                                                                                                                      ELSE 0
                                                                                                                                                                      END) AS acct500204, (
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                                                      AND             cctl.name_stg=''Expense''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Expense''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Salvage Expense''
                                                                                                                                                                                                      AND             (
                                                                                                                                                                                                        (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp))
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        cast(ch.updatetime_stg AS DATE) >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                        AND             cast(ch.updatetime_stg AS DATE) <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                                                      AND             rctl.name_stg = ''Credit to expense''
                                                                                                                                                                                                      AND             cctl.name_stg=''Expense''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Expense''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Salvage Expense''
                                                                                                                                                                                                      AND             txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                      AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                                                      AND             rctl.name_stg = ''Salvage''
                                                                                                                                                                                                      AND             cctl.name_stg=''Expense''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Expense''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Salvage Expense''
                                                                                                                                                                                                      AND             txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                      AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                                                      AND             rctl.name_stg = ''Salvage''
                                                                                                                                                                                                      AND             cctl.name_stg=''Expense''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Expense''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Recovery''
                                                                                                                                                                                                      AND             txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                      AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                                                      ELSE 0
                                                                                                                                                                      END) AS acct500214, (
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                                                      AND             rctl.name_stg = ''Subrogation''
                                                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Recovery''
                                                                                                                                                                                                      AND             txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                      AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)) THEN txli.transactionamount_stg
                                                                                                                                                                                      ELSE 0
                                                                                                                                                                      END) AS acct500304, (
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                                                      AND             cctl.name_stg=''Expense''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Expense''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Subrogation Expense''
                                                                                                                                                                                                      AND             (
                                                                                                                                                                                                        (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp))
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                                                      AND             rctl.name_stg = ''Credit to expense''
                                                                                                                                                                                                      AND             cctl.name_stg=''Expense''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Expense''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Subrogation Expense''
                                                                                                                                                                                                      AND             txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                      AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                                                      AND             rctl.name_stg = ''Subrogation''
                                                                                                                                                                                                      AND             cctl.name_stg=''Expense''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Expense''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Subrogation Expense''
                                                                                                                                                                                                      AND             txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                      AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                                                      AND             rctl.name_stg = ''Subrogation''
                                                                                                                                                                                                      AND             cctl.name_stg=''Expense''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Expense''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Recovery''
                                                                                                                                                                                                      AND             txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                      AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                                                      ELSE 0
                                                                                                                                                                      END) AS acct500314, (
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                                                      AND             cctl.name_stg=''Expense''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Expense''
                                                                                                                                                                                                      AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Legal - Defense''
                                                                                                                                                                                                      AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                      AND             (
                                                                                                                                                                                                        (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp))
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                                                      AND             rctl.name_stg = ''Credit to expense''
                                                                                                                                                                                                      AND             cctl.name_stg=''Expense''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Expense''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Legal - Defense''
                                                                                                                                                                                                      AND             txli.createtime_stg >= cast(:cc_boy AS timestamp)
                                                                                                                                                                                                      AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                                                      ELSE 0
                                                                                                                                                                      END) AS acct521004,
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Reserve''
                                                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                                                      AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp) ) THEN txli.transactionamount_stg
                                                                                                                                                                                      ELSE 0
                                                                                                                                                                      END AS "[Outstanding Reserves as of EOM]",
                                                                                                                                                                      CASE
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                                                      AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Deductible''
                                                                                                                                                                                                      AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                      AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp) ) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                                                      AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Deductible Refund''
                                                                                                                                                                                                      AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                      AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp) ) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                                                      AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Former Deductible''
                                                                                                                                                                                                      AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                      AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp) ) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                                                      AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Loss''
                                                                                                                                                                                                      AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                      AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                                                                      /* Added as part of ticket EIM-46304 */
                                                                                                                                                                                      WHEN (
                                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                                                      AND             rctl.name_stg IS NULL
                                                                                                                                                                                                      AND             cctl.name_stg=''Loss''
                                                                                                                                                                                                      AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                                                      AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                                                      AND             lctl.name_stg = ''Diminished Value''
                                                                                                                                                                                                      AND             ch.issuedate_stg <= cast(:cc_eoy AS timestamp)
                                                                                                                                                                                                      AND             txli.createtime_stg <= cast(:cc_eoy AS timestamp)) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                                                      ELSE 0
                                                                                                                                                                      END                  AS losspaymentscurrmo ,
                                                                                                                                                                      pol.policynumber_stg AS policynumber,
                                                                                                                                                                      txli.id_stg,
                                                                                                                                                                      addr.city_stg,
                                                                                                                                                                      psttl.typecode_stg                                        AS policysubtype,
                                                                                                                                                                      left ((cast(exps.claimantdenormid_stg AS VARCHAR(40))),3)    claimantdenormid,
                                                                                                                                                                      pol.effectivedate_stg                                        effectivedate,
                                                                                                                                                                      yearbuilt_alfa_stg,
                                                                                                                                                                      ''00''                                no_of_family_codes_stg,
                                                                                                                                                                      ''00''                                AS construction,
                                                                                                                                                                      rsk.dwellingprotectionclasscode_stg AS protectionclasscode
                                                                                                                                                      FROM            db_t_prod_stag.cc_claim clm
                                                                                                                                                      join            db_t_prod_stag.cc_policy pol
                                                                                                                                                      ON              clm.policyid_stg=pol.id_stg
                                                                                                                                                      AND             ((
                                                                                                                                                                                                      clm.reporteddate_stg >= cast(:cc_eoy AS timestamp) - interval ''5 year''
                                                                                                                                                                                      AND             clm.reporteddate_stg <= cast(:cc_eoy AS timestamp))
                                                                                                                                                                      AND             (
                                                                                                                                                                                                      clm.lossdate_stg >= cast(:cc_eoy AS timestamp) - interval ''5 year''
                                                                                                                                                                                      AND             clm.lossdate_stg <= cast(:cc_eoy AS timestamp)))
                                                                                                                                                      AND             clm.claimnumber_stg LIKE ''H%''
                                                                                                                                                      join            db_t_prod_stag.cctl_claimstate sts
                                                                                                                                                      ON              clm.state_stg= sts.id_stg
                                                                                                                                                      AND             sts.name_stg <> ''Draft''
                                                                                                                                                      join            db_t_prod_stag.cctl_underwritingcompanytype uwc
                                                                                                                                                      ON              pol.underwritingco_stg=uwc.id_stg
                                                                                                                                                      join            db_t_prod_stag.cctl_jurisdiction jd
                                                                                                                                                      ON              pol.basesate_alfa_stg=jd.id_stg
                                                                                                                                                      join            db_t_prod_stag.cctl_policytype pttl
                                                                                                                                                      ON              pttl.id_stg = pol.policytype_stg
                                                                                                                                                      join            db_t_prod_stag.cctl_policysubtype_alfa psttl
                                                                                                                                                      ON              psttl.id_stg = pol.policysubtype_alfa_stg
                                                                                                                                                      join            db_t_prod_stag.cc_incident inc
                                                                                                                                                      ON              clm.id_stg=inc.claimid_stg
                                                                                                                                                      join            db_t_prod_stag.cc_exposure exps
                                                                                                                                                      ON              inc.id_stg=exps.incidentid_stg
                                                                                                                                                      AND             exps.retired_stg = 0
                                                                                                                                                      left join       db_t_prod_stag.cc_coverage cc_cov
                                                                                                                                                      ON              cc_cov.id_stg=exps.coverageid_stg
                                                                                                                                                      join            db_t_prod_stag.cctl_coveragesubtype cov
                                                                                                                                                      ON              cov.id_stg=exps.coveragesubtype_stg
                                                                                                                                                      left join       db_t_prod_stag.cc_riskunit rsk
                                                                                                                                                      ON              rsk.policyid_stg=clm.policyid_stg
                                                                                                                                                      left join       db_t_prod_stag.cctl_constructiontype_alfa cc_ct
                                                                                                                                                      ON              cc_ct.id_stg=constructiontype_alfa_stg
                                                                                                                                                      left join       db_t_prod_stag.cctl_dwellingusage_alfa cc_dt
                                                                                                                                                      ON              cc_dt.id_stg=dwellingusage_alfa_stg
                                                                                                                                                      join            db_t_prod_stag.cctl_coveragetype tl3
                                                                                                                                                      ON              tl3.id_stg = exps.primarycoverage_stg
                                                                                                                                                      join            db_t_prod_stag.cc_transaction tx
                                                                                                                                                      ON              tx.exposureid_stg = exps.id_stg
                                                                                                                                                      AND             tx.retired_stg = 0
                                                                                                                                                      join            db_t_prod_stag.cc_transactionlineitem txli
                                                                                                                                                      ON              txli.transactionid_stg = tx.id_stg
                                                                                                                                                      join            db_t_prod_stag.cctl_transactionstatus tl4
                                                                                                                                                      ON              tl4.id_stg = tx.status_stg
                                                                                                                                                      left join       db_t_prod_stag.cctl_transaction txtl
                                                                                                                                                      ON              txtl.id_stg = tx.subtype_stg
                                                                                                                                                      left join       db_t_prod_stag.cctl_linecategory lctl
                                                                                                                                                      ON              lctl.id_stg = txli.linecategory_stg
                                                                                                                                                      left join       db_t_prod_stag.cctl_recoverycategory rctl
                                                                                                                                                      ON              rctl.id_stg = tx.recoverycategory_stg
                                                                                                                                                      left join       db_t_prod_stag.cctl_costcategory cctl
                                                                                                                                                      ON              cctl.id_stg = tx.costcategory_stg
                                                                                                                                                      left join       db_t_prod_stag.cctl_costtype cttl
                                                                                                                                                      ON              cttl.id_stg = tx.costtype_stg
                                                                                                                                                      left join       db_t_prod_stag.cc_catastrophe cat
                                                                                                                                                      ON              cat.id_stg = clm.catastropheid_stg
                                                                                                                                                      left join       db_t_prod_stag.cc_check ch
                                                                                                                                                      ON              ch.id_stg = tx.checkid_stg
                                                                                                                                                      left join       db_t_prod_stag.cctl_paymentmethod pmtl
                                                                                                                                                      ON              pmtl.id_stg = ch.paymentmethod_stg
                                                                                                                                                      left join       db_t_prod_stag.cc_vehicle c
                                                                                                                                                      ON              inc.vehicleid_stg= c.id_stg
                                                                                                                                                      join            db_t_prod_stag.cctl_losscause lc
                                                                                                                                                      ON              lc.id_stg = clm.losscause_stg
                                                                                                                                                      left join       db_t_prod_stag.cctl_residencetype_alfa cctl_rt
                                                                                                                                                      ON              cctl_rt.id_stg=residencetype_alfa_stg
                                                                                                                                                      left join       db_t_prod_stag.cc_policylocation cc_pl
                                                                                                                                                      ON              pol.id_stg=cc_pl.policyid_stg
                                                                                                                                                      left join       db_t_prod_stag.cc_address addr
                                                                                                                                                      ON              addr.id_stg=cc_pl.addressid_stg
                                                                                                                                                      left join       db_t_prod_stag.cctl_state c_st
                                                                                                                                                      ON              jd.id_stg=addr.state_stg
                                                                                                                                                      AND             jd.typecode_stg IN (''al'',
                                                                                                                                                                                          ''ga'',
                                                                                                                                                                                          ''ms'')
                                                                                                                                                      WHERE           tl4.name_stg NOT IN (''Awaiting submission'',
                                                                                                                                                                                           ''Rejected'',
                                                                                                                                                                                           ''Submitting'',
                                                                                                                                                                                           ''Pending approval'') ) a
                                                                                                                             GROUP BY policysystemperiodid,
                                                                                                                                      companynumber,
                                                                                                                                      lob,
                                                                                                                                      statecode,
                                                                                                                                      cvge_code,
                                                                                                                                      covrank,
                                                                                                                                      closedate,
                                                                                                                                      callyear,
                                                                                                                                      accountingyear,
                                                                                                                                      exp_yr,
                                                                                                                                      exp_mth,
                                                                                                                                      exp_day,
                                                                                                                                      territorycode,
                                                                                                                                      zipcode,
                                                                                                                                      policyeffectiveyear,
                                                                                                                                      a.policysubtype,
                                                                                                                                      claimidentifier,
                                                                                                                                      city_stg,
                                                                                                                                      policynumber,
                                                                                                                                      claimantdenormid,
                                                                                                                                      losscause,
                                                                                                                                      effectivedate,
                                                                                                                                      legacypolind_alfa_stg,
                                                                                                                                      yearbuilt_alfa_stg,
                                                                                                                                      no_of_family_codes_stg,
                                                                                                                                      a.construction,
                                                                                                                                      protectionclasscode) b
                                                                                                           GROUP BY policysystemperiodid,
                                                                                                                    effectivedate,
                                                                                                                    legacypolind_alfa_stg,
                                                                                                                    companynumber,
                                                                                                                    lob,
                                                                                                                    statecode,
                                                                                                                    callyear,
                                                                                                                    accountingyear,
                                                                                                                    exp_yr,
                                                                                                                    exp_mth,
                                                                                                                    exp_day,
                                                                                                                    cvge_code,
                                                                                                                    territorycode,
                                                                                                                    zipcode,
                                                                                                                    policyeffectiveyear,
                                                                                                                    claimidentifier,
                                                                                                                    policynumber,
                                                                                                                    city_stg,
                                                                                                                    losscause,
                                                                                                                    ann_stmt_lob,
                                                                                                                    policysubtype,
                                                                                                                    claimantdenormid,
                                                                                                                    yearbuilt_alfa_stg,
                                                                                                                    no_of_family_codes_stg,
                                                                                                                    construction,
                                                                                                                    protectionclasscode
                                                                                                           HAVING   SUM(outres)<>0.00
                                                                                                           OR       SUM(paidloss)<>0.00 
																		)c 
												  ) sq_clm
                                                  left outer join ( WITH terr AS
                                                                  (
                                                                            SELECT    branchid_stg                                                                                                             id,
                                                                                      max(cast(coalesce(old_code,pht1.naiipcicode_alfa_stg, pht2.naiipcicode_alfa_stg, pht3.naiipcicode_alfa_stg) AS INTEGER) )code,
                                                                                      max(postalcodeinternal)                                                                                                  postalcodeinternal
                                                                            FROM      (
                                                                                                SELECT    e.branchid_stg,
                                                                                                          policynumber_stg,
                                                                                                          c.code_stg,
                                                                                                          g.typecode_stg,
                                                                                                          c.countycode_alfa_stg,
                                                                                                          pc_policyline.hopolicytype_stg,
                                                                                                          coalesce(postalcodeinternal_stg, postalcode_stg)postalcodeinternal,
                                                                                                          row_number() over( PARTITION BY e.branchid_stg,policynumber_stg, c.code_stg,g.typecode_stg, c.countycode_alfa_stg, pc_policyline.hopolicytype_stg ORDER BY
                                                                                                          CASE
                                                                                                                    WHEN (
                                                                                                                                        c.policylocation_stg = b.id_stg ) THEN 1
                                                                                                                    ELSE 2
                                                                                                          END) ROWNUM,
                                                                                                          cityinternal_stg,
                                                                                                          countyinternal_stg,
                                                                                                          county_stg,
                                                                                                          CASE
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       upper(cityinternal_stg)= ''BIRMINGHAM''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''JEFFERSON'' THEN ''32''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       upper(cityinternal_stg)= ''HUNTSVILLE''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''MADISON'' THEN ''35''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       upper(cityinternal_stg)= ''MONTGOMERY''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''MONTGOMERY'' THEN ''37''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       upper(cityinternal_stg)= ''MOBILE''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''MOBILE'' THEN ''30''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''AUTAUGA'',
                                                                                                                                                                                   ''ELMORE'',
                                                                                                                                                                                   ''MONTGOMERY'') THEN ''38''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''BALDWIN''
                                                                                                                    AND       cast(c.code_stg AS INTEGER)=26 THEN ''41''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''BALDWIN''
                                                                                                                    AND       cast(c.code_stg AS INTEGER)=28 THEN ''31''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''BALDWIN''
                                                                                                                    AND       (
                                                                                                                                        cast(c.code_stg AS INTEGER)=11
                                                                                                                              OR        cast(c.code_stg AS INTEGER) IS NULL
                                                                                                                              OR        cast(c.code_stg AS INTEGER) IN (1,2,3) ) THEN ''05''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''BARBOUR'',
                                                                                                                                                                                   ''BIBB'',
                                                                                                                                                                                   ''BLOUNT'',
                                                                                                                                                                                   ''BULLOCK'',
                                                                                                                                                                                   ''BUTLER'',
                                                                                                                                                                                   ''CHAMBERS'',
                                                                                                                                                                                   ''CHEROKEE'',
                                                                                                                                                                                   ''CHILTON'',
                                                                                                                                                                                   ''CHOCTAW'',
                                                                                                                                                                                   ''CLARKE'',
                                                                                                                                                                                   ''CLAY'',
                                                                                                                                                                                   ''CLEBURNE'',
                                                                                                                                                                                   ''COFFEE'',
                                                                                                                                                                                   ''CONECUH'',
                                                                                                                                                                                   ''COOSA'',
                                                                                                                                                                                   ''COVINGTON'',
                                                                                                                                                                                   ''CRENSHAW'',
                                                                                                                                                                                   ''CULLMAN'',
                                                                                                                                                                                   ''DALE'',
                                                                                                                                                                                   ''DALLAS'',
                                                                                                                                                                                   ''DEKALB'',
                                                                                                                                                                                   ''DE KALB'',
                                                                                                                                                                                   ''ESCAMBIA'',
                                                                                                                                                                                   ''FAYETTE'',
                                                                                                                                                                                   ''FRANKLIN'',
                                                                                                                                                                                   ''GENEVA'',
                                                                                                                                                                                   ''GREENE'',
                                                                                                                                                                                   ''HALE'',
                                                                                                                                                                                   ''HENRY'',
                                                                                                                                                                                   ''HOUSTON'',
                                                                                                                                                                                   ''JACKSON'',
                                                                                                                                                                                   ''LAMAR'',
                                                                                                                                                                                   ''LAWRENCE'',
                                                                                                                                                                                   ''LEE'',
                                                                                                                                                                                   ''LOWNDES'',
                                                                                                                                                                                   ''MACON'',
                                                                                                                                                                                   ''MONROE'',
                                                                                                                                                                                   ''MARENGO'',
                                                                                                                                                                                   ''MARION'',
                                                                                                                                                                                   ''MARSHALL'',
                                                                                                                                                                                   ''PERRY'',
                                                                                                                                                                                   ''PICKENS'',
                                                                                                                                                                                   ''PIKE'',
                                                                                                                                                                                   ''RANDOLPH'',
                                                                                                                                                                                   ''RUSSELL'',
                                                                                                                                                                                   ''SAINT CLAIR'',
                                                                                                                                                                                   ''ST. CLAIR'',
                                                                                                                                                                                   ''SUMTER'',
                                                                                                                                                                                   ''TALLADEGA'',
                                                                                                                                                                                   ''TALLAPOOSA'',
                                                                                                                                                                                   ''WASHINGTON'',
                                                                                                                                                                                   ''WILCOX'',
                                                                                                                                                                                   ''WINSTON'') THEN ''41''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''CALHOUN'',
                                                                                                                                                                                   ''ETOWAH'') THEN ''40''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''COLBERT'',
                                                                                                                                                                                   ''LAUDERDALE'',
                                                                                                                                                                                   ''LIMESTONE'',
                                                                                                                                                                                   ''MADISON'',
                                                                                                                                                                                   ''MORGAN'') THEN ''36''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''JEFFERSON'' THEN ''33''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''MOBILE''
                                                                                                                    AND       (
                                                                                                                                        cast(c.code_stg AS INTEGER)IN (2,1,26,3 )
                                                                                                                              OR        cast(c.code_stg AS INTEGER)IS NULL) THEN ''41''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''MOBILE''
                                                                                                                    AND       cast(c.code_stg AS INTEGER)=28 THEN ''31''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''MOBILE''
                                                                                                                    AND       cast(c.code_stg AS INTEGER)=11 THEN ''05''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''SHELBY'',
                                                                                                                                                                                   ''WALKER'') THEN ''34''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''TUSCALOOSA'' THEN ''39''
                                                                                                                    /*WHEN g.typecode_stg=''AL''
                                                                                                                    AND       (
                                                                                                                                        substring (b.postalcodeinternal_stg,0,INDEX(''-'',b.postalcodeinternal_stg))=''36511''
                                                                                                                              OR        b.postalcodeinternal_stg=''36511'')
                                                                                                                    AND       upper(cityinternal_stg)= ''BON SECOUR'' THEN ''06''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       (
                                                                                                                                        substring (b.postalcodeinternal_stg,0,INDEX(''-'',b.postalcodeinternal_stg))=''36528''
                                                                                                                              OR        b.postalcodeinternal_stg=''36528'')
                                                                                                                    AND       upper(cityinternal_stg)= ''DAUPHIN ISLAND'' THEN ''06''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       (
                                                                                                                                        substring (b.postalcodeinternal_stg,0,INDEX(''-'',b.postalcodeinternal_stg)) IN (''36542'',
                                                                                                                                                                                                        ''36547'')
                                                                                                                              OR        b.postalcodeinternal_stg IN (''36542'',
                                                                                                                                                                     ''36547''))
                                                                                                                    AND       upper(cityinternal_stg)= ''GULF SHORES'' THEN ''06''
                                                                                                                    WHEN g.typecode_stg=''AL''
                                                                                                                    AND       (
                                                                                                                                        substring (b.postalcodeinternal_stg,0,INDEX(''-'',b.postalcodeinternal_stg))=''36561''
                                                                                                                              OR        b.postalcodeinternal_stg=''36561'')
                                                                                                                    AND       upper(cityinternal_stg)= ''ORANGE BEACH'' THEN ''06'' */
                                                                                                                    WHEN g.typecode_stg = ''AL''
                                                                                                                       AND (
                                                                                                                       IFF(POSITION(''-'' IN b.postalcodeinternal_stg) > 0,
                                                                                                                              SUBSTRING(b.postalcodeinternal_stg, 1, POSITION(''-'' IN b.postalcodeinternal_stg) - 1),
                                                                                                                              b.postalcodeinternal_stg
                                                                                                                       ) = ''36511''
                                                                                                                       OR b.postalcodeinternal_stg = ''36511''
                                                                                                                       )
                                                                                                                       AND UPPER(cityinternal_stg) = ''BON SECOUR'' THEN ''06''

                                                                                                                       WHEN g.typecode_stg = ''AL''
                                                                                                                       AND (
                                                                                                                       IFF(POSITION(''-'' IN b.postalcodeinternal_stg) > 0,
                                                                                                                              SUBSTRING(b.postalcodeinternal_stg, 1, POSITION(''-'' IN b.postalcodeinternal_stg) - 1),
                                                                                                                              b.postalcodeinternal_stg
                                                                                                                       ) = ''36528''
                                                                                                                       OR b.postalcodeinternal_stg = ''36528''
                                                                                                                       )
                                                                                                                       AND UPPER(cityinternal_stg) = ''DAUPHIN ISLAND'' THEN ''06''

                                                                                                                       WHEN g.typecode_stg = ''AL''
                                                                                                                       AND (
                                                                                                                       IFF(POSITION(''-'' IN b.postalcodeinternal_stg) > 0,
                                                                                                                              SUBSTRING(b.postalcodeinternal_stg, 1, POSITION(''-'' IN b.postalcodeinternal_stg) - 1),
                                                                                                                              b.postalcodeinternal_stg
                                                                                                                       ) IN (''36542'', ''36547'')
                                                                                                                       OR b.postalcodeinternal_stg IN (''36542'', ''36547'')
                                                                                                                       )
                                                                                                                       AND UPPER(cityinternal_stg) = ''GULF SHORES'' THEN ''06''

                                                                                                                       WHEN g.typecode_stg = ''AL''
                                                                                                                       AND (
                                                                                                                       IFF(POSITION(''-'' IN b.postalcodeinternal_stg) > 0,
                                                                                                                              SUBSTRING(b.postalcodeinternal_stg, 1, POSITION(''-'' IN b.postalcodeinternal_stg) - 1),
                                                                                                                              b.postalcodeinternal_stg
                                                                                                                       ) = ''36561''
                                                                                                                       OR b.postalcodeinternal_stg = ''36561''
                                                                                                                       )
                                                                                                                       AND UPPER(cityinternal_stg) = ''ORANGE BEACH'' THEN ''06''

                                                                                                                    WHEN g.typecode_stg=''MS''
                                                                                                                    AND       upper(cityinternal_stg)= ''JACKSON''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''HINDS'',
                                                                                                                                                                                   ''RANKIN'') THEN ''30''
                                                                                                                    WHEN g.typecode_stg=''MS''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''AMITE'',
                                                                                                                                                                                   ''FORREST'',
                                                                                                                                                                                   ''GREENE'',
                                                                                                                                                                                   ''LAMAR'',
                                                                                                                                                                                   ''MARION'',
                                                                                                                                                                                   ''PERRY'',
                                                                                                                                                                                   ''PIKE'',
                                                                                                                                                                                   ''WALTHALL'',
                                                                                                                                                                                   ''WILKINSON'') THEN ''03''
                                                                                                                    WHEN g.typecode_stg=''MS''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''GEORGE'',
                                                                                                                                                                                   ''PEARL RIVER'',
                                                                                                                                                                                   ''STONE'') THEN ''05''
                                                                                                                    WHEN g.typecode_stg=''MS''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''HANCOCK'',
                                                                                                                                                                                   ''HARRISON'',
                                                                                                                                                                                   ''JACKSON'') THEN ''06''
                                                                                                                    WHEN g.typecode_stg=''MS''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''HINDS'',
                                                                                                                                                                                   ''MADISON'',
                                                                                                                                                                                   ''RANKIN'') THEN ''31''
                                                                                                                    WHEN g.typecode_stg=''MS''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''ADAMS'',
                                                                                                                                                                                   ''ALCORN'',
                                                                                                                                                                                   ''ATTALA'',
                                                                                                                                                                                   ''BENTON'',
                                                                                                                                                                                   ''BOLIVAR'',
                                                                                                                                                                                   ''CALHOUN'',
                                                                                                                                                                                   ''CARROLL'',
                                                                                                                                                                                   ''CHICKASAW'',
                                                                                                                                                                                   ''CHOCTAW'',
                                                                                                                                                                                   ''CLAIBORNE'',
                                                                                                                                                                                   ''CLARKE'',
                                                                                                                                                                                   ''CLAY'',
                                                                                                                                                                                   ''COAHOMA'',
                                                                                                                                                                                   ''COPIAH'',
                                                                                                                                                                                   ''COVINGTON'',
                                                                                                                                                                                   ''DESOTO'',
                                                                                                                                                                                   ''FRANKLIN'',
                                                                                                                                                                                   ''GRENADA'',
                                                                                                                                                                                   ''HOLMES'',
                                                                                                                                                                                   ''HUMPHREYS'',
                                                                                                                                                                                   ''ISSAQUENA'',
                                                                                                                                                                                   ''ITAWAMBA'',
                                                                                                                                                                                   ''JASPER'',
                                                                                                                                                                                   ''JEFFERSON'',
                                                                                                                                                                                   ''JEFFERSON DAVIS'',
                                                                                                                                                                                   ''JONES'',
                                                                                                                                                                                   ''KEMPER'',
                                                                                                                                                                                   ''LAFAYETTE'',
                                                                                                                                                                                   ''LAUDERDALE'',
                                                                                                                                                                                   ''LAWRENCE'',
                                                                                                                                                                                   ''LEAKE'',
                                                                                                                                                                                   ''LEE'',
                                                                                                                                                                                   ''LEFLORE'',
                                                                                                                                                                                   ''LINCOLN'',
                                                                                                                                                                                   ''LOWNDES'',
                                                                                                                                                                                   ''MARSHALL'',
                                                                                                                                                                                   ''MONROE'',
                                                                                                                                                                                   ''MONTGOMERY'',
                                                                                                                                                                                   ''NESHOBA'',
                                                                                                                                                                                   ''NEWTON'',
                                                                                                                                                                                   ''NOXUBEE'',
                                                                                                                                                                                   ''OKTIBBEHA'',
                                                                                                                                                                                   ''PANOLA'',
                                                                                                                                                                                   ''PONTOTOC'',
                                                                                                                                                                                   ''PRENTISS'',
                                                                                                                                                                                   ''QUITMAN'',
                                                                                                                                                                                   ''SCOTT'',
                                                                                                                                                                                   ''SHARKEY'',
                                                                                                                                                                                   ''SIMPSON'',
                                                                                                                                                                                   ''SMITH'',
                                                                                                                                                                                   ''SUNFLOWER'',
                                                                                                                                                                                   ''TALLAHATCHIE'',
                                                                                                                                                                                   ''TATE'',
                                                                                                                                                                                   ''TIPPAH'',
                                                                                                                                                                                   ''TISHOMINGO'',
                                                                                                                                                                                   ''TUNICA'',
                                                                                                                                                                                   ''UNION'',
                                                                                                                                                                                   ''WARREN'',
                                                                                                                                                                                   ''WASHINGTON'',
                                                                                                                                                                                   ''WAYNE'',
                                                                                                                                                                                   ''WEBSTER'',
                                                                                                                                                                                   ''WINSTON'',
                                                                                                                                                                                   ''YALOBUSHA'',
                                                                                                                                                                                   ''YAZOO'') THEN ''32''
                                                                                                                    WHEN g.typecode_stg=''GA''
                                                                                                                    AND       upper(cityinternal_stg)= ''ATLANTA''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''DE KALB'',
                                                                                                                                                                                   ''DEKALB'',
                                                                                                                                                                                   ''FULTON'') THEN ''32''
                                                                                                                    WHEN g.typecode_stg=''GA''
                                                                                                                    AND       upper(cityinternal_stg)= ''MACON''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''BIBB'' THEN ''35''
                                                                                                                    WHEN g.typecode_stg=''GA''
                                                                                                                    AND       upper(cityinternal_stg)= ''SAVANNAH''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg))=''CHATHAM'' THEN ''30''
                                                                                                                    WHEN g.typecode_stg=''GA''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''DE KALB'',
                                                                                                                                                                                   ''DEKALB'',
                                                                                                                                                                                   ''FULTON'') THEN ''33''
                                                                                                                    WHEN g.typecode_stg=''GA''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''BRYAN'',
                                                                                                                                                                                   ''CAMDEN'',
                                                                                                                                                                                   ''CHATHAM'',
                                                                                                                                                                                   ''GLYNN'',
                                                                                                                                                                                   ''LIBERTY'',
                                                                                                                                                                                   ''MCINTOSH'') THEN ''31''
                                                                                                                    WHEN g.typecode_stg=''GA''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''DE KALB'',
                                                                                                                                                                                   ''DEKALB'',
                                                                                                                                                                                   ''FULTON'') THEN ''33''
                                                                                                                    WHEN g.typecode_stg=''GA''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''CLAYTON'',
                                                                                                                                                                                   ''COBB'',
                                                                                                                                                                                   ''GWINNETT'') THEN ''34''
                                                                                                                    WHEN g.typecode_stg=''GA''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''CATOOSA'',
                                                                                                                                                                                   ''WALKER'',
                                                                                                                                                                                   ''WHITFIELD'') THEN ''36''
                                                                                                                    WHEN g.typecode_stg=''GA''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) =''RICHMOND'' THEN ''37''
                                                                                                                    WHEN g.typecode_stg=''GA''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''CHATTAHOOCHEE'',
                                                                                                                                                                                   ''MUSCOGEE'') THEN ''38''
                                                                                                                    WHEN g.typecode_stg=''GA''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''BUTTS'',
                                                                                                                                                                                   ''CHEROKEE'',
                                                                                                                                                                                   ''DOUGLAS'',
                                                                                                                                                                                   ''FAYETTE'',
                                                                                                                                                                                   ''FORSYTH'',
                                                                                                                                                                                   ''HENRY'',
                                                                                                                                                                                   ''NEWTON'',
                                                                                                                                                                                   ''PAULDING'',
                                                                                                                                                                                   ''ROCKDALE'',
                                                                                                                                                                                   ''WALTON'') THEN ''39''
                                                                                                                    WHEN g.typecode_stg=''GA''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''BALDWIN'',
                                                                                                                                                                                   ''BANKS'',
                                                                                                                                                                                   ''BARROW'',
                                                                                                                                                                                   ''BARTOW'',
                                                                                                                                                                                   ''CARROLL'',
                                                                                                                                                                                   ''CHATTOOGA'',
                                                                                                                                                                                   ''CLARKE'',
                                                                                                                                                                                   ''COLUMBIA'',
                                                                                                                                                                                   ''COWETA'',
                                                                                                                                                                                   ''DADE'',
                                                                                                                                                                                   ''DAWSON'',
                                                                                                                                                                                   ''ELBERT'',
                                                                                                                                                                                   ''FANNIN'',
                                                                                                                                                                                   ''FLOYD'',
                                                                                                                                                                                   ''FRANKLIN'',
                                                                                                                                                                                   ''GILMER'',
                                                                                                                                                                                   ''GORDON'',
                                                                                                                                                                                   ''GREENE'',
                                                                                                                                                                                   ''HABERSHAM'',
                                                                                                                                                                                   ''HALL'',
                                                                                                                                                                                   ''HANCOCK'',
                                                                                                                                                                                   ''HARALSON'',
                                                                                                                                                                                   ''HART'',
                                                                                                                                                                                   ''HEARD'',
                                                                                                                                                                                   ''JACKSON'',
                                                                                                                                                                                   ''JASPER'',
                                                                                                                                                                                   ''JONES'',
                                                                                                                                                                                   ''LAMAR'',
                                                                                                                                                                                   ''LINCOLN'',
                                                                                                                                                                                   ''LUMPKIN'',
                                                                                                                                                                                   ''MADISON'',
                                                                                                                                                                                   ''MCDUFFIE'',
                                                                                                                                                                                   ''MERIWETHER'',
                                                                                                                                                                                   ''MONROE'',
                                                                                                                                                                                   ''MORGAN'',
                                                                                                                                                                                   ''MURRAY'',
                                                                                                                                                                                   ''OCONEE'',
                                                                                                                                                                                   ''OGLETHORPE'',
                                                                                                                                                                                   ''PICKENS'',
                                                                                                                                                                                   ''PIKE'',
                                                                                                                                                                                   ''POLK'',
                                                                                                                                                                                   ''PUTNAM'',
                                                                                                                                                                                   ''RABUN'',
                                                                                                                                                                                   ''SPALDING'',
                                                                                                                                                                                   ''STEPHENS'',
                                                                                                                                                                                   ''TALIAFERRO'',
                                                                                                                                                                                   ''TOWNS'',
                                                                                                                                                                                   ''TROUP'',
                                                                                                                                                                                   ''UNION'',
                                                                                                                                                                                   ''WARREN'',
                                                                                                                                                                                   ''WHITE'',
                                                                                                                                                                                   ''WILKES'') THEN ''40''
                                                                                                                    WHEN g.typecode_stg=''GA''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''BAKER'',
                                                                                                                                                                                   ''BIBB'',
                                                                                                                                                                                   ''BROOKS'',
                                                                                                                                                                                   ''CALHOUN'',
                                                                                                                                                                                   ''CLAY'',
                                                                                                                                                                                   ''COLQUITT'',
                                                                                                                                                                                   ''CRAWFORD'',
                                                                                                                                                                                   ''CRISP'',
                                                                                                                                                                                   ''DECATUR'',
                                                                                                                                                                                   ''DOOLY'',
                                                                                                                                                                                   ''DOUGHERTY'',
                                                                                                                                                                                   ''EARLY'',
                                                                                                                                                                                   ''GRADY'',
                                                                                                                                                                                   ''HARRIS'',
                                                                                                                                                                                   ''HOUSTON'',
                                                                                                                                                                                   ''LEE'',
                                                                                                                                                                                   ''MACON'',
                                                                                                                                                                                   ''MARION'',
                                                                                                                                                                                   ''MILLER'',
                                                                                                                                                                                   ''MITCHELL'',
                                                                                                                                                                                   ''PEACH'',
                                                                                                                                                                                   ''QUITMAN'',
                                                                                                                                                                                   ''RANDOLPH'',
                                                                                                                                                                                   ''SCHLEY'',
                                                                                                                                                                                   ''SEMINOLE'',
                                                                                                                                                                                   ''STEWART'',
                                                                                                                                                                                   ''SUMTER'',
                                                                                                                                                                                   ''TALBOT'',
                                                                                                                                                                                   ''TAYLOR'',
                                                                                                                                                                                   ''TERRELL'',
                                                                                                                                                                                   ''THOMAS'',
                                                                                                                                                                                   ''TIFT'',
                                                                                                                                                                                   ''TURNER'',
                                                                                                                                                                                   ''UPSON'',
                                                                                                                                                                                   ''WEBSTER'',
                                                                                                                                                                                   ''WORTH'') THEN ''41''
                                                                                                                    WHEN g.typecode_stg=''GA''
                                                                                                                    AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''APPLING'',
                                                                                                                                                                                   ''ATKINSON'',
                                                                                                                                                                                   ''BACON'',
                                                                                                                                                                                   ''BEN HILL'',
                                                                                                                                                                                   ''BERRIEN'',
                                                                                                                                                                                   ''BLECKLEY'',
                                                                                                                                                                                   ''BRANTLEY'',
                                                                                                                                                                                   ''BULLOCH'',
                                                                                                                                                                                   ''BURKE'',
                                                                                                                                                                                   ''CANDLER'',
                                                                                                                                                                                   ''CHARLTON'',
                                                                                                                                                                                   ''CLINCH'',
                                                                                                                                                                                   ''COFFEE'',
                                                                                                                                                                                   ''COOK'',
                                                                                                                                                                                   ''DODGE'',
                                                                                                                                                                                   ''ECHOLS'',
                                                                                                                                                                                   ''EFFINGHAM'',
                                                                                                                                                                                   ''EMANUEL'',
                                                                                                                                                                                   ''EVANS'',
                                                                                                                                                                                   ''GLASCOCK'',
                                                                                                                                                                                   ''IRWIN'',
                                                                                                                                                                                   ''JEFF DAVIS'',
                                                                                                                                                                                   ''JEFFERSON'',
                                                                                                                                                                                   ''JENKINS'',
                                                                                                                                                                                   ''JOHNSON'',
                                                                                                                                                                                   ''LANIER'',
                                                                                                                                                                                   ''LAURENS'',
                                                                                                                                                                                   ''LONG'',
                                                                                                                                                                                   ''LOWNDES'',
                                                                                                                                                                                   ''MONTGOMERY'',
                                                                                                                                                                                   ''PIERCE'',
                                                                                                                                                                                   ''PULASKI'',
                                                                                                                                                                                   ''SCREVEN'',
                                                                                                                                                                                   ''TATTNALL'',
                                                                                                                                                                                   ''TELFAIR'',
                                                                                                                                                                                   ''TOOMBS'',
                                                                                                                                                                                   ''TREUTLEN'',
                                                                                                                                                                                   ''TWIGGS'',
                                                                                                                                                                                   ''WARE'',
                                                                                                                                                                                   ''WASHINGTON'',
                                                                                                                                                                                   ''WAYNE'',
                                                                                                                                                                                   ''WHEELER'',
                                                                                                                                                                                   ''WILCOX'',
                                                                                                                                                                                   ''WILKINSON'') THEN ''42''
                                                                                                          END AS old_code
                                                                                                FROM      db_t_prod_stag.pcx_holocation_hoe a
                                                                                                join      db_t_prod_stag.pcx_dwelling_hoe e
                                                                                                ON        e.holocation_stg=a.id_stg
                                                                                                join      db_t_prod_stag.pc_policyperiod f
                                                                                                ON        e.branchid_stg =f.id_stg
                                                                                                left join db_t_prod_stag.pc_effectivedatedfields eff
                                                                                                ON        eff.branchid_stg = f.id_stg
                                                                                                AND       eff.expirationdate_stg IS NULL
                                                                                                left join db_t_prod_stag.pc_policylocation b
                                                                                                ON        b.id_stg = eff.primarylocation_stg
                                                                                                AND       b.expirationdate_stg IS NULL
                                                                                                join      db_t_prod_stag.pc_territorycode c
                                                                                                ON        c.branchid_stg = f.id_stg
                                                                                                join      db_t_prod_stag.pctl_territorycode d
                                                                                                ON        c.subtype_stg=d.id_stg
                                                                                                AND       d.typecode_stg = ''HOTerritoryCode_alfa''
                                                                                                left join db_t_prod_stag.pc_contact pc
                                                                                                ON        pc.id_stg =pnicontactdenorm_stg
                                                                                                left join db_t_prod_stag.pc_address
                                                                                                ON        pc.primaryaddressid_stg = pc_address.id_stg
                                                                                                join      db_t_prod_stag.pc_policyline
                                                                                                ON        f.id_stg = pc_policyline.branchid_stg
                                                                                                AND       pc_policyline.expirationdate_stg IS NULL
                                                                                                join      db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                ON        pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                AND       pctl_hopolicytype_hoe.typecode_stg LIKE ''HO%''
                                                                                                join      db_t_prod_stag.pctl_jurisdiction g
                                                                                                ON        basestate_stg=g.id_stg
                                                                                                AND       g.typecode_stg IN (''AL'',
                                                                                                                             ''GA'',
                                                                                                                             ''MS'')) loc
                                                                            left join db_t_prod_stag.pcx_hodbterritory_alfa pht1
                                                                            ON        pht1.code_stg =loc.code_stg
                                                                            AND       cast(pht1.countycode_alfa_stg AS VARCHAR(50))=cast(loc.countycode_alfa_stg AS VARCHAR(50))
                                                                            AND       substring(pht1.publicid_stg,7,2) =loc.typecode_stg
                                                                            AND       pht1.hopolicytype_hoe_stg=loc.hopolicytype_stg
                                                                            left join
                                                                                      (
                                                                                                      SELECT DISTINCT naiipcicode_alfa_stg,
                                                                                                                      substring(publicid_stg,7,2) state,
                                                                                                                      code_stg                    territory_code,
                                                                                                                      hopolicytype_hoe_stg,
                                                                                                                      rank() over( PARTITION BY naiipcicode_alfa_stg, substring(publicid_stg,7,2) , code_stg , hopolicytype_hoe_stg ORDER BY naiipcicode_alfa_stg,countycode_alfa_stg )row1
                                                                                                      FROM            db_t_prod_stag.pcx_hodbterritory_alfa
                                                                                                      WHERE           publicid_stg LIKE ''%HO%'')pht2
                                                                            ON        pht2.row1=1
                                                                            AND       pht2.territory_code =loc.code_stg
                                                                            AND       pht2.state =loc.typecode_stg
                                                                            AND       pht2.hopolicytype_hoe_stg=loc.hopolicytype_stg
                                                                            left join
                                                                                      (
                                                                                                      SELECT DISTINCT naiipcicode_alfa_stg,
                                                                                                                      substring(publicid_stg,7,2) state,
                                                                                                                      countycode_alfa_stg         countycode_alfa,
                                                                                                                      hopolicytype_hoe_stg,
                                                                                                                      rank() over( PARTITION BY naiipcicode_alfa_stg, substring(publicid_stg,7,2) , countycode_alfa_stg , hopolicytype_hoe_stg ORDER BY naiipcicode_alfa_stg,code_stg )row1
                                                                                                      FROM            db_t_prod_stag.pcx_hodbterritory_alfa
                                                                                                      WHERE           publicid_stg LIKE ''%HO%'')pht3
                                                                            ON        pht3.row1=1
                                                                            AND       cast(pht3.countycode_alfa AS VARCHAR(50))=cast(loc.countycode_alfa_stg AS VARCHAR(50))
                                                                            AND       pht3.state =loc.typecode_stg
                                                                            AND       pht3.hopolicytype_hoe_stg=loc.hopolicytype_stg
                                                                            WHERE     ROWNUM=1
                                                                            GROUP BY  branchid_stg ),  -- end of terr cte
                                                                  cov AS
                                                                  (
                                                                                  SELECT DISTINCT branchid_stg,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN covterm.covtermpatternid=''HODW_Dwelling_Limit_HOE'' THEN (lpad(cast(cast(round(cast(polcov.val/1000 AS DECIMAL(18,4)), 0)AS INTEGER) AS VARCHAR(10)) ,5, ''0''))
                                                                                                  END)dw_limit,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN covterm.covtermpatternid=''HODW_DwellingAdditionalLimit_alfa'' THEN cast(polcov.val AS DECIMAL(18,4))
                                                                                                  END )lia_limit,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN covterm.covtermpatternid=''HODW_PersonalPropertyLimit_alfa'' THEN
                                                                                                                                  CASE
                                                                                                                                                  WHEN length(polcov.val)>12 THEN 0000
                                                                                                                                                  ELSE substring(''00000'',1,(5-length( cast(cast(round(cast(polcov.val AS DECIMAL(18,4))/1000,0)AS INTEGER) AS VARCHAR(10)))))
                                                                                                                                                                                  || cast(cast(round(cast(polcov.val AS DECIMAL(18,4))/1000, 0)AS INTEGER) AS VARCHAR(10))
                                                                                                                                  END
                                                                                                  END )pp_limit,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN covterm.covtermpatternid=''HODW_OtherPerils_Ded_HOE'' THEN (
                                                                                                                                  CASE
                                                                                                                                                  WHEN substring(name1 ,length(name1),1)=''%'' THEN ''F''
                                                                                                                                                  ELSE ''D''
                                                                                                                                  END)
                                                                                                  END)perils_limit_ind,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN covterm.covtermpatternid=''HODW_OtherPerils_Ded_HOE'' THEN (
                                                                                                                                  CASE
                                                                                                                                                  WHEN substring(name1 ,length(name1),1)=''%'' THEN substring(''0000000'',1, (7-length(cast(cast(cast(cast(value1 AS DECIMAL(18,4)) *10000 AS DECIMAL(18,4)) AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                                                  ||cast(cast(cast(cast(value1 AS DECIMAL(18,4)) *10000 AS DECIMAL(18,4)) AS INTEGER) AS VARCHAR(10))
                                                                                                                                                  ELSE substring(''0000000'',1,(7-length(cast(cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                                                  ||cast(cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER) AS VARCHAR(10))
                                                                                                                                  END)
                                                                                                  END)perils_limit,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN patterncode =''HODW_Earthquake_HOE'' THEN patterncode
                                                                                                  END) earthquake,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN patterncode =''HODW_PersonalPropertyReplacementCost_alfa'' THEN patterncode
                                                                                                  END) replacement,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  covtermpatternid =''HODW_WindHail_Ded_HOE'') THEN covtermpatternid
                                                                                                  END )windhail,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  covtermpatternid =''HODW_Hurricane_Ded_HOE'' ) THEN covtermpatternid
                                                                                                  END )hurricane,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  covtermpatternid =''HODW_SectionI_DedWindAndHailExcl_alfa'') THEN covtermpatternid
                                                                                                  END )windstormhailexcl,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  covtermpatternid =''HODW_SectionI_DedWindAndHailExcl_alfa'') THEN coalesce( polcov.val, value1)
                                                                                                  END )windstormhailexcl_amt,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN covtermpatternid IN (''HODW_WindHail_Ded_HOE'',
                                                                                                                                            ''HODW_Hurricane_Ded_HOE'',
                                                                                                                                            ''HODW_SectionI_DedWindstorm_alfa'')
                                                                                                                  AND             polcov.columnname LIKE ''%direct%'' THEN polcov.val
                                                                                                                  WHEN covtermpatternid IN (''HODW_WindHail_Ded_HOE'',
                                                                                                                                            ''HODW_Hurricane_Ded_HOE'',
                                                                                                                                            ''HODW_SectionI_DedWindstorm_alfa'')
                                                                                                                  AND             polcov.columnname NOT LIKE ''%direct%'' THEN (
                                                                                                                                  CASE
                                                                                                                                                  WHEN (
                                                                                                                                                                                  cast(value1 AS DECIMAL(18,4))<=1.0000 ) THEN substring(''0000000'',1, (7-length(cast(cast(cast(value1 AS DECIMAL(18,4))*10000 AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                                                  ||cast(cast(cast(value1 AS DECIMAL(18,4))                             *10000 AS INTEGER) AS VARCHAR(10))
                                                                                                                                                  WHEN value1 IS NULL
                                                                                                                                                  OR              value1 =0 THEN 0
                                                                                                                                                  ELSE substring(''0000000'',1,(7                                             -length(cast(cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                                                  ||cast(cast(cast(value1 AS DECIMAL(18,4)) *10000 AS INTEGER) AS VARCHAR(10))
                                                                                                                                  END )
                                                                                                  END ) AS deductibleamountws,
                                                                                                  max(
                                                                                                  CASE
                                                                                                                  WHEN covtermpatternid IN (''HODW_WindHail_Ded_HOE'',
                                                                                                                                            ''HODW_Hurricane_Ded_HOE'',
                                                                                                                                            ''HODW_SectionI_DedWindstorm_alfa'')
                                                                                                                  AND             polcov.columnname LIKE ''%direct%'' THEN polcov.val
                                                                                                                  WHEN covtermpatternid IN (''HODW_WindHail_Ded_HOE'',
                                                                                                                                            ''HODW_Hurricane_Ded_HOE'',
                                                                                                                                            ''HODW_SectionI_DedWindstorm_alfa'')
                                                                                                                  AND             polcov.columnname NOT LIKE ''%direct%'' THEN (
                                                                                                                                  CASE
                                                                                                                                                  WHEN (
                                                                                                                                                                                  cast(value1 AS DECIMAL(18,4))<=1.0000) THEN ''F''
                                                                                                                                                  WHEN value1 IS NULL
                                                                                                                                                  OR              value1=0 THEN NULL
                                                                                                                                                  ELSE ''D''
                                                                                                                                  END )
                                                                                                  END ) AS deductiblews
                                                                                  FROM            (
                                                                                                             SELECT     cast(''DirectTerm1'' AS                       VARCHAR(100)) AS columnname,
                                                                                                                        cast(directterm1_stg AS                     VARCHAR(255)) AS val,
                                                                                                                        cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid_stg,
                                                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                        pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                                                        pcx_dwellingcov_hoe.          expirationdate_stg,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                             join       db_t_prod_stag.pc_policyline
                                                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                               ''HO3'',
                                                                                                                                                               ''HO4'',
                                                                                                                                                               ''HO5'',
                                                                                                                                                               ''HO6'',
                                                                                                                                                               ''HO8'')
                                                                                                             WHERE      directterm1avl_stg = 1
                                                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                             UNION
                                                                                                             SELECT     ''Clause''                   AS columnname,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS val,
                                                                                                                        pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                        pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                                                        pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                             join       db_t_prod_stag.pc_policyline
                                                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                               ''HO3'',
                                                                                                                                                               ''HO4'',
                                                                                                                                                               ''HO5'',
                                                                                                                                                               ''HO6'',
                                                                                                                                                               ''HO8'')
                                                                                                             WHERE      pcx_dwellingcov_hoe.patterncode_stg= ''HODW_PersonalPropertyReplacementCost_alfa''
                                                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                             UNION
                                                                                                             SELECT     ''DirectTerm2''                         AS columnname,
                                                                                                                        cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                                                                                        pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                        pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                                                        pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                             join       db_t_prod_stag.pc_policyline
                                                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                               ''HO3'',
                                                                                                                                                               ''HO4'',
                                                                                                                                                               ''HO5'',
                                                                                                                                                               ''HO6'',
                                                                                                                                                               ''HO8'')
                                                                                                             WHERE      directterm2avl_stg = 1
                                                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                             UNION
                                                                                                             SELECT     ''DirectTerm3''                         AS columnname,
                                                                                                                        cast(directterm3_stg AS VARCHAR(255)) AS val,
                                                                                                                        pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                        pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                                                        pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                             join       db_t_prod_stag.pc_policyline
                                                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                               ''HO3'',
                                                                                                                                                               ''HO4'',
                                                                                                                                                               ''HO5'',
                                                                                                                                                               ''HO6'',
                                                                                                                                                               ''HO8'')
                                                                                                             WHERE      directterm3avl_stg = 1
                                                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                             UNION
                                                                                                             SELECT     ''DirectTerm4''                         AS columnname,
                                                                                                                        cast(directterm4_stg AS VARCHAR(255)) AS val,
                                                                                                                        pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                        pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                                                        pcx_dwellingcov_hoe.          expirationdate_stg,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                             join       db_t_prod_stag.pc_policyline
                                                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                               ''HO3'',
                                                                                                                                                               ''HO4'',
                                                                                                                                                               ''HO5'',
                                                                                                                                                               ''HO6'',
                                                                                                                                                               ''HO8'')
                                                                                                             WHERE      directterm4avl_stg = 1
                                                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                             UNION
                                                                                                             SELECT     ''ChoiceTerm2''                         AS columnname,
                                                                                                                        cast(choiceterm2_stg AS VARCHAR(255)) AS val,
                                                                                                                        pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                        pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                                                        pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                             join       db_t_prod_stag.pc_policyline
                                                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                               ''HO3'',
                                                                                                                                                               ''HO4'',
                                                                                                                                                               ''HO5'',
                                                                                                                                                               ''HO6'',
                                                                                                                                                               ''HO8'')
                                                                                                             WHERE      choiceterm2avl_stg = 1
                                                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                             UNION
                                                                                                             SELECT     ''ChoiceTerm3''                         AS columnname,
                                                                                                                        cast(choiceterm3_stg AS VARCHAR(255)) AS val,
                                                                                                                        pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                        pcx_dwellingcov_hoe. effectivedate_stg,
                                                                                                                        pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                             join       db_t_prod_stag.pc_policyline
                                                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                               ''HO3'',
                                                                                                                                                               ''HO4'',
                                                                                                                                                               ''HO5'',
                                                                                                                                                               ''HO6'',
                                                                                                                                                               ''HO8'')
                                                                                                             WHERE      choiceterm3avl_stg = 1
                                                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                             UNION
                                                                                                             SELECT     cast(''ChoiceTerm1'' AS                       VARCHAR(250)) AS columnname,
                                                                                                                        cast(choiceterm1_stg AS                     VARCHAR(255)) AS val,
                                                                                                                        cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid_stg,
                                                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                        pcx_dwellingcov_hoe. effectivedate_stg,
                                                                                                                        pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                             join       db_t_prod_stag.pc_policyline
                                                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                               ''HO3'',
                                                                                                                                                               ''HO4'',
                                                                                                                                                               ''HO5'',
                                                                                                                                                               ''HO6'',
                                                                                                                                                               ''HO8'')
                                                                                                             WHERE      choiceterm1avl_stg = 1
                                                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                             UNION
                                                                                                             SELECT     cast(''ChoiceTerm4'' AS                       VARCHAR(250)) AS columnname,
                                                                                                                        cast(choiceterm4_stg AS                     VARCHAR(255)) AS val,
                                                                                                                        cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid_stg,
                                                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                        pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                                                        pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                             join       db_t_prod_stag.pc_policyline
                                                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                               ''HO3'',
                                                                                                                                                               ''HO4'',
                                                                                                                                                               ''HO5'',
                                                                                                                                                               ''HO6'',
                                                                                                                                                               ''HO8'')
                                                                                                             WHERE      choiceterm4avl_stg = 1
                                                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                             UNION
                                                                                                             SELECT     cast(''BooleanTerm1'' AS                      VARCHAR(250)) AS columnname,
                                                                                                                        cast(booleanterm1_stg AS                    VARCHAR(255)) AS val,
                                                                                                                        cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid_stg,
                                                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                        pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                                                        pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                             join       db_t_prod_stag.pc_policyline
                                                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                               ''HO3'',
                                                                                                                                                               ''HO4'',
                                                                                                                                                               ''HO5'',
                                                                                                                                                               ''HO6'',
                                                                                                                                                               ''HO8'')
                                                                                                             WHERE      pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                             AND        pcx_dwellingcov_hoe.patterncode_stg =''HODW_SectionI_Ded_HOE'' ) polcov
                                                                                  left join
                                                                                                  (
                                                                                                         SELECT pcl.patternid_stg     clausepatternid,
                                                                                                                pcv.patternid_stg     covtermpatternid,
                                                                                                                pcv.columnname_stg  AS columnname,
                                                                                                                pcv.covtermtype_stg AS covtermtype,
                                                                                                                pcl.name_stg           clausename
                                                                                                         FROM   db_t_prod_stag.pc_etlclausepattern pcl
                                                                                                         join   db_t_prod_stag.pc_etlcovtermpattern pcv
                                                                                                         ON     pcl.id_stg = pcv.clausepatternid_stg ) covterm
                                                                                  ON              covterm.clausepatternid = polcov.patterncode
                                                                                  AND             covterm.columnname = polcov.columnname
                                                                                  left outer join
                                                                                                  (
                                                                                                             SELECT     pco.patternid_stg                      optionpatternid,
                                                                                                                        pco.optioncode_stg                     name1,
                                                                                                                        cast(pco.value_stg AS VARCHAR(255)) AS value1,
                                                                                                                        pcp.valuetype_stg                   AS valuetype
                                                                                                             FROM       db_t_prod_stag.pc_etlcovtermpattern pcp
                                                                                                             inner join db_t_prod_stag.pc_etlcovtermoption pco
                                                                                                             ON         pcp.id_stg = pco.coveragetermpatternid_stg ) optn
                                                                                  ON              optn.optionpatternid = polcov.val
                                                                                  GROUP BY        branchid_stg )  -- end cov cte
                                                  SELECT DISTINCT coalesce(territorycode, ''00'') territorycode,
                                                                  coalesce(zipcode, ''00000'')    zipcode,
                                                                  itemcode,
                                                                  sublinecode,
                                                                  no_of_family_codes,
                                                                  construction,
                                                                  coalesce(protectionclasscode, ''00'')        protectionclasscode,
                                                                  coalesce(typeofdeductiblecode, ''00'')       typeofdeductiblecode,
                                                                  coalesce(amountofinsurance, ''00000'')       amountofinsurance,
                                                                  coalesce(yearofmanufacture, ''0000'')        yearofmanufacture,
                                                                  coalesce(ded_ind,''0'')                      ded_ind,
                                                                  coalesce(ded_amount, ''0000000'')            ded_amount,
                                                                  coalesce(deductibleindicatorws, ''0'')       deductibleindicatorws,
                                                                  coalesce(deductibleamountws, ''0000000'')    deductibleamountws,
                                                                  policynumber_stg                        AS policynumber,
                                                                  policyperiodid,
                                                                  coverage
                                                  FROM            (
                                                                                  SELECT DISTINCT coalesce(terr.code,''00'')                   AS territorycode,
                                                                                                  coalesce( terr.postalcodeinternal, ''00000'')AS zipcode,
                                                                                                  CASE
                                                                                                                  WHEN earthquake =''HODW_Earthquake_HOE'' THEN ''01''
                                                                                                                  WHEN ph.typecode_stg =''HO4'' THEN ''02''
                                                                                                                  ELSE ''03''
                                                                                                  END AS itemcode,
                                                                                                  CASE
                                                                                                                  WHEN earthquake =''HODW_Earthquake_HOE'' THEN ''60''
                                                                                                                  WHEN replacement =''HODW_PersonalPropertyReplacementCost_alfa''
                                                                                                                  AND             ph.typecode_stg<>''HO8'' THEN ''03''
                                                                                                                  WHEN ph.typecode_stg=''HO8'' THEN ''02''
                                                                                                                  ELSE ''02''
                                                                                                  END AS sublinecode,
                                                                                                  CASE
                                                                                                                  WHEN ph.typecode_stg=''HO2'' THEN ''02''
                                                                                                                  WHEN ph.typecode_stg=''HO4'' THEN ''04''
                                                                                                                  WHEN ph.typecode_stg=''HO5'' THEN ''05''
                                                                                                                  WHEN ph.typecode_stg=''HO6'' THEN ''06''
                                                                                                                  WHEN ph.typecode_stg=''HO8'' THEN ''08''
                                                                                                                  ELSE ''03''
                                                                                                  END policy_form_code,
                                                                                                  CASE
                                                                                                                  WHEN ph.typecode_stg=''HO4'' THEN ''2''
                                                                                                                  WHEN pr.typecode_stg IN (''Apt'' ,
                                                                                                                                           ''Condo'',
                                                                                                                                           ''Coop'',
                                                                                                                                           ''Duplex'',
                                                                                                                                           ''Mobile'',
                                                                                                                                           ''Modular_alfa'',
                                                                                                                                           ''TownRow'',
                                                                                                                                           ''Fam1'',
                                                                                                                                           ''Fam2'',
                                                                                                                                           ''Fam3'',
                                                                                                                                           ''Fam3To4_alfa'',
                                                                                                                                           ''Fam4'') THEN ''1''
                                                                                                                  ELSE ''2''
                                                                                                  END no_of_family_codes,
                                                                                                  CASE
                                                                                                                  WHEN pct.typecode_stg IN (''ADB'',
                                                                                                                                            ''AOD'',
                                                                                                                                            ''CLP'',
                                                                                                                                            ''COM'',
                                                                                                                                            ''CUS'',
                                                                                                                                            ''DOM'',
                                                                                                                                            ''F'',
                                                                                                                                            ''FRM'',
                                                                                                                                            ''FST'',
                                                                                                                                            ''GLA'',
                                                                                                                                            ''HEA'',
                                                                                                                                            ''L'',
                                                                                                                                            ''LOG'',
                                                                                                                                            ''LIG'',
                                                                                                                                            ''NON'',
                                                                                                                                            ''OTH'',
                                                                                                                                            ''OTHER'',
                                                                                                                                            ''PFR'',
                                                                                                                                            ''STU'',
                                                                                                                                            ''STW'',
                                                                                                                                            ''TLU'',
                                                                                                                                            ''UNK'',
                                                                                                                                            ''WOO'',
                                                                                                                                            ''WSC'') THEN ''1''
                                                                                                                  WHEN pct.typecode_stg IN (''BCB'',
                                                                                                                                            ''BLB'',
                                                                                                                                            ''BRC'',
                                                                                                                                            ''BRF'',
                                                                                                                                            ''BRS'',
                                                                                                                                            ''BST'',
                                                                                                                                            ''FRY'',
                                                                                                                                            ''SRO'',
                                                                                                                                            ''STV'',
                                                                                                                                            ''TBM'',
                                                                                                                                            ''WBR'',
                                                                                                                                            ''WCB'',
                                                                                                                                            ''WSN'') THEN ''2''
                                                                                                                  WHEN pct.typecode_stg IN (''BRK'',
                                                                                                                                            ''CCM'',
                                                                                                                                            ''CNB'',
                                                                                                                                            ''CND'',
                                                                                                                                            ''CRE'',
                                                                                                                                            ''FLX'',
                                                                                                                                            ''M'',
                                                                                                                                            ''MAS'',
                                                                                                                                            ''MTU'',
                                                                                                                                            ''TUC'',
                                                                                                                                            ''SFM'') THEN ''3''
                                                                                                                  WHEN pct.typecode_stg IN (''BLM'',
                                                                                                                                            ''BLS'',
                                                                                                                                            ''BRL'',
                                                                                                                                            ''BRM'',
                                                                                                                                            ''CCS'',
                                                                                                                                            ''MET'',
                                                                                                                                            ''PRM'',
                                                                                                                                            ''S'',
                                                                                                                                            ''STE'',
                                                                                                                                            ''STS'') THEN ''4''
                                                                                                                  WHEN pct.typecode_stg IN (''ALF'',
                                                                                                                                            ''ALS'',
                                                                                                                                            ''ALV'',
                                                                                                                                            ''FRS'',
                                                                                                                                            ''WMT'',
                                                                                                                                            ''WSL'') THEN ''5''
                                                                                                                  WHEN pct.typecode_stg IN (''MAN'') THEN ''6''
                                                                                                  END construction,
                                                                                                  CASE
                                                                                                                  WHEN phh.dwellingprotectionclasscode_stg IS NULL
                                                                                                                  AND             jd.typecode_stg =''AL'' THEN ''05''
                                                                                                                  WHEN phh.dwellingprotectionclasscode_stg IS NULL
                                                                                                                  AND             jd.typecode_stg =''GA'' THEN ''03''
                                                                                                                  WHEN phh.dwellingprotectionclasscode_stg IS NULL
                                                                                                                  AND             jd.typecode_stg =''MS'' THEN ''10''
                                                                                                                  ELSE coalesce(phh.dwellingprotectionclasscode_stg, ''00'')
                                                                                                  END AS protectionclasscode,
                                                                                                  CASE
                                                                                                                  WHEN jd.typecode_stg=''AL'' THEN
                                                                                                                                  CASE
                                                                                                                                                  WHEN (
                                                                                                                                                                                  windhail =''HODW_WindHail_Ded_HOE'')
                                                                                                                                                  AND             deductibleamountws >0 THEN ''35''
                                                                                                                                                  WHEN (
                                                                                                                                                                                  hurricane =''HODW_Hurricane_Ded_HOE''
                                                                                                                                                                  AND             terr.code IN (''05'',
                                                                                                                                                                                                ''06'',
                                                                                                                                                                                                ''30'',
                                                                                                                                                                                                ''31''))
                                                                                                                                                  AND             deductibleamountws >0 THEN ''55''
                                                                                                                                                  WHEN (
                                                                                                                                                                                  hurricane =''HODW_Hurricane_Ded_HOE''
                                                                                                                                                                  AND             terr.code NOT IN (''05'',
                                                                                                                                                                                                    ''06'',
                                                                                                                                                                                                    ''30'',
                                                                                                                                                                                                    ''31''))
                                                                                                                                                  AND             deductibleamountws >0 THEN ''35''
                                                                                                                                                  WHEN (
                                                                                                                                                                                  windstormhailexcl =''HODW_SectionI_DedWindAndHailExcl_alfa'')
                                                                                                                                                  AND             (
                                                                                                                                                                                  windstormhailexcl_amt <> 0
                                                                                                                                                                  AND             windstormhailexcl_amt IS NOT NULL) THEN ''07''
                                                                                                                                                  ELSE ''05''
                                                                                                                                  END
                                                                                                                  WHEN jd.typecode_stg IN (''GA'',
                                                                                                                                           ''MS'') THEN
                                                                                                                                  CASE
                                                                                                                                                  WHEN (
                                                                                                                                                                                  (
                                                                                                                                                                                                  windhail =''HODW_WindHail_Ded_HOE'')
                                                                                                                                                                  OR              (
                                                                                                                                                                                                  hurricane =''HODW_Hurricane_Ded_HOE'' ) )
                                                                                                                                                  AND             deductibleamountws >0 THEN ''05''
                                                                                                                                                  WHEN (
                                                                                                                                                                                  windstormhailexcl =''HODW_SectionI_DedWindAndHailExcl_alfa'')
                                                                                                                                                  AND             (
                                                                                                                                                                                  windstormhailexcl_amt <> 0
                                                                                                                                                                  AND             windstormhailexcl_amt IS NOT NULL) THEN ''07''
                                                                                                                                                  ELSE ''05''
                                                                                                                                  END
                                                                                                  END AS typeofdeductiblecode,
                                                                                                  coalesce((
                                                                                                  CASE
                                                                                                                  WHEN ph.typecode_stg IN (''HO4'' ,
                                                                                                                                           ''HO6'') THEN pp_limit
                                                                                                                  ELSE dw_limit
                                                                                                  END),''00000'') AS amountofinsurance,
                                                                                                  coalesce(
                                                                                                  CASE
                                                                                                                  WHEN (
                                                                                                                                                  ph.typecode_stg IN (''HO2'',
                                                                                                                                                                      ''HO3'' ,
                                                                                                                                                                      ''HO5'',
                                                                                                                                                                      ''HO8''))
                                                                                                                  AND             (
                                                                                                                                                  pdh.yearbuilt_stg<=1959) THEN ''1959''
                                                                                                                  WHEN (
                                                                                                                                                  ph.typecode_stg IN (''HO2'',
                                                                                                                                                                      ''HO3'' ,
                                                                                                                                                                      ''HO5'',
                                                                                                                                                                      ''HO8''))
                                                                                                                  AND             (
                                                                                                                                                  pdh.yearbuilt_stg>1959) THEN pdh.yearbuilt_stg
                                                                                                                  WHEN (
                                                                                                                                                  ph.typecode_stg IN (''HO4'')) THEN ''0000''
                                                                                                                  WHEN ph.typecode_stg IN (''HO6'')
                                                                                                                  AND             lia_limit BETWEEN 1001.00 AND             9999.00 THEN ''0002''
                                                                                                                  WHEN ph.typecode_stg IN (''HO6'')
                                                                                                                  AND             lia_limit BETWEEN 10000.00 AND             19999.00 THEN ''0003''
                                                                                                                  WHEN ph.typecode_stg IN (''HO6'')
                                                                                                                  AND             lia_limit BETWEEN 20000.00 AND             29999.00 THEN ''0004''
                                                                                                                  WHEN ph.typecode_stg IN (''HO6'')
                                                                                                                  AND             lia_limit BETWEEN 30000.00 AND             39999.00 THEN ''0005''
                                                                                                                  WHEN ph.typecode_stg IN (''HO6'')
                                                                                                                  AND             lia_limit BETWEEN 40000.00 AND             49999.00 THEN ''0006''
                                                                                                                  WHEN ph.typecode_stg IN (''HO6'')
                                                                                                                  AND             lia_limit BETWEEN 50000.00 AND             59999.00 THEN ''0007''
                                                                                                                  WHEN ph.typecode_stg IN (''HO6'')
                                                                                                                  AND             lia_limit BETWEEN 60000.00 AND             69999.00 THEN ''0008''
                                                                                                                  WHEN ph.typecode_stg IN (''HO6'')
                                                                                                                  AND             lia_limit >70000.00 THEN ''0009''
                                                                                                                  ELSE ''0001''
                                                                                                  END ,''0000'')                     AS yearofmanufacture,
                                                                                                  coalesce(perils_limit_ind , ''0'')    ded_ind,
                                                                                                  coalesce(perils_limit,''0000000'')    ded_amount,
                                                                                                  ''3''                              AS tiedowncode,
                                                                                                  coalesce(
                                                                                                  CASE
                                                                                                                  WHEN jd.typecode_stg=''AL'' THEN deductiblews
                                                                                                                  ELSE ''0''
                                                                                                  END,''0'') AS deductibleindicatorws,
                                                                                                  coalesce(
                                                                                                  CASE
                                                                                                                  WHEN jd.typecode_stg=''AL'' THEN cast(cast(deductibleamountws AS INTEGER) AS VARCHAR(7) )
                                                                                                                  ELSE ''0''
                                                                                                  END ,''0000000'')AS deductibleamountws,
                                                                                                  pp.policynumber_stg,
                                                                                                  pp.id_stg                                                                           policyperiodid ,
                                                                                                  row_number() over( PARTITION BY (pp.id_stg) ORDER BY pp.editeffectivedate_stg DESC) row1 ,
                                                                                                  CASE
                                                                                                                  WHEN pcdh.name_stg= ''Secondary'' THEN ''07''
                                                                                                                  ELSE ''01''
                                                                                                  END coverage
                                                                                  FROM            db_t_prod_stag.pc_policyperiod pp
                                                                                  join            db_t_prod_stag.pctl_jurisdiction jd
                                                                                  ON              pp.basestate_stg=jd.id_stg
                                                                                  join            db_t_prod_stag.pcx_dwelling_hoe pdh
                                                                                  ON              pdh.branchid_stg=pp.id_stg
                                                                                  left join       db_t_prod_stag.pctl_dwellingusage_hoe pcdh
                                                                                  ON              pcdh.id_stg=pdh.dwellingusage_stg
                                                                                  join            terr  -- using cte
                                                                                  ON              terr.id=pp.id_stg
                                                                                  join            db_t_prod_stag.pc_job pj
                                                                                  ON              pp.jobid_stg = pj.id_stg
                                                                                  join            db_t_prod_stag.pctl_job pcj
                                                                                  ON              pj.subtype_stg = pcj.id_stg
                                                                                  join            db_t_prod_stag.pcx_holocation_hoe phh
                                                                                  ON              pdh.holocation_stg= phh.id_stg
                                                                                  join            db_t_prod_stag.pctl_hopolicytype_hoe ph
                                                                                  ON              ph.id_stg=pdh.hopolicytype_stg
                                                                                  AND             ph.typecode_stg LIKE ''%HO%''
                                                                                  join            cov  -- using cte
                                                                                  ON              cast(cov.branchid_stg AS INTEGER) =pp.id_stg
                                                                                  left join       db_t_prod_stag.pctl_residencetype_hoe pr
                                                                                  ON              residencetype_stg= pr.id_stg
                                                                                  left join       db_t_prod_stag.pctl_constructiontype_hoe pct
                                                                                  ON              pct.id_stg=constructiontype_stg
                                                                                  join            db_t_prod_stag.pctl_policyperiodstatus
                                                                                  ON              pp.status_stg=pctl_policyperiodstatus.id_stg
                                                                                  join            db_t_prod_stag.pc_policyterm pt
                                                                                  ON              pt.id_stg = pp.policytermid_stg
                                                                                  join            db_t_prod_stag.pc_policyline
                                                                                  ON              pp.id_stg = pc_policyline.branchid_stg
                                                                                  AND             pc_policyline.expirationdate_stg IS NULL
                                                                                  join            db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                  ON              pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                  AND             pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                         ''HO3'',
                                                                                                                                         ''HO4'',
                                                                                                                                         ''HO5'',
                                                                                                                                         ''HO6'',
                                                                                                                                         ''HO8'') )a
                                                  WHERE           row1=1 ) sq_pc
                                                  ON              cast(sq_clm.policyperiodid AS INTEGER)=cast(sq_pc.policyperiodid AS INTEGER)
                                                  left outer join
                                                                  (
                                                                         SELECT pc_policyperiod.territorycode         AS territorycode,
                                                                                pc_policyperiod.zipcode               AS zipcode,
                                                                                pc_policyperiod.itemcode              AS itemcode,
                                                                                pc_policyperiod.sublinecode           AS sublinecode,
                                                                                pc_policyperiod.no_of_family_codes    AS no_of_family_code,
                                                                                pc_policyperiod.construction          AS construction,
                                                                                pc_policyperiod.protectionclasscode   AS protectionclass,
                                                                                pc_policyperiod.typeofdeductiblecode  AS typeofdeductible,
                                                                                pc_policyperiod.amountofinsurance     AS amountofinsurance,
                                                                                pc_policyperiod.yearofmanufacture     AS yearofmanufacutre,
                                                                                pc_policyperiod.ded_ind               AS ded_ind,
                                                                                pc_policyperiod.ded_amount            AS ded_amt,
                                                                                pc_policyperiod.deductibleindicatorws AS ded_wind_ind,
                                                                                pc_policyperiod.deductibleamountws    AS ded_wind_amt,
                                                                                pc_policyperiod.policyperiodid        AS policyperiodid,
                                                                                coverage            AS coverage,
                                                                                pc_policyperiod.policynumber_stg      AS policynumber
                                                                         FROM   (
                                                                                                SELECT DISTINCT coalesce(territorycode,''00'') territorycode ,
                                                                                                                coalesce(zipcode, ''00000'')   zipcode,
                                                                                                                itemcode,
                                                                                                                sublinecode,
                                                                                                                no_of_family_codes,
                                                                                                                construction,
                                                                                                                coalesce(protectionclasscode,''00'')      protectionclasscode,
                                                                                                                coalesce(typeofdeductiblecode, ''00'')    typeofdeductiblecode,
                                                                                                                coalesce(amountofinsurance, ''00000'')    amountofinsurance,
                                                                                                                coalesce(yearofmanufacture, ''0000'')     yearofmanufacture,
                                                                                                                coalesce(ded_ind, ''0'')                  ded_ind,
                                                                                                                coalesce(ded_amount,''0000000'')          ded_amount,
                                                                                                                coalesce(deductibleindicatorws, ''0'')    deductibleindicatorws,
                                                                                                                coalesce(deductibleamountws, ''0000000'') deductibleamountws,
                                                                                                                policynumber_stg,
                                                                                                                policyperiodid ,
                                                                                                                coverage
                                                                                                FROM            (
                                                                                                                                SELECT DISTINCT coalesce(terr.code,''00'')                    AS territorycode,
                                                                                                                                                coalesce( terr.postalcodeinternal , ''00000'')AS zipcode,
                                                                                                                                                CASE
                                                                                                                                                                WHEN earthquake =''HODW_Earthquake_HOE'' THEN ''01''
                                                                                                                                                                WHEN ph.typecode_stg =''HO4'' THEN ''02''
                                                                                                                                                                ELSE ''03''
                                                                                                                                                END AS itemcode,
                                                                                                                                                CASE
                                                                                                                                                                WHEN earthquake =''HODW_Earthquake_HOE'' THEN ''60''
                                                                                                                                                                WHEN replacement =''HODW_PersonalPropertyReplacementCost_alfa''
                                                                                                                                                                AND             ph.typecode_stg<>''HO8'' THEN ''03''
                                                                                                                                                                WHEN ph.typecode_stg=''HO8'' THEN ''02''
                                                                                                                                                                ELSE ''02''
                                                                                                                                                END AS sublinecode,
                                                                                                                                                CASE
                                                                                                                                                                WHEN ph.typecode_stg=''HO2'' THEN ''02''
                                                                                                                                                                WHEN ph.typecode_stg=''HO4'' THEN ''04''
                                                                                                                                                                WHEN ph.typecode_stg=''HO5'' THEN ''05''
                                                                                                                                                                WHEN ph.typecode_stg=''HO6'' THEN ''06''
                                                                                                                                                                WHEN ph.typecode_stg=''HO8'' THEN ''08''
                                                                                                                                                                ELSE ''03''
                                                                                                                                                END policy_form_code,
                                                                                                                                                CASE
                                                                                                                                                                WHEN ph.typecode_stg=''HO4'' THEN ''2''
                                                                                                                                                                WHEN pr.typecode_stg IN (''Apt'' ,
                                                                                                                                                                                         ''Condo'',
                                                                                                                                                                                         ''Coop'',
                                                                                                                                                                                         ''Duplex'',
                                                                                                                                                                                         ''Mobile'',
                                                                                                                                                                                         ''Modular_alfa'',
                                                                                                                                                                                         ''TownRow'',
                                                                                                                                                                                         ''Fam1'',
                                                                                                                                                                                         ''Fam2'',
                                                                                                                                                                                         ''Fam3'',
                                                                                                                                                                                         ''Fam3To4_alfa'',
                                                                                                                                                                                         ''Fam4'') THEN ''1''
                                                                                                                                                                ELSE ''2''
                                                                                                                                                END no_of_family_codes,
                                                                                                                                                CASE
                                                                                                                                                                WHEN pct.typecode_stg IN (''ADB'',
                                                                                                                                                                                          ''AOD'',
                                                                                                                                                                                          ''CLP'',
                                                                                                                                                                                          ''COM'',
                                                                                                                                                                                          ''CUS'',
                                                                                                                                                                                          ''DOM'',
                                                                                                                                                                                          ''F'',
                                                                                                                                                                                          ''FRM'',
                                                                                                                                                                                          ''FST'',
                                                                                                                                                                                          ''GLA'',
                                                                                                                                                                                          ''HEA'',
                                                                                                                                                                                          ''L'',
                                                                                                                                                                                          ''LOG'',
                                                                                                                                                                                          ''LIG'',
                                                                                                                                                                                          ''NON'',
                                                                                                                                                                                          ''OTH'',
                                                                                                                                                                                          ''OTHER'',
                                                                                                                                                                                          ''PFR'',
                                                                                                                                                                                          ''STU'',
                                                                                                                                                                                          ''STW'',
                                                                                                                                                                                          ''TLU'',
                                                                                                                                                                                          ''UNK'',
                                                                                                                                                                                          ''WOO'',
                                                                                                                                                                                          ''WSC'') THEN ''1''
                                                                                                                                                                WHEN pct.typecode_stg IN (''BCB'',
                                                                                                                                                                                          ''BLB'',
                                                                                                                                                                                          ''BRC'',
                                                                                                                                                                                          ''BRF'',
                                                                                                                                                                                          ''BRS'',
                                                                                                                                                                                          ''BST'',
                                                                                                                                                                                          ''FRY'',
                                                                                                                                                                                          ''SRO'',
                                                                                                                                                                                          ''STV'',
                                                                                                                                                                                          ''TBM'',
                                                                                                                                                                                          ''WBR'',
                                                                                                                                                                                          ''WCB'',
                                                                                                                                                                                          ''WSN'') THEN ''2''
                                                                                                                                                                WHEN pct.typecode_stg IN (''BRK'',
                                                                                                                                                                                          ''CCM'',
                                                                                                                                                                                          ''CNB'',
                                                                                                                                                                                          ''CND'',
                                                                                                                                                                                          ''CRE'',
                                                                                                                                                                                          ''FLX'',
                                                                                                                                                                                          ''M'',
                                                                                                                                                                                          ''MAS'',
                                                                                                                                                                                          ''MTU'',
                                                                                                                                                                                          ''TUC'',
                                                                                                                                                                                          ''SFM'') THEN ''3''
                                                                                                                                                                WHEN pct.typecode_stg IN (''BLM'',
                                                                                                                                                                                          ''BLS'',
                                                                                                                                                                                          ''BRL'',
                                                                                                                                                                                          ''BRM'',
                                                                                                                                                                                          ''CCS'',
                                                                                                                                                                                          ''MET'',
                                                                                                                                                                                          ''PRM'',
                                                                                                                                                                                          ''S'',
                                                                                                                                                                                          ''STE'',
                                                                                                                                                                                          ''STS'') THEN ''4''
                                                                                                                                                                WHEN pct.typecode_stg IN (''ALF'',
                                                                                                                                                                                          ''ALS'',
                                                                                                                                                                                          ''ALV'',
                                                                                                                                                                                          ''FRS'',
                                                                                                                                                                                          ''WMT'',
                                                                                                                                                                                          ''WSL'') THEN ''5''
                                                                                                                                                                WHEN pct.typecode_stg IN (''MAN'') THEN ''6''
                                                                                                                                                END construction,
                                                                                                                                                CASE
                                                                                                                                                                WHEN phh.dwellingprotectionclasscode_stg IS NULL
                                                                                                                                                                AND             jd.typecode_stg =''AL'' THEN ''05''
                                                                                                                                                                WHEN phh.dwellingprotectionclasscode_stg IS NULL
                                                                                                                                                                AND             jd.typecode_stg =''GA'' THEN ''03''
                                                                                                                                                                WHEN phh.dwellingprotectionclasscode_stg IS NULL
                                                                                                                                                                AND             jd.typecode_stg =''MS'' THEN ''10''
                                                                                                                                                                ELSE coalesce(phh.dwellingprotectionclasscode_stg, ''00'')
                                                                                                                                                END AS protectionclasscode,
                                                                                                                                                CASE
                                                                                                                                                                WHEN jd.typecode_stg=''AL'' THEN
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN (
                                                                                                                                                                                                        windhail =''HODW_WindHail_Ded_HOE'')
                                                                                                                                                                                                AND             deductibleamountws >0 THEN ''35''
                                                                                                                                                                                                WHEN (
                                                                                                                                                                                                        hurricane =''HODW_Hurricane_Ded_HOE''
                                                                                                                                                                                                        AND             terr.code IN (''05'',
                                                                                                                                                                                                        ''06'',
                                                                                                                                                                                                        ''30'',
                                                                                                                                                                                                        ''31''))
                                                                                                                                                                                                AND             deductibleamountws >0 THEN ''55''
                                                                                                                                                                                                WHEN (
                                                                                                                                                                                                        hurricane =''HODW_Hurricane_Ded_HOE''
                                                                                                                                                                                                        AND             terr.code NOT IN (''05'',
                                                                                                                                                                                                        ''06'',
                                                                                                                                                                                                        ''30'',
                                                                                                                                                                                                        ''31''))
                                                                                                                                                                                                AND             deductibleamountws >0 THEN ''35''
                                                                                                                                                                                                WHEN (
                                                                                                                                                                                                        windstormhailexcl =''HODW_SectionI_DedWindAndHailExcl_alfa'')
                                                                                                                                                                                                AND             (
                                                                                                                                                                                                        windstormhailexcl_amt <> 0
                                                                                                                                                                                                        AND             windstormhailexcl_amt IS NOT NULL) THEN ''07''
                                                                                                                                                                                                ELSE ''05''
                                                                                                                                                                                END
                                                                                                                                                                WHEN jd.typecode_stg IN (''GA'',
                                                                                                                                                                                         ''MS'') THEN
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN (
                                                                                                                                                                                                        (
                                                                                                                                                                                                        windhail =''HODW_WindHail_Ded_HOE'')
                                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        hurricane =''HODW_Hurricane_Ded_HOE'' ) )
                                                                                                                                                                                                AND             deductibleamountws >0 THEN ''05''
                                                                                                                                                                                                WHEN (
                                                                                                                                                                                                        windstormhailexcl =''HODW_SectionI_DedWindAndHailExcl_alfa'')
                                                                                                                                                                                                AND             (
                                                                                                                                                                                                        windstormhailexcl_amt <> 0
                                                                                                                                                                                                        AND             windstormhailexcl_amt IS NOT NULL) THEN ''07''
                                                                                                                                                                                                ELSE ''05''
                                                                                                                                                                                END
                                                                                                                                                END AS typeofdeductiblecode,
                                                                                                                                                coalesce((
                                                                                                                                                CASE
                                                                                                                                                                WHEN ph.typecode_stg IN (''HO4'' ,
                                                                                                                                                                                         ''HO6'') THEN pp_limit
                                                                                                                                                                ELSE dw_limit
                                                                                                                                                END),''00000'') AS amountofinsurance,
                                                                                                                                                coalesce(
                                                                                                                                                CASE
                                                                                                                                                                WHEN (
                                                                                                                                                                                                ph.typecode_stg IN (''HO2'',
                                                                                                                                                                                                        ''HO3'' ,
                                                                                                                                                                                                        ''HO5'',
                                                                                                                                                                                                        ''HO8''))
                                                                                                                                                                AND             (
                                                                                                                                                                                                pdh.yearbuilt_stg<=1959) THEN ''1959''
                                                                                                                                                                WHEN (
                                                                                                                                                                                                ph.typecode_stg IN (''HO2'',
                                                                                                                                                                                                        ''HO3'' ,
                                                                                                                                                                                                        ''HO5'',
                                                                                                                                                                                                        ''HO8''))
                                                                                                                                                                AND             (
                                                                                                                                                                                                pdh.yearbuilt_stg>1959) THEN pdh.yearbuilt_stg
                                                                                                                                                                WHEN (
                                                                                                                                                                                                ph.typecode_stg IN (''HO4'')) THEN ''0000''
                                                                                                                                                                WHEN ph.typecode_stg IN (''HO6'')
                                                                                                                                                                AND             lia_limit BETWEEN 1001.00 AND             9999.00 THEN ''0002''
                                                                                                                                                                WHEN ph.typecode_stg IN (''HO6'')
                                                                                                                                                                AND             lia_limit BETWEEN 10000.00 AND             19999.00 THEN ''0003''
                                                                                                                                                                WHEN ph.typecode_stg IN (''HO6'')
                                                                                                                                                                AND             lia_limit BETWEEN 20000.00 AND             29999.00 THEN ''0004''
                                                                                                                                                                WHEN ph.typecode_stg IN (''HO6'')
                                                                                                                                                                AND             lia_limit BETWEEN 30000.00 AND             39999.00 THEN ''0005''
                                                                                                                                                                WHEN ph.typecode_stg IN (''HO6'')
                                                                                                                                                                AND             lia_limit BETWEEN 40000.00 AND             49999.00 THEN ''0006''
                                                                                                                                                                WHEN ph.typecode_stg IN (''HO6'')
                                                                                                                                                                AND             lia_limit BETWEEN 50000.00 AND             59999.00 THEN ''0007''
                                                                                                                                                                WHEN ph.typecode_stg IN (''HO6'')
                                                                                                                                                                AND             lia_limit BETWEEN 60000.00 AND             69999.00 THEN ''0008''
                                                                                                                                                                WHEN ph.typecode_stg IN (''HO6'')
                                                                                                                                                                AND             lia_limit >70000.00 THEN ''0009''
                                                                                                                                                                ELSE ''0001''
                                                                                                                                                END ,''0000'')                     AS yearofmanufacture,
                                                                                                                                                coalesce(perils_limit_ind , ''0'')    ded_ind,
                                                                                                                                                coalesce(perils_limit,''0000000'')    ded_amount,
                                                                                                                                                ''3''                              AS tiedowncode,
                                                                                                                                                coalesce(
                                                                                                                                                CASE
                                                                                                                                                                WHEN jd.typecode_stg=''AL'' THEN deductiblews
                                                                                                                                                                ELSE ''0''
                                                                                                                                                END,''0'') AS deductibleindicatorws,
                                                                                                                                                coalesce(
                                                                                                                                                CASE
                                                                                                                                                                WHEN jd.typecode_stg=''AL'' THEN cast(cast(deductibleamountws AS INTEGER) AS VARCHAR(7) )
                                                                                                                                                                ELSE ''0''
                                                                                                                                                END ,''0000000'')AS deductibleamountws,
                                                                                                                                                pp.policynumber_stg,
                                                                                                                                                coalesce(cast(pp.id_stg AS VARCHAR(30)) , pp.policynumber_stg)                                policyperiodid ,
                                                                                                                                                row_number() over( PARTITION BY (pp.policynumber_stg) ORDER BY pp.editeffectivedate_stg DESC) row1 ,
                                                                                                                                                CASE
                                                                                                                                                                WHEN pcdh.name_stg= ''Secondary'' THEN ''07''
                                                                                                                                                                ELSE ''01''
                                                                                                                                                END coverage
                                                                                                                                FROM            db_t_prod_stag.pc_policyperiod pp
                                                                                                                                join            db_t_prod_stag.pctl_jurisdiction jd
                                                                                                                                ON              pp.basestate_stg=jd.id_stg
                                                                                                                                join            db_t_prod_stag.pcx_dwelling_hoe pdh
                                                                                                                                ON              pdh.branchid_stg=pp.id_stg
                                                                                                                                left join       db_t_prod_stag.pctl_dwellingusage_hoe pcdh
                                                                                                                                ON              pcdh.id_stg =pdh.dwellingusage_stg
                                                                                                                                join
                                                                                                                                                (
                                                                                                                                                          SELECT    branchid_stg                                                                                                             id,
                                                                                                                                                                    max(cast(coalesce(old_code,pht1.naiipcicode_alfa_stg, pht2.naiipcicode_alfa_stg, pht3.naiipcicode_alfa_stg) AS INTEGER) )code,
                                                                                                                                                                    max(postalcodeinternal)                                                                                                  postalcodeinternal
                                                                                                                                                          FROM      (
                                                                                                                                                                              SELECT    e.branchid_stg,
                                                                                                                                                                                        policynumber_stg,
                                                                                                                                                                                        c.code_stg,
                                                                                                                                                                                        g.typecode_stg,
                                                                                                                                                                                        c.countycode_alfa_stg,
                                                                                                                                                                                        pc_policyline.hopolicytype_stg,
                                                                                                                                                                                        coalesce(postalcodeinternal_stg, postalcode_stg)postalcodeinternal,
                                                                                                                                                                                        row_number() over( PARTITION BY e.branchid_stg,policynumber_stg, c.code_stg,g.typecode_stg, c.countycode_alfa_stg, pc_policyline.hopolicytype_stg ORDER BY
                                                                                                                                                                                        CASE
                                                                                                                                                                                                  WHEN (
                                                                                                                                                                                                        c.policylocation_stg = b.id_stg ) THEN 1
                                                                                                                                                                                                  ELSE 2
                                                                                                                                                                                        END) ROWNUM,
                                                                                                                                                                                        cityinternal_stg,
                                                                                                                                                                                        countyinternal_stg,
                                                                                                                                                                                        county_stg,
                                                                                                                                                                                        CASE
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       upper(cityinternal_stg)= ''BIRMINGHAM''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg))=''JEFFERSON'' THEN ''32''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       upper(cityinternal_stg)= ''HUNTSVILLE''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg))=''MADISON'' THEN ''35''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       upper(cityinternal_stg)= ''MONTGOMERY''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg))=''MONTGOMERY'' THEN ''37''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       upper(cityinternal_stg)= ''MOBILE''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg))=''MOBILE'' THEN ''30''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''AUTAUGA'',
                                                                                                                                                                                                        ''ELMORE'',
                                                                                                                                                                                                        ''MONTGOMERY'') THEN ''38''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg))=''BALDWIN''
                                                                                                                                                                                                  AND       cast(c.code_stg AS INTEGER)=26 THEN ''41''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg))=''BALDWIN''
                                                                                                                                                                                                  AND       cast(c.code_stg AS INTEGER)=28 THEN ''31''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg))=''BALDWIN''
                                                                                                                                                                                                  AND       (
                                                                                                                                                                                                        cast(c.code_stg AS INTEGER)=11
                                                                                                                                                                                                        OR        cast(c.code_stg AS INTEGER) IS NULL
                                                                                                                                                                                                        OR        cast(c.code_stg AS INTEGER) IN (1,2,3) ) THEN ''05''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''BARBOUR'',
                                                                                                                                                                                                        ''BIBB'',
                                                                                                                                                                                                        ''BLOUNT'',
                                                                                                                                                                                                        ''BULLOCK'',
                                                                                                                                                                                                        ''BUTLER'',
                                                                                                                                                                                                        ''CHAMBERS'',
                                                                                                                                                                                                        ''CHEROKEE'',
                                                                                                                                                                                                        ''CHILTON'',
                                                                                                                                                                                                        ''CHOCTAW'',
                                                                                                                                                                                                        ''CLARKE'',
                                                                                                                                                                                                        ''CLAY'',
                                                                                                                                                                                                        ''CLEBURNE'',
                                                                                                                                                                                                        ''COFFEE'',
                                                                                                                                                                                                        ''CONECUH'',
                                                                                                                                                                                                        ''COOSA'',
                                                                                                                                                                                                        ''COVINGTON'',
                                                                                                                                                                                                        ''CRENSHAW'',
                                                                                                                                                                                                        ''CULLMAN'',
                                                                                                                                                                                                        ''DALE'',
                                                                                                                                                                                                        ''DALLAS'',
                                                                                                                                                                                                        ''DE KALB'',
                                                                                                                                                                                                        ''DEKALB'',
                                                                                                                                                                                                        ''ESCAMBIA'',
                                                                                                                                                                                                        ''FAYETTE'',
                                                                                                                                                                                                        ''FRANKLIN'',
                                                                                                                                                                                                        ''GENEVA'',
                                                                                                                                                                                                        ''GREENE'',
                                                                                                                                                                                                        ''HALE'',
                                                                                                                                                                                                        ''HENRY'',
                                                                                                                                                                                                        ''HOUSTON'',
                                                                                                                                                                                                        ''JACKSON'',
                                                                                                                                                                                                        ''LAMAR'',
                                                                                                                                                                                                        ''LAWRENCE'',
                                                                                                                                                                                                        ''LEE'',
                                                                                                                                                                                                        ''LOWNDES'',
                                                                                                                                                                                                        ''MACON'',
                                                                                                                                                                                                        ''MONROE'',
                                                                                                                                                                                                        ''MARENGO'',
                                                                                                                                                                                                        ''MARION'',
                                                                                                                                                                                                        ''MARSHALL'',
                                                                                                                                                                                                        ''PERRY'',
                                                                                                                                                                                                        ''PICKENS'',
                                                                                                                                                                                                        ''PIKE'',
                                                                                                                                                                                                        ''RANDOLPH'',
                                                                                                                                                                                                        ''RUSSELL'',
                                                                                                                                                                                                        ''SAINT CLAIR'',
                                                                                                                                                                                                        ''ST. CLAIR'',
                                                                                                                                                                                                        ''SUMTER'',
                                                                                                                                                                                                        ''TALLADEGA'',
                                                                                                                                                                                                        ''TALLAPOOSA'',
                                                                                                                                                                                                        ''WASHINGTON'',
                                                                                                                                                                                                        ''WILCOX'',
                                                                                                                                                                                                        ''WINSTON'') THEN ''41''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''CALHOUN'',
                                                                                                                                                                                                        ''ETOWAH'') THEN ''40''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''COLBERT'',
                                                                                                                                                                                                        ''LAUDERDALE'',
                                                                                                                                                                                                        ''LIMESTONE'',
                                                                                                                                                                                                        ''MADISON'',
                                                                                                                                                                                                        ''MORGAN'') THEN ''36''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg))=''JEFFERSON'' THEN ''33''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg))=''MOBILE''
                                                                                                                                                                                                  AND       (
                                                                                                                                                                                                        cast(c.code_stg AS INTEGER)IN (2,1,26,3 )
                                                                                                                                                                                                        OR        cast(c.code_stg AS INTEGER)IS NULL) THEN ''41''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg))=''MOBILE''
                                                                                                                                                                                                  AND       cast(c.code_stg AS INTEGER)=28 THEN ''31''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg))=''MOBILE''
                                                                                                                                                                                                  AND       cast(c.code_stg AS INTEGER)=11 THEN ''05''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''SHELBY'',
                                                                                                                                                                                                        ''WALKER'') THEN ''34''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg))=''TUSCALOOSA'' THEN ''39''
                                                                                                                                                                                                  /*WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       (
                                                                                                                                                                                                        substring (b.postalcodeinternal_stg,0,INDEX(''-'',b.postalcodeinternal_stg))=''36511''
                                                                                                                                                                                                        OR        b.postalcodeinternal_stg=''36511'')
                                                                                                                                                                                                  AND       upper(cityinternal_stg)= ''BON SECOUR'' THEN ''06''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       (
                                                                                                                                                                                                        substring (b.postalcodeinternal_stg,0,INDEX(''-'',b.postalcodeinternal_stg))=''36528''
                                                                                                                                                                                                        OR        b.postalcodeinternal_stg=''36528'')
                                                                                                                                                                                                  AND       upper(cityinternal_stg)= ''DAUPHIN ISLAND'' THEN ''06''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       (
                                                                                                                                                                                                        substring (b.postalcodeinternal_stg,0,INDEX(''-'',b.postalcodeinternal_stg)) IN (''36542'',
                                                                                                                                                                                                        ''36547'')
                                                                                                                                                                                                        OR        b.postalcodeinternal_stg IN (''36542'',
                                                                                                                                                                                                        ''36547''))
                                                                                                                                                                                                  AND       upper(cityinternal_stg)= ''GULF SHORES'' THEN ''06''
                                                                                                                                                                                                  WHEN g.typecode_stg=''AL''
                                                                                                                                                                                                  AND       (
                                                                                                                                                                                                        substring (b.postalcodeinternal_stg,0,INDEX(''-'',b.postalcodeinternal_stg))=''36561''
                                                                                                                                                                                                        OR        b.postalcodeinternal_stg=''36561'')
                                                                                                                                                                                                  AND       upper(cityinternal_stg)= ''ORANGE BEACH'' THEN ''06'' */


                                                                                                                                                                                                    WHEN g.typecode_stg = ''AL''
                                                                                                                                                                                                           AND (
                                                                                                                                                                                                           IFF(POSITION(''-'' IN b.postalcodeinternal_stg) > 0,
                                                                                                                                                                                                                  SUBSTRING(b.postalcodeinternal_stg, 1, POSITION(''-'' IN b.postalcodeinternal_stg) - 1),
                                                                                                                                                                                                                  b.postalcodeinternal_stg
                                                                                                                                                                                                           ) = ''36511''
                                                                                                                                                                                                           OR b.postalcodeinternal_stg = ''36511''
                                                                                                                                                                                                           )
                                                                                                                                                                                                           AND UPPER(cityinternal_stg) = ''BON SECOUR''
                                                                                                                                                                                                           THEN ''06''

                                                                                                                                                                                                    WHEN g.typecode_stg = ''AL''
                                                                                                                                                                                                           AND (
                                                                                                                                                                                                           IFF(POSITION(''-'' IN b.postalcodeinternal_stg) > 0,
                                                                                                                                                                                                                  SUBSTRING(b.postalcodeinternal_stg, 1, POSITION(''-'' IN b.postalcodeinternal_stg) - 1),
                                                                                                                                                                                                                  b.postalcodeinternal_stg
                                                                                                                                                                                                           ) = ''36528''
                                                                                                                                                                                                           OR b.postalcodeinternal_stg = ''36528''
                                                                                                                                                                                                           )
                                                                                                                                                                                                           AND UPPER(cityinternal_stg) = ''DAUPHIN ISLAND''
                                                                                                                                                                                                           THEN ''06''

                                                                                                                                                                                                    WHEN g.typecode_stg = ''AL''
                                                                                                                                                                                                           AND (
                                                                                                                                                                                                           IFF(POSITION(''-'' IN b.postalcodeinternal_stg) > 0,
                                                                                                                                                                                                                  SUBSTRING(b.postalcodeinternal_stg, 1, POSITION(''-'' IN b.postalcodeinternal_stg) - 1),
                                                                                                                                                                                                                  b.postalcodeinternal_stg
                                                                                                                                                                                                           ) IN (''36542'', ''36547'')
                                                                                                                                                                                                           OR b.postalcodeinternal_stg IN (''36542'', ''36547'')
                                                                                                                                                                                                           )
                                                                                                                                                                                                           AND UPPER(cityinternal_stg) = ''GULF SHORES''
                                                                                                                                                                                                           THEN ''06''

                                                                                                                                                                                                    WHEN g.typecode_stg = ''AL''
                                                                                                                                                                                                           AND (
                                                                                                                                                                                                           IFF(POSITION(''-'' IN b.postalcodeinternal_stg) > 0,
                                                                                                                                                                                                                  SUBSTRING(b.postalcodeinternal_stg, 1, POSITION(''-'' IN b.postalcodeinternal_stg) - 1),
                                                                                                                                                                                                                  b.postalcodeinternal_stg
                                                                                                                                                                                                           ) = ''36561''
                                                                                                                                                                                                           OR b.postalcodeinternal_stg = ''36561''
                                                                                                                                                                                                           )
                                                                                                                                                                                                           AND UPPER(cityinternal_stg) = ''ORANGE BEACH''
                                                                                                                                                                                                           THEN ''06''

                                                                                                                                                                                                  WHEN g.typecode_stg=''MS''
                                                                                                                                                                                                  AND       upper(cityinternal_stg)= ''JACKSON''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''HINDS'',
                                                                                                                                                                                                        ''RANKIN'') THEN ''30''
                                                                                                                                                                                                  WHEN g.typecode_stg=''MS''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''AMITE'',
                                                                                                                                                                                                        ''FORREST'',
                                                                                                                                                                                                        ''GREENE'',
                                                                                                                                                                                                        ''LAMAR'',
                                                                                                                                                                                                        ''MARION'',
                                                                                                                                                                                                        ''PERRY'',
                                                                                                                                                                                                        ''PIKE'',
                                                                                                                                                                                                        ''WALTHALL'',
                                                                                                                                                                                                        ''WILKINSON'') THEN ''03''
                                                                                                                                                                                                  WHEN g.typecode_stg=''MS''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''GEORGE'',
                                                                                                                                                                                                        ''PEARL RIVER'',
                                                                                                                                                                                                        ''STONE'') THEN ''05''
                                                                                                                                                                                                  WHEN g.typecode_stg=''MS''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''HANCOCK'',
                                                                                                                                                                                                        ''HARRISON'',
                                                                                                                                                                                                        ''JACKSON'') THEN ''06''
                                                                                                                                                                                                  WHEN g.typecode_stg=''MS''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''HINDS'',
                                                                                                                                                                                                        ''MADISON'',
                                                                                                                                                                                                        ''RANKIN'') THEN ''31''
                                                                                                                                                                                                  WHEN g.typecode_stg=''MS''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''ADAMS'',
                                                                                                                                                                                                        ''ALCORN'',
                                                                                                                                                                                                        ''ATTALA'',
                                                                                                                                                                                                        ''BENTON'',
                                                                                                                                                                                                        ''BOLIVAR'',
                                                                                                                                                                                                        ''CALHOUN'',
                                                                                                                                                                                                        ''CARROLL'',
                                                                                                                                                                                                        ''CHICKASAW'',
                                                                                                                                                                                                        ''CHOCTAW'',
                                                                                                                                                                                                        ''CLAIBORNE'',
                                                                                                                                                                                                        ''CLARKE'',
                                                                                                                                                                                                        ''CLAY'',
                                                                                                                                                                                                        ''COAHOMA'',
                                                                                                                                                                                                        ''COPIAH'',
                                                                                                                                                                                                        ''COVINGTON'',
                                                                                                                                                                                                        ''DESOTO'',
                                                                                                                                                                                                        ''FRANKLIN'',
                                                                                                                                                                                                        ''GRENADA'',
                                                                                                                                                                                                        ''HOLMES'',
                                                                                                                                                                                                        ''HUMPHREYS'',
                                                                                                                                                                                                        ''ISSAQUENA'',
                                                                                                                                                                                                        ''ITAWAMBA'',
                                                                                                                                                                                                        ''JASPER'',
                                                                                                                                                                                                        ''JEFFERSON'',
                                                                                                                                                                                                        ''JEFFERSON DAVIS'',
                                                                                                                                                                                                        ''JONES'',
                                                                                                                                                                                                        ''KEMPER'',
                                                                                                                                                                                                        ''LAFAYETTE'',
                                                                                                                                                                                                        ''LAUDERDALE'',
                                                                                                                                                                                                        ''LAWRENCE'',
                                                                                                                                                                                                        ''LEAKE'',
                                                                                                                                                                                                        ''LEE'',
                                                                                                                                                                                                        ''LEFLORE'',
                                                                                                                                                                                                        ''LINCOLN'',
                                                                                                                                                                                                        ''LOWNDES'',
                                                                                                                                                                                                        ''MARSHALL'',
                                                                                                                                                                                                        ''MONROE'',
                                                                                                                                                                                                        ''MONTGOMERY'',
                                                                                                                                                                                                        ''NESHOBA'',
                                                                                                                                                                                                        ''NEWTON'',
                                                                                                                                                                                                        ''NOXUBEE'',
                                                                                                                                                                                                        ''OKTIBBEHA'',
                                                                                                                                                                                                        ''PANOLA'',
                                                                                                                                                                                                        ''PONTOTOC'',
                                                                                                                                                                                                        ''PRENTISS'',
                                                                                                                                                                                                        ''QUITMAN'',
                                                                                                                                                                                                        ''SCOTT'',
                                                                                                                                                                                                        ''SHARKEY'',
                                                                                                                                                                                                        ''SIMPSON'',
                                                                                                                                                                                                        ''SMITH'',
                                                                                                                                                                                                        ''SUNFLOWER'',
                                                                                                                                                                                                        ''TALLAHATCHIE'',
                                                                                                                                                                                                        ''TATE'',
                                                                                                                                                                                                        ''TIPPAH'',
                                                                                                                                                                                                        ''TISHOMINGO'',
                                                                                                                                                                                                        ''TUNICA'',
                                                                                                                                                                                                        ''UNION'',
                                                                                                                                                                                                        ''WARREN'',
                                                                                                                                                                                                        ''WASHINGTON'',
                                                                                                                                                                                                        ''WAYNE'',
                                                                                                                                                                                                        ''WEBSTER'',
                                                                                                                                                                                                        ''WINSTON'',
                                                                                                                                                                                                        ''YALOBUSHA'',
                                                                                                                                                                                                        ''YAZOO'') THEN ''32''
                                                                                                                                                                                                  WHEN g.typecode_stg=''GA''
                                                                                                                                                                                                  AND       upper(cityinternal_stg)= ''ATLANTA''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''DEKALB'',
                                                                                                                                                                                                        ''DE KALB'',
                                                                                                                                                                                                        ''FULTON'') THEN ''32''
                                                                                                                                                                                                  WHEN g.typecode_stg=''GA''
                                                                                                                                                                                                  AND       upper(cityinternal_stg)= ''MACON''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg))=''BIBB'' THEN ''35''
                                                                                                                                                                                                  WHEN g.typecode_stg=''GA''
                                                                                                                                                                                                  AND       upper(cityinternal_stg)= ''SAVANNAH''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg))=''CHATHAM'' THEN ''30''
                                                                                                                                                                                                  WHEN g.typecode_stg=''GA''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''DEKALB'',
                                                                                                                                                                                                        ''DE KALB'',
                                                                                                                                                                                                        ''DEKALB'',
                                                                                                                                                                                                        ''DE KALB'',
                                                                                                                                                                                                        ''FULTON'') THEN ''33''
                                                                                                                                                                                                  WHEN g.typecode_stg=''GA''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''BRYAN'',
                                                                                                                                                                                                        ''CAMDEN'',
                                                                                                                                                                                                        ''CHATHAM'',
                                                                                                                                                                                                        ''GLYNN'',
                                                                                                                                                                                                        ''LIBERTY'',
                                                                                                                                                                                                        ''MCINTOSH'') THEN ''31''
                                                                                                                                                                                                  WHEN g.typecode_stg=''GA''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''DE KALB'',
                                                                                                                                                                                                        ''DEKALB'',
                                                                                                                                                                                                        ''FULTON'') THEN ''33''
                                                                                                                                                                                                  WHEN g.typecode_stg=''GA''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''CLAYTON'',
                                                                                                                                                                                                        ''COBB'',
                                                                                                                                                                                                        ''GWINNETT'') THEN ''34''
                                                                                                                                                                                                  WHEN g.typecode_stg=''GA''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''CATOOSA'',
                                                                                                                                                                                                        ''WALKER'',
                                                                                                                                                                                                        ''WHITFIELD'') THEN ''36''
                                                                                                                                                                                                  WHEN g.typecode_stg=''GA''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) =''RICHMOND'' THEN ''37''
                                                                                                                                                                                                  WHEN g.typecode_stg=''GA''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''CHATTAHOOCHEE'',
                                                                                                                                                                                                        ''MUSCOGEE'') THEN ''38''
                                                                                                                                                                                                  WHEN g.typecode_stg=''GA''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''BUTTS'',
                                                                                                                                                                                                        ''CHEROKEE'',
                                                                                                                                                                                                        ''DOUGLAS'',
                                                                                                                                                                                                        ''FAYETTE'',
                                                                                                                                                                                                        ''FORSYTH'',
                                                                                                                                                                                                        ''HENRY'',
                                                                                                                                                                                                        ''NEWTON'',
                                                                                                                                                                                                        ''PAULDING'',
                                                                                                                                                                                                        ''ROCKDALE'',
                                                                                                                                                                                                        ''WALTON'') THEN ''39''
                                                                                                                                                                                                  WHEN g.typecode_stg=''GA''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''BALDWIN'',
                                                                                                                                                                                                        ''BANKS'',
                                                                                                                                                                                                        ''BARROW'',
                                                                                                                                                                                                        ''BARTOW'',
                                                                                                                                                                                                        ''CARROLL'',
                                                                                                                                                                                                        ''CHATTOOGA'',
                                                                                                                                                                                                        ''CLARKE'',
                                                                                                                                                                                                        ''COLUMBIA'',
                                                                                                                                                                                                        ''COWETA'',
                                                                                                                                                                                                        ''DADE'',
                                                                                                                                                                                                        ''DAWSON'',
                                                                                                                                                                                                        ''ELBERT'',
                                                                                                                                                                                                        ''FANNIN'',
                                                                                                                                                                                                        ''FLOYD'',
                                                                                                                                                                                                        ''FRANKLIN'',
                                                                                                                                                                                                        ''GILMER'',
                                                                                                                                                                                                        ''GORDON'',
                                                                                                                                                                                                        ''GREENE'',
                                                                                                                                                                                                        ''HABERSHAM'',
                                                                                                                                                                                                        ''HALL'',
                                                                                                                                                                                                        ''HANCOCK'',
                                                                                                                                                                                                        ''HARALSON'',
                                                                                                                                                                                                        ''HART'',
                                                                                                                                                                                                        ''HEARD'',
                                                                                                                                                                                                        ''JACKSON'',
                                                                                                                                                                                                        ''JASPER'',
                                                                                                                                                                                                        ''JONES'',
                                                                                                                                                                                                        ''LAMAR'',
                                                                                                                                                                                                        ''LINCOLN'',
                                                                                                                                                                                                        ''LUMPKIN'',
                                                                                                                                                                                                        ''MADISON'',
                                                                                                                                                                                                        ''MCDUFFIE'',
                                                                                                                                                                                                        ''MERIWETHER'',
                                                                                                                                                                                                        ''MONROE'',
                                                                                                                                                                                                        ''MORGAN'',
                                                                                                                                                                                                        ''MURRAY'',
                                                                                                                                                                                                        ''OCONEE'',
                                                                                                                                                                                                        ''OGLETHORPE'',
                                                                                                                                                                                                        ''PICKENS'',
                                                                                                                                                                                                        ''PIKE'',
                                                                                                                                                                                                        ''POLK'',
                                                                                                                                                                                                        ''PUTNAM'',
                                                                                                                                                                                                        ''RABUN'',
                                                                                                                                                                                                        ''SPALDING'',
                                                                                                                                                                                                        ''STEPHENS'',
                                                                                                                                                                                                        ''TALIAFERRO'',
                                                                                                                                                                                                        ''TOWNS'',
                                                                                                                                                                                                        ''TROUP'',
                                                                                                                                                                                                        ''UNION'',
                                                                                                                                                                                                        ''WARREN'',
                                                                                                                                                                                                        ''WHITE'',
                                                                                                                                                                                                        ''WILKES'') THEN ''40''
                                                                                                                                                                                                  WHEN g.typecode_stg=''GA''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''BAKER'',
                                                                                                                                                                                                        ''BIBB'',
                                                                                                                                                                                                        ''BROOKS'',
                                                                                                                                                                                                        ''CALHOUN'',
                                                                                                                                                                                                        ''CLAY'',
                                                                                                                                                                                                        ''COLQUITT'',
                                                                                                                                                                                                        ''CRAWFORD'',
                                                                                                                                                                                                        ''CRISP'',
                                                                                                                                                                                                        ''DECATUR'',
                                                                                                                                                                                                        ''DOOLY'',
                                                                                                                                                                                                        ''DOUGHERTY'',
                                                                                                                                                                                                        ''EARLY'',
                                                                                                                                                                                                        ''GRADY'',
                                                                                                                                                                                                        ''HARRIS'',
                                                                                                                                                                                                        ''HOUSTON'',
                                                                                                                                                                                                        ''LEE'',
                                                                                                                                                                                                        ''MACON'',
                                                                                                                                                                                                        ''MARION'',
                                                                                                                                                                                                        ''MILLER'',
                                                                                                                                                                                                        ''MITCHELL'',
                                                                                                                                                                                                        ''PEACH'',
                                                                                                                                                                                                        ''QUITMAN'',
                                                                                                                                                                                                        ''RANDOLPH'',
                                                                                                                                                                                                        ''SCHLEY'',
                                                                                                                                                                                                        ''SEMINOLE'',
                                                                                                                                                                                                        ''STEWART'',
                                                                                                                                                                                                        ''SUMTER'',
                                                                                                                                                                                                        ''TALBOT'',
                                                                                                                                                                                                        ''TAYLOR'',
                                                                                                                                                                                                        ''TERRELL'',
                                                                                                                                                                                                        ''THOMAS'',
                                                                                                                                                                                                        ''TIFT'',
                                                                                                                                                                                                        ''TURNER'',
                                                                                                                                                                                                        ''UPSON'',
                                                                                                                                                                                                        ''WEBSTER'',
                                                                                                                                                                                                        ''WORTH'') THEN ''41''
                                                                                                                                                                                                  WHEN g.typecode_stg=''GA''
                                                                                                                                                                                                  AND       upper(coalesce(b.countyinternal_stg,county_stg)) IN (''APPLING'',
                                                                                                                                                                                                        ''ATKINSON'',
                                                                                                                                                                                                        ''BACON'',
                                                                                                                                                                                                        ''BEN HILL'',
                                                                                                                                                                                                        ''BERRIEN'',
                                                                                                                                                                                                        ''BLECKLEY'',
                                                                                                                                                                                                        ''BRANTLEY'',
                                                                                                                                                                                                        ''BULLOCH'',
                                                                                                                                                                                                        ''BURKE'',
                                                                                                                                                                                                        ''CANDLER'',
                                                                                                                                                                                                        ''CHARLTON'',
                                                                                                                                                                                                        ''CLINCH'',
                                                                                                                                                                                                        ''COFFEE'',
                                                                                                                                                                                                        ''COOK'',
                                                                                                                                                                                                        ''DODGE'',
                                                                                                                                                                                                        ''ECHOLS'',
                                                                                                                                                                                                        ''EFFINGHAM'',
                                                                                                                                                                                                        ''EMANUEL'',
                                                                                                                                                                                                        ''EVANS'',
                                                                                                                                                                                                        ''GLASCOCK'',
                                                                                                                                                                                                        ''IRWIN'',
                                                                                                                                                                                                        ''JEFF DAVIS'',
                                                                                                                                                                                                        ''JEFFERSON'',
                                                                                                                                                                                                        ''JENKINS'',
                                                                                                                                                                                                        ''JOHNSON'',
                                                                                                                                                                                                        ''LANIER'',
                                                                                                                                                                                                        ''LAURENS'',
                                                                                                                                                                                                        ''LONG'',
                                                                                                                                                                                                        ''LOWNDES'',
                                                                                                                                                                                                        ''MONTGOMERY'',
                                                                                                                                                                                                        ''PIERCE'',
                                                                                                                                                                                                        ''PULASKI'',
                                                                                                                                                                                                        ''SCREVEN'',
                                                                                                                                                                                                        ''TATTNALL'',
                                                                                                                                                                                                        ''TELFAIR'',
                                                                                                                                                                                                        ''TOOMBS'',
                                                                                                                                                                                                        ''TREUTLEN'',
                                                                                                                                                                                                        ''TWIGGS'',
                                                                                                                                                                                                        ''WARE'',
                                                                                                                                                                                                        ''WASHINGTON'',
                                                                                                                                                                                                        ''WAYNE'',
                                                                                                                                                                                                        ''WHEELER'',
                                                                                                                                                                                                        ''WILCOX'',
                                                                                                                                                                                                        ''WILKINSON'') THEN ''42''
                                                                                                                                                                                        END AS old_code
                                                                                                                                                                              FROM      db_t_prod_stag.pcx_holocation_hoe a
                                                                                                                                                                              join      db_t_prod_stag.pcx_dwelling_hoe e
                                                                                                                                                                              ON        e.holocation_stg=a.id_stg
                                                                                                                                                                              join      db_t_prod_stag.pc_policyperiod f
                                                                                                                                                                              ON        e.branchid_stg =f.id_stg
                                                                                                                                                                              left join db_t_prod_stag.pc_effectivedatedfields eff
                                                                                                                                                                              ON        eff.branchid_stg = f.id_stg
                                                                                                                                                                              AND       eff.expirationdate_stg IS NULL
                                                                                                                                                                              left join db_t_prod_stag.pc_policylocation b
                                                                                                                                                                              ON        b.id_stg = eff.primarylocation_stg
                                                                                                                                                                              AND       b.expirationdate_stg IS NULL
                                                                                                                                                                              join      db_t_prod_stag.pc_territorycode c
                                                                                                                                                                              ON        c.branchid_stg = f.id_stg
                                                                                                                                                                              join      db_t_prod_stag.pctl_territorycode d
                                                                                                                                                                              ON        c.subtype_stg=d.id_stg
                                                                                                                                                                              AND       d.typecode_stg = ''HOTerritoryCode_alfa''
                                                                                                                                                                              left join db_t_prod_stag.pc_contact pc
                                                                                                                                                                              ON        pc.id_stg =pnicontactdenorm_stg
                                                                                                                                                                              left join db_t_prod_stag.pc_address
                                                                                                                                                                              ON        pc.primaryaddressid_stg = pc_address.id_stg
                                                                                                                                                                              join      db_t_prod_stag.pc_policyline
                                                                                                                                                                              ON        f.id_stg = pc_policyline.branchid_stg
                                                                                                                                                                              AND       pc_policyline.expirationdate_stg IS NULL
                                                                                                                                                                              join      db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                                                                                              ON        pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                                                                                              AND       pctl_hopolicytype_hoe.typecode_stg LIKE ''HO%''
                                                                                                                                                                              join      db_t_prod_stag.pctl_jurisdiction g
                                                                                                                                                                              ON        basestate_stg=g.id_stg
                                                                                                                                                                              AND       g.typecode_stg IN (''AL'',
                                                                                                                                                                                                        ''GA'',
                                                                                                                                                                                                        ''MS'')) loc
                                                                                                                                                          left join db_t_prod_stag.pcx_hodbterritory_alfa pht1
                                                                                                                                                          ON        pht1.code_stg =loc.code_stg
                                                                                                                                                          AND       cast(pht1.countycode_alfa_stg AS VARCHAR(50))=cast(loc.countycode_alfa_stg AS VARCHAR(50))
                                                                                                                                                          AND       substring(pht1.publicid_stg,7,2) =loc.typecode_stg
                                                                                                                                                          AND       pht1.hopolicytype_hoe_stg=loc.hopolicytype_stg
                                                                                                                                                          left join
                                                                                                                                                                    (
                                                                                                                                                                                    SELECT DISTINCT naiipcicode_alfa_stg,
                                                                                                                                                                                                    substring(publicid_stg,7,2) state,
                                                                                                                                                                                                    code_stg                    territory_code,
                                                                                                                                                                                                    hopolicytype_hoe_stg,
                                                                                                                                                                                                    rank() over( PARTITION BY naiipcicode_alfa_stg, substring(publicid_stg,7,2) , code_stg , hopolicytype_hoe_stg ORDER BY naiipcicode_alfa_stg,countycode_alfa_stg )row1
                                                                                                                                                                                    FROM            db_t_prod_stag.pcx_hodbterritory_alfa
                                                                                                                                                                                    WHERE           publicid_stg LIKE ''%HO%'') pht2
                                                                                                                                                          ON        pht2.row1=1
                                                                                                                                                          AND       pht2.territory_code =loc.code_stg
                                                                                                                                                          AND       pht2.state =loc.typecode_stg
                                                                                                                                                          AND       pht2.hopolicytype_hoe_stg=loc.hopolicytype_stg
                                                                                                                                                          left join
                                                                                                                                                                    (
                                                                                                                                                                                    SELECT DISTINCT naiipcicode_alfa_stg,
                                                                                                                                                                                                    substring(publicid_stg,7,2) state,
                                                                                                                                                                                                    countycode_alfa_stg         countycode_alfa,
                                                                                                                                                                                                    hopolicytype_hoe_stg,
                                                                                                                                                                                                    rank() over( PARTITION BY naiipcicode_alfa_stg, substring(publicid_stg,7,2) , countycode_alfa_stg , hopolicytype_hoe_stg ORDER BY naiipcicode_alfa_stg,code_stg )row1
                                                                                                                                                                                    FROM            db_t_prod_stag.pcx_hodbterritory_alfa
                                                                                                                                                                                    WHERE           publicid_stg LIKE ''%HO%'')pht3
                                                                                                                                                          ON        pht3.row1=1
                                                                                                                                                          AND       cast(pht3.countycode_alfa AS VARCHAR(50))=cast(loc.countycode_alfa_stg AS VARCHAR(50))
                                                                                                                                                          AND       pht3.state =loc.typecode_stg
                                                                                                                                                          AND       pht3.hopolicytype_hoe_stg=loc.hopolicytype_stg
                                                                                                                                                          WHERE     ROWNUM=1
                                                                                                                                                          GROUP BY  branchid_stg ) terr
                                                                                                                                ON              terr.id=pp.id_stg
                                                                                                                                join            db_t_prod_stag.pc_job pj
                                                                                                                                ON              pp.jobid_stg = pj.id_stg
                                                                                                                                join            db_t_prod_stag.pctl_job pcj
                                                                                                                                ON              pj.subtype_stg = pcj.id_stg
                                                                                                                                join            db_t_prod_stag.pcx_holocation_hoe phh
                                                                                                                                ON              pdh.holocation_stg= phh.id_stg
                                                                                                                                join            db_t_prod_stag.pctl_hopolicytype_hoe ph
                                                                                                                                ON              ph.id_stg=pdh.hopolicytype_stg
                                                                                                                                AND             ph.typecode_stg LIKE ''%HO%''
                                                                                                                                join
                                                                                                                                                (
                                                                                                                                                                SELECT DISTINCT branchid_stg,
                                                                                                                                                                                max(
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN covterm.covtermpatternid=''HODW_Dwelling_Limit_HOE'' THEN (lpad(cast(cast(round(cast(polcov.val AS DECIMAL(18,4)), 0)AS INTEGER) AS VARCHAR(10)) ,5, ''0''))
                                                                                                                                                                                END )dw_limit,
                                                                                                                                                                                max(
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN covterm.covtermpatternid=''HODW_DwellingAdditionalLimit_alfa'' THEN cast(polcov.val AS DECIMAL(18,4))
                                                                                                                                                                                END )lia_limit,
                                                                                                                                                                                max(
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN covterm.covtermpatternid=''HODW_PersonalPropertyLimit_alfa'' THEN
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN length(polcov.val)>12 THEN 0000
                                                                                                                                                                                                        ELSE substring(''00000'',1,(5-length( cast(cast(round(cast(polcov.val AS DECIMAL(18,4))/1000,0)AS INTEGER) AS VARCHAR(10)))))
                                                                                                                                                                                                        || cast(cast(round(cast(polcov.val AS DECIMAL(18,4))/1000, 0)AS INTEGER) AS VARCHAR(10))
                                                                                                                                                                                                        END
                                                                                                                                                                                END )pp_limit,
                                                                                                                                                                                max(
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN covterm.covtermpatternid=''HODW_OtherPerils_Ded_HOE'' THEN (
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN substring(name1 ,length(name1),1)=''%'' THEN ''F''
                                                                                                                                                                                                        ELSE ''D''
                                                                                                                                                                                                        END)
                                                                                                                                                                                END)perils_limit_ind,
                                                                                                                                                                                max(
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN covterm.covtermpatternid=''HODW_OtherPerils_Ded_HOE'' THEN (
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN substring(name1 ,length(name1),1)=''%'' THEN substring(''0000000'',1, (7-length(cast(cast(cast(cast(value1 AS DECIMAL(18,4)) *10000 AS DECIMAL(18,4)) AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                                                                        ||cast(cast(cast(cast(value1 AS DECIMAL(18,4)) *10000 AS DECIMAL(18,4)) AS INTEGER) AS VARCHAR(10))
                                                                                                                                                                                                        ELSE substring(''0000000'',1,(7-length(cast(cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                                                                        ||cast(cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER) AS VARCHAR(10))
                                                                                                                                                                                                        END)
                                                                                                                                                                                END)perils_limit,
                                                                                                                                                                                max(
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN patterncode =''HODW_Earthquake_HOE'' THEN patterncode
                                                                                                                                                                                END) earthquake,
                                                                                                                                                                                max(
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN patterncode =''HODW_PersonalPropertyReplacementCost_alfa'' THEN patterncode
                                                                                                                                                                                END) replacement,
                                                                                                                                                                                max(
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN (
                                                                                                                                                                                                        covtermpatternid =''HODW_WindHail_Ded_HOE'') THEN covtermpatternid
                                                                                                                                                                                END )windhail,
                                                                                                                                                                                max(
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN (
                                                                                                                                                                                                        covtermpatternid =''HODW_Hurricane_Ded_HOE'' ) THEN covtermpatternid
                                                                                                                                                                                END )hurricane,
                                                                                                                                                                                max(
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN (
                                                                                                                                                                                                        covtermpatternid =''HODW_SectionI_DedWindAndHailExcl_alfa'') THEN covtermpatternid
                                                                                                                                                                                END )windstormhailexcl,
                                                                                                                                                                                max(
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN (
                                                                                                                                                                                                        covtermpatternid =''HODW_SectionI_DedWindAndHailExcl_alfa'') THEN coalesce( polcov.val, value1)
                                                                                                                                                                                END )windstormhailexcl_amt,
                                                                                                                                                                                max(
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN covtermpatternid IN (''HODW_WindHail_Ded_HOE'',
                                                                                                                                                                                                        ''HODW_Hurricane_Ded_HOE'',
                                                                                                                                                                                                        ''HODW_SectionI_DedWindstorm_alfa'')
                                                                                                                                                                                                AND             polcov.columnname LIKE ''%direct%'' THEN polcov.val
                                                                                                                                                                                                WHEN covtermpatternid IN (''HODW_WindHail_Ded_HOE'',
                                                                                                                                                                                                        ''HODW_Hurricane_Ded_HOE'',
                                                                                                                                                                                                        ''HODW_SectionI_DedWindstorm_alfa'')
                                                                                                                                                                                                AND             polcov.columnname NOT LIKE ''%direct%'' THEN (
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN (
                                                                                                                                                                                                        cast(value1 AS DECIMAL(18,4))<=1.0000 ) THEN substring(''0000000'',1, (7-length(cast(cast(cast(value1 AS DECIMAL(18,4))*10000 AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                                                                        ||cast(cast(cast(value1 AS DECIMAL(18,4))                             *10000 AS INTEGER) AS VARCHAR(10))
                                                                                                                                                                                                        WHEN value1 IS NULL
                                                                                                                                                                                                        OR              value1 =0 THEN 0
                                                                                                                                                                                                        ELSE substring(''0000000'',1,(7                                             -length(cast(cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                                                                        ||cast(cast(cast(value1 AS DECIMAL(18,4)) *10000 AS INTEGER) AS VARCHAR(10))
                                                                                                                                                                                                        END )
                                                                                                                                                                                END ) AS deductibleamountws,
                                                                                                                                                                                max(
                                                                                                                                                                                CASE
                                                                                                                                                                                                WHEN covtermpatternid IN (''HODW_WindHail_Ded_HOE'',
                                                                                                                                                                                                        ''HODW_Hurricane_Ded_HOE'',
                                                                                                                                                                                                        ''HODW_SectionI_DedWindstorm_alfa'')
                                                                                                                                                                                                AND             polcov.columnname LIKE ''%direct%'' THEN polcov.val
                                                                                                                                                                                                WHEN covtermpatternid IN (''HODW_WindHail_Ded_HOE'',
                                                                                                                                                                                                        ''HODW_Hurricane_Ded_HOE'',
                                                                                                                                                                                                        ''HODW_SectionI_DedWindstorm_alfa'')
                                                                                                                                                                                                AND             polcov.columnname NOT LIKE ''%direct%'' THEN (
                                                                                                                                                                                                        CASE
                                                                                                                                                                                                        WHEN (
                                                                                                                                                                                                        cast(value1 AS DECIMAL(18,4))<=1.0000) THEN ''F''
                                                                                                                                                                                                        WHEN value1 IS NULL
                                                                                                                                                                                                        OR              value1=0 THEN NULL
                                                                                                                                                                                                        ELSE ''D''
                                                                                                                                                                                                        END )
                                                                                                                                                                                END ) AS deductiblews
                                                                                                                                                                FROM            (
                                                                                                                                                                                           SELECT     cast(''DirectTerm1'' AS                       VARCHAR(100)) AS columnname,
                                                                                                                                                                                                      cast(directterm1_stg AS                     VARCHAR(255)) AS val,
                                                                                                                                                                                                      cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                                                                                                                                      cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.          expirationdate_stg,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.updatetime_stg
                                                                                                                                                                                           FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                                                                                                           inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                                                           ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                                                                                                           join       db_t_prod_stag.pc_policyline
                                                                                                                                                                                           ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                                                                                                           AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                                                                                                           join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                                                                                                           ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                                                                                                           AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                                                                        ''HO3'',
                                                                                                                                                                                                        ''HO4'',
                                                                                                                                                                                                        ''HO5'',
                                                                                                                                                                                                        ''HO6'',
                                                                                                                                                                                                        ''HO8'')
                                                                                                                                                                                           WHERE      directterm1avl_stg = 1
                                                                                                                                                                                           AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT     ''Clause''                   AS columnname,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS val,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                                                                                                      cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.updatetime_stg
                                                                                                                                                                                           FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                                                                                                           inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                                                           ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                                                                                                           join       db_t_prod_stag.pc_policyline
                                                                                                                                                                                           ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                                                                                                           AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                                                                                                           join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                                                                                                           ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                                                                                                           AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                                                                        ''HO3'',
                                                                                                                                                                                                        ''HO4'',
                                                                                                                                                                                                        ''HO5'',
                                                                                                                                                                                                        ''HO6'',
                                                                                                                                                                                                        ''HO8'')
                                                                                                                                                                                           WHERE      pcx_dwellingcov_hoe.patterncode_stg= ''HODW_PersonalPropertyReplacementCost_alfa''
                                                                                                                                                                                           AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT     ''DirectTerm2''                         AS columnname,
                                                                                                                                                                                                      cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                                                                                                      cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.updatetime_stg
                                                                                                                                                                                           FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                                                                                                           inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                                                           ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                                                                                                           join       db_t_prod_stag.pc_policyline
                                                                                                                                                                                           ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                                                                                                           AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                                                                                                           join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                                                                                                           ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                                                                                                           AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                                                                        ''HO3'',
                                                                                                                                                                                                        ''HO4'',
                                                                                                                                                                                                        ''HO5'',
                                                                                                                                                                                                        ''HO6'',
                                                                                                                                                                                                        ''HO8'')
                                                                                                                                                                                           WHERE      directterm2avl_stg = 1
                                                                                                                                                                                           AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT     ''DirectTerm3''                         AS columnname,
                                                                                                                                                                                                      cast(directterm3_stg AS VARCHAR(255)) AS val,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                                                                                                      cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.updatetime_stg
                                                                                                                                                                                           FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                                                                                                           inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                                                           ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                                                                                                           join       db_t_prod_stag.pc_policyline
                                                                                                                                                                                           ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                                                                                                           AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                                                                                                           join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                                                                                                           ON         pc_policyline.hopolicytype_stg= pctl_hopolicytype_hoe.id_stg
                                                                                                                                                                                           AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                                                                        ''HO3'',
                                                                                                                                                                                                        ''HO4'',
                                                                                                                                                                                                        ''HO5'',
                                                                                                                                                                                                        ''HO6'',
                                                                                                                                                                                                        ''HO8'')
                                                                                                                                                                                           WHERE      directterm3avl_stg = 1
                                                                                                                                                                                           AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT     ''DirectTerm4''                         AS columnname,
                                                                                                                                                                                                      cast(directterm4_stg AS VARCHAR(255)) AS val,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                                                                                                      cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.          expirationdate_stg,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.updatetime_stg
                                                                                                                                                                                           FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                                                                                                           inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                                                           ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                                                                                                           join       db_t_prod_stag.pc_policyline
                                                                                                                                                                                           ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                                                                                                           AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                                                                                                           join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                                                                                                           ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                                                                                                           AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                                                                        ''HO3'',
                                                                                                                                                                                                        ''HO4'',
                                                                                                                                                                                                        ''HO5'',
                                                                                                                                                                                                        ''HO6'',
                                                                                                                                                                                                        ''HO8'')
                                                                                                                                                                                           WHERE      directterm4avl_stg = 1
                                                                                                                                                                                           AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT     ''ChoiceTerm2''                         AS columnname,
                                                                                                                                                                                                      cast(choiceterm2_stg AS VARCHAR(255)) AS val,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                                                                                                      cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.updatetime_stg
                                                                                                                                                                                           FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                                                                                                           inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                                                           ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                                                                                                           join       db_t_prod_stag.pc_policyline
                                                                                                                                                                                           ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                                                                                                           AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                                                                                                           join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                                                                                                           ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                                                                                                           AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                                                                        ''HO3'',
                                                                                                                                                                                                        ''HO4'',
                                                                                                                                                                                                        ''HO5'',
                                                                                                                                                                                                        ''HO6'',
                                                                                                                                                                                                        ''HO8'')
                                                                                                                                                                                           WHERE      choiceterm2avl_stg = 1
                                                                                                                                                                                           AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT     ''ChoiceTerm3''                         AS columnname,
                                                                                                                                                                                                      cast(choiceterm3_stg AS VARCHAR(255)) AS val,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                                                                                                      cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe. effectivedate_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.updatetime_stg
                                                                                                                                                                                           FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                                                                                                           inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                                                           ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                                                                                                           join       db_t_prod_stag.pc_policyline
                                                                                                                                                                                           ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                                                                                                           AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                                                                                                           join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                                                                                                           ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                                                                                                           AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                                                                        ''HO3'',
                                                                                                                                                                                                        ''HO4'',
                                                                                                                                                                                                        ''HO5'',
                                                                                                                                                                                                        ''HO6'',
                                                                                                                                                                                                        ''HO8'')
                                                                                                                                                                                           WHERE      choiceterm3avl_stg = 1
                                                                                                                                                                                           AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT     cast(''ChoiceTerm1'' AS                       VARCHAR(250)) AS columnname,
                                                                                                                                                                                                      cast(choiceterm1_stg AS                     VARCHAR(255)) AS val,
                                                                                                                                                                                                      cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                                                                                                                                      cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe. effectivedate_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.updatetime_stg
                                                                                                                                                                                           FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                                                                                                           inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                                                           ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                                                                                                           join       db_t_prod_stag.pc_policyline
                                                                                                                                                                                           ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                                                                                                           AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                                                                                                           join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                                                                                                           ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                                                                                                           AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                                                                        ''HO3'',
                                                                                                                                                                                                        ''HO4'',
                                                                                                                                                                                                        ''HO5'',
                                                                                                                                                                                                        ''HO6'',
                                                                                                                                                                                                        ''HO8'')
                                                                                                                                                                                           WHERE      choiceterm1avl_stg = 1
                                                                                                                                                                                           AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT     cast(''ChoiceTerm4'' AS                       VARCHAR(250)) AS columnname,
                                                                                                                                                                                                      cast(choiceterm4_stg AS                     VARCHAR(255)) AS val,
                                                                                                                                                                                                      cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                                                                                                                                      cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.updatetime_stg
                                                                                                                                                                                           FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                                                                                                           inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                                                           ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                                                                                                           join       db_t_prod_stag.pc_policyline
                                                                                                                                                                                           ON         pp.id_stg= pc_policyline.branchid_stg
                                                                                                                                                                                           AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                                                                                                           join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                                                                                                           ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                                                                                                           AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                                                                        ''HO3'',
                                                                                                                                                                                                        ''HO4'',
                                                                                                                                                                                                        ''HO5'',
                                                                                                                                                                                                        ''HO6'',
                                                                                                                                                                                                        ''HO8'')
                                                                                                                                                                                           WHERE      choiceterm4avl_stg = 1
                                                                                                                                                                                           AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                                                                           UNION
                                                                                                                                                                                           SELECT     cast(''BooleanTerm1'' AS                      VARCHAR(250)) AS columnname,
                                                                                                                                                                                                      cast(booleanterm1_stg AS                    VARCHAR(255)) AS val,
                                                                                                                                                                                                      cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                                                                                                                                      cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.createtime_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                                                                                                                                      cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                                                                                                                                      pcx_dwellingcov_hoe.updatetime_stg
                                                                                                                                                                                           FROM       db_t_prod_stag.pcx_dwellingcov_hoe
                                                                                                                                                                                           inner join db_t_prod_stag.pc_policyperiod pp
                                                                                                                                                                                           ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                                                                                                                                           join       db_t_prod_stag.pc_policyline
                                                                                                                                                                                           ON         pp.id_stg = pc_policyline.branchid_stg
                                                                                                                                                                                           AND        pc_policyline.expirationdate_stg IS NULL
                                                                                                                                                                                           join       db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                                                                                                           ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                                                                                                           AND        pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                                                                        ''HO3'',
                                                                                                                                                                                                        ''HO4'',
                                                                                                                                                                                                        ''HO5'',
                                                                                                                                                                                                        ''HO6'',
                                                                                                                                                                                                        ''HO8'')
                                                                                                                                                                                           WHERE      pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                                                                           AND        pcx_dwellingcov_hoe.patterncode_stg =''HODW_SectionI_Ded_HOE'' ) polcov
                                                                                                                                                                left join
                                                                                                                                                                                (
                                                                                                                                                                                       SELECT pcl.patternid_stg     clausepatternid,
                                                                                                                                                                                              pcv.patternid_stg     covtermpatternid,
                                                                                                                                                                                              pcv.columnname_stg  AS columnname,
                                                                                                                                                                                              pcv.covtermtype_stg AS covtermtype,
                                                                                                                                                                                              pcl.name_stg           clausename
                                                                                                                                                                                       FROM   db_t_prod_stag.pc_etlclausepattern pcl
                                                                                                                                                                                       join   db_t_prod_stag.pc_etlcovtermpattern pcv
                                                                                                                                                                                       ON     pcl.id_stg = pcv.clausepatternid_stg ) covterm
                                                                                                                                                                ON              covterm.clausepatternid = polcov.patterncode
                                                                                                                                                                AND             covterm.columnname = polcov.columnname
                                                                                                                                                                left outer join
                                                                                                                                                                                (
                                                                                                                                                                                           SELECT     pco.patternid_stg                      optionpatternid,
                                                                                                                                                                                                      pco.optioncode_stg                     name1,
                                                                                                                                                                                                      cast(pco.value_stg AS VARCHAR(255)) AS value1,
                                                                                                                                                                                                      pcp.valuetype_stg                   AS valuetype
                                                                                                                                                                                           FROM       db_t_prod_stag.pc_etlcovtermpattern pcp
                                                                                                                                                                                           inner join db_t_prod_stag.pc_etlcovtermoption pco
                                                                                                                                                                                           ON         pcp.id_stg = pco.coveragetermpatternid_stg ) optn
                                                                                                                                                                ON              optn.optionpatternid = polcov.val
                                                                                                                                                                GROUP BY        branchid_stg ) cov
                                                                                                                                ON              cast(cov.branchid_stg AS INTEGER) =cast(pp.id_stg AS INTEGER)
                                                                                                                                left join       db_t_prod_stag.pctl_residencetype_hoe pr
                                                                                                                                ON              residencetype_stg= pr.id_stg
                                                                                                                                left join       db_t_prod_stag.pctl_constructiontype_hoe pct
                                                                                                                                ON              pct.id_stg=constructiontype_stg
                                                                                                                                join            db_t_prod_stag.pctl_policyperiodstatus
                                                                                                                                ON              pp.status_stg =pctl_policyperiodstatus.id_stg
                                                                                                                                join            db_t_prod_stag.pc_policyterm pt
                                                                                                                                ON              pt.id_stg = pp.policytermid_stg
                                                                                                                                join            db_t_prod_stag.pc_policyline
                                                                                                                                ON              pp.id_stg = pc_policyline.branchid_stg
                                                                                                                                AND             pc_policyline.expirationdate_stg IS NULL
                                                                                                                                join            db_t_prod_stag.pctl_hopolicytype_hoe
                                                                                                                                ON              pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                                                                AND             pctl_hopolicytype_hoe.typecode_stg IN (''HO2'',
                                                                                                                                                                                       ''HO3'',
                                                                                                                                                                                       ''HO4'',
                                                                                                                                                                                       ''HO5'',
                                                                                                                                                                                       ''HO6'',
                                                                                                                                                                                       ''HO8'') )a
                                                                                                WHERE           row1=1) pc_policyperiod ) lkp_pc
                                                  ON              lkp_pc.policynumber=sq_clm.policynumber) ) src ) );
  -- Component exp_all, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_all AS
  (
         SELECT sq_cc_claim.companynumber               AS companynumber,
                sq_cc_claim.lineofbusinesscode          AS lineofbusinesscode,
                sq_cc_claim.statecode                   AS statecode,
                sq_cc_claim.callyear                    AS callyear,
                sq_cc_claim.accountingyear              AS accountingyear,
                sq_cc_claim.expperiodyear               AS expperiodyear,
                sq_cc_claim.expperiodmonth              AS expperiodmonth,
                sq_cc_claim.expeperiodday               AS expeperiodday,
                sq_cc_claim.classificationcode          AS classificationcode,
                sq_cc_claim.territorycode               AS territorycode,
                sq_cc_claim.stateexceptionind           AS stateexceptionind,
                sq_cc_claim.zipcode                     AS zipcode,
                sq_cc_claim.policyeffectiveyear         AS policyeffectiveyear,
                sq_cc_claim.newrecordformat             AS newrecordformat,
                sq_cc_claim.aslob                       AS aslob,
                sq_cc_claim.itemcode                    AS itemcode,
                sq_cc_claim.sublinecode                 AS sublinecode,
                sq_cc_claim.policyprogramcode           AS policyprogramcode,
                sq_cc_claim.policyformcode              AS policyformcode,
                sq_cc_claim.no_of_family_codes          AS no_of_family_codes,
                sq_cc_claim.construction                AS construction,
                sq_cc_claim.protectioncode              AS protectioncode,
                sq_cc_claim.exceptioncode               AS exceptioncode,
                sq_cc_claim.typeofdeductiblecode        AS typeofdeductiblecode,
                sq_cc_claim.policytermcode              AS policytermcode,
                sq_cc_claim.typeoflosscode              AS typeoflosscode,
                sq_cc_claim.stateexceptionb             AS stateexceptionb,
                sq_cc_claim.amountofinsurance           AS amountofinsurance,
                sq_cc_claim.yeaofmanufacture            AS yeaofmanufacture,
                sq_cc_claim.coveragecodeb               AS coveragecodeb,
                sq_cc_claim.exposurecodes               AS exposurecodes,
                sq_cc_claim.leadpoisioning              AS leadpoisioning,
                sq_cc_claim.dedindicator                AS dedindicator,
                sq_cc_claim.dedamount                   AS dedamount,
                sq_cc_claim.deductibleindicatorws       AS deductibleindicatorws,
                sq_cc_claim.deductibleamountws          AS deductibleamountws,
                sq_cc_claim.claimidentifier             AS claimidentifier,
                sq_cc_claim.claimantidentifier          AS claimantidentifier,
                sq_cc_claim.writtenexposure             AS writtenexposure,
                sq_cc_claim.writtenpremium              AS writtenpremium,
                sq_cc_claim.paidlosses                  AS paidlosses,
                sq_cc_claim.paidnumberofclaims          AS paidnumberofclaims,
                sq_cc_claim.outstandinglosses           AS outstandinglosses,
                sq_cc_claim.outstandingnoofclaims       AS outstandingnoofclaims,
                sq_cc_claim.policynumber                AS policynumber,
                sq_cc_claim.policyperiodid              AS policyperiodid,
                sq_cc_claim.policysystemperiodid        AS policysystemperiodid,
                sq_cc_claim.territorycode_pc            AS territorycode1,
                sq_cc_claim.zipcode_pc                  AS zipcode1,
                sq_cc_claim.itemcode_pc                 AS itemcode1,
                sq_cc_claim.sublinecode_pc              AS sublinecode1,
                sq_cc_claim.numberoffamilycodes_pc      AS numberoffamilycodes,
                sq_cc_claim.constructioncode_pc         AS constructioncode,
                sq_cc_claim.protectioncasscode_pc       AS protectioncasscode,
                sq_cc_claim.typeofdeductiblecode_pc     AS typeofdeductiblecode1,
                sq_cc_claim.amountofinsurance_pc        AS amountofinsurance1,
                sq_cc_claim.yeaofconstructionliabblt_pc AS yeaofconstructionliablt,
                sq_cc_claim.deductibleindicator_pc      AS deductibleindicator,
                sq_cc_claim.deductibleamount_pc         AS deductibleamount,
                sq_cc_claim.deductibleindicatorws_pc    AS deductibleindicatorws1,
                sq_cc_claim.deductibleamountws_pc       AS deductibleamountws1,
                sq_cc_claim.policynumber_pc             AS policynumber1,
                sq_cc_claim.policyidentifier_pc         AS policyidentifier,
                sq_cc_claim.coveragecode_pc             AS coveragecode1,
                sq_cc_claim.territorycode_pclkp         AS territorycode_pclkp,
                sq_cc_claim.zipcode_pclkp               AS zipcode_pclkp,
                sq_cc_claim.itemcode_pclkp              AS itemcode_pclkp,
                sq_cc_claim.sublinecode_pclkp           AS sublinecode_pclkp,
                sq_cc_claim.no_of_family_code_pclkp     AS no_of_family_code_pclkp,
                sq_cc_claim.construction_pclkp          AS construction_pclkp,
                sq_cc_claim.protectionclass_pclkp       AS protectionclass_pclkp,
                sq_cc_claim.typeofdeductible_pclkp      AS typeofdeductible_pclkp,
                sq_cc_claim.amountofinsurance_pclkp     AS amountofinsurance_pclkp,
                sq_cc_claim.yeaofmanufacture_pclkp      AS yeaofmanufacture_pclkp,
                sq_cc_claim.ded_ind_pclkp               AS ded_ind_pclkp,
                sq_cc_claim.ded_amt_pclkp               AS ded_amt_pclkp,
                sq_cc_claim.ded_wind_ind_pclkp          AS ded_wind_ind_pclkp,
                sq_cc_claim.ded_wind_amt_pclkp          AS ded_wind_amt_pclkp,
                sq_cc_claim.coverage_pclkp              AS coverage_pclkp,
                sq_cc_claim.source_record_id
         FROM   sq_cc_claim );
  -- Component exp_clm_trans_logic, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_clm_trans_logic AS
  (
         SELECT exp_all.companynumber      AS companynumber,
                exp_all.lineofbusinesscode AS lineofbusinesscode,
                exp_all.statecode          AS statecode,
                exp_all.callyear           AS callyear,
                exp_all.accountingyear     AS accountingyear,
                exp_all.expperiodyear      AS expperiodyear,
                exp_all.expperiodmonth     AS expperiodmonth,
                exp_all.expeperiodday      AS expeperiodday,
                CASE
                       WHEN exp_all.coveragecode1 IS NULL
                       AND    exp_all.coverage_pclkp IS NULL THEN ''01''
                       ELSE (
                              CASE
                                     WHEN exp_all.coveragecode1 IS NULL THEN exp_all.coverage_pclkp
                                     ELSE exp_all.coveragecode1
                              END )
                END                         AS coveragecode,
                exp_all.classificationcode  AS classificationcode,
                exp_all.stateexceptionind   AS stateexceptionind,
                exp_all.policyeffectiveyear AS policyeffectiveyear,
                exp_all.newrecordformat     AS newrecordformat,
                exp_all.aslob               AS aslob,
                exp_all.policyprogramcode   AS policyprogramcode,
                exp_all.policyformcode      AS policyformcode,
                exp_all.exceptioncode       AS exceptioncode,
                exp_all.policytermcode      AS policytermcode,
                exp_all.typeoflosscode      AS typeoflosscode,
                exp_all.stateexceptionb     AS stateexceptionb,
                exp_all.coveragecodeb       AS coveragecodeb,
                exp_all.exposurecodes       AS exposurecodes,
                exp_all.leadpoisioning      AS leadpoisioning,
                substr ( ''000000000000000'' , 1 , 15 - length ( exp_all.claimidentifier ) )
                       || exp_all.claimidentifier AS claimidentifier1,
                exp_all.claimantidentifier        AS claimantidentifier,
                exp_all.writtenexposure           AS writtenexposure,
                exp_all.writtenpremium            AS writtenpremium,
                exp_all.paidlosses                AS paidlosses,
                exp_all.paidnumberofclaims        AS paidnumberofclaims,
                exp_all.outstandinglosses         AS outstandinglosses,
                exp_all.outstandingnoofclaims     AS outstandingnoofclaims,
                exp_all.policynumber              AS policynumber,
                exp_all.policyperiodid            AS policyperiodid,
                substr ( ''00'' , 1 , 2 - length (
                CASE
                       WHEN
                              CASE
                                     WHEN exp_all.territorycode1 IS NULL THEN exp_all.territorycode_pclkp
                                     ELSE exp_all.territorycode1
                              END IS NULL THEN ''00''
                       ELSE
                              CASE
                                     WHEN exp_all.territorycode1 IS NULL THEN exp_all.territorycode_pclkp
                                     ELSE exp_all.territorycode1
                              END
                END ) )
                       ||
                CASE
                       WHEN
                              CASE
                                     WHEN exp_all.territorycode1 IS NULL THEN exp_all.territorycode_pclkp
                                     ELSE exp_all.territorycode1
                              END IS NULL THEN ''00''
                       ELSE
                              CASE
                                     WHEN exp_all.territorycode1 IS NULL THEN exp_all.territorycode_pclkp
                                     ELSE exp_all.territorycode1
                              END
                END AS out_territorycode,
                CASE
                       WHEN
                              CASE
                                     WHEN exp_all.zipcode1 IS NULL THEN exp_all.zipcode_pclkp
                                     ELSE exp_all.zipcode1
                              END IS NULL THEN ''00000''
                       ELSE
                              CASE
                                     WHEN exp_all.zipcode1 IS NULL THEN exp_all.zipcode_pclkp
                                     ELSE exp_all.zipcode1
                              END
                END AS out_zipcode,
                CASE
                       WHEN
                              CASE
                                     WHEN exp_all.itemcode1 IS NULL THEN exp_all.itemcode_pclkp
                                     ELSE exp_all.itemcode1
                              END IS NULL THEN ''00''
                       ELSE
                              CASE
                                     WHEN exp_all.itemcode1 IS NULL THEN exp_all.itemcode_pclkp
                                     ELSE exp_all.itemcode1
                              END
                END AS out_itemcode,
                CASE
                       WHEN
                              CASE
                                     WHEN exp_all.sublinecode1 IS NULL THEN exp_all.sublinecode_pclkp
                                     ELSE exp_all.sublinecode1
                              END IS NULL THEN ''00''
                       ELSE
                              CASE
                                     WHEN exp_all.sublinecode1 IS NULL THEN exp_all.sublinecode_pclkp
                                     ELSE exp_all.sublinecode1
                              END
                END AS out_sublinecode,
                CASE
                       WHEN
                              CASE
                                     WHEN exp_all.numberoffamilycodes IS NULL THEN exp_all.no_of_family_code_pclkp
                                     ELSE exp_all.numberoffamilycodes
                              END IS NULL THEN ''0''
                       ELSE
                              CASE
                                     WHEN exp_all.numberoffamilycodes IS NULL THEN exp_all.no_of_family_code_pclkp
                                     ELSE exp_all.numberoffamilycodes
                              END
                END AS out_numberoffamilycodes,
                CASE
                       WHEN
                              CASE
                                     WHEN exp_all.constructioncode IS NULL THEN exp_all.construction_pclkp
                                     ELSE exp_all.constructioncode
                              END IS NULL THEN ''0''
                       ELSE
                              CASE
                                     WHEN exp_all.constructioncode IS NULL THEN exp_all.construction_pclkp
                                     ELSE exp_all.constructioncode
                              END
                END AS out_constructioncode,
                CASE
                       WHEN
                              CASE
                                     WHEN exp_all.protectioncasscode IS NULL THEN exp_all.protectionclass_pclkp
                                     ELSE exp_all.protectioncasscode
                              END IS NULL THEN ''00''
                       ELSE
                              CASE
                                     WHEN exp_all.protectioncasscode IS NULL THEN exp_all.protectionclass_pclkp
                                     ELSE exp_all.protectioncasscode
                              END
                END AS out_protectioncasscode,
                CASE
                       WHEN
                              CASE
                                     WHEN exp_all.typeofdeductiblecode1 IS NULL THEN exp_all.typeofdeductible_pclkp
                                     ELSE exp_all.typeofdeductiblecode1
                              END IS NULL THEN ''00''
                       ELSE
                              CASE
                                     WHEN exp_all.typeofdeductiblecode1 IS NULL THEN exp_all.typeofdeductible_pclkp
                                     ELSE exp_all.typeofdeductiblecode1
                              END
                END AS out_typeofdeductiblecode,
                substr ( ''00000'' , 1 , 5 - length (
                CASE
                       WHEN
                              CASE
                                     WHEN exp_all.amountofinsurance1 IS NULL THEN exp_all.amountofinsurance_pclkp
                                     ELSE exp_all.amountofinsurance1
                              END IS NULL THEN ''00000''
                       ELSE
                              CASE
                                     WHEN exp_all.amountofinsurance1 IS NULL THEN exp_all.amountofinsurance_pclkp
                                     ELSE exp_all.amountofinsurance1
                              END
                END ) )
                       ||
                CASE
                       WHEN
                              CASE
                                     WHEN exp_all.amountofinsurance1 IS NULL THEN exp_all.amountofinsurance_pclkp
                                     ELSE exp_all.amountofinsurance1
                              END IS NULL THEN ''00000''
                       ELSE
                              CASE
                                     WHEN exp_all.amountofinsurance1 IS NULL THEN exp_all.amountofinsurance_pclkp
                                     ELSE exp_all.amountofinsurance1
                              END
                END AS out_amountofinsurance,
                substr ( ''0000'' , 1 , 4 - length (
                CASE
                       WHEN
                              CASE
                                     WHEN exp_all.yeaofconstructionliablt IS NULL THEN exp_all.yeaofmanufacture_pclkp
                                     ELSE exp_all.yeaofconstructionliablt
                              END IS NULL THEN ''0000''
                       ELSE
                              CASE
                                     WHEN exp_all.yeaofconstructionliablt IS NULL THEN exp_all.yeaofmanufacture_pclkp
                                     ELSE exp_all.yeaofconstructionliablt
                              END
                END ) )
                       ||
                CASE
                       WHEN
                              CASE
                                     WHEN exp_all.yeaofconstructionliablt IS NULL THEN exp_all.yeaofmanufacture_pclkp
                                     ELSE exp_all.yeaofconstructionliablt
                              END IS NULL THEN ''0000''
                       ELSE
                              CASE
                                     WHEN exp_all.yeaofconstructionliablt IS NULL THEN exp_all.yeaofmanufacture_pclkp
                                     ELSE exp_all.yeaofconstructionliablt
                              END
                END AS out_yeaofconstructionliablt,
                CASE
                       WHEN
                              CASE
                                     WHEN exp_all.deductibleindicator IS NULL THEN exp_all.ded_ind_pclkp
                                     ELSE exp_all.deductibleindicator
                              END IS NULL THEN ''0''
                       ELSE
                              CASE
                                     WHEN exp_all.deductibleindicator IS NULL THEN exp_all.ded_ind_pclkp
                                     ELSE exp_all.deductibleindicator
                              END
                END AS out_deductibleindicator,
                CASE
                       WHEN
                              CASE
                                     WHEN exp_all.deductibleamount IS NULL THEN exp_all.ded_amt_pclkp
                                     ELSE exp_all.deductibleamount
                              END IS NULL THEN ''0000000''
                       ELSE
                              CASE
                                     WHEN exp_all.deductibleamount IS NULL THEN exp_all.ded_amt_pclkp
                                     ELSE exp_all.deductibleamount
                              END
                END AS out_deductibleamount,
                CASE
                       WHEN
                              CASE
                                     WHEN exp_all.deductibleindicatorws1 IS NULL THEN exp_all.ded_wind_ind_pclkp
                                     ELSE exp_all.deductibleindicatorws1
                              END IS NULL THEN ''0''
                       ELSE
                              CASE
                                     WHEN exp_all.deductibleindicatorws1 IS NULL THEN exp_all.ded_wind_ind_pclkp
                                     ELSE exp_all.deductibleindicatorws1
                              END
                END AS out_deductibleindicatorws,
                substr ( ''0000000'' , 1 , 7 - length (
                CASE
                       WHEN
                              CASE
                                     WHEN exp_all.deductibleamountws1 IS NULL THEN exp_all.ded_wind_amt_pclkp
                                     ELSE exp_all.deductibleamountws1
                              END IS NULL THEN ''0000000''
                       ELSE
                              CASE
                                     WHEN exp_all.deductibleamountws1 IS NULL THEN exp_all.ded_wind_amt_pclkp
                                     ELSE exp_all.deductibleamountws1
                              END
                END ) )
                       ||
                CASE
                       WHEN
                              CASE
                                     WHEN exp_all.deductibleamountws1 IS NULL THEN exp_all.ded_wind_amt_pclkp
                                     ELSE exp_all.deductibleamountws1
                              END IS NULL THEN ''0000000''
                       ELSE
                              CASE
                                     WHEN exp_all.deductibleamountws1 IS NULL THEN exp_all.ded_wind_amt_pclkp
                                     ELSE exp_all.deductibleamountws1
                              END
                END                      AS out_deductibleamountws,
                exp_all.policyidentifier AS policyidentifier,
                current_timestamp        AS creationts,
                ''0''                      AS createdby,
                current_timestamp        AS updatets,
                ''0''                      AS updateid,
                exp_all.source_record_id
         FROM   exp_all );
  -- Component OUT_NAIIPCI_HO1_claim, Type TARGET
  INSERT INTO db_t_prod_comn.out_naiipci_ho
              (
                          companynumber,
                          lob,
                          stateofprincipalgarage,
                          callyear,
                          accountingyear,
                          expperiodyear,
                          expperiodmonth,
                          expperiodday,
                          coveragecode,
                          classificationcode,
                          territorycode,
                          stateexceptionind,
                          zipcode,
                          policyeffectiveyear,
                          newrecordformat,
                          aslob,
                          itemcode,
                          sublinecode,
                          policyprogramcode,
                          policyformcode,
                          numberoffamilycodes,
                          constructioncode,
                          protectionclasscode,
                          exceptioncode,
                          typeofdeductiblecode,
                          policytermcode,
                          typelosscode,
                          stateofexceptionb,
                          amountofinsurance,
                          yearofconstnliablt,
                          coveragecodeordlaw,
                          exposurecodes,
                          leadpoisoningliability,
                          deductibleindicator,
                          deductibleamount,
                          deductindwindstorm,
                          deductamountwindstorm,
                          claimidentifier,
                          claimantidentifier,
                          writtenexposure,
                          writtenpremium,
                          paidlosses,
                          paidnumberofclaims,
                          outstandinglosses,
                          outstandingnoofclaims,
                          policynumber,
                          policyperiodid,
                          creationts,
                          creationuid,
                          updatets,
                          updateuid,
                          policyidentifier
              )
  SELECT exp_clm_trans_logic.companynumber               AS companynumber,
         exp_clm_trans_logic.lineofbusinesscode          AS lob,
         exp_clm_trans_logic.statecode                   AS stateofprincipalgarage,
         exp_clm_trans_logic.callyear                    AS callyear,
         exp_clm_trans_logic.accountingyear              AS accountingyear,
         exp_clm_trans_logic.expperiodyear               AS expperiodyear,
         exp_clm_trans_logic.expperiodmonth              AS expperiodmonth,
         exp_clm_trans_logic.expeperiodday               AS expperiodday,
         exp_clm_trans_logic.coveragecode                AS coveragecode,
         exp_clm_trans_logic.classificationcode          AS classificationcode,
         exp_clm_trans_logic.out_territorycode           AS territorycode,
         exp_clm_trans_logic.stateexceptionind           AS stateexceptionind,
         exp_clm_trans_logic.out_zipcode                 AS zipcode,
         exp_clm_trans_logic.policyeffectiveyear         AS policyeffectiveyear,
         exp_clm_trans_logic.newrecordformat             AS newrecordformat,
         exp_clm_trans_logic.aslob                       AS aslob,
         exp_clm_trans_logic.out_itemcode                AS itemcode,
         exp_clm_trans_logic.out_sublinecode             AS sublinecode,
         exp_clm_trans_logic.policyprogramcode           AS policyprogramcode,
         exp_clm_trans_logic.policyformcode              AS policyformcode,
         exp_clm_trans_logic.out_numberoffamilycodes     AS numberoffamilycodes,
         exp_clm_trans_logic.out_constructioncode        AS constructioncode,
         exp_clm_trans_logic.out_protectioncasscode      AS protectionclasscode,
         exp_clm_trans_logic.exceptioncode               AS exceptioncode,
         exp_clm_trans_logic.out_typeofdeductiblecode    AS typeofdeductiblecode,
         exp_clm_trans_logic.policytermcode              AS policytermcode,
         exp_clm_trans_logic.typeoflosscode              AS typelosscode,
         exp_clm_trans_logic.stateexceptionb             AS stateofexceptionb,
         exp_clm_trans_logic.out_amountofinsurance       AS amountofinsurance,
         exp_clm_trans_logic.out_yeaofconstructionliablt AS yearofconstnliablt,
         exp_clm_trans_logic.coveragecodeb               AS coveragecodeordlaw,
         exp_clm_trans_logic.exposurecodes               AS exposurecodes,
         exp_clm_trans_logic.leadpoisioning              AS leadpoisoningliability,
         exp_clm_trans_logic.out_deductibleindicator     AS deductibleindicator,
         exp_clm_trans_logic.out_deductibleamount        AS deductibleamount,
         exp_clm_trans_logic.out_deductibleindicatorws   AS deductindwindstorm,
         exp_clm_trans_logic.out_deductibleamountws      AS deductamountwindstorm,
         exp_clm_trans_logic.claimidentifier1            AS claimidentifier,
         exp_clm_trans_logic.claimantidentifier          AS claimantidentifier,
         exp_clm_trans_logic.writtenexposure             AS writtenexposure,
         exp_clm_trans_logic.writtenpremium              AS writtenpremium,
         exp_clm_trans_logic.paidlosses                  AS paidlosses,
         exp_clm_trans_logic.paidnumberofclaims          AS paidnumberofclaims,
         exp_clm_trans_logic.outstandinglosses           AS outstandinglosses,
         exp_clm_trans_logic.outstandingnoofclaims       AS outstandingnoofclaims,
         exp_clm_trans_logic.policynumber                AS policynumber,
         exp_clm_trans_logic.policyperiodid              AS policyperiodid,
         exp_clm_trans_logic.creationts                  AS creationts,
         exp_clm_trans_logic.createdby                   AS creationuid,
         exp_clm_trans_logic.updatets                    AS updatets,
         exp_clm_trans_logic.updateid                    AS updateuid,
         exp_clm_trans_logic.policyidentifier            AS policyidentifier
  FROM   exp_clm_trans_logic;
  
  -- PIPELINE END FOR 2
END;
';