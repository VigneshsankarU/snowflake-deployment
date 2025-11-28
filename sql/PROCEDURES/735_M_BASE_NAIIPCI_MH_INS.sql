-- Object Type: PROCEDURES
CREATE OR REPLACE PROCEDURE ALFA_EDW_DEV.PUBLIC.M_BASE_NAIIPCI_MH_INS("WORKLET_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS '
DECLARE
  run_id STRING;
  prcs_id int;
  CC_BOY string;
  CC_EOY string;
  PC_EOY string;
  PC_BOY string;

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
                $10 AS territorycode,
                $11 AS zipcode,
                $12 AS policyeffectiveyear,
                $13 AS newrecordformat,
                $14 AS aslob,
                $15 AS itemcode,
                $16 AS sublinecode,
                $17 AS protectioncasscode,
                $18 AS typeofdeductiblecode,
                $19 AS policytermcode,
                $20 AS typeoflosscode,
                $21 AS locationcode,
                $22 AS amountofinsurance,
                $23 AS yeaofmanufacture,
                $24 AS tiedowncode,
                $25 AS deductibleindicator,
                $26 AS deductibleamount,
                $27 AS deductibleindicatorws,
                $28 AS deductibleamountws,
                $29 AS claimidentifier,
                $30 AS claimantidentifier,
                $31 AS writtenexposure,
                $32 AS writtenpremium,
                $33 AS paidlosses,
                $34 AS paidnumberofclaims,
                $35 AS outstandinglosses,
                $36 AS outstandingnoofclaims,
                $37 AS policynumber,
                $38 AS policyperiodid,
                $39 AS policyidentifier,
                $40 AS source_record_id
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
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       (
                                                                                                        substring (b.postalcodeinternal_stg,0,position(''-'' IN b.postalcodeinternal_stg))=''36511''
                                                                                              OR        b.postalcodeinternal_stg=''36511'')
                                                                                    AND       upper(cityinternal_stg)= ''BON SECOUR'' THEN ''06''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       (
                                                                                                        substring (b.postalcodeinternal_stg,0,position(''-'' IN b.postalcodeinternal_stg))=''36528''
                                                                                              OR        b.postalcodeinternal_stg=''36528'')
                                                                                    AND       upper(cityinternal_stg)= ''DAUPHIN ISLAND'' THEN ''06''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       (
                                                                                                        substring (b.postalcodeinternal_stg,0,position(''-'' IN b.postalcodeinternal_stg)) IN (''36542'',
                                                                                                                                                                                             ''36547'')
                                                                                              OR        b.postalcodeinternal_stg IN (''36542'',
                                                                                                                                     ''36547''))
                                                                                    AND       upper(cityinternal_stg)= ''GULF SHORES'' THEN ''06''
                                                                                    WHEN g.typecode_stg=''AL''
                                                                                    AND       (
                                                                                                        substring (b.postalcodeinternal_stg,0,position(''-'' IN b.postalcodeinternal_stg))=''36561''
                                                                                              OR        b.postalcodeinternal_stg=''36561'')
                                                                                    AND       upper(cityinternal_stg)= ''ORANGE BEACH'' THEN ''06''
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
                                                                AND       pctl_hopolicytype_hoe.typecode_stg LIKE ''MH%''
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
                                                                      WHERE           publicid_stg LIKE ''%MH%'')pht2
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
                                                                      WHERE           publicid_stg LIKE ''%MH%'')pht3
                                            ON        pht3.row1=1
                                            AND       cast(pht3.countycode_alfa AS VARCHAR(50))=cast(loc.countycode_alfa_stg AS VARCHAR(50))
                                            AND       pht3.state =loc.typecode_stg
                                            AND       pht3.hopolicytype_hoe_stg=loc.hopolicytype_stg
                                            WHERE     ROWNUM=1
                                            GROUP BY  branchid_stg ), 
								 polcov AS
                                  	(
                                                  SELECT DISTINCT branchid,
                                                                  max(
                                                                  CASE
                                                                                  WHEN covterm.covtermpatternid=''HODW_Dwelling_Limit_HOE'' THEN substring(''00000'',1, (5-length( cast(cast(round(cast(polcov.val AS DECIMAL(18,4))/1000,0)AS INTEGER) AS VARCHAR(10)))))
                                                                                                                  || cast(cast(round(cast(polcov.val AS DECIMAL(18,4))/1000, 0)AS INTEGER) AS VARCHAR(10))
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
                                                                                                                  WHEN substring(name1 ,length(name1),1)=''%'' THEN ''P''
                                                                                                                  ELSE ''D''
                                                                                                  END)
                                                                  END)perils_limit_ind,
                                                                  max(
                                                                  CASE
                                                                                  WHEN covterm.covtermpatternid=''HODW_OtherPerils_Ded_HOE'' THEN (
                                                                                                  CASE
                                                                                                                  WHEN substring(name1 ,length(name1),1)=''%'' THEN substring(''0000000'',1, (7-length(cast(cast(cast(cast(value1 AS DECIMAL(18,4)) *10000 AS DECIMAL(18,4)) AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                  || cast(cast(cast(cast(value1 AS DECIMAL(18,4)) *10000 AS DECIMAL(18,4)) AS INTEGER) AS VARCHAR(10))
                                                                                                                  ELSE substring(''0000000'',1,(7-length(cast(cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                  || cast(cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER) AS VARCHAR(10))
                                                                                                  END)
                                                                  END)perils_limit,
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
                                                                                                                                                  || cast(cast(cast(value1 AS DECIMAL(18,4))                            *10000 AS INTEGER) AS VARCHAR(10))
                                                                                                                  WHEN value1 IS NULL
                                                                                                                  OR              cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER) =0 THEN 0
                                                                                                                  ELSE substring(''0000000'',1,(7                                              -length(cast(cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                  || cast(cast(cast(value1 AS DECIMAL(18,4)) *10000 AS INTEGER) AS VARCHAR(10))
                                                                                                  END )
                                                                  END ) AS deductibleamountws
                                                  FROM            (
                                                                             SELECT     cast( ''DirectTerm1'' AS                      VARCHAR(100)) AS columnname,
                                                                                        cast(directterm1_stg AS                     VARCHAR(255)) AS val,
                                                                                        cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid,
                                                                                        pcx_dwellingcov_hoe.createtime_stg,
                                                                                        pcx_dwellingcov_hoe.effectivedate_stg,
                                                                                        pcx_dwellingcov_hoe.expirationdate_stg,
                                                                                        cast(NULL AS VARCHAR(255)) AS choiceterm1,
                                                                                        cast(NULL AS VARCHAR(255)) AS patternid,
                                                                                        pcx_dwellingcov_hoe.updatetime_stg
                                                                             FROM       db_t_prod_stag.pcx_dwellingcov_hoe 
                                                                             inner join db_t_prod_stag.pc_policyperiod pp
                                                                             ON         pp.id_stg = pcx_dwellingcov_hoe.branchid_stg
                                                                             join       db_t_prod_stag.pc_policyline pc_policyline
                                                                             ON         pp.id_stg = pc_policyline.branchid_stg
                                                                             AND        pc_policyline.expirationdate_stg IS NULL
                                                                             join       db_t_prod_stag.pctl_hopolicytype_hoe 
                                                                             ON         pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                               ''MH4'',
                                                                                                                               ''MH7'',
                                                                                                                               ''MH9'')
                                                                             WHERE      directterm1avl_stg = 1
                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                             UNION
                                                                             SELECT     ''Clause''                   AS columnname,
                                                                                        cast(NULL AS VARCHAR(255)) AS val,
                                                                                        pcx_dwellingcov_hoe.patterncode_stg,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid,
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
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                               ''MH4'',
                                                                                                                               ''MH7'',
                                                                                                                               ''MH9'')
                                                                             WHERE      pcx_dwellingcov_hoe.patterncode_stg= ''HODW_PersonalPropertyReplacementCost_alfa''
                                                                             UNION
                                                                             SELECT     ''DirectTerm2''                         AS columnname,
                                                                                        cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                                                        pcx_dwellingcov_hoe.patterncode_stg,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid,
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
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                               ''MH4'',
                                                                                                                               ''MH7'',
                                                                                                                               ''MH9'')
                                                                             WHERE      directterm2avl_stg = 1
                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                             UNION
                                                                             SELECT     ''DirectTerm3''                         AS columnname,
                                                                                        cast(directterm3_stg AS VARCHAR(255)) AS val,
                                                                                        pcx_dwellingcov_hoe.patterncode_stg,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid,
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
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                               ''MH4'',
                                                                                                                               ''MH7'',
                                                                                                                               ''MH9'')
                                                                             WHERE      directterm3avl_stg = 1
                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                             UNION
                                                                             SELECT     ''DirectTerm4''                         AS columnname,
                                                                                        cast(directterm4_stg AS VARCHAR(255)) AS val,
                                                                                        pcx_dwellingcov_hoe.patterncode_stg,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid,
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
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                               ''MH4'',
                                                                                                                               ''MH7'',
                                                                                                                               ''MH9'')
                                                                             WHERE      directterm4avl_stg = 1
                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                             UNION
                                                                             SELECT     ''ChoiceTerm2''                         AS columnname,
                                                                                        cast(choiceterm2_stg AS VARCHAR(255)) AS val,
                                                                                        pcx_dwellingcov_hoe.patterncode_stg,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid,
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
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                               ''MH4'',
                                                                                                                               ''MH7'',
                                                                                                                               ''MH9'')
                                                                             WHERE      choiceterm2avl_stg = 1
                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                             UNION
                                                                             SELECT     ''ChoiceTerm3''                         AS columnname,
                                                                                        cast(choiceterm3_stg AS VARCHAR(255)) AS val,
                                                                                        pcx_dwellingcov_hoe.patterncode_stg,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid,
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
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                               ''MH4'',
                                                                                                                               ''MH7'',
                                                                                                                               ''MH9'')
                                                                             WHERE      choiceterm3avl_stg = 1
                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                             UNION
                                                                             SELECT     cast(''ChoiceTerm1'' AS                       VARCHAR(250)) AS columnname,
                                                                                        cast(choiceterm1_stg AS                     VARCHAR(255)) AS val,
                                                                                        cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid,
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
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                               ''MH4'',
                                                                                                                               ''MH7'',
                                                                                                                               ''MH9'')
                                                                             WHERE      choiceterm1avl_stg = 1
                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                             UNION
                                                                             SELECT     cast(''ChoiceTerm4'' AS                       VARCHAR(250)) AS columnname,
                                                                                        cast(choiceterm4_stg AS                     VARCHAR(255)) AS val,
                                                                                        cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid,
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
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                               ''MH4'',
                                                                                                                               ''MH7'',
                                                                                                                               ''MH9'')
                                                                             WHERE      choiceterm4avl_stg = 1
                                                                             AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                             UNION
                                                                             SELECT     cast(''BooleanTerm1'' AS                      VARCHAR(250)) AS columnname,
                                                                                        cast(booleanterm1_stg AS                    VARCHAR(255)) AS val,
                                                                                        cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                        cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid,
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
                                                                             AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                               ''MH4'',
                                                                                                                               ''MH7'',
                                                                                                                               ''MH9'')
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
                                                  GROUP BY        branchid )  -- end of polcov cte
                  SELECT *
                  FROM   (
                                  SELECT   companynumber,
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
                                           protectionclasscode,
                                           typeofdeductiblecode,
                                           abs(writtenexposure) policytermcode,
                                           losscode,
                                           locationcode,
                                           amountofinsurance,
                                           yearofmanufacture,
                                           tiedowncode,
                                           ded_ind_code,
                                           deductible_amt,
                                           deductibleindicatorws,
                                           deductibleamountws,
                                           claimidentifier,
                                           claimantidentifier,
                                           writtenexposure,
                                           CASE
                                                    WHEN coalesce(cast(cast(SUM(wrtnprem) AS DECIMAL(12,2)) AS VARCHAR(20)), ''0.00'') = ''.00'' THEN ''0.00''
                                                    ELSE coalesce(cast(cast(SUM(wrtnprem) AS DECIMAL(12,2)) AS VARCHAR(20)), ''0.00'')
                                           END AS wrtnprem,
                                           paidlosses,
                                           paidnumberofclaims,
                                           outstandinglosses,
                                           outstandingnoofclaims,
                                           policynumber_stg AS policynumber,
                                           policyperiodid,
                                           policyidentifier
                                  FROM     (
                                                           SELECT DISTINCT
                                                                           CASE
                                                                                           WHEN uwc.publicid_stg=''AMI'' THEN ''0005''
                                                                                           ELSE ''0050''
                                                                           END  AS companynumber,
                                                                           ''18'' AS lob,
                                                                           CASE
                                                                                           WHEN jd.typecode_stg=''AL'' THEN ''01''
                                                                                           WHEN jd.typecode_stg=''GA'' THEN ''10''
                                                                                           WHEN jd.typecode_stg=''MS'' THEN ''23''
                                                                           END                                                               statecode,
                                                                           extract(year FROM cast(:PC_EOY AS timestamp) + interval ''1 year'') callyear,
                                                                           extract(year FROM cast(:PC_EOY AS timestamp))                     accountingyear,
                                                                           ''0000''                                                            expperiodyear,
                                                                           ''00''                                                              expperiodmonth,
                                                                           ''00''                                                              expperiodday,
                                                                           CASE
                                                                                           WHEN pcdh.typecode_stg= ''sec'' THEN ''07''
                                                                                           ELSE ''01''
                                                                           END          coveragecode,
                                                                           terr.code AS territorycode,
                                                                           CASE
                                                                                           WHEN substring (terr.postalcodeinternal,0,position(''-'' IN terr.postalcodeinternal)) = '''' THEN terr.postalcodeinternal
                                                                                           ELSE substring (terr.postalcodeinternal,0,position(''-'' IN terr.postalcodeinternal))
                                                                           END AS zipcode,
                                                                           CASE
                                                                                           WHEN pcj.typecode_stg=''Cancellation'' THEN year(pp.cancellationdate_stg)
                                                                                           ELSE year(pp.periodstart_stg)
                                                                           END                                 AS policy_eff_yr,
                                                                           ''D''                                 AS newrecordformat,
                                                                           ''040''                               AS aslob,
                                                                           ''03''                                AS itemcode,
                                                                           ''49''                                AS sublinecode,
                                                                           phh.dwellingprotectionclasscode_stg AS protectionclasscode,
                                                                           CASE
                                                                                           WHEN (
                                                                                                                           (
                                                                                                                                           windhail =''HODW_WindHail_Ded_HOE'')
                                                                                                           OR              (
                                                                                                                                           hurricane =''HODW_Hurricane_Ded_HOE'' ))
                                                                                           AND             deductibleamountws >0 THEN ''03''
                                                                                           WHEN (
                                                                                                                           windstormhailexcl =''HODW_SectionI_DedWindAndHailExcl_alfa'')
                                                                                           AND             (
                                                                                                                           windstormhailexcl_amt <> 0
                                                                                                           AND             windstormhailexcl_amt IS NOT NULL) THEN ''07''
                                                                                           ELSE ''05''
                                                                           END AS typeofdeductiblecode,
                                                                           /*CASE
                                                                                           WHEN pp.cancellationdate_stg IS NOT NULL THEN cast(((cast(pp.cancellationdate_stg AS DATE)-cast(pp.periodstart_stg AS DATE)) month) AS INTEGER)
                                                                                           WHEN pp.cancellationdate_stg IS NULL THEN cast(((cast(pp.periodend_stg AS            DATE)-cast(pp.periodstart_stg AS DATE)) month) AS INTEGER)
                                                                           END AS policytermcode,*/
																			CASE
																			WHEN pp.cancellationdate_stg IS NOT NULL THEN
																				DATEDIFF(month, pp.periodstart_stg, pp.cancellationdate_stg)

																			WHEN pp.cancellationdate_stg IS NULL THEN
																				DATEDIFF(month, pp.periodstart_stg, pp.periodend_stg)
																			END AS policytermcode,

                                                                           ''0'' AS losscode,
                                                                           CASE
                                                                                           WHEN manhomeparkcode_alfa_stg IS NOT NULL THEN ''1''
                                                                                           ELSE ''2''
                                                                           END AS locationcode,
                                                                           cast(cast((
                                                                           CASE
                                                                                           WHEN pplh.typecode_stg NOT IN (''MH4'') THEN dw_limit
                                                                                           ELSE pp_limit
                                                                           END) AS NUMBER ) AS VARCHAR(255)) AS amountofinsurance,
                                                                           cast(
                                                                           CASE
                                                                                           WHEN pdh.yearbuilt_stg < ''1960'' THEN ''1959''
                                                                                           ELSE pdh.yearbuilt_stg
                                                                           END AS INTEGER)  AS yearofmanufacture,
                                                                           ''3''              AS tiedowncode,
                                                                           perils_limit_ind    ded_ind_code,
                                                                           perils_limit     AS deductible_amt,
                                                                           ''0''              AS deductibleindicatorws,
                                                                           ''0''              AS deductibleamountws,
                                                                           ''0''              AS claimidentifier,
                                                                           ''0''              AS claimantidentifier,
                                                                           /*CASE
                                                                                           WHEN phth.amount_stg < 0 THEN
                                                                                                           CASE
                                                                                                                           WHEN (
                                                                                                                                                           month(editeffectivedate_stg) =2
                                                                                                                                           AND             month(periodend_stg) =2
                                                                                                                                           AND             abs(cast(pp.editeffectivedate_stg AS DATE) - cast(pp.periodend_stg AS DATE)) BETWEEN 0 AND             29) THEN -1
                                                                                                                           WHEN (
                                                                                                                                                           month(editeffectivedate_stg) <>2
                                                                                                                                           AND             month(periodend_stg) <>2
                                                                                                                                           AND             abs(cast(pp.editeffectivedate_stg AS DATE) - cast(pp.periodend_stg AS DATE)) BETWEEN 0 AND             30) THEN -1
                                                                                                                           ELSE abs(cast((cast(pp.editeffectivedate_stg AS DATE)-cast(pp.periodend_stg AS DATE) month(4)) AS INTEGER) )*-1
                                                                                                           END
                                                                                           WHEN phth.amount_stg > 0 THEN
                                                                                                           CASE
                                                                                                                           WHEN (
                                                                                                                                                           month(editeffectivedate_stg) =2
                                                                                                                                           AND             month(periodend_stg) =2
                                                                                                                                           AND             abs(cast(pp.editeffectivedate_stg AS DATE) - cast(pp.periodend_stg AS DATE)) BETWEEN 0 AND             29) THEN 1
                                                                                                                           WHEN (
                                                                                                                                                           month(editeffectivedate_stg) <>2
                                                                                                                                           AND             month(periodend_stg) <>2
                                                                                                                                           AND             abs(cast(pp.editeffectivedate_stg AS DATE) - cast(pp.periodend_stg AS DATE)) BETWEEN 0 AND             30) THEN 1
                                                                                                                           ELSE abs(abs(cast((cast(pp.editeffectivedate_stg AS DATE)-cast(pp.periodend_stg AS DATE) month(4)) AS INTEGER) ))
                                                                                                           END
                                                                                           ELSE 0
                                                                           END AS writtenexposure, */
																		   CASE
																			WHEN phth.amount_stg < 0 THEN
																				CASE
																				WHEN MONTH(pp.editeffectivedate_stg) = 2
																					AND MONTH(pp.periodend_stg) = 2
																					AND ABS(DATEDIFF(day, pp.editeffectivedate_stg, pp.periodend_stg)) BETWEEN 0 AND 29
																					THEN -1

																				WHEN MONTH(pp.editeffectivedate_stg) <> 2
																					AND MONTH(pp.periodend_stg) <> 2
																					AND ABS(DATEDIFF(day, pp.editeffectivedate_stg, pp.periodend_stg)) BETWEEN 0 AND 30
																					THEN -1

																				ELSE -1 * ABS(DATEDIFF(month, pp.editeffectivedate_stg, pp.periodend_stg))
																				END

																			WHEN phth.amount_stg > 0 THEN
																				CASE
																				WHEN MONTH(pp.editeffectivedate_stg) = 2
																					AND MONTH(pp.periodend_stg) = 2
																					AND ABS(DATEDIFF(day, pp.editeffectivedate_stg, pp.periodend_stg)) BETWEEN 0 AND 29
																					THEN 1

																				WHEN MONTH(pp.editeffectivedate_stg) <> 2
																					AND MONTH(pp.periodend_stg) <> 2
																					AND ABS(DATEDIFF(day, pp.editeffectivedate_stg, pp.periodend_stg)) BETWEEN 0 AND 30
																					THEN 1

																				ELSE ABS(DATEDIFF(month, pp.editeffectivedate_stg, pp.periodend_stg))
																				END

																			ELSE 0
																			END AS writtenexposure,																	   
																		   
																		   /*(
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
                                                                                                                                AND    pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                              ''MH4'',
                                                                                                                                                                              ''MH7'',
                                                                                                                                                                              ''MH9'')
                                                                                                                                join   db_t_prod_stag.pc_job job2
                                                                                                                                ON     job2.id_stg = pc_policyperiod2.jobid_stg
                                                                                                                                join   db_t_prod_stag.pctl_job pctl_job2
                                                                                                                                ON     pctl_job2.id_stg = job2.subtype_stg
                                                                                                                                WHERE  pctl_job2.name_stg = ''Renewal''
                                                                                                                                AND    (
                                                                                                                                              pt.confirmationdate_alfa_stg > :PC_EOY
                                                                                                                                       OR     pt.confirmationdate_alfa_stg IS NULL)
                                                                                                                                AND    pc_policyperiod2.policynumber_stg = pp.policynumber_stg
                                                                                                                                AND    pc_policyperiod2.termnumber_stg = pp.termnumber_stg )) THEN 0
                                                                                           ELSE phth.amount_stg
                                                                           END) AS wrtnprem, */
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
																				AND pctl_hopolicytype_hoe.typecode_stg IN (''MH3'', ''MH4'', ''MH7'', ''MH9'')
																				JOIN db_t_prod_stag.pc_job job2
																				ON job2.id_stg = pc_policyperiod2.jobid_stg
																				JOIN db_t_prod_stag.pctl_job pctl_job2
																				ON pctl_job2.id_stg = job2.subtype_stg
																				WHERE pctl_job2.name_stg = ''Renewal''
																				AND (pt2.confirmationdate_alfa_stg > :PC_EOY OR pt2.confirmationdate_alfa_stg IS NULL)
																				AND pc_policyperiod2.policynumber_stg = pp.policynumber_stg
																				AND pc_policyperiod2.termnumber_stg = pp.termnumber_stg
																			) THEN 0
																			ELSE phth.amount_stg
																			END AS wrtnprem,

                                                                           ''0''  AS paidlosses,
                                                                           ''0''  AS paidnumberofclaims,
                                                                           ''0''  AS outstandinglosses,
                                                                           ''0''  AS outstandingnoofclaims,
                                                                           pp.policynumber_stg,
                                                                           cast(pp.publicid_stg AS VARCHAR(64)) AS policyperiodid,
                                                                           pj.jobnumber_stg                        policyidentifier,
                                                                           phth.id_stg
                                                           FROM            db_t_prod_stag.pcx_hotransaction_hoe phth
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
                                                           left join       db_t_prod_stag.pc_effectivedatedfields eff
                                                           ON              eff.branchid_stg = pp.id_stg
                                                           AND             eff.expirationdate_stg IS NULL
                                                           join            db_t_prod_stag.pcx_homeownerscost_hoe phch
                                                           ON              phth.homeownerscost_stg = phch.id_stg
                                                           left join       terr  -- using cte
                                                           ON              terr.id=pp.id_stg
                                                           join            db_t_prod_stag.pc_job pj
                                                           ON              pp.jobid_stg = pj.id_stg
                                                           join            db_t_prod_stag.pctl_job pcj
                                                           ON              pj.subtype_stg = pcj.id_stg
                                                           left join       db_t_prod_stag.pcx_holocation_hoe phh
                                                           ON              pdh.holocation_stg= phh.id_stg
                                                           join            db_t_prod_stag.pc_policyline ppl
                                                           ON              pp.id_stg = ppl.branchid_stg
                                                           AND             ppl.expirationdate_stg IS NULL
                                                           join            db_t_prod_stag.pctl_hopolicytype_hoe pplh
                                                           ON              ppl.hopolicytype_stg = pplh.id_stg
                                                           AND             pplh.typecode_stg IN (''MH3'',
                                                                                                 ''MH4'',
                                                                                                 ''MH7'',
                                                                                                 ''MH9'')
                                                           join            polcov   -- using cte
                                                           ON              to_number(polcov.branchid) = pp.id_stg
                                                           left join       db_t_prod_stag.pctl_policyperiodstatus pps
                                                           ON              pp.status_stg=pps.id_stg
                                                           join            db_t_prod_stag.pc_policyterm pt
                                                           ON              pt.id_stg = pp.policytermid_stg
                                                           AND
                                                                           CASE
                                                                                           WHEN pp.editeffectivedate_stg >= pp.modeldate_stg
                                                                                           AND             pp.editeffectivedate_stg>= coalesce(cast(pt.confirmationdate_alfa_stg AS timestamp), cast(''1900-01-01 00:00:00.000000'' AS timestamp)) THEN pp.editeffectivedate_stg
                                                                                           WHEN coalesce(cast(pt.confirmationdate_alfa_stg AS timestamp), cast(''1900-01-01 00:00:00.000000''AS timestamp)) >= pp.modeldate_stg THEN coalesce(cast(pt.confirmationdate_alfa_stg AS timestamp), cast(''1900-01-01 00:00:00.000000'' AS timestamp))
                                                                                           ELSE pp.modeldate_stg
                                                                           END BETWEEN :PC_BOY AND             :PC_EOY ) asa
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
                                           protectionclasscode,
                                           typeofdeductiblecode,
                                           policytermcode,
                                           losscode,
                                           locationcode,
                                           yearofmanufacture,
                                           tiedowncode,
                                           ded_ind_code,
                                           deductibleindicatorws,
                                           deductibleamountws,
                                           claimidentifier,
                                           claimantidentifier,
                                           writtenexposure,
                                           paidlosses,
                                           paidnumberofclaims,
                                           outstandinglosses,
                                           outstandingnoofclaims,
                                           policynumber_stg,
                                           policyperiodid,
                                           policyidentifier,
                                           amountofinsurance,
                                           deductible_amt) AS a
                  WHERE  wrtnprem <> ''0.00'' ) src ) );
  -- Component exp_policy_pass_through, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_policy_pass_through AS
  (
         SELECT sq_pc_policyperiod.companynumber         AS companynumber,
                sq_pc_policyperiod.lineofbusinesscode    AS lineofbusinesscode,
                sq_pc_policyperiod.statecode             AS statecode,
                sq_pc_policyperiod.callyear              AS callyear,
                sq_pc_policyperiod.accountingyear        AS accountingyear,
                sq_pc_policyperiod.expperiodyear         AS expperiodyear,
                sq_pc_policyperiod.expperiodmonth        AS expperiodmonth,
                sq_pc_policyperiod.expeperiodday         AS expeperiodday,
                sq_pc_policyperiod.coveragecode          AS coveragecode,
                sq_pc_policyperiod.territorycode         AS territorycode,
                sq_pc_policyperiod.zipcode               AS zipcode,
                sq_pc_policyperiod.policyeffectiveyear   AS policyeffectiveyear,
                sq_pc_policyperiod.newrecordformat       AS newrecordformat,
                sq_pc_policyperiod.aslob                 AS aslob,
                sq_pc_policyperiod.itemcode              AS itemcode,
                sq_pc_policyperiod.sublinecode           AS sublinecode,
                sq_pc_policyperiod.protectioncasscode    AS protectioncasscode,
                sq_pc_policyperiod.typeofdeductiblecode  AS typeofdeductiblecode,
                sq_pc_policyperiod.policytermcode        AS policytermcode,
                sq_pc_policyperiod.typeoflosscode        AS typeoflosscode,
                sq_pc_policyperiod.locationcode          AS locationcode,
                sq_pc_policyperiod.amountofinsurance     AS amountofinsurance,
                sq_pc_policyperiod.yeaofmanufacture      AS yeaofmanufacture,
                sq_pc_policyperiod.tiedowncode           AS tiedowncode,
                sq_pc_policyperiod.deductibleindicator   AS deductibleindicator,
                sq_pc_policyperiod.deductibleamount      AS deductibleamount,
                sq_pc_policyperiod.deductibleindicatorws AS deductibleindicatorws,
                sq_pc_policyperiod.deductibleamountws    AS deductibleamountws,
                sq_pc_policyperiod.claimidentifier       AS claimidentifier,
                sq_pc_policyperiod.claimantidentifier    AS claimantidentifier,
                sq_pc_policyperiod.writtenexposure       AS writtenexposure,
                sq_pc_policyperiod.writtenpremium        AS writtenpremium,
                sq_pc_policyperiod.paidlosses            AS paidlosses,
                sq_pc_policyperiod.paidnumberofclaims    AS paidnumberofclaims,
                sq_pc_policyperiod.outstandinglosses     AS outstandinglosses,
                sq_pc_policyperiod.outstandingnoofclaims AS outstandingnoofclaims,
                sq_pc_policyperiod.policynumber          AS policynumber,
                sq_pc_policyperiod.policyperiodid        AS policyperiodid,
                sq_pc_policyperiod.policyidentifier      AS policyidentifier,
                sq_pc_policyperiod.source_record_id
         FROM   sq_pc_policyperiod );
  -- Component exp_default, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_default AS
  (
         SELECT
                CASE
                       WHEN exp_policy_pass_through.companynumber IS NULL THEN ''0000''
                       ELSE lpad ( exp_policy_pass_through.companynumber , 4 , ''0'' )
                END                                        AS o_companynumber,
                exp_policy_pass_through.lineofbusinesscode AS lineofbusinesscode,
                CASE
                       WHEN exp_policy_pass_through.statecode IS NULL THEN ''00''
                       ELSE exp_policy_pass_through.statecode
                END                                    AS o_statecode,
                exp_policy_pass_through.callyear       AS callyear,
                exp_policy_pass_through.accountingyear AS accountingyear,
                exp_policy_pass_through.expperiodyear  AS expperiodyear,
                exp_policy_pass_through.expperiodmonth AS expperiodmonth,
                exp_policy_pass_through.expeperiodday  AS expeperiodday,
                exp_policy_pass_through.coveragecode   AS coveragecode,
                CASE
                       WHEN exp_policy_pass_through.territorycode IS NULL THEN ''00''
                       ELSE lpad ( exp_policy_pass_through.territorycode , 2 , ''0'' )
                END AS o_territorycode,
                CASE
                       WHEN exp_policy_pass_through.zipcode IS NULL THEN ''00000''
                       ELSE lpad ( exp_policy_pass_through.zipcode , 5 , ''0'' )
                END                                         AS o_zipcode,
                exp_policy_pass_through.policyeffectiveyear AS policyeffectiveyear,
                exp_policy_pass_through.newrecordformat     AS newrecordformat,
                exp_policy_pass_through.aslob               AS aslob,
                exp_policy_pass_through.itemcode            AS itemcode,
                exp_policy_pass_through.sublinecode         AS sublinecode,
                CASE
                       WHEN exp_policy_pass_through.protectioncasscode IS NULL THEN ''00''
                       ELSE lpad ( exp_policy_pass_through.protectioncasscode , 2 , ''0'' )
                END AS o_protectioncasscode,
                CASE
                       WHEN exp_policy_pass_through.typeofdeductiblecode IS NULL THEN ''00''
                       ELSE lpad ( exp_policy_pass_through.typeofdeductiblecode , 2 , ''0'' )
                END AS o_typeofdeductiblecode,
                CASE
                       WHEN exp_policy_pass_through.policytermcode IS NULL THEN ''00''
                       ELSE lpad ( exp_policy_pass_through.policytermcode , 2 , ''0'' )
                END AS o_policytermcode,
                CASE
                       WHEN exp_policy_pass_through.typeoflosscode IS NULL THEN ''00''
                       ELSE lpad ( exp_policy_pass_through.typeoflosscode , 2 , ''0'' )
                END                                  AS o_typeoflosscode,
                exp_policy_pass_through.locationcode AS locationcode,
                CASE
                       WHEN exp_policy_pass_through.amountofinsurance IS NULL THEN ''00000''
                       ELSE lpad ( exp_policy_pass_through.amountofinsurance , 5 , ''0'' )
                END AS o_amountofinsurance,
                CASE
                       WHEN exp_policy_pass_through.yeaofmanufacture IS NULL THEN ''0000''
                       ELSE exp_policy_pass_through.yeaofmanufacture
                END                                 AS o_yeaofmanufacture,
                exp_policy_pass_through.tiedowncode AS tiedowncode,
                CASE
                       WHEN exp_policy_pass_through.deductibleindicator IS NULL THEN ''0''
                       ELSE exp_policy_pass_through.deductibleindicator
                END AS o_deductibleindicator,
                CASE
                       WHEN exp_policy_pass_through.deductibleamount IS NULL THEN ''0000000''
                       ELSE lpad ( exp_policy_pass_through.deductibleamount , 7 , ''0'' )
                END                                           AS o_deductibleamount,
                exp_policy_pass_through.deductibleindicatorws AS deductibleindicatorws,
                CASE
                       WHEN exp_policy_pass_through.deductibleamountws IS NULL THEN ''0000000''
                       ELSE lpad ( exp_policy_pass_through.deductibleamountws , 7 , ''0'' )
                END AS o_deductibleamountws,
                CASE
                       WHEN exp_policy_pass_through.claimidentifier IS NULL THEN ''000000000000000''
                       ELSE lpad ( exp_policy_pass_through.claimidentifier , 15 , ''0'' )
                END AS o_claimidentifier,
                CASE
                       WHEN exp_policy_pass_through.claimantidentifier IS NULL THEN ''000''
                       ELSE lpad ( exp_policy_pass_through.claimantidentifier , 3 , ''0'' )
                END AS o_claimantidentifier,
                CASE
                       WHEN exp_policy_pass_through.writtenexposure IS NULL THEN ''000000000000''
                       ELSE exp_policy_pass_through.writtenexposure
                END AS o_writtenexposure,
                CASE
                       WHEN exp_policy_pass_through.writtenpremium IS NULL THEN ''000000000000''
                       ELSE rpad ( exp_policy_pass_through.writtenpremium , 12 , ''0'' )
                END AS o_writtenpremium,
                CASE
                       WHEN (
                                     exp_policy_pass_through.paidlosses IS NULL
                              OR     exp_policy_pass_through.paidlosses = ''0'' ) THEN ''000000000000''
                       ELSE exp_policy_pass_through.paidlosses
                END AS o_paidlosses,
                CASE
                       WHEN (
                                     exp_policy_pass_through.paidnumberofclaims IS NULL
                              OR     exp_policy_pass_through.paidnumberofclaims = ''0'' ) THEN ''000000000000''
                       ELSE exp_policy_pass_through.paidnumberofclaims
                END AS o_paidnumberofclaims,
                CASE
                       WHEN (
                                     exp_policy_pass_through.outstandinglosses IS NULL
                              OR     exp_policy_pass_through.outstandinglosses = ''0'' ) THEN ''000000000000''
                       ELSE exp_policy_pass_through.outstandinglosses
                END AS o_outstandinglosses,
                CASE
                       WHEN (
                                     exp_policy_pass_through.outstandingnoofclaims IS NULL
                              OR     exp_policy_pass_through.outstandingnoofclaims = ''0'' ) THEN ''000000000000''
                       ELSE lpad ( exp_policy_pass_through.outstandingnoofclaims , 12 , ''0'' )
                END                                  AS o_outstandingnoofclaims,
                exp_policy_pass_through.policynumber AS policynumber,
                CASE
                       WHEN exp_policy_pass_through.policyperiodid IS NULL THEN ''''
                       ELSE rpad ( exp_policy_pass_through.policyperiodid , 20 , ''0'' )
                END AS o_policyperiodid,
                CASE
                       WHEN exp_policy_pass_through.policyidentifier IS NULL THEN ''00000000000000000000''
                       ELSE lpad ( exp_policy_pass_through.policyidentifier , 20 , ''0'' )
                END               AS o_policyidentifier,
                current_timestamp AS creationts,
                ''0''               AS creationuid,
                current_timestamp AS updatets,
                ''0''               AS updateuid,
                exp_policy_pass_through.source_record_id
         FROM   exp_policy_pass_through );
  -- Component OUT_NAIIPCI_MH, Type TARGET
  INSERT INTO db_t_prod_comn.out_naiipci_mh
              (
                          companynumber,
                          lineofbusinesscode,
                          statecode,
                          callyear,
                          accountingyear,
                          expperiodyear,
                          expperiodmonth,
                          expeperiodday,
                          coveragecode,
                          territorycode,
                          zipcode,
                          policyeffectiveyear,
                          newrecordformat,
                          aslob,
                          itemcode,
                          sublinecode,
                          protectioncasscode,
                          typeofdeductiblecode,
                          policytermcode,
                          typeoflosscode,
                          locationcode,
                          amountofinsurance,
                          yeaofmanufacture,
                          tiedowncode,
                          deductibleindicator,
                          deductibleamount,
                          deductibleindicatorws,
                          deductibleamountws,
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
  SELECT exp_default.o_companynumber         AS companynumber,
         exp_default.lineofbusinesscode      AS lineofbusinesscode,
         exp_default.o_statecode             AS statecode,
         exp_default.callyear                AS callyear,
         exp_default.accountingyear          AS accountingyear,
         exp_default.expperiodyear           AS expperiodyear,
         exp_default.expperiodmonth          AS expperiodmonth,
         exp_default.expeperiodday           AS expeperiodday,
         exp_default.coveragecode            AS coveragecode,
         exp_default.o_territorycode         AS territorycode,
         exp_default.o_zipcode               AS zipcode,
         exp_default.policyeffectiveyear     AS policyeffectiveyear,
         exp_default.newrecordformat         AS newrecordformat,
         exp_default.aslob                   AS aslob,
         exp_default.itemcode                AS itemcode,
         exp_default.sublinecode             AS sublinecode,
         exp_default.o_protectioncasscode    AS protectioncasscode,
         exp_default.o_typeofdeductiblecode  AS typeofdeductiblecode,
         exp_default.o_policytermcode        AS policytermcode,
         exp_default.o_typeoflosscode        AS typeoflosscode,
         exp_default.locationcode            AS locationcode,
         exp_default.o_amountofinsurance     AS amountofinsurance,
         exp_default.o_yeaofmanufacture      AS yeaofmanufacture,
         exp_default.tiedowncode             AS tiedowncode,
         exp_default.o_deductibleindicator   AS deductibleindicator,
         exp_default.o_deductibleamount      AS deductibleamount,
         exp_default.deductibleindicatorws   AS deductibleindicatorws,
         exp_default.o_deductibleamountws    AS deductibleamountws,
         exp_default.o_claimidentifier       AS claimidentifier,
         exp_default.o_claimantidentifier    AS claimantidentifier,
         exp_default.o_writtenexposure       AS writtenexposure,
         exp_default.o_writtenpremium        AS writtenpremium,
         exp_default.o_paidlosses            AS paidlosses,
         exp_default.o_paidnumberofclaims    AS paidnumberofclaims,
         exp_default.o_outstandinglosses     AS outstandinglosses,
         exp_default.o_outstandingnoofclaims AS outstandingnoofclaims,
         exp_default.policynumber            AS policynumber,
         exp_default.o_policyperiodid        AS policyperiodid,
         exp_default.creationts              AS creationts,
         exp_default.creationuid             AS creationuid,
         exp_default.updatets                AS updatets,
         exp_default.updateuid               AS updateuid,
         exp_default.o_policyidentifier      AS policyidentifier
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
                $1  AS policysystemperiodid,
                $2  AS companynumber,
                $3  AS lineofbusinesscode,
                $4  AS statecode,
                $5  AS callyear,
                $6  AS accountingyear,
                $7  AS expperiodyear,
                $8  AS expperiodmonth,
                $9  AS expeperiodday,
                $10 AS coveragecode,
                $11 AS territorycode,
                $12 AS zipcode,
                $13 AS policyeffectiveyear,
                $14 AS newrecordformat,
                $15 AS aslob,
                $16 AS itemcode,
                $17 AS sublinecode,
                $18 AS protectioncasscode,
                $19 AS typeofdeductiblecode,
                $20 AS policytermcode,
                $21 AS typeoflosscode,
                $22 AS locationcode,
                $23 AS amountofinsurance,
                $24 AS yeaofmanufacture,
                $25 AS tiedowncode,
                $26 AS deductibleindicator,
                $27 AS deductibleamount,
                $28 AS deductibleindicatorws,
                $29 AS deductibleamountws,
                $30 AS claimidentifier,
                $31 AS claimantidentifier,
                $32 AS writtenexposure,
                $33 AS writtenpremium,
                $34 AS paidlosses,
                $35 AS paidnumberofclaims,
                $36 AS outstandinglosses,
                $37 AS outstandingnoofclaims,
                $38 AS policynumber,
                $39 AS policyperiodid,
                $40 AS policyidentifier,
                $41 AS territorycode_pc,
                $42 AS zipcode_pc,
                $43 AS protectioncasscode_pc,
                $44 AS typeofdeductiblecode_pc,
                $45 AS amountofinsurance_pc,
                $46 AS yearofmanufacture_pc,
                $47 AS deductibleindicator_pc,
                $48 AS deductibleamount_pc,
                $49 AS territorycode_lkppc,
                $50 AS zipcode_lkppc,
                $51 AS protectioncasscode_lkppc,
                $52 AS typeofdeductiblecode_lkppc,
                $53 AS amountofinsurance_lkppc,
                $54 AS yearofmanufacture_lkppc,
                $55 AS deductibleindicator_lkppc,
                $56 AS deductibleamount_lkppc,
                $57 AS source_record_id
         FROM   (
                         SELECT   src.*,
                                  row_number() over (ORDER BY 1) AS source_record_id
                         FROM     (
                                            SELECT    c_claim.policysystemperiodid AS policysystemperiodid,
                                                      c_claim.companynumber        AS companynumber,
                                                      c_claim.lob                  AS lineofbusinesscode,
                                                      c_claim.statecode            AS statecode,
                                                      c_claim.callyear             AS callyear,
                                                      c_claim.accountingyear       AS accountingyear,
                                                      c_claim.exp_yr               AS expperiodyear,
                                                      c_claim.exp_mth              AS expperiodmonth,
                                                      c_claim.exp_day              AS expeperiodday,
                                                      CASE
                                                                WHEN cov=''sec'' THEN ''07''
                                                                ELSE ''01''
                                                      END                                 AS coveragecode,
                                                      c_claim.territorycode               AS territorycode,
                                                      c_claim.zipcode                     AS zipcode,
                                                      c_claim.policy_eff_yr               AS policyeffectiveyear,
                                                      c_claim.newrecordformat             AS newrecordformat,
                                                      c_claim.aslob                       AS aslob,
                                                      c_claim.itemcode                    AS itemcode,
                                                      c_claim.sublinecode                 AS sublinecode,
                                                      c_claim.dwellingprotectionclasscode AS protectioncasscode,
                                                      c_claim.typeofdeductiblecode        AS typeofdeductiblecode,
                                                      c_claim.policytermcode              AS policytermcode,
                                                      c_claim.losscause                   AS typeoflosscode,
                                                      c_claim.locationcode                AS locationcode,
                                                      c_claim.amountofinsurance           AS amountofinsurance,
                                                      c_claim.yearofmanufacture           AS yeaofmanufacture,
                                                      c_claim.tiedowncode                 AS tiedowncode,
                                                      c_claim.deductibleindicator         AS deductibleindicator,
                                                      c_claim.deductibleamount            AS deductibleamount,
                                                      c_claim.deductibleindicatorws       AS deductibleindicatorws,
                                                      c_claim.deductibleamountws          AS deductibleamountws,
                                                      c_claim.claimidentifier             AS claimidentifier,
                                                      c_claim.claimantidentifier          AS claimantidentifier,
                                                      c_claim.writtenexposure             AS writtenexposure,
                                                      c_claim.wrtnprem                    AS writtenpremium,
                                                      CASE
                                                                WHEN c_claim.paidloss = cast(''0.00'' AS VARCHAR(20)) THEN ''.00''
                                                                ELSE cast(c_claim.paidloss AS          VARCHAR(20))
                                                      END                AS paidlosses,
                                                      c_claim.paidclaims AS paidnumberofclaims,
                                                      CASE
                                                                WHEN c_claim.outstandinglosses = cast(''0.00'' AS VARCHAR(20)) THEN ''.00''
                                                                ELSE cast(c_claim.outstandinglosses AS          VARCHAR(20))
                                                      END                            AS outstandinglosses,
                                                      c_claim.outstandingclaims      AS outstandingnoofclaims,
                                                      c_claim.policynumber           AS policynumber,
                                                      c_claim.policyperiodid         AS policyperiodid,
                                                      c_claim.policyidentifier       AS policyidentifier,
                                                      policy1.territorycode          AS territorycode_pc,
                                                      policy1.zipcode                AS zipcode_pc,
                                                      policy1.protectioncasscode     AS protectioncasscode_pc,
                                                      policy1.typeofdeductiblecode   AS typeofdeductiblecode_pc,
                                                      policy1.amountofinsurance      AS amountofinsurance_pc,
                                                      policy1.yeaofmanufacture       AS yearofmanufacture_pc,
                                                      policy1.deductibleindicator    AS deductibleindicator_pc,
                                                      policy1.deductibleamount       AS deductibleamount_pc,
                                                      lkp_query.territorycode        AS territorycode_lkppc,
                                                      lkp_query.zipcode              AS zipcode_lkppc,
                                                      lkp_query.protectioncasscode   AS protectioncasscode_lkppc,
                                                      lkp_query.typeofdeductiblecode AS typeofdeductiblecode_lkppc,
                                                      lkp_query.amountofinsurance    AS amountofinsurance_lkppc,
                                                      lkp_query.yeaofmanufacture     AS yearofmanufacture_lkppc,
                                                      lkp_query.deductibleindicator  AS deductibleindicator_lkppc,
                                                      lkp_query.deductibleamount     AS deductibleamount_lkppc
                                            FROM      (
                                                      (
                                                             SELECT *
                                                             FROM   (
                                                                             SELECT   policysystemperiodid_stg AS policysystemperiodid,
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
                                                                                      policy_eff_yr,
                                                                                      newrecordformat,
                                                                                      aslob,
                                                                                      itemcode,
                                                                                      sublinecode,
                                                                                      dwellingprotectionclasscode_stg AS dwellingprotectionclasscode,
                                                                                      typeofdeductiblecode,
                                                                                      policytermcode,
                                                                                      losscause,
                                                                                      locationcode,
                                                                                      amountofinsurance,
                                                                                      yearofmanufacture,
                                                                                      tiedowncode,
                                                                                      deductibleindicator,
                                                                                      deductibleamount,
                                                                                      deductibleindicatorws,
                                                                                      deductibleamountws,
                                                                                      claimidentifier,
                                                                                      claimantidentifier,
                                                                                      writtenexposure,
                                                                                      wrtnprem,
                                                                                      SUM(paidloss) paidloss,
                                                                                      CASE
                                                                                               WHEN(
                                                                                                                 max(closedate) > cast(:CC_BOY AS timestamp)
                                                                                                        AND      max(closedate) < cast(:CC_EOY AS timestamp)
                                                                                                        AND      SUM(paidloss) > 0
                                                                                                        AND      max(covrank) >= 1) THEN 1
                                                                                               ELSE 0
                                                                                      END         AS paidclaims,
                                                                                      SUM(outres)    outstandinglosses,
                                                                                      CASE
                                                                                               WHEN(
                                                                                                                 max(closedate) IS NULL
                                                                                                        OR       max(closedate) > cast(:CC_EOY AS timestamp) )
                                                                                               AND      SUM (outres)>0
                                                                                               AND      max(covrank) >= 1 THEN 1
                                                                                               ELSE 0
                                                                                      END              AS outstandingclaims,
                                                                                      policynumber_stg AS policynumber,
                                                                                      policyperiodid,
                                                                                      policyidentifier,
                                                                                      effectivedate
                                                                             FROM     (
                                                                                               SELECT   policysystemperiodid_stg,
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
                                                                                                        policy_eff_yr,
                                                                                                        newrecordformat,
                                                                                                        aslob,
                                                                                                        itemcode,
                                                                                                        sublinecode,
                                                                                                        dwellingprotectionclasscode_stg,
                                                                                                        typeofdeductiblecode,
                                                                                                        policytermcode,
                                                                                                        losscause,
                                                                                                        locationcode,
                                                                                                        amountofinsurance,
                                                                                                        yearofmanufacture,
                                                                                                        tiedowncode,
                                                                                                        deductibleindicator,
                                                                                                        deductibleamount,
                                                                                                        deductibleindicatorws,
                                                                                                        deductibleamountws,
                                                                                                        claimidentifier,
                                                                                                        claimantidentifier,
                                                                                                        writtenexposure,
                                                                                                        wrtnprem,
                                                                                                        policynumber_stg,
                                                                                                        policyperiodid,
                                                                                                        policyidentifier,
                                                                                                        policysubtype,
                                                                                                        effectivedate,
                                                                                                        covrank,
                                                                                                        closedate,
                                                                                                        SUM(acct500104 - acct500204 + acct500214 - acct500304 + acct500314) AS paidloss,
                                                                                                        SUM(acct521004)                                                     AS paidalae,
                                                                                                        SUM(a."[Outstanding Reserves as of EOM]"+a.losspaymentscurrmo)      AS outres
                                                                                               FROM     (
                                                                                                                        SELECT DISTINCT policysystemperiodid_stg,
                                                                                                                                        CASE
                                                                                                                                                        WHEN uwc.typecode_stg=''AMI'' THEN ''0005''
                                                                                                                                                        ELSE ''0050''
                                                                                                                                        END  AS companynumber,
                                                                                                                                        ''18'' AS lob,
                                                                                                                                        CASE
                                                                                                                                                        WHEN jd.typecode_stg=''AL'' THEN ''01''
                                                                                                                                                        WHEN jd.typecode_stg=''GA'' THEN ''10''
                                                                                                                                                        WHEN jd.typecode_stg=''MS'' THEN ''23''
                                                                                                                                        END                                                               statecode,
                                                                                                                                        extract(year FROM cast(:CC_EOY AS timestamp) + interval ''1 year'') callyear,
                                                                                                                                        extract(year FROM cast(:CC_EOY AS timestamp))                     accountingyear,
                                                                                                                                        extract(year FROM clm.lossdate_stg)                               exp_yr,
                                                                                                                                       /* right(''00''
                                                                                                                                                        || (trim(extract(month FROM cast(clm.lossdate_stg AS timestamp)) (format ''99''))),2) AS exp_mth,
                                                                                                                                        right(''00''
                                                                                                                                                        || (trim(extract(day FROM cast(clm.lossdate_stg AS timestamp)) (format ''99''))),2) AS exp_day,*/
																																		LPAD(TRIM(EXTRACT(month FROM CAST(clm.lossdate_stg AS TIMESTAMP))::STRING), 2, ''0'') AS exp_mth,
																																		LPAD(TRIM(EXTRACT(day FROM CAST(clm.lossdate_stg AS TIMESTAMP))::STRING), 2, ''0'') AS exp_day,

                                                                                                                                        ''01''                                                                                                 cvge_code,
                                                                                                                                        coalesce(cc_pl.naiipcitc_alfa_stg,(
                                                                                                                                        CASE
                                                                                                                                                        WHEN c_st.typecode_stg=''AL''
                                                                                                                                                        AND             upper(addr.city_stg)= ''BIRMINGHAM''
                                                                                                                                                        AND             upper(addr.county_stg)=''JEFFERSON'' THEN ''32''
                                                                                                                                                        WHEN c_st.typecode_stg=''AL''
                                                                                                                                                        AND             upper(addr.city_stg)= ''HUNTSVILLE''
                                                                                                                                                        AND             upper(addr.county_stg)=''MADISON'' THEN ''35''
                                                                                                                                                        WHEN c_st.typecode_stg=''AL''
                                                                                                                                                        AND             upper(addr.city_stg)= ''MONTGOMERY''
                                                                                                                                                        AND             upper(addr.county_stg)=''MONTGOMERY'' THEN ''37''
                                                                                                                                                        WHEN c_st.typecode_stg=''AL''
                                                                                                                                                        AND             upper(addr.city_stg)= ''MOBILE''
                                                                                                                                                        AND             upper(addr.county_stg)=''MOBILE'' THEN ''30''
                                                                                                                                                        WHEN c_st.typecode_stg=''AL''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''AUTAUGA'',
                                                                                                                                                                                                   ''ELMORE'',
                                                                                                                                                                                                   ''MONTGOMERY'') THEN ''38''
                                                                                                                                                        WHEN c_st.typecode_stg=''AL''
                                                                                                                                                        AND             upper(addr.county_stg)=''BALDWIN'' THEN ''41''
                                                                                                                                                        WHEN c_st.typecode_stg=''AL''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''BARBOUR'',
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
                                                                                                                                                                                                   ''SUMTER'',
                                                                                                                                                                                                   ''TALLADEGA'',
                                                                                                                                                                                                   ''TALLAPOOSA'',
                                                                                                                                                                                                   ''WASHINGTON'',
                                                                                                                                                                                                   ''WILCOX'',
                                                                                                                                                                                                   ''WINSTON'') THEN ''41''
                                                                                                                                                        WHEN c_st.typecode_stg=''AL''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''CALHOUN'',
                                                                                                                                                                                                   ''ETOWAH'') THEN ''40''
                                                                                                                                                        WHEN c_st.typecode_stg=''AL''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''COLBERT'',
                                                                                                                                                                                                   ''LAUDERDALE'',
                                                                                                                                                                                                   ''LIMESTONE'',
                                                                                                                                                                                                   ''MADISON'',
                                                                                                                                                                                                   ''MORGAN'') THEN ''36''
                                                                                                                                                        WHEN c_st.typecode_stg=''AL''
                                                                                                                                                        AND             upper(addr.county_stg)=''JEFFERSON'' THEN ''33''
                                                                                                                                                        WHEN c_st.typecode_stg=''AL''
                                                                                                                                                        AND             upper(addr.county_stg)=''MOBILE'' THEN ''41''
                                                                                                                                                        WHEN c_st.typecode_stg=''AL''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''SHELBY'',
                                                                                                                                                                                                   ''WALKER'') THEN ''34''
                                                                                                                                                        WHEN c_st.typecode_stg=''AL''
                                                                                                                                                        AND             upper(addr.county_stg)=''TUSCALOOSA'' THEN ''39''
                                                                                                                                                        WHEN c_st.typecode_stg=''AL''
                                                                                                                                                        AND             (
                                                                                                                                                                                        substring (addr.postalcode_stg,0,position(''-'' IN addr.postalcode_stg))=''36511''
                                                                                                                                                                        OR              addr.postalcode_stg=''36511'')
                                                                                                                                                        AND             upper(addr.city_stg)= ''BON SECOUR'' THEN ''6''
                                                                                                                                                        WHEN c_st.typecode_stg=''AL''
                                                                                                                                                        AND             (
                                                                                                                                                                                        substring (addr.postalcode_stg,0,position(''-'' IN addr.postalcode_stg))=''36528''
                                                                                                                                                                        OR              addr.postalcode_stg=''36528'')
                                                                                                                                                        AND             upper(addr.city_stg)= ''DAUPHIN ISLAND'' THEN ''6''
                                                                                                                                                        WHEN c_st.typecode_stg=''AL''
                                                                                                                                                        AND             (
                                                                                                                                                                                        substring (addr.postalcode_stg,0,position(''-'' IN addr.postalcode_stg)) IN (''36542'',
                                                                                                                                                                                                        ''36547'')
                                                                                                                                                                        OR              addr.postalcode_stg IN (''36542'',
                                                                                                                                                                                                        ''36547''))
                                                                                                                                                        AND             upper(addr.city_stg)= ''GULF SHORES'' THEN ''6''
                                                                                                                                                        WHEN c_st.typecode_stg=''AL''
                                                                                                                                                        AND             (
                                                                                                                                                                                        substring (addr.postalcode_stg,0,position(''-'' IN addr.postalcode_stg))=''36561''
                                                                                                                                                                        OR              addr.postalcode_stg=''36561'')
                                                                                                                                                        AND             upper(addr.city_stg)= ''ORANGE BEACH'' THEN ''6''
                                                                                                                                                        WHEN c_st.typecode_stg=''MS''
                                                                                                                                                        AND             upper(addr.city_stg)= ''JACKSON''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''HINDS'',
                                                                                                                                                                                                   ''RANKIN'') THEN ''30''
                                                                                                                                                        WHEN c_st.typecode_stg=''MS''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''AMITE'',
                                                                                                                                                                                                   ''FORREST'',
                                                                                                                                                                                                   ''GREENE'',
                                                                                                                                                                                                   ''LAMAR'',
                                                                                                                                                                                                   ''MARION'',
                                                                                                                                                                                                   ''PERRY'',
                                                                                                                                                                                                   ''PIKE'',
                                                                                                                                                                                                   ''WALTHALL'',
                                                                                                                                                                                                   ''WILKINSON'') THEN ''3''
                                                                                                                                                        WHEN c_st.typecode_stg=''MS''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''GEORGE'',
                                                                                                                                                                                                   ''PEARL RIVER'',
                                                                                                                                                                                                   ''STONE'') THEN ''5''
                                                                                                                                                        WHEN c_st.typecode_stg=''MS''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''HANCOCK'',
                                                                                                                                                                                                   ''HARRISON'',
                                                                                                                                                                                                   ''JACKSON'') THEN ''6''
                                                                                                                                                        WHEN c_st.typecode_stg=''MS''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''HINDS'',
                                                                                                                                                                                                   ''MADISON'',
                                                                                                                                                                                                   ''RANKIN'') THEN ''31''
                                                                                                                                                        WHEN c_st.typecode_stg=''MS''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''ADAMS'',
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
                                                                                                                                                        WHEN c_st.typecode_stg=''GA''
                                                                                                                                                        AND             upper(addr.city_stg)= ''ATLANTA''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''DE KALB'',
                                                                                                                                                                                                   ''FULTON'') THEN ''32''
                                                                                                                                                        WHEN c_st.typecode_stg=''GA''
                                                                                                                                                        AND             upper(addr.city_stg)= ''MACON''
                                                                                                                                                        AND             upper(addr.county_stg)=''BIBB'' THEN ''35''
                                                                                                                                                        WHEN c_st.typecode_stg=''GA''
                                                                                                                                                        AND             upper(addr.city_stg)= ''SAVANNAH''
                                                                                                                                                        AND             upper(addr.county_stg)=''CHATHAM'' THEN ''30''
                                                                                                                                                        WHEN c_st.typecode_stg=''GA''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''BRYAN'',
                                                                                                                                                                                                   ''CAMDEN'',
                                                                                                                                                                                                   ''CHATHAM'',
                                                                                                                                                                                                   ''GLYNN'',
                                                                                                                                                                                                   ''LIBERTY'',
                                                                                                                                                                                                   ''MCINTOSH'') THEN ''31''
                                                                                                                                                        WHEN c_st.typecode_stg=''GA''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''DE KALB'',
                                                                                                                                                                                                   ''DE KALB'',
                                                                                                                                                                                                   ''FULTON'') THEN ''33''
                                                                                                                                                        WHEN c_st.typecode_stg=''GA''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''CLAYTON'',
                                                                                                                                                                                                   ''COBB'',
                                                                                                                                                                                                   ''GWINNETT'') THEN ''34''
                                                                                                                                                        WHEN c_st.typecode_stg=''GA''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''CATOOSA'',
                                                                                                                                                                                                   ''WALKER'',
                                                                                                                                                                                                   ''WHITFIELD'') THEN ''36''
                                                                                                                                                        WHEN c_st.typecode_stg=''GA''
                                                                                                                                                        AND             upper(addr.county_stg) =''RICHMOND'' THEN ''37''
                                                                                                                                                        WHEN c_st.typecode_stg=''GA''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''CHATTAHOOCHEE'',
                                                                                                                                                                                                   ''MUSCOGEE'') THEN ''38''
                                                                                                                                                        WHEN c_st.typecode_stg=''GA''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''BUTTS'',
                                                                                                                                                                                                   ''CHEROKEE'',
                                                                                                                                                                                                   ''DOUGLAS'',
                                                                                                                                                                                                   ''FAYETTE'',
                                                                                                                                                                                                   ''FORSYTH'',
                                                                                                                                                                                                   ''HENRY'',
                                                                                                                                                                                                   ''NEWTON'',
                                                                                                                                                                                                   ''PAULDING'',
                                                                                                                                                                                                   ''ROCKDALE'',
                                                                                                                                                                                                   ''WALTON'') THEN ''39''
                                                                                                                                                        WHEN c_st.typecode_stg=''GA''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''BALDWIN'',
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
                                                                                                                                                        WHEN c_st.typecode_stg=''GA''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''BAKER'',
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
                                                                                                                                                        WHEN c_st.typecode_stg=''GA''
                                                                                                                                                        AND             upper(addr.county_stg) IN (''APPLING'',
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
                                                                                                                                        END))                                                   AS territorycode,
                                                                                                                                        left(cast(addr.postalcodedenorm_stg AS VARCHAR(255)),5) AS zipcode,
                                                                                                                                        extract(year FROM pol.effectivedate_stg)                AS policy_eff_yr,
                                                                                                                                        ''D''                                                     AS newrecordformat,
                                                                                                                                        ''040''                                                   AS aslob,
                                                                                                                                        ''03''                                                    AS itemcode,
                                                                                                                                        ''49''                                                    AS sublinecode,
                                                                                                                                        rsk.dwellingprotectionclasscode_stg,
                                                                                                                                        CASE
                                                                                                                                                        WHEN det.hurricane=''HODW_Hurricane_Ded_HOE'' THEN ''03''
                                                                                                                                                        WHEN det.windhail=''HODW_WindHail_Ded_HOE'' THEN ''03''
                                                                                                                                                        WHEN det.windstormhailexcl=''HODW_SectionI_DedWindAndHailExcl_alfa'' THEN ''07''
                                                                                                                                                        ELSE ''05''
                                                                                                                                        END AS typeofdeductiblecode,
                                                                                                                                        ''0'' AS policytermcode,
                                                                                                                                        CASE
                                                                                                                                                        WHEN lc.name_stg IN (''Fire - Total / Other'',
                                                                                                                                                                             ''Fire - Partial / Other'',
                                                                                                                                                                             ''Fire '',
                                                                                                                                                                             ''Total Fire'',
                                                                                                                                                                             ''Fire - Partial / Lightning'',
                                                                                                                                                                             ''Fire - Total / Lightning'',
                                                                                                                                                                             ''Lightning'') THEN ''01''
                                                                                                                                                        WHEN lc.name_stg IN (''Wind'',
                                                                                                                                                                             ''Hail'',
                                                                                                                                                                             ''EC'',
                                                                                                                                                                             ''Earthquake'') THEN ''02''
                                                                                                                                                        WHEN lc.name_stg IN (''Water / Frozen Pipes'',
                                                                                                                                                                             ''Water / Other'') THEN ''03''
                                                                                                                                                        WHEN lc.name_stg =''Theft'' THEN ''04''
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
                                                                                                                                                        WHEN lc.name_stg IN (''Pharmacists Liability '',
                                                                                                                                                                             ''Pharmacists Liability '') THEN ''08''
                                                                                                                                                        ELSE ''05''
                                                                                                                                        END AS losscause,
                                                                                                                                        CASE
                                                                                                                                                        WHEN rsk.locationcode_alfa_stg IS NOT NULL THEN ''1''
                                                                                                                                                        ELSE ''2''
                                                                                                                                        END        AS locationcode,
                                                                                                                                        det.amount AS amountofinsurance,
                                                                                                                                        cast(
                                                                                                                                        CASE
                                                                                                                                                        WHEN rsk.yearbuilt_alfa_stg <1960 THEN ''1959''
                                                                                                                                                        ELSE rsk.yearbuilt_alfa_stg
                                                                                                                                        END AS INTEGER)                                         AS yearofmanufacture,
                                                                                                                                        ''3''                                                     AS tiedowncode,
                                                                                                                                        ded_ind                                                 AS deductibleindicator,
                                                                                                                                        ded_amt                                                 AS deductibleamount,
                                                                                                                                        ''0''                                                     AS deductibleindicatorws,
                                                                                                                                        ''0''                                                     AS deductibleamountws,
                                                                                                                                        clm.claimnumber_stg                                     AS claimidentifier,
                                                                                                                                        left(cast(exps.claimantdenormid_stg AS VARCHAR(255)),3) AS claimantidentifier,
                                                                                                                                        ''0''                                                     AS writtenexposure,
                                                                                                                                        ''0''                                                     AS wrtnprem,
                                                                                                                                        pol.policynumber_stg,
                                                                                                                                        policysystemperiodid_stg AS policyperiodid,
                                                                                                                                        ''0''                         policyidentifier,
                                                                                                                                        addr.city_stg,
                                                                                                                                        psttl.typecode_stg                                                           AS policysubtype,
                                                                                                                                        lossdate_stg                                                                    effectivedate,
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
                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                        AND             (
                                                                                                                                                                                                        (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp))
                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                        AND             rctl.name_stg IS NULL
                                                                                                                                                                        AND             cctl.name_stg=''Loss''
                                                                                                                                                                        AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                        AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                        AND             lctl.name_stg = ''Deductible Refund''
                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                        AND             (
                                                                                                                                                                                                        (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp))
                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                        AND             rctl.name_stg IS NULL
                                                                                                                                                                        AND             cctl.name_stg=''Loss''
                                                                                                                                                                        AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                        AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                        AND             lctl.name_stg = ''Former Deductible''
                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                        AND             (
                                                                                                                                                                                                        (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp))
                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                        AND             rctl.name_stg IS NULL
                                                                                                                                                                        AND             cctl.name_stg=''Loss''
                                                                                                                                                                        AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                        AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                        AND             lctl.name_stg = ''Loss''
                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                        AND             (
                                                                                                                                                                                                        (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp))
                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                                        /* EIM-46304 - DV CHANGES */
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                        AND             rctl.name_stg IS NULL
                                                                                                                                                                        AND             cctl.name_stg=''Loss''
                                                                                                                                                                        AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                        AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                        AND             lctl.name_stg = ''Diminished Value''
                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                        AND             (
                                                                                                                                                                                                        (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp))
                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                        AND             rctl.name_stg = ''Credit to loss''
                                                                                                                                                                        AND             cctl.name_stg=''Loss''
                                                                                                                                                                        AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                        AND             lctl.name_stg = ''Deductible''
                                                                                                                                                                        AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                        AND             rctl.name_stg = ''Credit to loss''
                                                                                                                                                                        AND             cctl.name_stg=''Loss''
                                                                                                                                                                        AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                        AND             lctl.name_stg = ''Loss''
                                                                                                                                                                        AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                        AND             rctl.name_stg = ''Credit to loss''
                                                                                                                                                                        AND             cctl.name_stg=''Loss''
                                                                                                                                                                        AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                        AND             lctl.name_stg = ''Recovery''
                                                                                                                                                                        AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                        ELSE 0
                                                                                                                                        END) AS acct500104, (
                                                                                                                                        CASE
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                        AND             rctl.name_stg = ''Salvage''
                                                                                                                                                                        AND             cctl.name_stg=''Loss''
                                                                                                                                                                        AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                        AND             lctl.name_stg = ''Recovery''
                                                                                                                                                                        AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg
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
                                                                                                                                                                                                        txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp))
                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        cast(ch.updatetime_stg AS DATE) >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                        AND             cast(ch.updatetime_stg AS DATE) <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                        AND             rctl.name_stg = ''Credit to expense''
                                                                                                                                                                        AND             cctl.name_stg=''Expense''
                                                                                                                                                                        AND             cttl.name_stg = ''Expense''
                                                                                                                                                                        AND             lctl.name_stg = ''Salvage Expense''
                                                                                                                                                                        AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                        AND             rctl.name_stg = ''Salvage''
                                                                                                                                                                        AND             cctl.name_stg=''Expense''
                                                                                                                                                                        AND             cttl.name_stg = ''Expense''
                                                                                                                                                                        AND             lctl.name_stg = ''Salvage Expense''
                                                                                                                                                                        AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                        AND             rctl.name_stg = ''Salvage''
                                                                                                                                                                        AND             cctl.name_stg=''Expense''
                                                                                                                                                                        AND             cttl.name_stg = ''Expense''
                                                                                                                                                                        AND             lctl.name_stg = ''Recovery''
                                                                                                                                                                        AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                        ELSE 0
                                                                                                                                        END) AS acct500214, (
                                                                                                                                        CASE
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                        AND             rctl.name_stg = ''Subrogation''
                                                                                                                                                                        AND             cctl.name_stg=''Loss''
                                                                                                                                                                        AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                        AND             lctl.name_stg = ''Recovery''
                                                                                                                                                                        AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg
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
                                                                                                                                                                                                        txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp))
                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                        AND             rctl.name_stg = ''Credit to expense''
                                                                                                                                                                        AND             cctl.name_stg=''Expense''
                                                                                                                                                                        AND             cttl.name_stg = ''Expense''
                                                                                                                                                                        AND             lctl.name_stg = ''Subrogation Expense''
                                                                                                                                                                        AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                        AND             rctl.name_stg = ''Subrogation''
                                                                                                                                                                        AND             cctl.name_stg=''Expense''
                                                                                                                                                                        AND             cttl.name_stg = ''Expense''
                                                                                                                                                                        AND             lctl.name_stg = ''Subrogation Expense''
                                                                                                                                                                        AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                        AND             rctl.name_stg = ''Subrogation''
                                                                                                                                                                        AND             cctl.name_stg=''Expense''
                                                                                                                                                                        AND             cttl.name_stg = ''Expense''
                                                                                                                                                                        AND             lctl.name_stg = ''Recovery''
                                                                                                                                                                        AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
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
                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                        AND             (
                                                                                                                                                                                                        (
                                                                                                                                                                                                        txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp))
                                                                                                                                                                                        OR              (
                                                                                                                                                                                                        ch.issuedate_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)))) THEN txli.transactionamount_stg
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Recovery''
                                                                                                                                                                        AND             rctl.name_stg = ''Credit to expense''
                                                                                                                                                                        AND             cctl.name_stg=''Expense''
                                                                                                                                                                        AND             cttl.name_stg = ''Expense''
                                                                                                                                                                        AND             lctl.name_stg = ''Legal - Defense''
                                                                                                                                                                        AND             txli.createtime_stg >= cast(:CC_BOY AS timestamp)
                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg*-1
                                                                                                                                                        ELSE 0
                                                                                                                                        END) AS acct521004,
                                                                                                                                        CASE
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Reserve''
                                                                                                                                                                        AND             rctl.name_stg IS NULL
                                                                                                                                                                        AND             cctl.name_stg=''Loss''
                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN txli.transactionamount_stg
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
                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp) ) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                        AND             rctl.name_stg IS NULL
                                                                                                                                                                        AND             cctl.name_stg=''Loss''
                                                                                                                                                                        AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                        AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                        AND             lctl.name_stg = ''Deductible Refund''
                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp) ) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                        AND             rctl.name_stg IS NULL
                                                                                                                                                                        AND             cctl.name_stg=''Loss''
                                                                                                                                                                        AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                        AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                        AND             lctl.name_stg = ''Former Deductible''
                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp) ) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                        AND             rctl.name_stg IS NULL
                                                                                                                                                                        AND             cctl.name_stg=''Loss''
                                                                                                                                                                        AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                        AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                        AND             lctl.name_stg= ''Loss''
                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                                        /* EIM-46304 - DV CHANGES */
                                                                                                                                                        WHEN (
                                                                                                                                                                                        txtl.name_stg=''Payment''
                                                                                                                                                                        AND             rctl.name_stg IS NULL
                                                                                                                                                                        AND             cctl.name_stg=''Loss''
                                                                                                                                                                        AND             cttl.name_stg = ''Paid Loss''
                                                                                                                                                                        AND             pmtl.name_stg <> ''Expense Withheld''
                                                                                                                                                                        AND             lctl.name_stg= ''Diminished Value''
                                                                                                                                                                        AND             ch.issuedate_stg <= cast(:CC_EOY AS timestamp)
                                                                                                                                                                        AND             txli.createtime_stg <= cast(:CC_EOY AS timestamp)) THEN (tx.doesnoterodereserves_stg-1)*txli.transactionamount_stg
                                                                                                                                                        ELSE 0
                                                                                                                                        END AS losspaymentscurrmo,
                                                                                                                                        txli.id_stg
                                                                                                                        FROM            db_t_prod_stag.cc_claim clm
                                                                                                                        left join       db_t_prod_stag.cc_policy pol
                                                                                                                        ON              clm.policyid_stg=pol.id_stg
                                                                                                                        AND             ((
                                                                                                                                                                        clm.reporteddate_stg >= cast(cast(:CC_EOY AS timestamp) AS DATE) - interval ''5 year''
                                                                                                                                                        AND             clm.reporteddate_stg <= cast(:CC_EOY AS timestamp))
                                                                                                                                        AND             (
                                                                                                                                                                        clm.lossdate_stg >= cast(cast(:CC_EOY AS timestamp) AS DATE) - interval ''5 year''
                                                                                                                                                        AND             clm.lossdate_stg <= cast(:CC_EOY AS timestamp)))
                                                                                                                        AND             clm.claimnumber_stg LIKE ''T%''
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
                                                                                                                        left join       db_t_prod_stag.cc_riskunit rsk
                                                                                                                        ON              rsk.id_stg=cc_cov.riskunitid_stg
                                                                                                                        join            db_t_prod_stag.cctl_coveragesubtype cov
                                                                                                                        ON              cov.id_stg=exps.coveragesubtype_stg
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
                                                                                                                        left join       db_t_prod_stag.cc_vehicle ccvehicle1
                                                                                                                        ON              inc.vehicleid_stg=ccvehicle1.id_stg
                                                                                                                        join            db_t_prod_stag.cctl_losscause lc
                                                                                                                        ON              lc.id_stg = clm.losscause_stg
                                                                                                                        left join
                                                                                                                                        (
                                                                                                                                                 SELECT   policyid_stg,
                                                                                                                                                          max(
                                                                                                                                                          CASE
                                                                                                                                                                   WHEN naiipcidetailtype_stg=''HODW_OtherPerils_Ded_HOE'' THEN (
                                                                                                                                                                            CASE
                                                                                                                                                                                     WHEN description_stg=''PERCENTAGE VALUE'' THEN ''P''
                                                                                                                                                                                     ELSE ''D''
                                                                                                                                                                            END)
                                                                                                                                                          END)AS ded_ind,
                                                                                                                                                          max(
                                                                                                                                                          CASE
                                                                                                                                                                   WHEN (
                                                                                                                                                                                     naiipcidetailtype_stg =''HODW_WindHail_Ded_HOE'') THEN naiipcidetailtype_stg
                                                                                                                                                          END )windhail,
                                                                                                                                                          max(
                                                                                                                                                          CASE
                                                                                                                                                                   WHEN (
                                                                                                                                                                                     naiipcidetailtype_stg =''HODW_Hurricane_Ded_HOE'' ) THEN naiipcidetailtype_stg
                                                                                                                                                          END )hurricane,
                                                                                                                                                          max(
                                                                                                                                                          CASE
                                                                                                                                                                   WHEN (
                                                                                                                                                                                     naiipcidetailtype_stg =''HODW_SectionI_DedWindAndHailExcl_alfa'') THEN naiipcidetailtype_stg
                                                                                                                                                          END )              windstormhailexcl,
                                                                                                                                                          max(amount_stg) AS amount,
                                                                                                                                                          max(value_stg)  AS ded_amt
                                                                                                                                                 FROM     db_t_prod_stag.ccx_naiipcidetail_alfa 
                                                                                                                                                 WHERE    naiipcidetailtype_stg LIKE ''HODW%''
                                                                                                                                                 GROUP BY policyid_stg) det
                                                                                                                        ON              clm.policyid_stg=det.policyid_stg
                                                                                                                        left join       db_t_prod_stag.cc_policylocation cc_pl
                                                                                                                        ON              pol.id_stg=cc_pl.policyid_stg
                                                                                                                        left join       db_t_prod_stag.cc_address addr
                                                                                                                        ON              addr.id_stg=cc_pl.addressid_stg
                                                                                                                        left join       db_t_prod_stag.cctl_state c_st
                                                                                                                        ON              c_st.id_stg=addr.state_stg
                                                                                                                        AND             c_st.typecode_stg IN (''AL'',
                                                                                                                                                              ''GA'',
                                                                                                                                                              ''MS'')
                                                                                                                        WHERE           tl4.name_stg NOT IN (''Awaiting submission'',
                                                                                                                                                             ''Rejected'',
                                                                                                                                                             ''Submitting'',
                                                                                                                                                             ''Pending approval'')
                                                                                                                        AND             psttl.typecode_stg IN (''MH3'',
                                                                                                                                                               ''MH4'',
                                                                                                                                                               ''MH7'',
                                                                                                                                                               ''MH9'') ) a
                                                                                               GROUP BY policysystemperiodid_stg,
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
                                                                                                        policy_eff_yr,
                                                                                                        newrecordformat,
                                                                                                        aslob,
                                                                                                        itemcode,
                                                                                                        sublinecode,
                                                                                                        dwellingprotectionclasscode_stg,
                                                                                                        typeofdeductiblecode,
                                                                                                        policytermcode,
                                                                                                        losscause,
                                                                                                        locationcode,
                                                                                                        amountofinsurance,
                                                                                                        yearofmanufacture,
                                                                                                        tiedowncode,
                                                                                                        deductibleindicator,
                                                                                                        deductibleamount,
                                                                                                        deductibleindicatorws,
                                                                                                        deductibleamountws,
                                                                                                        claimidentifier,
                                                                                                        claimantidentifier,
                                                                                                        writtenexposure,
                                                                                                        wrtnprem,
                                                                                                        policynumber_stg,
                                                                                                        policyperiodid,
                                                                                                        policyidentifier,
                                                                                                        policysubtype,
                                                                                                        effectivedate,
                                                                                                        covrank,
                                                                                                        closedate ) b
                                                                             GROUP BY policysystemperiodid_stg,
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
                                                                                      policy_eff_yr,
                                                                                      newrecordformat,
                                                                                      aslob,
                                                                                      itemcode,
                                                                                      sublinecode,
                                                                                      dwellingprotectionclasscode_stg,
                                                                                      typeofdeductiblecode,
                                                                                      policytermcode,
                                                                                      losscause,
                                                                                      locationcode,
                                                                                      amountofinsurance,
                                                                                      yearofmanufacture,
                                                                                      tiedowncode,
                                                                                      deductibleindicator,
                                                                                      deductibleamount,
                                                                                      deductibleindicatorws,
                                                                                      deductibleamountws,
                                                                                      claimidentifier,
                                                                                      claimantidentifier,
                                                                                      writtenexposure,
                                                                                      wrtnprem,
                                                                                      policynumber_stg,
                                                                                      policyperiodid,
                                                                                      policyidentifier,
                                                                                      effectivedate) AS a
                                                             WHERE  (
                                                                           cast(coalesce(paidloss,0) AS          DECIMAL(18,2))<> 0.00
                                                                    OR     cast(coalesce(paidclaims,0) AS        INTEGER) <> 0
                                                                    OR     cast(coalesce(outstandinglosses,0) AS DECIMAL(18,2))<> 0.00
                                                                    OR     cast(coalesce(outstandingclaims,0) AS INTEGER) <> 0 ) 
											) c_claim
                                            left join
                                                      (
                                                             SELECT pc_policyperiod.territorycode        AS territorycode,
                                                                    pc_policyperiod.zipcode              AS zipcode,
                                                                    pc_policyperiod.protectioncasscode   AS protectioncasscode,
                                                                    pc_policyperiod.typeofdeductiblecode AS typeofdeductiblecode,
                                                                    pc_policyperiod.amountofinsurance    AS amountofinsurance,
                                                                    pc_policyperiod.yeaofmanufacture     AS yeaofmanufacture,
                                                                    pc_policyperiod.deductibleindicator  AS deductibleindicator,
                                                                    pc_policyperiod.deductibleamount     AS deductibleamount,
                                                                    pc_policyperiod.policyperiodid       AS policyperiodid ,
                                                                    cov
                                                             FROM   (
                                                                                    SELECT DISTINCT terr.code AS territorycode,
                                                                                                    CASE
                                                                                                                    WHEN substring (terr.postalcodeinternal,0,position(''-'' IN terr.postalcodeinternal)) = '''' THEN terr.postalcodeinternal
                                                                                                                    ELSE substring (terr.postalcodeinternal,0,position(''-'' IN terr.postalcodeinternal))
                                                                                                    END                                 AS zipcode,
                                                                                                    phh.dwellingprotectionclasscode_stg AS protectioncasscode,
                                                                                                    CASE
                                                                                                                    WHEN (
                                                                                                                                                    (
                                                                                                                                                                    windhail =''HODW_WindHail_Ded_HOE'')
                                                                                                                                    OR              (
                                                                                                                                                                    hurricane =''HODW_Hurricane_Ded_HOE'' ))
                                                                                                                    AND             deductibleamountws >0 THEN ''03''
                                                                                                                    WHEN (
                                                                                                                                                    windstormhailexcl =''HODW_SectionI_DedWindAndHailExcl_alfa'')
                                                                                                                    AND             (
                                                                                                                                                    windstormhailexcl_amt <> 0
                                                                                                                                    AND             windstormhailexcl_amt IS NOT NULL) THEN ''07''
                                                                                                                    ELSE ''05''
                                                                                                    END AS typeofdeductiblecode,
                                                                                                    cast(
                                                                                                    CASE
                                                                                                                    WHEN ph.typecode_stg=''MH4'' THEN pp_limit
                                                                                                                    ELSE dw_limit
                                                                                                    END AS INTEGER)AS amountofinsurance,
                                                                                                    coalesce( cast(
                                                                                                    CASE
                                                                                                                    WHEN (
                                                                                                                                                    pdh.yearbuilt_stg<1960) THEN ''1959''
                                                                                                                    ELSE pdh.yearbuilt_stg
                                                                                                    END AS INTEGER),cast(''0000'' AS INTEGER)) AS yeaofmanufacture,
                                                                                                    perils_limit_ind                            deductibleindicator,
                                                                                                    perils_limit                                deductibleamount,
                                                                                                    pp.policynumber_stg,
                                                                                                    pp.id_stg policyperiodid,
                                                                                                    pp.editeffectivedate_stg,
                                                                                                    pj.jobnumber_stg                                                                                                             policyidentifier,
                                                                                                    pcdh.typecode_stg                                                                                                            cov,
                                                                                                    row_number() over ( PARTITION BY policynumber_stg , pp.id_stg ORDER BY pp.updatetime_stg DESC,editeffectivedate_stg DESC) AS rnk
                                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                                    join            db_t_prod_stag.pctl_jurisdiction jd
                                                                                    ON              pp.basestate_stg=jd.id_stg
                                                                                    join            db_t_prod_stag.pcx_dwelling_hoe pdh
                                                                                    ON              pdh.branchid_stg=pp.id_stg
                                                                                    AND             pdh.expirationdate_stg IS NULL
                                                                                    left join       db_t_prod_stag.pctl_dwellingusage_hoe pcdh
                                                                                    ON              pcdh.id_stg =pdh.dwellingusage_stg
                                                                                    join            db_t_prod_stag.pc_job pj
                                                                                    ON              pp.jobid_stg = pj.id_stg
                                                                                    join            db_t_prod_stag.pctl_job pcj
                                                                                    ON              pj.subtype_stg = pcj.id_stg
                                                                                    left join
                                                                                                    (
                                                                                                              SELECT    branchid_stg                                                                                                              id,
                                                                                                                        max(cast(coalesce(old_code, pht1.naiipcicode_alfa_stg, pht2.naiipcicode_alfa_stg, pht3.naiipcicode_alfa_stg) AS INTEGER) )code,
                                                                                                                        max(postalcodeinternal)                                                                                                   postalcodeinternal
                                                                                                              FROM      (
                                                                                                                                  SELECT    e.branchid_stg,
                                                                                                                                            policynumber_stg,
                                                                                                                                            c.code_stg,
                                                                                                                                            g.typecode_stg,
                                                                                                                                            c.countycode_alfa_stg,
                                                                                                                                            pc_policyline.hopolicytype_stg,
                                                                                                                                            coalesce(postalcodeinternal_stg, postalcode_stg)postalcodeinternal,
                                                                                                                                            row_number() over( PARTITION BY e.branchid_stg, policynumber_stg, c.code_stg,g.typecode_stg, c.countycode_alfa_stg, pc_policyline.hopolicytype_stg ORDER BY
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
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''JEFFERSON'' THEN ''32''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(cityinternal_stg)= ''HUNTSVILLE''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''MADISON'' THEN ''35''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(cityinternal_stg)= ''MONTGOMERY''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''MONTGOMERY'' THEN ''37''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(cityinternal_stg)= ''MOBILE''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''MOBILE'' THEN ''30''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''AUTAUGA'',
                                                                                                                                                                                                        ''ELMORE'',
                                                                                                                                                                                                        ''MONTGOMERY'') THEN ''38''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''BALDWIN''
                                                                                                                                                      AND       cast(c.code_stg AS INTEGER)=26 THEN ''41''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''BALDWIN''
                                                                                                                                                      AND       cast(c.code_stg AS INTEGER)=28 THEN ''31''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''BALDWIN''
                                                                                                                                                      AND       (
                                                                                                                                                                          cast(c.code_stg AS INTEGER)=11
                                                                                                                                                                OR        cast(c.code_stg AS INTEGER) IS NULL
                                                                                                                                                                OR        cast(c.code_stg AS INTEGER) IN (1,2,3) ) THEN ''05''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''BARBOUR'',
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
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''CALHOUN'',
                                                                                                                                                                                                        ''ETOWAH'') THEN ''40''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''COLBERT'',
                                                                                                                                                                                                        ''LAUDERDALE'',
                                                                                                                                                                                                        ''LIMESTONE'',
                                                                                                                                                                                                        ''MADISON'',
                                                                                                                                                                                                        ''MORGAN'') THEN ''36''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''JEFFERSON'' THEN ''33''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''MOBILE''
                                                                                                                                                      AND       (
                                                                                                                                                                          cast(c.code_stg AS INTEGER)IN (2,1,26,3 )
                                                                                                                                                                OR        cast(c.code_stg AS INTEGER)IS NULL) THEN ''41''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''MOBILE''
                                                                                                                                                      AND       cast(c.code_stg AS INTEGER)=28 THEN ''31''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''MOBILE''
                                                                                                                                                      AND       cast(c.code_stg AS INTEGER)=11 THEN ''05''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''SHELBY'',
                                                                                                                                                                                                        ''WALKER'') THEN ''34''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''TUSCALOOSA'' THEN ''39''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       (
                                                                                                                                                                          substring (b.postalcodeinternal_stg,0, position(''-'' IN b.postalcodeinternal_stg))=''36511''
                                                                                                                                                                OR        b.postalcodeinternal_stg=''36511'')
                                                                                                                                                      AND       upper(cityinternal_stg)= ''BON SECOUR'' THEN ''06''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       (
                                                                                                                                                                          substring (b.postalcodeinternal_stg,0, position(''-'' IN b.postalcodeinternal_stg))=''36528''
                                                                                                                                                                OR        b.postalcodeinternal_stg=''36528'')
                                                                                                                                                      AND       upper(cityinternal_stg)= ''DAUPHIN ISLAND'' THEN ''06''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       (
                                                                                                                                                                          substring (b.postalcodeinternal_stg,0, position(''-'' IN b.postalcodeinternal_stg)) IN (''36542'',
                                                                                                                                                                                                        ''36547'')
                                                                                                                                                                OR        b.postalcodeinternal_stg IN (''36542'',
                                                                                                                                                                                                       ''36547''))
                                                                                                                                                      AND       upper(cityinternal_stg)= ''GULF SHORES'' THEN ''06''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       (
                                                                                                                                                                          substring (b.postalcodeinternal_stg,0, position(''-'' IN b.postalcodeinternal_stg))=''36561''
                                                                                                                                                                OR        b.postalcodeinternal_stg=''36561'')
                                                                                                                                                      AND       upper(cityinternal_stg)= ''ORANGE BEACH'' THEN ''06''
                                                                                                                                                      WHEN g.typecode_stg=''MS''
                                                                                                                                                      AND       upper(cityinternal_stg)= ''JACKSON''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''HINDS'',
                                                                                                                                                                                                        ''RANKIN'') THEN ''30''
                                                                                                                                                      WHEN g.typecode_stg=''MS''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''AMITE'',
                                                                                                                                                                                                        ''FORREST'',
                                                                                                                                                                                                        ''GREENE'',
                                                                                                                                                                                                        ''LAMAR'',
                                                                                                                                                                                                        ''MARION'',
                                                                                                                                                                                                        ''PERRY'',
                                                                                                                                                                                                        ''PIKE'',
                                                                                                                                                                                                        ''WALTHALL'',
                                                                                                                                                                                                        ''WILKINSON'') THEN ''03''
                                                                                                                                                      WHEN g.typecode_stg=''MS''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''GEORGE'',
                                                                                                                                                                                                        ''PEARL RIVER'',
                                                                                                                                                                                                        ''STONE'') THEN ''05''
                                                                                                                                                      WHEN g.typecode_stg=''MS''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''HANCOCK'',
                                                                                                                                                                                                        ''HARRISON'',
                                                                                                                                                                                                        ''JACKSON'') THEN ''06''
                                                                                                                                                      WHEN g.typecode_stg=''MS''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''HINDS'',
                                                                                                                                                                                                        ''MADISON'',
                                                                                                                                                                                                        ''RANKIN'') THEN ''31''
                                                                                                                                                      WHEN g.typecode_stg=''MS''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''ADAMS'',
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
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''DE KALB'',
                                                                                                                                                                                                        ''DEKALB'',
                                                                                                                                                                                                        ''FULTON'') THEN ''32''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(cityinternal_stg)= ''MACON''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''BIBB'' THEN ''35''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(cityinternal_stg)= ''SAVANNAH''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''CHATHAM'' THEN ''30''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''DE KALB'',
                                                                                                                                                                                                        ''DEKALB'',
                                                                                                                                                                                                        ''FULTON'') THEN ''33''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''BRYAN'',
                                                                                                                                                                                                        ''CAMDEN'',
                                                                                                                                                                                                        ''CHATHAM'',
                                                                                                                                                                                                        ''GLYNN'',
                                                                                                                                                                                                        ''LIBERTY'',
                                                                                                                                                                                                        ''MCINTOSH'') THEN ''31''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''DE KALB'',
                                                                                                                                                                                                        ''DEKALB'',
                                                                                                                                                                                                        ''FULTON'') THEN ''33''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''CLAYTON'',
                                                                                                                                                                                                        ''COBB'',
                                                                                                                                                                                                        ''GWINNETT'') THEN ''34''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''CATOOSA'',
                                                                                                                                                                                                        ''WALKER'',
                                                                                                                                                                                                        ''WHITFIELD'') THEN ''36''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) =''RICHMOND'' THEN ''37''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''CHATTAHOOCHEE'',
                                                                                                                                                                                                        ''MUSCOGEE'') THEN ''38''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''BUTTS'',
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
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''BALDWIN'',
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
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''BAKER'',
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
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''APPLING'',
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
                                                                                                                                  AND       pctl_hopolicytype_hoe.typecode_stg LIKE ''MH%''
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
                                                                                                                                        WHERE           publicid_stg LIKE ''%MH%'')pht2
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
                                                                                                                                                        rank() over( PARTITION BY naiipcicode_alfa_stg, substring(publicid_stg,7,2) , countycode_alfa_stg , hopolicytype_hoe_stg ORDER BY naiipcicode_alfa_stg, code_stg )row1
                                                                                                                                        FROM            db_t_prod_stag.pcx_hodbterritory_alfa 
                                                                                                                                        WHERE           publicid_stg LIKE ''%MH%'')pht3
                                                                                                              ON        pht3.row1=1
                                                                                                              AND       cast(pht3.countycode_alfa AS VARCHAR(50))=cast(loc.countycode_alfa_stg AS VARCHAR(50))
                                                                                                              AND       pht3.state =loc.typecode_stg
                                                                                                              AND       pht3.hopolicytype_hoe_stg=loc.hopolicytype_stg
                                                                                                              WHERE     ROWNUM=1
                                                                                                              GROUP BY  branchid_stg ) terr
                                                                                    ON              terr.id=pp.id_stg
                                                                                    join            db_t_prod_stag.pcx_holocation_hoe phh
                                                                                    ON              pdh.holocation_stg= phh.id_stg
                                                                                    join            db_t_prod_stag.pctl_hopolicytype_hoe ph
                                                                                    ON              ph.id_stg=pdh.hopolicytype_stg
                                                                                    AND             ph.typecode_stg IN (''MH3'',
                                                                                                                        ''MH4'',
                                                                                                                        ''MH7'',
                                                                                                                        ''MH9'')
                                                                                    join
                                                                                                    (
                                                                                                                    SELECT DISTINCT branchid,
                                                                                                                                    max(
                                                                                                                                    CASE
                                                                                                                                                    WHEN covterm.covtermpatternid=''HODW_Dwelling_Limit_HOE'' THEN substring(''00000'',1, (5-length( cast(cast(round(cast(polcov.val AS DECIMAL(18,4))/1000,0)AS INTEGER) AS VARCHAR(10)))))
                                                                                                                                                                                    || cast(cast(round(cast(polcov.val AS DECIMAL(18,4))/1000, 0)AS INTEGER) AS VARCHAR(10))
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
                                                                                                                                                                                    WHEN substring(name1 ,length(name1),1)=''%'' THEN ''P''
                                                                                                                                                                                    ELSE ''D''
                                                                                                                                                                    END)
                                                                                                                                    END)perils_limit_ind,
                                                                                                                                    max(
                                                                                                                                    CASE
                                                                                                                                                    WHEN covterm.covtermpatternid=''HODW_OtherPerils_Ded_HOE'' THEN (
                                                                                                                                                                    CASE
                                                                                                                                                                                    WHEN substring(name1 ,length(name1),1)=''%'' THEN substring(''0000000'',1, (7-length(cast(cast(cast(cast(value1 AS DECIMAL(18,4)) *10000 AS DECIMAL(18,4)) AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                                                                        || cast(cast(cast(cast(value1 AS DECIMAL(18,4)) *10000 AS DECIMAL(18,4)) AS INTEGER) AS VARCHAR(10))
                                                                                                                                                                                    ELSE substring(''0000000'',1, (7-length(cast(cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                                                                        || cast(cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER) AS VARCHAR(10))
                                                                                                                                                                    END)
                                                                                                                                    END)perils_limit,
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
                                                                                                                                                                                                        || cast(cast(cast(value1 AS DECIMAL(18,4))                            *10000 AS INTEGER) AS VARCHAR(10))
                                                                                                                                                                                    WHEN value1 IS NULL
                                                                                                                                                                                    OR              cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER) =0 THEN 0
                                                                                                                                                                                    ELSE substring(''0000000'',1,(7                                              -length(cast(cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                                                                        || cast(cast(cast(value1 AS DECIMAL(18,4)) *10000 AS INTEGER) AS VARCHAR(10))
                                                                                                                                                                    END )
                                                                                                                                    END ) AS deductibleamountws
                                                                                                                    FROM            (
                                                                                                                                               SELECT     cast(''DirectTerm1'' AS                       VARCHAR(100)) AS columnname,
                                                                                                                                                          cast(directterm1_stg AS                     VARCHAR(255)) AS val,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
                                                                                                                                               WHERE      directterm1avl_stg = 1
                                                                                                                                               AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                               UNION
                                                                                                                                               SELECT     ''Clause''                   AS columnname,
                                                                                                                                                          cast(NULL AS VARCHAR(255)) AS val,
                                                                                                                                                          pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
                                                                                                                                               WHERE      pcx_dwellingcov_hoe.patterncode_stg= ''HODW_PersonalPropertyReplacementCost_alfa''
                                                                                                                                               UNION
                                                                                                                                               SELECT     ''DirectTerm2''                         AS columnname,
                                                                                                                                                          cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                                                                                                                          pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
                                                                                                                                               WHERE      directterm2avl_stg = 1
                                                                                                                                               AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                               UNION
                                                                                                                                               SELECT     ''DirectTerm3''                         AS columnname,
                                                                                                                                                          cast(directterm3_stg AS VARCHAR(255)) AS val,
                                                                                                                                                          pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
                                                                                                                                               WHERE      directterm3avl_stg = 1
                                                                                                                                               AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                               UNION
                                                                                                                                               SELECT     ''DirectTerm4''                         AS columnname,
                                                                                                                                                          cast(directterm4_stg AS VARCHAR(255)) AS val,
                                                                                                                                                          pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
                                                                                                                                               WHERE      directterm4avl_stg = 1
                                                                                                                                               AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                               UNION
                                                                                                                                               SELECT     ''ChoiceTerm2''                         AS columnname,
                                                                                                                                                          cast(choiceterm2_stg AS VARCHAR(255)) AS val,
                                                                                                                                                          pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
                                                                                                                                               WHERE      choiceterm2avl_stg = 1
                                                                                                                                               AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                               UNION
                                                                                                                                               SELECT     ''ChoiceTerm3''                         AS columnname,
                                                                                                                                                          cast(choiceterm3_stg AS VARCHAR(255)) AS val,
                                                                                                                                                          pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
                                                                                                                                               WHERE      choiceterm3avl_stg = 1
                                                                                                                                               AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                               UNION
                                                                                                                                               SELECT     cast(''ChoiceTerm1'' AS                       VARCHAR(250)) AS columnname,
                                                                                                                                                          cast(choiceterm1_stg AS                     VARCHAR(255)) AS val,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
                                                                                                                                               WHERE      choiceterm1avl_stg = 1
                                                                                                                                               AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                               UNION
                                                                                                                                               SELECT     cast(''ChoiceTerm4'' AS                       VARCHAR(250)) AS columnname,
                                                                                                                                                          cast(choiceterm4_stg AS                     VARCHAR(255)) AS val,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
                                                                                                                                               WHERE      choiceterm4avl_stg = 1
                                                                                                                                               AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                               UNION
                                                                                                                                               SELECT     cast(''BooleanTerm1'' AS                      VARCHAR(250)) AS columnname,
                                                                                                                                                          cast(booleanterm1_stg AS                    VARCHAR(255)) AS val,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
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
                                                                                                                    GROUP BY        branchid ) cov
                                                                                    ON              to_number(cov.branchid) =pp.id_stg
                                                                                    join            db_t_prod_stag.pctl_policyperiodstatus 
                                                                                    ON              pp.status_stg=pctl_policyperiodstatus.id_stg
                                                                                    join            db_t_prod_stag.pc_policyterm pt
                                                                                    ON              pt.id_stg = pp.policytermid_stg
                                                                                    join            db_t_prod_stag.pc_policyline 
                                                                                    ON              pp.id_stg = pc_policyline.branchid_stg
                                                                                    AND             pc_policyline.expirationdate_stg IS NULL
                                                                                    join            db_t_prod_stag.pctl_hopolicytype_hoe 
                                                                                    ON              pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                    AND             pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                           ''MH4'',
                                                                                                                                           ''MH7'',
                                                                                                                                           ''MH9'')) pc_policyperiod
                                                             WHERE  rnk =1 ) policy1
                                            ON        c_claim.policysystemperiodid = policy1.policyperiodid
                                            left join
                                                      (
                                                             SELECT pc_policyperiod.territorycode        AS territorycode,
                                                                    pc_policyperiod.zipcode              AS zipcode,
                                                                    pc_policyperiod.protectioncasscode   AS protectioncasscode,
                                                                    pc_policyperiod.typeofdeductiblecode AS typeofdeductiblecode,
                                                                    pc_policyperiod.amountofinsurance    AS amountofinsurance,
                                                                    pc_policyperiod.yeaofmanufacture     AS yeaofmanufacture,
                                                                    pc_policyperiod.deductibleindicator  AS deductibleindicator,
                                                                    pc_policyperiod.deductibleamount     AS deductibleamount,
                                                                    pc_policyperiod.policynumber_stg     AS policynumber
                                                             FROM   (
                                                                                    SELECT DISTINCT terr.code AS territorycode,
                                                                                                    CASE
                                                                                                                    WHEN substring (terr.postalcodeinternal,0,position(''-'' IN terr.postalcodeinternal)) = '''' THEN terr.postalcodeinternal
                                                                                                                    ELSE substring (terr.postalcodeinternal,0,position(''-'' IN terr.postalcodeinternal))
                                                                                                    END                                 AS zipcode,
                                                                                                    phh.dwellingprotectionclasscode_stg AS protectioncasscode,
                                                                                                    CASE
                                                                                                                    WHEN (
                                                                                                                                                    (
                                                                                                                                                                    windhail =''HODW_WindHail_Ded_HOE'')
                                                                                                                                    OR              (
                                                                                                                                                                    hurricane =''HODW_Hurricane_Ded_HOE'' ))
                                                                                                                    AND             deductibleamountws >0 THEN ''03''
                                                                                                                    WHEN (
                                                                                                                                                    windstormhailexcl =''HODW_SectionI_DedWindAndHailExcl_alfa'')
                                                                                                                    AND             (
                                                                                                                                                    windstormhailexcl_amt <> 0
                                                                                                                                    AND             windstormhailexcl_amt IS NOT NULL) THEN ''07''
                                                                                                                    ELSE ''05''
                                                                                                    END AS typeofdeductiblecode,
                                                                                                    cast(
                                                                                                    CASE
                                                                                                                    WHEN ph.typecode_stg=''MH4'' THEN pp_limit
                                                                                                                    ELSE dw_limit
                                                                                                    END AS INTEGER) AS amountofinsurance,
                                                                                                    coalesce( cast(
                                                                                                    CASE
                                                                                                                    WHEN (
                                                                                                                                                    pdh.yearbuilt_stg<1960) THEN ''1959''
                                                                                                                    ELSE pdh.yearbuilt_stg
                                                                                                    END AS INTEGER),cast(''0000'' AS INTEGER)) AS yeaofmanufacture,
                                                                                                    perils_limit_ind                            deductibleindicator,
                                                                                                    perils_limit                                deductibleamount,
                                                                                                    pp.policynumber_stg,
                                                                                                    pp.id_stg policyperiodid,
                                                                                                    pp.editeffectivedate_stg,
                                                                                                    pj.jobnumber_stg                                                                                                 policyidentifier,
                                                                                                    row_number() over ( PARTITION BY policynumber_stg ORDER BY pp.updatetime_stg DESC,editeffectivedate_stg DESC) AS rnk
                                                                                    FROM            db_t_prod_stag.pc_policyperiod pp
                                                                                    join            db_t_prod_stag.pctl_jurisdiction jd
                                                                                    ON              pp.basestate_stg=jd.id_stg
                                                                                    join            db_t_prod_stag.pcx_dwelling_hoe pdh
                                                                                    ON              pdh.branchid_stg=pp.id_stg
                                                                                    AND             pdh.expirationdate_stg IS NULL
                                                                                    left join       db_t_prod_stag.pctl_dwellingusage_hoe pcdh
                                                                                    ON              pcdh.id_stg =pdh.dwellingusage_stg
                                                                                    join            db_t_prod_stag.pc_job pj
                                                                                    ON              pp.jobid_stg = pj.id_stg
                                                                                    join            db_t_prod_stag.pctl_job pcj
                                                                                    ON              pj.subtype_stg = pcj.id_stg
                                                                                    left join
                                                                                                    (
                                                                                                              SELECT    branchid_stg                                                                                                              id,
                                                                                                                        max(cast(coalesce(old_code, pht1.naiipcicode_alfa_stg, pht2.naiipcicode_alfa_stg, pht3.naiipcicode_alfa_stg) AS INTEGER) )code,
                                                                                                                        max(postalcodeinternal)                                                                                                   postalcodeinternal
                                                                                                              FROM      (
                                                                                                                                  SELECT    e.branchid_stg,
                                                                                                                                            policynumber_stg,
                                                                                                                                            c.code_stg,
                                                                                                                                            g.typecode_stg,
                                                                                                                                            c.countycode_alfa_stg,
                                                                                                                                            pc_policyline.hopolicytype_stg,
                                                                                                                                            coalesce(postalcodeinternal_stg, postalcode_stg)postalcodeinternal,
                                                                                                                                            row_number() over( PARTITION BY e.branchid_stg, policynumber_stg, c.code_stg,g.typecode_stg, c.countycode_alfa_stg, pc_policyline.hopolicytype_stg ORDER BY
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
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''JEFFERSON'' THEN ''32''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(cityinternal_stg)= ''HUNTSVILLE''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''MADISON'' THEN ''35''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(cityinternal_stg)= ''MONTGOMERY''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''MONTGOMERY'' THEN ''37''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(cityinternal_stg)= ''MOBILE''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''MOBILE'' THEN ''30''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''AUTAUGA'',
                                                                                                                                                                                                        ''ELMORE'',
                                                                                                                                                                                                        ''MONTGOMERY'') THEN ''38''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''BALDWIN''
                                                                                                                                                      AND       cast(c.code_stg AS INTEGER)=26 THEN ''41''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''BALDWIN''
                                                                                                                                                      AND       cast(c.code_stg AS INTEGER)=28 THEN ''31''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''BALDWIN''
                                                                                                                                                      AND       (
                                                                                                                                                                          cast(c.code_stg AS INTEGER)=11
                                                                                                                                                                OR        cast(c.code_stg AS INTEGER) IS NULL
                                                                                                                                                                OR        cast(c.code_stg AS INTEGER) IN (1,2,3) ) THEN ''05''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''BARBOUR'',
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
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''CALHOUN'',
                                                                                                                                                                                                        ''ETOWAH'') THEN ''40''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''COLBERT'',
                                                                                                                                                                                                        ''LAUDERDALE'',
                                                                                                                                                                                                        ''LIMESTONE'',
                                                                                                                                                                                                        ''MADISON'',
                                                                                                                                                                                                        ''MORGAN'') THEN ''36''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''JEFFERSON'' THEN ''33''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''MOBILE''
                                                                                                                                                      AND       (
                                                                                                                                                                          cast(c.code_stg AS INTEGER)IN (2,1,26,3 )
                                                                                                                                                                OR        cast(c.code_stg AS INTEGER)IS NULL) THEN ''41''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''MOBILE''
                                                                                                                                                      AND       cast(c.code_stg AS INTEGER)=28 THEN ''31''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''MOBILE''
                                                                                                                                                      AND       cast(c.code_stg AS INTEGER)=11 THEN ''05''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''SHELBY'',
                                                                                                                                                                                                        ''WALKER'') THEN ''34''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''TUSCALOOSA'' THEN ''39''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       (
                                                                                                                                                                          substring (b.postalcodeinternal_stg,0, position(''-'' IN b.postalcodeinternal_stg))=''36511''
                                                                                                                                                                OR        b.postalcodeinternal_stg=''36511'')
                                                                                                                                                      AND       upper(cityinternal_stg)= ''BON SECOUR'' THEN ''06''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       (
                                                                                                                                                                          substring (b.postalcodeinternal_stg,0, position(''-'' IN b.postalcodeinternal_stg))=''36528''
                                                                                                                                                                OR        b.postalcodeinternal_stg=''36528'')
                                                                                                                                                      AND       upper(cityinternal_stg)= ''DAUPHIN ISLAND'' THEN ''06''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       (
                                                                                                                                                                          substring (b.postalcodeinternal_stg,0, position(''-'' IN b.postalcodeinternal_stg)) IN (''36542'',
                                                                                                                                                                                                        ''36547'')
                                                                                                                                                                OR        b.postalcodeinternal_stg IN (''36542'',
                                                                                                                                                                                                       ''36547''))
                                                                                                                                                      AND       upper(cityinternal_stg)= ''GULF SHORES'' THEN ''06''
                                                                                                                                                      WHEN g.typecode_stg=''AL''
                                                                                                                                                      AND       (
                                                                                                                                                                          substring (b.postalcodeinternal_stg,0, position(''-'' IN b.postalcodeinternal_stg))=''36561''
                                                                                                                                                                OR        b.postalcodeinternal_stg=''36561'')
                                                                                                                                                      AND       upper(cityinternal_stg)= ''ORANGE BEACH'' THEN ''06''
                                                                                                                                                      WHEN g.typecode_stg=''MS''
                                                                                                                                                      AND       upper(cityinternal_stg)= ''JACKSON''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''HINDS'',
                                                                                                                                                                                                        ''RANKIN'') THEN ''30''
                                                                                                                                                      WHEN g.typecode_stg=''MS''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''AMITE'',
                                                                                                                                                                                                        ''FORREST'',
                                                                                                                                                                                                        ''GREENE'',
                                                                                                                                                                                                        ''LAMAR'',
                                                                                                                                                                                                        ''MARION'',
                                                                                                                                                                                                        ''PERRY'',
                                                                                                                                                                                                        ''PIKE'',
                                                                                                                                                                                                        ''WALTHALL'',
                                                                                                                                                                                                        ''WILKINSON'') THEN ''03''
                                                                                                                                                      WHEN g.typecode_stg=''MS''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''GEORGE'',
                                                                                                                                                                                                        ''PEARL RIVER'',
                                                                                                                                                                                                        ''STONE'') THEN ''05''
                                                                                                                                                      WHEN g.typecode_stg=''MS''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''HANCOCK'',
                                                                                                                                                                                                        ''HARRISON'',
                                                                                                                                                                                                        ''JACKSON'') THEN ''06''
                                                                                                                                                      WHEN g.typecode_stg=''MS''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''HINDS'',
                                                                                                                                                                                                        ''MADISON'',
                                                                                                                                                                                                        ''RANKIN'') THEN ''31''
                                                                                                                                                      WHEN g.typecode_stg=''MS''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''ADAMS'',
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
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''DE KALB'',
                                                                                                                                                                                                        ''DEKALB'',
                                                                                                                                                                                                        ''FULTON'') THEN ''32''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(cityinternal_stg)= ''MACON''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''BIBB'' THEN ''35''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(cityinternal_stg)= ''SAVANNAH''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg))=''CHATHAM'' THEN ''30''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''DE KALB'',
                                                                                                                                                                                                        ''DEKALB'',
                                                                                                                                                                                                        ''FULTON'') THEN ''33''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''BRYAN'',
                                                                                                                                                                                                        ''CAMDEN'',
                                                                                                                                                                                                        ''CHATHAM'',
                                                                                                                                                                                                        ''GLYNN'',
                                                                                                                                                                                                        ''LIBERTY'',
                                                                                                                                                                                                        ''MCINTOSH'') THEN ''31''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''DE KALB'',
                                                                                                                                                                                                        ''DEKALB'',
                                                                                                                                                                                                        ''FULTON'') THEN ''33''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''CLAYTON'',
                                                                                                                                                                                                        ''COBB'',
                                                                                                                                                                                                        ''GWINNETT'') THEN ''34''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''CATOOSA'',
                                                                                                                                                                                                        ''WALKER'',
                                                                                                                                                                                                        ''WHITFIELD'') THEN ''36''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) =''RICHMOND'' THEN ''37''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''CHATTAHOOCHEE'',
                                                                                                                                                                                                        ''MUSCOGEE'') THEN ''38''
                                                                                                                                                      WHEN g.typecode_stg=''GA''
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''BUTTS'',
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
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''BALDWIN'',
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
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''BAKER'',
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
                                                                                                                                                      AND       upper(coalesce(b.countyinternal_stg, county_stg)) IN (''APPLING'',
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
                                                                                                                                  AND       pctl_hopolicytype_hoe.typecode_stg LIKE ''MH%''
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
                                                                                                                                                        code_stg                    territory_code_stg,
                                                                                                                                                        hopolicytype_hoe_stg,
                                                                                                                                                        rank() over( PARTITION BY naiipcicode_alfa_stg, substring(publicid_stg,7,2) , code_stg , hopolicytype_hoe_stg ORDER BY naiipcicode_alfa_stg,countycode_alfa_stg )row1
                                                                                                                                        FROM            db_t_prod_stag.pcx_hodbterritory_alfa
                                                                                                                                        WHERE           publicid_stg LIKE ''%MH%'')pht2
                                                                                                              ON        pht2.row1=1
                                                                                                              AND       pht2.territory_code_stg =loc.code_stg
                                                                                                              AND       pht2.state =loc.typecode_stg
                                                                                                              AND       pht2.hopolicytype_hoe_stg=loc.hopolicytype_stg
                                                                                                              left join
                                                                                                                        (
                                                                                                                                        SELECT DISTINCT naiipcicode_alfa_stg,
                                                                                                                                                        substring(publicid_stg,7,2) state,
                                                                                                                                                        countycode_alfa_stg         countycode_alfa,
                                                                                                                                                        hopolicytype_hoe_stg,
                                                                                                                                                        rank() over( PARTITION BY naiipcicode_alfa_stg, substring(publicid_stg,7,2) , countycode_alfa_stg , hopolicytype_hoe_stg ORDER BY naiipcicode_alfa_stg, code_stg )row1
                                                                                                                                        FROM            db_t_prod_stag.pcx_hodbterritory_alfa
                                                                                                                                        WHERE           publicid_stg LIKE ''%MH%'')pht3
                                                                                                              ON        pht3.row1=1
                                                                                                              AND       cast(pht3.countycode_alfa AS VARCHAR(50))=cast(loc.countycode_alfa_stg AS VARCHAR(50))
                                                                                                              AND       pht3.state =loc.typecode_stg
                                                                                                              AND       pht3.hopolicytype_hoe_stg=loc.hopolicytype_stg
                                                                                                              WHERE     ROWNUM=1
                                                                                                              GROUP BY  branchid_stg ) terr
                                                                                    ON              terr.id=pp.id_stg
                                                                                    join            db_t_prod_stag.pcx_holocation_hoe phh
                                                                                    ON              pdh.holocation_stg= phh.id_stg
                                                                                    join            db_t_prod_stag.pctl_hopolicytype_hoe ph
                                                                                    ON              ph.id_stg=pdh.hopolicytype_stg
                                                                                    AND             ph.typecode_stg IN (''MH3'',
                                                                                                                        ''MH4'',
                                                                                                                        ''MH7'',
                                                                                                                        ''MH9'')
                                                                                    join
                                                                                                    (
                                                                                                                    SELECT DISTINCT branchid,
                                                                                                                                    max(
                                                                                                                                    CASE
                                                                                                                                                    WHEN covterm.covtermpatternid=''HODW_Dwelling_Limit_HOE'' THEN substring(''00000'',1, (5-length( cast(cast(round(cast(polcov.val AS DECIMAL(18,4))/1000,0)AS INTEGER) AS VARCHAR(10)))))
                                                                                                                                                                                    || cast(cast(round(cast(polcov.val AS DECIMAL(18,4))/1000, 0)AS INTEGER) AS VARCHAR(10))
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
                                                                                                                                                                                    WHEN substring(name1 ,length(name1),1)=''%'' THEN ''P''
                                                                                                                                                                                    ELSE ''D''
                                                                                                                                                                    END)
                                                                                                                                    END)perils_limit_ind,
                                                                                                                                    max(
                                                                                                                                    CASE
                                                                                                                                                    WHEN covterm.covtermpatternid=''HODW_OtherPerils_Ded_HOE'' THEN (
                                                                                                                                                                    CASE
                                                                                                                                                                                    WHEN substring(name1 ,length(name1),1)=''%'' THEN substring(''0000000'',1, (7-length(cast(cast(cast(cast(value1 AS DECIMAL(18,4)) *10000 AS DECIMAL(18,4)) AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                                                                        || cast(cast(cast(cast(value1 AS DECIMAL(18,4)) *10000 AS DECIMAL(18,4)) AS INTEGER) AS VARCHAR(10))
                                                                                                                                                                                    ELSE substring(''0000000'',1, (7-length(cast(cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                                                                        || cast(cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER) AS VARCHAR(10))
                                                                                                                                                                    END)
                                                                                                                                    END)perils_limit,
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
                                                                                                                                                                                                        || cast(cast(cast(value1 AS DECIMAL(18,4))                            *10000 AS INTEGER) AS VARCHAR(10))
                                                                                                                                                                                    WHEN value1 IS NULL
                                                                                                                                                                                    OR              cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER) =0 THEN 0
                                                                                                                                                                                    ELSE substring(''0000000'',1,(7                                              -length(cast(cast(cast(value1 AS DECIMAL(18,4)) AS INTEGER)AS VARCHAR(10)))))
                                                                                                                                                                                                        || cast(cast(cast(value1 AS DECIMAL(18,4)) *10000 AS INTEGER) AS VARCHAR(10))
                                                                                                                                                                    END )
                                                                                                                                    END ) AS deductibleamountws
                                                                                                                    FROM            (
                                                                                                                                               SELECT     cast(''DirectTerm1'' AS                       VARCHAR(100)) AS columnname,
                                                                                                                                                          cast(directterm1_stg AS                     VARCHAR(255)) AS val,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
                                                                                                                                               WHERE      directterm1avl_stg = 1
                                                                                                                                               AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                               UNION
                                                                                                                                               SELECT     ''Clause''                   AS columnname,
                                                                                                                                                          cast(NULL AS VARCHAR(255)) AS val,
                                                                                                                                                          pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
                                                                                                                                               WHERE      pcx_dwellingcov_hoe.patterncode_stg= ''HODW_PersonalPropertyReplacementCost_alfa''
                                                                                                                                               UNION
                                                                                                                                               SELECT     ''DirectTerm2''                         AS columnname,
                                                                                                                                                          cast(directterm2_stg AS VARCHAR(255)) AS val,
                                                                                                                                                          pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
                                                                                                                                               WHERE      directterm2avl_stg = 1
                                                                                                                                               AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                               UNION
                                                                                                                                               SELECT     ''DirectTerm3''                         AS columnname,
                                                                                                                                                          cast(directterm3_stg AS VARCHAR(255)) AS val,
                                                                                                                                                          pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
                                                                                                                                               WHERE      directterm3avl_stg = 1
                                                                                                                                               AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                               UNION
                                                                                                                                               SELECT     ''DirectTerm4''                         AS columnname,
                                                                                                                                                          cast(directterm4_stg AS VARCHAR(255)) AS val,
                                                                                                                                                          pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
                                                                                                                                               WHERE      directterm4avl_stg = 1
                                                                                                                                               AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                               UNION
                                                                                                                                               SELECT     ''ChoiceTerm2''                         AS columnname,
                                                                                                                                                          cast(choiceterm2_stg AS VARCHAR(255)) AS val,
                                                                                                                                                          pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
                                                                                                                                               WHERE      choiceterm2avl_stg = 1
                                                                                                                                               AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                               UNION
                                                                                                                                               SELECT     ''ChoiceTerm3''                         AS columnname,
                                                                                                                                                          cast(choiceterm3_stg AS VARCHAR(255)) AS val,
                                                                                                                                                          pcx_dwellingcov_hoe.patterncode_stg,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
                                                                                                                                               WHERE      choiceterm3avl_stg = 1
                                                                                                                                               AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                               UNION
                                                                                                                                               SELECT     cast(''ChoiceTerm1'' AS                       VARCHAR(250)) AS columnname,
                                                                                                                                                          cast(choiceterm1_stg AS                     VARCHAR(255)) AS val,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
                                                                                                                                               WHERE      choiceterm1avl_stg = 1
                                                                                                                                               AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                               UNION
                                                                                                                                               SELECT     cast(''ChoiceTerm4'' AS                       VARCHAR(250)) AS columnname,
                                                                                                                                                          cast(choiceterm4_stg AS                     VARCHAR(255)) AS val,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
                                                                                                                                               WHERE      choiceterm4avl_stg = 1
                                                                                                                                               AND        pcx_dwellingcov_hoe.expirationdate_stg IS NULL
                                                                                                                                               UNION
                                                                                                                                               SELECT     cast(''BooleanTerm1'' AS                      VARCHAR(250)) AS columnname,
                                                                                                                                                          cast(booleanterm1_stg AS                    VARCHAR(255)) AS val,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.patterncode_stg AS VARCHAR(250))    patterncode,
                                                                                                                                                          cast(pcx_dwellingcov_hoe.branchid_stg AS    VARCHAR(255)) AS branchid,
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
                                                                                                                                               AND        pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                                                                                 ''MH4'',
                                                                                                                                                                                                 ''MH7'',
                                                                                                                                                                                                 ''MH9'')
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
                                                                                                                    GROUP BY        branchid ) cov
                                                                                    ON              to_number(cov.branchid) =pp.id_stg
                                                                                    join            db_t_prod_stag.pctl_policyperiodstatus 
                                                                                    ON              pp.status_stg=pctl_policyperiodstatus.id_stg
                                                                                    join            db_t_prod_stag.pc_policyterm pt
                                                                                    ON              pt.id_stg = pp.policytermid_stg
                                                                                    join            db_t_prod_stag.pc_policyline 
                                                                                    ON              pp.id_stg = pc_policyline.branchid_stg
                                                                                    AND             pc_policyline.expirationdate_stg IS NULL
                                                                                    join            db_t_prod_stag.pctl_hopolicytype_hoe 
                                                                                    ON              pc_policyline.hopolicytype_stg = pctl_hopolicytype_hoe.id_stg
                                                                                    AND             pctl_hopolicytype_hoe.typecode_stg IN (''MH3'',
                                                                                                                                           ''MH4'',
                                                                                                                                           ''MH7'',
                                                                                                                                           ''MH9'')) pc_policyperiod
                                                             WHERE  rnk = 1 ) lkp_query
                                            ON        c_claim.policynumber = lkp_query.policynumber ) ) src ) );
  -- Component exp_clm_trans_logic1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_clm_trans_logic1 AS
  (
         SELECT sq_cc_claim.policysystemperiodid  AS policysystemperiodid,
                sq_cc_claim.companynumber         AS companynumber,
                sq_cc_claim.lineofbusinesscode    AS lineofbusinesscode,
                sq_cc_claim.statecode             AS statecode,
                sq_cc_claim.callyear              AS callyear,
                sq_cc_claim.accountingyear        AS accountingyear,
                sq_cc_claim.expperiodyear         AS expperiodyear,
                sq_cc_claim.expperiodmonth        AS expperiodmonth,
                sq_cc_claim.expeperiodday         AS expeperiodday,
                sq_cc_claim.coveragecode          AS coveragecode,
                sq_cc_claim.policyeffectiveyear   AS policyeffectiveyear,
                sq_cc_claim.newrecordformat       AS newrecordformat,
                sq_cc_claim.aslob                 AS aslob,
                sq_cc_claim.itemcode              AS itemcode,
                sq_cc_claim.sublinecode           AS sublinecode,
                sq_cc_claim.policytermcode        AS policytermcode,
                sq_cc_claim.typeoflosscode        AS typeoflosscode,
                sq_cc_claim.locationcode          AS locationcode,
                sq_cc_claim.tiedowncode           AS tiedowncode,
                sq_cc_claim.deductibleindicatorws AS deductibleindicatorws,
                sq_cc_claim.deductibleamountws    AS deductibleamountws,
                sq_cc_claim.claimidentifier       AS claimidentifier,
                sq_cc_claim.claimantidentifier    AS claimantidentifier,
                sq_cc_claim.writtenexposure       AS writtenexposure,
                sq_cc_claim.writtenpremium        AS writtenpremium,
                sq_cc_claim.paidlosses            AS paidlosses,
                sq_cc_claim.paidnumberofclaims    AS paidnumberofclaims,
                sq_cc_claim.outstandinglosses     AS outstandinglosses,
                sq_cc_claim.outstandingnoofclaims AS outstandingnoofclaims,
                sq_cc_claim.policynumber          AS policynumber,
                sq_cc_claim.policyperiodid        AS policyperiodid,
                sq_cc_claim.policyidentifier      AS policyidentifier,
                CASE
                       WHEN sq_cc_claim.territorycode_pc IS NULL THEN sq_cc_claim.territorycode
                       ELSE sq_cc_claim.territorycode_pc
                END AS o_territorycode,
                CASE
                       WHEN sq_cc_claim.zipcode_pc IS NULL THEN sq_cc_claim.zipcode
                       ELSE sq_cc_claim.zipcode_pc
                END AS o_zipcode,
                CASE
                       WHEN sq_cc_claim.protectioncasscode_pc IS NULL THEN sq_cc_claim.protectioncasscode
                       ELSE sq_cc_claim.protectioncasscode_pc
                END AS o_protectioncasscode,
                CASE
                       WHEN sq_cc_claim.typeofdeductiblecode_pc IS NULL THEN sq_cc_claim.typeofdeductiblecode
                       ELSE sq_cc_claim.typeofdeductiblecode_pc
                END AS o_typeofdeductiblecode,
                CASE
                       WHEN sq_cc_claim.amountofinsurance_pc IS NULL THEN sq_cc_claim.amountofinsurance
                       ELSE sq_cc_claim.amountofinsurance_pc
                END AS o_amountofinsurance,
                CASE
                       WHEN sq_cc_claim.yearofmanufacture_pc IS NULL THEN sq_cc_claim.yeaofmanufacture
                       ELSE sq_cc_claim.yearofmanufacture_pc
                END AS o_yearofmanufacture,
                CASE
                       WHEN sq_cc_claim.deductibleindicator_pc IS NULL THEN sq_cc_claim.deductibleindicator
                       ELSE sq_cc_claim.deductibleindicator_pc
                END AS o_deductibleindicator,
                CASE
                       WHEN sq_cc_claim.deductibleamount_pc IS NULL THEN sq_cc_claim.deductibleamount
                       ELSE sq_cc_claim.deductibleamount_pc
                END                                    AS o_deductibleamount,
                sq_cc_claim.territorycode_lkppc        AS territorycode_lkppc,
                sq_cc_claim.zipcode_lkppc              AS zipcode_lkppc,
                sq_cc_claim.protectioncasscode_lkppc   AS protectioncasscode_lkppc,
                sq_cc_claim.typeofdeductiblecode_lkppc AS typeofdeductiblecode_lkppc,
                sq_cc_claim.amountofinsurance_lkppc    AS amountofinsurance_lkppc,
                sq_cc_claim.yearofmanufacture_lkppc    AS yearofmanufacture_lkppc,
                sq_cc_claim.deductibleindicator_lkppc  AS deductibleindicator_lkppc,
                sq_cc_claim.deductibleamount_lkppc     AS deductibleamount_lkppc,
                sq_cc_claim.source_record_id
         FROM   sq_cc_claim );
  -- Component exp_clm_trans_logic, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_clm_trans_logic AS
  (
         SELECT exp_clm_trans_logic1.companynumber         AS companynumber,
                exp_clm_trans_logic1.lineofbusinesscode    AS lineofbusinesscode,
                exp_clm_trans_logic1.statecode             AS statecode,
                exp_clm_trans_logic1.callyear              AS callyear,
                exp_clm_trans_logic1.accountingyear        AS accountingyear,
                exp_clm_trans_logic1.expperiodyear         AS expperiodyear,
                exp_clm_trans_logic1.expperiodmonth        AS expperiodmonth,
                exp_clm_trans_logic1.expeperiodday         AS expeperiodday,
                exp_clm_trans_logic1.coveragecode          AS coveragecode,
                exp_clm_trans_logic1.policyeffectiveyear   AS policyeffectiveyear,
                exp_clm_trans_logic1.newrecordformat       AS newrecordformat,
                exp_clm_trans_logic1.aslob                 AS aslob,
                exp_clm_trans_logic1.itemcode              AS itemcode,
                exp_clm_trans_logic1.sublinecode           AS sublinecode,
                exp_clm_trans_logic1.policytermcode        AS policytermcode,
                exp_clm_trans_logic1.typeoflosscode        AS typeoflosscode,
                exp_clm_trans_logic1.locationcode          AS locationcode,
                exp_clm_trans_logic1.tiedowncode           AS tiedowncode,
                exp_clm_trans_logic1.deductibleindicatorws AS deductibleindicatorws,
                exp_clm_trans_logic1.deductibleamountws    AS deductibleamountws,
                exp_clm_trans_logic1.claimidentifier       AS claimidentifier,
                exp_clm_trans_logic1.claimantidentifier    AS claimantidentifier,
                exp_clm_trans_logic1.writtenexposure       AS writtenexposure,
                exp_clm_trans_logic1.writtenpremium        AS writtenpremium,
                exp_clm_trans_logic1.paidlosses            AS paidlosses,
                exp_clm_trans_logic1.paidnumberofclaims    AS paidnumberofclaims,
                exp_clm_trans_logic1.outstandinglosses     AS outstandinglosses,
                exp_clm_trans_logic1.outstandingnoofclaims AS outstandingnoofclaims,
                exp_clm_trans_logic1.policynumber          AS policynumber,
                exp_clm_trans_logic1.policyperiodid        AS policyperiodid,
                CASE
                       WHEN exp_clm_trans_logic1.o_territorycode IS NULL THEN exp_clm_trans_logic1.territorycode_lkppc
                       ELSE exp_clm_trans_logic1.o_territorycode
                END AS o_territorycode,
                CASE
                       WHEN exp_clm_trans_logic1.o_zipcode IS NULL THEN exp_clm_trans_logic1.zipcode_lkppc
                       ELSE exp_clm_trans_logic1.o_zipcode
                END AS o_zipcode,
                CASE
                       WHEN exp_clm_trans_logic1.o_protectioncasscode IS NULL THEN exp_clm_trans_logic1.protectioncasscode_lkppc
                       ELSE exp_clm_trans_logic1.o_protectioncasscode
                END AS o_protectioncasscode,
                CASE
                       WHEN exp_clm_trans_logic1.o_typeofdeductiblecode IS NULL THEN exp_clm_trans_logic1.typeofdeductiblecode_lkppc
                       ELSE exp_clm_trans_logic1.o_typeofdeductiblecode
                END AS o_typeofdeductiblecode,
                CASE
                       WHEN exp_clm_trans_logic1.o_amountofinsurance IS NULL THEN exp_clm_trans_logic1.amountofinsurance_lkppc
                       ELSE exp_clm_trans_logic1.o_amountofinsurance
                END AS o_amountofinsurance,
                CASE
                       WHEN exp_clm_trans_logic1.o_yearofmanufacture IS NULL THEN exp_clm_trans_logic1.yearofmanufacture_lkppc
                       ELSE exp_clm_trans_logic1.o_yearofmanufacture
                END AS o_yearofmanufacture,
                CASE
                       WHEN exp_clm_trans_logic1.o_deductibleindicator IS NULL THEN exp_clm_trans_logic1.deductibleindicator_lkppc
                       ELSE exp_clm_trans_logic1.o_deductibleindicator
                END AS o_deductibleindicator,
                CASE
                       WHEN exp_clm_trans_logic1.o_deductibleamount IS NULL THEN exp_clm_trans_logic1.deductibleamount_lkppc
                       ELSE exp_clm_trans_logic1.o_deductibleamount
                END                                   AS o_deductibleamount,
                exp_clm_trans_logic1.policyidentifier AS policyidentifier,
                exp_clm_trans_logic1.source_record_id
         FROM   exp_clm_trans_logic1 );
  -- Component exp_default1, Type EXPRESSION
  CREATE
  OR
  replace TEMPORARY TABLE exp_default1 AS
  (
         SELECT
                CASE
                       WHEN exp_clm_trans_logic.companynumber IS NULL THEN ''0000''
                       ELSE lpad ( exp_clm_trans_logic.companynumber , 4 , ''0'' )
                END                                    AS o_companynumber,
                exp_clm_trans_logic.lineofbusinesscode AS lineofbusinesscode,
                CASE
                       WHEN exp_clm_trans_logic.statecode IS NULL THEN ''00''
                       ELSE exp_clm_trans_logic.statecode
                END                                AS o_statecode,
                exp_clm_trans_logic.callyear       AS callyear,
                exp_clm_trans_logic.accountingyear AS accountingyear,
                exp_clm_trans_logic.expperiodyear  AS expperiodyear,
                exp_clm_trans_logic.expperiodmonth AS expperiodmonth,
                exp_clm_trans_logic.expeperiodday  AS expeperiodday,
                exp_clm_trans_logic.coveragecode   AS coveragecode,
                CASE
                       WHEN exp_clm_trans_logic.o_territorycode IS NULL THEN ''00''
                       ELSE lpad ( exp_clm_trans_logic.o_territorycode , 2 , ''0'' )
                END AS o_territorycode,
                CASE
                       WHEN exp_clm_trans_logic.o_zipcode IS NULL THEN ''00000''
                       ELSE lpad ( exp_clm_trans_logic.o_zipcode , 5 , ''0'' )
                END                                     AS o_zipcode,
                exp_clm_trans_logic.policyeffectiveyear AS policyeffectiveyear,
                exp_clm_trans_logic.newrecordformat     AS newrecordformat,
                exp_clm_trans_logic.aslob               AS aslob,
                exp_clm_trans_logic.itemcode            AS itemcode,
                exp_clm_trans_logic.sublinecode         AS sublinecode,
                CASE
                       WHEN exp_clm_trans_logic.o_protectioncasscode IS NULL THEN ''00''
                       ELSE lpad ( exp_clm_trans_logic.o_protectioncasscode , 2 , ''0'' )
                END AS o_protectioncasscode,
                CASE
                       WHEN exp_clm_trans_logic.o_typeofdeductiblecode IS NULL THEN ''00''
                       ELSE lpad ( exp_clm_trans_logic.o_typeofdeductiblecode , 2 , ''0'' )
                END AS o_typeofdeductiblecode,
                CASE
                       WHEN exp_clm_trans_logic.policytermcode IS NULL THEN ''00''
                       ELSE lpad ( exp_clm_trans_logic.policytermcode , 2 , ''0'' )
                END AS o_policytermcode,
                CASE
                       WHEN exp_clm_trans_logic.typeoflosscode IS NULL THEN ''00''
                       ELSE lpad ( exp_clm_trans_logic.typeoflosscode , 2 , ''0'' )
                END                              AS o_typeoflosscode,
                exp_clm_trans_logic.locationcode AS locationcode,
                CASE
                       WHEN exp_clm_trans_logic.o_amountofinsurance IS NULL THEN ''00000''
                       ELSE lpad ( exp_clm_trans_logic.o_amountofinsurance , 5 , ''0'' )
                END AS o_amountofinsurance,
                CASE
                       WHEN exp_clm_trans_logic.o_yearofmanufacture IS NULL THEN ''0000''
                       ELSE exp_clm_trans_logic.o_yearofmanufacture
                END                             AS o_yeaofmanufacture,
                exp_clm_trans_logic.tiedowncode AS tiedowncode,
                CASE
                       WHEN exp_clm_trans_logic.o_deductibleindicator IS NULL THEN ''0''
                       ELSE exp_clm_trans_logic.o_deductibleindicator
                END AS o_deductibleindicator,
                CASE
                       WHEN exp_clm_trans_logic.o_deductibleamount IS NULL THEN ''0000000''
                       ELSE lpad ( exp_clm_trans_logic.o_deductibleamount , 7 , ''0'' )
                END                                       AS o_deductibleamount,
                exp_clm_trans_logic.deductibleindicatorws AS deductibleindicatorws,
                CASE
                       WHEN exp_clm_trans_logic.deductibleamountws IS NULL THEN ''0000000''
                       ELSE lpad ( exp_clm_trans_logic.deductibleamountws , 7 , ''0'' )
                END AS o_deductibleamountws,
                CASE
                       WHEN exp_clm_trans_logic.claimidentifier IS NULL THEN ''000000000000000''
                       ELSE lpad ( exp_clm_trans_logic.claimidentifier , 15 , ''0'' )
                END AS o_claimidentifier,
                CASE
                       WHEN exp_clm_trans_logic.claimantidentifier IS NULL THEN ''000''
                       ELSE lpad ( exp_clm_trans_logic.claimantidentifier , 3 , ''0'' )
                END AS o_claimantidentifier,
                CASE
                       WHEN exp_clm_trans_logic.writtenexposure IS NULL THEN ''000000000000''
                       ELSE exp_clm_trans_logic.writtenexposure
                END AS o_writtenexposure,
                CASE
                       WHEN exp_clm_trans_logic.writtenpremium IS NULL THEN ''000000000000''
                       ELSE rpad ( exp_clm_trans_logic.writtenpremium , 12 , ''0'' )
                END AS o_writtenpremium,
                CASE
                       WHEN (
                                     exp_clm_trans_logic.paidlosses IS NULL
                              OR     exp_clm_trans_logic.paidlosses = ''0'' ) THEN ''000000000000''
                       ELSE exp_clm_trans_logic.paidlosses
                END AS o_paidlosses,
                CASE
                       WHEN (
                                     exp_clm_trans_logic.paidnumberofclaims IS NULL
                              OR     exp_clm_trans_logic.paidnumberofclaims = ''0'' ) THEN ''000000000000''
                       ELSE exp_clm_trans_logic.paidnumberofclaims
                END AS o_paidnumberofclaims,
                CASE
                       WHEN (
                                     exp_clm_trans_logic.outstandinglosses IS NULL
                              OR     exp_clm_trans_logic.outstandinglosses = ''0'' ) THEN ''000000000000''
                       ELSE exp_clm_trans_logic.outstandinglosses
                END AS o_outstandinglosses,
                CASE
                       WHEN (
                                     exp_clm_trans_logic.outstandingnoofclaims IS NULL
                              OR     exp_clm_trans_logic.outstandingnoofclaims = ''0'' ) THEN ''000000000000''
                       ELSE lpad ( exp_clm_trans_logic.outstandingnoofclaims , 12 , ''0'' )
                END                              AS o_outstandingnoofclaims,
                exp_clm_trans_logic.policynumber AS policynumber,
                CASE
                       WHEN exp_clm_trans_logic.policyperiodid IS NULL THEN ''''
                       ELSE rpad ( exp_clm_trans_logic.policyperiodid , 20 , ''0'' )
                END AS o_policyperiodid,
                CASE
                       WHEN exp_clm_trans_logic.policyidentifier IS NULL THEN ''00000000000000000000''
                       ELSE lpad ( exp_clm_trans_logic.policyidentifier , 20 , ''0'' )
                END               AS o_policyidentifier,
                current_timestamp AS creationts,
                ''0''               AS creationuid,
                current_timestamp AS updatets,
                ''0''               AS updateuid,
                exp_clm_trans_logic.source_record_id
         FROM   exp_clm_trans_logic );
  -- Component OUT_NAIIPCI_MH1, Type TARGET
  INSERT INTO db_t_prod_comn.out_naiipci_mh
              (
                          companynumber,
                          lineofbusinesscode,
                          statecode,
                          callyear,
                          accountingyear,
                          expperiodyear,
                          expperiodmonth,
                          expeperiodday,
                          coveragecode,
                          territorycode,
                          zipcode,
                          policyeffectiveyear,
                          newrecordformat,
                          aslob,
                          itemcode,
                          sublinecode,
                          protectioncasscode,
                          typeofdeductiblecode,
                          policytermcode,
                          typeoflosscode,
                          locationcode,
                          amountofinsurance,
                          yeaofmanufacture,
                          tiedowncode,
                          deductibleindicator,
                          deductibleamount,
                          deductibleindicatorws,
                          deductibleamountws,
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
  SELECT exp_default1.o_companynumber         AS companynumber,
         exp_default1.lineofbusinesscode      AS lineofbusinesscode,
         exp_default1.o_statecode             AS statecode,
         exp_default1.callyear                AS callyear,
         exp_default1.accountingyear          AS accountingyear,
         exp_default1.expperiodyear           AS expperiodyear,
         exp_default1.expperiodmonth          AS expperiodmonth,
         exp_default1.expeperiodday           AS expeperiodday,
         exp_default1.coveragecode            AS coveragecode,
         exp_default1.o_territorycode         AS territorycode,
         exp_default1.o_zipcode               AS zipcode,
         exp_default1.policyeffectiveyear     AS policyeffectiveyear,
         exp_default1.newrecordformat         AS newrecordformat,
         exp_default1.aslob                   AS aslob,
         exp_default1.itemcode                AS itemcode,
         exp_default1.sublinecode             AS sublinecode,
         exp_default1.o_protectioncasscode    AS protectioncasscode,
         exp_default1.o_typeofdeductiblecode  AS typeofdeductiblecode,
         exp_default1.o_policytermcode        AS policytermcode,
         exp_default1.o_typeoflosscode        AS typeoflosscode,
         exp_default1.locationcode            AS locationcode,
         exp_default1.o_amountofinsurance     AS amountofinsurance,
         exp_default1.o_yeaofmanufacture      AS yeaofmanufacture,
         exp_default1.tiedowncode             AS tiedowncode,
         exp_default1.o_deductibleindicator   AS deductibleindicator,
         exp_default1.o_deductibleamount      AS deductibleamount,
         exp_default1.deductibleindicatorws   AS deductibleindicatorws,
         exp_default1.o_deductibleamountws    AS deductibleamountws,
         exp_default1.o_claimidentifier       AS claimidentifier,
         exp_default1.o_claimantidentifier    AS claimantidentifier,
         exp_default1.o_writtenexposure       AS writtenexposure,
         exp_default1.o_writtenpremium        AS writtenpremium,
         exp_default1.o_paidlosses            AS paidlosses,
         exp_default1.o_paidnumberofclaims    AS paidnumberofclaims,
         exp_default1.o_outstandinglosses     AS outstandinglosses,
         exp_default1.o_outstandingnoofclaims AS outstandingnoofclaims,
         exp_default1.policynumber            AS policynumber,
         exp_default1.o_policyperiodid        AS policyperiodid,
         exp_default1.creationts              AS creationts,
         exp_default1.creationuid             AS creationuid,
         exp_default1.updatets                AS updatets,
         exp_default1.updateuid               AS updateuid,
         exp_default1.o_policyidentifier      AS policyidentifier
  FROM   exp_default1;
  
  -- PIPELINE END FOR 2
END;
';