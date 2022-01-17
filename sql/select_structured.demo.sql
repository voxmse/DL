DECLARE var_path_data ARRAY<STRUCT<path STRING, types STRING>>;
DECLARE var_path_data_deep ARRAY<STRUCT<path STRING, types STRING>>;
DECLARE var_types_children STRING;

-- unified source raw data structure. Normally is stored as a persistent table
CREATE TEMPORARY TABLE source_data AS
SELECT
    TIMESTAMP('2020-01-01 18:30:12') sys_record_ts,
    TIMESTAMP('2020-01-01 18:30:10') sys_event_ts,
    'dfcb4730a046d455e56b10f0d9f37bb6449e2d1b' AS sys_hash,
    NULL sys_cdc_event,
    NULL sys_meta,
    '{"ADDRESS:s:200":"221b, Baker Street, London, NW1 6XE, UK","BIRTHDAY:d":"2020-01-03","CHILDREN":[],"COMPLEX":{"FILEBODY:x":"s4pQ","FILENAME:s":"FILE1.TXT"},"DEEP":[{"A:i":11,"B:i":12},{"C:i":31}],"ID:i":1,"IS_MARRIED:b":false,"NAME:s":"Sherlock Holmes","SALARY:n:16:2":12.45,"TAXES:n":92437878367.89012,"TEMPERATURE":{"INDOOR:e":21.8,"OUTDOOR:e":2.3},"VISIT_TS:z":"2022-01-16 23:27:36.979902"}' sys_data
UNION ALL
SELECT
    TIMESTAMP('2020-01-01 18:30:12') sys_record_ts,
    TIMESTAMP('2020-01-01 18:30:10') sys_event_ts,
    'c6a259885b5aefd382473c15021f190e2839fa4d' AS sys_hash,
    NULL sys_cdc_event,
    NULL sys_meta,
    '{"ADDRESS:s":null,"BIRTHDAY:d":"1987-12-16","CHILDREN":[{":s":"John Watson Jr"}],"COMPLEX":{"FILEBODY:x":"s4pQ","FILENAME:s":"FILE1.TXT"},"DEEP":[{"A:i":1,"B:i":2},{"A:i":3,"B:i":4}],"ID:i":1,"NAME:s":"John H. Watson","SALARY:n:16:2":1245.12,"TAXES:s":"0.45","TEMPERATURE":{"INDOOR:e":21.8,"OUTDOOR:e":2.3},"VISIT_TS:z":null}' sys_data
;

-- paths to extract column data. Normally is stored as a persistent table
CREATE TEMPORARY TABLE path_data AS
          SELECT 'TEST' source_name, 'TABLE1' table_name, TRUE  active, FALSE optional,       NULL prefix,                'DEEP' path, 'a'  types
UNION ALL SELECT 'TEST' source_name, 'TABLE1' table_name, TRUE  active, FALSE optional,       NULL prefix,                  'ID' path, 'i'  types          
UNION ALL SELECT 'TEST' source_name, 'TABLE1' table_name, TRUE  active, FALSE optional,       NULL prefix,            'BIRTHDAY' path, 'd'  types
UNION ALL SELECT 'TEST' source_name, 'TABLE1' table_name, TRUE  active, FALSE optional,       NULL prefix,            'VISIT_TS' path, 'z'  types
UNION ALL SELECT 'TEST' source_name, 'TABLE1' table_name, TRUE  active, FALSE optional,       NULL prefix,          'IS_MARRIED' path, 'b'  types
UNION ALL SELECT 'TEST' source_name, 'TABLE1' table_name, TRUE  active, FALSE optional,       NULL prefix,                'NAME' path, 's'  types
UNION ALL SELECT 'TEST' source_name, 'TABLE1' table_name, TRUE  active, FALSE optional,       NULL prefix,             'ADDRESS' path, 's'  types
UNION ALL SELECT 'TEST' source_name, 'TABLE1' table_name, TRUE  active, FALSE optional,       NULL prefix,              'SALARY' path, 'n'  types
UNION ALL SELECT 'TEST' source_name, 'TABLE1' table_name, TRUE  active, FALSE optional,       NULL prefix,               'TAXES' path, 'ns' types
UNION ALL SELECT 'TEST' source_name, 'TABLE1' table_name, TRUE  active, FALSE optional,       NULL prefix,    'COMPLEX.FILEBODY' path, 'x'  types
UNION ALL SELECT 'TEST' source_name, 'TABLE1' table_name, TRUE  active, FALSE optional,       NULL prefix,    'COMPLEX.FILENAME' path, 's'  types
UNION ALL SELECT 'TEST' source_name, 'TABLE1' table_name, TRUE  active, FALSE optional,       NULL prefix,  'TEMPERATURE.INDOOR' path, 'e'  types
UNION ALL SELECT 'TEST' source_name, 'TABLE1' table_name, TRUE  active, FALSE optional,       NULL prefix, 'TEMPERATURE.OUTDOOR' path, 'e'  types
UNION ALL SELECT 'TEST' source_name, 'TABLE1' table_name, TRUE  active, FALSE optional,       NULL prefix,            'CHILDREN' path, 'a'  types
UNION ALL SELECT 'TEST' source_name, 'TABLE1' table_name, FALSE active, FALSE optional, 'CHILDREN' prefix,                  ''   path, 's'  types
UNION ALL SELECT 'TEST' source_name, 'TABLE1' table_name, TRUE  active, FALSE optional,       NULL prefix,          'WRONG_PATH' path, 's'  types
UNION ALL SELECT 'TEST' source_name, 'TABLE1' table_name, TRUE  active, FALSE optional,     'DEEP' prefix,                   'A' path, 'i'  types
UNION ALL SELECT 'TEST' source_name, 'TABLE1' table_name, TRUE  active, FALSE optional,     'DEEP' prefix,                   'B' path, 'i'  types
UNION ALL SELECT 'TEST' source_name, 'TABLE1' table_name, TRUE  active, FALSE optional,     'DEEP' prefix,                   'C' path, 'i'  types
-- UNION ALL SELECT 'TEST' source_name, 'WRONG_TYPE' path, 'q' types
;



-- function to parse row using array of path elements to array of values. Normally is created as persistent
CREATE TEMP FUNCTION parseJson(jsonData STRING, pathData ARRAY<STRUCT<path STRING, types STRING>>) RETURNS ARRAY<STRUCT<type STRING, value STRING>> LANGUAGE js AS """
    const TYPEPROPERTY = 'tNpTfVlk3kRJRDnE7kEGVQlVldp2Og';
    const REFPROPERTY = 'Bi66fkj0XChgG2F1aB94d9kZgVFfyw';
    // extract value for path
    function getValues(data, pathArr, types) {
        var obj = data;
        for (var i=0; i<pathArr.length; i++) {
            if (obj !== null && obj.hasOwnProperty(pathArr[i])) {
                obj = obj[pathArr[i]];
                if (i==pathArr.length-1 && types.indexOf(obj[TYPEPROPERTY])>=0) {
                    return [obj[TYPEPROPERTY], obj[REFPROPERTY]];
                }                    
            } else {;
                return null;
            }
        }
        
        return null;
    }
    // create aux structure with keys without suffixes
    function fillSearchModel(obj, model) {
        for (var k in obj) {
            if (obj[k] === null) {
                var karr = k.split(":");
                var val = new Object();
                val[TYPEPROPERTY] = karr[1];
                val[REFPROPERTY] = obj[k];
                model[karr[0]] = val;
            } else if (Array.isArray(obj[k])) {
                var val = new Object();
                val[TYPEPROPERTY] = 'a';
                val[REFPROPERTY] = obj[k];
                model[k] = val;
            } else {
                switch (typeof obj[k]) {
                    case 'string':
                    case 'number':
                    case 'boolean':
                        var karr = k.split(":");
                        var val = new Object();
                        val[TYPEPROPERTY] = karr[1];
                        val[REFPROPERTY] = obj[k];
                        model[karr[0]] = val;
                        break;    
                    default:
                        model[k] = new Object();
                        model[k][TYPEPROPERTY] = 'm';
                        model[k][REFPROPERTY] = obj[k];
                        fillSearchModel(obj[k], model[k]);
                }
            }
        }
    };
    // parse JSON data
    var objdata = JSON.parse(jsonData);
    // fill aux structure
    var model = Object();
    fillSearchModel(objdata, model);
    // get data for each search path
    var resarr = new Array(pathData.length);
    for (var i = 0; i < pathData.length; i++) {
        var res = getValues(model, pathData[i].path.split('.'), pathData[i].types);
        resarr[i] = (res === null) ? {type: null, value: null} : {type: res[0], value: ('dtzpsx'.indexOf(res[0])>=0 ? res[1] : JSON.stringify(res[1]))};
    }
    return resarr;
""";

-- extract INT value from results of parsing
CREATE TEMP FUNCTION getType(target_path STRING, path_arr ARRAY<STRUCT<path STRING, types STRING>>, data_arr ARRAY<STRUCT<type STRING, value STRING>>) RETURNS STRING AS
  ((WITH guess AS(SELECT ((SELECT offset idx FROM UNNEST(path_arr)t WITH OFFSET WHERE t.path=target_path)) AS idx)
    SELECT CASE WHEN idx IS NOT NULL THEN data_arr[OFFSET(idx)].type END FROM guess)); 
    
-- decode INT value 
CREATE TEMP FUNCTION decodeInteger(data STRUCT<type STRING, value STRING>, valid_types STRING, default_value INT64) RETURNS INT64 AS
    (CASE WHEN data.type<>'i' OR INSTR(valid_types, 'i')<=0 THEN default_value ELSE CAST(data.value AS INT64) END);

-- decode STRING value 
CREATE TEMP FUNCTION decodeString(data STRUCT<type STRING, value STRING>, valid_types STRING, default_value STRING) RETURNS STRING AS
    (CASE WHEN data.type<>'s' OR INSTR(valid_types, 's')<=0 THEN default_value ELSE data.value END);

-- extract INT value from results of parsing
CREATE TEMP FUNCTION getInteger(target_path STRING, path_arr ARRAY<STRUCT<path STRING, types STRING>>, data_arr ARRAY<STRUCT<type STRING, value STRING>>, default_value INT64) RETURNS INT64 AS
  ((WITH guess AS(SELECT ((SELECT offset idx FROM UNNEST(path_arr)t WITH OFFSET WHERE t.path=target_path)) AS idx)
    SELECT CASE WHEN idx IS NULL OR data_arr[OFFSET(idx)].type IS NULL OR data_arr[OFFSET(idx)].type<>'i' THEN default_value ELSE CAST(data_arr[OFFSET(idx)].value AS INT64) END FROM guess));     

-- extract DOUBLE value from results of parsing
CREATE TEMP FUNCTION getDouble(target_path STRING, path_arr ARRAY<STRUCT<path STRING, types STRING>>, data_arr ARRAY<STRUCT<type STRING, value STRING>>, default_value FLOAT64) RETURNS FLOAT64 AS
  ((WITH guess AS(SELECT ((SELECT offset idx FROM UNNEST(path_arr)t WITH OFFSET WHERE t.path=target_path)) AS idx)
    SELECT CASE WHEN idx IS NULL OR data_arr[OFFSET(idx)].type IS NULL OR data_arr[OFFSET(idx)].type<>'e' THEN default_value ELSE CAST(data_arr[OFFSET(idx)].value AS FLOAT64) END FROM guess)); 

-- extract DECIMAL value from results of parsing
CREATE TEMP FUNCTION getDecimal(target_path STRING, path_arr ARRAY<STRUCT<path STRING, types STRING>>, data_arr ARRAY<STRUCT<type STRING, value STRING>>, default_value NUMERIC) RETURNS NUMERIC AS
  ((WITH guess AS(SELECT ((SELECT offset idx FROM UNNEST(path_arr)t WITH OFFSET WHERE t.path=target_path)) AS idx)
    SELECT CASE WHEN idx IS NULL OR data_arr[OFFSET(idx)].type IS NULL OR data_arr[OFFSET(idx)].type<>'n' THEN default_value ELSE PARSE_NUMERIC(data_arr[OFFSET(idx)].value) END FROM guess));     

-- extract STRING value from results of parsing
CREATE TEMP FUNCTION getString(target_path STRING, path_arr ARRAY<STRUCT<path STRING, types STRING>>, data_arr ARRAY<STRUCT<type STRING, value STRING>>, default_value STRING) RETURNS STRING AS
  ((WITH guess AS(SELECT ((SELECT offset idx FROM UNNEST(path_arr)t WITH OFFSET WHERE t.path=target_path)) AS idx)
    SELECT CASE WHEN idx IS NULL OR data_arr[OFFSET(idx)].type IS NULL OR data_arr[OFFSET(idx)].type<>'s' THEN default_value ELSE data_arr[OFFSET(idx)].value END FROM guess));     

-- extract DATE value from results of parsing
CREATE TEMP FUNCTION getDate(target_path STRING, path_arr ARRAY<STRUCT<path STRING, types STRING>>, data_arr ARRAY<STRUCT<type STRING, value STRING>>, default_value DATE) RETURNS DATE AS
  ((WITH guess AS(SELECT ((SELECT offset idx FROM UNNEST(path_arr)t WITH OFFSET WHERE t.path=target_path)) AS idx)
    SELECT CASE WHEN idx IS NULL OR data_arr[OFFSET(idx)].type IS NULL OR data_arr[OFFSET(idx)].type<>'d' THEN default_value ELSE CAST(data_arr[OFFSET(idx)].value AS DATE) END FROM guess));  

-- extract TIMESTAMP value from results of parsing
CREATE TEMP FUNCTION getTimestamp(target_path STRING, path_arr ARRAY<STRUCT<path STRING, types STRING>>, data_arr ARRAY<STRUCT<type STRING, value STRING>>, default_value TIMESTAMP) RETURNS TIMESTAMP AS
  ((WITH guess AS(SELECT ((SELECT offset idx FROM UNNEST(path_arr)t WITH OFFSET WHERE t.path=target_path)) AS idx)
    SELECT CASE WHEN idx IS NULL OR data_arr[OFFSET(idx)].type IS NULL OR data_arr[OFFSET(idx)].type<>'z' THEN default_value ELSE CAST(data_arr[OFFSET(idx)].value AS TIMESTAMP) END FROM guess)); 
    
-- extract RAW value from results of parsing
CREATE TEMP FUNCTION getRaw(target_path STRING, path_arr ARRAY<STRUCT<path STRING, types STRING>>, data_arr ARRAY<STRUCT<type STRING, value STRING>>, default_value BYTES) RETURNS BYTES AS
  ((WITH guess AS(SELECT ((SELECT offset idx FROM UNNEST(path_arr)t WITH OFFSET WHERE t.path=target_path)) AS idx)
    SELECT CASE WHEN idx IS NULL OR data_arr[OFFSET(idx)].type IS NULL OR data_arr[OFFSET(idx)].type<>'x' THEN default_value ELSE FROM_BASE64(data_arr[OFFSET(idx)].value) END FROM guess));     

-- extract BOOLEAN value from results of parsing
CREATE TEMP FUNCTION getBoolean(target_path STRING, path_arr ARRAY<STRUCT<path STRING, types STRING>>, data_arr ARRAY<STRUCT<type STRING, value STRING>>, default_value BOOL) RETURNS BOOL AS
  ((WITH guess AS(SELECT ((SELECT offset idx FROM UNNEST(path_arr)t WITH OFFSET WHERE t.path=target_path)) AS idx)
    SELECT CASE WHEN idx IS NULL OR data_arr[OFFSET(idx)].type IS NULL OR data_arr[OFFSET(idx)].type<>'b' THEN default_value ELSE CAST(data_arr[OFFSET(idx)].value AS BOOL) END FROM guess));
    
-- extract ARRAY value from results of parsing
CREATE TEMP FUNCTION getArray(target_path STRING, path_arr ARRAY<STRUCT<path STRING, types STRING>>, data_arr ARRAY<STRUCT<type STRING, value STRING>>, default_value ARRAY<STRUCT<type STRING, value STRING>>) RETURNS ARRAY<STRUCT<type STRING, value STRING>> AS
  ((WITH guess AS(SELECT ((SELECT offset idx FROM UNNEST(path_arr)t WITH OFFSET WHERE t.path=target_path)) AS idx)
    SELECT
      CASE
        WHEN idx IS NULL OR data_arr[OFFSET(idx)].type IS NULL OR data_arr[OFFSET(idx)].type<>'a' THEN default_value
        ELSE ((
          SELECT
              ARRAY_AGG(
              CASE
                WHEN t LIKE '{":%' THEN
                  CASE
                    WHEN SUBSTR(t,5,7)='":null}' THEN STRUCT(SUBSTR(t,4,1) AS type, CAST(NULL AS STRING) AS value)
                    WHEN SUBSTR(t,4,1) IN ('d','t','z','p','s','x') THEN STRUCT(SUBSTR(t,4,1) AS type, SUBSTR(t,8,LENGTH(t)-9) AS value)
                    ELSE STRUCT(SUBSTR(t,4,1) AS type, SUBSTR(t,7,LENGTH(t)-7) AS value)
                  END
                ELSE
                  CASE
                    WHEN t LIKE '[%' THEN STRUCT('a' AS type, t AS value)
                    ELSE STRUCT('m' AS type, t AS value)
                  END
              END
              ORDER BY offset
            )
          FROM
            UNNEST(JSON_QUERY_ARRAY(data_arr[OFFSET(idx)].value))t WITH OFFSET))          
      END FROM guess));  

-- normally this code should be wrapped into a table function
SET var_path_data = ((
  SELECT
    ARRAY_AGG(STRUCT(path, types) ORDER BY path) AS path_arr
  FROM 
    path_data
  WHERE
    source_name='TEST'
    AND table_name='TABLE1'
    AND prefix IS NULL
    AND active));

SET var_path_data_deep  = ((
  SELECT
    ARRAY_AGG(STRUCT(path, types) ORDER BY path) AS path_arr
  FROM 
    path_data
  WHERE
    source_name='TEST'
    AND table_name='TABLE1'
    AND prefix='DEEP'
    AND active));

SET var_types_children  = ((
  SELECT
    types
  FROM 
    path_data
  WHERE
    source_name='TEST'
    AND table_name='TABLE1'
    AND prefix='CHILDREN'
    AND path=''
    AND active));

-- principal query
-- parse JSON data
WITH parsed_data AS (
  SELECT
    source_data.* EXCEPT(sys_data),
    parseJson(sys_data, var_path_data) parsed_arr
  FROM
    source_data
)
-- extract column values
SELECT
  parsed_data.* EXCEPT(parsed_arr),
  -- get column values of simple types from the results of parsing
  getString('NAME', var_path_data, parsed_arr, NULL) AS name,
  getString('ADDRESS', var_path_data, parsed_arr, NULL) AS address,
  getDate('BIRTHDAY', var_path_data, parsed_arr, NULL) AS birthday,
  getInteger('ID', var_path_data, parsed_arr, NULL) AS id,
  -- replace absent(not NULL) attribute by a default value
  getBoolean('IS_MARRIED', var_path_data, parsed_arr, FALSE) AS is_married,
  -- two different source types
  CASE getType('TAXES', var_path_data, parsed_arr) 
    WHEN 'n' THEN getDecimal('TAXES', var_path_data, parsed_arr, NULL)
    ELSE CAST(getString('TAXES', var_path_data, parsed_arr, NULL) AS NUMERIC)
  END  AS taxes,  -- struct column type
  getDecimal('SALARY', var_path_data, parsed_arr, NULL) AS salary,
  getTimestamp('VISIT_TS', var_path_data, parsed_arr, NULL) AS visit_ts,
  STRUCT(getDouble('TEMPERATURE.INDOOR', var_path_data, parsed_arr, NULL) AS INDOOR, getDouble('TEMPERATURE.OUTDOOR', var_path_data, parsed_arr, NULL) AS OUTDOOR) AS temperature,
  STRUCT(getRaw('COMPLEX.FILEBODY', var_path_data, parsed_arr, NULL) AS FILEBODY, getString('COMPLEX.FILENAME', var_path_data, parsed_arr, NULL) AS FILENAME) AS complex,
  -- get simple array values. Take care about NULL values of array elements(not supported by BQ)
  (SELECT ARRAY_AGG(decodeString(elm, var_types_children, 'UNKNOWN') ORDER BY offset) FROM UNNEST(getArray('CHILDREN', var_path_data, parsed_arr, []))elm WITH OFFSET WHERE elm.value<>'null') AS children,
  -- parse JSON of ARRAY<STRUCT<A STRING,B STRING,C STRING>>, extract results and transform to BQ data types
  (
    SELECT ARRAY_AGG(
      ((SELECT AS STRUCT 
          getInteger('A', var_path_data_deep, elm_parsed, NULL) AS A,
          getInteger('B', var_path_data_deep, elm_parsed, NULL) AS B,
          getInteger('C', var_path_data_deep, elm_parsed, NULL) AS C
        FROM
          (
            SELECT
              CASE
                WHEN elm.type='m' THEN parseJson(elm.value, var_path_data_deep)
                ELSE [STRUCT(CAST(NULL AS STRING)AS type, CAST(NULL AS STRING) AS value)]
              END elm_parsed
          )t
      ))
      ORDER BY offset
    )
    FROM UNNEST(getArray('DEEP', var_path_data, parsed_arr, NULL))elm WITH OFFSET
  ) AS deep
FROM
  parsed_data
;