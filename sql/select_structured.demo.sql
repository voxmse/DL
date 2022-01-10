DECLARE var_source_name STRING DEFAULT 'TEST';
DECLARE var_path_data ARRAY<STRUCT<path STRING, types STRING>>;
DECLARE _Z BYTES;

-- unified source raw data structure. Normally is stored as a persistent table
CREATE TEMPORARY TABLE source_data AS
SELECT
    TIMESTAMP('2020-01-01 18:30:12') sys_record_ts,
    TIMESTAMP('2020-01-01 18:30:10') sys_event_ts,
    'dfcb4730a046d455e56b10f0d9f37bb6449e2d1b' AS sys_hash,
    NULL sys_cdc_event,
    NULL sys_meta,
    '{"ADDRESS:s:200":"221b, Baker Street, London, NW1 6XE, UK","BIRTHDAY:d":"2020-01-03","CHILDREN":[],"COMPLEX":{"FILEBODY:x":"s4pQ","FILENAME:s":"FILE1.TXT"},"DEEP":[{"A:i":11,"B:i":12},{"C:i":31}],"ID:i":1,"IS_MARRIED:b":false,"NAME:s":"Sherlock Holmes","SALARY:n:16:2":"12.45","TAXES:n":"687192025652473624789243787872498713367.89012","TEMPERATURE":{"INDOOR:e":21.8,"OUTDOOR:e":2.3},"VISIT_TS:z":"2022-01-09 18:23:48.919507"}' sys_data
UNION ALL
SELECT
    TIMESTAMP('2020-01-01 18:30:12') sys_record_ts,
    TIMESTAMP('2020-01-01 18:30:10') sys_event_ts,
    'c6a259885b5aefd382473c15021f190e2839fa4d' AS sys_hash,
    NULL sys_cdc_event,
    NULL sys_meta,
    '{"ADDRESS:s":null,"BIRTHDAY:d":"1987-12-16","CHILDREN":[{":s":"John Watson Jr"}],"COMPLEX":{"FILEBODY:x":"s4pQ","FILENAME:s":"FILE1.TXT"},"DEEP":[{"A:i":1,"B:i":2},{"A:i":3,"B:i":4}],"ID:i":1,"NAME:s":"John H. Watson","SALARY:n:16:2":"1245.12","TAXES:s":"0.45","TEMPERATURE":{"INDOOR:e":21.8,"OUTDOOR:e":2.3},"VISIT_TS:z":null}' sys_data
;

-- paths to extract column data. Normally is stored as a persistent table
CREATE TEMPORARY TABLE data_path AS
SELECT
    'TEST' source_name,
    'DEEP' path,
    'a' types
UNION ALL
SELECT
    'TEST' source_name,
    'BIRTHDAY' path,
    'd' types
UNION ALL
SELECT
    'TEST' source_name,
    'IS_MARRIED' path,
    'b' types
UNION ALL
SELECT
    'TEST' source_name,
    'COMPLEX.FILEBODY' path,
    'x' types
UNION ALL
SELECT
    'TEST' source_name,
    'COMPLEX.FILENAME' path,
    's' types
UNION ALL
SELECT
    'TEST' source_name,
    'CHILDREN' path,
    's' types
UNION ALL
SELECT
    'TEST' source_name,
    'WRONG_PATH' path,
    's' types
-- UNION ALL
-- SELECT
--     'TEST' source_name,
--     'WRONG_TYPE' path,
--     'q' types
;


-- array of data_path elements ordered by data_path.path
-- fails if col_path.name is not unique or a type is wrong. Normally is created as a table function
SET var_path_data = (
  SELECT
    ARRAY_AGG(STRUCT(path, types) ORDER BY path) AS path_data
  FROM (
    SELECT
      path,
      types,
      1 / CASE WHEN COUNT(*)OVER(PARTITION BY path)>1 OR EXISTS (SELECT t FROM UNNEST(SPLIT(types, ''))t WHERE t NOT IN('a','b','d','e','i','n','o','p','s','t','x','z')) THEN 0 ELSE 1 END AS tst
    FROM
      data_path
    WHERE
      source_name='TEST'  
  )t
  HAVING SUM(tst)>0
);

-- string of concatenated ',' + data_path.path + ':' + ROW_NUMBER() + ',' values in the same order as elements of the above-mentioned array
SET _Z = (
  SELECT 
     CAST(CONCAT(',', STRING_AGG(CONCAT(t.path, ':', CAST(offset AS STRING)) ORDER BY t.path), ',') AS BYTES)
  FROM
      UNNEST(var_path_data) AS t WITH OFFSET
);



-- function to parse row using array of path elements to array of values. Normally is created as persistent
CREATE TEMP FUNCTION parseRow(jsonData STRING, pathData ARRAY<STRUCT<path STRING, types STRING>>) RETURNS ARRAY<STRUCT<type STRING, value STRING>> LANGUAGE js AS """
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
                val[TYPEPROPERTY] = karr.slice(1);
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
                        val[TYPEPROPERTY] = karr.slice(1);
                        val[REFPROPERTY] = obj[k];
                        model[karr[0]] = val;
                        break;    
                    default:
                        model[k] = new Object();
                        model[k][TYPEPROPERTY] = 'o';
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


WITH
    parsed AS(
        SELECT
            


parseRow(
    '{"ADDRESS:s:200":"221b, Baker Street, London, NW1 6XE, UK","BIRTHDAY:d":"2020-01-03","CHILDREN:s":null,"COMPLEX":{"FILEBODY:x":"s4pQ","FILENAME:s":"FILE1.TXT"},"DEEP":[{"A:i":null,"B:i":12},{"C:i":31}],"ID:i":1,"IS_MARRIED:b":false,"NAME:s":"Sherlock Holmes","SALARY:n:16:2":"12.45","TAXES:n":"687192025652473624789243787872498713367.89012","TEMPERATURE":{"INDOOR:e":21.8,"OUTDOOR:e":2.3},"VISIT_TS:z":"2022-01-07 21:51:57.205231"}',
    [STRUCT('DEEP' AS path, 'a' AS types), STRUCT('BIRTHDAY' AS path, 'd' AS types), STRUCT('IS_MARRIED' AS path, 'b' AS types), STRUCT('COMPLEX.FILEBODY' AS path, 'x' AS types), STRUCT('COMPLEX.FILENAME' AS path, 's' AS types), STRUCT('CHILDREN' AS path, 's' AS types)]
);

WITH raw AS (
  SELECT '{"ADDRESS:s:200":"221b, Baker Street, London, NW1 6XE, UK","BIRTHDAY:d":"2020-01-03","CHILDREN":[],"DEEP":[{"A:i":11,"B:i":12},{"C:i":31}],"ID:i":1,"IS_MARRIED:b":false,"NAME:s":"Sherlock Holmes","SALARY:n:16:2":"12.45","TAXES:n":"687192025652473624789243787872498713367.89012","TEMPERATURE":{"INDOOR:e":21.8,"OUTDOOR:e":2.3},"VISIT_TS:z":"2022-01-05 21:48:59.080790"}' row_data
  UNION ALL
  SELECT '{"ADDRESS:s":null,"BIRTHDAY:d":"1987-12-16","CHILDREN":[{":s":"John Watson Jr"}],"DEEP":[{"A:i":1,"B:i":2},{"A:i":3,"B:i":4}],"ID:i":1,"NAME:s":"John H. Watson","SALARY:n:16:2":"1245.12","TAXES:s":"0.45","TEMPERATURE":{"INDOOR:e":21.8,"OUTDOOR:e":2.3},"VISIT_TS:z":null}'
)