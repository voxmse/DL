CREATE TEMPORARY TABLE source_data AS
SELECT
    CURRENT_TIMESTAMP() sys_ingestion_ts,
    CURRENT_TIMESTAMP() sys_event_ts,
    '' AS sys_hash,
    NULL sys_meta
    CAST(NULL AS STRING) AS sys_cdc,
    

-- fails if col_path.name is not unique, is NULL or is empty
DECLARE _Z DEFAULT (
  SELECT AS STRUCT
    col_path,
    (
      SELECT
        CONCAT(',', STRING_AGG(name ORDER BY offset), ',') AS search_buf
      FROM (
        SELECT
          cp.name,
          offset,
          1 / CASE WHEN COUNT(*)OVER(PARTITION BY cp.name)>1 OR cp.name IS NULL OR LENGTH(cp.name)=0 THEN 0 ELSE 1 END AS tst
        FROM
          UNNEST(col_path) AS cp WITH OFFSET    
    )t
    HAVING SUM(tst)>0) AS search_buf
);


CREATE TEMP FUNCTION parseRow(jsonData STRING, pathData ARRAY<STRUCT<path STRING, types STRING>>) RETURNS ARRAY<STRUCT<type STRING, value STRING>> LANGUAGE js AS """
    const TYPEPROPERTY = 'tNpTfVlk3kRJRDnE7kEGVQlVldp2Og';
    const REFPROPERTY = 'Bi66fkj0XChgG2F1aB94d9kZgVFfyw';
    // extract value for path
    function getValues(data, pathArr, types) {
        var obj = data;
        for (var i=0; i<pathArr.length; i++) {
            if (obj !== null && obj.hasOwnProperty(pathArr[i])) {
                obj = obj[pathArr[i]];
                if (i==pathArr.length-1) {
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
            } else if (obj instanceof Array) {
                var val = new Object();
                val[TYPEPROPERTY] = 'a';
                val[REFPROPERTY] = obj[k];
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


SELECT parseRow(
    '{"ADDRESS:s:200":"221b, Baker Street, London, NW1 6XE, UK","BIRTHDAY:d":"2020-01-03","CHILDREN:s":null,"COMPLEX":{"FILEBODY:x":"s4pQ","FILENAME:s":"FILE1.TXT"},"DEEP":[{"A:i":null,"B:i":12},{"C:i":31}],"ID:i":1,"IS_MARRIED:b":false,"NAME:s":"Sherlock Holmes","SALARY:n:16:2":"12.45","TAXES:n":"687192025652473624789243787872498713367.89012","TEMPERATURE":{"INDOOR:e":21.8,"OUTDOOR:e":2.3},"VISIT_TS:z":"2022-01-07 21:51:57.205231"}',
    [STRUCT('DEEP' AS path, 'a' AS types), STRUCT('BIRTHDAY' AS path, 'd' AS types), STRUCT('IS_MARRIED' AS path, 'b' AS types), STRUCT('COMPLEX.FILEBODY' AS path, 'x' AS types), STRUCT('COMPLEX.FILENAME' AS path, 's' AS types), STRUCT('CHILDREN' AS path, 's' AS types)]
);

WITH raw AS (
  SELECT '{"ADDRESS:s:200":"221b, Baker Street, London, NW1 6XE, UK","BIRTHDAY:d":"2020-01-03","CHILDREN":[],"DEEP":[{"A:i":11,"B:i":12},{"C:i":31}],"ID:i":1,"IS_MARRIED:b":false,"NAME:s":"Sherlock Holmes","SALARY:n:16:2":"12.45","TAXES:n":"687192025652473624789243787872498713367.89012","TEMPERATURE":{"INDOOR:e":21.8,"OUTDOOR:e":2.3},"VISIT_TS:z":"2022-01-05 21:48:59.080790"}' row_data
  UNION ALL
  SELECT '{"ADDRESS:s":null,"BIRTHDAY:d":"1987-12-16","CHILDREN":[{":s":"John Watson Jr"}],"DEEP":[{"A:i":1,"B:i":2},{"A:i":3,"B:i":4}],"ID:i":1,"NAME:s":"John H. Watson","SALARY:n:16:2":"1245.12","TAXES:s":"0.45","TEMPERATURE":{"INDOOR:e":21.8,"OUTDOOR:e":2.3},"VISIT_TS:z":null}'
)