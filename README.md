## DlSchema

Storage of arbitrary data in JSON format preserving elementary source datatypes' attributes

## Datatypes

| Datatype | Code | Attributes | JSON example | note |
| :---: | :---: | --- | --- | --- |
| NULL | o | | "name:n": null | |
| INTEGER | i | | "name:i": 12 | |
| DOUBLE | e | | "name:e": 12.3456789 | |
| DECIMAL | n | precision, scale | "name:n:16:2": 123.45 | attributes are optional |
| STRING | s | typelength | "name:s:120": "ABC" | attribute is optional |
| DATE | d | | "name:d": "2020-01-28" | |
| TIME | t | | "name:t": "21:08:59.234576" | |
| DATETIME | p | | "name:p": "2020-01-28 21:08:59.234576" | |
| TIMESTAMP | z | | "name:z": "2020-01-28 21:08:59.234576+02:00" | |
| BOOLEAN | b | | "name:b": True | |
| RAW | x | typelength | "name:x": "1c24aH"| BASE64, attribute is optional |
| LIST | a | | "name": [{":i":1}, {":s":"ABC"}] | dictionary as wrapper for elements |
| DICTIONARY | o | | "name": {"field1:i": 1, "field2:s": "ABC", "field3": [{":d": "2020-01-01"}]} | |

Only terminal elements have datatype suffix concatenated with name.

## Datalake RAW data schema

| Name | Type(Bigquery) | NOT NULL | Comments |
| --- | --- | --- | --- |
| sys_record_ts | TIMESTAMP | Y | When was registered |
| sys_event_ts | TIMESTAMP | | When was induced. May be derived from row data later |
| sys_hash | STRING | Y | Hash of data |
| sys_meta | STRING | | Aux information in JSON format |
| sys_cdc_event | STRING | | CDC action code for CDC sources(C/U/D) |
| sys_data | STRING | Y | JSON-encoded data+schema | 

## JSON data extraction paths schema

| Name | Type(Bigquery) | NOT NULL | Comments |
| --- | --- | --- | --- |
| source_name | STRING | Y | source name |
| table_name | TIMESTAMP | Y | table name |
| active | BOOLEAN | Y | use for parsing of JSON |
| prefix | STRING | | path's prefix. Is used for two-stage extraction of complex array data |
| path | STRING | Y | Dot-separated path to the target field |
| types | STRING | Y | set of allowed for the path types: "dtzs" |
