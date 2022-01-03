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
| LIST | | | "name": [{":i":1}, {":s":"ABC"}] | dictionary as wrapper for elements |
| DICTIONARY | | | "name": {"field1:i": 1, "field2:s": "ABC", "field3": [{":d": "2020-01-01"}]} | |

Only terminal elements have datatype suffix concatenated with name.
