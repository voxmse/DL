import sys
sys.path.append(".")
from dlschema.encoder import DlNull, DlInteger, DlDouble, DlDecimal, DlString, DlDate, DlTime, DlDatetime, DlTimestamp, DlBoolean, DlRaw, DlList, DlDictionary, jsondata
import unittest
from decimal import Decimal
import datetime
import json

if __name__ == '__main__':
    v = [
        DlDictionary({"ID": DlInteger(1), "NAME": DlString("Sherlock Holmes"), "ADDRESS": DlString("221b, Baker Street, London, NW1 6XE, UK", 200), "SALARY": DlDecimal(Decimal("12.45"), 16, 2), "TAXES": DlDecimal(Decimal("687192025652473624789243787872498713367.89012")), "BIRTHDAY": DlDate(datetime.date(2020, 1, 3)), "VISIT_TS": DlTimestamp(datetime.datetime.now()), "IS_MARRIED": DlBoolean(False), "CHILDREN": DlList([]), "TEMPERATURE": DlDictionary({"OUTDOOR": DlDouble(2.3), "INDOOR": DlDouble(21.8)}), "COMPLEX": DlDictionary({"FILENAME": DlString("FILE1.TXT"), "FILEBODY": DlRaw("s4pQ")}), "DEEP": DlList([DlDictionary({"A": DlInteger(11), "B": DlInteger(12)}), DlDictionary({"C": DlInteger(31)})])}),
        DlDictionary({"ID": DlInteger(1), "NAME": DlString("John H. Watson"), "ADDRESS": DlString(None), "SALARY": DlDecimal(Decimal("1245.12"), 16, 2), "TAXES": DlString("0.45"), "BIRTHDAY": DlDate(datetime.date(1987, 12, 16)), "VISIT_TS": DlTimestamp(None), "CHILDREN": DlList([DlString("John Watson Jr")]), "TEMPERATURE": DlDictionary({"OUTDOOR": DlDouble(2.3), "INDOOR": DlDouble(21.8)}), "COMPLEX": DlDictionary({"FILENAME": DlString("FILE1.TXT"), "FILEBODY": DlRaw("s4pQ")}), "DEEP": DlList([DlDictionary({"A": DlInteger(1), "B": DlInteger(2)}), DlDictionary({"A": DlInteger(3), "B": DlInteger(4)})])})
    ]    

    for d in v:
        print(jsondata(d)[0])