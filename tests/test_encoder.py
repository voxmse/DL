from dlschema.encoder import DlNull, DlInteger, DlDouble, DlDecimal, DlString, DlDate, DlTime, DlDatetime, DlTimestamp, DlBoolean, DlList, DlDictionary, jsondata
import unittest
from decimal import Decimal
import datetime
import json


class TestNull(unittest.TestCase):
    def test_typecode(self):
        v = DlNull()
        self.assertEqual(v.typecode, "o")

    def test_fulltypecode(self):
        v = DlNull()
        self.assertEqual(v.fulltypecode, "o")

    def test_typename(self):
        v = DlNull()
        self.assertEqual(v.typename, "NULL")

    def test_fulltypename(self):
        v = DlNull()
        self.assertEqual(v.fulltypename, "NULL")

    def test_jsonvalue(self):
        v = DlNull()
        self.assertIsNone(v.jsonvalue)


class TestInteger(unittest.TestCase):
    def test_typecode(self):
        v = DlInteger(12)
        self.assertEqual(v.typecode, "i")

    def test_fulltypecode(self):
        v = DlInteger(12)
        self.assertEqual(v.fulltypecode, "i")

    def test_typename(self):
        v = DlInteger(12)
        self.assertEqual(v.typename, "INTEGER")

    def test_fulltypename(self):
        v = DlInteger(12)
        self.assertEqual(v.fulltypename, "INTEGER")

    def test_jsonvalue(self):
        v = DlInteger(12)
        self.assertEqual(v.jsonvalue, 12)

    def test_jsonvaluenone(self):
        v = DlInteger(None)
        self.assertIsNone(v.jsonvalue)


class TestDouble(unittest.TestCase):
    def test_typecode(self):
        v = DlDouble(12.3456789)
        self.assertEqual(v.typecode, "e")

    def test_fulltypecode(self):
        v = DlDouble(12.3456789)
        self.assertEqual(v.fulltypecode, "e")

    def test_typename(self):
        v = DlDouble(12.3456789)
        self.assertEqual(v.typename, "DOUBLE")

    def test_fulltypename(self):
        v = DlDouble(12.3456789)
        self.assertEqual(v.fulltypename, "DOUBLE")

    def test_jsonvalue(self):
        v = DlDouble(12.3456789)
        self.assertEqual(v.jsonvalue, 12.3456789)

    def test_jsonvaluenone(self):
        v = DlDouble(None)
        self.assertIsNone(v.jsonvalue)


class TestDeciamal(unittest.TestCase):
    def test_typecode(self):
        v = DlDecimal(Decimal("12.3"))
        self.assertEqual(v.typecode, "n")

    def test_fulltypecode(self):
        v = DlDecimal(Decimal("12.3"))
        self.assertEqual(v.fulltypecode, "n")

    def test_fulltypecode2(self):
        v = DlDecimal(Decimal("12.3"),16)
        self.assertEqual(v.fulltypecode, "n:16")

    def test_fulltypecode3(self):
        v = DlDecimal(Decimal("12.3"), 16, 2)
        self.assertEqual(v.fulltypecode, "n:16:2")

    def test_typename(self):
        v = DlDecimal(Decimal("12.3"))
        self.assertEqual(v.typename, "DECIMAL")

    def test_fulltypename(self):
        v = DlDecimal(Decimal("12.3"))
        self.assertEqual(v.fulltypename, "DECIMAL")

    def test_fulltypename2(self):
        v = DlDecimal(Decimal("12.3"), 16)
        self.assertEqual(v.fulltypename, "DECIMAL(16)")

    def test_fulltypename3(self):
        v = DlDecimal(Decimal("12.3"), 16, 2)
        self.assertEqual(v.fulltypename, "DECIMAL(16, 2)")

    def test_precision(self):
        v = DlDecimal(Decimal("12.3"), 16)
        self.assertEqual(v.precision, 16)

    def test_precision2(self):
        v = DlDecimal(Decimal("12.3"), 16, 2)
        self.assertEqual(v.precision, 16)

    def test_scale(self):
        v = DlDecimal(Decimal("12.3"), 16, 2)
        self.assertEqual(v.scale, 2)

    def test_init1(self):
        with self.assertRaises(ValueError):
            v = DlDecimal(Decimal("12.3"), -1)

    def test_init2(self):
        with self.assertRaises(ValueError):
            v = DlDecimal(Decimal("12.3"), None, 1)

    def test_init3(self):
        with self.assertRaises(ValueError):
            v = DlDecimal(Decimal("12.3"), 3, 5)

    def test_init4(self):
        with self.assertRaises(ValueError):
            v = DlDecimal(Decimal("12.3"), 2, 1)

    def test_init5(self):
        with self.assertRaises(ValueError):
            v = DlDecimal(Decimal("12.3"), 5, 0)

    def test_jsonvalue(self):
        v = DlDecimal(Decimal("12.3"))
        self.assertEqual(v.jsonvalue, Decimal("12.3"))

    def test_jsonvaluenone(self):
        v = DlDecimal(None)
        self.assertIsNone(v.jsonvalue)


class TestString(unittest.TestCase):
    def test_typecode(self):
        v = DlString("ABC")
        self.assertEqual(v.typecode, "s")

    def test_fulltypecode(self):
        v = DlString("ABC")
        self.assertEqual(v.fulltypecode, "s")

    def test_typename(self):
        v = DlString("ABC")
        self.assertEqual(v.typename, "STRING")

    def test_fulltypename(self):
        v = DlString("ABC")
        self.assertEqual(v.fulltypename, "STRING")

    def test_fulltypename(self):
        v = DlString("ABC", 10)
        self.assertEqual(v.fulltypename, "STRING(10)")

    def test_typelength(self):
        v = DlString("ABC", 10)
        self.assertEqual(v.typelength, 10)

    def test_init1(self):
        with self.assertRaises(ValueError):
            v = DlString("ABC", 0)

    def test_init2(self):
        with self.assertRaises(ValueError):
            v = DlString("ABC", 2)

    def test_jsonvalue(self):
        v = DlString("ABC")
        self.assertEqual(v.jsonvalue, "ABC")

    def test_jsonvaluenone(self):
        v = DlString(None)
        self.assertIsNone(v.jsonvalue)


class TestDate(unittest.TestCase):
    def test_typecode(self):
        v = DlDate(datetime.date(2020, 3, 12))
        self.assertEqual(v.typecode, "d")

    def test_fulltypecode(self):
        v = DlDate(datetime.date(2020, 3, 12))
        self.assertEqual(v.fulltypecode, "d")

    def test_typename(self):
        v = DlDate(datetime.date(2020, 3, 12))
        self.assertEqual(v.typename, "DATE")

    def test_fulltypename(self):
        v = DlDate(datetime.date(2020, 3, 12))
        self.assertEqual(v.fulltypename, "DATE")

    def test_jsonvalue(self):
        v = DlDate(datetime.date(2020, 3, 12))
        self.assertEqual(v.jsonvalue, "2020-03-12")

    def test_jsonvaluenone(self):
        v = DlDate(None)
        self.assertIsNone(v.jsonvalue)


class TestTime(unittest.TestCase):
    def test_typecode(self):
        v = DlTime(datetime.time(20, 3, 12, 456789))
        self.assertEqual(v.typecode, "t")

    def test_fulltypecode(self):
        v = DlTime(datetime.time(20, 3, 12, 456789))
        self.assertEqual(v.fulltypecode, "t")

    def test_typename(self):
        v = DlTime(datetime.time(20, 3, 12, 456789))
        self.assertEqual(v.typename, "TIME")

    def test_fulltypename(self):
        v = DlTime(datetime.time(20, 3, 12, 456789))
        self.assertEqual(v.fulltypename, "TIME")

    def test_jsonvalue(self):
        v = DlTime(datetime.time(20, 3, 12, 456789))
        self.assertEqual(v.jsonvalue, "20:03:12.456789")

    def test_jsonvaluenone(self):
        v = DlTime(None)
        self.assertIsNone(v.jsonvalue)


class TestDatetime(unittest.TestCase):
    def test_typecode(self):
        v = DlDatetime(datetime.datetime(2020, 9, 17, 14, 3, 12, 456789))
        self.assertEqual(v.typecode, "p")

    def test_fulltypecode(self):
        v = DlDatetime(datetime.datetime(2020, 9, 17, 14, 3, 12, 456789))
        self.assertEqual(v.fulltypecode, "p")

    def test_typename(self):
        v = DlDatetime(datetime.datetime(2020, 9, 17, 14, 3, 12, 456789))
        self.assertEqual(v.typename, "DATETIME")

    def test_fulltypename(self):
        v = DlDatetime(datetime.datetime(2020, 9, 17, 14, 3, 12, 456789))
        self.assertEqual(v.fulltypename, "DATETIME")

    def test_jsonvalue(self):
        v = DlDatetime(datetime.datetime(2020, 9, 17, 14, 3, 12, 456789))
        self.assertEqual(v.jsonvalue, "2020-09-17 14:03:12.456789")

    def test_jsonvaluenone(self):
        v = DlDatetime(None)
        self.assertIsNone(v.jsonvalue)


class TestTimestamp(unittest.TestCase):
    def test_typecode(self):
        v = DlTimestamp(datetime.datetime(2020, 9, 17, 14, 3, 12, 456789, datetime.timezone(datetime.timedelta(seconds=3600))))
        self.assertEqual(v.typecode, "z")

    def test_fulltypecode(self):
        v = DlTimestamp(datetime.datetime(2020, 9, 17, 14, 3, 12, 456789, datetime.timezone(datetime.timedelta(seconds=3600))))
        self.assertEqual(v.fulltypecode, "z")

    def test_typename(self):
        v = DlTimestamp(datetime.datetime(2020, 9, 17, 14, 3, 12, 456789, datetime.timezone(datetime.timedelta(seconds=3600))))
        self.assertEqual(v.typename, "TIMESTAMP")

    def test_fulltypename(self):
        v = DlTimestamp(datetime.datetime(2020, 9, 17, 14, 3, 12, 456789, datetime.timezone(datetime.timedelta(seconds=3600))))
        self.assertEqual(v.fulltypename, "TIMESTAMP")

    def test_jsonvalue(self):
        v = DlTimestamp(datetime.datetime(2020, 9, 17, 14, 3, 12, 456789, datetime.timezone(datetime.timedelta(seconds=3600))))
        self.assertEqual(v.jsonvalue, "2020-09-17 14:03:12.456789+0100")

    def test_jsonvaluenone(self):
        v = DlTimestamp(None)
        self.assertIsNone(v.jsonvalue)


class TestBoolean(unittest.TestCase):
    def test_typecode(self):
        v = DlBoolean(True)
        self.assertEqual(v.typecode, "b")

    def test_fulltypecode(self):
        v = DlBoolean(True)
        self.assertEqual(v.fulltypecode, "b")

    def test_typename(self):
        v = DlBoolean(True)
        self.assertEqual(v.typename, "BOOLEAN")

    def test_fulltypename(self):
        v = DlBoolean(True)
        self.assertEqual(v.fulltypename, "BOOLEAN")

    def test_jsonvalue(self):
        v = DlBoolean(True)
        self.assertEqual(v.jsonvalue, True)

    def test_jsonvaluenone(self):
        v = DlBoolean(None)
        self.assertIsNone(v.jsonvalue)


class TestList(unittest.TestCase):
    def test_typecode(self):
        v = DlList([DlInteger(1), DlInteger(2), DlDate(datetime.date(2020, 1, 3)), DlString(None)])
        self.assertEqual(v.typecode, "l")

    def test_fulltypecode(self):
        v = DlList([DlInteger(1), DlInteger(2), DlDate(datetime.date(2020, 1, 3)), DlString(None)])
        self.assertEqual(v.fulltypecode, "l")

    def test_typename(self):
        v = DlList([DlInteger(1), DlInteger(2), DlDate(datetime.date(2020, 1, 3)), DlString(None)])
        self.assertEqual(v.typename, "LIST")

    def test_fulltypename(self):
        v = DlList([DlInteger(1), DlInteger(2), DlDate(datetime.date(2020, 1, 3)), DlString(None)])
        self.assertEqual(v.fulltypename, "LIST")

    def test_jsonvalue(self):
        v = DlList([DlString("AAA"), DlList([DlInteger(1), DlDate(datetime.date(2020, 1, 3)), DlString(None), DlDictionary({"C1": DlInteger(1), "C2": DlDate(datetime.date(2020, 1, 3))})])])
        self.assertEqual(v.jsonvalue, [{':s': 'AAA'}, [{':i': 1}, {':d': '2020-01-03'}, {':s': None}, {'C1:i': 1, 'C2:d': '2020-01-03'}]])

    def test_initnone(self):
        with self.assertRaises(ValueError):
            v = DlList(None)

    def test_initnone2(self):
        with self.assertRaises(ValueError):
            v = DlList([None])


class TestDictionary(unittest.TestCase):
    def test_typecode(self):
        v = DlDictionary({"C1": DlInteger(1), "C2": DlDate(datetime.date(2020, 1, 3)), "C3": DlString(None), "C4": DlList([DlInteger(2), DlDate(datetime.date(2020, 1, 3)), DlString(None)]), "C5": DlDictionary({"C51": DlString("HHH")})})
        self.assertEqual(v.typecode, "m")

    def test_fulltypecode(self):
        v = DlDictionary({"C1": DlInteger(1), "C2": DlDate(datetime.date(2020, 1, 3)), "C3": DlString(None), "C4": DlList([DlInteger(2), DlDate(datetime.date(2020, 1, 3)), DlString(None)]), "C5": DlDictionary({"C51": DlString("HHH")})})
        self.assertEqual(v.fulltypecode, "m")

    def test_typename(self):
        v = DlDictionary({"C1": DlInteger(1), "C2": DlDate(datetime.date(2020, 1, 3)), "C3": DlString(None), "C4": DlList([DlInteger(2), DlDate(datetime.date(2020, 1, 3)), DlString(None)]), "C5": DlDictionary({"C51": DlString("HHH")})})
        self.assertEqual(v.typename, "DICTIONARY")

    def test_fulltypename(self):
        v = DlDictionary({"C1": DlInteger(1), "C2": DlDate(datetime.date(2020, 1, 3)), "C3": DlString(None), "C4": DlList([DlInteger(2), DlDate(datetime.date(2020, 1, 3)), DlString(None)]), "C5": DlDictionary({"C51": DlString("HHH")})})
        self.assertEqual(v.fulltypename, "DICTIONARY")

    def test_jsonvalue(self):
        v = DlDictionary({"C1": DlInteger(1), "C2": DlDate(datetime.date(2020, 1, 3)), "C3": DlString(None), "C4": DlList([DlInteger(2), DlDate(datetime.date(2020, 1, 3)), DlString(None)]), "C5": DlDictionary({"C51": DlString("HHH")})})
        self.assertEqual(json.dumps(v.jsonvalue), '{"C1:i": 1, "C2:d": "2020-01-03", "C3:s": null, "C4": [{":i": 2}, {":d": "2020-01-03"}, {":s": null}], "C5": {"C51:s": "HHH"}}')

    def test_initnone(self):
        with self.assertRaises(ValueError):
            v = DlDictionary(None)

    def test_initnone2(self):
        with self.assertRaises(ValueError):
            v = DlDictionary({'a': None})

class TestUtil(unittest.TestCase):
    def test_jsondata(self):
        v = DlDictionary({"C2": DlDate(datetime.date(2020, 1, 3)), "C1": DlInteger(1), "C3": DlString(None), "C4": DlList([DlInteger(2), DlDate(datetime.date(2020, 1, 3)), DlString(None)]), "C5": DlDictionary({"C51": DlString("HHH")})})
        self.assertEqual(jsondata(v), ('{"C1:i":1,"C2:d":"2020-01-03","C3:s":null,"C4":[{":i":2},{":d":"2020-01-03"},{":s":null}],"C5":{"C51:s":"HHH"}}', '2de76179ab2bbe63c1412b1bab782266fa82322f'))


if __name__ == '__main__':
    unittest.main()