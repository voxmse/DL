from typing import Any, List, Optional, Dict, Union, Tuple
from decimal import Decimal
from datetime import date, datetime, time
from base64 import b64decode
from json import dumps, JSONEncoder
from hashlib import sha1
from copy import deepcopy
from abc import ABC, abstractmethod


class DlBaseData(ABC):
    def __init__(self, value: Any, ):
        self._value = value

    @property
    def typecode(self):
        pass

    @property
    def fulltypecode(self) -> str:
        return self.typecode

    @property
    def iscontainer(self) -> bool:
        return False

    @property
    def typename(self) -> str:
        pass

    @property
    def fulltypename(self) -> str:
        return self.typename

    @property
    def jsonvalue(self) -> Union[str, int, float, Decimal, bool, Dict, List, None]:
        return self._value


class DlNull(DlBaseData):
    def __init__(self):
        DlBaseData.__init__(self, None)

    @property
    def typecode(self):
        return "o"
    
    @property
    def typename(self) -> str:
        return "NULL"


class DlInteger(DlBaseData):
    def __init__(self, value: Optional[int]):
        DlBaseData.__init__(self, value)

    @property
    def typecode(self):
        return "i"

    @property
    def typename(self) -> str:
        return "INTEGER"


class DlDouble(DlBaseData):
    def __init__(self, value: Optional[float]):
        DlBaseData.__init__(self, value)

    @property
    def typecode(self):
        return "e"

    @property
    def typename(self) -> str:
        return "DOUBLE"


class DecimalEncoder(JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return str(o)
        return super(DecimalEncoder, self).default(o)
        
class DlDecimal(DlBaseData):
    def __init__(self, value: Optional[Decimal], precision: Optional[int]=None, scale: Optional[int]=None):
        if precision is None and not scale is None: raise ValueError(f"scale({scale}) needs precision")
        if scale is not None and (scale < 0): raise ValueError(f"scale({scale}) should be positive")
        if precision is not None and (precision < 1 or (scale is not None and precision<scale)): raise ValueError(f"precision({precision}) should be positive and not less then scale")
        if value is not None and (precision is not None or scale is not None):
            sign, digits, exp = value.normalize().as_tuple()
            if precision is not None and precision < len(digits): raise ValueError(f"declared precision({precision}) is not enought to store the value({len(digits)})")
            if scale is not None and scale < -exp if exp<0 else 0: raise ValueError(f"declared scale({scale}) is not enought to store the value({value})")
        DlBaseData.__init__(self, value)
        self._precision = precision
        self._scale = scale

    @property
    def typecode(self):
        return "n"

    @property
    def typename(self) -> str:
        return "DECIMAL"

    @property
    def precision(self) -> Optional[int]:
        return self._precision

    @property
    def scale(self) -> Optional[int]:
        return self._scale

    @property
    def fulltypename(self) -> str:
        if self._precision is None: return self.typename
        else:
            if self._scale is None: return self.typename + "(" + str(self._precision) + ")"
            else: return self.typename + "(" + str(self._precision) + ", " + str(self._scale) + ")"

    @property
    def fulltypecode(self) -> str:
        if self._precision is None: return self.typecode
        else:
            if self._scale is None: return self.typecode + ":" + str(self._precision)
            else: return self.typecode + ":" + str(self._precision) + ":" + str(self._scale)


class DlString(DlBaseData):
    def __init__(self, value: Optional[str], typelength: Optional[int]=None):
        if typelength is not None and typelength < 0: raise ValueError(f"typelength({typelength}) should be positive")
        if value is not None and typelength is not None and typelength < len(value): raise ValueError(f"declared typelength({typelength}) is not enought to store the value({value})")
        DlBaseData.__init__(self, value)
        self._typelength = typelength

    @property
    def typecode(self):
        return "s"

    @property
    def typename(self) -> str:
        return "STRING"

    @property
    def typelength(self) -> Optional[int]:
        return self._typelength

    @property
    def fulltypename(self) -> str:
        if self._typelength is None: return self.typename
        else: return self.typename + "(" + str(self._typelength) + ")"

    @property
    def fulltypecode(self) -> str:
        if self._typelength is None: return self.typecode
        else: return self.typecode + ":" + str(self._typelength)


class DlDate(DlBaseData):
    def __init__(self, value: Optional[date]):
        DlBaseData.__init__(self, value)

    @property
    def typecode(self):
        return "d"

    @property
    def typename(self) -> str:
        return "DATE"

    @property
    def jsonvalue(self) -> Union[str, int, float, Decimal, bool, Dict, List, None]:
        return None if self._value is None else self._value.strftime("%Y-%m-%d")


class DlTime(DlBaseData):
    def __init__(self, value: Optional[time]):
        DlBaseData.__init__(self, value.replace(tzinfo=None) if value else None)

    @property
    def typecode(self):
        return "t"

    @property
    def typename(self) -> str:
        return "TIME"

    @property
    def jsonvalue(self) -> Union[str, int, float, Decimal, bool, Dict, List, None]:
        return None if self._value is None else self._value.strftime("%H:%M:%S.%f")


class DlDatetime(DlBaseData):
    def __init__(self, value: Optional[datetime]):
        DlBaseData.__init__(self, value.replace(tzinfo=None) if value else None)

    @property
    def typecode(self):
        return "p"

    @property
    def typename(self) -> str:
        return "DATETIME"

    @property
    def jsonvalue(self) -> Union[str, int, float, Decimal, bool, Dict, List, None]:
        return None if self._value is None else self._value.strftime("%Y-%m-%d %H:%M:%S.%f")


class DlTimestamp(DlBaseData):
    def __init__(self, value: Optional[datetime]):
        DlBaseData.__init__(self, value)

    @property
    def typecode(self):
        return "z"

    @property
    def typename(self) -> str:
        return "TIMESTAMP"

    @property
    def jsonvalue(self) -> Union[str, int, float, Decimal, bool, Dict, List, None]:
        return None if self._value is None else self._value.strftime("%Y-%m-%d %H:%M:%S.%f%z")


class DlBoolean(DlBaseData):
    def __init__(self, value: Optional[bool]):
        DlBaseData.__init__(self, value)

    @property
    def typecode(self):
        return "b"

    @property
    def typename(self) -> str:
        return "BOOLEAN"


class DlRaw(DlBaseData):
    def __init__(self, value: Optional[str], typelength: Optional[int]=None):
        if typelength is not None and typelength < 0: raise ValueError(f"typelength({typelength}) should be positive")
        if value is not None and typelength is not None and typelength < ceil(len(value)/1.5): raise ValueError(f"declared typelength({typelength}) is not enought to store the value({ceil(len(value)/1.5)})")    
        if value is not None:
            try:
                b64decode(value, validate=True)
            except Exception:
                raise ValueError("Is not BASE64-encoded")
        DlBaseData.__init__(self, value)
        self._typelength = typelength    

    @property
    def typecode(self):
        return "x"

    @property
    def typename(self) -> str:
        return "RAW"


class DlList(DlBaseData):
    def __init__(self, value: List[DlBaseData]):
        if value is None: raise ValueError("List is None")
        if None in value: raise ValueError("List can not contain None values")
        DlBaseData.__init__(self, value)

    @property
    def typecode(self):
        return "l"

    @property
    def typename(self) -> str:
        return "LIST"

    @property
    def iscontainer(self) -> bool:
        return True

    @property
    def lenght(self) -> int:
        return None if self._value is None else len(self._value)

    @property
    def jsonvalue(self) -> Union[str, int, float, Decimal, bool, Dict, List, None]:
        return [elm.jsonvalue if elm.iscontainer else {":" + elm.fulltypecode : elm.jsonvalue} for elm in self._value]
        
    @property
    def add(self, elm: DlBaseData):
        self._value.append(elm)


class DlDictionary(DlBaseData):
    def __init__(self, value: Dict[str, DlBaseData]):
        if value is None: raise ValueError("Dictionary is None")
        if None in value.values(): raise ValueError("Dictionary can not contain None values")
        DlBaseData.__init__(self, value)

    @property
    def typecode(self):
        return "m"

    @property
    def typename(self) -> str:
        return "DICTIONARY"

    @property
    def iscontainer(self) -> bool:
        return True

    @property
    def lenght(self) -> Optional[int]:
        return None if self._value is None else len(self._value)

    @property
    def jsonvalue(self) -> Union[str, int, float, Decimal, bool, Dict, List, None]:
        return {k if v.iscontainer else k + ":" + v.fulltypecode : v.jsonvalue for k,v in self._value.items()} if self._value else None

    @property
    def put(self, key: str, val: DlBaseData):
        self._value[key] = val


def jsondata(value: DlDictionary) -> Tuple[str, str]:
    hash = sha1()
    jsonstring = dumps(value.jsonvalue, separators=(",", ":"), sort_keys=True, cls=DecimalEncoder)
    hash.update(jsonstring.encode('utf-8'))
    return (jsonstring, hash.hexdigest())
