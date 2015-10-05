import logging
import types
import json
from collections import OrderedDict

from .common import *
from raw_types import *


class JSONInferrer(object):
    def __init__(self, name, content):
        self._name = name
        self._json = json.loads(content, object_pairs_hook=OrderedDict)

        # This hides the original exception making it harder to debug

    #        try:
    #            self._json = json.loads(content, object_pairs_hook=OrderedDict)
    #        except Exception as e:
    #            raise ParseException

    def infer_type(self):
        return self.__infer_type(self._json), None

    def __infer_type(self, j):
        if j is None:
            return rawSomeType()
        if isinstance(j, types.BooleanType):
            # Boolean must be checked first, as types.BooleanType is also of types.IntType
            return rawBooleanType()
        if isinstance(j, types.IntType):
            return rawIntType()
        if isinstance(j, types.FloatType):
            return rawFloatType()
        if isinstance(j, types.StringType) or isinstance(j, unicode):
            return rawStringType()
        if isinstance(j, OrderedDict):
            inner_type = OrderedDict((k, self.__infer_type(j[k])) for k in j)
            return rawRecordType(self._name, inner_type)
        if isinstance(j, types.ListType):
            inner_type = rawSomeType()
            for item in j:
                t = self.__infer_type(item)
                if inner_type.compatible_with(t):
                    inner_type = inner_type.max_of(t)
                else:
                    logging.error("%s incompatible with %s" % (inner_type, t))
                    raise TypeInferenceException(j)
            return rawListType(inner_type)

        raise TypeInferenceException(json.dumps(j, indent=4))
