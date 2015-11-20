import logging
import types
import json
from collections import OrderedDict
from splitstream import splitfile
import re

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

def json_sample(path, n_objs = 10):
    """ Tries to get n_objs objects from a json file
        Returns a json string with sample"""
    # probes file to see if it is an array of objects or not
    
    with open(path, 'r') as f:
        s = f.read(500).lstrip() 
    # this matches an array and some class inside
    # an array and then some atomic types will not be sampled
    is_array = re.match("^\s*\[\s*\{", s)

    with open(path, 'r') as f:
        if is_array:
            gen = splitfile(f, format="json", startdepth=1)
        else:
            gen = splitfile(f, format="json")
        objs = []
        for n in range(n_objs):
            try:
                sample = next(gen)
                objs.append(sample)
            except StopIteration: 
                print n
                if (n < 1):
                    raise ValueError("Empty json Array")
                break

    # Do we want to convert this file to an array all the time ?
    # should we return the eof?
    if is_array:
       return "[\n%s\n]" % "\n,".join(objs) 
    else:
        return"\n".join(objs) 
