import logging
import types
import json
from collections import OrderedDict
from splitstream import splitfile

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

def as_array(path):
    """ Checks if the file is to be treated as an Array or not by the json_sample
         the splitfile as a problem if the internal type is atomic 
         so it also checks if the internal type is atomic or a class """
    with open(path, 'r') as f:
        s = f.read(500).lstrip() 
    is_array = s[0] == '['
    #this is a hack, it will try to check if there is a { inside, if so then it is a class
    #TODO: try a better way to parse this 
    is_class = "{" in s 
    return is_array and is_class

def json_sample(path, n_objs = 10):
    """ Tries to get n_objs objects from a json file
        Returns a json string with sample"""
    # probes file to see if it is an array of objects or not
    is_array = as_array(path)
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
