# Infer schema from a local file

from csv_inferrer import CSVInferrer, csv_sample
from json_inferrer import JSONInferrer, json_sample
from .common import InferrerException
from raw_types import *

def check_types(rawType):
    """ Check if all types are defined """
    if isinstance(rawType, rawIntType) \
        or isinstance(rawType, rawStringType) \
        or isinstance(rawType, rawFloatType) \
        or isinstance(rawType, rawBooleanType):
        return True
    elif isinstance(rawType, rawListType):
        return check_types(rawType.desc)
    elif isinstance(rawType, rawRecordType):
        for (k, v) in rawType.desc.items():
            check_types(v)
    else:
        # ValueException is unknown, I switched it to ValueError
        raise InferrerException("Unknown type: %s" % rawType)

def from_local(path, file_type, n_objs=10):
    if file_type == 'json':
        inferrer = JSONInferrer(json_sample(path, n_objs))
    elif file_type == 'csv':
        inferrer = CSVInferrer(csv_sample(path, n_objs))
    elif file_type == 'text':
        return (rawListType(rawStringType()), {})
    else:
        raise ValueError("Type not supported")
    return inferrer.infer_type()
    
