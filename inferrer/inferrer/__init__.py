# Infer schema from a local file

from csv_inferrer import CSVInferrer, csv_sample
from json_inferrer import JSONInferrer, json_sample
from .common import InferrerException
from raw_types import *

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
    
