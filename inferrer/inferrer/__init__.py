# Infer schema from a local file

from csv_inferrer import CSVInferrer, csv_sample
from json_inferrer import JSONInferrer, json_sample
from .common import InferrerException
from raw_types import *

def from_local(path, file_type, n_objs=10):
    if file_type == 'json':
        inferrer = JSONInferrer(json_sample(path, n_objs, file_format="json"))
        return inferrer.infer_type(), None
    if file_type == 'hjson':
        inferrer = JSONInferrer(json_sample(path, n_objs, file_format="hjson"))
        return inferrer.infer_type(), None
    elif file_type == 'csv':
        inferrer = CSVInferrer(csv_sample(path, n_objs))
        return inferrer.infer_type()
    elif file_type == 'text':
        return (rawListType(rawStringType()), {})
    else:
        raise ValueError("Type not supported")

def supported_types():
    """ Helper function just to return the supported type by the inferrer """
    return ['json','csv', 'text', 'hjson']

    
