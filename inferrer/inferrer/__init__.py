# Infer schema from a local file
from splitstream import splitfile

from csv_inferrer import CSVInferrer, csv_sample
from json_inferrer import JSONInferrer, json_sample


def from_local(name, path, file_type, n_objs = 10):
    if file_type == 'json':
        inferrer = JSONInferrer(name, json_sample(path, n_objs))
    elif file_type == 'csv':
        inferrer = CSVInferrer(name, csv_sample(path, n_objs))
    else:
        raise ValueError("Type not supported")
    return inferrer.infer_type()
    
