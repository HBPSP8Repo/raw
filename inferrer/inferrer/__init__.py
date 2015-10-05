# Infer schema from a local file

from .csv_inferrer import CSVInferrer
from .json_inferrer import JSONInferrer


def from_local(name, path, file_type):
    with open(path, 'r') as f:
        if file_type == 'json':
            inferrer = JSONInferrer(name, f.read())
        elif file_type == 'csv':
            inferrer = CSVInferrer(name, f.read())
        else:
            raise ValueError("Type not supported")
        return inferrer.infer_type()
