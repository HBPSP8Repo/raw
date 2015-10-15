from .csv_parser import parse_csv
from .json_parser import parse_json

def parse(file_type, content, properties):
    if file_type == 'csv':
        return parse_csv(content, properties)
    elif file_type == 'json':
        return parse_json(content, properties)
    else:
        raise ValueError(file_type)
