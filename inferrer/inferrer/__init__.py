# Infer schema from a local file
from splitstream import splitfile

from .csv_inferrer import CSVInferrer
from .json_inferrer import JSONInferrer


def json_sample(path, n_objs = 10):
    """ Tries to get n_objs objects from a json file
        Returns a json string with sample"""
    # probes file to see if it is an array of objects or not
    with open(path, 'r') as f:
        s = f.read(500).lstrip() 
    is_array = s[0] == '['

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
                if (n < 1):
                    raise ValueError("Empty json Array")
                break
        # Do we want to convert this file to an array all the time ?
        # should we return the eof?
        if is_array:
           return "[\n%s\n]" % "\n,".join(objs) 
        else:
            return"\n".join(objs) 

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
        raise ValueError("Unknown type: %s" % rawType)

def csv_sample(path, n_lines = 100):
    # check if this is the best way of doing it
    with open(path, 'r') as f:
        lines = f.readlines(n_lines);
    return "".join(lines)
    
def from_local(name, path, file_type, n_objs = 10):
    if file_type == 'json':
        inferrer = JSONInferrer(name, json_sample(path, n_objs))
    elif file_type == 'csv':
        inferrer = CSVInferrer(name, csv_sample(path, n_objs))
    else:
        raise ValueError("Type not supported")
    return inferrer.infer_type()
