# Infer schema from a local file
from splitstream import splitfile

from .csv_inferrer import CSVInferrer
from .json_inferrer import JSONInferrer


def json_sample(path):
    with open(path, 'r') as f:
        s = f.read(500).lstrip() 
    if s[0] != '[':
        # check if another with block is needed 
        return  open(path, 'r').read()
    with open(path, 'r') as f:
        gen = splitfile(f, format="json", startdepth=1)
        try:
            sample = next(gen)
            return "[\n%s\n]" % (sample)
        except StopIteration: 
            # check if another with block is needed 
           return  open(path, 'r').read()

def csv_sample(path):
    # check if this is the best way of doing it
    N_LINES = 100
    with open(path, 'r') as f:
        lines = f.readlines(N_LINES);
    return "".join(lines)
    
def from_local(name, path, file_type):
    if file_type == 'json':
        inferrer = JSONInferrer(name,  json_sample(path) )
    elif file_type == 'csv':
        inferrer = CSVInferrer(name, csv_sample())
    else:
        raise ValueError("Type not supported")
    return inferrer.infer_type()
