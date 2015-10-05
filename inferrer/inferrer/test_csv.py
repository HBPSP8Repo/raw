import csv_inferrer

from raw_types import *

def test_csv():
    i = csv_inferrer.CSVInferrer("test_csv", "a,b\n1,2\n")
    tipe, properties = i.infer_type()
    assert(isinstance(tipe, rawListType))
    assert(isinstance(tipe.desc, rawRecordType))
    assert(tipe.desc.desc.keys() == ['a', 'b'])
    assert(isinstance(tipe.desc.desc['a'], rawIntType))
    assert(isinstance(tipe.desc.desc['b'], rawIntType))
    assert(properties['field_names'] == ['a', 'b'])
    assert(properties['has_header'] == True)
