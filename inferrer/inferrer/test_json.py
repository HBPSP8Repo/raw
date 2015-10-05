import json_inferrer

from raw_types import *

def test_json():
    i = json_inferrer.JSONInferrer("test_json", """[{"a": 1, "b": [1,2,3]}, {"a": 2, "b": [4,5,6]}]""")
    tipe, properties = i.infer_type()
    assert(isinstance(tipe, rawListType))
    assert(isinstance(tipe.desc, rawRecordType))
