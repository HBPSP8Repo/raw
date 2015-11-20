import random
import json
import tempfile
import uuid

import json_inferrer

from json_inferrer import json_sample
from raw_types import *

def test_json():
    i = json_inferrer.JSONInferrer( """[{"a": 1, "b": [1,2,3]}, {"a": 2, "b": [4,5,6]}]""")
    tipe, properties = i.infer_type()
    assert(isinstance(tipe, rawListType))
    assert(isinstance(tipe.desc, rawRecordType))

counter = 0
def createJsonObj():
    # creates json obj string with weird formatting
    
    r_str = str(uuid.uuid4()) 
    r_int = random.randrange(100000)
    global counter
    s =  """ {
                    "n" : %d,
"r_int" : %d,"r_str" : "%s { \\" ",
        "nested" : {
            "r_float" : %0.2f,
"r_str" : "%s" }}
    """ % (counter , r_int, r_str, r_int/1000.0, r_str)
    counter += 1
    return s , json.loads(s)
    
    
def test_json_sample_simple_array():
    # case array of atomic types
    a1 = [1,2,3,5,6]
    with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as f:
        f.write(json.dumps(a1))
    a2 = json.loads(json_sample(f.name, 2))
    # in this case it will have to get the full array
    assert a1 == a2
    
    a1=[ [1],[2],[3],[5],[6] ]
    # case array of arrays   
    with tempfile.NamedTemporaryFile(delete=False, suffix='.json') as f:
        f.write(json.dumps(a1))        
    a2 = json.loads(json_sample(f.name, 2))
    assert a2 == [[1],[2]]
            
def test_json_sample_array():
    random.seed()
    n_objs= 1000
    # creates a temp file with n_objs
    f = tempfile.NamedTemporaryFile(delete=False, suffix='.json')
    #adds some white space before just to be sure
    f.write('      \n \n              [\n')
    objs =[]
    for n in range(n_objs):
        s , o = createJsonObj()
        objs.append(o)
        f.write(s)
        if n < n_objs -1:
            f.write(',')
    f.write('\n]')
    f.close()
    # compares the sample with the original objs
    print "created temp file %s " % f.name
    n_sample = 100
    s_objs = json.loads(json_sample(f.name, n_sample))
    assert ( len(s_objs) == n_sample )
    for n in range(n_sample):
        for s in objs[n]:
            assert (objs[n][s] == s_objs[n][s])
