class InferrerException(Exception):
    def __init__(self, msg):
        super(InferrerException, self).__init__("[Inferrer] %s" % msg)


class ParseException(InferrerException):
    def __init__(self):
        super(ParseException, self).__init__("Could not parse file")


class TypeInferenceException(InferrerException):
    def __init__(self, msg):
        super(TypeInferenceException, self).__init__("Could not infer type: %s" % msg)
        
    
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
