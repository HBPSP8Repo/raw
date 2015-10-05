class InferrerException(Exception):
    def __init__(self, msg):
        super(InferrerException, self).__init__("[Inferrer] %s" % msg)


class ParseException(InferrerException):
    def __init__(self):
        super(ParseException, self).__init__("Could not parse file")


class TypeInferenceException(InferrerException):
    def __init__(self, msg):
        super(TypeInferenceException, self).__init__("Could not infer type: %s" % msg)
