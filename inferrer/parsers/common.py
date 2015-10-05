class ParserException(Exception):
    def __init__(self, msg):
        super(ParserException, self).__init__("[Parser] %s" % msg)
