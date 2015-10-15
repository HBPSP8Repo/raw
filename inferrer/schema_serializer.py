from inferrer.raw_types import *
from collections import OrderedDict
import xml.dom.minidom
import logging


class SerializerException(Exception):
    def __init__(self, msg):
        super(SerializerException, self).__init__("could not serialize: %s" % msg)


def recurse(rawType):
    if isinstance(rawType, rawIntType):
        return "<int/>"
    elif isinstance(rawType, rawStringType):
        return "<string/>"
    elif isinstance(rawType, rawFloatType):
        return "<float/>"
    elif isinstance(rawType, rawBooleanType):
        return "<boolean/>"
    elif isinstance(rawType, rawListType):
        return "<list>" + recurse(rawType.desc) + "</list>"
    elif isinstance(rawType, rawRecordType):
        tmp = "<record name=\"" + rawType.name + "\">"
        for (k, v) in rawType.desc.items():
            tmp += '<field name="%s">' % k
            tmp += recurse(v)
            tmp += '</field>'
        tmp += "</record>"
        return tmp
    else:
        # ValueException is unknown, I switched it to ValueError
        raise SerializerException("Unknown type: %s" % rawType)

def serialize(rawType):
    rawXml = recurse(rawType)
    xmlDom = xml.dom.minidom.parseString(rawXml)
    return xmlDom.toprettyxml()


if __name__ == '__main__':
    name = "foobarType"
    schema = rawListType(rawRecordType(name, OrderedDict([('field1', rawIntType()), ('field2', rawStringType())])))
    print serialize(schema)

