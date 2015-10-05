from raw_types import *
from collections import OrderedDict
import xml.dom.minidom
import logging

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
        raise ValueException("Unknown type: " + rawType)

def serialize(rawType):
    rawXml = recurse(rawType)
    xmlDom = xml.dom.minidom.parseString(rawXml)
    return xmlDom.toprettyxml()


if __name__ == '__main__':
    name = "foobarType"
    schema = rawListType(rawRecordType(name, OrderedDict([('field1', rawIntType()), ('field2', rawStringType())])))
    print serialize(schema)

