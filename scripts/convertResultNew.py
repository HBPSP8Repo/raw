import sys
from collections import OrderedDict

def listAsString(l):
    strList = [valueToString(x) for x in l]
    lsorted = sorted(strList)
    return "[" + ", ".join(lsorted) + "]"

def valueToString(value):
    if isinstance(value, OrderedDict):
        asList = ["{}: {}".format(key, valueToString(value[key])) for key in sorted(value.keys()) ]
        return listAsString(asList)
    elif isinstance(value, list):
        return listAsString(value)
    elif isinstance(value, set):
        return listAsString(list(value))
    else:
        return str(value)


def generateExpectedOutput(resultList):
    print "**************************************************"
    mapped = [valueToString(x) for x in resultList]
    mapped.sort()
    print "\n".join(mapped)

def processLine(line, length):
    return line + " "*(length-len(line))

lines = sys.stdin.readlines()
lines = map(lambda line: line.rstrip(), lines)
maxLength = max(map(lambda line: len(line), lines))
lines = map(lambda line: processLine(line, maxLength), lines)
code = "".join(lines).strip()
dict = eval(code)

result =dict["output"]
generateExpectedOutput(result)


