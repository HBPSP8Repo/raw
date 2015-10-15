import csv
import re

from .common import *
from raw_types import *


class CSVInferrer(object):
    def __init__(self, name, content):
        self._name = name
        self._content = content

    def infer_type(self):
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(self._content, delimiters=";,|")
        has_header = sniffer.has_header(self._content)
        reader = csv.reader(self._content.splitlines(), dialect)
        ctypes = {}
        field_names = []
        for row in reader:
            if ctypes == {} and field_names == []:
                if has_header:
                    field_names = row
                    # skip the first row
                    continue
                else:
                    field_names = ["v%s" % (i + 1) for i in range(len(row))]
            for i, value in enumerate(row):
                field = field_names[i]
                found_type = self.__what_is(value)
                try:
                    knownType = ctypes[field]
                except KeyError:
                    knownType = found_type
                if found_type.compatible_with(knownType):
                    ctypes[field] = found_type.max_of(knownType)
                else:
                    raise TypeInferenceException(value)

        inferred_type = rawListType(rawRecordType(self._name, ctypes))
        properties = dict(has_header=has_header,
                          field_names=field_names,
                          delimiter=dialect.delimiter,
                          doublequote=dialect.doublequote,
                          escapechar=dialect.escapechar,
                          lineterminator=dialect.lineterminator,
                          quotechar=dialect.quotechar,
                          quoting=dialect.quoting,
                          skipinitialspace=dialect.skipinitialspace)

        return inferred_type, properties

    __regexps = [
        (re.compile("[0-9]*\\.[0-9]+$"), rawFloatType()),
        (re.compile("[0-9]+$"), rawIntType()),
        (re.compile("(true|false)$"), rawBooleanType()),
    ]

    def __what_is(self, txt):
        for reg, v in self.__regexps:
            if reg.match(txt):
                return v
        return rawStringType()
        

def csv_sample(path, n_lines = 100):
    """Returns n_lines from an csv file"""
    # check if this is the best way of doing it
    with open(path, 'r') as f:
        lines = f.readlines(n_lines);
    return "".join(lines)
