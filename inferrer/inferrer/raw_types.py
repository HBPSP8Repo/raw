_uniqueId = 0

class rawType(object):

    def __cmp__(self, other):
        if isinstance(other, self.__class__):
            return 0
        else:
            return -1

    def compatible_with(self, other):
        if isinstance(other, self.__class__) or isinstance(self, rawSomeType) \
                or isinstance(other, rawSomeType):
            return True
        if isinstance(self, rawStringType) and not (isinstance(other, rawListType) or isinstance(other, rawRecordType)):
            return True
        if isinstance(other, rawStringType) and not (isinstance(self, rawListType) or isinstance(self, rawRecordType)):
            return True
        return False
        
    def max_of(self, other):
        if isinstance(other, self.__class__):
            return self
        if isinstance(self, rawSomeType):
            return other
        if isinstance(other, rawSomeType):
            return self
        if isinstance(self, rawStringType):
            return self
        if isinstance(other, rawStringType):
            return other
        return rawUnknownType()

class rawSomeType(rawType):

    def __str__(self):
        return "*"

    def compatible_with(self, other):
        return True

class rawUnknownType(rawType):

    def __str__(self):
        return "<???>"

    def compatible_with(self, other):
        return False

class rawIntType(rawType):

    def __str__(self):
        return "int"

    def compatible_with(self, other):
        r = super(rawIntType, self).compatible_with(other)
        if not r and isinstance(other, rawFloatType):
            return True
        return r

    def max_of(self, other):
        r = super(rawIntType, self).max_of(other)
        if r == rawUnknownType():
           if isinstance(other, rawFloatType):
               return rawFloatType()
        return r

class rawFloatType(rawType):

    def __str__(self):
        return "float"

    def compatible_with(self, other):
        r = super(rawFloatType, self).compatible_with(other)
        if not r and isinstance(other, rawIntType):
            return True
        return r

    def max_of(self, other):
        r = super(rawFloatType, self).max_of(other)
        if r == rawUnknownType():
           if isinstance(other, rawIntType):
               return rawFloatType()
        return r

class rawBooleanType(rawType):
    def __str__(self):
        return "bool"

class rawStringType(rawType):
    def __str__(self):
        return "string"

class rawRecordType(rawType):

    def __str__(self):
        return self.name + "(" + ", ".join("%s:%s" % x for x in self.desc.items()) + ")"

    def __init__(self, prefix, desc):
        self._prefix = prefix

        global _uniqueId
        _uniqueId += 1

        self.name = "%s_%d" % (prefix, _uniqueId)
        self.desc = desc

    def compatible_with(self, other):
        if not super(rawRecordType, self).compatible_with(other):
            return False
        # Records are compatible even if they have different keys
        #if set(other.desc.keys()) != set(self.desc.keys()):
        #    return False
        if isinstance(other, rawSomeType):
           return True
        for key in other.desc.keys():
            if key not in self.desc:
                continue
            if not self.desc[key].compatible_with(other.desc[key]):
                return False
        return True

    def __cmp__(self, other):
        if not isinstance(other, self.__class__):
            return 1
        if set(other.desc.keys()) != set(self.desc.keys()):
            return 1
        for key in other.desc.keys():
            if not (self.desc[key] == other.desc[key]):
                return 1
        return 0

    def max_of(self, other):
        if self.compatible_with(other):
            if isinstance(other, rawSomeType):
                return self
            newDict = self.desc.copy()
            for key, tipe in other.desc.items():
                if key not in self.desc:
                    newDict[key] = tipe
                else:
                    newDict[key] = tipe.max_of(self.desc[key])
            
            return rawRecordType(self._prefix, newDict)
        else:
            return rawUnknownType()

class rawListType(rawType):

    def __str__(self):
        return "list(%s)" % self.desc

    def __init__(self, desc=rawUnknownType):
        self.desc = desc

    def max_of(self, other):
        if self.compatible_with(other):
            return rawListType(self.desc.max_of(other.desc))
        else:
            return rawUnknownType()

    def compatible_with(self, other):
        return super(rawListType, self).compatible_with(other) \
               and self.desc.compatible_with(other.desc)

    def __cmp__(self, other):
        if not isinstance(other, self.__class__):
            return 1
        if not (self.desc == other.desc):
            return 1
        return 0
