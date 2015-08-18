# Generate test classes from .q files. Each .q file contains one or more query/test blocks separated by a line with
# the string "--". Each XXX.q file will be transformed into one XXXTest.scala file.
# The generated files are placed in the subdirectory "generated".
import os.path, shutil, re, sys
import xml.etree.ElementTree as ET

templateTestMethod ="""
  test("%(name)s") {
    val oql = \"\"\"
      %(query)s
    \"\"\"
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    val expected = convertExpected(\"\"\"
%(expectedResults)s
    \"\"\")

    assert(actual === expected, s"\\nActual: $actual\\nExpected: $expected")
  }
"""

templateTestMethodNoAssert ="""
  test("%(name)s") {
    val oql = \"\"\"
      %(query)s
    \"\"\"
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    val actual = convertToString(result)

    println(actual)
  }
"""

class TestGenerator:
    def __init__(self, template):
        self.template = template

    def indent(self, lines, level):
        return [(" "*level)+line for line in lines]

    def processQuery(self, testName, testDef):
        disabledAttr = testDef.get('disabled')
        if disabledAttr != None:
            print "Test disabled:", testName, ". Reason:", disabledAttr
            return ""
        qe = testDef.find("oql")
        oql = qe.text.strip()
        resultElem = testDef.find("result")
        if resultElem == None:
            testMethod = templateTestMethodNoAssert % {"name":testName, "query": oql}
        else:
            expectedResults = resultElem.text.strip()
            testMethod = templateTestMethod % {"name":testName, "query": oql, "expectedResults": expectedResults}
        return testMethod

    def processFile(self, root, filename):
        package = re.split('src/test/scala/', root)[1]
        package = package.replace("/",".")
        generatedDirectory = os.path.join(root, "generated")
        try:
            os.mkdir(generatedDirectory)
        except OSError:
            pass #Directory already exists
        queryFilename = os.path.join(root, filename)
        print "Found query file", queryFilename

        root = ET.parse(queryFilename).getroot()
        dataset =  root.get('dataset')
        if dataset == None:
            raise Exception('dataset attribute is mandatory')

        disabledAttr =  root.get('disabled')
        if disabledAttr != None:
            print "Skipping file, tests disabled. Reason:", disabledAttr
            return

        name = os.path.splitext(filename)[0]
        name = name[0].upper() + name[1:]
        testMethods = ""
        i = 0
        for child in root:
            testMethod = self.processQuery(name+str(i), child)
            testMethods += testMethod
            i+=1
        code = self.template % {"name": name, "package": package, "testMethods": testMethods, "dataset": dataset}
        scalaFilename = os.path.join(generatedDirectory, name+"Test.scala")
        print "Writing", i, "tests in file", scalaFilename
        outFile = open(scalaFilename, "w")
        outFile.write(code)
        outFile.close()

    def processAllFiles(self, baseDir, matchDir):
        for root, dirs, files in os.walk(baseDir):
            if not root.endswith(matchDir):
                continue
            first = True
            for file in files:
                if file.endswith(".xml"):
                    # Delete the subdirectory with previously generated tests the first time it encounters a directory with
                    # query files
                    if first:
                        first = False
                        targetDir = os.path.join(root, "generated")
                        print "Deleting target directory: ", targetDir
                        try:
                            shutil.rmtree(targetDir)
                        except OSError:
                            # Directory does not exist. Ignore error
                            pass
                    self.processFile(root, file)