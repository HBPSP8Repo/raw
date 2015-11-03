# Generate test classes from .q files. Each .q file contains one or more query/test blocks separated by a line with
# the string "--". Each XXX.q file will be transformed into one XXXTest.scala file.
# The generated files are placed in the subdirectory "generated".
import os.path
import sys
import shutil
import re
import xml.etree.ElementTree as ET
import utils

qrawlClassTemplate = """package %(package)s

import raw._

class %(name)sTest extends Abstract%(testType)sTest {
%(testMethods)s
}
"""

oqlClassTemplate = """package %(package)s

import org.scalatest.BeforeAndAfterAll
import raw._

class %(name)sTest extends Abstract%(testType)sTest with LDBDockerContainer with BeforeAndAfterAll {
%(testMethods)s
}
"""

templateTestMethod = """
  test("%(name)s") {
    val queryLanguage = QueryLanguages(\"%(queryLanguage)s\")
    val query = \"\"\"
      %(query)s
    \"\"\"
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    val actual = convertToString(result)

    val expected = convertExpected(\"\"\"
%(expectedResults)s
    \"\"\")

    assert(actual === expected, s"\\nActual: $actual\\nExpected: $expected")
  }
"""

templateTestMethodJsonCompareToFile = """
  test("%(name)s") {
    val queryLanguage = QueryLanguages(\"%(queryLanguage)s\")
    val query = \"\"\"
      %(query)s
    \"\"\"
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqualsFile(\"%(dataset)s\", \"%(resultfilename)s\", \"%(name)s\", result)
  }
"""

templateTestMethodPrintResult = """
  test("%(name)s") {
    val queryLanguage = QueryLanguages(\"%(queryLanguage)s\")
    val query = \"\"\"
      %(query)s
    \"\"\"
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    writeResult(\"%(dataset)s_%(name)s\", result)
  }
"""

templateTestMethodJsonCompareToString = """
  test("%(name)s") {
    val queryLanguage = QueryLanguages(\"%(queryLanguage)s\")
    val query = \"\"\"
      %(query)s
    \"\"\"
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    val expected = \"\"\"
        %(expectedResults)s
    \"\"\"
    assertJsonEqualsString(\"%(name)s\", expected, result)
  }
"""


class TestGenerator:
    def __init__(self, baseDir):
        self.baseDir = baseDir
        self.queryResultsPath = os.path.join(baseDir, "resources", "queryresults")
        print  "Base dir: "  + self.baseDir +", Query results" + self.queryResultsPath
        try:
            print "Deleting directory: " + self.queryResultsPath
            shutil.rmtree(self.queryResultsPath)
        except OSError:
            # Directory does not exist. Ignore error
            pass

    def indent(self, lines, level):
        return [(" " * level) + line for line in lines]

    # Return a string with the scala code of the tests or None if no tests were generated
    def processQuery(self, dataset, testName, testDef, queryLanguage, expectedResultsPath):
        disabledAttr = testDef.get('disabled')
        if disabledAttr != None:
            print "Test disabled:", testName, ". Reason:", disabledAttr
            return None

        id = ord('A')
        testMethods = ""
        for node in testDef:
            if node.tag.startswith(queryLanguage):
                query = node.text.strip()
                query = query.replace("\"\"\"", "\"\"\" + \"\\\"\\\"\\\"\" + \"\"\"")

                # Generate test method
                resultElem = testDef.find("result")
                testMethodName = testName + "_" + chr(id)

                if resultElem == None:
                    # There is no expected result to compare with, so print the results to the console and to a temp file
                    testMethod = templateTestMethodPrintResult % \
                                 {"dataset": dataset, "name": testMethodName, "queryLanguage": queryLanguage, "query": query}
                else:
                    # Compare with the expected results. Save the result to a JSON file
                    expectedResults = resultElem.text.strip()
                    resultFile = os.path.join(expectedResultsPath, testName+".json")
                    print "Saving result to file", resultFile
                    outFile = open(resultFile, "w")
                    outFile.write(expectedResults.encode("UTF-8"))
                    outFile.close()
                    testMethod = templateTestMethodJsonCompareToFile % \
                                 {"dataset": dataset, "name": testMethodName, "queryLanguage": queryLanguage, "query": query, "resultfilename": testName}
                testMethods += testMethod
                id += 1
        if testMethods == "":
            return None
        else:
            return testMethods

    def writeTestFile(self, directory, name, code):
        utils.createDirIfNotExists(directory)
        scalaFilename = os.path.join(directory, name + "Test.scala")
        print "Writing file", scalaFilename
        outFile = open(scalaFilename, "w")
        outFile.write(code.encode("UTF-8"))
        outFile.close()

    def processFile(self, basedirectory, filename, queryLanguage, classTemplate):
        package = re.split('src/test/scala/', basedirectory)[1]
        package = package.replace("/", ".")

        # Generated test source files
        generatedDirectory = os.path.join(basedirectory, "generated", queryLanguage)
        utils.createDirIfNotExists(generatedDirectory)

        queryFilename = os.path.join(basedirectory, filename)
        print "Found query file", queryFilename

        root = ET.parse(queryFilename).getroot()
        dataset = root.get('dataset')
        if dataset == None:
            raise Exception('dataset attribute is mandatory')

        expectedResultsPath = os.path.join(self.queryResultsPath, dataset)
        utils.createDirIfNotExists(expectedResultsPath)

        disabledAttr = root.get('disabled')
        if disabledAttr != None:
            print "Skipping file, tests disabled. Reason:", disabledAttr
            return

        name = os.path.splitext(filename)[0]
        name = name[0].upper() + name[1:]
        testMethods = ""
        i = 0
        for child in root:
            testName = name + str(i)
            testMethod = self.processQuery(dataset, testName, child, queryLanguage, expectedResultsPath)
            if (testMethod == None):
                continue
            testMethods += testMethod
            i += 1

        #No tests in the current XML file for this query language. Do not generate a scala file
        if i == 0:
            return

        sparkTestsDirectory = os.path.join(generatedDirectory, "spark")
        code = classTemplate % {
            "name": name,
            "package": package + ".generated." + queryLanguage + ".spark",
            "testMethods": testMethods,
            "dataset": dataset,
            "testType": "Spark"}
        self.writeTestFile(sparkTestsDirectory, name, code)

        scalaTestsDirectory = os.path.join(generatedDirectory, "scala")
        code = classTemplate % {
            "name": name,
            "package": package + ".generated." + queryLanguage +".scala",
            "testMethods": testMethods,
            "dataset": dataset,
            "testType": "Scala"}
        self.writeTestFile(scalaTestsDirectory, name, code)

    def processAllFiles(self, matchDir):
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
                    # if file.startswith("join"):
                    while True:
                        try:
                            # There are no OQL tests anymore.
                            # Places scala files in generated.oql.[scala|spark]
                            # self.processFile(root, file, "oql", oqlClassTemplate)
                            # Places scala files in generated.qrawl.[scala|spark]
                            self.processFile(root, file, "qrawl", qrawlClassTemplate)
                            break
                        except RuntimeWarning:
                            pass


if __name__ == '__main__':
    if len(sys.argv) > 1:
        baseDir = os.path.abspath(sys.argv[1])
    else:
        baseDir = os.path.abspath(".")

    generator = TestGenerator(baseDir)
    print "Searching for query files in: " + baseDir
    generator.processAllFiles("scala/raw/patients")
    generator.processAllFiles("scala/raw/publications")
    generator.processAllFiles("scala/raw/httplogs")
