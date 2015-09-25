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
    val queryLanguage = \"%(queryLanguage)s\"
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

templateTestMethodJsonCompare = """
  test("%(name)s") {
    val queryLanguage = \"%(queryLanguage)s\"
    val query = \"\"\"
      %(query)s
    \"\"\"
    val result = queryCompiler.compile(queryLanguage, query, scanners).computeResult
    assertJsonEqual(\"%(dataset)s\", \"%(name)s\", result)
  }
"""


class TestGenerator:
    def indent(self, lines, level):
        return [(" " * level) + line for line in lines]

    def processQuery(self, dataset, testName, testDef, queryLanguage):
        disabledAttr = testDef.get('disabled')
        if disabledAttr != None:
            print "Test disabled:", testName, ". Reason:", disabledAttr
            return ""
        qe = testDef.find(queryLanguage)
        # If there is no element in the XML matching this query language, skip code generation.
        if qe == None:
            return None
        query = qe.text.strip()

        # Generate test method
        # resultElem = testDef.find("result")
        # if resultElem == None:
        testMethod = templateTestMethodJsonCompare % \
                     {"dataset": dataset, "name": testName, "queryLanguage": queryLanguage, "query": query}
        # else:
        #     expectedResults = resultElem.text.strip()
        #     testMethod = templateTestMethod % {"name":testName, "query": oql, "expectedResults": expectedResults}
        return testMethod

    def writeTestFile(self, directory, name, code):
        utils.createDirIfNotExists(directory)
        scalaFilename = os.path.join(directory, name + "Test.scala")
        print "Writing file", scalaFilename
        outFile = open(scalaFilename, "w")
        outFile.write(code)
        outFile.close()

    def processFile(self, root, filename, queryLanguage, classTemplate):
        package = re.split('src/test/scala/', root)[1]
        package = package.replace("/", ".")

        # Generated test source files
        generatedDirectory = os.path.join(root, "generated", queryLanguage)
        utils.createDirIfNotExists(generatedDirectory)

        queryFilename = os.path.join(root, filename)
        print "Found query file", queryFilename

        root = ET.parse(queryFilename).getroot()
        dataset = root.get('dataset')
        if dataset == None:
            raise Exception('dataset attribute is mandatory')

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
            testMethod = self.processQuery(dataset, testName, child, queryLanguage)
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
                    # if file.startswith("join"):
                    while True:
                        try:
                            # Places scala files in generated.oql.[scala|spark]
                            self.processFile(root, file, "oql", oqlClassTemplate)
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

    generator = TestGenerator()
    print "Searching for query files in: " + baseDir
    generator.processAllFiles(baseDir, "scala/raw/patients")
    generator.processAllFiles(baseDir, "scala/raw/publications")