# Generate test classes from .q files. Each .q file contains one or more query/test blocks separated by a line with
# the string "--". Each XXX.q file will be transformed into one XXXTest.scala file.
# The generated files are placed in the subdirectory "generated".
import os.path, shutil, re, sys

template ="""package %(package)s.generated
import org.apache.spark.rdd.RDD
import raw.{rawQueryAnnotation, RawQuery}
import raw.datasets.patients._
import %(package)s._

%(queryClasses)s

class %(name)sTest extends AbstractSparkPatientsTest {
%(testMethods)s
}
"""

templateQueryClass ="""
@rawQueryAnnotation
class %(name)sQuery(val patients: RDD[Patient]) extends RawQuery {
  val oql = \"\"\"
%(query)s
  \"\"\"
}
"""

templateTestMethod ="""
  test("%(name)s") {
    val result = new %(name)sQuery(patientsRDD).computeResult
    val actual = convertToString(result)
%(asserts)s
    assert(actual === expected, s"\\nActual: $actual\\nExpected: $expected")
  }
"""

templateTestMethodNoAssert ="""
  test("%(name)s") {
    val result = new %(name)sQuery(patientsRDD).computeResult
    val actual = convertToString(result)

    println(actual)
  }
"""

def indent(lines, level):
    return [(" "*level)+line for line in lines]

def processSingleTest(testName, testDef):
    lines = testDef.strip().splitlines()
    slines = map(lambda x: x.strip(),  lines)
    try:
        b = slines.index("")
    except ValueError:
        b = len(lines)
    oql = "\n".join(indent(lines[:b], 4))
    asserts = "\n".join(indent(lines[b+1:], 4))
    queryClass = templateQueryClass % {"name":testName, "query": oql}
    if len(asserts.strip()) == 0:
        testMethod = templateTestMethodNoAssert % {"name":testName}
    else:
        testMethod = templateTestMethod % {"name":testName, "asserts": asserts}
    return (queryClass, testMethod)


def processFile(root, filename):
    package = re.split('src/test/scala/', root)[1]
    package = package.replace("/",".")
    generatedDirectory = os.path.join(root, "generated")
    try:
        os.mkdir(generatedDirectory)
    except OSError:
        pass #Directory already exists
    queryFilename = os.path.join(root, filename)
    print "Found query file", queryFilename
    f = open(queryFilename)
    contents = f.read().replace("\r", "")
    f.close()
    queryDefs = re.split("^--$", contents, flags = re.M)
    name = os.path.splitext(filename)[0]
    name = name[0].upper() + name[1:]
    queryClasses = ""
    testMethods = ""
    i = 0
    for queryDef in queryDefs:
        (queryClass, testMethod) = processSingleTest(name+str(i), queryDef)
        queryClasses += queryClass
        testMethods += testMethod
        i+=1
    code = template % {"name": name, "package": package, "queryClasses": queryClasses, "testMethods": testMethods}
    scalaFilename = os.path.join(generatedDirectory, name+"Test.scala")
    print "Writing", i, "tests in file", scalaFilename
    outFile = open(scalaFilename, "w")
    outFile.write(code)
    outFile.close()

# An optional argument specifies the base directory from where to recursively search for .q files.
# If no argument is given, assumes current directory.
if __name__ == '__main__':
    if len(sys.argv) > 1:
        baseDir = os.path.abspath(sys.argv[1])
    else:
        baseDir = os.path.abspath(".")
    print "Searching for query files in: " + baseDir
    for root, dirs, files in os.walk(baseDir):
        if not root.endswith("scala/raw/patients"):
            continue
        first = True
        for file in files:
            if file.endswith(".q"):
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
                processFile(root, file)