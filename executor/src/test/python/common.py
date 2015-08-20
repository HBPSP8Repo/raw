# Generate test classes from .q files. Each .q file contains one or more query/test blocks separated by a line with
# the string "--". Each XXX.q file will be transformed into one XXXTest.scala file.
# The generated files are placed in the subdirectory "generated".
import os.path, shutil, re, sys
import xml.etree.ElementTree as ET
import requests
import json
import subprocess
import time

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

templateTestMethodJsonCompare ="""
  test("%(name)s") {
    val oql = \"\"\"
      %(query)s
    \"\"\"
    val result = queryCompiler.compileOQL(oql, accessPaths).computeResult
    assertJsonEqual(\"%(name)s\", result)
  }
"""

class TestGenerator:
    def __init__(self, template):
        self.template = template

    def execQueryGCC(self, query):
        gccexecurl = "http://192.168.59.104:5001/query"
        payload = {"oql": query}
        i = 0
        while i < 3:
            print "Sending post to", gccexecurl, ", form:",payload
            r = requests.post(gccexecurl, data=payload)
            # The order of dictionary keys is not preserved
            jsonResponse = r.json()
            if jsonResponse["result"]["success"]:
                print r.text
                output = jsonResponse["result"]["output"]
                # print output
                pretty = json.dumps(output, indent=2, separators=(',', ': '))
                # print pretty
                return pretty
            print "Request failed:\n",r.text
            i+=1
        print "All attempts have failed."

    def indent(self, lines, level):
        return [(" "*level)+line for line in lines]

    def processQuery(self, testName, testDef, testResourcesPath):
        disabledAttr = testDef.get('disabled')
        if disabledAttr != None:
            print "Test disabled:", testName, ". Reason:", disabledAttr
            return ""
        qe = testDef.find("oql")
        oql = qe.text.strip()

        # Compute GCC executor result and save it to a file as JSON
        jsonResult = self.execQueryGCC(oql)
        resultFile = os.path.join(testResourcesPath, testName+".json")
        print "Saving result to file", resultFile
        outFile = open(resultFile, "w")
        outFile.write(jsonResult)
        outFile.close()

        # Generate test method
        resultElem = testDef.find("result")
        # if resultElem == None:
        testMethod = templateTestMethodJsonCompare % {"name":testName, "query": oql}
        # else:
        #     expectedResults = resultElem.text.strip()
        #     testMethod = templateTestMethod % {"name":testName, "query": oql, "expectedResults": expectedResults}
        return testMethod

    def startDocker(self, dataset):
        cmd = "docker run -d -p 5001:5000 raw/ldb //raw/scripts/run-with-gcc-backend.sh " + dataset
        print "Starting docker:", cmd
        cid = subprocess.check_output(cmd, shell=True).strip()
        print "CID:",cid
        return cid

    def waitForLDBContainer(self, cid):
        i=0
        while i<20:
            logs = subprocess.check_output("docker logs " + cid, shell=True, stderr=subprocess.STDOUT)
            if "Running on http://" in logs:
                print "LDB server started. Container logs:\n" + logs
                return
            if "Exceptions.LDBException: internal error" in logs:
                subprocess.check_output("docker logs " + cid, shell=True)
                raise Exception("Web server failed to start")
            i+=1
            print "Waiting for LDB web server to start:", i
            time.sleep(1)
        subprocess.check_output("docker logs " + cid, shell=True)
        raise Exception("Giving up waiting for python server")

    def stopDocker(self, cid):
        cmd = "docker stop -t 0 " + cid + " && docker rm " + cid
        print cmd
        try:
            status = subprocess.call(cmd, shell=True)
            print "Status:", status
        except:
            pass

    def processFile(self, root, filename):
        package = re.split('src/test/scala/', root)[1]
        package = package.replace("/",".")

        # Where to save the gcc executor results in json format
        i = root.rfind("/test/scala/")
        testPath = root[0:(i+5)]
        gccExecResultsPath = os.path.join(testPath, "resources", "generated")
        print gccExecResultsPath
        try:
            os.mkdir(gccExecResultsPath)
        except OSError:
            pass #Directory already exists

        # Generated test source files
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

        cid = self.startDocker(dataset)
        try:
            self.waitForLDBContainer(cid)
            disabledAttr =  root.get('disabled')
            if disabledAttr != None:
                print "Skipping file, tests disabled. Reason:", disabledAttr
                return

            name = os.path.splitext(filename)[0]
            name = name[0].upper() + name[1:]
            testMethods = ""
            i = 0
            for child in root:
                testName = name+str(i)
                testMethod = self.processQuery(testName, child, gccExecResultsPath)
                testMethods += testMethod
                i+=1
            code = self.template % {"name": name, "package": package, "testMethods": testMethods, "dataset": dataset}
            scalaFilename = os.path.join(generatedDirectory, name+"Test.scala")
            print "Writing", i, "tests in file", scalaFilename
            outFile = open(scalaFilename, "w")
            outFile.write(code)
            outFile.close()
        finally:
            self.stopDocker(cid)

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
                    if file.startswith("join"):
                        self.processFile(root, file)