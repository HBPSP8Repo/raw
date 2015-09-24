import os.path, sys
import xml.etree.ElementTree as ET
import json, requests
import dockerutils

def execQueryGCC(query):
    gccexecurl = "http://192.168.59.104:5001/query"
    payload = {"oql": query}
    i = 0
    execRetries = 3
    while i < execRetries:
        print "Sending post to", gccexecurl, ", form:",payload
        r = requests.post(gccexecurl, data=payload)
        # The order of dictionary keys is not preserved
        jsonResponse = r.json()
        if jsonResponse["result"]["success"]:
            output = jsonResponse["result"]["output"]
            pretty = json.dumps(output, indent=2, separators=(',', ': '))
            return pretty
        print "Request failed:\n",r.text
        i+=1
    raise RuntimeError("Failed to execute query " + str(execRetries) + " times. Giving up. Query: " + query)


def processQuery(testName, testDef, testResourcesPath):
    disabledAttr = testDef.get('disabled')
    if disabledAttr != None:
        print "Test disabled:", testName, ". Reason:", disabledAttr
        return ""
    qe = testDef.find("oql")
    oql = qe.text.strip()

    # Compute GCC executor result and save it to a file as JSON
    jsonResult = execQueryGCC(oql)
    resultFile = os.path.join(testResourcesPath, testName+".json")
    print "Saving result to file", resultFile
    outFile = open(resultFile, "w")
    outFile.write(jsonResult)
    outFile.close()


def processFile(root, filename):
    queryFilename = os.path.join(root, filename)
    print "Found query file", queryFilename

    xmlroot = ET.parse(queryFilename).getroot()
    dataset =  xmlroot.get('dataset')
    if dataset == None:
        raise Exception('dataset attribute is mandatory')

    disabledAttr =  xmlroot.get('disabled')
    if disabledAttr != None:
        print "Skipping file, tests disabled. Reason:", disabledAttr
        return

    # Where to save the gcc executor results in json format
    i = root.rfind("/test/scala/")
    testPath = root[0:(i+5)]
    gccExecResultsPath = os.path.join(testPath, "resources", "queryresults", dataset)
    print "GCC executor results:", gccExecResultsPath
    try:
        os.makedirs(gccExecResultsPath)
    except OSError:
        pass #Directory already exists

    ldbContainer = dockerutils.Docker()
    ldbContainer.start(dataset)
    try:
        ldbContainer.waitForLDBContainer()
        name = os.path.splitext(filename)[0]
        name = name[0].upper() + name[1:]
        i = 0
        for child in xmlroot:
            testName = name+str(i)
            processQuery(testName, child, gccExecResultsPath)
            i+=1
    finally:
        ldbContainer.stop()

def processAllFiles(baseDir):
    for root, dirs, files in os.walk(baseDir):
        if not "src/test/scala/raw/" in root:
            continue
        first = True
        for file in files:
            if file.endswith(".xml"):
                # Delete the subdirectory with previously generated tests the first time it encounters a directory with
                # query files
                if first:
                    first = False
                    targetDir = os.path.join(root, "generated")
                while True:
                    try:
                        processFile(root, file)
                        break
                    except RuntimeWarning:
                        pass



# An optional argument specifies the base directory from where to recursively search for .q files.
# If no argument is given, assumes current directory.
if __name__ == '__main__':
    if len(sys.argv) > 1:
        baseDir = os.path.abspath(sys.argv[1])
    else:
        baseDir = os.path.abspath(".")

    print "Searching for query files in: " + baseDir
    processAllFiles(baseDir)


