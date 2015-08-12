# Generate test classes from .q files. Each .q file contains one or more query/test blocks separated by a line with
# the string "--". Each XXX.q file will be transformed into one XXXTest.scala file.
# The generated files are placed in the subdirectory "generated".
import os.path, sys
import common

templateClass ="""package %(package)s.generated

import raw.publications.AbstractSparkPublicationsTest
import raw.datasets.publications.Publications

class %(name)sTest extends AbstractSparkPublicationsTest(Publications.publications) {
%(testMethods)s
}
"""

# An optional argument specifies the base directory from where to recursively search for .q files.
# If no argument is given, assumes current directory.
if __name__ == '__main__':
    if len(sys.argv) > 1:
        baseDir = os.path.abspath(sys.argv[1])
    else:
        baseDir = os.path.abspath(".")

    generator = common.TestGenerator(templateClass)
    print "Searching for query files in: " + baseDir
    generator.processAllFiles(baseDir, "scala/raw/publications")
