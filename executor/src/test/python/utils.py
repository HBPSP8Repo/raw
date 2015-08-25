import os

def createDirIfNotExists(directory):
    try:
        os.mkdir(directory)
    except OSError:
        pass #Directory already exists