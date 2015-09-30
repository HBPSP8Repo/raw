import os

def createDirIfNotExists(directory):
    try:
        os.makedirs(directory)
    except OSError as ex:
        pass #Directory already exists