import luigi
import os
import json
from os.path import basename, join
from luigi import LocalTarget

def getLocalTarget(key):
    return LocalTarget(key)

def getLocalDatedStateTarget(rootPath, date, fileName):
    statesPath = os.path.join(rootPath, os.path.join(str(date), "states"))
    filePath = os.path.join(statesPath, fileName)

    return LocalTarget(filePath)

def getLocalStateTarget(targetPath, fileName):
    targetKey = join(targetPath, fileName)
    return getLocalTarget(targetKey)

def getProductIdFromLocalSourceFile(sourceFile):
    productFilename = basename(sourceFile)
    return '%s_%s_%s_%s' % (productFilename[0:3], productFilename[17:25], productFilename[26:32], productFilename[42:48])

def getFormattedJson(jsonOutput):
    return json.dumps(jsonOutput, indent=4)