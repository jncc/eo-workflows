import luigi
import os
from os.path import basename, join
from luigi import LocalTarget

def getLocalTarget(key):
    return LocalTarget(key)

def getLocalStateTarget(targetPath, fileName):
    targetKey = join(targetPath, fileName)
    return getLocalTarget(targetKey)

def getProductIdFromLocalSourceFile(sourceFile):
    productFilename = basename(sourceFile)
    return '%s_%s_%s_%s' % (productFilename[0:3], productFilename[17:25], productFilename[26:32], productFilename[42:48])