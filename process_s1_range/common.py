import luigi
import os
from os.path import basename, join
from luigi import LocalTarget

def getLocalTarget(key):
    return LocalTarget(key)

def getLocalStateTarget(targetPath, fileName):
    targetKey = join(targetPath, fileName)
    return getLocalTarget(targetKey)