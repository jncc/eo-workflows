import luigi
import logging
import os
import glob
import workflow_common.common as wc
import json
import re
from os.path import join

log = logging.getLogger('luigi-interface')

class SetupWorkDir(luigi.Task):
    inputPath = luigi.Parameter()
    paths = luigi.DictParameter()
    spatialConfig = luigi.DictParameter()
    removeSourceFile = luigi.BoolParameter()

    def run(self):
        log.info("Setting up directories for {}".format(self.inputPath))

        srs = self.spatialConfig["filenameSrs"]
        srsSafe = re.sub("\W", '-', srs)
        productName = wc.getProductNameFromPath(self.inputPath)
        workspaceRoot = os.path.join(self.paths["processingDir"], srsSafe, productName)
        
        workingFileRoot = os.path.join(workspaceRoot, "working")
        if not os.path.exists(workingFileRoot):
            os.makedirs(workingFileRoot)

        stateFileRoot = os.path.join(workspaceRoot, "state")
        if not os.path.exists(stateFileRoot):
            os.makedirs(stateFileRoot)

        outputFile = {
            "inputPath": self.inputPath,
            "workspaceRoot": workspaceRoot,
            "workingFileRoot": workingFileRoot,
            "stateFileRoot": stateFileRoot
        }

        with self.output().open("w") as outFile:
            outFile.write(wc.getFormattedJson(outputFile))

    def output(self):
        outputFolder = self.paths["stateDir"]
        stateFilename = "SetupWorkDir_"+wc.getProductNameFromPath(self.inputPath)+".json"
        return wc.getLocalStateTarget(outputFolder, stateFilename)
