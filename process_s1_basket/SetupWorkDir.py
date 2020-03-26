import luigi
import logging
import os
import glob
import workflow_common.common as wc
import json
from process_s1_basket.CreateRunScript import CreateRunScript
from os.path import join

log = logging.getLogger('luigi-interface')

class SetupWorkDir(luigi.Task):
    inputPath = luigi.Parameter()
    paths = luigi.DictParameter()
    spatialConfig = luigi.DictParameter()
    removeSourceFile = luigi.BoolParameter()

    def run(self):
        log.info("Setting up directories for {}".format(self.inputPath))

        locationName = self.spatialConfig["metadataPlaceName"].replace(' ','-')
        productName = wc.getProductNameFromPath(self.inputPath)
        workspaceRoot = os.path.join(self.paths["processingDir"], locationName, productName)
        
        workingFileRoot = os.path.join(workspaceRoot, "working")
        if not os.path.exists(workingFileRoot):
            os.makedirs(workingFileRoot)

        stateFileRoot = os.path.join(workspaceRoot, "state")
        if not os.path.exists(stateFileRoot):
            os.makedirs(stateFileRoot)

        runScriptPath = os.path.join(workspaceRoot, "run_singularity_workflow.sh")
        if not os.path.isfile(runScriptPath):
            task = CreateRunScript(
                paths = self.paths,
                removeSourceFile = self.removeSourceFile,
                spatialConfig = self.spatialConfig,
                inputPath = self.inputPath,
                workingFileRoot = workingFileRoot,
                stateFileRoot = stateFileRoot,
                runScriptPath = runScriptPath
            )
            yield task

        outputFile = {
            "inputPath": self.inputPath,
            "workspaceRoot": workspaceRoot,
            "workingFileRoot": workingFileRoot,
            "stateFileRoot": stateFileRoot,
            "runScriptPath": runScriptPath
        }

        with self.output().open("w") as outFile:
            outFile.write(wc.getFormattedJson(outputFile))

    def output(self):
        outputFolder = self.paths["stateDir"]
        stateFilename = "SetupWorkDir_"+wc.getProductNameFromPath(self.inputPath)+".json"
        return wc.getLocalStateTarget(outputFolder, stateFilename)