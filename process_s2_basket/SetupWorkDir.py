import luigi
import logging
import os
import glob
import workflow_common.common as wc
import json
from process_s2_basket.CreateRunScript import CreateRunScript
from os.path import join

log = logging.getLogger('luigi-interface')

class SetupWorkDir(luigi.Task):
    swathDir = luigi.Parameter()
    mpi = luigi.BoolParameter()
    lsfCommandsDir = luigi.Parameter()
    paths = luigi.DictParameter()
    demFilename = luigi.Parameter()
    arcsiReprojection = luigi.BoolParameter()
    outWktFilename = luigi.Parameter()
    projAbbv = luigi.Parameter()
    metadataConfigFile = luigi.Parameter()
    metadataTemplate = luigi.Parameter()
    maxCogProcesses = luigi.IntParameter()

    def run(self):
        log.info("Setting up directories for {}".format(self.swathDir))

        swathDirName = os.path.basename(self.swathDir)
        workspaceRoot = os.path.join(self.paths["processingDir"], swathDirName)
        
        workingFileRoot = os.path.join(workspaceRoot, "working")
        if not os.path.exists(workingFileRoot):
            os.makedirs(workingFileRoot)

        stateFileRoot = os.path.join(workspaceRoot, "state")
        if not os.path.exists(stateFileRoot):
            os.makedirs(stateFileRoot)

        runScriptPath = os.path.join(workspaceRoot, "run_singularity_workflow.sh")
        task = CreateRunScript(
            paths = self.paths,
            mpi = self.mpi,
            lsfCommandsDir = self.lsfCommandsDir,
            swathDir = self.swathDir,
            workingFileRoot = workingFileRoot,
            stateFileRoot = stateFileRoot,
            runScriptPath = runScriptPath,
            demFilename = self.demFilename,
            arcsiReprojection = self.arcsiReprojection,
            outWktFilename = self.outWktFilename,
            projAbbv = self.projAbbv,
            metadataConfigFile = self.metadataConfigFile,
            metadataTemplate = self.metadataTemplate,
            maxCogProcesses = self.maxCogProcesses
        )
        yield task

        outputFile = {
            "swathDir": self.swathDir,
            "workspaceRoot": workspaceRoot,
            "workingFileRoot": workingFileRoot,
            "stateFileRoot": stateFileRoot,
            "runScriptPath": runScriptPath,
            "demFilename": self.demFilename,
            "arcsiReprojection": self.arcsiReprojection,
            "outWktFilename": self.outWktFilename,
            "projAbbv": self.projAbbv,
            "metadataConfigFile": self.metadataConfigFile,
            "metadataTemplate": self.metadataTemplate,
            "maxCogProcesses": self.maxCogProcesses
        }

        with self.output().open("w") as outFile:
            outFile.write(wc.getFormattedJson(outputFile))

    def output(self):
        outputFolder = self.paths["stateDir"]
        stateFilename = "SetupWorkDir_"+os.path.basename(self.swathDir)+".json"
        return wc.getLocalStateTarget(outputFolder, stateFilename)