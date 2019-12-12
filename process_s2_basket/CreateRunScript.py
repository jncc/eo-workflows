import luigi
import logging
import os
import glob
import workflow_common.common as wc
import json
import pathlib
from os.path import join
from pathlib import Path

log = logging.getLogger('luigi-interface')

class CreateRunScript(luigi.Task):
    paths = luigi.DictParameter()
    swathDir = luigi.Parameter()
    workingFileRoot = luigi.Parameter()
    stateFileRoot = luigi.Parameter()
    runScriptPath = luigi.Parameter()
    demFilename = luigi.Parameter()
    arcsiReprojection = luigi.BoolParameter()
    outWktFilename = luigi.Parameter()
    projAbbv = luigi.Parameter()
    metadataConfigFile = luigi.Parameter()
    metadataTemplate = luigi.Parameter()
    maxCogProcesses = luigi.IntParameter()

    def run(self):
        staticDir = self.paths["staticDir"]
        outputDir = self.paths["outputDir"]
        singularityDir = self.paths["singularityDir"]
        singularityImgPath = self.paths["singularityImgPath"]

        singularityCmd = ""

        if self.arcsiReprojection:
            singularityCmd = "{}/singularity exec --bind {}:/working --bind {}:/state --bind {}:/input --bind {}:/static --bind {}:/output {} /app/exec.sh "\
                "FinaliseOutputs --dem=dem/{} --outWkt={} --projAbbv={} --metadataConfigFile={} --metadataTemplate={} --maxCogProcesses={} --local-scheduler" \
                .format(singularityDir,
                    self.workingFileRoot,
                    self.stateFileRoot,
                    self.swathDir,
                    staticDir,
                    outputDir,
                    singularityImgPath,
                    self.demFilename,
                    self.outWktFilename,
                    self.projAbbv,
                    self.metadataConfigFile,
                    self.metadataTemplate,
                    self.maxCogProcesses)
        else:
            singularityCmd = "{}/singularity exec --bind {}:/working --bind {}:/state --bind {}:/input --bind {}:/static --bind {}:/output {} /app/exec.sh "\
                "FinaliseOutputs --dem=dem/{} --metadataConfigFile={} --metadataTemplate={} --maxCogProcesses={} --local-scheduler" \
                .format(singularityDir,
                    self.workingFileRoot,
                    self.stateFileRoot,
                    self.swathDir,
                    staticDir,
                    outputDir,
                    singularityImgPath,
                    self.demFilename,
                    self.metadataConfigFile,
                    self.metadataTemplate,
                    self.maxCogProcesses)
        
        with open(self.runScriptPath, 'w') as runScript:
            runScript.write(singularityCmd)

        st = os.stat(self.runScriptPath)
        os.chmod(self.runScriptPath, st.st_mode | 0o110 )

        log.info("Created run_singularity_workflow.sh for " + self.swathDir + " with command " + singularityCmd)

        outputFile = {
            "runScript": self.runScriptPath
        }

        with self.output().open("w") as outFile:
            outFile.write(wc.getFormattedJson(outputFile))

    def output(self):
        outputFolder = self.paths["stateDir"]
        stateFilename = "CreateRunScript_"+os.path.basename(self.swathDir)+".json"
        return wc.getLocalStateTarget(outputFolder, stateFilename)