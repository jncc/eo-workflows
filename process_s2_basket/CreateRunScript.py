import luigi
import logging
import os
import workflow_common.common as wc
import json
import pathlib
from os.path import join
from pathlib import Path

log = logging.getLogger('luigi-interface')

class CreateRunScript(luigi.Task):
    paths = luigi.DictParameter()
    mpi = luigi.BoolParameter()
    lsfCommandsDir = luigi.Parameter()
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
        luigiTargetTask = "ProcessRawToArd" if self.isFirstStepOfMpiProcessing() else "FinaliseOutputs"

        singularityCmd = "{}/singularity exec --bind {}:/working --bind {}:/state --bind {}:/input --bind {}:/static --bind {}:/output --bind {}:/apps/lsf {} /app/exec.sh "\
            "{} --dem={} --local-scheduler" \
            .format(singularityDir,
                self.workingFileRoot,
                self.stateFileRoot,
                self.swathDir,
                staticDir,
                outputDir,
                self.lsfCommandsDir,
                singularityImgPath,
                luigiTargetTask,
                self.demFilename)

        if not self.isFirstStepOfMpiProcessing():
            singularityCmd += " --metadataConfigFile={} --metadataTemplate={} --maxCogProcesses={}" \
                .format(
                    self.metadataConfigFile,
                    self.metadataTemplate,
                    self.maxCogProcesses)

        if self.arcsiReprojection:
            singularityCmd += " --outWkt={} --projAbbv={}" \
                .format(
                    self.outWktFilename,
                    self.projAbbv)

        if self.mpi:
            singularityCmd += " --jasminMpi"
            self.createJasminMpiConfigFile()
        
        
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

    def isFirstStepOfMpiProcessing(self):
        return self.mpi and not os.path.isfile(os.path.join(self.stateFileRoot, "ProcessRawToArd.json"))

    def createJasminMpiConfigFile(self):
        mpiConfigPath = os.path.join(self.workingFileRoot, "jasmin-mpi-config.json")
        mpiConfig = {
            "container" : {
                "location" : self.paths["arcsiMpiBaseImgPath"],
                "mounts" : [
                    (self.workingFileRoot,"/working"),
                    (self.stateFileRoot,"/state"),
                    (self.swathDir,"/input"),
                    (self.paths["staticDir"],"/static"),
                    (self.paths["outputDir"],"/output")
                ]
            },
            "jobTemplate" : "s2_mpi_job_template.bsub"
        }

        with open(mpiConfigPath, 'w') as mpiConfigFile:
            mpiConfigFile.write(wc.getFormattedJson(mpiConfig))

    def output(self):
        outputFolder = self.paths["stateDir"]
        stateFilename = "CreateRunScript_"+os.path.basename(self.swathDir)+".json"
        return wc.getLocalStateTarget(outputFolder, stateFilename)