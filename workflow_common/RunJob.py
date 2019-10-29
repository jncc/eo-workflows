import luigi
import logging
import subprocess
import json
import os
import stat
import random
import re
import datetime
import pathlib
import workflow_common.common as wc
from os.path import join
from pathlib import Path

log = logging.getLogger('luigi-interface')

class RunJob(luigi.Task):
    inputPath = luigi.Parameter()
    demFilename = luigi.Parameter()
    paths = luigi.DictParameter()
    spatialConfig = luigi.DictParameter()
    removeSourceFile = luigi.BoolParameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        log.info("Setting up directories for {}".format(self.inputPath))

        productName = wc.getProductNameFromPath(self.inputPath)
        workspaceRoot = os.path.join(self.paths["processingDir"], productName)
        
        workingFileRoot = os.path.join(workspaceRoot, "working")
        if not os.path.exists(workingFileRoot):
            os.makedirs(workingFileRoot)

        stateFileRoot = os.path.join(workspaceRoot, "state")
        if not os.path.exists(stateFileRoot):
            os.makedirs(stateFileRoot)

        singularityScriptPath = os.path.join(workspaceRoot, "run_singularity_workflow.sh")
        if not os.path.isfile(singularityScriptPath):
            self.createSingularityScript(self.inputPath, workingFileRoot, stateFileRoot, singularityScriptPath)

        outputFile = {
            "productId": productName,
            "jobId": None,
            "submitTime": None,
        }

        lotusCmd = "bsub -q short-serial -R 'rusage[mem=18000]' -M 18000 -W 12:00 -o {}/%J.out -e {}/%J.err {}" \
            .format(
                workspaceRoot,
                workspaceRoot,
                singularityScriptPath
            )

        try:
            outputString = ""
            if self.testProcessing:
                randomJobId = random.randint(1000000,9999999)
                outputString = "JOBID     USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME"\
                                +str(randomJobId)+"   test001  RUN   short-serial jasmin-sci1 16*host290. my-job1 Nov 16 16:51"
            else:
                output = subprocess.check_output(
                    lotusCmd,
                    stderr=subprocess.STDOUT,
                    shell=True)
                outputString = output.decode("utf-8")

            regex = '[0-9]{5,}' # job ID is at least 5 digits
            match = re.search(regex, outputString)
            jobId = match.group(0)

            log.info("Successfully submitted lotus job <%s> for %s using command: %s", jobId, productName, lotusCmd)

            outputFile["jobId"] = jobId
            outputFile["submitTime"] = str(datetime.datetime.now())
        except subprocess.CalledProcessError as e:
            errStr = "command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output)
            log.error(errStr)
            raise RuntimeError(errStr)

        with self.output().open('w') as out:
            out.write(wc.getFormattedJson(outputFile))
        
    def output(self):
        outputFolder = self.paths["stateDir"]
        stateFilename = "RunJob_"+wc.getProductNameFromPath(self.inputPath)+".json"
        return wc.getLocalStateTarget(outputFolder, stateFilename)

    def createSingularityScript(self, inputPath, workingFileRoot, stateFileRoot, singularityScriptPath):
        staticDir = self.paths["staticDir"]
        outputDir = self.paths["outputDir"]
        singularityDir = self.paths["singularityDir"]
        singularityImgPath = self.paths["singularityImgPath"]

        path = Path(inputPath)
        inputDir = path.parent
        productName = wc.getProductNameFromPath(inputPath)
        removeSourceFileFlag = "--removeInputFile" if self.removeSourceFile else ""

        singularityCmd = "{}/singularity exec --bind {}:/working --bind {}:/state --bind {}:/input --bind {}:/static --bind {}:/output "\
            "{} /app/exec.sh VerifyWorkflowOutput --productName={} --demFileName={} --memoryLimit=16 {} --noStateCopy " \
            "--snapConfigUtmProj='{}' --snapConfigCentralMeridian={} --snapConfigFalseNorthing={} --snapRunArguments='{}' --sourceSrs='{}' --targetSrs='{}' --finalSrsName={} --metadataProjection='{}'" \
            .format(singularityDir,
                workingFileRoot,
                stateFileRoot,
                inputDir,
                staticDir,
                outputDir,
                singularityImgPath,
                productName,
                self.demFilename,
                removeSourceFileFlag,
                self.spatialConfig["snapConfigUtmProj"],
                self.spatialConfig["snapConfigCentralMeridian"],
                self.spatialConfig["snapConfigFalseNorthing"],
                self.spatialConfig["snapRunArguments"],
                self.spatialConfig["sourceSrs"],
                self.spatialConfig["targetSrs"],
                self.spatialConfig["finalSrsName"],
                self.spatialConfig["metadataProjection"])
        
        with open(singularityScriptPath, 'w') as singularityScript:
            singularityScript.write(singularityCmd)

        st = os.stat(singularityScriptPath)
        os.chmod(singularityScriptPath, st.st_mode | 0o110 )

        log.info("Created run_singularity_workflow.sh for " + inputPath + " with command " + singularityCmd)