import luigi
import logging
import subprocess
import json
import os
import stat
import random
import re
import datetime
import workflow_common.common as wc
from os.path import join

log = logging.getLogger('luigi-interface')

class RunJob(luigi.Task):
    inputFile = luigi.Parameter()
    pathRoots = luigi.DictParameter()
    reprojectionFilePattern = luigi.Parameter()
    removeSourceFile = luigi.BoolParameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        log.info("Setting up directories for {}".format(self.inputFile))

        productName = os.path.basename(os.path.splitext(self.inputFile)[0])
        workspaceRoot = os.path.join(self.pathRoots["processingDir"], productName)
        
        workingFileRoot = os.path.join(workspaceRoot, "working")
        if not os.path.exists(workingFileRoot):
            os.makedirs(workingFileRoot)

        stateFileRoot = os.path.join(workspaceRoot, "state")
        if not os.path.exists(stateFileRoot):
            os.makedirs(stateFileRoot)

        singularityScriptPath = os.path.join(workspaceRoot, "run_singularity_workflow.sh")
        if not os.path.isfile(singularityScriptPath):
            self.createSingularityScript(self.inputFile, workingFileRoot, stateFileRoot, singularityScriptPath)

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

            regex = '[0-9]{7,}' # job ID is 7 digits
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
        outputFolder = self.pathRoots["stateDir"]
        stateFilename = "RunJob_"+os.path.basename(os.path.splitext(self.inputFile)[0])+".json"
        return wc.getLocalStateTarget(outputFolder, stateFilename)

    def createSingularityScript(self, inputFile, workingFileRoot, stateFileRoot, singularityScriptPath):
        staticDir = self.pathRoots["staticDir"]
        outputDir = self.pathRoots["outputDir"]
        singularityDir = self.pathRoots["singularityDir"]
        singularityImgDir = self.pathRoots["singularityImgDir"]

        realRawDir = os.path.dirname(os.path.realpath(inputFile))
        basketDir = os.path.dirname(inputFile)
        rawFilename = os.path.basename(inputFile)
        productId = wc.getProductIdFromLocalSourceFile(inputFile)
        removeSourceFileFlag = "--removeSourceFile" if self.removeSourceFile else ""

        singularityCmd = "{}/singularity exec --bind {}:/working --bind {}:/state --bind {}:/input/raw --bind {}:/static --bind {}:/output --bind {}:/input/basket {}/s1-ard-processor.simg /app/exec.sh --productId {} --sourceFile '/input/raw/{}' --reprojectionFilePattern '{}' {}" \
            .format(singularityDir,
                workingFileRoot,
                stateFileRoot,
                realRawDir,
                staticDir,
                outputDir,
                basketDir,
                singularityImgDir,
                productId,
                rawFilename,
                self.reprojectionFilePattern,
                removeSourceFileFlag)
        
        with open(singularityScriptPath, 'w') as singularityScript:
            singularityScript.write(singularityCmd)

        st = os.stat(singularityScriptPath)
        os.chmod(singularityScriptPath, st.st_mode | 0o110 )

        log.info("Created run_singularity_workflow.sh for " + self.inputFile + " with command " + singularityCmd)