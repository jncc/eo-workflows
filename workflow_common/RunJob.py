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
    paths = luigi.DictParameter()
    inputPath = luigi.Parameter()
    workspaceRoot = luigi.Parameter()
    runScriptPath = luigi.Parameter()
    queueName = luigi.Parameter()
    maxMemory = luigi.Parameter()
    maxTime = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        productName = wc.getProductNameFromPath(self.inputPath)
        bsubScriptPath = os.path.join(self.workspaceRoot, "submit_to_lotus.bsub")

        outputFile = {
            "productId": productName,
            "bsubScriptPath": bsubScriptPath,
            "workspaceRoot": self.workspaceRoot,
            "queue": self.queueName,
            "maxMemory": self.maxMemory,
            "maxTime": self.maxTime,
            "jobId": None,
            "submitTime": None
        }

        bsubScript = """#!/bin/bash
#BSUB -q {}
#BSUB -R 'rusage[mem={}]'
#BSUB -M {}
#BSUB -W {}
#BSUB -o {}/%J.out
#BSUB -e {}/%J.err

{}""" \
            .format(
                self.queueName,
                self.maxMemory,
                self.maxMemory,
                self.maxTime,
                self.workspaceRoot,
                self.workspaceRoot,
                self.runScriptPath
            )

        with open(bsubScriptPath, 'w') as bsubScriptFile:
            bsubScriptFile.write(bsubScript)

        try:
            outputString = ""
            if self.testProcessing:
                randomJobId = random.randint(1000000,9999999)
                outputString = "JOBID     USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME"\
                                +str(randomJobId)+"   test001  RUN   short-serial jasmin-sci1 16*host290. my-job1 Nov 16 16:51"
            else:
                bsubCmd = "bsub < {}".format(bsubScriptPath)
                log.info("Submitting job using command: %s", bsubCmd)
                output = subprocess.check_output(
                    bsubCmd,
                    stderr=subprocess.STDOUT,
                    shell=True)
                outputString = output.decode("utf-8")

            regex = '[0-9]{5,}' # job ID is at least 5 digits
            match = re.search(regex, outputString)
            jobId = match.group(0)

            log.info("Successfully submitted lotus job <%s> for %s using bsub script: %s", jobId, productName, bsubScriptPath)

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