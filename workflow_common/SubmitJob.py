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

class SubmitJob(luigi.Task):
    paths = luigi.DictParameter()
    productName = luigi.Parameter()
    bsubScriptPath = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default = False)
    jobId = ""

    def run(self):
        try:
            outputFile = {
                "productId": self.productName,
                "bsubScriptPath": self.bsubScriptPath,
                "jobId": None,
                "submitTime": None
            }

            outputString = ""
            if self.testProcessing:
                randomJobId = random.randint(1000000,9999999)
                outputString = "JOBID     USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME"\
                                +str(randomJobId)+"   test001  RUN   short-serial jasmin-sci1 16*host290. my-job1 Nov 16 16:51"
            else:
                bsubCmd = "bsub < {}".format(self.bsubScriptPath)
                log.info("Submitting job using command: %s", bsubCmd)
                output = subprocess.check_output(
                    bsubCmd,
                    stderr=subprocess.STDOUT,
                    shell=True)
                outputString = output.decode("utf-8")

            regex = '[0-9]{5,}' # job ID is at least 5 digits
            match = re.search(regex, outputString)
            self.jobId = match.group(0)

            log.info("Successfully submitted lotus job <%s> for %s using bsub script: %s", self.jobId, self.productName, self.bsubScriptPath)

            outputFile["jobId"] = self.jobId
            outputFile["submitTime"] = str(datetime.datetime.now())

            with self.output().open('w') as out:
                out.write(wc.getFormattedJson(outputFile))

        except subprocess.CalledProcessError as e:
            errStr = "command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output)
            log.error(errStr)
            raise RuntimeError(errStr)
        
    def output(self):
        outputFolder = self.paths["stateDir"]
        stateFilename = "SubmitJob_{}_{}.json".format(self.productName, self.jobId)
        return wc.getLocalStateTarget(outputFolder, stateFilename)