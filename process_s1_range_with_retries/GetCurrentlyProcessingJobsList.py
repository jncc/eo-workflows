import luigi
import logging
import json
import os
import datetime
import workflow_common.common as wc
import subprocess
import re
from os.path import join
from luigi.util import requires
from process_s1_range_with_retries.SetupDirectories import SetupDirectories

log = logging.getLogger('luigi-interface')

@requires(SetupDirectories)
class GetCurrentlyProcessingJobsList(luigi.Task):
    pathRoots = luigi.DictParameter()
    runDate = luigi.DateParameter()
    testProcessing = luigi.BoolParameter(default = False)
    
    def run(self):
        processingProducts = {
            "jobIds": []
        }

        try:
            if self.testProcessing:
                outputString = self.getTestJobList()
            else:
                output = subprocess.check_output(
                    "bjobs",
                    stderr=subprocess.STDOUT,
                    shell=True)
                outputString = output.decode("utf-8")
  
            pattern = re.compile('[0-9]{7,}') # job ID is 7 digits (or more?)
            jobIds = []
            for jobId in re.findall(pattern, outputString):
                jobIds.append(jobId)

            processingProducts["jobIds"] = jobIds
        except subprocess.CalledProcessError as e:
            errStr = "command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output)
            log.error(errStr)
            raise RuntimeError(errStr)

        with self.output().open('w') as out:
            out.write(json.dumps(processingProducts, indent=4))

    def output(self):
        outputFolder = os.path.join(self.pathRoots["processingRootDir"], os.path.join(str(self.runDate), "states"))
        return wc.getLocalStateTarget(outputFolder, "CurrentlyProcessingProductsList.json")

    def getTestJobList(self):
        return "JOBID     USER    STAT  QUEUE      FROM_HOST   EXEC_HOST   JOB_NAME   SUBMIT_TIME\
                5636278   test002 RUN   short-serial      jasmin-sci1 host232.jc. ./start_processing   Jul 20 10:06\
                5542041   test002 RUN   short-serial      jasmin-sci1 host150.jc. ./start_processing   Jul 21 11:46"