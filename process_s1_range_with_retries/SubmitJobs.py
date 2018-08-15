import luigi
import logging
import subprocess
import json
import os
import stat
import glob
import workflow_common.common as wc
import re
import random
from luigi.util import requires
from process_s1_range_with_retries.GetProductsToProcessList import GetProductsToProcessList
from workflow_common.RunJob import RunJob
from os.path import join

log = logging.getLogger('luigi-interface')

@requires(GetProductsToProcessList)
class SubmitJobs(luigi.Task):
    pathRoots = luigi.DictParameter()
    outputFile = luigi.Parameter()
    maxScenes = luigi.IntParameter()
    startDate = luigi.DateParameter()
    endDate = luigi.DateParameter()
    outputFilePattern = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        with self.input().open('r') as stateFile:
            contents = json.load(stateFile)

        submittedProducts = {
            "queryWindow": {
                "start": str(self.startDate),
                "end": str(self.endDate)
            },
            "maxScenes": self.maxScenes,
            "products": []
        }

        tasks = []
        for product in contents["products"]:
            task = RunJob(
                inputFile = product["filepath"],
                outputFilePattern = self.outputFilePattern,
                pathRoots = self.pathRoots,
                removeSourceFile = True,
                testProcessing = self.testProcessing
            )

            tasks.append(task)

        yield tasks

        for task in tasks:
            with task.output().open('r') as taskOutput:
                submittedProduct = json.load(taskOutput)
                submittedProducts["products"].append(submittedProduct)

        with self.output().open("w") as outFile:
            outFile.write(json.dumps(submittedProducts))

    def output(self):
        outputFolder = self.pathRoots["processingDir"]
        return wc.getLocalStateTarget(outputFolder, "SubmittedJobs.json")