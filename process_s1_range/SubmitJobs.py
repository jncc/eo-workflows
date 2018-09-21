import luigi
import logging
import subprocess
import json
import os
import stat
import glob
import workflow_common.common as wc
import re
from luigi.util import requires
from process_s1_range.PrepareBasket import PrepareBasket
from workflow_common.RunJob import RunJob
from os.path import join

log = logging.getLogger('luigi-interface')

@requires(PrepareBasket)
class SubmitJobs(luigi.Task):
    pathRoots = luigi.DictParameter()
    reprojectionFilePattern = luigi.Parameter()
    maxScenes = luigi.IntParameter()
    startDate = luigi.DateParameter()
    endDate = luigi.DateParameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        with self.input().open('r') as stateFile:
            contents = json.load(stateFile)

        inputDir = contents["basketPath"]

        tasks = []
        for inputFile in glob.glob(os.path.join(inputDir, "*.zip")):
            task = RunJob(
                inputFile = inputFile,
                reprojectionFilePattern = self.reprojectionFilePattern,
                pathRoots = self.pathRoots,
                removeSourceFile = True,
                testProcessing = self.testProcessing
            )

            tasks.append(task)

        yield tasks

        outputFile = {
            "queryWindow": {
                "start": str(self.startDate),
                "end": str(self.endDate)
            },
            "maxScenes": self.maxScenes,
            "products": []
        }

        for task in tasks:
            with task.output().open('r') as taskOutput:
                submittedProduct = json.load(taskOutput)
                outputFile["products"].append(submittedProduct)

        with self.output().open("w") as outFile:
            outFile.write(wc.getFormattedJson(outputFile))

    def output(self):
        outputFolder = self.pathRoots["stateDir"]
        return wc.getLocalStateTarget(outputFolder, "SubmittedJobs.json")