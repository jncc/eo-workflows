import luigi
import logging
import os
import glob
import workflow_common.common as wc
import json
from workflow_common.RunJob import RunJob
from os.path import join

log = logging.getLogger('luigi-interface')

class RunSingularityInLotus(luigi.Task):
    pathRoots = luigi.DictParameter()
    outputFilePattern = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        inputDir = self.pathRoots["inputDir"]

        tasks = []
        for inputFile in glob.glob(os.path.join(inputDir, "*.zip")):
            task = RunJob(
                inputFile = inputFile,
                outputFilePattern = self.outputFilePattern,
                pathRoots = self.pathRoots,
                removeSourceFile = True,
                testProcessing = self.testProcessing
            )

            tasks.append(task)

        yield tasks

        outputFile = {
            "submittedProducts": []
        }

        for task in tasks:
            with task.output().open('r') as taskOutput:
                submittedProduct = json.load(taskOutput)
                outputFile["submittedProducts"].append(submittedProduct)

        with self.output().open("w") as outFile:
            outFile.write(wc.getFormattedJson(outputFile))

    def output(self):
        outputFolder = self.pathRoots["statesDir"]
        return wc.getLocalStateTarget(outputFolder, "lotus_submit_success.json")