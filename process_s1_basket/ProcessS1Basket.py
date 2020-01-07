import luigi
import logging
import os
import glob
import workflow_common.common as wc
import json
from workflow_common.RunJob import RunJob
from process_s1_basket.SetupWorkDirs import SetupWorkDirs
from luigi.util import requires
from os.path import join

log = logging.getLogger('luigi-interface')

@requires(SetupWorkDirs)
class ProcessS1Basket(luigi.Task):
    paths = luigi.DictParameter()
    spatialConfig = luigi.DictParameter()
    queueName = luigi.Parameter()
    maxMemory = luigi.Parameter()
    maxTime = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        setupWorkDirs = {}
        with self.input().open('r') as setupWorkDirsInfo:
            setupWorkDirs = json.load(setupWorkDirsInfo)

        basketDir = self.paths["basketDir"]

        tasks = []
        for productSetup in setupWorkDirs["productSetups"]:
            task = RunJob(
                paths = self.paths,
                inputPath = productSetup["inputPath"],
                workspaceRoot = productSetup["workspaceRoot"],
                runScriptPath = productSetup["runScriptPath"],
                queueName = self.queueName,
                maxMemory = self.maxMemory,
                maxTime = self.maxTime,
                testProcessing = self.testProcessing
            )

            tasks.append(task)
        yield tasks

        outputFile = {
            "basket": basketDir,
            "submittedProducts": []
        }

        for task in tasks:
            with task.output().open('r') as taskOutput:
                submittedProduct = json.load(taskOutput)
                outputFile["submittedProducts"].append(submittedProduct)

        with self.output().open("w") as outFile:
            outFile.write(wc.getFormattedJson(outputFile))

    def output(self):
        outputFolder = self.paths["stateDir"]
        return wc.getLocalStateTarget(outputFolder, "ProcessS1Basket.json")