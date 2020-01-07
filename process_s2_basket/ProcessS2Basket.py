import luigi
import logging
import os
import workflow_common.common as wc
import json
from workflow_common.RunJob import RunJob
from process_s2_basket.GetInputSwaths import GetInputSwaths
from process_s2_basket.SetupWorkDirs import SetupWorkDirs
from luigi.util import requires
from os.path import join

log = logging.getLogger('luigi-interface')

@requires(GetInputSwaths, SetupWorkDirs)
class ProcessS2Basket(luigi.Task):
    paths = luigi.DictParameter()
    maxMemory = luigi.IntParameter()
    mpi = luigi.BoolParameter(default = False)
    hoursPerGranule = luigi.IntParameter()
    shortSerialQueueName = luigi.Parameter()
    shortSerialMaxHours = luigi.IntParameter()
    longSerialQueueName = luigi.Parameter()
    longSerialMaxHours = luigi.IntParameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        getInputSwaths = {}
        with self.input()[0].open('r') as getInputSwathsInfo:
            getInputSwaths = json.load(getInputSwathsInfo)

        setupWorkDirs = {}
        with self.input()[1].open('r') as setupWorkDirsInfo:
            setupWorkDirs = json.load(setupWorkDirsInfo)

        basketDir = self.paths["basketDir"]

        tasks = []
        for swathSetup in setupWorkDirs["swathSetups"]:
            queueName = self.shortSerialQueueName
            maxHours = 2

            if not self.mpi:
                for swath in getInputSwaths["swaths"]:
                    if swath["swathDir"] == swathSetup["swathDir"]:
                        noOfGranules = len(swath["productPaths"])
                        maxHours = noOfGranules * self.hoursPerGranule
                        if maxHours > self.shortSerialMaxHours and maxHours <= self.longSerialMaxHours:
                            queueName = self.longSerialQueueName
                        elif maxHours > self.longSerialMaxHours:
                            raise RuntimeError("Swath of size {} granules has max runtime of {} hours which exceeds long-serial limit of {} hours"\
                                .format(noOfGranules, maxHours, self.longSerialMaxHours))
                        break

            task = RunJob(
                paths = self.paths,
                inputPath = swathSetup["swathDir"],
                workspaceRoot = swathSetup["workspaceRoot"],
                runScriptPath = swathSetup["runScriptPath"],
                queueName = queueName,
                maxMemory = self.maxMemory,
                maxTime = str(maxHours) + ":00", # terrible but quick way to get it into the HH:MM format
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
        return wc.getLocalStateTarget(outputFolder, "ProcessS2Basket.json")