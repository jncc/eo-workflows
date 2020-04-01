import luigi
import logging
import os
import workflow_common.common as wc
import json

from datetime import datetime
from string import Template
from workflow_common.SubmitJob import SubmitJob
from process_s2_basket.GetInputSwaths import GetInputSwaths
from process_s2_basket.SetupWorkDirs import SetupWorkDirs
from luigi.util import requires
from os.path import join

log = logging.getLogger('luigi-interface')

@requires(GetInputSwaths, SetupWorkDirs)
class ProcessS2BasketSerial(luigi.Task):
    paths = luigi.DictParameter()
    hoursPerGranule = luigi.IntParameter()
    demFilename = luigi.Parameter()
    outWktFilename = luigi.Parameter()
    metadataConfigFile = luigi.Parameter()
    metadataTemplate = luigi.OptionalParameter(default=None)
    arcsiCmdTemplate = luigi.OptionalParameter(default=None)
    maxCogProcesses = luigi.IntParameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        getInputSwaths = {}
        with self.input()[0].open('r') as getInputSwathsInfo:
            getInputSwaths = json.load(getInputSwathsInfo)

        setupWorkDirs = {}
        with self.input()[1].open('r') as setupWorkDirsInfo:
            setupWorkDirs = json.load(setupWorkDirsInfo)

        basketDir = self.paths["basketDir"]

        with open(os.path.join(self.paths["templatesDir"], 's2_serial_GenerateReport_job_template.bsub'), 'r') as t:
            bsubTemplate = Template(t.read())

        tasks = []
        for swathSetup in setupWorkDirs["swathSetups"]:
            productName = wc.getProductNameFromPath(swathSetup["swathDir"])

            for swath in getInputSwaths["swaths"]:
                if swath["swathDir"] == swathSetup["swathDir"]:
                    noOfGranules = len(swath["productPaths"])
                    break

            arcsiReprojection = "--outWkt={} --projAbbv={}".format(self.outWktFilename, self.projAbbv) if self.arcsiReprojection else ""

            metadataTemplate = ""
            if self.metadataTemplate is not None:
                metadataTemplate = "--metadataTemplate={}".format(self.metadataTemplate)
            
            arcsiCmdTemplate = ""
            if self.arcsiCmdTemplate is not None:
                arcsiCmdTemplate = "--arcsiCmdTemplate={}".format(self.arcsiCmdTemplate)

            reportFileName = "{}-{}.csv".format(os.path.basename(self.paths["basketDir"]), datetime.now().strftime("%Y%m%d%H%M"))

            bsubParams = {
                "maxRunTime": noOfGranules * self.hoursPerGranule,
                "jobWorkingDir" : swathSetup["workspaceRoot"],
                "workingMount" : swathSetup["workingFileRoot"],
                "stateMount" : swathSetup["stateFileRoot"],
                "inputMount" : swathSetup["swathDir"],
                "staticMount" : self.paths["staticDir"],
                "outputMount" : self.paths["outputDir"],
                "s2ArdContainer": self.paths["singularityImgPath"],
                "dem": self.demFilename,
                "arcsiReprojection" : arcsiReprojection,
                "metadataConfigFile": self.metadataConfigFile,
                "metadataTemplate": metadataTemplate,
                "arcsiCmdTemplate": arcsiCmdTemplate,
                "reportFileName": reportFileName,
                "reportMount": self.paths["reportDir"],
                "databaseMount": self.paths["databaseDir"]
            }

            bsub = bsubTemplate.substitute(bsubParams)
            bsubScriptPath = os.path.join(swathSetup["workspaceRoot"], "submit_GenerateReport_job_for_{}.bsub".format(productName))

            with open(bsubScriptPath, 'w') as bsubScriptFile:
                bsubScriptFile.write(bsub)

            tasks.append(SubmitJob(
                paths = self.paths,
                productName = productName,
                bsubScriptPath = bsubScriptPath,
                testProcessing = self.testProcessing
            ))
        yield tasks

        outputFile = {
            "basket": basketDir,
            "submittedSwaths": []
        }

        for task in tasks:
            with task.output().open('r') as taskOutput:
                submittedSwath = json.load(taskOutput)
                outputFile["submittedSwaths"].append(submittedSwath)

        with self.output().open("w") as outFile:
            outFile.write(wc.getFormattedJson(outputFile))

    def output(self):
        outputFolder = self.paths["stateDir"]
        return wc.getLocalStateTarget(outputFolder, "ProcessS2BasketSerial.json")