import luigi
import logging
import os
import workflow_common.common as wc
import json

from datetime import datetime
from string import Template
from workflow_common.SubmitJob import SubmitJob
from process_s2_basket.SetupWorkDirs import SetupWorkDirs
from process_s2_basket.SubmitProcessRawToArdJobs import SubmitProcessRawToArdJobs
from luigi.util import requires
from os.path import join

log = logging.getLogger('luigi-interface')

@requires(SetupWorkDirs, SubmitProcessRawToArdJobs)
class ProcessS2BasketMpi(luigi.Task):
    paths = luigi.DictParameter()
    demFilename = luigi.Parameter()
    outWktFilename = luigi.Parameter()
    projAbbv = luigi.Parameter()
    arcsiReprojection = luigi.BoolParameter()
    metadataConfigFile = luigi.Parameter()
    metadataTemplate = luigi.OptionalParameter(default=None)
    maxCogProcesses = luigi.IntParameter()
    testProcessing = luigi.BoolParameter(default = False)
    arcsiCmdTemplate = luigi.OptionalParameter(default=None)
    oldFilenameDateThreshold = luigi.DateParameter()

    def run(self):
        setupWorkDirs = {}
        with self.input()[0].open('r') as setupWorkDirsInfo:
            setupWorkDirs = json.load(setupWorkDirsInfo)

        prepareArdProcessingJobs = {}
        with self.input()[1].open('r') as submitProcessRawToArdJobsInfo:
            prepareArdProcessingJobs = json.load(submitProcessRawToArdJobsInfo)

        basketDir = self.paths["basketDir"]

        with open(os.path.join(self.paths["templatesDir"], 's2_mpi_GenerateReport_job_template.sbatch'), 'r') as t:
            sbatchTemplate = Template(t.read())

        tasks = []
        for swathSetup in setupWorkDirs["swathSetups"]:
            productName = wc.getProductNameFromPath(swathSetup["swathDir"])

            for submittedSwath in prepareArdProcessingJobs["submittedSwaths"]:
                if submittedSwath["productId"] == productName:
                    upstreamJobId = submittedSwath["jobId"]

            arcsiReprojection = "--outWkt={} --projAbbv={}".format(self.outWktFilename, self.projAbbv) if self.arcsiReprojection else ""

            metadataTemplate = ""
            if self.metadataTemplate is not None:
                metadataTemplate = "--metadataTemplate={}".format(self.metadataTemplate)

            arcsiCmdTemplate = ""
            if self.arcsiCmdTemplate is not None:
                arcsiCmdTemplate = "--arcsiCmdTemplate={}".format(self.arcsiCmdTemplate)

            reportFileName = "{}-{}.csv".format(os.path.basename(self.paths["basketDir"]), datetime.now().strftime("%Y%m%d%H%M"))

            sbatchParams = {
                "upstreamJobId": upstreamJobId,
                "jobWorkingDir" : swathSetup["workspaceRoot"],
                "workingMount" : swathSetup["workingFileRoot"],
                "stateMount" : swathSetup["stateFileRoot"],
                "inputMount" : swathSetup["swathDir"],
                "staticMount" : self.paths["staticDir"],
                "outputMount" : self.paths["outputDir"],
                "s2ArdContainer": self.paths["singularityImgPath"],
                "arcsiReprojection": arcsiReprojection,
                "dem": self.demFilename,
                "metadataConfigFile": self.metadataConfigFile,
                "metadataTemplate": metadataTemplate,
                "arcsiCmdTemplate": arcsiCmdTemplate,
                "maxCogProcesses": self.maxCogProcesses,
                "reportFileName": reportFileName,
                "reportMount": self.paths["reportDir"],
                "databaseMount": self.paths["databaseDir"],
                "oldFilenameDateThreshold": self.oldFilenameDateThreshold
            }

            sbatch = sbatchTemplate.substitute(sbatchParams)
            sbatchScriptPath = os.path.join(swathSetup["workspaceRoot"], "submit_GenerateReport_job_for_{}.sbatch".format(productName))

            with open(sbatchScriptPath, 'w') as sbatchScriptFile:
                sbatchScriptFile.write(sbatch)

            tasks.append(SubmitJob(
                paths = self.paths,
                productName = productName,
                sbatchScriptPath = sbatchScriptPath,
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
        return wc.getLocalStateTarget(outputFolder, "ProcessS2BasketMpi.json")