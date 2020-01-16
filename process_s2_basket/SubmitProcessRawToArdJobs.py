import luigi
import logging
import os
import workflow_common.common as wc
import json
import pathlib
from string import Template
from workflow_common.SubmitJob import SubmitJob
from process_s2_basket.GetInputSwaths import GetInputSwaths
from process_s2_basket.SetupWorkDirs import SetupWorkDirs
from process_s2_basket.SubmitPrepareArdProcessingJobs import SubmitPrepareArdProcessingJobs
from luigi.util import requires
from os.path import join

log = logging.getLogger('luigi-interface')

@requires(GetInputSwaths, SetupWorkDirs, SubmitPrepareArdProcessingJobs)
class SubmitProcessRawToArdJobs(luigi.Task):
    paths = luigi.DictParameter()
    arcsiReprojection = luigi.BoolParameter(default = False)
    projAbbv = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        getInputSwaths = {}
        with self.input()[0].open('r') as getInputSwathsInfo:
            getInputSwaths = json.load(getInputSwathsInfo)

        setupWorkDirs = {}
        with self.input()[1].open('r') as setupWorkDirsInfo:
            setupWorkDirs = json.load(setupWorkDirsInfo)

        prepareArdProcessingJobs = {}
        with self.input()[2].open('r') as submitPrepareArdProcessingJobsInfo:
            prepareArdProcessingJobs = json.load(submitPrepareArdProcessingJobsInfo)

        basketDir = self.paths["basketDir"]

        with open(os.path.join(pathlib.Path(__file__).parent, 'templates/s2_mpi_ProcessRawToArd_job_template.bsub'), 'r') as t:
            bsubTemplate = Template(t.read())

        tasks = []
        for swathSetup in setupWorkDirs["swathSetups"]:
            productName = wc.getProductNameFromPath(swathSetup["swathDir"])

            for swath in getInputSwaths["swaths"]:
                if swath["swathDir"] == swathSetup["swathDir"]:
                    noOfGranules = len(swath["productPaths"])
                    break

            for submittedSwath in prepareArdProcessingJobs["submittedSwaths"]:
                if submittedSwath["productId"] == productName:
                    upstreamJobId = submittedSwath["jobId"]

            arcsiReprojection = "--arcsiReprojection --projAbbv {}".format(self.projAbbv) if self.arcsiReprojection else ""
            testProcessing = "--testProcessing" if self.testProcessing else ""

            bsubParams = {
                "upstreamJobId": upstreamJobId,
                "nodes": noOfGranules + 1,
                "jobWorkingDir" : swathSetup["workspaceRoot"],
                "workingMount" : swathSetup["workingFileRoot"],
                "stateMount" : swathSetup["stateFileRoot"],
                "inputMount" : swathSetup["swathDir"],
                "staticMount" : self.paths["staticDir"],
                "outputMount" : self.paths["outputDir"],
                "platformMpiMount" : self.paths["platformMpiDir"],
                "singularityDir": self.paths["singularityDir"],
                "s2ArdContainer": self.paths["singularityImgPath"],
                "arcsiReprojection": arcsiReprojection,
                "testProcessing": testProcessing
            }

            bsub = bsubTemplate.substitute(bsubParams)
            bsubScriptPath = os.path.join(swathSetup["workspaceRoot"], "submit_ProcessRawToArd_job_for_{}.bsub".format(productName))

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
        return wc.getLocalStateTarget(outputFolder, "SubmitProcessRawToArdJobs.json")