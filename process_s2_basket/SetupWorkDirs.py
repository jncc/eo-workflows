import luigi
import logging
import os
import glob
import workflow_common.common as wc
import json
from os.path import join
from process_s2_basket.GetInputSwaths import GetInputSwaths
from process_s2_basket.SetupWorkDir import SetupWorkDir
from luigi.util import requires

log = logging.getLogger('luigi-interface')

@requires(GetInputSwaths)
class SetupWorkDirs(luigi.Task):
    paths = luigi.DictParameter()
    demFilename = luigi.Parameter()
    arcsiReprojection = luigi.BoolParameter(default=True)
    outWktFilename = luigi.Parameter(default="BritishNationalGrid.wkt")
    projAbbv = luigi.Parameter(default="osgb")

    def run(self):
        getInputSwaths = {}
        with self.input().open('r') as getInputSwathsInfo:
            getInputSwaths = json.load(getInputSwathsInfo)
        
        tasks = []
        for swath in getInputSwaths["swaths"]:
            task = SetupWorkDir(
                swathDir = swath["swathDir"],
                paths = self.paths,
                demFilename = self.demFilename,
                arcsiReprojection = self.arcsiReprojection,
                outWktFilename = self.outWktFilename,
                projAbbv = self.projAbbv
            )
            tasks.append(task)
        yield tasks

        outputFile = {
            "swathSetups": []
        }

        for task in tasks:
            with task.output().open('r') as taskOutput:
                swathSetup = json.load(taskOutput)
                outputFile["swathSetups"].append(swathSetup)

        with self.output().open("w") as outFile:
            outFile.write(wc.getFormattedJson(outputFile))

    def output(self):
        outputFolder = self.paths["stateDir"]
        return wc.getLocalStateTarget(outputFolder, "SetupWorkDirs.json")