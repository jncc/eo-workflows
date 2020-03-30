import luigi
import logging
import os
import glob
import workflow_common.common as wc
import json
import pathlib

from datetime import datetime
from os.path import join
from pathlib import Path

log = logging.getLogger('luigi-interface')

class CreateRunScript(luigi.Task):
    paths = luigi.DictParameter()
    removeSourceFile = luigi.BoolParameter()
    spatialConfig = luigi.DictParameter()
    inputPath = luigi.Parameter()
    workingFileRoot = luigi.Parameter()
    stateFileRoot = luigi.Parameter()
    runScriptPath = luigi.Parameter()

    def run(self):
        staticDir = self.paths["staticDir"]
        outputDir = self.paths["outputDir"]
        singularityDir = self.paths["singularityDir"]
        singularityImgPath = self.paths["singularityImgPath"]
        reportDir = self.paths["reportDir"]

        path = Path(self.inputPath)
        inputDir = path.parent
        productName = wc.getProductNameFromPath(self.inputPath)
        removeSourceFileFlag = "--removeInputFile" if self.removeSourceFile else ""

        reportFileName = "{}-{}.csv".format(os.path.basename(self.paths["basketDir"]), datetime.now().strftime("%Y%m%d%H%M"))
    
        singularityCmd = "{}/singularity exec --bind {}:/report --bind {}:/working --bind {}:/state --bind {}:/input --bind {}:/static --bind {}:/output "\
            "{} /app/exec.sh VerifyWorkflowOutput --productName={} --memoryLimit=16 --noStateCopy --reportFileName={} --dbFileName=db/s1ArdProcessing.db --spatialConfig='{{" \
            "\"snapConfigUtmProj\": \"{}\", \"snapConfigCentralMeridian\": \"{}\", \"snapConfigFalseNorthing\": \"{}\", \"snapRunArguments\": \"{}\", " \
            "\"sourceSrs\": \"{}\", \"targetSrs\": \"{}\", \"filenameDemData\": \"{}\", \"filenameSrs\": \"{}\", \"demFilename\": \"{}\", \"demTitle\":\"{}\", " \
            "\"metadataProjection\": \"{}\", \"metadataPlaceName\":\"{}\", \"metadataParentPlaceName\":\"{}\"}}' {}" \
            .format(singularityDir,
                reportDir,
                self.workingFileRoot,
                self.stateFileRoot,
                inputDir,
                staticDir,
                outputDir,
                singularityImgPath,
                productName,
                reportFileName,
                self.spatialConfig["snapConfigUtmProj"],
                self.spatialConfig["snapConfigCentralMeridian"],
                self.spatialConfig["snapConfigFalseNorthing"],
                self.spatialConfig["snapRunArguments"],
                self.spatialConfig["sourceSrs"],
                self.spatialConfig["targetSrs"],
                self.spatialConfig["filenameDemData"],
                self.spatialConfig["filenameSrs"],
                self.spatialConfig["demFilename"],
                self.spatialConfig["demTitle"],
                self.spatialConfig["metadataProjection"],
                self.spatialConfig["metadataPlaceName"],
                self.spatialConfig["metadataParentPlaceName"],
                removeSourceFileFlag)
        
        with open(self.runScriptPath, 'w') as runScript:
            runScript.write(singularityCmd)

        st = os.stat(self.runScriptPath)
        os.chmod(self.runScriptPath, st.st_mode | 0o110 )

        log.info("Created run_singularity_workflow.sh for " + self.inputPath + " with command " + singularityCmd)

        outputFile = {
            "runScript": self.runScriptPath
        }

        with self.output().open("w") as outFile:
            outFile.write(wc.getFormattedJson(outputFile))

    def output(self):
        outputFolder = self.paths["stateDir"]
        stateFilename = "CreateRunScript_"+wc.getProductNameFromPath(self.inputPath)+".json"
        return wc.getLocalStateTarget(outputFolder, stateFilename)