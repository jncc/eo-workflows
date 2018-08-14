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
from process_s1_range_with_retries.PrepareBasket import PrepareBasket
from os.path import join

log = logging.getLogger('luigi-interface')

@requires(PrepareBasket)
class SubmitJobs(luigi.Task):
    pathRoots = luigi.DictParameter()
    outputFile = luigi.Parameter()
    maxScenes = luigi.IntParameter()
    startDate = luigi.DateParameter()
    endDate = luigi.DateParameter()

    def createSingularityScript(self, inputFile, processingFileRoot, stateFileRoot, singularityScriptPath):
        demDir = self.pathRoots["demDir"]
        outputDir = self.pathRoots["outputDir"]
        singularityDir = self.pathRoots["singularityDir"]
        singularityImgDir = self.pathRoots["singularityImgDir"]

        realRawDir = os.path.dirname(os.path.realpath(inputFile))
        basketDir = os.path.dirname(inputFile)
        rawFilename = os.path.basename(inputFile)
        productId = wc.getProductIdFromLocalSourceFile(inputFile)

        singularityCmd = "{}/singularity exec --bind {}:/data/sentinel/1 --bind {}:/data/states --bind {}:/data/raw --bind {}:/data/dem --bind {}:/data/processed --bind {}:/data/basket {}/s1-ard-processor.simg /app/exec.sh --productId {} --sourceFile '/data/raw/{}' --outputFile '{}'" \
            .format(singularityDir,
                processingFileRoot,
                stateFileRoot,
                realRawDir,
                demDir,
                outputDir,
                basketDir,
                singularityImgDir,
                productId,
                rawFilename,
                self.outputFile)
        
        with open(singularityScriptPath, 'w') as singularityScript:
            singularityScript.write(singularityCmd)

        st = os.stat(singularityScriptPath)
        os.chmod(singularityScriptPath, st.st_mode | 0o110 )

        log.info("Created run_singularity_workflow.sh for " + inputFile + " with command " + singularityCmd)

    def run(self):
        with self.input().open('r') as inputFile:
            contents = json.load(inputFile)

        inputDir = contents["basketPath"]
        processingDir = self.pathRoots["processingDir"]

        for inputFile in glob.glob(os.path.join(inputDir, "*.zip")):
            log.info("Found " + inputFile + ", setting up directories")

            filename = os.path.basename(os.path.splitext(inputFile)[0])

            workspaceRoot = os.path.join(processingDir, filename)
            
            processingFileRoot = os.path.join(workspaceRoot, "processing")
            if not os.path.exists(processingFileRoot):
                os.makedirs(processingFileRoot)

            stateFileRoot = os.path.join(workspaceRoot, "states")
            if not os.path.exists(stateFileRoot):
                os.makedirs(stateFileRoot)

            singularityScriptPath = os.path.join(workspaceRoot, "run_singularity_workflow.sh")
            if not os.path.isfile(singularityScriptPath):
                self.createSingularityScript(inputFile, processingFileRoot, stateFileRoot, singularityScriptPath)

            lotusCmd = "bsub -q short-serial -R 'rusage[mem=18000]' -M 18000 -W 10:00 -o {}/%J.out -e {}/%J.err {}" \
                .format(
                    workspaceRoot,
                    workspaceRoot,
                    singularityScriptPath
                )

            output = {
                "jobIds": list()
            }

            try:
                output = subprocess.check_output(
                    lotusCmd,
                    stderr=subprocess.STDOUT,
                    shell=True)

                regex = '[0-9]{7,}' # job ID is 7 digits
                match = re.search(regex, output.decode("utf-8"))
                jobId = match.group(0)

                log.info("Successfully submitted lotus job <%s> for %s using command: %s", jobId, inputFile, lotusCmd)

                output["jobIds"][filename] = jobId
            except subprocess.CalledProcessError as e:
                errStr = "command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output)
                log.error(errStr)
                raise RuntimeError(errStr)

            with self.output().open('w') as out:
                out.write(json.dumps(output))

    def output(self):
        outputFolder = self.pathRoots["processingDir"]
        return wc.getLocalStateTarget(outputFolder, "SubmittedJobs.json")