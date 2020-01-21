import luigi
import os
import json
import subprocess
import logging
from luigi import LocalTarget
from luigi.util import requires
from process_s2_basket.PrepareArdProcessing import PrepareArdProcessing

log = logging.getLogger("luigi-interface")

@requires(PrepareArdProcessing)
class ProcessRawToArd(luigi.Task):
    singularityDir = luigi.Parameter()
    singularityImgPath = luigi.Parameter()
    arcsiReprojection = luigi.BoolParameter(default = False)
    workingMount = luigi.Parameter()
    stateMount = luigi.Parameter()
    inputMount = luigi.Parameter()
    staticMount = luigi.Parameter()
    platformMpiMount = luigi.Parameter()
    projAbbv = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        prepareArdProcessing = {}
        with self.input().open('r') as prepareArdProcessingInfo:
            prepareArdProcessing = json.load(prepareArdProcessingInfo)

        expectedProducts = prepareArdProcessing["expectedProducts"]
        
        a = "mpirun.lotus {}/singularity exec --bind {}:/working --bind {}:/state --bind {}:/input --bind {}:/static --bind {}:/opt/platform_mpi {}" \
            .format(
                self.singularityDir,
                self.workingMount,
                self.stateMount,
                self.inputMount,
                self.staticMount,
                self.platformMpiMount,
                self.singularityImgPath
            )
        b = "arcsimpi.py -s sen2 --stats -f KEA --fullimgouts -p RAD SHARP SATURATE CLOUDS TOPOSHADOW STDSREF DOSAOTSGL METADATA"
        c = "-k clouds.kea meta.json sat.kea toposhad.kea valid.kea stdsref.kea --interpresamp near --interp cubic"
        d = "-t /tmp -o {} --dem {} -i {}" \
            .format(
                prepareArdProcessing["tempOutDir"],
                prepareArdProcessing["demFilePath"],
                prepareArdProcessing["fileListPath"]
            )

        cmd = "{} {} {} {}".format(a, b, c, d)

        if self.arcsiReprojection:
            cmd = cmd + " --outwkt {} --projabbv {}".format(prepareArdProcessing["projectionWktPath"], self.projAbbv)

        arcsiMpiRunScriptPath = os.path.join(self.workingMount, "run_arcsimpi.sh")
        with open(arcsiMpiRunScriptPath, 'w') as arcsiMpiRunFile:
            arcsiMpiRunFile.write(cmd)

        st = os.stat(arcsiMpiRunScriptPath)
        os.chmod(arcsiMpiRunScriptPath, st.st_mode | 0o110 )

        log.info("Created run_arcsimpi.sh with command " + cmd)

        if not self.testProcessing:
            try:
                log.info("Running " + arcsiMpiRunScriptPath)
                subprocess.run(arcsiMpiRunScriptPath, check=True, stderr=subprocess.STDOUT, shell=True)
            except subprocess.CalledProcessError as e:
                errStr = "command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output)
                log.error(errStr)
                raise RuntimeError(errStr)
        else:
            #TODO: this needs refactoring to an external command that creats mock files
            log.info("Generating mock output files")
            if not os.path.exists(prepareArdProcessing["tempOutDir"]):
                os.mkdir(prepareArdProcessing["tempOutDir"])

            for expectedProduct in expectedProducts:
                for filePattern in expectedProduct["files"]:
                    testFilename = filePattern.replace("*", "TEST")
                    testFilepath = os.path.join(prepareArdProcessing["tempOutDir"], testFilename)

                    if not os.path.exists(testFilepath):
                        with open(testFilepath, "w") as testFile:
                            testFile.write("TEST")

        expectedProducts["outputDir"] = prepareArdProcessing["tempOutDir"]

        with self.output().open('w') as o:
            json.dump(expectedProducts, o, indent=4)

    def output(self):
        outFile = os.path.join(self.stateMount, 'ProcessRawToArd.json')
        return LocalTarget(outFile)
