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
    workingMount = luigi.Parameter()
    stateMount = luigi.Parameter()
    staticMount = luigi.Parameter()
    testProcessing = luigi.BoolParameter(default = False)

    def run(self):
        prepareArdProcessing = {}
        with self.input().open('r') as prepareArdProcessingInfo:
            prepareArdProcessing = json.load(prepareArdProcessingInfo)

        expectedProducts = prepareArdProcessing["expectedProducts"]
        arcsiMpiRunScriptPath = os.path.join(self.workingMount, "run_arcsimpi.sh")

        a = "mpirun --mca plm_base_verbose 100 {}/singularity exec --bind {}:/working --bind {}:/static {} --bind /apps/eb/software:/apps/eb/software" \
            .format(
                self.singularityDir,
                self.workingMount,
                self.staticMount,
                self.singularityImgPath
            )
        b = prepareArdProcessing["arcsiCmd"]

        cmd = "{} {}".format(a, b)

        # lines = ["module load eb/OpenMPI/gcc/4.0.0\n"]
        # lines.append(cmd)

        with open(arcsiMpiRunScriptPath, 'w') as arcsiMpiRunFile:
            arcsiMpiRunFile.write(cmd)

        st = os.stat(arcsiMpiRunScriptPath)
        os.chmod(arcsiMpiRunScriptPath, st.st_mode | 0o110 )

        log.info("Created run_arcsimpi.sh with command " + cmd)

        # if not self.testProcessing:
        #     try:
        #         log.info("Running " + arcsiMpiRunScriptPath)
        #         subprocess.run(arcsiMpiRunScriptPath, check=True, stderr=subprocess.STDOUT, shell=True)
        #     except subprocess.CalledProcessError as e:
        #         errStr = "command '{}' return with error (code {}): {}".format(e.cmd, e.returncode, e.output)
        #         log.error(errStr)
        #         raise RuntimeError(errStr)
        # else:
        if self.testProcessing:
            log.info("Generating mock output files")
            for expectedProduct in expectedProducts["products"]:
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
