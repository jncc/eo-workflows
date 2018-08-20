import luigi
import logging
import json
import os
import workflow_common.common as wc
from os.path import join

log = logging.getLogger('luigi-interface')

class SetupDirectories(luigi.Task):
    pathRoots = luigi.DictParameter()
    runDate = luigi.DateParameter()

    def run(self):
        statesDir = os.path.join(self.pathRoots["processingRootDir"], os.path.join(str(self.runDate), "states"))
        processingDir = os.path.join(self.pathRoots["processingRootDir"], os.path.join(str(self.runDate), "processing"))

        if not os.path.exists(statesDir):
            os.makedirs(statesDir)
        
        if not os.path.exists(processingDir):
            os.makedirs(processingDir)

        output = {
            "statesDir": statesDir,
            "processingDir": processingDir
        }

        with self.output().open('w') as out:
            out.write(json.dumps(output, indent=4))

    def output(self):
        outputFolder = os.path.join(self.pathRoots["processingRootDir"], os.path.join(str(self.runDate), "states"))
        return wc.getLocalStateTarget(outputFolder, "CreateStateFolder.json")