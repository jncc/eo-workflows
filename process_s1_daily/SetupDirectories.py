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
        stateDir = os.path.join(self.pathRoots["processingRootDir"], os.path.join(str(self.runDate), "state"))
        productsDir = os.path.join(self.pathRoots["processingRootDir"], os.path.join(str(self.runDate), "products"))

        if not os.path.exists(stateDir):
            os.makedirs(stateDir)
        
        if not os.path.exists(productsDir):
            os.makedirs(productsDir)

        output = {
            "stateDir": stateDir,
            "productsDir": productsDir
        }

        with self.output().open('w') as out:
            out.write(wc.getFormattedJson(output))

    def output(self):
        return wc.getLocalDatedStateTarget(self.pathRoots["processingRootDir"], self.runDate, "CreateStateFolder.json")