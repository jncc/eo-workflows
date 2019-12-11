import luigi
import logging
import os
import glob
import workflow_common.common as wc
import json
from os.path import join

log = logging.getLogger('luigi-interface')

class GetInputSwaths(luigi.Task):
    paths = luigi.DictParameter()

    def run(self):
        basketDir = self.paths["basketDir"]
        basketSubDirs = next(os.walk(basketDir))[1]
        swaths = []
        
        for subDir in basketSubDirs:
            subDirProducts = []
            swathDir = os.path.join(basketDir, subDir)
            for product in glob.glob(os.path.join(swathDir, "S2*")):
                subDirProducts.append(product)

            if len(subDirProducts):
                swath = {
                    "swathDir": swathDir,
                    "productPaths": subDirProducts
                }
                swaths.append(swath)

        outputFile = {
            "basket": basketDir,
            "swaths": swaths
        }
        with self.output().open("w") as outFile:
            outFile.write(wc.getFormattedJson(outputFile))

    def output(self):
        outputFolder = self.paths["stateDir"]
        return wc.getLocalStateTarget(outputFolder, "GetInputSwaths.json")