import luigi
import logging
import json
import os
import datetime
import workflow_common.common as wc
from luigi.util import requires
from os.path import join
from process_s1_range.GetS1ScenesByDateAndPolygon import GetS1ScenesByDateAndPolygon

log = logging.getLogger('luigi-interface')

@requires(GetS1ScenesByDateAndPolygon)
class PrepareBasket(luigi.Task):
    pathRoots = luigi.DictParameter()
    startDate = luigi.DateParameter()
    endDate = luigi.DateParameter()

    def run(self):
        basketPath = self.createBasket()

        with self.input().open('r') as inputFile:
            data = json.load(inputFile)

            for filePath in data["rawList"]:
                log.info("Adding " + filePath + " to the basket")
                os.symlink(filePath, os.path.join(basketPath, os.path.basename(filePath)))

        output = {
            'basketPath': basketPath
        }

        with self.output().open('w') as out:
            out.write(wc.getFormattedJson(output))

    def createBasket(self):
        timestamp = str(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
        basketName = str(self.startDate).replace('-','') + "-" + str(self.endDate).replace('-','') + "_" + timestamp
        basketPath = os.path.join(self.pathRoots["basketRoot"], basketName)

        if not os.path.exists(basketPath):
            os.makedirs(basketPath)
            log.info("Created new basket: " + basketPath)

        return basketPath

    def output(self):
        outputFolder = self.pathRoots["statesDir"]
        return wc.getLocalStateTarget(outputFolder, "prepareBasket.json")