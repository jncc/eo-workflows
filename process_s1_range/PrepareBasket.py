import luigi
import logging
import json
import os
import datetime
import process_s1_basket.common as wc
from luigi.util import requires
from os.path import join
from process_s1_range.GetS1ScenesByDate import GetS1ScenesByDate

log = logging.getLogger('luigi-interface')

@requires(GetS1ScenesByDate)
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
            out.write(json.dumps(output))

    def createBasket(self):
        timestamp = str(datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
        basketName = str(self.startDate) + "-" + str(self.endDate) + "_" + timestamp
        basketPath = os.path.join(self.pathRoots["basketRoot"], basketName)

        if not os.path.exists(basketPath):
            os.makedirs(basketPath)
            log.info("Created new basket: " + basketPath)

        return basketPath

    def output(self):
        outputFolder = self.pathRoots["processingDir"]
        return wc.getLocalStateTarget(outputFolder, "prepareBasket.json")