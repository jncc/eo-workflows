import luigi
import logging
import json
import os
import process_s1_basket.common as wc
from os.path import join

log = logging.getLogger('luigi-interface')

class GetS1ScenesByDate(luigi.Task):
    pathRoots = luigi.DictParameter()
    startDate = luigi.Parameter()
    endDate = luigi.Parameter()

    def run(self):
        output = {
            'rawList': ["/home/vagrant/workspace/S1_processing/s1_raw/S1A_IW_GRDH_1SDV_20171101T180600_20171101T180625_019075_020438_0920.zip",
            "/home/vagrant/workspace/S1_processing/s1_raw/S1A_IW_GRDH_1SDV_20171105T173312_20171105T173337_019133_0205FC_60B5.zip"]
        }

        with self.output().open('w') as out:
            out.write(json.dumps(output))


    def output(self):
        outputFolder = self.pathRoots["processingDir"]
        return wc.getLocalStateTarget(outputFolder, "getS1ScenesByDate.json")