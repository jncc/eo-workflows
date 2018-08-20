import luigi
import logging
import os
import datetime
import workflow_common.common as wc
from datetime import timedelta
from os.path import join
from luigi.util import requires
from process_s1_daily.SetupDirectories import SetupDirectories

log = logging.getLogger('luigi-interface')

@requires(SetupDirectories)
class GetPreviousProductsList(luigi.ExternalTask):
    pathRoots = luigi.DictParameter()
    runDate = luigi.DateParameter()

    def output(self):
        previousRunDate = self.runDate - timedelta(days=1)
        return wc.getLocalDatedStateTarget(self.pathRoots["processingRootDir"], previousRunDate, 'ProductsList.json')