import luigi
import logging
import json
import os
import workflow_common.common as wc
import feedparser
import requests
import copy
import urllib
from os.path import join
from urllib.parse import urlencode
from luigi.util import requires
from process_s1_daily.SetupDirectories import SetupDirectories

log = logging.getLogger('luigi-interface')

CEDA_OPENSEARCH_URL = 'http://opensearch.ceda.ac.uk/opensearch/json?'
PAGE_SIZE = 20
@requires(SetupDirectories)
class GetS1ProductsByDateAndPolygon(luigi.Task):
    pathRoots = luigi.DictParameter()
    runDate = luigi.DateParameter()
    maxScenes = luigi.IntParameter()

    def run(self):
        polygon = self.getPolygonFromFile()
        page = 1

        query = self.getBaseQuery(polygon, page)
        url = CEDA_OPENSEARCH_URL + urlencode(query)
        result = self.queryCEDA(url)

        productList = self.getProductList(result)
        log.info("Records returned: %d", len(result["rows"]))

        if self.hasNextPage(result, page):
            log.info("Retrieving next page (%d)", page+1)
            productList += self.getNextPages(polygon, page+1)
        
        output = {
            "requestUrl": url,
            "products": productList
        }

        with self.output().open('w') as out:
            out.write(wc.getFormattedJson(output))

    def getBaseQuery(self, polygon, page):
        query = {
            "maximumRecords": PAGE_SIZE,
            "startPage": page,
            "mission": "sentinel-1",
            "productType": "GRD",
            "geometry": polygon,
            "startDate": self.runDate,
            "endDate": self.runDate
        }

        return query

    def hasNextPage(self, result, page):
        return int(result["totalResults"]) > PAGE_SIZE and (page * PAGE_SIZE) < int(result["totalResults"])

    def getNextPages(self, polygon, page):
        query = self.getBaseQuery(polygon, page)
        url = CEDA_OPENSEARCH_URL + urlencode(query)

        result = requests.get(url)
        jsonResult = result.json()

        productList = self.getProductList(jsonResult)

        if self.hasNextPage(jsonResult, page):
            log.info("Retrieving next page %d", page+1)
            productList += self.getNextPages(polygon, page + 1)
        else:
            return productList

    def queryCEDA(self, url):
        log.info("Sending request to OpenSearch: " + url)

        result = requests.get(url)
        jsonResult = result.json()

        if jsonResult == None or jsonResult["totalResults"] == 0:
            log.exception("Read CEDA feed failed")
            raise TypeError("No result returned")

        totalResults = int(jsonResult["totalResults"])
        log.info("Total results %d", totalResults)

        # if total records exceeds maxScenes shrink the window
        if totalResults > self.maxScenes:
            msg = "Found " + str(totalResults) + " results, however cannot process more than " + str(self.maxScenes) + " results at a time"
            log.exception(msg)
            raise Exception(msg)

        return jsonResult

    def getPolygonFromFile(self):
        with open(self.pathRoots["polygonFile"], "r") as polygonFile:
            polygon = polygonFile.read()
        
        return polygon

    def getProductList(self, result):
        productList = []
        for row in result["rows"]:
            product = {
                "productId": row["misc"]["product_info"]["Name"],
                "initialRunDate": str(self.runDate),
                "filepath": os.path.join(row["file"]["directory"], row["file"]["data_file"]),
                "jobId": None,
                "submitTime": None
            }
            productList.append(product)

        return productList

    def output(self):
        return wc.getLocalDatedStateTarget(self.pathRoots["processingRootDir"], self.runDate, "getS1ProductsByDateAndPolygon.json")




