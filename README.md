# EO Workflows
The EO workflow tasks for running on JASMIN. These tasks will prepare and run the Sentinel 1 and Sentinel 2 ARD workflows.

## Run Workflows

### Sentinel 1

#### Process S1 Basket
This workflow takes all raw products in a given directory and processes them to ARD in a Singularity container on LOTUS.
```
PYTHONPATH='.' luigi --module process_s1_basket RunSingularityInLotus
```

#### Process S1 Range
This workflow searches for scenes using a date range and polygon area and processes them to ARD in a Singularity container on LOTUS. maxScenes limits the number of scenes that can be processed in a run.
```
PYTHONPATH='.' luigi --module process_s1_range SubmitJobs --startDate 2018-07-30 --endDate 2018-07-30 --maxScenes 10
```

#### Process S1 Daily
This workflow is intended to be run daily by a cron job. It will look for the ProductsList.json file from the previous day's run and carry forward any unprocessed scenes. To start, you should copy the contents of the seed directory into your processing root directory and change the date as appropriate. Using --testProcessing will mock the interaction with LOTUS for local testing.
```
PYTHONPATH='.' luigi --module process_s1_daily GenerateProductsList --runDate 2018-07-30 --maxScenes 10
```


## Development
### Setup
Create virtual env
```
virtualenv -p python3 /<project path>/eo-workflows-venv
```
Activate the virtual env
```
source ./eo-workflows-venv/bin/activate
```
Install Requirements
```
pip install -r requirements.txt
```

#### Update Requirements
```
pip freeze > requirements.txt
```