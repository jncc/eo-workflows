# EO Workflows
The EO workflow tasks for running on JASMIN. These tasks will prepare and run the Sentinel 1 and Sentinel 2 ARD workflows.

## Run Workflows

### Sentinel 1

#### Process S1 Basket
This workflow takes all raw products in a given directory and processes them to ARD in a Singularity container on LOTUS.
```
PYTHONPATH='.' luigi --module process_s1_basket ProcessBasket
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