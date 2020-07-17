#!/bin/bash
# Run ARCSI processing task
#SBATCH -d 'afterok:$upstreamJobId'
#SBATCH -p par-multi
# #SBATCH -C 
# #BSUB -R "select[hname!=host283]"
#SBATCH --nodes=$nodes
#SBATCH --time-min=06:00
#SBATCH --time=0-12:00:00
#SBATCH -D $jobWorkingDir
#SBATCH -o $jobWorkingDir/%J_ProcessRawToArd.out
#SBATCH -e $jobWorkingDir/%J_ProcessRawToArd.err
#SBATCH -A defra_eo_jncc_s2_ard

source /gws/smf/j04/defra_eo/eo-workflows/eo-workflows-venv/bin/activate
LUIGI_CONFIG_PATH='/gws/smf/j04/defra_eo/eo-workflows/luigi.cfg' PYTHONPATH='/gws/smf/j04/defra_eo/eo-workflows' luigi --module process_s2_basket ProcessRawToArd --singularityDir $singularityDir --singularityImgPath $arcsiContainer --workingMount $workingMount --stateMount $stateMount --staticMount $staticMount $testProcessing --local-scheduler


