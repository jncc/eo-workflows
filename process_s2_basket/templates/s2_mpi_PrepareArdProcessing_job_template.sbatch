#!/bin/bash
# Run pre-ARCSI processing tasks
#SBATCH -p short-serial
#SBATCH --time-min=01:00
#SBATCH --time=0-04:00:00
#SBATCH -D $jobWorkingDir
#SBATCH -o $jobWorkingDir/%J_PrepareArdProcessing.out
#SBATCH -e $jobWorkingDir/%J_PrepareArdProcessing.err
#SBATCH -A defra_eo_jncc_s2_ard

/apps/singularity/bin/singularity exec --bind $workingMount:/working --bind $stateMount:/state --bind $inputMount:/input --bind $staticMount:/static --bind $outputMount:/output $s2ArdContainer /app/exec.sh PrepareArdProcessing --dem=$dem $outWktArg $projAbbvArg $arcsiCmdTemplate --local-scheduler