#!/bin/bash
# Run workflow serially
#SBATCH -p long-serial
#SBATCH --time=1-00:00:00
#SBATCH --mem=32G
#SBATCH -D $jobWorkingDir
#SBATCH -o $jobWorkingDir/%J.out
#SBATCH -e $jobWorkingDir/%J.err
#SBATCH -A defra_eo_jncc_s2_ard

/usr/bin/singularity exec --bind $reportMount:/report --bind $databaseMount:/database --bind $workingMount:/working --bind $stateMount:/state --bind $inputMount:/input --bind $staticMount:/static --bind $outputMount:/output $s2ArdContainer /app/exec.sh GenerateReport --dbFileName=s2ArdProcessing.db --reportFileName=$reportFileName --dem=$dem $arcsiReprojection --metadataConfigFile=$metadataConfigFile $metadataTemplate $arcsiCmdTemplate --removeInputFiles --local-scheduler
