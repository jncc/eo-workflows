#!/bin/bash
# Run post-ARCSI processing tasks
#SBATCH -d 'afterok:$upstreamJobId'
#SBATCH -p short-serial
#SBATCH --time-min=03:00
#SBATCH --time=0-06:00:00
#SBATCH -D $jobWorkingDir
#SBATCH -o $jobWorkingDir/%J_GenerateReport.out
#SBATCH -e $jobWorkingDir/%J_GenerateReport.err
#SBATCH -A defra_eo_jncc_s2_ard

/usr/bin/singularity exec --bind $reportMount:/report --bind $databaseMount:/database --bind $workingMount:/working --bind $stateMount:/state --bind $inputMount:/input --bind $staticMount:/static --bind $outputMount:/output $s2ArdContainer /app/exec.sh GenerateReport --dbFileName=s2ArdProcessing.db --reportFileName=$reportFileName --dem=$dem $arcsiReprojection --metadataConfigFile=$metadataConfigFile $metadataTemplate $arcsiCmdTemplate --oldFilenameDateThreshold=$oldFilenameDateThreshold --maxCogProcesses=$maxCogProcesses --removeInputFiles --local-scheduler
