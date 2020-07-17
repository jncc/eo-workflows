# Run workflow serially
#!/bin/bash
#SBATCH -p short-serial
#SBATCH --time-min=01:00
#SBATCH --time=0-10:00:00
#SBATCH --mem=18G
#SBATCH -D $jobWorkingDir
#SBATCH -o $jobWorkingDir/%J.out
#SBATCH -e $jobWorkingDir/%J.err
##SBATCH -A defra_eo_jncc_s1_ard

/apps/singularity/bin/singularity exec --bind $reportMount:/report --bind $databaseMount:/database --bind $workingMount:/working --bind $stateMount:/state --bind $inputMount:/input --bind $staticMount:/static --bind $outputMount:/output $s1ArdContainer /app/exec.sh GenerateReport --productName=$productName --memoryLimit=16 --noStateCopy --reportFileName=$reportFileName --dbFileName=s1ArdProcessing.db --spatialConfig='{"snapConfigUtmProj": "$snapConfigUtmProj", "snapConfigCentralMeridian": "$snapConfigCentralMeridian", "snapConfigFalseNorthing": "$snapConfigFalseNorthing", "snapRunArguments": "$snapRunArguments", "sourceSrs": "$sourceSrs", "targetSrs": "$targetSrs", "filenameDemData": "$filenameDemData", "filenameSrs": "$filenameSrs", "demFilename": "$demFilename", "demTitle":"$demTitle", "metadataProjection": "$metadataProjection", "metadataPlaceName":"$metadataPlaceName", "metadataParentPlaceName":"$metadataParentPlaceName"}' $removeSourceFileFlag
