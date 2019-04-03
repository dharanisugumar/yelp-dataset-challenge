#!/bin/bash

#-----------------------------------------------------------------------------------------
#       COPYRIGHT:  IMS AMERICA CORPORATION  -  ALL RIGHTS ARE RESERVED                  #
#                                                                                        #
#               NO part of this document may be stored, reproduced,                      #
#               or otherwise copied, in any form, or by any means,                       #
#             WITHOUT THE EXPRESSED WRITTEN PERMISSION OF IMS AMERICA.                   #
#                                                                                        #
#-----------------------------------------------------------------------------------------
# IDENTIFICATION
# ==============
# Program name : fr9_ch_processdwhstream_runsparkcluster.sh
# Program type : Batch
# Created by   : FRANCE Development Team
# Return       : 0 - Normal return
# Description  : Script to invoke dwh Stream process to load data from ODS to DWH.
#
#
# VERSION DETAILS
#-----------------------------------------------------------------------------------------
#  Version   Date              Modified By           Description
#-----------------------------------------------------------------------------------------
#  0.1       27-JUN-2018       Samuel William I       Initial Draft Version
#
#-----------------------------------------------------------------------------------------

# Example usage: ${FR9_APPS_VERSION_SCRIPTS}/spark/loadAsset/fr9_ch_ProcessDwhStream_runSparkCluster.sh "-p /development/fr9/apps/ch/current/scripts/001.txt -l 1"

# Capturing the Start Time of process.

start=`date +%s`
export YELP_DATASET_FILE=$1

hdfs dfs -put /development/fr9/data/sampleInput/jsonFiles/ /user/fr9dusr/src/main/resources/Files/

eval "$(sed -r 's/^([^=]*)=(.*)/export \1=\"\2\";/' ${APP_PROPERTY_FILE})"

echo "Getting kerberos ticket for user"
kinit -kt "/home/fr9dusr/fr9dusr.keytab" -V "fr9dusr@INTERNAL.IMSGLOBAL.COM"

echo "Invoking Spark2-submit command ...!"
spark2-submit \
  --master yarn \
  --deploy-mode client \
  --keytab "/home/fr9dusr/fr9dusr.keytab" \
  --principal "fr9dusr@INTERNAL.IMSGLOBAL.COM" \
  --conf spark.driver.extraClassPath="/development/fr9/apps/ch/1.0.1-SNAPSHOT-FRPARAPH-898/lib/com.newyorker.challenge-1.0-SNAPSHOT.jar" \
  --driver-memory 5G \
  --driver-cores 4 \
  --executor-memory 10G \
  --executor-cores 4 \
  --class service.jsonRead \
    "/development/fr9/apps/ch/1.0.1-SNAPSHOT-FRPARAPH-898/lib/com.newyorker.challenge-1.0-SNAPSHOT.jar"

if [ $? -ne 0 ]; then
    echo "spark2-submit to load json Stream Process has failed, hence Exiting ...!"
    exit 1
fi
echo "spark2-submit to load json Stream Process has completed successfully ...!"

# Function to calculate Total Time taken to complete this process.
function total_time_taken {
end=$1
runtime=$((end-start))

        echo "Duration: $((${runtime} / 3600 )) Hours, $(((${runtime} % 3600) / 60)) Minutes, $((${runtime} % 60)) Seconds"
}

end=`date +%s`
total_time_taken ${end}