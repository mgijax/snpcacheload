#!/bin/sh
#
#  snpmarker_wrapper.sh
###########################################################################
#
#  Purpose:
#
#      This script is a wrapper around the snp/marker cache load that
#      provides the process control.
#
#  Usage:
#
#      snpmarker_wrapper.sh
#
#  Env Vars:
#
#      See the configuration file
#
#  Inputs:  None
#
#  Outputs:
#
#      - Log file
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  Fatal error occurred
#
#  Assumes:  Nothing
#
#  Implementation:
#
#      This script will perform following steps:
#
#      1) Source the configuration files to establish the environment.
#      2) Wait for the flag to signal that the exporter is done exporting
#         the mgd database from Sybase to Postgres.
#      3) Run the snp/marker cache load.
#      4) Set the flag to signal that the snp database is ready.
#
#  Notes:  None
#
###########################################################################

cd `dirname $0`; . ./Configuration

SCRIPT_NAME=`basename $0`

LOG=${LOG_DIR}/${SCRIPT_NAME}.log
rm -f ${LOG}
touch ${LOG}

echo "$0" >> ${LOG}
env | sort >> ${LOG}

#
# Wait for the "Export Done" flag to be set. Stop waiting if the number
# of retries expires or the abort flag is found.
#
date >> ${LOG}
echo 'Wait for the "Export Done" flag to be set' >> ${LOG}

RETRY=${PROC_CTRL_RETRIES}
while [ ${RETRY} -gt 0 ]
do
    READY=`${PROC_CTRL_CMD_PROD}/getFlag ${NS_DATA_PREP} ${FLAG_EXPORT_DONE}`
    ABORT=`${PROC_CTRL_CMD_PROD}/getFlag ${NS_DATA_PREP} ${FLAG_ABORT}`

    if [ ${READY} -eq 1 -o ${ABORT} -eq 1 ]
    then
        break
    else
        sleep ${PROC_CTRL_WAIT_TIME}
    fi

    RETRY=`expr ${RETRY} - 1`
done

#
# Terminate the script if the number of retries expired or the abort flag
# was found.
#
if [ ${RETRY} -eq 0 ]
then
    echo "${SCRIPT_NAME} timed out" >> ${LOG}
    date >> ${LOG}
    exit 1
elif [ ${ABORT} -eq 1 ]
then
    echo "${SCRIPT_NAME} aborted by process controller" >> ${LOG}
    date >> ${LOG}
    exit 1
fi

#
# Run the snp/marker cache load.
#
date >> ${LOG}
echo 'Run the snp/marker cache load' >> ${LOG}
${SNPCACHELOAD}/snpmarker.sh >> ${LOG} 2>&1
if [ $? -ne 0 ]
then
    echo "${SCRIPT_NAME} failed" >> ${LOG}
    date >> ${LOG}
    exit 1
fi

#
# Set the "SNP Loaded" flag.
#
date | tee -a ${LOG}
echo 'Set process control flag: SNP Loaded' | tee -a ${LOG}
${PROC_CTRL_CMD_PROD}/setFlag ${NS_DATA_PREP} ${FLAG_SNP_LOADED} ${SCRIPT_NAME}

echo "${SCRIPT_NAME} completed successfully" >> ${LOG}
date >> ${LOG}

exit 0
