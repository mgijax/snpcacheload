#!/bin/sh -x

#
# Program: snpmarker_weekly.sh
#
# Purpose:
#       1) run snpmarker.sh to create new snp/marker associations in 
#          a backend snp database
#       2) create a backup of the source snp db
#       3) load the backup into the destination snp db
#
# Usage:  snpmarker_weekly.sh [-a] [-r] [-c] [-l]
#         where:
#             -a is an option to archive output directory
#             -r is an option to archive logs directory
#             -c is an option to clean output directory
#             -l is an option to clean logs directory
#
# History
#
# lec 06/30/2006 - modified for mgiconfig
# 
# sc    05/01/2006 - updated to bourne shell
#		   - added optional archiving and directory cleaning
#                    for logs and output dirs
#		   - updated to use new bcpin_withTruncateDropIndex.csh
# sc    03/27/2006 - created

#
#  Set up a log file for the shell script in case there is an error
#  during configuration and initialization.
#
cd `dirname $0` 
LOG=`pwd`/`basename $0`.log
rm -f ${LOG}
 
echo date | tee -a ${LOG}
#
#  Verify the argument(s) to the shell script.
#
ARC_OUTPUT=no
ARC_LOGS=no
CLEAN_OUTPUT=no
CLEAN_LOGS=no

usage="Usage: snpmarker_weekly.sh [-a] [-r] [-c] [-l]"

#
# report usage if there are unrecognized arguments
#
set -- `getopt arcl $*`
if [ $? != 0 ]
then
    echo ${usage} | tee -a ${LOG}
    exit 1
fi

#
# determine which options on command line
#
for i in $*
do
    case $i in
        -a) ARC_OUTPUT=yes; shift;;
        -r) ARC_LOGS=yes; shift;;
        -c) CLEAN_OUTPUT=yes; shift;;
        -l) CLEAN_LOGS=yes; shift;;
        --) shift; break;;
    esac
done

#
#  Establish the configuration file name, if readable source it
#
CONFIG_LOAD=`pwd`/Configuration
if [ ! -r ${CONFIG_LOAD} ]
then
    echo "Cannot read configuration file: ${CONFIG_LOAD}" | tee -a ${LOG}
    exit 1
fi

. ${CONFIG_LOAD}

#
#  Source the DLA library functions.
#
if [ "${DLAJOBSTREAMFUNC}" != "" ]
then
    if [ -r ${DLAJOBSTREAMFUNC} ]
    then
        . ${DLAJOBSTREAMFUNC}
    else
        echo "Cannot source DLA functions script: ${DLAJOBSTREAMFUNC}" | tee -a ${LOG}
        exit 1
    fi
else
    echo "Environment variable DLAJOBSTREAMFUNC has not been defined." | tee -a ${LOG}
    exit 1
fi

#
# archive/clean the logs and/or output directories?
#
if [ ${ARC_OUTPUT} = "yes" ]
then
    date | tee -a ${LOG}
    echo "archiving  output directory" | tee -a ${LOG}
    createArchive ${ARCHIVEDIR}/output ${CACHEDATADIR}
    STAT=$?
    if [ ${STAT} -ne 0 ]
    then
        echo "createArchive for output directory failed" | tee -a ${LOG}
        exit 1
    fi
fi

if [ ${CLEAN_OUTPUT} = "yes" ]
then
    date | tee -a ${LOG}
    echo "cleaning  output directory" | tee -a ${LOG}
    cleanDir ${CACHEDATADIR}
    STAT=$?
    if [ ${STAT} -ne 0 ]
    then
        echo "cleanDir for output directory failed" | tee -a ${LOG}
        exit 1
    fi

fi

if [ ${ARC_LOGS} = "yes" ]
then
    date | tee -a ${LOG}
    echo "archiving logs directory" | tee -a ${LOG}
    createArchive ${ARCHIVEDIR}/logs ${CACHELOGSDIR}
    STAT=$?
    if [ ${STAT} -ne 0 ]
    then
        echo "createArchive for logs directory failed" | tee -a ${LOG}
        exit 1
    fi
fi

if [ ${CLEAN_LOGS} = "yes" ]
then
    date | tee -a ${LOG}
    echo "cleaning logs directory" | tee -a ${LOG}
    cleanDir ${CACHELOGSDIR}
    STAT=$?
    if [ ${STAT} -ne 0 ]
    then
        echo "cleanDir for logs directory failed" | tee -a ${LOG}
        exit 1
    fi
fi

date | tee -a ${LOG}

#
#   Establish the load log now that we have archived/cleaned
#
LOAD_LOG=${CACHELOGSDIR}/`basename $0`.log
date | tee -a ${LOAD_LOG}

cd ${CACHEDATADIR}

#
# run snpmarker.sh with no archive/clean to load the source db 
#

date | tee -a ${LOAD_LOG}
echo "Calling ${SNPCACHELOAD}/snpmarker.sh" | tee -a ${LOAD_LOG}
${SNPCACHELOAD}/snpmarker.sh
STAT=$?
if [ ${STAT} -ne 0 ]
then
     echo "${SNPCACHELOAD}/snpmarker.sh failed" | tee -a ${LOAD_LOG}
     exit 1
fi

#
# backup back end production snp database
#
echo "" | tee -a ${LOAD_LOG}
date | tee -a ${LOAD_LOG}
echo "Backing up ${SNPBE_DBSERVER}..${SNPBE_DBNAME}"
${MGI_DBUTILS}/bin/dump_db.csh ${SNPBE_DBSERVER} ${SNPBE_DBNAME} ${SNP_BACKUP_LOCALPATH} | tee -a ${LOAD_LOG}

#
# load front-end snp database
#
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Loading ${SNP_DBSERVER}..${SNP_DBNAME}"
${MGI_DBUTILS}/bin/load_db.csh ${SNP_DBSERVER} ${SNP_DBNAME} ${SNP_BACKUP_REMOTEPATH} | tee -a ${LOAD_LOG}

echo "" | tee -a ${LOAD_LOG}
date | tee -a ${LOAD_LOG}

