#!/bin/sh
#
# Purpose:
#
#       Script for loading SNP_ConsensusSnp_Marker table
#       with dbSNP determined snp to marker associations
#       and MGI determined snp to marker associations.
#
# Since this runs on a server that does *not* have radar installed,
# this script cannot use the radar job stream functions.
#
# This is run as part of the Pipeline and uses LASTRUN.
#
# History
#
# lec	01/31/2025
#	e4g-127/Process Alliance File to Load SNP_ConsensusSnp_Marker
#	snpmarker.sh previous version is saved snpmarker.sh.bak
#

cd `dirname $0` 

COMMON_CONFIG=Configuration

#
# Make sure the common configuration file exists and source it. 
#
if [ -f ${COMMON_CONFIG} ]
then
    . ${COMMON_CONFIG}
else
    echo "Missing configuration file: ${COMMON_CONFIG}"
    exit 1
fi

rm -rf ${SNPMARKER_LOG}
touch ${SNPMARKER_LOG}

#
# If the checked-file exists and is more recent than the last checked-file, the load does not need to be run.
# The checked-file : ${CACHEDATADIR}/snpalliance.output.Y.tsv
#
LASTRUN_FILE=${CACHEDATADIR}/lastrun
if [ -f ${LASTRUN_FILE} ]
then
	echo "LASTRUN_FILE exists - skipping load" | tee -a ${SNPMARKER_LOG}
        exit 0
fi

#
# create SNP_ConsensusSnp_Marker bcp files
# 
date | tee -a ${SNPMARKER_LOG}
echo "Processing snpmrkwithin.py to create SNP_ConsensusSnp_Marker bcp files" | tee -a ${SNPMARKER_LOG}
${PYTHON} ${SNPCACHELOAD}/snpmrkwithin.py >> ${SNPMARKER_LOG} 2>&1
STAT=$?
if [ ${STAT} -ne 0 ]
then
	echo "${SNPCACHELOAD}/snpmrkwithin.py failed" | tee -a ${SNPMARKER_LOG}
	exit 1
fi

#
# drop foreign keys & indexes, truncate SNP_ConsensusSnp_Marker
#
date | tee -a ${SNPMARKER_LOG}
cd ${CACHEDATADIR}
${SNP_DBSCHEMADIR}/key/SNP_ConsensusSnp_Marker_drop.object >> ${SNPMARKER_LOG} 2>&1
${SNP_DBSCHEMADIR}/index/SNP_ConsensusSnp_Marker_drop.object >> ${SNPMARKER_LOG} 2>&1
${SNP_DBSCHEMADIR}/table/SNP_ConsensusSnp_Marker_truncate.object >> ${SNPMARKER_LOG} 2>&1

#
# load each bcp file into the SNP_ConsensusSnp_Marker table
#
date | tee -a ${SNPMARKER_LOG}
echo "Loading SNP_ConsensusSnp_Marker by Chromosome"  | tee -a ${SNPMARKER_LOG}
for i in `ls ${SNP_MRK_FILE}*`
do
    date | tee -a ${SNPMARKER_LOG}
    echo "Load ${i} into ${SNP_MRK_TABLE} table" | tee -a ${SNPMARKER_LOG}
    echo "" | tee -a ${SNPMARKER_LOG}
    ${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} ${SNP_MRK_TABLE} ${CACHEDATADIR} ${i} "|" "" snp >> ${SNPMARKER_LOG} 2>&1
    STAT=$?
    if [ ${STAT} -ne 0 ]
    then
    	echo "${PG_DBUTILS}/bin/bcpin.csh failed" | tee -a ${SNPMARKER_LOG}
	exit 1
    fi
done

#
# re-create foreign keys & indexes
#
date | tee -a ${SNPMARKER_LOG}
echo "Create primary key & index on SNP_ConsensusSnp_Marker"  | tee -a ${SNPMARKER_LOG}
${SNP_DBSCHEMADIR}/key/SNP_ConsensusSnp_Marker_create.object >> ${SNPMARKER_LOG} 2>&1
${SNP_DBSCHEMADIR}/index/SNP_ConsensusSnp_Marker_create.object >> ${SNPMARKER_LOG} 2>&1
STAT=$?
if [ ${STAT} -ne 0 ]
then
	echo "${SNPCACHELOAD}/snpmrkwithin.py failed" | tee -a ${SNPMARKER_LOG}
	exit 1
fi
date | tee -a ${SNPMARKER_LOG}

#
# Touch the "lastrun" file to note when the load was run.
#
if [ ${STAT} = 0 ]
then
    touch ${LASTRUN_FILE}
fi

rm -rf ${CACHEDATADIR}/lastrun.dump

