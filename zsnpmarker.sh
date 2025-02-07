#!/bin/sh
#
# Program: snpmarker.sh
#
# Purpose:
#
#       Script for loading SNP_ConsensusSnp_Marker table
#       with dbSNP determined snp to marker associations
#       and MGI determined snp to marker associations.
#
# Usage:  snpmarker.sh
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

#
# Establish bcp file delimiters
# 
# bcp file row delimiter
NL="\n"
# bcp file column delimiter
DL="|"
# name of the snp schema
SCHEMA='snp'

CONFIG_LOAD=`pwd`/Configuration
. ${CONFIG_LOAD}
rm -rf ${SNPMARKER_LOG}
touch ${SNPMARKER_LOG}

cd ${CACHEDATADIR}
${SNP_DBSCHEMADIR}/key/SNP_ConsensusSnp_Marker_drop.object >> ${SNPMARKER_LOG} 2>&1
${SNP_DBSCHEMADIR}/index/SNP_ConsensusSnp_Marker_drop.object >> ${SNPMARKER_LOG} 2>&1
${SNP_DBSCHEMADIR}/table/SNP_ConsensusSnp_Marker_truncate.object >> ${SNPMARKER_LOG} 2>&1
date | tee -a ${SNPMARKER_LOG}
echo "Loading SNP_ConsensusSnp_Marker by Chromosome"  | tee -a ${SNPMARKER_LOG}
for i in `ls ${SNP_MRK_FILE}*`
do
	date | tee -a ${SNPMARKER_LOG}
	echo "Load ${i} into ${SNP_MRK_TABLE} table" | tee -a ${SNPMARKER_LOG}
	echo "" | tee -a ${SNPMARKER_LOG}
	${PG_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} ${SNP_MRK_TABLE} ${CACHEDATADIR} ${i} ${DL} 'notused' ${SCHEMA} >> ${SNPMARKER_LOG} 2>&1
	STAT=$?
	if [ ${STAT} -ne 0 ]
	then
	    echo "${PG_DBUTILS}/bin/bcpin.csh failed" | tee -a ${SNPMARKER_LOG}
	    exit 1
	fi
done

#
# re-create keys at the end
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

