#!/bin/csh -fx

#
# Program: snpmarker.csh
#
# Purpose:
#
#       Script for loading
#	SNP_ConsensusSnp_Marker table
#       with dbSNP determined snp to
#       marker associations
#
# Usage:
#
# Usage:  snpmarker.csh
#
# History
#
# sc    08/17/2005
#

cd `dirname $0` && source ./Configuration

cd ${CACHEDATADIR}

setenv LOG	${CACHELOGSDIR}/`basename $0`.log
rm -rf ${LOG}
touch ${LOG}

date | tee -a ${LOG}

# Create the bcp file

${CACHEINSTALLDIR}/snpmarker.py | tee -a ${LOG}

# Allow bcp into database and truncate MRKR_TABLE

${DBUTILSBINDIR}/turnonbulkcopy.csh ${DBSERVER} ${DBNAME} | tee -a ${LOG}
${SCHEMADIR}/table/${MRKR_TABLE}_truncate.object | tee -a ${LOG}

# Drop indexes on MRKR_TABLE
${SCHEMADIR}/index/${MRKR_TABLE}_drop.object | tee -a ${LOG}

# BCP new data into MRKR_TABLE
cat ${DBPASSWORDFILE} | bcp ${DBNAME}..${MRKR_TABLE} in ${MRKR_TABLE}.bcp -c -t\| -S${DBSERVER} -U${DBUSER} | tee -a ${LOG}

# Create indexes on MRKR_TABLE
${SCHEMADIR}/index/${MRKR_TABLE}_create.object | tee -a ${LOG}

${DBUTILSBINDIR}/updateStatistics.csh ${DBSERVER} ${DBNAME} ${MRKR_TABLE} | tee -a ${LOG}

# Drop indexes on ACC_TABLE
${SCHEMADIR}/index/${ACC_TABLE}_drop.object | tee -a ${LOG}

# BCP new data into ACC_TABLE
cat ${DBPASSWORDFILE} | bcp ${DBNAME}..${ACC_TABLE} in ${ACC_TABLE}.bcp -c -t \| -S${DBSERVER} -U${DBUSER} | tee -a ${LOG}

# Create indexes on ACC_TABLE
${SCHEMADIR}/index/${ACC_TABLE}_create.object | tee -a ${LOG}

${DBUTILSBINDIR}/updateStatistics.csh ${DBSERVER} ${DBNAME} ${ACC_TABLE} | tee -a ${LOG}
date | tee -a ${LOG}
