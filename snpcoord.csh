#!/bin/csh -fx

#
# Usage:  snpcoord.csh
#
# History
#
# sc	08/17/2005
#

cd `dirname $0` && source ./Configuration

cd ${CACHEDATADIR}

setenv LOG	${CACHELOGSDIR}/`basename $0`.log
rm -rf ${LOG}
touch ${LOG}

setenv TABLE	SNP_Coord_Cache

date | tee -a ${LOG}

# Create the bcp file

${CACHEINSTALLDIR}/snpcoord.py | tee -a ${LOG}

# Allow bcp into database and truncate tables

#${DBUTILSBINDIR}/turnonbulkcopy.csh ${DBSERVER} ${DBNAME} | tee -a ${LOG}
${SCHEMADIR}/table/${TABLE}_truncate.object | tee -a ${LOG}

# Drop indexes
${SCHEMADIR}/index/${TABLE}_drop.object | tee -a ${LOG}

# BCP new data into tables
cat ${DBPASSWORDFILE} | bcp ${DBNAME}..${TABLE} in ${TABLE}.bcp -c -t\| -S${DBSERVER} -U${DBUSER} | tee -a ${LOG}

# Create indexes
${SCHEMADIR}/index/${TABLE}_create.object | tee -a ${LOG}

${DBUTILSBINDIR}/updateStatistics.csh ${DBSERVER} ${DBNAME} ${TABLE} | tee -a ${LOG}

date | tee -a ${LOG}
