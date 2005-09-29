#!/bin/csh -fx

#
# Program: snpmarker.csh
#
# Purpose:
#
#       Script for loading SNP_ConsensusSnp_Marker table
#       with dbSNP determined snp to marker associations
#       and RefSNP to marker associations.
#
# Usage:
#
# Usage:  snpmarker.csh
#
# History
#
# sc	08/17/2005
# dbm	09/28/2005 - Added snpmrklocus.py & snpmrkwithin.py
#

cd `dirname $0` && source ./Configuration

cd ${CACHEDATADIR}

setenv LOG	${CACHELOGSDIR}/`basename $0`.log
rm -rf ${LOG}
touch ${LOG}

date | tee -a ${LOG}
echo "Calling snpmarker.py" | tee -a ${LOG}
${CACHEINSTALLDIR}/snpmarker.py | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Allow bcp into database" | tee -a ${LOG}
${DBUTILSBINDIR}/turnonbulkcopy.csh ${DBSERVER} ${DBNAME} | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Truncate ${MRKR_TABLE} table" | tee -a ${LOG}
${SCHEMADIR}/table/${MRKR_TABLE}_truncate.object | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Drop indexes on ${MRKR_TABLE} table" | tee -a ${LOG}
${SCHEMADIR}/index/${MRKR_TABLE}_drop.object | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Load bcp file into ${MRKR_TABLE} table" | tee -a ${LOG}
cat ${DBPASSWORDFILE} | bcp ${DBNAME}..${MRKR_TABLE} in ${MRKR_TABLE}.bcp -c -t\| -S${DBSERVER} -U${DBUSER} | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Create indexes on ${MRKR_TABLE} table" | tee -a ${LOG}
${SCHEMADIR}/index/${MRKR_TABLE}_create.object | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Update statistics on ${MRKR_TABLE} table" | tee -a ${LOG}
${DBUTILSBINDIR}/updateStatistics.csh ${DBSERVER} ${DBNAME} ${MRKR_TABLE} | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Drop indexes on ${ACC_TABLE} table" | tee -a ${LOG}
${SCHEMADIR}/index/${ACC_TABLE}_drop.object | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Load bcp file into ${ACC_TABLE} table" | tee -a ${LOG}
cat ${DBPASSWORDFILE} | bcp ${DBNAME}..${ACC_TABLE} in ${ACC_TABLE}.bcp -c -t \| -S${DBSERVER} -U${DBUSER} | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Create indexes on ${ACC_TABLE} table" | tee -a ${LOG}
${SCHEMADIR}/index/${ACC_TABLE}_create.object | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Update statistics on ${ACC_TABLE} table" | tee -a ${LOG}
${DBUTILSBINDIR}/updateStatistics.csh ${DBSERVER} ${DBNAME} ${ACC_TABLE} | tee -a ${LOG}

# Only run the following steps if the dbSNP and MGI coordinates are
# synchronized (same mouse genome build).
#
if ( ${IN_SYNC} == "yes" ) then

    echo "" | tee -a ${LOG}
    date | tee -a ${LOG}
    echo "Calling snpmrklocus.py" | tee -a ${LOG}
    ${CACHEINSTALLDIR}/snpmrklocus.py | tee -a ${LOG}

    echo "" | tee -a ${LOG}
    date | tee -a ${LOG}
    echo "Calling snpmrkwithin.py" | tee -a ${LOG}
    ${CACHEINSTALLDIR}/snpmrkwithin.py | tee -a ${LOG}

    echo "" | tee -a ${LOG}
    date | tee -a ${LOG}
    echo "Calling snpmrklocus.py" | tee -a ${LOG}
    echo "Drop indexes on ${SNP_MRK_TABLE} table" | tee -a ${LOG}
    ${SCHEMADIR}/index/${SNP_MRK_TABLE}_drop.object | tee -a ${LOG}

    echo "" | tee -a ${LOG}
    date | tee -a ${LOG}
    echo "Load bcp file into ${SNP_MRK_TABLE} table" | tee -a ${LOG}
    cat ${DBPASSWORDFILE} | bcp ${DBNAME}..${SNP_MRK_TABLE} in ${SNP_MRK_FILE} -c -t\| -S${DBSERVER} -U${DBUSER} | tee -a ${LOG}

    echo "" | tee -a ${LOG}
    date | tee -a ${LOG}
    echo "Create indexes on ${SNP_MRK_TABLE} table" | tee -a ${LOG}
    ${SCHEMADIR}/index/${SNP_MRK_TABLE}_create.object | tee -a ${LOG}

    echo "" | tee -a ${LOG}
    date | tee -a ${LOG}
    echo "Update statistics on ${SNP_MRK_TABLE} table" | tee -a ${LOG}
    ${DBUTILSBINDIR}/updateStatistics.csh ${DBSERVER} ${DBNAME} ${SNP_MRK_TABLE} | tee -a ${LOG}

endif

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
