#!/bin/csh -fx

#
# Program: snpmarker.csh
#
# Purpose:
#
#       Script for loading SNP_ConsensusSnp_Marker table
#       with dbSNP determined snp to marker associations
#       and MGI determined snp to marker associations.
#
# Usage:
#
# Usage:  snpmarker.csh
#
# History
#
# sc	08/17/2005 - created
# dbm	09/28/2005 - Added snpmrklocus.py & snpmrkwithin.py
# sc    01/2006 - process multiple snpmrkwithin.bcp files
# sc    03/2006 - convert to snp database add load of snp MRK_Location_Cache

cd `dirname $0` && source ./Configuration

cd ${CACHEDATADIR}

setenv LOG	${CACHELOGSDIR}/`basename $0`.log
rm -rf ${LOG}
touch ${LOG}

#
# Allow bcp into database
#
date | tee -a ${LOG}
echo "Allow bcp into database" | tee -a ${LOG}
${MGI_DBUTILS}/bin/turnonbulkcopy.csh ${SNPBE_DBSERVER} ${SNPBE_DBNAME} | tee -a ${LOG}

#
# Load dbSNP marker relationships
#
echo "Calling snpmarker.py" | tee -a ${LOG}
${CACHEINSTALLDIR}/snpmarker.py | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Truncate ${SNP_MRK_TABLE} table" | tee -a ${LOG}
${SNPBE_DBSCHEMADIR}/table/${SNP_MRK_TABLE}_truncate.object | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Drop indexes on ${SNP_MRK_TABLE} table" | tee -a ${LOG}
${SNPBE_DBSCHEMADIR}/index/${SNP_MRK_TABLE}_drop.object | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Load bcp file into ${SNP_MRK_TABLE} table" | tee -a ${LOG}
cat ${SNPBE_DBPASSWORDFILE} | bcp ${SNPBE_DBNAME}..${SNP_MRK_TABLE} in ${SNP_MRK_FILE} -c -t\| -S${SNPBE_DBSERVER} -U${SNPBE_DBUSER} | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Create indexes on ${SNP_MRK_TABLE} table" | tee -a ${LOG}
${SNPBE_DBSCHEMADIR}/index/${SNP_MRK_TABLE}_create.object | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Update statistics on ${SNP_MRK_TABLE} table" | tee -a ${LOG}
${MGI_DBUTILS}/bin/updateStatistics.csh ${SNPBE_DBSERVER} ${SNPBE_DBNAME} ${SNP_MRK_TABLE} | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Drop indexes on ${ACC_TABLE} table" | tee -a ${LOG}
${SNPBE_DBSCHEMADIR}/index/${ACC_TABLE}_drop.object | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Load bcp file into ${ACC_TABLE} table" | tee -a ${LOG}
cat ${SNPBE_DBPASSWORDFILE} | bcp ${SNPBE_DBNAME}..${ACC_TABLE} in ${ACC_FILE} -c -t \| -S${SNPBE_DBSERVER} -U${SNPBE_DBUSER} | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Create indexes on ${ACC_TABLE} table" | tee -a ${LOG}
${SNPBE_DBSCHEMADIR}/index/${ACC_TABLE}_create.object | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Update statistics on ${ACC_TABLE} table" | tee -a ${LOG}
${MGI_DBUTILS}/bin/updateStatistics.csh ${SNPBE_DBSERVER} ${SNPBE_DBNAME} ${ACC_TABLE} | tee -a ${LOG}

# Only run the following steps if the dbSNP and MGI coordinates are
# synchronized (same mouse genome build).
#
if ( ${IN_SYNC} == "yes" ) then

    # 
    # load snp..MRK_Location_Cache
    # 
    date | tee -a ${LOG}
    "calling  snpmrklocation.py"
    ${CACHEINSTALLDIR}/snpmrklocation.py

    # truncate snp..MRK_Location_Cache
    echo "truncating ${MRKLOC_CACHETABLE}"
    ${SNPBE_DBSCHEMADIR}/table/${MRKLOC_CACHETABLE}_truncate.object | tee -a ${LOG}

    echo "dropping indexes on ${MRKLOC_CACHETABLE}"
    ${SNPBE_DBSCHEMADIR}/index/${MRKLOC_CACHETABLE}_drop.object | tee -a ${LOG}

    echo "bcp'ing data into ${MRKLOC_CACHETABLE}"
    cat ${SNPBE_DBPASSWORDFILE} | bcp ${SNPBE_DBNAME}..${MRKLOC_CACHETABLE} in ${CACHEDATADIR}/${MRKLOC_CACHEFILE} -c -t\| -S${SNPBE_DBSERVER} -U${SNPBE_DBUSER} | tee -a  ${LOG}

    echo "creating indexes on ${MRKLOC_CACHETABLE}"
    ${SNPBE_DBSCHEMADIR}/index/${MRKLOC_CACHETABLE}_create.object | tee -a ${LOG}

    echo "updating statistics on ${MRKLOC_CACHETABLE}"
    ${MGI_DBUTILS}/bin/updateStatistics.csh ${SNPBE_DBSERVER} ${SNPBE_DBNAME} ${MRKLOC_CACHETABLE} | tee -a ${LOG}

    #
    # Update dbSNP locus-region function class to upstream/downstream
    #
    echo "" | tee -a ${LOG}
    date | tee -a ${LOG}
    echo "Calling snpmrklocus.py" | tee -a ${LOG}
    ${CACHEINSTALLDIR}/snpmrklocus.py | tee -a ${LOG}

    #
    # load MGI snp to marker relationships
    # 
    echo "" | tee -a ${LOG}
    date | tee -a ${LOG}
    echo "Calling snpmrkwithin.py" | tee -a ${LOG}
    ${CACHEINSTALLDIR}/snpmrkwithin.py | tee -a ${LOG}

    echo "" | tee -a ${LOG}
    date | tee -a ${LOG}
    echo "Drop indexes on ${SNP_MRK_TABLE} table" | tee -a ${LOG}
    ${SNPBE_DBSCHEMADIR}/index/${SNP_MRK_TABLE}_drop.object | tee -a ${LOG}
    echo "" | tee -a ${LOG}
    
    foreach i (${SNP_MRK_WITHIN_FILE}*)
	date | tee -a ${LOG}
	echo "Load bcp file into ${SNP_MRK_TABLE} table" | tee -a ${LOG}
	cat ${SNPBE_DBPASSWORDFILE} | bcp ${SNPBE_DBNAME}..${SNP_MRK_TABLE} in $i -c -t\| -S${SNPBE_DBSERVER} -U${SNPBE_DBUSER} | tee -a ${LOG}
    end

    echo "" | tee -a ${LOG}
    date | tee -a ${LOG}
    echo "Create indexes on ${SNP_MRK_TABLE} table" | tee -a ${LOG}
    ${SNPBE_DBSCHEMADIR}/index/${SNP_MRK_TABLE}_create.object | tee -a ${LOG}

    echo "" | tee -a ${LOG}
    date | tee -a ${LOG}
    echo "Update statistics on ${SNP_MRK_TABLE} table" | tee -a ${LOG}
    ${MGI_DBUTILS}/bin/updateStatistics.csh ${SNPBE_DBSERVER} ${SNPBE_DBNAME} ${SNP_MRK_TABLE} | tee -a ${LOG}

endif

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
