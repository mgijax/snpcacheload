#!/bin/csh -fx

#
# Program: snpmarker_weekly.csh
#
# Purpose:
#       1) run snpmarker.csh to create new snp/marker associations in 
#          a 'load' snp database (source snp db)
#       2) create bcp files for SNP_ConsensusSnp_Marker and SNP_Accession
#          from a source snp database to bcp into a target snp database
#       3) truncates SNP_ConsensusSnp_Marker and SNP_Accession in target db
#       4) bcps into target db
#
# Usage:  snpmarker_weekly.csh
#
# History
#
# sc	03/27/2006 - created

cd `dirname $0` && source ./Configuration

cd ${CACHEDATADIR}

setenv LOG	${CACHELOGSDIR}/`basename $0`.log
rm -rf ${LOG}
touch ${LOG}

#
# run snpmarker.csh - to load the source db 
#

date | tee -a ${LOG}
echo "Calling snpmarker.csh" | tee -a ${LOG}
${CACHEINSTALLDIR}/snpmarker.csh
if ( $status ) then
     echo "${CACHEINSTALLDIR}/snpmarker.csh failed" | tee -a ${LOG}
     exit 1
endif

#
# create bcp files for SNP_ConsensusSnp_Marker 
# from back end production snp database 
#
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Calling ${MGIDBUTILSBINDIR}/bcpout.csh" | tee -a ${LOG}
${MGIDBUTILSBINDIR}/bcpout.csh ${SNP_DBSCHEMADIR} ${SNP_MRK_TABLE} ${CACHEDATADIR} ${PROD_SNP_MRK_FILE}| tee -a ${LOG}
if ( $status ) then
    echo "${MGIDBUTILSBINDIR}/bcpout.csh failed" | tee -a ${LOG}
    exit 1
endif

#
# truncate production SNP_ConsensusSnp_Marker
#
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Truncate ${PRODSNP_DBSERVER}..${PRODSNP_DBNAME}..${SNP_MRK_TABLE} table" | tee -a ${LOG}
${PRODSNP_DBSCHEMADIR}/table/${SNP_MRK_TABLE}_truncate.object | tee -a ${LOG}

#
# drop indexes on production SNP_ConsensusSnp_Marker
# 
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Drop indexes on ${PRODSNP_DBSERVER}..${PRODSNP_DBNAME}..${SNP_MRK_TABLE} table" | tee -a ${LOG}
${PRODSNP_DBSCHEMADIR}/index/${SNP_MRK_TABLE}_drop.object | tee -a ${LOG}

#
# load bcp file for SNP_ConsensusSnp_Marker into production snp database
#
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Calling ${MGIDBUTILSBINDIR}/bcpin.csh" | tee -a ${LOG}
${MGIDBUTILSBINDIR}/bcpin.csh ${PRODSNP_DBSCHEMADIR} ${SNP_MRK_TABLE} ${CACHEDATADIR} ${PROD_SNP_MRK_FILE} | tee -a ${LOG}
if ( $status ) then
    echo "${MGIDBUTILSBINDIR}/bcpout.csh failed" | tee -a ${LOG}
    exit 1
endif

#
# create indexes on production SNP_ConsensusSnp_Marker
#
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Create indexes on ${PRODSNP_DBSERVER}..${PRODSNP_DBNAME}..${SNP_MRK_TABLE} table" | tee -a ${LOG}
${PRODSNP_DBSCHEMADIR}/index/${SNP_MRK_TABLE}_create.object | tee -a ${LOG}

#
# update statistics on production SNP_ConsensusSnp_Marker
#
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Update statistics on ${PRODSNP_DBSERVER}..${PRODSNP_DBNAME}..${SNP_MRK_TABLE} table" | tee -a ${LOG}
${DBUTILSBINDIR}/updateStatistics.csh ${PRODSNP_DBSERVER} ${PRODSNP_DBNAME} ${SNP_MRK_TABLE} | tee -a ${LOG}

#
# create bcp files for SNP_Accession
# from back end production snp database
#
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Calling ${MGIDBUTILSBINDIR}/bcpout.csh" | tee -a ${LOG}
${MGIDBUTILSBINDIR}/bcpout.csh ${SNP_DBSCHEMADIR} ${ACC_TABLE} ${CACHEDATADIR} ${PROD_ACC_FILE}| tee -a ${LOG}
if ( $status ) then
    echo "${MGIDBUTILSBINDIR}/bcpout.csh failed" | tee -a ${LOG}
    exit 1
endif

#
# truncate production SNP_Accession
#
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Truncate ${PRODSNP_DBSERVER}..${PRODSNP_DBNAME}..${ACC_TABLE} table" | tee -a ${LOG}
${PRODSNP_DBSCHEMADIR}/table/${ACC_TABLE}_truncate.object | tee -a ${LOG}

#
# drop indexes on production SNP_Accession 
#
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Drop indexes on ${PRODSNP_DBSERVER}..${PRODSNP_DBNAME}..${ACC_TABLE} table" | tee -a ${LOG}
${PRODSNP_DBSCHEMADIR}/index/${ACC_TABLE}_drop.object | tee -a ${LOG}

#
# load bcp file for SNP_Accession into production snp database
#
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Calling ${MGIDBUTILSBINDIR}/bcpin.csh" | tee -a ${LOG}
${MGIDBUTILSBINDIR}/bcpin.csh ${PRODSNP_DBSCHEMADIR} ${ACC_TABLE} ${CACHEDATADIR} ${PROD_ACC_FILE} | tee -a ${LOG}
if ( $status ) then
    echo "${MGIDBUTILSBINDIR}/bcpout.csh failed" | tee -a ${LOG}
    exit 1
endif


#
# create indexes on production SNP_Accession
#
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Create indexes on ${PRODSNP_DBSERVER}..${PRODSNP_DBNAME}..${ACC_TABLE} table" | tee -a ${LOG}
${PRODSNP_DBSCHEMADIR}/index/${ACC_TABLE}_create.object | tee -a ${LOG}

#
# update statistics on production SNP_Accession
#
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Update statistics on ${PRODSNP_DBSERVER}..${PRODSNP_DBNAME}..${ACC_TABLE} table" | tee -a ${LOG}
${DBUTILSBINDIR}/updateStatistics.csh ${PRODSNP_DBSERVER} ${PRODSNP_DBNAME} ${ACC_TABLE} | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
