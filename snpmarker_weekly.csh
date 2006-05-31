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

#date | tee -a ${LOG}
#echo "Calling snpmarker.csh" | tee -a ${LOG}
#${CACHEINSTALLDIR}/snpmarker.csh
#if ( $status ) then
#     echo "${CACHEINSTALLDIR}/snpmarker.csh failed" | tee -a ${LOG}
#     exit 1
#endif

#
# backup back-end snp database
#
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Backing up ${SNP_DBSERVER}..${SNP_DBNAME}"
${MGIDBUTILSBINDIR}/dump_db.csh ${SNP_DBSERVER} ${SNP_DBNAME} ${SNP_BACKUP} | tee -a ${LOG}

#
# load front-end snp database
# 
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Loading ${PRODSNP_DBSERVER}..${PRODSNP_DBNAME}" 
${MGIDBUTILSBINDIR}/load_db.csh ${PRODSNP_DBSERVER} ${PRODSNP_DBNAME} ${PRODSNP_BACKUP} | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
