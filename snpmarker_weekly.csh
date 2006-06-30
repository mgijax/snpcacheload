#!/bin/csh -fx

#
# Program: snpmarker_weekly.csh
#
# Purpose:
#       1) run snpmarker.csh to create new snp/marker associations in 
#          a 'load' snp database (source snp db)
#       2) create a backup of the source snp db
#       3) load the backup into the destination snp db
#
# Usage:  snpmarker_weekly.csh
#
# History
#
# sc	03/27/2006 - created
# lec	06/30/2006 - modified for mgiconfig

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
# backup back-end snp database
#
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Backing up ${SNPBE_DBSERVER}..${SNPBE_DBNAME}"
${MGI_DBUTILS}/bin/dump_db.csh ${SNPBE_DBSERVER} ${SNPBE_DBNAME} ${SNP_BACKUP} | tee -a ${LOG}

#
# load front-end snp database
# 
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Loading ${SNP_DBSERVER}..${SNP_DBNAME}" 
${MGI_DBUTILS}/bin/load_db.csh ${SNP_DBSERVER} ${SNP_DBNAME} ${PRODSNP_BACKUP} | tee -a ${LOG}

echo "" | tee -a ${LOG}
date | tee -a ${LOG}
