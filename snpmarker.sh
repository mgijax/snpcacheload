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
# History
#
# lec	01/21/2013
#	- TR10788/testing on linux server (TR11248 scrum project)
#
# sc	04/20/2012 
#	- TR10788
#	- updated for postgres, this includes removing
#	   - rm bcp in/out of mgd..MRK_Location_Cache to snp..MRK_Location_Cache
#	   - bcpin.csh instead of bcpin.csh
#	   - rm all sybase specific stuff 
# lec	09/01/2011 
#	- TR10805
#	- bcpout/in for MRK_Location_Cache uses default bcp settings
#	  that is, do *not* use NL/DL
#	- make sure the MRK_Location_Cache is synced up with 
#	  mgd..MRK_Location_Cache
#
# sc    07/27/2006 - converted to bourne shell
# lec   06/30/2006 - modified for mgiconfig
# sc    03/2006 - convert to snp database add load of snp MRK_Location_Cache
# sc    01/2006 - process multiple snpmrkwithin.bcp files
# dbm   09/28/2005 - Added snpmrklocus.py & snpmrkwithin.py
# sc	08/17/2005 - created
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

#
# Load MGI snp/marker distance relationships
# 
date | tee -a ${SNPMARKER_LOG}
echo "Calling snpmrkwithin.py to create bcp files" | tee -a ${SNPMARKER_LOG}
${PYTHON} ${SNPCACHELOAD}/snpmrkwithin.py >> ${SNPMARKER_LOG} 2>&1
STAT=$?
if [ ${STAT} -ne 0 ]
then
	echo "${SNPCACHELOAD}/snpmrkwithin.py failed" | tee -a ${SNPMARKER_LOG}
	exit 1
fi

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
echo "Create index on SNP_ConsensusSnp_Marker"  | tee -a ${SNPMARKER_LOG}
${SNP_DBSCHEMADIR}/key/SNP_ConsensusSnp_Marker_create.object >> ${SNPMARKER_LOG} 2>&1
${SNP_DBSCHEMADIR}/index/SNP_ConsensusSnp_Marker_create.object >> ${SNPMARKER_LOG} 2>&1
date | tee -a ${SNPMARKER_LOG}

