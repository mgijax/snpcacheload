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
# Usage:  snpmarker.sh [-a] [-r] [-c] [-l] 
#         where: 
#             -a is an option to archive output directory
#             -r is an option to archive logs directory	
#             -c is an option to clean output directory
#             -l is an option to clean logs directory
#
# History
#
# sc	04/20/2012 
#	- TR10788
#	- updated for postgres, this includes removing
#	   - rm bcp in/out of mgd..MRK_Location_Cache to snp..MRK_Location_Cache
#	   - bulkLoadPostgres.csh instead of bcpin.csh
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
export SCHEMA
#
#  Set up a log file for the shell script in case there is an error
#  during configuration and initialization.
#
cd `dirname $0` 
LOG=`pwd`/snpmarker.log
rm -f ${LOG}

#
#  Verify the argument(s) to the shell script.
#
ARC_OUTPUT=no
ARC_LOGS=no
CLEAN_OUTPUT=no
CLEAN_LOGS=no

usage="Usage: snpmarker.sh [-a] [-r] [-c] [-l]"

#
# report usage if there are unrecognized arguments
#
set -- `getopt arcl $*`
if [ $? != 0 ]
then
    echo ${usage} | tee -a ${LOG}
    exit 1
fi

#
# determine which options on command line
#
for i in $*
do
    case $i in
        -a) ARC_OUTPUT=yes; shift;;
        -r) ARC_LOGS=yes; shift;;
	-c) CLEAN_OUTPUT=yes; shift;;
	-l) CLEAN_LOGS=yes; shift;;
        --) shift; break;;
    esac
done

#
#  Establish the configuration file name, if readable source it
#
CONFIG_LOAD=`pwd`/Configuration
if [ ! -r ${CONFIG_LOAD} ]
then
    echo "Cannot read configuration file: ${CONFIG_LOAD}" | tee -a ${LOG}
    exit 1
fi

. ${CONFIG_LOAD}

# set PGPASSWORD in the environment
PGPASSWORD=`cat ${PGPASSFILE} | grep ${PG_DBUSER} | cut -d':' -f5`

#
#  Source the DLA library functions.
#
if [ "${DLAJOBSTREAMFUNC}" != "" ]
then
    if [ -r ${DLAJOBSTREAMFUNC} ]
    then
        . ${DLAJOBSTREAMFUNC}
    else
        echo "Cannot source DLA functions script: ${DLAJOBSTREAMFUNC}" | tee -a  ${LOG}
        exit 1
    fi
else
    echo "Environment variable DLAJOBSTREAMFUNC has not been defined." | tee -a ${LOG}
    exit 1
fi

# 
# archive/clean the logs/output directories?
#
if [ ${ARC_OUTPUT} = "yes" ]
then
    date | tee -a ${LOG}
    echo "archiving  output directory" | tee -a ${LOG}
    createArchive ${ARCHIVEDIR}/output ${CACHEDATADIR}
    STAT=$?
    if [ ${STAT} -ne 0 ]
    then
        echo "createArchive for output directory failed" | tee -a ${LOG}
        exit 1
    fi
fi

if [ ${CLEAN_OUTPUT} = "yes" ]
then
    date | tee -a ${LOG}
    echo "cleaning  output directory" | tee -a ${LOG}
    cleanDir ${CACHEDATADIR}
    STAT=$?
    if [ ${STAT} -ne 0 ]
    then
        echo "cleanDir for output directory failed" | tee -a ${LOG}
        exit 1
    fi

fi

if [ ${ARC_LOGS} = "yes" ]
then
    date | tee -a ${LOG}
    echo "archiving logs directory" | tee -a ${LOG}
    createArchive ${ARCHIVEDIR}/logs ${CACHELOGSDIR}
    STAT=$?
    if [ ${STAT} -ne 0 ]
    then
        echo "createArchive for logs directory failed" | tee -a ${LOG}
        exit 1
    fi
fi

if [ ${CLEAN_LOGS} = "yes" ]
then
    date | tee -a ${LOG}
    echo "cleaning logs directory" | tee -a ${LOG}
    cleanDir ${CACHELOGSDIR}
    STAT=$?
    if [ ${STAT} -ne 0 ]
    then
        echo "cleanDir for logs directory failed" | tee -a ${LOG}
        exit 1
    fi
fi

#
#   Start using the load log now that we have archived/cleaned
#
date > ${SNPMARKER_LOG}

cd ${CACHEDATADIR}

#
# Load dbSNP marker relationships
#

date | tee -a ${SNPMARKER_LOG}
echo "Calling snpmarker.py" | tee -a ${SNPMARKER_LOG}
${SNPCACHELOAD}/snpmarker.py >> ${SNPMARKER_LOG} 2>&1
STAT=$?
if [ ${STAT} -ne 0 ]
then
    echo "snpmarker.py failed" | tee -a ${SNPMARKER_LOG}
    exit 1
fi

#
# copy in SNP_MRK_TABLE, truncating and dropping/recreating indexes
#

# Note: we can't drop the index of the primary key because it is constraint 
# on the primary key
echo "Truncate and drop indexes on ${SNP_MRK_TABLE}"  | tee -a ${LOG}
${SNP_DBSCHEMADIR}/table/SNP_ConsensusSnp_Marker_truncate.object
${SNP_DBSCHEMADIR}/index/SNP_ConsensusSnp_Marker_drop.object

date | tee -a ${SNPMARKER_LOG}
echo "copy in  ${SNP_MRK_TABLE}" | tee -a ${SNPMARKER_LOG}
echo "" | tee -a ${SNPMARKER_LOG}
${PG_DBUTILS}/bin/bulkLoadPostgres.csh ${PG_DBSERVER} ${PG_DBNAME} ${PG_DBUSER} ${PGPASSWORD} ${CACHEDATADIR}/${SNP_MRK_FILE} ${DL} ${SNP_MRK_TABLE} ${SCHEMA}
STAT=$?
echo "snpmarker.sh exit code from bulkLoadPostres ${STAT}"
if [ ${STAT} -ne 0 ]
then
    echo "bulkLoadPostgres.csh failed" | tee -a ${SNPMARKER_LOG}
    exit 1
fi

date | tee -a ${SNPMARKER_LOG}
echo "Create index on ${SNP_MRK_TABLE}"  | tee -a ${LOG}
echo "" | tee -a ${SNPMARKER_LOG}

${SNP_DBSCHEMADIR}/index/SNP_ConsensusSnp_Marker_create.object

#
# copy in ACC_TABLE, dropping/recreating indexes
#
date | tee -a ${SNPMARKER_LOG}
echo "Drop indexes on ${ACC_TABLE}"  | tee -a ${LOG}
echo "" | tee -a ${SNPMARKER_LOG}
${SNP_DBSCHEMADIR}/index/SNP_Accession_drop.object

date | tee -a ${SNPMARKER_LOG}
echo "copy in  ${ACC_TABLE} " | tee -a ${SNPMARKER_LOG}
echo "" | tee -a ${SNPMARKER_LOG}
${PG_DBUTILS}/bin/bulkLoadPostgres.csh ${PG_DBSERVER} ${PG_DBNAME} ${PG_DBUSER} ${PGPASSWORD} ${CACHEDATADIR}/${ACC_FILE} ${DL} ${ACC_TABLE} ${SCHEMA}
STAT=$?
if [ ${STAT} -ne 0 ]
then
    echo "${PG_DBUTILS}/bin/bcpin.csh failed" | tee -a ${SNPMARKER_LOG}
    exit 1
fi

date | tee -a ${SNPMARKER_LOG}
echo "Create indexes on ${ACC_TABLE} table" 
echo "" | tee -a ${SNPMARKER_LOG}
${SNP_DBSCHEMADIR}/index/SNP_Accession_create.object

#
# Load MGI snp/marker distance relationships
#
# Only run the following steps if the dbSNP and MGI coordinates are
# synchronized (same mouse genome build).
#

# 5/14 still get unexpected end of file even when this value is 'no'
if [ ${IN_SYNC} = "yes" ] 
then

    #
    # Update dbSNP locus-region function class to upstream/downstream
    #

    date | tee -a ${SNPMARKER_LOG}
    echo "Calling snpmrklocus.py" | tee -a ${SNPMARKER_LOG}
    echo "" | tee -a ${SNPMARKER_LOG}
    ${SNPCACHELOAD}/snpmrklocus.py >> ${SNPMARKER_LOG} 2>&1
    STAT=$?
    if [ ${STAT} -ne 0 ]
    then
        echo "${SNPCACHELOAD}/snpmrklocus.py failed" | tee -a ${SNPMARKER_LOG}
        exit 1
    fi

    #
    # load MGI snp to marker relationships
    # 

    date | tee -a ${SNPMARKER_LOG}
    echo "Calling snpmrkwithin.py" | tee -a ${SNPMARKER_LOG}
    echo "" | tee -a ${SNPMARKER_LOG}
    ${SNPCACHELOAD}/snpmrkwithin.py >> ${SNPMARKER_LOG} 2>&1
    STAT=$?
    if [ ${STAT} -ne 0 ]
    then
        echo "${SNPCACHELOAD}/snpmrkwithin.py failed" | tee -a ${SNPMARKER_LOG}
        exit 1
    fi

    date | tee -a ${SNPMARKER_LOG}
    echo "dropping indexes on ${SNP_MRK_TABLE}"  | tee -a ${LOG}
    echo "" | tee -a ${SNPMARKER_LOG}
    ${SNP_DBSCHEMADIR}/index/SNP_ConsensusSnp_Marker_drop.object

    echo "" | tee -a ${LOG}

    # SNP_MRK_TABLE bcp in each file
    cd ${CACHEDATADIR}
    for i in `ls ${SNP_MRK_WITHIN_FILE}*`
    do
	date | tee -a ${SNPMARKER_LOG}
	echo "Load ${i} into ${SNP_MRK_TABLE} table" | tee -a ${SNPMARKER_LOG}
	echo "" | tee -a ${SNPMARKER_LOG}
	${PG_DBUTILS}/bin/bulkLoadPostgres.csh ${PG_DBSERVER} ${PG_DBNAME} ${PG_DBUSER} ${PGPASSWORD} ${CACHEDATADIR}/${i} ${DL} ${SNP_MRK_TABLE} ${SCHEMA}
	STAT=$?
	if [ ${STAT} -ne 0 ]
	then
	    echo "${PG_DBUTILS}/bin/bulkLoadPostgres.csh failed" | tee -a ${SNPMARKER_LOG}
	    exit 1
	fi
    done

    # SNP_MRK_TABLE CREATE indexes
    date | tee -a ${SNPMARKER_LOG}
    echo "Create index on ${SNP_MRK_TABLE}"  | tee -a ${SNPMARKER_LOG}
    echo "" | tee -a ${SNPMARKER_LOG}
    ${SNP_DBSCHEMADIR}/index/SNP_ConsensusSnp_Marker_create.object
fi
echo "" | tee -a ${SNPMARKER_LOG}

