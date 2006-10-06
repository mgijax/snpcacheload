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
# sc    07/27/2006 - converted to bourne shell
# lec   06/30/2006 - modified for mgiconfig
# sc    03/2006 - convert to snp database add load of snp MRK_Location_Cache
# sc    01/2006 - process multiple snpmrkwithin.bcp files
# dbm   09/28/2005 - Added snpmrklocus.py & snpmrkwithin.py
# sc	08/17/2005 - created

#
# Establish bcp file delimiters
# 

# bcp file row delimiter
NL="\n"

# bcp file column delimiter
DL="|"

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

#
#  Source the DLA library functions.
#
if [ "${DLAJOBSTREAMFUNC}" != "" ]
then
    if [ -r ${DLAJOBSTREAMFUNC} ]
    then
        . ${DLAJOBSTREAMFUNC}
    else
        echo "Cannot source DLA functions script: ${DLAJOBSTREAMFUNC}" | tee -a ${LOG}
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

date | tee -a ${LOG}

#
#   Establish the load log now that we have archived/cleaned
#
LOAD_LOG=${CACHELOGSDIR}/`basename $0`.log
date | tee -a ${LOAD_LOG}

cd ${CACHEDATADIR}

#
# Allow bcp into database
#
date | tee -a ${LOAD_LOG}
echo "Allow bcp into database" | tee -a ${LOAD_LOG}
${MGI_DBUTILS}/bin/turnonbulkcopy.csh ${SNPBE_DBSERVER} ${SNPBE_DBNAME} | tee -a ${LOAD_LOG}

#
# Load dbSNP marker relationships
#

# create bcp file
echo "Calling snpmarker.py" | tee -a ${LOAD_LOG}
${SNPCACHELOAD}/snpmarker.py | tee -a ${LOAD_LOG}
STAT=$?
if [ ${STAT} -ne 0 ]
then
    echo "snpmarker.py failed" | tee -a ${LOAD_LOG}
    exit 1
fi

# SNP_MRK_TABLE truncate, drop indexes, bcp in, create indexes
date | tee -a ${LOAD_LOG}
echo "bcp in  ${SNP_MRK_TABLE}" | tee -a ${LOAD_LOG}
echo "" | tee -a ${LOAD_LOG}
${MGI_DBUTILS}/bin/bcpin_withTruncateDropIndex.csh ${SNPBE_DBSCHEMADIR} ${SNPBE_DBSERVER} ${SNPBE_DBNAME} ${SNP_MRK_TABLE} ${CACHEDATADIR} ${SNP_MRK_FILE} ${DL} ${NL}
STAT=$?
if [ ${STAT} -ne 0 ]
then
    echo "${MGI_DBUTILS}/bin/bcpin_withTruncateDropIndex.csh failed" | tee -a ${LOAD_LOG}
    exit 1
fi

# SNP_MRK_TABLE update statistics
echo "" | tee -a ${LOAD_LOG}
date | tee -a ${LOAD_LOG}
echo "Update statistics on ${SNP_MRK_TABLE} table" | tee -a ${LOAD_LOG}
${MGI_DBUTILS}/bin/updateStatistics.csh ${SNPBE_DBSERVER} ${SNPBE_DBNAME} ${SNP_MRK_TABLE} | tee -a ${LOAD_LOG}

#
# bcp in ACC_TABLE, dropping/recreating indexes
#

# ACC_TABLE drop indexes
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Drop indexes on ${ACC_TABLE} table" | tee -a ${LOG}
${SNPBE_DBSCHEMADIR}/index/${ACC_TABLE}_drop.object | tee -a ${LOG}

# ACC_TABLE bcp in
date | tee -a ${LOAD_LOG}
echo "bcp in  ${ACC_TABLE} " | tee -a ${LOAD_LOG}
echo "" | tee -a ${LOAD_LOG}
${MGI_DBUTILS}/bin/bcpin.csh ${SNPBE_DBSERVER} ${SNPBE_DBNAME} ${ACC_TABLE} ${CACHEDATADIR} ${ACC_FILE} ${DL} ${NL}
STAT=$?
if [ ${STAT} -ne 0 ]
then
    echo "${MGI_DBUTILS}/bin/bcpin.csh failed" | tee -a ${LOAD_LOG}
    exit 1
fi

# ACC_TABLE create indexes
echo "" | tee -a ${LOG}
date | tee -a ${LOG}
echo "Create indexes on ${ACC_TABLE} table" | tee -a ${LOG}
${SNPBE_DBSCHEMADIR}/index/${ACC_TABLE}_create.object | tee -a ${LOG}

# ACC_TABLE update statistics
echo "" | tee -a ${LOAD_LOG}
date | tee -a ${LOAD_LOG}
echo "Update statistics on ${ACC_TABLE} table" | tee -a ${LOAD_LOG}
${MGI_DBUTILS}/bin/updateStatistics.csh ${SNPBE_DBSERVER} ${SNPBE_DBNAME} ${ACC_TABLE} | tee -a ${LOAD_LOG}

#
# Load MGI snp/marker distance relationships
#
# Only run the following steps if the dbSNP and MGI coordinates are
# synchronized (same mouse genome build).
#
if [ ${IN_SYNC} = "yes" ] 
then

    # 
    # load snp..MRK_Location_Cache
    #

    # bcp out MRKLOC_CACHETABLE	 
    echo "" | tee -a ${LOAD_LOG}
    date | tee -a ${LOAD_LOG}
    echo "bcp out ${MRKLOC_CACHETABLE}" | tee -a ${LOAD_LOG}
    ${MGI_DBUTILS}/bin/bcpout.csh ${MGD_DBSERVER} ${MGD_DBNAME} ${MRKLOC_CACHETABLE} ${CACHEDATADIR} ${MRKLOC_CACHEFILE} ${DL} ${NL} | tee -a ${LOAD_LOG}
    STAT=$?
    if [ ${STAT} -ne 0 ]
    then
	echo "${MGI_DBUTILS}/bin/bcpout.csh failed" | tee -a ${LOAD_LOG}
	exit 1
    fi

    # bcp in MRKLOC_CACHETABLE, truncating and dropping/recreating indexes
    echo "" | tee -a ${LOAD_LOG}
    date | tee -a ${LOAD_LOG}
    echo "bcp in MRK_Location_Cache" | tee -a ${LOAD_LOG}
    ${MGI_DBUTILS}/bin/bcpin_withTruncateDropIndex.csh ${SNPBE_DBSCHEMADIR} ${SNPBE_DBSERVER} ${SNPBE_DBNAME} ${MRKLOC_CACHETABLE} ${CACHEDATADIR} ${MRKLOC_CACHEFILE} ${DL} ${NL}
    STAT=$?
    if [ ${STAT} -ne 0 ]
    then
        echo "${MGI_DBUTILS}/bin/bcpin_withTruncatDropIndex.csh failed" | tee -a ${LOAD_LOG}
        exit 1
    fi

    # update statistics
    echo "updating statistics on ${MRKLOC_CACHETABLE}" | tee -a  ${LOAD_LOG}
    ${MGI_DBUTILS}/bin/updateStatistics.csh ${SNPBE_DBSERVER} ${SNPBE_DBNAME} ${MRKLOC_CACHETABLE} | tee -a ${LOAD_LOG}

    #
    # Update dbSNP locus-region function class to upstream/downstream
    #

    echo "" | tee -a ${LOAD_LOG} 
    date | tee -a ${LOAD_LOG}
    echo "Calling snpmrklocus.py" | tee -a ${LOAD_LOG}
    ${SNPCACHELOAD}/snpmrklocus.py | tee -a ${LOAD_LOG}
    STAT=$?
    if [ ${STAT} -ne 0 ]
    then
        echo "${SNPCACHELOAD}/snpmrklocus.py failed" | tee -a ${LOAD_LOG}
        exit 1
    fi

    #
    # load MGI snp to marker relationships
    # 

    # create the bcp file(s)
    echo "" | tee -a ${LOAD_LOG}
    date | tee -a ${LOAD_LOG}
    echo "Calling snpmrkwithin.py" | tee -a ${LOAD_LOG}
    ${SNPCACHELOAD}/snpmrkwithin.py | tee -a ${LOAD_LOG}
    STAT=$?
    if [ ${STAT} -ne 0 ]
    then
        echo "${SNPCACHELOAD}/snpmrkwithin.py failed" | tee -a ${LOAD_LOG}
        exit 1
    fi

    # SNP_MRK_TABLE drop indexes
    echo "" | tee -a ${LOG}
    date | tee -a ${LOG}
    echo "Drop indexes on ${SNP_MRK_TABLE} table" | tee -a ${LOG}
    ${SNPBE_DBSCHEMADIR}/index/${SNP_MRK_TABLE}_drop.object | tee -a ${LOG}
    echo "" | tee -a ${LOG}

    # SNP_MRK_TABLE bcp in each file
    cd ${CACHEDATADIR}
    for i in `ls ${SNP_MRK_WITHIN_FILE}*`
    do
	date | tee -a ${LOAD_LOG}
	echo "Load ${i} into ${SNP_MRK_TABLE} table" | tee -a ${LOAD_LOG}
	echo "" | tee -a ${LOAD_LOG}
	${MGI_DBUTILS}/bin/bcpin.csh ${SNPBE_DBSERVER} ${SNPBE_DBNAME} ${SNP_MRK_TABLE} ${CACHEDATADIR} ${i} ${DL} ${NL}
	STAT=$?
	if [ ${STAT} -ne 0 ]
	then
	    echo "${MGI_DBUTILS}/bin/bcpin.csh failed" | tee -a ${LOAD_LOG}
	    exit 1
	fi
    done

    # SNP_MRK_TABLE create indexes
    echo "" | tee -a ${LOG}
    date | tee -a ${LOG}
    echo "Create indexes on ${SNP_MRK_TABLE} table" | tee -a ${LOG}
    ${SNPBE_DBSCHEMADIR}/index/${SNP_MRK_TABLE}_create.object | tee -a ${LOG}

    # SNP_MRK_TABLE update statistics
    echo "" | tee -a ${LOAD_LOG}
    date | tee -a ${LOAD_LOG}
    echo "Update statistics on ${SNP_MRK_TABLE} table" | tee -a ${LOAD_LOG}
    ${MGI_DBUTILS}/bin/updateStatistics.csh ${SNPBE_DBSERVER} ${SNPBE_DBNAME} ${SNP_MRK_TABLE} | tee -a ${LOAD_LOG}

fi

echo "" | tee -a ${LOAD_LOG}
date | tee -a ${LOAD_LOG}
