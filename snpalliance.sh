#!/bin/sh

#
# This script is a wrapper around the process that generates the Alliance feed output file.
#     snpalliance.sh 
#
# For each chromosome
#	. copy & unzip each Alliance vep/vcf to the /data/loads/mgi/snpcacheload/output folder
#	. sort & uniq the file -> chr.tsv
#
# Run the snpmrkalliance.py (which is a symlink to snpmrkwithin.py
# This will generate 1 SNP_ConsensusSnp_Marker_Alliance.bcp.${chr} per chromosome
# The Alliance bcps will remain static until the next time this script is run
# Truncate SNP_ConsensusSnp_Marker_Alliance
# Drop index and primary key
# Load the Alliance bcps into SNP_ConsensusSnp_Marker_Alliance
# Create index and primary key
#
# select max(_consensussnp_marker_key) from SNP_ConsensusSnp_Marker
# Configuration:SNP_ALLIANCE_LAST_KEY
# set to the last _consensussnp_marker_key
# the snpmarker.csh must delete all records *after* the Alliance SNPs
#

cd `dirname $0` 

COMMON_CONFIG=Configuration

#
# Make sure the common configuration file exists and source it. 
#
if [ -f ${COMMON_CONFIG} ]
then
    . ${COMMON_CONFIG}
else
    echo "Missing configuration file: ${COMMON_CONFIG}"
    exit 1
fi

#
# Initialize the log file.
# open LOG in append mode and redirect stdout
#
LOG=${SNP_ALLIANCE_LOG}
rm -rf ${LOG}
>>${LOG}

# duplicate index ; can be removed after the first SNP Alliance run
cat - <<EOSQL | ${PG_DBUTILS}/bin/doisql.csh $0 | tee -a $LOG
drop index if exists snp.SNP_ConsensusSnp_Marker_idx_consensussnp_key;
EOSQL

date >> ${LOG} 2>&1
echo "Process SNP Alliance Feed TSV files"  >> ${LOG} 2>&1
${PYTHON} ${SNPCACHELOAD}/snpalliance.py >> ${LOG} 2>&1
cd ${CACHEDATADIR}
rm -rf *.tsv
for i in `ls snpalliance.output.*`
do
sort ${i} | uniq > ${i}.tsv
rm -rf ${i}
done
date >> ${LOG} 2>&1

echo "Create the Alliance bcp files"  | tee -a ${LOG}
${PYTHON} ${SNPCACHELOAD}/snpmrkalliance.py >> ${LOG} 2>&1
date >> ${LOG} 2>&1

