#!/bin/sh

#
# This script is a wrapper around the process that generates the Alliance feed TSV file.
#     snpalliance.sh 
#
# For each chromosome
#	. copy each Alliance vep/vcf to the /data/loads/mgi/snpcacheload/output folder
#	. sort & uniq the file -> chr.tsv
#
# The TSV files remain static until this is run again.
# This script should be run again if a new Alliance vep/vcf file is mirroed via mirror_wget/alliancegenome.org.variants
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

cd ${CACHEDATADIR}

date >> ${LOG} 2>&1
echo "Process SNP Alliance Feed TSV files"  >> ${LOG} 2>&1
${PYTHON} ${SNPCACHELOAD}/snpalliance.py >> ${LOG} 2>&1
rm -rf *.tsv
for i in `ls snpalliance.output.*`
do
sort ${i} | uniq > ${i}.tsv
rm -rf ${i}
done
date >> ${LOG} 2>&1

echo "Tar the TSV files and copy to proper server" | tee -a ${LOG}
rm -rf ${CACHEDIR}/allianceTSV.tar | tee -a ${LOG}
tar -cvf ../allianceTSV.tar *tsv | tee -a ${LOG}
cd ${CACHEDIR}
if [ "`uname -n | cut -d'.' -f1`" = "bhmgiapp01" ]
then
  SNP_SERVER=bhmgidb03lp.jax.org
else
  SNP_SERVER=bhmgidb05ld.jax.org
fi
echo $SNP_SERVER | tee -a ${LOG}
ls -l ${CACHEDIR}/allianceTSV.tar | tee -a ${LOG}
scp -p ${CACHEDIR}/allianceTSV.tar ${SNP_SERVER}:${CACHEDIR} | tee -a ${LOG}

