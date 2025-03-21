#!/bin/csh -f

#
# Template
#


if ( ${?MGICONFIG} == 0 ) then
        setenv MGICONFIG /usr/local/mgi/live/mgiconfig
endif

source ${MGICONFIG}/master.config.csh

cd `dirname $0`

setenv LOG $0.log
rm -rf $LOG
touch $LOG
 
date | tee -a $LOG
 
#$PYTHON snpcheck.py | tee -a $LOG

cat - <<EOSQL | ${PG_DBUTILS}/bin/doisql.csh $0 | tee -a $LOG


select * from SNP_Accession a where a.accid in ('rs37184917');
select * from SNP_ConsensusSnp_Marker where _consensussnp_key = 15026935;

select m.chromosome, m.symbol, ma.accid, a.accid, m._marker_key, s._consensussnp_key
from MRK_Marker m, SNP_ConsensusSnp_Marker s, SNP_Accession a, ACC_Accession ma
where s._marker_key = m._marker_key
and s._consensussnp_key = a._object_key
and a._mgitype_key = 30
and m._marker_key = ma._object_key
and ma._mgitype_key = 2
and ma._logicaldb_key = 1
and ma.preferred = 1
and a.accid in ('rs37184917')
;

EOSQL


date |tee -a $LOG

