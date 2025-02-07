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
 
$PYTHON snpcheck.py | tee -a $LOG

cat - <<EOSQL | ${PG_DBUTILS}/bin/doisql.csh $0 | tee -a $LOG

select m.chromosome, m.symbol, ma.accid, a.accid, m._marker_key, s._consensussnp_key
from MRK_Marker m, zSNP_ConsensusSnp_Marker s, SNP_Accession a, ACC_Accession ma
where s._marker_key = m._marker_key
and s._consensussnp_key = a._object_key
and a._mgitype_key = 30
and m._marker_key = ma._object_key
and ma._mgitype_key = 2
and ma._logicaldb_key = 1
and ma.preferred = 1
and not exists (select 1 from SNP_ConsensusSnp_Marker ss
    where s._consensussnp_key = ss._consensussnp_key
    and s._marker_key = ss._marker_key
    )
;

select m.chromosome, m.symbol, ma.accid, a.accid, m._marker_key, s._consensussnp_key
from MRK_Marker m, SNP_ConsensusSnp_Marker s, SNP_Accession a, ACC_Accession ma
where s._marker_key = m._marker_key
and s._consensussnp_key = a._object_key
and a._mgitype_key = 30
and m._marker_key = ma._object_key
and ma._mgitype_key = 2
and ma._logicaldb_key = 1
and ma.preferred = 1
and not exists (select 1 from zSNP_ConsensusSnp_Marker ss
    where s._consensussnp_key = ss._consensussnp_key
    and s._marker_key = ss._marker_key
    )
;

select m.chromosome, m.symbol, ma.accid, a.accid, m._marker_key, s._consensussnp_key
from MRK_Marker m, SNP_ConsensusSnp_Marker s, SNP_Accession a, ACC_Accession ma
where s._marker_key = m._marker_key
and s._consensussnp_key = a._object_key
and a._mgitype_key = 30
and m._marker_key = ma._object_key
and ma._mgitype_key = 2
and ma._logicaldb_key = 1
and ma.preferred = 1
and not exists (select 1 from zSNP_ConsensusSnp_Marker ss
    where s._consensussnp_key = ss._consensussnp_key
    )
;

EOSQL


date |tee -a $LOG

