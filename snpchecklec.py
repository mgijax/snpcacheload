import sys 
import os
import db

db.setTrace(True)

results = db.sql('''
select m.chromosome, a.accid, count(_consensussnp_marker_key)
from MRK_Marker m, SNP_ConsensusSnp_Marker s, SNP_Accession a
where s._marker_key = m._marker_key
and m.chromosome = 'Y'
and s._consensussnp_key = a._object_key
and a._mgitype_key = 30
group by 1,2
having count(_consensussnp_marker_key) > 1
''', 'auto')
for r in results:
    print(r)
print("(%s rows)" % str(len(results)))
sys.stdout.flush()

results = db.sql('''
select m.chromosome, a.accid, count(_consensussnp_marker_key)
from MRK_Marker m, zSNP_ConsensusSnp_Marker s, SNP_Accession a
where s._marker_key = m._marker_key
and m.chromosome = 'Y'
and s._consensussnp_key = a._object_key
and a._mgitype_key = 30
group by 1,2
having count(_consensussnp_marker_key) > 1
''', 'auto')
for r in results:
    print(r)
print("(%s rows)" % str(len(results)))
sys.stdout.flush()

print("\n\n")

