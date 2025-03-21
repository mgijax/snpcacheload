import sys 
import os
import db

db.setTrace(True)

# list of chromosomes to process
chrList = [
'1','2','3','4','5','6','7','8','9','10',
'11','12','13','14','15','16','17','18','19',
'X','Y','MT'
]

print("\ntotal count in Production (zSNP_ConsensusSnp_Marker)")
results = db.sql(''' select count(*) as counter from zSNP_ConsensusSnp_Marker ''', 'auto')
print(results[0]['counter'])
sys.stdout.flush()

for chr in chrList:
    results = db.sql('''
    select m.chromosome, count(*)
    from MRK_Marker m, zSNP_ConsensusSnp_Marker s
    where s._marker_key = m._marker_key
    and m.chromosome = '%s'
    group by m.chromosome
    ''' % (chr), 'auto')
    print(results)
sys.stdout.flush()

print("\ntotal count in new set (SNP_ConsensusSnp_Marker)")
results = db.sql(''' select count(*) as counter from SNP_ConsensusSnp_Marker ''', 'auto')
print(results[0]['counter'])
sys.stdout.flush()

print("counts in new set (SNP_ConsensusSnp_Marker)")
for chr in chrList:
    results = db.sql('''
    select m.chromosome, count(*)
    from MRK_Marker m, SNP_ConsensusSnp_Marker s
    where s._marker_key = m._marker_key
    and m.chromosome = '%s'
    group by m.chromosome
    ''' % (chr), 'auto')
    print(results)
sys.stdout.flush()

print("\n\n")

