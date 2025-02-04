#
# Input: Alliance Feed
# Output:  SNP_ALLIANCE_TSV, 1 per Chromosome
#
#   SNP ID
#   MGI ID
#   Symbol
#   SNP Term
#   Term key
#   MGD Term
#
# Create SNP Funcation Class lookup (fxdLookup) using the MGI Translation
# For each VCF file (1 per Chromosome)
#   create a corresponding TSV
#
 
import sys 
import os
import gzip
import db

db.setTrace(True)

# list of chromosomes to process
chrList = [
'1','2','3','4','5','6','7','8','9','10',
'11','12','13','14','15','16','17','18','19',
'X','Y','MT'
]

#
# SNP Function Class -> Marker Function Class translator
#
fxnLookup = {}
results = db.sql('''
select t._term_key, t.term, s.badname, a.accid
from voc_term t, mgi_translation s, acc_accession a
where t._vocab_key = 49
and s._translationtype_key = 1014
and s._object_key = t._term_key
and t._term_key = a._object_key
and a._mgitype_key = 13
''', 'auto')
for r in results:
    key = r['badname']
    value = r
    fxnLookup[key] = []
    fxnLookup[key].append(value)
#print(fxnLookup)

for chr in chrList:

    try:
        vep = 'MGI.vep.' + str(chr) + '.vcf.gz'
        inFile = gzip.open(os.environ['SNP_ALLIANCE_INPUT'] + vep, 'rt')
        outFile = open(os.environ['SNP_ALLIANCE_TSV'] + '.' + str(chr), 'w')
    except:
        inFile.close()
        continue

    for line in inFile:

        if line.startswith("##"):
            #print(line)
            continue

        if line.startswith("#"):
            #print(line)
            continue

        #if line.find("MGI:1351639") <= -1:
        #    continue

        columns = line.split('\t')
        rsid = columns[2]

        # split col 7 by ';'
        properties = columns[7].split(';')
        for property in properties:
            # if starts with CSQ
            if property.startswith('CSQ'):
                # split by ',' to find consequence entries
                centries = property.split(',')
                for entry in centries:
                    fields = entry.split('|')
                    if not fields[4].startswith("MGI:") :
                        continue
                    cterms = fields[1].split('&')
                    symbol = fields[3]
                    mgiid = fields[4]
                    for term in cterms:
                        if term in fxnLookup:
                            for t in fxnLookup[term]:
                                outFile.write(rsid + "|" + \
                                    mgiid + "|" + \
                                    symbol + "|" + \
                                    term + "|" + \
                                    str(t['_term_key']) + "|" + \
                                    t['term'] + "\n")

    outFile.close()

inFile.close()
