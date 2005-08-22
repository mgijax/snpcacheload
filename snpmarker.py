#!/usr/local/bin/python

'''
#
# Purpose:
#
# Create bcp files for SNP_ConsensusSnp_Marker and 
# ACC_Accession (associate RefSeq id's with SNP_ConsensusSnp_Marker objects)
#
# Uses environment variables to determine Server and Database
# (DSQUERY, MGD, and RADAR_DBNAME).
#
# Usage:
#	snpmarker.py
#
# History
#
# 08/17/2005	sc
#	- SNP (TR 1560)
#
'''

import sys
import os
import db
import mgi_utils
import loadlib

NL = '\n'
DL = '|'
mrkrTable = os.environ['MRKR_TABLE']
accTable = os.environ['ACC_TABLE']
radar = os.environ['RADAR_DBNAME']
userKey = 0
loaddate = loadlib.loaddate
refSeqLdbKey = -1
snpMkrmgiTypeKey = -1
accessionKey = -1
mrkrBCP = open('%s.bcp' % (mrkrTable), 'w')
accBCP = open('%s.bcp' % (accTable), 'w')

def setup():
	global refSeqLdbKey
	global snpMkrmgiTypeKey
	global acckey
	cmds = []
	cmds.append('select _LogicalDB_key ' + \
		'from ACC_LogicalDB ' + \
		'where name = "%s" ' % os.environ['LOGICALDB_NAME'])
	cmds.append('select _MGIType_key ' + \
                'from ACC_MGIType ' + \
                'where name = "%s" ' % os.environ['MGITYPE_NAME'])
	cmds.append('select max(_Accession_key) ' + \
		'from ACC_Accession')
	results = db.sql(cmds, 'auto')
        refSeqLdbKey = results[0][0]['_LogicalDB_key']
	snpMkrmgiTypeKey = results[1][0]['_MGIType_key']
	acckey = results[2][0]['']
	print "ldbkey %s" % refSeqLdbKey
	print "mgiTypeKey %s" % snpMkrmgiTypeKey
	print "accKey %s" % acckey

def createBCP():

	print 'Creating %s.bcp...%s' % (mrkrTable, mgi_utils.date())
	print 'and  %s.bcp...%s' % (accTable, mgi_utils.date())
	print 'Querying'
	cmds = []
	# get subset of MGI_SNP_Marker attributes into a temp table
        cmds.append('select a.accid as rsId, m.entrezGeneId, m.fxnClass, ' + \
	    'm.chromosome, m.startCoord, m.refseqNucleotide, ' + \
	    'm.refseqProtein, m.contig_allele, m.residue, ' + \
	    'm.aa_position, m.reading_frame ' + \
	    'into #radar ' + \
	    'from %s..MGI_SNP_Marker m, %s..MGI_SNP_Accession a ' % (radar, radar) + \
	    'where m._ConsensusSNP_key = a._Object_key ' + \
	    'and a.objectType = "Consensus SNP" ' + \
	    'and a.logicalDB = "RefSNP" ')

	# get _Fxn_key and the _Marker_key
	cmds.append('select r.*, a._Object_key as _Marker_key, ' + \
	    'v._term_key as _Fxn_key ' + \
	    'into #r_mkrfxn ' + \
	    'from #radar r, ACC_Accession a, VOC_Term v '  + \
	    'where r.entrezGeneId = a.accid ' + \
	    'and a._LogicalDB_key = 55 ' + \
	    'and a._MGIType_key = 2 ' + \
	    'and a.preferred = 1 ' + \
	    'and v._Vocab_key = 45 ' + \
	    'and r.fxnClass = v.term')

	# get the _ConsensusSnp_key
	print "Adding _ConsensusSnp_key"
	cmds.append('select r.*, a._Object_key as _ConsensusSnp_key ' + \
	    'into #rmf_cskey ' + \
	    'from #r_mkrfxn r, ACC_Accession a ' + \
	    'where _MGIType_key = 29 ' + \
	    'and preferred = 1 ' + \
	    'and r.rsId = a.accid')

	# get the _Feature_key
	print "Adding _Feature_key"
	cmds.append('select r.*, f._Feature_key ' + \
	    'from #rmf_cskey r, MAP_Coord_Feature f, ' + \
	    'MAP_Coordinate m, MRK_Chromosome c ' + \
	    'where r._ConsensusSnp_key = f._Object_key ' + \
	    'and f._MGIType_key = 29 ' + \
	    'and f._Map_key = m._Map_key ' + \
	    'and m._MGIType_key = 27 ' + \
	    'and m._Object_key = c._Chromosome_key ' + \
	    'and c._Organism_key = 1 ' + \
	    'and r.chromosome = c.chromosome ' + \
	    'and r.startCoord = f.startCoordinate')
        results = db.sql(cmds, 'auto')
	
	print 'Writing bcp file'
	# current primary key
        primaryKey = 0
	for r in results[3]:
	    primaryKey = primaryKey + 1
	    mrkrBCP.write(str(primaryKey) + DL + \
                        str(r['_ConsensusSnp_key']) + DL + \
                        str(r['_Marker_key']) + DL + \
                        str(r['_Fxn_key']) + DL + \
                        str(r['_Feature_key']) + DL + \
                        str(r['contig_allele']) + DL + \
                        str(r['residue']) + DL + \
                        str(r['aa_position']) + DL + \
			str(r['reading_frame']) + DL + \
                        str(userKey) + DL + str(userKey) + DL + \
                        loaddate + DL + loaddate + NL)
	    nuclId = r['refseqNucleotide']
	    protId = r['refseqProtein']
	    if nuclId != None:
		createAccession(nuclId, primaryKey)
	    if protId != None:
		createAccession(protId, primaryKey)
	mrkrBCP.close()
	accBCP.close()

def createAccession(accid, objectKey):
    global acckey
    acckey = acckey + 1
    accBCP.write(str(acckey) + DL + \
	str(accid) + DL + \
	"" + DL + \
	"" + DL + \
	str(refSeqLdbKey) + DL + \
	str(objectKey) + DL + \
	str(snpMkrmgiTypeKey) + DL + \
	str(0) + DL + \
	str(1)+ DL + \
	str(userKey) + DL + str(userKey) + DL + \
	loaddate + DL + loaddate + NL)
#
# Main Routine
#

userKey = loadlib.verifyUser(os.environ['DBUSER'], 1, None)

print '%s' % mgi_utils.date()
setup()
createBCP()
print '%s' % mgi_utils.date()

