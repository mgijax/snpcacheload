#!/usr/local/bin/python

'''
# Program: snpmarker.py
# Purpose: Create bcp file for SNP_ConsensusSnp_Marker 
#          delete SNP_ConsensusSnp_Marker accession records
#          Create bcp file for ACC_Accession 
#	     (associate RefSeq id's with SNP_ConsensusSnp_Marker objects)
#
# Usage:
#	snpmarker.py
#
# Inputs: 1) radar and mgd database
#         2) Configuration (see list below)
# Outputs: 1) log file
#    	   2) bcp files
# History
#
# 08/17/2005	sc
#	- SNP (TR 1560)
#
'''

import sys
import os
import db
import string

# MGI python libraries
import mgi_utils
import loadlib
import accessionlib

# constants
NL = '\n'
DL = '|'

# database errors
DB_ERROR = 'A database error occured: '
DB_CONNECT_ERROR = 'Connection to the database failed: '

#
# get values from environment
#
snpMrkrTable = os.environ['MRKR_TABLE']
refSeqLdb = os.environ['REFSEQ_LOGICALDB_NAME']
snpMrkrMGIType = os.environ['MRKR_MGITYPE_NAME']
accTable = os.environ['ACC_TABLE']

# database environment variables
server = os.environ['DBSERVER']
mgdDB = os.environ['DBNAME']
passwdfile = os.environ['DBPASSWORDFILE']
password = string.strip(open(passwdfile, 'r').readline())
user = os.environ['DBUSER']

# keys we need to resolve
refSeqLdbKey = 0
snpMkrmgiTypeKey = 0
accKey = 0
userKey = 0

radarDB = os.environ['RADAR_DBNAME']

loaddate = loadlib.loaddate

# bcp file writers
mrkrBCP = open('%s.bcp' % (snpMrkrTable), 'w')
accBCP = open('%s.bcp' % (accTable), 'w')

#
# Functions
#

def setup():
    # Purpose: setup connection to the database and query the 
    #          database to resolve keys
    # Returns: nothing
    # Assumes: nothing
    # Effects: queries a database
    # Throws:  db.error, db.connection_exc
    
    global refSeqLdbKey
    global snpMkrmgiTypeKey
    global acckey

    # set up connection the the database
    print "%s, %s, %s, %s" % (server, mgdDB, user, password)
    db.set_sqlLogin(user, password, server, mgdDB)

    # resolve logicalDB and MGIType
    # get the max accession key
    cmds = []
    cmds.append('select _LogicalDB_key ' + \
	'from ACC_LogicalDB ' + \
	'where name = "%s" ' % refSeqLdb)
    cmds.append('select _MGIType_key ' + \
	    'from ACC_MGIType ' + \
	    'where name = "%s" ' % snpMrkrMGIType)
    cmds.append('select max(_Accession_key) ' + \
	    'from ACC_Accession')
    
    results = db.sql(cmds, 'auto')
    refSeqLdbKey = results[0][0]['_LogicalDB_key']
    snpMkrmgiTypeKey = results[1][0]['_MGIType_key']
    acckey = results[2][0]['']

def deleteAccessions():
    # Purpose: delete accession records 
    # Returns: nothing
    # Assumes: nothing
    # Effects: queries a database, deletes records from a database
    # Throws:  db.error, db.connection_exc
    print 'Deleting Accessions'

    cmds = []
    cmds.append('select a._Accession_key ' + \
    'into #todelete ' + \
    'from ACC_Accession a ' + \
    'where a._MGIType_key = %s ' % snpMkrmgiTypeKey + \
    'and a._LogicalDB_key = %s' % refSeqLdbKey)

    cmds.append('create index idx1 on #todelete(_Accession_key)')

    cmds.append('delete ACC_Accession ' + \
    'from #todelete d, ACC_Accession a ' + \
    'where d._Accession_key = a._Accession_key')

    results = db.sql(cmds, 'auto')

def createBCP():
    # Purpose: creates SNP_ConsensusSnp_Marker and ACC_Accession bcp files
    # Returns: nothing
    # Assumes: nothing
    # Effects: queries a database, creates files in the filesystem
    # Throws:  db.error, db.connection_exc

    print 'Creating %s.bcp...%s' % (snpMrkrTable, mgi_utils.date())
    print 'and  %s.bcp...%s' % (accTable, mgi_utils.date())
    print 'Querying'
    cmds = []
    # get subset of MGI_SNP_Marker attributes into a temp table
    cmds.append('select a.accid as rsId, m.entrezGeneId, m.fxnClass, ' + \
	'm.chromosome, m.startCoord, m.refseqNucleotide, ' + \
	'm.refseqProtein, m.contig_allele, m.residue, ' + \
	'm.aa_position, m.reading_frame ' + \
	'into #radar ' + \
	'from %s..MGI_SNP_Marker m, %s..MGI_SNP_Accession a ' % (radarDB, radarDB) + \
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
	'and v._Vocab_key = 49 ' + \
	'and r.fxnClass = v.term '
	'union ' + \
	'select r.*, a._Object_key as _Marker_key, ' + \
        't._Object_key as _Fxn_key ' + \
        'from #radar r, ACC_Accession a, MGI_Translation t ' + \
        'where r.entrezGeneId = a.accid ' + \
        'and a._LogicalDB_key = 55 ' + \
        'and a._MGIType_key = 2 ' + \
        'and a.preferred = 1 ' + \
        'and t._TranslationType_key = 1010 ' + \
        'and r.fxnClass = t.badname')

    # get the _ConsensusSnp_key
    cmds.append('select r.*, a._Object_key as _ConsensusSnp_key ' + \
	'into #rmf_cskey ' + \
	'from #r_mkrfxn r, ACC_Accession a ' + \
	'where a._MGIType_key = 30 ' + \
	'and a.preferred = 1 ' + \
	'and r.rsId  = substring(a.accid, 3, 15)' )

    # get the _Feature_key
    cmds.append('select r.*, f._Feature_key ' + \
	'from #rmf_cskey r, MAP_Coord_Feature f, ' + \
	'MAP_Coordinate m, MRK_Chromosome c ' + \
	'where r._ConsensusSnp_key = f._Object_key ' + \
	'and f._MGIType_key = 30 ' + \
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
	allele = r['contig_allele']
	if allele == None:
	    allele = ""
	residue = r['residue']
	if residue == None:
	    residue = ""
	aa_pos = r['aa_position']
	if aa_pos == None:
	    aa_pos = ""
	r_frame = r['reading_frame']
	if r_frame == None:
	    r_frame = ""
	mrkrBCP.write(str(primaryKey) + DL + \
	    str(r['_ConsensusSnp_key']) + DL + \
	    str(r['_Marker_key']) + DL + \
	    str(r['_Fxn_key']) + DL + \
	    str(r['_Feature_key']) + DL + \
	    str(allele) + DL + \
	    str(residue) + DL + \
	    str(aa_pos) + DL + \
	    str(r_frame) + NL)
	nuclId = r['refseqNucleotide']
	protId = r['refseqProtein']
	if nuclId != None:
	    createAccession(nuclId, primaryKey)
	if protId != None:
	    createAccession(protId, primaryKey)
    mrkrBCP.close()
    accBCP.close()

def createAccession(accid, objectKey):
    # Purpose: creates ACC_Accesssion bcp file
    # Returns: nothing
    # Assumes: nothing
    # Effects: creates a file in the file system
    # Throws:  nothing

    global acckey
    acckey = acckey + 1
    prefixpart, numericpart = accessionlib.split_accnum(accid)
    accBCP.write(str(acckey) + DL + \
	str(accid) + DL + \
	str(prefixpart) + DL + \
	str(numericpart) + DL + \
	str(refSeqLdbKey) + DL + \
	str(objectKey) + DL + \
	str(snpMkrmgiTypeKey) + DL + \
	str(1) + DL + \
	str(1)+ DL + \
	str(userKey) + DL + str(userKey) + DL + \
	loaddate + DL + loaddate + NL)
#
# Main Routine
#

userKey = loadlib.verifyUser(user, 1, None)

print 'snpmarker.py start: %s' % mgi_utils.date()
try:
    setup()
    deleteAccessions()
    createBCP()
except db.connection_exc, message:
    error = '%s%s' % (DB_CONNECT_ERROR, message)
    sys.stderr.write(message)
    sys.exit(message)
except db.error, message:
    error = '%s%s' % (DB_ERROR, message)
    sys.stderr.write(message)
    sys.exit(message)

print 'snpmarker.py end: %s' % mgi_utils.date()

