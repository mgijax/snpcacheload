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
# 03/16/2006 sc convert to snp database
#
'''

import sys
import os
import db
import string

# MGI python libraries
import mgi_utils
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
accTable = os.environ['ACC_TABLE']
refSeqLdbKey = os.environ['REFSEQ_LOGICALDB_KEY']
snpMkrMgiTypeKey = os.environ['SNPMRKR_MGITYPE_KEY']
csLdbKey = os.environ['CS_LOGICALDB_KEY']
csMgiTypeKey = os.environ['CS_MGITYPE_KEY']
egLdbKey = os.environ['EG_LOGICALDB_KEY']
mrkMgiTypeKey = os.environ['MRKR_MGITYPE_KEY']

# database environment variables
snpServer = os.environ['SNP_DBSERVER']
snpDB = os.environ['SNP_DBNAME']
mgdServer = os.environ['MGD_DBSERVER']
mgdDB = os.environ['MGD_DBNAME']
passwdfile = os.environ['SNP_DBPASSWORDFILE']
password = string.strip(open(passwdfile, 'r').readline())
user = os.environ['DBUSER']

# current max(_Accession_key)
accKey = 0

# marker lookup by entrezgene id
markerLookup = {}

# bcp file writers
mrkrBCP = open('%s.bcp' % (snpMrkrTable), 'w')
accBCP = open('%s.bcp' % (accTable), 'w')

#
# Functions
#

def initialize():
    # Purpose: create mgd marker lookup
    #          setup connection to a snp database
    #          get SNP_Accession max(_Accession_key)
    # Returns: nothing
    # Assumes: nothing
    # Effects: queries a database
    # Throws:  db.error, db.connection_exc
    print 'connecting to mgd and loading markerLookup...%s' % NL
    # set up connection to the mgd database
    db.useOneConnection(1)
    db.set_sqlLogin(user, password, mgdServer, mgdDB)

    # query for all egId to marker associations
    cmds = []
    cmds.append('select accID as egId, _Object_key as _Marker_key ' + \
	'from ACC_Accession '  + \
        'where _LogicalDB_key = %s ' % egLdbKey+ \
        'and _MGIType_key = %s ' % mrkMgiTypeKey+ \
        'and preferred = 1')

    results = db.sql(cmds, 'auto')
   
    # load lookup with egId to marker associations
    for r in results[0]:
	markerLookup[ r['egId'] ] = r['_Marker_key'] 
    
    print 'connecting to %s..%s ...%s' % (snpServer, snpDB, NL)
    # set up connection the snp database
    db.useOneConnection(0)
    db.set_sqlLogin(user, password, snpServer, snpDB)

def deleteAccessions():
    # Purpose: delete accession records 
    # Returns: nothing
    # Assumes: nothing
    # Effects: queries a database, deletes records from a database
    # Throws:  db.error, db.connection_exc
    print 'deleting accessions ...%s' % NL

    cmds = []
    cmds.append('select a._Accession_key ' + \
    'into #todelete ' + \
    'from SNP_Accession a ' + \
    'where a._MGIType_key = %s ' % snpMkrMgiTypeKey + \
    'and a._LogicalDB_key = %s' % refSeqLdbKey)

    cmds.append('create index idx1 on #todelete(_Accession_key)')

    cmds.append('delete SNP_Accession ' + \
    'from #todelete d, SNP_Accession a ' + \
    'where d._Accession_key = a._Accession_key')

    results = db.sql(cmds, 'auto')

def getMaxAccessionKey():
    # Purpose: get max(_Accession_key) from a snp database
    # Returns: nothing
    # Assumes: nothing
    # Effects: queries a database
    # Throws:  db.error, db.connection_exc

    # current max(_Accession_key)
    global accKey
    print 'getting max snp accession key ...%s' % NL
    cmds = []
    cmds.append('select max(_Accession_key) ' + \
            'from SNP_Accession')
	
    results = db.sql(cmds, 'auto')
    accKey = results[0][0]['']

def createBCP():
    # Purpose: creates SNP_ConsensusSnp_Marker and SNP_Accession bcp files
    # Returns: nothing
    # Assumes: nothing
    # Effects: queries a database, creates files in the filesystem
    # Throws:  db.error, db.connection_exc
    
    print 'creating %s.bcp...%s' % (snpMrkrTable, mgi_utils.date())
    print 'and  %s.bcp...%s%s' % (accTable, mgi_utils.date(), NL)
    print 'querying ... %s' % NL

    cmds = []
    # get set of DP_SNP_Marker attributes into a temp table
    cmds.append('select a.accID as rsId, ' + \
	'a._Object_key as _ConsensusSnp_key, ' + \
	'm.entrezGeneId as egId, m._Fxn_key, ' + \
	'm.chromosome, m.startCoord, m.refseqNucleotide, ' + \
	'm.refseqProtein, m.contig_allele, m.residue, ' + \
	'm.aa_position, m.reading_frame ' + \
	'into #snpmkr1 ' + \
	'from DP_SNP_Marker m, SNP_Accession a ' + \
	'where m.accID  = substring(a.accid, 3, 15) ' + \
	'and a._MGIType_key = %s ' % csMgiTypeKey  + \
	'and a._logicalDB_key = %s' % csLdbKey)

    # get the _Coord_Cache_key
    cmds.append('select r.*, c._Coord_Cache_key ' + \
	'from #snpmkr1 r, SNP_Coord_Cache c ' + \
	'where r._ConsensusSnp_key = c._ConsensusSnp_key ' + \
	'and r.chromosome = c.chromosome ' + \
	'and r.startCoord = c.startCoordinate')
    results = db.sql(cmds, 'auto')
    
    print 'writing bcp file ...%s' % NL
    # current primary key
    primaryKey = 0
    for r in results[1]:
        egId = r['egId']
	#
	# if egId is not associated with an MGI marker, skip it  
	#
        if not markerLookup.has_key(egId):
	    continue
	#
	# get the marker key for 'egId' and write a line to the bcp file
	# 
        markerKey = markerLookup[ r['egId'] ]
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
	    str(markerKey) + DL + \
	    str(r['_Fxn_key']) + DL + \
	    str(r['_Coord_Cache_key']) + DL + \
	    str(allele) + DL + \
	    str(residue) + DL + \
	    str(aa_pos) + DL + \
	    str(r_frame) + NL)
	nuclId = r['refseqNucleotide']
	protId = r['refseqProtein']
	# if we have a refseq nucleotide seqid, associate it with
        # the current SNP_ConsensusSnp_Marker object
	if nuclId != None:
	    createAccession(nuclId, primaryKey)
	# if we have a refseq protein seqid, associate it with
        # the current SNP_ConsensusSnp_Marker object
	if protId != None:
	    createAccession(protId, primaryKey)

def createAccession(accid, objectKey):
    # Purpose: creates ACC_Accesssion bcp file
    # Returns: nothing
    # Assumes: nothing
    # Effects: creates a file in the file system
    # Throws:  nothing

    global accKey
    accKey = accKey + 1
    prefixpart, numericpart = accessionlib.split_accnum(accid)
    accBCP.write(str(accKey) + DL + \
	str(accid) + DL + \
	str(prefixpart) + DL + \
	str(numericpart) + DL + \
	str(refSeqLdbKey) + DL + \
	str(objectKey) + DL + \
	str(snpMkrMgiTypeKey) + NL)

# Purpose: Perform cleanup steps for the script.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing

def finalize():
    global fpSnpMrk
    
    db.useOneConnection(0)
    #
    #  Close the bcp files.
    #
    mrkrBCP.close()
    accBCP.close()
    return

#
# Main Routine
#

print 'snpmarker.py start: %s' % mgi_utils.date()
try:
    initialize()
    getMaxAccessionKey()
    deleteAccessions()
    createBCP()
    finalize()
except db.connection_exc, message:
    error = '%s%s' % (DB_CONNECT_ERROR, message)
    sys.stderr.write(message)
    sys.exit(message)
except db.error, message:
    error = '%s%s' % (DB_ERROR, message)
    sys.stderr.write(message)
    sys.exit(message)

print 'snpmarker.py end: %s' % mgi_utils.date()

