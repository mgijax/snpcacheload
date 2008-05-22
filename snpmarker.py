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
# lec   06/30/2006 - modified for mgiconfig
#
# sc	03/16/2006 - convert to snp database
#
# sc    08/17/2005 - SNP (TR 1560)
#
# lec   06/30/2006 - modified for mgiconfig
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

# table names and bcp file paths
snpMrkrTable = os.environ['SNP_MRK_TABLE']
snpMrkrFile = os.environ['SNP_MRK_FILE']
accTable = os.environ['ACC_TABLE']
accFile = os.environ['ACC_FILE']
refSeqLdbKey = os.environ['REFSEQ_LOGICALDB_KEY']
snpMkrMgiTypeKey = os.environ['SNPMRKR_MGITYPE_KEY']
csLdbKey = os.environ['CS_LOGICALDB_KEY']
csMgiTypeKey = os.environ['CS_MGITYPE_KEY']
egLdbKey = os.environ['EG_LOGICALDB_KEY']
mrkMgiTypeKey = os.environ['MRKR_MGITYPE_KEY']

# database environment variables
snpServer = os.environ['SNPBE_DBSERVER']
snpDB = os.environ['SNPBE_DBNAME']
mgdServer = os.environ['MGD_DBSERVER']
mgdDB = os.environ['MGD_DBNAME']
passwdfile = os.environ['SNPBE_DBPASSWORDFILE']
password = string.strip(open(passwdfile, 'r').readline())
user = os.environ['SNPBE_DBUSER']

# current max(_Accession_key)
accKey = 0

# marker lookup by entrezgene id
markerLookup = {}

# bcp file writers
mrkrBCP = open(snpMrkrFile, 'w')
accBCP = open(accFile, 'w')

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
    sys.stdout.flush()
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
    sys.stdout.flush()
    # set up connection the snp database
    db.useOneConnection(0)
    db.useOneConnection(1)
    db.set_sqlLogin(user, password, snpServer, snpDB)

def deleteAccessions():
    # Purpose: delete accession records 
    # Returns: nothing
    # Assumes: nothing
    # Effects: queries a database, deletes records from a database
    # Throws:  db.error, db.connection_exc

    print 'deleting accessions ...%s' % NL
    sys.stdout.flush()

    # 5/2008 note: dumping the transaction log alone is not enough to
    # prevent log suspend, for some reason looping, in addition to 
    # this single dump transaction seems to do the trick
    db.sql('dump transaction %s with truncate_only' % snpDB, None)

    # get the number of total accessions to delete
    cmds = []
    cmds.append('select count(*) as cacheCount ' + \
        'from SNP_Accession a ' + \
        'where a._MGIType_key = %s ' % snpMkrMgiTypeKey + \
        'and a._LogicalDB_key = %s' % refSeqLdbKey)
    results = db.sql(cmds, 'auto')
    numToDelete = int(results[0][0]['cacheCount'])
    print "total to delete: %s " % numToDelete
    sys.stdout.flush()

    # commands to accomplish the delete:
    cmds = []
    cmds.append('set rowcount 1000000')
    cmds.append('select a._Accession_key ' + \
    'into #todelete ' + \
    'from SNP_Accession a ' + \
    'where a._MGIType_key = %s ' % snpMkrMgiTypeKey + \
    'and a._LogicalDB_key = %s' % refSeqLdbKey)

    cmds.append('create index idx1 on #todelete(_Accession_key)')

    cmds.append('delete SNP_Accession ' + \
    'from #todelete d, SNP_Accession a ' + \
    'where d._Accession_key = a._Accession_key')

    # do the deletes in multiples of 1mill
    while numToDelete > 0:
        db.sql(cmds, None)
        results = db.sql('select count(*) as delCount from #todelete', 'auto')
        print 'Deleted %s' % results[0]['delCount']
	sys.stdout.flush()
        numToDelete = numToDelete - 1000000
        db.sql('drop table #todelete', None)

def getMaxAccessionKey():
    # Purpose: get max(_Accession_key) from a snp database
    # Returns: nothing
    # Assumes: nothing
    # Effects: queries a database
    # Throws:  db.error, db.connection_exc

    # current max(_Accession_key)
    global accKey
    print 'getting max snp accession key ...%s' % NL
    sys.stdout.flush()
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
    
    print 'creating %s...%s' % (snpMrkrFile, mgi_utils.date())
    print 'and  %s...%s%s' % (accFile, mgi_utils.date(), NL)
    print 'querying ... %s' % NL
    sys.stdout.flush()

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
    sys.stdout.flush()
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
sys.stdout.flush()
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
sys.stdout.flush()

