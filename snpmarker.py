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
# lec	01/22/2013 - TR10778/conversion to postgres
#	
# sc	04/20/2012 - TR10778 convert to postgres
#
# lec   06/30/2006 - modified for mgiconfig
#
# sc	03/16/2006 - convert to snp database
#
# sc    08/17/2005 - SNP (TR 1560)
#
'''

import sys
import os

# MGI python libraries
import mgi_utils
import accessionlib
import pg_db
db = pg_db

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
server = os.environ['PG_DBSERVER']
database = os.environ['PG_DBNAME']
passwdfile = os.environ['PG_DBPASSWORDFILE']
user = os.environ['PG_DBUSER']

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
    #          setup connection to a database
    #          get SNP_Accession max(_Accession_key)
    # Returns: nothing
    # Assumes: nothing
    # Effects: queries a database

    # turn of tracing statements
    db.setTrace(True)

    password = db.get_sqlPassword()

    print 'connecting to database and loading markerLookup...%s' % NL
    sys.stdout.flush()

    # set up connection to the mgd database
    db.useOneConnection(1)
    db.set_sqlLogin(user, password, server, database)

    # Get postgres output, don't translate to old db.py output
    db.setReturnAsSybase(False)

    # query for all egId to marker associations
    results = db.sql('''SELECT accID AS egId, _Object_key AS _Marker_key 
	FROM ACC_Accession 
        WHERE _LogicalDB_key = %s 
        AND _MGIType_key = %s 
        AND preferred = 1 ''' % (egLdbKey, mrkMgiTypeKey), 'auto' )

    for r in results[1]:
	markerLookup[ r[0] ] = r[1] 
    
    print 'connecting to %s..%s ...%s' % (server, database, NL)
    sys.stdout.flush()

def deleteAccessions():
    # Purpose: delete accession records 
    # Returns: nothing
    # Assumes: nothing
    # Effects: queries a database, deletes records from a database
    # Throws:  db.error, db.connection_exc

    #
    # note that the wrapper deletes non-essential SNP_Accession indexes
    #

    print 'deleting accessions...%s' % NL
    sys.stdout.flush()

    # get the number of total accessions to delete
    results = db.sql('''SELECT COUNT(*) AS cacheCount 
	FROM SNP_Accession a
	WHERE a._MGIType_key = %s 
	AND a._LogicalDB_key = %s''' % (snpMkrMgiTypeKey, refSeqLdbKey), 'auto')
    numToDelete = int(results[1][0][0])
    sys.stdout.flush()

    # commands to accomplish the delete:
    cmds = []
    cmds.append('''CREATE TEMPORARY TABLE todelete
	AS SELECT _Accession_key
	FROM SNP_Accession
	WHERE _MGIType_key = %s
	AND _LogicalDB_key = %s
	LIMIT 1000000''' % (snpMkrMgiTypeKey, refSeqLdbKey))
    cmds.append('CREATE INDEX idx1 on todelete(_Accession_key)')
    cmds.append('''DELETE FROM SNP_Accession a
	USING todelete d
	WHERE d._Accession_key = a._Accession_key''')

    # do the deletes in multiples of 1mill (limit 1000000)
    while numToDelete > 0:
        print 'deleting accessions...%s%s' % (numToDelete, NL)
        sys.stdout.flush()
	db.sql(cmds, None)
	db.commit()
	results = db.sql('SELECT count(*) AS delCount FROM todelete', 'auto')
	sys.stdout.flush()
	numToDelete = numToDelete - 1000000
	db.sql('DROP TABLE todelete', None)
	db.commit()

def getMaxAccessionKey():
    # Purpose: get max(_Accession_key) from a snp database
    # Returns: nothing
    # Assumes: nothing
    # Effects: queries a database
    # Throws:  db.error, db.connection_exc

    # current max(_Accession_key)
    global accKey

    sys.stdout.flush()
    results = db.sql('''SELECT max(_Accession_key) as maxKey FROM SNP_Accession''', 'auto')
    accKey = results[1][0][0]

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

    # get set of DP_SNP_Marker attributes into a temp table
    db.sql('''SELECT a.accID AS rsId,
		a._Object_key AS _ConsensusSnp_key, 
		m.entrezGeneId AS egId, m._Fxn_key, 
		m.chromosome, m.startCoord, m.refseqNucleotide,
		m.refseqProtein, m.contig_allele, m.residue,
		m.aa_position, m.reading_frame
	INTO TEMPORARY TABLE snpmkr1 
	FROM DP_SNP_Marker m, SNP_Accession a 
	WHERE m.accID  = SUBSTRING(a.accid, 3, 15) 
	AND a._MGIType_key = %s 
	AND a._LogicalDB_key = %s''' % (csMgiTypeKey, csLdbKey), None)

    results = db.sql('''select count(*) as tmpCt
	from snpmkr1''', 'auto')
    sys.stdout.flush()

    # create indexes
    db.sql('CREATE INDEX idx1 ON snpmkr1(_ConsensusSnp_key)', None)
    db.sql('CREATE INDEX idx2 ON snpmkr1(chromosome)', None)
    db.sql('CREATE INDEX idx3 ON snpmkr1(startCoord)', None)

    # get the _Coord_Cache_key
    results = db.sql('''SELECT r.*, c._Coord_Cache_key 
	FROM snpmkr1 r, SNP_Coord_Cache c 
	WHERE r._ConsensusSnp_key = c._ConsensusSnp_key 
	AND r.chromosome = c.chromosome 
	AND r.startCoord = c.startCoordinate''', 'auto')
    
    print 'writing bcp file ...%s' % NL
    sys.stdout.flush()

    # current primary key
    primaryKey = 0
    sys.stdout.flush()
  
    for r in results[1]:
        egId = r[2]

	#
	# if egId is not associated with an MGI marker, skip it  
	#
        if not markerLookup.has_key(egId):
	    continue

	#
	# get the marker key for 'egId' and write a line to the bcp file
	# 
        markerKey = markerLookup[ egId ]
	primaryKey = primaryKey + 1

	allele = r[8]
	if allele == None:
	    allele = ""

	residue = r[9]
	if residue == None:
	    residue = ""

	aa_pos = r[10]
	if aa_pos == None:
	    aa_pos = ""

	r_frame = r[11]
	if r_frame == None:
	    r_frame = ""

	mrkrBCP.write(str(primaryKey) + DL + \
	    str(r[1]) + DL + \
	    str(markerKey) + DL + \
	    str(r[3]) + DL + \
	    str(r[12]) + DL + \
	    str(allele) + DL + \
	    str(residue) + DL + \
	    str(aa_pos) + DL + \
	    str(r_frame) + NL)

	nuclId = r[6]
	protId = r[7]

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
    
    #
    #  Close the bcp files.
    #
    db.useOneConnection(0)
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

