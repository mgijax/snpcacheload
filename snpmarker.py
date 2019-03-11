#!/usr/local/bin/python

'''
# Program: snpmarker.py
# Purpose: Create bcp file for SNP_ConsensusSnp_Marker 
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
# sc    11/23/2015 - TR11937/dbSNP 142
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
import db

# constants
NL = '\n'
DL = '|'

# database errors
DB_ERROR = 'A database error occured: '
DB_CONNECT_ERROR = 'Connection to the database failed: '

# max number of cs keys - remember some dp_snp_marker CS have upwards of 35
# refseqs for which the record is complete dup except for the refseq
# so, 75000 cs keys on 11/14 returned
# min records: 137537
# max records: 573385
CS_MAX = 75000
distance_from = 0
distance_direction = 'not applicable'

#
# get values from environment
#

# table names and bcp file paths
snpMrkrTable = os.environ['SNP_MRK_TABLE']
snpMrkrFile = os.environ['SNP_MRK_FILE']
accTable = os.environ['ACC_TABLE']
refSeqLdbKey = os.environ['REFSEQ_LOGICALDB_KEY']
snpMkrMgiTypeKey = os.environ['SNPMRKR_MGITYPE_KEY']
csLdbKey = os.environ['CS_LOGICALDB_KEY']
csMgiTypeKey = os.environ['CS_MGITYPE_KEY']
egLdbKey = os.environ['EG_LOGICALDB_KEY']
mrkMgiTypeKey = os.environ['MRKR_MGITYPE_KEY']

# database environment variables
server = os.environ['MGD_DBSERVER']
database = os.environ['MGD_DBNAME']
user = os.environ['MGD_DBUSER']

# marker lookup by entrezgene id
markerLookup = {}

# bcp file writers
mrkrBCP = open(snpMrkrFile, 'w')

# current SNP_ConsensusSnp_Marker primary key
primaryKey = 0

# lookup for refseq pairs
# (transcriptId|proteinId: _Transcript_Protein_key, ...}
# where key is transcriptId + | + proteinId
# or just transcriptId  + | if proteinId is null
refSeqPairDict =  {}

#
# Functions
#

def initialize():
    # Purpose:  setup connection to a database
    # 		create mgd marker lookup
    #           create refseq transcript/protein pair lookup
    # Returns: nothing
    # Assumes: nothing
    # Effects: queries a database

    # turn on tracing statements
    #db.setTrace(True)

    print 'connecting to database and loading markerLookup...%s' % NL
    sys.stdout.flush()

    # set up connection to the mgd database
    db.useOneConnection(1)

    # query for all egId to marker associations
    # exclude: withdrawn markers, marker type QTL and Cytogenetic, feature type heritable phenotypic
    results = db.sql('''SELECT a.accID AS egId, a._Object_key AS _Marker_key 
	FROM ACC_Accession a, MRK_Marker m
        WHERE a._LogicalDB_key = %s 
        AND a._MGIType_key = %s 
        AND a.preferred = 1 
	AND a._Object_key = m._Marker_key
	AND m._Marker_Status_key = 1
	''' % (egLdbKey, mrkMgiTypeKey), 'auto' )

    print 'count of marker/EG records %s\n' % len(results)
    for r in results:
	markerLookup[r['egId']] = r['_Marker_key'] 

    results = db.sql('''select * from  SNP_Transcript_Protein''', 'auto')

    for r in results:
	tableKey = r['_Transcript_Protein_key']
	tId = r['transcriptId']
	pId = r['proteinId']
        if pId == None:
	    pId = ''
	key = '%s|%s' % (tId, pId)
	refSeqPairDict[key] = tableKey

    print 'connected to %s..%s ...%s' % (server, database, NL)
    sys.stdout.flush()

def createBCP():
    # Purpose: creates SNP_ConsensusSnp_Marker bcp file
    # Returns: nothing
    # Assumes: nothing
    # Effects: queries a database, creates files in the filesystem
    # Throws:  db.error, db.connection_exc

    print 'creating %s...%s' % (snpMrkrFile, mgi_utils.date())
    print 'querying ... %s' % NL
    sys.stdout.flush()

    # get set of DP_SNP_Marker attributes into a temp table
    # sc - Looks like this is done with temp tables because DP_SNP_Marker has no index on
    # chromosome or startCoord
    db.sql('''SELECT a.accID AS rsId,
                a._Object_key AS _ConsensusSnp_key,
                m.entrezGeneId AS egId, m._Fxn_key,
                m.chromosome, m.startCoord, m.refseqNucleotide,
                m.refseqProtein, m.contig_allele, m.residue,
                m.aa_position, m.reading_frame
        INTO TEMPORARY TABLE snpmkr
        FROM DP_SNP_Marker m, SNP_Accession a
        WHERE m.accID  = SUBSTRING(a.accid, 3, 15)
        AND a._MGIType_key = %s
	AND a._LogicalDB_key = %s ''' % (csMgiTypeKey, csLdbKey), None)
#	AND a._LogicalDB_key = %s
#         and m.chromosome = '19' ''' % (csMgiTypeKey, csLdbKey), None)

    results = db.sql('''select count(*) as tmpCt from snpmkr''', 'auto')
    totalCt = results[0]['tmpCt']
    print 'totalCt: %s' % totalCt
    sys.stdout.flush()

    # create indexes
    db.sql('CREATE INDEX idx1 ON snpmkr(_ConsensusSnp_key)', None)
    db.sql('CREATE INDEX idx2 ON snpmkr(chromosome)', None)
    db.sql('CREATE INDEX idx3 ON snpmkr(startCoord)', None)

    # get the _Coord_Cache_key; load another temp table, so we can get data
    # in batches
    results = db.sql('''SELECT r.*, c._Coord_Cache_key
	INTO TEMPORARY TABLE snpmkr1
        FROM snpmkr r, SNP_Coord_Cache c
        WHERE r._ConsensusSnp_key = c._ConsensusSnp_key
        AND r.chromosome = c.chromosome
        AND r.startCoord = c.startCoordinate''', 'auto')

    print 'loading csList ...%s' % (mgi_utils.date())
    sys.stdout.flush()
    # csList is an ordered list of distinct cs keys from snpmkr1. We use this 
    # list to batch queries
    csList = []

    results = db.sql('SELECT distinct _ConsensusSnp_key as csKey FROM snpmkr1 order by _ConsensusSnp_key', 'auto')
    for r in results:
	#print r['csKey']
	sys.stdout.flush()
	csList.append(r['csKey'])
    print 'total cs to process (len(csList)): %s %s' % (len(csList), mgi_utils.date())
    print 'Our csKey batch size is: %s' % CS_MAX 
    print 'writing bcp file ...%s' % NL
    sys.stdout.flush()

    # the command which will get a batch of records from the temp table
    cmd = '''select * from snpmkr1
                where _ConsensusSnp_key between %s and %s'''

    # The total number of distinct refSnps with marker data from DP_SNP_Marker
    totalCsCt = len(csList)
    sys.stdout.flush()
    
    # start and end index in csList to get a batch of csKeys
    # a batch of csKeys will return and unknown number of records from snpmkr1
    # because many snps have multiple RefSeqs (and coordinates too)

    startIndex = 0
    endIndex = startIndex + CS_MAX

    # test is '<' because list index starts at 0
    while endIndex < totalCsCt:
	print 'startIndex: %s endIndex: %s %s' % (startIndex, endIndex,  mgi_utils.date())
	sys.stdout.flush()
	# get a batch of csKeys from csList
   	currentList = csList[startIndex:endIndex]

	# get the lowest and highest csKey in the batch, we will query snpmkr1
 	# for all records between these two keys, remember - an unknown number
	# of records will be returned
        startKey = currentList[0]
	endKey = currentList[-1]

	print 'querying for %s consensusSnps using startKey: %s endKey: %s %s' % (len(currentList), startKey, endKey, mgi_utils.date())
	sys.stdout.flush()

	results = db.sql(cmd % (startKey, endKey), 'auto')
	print 'done querying %s' %  mgi_utils.date()
	print '%s records were returned between csKey %s and %s' % (len(results), startKey, endKey)
	sys.stdout.flush()

	writeBCP(results)

	startIndex = endIndex
        endIndex = startIndex + CS_MAX

    # Process the remainder
    if startIndex < totalCsCt:
	print 'startIndex: %s endIndex: %s %s' % (startIndex, endIndex,  mgi_utils.date())
        currentList = csList[startIndex:]
	startKey = currentList[0]
        endKey = currentList[-1]
	print 'querying for %s consensusSnps using startKey: %s endKey: %s %s' % (len(currentList), startKey, endKey, mgi_utils.date())
	sys.stdout.flush()
	results = db.sql(cmd % (startKey, endKey), 'auto')
	print 'done querying %s' %  mgi_utils.date()
        print '%s records were returned between csKey %s and %s' % (len(results), startKey, endKey)
        sys.stdout.flush()

	writeBCP(results)

def writeBCP(results):
    # Purpose: creates SNP_ConsensusSnp_Marker bcp file
    # Returns: nothing
    # Assumes: nothing
    # Effects: creates files in the filesystem
    # Throws:  nothing
    global primaryKey

    for r in results:
	#print r
	# sys.stdout.flush()
	egId = r['egId']
	#print 'egId: %s' % egId
	sys.stdout.flush()
	#
	# if egId is not associated with an MGI marker, skip it  
	#
	if not markerLookup.has_key(egId):
	    print 'egId not associated with MGI marker: %s' % egId
	    continue

	#
	# get the marker key for 'egId' and write a line to the bcp file
	# 
	markerKey = markerLookup[ egId ]
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

        # if we have a refseq nucleotide seqid, find the _Transcript_Protein_key
	nuclId = r['refseqNucleotide']
        protId = r['refseqProtein']
	trKey = None
        if nuclId != None:
            if protId == None:
                protId = ''
            key = '%s|%s' % (nuclId, protId)
            if refSeqPairDict.has_key(key):
                trKey = refSeqPairDict[key]
	    else:
		print 'trKey not in refSeqPairDict: %s' % key

	mrkrBCP.write(str(primaryKey) + DL + \
	    str(r['_ConsensusSnp_key']) + DL + \
	    str(markerKey) + DL + \
	    str(r['_Fxn_key']) + DL + \
	    str(r['_Coord_Cache_key']) + DL + \
	    str(allele) + DL + \
	    str(residue) + DL + \
	    str(aa_pos) + DL + \
	    str(r_frame) + DL + \
	    str(distance_from) + DL + \
	    str(distance_direction) + DL + \
	    str(trKey) + NL)

def finalize():
    # Purpose: Perform cleanup steps for the script.
    # Returns: Nothing
    # Assumes: Nothing
    # Effects: Nothing
    # Throws: Nothing

    global fpSnpMrk
    
    #
    #  Close the bcp files.
    #
    db.useOneConnection(0)
    mrkrBCP.close()
    return

#
# Main Routine
#

print 'snpmarker.py start: %s' % mgi_utils.date()
sys.stdout.flush()
try:
    initialize()
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

