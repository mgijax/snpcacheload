#!/usr/local/bin/python

#  snpmrkwithin.py
###########################################################################
#
#  Purpose:
#
#      This script will identify all SNP/marker pairs where the SNP is
#      located within 1000 kb of the marker and there is no existing
#      annotation in the SNP_ConsensusSnp_Marker table. A new upstream or
#      downstream annotation is created, depending on the SNP/marker
#      coordinates.
#
#  Usage:
#
#      snpmrkwithin.py
#
#  Env Vars:  None
#
#  Inputs:
#
#      The following tables in the MGD database are used as input:
#
#      1) SNP_ConsensusSnp_Marker
#      2) SNP_Coordinate_Cache
#      3) MRK_Location_Cache
#
#  Outputs:
#
#      A "|" delimited bcp file to load records into the
#      SNP_ConsensusSnp_Marker table.
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
#
#  Assumes:  Nothing
#
#  Notes:  None
#
###########################################################################
#
#  Modification History:
#
#  Date        SE   Change Description
#  ----------  ---  -------------------------------------------------------
#
#  06/30/2006  lec  modified for mgiconfig
#  05/17/2006  sc   add case for null strand (MIT markers, unistsload)
#  04/2006     jak  new algorithm that uses an exclude list
#              sc   updated to use snp db and process more efficiently
#              sc   added query time reporting
#  02/2006     sc   updated to use binary search
#  09/28/2005  DBM  Initial development
#
###########################################################################

import sys
import os
import string
import time
import db
import loadlib

#
# exceptions
#

# error messages written to stdout
SNP_NOT_WITHIN  = 'Warning: SNP %s not within %s +/- bp of marker %s ' + \
		  '- this should never happen'
#
#  CONSTANTS
#

DL = '|'
CRT = '\n'
NULL = ''

FNCT_CLASS_VOCAB = 'SNP Function Class'
WITHIN_COORD_TERM = 'within coordinates of'
WITHIN_KB_TERM = 'within %s kb %s of'

MARKER_PAD      = 1000000	# max number of BP away a SNP can be from a
				#  marker to compute a SNP-marker association

# max number of SNPs in chr region to process at at time
MAX_NUMBER_SNPS = string.atoi(os.environ['MAX_QUERY_BATCH'])

# max number of lines per bcp file to avoid file > 2Gb
MAX_BCP_LINES = string.atoi(os.environ['MAX_BCP_LINES'])

#
# globals
#

# number suffix for the current bcp file name
snpMrkFileCtr = 0
 
# current number of lines in the current bcp file
bcpLines = 0

# bcp file name prefix
snpMrkFile = None

# file pointer for the bcp file
fpSnpMrk = None

# lookup to resolve function class string to key
fxnLookup = {}

# list of chromosomes to process
chrList = []

# next available _SNP_ConsensusSnp_Marker_key
primaryKey = None

#
#  FUNCTIONS
#

# Purpose: Perform initialization for the script.
# Returns: Nothing
# Assumes: Nothing
# Effects: Queries databases
# Throws: Nothing

def initialize():
    #
    # The following globals will be initialized
    #
    global fxnLookup   # create lookup to resolve function class string to key
    global chrList     # create list of chromosomes to process
    global primaryKey  # get next available _SNP_ConsensusSnp_Marker_key 
    global snpMrkFile  # get bcp file name prefix

    print 'Perform initialization'
    sys.stdout.flush()

    #
    #  Initialize variables.
    #
    dataDir = os.environ['CACHEDATADIR']
    fileName = os.environ['SNP_MRK_WITHIN_FILE']
    snpMrkFile = '%s/%s' % (dataDir, fileName)

    dbServer = os.environ['MGD_DBSERVER']
    dbName = os.environ['MGD_DBNAME']
    dbUser = os.environ['MGD_DBUSER']
    snpDbServer = os.environ['SNPBE_DBSERVER']
    snpDbName = os.environ['SNPBE_DBNAME']
    snpDbUser = os.environ['SNPBE_DBUSER']

    dbPasswordFile = os.environ['SNPBE_DBPASSWORDFILE']
    dbPassword = string.strip(open(dbPasswordFile,'r').readline())

    #
    #  Set up a connection to the mgd database.
    #
    db.useOneConnection(1)
    db.set_sqlLogin(dbUser, dbPassword, dbServer, dbName)

    #
    #  Create a lookup for upstream/downstream function class terms.
    #
    results = db.sql('select t._Term_key, t.term ' + \
                     'from VOC_Vocab v, VOC_Term t ' + \
                     'where v.name = "' + FNCT_CLASS_VOCAB + '" and ' + \
                           'v._Vocab_key = t._Vocab_key and ' + \
                           't.term like "within % of"', 'auto')

    for r in results:
        fxnLookup[r['term']] = r['_Term_key']

    # close connection to the mgd database
    db.useOneConnection(0)

    #
    #  Set up a connection to the snp database.
    #
    db.useOneConnection(1)
    db.set_sqlLogin(snpDbUser, dbPassword, snpDbServer, snpDbName)
 
    #
    #  Create of list of chromosomes.
    #
    results = db.sql('select distinct chromosome from SNP_Coord_Cache', 'auto')

    for r in results:
        chrList.append(r['chromosome'])

    #
    #  Get the max primary key for the SNP_ConsensusSnp_Marker table
    #
    results = db.sql('select max(_ConsensusSnp_Marker_key) + 1 "key" ' + \
                     'from SNP_ConsensusSnp_Marker', 'auto')

    primaryKey = results[0]['key']
    print 'primaryKey: %s' % primaryKey
    openBCPFile()

    return

# Purpose: Creates a new bcp file pointer. Uses a counter to
#          create a unique name
# Returns: Nothing
# Assumes: Nothing
# Effects: Exits if can't open the new bcp file
# Throws: Nothing

def openBCPFile():
    global fpSnpMrk
    global snpMrkFileCtr
    #global snpMrkFile

    # append this to next bcp filename
    snpMrkFileCtr = snpMrkFileCtr + 1
    try:
        fpSnpMrk = open("%s%s" % (snpMrkFile, snpMrkFileCtr),'w')
    except:
        sys.stderr.write('Could not open bcp file: %s\n' % snpMrkFile)
        sys.exit(1)

    return

# Purpose: Perform cleanup steps for the script.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing

def finalize():
    global fpSnpMrk

    db.useOneConnection(0)

    #
    #  Close the bcp file.
    #
    fpSnpMrk.close()

    return



# Purpose: Create a bcp file with annotations for SNP/marker pairs where
#          the SNP is within 1000 kb of the marker and there is no existing
#          annotation for the SNP/marker.
# Returns: Nothing
# Assumes: Nothing
# Effects: Queries a database
#          Outputs to BCP file represented by fpSnpMrk
# Throws:  Nothing

def process():

    #
    #  Process one chromosome at a time to break up the size of the
    #  results set.
    #
    for chr in chrList:
	
	MAX_COORD_QUERY = 'select max(startCoordinate) as maxCoord ' + \
                'from SNP_Coord_Cache ' + \
                'where chromosome = "%s"'

	print '%sQuery for max SNP coordinate on chr %s' % (CRT, chr)
	results = db.sql(MAX_COORD_QUERY % chr, 'auto')
	maxCoord = int(results[0]['maxCoord'])
	print 'Max coord on chr %s %s' % (chr, maxCoord)
	print 'Get SNP/marker pairs for chromosome %s' % chr
	sys.stdout.flush()
	binProcess(chr, 1, maxCoord)
    	sys.stdout.flush()

    return

# Purpose: Process all SNPs within the startCoord-endCoord range on the given
#	   chr - by using binary search to find sub-regions with a small
#	   enough number of SNPs (< MAX_NUMBER_SNPS) to process at a time
#	   "Process" means: Create a bcp file with annotations for SNP/marker
#	   pairs where the SNP is within 1000 kb of the marker and there is
#	   no existing annotation for the SNP/marker.
# Returns: Nothing
# Assumes: startCoord and endCoord are integers
# Effects: Outputs to BCP file represented by fpSnpMrk
# Throws:  Nothing

def binProcess(chr, startCoord, endCoord):

	NUM_SNP_QUERY = 'select count(_ConsensusSnp_key) as snpCount ' + \
                'from SNP_Coord_Cache ' + \
                'where chromosome = "%s"' + \
                'and startCoordinate between %s and %s'

	results = db.sql(NUM_SNP_QUERY % (chr, startCoord, endCoord), 'auto' )
	snpCount = results[0]['snpCount']
	print 'Total snp coordinates on chr %s between coord %s and %s is %s' \
				% (chr, startCoord, endCoord, snpCount)
	sys.stdout.flush()
	if snpCount < MAX_NUMBER_SNPS:
	    processSNPregion(chr, startCoord, endCoord)

	else:
	    print 'snp coord count %s > MAX_NUMBER_SNPS %s, recursing' \
					% (snpCount, MAX_NUMBER_SNPS)
	    midpt = (endCoord + startCoord)/2

	    print 'Calling binProcess(chr %s, startCoord %s, midpt %s)' \
					% (chr, startCoord, midpt)
	    binProcess(chr, startCoord, midpt)

	    print 'Calling binProcess(chr %s, midpt+1 %s, endCoord %s)' \
					% (chr, midpt+1, endCoord)
	    binProcess(chr, midpt + 1, endCoord)

	return

# Purpose: Process all SNPs within the startCoord-endCoord range on the given
#	   chromosome. 
#	   "Process" means: Write to a bcp file annotations for SNP/marker
#	   pairs where the SNP is within MARKER_PAD of the marker and there is
#	   no existing annotation for the SNP/marker.
# Returns: Nothing
# Assumes: Nothing
# Effects: Outputs to BCP file fpSnpMrk
# Throws: Nothing

def processSNPregion(chr, startCoord, endCoord):
	# 
	# Terminology:
	# SNPregion	- the region of the chromosome between 'startCoord'
	#			'endCoord' (passed to this routine)
	#
	# MarkerRegion	- the region of the chr including SNPregion and
	#			MARKER_PAD BP on either side of SNPregion
	#
	# left of, right of - A is "left of" B if in
	#		the chr region we are working on, A's coord is less
	#		than B's (or if A and B are intervals, A's endCoord
	#		is less than B's startCoord.
	#		"right of" is defined similarly.
	# Algorithm Outline:
	# 1) Query Sybase for all the SNPs in the SNPregion, ordered by SNP
	#     location. Call this SNPlist.
	# 2) Query Sybase for all markers in the MarkerRegion.
	#     Call this MarkerList.
	# 3) Query Sybase for the ExcludeList - all SNP-Marker
	#     pairs (in the region) that are already related by a dbSNP
	#     association (we do not output SNP-Marker associations for these)
	# 
	# 4) Compute the "join" between markers and SNPs that are within
	#    MARKER_PAD of each other. We do this here, rather than asking
	#    Sybase to do it as we can do it more efficiently. Here is how:
	# 
	# For each marker in MarkerList # i.e., the typically smaller list
	#     do binary search to find  # i.e., bin search the larger list
	#     the SNP w/ highest coord that is <= marker.endcoord+MARKER_PAD
	#     (any SNPs right of this do not have to be considered for this
	#     marker)
	# 
	#     Starting w/ this SNP, scan backward (left) through the SNPlist
	#     computing SNP-marker relationships for the marker
	#     (using ExcludeList),
	#     until we find a SNP w/ location < marker.startcoord-MARKER_PAD
	#     (any SNPs left of this do not have be considered for this marker)
	# 
	# Done.
	# 
	# Credits: Joel had the idea to use binary search to quickly find the
	# spot in the SNPlist to start computing SNP-marker associations.  He
	# thought this up to handle the more general case of two sets of
	# features (intervals) that you want to compute overlaps between.
	# For the general case (where we would have a set of intervals instead
	# of SNPs w/ a single coordinate), there is more preprocessing needed.
	# 
	# How we handle the ExcludeList:
        # 1) actually stored as a dictionary, see ExcludeDict, below.
        #
        #     We tried using getting the ExcludeList ordered by snp key and
	#     looking up the snps by binary search, but that took longer.
	#
	# Notes:
	# a) since we don't actually store all SNP-marker relationships here,
	#	we can probably increase MAX_NUMBER_SNPS. Either SNPlist or
	#	ExcludeDict will be the biggest data structures here.
	# 
	# The Data Structures:
	#
	# * SNPlist is the list of all Consensus_SNPs that lie in the coord
	#	range - ORDERED BY SNP coord.
	# 	Each SNP on SNPlist is
	#	(_ConsensusSnp_key, _Coord_Cache_key, snpLoc)
	#	- populated by SQL query
	#
	# * Markers is the list of all Markers (w/ coordinates) in MarkerRegion
	# 	Each Marker on Markers is
	#	(_Marker_key, markerStart, markerEnd, markerStrand)
	#	- populated by SQL query
	#	- For now, order is unimportant. Could order by increasing
	#	  endCoordinate, then as we process markers, we could
	#	  increase the start coord of the binary search for SNPs...
	#
	# * ExcludeDict is the dict of (_ConsensusSnp_key, _Marker_key) pairs
	#       for SNPs in the SNPregion that are already associated by
	#	dbSNP associations

	# query to fill SNPlist
	SNPQUERY = 'select sc._ConsensusSnp_key, ' + \
		       'sc._Coord_Cache_key, ' + \
		       'sc.startCoordinate "snpLoc" ' + \
		'from SNP_Coord_Cache sc ' + \
		'where sc.chromosome = "%s" ' + \
		'and sc.startCoordinate between %s and %s ' + \
		'order by sc.startCoordinate'

	print 'SNPlist Query start time: %s' \
		    % time.strftime("%H.%M.%S.%m.%d.%y",  \
		    time.localtime(time.time()))
	sys.stdout.flush()
	SNPlist = db.sql(SNPQUERY % (chr, startCoord, endCoord), 'auto')
	print 'SNPlist Query end time: %s' \
		    % time.strftime("%H.%M.%S.%m.%d.%y",  \
		    time.localtime(time.time()))
	sys.stdout.flush()

	# query to fill Markers
	MARKERQUERY = 'select mc._Marker_key, ' + \
		       'mc.startCoordinate "markerStart", ' + \
		       'mc.endCoordinate "markerEnd", ' + \
		       'mc.strand "markerStrand" ' + \
		'from MRK_Location_Cache mc ' + \
		'where mc.chromosome = "%s" and ' + \
		      'mc.endCoordinate >= %s and ' + \
		      'mc.startCoordinate <= %s '

	print 'Marker Query start time: %s' \
		    % time.strftime("%H.%M.%S.%m.%d.%y",  \
		    time.localtime(time.time()))
	sys.stdout.flush()
	Markers = db.sql(MARKERQUERY \
		% (chr, startCoord-MARKER_PAD, endCoord+MARKER_PAD), 'auto')
	print 'Marker Query end time: %s' \
		    % time.strftime("%H.%M.%S.%m.%d.%y",  \
		    time.localtime(time.time()))
	sys.stdout.flush()

	# query to get ExcludeList
	EXCLUDEQUERY = 'select cm._ConsensusSnp_key, ' + \
		       'cm._Marker_key ' + \
		'from SNP_Coord_Cache sc, ' + \
		     'SNP_ConsensusSnp_Marker cm ' + \
		'where sc.chromosome = "%s" ' + \
		      'and sc.startCoordinate between %s and %s ' + \
		      'and sc._ConsensusSnp_key = cm._ConsensusSnp_key '

	print 'ExcludeList Query start time: %s' \
		    % time.strftime("%H.%M.%S.%m.%d.%y",  \
		    time.localtime(time.time()))
	sys.stdout.flush()

	ExcludeList =db.sql(EXCLUDEQUERY % (chr, startCoord, endCoord), 'auto')

	print 'ExcludeList Query end time: %s' \
		    % time.strftime("%H.%M.%S.%m.%d.%y",  \
		    time.localtime(time.time()))
	sys.stdout.flush()

	ExcludeDict = {}	# empty the exclude list
	for r in ExcludeList:
	    ExcludeDict[(r['_ConsensusSnp_key'],r['_Marker_key'])] = 1

	#
	#  Process each SNP on SNPlist
	#
	print 'Process SNPlist start time: %s' \
                    % time.strftime("%H.%M.%S.%m.%d.%y",  \
                    time.localtime(time.time()))
	sys.stdout.flush()
	idxLastSnp = len(SNPlist)-1	# index of last SNP in SNPlist
	prevSnpIdx = 0			# index of SNP found on prev iteration
					# (start binary search from there)
	for curMarker in Markers:
	    markerKey   = curMarker['_Marker_key']
	    markerStart = curMarker['markerStart']
	    markerEnd   = curMarker['markerEnd']

	    # use binary search to find the index in SNPlist of the
	    #  farthest "right" SNP to consider for this marker
	    snpIdx = listBinarySearch(SNPlist, 'snpLoc', \
		markerEnd+MARKER_PAD, prevSnpIdx, idxLastSnp)
	    # iterate backward through the SNPs from snpIdx and
	    #  process SNP-Marker pairs.
	    #  (deal w/ boundary condition, no SNP is within range?)

	    i = snpIdx
	    leftmostCoord = markerStart-MARKER_PAD
	    while (i >= 0 and SNPlist[i]['snpLoc'] >= leftmostCoord):

		if ( not ExcludeDict.has_key( \
			(SNPlist[i]['_ConsensusSnp_key'], markerKey))):
		    processSNPmarkerPair(SNPlist[i], curMarker)
		i = i-1
	    # prevSnpIdx = snpIdx
	    # end SNP loop
        print 'Process SNPlist end time: %s' \
                    % time.strftime("%H.%M.%S.%m.%d.%y",  \
                    time.localtime(time.time()))	
	sys.stdout.flush()
	return


# Purpose: Process a SNP-marker pair where the SNP and marker are within
#	   MARKER_PAD BP of each other.
#	   "Process" means: compute the appropriate fxn class for the
#	   for the relationship and output the record representing the
#	   relationship to the BCP file.
# Returns: Nothing
# Assumes: fpSnpMrk is an open filepointer to the BCP file.
# Effects: Outputs to BCP file fpSnpMrk
# Throws: Nothing

def processSNPmarkerPair(snp,	  # dictionary w/ keys as above
			 marker): # dictionary w/ keys as above
    # current number of bcp lines written to the current bcp file
    global bcpLines

    # next available _SNP_ConsensusSnp_Marker_key
    global primaryKey

    KB_DISTANCE = [ 2, 10, 100, 500, 1000 ]

    markerStart  = marker['markerStart']
    markerEnd    = marker['markerEnd']
    markerStrand = marker['markerStrand']
    markerKey    = marker['_Marker_key']

    snpLoc = snp['snpLoc']
    snpKey = snp['_ConsensusSnp_key']
    featureKey = snp['_Coord_Cache_key']

    #
    #  The SNP is located within the coordinates of the marker.
    #
    if snpLoc >= markerStart and snpLoc <= markerEnd:
	sys.stdout.flush()
	fxnKey = fxnLookup[WITHIN_COORD_TERM]

    #
    #  The SNP must be located within one of the pre-defined "KB"
    #  distances from the marker. Check each distance (starting
    #  with the small range) to see which one it is.
    #
    else:
	sys.stdout.flush()
	for kbDist in KB_DISTANCE:

	    fxnKey = getKBTerm(snpLoc, markerStart, markerEnd,
			       markerStrand, kbDist) 

	    #
	    #  If the distance has been determined, don't check
	    #  any others.
	    #
	    if fxnKey > 0:
		break
    # if fxnKey can't be determined print msg to stdout
    # so it will be logged and return
    if fxnKey == -1:
        print SNP_NOT_WITHIN % (snp, MARKER_PAD, marker)
	sys.stdout.flush()
	return

    # check the number of bcp lines in the current file, creating
    # new file if >= the configured max
    if bcpLines >= MAX_BCP_LINES:
	fpSnpMrk.close()
	openBCPFile()
	bcpLines = 0

    #
    #  Write a record to the bcp file that annotates the SNP/marker
    #  to the proper function class.
    #
    sys.stdout.flush()
    fpSnpMrk.write(str(primaryKey) + DL + \
		   str(snpKey) + DL + \
		   str(markerKey) + DL + \
		   str(fxnKey) + DL + \
		   str(featureKey) + DL + \
		   NULL + DL + NULL + DL + \
		   NULL + DL + NULL + CRT)

    # increment key 
    primaryKey = primaryKey + 1
    return

# Purpose: Use the SNP/marker coordinates and marker strand to determine
#          if the SNP is within a given "kb" distance from the marker.
#          If it is, the appropriate term is returned for the annotation.
# Returns: The term key or -1 (if the SNP is not within the distance)
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing

def getKBTerm(snpLoc, markerStart, markerEnd, markerStrand, kbDist):

    #
    #  If the SNP is not within the given KB distance from the marker,
    #  don't check any further.
    #
    if snpLoc < (markerStart - (kbDist * 1000)) or \
       snpLoc > (markerEnd + (kbDist * 1000)):
        return -1

    #
    #  Find the midpoint of the marker.
    #
    midPoint = (markerStart + markerEnd) / 2.0

    #
    #  If the SNP coordinate is <= the midpoint of the marker on a
    #  "+" strand, the SNP is considered to be upstream.
    #
    if markerStrand == '+' and snpLoc <= midPoint:
        direction = 'upstream'

    #
    #  If the SNP coordinate is > the midpoint of the marker on a
    #  "+" strand, the SNP is considered to be downstream.
    #
    elif markerStrand == '+' and snpLoc > midPoint:
        direction = 'downstream'

    #
    #  If the SNP coordinate is <= the midpoint of the marker on a
    #  "-" strand, the SNP is considered to be downstream.
    #
    elif markerStrand == '-' and snpLoc <= midPoint:
        direction = 'downstream'

    #
    #  If the SNP coordinate is > the midpoint of the marker on a
    #  "-" strand, the SNP is considered to be upstream.
    #
    elif markerStrand == '-' and snpLoc > midPoint:
        direction = 'upstream'
    #
    #  If the SNP coordinate is <= the midpoint of the marker
    #  and strand is Null (MIT marker), the SNP is considered to be upstream.
    #
    elif markerStrand == None and snpLoc <= midPoint:
        direction = 'upstream'

    #
    #  If the SNP coordinate is > the midpoint of the marker
    #  and strand is Null (MIT marker, the SNP is considered to be downstream.
    #
    elif markerStrand == None and snpLoc > midPoint:
        direction = 'downstream'

    else:
        return -1

    return fxnLookup[WITHIN_KB_TERM % (str(kbDist),direction)]

# Purpose: Do binary search through a list of dictionaries as typically
#          returned from a call to db.sql()
#          The list should be sorted in increasing order on some dict key.
# Returns: Index in the list of a dictionary item whose key = the searchKey.
#	   Or if no dictionary item matches that key,
#	   Returns the max index of the list item whose key is < searchKey.
#	   Returns -1 if searchKey < all dictionary item keys.
# Assumes: list is sorted in increasing order of the keyField
# Effects: Nothing
# Throws: Nothing

def listBinarySearch(list,	# the list to search, sorted by keyField
		     keyField, 	# the name of the dict field of the sort key
		     searchKey, # the value to look for
		     bottomIdx, # lowest index in list[] to search
		     topIdx):	# max index in list[] to search

    found = 0

    while (bottomIdx != topIdx+1 and not found):
	midIdx = (bottomIdx+topIdx)/2		# integer division?
				# check that (0+1)/2 = 0, (3+4)/2 = 3, etc.
				# sc - tested and performs as expected
	listvalue = list[midIdx][keyField]
	if searchKey == listvalue:
	    found = 1
	elif searchKey < listvalue:
	    topIdx = midIdx -1
	else:
	    bottomIdx = midIdx +1
    # end while

    if found:
	return midIdx
    else:
	return topIdx


#
#  MAIN
#
initialize()
process()
finalize()

sys.exit(0)
