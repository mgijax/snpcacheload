#!/usr/local/bin/python

#  snpmrkwithin.py
###########################################################################
#
#  Purpose:
#
#      This script will identify all SNP/marker pairs where the SNP is
#      located within 10 kb of the marker and there is no existing
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
#  03/07/2019  sc   TR12934/TR12991 - change to within 2KB, do not assign WITHIN_KB to 
#		    Other Genome Feature. Only assign WITHIN_COORD of marker
#  11/23/2015  sc   TR11937/dbSNP 142
#  01/25/2013  lec  TR11248/10788 - conversion to postgres
#  09/01/2011  lec  TR10805/add _Organism_key = 1
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
import loadlib
import db

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

WITHIN_COORD_TERM = 'within coordinates of'
WITHIN_KB_TERM = 'within distance of'

MARKER_PAD      = 2000	# max number of BP away a SNP can be FROM a
			# marker to compute a SNP-marker association

# max number of SNPs in chr region to process at at time
MAX_NUMBER_SNPS = string.atoi(os.environ['MAX_QUERY_BATCH'])

# max number of lines per bcp file to avoid file > 2Gb
MAX_BCP_LINES = string.atoi(os.environ['MAX_BCP_LINES'])

#
# GLOBALS
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

# database environment variables
server = os.environ['MGD_DBSERVER']
database = os.environ['MGD_DBNAME']
user = os.environ['MGD_DBUSER']

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

    password = db.get_sqlPassword()

    #
    #  Set up a connection to the mgd database.
    #
    db.useOneConnection(1)
    db.setReturnAsSybase(False)

    #
    #  Create a lookup for within* function class terms.
    #
    results = db.sql('''
        SELECT t._Term_key, t.term 
	FROM VOC_Term t 
	WHERE t._Vocab_key = 49
	AND t.term LIKE 'within % of' 
	''', 'auto')

    for r in results[1]:
	fxnLookup[r[1]] = r[0]

    #
    #  Create of list of chromosomes.
    #
    results = db.sql('SELECT DISTINCT chromosome FROM SNP_Coord_Cache', 'auto')

    for r in results[1]:
        chrList.append(r[0])
    #chrList.append('19')

    #
    #  Get the max primary key for the SNP_ConsensusSnp_Marker table
    #
    results = db.sql('''SELECT MAX(_ConsensusSnp_Marker_key) as key
                     FROM SNP_ConsensusSnp_Marker''', 'auto')
    primaryKey = results[1][0][0]
    if primaryKey == None:
	sys.stderr.write('SNP_ConsensusSnp_Marker table is empty, load dbSNP Marker associations first')
        sys.exit(1)
    primaryKey += 1
    openBCPFile()

    return

# Purpose: Create a bcp file with annotations for SNP/marker pairs where
#          the SNP is within 2 kb of the marker and there is no existing
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
	
	print '%sQuery for max SNP coordinate on chr %s' % (CRT, chr)
	results = db.sql('''
	        SELECT MAX(startCoordinate) as maxCoord 
                FROM SNP_Coord_Cache 
                WHERE chromosome = '%s' 
		''' % (chr), 'auto')
	maxCoord = (results[1][0][0])
	print 'Max coord on chr %s %s' % (chr, maxCoord)
	print 'Get SNP/marker pairs for chromosome %s' % chr
	sys.stdout.flush()
	binProcess(chr, 1, maxCoord)
    	sys.stdout.flush()

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

# Purpose: Creates a new bcp file pointer. Uses a counter to
#          create a unique name
# Returns: Nothing
# Assumes: Nothing
# Effects: Exits if can't open the new bcp file
# Throws: Nothing

def openBCPFile():
    global fpSnpMrk
    global snpMrkFileCtr

    # append this to next bcp filename
    snpMrkFileCtr = snpMrkFileCtr + 1
    try:
        fpSnpMrk = open("%s%s" % (snpMrkFile, snpMrkFileCtr),'w')
    except:
        sys.stderr.write('Could not open bcp file: %s\n' % snpMrkFile)
        sys.exit(1)

    return

# Purpose: Process all SNPs within the startCoord-endCoord range on the given
#	   chr - by using binary search to find sub-regions with a small
#	   enough number of SNPs (< MAX_NUMBER_SNPS) to process at a time
#	   "Process" means: Create a bcp file with annotations for SNP/marker
#	   pairs where the SNP is within 10 kb of the marker and there is
#	   no existing annotation for the SNP/marker.
# Returns: Nothing
# Assumes: startCoord and endCoord are integers
# Effects: Outputs to BCP file represented by fpSnpMrk
# Throws:  Nothing

def binProcess(chr, startCoord, endCoord):

	results = db.sql('''
	        SELECT COUNT(_ConsensusSnp_key) as snpCount 
                FROM SNP_Coord_Cache 
                WHERE chromosome = '%s'
                AND startCoordinate BETWEEN %s AND %s
		''' % (chr, startCoord, endCoord), 'auto')

	snpCount = results[1][0][0]
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
	# 1) Query Postgres for all the SNPs in the SNPregion, ordered by SNP
	#     location. Call this SNPlist.
	# 2) Query Postgres for all markers in the MarkerRegion.
	#     Call this MarkerList.
	# 3) Query Postgres for the ExcludeList - all SNP-Marker
	#     pairs (in the region) that are already related by a dbSNP
	#     association (we do not output SNP-Marker associations for these)
	# 
	# 4) Compute the "join" between markers and SNPs that are within
	#    MARKER_PAD of each other. We do this here, rather than asking
	#    Postgres to do it as we can do it more efficiently. Here is how:
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

	print 'SNPlist Query start time: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time()))
	sys.stdout.flush()

	# query to fill SNPlist
	SNPs = db.sql('''
	        SELECT sc._ConsensusSnp_key,
		       sc._Coord_Cache_key, 
		       sc.startCoordinate as snpLoc
		FROM SNP_Coord_Cache sc 
		WHERE sc.chromosome = '%s' 
		AND sc.startCoordinate BETWEEN %s AND %s 
		ORDER BY sc.startCoordinate
		''' % (chr, startCoord, endCoord), 'auto')
	SNPlist = SNPs[1]
	print 'SNPlist Query end time: %s' \
		    % time.strftime("%H.%M.%S.%m.%d.%y",  \
		    time.localtime(time.time()))
	sys.stdout.flush()

	print 'Marker Query start time: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time()))
	sys.stdout.flush()

	# query to fill Markers
	# exclude: withdrawn markers, marker type QTL and Cytogenetic, feature type heritable phenotypic
	Markers = db.sql('''
	        SELECT mc._Marker_key, 
		       mc.startCoordinate as markerStart,
		       mc.endCoordinate as markerEnd, 
		       mc.strand as markerStrand, 
		       mc._Marker_Type_key
		FROM MRK_Location_Cache mc, MRK_Marker m, MRK_MCV_Cache mcv
		WHERE mc._Marker_Type_key not in (3, 6) 
		AND mc._Organism_key = 1
		AND mc.genomicchromosome = '%s' 
		AND mc.endCoordinate >= %s 
		AND mc.startCoordinate <= %s
		AND mc._Marker_key = m._Marker_key
		AND m._Marker_Status_key = 1
		AND m._Marker_key = mcv._Marker_key
		AND mcv.qualifier = 'D'
		AND mcv._mcvTerm_key != 6238170
		''' % (chr, startCoord-MARKER_PAD, endCoord+MARKER_PAD), 'auto')

	print 'Marker Query end time: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time()))
	sys.stdout.flush()

	print 'ExcludeList Query start time: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time()))
	sys.stdout.flush()

	# query to get ExcludeList
	ExcludeList = db.sql('''
	        SELECT cm._ConsensusSnp_key, 
		       cm._Marker_key 
		FROM SNP_Coord_Cache sc, SNP_ConsensusSnp_Marker cm 
		WHERE sc.chromosome = '%s'
		AND sc.startCoordinate BETWEEN %s AND %s 
		AND sc._ConsensusSnp_key = cm._ConsensusSnp_key
		''' % (chr, startCoord, endCoord), 'auto')

	print 'ExcludeList Query end time: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time()))
	sys.stdout.flush()

	ExcludeDict = {}	# empty the exclude list
	for r in ExcludeList[1]:
	    ExcludeDict[(r[0],r[1])] = 1

	#
	#  Process each SNP on SNPlist
	#
	print 'Process SNPlist start time: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time()))
	sys.stdout.flush()
	idxLastSnp = len(SNPlist)-1	# index of last SNP in SNPlist
	prevSnpIdx = 0			# index of SNP found on prev iteration
					# (start binary search from there)
	for curMarker in Markers[1]:
	    markerKey   = curMarker[0]
	    markerStart = curMarker[1]
	    markerEnd   = curMarker[2]

	    # use binary search to find the index in SNPlist of the
	    #  farthest "right" SNP to consider for this marker
	    snpIdx = listBinarySearch(SNPlist, 2, \
		markerEnd+MARKER_PAD, prevSnpIdx, idxLastSnp)
	    # iterate backward through the SNPs from snpIdx and
	    #  process SNP-Marker pairs.
	    #  (deal w/ boundary condition, no SNP is within range?)

	    i = snpIdx
	    leftmostCoord = markerStart-MARKER_PAD
	    while (i >= 0 and SNPlist[i][2] >= leftmostCoord):

		if ( not ExcludeDict.has_key( \
			(SNPlist[i][0], markerKey))):
		    processSNPmarkerPair(SNPlist[i], curMarker)
		i = i-1
	    # prevSnpIdx = snpIdx
	    # end SNP loop
        print 'Process SNPlist end time: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time()))	
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

    markerKey    = marker[0]
    markerStart  = marker[1]
    markerEnd    = marker[2]
    markerStrand = marker[3]
    markerTypeKey = marker[4]

    snpLoc = snp[2]
    snpKey = snp[0]
    featureKey = snp[1]

    fxnKey = -1
    dirDist = []
    #
    #  The SNP is located within the coordinates of the marker.
    #
    if snpLoc >= markerStart and snpLoc <= markerEnd:
	sys.stdout.flush()
	fxnKey = fxnLookup[WITHIN_COORD_TERM]
	dirDist = ['not applicable', 0]

    # Other Genome Feature - do not want to assign WITHIN_KB_TERM
    elif markerTypeKey == 9:
        return

    #
    #  The SNP must be located within one of the pre-defined "KB"
    #  distances from the marker. Check each distance (starting
    #  with the small range) to see which one it is.
    #
    else:
    	sys.stdout.flush()
        dirDist = getKBTerm(snpLoc, markerStart, markerEnd, markerStrand)

    if dirDist == []:
        print SNP_NOT_WITHIN % (snp, MARKER_PAD, marker)
        sys.sys.stdout.flush()
        return

    # otherwise direction and distance are set. If fxnKey not yet set ([0, 'not applicable']
    # then set it
    else:
        if fxnKey == -1:
            fxnKey = fxnLookup[WITHIN_KB_TERM]
        direction = dirDist[0]
        distance = int(dirDist[1])

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
                   NULL + DL + NULL + DL + \
                   str(distance) + DL + str(direction) + DL + CRT)

    # increment key 
    primaryKey = primaryKey + 1
    return

# Purpose: Use the SNP/marker coordinates and marker strand to determine
#          if the SNP is within a MARKER_PAD distance from the marker.
#          If it is, the appropriate term is returned for the annotation.
# Returns: The term key or -1 (if the SNP is not within the distance)
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing

def getKBTerm(snpLoc, markerStart, markerEnd, markerStrand):

    #
    #  If the SNP is not within MARKER_PAD distance from the marker,
    #  don't check any further.
    #
    if snpLoc < (markerStart - MARKER_PAD) or \
       snpLoc > (markerEnd + MARKER_PAD):
        return []
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
 	distance = markerStart - snpLoc
    #
    #  If the SNP coordinate is > the midpoint of the marker on a
    #  "+" strand, the SNP is considered to be downstream.
    #
    elif markerStrand == '+' and snpLoc > midPoint:
        direction = 'downstream'
	distance = snpLoc - markerEnd
    #
    #  If the SNP coordinate is <= the midpoint of the marker on a
    #  "-" strand, the SNP is considered to be downstream.
    #
    elif markerStrand == '-' and snpLoc <= midPoint:
        direction = 'downstream'
	distance = markerStart - snpLoc
    #
    #  If the SNP coordinate is > the midpoint of the marker on a
    #  "-" strand, the SNP is considered to be upstream.
    #
    elif markerStrand == '-' and snpLoc > midPoint:
        direction = 'upstream'
	distance = snpLoc - markerEnd
    #
    #  If the SNP coordinate is <= the midpoint of the marker
    #  and strand is Null, the SNP is considered to be proximal
    #
    elif markerStrand == None and snpLoc <= midPoint:
        direction = 'proximal'
	distance = markerStart - snpLoc
    #
    #  If the SNP coordinate is > the midpoint of the marker
    #  and strand is Null, the SNP is considered to be downstream.
    #
    elif markerStrand == None and snpLoc > midPoint:
        direction = 'distal'
	distance = snpLoc - markerEnd
    else:
        return []
    dirDistList = [direction, distance]
    return dirDistList

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
