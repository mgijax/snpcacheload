#  snpmrkwithin.py
###########################################################################
#
#  Purpose:
#
#  SNP Marker Alliance, Within Coordinates & Distance: snpmrkwithin.py
#  . Truncate SNP_ConsensusSnp_Marker
#  . Find set of SNP keys (SNP_ConsensusSnp)
#  . If SNP id exists in Alliance TSV, then Alliance function classification : may be > 1
#  . Else if SNP is within the coordinates of the marker, then use it
#  . Else use Distance within 2kb of the marker
# 
#  The snpmarkwithin.py is run weekly as part of the Production Pipeline:
#  Jenkins Tasks: http://bhmgijenkins01lp.jax.org:10082/job/Pipeline/job/Step 02 - SNP Cache Load/
# 
#  Usage:
#
#      snpmrkwithin.py
#
#  Inputs:
#
#      The following tables in the MGD database are used as input:
#      1) SNP_ConsensusSnp_Marker : drop/reload
#      2) SNP_Coordinate_Cache
#      3) MRK_Location_Cache
#
#  Outputs:
#
#      "|" delimited bcp files, 1 per chromosome, to load records into the SNP_ConsensusSnp_Marker table.
#
###########################################################################
#
#  Modification History:
#
#  Date        SE   Change Description
#  ----------  ---  -------------------------------------------------------
#  01/27/2025  lec  e4g-127 ; part of Enhancements for GXD Project
#  09/06/2022  sc   WTS2-837 remap snp coordinates (b39) 
#       - not loading dbSNP marker associations, so removed use of excludeDict
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
import time
import loadlib
import db

db.setTrace(True)

DL = '|'
CRT = '\n'

WITHIN_COORD_TERM = 'within coordinates of'
WITHIN_KB_TERM = 'within distance of'
SNP_NOT_WITHIN  = 'Warning: SNP %s not within %s +/- bp of marker %s,s,%s,%s ' + '- this should never happen'

# max number of BP away a SNP can be from a marker to compute a SNP-marker association
MARKER_PAD = 2000	

# SNP write format
snpWrite = '%s|%s|%s|%s|%s|||||%s|%s|\n'

# bcp file name prefix
snpFile = None
# alliance input file name
snpAllianceFile = None

# file pointer for the bcp file
fpSnpBCP = None
# file pointer for the alliance file
fpSnpAlliance = None

# lookup to resolve function class string to key
fxnLookup = {}

# list of chromosomes to process
chrList = [
'1','2','3','4','5','6','7','8','9','10',
'11','12','13','14','15','16','17','18','19',
'X','Y','MT'
]

# next available _SNP_ConsensusSnp_Marker_key
primaryKey = 1

# Alliance Lookup
allianceLookup = {}

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

    print('initialize(): perform initialization')
    sys.stdout.flush()

    #
    #  Create a lookup for within* function class terms
    #
    results = db.sql('''
            select t._term_key, t.term 
            from VOC_Term t 
            where t._Vocab_key = 49
            and t.term LIKE 'within % of' 
            ''', 'auto')
    for r in results:
        fxnLookup[r['term']] = r['_term_key']

    return

# Purpose: For each Chromosome, create a bcp file with annotations for SNP/marker pairs where the SNP is within 2 kb of the marker
# Returns: Nothing
# Assumes: Nothing
# Effects: Queries a database, Outputs to BCP file represented by fpSnpBCP
# Throws:  Nothing

def process():
    #
    #  Process one chromosome at a time to break up the size of the results set.
    #   Create one bcp file per chromosome
    #
    global snpAllianceFile  # get alliance input file
    global fpSnpBCP
    global fpSnpAlliance
    global allianceLookup

    for chr in chrList:

        print('\nprocess(): chromosome: %s' % (chr))

        try:
            print('process(): create read/write files')
            snpAllianceFile = os.environ['SNP_ALLIANCE_TSV'] + '.' + str(chr) + '.tsv'
            fpSnpAlliance = open("%s" % (snpAllianceFile),'r')
            snpFile = os.environ['CACHEDATADIR'] + '/' + os.environ['SNP_MRK_FILE'] + '.' + str(chr)
            fpSnpBCP = open("%s" % (snpFile),'w')
        except:
            sys.stderr.write('Cannot Read SNP Alliance File: %s\n' % snpAllianceFile)
            sys.stderr.write('Cannot Write SNP File: %s\n' % snpFile)
            sys.exit(1)
            
        print('process(): create Alliance lookup')
        allianceLookup = {}
        for line in fpSnpAlliance:
            tokens = line[:-1].split('|')
            key = tokens[0] + ':' + tokens[1]
            if key not in allianceLookup:
                allianceLookup[key] = []
            allianceLookup[key].append(tokens)
        print('process(): Alliance lookup: ' + str(len(allianceLookup)))
        #print(allianceLookup)

        print('process(): query for max SNP coordinate')
        results = db.sql('''
                select max(startCoordinate) as maxCoord 
                from SNP_Coord_Cache 
                where chromosome = '%s' 
                ''' % (chr), 'auto')
        maxCoord = results[0]['maxCoord']
        print('process(): max coord: %s' % (maxCoord))
        sys.stdout.flush()
        binProcess(chr, 1, maxCoord)
        sys.stdout.flush()

        fpSnpAlliance.close()
        fpSnpBCP.close()

    return

# Purpose: Process all SNPs within the startCoord-endCoord range on the given
#	   chr - by using binary search to find sub-regions to process at a time
#	   "Process" means: Create a bcp file with annotations for SNP/marker
#	   pairs where the SNP is within 2 kb of the marker and there is no existing annotation for the SNP/marker.
# Returns: Nothing
# Assumes: startCoord and endCoord are integers
# Effects: Outputs to BCP file represented by fpSnpBCP
# Throws:  Nothing

def binProcess(chr, startCoord, endCoord):
    global SNPlist

    print('binProcess(): SNPlist query start time: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
    sys.stdout.flush()

    # query to fill SNPlist
    SNPlist = db.sql('''
        select sc._ConsensusSnp_key, sc._Coord_Cache_key, sc.startCoordinate, a.accid
        from SNP_Coord_Cache sc, SNP_Accession a
        where sc.chromosome = '%s' 
        and sc.startCoordinate between %s and %s 
        and sc._consensussnp_key = a._object_key
        and a._mgitype_key = 30
        order by sc.startCoordinate
        ''' % (chr, startCoord, endCoord), 'auto')

    print('binProcess(): total snp coordinates between coord %s and %s is %s' % (startCoord, endCoord, str(len(SNPlist))))
    print('binProcess(): SNPlist query end time: %s' % time.strftime("%H.%M.%S.%m.%d.%y",  time.localtime(time.time())))
    sys.stdout.flush()
    processSNPregion(fpSnpBCP, chr, startCoord, endCoord)

# Purpose: Process all SNPs within the startCoord-endCoord range on the given chromosome. 
#	   "Process" means: Write to a bcp file annotations for SNP/marker pairs 
#       where the SNP is within MARKER_PAD of the marker and there is no existing annotation for the SNP/marker.
# Returns: Nothing
# Assumes: Nothing
# Effects: Outputs to BCP file
# Throws: Nothing

def processSNPregion(fp, chr, startCoord, endCoord):
        # 
        # Terminology:
        # SNPregion	- the region of the chromosome between 'startCoord' 'endCoord' (passed to this routine)
        #
        # MarkerRegion	- the region of the chr including SNPregion and
        #			MARKER_PAD BP on either side of SNPregion
        #
        # left of, right of - A is "left of" B if in
        #		the chr region we are working on, A's coord is less
        #		than B's (or if A and B are intervals, A's endCoord
        #		is less than B's startCoord.
        #		"right of" is defined similarly.
        #
        # Algorithm Outline:
        #
        # 1) Query Postgres for all the SNPs in the SNPregion, ordered by SNP location. Call this SNPlist.
        #
        # 2) Query Postgres for all markers in the MarkerRegion.  Call this MarkerList.
        #
        # 3) Compute the "join" between markers and SNPs that are within MARKER_PAD of each other. 
        #    We do this here, rather than asking Postgres to do it as we can do it more efficiently. 
        #    Here is how:
        # 
        # For each marker in MarkerList # i.e., the typically smaller list
        #     do binary search to find  # i.e., bin search the larger list
        #     the SNP w/ highest coord that is <= marker.endcoord+MARKER_PAD
        #     (any SNPs right of this do not have to be considered for this marker)
        # 
        #     Starting w/ this SNP, scan backward (left) through the SNPlist
        #     computing SNP-marker relationships for the marker,
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
        # The Data Structures:
        #
        # * SNPlist is the list of all Consensus_SNPs that lie in the coord
        #	range - ORDERED BY SNP coord.
        # 	Each SNP on SNPlist is
        #	(_ConsensusSnp_key, _Coord_Cache_key, startCoordinate)
        #	- populated by SQL query
        #
        # * Markers is the list of all Markers (w/ coordinates) in MarkerRegion
        # 	Each Marker on Markers is
        #	(_Marker_key, markerStart, markerEnd, markerStrand)
        #	- populated by SQL query
        #	- For now, order is unimportant. Could order by increasing
        #	  endCoordinate, then as we process markers, we could
        #	  increase the start coord of the binary search for SNPs.
        #

        print('processSNPregion(): marker query start time: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
        sys.stdout.flush()

        # query to fill Markers
        # exclude: withdrawn markers, marker type QTL and Cytogenetic, feature type heritable phenotypic
        Markers = db.sql('''
                select a.accid as markerId,
                       mc._marker_key, 
                       mc.startCoordinate as markerStart,
                       mc.endCoordinate as markerEnd, 
                       mc.strand as markerStrand 
                from MRK_Location_Cache mc, MRK_Marker m, MRK_MCV_Cache mcv, ACC_Accession a
                where mc._Marker_Type_key not in (3, 6) 
                and mc._Organism_key = 1
                and mc.genomicchromosome = '%s' 
                and mc.endCoordinate >= %s 
                and mc.startCoordinate <= %s
                and mc._Marker_key = m._Marker_key
                and m._Marker_Status_key = 1
                and m._Marker_key = mcv._Marker_key
                and mcv.qualifier = 'D'
                and mcv._mcvTerm_key != 6238170
                and mc._Marker_key = a._Object_key
                and a._MGIType_key = 2
                and a._LogicalDB_key = 1
                and a.preferred = 1
                ''' % (chr, startCoord-MARKER_PAD, endCoord+MARKER_PAD), 'auto')

        print('processSNPregion(): marker query end time: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
        sys.stdout.flush()

        #
        #  Process each SNP on SNPlist
        #
        print('processSNPregion(): process SNPlist start time: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))
        sys.stdout.flush()
        idxLastSnp = len(SNPlist)-1	# index of last SNP in SNPlist
        prevSnpIdx = 0			    # index of SNP found on prev iteration (start binary search from there)

        for marker in Markers:
            markerStart = marker['markerStart']
            markerEnd = marker['markerEnd']

            # use binary search to find the index in SNPlist of the farthest "right" SNP to consider for this marker
            snpIdx = listBinarySearch(SNPlist, markerEnd+MARKER_PAD, prevSnpIdx, idxLastSnp)

            # iterate backward through the SNPs from snpIdx and process SNP-Marker pairs.
            # (deal w/ boundary condition, no SNP is within range?)
            i = snpIdx
            leftmostCoord = markerStart-MARKER_PAD
            while (i >= 0 and SNPlist[i]['startCoordinate'] >= leftmostCoord):
                processSNPmarkerPair(fp, SNPlist[i], marker)
                i = i-1

        # prevSnpIdx = snpIdx end SNP loop
        print('processSNPregion(): process SNPlist end time: %s' % time.strftime("%H.%M.%S.%m.%d.%y", time.localtime(time.time())))	
        sys.stdout.flush()
        return


# Purpose: Process a SNP-marker pair where the SNP and marker are within
#	   MARKER_PAD BP of each other.
#	   "Process" means: compute the appropriate fxn class for the
#	   for the relationship and output the record representing the
#	   relationship to the BCP file.
# Returns: Nothing
# Assumes: fp is an open filepointer to the BCP file.
# Effects: Outputs to BCP file
# Throws: Nothing

def processSNPmarkerPair(fp,      # file pointer of output file
                         snp,	  # dictionary w/ keys as above
                         marker): # dictionary w/ keys as above

    # next available _SNP_ConsensusSnp_Marker_key
    global primaryKey

    markerId = marker['markerId']
    markerKey = marker['_marker_key']
    markerStart = marker['markerStart']
    markerEnd = marker['markerEnd']
    markerStrand = marker['markerStrand']
    snpLoc = snp['startCoordinate']
    snpKey = snp['_consensussnp_key']
    coordCacheKey = snp['_coord_cache_key']
    snpId = snp['accid']
    fxnKey = -1
    dirDist = []

    #
    # if SNP exists in the Alliane file
    #   then set the fxnKey from the allianceLookup
    #   there may be > 1 fxnKey
    #
    allianceKey = snpId + ':' + markerId
    if allianceKey in allianceLookup:
        dirDist = ['not applicable', 0]
        direction = dirDist[0]
        distance = int(dirDist[1])
        for f in allianceLookup[allianceKey]:
            fxnKey = f[4]
            fp.write(snpWrite % (primaryKey, snpKey, markerKey, fxnKey, coordCacheKey, distance, direction))
            primaryKey = primaryKey + 1
        sys.stdout.flush()
        return

    #
    # else the SNP is located within the coordinates of the marker
    #
    elif snpLoc >= markerStart and snpLoc <= markerEnd:
        fxnKey = fxnLookup[WITHIN_COORD_TERM]
        dirDist = ['not applicable', 0]
        sys.stdout.flush()
    
    #
    # the SNP must be located within one of the pre-defined "KB" distances from the marker. 
    # Check each distance (starting with the small range) to see which one it is.
    #
    else:
        dirDist = getKBTerm(snpLoc, markerStart, markerEnd, markerStrand)
        sys.stdout.flush()
    
    if dirDist == []:
        print(SNP_NOT_WITHIN % (snp, MARKER_PAD, marker, snpLoc, markerStart, markerEnd))
        sys.stdout.flush()
        return

    # otherwise direction and distance are set. If fxnKey not yet set ([0, 'not applicable'] then set it
    else:
        if fxnKey == -1:
            fxnKey = fxnLookup[WITHIN_KB_TERM]
        direction = dirDist[0]
        distance = int(dirDist[1])

    fp.write(snpWrite % (primaryKey, snpKey, markerKey, fxnKey, coordCacheKey, distance, direction))
    primaryKey = primaryKey + 1
    sys.stdout.flush()
    return

# Purpose: Use the SNP/marker coordinates and marker strand to determine
#          if the SNP is within a MARKER_PAD distance from the marker.
#          if it is, the appropriate term is returned for the annotation.
# Returns: The term key or -1 (if the SNP is not within the distance)
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing

def getKBTerm(snpLoc, markerStart, markerEnd, markerStrand):

    #
    #  If the SNP is not within MARKER_PAD distance from the marker, don't check any further.
    #
    if snpLoc < (markerStart - MARKER_PAD) or snpLoc > (markerEnd + MARKER_PAD):
        return []

    #
    #  Find the midpoint of the marker.
    #
    midPoint = (markerStart + markerEnd) / 2.0

    #
    #  If the SNP coordinate is <= the midpoint of the marker on a "+" strand, 
    #   the SNP is considered to be upstream.
    #
    if markerStrand == '+' and snpLoc <= midPoint:
        direction = 'upstream'
        distance = markerStart - snpLoc
    #
    #  If the SNP coordinate is > the midpoint of the marker on a "+" strand, 
    #   the SNP is considered to be downstream.
    #
    elif markerStrand == '+' and snpLoc > midPoint:
        direction = 'downstream'
        distance = snpLoc - markerEnd
    #
    #  If the SNP coordinate is <= the midpoint of the marker on a "-" strand, 
    #   the SNP is considered to be downstream.
    #
    elif markerStrand == '-' and snpLoc <= midPoint:
        direction = 'downstream'
        distance = markerStart - snpLoc
    #
    #  If the SNP coordinate is > the midpoint of the marker on a "-" strand, 
    #   the SNP is considered to be upstream.
    #
    elif markerStrand == '-' and snpLoc > midPoint:
        direction = 'upstream'
        distance = snpLoc - markerEnd
    #
    #  If the SNP coordinate is <= the midpoint of the marker and strand is Null, 
    #   the SNP is considered to be proximal '.' strand 
    #   for VISTA and Ensembl Regulatory features loaded as Gene Models because seq_coord_cache does not allow nulls
    elif (markerStrand == None or markerStrand == '.') and snpLoc <= midPoint:
        direction = 'proximal'
        distance = markerStart - snpLoc
    #
    #  If the SNP coordinate is > the midpoint of the marker and strand is Null, 
    #   the SNP is considered to be downstream.
    #
    elif (markerStrand == None or markerStrand == '.') and snpLoc > midPoint:
        direction = 'distal'
        distance = snpLoc - markerEnd
    else:
        return []

    dirDistList = [direction, distance]
    return dirDistList

# Purpose: Do binary search through a list of dictionaries as typically returned from a call to db.sql()
#          The list should be sorted in increasing order on some dict key.
# Returns: Index in the list of a dictionary item whose key = the searchKey.
#	   Or if no dictionary item matches that key,
#	   Returns the max index of the list item whose key is < searchKey.
#	   Returns -1 if searchKey < all dictionary item keys.
# Assumes: list is sorted in increasing order of the keyField
# Effects: Nothing
# Throws: Nothing

def listBinarySearch(list,	# the list to search, sorted by keyField
                     searchKey, # the value to look for
                     bottomIdx, # lowest index in list[] to search
                     topIdx):	# max index in list[] to search

    found = 0

    while (bottomIdx != topIdx+1 and not found):
        # check that (0+1)/2 = 0, (3+4)/2 = 3, etc.
        midIdx = int((bottomIdx+topIdx)/2)		# integer division?
        listvalue = list[midIdx]['startCoordinate']
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
sys.exit(0)

