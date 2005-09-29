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
#  09/28/2005  DBM  Initial development
#
###########################################################################

import sys
import os
import string
import db
import loadlib

#
#  CONSTANTS
#

DL = '|'
CRT = '\n'
NULL = ''

FNCT_CLASS_VOCAB = 'SNP Function Class'
WITHIN_COORD_TERM = 'within coordinates of'
WITHIN_KB_TERM = 'within %s kb %s of'

KB_DISTANCE = [ 2, 10, 100, 500, 1000 ]

QUERY = 'select sc._ConsensusSnp_key, ' + \
               'mc._Marker_key, ' + \
               'sc._Feature_key, ' + \
               'sc.startCoordinate "snpStart", ' + \
               'mc.startCoordinate "markerStart", ' + \
               'mc.endCoordinate "markerEnd", ' + \
               'mc.strand "markerStrand" ' + \
        'from SNP_Coord_Cache sc, ' + \
             'MRK_Location_Cache mc ' + \
        'where mc.chromosome = "%s" and ' + \
              'mc.startCoordinate is not null and ' + \
              'mc.endCoordinate is not null and ' + \
              'mc.chromosome = sc.chromosome and ' + \
              'sc.startCoordinate >= (mc.startCoordinate - 1000000) and ' + \
              'sc.startCoordinate <= (mc.endCoordinate + 1000000) and ' + \
              'not exists (select 1 ' + \
                      'from SNP_ConsensusSnp_Marker cm ' + \
                      'where cm._Marker_key = mc._Marker_key and ' + \
                            'cm._ConsensusSnp_key = sc._ConsensusSnp_key)'


#
#  FUNCTIONS
#

# Purpose: Perform initialization for the script.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing

def initialize():
    global userKey, loadDate
    global fxnLookup, chrList, primaryKey
    global fpSnpMrk

    print 'Perform initialization'
    sys.stdout.flush()

    #
    #  Initialize variables.
    #
    dataDir = os.environ['CACHEDATADIR']
    snpMrkFile = dataDir + '/' + os.environ['SNP_MRK_FILE']

    dbServer = os.environ['DBSERVER']
    dbName = os.environ['DBNAME']
    dbUser = os.environ['DBUSER']
    dbPasswordFile = os.environ['DBPASSWORDFILE']
    dbPassword = string.strip(open(dbPasswordFile,'r').readline())

    userKey = loadlib.verifyUser(dbUser, 1, None)
    loadDate = loadlib.loaddate

    #
    #  Set up a connection to the database.
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

    fxnLookup = {}
    for r in results:
        fxnLookup[r['term']] = r['_Term_key']

    #
    #  Create of list of chromosomes.
    #
    results = db.sql('select distinct chromosome from SNP_Coord_Cache', 'auto')

    chrList = []
    for r in results:
        chrList.append(r['chromosome'])

    #
    #  Get the next available primary key for the SNP_ConsensusSnp_Marker
    #  table.
    #
    results = db.sql('select max(_ConsensusSnp_Marker_key) + 1 "key" ' + \
                     'from SNP_ConsensusSnp_Marker', 'auto')

    primaryKey = results[0]['key']

    #
    #  Open the bcp file.
    #
    try:
        fpSnpMrk = open(snpMrkFile,'w')
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


# Purpose: Use the SNP/marker coordinates and marker strand to determine
#          if the SNP is within a given "kb" distance from the marker.
#          If it is, the appropriate term is returned for the annotation.
# Returns: The term key or -1 (if the SNP is not within the distance)
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing

def getKBTerm(snpStart, markerStart, markerEnd, markerStrand, kbDist):
    global fxnLookup

    #
    #  If the SNP is not within the given KB distance from the marker,
    #  don't check any further.
    #
    if snpStart < (markerStart - (kbDist * 1000)) or \
       snpStart > (markerEnd + (kbDist * 1000)):
        return -1

    #
    #  Find the midpoint of the marker.
    #
    midPoint = (markerStart + markerEnd) / 2.0

    #
    #  If the SNP coordinate is <= the midpoint of the marker on a
    #  "+" strand, the SNP is considered to be upstream.
    #
    if markerStrand == '+' and snpStart <= midPoint:
        direction = 'upstream'

    #
    #  If the SNP coordinate is > the midpoint of the marker on a
    #  "+" strand, the SNP is considered to be downstream.
    #
    elif markerStrand == '+' and snpStart > midPoint:
        direction = 'downstream'

    #
    #  If the SNP coordinate is <= the midpoint of the marker on a
    #  "-" strand, the SNP is considered to be downstream.
    #
    elif markerStrand == '-' and snpStart <= midPoint:
        direction = 'downstream'

    #
    #  If the SNP coordinate is > the midpoint of the marker on a
    #  "-" strand, the SNP is considered to be upstream.
    #
    elif markerStrand == '-' and snpStart > midPoint:
        direction = 'upstream'

    else:
        return -1

    return fxnLookup[WITHIN_KB_TERM % (str(kbDist),direction)]


# Purpose: Create a bcp file with annotations for SNP/marker pairs where
#          the SNP is within 1000 kb of the marker and there is no existing
#          annotation for the SNP/marker.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing

def process():
    global userKey, loadDate
    global fxnLookup, chrList, primaryKey
    global fpSnpMrk

    total = 0
    #
    #  Process one chromosome at a time to break up the size of the
    #  results set.
    #
    for chr in chrList:

        print 'Get SNP/marker pairs for chromosome ' + chr
        sys.stdout.flush()

        results = db.sql(QUERY % chr, 'auto')

        print 'Add ' + str(len(results)) + ' annotations to bcp file'
        sys.stdout.flush()
        total = total + len(results)

        #
        #  Process each row of the results set for the current chromosome.
        #
        for r in results:
            snpKey = r['_ConsensusSnp_key']
            markerKey = r['_Marker_key']
            featureKey = r['_Feature_key']
            snpStart = r['snpStart']
            markerStart = r['markerStart']
            markerEnd = r['markerEnd']
            markerStrand = r['markerStrand']

            #
            #  The SNP is located within the coordinates of the marker.
            #
            if snpStart >= markerStart and snpStart <= markerEnd:
                fxnKey = fxnLookup[WITHIN_COORD_TERM]

            #
            #  The SNP must be located within one of the pre-defined "KB"
            #  distances from the marker. Check each distance (starting
            #  with the small range) to see which one it is.
            #
            else:
                for kbDist in KB_DISTANCE:
                    fxnKey = getKBTerm(snpStart, markerStart, markerEnd,
                                       markerStrand, kbDist) 

                    #
                    #  If the distance has been determined, don't check
                    #  any others.
                    #
                    if fxnKey > 0:
                        break

            #
            #  Write a record to the bcp file that annotates the SNP/marker
            #  to the proper function class.
            #
            fpSnpMrk.write(str(primaryKey) + DL + \
                           str(snpKey) + DL + \
                           str(markerKey) + DL + \
                           str(fxnKey) + DL + \
                           str(featureKey) + DL + \
                           NULL + DL + NULL + DL + \
                           NULL + DL + NULL + DL + \
                           str(userKey) + DL + \
                           str(userKey) + DL + \
                           loadDate + DL + \
                           loadDate + CRT)

            primaryKey = primaryKey + 1

    print 'Total annotations: ' + str(total)
    sys.stdout.flush()

    return


#
#  MAIN
#
initialize()
process()
finalize()

sys.exit(0)
