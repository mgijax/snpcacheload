#!/usr/local/bin/python -x

#  snpmrklocus.py
###########################################################################
#
#  Purpose:
#
#      This script will check records in the SNP_ConsensusSnp_Marker table
#      where SNP/marker pairs have been annotated to the "locus-region"
#      SNP function class and determine whether the annotation should be
#      upstream or downstream, depending on the SNP/marker coordinates.
#      The primary key for each SNP_ConsensusSnp_Marker record and the key
#      for the new SNP function class are written to a bcp file to load a
#      temp table. The keys in this temp table are used to update the
#      SNP_ConsensusSnp_Marker table with the new function class.
#
#  Usage:
#
#      snpmrklocus.py
#
#  Env Vars:  None
#
#  Inputs:
#
#      The following tables in the SNP database are used as input:
#
#      1) SNP_ConsensusSnp_Marker
#      2) SNP_Coordinate_Cache
#      3) MRK_Location_Cache
#
#  Outputs:
#
#      A "|" delimited bcp file to load records into a temporary table.
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
#  01/25/2013  lec  TR11248/TR10778 convert to postgres
#
#  04/20/2012  sc   TR10778 convert to postgres
#
#  09/01/2011  lec  TR10805/add _Organism_key = 1
#
#  06/30/2006  lec  modified for mgiconfig
#
#  03/21/2006  sc   updated locus-region upstream/downstream algorithm tr7563
#                   MGI3.44
#  09/28/2005  DBM  Initial development
#  
###########################################################################

import sys
import os
import string
import loadlib
import db
import StringIO
#
#  CONSTANTS
#

DL = '|'
CRT = '\n'
NULL = ''

LOCUS_REGION_TERM = 'Locus-Region'
UPSTREAM_TERM = 'upstream'
DOWNSTREAM_TERM = 'downstream'

# _Term_key for 'Locus-Region' function class
locusRegionKey = 0

# database environment variables
server = os.environ['MGD_DBSERVER']
database = os.environ['MGD_DBNAME']
user = os.environ['MGD_DBUSER']
#print server
#print database

# lookup to resolve function class string to key
fxnLookup = {}

#
#  FUNCTIONS
#

# Purpose: Perform initialization for the script.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing

def initialize():
    global locusRegionKey
    global fxnLookup
    global tmpFxnTable, tmpFxnFile, fpTmpFxn

    print 'Perform initialization'
    sys.stdout.flush()

    #
    #  Initialize variables.
    #
    dataDir = os.environ['CACHEDATADIR']
    tmpFxnTable = os.environ['TMP_FXN_TABLE']
    tmpFxnFile = dataDir + '/' + os.environ['TMP_FXN_FILE']

    #
    #  Set up a connection to the mgd database.
    #
    db.useOneConnection(1)
    db.setReturnAsSybase(False)
    results = db.sql('''
    	SELECT t._Term_key
        FROM VOC_Term t
        WHERE t._Vocab_key = 49 
        AND t.term = '%s'
	''' % (LOCUS_REGION_TERM), 'auto')
    locusRegionKey = results[1][0]

    #
    #  Open the bcp file.
    #
    try:
        fpTmpFxn = open(tmpFxnFile,'w')
    except:
        sys.stderr.write('Could not open bcp file: %s\n' % tmpFxnFile)
        sys.exit(1)

    return


# Purpose: Perform cleanup steps for the script.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing

def finalize():

    db.useOneConnection(0)

    return


# Purpose: Create a bcp file that contains the primary key for each
#          "locus-region" record, along with the key for the new function
#          class that should be used to update each record.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing

def createBCPFile():
    global fpTmpFxn

    print 'Get locus-region SNP/marker annotations'
    sys.stdout.flush()

    results = db.sql('''
    	SELECT sm._ConsensusSnp_Marker_key, 
        	sc.startCoordinate as snpLoc, 
        	mc.startCoordinate as markerStart, 
        	mc.endCoordinate as markerEnd, 
        	mc.strand as markerStrand 
        FROM SNP_ConsensusSnp_Marker sm, 
                SNP_Coord_Cache sc, 
                MRK_Location_Cache mc 
        WHERE sm._ConsensusSnp_key = sc._ConsensusSnp_key 
                AND sm._Coord_Cache_key = sc._Coord_Cache_key 
                AND sm._Marker_key = mc._Marker_key 
		AND mc._Organism_key = 1 
                AND sm._Fxn_key = %s 
                AND mc.startCoordinate IS NOT NULL 
                AND mc.endCoordinate IS NOT NULL 
		''' % (locusRegionKey), 'auto')

    print 'Create the bcp file'
    sys.stdout.flush()

    for r in results[1]:
        primaryKey = r[0]
        snpLoc = r[1]
        markerStart = r[2]
        markerEnd = r[3]
        markerStrand = r[4]


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
        #  and strand is Null, the SNP is considered to be proximal
        #
	elif markerStrand == None and snpLoc <= midPoint:
	    direction = 'upstream'
	#
	#  If the SNP coordinate is > the midpoint of the marker
	#  and strand is Null, the SNP is considered to be downstream.
	#
	elif markerStrand == None and snpLoc > midPoint:
	    direction = 'downstream'
	else:
	    print 'not covered by algorithm'
	    print '    primaryKey: %s snpLoc: %s markerStart: %s markerEnd: %s markerStrand: %s' % ( primaryKey, snpLoc, markerStart, markerEnd, markerEnd) 

	fpTmpFxn.write(str(primaryKey) + DL + direction + CRT)

    #
    #  Close the bcp file.
    #
    fpTmpFxn.close()

    return


# Purpose: Load the bcp file into a new temp table.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing

def loadBCPFile():
    global tmpFxnTable, tmpFxnFile

    print 'Create the temp table'
    sys.stdout.flush()
    db.sql('''
    	CREATE TEMPORARY TABLE %s
        (_ConsensusSnp_Marker_key int not null,
         direction varchar not null
	)
	''' % (tmpFxnTable), None)

    print 'Load the bcp file into the temp table'
    sys.stdout.flush()

    tmpFile = open(tmpFxnFile, 'r')
    db.executeCopyFrom(tmpFile, tmpFxnTable, DL)
    db.commit()

    print 'Create indexes on the temp table'
    sys.stdout.flush()
    db.sql('CREATE index idx1 on %s (_ConsensusSnp_Marker_key)' % (tmpFxnTable), None)
    db.sql('CREATE index idx2 on %s (direction)' % (tmpFxnTable), None)

# Purpose: Update the function classes using the keys in the temp table.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing

def applyUpdates():
    global tmpFxnTable

    print 'Update the distance direction'
    sys.stdout.flush()

    db.sql('''
    	UPDATE SNP_ConsensusSnp_Marker sm 
        SET distance_direction = t.direction
        FROM %s t
        WHERE sm._ConsensusSnp_Marker_key = t._ConsensusSnp_Marker_key
	''' % (tmpFxnTable), 'auto')

    results = db.sql('''
    	SELECT t.* 
	FROM SNP_ConsensusSnp_Marker sm, %s t
	WHERE sm._ConsensusSnp_Marker_key = t._ConsensusSnp_Marker_key
	''' % (tmpFxnTable), 'auto')
    db.commit()
#
#  MAIN
#
initialize()
createBCPFile()
loadBCPFile()
applyUpdates()
finalize()

sys.exit(0)
