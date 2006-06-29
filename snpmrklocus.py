#!/usr/local/bin/python

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
#  03/21/2006  sc   updated locus-region upstream/downstream algorithm tr7563
#                   MGI3.44
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
LOCUS_REGION_TERM = 'Locus-Region'
UPSTREAM_TERM = 'Locus-Region (upstream)'
DOWNSTREAM_TERM = 'Locus-Region (downstream)'

# _Term_key for 'Locus-Region' function class
locusRegionKey = 0
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
    global snpDbServer, snpDbUser, dbPasswordFile
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

    mgdDbServer = os.environ['MGD_DBSERVER']
    mgdDbName = os.environ['MGD_DBNAME']
    mgdDbUser = os.environ['MGD_DBUSER']
    snpDbServer = os.environ['SNPBE_DBSERVER']
    snpDbName = os.environ['SNPBE_DBNAME']
    snpDbUser = os.environ['SNPBE_DBUSER']

    dbPasswordFile = os.environ['SNPBE_DBPASSWORDFILE']
    dbPassword = string.strip(open(dbPasswordFile,'r').readline())

    #
    #  Set up a connection to the mgd database.
    #
    db.useOneConnection(1)
    db.set_sqlLogin(mgdDbUser, dbPassword, mgdDbServer, mgdDbName)

    #
    #  Create a lookup for upstream/downstream function class terms.
    #
    cmds = []
    cmds.append('select t._Term_key, t.term ' + \
                     'from VOC_Vocab v, VOC_Term t ' + \
                     'where v.name = "%s" ' % FNCT_CLASS_VOCAB + \
                           'and v._Vocab_key = t._Vocab_key ' + \
                           'and t.term in ("%s", "%s") ' % (UPSTREAM_TERM , DOWNSTREAM_TERM) )
    cmds.append('select t._Term_key ' + \
		    'from VOC_Term t, VOC_Vocab v ' + \
		    'where t.term = "%s" ' % LOCUS_REGION_TERM + \
                    'and t._Vocab_key = v._Vocab_key ' + \
                    'and v.name = "%s"' % FNCT_CLASS_VOCAB)

    results = db.sql(cmds, 'auto')

    fxnLookup = {}
    for r in results[0]:
        fxnLookup[r['term']] = r['_Term_key']
    locusRegionKey = results[1][0]['_Term_key']
    # close connection to the mgd database
    db.useOneConnection(0)

    #
    #  Set up a connection to the snp database.
    #
    db.useOneConnection(1)
    db.set_sqlLogin(snpDbUser, dbPassword, snpDbServer, snpDbName)
    
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
    global tmpFxnTable

    print 'Drop the temp table'
    sys.stdout.flush()

    db.sql('drop table tempdb..' + tmpFxnTable, 'auto')

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
    global fxnLookup
    global fpTmpFxn

    print 'Get locus-region SNP/marker annotations'
    sys.stdout.flush()

    cmd = []
    cmd.append('select sm._ConsensusSnp_Marker_key, ' + \
                      'sc.startCoordinate "snpStart", ' + \
                      'mc.startCoordinate "markerStart", ' + \
                      'mc.endCoordinate "markerEnd", ' + \
                      'mc.strand "markerStrand" ' + \
               'from SNP_ConsensusSnp_Marker sm, ' + \
                    'SNP_Coord_Cache sc, ' + \
                    'MRK_Location_Cache mc ' + \
               'where sm._ConsensusSnp_key = sc._ConsensusSnp_key ' + \
                     'and sm._Coord_Cache_key = sc._Coord_Cache_key ' + \
                     'and sm._Marker_key = mc._Marker_key ' + \
                     'and sm._Fxn_key = %s ' % locusRegionKey + \
                     'and mc.startCoordinate is not null ' + \
                     'and mc.endCoordinate is not null ' + \
                     'and mc.strand is not null')

    results = db.sql(cmd, 'auto')

    print 'Create the bcp file'
    sys.stdout.flush()

    for r in results[0]:
        primaryKey = r['_ConsensusSnp_Marker_key']
        snpStart = r['snpStart']
        markerStart = r['markerStart']
        markerEnd = r['markerEnd']
        markerStrand = r['markerStrand']

	# if snp *within* the marker, do not update
	# Note: marker start coordinates in MGI are always < end coordinates
        if snpStart >= markerStart and snpStart <= markerEnd:
	    #print 'snpStart %s' % snpStart
	    #print 'markerStart %s' % markerStart
            #print 'markerEnd %s' % markerEnd
            #print 'strand %s' % markerStrand
            #print '_CS_Marker_key %s' % primaryKey
 	    #print '' 
	    continue
	# if the marker straind in '+' determine fxn class accordingly
	elif markerStrand == '+':
	    # if snpStart < markerStart, the SNP is considered to be upstream
	    if snpStart < markerStart:
		fxnKey = fxnLookup[UPSTREAM_TERM]
	    # if snpStart > markerStart, the SNP is considered to be downstream
	    elif snpStart > markerStart:
		fxnKey = fxnLookup[DOWNSTREAM_TERM]

	# if marker strand is '-' determine fxn class accordingly
	elif markerStrand == '-':
	    # if snpStart < markerStart, the SNP is considered to be downstream
            if snpStart < markerStart:
                fxnKey = fxnLookup[DOWNSTREAM_TERM]
	    # if snpStart > markerStart, the SNP is considered to be upstream
	    elif snpStart > markerStart:
                fxnKey = fxnLookup[UPSTREAM_TERM]
	fpTmpFxn.write(str(primaryKey) + DL + str(fxnKey) + CRT)

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
    global snpDbServer, snpDbUser, dbPasswordFile
    global tmpFxnTable, tmpFxnFile

    print 'Create the temp table'
    sys.stdout.flush()
    db.sql('create table tempdb..' + tmpFxnTable + ' ' + \
           '(_ConsensusSnp_Marker_key int not null, ' + \
            '_Fxn_key int not null)', 'auto')

    print 'Load the bcp file into the temp table'
    sys.stdout.flush()
    bcpCmd = 'cat ' + dbPasswordFile + \
             ' | bcp tempdb..' + tmpFxnTable + ' in ' + \
             tmpFxnFile + ' -c -t\| -S' + snpDbServer + ' -U' + snpDbUser
    os.system(bcpCmd)

    print 'Create indexes on the temp table'
    sys.stdout.flush()
    db.sql('create index idx1 on tempdb..' + tmpFxnTable + ' ' + \
           '(_ConsensusSnp_Marker_key)', 'auto')
    db.sql('create index idx2 on tempdb..' + tmpFxnTable + ' ' + \
           '(_Fxn_key)', 'auto')


# Purpose: Update the function classes using the keys in the temp table.
# Returns: Nothing
# Assumes: Nothing
# Effects: Nothing
# Throws: Nothing

def applyUpdates():
    global tmpFxnTable

    print 'Update the function classes'
    sys.stdout.flush()
    db.sql('update SNP_ConsensusSnp_Marker ' + \
           'set _Fxn_key = t._Fxn_key ' + \
           'from SNP_ConsensusSnp_Marker sm, ' + \
                'tempdb..' + tmpFxnTable + ' t ' + \
           'where sm._ConsensusSnp_Marker_key = t._ConsensusSnp_Marker_key',
           'auto')


#
#  MAIN
#
initialize()
createBCPFile()
loadBCPFile()
applyUpdates()
finalize()

sys.exit(0)
