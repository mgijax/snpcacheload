#!/usr/local/bin/python

'''
# Program: snpcoord.py
#
# Purpose: Create bcp file for SNP_Coord_Cache
#
# Usage:
#	snpcoord.py
#
# Inputs: 1) mgd database
#         2) Configuration (see list below)
# Outputs: 1) log file
#          2) bcp file
# Exit Codes: 1 if database connection or sql error
#
# History
#
# 08/17/2005	sc
#	- SNP (TR 1560)
#
'''

import sys
import os
import db

# MGI python libraries
import mgi_utils
import loadlib


# constants
NL = '\n'
DL = '|'

# database errors
DB_ERROR = 'A database error occured: '
DB_CONNECT_ERROR = 'Connection to the database failed: '

table = os.environ['COORD_CACHE_TABLE']

#
# Functions
#

def createBCP():
    # Purpose: creates SNP_Coord_Cache bcp file
    # Returns: nothing
    # Assumes: nothing
    # Effects: creates file in the filesystem
    # Throws:  nothing

    print 'Creating %s.bcp...%s' % (table, mgi_utils.date())

    outBCP = open('%s.bcp' % (table), 'w')

    cmd = 'select distinct ' + \
	  'mcf._Object_key, mcf._Feature_key, mcf.startCoordinate, mcf.strand, ' + \
	  'c.chromosome, c.sequenceNum, s._varClass_key, ' + \
	  's.alleleSummary, s.iupacCode ' + \
	  'from MAP_Coordinate mc, MAP_Coord_Feature mcf, ' + \
	  'MRK_Chromosome c, SNP_ConsensusSnp s ' + \
	  'where mc._MGIType_key = 27 ' + \
	  'and mc._Object_key = c._Chromosome_key ' + \
	  'and mc._Map_key = mcf._Map_key ' + \
	  'and mcf._MGIType_key = 30 ' + \
	  'and mcf._Object_key = s._ConsensusSnp_key ' + \
	  'order by mcf._Object_key '
    results = db.sql(cmd, 'auto')
    # create a dictionary of cs keys to there values
    # so we can easily determine multi coord cs
    coordDict = {}
    for r in results:
	currentCSKey = r['_Object_key']
	if coordDict.has_key(currentCSKey):
	    valueList = coordDict[currentCSKey]
	    valueList.append(r)
	    coordDict[currentCSKey] = valueList
	else:
	    coordDict[currentCSKey] = [r]
    # create bcp file determing multi-coord CS as we go
    for csKey in coordDict.keys():
	resultList = coordDict[csKey]
	isMultiCoord = 0
	if len(resultList) > 1:
	    isMultiCoord = 1
	for r in resultList:
	    outBCP.write(str(r['_Object_key']) + DL + \
		str(r['_Feature_key']) + DL + \
		str(r['chromosome']) + DL + \
		str(r['sequenceNum']) + DL + \
		str(r['startCoordinate']) + DL + \
		str(isMultiCoord) + DL + \
		str(r['strand']) + DL + \
		str(r['_varClass_key']) + DL + \
		str(r['alleleSummary']) + DL + \
		str(r['iupacCode']) + NL)

    outBCP.close()

#
# Main Routine
#

print 'snpcoord.py start: %s' % mgi_utils.date()
try:
    createBCP()
except db.connection_exc, message:
    error = '%s%s' % (DB_CONNECT_ERROR, message)
    sys.stderr.write(message)
    sys.exit(message)
except db.error, message:
    error = '%s%s' % (DB_ERROR, message)
    sys.stderr.write(message)
    sys.exit(message)

print 'snpcoord.py end: %s' % mgi_utils.date()

