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
# History
#
# 08/17/2005	sc
#	- SNP (TR 1560)
#
'''

import sys
import os
import db
import mgi_utils
import loadlib

NL = '\n'
DL = '|'
table = os.environ['COORD_CACHE_TABLE']
userKey = 0
loaddate = loadlib.loaddate

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
	  's.allele_summary, s.iupacCode ' + \
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
		str(r['allele_summary']) + DL + \
		str(r['iupacCode']) + DL + \
		str(userKey) + DL + str(userKey) + DL + \
		loaddate + DL + loaddate + NL)

    outBCP.close()

#
# Main Routine
#

userKey = loadlib.verifyUser(os.environ['DBUSER'], 1, None)

print 'snpcoord.py start: %s' % mgi_utils.date()
createBCP()
print 'snpcoord.py end: %s' % mgi_utils.date()

