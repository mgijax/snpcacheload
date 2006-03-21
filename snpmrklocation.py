#!/usr/local/bin/python

'''
#
# Purpose:
#
# Create bcp files for snp..MRK_Location_Cache
#
# Usage:
#	snpmrklocation.py
#
# History
#
# 03/14/2006	sc created
#
'''

import sys
import os
import db
import mgi_utils
import string
import accessionlib

NL = '\n'
DL = '|'
TAB = '\t'
outputdir = os.environ['OUTPUTDIR']
cacheTable = os.environ['MRKLOC_CACHETABLE']

bcpFile = open('%s/%s.bcp' % (outputdir, cacheTable), 'w')

def setup():
        # set up connection to the mgd database
        server = os.environ['MGD_DBSERVER']
        mgdDB = os.environ['MGD_DBNAME']
        user = os.environ['MGD_DBUSER']
        password = string.strip(open(os.environ['MGD_DBPASSWORDFILE'], 'r').readline())
        db.set_sqlLogin(user, password, server, mgdDB)

def createBCP():
	print 'Creating %s/%s.bcp' % (outputdir, cacheTable)
	cmds = []
        cmds.append('select * ' + \
                'from MRK_Location_Cache')
        results = db.sql(cmds, 'auto')
	for r in results[0]:
	    markerKey = r['_Marker_key']
	    chromosome = r['chromosome']
	    sequenceNum = r['sequenceNum']
	    cytoOffset =  r['cytogeneticOffset']
	    if cytoOffset == None:
		cytoOffset = ''
	    offset = r['offset']
	    startCoord = r['startCoordinate']
	    if startCoord == None:
		startCoord = ''
	    endCoord = r['endCoordinate']
	    if endCoord == None:
		endCoord = ''
	    strand = r['strand']
	    if strand == None:
		strand = ''
	    mapUnits = r['mapUnits']
	    if mapUnits == None:
		mapUnits = ''
	    provider = r['provider']
	    if provider == None:
		provider = ''
	    version = r['version']
	    if version == None:
		version = ''
	    bcpFile.write(str(markerKey) + DL + \
		str(chromosome) + DL + \
	 	str(sequenceNum) + DL + \
                str(cytoOffset) + DL + \
                str(offset) + DL + \
                str(startCoord) + DL + \
                str(endCoord) + DL + \
                str(strand) + DL + \
                str(mapUnits) + DL + \
                str(provider) + DL + \
                str(version) + NL)
	bcpFile.close()

#
# Main Routine
#

print '%s' % mgi_utils.date()
setup()
createBCP()
print '%s' % mgi_utils.date()

