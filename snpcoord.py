#!/usr/local/bin/python

'''
#
# Purpose:
#
# Create bcp file for SNP_Coord_Cache
#
# Uses environment variables to determine Server and Database
# (DSQUERY and MGD).
#
# Usage:
#	snpcoord.py
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
import mgi_utils
import loadlib

NL = '\n'
DL = '|'
table = os.environ['TABLE']
userKey = 0
loaddate = loadlib.loaddate

def createBCP():

	print 'Creating %s.bcp...%s' % (table, mgi_utils.date())

	outBCP = open('%s.bcp' % (table), 'w')

        cmd = 'select distinct ' + \
	      'mcf._Object_key, mcf._Feature_key, mcf.startCoordinate, mcf.strand, ' + \
	      'c.chromosome, s._varClass_key, s.allele_summary, s.iupacCode ' + \
              'from MAP_Coordinate mc, MAP_Coord_Feature mcf, ' + \
	      'MRK_Chromosome c, SNP_ConsensusSnp s ' + \
	      'where mc._MGIType_key = 27 ' + \
	      'and mc._Object_key = c._Chromosome_key ' + \
	      'and mc._Map_key = mcf._Map_key ' + \
	      'and mcf._MGIType_key = 29 ' + \
	      'and mcf._Object_key = s._ConsensusSnp_key '

	results = db.sql(cmd, 'auto')

	for r in results:

		outBCP.write(str(r['_Object_key']) + DL + \
			str(r['_Feature_key']) + DL + \
			r['chromosome'] + DL + \
			str(r['startCoordinate']) + DL + \
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

print '%s' % mgi_utils.date()
createBCP()
print '%s' % mgi_utils.date()

