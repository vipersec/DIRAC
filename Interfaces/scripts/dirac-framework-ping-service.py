#!/usr/bin/env python
########################################################################
# $HeadURL$
# File :    dirac-framework-ping-service
# Author :  Stuart Paterson
########################################################################
"""
  Ping the given DIRAC Service
"""
__RCSID__ = "$Id$"
import DIRAC
from DIRAC.Core.Base import Script

Script.setUsageMessage( '\n'.join( [ __doc__.split( '\n' )[1],
                                    'Usage:',
                                    '  %s [option|cfgfile] ... System Service|System/Agent' % Script.scriptName,
                                    'Arguments:',
                                    '  System:   Name of the DIRAC system (ie: WorkloadManagement)',
                                    '  Service:  Name of the DIRAC service (ie: Matcher)',
                                    '  url: URL of the service to ping (instead of System and Service)'] ) )
Script.parseCommandLine( ignoreErrors = True )
args = Script.getPositionalArgs()
system = None
service = None
url = None
if len( args ) == 1:
  # it is a URL
  if args[0].startswith( 'dips://' ):
    url = args[0]
  # It is System/Service
  else:
    sys_serv = args[0].split( '/' )
    if len( sys_serv ) != 2:
      Script.showHelp()
    else:
      system, service = sys_serv

elif len( args ) == 2:
  system, service = args[0], args[1]
else:
  Script.showHelp()


from DIRAC.Interfaces.API.Dirac                        import Dirac
dirac = Dirac()
exitCode = 0

result = dirac.ping( system, service, printOutput = True, url = url )

if not result:
  print 'ERROR: Null result from ping()'
  exitCode = 2
elif not result['OK']:
  print 'ERROR: ', result['Message']
  exitCode = 2

DIRAC.exit( exitCode )
