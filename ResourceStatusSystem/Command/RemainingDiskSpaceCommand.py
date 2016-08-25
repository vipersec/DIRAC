''' RemainingDiskSpaceCommand

  The Command gets the remaining space that is left in a DIRAC Storage Element

'''

import os
from DIRAC                                                      import S_OK, S_ERROR, gConfig
from DIRAC.ResourceStatusSystem.Command.Command                 import Command
from DIRAC.Core.DISET.RPCClient                                 import RPCClient
from DIRAC.ResourceStatusSystem.Client.ResourceManagementClient import ResourceManagementClient

__RCSID__ = '$Id:  $'


class RemainingDiskSpaceCommand( Command ):
  '''
  Uses diskSpace method to get the remaining space
  '''

  def __init__( self, args = None ):

    super( RemainingDiskSpaceCommand, self ).__init__( args )

    self.rpc = None
    self.rsClient = None

  def getUrl(self, SE, protocol = None ):
    """
    Gets the url of a storage element from the CS.
    If protocol is set, then it is going to fetch the
    url only if it uses the given protocol.

    :param SE: String
    :param protocol: String
    :return: String
    """
    attributes = [ "Protocol", "Host", "Port", "Path"]

    result = ""
    for attribute in attributes:
      res = gConfig.getValue( "/Resources/StorageElements/%s/AccessProtocol.1/%s" % ( SE, attribute ) )

      if protocol:

        # Not case-sensitive
        protocol = protocol.lower()

        if attribute is "Protocol" and res != protocol:
          result = None
          break

      if attribute is "Protocol":
        result += res + "://"
      elif attribute is "Port":
        result += ":" + res
      else:
        result += res

    return result

  def getAllUrls(self, protocol = None ):
    """
    Gets all the urls of storage elements from CS.
    If protocol is set, then it is going to fetch only
    the urls that use the given protocol.

    :param protocol: String
    :return: Dictionary of { StorageElementName : Url }
    """

    if protocol:
      # Not case-sensitive
      protocol = protocol.lower()

    SEs = gConfig.getSections("/Resources/StorageElements/")

    urls = {}
    for SE in SEs['Value']:
      res = self.getUrl( SE, protocol )
      if res:
        urls.update( {SE: res} )

    return urls

  def doNew( self, masterParams = None ):
    pass

  def doCache( self ):
    pass

  def doMaster( self ):

    self.rsClient = ResourceManagementClient()

    DIPSurls = self.getAllUrls( "dips" )

    for name in DIPSurls:
      self.rpc = RPCClient( DIPSurls[name], timeout=120 )
      space = self.rpc.getRemainingDiskSpace("/", "GB")
      self.rsClient.addOrModifySpaceTokenOccupancyCache(name, free = space )

    return S_OK()