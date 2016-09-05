=======================
dirac-proxy-info
=======================

Obtain detailed info about user proxies.

Usage::

  dirac-proxy-info.py (<options>|<cfgFile>)* 

 

Options::

  -f:  --file=           : File to use as user key 

  -i   --version         : Print version 

  -n   --novoms          : Disable VOMS 

  -v   --checkvalid      : Return error if the proxy is invalid 

  -x   --nocs            : Disable CS 

  -e   --steps           : Show steps info 

  -j   --noclockcheck    : Disable checking if time is ok 

  -m   --uploadedinto    : Show uploaded proxies info 

Example::

  $ dirac-proxy-info
  subject      : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar/CN=proxy/CN=proxy
  issuer       : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar/CN=proxy
  identity     : /O=GRID-FR/C=FR/O=CNRS/OU=CPPM/CN=Vanessa Hamar
  timeleft     : 23:53:55
  DIRAC group  : dirac_user
  path         : /tmp/x509up_u40885
  username     : vhamar
  VOMS         : True
  VOMS fqan    : ['/formation']


