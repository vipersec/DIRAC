===========================
dirac-wms-job-status
===========================

  Retrieve status of the given DIRAC job

Usage::

  dirac-wms-job-status [option|cfgfile] ... JobID ...

Arguments::

  JobID:    DIRAC Job ID 

 

Options::

  -f:  --file=           : Get status for jobs with IDs from the file 

  -g:  --group=          : Get status for jobs in the given group 

Example::

  $ dirac-wms-job-status 2
  JobID=2 Status=Done; MinorStatus=Execution Complete; Site=EELA.UTFSM.cl;


