pywqp
=====

A generic scriptable Python client for downloading datasets from the USGS/EPA Water Quality Portal: an alternative to manual use of the website at http://www.waterqualitydata.us.

The project consists of the following components:
 - a client script, `pywqp-client`, which obtains WQP data in native Water Quality XML and (if desired) stashes the result on the file system
 - a pandas import utility, `pywqp-pandas.py`, which converts downloaded datasets into pandas data
 - a parameter validation module, `pywqp_validator.py`, which is the sole source of validation logic for WQP's somewhat pretzelesque API
 - a convenience wrapper, `pywqp.py`, which manages common query-and-convert actions. When it is invoked, the commandline parameters control the URL parameters sent to WQP.

