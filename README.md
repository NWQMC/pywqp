pywqp
=====

A generic scriptable Python client for downloading datasets from the USGS/EPA Water Quality Portal: an alternative to manual use of the website at http://www.waterqualitydata.us.

The project consists of the following components:
 - a client module, `pywqp-client.py`, which obtains WQP data in native Water Quality XML and (if desired) stashes the result on the file system. This is suitable for inclusion in Python programs.
 - a convenience wrapper, `pywqp.py`, which manages common query-and-convert actions from the command line. When it is invoked, the commandline parameters are sent to an instance of pywqp_client.py.
 - a parameter validation module, `pywqp_validator.py`

The project also contains a BDD test suite written in [lettuce](http://lettuce.it/). This is located in the `tests` folder. Shocking, I know.
