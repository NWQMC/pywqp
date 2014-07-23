pywqp
=====

A generic scriptable Python client for downloading datasets from the USGS/EPA Water Quality Portal: an alternative to manual use of the website at http://www.waterqualitydata.us.

The project consists of the following components:

-  A client module, `pywqp_client.py`, which obtains WQP data in native Water Quality XML and (if desired) stashes the result on the file system. This is suitable for inclusion in Python programs.

-  A convenience wrapper, `pywqp.py`, which manages common query-and-convert actions from the command line. When it is invoked, the commandline parameters are sent to an instance of pywqp_client.py.

-  A parameter validation module, `pywqp_validator.py`

<br/>
#### Using <tt>pywqp-client.py</tt> in your Python program
The core resource of `pywqp_client` is the class `RESTClient`. The pattern of usage is fairly simple:
<pre>
<tt>import pywpq_client
client_instance = pywqp_client.RESTClient()</tt>
</pre>

**Note** that you will need to ensure that the `pywqp` folder is in your system path, or else you will not be able to `import pywqp_client`.

`client_instance` is now ready to run any of the functions exposed by `RESTClient`.

##### Downloading WQP Data: <tt>request_wqp_data</tt>

Example: downloading pH data for Boone County, Iowa, US.
<pre>
<tt>verb = 'get'
host_url = 'http://waterqualitydata.us'
resource_label = 'station'
params = {'countrycode': 'US', 'statecode': 'US:19', 'countycode': 'US:19:015', 'parameterName': 'pH'}
result = client_instance.request_wqp_data(self, verb, host_url, resource_label, parameters, mime_type='text/csv')</tt>
</pre>

`request_wqp_data` returns a `requests.response` object. `pywqp_client` lets you do two things with that `response`:

 - Convert the dataset to an in-memory pandas dataframe

 - Stash the dataset on your local filesystem

##### Converting WQP Data to a <tt>pandas</tt> dataframe with <tt>response_as_pandas_dataframe</tt>


Example:
<pre>
<tt>dataframe = client__instance.response_as_pandas_dataframe(response)</tt>
</pre>


##### Stashing WQP Data to your local machine: <tt>stash_response</tt>


Example:
<pre>
<tt>filepathname = '/home/whb/examples/wqp_example.csv'
client_instance.stash_response(response, filepathname)</tt>
</pre>


<br/>
#### Running the pywqp tests
The project also contains a BDD test suite written in [lettuce](http://lettuce.it/). This is located in the `tests` folder. Shocking, I know.

You can run pywqp's tests whenever you like. You **should** run them whenever you have made significant local changes. Especially if you want to submit a pull request, of course.

If you don't take direct advantage of the virtualenv setup information (`dev_setup.sh` and `requirements.txt`), you can still use them as a guide to ensuring that you know which needed versions and libraries to install. The only dependency for running the tests should be `lettuce` itself.

Lettuce is extremely simple to run. From the pywqp root:
<pre>
<tt>cd tests
lettuce</tt>
</pre>


Its output is pretty straightforward to read, too.
