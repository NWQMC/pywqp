pywqp
=====

A generic scriptable Python client for downloading datasets from the USGS/EPA Water Quality Portal: an alternative to manual use of the website at http://www.waterqualitydata.us.

The project consists of the following components:

-  A client module, `pywqp_client.py`, which obtains WQP data in CSV format or in native Water Quality XML. This is suitable for inclusion in Python programs.

-  A convenience wrapper, `pywqp.py`, which manages common query-and-convert actions from the command line. When it is invoked, the commandline parameters are sent to an instance of pywqp_client.py.

-  A parameter validation module, `pywqp_validator.py`

<br/>
### Quick answers
[How Do I download WQP data from my Python program?](https://github.com/wblondeau-usgs/pywqp/blob/master/README.md#downloading-wqp-data-with-request_wqp_data)

[How do I convert my download to a pandas dataframe?](#converting-wqp-response-data-to-a-pandas-dataframe-with-response_as_pandas_dataframe)

[How do I stash my download to the local filesysstem?](https://github.com/wblondeau-usgs/pywqp/blob/master/README.md#stashing-wqp-response-data-to-your-local-machine-with-stash_response)


<br/>
### Using <tt>pywqp-client.py</tt> in your Python program
The core resource of `pywqp_client` is the class `RESTClient`. The pattern of usage is fairly simple:
<pre>
<tt>import pywpq_client
client_instance = pywqp_client.RESTClient()</tt>
</pre>

**Note** that you will need to ensure that the `pywqp` folder is in your system path, or else you will not be able to `import pywqp_client`.

`client_instance` is now ready to run any of the functions exposed by `RESTClient`. Examples of the important ones are shown below. In all cases, the example name "client_instance" is reused for simplicity, but that name has no particular significance. Name your objects as you wish.

<br/>
#### Downloading WQP Data with <tt>request_wqp_data</tt>

This function makes a call to the Water Quality Portal server specified in the `host_url` argument. The other arguments are as follows:

 - `verb`: a literal String representing the HTTP method of the Request. This method accepts only `'get'` or `'head'`: WQP currently doesn't support any other HTTP methods.

 - `resource_label`: an identifier for the kind of data being requested. The defined labels are the keys of the `RESTClient.resource_types` Dictionary. At time of writing, these are supported:

  - `'station'`: Station, or "Site", data refers to locations at which sampling is deemed to have occurred.

  - `'result'`: Result data refers to actual measurements. Will always include Station, date/time of observation, and metadata about the observation event.

  - `'simplestation'`: a very small subset of Station information, used mostly for interaction with geospatial systems.


 - `params` is a Dictionary containing [WQP REST parameters](http://www.waterqualitydata.us/webservices_documentation.jsp#WQPWebServicesGuide-RestParamDefs). 

 - There is one standard WQP parameter which is **not** recognized in `params`: `mimeType`. This one is given its own Python parameter, `mime-type`, because currently pywqp supports only CSV and XML download formats. There are only two accepted values for this parameter:

  - `'text/xml'`

  - `'text/csv'` (which is the default value if this parameter is omitted.)

<br/>
##### Example: downloading CSV data for Stations in Boone County, Iowa, US that have made pH observations.
<pre>
<tt>verb = 'get'
host_url = 'http://waterqualitydata.us'
resource_label = 'station'
params = {'countrycode': 'US', 'statecode': 'US:19', 'countycode': 'US:19:015', 'characteristicName': 'pH'}
result = client_instance.request_wqp_data(verb, host_url, resource_label, parameters, mime_type='text/csv')</tt>
</pre>

<br/>
##### Troublesooting help: getting an equivalent REST query URL
When working with a module like pywqp, it's often very helpful to be able to produce a query that duplicates the one being issued by the module.  The duplicate query can be run independently though a utility such as curl (or a browser, as long as the browser handles outbound query parameter urlencoding correctly.)

pywqp provides this via `create_rest_url`, a function that takes the same `host_url`, `resource_label`, `params`, and `mime_type` arguments that are made to a call to `request_wqp_data`. Instead of making a call to WQP and returning a `requests.response` object, `create_rest_url` returns a paste-ready URL that can be set from a different client.

<pre>
<tt>host_url = 'http://waterqualitydata.us'
resource_label = 'station'
params = {'countrycode': 'US', 'statecode': 'US:19', 'countycode': 'US:19:015', 'characteristicName': 'pH'}
equivalent_url = client_instance.create_rest_url(host_url, resource_label, params, mime_type='text/csv')
print(equivalent_url)</tt>
</pre>

will print

<pre>
<tt>http://waterqualitydata.us/Station/search?characteristicName=pH&mimeType=csv&zip=no&statecode=US%3A19&countrycode=US&countycode=US%3A19%3A015</tt>
</pre>

<br/>
##### When pywqp gets the HTTP Response from WQP

`request_wqp_data` returns a [`requests.response` object](http://docs.python-requests.org/en/latest/api/#requests.Response). `pywqp_client` lets you do two things with that `response`:

 - Convert the dataset to an in-memory pandas dataframe.

 - Stash the dataset on your local filesystem.

The next two examples show how to do those things.

<br/>
#### Converting WQP <tt>response</tt> Data to a <tt>pandas</tt> dataframe with <tt>response_as_pandas_dataframe</tt>

<br/>
##### Example:
<pre>
<tt>dataframe = client_instance.response_as_pandas_dataframe(response)</tt>
</pre>


<br/>
#### Stashing WQP <tt>response</tt> Data to your local machine with <tt>stash_response</tt>

Note that the `filepathname` argument can be either relative or absolute. If it's relative, the `stash_response` method will coerce it to an absolute based on the current directory. However, sometimes during Python execution the "current directory" is not obvious. Absolute `filepathnme`s are recommended. 

<br/>
##### Example:
<pre>
<tt>filepathname = '/home/whb/examples/wqp_example.csv'
client_instance.stash_response(response, filepathname)</tt>
</pre>

<br/>
##### Troubleshooting help: saving an entire HTTP message
As a convenience, pywqp also allows the storage of a complete HTTP response message, including status line and headers. This is done by setting the optional boolean parameter `raw_http=True`.
<pre>
<tt>filepathname = '/home/whb/examples/wqp_example.csv'
client_instance.stash_response(response, filepathname, raw_http=True)</tt>
</pre>

This will give you a file on disk that opens with content something like this:

<pre>
<tt>HTTP/1.1 200 OK
Date: Thu, 24 Jul 2014 15:42:52 GMT
NWIS-Site-Count: 49
Total-Site-Count: 203
STORET-Site-Count: 154
WQP-Job-ID: 14242
STEWARDS-Site-Count: 0
Access-Control-Allow-Origin: *
Access-Control-Expose-Headers: Total-Result-Count
Access-Control-Expose-Headers: Total-Site-Count
Content-Type: text/csv
</tt>
</pre>

<br/>
#### No direct HDF5 support
Note that stashing HTTP Response data to disk is a simple convenience to incorporate. On the other hand, pywqp does **not** support saving pandas dataframes to disk. If you're sufficiently advanced to do that, you probably already know how to use HDF5; if not, there are plenty of resources out there (e.g. [Python and HDF5](http://shop.oreilly.com/product/0636920030249.do)


<br/>
### Running the pywqp tests
The project also contains a BDD test suite written in [lettuce](http://lettuce.it/). This is located in the `tests` folder. Shocking, I know.

You can run pywqp's tests whenever you like. You **should** run them whenever you have made significant local changes. Especially if you want to submit a pull request, of course.

If you don't take direct advantage of the virtualenv setup information (`dev_setup.sh` and `requirements.txt`), you can still use them as a guide to ensuring that you know which needed versions and libraries to install. The only dependency for running the tests should be `lettuce` itself.

Lettuce is extremely simple to run. From the pywqp root:
<pre>
<tt>cd tests
lettuce</tt>
</pre>


Its output is pretty straightforward to read, too.
