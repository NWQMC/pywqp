import sys
import pywqp_client
import pywqp_validator


usage = '''
    All parameters are named. The valid parameters are:

    paramfile (a local filesystem reference to a parameter list);

    wqpResourceType (a REQUIRED parameter with the following valid values:
        - station
        - result
        - simple_station
        - biological_result)

    providers (an OPTIONAL (default = all) tuple of providers 
        to which the query will be restricted);

    GEOSPATIAL CONSTRAINTS:
        [NOTE: all latitude and longitude values are expressed decimally
            as per WGS84.]
    bBox (a bounding box whose top and bottom are geographical parallels of 
        latitude, and whose sides are meridians of longitude.)
    lat, lon, within (three separate arguments that together represent
        a circle on the Earth's surface. 'lat' and 'long' define the circle's
        center; 'within' is a radius expressed in decimal miles.

    POLITICAL JURISDICTION:
        [NOTE: countrycode, statecode, and countycode are interdependent
            and are required to be consistent. All codes are based on the US
            FIPS Publications.]
    countrycode 
    statecode
    countycode

    SITE CONSTRAINTS:
    organizationId (ID of the administrative subunit responsible for maintenance
        and sampling activities at the site)
    siteType
    siteId (the site identifier used by the owning organization, appended to
        the organization's designation, with a single hyphen discriminator.)
    huc (Hydrologic Unit Code as maintained by USGS)

    SAMPLING CONSTRAINTS:
    activityId (the project or program responsible for a specific sampling
        effort)
    startDateLo ("startDate" means the first date of a particular collection
        effort, which is applied to all samples obtained by that effort.
        In effect, it's a kind of nominal date for the entire effort, rather
        than the date on which a particular measurement is actually obtained.)
    startDateHi
    sampleMedia
    characteristicType
    characteristicName
    pCode (An NWIS-only classification scheme for sampling procedures. Non-NWIS
        data sources will not provide any results to a query with a pCode
        parameter.)
    analyticalMethod (a classification of analytical protocols curated by NEMI
        (see [http://www.nemi.gov] for more information.) The value of this
        parameter is a published NEMI URI, _fully urlencoded_.)
'''

# paramfile is a listing of parameters, one per line, empty lines ignored.
# each parameter is of the form <name>=<value> (no angle brackets in real life.)
client = pywqp_client.RESTClient()
validator = pywqp_validator.WQPValidator()


def paramfile_args(filename):
    paramfile = open(filename, 'r')
    paramexprs = paramfile.readlines()
    paramfile.close()
    retval = {}
    for paramexpr in paramexprs:
        # process commandline arguments
        #if sys.argv['paramfile']:
            # do nothing so far
        if sys.argv['wqpResourceType']:
            for label in ('station', 'result', 'simplestation', 'bio'):
                print('label: ' + label + '; response type path: ' + client.resource_type(label))
        print(validator.param_names())

print
print('STATIC REQUEST')

# Boone County, IA

boone_county = {'countrycode': 'US', 'statecode': 'US:19', 'countycode': 'US:19:015'}
base = {'mimeType': 'xml', 'zip': 'no'}
print('boone_county:')
print(boone_county)
print('base:')
print(base)

allparams = boone_county
print('allparams:')
print(allparams)

allparams.update(base)
print('allparams after:')
print(allparams)

host_url = 'http://www.waterqualitydata.us'
resource_label = 'station'

full_path = host_url + client.resource_type(resource_label)
print('FULL URL: ' + full_path)
print('PARAMS:')
print(allparams)

print

response = client.make_wqp_request('get', host_url, resource_label,  allparams)

print('response received BACK HERE.')
print(response.url)
print(client.serialize_http_response_head(response))

client.stash_response(response, '../../demo', 'xml')
