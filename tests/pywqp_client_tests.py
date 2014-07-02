import sys, os, inspect

# establish reference to directory that contains what we're testing,
# and put that into the classpath
currframe = inspect.currentframe()
myfile = os.path.abspath(inspect.getfile(currframe))
# must get rid of frame reference to avoid nasty cycles
del currframe
#print('myfile:')
#print(myfile)
mydir = os.path.split(myfile)[0]
#print('mydir:')
#print(mydir)
topdir = os.path.split(mydir)[0]
#print('topdir:')
#print(topdir)
victim_folder = os.path.join(topdir, 'pywqp')
#print('victim_folder')
#print(victim_folder)

if victim_folder not in sys.path:
    sys.path.insert(0, victim_folder)

#print sys.path
    
import pywqp_client
client_instance = pywqp_client.RESTClient() 

# test resource_type(self, label)
print('TESTING resource_type(label)')
resource_types = {}
resource_types['station'] = '/Station/search'
resource_types['result'] = '/Result/search'
resource_types['simplestation'] = '/simplestation/search'
resource_types['bio'] = '/biologicalresult/search'

for label in resource_types:
    print('\tlabel "' + label + '"')
    assert client_instance.resource_type(label) == resource_types[label]
print("PASS")
print


# test wqp_request(self, verb, host_url, resource_label,  parameters)
print('TESTING wqp_request(verb, host_url, resource_label,  parameters)')
host_url = 'http://cida-eros-wqpdev.er.usgs.gov:8080/qw_portal_core'
print('host_url: "' + host_url + '"')
resource_label = 'station'
print('resource_label = "' + resource_label + '"')

print('Boone County, Iowa')
parameters = {'countrycode': 'US', 'statecode': 'US:19', 'countycode': 'US:19:015'}

print
verb = 'head'
print('Using verb = "HEAD"')
response = client_instance.wqp_request(verb, host_url, resource_label,  parameters)
print(str(response.status_code) + ' ' + response.reason)
for header in response.headers:
    print('\t' + header + ': ' + response.headers[header])

print
print
verb = 'get'
print('Using verb = "GET"')
response = client_instance.wqp_request(verb, host_url, resource_label,  parameters)
print(str(response.status_code) + ' ' + response.reason)
for header in response.headers:
    print('\t' + header + ': ' + response.headers[header])

print
# test serialize_http_response_head(self, response)
print('TESTING serialize_http_response_head(response), using previous response')
http_head= client_instance.serialize_http_response_head(response)
print(http_head)
print('-----------------------------------')

print
#startline test stash_response(self, response, filename, fileformat)
print('TESTING stash_response(response, filename, fileformat), using previous response')
# we need to ensure the filepathname is absolute
filename = mydir + '/sandbox/client_test'
fileformat = 'xml'
client_instance.stash_response(response, filename, fileformat)
