import sys
import pywqp_client
import pywqp_validator


client = pywqp_client.RESTClient()
validator = pywqp_validator.WQPValidator()

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

response = client.wqp_request('get', host_url, resource_label,  allparams)

print('response received BACK HERE.')
print(response.url)
print(client.serialize_http_msg_head(response))

client.stash_response(response, '../../demo', 'xml')
