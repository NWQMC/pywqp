import utils
import os.path

util = utils.Utils()

# make sure we have the target directory in our sys.path
util.setup_target_path()

import pywqp_client as cli


rest_client = cli.RESTClient()
stash_path = "../scratch/bare.csv"
stash_path = os.path.abspath(stash_path)

# statecode=US%3A55&countycode=US%3A55%3A015&characteristicName=pH

calumet_yo = {'statecode': 'US:55', 'countycode': 'US:55:015', 'characteristicName': 'pH'}

# download the file
response = rest_client.make_wqp_request('get', 'http://wqp-test.er.usgs.gov', 'result', calumet_yo, mime_type='text/csv')

print(response.url)


print(response.status_code)
for header in response.headers:
    print(header + ': ' + response.headers[header])


print
# write the file to disk as regular csv
print('writing bare CSV to disk as bare.csv...')
rest_client.stash_response(response, stash_path, as_hdf5=False, raw_http=False)
print('...done')
print

# write the file to disk as csv.http
print('writing entire message to disk as bare.csv.http...')
rest_client.stash_response(response, stash_path, as_hdf5=False, raw_http=True)
print('...done')
print

# write the file to disk as a bare HDF5
print('writing HDF5 to disk as bare.h5...')
rest_client.stash_response(response,stash_path, as_hdf5=True, raw_http=False)
print('...done')
print

# read the HDF file back into a CSV


print
print('ok')
