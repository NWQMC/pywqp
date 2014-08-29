from lxml import etree
import inspect
import os, os.path, sys
import pandas

# ANSI formatting codes
color_red = '\033[31m'
color_green = '\033[32m'
color_yellow = '\033[33m'
color_blue = '\033[34m'
color_magenta = '\033[35m'
color_cyan = '\033[36m'
color_stop = '\033[0m'


# ensure that the thing we are importing is available:
# THIS SCRIPT IS RUN FROM A SIBLING DIRECTORY
currframe = inspect.currentframe()
myfile = os.path.abspath(inspect.getfile(currframe))
# must get rid of frame reference to avoid nasty cycles
del currframe
# go up one directory and then descend into pywqp directory, which is where 
# the modules we want to test are dwelling. 
mydir = os.path.split(myfile)[0]
myparent = os.path.split(mydir)[0]
victim_folder = os.path.join(myparent, 'pywqp')

# add victim_folder to path for imports
if victim_folder not in sys.path:
    sys.path.insert(0, victim_folder)

import wqx_mappings
mapper = wqx_mappings.WQXMapper()

print('imports done')


# parsing a local WQX XML file
# (any valid filepath)
#tree = etree.parse('../sandbox/boone_nwis_result_baseline.xml')
tree = etree.parse('../sandbox/boone_stations_baseline.xml')
root = tree.getroot()
print('parsed')


# testing wqx_mapper orgs.nodeq precompiled query
orgsfinder = mapper.context_xpaths_compl['org']
orgsfinder_orgs = orgsfinder(root)
print('orgsfinder finds ' + str(len(orgsfinder_orgs)) + ' orgs.')

# what row out of the dataset to we want to compare?
row_to_display = 200

# what kind of dataset is it? 'result' or 'station'?
table_type = 'station'

# testing wqx_mapper xml_to_dict_of_lists
print('\n\n' + color_cyan + 'DATASTRUCTURE FROM xml_to_dict_of_lists: row ' + str(row_to_display) + color_stop)
datadict1 = mapper.xml_to_dict_of_lists(table_type, root)
if datadict1:
    count = 0
    for rownum in range(row_to_display, row_to_display + 1):
        print('\n\n slice of all columns at row #' + str(rownum))
        for colname in wqx_mappings.tabular_defs[table_type]:
            # print the rowcount, just once
            if not count:
                count = len(datadict1[colname])
                print(str(count) + ' rows in datadict1 elements')
            # print the row
            print(colname + ': ' + color_cyan + datadict1[colname][rownum] + color_stop)
    print(color_cyan + 'done with xml_to_list_of_dicts: row ' + str(row_to_display) + color_stop)


# testing wqx_mapper xml_to_list_of_dicts
print('\n\n' + color_magenta + 'DATASTRUCTURE FROM xml_to_dict_of_lists: row ' + str(row_to_display) + color_stop)
datadict2 = mapper.xml_to_list_of_dicts(table_type, root)
if datadict2:
    row = datadict2[row_to_display]
    for colname in wqx_mappings.tabular_defs[table_type]:
        print(colname + ': ' + color_magenta + row[colname] + color_stop)
    print(color_magenta + 'done with xml_to_dict_of_lists: row ' + str(row_to_display) + color_stop)
        

# make pandas.DataFrame from one of the data structures (columns-first)
if datadict1:
    dataframe = pandas.DataFrame(data=datadict1, columns=wqx_mappings.tabular_defs[table_type])
    print('\n\n' + color_green + 'calling dataframe irow(' + str(row_to_display) + ')' + color_stop)
    print(dataframe.irow(row_to_display))
    print(color_green + 'done with irow(' + str(row_to_display) + ')' + color_stop)

# make pandas.DataFrame from the other data structure (rows-first)
if datadict2:
    dataframe = pandas.DataFrame(data=datadict2, columns=wqx_mappings.tabular_defs[table_type])
    print('\n\n' + color_yellow + 'calling dataframe irow(' + str(row_to_display) + ')' + color_stop)
    print(dataframe.irow(row_to_display))
    print(color_yellow + 'done with irow(' + str(row_to_display) + ')' + color_stop)

print('\n\n')

# use pywqp_client to fetch as dataframe
import pywqp_client
client_instance = pywqp_client.RESTClient()

# params
params = {}
params['countrycode'] = 'US'
params['statecode'] = 'US:55'
params['countycode'] = 'US:55:015'
params['characteristicName'] = 'pH'

response = client_instance.request_wqp_data('get', 'http://www.waterqualitydata.us', 'station', params, 'text/xml')

print('status code: ' + str(response.status_code))
for header in response.headers:
    print(header + ': ' + response.headers[header])

print

dataframe = client_instance.response_as_pandas_dataframe(response)

print(dataframe)

print
