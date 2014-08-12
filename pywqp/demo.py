import pandas
import wqx_mappings
import pywqp_client
import lxml.etree as ET

print('obtaining station dataframe')
wqx = wqx_mappings.WQXMapper()
frame = wqx.empty_station_dataframe()
print(frame)

print('instantiating REST client')
client_instance = pywqp_client.RESTClient()

print('getting WQX data')
verb = 'get'
host_url = 'http://waterqualitydata.us'
resource_label = 'station'
params = {'countrycode': 'US', 'statecode': 'US:55', 'countycode': 'US:55:015', 'characteristicName': 'pH'}
result = client_instance.request_wqp_data(verb, host_url, resource_label, params, mime_type='text/xml')

print('parsing WQX data')
dom = ET.fromstring(result.content)
rownodes = dom.findall(wqx.station_row_nodepath())

for node in rownodes:
    print(rownode.tag)

#xslt = ET.parse(xsl_filename)
#transform = ET.XSLT(xslt)
#newdom = transform(dom)
#print(ET.tostring(newdom, pretty_print=True))


