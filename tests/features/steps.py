from lettuce import *
import sys, os, inspect
import requests

#wqp-job-id: 2545
#stewards-site-count: 5
#nwis-site-count: 107
#access-control-expose-headers: Total-Result-Count, Total-Site-Count
#content-length: 0
## NOT PRESENT server: Apache-Coyote/1.1
#total-site-count: 374
#date: Wed, 16 Jul 2014 18:59:58 GMT
#access-control-allow-origin: *
#storet-site-count: 262

# This is ONLY for head. It checks with premade literals in case the problem is in
# the actual HTTP header content, or in the datatype of the regex extracted parameter.
# Because this blows goats.
@step(u'And I should see all the HEAD things')
def all_the_things(step):
    copy = []
    for headername in world.response.headers.keys():
        copy.append(str(headername))
    assert len(copy) > 0
    assert len(copy) > 1
    assert len(copy) > 2
    assert len(copy) > 3
    #assert len(copy) > 4
    #assert len(copy) > 5
    #assert len(copy) > 6
    #assert len(copy) > 7
    #assert len(copy) > 8
    assert 'date' in copy
    #assert 'access-control-expose-headers' in copy
    #assert 'access-control-allow-origin' in copy
    assert 'content-length' in copy
    assert 'wqp-job-id' in copy
    assert 'total-site-count' in copy
    assert 'nwis-site-count' in copy
    assert 'storet-site-count' in copy
    assert 'stewards-site-count' in copy


@step(u'Given WQPServer = "([^"]*)"')
def wqp_server(step, wqpserver):
    world.wqpserver = wqpserver

@step(u'And countrycode = "([^"]*)"')
def set_countrycode(step, countrycode):
    world.countrycode = countrycode

@step(u'And statecode = "([^"]*)"')
def set_statecode(step, statecode):
    world.statecode = statecode

@step(u'And countycode = "([^"]*)"')
def set_countycode(step, countycode):
    world.countycode = countycode

@step(u'And characteristicName = "([^"]*)"')
def set_characteristicname(step, characteristic):
    world.characteristic = characteristic

@step(u'And I want to search for "([^"]*)" data')
def set_resourcetype(step, resourcetype):
    world.resourcetype = resourcetype

@step(u'And I want it as "([^"]*)"')
def set_contenttype(step, contenttype):
    world.contenttype = contenttype


@step(u'When I send a "([^"]*)" request')
@step(u'When I query WQP with a "([^"]*)" request')
def make_call(step, verb):

    verb = verb.lower()

    # use pywqp_client
    setup_target_path()
    import pywqp_client
    client_instance = pywqp_client.RESTClient()

    # params
    params = {}
    params['countrycode'] = world.countrycode
    params['statecode'] = world.statecode
    params['countycode'] = world.countycode
    params['characteristicName'] = world.characteristic

    # default
    contenttype = 'text/csv'
    if hasattr(world, 'contenttype'):
        contenttype = world.contenttype
        

    # make_wqp_request(self, verb, host_url, resource_label, parameters, mime_type='text/csv'):
    world.response = client_instance.make_wqp_request(verb, world.wqpserver, world.resourcetype, params, contenttype)
 
@step(u'Then I should receive a "([^"]*)" status')
def status_code(step, code):
    assert world.response.status_code == int(code)

@step(u'And I should see a total-"([^"]*)" greater than 0')
def and_i_should_see_a_count_header_greater_than_0(step, header_part):
    header_name = 'total-' + header_part
    assert header_name in world.response.headers
    assert world.response.headers[header_name] > 0

@step(u'And I should see a "([^"]*)" header')
def and_i_should_see_a_header_named(step, header_name):
    assert header_name in world.response.headers

@step(u'And total-"([^"]*)" should equal the sum of all contributing counts')
def and_total_resource_count_should_equal_the_sum_of_all_contributing_counts(step, resource_type):
    total = int(world.response.headers['total-' + resource_type])
    contrib = 0
    prefixes = ('nwis-', 'storet-', 'stewards-')
    for prefix in prefixes:
        if prefix + resource_type in world.response.headers:
            contrib += int(world.response.headers[prefix + resource_type])
    assert contrib == total

@step(u'And the messagebody should contain as many data rows as the total-site-count reported in the header')
def messagebody_to_header_consistency_check(step):
    assert False, 'This step must be implemented'
@step(u'Given that I have downloaded WQP data in CSV form')
def given_that_i_have_downloaded_wqp_data_in_csv_form(step):
    assert False, 'This step must be implemented'
@step(u'When I stash that data to disk using pywqp')
def when_i_stash_that_data_to_disk_using_pywqp(step):
    assert False, 'This step must be implemented'
@step(u'Then I should see the file on disk with the same byte size as the downloaded file')
def then_i_should_see_the_file_on_disk_with_the_same_byte_size_as_the_downloaded_file(step):
    assert False, 'This step must be implemented'



# ----------------- supporting functions -------------

def setup_target_path():
    '''
    This method modifies sys.os.path to include ../pywqp, where the tested
    resources live.
    '''
    # TODO fix this to accept arbitrary path and to do sanity check
    # establish reference to directory that contains what we're testing,
    # and put that into the classpath
    currframe = inspect.currentframe()
    myfile = os.path.abspath(inspect.getfile(currframe))
    # must get rid of frame reference to avoid nasty cycles
    del currframe
    # go up two directories and then descend into pywqp directory, which is where 
    # the modules we want to test are dwelling. 
    mydir = os.path.split(myfile)[0]
    myparent = os.path.split(mydir)[0]
    mygrandparent = os.path.split(myparent)[0]
    victim_folder = os.path.join(mygrandparent, 'pywqp')

    # add victim_folder to path for imports
    if victim_folder not in sys.path:
        sys.path.insert(0, victim_folder)
