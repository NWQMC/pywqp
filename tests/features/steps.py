from lettuce import *
import sys, os, os.path, inspect
import requests
import xml.etree.ElementTree as et


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
        
    world.response = client_instance.request_wqp_data(verb, world.wqpserver, world.resourcetype, params, contenttype)
 

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


@step(u'And the XML messagebody site count should match the total-site-count header')
def xml_messagebody__to_header_consistency_check(step):
    tree = et.ElementTree()
    root = et.fromstring(world.response.text)
    stations = root.findall('.//{http://qwwebservices.usgs.gov/schemas/WQX-Outbound/2_0/}MonitoringLocation')
    headertotal = int(world.response.headers['total-site-count'])
    assert headertotal == len(stations)


@step(u'And the messagebody should contain as many data rows as the total-site-count reported in the header')
@step(u'the messagebody data row count should match the total-site-count header')
def messagebody_to_header_consistency_check(step):
    headertotal = int(world.response.headers['total-site-count'])
    # csv
    bodytotal = world.response.text.count('\n')
    assert headertotal == bodytotal


@step(u'Given that I have downloaded WQP data in "([^"]*)" form')
def i_have_downloaded_csv(step, content_type):
    assert world.response.headers['content-type'] == content_type


@step(u'Given that I have downloaded WQP data')
@step(u'And I have retained a copy in memory')
def i_have_downloaded(step):
    assert hasattr(world.response, 'text')
    assert len(world.response.text) > 0


@step(u'When I stash that data to disk using pywqp')
def stash_data_to_disk(step):
    world.stashfile_pathname = os.path.abspath('scratch/bare.csv')
    #remove any preexisting/leftover stash instance with this file pathname
    if os.path.exists(world.stashfile_pathname):
        os.remove(world.stashfile_pathname)

    import pywqp_client
    client_instance = pywqp_client.RESTClient()
    client_instance.stash_response(world.response, world.stashfile_pathname)
    assert os.path.exists(world.stashfile_pathname)


@step(u'Then I should see the file on disk with the same byte size as the downloaded file')
def crosscheck_stashfile_size(step):
    stashlen = os.path.getsize(world.stashfile_pathname)
    memlen = len(world.response.text)
    assert(stashlen == memlen)


@step(u'And I have stashed that data on disk using pywqp')
def data_is_stashed(step):
    assert os.path.exists(world.stashfile_pathname)


@step(u'When I read the data from disk')
def when_i_read_the_data_from_disk(step):
    #stashfile = 
    stashfile = open(world.stashfile_pathname, 'r')


@step(u'Then the two CSV files should contain the same number of rows')
def same_number_of_rows(step):
    stashfile = open(world.stashfile_pathname, 'r')
    data = stashfile.read()
    assert data.count('\n') == world.response.text.count('\n')


@step(u'When I stash the entire message to disk using pywqp')
def stash_message_to_disk(step):
    world.stashfile_pathname = os.path.abspath('scratch/bare.csv')
    #remove any preexisting/leftover stash instance with this EXPECTED file pathname
    expected_stashfile_pathname = world.stashfile_pathname + '.http'
    if os.path.exists(expected_stashfile_pathname):
        os.remove(expected_stashfile_pathname)

    import pywqp_client
    client_instance = pywqp_client.RESTClient()
    client_instance.stash_response(world.response, world.stashfile_pathname, raw_http=True)
    # did the method call do the expected thing?
    assert os.path.exists(expected_stashfile_pathname)


@step(u'And I have stashed the message on disk using pywqp')
@step(u'Then I should see the filepath on disk with appended http suffix')
def verify_modified_pathname(step):
    assert os.path.exists(world.stashfile_pathname + '.http')


@step(u'Then the status line should be present')
def then_the_status_line_should_be_present(step):
    expected_stashfile_pathname = world.stashfile_pathname + '.http'
    stashfile = open(expected_stashfile_pathname, 'r')
    world.lines = stashfile.readlines()
    assert str(world.response.status_code) in world.lines[0]


@step(u'And the headers should be present')
def and_the_headers_should_be_present(step):
    for headerline in  world.lines[1: len(world.response.headers)]:
        written_name = headerline[:headerline.find(':')]
        assert written_name in world.response.headers.keys()


@step(u'And there should be a blank line before the messagebody')
def verify_header_terminator(step):
    assert world.lines[len(world.response.headers) + 1] == '\n'



@step(u'Given "([^"]*)" as the authoritative XML-to-column definition')
def given_group1_as_the_authoritative_xml_to_column_definition(step, group1):
    # use wqx_mappings (probably going to have to shift this to setup code...)
    setup_target_path()
    import wqx_mappings
    world.mapper = wqx_mappings.WQXMapper()
    
    if not 'wqx_mappings.' in group1:
        raise BaseException('Fix this step definition. It was written under outdated assumptions.')
    else:
        world.column_mappings = eval(group1)

    assert(len(world.column_mappings) > 0)


@step(u'And "([^"]*)" as the authoritative context node descriptor')
def and_group1_as_the_authoritative_context_node_descriptor(step, group1):
    import wqx_mappings
    
    if not 'wqx_mappings.' in group1:
        raise BaseException('Fix this step definition. It was written under outdated assumptions.')
    else:
        world.context_descriptors = eval(group1)

    assert(len(world.context_descriptors) > 0)

@step(u'And "([^"]*)" as the authoritative tabular format descriptor')
def and_group1_as_the_authoritative_tabular_format_descriptor(step, group1):
    import wqx_mappings
    if not 'wqx_mappings.' in group1:
        raise BaseException('Fix this step definition. It was written under outdated assumptions.')
    else:
        world.tabular_defs = eval(group1)

    assert('station' in world.tabular_defs)
    assert('result' in world.tabular_defs)


@step(u'And "([^"]*)" as the definitive context node XPath expression set')
def and_group1_as_the_definitive_context_node_xpath_expression_set(step, group1):
    import wqx_mappings
    if not 'wqx_mappings.' in group1:
        raise BaseException('Fix this step definition. It was written under outdated assumptions.')
    else:
        world.context_xpaths = eval(group1)
    assert(len(world.context_xpaths) > 0)


@step(u'And "([^"]*)" as the definitive column value XPath expression set')
def and_group1_as_the_definitive_column_value_xpath_expression_set(step, group1):
    import wqx_mappings
    if not 'wqx_mappings.' in group1:
        raise BaseException('Fix this step definition. It was written under outdated assumptions.')
    else:
        world.val_xpaths = eval(group1)
    assert(len(world.val_xpaths) > 0)

@step(u'Then the tabular format descriptor must be contained in the XML-to-column definition')
def then_the_tabular_format_descriptor_must_be_contained_in_the_xml_to_column_definition(step):
    import wqx_mappings
    for table_type in wqx_mappings.tabular_defs:
        world.context_descriptor_exclusions = wqx_mappings.context_descriptor_exclusions
        valid_mappings = get_valid_column_nappings(table_type, world)
        valid_uniques = set(valid_mappings.values())

        # no duplicates permitted! 
        valid_uniques = set(valid_mappings.values())
        assert(len(valid_mappings.values()) == len(valid_uniques))

        # same number of column names as corresponding tabular def
        assert(len(valid_uniques) == len(wqx_mappings.tabular_defs[table_type]))

        # there must be a one-to-one mapping with tabular def
        for colname in wqx_mappings.tabular_defs[table_type]:
            assert(colname in valid_uniques)
            
            

@step(u'And the column value XPath expression set must be contained in the XML-to-column definition')
def and_the_column_value_xpath_expression_set_must_be_contained_in_the_xml_to_column_definition(step):
    # this test step verifies that 
    for nodename in world.val_xpaths.keys():
        for colname in world.val_xpaths[nodename]:
            print('colname:' + colname)
            segments = world.val_xpaths[nodename][colname].split('/')
            # strip out namespace aliases (not too perfect, but this is not a perfect test)
            segments = [segment.split(':')[1] for segment in segments]
            print('/'.join(segments))
            #assert(world.xml2coldef[colname][- len(segments):] == segments)



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


def get_valid_column_nappings(table_type, world):
    # To say the tabular_format_descriptor must be "contained in" the xml_to_column_definition
    # means the following:
    # 1. For each tabular type (currently, "station" or "result") in the 
    #   tabular_format_descriptor:
    # 2. The subset ofxml_to_column_definition entries is established by the 
    #   presence of the corresponding context node path in the ancestor chain 
    #   of the xml_to_column_definition key. 
    #   (Less formally: the xml_to_column_definition entry must be applicable to the kind 
    #   of table being defined. Result paths don't matter for station tables, and vice versa.))
    # 3. For each column name in the tabular_format_descriptor for the given 
    #   tablular type, there must exist exactly one equal value in the mapping subset
    #   defined as desribed in the previous step.

    import wqx_mappings
    valid_mappings = {}
    for candidate_path in world.column_mappings:
        excluded = False
        for exc_name in world.context_descriptor_exclusions[table_type]:
            exc_path = world.context_descriptors[exc_name]
            if exc_path in candidate_path:
                excluded = True
                break
        if not excluded:
            valid_mappings[candidate_path] = world.column_mappings[candidate_path]

    return valid_mappings
