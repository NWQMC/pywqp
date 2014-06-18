import requests
import sys

class RESTClient():

    resource_types = {}
    resource_types['station'] = '/Station/search'
    resource_types['result'] = '/Result/search'
    resource_types['simplestation'] = '/simplestation/search'
    resource_types['bio'] = '/biologicalresult/search'

    def resource_type(self, label):
        if label in self.resource_types:
            return self.resource_types[label]
        return ''


    def wqp_request(self, verb, host_url, resource_label,  parameters):
        '''
        verb must be "get" or "head". This client doesn't support any other 
        HTTP methods.
        the host_url needs to include context as necessary. 
        resource_type is one of "station", "result", "simplestation", or "bio".
        parameters is a proper set of wqp parameters, assumed to have already
        been validated.
        '''

        if verb not in ('head', 'get'):
            raise('''This function does not handle any HTTP Methods 
                except "get" and "head".''')

        request_url = host_url + self.resource_type(resource_label)
        if verb == 'head':
            response = requests.head(request_url, params=parameters)
        elif verb == 'get':
            response = requests.get(request_url, params=parameters)
           

        return response

    def serialize_http_msg_head(self, response):
        retval = 'HTTP/1.1 ' + str(response.status_code) + ' ' + response.reason + '\n'
        for header in response.headers:
            retval += header + ':' + response.headers[header] + '\n'
        return retval
    
    def stash_response(self, response, filename, fileformat):

        if fileformat not in ('xml', 'hdf'):
            raise('''the only accepted fileformat values are 
                "xml" (for Water Quality XML in text/xml serialization)
                and "hdf" (for python-pandas HDF5).''')

        if not filename.endswith(fileformat):
            filename += '.' + fileformat + '.http'

        writefile = open(filename, 'w')

        #replicate HTTP message
        writefile.write(self.serialize_http_msg_head(response))
        writefile.write('\n')

        # write content, if any
        if 'transfer-encoding' in response.headers and response.headers['transfer-encoding'] == 'chunked':
            for chunk in response.iter_content():
                writefile.write(chunk)
        elif 'content-length' in response.headers and response.headers['content-length'] > 0:
            writefile.write(response.content)
        else:
            # it's possible the headers are screwed up and we still want content
            content = response.content
            if content:
                writefile.write(content)
                




