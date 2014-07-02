import requests
import sys
import os.path

class RESTClient():

    resource_types = {}
    resource_types['station'] = '/Station/search'
    resource_types['result'] = '/Result/search'
    resource_types['simplestation'] = '/simplestation/search'
    resource_types['bio'] = '/biologicalresult/search'

    def resource_type(self, label):
        '''
        Returns the URL path fragment that will be appended to the application
        context in order to perform the kind of search identified by the
        named "label" argument. Returns an empty string if the label does
        not match a known search type.
        '''
        if label in self.resource_types:
            return self.resource_types[label]
        return ''


    def wqp_request(self, verb, host_url, resource_label,  parameters):
        '''
        "verb" must be "get" or "head". This client doesn't support any other 
        HTTP methods.
        "host_url" needs to include context as necessary (including port number). 
        "resource_label" is one of "station", "result", "simplestation", or "bio".
        "parameters" is a list or tuple of wqp parameters, assumed to have already
        been validated.
        '''

        if verb not in ('head', 'get'):
            raise('''This function does not handle any HTTP Methods 
                except "get" and "head".''')

        request_url = host_url + self.resource_type(resource_label)

        # baked-in parameters
        # TODO should transfer as zipped (for efficiency on the wire) 
        # and then unzip in client. Keeping it simple for now.
        baked = {'mimeType': 'xml', 'zip': 'no'}
        parameters.update(baked)

        if verb == 'head':
            response = requests.head(request_url, params=parameters)
        elif verb == 'get':
            response = requests.get(request_url, params=parameters)

        return response

    def serialize_http_response_head(self, response):
        retval = 'HTTP/1.1 ' + str(response.status_code) + ' ' + response.reason + '\n'
        for header in response.headers:
            retval += header + ':' + response.headers[header] + '\n'
        return retval
    
    def stash_response(self, response, filepathname, fileformat):
        '''
        This function saves the WQP response as a file containing the full 
        HTTP response: start-line, headers, an empty line, and the message
        body. If the "fileformat" parameter is 'hdf', it will discard the
        HTTP envelope and save an HDF5 file ready to go.
        The "filepathname" argument must be an absolute system filepath.
        '''

        if fileformat not in ('xml', 'hdf'):
            raise('''the only accepted fileformat values are 
                "xml" (for Water Quality XML in text/xml serialization)
                and "hdf" (for python-pandas HDF5).''')
        
        #TODO fix this
        if fileformat == 'hdf':
            raise('The HDF5 feature is not yet supported.')

        # the filepathname must be a full filepath
        if filepathname.startswith('/'):
            filepathname = os.path.abspath(filepathname)
            if not os.path.exists(os.path.split(filepathname)[0]):
                os.makedirs(os.path.split(filepathname)[0])
        else:
            raise('The filepathname parameter "' 
                    + filepathname + '" is not an absolute path.')

        if not filepathname.endswith(fileformat):
            filepathname += '.' + fileformat
        filepathname += '.http'

        writefile = open(filepathname, 'w')

        # replicate HTTP message
        writefile.write(self.serialize_http_response_head(response))
        writefile.write('\n')

        # write message body content, if any
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
                
        writefile.close()

