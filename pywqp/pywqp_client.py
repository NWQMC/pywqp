import requests
import sys
import os.path
import pandas as pd
import StringIO

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


    def make_wqp_request(self, verb, host_url, resource_label, parameters, mime_type='text/csv'):
        '''
        "verb" must be "get" or "head". This client doesn't support any other 
        HTTP methods.
        "host_url" needs to include context as necessary (including port number). 
        "resource_label" is one of "station", "result", "simplestation", or "bio".
        "parameters" is a list or tuple of wqp parameters, assumed to have already
        been validated.
        "mime_type" can be "text/xml" or "text/csv"
        '''

        if verb not in ('head', 'get'):
            raise('''This function does not handle any HTTP Methods 
                except "get" and "head".''')

        request_url = host_url + self.resource_type(resource_label)

        mime_types = {'text/xml': 'xml', 'text/csv': 'csv'}

        # baked-in parameters
        # TODO should transfer as zipped (for efficiency on the wire) 
        # and then unzip in client. Keeping it simple for now.
        translated_mime_type = mime_types[mime_type]
        if not translated_mime_type:
            translated_mime_type = 'csv'
        baked = {'mimeType': translated_mime_type, 'zip': 'no'}
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
    
    def stash_response(self, response, filepathname, as_hdf5=True, raw_http=False):
        '''
        This function saves the requests.response parameter as a file.
        The "filepathname" argument must be an absolute system filepath.
        "as_hdf5" is a boolean directive: 
            If True, the filename extension will be ".h5", and the file
                will be written to disk by pandas's serialization for HDF5.
            If False, the native mime type of the response (as determined
                by the http "mime-type" header) will determine the filename
                extension, and the messagebody will be written directly to 
                the file.
        "raw_http" is a boolean stating whether to write the full 
            HTTP response: start-line, headers, an empty line, and 
            message body, when the file is stashed (this feature is
            intended primarily for troubleshooting network/HTTP issues.)
            If True, ".http" will be appended to the filename
        '''

        mimetype = response.headers['content-type']
        if mimetype:
            # extract the mime subtype ('xml', 'csv' etc)
            mimetype = mimetype.split('/')[-1]
            if ';' in mimetype:
                mimetype = mimetype.split(';')[0]


        fileformat = ''
        if as_hdf5:
            fileformat = 'h5'
        elif mimetype:
            fileformat = mimetype
        else:
            #desperate default
            fileformat = 'txt'
                        
        # the filepathname must be a full filepath
        if filepathname.startswith('/'):
            filepathname = os.path.abspath(filepathname)
            if not os.path.exists(os.path.split(filepathname)[0]):
                os.makedirs(os.path.split(filepathname)[0])
        else:
            raise(BaseException('The filepathname parameter "' 
                    + filepathname + '" is not an absolute path.'))

        if not filepathname.endswith(fileformat):
            filepathname += '.' + fileformat

        if raw_http:
            filepathname += '.http'

        if as_hdf5:
            # we need a dataframe
            dataframe = ''
            if mimetype == 'csv':
                dataframe = pd.read_csv(StringIO.StringIO(response.content))
            # TODO what about non-CSV content types?

            # Write the resulting dataframe to the hdf5 file
            dataframe.to_hdf(filepathname, 'table', mode='w')
        else:
            # NOT hdf5
            writefile = open(filepathname, 'w')

            if raw_http:
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

    def read_stashed_data(self, filepath):
        '''
        Returns a read-only copy of the stashed data. If it's an HDF5
        dataframe, the returned value is a pandas dataframe. Otherwise,
        it's just an ordinary file handle in read mode.
        '''
        # the filepathname must be a full filepath
        if filepathname.startswith('/'):
            filepathname = os.path.abspath(filepathname)
        else:
            raise(BaseException('The filepathname parameter "' 
                    + filepathname + '" is not an absolute path.'))
        if not os.path.isfile(filepath):
            raise(BaseException('The filepathname parameter "'
                    + filepathname + '" does not identify an actual file.'))

            if filepathname.endswith('h5'):
                # HDF5 dataframe
                dataframe = pd.read_hdf(filepathname, 'table', mode='r')
                return dataframe
            else:
                file = open(filepathname, 'r')
                return file


