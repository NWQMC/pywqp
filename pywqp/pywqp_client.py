import requests
import sys
import os.path
import pandas as pd
import StringIO


class RESTClient():
    resource_types = {
        'station': '/Station/search',
        'result': '/Result/search',
        'simplestation': '/simplestation/search',
        'bio': '/biologicalresult/search'
    }

    '''
    The WQP interface includes a parameter called 'mimeType'. This dictionary
    ontains the subset of WQP mimeTypes supported in pywqp.
    Keys are the official [internet media type](http://en.wikipedia.org/wiki/Internet_media_type).
    Values are the corresponding WQP mimeType parameters.
    '''
    supported_mime_types = {
        'text/xml': 'xml', 
        'text/csv': 'csv'
    }

    def resource_type(self, label):
        """
        Returns the URL path fragment that will be appended to the application
        context in order to perform the kind of search identified by the
        named "label" argument. Returns an empty string if the label does
        not match a known search type.
        """
        if label in self.resource_types:
            return self.resource_types[label]
        return ''

    def request_wqp_data(self, verb, host_url, resource_label, parameters, mime_type='text/csv'):
        """
        This function sends a query to the WQP server, and returns a requests.response object.
        "verb" must be "get" or "head". This client doesn't support any other 
        HTTP methods.
        "host_url" needs to include context as necessary (including port number). 
        "resource_label" is one of "station", "result", "simplestation", or "bio".
        "parameters" is a list or tuple of wqp parameters, assumed to have already
        been validated.
        "mime_type" can be "text/xml" or "text/csv"
        """

        if verb not in ('head', 'get'):
            raise ('''This function does not handle any HTTP Methods
                except "get" and "head".''')

        request_url = host_url + self.resource_type(resource_label)

        # baked-in parameters
        # TODO should transfer as zipped (for efficiency on the wire) 
        # and then unzip in client. Keeping it simple for now.
        translated_mime_type = self.supported_mime_types[mime_type]
        if not translated_mime_type:
            # insert a reasonable default
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

    def get_content_type(self, response):
        """
        Returns the Internet Content type (a.k.a. "mimeType") of the response
        argument.
        If the response is empty or None, returns None.
        If the response does contain data, but this function cannot determine
        what the correct answer is, returns None. (For now.)
        """
        if not response:
            return None
        content_type = response.headers.get('content-type')
        if not content_type:
            # TODO: determine if there's any good default value
            # TODO: any worthwhile analysis/guessing algorithms?
            content_type = None
        return content_type

    def response_as_pandas_dataframe(self, response):
        """
        Converts the response's message body into a frame, which is returned.
        If the response is empty or None, returns None.
        """
        if not response:
            return None

        content_type = self.get_content_type(response)
        if not content_type:
            return None

        dataframe = None
        if content_type == 'text/csv':
            dataframe = pd.read_csv(StringIO.StringIO(response.content))
        elif content_type == 'text/xml' or content_type == 'application_xml':
            # TODO this needs to be fixed, but it's complicated...
            raise (BaseException, 'XML conversion to dataframe is not yet implemented.')
        return dataframe

    def stash_response(self, response, filepathname, raw_http=False):
        """
        This function saves the requests.response parameter as a file.
        "raw_http" is a boolean stating whether to write the full 
            HTTP response: start-line, headers, an empty line, and 
            message body, when the file is stashed (this feature is
            intended primarily for troubleshooting network/HTTP issues.)
            If True, ".http" will be appended to the filename
        """
        content_type = self.get_content_type(response)

        if content_type:
            # extract subtype
            fileformat = content_type.split('/')[-1]
            # snip off trailing parameter(s)
            fileformat = fileformat.split(';')[0]
        else:
            # desperate default
            fileformat = 'txt'

        # coerce to absolute path
        filepathname = os.path.abspath(filepathname)
        if not os.path.exists(os.path.split(filepathname)[0]):
            os.makedirs(os.path.split(filepathname)[0])

        if not filepathname.endswith(fileformat):
            filepathname += '.' + fileformat

        if raw_http:
            filepathname += '.http'

        writefile = open(filepathname, 'w')

        if raw_http:
            # replicate HTTP message head
            writefile.write(self.serialize_http_response_head(response))
            writefile.write('\n')

        # write message body content, if any
        if 'transfer-encoding' in response.headers and response.headers['transfer-encoding'] == 'chunked':
            for chunk in response.iter_content():
                writefile.write(chunk)
        elif 'content-length' in response.headers and response.headers['content-length'] > 0:
            writefile.write(response.content)
        else:
            # it's possible the headers are screwed up and we still want the content
            content = response.content
            if content:
                writefile.write(content)

        writefile.close()

    def read_stashed_data(self, filepath):
        """
        Returns a read-only copy of the stashed data as an ordinary file 
	object in read mode.
        """
        # the filepathname must be a full filepath
        filepathname = os.path.abspath(filepath)
        
        if not os.path.isfile(filepath):
            raise(BaseException('The filepath parameter "'
                    + filepath + '" does not identify an actual file.'))

        file = open(filepathname, 'r')
        return file


