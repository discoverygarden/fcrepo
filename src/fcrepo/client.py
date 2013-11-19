# Copyright (c) 2010 Infrae / Technical University Delft. All rights reserved.
# See also LICENSE.txt

import urllib
from collections import defaultdict

from copy import copy

from lxml import etree
from lxml.builder import ElementMaker

from fcrepo.wadl import API
from fcrepo.utils import NS
from fcrepo.object import FedoraObject

NSMAP = {'foxml': 'info:fedora/fedora-system:def/foxml#'}

class FedoraClient(object):
    @newrelic.agent.function_trace()
    def __init__(self, connection):
        self.api = API(connection)

    @newrelic.agent.function_trace()
    def getNextPID(self, namespace, numPIDs=1, format=u'text/xml'):
        request = self.api.getNextPID()
        response = request.submit(namespace=namespace,
                                  numPIDs=numPIDs,
                                  format=format)
        xml = response.read()
        response.close()
        '''
        The following use of default namespace is to make this code work in 3.4 and 3.5 where the namespacing is different
        '''
        doc = etree.fromstring(xml)
        fake_namespace_dictionary = {}
        try:
            fake_namespace_dictionary['default'] = doc.nsmap[None]
            ids = [id.decode('utf8') for id in doc.xpath('/default:pidList/default:pid/text()', namespaces = fake_namespace_dictionary)]
        except KeyError:
            ids = [id.decode('utf8') for id in doc.xpath('/pidList/pid/text()')]
            
        if len(ids) == 1:
            return ids[0]
        return ids

    @newrelic.agent.function_trace()
    def createObject(self, pid, label, state=u'A'):
        foxml = ElementMaker(namespace=NSMAP['foxml'], nsmap=NSMAP)
        foxml_state = {'A': u'Active',
                       'I': u'Inactive',
                       'D': u'Deleted'}[state]
        doc = foxml.digitalObject(
            foxml.objectProperties(
              foxml.property(NAME="info:fedora/fedora-system:def/model#state",
                             VALUE=state),
              foxml.property(NAME="info:fedora/fedora-system:def/model#label",
                             VALUE=label)
              ),
            VERSION='1.1', PID=pid)
        body = etree.tostring(doc, encoding="UTF-8", xml_declaration=False)
        
        request = self.api.createObject(pid=pid)
        request.headers['Content-Type'] = 'text/xml; charset=utf-8'
        response = request.submit(body, state=state[0], label=label)
        return self.getObject(pid)

    @newrelic.agent.function_trace()
    def getObject(self, pid):
        return FedoraObject(pid, self)

    @newrelic.agent.function_trace()
    def getObjectProfile(self, pid):
        request = self.api.getObjectProfile(pid=pid)
        response = request.submit(format=u'text/xml')
        xml = response.read()
        response.close()
        doc = etree.fromstring(xml)
        result = {'ownerId': u''}
        for child in doc:
            # rename elementnames to match property names in foxml
            # the xml data is namespaced in 3.4, but not in 3.3, so strip out
            # the namespace, to be compatible with both
            name = {'objLabel': 'label',
                    'objOwnerId': 'ownerId',
                    'objCreateDate': 'createdDate',
                    'objLastModDate': 'lastModifiedDate',
                    'objState': 'state'}.get(child.tag.split('}')[-1])
            if name is None or child.text is None:
                continue
            value = child.text
            if not isinstance(value, unicode):
                value = value.decode('utf8')
            result[name] = value
        return result

    @newrelic.agent.function_trace()
    def updateObject(self, pid, body='', **params):
        request = self.api.updateObject(pid=pid)
        response = request.submit(body, **params)

    @newrelic.agent.function_trace()
    def deleteObject(self, pid, **params):
        request = self.api.deleteObject(pid=pid)
        response = request.submit(**params)

    @newrelic.agent.function_trace()
    def listDatastreams(self, pid):
        request = self.api.listDatastreams(pid=pid)
        response = request.submit(format=u'text/xml')
        xml = response.read()
        response.close()
        doc = etree.fromstring(xml)
        return [child.attrib['dsid'] for child in doc]

    @newrelic.agent.function_trace()
    def addDatastream(self, pid, dsid, body='', **params):
        if dsid == 'RELS-EXT' and not body:
            body = ('<rdf:RDF xmlns:rdf="%s"/>' % NS.rdf)
            params['mimeType'] = u'application/rdf+xml'
            params['formatURI'] = (
                u'info:fedora/fedora-system:FedoraRELSExt-1.0')

        if params.get('controlGroup', u'X') == u'X':
            if not 'mimeType' in params:
                params['mimeType'] = u'text/xml'
        if not 'mimeType' in params:
            params['mimeType'] = u'application/binary'
            
        if 'checksumType' not in params:
            params['checksumType'] = u'MD5'

        params = self._fix_ds_params(params)

        request = self.api.addDatastream(pid=pid, dsID=dsid)
        request.headers['Content-Type'] = params['mimeType']
        response = request.submit(body, **params)        

    @newrelic.agent.function_trace()
    def _fix_ds_params(self, params):
        for name, param in params.items():
            newname = {'label': 'dsLabel',
                       'location': 'dsLocation',
                       'state': 'dsState'}.get(name, name)
            if newname != name:
                params[newname] = param
                del params[name]
                
        return params

    @newrelic.agent.function_trace()
    def getDatastreamProfile(self, pid, dsid):
        request = self.api.getDatastreamProfile(pid=pid, dsID=dsid)
        response = request.submit(format=u'text/xml')
        xml = response.read()
        response.close()
        doc = etree.fromstring(xml)
        result = {}
        tags = {
            'dsLabel': 'label',
            'dsVerionId': 'versionId',
            'dsCreateDate': 'createdDate',
            'dsState': 'state',
            'dsMIME': 'mimeType',
            'dsFormatURI': 'formatURI',
            'dsControlGroup': 'controlGroup',
            'dsSize': 'size',
            'dsVersionable': 'versionable',
            'dsInfoType': 'infoType',
            'dsLocation': 'location',
            'dsLocationType': 'locationType',
            'dsChecksum': 'checksum',
            'dsChecksumType': 'checksumType'
        }
        
        for child in doc:
            # rename elementnames to match property names in foxml
            # the xml data is namespaced in 3.4, but not in 3.3, so strip out
            # the namespace, to be compatible with both
            name = tags.get(child.tag.rpartition('}')[2], None)
            if name is not None:
                value = child.text
                if value and not isinstance(value, unicode):
                    value = value.decode('utf8')
                result[name] = value
        return result

    @newrelic.agent.function_trace()
    def modifyDatastream(self, pid, dsid, body='', **params):
        params = self._fix_ds_params(params)
        request = self.api.modifyDatastream(pid=pid, dsID=dsid)
        response = request.submit(body, **params)
        
    @newrelic.agent.function_trace()
    def getDatastream(self, pid, dsid):
        request = self.api.getDatastream(pid=pid, dsID=dsid)
        return request.submit()

    @newrelic.agent.function_trace()
    def deleteDatastream(self, pid, dsid, **params):
        request = self.api.deleteDatastream(pid=pid, dsID=dsid)
        return request.submit(**params)

    @newrelic.agent.function_trace()
    def getAllObjectMethods(self, pid, **params):
        params['format'] = u'text/xml'
        request = self.api.getAllObjectMethods(pid=pid)
        response = request.submit(**params)
        xml = response.read()
        doc = etree.fromstring(xml)
        method_list = []
        for sdef_el in doc:
            sdef = sdef_el.attrib['pid']
            for method_el in sdef_el:
                method = method_el.attrib['name']
                method_list.append((sdef, method))
        response.close()
        return method_list

    @newrelic.agent.function_trace()
    def invokeSDefMethodUsingGET(self, pid, sdef, method, **params):
        request = self.api.invokeSDefMethodUsingGET(pid=pid, sDef=sdef,
                                                    method=method)
        return request.submit(**params)

    @newrelic.agent.function_trace()
    def searchObjects(self, query, fields, terms=False, maxResults=10):
        field_params = {}
        assert isinstance(fields, list)
        for field in fields:
            field_params[field] = u'true'
            
        token = True
        NS = 'http://www.fedora.info/definitions/1/0/types/'
        while token:
            if token is True:
                token = False
            request = self.api.searchObjects()
            request.undocumented_params = field_params

            if token:
                if terms:
                    response = request.submit(terms=query,
                                              sessionToken=token,
                                              maxResults=maxResults,
                                              resultFormat=u'text/xml')
                else:
                    response = request.submit(query=query,
                                              sessionToken=token,
                                              maxResults=maxResults,
                                              resultFormat=u'text/xml')
            else:
                if terms:
                    response = request.submit(terms=query,
                                              maxResults=maxResults,
                                              resultFormat=u'text/xml')
                else:
                    response = request.submit(query=query,
                                              maxResults=maxResults,
                                              resultFormat=u'text/xml')

            xml = response.read()
            response.close()
            doc = etree.fromstring(xml)
                
            tokens = doc.xpath('//f:token/text()', namespaces={'f': NS})
            if tokens:
                token = tokens[0].decode('utf8')
            else:
                token = False
            
            for result in doc.xpath('//f:objectFields', namespaces={'f': NS}):
                data = defaultdict(list)

                for child in result:
                    field_name = child.tag.split('}')[-1].decode('utf8')
                    value = child.text or u''
                    if not isinstance(value, unicode):
                        value = value.decode('utf8')
                    data[field_name].append(value)
                yield data

    @newrelic.agent.function_trace()
    def searchTriples(self, query, lang='sparql', format='Sparql',
                      limit=100, type='tuples', dt='on', flush=True):
        
        flush = str(flush).lower()
        URL_pramaters = {'query':query,
                   'lang':lang,
                   'flush': flush,
                   'format':format,
                   'type':type,
                   'dt':dt}
        
        #conditionaly set limit if there is one. (so it can be set to None)
        if limit:
            URL_pramaters['limit'] = limit
        
        # Encode in utf8 to let unicode pass through urlencode
        for key in URL_pramaters:
            URL_pramaters[key] = URL_pramaters[key].encode('utf8')
        
        url = u'/risearch?%s' % urllib.urlencode(URL_pramaters)
        #Fedora started needing authentication in 3.5 for RI, tested in 3.4 as well
        headers = copy(self.api.connection.form_headers)
        headers['Accept:'] = 'text/xml'
        response = self.api.connection.open(url, '', headers, method='POST')
        xml = response.read()
        doc = etree.fromstring(xml)
        NS = 'http://www.w3.org/2001/sw/DataAccess/rf1/result' # ouch, old!
        for result in doc.xpath('//sparql:result', namespaces={'sparql': NS}):
            data = {}
            for el in result:
                name = el.tag.split('}')[-1]
                value = {}
                uri = el.attrib.get('uri')
                if uri:
                    value['value'] = uri.decode('utf8')
                    value['type'] = 'uri'
                else:
                    value['type'] = 'literal'
                    if isinstance(el.text, unicode):
                        value['value'] = el.text
                    elif el.text:
                        value['value'] = el.text.decode('utf8')
                    else:
                        value['value'] = u''
                    datatype = el.attrib.get('datatype')
                    lang = el.attrib.get('lang')
                    if datatype:
                        value['datatype'] = datatype
                    elif lang:
                        value['lang'] = lang
                data[name] = value
            yield data
        
