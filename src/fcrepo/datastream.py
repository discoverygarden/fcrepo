# Copyright (c) 2010 Infrae / Technical University Delft. All rights reserved.
# See also LICENSE.txt

from collections import defaultdict

from lxml import etree
import newrelic.agent

from fcrepo.utils import rdfxml2dict, dict2rdfxml
    
class typedproperty(property):
    @newrelic.agent.function_trace()
    def __init__(self, fget, fset=None, fdel=None, doc=None, pytype=None):
        # like a normal property, but converts types to/from strings
        def typed_get(self):
            if pytype is bool:
                value = fget(self)
                if isinstance(value, bool):
                    return value
                return fget(self) == 'true'
            return pytype(fget(self))
        
        def typed_set(self, value):
            # we don't change the type here, this is done in wadl client
            # otherwise the wadl client can't determine the correct type
            return fset(self, value)
            
        super(typedproperty, self).__init__(typed_get, typed_set, fdel, doc)

class FedoraDatastream(object):
    @newrelic.agent.function_trace()
    def __init__(self, dsid, object):
        self.object = object
        self.dsid = dsid
        self._info = self.object.client.getDatastreamProfile(self.object.pid,
                                                             self.dsid)

    @newrelic.agent.function_trace()
    def delete(self, **params):
        self.object.client.deleteDatastream(self.object.pid,
                                            self.dsid,
                                            **params)
        self.object._dsids = None
    
    @newrelic.agent.function_trace()
    def getContent(self):
        return self.object.client.getDatastream(self.object.pid, self.dsid)
    
    @newrelic.agent.function_trace()
    def setContent(self, data='', **params):
            
        if self._info['controlGroup'] == 'X':
            # for some reason we need to add 2 characters to the body
            # or we get a parsing error in fedora
            data += '\r\n'
        
        self.object.client.modifyDatastream(self.object.pid,
                                            self.dsid,
                                            data,
                                            **params)
        self._info = self.object.client.getDatastreamProfile(self.object.pid,
                                                             self.dsid)
        
    @newrelic.agent.function_trace()
    def _setProperty(self, name, value):
        msg = u'Changed %s datastream property' % name
        name = {'label': 'dsLabel',
                'location': 'dsLocation',
                'state': 'dsState'}.get(name, name)
        params = {name: value, 'logMessage': msg, 'ignoreContent': True}
        self.object.client.modifyDatastream(self.object.pid,
                                            self.dsid,
                                            **params)
        self._info = self.object.client.getDatastreamProfile(self.object.pid,
                                                             self.dsid)

    label = property(lambda self: self._info['label'],
                     lambda self, value: self._setProperty('label', value))
    location = property(lambda self: self._info['location'],
                        lambda self, value: self._setProperty('location', value))
    state = property(lambda self: self._info['state'],
                           lambda self, value: self._setProperty('state',
                                                                 value))
    
    versionId = property(lambda self: self._info['versionId'],
                        lambda self, value: self._setProperty('versionId',
                                                              value)) 
    mimeType = property(lambda self: self._info['mimeType'],
                        lambda self, value: self._setProperty('mimeType',
                                                              value)) 
    formatURI = property(lambda self: self._info['formatURI'],
                         lambda self, value: self._setProperty('formatURI',
                                                               value)) 


    versionable = typedproperty(lambda self: self._info['versionable'],
                                lambda self, value: self._setProperty(
                                  'versionable', value), pytype=bool) 

    # read only
    createdDate = property(lambda self: self._info['createdDate'])
    controlGroup = property(lambda self: self._info['controlGroup'])
    size = typedproperty(lambda self: self._info['size'], pytype=int)
    checksum = property(lambda self: self._info['checksum'])
    checksumType = property(lambda self: self._info['checksumType'])



class RELSEXTDatastream(FedoraDatastream):
    @newrelic.agent.function_trace()
    def __init__(self, dsid, object):
        super(RELSEXTDatastream, self).__init__(dsid, object)
        self._rdf = None

    @newrelic.agent.function_trace()
    def _get_rdf(self):
        if self._rdf is None:
            rdfxml = self.getContent().read()
            self._rdf = rdfxml2dict(rdfxml)
        return self._rdf
    
    @newrelic.agent.function_trace()
    def keys(self):
        rdf = self._get_rdf()
        keys = rdf.keys()
        keys.sort()
        return keys
    predicates = keys
    
    @newrelic.agent.function_trace()
    def setContent(self, data='', **params):
        if not data:
            rdf = self._get_rdf()
            data = dict2rdfxml(self.object.pid, rdf)
            self._rdf = None
        super(RELSEXTDatastream, self).setContent(data, **params)
        
    @newrelic.agent.function_trace()
    def __setitem__(self, key, value):
        rdf = self._get_rdf()
        rdf[key]=value
    
    @newrelic.agent.function_trace()
    def __getitem__(self, key):
        rdf = self._get_rdf()
        return rdf[key]
    
    @newrelic.agent.function_trace()
    def __delitem__(self, key):
        rdf = self._get_rdf()
        del rdf[key]
    
    @newrelic.agent.function_trace()
    def __contains__(self, key):
        rdf = self._get_rdf()
        return key in rdf

    @newrelic.agent.function_trace()
    def __iter__(self):
        rdf = self._get_rdf()
        return rdf.__iter__()
    
class DCDatastream(FedoraDatastream):
    
    @newrelic.agent.function_trace()
    def __init__(self, dsid, object):
        super(DCDatastream, self).__init__(dsid, object)
        self._dc = None

    @newrelic.agent.function_trace()
    def _get_dc(self):
        if self._dc is None:
            xml = self.getContent().read()
            doc = etree.fromstring(xml)
            self._dc = defaultdict(list)
            for child in doc:
                name = child.tag.split('}')[-1]
                value = child.text
                if value is None:
                    continue
                if not isinstance(value, unicode):
                    value = value.decode('utf8')
                self._dc[name].append(value)
        return self._dc
    
    @newrelic.agent.function_trace()
    def keys(self):
        dc = self._get_dc()
        keys = dc.keys()
        keys.sort()
        return keys
    properties = keys

    @newrelic.agent.function_trace()
    def setContent(self, data='', **params):
        if not data:
            dc = self._get_dc()
            nsmap = {'dc': 'http://purl.org/dc/elements/1.1/',
                     'oai_dc': 'http://www.openarchives.org/OAI/2.0/oai_dc/'}
            doc = etree.Element('{%s}dc' % nsmap['oai_dc'], nsmap=nsmap)
            for key, values in dc.items():
                for value in values:
                    el = etree.SubElement(doc, '{%s}%s' % (nsmap['dc'], key))
                    el.text = value
            data = etree.tostring(doc, encoding="UTF-8",
                                  pretty_print=True, xml_declaration=False)
            self._dc = None
        super(DCDatastream, self).setContent(data, **params)

    @newrelic.agent.function_trace()
    def __setitem__(self, key, value):
        dc = self._get_dc()
        dc[key]=value
    
    @newrelic.agent.function_trace()
    def __getitem__(self, key):
        dc = self._get_dc()
        return dc[key]
    
    @newrelic.agent.function_trace()
    def __delitem__(self, key):
        dc = self._get_dc()
        del dc[key]
    
    @newrelic.agent.function_trace()
    def __contains__(self, key):
        dc = self._get_dc()
        return key in dc

    @newrelic.agent.function_trace()
    def __iter__(self):
        dc = self._get_dc()
        return dc.__iter__()
