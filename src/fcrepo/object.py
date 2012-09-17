# Copyright (c) 2010 Infrae / Technical University Delft. All rights reserved.
# See also LICENSE.txt

from fcrepo.datastream import FedoraDatastream, RELSEXTDatastream, DCDatastream
from fcrepo.connection import FedoraConnectionException
import logging

logger = logging.getLogger('fcrepo.object.FedoraObject')
class FedoraObject(object):
    def __init__(self, pid, client):
        self.pid = pid
        self.client = client
        self._info = self.client.getObjectProfile(self.pid)
        self._dsids = None # load lazy
        self._methods = None
        self._ds_cache = {}
        
    def _setProperty(self, name, value):
        msg = u'Changed %s object property' % name
        kwargs = {name: value, 'logMessage': msg}
        self.client.updateObject(self.pid, **kwargs)
        self._info = self.client.getObjectProfile(self.pid)

    label = property(lambda self: self._info['label'],
                     lambda self, value: self._setProperty('label', value))
    ownerId = property(lambda self: self._info['ownerId'],
                       lambda self, value: self._setProperty('ownerId', value))
    state = property(lambda self: self._info['state'],
                           lambda self, value: self._setProperty('state',
                                                                 value))
    # read only
    createdDate = property(lambda self: self._info['createdDate'])
    lastModifiedDate = property(lambda self: self._info['lastModifiedDate'])

    def datastreams(self):
        if self._dsids is None:
            '''
            XXX: Wrapped in try/except, as it can cause race condition issues when
            attempting to use the 'in' operator, when trying to determine if an
            object contains a certain datastream.
   
            Race condition:
            1. We get the object (profile) as a FedoraObject
            2. Another system purges the object
            3. We got the object, so we try to enumate the DSs via the 'in'
              operator, and fail.
            '''
            try:
                self._dsids = self.client.listDatastreams(self.pid)
            except FedoraConnectionException:
                self._dsids = []
                logger.exception('Unable to get the list of datastream! (perhaps the object has been purged?) Returning an empty list.')
        return self._dsids

    def __iter__(self):
        return iter(self.datastreams())
    
    def __in__(self, dsid):
        return dsid in self.datastreams()

    def __getitem__(self, dsid):
        ds = self._ds_cache.get(dsid)
        if not ds is None:
            return ds
        elif dsid == 'DC':
            ds = DCDatastream(dsid, self)
        elif dsid == 'RELS-EXT':
            ds = RELSEXTDatastream(dsid, self)
        else:
            ds = FedoraDatastream(dsid, self)
        self._ds_cache[dsid] = ds
        return ds

    def __delitem__(self, dsid):
        self.client.deleteDatastream(self.pid, dsid)
        self._dsids = None

    def delete(self, **params):
        self.client.deleteObject(self.pid, **params)
        
    def addDataStream(self, dsid, body='', **params):            
        self.client.addDatastream(self.pid, dsid, body, **params)
        self._dsids=None

    def methods(self):
        if self._methods is None:
            self._methods = self.client.getAllObjectMethods(self.pid)
        return [m[1] for m in self._methods]

    def call(self, method_name, **params):
        for sdef, method in self._methods:
            if method == method_name:
                break
        else:
            raise KeyError('No such method: %s' % method_name)
        
        return self.client.invokeSDefMethodUsingGET(self.pid, sdef,
                                                    method, **params)
