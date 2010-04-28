from telephus.cassandra.ttypes import *
from telephus.protocol import ManagedThriftRequest
from collections import defaultdict
import time

class CassandraClient(object):
    def __init__(self, manager, keyspace, consistency=ConsistencyLevel.ONE):
        self.manager = manager
        self.keyspace = keyspace
        self.consistency = consistency
        
    def _time(self):
        return int(time.time() * 1000)
    
    def _getparent(self, columnParentOrCF, super_column=None):
        if isinstance(columnParentOrCF, str):
            return ColumnParent(columnParentOrCF, super_column=super_column)
        else:
            return columnParentOrCF
        
    def _getpath(self, columnPathOrCF, col, super_column=None):
        if isinstance(columnPathOrCF, str):
            return ColumnPath(columnPathOrCF, super_column=super_column, column=col)
        else:
            return columnPathOrCF
    
    def get(self, key, columnPath, column=None, super_column=None, consistency=None,
            retries=None):
        cp = self._getpath(columnPath, column, super_column)
        consistency = consistency or self.consistency
        req = ManagedThriftRequest('get', self.keyspace, key, cp, consistency)
        return self.manager.pushRequest(req, retries=retries)
    
    def get_slice(self, key, columnParent, names=None, start='', finish='',
                  reverse=False, count=100, consistency=None, super_column=None,
                  retries=None):
        cp = self._getparent(columnParent, super_column)
        if names:
            srange = None
        else:
            srange = SliceRange(start, finish, reverse, count)
        consistency = consistency or self.consistency
        pred = SlicePredicate(names, srange)
        req = ManagedThriftRequest('get_slice', self.keyspace, key, cp, pred,
                                   consistency)
        return self.manager.pushRequest(req, retries=retries)
    
    def multiget(self, keys, columnPath, column=None, super_column=None,
                 consistency=None, retries=None):
        cp = self._getpath(columnPath, column, super_column)
        consistency = consistency or self.consistency
        req = ManagedThriftRequest('multiget', self.keyspace, keys, cp, consistency)
        return self.manager.pushRequest(req, retries=retries)
    
    def multiget_slice(self, keys, columnParent, names=None, start='', finish='',
                  reverse=False, count=100, consistency=None, super_column=None,
                  retries=None):
        cp = self._getparent(columnParent, super_column)
        if names:
            srange = None
        else:
            srange = SliceRange(start, finish, reverse, count)
        consistency = consistency or self.consistency
        pred = SlicePredicate(names, srange)
        req = ManagedThriftRequest('multiget_slice', self.keyspace, keys, cp,
                                   pred, consistency)
        return self.manager.pushRequest(req, retries=retries)
    
    def get_count(self, key, columnParent, super_column=None, consistency=None, retries=None):    
        cp = self._getparent(columnParent, super_column)
        consistency = consistency or self.consistency
        req = ManagedThriftRequest('get_count', self.keyspace, key, cp,
                                   consistency)
        return self.manager.pushRequest(req, retries=retries)
    
    def get_key_range(self, columnParent, start='', finish='', count=100,
                    column_count=100, reverse=False, consistency=None,
                    retries=None):
        consistency = consistency or self.consistency
        return self.get_range_slice(columnParent, start=start, finish=finish,
                                    count=count, column_count=column_count,
                                    consistency=consistency, reverse=reverse,
                                    retries=retries)
    
    def get_range_slice(self, columnParent, start='', finish='', column_start='',
            column_finish='', names=None, count=100, column_count=100, 
            reverse=False, consistency=None, super_column=None, retries=None):
        cp = self._getparent(columnParent, super_column)
        consistency = consistency or self.consistency
        if names:
            srange = None
        else:
            srange = SliceRange(column_start, column_finish, reverse, column_count)
        pred = SlicePredicate(names, srange)
        req = ManagedThriftRequest('get_range_slice', self.keyspace, cp, pred,
                                   start, finish, count, consistency)
        return self.manager.pushRequest(req, retries=retries)

    def insert(self, key, columnPath, value, column=None, super_column=None,
               timestamp=None, consistency=None, retries=None):
        timestamp = timestamp or self._time()
        cp = self._getpath(columnPath, column, super_column)
        consistency = consistency or self.consistency
        req = ManagedThriftRequest('insert', self.keyspace, key, cp, value,
                                   timestamp, consistency)
        return self.manager.pushRequest(req, retries=retries)

    def remove(self, key, columnPath, column=None, super_column=None, 
               timestamp=None, consistency=None, retries=None):
        cp = self._getpath(columnPath, column, super_column)
        timestamp = timestamp or self._time()
        consistency = consistency or self.consistency
        req = ManagedThriftRequest('remove', self.keyspace, key, cp,
                                   timestamp, self.consistency)
        return self.manager.pushRequest(req, retries=retries)

    def batch_insert(self, key, columnFamily, mapping, timestamp=None,
                     consistency=None, retries=None):
        if isinstance(mapping, list) and timestamp is not None:
            raise RuntimeError('Timestamp cannot be specified with a list of Mutations')
        timestamp = timestamp or self._time()
        consistency = consistency or self.consistency
        mutmap = {key: {columnFamily: self._mk_cols_or_supers(mapping, timestamp)}}
        return self.batch_mutate(mutmap, timestamp=timestamp, consistency=consistency,
                                 retries=retries)
    
    def batch_remove(self, cfmap, start='', finish='', count=100, names=None,
                     reverse=False, consistency=None, timestamp=None, supercolumn=None,
                     retries=None):
        timestamp = timestamp or self._time()
        consistency = consistency or self.consistency
        mutmap = defaultdict(dict)
        for cf, keys in cfmap.iteritems():
            if names:
                srange = None
            else:
                srange = SliceRange(start, finish, reverse, count)
            pred = SlicePredicate(names, srange)
            for key in keys:
                mutmap[key][cf] = [Mutation(deletion=Deletion(timestamp, supercolumn, pred))]
        req = ManagedThriftRequest('batch_mutate', self.keyspace, mutmap, consistency)
        return self.manager.pushRequest(req, retries=retries)
    
    def batch_mutate(self, mutationmap, timestamp=None, consistency=None, retries=None):
        timestamp = timestamp or self._time()
        consistency = consistency or self.consistency
        mutmap = defaultdict(dict)
        for key, cfmap in mutationmap.iteritems():
            for cf, colmap in cfmap.iteritems():
                colsorsupers = self._mk_cols_or_supers(colmap, timestamp)
                muts = []
                for c in colsorsupers:
                    if isinstance(c, SuperColumn):
                        muts.append(Mutation(ColumnOrSuperColumn(super_column=c)))
                    elif isinstance(c, Column):
                        muts.append(Mutation(ColumnOrSuperColumn(column=c)))
                    else:
                        muts.append(c)
                mutmap[key][cf] = muts
        req = ManagedThriftRequest('batch_mutate', self.keyspace, mutmap, consistency)
        return self.manager.pushRequest(req, retries=retries)
        
    def _mk_cols_or_supers(self, mapping, timestamp):
        if isinstance(mapping, list):
            return mapping
        colsorsupers = []
        if isinstance(mapping, dict):
            first = mapping.keys()[0]
            if isinstance(mapping[first], dict):
                for name in mapping:
                    cols = []
                    for col,val in mapping[name].iteritems():
                        cols.append(Column(col, val, timestamp))
                    colsorsupers.append(SuperColumn(name=name, columns=cols))
            else:
                for col,val in mapping.iteritems():
                    colsorsupers.append(Column(col, val, timestamp))
        else:
            raise TypeError('dict (of dicts) or list of Columns/SuperColumns expected')
        return colsorsupers
        
    def get_string_property(self, name, retries=None):
        req = ManagedThriftRequest('get_string_property', name)
        return self.manager.pushRequest(req, retries=retries)

    def get_string_list_property(self, name, retries=None):
        req = ManagedThriftRequest('get_string_list_property', name)
        return self.manager.pushRequest(req, retries=retries)

    def describe_keyspace(self, keyspace, retries=None):
        req = ManagedThriftRequest('describe_keyspace', keyspace)
        return self.manager.pushRequest(req, retries=retries)
