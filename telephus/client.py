from telephus.cassandra.ttypes import *
from telephus.protocol import ManagedThriftRequest
from collections import defaultdict
import time

class CassandraClient(object):
    def __init__(self, manager, consistency=ConsistencyLevel.ONE):
        self.manager = manager
        self.consistency = consistency
        
    def _time(self):
        return Clock(timestamp=int(time.time() * 1000000))
    
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
    def _mkpred(self, names, start, finish, reverse, count):
        if names:
            srange = None
        else:
            srange = SliceRange(start, finish, reverse, count)
        return SlicePredicate(names, srange)
    
    def get(self, key, columnPath, column=None, super_column=None, consistency=None,
            retries=None):
        cp = self._getpath(columnPath, column, super_column)
        consistency = consistency or self.consistency
        req = ManagedThriftRequest('get', key, cp, consistency)
        return self.manager.pushRequest(req, retries=retries)
    
    def get_slice(self, key, columnParent, names=None, start='', finish='',
                  reverse=False, count=100, consistency=None, super_column=None,
                  retries=None):
        cp = self._getparent(columnParent, super_column)
        consistency = consistency or self.consistency
        pred = self._mkpred(names, start, finish, reverse, count)
        req = ManagedThriftRequest('get_slice', key, cp, pred, consistency)
        return self.manager.pushRequest(req, retries=retries)
    
    def multiget(self, keys, columnParent, column=None, super_column=None,
                  consistency=None, retries=None):
        return self.multiget_slice(keys, columnParent, names=[column], count=100,
                   consistency=consistency, retries=retries, super_column=super_column)
    
    def multiget_slice(self, keys, columnParent, names=None, start='', finish='',
                  reverse=False, count=100, consistency=None, super_column=None,
                  retries=None):
        cp = self._getparent(columnParent, super_column)
        consistency = consistency or self.consistency
        pred = self._mkpred(names, start, finish, reverse, count)
        req = ManagedThriftRequest('multiget_slice', keys, cp, pred, consistency)
        return self.manager.pushRequest(req, retries=retries)
    
    def get_count(self, key, columnParent, super_column=None, start='', finish='',
                  consistency=None, retries=None):    
        cp = self._getparent(columnParent, super_column)
        pred = self._mkpred(None, start, finish, False, 2147483647)
        consistency = consistency or self.consistency
        req = ManagedThriftRequest('get_count', key, cp, pred, consistency)
        return self.manager.pushRequest(req, retries=retries)
    
    def get_key_range(self, columnParent, **kwargs):
        return self.get_range_slices(columnParent, **kwargs)
    
    def get_range_slice(self, columnParent, **kwargs):
        return self.get_range_slices(columnParent, **kwargs)
    
    def get_range_slices(self, columnParent, start='', finish='', column_start='',
            column_finish='', names=None, count=100, column_count=100,
            reverse=False, use_tokens=False, consistency=None, super_column=None,
            retries=None):
        cp = self._getparent(columnParent, super_column)
        consistency = consistency or self.consistency
        if not use_tokens:
            krange = KeyRange(start_key=start, end_key=finish, count=count)
        else:
            krange = KeyRange(start_token=start, end_token=finish, count=count)
        pred = self._mkpred(names, column_start, column_finish, reverse, column_count)
        req = ManagedThriftRequest('get_range_slices', cp, pred, krange, consistency)
        return self.manager.pushRequest(req, retries=retries)

    def insert(self, key, columnParent, value, column=None, super_column=None,
               timestamp=None, consistency=None, retries=None):
        timestamp = timestamp or self._time()
        cp = self._getparent(columnParent, super_column)
        consistency = consistency or self.consistency
        req = ManagedThriftRequest('insert', key, cp, Column(column, value, timestamp), consistency)
        return self.manager.pushRequest(req, retries=retries)

    def remove(self, key, columnPath, column=None, super_column=None, 
               timestamp=None, consistency=None, retries=None):
        cp = self._getpath(columnPath, column, super_column)
        timestamp = timestamp or self._time()
        consistency = consistency or self.consistency
        req = ManagedThriftRequest('remove', key, cp, timestamp, self.consistency)
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
            pred = self._mkpred(names, start, finish, reverse, count)
            for key in keys:
                mutmap[key][cf] = [Mutation(deletion=Deletion(timestamp, supercolumn, pred))]
        req = ManagedThriftRequest('batch_mutate', mutmap, consistency)
        return self.manager.pushRequest(req, retries=retries)
    
    def batch_mutate(self, mutationmap, timestamp=None, consistency=None, retries=None):
        timestamp = timestamp or self._time()
        consistency = consistency or self.consistency
        mutmap = defaultdict(dict)
        for key, cfmap in mutationmap.iteritems():
            for cf, colmap in cfmap.iteritems():
                cols_or_supers_or_deletions = self._mk_cols_or_supers(colmap, timestamp, make_deletions=True)
                muts = []
                for c in cols_or_supers_or_deletions:
                    if isinstance(c, SuperColumn):
                        muts.append(Mutation(ColumnOrSuperColumn(super_column=c)))
                    elif isinstance(c, Column):
                        muts.append(Mutation(ColumnOrSuperColumn(column=c)))
                    elif isinstance(c, Deletion):
                        muts.append(Mutation(deletion=c))
                    else:
                        muts.append(c)
                mutmap[key][cf] = muts
        req = ManagedThriftRequest('batch_mutate', mutmap, consistency)
        return self.manager.pushRequest(req, retries=retries)
        
    def _mk_cols_or_supers(self, mapping, timestamp, make_deletions=False):
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
                cols2delete = []
                for col, val in mapping.iteritems():
                    if val is None and make_deletions:
                        cols2delete.append(col)
                    else:
                        colsorsupers.append(Column(col, val, timestamp))
                if cols2delete:
                    colsorsupers.append(Deletion(timestamp, None, SlicePredicate(column_names=cols2delete)))
        else:
            raise TypeError('dict (of dicts) or list of Columns/SuperColumns expected')
        return colsorsupers
        
    def get_string_property(self, name, retries=None):
        req = ManagedThriftRequest('get_string_property', name)
        return self.manager.pushRequest(req, retries=retries)

    def get_string_list_property(self, name, retries=None):
        req = ManagedThriftRequest('get_string_list_property', name)
        return self.manager.pushRequest(req, retries=retries)

    def describe_keyspaces(self, retries=None):
        req = ManagedThriftRequest('describe_keyspaces')
        return self.manager.pushRequest(req, retries=retries)
    
    def describe_keyspace(self, keyspace, retries=None):
        req = ManagedThriftRequest('describe_keyspace', keyspace)
        return self.manager.pushRequest(req, retries=retries)

    def describe_cluster_name(self, retries=None):
        req = ManagedThriftRequest('describe_cluster_name')
        return self.manager.pushRequest(req, retries=retries)
    
    def describe_partitioner(self, retries=None):
        req = ManagedThriftRequest('describe_partitioner')
        return self.manager.pushRequest(req, retries=retries)
    
    def describe_ring(self, keyspace, retries=None):
        req = ManagedThriftRequest('describe_ring', keyspace)
        return self.manager.pushRequest(req, retries=retries)
    
    def describe_splits(self, keyspace, cfName, start_token, end_token, keys_per_split,
                        retries=None):
        req = ManagedThriftRequest('describe_splits', keyspace, cfName, start_token,
                                   end_token, keys_per_split)
        return self.manager.pushRequest(req, retries=retries)
    
    def truncate(self, cfName, retries=None):
        req = ManagedThriftRequest('truncate', cfName)
        return self.manager.pushRequest(req, retries=retries)
    
    def check_schema_agreement(self, retries=None):
        req = ManagedThriftRequest('check_schema_agreement')
        self.manager.pushRequest(req, retries=retries)
    
    def system_drop_column_family(self, cfName, retries=None):
        req = ManagedThriftRequest('system_drop_column_family', cfName)
        return self.manager.pushRequest(req, retries=retries)
    
    def system_drop_keyspace(self, keyspace, retries=None):
        req = ManagedThriftRequest('system_drop_keyspace', keyspace)
        return self.manager.pushRequest(req, retries=retries)
    
    def system_rename_column_family(self, oldname, newname, retries=None):
        req = ManagedThriftRequest('system_rename_column_family', oldname, newname)
        return self.manager.pushRequest(req, retries=retries)
    
    def system_rename_keyspace(self, oldname, newname, retries=None):
        req = ManagedThriftRequest('system_rename_keyspace', oldname, newname)
        return self.manager.pushRequest(req, retries=retries)
    
    # TODO: make friendly
    def system_add_column_family(self, cfDef, retries=None):
        req = ManagedThriftRequest('system_add_column_family', cfDef)
        return self.manager.pushRequest(req, cfDef, retries=reties)
    
    def system_add_keyspace(self, ksDef, retries=None):
        req = ManagedThriftRequest('system_add_keyspace', ksDef)
        return self.manager.pushRequest(req, ksDef, retries=retries)

    def describe_version(self, retries=None):
        req = ManagedThriftRequest('describe_version')
        return self.manager.pushRequest(req, retries=retries)
