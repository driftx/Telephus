import telephus.cassandra.c08.ttypes

class KsDef(telephus.cassandra.c08.ttypes.KsDef):

    def to07(self):
        if self.strategy_options and 'replication_factor' in self.strategy_options:
            if self.replication_factor is None:
                self.replication_factor = self.strategy_options['replication_factor']
        return self

    def to08(self):
        if self.replication_factor is not None:
            if self.strategy_options is None:
                self.strategy_options = {}
            if 'replication_factor' not in self.strategy_options:
                self.strategy_options['replication_factor'] = str(self.replication_factor)
        return self

ColumnDef = telephus.cassandra.c08.ttypes.ColumnDef
CfDef = telephus.cassandra.c08.ttypes.CfDef

NotFoundException = telephus.cassandra.c08.ttypes.NotFoundException
InvalidRequestException = telephus.cassandra.c08.ttypes.InvalidRequestException
UnavailableException = telephus.cassandra.c08.ttypes.UnavailableException
TimedOutException = telephus.cassandra.c08.ttypes.TimedOutException
AuthenticationException = telephus.cassandra.c08.ttypes.AuthenticationException
AuthorizationException = telephus.cassandra.c08.ttypes.AuthorizationException
SchemaDisagreementException = telephus.cassandra.c08.ttypes.SchemaDisagreementException

ConsistencyLevel = telephus.cassandra.c08.ttypes.ConsistencyLevel
IndexOperator = telephus.cassandra.c08.ttypes.IndexOperator
IndexType = telephus.cassandra.c08.ttypes.IndexType
Compression = telephus.cassandra.c08.ttypes.Compression
CqlResultType = telephus.cassandra.c08.ttypes.CqlResultType
Column = telephus.cassandra.c08.ttypes.Column
SuperColumn = telephus.cassandra.c08.ttypes.SuperColumn
CounterColumn = telephus.cassandra.c08.ttypes.CounterColumn
CounterSuperColumn = telephus.cassandra.c08.ttypes.CounterSuperColumn
ColumnOrSuperColumn = telephus.cassandra.c08.ttypes.ColumnOrSuperColumn
ColumnParent = telephus.cassandra.c08.ttypes.ColumnParent
ColumnPath = telephus.cassandra.c08.ttypes.ColumnPath
SliceRange = telephus.cassandra.c08.ttypes.SliceRange
SlicePredicate = telephus.cassandra.c08.ttypes.SlicePredicate
IndexExpression = telephus.cassandra.c08.ttypes.IndexExpression
IndexClause = telephus.cassandra.c08.ttypes.IndexClause
KeyRange = telephus.cassandra.c08.ttypes.KeyRange
KeySlice = telephus.cassandra.c08.ttypes.KeySlice
KeyCount = telephus.cassandra.c08.ttypes.KeyCount
Deletion = telephus.cassandra.c08.ttypes.Deletion
Mutation = telephus.cassandra.c08.ttypes.Mutation
TokenRange = telephus.cassandra.c08.ttypes.TokenRange
AuthenticationRequest = telephus.cassandra.c08.ttypes.AuthenticationRequest
CqlRow = telephus.cassandra.c08.ttypes.CqlRow
CqlResult = telephus.cassandra.c08.ttypes.CqlResult
