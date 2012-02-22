class APIMismatch(Exception):
    pass

def translateArgs(request, api_version):
    args = request.args
    if request.method == 'system_add_keyspace' \
    or request.method == 'system_update_keyspace':
        args = adapt_ksdef_rf(args[0]) + args[1:]
    return args

def postProcess(results, method):
    if method == 'describe_keyspace':
        results = adapt_ksdef_rf(results)
    elif method == 'describe_keyspaces':
        results = map(adapt_ksdef_rf, results)
    return results

def adapt_ksdef_rf(ksdef):
    """
    try to always have both KsDef.strategy_options['replication_factor'] and
    KsDef.replication_factor available, and let the thrift api code and client
    code work out what they want to use.
    """
    if getattr(ksdef, 'strategy_options', None) is None:
        ksdef.strategy_options = {}
    if 'replication_factor' in ksdef.strategy_options:
        if ksdef.replication_factor is None:
            ksdef.replication_factor = int(ksdef.strategy_options['replication_factor'])
    elif ksdef.replication_factor is not None:
        ksdef.strategy_options['replication_factor'] = str(ksdef.replication_factor)
    return ksdef
