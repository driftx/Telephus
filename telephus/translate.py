from telephus.cassandra.constants import *

__all__ = ['APIMismatch', 'getAPIVersion', 'translateArgs', 'postProcess']

class APIMismatch(Exception):
    pass

def getAPIVersion(remoteversion):
    """
    Try to determine if the remote thrift api version is likely to work the
    way we expect it to. A mismatch in major version number will definitely
    break, but a mismatch in minor version is probably ok if the remote side
    is higher (it should be backwards compatible). A change in the patch
    number should not affect anything noticeable.
    """
    r_major, r_minor, r_patch = map(int, remoteversion.split('.'))
    for version in [CASSANDRA_08, CASSANDRA_07]:
        o_major, o_minor, o_patch = map(int, version.split('.'))
        if (r_major == o_major) and (r_minor >= o_minor):
            return version
    msg = 'Cassandra API version %s is not compatible with telephus' % ver
    raise APIMismatch(msg)

def translateArgs(request, api_version):
    args = request.args
    if request.method == 'system_add_keyspace' or \
       request.method == 'system_update_keyspace':
        if api_version == CASSANDRA_07:
            args = (args[0].to07(),)
        else:
            args = (args[0].to08(),)
    return args

def postProcess(results, method):
    if method == 'describe_keyspace':
        return translate_describe_ks(results)
    elif method == 'describe_keyspaces':
        return map(translate_describe_ks, results)
    else:
        return results

def translate_describe_ks(ksdef):
    if ksdef.strategy_options and 'replication_factor' in ksdef.strategy_options:
        ksdef.replication_factor = int(ksdef.strategy_options['replication_factor'])
    return ksdef
