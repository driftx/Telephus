from telephus.cassandra.c07.constants import VERSION as CASSANDRA_07_VERSION
from telephus.cassandra.c08.constants import VERSION as CASSANDRA_08_VERSION

supported_versions = (
    ('0.8', CASSANDRA_08_VERSION),
    ('0.7', CASSANDRA_07_VERSION),
)

class APIMismatch(Exception):
    pass

def thrift_api_ver_to_cassandra_ver(remoteversion):
    """
    Try to determine if the remote thrift api version is likely to work the
    way we expect it to. A mismatch in major version number will definitely
    break, but a mismatch in minor version is probably ok if the remote side
    is higher (it should be backwards compatible). A change in the patch
    number should not affect anything noticeable.
    """
    r_major, r_minor, r_patch = map(int, remoteversion.split('.'))
    for cassversion, thriftversion in supported_versions:
        o_major, o_minor, o_patch = map(int, thriftversion.split('.'))
        if (r_major == o_major) and (r_minor >= o_minor):
            return thriftversion
    msg = 'Cassandra API version %s is not compatible with telephus' % ver
    raise APIMismatch(msg)

def translateArgs(request, api_version):
    args = request.args
    if request.method == 'system_add_keyspace' or \
       request.method == 'system_update_keyspace':
        if api_version == CASSANDRA_07_VERSION:
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
