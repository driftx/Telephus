import os

vers = [v for v in os.listdir(os.path.dirname(__file__)) if v.startswith('c') and v[1:].isdigit()]
vers.sort()
max_ver = vers[-1]

latest_api_pkg = __import__('telephus.cassandra.' + max_ver, fromlist=('ttypes', 'constants', 'Cassandra'))

ttypes = latest_api_pkg.ttypes
constants = latest_api_pkg.constants
Cassandra = latest_api_pkg.Cassandra
