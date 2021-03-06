class NodeInfo(object):

    def __init__(self, host, port, size_limit):
        self._host = host
        self._port = port
        self._size = 0
        self._tables = dict()
        self._size_limit = size_limit

    def add_size(self, size):
        self._size += size

    @property
    def host(self):
        return self._host

    @property
    def port(self):
        return self._port

    @property
    def size(self):
        return self._size

    @property
    def available_size(self):
        return self._size_limit - self._size

    def __getitem__(self, table_name):
        return self._tables[table_name]

    def __setitem__(self, table_name, table):
        self._tables[table_name] = table

    @property
    def address(self):
        return (self._host, self._port)

    @property
    def url(self):
        return '{}:{}'.format(self._host, self._port)

    def __str__(self):
        return '{}:{}. Total size: {}\nLimit size: {}'.format(self._host, self._port, self._size, self._size_limit)
