import os


class MasterConfigParser(object):

    def __init__(self, config_path):
        self._config_path = config_path
        self._host = None
        self._port = None

    def _read(self):
        self._host = '127.0.0.1'
        self._port = 2222
        # with open(self._config_path, 'r') as fin:
        #     lines = fin.readlines()
        #     assert len(lines) == 1, 'Config file is invalid'
        #     self._host, self._port = lines[0].split(':')
        #     self._port = int(self._port)

    def _write_new_config(self, config):
        with open(self._config_path, 'w') as fout:
            fout.write(config)

    def _write(self, host=None, port=None):
        self._read()
        config = '{}:{}'.format(host or self._host, port or self._port)
        self._write_new_config(config)

    def update(self, host=None, port=None):
        self._write(host, port)

    @property
    def host(self):
        self._read()
        return self._host

    @property
    def port(self):
        self._read()
        return self._port

    @property
    def url(self):
        self._read()
        return '{}:{}'.format(self._host, self._port)


class MasterConfig(object):

    def __init__(self, config_path=None):
        config_path = config_path or '.mr_master_host.conf'
        self._config_parser = MasterConfigParser(config_path)

    @property
    def address(self):
        return (self._config_parser.host, self._config_parser.port)

    @property
    def port(self):
        return self._config_parser.port

    @property
    def host(self):
        return self._config_parser.host
