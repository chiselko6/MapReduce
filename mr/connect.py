import socket


class Connect(object):

    BUF_SIZE = 1024

    def __init__(self, host=None, port=None, socket=None):
        self.host = host
        self.port = port
        self._socket = socket
        if socket is not None:
            self.host, self.port = socket.getsockname()

    def _connect(self):
        self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket.connect((self.host, self.port))

    def send(self, msg):
        if self._socket is None:
            self._connect()
        self._socket.sendall(msg)

    def send_once(self, msg):
        self.send(msg)
        self._socket.shutdown(socket.SHUT_WR)

    def send_and_receive(self, command):
        # sock = self.connect()
        recv_data = ''
        data = True

        self.send(command)
        # self.close()
        # self._connect()

        while data:
            data = self.receive()
            recv_data += data

        # sock.close()
        return recv_data

    def receive_by_line(self):
        if self._socket is None:
            self._connect()
        next_line = ''
        data = True

        while data:
            data = self._socket.recv(self.BUF_SIZE)
            next_line += data
            # print 'next_line before: ', next_line
            if '\n' in data:
                lines = next_line.split('\n')
                for line in lines[:-1]:
                    yield line
                next_line = lines[-1]
            # print 'received: ' + data
        yield next_line

    def send_file(self, file_path):
        with open(file_path, 'r') as fin:
            for line in fin.readline():
                self.send(line)
        self._socket.shutdown(socket.SHUT_WR)

    def receive_file(self, file_path):
        with open(file_path, 'w') as fout:
            fout.write(self.receive())

    def receive(self):
        if self._socket is None:
            self._connect()
        data = True
        recv = ''

        while data:
            print 'wait another data...'
            data = self._socket.recv(self.BUF_SIZE)
            print 'received data: ', data
            recv += data

        return recv

    def close(self):
        self._socket.close()
        self._socket = None

    def __enter__(self):
        print 'connecting to host...', self.host, self.port
        if self._socket is None:
            self._connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def socket(self):
        return self._socket
