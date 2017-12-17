import socket
import time


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

    def receive_by_line(self):
        if self._socket is None:
            self._connect()
        next_line = ''
        data = True

        while data:
            data = self._socket.recv(self.BUF_SIZE)
            next_line += data
            if '\n' in data:
                lines = next_line.split('\n')
                for line in lines[:-1]:
                    yield line
                next_line = lines[-1]
        yield next_line

    def send_file(self, proc_id, file_path):
        with open(file_path, 'r') as fin:
            for line in fin.readlines():
                self.send(line)
        self._socket.shutdown(socket.SHUT_WR)

    def receive_file(self, proc_id, file_path):
        with open(file_path, 'w') as fout:
            for line in self.receive_by_line():
                fout.write(line + '\n')

    def receive(self, timeout=0.3):
        if self._socket is None:
            self._connect()
        self._socket.setblocking(0)
        total_data = []

        begin = time.time()
        while True:
            if total_data and time.time() - begin > timeout:
                break
            elif time.time() - begin > timeout * 2:
                break

            try:
                data = self._socket.recv(self.BUF_SIZE)
                if data:
                    total_data.append(data)
                    begin = time.time()
                else:
                    time.sleep(0.05)
            except:
                pass

        return ''.join(total_data)

    def close(self):
        self._socket.close()
        self._socket = None

    def stop_receive(self):
        self.shutdown(socket.SHUT_RD)

    def __enter__(self):
        if self._socket is None:
            self._connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    @property
    def socket(self):
        return self._socket
