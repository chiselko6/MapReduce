import socket
import time


class Connect(object):

    BUF_SIZE = 1024
    FILE_SIZE_BUF = 4

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

    def send_line_and_receive(self, command):
        self.send(str(command) + '\n')
        recv_data = ''
        data = True

        while data:
            data = self.receive()
            recv_data += data

        return recv_data or False

    def receive_by_line(self):
        if self._socket is None:
            self._connect()
        next_line = ''
        data = True

        while data:
            data = self.receive()
            next_line += data
            # print 'next_line before: ', next_line
            if '\n' in data:
                lines = next_line.split('\n')
                for line in lines[:-1]:
                    yield line
                next_line = lines[-1]
            # print 'received: ' + data
        yield next_line

    def send_file(self, proc_id, file_path):
        print 'sending file to', proc_id, file_path
        with open(file_path, 'r') as fin:
            for line in fin.readlines():
                print 'sending line...'
                print line
                self.send(line)
        self._socket.shutdown(socket.SHUT_WR)

    def receive_file(self, proc_id, file_path):
        print 'receiving file to', proc_id, file_path
        # file_size = int(self.receive_sized(self.FILE_SIZE_BUF))
        with open(file_path, 'w') as fout:
            for line in self.receive_by_line():
                print 'received line:', line
                fout.write(line + '\n')

    def receive_sized(self, max_size):
        if self._socket is None:
            self._connect()
        data = True
        recv = ''
        recv_size = 0

        while data and recv_size < max_size:
            print 'wait another data...'
            chunk = self.BUF_SIZE if recv_size + self.BUF_SIZE <= max_size else max_size - recv_size
            data = self._socket.recv(chunk)
            recv_size += chunk
            print 'received data: ', data
            recv += data

        return recv

    def receive(self, timeout=0.3):
        if self._socket is None:
            self._connect()

        self._socket.setblocking(0)
        total_data = []

        begin = time.time()
        print 'started at', begin
        while True:
            #if you got some data, then break after timeout
            if total_data and time.time() - begin > timeout:
                break

            #if you got no data at all, wait a little longer, twice the timeout
            elif time.time() - begin > timeout * 2:
                break

            #recv something
            try:
                data = self._socket.recv(8192)
                if data:
                    total_data.append(data)
                    #change the beginning time for measurement
                    begin = time.time()
                else:
                    #sleep for sometime to indicate a gap
                    time.sleep(0.1)
            except:
                pass

        #join all parts to make final string
        return ''.join(total_data)
        # data = True
        # recv = ''
        #
        # while data:
        #     print 'wait another data...'
        #     data = self._socket.recv(self.BUF_SIZE)
        #     print 'received data: ', data
        #     recv += data
        #
        # return recv

    def close(self):
        self._socket.close()
        self._socket = None

    def stop_receive(self):
        self.shutdown(socket.SHUT_RD)

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
