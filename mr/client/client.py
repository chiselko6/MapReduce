from config import MasterConfig
import socket
import sys
from client_errors import OutOfRecourseError
from mr.connect import Connect


class Client(object):

    def __init__(self):
        self._master_config = MasterConfig()

    def write(self, table_path):
        with Connect(self._master_config.host, self._master_config.port) as connect:
            connect.send_once('write')
            node_addr = connect.receive()

            if not node_addr:
                raise OutOfRecourseError('An error has occured')
            if node_addr.startswith('Error'):
                raise OutOfRecourseError('No free nodes')

            node_host, node_port = node_addr.split(':')
            node_port = int(node_port)
            with Connect(node_host, node_port) as node_connect:

                node_connect.send('write\n')
                node_connect.send(table_path + '\n')
                while True:
                    line = sys.stdin.readline()
                    if line == '':
                        break
                    node_connect.send(line)

    def read(self, table_path):
        with Connect(self._master_config.host, self._master_config.port) as connect:
            connect.send_once('read\n' + table_path)
            nodes_with_table = connect.receive_by_line()
            print 'received nodes generator'
            for node_addr in nodes_with_table:
                print 'parsing node_addr', node_addr
                node_host, node_port = node_addr.split(':')
                node_port = int(node_port)
                print 'getting table from ', node_host, node_port
                with Connect(node_host, node_port) as node_connect:
                    node_connect.send('read\n')
                    node_connect.send(table_path + '\n')
                    table_lines = node_connect.receive_by_line()
                    print 'got table_lines generator'
                    for table_line in table_lines:
                        print table_line

    def add_node(self, port, master_host, master_port, size_limit):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((master_host, master_port))
        s.sendall('add_node')
        node_infos = [
        # can take port from the caller??
            str(port),
            str(size_limit)
        ]
        for node_info in node_infos:
            s.sendall(node_info)
        s.close()
