import socket
import sys
from node_info import NodeInfo


class MasterNode(object):

    BUF_SIZE = 1024

    def __init__(self):
        self._tables = []
        self._nodes = []

    def add_node(self, new_slave_msg):
        msg, host, port, size_limit = new_slave_msg.split()
        node = NodeInfo(host, port, float(size_limit))
        self._nodes.append(node)

    # def add_node(self, sock, addr):
    #     size_limit = sock.recv(self.BUF_SIZE)
    #     print addr, size_limit, self.BUF_SIZE
    #     node = NodeInfo(addr[0], addr[1], int(size_limit))
    #     self._nodes.append(node)

    def get_free_node(self):
        for node in self._nodes:
            if node.available_size > 0:
                return node

    def redirect_to_free_node(self, sock):
        free_node = self.get_free_node()
        if free_node is None:
            sock.sendall('Error: No free nodes')
            return False
        sock.sendall(free_node.url)
        return True

    def call_slave(self, slave):
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect(slave.address)
        return s

    def map(self, table_in, table_out, script, files):
        pass
