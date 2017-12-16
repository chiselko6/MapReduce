import socket
import sys
from node_info import NodeInfo
from mr.table.table_info import TableInfo


class MasterNode(object):

    BUF_SIZE = 1024

    def __init__(self):
        self._tables = dict()
        self._nodes = dict()

    def add_node(self, host, port, size_limit):
        # msg, host, port, size_limit = new_slave_msg.split()
        node = NodeInfo(host, int(port), float(size_limit))
        self._nodes[(host, port)] = node
        # self._nodes.append(node)

    # def add_node(self, sock, addr):
    #     size_limit = sock.recv(self.BUF_SIZE)
    #     print addr, size_limit, self.BUF_SIZE
    #     node = NodeInfo(addr[0], addr[1], int(size_limit))
    #     self._nodes.append(node)

    def get_free_node(self):
        for node in self._nodes.values():
            if node.available_size > 0:
                return node

    def redirect_to_free_node(self, sock):
        free_node = self.get_free_node()
        if free_node is None:
            sock.sendall('Error: No free nodes')
            return False
        sock.sendall(free_node.url)
        return True

    def add_table_node(self, table_path, table_size, node):
        table = self._tables.get(table_path)
        if table is None:
            table = TableInfo(table_path)
            table.add_node(node, table_size)
            self._tables[table_path] = table
        else:
            table.add_node(node, table_size)

    def map(self, table_in, table_out, script, files):
        pass

    @property
    def nodes(self):
        return self._nodes.values()

    def get_node(self, node_addr):
        print 'get_node: ', node_addr
        print self._nodes.values()[0]
        return self._nodes.get(node_addr)

    def get_table_info(self, table_path):
        return self._tables.get(table_path)
