import sys
from mr.node_info import NodeInfo
from mr.table.table_info import TableInfo
from mr.table.errors import TableNotFoundError


class MasterNode(object):

    def __init__(self):
        self._tables = dict()
        self._nodes = dict()

    def add_node(self, host, port, size_limit):
        node = NodeInfo(host, int(port), float(size_limit))
        self._nodes[(host, port)] = node

    def get_free_node(self):
        for node in self._nodes.values():
            if node.available_size > 0:
                return node

    def add_table_node(self, table_path, table_size, node):
        table_size = float(table_size)
        table = self._tables.get(table_path)
        if table is None:
            table = TableInfo(table_path)
            table.add_node(node, table_size)
            self._tables[table_path] = table
        else:
            table.add_node(node, table_size)
        node.add_size(table_size)

    def remove_table_node(self, table_path, node):
        table = self._tables.get(table_path)
        if table is None:
            raise TableNotFoundError('Table not found', table_path)
        else:
            table.remove_node(node)
        if len(table.nodes) == 0:
            del self._tables[table_path]

    @property
    def nodes(self):
        return self._nodes.values()

    def get_table_nodes(self, table_path):
        return self._tables[table_path].nodes

    def get_node(self, node_addr):
        return self._nodes.get(node_addr)

    def get_table_info(self, table_path):
        return self._tables.get(table_path)

    def get_cluster_info(self, cluster_path):
        cluster_info = []
        for table in self._tables.values():
            if table.name.startswith(cluster_path):
                cluster_info.append(table.name)
        return cluster_info
