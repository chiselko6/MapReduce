from table import Table


class NodeTableInfo(object):

    def __init__(self, node, size):
        self.node = node
        self.size = size

    def __str__(self):
        return '{}. Size: {}'.format(str(self.node), self.size)

class TableInfo(object):

    def __init__(self, name):
        self._node_infos = dict()
        self.name = name

    def add_node(self, node_addr, size):
        node_info = self._node_infos.get(node_addr)
        if node_info is None:
            self._node_infos[node_addr] = NodeTableInfo(node_addr, size)

    def remove_node(self, node_addr):
        node_info = self._node_infos.get(node_addr)
        if node_info is not None:
            del self._node_infos[node_addr]

    @property
    def size(self):
        return sum(map(lambda node: node.size, self.nodes))

    @property
    def nodes(self):
        return map(lambda inf: inf.node, self._node_infos.values())

    def __str__(self):
        return '{}\nTotal size: {}\nNodes used: {}'.format(self.name, self.size, len(self.nodes))
