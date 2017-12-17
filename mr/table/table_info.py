from table import Table


class NodeTableInfo(object):

    def __init__(self, node, size):
        self.node = node
        self.size = size

    def __str__(self):
        return '{}. Size: {}'.format(str(self.node), self.size)

class TableInfo(object):

    def __init__(self, name):
        self._node_infos = []
        self.name = name

    def add_node(self, node_addr, size):
        node_info = self.get_by_node(node_addr)
        if node_info is None:
            self._node_infos.append(NodeTableInfo(node_addr, size))

    def remove_node(self, node_addr):
        for idx, inf in enumerate(self._node_infos):
            if inf.node.address == node_addr:
                self._node_infos = self._node_infos[:idx] + self._node_infos[idx + 1:]

    @property
    def size(self):
        return sum(map(lambda node: node.size, self._node_infos))

    def get_by_node(self, node_addr):
        for inf in self._node_infos:
            if inf.node.address == node_addr:
                return inf.node

    @property
    def nodes(self):
        return map(lambda inf: inf.node, self._node_infos)

    def __str__(self):
        return '{}\nTotal size: {}\nNodes used: {}'.format(self.name, self.size, len(self.nodes))
