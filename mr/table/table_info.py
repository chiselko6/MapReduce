from table import Table


class NodeTableInfo(object):

    def __init__(self, node, size):
        self.node = node
        # self.rec_from = rec_from
        # self.rec_to = rec_to
        # self.name = name
        self.size = size

    def __str__(self):
        return '{}. Size: {}'.format(str(self.node), self.size)

class TableInfo(object):

    def __init__(self, name):
        self._size = 0
        self._node_infos = dict()
        self.name = name

    def add_node(self, node, size):
        node_info = self._node_infos.get(node)
        if node_info is None:
            self._node_infos[node] = NodeTableInfo(node, size)

    def remove_node(self, node):
        node_info = self._node_infos.get(node)
        if node_info is not None:
            del self._node_infos[node]

    @property
    def nodes(self):
        return map(lambda inf: inf.node, self._node_infos.values())

    def __str__(self):
        return '{}\nTotal size: {}\n{}'.format(self.name, self._size, '\n'.join(map(str, self._node_infos)))
    # @property
    # def tables(self):
    #     return map(lambda inf: Table(inf.node, inf.name), self._nodes_info)
