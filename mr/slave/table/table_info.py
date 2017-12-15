from table import Table


class TableInfo(object):

    class NodeTableInfo(object):

        def __init__(self, node, name, rec_from, rec_to):
            self.node = node
            self.rec_from = rec_from
            self.rec_to = rec_to
            self.name = name


    def __init__(self):
        self._size = 0
        self._node_infos = []

    def add_node(self, node, table_path, record_from, record_to):
        self._node_infos.append(NodeTableInfo(node, table_path, record_from, record_to))

    @property
    def tables(self):
        return map(lambda inf: Table(inf.node, inf.name), self._nodes_info)
