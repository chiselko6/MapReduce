class MapperInfo(object):

    def __init__(self, map_id, table_in, table_out):
        self.id = map_id
        self.table_in = table_in
        self.table_out = table_out


class MasterMapper(object):

    def __init__(self):
        self._mappers = dict()
        self._map_id_gen = 0

    def create_map(self, table_in, table_out):
        map_id = self._map_id_gen
        self._map_id_gen += 1
        mapper = MapperInfo(map_id, table_in, table_out)
        self._mappers[map_id] = mapper
        return mapper
