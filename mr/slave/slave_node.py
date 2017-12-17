import socket
from mr.table.table import Table
from mr.table.errors import TableNotFoundError
import subprocess
from mr.node_info import NodeInfo


class SlaveNode(object):

    LINE_MAX_SIZE = 1024

    def __init__(self, host, port, size_limit):
        self._tables = dict()
        self._node_info = NodeInfo(host, port, size_limit)

    def add_table(self, table_path):
        new_table = Table(table_path)
        self._tables[table_path] = new_table
        return new_table

    def write_table(self, table_path, lines):
        table = self._tables.get(table_path)
        if table is None:
            table = self.add_table(table_path)
        for line in lines:
            table.write_line(line)

    def write_table_line(self, table_path, line):
        table = self._tables.get(table_path)
        if table is None:
            table = self.add_table(table_path)
        if self._node_info.available_size < len(line):
            return False
        table.write_line(line)
        self._node_info.add_size(len(line))
        return True

    def read_table(self, table_path):
        # table_path = next(table_data)
        print 'reading ', table_path
        table = self._tables.get(table_path)
        if table is None:
            raise TableNotFoundError('Table not found', table_path)
        return table.read_line()

    def delete_table(self, table_data):
        table_path = next(table_data)
        print 'deleting', table_path
        table = self._tables.get(table_path)
        if table is None:
            raise TableNotFoundError('Table not found', table_path)
        table.delete()
        del self._tables[table_path]

    def map(self, table_in_path, table_out_path, script, helping_files=[]):
        # mv files here
        # table_in_path = next(map_data)
        # table_out_path = next(map_data)
        table_out = self._tables.get(table_out_path)
        if table_out is None:
            table_out = self.add_table(table_out_path)
        # script = next(map_data)
        # helping_files = [help_file for help_file in map_data]

        for line in self.read_table(table_in_path):
            # print line
            # subprocess.call(script)
            # table_out.write_line(raw_input())

            def simple_map(line):
                # line = raw_input()
                return line + '#' + line
            table_out.write_line(simple_map(line))

    def download_files(self, files):
        pass

    # def map(self, table_in, script, files):
    #     self.download_files(files)
    #     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #
    #     os.system(script)
