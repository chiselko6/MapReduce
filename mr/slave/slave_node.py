import socket
from table.table import Table
from table.errors import TableNotFoundError


class SlaveNode(object):

    LINE_MAX_SIZE = 1024

    def __init__(self):
        self._tables = dict()

    def add_table(self, table_path):
        new_table = Table(table_path)
        self._tables[table_path] = new_table
        return new_table

    def write_table(self, table_data):
        table_path = next(table_data)
        table = self._tables.get(table_path)
        if table is None:
            table = self.add_table(table_path)
        for line in table_data:
            # print 'received line: ', line
            table.write_line(line)
            # print 'written ', line

    def read_table(self, table_data):
        table_path = next(table_data)
        print 'reading ', table_path
        table = self._tables.get(table_path)
        if table is None:
            raise TableNotFoundError('Table not found', table_path)
        return table.read_line()

    def download_files(self, files):
        pass

    # def map(self, table_in, script, files):
    #     self.download_files(files)
    #     s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #
    #     os.system(script)
