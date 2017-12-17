import socket
from mr.table.table import Table
from mr.table.errors import TableNotFoundError
import subprocess
from mr.node_info import NodeInfo
import sys


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

    def map(self, table_in_path, table_out_path, script, exec_dir):
        total_size_written = 0
        for line in self.read_table(table_in_path):
            process = subprocess.Popen(script, cwd=exec_dir, shell=True, stdin=subprocess.PIPE, stdout=subprocess.PIPE)
            print 'input line:', line
            stdout, stderr = process.communicate(line)
            print 'map result', stdout
            for out_line in stdout.split('\n'):
                print 'out_line:', out_line
                was_written = self.write_table_line(table_out_path, out_line)
                print 'was_written', was_written
                if not was_written:
                    return False, total_size_written
                else:
                    total_size_written += len(out_line)
        return True, total_size_written
