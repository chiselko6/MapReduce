from config import MasterConfig
import socket
import sys
from client_errors import OutOfRecourseError
from mr.connect import Connect
from mr.util.message import line_packing
from threading import Thread


class Client(object):

    def __init__(self):
        self._master_config = MasterConfig()

    def write(self, table_path):
        sent = False
        line = False
        while not sent:
            with Connect(self._master_config.host, self._master_config.port) as connect:
                connect.send('write')
                node_addr = connect.receive()
                if not node_addr:
                    raise RuntimeError('An error has occured')
                if node_addr.startswith('ERROR:'):
                    raise OutOfRecourseError(node_addr)

                node_host, node_port = node_addr.split(':')
                node_port = int(node_port)
                with Connect(node_host, node_port) as node_connect:

                    node_connect.send('write\n')
                    node_connect.send(table_path + '\n')

                    received = 'ok'
                    while True:
                        if not line:
                            line = sys.stdin.readline()
                        print 'read line:', line
                        if line == '':
                            sent = True
                            break
                        node_connect.send(line)
                        received = node_connect.receive()
                        if received == 'ok':
                            line = False
                        else:
                            break

    def read(self, table_path):
        with Connect(self._master_config.host, self._master_config.port) as connect:
            connect.send_once('read\n' + table_path)
            nodes_with_table = connect.receive_by_line()
            if not nodes_with_table:
                raise RuntimeError('Cannot get nodes with table')
            for node_addr in nodes_with_table:
                if node_addr.startswith('ERROR:'):
                    raise KeyError(node_addr)
                node_host, node_port = node_addr.split(':')
                node_port = int(node_port)
                with Connect(node_host, node_port) as node_connect:
                    node_connect.send('read\n')
                    node_connect.send(table_path + '\n')
                    table_lines = node_connect.receive_by_line()
                    for table_line in table_lines:
                        print table_line

    def delete(self, table_path):
        with Connect(self._master_config.host, self._master_config.port) as connect:
            connect.send_once('delete\n' + table_path)
            nodes_with_table = connect.receive_by_line()
            if not nodes_with_table:
                raise RuntimeError('Cannot get nodes with table')
            for node_addr in nodes_with_table:
                if node_addr.startswith('ERROR:'):
                    raise KeyError(node_addr)
                node_host, node_port = node_addr.split(':')
                node_port = int(node_port)
                with Connect(node_host, node_port) as node_connect:
                    node_connect.send('delete\n')
                    node_connect.send(table_path + '\n')

    def map(self, table_in, table_out, script, helping_files=[]):
        is_all_mapped = True
        with Connect(self._master_config.host, self._master_config.port) as connect:
            connect.send_once(line_packing('map', table_in, table_out))
            map_info = connect.receive_by_line()
            if not map_info:
                raise KeyError('Cannot get nodes with table')
            map_id = next(map_info)
            if map_id.startswith('ERROR:'):
                raise KeyError(map_id)

            def map_node(node_addr):
                if node_addr.startswith('ERROR:'):
                    raise KeyError(node_addr)
                node_host, node_port = node_addr.split(':')
                node_port = int(node_port)
                map_msg = 'map\n'
                with Connect(node_host, node_port) as node_connect:
                    node_connect.send_once(map_msg)
                    proc_id = node_connect.receive()
                    if proc_id.startswith('ERROR:'):
                        raise RuntimeError(proc_id)

                for help_file in helping_files:
                    with Connect(node_host, node_port) as node_connect:
                        help_file_local_path = help_file.split('/')[-1]
                        file_add_msg = line_packing(
                            'file_add', proc_id, help_file_local_path)
                        node_connect.send(file_add_msg + '\n')
                        file_send_start_status = node_connect.receive()
                        if file_send_start_status == 'ok':
                            node_connect.send_file(proc_id, help_file)

                map_msg = line_packing(
                    'map_run', table_in, table_out, script, proc_id, line_packing(helping_files))
                with Connect(node_host, node_port) as node_connect:
                    node_connect.send_once(map_msg)
                    node_connect.receive()

            for node_addr in map_info:
                thread = Thread(target=map_node, args=(node_addr,))
                thread.start()
        return map_id

    def get_table_info(self, table_path):
        with Connect(self._master_config.host, self._master_config.port) as connect:
            connect.send('table_info\n')
            connect.send(table_path)
            table_info = connect.receive()
            if table_info.startswith('ERROR:'):
                raise KeyError(table_info)
            return table_info

    def list_dir(self, cluster_path):
        with Connect(self._master_config.host, self._master_config.port) as connect:
            connect.send(line_packing('list_dir', cluster_path))
            cluster_info = connect.receive_by_line()
            for cl in cluster_info:
                if cl.startswith('ERROR:'):
                    raise RuntimeError(cl)
                yield cl
