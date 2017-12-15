import argparse
from client.client import Client
from master.master_server import start as master_start
from slave.slave_server import start as slave_start


def create_parser():
    parser = argparse.ArgumentParser(prog='mr', description='MapReduce',
                                     epilog='Have a good day')

    parser.add_argument('--write')
    parser.add_argument('--read')
    parser.add_argument('--delete')
    parser.add_argument('--table-info')
    parser.add_argument('--list-dir', action='store_true')

    parser.add_argument('--start-master', nargs=2)
    parser.add_argument('--start-slave', nargs=4)

    return parser


def run_command(namespace):
    if namespace.write:
        table_path = namespace.write
        client = Client()
        client.write(table_path)
    elif namespace.read:
        table_path = namespace.read
        client = Client()
        client.read(table_path)
    elif namespace.delete:
        print namespace.delete
    elif namespace.table_info:
        print namespace.table_info
    elif namespace.list_dir:
        print 'list_dir'
    elif namespace.start_master:
        host, port = namespace.start_master
        # host, port = '', 2222
        master_start(host, int(port))
    elif namespace.start_slave:
        port, master_host, master_port, size_limit = namespace.start_slave
        slave_start(int(port), master_host, int(master_port), float(size_limit))
    else:
        print 'undefined'


def start():
    parser = create_parser()
    namespace = parser.parse_args()
    run_command(namespace)
    # client_start('', 1111)
    # master_start('', 2222)
    # slave_start('', 4444)
