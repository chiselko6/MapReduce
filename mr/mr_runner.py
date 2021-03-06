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
    parser.add_argument('--map', nargs='+')
    parser.add_argument('--table-info')
    parser.add_argument('--list-dir')

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
        client = Client()
        table_path = namespace.delete
        client.delete(table_path)
    elif namespace.map:
        client = Client()
        table_in, table_out, script = namespace.map[:3]
        helping_files = namespace.map[3:]
        map_id = client.map(table_in, table_out, script, helping_files)
        print 'Map id:', map_id
    elif namespace.table_info:
        client = Client()
        table_path = namespace.table_info
        table_info = client.get_table_info(table_path)
        print table_info
    elif namespace.list_dir:
        client = Client()
        cluster_path = namespace.list_dir
        cluster_info = client.list_dir(cluster_path)
        for cl in cluster_info:
            print cl
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
