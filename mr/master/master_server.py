import socket
import sys
from master_node import MasterNode
from mr.connect import Connect
from mr.util.message import line_packing
from threading import Thread
from mr.server_errors import IncorrectMessageError, ServerError
from mapper_info import MasterMapper
import signal


mapper = MasterMapper()


def parse_input(data):
    parts = data.split('\n')
    return parts[0], parts[1:]


def inform_error(connect, msg):
    connect.send_once(msg)


def write(master, connect, args):
    free_node = master.get_free_node()
    if free_node is None:
        raise ServerError('No free node')
    else:
        connect.send_once(free_node.url)


def read(master, connect, args):
    if len(args) != 1:
        raise IncorrectMessageError()
    table_path = args[0]
    table_info = master.get_table_info(table_path)
    if table_info is None:
        raise ServerError('Requested table does not exist')
    table_nodes = table_info.nodes
    nodes_addr = map(lambda node: node.url, table_nodes)
    connect.send_once(line_packing(*nodes_addr))


def add_table(master, connect, args):
    if len(args) < 2:
        raise IncorrectMessageError()
    node = master.get_node((args[0], args[1]))
    if node is None:
        raise ServerError('Requested node does not exist')
    new_table_args = args[2:] + [node]
    master.add_table_node(*new_table_args)


def add_slave(master, connect, args):
    if len(args) != 3:
        raise IncorrectMessageError()
    master.add_node(*args)


def delete_table_request(master, connect, args):
    if len(args) != 1:
        raise IncorrectMessageError()
    table_path = args[0]
    table_info = master.get_table_info(table_path)
    if table_info is None:
        raise ServerError('Requested table does not exist')
    table_nodes = table_info.nodes
    nodes_addr = map(lambda node: node.url, table_nodes)
    connect.send_once(line_packing(*nodes_addr))


def remove_node_table(master, connect, args):
    if len(args) < 2:
        raise IncorrectMessageError()
    node = master.get_node((args[0], args[1]))
    remove_table_args = args[2:] + [node]
    master.remove_table_node(*remove_table_args)


def map_request(master, connect, args):
    if len(args) != 2:
        raise IncorrectMessageError()
    table_in, table_out = args
    map_info = mapper.create_map(table_in, table_out)
    table_nodes = master.get_table_nodes(table_in)
    table_nodes = map(lambda node: node.url, table_nodes)
    connect.send_once(line_packing(map_info.id, *table_nodes))


def get_table_info(master, connect, args):
    if len(args) != 1:
        raise IncorrectMessageError()
    table_name = args[0]
    table_info = master.get_table_info(table_name)
    if table_info is None:
        raise ServerError('Requested table does not exist')
    connect.send_once(str(table_info))


def list_dir(master, connect, args):
    if len(args) != 1:
        raise IncorrectMessageError()
    cluster_path = args[0]
    cluster_info = master.get_cluster_info(cluster_path)
    if not cluster_info:
        raise ServerError('Cluster not found')
    connect.send_once(line_packing(*cluster_info))


def handle_command(command, master, connect, args):
    print 'running command...', command, args
    if command == 'write':
        write(master, connect, args)
    elif command == 'read':
        read(master, connect, args)
    elif command == 'table_add':
        add_table(master, connect, args)
    elif command == 'new_slave':
        add_slave(master, connect, args)
    elif command == 'delete':
        delete_table_request(master, connect, args)
    elif command == 'table_remove':
        remove_node_table(master, connect, args)
    elif command == 'map':
        map_request(master, connect, args)
    elif command == 'table_info':
        get_table_info(master, connect, args)
    elif command == 'list_dir':
        list_dir(master, connect, args)


def handle_connection(master, conn, addr):
    with Connect(socket=conn) as connect:
        action = connect.receive()
        print 'action:', action
        command, args = parse_input(action)
        try:
            handle_command(command, master, connect, args)
        except ServerError as e:
            print e.msg
            inform_error(connect, e.msg)


def remove_finished():
    global threads
    remaining_threads = []
    for thr in threads:
        if thr.isAlive():
            remaining_threads.append(thr)
    threads = remaining_threads


threads = []
def start(host, port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind((host, port))
    except socket.error as msg:
        print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        sys.exit()
    print 'Socket bind complete at ', host, port
    s.listen(2)
    print 'Master now listening'

    master = MasterNode()
    while True:
        conn, addr = s.accept()
        print 'Connected with ' + addr[0] + ':' + str(addr[1])
        thread = Thread(target=handle_connection, args=(master, conn, addr))
        thread.start()
        threads.append(thread)
        remove_finished()

    s.close()


def shutdown_handler(signal, frame):
    for thr in threads:
        thr.join()
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown_handler)
