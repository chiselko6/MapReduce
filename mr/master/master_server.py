import socket
import sys
from master_node import MasterNode
from mr.connect import Connect
from mr.util.message import line_packing
from threading import Thread


def parse_input(data):
    parts = data.split('\n')
    return parts[0], parts[1:]


def write(master, connect, args):
    free_node = master.get_free_node()
    if free_node is None:
        connect.send_once('Error: No free nodes')
    else:
        connect.send_once(free_node.url)


def read(master, connect, args):
    table_path = args[0]
    table_nodes = master.get_table_info(table_path).nodes
    nodes_addr = map(lambda node: node.url, table_nodes)
    connect.send_once('\n'.join(nodes_addr))


def add_table(master, connect, args):
    node = master.get_node((args[0], args[1]))
    new_table_args = args[2:] + [node]
    master.add_table_node(*new_table_args)


def add_slave(master, connect, args):
    master.add_node(*args)


def delete_table_request(master, connect, args):
    table_path = args[0]
    table_nodes = master.get_table_info(table_path).nodes
    nodes_addr = map(lambda node: node.url, table_nodes)
    connect.send_once('\n'.join(nodes_addr))


def remove_node_table(master, connect, args):
    node = master.get_node((args[0], args[1]))
    remove_table_args = args[2:] + [node]
    master.remove_table_node(*remove_table_args)


def map_request(master, connect, args):
    table_in = args[0]
    table_nodes = master.get_table_nodes(table_in)
    table_nodes = map(lambda node: node.url, table_nodes)
    connect.send_once(line_packing(*table_nodes))


def get_table_info(master, connect, args):
    table_name = args[0]
    table_info = master.get_table_info(table_name)
    connect.send_once(str(table_info))


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

        with Connect(socket=conn) as connect:
            action = connect.receive()
            print 'action:', action
            command, args = parse_input(action)
            handle_command(command, master, connect, args)
            # thread = Thread(target=handle_command, args = (command, master, connect, args))
            # thread.start()
    s.close()
