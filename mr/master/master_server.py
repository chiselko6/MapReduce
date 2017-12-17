import socket
import sys
from master_node import MasterNode
from mr.connect import Connect
from mr.util.message import line_packing


def parse_input(data):
    print 'parsing: ', data
    if data == 'write':
        return data, ()
    parts = data.split('\n')
    if parts[0] == 'read':
        return 'read', parts[1:]
    elif parts[0] == 'table_add':
        return 'table_add', parts[1:]
    elif parts[0] == 'table_remove':
        return 'table_remove', parts[1:]
    elif parts[0] == 'new_slave':
        return 'new_slave', parts[1:]
    elif parts[0] == 'delete':
        return 'delete', parts[1:]
    elif parts[0] == 'map':
        return 'map', parts[1:]
    elif parts[0] =='table_info':
        return 'table_info', parts[1:]


def start(host, port):

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print 'Socket created'

    try:
        s.bind((host, port))
    except socket.error as msg:
        print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        sys.exit()

    print 'Socket bind complete at ', host, port

    s.listen(2)
    print 'Master now listening'

    master = MasterNode()

    while 1:
        conn, addr = s.accept()
        print '/////////Connected with ' + addr[0] + ':' + str(addr[1])

        with Connect(socket=conn) as connect:

            action = connect.receive()
            command, args = parse_input(action)
            if command == 'write':
                free_node = master.get_free_node()
                if free_node is None:
                    connect.send_once('Error: No free nodes')
                else:
                    connect.send_once(free_node.url)
            elif command == 'read':
                table_path = args[0]
                table_nodes = master.get_table_info(table_path).nodes
                nodes_addr = map(lambda node: node.url, table_nodes)
                connect.send_once('\n'.join(nodes_addr))
            elif command == 'table_add':
                node = master.get_node((args[0], args[1]))
                new_table_args = args[2:] + [node]
                master.add_table_node(*new_table_args)
            elif command == 'new_slave':
                master.add_node(*args)
            elif command == 'delete':
                table_nodes = master.get_table_info(table_path).nodes
                nodes_addr = map(lambda node: node.url, table_nodes)
                connect.send_once('\n'.join(nodes_addr))
            elif command == 'table_remove':
                node = master.get_node((args[0], args[1]))
                remove_table_args = args[2:] + [node]
                master.remove_table_node(*remove_table_args)
            elif command == 'map':
                table_in = args[0]
                table_nodes = master.get_table_nodes(table_in)
                table_nodes = map(lambda node: node.url, table_nodes)
                connect.send_once(line_packing(*table_nodes))
            elif command == 'table_info':
                table_name = args[0]
                table_info = master.get_table_info(table_name)
                print 'master tableinfo:', table_info
                connect.send_once(str(table_info))

    s.close()
