import socket
import sys
from master_node import MasterNode
from mr.connect import Connect


def parse_input(data):
    print 'parsing: ', data
    if data == 'write':
        return data, ()
    elif data == 'read':
        return data, ()
    parts = data.split('\n')
    if parts[0] == 'table_add':
        return 'table_add', parts[1:]
    elif parts[0] == 'new_slave':
        return 'new_slave', parts[1:]


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
            print 'master action received', command, args
            if command == 'write':
                master.redirect_to_free_node(conn)
            elif command == 'read':
                pass
            elif command == 'table_add':
                node = master.get_node(conn.getsockname())
                new_table_args = args + [node]
                master.add_table_node(*new_table_args)

                print master.get_table_info(args[0])
            elif command == 'new_slave':
                master.add_node(*args)
    s.close()
