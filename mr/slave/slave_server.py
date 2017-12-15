import socket
import sys
from slave_node import SlaveNode
from mr.connect import Connect


def connect_to_master(host, port, master_host, master_port, size_limit):
    Connect(master_host, master_port).send('new_slave {} {} {}'.format(host, port, str(size_limit)))


def start(port, master_host, master_port, size_limit):
    host = 'localhost'
    connect_to_master(host, port, master_host, master_port, size_limit)

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    print 'Slave socket created'

    try:
        s.bind((host, port))
    except socket.error as msg:
        print 'Bind failed. Error Code : ' + str(msg[0]) + ' Message ' + msg[1]
        sys.exit()

    print 'Slave socket bind complete'

    s.listen(2)
    print 'Slave socket now listening at ', host, port

    slave = SlaveNode()

    while 1:
        conn, addr = s.accept()
        print 'Connected with ' + addr[0] + ':' + str(addr[1])

        with Connect(*addr) as connect:
        # connect = Connect(*addr)
            data_recv = connect.receive_by_line(conn)
            command = next(data_recv)
            # print 'command: ', command
            if command == 'write':
                # print 'table_name', table_name
                slave.write_table(data_recv)
            elif command == 'read':
                for line in slave.read_table(data_recv):
                    conn.sendall(line)

        conn.close()

    s.close()


# if __name__ == '__main__':
#     start('', 4444)
