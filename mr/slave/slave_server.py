import socket
import sys
from slave_node import SlaveNode
from mr.connect import Connect
from mr.util.message import line_packing


def connect_to_master(host, port, master_host, master_port, size_limit):
    Connect(master_host, master_port).send_once('new_slave\n{}\n{}\n{}'.format(host, port, str(size_limit)))


def start(port, master_host, master_port, size_limit):
    host = '127.0.0.1'
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

        with Connect(socket=conn) as connect:
            data_recv = connect.receive_by_line()
            command = next(data_recv)
            if command == 'write':
                table_path = next(data_recv)
                slave.write_table(table_path, data_recv)
                with Connect(master_host, master_port) as master_connect:
                    # with size
                    new_table_inform = '\n'.join(['table_add', host, str(port), table_path, str(0)])
                    master_connect.send_once(new_table_inform)
            elif command == 'read':
                table_path = next(data_recv)
                for line in slave.read_table(table_path):
                    connect.send(line)
            elif command == 'delete':
                slave.delete_table(data_recv)
                with Connect(master_host, master_port) as master_connect:
                    removed_table_inform = '\n'.join(['table_remove', host, str(port), table_path])
                    master_connect.send_once(removed_table_inform)
            elif command == 'map':
                table_in = next(data_recv)
                table_out = next(data_recv)
                script = next(data_recv)
                helping_files = [help_file for help_file in data_recv]
                slave.map(table_in, table_out, script, helping_files)
                with Connect(master_host, master_port) as master_connect:
                    new_table_inform = line_packing('table_add', host, port, table_out, 0)
                    master_connect.send_once(new_table_inform)

    s.close()


# if __name__ == '__main__':
#     start('', 4444)
