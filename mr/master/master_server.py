import socket
import sys
from master_node import MasterNode


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

        action = conn.recv(1024)
        if action == 'write':
            master.redirect_to_free_node(conn)
        elif action.startswith('new_slave'):
            master.add_node(action)

    s.close()

# if __name__ == '__main__':
#     start('', 2222)
