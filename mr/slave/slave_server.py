import socket
import sys
from slave_node import SlaveNode
from mr.connect import Connect
from mr.util.message import line_packing
from file_manager import SlaveFileManager
from threading import Thread
from mr.server_errors import IncorrectMessageError, ServerError
import signal
from time import sleep


master_addr = None
slave_addr = None


def parse_input(data):
    parts = data.split('\n')
    return parts[0], parts[1:]


def inform_error(connect, msg):
    connect.send_once(msg)


def connect_to_master(size_limit):
    connect_msg = line_packing(
        'new_slave', slave_addr[0], slave_addr[1], size_limit)
    Connect(*master_addr).send_once(connect_msg)


def write(slave, connect, args):
    table_path = next(args)
    size_written = 0
    for line in args:
        is_written = slave.write_table_line(table_path, line)
        if not is_written:
            connect.send_once('not ok')
            break
        else:
            connect.send('ok')
        size_written += len(line)
    with Connect(*master_addr) as master_connect:
        new_table_inform = line_packing(
            'table_add', slave_addr[0], slave_addr[1], table_path, size_written)
        master_connect.send_once(new_table_inform)


def read(slave, connect, args):
    table_path = next(args)
    for line in slave.read_table(table_path):
        connect.send(line)


def delete_table(slave, connect, args):
    table_path = next(args)
    slave.delete_table(table_path)
    with Connect(*master_addr) as master_connect:
        removed_table_inform = line_packing(
            'table_remove', slave_addr[0], slave_addr[1], table_path)
        master_connect.send_once(removed_table_inform)


def map_request(slave, file_manager, connect, args):
    proc_id = file_manager.get_empty_slot()
    file_manager.reserve_space(proc_id)
    connect.send_once(proc_id)


def add_file(slave, file_manager, connect, args):
    proc_id = next(args)
    help_file = next(args)
    connect.send('ok')
    file_manager.receive_file(connect, proc_id, help_file)


def map_start(slave, file_manager, connect, args):
    table_in = next(args)
    table_out = next(args)
    script = next(args)
    proc_id = next(args)

    exec_dir = file_manager.get_slot_path(proc_id)
    is_mapped, size_written = slave.map(table_in, table_out, script, exec_dir)
    connect.send_once('ok' if is_mapped else 'not ok')
    with Connect(*master_addr) as master_connect:
        new_table_inform = line_packing(
            'table_add', slave_addr[0], slave_addr[1], table_out, size_written)
        master_connect.send_once(new_table_inform)


def handle_command(command, slave, file_manager, connect, args):
    if command == 'write':
        write(slave, connect, args)
    elif command == 'read':
        read(slave, connect, args)
    elif command == 'delete':
        delete_table(slave, connect, args)
    elif command == 'map':
        map_request(slave, file_manager, connect, args)
    elif command == 'file_add':
        add_file(slave, file_manager, connect, args)
    elif command == 'map_run':
        map_start(slave, file_manager, connect, args)


def handle_connection(slave, file_manager, conn, addr):
    with Connect(socket=conn) as connect:
        data_recv = connect.receive_by_line()
        command = next(data_recv)
        try:
            handle_command(command, slave, file_manager, connect, data_recv)
        except ServerError as e:
            inform_error(connect, e.msg)
        except StopIteration as e:
            inform_error(connect, 'ERROR: Invalid message')
        except Exception as e:
            inform_error(connect, str(e))


def remove_finished():
    global threads
    remaining_threads = []
    for thr in threads:
        if thr.isAlive():
            remaining_threads.append(thr)
    threads = remaining_threads


threads = []
def start(port, master_host, master_port, size_limit):
    global master_addr
    global slave_addr
    master_addr = (master_host, master_port)
    host = '127.0.0.1'
    slave_addr = (host, port)

    connect_to_master(size_limit)

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

    slave = SlaveNode(host, port, size_limit)
    file_manager = SlaveFileManager()

    while True:
        conn, addr = s.accept()
        print 'Connected with ' + addr[0] + ':' + str(addr[1])
        thread = Thread(target=handle_connection, args=(
            slave, file_manager, conn, addr))
        thread.start()
        threads.append(thread)
        remove_finished()

    s.close()


def shutdown_handler(signal, frame):
    for thr in threads:
        thr.join()
    sys.exit(0)


signal.signal(signal.SIGINT, shutdown_handler)
