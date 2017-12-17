import os


class SlaveFileManager(object):

    def __init__(self):
        self.latest_slot = 0
        self.space_path = '/free_space/'

    def get_empty_slot(self):
        return str(self.latest_slot + 1)

    def get_slot_path(self, slot):
        return '{}{}/'.format(self.space_path, str(slot))

    def reserve_space(self, slot):
        slot_path = self.get_slot_path(slot)
        if not os.path.exists(slot_path):
            os.mkdir(slot_path)
        self.latest_slot += 1
        return slot_path

    def receive_file(self, connect, proc_id, file_name):
        file_path = '{}{}'.format(self.get_slot_path(proc_id), file_name)
        connect.receive_file(proc_id, file_path)
