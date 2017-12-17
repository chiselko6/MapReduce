import os

class Table(object):

    def __init__(self, name):
        self._name = name
        basedir = os.path.dirname(name)
        if not os.path.exists(basedir):
            os.makedirs(basedir)
        self._size = 0

    def read_line(self):
        with open(self._name, 'r') as fin:
            for line in fin:
                yield line

    def read_all(self):
        with open(self._name, 'r') as fin:
            return fin.readlines()

    def write_line(self, line):
        added_size = len(line)
        with open(self._name, 'a+') as fout:
            fout.write(line + '\n')
        self._size += added_size
        return added_size

    def delete(self):
        self._size = 0
        os.remove(self._name)

    @property
    def size(self):
        return self._size
