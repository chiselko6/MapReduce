class Table(object):

    def __init__(self, name):
        self._name = name

    def read_line(self):
        print 'reading table ', self._name
        with open(self._name, 'r') as fin:
            for line in fin:
                print 'table line: ', line
                yield line

    def write_line(self, line):
        # print 'table write_line: ', self._name, line
        with open(self._name, 'a+') as fout:
            fout.write(line + '\n')
