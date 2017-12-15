import os


class Mapper(object):

    def __init__(self):
        pass

    # table_in, table_out -> TableInfo
    def map(self, table_in, table_out, script, files):
        for record in table_in.read():
            os.system(script)
