class TableNotFoundError(Exception):

    def __init__(self, msg, table_name=None):
        self.msg = msg
        self.table_name = table_name

    def __str__(self):
        msg = self.msg
        if self.table_name:
            msg += ' Table path: ' + self.table_name
        return msg
