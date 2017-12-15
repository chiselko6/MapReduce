class OutOfRecourseError(Exception):

    def __init__(self, msg, node=None):
        self.msg = msg
        self.node = node

    def __str__(self):
        msg = self.msg
        if self.node:
            msg += ' Failing on: ' + self.node.address
        return msg
