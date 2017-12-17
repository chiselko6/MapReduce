class ServerError(Exception):

    def __init__(self, msg=None):
        self.msg = 'ERROR: ' + (msg or '')

    def __str__(self):
        return self.msg


class IncorrectMessageError(ServerError):

    def __init__(self, msg=None):
        msg = msg or 'Message is incorrect'
        ServerError.__init__(self, msg)
