# Harmonize functionality between the different OS implementations

import errno


def err_bind_inuse(err):
    return err == errno.EADDRINUSE


def err_conn_refused(errex):
    return (errex.errno in [errno.ECONNREFUSED, errno.EPIPE] or
            (hasattr(errex, 'winerror') and
             errex.winerror == 10057))  # 10057 == WSAENOTCONN


def err_send_inprogress(err):
    return err in [errno.EINPROGRESS, errno.EAGAIN]


def err_send_connrefused(errex):
    return err_conn_refused(errex)


def err_recv_retry(err):
    return err == errno.EAGAIN


def err_recv_connreset(errex):
    return (errex.errno in [errno.ECONNRESET, errno.EPIPE] or
            (hasattr(errex, 'winerror') and
             errex.winerror == 10053))  # 10053 == WSAECONNABORTED


def err_send_connreset(errex):
    return err_recv_connreset(errex)


def err_select_retry(err):
    return err in [errno.EINVAL, errno.EINTR]


def err_bad_fileno(err):
    return err == errno.EBADF


def err_too_many_open_sockets(errex):
    return errex.errno == errno.EMFILE


try:
    # Access these to see if the exist
    errno.WSAEINVAL
    errno.WSAEWOULDBLOCK

    # They exist, so use them
    def err_inprogress(err):
        return err in [errno.EINPROGRESS,
                       errno.WSAEINVAL,
                       errno.WSAEWOULDBLOCK]

    def err_recv_inprogress(err):
        return err in [errno.EAGAIN, errno.EWOULDBLOCK,
                       errno.WSAEWOULDBLOCK]

except Exception:
    # The above constants don't exist; use Linux standards
    def err_inprogress(err):
        return err == errno.EINPROGRESS

    def err_recv_inprogress(err):
        return err in [errno.EAGAIN, errno.EWOULDBLOCK]
