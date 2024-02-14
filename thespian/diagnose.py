#!/usr/bin/env python3

# Script used to diagnose the current environment to look for conditions or
# configurations that would interfere with Thespian being able to startup
# successfully.

from datetime import timedelta
import os
import sys
import tempfile
import threading

class Diagnoser:
    def __init__(self, do_colors=True):
        self._marks = { 'red': "\033[0;31m",
                        'green': "\033[0;32m",
                        'magenta': "\033[0;35m",
                        'yellow': "\033[0;33m",
                        'reset': "\033[0m",
                        'bold': "\033[1m",
                        'reprint': "\r",
                       } if do_colors else {}
        self._checking = None
        self._diaglog = open('thespian_diagnostics.log', 'wt')
        import platform
        if platform.system() == "Windows":
            # set Windows console to VT mode
            kernel32 = __import__("ctypes").windll.kernel32
            kernel32.SetConsoleMode(kernel32.GetStdHandle(-11), 7)
            del kernel32
    def __del__(self):
        print('--> Wrote',self._diaglog.name)
    def _as(self, msg, *marks, **kw):
        mark = ''.join([self._marks.get(m,'') for m in marks])
        print(mark,msg,self._marks.get('reset',''), sep='', flush=True, **kw)
        if 'reprint' in marks:
            print('', file=self._diaglog)
        print(msg, file=self._diaglog, flush=True, **kw)
    def report(self, rpt):
        self._as(rpt, 'bold')
    def info(self, lbl, info):
        print('  ','[{:.>12}]'.format(lbl),':',info, flush=True)
        print('  ','[{:.>12}]'.format(lbl),':',info, flush=True, file=self._diaglog)
    def warn(self, warn, *supplemental):
        print()
        self._as("## WARNING: ", 'bold', 'yellow', end='')
        print(warn)
        print(warn, file=self._diaglog)
        for s in supplemental:
            self._as('##            ', 'yellow', end='')
            print(s)
            print(s, file=self._diaglog)
        sys.stdout.flush()
        self._diaglog.flush()
    def _show_checking(self):
        if self._checking:
            self._as('# checking ', 'reprint', end='')
            self._as(self._checking, 'magenta', end=' ')
    def _start_check(self, name):
        self._checking = name
        self._show_checking()
        print("... ", end='')
        sys.stdout.flush()
    def _success(self):
        self._show_checking()
        self._checking = None
        self._as("-> verified ok", 'green')
    def _skipped(self, why):
        self._show_checking()
        self._checking = None
        self._as("was skipped -", 'yellow', end=' ')
        print(why, flush=True)
    def _failure(self):
        sys.stderr.flush()
        self._show_checking()
        self._checking = None
        self._as("has FAILED", 'red', 'bold')

    def _check(*show_args):
        def _chk(op):
            self_offset = (1 if op.__code__.co_varnames
                           and op.__code__.co_varnames[0] == 'self'
                           else 0)
            sa = [ (a, op.__code__.co_varnames.index(a) - self_offset)
                   for a in show_args
                   if a in op.__code__.co_varnames[:op.__code__.co_argcount] ]
            def wrap(self, *args, **kw):
                n = op.__name__.replace('check_','').replace("_"," ")
                ds = [ ('{}={}'.format(d,v) if v is not None else None)
                       for (d,i) in sa
                       for v in [kw.get(d, args[i] if i < len(args) else None)]
                      ]
                d = ' '.join(filter(None, [n] + ds))
                self._start_check(d)
                tf = tempfile.NamedTemporaryFile()
                os.putenv('THESPLOG_FILE', tf.name)
                os.putenv('THESPLOG_THRESHOLD', 'DEBUG')
                try:
                    rval = op(self, *args, **kw)
                    self._success()
                    return rval
                except Skipped as e:
                    self._skipped(e.why)
                except Exception:
                    self._failure()
                    raise
                finally:
                    tf.seek(0)
                    for l in tf:
                        print(l, file=self._diaglog)
                    if tf.tell():
                        print('## Copied %d bytes from %s thesplog into diaglog'
                              % (tf.tell(), tf.name),
                              file=self._diaglog)
                        print('', file=self._diaglog, flush=True)
            return wrap
        return _chk

    @_check()
    def check_imports(self):
        try:
            import thespian
            return True
        except ImportError:
            myfile = sys.modules['__main__'].__file__
            p = os.path.join(os.getcwd(),
                             os.path.dirname(os.path.dirname(myfile)))
            self.warn('thespian installation missing from PYTHONPATH'
                      '; consider adding it',
                      'Example',
                      '  export PYTHONPATH={}:$PYTHONPATH'.format(p))
            raise

    @_check()
    def check_thespian_internal_system_imports(self):
        # Assumes check_imports has already been called to verify the path
        import thespian.system

    @_check()
    def check_existing_running_actors(self):
        # Note: this will only work if any actors were used from a Thespian
        # process with setproctitle installed, such that the process name
        # contains 'ActorAddr'.  This is mostly informative.
        try:
            import psutil
            actors = [ '%d %s' % (p.pid, ' '.join(p.cmdline()))
                       for p in psutil.process_iter()
                       if any(map(lambda a: 'ActorAddr' in a, p.cmdline()))
                      ]
        except ImportError:
            if sys.platform == 'linux':
                # invoke the 'ps' utility to get this information
                from subprocess import Popen, PIPE
                p = Popen(['ps', '-eo', 'pid,args'], stdout=PIPE)
                o, _ = p.communicate()
                actors = [ l.decode('utf-8')
                           for l in o.splitlines()
                           if b'ActorAddr' in l]
            else:
                raise Skipped('please install psutils python package to support this')
        if actors:
            print('%d running actors detected' % len(actors))
            for a in actors:
                print('   ',a)
        else:
            print('no running actors identified')

    @_check()
    def check_IP_addresses(self):
        import thespian.system.transport.IPBase as IPBase
        addrs = IPBase.getLocalAddresses()
        if addrs:
            print('Got %d IP addresses' % len(addrs))
            for a in addrs:
                print('   ',a)
        else:
            self.warn('No IP addresses detected')

    @_check()
    def check_hostname(self):
        import socket
        try:
            return socket.gethostname()
        except Exception:
            self.warn('No hostname detected',
                      '[Result of: socket.gethostname()]',
                      'This will not affect Actor Systems running on this host',
                      'but it will potentially prevent joining an Actor System',
                      'Convention with other hosts and communicating with Actors',
                      'on those hosts')
            return None

    @_check()
    def check_fqdn(self):
        import socket
        try:
            return socket.getfqdn()
        except Exception:
            self.warn('No fully-qualified domain name detected.',
                      '[Result of: socket.getfqdn()]',
                      'This will not affect Actor Systems running on this host',
                      'but it will potentially prevent joining an Actor System',
                      'Convention with other hosts and communicating with Actors',
                      'on those hosts')
            return None

    @_check('addr', 'proto', 'desc', 'usage')
    def check_addr_info(self, addr, usage, proto, desc):
        # if not can_run:
        #     raise Skipped('due to prevously noted issues with hostname/fqdn')
        import socket
        af, st, p = socket_args(proto)
        try:
            socket.getaddrinfo(addr, 0, af, st, p, socket.AI_PASSIVE if usage else 0)
        except Exception:
            self.warn("Unable to get address info",
                      "[Result of socket.getaddrinfo()]",
                      'This will not affect Actor Systems running on this host',
                      'but it will potentially prevent joining an Actor System',
                      'Convention with other hosts and communicating with Actors',
                      'on those hosts')

    @_check('base', 'port')
    def check_AdminPort_available(self, proto, port, base):
        import socket
        af, st, p = socket_args(proto)
        s = socket.socket(af, st)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind(('', port))
            return True
        except OSError as e:
            from thespian.system.transport.errmgmt import err_bind_inuse
            if err_bind_inuse(e.errno):
                import thespian.actors
                asys = thespian.actors.ActorSystem(base)
                # If this completed successfully, there is a running
                # Actor System available and everything is OK.
                print('Existing %s running at port %d' % (base, port))
                # all good, but cannot run subsequent actor tests because
                # there's no guarantee the Actors are known, so return False
                return False
            else:
                raise
        finally:
            s.close()
            del s

    def check_multiprocUDPBase_port_available(self):
        import thespian.system.transport.UDPTransport as UDPT
        return self.check_AdminPort_available("UDP", UDPT.DEFAULT_ADMIN_PORT,
                                              "multiprocUDPBase")

    def check_multiprocTCPBase_port_available(self):
        import thespian.system.transport.TCPTransport as TCPT
        return self.check_AdminPort_available("TCP", TCPT.DEFAULT_ADMIN_PORT,
                                              "multiprocTCPBase")

    @_check('proto', 'port')
    def check_socket_communications(self, can_run, proto, port):
        if not can_run:
            raise Skipped('due to prevously noted issues with TCP actor systems')
        af, p, _ = socket_args(proto)
        server_success = threading.Event()
        client_success = threading.Event()
        server = SockServer(proto=proto,
                            port=port,
                            flag=server_success,
                            report=self._diaglog,
                            name=proto+"server")
        client = SockClient(proto=proto,
                            port=port,
                            flag=client_success,
                            report=self._diaglog,
                            name=proto+"client")
        server.start()
        import time
        time.sleep(1)  # let server start and bind the address
        client.start()
        client.join(timeout=2.0)
        server.join(timeout=5.0)
        if not server_success.is_set():
            self.info('server','did not register successful completion')
        if not client_success.is_set():
            self.info('client','did not register successful completion')
        if not (server_success.is_set() and server_success.is_set()):
            self._failure()

    def check_TCP_sockets(self, can_run):
        import thespian.system.transport.TCPTransport as TCPT
        return self.check_socket_communications(can_run, 'TCP',
                                                TCPT.DEFAULT_ADMIN_PORT)

    def check_UDP_sockets(self, can_run):
        import thespian.system.transport.UDPTransport as UDPT
        return self.check_socket_communications(can_run, 'UDP',
                                                UDPT.DEFAULT_ADMIN_PORT)

    @_check()
    def check_simpleSystemBase(self):
        from subprocess import Popen, PIPE
        myfile = sys.modules['__main__'].__file__
        hello = os.path.join(os.getcwd(),
                         os.path.dirname(os.path.dirname(myfile)),
                         'examples', 'hellogoodbye.py')
        p = Popen([sys.executable, hello], stdout=PIPE, stderr=PIPE)
        o, e = p.communicate()
        import platform
        if platform.system() == "Windows":
            assert o == b'Hello, world!\r\nGoodbye\r\n'
        else:
            assert o == b'Hello, world!\nGoodbye\n'
        assert e == b''

    @_check('base')
    def check_simple_actors(self, can_run, base):
        if not can_run:
            raise Skipped('due to prevously noted issues with %s actor systems'
                          % base)
        from subprocess import Popen, PIPE
        myfile = sys.modules['__main__'].__file__
        hello = os.path.join(os.getcwd(),
                         os.path.dirname(os.path.dirname(myfile)),
                         'examples', 'hellogoodbye.py')
        args = [sys.executable, hello, base] if base else [sys.executable, hello]
        p = Popen(args, stdout=PIPE, stderr=PIPE)
        o, e = p.communicate()
        import platform
        exp_o = (b'Hello, world!\r\nGoodbye\r\n'
                 if platform.system() == "Windows" else
                 b'Hello, world!\nGoodbye\n')
        exp_e = b''
        if exp_o != o:
            self.info('Unexpected output', o)
        if exp_e != e:
            self.info('Unexpected errors', e)
        assert o == exp_o
        assert e == exp_e


class SockTest(threading.Thread):
    def __init__(self, proto, port, report, flag, *args, **kw):
        self.fam = proto
        self.port = port
        self.good = flag
        self.report = report
        super().__init__(*args, **kw)
    def run(self):
        af, proto, _ = socket_args(self.fam)
        self.test_socket(af, proto)
        self.good.set()

def socket_args(proto_str):
    import socket
    return { 'TCP': (socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP),
             'UDP': (socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP),
            }[proto_str]
    
class SockServer(SockTest):
    def test_socket(self, af, proto):
        import socket
        s = socket.socket(af, proto)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        print('Server binding socket to port %d' % self.port, file=self.report)
        s.bind(('', self.port))
        if proto == socket.SOCK_STREAM:
            print('Server waiting for connection', file=self.report)
            s.listen()
            (r,a) = s.accept()
            print('Server got connection from {}'.format(a), file=self.report)
            m = r.recv(1024)
        else:
            (m,a) = s.recvfrom(1024)
        print('Server got "%s" from %s' % (m, a), file=self.report)
        if proto == socket.SOCK_STREAM:
            r.send(b'Goodbye')
            print('Server shutting down: %s' % r, file=self.report)
            r.close()
            print('Server closed client socket %s' % s, file=self.report)
            s.shutdown(socket.SHUT_RDWR)
            print('Server shutdown listening socket %s' % s, file=self.report)
        else:
            s.sendto(b'Goodbye', a)
        print('Server closing listening socket %s' % s, file=self.report)
        s.close()
        print('Server closed listening socket %s' % s, file=self.report)
        del s
        print('server done', file=self.report)


class SockClient(SockTest):
    def test_socket(self, af, proto):
        import socket
        s = socket.socket(af, proto)
        if proto == socket.SOCK_STREAM:
            print('Client connecting to port %d' % self.port, file=self.report)
            s.connect(('127.0.0.1', self.port))
            print('Client sending message', file=self.report)
            s.send(b'Hello')
            r = s.recv(1024)
        else:
            s.sendto(b'Hello', ('127.0.0.1', self.port))
            (r,a) = s.recvfrom(1024)
        print('Client receive message: %s' % r, file=self.report)
        s.close()
        assert r == b'Goodbye'
        self.good.set()

class Skipped(Exception):
    def __init__(self, reason):
        self.why = reason

if __name__ == "__main__":
    d = Diagnoser(len(sys.argv) < 2 and sys.stdout.isatty())
    d.report('Initiating diagnostics')
    d.info('Python', sys.implementation)
    d.info('(t)', sys.thread_info)
    d.info('(p)', sys.platform)
    d.info('(mp)', sys.meta_path)
    package_ok = d.check_imports()
    d.check_thespian_internal_system_imports()
    d.check_existing_running_actors()
    hn = d.check_hostname()
    fqdn = d.check_fqdn()
    d.check_addr_info(None, 0, 'UDP', "default")
    d.check_addr_info(None, 'passive', 'UDP', "default")
    if hn:
        d.check_addr_info(hn, 0, 'UDP', "hostname")
        d.check_addr_info(hn, 'passive', 'UDP', "hostname")
    if fqdn:
        d.check_addr_info(fqdn, 0, 'UDP', "fqdn")
        d.check_addr_info(fqdn, 'passive', 'UDP', "fqdn")
    d.check_addr_info(None, 0, 'TCP', "default")
    d.check_addr_info(None, 'passive', 'TCP', "default")
    if hn:
        d.check_addr_info(hn, 0, 'TCP', "hostname")
        d.check_addr_info(hn, 'passive', 'TCP', "hostname")
    if fqdn:
        d.check_addr_info(fqdn, 0, 'TCP', "fqdn")
        d.check_addr_info(fqdn, 'passive', 'TCP', "fqdn")
    d.check_IP_addresses()
    udp_ok = d.check_multiprocUDPBase_port_available()
    tcp_ok = d.check_multiprocTCPBase_port_available()
    d.check_UDP_sockets(udp_ok)
    d.check_TCP_sockets(tcp_ok)
    d.check_simple_actors(True, 'simpleSystemBase')
    d.check_simple_actors(package_ok, 'multiprocQueueBase')
    d.check_simple_actors(package_ok and udp_ok, 'multiprocUDPBase')
    d.check_simple_actors(package_ok and tcp_ok, 'multiprocTCPBase')
    d.report('Diagnostics completed.')
