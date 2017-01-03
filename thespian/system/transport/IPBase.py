"Base definitions for transports based on IP networking."


import socket
import logging
from thespian.actors import ActorAddress


_localAddresses = set(['', '127.0.0.1', 'localhost', None])


def _probeAddrInfo(usage, useAddr, af, socktype, proto):
    try:
        return socket.getaddrinfo(useAddr, 0, af, socktype, proto, usage)
    except Exception as ex:
        logging.warning('Unable to get address info'
                        ' for address %s (%s, %s, %s, %s): %s %s',
                        useAddr, af, socktype, proto, usage, type(ex), ex)
        return [None]


def getLocalAddresses():
    # Use a quick UDP socket to get this system's INET addresses
    af = socket.AF_INET
    socktype = socket.SOCK_DGRAM
    proto = socket.IPPROTO_UDP
    try:
        hostname = socket.gethostname()
    except Exception:
        logging.warning('Unable to determine hostname')
        hostname = None
    try:
        fqdn = socket.getfqdn()
    except Exception:
        logging.warning('Unable to determine fqdn')
        fqdn = None
    return set(rslt[4][0]
               for usage in [0, socket.AI_PASSIVE]
               for useAddr in [None, hostname, fqdn]
               for rslt in _probeAddrInfo(usage, useAddr, af, socktype, proto)
               if rslt).union(_localAddresses)


class ThisSystem(object):
    def __init__(self):
        self._myAddresses = getLocalAddresses()

    def cmpIP2Tuple(self, t1, t2):
        """Function to compare two IP 2-tuple addresses.  Direct equality is
           easiest, but there are several additional equalities for the
           first element: '', '0.0.0.0', '127.0.0.1', any localIP address.
           Also, a port of 0 or None should match any other port.
        """
        if t1 == t2:
            return True  # easiest
        # Start by comparing ports, and if they are a match, check all
        # possible addresses.
        return (t1[1] == t2[1] or
                t1[1] in [None, 0] or
                t2[1] in [None, 0]) and \
            self.isSameSystem(t1, t2)

    def isSameSystem(self, t1, t2):
        """Function to compare two IP 2-tuple addresses ignoring ports to see
           if they exist on the same system.  Direct equality is
           easiest, but there are several additional equalities for
           the the local system: '', '0.0.0.0', '127.0.0.1', any localIP
           address.
        """
        if t1[0] == t2[0]:
            return True
        # The local system has several alternative references: if both
        # addresses refer to the local system with one of the
        # references then they are equal.
        localIDs = self._myAddresses
        return t1[0] in localIDs and t2[0] in localIDs

    def add_local_addr(self, newaddr):
        if newaddr not in self._myAddresses:
            self._myAddresses.add(newaddr)

    def isLocalAddr(self, addr):
        return addr in self._myAddresses

    @staticmethod
    def _isLocalReference(addr): return addr in _localAddresses


thisSystem = ThisSystem()


class IPActorAddress(object):
    def __init__(self, af, socktype, proto, baseaddr, port, external=False):
        """If external is "truthy", this should be an address that is reachable
           from external nodes.  If external is false, this is usually
           an address that is going to be listened on locally.  For
           example, "0.0.0.0" can be used for a non-external
           listen-any address, but cannot be used for sending messages.

           A "truthy" value of external can be an external address to
           try.  Using the address of the Convention Leader (if any)
           is recommended to ensure that the address chosen is
           appropriate for the network supporting the Convention.  By
           default, the address is Go Daddy's public webserver
           address.
        """
        self.af = af
        self.socktype = socktype
        self.proto = proto
        if baseaddr and external and baseaddr == '0.0.0.0':
            baseaddr = None
        if baseaddr == '':
            baseaddr = None if external else '127.0.0.1'
        if external and not baseaddr:
            # Trick to get the "public" IP address... doesn't work so
            # well if there are multiple routes, or if the public site
            # is not accessible.  (needs work)
            remoteAddr = (
                external
                if isinstance(external, tuple)
                else ((external, 80)
                      if isinstance(external, str)
                      else (external.bindname
                            if isinstance(external, IPActorAddress)
                            else (external.addressDetails.sockname
                                  if (isinstance(external, ActorAddress) and
                                      isinstance(external.addressDetails,
                                                 IPActorAddress))
                                  else ('8.8.8.8', 80)))))
            if thisSystem._isLocalReference(remoteAddr[0]):
                remoteAddr = ('8.8.8.8', remoteAddr[1])
            try:
                # Use a UDP socket: no actual connection is made
                s = socket.socket(socket.AF_INET,
                                  socket.SOCK_DGRAM,
                                  socket.IPPROTO_UDP)
                try:
                    s.connect(remoteAddr)
                    baseaddr = s.getsockname()[0]
                finally:
                    s.close()
            except TypeError:
                # Probably specified the Admin Port as a string...
                print('Error connecting to %s' % (str(remoteAddr)))
                import traceback
                traceback.print_exc()
            except Exception:
                pass
            if not baseaddr or thisSystem._isLocalReference(baseaddr):
                raise RuntimeError('Unable to determine valid external socket address.')
            thisSystem.add_local_addr(baseaddr)
        res = socket.getaddrinfo(baseaddr, port, af, socktype, proto,
                                 socket.AI_PASSIVE
                                 if baseaddr is None and not external else 0)
        af, socktype, proto, canonname, sa = res[0]
        self.sockname = sa
        self.bindname = ('', sa[1]) if external else sa

    def __eq__(self, o):
        return self.af == o.af and self.socktype == o.socktype and \
            self.proto == o.proto and \
            thisSystem.cmpIP2Tuple(self.sockname, o.sockname)

    def __ne__(self, o):
        return not self.__eq__(o)

    def __hash__(self):
        return hash((self.socketArgs, self.connectArgs))

    def isLocalAddr(self):
        return thisSystem.isLocalAddr(self.sockname[0])

    def __str__(self):
        if not hasattr(self, '_str_form'):
            self._str_form = ''.join(['(',
                                      self._str_kind(),
                                      '|',
                                      self._str_aps(),
                                      ')'])
        return self._str_form

    def __getstate__(self):
        # Removed the cached str() form when pickling so that it is
        # regenerated on the (possibly) remote end.
        if not hasattr(self, '_str_form'):
            return self.__dict__
        odict = self.__dict__.copy()
        del odict['_str_form']
        return odict

    def _str_kind(self):
        # n.b. ignores self.socktype of SOCK_STREAM or SOCK_DGRAM
        if self.proto == socket.IPPROTO_TCP:
            return ('TCP' if self.af == socket.AF_INET
                    else ('TCP6' if self.af == socket.AF_INET6
                          else 'TCP?'))
        return 'UDP' if self.proto == socket.IPPROTO_TCP \
            else '%s.%s.%s' % (self.af, self.socktype, self.proto)

    def _str_addr(self):
        return '' if self.isLocalAddr() else self.sockname[0]

    def _str_port(self):
        return ':%d' % self.sockname[1]

    def _str_suffix(self):
        return ''

    def _str_aps(self):
        return ''.join([self._str_addr(),
                        self._str_port(),
                        self._str_suffix()])

    @property
    def socketArgs(self): return (self.af, self.socktype, self.proto)

    @property
    def bindArgs(self): return self.bindname,

    @property
    def connectArgs(self): return self.sockname,


class UDPv4ActorAddress(IPActorAddress):
    def __init__(self, initialIPAddr=None, initialIPPort=0, external=False):
        super(UDPv4ActorAddress, self).__init__(socket.AF_INET,
                                                socket.SOCK_DGRAM,
                                                socket.IPPROTO_UDP,
                                                initialIPAddr,
                                                initialIPPort,
                                                external)

    def _str_kind(self):
        return 'UDP'


class TCPv4ActorAddress(IPActorAddress):
    def __init__(self, initialIPAddr=None, initialIPPort=0, external=False):
        super(TCPv4ActorAddress, self).__init__(socket.AF_INET,
                                                socket.SOCK_STREAM,
                                                socket.IPPROTO_TCP,
                                                initialIPAddr,
                                                initialIPPort,
                                                external)

    def _str_kind(self):
        return 'T'


class TCPv6ActorAddress(IPActorAddress):
    def __init__(self, initialIPAddr=None, initialIPPort=0, external=False):
        super(TCPv6ActorAddress, self).__init__(socket.AF_INET6,
                                                socket.SOCK_STREAM,
                                                socket.IPPROTO_TCP,
                                                initialIPAddr,
                                                initialIPPort,
                                                external)

    def _str_kind(self):
        return 'TCP6'

    def _str_addr(self):
        return '[%s]' % self.sockname[0]

    def _str_port(self):
        return ':%d.%d.%d' % self.sockname[1:]


class RoutedTCPv4ActorAddress(TCPv4ActorAddress):
    def __init__(self, anIPAddr, anIPPort, adminAddr, txOnly, external=False):
        super(RoutedTCPv4ActorAddress, self).__init__(anIPAddr, anIPPort,
                                                      external=external)
        self.routing = [None, adminAddr] if txOnly else [adminAddr]

    def _str_suffix(self):
        return '~' + '~'.join(['A' if A is None else
                               (A.addressDetails._str_aps()
                                if hasattr(A.addressDetails, '_str_aps')
                                else str(A))
                               for A in self.routing])


class TXOnlyAdminTCPv4ActorAddress(TCPv4ActorAddress):
    # Only assigned to the Admin; allows remote admins to know to wait
    # for a connection instead of trying to initiate one.
    def __init__(self, anIPAddr, anIPPort, external):
        super(TXOnlyAdminTCPv4ActorAddress, self).__init__(anIPAddr, anIPPort,
                                                           external=external)
        self.routing = [None]  # remotes must communicate via their local admin

    def _str_suffix(self):
        return '>'
