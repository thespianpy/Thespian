"Base definitions for transports based on IP networking."


import socket
from thespian.actors import ActorAddress


_localAddresses = ['', '127.0.0.1', 'localhost', None]


class ThisSystem(object):
    def __init__(self):
        # Use a quick UDP socket to get this system's INET addresses
        af       = socket.AF_INET
        socktype = socket.SOCK_DGRAM
        proto    = socket.IPPROTO_UDP
        self._myAddresses = [ rslt[4][0]
                              for usage in [0, socket.AI_PASSIVE]
                              for useAddr in [None, socket.gethostname(), socket.getfqdn()]
                              for rslt in socket.getaddrinfo(useAddr, 0, af, socktype, proto, usage)
                          ]


    def cmpIP2Tuple(self, af, socktype, proto, t1, t2):
        """Function to compare two IP 2-tuple addresses.  Direct equality is
           easiest, but there are several additional equalities for the
           first element: '', '0.0.0.0', '127.0.0.1', any localIP address.
           Also, a port of 0 or None should match any other port.
        """
        if t1 == t2: return True  # easiest
        localIDs = _localAddresses + self._myAddresses
        if t1[0] in localIDs and t2[0] in localIDs:
            # Got a match, compare ports
            return t1[1] == t2[1] or t1[1] in [None, 0] or t2[1] in [None, 0]
        return False


    @staticmethod
    def _localAddr(addr): return addr in _localAddresses


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
        self.af       = af
        self.socktype = socktype
        self.proto    = proto
        if baseaddr and external and baseaddr == '0.0.0.0': baseaddr = None
        if baseaddr == '': baseaddr = None if external else '127.0.0.1'
        if external and not baseaddr:
            # Trick to get the "public" IP address... doesn't work so
            # well if there are multiple routes, or if the public site
            # is not accessible.  (needs work)
            remoteAddr = (external
                          if type(external) == type( ('',0) )
                          else ( (external, 80)
                                 if type(external) == type("")
                                 else (external.bindname
                                       if isinstance(external, IPActorAddress)
                                       else (external.addressDetails.sockname
                                             if (isinstance(external, ActorAddress) and
                                                 isinstance(external.addressDetails, IPActorAddress))
                                             else ('8.8.8.8', 80) ))))
            if thisSystem._localAddr(remoteAddr[0]): remoteAddr = ('8.8.8.8', remoteAddr[1])
            try:
                # Use a UDP socket: no actual connection is made
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
                try:
                    s.connect( remoteAddr )
                    baseaddr = s.getsockname()[0]
                finally:
                    s.close()
            except Exception as ex:
                pass
            if not baseaddr or thisSystem._localAddr(baseaddr): # (baseaddr == '127.0.0.1' and not thisSystem._localAddr(remoteAddr[0])):
                raise RuntimeError('Unable to determine valid external socket address.')
        res = socket.getaddrinfo(baseaddr, port, af, socktype, proto,
                                 socket.AI_PASSIVE if baseaddr is None and not external else 0)
        af, socktype, proto, canonname, sa = res[0]
        self.sockname = sa
        self.bindname = ('',sa[1]) if external else sa
    def __eq__(self, o):
        return self.af == o.af and self.socktype == o.socktype and self.proto == o.proto and \
            thisSystem.cmpIP2Tuple(self.af, self.socktype, self.proto, self.sockname, o.sockname)
    def __ne__(self, o): return not self.__eq__(o)
    def __hash__(self): return hash(str(self))
    def __str__(self):
        if self.af == socket.AF_INET:
            if self.socktype == socket.SOCK_STREAM:
                if self.proto == socket.IPPROTO_TCP:
                    return '(TCP|%s:%d)'%self.sockname
            if self.socktype == socket.SOCK_DGRAM:
                if self.proto == socket.IPPROTO_UDP:
                    return '(UDP|%s:%d)'%self.sockname
        if self.af == socket.AF_INET6:
            if self.socktype == socket.SOCK_STREAM:
                if self.proto == socket.IPPROTO_TCP:
                    return '(TCP6|[%s]:%d %d %d)'%self.sockname
        return '(%s)'%str( ((self.af, self.socktype, self.proto), self.sockname) )
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
                                                initialIPAddr, initialIPPort, external)
    def __str__(self): return '(UDP|%s:%d)'%self.sockname


class TCPv4ActorAddress(IPActorAddress):
    def __init__(self, initialIPAddr=None, initialIPPort=0, external=False):
        super(TCPv4ActorAddress, self).__init__(socket.AF_INET,
                                                socket.SOCK_STREAM,
                                                socket.IPPROTO_TCP,
                                                initialIPAddr, initialIPPort, external)
    def __str__(self): return '(TCP|%s:%d)'%self.sockname


class TCPv6ActorAddress(IPActorAddress):
    def __init__(self, initialIPAddr=None, initialIPPort=0, external=False):
        super(TCPv6ActorAddress, self).__init__(socket.AF_INET6,
                                                socket.SOCK_STREAM,
                                                socket.IPPROTO_TCP,
                                                initialIPAddr, initialIPPort, external)
    def __str__(self): return '(TCP6|[%s]:%d %d %d)'%self.sockname
