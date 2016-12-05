import socket
import pytest
from thespian.system.transport.IPBase import *


cmpTCP = lambda a, b: thisSystem.cmpIP2Tuple(a, b)

@pytest.fixture(params=[ rslt[4][0]
                         for usage in [0, socket.AI_PASSIVE]
                         for useAddr in [None, socket.gethostname(), socket.getfqdn()]
                         for rslt in socket.getaddrinfo(useAddr, 0, socket.AF_INET,
                                                        socket.SOCK_STREAM,
                                                        socket.IPPROTO_TCP, usage)
])
def myAddrs(request):
    print(request.param)
    return request.param


class TestUnitIP2Tuple(object):
    def test_eq01(self): assert cmpTCP( ('127.0.0.1', 32),   ('127.0.0.1', 32) )
    def test_eq02(self): assert cmpTCP( ('127.0.0.1', 0),    ('127.0.0.1', 32) )
    def test_eq03(self): assert cmpTCP( ('127.0.0.1', None), ('127.0.0.1', 32) )
    def test_eq04(self): assert cmpTCP( ('127.0.0.1', 32),   ('127.0.0.1', 0) )
    def test_eq05(self): assert cmpTCP( ('127.0.0.1', 32),   ('127.0.0.1', None) )
    def test_eq06(self): assert cmpTCP( (None,        32),   ('127.0.0.1', 32) )
    def test_eq07(self): assert cmpTCP( ('',          32),   ('127.0.0.1', 32) )
    def test_eq08(self): assert cmpTCP( ('0.0.0.0',   32),   ('127.0.0.1', 32) )
    def test_eq09(self): assert cmpTCP( ('127.0.0.1', 32),   (None,        32) )
    def test_eq10(self): assert cmpTCP( ('127.0.0.1', 32),   ('',          32) )
    def test_eq11(self): assert cmpTCP( ('127.0.0.1', 32),   ('0.0.0.0',   32) )
    def test_ne12(self): assert not cmpTCP( ('127.0.0.1', 32),   ('0.0.0.0',   23) )
    # Following assume the local host never has an address of 1.0.0.0
    def test_ne13(self): assert not cmpTCP( ('127.0.0.1', 32),   ('1.0.0.0',   32) )
    def test_ne14(self): assert not cmpTCP( ('0.0.0.0', 32),   ('1.0.0.0',   32) )
    def test_ne15(self): assert not cmpTCP( ('', 32),   ('1.0.0.0',   32) )
    def test_ne16(self): assert not cmpTCP( (None, 32),   ('1.0.0.0',   32) )
    def test_eq17(self): assert cmpTCP( ('127.0.0.1', 1900),   ('0.0.0.0',   1900) )

    def test_eq21(self, myAddrs): assert cmpTCP( ('127.0.0.1', 32),   (myAddrs, 32) )
    def test_eq22(self, myAddrs): assert cmpTCP( ('127.0.0.1', 0),    (myAddrs, 32) )
    def test_eq23(self, myAddrs): assert cmpTCP( ('127.0.0.1', None), (myAddrs, 32) )
    def test_eq24(self, myAddrs): assert cmpTCP( (myAddrs, 32),   ('127.0.0.1', 0) )
    def test_eq25(self, myAddrs): assert cmpTCP( (myAddrs, 32),   ('127.0.0.1', None) )
    def test_eq26(self, myAddrs): assert cmpTCP( (None,        32),   (myAddrs, 32) )
    def test_eq27(self, myAddrs): assert cmpTCP( ('',          32),   (myAddrs, 32) )
    def test_eq28(self, myAddrs): assert cmpTCP( ('0.0.0.0',   32),   (myAddrs, 32) )
    def test_eq29(self, myAddrs): assert cmpTCP( (myAddrs, 32),   (None,        32) )
    def test_eq30(self, myAddrs): assert cmpTCP( (myAddrs, 32),   ('',          32) )
    def test_eq31(self, myAddrs): assert cmpTCP( (myAddrs, 32),   ('0.0.0.0',   32) )
    def test_ne32(self, myAddrs): assert not cmpTCP( (myAddrs, 32),   ('0.0.0.0',   23) )
    # Following assume the local host never has an address of 1.0.0.0
    def test_ne33(self, myAddrs): assert not cmpTCP( (myAddrs, 32),   ('1.0.0.0',   32) )
