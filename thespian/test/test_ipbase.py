import unittest
import socket
from thespian.system.transport.IPBase import *


cmpTCP = lambda a, b: thisSystem.cmpIP2Tuple(socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_TCP, a, b)


class IP2TupleTests(unittest.TestCase):
    scope='unit'

    def setUp(self):
        self.myAddrs = [ rslt[4][0]
                         for usage in [0, socket.AI_PASSIVE]
                         for useAddr in [None, socket.gethostname(), socket.getfqdn()]
                         for rslt in socket.getaddrinfo(useAddr, 0, socket.AF_INET,
                                                        socket.SOCK_STREAM,
                                                        socket.IPPROTO_TCP, usage)
                     ]
        print(self.myAddrs)

    def test_eq01(self): self.assertTrue(cmpTCP( ('127.0.0.1', 32),   ('127.0.0.1', 32) ))
    def test_eq02(self): self.assertTrue(cmpTCP( ('127.0.0.1', 0),    ('127.0.0.1', 32) ))
    def test_eq03(self): self.assertTrue(cmpTCP( ('127.0.0.1', None), ('127.0.0.1', 32) ))
    def test_eq04(self): self.assertTrue(cmpTCP( ('127.0.0.1', 32),   ('127.0.0.1', 0) ))
    def test_eq05(self): self.assertTrue(cmpTCP( ('127.0.0.1', 32),   ('127.0.0.1', None) ))
    def test_eq06(self): self.assertTrue(cmpTCP( (None,        32),   ('127.0.0.1', 32) ))
    def test_eq07(self): self.assertTrue(cmpTCP( ('',          32),   ('127.0.0.1', 32) ))
    def test_eq08(self): self.assertTrue(cmpTCP( ('0.0.0.0',   32),   ('127.0.0.1', 32) ))
    def test_eq09(self): self.assertTrue(cmpTCP( ('127.0.0.1', 32),   (None,        32) ))
    def test_eq10(self): self.assertTrue(cmpTCP( ('127.0.0.1', 32),   ('',          32) ))
    def test_eq11(self): self.assertTrue(cmpTCP( ('127.0.0.1', 32),   ('0.0.0.0',   32) ))
    def test_ne12(self): self.assertFalse(cmpTCP( ('127.0.0.1', 32),   ('0.0.0.0',   23) ))
    # Following assume the local host never has an address of 1.0.0.0
    def test_ne13(self): self.assertFalse(cmpTCP( ('127.0.0.1', 32),   ('1.0.0.0',   32) ))
    def test_ne14(self): self.assertFalse(cmpTCP( ('0.0.0.0', 32),   ('1.0.0.0',   32) ))
    def test_ne15(self): self.assertFalse(cmpTCP( ('', 32),   ('1.0.0.0',   32) ))
    def test_ne16(self): self.assertFalse(cmpTCP( (None, 32),   ('1.0.0.0',   32) ))
    def test_eq17(self): self.assertTrue(cmpTCP( ('127.0.0.1', 1900),   ('0.0.0.0',   1900) ))

    def test_eq21(self):
        for each in self.myAddrs: self.assertTrue(cmpTCP( ('127.0.0.1', 32),   (each, 32) ))
    def test_eq22(self):
        for each in self.myAddrs: self.assertTrue(cmpTCP( ('127.0.0.1', 0),    (each, 32) ))
    def test_eq23(self):
        for each in self.myAddrs: self.assertTrue(cmpTCP( ('127.0.0.1', None), (each, 32) ))
    def test_eq24(self):
        for each in self.myAddrs: self.assertTrue(cmpTCP( (each, 32),   ('127.0.0.1', 0) ))
    def test_eq25(self):
        for each in self.myAddrs: self.assertTrue(cmpTCP( (each, 32),   ('127.0.0.1', None) ))
    def test_eq26(self):
        for each in self.myAddrs: self.assertTrue(cmpTCP( (None,        32),   (each, 32) ))
    def test_eq27(self):
        for each in self.myAddrs: self.assertTrue(cmpTCP( ('',          32),   (each, 32) ))
    def test_eq28(self):
        for each in self.myAddrs: self.assertTrue(cmpTCP( ('0.0.0.0',   32),   (each, 32) ))
    def test_eq29(self):
        for each in self.myAddrs: self.assertTrue(cmpTCP( (each, 32),   (None,        32) ))
    def test_eq30(self):
        for each in self.myAddrs: self.assertTrue(cmpTCP( (each, 32),   ('',          32) ))
    def test_eq31(self):
        for each in self.myAddrs: self.assertTrue(cmpTCP( (each, 32),   ('0.0.0.0',   32) ))
    def test_ne32(self):
        for each in self.myAddrs: self.assertFalse(cmpTCP( (each, 32),   ('0.0.0.0',   23) ))
    # Following assume the local host never has an address of 1.0.0.0
    def test_ne33(self):
        for each in self.myAddrs: self.assertFalse(cmpTCP( (each, 32),   ('1.0.0.0',   32) ))
