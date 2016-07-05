from pytest import raises
from thespian.actors import *
from thespian.system.transport.TCPTransport import *


class TestUnitTCPAddresses(object):

    def testRegularEquality(self):
        # Only remote IP and port address are used for equality
        a1 = ActorAddress(TCPv4ActorAddress('1.2.3.4', 1234, external=False))
        assert a1 != ActorAddress(TCPv4ActorAddress('1.2.3.5', 1234,
                                                    external=False))
        assert a1 != ActorAddress(TCPv4ActorAddress('1.2.3.4', 1235,
                                                    external=False))
        assert a1 == ActorAddress(TCPv4ActorAddress('1.2.3.4', 1234,
                                                    external=True))
        assert a1 == ActorAddress(TCPv4ActorAddress('1.2.3.4', 0,
                                                    external=True))
        assert a1 != ActorAddress(TCPv4ActorAddress('', 1234, external=False))
        assert a1 == ActorAddress(TCPv4ActorAddress('1.2.3.4', 1234,
                                                    external=False))
        assert a1 == ActorAddress(TCPv4ActorAddress('1.2.3.4', 0,
                                                    external=False))

    def testRegularHashing(self):
        a1 = ActorAddress(TCPv4ActorAddress('1.2.3.4', 1234, external=False))
        a2 = ActorAddress(TCPv4ActorAddress('10.20.30.40', 1234, external=False))
        a3 = ActorAddress(TCPv4ActorAddress('31.32.33.34', 1234, external=False))
        a4 = ActorAddress(TCPv4ActorAddress('41.42.43.44', 1234, external=False))

        raises(TypeError, hash, a1)
        raises(TypeError, hash, a2)
        raises(TypeError, hash, a3)
        raises(TypeError, hash, a4)


    def testRoutedEquality(self):
        admin = '42'
        # Only remote IP and port address are used for equality
        a1 = ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1234, admin,
                                                  txOnly=False,
                                                  external=False))
        assert a1 != ActorAddress(RoutedTCPv4ActorAddress('1.2.3.5', 1234,
                                                          admin,
                                                          txOnly=False,
                                                          external=False))
        assert a1 != ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1235,
                                                          admin,
                                                          txOnly=False,
                                                          external=False))
        assert a1 == ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1234,
                                                          admin+admin,
                                                          txOnly=False,
                                                          external=False))
        assert a1 == ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1234,
                                                          admin,
                                                          txOnly=False,
                                                          external=True))
        assert a1 == ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1234,
                                                          admin,
                                                          txOnly=True,
                                                          external=False))
        assert a1 == ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1234,
                                                          admin,
                                                          txOnly=False,
                                                          external=False))
        assert a1 == ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 0, admin,
                                                          txOnly=False,
                                                          external=False))

    def testRoutedHashing(self):
        admin1 = ActorAddress(TCPv4ActorAddress('1.0.0.100', 1234,
                                                external=False))
        admin2 = ActorAddress(TCPv4ActorAddress('99.99.99.99', 1234,
                                                external=False))
        a1 = ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1234, admin1,
                                                  txOnly=False, external=False))
        a2 = ActorAddress(RoutedTCPv4ActorAddress('10.20.30.40', 1234, admin1,
                                                  txOnly=True, external=False))
        a3 = ActorAddress(RoutedTCPv4ActorAddress('31.32.33.34', 1234, admin2,
                                                  txOnly=False, external=False))
        a4 = ActorAddress(RoutedTCPv4ActorAddress('41.42.43.44', 1234, admin1,
                                                  txOnly=False, external=False))

        raises(TypeError, hash, a1)
        raises(TypeError, hash, a2)
        raises(TypeError, hash, a3)
        raises(TypeError, hash, a4)
        raises(TypeError, hash, admin1)
        raises(TypeError, hash, admin2)


    def testTXOnlyAdminEquality(self):
        a1 = ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1234,
                                                       external=False))
        assert a1 != ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.5', 1234,
                                                               external=False))
        assert a1 != ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1235,
                                                               external=False))
        assert a1 == ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1234,
                                                               external=True))
        assert a1 == ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1234,
                                                               external=False))
        assert a1 == ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 0,
                                                               external=False))


    def testTXOnlyAdminHashing(self):
        a1 = ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1234,
                                                       external=False))
        a2 = ActorAddress(TXOnlyAdminTCPv4ActorAddress('10.20.30.40', 1234,
                                                       external=False))
        a3 = ActorAddress(TXOnlyAdminTCPv4ActorAddress('31.32.33.34', 1234,
                                                       external=False))
        a4 = ActorAddress(TXOnlyAdminTCPv4ActorAddress('41.42.43.44', 1234,
                                                       external=False))

        raises(TypeError, hash, a1)
        raises(TypeError, hash, a2)
        raises(TypeError, hash, a3)
        raises(TypeError, hash, a4)


    def testMixedEquality(self):
        admin = '42'
        # Only remote IP and port address are used for equality
        a1 = ActorAddress(TCPv4ActorAddress('1.2.3.4', 1234, external=False))
        a2 = ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1234, admin,
                                                  txOnly=False, external=False))
        a3 = ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1234,
                                                       external=False))
        assert a1 == a2
        assert a2 == a1

        assert a1 == a3
        assert a3 == a1

        assert a2 == a3
        assert a3 == a2

        assert a1 != ActorAddress(TCPv4ActorAddress('1.2.3.5', 1234,
                                                    external=False))
        assert a1 != ActorAddress(RoutedTCPv4ActorAddress('1.2.3.5', 1234,
                                                          admin,
                                                          txOnly=False,
                                                          external=False))
        assert a1 != ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.5', 1234,
                                                               external=False))

        assert a2 != ActorAddress(TCPv4ActorAddress('1.2.3.5', 1234,
                                                    external=False))
        assert a2 != ActorAddress(RoutedTCPv4ActorAddress('1.2.3.5', 1234,
                                                          admin,
                                                          txOnly=False,
                                                          external=False))
        assert a2 != ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.5', 1234,
                                                               external=False))

        assert a3 != ActorAddress(TCPv4ActorAddress('1.2.3.5', 1234,
                                                    external=False))
        assert a3 != ActorAddress(RoutedTCPv4ActorAddress('1.2.3.5', 1234,
                                                          admin,
                                                          txOnly=False,
                                                          external=False))
        assert a3 != ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.5', 1234,
                                                               external=False))

        assert a1 != ActorAddress(TCPv4ActorAddress('1.2.3.4', 1235,
                                                    external=False))
        assert a1 != ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1235,
                                                          admin,
                                                          txOnly=False,
                                                          external=False))
        assert a1 != ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1235,
                                                               external=False))

        assert a2 != ActorAddress(TCPv4ActorAddress('1.2.3.4', 1235,
                                                    external=False))
        assert a2 != ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1235,
                                                          admin,
                                                          txOnly=False,
                                                          external=False))
        assert a2 != ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1235,
                                                               external=False))

        assert a3 != ActorAddress(TCPv4ActorAddress('1.2.3.4', 1235,
                                                    external=False))
        assert a3 != ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1235,
                                                          admin,
                                                          txOnly=False,
                                                          external=False))
        assert a3 != ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1235,
                                                               external=False))


    def testMixedHashing(self):
        admin = '42'
        # Only remote IP and port address are used for equality
        a1 = ActorAddress(TCPv4ActorAddress('1.2.3.4', 1234, external=False))
        a2 = ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1234, admin,
                                                  txOnly=False, external=False))
        a3 = ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1234, admin,
                                                  txOnly=True, external=False))
        a4 = ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1234,
                                                       external=False))
        a5 = ActorAddress(RoutedTCPv4ActorAddress('11.22.33.44', 234, admin,
                                                  txOnly=True, external=False))

        raises(TypeError, hash, a1)
        raises(TypeError, hash, a2)
        raises(TypeError, hash, a3)
        raises(TypeError, hash, a4)
        raises(TypeError, hash, a5)
