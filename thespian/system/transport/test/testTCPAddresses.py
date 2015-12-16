import unittest
from thespian.actors import *
from thespian.system.transport.TCPTransport import *


class TestTCPAddresses(unittest.TestCase):
    scope='unit'

    def testRegularEquality(self):
        # Only remote IP and port address are used for equality
        a1 = ActorAddress(TCPv4ActorAddress('1.2.3.4', 1234, external=False))
        self.assertNotEqual(a1, ActorAddress(TCPv4ActorAddress('1.2.3.5', 1234, external=False)))
        self.assertNotEqual(a1, ActorAddress(TCPv4ActorAddress('1.2.3.4', 1235, external=False)))
        self.assertEqual(a1, ActorAddress(TCPv4ActorAddress('1.2.3.4', 1234, external=True)))
        self.assertEqual(a1, ActorAddress(TCPv4ActorAddress('1.2.3.4', 0, external=True)))
        self.assertNotEqual(a1, ActorAddress(TCPv4ActorAddress('', 1234, external=False)))
        self.assertEqual(a1, ActorAddress(TCPv4ActorAddress('1.2.3.4', 1234, external=False)))
        self.assertEqual(a1, ActorAddress(TCPv4ActorAddress('1.2.3.4', 0, external=False)))

    def testRoutedEquality(self):
        admin = '42'
        # Only remote IP and port address are used for equality
        a1 = ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1234, admin, txOnly=False, external=False))
        self.assertNotEqual(a1, ActorAddress(RoutedTCPv4ActorAddress('1.2.3.5', 1234, admin, txOnly=False, external=False)))
        self.assertNotEqual(a1, ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1235, admin, txOnly=False, external=False)))
        self.assertEqual(a1, ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1234, admin+admin, txOnly=False, external=False)))
        self.assertEqual(a1, ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1234, admin, txOnly=False, external=True)))
        self.assertEqual(a1, ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1234, admin, txOnly=True, external=False)))
        self.assertEqual(a1, ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1234, admin, txOnly=False, external=False)))
        self.assertEqual(a1, ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 0, admin, txOnly=False, external=False)))

    def testTXOnlyEquality(self):
        a1 = ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1234, external=False))
        self.assertNotEqual(a1, ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.5', 1234, external=False)))
        self.assertNotEqual(a1, ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1235, external=False)))
        self.assertEqual(a1, ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1234, external=True)))
        self.assertEqual(a1, ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1234, external=False)))
        self.assertEqual(a1, ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 0, external=False)))


    def testMixedEquality(self):
        admin = '42'
        # Only remote IP and port address are used for equality
        a1 = ActorAddress(TCPv4ActorAddress('1.2.3.4', 1234, external=False))
        a2 = ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1234, admin, txOnly=False, external=False))
        a3 = ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1234, external=False))
        self.assertEqual(a1, a2)
        self.assertEqual(a2, a1)

        self.assertEqual(a1, a3)
        self.assertEqual(a3, a1)

        self.assertEqual(a2, a3)
        self.assertEqual(a3, a2)

        self.assertNotEqual(a1, ActorAddress(TCPv4ActorAddress('1.2.3.5', 1234, external=False)))
        self.assertNotEqual(a1, ActorAddress(RoutedTCPv4ActorAddress('1.2.3.5', 1234, admin, txOnly=False, external=False)))
        self.assertNotEqual(a1, ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.5', 1234, external=False)))

        self.assertNotEqual(a2, ActorAddress(TCPv4ActorAddress('1.2.3.5', 1234, external=False)))
        self.assertNotEqual(a2, ActorAddress(RoutedTCPv4ActorAddress('1.2.3.5', 1234, admin, txOnly=False, external=False)))
        self.assertNotEqual(a2, ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.5', 1234, external=False)))

        self.assertNotEqual(a3, ActorAddress(TCPv4ActorAddress('1.2.3.5', 1234, external=False)))
        self.assertNotEqual(a3, ActorAddress(RoutedTCPv4ActorAddress('1.2.3.5', 1234, admin, txOnly=False, external=False)))
        self.assertNotEqual(a3, ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.5', 1234, external=False)))

        self.assertNotEqual(a1, ActorAddress(TCPv4ActorAddress('1.2.3.4', 1235, external=False)))
        self.assertNotEqual(a1, ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1235, admin, txOnly=False, external=False)))
        self.assertNotEqual(a1, ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1235, external=False)))

        self.assertNotEqual(a2, ActorAddress(TCPv4ActorAddress('1.2.3.4', 1235, external=False)))
        self.assertNotEqual(a2, ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1235, admin, txOnly=False, external=False)))
        self.assertNotEqual(a2, ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1235, external=False)))

        self.assertNotEqual(a3, ActorAddress(TCPv4ActorAddress('1.2.3.4', 1235, external=False)))
        self.assertNotEqual(a3, ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1235, admin, txOnly=False, external=False)))
        self.assertNotEqual(a3, ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1235, external=False)))
