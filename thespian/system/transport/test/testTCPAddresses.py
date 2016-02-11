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

    def testRegularHashing(self):
        a1 = ActorAddress(TCPv4ActorAddress('1.2.3.4', 1234, external=False))
        a2 = ActorAddress(TCPv4ActorAddress('10.20.30.40', 1234, external=False))
        a3 = ActorAddress(TCPv4ActorAddress('31.32.33.34', 1234, external=False))
        a4 = ActorAddress(TCPv4ActorAddress('41.42.43.44', 1234, external=False))

        adict = { a1: 'a1', a2: 'a2', a4:'a4'}

        self.assertIn(a1, adict)
        self.assertIn(a2, adict)
        self.assertNotIn(a3, adict)
        self.assertIn(a4, adict)


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

    def testRoutedHashing(self):
        admin1 = ActorAddress(TCPv4ActorAddress('1.0.0.100', 1234, external=False))
        admin2 = ActorAddress(TCPv4ActorAddress('99.99.99.99', 1234, external=False))
        a1 = ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1234, admin1, txOnly=False, external=False))
        a2 = ActorAddress(RoutedTCPv4ActorAddress('10.20.30.40', 1234, admin1, txOnly=True, external=False))
        a3 = ActorAddress(RoutedTCPv4ActorAddress('31.32.33.34', 1234, admin2, txOnly=False, external=False))
        a4 = ActorAddress(RoutedTCPv4ActorAddress('41.42.43.44', 1234, admin1, txOnly=False, external=False))

        adict = { a1: 'a1', a2: 'a2', a4:'a4'}

        self.assertIn(a1, adict)
        self.assertIn(a2, adict)
        self.assertNotIn(a3, adict)
        self.assertIn(a4, adict)


    def testTXOnlyAdminEquality(self):
        a1 = ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1234, external=False))
        self.assertNotEqual(a1, ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.5', 1234, external=False)))
        self.assertNotEqual(a1, ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1235, external=False)))
        self.assertEqual(a1, ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1234, external=True)))
        self.assertEqual(a1, ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1234, external=False)))
        self.assertEqual(a1, ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 0, external=False)))


    def testTXOnlyAdminHashing(self):
        a1 = ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1234, external=False))
        a2 = ActorAddress(TXOnlyAdminTCPv4ActorAddress('10.20.30.40', 1234, external=False))
        a3 = ActorAddress(TXOnlyAdminTCPv4ActorAddress('31.32.33.34', 1234, external=False))
        a4 = ActorAddress(TXOnlyAdminTCPv4ActorAddress('41.42.43.44', 1234, external=False))

        adict = { a1: 'a1', a2: 'a2', a4:'a4'}

        self.assertIn(a1, adict)
        self.assertIn(a2, adict)
        self.assertNotIn(a3, adict)
        self.assertIn(a4, adict)


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


    def testMixedHashing(self):
        admin = '42'
        # Only remote IP and port address are used for equality
        a1 = ActorAddress(TCPv4ActorAddress('1.2.3.4', 1234, external=False))
        a2 = ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1234, admin, txOnly=False, external=False))
        a3 = ActorAddress(RoutedTCPv4ActorAddress('1.2.3.4', 1234, admin, txOnly=True, external=False))
        a4 = ActorAddress(TXOnlyAdminTCPv4ActorAddress('1.2.3.4', 1234, external=False))
        a5 = ActorAddress(RoutedTCPv4ActorAddress('11.22.33.44', 234, admin, txOnly=True, external=False))

        l1 = [a1, a2]
        l2 = [a1, a3]
        l3 = [a2, a3]
        l4 = [a1, a2, a3]
        l5 = [a1, a2, a3, a4]

        self.assertEqual(a1, a2)
        self.assertEqual(a1, a3)
        self.assertEqual(a1, a4)
        self.assertNotEqual(a1, a5)

        self.assertEqual(a1 in l1, a1 in dict(zip(l1,[True]*100)))
        self.assertEqual(a1 in l2, a1 in dict(zip(l2,[True]*100)))
        print('Check for %s::%s in %s:%s::%s and %s'%(str(a1), hash(a1), str(l3), str(list(map(str,l3))), str(list(map(hash,l3))), str(dict(zip(l3,[True]*100)))))
        self.assertEqual(a1 in l3, a1 in dict(zip(l3,[True]*100)))
        self.assertEqual(a1 in l4, a1 in dict(zip(l4,[True]*100)))
        self.assertEqual(a1 in l5, a1 in dict(zip(l5,[True]*100)))

        self.assertEqual(a2 in l1, a2 in dict(zip(l1,[True]*100)))
        self.assertEqual(a2 in l2, a2 in dict(zip(l2,[True]*100)))
        self.assertEqual(a2 in l3, a2 in dict(zip(l3,[True]*100)))
        self.assertEqual(a2 in l4, a2 in dict(zip(l4,[True]*100)))
        self.assertEqual(a2 in l5, a2 in dict(zip(l5,[True]*100)))

        self.assertEqual(a3 in l1, a3 in dict(zip(l1,[True]*100)))
        self.assertEqual(a3 in l2, a3 in dict(zip(l2,[True]*100)))
        self.assertEqual(a3 in l3, a3 in dict(zip(l3,[True]*100)))
        self.assertEqual(a3 in l4, a3 in dict(zip(l4,[True]*100)))
        self.assertEqual(a3 in l5, a3 in dict(zip(l5,[True]*100)))

        self.assertEqual(a4 in l1, a4 in dict(zip(l1,[True]*100)))
        self.assertEqual(a4 in l2, a4 in dict(zip(l2,[True]*100)))
        self.assertEqual(a4 in l3, a4 in dict(zip(l3,[True]*100)))
        self.assertEqual(a4 in l4, a4 in dict(zip(l4,[True]*100)))
        self.assertEqual(a4 in l5, a4 in dict(zip(l5,[True]*100)))

        self.assertEqual(a5 in l1, a5 in dict(zip(l1,[True]*100)))
        self.assertEqual(a5 in l2, a5 in dict(zip(l2,[True]*100)))
        self.assertEqual(a5 in l3, a5 in dict(zip(l3,[True]*100)))
        self.assertEqual(a5 in l4, a5 in dict(zip(l4,[True]*100)))
        self.assertEqual(a5 in l5, a5 in dict(zip(l5,[True]*100)))

        for alist in l1, l2, l3, l4, l5:
            for addr in a1, a2, a3, a4:
                self.assertEqual(addr in alist, addr in dict(zip(alist,[True]*100)))


        d1 = {a1: 'a1', a2: 'a2'}
        d2 = {a1: 'a1', a3: 'a3'}
        d3 = {a2: 'a2', a3: 'a3'}
        d4 = {a1: 'a1', a2: 'a2', a3:'a3'}
        d5 = {a1: 'a1', a2: 'a2', a3:'a3', a4:'a4'}

        self.assertIn(a1, d1)
        self.assertIn(a2, d1)
        self.assertIn(a3, d1)  # a3 == a1
        self.assertIn(a4, d1)  # a4 == a1
        self.assertNotIn(a5, d1)

        self.assertIn(a1, d2)
        self.assertIn(a2, d2)  # a1 == a2
        self.assertIn(a3, d2)
        self.assertIn(a4, d2)  # a4 == a1
        self.assertNotIn(a5, d2)

        self.assertIn(a1, d3)  # a1 == a3
        self.assertIn(a2, d3)
        self.assertIn(a3, d3)
        self.assertIn(a4, d3)  # a4 == a2
        self.assertNotIn(a5, d3)

        self.assertIn(a1, d4)
        self.assertIn(a2, d4)
        self.assertIn(a3, d4)
        self.assertIn(a4, d4)  # a4 == a1
        self.assertNotIn(a5, d4)

        self.assertIn(a1, d5)
        self.assertIn(a2, d5)
        self.assertIn(a3, d5)
        self.assertIn(a4, d5)
        self.assertNotIn(a5, d5)

