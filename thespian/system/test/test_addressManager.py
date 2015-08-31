from thespian.actors import ActorAddress
from thespian.system.addressManager import (ActorLocalAddress,
                                            CannotPickle,
                                            CannotPickleAddress,
                                            ActorAddressManager)
import unittest
import thespian.test.helpers
import pickle


# When creating ActorAddress objects in this file for non-LocalAddress
# objects, the addressDetails is largely immaterial as long as it has
# proper __eq__ characteristics.  Often, simply the id of an object is
# used for variance.

class FakeAddress(object):
    def __init__(self):
        self.addressDetails = 'TestIntentAddressDetails'
    def __eq__(self, o):
        return id(self) == id(o)


class TestActorLocalAddress(unittest.TestCase):
    scope='unit'

    # n.b. the ActorLocalAddress is the addressDetails portion of an ActorAddress

    def testCreate(self):
        genAddr = ActorAddress(id(self))
        addr = ActorLocalAddress(genAddr, 0, None)
        # No exception thrown
        self.assertTrue(True)

    def testUniqueHash(self):
        genAddr = ActorAddress(id(self))
        addrs = [ActorLocalAddress(genAddr, N, None) for N in range(32)]
        hashes = set([hash(A) for A in addrs])
        self.assertEqual(len(addrs), len(hashes))

    def testEquality(self):
        genAddr = ActorAddress(id(self))
        addr1 = ActorLocalAddress(genAddr, 0, None)
        addr2 = ActorLocalAddress(genAddr, 0, None)
        self.assertEqual(addr1, addr2)

    def testEqualityFailsIfDifferentGeneratingAddress(self):
        genAddr = ActorAddress(id(self))
        genAddr2 = ActorAddress('hi')
        self.assertNotEqual(genAddr, genAddr2)
        addr1 = ActorLocalAddress(genAddr, 0, None)
        addr2 = ActorLocalAddress(genAddr2, 0, None)
        self.assertNotEqual(addr1, addr2)

    def testEqualityFailsIfDifferentInstanceNums(self):
        genAddr = ActorAddress(id(self))
        addr1 = ActorLocalAddress(genAddr, 0, None)
        addr2 = ActorLocalAddress(genAddr, 1, None)
        self.assertNotEqual(addr1, addr2)

    # n.b. no test for equality failure on different AddressManagers:
    # that element is simply not part of the equality validation, but
    # it is not a requirement at this time that it is not part of it.

    def testStringForm(self):
        self.assertNotEqual('', str(ActorLocalAddress(ActorAddress(id(self)), 0, None)))

    def testInstanceID(self):
        self.assertEqual(5, ActorLocalAddress(ActorAddress(id(self)), 5, None).addressInstanceNum)


class TestAddressManager(unittest.TestCase):
    scope='unit'

    def testCreate(self):
        am = ActorAddressManager(None, 'me')
        # no exception thrown
        self.assertTrue(True)

class TestLocalAddresses(unittest.TestCase):
    scope='unit'

    def setUp(self):
        self.myAddress = 'me'
        self.am = ActorAddressManager(None, self.myAddress)

    def testGetValidLocalAddress(self):
        addr = ActorAddress(ActorLocalAddress(self.myAddress, 12, self.am))
        self.assertIsInstance(addr.addressDetails, ActorLocalAddress)

    def testGetUniqueLocalAddresses(self):
        addr1 = self.am.createLocalAddress()
        addr2 = self.am.createLocalAddress()
        self.assertNotEqual(addr1, addr2)
        self.assertNotEqual(addr2, addr1)
        self.assertEqual(addr1, addr1)
        self.assertEqual(addr2, addr2)

    def testLocalAddressCannotBePickled(self):
        self.am = ActorAddressManager(None, 'me')
        addr = self.am.createLocalAddress()
        self.assertRaises(CannotPickleAddress, pickle.dumps, addr)

    def testLocalAddressCanBeReconstituted(self):
        addr = self.am.createLocalAddress()
        addr2 = self.am.getLocalAddress(addr.addressDetails.addressInstanceNum)
        self.assertEqual(addr, addr2)

    def testLocalAddressCannotBeUsedForSending(self):
        addr = self.am.createLocalAddress()
        self.assertIsNone(self.am.sendToAddress(addr))



class TestAddressManagerLocalAddresses(unittest.TestCase):
    scope='unit'

    def setUp(self):
        self.myAddress = 'me'
        self.am = ActorAddressManager(None, self.myAddress)

    def testGetValidLocalAddress(self):
        addr = self.am.createLocalAddress()
        self.assertIsInstance(addr.addressDetails, ActorLocalAddress)

    def testGetUniqueLocalAddresses(self):
        addr1 = self.am.createLocalAddress()
        addr2 = self.am.createLocalAddress()
        self.assertNotEqual(addr1, addr2)
        self.assertNotEqual(addr2, addr1)
        self.assertEqual(addr1, addr1)
        self.assertEqual(addr2, addr2)

    def testLocalAddressCannotBePickled(self):
        self.am = ActorAddressManager(None, 'me')
        addr = self.am.createLocalAddress()
        self.assertRaises(CannotPickleAddress, pickle.dumps, addr)

    def testLocalAddressCanBeReconstituted(self):
        addr = self.am.createLocalAddress()
        addr2 = self.am.getLocalAddress(addr.addressDetails.addressInstanceNum)
        self.assertEqual(addr, addr2)

    def testLocalAddressCannotBeUsedForSending(self):
        addr = self.am.createLocalAddress()
        self.assertIsNone(self.am.sendToAddress(addr))



class TestAddressManagerLocalAddressAssociations(unittest.TestCase):
    scope='unit'

    def setUp(self):
        self.myAddress = 'me'
        self.am = ActorAddressManager(None, self.myAddress)

    def testMakeAssociation(self):
        lclAddr = self.am.createLocalAddress()
        mainAddr = ActorAddress(id(self))
        self.am.associateUseableAddress(self.myAddress,
                                        lclAddr.addressDetails.addressInstanceNum,
                                        mainAddr)
        # No exception thrown
        self.assertTrue(True)

    def _makeLocalAndAssociated(self):
        lclAddr = self.am.createLocalAddress()
        mainAddr = ActorAddress(id(lclAddr))
        self.am.associateUseableAddress(self.myAddress,
                                        lclAddr.addressDetails.addressInstanceNum,
                                        mainAddr)
        return lclAddr, mainAddr

    def testAssociationRemembered(self):
        lclAddr, mainAddr = self._makeLocalAndAssociated()
        self.assertEqual(mainAddr, self.am.sendToAddress(lclAddr))
        self.assertEqual(mainAddr, self.am.sendToAddress(mainAddr))

    def testAssociationUnique(self):
        self.testAssociationRemembered()
        lclAddr2 = self.am.createLocalAddress()
        self.assertIsNone(self.am.sendToAddress(lclAddr2))

    def testAssociationNotRequiredForUseableNonLocal(self):
        addr = ActorAddress(id(self))
        self.assertEqual(addr, self.am.sendToAddress(addr))

    def testAssociationEquality(self):
        lclAddr = self.am.createLocalAddress()
        mainAddr = ActorAddress(id(self))

        self.assertNotEqual(lclAddr, mainAddr)
        self.assertNotEqual(mainAddr, lclAddr)

        self.am.associateUseableAddress(self.myAddress,
                                        lclAddr.addressDetails.addressInstanceNum,
                                        mainAddr)

        self.assertEqual(lclAddr, mainAddr)
        self.assertEqual(mainAddr, lclAddr)

    def testAssociationEqualityWithReconstitutedNonLocalAddress(self):
        lclAddr = self.am.createLocalAddress()
        mainAddr1 = ActorAddress(None)
        mainAddr2 = ActorAddress(9)
        self.assertNotEqual(mainAddr1, mainAddr2)

        self.am.associateUseableAddress(self.myAddress,
                                        lclAddr.addressDetails.addressInstanceNum,
                                        mainAddr1)

        self.assertEqual(lclAddr, mainAddr1)
        self.assertEqual(mainAddr1, lclAddr)

        self.assertNotEqual(lclAddr, mainAddr2)
        self.assertNotEqual(mainAddr2, lclAddr)

        mainAddr1_dup = ActorAddress(None)
        self.assertEqual(mainAddr1, mainAddr1_dup)

        self.assertNotEqual(mainAddr1_dup, lclAddr)
        self.assertEqual(lclAddr, mainAddr1_dup)

        self.am.importAddr(mainAddr1_dup)

        self.assertEqual(mainAddr1_dup, lclAddr)
        self.assertEqual(lclAddr, mainAddr1_dup)
        self.assertEqual(mainAddr1_dup, mainAddr1)

    def testAssociatedAddressesDoNotMatchArbitraryStuff(self):
        lclAddr1, mainAddr1 = self._makeLocalAndAssociated()

        self.assertNotEqual(None, lclAddr1)
        self.assertNotEqual(id(self), lclAddr1)
        self.assertNotEqual(0, lclAddr1)
        self.assertNotEqual("hi", lclAddr1)
        self.assertNotEqual(unittest.TestCase, lclAddr1)

        self.assertNotEqual(None, mainAddr1)
        self.assertNotEqual(id(self), mainAddr1)
        self.assertNotEqual(0, mainAddr1)
        self.assertNotEqual("hi", mainAddr1)
        self.assertNotEqual(unittest.TestCase, mainAddr1)

        self.assertNotEqual(lclAddr1, None)
        self.assertNotEqual(lclAddr1, id(self))
        self.assertNotEqual(lclAddr1, 0)
        self.assertNotEqual(lclAddr1, "hi")
        self.assertNotEqual(lclAddr1, unittest.TestCase)

        self.assertNotEqual(mainAddr1, None)
        self.assertNotEqual(mainAddr1, id(self))
        self.assertNotEqual(mainAddr1, 0)
        self.assertNotEqual(mainAddr1, "hi")
        self.assertNotEqual(mainAddr1, unittest.TestCase)

    def testAssociatedAddressEqualityIsUnique(self):
        lclAddr1, mainAddr1 = self._makeLocalAndAssociated()
        print('Set 1: %s --> %s'%(str(lclAddr1), str(mainAddr1)))
        lclAddr2, mainAddr2 = self._makeLocalAndAssociated()
        print('Set 2: %s --> %s'%(str(lclAddr2), str(mainAddr2)))
        lclAddr3, mainAddr3 = self._makeLocalAndAssociated()
        print('Set 3: %s --> %s'%(str(lclAddr3), str(mainAddr3)))

        self.assertEqual(lclAddr1, lclAddr1)
        self.assertEqual(lclAddr2, lclAddr2)
        self.assertEqual(lclAddr3, lclAddr3)

        self.assertEqual(mainAddr1, mainAddr1)
        self.assertEqual(mainAddr2, mainAddr2)
        self.assertEqual(mainAddr3, mainAddr3)

        self.assertEqual(lclAddr1, mainAddr1)
        self.assertEqual(lclAddr2, mainAddr2)
        self.assertEqual(lclAddr3, mainAddr3)

        self.assertEqual(mainAddr1, lclAddr1)
        self.assertEqual(mainAddr2, lclAddr2)
        self.assertEqual(mainAddr3, lclAddr3)

        self.assertNotEqual(lclAddr1, lclAddr2)
        self.assertNotEqual(lclAddr2, lclAddr1)
        self.assertNotEqual(lclAddr3, lclAddr2)
        self.assertNotEqual(lclAddr2, lclAddr3)
        self.assertNotEqual(lclAddr3, lclAddr1)
        self.assertNotEqual(lclAddr1, lclAddr3)

        self.assertNotEqual(mainAddr1, mainAddr2)
        self.assertNotEqual(mainAddr2, mainAddr1)
        self.assertNotEqual(mainAddr3, mainAddr2)
        self.assertNotEqual(mainAddr2, mainAddr3)
        self.assertNotEqual(mainAddr3, mainAddr1)
        self.assertNotEqual(mainAddr1, mainAddr3)

        self.assertNotEqual(mainAddr1, lclAddr2)
        self.assertNotEqual(mainAddr2, lclAddr1)
        self.assertNotEqual(mainAddr3, lclAddr2)
        self.assertNotEqual(mainAddr2, lclAddr3)
        self.assertNotEqual(mainAddr3, lclAddr1)
        self.assertNotEqual(mainAddr1, lclAddr3)

        self.assertNotEqual(lclAddr1, mainAddr2)
        self.assertNotEqual(lclAddr2, mainAddr1)
        self.assertNotEqual(lclAddr3, mainAddr2)
        self.assertNotEqual(lclAddr2, mainAddr3)
        self.assertNotEqual(lclAddr3, mainAddr1)
        self.assertNotEqual(lclAddr1, mainAddr3)


class TestAddressManagerAddressRevocation(unittest.TestCase):
    scope='unit'

    def setUp(self):
        self.myAddress = 'me'
        self.am = ActorAddressManager(None, self.myAddress)

    def _makeLocalAndAssociated(self):
        lclAddr = self.am.createLocalAddress()
        mainAddr = ActorAddress(id(lclAddr))
        self.am.associateUseableAddress(self.myAddress,
                                        lclAddr.addressDetails.addressInstanceNum,
                                        mainAddr)
        return lclAddr, mainAddr

    def testLocalAddressRevocation(self):
        lcladdr = self.am.createLocalAddress()
        self.am.deadAddress(lcladdr)
        self.assertIsNone(self.am.sendToAddress(lcladdr))
        self.assertTrue(self.am.isDeadAddress(lcladdr))
        self.assertEqual(lcladdr,
                         self.am.getLocalAddress(lcladdr.addressDetails.addressInstanceNum))

    def testMainAddressRevocation(self):
        addr = ActorAddress(id(self))
        self.am.deadAddress(addr)
        self.assertEqual(addr, self.am.sendToAddress(addr))
        self.assertTrue(self.am.isDeadAddress(addr))

    def testLocalAddressRevocationAssociation(self):
        lclAddr, mainAddr = self._makeLocalAndAssociated()
        self.am.deadAddress(lclAddr)
        self.assertEqual(mainAddr, self.am.sendToAddress(lclAddr))
        self.assertEqual(mainAddr, self.am.sendToAddress(mainAddr))
        self.assertTrue(self.am.isDeadAddress(lclAddr))
        self.assertTrue(self.am.isDeadAddress(mainAddr))
        self.assertTrue(self.am.isDeadAddress(self.am.sendToAddress(mainAddr)))
        self.assertTrue(self.am.isDeadAddress(self.am.sendToAddress(lclAddr)))

    def testNonLocalAddressRevocationAssociation(self):
        lclAddr, mainAddr = self._makeLocalAndAssociated()
        self.am.deadAddress(mainAddr)
        self.assertEqual(mainAddr, self.am.sendToAddress(lclAddr))
        self.assertEqual(mainAddr, self.am.sendToAddress(mainAddr))
        self.assertTrue(self.am.isDeadAddress(lclAddr))
        self.assertTrue(self.am.isDeadAddress(mainAddr))
        self.assertTrue(self.am.isDeadAddress(self.am.sendToAddress(mainAddr)))
        self.assertTrue(self.am.isDeadAddress(self.am.sendToAddress(lclAddr)))

    def testAssociatedAddressRevocationIsUnique(self):
        lclAddr1, mainAddr1 = self._makeLocalAndAssociated()
        lclAddr2, mainAddr2 = self._makeLocalAndAssociated()
        lclAddr3, mainAddr3 = self._makeLocalAndAssociated()

        self.assertFalse(self.am.isDeadAddress(lclAddr1))
        self.assertFalse(self.am.isDeadAddress(mainAddr1))
        self.assertFalse(self.am.isDeadAddress(self.am.sendToAddress(mainAddr1)))
        self.assertFalse(self.am.isDeadAddress(self.am.sendToAddress(lclAddr1)))

        self.assertFalse(self.am.isDeadAddress(lclAddr2))
        self.assertFalse(self.am.isDeadAddress(mainAddr2))
        self.assertFalse(self.am.isDeadAddress(self.am.sendToAddress(mainAddr2)))
        self.assertFalse(self.am.isDeadAddress(self.am.sendToAddress(lclAddr2)))

        self.assertFalse(self.am.isDeadAddress(lclAddr3))
        self.assertFalse(self.am.isDeadAddress(mainAddr3))
        self.assertFalse(self.am.isDeadAddress(self.am.sendToAddress(mainAddr3)))
        self.assertFalse(self.am.isDeadAddress(self.am.sendToAddress(lclAddr3)))

        self.am.deadAddress(lclAddr1)
        self.am.deadAddress(mainAddr2)

        self.assertTrue(self.am.isDeadAddress(lclAddr1))
        self.assertTrue(self.am.isDeadAddress(mainAddr1))
        self.assertTrue(self.am.isDeadAddress(self.am.sendToAddress(mainAddr1)))
        self.assertTrue(self.am.isDeadAddress(self.am.sendToAddress(lclAddr1)))

        self.assertTrue(self.am.isDeadAddress(lclAddr2))
        self.assertTrue(self.am.isDeadAddress(mainAddr2))
        self.assertTrue(self.am.isDeadAddress(self.am.sendToAddress(mainAddr2)))
        self.assertTrue(self.am.isDeadAddress(self.am.sendToAddress(lclAddr2)))

        self.assertFalse(self.am.isDeadAddress(lclAddr3))
        self.assertFalse(self.am.isDeadAddress(mainAddr3))
        self.assertFalse(self.am.isDeadAddress(self.am.sendToAddress(mainAddr3)))
        self.assertFalse(self.am.isDeadAddress(self.am.sendToAddress(lclAddr3)))
