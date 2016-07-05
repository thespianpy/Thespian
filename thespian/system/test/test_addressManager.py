from pytest import raises
from thespian.actors import ActorAddress
from thespian.system.addressManager import (ActorLocalAddress,
                                            CannotPickle,
                                            CannotPickleAddress,
                                            ActorAddressManager)
import pickle

class SomeRandomObject(object): pass


# When creating ActorAddress objects in this file for non-LocalAddress
# objects, the addressDetails is largely immaterial as long as it has
# proper __eq__ characteristics.  Often, simply the id of an object is
# used for variance.

class FakeAddress(object):
    def __init__(self):
        self.addressDetails = 'TestIntentAddressDetails'
    def __eq__(self, o):
        return id(self) == id(o)


class TestUnitActorLocalAddress(object):

    # n.b. the ActorLocalAddress is the addressDetails portion of an ActorAddress

    def testCreate(self):
        genAddr = ActorAddress(id(self))
        addr = ActorLocalAddress(genAddr, 0, None)
        # No exception thrown
        assert True

    def testNonHashable(self):
        genAddr = ActorAddress(id(self))
        lclAddr1 = ActorLocalAddress(genAddr, 0, None)
        lclAddr2 = ActorLocalAddress(genAddr, 1, None)
        raises(TypeError, hash, lclAddr1)
        raises(TypeError, hash, lclAddr2)

    def testEquality(self):
        genAddr = ActorAddress(id(self))
        addr1 = ActorLocalAddress(genAddr, 0, None)
        addr2 = ActorLocalAddress(genAddr, 0, None)
        assert addr1 == addr2

    def testEqualityFailsIfDifferentGeneratingAddress(self):
        genAddr = ActorAddress(id(self))
        genAddr2 = ActorAddress('hi')
        assert genAddr != genAddr2
        addr1 = ActorLocalAddress(genAddr, 0, None)
        addr2 = ActorLocalAddress(genAddr2, 0, None)
        assert addr1 != addr2

    def testEqualityFailsIfDifferentInstanceNums(self):
        genAddr = ActorAddress(id(self))
        addr1 = ActorLocalAddress(genAddr, 0, None)
        addr2 = ActorLocalAddress(genAddr, 1, None)
        assert addr1 != addr2

    # n.b. no test for equality failure on different AddressManagers:
    # that element is simply not part of the equality validation, but
    # it is not a requirement at this time that it is not part of it.

    def testStringForm(self):
        assert '' != str(ActorLocalAddress(ActorAddress(id(self)), 0, None))

    def testInstanceID(self):
        assert 5 == ActorLocalAddress(ActorAddress(id(self)), 5, None).addressInstanceNum


class TestUnitAddressManager(object):

    def testCreate(self):
        am = ActorAddressManager(None, 'me')
        # no exception thrown
        assert True


class TestUnitLocalAddresses(object):

    def testGetValidLocalAddress(self):
        am = ActorAddressManager(None, "I am me")
        addr = ActorAddress(ActorLocalAddress('I am me', 12, am))
        assert isinstance(addr.addressDetails, ActorLocalAddress)

    def testGetUniqueLocalAddresses(self):
        am = ActorAddressManager(None, "an address")
        addr1 = am.createLocalAddress()
        addr2 = am.createLocalAddress()
        assert addr1 != addr2
        assert addr2 != addr1
        assert addr1 == addr1
        assert addr2 == addr2

    def testLocalAddressCannotBePickled(self):
        am = ActorAddressManager(None, 'me')
        addr = am.createLocalAddress()
        raises(CannotPickleAddress, pickle.dumps, addr)

    def testLocalAddressCanBeReconstituted(self):
        am = ActorAddressManager(None, 'my-address')
        addr = am.createLocalAddress()
        addr2 = am.getLocalAddress(addr.addressDetails.addressInstanceNum)
        assert addr == addr2

    def testLocalAddressCannotBeUsedForSending(self):
        am = ActorAddressManager(None, 'my:address')
        addr = am.createLocalAddress()
        assert am.sendToAddress(addr) is None



class TestUnitAddressManagerLocalAddresses(object):

    def testGetValidLocalAddress(self):
        am = ActorAddressManager(None, "self.myAddress")
        addr = am.createLocalAddress()
        assert isinstance(addr.addressDetails, ActorLocalAddress)

    def testGetUniqueLocalAddresses(self):
        am = ActorAddressManager(None, "my address")
        addr1 = am.createLocalAddress()
        addr2 = am.createLocalAddress()
        assert addr1 != addr2
        assert addr2 != addr1
        assert addr1 == addr1
        assert addr2 == addr2

    def testLocalAddressCannotBePickled(self):
        am = ActorAddressManager(None, 'me')
        addr = am.createLocalAddress()
        raises(CannotPickleAddress, pickle.dumps, addr)

    def testLocalAddressCanBeReconstituted(self):
        am = ActorAddressManager(None, "my own: address")
        addr = am.createLocalAddress()
        addr2 = am.getLocalAddress(addr.addressDetails.addressInstanceNum)
        assert addr == addr2

    def testLocalAddressCannotBeUsedForSending(self):
        am = ActorAddressManager(None, "here")
        addr = am.createLocalAddress()
        assert am.sendToAddress(addr) is None



class TestUnitAddressManagerLocalAddressAssociations(object):

    def testMakeAssociation(self):
        myAddress = 'me'
        am = ActorAddressManager(None, myAddress)
        lclAddr = am.createLocalAddress()
        mainAddr = ActorAddress(id(self))
        am.associateUseableAddress(myAddress,
                                   lclAddr.addressDetails.addressInstanceNum,
                                   mainAddr)
        # No exception thrown
        assert True

    def _makeLocalAndAssociated(self, myAddress, am):
        lclAddr = am.createLocalAddress()
        mainAddr = ActorAddress(id(lclAddr))
        am.associateUseableAddress(myAddress,
                                   lclAddr.addressDetails.addressInstanceNum,
                                   mainAddr)
        return lclAddr, mainAddr

    def testAssociationRemembered(self):
        myAddress = 'me'
        am = ActorAddressManager(None, myAddress)
        lclAddr, mainAddr = self._makeLocalAndAssociated(myAddress, am)
        assert mainAddr == am.sendToAddress(lclAddr)
        assert mainAddr == am.sendToAddress(mainAddr)

    def testAssociationUnique(self):
        myAddress = 'me'
        am = ActorAddressManager(None, myAddress)
        self.testAssociationRemembered()
        lclAddr2 = am.createLocalAddress()
        assert am.sendToAddress(lclAddr2) is None

    def testAssociationNotRequiredForUseableNonLocal(self):
        myAddress = 'me'
        am = ActorAddressManager(None, myAddress)
        addr = ActorAddress(id(self))
        assert addr == am.sendToAddress(addr)

    def testAssociationEquality(self):
        myAddress = 'me'
        am = ActorAddressManager(None, myAddress)
        lclAddr = am.createLocalAddress()
        mainAddr = ActorAddress(id(self))

        assert lclAddr != mainAddr
        assert mainAddr != lclAddr

        am.associateUseableAddress(myAddress,
                                   lclAddr.addressDetails.addressInstanceNum,
                                   mainAddr)

        assert lclAddr == mainAddr
        assert mainAddr == lclAddr

    def testAssociationStillNotHashable(self):
        myAddress = 'me'
        am = ActorAddressManager(None, myAddress)
        lclAddr = am.createLocalAddress()
        mainAddr = ActorAddress(id(self))

        raises(TypeError, hash, lclAddr)
        raises(TypeError, hash, mainAddr)

        am.associateUseableAddress(myAddress,
                                   lclAddr.addressDetails.addressInstanceNum,
                                   mainAddr)

        raises(TypeError, hash, lclAddr)
        raises(TypeError, hash, mainAddr)

    def testAssociationEqualityWithReconstitutedNonLocalAddress(self):
        myAddress = 'me'
        am = ActorAddressManager(None, myAddress)
        lclAddr = am.createLocalAddress()
        mainAddr1 = ActorAddress(None)
        mainAddr2 = ActorAddress(9)
        assert mainAddr1 != mainAddr2

        am.associateUseableAddress(myAddress,
                                   lclAddr.addressDetails.addressInstanceNum,
                                   mainAddr1)

        assert lclAddr == mainAddr1
        assert mainAddr1 == lclAddr

        assert lclAddr != mainAddr2
        assert mainAddr2 != lclAddr

        mainAddr1_dup = ActorAddress(None)
        assert mainAddr1 == mainAddr1_dup

        assert mainAddr1_dup != lclAddr
        assert lclAddr == mainAddr1_dup

        am.importAddr(mainAddr1_dup)

        assert mainAddr1_dup == lclAddr
        assert lclAddr == mainAddr1_dup
        assert mainAddr1_dup == mainAddr1

    def testAssociationHashEqualityWithReconstitutedNonLocalAddress(self):
        myAddress = 'me'
        am = ActorAddressManager(None, myAddress)
        lclAddr = am.createLocalAddress()
        mainAddr1 = ActorAddress(None)
        mainAddr2 = ActorAddress(9)
        assert mainAddr1 != mainAddr2

        raises(TypeError, hash, lclAddr)
        raises(TypeError, hash, mainAddr1)
        raises(TypeError, hash, mainAddr2)

        am.associateUseableAddress(myAddress,
                                   lclAddr.addressDetails.addressInstanceNum,
                                   mainAddr1)

        raises(TypeError, hash, lclAddr)
        raises(TypeError, hash, mainAddr1)
        raises(TypeError, hash, mainAddr2)

        mainAddr1_dup = ActorAddress(None)
        assert mainAddr1 == mainAddr1_dup

        raises(TypeError, hash, lclAddr)
        raises(TypeError, hash, mainAddr1)
        raises(TypeError, hash, mainAddr1_dup)
        raises(TypeError, hash, mainAddr2)

        am.importAddr(mainAddr1_dup)

        raises(TypeError, hash, lclAddr)
        raises(TypeError, hash, mainAddr1)
        raises(TypeError, hash, mainAddr1_dup)
        raises(TypeError, hash, mainAddr2)


    def testAssociatedAddressesDoNotMatchArbitraryStuff(self):
        myAddress = 'my addr'
        am = ActorAddressManager(None, myAddress)
        lclAddr1, mainAddr1 = self._makeLocalAndAssociated(myAddress, am)

        assert None != lclAddr1
        assert id(self) != lclAddr1
        assert 0 != lclAddr1
        assert "hi" != lclAddr1
        assert SomeRandomObject != lclAddr1

        assert None != mainAddr1
        assert id(self) != mainAddr1
        assert 0 != mainAddr1
        assert "hi" != mainAddr1
        assert SomeRandomObject != mainAddr1

        assert lclAddr1 != None
        assert lclAddr1 != id(self)
        assert lclAddr1 != 0
        assert lclAddr1 != "hi"
        assert lclAddr1 != SomeRandomObject

        assert mainAddr1 != None
        assert mainAddr1 != id(self)
        assert mainAddr1 != 0
        assert mainAddr1 != "hi"
        assert mainAddr1 != SomeRandomObject

    def testAssociatedAddressEqualityIsUnique(self):
        myAddress = 'thisaddr'
        am = ActorAddressManager(None, myAddress)
        lclAddr1, mainAddr1 = self._makeLocalAndAssociated(myAddress, am)
        print('Set 1: %s --> %s'%(str(lclAddr1), str(mainAddr1)))
        lclAddr2, mainAddr2 = self._makeLocalAndAssociated(myAddress, am)
        print('Set 2: %s --> %s'%(str(lclAddr2), str(mainAddr2)))
        lclAddr3, mainAddr3 = self._makeLocalAndAssociated(myAddress, am)
        print('Set 3: %s --> %s'%(str(lclAddr3), str(mainAddr3)))

        assert lclAddr1 == lclAddr1
        assert lclAddr2 == lclAddr2
        assert lclAddr3 == lclAddr3

        assert mainAddr1 == mainAddr1
        assert mainAddr2 == mainAddr2
        assert mainAddr3 == mainAddr3

        assert lclAddr1 == mainAddr1
        assert lclAddr2 == mainAddr2
        assert lclAddr3 == mainAddr3

        assert mainAddr1 == lclAddr1
        assert mainAddr2 == lclAddr2
        assert mainAddr3 == lclAddr3

        assert lclAddr1 != lclAddr2
        assert lclAddr2 != lclAddr1
        assert lclAddr3 != lclAddr2
        assert lclAddr2 != lclAddr3
        assert lclAddr3 != lclAddr1
        assert lclAddr1 != lclAddr3

        assert mainAddr1 != mainAddr2
        assert mainAddr2 != mainAddr1
        assert mainAddr3 != mainAddr2
        assert mainAddr2 != mainAddr3
        assert mainAddr3 != mainAddr1
        assert mainAddr1 != mainAddr3

        assert mainAddr1 != lclAddr2
        assert mainAddr2 != lclAddr1
        assert mainAddr3 != lclAddr2
        assert mainAddr2 != lclAddr3
        assert mainAddr3 != lclAddr1
        assert mainAddr1 != lclAddr3

        assert lclAddr1 != mainAddr2
        assert lclAddr2 != mainAddr1
        assert lclAddr3 != mainAddr2
        assert lclAddr2 != mainAddr3
        assert lclAddr3 != mainAddr1
        assert lclAddr1 != mainAddr3


class TestUnitAddressManagerAddressRevocation(object):

    def _makeLocalAndAssociated(self, myAddress, am):
        lclAddr = am.createLocalAddress()
        mainAddr = ActorAddress(id(lclAddr))
        am.associateUseableAddress(myAddress,
                                   lclAddr.addressDetails.addressInstanceNum,
                                   mainAddr)
        return lclAddr, mainAddr

    def testLocalAddressRevocation(self):
        myAddress = 'me'
        am = ActorAddressManager(None, myAddress)
        lcladdr = am.createLocalAddress()
        am.deadAddress(lcladdr)
        assert am.sendToAddress(lcladdr) is None
        assert am.isDeadAddress(lcladdr)
        assert lcladdr == am.getLocalAddress(
            lcladdr.addressDetails.addressInstanceNum)

    def testMainAddressRevocation(self):
        myAddress = 'me'
        am = ActorAddressManager(None, myAddress)
        addr = ActorAddress(id(self))
        am.deadAddress(addr)
        assert addr == am.sendToAddress(addr)
        assert am.isDeadAddress(addr)

    def testLocalAddressRevocationAssociation(self):
        myAddress = 'me'
        am = ActorAddressManager(None, myAddress)
        lclAddr, mainAddr = self._makeLocalAndAssociated(myAddress, am)
        am.deadAddress(lclAddr)
        assert mainAddr == am.sendToAddress(lclAddr)
        assert mainAddr == am.sendToAddress(mainAddr)
        assert am.isDeadAddress(lclAddr)
        assert am.isDeadAddress(mainAddr)
        assert am.isDeadAddress(am.sendToAddress(mainAddr))
        assert am.isDeadAddress(am.sendToAddress(lclAddr))

    def testNonLocalAddressRevocationAssociation(self):
        myAddress = 'me'
        am = ActorAddressManager(None, myAddress)
        lclAddr, mainAddr = self._makeLocalAndAssociated(myAddress, am)
        am.deadAddress(mainAddr)
        assert mainAddr == am.sendToAddress(lclAddr)
        assert mainAddr == am.sendToAddress(mainAddr)
        assert am.isDeadAddress(lclAddr)
        assert am.isDeadAddress(mainAddr)
        assert am.isDeadAddress(am.sendToAddress(mainAddr))
        assert am.isDeadAddress(am.sendToAddress(lclAddr))

    def testAssociatedAddressRevocationIsUnique(self):
        myAddress = 'me'
        am = ActorAddressManager(None, myAddress)
        lclAddr1, mainAddr1 = self._makeLocalAndAssociated(myAddress, am)
        print('1',str(lclAddr1),str(mainAddr1))
        lclAddr2, mainAddr2 = self._makeLocalAndAssociated(myAddress, am)
        print('2',str(lclAddr2),str(mainAddr2))
        lclAddr3, mainAddr3 = self._makeLocalAndAssociated(myAddress, am)
        print('3',str(lclAddr3),str(mainAddr3))

        assert not am.isDeadAddress(lclAddr1)
        assert not am.isDeadAddress(mainAddr1)
        print('1sm',str(am.sendToAddress(mainAddr1)))
        assert not am.isDeadAddress(am.sendToAddress(mainAddr1))
        print('1sl',str(am.sendToAddress(lclAddr1)))
        assert not am.isDeadAddress(am.sendToAddress(lclAddr1))

        assert not am.isDeadAddress(lclAddr2)
        assert not am.isDeadAddress(mainAddr2)
        assert not am.isDeadAddress(am.sendToAddress(mainAddr2))
        assert not am.isDeadAddress(am.sendToAddress(lclAddr2))

        assert not am.isDeadAddress(lclAddr3)
        assert not am.isDeadAddress(mainAddr3)
        assert not am.isDeadAddress(am.sendToAddress(mainAddr3))
        assert not am.isDeadAddress(am.sendToAddress(lclAddr3))

        am.deadAddress(lclAddr1)
        am.deadAddress(mainAddr2)

        assert am.isDeadAddress(lclAddr1)
        assert am.isDeadAddress(mainAddr1)
        assert am.isDeadAddress(am.sendToAddress(mainAddr1))
        assert am.isDeadAddress(am.sendToAddress(lclAddr1))

        assert am.isDeadAddress(lclAddr2)
        assert am.isDeadAddress(mainAddr2)
        assert am.isDeadAddress(am.sendToAddress(mainAddr2))
        assert am.isDeadAddress(am.sendToAddress(lclAddr2))

        assert not am.isDeadAddress(lclAddr3)
        assert not am.isDeadAddress(mainAddr3)
        assert not am.isDeadAddress(am.sendToAddress(mainAddr3))
        assert not am.isDeadAddress(am.sendToAddress(lclAddr3))
