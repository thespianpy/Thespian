from thespian.actors import ActorSystemConventionUpdate, ActorSystemMessage


class TestUnitConventionUpdate(object):

    def test_equality(self):
        c1 = ActorSystemConventionUpdate('addr1', 'cap1', True)
        assert c1, ActorSystemConventionUpdate('addr1', 'cap1' == True)

    def test_inequality(self):
        c1 = ActorSystemConventionUpdate('addr1', 'cap1', True)
        assert c1 != ActorSystemConventionUpdate('addr1', 'cap1', False)
        assert c1 != ActorSystemConventionUpdate('addr1', ['cap1'], True)
        assert c1 != ActorSystemConventionUpdate(2, 'cap1', True)

    def test_properties(self):
        c1 = ActorSystemConventionUpdate('addr', {'caps':1}, True)
        assert c1.remoteAdminAddress == 'addr'
        assert c1.remoteCapabilities == {'caps':1}
        assert c1.remoteAdded == True

        c2 = ActorSystemConventionUpdate('addr', {'caps':1}, False)
        assert c2.remoteAdded == False

        c3 = ActorSystemConventionUpdate('addr', {'caps':1})
        assert c3.remoteAdded == True

    def test_inheritance(self):
        assert isinstance(ActorSystemConventionUpdate(1, 2, True), ActorSystemConventionUpdate)
        assert isinstance(ActorSystemConventionUpdate('one', None, False), ActorSystemConventionUpdate)
        assert isinstance(ActorSystemConventionUpdate(1, 2, True), ActorSystemMessage)
        assert isinstance(ActorSystemConventionUpdate('one', None, False), ActorSystemMessage)
