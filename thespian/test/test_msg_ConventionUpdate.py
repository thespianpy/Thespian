from unittest import TestCase

from thespian.actors import ActorSystemConventionUpdate, ActorSystemMessage

class TestConventionUpdate(TestCase):
    scope = 'unit'

    def test_equality(self):
        c1 = ActorSystemConventionUpdate('addr1', 'cap1', True)
        self.assertEqual(c1, ActorSystemConventionUpdate('addr1', 'cap1', True))

    def test_inequality(self):
        c1 = ActorSystemConventionUpdate('addr1', 'cap1', True)
        self.assertNotEqual(c1, ActorSystemConventionUpdate('addr1', 'cap1', False))
        self.assertNotEqual(c1, ActorSystemConventionUpdate('addr1', ['cap1'], True))
        self.assertNotEqual(c1, ActorSystemConventionUpdate(2, 'cap1', True))

    def test_properties(self):
        c1 = ActorSystemConventionUpdate('addr', {'caps':1}, True)
        self.assertEqual(c1.remoteAdminAddress, 'addr')
        self.assertEqual(c1.remoteCapabilities, {'caps':1})
        self.assertEqual(c1.remoteAdded, True)

        c2 = ActorSystemConventionUpdate('addr', {'caps':1}, False)
        self.assertEqual(c2.remoteAdded, False)

        c3 = ActorSystemConventionUpdate('addr', {'caps':1})
        self.assertEqual(c3.remoteAdded, True)

    def test_inheritance(self):
        self.assertTrue(isinstance(ActorSystemConventionUpdate(1, 2, True), ActorSystemConventionUpdate))
        self.assertTrue(isinstance(ActorSystemConventionUpdate('one', None, False), ActorSystemConventionUpdate))
        self.assertTrue(isinstance(ActorSystemConventionUpdate(1, 2, True), ActorSystemMessage))
        self.assertTrue(isinstance(ActorSystemConventionUpdate('one', None, False), ActorSystemMessage))
