from unittest import TestCase

from thespian.actors import PoisonMessage, ActorSystemMessage

class TestPoisonMsg(TestCase):
    scope = 'unit'

    def test_equality(self):
        m1 = PoisonMessage(1)
        self.assertEqual(m1, PoisonMessage(1))
        m2 = PoisonMessage('hi')
        self.assertEqual(m2, PoisonMessage('hi'))

    def test_inequality(self):
        m1 = PoisonMessage(1)
        self.assertNotEqual(m1, 1)
        self.assertNotEqual(m1, PoisonMessage('hi'))
        self.assertNotEqual(m1, PoisonMessage(0))
        self.assertNotEqual(m1, PoisonMessage(None))

    def test_properties(self):
        self.assertEqual(1, PoisonMessage(1).poisonMessage)
        self.assertEqual(None, PoisonMessage(None).poisonMessage)
        self.assertEqual('foo', PoisonMessage('foo').poisonMessage)

    def test_inheritance(self):
        self.assertTrue(isinstance(PoisonMessage(1), PoisonMessage))
        self.assertTrue(isinstance(PoisonMessage('hi'), PoisonMessage))
        self.assertTrue(isinstance(PoisonMessage(1), ActorSystemMessage))
        self.assertTrue(isinstance(PoisonMessage('hi'), ActorSystemMessage))

