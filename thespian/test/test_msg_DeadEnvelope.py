from unittest import TestCase

from thespian.actors import DeadEnvelope, ActorSystemMessage

class TestDeadEnvelopeMsg(TestCase):
    scope = 'unit'

    def test_equality(self):
        m1 = DeadEnvelope(1, 2)
        self.assertEqual(m1, DeadEnvelope(1, 2))
        m2 = DeadEnvelope('hi', False)
        self.assertEqual(m2, DeadEnvelope('hi', False))

    def test_inequality(self):
        m1 = DeadEnvelope(1, 2)
        self.assertNotEqual(m1, 1)
        self.assertNotEqual(m1, DeadEnvelope('hi', True))
        self.assertNotEqual(m1, DeadEnvelope(0, 2))
        self.assertNotEqual(m1, DeadEnvelope(1, 3))
        self.assertNotEqual(m1, DeadEnvelope(None, None))

    def test_properties(self):
        self.assertEqual(1, DeadEnvelope(1, 2).deadAddress)
        self.assertEqual(2, DeadEnvelope(1, 2).deadMessage)

        self.assertEqual(None, DeadEnvelope(None, '').deadAddress)
        self.assertEqual('', DeadEnvelope(None, '').deadMessage)

        self.assertEqual('foo', DeadEnvelope('foo', 3.3).deadAddress)
        self.assertEqual(3.3, DeadEnvelope('foo', 3.3).deadMessage)

    def test_inheritance(self):
        self.assertTrue(isinstance(DeadEnvelope(1, 2), DeadEnvelope))
        self.assertTrue(isinstance(DeadEnvelope('one', None), DeadEnvelope))
        self.assertTrue(isinstance(DeadEnvelope(1, 2), ActorSystemMessage))
        self.assertTrue(isinstance(DeadEnvelope('one', None), ActorSystemMessage))
