from unittest import TestCase
import thespian.test.helpers


from thespian.actors import WakeupMessage, ActorSystemMessage

class TestExitRequestMsg(TestCase):
    scope = 'unit'

    def test_equality(self):
        m1 = WakeupMessage(1)
        self.assertEqual(m1, WakeupMessage(1))
        m2 = WakeupMessage('hi')
        self.assertEqual(m2, WakeupMessage('hi'))

    def test_inequality(self):
        m1 = WakeupMessage(1)
        self.assertNotEqual(m1, 1)
        self.assertNotEqual(m1, WakeupMessage('hi'))
        self.assertNotEqual(m1, WakeupMessage(0))
        self.assertNotEqual(m1, WakeupMessage(None))

    def test_properties(self):
        self.assertEqual(1, WakeupMessage(1).delayPeriod)
        self.assertEqual(None, WakeupMessage(None).delayPeriod)
        self.assertEqual('foo', WakeupMessage('foo').delayPeriod)

    def test_inheritance(self):
        self.assertIsInstance(WakeupMessage(1), WakeupMessage)
        self.assertIsInstance(WakeupMessage('one'), WakeupMessage)
        self.assertIsInstance(WakeupMessage(1), ActorSystemMessage)
        self.assertIsInstance(WakeupMessage('one'), ActorSystemMessage)
