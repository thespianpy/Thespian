from unittest import TestCase

from thespian.actors import ActorExitRequest, ActorSystemMessage

class TestExitRequestMsg(TestCase):
    scope = 'unit'

    def test_equality(self):
        m1 = ActorExitRequest()
        self.assertEqual(m1, ActorExitRequest())
        self.assertEqual(m1, ActorExitRequest(True))
        self.assertEqual(m1, ActorExitRequest(False))

    def test_inequality(self):
        m1 = ActorExitRequest()
        self.assertNotEqual(m1, True)
        self.assertNotEqual(m1, None)
        self.assertNotEqual(m1, 0)

    def test_properties(self):
        self.assertTrue(ActorExitRequest().isRecursive)
        self.assertTrue(ActorExitRequest(True).isRecursive)
        self.assertFalse(ActorExitRequest(False).isRecursive)

        m1 = ActorExitRequest()
        self.assertTrue(m1.isRecursive)
        m1.notRecursive()
        self.assertFalse(m1.isRecursive)

    def test_inheritance(self):
        self.assertTrue(isinstance(ActorExitRequest(False), ActorExitRequest))
        self.assertTrue(isinstance(ActorExitRequest(), ActorExitRequest))
        self.assertTrue(isinstance(ActorExitRequest(False), ActorSystemMessage))
        self.assertTrue(isinstance(ActorExitRequest(), ActorSystemMessage))

