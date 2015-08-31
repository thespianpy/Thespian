from unittest import TestCase

from thespian.actors import ChildActorExited, ActorSystemMessage

class TestChildExitedMsg(TestCase):
    scope = 'unit'

    def test_equality(self):
        m1 = ChildActorExited(1)
        self.assertEqual(m1, ChildActorExited(1))

    def test_inequality(self):
        m1 = ChildActorExited(1)
        self.assertNotEqual(m1, 1)
        self.assertNotEqual(m1, ChildActorExited('one'))
        self.assertNotEqual(m1, ChildActorExited(0))
        self.assertNotEqual(m1, ChildActorExited(None))

    def test_properties(self):
        self.assertEqual(1, ChildActorExited(1).childAddress)
        self.assertEqual(None, ChildActorExited(None).childAddress)
        self.assertEqual('foo', ChildActorExited('foo').childAddress)

    def test_inheritance(self):
        self.assertTrue(isinstance(ChildActorExited(1), ChildActorExited))
        self.assertTrue(isinstance(ChildActorExited('one'), ChildActorExited))
        self.assertTrue(isinstance(ChildActorExited(1), ActorSystemMessage))
        self.assertTrue(isinstance(ChildActorExited('one'), ActorSystemMessage))

