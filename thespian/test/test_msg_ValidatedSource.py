from unittest import TestCase

from thespian.actors import ValidatedSource, ActorSystemMessage

class TestValidatedSourceMsg(TestCase):
    scope = 'unit'

    def test_equality(self):
        m1 = ValidatedSource(1, 2)
        self.assertEqual(m1, ValidatedSource(1, 2))
        self.assertEqual(m1, ValidatedSource(1, 4))
        self.assertEqual(m1, ValidatedSource(1, 'nine'))
        m2 = ValidatedSource('hi', False)
        self.assertEqual(m2, ValidatedSource('hi', False))
        self.assertEqual(m2, ValidatedSource('hi', True))
        self.assertEqual(m2, ValidatedSource('hi', 'ignored'))

    def test_inequality(self):
        m1 = ValidatedSource(1, 2)
        self.assertNotEqual(m1, 1)
        self.assertNotEqual(m1, ValidatedSource('hi', True))
        self.assertNotEqual(m1, ValidatedSource(0, 2))
        self.assertNotEqual(m1, ValidatedSource(None, None))

    def test_properties(self):
        self.assertEqual(1, ValidatedSource(1, 2).sourceHash)
        self.assertEqual(2, ValidatedSource(1, 2).sourceZip)

        self.assertEqual(None, ValidatedSource(None, '').sourceHash)
        self.assertEqual('', ValidatedSource(None, '').sourceZip)

        self.assertEqual('foo', ValidatedSource('foo', 3.3).sourceHash)
        self.assertEqual(3.3, ValidatedSource('foo', 3.3).sourceZip)

    def test_inheritance(self):
        self.assertTrue(isinstance(ValidatedSource(1, 2), ValidatedSource))
        self.assertTrue(isinstance(ValidatedSource('one', None), ValidatedSource))
        self.assertTrue(isinstance(ValidatedSource(1, 2), ActorSystemMessage))
        self.assertTrue(isinstance(ValidatedSource('one', None), ActorSystemMessage))
