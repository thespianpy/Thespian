from unittest import TestCase

from thespian.actors import ValidateSource, ActorSystemMessage

class TestValidateSourceMsg(TestCase):
    scope = 'unit'

    def test_equality(self):
        m1 = ValidateSource(1, 2)
        self.assertEqual(m1, ValidateSource(1, 2))
        self.assertEqual(m1, ValidateSource(1, 3))
        self.assertEqual(m1, ValidateSource(1, 'ignored'))
        self.assertEqual(m1, ValidateSource(1, None))
        m2 = ValidateSource('hi', False)
        self.assertEqual(m2, ValidateSource('hi', False))
        self.assertEqual(m2, ValidateSource('hi', True))
        self.assertEqual(m2, ValidateSource('hi', 1))
        self.assertEqual(m2, ValidateSource('hi', 0))
        self.assertEqual(m2, ValidateSource('hi', None))

    def test_inequality(self):
        m1 = ValidateSource(1, 2)
        self.assertNotEqual(m1, 1)
        self.assertNotEqual(m1, ValidateSource('hi', True))
        self.assertNotEqual(m1, ValidateSource(0, 2))
        self.assertNotEqual(m1, ValidateSource(9, 3))
        self.assertNotEqual(m1, ValidateSource(None, None))

    def test_properties(self):
        self.assertEqual(1, ValidateSource(1, 2).sourceHash)
        self.assertEqual(2, ValidateSource(1, 2).sourceData)

        self.assertEqual(None, ValidateSource(None, '').sourceHash)
        self.assertEqual('', ValidateSource(None, '').sourceData)

        self.assertEqual('foo', ValidateSource('foo', 3.3).sourceHash)
        self.assertEqual(3.3, ValidateSource('foo', 3.3).sourceData)

    def test_inheritance(self):
        self.assertTrue(isinstance(ValidateSource(1, 2), ValidateSource))
        self.assertTrue(isinstance(ValidateSource('one', None), ValidateSource))
        self.assertTrue(isinstance(ValidateSource(1, 2), ActorSystemMessage))
        self.assertTrue(isinstance(ValidateSource('one', None), ActorSystemMessage))
