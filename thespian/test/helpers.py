"""This file provides various testing helpers to ensure that the
Thespian testing works for Python3 and Python2.6 or later."""

import unittest

if not hasattr(unittest.TestCase, 'assertIsInstance'):
    def assertIsInstance(self, obj, cls):
        assert isinstance(obj, cls), \
            "Object %s is not an instance of class %s"%(str(obj), str(cls))
    unittest.TestCase.assertIsInstance = assertIsInstance


if not hasattr(unittest.TestCase, 'assertIn'):
    def assertIn(self, what, collection):
        assert what in collection, \
            'Element "%s" not found in collection "%s"'%(str(what), str(collection))
    unittest.TestCase.assertIn = assertIn


if not hasattr(unittest.TestCase, 'assertGreater'):
    def assertGreater(self, what, val):
        assert what > val, '%s is not > %s'%(str(what),str(val))
    unittest.TestCase.assertGreater = assertGreater


if not hasattr(unittest.TestCase, 'assertLess'):
    def assertLess(self, what, val):
        assert what < val, '%s is not < %s'%(str(what),str(val))
    unittest.TestCase.assertLess = assertLess


if not hasattr(unittest.TestCase, 'assertGreaterEqual'):
    def assertGreaterEqual(self, what, val):
        assert what >= val, '%s is not >= %s'%(str(what),str(val))
    unittest.TestCase.assertGreaterEqual = assertGreaterEqual


if not hasattr(unittest.TestCase, 'assertLessEqual'):
    def assertLessEqual(self, what, val):
        assert what <= val, '%s is not <= %s'%(str(what),str(val))
    unittest.TestCase.assertLessEqual = assertLessEqual


if not hasattr(unittest.TestCase, 'assertIsNot'):
    def assertIsNot(self, what, isnota):
        assert what is not isnota, \
            'Input "%s" is a "%s" BUT SHOULD NOT BE!'%(str(what), str(isnota))
    unittest.TestCase.assertIsNot = assertIsNot

if not hasattr(unittest.TestCase, 'assertIsNone'):
    def assertIsNone(self, what):
        assert what is None, \
            'Input "%s" is not None BUT SHOULD BE!'%(str(what))
    unittest.TestCase.assertIsNone = assertIsNone

if not hasattr(unittest.TestCase, 'assertIsNotNone'):
    def assertIsNotNone(self, what):
        assert what is not None, \
            'Input "%s" is None BUT SHOULD NOT BE!'%(str(what))
    unittest.TestCase.assertIsNotNone = assertIsNotNone


if not hasattr(unittest.TestCase, 'assertRaisesRegex'):
    def assertRaisesRegex(self, assertType, regex, call, *args, **kw):
        completed = False
        try:
            call(*args, **kw)
            completed = True
        except assertType as ex:
            import re
            assert re.match(regex, str(ex)), 'Regex "%s" does not match exception: %s'%(
                regex, str(ex))
        except Exception as ex:
            self.assertFalse(True, 'Call "%s" got wrong exception "%s"; expected exception type %s'%(
                str(call), str(ex), str(assertType)))
        if completed:
            self.assertFalse(True, 'Call %s(%s, %s) completed; expected assertion %s'%(
                str(call), str(args), str(kw), str(assertType)))

    unittest.TestCase.assertRaisesRegex = assertRaisesRegex


try:
    skip = unittest.skip
except:
    def skip(mssg):
        #raise unittest.SkipTest("Test %s skipped"%test.__doc__)
        #SkipTest does not exist in python 2.6 :(
        return lambda s: True
