import unittest
import thespian.test.helpers
from thespian.system.transport import ResultCallback
from datetime import datetime, timedelta
from time import sleep


class TestResultCallback(unittest.TestCase):
    scope="unit"

    def _good(self, result, value):
        if not hasattr(self, 'goods'): self.goods = []
        self.goods.append( (result, value) )

    def _fail(self, result, value):
        if not hasattr(self, 'fails'): self.fails = []
        self.fails.append( (result, value) )

    def testGoodCallback(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc.resultCallback(True, 5)
        self.assertEqual(self.goods, [(True, 5)])
        self.assertEqual(self.fails, [])

    def testFailCallback(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc.resultCallback(False, 9)
        self.assertEqual(self.goods, [])
        self.assertEqual(self.fails, [(False, 9)])

    def testGoodCallbackReCall(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc.resultCallback(True, 5)
        self.assertEqual(self.goods, [(True, 5)])
        self.assertEqual(self.fails, [])
        rc.resultCallback(True, 4)
        self.assertEqual(self.goods, [(True, 5)])
        self.assertEqual(self.fails, [])

    def testFailCallbackReCall(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc.resultCallback(False, 9)
        self.assertEqual(self.goods, [])
        self.assertEqual(self.fails, [(False, 9)])
        rc.resultCallback(False, 8)
        self.assertEqual(self.goods, [])
        self.assertEqual(self.fails, [(False, 9)])

    def testGoodCallbackReCallFail(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc.resultCallback(True, 5)
        self.assertEqual(self.goods, [(True, 5)])
        self.assertEqual(self.fails, [])
        rc.resultCallback(False, 4)
        self.assertEqual(self.goods, [(True, 5)])
        self.assertEqual(self.fails, [])

    def testFailCallbackReCallGood(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc.resultCallback(False, 9)
        self.assertEqual(self.goods, [])
        self.assertEqual(self.fails, [(False, 9)])
        rc.resultCallback(True, 8)
        self.assertEqual(self.goods, [])
        self.assertEqual(self.fails, [(False, 9)])

    def testManyGoodCallbacks(self):
        self.goods = []
        self.fails = []
        rc = [ResultCallback(self._good, self._fail) for N in range(20)]
        for num,each in enumerate(rc):
            each.resultCallback(True, num)
        self.assertEqual(self.goods, [(True, N) for N in range(20)])
        self.assertEqual(self.fails, [])

    def testManyFailCallbacks(self):
        self.goods = []
        self.fails = []
        rc = [ResultCallback(self._good, self._fail) for N in range(20)]
        for num,each in enumerate(rc):
            each.resultCallback(False, num)
        self.assertEqual(self.goods, [])
        self.assertEqual(self.fails, [(False, N) for N in range(20)])

    def testManyGoodAndFailCallbacks(self):
        self.goods = []
        self.fails = []
        rc = [ResultCallback(self._good, self._fail) for N in range(20)]
        for num,each in enumerate(rc):
            each.resultCallback(0 == num % 3, num)
        self.assertEqual(self.goods, [(True, N) for N in range(20) if N % 3 == 0])
        self.assertEqual(self.fails, [(False, N) for N in range(20) if N % 3])

    def testChainedGoodCallbacks(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc2 = ResultCallback(self._good, self._fail, rc)
        rc3 = ResultCallback(self._good, self._fail, rc2)
        rc3.resultCallback(True, 'good')
        self.assertEqual(self.goods, [(True, 'good')] * 3)
        self.assertEqual(self.fails, [])

    def testChainedFailCallbacks(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc2 = ResultCallback(self._good, self._fail, rc)
        rc3 = ResultCallback(self._good, self._fail, rc2)
        rc3.resultCallback(False, 'oops')
        self.assertEqual(self.goods, [])
        self.assertEqual(self.fails, [(False, 'oops')] * 3)

    def testChainedGoodCallbacksDoNotDuplicate(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc2 = ResultCallback(self._good, self._fail, rc)
        rc3 = ResultCallback(self._good, self._fail, rc2)
        rc2.resultCallback(True, 'ok')
        self.assertEqual(self.goods, [(True, 'ok'), (True, 'ok')])
        self.assertEqual(self.fails, [])
        rc3.resultCallback(True, 'good')
        self.assertEqual(self.goods, [(True, 'ok'), (True, 'ok'), (True, 'good')])
        self.assertEqual(self.fails, [])

    def testChainedFailCallbacksDoNotDuplicate(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc2 = ResultCallback(self._good, self._fail, rc)
        rc3 = ResultCallback(self._good, self._fail, rc2)
        rc2.resultCallback(False, 'bad')
        self.assertEqual(self.goods, [])
        self.assertEqual(self.fails, [(False, 'bad'), (False, 'bad')])
        rc3.resultCallback(False, 'oops')
        self.assertEqual(self.goods, [])
        self.assertEqual(self.fails, [(False, 'bad'), (False, 'bad'), (False, 'oops')])

    def testChainedGoodCallbacksDoNotDuplicateOnFail(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc2 = ResultCallback(self._good, self._fail, rc)
        rc3 = ResultCallback(self._good, self._fail, rc2)
        rc2.resultCallback(True, 'ok')
        self.assertEqual(self.goods, [(True, 'ok'), (True, 'ok')])
        self.assertEqual(self.fails, [])
        rc3.resultCallback(False, 'bad')
        self.assertEqual(self.goods, [(True, 'ok'), (True, 'ok')])
        self.assertEqual(self.fails, [(False, 'bad')])

    def testChainedFailCallbacksDoNotDuplicateOnGood(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc2 = ResultCallback(self._good, self._fail, rc)
        rc3 = ResultCallback(self._good, self._fail, rc2)
        rc2.resultCallback(False, 'bad')
        self.assertEqual(self.goods, [])
        self.assertEqual(self.fails, [(False, 'bad'), (False, 'bad')])
        rc3.resultCallback(True, 'yippee')
        self.assertEqual(self.goods, [(True, 'yippee')])
        self.assertEqual(self.fails, [(False, 'bad'), (False, 'bad')])

