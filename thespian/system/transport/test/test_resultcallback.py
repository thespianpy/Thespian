from thespian.system.transport import ResultCallback
from datetime import datetime, timedelta
from time import sleep


class TestUnitResultCallback(object):

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
        assert self.goods == [(True, 5)]
        assert self.fails == []

    def testFailCallback(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc.resultCallback(False, 9)
        assert self.goods == []
        assert self.fails == [(False, 9)]

    def testGoodCallbackReCall(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc.resultCallback(True, 5)
        assert self.goods == [(True, 5)]
        assert self.fails == []
        rc.resultCallback(True, 4)
        assert self.goods == [(True, 5)]
        assert self.fails == []

    def testFailCallbackReCall(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc.resultCallback(False, 9)
        assert self.goods == []
        assert self.fails == [(False, 9)]
        rc.resultCallback(False, 8)
        assert self.goods == []
        assert self.fails == [(False, 9)]

    def testGoodCallbackReCallFail(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc.resultCallback(True, 5)
        assert self.goods == [(True, 5)]
        assert self.fails == []
        rc.resultCallback(False, 4)
        assert self.goods == [(True, 5)]
        assert self.fails == []

    def testFailCallbackReCallGood(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc.resultCallback(False, 9)
        assert self.goods == []
        assert self.fails == [(False, 9)]
        rc.resultCallback(True, 8)
        assert self.goods == []
        assert self.fails == [(False, 9)]

    def testManyGoodCallbacks(self):
        self.goods = []
        self.fails = []
        rc = [ResultCallback(self._good, self._fail) for N in range(20)]
        for num,each in enumerate(rc):
            each.resultCallback(True, num)
        assert self.goods == [(True, N) for N in range(20)]
        assert self.fails == []

    def testManyFailCallbacks(self):
        self.goods = []
        self.fails = []
        rc = [ResultCallback(self._good, self._fail) for N in range(20)]
        for num,each in enumerate(rc):
            each.resultCallback(False, num)
        assert self.goods == []
        assert self.fails == [(False, N) for N in range(20)]

    def testManyGoodAndFailCallbacks(self):
        self.goods = []
        self.fails = []
        rc = [ResultCallback(self._good, self._fail) for N in range(20)]
        for num,each in enumerate(rc):
            each.resultCallback(0 == num % 3, num)
        assert self.goods == [(True, N) for N in range(20) if N % 3 == 0]
        assert self.fails == [(False, N) for N in range(20) if N % 3]

    def testChainedGoodCallbacks(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc2 = ResultCallback(self._good, self._fail, rc)
        rc3 = ResultCallback(self._good, self._fail, rc2)
        rc3.resultCallback(True, 'good')
        assert self.goods == [(True, 'good')] * 3
        assert self.fails == []

    def testChainedFailCallbacks(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc2 = ResultCallback(self._good, self._fail, rc)
        rc3 = ResultCallback(self._good, self._fail, rc2)
        rc3.resultCallback(False, 'oops')
        assert self.goods == []
        assert self.fails == [(False, 'oops')] * 3

    def testChainedGoodCallbacksDoNotDuplicate(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc2 = ResultCallback(self._good, self._fail, rc)
        rc3 = ResultCallback(self._good, self._fail, rc2)
        rc2.resultCallback(True, 'ok')
        assert self.goods == [(True, 'ok'), (True, 'ok')]
        assert self.fails == []
        rc3.resultCallback(True, 'good')
        assert self.goods == [(True, 'ok'), (True, 'ok'), (True, 'good')]
        assert self.fails == []

    def testChainedFailCallbacksDoNotDuplicate(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc2 = ResultCallback(self._good, self._fail, rc)
        rc3 = ResultCallback(self._good, self._fail, rc2)
        rc2.resultCallback(False, 'bad')
        assert self.goods == []
        assert self.fails == [(False, 'bad'), (False, 'bad')]
        rc3.resultCallback(False, 'oops')
        assert self.goods == []
        assert self.fails == [(False, 'bad'), (False, 'bad'), (False, 'oops')]

    def testChainedGoodCallbacksDoNotDuplicateOnFail(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc2 = ResultCallback(self._good, self._fail, rc)
        rc3 = ResultCallback(self._good, self._fail, rc2)
        rc2.resultCallback(True, 'ok')
        assert self.goods == [(True, 'ok'), (True, 'ok')]
        assert self.fails == []
        rc3.resultCallback(False, 'bad')
        assert self.goods == [(True, 'ok'), (True, 'ok')]
        assert self.fails == [(False, 'bad')]

    def testChainedFailCallbacksDoNotDuplicateOnGood(self):
        self.goods = []
        self.fails = []
        rc = ResultCallback(self._good, self._fail)
        rc2 = ResultCallback(self._good, self._fail, rc)
        rc3 = ResultCallback(self._good, self._fail, rc2)
        rc2.resultCallback(False, 'bad')
        assert self.goods == []
        assert self.fails == [(False, 'bad'), (False, 'bad')]
        rc3.resultCallback(True, 'yippee')
        assert self.goods == [(True, 'yippee')]
        assert self.fails == [(False, 'bad'), (False, 'bad')]

