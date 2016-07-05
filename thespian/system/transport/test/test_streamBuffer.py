from thespian.system.transport.streamBuffer import ReceiveBuffer, toSendBuffer


def fibonacci(limit=10):
    p = 1
    f = 1
    while f < limit:
        yield f
        (f, p) = (f + p, f)


class TestUnitReceiveBufferReconstruct(object):

    sampleBuffer = 'The struggle of today, is not altogether for today -- it is for a vast future also. -- Abraham Lincoln'

    def finalTests(self, rcv, expectedBuf, descr, expectedExtra=None):
        assert rcv.isDone(), 'ReceiveBuffer isDone %s completed but isDone is False'
        assert rcv.remainingAmount() == 0, (
            'Checking if no remaining amount (%s) for %s'%(
                str(rcv.remainingAmount()),
                descr))
        completed,extra = rcv.completed()
        assert completed == expectedBuf, (
            'Checking if completed %s == %s for %s'%(str(completed),
                                                     str(expectedBuf),
                                                     descr))
        if expectedExtra:
            assert extra == expectedExtra, (
                'Checking if extra %s == %s for %s'%(str(extra),
                                                     str(expectedExtra),
                                                     descr))
        else:
            assert not extra, 'Verifying no extra in %s for %s'%(str(extra),
                                                                 descr)

    def partialTests(self, rcv, amount, totalAmount, descr):
        assert not rcv.isDone(), (
            'ReceiveBuffer isDone %s add %d of %d test is not False'
            % (descr, amount, totalAmount))
        # Before the size specification is received, remaining
        # amount may be arbitrary to cause receipt of the size
        # amount.
        remAmount = rcv.remainingAmount()
        if remAmount != 20:
            assert remAmount == totalAmount - amount, (
                'ReceiveBuffer remaining %s after %d of %d should be %d'
                ' but is %d'
                % (descr, amount, totalAmount,
                   totalAmount - amount, rcv.remainingAmount()))
        assert rcv.completed() is None, (
            'ReceiveBuffer completed %s after initial %d of %d should be None'
            'but is %s'
            % (descr, amount, totalAmount, rcv.completed()))

    def test_fullReceive(self):
        msg = toSendBuffer(self.sampleBuffer)
        msglen = len(msg)

        rcv = ReceiveBuffer()
        assert not rcv.isDone(), 'initial ReceiveBuffer isDone test'

        rcv.addMore(msg)

        self.finalTests(rcv, self.sampleBuffer, 'completion')

    def test_fullReceiveTinyMessage(self):
        msg = toSendBuffer('I')
        msglen = len(msg)

        rcv = ReceiveBuffer()
        assert not rcv.isDone(), 'initial ReceiveBuffer isDone test'

        rcv.addMore(msg)

        self.finalTests(rcv, 'I', 'completion')

    def test_fullReceiveHugeMessage(self):
        bigMessage = 'I' * 1024 * 1024 * 100   # 100MB
        msg = toSendBuffer(bigMessage)
        msglen = len(msg)

        rcv = ReceiveBuffer()
        assert not rcv.isDone(), 'initial ReceiveBuffer isDone test'

        rcv.addMore(msg)

        self.finalTests(rcv, bigMessage, 'completion')

    def test_singleBreakAtVariousPoints(self):
        msg = toSendBuffer(self.sampleBuffer)
        msglen = len(msg)
        for point in range(msglen):
            rcv = ReceiveBuffer()
            assert not rcv.isDone(), 'initial ReceiveBuffer isDone test'

            rcv.addMore(msg[:point])
            self.partialTests(rcv, point, msglen, 'sample message first add @ %s'%point)

            rcv.addMore(msg[point:])
            self.finalTests(rcv, self.sampleBuffer, 'sample message completion @ %s'%point)

    def test_singleBreakAtVariousPointsBigMessage(self):
        bigMessage = '0123456789' * 1024 * 1024 * 10   # 100MB
        msg = toSendBuffer(bigMessage)
        msglen = len(msg)
        for point in fibonacci(msglen):
            rcv = ReceiveBuffer()
            assert not rcv.isDone(), 'initial ReceiveBuffer isDone test'

            rcv.addMore(msg[:point])
            self.partialTests(rcv, point, msglen, 'big message first add @ %s'%point)

            rcv.addMore(msg[point:])
            self.finalTests(rcv, bigMessage, 'big message completion @ %s'%point)


    def test_multipleBreaksAtVariousSizes(self):
        # This test is important because it breaks at *every* size,
        # meaning that all of the prefix length and corresponding
        # elements are tested in various multiple segment pieces.
        msg = toSendBuffer(self.sampleBuffer)
        msglen = len(msg)
        for partLen in range(1, msglen):
            rcv = ReceiveBuffer()
            assert not rcv.isDone(), 'initial ReceiveBuffer isDone test'

            for partnum, point in enumerate(range(0, msglen, partLen)):
                rcv.addMore(msg[point:point+partLen])
                if point + partLen < msglen:
                    self.partialTests(rcv, point+partLen, msglen,
                                      'partial add #%d of %d' % (partnum, partLen))

            self.finalTests(rcv, self.sampleBuffer, 'completion')

    def test_multipleBreaksAtVariousBigBuffer(self):
        # This test is less specific than the
        # test_multipleBreaksAtVariousSizes because it only breaks the
        # middle of the buffer... it mostly just verifies large buffer
        # reconstruction.
        bigMessage = 'ABCDEfghij' * 1024 * 1024 * 10   # 100MB
        msg = toSendBuffer(bigMessage)
        msglen = len(msg)
        # Check last three fibonacci sizes only... don't have all day
        for partLen in [F for F in fibonacci(msglen)][-1:-3:-1]:
            if partLen < 5000: continue  # those sizes should already have been verified
            rcv = ReceiveBuffer()
            assert not rcv.isDone(), 'initial ReceiveBuffer isDone test'

            for partnum, point in enumerate(range(0, msglen, partLen)):
                rcv.addMore(msg[point:point+partLen])
                if point + partLen < msglen:
                    self.partialTests(rcv, point+partLen, msglen,
                                      'partial add #%d of %d' % (partnum, partLen))

            self.finalTests(rcv, bigMessage, 'completion')
