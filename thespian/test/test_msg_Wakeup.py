from thespian.actors import WakeupMessage, ActorSystemMessage


class TestUnitExitRequestMsg(object):

    def test_equality(self):
        m1 = WakeupMessage(1)
        assert m1 == WakeupMessage(1)
        m2 = WakeupMessage('hi')
        assert m2 == WakeupMessage('hi')

    def test_inequality(self):
        m1 = WakeupMessage(1)
        assert m1 != 1
        assert m1 != WakeupMessage('hi')
        assert m1 != WakeupMessage(0)
        assert m1 != WakeupMessage(None)

    def test_properties(self):
        assert 1 == WakeupMessage(1).delayPeriod
        assert WakeupMessage(None).delayPeriod is None
        assert 'foo' == WakeupMessage('foo').delayPeriod

    def test_inheritance(self):
        assert isinstance(WakeupMessage(1), WakeupMessage)
        assert isinstance(WakeupMessage('one'), WakeupMessage)
        assert isinstance(WakeupMessage(1), ActorSystemMessage)
        assert isinstance(WakeupMessage('one'), ActorSystemMessage)
