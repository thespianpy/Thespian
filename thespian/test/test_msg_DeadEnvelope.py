from thespian.actors import DeadEnvelope, ActorSystemMessage


class TestUnitDeadEnvelopeMsg(object):

    def test_equality(self):
        m1 = DeadEnvelope(1, 2)
        assert m1 == DeadEnvelope(1, 2)
        m2 = DeadEnvelope('hi', False)
        assert m2 == DeadEnvelope('hi', False)

    def test_inequality(self):
        m1 = DeadEnvelope(1, 2)
        assert m1 != 1
        assert m1 != DeadEnvelope('hi', True)
        assert m1 != DeadEnvelope(0, 2)
        assert m1 != DeadEnvelope(1, 3)
        assert m1 != DeadEnvelope(None, None)

    def test_properties(self):
        assert 1 == DeadEnvelope(1, 2).deadAddress
        assert 2 == DeadEnvelope(1, 2).deadMessage

        assert DeadEnvelope(None, '').deadAddress is None
        assert '' == DeadEnvelope(None, '').deadMessage

        assert 'foo' == DeadEnvelope('foo', 3.3).deadAddress
        assert 3.3 == DeadEnvelope('foo', 3.3).deadMessage

    def test_inheritance(self):
        assert isinstance(DeadEnvelope(1, 2), DeadEnvelope)
        assert isinstance(DeadEnvelope('one', None), DeadEnvelope)
        assert isinstance(DeadEnvelope(1, 2), ActorSystemMessage)
        assert isinstance(DeadEnvelope('one', None), ActorSystemMessage)
