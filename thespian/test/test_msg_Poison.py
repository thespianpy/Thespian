from thespian.actors import PoisonMessage, ActorSystemMessage


class TestUnitPoisonMsg(object):

    def test_equality(self):
        m1 = PoisonMessage(1)
        assert m1 == PoisonMessage(1)
        m2 = PoisonMessage('hi')
        assert m2 == PoisonMessage('hi')

    def test_inequality(self):
        m1 = PoisonMessage(1)
        assert m1 != 1
        assert m1 != PoisonMessage('hi')
        assert m1 != PoisonMessage(0)
        assert m1 != PoisonMessage(None)

    def test_properties(self):
        assert 1 == PoisonMessage(1).poisonMessage
        assert PoisonMessage(None).poisonMessage is None
        assert 'foo' == PoisonMessage('foo').poisonMessage

    def test_inheritance(self):
        assert isinstance(PoisonMessage(1), PoisonMessage)
        assert isinstance(PoisonMessage('hi'), PoisonMessage)
        assert isinstance(PoisonMessage(1), ActorSystemMessage)
        assert isinstance(PoisonMessage('hi'), ActorSystemMessage)

