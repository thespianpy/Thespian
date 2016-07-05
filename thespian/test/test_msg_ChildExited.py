from thespian.actors import ChildActorExited, ActorSystemMessage


class TestUnitChildExitedMsg(object):

    def test_equality(self):
        m1 = ChildActorExited(1)
        assert m1 == ChildActorExited(1)

    def test_inequality(self):
        m1 = ChildActorExited(1)
        assert m1 != 1
        assert m1 != ChildActorExited('one')
        assert m1 != ChildActorExited(0)
        assert m1 != ChildActorExited(None)

    def test_properties(self):
        assert 1 == ChildActorExited(1).childAddress
        assert ChildActorExited(None).childAddress is None
        assert 'foo' == ChildActorExited('foo').childAddress

    def test_inheritance(self):
        assert isinstance(ChildActorExited(1), ChildActorExited)
        assert isinstance(ChildActorExited('one'), ChildActorExited)
        assert isinstance(ChildActorExited(1), ActorSystemMessage)
        assert isinstance(ChildActorExited('one'), ActorSystemMessage)

