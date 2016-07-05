from thespian.actors import ActorExitRequest, ActorSystemMessage


class TestUnitExitRequestMsg(object):

    def test_equality(self):
        m1 = ActorExitRequest()
        assert m1 == ActorExitRequest()
        assert m1 == ActorExitRequest(True)
        assert m1 == ActorExitRequest(False)

    def test_inequality(self):
        m1 = ActorExitRequest()
        assert m1 !=  True
        assert m1 != None
        assert m1 != 0

    def test_properties(self):
        assert ActorExitRequest().isRecursive
        assert ActorExitRequest(True).isRecursive
        assert not ActorExitRequest(False).isRecursive

        m1 = ActorExitRequest()
        assert m1.isRecursive
        m1.notRecursive()
        assert not m1.isRecursive

    def test_inheritance(self):
        assert isinstance(ActorExitRequest(False), ActorExitRequest)
        assert isinstance(ActorExitRequest(), ActorExitRequest)
        assert isinstance(ActorExitRequest(False), ActorSystemMessage)
        assert isinstance(ActorExitRequest(), ActorSystemMessage)

