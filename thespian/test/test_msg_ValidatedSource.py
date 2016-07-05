from thespian.actors import ValidatedSource, ActorSystemMessage


class TestUnitValidatedSourceMsg(object):

    def test_equality(self):
        m1 = ValidatedSource(1, 2)
        assert m1 == ValidatedSource(1, 2)
        assert m1 == ValidatedSource(1, 4)
        assert m1 == ValidatedSource(1, 'nine')
        m2 = ValidatedSource('hi', False)
        assert m2 == ValidatedSource('hi', False)
        assert m2 == ValidatedSource('hi', True)
        assert m2 == ValidatedSource('hi', 'ignored')

    def test_inequality(self):
        m1 = ValidatedSource(1, 2)
        assert m1 != 1
        assert m1 != ValidatedSource('hi', True)
        assert m1 != ValidatedSource(0, 2)
        assert m1 != ValidatedSource(None, None)

    def test_properties(self):
        assert 1 == ValidatedSource(1, 2).sourceHash
        assert 2 == ValidatedSource(1, 2).sourceZip

        assert ValidatedSource(None, '').sourceHash is None
        assert '' == ValidatedSource(None, '').sourceZip

        assert 'foo' == ValidatedSource('foo', 3.3).sourceHash
        assert 3.3 == ValidatedSource('foo', 3.3).sourceZip

    def test_inheritance(self):
        assert isinstance(ValidatedSource(1, 2), ValidatedSource)
        assert isinstance(ValidatedSource('one', None), ValidatedSource)
        assert isinstance(ValidatedSource(1, 2), ActorSystemMessage)
        assert isinstance(ValidatedSource('one', None), ActorSystemMessage)
