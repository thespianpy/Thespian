from thespian.actors import ValidateSource, ActorSystemMessage

class TestUnitValidateSourceMsg(object):

    def test_equality(self):
        m1 = ValidateSource(1, 2)
        assert m1 == ValidateSource(1, 2)
        assert m1 == ValidateSource(1, 3)
        assert m1 == ValidateSource(1, 'ignored')
        assert m1 == ValidateSource(1, None)
        m2 = ValidateSource('hi', False)
        assert m2 == ValidateSource('hi', False)
        assert m2 == ValidateSource('hi', True)
        assert m2 == ValidateSource('hi', 1)
        assert m2 == ValidateSource('hi', 0)
        assert m2 == ValidateSource('hi', None)

    def test_inequality(self):
        m1 = ValidateSource(1, 2)
        assert m1 != 1
        assert m1 != ValidateSource('hi', True)
        assert m1 != ValidateSource(0, 2)
        assert m1 != ValidateSource(9, 3)
        assert m1 != ValidateSource(None, None)

    def test_properties(self):
        assert 1 == ValidateSource(1, 2).sourceHash
        assert 2 == ValidateSource(1, 2).sourceData

        assert ValidateSource(None, '').sourceHash is None
        assert '' == ValidateSource(None, '').sourceData

        assert 'foo' == ValidateSource('foo', 3.3).sourceHash
        assert 3.3 == ValidateSource('foo', 3.3).sourceData

    def test_inheritance(self):
        assert isinstance(ValidateSource(1, 2), ValidateSource)
        assert isinstance(ValidateSource('one', None), ValidateSource)
        assert isinstance(ValidateSource(1, 2), ActorSystemMessage)
        assert isinstance(ValidateSource('one', None), ActorSystemMessage)
