from thespian.actors import requireCapability


class TestUnitRequireCapability(object):

    @requireCapability('asdf')
    class req1: pass


    def test_oneReq(self):
        capcheck = TestUnitRequireCapability.req1.actorSystemCapabilityCheck
        assert not capcheck({}, 0)
        assert not capcheck({'asdf':False}, 0)
        assert capcheck({'asdf':True}, 0)
        assert capcheck({'asdf':True,'qwer':False}, 0)
        assert capcheck({'asdf':True,'qwer':True}, 0)
        assert capcheck({'qwer':False,'asdf':True}, 0)
        assert capcheck({'qwer':True,'asdf':True}, 0)
        assert not capcheck({'qwer':False,'asdf':False}, 0)
        assert not capcheck({'qwer':True,'asdf':False}, 0)


    @requireCapability('asdf')
    @requireCapability('qwer')
    class req2: pass
    def test_twoReq(self):
        capcheck = TestUnitRequireCapability.req2.actorSystemCapabilityCheck
        assert not capcheck({}, 0)
        assert not capcheck({'asdf':False}, 0)
        assert not capcheck({'asdf':True}, 0)
        assert not capcheck({'asdf':True,'qwer':False}, 0)
        assert capcheck({'asdf':True,'qwer':True}, 0)
        assert not capcheck({'qwer':False,'asdf':True}, 0)
        assert capcheck({'qwer':True,'asdf':True}, 0)
        assert not capcheck({'qwer':False,'asdf':False}, 0)
        assert not capcheck({'qwer':True,'asdf':False}, 0)


    @requireCapability('qwer')
    @requireCapability('asdf')
    class req2rev: pass
    def test_twoReqReverse(self):
        capcheck = TestUnitRequireCapability.req2rev.actorSystemCapabilityCheck
        assert not capcheck({}, 0)
        assert not capcheck({'asdf':False}, 0)
        assert not capcheck({'asdf':True}, 0)
        assert not capcheck({'asdf':True,'qwer':False}, 0)
        assert capcheck({'asdf':True,'qwer':True}, 0)
        assert not capcheck({'qwer':False,'asdf':True}, 0)
        assert capcheck({'qwer':True,'asdf':True}, 0)
        assert not capcheck({'qwer':False,'asdf':False}, 0)
        assert not capcheck({'qwer':True,'asdf':False}, 0)


    @requireCapability('frog', 'ribbet')
    class req3rev: pass
    def test_threeReq(self):
        check3 = TestUnitRequireCapability.req3rev.actorSystemCapabilityCheck
        assert check3({'frog':'ribbet'}, 0)
        assert not check3({'frog':'moo'}, 0)
        assert not check3({'frog':True}, 0)
        assert not check3({'frog':False}, 0)
        assert not check3({'frog':1}, 0)
        assert not check3({'frog':0}, 0)
        assert not check3({'frog':None}, 0)
        assert not check3({'Frog':'ribbet'}, 0)


class TestUnitRequireRequirements(object):

    class req1:
        @staticmethod
        def actorSystemCapabilityCheck(cap, req):
            return req.get('foo', 'bar') == 'woof'

    def test_ActorReqs(self):
        reqCheck = TestUnitRequireRequirements.req1.actorSystemCapabilityCheck
        assert not reqCheck({}, {})
        assert not reqCheck({}, {'foo':None})
        assert not reqCheck({}, {'foo':True})
        assert not reqCheck({}, {'foo':'boo'})
        assert not reqCheck({}, {'dog':'woof'})
        assert reqCheck({}, {'foo':'woof'})
        assert reqCheck({}, {'foo':'woof', 'bar':'foo'})
