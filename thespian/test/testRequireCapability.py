from unittest import TestCase
from thespian.actors import requireCapability

class TestRequireCapability(TestCase):
    scope="unit"
    @requireCapability('asdf')
    class req1: pass
    def test_oneReq(self):
        self.assertFalse(TestRequireCapability.req1.actorSystemCapabilityCheck({}, 0))
        self.assertFalse(TestRequireCapability.req1.actorSystemCapabilityCheck({'asdf':False}, 0))
        self.assertTrue(TestRequireCapability.req1.actorSystemCapabilityCheck({'asdf':True}, 0))
        self.assertTrue(TestRequireCapability.req1.actorSystemCapabilityCheck({'asdf':True,'qwer':False}, 0))
        self.assertTrue(TestRequireCapability.req1.actorSystemCapabilityCheck({'asdf':True,'qwer':True}, 0))
        self.assertTrue(TestRequireCapability.req1.actorSystemCapabilityCheck({'qwer':False,'asdf':True}, 0))
        self.assertTrue(TestRequireCapability.req1.actorSystemCapabilityCheck({'qwer':True,'asdf':True}, 0))
        self.assertFalse(TestRequireCapability.req1.actorSystemCapabilityCheck({'qwer':False,'asdf':False}, 0))
        self.assertFalse(TestRequireCapability.req1.actorSystemCapabilityCheck({'qwer':True,'asdf':False}, 0))
    @requireCapability('asdf')
    @requireCapability('qwer')
    class req2: pass
    def test_twoReq(self):
        self.assertFalse(TestRequireCapability.req2.actorSystemCapabilityCheck({}, 0))
        self.assertFalse(TestRequireCapability.req2.actorSystemCapabilityCheck({'asdf':False}, 0))
        self.assertFalse(TestRequireCapability.req2.actorSystemCapabilityCheck({'asdf':True}, 0))
        self.assertFalse(TestRequireCapability.req2.actorSystemCapabilityCheck({'asdf':True,'qwer':False}, 0))
        self.assertTrue(TestRequireCapability.req2.actorSystemCapabilityCheck({'asdf':True,'qwer':True}, 0))
        self.assertFalse(TestRequireCapability.req2.actorSystemCapabilityCheck({'qwer':False,'asdf':True}, 0))
        self.assertTrue(TestRequireCapability.req2.actorSystemCapabilityCheck({'qwer':True,'asdf':True}, 0))
        self.assertFalse(TestRequireCapability.req2.actorSystemCapabilityCheck({'qwer':False,'asdf':False}, 0))
        self.assertFalse(TestRequireCapability.req2.actorSystemCapabilityCheck({'qwer':True,'asdf':False}, 0))
    @requireCapability('qwer')
    @requireCapability('asdf')
    class req2rev: pass
    def test_twoReqReverse(self):
        self.assertFalse(TestRequireCapability.req2rev.actorSystemCapabilityCheck({}, 0))
        self.assertFalse(TestRequireCapability.req2rev.actorSystemCapabilityCheck({'asdf':False}, 0))
        self.assertFalse(TestRequireCapability.req2rev.actorSystemCapabilityCheck({'asdf':True}, 0))
        self.assertFalse(TestRequireCapability.req2rev.actorSystemCapabilityCheck({'asdf':True,'qwer':False}, 0))
        self.assertTrue(TestRequireCapability.req2rev.actorSystemCapabilityCheck({'asdf':True,'qwer':True}, 0))
        self.assertFalse(TestRequireCapability.req2rev.actorSystemCapabilityCheck({'qwer':False,'asdf':True}, 0))
        self.assertTrue(TestRequireCapability.req2rev.actorSystemCapabilityCheck({'qwer':True,'asdf':True}, 0))
        self.assertFalse(TestRequireCapability.req2rev.actorSystemCapabilityCheck({'qwer':False,'asdf':False}, 0))
        self.assertFalse(TestRequireCapability.req2rev.actorSystemCapabilityCheck({'qwer':True,'asdf':False}, 0))


class TestRequireRequirements(TestCase):
    scope="unit"

    class req1:
        @staticmethod
        def actorSystemCapabilityCheck(cap, req):
            return req.get('foo', 'bar') == 'woof'

    def test_ActorReqs(self):
        reqCheck = TestRequireRequirements.req1.actorSystemCapabilityCheck
        self.assertFalse(reqCheck({}, {}))
        self.assertFalse(reqCheck({}, {'foo':None}))
        self.assertFalse(reqCheck({}, {'foo':True}))
        self.assertFalse(reqCheck({}, {'foo':'boo'}))
        self.assertFalse(reqCheck({}, {'dog':'woof'}))
        self.assertTrue(reqCheck({}, {'foo':'woof'}))
        self.assertTrue(reqCheck({}, {'foo':'woof', 'bar':'foo'}))
