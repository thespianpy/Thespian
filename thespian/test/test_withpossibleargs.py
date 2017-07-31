from thespian.system.utilis import withPossibleInitArgs

class NoArgs(object):
    def __init__(self):
        self.ready = True

class ReqArgs(object):
    def __init__(self, requirements):
        self.ready = True
        self.reqs = requirements

class PossibleReqArgs(object):
    def __init__(self, requirements=None):
        self.ready = True
        self.reqs = requirements

class CapArgs(object):
    def __init__(self, capabilities):
        self.ready = True
        self.caps = capabilities

class PossibleCapArgs(object):
    def __init__(self, capabilities=None):
        self.ready = True
        self.caps = capabilities

class CapReqArgs(object):
    def __init__(self, capabilities, requirements):
        self.ready = True
        self.reqs = requirements
        self.caps = capabilities

class CapPossibleReqArgs(object):
    def __init__(self, capabilities, requirements=None):
        self.ready = True
        self.reqs = requirements
        self.caps = capabilities

class PossibleCapPossibleReqArgs(object):
    def __init__(self, requirements=None, capabilities=None):
        self.ready = True
        self.reqs = requirements
        self.caps = capabilities

class CapFooArgs(object):
    def __init__(self, foo=None, capabilities=None):
        self.ready = True
        self.caps = capabilities

class ReqCapFooArgs(object):
    def __init__(self, requirements=None, foo=None, capabilities=None):
        self.ready = True
        self.reqs = requirements
        self.caps = capabilities

class ReqFooArgs(object):
    def __init__(self, requirements=None, foo=None):
        self.ready = True
        self.reqs = requirements

wpa = withPossibleInitArgs(capabilities={'caps':'here', 'capa':'bilities'},
                           requirements={'reqs':'requirements', 'r':True})

def test_noargs():
    obj = wpa.create(NoArgs)
    assert obj
    assert not hasattr(obj, 'caps')
    assert not hasattr(obj, 'reqs')

def test_reqargs():
    obj = wpa.create(ReqArgs)
    assert obj
    assert not hasattr(obj, 'caps')
    assert obj.reqs['r']
    assert obj.reqs['reqs'] == 'requirements'

def test_possiblereqargs():
    obj = wpa.create(PossibleReqArgs)
    assert obj
    assert not hasattr(obj, 'caps')
    assert obj.reqs['r']
    assert obj.reqs['reqs'] == 'requirements'

def test_reqfooargs():
    obj = wpa.create(ReqFooArgs)
    assert obj
    assert not hasattr(obj, 'caps')
    assert obj.reqs['r']
    assert obj.reqs['reqs'] == 'requirements'

def test_capargs():
    obj = wpa.create(CapArgs)
    assert obj
    assert not hasattr(obj, 'reqs')
    assert obj.caps['caps'] == 'here'
    assert obj.caps['capa'] == 'bilities'

def test_possiblecapargs():
    obj = wpa.create(PossibleCapArgs)
    assert obj
    assert not hasattr(obj, 'reqs')
    assert obj.caps['caps'] == 'here'
    assert obj.caps['capa'] == 'bilities'

def test_capfooargs():
    obj = wpa.create(CapFooArgs)
    assert obj
    assert not hasattr(obj, 'reqs')
    assert obj.caps['caps'] == 'here'
    assert obj.caps['capa'] == 'bilities'

def test_capreqargs():
    obj = wpa.create(CapReqArgs)
    assert obj
    assert obj.caps['caps'] == 'here'
    assert obj.caps['capa'] == 'bilities'
    assert obj.reqs['r']
    assert obj.reqs['reqs'] == 'requirements'

def test_cappossiblereqargs():
    obj = wpa.create(CapPossibleReqArgs)
    assert obj
    assert obj.caps['caps'] == 'here'
    assert obj.caps['capa'] == 'bilities'
    assert obj.reqs['r']
    assert obj.reqs['reqs'] == 'requirements'

def test_possiblecappossiblereqargs():
    obj = wpa.create(CapPossibleReqArgs)
    assert obj
    assert obj.caps['caps'] == 'here'
    assert obj.caps['capa'] == 'bilities'
    assert obj.reqs['r']
    assert obj.reqs['reqs'] == 'requirements'

def test_reqcapfooargs():
    obj = wpa.create(ReqCapFooArgs)
    assert obj
    assert obj.caps['caps'] == 'here'
    assert obj.caps['capa'] == 'bilities'
    assert obj.reqs['r']
    assert obj.reqs['reqs'] == 'requirements'
