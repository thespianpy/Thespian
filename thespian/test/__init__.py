"""Defines various classes and definitions that provide assistance for
unit testing Actors in an ActorSystem."""

import unittest
import pytest
import logging
import time
from thespian.actors import ActorSystem


def simpleActorTestLogging():
    """This function returns a logging dictionary that can be passed as
       the logDefs argument for ActorSystem() initialization to get
       simple stdout logging configuration.  This is not necessary for
       typical unit testing that uses the simpleActorSystemBase, but
       it can be useful for multiproc.. ActorSystems where the
       separate processes created should have a very simple logging
       configuration.
    """
    import sys
    if sys.platform == 'win32':
        # Windows will not allow sys.stdout to be passed to a child
        # process, which breaks the startup/config for some of the
        # tests.
        handler = { 'class': 'logging.handlers.RotatingFileHandler',
                    'filename': 'nosetests.log',
                    'maxBytes': 256*1024,
                    'backupCount':3,
        }
    else:
        handler = { 'class': 'logging.StreamHandler',
                    'stream': sys.stdout,
        }
    return {
        'version' : 1,
        'handlers': { #'discarder': {'class': 'logging.NullHandler' },
            'testStream' : handler,
        },
        'root': { 'handlers': ['testStream'] },
        'disable_existing_loggers': False,
    }


class TestSystem(object):
    "Functions as a context manager for a transient system base"
    def __init__(self, newBase='simpleSystemBase',
                 systemCapabilities=None,
                 logDefs='BestForBase'):
            self._asys = ActorSystem(systemBase=newBase,
                                     capabilities=systemCapabilities,
                                     logDefs=logDefs if logDefs != 'BestForBase' else (
                                         simpleActorTestLogging() if newBase.startswith('multiproc')
                                         else False),
                                     transientUnique=True)

    def __enter__(self):
        return self._asys
    def __exit__(self, exc_type, exc_val, exc_tb):
        self._asys.shutdown()
        self._asys = None


class LocallyManagedActorSystem(object):

    def setSystemBase(self, newBase='simpleSystemBase', systemCapabilities=None, logDefs='BestForBase'):
        newBaseStr = str(newBase)
        if not hasattr(self, 'currentBase') or self.currentBase != newBaseStr:
            ldefs = logDefs if logDefs != 'BestForBase' else (simpleActorTestLogging() if newBase.startswith('multiproc') else False)
            # In case the ActorSystem was *already* setup, break the singleton aspect and re-init
            ActorSystem(logDefs = ldefs).shutdown()
            ActorSystem(newBase, systemCapabilities, logDefs = ldefs)
            self.currentBase = newBaseStr


class ActorSystemTestCase(unittest.TestCase, LocallyManagedActorSystem):

    """The ActorSystemTestCase is a wrapper for the unittest TestCase
       class that will startup a default ActorSystem in the provided
       setUp() and tearDown() any active ActorSystem after testing.

       If a non-default ActorSystem is to be used, the setSystemBase()
       method should be called with that system base.

       It also provides some additional methods for assistance in testing Actors.

    """
    def setUp(self):
        if not hasattr(self, 'currentBase'):
            self.setSystemBase()


    def tearDown(self):
        if hasattr(self, 'currentBase'):
            ActorSystem().shutdown()
            delattr(self, 'currentBase')
            import time
            time.sleep(0.02)


    @staticmethod
    def actualActorObject(actorClass):
        """Normally an Actor is only instantiated in the context of an
           ActorSystem, and then only responds to messages delivered
           via that system.  For testing purposes *only*, it may be
           desireable to have the actual Actor instance to test
           methods on that Actor directly.  This method will return
           that actual Actor instance after instantiating the actor in
           an ActorSystem.

           This method can ONLY be used with an ActorSystem that will
           instantiate the Actor in the context of the current process
           (e.g. simpleSystemBase) and the methods tested on the
           resulting Actor CANNOT perform any Actor-related actions
           (e.g. self.createActor(), self.send()).

           This method is for TESTING only under very special
           circumstances; if you're not sure you need this, then you
           probably don't.
        """
        # Create the Actor within the system.
        aAddr = ActorSystem().createActor(actorClass)
        # This depends on the internals of the systemBase
        return ActorSystem()._systemBase.actorRegistry[aAddr.actorAddressString].instance


###
### pytest fixtures and helpers
###

testAdminPort = None

def get_free_admin_port_random():
    global testAdminPort
    if testAdminPort is None:
        import random
        testAdminPort = random.randint(5,60) * 1000
    else:
        testAdminPort = testAdminPort + 1
    return testAdminPort

def get_free_admin_port():
    import socket
    import random
    for tries in range(100):
        port = random.randint(5000, 60000)
        try:
            socket.socket(socket.AF_INET,
                          socket.SOCK_STREAM,
                          socket.IPPROTO_TCP).bind(('',port))
            socket.socket(socket.AF_INET,
                          socket.SOCK_DGRAM,
                          socket.IPPROTO_UDP).bind(('',port))
            return port
        except Exception:
            pass
    return get_free_admin_port_random()


@pytest.fixture(params=['simpleSystemBase',
                        'multiprocQueueBase',
                        'multiprocUDPBase',
                        'multiprocTCPBase',
                        'multiprocTCPBase-AdminRouting',
                        'multiprocTCPBase-AdminRoutingTXOnly',
])
def asys(request):
    caps = {'Foo Allowed': True,
            'Cows Allowed': True,
            'Dogs Allowed': True,
            'dog': 'food'}
    if request.param.startswith('multiprocTCP') or \
       request.param.startswith('multiprocUDP'):
        caps['Admin Port'] = get_free_admin_port()
        caps['Convention Address.IPv4'] = '', caps['Admin Port']
    if request.param.endswith('-AdminRouting'):
        caps['Admin Routing'] = True
    if request.param.endswith('-AdminRoutingTXOnly'):
        caps['Admin Routing'] = True
        caps['Outbound Only'] = True
    asys = ActorSystem(systemBase=request.param.partition('-')[0],
                       capabilities=caps,
                       logDefs=(simpleActorTestLogging()
                                if request.param.startswith('multiproc')
                                else False),
                       transientUnique=True)
    asys.base_name = request.param
    asys.port_num  = caps.get('Admin Port', None)
    asys.txonly = request.param.endswith('-AdminRoutingTXOnly')
    request.addfinalizer(lambda asys=asys: asys.shutdown())
    return asys


def similar_asys(asys, in_convention=True, start_wait=True, capabilities=None):
    caps = capabilities or {}
    if asys.base_name.startswith('multiprocTCP') or \
       asys.base_name.startswith('multiprocUDP'):
        caps['Admin Port'] = get_free_admin_port()
        if in_convention:
            caps['Convention Address.IPv4'] = '', asys.port_num
    if asys.base_name.endswith('-AdminRouting'):
        caps['Admin Routing'] = True
    asys2 = ActorSystem(systemBase=asys.base_name.partition('-')[0],
                        capabilities=caps,
                        logDefs=(simpleActorTestLogging()
                                if asys.base_name.startswith('multiproc')
                                else False),
                       transientUnique=True)
    asys2.base_name = asys.base_name
    asys2.port_num  = caps.get('Admin Port', None)
    if in_convention and start_wait:
        time.sleep(0.25)  # Wait for Actor Systems to start and connect together
    return asys2


@pytest.fixture
def asys2(request, asys):
    asys2 = similar_asys(asys, in_convention=False)
    # n.b. shutdown the second actor system first:
    #   1. Some tests ask asys1 to create an actor
    #   2. That actor is actually supported by asys2
    #   3. There is an external port the tester uses for each asys
    #   4. When asys2 is shutdown, it will attempt to notify the
    #      parent of the actor that the actor is dead
    #   5. This parent is the external port for asys1.
    #   6. If asys1 is shutdown first, then asys2 must time out
    #      on the transmit attempt (usually 5 minutes) before
    #      it can exit.
    #   7. If the test is re-run within this 5 minute period, it will fail
    #      because the old asys2 is still existing but in shutdown state
    #      (and will therefore rightfully refuse new actions).
    # By shutting down asys2 first, the parent notification can be
    # performed and subsequent runs don't encounter the lingering
    # asys2.
    request.addfinalizer(lambda asys=asys2: asys2.shutdown())
    return asys2


@pytest.fixture
def asys_pair(request, asys):
    asys2 = similar_asys(asys, in_convention=True)
    # n.b. shutdown the second actor system first:
    #   1. Some tests ask asys1 to create an actor
    #   2. That actor is actually supported by asys2
    #   3. There is an external port the tester uses for each asys
    #   4. When asys2 is shutdown, it will attempt to notify the
    #      parent of the actor that the actor is dead
    #   5. This parent is the external port for asys1.
    #   6. If asys1 is shutdown first, then asys2 must time out
    #      on the transmit attempt (usually 5 minutes) before
    #      it can exit.
    #   7. If the test is re-run within this 5 minute period, it will fail
    #      because the old asys2 is still existing but in shutdown state
    #      (and will therefore rightfully refuse new actions).
    # By shutting down asys2 first, the parent notification can be
    # performed and subsequent runs don't encounter the lingering
    # asys2.
    request.addfinalizer(lambda asys=asys2: asys2.shutdown())
    return (asys, asys2)


def unstable_test(asys, *unstable_bases):
    if asys.base_name in unstable_bases and \
       not pytest.config.getoption('unstable', default=False):
        pytest.skip("Test unstable for %s system base"%asys.base_name)


def actor_system_unsupported(asys, *unsupported_bases):
    if asys.base_name in unsupported_bases:
        pytest.skip("Functionality not supported for %s system base"%asys.base_name)


from thespian.system.timing import timePeriodSeconds
import time

inTestDelay = lambda period: time.sleep(timePeriodSeconds(period))

