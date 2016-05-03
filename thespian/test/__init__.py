"""Defines various classes and definitions that provide assistance for
unit testing Actors in an ActorSystem."""

import unittest
import logging
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

