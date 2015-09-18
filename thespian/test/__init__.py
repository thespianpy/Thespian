"""Defines various classes and definitions that provide assistance for
unit testing Actors in an ActorSystem."""

import unittest
import logging
from thespian.actors import ActorSystem


class LocallyManagedActorSystem(object):

    # n.b. initialize logging here instead of globally to get the
    # version of sys.stdout established by python unittest
    # framework on startup.
    @staticmethod
    def getDefaultTestLogging():
        import sys
        return {
            'version' : 1,
            'handlers': { #'discarder': {'class': 'logging.NullHandler' },
                'testStream' : { 'class': 'logging.StreamHandler',
                                 'stream': sys.stdout,
                             },
            },
            'root': { 'handlers': ['testStream'] },
            'disable_existing_loggers': False,
        }


    def setSystemBase(self, newBase='simpleSystemBase', systemCapabilities=None, logDefs=None):
        newBaseStr = str(newBase)
        if not hasattr(self, 'currentBase') or self.currentBase != newBaseStr:
            # In case the ActorSystem was *already* setup, break the singleton aspect and re-init
            ActorSystem().shutdown()
            if logDefs is None: logDefs = ActorSystemTestCase.getDefaultTestLogging()
            ActorSystem(newBase, systemCapabilities, logDefs)
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

