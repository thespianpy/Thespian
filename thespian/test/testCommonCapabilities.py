"""Verify common capability specifications.

Ensure that each ActorSystem provides the set of expected capabilities.
"""

import unittest
from datetime import datetime, timedelta
import time
import thespian.test.helpers
from thespian.actors import *
from thespian.test import ActorSystemTestCase
from thespian.system.utilis import thesplog



class CapabilityCheck(Actor):
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, requirements):
        import sys
        thesplog('Capabilities: %s', capabilities)
        assert capabilities['Python Version'] == sys.version_info
        assert capabilities['Thespian Generation'] == ThespianGeneration
        assert 'Thespian Version' in capabilities
        assert 'Thespian ActorSystem Name' in capabilities
        assert 'Thespian ActorSystem Version' in capabilities
        return capabilities['Python Version'] == sys.version_info and \
            capabilities['Thespian Generation'] == ThespianGeneration and \
            'Thespian Version' in capabilities and \
            'Thespian ActorSystem Name' in capabilities and \
            'Thespian ActorSystem Version' in capabilities
    def receiveMessage(self, msg, sender):
        pass


class TestASimpleSystem(ActorSystemTestCase):
    testbase='Simple'
    scope='func'

    def test_valid_capabilities(self):
        aS = ActorSystem()
        checkActor = aS.createActor(CapabilityCheck)
        aS.tell(checkActor, 'hello')
        self.assertTrue(True)


class TestMultiprocUDPSystem(TestASimpleSystem):
    testbase='MultiprocUDP'
    def setUp(self):
        self.setSystemBase('multiprocUDPBase')
        super(TestMultiprocUDPSystem, self).setUp()

class TestMultiprocTCPSystem(TestASimpleSystem):
    testbase='MultiprocTCP'
    def setUp(self):
        self.setSystemBase('multiprocTCPBase')
        super(TestMultiprocTCPSystem, self).setUp()

class TestMultiprocQueueSystem(TestASimpleSystem):
    testbase='MultiprocQueue'
    def setUp(self):
        self.setSystemBase('multiprocQueueBase')
        super(TestMultiprocQueueSystem, self).setUp()

