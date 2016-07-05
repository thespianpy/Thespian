"""Verify common capability specifications.

Ensure that each ActorSystem provides the set of expected capabilities.
"""

from datetime import datetime, timedelta
import time
from thespian.actors import *
from thespian.test import *
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


class TestFuncCommonCapabilities(object):

    def test_valid_capabilities(self, asys):
        checkActor = asys.createActor(CapabilityCheck)
        asys.tell(checkActor, 'hello')
        assert True

