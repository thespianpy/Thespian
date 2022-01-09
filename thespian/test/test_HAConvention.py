from thespian.test import *
from time import sleep
import pytest
from thespian.actors import *
from datetime import timedelta


class PreRegActor(ActorTypeDispatcher):
    def receiveMsg_str(self, regaddr, sender):
        self.preRegisterRemoteSystem(regaddr, {})
        self.send(sender, 'Registered')


@pytest.fixture(params=['simpleSystemBase',
                        'multiprocQueueBase',
                        'multiprocUDPBase',
                        'multiprocTCPBase',
                        'multiprocTCPBase-AdminRouting',
                        'multiprocTCPBase-AdminRoutingTXOnly',
])
def testsystems(request):
    sysbase = request.param.partition('-')[0]
    adminRouting = request.param.endswith('-AdminRouting')
    txOnly = request.param.endswith('-AdminRoutingTXOnly')
    victoria_port = get_free_admin_port()
    leicester_port = get_free_admin_port()
    picadilly_port = get_free_admin_port()
    tottenham_port = get_free_admin_port()
    convaddrs = [ 'localhost:%d' % victoria_port,
                  'localhost:%d' % leicester_port,
                  'localhost:%d' % picadilly_port,
                  # tottenham cannot be a leader
                 ]
    basecaps = { 'Convention Address.IPv4': convaddrs,
                 'Admin Routing': adminRouting,
                }
    victoria_caps = basecaps.copy()
    victoria_caps.update({ 'Cyan': 19,
                           'Yellow': 11,
                           'Green': 11,
                           'Admin Port': victoria_port,
                          })
    leicester_caps = basecaps.copy()
    leicester_caps.update({ 'Blue': 4,
                            'Black': 8,
                            'Admin Port': leicester_port,
                           })
    picadilly_caps = basecaps.copy()
    picadilly_caps.update({ 'Blue': 6,
                            'Brown': 12,
                            'Admin Port': picadilly_port,
                           })
    tottenham_caps = basecaps.copy()
    tottenham_caps.update({ 'Brown': 7, 'Red': 10,
                            'Admin Port': tottenham_port,
                           })
    victoria = ActorSystem(systemBase=sysbase,
                           transientUnique=True,
                           logDefs=simpleActorTestLogging(),
                           capabilities=victoria_caps)
    victoria.base_name = request.param
    victoria.port_num = victoria_port
    leicester = ActorSystem(systemBase=sysbase,
                            transientUnique=True,
                            logDefs=simpleActorTestLogging(),
                            capabilities=leicester_caps)
    leicester.base_name = request.param
    leicester.port_num = leicester_port
    picadilly = ActorSystem(systemBase=sysbase,
                            transientUnique=True,
                            logDefs=simpleActorTestLogging(),
                            capabilities=picadilly_caps)
    picadilly.base_name = request.param
    picadilly.port_num = picadilly_port
    tottenham = ActorSystem(systemBase=sysbase,
                            transientUnique=True,
                            logDefs=simpleActorTestLogging(),
                            capabilities=tottenham_caps)
    tottenham.base_name = request.param
    tottenham.port_num = tottenham_port
    request.addfinalizer(lambda victoria=victoria, leicester=leicester,
                         picadilly=picadilly, tottenham=tottenham:
                         tottenham.shutdown() or
                         leicester.shutdown() or
                         picadilly.shutdown() or
                         victoria.shutdown())
    if txOnly:
        assert 'Registered' == victoria.ask(victoria.createActor(PreRegActor),
                                            'localhost:%d'%victoria.port_num,
                                            timedelta(seconds=3))
        assert 'Registered' == leicester.ask(leicester.createActor(PreRegActor),
                                             'localhost:%d'%leicester.port_num,
                                             timedelta(seconds=3))
        assert 'Registered' == picadilly.ask(picadilly.createActor(PreRegActor),
                                             'localhost:%d'%picadilly.port_num,
                                             timedelta(seconds=3))
        assert 'Registered' == tottenham.ask(tottenham.createActor(PreRegActor),
                                             'localhost:%d'%tottenham.port_num,
                                             timedelta(seconds=3))
    sleep(1.25)  # allow all systems to join the Convention
    return convaddrs, victoria, leicester, picadilly, tottenham


class Sean(Actor):
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, actorRequirements):
        return (capabilities.get('Blue', 0) +
                capabilities.get('Green', 0)) > 3;
    def receiveMessage(self, message, sender):
        if isinstance(message, str):
            self.send(sender, '%s is not enough' % message)

class Roger(Actor):
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, actorRequirements):
        return capabilities.get('Cyan', 0) > 0
    def receiveMessage(self, message, sender):
        if isinstance(message, str):
            self.send(sender, "Don't count on it, %s" % message)

class M(Actor):
    @staticmethod
    def actorSystemCapabilityCheck(capabilities, actorRequirements):
        return capabilities.get('Red', 0) > 0
    def receiveMessage(self, message, sender):
        if isinstance(message, str):
            if message == 'Sean':
                self.send(sender, self.createActor(Sean))
            if message == 'Roger':
                self.send(sender, self.createActor(Roger))


class TestFuncHAConvention():

    def test01_systems_can_start(self, testsystems):
        convaddrs, victoria, leicester, picadilly, tottenham = testsystems
        actor_system_unsupported(victoria,
                                 'simpleSystemBase', 'multiprocQueueBase')
        pass

    def test02_actors_can_start(self, testsystems):
        convaddrs, victoria, leicester, picadilly, tottenham = testsystems
        actor_system_unsupported(victoria,
                                 'simpleSystemBase', 'multiprocQueueBase')
        sean = victoria.createActor(Sean)
        roger = victoria.createActor(Roger)
        m = picadilly.createActor(M)
        sleep(1)  # wait for things to settle

        r = victoria.ask(sean, "diamonds", 0.25)
        assert r == "diamonds is not enough"
        r = victoria.ask(roger, "zorin", 0.25)
        assert r == "Don't count on it, zorin"

        bond1 = leicester.ask(m, "Sean", 0.25)
        assert bond1
        r = leicester.ask(bond1, "forever", 0.25)
        assert r == "forever is not enough"

        bond2 = leicester.ask(m, "Roger", 0.25)
        assert bond2
        r = leicester.ask(bond2, "jaws", 0.25)
        assert r == "Don't count on it, jaws"

    def test03_actor_create_failure_on_leader_exit(self, testsystems):
        convaddrs, victoria, leicester, picadilly, tottenham = testsystems
        actor_system_unsupported(victoria,
                                 'simpleSystemBase', 'multiprocQueueBase')
        sean = victoria.createActor(Sean)
        roger = victoria.createActor(Roger)
        m = picadilly.createActor(M)
        sleep(1)  # wait for things to settle

        bond1 = leicester.ask(m, "Sean", 0.25)
        assert bond1
        r = leicester.ask(bond1, "forever", 0.25)
        assert r == "forever is not enough"

        bond2 = leicester.ask(m, "Roger", 0.25)
        assert bond2
        r = leicester.ask(bond2, "jaws", 0.25)
        assert r == "Don't count on it, jaws"

        victoria.shutdown()
        sleep(2)

        bond3 = leicester.ask(m, "Sean", 0.25)
        assert bond3
        r = leicester.ask(bond3, "forever", 0.25)
        assert r == "forever is not enough"

        bond4 = leicester.ask(m, "Roger", 0.25)
        assert (bond4 is None)

    def test04_actor_create_on_leader_re_enter(self, testsystems):
        convaddrs, victoria, leicester, picadilly, tottenham = testsystems
        actor_system_unsupported(victoria,
                                 'simpleSystemBase', 'multiprocQueueBase')
        sean = victoria.createActor(Sean)
        roger = victoria.createActor(Roger)
        m = picadilly.createActor(M)
        sleep(1)  # wait for things to settle

        bond1 = leicester.ask(m, "Sean", 0.25)
        assert bond1
        r = leicester.ask(bond1, "forever", 0.25)
        assert r == "forever is not enough"

        bond2 = leicester.ask(m, "Roger", 0.25)
        assert bond2
        r = leicester.ask(bond2, "jaws", 0.25)
        assert r == "Don't count on it, jaws"

        victoria.shutdown()
        sleep(2)

        bond3 = leicester.ask(m, "Sean", 0.25)
        assert bond3
        r = leicester.ask(bond3, "forever", 0.25)
        assert r == "forever is not enough"

        bond4 = leicester.ask(m, "Roger", 0.25)
        assert (bond4 is None)

        # --- same as test03 up to this point ---

        victoria2 = ActorSystem(systemBase=victoria.base_name.partition('-')[0],
                                transientUnique=True,
                                logDefs=simpleActorTestLogging(),
                                capabilities={ 'Cyan': 12,
                                               'Admin Port': victoria.port_num,
                                               'Convention Address.IPv4': convaddrs
                                              })
        victoria2.base_name = victoria.base_name
        victoria2.port_num = victoria.port_num
        sleep(2) # wait for victoria to become

        try:

            bond5 = leicester.ask(m, "Sean", 0.25)
            assert bond5
            r = leicester.ask(bond5, "money", 0.25)
            assert r == "money is not enough"

            bond6 = leicester.ask(m, "Roger", 0.25)
            assert bond6
            r = leicester.ask(bond6, "sharks", 0.25)
            assert r == "Don't count on it, sharks"

        finally:
            victoria2.shutdown()
