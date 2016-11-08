from thespian.actors import ActorAddress, ActorSystemConventionUpdate
from thespian.system.messages.convention import (ConventionRegister,
                                                 ConventionDeRegister)
from thespian.system.admin.convention import (LocalConventionState, LostRemote,
                                              HysteresisSend, HysteresisCancel,
                                              CONVENTION_REREGISTRATION_PERIOD,
                                              CONVENTION_REGISTRATION_MISS_MAX)
from thespian.system.utilis import fmap, StatsManager
from thespian.system.logdirector import LogAggregator
try:
    from unittest.mock import patch
except ImportError:
    try:
        from mock import patch
    except ImportError:
        patch = None
from datetime import datetime, timedelta
from pytest import fixture, mark


@fixture
def lcs1():
    ret = LocalConventionState(ActorAddress(1),
                                {'Admin Port': 1,
                                 'Convention Address.IPv4': ActorAddress(1),
                                 'popsicle': 'cold'},
                                StatsManager(),
                               lambda x: ActorAddress(1))
    # Activate the system
    verify_io(ret.setup_convention(activation=True), [])
    return ret


@fixture
def lcs2():
    ret = LocalConventionState(ActorAddress(2),
                                {'Admin Port': 2,
                                 'Convention Address.IPv4': ActorAddress(1),
                                 'apple pie': 'hot'},
                                StatsManager(),
                               lambda x: ActorAddress(1))
    ret._expected_setup_convreg = convreg2_first(ret)
    # Activate the system
    verify_io(ret.setup_convention(activation=True),
              [ (ConventionRegister, Sends(ret._expected_setup_convreg) >= ActorAddress(1)),
                (LogAggregator, None),
              ])
    # KWQ: above is a HysteresisSend
    return ret


@fixture
def solo_lcs1():
    # Like lcs1, but does not specify a convention address; intended
    # for use with pre-registration (e.g. to simulate TXOnly
    # environments.
    ret = LocalConventionState(ActorAddress(1),
                                {'Admin Port': 1, 'popsicle': 'cold'},
                                StatsManager(),
                               lambda x: None)
    # Activate the system
    assert [] == ret.setup_convention()
    return ret


@fixture
def solo_lcs2():
    # Like lcs2, but does not specify a convention address; intended
    # for use with pre-registration (e.g. to simulate TXOnly
    # environments.
    ret = LocalConventionState(ActorAddress(2),
                               {'Admin Port': 2, 'apple pie': 'hot'},
                               StatsManager(),
                               lambda x: None)
    # Activate the system
    assert [] == ret.setup_convention()
    return ret


@fixture
def convreg1(lcs1):
    return ConventionRegister(lcs1.myAddress,
                              lcs1.capabilities,
                              firstTime=False,
                              preRegister=False)

@fixture
def convreg1_first(lcs1):
    return ConventionRegister(lcs1.myAddress,
                              lcs1.capabilities,
                              firstTime=True,
                              preRegister=False)

@fixture
def convreg1_noadmin(lcs1):
    return ConventionRegister(lcs1.myAddress,
                              dict([(K,lcs1.capabilities[K])
                                    for K in lcs1.capabilities
                                    if K != 'Convention Address.IPv4']),
                              firstTime=False,
                              preRegister=False)

@fixture
def convreg1_first_noadmin(lcs1):
    return ConventionRegister(lcs1.myAddress,
                              dict([(K,lcs1.capabilities[K])
                                    for K in lcs1.capabilities
                                    if K != 'Convention Address.IPv4']),
                              firstTime=True,
                              preRegister=False)

@fixture
def convreg2(lcs2):
    return ConventionRegister(lcs2.myAddress,
                              lcs2.capabilities,
                              firstTime=False,
                              preRegister=False)

@fixture
def convreg2_prereg(lcs2):
    return ConventionRegister(lcs2.myAddress,
                              {'Admin Port': lcs2.capabilities['Admin Port']},
                              firstTime=False,
                              preRegister=True)

@fixture
def convreg2_first(lcs2):
    return ConventionRegister(lcs2.myAddress,
                              lcs2.capabilities,
                              firstTime=True,
                              preRegister=False)

@fixture
def convreg2_noadmin(lcs2):
    return ConventionRegister(lcs2.myAddress,
                              dict([(K,lcs2.capabilities[K])
                                    for K in lcs2.capabilities
                                    if K != 'Convention Address.IPv4']),
                              firstTime=False,
                              preRegister=False)


@fixture
def convdereg_lcs2(lcs2):
    return ConventionDeRegister(lcs2.myAddress, preRegistered=False)


@fixture
def conv1_notifyAddr(lcs1):
    notifyAddr = ActorAddress('notify')
    lcs1.add_notification_handler(notifyAddr)
    return notifyAddr


@fixture
def update_lcs2_added(lcs2):
    return ActorSystemConventionUpdate(lcs2.myAddress,
                                       lcs2.capabilities,
                                       added=True)


## ############################################################
## Tests
## ############################################################

def test_S2A_prereg_reg(solo_lcs1, solo_lcs2,
                        convreg1_first_noadmin, convreg1_noadmin,
                        convreg2_prereg, convreg2_noadmin):

    lcs1, lcs2 = solo_lcs1, solo_lcs2

    # This test sends a pre-registration to lcs1 for lcs2, which
    # should cause lcs1 to actually register with lcs2 and lcs2 to
    # retro-register with its actual data.

    # Pre-register lcs2 with lcs1 and verify lcs1 sends its own info
    # to lcs2.  The registration indicated pre-registration but not an
    # assertion of first time (which would cause all existing remote
    # information to be dropped and all remote actors to be shutdown)
    # because this system may already know about the remote.  In this
    # scenario, lcs1 does not know about lcs2, so it should set the
    # first time indication on the info sent to lcs2.

    verify_io(lcs1.got_convention_register(convreg2_prereg),
              [ (LostRemote, None),
                (HysteresisCancel, None),
                (ConventionRegister, Sends(convreg1_first_noadmin) >= lcs2.myAddress),
              ])

    # lcs2 gets the ConventionRegister generated above, and responds
    # with actual info of its own.  If the other side is indicating
    # firstTime, that means it has no previous knowledge; this side
    # should not also set firstTime or that will bounce back and forth
    # indefinitely.  Note that this side will perform a transport
    # reset (LostRemote and HysteresisCancel); the TCPTransport may
    # ignore the transport reset for TXOnly addresses.

    verify_io(lcs2.got_convention_register(convreg1_first_noadmin),
              [ (LostRemote, None),
                (HysteresisCancel, None),
                (ConventionRegister, Sends(convreg2_noadmin) >= lcs1.myAddress),
              ])

    # lcs1 gets full ConventionRegister from lcs2.  This should also
    # cause an update notification with the full specification.
    verify_io(lcs1.got_convention_register(convreg2_noadmin),
              [ (ConventionRegister, Sends(convreg1_noadmin) >= lcs2.myAddress),
              ])

    verify_normal_notification_updates(lcs1, lcs2, convreg1_noadmin, convreg2_noadmin)

    assert [] == lcs1.check_convention()
    assert [] == lcs2.check_convention()


###
### Notification Tests
###

def test_notification_management(solo_lcs1, solo_lcs2):
    lcs1, lcs2 = solo_lcs1, solo_lcs2

    notifyAddr = ActorAddress('notify')
    verify_io(lcs1.add_notification_handler(notifyAddr),
              [])

    # Re-registration does nothing
    verify_io(lcs1.add_notification_handler(notifyAddr),
              [])

    # Registering a another handler is fine
    notifyAddr2 = ActorAddress('notify2')
    verify_io(lcs1.add_notification_handler(notifyAddr2),
              [])

    # Re-registration still does nothing
    verify_io(lcs1.add_notification_handler(notifyAddr),
              [])

    # De-registration
    lcs1.remove_notification_handler(notifyAddr)

    # Re-registration now adds it back
    verify_io(lcs1.add_notification_handler(notifyAddr),
              [])

    # Multiple de-registration is ok
    lcs1.remove_notification_handler(notifyAddr)
    lcs1.remove_notification_handler(notifyAddr)

    # Re-registration now adds it back again
    verify_io(lcs1.add_notification_handler(notifyAddr),
              [])


def test_notification_management_with_registrations(lcs1, lcs2, convreg1,
                                                    convreg2, convreg2_first,
                                                    conv1_notifyAddr, update_lcs2_added):

    # Setup both in the registered condition
    notifyAddr = test_reg_with_notifications(lcs1, lcs2, convreg1,
                                             convreg2, convreg2_first,
                                             conv1_notifyAddr, update_lcs2_added)


    # Re-registration does nothing
    verify_io(lcs1.add_notification_handler(notifyAddr),
              [])

    # Registering a another handler is fine
    notifyAddr2 = ActorAddress('notify2')

    notify_of_lcs2 = ActorSystemConventionUpdate(lcs2.myAddress,
                                                 lcs2.capabilities,
                                                 added=True)

    verify_io(lcs1.add_notification_handler(notifyAddr2),
              [ (ActorSystemConventionUpdate, Sends(notify_of_lcs2) >= notifyAddr2),
              ])

    # Re-registration still does nothing
    verify_io(lcs1.add_notification_handler(notifyAddr),
              [])

    # De-registration
    lcs1.remove_notification_handler(notifyAddr)

    # Re-registration now adds it back
    verify_io(lcs1.add_notification_handler(notifyAddr),
              [ (ActorSystemConventionUpdate, Sends(notify_of_lcs2) >= notifyAddr),
              ])

    # Multiple de-registration is ok
    lcs1.remove_notification_handler(notifyAddr)
    lcs1.remove_notification_handler(notifyAddr)

    # Re-registration now adds it back again
    verify_io(lcs1.add_notification_handler(notifyAddr),
              [ (ActorSystemConventionUpdate, Sends(notify_of_lcs2) >= notifyAddr),
              ])


def test_prereg_reg_with_notifications(solo_lcs1, solo_lcs2,
                                       convreg1_noadmin, convreg1_first_noadmin,
                                       convreg2_noadmin, convreg2_prereg):
    lcs1, lcs2 = solo_lcs1, solo_lcs2

    notifyAddr = ActorAddress('notify')

    lcs1.add_notification_handler(notifyAddr)

    verify_io(lcs1.got_convention_register(convreg2_prereg),
              [ (LostRemote, None),
                (HysteresisCancel, None),
                (ConventionRegister, Sends(convreg1_first_noadmin) >= lcs2.myAddress),
              ])

    # lcs2 gets the ConventionRegister generated above, and responds
    # with actual info of its own.  If the other side is indicating
    # firstTime, that means it has no previous knowledge; this side
    # should not also set firstTime or that will bounce back and forth
    # indefinitely.  Note that this side will perform a transport
    # reset (LostRemote and HysteresisCancel); the TCPTransport may
    # ignore the transport reset for TXOnly addresses.

    verify_io(lcs2.got_convention_register(convreg1_first_noadmin),
              [ (LostRemote, None),
                (HysteresisCancel, None),
                (ConventionRegister, Sends(convreg2_noadmin) >= lcs1.myAddress),
              ])

    # lcs1 gets full ConventionRegister from lcs2.  This should also
    # cause an update notification with the full specification.
    verify_io(lcs1.got_convention_register(convreg2_noadmin),
              [ (ConventionRegister, Sends(convreg1_noadmin) >= lcs2.myAddress),
                (ActorSystemConventionUpdate,
                 Sends(ActorSystemConventionUpdate(lcs2.myAddress,
                                                   lcs2.capabilities,
                                                   added=True)) >= notifyAddr),
              ])

    verify_normal_notification_updates(lcs1, lcs2, convreg1_noadmin, convreg2_noadmin)


def test_multi_prereg_reg_with_notifications(solo_lcs1, solo_lcs2,
                                             convreg1_first, convreg1_noadmin,
                                             convreg1_first_noadmin,
                                             convreg2_prereg, convreg2_noadmin):
    lcs1, lcs2 = solo_lcs1, solo_lcs2

    notifyAddr = ActorAddress('notify')

    lcs1.add_notification_handler(notifyAddr)

    verify_io(lcs1.got_convention_register(convreg2_prereg),
              [ (LostRemote, None),
                (HysteresisCancel, None),
                (ConventionRegister, Sends(convreg1_first_noadmin) >= lcs2.myAddress),
              ])

    # Another prereg should have no effect because the previous is in progress
    verify_io(lcs1.got_convention_register(convreg2_prereg),
              [
                #   (LostRemote, None),
                # (HysteresisCancel, None),
                # (ConventionRegister, Sends(convreg1_first) >=lcs2.myAddress),
              ])

    # lcs2 gets the ConventionRegister generated above, and responds
    # with actual info of its own.  If the other side is indicating
    # firstTime, that means it has no previous knowledge; this side
    # should not also set firstTime or that will bounce back and forth
    # indefinitely.  Note that this side will perform a transport
    # reset (LostRemote and HysteresisCancel); the TCPTransport may
    # ignore the transport reset for TXOnly addresses.

    verify_io(lcs2.got_convention_register(convreg1_first),
              [ (LostRemote, None),
                (HysteresisCancel, None),
                (ConventionRegister, Sends(convreg2_noadmin) >= lcs1.myAddress),
              ])

    # lcs1 gets full ConventionRegister from lcs2.  This should also
    # cause an update notification with the full specification.
    verify_io(lcs1.got_convention_register(convreg2_noadmin),
              [ (ConventionRegister, Sends(convreg1_noadmin) >= lcs2.myAddress),
                (ActorSystemConventionUpdate,
                 Sends(ActorSystemConventionUpdate(lcs2.myAddress,
                                                   lcs2.capabilities,
                                                   added=True)) >= notifyAddr),
              ])

    verify_normal_notification_updates(lcs1, lcs2, convreg1_noadmin, convreg2_noadmin)


def test_prereg_reg_prereg_with_notifications(solo_lcs1, solo_lcs2,
                                              convreg1_noadmin, convreg1_first_noadmin,
                                              convreg2_noadmin, convreg2_prereg):
    lcs1, lcs2 = solo_lcs1, solo_lcs2

    notifyAddr = ActorAddress('notify')

    lcs1.add_notification_handler(notifyAddr)

    verify_io(lcs1.got_convention_register(convreg2_prereg),
              [ (LostRemote, None),
                (HysteresisCancel, None),
                (ConventionRegister, Sends(convreg1_first_noadmin) >= lcs2.myAddress),
              ])

    # lcs2 gets the ConventionRegister generated above, and responds
    # with actual info of its own.  If the other side is indicating
    # firstTime, that means it has no previous knowledge; this side
    # should not also set firstTime or that will bounce back and forth
    # indefinitely.  Note that this side will perform a transport
    # reset (LostRemote and HysteresisCancel); the TCPTransport may
    # ignore the transport reset for TXOnly addresses.

    verify_io(lcs2.got_convention_register(convreg1_first_noadmin),
              [ (LostRemote, None),
                (HysteresisCancel, None),
                (ConventionRegister, Sends(convreg2_noadmin) >= lcs1.myAddress),
              ])

    # lcs1 gets full ConventionRegister from lcs2.  lcs1 as
    # ConventionLeader sends back its registration (not a first-time
    # registration) as it normally would, and also generates an update
    # notification with the full specification.
    verify_io(lcs1.got_convention_register(convreg2_noadmin),
              [ (ConventionRegister, Sends(convreg1_noadmin) >= lcs2.myAddress),
                (ActorSystemConventionUpdate,
                 Sends(ActorSystemConventionUpdate(lcs2.myAddress,
                                                   lcs2.capabilities,
                                                   added=True)) >= notifyAddr),
              ])

    verify_normal_notification_updates(lcs1, lcs2, convreg1_noadmin, convreg2_noadmin)

    # Another prereg has no effect because it is already registered
    verify_io(lcs1.got_convention_register(convreg2_prereg), [])


def test_reg_with_notifications(lcs1, lcs2, convreg1, convreg2, convreg2_first,
                                conv1_notifyAddr, update_lcs2_added):
    # S1A
    verify_io(lcs1.got_convention_register(convreg2_first),
              [ (LostRemote, None),
                (HysteresisCancel, None),
                (ConventionRegister, Sends(convreg1) >= lcs2.myAddress),
                (ActorSystemConventionUpdate, Sends(update_lcs2_added) >= conv1_notifyAddr),
              ])

    verify_io(lcs2.got_convention_register(convreg1), [])

    # Non-convention leader generates periodic registrations to the
    # leader (i.e. keepalive) and the leader responds accordingly.

    verify_io(lcs1.got_convention_register(convreg2),
              [ (ConventionRegister, Sends(convreg1) >= lcs2.myAddress),
              ])
    verify_io(lcs2.got_convention_register(convreg1), [])
    verify_io(lcs1.got_convention_register(convreg2),
              [ (ConventionRegister, Sends(convreg1) >= lcs2.myAddress),
              ])
    verify_io(lcs2.got_convention_register(convreg1), [])

    # Convention check shows all is in order and nothing needs to be done

    assert [] == lcs1.check_convention()
    assert [] == lcs2.check_convention()

    return conv1_notifyAddr  # used by callers


def test_check_before_activate_with_notifications(lcs1, lcs2, convreg2_first):
    ret = LocalConventionState(ActorAddress(1),
                                {'Admin Port': 1,
                                 'Convention Address.IPv4': ActorAddress(1),
                                 'popsicle': 'cold'},
                                StatsManager(),
                               lambda x: ActorAddress(1))
    # Activate the system
    verify_io(ret.setup_convention(), [])

    notifyAddr = ActorAddress('notify')

    lcs1.add_notification_handler(notifyAddr)

    ret = LocalConventionState(ActorAddress(2),
                                {'Admin Port': 2,
                                 'Convention Address.IPv4': ActorAddress(1),
                                 'apple pie': 'hot'},
                                StatsManager(),
                               lambda x: ActorAddress(1))
    ret._expected_setup_convreg = convreg2_first

    verify_io(ret.check_convention(), [])

    # Activate the system
    verify_io(ret.setup_convention(),
              [ (ConventionRegister, Sends(ret._expected_setup_convreg) >=ActorAddress(1)),
                (LogAggregator, None),
              ])



def test_reg_dereg_with_notifications(lcs1, lcs2,
                                      convreg1, convreg2, convreg2_first,
                                      convdereg_lcs2,
                                      conv1_notifyAddr, update_lcs2_added):

    # Setup both in the registered condition
    notifyAddr = test_reg_with_notifications(lcs1, lcs2, convreg1, convreg2, convreg2_first, conv1_notifyAddr, update_lcs2_added)

    verify_io(lcs1.got_convention_deregister(convdereg_lcs2),
              [ (LostRemote, None),
                (ActorSystemConventionUpdate,
                 Sends(ActorSystemConventionUpdate(lcs2.myAddress,
                                                   lcs2.capabilities,
                                                   added=False)) >= notifyAddr),
                (HysteresisCancel, None),
              ])


def test_reg_dereg_rereg_with_notifications(lcs1, lcs2,
                                            convreg1,
                                            convreg2, convreg2_first,
                                            convdereg_lcs2,
                                            conv1_notifyAddr, update_lcs2_added):

    # Setup both in the registered condition
    notifyAddr = test_reg_with_notifications(lcs1, lcs2, convreg1, convreg2, convreg2_first, conv1_notifyAddr, update_lcs2_added)

    verify_io(lcs1.got_convention_deregister(convdereg_lcs2),
              [ (LostRemote, None),
                (ActorSystemConventionUpdate,
                 Sends(ActorSystemConventionUpdate(lcs2.myAddress,
                                                   lcs2.capabilities,
                                                   added=False)) >= notifyAddr),
                (HysteresisCancel, None),
              ])

    test_reg_with_notifications(lcs1, lcs2, convreg1, convreg2, convreg2_first, conv1_notifyAddr, update_lcs2_added)


def test_reg_with_multiple_notifications(lcs1, lcs2,
                                         convreg1, convreg2, convreg2_first,
                                         convdereg_lcs2):

    notifyAddr1 = ActorAddress('notify1')
    lcs1.add_notification_handler(notifyAddr1)

    notifyAddr2 = ActorAddress('notify2')
    lcs1.add_notification_handler(notifyAddr2)

    notifyAddr3 = ActorAddress('notify3')
    lcs1.add_notification_handler(notifyAddr3)

    # KWQ: lcs2.add_notification_handler .. never gets called

    notificationUp = ActorSystemConventionUpdate(lcs2.myAddress,
                                                 lcs2.capabilities,
                                                 added=True)

    verify_io(lcs1.got_convention_register(convreg2_first),
              [ (LostRemote, None),
                (HysteresisCancel, None),
                (ConventionRegister, Sends(convreg1) >= lcs2.myAddress),
                (ActorSystemConventionUpdate, Sends(notificationUp) >= notifyAddr1),
                (ActorSystemConventionUpdate, Sends(notificationUp) >= notifyAddr2),
                (ActorSystemConventionUpdate, Sends(notificationUp) >= notifyAddr3),
              ])

    verify_io(lcs2.got_convention_register(convreg1), [])

    # Non-convention leader generates periodic registrations to the
    # leader (i.e. keepalive) and the leader responds accordingly.

    verify_io(lcs1.got_convention_register(convreg2),
              [ (ConventionRegister, Sends(convreg1) >= lcs2.myAddress),
              ])
    verify_io(lcs2.got_convention_register(convreg1), [])
    verify_io(lcs1.got_convention_register(convreg2),
              [ (ConventionRegister, Sends(convreg1) >= lcs2.myAddress),
              ])
    verify_io(lcs2.got_convention_register(convreg1), [])

    # De-registration

    notificationDown = ActorSystemConventionUpdate(lcs2.myAddress,
                                                   lcs2.capabilities,
                                                   added=False)

    verify_io(lcs1.got_convention_deregister(convdereg_lcs2),
              [ (LostRemote, None),
                (ActorSystemConventionUpdate, Sends(notificationDown) >= notifyAddr1),
                (ActorSystemConventionUpdate, Sends(notificationDown) >= notifyAddr2),
                (ActorSystemConventionUpdate, Sends(notificationDown) >= notifyAddr3),
                (HysteresisCancel, None),
              ])


@mark.skipif(not patch, reason='requires mock patch')
def test_reg_dereg_rereg_with_delay_and_updates(lcs1, lcs2,
                                                convreg1, convreg2, convreg2_first,
                                                convdereg_lcs2,
                                                conv1_notifyAddr, update_lcs2_added):

    # S1A:
    # Setup both in the registered condition
    notifyAddr = test_reg_with_notifications(lcs1, lcs2, convreg1, convreg2, convreg2_first, conv1_notifyAddr, update_lcs2_added)

    # Now add some elapsed time and check update messages

    lcs2_exited_update = ActorSystemConventionUpdate(
        lcs2.myAddress,
        lcs2.capabilities,
        added=False)

    with patch('thespian.system.timing.datetime') as p_datetime:
        p_datetime.now.return_value = (
            datetime.now() +
            CONVENTION_REREGISTRATION_PERIOD +
            timedelta(seconds=1))
        # Convention leader does not take action
        verify_io(lcs1.check_convention(), [])
        # But convention member should re-up their membership
        verify_io(lcs2.check_convention(),
                  [(ConventionRegister, Sends(convreg2) >= lcs1.myAddress),
                   (LogAggregator, None),
                  ])

    # Too long
    with patch('thespian.system.timing.datetime') as p_datetime:
        p_datetime.now.return_value = (
            datetime.now() +
            (CONVENTION_REREGISTRATION_PERIOD *
             CONVENTION_REGISTRATION_MISS_MAX) +
            timedelta(seconds=1))
        # Convention leader indicates that it is no longer a member
        verify_io(lcs1.check_convention(),
                  [ (LostRemote, None),
                    (ActorSystemConventionUpdate, Sends(lcs2_exited_update) >= notifyAddr),
                    (HysteresisCancel, None),
                  ])
        # But convention member should re-up their membership
        verify_io(lcs2.check_convention(),
                  [(ConventionRegister, Sends(convreg2) >= lcs1.myAddress),
                   (LogAggregator, None),
                  ])

    verify_io(lcs1.got_convention_deregister(convdereg_lcs2),
              [ (LostRemote, None),
                (HysteresisCancel, None),
              ])

    test_reg_with_notifications(lcs1, lcs2, convreg1, convreg2, convreg2_first, conv1_notifyAddr, update_lcs2_added)


## ############################################################
## Support and validation
## ############################################################

class Sends(object):
    def __init__(self, msg):
        self.msg = msg
    def __ge__(self, target_addr):
        self.tgt = target_addr
        return self
    def __call__(self, actual_msg, actual_target):
        assert self.msg == actual_msg, \
            "Expected send of %s but actually sent %s" % (self.msg, actual_msg)
        if hasattr(self, 'tgt'):
            assert self.tgt == actual_target, \
                "Expected send to %s but actually sent to %s" % (self.tgt,
                                                                 actual_target)
        return True


def verify_normal_notification_updates(lcs1, lcs2, convreg1_noadmin, convreg2):

    # Convention member generates periodic registrations to the leader
    # (i.e. keepalive) and the leader responds accordingly.

    verify_io(lcs1.got_convention_register(convreg2),
              [ (ConventionRegister, Sends(convreg1_noadmin) >= lcs2.myAddress),
              ])
    verify_io(lcs2.got_convention_register(convreg1_noadmin), [])
    verify_io(lcs1.got_convention_register(convreg2),
              [ (ConventionRegister, Sends(convreg1_noadmin) >= lcs2.myAddress),
              ])
    verify_io(lcs2.got_convention_register(convreg1_noadmin), [])

    
def verify_io(iolist, expected, any_order=False, echo=True):
    if echo:
        print("%s:: %s"%(len(iolist),'\n    '.join(fmap(str, iolist))))
    expected_type = [E[0] for E in expected]
    for each in iolist:
        if not expected:
            # Unexpected or too many response messages
            assert str(each) == expected_type
        if isinstance(each, (LogAggregator, LostRemote, HysteresisCancel)):
            if any_order:
                assert type(each) in expected_type
                expi = expected_type.index(type(each))
            else:
                assert type(each) == expected_type[0]
                expi = 0
            v = each, None
        else:
            # Either TransmitIntent or HysteresisSend; currently no
            # way to expclitly test for the latter without extending
            # `expected`
            if any_order:
                assert type(each.message) in expected_type
                expi = expected_type.index(type(each.message))
            else:
                assert type(each.message) == expected_type[0]
                expi = 0
            v = each.message, each.targetAddr
        if expected[expi][1]:
            assert expected[expi][1](*v), \
                'Validation of (%s): %s' % (type(each), fmap(str, each))
        del expected[expi]
        del expected_type[expi]
    # Did not see these remaining expected messages
    assert not expected_type


# KWQ: test: preregister, then remote sends deregister.  Registration sticks around and can be re-established.  At what point is registration re-attempted?  Active use?  Timeout? Die beiden?

