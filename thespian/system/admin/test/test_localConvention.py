from thespian.actors import ActorAddress, ActorSystemConventionUpdate
from thespian.system.messages.convention import (ConventionRegister,
                                                 ConventionDeRegister,
                                                 ConventionInvite)
from thespian.system.admin.convention import (LocalConventionState, LostRemote,
                                              HysteresisCancel,
                                              CONVENTION_REREGISTRATION_PERIOD,
                                              CONVENTION_REGISTRATION_MISS_MAX,
                                              convention_reinvite_adjustment)
from thespian.system.utilis import fmap, StatsManager
from thespian.system.timing import timePeriodSeconds
from thespian.system.logdirector import LogAggregator
from thespian.system.transport import SendStatus
try:
    from unittest.mock import patch
except ImportError:
    try:
        from mock import patch
    except ImportError:
        patch = None
from datetime import timedelta
from pytest import fixture, mark
import inspect
from contextlib import contextmanager


@contextmanager
def update_elapsed_time(time_base, elapsed):
    with patch('thespian.system.timing.currentTime') as p_ctime:
        p_ctime.return_value = time_base + (timePeriodSeconds(elapsed)
                                            if isinstance(elapsed, timedelta)
                                            else elapsed)
        yield p_ctime.return_value


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
    assert [] == ret.setup_convention(activation=True)
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
    assert [] == ret.setup_convention(activation=True)
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
def convreg1_noadmin(solo_lcs1):
    return ConventionRegister(solo_lcs1.myAddress,
                              solo_lcs1.capabilities,
                              firstTime=False,
                              preRegister=False)

@fixture
def convreg1_first_noadmin(solo_lcs1):
    return ConventionRegister(solo_lcs1.myAddress,
                              solo_lcs1.capabilities,
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


# solo relationships causing mincap?  These are actual preregister reqs?

@fixture
def convreg2_noadmin(solo_lcs2):
    return ConventionRegister(solo_lcs2.myAddress,
                              solo_lcs2.capabilities,
                              firstTime=False,
                              preRegister=False)

@fixture
def convreg2_first_noadmin(solo_lcs2):
    return ConventionRegister(solo_lcs2.myAddress,
                              solo_lcs2.capabilities,
                              firstTime=True,
                              preRegister=False)


@fixture
def convdereg_lcs1(lcs1):
    return ConventionDeRegister(lcs1.myAddress, preRegistered=False)


@fixture
def convdereg_lcs2(lcs2):
    return ConventionDeRegister(lcs2.myAddress, preRegistered=False)


@fixture
def convdereg_lcs2_prereg(lcs2):
    return ConventionDeRegister(lcs2.myAddress, preRegistered=True)


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

@fixture
def update_lcs2_added_noadmin(solo_lcs2):
    return ActorSystemConventionUpdate(solo_lcs2.myAddress,
                                       solo_lcs2.capabilities,
                                       added=True)

@fixture
def update_lcs2_removed(lcs2):
    return ActorSystemConventionUpdate(lcs2.myAddress,
                                       lcs2.capabilities,
                                       added=False)

@fixture
def update_lcs2_removed_noadmin(solo_lcs2):
    return ActorSystemConventionUpdate(solo_lcs2.myAddress,
                                       solo_lcs2.capabilities,
                                       added=False)


@fixture
def solo_conv1_notifyAddr(solo_lcs1):
    notifyAddr = ActorAddress('notify')
    solo_lcs1.add_notification_handler(notifyAddr)
    return notifyAddr



## ############################################################
## Tests
## ############################################################

def test_prereg_reg(solo_lcs1, solo_lcs2,
                    convreg1_first_noadmin, convreg1_noadmin,
                    convreg2_first_noadmin, convreg2_prereg, convreg2_noadmin,
                    update_lcs2_added_noadmin):

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

    ops = lcs1.got_convention_register(convreg2_prereg)
    verify_io(ops,
              [ (HysteresisCancel, None),
                Sends(ConventionInvite) >= lcs2.myAddress,
              ])
    ops[1].tx_done(SendStatus.Failed)  # indicate failure

    verify_io(lcs2.got_convention_invite(lcs1.myAddress),
              [ (ConventionRegister, Sends(convreg2_first_noadmin) >= lcs1.myAddress),
                (LogAggregator, None),
              ])

    # lcs1 gets full ConventionRegister from lcs2.  This should also
    # cause an update notification with the full specification.
    verify_io(lcs1.got_convention_register(convreg2_first_noadmin),
              [ Sends(convreg1_noadmin) >= lcs2.myAddress,
              ])

    verify_io(lcs2.got_convention_register(convreg1_noadmin), [])

    verify_normal_notification_updates(lcs1, lcs2, convreg1_noadmin, convreg2_noadmin)

    assert [] == lcs1.check_convention()
    assert [] == lcs2.check_convention()


###
### Notification Tests
###

def test_notification_management(solo_lcs1, solo_lcs2):
    lcs1, lcs2 = solo_lcs1, solo_lcs2

    notifyAddr = ActorAddress('notify')
    verify_io(lcs1.add_notification_handler(notifyAddr), [])

    # Re-registration does nothing
    verify_io(lcs1.add_notification_handler(notifyAddr), [])

    # Registering a another handler is fine
    notifyAddr2 = ActorAddress('notify2')
    verify_io(lcs1.add_notification_handler(notifyAddr2), [])

    # Re-registration still does nothing
    verify_io(lcs1.add_notification_handler(notifyAddr), [])

    # De-registration
    lcs1.remove_notification_handler(notifyAddr)

    # Re-registration now adds it back
    verify_io(lcs1.add_notification_handler(notifyAddr), [])

    # Multiple de-registration is ok
    lcs1.remove_notification_handler(notifyAddr)
    lcs1.remove_notification_handler(notifyAddr)

    # Re-registration now adds it back again
    verify_io(lcs1.add_notification_handler(notifyAddr), [])


@mark.skipif(not patch, reason='requires mock patch')
def test_notification_management_with_registrations(lcs1, lcs2, convreg1,
                                                    convreg2, convreg2_first,
                                                    conv1_notifyAddr,
                                                    update_lcs2_added,
                                                    update_lcs2_removed):

    # Setup both in the registered condition
    test_reg_with_notifications(lcs1, lcs2, convreg1,
                                convreg2, convreg2_first,
                                conv1_notifyAddr,
                                update_lcs2_added, update_lcs2_removed)


    # Re-registration does nothing
    verify_io(lcs1.add_notification_handler(conv1_notifyAddr), [])

    # Registering a another handler is fine
    notifyAddr2 = ActorAddress('notify2')

    notify_of_lcs2 = ActorSystemConventionUpdate(lcs2.myAddress,
                                                 lcs2.capabilities,
                                                 added=True)

    verify_io(lcs1.add_notification_handler(notifyAddr2),
              [ Sends(update_lcs2_added) >= notifyAddr2,
              ])

    # Re-registration still does nothing
    verify_io(lcs1.add_notification_handler(conv1_notifyAddr), [])

    # De-registration
    lcs1.remove_notification_handler(conv1_notifyAddr)

    # Re-registration now adds it back
    verify_io(lcs1.add_notification_handler(conv1_notifyAddr),
              [ Sends(update_lcs2_added) >= conv1_notifyAddr,
              ])

    # Multiple de-registration is ok
    lcs1.remove_notification_handler(conv1_notifyAddr)
    lcs1.remove_notification_handler(conv1_notifyAddr)

    # Re-registration now adds it back again
    verify_io(lcs1.add_notification_handler(conv1_notifyAddr),
              [ Sends(update_lcs2_added) >= conv1_notifyAddr,
              ])


def test_prereg_reg_with_notifications(solo_lcs1, solo_lcs2,
                                       convreg1_noadmin, #convreg1_first_noadmin,
                                       convreg2_noadmin, convreg2_first_noadmin,
                                       convreg2_prereg,
                                       solo_conv1_notifyAddr,
                                       update_lcs2_added_noadmin):
    lcs1, lcs2 = solo_lcs1, solo_lcs2

    notifyAddr = solo_conv1_notifyAddr

    ops = lcs1.got_convention_register(convreg2_prereg)
    verify_io(ops,
              [ (HysteresisCancel, None),
                Sends(ConventionInvite) >= lcs2.myAddress,
              ])
    ops[1].tx_done(SendStatus.Failed)  # indicate failure

    # lcs2 gets the ConventionRegister generated above, and responds
    # with actual info of its own.  If the other side is indicating
    # firstTime, that means it has no previous knowledge; this side
    # should not also set firstTime or that will bounce back and forth
    # indefinitely.  Note that this side will perform a transport
    # reset (LostRemote and HysteresisCancel); the TCPTransport may
    # ignore the transport reset for TXOnly addresses.

    verify_io(lcs2.got_convention_invite(lcs1.myAddress),
              [ Sends(convreg2_first_noadmin) >= lcs1.myAddress,
                (LogAggregator, None),
              ])

    # lcs1 gets full ConventionRegister from lcs2.  This should also
    # cause an update notification with the full specification.
    verify_io(lcs1.got_convention_register(convreg2_first_noadmin),
              [ Sends(convreg1_noadmin) >= lcs2.myAddress,
                Sends(update_lcs2_added_noadmin) >= notifyAddr,
              ])

    verify_io(lcs2.got_convention_register(convreg1_noadmin), [])

    verify_normal_notification_updates(lcs1, lcs2, convreg1_noadmin, convreg2_noadmin)


def test_multi_prereg_reg_with_notifications(solo_lcs1, solo_lcs2,
                                             convreg1_noadmin,
                                             convreg2_prereg, convreg2_first_noadmin,
                                             convreg2_noadmin,
                                             conv1_notifyAddr,
                                             update_lcs2_added_noadmin):
    lcs1, lcs2 = solo_lcs1, solo_lcs2

    ops = lcs1.got_convention_register(convreg2_prereg)
    verify_io(ops,
              [ (HysteresisCancel, None),
                Sends(ConventionInvite) >= lcs2.myAddress,
              ])
    ops[1].tx_done(SendStatus.Failed)  # indicate failure

    # Another prereg should just repeat the invitation but have no
    # other effect because the previous is in progress
    ops = lcs1.got_convention_register(convreg2_prereg)
    verify_io(ops,
              [ (HysteresisCancel, None),
                Sends(ConventionInvite) >= lcs2.myAddress,
              ])
    ops[1].tx_done(SendStatus.Failed)  # indicate failure

    # lcs2 gets the ConventionRegister generated above, and responds
    # with actual info of its own.  If the other side is indicating
    # firstTime, that means it has no previous knowledge; this side
    # should not also set firstTime or that will bounce back and forth
    # indefinitely.  Note that this side will perform a transport
    # reset (LostRemote and HysteresisCancel); the TCPTransport may
    # ignore the transport reset for TXOnly addresses.

    verify_io(lcs2.got_convention_invite(lcs1.myAddress),
              [ Sends(convreg2_first_noadmin) >= lcs1.myAddress,
                (LogAggregator, None),
              ])

    # lcs1 gets full ConventionRegister from lcs2.  This should also
    # cause an update notification with the full specification.
    verify_io(lcs1.got_convention_register(convreg2_first_noadmin),
              [ Sends(convreg1_noadmin) >= lcs2.myAddress,
              ])

    verify_io(lcs2.got_convention_register(convreg1_noadmin), [])

    verify_normal_notification_updates(lcs1, lcs2, convreg1_noadmin, convreg2_noadmin)


def test_prereg_reg_prereg_with_notifications(solo_lcs1, solo_lcs2,
                                              convreg1_noadmin, #convreg1_first_noadmin,
                                              convreg2_noadmin, convreg2_first_noadmin,
                                              convreg2_prereg,
                                              update_lcs2_added_noadmin):
    #, update_lcs2_removed):
    lcs1, lcs2 = solo_lcs1, solo_lcs2

    notifyAddr = ActorAddress('notify')

    lcs1.add_notification_handler(notifyAddr)

    ops = lcs1.got_convention_register(convreg2_prereg)
    verify_io(ops,
              [ (HysteresisCancel, None),
                Sends(ConventionInvite) >= lcs2.myAddress,
              ])
    ops[1].tx_done(SendStatus.Failed)  # indicate failure

    # Lcs2 gets the ConventionInvite generated above, and responds
    # with actual info of its own.  If the other side is indicating
    # firstTime, that means it has no previous knowledge; this side
    # should not also set firstTime or that will bounce back and forth
    # indefinitely.  Note that this side will perform a transport
    # reset (LostRemote and HysteresisCancel); the TCPTransport may
    # ignore the transport reset for TXOnly addresses.

    verify_io(lcs2.got_convention_invite(lcs1.myAddress),
              [ Sends(convreg2_first_noadmin) >= lcs1.myAddress,
                (LogAggregator, None),
              ])

    # lcs1 gets full ConventionRegister from lcs2.  lcs1 as
    # ConventionLeader sends back its registration (not a first-time
    # registration) as it normally would, and also generates an update
    # notification with the full specification.
    verify_io(lcs1.got_convention_register(convreg2_first_noadmin),
              [ Sends(convreg1_noadmin) >= lcs2.myAddress,
                Sends(update_lcs2_added_noadmin) >= notifyAddr,
              ])

    verify_io(lcs2.got_convention_register(convreg1_noadmin), [])

    verify_normal_notification_updates(lcs1, lcs2, convreg1_noadmin, convreg2_noadmin)

    # Another prereg has no effect other than causing a new invite to
    # be sent because it is already registered
    ops = lcs1.got_convention_register(convreg2_prereg)
    verify_io(ops,
              [ (HysteresisCancel, None),
                Sends(ConventionInvite) >= lcs2.myAddress,
              ])
    ops[1].tx_done(SendStatus.Failed)  # indicate failure


@mark.skipif(not patch, reason='requires mock patch')
def test_reg_with_notifications(lcs1, lcs2, convreg1, convreg2, convreg2_first,
                                conv1_notifyAddr, update_lcs2_added, update_lcs2_removed,
                                lcs2_was_registered=False,
                                time_offset=timedelta(0)):
    # Similar to S1A, but without testing of subsequent delay periods.

    with update_elapsed_time(0.001, time_offset) as the_time:

        # This test is also called from several other tests to provide
        # common functionality for validating convention entry.  Some of
        # those calls may occur after convention membership was lost, so
        # there may be an extra notification needed.

        expected_messages = \
                  [ Sends(convreg1) >= lcs2.myAddress,
                    Sends(update_lcs2_added) >= conv1_notifyAddr,
                  ]
        if lcs2_was_registered:
            # lcs2 was previously registered, so since it is now sending a
            # registration with "first" specified, that should cause a
            # reset of its membership, which includes a notification
            # update.
            expected_messages = [
                (LostRemote, None),
                Sends(update_lcs2_removed) >= conv1_notifyAddr,
                (HysteresisCancel, None),
            ] + expected_messages

        verify_io(lcs1.got_convention_register(convreg2_first), expected_messages)

        verify_io(lcs2.got_convention_register(convreg1), [])

        # Non-convention leader generates periodic registrations to the
        # leader (i.e. keepalive) and the leader responds accordingly.

        # A pair of messages within the initial time period causes an
        # acknowledgement, but no other activities.

        verify_normal_notification_updates(lcs1, lcs2, convreg1, convreg2)

        # Convention check shows all is in order and nothing needs to be done

        verify_io(lcs1.check_convention(), [])
        verify_io(lcs2.check_convention(), [])

        time_base = the_time

    return conv1_notifyAddr, time_base  # used by callers


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
              [ Sends(ret._expected_setup_convreg) >=ActorAddress(1),
                (LogAggregator, None),
              ])



@mark.skipif(not patch, reason='requires mock patch')
def test_reg_dereg_with_notifications(lcs1, lcs2,
                                      convreg1, convreg2, convreg2_first,
                                      convdereg_lcs2,
                                      conv1_notifyAddr,
                                      update_lcs2_added, update_lcs2_removed):

    # Setup both in the registered condition
    notifyAddr, time_base = test_reg_with_notifications(lcs1, lcs2,
                                                        convreg1, convreg2,
                                                        convreg2_first,
                                                        conv1_notifyAddr,
                                                        update_lcs2_added,
                                                        update_lcs2_removed)

    verify_io(lcs1.got_convention_deregister(convdereg_lcs2),
              [ (LostRemote, None),
                Sends(update_lcs2_removed) >= notifyAddr,
                (HysteresisCancel, None),
              ])


@mark.skipif(not patch, reason='requires mock patch')
def test_reg_dereg_rereg_with_notifications(lcs1, lcs2,
                                            convreg1,
                                            convreg2, convreg2_first,
                                            convdereg_lcs1, convdereg_lcs2,
                                            conv1_notifyAddr,
                                            update_lcs2_added, update_lcs2_removed):

    # Setup both in the registered condition
    notifyAddr, time_base = test_reg_with_notifications(lcs1, lcs2,
                                                        convreg1, convreg2,
                                                        convreg2_first,
                                                        conv1_notifyAddr,
                                                        update_lcs2_added,
                                                        update_lcs2_removed)

    verify_io(lcs1.got_convention_deregister(convdereg_lcs2),
              [ (LostRemote, None),
                Sends(ActorSystemConventionUpdate(lcs2.myAddress,
                                                  lcs2.capabilities,
                                                  added=False)) >= notifyAddr,
                (HysteresisCancel, None),
              ])

    verify_io(lcs2.got_convention_deregister(convdereg_lcs1),
              [ (LostRemote, None),
                (HysteresisCancel, None),
              ])


    test_reg_with_notifications(lcs1, lcs2, convreg1, convreg2,
                                convreg2_first, conv1_notifyAddr,
                                update_lcs2_added, update_lcs2_removed)


def test_reg_with_multiple_notifications(lcs1, lcs2,
                                         convreg1, convreg2, convreg2_first,
                                         convdereg_lcs2, update_lcs2_added,
                                         update_lcs2_removed):

    notifyAddr1 = ActorAddress('notify1')
    lcs1.add_notification_handler(notifyAddr1)

    notifyAddr2 = ActorAddress('notify2')
    lcs1.add_notification_handler(notifyAddr2)

    notifyAddr3 = ActorAddress('notify3')
    lcs1.add_notification_handler(notifyAddr3)

    verify_io(lcs1.got_convention_register(convreg2_first),
              [ Sends(convreg1) >= lcs2.myAddress,
                Sends(update_lcs2_added) >= notifyAddr1,
                Sends(update_lcs2_added) >= notifyAddr2,
                Sends(update_lcs2_added) >= notifyAddr3,
              ])

    verify_io(lcs2.got_convention_register(convreg1), [])

    # Non-convention leader generates periodic registrations to the
    # leader (i.e. keepalive) and the leader responds accordingly.

    verify_io(lcs1.got_convention_register(convreg2),
              [ Sends(convreg1) >= lcs2.myAddress,
              ])
    verify_io(lcs2.got_convention_register(convreg1), [])
    verify_io(lcs1.got_convention_register(convreg2),
              [ Sends(convreg1) >= lcs2.myAddress,
              ])
    verify_io(lcs2.got_convention_register(convreg1), [])

    # De-registration

    verify_io(lcs1.got_convention_deregister(convdereg_lcs2),
              [ (LostRemote, None),
                Sends(update_lcs2_removed) >= notifyAddr1,
                Sends(update_lcs2_removed) >= notifyAddr2,
                Sends(update_lcs2_removed) >= notifyAddr3,
                (HysteresisCancel, None),
              ])



@mark.skipif(not patch, reason='requires mock patch')
def test_S1A(lcs1, lcs2,
             convreg1, convreg2, convreg2_first,
             convdereg_lcs2,
             conv1_notifyAddr, update_lcs2_added, update_lcs2_removed,
             lcs2_was_registered=False,
             time_offset=timedelta(0)):

    # Setup both in the registered condition
    _, time_base = test_reg_with_notifications(
        lcs1, lcs2, convreg1, convreg2,
        convreg2_first, conv1_notifyAddr,
        update_lcs2_added, update_lcs2_removed,
        lcs2_was_registered=lcs2_was_registered,
        time_offset = time_offset)

    assert lcs2.active_in_convention()

    # Now add some elapsed time and check update messages

    with update_elapsed_time(time_base,
                             # time_offset +
                             CONVENTION_REREGISTRATION_PERIOD +
                             timedelta(seconds=1)) as later:

        # Convention leader does not take action
        verify_io(lcs1.check_convention(), [])
        # But convention member should re-up their membership
        verify_io(lcs2.check_convention(),
                  [ Sends(convreg2) >= lcs1.myAddress,
                    (LogAggregator, None),
                  ], echo=True)

        assert lcs2.active_in_convention()

        # And convention leader should respond
        verify_io(lcs1.got_convention_register(convreg2),
                  [(ConventionRegister, Sends(convreg1) >= lcs2.myAddress),
                  ])
        # But convention member ends the conversation
        verify_io(lcs2.got_convention_register(convreg1),
                  [])

    return time_base


# S1B is not really testable, other than as a subset of S1C


@mark.skipif(not patch, reason='requires mock patch')
def test_S1C(lcs1, lcs2, convreg1, convreg2, convreg2_first,
             convdereg_lcs2,
             conv1_notifyAddr, update_lcs2_added, update_lcs2_removed):

    time_base = test_S1A(lcs1, lcs2, convreg1, convreg2, convreg2_first,
                         convdereg_lcs2,
                         conv1_notifyAddr,
                         update_lcs2_added, update_lcs2_removed)

    # Now add some more elapsed time and check update messages

    timeoffset = CONVENTION_REREGISTRATION_PERIOD  + timedelta(seconds=1) # from test_S1A
    for retries in range(0, CONVENTION_REGISTRATION_MISS_MAX):
        timeoffset += CONVENTION_REREGISTRATION_PERIOD
        with update_elapsed_time(time_base, timeoffset) as later:
            # convention member should attempt to re-up their membership
            ops = lcs2.check_convention()
            verify_io(ops,
                      [ Sends(convreg2) >= lcs1.myAddress,
                        (LogAggregator, None),
                      ])
            ops[0].tx_done(SendStatus.Failed)  # indicate failure

    # Too many misses, the member should believe it is no longer a member

    timeoffset += CONVENTION_REREGISTRATION_PERIOD
    with update_elapsed_time(time_base, timeoffset) as later:
        # convention member should attempt to re-up their membership
        ops = lcs2.check_convention()
        verify_io(ops,
                  [ (LostRemote, None),
                    (HysteresisCancel, None),
                  ])

    assert not lcs2.active_in_convention()

    # After a really long time, connectivity is restored

    timeoffset += timedelta(seconds=100)
    test_S1A(lcs1, lcs2,
             convreg1, convreg2, convreg2_first,
             convdereg_lcs2,
             conv1_notifyAddr, update_lcs2_added, update_lcs2_removed,
             lcs2_was_registered=True,
             time_offset=timeoffset)


@mark.skipif(not patch, reason='requires mock patch')
def test_S1D(lcs1, lcs2,
             convreg1, convreg2, convreg2_first,
             convdereg_lcs2,
             conv1_notifyAddr, update_lcs2_added, update_lcs2_removed):

    time_base = test_S1A(lcs1, lcs2,
                         convreg1, convreg2, convreg2_first,
                         convdereg_lcs2,
                         conv1_notifyAddr,
                         update_lcs2_added,
                         update_lcs2_removed)

    # Skip time ahead to a point beyond then the convention would
    # expire.  S1A performed one refresh, so this needs to skip
    # forward from there.

    time_offset = (CONVENTION_REREGISTRATION_PERIOD *
                   (CONVENTION_REGISTRATION_MISS_MAX + 1)) + \
                   timedelta(seconds=1)

    with update_elapsed_time(time_base, time_offset) as later:
        # Convention leader indicates that it is no longer a member
        verify_io(lcs1.check_convention(),
                  [ (LostRemote, None),
                    Sends(update_lcs2_removed) >= conv1_notifyAddr,
                    (HysteresisCancel, None),
                  ])
        # But convention member should re-up their membership
        verify_io(lcs2.check_convention(),
                  [Sends(convreg2) >= lcs1.myAddress,
                   (LogAggregator, None),
                  ])

    test_reg_with_notifications(lcs1, lcs2,
                                convreg1, convreg2,
                                # n.b. member did not disconnect or
                                # timeout, so it is still sending
                                # normal registration messages
                                convreg2,
                                conv1_notifyAddr,
                                update_lcs2_added, update_lcs2_removed,
                                time_offset=time_offset)


@mark.skipif(not patch, reason='requires mock patch')
def test_S1E(lcs1, lcs2,
             convreg1, convreg2, convreg2_first,
             convdereg_lcs2,
             conv1_notifyAddr, update_lcs2_added, update_lcs2_removed):

    test_S1A(lcs1, lcs2,
             convreg1, convreg2, convreg2_first,
             convdereg_lcs2,
             conv1_notifyAddr, update_lcs2_added, update_lcs2_removed)

    # Remote de-registers
    verify_io(lcs2.exit_convention(),
              [ (HysteresisCancel, None),
                Sends(convdereg_lcs2) >= lcs1.myAddress,
              ])
    verify_io(lcs1.got_convention_deregister(convdereg_lcs2),
              [ (LostRemote, None),
                Sends(update_lcs2_removed) >= conv1_notifyAddr,
                (HysteresisCancel, None),
              ])

    test_reg_with_notifications(lcs1, lcs2,
                                convreg1, convreg2, convreg2_first,
                                conv1_notifyAddr,
                                update_lcs2_added, update_lcs2_removed)


# S1F not directly testable (needs stimulus not provided in lcs).  Similar to S1D.


############################################################

@mark.skipif(not patch, reason='requires mock patch')
def test_S2A(solo_lcs1, solo_lcs2,
             convreg1_noadmin,
             convreg2_prereg, convreg2_noadmin, convreg2_first_noadmin,
             solo_conv1_notifyAddr,
             update_lcs2_added_noadmin, update_lcs2_removed_noadmin,
             time_offset=timedelta(seconds=0)):

    with update_elapsed_time(0.002, time_offset) as the_time:

        lcs1, lcs2 = solo_lcs1, solo_lcs2

        ops = lcs1.got_convention_register(convreg2_prereg)
        verify_io(ops, [(HysteresisCancel, None),
                        Sends(ConventionInvite) >= lcs2.myAddress,])
        ops[1].tx_done(SendStatus.Sent)  # indicate failure

    return S2A_1(lcs1, lcs2, convreg1_noadmin,
                 convreg2_prereg, convreg2_noadmin, convreg2_first_noadmin,
                 solo_conv1_notifyAddr,
                 update_lcs2_added_noadmin, update_lcs2_removed_noadmin,
                 time_offset=time_offset + timedelta(milliseconds=2))


def S2A_1(lcs1, lcs2,
          convreg1_noadmin,
          convreg2_prereg, convreg2_noadmin, convreg2_first_noadmin,
          notifyAddr, update_lcs2_added, update_lcs2_removed,
          lcs2_was_in_convention=False,
          time_offset=timedelta(0)):

    with update_elapsed_time(0, time_offset) as the_time:
        verify_io(lcs2.got_convention_invite(lcs1.myAddress),
                  [ Sends(convreg2_first_noadmin) >= lcs1.myAddress,
                    (LogAggregator, None),
                  ])

    return S2A_2(lcs1, lcs2,
                 convreg1_noadmin,
                 convreg2_prereg, convreg2_noadmin, convreg2_first_noadmin,
                 notifyAddr, update_lcs2_added, update_lcs2_removed,
                 lcs2_was_in_convention=lcs2_was_in_convention,
                 time_offset=time_offset)


def S2A_2(lcs1, lcs2,
          convreg1_noadmin,
          convreg2_prereg, convreg2_noadmin, convreg2_first_noadmin,
          notifyAddr, update_lcs2_added, update_lcs2_removed,
          lcs2_was_in_convention=False,
          time_offset=timedelta(0)):

    with update_elapsed_time(0, time_offset):

        # lcs1 gets full ConventionRegister from lcs2.  This should also
        # cause an update notification with the full specification.
        verify_io(lcs1.got_convention_register(convreg2_first_noadmin),
                  ([ (LostRemote, None),
                     Sends(update_lcs2_removed) >= notifyAddr,
                     (HysteresisCancel, None),
                   ] if lcs2_was_in_convention else []
                  ) +
                  [ Sends(convreg1_noadmin) >= lcs2.myAddress,
                    Sends(update_lcs2_added) >= notifyAddr,
                  ], echo=True)

        verify_io(lcs2.got_convention_register(convreg1_noadmin), [])

        verify_normal_notification_updates(lcs1, lcs2,
                                           convreg1_noadmin, convreg2_noadmin)

    # Now add some elapsed time and check update messages

    with update_elapsed_time(0, time_offset +
                             CONVENTION_REREGISTRATION_PERIOD +
                             timedelta(seconds=1)):
        # Convention leader issues an invite
        ops = lcs1.check_convention()
        verify_io(ops,
                  [ (ConventionInvite, None),
                  ])
        ops[0].tx_done(SendStatus.Sent)  # indicate failure
        verify_io(lcs2.got_convention_invite(lcs1.myAddress),
                  [ Sends(convreg2_noadmin) >= lcs1.myAddress,
                    (LogAggregator, None),
                  ])
        verify_io(lcs2.check_convention(),
                  [
                  ])
        # # And convention leader should respond
        verify_io(lcs1.got_convention_register(convreg2_noadmin),
                  [(ConventionRegister,
                    Sends(convreg1_noadmin) >= lcs2.myAddress),
                  ])
        # But convention member ends the conversation
        verify_io(lcs2.got_convention_register(convreg1_noadmin),
                  [])

    return time_offset + CONVENTION_REREGISTRATION_PERIOD + timedelta(seconds=1)



@mark.skipif(not patch, reason='requires mock patch')
def test_S2B(solo_lcs1, solo_lcs2,
             convreg1_noadmin,
             convreg2_prereg, convreg2_noadmin, convreg2_first_noadmin,
             solo_conv1_notifyAddr,
             update_lcs2_added_noadmin, update_lcs2_removed_noadmin):

    time_base = 0.003

    with update_elapsed_time(time_base, timedelta(0)):
        lcs1, lcs2 = solo_lcs1, solo_lcs2

        ops = lcs1.got_convention_register(convreg2_prereg)
        verify_io(ops,
                  [ (HysteresisCancel, None),
                    Sends(ConventionInvite) >= lcs2.myAddress,
                  ])
        ops[1].tx_done(SendStatus.Failed)  # indicate failure

    time_offset = convention_reinvite_adjustment(CONVENTION_REREGISTRATION_PERIOD) + timedelta(seconds=1)
    with update_elapsed_time(time_base, time_offset):
        # Convention leader has not connecteed to remote and retries
        ops = lcs1.check_convention()
        verify_io(ops,
                  [ Sends(ConventionInvite) >= lcs2.myAddress,
                  ])
        ops[0].tx_done(SendStatus.Failed)  # indicate failure

    time_offset += convention_reinvite_adjustment(CONVENTION_REREGISTRATION_PERIOD) + timedelta(seconds=1)
    with update_elapsed_time(time_base, time_offset):
        # Convention leader has not connecteed to remote and retries
        ops = lcs1.check_convention()
        verify_io(ops,
                  [ Sends(ConventionInvite) >= lcs2.myAddress,
                  ])
        ops[0].tx_done(SendStatus.Failed)  # indicate failure


    time_offset += CONVENTION_REREGISTRATION_PERIOD
    with update_elapsed_time(time_base, time_offset):
        S2A_1(lcs1, lcs2, convreg1_noadmin,
              convreg2_prereg, convreg2_noadmin, convreg2_first_noadmin,
              solo_conv1_notifyAddr, update_lcs2_added_noadmin, update_lcs2_removed_noadmin,
              time_offset=time_offset)


@mark.skipif(not patch, reason='requires mock patch')
def test_S2C(solo_lcs1, solo_lcs2, convreg1_noadmin, convreg2_noadmin,
             convreg2_first_noadmin, convreg2_prereg,
             convdereg_lcs2,
             solo_conv1_notifyAddr,
             update_lcs2_added_noadmin, update_lcs2_removed_noadmin,):

    lcs1, lcs2 = solo_lcs1, solo_lcs2

    time_base = 0.003

    with update_elapsed_time(time_base, timedelta(0)):
        timeoffset = test_S2A(solo_lcs1, solo_lcs2,
                              convreg1_noadmin,
                              convreg2_prereg,
                              convreg2_noadmin, convreg2_first_noadmin,
                              solo_conv1_notifyAddr,
                              update_lcs2_added_noadmin,
                              update_lcs2_removed_noadmin)

    # Now add some more elapsed time and check update messages

    timeoffset += timedelta(milliseconds=10) # step over thresholds

    for retries in range(0, CONVENTION_REGISTRATION_MISS_MAX):
        timeoffset += CONVENTION_REREGISTRATION_PERIOD
        with update_elapsed_time(time_base, timeoffset):
            # convention member should attempt to re-up their membership
            ops = lcs2.check_convention()
            verify_io(ops,
                      [ Sends(convreg2_noadmin) >= lcs1.myAddress,
                        (LogAggregator, None),
                      ])
            ops[0].tx_done(SendStatus.Failed)  # indicate failure

    # Too many misses, the member should believe it is no longer a member

    timeoffset += CONVENTION_REREGISTRATION_PERIOD
    with update_elapsed_time(time_base, timeoffset):
        ops = lcs2.check_convention()
        verify_io(ops,
                  [ (LostRemote, None),
                    (HysteresisCancel, None),
                  ])

    assert not lcs2.active_in_convention()

    # Ignores any Convention Register (acks) received after that

    timeoffset += timedelta(seconds=2)
    with update_elapsed_time(time_base, timeoffset):
        verify_io(lcs2.got_convention_register(convreg1_noadmin), [], echo=True)

    # But a new invitation restores everything

    timeoffset += timedelta(seconds=1)
    with update_elapsed_time(time_base, timeoffset):
        S2A_1(lcs1, lcs2,
              convreg1_noadmin,
              convreg2_prereg, convreg2_noadmin, convreg2_first_noadmin,
              solo_conv1_notifyAddr,
              update_lcs2_added_noadmin, update_lcs2_removed_noadmin,
              lcs2_was_in_convention=True,
              time_offset=timeoffset
        )


@mark.skipif(not patch, reason='requires mock patch')
def test_S2D(solo_lcs1, solo_lcs2, convreg1_noadmin, convreg2_noadmin,
             convreg2_first_noadmin, convreg2_prereg,
             convdereg_lcs2,
             conv1_notifyAddr,
             solo_conv1_notifyAddr,
             update_lcs2_added_noadmin, update_lcs2_removed_noadmin):

    lcs1, lcs2 = solo_lcs1, solo_lcs2

    time_base = 0.003

    with update_elapsed_time(time_base, timedelta(0)):
        test_S2A(solo_lcs1, solo_lcs2,
                 convreg1_noadmin,
                 convreg2_prereg,
                 convreg2_noadmin, convreg2_first_noadmin,
                 solo_conv1_notifyAddr,
                 update_lcs2_added_noadmin,
                 update_lcs2_removed_noadmin)

    # Now add some more elapsed time and check update messages

    timeoffset = (convention_reinvite_adjustment(CONVENTION_REREGISTRATION_PERIOD) +
                  timedelta(seconds=1))

    for retries in range(0, CONVENTION_REGISTRATION_MISS_MAX-1):
        timeoffset += convention_reinvite_adjustment(CONVENTION_REREGISTRATION_PERIOD) + \
                      timedelta(seconds=1)
        with update_elapsed_time(time_base, timeoffset):
            # convention member should attempt to re-up their membership
            ops = lcs1.check_convention()
            verify_io(ops,
                      [ Sends(ConventionInvite) >= lcs2.myAddress,
                      ])
            ops[0].tx_done(SendStatus.Failed)  # indicate failure

    # Too many misses, the member should believe it is no longer a member

    timeoffset += CONVENTION_REREGISTRATION_PERIOD
    with update_elapsed_time(time_base, timeoffset):
        # convention member should attempt to re-up their membership
        verify_io(lcs1.check_convention(),
                  [ (LostRemote, None),
                    Sends(update_lcs2_removed_noadmin) >= conv1_notifyAddr,
                    (HysteresisCancel, None),
                  ])

    # Exit notification and cleanup is only performed once, but still issues invitations

    for retry in range(2):
        timeoffset += convention_reinvite_adjustment(CONVENTION_REREGISTRATION_PERIOD) + \
                      timedelta(seconds=1)
        with update_elapsed_time(time_base, timeoffset):
            # convention member should attempt to re-up their membership
            ops = lcs1.check_convention()
            verify_io(ops,
                      [ Sends(ConventionInvite) >= lcs2.myAddress,
                      ])
            ops[0].tx_done(SendStatus.Failed)  # indicate failure

    # After a really long time, connectivity is restored

    timeoffset = ((CONVENTION_REREGISTRATION_PERIOD *
                   CONVENTION_REGISTRATION_MISS_MAX) +
                  timedelta(seconds=100))
    with update_elapsed_time(time_base, timeoffset):
        S2A_2(lcs1, lcs2,
              convreg1_noadmin,
              convreg2_prereg, convreg2_noadmin,
              convreg2_noadmin,  # not first because lcs2 still thinks it is a member
              conv1_notifyAddr, update_lcs2_added_noadmin, update_lcs2_removed_noadmin,
              time_offset=timeoffset
        )


@mark.skipif(not patch, reason='requires mock patch')
def test_S2E(solo_lcs1, solo_lcs2, convreg1_noadmin, convreg2_noadmin,
             convreg2_first_noadmin, convreg2_prereg,
             convdereg_lcs2,
             conv1_notifyAddr,
             solo_conv1_notifyAddr,
             update_lcs2_added_noadmin, update_lcs2_removed_noadmin):

    lcs1, lcs2 = solo_lcs1, solo_lcs2

    time_base = 0.003

    with update_elapsed_time(time_base, timedelta(0)):
        test_S2A(solo_lcs1, solo_lcs2,
                 convreg1_noadmin,
                 convreg2_prereg, convreg2_noadmin, convreg2_first_noadmin,
                 solo_conv1_notifyAddr,
                 update_lcs2_added_noadmin, update_lcs2_removed_noadmin)


        verify_io(lcs1.got_convention_deregister(convdereg_lcs2),
                  [ (LostRemote, None),
                    Sends(update_lcs2_removed_noadmin) >= solo_conv1_notifyAddr,
                    (HysteresisCancel, None),
                  ])

    # Now add some more elapsed time and check update messages
    # Exit notification and cleanup is only performed once, but still issues invitations

    timeoffset = (convention_reinvite_adjustment(CONVENTION_REREGISTRATION_PERIOD) +
                  timedelta(seconds=1))

    for retry in range(2):
        timeoffset += convention_reinvite_adjustment(CONVENTION_REREGISTRATION_PERIOD) + \
                      timedelta(seconds=1)
        with update_elapsed_time(time_base, timeoffset):
            # convention member should attempt to re-up their membership
            ops = lcs1.check_convention()
            verify_io(ops,
                      [ Sends(ConventionInvite) >= lcs2.myAddress,
                      ])
            ops[0].tx_done(SendStatus.Failed)  # indicate failure

    return time_base, timeoffset


@mark.skipif(not patch, reason='requires mock patch')
def test_S2F(solo_lcs1, solo_lcs2, convreg1_noadmin, convreg2_noadmin,
             convreg1_first_noadmin, convreg2_first_noadmin, convreg2_prereg,
             convdereg_lcs2,
             conv1_notifyAddr,
             solo_conv1_notifyAddr,
             update_lcs2_added_noadmin, update_lcs2_removed_noadmin):

    lcs1, lcs2 = solo_lcs1, solo_lcs2

    time_base, timeoffset = test_S2E(solo_lcs1, solo_lcs2,
                                     convreg1_noadmin, convreg2_noadmin,
                                     convreg2_first_noadmin,
                                     convreg2_prereg,
                                     convdereg_lcs2,
                                     conv1_notifyAddr,
                                     solo_conv1_notifyAddr,
                                     update_lcs2_added_noadmin,
                                     update_lcs2_removed_noadmin)

    # After a really long time, connectivity is restored

    timeoffset = ((CONVENTION_REREGISTRATION_PERIOD *
                   CONVENTION_REGISTRATION_MISS_MAX) +
                  timedelta(seconds=100))
    with update_elapsed_time(time_base, timeoffset):
        S2A_2(lcs1, lcs2,
              convreg1_noadmin,
              convreg2_prereg, convreg2_noadmin,
              convreg2_noadmin,  # not first because lcs2 still thinks it is a member
              conv1_notifyAddr,
              update_lcs2_added_noadmin, update_lcs2_removed_noadmin,
              time_offset=timeoffset
        )


@mark.skipif(not patch, reason='requires mock patch')
def test_S2G(solo_lcs1, solo_lcs2, convreg1_noadmin, convreg2_noadmin,
             convreg2_first_noadmin, convreg2_prereg,
             convdereg_lcs1, convdereg_lcs2, convdereg_lcs2_prereg,
             conv1_notifyAddr,
             solo_conv1_notifyAddr,
             update_lcs2_added_noadmin, update_lcs2_removed_noadmin):

    lcs1, lcs2 = solo_lcs1, solo_lcs2

    time_base = 0.003

    with update_elapsed_time(time_base, timedelta(0)):
        timeoffset = test_S2A(solo_lcs1, solo_lcs2,
                              convreg1_noadmin,
                              convreg2_prereg, convreg2_noadmin,
                              convreg2_first_noadmin,
                              solo_conv1_notifyAddr,
                              update_lcs2_added_noadmin,
                              update_lcs2_removed_noadmin)

    with update_elapsed_time(time_base, timeoffset):
        verify_io(lcs1.got_convention_deregister(convdereg_lcs2_prereg),
                  [ Sends(convdereg_lcs1) >= lcs2.myAddress,
                    (LostRemote, None),
                    Sends(update_lcs2_removed_noadmin) >= solo_conv1_notifyAddr,
                    (HysteresisCancel, None),
                  ], echo=True)

        verify_io(lcs2.got_convention_deregister(convdereg_lcs1),
                  [ (LostRemote, None),
                    (HysteresisCancel, None),
                  ])

    # Time passes, both sides are idle

    timeoffset = ((CONVENTION_REREGISTRATION_PERIOD *
                   CONVENTION_REGISTRATION_MISS_MAX) +
                  timedelta(seconds=100))

    for each in range(3):
        timeoffset += CONVENTION_REREGISTRATION_PERIOD
        with update_elapsed_time(time_base, timeoffset):
            verify_io(lcs1.check_convention(), [])
            verify_io(lcs2.check_convention(), [])

    # But a new pre-register gets them both talking again

    with update_elapsed_time(time_base, timeoffset):
        test_S2A(solo_lcs1, solo_lcs2,
                 convreg1_noadmin,
                 convreg2_prereg, convreg2_noadmin, convreg2_first_noadmin,
                 solo_conv1_notifyAddr,
                 update_lcs2_added_noadmin, update_lcs2_removed_noadmin,
                 time_offset = timeoffset + timedelta(seconds=30))



## ############################################################
## Support and validation
## ############################################################

class Sends(object):
    """Convenience argument for including in the validation list passed to
       verify_io.  The latter expects an array of tuples, the first
       being a message type and the second being a validation function
       taking the actual message and the target address.

           verify_io(operation,
                     [ ..., Sends(class_or_instance) >= targetAddr, ... ])

       Sends overloads the '>=' to imply "sends-to' and when called,
       verifies that the class_or_instance matches the first argument,
       and the targetAddr (if specified) matches the second argument.
       It also acts as a tuple returning the class as the first tuple
       entry and itself as the second tuple entry.
    """
    def __init__(self, msg):
        self.msg = msg
    def __ge__(self, target_addr):
        self.tgt = target_addr
        return self
    def __call__(self, actual_msg, actual_target):
        if inspect.isclass(self.msg):
            assert isinstance(actual_msg, self.msg), \
                "%s is not an instance of %s" % (actual_msg, self.msg)
        else:
            assert self.msg == actual_msg, \
                "Expected send of %s but actually sent %s" % (self.msg, actual_msg)
        if hasattr(self, 'tgt'):
            assert self.tgt == actual_target, \
                "Expected send to %s but actually sent to %s" % (self.tgt,
                                                                 actual_target)
        return True
    def __getitem__(self, idx):
        if idx == 0:
            return self.msg if inspect.isclass(self.msg) else self.msg.__class__
        if idx == 1:
            return self
        raise IndexError



def verify_normal_notification_updates(lcs1, lcs2, convreg1, convreg2):

    # Convention member generates periodic registrations to the leader
    # (i.e. keepalive) and the leader responds accordingly.

    verify_io(lcs1.got_convention_register(convreg2), [Sends(convreg1) >= lcs2.myAddress])
    verify_io(lcs2.got_convention_register(convreg1), [])

    # Twice, just to make sure it responds the same way each time
    verify_io(lcs1.got_convention_register(convreg2), [Sends(convreg1) >= lcs2.myAddress])
    verify_io(lcs2.got_convention_register(convreg1), [])

    
def verify_io(iolist, expected, any_order=False, echo=False):
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
