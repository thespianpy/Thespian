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
    verify_io(ret.setup_convention(), [])
    return ret


@fixture
def lcs2():
    ret = LocalConventionState(ActorAddress(2),
                                {'Admin Port': 2,
                                 'Convention Address.IPv4': ActorAddress(1),
                                 'apple pie': 'hot'},
                                StatsManager(),
                               lambda x: ActorAddress(1))
    ret._expected_setup_convreg = ConventionRegister(ActorAddress(2),
                                                     ret.capabilities,
                                                     firstTime=True,
                                                     preRegister=False)
    # Activate the system
    verify_io(ret.setup_convention(),
              [ (ConventionRegister,
                 lambda r,a: (r == ret._expected_setup_convreg and
                              a == ActorAddress(1))),
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


def test_prereg_reg(solo_lcs1, solo_lcs2):

    lcs1, lcs2 = solo_lcs1, solo_lcs2

    # This test sends a pre-registration to lcs1 for lcs2, which
    # should cause lcs1 to actually register with lcs2 and lcs2 to
    # retro-register with its actual data.
    lcs1_prereg_2_convreg = ConventionRegister(lcs2.myAddress,
                                               lcs2.capabilities,
                                               firstTime=False,
                                               preRegister=True)
    lcs1_to_2_convreg_first = ConventionRegister(lcs1.myAddress,
                                                 lcs1.capabilities,
                                                 firstTime=True,
                                                 preRegister=False)
    lcs2_to_1_convreg = ConventionRegister(lcs2.myAddress,
                                           lcs2.capabilities,
                                           firstTime=False,
                                           preRegister=False)
    lcs1_to_2_convreg = ConventionRegister(lcs1.myAddress,
                                           lcs1.capabilities,
                                           firstTime=False,
                                           preRegister=False)

    # Pre-register lcs2 with lcs1 and verify lcs1 sends its own info
    # to lcs2.  The registration indicated pre-registration but not an
    # assertion of first time (which would cause all existing remote
    # information to be dropped and all remote actors to be shutdown)
    # because this system may already know about the remote.  In this
    # scenario, lcs1 does not know about lcs2, so it should set the
    # first time indication on the info sent to lcs2.

    verify_io(lcs1.got_convention_register(lcs1_prereg_2_convreg),
              [ (LostRemote, None),
                (HysteresisCancel, None),
                (ConventionRegister,
                 lambda r,a: (r == lcs1_to_2_convreg_first and
                              a == lcs2.myAddress)),
              ])

    # lcs2 gets the ConventionRegister generated above, and responds
    # with actual info of its own.  If the other side is indicating
    # firstTime, that means it has no previous knowledge; this side
    # should not also set firstTime or that will bounce back and forth
    # indefinitely.  Note that this side will perform a transport
    # reset (LostRemote and HysteresisCancel); the TCPTransport may
    # ignore the transport reset for TXOnly addresses.

    verify_io(lcs2.got_convention_register(lcs1_to_2_convreg_first),
              [ (LostRemote, None),
                (HysteresisCancel, None),
                (ConventionRegister,
                 lambda r,a: (r == lcs2_to_1_convreg and a == lcs1.myAddress)),
              ])

    # lcs1 gets full ConventionRegister from lcs2
    assert [] == lcs1.got_convention_register(lcs2_to_1_convreg)

    # Additional registration events are ignored
    assert [] == lcs2.got_convention_register(lcs1_to_2_convreg)
    assert [] == lcs1.got_convention_register(lcs2_to_1_convreg)
    assert [] == lcs2.got_convention_register(lcs1_to_2_convreg)
    assert [] == lcs1.got_convention_register(lcs2_to_1_convreg)



def test_prereg_reg_with_notifications(solo_lcs1, solo_lcs2):
    lcs1, lcs2 = solo_lcs1, solo_lcs2

    notifyAddr = ActorAddress('notify')

    lcs1.add_notification_handler(notifyAddr)

    lcs1_prereg_2_convreg = ConventionRegister(
        lcs2.myAddress,
        {'Admin Port': lcs2.capabilities['Admin Port']},
        firstTime=False,
        preRegister=True)
    lcs1_to_2_convreg_first = ConventionRegister(lcs1.myAddress,
                                                 lcs1.capabilities,
                                                 firstTime=True,
                                                 preRegister=False)
    lcs2_to_1_convreg = ConventionRegister(lcs2.myAddress,
                                           lcs2.capabilities,
                                           firstTime=False,
                                           preRegister=False)
    lcs1_to_2_convreg = ConventionRegister(lcs1.myAddress,
                                           lcs1.capabilities,
                                           firstTime=False,
                                           preRegister=False)

    verify_io(lcs1.got_convention_register(lcs1_prereg_2_convreg),
              [ (LostRemote, None),
                (HysteresisCancel, None),
                (ConventionRegister,
                 lambda r,a: (r == lcs1_to_2_convreg_first and
                              a == lcs2.myAddress)),
              ])

    # lcs2 gets the ConventionRegister generated above, and responds
    # with actual info of its own.  If the other side is indicating
    # firstTime, that means it has no previous knowledge; this side
    # should not also set firstTime or that will bounce back and forth
    # indefinitely.  Note that this side will perform a transport
    # reset (LostRemote and HysteresisCancel); the TCPTransport may
    # ignore the transport reset for TXOnly addresses.

    verify_io(lcs2.got_convention_register(lcs1_to_2_convreg_first),
              [ (LostRemote, None),
                (HysteresisCancel, None),
                (ConventionRegister, lambda r, a: (r == lcs2_to_1_convreg and
                                                   a == lcs1.myAddress)),
              ])

    # lcs1 gets full ConventionRegister from lcs2.  This should also
    # cause an update notification with the full specification.
    verify_io(lcs1.got_convention_register(lcs2_to_1_convreg),
              [ (ActorSystemConventionUpdate,
                 lambda r, a: (
                     r == ActorSystemConventionUpdate(
                         lcs2.myAddress,
                         lcs2.capabilities,
                         added=True) and
                     a == notifyAddr)),
              ])

    # Additional registration events are ignored
    assert [] == lcs2.got_convention_register(lcs1_to_2_convreg)
    assert [] == lcs1.got_convention_register(lcs2_to_1_convreg)
    assert [] == lcs2.got_convention_register(lcs1_to_2_convreg)
    assert [] == lcs1.got_convention_register(lcs2_to_1_convreg)


def test_reg_with_notifications(lcs1, lcs2):
    notifyAddr = ActorAddress('notify')

    lcs1.add_notification_handler(notifyAddr)


    lcs2_to_1_convreg_first = lcs2._expected_setup_convreg
    lcs1_to_2_convreg_first = ConventionRegister(lcs1.myAddress,
                                                 lcs1.capabilities,
                                                 firstTime=False,
                                                 preRegister=False)
    lcs2_to_1_convreg = ConventionRegister(lcs2.myAddress,
                                           lcs2.capabilities,
                                           firstTime=False,
                                           preRegister=False)
    lcs1_to_2_convreg = ConventionRegister(lcs1.myAddress,
                                           lcs1.capabilities,
                                           firstTime=False,
                                           preRegister=False)

    verify_io(lcs1.got_convention_register(lcs2_to_1_convreg_first),
              [ (LostRemote, None),
                (HysteresisCancel, None),
                (ConventionRegister,
                 lambda r, a: (r == lcs1_to_2_convreg_first and
                               a == lcs2.myAddress)),
                (ActorSystemConventionUpdate,
                 lambda r, a: (r == ActorSystemConventionUpdate(
                     lcs2.myAddress,
                     lcs2.capabilities,
                     added=True) and
                               a == notifyAddr)),
              ])

    verify_io(lcs2.got_convention_register(lcs1_to_2_convreg_first), [])

    # Additional registration events are ignored
    assert [] == lcs2.got_convention_register(lcs1_to_2_convreg)
    assert [] == lcs1.got_convention_register(lcs2_to_1_convreg)
    assert [] == lcs2.got_convention_register(lcs1_to_2_convreg)
    assert [] == lcs1.got_convention_register(lcs2_to_1_convreg)

    assert [] == lcs1.check_convention()
    assert [] == lcs2.check_convention()

    return notifyAddr  # used by callers


def test_reg_dereg_with_notifications(lcs1, lcs2):

    # Setup both in the registered condition
    notifyAddr = test_reg_with_notifications(lcs1, lcs2)

    lcs2_to_1_convdereg = ConventionDeRegister(lcs2.myAddress,
                                               preRegistered=False)

    verify_io(lcs1.got_convention_deregister(lcs2_to_1_convdereg),
              [ (LostRemote, None),
                (ActorSystemConventionUpdate,
                 lambda r, a: (r == ActorSystemConventionUpdate(
                     lcs2.myAddress,
                     lcs2.capabilities,
                     added=False)
                               and a == notifyAddr
                 )),
                (HysteresisCancel, None),
              ])


def test_reg_dereg_rereg_with_notifications(lcs1, lcs2):

    # Setup both in the registered condition
    notifyAddr = test_reg_with_notifications(lcs1, lcs2)

    lcs2_to_1_convdereg = ConventionDeRegister(lcs2.myAddress,
                                               preRegistered=False)

    verify_io(lcs1.got_convention_deregister(lcs2_to_1_convdereg),
              [ (LostRemote, None),
                (ActorSystemConventionUpdate,
                 lambda r, a: (r == ActorSystemConventionUpdate(
                     lcs2.myAddress,
                     lcs2.capabilities,
                     added=False))
                 and a == notifyAddr
                ),
                (HysteresisCancel, None),
              ])

    test_reg_with_notifications(lcs1, lcs2)


def test_reg_with_multiple_notifications(lcs1, lcs2):

    notifyAddr1 = ActorAddress('notify1')
    lcs1.add_notification_handler(notifyAddr1)

    notifyAddr2 = ActorAddress('notify2')
    lcs1.add_notification_handler(notifyAddr2)

    notifyAddr3 = ActorAddress('notify3')
    lcs1.add_notification_handler(notifyAddr3)

    # KWQ: lcs2.add_notification_handler .. never gets called


    lcs2_to_1_convreg_first = lcs2._expected_setup_convreg
    lcs1_to_2_convreg_first = ConventionRegister(lcs1.myAddress,
                                                 lcs1.capabilities,
                                                 firstTime=False,
                                                 preRegister=False)
    lcs2_to_1_convreg = ConventionRegister(lcs2.myAddress,
                                           lcs2.capabilities,
                                           firstTime=False,
                                           preRegister=False)
    lcs1_to_2_convreg = ConventionRegister(lcs1.myAddress,
                                           lcs1.capabilities,
                                           firstTime=False,
                                           preRegister=False)
    notificationUp = ActorSystemConventionUpdate(lcs2.myAddress,
                                                 lcs2.capabilities,
                                                 added=True)

    verify_io(lcs1.got_convention_register(lcs2_to_1_convreg_first),
              [ (LostRemote, None),
                (HysteresisCancel, None),
                (ConventionRegister,
                 lambda r, a: (r == lcs1_to_2_convreg_first and
                               a == lcs2.myAddress)),
                (ActorSystemConventionUpdate,
                 lambda r, a: (r == notificationUp and a == notifyAddr1)),
                (ActorSystemConventionUpdate,
                 lambda r, a: (r == notificationUp and a == notifyAddr2)),
                (ActorSystemConventionUpdate,
                 lambda r, a: (r == notificationUp and a == notifyAddr3)),
              ])

    verify_io(lcs2.got_convention_register(lcs1_to_2_convreg_first), [])

    # Additional registration events are ignored
    assert [] == lcs2.got_convention_register(lcs1_to_2_convreg)
    assert [] == lcs1.got_convention_register(lcs2_to_1_convreg)
    assert [] == lcs2.got_convention_register(lcs1_to_2_convreg)
    assert [] == lcs1.got_convention_register(lcs2_to_1_convreg)

    lcs2_to_1_convdereg = ConventionDeRegister(lcs2.myAddress,
                                               preRegistered=False)
    notificationDown = ActorSystemConventionUpdate(lcs2.myAddress,
                                                   lcs2.capabilities,
                                                   added=False)

    verify_io(lcs1.got_convention_deregister(lcs2_to_1_convdereg),
              [ (LostRemote, None),
                (ActorSystemConventionUpdate,
                 lambda r, a: (r == notificationDown and a == notifyAddr1)),
                (ActorSystemConventionUpdate,
                 lambda r, a: (r == notificationDown and a == notifyAddr2)),
                (ActorSystemConventionUpdate,
                 lambda r, a: (r == notificationDown and a == notifyAddr3)),
                (HysteresisCancel, None),
              ])


@mark.skipif(not patch, reason='requires mock patch')
def test_reg_dereg_rereg_with_delay_and_updates(lcs1, lcs2):

    # Setup both in the registered condition
    notifyAddr = test_reg_with_notifications(lcs1, lcs2)

    # Now add some elapsed time and check update messages

    lcs2_rereg = ConventionRegister(lcs2.myAddress,
                                    lcs2.capabilities,
                                    preRegister=False,
                                    firstTime=False)

    lcs2_exited_update = ActorSystemConventionUpdate(
        lcs2.myAddress,
        lcs2.capabilities,
        added=False)

    with patch('thespian.system.utilis.datetime') as p_datetime:
        p_datetime.now.return_value = (
            datetime.now() +
            CONVENTION_REREGISTRATION_PERIOD +
            timedelta(seconds=1))
        # Convention leader does not take action
        verify_io(lcs1.check_convention(), [])
        # But convention member should re-up their membership
        verify_io(lcs2.check_convention(),
                  [(ConventionRegister,
                    lambda r, a: r == lcs2_rereg and a == lcs1.myAddress),
                   (LogAggregator, None),
                  ])

    # Too long
    with patch('thespian.system.utilis.datetime') as p_datetime:
        p_datetime.now.return_value = (
            datetime.now() +
            (CONVENTION_REREGISTRATION_PERIOD *
             CONVENTION_REGISTRATION_MISS_MAX) +
            timedelta(seconds=1))
        # Convention leader does not take action
        verify_io(lcs1.check_convention(),
                  [ (LostRemote, None),
                    (ActorSystemConventionUpdate,
                     lambda r, a: (r == lcs2_exited_update and
                                   a == notifyAddr)),
                    (HysteresisCancel, None),
                  ])
        # But convention member should re-up their membership
        verify_io(lcs2.check_convention(),
                  [(ConventionRegister,
                    lambda r, a: r == lcs2_rereg and a == lcs1.myAddress),
                   (LogAggregator, None),
                  ])

    lcs2_to_1_convdereg = ConventionDeRegister(lcs2.myAddress,
                                               preRegistered=False)

    verify_io(lcs1.got_convention_deregister(lcs2_to_1_convdereg),
              [ (LostRemote, None),
                (HysteresisCancel, None),
              ])

    test_reg_with_notifications(lcs1, lcs2)


def verify_io(iolist, expected, any_order=False, echo=True):
    if echo:
        print(fmap(str, iolist))
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

