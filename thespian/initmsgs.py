"""Provides the "initializing_messages" decorator that can be applied
to Actor definitions.  This decorator is used to specify a set of
initialization messages that the Actor should receive before beginning
normal operations.

This pattern is especially useful for Actors which should receive
multiple different messages to initialize their state before normal
processing begins.  Each message is stored in a corresponding
attribute on the Actor class, and (unless init_passthru is True) no
messages are delivered to the receiveMessages entrypoint until all of
the initialization messages have been received.

Example:

    @initializing_messages([('init_m1', Msg1, True),
                            ('init_tgt', ActorAddress),
                            ('frog', str)],
                            initdone='init_completed')
    class FooActor(ActorTypeDispatcher):
        def init_completed(self):
            self.send(self.init_m1_sender, 'Running')
        def receiveMsg_Croak(self, croakmsg, sender):
            self.send(sender, self.frog)
            self.send(self.init_m1_sender, self.init_m1)
            self.send(self.init_tgt, croakmsg)

This actor will wait for three different initialization messages
(Msg1, an ActorAddress, and a string) and store each as an attribute,
as well as storing the sender address for the Msg1 message.  When all
three have been received, the init_completed method is called, and
then all Croak messages cause three messages to be sent.  Note that
any Croak messages received before all three initialization messages
will be stored and delivered after initialization is complete.

This decorator is incompatible with the @troupe decorator.

This decorator must appear before the @transient or @transient_idle
decorator:

    @initializing_messages([('init_m1', Msg1, True),
                            ('init_tgt', ActorAddress),
                            ('frog', str)],
                            initdone='init_completed')
    @transient(timedelta(seconds=2, milliseconds=250))
    class FooActor(ActorTypeDispatcher): ...
"""

import thespian.actors
import logging


def initializing_messages(initset, initdone=None, init_passthru=False):

    """The initset is a list of tuples, where the first tuple value is the
       name of an attribute to be set on this actor and the second
       tuple value is the type of the corresponding message.  If the
       tuple has three entries and the third is True, then when the
       message type is matched, an attribute corresponding to the
       first element plus the suffix "_sender" is set to the sender of
       that message.

       If multiple initialization messages of the same type are
       received, the corresponding attribute will be set to the last
       messages received.

       The initdone argument, if set, specifies a method name (as a
       string) that should be called when all initialization message
       have been received.

       The init_passthru can be set to True to pass *all* messages
       received during initialization to the normal Actor handling;
       the default is false and init messages are discarded and
       non-init messages are saved and delivered after initialization
       is completed.  (All ActorSystemMessage messages are treated as
       if init_passthru was True).

    """
    def require_init(self, message, sender):
        done = True
        found = False
        for each in initset:
            if isinstance(message, each[1]):
                found = True
                setattr(self, each[0], message)
                if len(each) >= 3 and each[2]:
                    setattr(self, each[0] + '_sender', sender)
            done = done and hasattr(self, each[0])
        if init_passthru or isinstance(message, thespian.actors.ActorSystemMessage):
            self._post_init_receiveMessage(message, sender)
        else:
            if not found:
                self._post_init_pending.append( (message, sender) )
        if done:
            self.receiveMessage = self._post_init_receiveMessage
            if initdone:
                if hasattr(self, initdone):
                    getattr(self, initdone)()
                else:
                    logging.log('The initdone method "%s" was not present on Actor %s',
                                initdone, self.__class__.__name__,
                                level=logging.WARNING)
            # n.b. preserve and update to be able to continue and
            # complete this in case of exception
            while self._post_init_pending:
                nxt = self._post_init_pending.pop(0)
                self.receiveMessage(*nxt)
    def _i_msgs(actor_class):
        actor_class._post_init_pending = []
        actor_class._post_init_receiveMessage = actor_class.receiveMessage
        actor_class.receiveMessage = require_init
        return actor_class
    return _i_msgs

