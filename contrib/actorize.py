"""
Define handlers using decorators.
Unhandled messages get passed up the inheritance hierarchy.

Usage:

@Actorize
class Hello(Actor):

    @handle(SomeMessageClass)
    def somemethod(self, msg, sender):
        pass


"""

import inspect

from thespian.actors import Actor

def Actorize(cls):

    def receiveMessage(self, message, sender):
        klass = message.__class__
        registry = self._msg_registry
        if not klass in registry:
             for k in registry:
                if isinstance(message, k):
                    registry[klass] = registry[k]
                    break
        method = registry.get(klass, None)
        if method is not None:
            method(self, message, sender)
        else:
            super(cls, self).receiveMessage(message, sender)

    # make sure cls is a subclass of Actor
    assert issubclass(cls, Actor), "@Actorize'd objects must be a subclass of Actor"
    #   someday, just add it ourselves; fails now for unknown reason:
    #   cls.__class__ = type(cls.__name__, (cls, Actor), {})

    # make sure we can monkeypatch receiveMessage safely
    assert 'receiveMessage' in vars(cls), "@Actorize'd objects must not define receiveMessage"
    cls.receiveMessage = receiveMessage

    # inheritance makes this possible (subclass of a decorated class)
    if not hasattr(cls, '_msg_registry'):
        cls._msg_registry = {}

    # gather up all the handle-marked methods into the registry
    for name, method in inspect.getmembers(cls, predicate=inspect.ismethod):
        if hasattr(method, 'handles'):
            for klass in method.handles:
                cls._msg_registry[klass] = method

    return cls


def handle(klass):
    def wrapper(f):
        f.handles = getattr(f, 'handles', []) + [ klass ]
        return f
    return wrapper



