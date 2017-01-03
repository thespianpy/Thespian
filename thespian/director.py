#!/usr/bin/env python

## The Director is optional functionality that can be used with the
## Thespian environment.  The Director provides a management facility
## for managing and coordinating between loadable sources.  It is
## possible to use Thespian without using the Director, and it is
## possible to use Thespian and loadable sources with alternate
## functionality instead of the Director.  The Director is a
## convenience utility that can help make it easier to use loadable
## sources with Thespian.


"""The Thespian Director is a set of optional functionality designed
to help with environments that are using loadable sources.  In these
environments, it is common to update a loaded source with a newer
version, and there may be multiple different sources loaded at any one
time.  The Director module helps to load and manage those different
sources (in conjuction with the Source Authority).

The Director consists of an actor with a globalName of "Director", and
a command-line utility (invoked by running "$ python
thespian/director.py [cmd ...]") that can be used to interact with the
Director actor to load and unload sources and manage actors associated
with those loadable sources.  The Director actor also functions as a
Thespian Source Authority which validates that loaded sources are
signed with one of the keys present in the .tskey files.

Loaded sources are arranged into "groups".  There may be multiple
versions of a loaded source file within a group, but only one loaded
source in the group is considered to be the "current" or "active"
member of the group (defaulting to the last succesfully loaded source
in the group).

All messages to and from the Director are Python dictionaries, and
their members are all part of the standard built-in types to allow
them to be exchanged with loaded modules without depending on
definitions encapsulated by source loading constraints.  All messages
to the Director specify a DirectorOp entry whose value is a string
defining what operation is being requested; all responses from the
Director specify a DirectorResponse entry whose value is a string
defining the response type and a Success entry whose value is True or
False indicating the success or failure of the requested operation.

Some messages fields contain optional fields.  These fields are
considered to be specified if they are present and not blank or empty;
a blank or empty value is the same as the field not being present at
all.

The Director handles the messages:

  * DeclareGroup message

    { "DirectorOp": "DefineGroup",
      "Group" : "group-name",
      "Actors" : {
          actor-class-name : {
              "OnLoad" : {
                  "Role" : "optional role of this actor in this group",
                  "GlobalName": "optional global name for started actor",
                  "Message": optional-message,
              },
              "OnDeactivate" : {
                  "Message": optional-message,
              },
              "OnReactivate" : {
                  "Message": optional-message,
              },
          ],
        },
      "AutoUnload" : True or False or positive-int,
    }

    This message is used to declare a Group, which roughly correlates
    to a specific loadable source: there may be multiple members of
    the Group but each member is assumed to be a different version of
    the same source.  There is only one "active" or "current" member
    of a Group; by default this is the most recently loaded member.

    The Actors element specifies the class names of Actors that are to
    be created once the load completes.  The OnLoad provides
    additional characteristics of those started actors: the initial
    Message to send the actor after startup (if any), the optional
    globalName to use when creating the actor, and the internal
    Role name.

    The Role is the name this actor operates under within the
    group; this name is stored internally to the Directory and the
    Director can be queried regarding the Role name.

    The Message field specifies an optional message to be sent to the
    Actor on startup/activation or on deactivation.  The message is a
    Python object, and can reference items in the "thespian.actors",
    "sys", and "os" module namespace.

    The OnDeactivate defines actions that are to be taken when a
    loaded source is replaced by a new active version: if a Message
    entry is specified, the corresponding actor will be sent that
    message.  The OnDeactivate actions will be performed for the older
    loaded source and then the new actor instances are created from
    the newly active source load; this allows any GlobalName actors
    running in the old source context to exit and be replaced by their
    corresponding versions in the newly loaded source.

    Note that if the OnLoad specifies a GlobalName and the
    OnDeactivate does not cause the previously running instance to
    exit that the previous Actor existing at the globalName will
    prevent the OnLoad version from being created.  The GlobalName
    may be None, blank, or the field may be omitted if no global name
    is to be assigned to the started actor.

    If a loaded source became de-activated by loading a new active
    version, but then that new active version is unloaded, the
    original loaded source becomes re-activated; in that scenario, the
    OnReactivate section specifies actions to be taken when the loaded
    source becomes re-activated.

    If the optional "AutoUnload" parameter is specified and is True,
    then this older source is unloaded when it is deactivated (after
    any and all deactivate Messages are delivered).  If the AutoUnload
    parameter is a positive, non-zero integer, it specifies the
    maximum number of loaded source instances that should be active;
    if loading a new source exceeds this count, the oldest loaded
    source is unloaded.

    Note that unloading a loaded source will cause an ActorExitRequest
    to be sent to all actors still running from that source.  If the
    Actors that were sent messages for the "OnDeactivate" event cannot
    exit immediately and require additional time should have "Unload"
    set to False and are responsible for calling the
    ~self.unloadActorSource()~ call with their source hash at whatever
    future point the unload should be performed.

    The Director will automatically restart any actors in the current
    active load for each group that exit, and send them the OnLoad
    message (if any) when restarted.

    If the Group declaration is updated by a new DeclareGroup message
    for the same Group name, the new definition replaces the old and
    applies to all *subsequent* LoadSource activity within the group:
    the OnLoad and OnDeactivate sections that were in place for any
    currently loaded sources are still used for those older loaded
    sources.

    The Director's response to the DeclareGroup message is:

    { "DirectorResponse": "DeclaredGroup"
      "Success": True,
      "Message": "optional-message-especially-if-success-is-false"
    }


  * LoadSource message

    { "DirectorOp": "LoadSource",
      "Source": "/path/to/sourcefile",
      "Group": "optional-group-name",
    }

    This message is used to specify that a new version of a loadable
    source file is to be loaded for the specified group.  The groups
    declared OnLoad operations will be run, after any previously
    active version's OnDeactivate operations are run.  If the Group
    specified does not exist, it is automatically created with no
    OnLoad or OnDeactivate sections.

    Upon successful load, the OnDeactivate section of the previously
    active instance in the Group is executed, followed by the OnLoad
    section of this newly loaded and now active source.

    The Director's response to the LoadSource message is:

    { "DirectorResponse": "SourceLoading"
      "Success": True,
      "Message": "optional-message-especially-if-success-is-false",
      "SourceHash": "source-hash-string-if-success-is-true"
    }

    Note that this response does *not* indicate that the source has
    been validated and is now active, only that the source load was
    initiated and what the corresponding source hash is.  If Success
    is False, no SourceHash entry is provided.

  * RetrieveAll message

    { "DirectorOp": "RetrieveAll",
    }

    This operation retrieves all source hashes and actor addresses
    registered with the Director.  If the input Group is None, blank,
    or not provided then all groups are returned, otherwise only the
    requested group is returned.

    The Director's response to the RetrieveAll message is:

    { "DirectorResponse": "AllAddresses",
      "Success": True,
      "Groups": { "group-name" :
                       { "ActiveHash": "active-source-hash",
                         "Loaded": [ "most-recent-souce-hash", ...,
                                     "oldest-source-hash" ],
                         "Running": { "source-hash": [{
                                        "ActorClass": "actor-class-name",
                                        "Role": "actor-role-name",
                                        "ActorAddress": address,
                                     }],
                                     ...
                                   }
                       }
                }
    }

    An empty dictionary of Groups is returned if there are no matching
    registrations).

  * RetrieveRole message

    { "DirectorOp": "RetrieveRole",
      "Group": "optional-group-name",
      "Role": "role-name",
    }

    This message is an optimized version of the RetrieveAll: it
    returns only the information for the active actor in the specified
    Role and optional Group.  If the Group is not specified, the first
    active actor matching the specified Role is returned.  This same
    information is available in the RetrieveAll response, but this
    operation returns the filtered response for only the specified
    role.

    The Director's response to the RetrieveRole message is:

    { "DirectorResponse": "RoleAddress",
      "Success": True,
      "Group": "group-name",
      "SourceHash": "source-hash",
      "ActorClass": "actor-class-name",
      "Role": "role-name",
      "ActorAddress": address,
    }

    If the group name or source hash is invalid, this returns Success
    as False and None as the ActorAddress and ActorClss values instead
    of any useable values.

  * RequestNotification message

    { "DirectorOp": "RequestNotification",
    }

    This operation requests the Director to notify the sender of the
    next active change---when a new hash within a group becomes active
    because of a new source load or unload.

    This request is single-shot: once a change occurs and the
    notification is sent, the notification request is forgotten.  If
    the sender wishes to be notified about future changes, it must
    send another RequestNotification message.  There can be multiple
    remotes that have simultaneously registered for notification by
    the Director and the notification will be send to all of them upon
    an active change.

    There is no immediate response to this operation, but at some
    future point the Director will send the NotificationOfUpdate
    message:

    { "DirectorResponse": "NotificationOfUpdate",
      "Success": True,
    }

    This response indicates to the sender that there was an update,
    but not what the update was; the sender must query the Director to
    get the current information.

"""

import os
import sys
import thespian.actors
from collections import defaultdict
import logging


class Director(thespian.actors.ActorTypeDispatcher):
    "This is the main Director Actor providing the Director functionality."

    def __init__(self, *args, **kw):
        super(Director, self).__init__(*args, **kw)
        self.groups = {}  # key = group name, value = DefineGroup dict
        self.pending_loads = {} # key = sourceHash, value = group_name
        self.loaded = defaultdict(list) # key = group_name, value = [LoadedSourceInfo]
        self.active = defaultdict(lambda: None) # key = group_name, value = sourceHash
        self.watching_sources = False
        self.notification_reqs = [] # array of requestor addresses

    def receiveMsg_dict(self, dictmsg, sender):
        getattr(self, dictmsg.get('DirectorOp', 'no_op_specified'),
                self.no_handler)(dictmsg, sender)

    def no_op_specified(self, m ,s):
        logging.warning('Director received invalid request: %s', m)

    def no_handler(self, m, s):
        logging.warning('Director does not handle the %s request',
                        m['DirectorOp'])

    # def receiveUnrecognizedMessage(self, m, s):
    #     logging.warning('Director received unrecognized message: %s', m)

    def RequestNotification(self, msg, sender):
        if sender not in self.notification_reqs:
            self.notification_reqs.append(sender)

    def DefineGroup(self, msg, sender):
        self.groups[msg['Group']] = {
            'Actors': defaultdict(dict),
            'AutoUnload': msg.get('AutoUnload', False),
        }
        # Convert input to defaultdicts to make subsequent use easier
        if 'Actors' in msg:
            for actor in msg['Actors']:
                a = defaultdict(lambda: defaultdict(lambda: None))
                for k in msg['Actors'][actor]:
                    a[k] = defaultdict(lambda: None)
                    for sk in msg['Actors'][actor][k]:
                        a[k][sk] = msg['Actors'][actor][k][sk]
                self.groups[msg['Group']]['Actors'][actor] = a
        self.send(sender, {'DirectorResponse': 'DeclaredGroup',
                           'Success': True,
        })

    def LoadSource(self, msg, sender):
        if not self.watching_sources:
            self.notifyOnSourceAvailability()
            self.watching_sources = True
        sourceHash = self.loadActorSource(msg['Source'])
        self.pending_loads[sourceHash] = msg['Group']
        self.send(sender, {'DirectorResponse': 'SourceLoading',
                           'Success': True,
                           'SourceHash': sourceHash,
        })

    class LoadedSourceInfo(object):
        def __init__(self, source_hash, source_info, auto_unload):
            self.source_hash = source_hash
            self.source_info = source_info
            self.auto_unload = auto_unload
            self.actors = []
        def add_actor(self, running_actor_info):
            self.actors.append(running_actor_info)


    class RunningActorInfo(object):
        def __init__(self, address, role, globalname, classname,
                     activate_msg, deactivate_msg, reactivate_msg):
            self.address = address
            self.role = role
            self.global_name = globalname
            self.classname = classname
            self.activate_msg = activate_msg
            self.deactivate_msg = deactivate_msg
            self.reactivate_msg = reactivate_msg

    def receiveMsg_LoadedSource(self, loaded_msg, sender):
        group_name = self.pending_loads.pop(loaded_msg.sourceHash, None)
        if not group_name:
            # Some other source load not requested by the Director; ignore it
            return
        lsi = Director.LoadedSourceInfo(
            loaded_msg.sourceHash,
            loaded_msg.sourceInfo,
            self.groups[group_name]['AutoUnload'])
        active_hash = self.active[group_name]
        for loaded in self.loaded[group_name]:
            if loaded.source_hash == active_hash:
                for actor in loaded.actors:
                    self.deactivate_actor(group_name, actor)
                break
        self.active[group_name] = loaded_msg.sourceHash

        for each in self.groups[group_name]['Actors']:
            agi = self.groups[group_name]['Actors'][each]
            onload = agi['OnLoad']
            lsi.add_actor(
                Director.RunningActorInfo(
                    address=self.start_actor(each, loaded_msg.sourceHash,
                                             onload['Role'],
                                             global_name=onload['GlobalName'],
                                             startmsg=onload['Message']),
                    role=onload['Role'],
                    globalname=onload['GlobalName'],
                    classname=each,
                    activate_msg=onload['Message'],
                    reactivate_msg=agi['OnReactivate']['Message'],
                    deactivate_msg=agi['OnDeactivate']['Message']))
        self.loaded[group_name].append(lsi)

        kept = 1 # just loaded instance
        for loaded in self.loaded[group_name][-2::-1]:
            unload = loaded.auto_unload \
                     if isinstance(loaded.auto_unload, bool) else \
                        (kept >= loaded.auto_unload)
            if unload:
                self.unloadActorSource(loaded.source_hash)
            else:
                kept += 1

        self.send_notifications()


    def send_notifications(self):
        for each in self.notification_reqs:
            self.send(each, { "DirectorResponse": "NotificationOfUpdate",
                              "Success": True,
            })
        self.notification_reqs = []


    def start_actor(self, actor_class, source_hash,
                    role=None,
                    global_name=None,
                    startmsg=None):
        actor = self.createActor(actor_class,
                                 sourceHash=source_hash,
                                 globalName=global_name)
        role = (' (%s)' % role) if role else ''
        if startmsg:
            logging.info('Sending startmsg to new %s%s @ %s',
                         actor_class, role, actor)
            self.send(actor, startmsg)
        else:
            logging.info('Started new %s%s @ %s', actor_class, role, actor)
        return actor

    def deactivate_actor(self, group_name, actor):
        if not actor.deactivate_msg:
            return
        logging.info('Deactivating %s%s @ %s in %s', actor.classname,
                     ' (%s)' % actor.classname if actor.classname else '',
                     actor.address, group_name)
        self.send(actor.address, actor.deactivate_msg)


    def receiveMsg_UnloadedSource(self, unload_msg, sender):
        self.pending_loads.pop(unload_msg.sourceHash, None)
        # Remove source from loaded list, shutting down all associated
        # actors.
        for group in self.loaded:
            for idx, loaded in enumerate(self.loaded[group]):
                if loaded.source_hash == unload_msg.sourceHash:
                    isActive = self.active[group] == unload_msg.sourceHash
                    for actor in loaded.actors:
                        if actor.address:
                            if isActive:
                                self.deactivate_actor(group, actor)
                            self.send(actor.address,
                                      thespian.actors.ActorExitRequest())
                    self.loaded[group].pop(idx)
                    # If the unloaded source was the active source,
                    # re-activate the most recent older source to take
                    # its place.
                    if self.loaded[group]:
                        new_active = self.loaded[group][-1]
                        self.active[group] = new_active.source_hash
                        # The actors that were started on the original
                        # load should still be present.  Restart any
                        # that did exit and inform all actors that are
                        # part of the newly active load that they are
                        # re-activated.
                        for actor in new_active.actors:
                            if actor.address:
                                self.send(actor.address,
                                          actor.reactivate_msg)
                            else:
                                actor.address = self.createActor(
                                    actor.classname,
                                    globalName=actor.global_name)
                                self.send(actor.address,
                                          actor.activate_msg)
                    else:
                        self.active[group] = None
                    self.send_notifications()
                    return


    def receiveMsg_ChildActorExited(self, exitmsg, sender):
        child = exitmsg.childAddress
        # If this actor was one of the currently active actors,
        # restart it and send its startup message again
        for group in self.active:
            for loaded in self.loaded[group]:
                if loaded.source_hash == self.active[group]:
                    for actor in loaded.actors:
                        if actor.address == child:
                            actor.address = self.start_actor(
                                actor.classname,
                                loaded.source_hash,
                                actor.role,
                                global_name=actor.global_name,
                                startmsg=actor.activate_msg)
                            # n.b. fall through for inactive cleanup;
                            # globalName can exist in multiple places.
        # Remove this actor from any and *all* non-active entries
        for group in self.loaded:
            for loaded in self.loaded[group]:
                for actor in loaded.actors:
                    if actor.address == child:
                        actor.address = None


    def RetrieveAll(self, msg, sender):
        self.send(sender,  {
            'DirectorResponse': 'AllAddresses',
            'Success': True,
            'Groups': {
                group: {
                    'ActiveHash': self.active[group],
                    'Loaded': [L.source_hash
                               for L in self.loaded[group]],
                    'Running': {
                        L.source_hash: [{'ActorClass': actor.classname,
                                         'Role': actor.role,
                                         'ActorAddress': actor.address}
                                        for actor in L.actors]
                        for L in self.loaded[group]
                    }
                }
                for group in ([msg['Group']]
                              if 'Group' in msg else self.groups.keys())
            }
        })

    def RetrieveRole(self, msg, sender):
        good = (lambda g: g == msg['Group']) \
               if 'Group' in msg and msg['Group'] else (lambda g: True)
        for group in self.loaded:
            if not good(group):
                continue
            for loaded in self.loaded[group]:
                if loaded.source_hash != self.active[group]:
                    continue
                for actor in loaded.actors:
                    if actor.role == msg['Role']:
                        self.send(sender, {
                            "DirectorResponse": "RoleAddress",
                            "Success": True,
                            "Group": group,
                            "SourceHash": loaded.source_hash,
                            "ActorClass": actor.classname,
                            "Role": actor.role,
                            "ActorAddress": actor.address,
                            })
                        return
        self.send(sender, {"DirectorResponse": "RoleAddress",
                           "Success": False,
                           "Group": msg.get('Group', None),
                           "SourceHash": None,
                           "ActorClass": None,
                            "Role": msg['Role'],
                           "ActorAddress": None,
        })


class SourceAuthority(thespian.actors.ActorTypeDispatcher):
    def receiveMsg_str(self, strmsg, sender):
        if strmsg.startswith('Startup:'):
            self.registerSourceAuthority()
            self.sources_dir = strmsg[len('Startup:'):]

    def receiveMsg_ValidateSource(self, msg, sender):
        for keyfile in glob.glob(os.path.join(self.sources_dir, '*_tls.key')):
            with open(keyfile, 'r') as keyf:
                public_key = keyf.read()
            sdata = SourceEncoding.tls_to_zip(msg.sourceData, public_key)
            if not sdata:
                continue
            logging.info('Validated loaded source %s (%s)', msg.sourceHash,
                         getattr(msg, 'sourceInfo', 'no-source-info'))
            if hasattr(msg, 'sourceInfo'):
                vresp = thespian.actors.ValidatedSource(msg.sourceHash,
                                                        sdata,
                                                        msg.sourceInfo)
            else:
                vresp = thespian.actors.ValidatedSource(msg.sourceHash,
                                                        sdata)
            self.send(sender, vresp)
            return
        logging.warning('Invalid Source not loaded (%s)', msg.sourceHash)


### ----------------------------------------------------------------------
### DirectorControl client
###

import glob
import shutil
from datetime import datetime, timedelta
import subprocess
import zipfile
import json
import time
import hashlib
from io import BytesIO


class DirectorControl(object):
    """\
The Director is controlled by specifying a command with optional
arguments.  Supported commands:

      help   -- show this help information
      verbose {cmd} -- command prefix enabling verbose output for cmd
      config -- output general configuration information
      bootstart -- update system config to start thespian + director at boot
      start    -- starts the actor system, director, source authority
                  and refreshes loaded sources.  Many other operations will
                  implicitly start if needed, but this is good to use at
                  startup time for a complete startup operation.
      shutdown -- shuts down the entire running Actor System
      avail  -- list available loadable source files
      gensrc -- generate a signed loadable source file
      tlsinfo -- show information or extract contents from a
                 signed loadable source file
      load -- load specified loadable source
      refresh -- ensure latest version of each group is loaded
      unload -- unloads a loaded source
      list -- list currently loaded sources

The THESPIAN_DIRECTOR_DIR environment variable can be set to specify
the location of loadable sources and configuration files on the
current system.  The default is "C:thespian/director" under Windows or
"/opt/thespian/director" under Linux, and the current value is:
"%(sources_dir)s".

Each configuration value has a reasonable default value that can be
overridden by using ".cfg" files in the THESPIAN_DIRECTOR_DIR.  A
separate file is used for each configuration value to make it easy to
update the configuration even if only crude tools are available for
the update.

    thespbase.cfg -- sets the Actor System Base to use.  The default is
                     "multiprocTCPBase". The current value is:
                     "%(system_base)s"

    oldthespbase.cfg -- this can be set to specify which old system
                        base to shutdown before starting the
                        thespbase.cfg-specified system.  This file is
                        only needed if updating to a new system base
                        type.

    adminport.cfg -- sets the Admin Port for the System Base (if
                     appropriate). The default is 1900. The current
                     value is %(admin_port)d.

    convleader.cfg -- Specifies a convention leader if this system
                      should actively join in a convention with the
                      leader.  If not specified, the local system will
                      not actively join a convention unless invited,
                      although it may be a convention leader.

    othercaps.cfg -- specifies other capabilities to set when
                     starting up the Actor System.

    thesplog.cfg -- specifies the logging configuration (in dict
                    format) for the logging performed by running
                    Actors.

    thesplogd.cfg -- specifies the director for thespian internal
                     logging.  The default is /var/log under Posix and
                     c:\Windows\temp under Windows.

    thesplogf.cfg -- specifies the name of the thespian internal
                     logging file.  The default is
                     "thespian_system.log".

    thesplogs.cfg -- specifies the maximum size of the thespian
                     internal logging file in bytes.  The default is
                     51200.

    """

    ask_wait = timedelta(seconds=5)

    def __init__(self, sources_dir=None, system_base=None, admin_port=None):
        """The sources_dir specifies the directory where the Director loadable
           sources and configuration files are found; if not specified
           the value of the THESPIAN_DIRECTOR_DIR is used, or if not
           set, C:thespian/director is used under Windows or else
           /opt/thespian/director is used.

           The system_base specifies the Actor System Base name to
           use; if not specified the contents of the thespbase.cfg
           file in the THESPIAN_DIRECTOR_DIR is used, or the default
           of 'multiprocTCPBase'.

           The admin_port specifies the admin port for the system base
           (if applicable); if not specified the contents of the
           adminport.cfg file in the THESPIAN_DIRECTOR_DIR is used, or
           the default of 1900.

        """
        self.sources_dir = sources_dir or os.getenv('THESPIAN_DIRECTOR_DIR',
                                                    'C:/thespian/director'
                                                    if os.name == 'nt' else
                                                    '/opt/thespian/director')
        self.system_base = system_base or \
                           self.filecfg('thespbase.cfg', 'multiprocTCPBase')

        try:
            self.admin_port = int(admin_port or
                                  self.filecfg('adminport.cfg', '1900'))
        except (ValueError, TypeError):
            self.admin_port = 1900

        self.verbose = lambda fmt, *args: None

    def filecfg(self, cfg_fname, default):
        try:
            with open(os.path.join(self.sources_dir, cfg_fname), 'r') as cf:
                return cf.read().strip()
        except Exception:
            return default

    def __call__(self, cmd, *args):
        if cmd == 'verbose':
            self.verbose = lambda fmt, *args: (
                sys.stdout.write(fmt % args) and
                sys.stdout.write('\n' if fmt[-1] != '\n' else '') and
                sys.stdout.flush())
            cmd = args[0]
            args = args[1:]

        if cmd == 'help':
            if args:
                import inspect
                print(inspect.getdoc(getattr(self, 'cmd_' + args[0],
                                             self.bad_cmd)))
            else:
                print(self.__doc__ % self.__dict__)
            return 0

        return getattr(self, 'cmd_'+cmd, self.bad_cmd)(*args)

    def bad_cmd(self, *args):
        'Unrecognized command'
        print('Unrecognized command;'
              ' use the "help" command to get usage information.')
        return 2


    def cmd_config(self):
        "Shows current running configuration information"
        print('   Sources location: %s' % self.sources_dir)
        print('        System Base: %s' % self.system_base)
        old_sytem_base = self.filecfg('oldthespbase.cfg', None)
        if old_sytem_base:
            print('    OLD System Base: %s' % old_sytem_base)
        print('         Admin Port: %s' % self.admin_port)
        print('  Logging directory: %s' % self.logdir)
        convleader = self.filecfg('convleader.cfg', None)
        if convleader:
            print('  Convention Leader: %s' % convleader)
        print(' Other Capabilities: %s' %
              ('\n' + ' '*21).join(self.filecfg('othercaps.cfg', '{}')
                                   .split('\n')))
        return 0


    @property
    def logdir(self):
        return self.filecfg('thesplogd.cfg', None) or \
            [P for P in [ 'C:\\Windows\\temp',
                          '/var/log/', os.getcwd() ]
             if os.path.isdir(P) and os.access(P, os.W_OK)][0]

    @property
    def asys(self):
        if not hasattr(self, '_asys'):
            logdir = self.logdir
            import logging.handlers
            class LFMT(logging.Formatter):
                def format(self, rec):
                    rec.setdefault('actorAddress', '--non-Actor--')
                    return super(LFMT, self).format(rec)
            logcfg = eval(str(
                self.filecfg('thesplog.cfg',
                             { 'version': 1,
                               'disable_existing_loggers': True,
                               'formatters': {
                                   'def': { 'format': '%(asctime)s %(levelname)-7s %(actorAddress)s  %(message)s [%(filename)s:%(lineno)s]',
                                   },
                               },
                               'handlers': {
                                   'debugFile': {
                                       'level': 'DEBUG',
                                       'class': 'logging.handlers.RotatingFileHandler',
                                       'filename': '%s/thespian_debug.log' % logdir,
                                       'maxBytes': 1024*1024*5, # 5MB
                                       'backupCount': 3,
                                       'formatter': 'def',
                                   },
                                   'mainFile': {
                                       'level': 'DEBUG',
                                       'class': 'logging.handlers.RotatingFileHandler',
                                       'filename': '%s/thespian.log' % logdir,
                                       'maxBytes': 1024*1024*5, # 5MB
                                       'backupCount': 3,
                                       'formatter': 'def',
                                   },
                               },
                               'loggers': {
                                   '': { 'handlers': ['mainFile', 'debugFile' ],
                                         'propagate': True
                                   },
                               },
                               'root' : { 'handlers': ['mainFile', 'debugFile' ],
                               },
                             })), globals(), {'logdir':logdir})

            capabilities = eval(self.filecfg('othercaps.cfg', '{}'))
            capabilities['Admin Port'] = self.admin_port
            capabilities['DirectorFmt'] = [1]
            convleader = self.filecfg('convleader.cfg', None)
            if convleader:
                capabilities['Convention Address.IPv4'] = convleader
            thesplogf = self.filecfg('thesplogf.cfg',
                                     os.path.join(self.sources_dir,
                                                  'thespian_system.log'))
            thesplogs = self.filecfg('thesplogs.cfg', '51200')
            os.environ['THESPLOG_FILE'] = thesplogf
            os.environ['THESPLOG_FILE_MAXSIZE'] = thesplogs
            self._asys = thespian.actors.ActorSystem(self.system_base,
                                                     capabilities,
                                                     logDefs = logcfg)
            self._start_source_authority(self._asys)
        return self._asys

    def _start_source_authority(self, asys):
        asys.tell(asys.createActor(
            'thespian.director.SourceAuthority',
            globalName='Director Source Authority'),
                  'Startup:%s' % self.sources_dir)

    @property
    def director(self):
        if not hasattr(self, '_director'):
            self._director = self.asys.createActor('thespian.director.Director',
                                                   globalName='Director')
        return self._director

    @staticmethod
    def _ask_for(asys, director, req, check_resp, timeout=ask_wait):
        endtime = datetime.now() + timeout
        rsp = asys.ask(director, req, timeout)
        if check_resp(rsp):
            return rsp
        timenow = datetime.now()
        while timenow < endtime:
            rsp = asys.listen(endtime - timenow)
            if check_resp(rsp):
                return rsp
            print('Discarding unexpected: %s'%str(rsp))
            timenow = datetime.now()
        return None

    @staticmethod
    def _ask_director(asys, director, req, dir_resp,
                      timeout=ask_wait, silent=False):
        r = DirectorControl._ask_for(asys, director, req,
                                     lambda m: m and
                                     'DirectorResponse' in m and
                                     m['DirectorResponse'] == dir_resp,
                                     timeout=timeout)
        if not r or not r['Success']:
            if not silent:
                sys.stderr.write('%s ERROR: %s\n' %
                                 (r['DirectorResponse'] if r else str(req),
                                  str(r or 'Timeout')))
            return None
        return r


    # def cmd_source_info(self, args): vds_decode output for specified source

    def cmd_gensrc(self, group_name, private_keyfile, inpsrc_dir, *srcs):
        """The gensrc command is used to create a loadable source file that is
           signed by a private key (for which there is a .tskey public key that
           the director can use to validate the loaded source).

           Arguments:  group private_keyfile sources_dir [version:v] [deps:depfile] [tli:tlifile] srcfile [...]

               group is the group name (there should be a
               corresponding .tli file)

               private_keyfile is filename containing the private
               X.509 key used to sign the output source


               sources_dir is the path to the root of the source directory tree

               depfile specifies a file listing dependent packages by
               import name, one per line.  If not specified, no
               dependent packages are included in the loadable source
               file.

               v specifies the optional version; the default is no
               version.  If v is "date", then the version is the
               timestamp YYYYMMDDHHMM.

               srcfile ... specifies the source files to be packaged
               into the loadable source file.

        """
        try:
            opts = {'deps': None,
                    'tli': None,
                    'version': None}
            while srcs[0].split(':')[0] in opts:
                opt = srcs[0].split(':')[0]
                opts[opt] = srcs[0][len(opt)+1:]
                if opt == 'version' and opts[opt] == 'date':
                    opts[opt] = datetime.now().strftime('%Y%m%d%H%M')
                srcs = srcs[1:]
        except IndexError:
            sys.stderr.write('Missing required gensrc command arguments\n')
            return 3
        self.verbose(': %s\n  '
                     .join('Package version source_dir deps signing_key'
                           ' info_file source_count .'
                           .split()) %
                     (os.path.join(self.sources_dir, group_name),
                      opts['version'], inpsrc_dir, opts['deps'],
                      private_keyfile, opts['tli'], len(srcs)))
        zfname = ''.join([group_name,
                          '-%s'%opts['version'] if 'version' in opts else '',
                          '.zip'])
        zfpath = os.path.join(self.sources_dir, zfname)
        os.chdir(inpsrc_dir)
        sys.path.insert(0, '.')
        zipfile.os.stat = zipstat

        cleanpath = lambda p: os.path.expandvars(
            os.path.expanduser(p[len(inpsrc_dir)+1:]
                               if p.startswith(inpsrc_dir) else p))
        self.verbose('Writing %s', zfpath)
        import importlib
        try:
            with zipfile.PyZipFile(zfpath, 'w', zipfile.ZIP_DEFLATED) as zf:
                for src in srcs:
                    for filename in glob.glob(cleanpath(src)):
                        # syntax checking; throw exception on failure:
                        importlib.import_module(os.path.splitext(filename)[0]
                                                .replace('/', '.'))
                        zf.write(filename)
                if opts.get('deps', []):
                    for line in open(cleanpath(opts['deps']), 'r'):
                        dep = line.strip()
                        if not dep or dep[0] == '#': continue
                        try:
                            importlib.import_module(dep)
                        except ImportError as ex:
                            print('Warning on import of dep %s: %s' %
                                  (dep, str(ex)))
                        depmod = sys.modules[dep]
                        if getattr(depmod, '__path__', None):
                            root = depmod.__path__[0]
                            relrootlen = len(os.path.dirname(root))+1
                            self.verbose(' Adding dep:: [%s]%s',
                                         root[:relrootlen],
                                         root[relrootlen:])
                            for adir, dirs, files in os.walk(root):
                                for pyfile in [F for F in files
                                               if F.endswith('.py')]:
                                    zf.write(
                                        os.path.join(adir, pyfile),
                                        os.path.join(adir[relrootlen:], pyfile))
                        else:
                            root = depmod.__file__
                            self.verbose(' Adding dep.: %s', root)
                            zf.write(root, os.path.basename(root))
            sfpath = SourceEncoding.zip_to_tls(zfpath, private_keyfile,
                                               self.verbose)
            print('Wrote',sfpath)
            inptli = opts.get('tli', None) or \
                     os.path.join(inpsrc_dir, group_name + '.tli')
            if os.path.exists(inptli):
                shutil.copyfile(inptli,
                                os.path.join(self.sources_dir,
                                             group_name + '.tli'))
                tli = eval(open(inptli,'r').read())
                try:
                    limit = int(tli.pop('TLS_Keep_Limit', 0))
                except Exception:
                    limit = 0
                if limit > 0:
                    glf = GroupLoadableFiles(self.sources_dir, group_name)
                    for oldfile in glf.tls_filenames()[limit:]:
                        os.remove(oldfile)

            return 0
        finally:
            try:
                os.remove(zfpath)
            except Exception: pass
        return 5 # Failure (normally skipped due to exception exit)

    def all_group_loadable_sources(self):
        return [GroupLoadableFiles(self.sources_dir, F)
                for F in glob.glob(os.path.join(self.sources_dir, '*.tli'))]

    def cmd_avail(self):
        "Shows all available loadable sources and hashes by group, most recent first."
        print('THESPIAN_DIRECTOR_DIR=%(sources_dir)s' % self.__dict__)
        for each in self.all_group_loadable_sources():
            print('%s: %s' % (each.group_name,
                              ('\n' + (' '*len(each.group_name)) + '+ ').join(
                                  ['%s    %s' % (
                                      F[len(self.sources_dir)+1:]
                                      if F.startswith(self.sources_dir + '/')
                                      else F,
                                      hashlib.md5(open(F,'rb').read())
                                      .hexdigest())
                                   for F in each.tls_filenames()])))
        return 0

    def cmd_shutdown(self):
        "Shuts down any currently running sytem"
        # Use minimal actor system with no extra startups for shutdown req.
        thespian.actors.ActorSystem(self.system_base,
                                    {'Admin Port': self.admin_port })\
                       .shutdown()
        old_system_base = self.filecfg('oldthespbase.cfg', None)
        if old_system_base and old_system_base != self.system_base:
            thespian.actors.ActorSystem(old_system_base,
                                        {'Admin Port': self.admin_port })\
                           .shutdown()
        print('Shutdown completed')


    def cmd_bootstart(self, name=None, nostart=None):
        """Makes system changes to cause the Thespian and Director to be
           started up at system boot time, refreshing all available
           loadable sources in the $THESPIAN_DIRECTOR_DIR.

             bootstart [name] [nostart]

           The optional first argument is the name to assign to the
           system startup service, defaulting to "thespian.director".

           If nostart is specified as an optional argument then only
           the boot configuration is written.  By default, the service
           is started via the configuration after it is written.
        """
        if name == "nostart":
            nostart = "nostart"
            name = None
        name = name or 'thespian.director'
        description = "Thespian Actor Director to manage loaded sources"
        if not os.path.isdir(self.sources_dir):
            sys.stderr.write('THESPIAN_DIRECTOR_DIR "%s" does not exist'
                             '; cannot create boot start configuration\n' %
                             self.sources_dir)
            return 5
        if os.name == 'nt':
            # Requires nssm (https://nssm.cc/) to be installed and in the path
            nssm = lambda *args: subprocess.call(['nssm'] + list(args))
            import subprocess
            try:
                r = nssm('stop', name) or \
                    nssm('remove', name, 'confirm')
            except FileNotFoundError:
                # nssm executable does not exist... try to install it.
                try:
                    from urllib.request import urlopen
                except Exception:
                    from urllib2 import urlopen
                from io import BytesIO
                rmt = urlopen('http://nssm.cc/release/nssm-2.24.zip')
                zf = zipfile.ZipFile(BytesIO(rmt.read()))
                import platform
                if '64' in platform.architecture()[0]:
                    zf_fname = 'nssm-2.24/win64/nssm.exe'
                    zf_fhash = 'f689ee9af94b00e9e3f0bb072b34caaf207f32dcb4f5782fc9ca351df9a06c97'
                else:
                    zf_fname = 'nssm-2.24/win64/nssm.exe'
                    zf_fhash = '472232ca821b5c2ef562ab07f53638bc2cc82eae84cea13fbe674d6022b6481c'
                tgt_fname = 'c:/Windows/System32/nssm.exe'
                import hashlib
                if False and hashlib.sha256(zf.read(zf_fname)).hexdigest() != zf_fhash:
                    sys.stderr.write('Invalid hash for nssm executable!'
                                     ' Cannot install bootstart\n')
                    return 8
                if os.path.exists(tgt_fname):
                    sys.stderr.write('The nssm target %s already exists.'
                                     ' Cannot install bootstart\n')
                    return 9
                with open(tgt_fname, 'wb') as nssmf:
                    nssmf.write(zf.read(zf_fname))
                print('Installed %s' % tgt_fname)
            except Exception:
                # The "name" service is probably not configured or running
                pass
            r = nssm('install', name,
                     os.path.normpath(sys.executable),
                     '-m', 'thespian.director', 'start', 'wait') or \
                nssm('set', name, 'AppDirectory',
                     os.path.normpath(self.sources_dir)) or \
                nssm('set', name, 'AppExit', '2', 'Exit') or \
                nssm('set', name, 'Description', description)
            # The following does not appear to work at present:
            # nssm('set', name, 'AppEnvironmentExtra',
            #      'THESPIAN_DIRECTOR_DIR=%s' % self.sources_dir)
            if r:
                sys.stderr.write('Windows bootstart configuration failed.\n')
                return r
            print('Windows bootstart installed')
            return r if nostart else nssm('start', name)
        elif os.name == 'posix':
            import subprocess
            try:
                r = subprocess.check_output(['systemctl', '--version'])
            except OSError:
                r = b'no'
            if b'systemd' in r:
                name = name.replace('.', '-')
                with open(os.path.join('/usr/lib/systemd/system',
                                       name + '.service'), 'w') as sf:
                    sf.write('''# Thespian Director (http://thespianpy.org) system boot startup.

[Unit]
Description=%(description)s
After=network-online.target

[Service]
Type=forking
Restart=on-failure
RestartSec=60
Environment=THESPIAN_DIRECTOR_DIR=%(sources_dir)s
EnvironmentFile=-/etc/sysconfig/thespian
ExecStart=%(python)s -m thespian.director start
ExecStop=%(python)s -m thespian.director shutdown

[Install]
WantedBy=multi-user.target
'''
                             % { 'python': sys.executable,
                                 'description': description,
                                 'sources_dir': self.sources_dir,
                             })
                # KWQ: preset instead of start if nostart
                # KWQ: try-restart
                print('systemctl enable %s' % name)
                r = subprocess.call(['systemctl', 'enable', name])
                if r:
                    sys.stderr.write('Error enabling %s: %d\n' % (name, r))
                    return r
                time.sleep(1)  # daemon gets confused if restarted immediately
                print('systemctl daemon-reload')
                r = subprocess.call(['systemctl', 'daemon-reload'])
                if r:
                    sys.stderr.write('Error reloading systemctl daemon: %s\n' % str(r))
                    return r
                if nostart:
                    return r
                print('systemctl start --no-block %s' % name)
                r = subprocess.call(['systemctl', 'start', '--no-block', name])
                if r:
                    sys.stderr.write('Error starting %s: %d\n' % (name, r))
                return r
            if os.path.isdir('/etc/init'):
                name = name.replace('.', '-')
                with open(os.path.join('/etc/init',
                                       name + '.conf'), 'w') as uf:
                    uf.write('''# Thespian Director (http://thespianpy.org) system boot startup.
description "%(description)s"
#start on runlevel [23]
# The above does not always emit the right events; the solution below
# seems to be more functional.
start on stopped rc RUNLEVEL=[23]
stop on [06]

#respawn
expect daemon
#oom score -5

env THESPIAN_DIRECTOR_DIR=%(sources_dir)s
export THESPIAN_DIRECTOR_DIR

script
    if [ -f /etc/syconfig/thespian ]; then . /etc/sysconfig/thespian; fi
    exec ${PYTHON:-%(python)s} -m thespian.director start
end script
pre-stop script
    exec ${PYTHON:-%(python)s} -m thespian.director shutdown
end script
'''
                             % { 'python': sys.executable,
                                 'description': description,
                                 'sources_dir': self.sources_dir,
                             })
                if nostart:
                    return 0
                print('initctl start %s' % name)
                return subprocess.call(['initctl', 'start', name])
            if os.path.isdir('/etc/init.d'):
                # service name status|start|stop|restart
                print('SystemV bootstart TBD')
                return 1

        print('Unknown os type "%s", cannot create boot configuration.' %
              os.name)
        return 2


    def cmd_tlsinfo(self, tlsfname, contents_or_extract_name=None):

        """Shows information about the specified .tls file specified as the
           first argument.  If the optional second argument is
           "contents" then a table of contents listing of the .tls
           file is generated.  If the optional second argument is not
           contents, it should be the name of a file in the .tls file
           which will be written to stdout.  The THESPIAN_DIRECTOR_DIR
           will be searched for a public key that can be used to
           validate the tls file, just as the Director's Source
           Authority would.
        """
        if not os.path.exists(tlsfname):
            f2 = os.path.join(self.sources_dir, tlsfname)
            if not os.path.exists(f2):
                sys.stderr.write('The tls file "%s" does not exist\n' % tlsfname)
                return 4
            tlsfname = f2
        with open(tlsfname, 'rb') as tlsf:
            tlsdata = tlsf.read()
        for keyfile in glob.glob(os.path.join(self.sources_dir, '*_tls.key')):
            self.verbose('..Attempting validation with key %s', keyfile)
            with open(keyfile, 'r') as keyf:
                public_key = keyf.read()
            sdata = SourceEncoding.tls_to_zip(tlsdata, public_key, self.verbose)
            if not sdata:
                continue
            zipf = zipfile.ZipFile(BytesIO(sdata))
            if not contents_or_extract_name:
                badf = zipf.testzip()
                if badf:
                    sys.stderr.write('Bad file in verified zipfile: %s\n' % badf)
                    return 6
                print('Validated "%s" loadable with key %s' %
                      (tlsfname, keyfile))
            elif contents_or_extract_name == 'contents':
                for info in zipf.infolist():
                    print(' %6d  %s' % (info.file_size, info.filename))
            else:
                for line in zipf.read(contents_or_extract_name).split(b'\n'):
                    print(line.decode('utf-8'))
            return 0
        sys.stderr.write('Could not validate signature on "%s"\n' % tlsfname)
        return 7

    def cmd_load(self, group_or_file):
        """Loads the specified loadable source along with the info file.  The
argument specifies the group name or loadable source filename.

This command will also startup the Director and the Actor System if it
is not already started.

Loadable sources are found in the THESPIAN_DIRECTOR_DIR with the
suffix ".tls", and described by files in the same directory with the
suffix ".tli".  Each .tli file describes a separate loadable group
(see the DeclareGroup message), and the corresponding loadable sources
have the same filename with an optional "VERSION" appended and the
.tls suffix.

The contents of the tli file are evaluated to form the python
expression that specifies the message passed to the started actor.
The tli also recognizes the following top-level entries in the main
dictionary read from the tli file:

    "TLS_Keep_Limit": integer,

  * The TLS_Keep_Limit specifies the limit of the number of tls
    files for each group will be left after a successful gensrc
    command operation; gensrc will delete older sources until this
    limit is reached.  If not specified, or if the value is less than
    one, this directive is ignored.

The Director splits the tls file VERSION into a sequence of numbers
and characters and sorts each against each other, interpreting the
highest value as the most recent.  The following examples are ordered
with the highest version at the top of the list:

   foo-201601280945.tls    # datetime versioning
   foo-201601251343.tls
   foo-201601251202.tls
   foo-15a5gamma1.tls      # mixed numeric and string versioning
   foo-15a5beta4.tls
   foo-15a3.tls
   foo-15a1.tls
   foo-15a.tls
   foo-15.2.1.tls          # multi-field numeric versioning
   foo-15.2.tls
   foo-15.1.tls
   foo-15.tls              # simple numeric versioning
   foo-05.tls
   foo-1.tls
   foo-0.tls
   foo.tls                 # no version

        """
        lg = GroupLoadableFiles(self.sources_dir, group_or_file)
        tlinfo = eval(open(lg.tli_filename(), 'r').read())
        tlinfo['DirectorOp'] = 'DefineGroup'
        tlinfo['Group'] = lg.group_name
        r = self._ask_director(self.asys, self.director, tlinfo, 'DeclaredGroup')
        if not r:
            return 1
        r = self._ask_director(self.asys, self.director,
                               { 'DirectorOp': 'LoadSource',
                                 'Source': lg.tls_filenames()[0],
                                 'Group': lg.group_name,
                               },
                               'SourceLoading',
                               self.ask_wait)
        if not r:
            return 1
        print('Loaded "%s" %s: %s' % (lg.group_name, lg.tls_filenames()[0],
                                      r['SourceHash']))
        return 0

    def cmd_start(self, and_wait=False):
        """This command starts up the Thespian Actor System, Director, and
           Source Authority (if not already started) and refreshes all
           loaded modules.  The latter is essentially a call to the
           refresh command, but the latter will not start the system
           if there are no source to be loaded whereas this command
           will.

           If an optional argument is supplied then this command will
           block indefinitely after performing the above actions,
           which makes it suitable for situations where a long-running
           start application is needed.
        """
        # starts actor system, director, and source authority
        self.asys.tell(self.director, 'start')
        r = self('refresh')
        if not r and and_wait:
            import time
            while True:
                time.sleep(3600)
        return r

    def cmd_refresh(self):
        "Verifies latest version of all group's sources is loaded."
        # load the latest version of all locally available sources
        for each in self.all_group_loadable_sources():
            r = self('load', each.group_name)
            if r:
                return r
        return 0

    def cmd_unload(self, source_hash):
        """Unloads the loaded source specified by the hash value argument.
           All actors created from that loaded source are killed.  If
           the active loaded source is unloaded, a new source from the
           same group is activated (the last loaded source).
        """
        self.asys.unloadActorSource(source_hash)
        return 0

    def get_role_address(self, role, group=None, silent=False):
        return self._ask_director(self.asys, self.director,
                                  { 'DirectorOp': 'RetrieveRole',
                                    'Group': group,
                                    'Role': role,
                                  }, 'RoleAddress',
                                  silent=silent)

    def cmd_list(self, group=None):
        """Lists the currently loaded sources and the actors created from
           those sources.  The optional argument can specify a group
           name to retrieve information only for that group.
        """
        reqmsg = { 'DirectorOp': 'RetrieveAll' }
        if group:
            reqmsg['Group'] = group
        r = self._ask_director(self.asys, self.director, reqmsg, 'AllAddresses')
        if not r:
            return 1
        groups = sorted(r['Groups'].keys())
        for group in groups:
            print('Group %s::' % group)
            gi = r['Groups'][group]
            for srchash in gi['Loaded']:
                print('  Source hash %s%s' %
                      (srchash,
                       '  [ACTIVE]'
                       if srchash == gi['ActiveHash'] else '') )
                for actor in gi['Running'].get(srchash, []):
                    role = actor.get('Role', None)
                    print('      %s -- %s%s' %
                          (actor['ActorAddress'],
                           actor['ActorClass'],
                           (' (%s)' % role) if role else ''))


class SourceEncoding(object):

    @staticmethod
    def zip_to_tls(zfpath, private_keyfile, verbose=lambda *a: None):
        # Adds a preface and appends the digest signature based on the
        # private key.  The zip is still useable in that form because
        # zip searches for internal markers and ignores prefix/suffix
        # data.
        sfdir = os.path.dirname(zfpath)
        sfname = os.path.splitext(os.path.basename(zfpath))[0] + \
                 GroupLoadableFiles.src_suffix
        sftmp = os.path.join(sfdir, '.'+sfname)
        sfpath = os.path.join(sfdir, sfname)
        sig = subprocess.check_output(["openssl", "dgst", "-sha256",
                                       "-sign", private_keyfile,
                                       zfpath])
        try:
            with open(sftmp, 'wb') as sf:
                verbose('Signing %s', sfpath)
                sf.write("ThespianDirectorFMT1-sha256\n".encode('utf-8'))
                with open(zfpath, 'rb') as zf:
                    sf.write(str(len(sig)).encode('utf-8'))
                    sf.write(b"\n")
                    # Remove zip's EOCD to disable zip access until file
                    # is validated, but remember where to put it back.
                    zfc_split = zf.read().split(b'PK\5\6')
                    sf.write(str(len(zfc_split[0])).encode('utf-8'))
                    sf.write(b"\n")
                    assert len(zfc_split) == 2
                    sf.write(zfc_split[0])
                    sf.write(zfc_split[1])
                    sf.write(sig)
                    os.rename(sftmp, sfpath)
                    return sfpath
        finally:
            try:
                os.remove(sftmp)
            except Exception: pass

    @staticmethod
    def tls_to_zip(tlsdata, public_key, verbose=None):
        verbose = verbose or (lambda *a: None)
        try:
            import thespian.rsasig
            hdr, data = thespian.rsasig.extract_ascii(tlsdata, 40)
            l1e = hdr.find('\n')
            l2e = hdr.find('\n', l1e+1)
            l1 = hdr[:l1e]  # format spec

            l1pf, l1ph = l1.split('-')
            hashfunc = getattr(hashlib, l1ph)
            if l1pf.startswith('ThespianDirectorFMT1'):
                verbose('Decoding director fmt 1')
                l3e = hdr.find('\n', l2e+1)
                return SourceEncoding.decode_fmt_d1 \
                    (tlsdata, public_key, hdr[l1e+1:l2e], hdr[l2e+1:l3e],
                     l3e, hashfunc)
            elif l1pf.startswith('ViceroyFMT'):
                # older encoding
                fmtnum = l1pf[len('ViceryFMT')+1:]
                verbose('Decoding format %s', fmtnum)
                return getattr(SourceEncoding, 'decode_fmt_v%s' % fmtnum) \
                    (tlsdata, public_key, hdr[l1e+1:l2e], l2e, hashfunc)
        except (ValueError, IndexError):
            pass
        return None

    @staticmethod
    def decode_fmt_d1(inpdata, publickey, hdr_l2, hdr_l3, hdr_endp, hashfunc):
        from thespian.rsasig import (to_bytelist, key_factors, verify, list_to_str)
        siglen = int(hdr_l2)
        eocdp = int(hdr_l3)
        inpDList = to_bytelist(inpdata)
        sig = inpDList[-siglen:]
        dta = inpDList[hdr_endp+1:hdr_endp+1+eocdp] + to_bytelist(b'PK\5\6') + \
              inpDList[hdr_endp+eocdp+1:-siglen]
        modN, e = key_factors(publickey)
        if not verify(dta, sig, modN, e, hashfunc): return None
        return list_to_str(dta)

    @staticmethod
    def decode_fmt_v1(inpdata, publickey, hdr_line_2, hdr_line_2_endpos, hashfunc):
        from thespian.rsasig import (to_bytelist, key_factors, verify, list_to_str)
        siglen = int(hdr_line_2)
        inpDList = to_bytelist(inpdata)
        sig = inpDList[hdr_line_2_endpos+1:hdr_line_2_endpos+1+siglen]
        dta = inpDList[hdr_line_2_endpos+1+siglen:]
        modN, e = key_factors(publickey)
        decDataDict = dict(zip(map(ord, ':ai \n='), map(ord, ' \n=ia:')))
        decdata = [decDataDict.get(E, E) for E in dta]
        if not verify(decdata, sig, modN, e, hashfunc): return None
        return list_to_str(decdata)

    @staticmethod
    def decode_fmt_v2(inpdata, publicKey, hdr_line_2, hdr_line_2_endpos, hashfunc):
        from thespian.rsasig import (to_bytelist, key_factors, verify, list_to_str)
        siglen = int(hdr_line_2)
        inpDList = to_bytelist(inpdata)
        sig = inpDList[-siglen:]
        dta = inpDList[hdr_line_2_endpos+1:-siglen]
        modN, e = key_factors(publicKey)
        if not verify(dta, sig, modN, e, hashfunc): return None
        return list_to_str(dta)


# Python restricts zipfile contents to a date >= 1980...
realstat = os.stat
def zipstat(fname):
    s = realstat(fname)
    timefix = lambda t: (time.mktime((1980,1,1,0,0,0,0,0,0))
                         if time.localtime(s.st_mtime)[0] < 1980
                         else t)
    return os.stat_result((s[0], s[1], s[2], s[3], s[4], s[5], s[6],
                          timefix(s[7]), timefix(s[8]), timefix(s[9])))


class GroupLoadableFiles(object):
    info_suffix = '.tli'
    src_suffix = '.tls'

    def __init__(self, director_dir, group_name):
        self.dirdir = director_dir
        # Input for group name can be the simple name, or a tli or tls filename
        self.group_name, ext = os.path.splitext(os.path.basename(group_name))
        if ext == self.src_suffix:
            self.group_name = self.group_name.split('-')[0]

    def tli_filename(self):
        return os.path.join(self.dirdir, self.group_name + self.info_suffix)

    def tli_contents(self):
        try:
            with open(self.tli_filename(), 'r') as tli:
                return json.loads(tli.read())
        except json.decoder.JSONDecodeError as ex:
            sys.stderr.write('Unable to load %s: %s\n' %
                             (self.tli_filename(), str(ex)))

    @staticmethod
    def versionExtract(baseName, extension='.tls'):
        class VerPart(object):
            def __init__(self, verpart): self._vp = verpart
            def __str__(self): return str(self._vp)
            def __eq__(self, o):
                try:
                    return self._vp == o._vp
                except TypeError:
                    return False
            def __lt__(self, o):
                try:
                    return self._vp < o._vp
                except TypeError:
                    if isinstance(self._vp, int):
                        return True
                    return False
            def __call__(self): return self._vp
        def _getVerPart(name):
            if not os.path.basename(name).startswith(baseName): return []
            isNum = None
            isStr = None
            retVer = []
            for each in os.path.basename(name)[len(baseName):
                                               (-len(extension)
                                                if name.endswith(extension)
                                                else None)]:
                if each in '0123456789':
                    if isStr is not None:
                        retVer.append(isStr)
                        isStr = None
                    digit = eval(each)
                    isNum = digit if isNum is None else ((isNum * 10) + digit)
                else:
                    if isNum is not None:
                        retVer.append(isNum)
                        isNum = None
                    isStr = each if isStr is None else (isStr + each)
            return list(map(VerPart, filter(lambda x: x is not None,
                                            retVer + [isStr, isNum])))
        return _getVerPart

    def tls_filenames(self):
        tls_file = os.path.join(self.dirdir, self.group_name+self.src_suffix)
        tls_files = glob.glob(os.path.join(self.dirdir,
                                           self.group_name +
                                           '-*' + self.src_suffix)) + \
                    ([tls_file] if os.path.exists(tls_file) else [])
        tls_files.sort(key=self.versionExtract(self.group_name),
                       reverse=True)
        return tls_files



### ----------------------------------------------------------------------
### CLI invocation
###

if __name__ == "__main__":
    sys.exit(DirectorControl()(*tuple(sys.argv[1:] or ["help"])))
