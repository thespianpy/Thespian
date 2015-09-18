#!/usr/bin/env python

#"This is an administrative tool for the Thespian system."

import cmd
import socket
import traceback
import logging
import datetime
import sys, os
sys.path.insert(0, os.getcwd())
from thespian.actors import *
from thespian.system.messages.status import *


class ThespianShell(cmd.Cmd):
    intro = "Thespian Actor shell.  Type help or '?' to list commands.'\n"
    prompt = 'thespian> '

    def __init__(self, *args, **kw):
        cmd.Cmd.__init__(self, *args, **kw)
        #super(ThespianShell, self).__init__(*args, **kw)
        self.system = None
        self.knownActorAddresses = []   # val=ActorAddress

    def emptyline(self): pass  # do nothing

    def do_exit(self, arg):
        'Exit the Thespian shell.  No cleanup or shutdown of existing Actor Systems or Actors.'
        self.system = None
        print ('Exiting.')
        return True

    def do_quit(self, arg):
        "Exits the Thespian shell immediately; no shutdown or cleanup of ActorSystems or Actors."
        return self.do_exit(arg)

    def do_EOF(self, arg):
        "Exits the Thespian shell immediately; no shutdown or cleanup of ActorSystems or Actors."
        return self.do_exit(arg)

    def parseActorAddress(self, arg):
        args = dict(zip(['ipaddr', 'port'], arg.split()))
        if not args.get('port', None) and ':' in args.get('ipaddr', ''):
            args = dict(zip(['ipaddr', 'port'], args['ipaddr'].split(':')))
        print ('Args is: %s'%args)
        try:
            # This reaches into the internals: most of the time it
            # should not be possible to simply synthsize an Actor
            # Address.
            return self.system._systemBase.transport.getAddressFromString((args['ipaddr'], args['port']))
        except Exception as ex:
            print ('***ERROR: unable to parse IP address or port specification (%s)'%str(ex))
            traceback.print_exc(limit=3)
            return None


    def parseActorNum(self, arg, useDefault=False):
        if arg.strip():
            try:
                split = arg.split(' ')
                anum = eval(split[0])
                rem  = ' '.join(split[1:])
                if anum >= len(self.knownActorAddresses):
                    print ('***ERROR: Specified Actor Address #%d is not known'%anum)
                    return None
                return anum, self.knownActorAddresses[anum], rem
            except:
                print ('***ERROR parsing Actor number from command line.')
                traceback.print_exc(limit=3)
        else:
            if useDefault:
                if not len(self.knownActorAddresses):
                    self.knownActorAddresses.append(self.parseActorAddress('127.0.0.1 14334'))
                return 0, self.knownActorAddresses[0], ''
        return None


    def getOrAddAddress(self, address):
        for N,A in enumerate(self.knownActorAddresses):
            if A == address:
                return N,A
        self.knownActorAddresses.append(address)
        return len(self.knownActorAddresses) - 1, address


    def showAddress(self, address):
        if address:
            addrinfo = self.getOrAddAddress(address)
            return '%s [#%d]'%(addrinfo[1], addrinfo[0])
        return str(address)


    def do_start(self, arg):
        """Starts an ActorSystem.  The first optional argument is the
           SystemBase.  The remainder of the line (if any) is parsed
           as the capabilities dictionary to pass to the
           ActorSystem.
        """
        if self.system:
            print ('Shutting down previous ActorSystem')
            self.system.shutdown()
            del self.system
            self.system = None
        if arg:
            base = arg.split()[0]
            capspec = ' '.join(arg.split()[1:])
            caps = eval(capspec) if capspec else {}
            print ('Starting %s ActorSystem\nCapabilities: %s'%(base, str(caps)))
            try:
                self.system = ActorSystem(base, caps)
                print ('Started %s ActorSystem'%base)
            except ImportError:
                print ('***ERROR starting ActorSystem with specified Base: %s'%base)
                import traceback
                traceback.print_exc()
            except ActorSystemException as ex:
                print ('***ERROR from Actor System: %s'%str(ex))
        else:
            self.system = ActorSystem()
            print ('Started default ActorSystem')

    def do_stop(self, arg):
        """Stops any currently-running ActorSystem."""
        ActorSystem().shutdown()
        if self.system:
            print ('Erasing previous ActorSystem')
            del self.system
            self.system = None

    def do_info(self, arg):
        "Gets information about the local environment."
        print ('Thespian local information:')
        if self.system:
            print ('Local system: %s'%str(self.system))
        if hasattr(ActorSystem, 'systemBase'):
            print ('Singleton ActorSystem base: %s'%str(ActorSystem.systemBase.__class__.__name__))
        else:
            print ('No ActorSystem seems to be running')
        for N,A in enumerate(self.knownActorAddresses):
            print ('Actor Address:  %s [#%d]'%(str(A), N))

    def do_actorCount(self, arg):
        "Counts running Actors and ActorSystems.  The optional argument is the addres ID# of the ActorSystem or Actor to begin counting at."
        systems = {}  # key=actorsystemAdminaddress (None if starting at an Actor), value = dict{Actortype:count}
        startTime = datetime.datetime.now()
        actorAddrAndMsg = self.parseActorNum(arg, True)
        if not getattr(self, 'system', None):
            print ('***ERROR: no current ActorSystem to send ask to.')
        elif actorAddrAndMsg:
            anum, actorAddr, msg = actorAddrAndMsg
        response = self.system.ask(actorAddr, Thespian_StatusReq(), 10)
        systems = self._parseStatusResponse(response)
        sys.stdout.write('\n')

        emptySystems        = 0
        minNonZeroPerSystem = 0
        maxPerSystem        = 0
        totalAllSystems     = 0
        countPerType        = {}
        for system in systems:
            if system == 'Unresponsive Actors':
                totalAllSystems += systems[system]
            elif len(systems[system]) == 0:
                emptySystems += 1
            else:
                systemCount = sum([systems[system][S] for S in systems[system]])
                if minNonZeroPerSystem == 0:
                    minNonZeroPerSystem = systemCount
                else:
                    if systemCount < minNonZeroPerSystem: minNonZeroPerSystem = systemCount
                if systemCount > maxPerSystem: maxPerSystem = systemCount
                totalAllSystems += systemCount
                for aType in systems[system]:
                    countPerType = self._countSubActor(countPerType, aType, systems[system][aType])
        endTime = datetime.datetime.now()

        print('# Actor Systems - Total: %d'%len(systems))
        print('                - Empty: %d'%emptySystems)
        print('# Actors -        Total: %d'%totalAllSystems)
        print('         - Unresponsive: %d'%systems.get('Unresponsive Actors', 0))
        print('         -   Min/System: %d'%minNonZeroPerSystem)
        print('         -   Max/System: %d'%maxPerSystem)
        print('         -   Avg/System: %.02f'%(totalAllSystems * 1.0 / len(systems)))
        for aType in countPerType:
            print('         = %4d  %s'%(countPerType[aType], aType))
        print(' Time to count: %s'%(endTime - startTime))
        print('')


    def _parseStatusResponse(self, response):
        systems = {}
        if not response: return systems
        if isinstance(response, Thespian_SystemStatus):
            for each in response.conventionAttendees:
                if isinstance(each, ActorAddress):
                    systems[each[0]] = self._parseStatusResponse(self.system.ask(each[0], Thespian_StatusReq(), 10))
            systems = self._gatherSubActorCounts(systems, response)
            # globals and deadletter handlers should be in children, so shouldn't need to count them
            # systems[response.adminAddress]['global'] = len(Response.globalActors)
            # dead letters...
        elif isinstance(response, Thespian_ActorStatus):
            systems = self._gatherSubActorCounts(systems, response)
        return systems


    def _gatherSubActorCounts(self, systems, response):
        sys.stdout.write('.')
        sys.stdout.flush()
        if response is None:
            systems['Unresponsive Actors'] = systems.get('Unresponsive Actors', 0) + 1
        else:
            if isinstance(response, Thespian_ActorStatus):
                systems[response.adminAddress] = self._countSubActor(systems.get(response.adminAddress, None), response.actorClass)
            for each in response.childActors:
                systems = self._gatherSubActorCounts(systems, self.system.ask(each, Thespian_StatusReq(), 10))
        return systems


    @staticmethod
    def _countSubActor(counts, actorClass, number=1):
        idx = actorClass
        if counts:
            counts[idx] = counts.get(idx, 0) + number
        else:
            counts = {idx: number}
        return counts


    def do_python(self, arg):
        "Runs a python command"
        print (eval(arg))


    def do_status(self, arg):
        """Get the status of an Actor or ActorSystem.  The optional argument is the address ID# of the ActorSystem or Actor, defaulting to address ID0, which defaults to (IPv4 127.0.0.1:14334) if not set."""
        actorAddrAndMsg = self.parseActorNum(arg, True)
        if not getattr(self, 'system', None):
            print ('***ERROR: no current ActorSystem to send ask to.')
        elif actorAddrAndMsg:
            anum, actorAddr, msg = actorAddrAndMsg
            print ('Requesting status from Actor (or Admin) @ %s (#%d)'%(str(actorAddr), anum))
            response = self.system.ask(actorAddr, Thespian_StatusReq(), 10)
            if response is None:
                print ('***ERROR: no response from %s [#%d]'%(str(actorAddr), anum))
            else:
                formatStatus(response, self.showAddress)


    def do_address(self, arg):
        """Adds the IP address and port as a known ActorAddress that can be used.  The first optional argument is the IPv4 address to specify (defaulting to 127.0.0.1) and the second optional argument is the socket (defaulting to 14334).

Alternatively, if the first optional argument contains a colon, that is assumed to be the separator between the IPv4 address and the port number."""
        actorAddr = self.parseActorAddress(arg)
        if actorAddr:
            N,A = self.getOrAddAddress(actorAddr)
            print ('Actor Address %d:  %s'%(N, str(A)))
        else:
            print ('Not able to determine a valid Actor Address from command-line arguments.')


    def do_create_testActor(self, arg):
        "Creates a Test Actor"
        import StringIO, zipfile
        zipdata = StringIO.StringIO()
        zf = zipfile.ZipFile(zipdata, 'a')
        zf.writestr('t.py', test_actor_source)
        zf.close()
        loadf = StringIO.StringIO(zipdata.getvalue())
        actorSys = self.system or ActorSystem()
        loadf_hash = actorSys.loadActorSource(loadf)
        try:
            na = actorSys.createActor('t.TestActor', sourceHash = loadf_hash)
        except:
            print ('***ERROR creating Actor t.TestActor from sourceHash %s'%(loadf_hash))
            traceback.print_exc(limit=3)
        else:
            N,A = self.getOrAddAddress(na)
            print ('Created new TestActor %d @ %s'%(N, str(A)))


    def do_tell(self, arg):
        """Sends a message to the identified Actor; does not wait for a response.  The first argument is required and is the Actor number (see create_testActor) and the rest of the line is the message string to send to the Actor."""
        actorAddrAndMsg = self.parseActorNum(arg)
        if actorAddrAndMsg:
            anum, addr, msg = actorAddrAndMsg
            try:
                (self.system or ActorSystem()).tell(addr, msg)
            except:
                print ('***ERROR telling Actor #%d (%s)'%(anum, str(addr)))
                traceback.print_exc(limit=3)


    def do_ask(self, arg):
        """Sends a message to the identified Actor and waits for a response from any Actor (for up to 10 seconds).  The first argument is required and is the Actor number (see create_testActor) and the rest of the line is the message string to send to the Actor."""
        actorAddrAndMsg = self.parseActorNum(arg)
        if actorAddrAndMsg:
            anum, addr, msg = actorAddrAndMsg
            try:
                r = (self.system or ActorSystem()).ask(addr, msg, 10)
                print ('Response: %s'%(r or '<None... timed out>'))
            except:
                print ('***ERROR asking Actor #%d (%s)'%(anum, str(addr)))
                traceback.print_exc(limit=3)


    def do_listen(self, arg):
        """Listens for messages from any Actor in the system for up to 10 seconds."""
        try:
            r = (self.system or ActorSystem()).listen(10)
            print('Received message: %s'%(r or '<None... timed out>'))
        except:
                print ('***ERROR listening')
                traceback.print_exc(limit=3)


    def do_kill(self, arg):
        "Kills the specified Actor by sending it an ActorExitRequest. The required argument is the Actor number."
        actorAddrAndMsg = self.parseActorNum(arg)
        if actorAddrAndMsg:
            anum, addr, msg = actorAddrAndMsg
            r = (self.system or ActorSystem()).tell(addr, ActorExitRequest())
            print ('Actor #%d (%s) exit request sent.'%(anum, addr))


    def do_report_exit(self, arg):
        """Informs the parent (first argument) that the child specified by the
           second argument has exited.  This is especially useful when
           the child is hung in the "defunct" state.
        """
        actorAddrAndMsg = self.parseActorNum(arg)
        if actorAddrAndMsg:
            parent_anum, parent_addr, msg_rem = actorAddrAndMsg
            actorAddrAndMsg = self.parseActorNum(msg_rem)
            if actorAddrAndMsg:
                child_anum, child_addr, rem = actorAddrAndMsg
                r = (self.system or ActorSystem()).tell(parent_addr,
                                                        ChildActorExited(child_addr))
                print ('Actor #%d (%s) sent notification of child #%d (%s) exit.'%(
                    parent_anum, parent_addr, child_anum, child_addr))
            else:
                print('***ERROR: requires specification of child actor instance')
        else:
            print('***ERROR: requires specification of parent and child actor instances')


    def do_set_thesplog(self, arg):
        'Updates the Thespian thesplog internal call functionality.  The first argument is the Actor number, the second argument is the logging threshold (e.g. "debug", "warning", etc.), the third argument is true or false to specify the forwarding of thesplog calls to python logging, and the fourth argument is true or false to specify whether to append thesplog output to /tmp/Thespian.log'
        try:
            from thespian.system.messages.logcontrol import SetLogging
        except ImportError:
            print ('** Sorry, log control not available on this system')
            return
        actorAddrAndSettings = self.parseActorNum(arg)
        if actorAddrAndSettings:
            anum, addr, settings = actorAddrAndSettings
            print('settings is <%s>'%(str(settings)))
            threshold,useLogging,useFile = tuple(settings.split(' '))
            l1 = {'debug': logging.DEBUG,
                  'info' : logging.INFO,
                  'warning': logging.WARNING,
                  'error' : logging.ERROR,
                  'critical' : logging.CRITICAL}.get(threshold.lower(),
                                                     logging.INFO)
            l2 = useLogging.lower() not in ['0', 'no', 'false']
            l3 = useFile.lower() not in ['0', 'no', 'false']
            r = (self.system or ActorSystem()).tell(addr,
                                                    SetLogging(l1, l2, l3))
            print('Actor #%d (%s) logging settings updated.'%(anum, addr))

test_actor_source = '''
from thespian.actors import *
import logging

class TestActor(Actor):
    def receiveMessage(self, msg, sender):
        logger = logging.getLogger('Thespian.Actor')
        logger.debug('TestActor @ %s got message "%s" from %s',
                     str(self.myAddress), str(msg), str(sender))
        if type(msg) == type(''):
            if msg == 'create':
                self.send(sender, self.createActor(TestActor))
            else:
                self.send(sender, 'TestActor @ %s got: %s'%(str(self.myAddress), str(msg)))
'''

        
if __name__ == "__main__":
    ThespianShell().cmdloop()
